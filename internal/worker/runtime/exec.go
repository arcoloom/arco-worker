package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

const storageDriverS3FS = "s3fs"

// ExecEngine runs workloads as plain host processes.
type ExecEngine struct {
	logger *slog.Logger

	mu              sync.Mutex
	cmd             *exec.Cmd
	doneCh          chan struct{}
	waitErr         error
	logEmitter      LogEmitter
	logWG           sync.WaitGroup
	mountedStorage  []mountedStorage
	preparedPayload *ExecPayload
}

type mountedStorage struct {
	mountPath  string
	passwdFile string
}

var _ Engine = (*ExecEngine)(nil)
var _ LogEmitterAware = (*ExecEngine)(nil)

// NewExecEngine constructs an Engine backed by os/exec.
func NewExecEngine(logger *slog.Logger) *ExecEngine {
	if logger == nil {
		logger = slog.Default()
	}

	return &ExecEngine{
		logger: logger,
	}
}

func (e *ExecEngine) SetLogEmitter(emitter LogEmitter) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.logEmitter = emitter
}

// Prepare validates the payload and checks that the target command can be resolved.
func (e *ExecEngine) Prepare(ctx context.Context, payload []byte) error {
	execPayload, err := parseExecPayload(ctx, payload)
	if err != nil {
		return err
	}

	workspace, err := prepareWorkspace(ctx, e.logger, execPayload.WorkspaceRoot, execPayload.Source, execPayload.WorkDir)
	if err != nil {
		return err
	}
	execPayload.WorkDir = workspace.hostWorkDir

	if err := checkCommandAvailable(ctx, execPayload); err != nil {
		return err
	}
	if err := validateStorageMounts(execPayload.Mounts); err != nil {
		return err
	}
	if err := checkStorageTools(execPayload.Mounts); err != nil {
		return err
	}

	e.mu.Lock()
	e.preparedPayload = &execPayload
	e.mu.Unlock()

	return nil
}

// Start launches the workload and returns immediately after the process starts.
func (e *ExecEngine) Start(ctx context.Context, payload []byte) error {
	execPayload, err := e.preparedExecPayload(ctx, payload)
	if err != nil {
		return err
	}

	cmd, stdoutPipe, stderrPipe, err := e.buildCommand(ctx, execPayload)
	if err != nil {
		return err
	}

	logCtx := context.WithoutCancel(ctx)
	mounted, err := e.mountStorage(logCtx, execPayload.Mounts)
	if err != nil {
		return err
	}

	e.mu.Lock()
	if e.cmd != nil {
		e.mu.Unlock()
		cleanupErr := e.cleanupStorage(logCtx, mounted)
		if cleanupErr != nil {
			return errors.Join(errors.New("exec workload already started"), cleanupErr)
		}
		return errors.New("exec workload already started")
	}

	if err := cmd.Start(); err != nil {
		e.mu.Unlock()
		cleanupErr := e.cleanupStorage(logCtx, mounted)
		startErr := fmt.Errorf("start command %q: %w", execPayload.Command, err)
		if cleanupErr != nil {
			return errors.Join(startErr, cleanupErr)
		}
		return startErr
	}

	logEmitter := e.logEmitter
	e.cmd = cmd
	e.doneCh = make(chan struct{})
	e.waitErr = nil
	e.mountedStorage = mounted

	e.logWG.Add(2)
	go e.forwardOutput(logCtx, stdoutPipe, LogStreamStdout, cmd.Process.Pid, logEmitter)
	go e.forwardOutput(logCtx, stderrPipe, LogStreamStderr, cmd.Process.Pid, logEmitter)
	go e.waitForExit(logCtx, cmd, mounted)
	e.mu.Unlock()

	e.logger.InfoContext(
		logCtx,
		"exec workload started",
		slog.Int("pid", cmd.Process.Pid),
		slog.String("command", execPayload.Command),
		slog.String("work_dir", execPayload.WorkDir),
		slog.Int("storage_mounts", len(mounted)),
	)

	return nil
}

// Stop sends SIGTERM to the workload process group.
func (e *ExecEngine) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	e.mu.Lock()
	cmd := e.cmd
	e.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		return nil
	}

	if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
		return nil
	}

	pid := cmd.Process.Pid
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return fmt.Errorf("send SIGTERM to process group %d: %w", pid, err)
	}

	e.logger.InfoContext(ctx, "exec workload stopping", slog.Int("pid", pid), slog.String("signal", "SIGTERM"))

	return nil
}

// Wait blocks until the workload exits or the provided context is canceled.
func (e *ExecEngine) Wait(ctx context.Context) error {
	e.mu.Lock()
	doneCh := e.doneCh
	e.mu.Unlock()

	if doneCh == nil {
		return errors.New("exec workload not started")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-doneCh:
		e.mu.Lock()
		defer e.mu.Unlock()
		return e.waitErr
	}
}

func (e *ExecEngine) buildCommand(ctx context.Context, payload ExecPayload) (*exec.Cmd, io.ReadCloser, io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}

	cmd := exec.Command(payload.Command, payload.Args...)
	cmd.Env = mergeEnv(os.Environ(), payload.Env)
	cmd.Dir = payload.WorkDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create stdout pipe: %w", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create stderr pipe: %w", err)
	}

	return cmd, stdoutPipe, stderrPipe, nil
}

func (e *ExecEngine) waitForExit(ctx context.Context, cmd *exec.Cmd, mounted []mountedStorage) {
	err := cmd.Wait()
	e.logWG.Wait()

	waitErr := normalizeExitError(err)
	cleanupErr := e.cleanupStorage(ctx, mounted)
	if cleanupErr != nil {
		waitErr = errors.Join(waitErr, cleanupErr)
	}
	if waitErr != nil {
		e.logger.ErrorContext(ctx, "exec workload finished with error", slog.Int("pid", cmd.Process.Pid), slog.String("error", waitErr.Error()))
	} else {
		e.logger.InfoContext(ctx, "exec workload finished", slog.Int("pid", cmd.Process.Pid))
	}

	e.mu.Lock()
	e.cmd = nil
	e.waitErr = waitErr
	doneCh := e.doneCh
	e.mountedStorage = nil
	e.preparedPayload = nil
	e.mu.Unlock()

	if doneCh != nil {
		close(doneCh)
	}
}

func (e *ExecEngine) preparedExecPayload(ctx context.Context, raw []byte) (ExecPayload, error) {
	e.mu.Lock()
	if e.preparedPayload != nil {
		payload := *e.preparedPayload
		e.mu.Unlock()
		return payload, nil
	}
	e.mu.Unlock()

	if err := e.Prepare(ctx, raw); err != nil {
		return ExecPayload{}, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.preparedPayload == nil {
		return ExecPayload{}, errors.New("exec payload was not prepared")
	}
	return *e.preparedPayload, nil
}

func (e *ExecEngine) forwardOutput(ctx context.Context, reader io.ReadCloser, stream LogStream, pid int, emitter LogEmitter) {
	defer e.logWG.Done()
	defer reader.Close()

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if emitter != nil {
			emitter(LogEntry{
				Stream: stream,
				Line:   line,
			})
		}
	}

	if err := scanner.Err(); err != nil {
		e.logger.WarnContext(
			ctx,
			"failed to read exec output",
			slog.Int("pid", pid),
			slog.String("stream", string(stream)),
			slog.String("error", err.Error()),
		)
	}
}

func parseExecPayload(ctx context.Context, raw []byte) (ExecPayload, error) {
	if err := ctx.Err(); err != nil {
		return ExecPayload{}, err
	}

	if len(raw) == 0 {
		return ExecPayload{}, errors.New("exec payload is empty")
	}

	var payload ExecPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ExecPayload{}, fmt.Errorf("decode exec payload: %w", err)
	}

	if strings.TrimSpace(payload.Command) == "" {
		return ExecPayload{}, errors.New("exec payload command is required")
	}
	if strings.TrimSpace(payload.WorkspaceRoot) == "" {
		return ExecPayload{}, errors.New("exec payload workspace_root is required")
	}

	return payload, nil
}

func checkCommandAvailable(ctx context.Context, payload ExecPayload) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if strings.Contains(payload.Command, string(os.PathSeparator)) {
		commandPath := payload.Command
		if payload.WorkDir != "" && !filepath.IsAbs(commandPath) {
			commandPath = filepath.Join(payload.WorkDir, commandPath)
		}

		info, err := os.Stat(commandPath)
		if err != nil {
			return fmt.Errorf("stat command %q: %w", commandPath, err)
		}
		if info.IsDir() {
			return fmt.Errorf("command %q points to a directory", commandPath)
		}

		return nil
	}

	if _, err := exec.LookPath(payload.Command); err != nil {
		return fmt.Errorf("look up command %q: %w", payload.Command, err)
	}

	return nil
}

func validateStorageMounts(mounts []StorageMount) error {
	if len(mounts) == 0 {
		return nil
	}

	seenPaths := make(map[string]struct{}, len(mounts))
	for index, mount := range mounts {
		driver := strings.TrimSpace(mount.Driver)
		if driver == "" {
			driver = storageDriverS3FS
		}
		if !strings.EqualFold(driver, storageDriverS3FS) {
			return fmt.Errorf("mount %d uses unsupported storage driver %q", index+1, mount.Driver)
		}

		if strings.TrimSpace(mount.Bucket) == "" {
			return fmt.Errorf("mount %d bucket is required", index+1)
		}
		if strings.TrimSpace(mount.Endpoint) == "" {
			return fmt.Errorf("mount %d endpoint is required", index+1)
		}
		if strings.TrimSpace(mount.AccessKeyID) == "" {
			return fmt.Errorf("mount %d access_key_id is required", index+1)
		}
		if strings.TrimSpace(mount.SecretAccessKey) == "" {
			return fmt.Errorf("mount %d secret_access_key is required", index+1)
		}

		mountPath := filepath.Clean(strings.TrimSpace(mount.MountPath))
		if mountPath == "." || !filepath.IsAbs(mountPath) {
			return fmt.Errorf("mount %d path %q must be absolute", index+1, mount.MountPath)
		}
		if mountPath == string(os.PathSeparator) {
			return fmt.Errorf("mount %d path %q cannot be /", index+1, mount.MountPath)
		}
		if _, exists := seenPaths[mountPath]; exists {
			return fmt.Errorf("mount path %q is duplicated", mountPath)
		}
		seenPaths[mountPath] = struct{}{}

		for _, option := range mount.ExtraOptions {
			trimmed := strings.TrimSpace(option)
			if trimmed == "" {
				return fmt.Errorf("mount %d extra_options contains an empty entry", index+1)
			}
			lower := strings.ToLower(trimmed)
			switch {
			case lower == "use_path_request_style",
				strings.HasPrefix(lower, "url="),
				strings.HasPrefix(lower, "endpoint="),
				strings.HasPrefix(lower, "passwd_file="):
				return fmt.Errorf("mount %d extra option %q conflicts with managed s3fs options", index+1, trimmed)
			}
		}
	}

	return nil
}

func checkStorageTools(mounts []StorageMount) error {
	if len(mounts) == 0 {
		return nil
	}

	if _, err := exec.LookPath(storageDriverS3FS); err != nil {
		return fmt.Errorf("look up %q: %w", storageDriverS3FS, err)
	}
	if _, _, err := unmountCommand(); err != nil {
		return err
	}
	return nil
}

func (e *ExecEngine) mountStorage(ctx context.Context, mounts []StorageMount) ([]mountedStorage, error) {
	if len(mounts) == 0 {
		return []mountedStorage{}, nil
	}

	mounted := make([]mountedStorage, 0, len(mounts))
	for index, mount := range mounts {
		mountedItem, err := e.mountSingleStorage(ctx, mount)
		if err != nil {
			rollbackErr := e.cleanupStorage(ctx, mounted)
			mountErr := fmt.Errorf("mount storage %d at %q: %w", index+1, mount.MountPath, err)
			if rollbackErr != nil {
				return nil, errors.Join(mountErr, rollbackErr)
			}
			return nil, mountErr
		}
		mounted = append(mounted, mountedItem)
	}

	return mounted, nil
}

func (e *ExecEngine) mountSingleStorage(ctx context.Context, mount StorageMount) (mountedStorage, error) {
	if err := ctx.Err(); err != nil {
		return mountedStorage{}, err
	}

	mountPath := filepath.Clean(strings.TrimSpace(mount.MountPath))
	if err := os.MkdirAll(mountPath, 0o755); err != nil {
		return mountedStorage{}, fmt.Errorf("create mount path %q: %w", mountPath, err)
	}

	passwdFile, err := writeS3FSPasswordFile(mount.AccessKeyID, mount.SecretAccessKey)
	if err != nil {
		return mountedStorage{}, err
	}

	bucketArg := strings.TrimSpace(mount.Bucket)
	prefix := strings.Trim(strings.TrimSpace(mount.Prefix), "/")
	if prefix != "" {
		bucketArg = fmt.Sprintf("%s:/%s", bucketArg, prefix)
	}

	args := []string{
		bucketArg,
		mountPath,
		"-o", "passwd_file=" + passwdFile,
		"-o", "url=" + strings.TrimSpace(mount.Endpoint),
	}
	if mount.UsePathStyle {
		args = append(args, "-o", "use_path_request_style")
	}
	if region := strings.TrimSpace(mount.Region); region != "" {
		args = append(args, "-o", "endpoint="+region)
	}
	for _, option := range mount.ExtraOptions {
		args = append(args, "-o", strings.TrimSpace(option))
	}

	command := exec.CommandContext(ctx, storageDriverS3FS, args...)
	command.Env = storageCommandEnv(mount)
	output, err := command.CombinedOutput()
	if err != nil {
		_ = os.Remove(passwdFile)
		if trimmed := strings.TrimSpace(string(output)); trimmed != "" {
			return mountedStorage{}, fmt.Errorf("run s3fs: %w: %s", err, trimmed)
		}
		return mountedStorage{}, fmt.Errorf("run s3fs: %w", err)
	}

	e.logger.InfoContext(
		ctx,
		"mounted s3-compatible storage",
		slog.String("bucket", mount.Bucket),
		slog.String("mount_path", mountPath),
		slog.Bool("has_prefix", prefix != ""),
	)

	return mountedStorage{
		mountPath:  mountPath,
		passwdFile: passwdFile,
	}, nil
}

func (e *ExecEngine) cleanupStorage(ctx context.Context, mounts []mountedStorage) error {
	if len(mounts) == 0 {
		return nil
	}

	var cleanupErr error
	for index := len(mounts) - 1; index >= 0; index-- {
		mount := mounts[index]
		if mount.mountPath != "" {
			if err := unmountStorage(mount.mountPath); err != nil {
				cleanupErr = errors.Join(cleanupErr, err)
			} else {
				e.logger.InfoContext(ctx, "unmounted s3-compatible storage", slog.String("mount_path", mount.mountPath))
			}
		}
		if mount.passwdFile != "" {
			if err := os.Remove(mount.passwdFile); err != nil && !errors.Is(err, os.ErrNotExist) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove passwd file %q: %w", mount.passwdFile, err))
			}
		}
	}
	return cleanupErr
}

func writeS3FSPasswordFile(accessKeyID string, secretAccessKey string) (string, error) {
	file, err := os.CreateTemp("", "arco-s3fs-passwd-*")
	if err != nil {
		return "", fmt.Errorf("create s3fs passwd file: %w", err)
	}
	path := file.Name()
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		_ = os.Remove(path)
		return "", fmt.Errorf("chmod s3fs passwd file %q: %w", path, err)
	}
	if _, err := file.WriteString(strings.TrimSpace(accessKeyID) + ":" + strings.TrimSpace(secretAccessKey)); err != nil {
		file.Close()
		_ = os.Remove(path)
		return "", fmt.Errorf("write s3fs passwd file %q: %w", path, err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("close s3fs passwd file %q: %w", path, err)
	}
	return path, nil
}

func storageCommandEnv(mount StorageMount) []string {
	env := mergeEnv(os.Environ(), map[string]string{
		"AWS_ACCESS_KEY_ID":     strings.TrimSpace(mount.AccessKeyID),
		"AWS_SECRET_ACCESS_KEY": strings.TrimSpace(mount.SecretAccessKey),
	})
	if token := strings.TrimSpace(mount.SessionToken); token != "" {
		env = mergeEnv(env, map[string]string{"AWS_SESSION_TOKEN": token})
	}
	return env
}

func unmountStorage(mountPath string) error {
	command, baseArgs, err := unmountCommand()
	if err != nil {
		return err
	}

	args := append(append([]string(nil), baseArgs...), mountPath)
	cmd := exec.Command(command, args...)
	output, runErr := cmd.CombinedOutput()
	if runErr != nil {
		if trimmed := strings.TrimSpace(string(output)); trimmed != "" {
			return fmt.Errorf("unmount %q with %s: %w: %s", mountPath, command, runErr, trimmed)
		}
		return fmt.Errorf("unmount %q with %s: %w", mountPath, command, runErr)
	}
	return nil
}

func unmountCommand() (string, []string, error) {
	for _, candidate := range []struct {
		command string
		args    []string
	}{
		{command: "fusermount3", args: []string{"-u"}},
		{command: "fusermount", args: []string{"-u"}},
		{command: "umount", args: []string{}},
	} {
		if _, err := exec.LookPath(candidate.command); err == nil {
			return candidate.command, candidate.args, nil
		}
	}
	return "", nil, errors.New("no supported unmount command found (tried fusermount3, fusermount, umount)")
}

func normalizeExitError(err error) error {
	if err == nil {
		return nil
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		exitCode := exitErr.ExitCode()
		if exitCode >= 0 {
			return fmt.Errorf("process exited with code %d: %w", exitCode, err)
		}
		return fmt.Errorf("process exited abnormally: %w", err)
	}

	return fmt.Errorf("wait for process: %w", err)
}

func mergeEnv(base []string, overrides map[string]string) []string {
	if len(overrides) == 0 {
		return base
	}

	values := make(map[string]string, len(base)+len(overrides))
	order := make([]string, 0, len(base)+len(overrides))

	for _, entry := range base {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if _, exists := values[key]; !exists {
			order = append(order, key)
		}
		values[key] = value
	}

	for key, value := range overrides {
		if _, exists := values[key]; !exists {
			order = append(order, key)
		}
		values[key] = value
	}

	merged := make([]string, 0, len(order))
	for _, key := range order {
		merged = append(merged, key+"="+values[key])
	}

	return merged
}
