package runtime

import (
	"archive/zip"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	goRuntime "runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	storageProtocolS3  = "s3"
	storageMountTool   = "rclone"
	rcloneDaemonWait   = "30s"
	rcloneVFSCacheMode = "writes"
)

// ExecEngine runs workloads as plain host processes.
type ExecEngine struct {
	logger *slog.Logger

	mu              sync.Mutex
	cmd             *exec.Cmd
	doneCh          chan struct{}
	waitErr         error
	logEmitter      LogEmitter
	mountedStorage  []mountedStorage
	preparedPayload *ExecPayload
}

type mountedStorage struct {
	mountPath string
	cacheDir  string
}

type storageMountProbeResult struct {
	PathExists     bool
	PathMode       os.FileMode
	MountInfo      *storageMountInfo
	MountInfoError error
	ProbeError     error
}

type storageMountInfo struct {
	MountPoint   string
	Filesystem   string
	Source       string
	MountOptions string
	SuperOptions string
}

var _ Engine = (*ExecEngine)(nil)
var _ LogEmitterAware = (*ExecEngine)(nil)

var inspectMountedStorage = defaultInspectMountedStorage
var rcloneDownloadBaseURL = "https://downloads.rclone.org"
var storageToolInstallRootOverride string

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

	if err := validateStorageMounts(execPayload.Mounts); err != nil {
		return err
	}
	if err := checkStorageTools(ctx, e.logger, execPayload.Mounts); err != nil {
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

	e.mu.Lock()
	logEmitter := e.logEmitter
	e.mu.Unlock()

	cmd, stdoutWriter, stderrWriter, err := e.buildCommand(ctx, execPayload, logEmitter)
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

	e.cmd = cmd
	e.doneCh = make(chan struct{})
	e.waitErr = nil
	e.mountedStorage = mounted

	go e.waitForExit(logCtx, cmd, mounted, stdoutWriter, stderrWriter)
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

// Interrupt sends SIGINT to the workload process group.
func (e *ExecEngine) Interrupt(ctx context.Context) error {
	return e.signalProcessGroup(ctx, syscall.SIGINT, "SIGINT")
}

// Stop sends SIGTERM to the workload process group.
func (e *ExecEngine) Stop(ctx context.Context) error {
	return e.signalProcessGroup(ctx, syscall.SIGTERM, "SIGTERM")
}

func (e *ExecEngine) signalProcessGroup(ctx context.Context, signal syscall.Signal, signalName string) error {
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
	if err := syscall.Kill(-pid, signal); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return fmt.Errorf("send %s to process group %d: %w", signalName, pid, err)
	}

	e.logger.InfoContext(ctx, "exec workload stopping", slog.Int("pid", pid), slog.String("signal", signalName))

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

func (e *ExecEngine) buildCommand(
	ctx context.Context,
	payload ExecPayload,
	emitter LogEmitter,
) (*exec.Cmd, *lineEmitterWriter, *lineEmitterWriter, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}

	cmd := exec.Command(payload.Command, payload.Args...)
	cmd.Env = mergeEnv(os.Environ(), payload.Env)
	cmd.Dir = payload.WorkDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdoutWriter := newLineEmitterWriter(LogStreamStdout, emitter)
	stderrWriter := newLineEmitterWriter(LogStreamStderr, emitter)
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter

	return cmd, stdoutWriter, stderrWriter, nil
}

func (e *ExecEngine) waitForExit(
	ctx context.Context,
	cmd *exec.Cmd,
	mounted []mountedStorage,
	stdoutWriter *lineEmitterWriter,
	stderrWriter *lineEmitterWriter,
) {
	err := cmd.Wait()
	stdoutWriter.Flush()
	stderrWriter.Flush()

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
		protocol := strings.TrimSpace(mount.Protocol)
		if protocol == "" {
			protocol = storageProtocolS3
		}
		if !strings.EqualFold(protocol, storageProtocolS3) {
			return fmt.Errorf("mount %d uses unsupported storage protocol %q", index+1, mount.Protocol)
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

	}

	return nil
}

func checkStorageTools(ctx context.Context, logger *slog.Logger, mounts []StorageMount) error {
	if len(mounts) == 0 {
		return nil
	}

	if _, err := ensureRcloneAvailable(ctx, logger); err != nil {
		return err
	}
	if _, _, err := unmountCommand(); err != nil {
		return err
	}
	return nil
}

func ensureRcloneAvailable(ctx context.Context, logger *slog.Logger) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	path, pathErr := exec.LookPath(storageMountTool)
	if pathErr == nil {
		return path, nil
	}

	installedPath, installErr := installRclone(ctx, logger)
	if installErr != nil {
		return "", fmt.Errorf("look up %q: %w; automatic download failed: %w", storageMountTool, pathErr, installErr)
	}

	if _, err := os.Stat(installedPath); err != nil {
		return "", fmt.Errorf("look up %q after automatic download: %w", storageMountTool, err)
	}

	return installedPath, nil
}

func installRclone(ctx context.Context, logger *slog.Logger) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	installRoot, err := storageToolInstallRoot()
	if err != nil {
		return "", err
	}
	targetPath := filepath.Join(installRoot, storageMountTool)
	if info, statErr := os.Stat(targetPath); statErr == nil && info.Mode().IsRegular() {
		return targetPath, nil
	}

	if err := os.MkdirAll(installRoot, 0o755); err != nil {
		return "", fmt.Errorf("create rclone install root %q: %w", installRoot, err)
	}

	archiveURL, err := rcloneArchiveURL()
	if err != nil {
		return "", err
	}
	if logger != nil {
		logger.InfoContext(ctx, "downloading rclone automatically", slog.String("url", archiveURL))
	}

	archivePath, err := downloadRcloneArchive(ctx, archiveURL)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = os.Remove(archivePath)
	}()

	if err := extractRcloneBinary(archivePath, targetPath); err != nil {
		return "", err
	}

	if logger != nil {
		logger.InfoContext(ctx, "downloaded rclone automatically", slog.String("path", targetPath))
	}
	return targetPath, nil
}

func storageToolInstallRoot() (string, error) {
	if storageToolInstallRootOverride != "" {
		return storageToolInstallRootOverride, nil
	}

	if dir, err := os.UserCacheDir(); err == nil && strings.TrimSpace(dir) != "" {
		return filepath.Join(dir, "arco", "tools", storageMountTool), nil
	}

	return filepath.Join(os.TempDir(), "arco-tools", storageMountTool), nil
}

func rcloneArchiveURL() (string, error) {
	if goRuntime.GOOS != "linux" {
		return "", fmt.Errorf("%s auto-download is only supported on linux", storageMountTool)
	}

	arch := ""
	switch goRuntime.GOARCH {
	case "amd64":
		arch = "amd64"
	case "arm64":
		arch = "arm64"
	default:
		return "", fmt.Errorf("%s auto-download is not supported on architecture %q", storageMountTool, goRuntime.GOARCH)
	}

	return fmt.Sprintf("%s/rclone-current-linux-%s.zip", rcloneDownloadBaseURL, arch), nil
}

func downloadRcloneArchive(ctx context.Context, archiveURL string) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, archiveURL, nil)
	if err != nil {
		return "", fmt.Errorf("build rclone download request: %w", err)
	}

	client := &http.Client{Timeout: 5 * time.Minute}
	response, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("download rclone archive from %s: %w", archiveURL, err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		if trimmed := strings.TrimSpace(string(body)); trimmed != "" {
			return "", fmt.Errorf("download rclone archive from %s: unexpected status %s: %s", archiveURL, response.Status, trimmed)
		}
		return "", fmt.Errorf("download rclone archive from %s: unexpected status %s", archiveURL, response.Status)
	}

	file, err := os.CreateTemp("", "arco-rclone-*.zip")
	if err != nil {
		return "", fmt.Errorf("create temporary rclone archive: %w", err)
	}
	defer file.Close()

	if _, err := io.Copy(file, response.Body); err != nil {
		_ = os.Remove(file.Name())
		return "", fmt.Errorf("write temporary rclone archive %q: %w", file.Name(), err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(file.Name())
		return "", fmt.Errorf("close temporary rclone archive %q: %w", file.Name(), err)
	}

	return file.Name(), nil
}

func extractRcloneBinary(archivePath string, targetPath string) error {
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		return fmt.Errorf("open rclone archive %q: %w", archivePath, err)
	}
	defer reader.Close()

	temporaryFile, err := os.CreateTemp(filepath.Dir(targetPath), "rclone-*")
	if err != nil {
		return fmt.Errorf("create temporary rclone binary in %q: %w", filepath.Dir(targetPath), err)
	}
	temporaryPath := temporaryFile.Name()
	success := false
	defer func() {
		if !success {
			temporaryFile.Close()
			_ = os.Remove(temporaryPath)
		}
	}()

	foundBinary := false
	for _, file := range reader.File {
		if file.FileInfo().IsDir() || filepath.Base(file.Name) != storageMountTool {
			continue
		}

		openedFile, err := file.Open()
		if err != nil {
			return fmt.Errorf("open %q from rclone archive %q: %w", file.Name, archivePath, err)
		}
		_, copyErr := io.Copy(temporaryFile, openedFile)
		closeErr := openedFile.Close()
		if copyErr != nil {
			return fmt.Errorf("extract %q from rclone archive %q: %w", file.Name, archivePath, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close %q from rclone archive %q: %w", file.Name, archivePath, closeErr)
		}
		foundBinary = true
		break
	}

	if !foundBinary {
		return fmt.Errorf("rclone archive %q did not contain %q", archivePath, storageMountTool)
	}
	if err := temporaryFile.Chmod(0o755); err != nil {
		return fmt.Errorf("chmod temporary rclone binary %q: %w", temporaryPath, err)
	}
	if err := temporaryFile.Close(); err != nil {
		return fmt.Errorf("close temporary rclone binary %q: %w", temporaryPath, err)
	}
	if err := os.Rename(temporaryPath, targetPath); err != nil {
		return fmt.Errorf("move temporary rclone binary %q to %q: %w", temporaryPath, targetPath, err)
	}
	success = true

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

	mountBinary, err := ensureRcloneAvailable(ctx, e.logger)
	if err != nil {
		return mountedStorage{}, err
	}

	mountPath := filepath.Clean(strings.TrimSpace(mount.MountPath))
	if err := os.MkdirAll(mountPath, 0o755); err != nil {
		return mountedStorage{}, fmt.Errorf("create mount path %q: %w", mountPath, err)
	}

	cacheDir, err := os.MkdirTemp("", "arco-rclone-cache-*")
	if err != nil {
		return mountedStorage{}, fmt.Errorf("create rclone cache dir: %w", err)
	}
	success := false
	defer func() {
		if !success {
			_ = os.RemoveAll(cacheDir)
		}
	}()

	remoteSpec := buildRcloneRemoteSpec(mount)
	prefix := strings.Trim(strings.TrimSpace(mount.Prefix), "/")
	args := []string{
		"mount",
		remoteSpec,
		mountPath,
		"--daemon",
		"--daemon-wait", rcloneDaemonWait,
		"--cache-dir", cacheDir,
		"--vfs-cache-mode", rcloneVFSCacheMode,
		"--log-level", "ERROR",
	}

	command := exec.CommandContext(ctx, mountBinary, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		if trimmed := strings.TrimSpace(string(output)); trimmed != "" {
			return mountedStorage{}, fmt.Errorf("run rclone mount: %w: %s", err, trimmed)
		}
		return mountedStorage{}, fmt.Errorf("run rclone mount: %w", err)
	}

	mountedItem := mountedStorage{
		mountPath: mountPath,
		cacheDir:  cacheDir,
	}
	probe := inspectMountedStorage(mountPath)
	if err := validateMountedStorage(mount, probe); err != nil {
		e.logger.WarnContext(
			ctx,
			"s3-compatible storage mount failed validation",
			slog.String("bucket", strings.TrimSpace(mount.Bucket)),
			slog.String("mount_path", mountPath),
			slog.String("diagnostics", formatStorageMountDiagnostics(probe)),
			slog.String("error", err.Error()),
		)

		cleanupErr := e.cleanupMountedStorageAfterProbeFailure(ctx, mountedItem, probe)
		if cleanupErr != nil {
			return mountedStorage{}, errors.Join(err, cleanupErr)
		}
		return mountedStorage{}, err
	}

	e.logger.InfoContext(
		ctx,
		"mounted s3-compatible storage",
		slog.String("bucket", mount.Bucket),
		slog.String("mount_path", mountPath),
		slog.Bool("has_prefix", prefix != ""),
	)

	success = true
	return mountedItem, nil
}

func buildRcloneRemoteSpec(mount StorageMount) string {
	bucketPath := strings.TrimSpace(mount.Bucket)
	if prefix := strings.Trim(strings.TrimSpace(mount.Prefix), "/"); prefix != "" {
		bucketPath = bucketPath + "/" + prefix
	}

	options := []string{
		quoteRcloneConnectionOption("provider", "Other"),
		quoteRcloneConnectionOption("access_key_id", strings.TrimSpace(mount.AccessKeyID)),
		quoteRcloneConnectionOption("secret_access_key", strings.TrimSpace(mount.SecretAccessKey)),
		quoteRcloneConnectionOption("endpoint", strings.TrimSpace(mount.Endpoint)),
		"env_auth=false",
		fmt.Sprintf("force_path_style=%t", mount.UsePathStyle),
	}
	if region := strings.TrimSpace(mount.Region); region != "" {
		options = append(options, quoteRcloneConnectionOption("region", region))
	}
	if sessionToken := strings.TrimSpace(mount.SessionToken); sessionToken != "" {
		options = append(options, quoteRcloneConnectionOption("session_token", sessionToken))
	}

	return fmt.Sprintf(":s3,%s:%s", strings.Join(options, ","), bucketPath)
}

func quoteRcloneConnectionOption(key string, value string) string {
	escaped := strings.ReplaceAll(value, `"`, `""`)
	return fmt.Sprintf(`%s="%s"`, key, escaped)
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
		if mount.cacheDir != "" {
			if err := os.RemoveAll(mount.cacheDir); err != nil && !errors.Is(err, os.ErrNotExist) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove rclone cache dir %q: %w", mount.cacheDir, err))
			}
		}
	}
	return cleanupErr
}

func (e *ExecEngine) cleanupMountedStorageAfterProbeFailure(
	ctx context.Context,
	mount mountedStorage,
	probe storageMountProbeResult,
) error {
	var cleanupErr error

	shouldUnmount := mount.mountPath != "" && (probe.MountInfo != nil || probe.MountInfoError != nil)
	if shouldUnmount {
		if err := unmountStorage(mount.mountPath); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		} else {
			e.logger.InfoContext(ctx, "unmounted s3-compatible storage", slog.String("mount_path", mount.mountPath))
		}
	}

	if mount.cacheDir != "" {
		if err := os.RemoveAll(mount.cacheDir); err != nil && !errors.Is(err, os.ErrNotExist) {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove rclone cache dir %q: %w", mount.cacheDir, err))
		}
	}

	return cleanupErr
}

func validateMountedStorage(mount StorageMount, probe storageMountProbeResult) error {
	mountPath := filepath.Clean(strings.TrimSpace(mount.MountPath))
	summary := summarizeMountedStorage(mount)
	diagnostics := formatStorageMountDiagnostics(probe)

	switch {
	case !probe.PathExists:
		return fmt.Errorf(
			"storage mount command reported success but mount path %q does not exist (%s); diagnostics: %s",
			mountPath,
			summary,
			diagnostics,
		)
	case probe.MountInfo == nil && probe.MountInfoError == nil:
		return fmt.Errorf(
			"storage mount command reported success but mount path %q is not present in /proc/self/mountinfo (%s); diagnostics: %s",
			mountPath,
			summary,
			diagnostics,
		)
	case probe.ProbeError != nil:
		return fmt.Errorf(
			"storage mount command reported success but mount path %q is not readable (%s): %w; diagnostics: %s",
			mountPath,
			summary,
			probe.ProbeError,
			diagnostics,
		)
	default:
		return nil
	}
}

func summarizeMountedStorage(mount StorageMount) string {
	parts := []string{
		fmt.Sprintf("bucket=%q", strings.TrimSpace(mount.Bucket)),
		fmt.Sprintf("endpoint=%q", strings.TrimSpace(mount.Endpoint)),
		fmt.Sprintf("use_path_style=%t", mount.UsePathStyle),
		fmt.Sprintf("use_ssl=%t", mount.UseSSL),
	}

	if prefix := strings.Trim(strings.TrimSpace(mount.Prefix), "/"); prefix != "" {
		parts = append(parts, fmt.Sprintf("prefix=%q", prefix))
	}
	if region := strings.TrimSpace(mount.Region); region != "" {
		parts = append(parts, fmt.Sprintf("region=%q", region))
	}

	return strings.Join(parts, ", ")
}

func formatStorageMountDiagnostics(probe storageMountProbeResult) string {
	parts := []string{fmt.Sprintf("path_exists=%t", probe.PathExists)}
	if probe.PathExists {
		parts = append(parts, fmt.Sprintf("path_mode=%q", probe.PathMode.String()))
	}

	if probe.MountInfo != nil {
		parts = append(parts,
			fmt.Sprintf("mount_source=%q", probe.MountInfo.Source),
			fmt.Sprintf("mount_fs=%q", probe.MountInfo.Filesystem),
		)
		if probe.MountInfo.MountOptions != "" {
			parts = append(parts, fmt.Sprintf("mount_options=%q", probe.MountInfo.MountOptions))
		}
		if probe.MountInfo.SuperOptions != "" {
			parts = append(parts, fmt.Sprintf("super_options=%q", probe.MountInfo.SuperOptions))
		}
	} else if probe.MountInfoError != nil {
		parts = append(parts, fmt.Sprintf("mountinfo_error=%q", probe.MountInfoError.Error()))
	} else {
		parts = append(parts, `mount_entry="missing"`)
	}

	return strings.Join(parts, ", ")
}

func defaultInspectMountedStorage(mountPath string) storageMountProbeResult {
	result := storageMountProbeResult{}

	info, err := os.Stat(mountPath)
	switch {
	case err == nil:
		result.PathExists = true
		result.PathMode = info.Mode()
	case errors.Is(err, os.ErrNotExist):
		return result
	default:
		result.ProbeError = fmt.Errorf("stat mount path: %w", err)
	}

	mountInfo, err := readSelfMountInfo(mountPath)
	if err != nil {
		result.MountInfoError = err
	} else {
		result.MountInfo = mountInfo
	}

	if !result.PathExists {
		return result
	}
	if result.ProbeError != nil {
		return result
	}

	dir, err := os.Open(mountPath)
	if err != nil {
		result.ProbeError = fmt.Errorf("open mount path: %w", err)
		return result
	}
	defer dir.Close()

	if _, err := dir.ReadDir(1); err != nil && !errors.Is(err, io.EOF) {
		result.ProbeError = fmt.Errorf("read directory entries: %w", err)
	}

	return result
}

func readSelfMountInfo(targetPath string) (*storageMountInfo, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	targetPath = filepath.Clean(targetPath)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		entry, err := parseMountInfoLine(scanner.Text())
		if err != nil {
			continue
		}
		if filepath.Clean(entry.MountPoint) == targetPath {
			return entry, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan /proc/self/mountinfo: %w", err)
	}

	return nil, nil
}

func parseMountInfoLine(line string) (*storageMountInfo, error) {
	left, right, ok := strings.Cut(line, " - ")
	if !ok {
		return nil, fmt.Errorf("invalid mountinfo line: missing separator")
	}

	leftFields := strings.Fields(left)
	rightFields := strings.Fields(right)
	if len(leftFields) < 6 || len(rightFields) < 3 {
		return nil, fmt.Errorf("invalid mountinfo line: not enough fields")
	}

	return &storageMountInfo{
		MountPoint:   decodeMountInfoField(leftFields[4]),
		Filesystem:   decodeMountInfoField(rightFields[0]),
		Source:       decodeMountInfoField(rightFields[1]),
		MountOptions: decodeMountInfoField(leftFields[5]),
		SuperOptions: decodeMountInfoField(strings.Join(rightFields[2:], " ")),
	}, nil
}

func decodeMountInfoField(value string) string {
	if !strings.Contains(value, `\`) {
		return value
	}

	var builder strings.Builder
	builder.Grow(len(value))

	for index := 0; index < len(value); index++ {
		if value[index] == '\\' && index+3 < len(value) {
			octal := value[index+1 : index+4]
			decoded, ok := decodeMountInfoOctal(octal)
			if ok {
				builder.WriteByte(decoded)
				index += 3
				continue
			}
		}
		builder.WriteByte(value[index])
	}

	return builder.String()
}

func decodeMountInfoOctal(value string) (byte, bool) {
	var decoded byte
	for index := 0; index < len(value); index++ {
		digit := value[index]
		if digit < '0' || digit > '7' {
			return 0, false
		}
		decoded = decoded*8 + (digit - '0')
	}
	return decoded, true
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
