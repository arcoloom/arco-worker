package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// DockerEngine runs workloads as Docker containers on the worker host.
type DockerEngine struct {
	logger *slog.Logger

	mu              sync.Mutex
	cmd             *exec.Cmd
	doneCh          chan struct{}
	waitErr         error
	logEmitter      LogEmitter
	logWG           sync.WaitGroup
	mountedStorage  []mountedStorage
	preparedPayload *DockerPayload
	containerName   string
}

var _ Engine = (*DockerEngine)(nil)
var _ LogEmitterAware = (*DockerEngine)(nil)

// NewDockerEngine constructs an Engine backed by the Docker CLI.
func NewDockerEngine(logger *slog.Logger) *DockerEngine {
	if logger == nil {
		logger = slog.Default()
	}

	return &DockerEngine{
		logger: logger,
	}
}

func (e *DockerEngine) SetLogEmitter(emitter LogEmitter) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.logEmitter = emitter
}

func (e *DockerEngine) Prepare(ctx context.Context, payload []byte) error {
	dockerPayload, err := parseDockerPayload(ctx, payload)
	if err != nil {
		return err
	}

	if err := checkDockerAvailable(ctx); err != nil {
		return err
	}
	if err := validateStorageMounts(dockerPayload.Mounts); err != nil {
		return err
	}
	if err := checkStorageTools(dockerPayload.Mounts); err != nil {
		return err
	}

	workspace, err := prepareWorkspace(ctx, e.logger, dockerPayload.WorkspaceRoot, dockerPayload.Source, dockerPayload.WorkDir)
	if err != nil {
		return err
	}
	dockerPayload.WorkspaceRoot = workspace.hostRoot
	dockerPayload.WorkDir = workspace.containerWorkDir

	if err := ensureDockerImage(ctx, e.logger, dockerPayload.Image); err != nil {
		return err
	}

	e.mu.Lock()
	e.preparedPayload = &dockerPayload
	e.mu.Unlock()

	return nil
}

func (e *DockerEngine) Start(ctx context.Context, payload []byte) error {
	dockerPayload, err := e.preparedDockerPayload(ctx, payload)
	if err != nil {
		return err
	}

	containerName := dockerContainerName(dockerPayload.TaskID)
	cmd, stdoutPipe, stderrPipe, err := e.buildCommand(ctx, dockerPayload, containerName)
	if err != nil {
		return err
	}

	logCtx := context.WithoutCancel(ctx)
	mounted, err := e.mountStorage(logCtx, dockerPayload.Mounts)
	if err != nil {
		return err
	}

	e.mu.Lock()
	if e.cmd != nil {
		e.mu.Unlock()
		cleanupErr := e.cleanupStorage(logCtx, mounted)
		if cleanupErr != nil {
			return errors.Join(errors.New("docker workload already started"), cleanupErr)
		}
		return errors.New("docker workload already started")
	}

	if err := cmd.Start(); err != nil {
		e.mu.Unlock()
		cleanupErr := e.cleanupStorage(logCtx, mounted)
		startErr := fmt.Errorf("start docker container %q: %w", containerName, err)
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
	e.containerName = containerName

	e.logWG.Add(2)
	go e.forwardOutput(logCtx, stdoutPipe, LogStreamStdout, logEmitter)
	go e.forwardOutput(logCtx, stderrPipe, LogStreamStderr, logEmitter)
	go e.waitForExit(logCtx, cmd, mounted, containerName)
	e.mu.Unlock()

	e.logger.InfoContext(
		logCtx,
		"docker workload started",
		slog.String("container_name", containerName),
		slog.String("image", dockerPayload.Image),
		slog.String("work_dir", dockerPayload.WorkDir),
		slog.Int("storage_mounts", len(mounted)),
	)

	return nil
}

func (e *DockerEngine) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	e.mu.Lock()
	cmd := e.cmd
	containerName := e.containerName
	e.mu.Unlock()

	if cmd == nil || containerName == "" {
		return nil
	}
	if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
		return nil
	}

	command := exec.CommandContext(ctx, "docker", "stop", "--time", strconv.Itoa(dockerStopTimeoutSeconds), containerName)
	output, err := command.CombinedOutput()
	if err != nil {
		trimmed := strings.TrimSpace(string(output))
		if strings.Contains(trimmed, "No such container") {
			return nil
		}
		if trimmed != "" {
			return fmt.Errorf("docker stop %q: %w: %s", containerName, err, trimmed)
		}
		return fmt.Errorf("docker stop %q: %w", containerName, err)
	}

	e.logger.InfoContext(ctx, "docker workload stopping", slog.String("container_name", containerName), slog.String("signal", "SIGTERM"))

	return nil
}

func (e *DockerEngine) Wait(ctx context.Context) error {
	e.mu.Lock()
	doneCh := e.doneCh
	e.mu.Unlock()

	if doneCh == nil {
		return errors.New("docker workload not started")
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

const dockerStopTimeoutSeconds = 15

func (e *DockerEngine) buildCommand(
	ctx context.Context,
	payload DockerPayload,
	containerName string,
) (*exec.Cmd, io.ReadCloser, io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}

	args := []string{
		"run",
		"--rm",
		"--name", containerName,
		"--volume", payload.WorkspaceRoot + ":" + containerWorkspaceRoot,
	}
	if workDir := strings.TrimSpace(payload.WorkDir); workDir != "" {
		args = append(args, "--workdir", workDir)
	}

	for _, mount := range payload.Mounts {
		mountPath := strings.TrimSpace(mount.MountPath)
		if mountPath == "" {
			continue
		}
		args = append(args, "--volume", mountPath+":"+mountPath)
	}
	for key, value := range payload.Env {
		args = append(args, "--env", key+"="+value)
	}

	args = append(args, payload.Image)
	if len(payload.Command) > 0 {
		args = append(args, payload.Command...)
	}

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create docker stdout pipe: %w", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create docker stderr pipe: %w", err)
	}

	return cmd, stdoutPipe, stderrPipe, nil
}

func (e *DockerEngine) waitForExit(
	ctx context.Context,
	cmd *exec.Cmd,
	mounted []mountedStorage,
	containerName string,
) {
	err := cmd.Wait()
	e.logWG.Wait()

	waitErr := normalizeExitError(err)
	cleanupErr := e.cleanupStorage(ctx, mounted)
	if cleanupErr != nil {
		waitErr = errors.Join(waitErr, cleanupErr)
	}
	if waitErr != nil {
		e.logger.ErrorContext(ctx, "docker workload finished with error", slog.String("container_name", containerName), slog.String("error", waitErr.Error()))
	} else {
		e.logger.InfoContext(ctx, "docker workload finished", slog.String("container_name", containerName))
	}

	e.mu.Lock()
	e.cmd = nil
	e.waitErr = waitErr
	doneCh := e.doneCh
	e.mountedStorage = nil
	e.preparedPayload = nil
	e.containerName = ""
	e.mu.Unlock()

	if doneCh != nil {
		close(doneCh)
	}
}

func (e *DockerEngine) preparedDockerPayload(ctx context.Context, raw []byte) (DockerPayload, error) {
	e.mu.Lock()
	if e.preparedPayload != nil {
		payload := *e.preparedPayload
		e.mu.Unlock()
		return payload, nil
	}
	e.mu.Unlock()

	if err := e.Prepare(ctx, raw); err != nil {
		return DockerPayload{}, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.preparedPayload == nil {
		return DockerPayload{}, errors.New("docker payload was not prepared")
	}
	return *e.preparedPayload, nil
}

func (e *DockerEngine) forwardOutput(ctx context.Context, reader io.ReadCloser, stream LogStream, emitter LogEmitter) {
	defer e.logWG.Done()
	defer reader.Close()

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		if emitter != nil {
			emitter(LogEntry{
				Stream: stream,
				Line:   scanner.Text(),
			})
		}
	}

	if err := scanner.Err(); err != nil {
		e.logger.WarnContext(
			ctx,
			"failed to read docker output",
			slog.String("stream", string(stream)),
			slog.String("error", err.Error()),
		)
	}
}

func (e *DockerEngine) mountStorage(ctx context.Context, mounts []StorageMount) ([]mountedStorage, error) {
	execEngine := NewExecEngine(e.logger)
	return execEngine.mountStorage(ctx, mounts)
}

func (e *DockerEngine) cleanupStorage(ctx context.Context, mounts []mountedStorage) error {
	execEngine := NewExecEngine(e.logger)
	return execEngine.cleanupStorage(ctx, mounts)
}

func parseDockerPayload(ctx context.Context, raw []byte) (DockerPayload, error) {
	if err := ctx.Err(); err != nil {
		return DockerPayload{}, err
	}
	if len(raw) == 0 {
		return DockerPayload{}, errors.New("docker payload is empty")
	}

	var payload DockerPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return DockerPayload{}, fmt.Errorf("decode docker payload: %w", err)
	}

	if strings.TrimSpace(payload.TaskID) == "" {
		return DockerPayload{}, errors.New("docker payload task_id is required")
	}
	if strings.TrimSpace(payload.WorkspaceRoot) == "" {
		return DockerPayload{}, errors.New("docker payload workspace_root is required")
	}
	if strings.TrimSpace(payload.Image) == "" {
		return DockerPayload{}, errors.New("docker payload image is required")
	}
	if len(payload.Command) > 0 && strings.TrimSpace(payload.Command[0]) == "" {
		return DockerPayload{}, errors.New("docker payload command[0] must not be empty")
	}

	return payload, nil
}

func checkDockerAvailable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := exec.LookPath("docker"); err != nil {
		return fmt.Errorf("look up \"docker\": %w", err)
	}

	command := exec.CommandContext(ctx, "docker", "info")
	output, err := command.CombinedOutput()
	if err != nil {
		trimmed := strings.TrimSpace(string(output))
		if trimmed != "" {
			return fmt.Errorf("docker info: %w: %s", err, trimmed)
		}
		return fmt.Errorf("docker info: %w", err)
	}
	return nil
}

func ensureDockerImage(ctx context.Context, logger *slog.Logger, image string) error {
	command := exec.CommandContext(ctx, "docker", "image", "inspect", image)
	if output, err := command.CombinedOutput(); err == nil {
		_ = output
		return nil
	}

	if logger != nil {
		logger.InfoContext(ctx, "pulling docker image", slog.String("image", image))
	}

	command = exec.CommandContext(ctx, "docker", "pull", image)
	output, err := command.CombinedOutput()
	if err != nil {
		trimmed := strings.TrimSpace(string(output))
		if trimmed != "" {
			return fmt.Errorf("docker pull %q: %w: %s", image, err, trimmed)
		}
		return fmt.Errorf("docker pull %q: %w", image, err)
	}

	return nil
}

func dockerContainerName(taskID string) string {
	base := "arco-task-" + strings.ToLower(strings.TrimSpace(taskID))
	var builder strings.Builder
	builder.Grow(len(base))

	lastDash := false
	for _, r := range base {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			builder.WriteRune(r)
			lastDash = false
		case r == '-', r == '_', r == '.', r == '/':
			if builder.Len() == 0 || lastDash {
				continue
			}
			builder.WriteByte('-')
			lastDash = true
		}
	}

	name := strings.Trim(builder.String(), "-")
	if name == "" {
		return "arco-task"
	}
	if len(name) > 120 {
		return strings.TrimRight(name[:120], "-")
	}
	return name
}
