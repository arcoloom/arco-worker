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

// ExecEngine runs workloads as plain host processes.
type ExecEngine struct {
	logger *slog.Logger

	mu         sync.Mutex
	cmd        *exec.Cmd
	doneCh     chan struct{}
	waitErr    error
	logEmitter LogEmitter
	logWG      sync.WaitGroup
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

	if execPayload.WorkDir != "" {
		workDirInfo, statErr := os.Stat(execPayload.WorkDir)
		if statErr != nil {
			return fmt.Errorf("stat work dir %q: %w", execPayload.WorkDir, statErr)
		}
		if !workDirInfo.IsDir() {
			return fmt.Errorf("work dir %q is not a directory", execPayload.WorkDir)
		}
	}

	if err := checkCommandAvailable(ctx, execPayload); err != nil {
		return err
	}

	return nil
}

// Start launches the workload and returns immediately after the process starts.
func (e *ExecEngine) Start(ctx context.Context, payload []byte) error {
	execPayload, err := parseExecPayload(ctx, payload)
	if err != nil {
		return err
	}

	cmd, stdoutPipe, stderrPipe, err := e.buildCommand(ctx, execPayload)
	if err != nil {
		return err
	}

	logCtx := context.WithoutCancel(ctx)

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.cmd != nil {
		return errors.New("exec workload already started")
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start command %q: %w", execPayload.Command, err)
	}

	logEmitter := e.logEmitter
	e.cmd = cmd
	e.doneCh = make(chan struct{})
	e.waitErr = nil

	e.logWG.Add(2)
	go e.forwardOutput(logCtx, stdoutPipe, LogStreamStdout, cmd.Process.Pid, logEmitter)
	go e.forwardOutput(logCtx, stderrPipe, LogStreamStderr, cmd.Process.Pid, logEmitter)
	go e.waitForExit(logCtx, cmd)

	e.logger.InfoContext(
		logCtx,
		"exec workload started",
		slog.Int("pid", cmd.Process.Pid),
		slog.String("command", execPayload.Command),
		slog.String("work_dir", execPayload.WorkDir),
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

func (e *ExecEngine) waitForExit(ctx context.Context, cmd *exec.Cmd) {
	err := cmd.Wait()
	e.logWG.Wait()

	waitErr := normalizeExitError(err)
	if waitErr != nil {
		e.logger.ErrorContext(ctx, "exec workload finished with error", slog.Int("pid", cmd.Process.Pid), slog.String("error", waitErr.Error()))
	} else {
		e.logger.InfoContext(ctx, "exec workload finished", slog.Int("pid", cmd.Process.Pid))
	}

	e.mu.Lock()
	e.waitErr = waitErr
	doneCh := e.doneCh
	e.mu.Unlock()

	close(doneCh)
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
