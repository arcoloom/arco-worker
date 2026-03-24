package terminal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"github.com/creack/pty"
)

type EventType string

const (
	EventTypeReady  EventType = "ready"
	EventTypeOutput EventType = "output"
	EventTypeExit   EventType = "exit"
	EventTypeError  EventType = "error"
)

type Event struct {
	Type     EventType
	SessionID string
	Data     []byte
	ExitCode int32
	Message  string
}

type LaunchOptions struct {
	Cols uint16
	Rows uint16
}

type Process interface {
	io.ReadWriteCloser
	Resize(cols uint16, rows uint16) error
	Wait() error
	Terminate(context.Context) error
}

type Launcher interface {
	Launch(context.Context, LaunchOptions) (Process, error)
}

type Manager struct {
	logger   *slog.Logger
	launcher Launcher

	mu       sync.Mutex
	sessions map[string]*shellSession
	events   chan Event
}

type shellSession struct {
	process Process
}

func NewManager(logger *slog.Logger, launcher Launcher) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	if launcher == nil {
		launcher = DefaultLauncher{}
	}

	return &Manager{
		logger:   logger.With("component", "worker-terminal-manager"),
		launcher: launcher,
		sessions: make(map[string]*shellSession),
		events:   make(chan Event, 256),
	}
}

func (m *Manager) Events() <-chan Event {
	return m.events
}

func (m *Manager) Open(ctx context.Context, sessionID string, cols uint32, rows uint32) error {
	if sessionID == "" {
		return errors.New("session id is required")
	}

	process, err := m.launcher.Launch(ctx, LaunchOptions{
		Cols: normalizeDimension(cols, 120),
		Rows: normalizeDimension(rows, 32),
	})
	if err != nil {
		return fmt.Errorf("launch terminal shell: %w", err)
	}

	m.mu.Lock()
	if _, exists := m.sessions[sessionID]; exists {
		m.mu.Unlock()
		_ = process.Close()
		return fmt.Errorf("terminal session %q already exists", sessionID)
	}
	m.sessions[sessionID] = &shellSession{process: process}
	m.mu.Unlock()

	m.emit(Event{
		Type:      EventTypeReady,
		SessionID: sessionID,
	})
	go m.forwardOutput(sessionID, process)
	go m.waitForExit(sessionID, process)

	return nil
}

func (m *Manager) Input(sessionID string, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	session, err := m.getSession(sessionID)
	if err != nil {
		return err
	}

	if _, err := session.process.Write(data); err != nil {
		return fmt.Errorf("write terminal input: %w", err)
	}
	return nil
}

func (m *Manager) Resize(sessionID string, cols uint32, rows uint32) error {
	session, err := m.getSession(sessionID)
	if err != nil {
		return err
	}

	if err := session.process.Resize(normalizeDimension(cols, 120), normalizeDimension(rows, 32)); err != nil {
		return fmt.Errorf("resize terminal: %w", err)
	}
	return nil
}

func (m *Manager) Close(ctx context.Context, sessionID string, _ string) error {
	session, err := m.takeSession(sessionID)
	if err != nil {
		return err
	}
	if err := session.process.Terminate(ctx); err != nil {
		return fmt.Errorf("terminate terminal: %w", err)
	}
	return nil
}

func (m *Manager) CloseAll(ctx context.Context) {
	m.mu.Lock()
	sessions := make(map[string]*shellSession, len(m.sessions))
	for sessionID, session := range m.sessions {
		sessions[sessionID] = session
	}
	m.sessions = make(map[string]*shellSession)
	m.mu.Unlock()

	for sessionID, session := range sessions {
		if err := session.process.Terminate(ctx); err != nil && !errors.Is(err, context.Canceled) {
			m.logger.WarnContext(ctx, "terminate terminal session", slog.String("session_id", sessionID), slog.String("error", err.Error()))
		}
	}
}

func (m *Manager) forwardOutput(sessionID string, process Process) {
	buffer := make([]byte, 16*1024)
	for {
		n, err := process.Read(buffer)
		if n > 0 {
			if !m.sessionMatches(sessionID, process) {
				return
			}
			data := append([]byte(nil), buffer[:n]...)
			m.emit(Event{
				Type:      EventTypeOutput,
				SessionID: sessionID,
				Data:      data,
			})
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			return
		}
		if !m.sessionMatches(sessionID, process) {
			return
		}

		m.emit(Event{
			Type:      EventTypeError,
			SessionID: sessionID,
			Message:   fmt.Sprintf("read terminal output: %v", err),
		})
		return
	}
}

func (m *Manager) waitForExit(sessionID string, process Process) {
	err := process.Wait()
	exitCode, message := exitStatusFromError(err)
	if !m.removeSessionIfMatches(sessionID, process) {
		return
	}
	m.emit(Event{
		Type:      EventTypeExit,
		SessionID: sessionID,
		ExitCode:  exitCode,
		Message:   message,
	})
}

func (m *Manager) getSession(sessionID string) (*shellSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session := m.sessions[sessionID]
	if session == nil {
		return nil, fmt.Errorf("terminal session %q was not found", sessionID)
	}
	return session, nil
}

func (m *Manager) takeSession(sessionID string) (*shellSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session := m.sessions[sessionID]
	if session == nil {
		return nil, fmt.Errorf("terminal session %q was not found", sessionID)
	}
	delete(m.sessions, sessionID)
	return session, nil
}

func (m *Manager) removeSessionIfMatches(sessionID string, process Process) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	session := m.sessions[sessionID]
	if session == nil || session.process != process {
		return false
	}
	delete(m.sessions, sessionID)
	return true
}

func (m *Manager) sessionMatches(sessionID string, process Process) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	session := m.sessions[sessionID]
	return session != nil && session.process == process
}

func (m *Manager) emit(event Event) {
	select {
	case m.events <- event:
	default:
		m.logger.Warn("dropping terminal event", slog.String("type", string(event.Type)), slog.String("session_id", event.SessionID))
	}
}

func normalizeDimension(value uint32, fallback uint16) uint16 {
	if value == 0 {
		return fallback
	}
	if value > 65535 {
		return 65535
	}
	return uint16(value)
}

func exitStatusFromError(err error) (int32, string) {
	if err == nil {
		return 0, ""
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return int32(exitErr.ExitCode()), err.Error()
	}
	return -1, err.Error()
}

type DefaultLauncher struct{}

func (DefaultLauncher) Launch(_ context.Context, options LaunchOptions) (Process, error) {
	shell, args, err := resolveShellCommand()
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(shell, args...)
	if homeDir, err := os.UserHomeDir(); err == nil && homeDir != "" {
		cmd.Dir = homeDir
	}

	ptyFile, err := pty.StartWithSize(cmd, &pty.Winsize{
		Cols: options.Cols,
		Rows: options.Rows,
	})
	if err != nil {
		return nil, fmt.Errorf("start pty shell: %w", err)
	}

	return &ptyProcess{
		cmd:     cmd,
		ptyFile: ptyFile,
	}, nil
}

type ptyProcess struct {
	cmd     *exec.Cmd
	ptyFile *os.File

	closeOnce sync.Once
	closeErr  error
}

func (p *ptyProcess) Read(data []byte) (int, error) {
	return p.ptyFile.Read(data)
}

func (p *ptyProcess) Write(data []byte) (int, error) {
	return p.ptyFile.Write(data)
}

func (p *ptyProcess) Resize(cols uint16, rows uint16) error {
	return pty.Setsize(p.ptyFile, &pty.Winsize{
		Cols: cols,
		Rows: rows,
	})
}

func (p *ptyProcess) Wait() error {
	return p.cmd.Wait()
}

func (p *ptyProcess) Terminate(_ context.Context) error {
	if p.cmd == nil || p.cmd.Process == nil {
		return p.Close()
	}
	if p.cmd.ProcessState != nil && p.cmd.ProcessState.Exited() {
		return p.Close()
	}

	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return p.Close()
}

func (p *ptyProcess) Close() error {
	p.closeOnce.Do(func() {
		if p.ptyFile != nil {
			p.closeErr = p.ptyFile.Close()
		}
	})
	return p.closeErr
}

func resolveShellCommand() (string, []string, error) {
	candidates := []string{"/bin/bash", "/bin/sh"}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, []string{"-l"}, nil
		}
	}

	for _, candidate := range []string{"bash", "sh"} {
		resolved, err := exec.LookPath(candidate)
		if err != nil {
			continue
		}
		return resolved, []string{"-l"}, nil
	}

	return "", nil, errors.New("no interactive shell was found")
}
