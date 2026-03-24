package terminal

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func TestManagerOpenInputResizeAndExit(t *testing.T) {
	process := &fakeProcess{
		outputCh: make(chan []byte, 1),
		waitCh:   make(chan error, 1),
	}
	manager := NewManager(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		fakeLauncher{process: process},
	)

	if err := manager.Open(context.Background(), "session-1", 120, 40); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	event := awaitEvent(t, manager.Events())
	if event.Type != EventTypeReady || event.SessionID != "session-1" {
		t.Fatalf("ready event = %+v, want ready for session-1", event)
	}

	if err := manager.Input("session-1", []byte("ls\n")); err != nil {
		t.Fatalf("Input() error = %v", err)
	}
	if got := string(process.input); got != "ls\n" {
		t.Fatalf("process input = %q, want %q", got, "ls\n")
	}

	if err := manager.Resize("session-1", 132, 48); err != nil {
		t.Fatalf("Resize() error = %v", err)
	}
	if process.cols != 132 || process.rows != 48 {
		t.Fatalf("process resize = %dx%d, want 132x48", process.cols, process.rows)
	}

	process.outputCh <- []byte("hello")
	event = awaitEvent(t, manager.Events())
	if event.Type != EventTypeOutput || string(event.Data) != "hello" {
		t.Fatalf("output event = %+v, want hello output", event)
	}

	close(process.outputCh)
	process.waitCh <- nil
	event = awaitEvent(t, manager.Events())
	if event.Type != EventTypeExit || event.ExitCode != 0 {
		t.Fatalf("exit event = %+v, want exit code 0", event)
	}
}

func awaitEvent(t *testing.T, events <-chan Event) Event {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for terminal event")
		return Event{}
	}
}

type fakeLauncher struct {
	process Process
}

func (f fakeLauncher) Launch(context.Context, LaunchOptions) (Process, error) {
	return f.process, nil
}

type fakeProcess struct {
	outputCh chan []byte
	waitCh   chan error

	mu         sync.Mutex
	input      []byte
	cols       uint16
	rows       uint16
	terminated bool
}

func (f *fakeProcess) Read(data []byte) (int, error) {
	chunk, ok := <-f.outputCh
	if !ok {
		return 0, io.EOF
	}
	copy(data, chunk)
	return len(chunk), nil
}

func (f *fakeProcess) Write(data []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.input = append(f.input, data...)
	return len(data), nil
}

func (f *fakeProcess) Resize(cols uint16, rows uint16) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.cols = cols
	f.rows = rows
	return nil
}

func (f *fakeProcess) Wait() error {
	return <-f.waitCh
}

func (f *fakeProcess) Terminate(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.terminated = true
	return nil
}

func (*fakeProcess) Close() error {
	return nil
}
