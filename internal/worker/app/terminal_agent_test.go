package app

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	workerv1 "github.com/arcoloom/arco-proto/gen/go/arcoloom/worker/v1"
	workerTerminal "github.com/arcoloom/arco-worker/internal/worker/terminal"
)

func TestTerminalAgentRunOnceHandlesControlMessages(t *testing.T) {
	process := &agentFakeProcess{
		outputCh: make(chan []byte),
		waitCh:   make(chan error, 1),
	}
	manager := workerTerminal.NewManager(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		agentFakeLauncher{process: process},
	)
	session := &recordingTerminalSession{
		receiveMessages: []*workerv1.ControlToWorkerTerminal{
			{
				Message: &workerv1.ControlToWorkerTerminal_HelloAck{
					HelloAck: &workerv1.TerminalHelloAck{WorkerId: "worker-1"},
				},
			},
			{
				Message: &workerv1.ControlToWorkerTerminal_OpenShell{
					OpenShell: &workerv1.TerminalOpenShell{
						SessionId: "session-1",
						Cols:      100,
						Rows:      30,
					},
				},
			},
			{
				Message: &workerv1.ControlToWorkerTerminal_Input{
					Input: &workerv1.TerminalInput{
						SessionId: "session-1",
						Data:      []byte("pwd\n"),
					},
				},
			},
			{
				Message: &workerv1.ControlToWorkerTerminal_Resize{
					Resize: &workerv1.TerminalResize{
						SessionId: "session-1",
						Cols:      120,
						Rows:      40,
					},
				},
			},
			{
				Message: &workerv1.ControlToWorkerTerminal_Close{
					Close: &workerv1.TerminalClose{
						SessionId: "session-1",
						Reason:    "done",
					},
				},
			},
		},
		readySent: make(chan struct{}, 1),
	}

	agent, err := NewTerminalAgent(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		fakeTerminalClient{session: session},
		manager,
		TerminalAgentConfig{
			InstanceID:           "instance-1",
			Provider:             "aws",
			RegistrationToken:    "token-1",
			WorkerVersion:        "test",
			TerminalSessionToken: "terminal-token-1",
		},
	)
	if err != nil {
		t.Fatalf("NewTerminalAgent() error = %v", err)
	}

	if err := agent.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() error = %v", err)
	}

	if len(session.sentMessages) < 2 {
		t.Fatalf("sentMessages = %d, want hello + ready", len(session.sentMessages))
	}
	if session.sentMessages[0].GetHello() == nil {
		t.Fatalf("first sent message = %#v, want hello", session.sentMessages[0].GetMessage())
	}
	if got := session.sentMessages[0].GetHello().GetTerminalSessionToken(); got != "terminal-token-1" {
		t.Fatalf("hello terminal session token = %q, want %q", got, "terminal-token-1")
	}
	if session.sentMessages[1].GetReady() == nil || session.sentMessages[1].GetReady().GetSessionId() != "session-1" {
		t.Fatalf("second sent message = %#v, want ready for session-1", session.sentMessages[1].GetMessage())
	}

	if got := string(process.input); got != "pwd\n" {
		t.Fatalf("process input = %q, want %q", got, "pwd\n")
	}
	if process.cols != 120 || process.rows != 40 {
		t.Fatalf("process resize = %dx%d, want 120x40", process.cols, process.rows)
	}
	if !process.terminated {
		t.Fatal("expected process to be terminated")
	}
}

type fakeTerminalClient struct {
	session TerminalControlPlaneSession
}

func (fakeTerminalClient) Connect(context.Context) (ControlPlaneSession, error) {
	return nil, nil
}

func (f fakeTerminalClient) ConnectTerminal(context.Context) (TerminalControlPlaneSession, error) {
	return f.session, nil
}

type recordingTerminalSession struct {
	mu sync.Mutex

	receiveMessages []*workerv1.ControlToWorkerTerminal
	receiveIndex    int
	sentMessages    []*workerv1.WorkerTerminalToControl
	readySent       chan struct{}
}

func (s *recordingTerminalSession) Send(_ context.Context, message *workerv1.WorkerTerminalToControl) error {
	s.mu.Lock()
	s.sentMessages = append(s.sentMessages, message)
	s.mu.Unlock()
	if message.GetReady() != nil {
		select {
		case s.readySent <- struct{}{}:
		default:
		}
	}
	return nil
}

func (s *recordingTerminalSession) Receive(context.Context) (*workerv1.ControlToWorkerTerminal, error) {
	s.mu.Lock()
	if s.receiveIndex < len(s.receiveMessages) {
		message := s.receiveMessages[s.receiveIndex]
		s.receiveIndex++
		s.mu.Unlock()
		if message.GetInput() != nil {
			<-s.readySent
		}
		return message, nil
	}
	s.mu.Unlock()
	return nil, io.EOF
}

func (*recordingTerminalSession) CloseSend() error {
	return nil
}

type agentFakeLauncher struct {
	process workerTerminal.Process
}

func (f agentFakeLauncher) Launch(context.Context, workerTerminal.LaunchOptions) (workerTerminal.Process, error) {
	return f.process, nil
}

type agentFakeProcess struct {
	outputCh chan []byte
	waitCh   chan error

	mu         sync.Mutex
	input      []byte
	cols       uint16
	rows       uint16
	terminated bool
}

func (f *agentFakeProcess) Read(data []byte) (int, error) {
	chunk, ok := <-f.outputCh
	if !ok {
		return 0, io.EOF
	}
	copy(data, chunk)
	return len(chunk), nil
}

func (f *agentFakeProcess) Write(data []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.input = append(f.input, data...)
	return len(data), nil
}

func (f *agentFakeProcess) Resize(cols uint16, rows uint16) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.cols = cols
	f.rows = rows
	return nil
}

func (f *agentFakeProcess) Wait() error {
	return <-f.waitCh
}

func (f *agentFakeProcess) Terminate(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.terminated {
		f.terminated = true
		close(f.outputCh)
		f.waitCh <- nil
	}
	return nil
}

func (*agentFakeProcess) Close() error {
	return nil
}
