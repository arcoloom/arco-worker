package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	workerv1 "github.com/arcoloom/arco-proto/gen/go/arcoloom/worker/v1"
	workerRuntime "github.com/arcoloom/arco-worker/internal/worker/runtime"
	workerShutdown "github.com/arcoloom/arco-worker/internal/worker/shutdown"
)

type fakeControlPlaneClient struct {
	connectCtx      context.Context
	session         ControlPlaneSession
	terminalSession TerminalControlPlaneSession
}

func (c *fakeControlPlaneClient) Connect(ctx context.Context) (ControlPlaneSession, error) {
	c.connectCtx = ctx
	return c.session, nil
}

func (c *fakeControlPlaneClient) ConnectTerminal(context.Context) (TerminalControlPlaneSession, error) {
	if c.terminalSession != nil {
		return c.terminalSession, nil
	}
	return &scriptedTerminalSession{}, nil
}

type scriptedSession struct {
	messages []*workerv1.ControlToWorker
	index    int
	receive  func(context.Context) (*workerv1.ControlToWorker, error)
	mu       sync.Mutex
	sent     []*workerv1.WorkerToControl
}

func (s *scriptedSession) Send(_ context.Context, message *workerv1.WorkerToControl) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, message)
	return nil
}

func (s *scriptedSession) Receive(ctx context.Context) (*workerv1.ControlToWorker, error) {
	if s.receive != nil {
		return s.receive(ctx)
	}
	if s.index >= len(s.messages) {
		return nil, io.EOF
	}
	message := s.messages[s.index]
	s.index++
	return message, nil
}

func (s *scriptedSession) CloseSend() error {
	return nil
}

type scriptedTerminalSession struct{}

func (s *scriptedTerminalSession) Send(context.Context, *workerv1.WorkerTerminalToControl) error {
	return nil
}

func (s *scriptedTerminalSession) Receive(context.Context) (*workerv1.ControlToWorkerTerminal, error) {
	return nil, io.EOF
}

func (s *scriptedTerminalSession) CloseSend() error {
	return nil
}

type fakeEngine struct{}

func (fakeEngine) Prepare(context.Context, []byte) error { return nil }
func (fakeEngine) Start(context.Context, []byte) error   { return nil }
func (fakeEngine) Interrupt(context.Context) error       { return nil }
func (fakeEngine) Stop(context.Context) error            { return nil }
func (fakeEngine) Wait(context.Context) error            { return nil }

type blockingEngine struct {
	waitCh chan struct{}
}

func (e blockingEngine) Prepare(context.Context, []byte) error { return nil }
func (e blockingEngine) Start(context.Context, []byte) error   { return nil }
func (e blockingEngine) Interrupt(context.Context) error       { return nil }
func (e blockingEngine) Stop(context.Context) error            { return nil }
func (e blockingEngine) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.waitCh:
		return nil
	}
}

type fakeShutdownMonitor struct {
	run func(context.Context) error
}

func (m fakeShutdownMonitor) Run(ctx context.Context) error {
	if m.run != nil {
		return m.run(ctx)
	}
	return nil
}

func TestRunnerUsesUndeadlinedStreamContext(t *testing.T) {
	session := &scriptedSession{
		messages: []*workerv1.ControlToWorker{
			{
				Message: &workerv1.ControlToWorker_HelloAck{
					HelloAck: &workerv1.HelloAck{
						WorkerId:             "worker-1",
						TerminalSessionToken: "terminal-token-1",
					},
				},
			},
			{
				Message: &workerv1.ControlToWorker_Assignment{
					Assignment: &workerv1.Assignment{
						TaskId:      "task-1",
						RuntimeKind: workerv1.RuntimeKind_RUNTIME_KIND_EXEC,
						Payload:     `{"command":"true"}`,
					},
				},
			},
		},
	}

	client := &fakeControlPlaneClient{session: session}
	runner, err := NewRunner(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		client,
		func(context.Context, workerv1.RuntimeKind) (workerRuntime.Engine, error) { return fakeEngine{}, nil },
		RunnerConfig{
			InstanceID:        "instance-1",
			Provider:          "aws",
			RegistrationToken: "token-1",
			WorkerVersion:     "test",
			ConnectTimeout:    20 * time.Millisecond,
			HeartbeatInterval: time.Hour,
		},
	)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if client.connectCtx == nil {
		t.Fatal("Connect() was not called")
	}
	if _, ok := client.connectCtx.Deadline(); ok {
		t.Fatal("stream context unexpectedly carries the handshake deadline")
	}
}

func TestRunnerStoresWorkerMetadataFromHelloAck(t *testing.T) {
	t.Parallel()

	state := NewLocalState(LocalStateConfig{
		APIAddress:        DefaultLocalAPIListenAddress,
		InstanceID:        "instance-1",
		Provider:          "bootstrap-provider",
		WorkerVersion:     "test",
		ConnectTimeout:    time.Second,
		HeartbeatInterval: time.Hour,
	})
	session := &scriptedSession{
		messages: []*workerv1.ControlToWorker{
			{
				Message: &workerv1.ControlToWorker_HelloAck{
					HelloAck: &workerv1.HelloAck{
						WorkerId:             "worker-1",
						TerminalSessionToken: "terminal-token-1",
						CloudVendor:          "aws",
						InstanceType:         "c7g.large",
						Region:               "us-west-2",
						AvailabilityZone:     "us-west-2a",
					},
				},
			},
			{
				Message: &workerv1.ControlToWorker_Assignment{
					Assignment: &workerv1.Assignment{
						TaskId:      "task-1",
						RuntimeKind: workerv1.RuntimeKind_RUNTIME_KIND_EXEC,
						Payload:     `{"command":"true"}`,
					},
				},
			},
		},
	}

	client := &fakeControlPlaneClient{session: session}
	runner, err := NewRunner(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		client,
		func(context.Context, workerv1.RuntimeKind) (workerRuntime.Engine, error) { return fakeEngine{}, nil },
		RunnerConfig{
			InstanceID:        "instance-1",
			Provider:          "bootstrap-provider",
			RegistrationToken: "token-1",
			WorkerVersion:     "test",
			ConnectTimeout:    time.Second,
			HeartbeatInterval: time.Hour,
			LocalState:        state,
		},
	)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	snapshot := state.Snapshot()
	if snapshot.Worker.Provider != "bootstrap-provider" {
		t.Fatalf("provider = %q, want %q", snapshot.Worker.Provider, "bootstrap-provider")
	}
	if snapshot.Worker.CloudVendor != "aws" {
		t.Fatalf("cloud_vendor = %q, want %q", snapshot.Worker.CloudVendor, "aws")
	}
	if snapshot.Worker.InstanceType != "c7g.large" {
		t.Fatalf("instance_type = %q, want %q", snapshot.Worker.InstanceType, "c7g.large")
	}
	if snapshot.Worker.Region != "us-west-2" {
		t.Fatalf("region = %q, want %q", snapshot.Worker.Region, "us-west-2")
	}
	if snapshot.Worker.AvailabilityZone != "us-west-2a" {
		t.Fatalf("availability_zone = %q, want %q", snapshot.Worker.AvailabilityZone, "us-west-2a")
	}
	if snapshot.Worker.AZ != "us-west-2a" {
		t.Fatalf("az = %q, want %q", snapshot.Worker.AZ, "us-west-2a")
	}
}

func TestRunnerTimesOutDuringHandshake(t *testing.T) {
	session := &scriptedSession{
		receive: func(ctx context.Context) (*workerv1.ControlToWorker, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	client := &fakeControlPlaneClient{session: session}
	runner, err := NewRunner(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		client,
		func(context.Context, workerv1.RuntimeKind) (workerRuntime.Engine, error) { return fakeEngine{}, nil },
		RunnerConfig{
			InstanceID:        "instance-1",
			Provider:          "aws",
			RegistrationToken: "token-1",
			WorkerVersion:     "test",
			ConnectTimeout:    10 * time.Millisecond,
			HeartbeatInterval: time.Hour,
		},
	)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	err = runner.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want handshake timeout")
	}
	if !strings.Contains(err.Error(), "timed out waiting for the control plane assignment") && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want handshake timeout", err)
	}
}

func TestRunnerExitsCleanlyOnPreAssignmentShutdown(t *testing.T) {
	session := &scriptedSession{
		messages: []*workerv1.ControlToWorker{
			{
				Message: &workerv1.ControlToWorker_Shutdown{
					Shutdown: &workerv1.Shutdown{
						Reason: "instance terminated before the worker connected",
					},
				},
			},
		},
	}

	client := &fakeControlPlaneClient{session: session}
	runner, err := NewRunner(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		client,
		func(context.Context, workerv1.RuntimeKind) (workerRuntime.Engine, error) { return fakeEngine{}, nil },
		RunnerConfig{
			InstanceID:        "instance-1",
			Provider:          "aws",
			RegistrationToken: "token-1",
			WorkerVersion:     "test",
			ConnectTimeout:    time.Second,
			HeartbeatInterval: time.Hour,
		},
	)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
}

func TestRunnerStartsShutdownMonitorWhenEnabled(t *testing.T) {
	engineDone := make(chan struct{})
	session := &scriptedSession{
		messages: []*workerv1.ControlToWorker{
			{
				Message: &workerv1.ControlToWorker_HelloAck{
					HelloAck: &workerv1.HelloAck{
						WorkerId:             "worker-1",
						TerminalSessionToken: "terminal-token-1",
					},
				},
			},
			{
				Message: &workerv1.ControlToWorker_Assignment{
					Assignment: &workerv1.Assignment{
						TaskId:      "task-1",
						RuntimeKind: workerv1.RuntimeKind_RUNTIME_KIND_EXEC,
						Payload: `{
							"command": "true",
							"shutdown_monitor": {
								"enabled": true,
								"provider": "aws"
							}
						}`,
					},
				},
			},
		},
	}

	client := &fakeControlPlaneClient{session: session}
	runner, err := NewRunner(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		client,
		func(context.Context, workerv1.RuntimeKind) (workerRuntime.Engine, error) {
			return blockingEngine{waitCh: engineDone}, nil
		},
		RunnerConfig{
			InstanceID:        "instance-1",
			Provider:          "aws",
			RegistrationToken: "token-1",
			WorkerVersion:     "test",
			ConnectTimeout:    time.Second,
			HeartbeatInterval: time.Hour,
			ShutdownMonitor: func(_ *slog.Logger, reporter workerShutdown.Reporter, _ workerShutdown.MonitorConfig) ShutdownMonitor {
				return fakeShutdownMonitor{
					run: func(ctx context.Context) error {
						err := reporter.ReportNotice(ctx, workerShutdown.Notice{
							Provider:   "aws",
							Detail:     "aws spot interruption notice: action=terminate",
							ShutdownAt: time.Date(2026, 3, 26, 10, 0, 0, 0, time.UTC),
						})
						close(engineDone)
						return err
					},
				}
			},
		},
	)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	var found bool
	for _, message := range session.sent {
		signal := message.GetSignal()
		if signal == nil {
			continue
		}
		found = true
		if signal.GetTaskId() != "task-1" {
			t.Fatalf("signal task_id = %q, want task-1", signal.GetTaskId())
		}
		if signal.GetShutdownAt() == nil || signal.GetShutdownAt().AsTime().UTC().Format(time.RFC3339) != "2026-03-26T10:00:00Z" {
			t.Fatalf("signal shutdown_at = %v, want 2026-03-26T10:00:00Z", signal.GetShutdownAt())
		}
	}
	if !found {
		t.Fatal("expected runner to send a shutdown signal")
	}
}

func TestRunnerSkipsShutdownMonitorWhenDisabled(t *testing.T) {
	session := &scriptedSession{
		messages: []*workerv1.ControlToWorker{
			{
				Message: &workerv1.ControlToWorker_HelloAck{
					HelloAck: &workerv1.HelloAck{
						WorkerId:             "worker-1",
						TerminalSessionToken: "terminal-token-1",
					},
				},
			},
			{
				Message: &workerv1.ControlToWorker_Assignment{
					Assignment: &workerv1.Assignment{
						TaskId:      "task-1",
						RuntimeKind: workerv1.RuntimeKind_RUNTIME_KIND_EXEC,
						Payload: `{
							"command": "true",
							"shutdown_monitor": {
								"enabled": false,
								"provider": "aws"
							}
						}`,
					},
				},
			},
		},
	}

	client := &fakeControlPlaneClient{session: session}
	monitorStarted := false
	runner, err := NewRunner(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		client,
		func(context.Context, workerv1.RuntimeKind) (workerRuntime.Engine, error) { return fakeEngine{}, nil },
		RunnerConfig{
			InstanceID:        "instance-1",
			Provider:          "aws",
			RegistrationToken: "token-1",
			WorkerVersion:     "test",
			ConnectTimeout:    time.Second,
			HeartbeatInterval: time.Hour,
			ShutdownMonitor: func(_ *slog.Logger, reporter workerShutdown.Reporter, _ workerShutdown.MonitorConfig) ShutdownMonitor {
				_ = reporter
				monitorStarted = true
				return fakeShutdownMonitor{}
			},
		},
	)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if monitorStarted {
		t.Fatal("expected shutdown monitor to stay disabled")
	}
}
