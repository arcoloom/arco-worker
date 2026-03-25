package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	workerv1 "github.com/arcoloom/arco-proto/gen/go/arcoloom/worker/v1"
	workerTerminal "github.com/arcoloom/arco-worker/internal/worker/terminal"
)

const defaultTerminalReconnectInterval = 2 * time.Second

type TerminalAgentConfig struct {
	InstanceID           string
	Provider             string
	RegistrationToken    string
	WorkerVersion        string
	TerminalSessionToken string
	ReconnectInterval    time.Duration
}

type TerminalAgent struct {
	logger  *slog.Logger
	client  ControlPlaneClient
	config  TerminalAgentConfig
	manager *workerTerminal.Manager
}

func NewTerminalAgent(
	logger *slog.Logger,
	client ControlPlaneClient,
	manager *workerTerminal.Manager,
	config TerminalAgentConfig,
) (*TerminalAgent, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if client == nil {
		return nil, errors.New("control-plane client is required")
	}
	if manager == nil {
		manager = workerTerminal.NewManager(logger, nil)
	}
	if config.InstanceID == "" {
		return nil, errors.New("instance id is required")
	}
	if config.Provider == "" {
		return nil, errors.New("provider is required")
	}
	if config.RegistrationToken == "" {
		return nil, errors.New("registration token is required")
	}
	if config.WorkerVersion == "" {
		config.WorkerVersion = "dev"
	}
	if config.TerminalSessionToken == "" {
		return nil, errors.New("terminal session token is required")
	}
	if config.ReconnectInterval <= 0 {
		config.ReconnectInterval = defaultTerminalReconnectInterval
	}

	return &TerminalAgent{
		logger:  logger.With("component", "worker-terminal-agent"),
		client:  client,
		config:  config,
		manager: manager,
	}, nil
}

func (a *TerminalAgent) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		if err := a.runOnce(ctx); err != nil && ctx.Err() == nil {
			a.logger.WarnContext(ctx, "terminal stream ended", slog.String("error", err.Error()))
		}
		a.manager.CloseAll(context.Background())

		if err := ctx.Err(); err != nil {
			return nil
		}

		timer := time.NewTimer(a.config.ReconnectInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func (a *TerminalAgent) runOnce(ctx context.Context) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	session, err := a.client.ConnectTerminal(streamCtx)
	if err != nil {
		return fmt.Errorf("connect terminal stream: %w", err)
	}
	defer session.CloseSend()

	if err := session.Send(streamCtx, &workerv1.WorkerTerminalToControl{
		Message: &workerv1.WorkerTerminalToControl_Hello{
			Hello: &workerv1.TerminalHello{
				InstanceId:           a.config.InstanceID,
				Provider:             a.config.Provider,
				RegistrationToken:    a.config.RegistrationToken,
				WorkerVersion:        a.config.WorkerVersion,
				TerminalSessionToken: a.config.TerminalSessionToken,
			},
		},
	}); err != nil {
		return fmt.Errorf("send terminal hello: %w", err)
	}

	errCh := make(chan error, 2)
	go func() {
		errCh <- a.forwardManagerEvents(streamCtx, session)
	}()
	go func() {
		errCh <- a.receiveControlMessages(streamCtx, session)
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if err == nil {
			return nil
		}
		return err
	}
}

func (a *TerminalAgent) forwardManagerEvents(ctx context.Context, session TerminalControlPlaneSession) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-a.manager.Events():
			if err := a.sendEvent(ctx, session, event); err != nil {
				return err
			}
		}
	}
}

func (a *TerminalAgent) sendEvent(ctx context.Context, session TerminalControlPlaneSession, event workerTerminal.Event) error {
	switch event.Type {
	case workerTerminal.EventTypeReady:
		return session.Send(ctx, &workerv1.WorkerTerminalToControl{
			Message: &workerv1.WorkerTerminalToControl_Ready{
				Ready: &workerv1.TerminalReady{SessionId: event.SessionID},
			},
		})
	case workerTerminal.EventTypeOutput:
		return session.Send(ctx, &workerv1.WorkerTerminalToControl{
			Message: &workerv1.WorkerTerminalToControl_Output{
				Output: &workerv1.TerminalOutput{
					SessionId: event.SessionID,
					Data:      event.Data,
				},
			},
		})
	case workerTerminal.EventTypeExit:
		return session.Send(ctx, &workerv1.WorkerTerminalToControl{
			Message: &workerv1.WorkerTerminalToControl_Exit{
				Exit: &workerv1.TerminalExit{
					SessionId: event.SessionID,
					ExitCode:  event.ExitCode,
					Message:   event.Message,
				},
			},
		})
	case workerTerminal.EventTypeError:
		return session.Send(ctx, &workerv1.WorkerTerminalToControl{
			Message: &workerv1.WorkerTerminalToControl_Error{
				Error: &workerv1.TerminalError{
					SessionId: event.SessionID,
					Message:   event.Message,
				},
			},
		})
	default:
		return nil
	}
}

func (a *TerminalAgent) receiveControlMessages(ctx context.Context, session TerminalControlPlaneSession) error {
	for {
		message, err := session.Receive(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("receive terminal control message: %w", err)
		}

		switch payload := message.GetMessage().(type) {
		case *workerv1.ControlToWorkerTerminal_HelloAck:
			a.logger.InfoContext(ctx, "terminal stream connected", slog.String("worker_id", payload.HelloAck.GetWorkerId()))
		case *workerv1.ControlToWorkerTerminal_OpenShell:
			if err := a.manager.Open(ctx, payload.OpenShell.GetSessionId(), payload.OpenShell.GetCols(), payload.OpenShell.GetRows()); err != nil {
				if sendErr := a.sendEvent(ctx, session, workerTerminal.Event{
					Type:      workerTerminal.EventTypeError,
					SessionID: payload.OpenShell.GetSessionId(),
					Message:   err.Error(),
				}); sendErr != nil {
					return sendErr
				}
			}
		case *workerv1.ControlToWorkerTerminal_Input:
			if err := a.manager.Input(payload.Input.GetSessionId(), payload.Input.GetData()); err != nil {
				if sendErr := a.sendEvent(ctx, session, workerTerminal.Event{
					Type:      workerTerminal.EventTypeError,
					SessionID: payload.Input.GetSessionId(),
					Message:   err.Error(),
				}); sendErr != nil {
					return sendErr
				}
			}
		case *workerv1.ControlToWorkerTerminal_Resize:
			if err := a.manager.Resize(payload.Resize.GetSessionId(), payload.Resize.GetCols(), payload.Resize.GetRows()); err != nil {
				if sendErr := a.sendEvent(ctx, session, workerTerminal.Event{
					Type:      workerTerminal.EventTypeError,
					SessionID: payload.Resize.GetSessionId(),
					Message:   err.Error(),
				}); sendErr != nil {
					return sendErr
				}
			}
		case *workerv1.ControlToWorkerTerminal_Close:
			if err := a.manager.Close(context.Background(), payload.Close.GetSessionId(), payload.Close.GetReason()); err != nil {
				if sendErr := a.sendEvent(ctx, session, workerTerminal.Event{
					Type:      workerTerminal.EventTypeError,
					SessionID: payload.Close.GetSessionId(),
					Message:   err.Error(),
				}); sendErr != nil {
					return sendErr
				}
			}
		default:
			return errors.New("received unsupported terminal control message")
		}
	}
}
