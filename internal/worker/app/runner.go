package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	workerv1 "github.com/arcoloom/arco-proto/gen/go/arcoloom/worker/v1"
	workerRuntime "github.com/arcoloom/arco-worker/internal/worker/runtime"
)

type EngineFactory func(ctx context.Context, kind workerv1.RuntimeKind) (workerRuntime.Engine, error)

type RunnerConfig struct {
	InstanceID        string
	Provider          string
	RegistrationToken string
	WorkerVersion     string
	ConnectTimeout    time.Duration
	HeartbeatInterval time.Duration
	StopTimeout       time.Duration
}

type Runner struct {
	logger        *slog.Logger
	client        ControlPlaneClient
	engineFactory EngineFactory
	config        RunnerConfig
}

func NewRunner(logger *slog.Logger, client ControlPlaneClient, engineFactory EngineFactory, config RunnerConfig) (*Runner, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if client == nil {
		return nil, errors.New("control-plane client is required")
	}
	if engineFactory == nil {
		return nil, errors.New("engine factory is required")
	}
	if config.InstanceID == "" {
		return nil, errors.New("instance ID is required")
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
	if config.ConnectTimeout <= 0 {
		config.ConnectTimeout = 15 * time.Second
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 5 * time.Second
	}
	if config.StopTimeout <= 0 {
		config.StopTimeout = 15 * time.Second
	}

	return &Runner{
		logger:        logger,
		client:        client,
		engineFactory: engineFactory,
		config:        config,
	}, nil
}

func (r *Runner) Run(ctx context.Context) error {
	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()

	session, err := r.client.Connect(streamCtx)
	if err != nil {
		return fmt.Errorf("connect worker stream: %w", err)
	}
	defer session.CloseSend()

	type handshakeResult struct {
		workerID   string
		assignment *workerv1.Assignment
		err        error
	}
	handshakeCh := make(chan handshakeResult, 1)
	go func() {
		if err := session.Send(streamCtx, &workerv1.WorkerToControl{
			Message: &workerv1.WorkerToControl_Hello{
				Hello: &workerv1.Hello{
					InstanceId:        r.config.InstanceID,
					Provider:          r.config.Provider,
					RegistrationToken: r.config.RegistrationToken,
					WorkerVersion:     r.config.WorkerVersion,
				},
			},
		}); err != nil {
			handshakeCh <- handshakeResult{err: fmt.Errorf("send hello: %w", err)}
			return
		}

		workerID, assignment, err := r.waitForAssignment(streamCtx, session)
		handshakeCh <- handshakeResult{
			workerID:   workerID,
			assignment: assignment,
			err:        err,
		}
	}()

	timer := time.NewTimer(r.config.ConnectTimeout)
	defer timer.Stop()

	var (
		workerID   string
		assignment *workerv1.Assignment
	)
	select {
	case result := <-handshakeCh:
		if result.err != nil {
			return result.err
		}
		workerID = result.workerID
		assignment = result.assignment
	case <-timer.C:
		stopStream()
		return errors.New("timed out waiting for the control plane assignment")
	case <-ctx.Done():
		stopStream()
		return ctx.Err()
	}

	if workerID == "" {
		return errors.New("control plane returned an empty worker ID")
	}
	if assignment == nil {
		return errors.New("control plane did not send an assignment")
	}

	heartbeatCtx, stopHeartbeats := context.WithCancel(context.WithoutCancel(ctx))
	defer stopHeartbeats()
	go r.runHeartbeats(heartbeatCtx, session, workerID)

	return r.runAssignment(ctx, session, assignment)
}

func (r *Runner) waitForAssignment(ctx context.Context, session ControlPlaneSession) (string, *workerv1.Assignment, error) {
	workerID := ""
	for {
		message, err := session.Receive(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return "", nil, errors.New("control plane closed the stream before assignment")
			}
			return "", nil, fmt.Errorf("receive control-plane message: %w", err)
		}

		switch payload := message.GetMessage().(type) {
		case *workerv1.ControlToWorker_HelloAck:
			workerID = payload.HelloAck.GetWorkerId()
		case *workerv1.ControlToWorker_Assignment:
			return workerID, payload.Assignment, nil
		case *workerv1.ControlToWorker_Shutdown:
			reason := payload.Shutdown.GetReason()
			if reason == "" {
				reason = "control plane requested shutdown before assignment"
			}
			return "", nil, errors.New(reason)
		default:
			return "", nil, errors.New("received unsupported control-plane message")
		}
	}
}

func (r *Runner) runAssignment(ctx context.Context, session ControlPlaneSession, assignment *workerv1.Assignment) error {
	taskID := assignment.GetTaskId()
	if taskID == "" {
		return errors.New("assignment returned an empty task ID")
	}

	engine, err := r.engineFactory(context.WithoutCancel(ctx), assignment.GetRuntimeKind())
	if err != nil {
		r.logStatusError(ctx, session, taskID, workerv1.TaskState_TASK_STATE_FAILED, err.Error())
		return fmt.Errorf("resolve runtime %s: %w", assignment.GetRuntimeKind().String(), err)
	}

	payload := []byte(assignment.GetPayload())
	if err := r.sendStatus(ctx, session, taskID, workerv1.TaskState_TASK_STATE_PREPARING, "preparing workload"); err != nil {
		return err
	}
	if err := engine.Prepare(ctx, payload); err != nil {
		r.logStatusError(ctx, session, taskID, workerv1.TaskState_TASK_STATE_FAILED, err.Error())
		return fmt.Errorf("prepare workload: %w", err)
	}
	if err := engine.Start(ctx, payload); err != nil {
		r.logStatusError(ctx, session, taskID, workerv1.TaskState_TASK_STATE_FAILED, err.Error())
		return fmt.Errorf("start workload: %w", err)
	}
	if err := r.sendStatus(ctx, session, taskID, workerv1.TaskState_TASK_STATE_RUNNING, "workload started"); err != nil {
		r.stopWorkload(context.Background(), taskID, engine)
		return err
	}

	var interrupted atomic.Bool
	go func() {
		<-ctx.Done()
		interrupted.Store(true)
		stopCtx, cancel := context.WithTimeout(context.Background(), r.config.StopTimeout)
		defer cancel()
		if stopErr := engine.Stop(stopCtx); stopErr != nil {
			r.logger.ErrorContext(stopCtx, "failed to stop workload", slog.String("task_id", taskID), slog.String("error", stopErr.Error()))
		}
	}()

	waitErr := engine.Wait(context.WithoutCancel(ctx))
	if waitErr == nil && interrupted.Load() && ctx.Err() != nil {
		waitErr = ctx.Err()
	}
	if waitErr != nil {
		r.logStatusError(ctx, session, taskID, workerv1.TaskState_TASK_STATE_FAILED, waitErr.Error())
		return fmt.Errorf("wait workload: %w", waitErr)
	}
	if err := r.sendStatus(ctx, session, taskID, workerv1.TaskState_TASK_STATE_SUCCESS, "workload finished successfully"); err != nil {
		return err
	}

	r.logger.InfoContext(
		context.WithoutCancel(ctx),
		"worker completed task",
		slog.String("task_id", taskID),
		slog.String("runtime_kind", assignment.GetRuntimeKind().String()),
	)
	return nil
}

func (r *Runner) sendStatus(ctx context.Context, session ControlPlaneSession, taskID string, state workerv1.TaskState, message string) error {
	if err := session.Send(context.WithoutCancel(ctx), &workerv1.WorkerToControl{
		Message: &workerv1.WorkerToControl_Status{
			Status: &workerv1.StatusUpdate{
				TaskId:  taskID,
				State:   state,
				Message: message,
			},
		},
	}); err != nil {
		return fmt.Errorf("send status %s for task %s: %w", state.String(), taskID, err)
	}

	r.logger.InfoContext(
		context.WithoutCancel(ctx),
		"reported task status",
		slog.String("task_id", taskID),
		slog.String("state", state.String()),
		slog.String("message", message),
	)
	return nil
}

func (r *Runner) logStatusError(ctx context.Context, session ControlPlaneSession, taskID string, state workerv1.TaskState, message string) {
	if err := r.sendStatus(ctx, session, taskID, state, message); err != nil {
		r.logger.ErrorContext(
			context.WithoutCancel(ctx),
			"failed to report task status",
			slog.String("task_id", taskID),
			slog.String("state", state.String()),
			slog.String("message", message),
			slog.String("error", err.Error()),
		)
	}
}

func (r *Runner) runHeartbeats(ctx context.Context, session ControlPlaneSession, workerID string) {
	ticker := time.NewTicker(r.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := session.Send(context.WithoutCancel(ctx), &workerv1.WorkerToControl{
				Message: &workerv1.WorkerToControl_Heartbeat{
					Heartbeat: &workerv1.Heartbeat{WorkerId: workerID},
				},
			}); err != nil {
				r.logger.WarnContext(context.WithoutCancel(ctx), "failed to send heartbeat", slog.String("worker_id", workerID), slog.String("error", err.Error()))
				return
			}
		}
	}
}

func (r *Runner) stopWorkload(ctx context.Context, taskID string, engine workerRuntime.Engine) {
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.config.StopTimeout)
	defer cancel()

	if err := engine.Stop(stopCtx); err != nil {
		r.logger.ErrorContext(
			stopCtx,
			"failed to stop workload",
			slog.String("task_id", taskID),
			slog.String("error", err.Error()),
		)
	}
}
