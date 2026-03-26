package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	workerv1 "github.com/arcoloom/arco-proto/gen/go/arcoloom/worker/v1"
	workerRuntime "github.com/arcoloom/arco-worker/internal/worker/runtime"
	workerShutdown "github.com/arcoloom/arco-worker/internal/worker/shutdown"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type EngineFactory func(ctx context.Context, kind workerv1.RuntimeKind) (workerRuntime.Engine, error)
type ShutdownMonitor interface {
	Run(context.Context) error
}

type ShutdownMonitorFactory func(*slog.Logger, workerShutdown.Reporter, workerShutdown.MonitorConfig) ShutdownMonitor

type RunnerConfig struct {
	InstanceID        string
	Provider          string
	RegistrationToken string
	WorkerVersion     string
	ConnectTimeout    time.Duration
	HeartbeatInterval time.Duration
	StopTimeout       time.Duration
	LogRelay          *TaskLogRelay
	ShutdownMonitor   ShutdownMonitorFactory
}

type Runner struct {
	logger         *slog.Logger
	client         ControlPlaneClient
	engineFactory  EngineFactory
	monitorFactory ShutdownMonitorFactory
	config         RunnerConfig
	logRelay       *TaskLogRelay
}

type controlPlaneShutdownError struct {
	reason string
}

type shutdownRequest struct {
	source string
	reason string
}

func (e *controlPlaneShutdownError) Error() string {
	return e.reason
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
		logger:         logger,
		client:         client,
		engineFactory:  engineFactory,
		monitorFactory: nonNilShutdownMonitorFactory(config.ShutdownMonitor),
		config:         config,
		logRelay:       config.LogRelay,
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
		workerID             string
		terminalSessionToken string
		assignment           *workerv1.Assignment
		err                  error
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

		workerID, terminalSessionToken, assignment, err := r.waitForAssignment(streamCtx, session)
		handshakeCh <- handshakeResult{
			workerID:             workerID,
			terminalSessionToken: terminalSessionToken,
			assignment:           assignment,
			err:                  err,
		}
	}()

	timer := time.NewTimer(r.config.ConnectTimeout)
	defer timer.Stop()

	var (
		workerID             string
		terminalSessionToken string
		assignment           *workerv1.Assignment
	)
	select {
	case result := <-handshakeCh:
		if result.err != nil {
			var shutdownErr *controlPlaneShutdownError
			if errors.As(result.err, &shutdownErr) {
				r.logger.InfoContext(
					context.WithoutCancel(ctx),
					"control plane requested shutdown before assignment",
					slog.String("reason", shutdownErr.reason),
				)
				return nil
			}
			return result.err
		}
		workerID = result.workerID
		terminalSessionToken = result.terminalSessionToken
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
	if terminalSessionToken == "" {
		return errors.New("control plane did not return a terminal session token")
	}

	terminalAgent, err := NewTerminalAgent(r.logger, r.client, nil, TerminalAgentConfig{
		InstanceID:           r.config.InstanceID,
		Provider:             r.config.Provider,
		RegistrationToken:    r.config.RegistrationToken,
		WorkerVersion:        r.config.WorkerVersion,
		TerminalSessionToken: terminalSessionToken,
	})
	if err != nil {
		return fmt.Errorf("create terminal agent: %w", err)
	}
	terminalCtx, stopTerminal := context.WithCancel(ctx)
	defer stopTerminal()
	go func() {
		if err := terminalAgent.Run(terminalCtx); err != nil && terminalCtx.Err() == nil {
			r.logger.WarnContext(context.WithoutCancel(ctx), "terminal agent exited", slog.String("error", err.Error()))
		}
	}()

	heartbeatCtx, stopHeartbeats := context.WithCancel(context.WithoutCancel(ctx))
	defer stopHeartbeats()
	go r.runHeartbeats(heartbeatCtx, session, workerID)

	if err := r.runAssignment(ctx, session, assignment); err != nil {
		return err
	}
	return r.waitForControlPlaneClosure(ctx, session)
}

func (r *Runner) waitForAssignment(ctx context.Context, session ControlPlaneSession) (string, string, *workerv1.Assignment, error) {
	workerID := ""
	terminalSessionToken := ""
	for {
		message, err := session.Receive(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return "", "", nil, errors.New("control plane closed the stream before assignment")
			}
			return "", "", nil, fmt.Errorf("receive control-plane message: %w", err)
		}

		switch payload := message.GetMessage().(type) {
		case *workerv1.ControlToWorker_HelloAck:
			workerID = payload.HelloAck.GetWorkerId()
			terminalSessionToken = payload.HelloAck.GetTerminalSessionToken()
		case *workerv1.ControlToWorker_Assignment:
			return workerID, terminalSessionToken, payload.Assignment, nil
		case *workerv1.ControlToWorker_Shutdown:
			reason := payload.Shutdown.GetReason()
			if reason == "" {
				reason = "control plane requested shutdown before assignment"
			}
			return "", "", nil, &controlPlaneShutdownError{reason: reason}
		default:
			return "", "", nil, errors.New("received unsupported control-plane message")
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
		r.logger.ErrorContext(
			context.WithoutCancel(ctx),
			"failed to resolve runtime",
			slog.String("task_id", taskID),
			slog.String("runtime_kind", assignment.GetRuntimeKind().String()),
			slog.String("error", err.Error()),
		)
		return nil
	}
	if r.logRelay != nil {
		r.logRelay.BindTask(session, taskID)
		if emitterAware, ok := engine.(workerRuntime.LogEmitterAware); ok {
			emitterAware.SetLogEmitter(r.logRelay.ProgramEmitter())
		}
	}

	payload := []byte(assignment.GetPayload())
	gracePeriod := assignmentShutdownGracePeriod(payload)
	shutdownRequests := make(chan shutdownRequest, 4)
	monitorCtx, stopMonitor := context.WithCancel(ctx)
	defer stopMonitor()
	r.startShutdownMonitor(monitorCtx, session, taskID, payload, shutdownRequests)
	if err := r.sendStatus(ctx, session, taskID, workerv1.TaskState_TASK_STATE_PREPARING, "preparing workload"); err != nil {
		return err
	}
	if err := engine.Prepare(ctx, payload); err != nil {
		r.logStatusError(ctx, session, taskID, workerv1.TaskState_TASK_STATE_FAILED, err.Error())
		r.logger.ErrorContext(
			context.WithoutCancel(ctx),
			"failed to prepare workload",
			slog.String("task_id", taskID),
			slog.String("error", err.Error()),
		)
		return nil
	}
	if err := engine.Start(ctx, payload); err != nil {
		r.logStatusError(ctx, session, taskID, workerv1.TaskState_TASK_STATE_FAILED, err.Error())
		r.logger.ErrorContext(
			context.WithoutCancel(ctx),
			"failed to start workload",
			slog.String("task_id", taskID),
			slog.String("error", err.Error()),
		)
		return nil
	}
	if err := r.sendStatus(ctx, session, taskID, workerv1.TaskState_TASK_STATE_RUNNING, "workload started"); err != nil {
		r.stopWorkload(context.Background(), taskID, engine)
		return err
	}

	controlLoopCtx, stopControlLoop := context.WithCancel(ctx)
	defer stopControlLoop()
	go r.listenForControlPlaneShutdown(controlLoopCtx, session, shutdownRequests)

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- engine.Wait(context.WithoutCancel(ctx))
	}()

	var (
		shutdownTriggered bool
		shutdownReason    string
		shutdownSource    string
		graceTimer        *time.Timer
		graceTimerCh      <-chan time.Time
		ctxDoneCh         <-chan struct{} = ctx.Done()
	)
	defer func() {
		if graceTimer != nil {
			graceTimer.Stop()
		}
	}()

	for {
		select {
		case request := <-shutdownRequests:
			if shutdownTriggered {
				continue
			}
			shutdownTriggered = true
			shutdownReason = nonEmptyString(request.reason, "shutdown requested")
			shutdownSource = request.source
			if err := r.sendStatus(ctx, session, taskID, workerv1.TaskState_TASK_STATE_STOPPING, stoppingMessage(shutdownSource, shutdownReason, gracePeriod)); err != nil {
				return err
			}
			r.interruptWorkload(context.Background(), taskID, engine)
			graceTimer = time.NewTimer(gracePeriod)
			graceTimerCh = graceTimer.C
		case <-graceTimerCh:
			graceTimerCh = nil
			r.logger.WarnContext(
				context.WithoutCancel(ctx),
				"graceful shutdown timed out; forcing workload stop",
				slog.String("task_id", taskID),
				slog.String("reason", shutdownReason),
				slog.Duration("grace_period", gracePeriod),
			)
			r.stopWorkload(context.Background(), taskID, engine)
		case <-ctxDoneCh:
			ctxDoneCh = nil
			r.stopWorkload(context.Background(), taskID, engine)
		case waitErr := <-waitCh:
			stopControlLoop()
			if shutdownTriggered {
				return r.reportInterruptedExit(ctx, session, taskID, shutdownSource, shutdownReason, waitErr)
			}
			if waitErr != nil {
				r.logStatusError(ctx, session, taskID, workerv1.TaskState_TASK_STATE_FAILED, waitErr.Error())
				r.logger.ErrorContext(
					context.WithoutCancel(ctx),
					"workload finished with failure",
					slog.String("task_id", taskID),
					slog.String("error", waitErr.Error()),
				)
				return nil
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
	}
}

func (r *Runner) waitForControlPlaneClosure(ctx context.Context, session ControlPlaneSession) error {
	for {
		message, err := session.Receive(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return nil
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receive control-plane message after task completion: %w", err)
		}

		switch payload := message.GetMessage().(type) {
		case *workerv1.ControlToWorker_Shutdown:
			reason := payload.Shutdown.GetReason()
			if reason == "" {
				reason = "control plane requested shutdown"
			}
			r.logger.InfoContext(
				context.WithoutCancel(ctx),
				"received control-plane shutdown",
				slog.String("reason", reason),
			)
			return nil
		case *workerv1.ControlToWorker_HelloAck, *workerv1.ControlToWorker_Assignment:
			r.logger.WarnContext(
				context.WithoutCancel(ctx),
				"received unexpected control-plane message after task completion",
				slog.String("message_type", fmt.Sprintf("%T", payload)),
			)
		default:
			r.logger.WarnContext(
				context.WithoutCancel(ctx),
				"received unsupported control-plane message after task completion",
				slog.String("message_type", fmt.Sprintf("%T", payload)),
			)
		}
	}
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

func (r *Runner) interruptWorkload(ctx context.Context, taskID string, engine workerRuntime.Engine) {
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.config.StopTimeout)
	defer cancel()

	if err := engine.Interrupt(stopCtx); err != nil {
		r.logger.ErrorContext(
			stopCtx,
			"failed to interrupt workload",
			slog.String("task_id", taskID),
			slog.String("error", err.Error()),
		)
	}
}

func (r *Runner) startShutdownMonitor(ctx context.Context, session ControlPlaneSession, taskID string, payload []byte, requests chan<- shutdownRequest) {
	if r.monitorFactory == nil {
		return
	}
	config := workerShutdown.MonitorConfigFromAssignment(payload, r.config.Provider)
	if !config.Enabled {
		return
	}
	monitor := r.monitorFactory(r.logger, &shutdownReporter{
		logger:  r.logger,
		session: session,
		taskID:  taskID,
		onNotice: func(notice workerShutdown.Notice) {
			enqueueShutdownRequest(requests, shutdownRequest{source: "spot", reason: notice.Detail})
		},
	}, config)
	if monitor == nil {
		return
	}
	go func() {
		if err := monitor.Run(ctx); err != nil && ctx.Err() == nil {
			r.logger.WarnContext(
				context.WithoutCancel(ctx),
				"shutdown monitor exited",
				slog.String("provider", config.Provider),
				slog.String("error", err.Error()),
			)
		}
	}()
}

func nonNilShutdownMonitorFactory(factory ShutdownMonitorFactory) ShutdownMonitorFactory {
	if factory != nil {
		return factory
	}
	return func(logger *slog.Logger, reporter workerShutdown.Reporter, config workerShutdown.MonitorConfig) ShutdownMonitor {
		return workerShutdown.NewMonitor(logger, reporter, config, nil)
	}
}

func (r *Runner) listenForControlPlaneShutdown(ctx context.Context, session ControlPlaneSession, requests chan<- shutdownRequest) {
	for {
		message, err := session.Receive(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}
			r.logger.WarnContext(context.WithoutCancel(ctx), "control-plane receive loop exited", slog.String("error", err.Error()))
			return
		}

		switch payload := message.GetMessage().(type) {
		case *workerv1.ControlToWorker_Shutdown:
			reason := payload.Shutdown.GetReason()
			if reason == "" {
				reason = "control plane requested shutdown"
			}
			enqueueShutdownRequest(requests, shutdownRequest{source: "control-plane", reason: reason})
		case *workerv1.ControlToWorker_HelloAck, *workerv1.ControlToWorker_Assignment:
			r.logger.WarnContext(
				context.WithoutCancel(ctx),
				"received unexpected control-plane message while task is running",
				slog.String("message_type", fmt.Sprintf("%T", payload)),
			)
		default:
			r.logger.WarnContext(
				context.WithoutCancel(ctx),
				"received unsupported control-plane message while task is running",
				slog.String("message_type", fmt.Sprintf("%T", payload)),
			)
		}
	}
}

func enqueueShutdownRequest(ch chan<- shutdownRequest, request shutdownRequest) {
	if ch == nil {
		return
	}
	select {
	case ch <- request:
	default:
	}
}

type shutdownReporter struct {
	logger   *slog.Logger
	session  ControlPlaneSession
	taskID   string
	onNotice func(workerShutdown.Notice)
}

func (r *shutdownReporter) ReportNotice(ctx context.Context, notice workerShutdown.Notice) error {
	if r == nil || r.session == nil {
		return errors.New("shutdown reporter session is required")
	}
	signal := &workerv1.Signal{
		TaskId:     r.taskID,
		SignalType: workerv1.SignalType_SIGNAL_TYPE_SPOT_RECLAIM_RISK,
		Detail:     notice.Detail,
	}
	if !notice.ShutdownAt.IsZero() {
		signal.ShutdownAt = timestamppb.New(notice.ShutdownAt.UTC())
	}
	if err := r.session.Send(ctx, &workerv1.WorkerToControl{
		Message: &workerv1.WorkerToControl_Signal{Signal: signal},
	}); err != nil {
		return fmt.Errorf("send shutdown signal for task %s: %w", r.taskID, err)
	}
	if r.logger != nil {
		r.logger.InfoContext(
			context.WithoutCancel(ctx),
			"reported shutdown notice",
			slog.String("task_id", r.taskID),
			slog.String("provider", notice.Provider),
			slog.String("detail", notice.Detail),
		)
	}
	if r.onNotice != nil {
		r.onNotice(notice)
	}
	return nil
}

func (r *Runner) reportInterruptedExit(ctx context.Context, session ControlPlaneSession, taskID string, source string, reason string, waitErr error) error {
	message := interruptedMessage(source, reason, waitErr)
	if err := r.sendStatus(ctx, session, taskID, workerv1.TaskState_TASK_STATE_INTERRUPTED, message); err != nil {
		return err
	}
	if waitErr != nil {
		r.logger.WarnContext(
			context.WithoutCancel(ctx),
			"workload exited after shutdown request with error",
			slog.String("task_id", taskID),
			slog.String("error", waitErr.Error()),
		)
	} else {
		r.logger.InfoContext(
			context.WithoutCancel(ctx),
			"workload exited after shutdown request",
			slog.String("task_id", taskID),
			slog.String("source", source),
		)
	}
	return nil
}

func assignmentShutdownGracePeriod(payload []byte) time.Duration {
	const fallback = 5 * time.Minute
	var envelope struct {
		ShutdownPolicy *struct {
			GracePeriod string `json:"grace_period"`
		} `json:"shutdown_policy"`
	}
	if len(payload) == 0 || json.Unmarshal(payload, &envelope) != nil || envelope.ShutdownPolicy == nil {
		return fallback
	}
	if value, err := time.ParseDuration(envelope.ShutdownPolicy.GracePeriod); err == nil && value > 0 {
		return value
	}
	return fallback
}

func stoppingMessage(source string, reason string, gracePeriod time.Duration) string {
	return fmt.Sprintf("%s shutdown requested; sending Ctrl-C to workload and waiting up to %s: %s", nonEmptyString(source, "worker"), gracePeriod, nonEmptyString(reason, "shutdown requested"))
}

func interruptedMessage(source string, reason string, waitErr error) string {
	prefix := fmt.Sprintf("%s shutdown completed", nonEmptyString(source, "worker"))
	if waitErr == nil {
		return fmt.Sprintf("%s after Ctrl-C: %s", prefix, nonEmptyString(reason, "shutdown requested"))
	}
	return fmt.Sprintf("%s after Ctrl-C with exit detail %q: %s", prefix, waitErr.Error(), nonEmptyString(reason, "shutdown requested"))
}

func nonEmptyString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
