package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	workerv1 "github.com/arcoloom/arco-worker/gen/proto/arcoloom/worker/v1"
	workerRuntime "github.com/arcoloom/arco-worker/internal/worker/runtime"
)

// EngineFactory resolves the correct runtime engine for a task assignment.
type EngineFactory func(ctx context.Context, kind workerv1.RuntimeKind) (workerRuntime.Engine, error)

// RunnerConfig contains the worker identity and RPC timeout settings.
type RunnerConfig struct {
	InstanceID        string
	Provider          string
	RegistrationToken string
	RegisterTimeout   time.Duration
	ReportTimeout     time.Duration
	StopTimeout       time.Duration
}

// Runner coordinates register, report, and workload execution.
type Runner struct {
	logger        *slog.Logger
	client        ControlPlaneClient
	engineFactory EngineFactory
	config        RunnerConfig
}

// NewRunner constructs a worker runner.
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
	if config.RegisterTimeout <= 0 {
		config.RegisterTimeout = 15 * time.Second
	}
	if config.ReportTimeout <= 0 {
		config.ReportTimeout = 5 * time.Second
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

// Run executes the worker lifecycle once: register, run the assigned task, and report the result.
func (r *Runner) Run(ctx context.Context) error {
	registerCtx, cancel := context.WithTimeout(ctx, r.config.RegisterTimeout)
	defer cancel()

	registration, err := r.client.Register(registerCtx, &workerv1.RegisterRequest{
		InstanceId:        r.config.InstanceID,
		Provider:          r.config.Provider,
		RegistrationToken: r.config.RegistrationToken,
	})
	if err != nil {
		return fmt.Errorf("register worker: %w", err)
	}

	taskID := registration.GetTaskId()
	if taskID == "" {
		return errors.New("control plane returned an empty task ID")
	}

	engine, err := r.engineFactory(context.WithoutCancel(ctx), registration.GetRuntimeKind())
	if err != nil {
		r.logReportStatusError(ctx, taskID, workerv1.TaskState_TASK_STATE_FAILED, err.Error())
		return fmt.Errorf("resolve runtime %s: %w", registration.GetRuntimeKind().String(), err)
	}

	payload := []byte(registration.GetPayload())

	if err := r.reportStatus(ctx, taskID, workerv1.TaskState_TASK_STATE_PREPARING, "preparing workload"); err != nil {
		return err
	}

	if err := engine.Prepare(ctx, payload); err != nil {
		r.logReportStatusError(ctx, taskID, workerv1.TaskState_TASK_STATE_FAILED, err.Error())
		return fmt.Errorf("prepare workload: %w", err)
	}

	if err := engine.Start(ctx, payload); err != nil {
		r.logReportStatusError(ctx, taskID, workerv1.TaskState_TASK_STATE_FAILED, err.Error())
		return fmt.Errorf("start workload: %w", err)
	}

	if err := r.reportStatus(ctx, taskID, workerv1.TaskState_TASK_STATE_RUNNING, "workload started"); err != nil {
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
		r.logReportStatusError(ctx, taskID, workerv1.TaskState_TASK_STATE_FAILED, waitErr.Error())
		return fmt.Errorf("wait workload: %w", waitErr)
	}

	if err := r.reportStatus(ctx, taskID, workerv1.TaskState_TASK_STATE_SUCCESS, "workload finished successfully"); err != nil {
		return err
	}

	r.logger.InfoContext(
		context.WithoutCancel(ctx),
		"worker completed task",
		slog.String("task_id", taskID),
		slog.String("runtime_kind", registration.GetRuntimeKind().String()),
	)

	return nil
}

func (r *Runner) reportStatus(ctx context.Context, taskID string, state workerv1.TaskState, message string) error {
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.config.ReportTimeout)
	defer cancel()

	if err := r.client.ReportStatus(reportCtx, &workerv1.ReportStatusRequest{
		TaskId:  taskID,
		State:   state,
		Message: message,
	}); err != nil {
		return fmt.Errorf("report status %s for task %s: %w", state.String(), taskID, err)
	}

	r.logger.InfoContext(
		reportCtx,
		"reported task status",
		slog.String("task_id", taskID),
		slog.String("state", state.String()),
		slog.String("message", message),
	)

	return nil
}

func (r *Runner) logReportStatusError(ctx context.Context, taskID string, state workerv1.TaskState, message string) {
	if err := r.reportStatus(ctx, taskID, state, message); err != nil {
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
