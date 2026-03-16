package runtime

import "context"

// Engine defines the workload execution lifecycle behind a common runtime boundary.
type Engine interface {
	// Prepare makes the runtime environment ready before the workload is started.
	Prepare(ctx context.Context, payload []byte) error

	// Start launches the workload asynchronously and should return once the process is created.
	Start(ctx context.Context, payload []byte) error

	// Stop attempts a graceful shutdown of the running workload.
	Stop(ctx context.Context) error

	// Wait blocks until the workload exits and reports the final execution result.
	Wait(ctx context.Context) error
}

// ExecPayload describes a workload launched directly as a host process.
type ExecPayload struct {
	Command string            `json:"command"`
	Args    []string          `json:"args"`
	Env     map[string]string `json:"env"`
	WorkDir string            `json:"work_dir"`
}
