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

type LogStream string

const (
	LogStreamStdout LogStream = "stdout"
	LogStreamStderr LogStream = "stderr"
)

type LogEntry struct {
	Stream LogStream
	Line   string
}

type LogEmitter func(LogEntry)

type LogEmitterAware interface {
	SetLogEmitter(LogEmitter)
}

type StorageMount struct {
	Driver          string   `json:"driver"`
	Bucket          string   `json:"bucket"`
	Prefix          string   `json:"prefix,omitempty"`
	Endpoint        string   `json:"endpoint"`
	Region          string   `json:"region,omitempty"`
	AccessKeyID     string   `json:"access_key_id"`
	SecretAccessKey string   `json:"secret_access_key"`
	SessionToken    string   `json:"session_token,omitempty"`
	UsePathStyle    bool     `json:"use_path_style"`
	UseSSL          bool     `json:"use_ssl"`
	ExtraOptions    []string `json:"extra_options,omitempty"`
	MountPath       string   `json:"mount_path"`
}

// ExecPayload describes a workload launched directly as a host process.
type ExecPayload struct {
	Command string            `json:"command"`
	Args    []string          `json:"args"`
	Env     map[string]string `json:"env"`
	WorkDir string            `json:"work_dir"`
	Mounts  []StorageMount    `json:"mounts,omitempty"`
}
