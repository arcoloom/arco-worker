package runtime

import "context"

// Engine defines the workload execution lifecycle behind a common runtime boundary.
type Engine interface {
	// Prepare makes the runtime environment ready before the workload is started.
	Prepare(ctx context.Context, payload []byte) error

	// Start launches the workload asynchronously and should return once the process is created.
	Start(ctx context.Context, payload []byte) error

	// Interrupt requests a Ctrl-C style graceful interruption of the running workload.
	Interrupt(ctx context.Context) error

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
	Protocol        string `json:"protocol"`
	Bucket          string `json:"bucket"`
	Prefix          string `json:"prefix,omitempty"`
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region,omitempty"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token,omitempty"`
	UsePathStyle    bool   `json:"use_path_style"`
	UseSSL          bool   `json:"use_ssl"`
	MountPath       string `json:"mount_path"`
}

type SourceSpec struct {
	Kind     string `json:"kind"`
	URI      string `json:"uri"`
	Revision string `json:"revision,omitempty"`
	Path     string `json:"path,omitempty"`
}

type ContainerMount struct {
	Source   string `json:"source"`
	Target   string `json:"target"`
	ReadOnly bool   `json:"read_only,omitempty"`
}

// ExecPayload describes a workload launched directly as a host process.
type ExecPayload struct {
	TaskID        string            `json:"task_id"`
	WorkspaceRoot string            `json:"workspace_root"`
	Source        *SourceSpec       `json:"source,omitempty"`
	Command       string            `json:"command"`
	Args          []string          `json:"args"`
	Env           map[string]string `json:"env"`
	WorkDir       string            `json:"work_dir"`
	Mounts        []StorageMount    `json:"mounts,omitempty"`
}

// ContainerPayload describes a workload launched as a containerd-backed container.
type ContainerPayload struct {
	TaskID          string            `json:"task_id"`
	WorkspaceRoot   string            `json:"workspace_root"`
	Source          *SourceSpec       `json:"source,omitempty"`
	Image           string            `json:"image"`
	Command         []string          `json:"command,omitempty"`
	Env             map[string]string `json:"env"`
	WorkDir         string            `json:"work_dir"`
	Mounts          []StorageMount    `json:"mounts,omitempty"`
	DirectoryMounts []ContainerMount  `json:"directory_mounts,omitempty"`
	GPUCount        int               `json:"gpu_count,omitempty"`
	GPUDevices      []string          `json:"gpu_devices,omitempty"`
}
