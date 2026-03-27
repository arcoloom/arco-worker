package app

import (
	"encoding/json"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	workerv1 "github.com/arcoloom/arco-proto/gen/go/arcoloom/worker/v1"
	workerRuntime "github.com/arcoloom/arco-worker/internal/worker/runtime"
	workerShutdown "github.com/arcoloom/arco-worker/internal/worker/shutdown"
)

type LocalStateConfig struct {
	APIAddress             string
	InstanceID             string
	Provider               string
	InstanceType           string
	Region                 string
	AvailabilityZone       string
	WorkerVersion          string
	ControlPlaneAddress    string
	ControlPlaneServerName string
	ConnectTimeout         time.Duration
	HeartbeatInterval      time.Duration
	StopTimeout            time.Duration
}

type LocalState struct {
	mu       sync.RWMutex
	snapshot localSnapshot
}

type localSnapshot struct {
	APIAddress    string                    `json:"api_address"`
	StartedAt     time.Time                 `json:"started_at"`
	Phase         string                    `json:"phase"`
	StatusMessage string                    `json:"status_message,omitempty"`
	Worker        localWorkerSnapshot       `json:"worker"`
	ControlPlane  localControlPlaneSnapshot `json:"control_plane"`
	Machine       localMachineSnapshot      `json:"machine"`
	Task          *localTaskSnapshot        `json:"task,omitempty"`
	Shutdown      localShutdownSnapshot     `json:"shutdown"`
}

type localWorkerSnapshot struct {
	WorkerID         string     `json:"worker_id,omitempty"`
	InstanceID       string     `json:"instance_id"`
	Provider         string     `json:"provider"`
	CloudVendor      string     `json:"cloud_vendor,omitempty"`
	InstanceType     string     `json:"instance_type,omitempty"`
	Region           string     `json:"region,omitempty"`
	AvailabilityZone string     `json:"availability_zone,omitempty"`
	AZ               string     `json:"az,omitempty"`
	WorkerVersion    string     `json:"worker_version"`
	ConnectedAt      *time.Time `json:"connected_at,omitempty"`
	LastHeartbeatAt  *time.Time `json:"last_heartbeat_at,omitempty"`
}

type localControlPlaneSnapshot struct {
	Address           string `json:"address"`
	ServerName        string `json:"server_name"`
	ConnectTimeout    string `json:"connect_timeout"`
	HeartbeatInterval string `json:"heartbeat_interval"`
	StopTimeout       string `json:"stop_timeout"`
	StreamConnected   bool   `json:"stream_connected"`
}

type localMachineSnapshot struct {
	Hostname         string   `json:"hostname"`
	PID              int      `json:"pid"`
	OS               string   `json:"os"`
	Architecture     string   `json:"architecture"`
	CPUs             int      `json:"cpus"`
	IPAddresses      []string `json:"ip_addresses,omitempty"`
	TotalMemoryBytes uint64   `json:"total_memory_bytes,omitempty"`
}

type localTaskSnapshot struct {
	TaskID               string               `json:"task_id"`
	RuntimeKind          string               `json:"runtime_kind"`
	State                string               `json:"state"`
	Message              string               `json:"message,omitempty"`
	AssignmentReceivedAt *time.Time           `json:"assignment_received_at,omitempty"`
	Source               *localSourceSnapshot `json:"source,omitempty"`
	Command              string               `json:"command,omitempty"`
	Args                 []string             `json:"args,omitempty"`
	Image                string               `json:"image,omitempty"`
	WorkDir              string               `json:"work_dir,omitempty"`
	WorkspaceRoot        string               `json:"workspace_root,omitempty"`
	GPUCount             int                  `json:"gpu_count,omitempty"`
	EnvCount             int                  `json:"env_count,omitempty"`
	StorageMountCount    int                  `json:"storage_mount_count,omitempty"`
	ShutdownGracePeriod  string               `json:"shutdown_grace_period,omitempty"`
	ParseError           string               `json:"parse_error,omitempty"`
}

type localSourceSnapshot struct {
	Kind     string `json:"kind,omitempty"`
	URI      string `json:"uri,omitempty"`
	Revision string `json:"revision,omitempty"`
	Path     string `json:"path,omitempty"`
}

type localShutdownSnapshot struct {
	Monitor localShutdownMonitorSnapshot `json:"monitor"`
	Notice  localShutdownNoticeSnapshot  `json:"notice"`
	Request localShutdownRequestSnapshot `json:"request"`
}

type localShutdownMonitorSnapshot struct {
	Enabled      bool   `json:"enabled"`
	Provider     string `json:"provider,omitempty"`
	PricingModel string `json:"pricing_model,omitempty"`
	PollInterval string `json:"poll_interval,omitempty"`
}

type localShutdownNoticeSnapshot struct {
	Triggered  bool       `json:"triggered"`
	Provider   string     `json:"provider,omitempty"`
	Detail     string     `json:"detail,omitempty"`
	DetectedAt *time.Time `json:"detected_at,omitempty"`
	ShutdownAt *time.Time `json:"shutdown_at,omitempty"`
}

type localShutdownRequestSnapshot struct {
	Triggered   bool       `json:"triggered"`
	Source      string     `json:"source,omitempty"`
	Reason      string     `json:"reason,omitempty"`
	RequestedAt *time.Time `json:"requested_at,omitempty"`
	GracePeriod string     `json:"grace_period,omitempty"`
}

func NewLocalState(config LocalStateConfig) *LocalState {
	apiAddress := strings.TrimSpace(config.APIAddress)
	now := time.Now().UTC()
	return &LocalState{
		snapshot: localSnapshot{
			APIAddress:    apiAddress,
			StartedAt:     now,
			Phase:         "starting",
			StatusMessage: "worker process started",
			Worker: localWorkerSnapshot{
				InstanceID:       strings.TrimSpace(config.InstanceID),
				Provider:         strings.TrimSpace(config.Provider),
				CloudVendor:      strings.TrimSpace(config.Provider),
				InstanceType:     strings.TrimSpace(config.InstanceType),
				Region:           strings.TrimSpace(config.Region),
				AvailabilityZone: strings.TrimSpace(config.AvailabilityZone),
				AZ:               strings.TrimSpace(config.AvailabilityZone),
				WorkerVersion:    strings.TrimSpace(config.WorkerVersion),
			},
			ControlPlane: localControlPlaneSnapshot{
				Address:           strings.TrimSpace(config.ControlPlaneAddress),
				ServerName:        strings.TrimSpace(config.ControlPlaneServerName),
				ConnectTimeout:    durationString(config.ConnectTimeout),
				HeartbeatInterval: durationString(config.HeartbeatInterval),
				StopTimeout:       durationString(config.StopTimeout),
			},
			Machine: collectMachineSnapshot(),
		},
	}
}

func (s *LocalState) Snapshot() localSnapshot {
	if s == nil {
		return localSnapshot{}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot := s.snapshot
	snapshot.Machine.IPAddresses = append([]string(nil), snapshot.Machine.IPAddresses...)
	if snapshot.Task != nil {
		taskCopy := *snapshot.Task
		taskCopy.Args = append([]string(nil), taskCopy.Args...)
		snapshot.Task = &taskCopy
	}
	return snapshot
}

func (s *LocalState) SetAPIAddress(address string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.APIAddress = strings.TrimSpace(address)
}

func (s *LocalState) SetPhase(phase string, message string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.Phase = normalizedPhase(phase)
	s.snapshot.StatusMessage = strings.TrimSpace(message)
}

func (s *LocalState) MarkControlPlaneConnected() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.ControlPlane.StreamConnected = true
	s.snapshot.Phase = "waiting_assignment"
	s.snapshot.StatusMessage = "waiting for control plane assignment"
}

func (s *LocalState) MarkWorkerConnected(workerID string) {
	if s == nil {
		return
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.Worker.WorkerID = strings.TrimSpace(workerID)
	s.snapshot.Worker.ConnectedAt = &now
	if s.snapshot.Phase == "" || s.snapshot.Phase == "starting" || s.snapshot.Phase == "connecting" {
		s.snapshot.Phase = "waiting_assignment"
		s.snapshot.StatusMessage = "waiting for control plane assignment"
	}
}

func (s *LocalState) MarkHeartbeatSent() {
	if s == nil {
		return
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.Worker.LastHeartbeatAt = &now
}

func (s *LocalState) SetAssignment(assignment *workerv1.Assignment) {
	if s == nil || assignment == nil {
		return
	}
	now := time.Now().UTC()
	task := summarizeAssignment(assignment, now)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.Task = &task
	s.snapshot.Shutdown = localShutdownSnapshot{}
	s.snapshot.Phase = "assigned"
	s.snapshot.StatusMessage = "assignment received"
}

func (s *LocalState) SetShutdownMonitor(config workerShutdown.MonitorConfig) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.Shutdown.Monitor = localShutdownMonitorSnapshot{
		Enabled:      config.Enabled,
		Provider:     strings.TrimSpace(config.Provider),
		PricingModel: strings.TrimSpace(config.PricingModel),
		PollInterval: durationString(config.PollInterval),
	}
}

func (s *LocalState) RecordShutdownNotice(notice workerShutdown.Notice) {
	if s == nil {
		return
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.Shutdown.Notice.Triggered = true
	s.snapshot.Shutdown.Notice.Provider = strings.TrimSpace(notice.Provider)
	s.snapshot.Shutdown.Notice.Detail = strings.TrimSpace(notice.Detail)
	s.snapshot.Shutdown.Notice.DetectedAt = &now
	if !notice.ShutdownAt.IsZero() {
		shutdownAt := notice.ShutdownAt.UTC()
		s.snapshot.Shutdown.Notice.ShutdownAt = &shutdownAt
	}
}

func (s *LocalState) RecordShutdownRequest(source string, reason string, gracePeriod time.Duration) {
	if s == nil {
		return
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.Shutdown.Request.Triggered {
		return
	}
	s.snapshot.Shutdown.Request = localShutdownRequestSnapshot{
		Triggered:   true,
		Source:      strings.TrimSpace(source),
		Reason:      strings.TrimSpace(reason),
		RequestedAt: &now,
		GracePeriod: durationString(gracePeriod),
	}
}

func (s *LocalState) UpdateTaskState(taskID string, state workerv1.TaskState, message string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.snapshot.Task == nil {
		s.snapshot.Task = &localTaskSnapshot{
			TaskID:      strings.TrimSpace(taskID),
			RuntimeKind: "unspecified",
		}
	}
	if trimmedTaskID := strings.TrimSpace(taskID); trimmedTaskID != "" {
		s.snapshot.Task.TaskID = trimmedTaskID
	}
	stateName := taskStateName(state)
	s.snapshot.Task.State = stateName
	s.snapshot.Task.Message = strings.TrimSpace(message)
	s.snapshot.Phase = stateName
	s.snapshot.StatusMessage = strings.TrimSpace(message)
}

func summarizeAssignment(assignment *workerv1.Assignment, receivedAt time.Time) localTaskSnapshot {
	task := localTaskSnapshot{
		TaskID:               strings.TrimSpace(assignment.GetTaskId()),
		RuntimeKind:          runtimeKindName(assignment.GetRuntimeKind()),
		State:                "assigned",
		Message:              "assignment received",
		AssignmentReceivedAt: &receivedAt,
	}

	payload := []byte(assignment.GetPayload())
	task.ShutdownGracePeriod = durationString(assignmentShutdownGracePeriod(payload))

	switch assignment.GetRuntimeKind() {
	case workerv1.RuntimeKind_RUNTIME_KIND_EXEC:
		var execPayload workerRuntime.ExecPayload
		if err := json.Unmarshal(payload, &execPayload); err != nil {
			task.ParseError = err.Error()
			return task
		}
		task.Source = summarizeSource(execPayload.Source)
		task.Command = strings.TrimSpace(execPayload.Command)
		task.Args = append([]string(nil), execPayload.Args...)
		task.WorkDir = strings.TrimSpace(execPayload.WorkDir)
		task.WorkspaceRoot = strings.TrimSpace(execPayload.WorkspaceRoot)
		task.EnvCount = len(execPayload.Env)
		task.StorageMountCount = len(execPayload.Mounts)
	case workerv1.RuntimeKind_RUNTIME_KIND_DOCKER:
		var dockerPayload workerRuntime.DockerPayload
		if err := json.Unmarshal(payload, &dockerPayload); err != nil {
			task.ParseError = err.Error()
			return task
		}
		task.Source = summarizeSource(dockerPayload.Source)
		task.Image = strings.TrimSpace(dockerPayload.Image)
		task.Args = append([]string(nil), dockerPayload.Command...)
		task.WorkDir = strings.TrimSpace(dockerPayload.WorkDir)
		task.WorkspaceRoot = strings.TrimSpace(dockerPayload.WorkspaceRoot)
		task.GPUCount = dockerPayload.GPUCount
		task.EnvCount = len(dockerPayload.Env)
		task.StorageMountCount = len(dockerPayload.Mounts)
	default:
		if len(payload) > 0 {
			task.ParseError = "assignment payload is not summarized for this runtime"
		}
	}

	return task
}

func summarizeSource(source *workerRuntime.SourceSpec) *localSourceSnapshot {
	if source == nil {
		return nil
	}
	return &localSourceSnapshot{
		Kind:     strings.TrimSpace(source.Kind),
		URI:      strings.TrimSpace(source.URI),
		Revision: strings.TrimSpace(source.Revision),
		Path:     strings.TrimSpace(source.Path),
	}
}

func collectMachineSnapshot() localMachineSnapshot {
	hostname, _ := os.Hostname()
	machine := localMachineSnapshot{
		Hostname:     strings.TrimSpace(hostname),
		PID:          os.Getpid(),
		OS:           runtime.GOOS,
		Architecture: runtime.GOARCH,
		CPUs:         runtime.NumCPU(),
		IPAddresses:  collectIPAddresses(),
	}
	if total, err := readTotalMemoryBytes(); err == nil && total > 0 {
		machine.TotalMemoryBytes = total
	}
	return machine
}

func collectIPAddresses() []string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}

	seen := map[string]struct{}{}
	items := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		var ip net.IP
		switch value := addr.(type) {
		case *net.IPNet:
			ip = value.IP
		case *net.IPAddr:
			ip = value.IP
		}
		if ip == nil || ip.IsLoopback() {
			continue
		}
		text := ip.String()
		if text == "" {
			continue
		}
		if _, ok := seen[text]; ok {
			continue
		}
		seen[text] = struct{}{}
		items = append(items, text)
	}
	sort.Strings(items)
	return items
}

func readTotalMemoryBytes() (uint64, error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			break
		}
		value, err := parseUint(fields[1])
		if err != nil {
			return 0, err
		}
		return value * 1024, nil
	}
	return 0, os.ErrNotExist
}

func parseUint(raw string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
}

func normalizedPhase(phase string) string {
	if trimmed := strings.TrimSpace(phase); trimmed != "" {
		return trimmed
	}
	return "unknown"
}

func durationString(value time.Duration) string {
	if value <= 0 {
		return ""
	}
	return value.String()
}

func runtimeKindName(kind workerv1.RuntimeKind) string {
	switch kind {
	case workerv1.RuntimeKind_RUNTIME_KIND_EXEC:
		return "exec"
	case workerv1.RuntimeKind_RUNTIME_KIND_DOCKER:
		return "docker"
	default:
		return "unspecified"
	}
}

func taskStateName(state workerv1.TaskState) string {
	switch state {
	case workerv1.TaskState_TASK_STATE_PREPARING:
		return "preparing"
	case workerv1.TaskState_TASK_STATE_RUNNING:
		return "running"
	case workerv1.TaskState_TASK_STATE_SUCCESS:
		return "success"
	case workerv1.TaskState_TASK_STATE_FAILED:
		return "failed"
	case workerv1.TaskState_TASK_STATE_STOPPING:
		return "stopping"
	case workerv1.TaskState_TASK_STATE_INTERRUPTED:
		return "interrupted"
	default:
		return "unspecified"
	}
}
