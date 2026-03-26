package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	workerv1 "github.com/arcoloom/arco-proto/gen/go/arcoloom/worker/v1"
	workerShutdown "github.com/arcoloom/arco-worker/internal/worker/shutdown"
)

func TestLocalAPIHealthz(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	newLocalAPIHandler(NewLocalState(LocalStateConfig{APIAddress: DefaultLocalAPIListenAddress})).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", recorder.Code)
	}
	if got := recorder.Body.String(); got != "ok\n" {
		t.Fatalf("healthz body = %q, want %q", got, "ok\n")
	}
}

func TestLocalAPIInfoSummarizesTaskAndShutdownState(t *testing.T) {
	t.Parallel()

	state := NewLocalState(LocalStateConfig{
		APIAddress:             DefaultLocalAPIListenAddress,
		InstanceID:             "instance-1",
		Provider:               "aws",
		WorkerVersion:          "test",
		ControlPlaneAddress:    "127.0.0.1:8443",
		ControlPlaneServerName: "workers.arcoloom.internal",
		ConnectTimeout:         15 * time.Second,
		HeartbeatInterval:      5 * time.Second,
		StopTimeout:            15 * time.Second,
	})
	state.MarkControlPlaneConnected()
	state.MarkWorkerConnected("worker-1")
	state.SetAssignment(&workerv1.Assignment{
		TaskId:      "task-1",
		RuntimeKind: workerv1.RuntimeKind_RUNTIME_KIND_EXEC,
		Payload: `{
			"task_id":"task-1",
			"workspace_root":"/var/lib/arco/workspaces/task-1",
			"source":{"kind":"git","uri":"https://example.com/repo.git","revision":"main","path":"app"},
			"command":"bash",
			"args":["run.sh","--fast"],
			"env":{"SECRET":"x","TOKEN":"y"},
			"work_dir":"app",
			"mounts":[{"mount_path":"/mnt/data"}],
			"shutdown_policy":{"grace_period":"7m"}
		}`,
	})
	state.SetShutdownMonitor(workerShutdown.MonitorConfig{
		Enabled:      true,
		Provider:     "aws",
		PricingModel: "spot",
		PollInterval: 5 * time.Second,
	})
	shutdownAt := time.Date(2026, 3, 26, 12, 30, 0, 0, time.UTC)
	state.RecordShutdownNotice(workerShutdown.Notice{
		Provider:   "aws",
		Detail:     "aws spot interruption notice: action=terminate",
		ShutdownAt: shutdownAt,
	})
	state.RecordShutdownRequest("spot", "aws spot interruption notice: action=terminate", 7*time.Minute)
	state.UpdateTaskState("task-1", workerv1.TaskState_TASK_STATE_RUNNING, "workload started")

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/info", nil)
	newLocalAPIHandler(state).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("info status = %d, want 200", recorder.Code)
	}

	var response struct {
		APIAddress string `json:"api_address"`
		Phase      string `json:"phase"`
		Worker     struct {
			WorkerID      string `json:"worker_id"`
			InstanceID    string `json:"instance_id"`
			Provider      string `json:"provider"`
			WorkerVersion string `json:"worker_version"`
		} `json:"worker"`
		Task struct {
			TaskID              string   `json:"task_id"`
			RuntimeKind         string   `json:"runtime_kind"`
			State               string   `json:"state"`
			Command             string   `json:"command"`
			Args                []string `json:"args"`
			WorkDir             string   `json:"work_dir"`
			WorkspaceRoot       string   `json:"workspace_root"`
			EnvCount            int      `json:"env_count"`
			StorageMountCount   int      `json:"storage_mount_count"`
			ShutdownGracePeriod string   `json:"shutdown_grace_period"`
			Source              struct {
				Kind string `json:"kind"`
				URI  string `json:"uri"`
				Path string `json:"path"`
			} `json:"source"`
		} `json:"task"`
		Shutdown struct {
			Monitor struct {
				Enabled      bool   `json:"enabled"`
				Provider     string `json:"provider"`
				PricingModel string `json:"pricing_model"`
				PollInterval string `json:"poll_interval"`
			} `json:"monitor"`
			Notice struct {
				Triggered bool   `json:"triggered"`
				Provider  string `json:"provider"`
				Detail    string `json:"detail"`
			} `json:"notice"`
			Request struct {
				Triggered   bool   `json:"triggered"`
				Source      string `json:"source"`
				Reason      string `json:"reason"`
				GracePeriod string `json:"grace_period"`
			} `json:"request"`
		} `json:"shutdown"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode info response: %v", err)
	}

	if response.APIAddress != DefaultLocalAPIListenAddress {
		t.Fatalf("api_address = %q, want %q", response.APIAddress, DefaultLocalAPIListenAddress)
	}
	if response.Phase != "running" {
		t.Fatalf("phase = %q, want %q", response.Phase, "running")
	}
	if response.Worker.WorkerID != "worker-1" {
		t.Fatalf("worker_id = %q, want %q", response.Worker.WorkerID, "worker-1")
	}
	if response.Worker.InstanceID != "instance-1" {
		t.Fatalf("instance_id = %q, want %q", response.Worker.InstanceID, "instance-1")
	}
	if response.Task.TaskID != "task-1" {
		t.Fatalf("task_id = %q, want %q", response.Task.TaskID, "task-1")
	}
	if response.Task.RuntimeKind != "exec" {
		t.Fatalf("runtime_kind = %q, want %q", response.Task.RuntimeKind, "exec")
	}
	if response.Task.State != "running" {
		t.Fatalf("task state = %q, want %q", response.Task.State, "running")
	}
	if response.Task.Command != "bash" {
		t.Fatalf("command = %q, want %q", response.Task.Command, "bash")
	}
	if len(response.Task.Args) != 2 {
		t.Fatalf("args length = %d, want 2", len(response.Task.Args))
	}
	if response.Task.EnvCount != 2 {
		t.Fatalf("env_count = %d, want 2", response.Task.EnvCount)
	}
	if response.Task.StorageMountCount != 1 {
		t.Fatalf("storage_mount_count = %d, want 1", response.Task.StorageMountCount)
	}
	if response.Task.ShutdownGracePeriod != "7m0s" {
		t.Fatalf("shutdown_grace_period = %q, want %q", response.Task.ShutdownGracePeriod, "7m0s")
	}
	if response.Shutdown.Monitor.Provider != "aws" || !response.Shutdown.Monitor.Enabled {
		t.Fatalf("shutdown monitor = %#v, want enabled aws monitor", response.Shutdown.Monitor)
	}
	if !response.Shutdown.Notice.Triggered {
		t.Fatal("shutdown notice was not marked as triggered")
	}
	if response.Shutdown.Request.Source != "spot" {
		t.Fatalf("shutdown request source = %q, want %q", response.Shutdown.Request.Source, "spot")
	}
	if response.Shutdown.Request.GracePeriod != "7m0s" {
		t.Fatalf("shutdown request grace_period = %q, want %q", response.Shutdown.Request.GracePeriod, "7m0s")
	}
}
