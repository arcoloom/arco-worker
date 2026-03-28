package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestParseContainerPayloadNormalizesGPUDevices(t *testing.T) {
	t.Parallel()

	payload, err := parseContainerPayload(context.Background(), []byte(`{
		"task_id": "task-1",
		"workspace_root": "/tmp/workspace",
		"image": "ghcr.io/arcoloom/demo:latest",
		"gpu_devices": [" 0 ", "GPU-123", "0"]
	}`))
	if err != nil {
		t.Fatalf("parseContainerPayload() error = %v", err)
	}

	if len(payload.GPUDevices) != 2 || payload.GPUDevices[0] != "0" || payload.GPUDevices[1] != "GPU-123" {
		t.Fatalf("parseContainerPayload() gpu_devices = %#v, want [0 GPU-123]", payload.GPUDevices)
	}
}

func TestResolveContainerDirectoryMountsCreatesRelativeWorkspaceDirectory(t *testing.T) {
	t.Parallel()

	workspaceRoot := t.TempDir()
	mounts, err := resolveContainerDirectoryMounts(context.Background(), workspaceRoot, []ContainerMount{
		{
			Source: "artifacts/output",
			Target: "/mnt/output",
		},
	})
	if err != nil {
		t.Fatalf("resolveContainerDirectoryMounts() error = %v", err)
	}

	wantSource := filepath.Join(workspaceRoot, "artifacts", "output")
	if got := mounts[0].Source; got != wantSource {
		t.Fatalf("resolveContainerDirectoryMounts() source = %q, want %q", got, wantSource)
	}
	if _, err := os.Stat(wantSource); err != nil {
		t.Fatalf("resolveContainerDirectoryMounts() did not create %q: %v", wantSource, err)
	}
}

func TestContainerTaskNameSanitizesValue(t *testing.T) {
	t.Parallel()

	got := containerTaskName(" Task/ID.With_Weird Chars ")
	want := "arco-task-task-id-with-weirdchars"
	if got != want {
		t.Fatalf("containerTaskName() = %q, want %q", got, want)
	}
}

func TestResolveCDIDeviceRequestsUsesPreferredIndexDevices(t *testing.T) {
	t.Parallel()

	got, err := resolveCDIDeviceRequests("arcoloom.local", "gpu", []string{"0", "GPU-aaa", "1", "GPU-bbb", "all"}, nil, 2)
	if err != nil {
		t.Fatalf("resolveCDIDeviceRequests() error = %v", err)
	}

	want := []string{"arcoloom.local/gpu=0", "arcoloom.local/gpu=1"}
	if len(got) != len(want) {
		t.Fatalf("resolveCDIDeviceRequests() len = %d, want %d (%#v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("resolveCDIDeviceRequests()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestResolveCDIDeviceRequestsAcceptsQualifiedExplicitDevice(t *testing.T) {
	t.Parallel()

	got, err := resolveCDIDeviceRequests("arcoloom.local", "gpu", []string{"0", "GPU-aaa", "all"}, []string{"arcoloom.local/gpu=GPU-aaa"}, 0)
	if err != nil {
		t.Fatalf("resolveCDIDeviceRequests() error = %v", err)
	}

	if len(got) != 1 || got[0] != "arcoloom.local/gpu=GPU-aaa" {
		t.Fatalf("resolveCDIDeviceRequests() = %#v, want [arcoloom.local/gpu=GPU-aaa]", got)
	}
}
