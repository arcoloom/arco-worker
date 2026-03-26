package runtime

import (
	"context"
	"log/slog"
	"slices"
	"testing"
)

func TestBuildCommandIncludesGPUFlagsAndNvidiaRuntime(t *testing.T) {
	t.Parallel()

	previousInspector := inspectDockerRuntimes
	inspectDockerRuntimes = func(context.Context) (map[string]any, error) {
		return map[string]any{"nvidia": map[string]any{}}, nil
	}
	t.Cleanup(func() {
		inspectDockerRuntimes = previousInspector
	})

	engine := NewDockerEngine(slog.Default())
	cmd, _, _, err := engine.buildCommand(context.Background(), DockerPayload{
		WorkspaceRoot: "/tmp/workspace",
		Image:         "ghcr.io/arcoloom/demo:latest",
		GPUCount:      2,
	}, "arco-task-gpu", nil)
	if err != nil {
		t.Fatalf("buildCommand() error = %v", err)
	}

		want := []string{
		"docker",
		"run",
		"--rm",
		"--name", "arco-task-gpu",
		"--volume", "/tmp/workspace:" + containerWorkspaceRoot,
		"--gpus", "all",
		"--runtime", "nvidia",
		"ghcr.io/arcoloom/demo:latest",
	}
	if !slices.Equal(cmd.Args, want) {
		t.Fatalf("buildCommand() args = %#v, want %#v", cmd.Args, want)
	}
}

func TestBuildCommandSkipsNvidiaRuntimeWhenUnavailable(t *testing.T) {
	t.Parallel()

	previousInspector := inspectDockerRuntimes
	inspectDockerRuntimes = func(context.Context) (map[string]any, error) {
		return map[string]any{"runc": map[string]any{}}, nil
	}
	t.Cleanup(func() {
		inspectDockerRuntimes = previousInspector
	})

	engine := NewDockerEngine(slog.Default())
	cmd, _, _, err := engine.buildCommand(context.Background(), DockerPayload{
		WorkspaceRoot: "/tmp/workspace",
		Image:         "ghcr.io/arcoloom/demo:latest",
		GPUCount:      1,
	}, "arco-task-gpu", nil)
	if err != nil {
		t.Fatalf("buildCommand() error = %v", err)
	}

	if !slices.Contains(cmd.Args, "--gpus") || !slices.Contains(cmd.Args, "all") {
		t.Fatalf("buildCommand() args = %#v, want --gpus all", cmd.Args)
	}
	if slices.Contains(cmd.Args, "--runtime") {
		t.Fatalf("buildCommand() args = %#v, did not expect runtime override", cmd.Args)
	}
}
