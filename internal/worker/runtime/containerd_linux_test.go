//go:build linux

package runtime

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureManagedContainerdConfigWritesContainerdV3Config(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	paths := managedContainerdPaths{
		configPath: filepath.Join(root, "daemon", "config.toml"),
	}

	if err := ensureManagedContainerdConfig(paths); err != nil {
		t.Fatalf("ensureManagedContainerdConfig() error = %v", err)
	}

	got, err := os.ReadFile(paths.configPath)
	if err != nil {
		t.Fatalf("os.ReadFile(%q) error = %v", paths.configPath, err)
	}

	want := "version = 3\ndisabled_plugins = [\"io.containerd.grpc.v1.cri\"]\n"
	if string(got) != want {
		t.Fatalf("ensureManagedContainerdConfig() wrote %q, want %q", string(got), want)
	}
}
