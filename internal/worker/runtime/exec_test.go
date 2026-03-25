package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestExecEngineWaitDrainsOutputBeforeReturning(t *testing.T) {
	t.Parallel()

	var logBuffer bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuffer, nil))
	engine := NewExecEngine(logger)

	var (
		mu      sync.Mutex
		entries []LogEntry
	)
	engine.SetLogEmitter(func(entry LogEntry) {
		time.Sleep(time.Millisecond)
		mu.Lock()
		entries = append(entries, entry)
		mu.Unlock()
	})

	payload, err := json.Marshal(ExecPayload{
		WorkspaceRoot: t.TempDir(),
		Command:       "/bin/sh",
		Args: []string{
			"-c",
			`i=1
while [ "$i" -le 256 ]; do
	printf 'out-%03d\n' "$i"
	printf 'err-%03d\n' "$i" >&2
	i=$((i+1))
done
printf 'out-tail'
printf 'err-tail' >&2`,
		},
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := engine.Start(ctx, payload); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := engine.Wait(ctx); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	var stdoutCount, stderrCount int
	var sawStdoutTail, sawStderrTail bool
	for _, entry := range entries {
		switch entry.Stream {
		case LogStreamStdout:
			stdoutCount++
			if entry.Line == "out-tail" {
				sawStdoutTail = true
			}
		case LogStreamStderr:
			stderrCount++
			if entry.Line == "err-tail" {
				sawStderrTail = true
			}
		}
	}

	if stdoutCount != 257 {
		t.Fatalf("stdout entries = %d, want 257", stdoutCount)
	}
	if stderrCount != 257 {
		t.Fatalf("stderr entries = %d, want 257", stderrCount)
	}
	if !sawStdoutTail {
		t.Fatal("missing trailing stdout line")
	}
	if !sawStderrTail {
		t.Fatal("missing trailing stderr line")
	}
	if strings.Contains(logBuffer.String(), "failed to read exec output") {
		t.Fatalf("unexpected output read warning in logs: %s", logBuffer.String())
	}
}

func TestMountSingleStorageReportsProbeFailureDetails(t *testing.T) {
	tempDir := t.TempDir()
	prependFakeCommand(t, tempDir, "s3fs", "#!/bin/sh\nexit 0\n")
	prependFakeCommand(t, tempDir, "fusermount3", "#!/bin/sh\nexit 0\n")

	var logBuffer bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuffer, nil))
	engine := NewExecEngine(logger)

	previousInspector := inspectMountedStorage
	inspectMountedStorage = func(mountPath string) storageMountProbeResult {
		return storageMountProbeResult{
			PathExists: true,
			PathMode:   os.ModeDir | 0o755,
			MountInfo: &storageMountInfo{
				MountPoint:   mountPath,
				Filesystem:   "fuse.s3fs",
				Source:       "arco-test:/team-a/imagenet",
				MountOptions: "rw,nosuid,nodev,relatime",
				SuperOptions: "rw,user_id=0,group_id=0",
			},
			ProbeError: errors.New("read directory entries: readdirent: software caused connection abort"),
		}
	}
	defer func() {
		inspectMountedStorage = previousInspector
	}()

	_, err := engine.mountSingleStorage(context.Background(), StorageMount{
		Bucket:          "arco-test",
		Prefix:          "team-a/imagenet",
		Endpoint:        "https://s3.example.com",
		Region:          "us-east-1",
		AccessKeyID:     "ak",
		SecretAccessKey: "sk",
		UsePathStyle:    true,
		UseSSL:          true,
		ExtraOptions:    []string{"listobjectsv2"},
		MountPath:       filepath.Join(tempDir, "mnt"),
	})
	if err == nil {
		t.Fatal("mountSingleStorage() error = nil, want detailed probe failure")
	}

	message := err.Error()
	for _, fragment := range []string{
		`s3fs reported success but mount path`,
		`software caused connection abort`,
		`bucket="arco-test"`,
		`endpoint="https://s3.example.com"`,
		`prefix="team-a/imagenet"`,
		`region="us-east-1"`,
		`use_path_style=true`,
		`mount_source="arco-test:/team-a/imagenet"`,
		`mount_fs="fuse.s3fs"`,
	} {
		if !strings.Contains(message, fragment) {
			t.Fatalf("mountSingleStorage() error = %q, want fragment %q", message, fragment)
		}
	}

	if !strings.Contains(logBuffer.String(), "s3-compatible storage mount failed validation") {
		t.Fatalf("expected validation failure log, got %s", logBuffer.String())
	}
}

func TestMountSingleStorageReportsMissingMountEntry(t *testing.T) {
	tempDir := t.TempDir()
	prependFakeCommand(t, tempDir, "s3fs", "#!/bin/sh\nexit 0\n")

	var logBuffer bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuffer, nil))
	engine := NewExecEngine(logger)

	previousInspector := inspectMountedStorage
	inspectMountedStorage = func(string) storageMountProbeResult {
		return storageMountProbeResult{
			PathExists: true,
			PathMode:   os.ModeDir | 0o755,
		}
	}
	defer func() {
		inspectMountedStorage = previousInspector
	}()

	_, err := engine.mountSingleStorage(context.Background(), StorageMount{
		Bucket:          "arco-test",
		Endpoint:        "https://s3.example.com",
		AccessKeyID:     "ak",
		SecretAccessKey: "sk",
		MountPath:       filepath.Join(tempDir, "mnt"),
	})
	if err == nil {
		t.Fatal("mountSingleStorage() error = nil, want missing mount entry failure")
	}

	message := err.Error()
	if !strings.Contains(message, "not present in /proc/self/mountinfo") {
		t.Fatalf("mountSingleStorage() error = %q, want missing mountinfo detail", message)
	}
	if !strings.Contains(message, `mount_entry="missing"`) {
		t.Fatalf("mountSingleStorage() error = %q, want missing mount entry diagnostics", message)
	}
}

func prependFakeCommand(t *testing.T, dir string, name string, body string) {
	t.Helper()

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}

	currentPath := os.Getenv("PATH")
	if currentPath == "" {
		t.Setenv("PATH", dir)
		return
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+currentPath)
}
