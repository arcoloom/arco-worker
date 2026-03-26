package runtime

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	goRuntime "runtime"
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
	prependFakeCommand(t, tempDir, "rclone", "#!/bin/sh\nexit 0\n")
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
				Filesystem:   "fuse.rclone",
				Source:       "arco-test/team-a/imagenet",
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
		MountPath:       filepath.Join(tempDir, "mnt"),
	})
	if err == nil {
		t.Fatal("mountSingleStorage() error = nil, want detailed probe failure")
	}

	message := err.Error()
	for _, fragment := range []string{
		`storage mount command reported success but mount path`,
		`software caused connection abort`,
		`bucket="arco-test"`,
		`endpoint="https://s3.example.com"`,
		`prefix="team-a/imagenet"`,
		`region="us-east-1"`,
		`use_path_style=true`,
		`mount_source="arco-test/team-a/imagenet"`,
		`mount_fs="fuse.rclone"`,
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
	prependFakeCommand(t, tempDir, "rclone", "#!/bin/sh\nexit 0\n")
	prependFakeCommand(t, tempDir, "fusermount3", "#!/bin/sh\nexit 0\n")

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

func TestCheckStorageToolsDownloadsRcloneAutomatically(t *testing.T) {
	if goRuntime.GOOS != "linux" {
		t.Skip("automatic rclone download is only supported on linux")
	}

	tempDir := t.TempDir()
	prependFakeCommand(t, tempDir, "fusermount3", "#!/bin/sh\nexit 0\n")
	t.Setenv("PATH", tempDir)

	archivePath := "/rclone-current-linux-" + currentRcloneTestArch(t) + ".zip"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != archivePath {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "application/zip")
		if err := writeTestRcloneArchive(writer); err != nil {
			t.Fatalf("writeTestRcloneArchive() error = %v", err)
		}
	}))
	defer server.Close()

	previousDownloadBaseURL := rcloneDownloadBaseURL
	previousInstallRoot := storageToolInstallRootOverride
	rcloneDownloadBaseURL = server.URL
	storageToolInstallRootOverride = filepath.Join(tempDir, "installed-rclone")
	t.Cleanup(func() {
		rcloneDownloadBaseURL = previousDownloadBaseURL
		storageToolInstallRootOverride = previousInstallRoot
	})

	var logBuffer bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuffer, nil))

	if err := checkStorageTools(context.Background(), logger, []StorageMount{{MountPath: "/mnt/data"}}); err != nil {
		t.Fatalf("checkStorageTools() error = %v", err)
	}

	installedPath := filepath.Join(storageToolInstallRootOverride, storageMountTool)
	info, err := os.Stat(installedPath)
	if err != nil {
		t.Fatalf("Stat(%q) error = %v", installedPath, err)
	}
	if info.Mode()&0o111 == 0 {
		t.Fatalf("installed rclone mode = %#o, want executable bit set", info.Mode())
	}
	if !strings.Contains(logBuffer.String(), "downloaded rclone automatically") {
		t.Fatalf("expected download log entry, got %s", logBuffer.String())
	}
}

func TestBuildRcloneRemoteSpecIncludesManagedOptions(t *testing.T) {
	spec := buildRcloneRemoteSpec(StorageMount{
		Bucket:          "bucket-a",
		Prefix:          "team-a/data",
		Endpoint:        "https://s3.example.com",
		Region:          "us-east-1",
		AccessKeyID:     "ak",
		SecretAccessKey: "sk",
		SessionToken:    "token-123",
		UsePathStyle:    true,
	})

	for _, fragment := range []string{
		`:s3,`,
		`provider="Other"`,
		`access_key_id="ak"`,
		`secret_access_key="sk"`,
		`endpoint="https://s3.example.com"`,
		`region="us-east-1"`,
		`session_token="token-123"`,
		`force_path_style=true`,
		`:bucket-a/team-a/data`,
	} {
		if !strings.Contains(spec, fragment) {
			t.Fatalf("buildRcloneRemoteSpec() = %q, want fragment %q", spec, fragment)
		}
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

func currentRcloneTestArch(t *testing.T) string {
	t.Helper()

	switch goRuntime.GOARCH {
	case "amd64":
		return "amd64"
	case "arm64":
		return "arm64"
	default:
		t.Fatalf("unsupported GOARCH for rclone test fixture: %s", goRuntime.GOARCH)
		return ""
	}
}

func writeTestRcloneArchive(writer io.Writer) error {
	zipWriter := zip.NewWriter(writer)
	fileWriter, err := zipWriter.Create("rclone-test/rclone")
	if err != nil {
		return err
	}
	if _, err := fileWriter.Write([]byte("#!/bin/sh\nexit 0\n")); err != nil {
		return err
	}
	return zipWriter.Close()
}
