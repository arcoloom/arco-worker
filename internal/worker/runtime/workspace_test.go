package runtime

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

type archiveTestEntry struct {
	name    string
	content string
}

func TestExtractArchive_SupportedFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sourceURI    string
		create       func(string, []archiveTestEntry) error
		expectedFile string
		expectedBody string
	}{
		{
			name:         "zip",
			sourceURI:    "https://example.com/source.zip",
			create:       createZipArchive,
			expectedFile: "app/main.txt",
			expectedBody: "hello zip",
		},
		{
			name:         "tar.gz",
			sourceURI:    "https://example.com/source.tar.gz",
			create:       createTarGzArchive,
			expectedFile: "app/main.txt",
			expectedBody: "hello tar.gz",
		},
		{
			name:         "tar.xz",
			sourceURI:    "https://example.com/source.tar.xz",
			create:       createTarXZArchive,
			expectedFile: "app/main.txt",
			expectedBody: "hello tar.xz",
		},
		{
			name:         "tar.zst",
			sourceURI:    "https://example.com/source.tar.zst",
			create:       createTarZstdArchive,
			expectedFile: "app/main.txt",
			expectedBody: "hello tar.zst",
		},
		{
			name:         "gzip-single",
			sourceURI:    "https://example.com/notes.txt.gz?download=1",
			create:       createGzipSingleFileArchive,
			expectedFile: "notes.txt",
			expectedBody: "hello gzip",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			archivePath := filepath.Join(t.TempDir(), "archive.bin")
			workspaceRoot := filepath.Join(t.TempDir(), "workspace")
			if err := os.MkdirAll(workspaceRoot, 0o755); err != nil {
				t.Fatalf("mkdir workspace: %v", err)
			}

			entries := []archiveTestEntry{{name: "app/main.txt", content: tt.expectedBody}}
			if tt.name == "gzip-single" {
				entries = []archiveTestEntry{{name: "notes.txt", content: tt.expectedBody}}
			}
			if err := tt.create(archivePath, entries); err != nil {
				t.Fatalf("create archive: %v", err)
			}

			if err := extractArchive(context.Background(), archivePath, tt.sourceURI, workspaceRoot); err != nil {
				t.Fatalf("extract archive: %v", err)
			}

			body, err := os.ReadFile(filepath.Join(workspaceRoot, tt.expectedFile))
			if err != nil {
				t.Fatalf("read extracted file: %v", err)
			}
			if string(body) != tt.expectedBody {
				t.Fatalf("extracted body = %q, want %q", string(body), tt.expectedBody)
			}
		})
	}
}

func TestExtractArchive_RejectsEscapingEntries(t *testing.T) {
	t.Parallel()

	archivePath := filepath.Join(t.TempDir(), "escape.zip")
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspaceRoot, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}

	file, err := os.Create(archivePath)
	if err != nil {
		t.Fatalf("create zip: %v", err)
	}
	writer := zip.NewWriter(file)
	entry, err := writer.Create("../escape.txt")
	if err != nil {
		t.Fatalf("create escaping entry: %v", err)
	}
	if _, err := entry.Write([]byte("bad")); err != nil {
		t.Fatalf("write escaping entry: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close zip writer: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close zip file: %v", err)
	}

	if err := extractArchive(context.Background(), archivePath, "https://example.com/escape.zip", workspaceRoot); err == nil {
		t.Fatal("expected extraction to fail for escaping entry")
	}
}

func TestPrepareWorkspaceCreatesRelativeWorkDir(t *testing.T) {
	t.Parallel()

	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	runWorkDir := filepath.Join("app", "build")

	workspace, err := prepareWorkspace(context.Background(), nil, workspaceRoot, nil, runWorkDir)
	if err != nil {
		t.Fatalf("prepareWorkspace() error = %v", err)
	}

	wantHostWorkDir := filepath.Join(workspaceRoot, runWorkDir)
	if workspace.hostWorkDir != wantHostWorkDir {
		t.Fatalf("prepareWorkspace() hostWorkDir = %q, want %q", workspace.hostWorkDir, wantHostWorkDir)
	}

	info, err := os.Stat(workspace.hostWorkDir)
	if err != nil {
		t.Fatalf("os.Stat(%q) error = %v", workspace.hostWorkDir, err)
	}
	if !info.IsDir() {
		t.Fatalf("hostWorkDir %q is not a directory", workspace.hostWorkDir)
	}
}

func TestPrepareWorkspaceCreatesAbsoluteWorkDir(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	workspaceRoot := filepath.Join(root, "workspace")
	runWorkDir := filepath.Join(root, "runner", "install")

	workspace, err := prepareWorkspace(context.Background(), nil, workspaceRoot, nil, runWorkDir)
	if err != nil {
		t.Fatalf("prepareWorkspace() error = %v", err)
	}

	wantHostWorkDir := filepath.Clean(runWorkDir)
	if workspace.hostWorkDir != wantHostWorkDir {
		t.Fatalf("prepareWorkspace() hostWorkDir = %q, want %q", workspace.hostWorkDir, wantHostWorkDir)
	}

	info, err := os.Stat(workspace.hostWorkDir)
	if err != nil {
		t.Fatalf("os.Stat(%q) error = %v", workspace.hostWorkDir, err)
	}
	if !info.IsDir() {
		t.Fatalf("hostWorkDir %q is not a directory", workspace.hostWorkDir)
	}
}

func createZipArchive(archivePath string, entries []archiveTestEntry) error {
	file, err := os.Create(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := zip.NewWriter(file)
	for _, entry := range entries {
		target, err := writer.Create(entry.name)
		if err != nil {
			writer.Close()
			return err
		}
		if _, err := target.Write([]byte(entry.content)); err != nil {
			writer.Close()
			return err
		}
	}
	return writer.Close()
}

func createTarGzArchive(archivePath string, entries []archiveTestEntry) error {
	file, err := os.Create(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()

	gzipWriter := gzip.NewWriter(file)
	defer gzipWriter.Close()

	return writeTarArchive(gzipWriter, entries)
}

func createTarXZArchive(archivePath string, entries []archiveTestEntry) error {
	file, err := os.Create(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()

	xzWriter, err := xz.NewWriter(file)
	if err != nil {
		return err
	}
	defer xzWriter.Close()

	return writeTarArchive(xzWriter, entries)
}

func createTarZstdArchive(archivePath string, entries []archiveTestEntry) error {
	file, err := os.Create(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()

	zstdWriter, err := zstd.NewWriter(file)
	if err != nil {
		return err
	}
	defer zstdWriter.Close()

	return writeTarArchive(zstdWriter, entries)
}

func createGzipSingleFileArchive(archivePath string, entries []archiveTestEntry) error {
	if len(entries) != 1 {
		return nil
	}

	file, err := os.Create(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()

	gzipWriter := gzip.NewWriter(file)
	if _, err := gzipWriter.Write([]byte(entries[0].content)); err != nil {
		gzipWriter.Close()
		return err
	}
	return gzipWriter.Close()
}

func writeTarArchive(writer io.Writer, entries []archiveTestEntry) error {
	tarWriter := tar.NewWriter(writer)

	for _, entry := range entries {
		header := &tar.Header{
			Name: entry.name,
			Mode: 0o644,
			Size: int64(len(entry.content)),
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}
		if _, err := tarWriter.Write([]byte(entry.content)); err != nil {
			return err
		}
	}

	return tarWriter.Close()
}
