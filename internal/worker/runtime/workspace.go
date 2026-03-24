package runtime

import (
	"archive/tar"
	"archive/zip"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	"github.com/bodgit/sevenzip"
	"github.com/klauspost/compress/zstd"
	"github.com/nwaples/rardecode"
	"github.com/ulikunitz/xz"
)

const containerWorkspaceRoot = "/workspace"

type preparedWorkspace struct {
	hostRoot         string
	hostWorkDir      string
	containerRoot    string
	containerWorkDir string
}

func prepareWorkspace(
	ctx context.Context,
	logger *slog.Logger,
	workspaceRoot string,
	source *SourceSpec,
	runWorkDir string,
) (preparedWorkspace, error) {
	if err := ctx.Err(); err != nil {
		return preparedWorkspace{}, err
	}

	trimmedRoot := strings.TrimSpace(workspaceRoot)
	if trimmedRoot == "" {
		return preparedWorkspace{}, fmt.Errorf("workspace_root is required")
	}
	hostRoot := filepath.Clean(trimmedRoot)
	if err := os.MkdirAll(hostRoot, 0o755); err != nil {
		return preparedWorkspace{}, fmt.Errorf("create workspace root %q: %w", hostRoot, err)
	}
	if err := clearDirectoryContents(hostRoot); err != nil {
		return preparedWorkspace{}, err
	}

	if source != nil && strings.TrimSpace(source.Kind) != "" {
		if err := materializeSource(ctx, logger, hostRoot, source); err != nil {
			return preparedWorkspace{}, err
		}
	}

	hostWorkDir, err := resolveHostWorkDir(hostRoot, sourcePath(source), runWorkDir)
	if err != nil {
		return preparedWorkspace{}, err
	}
	if err := ensureDirectory(hostWorkDir, "host work dir"); err != nil {
		return preparedWorkspace{}, err
	}

	containerWorkDir, err := resolveContainerWorkDir(sourcePath(source), runWorkDir)
	if err != nil {
		return preparedWorkspace{}, err
	}

	return preparedWorkspace{
		hostRoot:         hostRoot,
		hostWorkDir:      hostWorkDir,
		containerRoot:    containerWorkspaceRoot,
		containerWorkDir: containerWorkDir,
	}, nil
}

func sourcePath(source *SourceSpec) string {
	if source == nil {
		return ""
	}
	return strings.TrimSpace(source.Path)
}

func clearDirectoryContents(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("read workspace root %q: %w", root, err)
	}
	for _, entry := range entries {
		target := filepath.Join(root, entry.Name())
		if err := os.RemoveAll(target); err != nil {
			return fmt.Errorf("clear workspace path %q: %w", target, err)
		}
	}
	return nil
}

func materializeSource(ctx context.Context, logger *slog.Logger, workspaceRoot string, source *SourceSpec) error {
	if source == nil {
		return nil
	}

	kind := strings.ToLower(strings.TrimSpace(source.Kind))
	switch kind {
	case "":
		return nil
	case "git":
		return materializeGitSource(ctx, logger, workspaceRoot, source)
	case "archive":
		return materializeArchiveSource(ctx, logger, workspaceRoot, source)
	default:
		return fmt.Errorf("unsupported source kind %q", source.Kind)
	}
}

func materializeGitSource(ctx context.Context, logger *slog.Logger, workspaceRoot string, source *SourceSpec) error {
	if _, err := exec.LookPath("git"); err != nil {
		return fmt.Errorf("git source requires git to be installed: %w", err)
	}

	uri := strings.TrimSpace(source.URI)
	if uri == "" {
		return fmt.Errorf("git source uri is required")
	}
	if logger != nil {
		logger.InfoContext(ctx, "materializing git source", slog.String("uri", uri), slog.String("workspace_root", workspaceRoot))
	}

	if output, err := exec.CommandContext(ctx, "git", "clone", uri, workspaceRoot).CombinedOutput(); err != nil {
		return fmt.Errorf("git clone %q: %w: %s", uri, err, strings.TrimSpace(string(output)))
	}

	revision := strings.TrimSpace(source.Revision)
	if revision == "" {
		return nil
	}
	if output, err := exec.CommandContext(ctx, "git", "-C", workspaceRoot, "checkout", revision).CombinedOutput(); err != nil {
		return fmt.Errorf("git checkout %q: %w: %s", revision, err, strings.TrimSpace(string(output)))
	}
	return nil
}

func materializeArchiveSource(ctx context.Context, logger *slog.Logger, workspaceRoot string, source *SourceSpec) error {
	uri := strings.TrimSpace(source.URI)
	if uri == "" {
		return fmt.Errorf("archive source uri is required")
	}
	if logger != nil {
		logger.InfoContext(ctx, "materializing archive source", slog.String("uri", uri), slog.String("workspace_root", workspaceRoot))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return fmt.Errorf("build archive request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("download archive %q: %w", uri, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("download archive %q: unexpected status %s", uri, resp.Status)
	}

	tmpFile, err := os.CreateTemp("", "arco-archive-*")
	if err != nil {
		return fmt.Errorf("create temp archive file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)
	defer tmpFile.Close()

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		return fmt.Errorf("write temp archive file: %w", err)
	}

	if err := extractArchive(ctx, tmpPath, uri, workspaceRoot); err != nil {
		return err
	}
	return nil
}

type archiveExtractor struct {
	name     string
	suffixes []string
	extract  func(context.Context, string, string, string) error
}

type archiveReaderOpener func(io.Reader) (io.ReadCloser, error)

func extractArchive(ctx context.Context, archivePath string, sourceURI string, workspaceRoot string) error {
	lowerURI := strings.ToLower(strings.TrimSpace(sourceURI))
	extractors := []archiveExtractor{
		{
			name:     "zip",
			suffixes: []string{".zip", ".jar", ".whl", ".apk", ".ipa"},
			extract: func(_ context.Context, archivePath string, _ string, workspaceRoot string) error {
				return extractZipArchive(archivePath, workspaceRoot)
			},
		},
		{
			name:     "tar.gz",
			suffixes: []string{".tar.gz", ".tgz"},
			extract: func(_ context.Context, archivePath string, _ string, workspaceRoot string) error {
				return extractCompressedTarArchive(archivePath, workspaceRoot, gzipArchiveReader)
			},
		},
		{
			name:     "tar.bz2",
			suffixes: []string{".tar.bz2", ".tbz2", ".tbz"},
			extract: func(_ context.Context, archivePath string, _ string, workspaceRoot string) error {
				return extractCompressedTarArchive(archivePath, workspaceRoot, bzip2ArchiveReader)
			},
		},
		{
			name:     "tar.xz",
			suffixes: []string{".tar.xz", ".txz"},
			extract: func(_ context.Context, archivePath string, _ string, workspaceRoot string) error {
				return extractCompressedTarArchive(archivePath, workspaceRoot, xzArchiveReader)
			},
		},
		{
			name:     "tar.zst",
			suffixes: []string{".tar.zst", ".tzst"},
			extract: func(_ context.Context, archivePath string, _ string, workspaceRoot string) error {
				return extractCompressedTarArchive(archivePath, workspaceRoot, zstdArchiveReader)
			},
		},
		{
			name:     "tar",
			suffixes: []string{".tar"},
			extract: func(_ context.Context, archivePath string, _ string, workspaceRoot string) error {
				return extractTarArchive(archivePath, workspaceRoot)
			},
		},
		{
			name:     "gzip",
			suffixes: []string{".gz"},
			extract: func(_ context.Context, archivePath string, sourceURI string, workspaceRoot string) error {
				return extractSingleCompressedFile(archivePath, sourceURI, workspaceRoot, []string{".gz"}, gzipArchiveReader)
			},
		},
		{
			name:     "bzip2",
			suffixes: []string{".bz2", ".bz"},
			extract: func(_ context.Context, archivePath string, sourceURI string, workspaceRoot string) error {
				return extractSingleCompressedFile(archivePath, sourceURI, workspaceRoot, []string{".bz2", ".bz"}, bzip2ArchiveReader)
			},
		},
		{
			name:     "xz",
			suffixes: []string{".xz"},
			extract: func(_ context.Context, archivePath string, sourceURI string, workspaceRoot string) error {
				return extractSingleCompressedFile(archivePath, sourceURI, workspaceRoot, []string{".xz"}, xzArchiveReader)
			},
		},
		{
			name:     "zstd",
			suffixes: []string{".zst"},
			extract: func(_ context.Context, archivePath string, sourceURI string, workspaceRoot string) error {
				return extractSingleCompressedFile(archivePath, sourceURI, workspaceRoot, []string{".zst"}, zstdArchiveReader)
			},
		},
		{
			name:     "7z",
			suffixes: []string{".7z"},
			extract:  extractSevenZipArchive,
		},
		{
			name:     "rar",
			suffixes: []string{".rar"},
			extract:  extractRARArchive,
		},
	}

	for _, extractor := range extractors {
		if matchesArchiveSuffix(lowerURI, extractor.suffixes) {
			return extractor.extract(ctx, archivePath, sourceURI, workspaceRoot)
		}
	}

	errorsByExtractor := make([]error, 0, len(extractors))
	for _, extractor := range extractors {
		if err := extractor.extract(ctx, archivePath, sourceURI, workspaceRoot); err == nil {
			return nil
		} else {
			errorsByExtractor = append(errorsByExtractor, fmt.Errorf("%s: %w", extractor.name, err))
		}
	}

	return fmt.Errorf("archive %q is not a supported payload: %w", sourceURI, errors.Join(errorsByExtractor...))
}

func matchesArchiveSuffix(value string, suffixes []string) bool {
	for _, suffix := range suffixes {
		if strings.HasSuffix(value, suffix) {
			return true
		}
	}
	return false
}

func extractZipArchive(archivePath string, workspaceRoot string) error {
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	for _, file := range reader.File {
		mode := file.Mode()
		if mode.IsDir() || mode&os.ModeSymlink != 0 || strings.HasSuffix(file.Name, "/") {
			if err := extractArchiveEntry(workspaceRoot, file.Name, mode, nil); err != nil {
				return fmt.Errorf("extract zip entry %q: %w", file.Name, err)
			}
			continue
		}

		rc, err := file.Open()
		if err != nil {
			return fmt.Errorf("open zip file %q: %w", file.Name, err)
		}
		if err := extractArchiveEntry(workspaceRoot, file.Name, mode, rc); err != nil {
			rc.Close()
			return fmt.Errorf("extract zip entry %q: %w", file.Name, err)
		}
		if err := rc.Close(); err != nil {
			return fmt.Errorf("close zip file %q: %w", file.Name, err)
		}
	}
	return nil
}

func extractCompressedTarArchive(archivePath string, workspaceRoot string, opener archiveReaderOpener) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader, err := opener(file)
	if err != nil {
		return err
	}
	defer reader.Close()

	return extractTarReader(reader, workspaceRoot)
}

func extractTarArchive(archivePath string, workspaceRoot string) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()

	return extractTarReader(file, workspaceRoot)
}

func extractTarReader(reader io.Reader, workspaceRoot string) error {
	tarReader := tar.NewReader(reader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		mode := os.FileMode(header.Mode)
		switch header.Typeflag {
		case tar.TypeDir:
			mode |= os.ModeDir
		case tar.TypeSymlink:
			mode |= os.ModeSymlink
		case tar.TypeReg, tar.TypeRegA:
		default:
			return fmt.Errorf("tar archive contains unsupported entry %q", header.Name)
		}
		if err := extractArchiveEntry(workspaceRoot, header.Name, mode, tarReader); err != nil {
			return fmt.Errorf("extract tar entry %q: %w", header.Name, err)
		}
	}
}

func extractSingleCompressedFile(
	archivePath string,
	sourceURI string,
	workspaceRoot string,
	suffixes []string,
	opener archiveReaderOpener,
) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader, err := opener(file)
	if err != nil {
		return err
	}
	defer reader.Close()

	targetName := singleCompressedOutputName(sourceURI, suffixes)
	return extractArchiveEntry(workspaceRoot, targetName, 0o644, reader)
}

func gzipArchiveReader(reader io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(reader)
}

func bzip2ArchiveReader(reader io.Reader) (io.ReadCloser, error) {
	return nopArchiveReadCloser{Reader: bzip2.NewReader(reader)}, nil
}

func xzArchiveReader(reader io.Reader) (io.ReadCloser, error) {
	xzReader, err := xz.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return nopArchiveReadCloser{Reader: xzReader}, nil
}

func zstdArchiveReader(reader io.Reader) (io.ReadCloser, error) {
	decoder, err := zstd.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return closeFuncReadCloser{
		Reader: decoder,
		closeFn: func() error {
			decoder.Close()
			return nil
		},
	}, nil
}

type nopArchiveReadCloser struct {
	io.Reader
}

func (nopArchiveReadCloser) Close() error {
	return nil
}

type closeFuncReadCloser struct {
	io.Reader
	closeFn func() error
}

func (r closeFuncReadCloser) Close() error {
	if r.closeFn == nil {
		return nil
	}
	return r.closeFn()
}

func extractSevenZipArchive(_ context.Context, archivePath string, _ string, workspaceRoot string) error {
	reader, err := sevenzip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	for _, file := range reader.File {
		mode := os.FileMode(file.Mode())
		if mode.IsDir() || mode&os.ModeSymlink != 0 || strings.HasSuffix(file.Name, "/") {
			if err := extractArchiveEntry(workspaceRoot, file.Name, mode, nil); err != nil {
				return fmt.Errorf("extract 7z entry %q: %w", file.Name, err)
			}
			continue
		}

		rc, err := file.Open()
		if err != nil {
			return fmt.Errorf("open 7z file %q: %w", file.Name, err)
		}
		if err := extractArchiveEntry(workspaceRoot, file.Name, mode, rc); err != nil {
			rc.Close()
			return fmt.Errorf("extract 7z entry %q: %w", file.Name, err)
		}
		if err := rc.Close(); err != nil {
			return fmt.Errorf("close 7z file %q: %w", file.Name, err)
		}
	}

	return nil
}

func extractRARArchive(_ context.Context, archivePath string, _ string, workspaceRoot string) error {
	reader, err := rardecode.OpenReader(archivePath, "")
	if err != nil {
		return err
	}
	defer reader.Close()

	for {
		header, err := reader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if err := extractArchiveEntry(workspaceRoot, header.Name, header.Mode(), reader); err != nil {
			return fmt.Errorf("extract rar entry %q: %w", header.Name, err)
		}
	}
}

func extractArchiveEntry(workspaceRoot string, entryName string, mode os.FileMode, reader io.Reader) error {
	targetPath, err := safeArchiveTarget(workspaceRoot, entryName)
	if err != nil {
		return err
	}

	switch {
	case mode.IsDir() || strings.HasSuffix(strings.TrimSpace(entryName), "/"):
		if err := os.MkdirAll(targetPath, 0o755); err != nil {
			return fmt.Errorf("create archive directory %q: %w", targetPath, err)
		}
	case mode&os.ModeSymlink != 0:
		return fmt.Errorf("archive contains unsupported symlink %q", entryName)
	default:
		if reader == nil {
			return fmt.Errorf("archive entry %q is missing content", entryName)
		}
		if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
			return fmt.Errorf("create parent directory for %q: %w", targetPath, err)
		}
		if err := writeArchiveFile(targetPath, mode.Perm(), reader); err != nil {
			return err
		}
	}

	return nil
}

func singleCompressedOutputName(sourceURI string, suffixes []string) string {
	name := archiveBaseName(sourceURI)
	lowerName := strings.ToLower(name)
	for _, suffix := range suffixes {
		if strings.HasSuffix(lowerName, suffix) {
			name = name[:len(name)-len(suffix)]
			break
		}
	}

	name = strings.TrimSpace(path.Base(name))
	if name == "" || name == "." || name == "/" {
		return "archive.bin"
	}
	return name
}

func archiveBaseName(sourceURI string) string {
	trimmed := strings.TrimSpace(sourceURI)
	if trimmed == "" {
		return "archive"
	}

	if parsed, err := url.Parse(trimmed); err == nil {
		base := strings.TrimSpace(path.Base(parsed.Path))
		if base != "" && base != "." && base != "/" {
			return base
		}
	}

	trimmed = strings.TrimRight(trimmed, "/")
	base := strings.TrimSpace(path.Base(trimmed))
	if base == "" || base == "." || base == "/" {
		return "archive"
	}
	return base
}

func writeArchiveFile(targetPath string, mode os.FileMode, reader io.Reader) error {
	file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, defaultFileMode(mode))
	if err != nil {
		return fmt.Errorf("create archive file %q: %w", targetPath, err)
	}
	defer file.Close()

	if _, err := io.Copy(file, reader); err != nil {
		return fmt.Errorf("write archive file %q: %w", targetPath, err)
	}
	return nil
}

func defaultFileMode(mode os.FileMode) os.FileMode {
	if mode == 0 {
		return 0o644
	}
	return mode
}

func safeArchiveTarget(workspaceRoot string, archivePath string) (string, error) {
	cleaned := path.Clean(strings.TrimSpace(archivePath))
	switch {
	case cleaned == ".", cleaned == "":
		return filepath.Clean(workspaceRoot), nil
	case strings.HasPrefix(cleaned, "../"), cleaned == "..", path.IsAbs(cleaned):
		return "", fmt.Errorf("archive entry %q escapes the workspace root", archivePath)
	}
	return filepath.Join(workspaceRoot, filepath.FromSlash(cleaned)), nil
}

func resolveHostWorkDir(workspaceRoot string, sourcePath string, runWorkDir string) (string, error) {
	baseDir, err := resolveHostSubdir(workspaceRoot, sourcePath)
	if err != nil {
		return "", err
	}

	trimmed := strings.TrimSpace(runWorkDir)
	if trimmed == "" {
		return baseDir, nil
	}
	if filepath.IsAbs(trimmed) {
		return filepath.Clean(trimmed), nil
	}
	return resolveHostSubdir(baseDir, trimmed)
}

func resolveContainerWorkDir(sourcePath string, runWorkDir string) (string, error) {
	baseDir, err := resolveContainerSubdir(containerWorkspaceRoot, sourcePath)
	if err != nil {
		return "", err
	}

	trimmed := strings.TrimSpace(runWorkDir)
	if trimmed == "" {
		return baseDir, nil
	}
	if path.IsAbs(trimmed) {
		return path.Clean(trimmed), nil
	}
	return resolveContainerSubdir(baseDir, trimmed)
}

func resolveHostSubdir(root string, subdir string) (string, error) {
	trimmed := strings.TrimSpace(subdir)
	if trimmed == "" || trimmed == "." {
		return filepath.Clean(root), nil
	}
	cleaned := filepath.Clean(trimmed)
	if filepath.IsAbs(cleaned) || cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("path %q escapes the workspace root", subdir)
	}
	return filepath.Join(root, cleaned), nil
}

func resolveContainerSubdir(root string, subdir string) (string, error) {
	trimmed := strings.TrimSpace(subdir)
	if trimmed == "" || trimmed == "." {
		return path.Clean(root), nil
	}
	cleaned := path.Clean(trimmed)
	if path.IsAbs(cleaned) || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("path %q escapes the container workspace root", subdir)
	}
	return path.Join(root, cleaned), nil
}

func ensureDirectory(target string, label string) error {
	info, err := os.Stat(target)
	if err != nil {
		return fmt.Errorf("stat %s %q: %w", label, target, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s %q is not a directory", label, target)
	}
	return nil
}
