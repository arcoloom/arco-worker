//go:build linux

package runtime

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	goRuntime "runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	runcoptions "github.com/containerd/containerd/api/types/runc/options"
	containerd "github.com/containerd/containerd/v2/client"
	containerdcdi "github.com/containerd/containerd/v2/pkg/cdi"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	officialcdi "tags.cncf.io/container-device-interface/pkg/cdi"
	cdispecs "tags.cncf.io/container-device-interface/specs-go"
)

const (
	managedContainerdNamespace         = "arco-worker"
	managedContainerdRuntimeName       = "io.containerd.runc.v2"
	managedContainerdConfigName        = "config.toml"
	managedContainerdShutdownTimeout   = 5 * time.Second
	managedContainerdConnectTimeout    = 3 * time.Second
	managedContainerdReadyPollInterval = 250 * time.Millisecond
	officialNVIDIACDIVendor            = "nvidia.com"
	officialNVIDIACDIClass             = "gpu"
	managedNVIDIACDIVendor             = "arcoloom.local"
	managedNVIDIACDIClass              = "gpu"
	managedNVIDIAAllDeviceName         = "all"
	managedNVIDIACDISpecName           = "arcoloom.local-gpu.json"
)

var managedContainerdVersion = "2.2.2"
var managedRuncVersion = "1.4.1"

var managedContainerdInstallRootOverride string

var globalManagedContainerd managedContainerdService

type ContainerdEngine struct {
	logger *slog.Logger

	mu              sync.Mutex
	task            containerd.Task
	container       containerd.Container
	runtime         *managedContainerdRuntime
	doneCh          chan struct{}
	waitErr         error
	logEmitter      LogEmitter
	mountedStorage  []mountedStorage
	preparedPayload *ContainerPayload
	containerName   string
}

type managedContainerdService struct {
	mu      sync.Mutex
	runtime *managedContainerdRuntime
}

type managedContainerdRuntime struct {
	logger *slog.Logger
	paths  managedContainerdPaths
	client *containerd.Client
	cmd    *exec.Cmd

	daemonDone chan struct{}
	daemonErr  error
	mu         sync.RWMutex
}

type managedContainerdPaths struct {
	installRoot          string
	containerdVersionDir string
	containerdBinDir     string
	containerdBin        string
	runcVersionDir       string
	runcBin              string
	daemonDir            string
	socketPath           string
	rootDir              string
	stateDir             string
	fifoDir              string
	logPath              string
	configPath           string
	cdiSpecDir           string
	nvidiaCDISpecPath    string
}

var _ Engine = (*ContainerdEngine)(nil)
var _ LogEmitterAware = (*ContainerdEngine)(nil)

func NewContainerdEngine(logger *slog.Logger) *ContainerdEngine {
	if logger == nil {
		logger = slog.Default()
	}

	return &ContainerdEngine{
		logger: logger,
	}
}

func (e *ContainerdEngine) SetLogEmitter(emitter LogEmitter) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.logEmitter = emitter
}

func (e *ContainerdEngine) Prepare(ctx context.Context, payload []byte) error {
	if os.Geteuid() != 0 {
		return errors.New("container runtime requires the worker to run as root on linux")
	}

	containerPayload, err := parseContainerPayload(ctx, payload)
	if err != nil {
		return err
	}

	runtime, err := globalManagedContainerd.ensure(ctx, e.logger)
	if err != nil {
		return err
	}
	if err := validateStorageMounts(containerPayload.Mounts); err != nil {
		return err
	}
	if err := checkStorageTools(ctx, e.logger, containerPayload.Mounts); err != nil {
		return err
	}

	workspace, err := prepareWorkspace(ctx, e.logger, containerPayload.WorkspaceRoot, containerPayload.Source, containerPayload.WorkDir)
	if err != nil {
		return err
	}
	containerPayload.WorkspaceRoot = workspace.hostRoot
	containerPayload.WorkDir = workspace.containerWorkDir

	if resolvedGPUDevices, err := runtime.ensureNVIDIACDI(ctx, containerPayload); err != nil {
		return err
	} else if len(resolvedGPUDevices) > 0 {
		containerPayload.GPUDevices = resolvedGPUDevices
	}

	if _, err := runtime.ensureImage(ctx, containerPayload.Image); err != nil {
		return err
	}

	e.mu.Lock()
	e.preparedPayload = &containerPayload
	e.mu.Unlock()

	return nil
}

func (e *ContainerdEngine) Start(ctx context.Context, payload []byte) error {
	containerPayload, err := e.preparedContainerPayload(ctx, payload)
	if err != nil {
		return err
	}

	runtime, err := globalManagedContainerd.ensure(ctx, e.logger)
	if err != nil {
		return err
	}

	e.mu.Lock()
	logEmitter := e.logEmitter
	e.mu.Unlock()

	logCtx := context.WithoutCancel(ctx)
	mounted, err := e.mountStorage(logCtx, containerPayload.Mounts)
	if err != nil {
		return err
	}

	directoryMounts, err := resolveContainerDirectoryMounts(logCtx, containerPayload.WorkspaceRoot, containerPayload.DirectoryMounts)
	if err != nil {
		cleanupErr := e.cleanupStorage(logCtx, mounted)
		if cleanupErr != nil {
			return errors.Join(err, cleanupErr)
		}
		return err
	}

	containerName := containerTaskName(containerPayload.TaskID)
	if err := runtime.cleanupContainer(logCtx, containerName); err != nil {
		cleanupErr := e.cleanupStorage(logCtx, mounted)
		if cleanupErr != nil {
			return errors.Join(err, cleanupErr)
		}
		return err
	}

	container, task, waitCh, stdoutWriter, stderrWriter, err := runtime.createTask(
		logCtx,
		containerPayload,
		mounted,
		directoryMounts,
		containerName,
		logEmitter,
	)
	if err != nil {
		cleanupErr := e.cleanupStorage(logCtx, mounted)
		return errors.Join(err, cleanupErr)
	}

	e.mu.Lock()
	if e.task != nil {
		e.mu.Unlock()
		cleanupErr := runtime.cleanupTaskAndContainer(logCtx, containerName, container, task)
		storageErr := e.cleanupStorage(logCtx, mounted)
		return errors.Join(errors.New("container workload already started"), cleanupErr, storageErr)
	}

	if err := task.Start(logCtx); err != nil {
		e.mu.Unlock()
		cleanupErr := runtime.cleanupTaskAndContainer(logCtx, containerName, container, task)
		storageErr := e.cleanupStorage(logCtx, mounted)
		startErr := fmt.Errorf("start container %q: %w", containerName, err)
		return errors.Join(startErr, cleanupErr, storageErr)
	}

	e.task = task
	e.container = container
	e.runtime = runtime
	e.doneCh = make(chan struct{})
	e.waitErr = nil
	e.mountedStorage = mounted
	e.preparedPayload = &containerPayload
	e.containerName = containerName

	go e.waitForExit(logCtx, runtime, container, task, waitCh, mounted, containerName, stdoutWriter, stderrWriter)
	e.mu.Unlock()

	e.logger.InfoContext(
		logCtx,
		"container workload started",
		slog.String("container_name", containerName),
		slog.String("image", containerPayload.Image),
		slog.String("work_dir", containerPayload.WorkDir),
		slog.Int("storage_mounts", len(mounted)),
		slog.Int("directory_mounts", len(directoryMounts)),
		slog.Int("gpu_devices", len(containerPayload.GPUDevices)),
	)

	return nil
}

func (e *ContainerdEngine) Stop(ctx context.Context) error {
	return e.signalTask(ctx, syscall.SIGTERM, "SIGTERM")
}

func (e *ContainerdEngine) Interrupt(ctx context.Context) error {
	return e.signalTask(ctx, syscall.SIGINT, "SIGINT")
}

func (e *ContainerdEngine) Wait(ctx context.Context) error {
	e.mu.Lock()
	doneCh := e.doneCh
	e.mu.Unlock()

	if doneCh == nil {
		return errors.New("container workload not started")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-doneCh:
		e.mu.Lock()
		defer e.mu.Unlock()
		return e.waitErr
	}
}

func (e *ContainerdEngine) signalTask(ctx context.Context, signal syscall.Signal, signalName string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	e.mu.Lock()
	task := e.task
	containerName := e.containerName
	e.mu.Unlock()

	if task == nil {
		return nil
	}

	if err := task.Kill(ctx, signal, containerd.WithKillAll); err != nil {
		if errdefs.IsNotFound(err) || errdefs.IsFailedPrecondition(err) {
			return nil
		}
		return fmt.Errorf("send %s to container %q: %w", signalName, containerName, err)
	}

	e.logger.InfoContext(ctx, "container workload stopping", slog.String("container_name", containerName), slog.String("signal", signalName))
	return nil
}

func (e *ContainerdEngine) waitForExit(
	ctx context.Context,
	runtime *managedContainerdRuntime,
	container containerd.Container,
	task containerd.Task,
	waitCh <-chan containerd.ExitStatus,
	mounted []mountedStorage,
	containerName string,
	stdoutWriter *lineEmitterWriter,
	stderrWriter *lineEmitterWriter,
) {
	var waitErr error
	select {
	case status, ok := <-waitCh:
		if !ok {
			waitErr = errors.New("wait for container task: exit status channel closed")
		} else {
			waitErr = normalizeContainerdExitStatus(status)
		}
	case <-ctx.Done():
		waitErr = ctx.Err()
	}

	cleanupErr := runtime.cleanupTaskAndContainer(ctx, containerName, container, task)
	stdoutWriter.Flush()
	stderrWriter.Flush()

	storageErr := e.cleanupStorage(ctx, mounted)
	waitErr = errors.Join(waitErr, cleanupErr, storageErr)
	if waitErr != nil {
		e.logger.ErrorContext(ctx, "container workload finished with error", slog.String("container_name", containerName), slog.String("error", waitErr.Error()))
	} else {
		e.logger.InfoContext(ctx, "container workload finished", slog.String("container_name", containerName))
	}

	e.mu.Lock()
	e.task = nil
	e.container = nil
	e.runtime = nil
	e.waitErr = waitErr
	doneCh := e.doneCh
	e.mountedStorage = nil
	e.preparedPayload = nil
	e.containerName = ""
	e.mu.Unlock()

	if doneCh != nil {
		close(doneCh)
	}
}

func (e *ContainerdEngine) preparedContainerPayload(ctx context.Context, raw []byte) (ContainerPayload, error) {
	e.mu.Lock()
	if e.preparedPayload != nil {
		payload := *e.preparedPayload
		e.mu.Unlock()
		return payload, nil
	}
	e.mu.Unlock()

	if err := e.Prepare(ctx, raw); err != nil {
		return ContainerPayload{}, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.preparedPayload == nil {
		return ContainerPayload{}, errors.New("container payload was not prepared")
	}
	return *e.preparedPayload, nil
}

func (e *ContainerdEngine) mountStorage(ctx context.Context, mounts []StorageMount) ([]mountedStorage, error) {
	execEngine := NewExecEngine(e.logger)
	return execEngine.mountStorage(ctx, mounts)
}

func (e *ContainerdEngine) cleanupStorage(ctx context.Context, mounts []mountedStorage) error {
	execEngine := NewExecEngine(e.logger)
	return execEngine.cleanupStorage(ctx, mounts)
}

func (s *managedContainerdService) ensure(ctx context.Context, logger *slog.Logger) (*managedContainerdRuntime, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.runtime != nil {
		if err := s.runtime.healthy(ctx); err == nil {
			return s.runtime, nil
		}
		_ = s.runtime.shutdown()
		s.runtime = nil
	}

	runtime, err := startManagedContainerdRuntime(ctx, logger)
	if err != nil {
		return nil, err
	}
	s.runtime = runtime
	return runtime, nil
}

func startManagedContainerdRuntime(ctx context.Context, logger *slog.Logger) (*managedContainerdRuntime, error) {
	paths, err := managedContainerdRuntimePaths()
	if err != nil {
		return nil, err
	}

	if err := ensureManagedContainerdArtifacts(ctx, logger, paths); err != nil {
		return nil, err
	}
	if err := ensureManagedContainerdConfig(paths); err != nil {
		return nil, err
	}

	client, err := connectManagedContainerd(ctx, paths)
	if err == nil {
		if err := ensureManagedNamespace(ctx, client); err != nil {
			_ = client.Close()
			return nil, err
		}
		return &managedContainerdRuntime{
			logger: logger,
			paths:  paths,
			client: client,
		}, nil
	}

	if removeErr := os.Remove(paths.socketPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale containerd socket %q: %w", paths.socketPath, removeErr)
	}

	logFile, err := os.OpenFile(paths.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open managed containerd log %q: %w", paths.logPath, err)
	}

	cmd := exec.CommandContext(
		context.Background(),
		paths.containerdBin,
		"--config", paths.configPath,
		"--address", paths.socketPath,
		"--root", paths.rootDir,
		"--state", paths.stateDir,
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = managedContainerdDaemonEnv(paths)
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("start managed containerd daemon: %w", err)
	}
	_ = logFile.Close()

	runtime := &managedContainerdRuntime{
		logger:     logger,
		paths:      paths,
		cmd:        cmd,
		daemonDone: make(chan struct{}),
	}
	go runtime.waitForDaemon()

	client, err = waitForManagedContainerd(ctx, paths)
	if err != nil {
		_ = runtime.shutdown()
		return nil, err
	}

	if err := ensureManagedNamespace(ctx, client); err != nil {
		_ = client.Close()
		_ = runtime.shutdown()
		return nil, err
	}

	runtime.client = client
	return runtime, nil
}

func (r *managedContainerdRuntime) waitForDaemon() {
	err := r.cmd.Wait()
	r.mu.Lock()
	r.daemonErr = err
	r.mu.Unlock()
	close(r.daemonDone)
}

func (r *managedContainerdRuntime) healthy(ctx context.Context) error {
	if r == nil || r.client == nil {
		return errors.New("managed containerd runtime is not initialized")
	}
	if r.cmd != nil {
		select {
		case <-r.daemonDone:
			r.mu.RLock()
			err := r.daemonErr
			r.mu.RUnlock()
			if err != nil {
				return fmt.Errorf("managed containerd daemon exited: %w", err)
			}
			return errors.New("managed containerd daemon exited")
		default:
		}
	}
	_, err := r.client.Version(ctx)
	return err
}

func (r *managedContainerdRuntime) shutdown() error {
	var errs []error
	if r == nil {
		return nil
	}
	if r.client != nil {
		errs = append(errs, r.client.Close())
	}
	if r.cmd == nil {
		return errors.Join(errs...)
	}

	select {
	case <-r.daemonDone:
		r.mu.RLock()
		errs = append(errs, r.daemonErr)
		r.mu.RUnlock()
		return errors.Join(errs...)
	default:
	}

	if r.cmd.Process != nil {
		if err := r.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
			errs = append(errs, err)
		}
	}

	select {
	case <-r.daemonDone:
	case <-time.After(managedContainerdShutdownTimeout):
		if r.cmd.Process != nil {
			if err := r.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				errs = append(errs, err)
			}
		}
		<-r.daemonDone
	}

	r.mu.RLock()
	errs = append(errs, r.daemonErr)
	r.mu.RUnlock()
	return errors.Join(errs...)
}

func (r *managedContainerdRuntime) ensureImage(ctx context.Context, imageRef string) (containerd.Image, error) {
	image, err := r.client.GetImage(ctx, imageRef)
	if err == nil {
		return image, nil
	}
	if !errdefs.IsNotFound(err) {
		return nil, fmt.Errorf("load image %q: %w", imageRef, err)
	}

	if r.logger != nil {
		r.logger.InfoContext(ctx, "pulling container image", slog.String("image", imageRef))
	}
	image, err = r.client.Pull(
		ctx,
		imageRef,
		containerd.WithPullUnpack,
		containerd.WithPlatform(platforms.DefaultString()),
	)
	if err != nil {
		return nil, fmt.Errorf("pull image %q: %w", imageRef, err)
	}
	return image, nil
}

func (r *managedContainerdRuntime) createTask(
	ctx context.Context,
	payload ContainerPayload,
	mountedStorage []mountedStorage,
	directoryMounts []resolvedContainerMount,
	containerName string,
	emitter LogEmitter,
) (containerd.Container, containerd.Task, <-chan containerd.ExitStatus, *lineEmitterWriter, *lineEmitterWriter, error) {
	image, err := r.ensureImage(ctx, payload.Image)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	specOpts, err := containerSpecOpts(payload, mountedStorage, directoryMounts, image)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	container, err := r.client.NewContainer(
		ctx,
		containerName,
		containerd.WithRuntime(managedContainerdRuntimeName, &runcoptions.Options{
			BinaryName: r.paths.runcBin,
		}),
		containerd.WithImage(image),
		containerd.WithNewSnapshot(containerName, image),
		containerd.WithNewSpec(specOpts...),
	)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("create container %q: %w", containerName, err)
	}

	stdoutWriter := newLineEmitterWriter(LogStreamStdout, emitter)
	stderrWriter := newLineEmitterWriter(LogStreamStderr, emitter)
	task, err := container.NewTask(
		ctx,
		cio.NewCreator(
			cio.WithStreams(nil, stdoutWriter, stderrWriter),
			cio.WithFIFODir(r.paths.fifoDir),
		),
	)
	if err != nil {
		deleteErr := container.Delete(ctx, containerd.WithSnapshotCleanup)
		return nil, nil, nil, nil, nil, errors.Join(fmt.Errorf("create task for container %q: %w", containerName, err), deleteErr)
	}

	waitCh, err := task.Wait(ctx)
	if err != nil {
		cleanupErr := r.cleanupTaskAndContainer(ctx, containerName, container, task)
		return nil, nil, nil, nil, nil, errors.Join(fmt.Errorf("wait on task for container %q: %w", containerName, err), cleanupErr)
	}

	return container, task, waitCh, stdoutWriter, stderrWriter, nil
}

func (r *managedContainerdRuntime) cleanupTaskAndContainer(
	ctx context.Context,
	containerName string,
	container containerd.Container,
	task containerd.Task,
) error {
	var errs []error

	if task != nil {
		if _, err := task.Delete(ctx, containerd.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete task for container %q: %w", containerName, err))
		}
	}
	if container != nil {
		if err := container.Delete(ctx, containerd.WithSnapshotCleanup); err != nil && !errdefs.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete container %q: %w", containerName, err))
		}
	}
	return errors.Join(errs...)
}

func (r *managedContainerdRuntime) cleanupContainer(ctx context.Context, containerName string) error {
	container, err := r.client.LoadContainer(ctx, containerName)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("load existing container %q: %w", containerName, err)
	}

	task, err := container.Task(ctx, nil)
	if err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("load existing task for container %q: %w", containerName, err)
	}

	if err := r.cleanupTaskAndContainer(ctx, containerName, container, task); err != nil {
		return err
	}
	return nil
}

func managedContainerdRuntimePaths() (managedContainerdPaths, error) {
	installRoot, err := managedContainerdInstallRoot()
	if err != nil {
		return managedContainerdPaths{}, err
	}

	return managedContainerdPaths{
		installRoot:          installRoot,
		containerdVersionDir: filepath.Join(installRoot, "containerd", managedContainerdVersion),
		containerdBinDir:     filepath.Join(installRoot, "containerd", managedContainerdVersion, "bin"),
		containerdBin:        filepath.Join(installRoot, "containerd", managedContainerdVersion, "bin", "containerd"),
		runcVersionDir:       filepath.Join(installRoot, "runc", managedRuncVersion),
		runcBin:              filepath.Join(installRoot, "runc", managedRuncVersion, "runc"),
		daemonDir:            filepath.Join(installRoot, "daemon"),
		socketPath:           filepath.Join(installRoot, "daemon", "containerd.sock"),
		rootDir:              filepath.Join(installRoot, "daemon", "root"),
		stateDir:             filepath.Join(installRoot, "daemon", "state"),
		fifoDir:              filepath.Join(installRoot, "daemon", "fifo"),
		logPath:              filepath.Join(installRoot, "daemon", "containerd.log"),
		configPath:           filepath.Join(installRoot, "daemon", managedContainerdConfigName),
		cdiSpecDir:           filepath.Join(string(os.PathSeparator), "var", "run", "cdi"),
		nvidiaCDISpecPath:    filepath.Join(string(os.PathSeparator), "var", "run", "cdi", managedNVIDIACDISpecName),
	}, nil
}

func managedContainerdInstallRoot() (string, error) {
	if trimmed := strings.TrimSpace(managedContainerdInstallRootOverride); trimmed != "" {
		return filepath.Clean(trimmed), nil
	}
	if os.Geteuid() == 0 {
		return filepath.Join(string(os.PathSeparator), "var", "lib", "arco", "managed-containerd"), nil
	}
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("resolve user cache dir for managed containerd: %w", err)
	}
	return filepath.Join(cacheDir, "arco", "managed-containerd"), nil
}

func ensureManagedContainerdArtifacts(ctx context.Context, logger *slog.Logger, paths managedContainerdPaths) error {
	if err := os.MkdirAll(paths.installRoot, 0o755); err != nil {
		return fmt.Errorf("create managed runtime root %q: %w", paths.installRoot, err)
	}
	for _, dir := range []string{paths.daemonDir, paths.rootDir, paths.stateDir, paths.fifoDir, paths.cdiSpecDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create managed runtime directory %q: %w", dir, err)
		}
	}

	if err := ensureManagedContainerdBinary(ctx, logger, paths); err != nil {
		return err
	}
	if err := ensureManagedRuncBinary(ctx, logger, paths); err != nil {
		return err
	}
	return nil
}

func ensureManagedContainerdBinary(ctx context.Context, logger *slog.Logger, paths managedContainerdPaths) error {
	required := []string{
		paths.containerdBin,
		filepath.Join(paths.containerdBinDir, "containerd-shim-runc-v2"),
	}
	if allPathsExist(required) {
		return nil
	}

	arch, err := managedContainerdReleaseArch()
	if err != nil {
		return err
	}
	url := fmt.Sprintf(
		"https://github.com/containerd/containerd/releases/download/v%s/containerd-%s-linux-%s.tar.gz",
		managedContainerdVersion,
		managedContainerdVersion,
		arch,
	)
	if logger != nil {
		logger.InfoContext(ctx, "installing managed containerd", slog.String("version", managedContainerdVersion), slog.String("url", url))
	}
	return downloadAndExtractTarGz(ctx, url, paths.containerdVersionDir)
}

func ensureManagedRuncBinary(ctx context.Context, logger *slog.Logger, paths managedContainerdPaths) error {
	if fileExists(paths.runcBin) {
		return nil
	}

	arch, err := managedContainerdReleaseArch()
	if err != nil {
		return err
	}
	url := fmt.Sprintf(
		"https://github.com/opencontainers/runc/releases/download/v%s/runc.%s",
		managedRuncVersion,
		arch,
	)
	if logger != nil {
		logger.InfoContext(ctx, "installing managed runc", slog.String("version", managedRuncVersion), slog.String("url", url))
	}
	return downloadExecutable(ctx, url, paths.runcBin)
}

func ensureManagedContainerdConfig(paths managedContainerdPaths) error {
	config := strings.Join([]string{
		"version = 3",
		`disabled_plugins = ["io.containerd.grpc.v1.cri"]`,
		"",
	}, "\n")
	return writeFileAtomically(paths.configPath, []byte(config), 0o644)
}

func connectManagedContainerd(ctx context.Context, paths managedContainerdPaths) (*containerd.Client, error) {
	connectCtx, cancel := context.WithTimeout(ctx, managedContainerdConnectTimeout)
	defer cancel()

	client, err := containerd.New(
		paths.socketPath,
		containerd.WithTimeout(managedContainerdConnectTimeout),
		containerd.WithDefaultNamespace(managedContainerdNamespace),
		containerd.WithDefaultRuntime(managedContainerdRuntimeName),
	)
	if err != nil {
		return nil, err
	}
	if _, err := client.Version(connectCtx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

func waitForManagedContainerd(ctx context.Context, paths managedContainerdPaths) (*containerd.Client, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	ticker := time.NewTicker(managedContainerdReadyPollInterval)
	defer ticker.Stop()

	for {
		client, err := connectManagedContainerd(deadlineCtx, paths)
		if err == nil {
			return client, nil
		}

		select {
		case <-deadlineCtx.Done():
			return nil, fmt.Errorf("wait for managed containerd: %w", err)
		case <-ticker.C:
		}
	}
}

func ensureManagedNamespace(ctx context.Context, client *containerd.Client) error {
	if err := client.NamespaceService().Create(ctx, managedContainerdNamespace, map[string]string{
		"managed_by": "arco-worker",
	}); err != nil && !errdefs.IsAlreadyExists(err) {
		return fmt.Errorf("create containerd namespace %q: %w", managedContainerdNamespace, err)
	}
	return nil
}

func (r *managedContainerdRuntime) ensureNVIDIACDI(ctx context.Context, payload ContainerPayload) ([]string, error) {
	if !containerGPURequested(payload) {
		return nil, nil
	}

	if officialAvailable, err := availableCDIDeviceNames(officialNVIDIACDIVendor, officialNVIDIACDIClass); err == nil && len(officialAvailable) > 0 {
		selected, resolveErr := resolveCDIDeviceRequests(
			officialNVIDIACDIVendor,
			officialNVIDIACDIClass,
			officialAvailable,
			payload.GPUDevices,
			payload.GPUCount,
		)
		if resolveErr == nil && len(selected) > 0 {
			if r.logger != nil {
				r.logger.InfoContext(
					ctx,
					"using host NVIDIA CDI devices",
					slog.Int("available_devices", len(preferredCDIDeviceNames(officialAvailable))),
					slog.Int("selected_devices", len(selected)),
				)
			}
			return selected, nil
		}
		if resolveErr != nil {
			return nil, resolveErr
		}
	} else if err != nil && r.logger != nil {
		r.logger.WarnContext(ctx, "failed to refresh host CDI registry for NVIDIA devices", slog.String("error", err.Error()))
	}

	spec, available, err := buildManagedNVIDIACDISpec()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(r.paths.cdiSpecDir, 0o755); err != nil {
		return nil, fmt.Errorf("create NVIDIA CDI directory %q: %w", r.paths.cdiSpecDir, err)
	}
	content, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode managed NVIDIA CDI spec: %w", err)
	}
	if err := writeFileAtomically(r.paths.nvidiaCDISpecPath, content, 0o644); err != nil {
		return nil, fmt.Errorf("write NVIDIA CDI spec %q: %w", r.paths.nvidiaCDISpecPath, err)
	}
	if err := officialcdi.Refresh(); err != nil {
		return nil, fmt.Errorf("refresh CDI registry after writing NVIDIA spec: %w", err)
	}

	selected, err := resolveCDIDeviceRequests(
		managedNVIDIACDIVendor,
		managedNVIDIACDIClass,
		available,
		payload.GPUDevices,
		payload.GPUCount,
	)
	if err != nil {
		return nil, err
	}
	if len(selected) == 0 {
		return nil, errors.New("gpu requested but no NVIDIA CDI devices were selected")
	}

	if r.logger != nil {
		r.logger.InfoContext(
			ctx,
			"prepared NVIDIA CDI devices",
			slog.String("spec_path", r.paths.nvidiaCDISpecPath),
			slog.Int("available_devices", len(preferredCDIDeviceNames(available))),
			slog.Int("selected_devices", len(selected)),
		)
	}

	return selected, nil
}

func containerGPURequested(payload ContainerPayload) bool {
	return payload.GPUCount > 0 || len(payload.GPUDevices) > 0
}

func availableCDIDeviceNames(vendor string, class string) ([]string, error) {
	if err := officialcdi.Refresh(); err != nil {
		return nil, err
	}

	prefix := vendor + "/" + class + "="
	devices := officialcdi.GetDefaultCache().ListDevices()
	names := make([]string, 0, len(devices))
	for _, qualified := range devices {
		if strings.HasPrefix(qualified, prefix) {
			names = append(names, strings.TrimPrefix(qualified, prefix))
		}
	}
	return normalizeContainerGPUDeviceRequests(names)
}

func buildManagedNVIDIACDISpec() (*cdispecs.Spec, []string, error) {
	perGPUDevices, err := discoverNVIDIAGPUDevices()
	if err != nil {
		return nil, nil, err
	}
	if len(perGPUDevices) == 0 {
		return nil, nil, errors.New("gpu requested but no /dev/nvidia[0-9]* devices were found")
	}

	commonDeviceNodes := discoverCommonNVIDIADeviceNodes()
	mounts := discoverManagedNVIDIADriverMounts()
	commonEdits := cdispecs.ContainerEdits{
		Env:         []string{"NVIDIA_VISIBLE_DEVICES=void"},
		DeviceNodes: commonDeviceNodes,
		Mounts:      mounts,
	}

	devices := make([]cdispecs.Device, 0, len(perGPUDevices)+1)
	available := make([]string, 0, len(perGPUDevices)+1)
	allDeviceNodes := make([]*cdispecs.DeviceNode, 0, len(perGPUDevices))
	for _, gpu := range perGPUDevices {
		deviceNode := cdiDeviceNode(gpu.DevicePath)
		devices = append(devices, cdispecs.Device{
			Name: gpu.Name,
			ContainerEdits: cdispecs.ContainerEdits{
				DeviceNodes: []*cdispecs.DeviceNode{deviceNode},
			},
		})
		available = append(available, gpu.Name)
		allDeviceNodes = append(allDeviceNodes, deviceNode)
	}
	devices = append(devices, cdispecs.Device{
		Name: managedNVIDIAAllDeviceName,
		ContainerEdits: cdispecs.ContainerEdits{
			DeviceNodes: allDeviceNodes,
		},
	})
	available = append(available, managedNVIDIAAllDeviceName)

	return &cdispecs.Spec{
		Version:        cdispecs.CurrentVersion,
		Kind:           managedNVIDIACDIVendor + "/" + managedNVIDIACDIClass,
		Devices:        devices,
		ContainerEdits: commonEdits,
	}, available, nil
}

type discoveredNVIDIAGPUDevice struct {
	Name       string
	DevicePath string
}

func discoverNVIDIAGPUDevices() ([]discoveredNVIDIAGPUDevice, error) {
	matches, err := filepath.Glob("/dev/nvidia[0-9]*")
	if err != nil {
		return nil, fmt.Errorf("glob NVIDIA GPU devices: %w", err)
	}

	result := make([]discoveredNVIDIAGPUDevice, 0, len(matches))
	for _, devicePath := range uniqueStrings(matches) {
		info, err := os.Stat(devicePath)
		if err != nil || info.IsDir() {
			continue
		}
		name := strings.TrimPrefix(filepath.Base(devicePath), "nvidia")
		if !isIndexStyleCDIDeviceName(name) {
			continue
		}
		result = append(result, discoveredNVIDIAGPUDevice{
			Name:       name,
			DevicePath: devicePath,
		})
	}

	slices.SortFunc(result, func(a discoveredNVIDIAGPUDevice, b discoveredNVIDIAGPUDevice) int {
		left, _ := strconv.Atoi(a.Name)
		right, _ := strconv.Atoi(b.Name)
		switch {
		case left < right:
			return -1
		case left > right:
			return 1
		default:
			return 0
		}
	})
	return result, nil
}

func discoverCommonNVIDIADeviceNodes() []*cdispecs.DeviceNode {
	candidates := []string{
		"/dev/nvidiactl",
		"/dev/nvidia-uvm",
		"/dev/nvidia-uvm-tools",
		"/dev/nvidia-modeset",
	}

	nodes := make([]*cdispecs.DeviceNode, 0, len(candidates))
	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err != nil || info.IsDir() {
			continue
		}
		nodes = append(nodes, cdiDeviceNode(candidate))
	}
	return nodes
}

func discoverManagedNVIDIADriverMounts() []*cdispecs.Mount {
	patterns := []string{
		"libcuda.so",
		"libcuda.so.1",
		"libcuda.so.*",
		"libnvidia-ml.so.1",
		"libnvidia-ml.so.*",
		"libnvidia-ptxjitcompiler.so.1",
		"libnvidia-ptxjitcompiler.so.*",
		"libnvidia-encode.so.1",
		"libnvidia-encode.so.*",
		"libnvcuvid.so.1",
		"libnvcuvid.so.*",
		"libnvidia-compiler.so.1",
		"libnvidia-compiler.so.*",
		"libnvidia-opticalflow.so.1",
		"libnvidia-opticalflow.so.*",
		"libnvidia-cfg.so.1",
		"libnvidia-cfg.so.*",
	}

	searchRoots := []string{
		"/usr/lib64",
		"/lib64",
		"/usr/local/nvidia/lib64",
		"/usr/local/nvidia/lib",
	}
	switch goRuntime.GOARCH {
	case "amd64":
		searchRoots = append(searchRoots, "/usr/lib/x86_64-linux-gnu", "/lib/x86_64-linux-gnu")
	case "arm64":
		searchRoots = append(searchRoots, "/usr/lib/aarch64-linux-gnu", "/lib/aarch64-linux-gnu")
	}

	mounts := make([]*cdispecs.Mount, 0, len(patterns))
	seen := make(map[string]struct{})
	for _, root := range uniqueStrings(searchRoots) {
		for _, pattern := range patterns {
			matches, err := filepath.Glob(filepath.Join(root, pattern))
			if err != nil {
				continue
			}
			for _, match := range matches {
				if _, exists := seen[match]; exists {
					continue
				}
				info, err := os.Lstat(match)
				if err != nil || info.IsDir() {
					continue
				}
				seen[match] = struct{}{}
				mounts = append(mounts, &cdispecs.Mount{
					HostPath:      match,
					ContainerPath: match,
					Options:       []string{"ro", "nosuid", "nodev", "rbind", "rprivate"},
				})
			}
		}
	}
	return mounts
}

func cdiDeviceNode(path string) *cdispecs.DeviceNode {
	return &cdispecs.DeviceNode{
		Path:        path,
		HostPath:    path,
		Permissions: "rwm",
	}
}

func managedContainerdDaemonEnv(paths managedContainerdPaths) []string {
	pathEntries := []string{paths.containerdBinDir, filepath.Dir(paths.runcBin)}
	if existing := os.Getenv("PATH"); strings.TrimSpace(existing) != "" {
		for _, part := range filepath.SplitList(existing) {
			if strings.TrimSpace(part) != "" {
				pathEntries = append(pathEntries, part)
			}
		}
	}

	env := os.Environ()
	pathValue := "PATH=" + strings.Join(uniqueStrings(pathEntries), string(os.PathListSeparator))
	replaced := false
	for index, entry := range env {
		if strings.HasPrefix(entry, "PATH=") {
			env[index] = pathValue
			replaced = true
			break
		}
	}
	if !replaced {
		env = append(env, pathValue)
	}
	return env
}

func containerSpecOpts(
	payload ContainerPayload,
	_ []mountedStorage,
	directoryMounts []resolvedContainerMount,
	image containerd.Image,
) ([]oci.SpecOpts, error) {
	mounts, err := containerSpecMounts(payload, directoryMounts)
	if err != nil {
		return nil, err
	}

	specOpts := []oci.SpecOpts{
		oci.WithDefaultSpec(),
		oci.WithImageConfig(image),
		oci.WithHostHostsFile,
		oci.WithHostResolvconf,
		oci.WithHostLocaltime,
		oci.WithHostNamespace(specs.NetworkNamespace),
		oci.WithMounts(mounts),
	}

	if len(payload.Command) > 0 {
		specOpts = append(specOpts, oci.WithProcessArgs(payload.Command...))
	}
	if workDir := strings.TrimSpace(payload.WorkDir); workDir != "" {
		specOpts = append(specOpts, oci.WithProcessCwd(workDir))
	}
	if env := containerSpecEnv(payload.Env); len(env) > 0 {
		specOpts = append(specOpts, oci.WithEnv(env))
	}
	if len(payload.GPUDevices) > 0 {
		specOpts = append(specOpts, containerdcdi.WithCDIDevices(payload.GPUDevices...))
	}

	return specOpts, nil
}

func containerSpecMounts(payload ContainerPayload, directoryMounts []resolvedContainerMount) ([]specs.Mount, error) {
	seenDestinations := map[string]string{
		containerWorkspaceRoot: "workspace",
	}
	mounts := []specs.Mount{
		{
			Source:      payload.WorkspaceRoot,
			Destination: containerWorkspaceRoot,
			Type:        "bind",
			Options:     []string{"rbind", "rw"},
		},
	}

	for _, mount := range payload.Mounts {
		destination := path.Clean(strings.TrimSpace(mount.MountPath))
		if destination == "" || destination == "." {
			continue
		}
		if owner, exists := seenDestinations[destination]; exists {
			return nil, fmt.Errorf("mount destination %q conflicts with %s mount", destination, owner)
		}
		seenDestinations[destination] = "storage"
		mounts = append(mounts, specs.Mount{
			Source:      mount.MountPath,
			Destination: destination,
			Type:        "bind",
			Options:     []string{"rbind", "rw"},
		})
	}
	for _, mount := range directoryMounts {
		destination := path.Clean(strings.TrimSpace(filepath.ToSlash(mount.Target)))
		if owner, exists := seenDestinations[destination]; exists {
			return nil, fmt.Errorf("mount destination %q conflicts with %s mount", destination, owner)
		}
		seenDestinations[destination] = "directory"
		options := []string{"rbind", "rw"}
		if mount.ReadOnly {
			options = []string{"rbind", "ro"}
		}
		mounts = append(mounts, specs.Mount{
			Source:      mount.Source,
			Destination: destination,
			Type:        "bind",
			Options:     options,
		})
	}

	return mounts, nil
}

func containerSpecEnv(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	env := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}

		env = append(env, key+"="+values[key])
	}
	return env
}

func normalizeContainerdExitStatus(status containerd.ExitStatus) error {
	if err := status.Error(); err != nil {
		return fmt.Errorf("wait for container task: %w", err)
	}
	if code := status.ExitCode(); code != 0 {
		return fmt.Errorf("container exited with code %d", code)
	}
	return nil
}

func managedContainerdReleaseArch() (string, error) {
	switch goRuntime.GOARCH {
	case "amd64", "arm64":
		return goRuntime.GOARCH, nil
	default:
		return "", fmt.Errorf("managed container runtime does not support architecture %q", goRuntime.GOARCH)
	}
}

func downloadAndExtractTarGz(ctx context.Context, url string, destinationDir string) error {
	if entryExists(destinationDir) {
		return nil
	}

	parentDir := filepath.Dir(destinationDir)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		return fmt.Errorf("create parent directory %q: %w", parentDir, err)
	}

	tempDir, err := os.MkdirTemp(parentDir, ".download-*")
	if err != nil {
		return fmt.Errorf("create temporary extraction directory in %q: %w", parentDir, err)
	}
	defer os.RemoveAll(tempDir)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build download request for %q: %w", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("download %q: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("download %q: unexpected status %s", url, resp.Status)
	}

	gzipReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("open gzip archive from %q: %w", url, err)
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar archive from %q: %w", url, err)
		}

		targetPath, err := safeArchivePath(tempDir, header.Name)
		if err != nil {
			return err
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, header.FileInfo().Mode().Perm()); err != nil {
				return fmt.Errorf("create directory %q: %w", targetPath, err)
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
				return fmt.Errorf("create parent directory for %q: %w", targetPath, err)
			}
			file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, header.FileInfo().Mode().Perm())
			if err != nil {
				return fmt.Errorf("create file %q: %w", targetPath, err)
			}
			if _, err := io.Copy(file, tarReader); err != nil {
				_ = file.Close()
				return fmt.Errorf("write file %q: %w", targetPath, err)
			}
			if err := file.Close(); err != nil {
				return fmt.Errorf("close file %q: %w", targetPath, err)
			}
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
				return fmt.Errorf("create parent directory for symlink %q: %w", targetPath, err)
			}
			if err := os.Symlink(header.Linkname, targetPath); err != nil {
				return fmt.Errorf("create symlink %q -> %q: %w", targetPath, header.Linkname, err)
			}
		}
	}

	if err := os.Rename(tempDir, destinationDir); err != nil {
		if entryExists(destinationDir) {
			return nil
		}
		return fmt.Errorf("move extracted archive into %q: %w", destinationDir, err)
	}
	return nil
}

func downloadExecutable(ctx context.Context, url string, destinationPath string) error {
	if fileExists(destinationPath) {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(destinationPath), 0o755); err != nil {
		return fmt.Errorf("create parent directory for %q: %w", destinationPath, err)
	}

	tempFile, err := os.CreateTemp(filepath.Dir(destinationPath), ".download-*")
	if err != nil {
		return fmt.Errorf("create temporary executable for %q: %w", destinationPath, err)
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("build download request for %q: %w", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("download %q: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = tempFile.Close()
		return fmt.Errorf("download %q: unexpected status %s", url, resp.Status)
	}
	if _, err := io.Copy(tempFile, resp.Body); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("write temporary executable for %q: %w", destinationPath, err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close temporary executable for %q: %w", destinationPath, err)
	}
	if err := os.Chmod(tempPath, 0o755); err != nil {
		return fmt.Errorf("chmod temporary executable %q: %w", tempPath, err)
	}
	if err := os.Rename(tempPath, destinationPath); err != nil {
		if fileExists(destinationPath) {
			return nil
		}
		return fmt.Errorf("move executable into %q: %w", destinationPath, err)
	}
	return nil
}

func writeFileAtomically(path string, content []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create parent directory for %q: %w", path, err)
	}

	tempFile, err := os.CreateTemp(filepath.Dir(path), ".write-*")
	if err != nil {
		return fmt.Errorf("create temporary file for %q: %w", path, err)
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	if _, err := tempFile.Write(content); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("write temporary file for %q: %w", path, err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close temporary file for %q: %w", path, err)
	}
	if err := os.Chmod(tempPath, mode); err != nil {
		return fmt.Errorf("chmod temporary file for %q: %w", path, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("move temporary file into %q: %w", path, err)
	}
	return nil
}

func safeArchivePath(root string, name string) (string, error) {
	cleanName := filepath.Clean(name)
	if cleanName == "." {
		return root, nil
	}
	target := filepath.Join(root, cleanName)
	rootWithSep := root + string(os.PathSeparator)
	if !strings.HasPrefix(target, rootWithSep) && target != root {
		return "", fmt.Errorf("archive entry %q escapes destination root", name)
	}
	return target, nil
}

func allPathsExist(paths []string) bool {
	for _, current := range paths {
		if !fileExists(current) {
			return false
		}
	}
	return true
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func entryExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func uniqueStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}
