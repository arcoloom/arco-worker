package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	workerv1 "github.com/arcoloom/arco-worker/gen/proto/arcoloom/worker/v1"
	workerApp "github.com/arcoloom/arco-worker/internal/worker/app"
	workerRuntime "github.com/arcoloom/arco-worker/internal/worker/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type config struct {
	ControlPlaneAddress  string
	ControlPlaneInsecure bool
	InstanceID           string
	Provider             string
	RegistrationToken    string
	LogLevel             string
	LogFormat            string
	DialTimeout          time.Duration
	RegisterTimeout      time.Duration
	ReportTimeout        time.Duration
	StopTimeout          time.Duration
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(2)
	}

	logger, err := newLogger(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger error: %v\n", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := dialControlPlane(ctx, cfg)
	if err != nil {
		logger.Error("failed to connect to control plane", slog.String("address", cfg.ControlPlaneAddress), slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer conn.Close()

	client := workerApp.NewGRPCControlPlaneClient(workerv1.NewWorkerServiceClient(conn))
	runner, err := workerApp.NewRunner(
		logger,
		client,
		func(ctx context.Context, kind workerv1.RuntimeKind) (workerRuntime.Engine, error) {
			switch kind {
			case workerv1.RuntimeKind_RUNTIME_KIND_EXEC:
				return workerRuntime.NewExecEngine(logger), nil
			case workerv1.RuntimeKind_RUNTIME_KIND_DOCKER:
				return nil, fmt.Errorf("runtime %s is not implemented yet", kind.String())
			default:
				return nil, fmt.Errorf("runtime %s is not supported", kind.String())
			}
		},
		workerApp.RunnerConfig{
			InstanceID:        cfg.InstanceID,
			Provider:          cfg.Provider,
			RegistrationToken: cfg.RegistrationToken,
			RegisterTimeout:   cfg.RegisterTimeout,
			ReportTimeout:     cfg.ReportTimeout,
			StopTimeout:       cfg.StopTimeout,
		},
	)
	if err != nil {
		logger.Error("failed to create worker runner", slog.String("error", err.Error()))
		os.Exit(1)
	}

	logger.Info(
		"starting arco-worker",
		slog.String("control_plane_address", cfg.ControlPlaneAddress),
		slog.String("instance_id", cfg.InstanceID),
		slog.String("provider", cfg.Provider),
		slog.String("log_format", cfg.LogFormat),
		slog.String("log_level", strings.ToLower(cfg.LogLevel)),
	)

	if err := runner.Run(ctx); err != nil {
		logger.Error("worker exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func parseConfig(args []string) (config, error) {
	fs := flag.NewFlagSet("arco-worker", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	cfg := config{}
	fs.StringVar(&cfg.ControlPlaneAddress, "control-plane-address", "", "Control plane gRPC address, for example 10.0.0.10:8443")
	fs.BoolVar(&cfg.ControlPlaneInsecure, "control-plane-insecure", true, "Use insecure gRPC transport credentials")
	fs.StringVar(&cfg.InstanceID, "instance-id", "", "Cloud instance identifier")
	fs.StringVar(&cfg.Provider, "provider", "", "Cloud provider name, for example aws")
	fs.StringVar(&cfg.RegistrationToken, "registration-token", "", "Registration token issued by the control plane")
	fs.StringVar(&cfg.LogLevel, "log-level", "info", "Log level: debug, info, warn, error")
	fs.StringVar(&cfg.LogFormat, "log-format", "json", "Log format: json or text")
	fs.DurationVar(&cfg.DialTimeout, "dial-timeout", 10*time.Second, "Timeout for establishing the initial gRPC connection")
	fs.DurationVar(&cfg.RegisterTimeout, "register-timeout", 15*time.Second, "Timeout for the register RPC")
	fs.DurationVar(&cfg.ReportTimeout, "report-timeout", 5*time.Second, "Timeout for report RPCs")
	fs.DurationVar(&cfg.StopTimeout, "stop-timeout", 15*time.Second, "Timeout for graceful workload stop after SIGTERM/SIGINT")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	switch {
	case cfg.ControlPlaneAddress == "":
		return config{}, fmt.Errorf("missing required flag --control-plane-address")
	case cfg.InstanceID == "":
		return config{}, fmt.Errorf("missing required flag --instance-id")
	case cfg.Provider == "":
		return config{}, fmt.Errorf("missing required flag --provider")
	case cfg.RegistrationToken == "":
		return config{}, fmt.Errorf("missing required flag --registration-token")
	case cfg.DialTimeout <= 0:
		return config{}, fmt.Errorf("--dial-timeout must be greater than zero")
	case cfg.RegisterTimeout <= 0:
		return config{}, fmt.Errorf("--register-timeout must be greater than zero")
	case cfg.ReportTimeout <= 0:
		return config{}, fmt.Errorf("--report-timeout must be greater than zero")
	case cfg.StopTimeout <= 0:
		return config{}, fmt.Errorf("--stop-timeout must be greater than zero")
	}

	return cfg, nil
}

func newLogger(cfg config) (*slog.Logger, error) {
	var level slog.Level
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("unsupported log level %q", cfg.LogLevel)
	}

	options := &slog.HandlerOptions{Level: level}

	switch strings.ToLower(cfg.LogFormat) {
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stdout, options)), nil
	case "text":
		return slog.New(slog.NewTextHandler(os.Stdout, options)), nil
	default:
		return nil, fmt.Errorf("unsupported log format %q", cfg.LogFormat)
	}
}

func dialControlPlane(ctx context.Context, cfg config) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()

	transportCredentials := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	if cfg.ControlPlaneInsecure {
		transportCredentials = insecure.NewCredentials()
	}

	conn, err := grpc.DialContext(
		dialCtx,
		cfg.ControlPlaneAddress,
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", cfg.ControlPlaneAddress, err)
	}

	return conn, nil
}
