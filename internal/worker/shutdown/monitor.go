package shutdown

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type Notice struct {
	Provider   string
	Detail     string
	ShutdownAt time.Time
}

type Reporter interface {
	ReportNotice(context.Context, Notice) error
}

type Detector interface {
	Detect(context.Context) (*Notice, error)
}

type DetectorFactory func(Options) Detector

type Options struct {
	HTTPClient *http.Client
	Logger     *slog.Logger
}

type Registry struct {
	factories map[string]DetectorFactory
}

func NewRegistry() *Registry {
	return &Registry{factories: map[string]DetectorFactory{}}
}

func (r *Registry) Register(provider string, factory DetectorFactory) {
	if r == nil || factory == nil {
		return
	}
	key := strings.ToLower(strings.TrimSpace(provider))
	if key == "" {
		return
	}
	r.factories[key] = factory
}

func (r *Registry) New(provider string, options Options) (Detector, bool) {
	if r == nil {
		return nil, false
	}
	factory, ok := r.factories[strings.ToLower(strings.TrimSpace(provider))]
	if !ok {
		return nil, false
	}
	return factory(options), true
}

func DefaultRegistry() *Registry {
	registry := NewRegistry()
	registry.Register("aws", func(options Options) Detector { return newAWSDetector(options) })
	registry.Register("ec2", func(options Options) Detector { return newAWSDetector(options) })
	registry.Register("gcp", func(options Options) Detector { return newGCPDetector(options) })
	registry.Register("google", func(options Options) Detector { return newGCPDetector(options) })
	return registry
}

type Monitor struct {
	logger   *slog.Logger
	reporter Reporter
	registry *Registry
	config   MonitorConfig
	client   *http.Client
}

func NewMonitor(logger *slog.Logger, reporter Reporter, config MonitorConfig, registry *Registry) *Monitor {
	if logger == nil {
		logger = slog.Default()
	}
	if registry == nil {
		registry = DefaultRegistry()
	}
	return &Monitor{
		logger:   logger,
		reporter: reporter,
		registry: registry,
		config:   config,
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

func (m *Monitor) Run(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if !m.config.Enabled {
		return nil
	}
	if m.reporter == nil {
		return errors.New("shutdown monitor reporter is required")
	}
	detector, ok := m.registry.New(m.config.Provider, Options{
		HTTPClient: m.client,
		Logger:     m.logger,
	})
	if !ok {
		m.logger.InfoContext(
			context.WithoutCancel(ctx),
			"shutdown monitor provider is not supported; skipping",
			slog.String("provider", m.config.Provider),
		)
		return nil
	}

	interval := m.config.PollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		notice, err := detector.Detect(ctx)
		if err != nil && ctx.Err() == nil {
			m.logger.WarnContext(
				context.WithoutCancel(ctx),
				"shutdown monitor probe failed",
				slog.String("provider", m.config.Provider),
				slog.String("error", err.Error()),
			)
		}
		if notice != nil {
			if notice.Provider == "" {
				notice.Provider = m.config.Provider
			}
			if err := m.reporter.ReportNotice(context.WithoutCancel(ctx), *notice); err != nil {
				return fmt.Errorf("report shutdown notice: %w", err)
			}
			m.logger.InfoContext(
				context.WithoutCancel(ctx),
				"shutdown notice reported",
				slog.String("provider", notice.Provider),
				slog.String("shutdown_at", notice.ShutdownAt.UTC().Format(time.RFC3339)),
			)
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
