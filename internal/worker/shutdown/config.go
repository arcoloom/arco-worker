package shutdown

import (
	"encoding/json"
	"strings"
	"time"
)

const defaultPollInterval = 5 * time.Second

type MonitorConfig struct {
	Enabled      bool
	Provider     string
	PricingModel string
	PollInterval time.Duration
}

type assignmentEnvelope struct {
	ShutdownMonitor *assignmentMonitorConfig `json:"shutdown_monitor"`
}

type assignmentMonitorConfig struct {
	Enabled      bool   `json:"enabled"`
	Provider     string `json:"provider"`
	PricingModel string `json:"pricing_model"`
	PollInterval string `json:"poll_interval"`
}

func MonitorConfigFromAssignment(payload []byte, fallbackProvider string) MonitorConfig {
	config := MonitorConfig{
		Provider:     strings.TrimSpace(fallbackProvider),
		PollInterval: defaultPollInterval,
	}

	var envelope assignmentEnvelope
	if len(payload) == 0 || json.Unmarshal(payload, &envelope) != nil || envelope.ShutdownMonitor == nil {
		return config
	}

	config.Enabled = envelope.ShutdownMonitor.Enabled
	if provider := strings.TrimSpace(envelope.ShutdownMonitor.Provider); provider != "" {
		config.Provider = provider
	}
	config.PricingModel = strings.TrimSpace(envelope.ShutdownMonitor.PricingModel)
	if raw := strings.TrimSpace(envelope.ShutdownMonitor.PollInterval); raw != "" {
		if interval, err := time.ParseDuration(raw); err == nil && interval > 0 {
			config.PollInterval = interval
		}
	}

	return config
}
