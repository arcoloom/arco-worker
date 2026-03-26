package shutdown

import (
	"testing"
	"time"
)

func TestMonitorConfigFromAssignment(t *testing.T) {
	config := MonitorConfigFromAssignment([]byte(`{
		"shutdown_monitor": {
			"enabled": true,
			"provider": "aws",
			"pricing_model": "spot",
			"poll_interval": "9s"
		}
	}`), "gcp")

	if !config.Enabled {
		t.Fatal("MonitorConfigFromAssignment() enabled = false, want true")
	}
	if config.Provider != "aws" {
		t.Fatalf("MonitorConfigFromAssignment() provider = %q, want aws", config.Provider)
	}
	if config.PricingModel != "spot" {
		t.Fatalf("MonitorConfigFromAssignment() pricing model = %q, want spot", config.PricingModel)
	}
	if config.PollInterval != 9*time.Second {
		t.Fatalf("MonitorConfigFromAssignment() poll interval = %s, want 9s", config.PollInterval)
	}
}

func TestMonitorConfigFromAssignmentDefaultsToDisabled(t *testing.T) {
	config := MonitorConfigFromAssignment([]byte(`{"task_id":"task-1"}`), "aws")
	if config.Enabled {
		t.Fatal("MonitorConfigFromAssignment() enabled = true, want false")
	}
	if config.Provider != "aws" {
		t.Fatalf("MonitorConfigFromAssignment() provider = %q, want aws", config.Provider)
	}
	if config.PollInterval != defaultPollInterval {
		t.Fatalf("MonitorConfigFromAssignment() poll interval = %s, want %s", config.PollInterval, defaultPollInterval)
	}
}
