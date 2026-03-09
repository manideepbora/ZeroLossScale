package autoscale

import (
	"context"
	"sync/atomic"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ComponentHealth represents the health of a single component.
type ComponentHealth struct {
	Healthy bool   `json:"healthy"`
	Message string `json:"message,omitempty"`
}

// HealthStatus aggregates component health into an overall status.
type HealthStatus struct {
	Healthy    bool                       `json:"healthy"`
	Components map[string]ComponentHealth `json:"components"`
}

// CheckHealth returns an aggregate health status for the core dependencies.
// Pass nil for optional parameters to skip that check.
func CheckHealth(ctx context.Context, nc *nats.Conn, js jetstream.JetStream, watchHealthy *atomic.Bool) HealthStatus {
	status := HealthStatus{
		Healthy:    true,
		Components: make(map[string]ComponentHealth),
	}

	if nc == nil || !nc.IsConnected() {
		status.Components["nats_connection"] = ComponentHealth{Healthy: false, Message: "disconnected"}
		status.Healthy = false
	} else {
		status.Components["nats_connection"] = ComponentHealth{Healthy: true, Message: "connected"}
	}

	if js != nil {
		if _, err := js.AccountInfo(ctx); err != nil {
			status.Components["jetstream"] = ComponentHealth{Healthy: false, Message: err.Error()}
			status.Healthy = false
		} else {
			status.Components["jetstream"] = ComponentHealth{Healthy: true, Message: "reachable"}
		}
	}

	if watchHealthy != nil {
		if !watchHealthy.Load() {
			status.Components["kv_watcher"] = ComponentHealth{Healthy: false, Message: "watcher disconnected"}
			status.Healthy = false
		} else {
			status.Components["kv_watcher"] = ComponentHealth{Healthy: true, Message: "watching"}
		}
	}

	return status
}
