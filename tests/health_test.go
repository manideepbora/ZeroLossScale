package tests

import (
	"context"
	"sync/atomic"
	"testing"

	"nats-poc/autoscale"
)

func TestCheckHealth_AllNil(t *testing.T) {
	status := autoscale.CheckHealth(context.Background(), nil, nil, nil)
	if status.Healthy {
		t.Error("expected unhealthy when nc is nil")
	}
	if comp, ok := status.Components["nats_connection"]; !ok || comp.Healthy {
		t.Error("expected nats_connection unhealthy")
	}
}

func TestCheckHealth_WatcherHealthy(t *testing.T) {
	var healthy atomic.Bool

	// Watcher unhealthy.
	healthy.Store(false)
	status := autoscale.CheckHealth(context.Background(), nil, nil, &healthy)
	if kv, ok := status.Components["kv_watcher"]; !ok || kv.Healthy {
		t.Error("expected kv_watcher unhealthy when false")
	}

	// Watcher healthy.
	healthy.Store(true)
	status = autoscale.CheckHealth(context.Background(), nil, nil, &healthy)
	if kv, ok := status.Components["kv_watcher"]; !ok || !kv.Healthy {
		t.Error("expected kv_watcher healthy when true")
	}
	// Overall still unhealthy because nc is nil.
	if status.Healthy {
		t.Error("expected overall unhealthy (nc is nil)")
	}
}

func TestCheckHealth_NilWatcher(t *testing.T) {
	// When watchHealthy is nil, kv_watcher component should not be present.
	status := autoscale.CheckHealth(context.Background(), nil, nil, nil)
	if _, ok := status.Components["kv_watcher"]; ok {
		t.Error("expected no kv_watcher component when watchHealthy is nil")
	}
}
