package autoscale

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// AutoScaleConfig controls the lag-based auto-scaler.
type AutoScaleConfig struct {
	Enabled       bool          // Feature flag — false means manual/API scaling only.
	CheckInterval time.Duration // How often to evaluate lag (default 10s).
	LagHighWater  uint64        // Total pending above this → scale up.
	LagLowWater   uint64        // Total pending below this → scale down.
	MinPartitions int           // Never scale below this.
	MaxPartitions int           // Never scale above this (capped by MaxPartitions const).
	CoolDown      time.Duration // Minimum time between scale decisions.
}

// DefaultAutoScaleConfig returns a disabled auto-scale config with sensible defaults.
func DefaultAutoScaleConfig() AutoScaleConfig {
	return AutoScaleConfig{
		Enabled:       false,
		CheckInterval: 10 * time.Second,
		LagHighWater:  5000,
		LagLowWater:   500,
		MinPartitions: 1,
		MaxPartitions: 10,
		CoolDown:      30 * time.Second,
	}
}

// AutoScaler monitors consumer lag and writes desired_partitions to KV.
// The existing Reconciler watches that key and triggers the repartition protocol.
type AutoScaler struct {
	js  jetstream.JetStream
	pm  *PartitionManager
	cfg Config
	asc AutoScaleConfig

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewAutoScaler creates a lag-based auto-scaler.
func NewAutoScaler(js jetstream.JetStream, pm *PartitionManager, cfg Config, asc AutoScaleConfig) *AutoScaler {
	if asc.MaxPartitions > MaxPartitions {
		asc.MaxPartitions = MaxPartitions
	}
	return &AutoScaler{
		js:  js,
		pm:  pm,
		cfg: cfg,
		asc: asc,
	}
}

// Start begins the auto-scale evaluation loop.
func (a *AutoScaler) Start(ctx context.Context) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.cancel != nil {
		return
	}

	loopCtx, cancel := context.WithCancel(ctx)
	a.cancel = cancel
	a.done = make(chan struct{})

	go a.run(loopCtx)
	log.Printf("[autoscaler] started (interval=%v high=%d low=%d min=%d max=%d cooldown=%v)",
		a.asc.CheckInterval, a.asc.LagHighWater, a.asc.LagLowWater,
		a.asc.MinPartitions, a.asc.MaxPartitions, a.asc.CoolDown)
}

// Stop terminates the auto-scale loop.
func (a *AutoScaler) Stop() {
	a.mu.Lock()
	cancel := a.cancel
	done := a.done
	a.cancel = nil
	a.mu.Unlock()

	if cancel != nil {
		cancel()
		<-done
	}
	log.Printf("[autoscaler] stopped")
}

func (a *AutoScaler) run(ctx context.Context) {
	defer close(a.done)

	ticker := time.NewTicker(a.asc.CheckInterval)
	defer ticker.Stop()

	var lastScale time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if time.Since(lastScale) < a.asc.CoolDown {
				continue
			}

			totalPending, err := a.getTotalPending(ctx)
			if err != nil {
				log.Printf("[autoscaler] lag check failed: %v", err)
				continue
			}

			current := a.pm.Count()
			desired := current

			if totalPending > a.asc.LagHighWater && current < a.asc.MaxPartitions {
				desired = current + 1
			} else if totalPending < a.asc.LagLowWater && current > a.asc.MinPartitions {
				desired = current - 1
			}

			if desired == current {
				continue
			}

			log.Printf("[autoscaler] lag=%d current=%d desired=%d — writing desired_partitions",
				totalPending, current, desired)

			if err := SetDesired(ctx, a.js, a.cfg.KVBucket, desired); err != nil {
				log.Printf("[autoscaler] write desired_partitions: %v", err)
				continue
			}

			lastScale = time.Now()
		}
	}
}

// getTotalPending sums NumPending + NumAckPending across all partition consumers.
func (a *AutoScaler) getTotalPending(ctx context.Context) (uint64, error) {
	count := a.pm.Count()
	var total uint64

	for i := 0; i < count; i++ {
		name := ConsumerName(i)
		cons, err := a.js.Consumer(ctx, a.cfg.StreamName, name)
		if err != nil {
			continue // consumer may not exist yet
		}
		ci, err := cons.Info(ctx)
		if err != nil {
			continue
		}
		total += ci.NumPending + uint64(ci.NumAckPending)
	}

	return total, nil
}
