package autoscale

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// ControlPlane coordinates the system components:
//   - Publisher: publishes directly to partition subjects (auto-switches via KV)
//   - ConsumerPool: consumes from partition subjects
//   - Scaler: handles repartition via buffer-drain-replay protocol
//   - PartitionManager: manages partition count and mode in KV
type ControlPlane struct {
	JS        jetstream.JetStream
	PM        *PartitionManager
	Publisher *DirectPublisher
	Pool      *ConsumerPool
	Scaler    *Scaler
	Stream    string // partition stream name
	Prefix    string
	Cfg       Config

	repartMu sync.Mutex
}

// PoolBackend implements ConsumerBackend for in-process consumer pools.
// Used by the ControlPlane in tests and the dashboard.
type PoolBackend struct {
	Pool *ConsumerPool
}

func (b *PoolBackend) ScaleConsumers(ctx context.Context, oldCount, newCount int) error {
	b.Pool.ScaleTo(ctx, newCount)
	return nil
}

// NewControlPlane creates the full system with default config.
func NewControlPlane(ctx context.Context, js jetstream.JetStream) (*ControlPlane, error) {
	return NewControlPlaneWithConfig(ctx, js, DefaultConfig())
}

func NewControlPlaneWithConfig(ctx context.Context, js jetstream.JetStream, cfg Config) (*ControlPlane, error) {
	if err := cfg.EnsureStreams(ctx, js); err != nil {
		return nil, err
	}

	pm, err := NewPartitionManager(ctx, js, cfg.KVBucket, cfg.InitialPartitions)
	if err != nil {
		return nil, fmt.Errorf("partition manager: %w", err)
	}

	// Seed mode so DirectPublisher can read it.
	if err := pm.SetMode(ctx, KVModeDirect); err != nil {
		return nil, fmt.Errorf("seed mode: %w", err)
	}

	pub, err := NewDirectPublisher(ctx, js, cfg.StreamName)
	if err != nil {
		return nil, fmt.Errorf("publisher: %w", err)
	}

	pool := NewConsumerPool(js, cfg.StreamName, cfg.SubjectPrefix, cfg)

	scaler := &Scaler{
		JS:      js,
		PM:      pm,
		Cfg:     cfg,
		Backend: &PoolBackend{Pool: pool},
		DLQ:     NewDLQPublisher(js, cfg.DLQSubject),
	}

	return &ControlPlane{
		JS:        js,
		PM:        pm,
		Publisher: pub,
		Pool:      pool,
		Scaler:    scaler,
		Stream:    cfg.StreamName,
		Prefix:    cfg.SubjectPrefix,
		Cfg:       cfg,
	}, nil
}

// Start initializes the consumer pool.
func (cp *ControlPlane) Start(ctx context.Context) error {
	cp.Pool.ScaleTo(ctx, cp.PM.Count())
	return nil
}

// Repartition changes the partition count. Delegates to the Scaler which
// uses the buffer-drain-replay protocol with KV-based mode signaling.
func (cp *ControlPlane) Repartition(ctx context.Context, newCount int) error {
	cp.repartMu.Lock()
	defer cp.repartMu.Unlock()
	return cp.Scaler.Scale(ctx, newCount)
}

// Cleanup gracefully shuts down consumers. Does NOT delete streams or KV.
func (cp *ControlPlane) Cleanup(ctx context.Context) {
	cp.Publisher.Stop()
	cp.Pool.StopAll()
}

// DestroyAll removes streams and KV bucket. Use only in tests or decommissioning.
func (cp *ControlPlane) DestroyAll(ctx context.Context) {
	cp.Publisher.Stop()
	cp.Pool.StopAll()
	if err := cp.JS.DeleteStream(ctx, cp.Cfg.BufferStreamName); err != nil {
		log.Printf("[controlplane] delete buffer stream %s: %v", cp.Cfg.BufferStreamName, err)
	}
	if err := cp.JS.DeleteStream(ctx, cp.Stream); err != nil {
		log.Printf("[controlplane] delete partition stream %s: %v", cp.Stream, err)
	}
	if cp.Cfg.DLQStreamName != "" {
		if err := cp.JS.DeleteStream(ctx, cp.Cfg.DLQStreamName); err != nil {
			log.Printf("[controlplane] delete DLQ stream %s: %v", cp.Cfg.DLQStreamName, err)
		}
	}
	if err := cp.JS.DeleteKeyValue(ctx, cp.Cfg.KVBucket); err != nil {
		log.Printf("[controlplane] delete KV %s: %v", cp.Cfg.KVBucket, err)
	}
}

// WaitForDrain waits until all consumers for partitions [0, oldCount) have
// processed all pending messages. It polls consumer info until pending == 0.
func WaitForDrain(ctx context.Context, js jetstream.JetStream, streamName, prefix string, oldCount int, consumerNames func(int) string, timeout time.Duration) error {
	deadline := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("drain timeout after %v", timeout)
		case <-ticker.C:
			allDrained := true
			for i := 0; i < oldCount; i++ {
				name := consumerNames(i)
				info, err := js.Consumer(ctx, streamName, name)
				if err != nil {
					continue // consumer may not exist yet
				}
				ci, err := info.Info(ctx)
				if err != nil {
					continue
				}
				if ci.NumPending > 0 || ci.NumAckPending > 0 {
					allDrained = false
					break
				}
			}
			if allDrained {
				return nil
			}
		}
	}
}
