package autoscale

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// LeaderElector provides KV-based leader election using TTL and create-only semantics.
// Multiple instances race to create a key; the winner is leader and must renew before TTL.
// If the leader dies, the key expires and followers race again.
type LeaderElector struct {
	js         jetstream.JetStream
	kv         jetstream.KeyValue
	bucket     string
	instanceID string
	ttl        time.Duration

	mu       sync.RWMutex
	leader   bool
	revision uint64

	onElected func()
	onLost    func()

	cancel context.CancelFunc
	done   chan struct{}
}

// LeaderOption configures the LeaderElector.
type LeaderOption func(*LeaderElector)

// WithOnElected sets the callback invoked when this instance becomes leader.
func WithOnElected(fn func()) LeaderOption {
	return func(le *LeaderElector) { le.onElected = fn }
}

// WithOnLost sets the callback invoked when this instance loses leadership.
func WithOnLost(fn func()) LeaderOption {
	return func(le *LeaderElector) { le.onLost = fn }
}

// NewLeaderElector creates a leader elector using a dedicated KV bucket with TTL.
// The bucket is created with MemoryStorage for low latency.
func NewLeaderElector(ctx context.Context, js jetstream.JetStream, bucket, instanceID string, ttl time.Duration, opts ...LeaderOption) (*LeaderElector, error) {
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  bucket,
		TTL:     ttl,
		Storage: jetstream.MemoryStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("create leader KV bucket %s: %w", bucket, err)
	}

	le := &LeaderElector{
		js:         js,
		kv:         kv,
		bucket:     bucket,
		instanceID: instanceID,
		ttl:        ttl,
		done:       make(chan struct{}),
	}
	for _, o := range opts {
		o(le)
	}
	return le, nil
}

// Start begins the leader election loop. Call Stop to terminate.
func (le *LeaderElector) Start(ctx context.Context) {
	ctx, le.cancel = context.WithCancel(ctx)
	go le.run(ctx)
}

// Stop terminates the election loop. If this instance is leader, it deletes the key
// for instant failover instead of waiting for TTL expiry.
func (le *LeaderElector) Stop() {
	if le.cancel != nil {
		le.cancel()
	}
	<-le.done

	// Release leadership immediately so followers can take over.
	le.mu.RLock()
	wasLeader := le.leader
	le.mu.RUnlock()
	if wasLeader {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		le.kv.Delete(ctx, "leader")
		le.setLeader(false)
	}
}

// IsLeader reports whether this instance currently holds the leader lock.
func (le *LeaderElector) IsLeader() bool {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.leader
}

// InstanceID returns this elector's instance identifier.
func (le *LeaderElector) InstanceID() string {
	return le.instanceID
}

// LeaderID returns the current leader's instance ID by reading the KV key.
// Returns "" if no leader.
func (le *LeaderElector) LeaderID(ctx context.Context) string {
	entry, err := le.kv.Get(ctx, "leader")
	if err != nil {
		return ""
	}
	return string(entry.Value())
}

func (le *LeaderElector) run(ctx context.Context) {
	defer close(le.done)

	renewInterval := le.ttl / 3
	ticker := time.NewTicker(renewInterval)
	defer ticker.Stop()

	// Try to acquire on startup.
	le.tryAcquire(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			le.mu.RLock()
			isLeader := le.leader
			le.mu.RUnlock()

			if isLeader {
				le.tryRenew(ctx)
			} else {
				le.tryAcquire(ctx)
			}
		}
	}
}

func (le *LeaderElector) tryAcquire(ctx context.Context) {
	rev, err := le.kv.Create(ctx, "leader", []byte(le.instanceID))
	if err != nil {
		// Key exists — someone else is leader.
		return
	}
	le.mu.Lock()
	le.revision = rev
	le.mu.Unlock()
	le.setLeader(true)
	log.Printf("[leader] %s acquired leadership (rev=%d)", le.instanceID, rev)
}

func (le *LeaderElector) tryRenew(ctx context.Context) {
	le.mu.RLock()
	rev := le.revision
	le.mu.RUnlock()

	newRev, err := le.kv.Update(ctx, "leader", []byte(le.instanceID), rev)
	if err != nil {
		log.Printf("[leader] %s lost leadership (renew failed: %v)", le.instanceID, err)
		le.setLeader(false)
		return
	}
	le.mu.Lock()
	le.revision = newRev
	le.mu.Unlock()
}

func (le *LeaderElector) setLeader(elected bool) {
	le.mu.Lock()
	was := le.leader
	le.leader = elected
	le.mu.Unlock()

	if elected && !was && le.onElected != nil {
		le.onElected()
	}
	if !elected && was && le.onLost != nil {
		le.onLost()
	}
}
