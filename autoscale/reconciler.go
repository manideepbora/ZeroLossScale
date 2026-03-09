package autoscale

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// KVKeyDesiredPartitions is written by the /scale API or KEDA.
	// The reconciler watches it and triggers repartition when desired != current.
	KVKeyDesiredPartitions = "desired_partitions"
)

// Reconciler watches the desired_partitions KV key and triggers repartition
// when the desired count differs from the current count. Only the leader
// instance should start the reconciler.
type Reconciler struct {
	js     jetstream.JetStream
	scaler *Scaler
	pm     *PartitionManager
	cfg    Config

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewReconciler creates a reconciler for a given stream configuration.
func NewReconciler(js jetstream.JetStream, scaler *Scaler, pm *PartitionManager, cfg Config) *Reconciler {
	return &Reconciler{
		js:     js,
		scaler: scaler,
		pm:     pm,
		cfg:    cfg,
	}
}

// Start begins watching the desired_partitions key. Call Stop to terminate.
// Should only be called when this instance is the leader.
func (r *Reconciler) Start(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cancel != nil {
		return // already running
	}

	watchCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.done = make(chan struct{})

	go r.run(watchCtx)
	log.Printf("[reconciler] started for stream %s", r.cfg.StreamName)
}

// Stop terminates the reconciliation loop.
func (r *Reconciler) Stop() {
	r.mu.Lock()
	cancel := r.cancel
	done := r.done
	r.cancel = nil
	r.mu.Unlock()

	if cancel != nil {
		cancel()
		<-done
	}
	log.Printf("[reconciler] stopped for stream %s", r.cfg.StreamName)
}

func (r *Reconciler) run(ctx context.Context) {
	defer close(r.done)

	// First, check for interrupted repartition and resume it.
	if err := r.scaler.ResumeRepartition(ctx); err != nil {
		log.Printf("[reconciler] resume repartition failed: %v", err)
	}

	backoff := 1 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		kv, err := r.js.KeyValue(ctx, r.cfg.KVBucket)
		if err != nil {
			log.Printf("[reconciler] cannot open KV %s: %v (retrying in %v)", r.cfg.KVBucket, err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}

		// Watch for desired_partitions changes.
		watcher, err := kv.Watch(ctx, KVKeyDesiredPartitions)
		if err != nil {
			log.Printf("[reconciler] cannot watch %s: %v (retrying in %v)", KVKeyDesiredPartitions, err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}

		backoff = 1 * time.Second // reset on success
		log.Printf("[reconciler] KV watcher connected for %s", r.cfg.StreamName)

		disconnected := false
		for !disconnected {
			select {
			case <-ctx.Done():
				watcher.Stop()
				return
			case entry, ok := <-watcher.Updates():
				if !ok {
					log.Printf("[reconciler] WARNING: watcher channel closed, reconnecting...")
					disconnected = true
					break
				}
				if entry == nil {
					continue // initial values done
				}

				desired, err := strconv.Atoi(string(entry.Value()))
				if err != nil || desired < 1 {
					log.Printf("[reconciler] invalid desired_partitions value: %s", string(entry.Value()))
					continue
				}

				current := r.pm.Count()
				if desired == current {
					continue
				}

				log.Printf("[reconciler] desired=%d current=%d — triggering repartition", desired, current)

				// Debounce: wait briefly in case rapid updates are coming.
				select {
				case <-ctx.Done():
					watcher.Stop()
					return
				case <-time.After(1 * time.Second):
				}

				// Re-read desired in case it changed during debounce.
				latestEntry, err := kv.Get(ctx, KVKeyDesiredPartitions)
				if err == nil {
					if latestDesired, err := strconv.Atoi(string(latestEntry.Value())); err == nil && latestDesired > 0 {
						desired = latestDesired
					}
				}

				if desired == r.pm.Count() {
					continue
				}

				if err := r.scaler.ScaleResumable(ctx, desired); err != nil {
					log.Printf("[reconciler] repartition failed: %v", err)
				}
			}
		}

		watcher.Stop()
	}
}

// SetDesired writes the desired partition count to KV. Any replica can call this.
// The leader's reconciler will pick it up and trigger repartition.
func SetDesired(ctx context.Context, js jetstream.JetStream, kvBucket string, count int) error {
	kv, err := js.KeyValue(ctx, kvBucket)
	if err != nil {
		return err
	}
	_, err = kv.Put(ctx, KVKeyDesiredPartitions, []byte(strconv.Itoa(count)))
	return err
}
