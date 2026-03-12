package autoscale

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Global monotonic counter for dedup MsgIDs — survives repartition flushes.
// Seeded from current time to ensure uniqueness across process restarts.
var publishSeqCounter atomic.Int64

func init() {
	publishSeqCounter.Store(time.Now().UnixNano())
}

// DirectPublisher publishes messages directly to partition subjects.
// During repartition, it automatically switches to buffer mode by watching
// the KV bucket managed by the controlplane.
//
// Usage:
//
//	pub, _ := autoscale.NewDirectPublisher(ctx, js, "MY_ORDERS")
//	pub.Publish(ctx, autoscale.Message{Key: "acct-42", Sequence: 1, Payload: "data"})
type DirectPublisher struct {
	js  jetstream.JetStream
	cfg Config

	mu             sync.RWMutex
	buffering      bool
	partitionCount int

	watchHealthy atomic.Bool
	cancelWatch  context.CancelFunc
}

// NewDirectPublisher creates a publisher that watches KV for mode/partition changes.
// It reads the initial state from KV and starts a background watcher.
func NewDirectPublisher(ctx context.Context, js jetstream.JetStream, streamName string) (*DirectPublisher, error) {
	cfg := NewConfig(streamName)

	count, err := ReadPartitionCount(ctx, js, cfg.KVBucket)
	if err != nil {
		return nil, fmt.Errorf("read initial partition count: %w", err)
	}

	p := &DirectPublisher{
		js:             js,
		cfg:            cfg,
		partitionCount: count,
		buffering:      false,
	}

	watchCtx, cancel := context.WithCancel(ctx)
	p.cancelWatch = cancel
	go p.watchKV(watchCtx)

	return p, nil
}

// watchKV watches the KV bucket for mode and partition_count changes.
// Reconnects automatically on watcher disconnection.
func (p *DirectPublisher) watchKV(ctx context.Context) {
	backoff := 1 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		kv, err := p.js.KeyValue(ctx, p.cfg.KVBucket)
		if err != nil {
			log.Printf("[publisher] WARNING: cannot open KV %s: %v (retrying in %v)", p.cfg.KVBucket, err, backoff)
			p.watchHealthy.Store(false)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}

		// WatchAll replays all current values as initial entries before
		// streaming live updates, so there is no gap between reading
		// initial state and receiving subsequent changes.
		watcher, err := kv.WatchAll(ctx)
		if err != nil {
			log.Printf("[publisher] WARNING: cannot start KV watch: %v (retrying in %v)", err, backoff)
			p.watchHealthy.Store(false)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}

		p.watchHealthy.Store(true)
		backoff = 1 * time.Second
		log.Printf("[publisher] KV watcher connected")

		disconnected := false
		for !disconnected {
			select {
			case <-ctx.Done():
				watcher.Stop()
				return
			case entry, ok := <-watcher.Updates():
				if !ok {
					log.Printf("[publisher] WARNING: KV watcher channel closed, reconnecting...")
					p.watchHealthy.Store(false)
					disconnected = true
					break
				}
				if entry == nil {
					continue // initial values done marker
				}

				switch entry.Key() {
				case KVKeyMode:
					mode := string(entry.Value())
					p.mu.Lock()
					p.buffering = (mode == KVModeBuffer)
					p.mu.Unlock()
					log.Printf("[publisher] mode changed to %s", mode)

				case KVKeyPartitionCount:
					count, err := strconv.Atoi(string(entry.Value()))
					if err != nil {
						continue
					}
					p.mu.Lock()
					p.partitionCount = count
					p.mu.Unlock()
					log.Printf("[publisher] partition count changed to %d", count)
				}
			}
		}

		watcher.Stop()
	}
}


// Publish sends a message to the appropriate subject based on current mode.
// In direct mode: publishes to the partition subject (e.g. "MY_ORDERS.02").
// In buffer mode: publishes to the buffer subject (e.g. "MY_ORDERS_BUFFER").
func (p *DirectPublisher) Publish(ctx context.Context, msg Message) error {
	p.mu.RLock()
	buffering := p.buffering
	count := p.partitionCount
	p.mu.RUnlock()

	if count < 1 {
		return fmt.Errorf("partition count not yet initialized")
	}

	partition := PartitionForKey(msg.Key, count)
	msg.Partition = partition

	var subject string
	if buffering {
		subject = p.cfg.BufferSubject
	} else {
		subject = SubjectForPartition(p.cfg.SubjectPrefix, partition)
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	nonce := publishSeqCounter.Add(1)
	msgID := fmt.Sprintf("%s-%d-%d", msg.Key, msg.Sequence, nonce)

	_, err = p.js.Publish(ctx, subject, data, jetstream.WithMsgID(msgID))
	return err
}

// Stop cancels the KV watcher.
func (p *DirectPublisher) Stop() {
	if p.cancelWatch != nil {
		p.cancelWatch()
	}
}

// IsBuffering reports whether the publisher is in buffer mode.
func (p *DirectPublisher) IsBuffering() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.buffering
}

// PartitionCount returns the current partition count.
func (p *DirectPublisher) PartitionCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.partitionCount
}

// StreamName returns the configured stream name.
func (p *DirectPublisher) StreamName() string {
	return p.cfg.StreamName
}

// WatchHealthy reports whether the KV watcher is connected.
func (p *DirectPublisher) WatchHealthy() bool {
	return p.watchHealthy.Load()
}
