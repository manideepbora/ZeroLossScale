package autoscale

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Max partition count enforced at the library level.
const MaxPartitions = 20

// ConsumerBackend abstracts how consumer instances are scaled.
// Implement this for Docker, Kubernetes, in-process, or any other runtime.
type ConsumerBackend interface {
	// ScaleConsumers adjusts the number of running consumers from oldCount to newCount.
	// The implementation handles starting/stopping individual instances as needed.
	ScaleConsumers(ctx context.Context, oldCount, newCount int) error
}

// Scaler orchestrates the repartition protocol for dynamic partition scaling.
// It signals mode changes via KV so any number of producers (in any language)
// can react by watching the KV bucket.
//
// Usage:
//
//	scaler := &autoscale.Scaler{
//	    JS:      js,
//	    PM:      partitionManager,
//	    Cfg:     autoscale.NewConfig("MY_ORDERS"),
//	    Backend: myDockerBackend,    // implements ConsumerBackend
//	}
//	err := scaler.Scale(ctx, 5) // scale to 5 partitions
type Scaler struct {
	JS      jetstream.JetStream
	PM      *PartitionManager
	Cfg     Config
	Backend ConsumerBackend
	DLQ     *DLQPublisher
}

// Scale changes the partition count using the buffer-drain-replay protocol.
// This is the simple (non-resumable) entry point — delegates to ScaleResumable.
func (s *Scaler) Scale(ctx context.Context, newCount int) error {
	return s.ScaleResumable(ctx, newCount)
}

// ScaleResumable performs repartition with step-level checkpointing to KV.
// If the process crashes, ResumeRepartition can continue from the last
// completed step.
func (s *Scaler) ScaleResumable(ctx context.Context, newCount int) error {
	if newCount < 1 || newCount > MaxPartitions {
		return fmt.Errorf("partition count must be 1-%d, got %d", MaxPartitions, newCount)
	}

	oldCount := s.PM.Count()
	if newCount == oldCount {
		return nil
	}

	kv, err := s.JS.KeyValue(ctx, s.Cfg.KVBucket)
	if err != nil {
		return fmt.Errorf("open KV for repartition state: %w", err)
	}

	state := &RepartitionState{
		Status:    RepartitionInProgress,
		Step:      0,
		OldCount:  oldCount,
		NewCount:  newCount,
		StartedAt: time.Now().UnixNano(),
	}
	rev, err := SaveRepartitionState(ctx, kv, state, 0)
	if err != nil {
		return fmt.Errorf("save initial repartition state: %w", err)
	}

	return s.executeFromStep(ctx, kv, state, rev)
}

// ResumeRepartition checks for an in-progress repartition and resumes it.
// Returns nil if no repartition is in progress.
func (s *Scaler) ResumeRepartition(ctx context.Context) error {
	kv, err := s.JS.KeyValue(ctx, s.Cfg.KVBucket)
	if err != nil {
		return fmt.Errorf("open KV for resume: %w", err)
	}

	state, rev, err := LoadRepartitionState(ctx, kv)
	if err != nil {
		return fmt.Errorf("load repartition state: %w", err)
	}
	if state == nil || state.Status != RepartitionInProgress {
		return nil
	}

	log.Printf("[scaler] resuming repartition %d -> %d from step %d",
		state.OldCount, state.NewCount, state.Step)
	return s.executeFromStep(ctx, kv, state, rev)
}

// revertToDirect is a best-effort mode revert used in error paths.
func (s *Scaler) revertToDirect(ctx context.Context, kv jetstream.KeyValue) {
	if err := s.PM.SetMode(ctx, KVModeDirect); err != nil {
		log.Printf("[scaler] WARNING: failed to revert to direct mode: %v", err)
	}
	if kv != nil {
		if err := ClearRepartitionState(ctx, kv); err != nil {
			log.Printf("[scaler] WARNING: failed to clear repartition state: %v", err)
		}
	}
}

// executeFromStep runs the repartition protocol, skipping already-completed steps.
func (s *Scaler) executeFromStep(ctx context.Context, kv jetstream.KeyValue, state *RepartitionState, rev uint64) error {
	oldCount := state.OldCount
	newCount := state.NewCount

	direction := "UP"
	if newCount < oldCount {
		direction = "DOWN"
	}
	log.Printf("[scaler] scaling %s: %d -> %d partitions (from step %d)",
		direction, oldCount, newCount, state.Step)

	checkpoint := func(step int) error {
		state.Step = step
		var err error
		rev, err = SaveRepartitionState(ctx, kv, state, rev)
		if err != nil {
			return fmt.Errorf("checkpoint step %d: %w", step, err)
		}
		return nil
	}

	// Step 1: Signal producers to buffer.
	if state.Step < 1 {
		if err := Retry(ctx, "set-buffer-mode", 3, 500*time.Millisecond, func() error {
			return s.PM.SetMode(ctx, KVModeBuffer)
		}); err != nil {
			return fmt.Errorf("set buffer mode: %w", err)
		}
		if err := checkpoint(1); err != nil {
			return err
		}
	}

	// Step 2: Wait for in-flight.
	if state.Step < 2 {
		time.Sleep(500 * time.Millisecond)
		if err := checkpoint(2); err != nil {
			return err
		}
	}

	// Step 3: Drain consumers.
	if state.Step < 3 {
		log.Printf("[scaler] draining %d partition consumers", oldCount)
		err := WaitForDrain(ctx, s.JS, s.Cfg.StreamName, s.Cfg.SubjectPrefix, oldCount, func(i int) string {
			return ConsumerName(i)
		}, s.Cfg.DrainTimeout)
		if err != nil {
			log.Printf("[scaler] drain failed: %v — reverting to direct mode", err)
			s.revertToDirect(ctx, kv)
			return fmt.Errorf("drain failed: %w", err)
		}
		if err := checkpoint(3); err != nil {
			return err
		}
	}

	// Step 4: Update partition count.
	if state.Step < 4 {
		if err := s.PM.SetCount(ctx, newCount); err != nil {
			log.Printf("[scaler] KV update failed: %v — reverting to direct mode", err)
			s.revertToDirect(ctx, kv)
			return fmt.Errorf("KV update failed: %w", err)
		}
		if err := checkpoint(4); err != nil {
			return err
		}
	}

	// Step 5: Scale consumers.
	if state.Step < 5 {
		if err := Retry(ctx, "scale-consumers", 3, 1*time.Second, func() error {
			return s.Backend.ScaleConsumers(ctx, oldCount, newCount)
		}); err != nil {
			log.Printf("[scaler] scale consumers failed after retries: %v", err)
			return fmt.Errorf("scale consumers: %w", err)
		}
		if err := checkpoint(5); err != nil {
			return err
		}
	}

	// Step 6: Switch to direct mode.
	if state.Step < 6 {
		if err := Retry(ctx, "set-direct-mode", 3, 500*time.Millisecond, func() error {
			return s.PM.SetMode(ctx, KVModeDirect)
		}); err != nil {
			log.Printf("[scaler] CRITICAL: could not restore direct mode: %v", err)
		}
		if err := checkpoint(6); err != nil {
			return err
		}
	}

	// Step 7: Replay buffer.
	if state.Step < 7 {
		replayed, err := s.ReplayBuffer(ctx, newCount)
		if err != nil {
			log.Printf("[scaler] WARNING: replay failed: %v", err)
		} else if replayed > 0 {
			log.Printf("[scaler] replayed %d buffered messages", replayed)
		}
		if err := checkpoint(7); err != nil {
			return err
		}
	}

	// Done — clear state.
	if err := ClearRepartitionState(ctx, kv); err != nil {
		log.Printf("[scaler] WARNING: could not clear repartition state: %v", err)
	}

	log.Printf("[scaler] scaled %s: %d -> %d partitions", direction, oldCount, newCount)
	return nil
}

// ReplayBuffer reads pending messages from the buffer stream and republishes
// them to partition subjects. Purges the buffer stream after successful replay.
func (s *Scaler) ReplayBuffer(ctx context.Context, partitionCount int) (int, error) {
	cons, err := s.JS.CreateOrUpdateConsumer(ctx, s.Cfg.BufferStreamName, jetstream.ConsumerConfig{
		Name:          "replay-consumer",
		Durable:       "replay-consumer",
		FilterSubject: s.Cfg.BufferSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return 0, fmt.Errorf("create replay consumer: %w", err)
	}
	defer s.JS.DeleteConsumer(ctx, s.Cfg.BufferStreamName, "replay-consumer")

	replayed := 0
	for {
		msgs, err := cons.Fetch(100, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			log.Printf("[scaler] replay fetch: %v", err)
			break
		}

		batchCount := 0
		for msg := range msgs.Messages() {
			var m Message
			if err := json.Unmarshal(msg.Data(), &m); err != nil {
				if s.DLQ != nil {
					s.DLQ.Send(ctx, msg.Data(), msg.Subject(), DLQReasonUnmarshal, err)
				}
				msg.Ack()
				continue
			}

			partition := PartitionForKey(m.Key, partitionCount)
			m.Partition = partition
			subject := SubjectForPartition(s.Cfg.SubjectPrefix, partition)

			data, err := json.Marshal(m)
			if err != nil {
				if s.DLQ != nil {
					s.DLQ.Send(ctx, msg.Data(), msg.Subject(), DLQReasonReplayFailed, err)
				}
				msg.Ack()
				continue
			}

			// Deterministic MsgID — makes replay idempotent across crash/resume.
			msgID := fmt.Sprintf("replay-%s-%d", m.Key, m.Sequence)

			pubErr := Retry(ctx, "replay-publish", 3, 200*time.Millisecond, func() error {
				_, err := s.JS.Publish(ctx, subject, data, jetstream.WithMsgID(msgID))
				return err
			})
			if pubErr != nil {
				if s.DLQ != nil {
					s.DLQ.Send(ctx, msg.Data(), msg.Subject(), DLQReasonReplayFailed, pubErr)
				}
				msg.Ack() // ack so replay can proceed; message is in DLQ
				continue
			}
			msg.Ack()
			replayed++
			batchCount++
		}

		if batchCount == 0 {
			break
		}
	}

	if replayed > 0 {
		if stream, err := s.JS.Stream(ctx, s.Cfg.BufferStreamName); err == nil {
			if err := stream.Purge(ctx); err != nil {
				log.Printf("[scaler] WARNING: buffer purge failed: %v", err)
			}
		}
	}

	return replayed, nil
}

// RegisterStream creates the streams and KV bucket for a given config,
// seeds the initial partition count and mode. Called by the controlplane
// when a producer registers.
func RegisterStream(ctx context.Context, js jetstream.JetStream, cfg Config, initialPartitions int) (*PartitionManager, error) {
	if err := cfg.EnsureStreams(ctx, js); err != nil {
		return nil, err
	}

	pm, err := NewPartitionManager(ctx, js, cfg.KVBucket, initialPartitions)
	if err != nil {
		return nil, fmt.Errorf("partition manager: %w", err)
	}

	if err := pm.SetMode(ctx, KVModeDirect); err != nil {
		return nil, fmt.Errorf("seed mode: %w", err)
	}

	return pm, nil
}

// ReadPartitionCount reads the current partition count from the KV bucket.
// Used by producers on startup before the watch loop kicks in.
func ReadPartitionCount(ctx context.Context, js jetstream.JetStream, kvBucket string) (int, error) {
	kv, err := js.KeyValue(ctx, kvBucket)
	if err != nil {
		return 0, fmt.Errorf("open KV bucket %s: %w", kvBucket, err)
	}
	entry, err := kv.Get(ctx, KVKeyPartitionCount)
	if err != nil {
		return 0, fmt.Errorf("get partition_count: %w", err)
	}
	count, err := strconv.Atoi(string(entry.Value()))
	if err != nil {
		return 0, fmt.Errorf("parse partition_count: %w", err)
	}
	return count, nil
}
