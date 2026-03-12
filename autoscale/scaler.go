package autoscale

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
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
	NC      *nats.Conn // underlying NATS connection (used to create dedicated replay connections)
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

// executeFromStep runs the v2 ordered repartition protocol, skipping
// already-completed steps.
//
// v2 protocol (replay-before-direct) ensures strict per-key ordering:
//
//	Step 1: Buffer mode ON — producers write to buffer stream
//	Step 2: Wait 500ms for in-flight publishes to land
//	Step 3: Fast drain — old consumers finish pending (fast: no new msgs arriving)
//	Step 4: Update partition count in KV
//	Step 5: Scale consumers (add new, remove extras)
//	Step 6: Continuous replay — drain buffer into new partition subjects
//	Step 7: Buffer empty → switch to direct mode
//	Step 8: Straggler replay + purge buffer
//
// The critical ordering fix: replay (step 6) happens BEFORE direct mode
// (step 7). Consumers see buffered messages first, then direct messages.
// This guarantees monotonically increasing sequences per key.
func (s *Scaler) executeFromStep(ctx context.Context, kv jetstream.KeyValue, state *RepartitionState, rev uint64) error {
	oldCount := state.OldCount
	newCount := state.NewCount

	direction := "UP"
	if newCount < oldCount {
		direction = "DOWN"
	}
	log.Printf("[scaler] v2 scaling %s: %d -> %d partitions (from step %d)",
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
		log.Printf("[scaler] step 1: buffer mode ON")
		if err := checkpoint(1); err != nil {
			return err
		}
	}

	// Step 2: Wait for in-flight publishes to complete.
	if state.Step < 2 {
		time.Sleep(s.Cfg.InFlightWait)
		log.Printf("[scaler] step 2: in-flight wait done")
		if err := checkpoint(2); err != nil {
			return err
		}
	}

	// Step 3: Fast drain — old consumers finish pending messages.
	// This is fast because buffer mode means no new messages arrive on
	// partition subjects; consumers only need to finish what's already pending.
	if state.Step < 3 {
		log.Printf("[scaler] step 3: fast drain of %d partition consumers", oldCount)
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

	// Step 4: Update partition count in KV.
	if state.Step < 4 {
		if err := s.PM.SetCount(ctx, newCount); err != nil {
			log.Printf("[scaler] KV update failed: %v — reverting to direct mode", err)
			s.revertToDirect(ctx, kv)
			return fmt.Errorf("KV update failed: %w", err)
		}
		log.Printf("[scaler] step 4: partition count updated to %d", newCount)
		if err := checkpoint(4); err != nil {
			return err
		}
	}

	// Step 5: Restart ALL consumers fresh.
	// Critical for v2: old consumers retain lastSeqByKey state from before
	// buffer mode. If we don't restart them, they'll see replayed messages
	// (lower seqs) as violations. Stop all → start fresh at new count.
	if state.Step < 5 {
		// First stop all existing consumers.
		if err := s.Backend.ScaleConsumers(ctx, oldCount, 0); err != nil {
			log.Printf("[scaler] stop consumers failed: %v — reverting to direct mode", err)
			s.revertToDirect(ctx, kv)
			return fmt.Errorf("stop consumers: %w", err)
		}
		log.Printf("[scaler] step 5a: stopped all consumers")

		// Then start fresh consumers at the new count.
		if err := Retry(ctx, "scale-consumers", 3, 1*time.Second, func() error {
			return s.Backend.ScaleConsumers(ctx, 0, newCount)
		}); err != nil {
			log.Printf("[scaler] scale consumers failed after retries: %v", err)
			return fmt.Errorf("scale consumers: %w", err)
		}
		log.Printf("[scaler] step 5b: started %d fresh consumers", newCount)
		if err := checkpoint(5); err != nil {
			return err
		}
	}

	// Steps 6-8: Replay buffer, switch to direct, drain stragglers.
	//
	// Critical ordering guarantee: the replay loop switches to direct mode
	// WHILE STILL DRAINING the buffer. This ensures:
	//   1. Producer stops writing to buffer (direct mode via KV watch)
	//   2. Replay finishes draining the now-finite buffer
	//   3. ALL buffered messages are published to partition subjects BEFORE
	//      any direct messages arrive (producer has KV watch latency)
	//   4. No straggler replay needed — everything is drained in one pass
	if state.Step < 8 {
		replayed, err := s.ReplayContinuous(ctx, newCount)
		if err != nil {
			log.Printf("[scaler] WARNING: continuous replay failed: %v", err)
		}
		log.Printf("[scaler] steps 6-8: replayed %d messages, direct mode ON", replayed)
		if err := checkpoint(8); err != nil {
			return err
		}
	}

	// Done — clear state.
	if err := ClearRepartitionState(ctx, kv); err != nil {
		log.Printf("[scaler] WARNING: could not clear repartition state: %v", err)
	}

	log.Printf("[scaler] v2 scaled %s: %d -> %d partitions", direction, oldCount, newCount)
	return nil
}

// ReplayContinuous drains the buffer stream into new partition subjects.
//
// This method implements the critical ordering guarantee by integrating the
// direct mode switch INTO the replay loop:
//
//  1. Phase 1 (catch-up): Replay messages while producer is in buffer mode.
//     Fetch-based loop guarantees strict stream ordering.
//  2. Phase 2 (switch): When replay is roughly caught up, switch to direct mode.
//     Producer stops writing to buffer (after KV watch propagation).
//  3. Phase 3 (drain): Finish draining the now-finite buffer. No new messages
//     arrive, so this completes quickly.
//
// After drain, ALL buffered messages have been published to partition subjects
// BEFORE any direct messages (producer has KV watch latency). Purges the buffer
// at the end.
func (s *Scaler) ReplayContinuous(ctx context.Context, partitionCount int) (int, error) {
	// Create a dedicated NATS connection for replay publishing.
	// Raw nats.Conn.PublishMsg writes to TCP without waiting for JetStream
	// acks — orders of magnitude faster than sync Publish for replay.
	var pubConn *nats.Conn
	if s.NC != nil {
		var err error
		pubConn, err = nats.Connect(s.NC.ConnectedUrl(),
			nats.Name("replay-dedicated"),
			nats.MaxReconnects(3),
		)
		if err != nil {
			log.Printf("[scaler] WARNING: dedicated replay connection failed: %v (falling back to shared JS)", err)
		} else {
			defer pubConn.Close()
		}
	}

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
	deadline := time.After(s.Cfg.DrainTimeout)

	// Publish helper — uses dedicated raw connection if available, falls back to JetStream.
	publish := func(subject string, data []byte, msgID string) error {
		if pubConn != nil {
			return pubConn.PublishMsg(&nats.Msg{
				Subject: subject,
				Data:    data,
				Header:  nats.Header{"Nats-Msg-Id": []string{msgID}},
			})
		}
		_, err := s.JS.Publish(ctx, subject, data, jetstream.WithMsgID(msgID))
		return err
	}

	replayBatch := func() (int, error) {
		msgs, err := cons.Fetch(s.Cfg.ReplayBatchSize, jetstream.FetchMaxWait(s.Cfg.ReplayFetchWait))
		if err != nil {
			return 0, fmt.Errorf("replay fetch: %w", err)
		}
		count := 0
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

			if pubErr := publish(subject, data, fmt.Sprintf("replay-%s-%d", m.Key, m.Sequence)); pubErr != nil {
				if s.DLQ != nil {
					s.DLQ.Send(ctx, msg.Data(), msg.Subject(), DLQReasonReplayFailed, pubErr)
				}
				msg.Ack()
				continue
			}
			msg.Ack()
			replayed++
			count++
		}
		if pubConn != nil && count > 0 {
			pubConn.Flush()
		}
		return count, nil
	}

	// drainBuffer flushes all remaining buffer messages, waits briefly for
	// straggler producer messages, then drains again.
	drainBuffer := func() {
		for {
			n, err := replayBatch()
			if err != nil {
				log.Printf("[scaler] replay batch error during drain: %v", err)
				break
			}
			if n == 0 {
				break
			}
		}
		time.Sleep(s.Cfg.StragglerWait)
		for {
			n, err := replayBatch()
			if err != nil {
				log.Printf("[scaler] replay batch error during straggler drain: %v", err)
				break
			}
			if n == 0 {
				break
			}
		}
	}

	// Phase 1: Catch-up replay while producer is in buffer mode.
	// Monitor NumPending via consumer Info. When it drops below threshold,
	// switch to direct mode (phase 2).
	infoTicker := time.NewTicker(s.Cfg.ReplayPollInterval)
	defer infoTicker.Stop()

	var replayErr error
replayLoop:
	for replayErr == nil {
		select {
		case <-deadline:
			log.Printf("[scaler] continuous replay timeout after %v (replayed %d)", s.Cfg.DrainTimeout, replayed)
			if err := s.PM.SetMode(ctx, KVModeDirect); err != nil {
				log.Printf("[scaler] WARNING: failed to set direct mode on timeout: %v", err)
			}
			drainBuffer()
			replayErr = fmt.Errorf("replay timeout after %v", s.Cfg.DrainTimeout)

		case <-ctx.Done():
			// Best-effort drain before returning — don't lose buffered messages.
			log.Printf("[scaler] context cancelled, draining buffer before exit (replayed %d so far)", replayed)
			if err := s.PM.SetMode(context.Background(), KVModeDirect); err != nil {
				log.Printf("[scaler] WARNING: failed to set direct mode on cancel: %v", err)
			}
			drainBuffer()
			replayErr = ctx.Err()

		case <-infoTicker.C:
			ci, err := cons.Info(ctx)
			if err != nil {
				log.Printf("[scaler] replay consumer info error: %v", err)
				continue
			}
			if ci.NumPending < s.Cfg.ReplaySwitchThreshold {
				if err := s.PM.SetMode(ctx, KVModeDirect); err != nil {
					log.Printf("[scaler] WARNING: failed to set direct mode: %v", err)
					continue
				}
				log.Printf("[scaler] step 7: direct mode ON (pending=%d, replayed=%d)", ci.NumPending, replayed)
				drainBuffer()
				break replayLoop
			}

		default:
			if _, err := replayBatch(); err != nil {
				log.Printf("[scaler] replay batch error: %v", err)
			}
		}
	}

	if stream, err := s.JS.Stream(ctx, s.Cfg.BufferStreamName); err == nil {
		if err := stream.Purge(ctx); err != nil {
			log.Printf("[scaler] WARNING: buffer purge failed: %v", err)
		}
	}

	log.Printf("[scaler] continuous replay complete: %d messages replayed", replayed)
	return replayed, replayErr
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
