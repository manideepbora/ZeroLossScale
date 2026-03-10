package autoscale

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/nats-io/nats.go/jetstream"
)

// ConsumerPool manages a dynamic set of partition consumers.
type ConsumerPool struct {
	js         jetstream.JetStream
	streamName string
	prefix     string
	cfg        Config
	dlq        *DLQPublisher

	mu        sync.Mutex
	consumers map[int]context.CancelFunc // partition -> cancel func
	// results uses an incrementing key so that restarted consumers don't
	// overwrite results from previous runs.
	results   []*ConsumerResult
	resultIdx map[int]int // partition -> index into results (current run)
	wg        sync.WaitGroup

	// Per-account last consumed sequence (shared across all consumers).
	consumedSeqs map[string]*AccountSeq

	totalReceived   atomic.Int64
	totalViolations atomic.Int64
	totalGaps       atomic.Int64 // number of gap events (skipped sequences)
	totalMissing    atomic.Int64 // total missing messages across all gaps
}

func NewConsumerPool(js jetstream.JetStream, streamName, prefix string, cfg Config) *ConsumerPool {
	var dlq *DLQPublisher
	if cfg.DLQSubject != "" {
		dlq = NewDLQPublisher(js, cfg.DLQSubject)
	}

	return &ConsumerPool{
		js:           js,
		streamName:   streamName,
		prefix:       prefix,
		cfg:          cfg,
		dlq:          dlq,
		consumers:    make(map[int]context.CancelFunc),
		resultIdx:    make(map[int]int),
		consumedSeqs: make(map[string]*AccountSeq),
	}
}

// ConsumerName returns the durable consumer name for a partition.
func ConsumerName(partition int) string {
	return fmt.Sprintf("consumer-p%d", partition)
}

// ScaleTo adjusts the consumer pool to exactly `count` consumers.
func (cp *ConsumerPool) ScaleTo(ctx context.Context, count int) {
	cp.mu.Lock()

	// Collect partitions to start and stop.
	var toStart []int
	for i := 0; i < count; i++ {
		if _, exists := cp.consumers[i]; !exists {
			toStart = append(toStart, i)
		}
	}

	var toStop []int
	for i := range cp.consumers {
		if i >= count {
			toStop = append(toStop, i)
		}
	}

	// Stop and delete consumers for removed partitions (while holding lock).
	for _, i := range toStop {
		cp.consumers[i]() // cancel
		delete(cp.consumers, i)
		delete(cp.resultIdx, i)
		name := ConsumerName(i)
		if err := cp.js.DeleteConsumer(ctx, cp.streamName, name); err != nil {
			log.Printf("[consumer-pool] failed to delete consumer %s: %v", name, err)
		}
		log.Printf("[consumer-pool] stopped and deleted consumer for partition %d", i)
	}

	// Register new consumers in the map (while holding lock).
	type pending struct {
		partition int
		cancel    context.CancelFunc
		cctx      context.Context
		result    *ConsumerResult
	}
	var pendings []pending
	for _, i := range toStart {
		cctx, cancel := context.WithCancel(ctx)
		cp.consumers[i] = cancel
		r := &ConsumerResult{Partition: i}
		cp.results = append(cp.results, r)
		cp.resultIdx[i] = len(cp.results) - 1
		pendings = append(pendings, pending{i, cancel, cctx, r})
	}

	cp.mu.Unlock()

	// Start consumers AFTER releasing the lock so callbacks can acquire it.
	for _, p := range pendings {
		cc, err := cp.startConsumer(ctx, p.partition, p.result)
		if err != nil {
			log.Printf("[consumer-pool] failed to start partition %d: %v", p.partition, err)
			p.cancel()
			cp.mu.Lock()
			delete(cp.consumers, p.partition)
			cp.mu.Unlock()
			continue
		}
		cp.wg.Add(1)
		go func(cctx context.Context, cc jetstream.ConsumeContext) {
			defer cp.wg.Done()
			<-cctx.Done()
			cc.Drain() // waits for in-flight message callbacks to complete
		}(p.cctx, cc)
		log.Printf("[consumer-pool] started consumer for partition %d", p.partition)
	}
}

// startConsumer creates a JetStream durable consumer and begins consuming.
// It purges the subject first to prevent re-delivery of old messages
// from a previous partition layout.
func (cp *ConsumerPool) startConsumer(ctx context.Context, partition int, result *ConsumerResult) (jetstream.ConsumeContext, error) {
	filterSubject := SubjectForPartition(cp.prefix, partition)
	name := ConsumerName(partition)

	// Purge old messages on this subject to avoid re-delivery after repartition.
	stream, err := cp.js.Stream(ctx, cp.streamName)
	if err != nil {
		log.Printf("[consumer-pool] WARNING: cannot get stream for purge: %v", err)
	} else if err := stream.Purge(ctx, jetstream.WithPurgeSubject(filterSubject)); err != nil {
		log.Printf("[consumer-pool] WARNING: purge %s failed: %v (stale messages may be re-delivered)", filterSubject, err)
	}

	cons, err := cp.js.CreateOrUpdateConsumer(ctx, cp.streamName, jetstream.ConsumerConfig{
		Name:          name,
		Durable:       name,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       cp.cfg.AckWait,
		MaxDeliver:    cp.cfg.MaxDeliver,
		MaxAckPending: cp.cfg.MaxAckPending,
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer %s: %w", name, err)
	}

	lastSeqByKey := make(map[string]int)

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		cp.totalReceived.Add(1)

		var m Message
		if err := json.Unmarshal(msg.Data(), &m); err != nil {
			cp.dlq.Send(context.Background(), msg.Data(), msg.Subject(), DLQReasonUnmarshal, err)
			if ackErr := msg.Ack(); ackErr != nil {
				log.Printf("[consumer] ack error on unmarshal: %v", ackErr)
			}
			return
		}

		// Check per-key ordering and detect gaps.
		if last, ok := lastSeqByKey[m.Key]; ok {
			if m.Sequence <= last {
				cp.totalViolations.Add(1)
				log.Printf("[consumer] VIOLATION partition=%d key=%s seq=%d after=%d", partition, m.Key, m.Sequence, last)
			} else if m.Sequence > last+1 {
				gap := m.Sequence - last - 1
				cp.totalGaps.Add(1)
				cp.totalMissing.Add(int64(gap))
				log.Printf("[consumer] GAP partition=%d key=%s expected=%d got=%d missing=%d", partition, m.Key, last+1, m.Sequence, gap)
			}
		}
		lastSeqByKey[m.Key] = m.Sequence

		cp.mu.Lock()
		result.Received++
		result.Messages = append(result.Messages, m)
		// Update per-account consumed sequence.
		if as, ok := cp.consumedSeqs[m.Key]; ok {
			if m.Sequence > as.Sequence+1 {
				gap := m.Sequence - as.Sequence - 1
				as.Gaps++
				as.GapTotal += gap
			}
			as.Sequence = m.Sequence
			as.Partition = partition
		} else {
			cp.consumedSeqs[m.Key] = &AccountSeq{
				Key:       m.Key,
				Sequence:  m.Sequence,
				Partition: partition,
			}
		}
		cp.mu.Unlock()

		if ackErr := msg.Ack(); ackErr != nil {
			log.Printf("[consumer] ack error partition=%d key=%s seq=%d: %v", partition, m.Key, m.Sequence, ackErr)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("consume %s: %w", name, err)
	}

	return cc, nil
}

// StopAll gracefully stops all consumers and waits for them to finish.
func (cp *ConsumerPool) StopAll() {
	cp.mu.Lock()
	for _, cancel := range cp.consumers {
		cancel()
	}
	cp.mu.Unlock()
	cp.wg.Wait()
}

// Results returns deep copies of all collected results across all consumer instances.
// Safe to read without holding any lock.
func (cp *ConsumerPool) Results() []*ConsumerResult {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	out := make([]*ConsumerResult, len(cp.results))
	for i, r := range cp.results {
		msgs := make([]Message, len(r.Messages))
		copy(msgs, r.Messages)
		out[i] = &ConsumerResult{
			Partition: r.Partition,
			Received:  r.Received,
			Messages:  msgs,
		}
	}
	return out
}

// ConsumedSeqs returns a snapshot of per-account consumed sequences.
func (cp *ConsumerPool) ConsumedSeqs() map[string]AccountSeq {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	out := make(map[string]AccountSeq, len(cp.consumedSeqs))
	for k, v := range cp.consumedSeqs {
		out[k] = *v
	}
	return out
}

func (cp *ConsumerPool) TotalReceived() int64   { return cp.totalReceived.Load() }
func (cp *ConsumerPool) TotalViolations() int64 { return cp.totalViolations.Load() }
func (cp *ConsumerPool) TotalGaps() int64       { return cp.totalGaps.Load() }
func (cp *ConsumerPool) TotalMissing() int64    { return cp.totalMissing.Load() }
