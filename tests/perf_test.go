package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"nats-poc/autoscale"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// =========================================================================
// PERF 1: Throughput — max messages/sec through the full pipeline
// =========================================================================
func TestPerfThroughput(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	for _, partitions := range []int{1, 3, 5} {
		t.Run(fmt.Sprintf("partitions=%d", partitions), func(t *testing.T) {
			cp := setupControlPlane(t, ctx, js)
			defer cp.DestroyAll(ctx)

			if partitions > 1 {
				if err := cp.Repartition(ctx, partitions); err != nil {
					t.Fatalf("repartition: %v", err)
				}
			}

			keys := acctKeys(10)
			msgCount := 5000
			seqs := make(map[string]int)

			// Measure publish throughput.
			pubStart := time.Now()
			for i := 0; i < msgCount; i++ {
				key := keys[i%len(keys)]
				seqs[key]++
				msg := autoscale.Message{
					Key:       key,
					Sequence:  seqs[key],
					Payload:   fmt.Sprintf("%s-%d", key, seqs[key]),
					Timestamp: time.Now().UnixNano(),
				}
				if err := cp.Publisher.Publish(ctx, msg); err != nil {
					t.Fatalf("publish: %v", err)
				}
			}
			pubDuration := time.Since(pubStart)
			pubRate := float64(msgCount) / pubDuration.Seconds()

			// Wait for all messages to be consumed.
			if !waitForConsumers(cp.Pool, msgCount, 30*time.Second) {
				t.Fatalf("timeout: expected %d, got %d",
					msgCount, cp.Pool.TotalReceived())
			}
			e2eDuration := time.Since(pubStart)
			e2eRate := float64(msgCount) / e2eDuration.Seconds()

			t.Logf("  Messages:       %d", msgCount)
			t.Logf("  Publish:        %.0f msg/s (%.2fs)", pubRate, pubDuration.Seconds())
			t.Logf("  End-to-end:     %.0f msg/s (%.2fs)", e2eRate, e2eDuration.Seconds())
			t.Logf("  Received:       %d", cp.Pool.TotalReceived())
			t.Logf("  Violations:     %d", cp.Pool.TotalViolations())

			if cp.Pool.TotalViolations() > 0 {
				t.Errorf("ordering violations: %d", cp.Pool.TotalViolations())
			}
		})
	}
}

// =========================================================================
// PERF 2: Concurrent producers — multiple goroutines publishing simultaneously
// =========================================================================
func TestPerfConcurrentProducers(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	if err := cp.Repartition(ctx, 3); err != nil {
		t.Fatalf("repartition: %v", err)
	}

	numProducers := 5
	msgsPerProducer := 1000

	// Each producer gets its own set of keys so per-key ordering is maintained.
	producerKeys := make([][]string, numProducers)
	for p := 0; p < numProducers; p++ {
		pkeys := make([]string, 4)
		for k := 0; k < 4; k++ {
			pkeys[k] = fmt.Sprintf("prod%d-acct-%d", p, k)
		}
		producerKeys[p] = pkeys
	}

	var totalPublished atomic.Int64
	var wg sync.WaitGroup

	start := time.Now()
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			pkeys := producerKeys[producerID]
			seqs := make(map[string]int)
			for i := 0; i < msgsPerProducer; i++ {
				key := pkeys[i%len(pkeys)]
				seqs[key]++
				msg := autoscale.Message{
					Key:       key,
					Sequence:  seqs[key],
					Payload:   fmt.Sprintf("p%d-%s-%d", producerID, key, seqs[key]),
					Timestamp: time.Now().UnixNano(),
				}
				if err := cp.Publisher.Publish(ctx, msg); err != nil {
					t.Errorf("publish: %v", err)
					return
				}
				totalPublished.Add(1)
			}
		}(p)
	}
	wg.Wait()
	pubDuration := time.Since(start)

	total := int(totalPublished.Load())
	pubRate := float64(total) / pubDuration.Seconds()

	if !waitForConsumers(cp.Pool, total, 30*time.Second) {
		t.Fatalf("timeout: expected %d, got %d", total, cp.Pool.TotalReceived())
	}
	e2eDuration := time.Since(start)

	t.Logf("  Producers:      %d", numProducers)
	t.Logf("  Total messages:  %d", total)
	t.Logf("  Publish:        %.0f msg/s (%.2fs)", pubRate, pubDuration.Seconds())
	t.Logf("  End-to-end:     %.0f msg/s (%.2fs)", float64(total)/e2eDuration.Seconds(), e2eDuration.Seconds())
	t.Logf("  Violations:     %d", cp.Pool.TotalViolations())

	if cp.Pool.TotalViolations() > 0 {
		t.Errorf("ordering violations: %d", cp.Pool.TotalViolations())
	}
}

// =========================================================================
// PERF 3: Repartition latency — how long does a scale event take?
// =========================================================================
func TestPerfRepartitionLatency(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	keys := acctKeys(10)

	// Pre-publish some messages to make drain non-trivial.
	seqs := make(map[string]int)
	for i := 0; i < 500; i++ {
		key := keys[i%len(keys)]
		seqs[key]++
		msg := autoscale.Message{Key: key, Sequence: seqs[key], Payload: fmt.Sprintf("%s-%d", key, seqs[key]), Timestamp: time.Now().UnixNano()}
		cp.Publisher.Publish(ctx, msg)
	}
	waitForConsumers(cp.Pool, 500, 15*time.Second)

	transitions := []struct{ from, to int }{
		{1, 3},
		{3, 5},
		{5, 2},
		{2, 8},
		{8, 1},
	}

	for _, tr := range transitions {
		start := time.Now()
		if err := cp.Repartition(ctx, tr.to); err != nil {
			t.Fatalf("repartition %d->%d: %v", tr.from, tr.to, err)
		}
		duration := time.Since(start)
		t.Logf("  %d -> %d partitions: %v", tr.from, tr.to, duration)

		// Publish after repartition to verify it still works.
		for i := 0; i < 100; i++ {
			key := keys[i%len(keys)]
			seqs[key]++
			msg := autoscale.Message{Key: key, Sequence: seqs[key], Payload: fmt.Sprintf("%s-%d", key, seqs[key]), Timestamp: time.Now().UnixNano()}
			cp.Publisher.Publish(ctx, msg)
		}
	}

	totalExpected := 500 + 5*100
	if !waitForConsumers(cp.Pool, totalExpected, 20*time.Second) {
		t.Fatalf("timeout: expected %d, got %d", totalExpected, cp.Pool.TotalReceived())
	}
	t.Logf("  All %d messages received after %d repartitions", totalExpected, len(transitions))
}

// =========================================================================
// PERF 4: Sustained load with periodic repartition
// =========================================================================
func TestPerfSustainedLoadWithRepartition(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	keys := acctKeys(10)
	var totalPublished atomic.Int64
	var publishErrors atomic.Int64

	// Publisher goroutine: constant stream at ~500 msg/s.
	done := make(chan struct{})
	var pubWg sync.WaitGroup
	pubWg.Add(1)

	seqs := make([]atomic.Int64, len(keys))

	go func() {
		defer pubWg.Done()
		ticker := time.NewTicker(2 * time.Millisecond) // ~500/s
		defer ticker.Stop()
		idx := 0
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				key := keys[idx%len(keys)]
				seq := seqs[idx%len(keys)].Add(1)
				msg := autoscale.Message{
					Key:       key,
					Sequence:  int(seq),
					Payload:   fmt.Sprintf("%s-%d", key, seq),
					Timestamp: time.Now().UnixNano(),
				}
				if err := cp.Publisher.Publish(ctx, msg); err != nil {
					publishErrors.Add(1)
				} else {
					totalPublished.Add(1)
				}
				idx++
			}
		}
	}()

	// Repartition every 500ms while messages flow.
	scales := []int{3, 5, 2, 4, 1, 3}
	start := time.Now()
	for _, s := range scales {
		time.Sleep(500 * time.Millisecond)
		rStart := time.Now()
		if err := cp.Repartition(ctx, s); err != nil {
			t.Errorf("repartition to %d: %v", s, err)
		}
		t.Logf("  repartition to %d: %v (published so far: %d)", s, time.Since(rStart), totalPublished.Load())
	}

	// Let it run a bit more after final repartition.
	time.Sleep(500 * time.Millisecond)
	close(done)
	pubWg.Wait()
	totalDuration := time.Since(start)

	total := int(totalPublished.Load())
	t.Logf("  Duration:        %.2fs", totalDuration.Seconds())
	t.Logf("  Published:       %d (%.0f msg/s)", total, float64(total)/totalDuration.Seconds())
	t.Logf("  Publish errors:  %d", publishErrors.Load())

	if !waitForConsumers(cp.Pool, total, 30*time.Second) {
		t.Logf("  WARNING: timeout waiting for consumers: got %d/%d", cp.Pool.TotalReceived(), total)
	}

	received := cp.Pool.TotalReceived()
	t.Logf("  Received:        %d", received)
	t.Logf("  Repartitions:    %d", len(scales))

	if received < int64(total) {
		t.Errorf("message loss: published %d, received %d", total, received)
	}
}

// =========================================================================
// PERF 5: Large burst — publish many messages at once, measure drain time
// =========================================================================
func TestPerfLargeBurst(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	if err := cp.Repartition(ctx, 4); err != nil {
		t.Fatalf("repartition: %v", err)
	}

	keys := acctKeys(20)
	burstSize := 10000
	seqs := make(map[string]int)

	start := time.Now()
	for i := 0; i < burstSize; i++ {
		key := keys[i%len(keys)]
		seqs[key]++
		msg := autoscale.Message{
			Key:       key,
			Sequence:  seqs[key],
			Payload:   fmt.Sprintf("%s-%d", key, seqs[key]),
			Timestamp: time.Now().UnixNano(),
		}
		if err := cp.Publisher.Publish(ctx, msg); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	pubDuration := time.Since(start)
	t.Logf("  Burst size:     %d messages", burstSize)
	t.Logf("  Publish:        %.0f msg/s (%.2fs)", float64(burstSize)/pubDuration.Seconds(), pubDuration.Seconds())

	if !waitForConsumers(cp.Pool, burstSize, 60*time.Second) {
		t.Fatalf("timeout: expected %d, got %d",
			burstSize, cp.Pool.TotalReceived())
	}
	drainDuration := time.Since(start)
	t.Logf("  Full drain:     %.0f msg/s (%.2fs)", float64(burstSize)/drainDuration.Seconds(), drainDuration.Seconds())
	t.Logf("  Violations:     %d", cp.Pool.TotalViolations())

	msgs := collectAllMessages(cp.Pool)
	dupes, violations := checkOrdering(t, msgs)
	missing := checkCompleteness(t, msgs, seqs)
	t.Logf("  Completeness:   %d/%d msgs, %d dupes, %d violations, %d missing",
		len(msgs), burstSize, dupes, violations, missing)
}

// =========================================================================
// COMPARISON: Raw JetStream vs Direct-Publish Partitioned vs Decoupled Pipeline
// =========================================================================
func TestPerfCompareAll(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	msgCount := 3000

	// ----- Baseline: Raw JetStream (single stream, direct publish + consume) -----
	t.Run("raw_jetstream", func(t *testing.T) {
		rawStream := "BENCH_RAW"
		rawSubject := "bench.raw"
		_ = js.DeleteStream(ctx, rawStream)

		_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:       rawStream,
			Subjects:   []string{rawSubject},
			Storage:    jetstream.FileStorage,
			Duplicates: 100 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("create stream: %v", err)
		}
		defer js.DeleteStream(ctx, rawStream)

		// Start consumer.
		var received atomic.Int64
		cons, err := js.CreateOrUpdateConsumer(ctx, rawStream, jetstream.ConsumerConfig{
			Name:          "bench-raw-consumer",
			Durable:       "bench-raw-consumer",
			FilterSubject: rawSubject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		})
		if err != nil {
			t.Fatalf("create consumer: %v", err)
		}
		cc, err := cons.Consume(func(msg jetstream.Msg) {
			received.Add(1)
			msg.Ack()
		})
		if err != nil {
			t.Fatalf("consume: %v", err)
		}
		defer cc.Stop()

		// Measure per-message publish latency.
		var totalPubLatency time.Duration
		data := []byte(`{"key":"acct-0","sequence":1,"payload":"test","partition":0,"ts":0}`)
		pubStart := time.Now()
		for i := 0; i < msgCount; i++ {
			t0 := time.Now()
			_, err := js.Publish(ctx, rawSubject, data)
			totalPubLatency += time.Since(t0)
			if err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
		pubDuration := time.Since(pubStart)

		// Wait for consumption.
		deadline := time.After(30 * time.Second)
		for received.Load() < int64(msgCount) {
			select {
			case <-deadline:
				t.Fatalf("timeout: got %d/%d", received.Load(), msgCount)
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
		e2eDuration := time.Since(pubStart)

		avgPubLatency := totalPubLatency / time.Duration(msgCount)

		t.Logf("  === RAW JETSTREAM (1 hop) ===")
		t.Logf("  Messages:         %d", msgCount)
		t.Logf("  Avg pub latency:  %v", avgPubLatency)
		t.Logf("  Publish rate:     %.0f msg/s (%.2fs)", float64(msgCount)/pubDuration.Seconds(), pubDuration.Seconds())
		t.Logf("  End-to-end rate:  %.0f msg/s (%.2fs)", float64(msgCount)/e2eDuration.Seconds(), e2eDuration.Seconds())
	})

	// ----- Direct-Publish Partitioned (1 hop, partitioned — new architecture) -----
	t.Run("direct_publish_partitioned", func(t *testing.T) {
		directStream := "BENCH_DIRECT"
		directPrefix := "bench.direct"
		_ = js.DeleteStream(ctx, directStream)

		numPartitions := 3
		_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:       directStream,
			Subjects:   []string{directPrefix + ".>"},
			Storage:    jetstream.FileStorage,
			Duplicates: 100 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("create stream: %v", err)
		}
		defer js.DeleteStream(ctx, directStream)

		// Start one consumer per partition (like the real architecture).
		var received atomic.Int64
		var ccs []jetstream.ConsumeContext
		for p := 0; p < numPartitions; p++ {
			subject := fmt.Sprintf("%s.%02d", directPrefix, p)
			consName := fmt.Sprintf("bench-direct-p%d", p)
			cons, err := js.CreateOrUpdateConsumer(ctx, directStream, jetstream.ConsumerConfig{
				Name:          consName,
				Durable:       consName,
				FilterSubject: subject,
				AckPolicy:     jetstream.AckExplicitPolicy,
				DeliverPolicy: jetstream.DeliverAllPolicy,
			})
			if err != nil {
				t.Fatalf("create consumer p%d: %v", p, err)
			}
			cc, err := cons.Consume(func(msg jetstream.Msg) {
				received.Add(1)
				msg.Ack()
			})
			if err != nil {
				t.Fatalf("consume p%d: %v", p, err)
			}
			ccs = append(ccs, cc)
		}
		defer func() {
			for _, cc := range ccs {
				cc.Stop()
			}
		}()

		// Publish directly to partition subjects (like the new producer does).
		var totalPubLatency time.Duration
		pubStart := time.Now()
		for i := 0; i < msgCount; i++ {
			key := fmt.Sprintf("acct-%d", i%10)
			partition := autoscale.PartitionForKey(key, numPartitions)
			subject := fmt.Sprintf("%s.%02d", directPrefix, partition)

			msg := autoscale.Message{
				Key:       key,
				Sequence:  i + 1,
				Partition: partition,
				Payload:   "test",
				Timestamp: time.Now().UnixNano(),
			}
			data, _ := json.Marshal(msg)

			t0 := time.Now()
			_, err := js.Publish(ctx, subject, data)
			totalPubLatency += time.Since(t0)
			if err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
		pubDuration := time.Since(pubStart)

		deadline := time.After(30 * time.Second)
		for received.Load() < int64(msgCount) {
			select {
			case <-deadline:
				t.Fatalf("timeout: got %d/%d", received.Load(), msgCount)
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
		e2eDuration := time.Since(pubStart)

		avgPubLatency := totalPubLatency / time.Duration(msgCount)

		t.Logf("  === DIRECT-PUBLISH PARTITIONED (1 hop, %d partitions) ===", numPartitions)
		t.Logf("  Messages:         %d", msgCount)
		t.Logf("  Avg pub latency:  %v", avgPubLatency)
		t.Logf("  Publish rate:     %.0f msg/s (%.2fs)", float64(msgCount)/pubDuration.Seconds(), pubDuration.Seconds())
		t.Logf("  End-to-end rate:  %.0f msg/s (%.2fs)", float64(msgCount)/e2eDuration.Seconds(), e2eDuration.Seconds())
	})

	// ----- Per-message latency comparison (publish → consumed) -----
	t.Run("e2e_latency", func(t *testing.T) {
		sampleCount := 200

		// Helper to measure publish-one-wait-one latency.
		measureLatency := func(name string, publishAndWait func(i int) time.Duration) []time.Duration {
			latencies := make([]time.Duration, 0, sampleCount)
			for i := 0; i < sampleCount; i++ {
				lat := publishAndWait(i)
				latencies = append(latencies, lat)
			}
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			return latencies
		}

		// --- Raw JetStream ---
		rawStream := "BENCH_LAT_RAW"
		rawSubject := "bench.lat.raw"
		_ = js.DeleteStream(ctx, rawStream)
		js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: rawStream, Subjects: []string{rawSubject},
			Storage: jetstream.FileStorage, Duplicates: 100 * time.Millisecond,
		})
		defer js.DeleteStream(ctx, rawStream)

		var rawReceived atomic.Int64
		rawCons, _ := js.CreateOrUpdateConsumer(ctx, rawStream, jetstream.ConsumerConfig{
			Name: "bench-lat-raw", Durable: "bench-lat-raw",
			FilterSubject: rawSubject, AckPolicy: jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		})
		rawCC, _ := rawCons.Consume(func(msg jetstream.Msg) {
			rawReceived.Add(1)
			msg.Ack()
		})
		defer rawCC.Stop()

		rawLatencies := measureLatency("raw", func(i int) time.Duration {
			m := autoscale.Message{Key: "acct-0", Sequence: i + 1, Payload: "lat", Timestamp: time.Now().UnixNano()}
			data, _ := json.Marshal(m)
			ts := time.Now()
			js.Publish(ctx, rawSubject, data)
			target := int64(i + 1)
			for rawReceived.Load() < target {
				time.Sleep(50 * time.Microsecond)
			}
			return time.Since(ts)
		})

		// --- Direct-Publish Partitioned ---
		directStream := "BENCH_LAT_DIRECT"
		directPrefix := "bench.lat.direct"
		_ = js.DeleteStream(ctx, directStream)
		js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: directStream, Subjects: []string{directPrefix + ".>"},
			Storage: jetstream.FileStorage, Duplicates: 100 * time.Millisecond,
		})
		defer js.DeleteStream(ctx, directStream)

		var directReceived atomic.Int64
		directCons, _ := js.CreateOrUpdateConsumer(ctx, directStream, jetstream.ConsumerConfig{
			Name: "bench-lat-direct", Durable: "bench-lat-direct",
			FilterSubject: directPrefix + ".00", AckPolicy: jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		})
		directCC, _ := directCons.Consume(func(msg jetstream.Msg) {
			directReceived.Add(1)
			msg.Ack()
		})
		defer directCC.Stop()

		directLatencies := measureLatency("direct", func(i int) time.Duration {
			m := autoscale.Message{Key: "acct-0", Sequence: i + 1, Partition: 0, Payload: "lat", Timestamp: time.Now().UnixNano()}
			data, _ := json.Marshal(m)
			ts := time.Now()
			js.Publish(ctx, directPrefix+".00", data)
			target := int64(i + 1)
			for directReceived.Load() < target {
				time.Sleep(50 * time.Microsecond)
			}
			return time.Since(ts)
		})

		// Print comparison table.
		p := func(lats []time.Duration, pct int) time.Duration {
			idx := len(lats) * pct / 100
			if idx >= len(lats) {
				idx = len(lats) - 1
			}
			return lats[idx]
		}

		t.Logf("")
		t.Logf("  ╔═══════════════════════════════════════════════════════════════╗")
		t.Logf("  ║           PER-MESSAGE E2E LATENCY COMPARISON                 ║")
		t.Logf("  ╠═══════════════════════════════════════════════════════════════╣")
		t.Logf("  ║  %-30s %10s %10s %10s ║", "Approach", "P50", "P95", "P99")
		t.Logf("  ╠═══════════════════════════════════════════════════════════════╣")
		t.Logf("  ║  %-30s %10v %10v %10v ║", "Raw JetStream (1 hop)", p(rawLatencies, 50), p(rawLatencies, 95), p(rawLatencies, 99))
		t.Logf("  ║  %-30s %10v %10v %10v ║", "Direct-Publish (1 hop, part.)", p(directLatencies, 50), p(directLatencies, 95), p(directLatencies, 99))
		t.Logf("  ╠═══════════════════════════════════════════════════════════════╣")
		t.Logf("  ║  Direct vs Raw overhead:   P50=%.2fx  P99=%.2fx               ║",
			float64(p(directLatencies, 50))/float64(p(rawLatencies, 50)),
			float64(p(directLatencies, 99))/float64(p(rawLatencies, 99)))
		t.Logf("  ╚═══════════════════════════════════════════════════════════════╝")
	})
}

// =========================================================================
// Go benchmarks for microbenchmarking publish path
// =========================================================================
func BenchmarkPublish(b *testing.B) {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		b.Skipf("NATS not available: %v", err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx := context.Background()

	cfg := autoscale.DefaultConfig()
	_ = js.DeleteStream(ctx, cfg.BufferStreamName)
	_ = js.DeleteStream(ctx, cfg.StreamName)
	_ = js.DeleteKeyValue(ctx, cfg.KVBucket)
	cfg.StreamConfig.Duplicates = 100 * time.Millisecond

	cp, err := autoscale.NewControlPlaneWithConfig(ctx, js, nc, cfg)
	if err != nil {
		b.Fatalf("control plane: %v", err)
	}
	if err := cp.Start(ctx); err != nil {
		b.Fatalf("start: %v", err)
	}
	defer cp.DestroyAll(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := autoscale.Message{
			Key:       fmt.Sprintf("acct-%d", i%10),
			Sequence:  i + 1,
			Payload:   "bench",
			Timestamp: time.Now().UnixNano(),
		}
		if err := cp.Publisher.Publish(ctx, msg); err != nil {
			b.Fatalf("publish: %v", err)
		}

	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}

func BenchmarkDirectPartitionedPublish(b *testing.B) {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		b.Skipf("NATS not available: %v", err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx := context.Background()

	streamName := "BENCH_DIRECT_B"
	prefix := "bench.directb"
	_ = js.DeleteStream(ctx, streamName)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       streamName,
		Subjects:   []string{prefix + ".>"},
		Storage:    jetstream.FileStorage,
		Duplicates: 100 * time.Millisecond,
	})
	if err != nil {
		b.Fatalf("create stream: %v", err)
	}
	defer js.DeleteStream(ctx, streamName)

	numPartitions := 3

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("acct-%d", i%10)
		partition := autoscale.PartitionForKey(key, numPartitions)
		subject := fmt.Sprintf("%s.%02d", prefix, partition)
		msg := autoscale.Message{
			Key: key, Sequence: i + 1, Partition: partition,
			Payload: "bench", Timestamp: time.Now().UnixNano(),
		}
		data, _ := json.Marshal(msg)
		if _, err := js.Publish(ctx, subject, data); err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}

func BenchmarkRawJetStreamPublish(b *testing.B) {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		b.Skipf("NATS not available: %v", err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx := context.Background()

	streamName := "BENCH_RAW_B"
	subject := "bench.rawb"
	_ = js.DeleteStream(ctx, streamName)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       streamName,
		Subjects:   []string{subject},
		Storage:    jetstream.FileStorage,
		Duplicates: 100 * time.Millisecond,
	})
	if err != nil {
		b.Fatalf("create stream: %v", err)
	}
	defer js.DeleteStream(ctx, streamName)

	data := []byte(`{"key":"acct-0","sequence":1,"payload":"bench","partition":0,"ts":0}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := js.Publish(ctx, subject, data); err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}
