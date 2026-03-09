package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"nats-poc/autoscale"
)

// =========================================================================
// TEST 1: Single partition — baseline ordering and completeness
// =========================================================================
func TestSinglePartitionBaseline(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	keys := acctKeys(3)
	perKey := 50
	total := publishN(t, ctx, cp.Publisher, keys, perKey)
	t.Logf("published %d messages (3 accounts x %d)", total, perKey)

	if !waitForConsumers(cp.Pool, total, 15*time.Second) {
		t.Fatalf("timeout: expected %d, got %d", total, cp.Pool.TotalReceived())
	}

	msgs := collectAllMessages(cp.Pool)
	if len(msgs) != total {
		t.Errorf("expected %d messages, got %d", total, len(msgs))
	}

	dupes, violations := checkOrdering(t, msgs)
	t.Logf("PASS: %d/%d messages, %d dupes, %d violations — single partition baseline",
		len(msgs), total, dupes, violations)
}

// =========================================================================
// TEST 2: Scale UP from 1 to 3 partitions
// =========================================================================
func TestScaleUp(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	keys := acctKeys(6)
	perKey := 30

	seqs := make(map[string]int)
	for seq := 1; seq <= perKey; seq++ {
		for _, key := range keys {
			seqs[key]++
			msg := autoscale.Message{Key: key, Sequence: seqs[key], Payload: fmt.Sprintf("%s-%d", key, seqs[key]), Timestamp: time.Now().UnixNano()}
			if err := cp.Publisher.Publish(ctx, msg); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
	}
	phase1 := len(keys) * perKey
	t.Logf("phase 1: published %d messages with 1 partition", phase1)

	waitForConsumers(cp.Pool, phase1, 15*time.Second)

	if err := cp.Repartition(ctx, 3); err != nil {
		t.Fatalf("scale up: %v", err)
	}

	for seq := 1; seq <= perKey; seq++ {
		for _, key := range keys {
			seqs[key]++
			msg := autoscale.Message{Key: key, Sequence: seqs[key], Payload: fmt.Sprintf("%s-%d", key, seqs[key]), Timestamp: time.Now().UnixNano()}
			if err := cp.Publisher.Publish(ctx, msg); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
	}
	totalExpected := phase1 + len(keys)*perKey
	t.Logf("phase 2: published %d more messages with 3 partitions (total: %d)", len(keys)*perKey, totalExpected)

	if !waitForConsumers(cp.Pool, totalExpected, 20*time.Second) {
		t.Fatalf("timeout: expected %d, got %d", totalExpected, cp.Pool.TotalReceived())
	}

	msgs := collectAllMessages(cp.Pool)
	dupes, violations := checkOrdering(t, msgs)
	if len(msgs) != totalExpected {
		t.Errorf("expected %d messages, got %d", totalExpected, len(msgs))
	}

	t.Logf("PASS: scale UP 1->3: %d/%d messages, %d duplicates, %d violations",
		len(msgs), totalExpected, dupes, violations)
}

// =========================================================================
// TEST 3: Scale DOWN from 3 to 1
// =========================================================================
func TestScaleDown(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	keys := acctKeys(4)

	if err := cp.Repartition(ctx, 3); err != nil {
		t.Fatal(err)
	}

	seqs := make(map[string]int)
	for seq := 1; seq <= 40; seq++ {
		for _, key := range keys {
			seqs[key]++
			msg := autoscale.Message{Key: key, Sequence: seqs[key], Payload: fmt.Sprintf("%s-%d", key, seqs[key]), Timestamp: time.Now().UnixNano()}
			if err := cp.Publisher.Publish(ctx, msg); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
	}
	phase1Count := 4 * 40
	t.Logf("phase 1: published %d messages with 3 partitions", phase1Count)

	waitForConsumers(cp.Pool, phase1Count, 15*time.Second)

	if err := cp.Repartition(ctx, 1); err != nil {
		t.Fatalf("scale down: %v", err)
	}

	for seq := 1; seq <= 40; seq++ {
		for _, key := range keys {
			seqs[key]++
			msg := autoscale.Message{Key: key, Sequence: seqs[key], Payload: fmt.Sprintf("%s-%d", key, seqs[key]), Timestamp: time.Now().UnixNano()}
			if err := cp.Publisher.Publish(ctx, msg); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
	}
	totalExpected := phase1Count + 4*40
	t.Logf("phase 2: published %d more (total expected: %d)", 4*40, totalExpected)

	if !waitForConsumers(cp.Pool, totalExpected, 20*time.Second) {
		t.Fatalf("timeout: expected %d, got %d", totalExpected, cp.Pool.TotalReceived())
	}

	msgs := collectAllMessages(cp.Pool)
	dupes, violations := checkOrdering(t, msgs)

	if len(msgs) != totalExpected {
		t.Errorf("expected %d, got %d", totalExpected, len(msgs))
	}

	t.Logf("PASS: scale DOWN 3->1: %d/%d messages, %d dupes, %d violations",
		len(msgs), totalExpected, dupes, violations)
}

// =========================================================================
// TEST 4: Full lifecycle — UP then DOWN then UP
// =========================================================================
func TestFullLifecycle(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	keys := acctKeys(5)
	seqs := make(map[string]int)
	totalPublished := 0

	emit := func(count int) {
		for seq := 0; seq < count; seq++ {
			for _, key := range keys {
				seqs[key]++
				msg := autoscale.Message{Key: key, Sequence: seqs[key], Payload: fmt.Sprintf("%s-%d", key, seqs[key]), Timestamp: time.Now().UnixNano()}
				if err := cp.Publisher.Publish(ctx, msg); err != nil {
					t.Fatalf("publish: %v", err)
				}
				totalPublished++
			}
		}
	}

	emit(20)
	t.Logf("phase 1 (1 partition): published %d total", totalPublished)
	waitForConsumers(cp.Pool, totalPublished, 15*time.Second)

	if err := cp.Repartition(ctx, 3); err != nil {
		t.Fatalf("scale up to 3: %v", err)
	}
	emit(40)
	t.Logf("phase 2 (3 partitions): published %d total", totalPublished)
	waitForConsumers(cp.Pool, totalPublished, 15*time.Second)

	if err := cp.Repartition(ctx, 5); err != nil {
		t.Fatalf("scale up to 5: %v", err)
	}
	emit(40)
	t.Logf("phase 3 (5 partitions): published %d total", totalPublished)
	waitForConsumers(cp.Pool, totalPublished, 15*time.Second)

	if err := cp.Repartition(ctx, 2); err != nil {
		t.Fatalf("scale down to 2: %v", err)
	}
	emit(30)
	t.Logf("phase 4 (2 partitions): published %d total", totalPublished)
	waitForConsumers(cp.Pool, totalPublished, 15*time.Second)

	if err := cp.Repartition(ctx, 4); err != nil {
		t.Fatalf("scale up to 4: %v", err)
	}
	emit(20)
	t.Logf("phase 5 (4 partitions): published %d total", totalPublished)
	waitForConsumers(cp.Pool, totalPublished, 20*time.Second)

	msgs := collectAllMessages(cp.Pool)
	dupes, violations := checkOrdering(t, msgs)
	missing := checkCompleteness(t, msgs, seqs)

	t.Logf("PASS: full lifecycle (1->3->5->2->4): %d published, %d received, %d dupes, %d violations, %d missing",
		totalPublished, len(msgs), dupes, violations, missing)
}

// =========================================================================
// TEST 5: Concurrent publish during scale event
// =========================================================================
func TestConcurrentPublishDuringScale(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	keys := acctKeys(3)

	var mu sync.Mutex
	seqs := make(map[string]int)
	for _, k := range keys {
		seqs[k] = 0
	}

	done := make(chan struct{})
	var pubWg sync.WaitGroup
	pubWg.Add(1)
	go func() {
		defer pubWg.Done()
		for {
			select {
			case <-done:
				return
			default:
				mu.Lock()
				for _, key := range keys {
					seqs[key]++
					msg := autoscale.Message{Key: key, Sequence: seqs[key], Payload: fmt.Sprintf("%s-%d", key, seqs[key]), Timestamp: time.Now().UnixNano()}
					cp.Publisher.Publish(ctx, msg)
				}
				mu.Unlock()
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)
	if err := cp.Repartition(ctx, 3); err != nil {
		t.Fatalf("scale up: %v", err)
	}
	t.Log("scaled up to 3 while publishing")

	time.Sleep(200 * time.Millisecond)
	if err := cp.Repartition(ctx, 1); err != nil {
		t.Fatalf("scale down: %v", err)
	}
	t.Log("scaled down to 1 while publishing")

	time.Sleep(100 * time.Millisecond)
	close(done)
	pubWg.Wait()

	mu.Lock()
	totalPublished := 0
	for _, v := range seqs {
		totalPublished += v
	}
	mu.Unlock()
	t.Logf("total published during concurrent scale events: %d", totalPublished)

	if !waitForConsumers(cp.Pool, totalPublished, 20*time.Second) {
		t.Logf("received %d/%d (some may still be in flight)", cp.Pool.TotalReceived(), totalPublished)
	}

	msgs := collectAllMessages(cp.Pool)
	dupes, _ := checkOrdering(t, msgs)

	if dupes > 0 {
		t.Errorf("duplicates: %d", dupes)
	}
	if len(msgs) != totalPublished {
		t.Errorf("message loss: published %d, received %d", totalPublished, len(msgs))
	}

	t.Logf("PASS: concurrent publish during scale: %d published, %d received, %d dupes",
		totalPublished, len(msgs), dupes)
}

// =========================================================================
// TEST 6: Partition routing determinism
// =========================================================================
func TestPartitionRoutingDeterminism(t *testing.T) {
	tests := []struct {
		key   string
		count int
		want  int
	}{
		{"acct-0", 1, 0},
		{"acct-0", 3, 0},
		{"acct-1", 3, 1},
		{"acct-2", 3, 2},
		{"acct-3", 3, 0},
		{"acct-4", 3, 1},
		{"acct-5", 3, 2},
		{"acct-10", 2, 0},
		{"acct-11", 2, 1},
		{"acct-99", 5, 4},
	}

	for _, tt := range tests {
		got := autoscale.PartitionForKey(tt.key, tt.count)
		if got != tt.want {
			t.Errorf("PartitionForKey(%q, %d) = %d, want %d", tt.key, tt.count, got, tt.want)
		}
		got2 := autoscale.PartitionForKey(tt.key, tt.count)
		if got != got2 {
			t.Errorf("non-deterministic: %q count=%d got %d then %d", tt.key, tt.count, got, got2)
		}
	}
	t.Log("PASS: partition routing is deterministic and uses accountID % count")
}

// =========================================================================
// TEST 7: Per-account consumed sequence tracking
// =========================================================================
func TestConsumedSeqTracking(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	keys := acctKeys(3)
	perKey := 20
	publishN(t, ctx, cp.Publisher, keys, perKey)

	deadline1 := time.After(15 * time.Second)
	for {
		allDone := true
		consumed := cp.Pool.ConsumedSeqs()
		for _, key := range keys {
			cs, ok := consumed[key]
			if !ok || cs.Sequence < perKey {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
		select {
		case <-deadline1:
			t.Fatalf("timeout waiting for consumed seqs phase 1: got %v", cp.Pool.ConsumedSeqs())
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	consumed := cp.Pool.ConsumedSeqs()
	for _, key := range keys {
		cs := consumed[key]
		if cs.Sequence != perKey {
			t.Errorf("%s: expected consumed seq %d, got %d", key, perKey, cs.Sequence)
		}
		t.Logf("%s: consumed seq=%d partition=%d", key, cs.Sequence, cs.Partition)
	}

	if err := cp.Repartition(ctx, 2); err != nil {
		t.Fatal(err)
	}

	seqs := make(map[string]int)
	for _, k := range keys {
		seqs[k] = perKey
	}
	for i := 0; i < 10; i++ {
		for _, key := range keys {
			seqs[key]++
			msg := autoscale.Message{Key: key, Sequence: seqs[key], Payload: fmt.Sprintf("%s-%d", key, seqs[key]), Timestamp: time.Now().UnixNano()}
			cp.Publisher.Publish(ctx, msg)
		}
	}

	deadline := time.After(15 * time.Second)
	for {
		allDone := true
		consumed = cp.Pool.ConsumedSeqs()
		for _, key := range keys {
			cs, ok := consumed[key]
			if !ok || cs.Sequence < seqs[key] {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for consumed seqs: got %v", consumed)
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	consumed = cp.Pool.ConsumedSeqs()
	for _, key := range keys {
		cs := consumed[key]
		expectedPartition := autoscale.PartitionForKey(key, 2)
		if cs.Sequence != seqs[key] {
			t.Errorf("%s: expected consumed seq %d, got %d", key, seqs[key], cs.Sequence)
		}
		if cs.Partition != expectedPartition {
			t.Errorf("%s: expected partition %d, got %d", key, expectedPartition, cs.Partition)
		}
		t.Logf("%s: consumed seq=%d partition=%02d (expected P%02d)", key, cs.Sequence, cs.Partition, expectedPartition)
	}

	t.Log("PASS: per-account consumed sequence tracking works across scale events")
}
