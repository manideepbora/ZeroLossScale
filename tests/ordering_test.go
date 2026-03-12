package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"nats-poc/autoscale"
)

// =============================================================================
// Ordering violation tests — prove that the current buffer-drain-replay protocol
// delivers messages out of order when publishing continues during repartition.
//
// The root cause: step 6 (direct mode) resumes BEFORE step 7 (replay), so
// new direct messages arrive at partition subjects before buffered messages
// are replayed. For accounts that moved partitions, the new consumer sees
// a higher application sequence (direct) before a lower one (replayed).
//
// These tests are expected to FAIL (detect violations) with the current v1
// protocol and PASS (zero violations) after the v2 ordered protocol.
// =============================================================================

// TestOrderingViolationDuringScaleUp publishes continuously while scaling 1→3.
// Checks the per-consumer violation counter which detects out-of-order delivery.
func TestOrderingViolationDuringScaleUp(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	keys := acctKeys(10) // 10 accounts → high chance some move partitions

	var mu sync.Mutex
	seqs := make(map[string]int)
	for _, k := range keys {
		seqs[k] = 0
	}
	var totalPublished atomic.Int64

	// Continuously publish at high rate.
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
					msg := autoscale.Message{
						Key:       key,
						Sequence:  seqs[key],
						Payload:   fmt.Sprintf("%s-seq%d", key, seqs[key]),
						Timestamp: time.Now().UnixNano(),
					}
					if err := cp.Publisher.Publish(ctx, msg); err == nil {
						totalPublished.Add(1)
					}
				}
				mu.Unlock()
				time.Sleep(2 * time.Millisecond) // ~500 msgs/s per key
			}
		}
	}()

	// Let some messages flow with 1 partition.
	time.Sleep(200 * time.Millisecond)
	t.Logf("pre-scale: published %d messages", totalPublished.Load())

	// Scale up while publishing — this is where violations occur.
	if err := cp.Repartition(ctx, 3); err != nil {
		close(done)
		pubWg.Wait()
		t.Fatalf("scale up: %v", err)
	}
	t.Log("repartition 1→3 completed")

	// Continue publishing after scale to let consumers catch up.
	time.Sleep(500 * time.Millisecond)
	close(done)
	pubWg.Wait()

	mu.Lock()
	total := 0
	for _, v := range seqs {
		total += v
	}
	mu.Unlock()
	t.Logf("total published: %d", total)

	// Wait for consumers to process all messages.
	if !waitForConsumers(cp.Pool, total, 30*time.Second) {
		t.Logf("WARNING: received %d/%d (proceeding with what we have)", cp.Pool.TotalReceived(), total)
	}

	violations := cp.Pool.TotalViolations()
	gaps := cp.Pool.TotalGaps()
	missing := cp.Pool.TotalMissing()

	t.Logf("received=%d violations=%d gaps=%d missing=%d",
		cp.Pool.TotalReceived(), violations, gaps, missing)

	// Log which accounts moved partitions (for debugging).
	for _, key := range keys {
		oldP := autoscale.PartitionForKey(key, 1)
		newP := autoscale.PartitionForKey(key, 3)
		moved := ""
		if oldP != newP {
			moved = " ← MOVED"
		}
		t.Logf("  %s: P%d → P%d%s", key, oldP, newP, moved)
	}

	if violations > 0 {
		t.Errorf("OUT-OF-ORDER DETECTED: %d ordering violations (this proves the v1 protocol bug)", violations)
	}
}

// TestOrderingViolationDuringScaleDown publishes continuously while scaling 3→1.
func TestOrderingViolationDuringScaleDown(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	// Start with 3 partitions.
	if err := cp.Repartition(ctx, 3); err != nil {
		t.Fatal(err)
	}
	t.Log("initial scale to 3 partitions")

	keys := acctKeys(10)
	var mu sync.Mutex
	seqs := make(map[string]int)
	for _, k := range keys {
		seqs[k] = 0
	}
	var totalPublished atomic.Int64

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
					msg := autoscale.Message{
						Key:       key,
						Sequence:  seqs[key],
						Payload:   fmt.Sprintf("%s-seq%d", key, seqs[key]),
						Timestamp: time.Now().UnixNano(),
					}
					if err := cp.Publisher.Publish(ctx, msg); err == nil {
						totalPublished.Add(1)
					}
				}
				mu.Unlock()
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	t.Logf("pre-scale: published %d messages", totalPublished.Load())

	// Scale down while publishing.
	if err := cp.Repartition(ctx, 1); err != nil {
		close(done)
		pubWg.Wait()
		t.Fatalf("scale down: %v", err)
	}
	t.Log("repartition 3→1 completed")

	time.Sleep(500 * time.Millisecond)
	close(done)
	pubWg.Wait()

	mu.Lock()
	total := 0
	for _, v := range seqs {
		total += v
	}
	mu.Unlock()
	t.Logf("total published: %d", total)

	if !waitForConsumers(cp.Pool, total, 30*time.Second) {
		t.Logf("WARNING: received %d/%d", cp.Pool.TotalReceived(), total)
	}

	violations := cp.Pool.TotalViolations()
	t.Logf("received=%d violations=%d gaps=%d missing=%d",
		cp.Pool.TotalReceived(), violations, cp.Pool.TotalGaps(), cp.Pool.TotalMissing())

	for _, key := range keys {
		oldP := autoscale.PartitionForKey(key, 3)
		newP := autoscale.PartitionForKey(key, 1)
		moved := ""
		if oldP != newP {
			moved = " ← MOVED"
		}
		t.Logf("  %s: P%d → P%d%s", key, oldP, newP, moved)
	}

	if violations > 0 {
		t.Errorf("OUT-OF-ORDER DETECTED: %d ordering violations (this proves the v1 protocol bug)", violations)
	}
}

// TestOrderingViolationRapidScaling publishes continuously through multiple
// rapid scale events: 1→3→5→2→4. Maximum chance of triggering the bug.
func TestOrderingViolationRapidScaling(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	keys := acctKeys(15) // more accounts = more partition moves
	var mu sync.Mutex
	seqs := make(map[string]int)
	for _, k := range keys {
		seqs[k] = 0
	}
	var totalPublished atomic.Int64

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
					msg := autoscale.Message{
						Key:       key,
						Sequence:  seqs[key],
						Payload:   fmt.Sprintf("%s-seq%d", key, seqs[key]),
						Timestamp: time.Now().UnixNano(),
					}
					if err := cp.Publisher.Publish(ctx, msg); err == nil {
						totalPublished.Add(1)
					}
				}
				mu.Unlock()
				time.Sleep(1 * time.Millisecond) // ~1000 msgs/s per key
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Rapid scale events while publishing continuously.
	scales := []int{3, 5, 2, 4, 1, 3}
	for _, target := range scales {
		t.Logf("scaling to %d (published so far: %d)", target, totalPublished.Load())
		if err := cp.Repartition(ctx, target); err != nil {
			t.Logf("repartition to %d failed: %v (continuing)", target, err)
			continue
		}
		time.Sleep(100 * time.Millisecond) // brief pause between scales
	}

	time.Sleep(500 * time.Millisecond)
	close(done)
	pubWg.Wait()

	mu.Lock()
	total := 0
	for _, v := range seqs {
		total += v
	}
	mu.Unlock()
	t.Logf("total published: %d across %d scale events", total, len(scales))

	if !waitForConsumers(cp.Pool, total, 60*time.Second) {
		t.Logf("WARNING: received %d/%d", cp.Pool.TotalReceived(), total)
	}

	violations := cp.Pool.TotalViolations()
	gaps := cp.Pool.TotalGaps()
	missing := cp.Pool.TotalMissing()
	received := cp.Pool.TotalReceived()

	t.Logf("RESULTS: published=%d received=%d violations=%d gaps=%d missing=%d",
		total, received, violations, gaps, missing)

	// Show per-account ordering from the consumer's perspective.
	msgs := collectAllMessages(cp.Pool)
	perConsumerOrder := make(map[int]map[string][]int) // partition -> key -> seq list
	for _, r := range cp.Pool.Results() {
		if _, ok := perConsumerOrder[r.Partition]; !ok {
			perConsumerOrder[r.Partition] = make(map[string][]int)
		}
		for _, m := range r.Messages {
			perConsumerOrder[r.Partition][m.Key] = append(perConsumerOrder[r.Partition][m.Key], m.Sequence)
		}
	}

	// Find specific out-of-order examples to show in output.
	exampleCount := 0
	for partition, keys := range perConsumerOrder {
		for key, seqList := range keys {
			for i := 1; i < len(seqList); i++ {
				if seqList[i] < seqList[i-1] {
					if exampleCount < 10 { // show up to 10 examples
						t.Logf("  VIOLATION EXAMPLE: P%d %s: ...seq %d then seq %d (out of order)",
							partition, key, seqList[i-1], seqList[i])
					}
					exampleCount++
				}
			}
		}
	}

	_ = msgs // used above indirectly

	if violations > 0 {
		t.Errorf("OUT-OF-ORDER DETECTED: %d violations across %d scale events "+
			"(this proves the v1 protocol delivers replay after direct)", violations, len(scales))
	}
	if gaps > 0 {
		t.Logf("GAPS DETECTED: %d gap events, %d total missing messages", gaps, missing)
	}
}

// TestOrderingViolationPerConsumerDetail provides a detailed trace of per-consumer
// message ordering to visualize exactly where violations occur.
func TestOrderingViolationPerConsumerDetail(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	// Use a small number of keys to make the trace readable.
	keys := []string{"acct-0", "acct-1", "acct-2", "acct-3", "acct-4"}

	// Show partition mapping for old and new counts.
	t.Log("Partition mapping (1→3):")
	for _, key := range keys {
		t.Logf("  %s: P%d → P%d", key, autoscale.PartitionForKey(key, 1), autoscale.PartitionForKey(key, 3))
	}

	seqs := make(map[string]int)
	var mu sync.Mutex
	var totalPublished atomic.Int64

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
					msg := autoscale.Message{
						Key:       key,
						Sequence:  seqs[key],
						Payload:   fmt.Sprintf("%s-seq%d", key, seqs[key]),
						Timestamp: time.Now().UnixNano(),
					}
					if err := cp.Publisher.Publish(ctx, msg); err == nil {
						totalPublished.Add(1)
					}
				}
				mu.Unlock()
				time.Sleep(3 * time.Millisecond)
			}
		}
	}()

	time.Sleep(150 * time.Millisecond)
	preScaleCount := totalPublished.Load()
	t.Logf("pre-scale: %d messages published with 1 partition", preScaleCount)

	if err := cp.Repartition(ctx, 3); err != nil {
		close(done)
		pubWg.Wait()
		t.Fatalf("repartition: %v", err)
	}

	postScaleStart := totalPublished.Load()
	t.Logf("repartition complete: %d messages total at this point", postScaleStart)

	time.Sleep(300 * time.Millisecond)
	close(done)
	pubWg.Wait()

	mu.Lock()
	total := 0
	for _, v := range seqs {
		total += v
	}
	mu.Unlock()

	if !waitForConsumers(cp.Pool, total, 30*time.Second) {
		t.Logf("WARNING: received %d/%d", cp.Pool.TotalReceived(), total)
	}

	// Trace per-consumer message order for each key.
	t.Log("\n=== Per-Consumer Delivery Order ===")
	for _, r := range cp.Pool.Results() {
		if len(r.Messages) == 0 {
			continue
		}
		byKey := make(map[string][]int)
		for _, m := range r.Messages {
			byKey[m.Key] = append(byKey[m.Key], m.Sequence)
		}
		for _, key := range keys {
			seqList := byKey[key]
			if len(seqList) == 0 {
				continue
			}
			// Find violations in this sequence.
			violationMarkers := ""
			for i := 1; i < len(seqList); i++ {
				if seqList[i] < seqList[i-1] {
					violationMarkers += fmt.Sprintf(" [OUT-OF-ORDER at idx %d: %d→%d]", i, seqList[i-1], seqList[i])
				}
			}
			if violationMarkers != "" {
				t.Logf("  P%d %s: %v%s", r.Partition, key, summarizeSeqs(seqList), violationMarkers)
			}
		}
	}

	violations := cp.Pool.TotalViolations()
	t.Logf("\nSUMMARY: published=%d received=%d violations=%d gaps=%d missing=%d",
		total, cp.Pool.TotalReceived(), violations, cp.Pool.TotalGaps(), cp.Pool.TotalMissing())

	if violations > 0 {
		t.Errorf("OUT-OF-ORDER DETECTED: %d violations — v1 protocol delivers direct messages before replayed buffer messages", violations)
	}
}

// summarizeSeqs compacts a sequence list for readable output.
// e.g. [1,2,3,4,50,51,52,10,11,12] → "1-4, 50-52, 10-12"
func summarizeSeqs(seqs []int) string {
	if len(seqs) == 0 {
		return "[]"
	}
	var parts []string
	start := seqs[0]
	prev := seqs[0]
	for i := 1; i < len(seqs); i++ {
		if seqs[i] == prev+1 {
			prev = seqs[i]
		} else {
			if start == prev {
				parts = append(parts, fmt.Sprintf("%d", start))
			} else {
				parts = append(parts, fmt.Sprintf("%d-%d", start, prev))
			}
			start = seqs[i]
			prev = seqs[i]
		}
	}
	if start == prev {
		parts = append(parts, fmt.Sprintf("%d", start))
	} else {
		parts = append(parts, fmt.Sprintf("%d-%d", start, prev))
	}
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += ", "
		}
		result += p
	}
	return result
}
