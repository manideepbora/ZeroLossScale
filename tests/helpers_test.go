package tests

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"nats-poc/autoscale"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func connectJS(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("connect: %v (is NATS running?)", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		t.Fatalf("jetstream: %v", err)
	}
	return nc, js
}

func setupControlPlane(t *testing.T, ctx context.Context, js jetstream.JetStream) *autoscale.ControlPlane {
	t.Helper()
	cfg := autoscale.DefaultConfig()

	_ = js.DeleteStream(ctx, cfg.BufferStreamName)
	_ = js.DeleteStream(ctx, cfg.StreamName)
	_ = js.DeleteStream(ctx, cfg.DLQStreamName)
	_ = js.DeleteKeyValue(ctx, cfg.KVBucket)

	cfg.StreamConfig.Duplicates = 100 * time.Millisecond

	cp, err := autoscale.NewControlPlaneWithConfig(ctx, js, cfg)
	if err != nil {
		t.Fatalf("new control plane: %v", err)
	}

	if err := cp.Start(ctx); err != nil {
		t.Fatalf("start control plane: %v", err)
	}

	return cp
}

func acctKeys(n int) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("acct-%d", i)
	}
	return keys
}

func publishN(t *testing.T, ctx context.Context, pub *autoscale.DirectPublisher, keys []string, perKey int) int {
	t.Helper()
	total := 0
	seqs := make(map[string]int)
	for seq := 1; seq <= perKey; seq++ {
		for _, key := range keys {
			seqs[key]++
			msg := autoscale.Message{
				Key:       key,
				Sequence:  seqs[key],
				Payload:   fmt.Sprintf("%s-seq%d", key, seqs[key]),
				Timestamp: time.Now().UnixNano(),
			}
			if err := pub.Publish(ctx, msg); err != nil {
				t.Fatalf("publish %s seq %d: %v", key, seq, err)
			}
			total++
		}
	}
	return total
}

func waitForConsumers(pool *autoscale.ConsumerPool, expected int, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return false
		default:
			if pool.TotalReceived() >= int64(expected) {
				return true
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func collectAllMessages(pool *autoscale.ConsumerPool) []autoscale.Message {
	var all []autoscale.Message
	for _, r := range pool.Results() {
		all = append(all, r.Messages...)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].Timestamp < all[j].Timestamp
	})
	return all
}

func checkOrdering(t *testing.T, msgs []autoscale.Message) (dupes int, violations int) {
	t.Helper()
	seen := make(map[string]bool)
	lastSeqByKey := make(map[string]int)

	for _, m := range msgs {
		k := fmt.Sprintf("%s-%d", m.Key, m.Sequence)
		if seen[k] {
			dupes++
			t.Errorf("DUPLICATE: %s", k)
		}
		seen[k] = true

		if last, ok := lastSeqByKey[m.Key]; ok && m.Sequence <= last {
			violations++
			t.Errorf("[%s] out of order: seq %d after %d", m.Key, m.Sequence, last)
		}
		lastSeqByKey[m.Key] = m.Sequence
	}
	return
}

func checkCompleteness(t *testing.T, msgs []autoscale.Message, expectedPerKey map[string]int) int {
	t.Helper()
	byKey := make(map[string][]int)
	for _, m := range msgs {
		byKey[m.Key] = append(byKey[m.Key], m.Sequence)
	}
	missing := 0
	for key, expected := range expectedPerKey {
		seqList := byKey[key]
		sort.Ints(seqList)
		if len(seqList) != expected {
			t.Errorf("[%s] expected %d messages, got %d", key, expected, len(seqList))
			missing += expected - len(seqList)
			continue
		}
		for i, s := range seqList {
			if s != i+1 {
				t.Errorf("[%s] gap at index %d: expected seq %d, got %d", key, i, i+1, s)
				break
			}
		}
	}
	return missing
}
