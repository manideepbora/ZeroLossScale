package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"nats-poc/autoscale"

	"github.com/nats-io/nats.go/jetstream"
)

// =========================================================================
// EDGE 1: ScaleResumable input validation
// =========================================================================
func TestScaleResumable_InvalidCounts(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	tests := []struct {
		name  string
		count int
	}{
		{"zero", 0},
		{"negative", -1},
		{"negative large", -100},
		{"over max", autoscale.MaxPartitions + 1},
		{"way over max", 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cp.Scaler.ScaleResumable(ctx, tt.count)
			if err == nil {
				t.Errorf("ScaleResumable(%d) should return error, got nil", tt.count)
			} else {
				t.Logf("ScaleResumable(%d) correctly rejected: %v", tt.count, err)
			}
		})
	}

	// Verify the partition count was not changed.
	if cp.PM.Count() != 1 {
		t.Errorf("partition count should still be 1, got %d", cp.PM.Count())
	}
}

// =========================================================================
// EDGE 2: ScaleResumable same count is no-op
// =========================================================================
func TestScaleResumable_SameCount(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	// Scale to same count should return nil immediately.
	err := cp.Scaler.ScaleResumable(ctx, 1)
	if err != nil {
		t.Fatalf("ScaleResumable(same count) should be no-op, got %v", err)
	}
	t.Log("PASS: ScaleResumable with same count is no-op")
}

// =========================================================================
// EDGE 3: ScaleResumable at MaxPartitions boundary
// =========================================================================
func TestScaleResumable_MaxPartitions(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	// Scale to exactly MaxPartitions should succeed.
	err := cp.Scaler.ScaleResumable(ctx, autoscale.MaxPartitions)
	if err != nil {
		t.Fatalf("ScaleResumable(%d) should succeed, got %v", autoscale.MaxPartitions, err)
	}
	if cp.PM.Count() != autoscale.MaxPartitions {
		t.Fatalf("expected %d partitions, got %d", autoscale.MaxPartitions, cp.PM.Count())
	}

	// Publish and consume to verify it works at max scale.
	keys := acctKeys(5)
	total := publishN(t, ctx, cp.Publisher, keys, 10)
	if !waitForConsumers(cp.Pool, total, 15*time.Second) {
		t.Fatalf("timeout at max partitions: got %d/%d", cp.Pool.TotalReceived(), total)
	}
	t.Logf("PASS: scaled to MaxPartitions=%d and processed %d messages", autoscale.MaxPartitions, total)
}

// =========================================================================
// EDGE 4: DLQ receives poison messages
// =========================================================================
func TestDLQ_PoisonMessage(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	// Publish a malformed message directly to a partition subject (bypass publisher).
	subject := autoscale.SubjectForPartition(cp.Prefix, 0)
	_, err := js.Publish(ctx, subject, []byte("not-valid-json"))
	if err != nil {
		t.Fatalf("publish malformed: %v", err)
	}

	// Wait for the consumer to process it (it should unmarshal-fail and DLQ it).
	time.Sleep(2 * time.Second)

	// Check the DLQ stream for the poison message.
	dlqStream, err := js.Stream(ctx, cp.Cfg.DLQStreamName)
	if err != nil {
		t.Fatalf("get DLQ stream: %v", err)
	}
	info, err := dlqStream.Info(ctx)
	if err != nil {
		t.Fatalf("DLQ stream info: %v", err)
	}
	if info.State.Msgs == 0 {
		t.Fatal("expected at least 1 message in DLQ, got 0")
	}

	// Read the DLQ message and verify headers.
	cons, err := js.CreateOrUpdateConsumer(ctx, cp.Cfg.DLQStreamName, jetstream.ConsumerConfig{
		Name:          "dlq-test-reader",
		FilterSubject: cp.Cfg.DLQSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("create DLQ consumer: %v", err)
	}
	defer js.DeleteConsumer(ctx, cp.Cfg.DLQStreamName, "dlq-test-reader")

	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("fetch DLQ: %v", err)
	}
	for msg := range msgs.Messages() {
		reason := msg.Headers().Get("X-DLQ-Reason")
		if reason != "unmarshal_failure" {
			t.Errorf("expected DLQ reason 'unmarshal_failure', got %q", reason)
		}
		if string(msg.Data()) != "not-valid-json" {
			t.Errorf("expected original data in DLQ, got %q", string(msg.Data()))
		}
		msg.Ack()
		t.Logf("PASS: DLQ received poison message with reason=%s", reason)
	}
}

// =========================================================================
// EDGE 5: DLQ.Send with nil publisher is safe
// =========================================================================
func TestDLQ_NilPublisherSafe(t *testing.T) {
	var dlq *autoscale.DLQPublisher // nil
	// Should not panic.
	dlq.Send(context.Background(), []byte("data"), "subject", "reason", fmt.Errorf("err"))
	t.Log("PASS: DLQ.Send on nil publisher does not panic")
}

// =========================================================================
// EDGE 6: ResumeRepartition with no prior state is no-op
// =========================================================================
func TestResumeRepartition_NoState(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	err := cp.Scaler.ResumeRepartition(ctx)
	if err != nil {
		t.Fatalf("ResumeRepartition with no state should be no-op, got %v", err)
	}
	if cp.PM.Count() != 1 {
		t.Fatalf("expected 1 partition (unchanged), got %d", cp.PM.Count())
	}
	t.Log("PASS: ResumeRepartition with no prior state is no-op")
}

// =========================================================================
// EDGE 7: ResumeRepartition with idle state is no-op
// =========================================================================
func TestResumeRepartition_IdleState(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	// Write an idle state — should be ignored.
	kv, _ := js.KeyValue(ctx, cp.Cfg.KVBucket)
	idleState := &autoscale.RepartitionState{
		Status: autoscale.RepartitionIdle,
	}
	if _, err := autoscale.SaveRepartitionState(ctx, kv, idleState, 0); err != nil {
		t.Fatalf("save idle state: %v", err)
	}

	err := cp.Scaler.ResumeRepartition(ctx)
	if err != nil {
		t.Fatalf("ResumeRepartition with idle state should be no-op, got %v", err)
	}
	t.Log("PASS: ResumeRepartition with idle state is no-op")
}

// =========================================================================
// EDGE 8: Publish before partition count is initialized
// =========================================================================
func TestPublish_BeforeInit(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	// Create a bare DirectPublisher that will fail because KV doesn't exist.
	_, err := autoscale.NewDirectPublisher(ctx, js, "NONEXISTENT_STREAM")
	if err == nil {
		t.Fatal("expected error creating publisher for nonexistent KV, got nil")
	}
	t.Logf("PASS: NewDirectPublisher correctly fails for missing KV: %v", err)
}

// =========================================================================
// EDGE 9: Buffer replay with empty buffer
// =========================================================================
func TestReplayBuffer_Empty(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	// Replay with nothing in the buffer.
	replayed, err := cp.Scaler.ReplayBuffer(ctx, 1)
	if err != nil {
		t.Fatalf("replay empty buffer: %v", err)
	}
	if replayed != 0 {
		t.Errorf("expected 0 replayed from empty buffer, got %d", replayed)
	}
	t.Log("PASS: ReplayBuffer with empty buffer returns 0")
}

// =========================================================================
// EDGE 10: Buffer replay republishes correctly
// =========================================================================
func TestReplayBuffer_WithMessages(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	// Put messages directly in the buffer stream.
	for i := 0; i < 5; i++ {
		msg := autoscale.Message{
			Key:       fmt.Sprintf("acct-%d", i%3),
			Sequence:  i + 1,
			Partition: 0,
			Payload:   fmt.Sprintf("buffered-%d", i),
			Timestamp: time.Now().UnixNano(),
		}
		data, _ := json.Marshal(msg)
		if _, err := js.Publish(ctx, cp.Cfg.BufferSubject, data); err != nil {
			t.Fatalf("publish to buffer: %v", err)
		}
	}

	// Replay to 1 partition.
	replayed, err := cp.Scaler.ReplayBuffer(ctx, 1)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed != 5 {
		t.Errorf("expected 5 replayed, got %d", replayed)
	}

	// Verify messages arrived on the partition stream.
	if !waitForConsumers(cp.Pool, 5, 10*time.Second) {
		t.Fatalf("timeout: expected 5 consumed, got %d", cp.Pool.TotalReceived())
	}
	t.Logf("PASS: ReplayBuffer replayed %d messages correctly", replayed)
}

// =========================================================================
// EDGE 11: Scale then verify mode returns to direct
// =========================================================================
func TestScale_ModeReturnsToDirect(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	if cp.PM.Mode() != autoscale.KVModeDirect {
		t.Fatalf("expected initial mode=direct, got %s", cp.PM.Mode())
	}

	if err := cp.Scaler.ScaleResumable(ctx, 3); err != nil {
		t.Fatalf("scale: %v", err)
	}

	if cp.PM.Mode() != autoscale.KVModeDirect {
		t.Fatalf("expected mode=direct after scale, got %s", cp.PM.Mode())
	}

	// Repartition state should be cleared.
	kv, _ := js.KeyValue(ctx, cp.Cfg.KVBucket)
	state, _, err := autoscale.LoadRepartitionState(ctx, kv)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state != nil && state.Status != autoscale.RepartitionIdle {
		t.Fatalf("expected idle state after scale, got %s", state.Status)
	}

	t.Log("PASS: mode returns to direct and state is cleared after scale")
}

// =========================================================================
// EDGE 12: Repartition state CAS conflict
// =========================================================================
func TestRepartitionState_CASConflict(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	kv, _ := js.KeyValue(ctx, cp.Cfg.KVBucket)

	state := &autoscale.RepartitionState{
		Status:   autoscale.RepartitionInProgress,
		OldCount: 1,
		NewCount: 3,
	}
	rev, err := autoscale.SaveRepartitionState(ctx, kv, state, 0)
	if err != nil {
		t.Fatalf("save: %v", err)
	}

	// Use a stale revision — should fail CAS.
	state.Step = 1
	_, err = autoscale.SaveRepartitionState(ctx, kv, state, rev-1)
	if err == nil {
		t.Fatal("expected CAS conflict error, got nil")
	}
	t.Logf("PASS: CAS conflict correctly detected: %v", err)

	// Use correct revision — should succeed.
	_, err = autoscale.SaveRepartitionState(ctx, kv, state, rev)
	if err != nil {
		t.Fatalf("save with correct rev should succeed: %v", err)
	}
}

// =========================================================================
// EDGE 13: ConsumerName format
// =========================================================================
func TestConsumerName(t *testing.T) {
	tests := []struct {
		partition int
		want      string
	}{
		{0, "consumer-p0"},
		{1, "consumer-p1"},
		{19, "consumer-p19"},
	}
	for _, tt := range tests {
		got := autoscale.ConsumerName(tt.partition)
		if got != tt.want {
			t.Errorf("ConsumerName(%d) = %q, want %q", tt.partition, got, tt.want)
		}
	}
}
