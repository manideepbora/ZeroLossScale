package tests

import (
	"context"
	"strconv"
	"testing"
	"time"

	"nats-poc/autoscale"
)

// =========================================================================
// TEST: Leader election — two instances, exactly one wins
// =========================================================================
func TestLeaderElection(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	bucket := "test-leader-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	defer js.DeleteKeyValue(ctx, bucket)

	le1, err := autoscale.NewLeaderElector(ctx, js, bucket, "instance-1", 3*time.Second)
	if err != nil {
		t.Fatalf("leader elector 1: %v", err)
	}
	le2, err := autoscale.NewLeaderElector(ctx, js, bucket, "instance-2", 3*time.Second)
	if err != nil {
		t.Fatalf("leader elector 2: %v", err)
	}

	le1.Start(ctx)
	defer le1.Stop()

	// Give le1 time to acquire.
	time.Sleep(500 * time.Millisecond)

	le2.Start(ctx)
	defer le2.Stop()

	time.Sleep(500 * time.Millisecond)

	if !le1.IsLeader() && !le2.IsLeader() {
		t.Fatal("expected at least one leader")
	}
	if le1.IsLeader() && le2.IsLeader() {
		t.Fatal("expected exactly one leader, both are leaders")
	}

	leaderID := le1.LeaderID(ctx)
	if le1.IsLeader() {
		if leaderID != "instance-1" {
			t.Errorf("expected leader ID instance-1, got %s", leaderID)
		}
		t.Log("instance-1 is leader")
	} else {
		if leaderID != "instance-2" {
			t.Errorf("expected leader ID instance-2, got %s", leaderID)
		}
		t.Log("instance-2 is leader")
	}
}

// =========================================================================
// TEST: Failover — leader stops, follower takes over
// =========================================================================
func TestLeaderFailover(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	bucket := "test-failover-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	defer js.DeleteKeyValue(ctx, bucket)

	elected1 := make(chan struct{}, 1)
	lost1 := make(chan struct{}, 1)
	elected2 := make(chan struct{}, 1)

	le1, _ := autoscale.NewLeaderElector(ctx, js, bucket, "instance-1", 3*time.Second,
		autoscale.WithOnElected(func() { elected1 <- struct{}{} }),
		autoscale.WithOnLost(func() { lost1 <- struct{}{} }),
	)
	le2, _ := autoscale.NewLeaderElector(ctx, js, bucket, "instance-2", 3*time.Second,
		autoscale.WithOnElected(func() { elected2 <- struct{}{} }),
	)

	le1.Start(ctx)

	// Wait for le1 to become leader.
	select {
	case <-elected1:
		t.Log("instance-1 elected")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for instance-1 election")
	}

	le2.Start(ctx)
	defer le2.Stop()

	// le1 stops — should release lock immediately.
	le1.Stop()

	// le2 should take over.
	select {
	case <-elected2:
		t.Log("instance-2 elected after failover")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for instance-2 failover")
	}

	if !le2.IsLeader() {
		t.Fatal("expected instance-2 to be leader after failover")
	}
}

// =========================================================================
// TEST: Desired-state reconciliation — write desired, leader reconciles
// =========================================================================
func TestReconcilerDesiredState(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	// Create reconciler using the in-process ControlPlane's scaler.
	reconciler := autoscale.NewReconciler(js, cp.Scaler, cp.PM, cp.Cfg)
	reconciler.Start(ctx)
	defer reconciler.Stop()

	// Current count is 1. Write desired=3.
	if err := autoscale.SetDesired(ctx, js, cp.Cfg.KVBucket, 3); err != nil {
		t.Fatalf("set desired: %v", err)
	}

	// Wait for reconciler to scale.
	deadline := time.After(20 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timeout: expected 3 partitions, got %d", cp.PM.Count())
		default:
			if cp.PM.Count() == 3 {
				t.Logf("reconciler scaled to 3 partitions")
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// =========================================================================
// TEST: Resumable repartition — ScaleResumable checkpoints, can resume
// =========================================================================
func TestScaleResumable(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	// Scale using resumable method (no prior messages needed — test the protocol).
	if err := cp.Scaler.ScaleResumable(ctx, 3); err != nil {
		t.Fatalf("scale resumable: %v", err)
	}

	if cp.PM.Count() != 3 {
		t.Fatalf("expected 3 partitions, got %d", cp.PM.Count())
	}

	// Verify repartition state is cleared.
	kv, _ := js.KeyValue(ctx, cp.Cfg.KVBucket)
	state, _, err := autoscale.LoadRepartitionState(ctx, kv)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state != nil && state.Status != autoscale.RepartitionIdle {
		t.Fatalf("expected idle state, got %s (step %d)", state.Status, state.Step)
	}

	// Publish after scale and verify ordering with 3 partitions.
	keys := acctKeys(3)
	total := publishN(t, ctx, cp.Publisher, keys, 30)
	if !waitForConsumers(cp.Pool, total, 15*time.Second) {
		t.Fatalf("timeout after scale: got %d/%d", cp.Pool.TotalReceived(), total)
	}

	msgs := collectAllMessages(cp.Pool)
	dupes, violations := checkOrdering(t, msgs)
	if violations > 0 {
		t.Fatalf("ordering violations: %d", violations)
	}
	t.Logf("PASS: resumable scale — %d messages, %d dupes, %d violations", len(msgs), dupes, violations)
}

// =========================================================================
// TEST: ResumeRepartition — simulate crash mid-repartition
// =========================================================================
func TestResumeAfterCrash(t *testing.T) {
	nc, js := connectJS(t)
	defer nc.Close()
	ctx := context.Background()

	cp := setupControlPlane(t, ctx, js)
	defer cp.DestroyAll(ctx)

	// Simulate a crash at step 3 by writing an in-progress state directly.
	kv, _ := js.KeyValue(ctx, cp.Cfg.KVBucket)
	crashState := &autoscale.RepartitionState{
		Status:    autoscale.RepartitionInProgress,
		Step:      1, // Only step 1 (set buffer mode) completed.
		OldCount:  1,
		NewCount:  3,
		StartedAt: time.Now().UnixNano(),
	}
	if _, err := autoscale.SaveRepartitionState(ctx, kv, crashState, 0); err != nil {
		t.Fatalf("save crash state: %v", err)
	}

	// Set mode to buffer (as step 1 would have done).
	cp.PM.SetMode(ctx, autoscale.KVModeBuffer)

	// Now "new leader" resumes.
	if err := cp.Scaler.ResumeRepartition(ctx); err != nil {
		t.Fatalf("resume: %v", err)
	}

	if cp.PM.Count() != 3 {
		t.Fatalf("expected 3 partitions after resume, got %d", cp.PM.Count())
	}

	// Mode should be back to direct.
	if cp.PM.Mode() != autoscale.KVModeDirect {
		t.Fatalf("expected direct mode after resume, got %s", cp.PM.Mode())
	}

	// State should be cleared.
	state, _, _ := autoscale.LoadRepartitionState(ctx, kv)
	if state != nil && state.Status != autoscale.RepartitionIdle {
		t.Fatalf("expected idle after resume, got %s", state.Status)
	}

	t.Log("PASS: resumed repartition from step 1, completed successfully")
}
