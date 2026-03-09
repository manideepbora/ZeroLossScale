package tests

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"nats-poc/autoscale"
)

func TestRetry_Success(t *testing.T) {
	calls := 0
	err := autoscale.Retry(context.Background(), "test-ok", 3, 10*time.Millisecond, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestRetry_SuccessOnSecondAttempt(t *testing.T) {
	calls := 0
	err := autoscale.Retry(context.Background(), "test-retry", 3, 10*time.Millisecond, func() error {
		calls++
		if calls < 2 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls, got %d", calls)
	}
}

func TestRetry_AllAttemptsFail(t *testing.T) {
	sentinel := errors.New("persistent failure")
	calls := 0
	err := autoscale.Retry(context.Background(), "test-fail", 3, 10*time.Millisecond, func() error {
		calls++
		return sentinel
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected error to wrap sentinel, got %v", err)
	}
	if !strings.Contains(err.Error(), "all 3 attempts failed") {
		t.Errorf("expected 'all 3 attempts failed' in error, got %v", err)
	}
}

func TestRetry_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := autoscale.Retry(ctx, "test-cancel", 5, 100*time.Millisecond, func() error {
		calls++
		if calls == 1 {
			cancel() // Cancel after first failure.
		}
		return errors.New("fail")
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled in error, got %v", err)
	}
	// Should have stopped early (not all 5 attempts).
	if calls >= 5 {
		t.Errorf("expected early termination, got %d calls", calls)
	}
}

func TestRetry_SingleAttempt(t *testing.T) {
	err := autoscale.Retry(context.Background(), "test-single", 1, 10*time.Millisecond, func() error {
		return errors.New("fail")
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "all 1 attempts failed") {
		t.Errorf("expected 'all 1 attempts failed', got %v", err)
	}
}
