package autoscale

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	KVKeyRepartitionState = "repartition_state"

	RepartitionIdle       = "idle"
	RepartitionInProgress = "in_progress"
)

// RepartitionState tracks the progress of a repartition operation.
// Stored in the stream's KV bucket so a new leader can resume.
type RepartitionState struct {
	Status    string `json:"status"`     // "idle" or "in_progress"
	Step      int    `json:"step"`       // last completed step (0 = none, 1-7 = step N done)
	OldCount  int    `json:"old_count"`
	NewCount  int    `json:"new_count"`
	StartedAt int64  `json:"started_at"` // unix nanos
	UpdatedAt int64  `json:"updated_at"` // unix nanos
}

// LoadRepartitionState reads the current state from KV. Returns nil state if not found.
func LoadRepartitionState(ctx context.Context, kv jetstream.KeyValue) (*RepartitionState, uint64, error) {
	entry, err := kv.Get(ctx, KVKeyRepartitionState)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("get repartition state: %w", err)
	}
	var state RepartitionState
	if err := json.Unmarshal(entry.Value(), &state); err != nil {
		return nil, 0, fmt.Errorf("unmarshal repartition state: %w", err)
	}
	return &state, entry.Revision(), nil
}

// SaveRepartitionState writes the state to KV with CAS (compare-and-swap).
// Pass revision=0 for initial create.
func SaveRepartitionState(ctx context.Context, kv jetstream.KeyValue, state *RepartitionState, revision uint64) (uint64, error) {
	state.UpdatedAt = time.Now().UnixNano()
	data, err := json.Marshal(state)
	if err != nil {
		return 0, fmt.Errorf("marshal repartition state: %w", err)
	}

	if revision == 0 {
		rev, err := kv.Put(ctx, KVKeyRepartitionState, data)
		return rev, err
	}
	rev, err := kv.Update(ctx, KVKeyRepartitionState, data, revision)
	if err != nil {
		return 0, fmt.Errorf("CAS update repartition state: %w", err)
	}
	return rev, nil
}

// ClearRepartitionState sets the state to idle.
func ClearRepartitionState(ctx context.Context, kv jetstream.KeyValue) error {
	state := &RepartitionState{
		Status:    RepartitionIdle,
		UpdatedAt: time.Now().UnixNano(),
	}
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal repartition state: %w", err)
	}
	_, err = kv.Put(ctx, KVKeyRepartitionState, data)
	return err
}
