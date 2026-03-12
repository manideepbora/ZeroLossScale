package autoscale

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

// PartitionManager manages partition count and mode in a NATS KV bucket.
// The KV bucket acts as the coordination point between controlplane and producers:
//   - "partition_count" — current number of partitions (producers watch this)
//   - "mode" — "direct" or "buffer" (producers watch this)
type PartitionManager struct {
	kv     jetstream.KeyValue
	bucket string

	mu    sync.RWMutex
	count int
	mode  string // "direct" or "buffer"
}

// NewPartitionManager creates a KV bucket for partition config and seeds initial values.
func NewPartitionManager(ctx context.Context, js jetstream.JetStream, kvBucket string, initial int) (*PartitionManager, error) {
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: kvBucket,
	})
	if err != nil {
		return nil, fmt.Errorf("create KV bucket %s: %w", kvBucket, err)
	}

	pm := &PartitionManager{kv: kv, bucket: kvBucket, count: initial, mode: KVModeDirect}

	// Seed the initial partition count.
	if _, err := kv.Put(ctx, KVKeyPartitionCount, []byte(strconv.Itoa(initial))); err != nil {
		return nil, fmt.Errorf("seed partition count: %w", err)
	}

	return pm, nil
}

// Count returns the current partition count.
func (pm *PartitionManager) Count() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.count
}

// SetCount updates the partition count in KV and local state atomically.
// The lock is held for the entire operation to prevent concurrent callers
// from interleaving KV writes with local state updates.
func (pm *PartitionManager) SetCount(ctx context.Context, n int) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if _, err := pm.kv.Put(ctx, KVKeyPartitionCount, []byte(strconv.Itoa(n))); err != nil {
		return err
	}
	pm.count = n
	return nil
}

// Mode returns the current mode ("direct" or "buffer").
func (pm *PartitionManager) Mode() string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.mode
}

// SetMode updates the mode in KV and local state atomically.
// The lock is held for the entire operation to prevent concurrent callers
// from interleaving KV writes with local state updates.
func (pm *PartitionManager) SetMode(ctx context.Context, mode string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if _, err := pm.kv.Put(ctx, KVKeyMode, []byte(mode)); err != nil {
		return err
	}
	pm.mode = mode
	return nil
}

// Bucket returns the KV bucket name.
func (pm *PartitionManager) Bucket() string {
	return pm.bucket
}

// SubjectForPartition returns the full subject for a given partition index.
// Format: "<prefix>.<2-digit partition>" e.g. "MY_ORDERS.00", "MY_ORDERS.01"
func SubjectForPartition(prefix string, partition int) string {
	return fmt.Sprintf("%s.%02d", prefix, partition)
}

// PartitionForKey deterministically maps a key to a partition.
// Extracts trailing number from key (e.g. "acct-42" -> 42) and uses modulo.
// Falls back to FNV-like hash for non-numeric keys.
func PartitionForKey(key string, count int) int {
	if count <= 0 {
		return 0
	}
	if count == 1 {
		return 0
	}
	num := ExtractTrailingNumber(key)
	if num >= 0 {
		return num % count
	}
	var h uint32
	for i := 0; i < len(key); i++ {
		h = h*31 + uint32(key[i])
	}
	return int(h % uint32(count))
}

// ExtractTrailingNumber returns the trailing integer from a string.
// For example, "acct-42" returns 42. Returns -1 if no trailing digits.
func ExtractTrailingNumber(s string) int {
	end := len(s)
	start := end
	for start > 0 && s[start-1] >= '0' && s[start-1] <= '9' {
		start--
	}
	if start == end {
		return -1
	}
	n, err := strconv.Atoi(s[start:end])
	if err != nil {
		return -1
	}
	return n
}
