package autoscale

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Config holds all configuration for the autoscale system.
// All names are derived from the StreamName supplied by the application.
type Config struct {
	// Primary stream — where partitioned messages flow.
	StreamName    string // e.g. "MY_ORDERS"
	SubjectPrefix string // e.g. "MY_ORDERS" (used as MY_ORDERS.00, MY_ORDERS.01, ...)
	StreamConfig  jetstream.StreamConfig

	// Buffer stream — used only during repartition.
	BufferStreamName string // e.g. "MY_ORDERS_BUFFER"
	BufferSubject    string // e.g. "MY_ORDERS_BUFFER"

	// KV bucket for mode + partition_count signaling.
	KVBucket string // e.g. "MY_ORDERS_config"

	// Dead-letter queue stream for poison messages.
	DLQStreamName string // e.g. "MY_ORDERS_DLQ"
	DLQSubject    string // e.g. "MY_ORDERS_DLQ"

	// Partition management.
	InitialPartitions int

	// Consumer settings.
	AckWait       time.Duration
	MaxDeliver    int
	MaxAckPending int

	// Drain / repartition.
	DrainTimeout time.Duration
}

// NewConfig creates a Config where all names are derived from the given stream name.
// For example, NewConfig("MY_ORDERS") produces:
//
//	StreamName:       "MY_ORDERS"
//	SubjectPrefix:    "MY_ORDERS"
//	BufferStreamName: "MY_ORDERS_BUFFER"
//	BufferSubject:    "MY_ORDERS_BUFFER"
//	KVBucket:         "MY_ORDERS_config"
func NewConfig(streamName string) Config {
	name := strings.ToUpper(streamName)
	return Config{
		StreamName:    name,
		SubjectPrefix: name,
		StreamConfig: jetstream.StreamConfig{
			Name:       name,
			Subjects:   []string{name + ".>"},
			Storage:    jetstream.FileStorage,
			Retention:  jetstream.LimitsPolicy,
			MaxMsgs:    1_000_000,
			MaxAge:     24 * time.Hour,
			Replicas:   1,
			Discard:    jetstream.DiscardOld,
			Duplicates: 2 * time.Minute,
		},

		BufferStreamName: name + "_BUFFER",
		BufferSubject:    name + "_BUFFER",

		DLQStreamName: name + "_DLQ",
		DLQSubject:    name + "_DLQ",

		KVBucket:          name + "_config",
		InitialPartitions: 1,

		AckWait:       30 * time.Second,
		MaxDeliver:    5,
		MaxAckPending: 1000,

		DrainTimeout: 30 * time.Second,
	}
}

// DefaultConfig returns a config using "AUTO_ORDERS" as the stream name.
// Kept for backward compatibility with tests.
func DefaultConfig() Config {
	return NewConfig("AUTO_ORDERS")
}

// KV keys written by the controlplane and watched by producers.
const (
	KVKeyPartitionCount = "partition_count"
	KVKeyMode           = "mode"    // "direct" or "buffer"
	KVModeBuffer        = "buffer"
	KVModeDirect        = "direct"
)

// EnsureStreams creates or updates both the partition and buffer streams.
// Safe to call from multiple services — uses CreateOrUpdate semantics.
func (c Config) EnsureStreams(ctx context.Context, js jetstream.JetStream) error {
	streamCfg := c.StreamConfig
	streamCfg.Name = c.StreamName
	if len(streamCfg.Subjects) == 0 {
		streamCfg.Subjects = []string{c.SubjectPrefix + ".>"}
	}
	if _, err := js.CreateOrUpdateStream(ctx, streamCfg); err != nil {
		return fmt.Errorf("create partition stream: %w", err)
	}

	bufferCfg := jetstream.StreamConfig{
		Name:       c.BufferStreamName,
		Subjects:   []string{c.BufferSubject},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		MaxMsgs:    1_000_000,
		MaxAge:     24 * time.Hour,
		Replicas:   1,
		Discard:    jetstream.DiscardOld,
		Duplicates: 2 * time.Minute,
	}
	if _, err := js.CreateOrUpdateStream(ctx, bufferCfg); err != nil {
		return fmt.Errorf("create buffer stream: %w", err)
	}

	// Create DLQ stream for poison messages.
	if c.DLQStreamName != "" {
		if err := EnsureDLQStream(ctx, js, c); err != nil {
			return err
		}
	}

	return nil
}
