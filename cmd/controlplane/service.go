package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"nats-poc/autoscale"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// StreamRegistration tracks a registered stream and its scaler + reconciler.
type StreamRegistration struct {
	Cfg        autoscale.Config
	PM         *autoscale.PartitionManager
	Scaler     *autoscale.Scaler
	Reconciler *autoscale.Reconciler
	AutoScaler *autoscale.AutoScaler // nil when auto-scaling is disabled
}

// Service holds the control plane state and provides query methods for the UI.
type Service struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	backend ConsumerRuntime
	seqKV   jetstream.KeyValue

	leader *autoscale.LeaderElector
	asCfg  autoscale.AutoScaleConfig // auto-scale configuration

	mu            sync.Mutex
	registrations map[string]*StreamRegistration // stream name -> registration
	buffering     bool
}

// Register creates streams, KV bucket, and initial consumers for a new stream.
func (s *Service) Register(ctx context.Context, streamName string, partitions int) (*StreamRegistration, error) {
	s.mu.Lock()
	if reg, ok := s.registrations[streamName]; ok {
		s.mu.Unlock()
		return reg, nil
	}
	s.mu.Unlock()

	cfg := autoscale.NewConfig(streamName)

	pm, err := autoscale.RegisterStream(ctx, s.js, cfg, partitions)
	if err != nil {
		return nil, fmt.Errorf("register stream %s: %w", streamName, err)
	}

	scaler := &autoscale.Scaler{
		JS:      s.js,
		PM:      pm,
		Cfg:     cfg,
		Backend: s.backend.NewScaleBackend(s.js, cfg.StreamName),
		DLQ:     autoscale.NewDLQPublisher(s.js, cfg.DLQSubject),
	}

	reconciler := autoscale.NewReconciler(s.js, scaler, pm, cfg)

	var as *autoscale.AutoScaler
	if s.asCfg.Enabled {
		as = autoscale.NewAutoScaler(s.js, pm, cfg, s.asCfg)
	}

	reg := &StreamRegistration{
		Cfg:        cfg,
		PM:         pm,
		Scaler:     scaler,
		Reconciler: reconciler,
		AutoScaler: as,
	}

	s.mu.Lock()
	s.registrations[streamName] = reg
	s.mu.Unlock()

	// Start initial consumers via the runtime backend.
	if err := s.backend.EnsureConsumers(ctx, partitions, cfg.StreamName, cfg.SubjectPrefix); err != nil {
		log.Printf("[controlplane] ensure initial consumers: %v", err)
	}

	// If this instance is already the leader, start the reconciler immediately.
	if s.leader != nil && s.leader.IsLeader() {
		log.Printf("[controlplane] starting reconciler for %s (already leader)", streamName)
		reconciler.Start(ctx)
		if as != nil {
			as.Start(ctx)
		}
	}

	log.Printf("[controlplane] registered stream %s with %d partitions (KV: %s)", streamName, partitions, cfg.KVBucket)
	return reg, nil
}

// Scale writes the desired partition count to KV. The leader's reconciler
// picks it up and runs the repartition protocol. This is safe to call
// from any replica.
func (s *Service) Scale(ctx context.Context, streamName string, newCount int) error {
	s.mu.Lock()
	reg, ok := s.registrations[streamName]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("stream %s not registered", streamName)
	}

	return autoscale.SetDesired(ctx, s.js, reg.Cfg.KVBucket, newCount)
}

// StartReconcilers starts reconcilers (and auto-scalers if enabled) for all registered streams.
// Called when this instance becomes leader.
func (s *Service) StartReconcilers(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, reg := range s.registrations {
		log.Printf("[controlplane] starting reconciler for %s (leader)", name)
		reg.Reconciler.Start(ctx)
		if reg.AutoScaler != nil {
			reg.AutoScaler.Start(ctx)
		}
	}
}

// StopReconcilers stops all running reconcilers and auto-scalers.
// Called when this instance loses leadership.
func (s *Service) StopReconcilers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, reg := range s.registrations {
		log.Printf("[controlplane] stopping reconciler for %s (lost leadership)", name)
		reg.Reconciler.Stop()
		if reg.AutoScaler != nil {
			reg.AutoScaler.Stop()
		}
	}
}

// DefaultRegistration returns the first (or only) registration.
func (s *Service) DefaultRegistration() *StreamRegistration {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, reg := range s.registrations {
		return reg
	}
	return nil
}

// PartitionCount returns the current partition count for the default stream.
func (s *Service) PartitionCount() int {
	reg := s.DefaultRegistration()
	if reg == nil {
		return 0
	}
	return reg.PM.Count()
}

// IsBuffering reports whether a repartition is in progress.
func (s *Service) IsBuffering() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buffering
}

// StreamInfo holds message counts for a JetStream stream.
type StreamInfo struct {
	Name     string `json:"name"`
	Messages uint64 `json:"messages"`
	Bytes    uint64 `json:"bytes"`
}

func (s *Service) getStreamInfo(ctx context.Context) (StreamInfo, StreamInfo) {
	reg := s.DefaultRegistration()
	if reg == nil {
		return StreamInfo{}, StreamInfo{}
	}

	var buffer, orders StreamInfo
	buffer.Name = reg.Cfg.BufferStreamName
	orders.Name = reg.Cfg.StreamName

	if stream, err := s.js.Stream(ctx, reg.Cfg.BufferStreamName); err == nil {
		if info, err := stream.Info(ctx); err == nil {
			buffer.Messages = info.State.Msgs
			buffer.Bytes = info.State.Bytes
		}
	}
	if stream, err := s.js.Stream(ctx, reg.Cfg.StreamName); err == nil {
		if info, err := stream.Info(ctx); err == nil {
			orders.Messages = info.State.Msgs
			orders.Bytes = info.State.Bytes
		}
	}
	return buffer, orders
}

// NATSConsumerInfo holds NATS consumer details for the UI.
type NATSConsumerInfo struct {
	Name       string `json:"name"`
	Partition  int    `json:"partition"`
	Delivered  uint64 `json:"delivered"`
	Pending    uint64 `json:"pending"`
	AckPending int    `json:"ack_pending"`
}

func (s *Service) getNATSConsumers(ctx context.Context) []NATSConsumerInfo {
	reg := s.DefaultRegistration()
	if reg == nil {
		return nil
	}

	count := reg.PM.Count()
	var consumers []NATSConsumerInfo
	for i := 0; i < count; i++ {
		name := autoscale.ConsumerName(i)
		cons, err := s.js.Consumer(ctx, reg.Cfg.StreamName, name)
		if err != nil {
			continue
		}
		ci, err := cons.Info(ctx)
		if err != nil {
			continue
		}
		consumers = append(consumers, NATSConsumerInfo{
			Name:       name,
			Partition:  i,
			Delivered:  ci.Delivered.Consumer,
			Pending:    ci.NumPending,
			AckPending: ci.NumAckPending,
		})
	}
	return consumers
}

// SequenceEntry represents a per-key sequence from the KV bucket.
type SequenceEntry struct {
	Key       string `json:"key"`
	Sequence  int    `json:"sequence"`
	Partition int    `json:"partition"`
	Source    string `json:"source"`
}

func (s *Service) getSequenceState(ctx context.Context) (map[string]int, map[string]SequenceEntry) {
	published := make(map[string]int)
	consumed := make(map[string]SequenceEntry)

	if s.seqKV == nil {
		return published, consumed
	}

	keys, err := s.seqKV.Keys(ctx)
	if err != nil {
		return published, consumed
	}

	for _, k := range keys {
		entry, err := s.seqKV.Get(ctx, k)
		if err != nil {
			continue
		}
		var se SequenceEntry
		if err := json.Unmarshal(entry.Value(), &se); err != nil {
			continue
		}

		if strings.HasPrefix(k, "pub.") {
			acctKey := strings.TrimPrefix(k, "pub.")
			published[acctKey] = se.Sequence
		} else if strings.HasPrefix(k, "cons.") {
			parts := strings.SplitN(k, ".", 3)
			if len(parts) == 3 {
				acctKey := parts[2]
				if existing, ok := consumed[acctKey]; !ok || se.Sequence > existing.Sequence {
					se.Key = acctKey
					consumed[acctKey] = se
				}
			}
		}
	}

	return published, consumed
}

func (s *Service) getProducerStatus() map[string]any {
	msg, err := s.nc.Request("producer.status", nil, 2*time.Second)
	if err != nil {
		return map[string]any{"rate": 0, "published": 0, "buffering": false, "partitions": 0}
	}
	var resp map[string]any
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return map[string]any{"rate": 0, "published": 0, "buffering": false, "partitions": 0}
	}
	return resp
}

// buildStatusData assembles the full status snapshot used by /status and /events.
func (s *Service) buildStatusData(ctx context.Context) map[string]any {
	bufferInfo, ordersInfo := s.getStreamInfo(ctx)
	pubSeqs, consSeqs := s.getSequenceState(ctx)
	prodStatus := s.getProducerStatus()

	reg := s.DefaultRegistration()
	streamName := ""
	bufferName := ""
	prefix := ""
	if reg != nil {
		streamName = reg.Cfg.StreamName
		bufferName = reg.Cfg.BufferStreamName
		prefix = reg.Cfg.SubjectPrefix
	}

	return map[string]any{
		"stream_name":    streamName,
		"buffer_stream":  bufferName,
		"subject_prefix": prefix,
		"partitions":     s.PartitionCount(),
		"buffering":      s.IsBuffering(),
		"consumers":      s.backend.ActivePartitions(),
		"nats_consumers": s.getNATSConsumers(ctx),
		"producer":       prodStatus,
		"streams":        map[string]any{"buffer": bufferInfo, "orders": ordersInfo},
		"pub_sequences":  pubSeqs,
		"cons_sequences": consSeqs,
		"registered":     s.RegisteredStreams(),
	}
}
