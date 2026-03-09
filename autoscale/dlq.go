package autoscale

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DLQ reasons.
const (
	DLQReasonUnmarshal    = "unmarshal_failure"
	DLQReasonMaxDelivery  = "max_delivery_exhausted"
	DLQReasonReplayFailed = "replay_publish_failed"
)

// DLQPublisher routes poison messages to a dead-letter stream.
type DLQPublisher struct {
	js      jetstream.JetStream
	subject string
}

// NewDLQPublisher creates a publisher that sends to the given DLQ subject.
func NewDLQPublisher(js jetstream.JetStream, dlqSubject string) *DLQPublisher {
	return &DLQPublisher{js: js, subject: dlqSubject}
}

// Send publishes a poison message to the DLQ with metadata headers.
func (d *DLQPublisher) Send(ctx context.Context, originalData []byte, originalSubject, reason string, err error) {
	if d == nil {
		return
	}

	headers := nats.Header{}
	headers.Set("X-DLQ-Reason", reason)
	headers.Set("X-DLQ-Original-Subject", originalSubject)
	headers.Set("X-DLQ-Timestamp", fmt.Sprintf("%d", time.Now().UnixNano()))
	if err != nil {
		headers.Set("X-DLQ-Error", err.Error())
	}

	msg := &nats.Msg{
		Subject: d.subject,
		Data:    originalData,
		Header:  headers,
	}

	if _, pubErr := d.js.PublishMsg(ctx, msg); pubErr != nil {
		log.Printf("[dlq] failed to publish to DLQ %s: %v (reason=%s)", d.subject, pubErr, reason)
	}
}

// EnsureDLQStream creates the DLQ stream if it doesn't exist.
// Uses DiscardNew so DLQ itself never silently drops messages.
func EnsureDLQStream(ctx context.Context, js jetstream.JetStream, cfg Config) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       cfg.DLQStreamName,
		Subjects:   []string{cfg.DLQSubject},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		MaxMsgs:    10_000_000,
		MaxAge:     7 * 24 * time.Hour,
		Replicas:   1,
		Discard:    jetstream.DiscardNew,
		Duplicates: 2 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("create DLQ stream %s: %w", cfg.DLQStreamName, err)
	}
	return nil
}
