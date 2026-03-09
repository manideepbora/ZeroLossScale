package main

import (
	"context"

	"nats-poc/autoscale"

	"github.com/nats-io/nats.go/jetstream"
)

// ConsumerRuntime abstracts how consumer instances are managed.
// Docker and Kubernetes backends both implement this interface.
type ConsumerRuntime interface {
	// EnsureConsumers brings up the specified number of consumers for a stream.
	EnsureConsumers(ctx context.Context, count int, streamName, prefix string) error

	// ActivePartitions returns sorted partition IDs of running consumers.
	ActivePartitions() []int

	// Shutdown cleans up consumers on controlplane exit.
	Shutdown(ctx context.Context)

	// NewScaleBackend returns a ConsumerBackend for the scaler to use during repartition.
	NewScaleBackend(js jetstream.JetStream, streamName string) autoscale.ConsumerBackend
}
