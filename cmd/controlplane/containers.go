package main

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"sort"
	"sync"
	"time"

	"nats-poc/autoscale"

	"github.com/nats-io/nats.go/jetstream"
)

// ContainerManager manages consumer Docker containers.
// It implements autoscale.ConsumerBackend.
type ContainerManager struct {
	mu               sync.Mutex
	networkName      string
	consumerImage    string
	natsURL          string         // internal NATS URL for consumer containers
	activePartitions map[int]string // partition -> container ID

	// Stream config passed to consumer containers via env vars.
	streamName    string
	subjectPrefix string
}

func NewContainerManager(networkName, consumerImage, natsURL string) *ContainerManager {
	return &ContainerManager{
		networkName:      networkName,
		consumerImage:    consumerImage,
		natsURL:          natsURL,
		activePartitions: make(map[int]string),
	}
}

// StartConsumer creates and starts a consumer container for the given partition.
// streamName and prefix are passed as env vars so the consumer knows which stream to consume.
func (cm *ContainerManager) StartConsumer(ctx context.Context, partitionID int, streamName, prefix string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.activePartitions[partitionID]; exists {
		return nil // already running
	}

	// Store for future StartConsumer calls via ConsumerBackend interface.
	cm.streamName = streamName
	cm.subjectPrefix = prefix

	return cm.startContainerLocked(ctx, partitionID)
}

// StartConsumerDefault starts a consumer using the stored stream config.
// This satisfies the autoscale.ConsumerBackend interface.
func (cm *ContainerManager) StartConsumerDefault(ctx context.Context, partitionID int) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.activePartitions[partitionID]; exists {
		return nil
	}

	return cm.startContainerLocked(ctx, partitionID)
}

func (cm *ContainerManager) startContainerLocked(ctx context.Context, partitionID int) error {
	name := fmt.Sprintf("nats-consumer-%d", partitionID)

	// Remove any leftover container with same name.
	exec.CommandContext(ctx, "docker", "rm", "-f", name).Run()

	var containerID string
	err := retryDocker(ctx, fmt.Sprintf("start-consumer-%d", partitionID), func() error {
		cmd := exec.CommandContext(ctx, "docker", "run", "-d",
			"--name", name,
			"--network", cm.networkName,
			"-e", fmt.Sprintf("PARTITION_ID=%d", partitionID),
			"-e", fmt.Sprintf("NATS_URL=%s", cm.natsURL),
			"-e", fmt.Sprintf("STREAM_NAME=%s", cm.streamName),
			"-e", fmt.Sprintf("SUBJECT_PREFIX=%s", cm.subjectPrefix),
			"--restart", "unless-stopped",
			cm.consumerImage,
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			// Clean up failed container before retry.
			exec.CommandContext(ctx, "docker", "rm", "-f", name).Run()
			return fmt.Errorf("docker run consumer-%d: %v: %s", partitionID, err, string(out))
		}
		containerID = string(out)
		return nil
	})
	if err != nil {
		return err
	}

	if len(containerID) > 12 {
		containerID = containerID[:12]
	}
	cm.activePartitions[partitionID] = containerID
	log.Printf("[containers] started consumer-%d (container: %s)", partitionID, containerID)
	return nil
}

// retryDocker retries a docker operation up to 3 times with 2s base delay.
func retryDocker(ctx context.Context, name string, fn func() error) error {
	var lastErr error
	delays := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second}
	for attempt := 0; attempt < 3; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if attempt < 2 {
			log.Printf("[containers] %s attempt %d/3 failed: %v (retrying)", name, attempt+1, lastErr)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delays[attempt]):
			}
		}
	}
	return lastErr
}

// StopConsumer stops and removes the consumer container for the given partition.
func (cm *ContainerManager) StopConsumer(ctx context.Context, partitionID int) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	name := fmt.Sprintf("nats-consumer-%d", partitionID)

	stopCmd := exec.CommandContext(ctx, "docker", "stop", "-t", "10", name)
	if out, err := stopCmd.CombinedOutput(); err != nil {
		log.Printf("[containers] stop consumer-%d: %v: %s", partitionID, err, string(out))
	}

	rmCmd := exec.CommandContext(ctx, "docker", "rm", "-f", name)
	if out, err := rmCmd.CombinedOutput(); err != nil {
		log.Printf("[containers] rm consumer-%d: %v: %s", partitionID, err, string(out))
	}

	delete(cm.activePartitions, partitionID)
	log.Printf("[containers] stopped consumer-%d", partitionID)
	return nil
}

// ActivePartitions returns the set of currently running partition IDs (sorted).
func (cm *ContainerManager) ActivePartitions() []int {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	ids := make([]int, 0, len(cm.activePartitions))
	for id := range cm.activePartitions {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

// ConsumerBackendAdapter wraps ContainerManager to satisfy autoscale.ConsumerBackend.
type ConsumerBackendAdapter struct {
	CM *ContainerManager
	JS interface {
		DeleteConsumer(ctx context.Context, stream, consumer string) error
	}
	StreamName string
}

func (a *ConsumerBackendAdapter) ScaleConsumers(ctx context.Context, oldCount, newCount int) error {
	// Stop removed consumers (scale down).
	if newCount < oldCount {
		for i := newCount; i < oldCount; i++ {
			if err := a.CM.StopConsumer(ctx, i); err != nil {
				log.Printf("[containers] stop consumer %d: %v", i, err)
			}
			if a.JS != nil && a.StreamName != "" {
				consName := autoscale.ConsumerName(i)
				if err := a.JS.DeleteConsumer(ctx, a.StreamName, consName); err != nil {
					log.Printf("[containers] delete NATS consumer %s: %v", consName, err)
				}
			}
		}
	}
	// Start new consumers (scale up).
	if newCount > oldCount {
		for i := oldCount; i < newCount; i++ {
			if err := a.CM.StartConsumerDefault(ctx, i); err != nil {
				log.Printf("[containers] start consumer %d: %v", i, err)
			}
		}
	}
	return nil
}

// EnsureConsumers starts consumer containers for partitions 0..count-1.
func (cm *ContainerManager) EnsureConsumers(ctx context.Context, count int, streamName, prefix string) error {
	for i := 0; i < count; i++ {
		if err := cm.StartConsumer(ctx, i, streamName, prefix); err != nil {
			log.Printf("[containers] start initial consumer-%d: %v", i, err)
		}
	}
	return nil
}

// Shutdown force-removes all consumer containers.
func (cm *ContainerManager) Shutdown(ctx context.Context) {
	cm.ForceRemoveAll(ctx)
}

// NewScaleBackend returns a ConsumerBackend for the scaler.
func (cm *ContainerManager) NewScaleBackend(js jetstream.JetStream, streamName string) autoscale.ConsumerBackend {
	return &ConsumerBackendAdapter{CM: cm, JS: js, StreamName: streamName}
}

// ForceRemoveAll force-removes all consumer containers without graceful stop.
func (cm *ContainerManager) ForceRemoveAll(ctx context.Context) {
	cm.mu.Lock()
	partitions := make([]int, 0, len(cm.activePartitions))
	for id := range cm.activePartitions {
		partitions = append(partitions, id)
	}
	cm.activePartitions = make(map[int]string)
	cm.mu.Unlock()

	for _, id := range partitions {
		name := fmt.Sprintf("nats-consumer-%d", id)
		cmd := exec.CommandContext(ctx, "docker", "rm", "-f", name)
		if out, err := cmd.CombinedOutput(); err != nil {
			log.Printf("[containers] force-rm consumer-%d: %v: %s", id, err, string(out))
		} else {
			log.Printf("[containers] force-removed consumer-%d", id)
		}
	}
}
