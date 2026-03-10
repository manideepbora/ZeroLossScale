package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"nats-poc/autoscale"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	natsURL := autoscale.EnvOr("NATS_URL", nats.DefaultURL)
	streamName := autoscale.EnvOr("STREAM_NAME", "AUTO_ORDERS")
	controlplaneURL := autoscale.EnvOr("CONTROLPLANE_URL", "http://controlplane:9090")
	rate := autoscale.EnvInt("PUBLISH_RATE", 10)
	numKeys := autoscale.EnvInt("NUM_KEYS", 5)
	initialPartitions := autoscale.EnvInt("INITIAL_PARTITIONS", 1)

	log.Printf("[producer] connecting to %s (stream=%s, rate=%d/s, keys=%d)", natsURL, streamName, rate, numKeys)

	nc, err := nats.Connect(natsURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.Name("producer-service"),
	)
	if err != nil {
		log.Fatalf("[producer] connect: %v", err)
	}
	defer nc.Close()
	log.Printf("[producer] connected to %s", nc.ConnectedUrl())

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("[producer] jetstream: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Register with controlplane (creates streams + KV if not already done).
	registerWithControlplane(controlplaneURL, streamName, initialPartitions)

	// Wait for KV to be available, then create the publisher.
	// The publisher watches KV for mode and partition_count changes automatically.
	var pub *autoscale.DirectPublisher
	for attempt := 0; attempt < 30; attempt++ {
		pub, err = autoscale.NewDirectPublisher(ctx, js, streamName)
		if err == nil {
			break
		}
		log.Printf("[producer] waiting for KV (attempt %d): %v", attempt+1, err)
		time.Sleep(1 * time.Second)
	}
	if pub == nil {
		log.Fatalf("[producer] could not create publisher after retries: %v", err)
	}
	defer pub.Stop()

	// Create KV bucket for sequence reporting.
	seqKV, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  "sequence-state",
		TTL:     5 * time.Minute,
		Storage: jetstream.MemoryStorage,
	})
	if err != nil {
		log.Fatalf("[producer] create sequence KV: %v", err)
	}

	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("acct-%d", i)
	}

	// Mutable state for rate control and stats.
	var mu sync.Mutex
	currentRate := rate
	seqs := make(map[string]int)
	published := 0

	// Listen for rate change commands.
	nc.Subscribe("producer.rate", func(msg *nats.Msg) {
		newRate, err := strconv.Atoi(string(msg.Data))
		if err != nil || newRate < 0 || newRate > 1000 {
			msg.Respond([]byte(`{"error":"rate must be 0-1000"}`))
			return
		}
		mu.Lock()
		currentRate = newRate
		mu.Unlock()
		log.Printf("[producer] rate changed to %d/s", newRate)
		resp, _ := json.Marshal(map[string]any{"ok": true, "rate": newRate})
		msg.Respond(resp)
	})

	// Listen for status queries.
	nc.Subscribe("producer.status", func(msg *nats.Msg) {
		mu.Lock()
		resp, _ := json.Marshal(map[string]any{
			"rate":       currentRate,
			"published":  published,
			"keys":       len(keys),
			"buffering":  pub.IsBuffering(),
			"partitions": pub.PartitionCount(),
			"stream":     pub.StreamName(),
		})
		mu.Unlock()
		msg.Respond(resp)
	})

	// Periodically publish per-key sequences to KV.
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				mu.Lock()
				for k, seq := range seqs {
					data, _ := json.Marshal(map[string]any{"sequence": seq, "source": "producer"})
					if _, err := seqKV.Put(ctx, "pub."+k, data); err != nil {
						log.Printf("[producer] WARNING: failed to report sequence for %s: %v", k, err)
					}
				}
				mu.Unlock()
			}
		}
	}()

	log.Printf("[producer] publishing to stream %s (mode watched via KV)", streamName)

	idx := 0
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			mu.Lock()
			log.Printf("[producer] shutting down. published %d messages", published)
			mu.Unlock()
			return
		case <-ticker.C:
			mu.Lock()
			r := currentRate
			mu.Unlock()

			if r == 0 {
				ticker.Reset(100 * time.Millisecond)
				continue
			}

			newInterval := time.Second / time.Duration(r)
			ticker.Reset(newInterval)

			key := keys[idx%len(keys)]
			mu.Lock()
			seqs[key]++
			seq := seqs[key]
			mu.Unlock()

			msg := autoscale.Message{
				Key:       key,
				Sequence:  seq,
				Payload:   fmt.Sprintf("%s-%d", key, seq),
				Timestamp: time.Now().UnixNano(),
			}

			pubCtx, pubCancel := context.WithTimeout(ctx, 5*time.Second)
			pubErr := pub.Publish(pubCtx, msg)
			pubCancel()

			if pubErr != nil {
				log.Printf("[producer] publish error: %v", pubErr)
			} else {
				mu.Lock()
				published++
				p := published
				mu.Unlock()
				if p%100 == 0 {
					log.Printf("[producer] published %d messages (rate=%d/s, mode=%s, partitions=%d)",
						p, r, modeStr(pub.IsBuffering()), pub.PartitionCount())
				}
			}
			idx++
		}
	}
}

// registerWithControlplane calls POST /register on the controlplane.
func registerWithControlplane(baseURL, streamName string, partitions int) {
	url := fmt.Sprintf("%s/register?stream=%s&partitions=%d", baseURL, streamName, partitions)
	for attempt := 0; attempt < 10; attempt++ {
		resp, err := http.Post(url, "application/json", bytes.NewReader(nil))
		if err != nil {
			log.Printf("[producer] register attempt %d: %v (retrying)", attempt+1, err)
			time.Sleep(2 * time.Second)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == 200 {
			log.Printf("[producer] registered stream %s with controlplane", streamName)
			return
		}
		log.Printf("[producer] register attempt %d: status %d (retrying)", attempt+1, resp.StatusCode)
		time.Sleep(2 * time.Second)
	}
	log.Printf("[producer] WARNING: could not register with controlplane (may already exist)")
}

func modeStr(buffering bool) string {
	if buffering {
		return "buffer"
	}
	return "direct"
}

