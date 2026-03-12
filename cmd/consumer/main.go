package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"nats-poc/autoscale"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	natsURL := autoscale.EnvOr("NATS_URL", nats.DefaultURL)
	healthPort := autoscale.EnvOr("HEALTH_PORT", "8080")
	streamName := autoscale.EnvOr("STREAM_NAME", "AUTO_ORDERS")
	subjectPrefix := autoscale.EnvOr("SUBJECT_PREFIX", streamName) // default: same as stream name

	// Partition ID: explicit env var takes precedence; otherwise derive from
	// StatefulSet pod hostname (e.g. "consumer-3" -> partition 3).
	partitionID := autoscale.EnvInt("PARTITION_ID", -1)
	if partitionID < 0 {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatalf("[consumer] cannot determine hostname: %v", err)
		}
		ordinal, err := autoscale.OrdinalFromHostname(hostname)
		if err != nil {
			log.Fatalf("[consumer] cannot derive partition from hostname %q (set PARTITION_ID explicitly): %v", hostname, err)
		}
		partitionID = ordinal
		log.Printf("[consumer] derived partition=%d from hostname=%s", partitionID, hostname)
	}

	cfg := autoscale.NewConfig(streamName)

	log.Printf("[consumer] partition=%d stream=%s connecting to %s", partitionID, streamName, natsURL)

	nc, err := nats.Connect(natsURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.Name(fmt.Sprintf("consumer-p%d", partitionID)),
	)
	if err != nil {
		log.Fatalf("[consumer] connect: %v", err)
	}
	defer nc.Close()
	log.Printf("[consumer] connected to %s", nc.ConnectedUrl())

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("[consumer] jetstream: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Ensure streams exist (idempotent).
	if err := cfg.EnsureStreams(ctx, js); err != nil {
		log.Fatalf("[consumer] %v", err)
	}

	// Create/get KV bucket for sequence reporting.
	seqKV, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  "sequence-state",
		TTL:     5 * time.Minute,
		Storage: jetstream.MemoryStorage,
	})
	if err != nil {
		log.Fatalf("[consumer] create sequence KV: %v", err)
	}

	filterSubject := autoscale.SubjectForPartition(subjectPrefix, partitionID)
	consumerName := autoscale.ConsumerName(partitionID)

	// Purge stale messages on this subject to prevent re-delivery after repartition.
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		log.Printf("[consumer] WARNING: cannot get stream for purge: %v", err)
	} else if err := stream.Purge(ctx, jetstream.WithPurgeSubject(filterSubject)); err != nil {
		log.Printf("[consumer] WARNING: purge %s failed: %v", filterSubject, err)
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       cfg.AckWait,
		MaxDeliver:    cfg.MaxDeliver,
		MaxAckPending: cfg.MaxAckPending,
	})
	if err != nil {
		log.Fatalf("[consumer] create consumer %s: %v", consumerName, err)
	}
	log.Printf("[consumer] consuming %s on stream %s", filterSubject, streamName)

	var received atomic.Int64
	var violations atomic.Int64
	var gaps atomic.Int64
	var totalMissing atomic.Int64
	var mu sync.Mutex
	lastSeqByKey := make(map[string]int)
	gapsByKey := make(map[string][2]int)     // key -> [gap_events, total_missing]
	violationsByKey := make(map[string]int64) // key -> violation count

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		received.Add(1)

		var m autoscale.Message
		if err := json.Unmarshal(msg.Data(), &m); err != nil {
			log.Printf("[consumer] unmarshal error: %v", err)
			msg.Ack()
			return
		}

		mu.Lock()
		if last, ok := lastSeqByKey[m.Key]; ok {
			if m.Sequence <= last {
				violations.Add(1)
				violationsByKey[m.Key]++
				log.Printf("[consumer] VIOLATION partition=%d key=%s seq=%d after=%d", partitionID, m.Key, m.Sequence, last)
			} else if m.Sequence > last+1 {
				gap := m.Sequence - last - 1
				gaps.Add(1)
				totalMissing.Add(int64(gap))
				g := gapsByKey[m.Key]
				g[0]++
				g[1] += gap
				gapsByKey[m.Key] = g
				log.Printf("[consumer] GAP partition=%d key=%s expected=%d got=%d missing=%d", partitionID, m.Key, last+1, m.Sequence, gap)
			}
		}
		lastSeqByKey[m.Key] = m.Sequence
		mu.Unlock()

		if received.Load()%100 == 0 {
			log.Printf("[consumer] partition=%d received=%d violations=%d", partitionID, received.Load(), violations.Load())
		}

		if ackErr := msg.Ack(); ackErr != nil {
			log.Printf("[consumer] ack error: %v", ackErr)
		}
	})
	if err != nil {
		log.Fatalf("[consumer] consume: %v", err)
	}

	// Periodically report per-key sequences to KV.
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				mu.Lock()
				for k, seq := range lastSeqByKey {
					entry := map[string]any{
						"sequence":  seq,
						"partition": partitionID,
						"source":    "consumer",
					}
					if g, ok := gapsByKey[k]; ok {
						entry["gaps"] = g[0]
						entry["gap_total"] = g[1]
					}
					if v, ok := violationsByKey[k]; ok && v > 0 {
						entry["violations"] = v
					}
					data, _ := json.Marshal(entry)
					seqKV.Put(ctx, fmt.Sprintf("cons.p%d.%s", partitionID, k), data)
				}
				mu.Unlock()
			}
		}
	}()

	// Health endpoint.
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"partition":%d,"received":%d,"violations":%d,"gaps":%d,"missing":%d,"stream":"%s","status":"ok"}`,
			partitionID, received.Load(), violations.Load(), gaps.Load(), totalMissing.Load(), streamName)
	})
	healthServer := &http.Server{
		Addr:    ":" + healthPort,
		Handler: healthMux,
	}
	go func() {
		log.Printf("[consumer] health endpoint on :%s/health", healthPort)
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[consumer] health server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("[consumer] shutting down. partition=%d received=%d violations=%d gaps=%d missing=%d",
		partitionID, received.Load(), violations.Load(), gaps.Load(), totalMissing.Load())

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("[consumer] health server shutdown: %v", err)
	}

	cc.Drain()

	// Delete the durable consumer on shutdown so the control plane can recreate cleanly.
	if err := js.DeleteConsumer(shutdownCtx, streamName, consumerName); err != nil {
		log.Printf("[consumer] cleanup consumer %s: %v", consumerName, err)
	} else {
		log.Printf("[consumer] deleted consumer %s", consumerName)
	}
}

