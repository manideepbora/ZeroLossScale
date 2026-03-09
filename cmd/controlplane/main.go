package main

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"nats-poc/autoscale"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

//go:embed static/index.html
var staticFiles embed.FS

func main() {
	natsURL := autoscale.EnvOr("NATS_URL", nats.DefaultURL)
	apiPort := autoscale.EnvOr("API_PORT", "9090")
	backendType := autoscale.EnvOr("BACKEND", "docker") // "docker" or "k8s"
	initialStream := autoscale.EnvOr("STREAM_NAME", "")  // optional: auto-register on startup
	initialPartitions := autoscale.EnvInt("INITIAL_PARTITIONS", 1)
	instanceID := autoscale.EnvOr("INSTANCE_ID", defaultInstanceID())
	leaderBucket := autoscale.EnvOr("LEADER_BUCKET", "controlplane_leader")

	// Auto-scaling config (disabled by default — set AUTOSCALE_ENABLED=true to enable).
	asCfg := autoscale.DefaultAutoScaleConfig()
	if autoscale.EnvOr("AUTOSCALE_ENABLED", "false") == "true" {
		asCfg.Enabled = true
	}
	asCfg.LagHighWater = uint64(autoscale.EnvInt("AUTOSCALE_LAG_HIGH", int(asCfg.LagHighWater)))
	asCfg.LagLowWater = uint64(autoscale.EnvInt("AUTOSCALE_LAG_LOW", int(asCfg.LagLowWater)))
	asCfg.MinPartitions = autoscale.EnvInt("AUTOSCALE_MIN", asCfg.MinPartitions)
	asCfg.MaxPartitions = autoscale.EnvInt("AUTOSCALE_MAX", asCfg.MaxPartitions)

	log.Printf("[controlplane] starting (instance=%s, backend=%s, nats=%s, api=:%s, autoscale=%v)",
		instanceID, backendType, natsURL, apiPort, asCfg.Enabled)

	nc, err := nats.Connect(natsURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.Name("controlplane-"+instanceID),
	)
	if err != nil {
		log.Fatalf("[controlplane] connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("[controlplane] jetstream: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Sequence state KV bucket (for UI monitoring).
	seqKV, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  "sequence-state",
		TTL:     5 * time.Minute,
		Storage: jetstream.MemoryStorage,
	})
	if err != nil {
		log.Printf("[controlplane] WARNING: sequence KV not available: %v", err)
	}

	// Initialize consumer runtime backend.
	var backend ConsumerRuntime
	switch backendType {
	case "k8s":
		k8sCfg := K8sConfig{
			Namespace:   autoscale.EnvOr("K8S_NAMESPACE", ""),
			StatefulSet: autoscale.EnvOr("K8S_STATEFULSET", "consumer"),
		}
		km, err := NewK8sManager(k8sCfg)
		if err != nil {
			log.Fatalf("[controlplane] k8s backend: %v", err)
		}
		backend = km
		log.Printf("[controlplane] using Kubernetes backend (ns=%s, sts=%s)", k8sCfg.Namespace, k8sCfg.StatefulSet)
	default:
		networkName := autoscale.EnvOr("DOCKER_NETWORK", "nats_poc_default")
		consumerImage := autoscale.EnvOr("CONSUMER_IMAGE", "nats_poc-consumer")
		internalNATSURL := autoscale.EnvOr("INTERNAL_NATS_URL", "nats://nats:4222")
		backend = NewContainerManager(networkName, consumerImage, internalNATSURL)
		log.Printf("[controlplane] using Docker backend (network=%s, image=%s)", networkName, consumerImage)
	}

	// Leader election — only the leader runs reconcilers.
	leader, err := autoscale.NewLeaderElector(ctx, js, leaderBucket, instanceID, 5*time.Second)
	if err != nil {
		log.Fatalf("[controlplane] leader elector: %v", err)
	}

	svc := &Service{
		nc:            nc,
		js:            js,
		backend:       backend,
		seqKV:         seqKV,
		leader:        leader,
		asCfg:         asCfg,
		registrations: make(map[string]*StreamRegistration),
	}

	leader.Start(ctx)

	// If STREAM_NAME is set, auto-register on startup (backward compat).
	if initialStream != "" {
		if _, err := svc.Register(ctx, initialStream, initialPartitions); err != nil {
			log.Fatalf("[controlplane] register initial stream: %v", err)
		}
		time.Sleep(2 * time.Second) // let consumers connect
		log.Printf("[controlplane] ready, stream=%s, %d initial consumers", initialStream, initialPartitions)
	} else {
		log.Printf("[controlplane] ready, no initial stream (use POST /register)")
	}

	// Wire leader callbacks — start/stop reconcilers on election changes.
	// Must be set after Register so reconcilers have streams to watch.
	autoscale.WithOnElected(func() {
		log.Printf("[controlplane] elected as leader (%s)", instanceID)
		svc.StartReconcilers(ctx)
	})(leader)
	autoscale.WithOnLost(func() {
		log.Printf("[controlplane] lost leadership (%s)", instanceID)
		svc.StopReconcilers()
	})(leader)

	// If already leader (won before callbacks were set), start now.
	if leader.IsLeader() {
		svc.StartReconcilers(ctx)
	}

	registerHandlers(svc, ctx)

	go func() {
		log.Printf("[controlplane] UI + API on http://localhost:%s", apiPort)
		if err := http.ListenAndServe(":"+apiPort, nil); err != nil {
			log.Fatalf("[controlplane] http: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[controlplane] shutting down...")

	svc.StopReconcilers()
	leader.Stop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	backend.Shutdown(shutdownCtx)
}

func defaultInstanceID() string {
	hostname, err := os.Hostname()
	if err == nil && hostname != "" {
		return hostname
	}
	b := make([]byte, 4)
	rand.Read(b)
	return "cp-" + hex.EncodeToString(b)
}

func registerHandlers(svc *Service, ctx context.Context) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		data, err := staticFiles.ReadFile("static/index.html")
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(data)
	})

	// POST /register?stream=MY_ORDERS&partitions=3
	http.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		streamName := strings.TrimSpace(r.URL.Query().Get("stream"))
		if streamName == "" {
			http.Error(w, "stream parameter required", http.StatusBadRequest)
			return
		}
		partitions := 1
		if p := r.URL.Query().Get("partitions"); p != "" {
			n, err := strconv.Atoi(p)
			if err != nil || n < 1 || n > autoscale.MaxPartitions {
				http.Error(w, fmt.Sprintf("partitions must be 1-%d", autoscale.MaxPartitions), http.StatusBadRequest)
				return
			}
			partitions = n
		}

		log.Printf("[controlplane] register request: stream=%s partitions=%d", streamName, partitions)
		reg, err := svc.Register(ctx, streamName, partitions)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":            "ok",
			"stream":            reg.Cfg.StreamName,
			"buffer_stream":     reg.Cfg.BufferStreamName,
			"kv_bucket":         reg.Cfg.KVBucket,
			"partitions":        partitions,
			"subject_prefix":    reg.Cfg.SubjectPrefix,
			"partition_count_key": "partition_count",
			"mode_key":          "mode",
		})
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(svc.buildStatusData(r.Context()))
	})

	http.HandleFunc("/scale", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		countStr := r.URL.Query().Get("partitions")
		count, err := strconv.Atoi(countStr)
		if err != nil || count < 1 || count > autoscale.MaxPartitions {
			http.Error(w, fmt.Sprintf("partitions must be 1-%d", autoscale.MaxPartitions), http.StatusBadRequest)
			return
		}

		// Use explicit stream param or default.
		streamName := r.URL.Query().Get("stream")
		if streamName == "" {
			reg := svc.DefaultRegistration()
			if reg == nil {
				http.Error(w, "no stream registered", http.StatusBadRequest)
				return
			}
			streamName = reg.Cfg.StreamName
		}

		log.Printf("[controlplane] scale request: stream=%s partitions=%d", streamName, count)
		if err := svc.Scale(ctx, streamName, count); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":     "accepted",
			"stream":     streamName,
			"desired":    count,
			"current":    svc.PartitionCount(),
		})
	})

	http.HandleFunc("/rate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		rateStr := r.URL.Query().Get("rate")
		rateVal, err := strconv.Atoi(rateStr)
		if err != nil || rateVal < 0 || rateVal > 1000 {
			http.Error(w, "rate must be 0-1000", http.StatusBadRequest)
			return
		}

		msg, err := svc.nc.Request("producer.rate", []byte(rateStr), 5*time.Second)
		if err != nil {
			http.Error(w, fmt.Sprintf("producer unreachable: %v", err), http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(msg.Data)
	})

	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", 500)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				jsonBytes, _ := json.Marshal(svc.buildStatusData(r.Context()))
				fmt.Fprintf(w, "data: %s\n\n", jsonBytes)
				flusher.Flush()
			}
		}
	})

	http.HandleFunc("/leader", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"instance_id": svc.leader.InstanceID(),
			"is_leader":   svc.leader.IsLeader(),
			"leader_id":   svc.leader.LeaderID(r.Context()),
		})
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		status := autoscale.CheckHealth(r.Context(), svc.nc, svc.js, nil)
		status.Components["leader"] = autoscale.ComponentHealth{
			Healthy: true,
			Message: fmt.Sprintf("instance=%s is_leader=%v", svc.leader.InstanceID(), svc.leader.IsLeader()),
		}

		w.Header().Set("Content-Type", "application/json")
		if !status.Healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(status)
	})

	// Readiness probe — returns 200 only when the service can accept traffic.
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		ready := svc.nc.IsConnected() && len(svc.RegisteredStreams()) > 0
		w.Header().Set("Content-Type", "application/json")
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"ready":      ready,
			"instance":   svc.leader.InstanceID(),
			"is_leader":  svc.leader.IsLeader(),
			"streams":    len(svc.RegisteredStreams()),
			"connected":  svc.nc.IsConnected(),
		})
	})
}

// RegisteredStreams returns a list of registered stream names.
func (s *Service) RegisteredStreams() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.registrations))
	for name := range s.registrations {
		names = append(names, name)
	}
	return names
}
