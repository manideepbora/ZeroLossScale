package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"nats-poc/autoscale"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// In-cluster paths for service account credentials.
	tokenPath  = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	caPath     = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	nsPath     = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	apiEnvHost = "KUBERNETES_SERVICE_HOST"
	apiEnvPort = "KUBERNETES_SERVICE_PORT"
)

// K8sManager manages consumer pods via a Kubernetes StatefulSet.
// It uses the raw K8s REST API — no client-go dependency required.
type K8sManager struct {
	mu sync.Mutex

	apiBase       string // e.g. "https://10.96.0.1:443"
	namespace     string
	statefulSet   string // StatefulSet name, e.g. "consumer"
	token         string
	httpClient    *http.Client
	streamName    string
	subjectPrefix string
}

// K8sConfig holds configuration for the Kubernetes backend.
type K8sConfig struct {
	Namespace   string // K8s namespace (auto-detected if empty)
	StatefulSet string // StatefulSet name for consumers
}

// NewK8sManager creates a K8sManager using in-cluster credentials.
func NewK8sManager(cfg K8sConfig) (*K8sManager, error) {
	host := os.Getenv(apiEnvHost)
	port := os.Getenv(apiEnvPort)
	if host == "" || port == "" {
		return nil, fmt.Errorf("not running in-cluster: %s/%s not set", apiEnvHost, apiEnvPort)
	}

	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}

	ns := cfg.Namespace
	if ns == "" {
		nsBytes, err := os.ReadFile(nsPath)
		if err != nil {
			return nil, fmt.Errorf("read namespace: %w", err)
		}
		ns = strings.TrimSpace(string(nsBytes))
	}

	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert: %w", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: caCertPool,
			},
		},
	}

	return &K8sManager{
		apiBase:     fmt.Sprintf("https://%s:%s", host, port),
		namespace:   ns,
		statefulSet: cfg.StatefulSet,
		token:       string(tokenBytes),
		httpClient:  httpClient,
	}, nil
}

// refreshToken re-reads the service account token from disk.
// Called only on 401 to handle K8s token rotation.
func (km *K8sManager) refreshToken() {
	data, err := os.ReadFile(tokenPath)
	if err != nil {
		log.Printf("[k8s] token refresh failed: %v", err)
		return
	}
	km.token = string(data)
	log.Printf("[k8s] token refreshed")
}

// EnsureConsumers scales the StatefulSet to the desired replica count.
func (km *K8sManager) EnsureConsumers(ctx context.Context, count int, streamName, prefix string) error {
	km.mu.Lock()
	km.streamName = streamName
	km.subjectPrefix = prefix
	km.mu.Unlock()

	return km.scaleStatefulSet(ctx, count)
}

// ActivePartitions returns partition IDs 0..replicas-1 based on the StatefulSet's ready replicas.
func (km *K8sManager) ActivePartitions() []int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	replicas, err := km.getReadyReplicas(ctx)
	if err != nil {
		log.Printf("[k8s] get ready replicas: %v", err)
		return nil
	}

	ids := make([]int, replicas)
	for i := 0; i < replicas; i++ {
		ids[i] = i
	}
	sort.Ints(ids)
	return ids
}

// Shutdown scales the StatefulSet to 0 replicas.
func (km *K8sManager) Shutdown(ctx context.Context) {
	if err := km.scaleStatefulSet(ctx, 0); err != nil {
		log.Printf("[k8s] shutdown scale-to-zero: %v", err)
	}
}

// NewScaleBackend returns a ConsumerBackend that scales the StatefulSet.
func (km *K8sManager) NewScaleBackend(js jetstream.JetStream, streamName string) autoscale.ConsumerBackend {
	return &K8sScaleBackend{km: km, js: js, streamName: streamName}
}

// scaleStatefulSet patches the StatefulSet's replica count.
func (km *K8sManager) scaleStatefulSet(ctx context.Context, replicas int) error {
	url := fmt.Sprintf("%s/apis/apps/v1/namespaces/%s/statefulsets/%s/scale",
		km.apiBase, km.namespace, km.statefulSet)

	doRequest := func() (*http.Response, error) {
		body := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
		req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, strings.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create scale request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+km.token)
		req.Header.Set("Content-Type", "application/merge-patch+json")
		return km.httpClient.Do(req)
	}

	resp, err := doRequest()
	if err != nil {
		return fmt.Errorf("scale statefulset: %w", err)
	}
	defer resp.Body.Close()

	// Retry once on 401 with a refreshed token.
	if resp.StatusCode == http.StatusUnauthorized {
		resp.Body.Close()
		km.refreshToken()
		resp, err = doRequest()
		if err != nil {
			return fmt.Errorf("scale statefulset (after token refresh): %w", err)
		}
		defer resp.Body.Close()
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("scale statefulset returned %d: %s", resp.StatusCode, string(respBody))
	}

	log.Printf("[k8s] scaled StatefulSet %s/%s to %d replicas", km.namespace, km.statefulSet, replicas)
	return nil
}

// getReadyReplicas reads the StatefulSet's status.readyReplicas.
func (km *K8sManager) getReadyReplicas(ctx context.Context) (int, error) {
	url := fmt.Sprintf("%s/apis/apps/v1/namespaces/%s/statefulsets/%s",
		km.apiBase, km.namespace, km.statefulSet)

	doRequest := func() (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+km.token)
		return km.httpClient.Do(req)
	}

	resp, err := doRequest()
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	// Retry once on 401 with a refreshed token.
	if resp.StatusCode == http.StatusUnauthorized {
		resp.Body.Close()
		km.refreshToken()
		resp, err = doRequest()
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
	}

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("get statefulset returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Status struct {
			ReadyReplicas int `json:"readyReplicas"`
		} `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("decode statefulset status: %w", err)
	}

	return result.Status.ReadyReplicas, nil
}

// K8sScaleBackend implements autoscale.ConsumerBackend by scaling the StatefulSet.
type K8sScaleBackend struct {
	km         *K8sManager
	js         jetstream.JetStream
	streamName string
}

func (b *K8sScaleBackend) ScaleConsumers(ctx context.Context, oldCount, newCount int) error {
	// Scale down: delete NATS consumers for removed partitions before scaling.
	if newCount < oldCount {
		for i := newCount; i < oldCount; i++ {
			consName := autoscale.ConsumerName(i)
			if err := b.js.DeleteConsumer(ctx, b.streamName, consName); err != nil {
				log.Printf("[k8s] delete NATS consumer %s: %v", consName, err)
			}
		}
	}

	// Patch StatefulSet replicas — K8s handles pod lifecycle.
	if err := b.km.scaleStatefulSet(ctx, newCount); err != nil {
		return fmt.Errorf("scale statefulset to %d: %w", newCount, err)
	}

	return nil
}
