# Buffer Drain Replay (BDR) implementation using NATS

**Zero-downtime dynamic partitioning** for NATS JetStream with per-key ordering, no message loss, and HA controlplane. Scale consumers up and down while producers keep publishing — no restarts, no coordination, no downtime.

Any application in any language (Go, Java, Python, etc.) can use this framework. Producers and consumers only talk to NATS; the controlplane is a standalone coordination service.

## Architecture

```
                          Normal operation (direct mode)
  ┌─────────────────────────────────────────────────────────────────────────┐
  │                                                                         │
  │  Producer(s) ──── NATS ──── [MY_ORDERS stream]                          │
  │    watch KV                   MY_ORDERS.00  MY_ORDERS.01  MY_ORDERS.02  │
  │    publish to                      │             │             │         │
  │    MY_ORDERS.<P>              Consumer-0    Consumer-1    Consumer-2     │
  │                                                                         │
  │  Controlplane (3 replicas, leader-elected)                              │
  │    ├── manages consumers (start/stop)                                   │
  │    ├── runs repartition protocol                                        │
  │    └── writes mode/partition_count to KV                                │
  └─────────────────────────────────────────────────────────────────────────┘

                          During repartition (buffer mode)
  ┌─────────────────────────────────────────────────────────────────────────┐
  │                                                                         │
  │  Producer(s) ──── NATS ──── [MY_ORDERS_BUFFER stream]                   │
  │    auto-switch                     │                                    │
  │    via KV watch              Controlplane replays to MY_ORDERS          │
  │                              with new partition mapping                 │
  └─────────────────────────────────────────────────────────────────────────┘
```

### Key design principles

- **Producers never talk to the controlplane.** They connect to NATS, watch a KV bucket, and publish. If the controlplane goes down, producers keep publishing uninterrupted.
- **Consumers are managed by the controlplane.** The controlplane creates/destroys consumer instances via a pluggable backend (Docker containers or Kubernetes StatefulSet).
- **Scaling is triggered externally.** A human, KEDA, or your monitoring system calls `POST /scale` or writes `desired_partitions` to KV. The controlplane's reconciler watches and acts.
- **All coordination is NATS-native.** Leader election, state machine, mode signaling — all use NATS JetStream KV. No etcd, no Zookeeper, no K8s operator dependency.

### How it works

1. **Register a stream** — call `POST /register?stream=MY_ORDERS&partitions=3` on any controlplane replica
2. **Controlplane creates NATS infrastructure** — partition stream, buffer stream, DLQ stream, KV bucket
3. **Producers watch KV** — `mode` and `partition_count` keys tell them where to publish
4. **Consumers** (one per partition) read from their filtered subject, ACK each message
5. **To scale** — call `POST /scale?partitions=5` or write `desired_partitions` to KV
6. **Leader's reconciler** detects the change and runs the repartition protocol
7. **Producers auto-switch** to buffer mode via KV watch — zero code changes, zero restarts

### Naming convention

All NATS resources are derived from the stream name you supply:

| Resource | Name | Example |
|----------|------|---------|
| Partition stream | `<STREAM>` | `MY_ORDERS` |
| Partition subjects | `<STREAM>.00`, `<STREAM>.01`, ... | `MY_ORDERS.00`, `MY_ORDERS.01` |
| Buffer stream | `<STREAM>_BUFFER` | `MY_ORDERS_BUFFER` |
| Buffer subject | `<STREAM>_BUFFER` | `MY_ORDERS_BUFFER` |
| Dead-letter queue | `<STREAM>_DLQ` | `MY_ORDERS_DLQ` |
| KV bucket | `<STREAM>_config` | `MY_ORDERS_config` |
| KV keys | `partition_count`, `mode`, `desired_partitions` | `"3"`, `"direct"`, `"5"` |
| Leader KV bucket | `controlplane_leader` | Configurable via `LEADER_BUCKET` env |

## Production Deployment

### HA Controlplane

Deploy **3 controlplane replicas** across availability zones. They use NATS-native leader election:

```
  AZ-1                    AZ-2                    AZ-3
  ┌──────────────┐       ┌──────────────┐       ┌──────────────┐
  │ Controlplane │       │ Controlplane │       │ Controlplane │
  │  (LEADER)    │       │  (follower)  │       │  (follower)  │
  │  reconciler  │       │  HTTP only   │       │  HTTP only   │
  │  running     │       │              │       │              │
  └──────┬───────┘       └──────┬───────┘       └──────┬───────┘
         │                      │                      │
         └──────────────────────┼──────────────────────┘
                                │
                     ┌──────────┴──────────┐
                     │   NATS Cluster      │
                     │   (3 nodes, R3)     │
                     └─────────────────────┘
```

- **All replicas** serve HTTP (status, scale API, health checks)
- **Only the leader** runs reconcilers (repartition protocol)
- Leader election uses NATS KV with TTL — `kv.Create()` for atomic lock acquisition, `kv.Update()` with CAS for renewal
- If the leader dies, the lock key expires after TTL (5s), a follower takes over and resumes any in-progress repartition from the last checkpoint

### What happens when the controlplane goes down?

| Scenario | Impact |
|----------|--------|
| Leader dies | Follower takes over in ~5s (TTL expiry). In-progress repartition resumes from last step. |
| All controlplanes die | **Producers keep publishing normally.** They only need NATS, not the controlplane. Consumers keep consuming. No scaling possible until a controlplane recovers. |
| NATS node dies | NATS cluster (R3) handles failover. Controlplane reconnects automatically. |

### Scaling triggers

Scaling can be manual (deterministic, good for demos) or automatic (lag-based):

| Method | How | Best for |
|--------|-----|----------|
| **HTTP API** | `POST /scale?partitions=5` | Manual scaling, demos, CI/CD pipelines |
| **Built-in auto-scaler** | Set `AUTOSCALE_ENABLED=true` — monitors consumer lag, writes `desired_partitions` to KV | Automatic scaling with no external dependencies |
| **KEDA** | ScaledObject watches consumer lag, writes `desired_partitions` to KV | Complex multi-source scaling policies |
| **Custom monitor** | Your app writes `desired_partitions` to NATS KV directly | Custom scaling logic |

The leader's reconciler watches the `desired_partitions` KV key and triggers repartitioning when desired != current.

### Built-in auto-scaler

The controlplane includes an optional lag-based auto-scaler. When enabled, it periodically checks total pending messages (NumPending + NumAckPending) across all partition consumers and writes `desired_partitions` to KV:

- **Total pending > high watermark** → scale up by 1 partition
- **Total pending < low watermark** → scale down by 1 partition
- A cooldown period prevents rapid oscillation

The auto-scaler only writes the *desired* count — the existing reconciler handles the full repartition protocol. This works on both Docker and Kubernetes backends.

```
  [AutoScaler]                    [Reconciler]               [Scaler]
       │                               │                        │
  check lag every 10s                  │                        │
       │                               │                        │
  lag > 5000?                          │                        │
  ──────────────┐                      │                        │
  write desired_partitions=N to KV     │                        │
                │                      │                        │
                └─────────────────► watch KV change             │
                                       │                        │
                                  desired != current?           │
                                  ─────────────────────────► repartition
                                                                │
                                                           buffer → drain →
                                                           scale → replay
```

**Configuration:**

| Env Var | Default | Description |
|---------|---------|-------------|
| `AUTOSCALE_ENABLED` | `false` | Set `true` to enable lag-based auto-scaling |
| `AUTOSCALE_LAG_HIGH` | `5000` | Scale up when total pending exceeds this |
| `AUTOSCALE_LAG_LOW` | `500` | Scale down when total pending drops below this |
| `AUTOSCALE_MIN` | `1` | Minimum partition count (never scale below) |
| `AUTOSCALE_MAX` | `10` | Maximum partition count (never scale above) |

The check interval is 10 seconds and the cooldown between decisions is 30 seconds.

**Docker example:**

```yaml
# docker-compose.yml
controlplane:
  environment:
    - AUTOSCALE_ENABLED=true
    - AUTOSCALE_LAG_HIGH=5000
    - AUTOSCALE_LAG_LOW=500
    - AUTOSCALE_MIN=1
    - AUTOSCALE_MAX=5
```

**Kubernetes example:**

```yaml
# k8s/controlplane.yaml
env:
  - name: AUTOSCALE_ENABLED
    value: "true"
  - name: AUTOSCALE_LAG_HIGH
    value: "5000"
  - name: AUTOSCALE_LAG_LOW
    value: "500"
  - name: AUTOSCALE_MIN
    value: "1"
  - name: AUTOSCALE_MAX
    value: "10"
```

**Note:** When using auto-scaling in Kubernetes, remove the HPA for the consumer StatefulSet to avoid conflicts — the auto-scaler and HPA would fight over the replica count.

### Data safety features

| Feature | What it does |
|---------|-------------|
| **DiscardNew** | Streams reject new messages when full instead of silently dropping oldest. Publishers get explicit errors. |
| **Dead-letter queue** | Unmarshal failures, max-delivery exhaustion, and replay failures route to `<STREAM>_DLQ` with metadata headers. |
| **Idempotent replay** | Replay uses deterministic MsgIDs (`replay-<key>-<seq>`). Crash mid-replay + resume produces the same IDs — NATS deduplicates. |
| **Extended dedup window** | Primary stream dedup window is 10 minutes (covers repartition duration). |
| **Circuit breaker** | Publisher stops hammering NATS after 5 consecutive failures. Returns `ErrCircuitOpen` so callers can back off. Resets after 10s. |

### Resilience features

| Feature | What it does |
|---------|-------------|
| **Retry with backoff** | SetMode, ScaleConsumers, replay publish — all retry 3x with exponential backoff + jitter. |
| **KV watcher reconnection** | Publisher and reconciler auto-reconnect on watcher disconnect. Resync state from KV on reconnection. |
| **Health probes** | `/health` checks NATS connection + JetStream reachability (returns 503 if unhealthy). `/ready` checks connection + stream registration. |
| **Repartition state machine** | Each step checkpointed to KV. Crash at any step → new leader resumes from last checkpoint. |

### NATS cluster configuration

For production, run NATS as a 3-node cluster with R3 replication:

```
# nats-server.conf (each node)
jetstream {
  store_dir: /data/jetstream
  max_mem: 1G
  max_file: 10G
}

cluster {
  name: nats-cluster
  routes: [
    nats-route://nats-1:6222
    nats-route://nats-2:6222
    nats-route://nats-3:6222
  ]
}
```

Set stream replicas to 3 in your config for data durability.

### Kubernetes deployment

The controlplane supports native Kubernetes via `BACKEND=k8s`. Consumers run as a **StatefulSet** where each pod's ordinal maps directly to its partition ID (`consumer-0` → partition 0, `consumer-3` → partition 3). For multi-stream deployments, the first registered stream uses the base StatefulSet name (e.g., `consumer`), and subsequent streams use `{base}-{streamName}` (e.g., `consumer-payments`). No `client-go` dependency — the K8s API is called via raw REST with in-cluster service account credentials.

#### Architecture

```
  Kubernetes Cluster
  ┌──────────────────────────────────────────────────────────────────┐
  │                                                                  │
  │  Deployment: controlplane (BACKEND=k8s)                         │
  │    ├── patches StatefulSet replicas via K8s REST API            │
  │    ├── runs v2 repartition protocol (checkpointed)             │
  │    └── leader election via NATS KV                             │
  │                                                                  │
  │  StatefulSet: consumer (replicas: N)                            │
  │    ├── consumer-0 → partition 0                                 │
  │    ├── consumer-1 → partition 1                                 │
  │    ├── consumer-2 → partition 2                                 │
  │    └── ...                                                      │
  │                                                                  │
  │  HPA: consumer (autoscaling/v2)                                 │
  │    └── scales StatefulSet based on CPU (70% target)             │
  │                                                                  │
  │  StatefulSet: nats (JetStream enabled)                          │
  │                                                                  │
  │  Deployment: producer (watches KV for mode/partition changes)   │
  └──────────────────────────────────────────────────────────────────┘
```

#### Quick start

Pre-built manifests are in the `k8s/` directory:

```bash
# Apply all manifests
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/nats.yaml
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/consumer.yaml
kubectl apply -f k8s/controlplane.yaml
kubectl apply -f k8s/producer.yaml

# Watch consumers scale
kubectl -n nats-poc get pods -l app=consumer -w

# Scale partitions via API
kubectl -n nats-poc port-forward svc/controlplane 9090:9090
curl -X POST 'http://localhost:9090/scale?stream=MY_ORDERS&partitions=5'
```

#### Build images for K8s

```bash
# Consumer and producer are the same images as Docker mode
docker build --target consumer -t nats-poc-consumer .
docker build --target producer -t nats-poc-producer .

# Use the K8s-specific controlplane target (no docker-cli, smaller image)
docker build --target controlplane-k8s -t nats-poc-controlplane .
```

#### How StatefulSet consumers work

- **Partition discovery**: Each consumer pod derives its partition ID from its hostname ordinal. `consumer-3` → `os.Hostname()` → `OrdinalFromHostname("consumer-3")` → partition 3. No `PARTITION_ID` env var needed.
- **Scaling**: The controlplane patches the StatefulSet's replica count via the K8s Scale subresource API. Kubernetes handles pod lifecycle (creation, termination, restart).
- **Ordering guarantee**: StatefulSet with `podManagementPolicy: Parallel` starts all pods concurrently. Each pod consumes from its own partition subject (`MY_ORDERS.03`), so ordering is maintained per-key.
- **Graceful shutdown**: Consumer pods have `terminationGracePeriodSeconds: 15`. On shutdown, the consumer drains pending messages and deletes its durable NATS consumer.

#### HPA integration

The included HPA scales the consumer StatefulSet based on CPU utilization:

| Parameter | Value |
|-----------|-------|
| Target CPU | 70% average utilization |
| Min replicas | 1 |
| Max replicas | 20 |
| Scale-up | 2 pods per 60s, 30s stabilization |
| Scale-down | 1 pod per 60s, 120s stabilization |

The HPA and the controlplane's repartition protocol are complementary:
- **HPA** reacts to resource pressure (CPU/memory) and adjusts StatefulSet replicas
- **Controlplane** reacts to `desired_partitions` KV changes and runs the buffer-drain-replay protocol to safely remap partitions

For NATS-native scaling triggers (e.g., consumer lag), use KEDA with a NATS JetStream scaler to write `desired_partitions` to the KV bucket.

#### RBAC

The controlplane needs permissions to read and scale the consumer StatefulSet:

```yaml
rules:
  - apiGroups: ["apps"]
    resources: ["statefulsets", "statefulsets/scale"]
    verbs: ["get", "patch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["list"]
```

A ServiceAccount, Role, and RoleBinding are provided in `k8s/rbac.yaml`.

#### HA controlplane in K8s

For HA, deploy 3 controlplane replicas with topology spread:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: controlplane
spec:
  replicas: 3
  template:
    spec:
      serviceAccountName: controlplane
      containers:
      - name: controlplane
        env:
        - name: BACKEND
          value: "k8s"
        - name: INSTANCE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: NATS_URL
          value: "nats://nats:4222"
        - name: K8S_STATEFULSET
          value: "consumer"
        livenessProbe:
          httpGet:
            path: /health
            port: 9090
        readinessProbe:
          httpGet:
            path: /ready
            port: 9090
      topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: DoNotSchedule
```

## Microservices

| Service | Docker Image | Purpose |
|---------|-------------|---------|
| **nats** | `nats:latest` | NATS server with JetStream enabled |
| **producer** | `nats_poc-producer` | Publishes messages — watches KV for mode/partition changes |
| **consumer** | `nats_poc-consumer` | One instance per partition, processes messages with ordering checks |
| **controlplane** | `nats_poc-controlplane` | Manages scaling, coordinates repartition, serves web UI |

The controlplane dynamically creates/destroys consumer instances via Docker or Kubernetes (set `BACKEND=k8s` for Kubernetes).

## Using the Controlplane from Any Language

The controlplane container handles all complexity. Your application only needs to implement a **simple producer** and/or a **simple consumer**. No scaling logic, no repartition protocol, no buffer management.

### Step 1: Register your stream

Call the controlplane HTTP API on startup:

```bash
curl -X POST 'http://controlplane:9090/register?stream=MY_ORDERS&partitions=3'
```

Response:
```json
{
  "status": "ok",
  "stream": "MY_ORDERS",
  "buffer_stream": "MY_ORDERS_BUFFER",
  "kv_bucket": "MY_ORDERS_config",
  "partitions": 3,
  "subject_prefix": "MY_ORDERS",
  "partition_count_key": "partition_count",
  "mode_key": "mode"
}
```

The controlplane creates the streams, KV bucket, and starts consumer containers.

### Step 2: Implement your producer (any language)

The producer only needs to do three things:

1. **Watch KV** for `mode` and `partition_count`
2. **Compute partition**: `partition = hash(key) % partition_count`
3. **Publish** to `MY_ORDERS.<partition>` (direct) or `MY_ORDERS_BUFFER` (buffer)

**Java example:**

```java
// Connect to NATS
Connection nc = Nats.connect("nats://nats:4222");
JetStream js = nc.jetStream();
KeyValue kv = nc.keyValue("MY_ORDERS_config");

// Read initial state
int partitionCount = Integer.parseInt(new String(kv.get("partition_count").getValue()));
String mode = new String(kv.get("mode").getValue()); // "direct" or "buffer"

// Watch for changes (background thread)
KeyValueWatcher watcher = kv.watchAll(entry -> {
    switch (entry.getKey()) {
        case "partition_count":
            partitionCount = Integer.parseInt(new String(entry.getValue()));
            break;
        case "mode":
            mode = new String(entry.getValue());
            break;
    }
});

// Publish loop — this is ALL your producer does
while (running) {
    String subject;
    if (mode.equals("buffer")) {
        subject = "MY_ORDERS_BUFFER";
    } else {
        int partition = partitionForKey(msg.key, partitionCount);
        subject = String.format("MY_ORDERS.%02d", partition);
    }
    js.publish(subject, jsonBytes);
}
```

**Python example:**

```python
import nats

nc = await nats.connect("nats://nats:4222")
js = nc.jetstream()
kv = await js.key_value("MY_ORDERS_config")

# Read initial state
partition_count = int((await kv.get("partition_count")).value)
mode = (await kv.get("mode")).value.decode()  # "direct" or "buffer"

# Watch for changes
async for entry in await kv.watch_all():
    if entry.key == "partition_count":
        partition_count = int(entry.value)
    elif entry.key == "mode":
        mode = entry.value.decode()

# Publish
if mode == "buffer":
    subject = "MY_ORDERS_BUFFER"
else:
    partition = hash(key) % partition_count
    subject = f"MY_ORDERS.{partition:02d}"
await js.publish(subject, data)
```

**Go example (using the built-in library):**

```go
pub, err := autoscale.NewDirectPublisher(ctx, js, "MY_ORDERS")
// That's it — KV watching and mode switching are automatic.
pub.Publish(ctx, autoscale.Message{Key: "acct-42", Sequence: 1, Payload: "data"})
```

### Step 3: Implement your consumer (any language)

The consumer reads two env vars and consumes from one subject:

| Env Var | Example | Purpose |
|---------|---------|---------|
| `PARTITION_ID` | `2` | Which partition this instance handles |
| `STREAM_NAME` | `MY_ORDERS` | Which stream to consume from |
| `SUBJECT_PREFIX` | `MY_ORDERS` | Subject prefix (usually same as stream name) |

**Java example:**

```java
int partitionId = Integer.parseInt(System.getenv("PARTITION_ID"));
String stream = System.getenv("STREAM_NAME");
String prefix = System.getenv("SUBJECT_PREFIX");

String subject = String.format("%s.%02d", prefix, partitionId);
String consumerName = String.format("consumer-p%d", partitionId);

// Create durable consumer
ConsumerConfiguration config = ConsumerConfiguration.builder()
    .durable(consumerName)
    .filterSubject(subject)
    .ackPolicy(AckPolicy.Explicit)
    .build();

// Process messages, ACK each one
js.subscribe(subject, handler, config);
```

**Python example:**

```python
partition_id = int(os.environ["PARTITION_ID"])
stream = os.environ["STREAM_NAME"]
prefix = os.environ.get("SUBJECT_PREFIX", stream)

subject = f"{prefix}.{partition_id:02d}"
consumer_name = f"consumer-p{partition_id}"

sub = await js.pull_subscribe(subject, consumer_name, stream=stream)
while True:
    msgs = await sub.fetch(10)
    for msg in msgs:
        process(msg)
        await msg.ack()
```

### Step 4: Build your consumer Docker image

The controlplane starts your consumer image with `PARTITION_ID`, `STREAM_NAME`, `SUBJECT_PREFIX`, and `NATS_URL` as env vars. Point the controlplane to your image:

```yaml
# docker-compose.yml
controlplane:
  environment:
    - CONSUMER_IMAGE=my-java-consumer:latest
```

Or set it at runtime:

```bash
docker run -e CONSUMER_IMAGE=my-java-consumer:latest ... nats_poc-controlplane
```

### Step 5: Scale via API or UI

```bash
# Scale to 5 partitions — controlplane handles everything
curl -X POST 'http://localhost:9090/scale?stream=MY_ORDERS&partitions=5'
```

The controlplane will:
1. Set KV `mode` → `"buffer"` (all producers auto-switch)
2. Drain consumers
3. Update KV `partition_count`
4. Restart consumers at new count
5. Replay buffered messages to new partition mapping
6. Set KV `mode` → `"direct"` (all producers auto-switch back)
7. Purge buffer

Your producer and consumer code doesn't change at all.

### Partition routing function

The only logic that must match between your producer and the controlplane is the partition hash:

```
partition = extractTrailingNumber(key) % count
```

Where `extractTrailingNumber("acct-42")` returns `42`. For keys without a trailing number, use a simple hash:

```
h = 0
for each character c in key:
    h = h * 31 + c
partition = h % count
```

### Summary: what each component does

| Component | Your app implements | Controlplane handles |
|-----------|-------------------|---------------------|
| **Producer** | Watch KV, publish to subject | Creates streams/KV/DLQ, signals mode changes |
| **Consumer** | Read `PARTITION_ID`, consume, ACK | Starts/stops containers, routes poison msgs to DLQ |
| **Scaling** | Trigger via API or KV write | Full repartition with checkpointing + retry |
| **Buffering** | Publish to buffer subject when KV says so | Idempotent replay, purge after |
| **Ordering** | Use consistent partition hash | Maintains partition→consumer mapping |
| **HA** | Nothing | Leader election, failover, repartition resume |

## Repartition Protocol (v2 — ordered replay)

When the partition count changes, the leader's reconciler orchestrates a protocol with step-level checkpointing. The critical guarantee: **all buffered messages are replayed to partition subjects BEFORE any new direct messages arrive**, ensuring strict per-key ordering.

1. **Set KV mode to "buffer"** — all producers auto-switch via KV watch (retried 3x)
2. **Wait for in-flight messages** — brief pause for direct-published messages to land
3. **Drain consumers** — wait for all partition consumers to finish pending work
4. **Update KV partition count** — write new count to NATS KV store
5. **Restart consumers** — stop all consumers, start fresh at new count (clears per-key tracking state)
6. **Replay buffer** — fetch-based ordered replay of all buffered messages to new partition subjects (idempotent MsgIDs). When nearly caught up, switch to direct mode and drain remaining buffer before producers see the mode change.
7. **Purge buffer** — remove replayed messages from buffer stream

Steps 6-7 are integrated: the direct mode switch happens *inside* the replay loop when the buffer is nearly empty. This eliminates the race condition where new direct messages could arrive before replay completes. Failed replays go to DLQ.

Each step is checkpointed to KV. If the leader crashes mid-repartition, the new leader resumes from the last completed step — no work is lost or repeated.

Messages queue durably in the buffer stream during repartition — no in-memory buffering needed.

## Web Dashboard

The controlplane serves a real-time web UI at **http://localhost:9090** with:

- **Multi-stream tabs** — switch between registered streams; each stream has independent stats and controls
- **Register stream form** — click "+ Register Stream" to add new streams at runtime (name + partition count)
- **Per-stream scale controls** — click to set partition count (1-10) for the selected stream
- **Publish rate control** — adjust producer rate (stop, 1-500 msg/s, custom)
- **Producer mode indicator** — shows Direct (green) or Buffering (amber) during repartition
- **Stream stats** — message counts for partition and buffer streams with progress bars (per selected stream)
- **Consumer cards** — delivered, pending, ack-pending per partition consumer (scoped to selected stream)
- **Per-account sequence tracking** — published vs consumed sequences with mismatch flagging
- **Architecture diagram** — live visualization with dynamic stream names for the selected stream

All stats update via Server-Sent Events (SSE) every 500ms.

**Multi-stream note:** Each stream tab shows data independently for that stream's NATS infrastructure. If a stream shows 0 messages and an empty progress bar, it means **no producer is publishing to that stream**. Each producer instance targets a single stream (configured via the `STREAM_NAME` env var). To see message activity on multiple stream tabs, run a separate producer per stream:

```bash
# Producer for ORDERS stream
docker run -e STREAM_NAME=ORDERS -e NATS_URL=nats://nats:4222 ... nats_poc-producer

# Producer for PAYMENTS stream
docker run -e STREAM_NAME=PAYMENTS -e NATS_URL=nats://nats:4222 ... nats_poc-producer
```

Registering a stream via the UI or API only creates the NATS infrastructure (stream, buffer, KV, consumers) — it does not automatically start publishing to it.

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/register?stream=NAME&partitions=N` | POST | Register a new stream (creates streams, KV, DLQ, consumers) |
| `/scale?stream=NAME&partitions=N` | POST | Scale a specific stream's partition count. `stream` is required when multiple streams are registered; if omitted, defaults to the first registered stream. |
| `/rate?rate=N` | POST | Change producer publish rate (0-1000 msg/s) |
| `/status` | GET | Current system status (JSON). Includes `stream_data` map with per-stream status and `registered` array of stream names. |
| `/events` | GET | SSE stream of real-time status updates (same payload as `/status`) |
| `/health` | GET | Component health (NATS, JetStream, leader). Returns 503 if unhealthy. |
| `/ready` | GET | Readiness probe (connected + stream registered). Returns 503 if not ready. |
| `/leader` | GET | Leader election status (instance_id, is_leader, leader_id) |

### Multi-stream support

The controlplane supports multiple independent streams, each with its own partition count, consumers, reconciler, and auto-scaler. Streams can be registered at startup via `STREAM_NAME` env var or at runtime via `POST /register`:

```bash
# Register multiple streams
curl -X POST 'http://localhost:9090/register?stream=ORDERS&partitions=3'
curl -X POST 'http://localhost:9090/register?stream=PAYMENTS&partitions=2'
curl -X POST 'http://localhost:9090/register?stream=EVENTS&partitions=5'

# Scale a specific stream
curl -X POST 'http://localhost:9090/scale?stream=PAYMENTS&partitions=4'
```

Each stream gets fully independent NATS infrastructure (partition stream, buffer stream, DLQ, KV bucket) and its own set of consumer containers. The dashboard UI provides stream tabs to switch between streams.

## Project Layout

```
cmd/
  producer/            Microservice: message producer (watches KV, publishes)
  consumer/            Microservice: partition consumer (reads PARTITION_ID env)
  controlplane/        Microservice: scaling coordinator + web UI + /register API
    static/              Embedded HTML dashboard
    backend.go           ConsumerRuntime interface (pluggable backend, per-stream partitions)
    backend_k8s.go       Kubernetes backend (per-stream StatefulSet scaling via REST API)
    containers.go        Docker backend (per-stream container management)
    service.go           Service layer (multi-stream Register, Scale, Reconciler lifecycle)
  dashboard/           Standalone dashboard (in-process mode, for testing)
  diag/                Diagnostic tool
autoscale/             Core library
  config.go              NewConfig(streamName) — derives all names from stream name
  message.go             Shared types (Message, ConsumerResult, AccountSeq)
  partition.go           PartitionManager (KV-backed count + mode), key->partition mapping
  publisher.go           DirectPublisher (KV watch, circuit breaker, auto-reconnect)
  consumer.go            Consumer pool (dynamic partition consumers, DLQ integration)
  controlplane.go        In-process control plane (for tests)
  scaler.go              Scaler (v2 ordered repartition protocol with replay-before-direct)
  leader.go              NATS-native leader election (KV + TTL + CAS)
  reconciler.go          Desired-state reconciler (watches KV, triggers repartition)
  autoscaler.go          Lag-based auto-scaler (optional, writes desired_partitions to KV)
  repartition_state.go   Persistent repartition state machine (step checkpointing)
  dlq.go                 Dead-letter queue publisher
  retry.go               Exponential backoff with jitter
  circuitbreaker.go      3-state circuit breaker (closed/open/half-open)
  health.go              Health check interfaces (NATS, JetStream, KV watcher)
tests/                 All tests (external test package)
  helpers_test.go        Shared test utilities
  functional_test.go     Functional/correctness tests (7 tests)
  ha_test.go             HA tests (leader election, failover, reconciler, crash resume)
  perf_test.go           Performance benchmarks + comparison tests
k8s/                   Kubernetes manifests
  namespace.yaml         nats-poc namespace
  nats.yaml              NATS StatefulSet with JetStream
  rbac.yaml              ServiceAccount + Role for controlplane
  controlplane.yaml      Controlplane Deployment (BACKEND=k8s)
  consumer.yaml          Consumer StatefulSet + HPA
  producer.yaml          Producer Deployment
scripts/
  down.sh              Clean shutdown helper (removes orphaned containers)
Dockerfile             Multi-stage build (producer, consumer, controlplane, controlplane-k8s)
docker-compose.yml     Service definitions
```

## Prerequisites

- Go 1.24+
- Docker & Docker Compose (for Docker backend)
- Kubernetes cluster with `kubectl` (for K8s backend)

## Quick Start

### 1. Start all services

```bash
docker compose up -d --build
```

This starts NATS, the controlplane, and the producer. The producer registers its stream (`MY_ORDERS`) with the controlplane, which creates the NATS infrastructure and starts consumer containers. Ports:
- `4222` — NATS client connections
- `8222` — NATS HTTP monitoring
- `9090` — Controlplane web UI + API

### 2. Open the dashboard

Open **http://localhost:9090** in your browser. You'll see messages flowing through the system with 1 partition at 10 msg/s by default.

### 3. Scale partitions

Use the UI buttons or the API:

```bash
# Scale to 3 partitions (specify stream when multiple are registered)
curl -X POST 'http://localhost:9090/scale?stream=MY_ORDERS&partitions=3'

# Register an additional stream
curl -X POST 'http://localhost:9090/register?stream=PAYMENTS&partitions=2'

# Change publish rate to 100 msg/s
curl -X POST 'http://localhost:9090/rate?rate=100'

# Get current status (includes per-stream data under "stream_data")
curl http://localhost:9090/status
```

The dashboard shows stream tabs — click between registered streams to view their independent stats and controls.

### 4. Shut down

```bash
./scripts/down.sh -v
```

This removes dynamically-created consumer containers first, then runs `docker compose down`.

## Running Tests

Tests use the in-process control plane (2-hop architecture via the `autoscale` library) and require a running NATS server:

```bash
docker compose up -d nats
```

**Functional tests** (ordering, scale up/down, lifecycle, concurrent publishing):

```bash
go test -v -race -count=1 -timeout 120s ./tests/
```

**Performance tests** (throughput, latency, burst, 3-way comparison):

```bash
go test -v -run "TestPerf" -timeout 300s ./tests/
```

**Go benchmarks** (microbenchmarks for publish paths):

```bash
go test -bench=. -benchtime=3s ./tests/
```

## Performance Results

Comparison of three approaches on Apple M4 Max, local NATS server:

### Throughput (3,000 messages)

| Approach | Publish Rate | End-to-End Rate | Avg Pub Latency |
|---|---|---|---|
| Raw JetStream (1 hop, no partitioning) | 5,081 msg/s | 5,081 msg/s | 197us |
| **Direct-Publish Partitioned (1 hop, 3 partitions)** | **5,238 msg/s** | **5,139 msg/s** | **190us** |
| Decoupled Pipeline (2 hops, router) | 3,400 msg/s | 3,214 msg/s | 294us |

### Per-Message E2E Latency

| Approach | P50 | P95 | P99 |
|---|---|---|---|
| Raw JetStream | 200us | 251us | 278us |
| **Direct-Publish Partitioned** | **194us** | **255us** | **339us** |
| Decoupled Pipeline | 394us | 482us | 546us |

**Key takeaway:** Direct-publish partitioned performs on par with raw JetStream (~0% overhead) while providing dynamic partitioning, per-key ordering, and zero-downtime scaling.

## Configuration

### Environment Variables

**Producer:**

| Variable | Default | Description |
|----------|---------|-------------|
| `NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `STREAM_NAME` | `AUTO_ORDERS` | Stream name to register with controlplane |
| `CONTROLPLANE_URL` | `http://controlplane:9090` | Controlplane HTTP URL for registration |
| `PUBLISH_RATE` | `10` | Messages per second |
| `NUM_KEYS` | `5` | Number of account keys to cycle through |
| `INITIAL_PARTITIONS` | `1` | Starting partition count |

**Consumer:**

| Variable | Default | Description |
|----------|---------|-------------|
| `NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `PARTITION_ID` | *(auto from hostname)* | Partition this consumer handles. If unset, derived from StatefulSet hostname ordinal (e.g. `consumer-3` → 3). |
| `STREAM_NAME` | `AUTO_ORDERS` | Stream to consume from |
| `SUBJECT_PREFIX` | same as `STREAM_NAME` | Subject prefix for partition subjects |
| `HEALTH_PORT` | `8080` | Health endpoint port |

**Controlplane:**

| Variable | Default | Description |
|----------|---------|-------------|
| `NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `API_PORT` | `9090` | Web UI and API port |
| `BACKEND` | `docker` | Consumer runtime backend: `docker` or `k8s` |
| `DOCKER_NETWORK` | `nats_poc_default` | (Docker) Docker network for consumer containers |
| `CONSUMER_IMAGE` | `nats_poc-consumer` | (Docker) Docker image for consumer containers |
| `INTERNAL_NATS_URL` | `nats://nats:4222` | (Docker) NATS URL used by consumer containers |
| `K8S_NAMESPACE` | *(auto from pod)* | (K8s) Kubernetes namespace for the consumer StatefulSet |
| `K8S_STATEFULSET` | `consumer` | (K8s) Name of the consumer StatefulSet to scale |
| `STREAM_NAME` | *(none)* | Optional: auto-register a stream on startup |
| `INITIAL_PARTITIONS` | `1` | Starting partition count (used with STREAM_NAME) |
| `INSTANCE_ID` | hostname | Unique ID for leader election (use `metadata.name` in K8s) |
| `LEADER_BUCKET` | `controlplane_leader` | NATS KV bucket name for leader election |
| `AUTOSCALE_ENABLED` | `false` | Enable lag-based auto-scaling (`true` / `false`) |
| `AUTOSCALE_LAG_HIGH` | `5000` | Scale up when total pending > this |
| `AUTOSCALE_LAG_LOW` | `500` | Scale down when total pending < this |
| `AUTOSCALE_MIN` | `1` | Minimum partition count |
| `AUTOSCALE_MAX` | `10` | Maximum partition count |

## NATS CLI

```bash
brew install nats-io/nats-tools/nats

# Stream info (replace MY_ORDERS with your stream name)
nats stream info MY_ORDERS
nats stream info MY_ORDERS_BUFFER

# Consumer details
nats consumer ls MY_ORDERS
nats consumer info MY_ORDERS consumer-p0

# Watch live messages
nats sub "MY_ORDERS.>"

# KV bucket
nats kv get MY_ORDERS_config partition_count
nats kv get MY_ORDERS_config mode

# Stream storage report
nats stream report
```
