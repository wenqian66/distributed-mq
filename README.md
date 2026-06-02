# Distributed Message Queue

## Problem

Traditional message brokers treat all messages equally, causing high-priority work to queue behind bulk operations during traffic spikes. This system implements a distributed message queue with priority-based routing, automatic retry with dead-letter handling, and horizontal autoscaling — targeting workloads where ordering guarantees, fault tolerance, and sub-second consumer lag matter at scale.

## Architecture

```
                              ┌─────────────────┐
                              │   OrderProducer  │
                              │  (messages.sent  │
                              │   counter/prio)  │
                              └───────┬──────────┘
                                      │ routes by Order.Priority
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                  ▼
             ┌────────────┐   ┌────────────┐   ┌────────────┐
             │ orders.high │   │orders.normal│   │ orders.low │
             └─────┬──────┘   └─────┬──────┘   └─────┬──────┘
                   └─────────────────┼─────────────────┘
                                     ▼
                          ┌─────────────────────┐
                          │    OrderConsumer     │
                          │  @RetryableTopic     │
                          │  attempts=4          │
                          │  10s → 30s → 90s     │
                          │  (messages.processed │
                          │   messages.failed)   │
                          └──┬──────────┬────────┘
                   success   │          │  exhausted retries
                             ▼          ▼
              ┌──────────────────┐   ┌──────────────────┐
              │ cacheService.save│   │   @DltHandler     │
              │ (PG + invalidate │   │  orders.high-dlt  │
              │  Redis cache)    │   │  orders.normal-dlt│
              └──┬───────────┬──┘   │  orders.low-dlt   │
                 ▼           │      └────────┬─────────-┘
          ┌───────────┐      │               │ marks FAILED in PG
          │ PostgreSQL │      │               │ POST /replay to resend
          │ message_   │◄────┘               │
          │ state      │◄────────────────────┘
          └─────┬─────┘
                │
   GET /admin/messages/{orderId}
                │
                ▼
          ┌───────────┐  hit    ┌────────────────┐
          │   Redis    │───────►│ return cached   │
          │  msg:{id}  │        └────────────────┘
          └─────┬─────┘
                │ miss
                ▼
          ┌───────────┐         ┌────────────────┐
          │ PostgreSQL │────────►│ populate Redis  │
          │   query    │        │ + return        │
          └───────────┘         └────────────────┘

  ┌─────────────────── Observability ───────────────────┐
  │                                                     │
  │  Prometheus ◄── scrapes ── mq-service:8081          │
  │      │                     /actuator/prometheus      │
  │      │         ◄── scrapes ── kafka-exporter:9308   │
  │      │                        (consumer lag)        │
  │      ▼                                              │
  │  Grafana (auto-provisioned dashboard)               │
  │    ├── Message Throughput (messages_sent by prio)   │
  │    ├── Consumer Latency (fetch avg + lookup timer)  │
  │    ├── Consumer Group Lag (kafka_consumergroup_lag)  │
  │    └── Cache Hit Rate (cache.hit / total)           │
  └─────────────────────────────────────────────────────┘

  ┌─────────────────── Kubernetes ──────────────────────┐
  │                                                     │
  │  Deployment (mq-service, 2–20 replicas)             │
  │      ▲                                              │
  │      │ manages                                      │
  │  KEDA ScaledObject                                  │
  │    ├── orders.high  lag > 100 → scale up            │
  │    └── orders.normal lag > 500 → scale up           │
  │                                                     │
  └─────────────────────────────────────────────────────┘
```

## Tech Stack

| Technology | Role | Key Design Decision |
|---|---|---|
| Java 17 / Spring Boot 3.2 | Application framework | `@RetryableTopic` for non-blocking retry; `@Transactional` consumer with manual offset commits (`AckMode.RECORD`) |
| Apache Kafka 3.6 (KRaft) | Message broker | Three priority topics instead of one; `acks=all` + idempotence enabled; lz4 compression with 5ms linger for batching |
| PostgreSQL 16 | Message metadata store | Tracks `orderId`, `status` (PENDING/COMPLETED/FAILED), `attempts`, `dltTopic`; indexed on `orderId` (unique) and `status` |
| Redis 7 | Cache-aside layer | Write-through invalidation (DEL, not SET) avoids stale-cache race; 600s TTL; Micrometer `cache.hit`/`cache.miss` counters |
| KEDA | Autoscaler | Scales on Kafka consumer lag (not CPU) — consumers are I/O-bound, CPU never spikes even under heavy lag |
| Kubernetes | Orchestration | Deployment with readiness/liveness probes on Actuator endpoints; ConfigMap + Secret for env vars |
| Prometheus | Metrics collection | Scrapes `/actuator/prometheus` (Micrometer) and `kafka-exporter:9308` (broker-level consumer lag) |
| Grafana 10.4 | Dashboards | Auto-provisioned on startup with 4 panels: throughput, latency, consumer lag, cache hit rate |
| Docker Compose | Local development | Full stack in one command: Kafka, PostgreSQL, Redis, 2x mq-service replicas, Prometheus, Grafana |

## Key Design Decisions

**1. Three Kafka topics for priority instead of a single topic with a priority field.**
Kafka has no concept of message priority within a partition — consumers read sequentially from a committed offset. Routing `HIGH`, `NORMAL`, and `LOW` to `orders.high`, `orders.normal`, and `orders.low` gives true isolation: a million low-priority messages piling up never delay a high-priority order. KEDA reinforces this by applying different lag thresholds per topic (100 for high, 500 for normal), so the cluster scales aggressively for high-priority backlog while tolerating some normal-priority queueing.

**2. `@RetryableTopic` with exponential backoff (10s, 30s, 90s) and automatic DLQ routing.**
A failed message is republished to `orders.high-retry-0`, then `-retry-1`, then `-retry-2`, each with increasing delay headers. The partition's read offset advances immediately so other messages keep flowing — no `Thread.sleep` blocking the consumer thread. After 4 total attempts (1 initial + 3 retries), the message lands in `orders.high-dlt` and the `@DltHandler` marks it `FAILED` in PostgreSQL. Operators inspect via `GET /admin/messages/failed` and replay via `POST /admin/messages/{orderId}/replay`. `IllegalArgumentException` is excluded from retries since validation errors will never succeed.

**3. Redis cache-aside on read with write-through invalidation on the consume path.**
On reads (`GET /admin/messages/{orderId}`), the service checks Redis first, falls through to PostgreSQL on miss, and populates the cache with a 600s TTL. On writes, `cacheService.save()` persists to PostgreSQL then DELs the Redis key — never SETs it — because a SET races with concurrent writers (thread A could overwrite a newer value from thread B). DEL is safe: the next reader rediscovers the current value from PostgreSQL. `cache.hit` and `cache.miss` Micrometer counters and the `@Timed("message.lookup.latency")` timer on the read endpoint make cache effectiveness measurable at runtime without fabricating a static number.

**4. KEDA scaling on consumer lag instead of CPU-based HPA.**
Kafka consumers spend most of their time blocked on `poll()` — CPU stays under 50% even when lag is massive. A CPU-based HPA would never trigger, and lag would silently pile up. KEDA's native Kafka trigger talks directly to the broker, reads the consumer group offset, and adds pods proportional to actual backlog. The ScaledObject defines min 2 / max 20 replicas, polls every 15 seconds, and cools down after 120 seconds of no triggers.

## How to Run

### Docker Compose (local)

```bash
docker compose up --build -d
```

| Service | URL |
|---|---|
| MQ Service API | http://localhost:8081 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin / admin) |

Publish a test message:

```bash
curl -X POST http://localhost:8081/admin/messages \
  -H "Content-Type: application/json" \
  -d '{"orderId":"order-1","customerId":"cust-1","amount":99.99,"priority":"HIGH"}'
```

Check message state:

```bash
curl http://localhost:8081/admin/messages/order-1
```

List failed messages:

```bash
curl http://localhost:8081/admin/messages/failed
```

Replay a failed message:

```bash
curl -X POST http://localhost:8081/admin/messages/order-1/replay
```

### Kubernetes

```bash
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/keda-scaler.yaml
```

Requires KEDA installed in the cluster:

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm install keda kedacore/keda --namespace keda --create-namespace
```

### Environment Variables

| Variable | Description | Example |
|---|---|---|
| `KAFKA_BOOTSTRAP` | Kafka broker address | `kafka:9092` |
| `PG_HOST` | PostgreSQL hostname | `postgres` |
| `PG_PORT` | PostgreSQL port | `5432` |
| `PG_DB` | PostgreSQL database name | `mq` |
| `PG_USER` | PostgreSQL username | `mq` |
| `PG_PASSWORD` | PostgreSQL password | `mq` |
| `REDIS_HOST` | Redis hostname | `redis` |
| `REDIS_PORT` | Redis port | `6379` |
