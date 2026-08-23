# ModelGate

[![CI](https://github.com/theHerta27/ModelGate/actions/workflows/ci.yml/badge.svg)](https://github.com/theHerta27/ModelGate/actions/workflows/ci.yml)

> A learning-driven LLM and Agent Gateway written in Go.

## Overview

ModelGate is a portfolio backend project that builds the infrastructure behind LLM applications one capability at a time. Each version stays runnable, testable, and deliberately small enough to explain in an interview.

## Current Status

**Version 4 — reproducible environment, real-dependency integration tests, benchmarks, and CI complete**

V4 packages the V1-V3 gateway into a reproducible Docker Compose environment and adds independent CI evidence for unit tests, the race detector, real Redis/PostgreSQL behavior, Docker build, and a five-service smoke test. The deterministic Mock Provider remains the default, and no real LLM key is required by local startup or CI.

## Features

- `GET /health` and `POST /v1/chat/completions`
- Mock, DeepSeek, and generic OpenAI-compatible providers behind one interface
- Incremental SSE forwarding, cancellation propagation, flushing, and deterministic cleanup
- Round Robin and smooth Weighted Round Robin routing across statically configured providers
- Per-attempt timeout, classified retries with exponential backoff/full jitter, and an overall request deadline
- Concurrency-safe CLOSED/OPEN/HALF_OPEN circuit breakers and per-provider channel semaphores
- Redis Lua token-bucket rate limiting by requests per minute
- Non-streaming `Idempotency-Key` conflict detection and completed-response replay
- Client-scoped response caching for explicitly deterministic requests
- PostgreSQL migrations and transactional request/usage persistence with pgx
- OpenAI-style errors, bounded timeouts, and graceful server shutdown
- Prometheus metrics at `GET /metrics`, JSON logs through `slog`, and an importable Grafana dashboard
- Multi-stage, non-root Docker image and a Compose stack for ModelGate, PostgreSQL, Redis, Prometheus, and Grafana
- Real-service integration tests, serial/parallel microbenchmarks, and GitHub Actions quality gates
- Unit, HTTP, integration, and CI tests that require no real LLM credentials

Intentionally outside V4: API-key authentication, distributed tracing, dynamic provider discovery, Kubernetes deployment, and optional message-queue decoupling.

## Architecture

```text
Client
  -> Gin Middleware          request ID, Prometheus HTTP metrics, JSON access log
  -> Gin Handler             HTTP/SSE parsing and response mapping
  -> Gateway Service         overall deadline, V2 policies, usage metering
       |-- Redis Limiter     atomic Lua token bucket
       |-- Idempotency       pending/completed state machine
       |-- Response Cache    explicit deterministic-request policy
       |-- Chat Service      request validation
       |     `-- Router      Round Robin / smooth Weighted Round Robin
       |           `-- Governed Provider (one wrapper per target)
       |                 |-- channel semaphore
       |                 |-- circuit breaker
       |                 |-- retry + per-attempt deadline
       |                 `-- Mock / DeepSeek / OpenAI-compatible adapter
       `-- Usage Recorder    pgx transaction + explicit SQL
```

Redis and PostgreSQL are optional adapters in direct Go startup. The Compose profile enables both, waits for their health checks, then starts ModelGate. Prometheus scrapes ModelGate over the private bridge network and Grafana reads the provisioned Prometheus datasource.

## Technology Stack

- Go 1.26, Gin, and the standard `net/http` stack
- Redis with go-redis and Lua scripts
- PostgreSQL with pgxpool and embedded SQL migrations
- Prometheus Go client, standard-library `slog` JSON logging, and a Grafana dashboard JSON
- Docker Compose, Prometheus 3.12, Grafana 13.1, PostgreSQL 18.6, and Redis 8.10
- Mock Provider, `httptest`, real service containers, and GitHub Actions

## Quick Start

### Minimal gateway

Requirement: Go 1.26.4 or later.

```bash
go run ./cmd/server
```

The default configuration listens on `:8080` and uses the Mock Provider.

```bash
curl http://localhost:8080/health

curl http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "mock-model",
    "messages": [{"role": "user", "content": "Hello, ModelGate"}]
  }'

curl -N http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "mock-model",
    "messages": [{"role": "user", "content": "Stream this response"}],
    "stream": true
  }'
```

Copy `.env.example` to `.env` as a reference, then export the values through your shell or runtime. ModelGate does not load `.env` files automatically.

### Complete stack

Requirement: Docker Engine or Docker Desktop with the Compose plugin.

```bash
docker compose up -d --build
docker compose ps
```

The one command starts:

- ModelGate: http://127.0.0.1:8080
- Prometheus: http://127.0.0.1:9090
- Grafana: http://127.0.0.1:3000
- PostgreSQL: `127.0.0.1:5432`
- Redis: `127.0.0.1:6379`

Compose uses clearly marked `modelgate_local_only` fallback passwords so a fresh clone can start without a secret file. They are local-development credentials, bind only to loopback host ports, and must be replaced for any shared environment.

```bash
docker compose logs -f modelgate
docker compose down
```

Named volumes preserve database, Redis, Prometheus, and Grafana data across a normal `down`. Use `docker compose down --volumes` only when you explicitly want to delete that local stack data.

## Configuration

### Gateway and providers

| Variable | Default | Purpose |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Gateway listen address |
| `MODEL_PROVIDER` | `mock` | Backward-compatible single provider: `mock`, `deepseek`, or `openai-compatible` |
| `MODEL_PROVIDERS` | empty | Optional `name:type:weight` list; when set, replaces the single-provider target (example: `primary:mock:2,backup:mock:1`) |
| `ROUTING_STRATEGY` | `round_robin` | `round_robin` or smooth `weighted_round_robin` |
| `REQUEST_TIMEOUT` | `30s` | Overall logical request/stream deadline and HTTP client upper bound |
| `DEEPSEEK_BASE_URL` | `https://api.deepseek.com` | DeepSeek API base URL |
| `DEEPSEEK_API_KEY` | empty | Required only for DeepSeek |
| `OPENAI_COMPATIBLE_BASE_URL` | `https://api.openai.com/v1` | Compatible API base URL |
| `OPENAI_API_KEY` | empty | Required only for the compatible provider |

### V3 provider governance

| Variable | Default | Purpose |
|---|---|---|
| `UPSTREAM_ATTEMPT_TIMEOUT` | `10s` | Deadline for one upstream attempt; cannot exceed `REQUEST_TIMEOUT` |
| `UPSTREAM_MAX_ATTEMPTS` | `3` | Total attempts including the first call; capped at 10 |
| `RETRY_BASE_BACKOFF` | `100ms` | First exponential-backoff ceiling before full jitter |
| `RETRY_MAX_BACKOFF` | `2s` | Maximum backoff ceiling |
| `CIRCUIT_BREAKER_WINDOW_SIZE` | `20` | Fixed sliding result window per provider |
| `CIRCUIT_BREAKER_MINIMUM_REQUESTS` | `10` | Minimum observations before the breaker may open |
| `CIRCUIT_BREAKER_FAILURE_RATIO` | `0.5` | Failure ratio in `(0, 1]` that opens the breaker |
| `CIRCUIT_BREAKER_OPEN_TIMEOUT` | `30s` | Cooldown before entering HALF_OPEN |
| `CIRCUIT_BREAKER_HALF_OPEN_MAX_REQUESTS` | `1` | Concurrent recovery probes |
| `PROVIDER_MAX_CONCURRENCY` | `50` | In-flight logical calls/streams allowed for each provider |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error` |

### Redis policies

| Variable | Default | Purpose |
|---|---|---|
| `REDIS_ENABLED` | `false` | Enable rate limit, idempotency, and response cache |
| `REDIS_ADDR` | `127.0.0.1:6379` | Redis address |
| `REDIS_PASSWORD` | empty | Redis password |
| `REDIS_DB` | `0` | Redis database number |
| `REDIS_TIMEOUT` | `2s` | Redis dial/read/write timeout |
| `RATE_LIMIT_REQUESTS_PER_MINUTE` | `60` | Token refill rate |
| `RATE_LIMIT_BURST` | same as RPM | Token-bucket capacity |
| `RATE_LIMIT_FAIL_OPEN` | `true` | Continue if the runtime limiter is unavailable |
| `IDEMPOTENCY_TTL` | `24h` | Completed response lifetime |
| `IDEMPOTENCY_LOCK_TTL` | `30s` | In-progress reservation lifetime |
| `CACHE_TTL` | `5m` | Cached response lifetime |

### PostgreSQL

| Variable | Default | Purpose |
|---|---|---|
| `POSTGRES_ENABLED` | `false` | Enable migrations and usage persistence |
| `POSTGRES_DSN` | empty | Full DSN; takes precedence over fields below |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `127.0.0.1` / `5432` | PostgreSQL endpoint |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | empty | Database credentials |
| `POSTGRES_DB` | empty | Database name |
| `POSTGRES_SSLMODE` | `disable` | pgx SSL mode when a DSN is assembled |
| `POSTGRES_MAX_CONNS` | `10` | Pool limit, capped at 100 |
| `STORAGE_TIMEOUT` | `2s` | Individual persistence-operation timeout |

## API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Liveness response: `{"status":"ok"}` |
| `GET` | `/metrics` | Prometheus exposition endpoint |
| `POST` | `/v1/chat/completions` | OpenAI-style non-streaming JSON or SSE chat completion |

Chat requests support `model`, `messages`, `temperature`, `top_p`, `max_tokens`, and `stream`. Non-streaming requests may send an `Idempotency-Key`. Streaming responses use `text/event-stream`, emit incremental JSON chunks, and terminate with `data: [DONE]`.

Useful response headers include `X-Request-ID`, `X-ModelGate-Provider`, `X-ModelGate-Cache`, `Idempotency-Replayed`, `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `Retry-After`. Errors use an OpenAI-style `error.message/type/code` envelope.

## Design Decisions

- One deployable process owns HTTP, policy orchestration, and provider selection; package boundaries keep infrastructure replaceable without premature microservices.
- Providers implement one small interface. CI and Compose use the deterministic Mock Provider, so no DeepSeek/OpenAI secret is needed.
- Redis and PostgreSQL are optional in direct startup but mandatory in the full Compose profile, making dependency ownership explicit.
- ModelGate does not fail over after a real upstream call has failed because the provider may already have charged tokens; reselection is limited to breaker availability races.
- Prompts, API keys, raw client IPs, and request IDs are excluded from metric labels and persistence payloads.

### V2 data policies

| Concern | Policy | Failure behavior |
|---|---|---|
| Rate limit | Lua token bucket scoped to the direct TCP client IP; the Redis key stores only its hash | Runtime Redis errors follow `RATE_LIMIT_FAIL_OPEN` |
| Idempotency | Non-streaming only; same key and payload replays the completed response, while a changed payload conflicts | Fail closed with `503` if state cannot be determined |
| Response cache | Non-streaming success only, with `temperature` explicitly set to `0`; keys are client-scoped request hashes | Read errors act as a miss; write errors do not replace a successful model response |
| Usage persistence | Provider, request metadata, latency, status, and token counts are written in one transaction | Best effort: database errors are logged and do not replace a successful model response |

ModelGate never writes the full prompt to PostgreSQL. Cache and idempotency Redis keys contain SHA-256 digests instead of the prompt or raw client IP.

Idempotency conflicts/in-progress requests return `409`; an unavailable required idempotency backend returns `503`; rate-limit rejection returns `429`.

## Reliability

- A retry is allowed only for HTTP `429`, HTTP `503`, deadlines, and explicitly classified transport failures. Client errors such as `400`, `401`, and `403`, malformed responses, and cancellations are not retried.
- Backoff uses exponential ceilings plus full jitter, and waiting stops immediately when the parent context is canceled.
- A provider breaker records final logical-call outcomes in a fixed sliding window. OPEN providers are skipped; after the cooldown, a bounded HALF_OPEN probe either closes the breaker or opens it again.
- The channel semaphore limits concurrent logical calls, not requests per minute. Streaming calls hold their permit, breaker probe, and contexts until EOF, error, or `Close`.
- Routing is deterministic. Round Robin distributes evenly; smooth Weighted Round Robin follows static weights. ModelGate does not call a second provider after a real upstream failure because that could duplicate token spend; it only reselects when an apparently healthy breaker loses an `Allow` race.
- Every request gets a server-generated `X-Request-ID`; incoming client values are replaced. Logs include bounded fields such as request ID, route, provider, model, status, and latency, but never prompts, API keys, or client IP labels.

## Observability

Prometheus scrapes `GET /metrics`. ModelGate exports:

- `modelgate_requests_total` and `modelgate_request_duration_seconds`
- `modelgate_tokens_total` and `modelgate_provider_errors_total`
- `modelgate_rate_limited_total` and `modelgate_cache_hits_total`
- `modelgate_upstream_requests_total` and `modelgate_circuit_breaker_state`

Labels are deliberately low-cardinality; request IDs and client identities are excluded. Compose automatically provisions the Prometheus datasource with UID `prometheus` and loads the [ModelGate dashboard](deploy/grafana/modelgate-dashboard.json) into the `ModelGate` folder. The CI Compose smoke waits until Prometheus reports the gateway target up and Grafana returns the provisioned dashboard by UID.

## PostgreSQL Schema

Embedded migrations create:

- `api_keys` for future hashed-key authentication metadata
- `providers` for referenced provider identities
- `requests` for request outcome, latency, cache status, and error code
- `usage_records` for input/output/total tokens and future estimated cost

`schema_migrations` and a PostgreSQL advisory transaction lock make startup migrations versioned and serialized. Application writes use explicit SQL and commit/rollback boundaries.

## Testing

```bash
# Deterministic local quality gates
go test ./...
go vet ./...
go build ./cmd/server
go test -race ./...

# Real Redis, PostgreSQL, migrations, Lua policies, and Gateway HTTP composition
docker compose --profile test run --rm --build integration
```

Unit tests use fakes, the Mock Provider, and local `httptest` upstreams. Build-tagged integration tests use real Redis and PostgreSQL, execute the actual Lua scripts and embedded migrations, verify transactional usage records, then exercise idempotency replay and cache hit behavior through the HTTP router. They create unique short-lived keys and delete only their own PostgreSQL request rows.

GitHub Actions has three independent jobs:

- quality: formatting, `go vet`, unit tests, race detector, and server build
- integration: Redis/PostgreSQL service containers plus `-tags=integration`
- container: Compose validation, image build, five-service startup, Gateway request, Prometheus target, and Grafana provisioning smoke

All jobs use the Mock Provider and ephemeral CI database credentials. No LLM token is stored or requested.

## Benchmark

V4 includes serial and `RunParallel` microbenchmarks for the Router, provider concurrency limiter, and Gateway + Mock Provider path. On the documented 16-worker workstation, five-run medians were:

| Case | 1 worker | 16 workers | Allocations |
|---|---:|---:|---:|
| Router / Round Robin | 64.09 ns/op | 133.6 ns/op | 1 alloc/op |
| Router / Weighted Round Robin | 83.69 ns/op | 135.3 ns/op | 1 alloc/op |
| Provider concurrency limiter | 76.98 ns/op | 115.2 ns/op | 1 alloc/op |
| Gateway + Mock Provider | 1,747 ns/op | 617.0 ns/op | 12 allocs/op |

See the [complete V4 benchmark methodology and environment](docs/benchmarks/v4.md). These are in-process microbenchmarks without network or model latency; they are regression baselines, not production QPS claims.

## Roadmap

- [x] **V0:** Repository initialization and Go foundations
- [x] **V1:** Minimal OpenAI-compatible gateway, provider abstraction, Mock Provider, and non-streaming completions
- [x] **V1.5:** SSE streaming, cancellation, and resource cleanup
- [x] **V2:** Redis rate limiting, idempotency, response-cache policy, and PostgreSQL usage persistence
- [x] **V3:** Timeouts, retries, circuit breaker, provider routing, concurrency control, and observability
- [x] **V4:** Docker Compose environment, real integration tests, benchmarks, and CI
- [ ] **V5 (optional):** Asynchronous usage events with a message queue when justified by measured requirements

## Development Principles

- Build one gateway service before considering service decomposition.
- Make infrastructure policy explicit, deterministic, and testable.
- Use mock providers for tests and CI; never require a real LLM key.
- Introduce dependencies only when the current version needs them.
- Publish performance results only with reproducible methodology.
- Never commit secrets, real environment files, full prompts, or private user data.

## License

ModelGate is licensed under the MIT License.
