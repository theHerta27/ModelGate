# ModelGate

> A learning-driven LLM and Agent Gateway written in Go.

ModelGate is a portfolio backend project that builds the infrastructure behind LLM applications one capability at a time. Each version stays runnable, testable, and deliberately small enough to explain in an interview.

## Current Status

**Version 2 — Redis policies and PostgreSQL usage persistence complete**

V2 supports streaming and non-streaming OpenAI-compatible chat completions. The deterministic Mock Provider and all external infrastructure are disabled by default, so the gateway still starts without credentials, Redis, or PostgreSQL.

## Features

- `GET /health` and `POST /v1/chat/completions`
- Mock, DeepSeek, and generic OpenAI-compatible providers behind one interface
- Incremental SSE forwarding, cancellation propagation, flushing, and deterministic cleanup
- Redis Lua token-bucket rate limiting by requests per minute
- Non-streaming `Idempotency-Key` conflict detection and completed-response replay
- Client-scoped response caching for explicitly deterministic requests
- PostgreSQL migrations and transactional request/usage persistence with pgx
- OpenAI-style errors, bounded timeouts, and graceful server shutdown
- Unit and HTTP tests that require no real LLM credentials

Not implemented yet: API-key authentication, retries, circuit breaking, provider routing, concurrency control, Prometheus metrics, or distributed tracing.

## Architecture

```text
Client
  -> Gin Handler             HTTP/SSE parsing and response mapping
  -> Gateway Service         policy orchestration and usage metering
       |-- Redis Limiter     atomic Lua token bucket
       |-- Idempotency       pending/completed state machine
       |-- Response Cache    explicit deterministic-request policy
       |-- Chat Service      request validation
       |     `-- Provider    Mock / DeepSeek / OpenAI-compatible
       `-- Usage Recorder    pgx transaction + explicit SQL
```

Redis and PostgreSQL are optional adapters. Setting either `*_ENABLED=true` makes ModelGate verify that dependency at startup; PostgreSQL migrations then run automatically.

## Technology Stack

- Go 1.26, Gin, and the standard `net/http` stack
- Redis with go-redis and Lua scripts
- PostgreSQL with pgxpool and embedded SQL migrations
- Mock Provider and `httptest` for deterministic tests

Docker Compose, Prometheus, Grafana, benchmarks, and CI remain later roadmap work.

## Quick Start

Requirements: Go 1.26.4 or later.

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

## Configuration

### Gateway and providers

| Variable | Default | Purpose |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Gateway listen address |
| `MODEL_PROVIDER` | `mock` | `mock`, `deepseek`, or `openai-compatible` |
| `REQUEST_TIMEOUT` | `30s` | Upstream request and startup-operation timeout |
| `DEEPSEEK_BASE_URL` | `https://api.deepseek.com` | DeepSeek API base URL |
| `DEEPSEEK_API_KEY` | empty | Required only for DeepSeek |
| `OPENAI_COMPATIBLE_BASE_URL` | `https://api.openai.com/v1` | Compatible API base URL |
| `OPENAI_API_KEY` | empty | Required only for the compatible provider |

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

## V2 Policies

| Concern | Policy | Failure behavior |
|---|---|---|
| Rate limit | Lua token bucket scoped to the direct TCP client IP; the Redis key stores only its hash | Runtime Redis errors follow `RATE_LIMIT_FAIL_OPEN` |
| Idempotency | Non-streaming only; same key and payload replays the completed response, while a changed payload conflicts | Fail closed with `503` if state cannot be determined |
| Response cache | Non-streaming success only, with `temperature` explicitly set to `0`; keys are client-scoped request hashes | Read errors act as a miss; write errors do not replace a successful model response |
| Usage persistence | Provider, request metadata, latency, status, and token counts are written in one transaction | Best effort: database errors are logged and do not replace a successful model response |

ModelGate never writes the full prompt to PostgreSQL. Cache and idempotency Redis keys contain SHA-256 digests instead of the prompt or raw client IP.

Useful response headers include `X-Request-ID`, `X-ModelGate-Cache`, `Idempotency-Replayed`, `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `Retry-After`. Idempotency conflicts/in-progress requests return `409`; an unavailable required idempotency backend returns `503`; rate-limit rejection returns `429`.

## PostgreSQL Schema

Embedded migrations create:

- `api_keys` for future hashed-key authentication metadata
- `providers` for referenced provider identities
- `requests` for request outcome, latency, cache status, and error code
- `usage_records` for input/output/total tokens and future estimated cost

`schema_migrations` and a PostgreSQL advisory transaction lock make startup migrations versioned and serialized. Application writes use explicit SQL and commit/rollback boundaries.

## Testing

```bash
go test ./...
go vet ./...
go build ./cmd/server
```

Tests use fakes, the Mock Provider, or local `httptest` upstreams. They cover policy decisions, Lua result handling, migration/transaction boundaries, request headers/status codes, SSE cleanup, cancellation, and provider body cleanup. A real Redis/PostgreSQL integration suite is deferred to the Docker Compose milestone.

The race detector can additionally be run with `go test -race ./...` when CGO and a supported C compiler are available.

## Roadmap

- [x] **V0:** Repository initialization and Go foundations
- [x] **V1:** Minimal OpenAI-compatible gateway, provider abstraction, Mock Provider, and non-streaming completions
- [x] **V1.5:** SSE streaming, cancellation, and resource cleanup
- [x] **V2:** Redis rate limiting, idempotency, response-cache policy, and PostgreSQL usage persistence
- [ ] **V3:** Timeouts, retries, circuit breaker, provider routing, concurrency control, and observability
- [ ] **V4:** Docker Compose environment, real integration tests, benchmarks, and CI
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
