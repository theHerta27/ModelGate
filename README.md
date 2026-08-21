# ModelGate

> High-performance LLM & Agent Gateway written in Go.

ModelGate is a learning-driven backend engineering project for building the infrastructure behind LLM and agent applications. It evolves incrementally, with each capability introduced only when it solves a concrete gateway problem.

## Current Status

**Version 1.5 — SSE streaming complete**

V1.5 provides streaming and non-streaming OpenAI-compatible chat completions. It uses a deterministic Mock Provider by default; DeepSeek and generic OpenAI-compatible upstreams can be selected explicitly through environment variables.

## Features

- `GET /health`
- `POST /v1/chat/completions`
- Provider interface with Mock, DeepSeek, and generic OpenAI-compatible adapters
- Request validation and OpenAI-style error envelopes
- Incremental SSE forwarding with `data: <JSON>` events and a final `data: [DONE]`
- Client-cancellation propagation, per-chunk flushing, and deterministic stream cleanup
- Request-context propagation and bounded upstream HTTP clients
- Graceful HTTP server shutdown
- Unit tests that do not require real LLM credentials

Not implemented yet: authentication, rate limiting, caching, persistence, retries, circuit breaking, or provider routing.

## Architecture

```text
Client
  -> Gin Handler       HTTP parsing and response mapping
  -> Chat Service      deterministic request validation
  -> Provider          vendor-independent interface
       |-- Stream      typed chunk receive and lifecycle contract
       |-- MockProvider (default)
       |-- DeepSeekProvider
       `-- OpenAICompatibleProvider
```

## Technology Stack

- Go and Gin
- PostgreSQL with pgx and SQL migrations
- Redis with go-redis and Lua scripts
- Prometheus and Grafana
- Docker and Docker Compose
- GitHub Actions

Gin is the only direct runtime dependency in V1. PostgreSQL, Redis, Prometheus, Grafana, and Docker are planned for later versions when their corresponding capabilities are implemented.

## Quick Start

Requirements: Go 1.26.4 or later.

```bash
go run ./cmd/server
```

The default configuration starts ModelGate on `:8080` with the Mock Provider, so no API key is needed.

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

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Gateway listen address |
| `MODEL_PROVIDER` | `mock` | `mock`, `deepseek`, or `openai-compatible` |
| `REQUEST_TIMEOUT` | `30s` | Upstream HTTP client timeout |
| `DEEPSEEK_BASE_URL` | `https://api.deepseek.com` | DeepSeek API base URL |
| `DEEPSEEK_API_KEY` | empty | Required only for DeepSeek |
| `OPENAI_COMPATIBLE_BASE_URL` | `https://api.openai.com/v1` | Generic compatible API base URL |
| `OPENAI_API_KEY` | empty | Required only for the generic compatible provider |

Copy `.env.example` to `.env` for local reference, but export the variables through your shell or runtime. ModelGate does not load `.env` files automatically in V1.

## API

### Health

```http
GET /health
```

```json
{"status":"ok"}
```

### Chat Completions

```http
POST /v1/chat/completions
Content-Type: application/json
```

```json
{
  "model": "mock-model",
  "messages": [
    {"role": "system", "content": "Be concise."},
    {"role": "user", "content": "Hello"}
  ],
  "temperature": 0.7,
  "max_tokens": 256,
  "stream": false
}
```

ModelGate accepts text messages with `developer`, `system`, `user`, or `assistant` roles. It also supports the optional `temperature`, `top_p`, and `max_tokens` fields.

Set `stream` to `true` to receive `Content-Type: text/event-stream`. Each incremental chunk is emitted as `data: <JSON>` followed by a blank line. A successful stream ends with:

```text
data: [DONE]
```

If a provider fails before the first chunk, ModelGate returns its normal JSON error envelope. If it fails after streaming has started, ModelGate closes the response because the HTTP status has already been committed.

## Testing

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/server
```

Tests use Mock Provider or local `httptest` upstreams and never require a real LLM key. The streaming tests cover SSE parsing, `[DONE]`, keep-alive comments, oversized events, cancellation propagation, flushing, error boundaries, and response-body cleanup.

## Roadmap

- [x] **V0:** Repository initialization and Go foundations
- [x] **V1:** Minimal OpenAI-compatible gateway, provider abstraction, mock provider, and non-streaming chat completions
- [x] **V1.5:** SSE streaming, cancellation, and resource cleanup
- [ ] **V2:** Redis rate limiting, idempotency, response-cache policy, and PostgreSQL usage persistence
- [ ] **V3:** Timeouts, retries, circuit breaker, provider routing, concurrency control, and observability
- [ ] **V4:** Docker Compose environment, integration tests, benchmarks, and CI
- [ ] **V5 (optional):** Asynchronous usage events with a message queue, only if justified by real requirements

## Development Principles

- Build one Go gateway service before considering service decomposition.
- Keep infrastructure decisions deterministic and testable.
- Use mock providers for tests and CI; no real LLM key is required.
- Introduce dependencies only when the current version needs them.
- Publish measured performance results with reproducible methodology.
- Never commit secrets, real environment files, or private user data.

## License

This project is licensed under the MIT License.
