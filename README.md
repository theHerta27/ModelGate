# ModelGate

> High-performance LLM & Agent Gateway written in Go.

ModelGate is a learning-driven backend engineering project for building the infrastructure behind LLM and agent applications. The project will evolve incrementally, with each capability introduced only when it solves a concrete gateway problem.

## Current Status

**Version 0 — Go foundations and repository setup**

The repository is currently being initialized. No gateway API or production feature is implemented yet.

## Planned Technology Stack

- Go and Gin
- PostgreSQL with pgx and SQL migrations
- Redis with go-redis and Lua scripts
- Prometheus and Grafana
- Docker and Docker Compose
- GitHub Actions

Technologies may be adjusted as the implementation evolves. Kafka, Kubernetes, service mesh, and premature microservice decomposition are intentionally out of scope for the initial versions.

## Roadmap

- [ ] **V0 (in progress):** Repository initialization and Go foundations
- [ ] **V1:** Minimal OpenAI-compatible gateway, provider abstraction, mock provider, and non-streaming chat completions
- [ ] **V1.5:** SSE streaming, cancellation, and resource cleanup
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
