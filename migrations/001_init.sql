CREATE TABLE api_keys (
    id UUID PRIMARY KEY,
    key_hash TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE providers (
    name TEXT PRIMARY KEY,
    base_url TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE requests (
    id UUID PRIMARY KEY,
    api_key_id UUID REFERENCES api_keys(id),
    provider TEXT NOT NULL REFERENCES providers(name),
    model TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'succeeded',
        'failed',
        'rate_limited',
        'idempotency_replay',
        'cache_hit',
        'rejected'
    )),
    latency_ms BIGINT NOT NULL CHECK (latency_ms >= 0),
    cache_status TEXT NOT NULL DEFAULT 'BYPASS',
    error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX requests_created_at_idx ON requests (created_at DESC);
CREATE INDEX requests_provider_model_idx ON requests (provider, model, created_at DESC);
CREATE INDEX requests_status_idx ON requests (status, created_at DESC);

CREATE TABLE usage_records (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_id UUID NOT NULL UNIQUE REFERENCES requests(id) ON DELETE CASCADE,
    input_tokens BIGINT NOT NULL CHECK (input_tokens >= 0),
    output_tokens BIGINT NOT NULL CHECK (output_tokens >= 0),
    total_tokens BIGINT NOT NULL CHECK (total_tokens >= 0),
    estimated_cost NUMERIC(20, 10) NOT NULL DEFAULT 0 CHECK (estimated_cost >= 0),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX usage_records_created_at_idx ON usage_records (created_at DESC);
