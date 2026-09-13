-- ============================================================================
-- Payment Processing System — schema
--
-- This file is the single source of truth for the database. It is executed by
-- the postgres image only when the data volume is empty, so applying a change
-- means:  docker compose down -v && docker compose up --build
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Payments
--
-- correlation_id is UNIQUE: it is what makes payment creation idempotent. A
-- redelivered PROCESS_PAYMENT command hits the conflict instead of creating a
-- second payment row for the same saga.
-- ----------------------------------------------------------------------------
CREATE TABLE payments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    correlation_id  VARCHAR(255) NOT NULL UNIQUE,
    order_id        VARCHAR(64) NOT NULL,
    amount          BIGINT NOT NULL CHECK (amount > 0),
    currency        VARCHAR(3) NOT NULL,
    status          VARCHAR(20) NOT NULL CHECK (status IN (
                        'CREATED',
                        'PROCESSING',
                        'SUCCEEDED',
                        'FAILED',
                        'CANCELLED',
                        'UNKNOWN',      -- provider outcome undetermined
                        'RECONCILING'   -- reconciler is resolving an UNKNOWN
                    )),
    provider_txn_id VARCHAR(64),
    attempts        INT NOT NULL DEFAULT 0,
    failure_reason  TEXT,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Auditable payment lifecycle: one row per state transition.
CREATE TABLE payment_events (
    id             BIGSERIAL PRIMARY KEY,
    payment_id     UUID NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    correlation_id VARCHAR(255) NOT NULL,
    from_status    VARCHAR(20),
    to_status      VARCHAR(20) NOT NULL,
    reason         TEXT,
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- Idempotency keys (HTTP layer)
--
-- request_hash guards against the same key being reused with a different body,
-- which would otherwise replay the wrong stored response.
-- ----------------------------------------------------------------------------
CREATE TABLE idempotency_keys (
    key            VARCHAR(255) PRIMARY KEY,
    request_hash   VARCHAR(64) NOT NULL,
    correlation_id VARCHAR(255),
    response_code  INT,
    response_body  JSONB,
    status         VARCHAR(20) NOT NULL CHECK (status IN ('PROCESSING', 'COMPLETED', 'FAILED')),
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- Transactional Outbox
--
-- A business state change and the message announcing it are written in the same
-- transaction. The relay publishes rows to RabbitMQ afterwards, so a crash
-- between commit and publish loses nothing.
--
-- target_queue makes routing data rather than an if/else in the relay.
-- aggregate_id is VARCHAR because it holds workflow UUIDs and order ids alike.
-- ----------------------------------------------------------------------------
CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type  VARCHAR(50) NOT NULL,          -- 'WORKFLOW' | 'PAYMENT' | 'INVENTORY'
    aggregate_id    VARCHAR(64) NOT NULL,
    event_type      VARCHAR(50) NOT NULL,          -- 'RESERVE_INVENTORY' | ...
    target_queue    VARCHAR(64) NOT NULL,
    correlation_id  VARCHAR(255),
    payload         JSONB NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'PROCESSED', 'FAILED')),
    retry_count     INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    next_attempt_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed_at    TIMESTAMP WITH TIME ZONE
);

-- ----------------------------------------------------------------------------
-- Inbox / idempotent consumer
--
-- Composite PK: each consumer processes a given event exactly once, but two
-- different consumers may both legitimately see the same event.
-- ----------------------------------------------------------------------------
CREATE TABLE processed_events (
    event_id      UUID NOT NULL,
    consumer_name VARCHAR(100) NOT NULL,
    processed_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id, consumer_name)
);

-- ----------------------------------------------------------------------------
-- Saga workflow state
-- ----------------------------------------------------------------------------
CREATE TYPE workflow_status AS ENUM (
    'STARTED',
    'RESERVING_INVENTORY',
    'PROCESSING_PAYMENT',
    'SENDING_NOTIFICATION',
    'COMPENSATING_INVENTORY',
    'AWAITING_RECONCILIATION',   -- payment outcome unknown; never blind-retried
    'COMPENSATED',               -- terminal: rolled back cleanly
    'COMPENSATION_FAILED',       -- terminal: needs a human
    'COMPLETED',                 -- terminal: success
    'FAILED'                     -- terminal: failed with nothing to undo
);

CREATE TABLE workflow_executions (
    workflow_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    correlation_id    VARCHAR(255) UNIQUE NOT NULL,
    order_id          VARCHAR(64) NOT NULL,
    current_state     workflow_status NOT NULL DEFAULT 'STARTED',
    payload           JSONB NOT NULL,
    failure_reason    TEXT,
    compensation_attempts INT NOT NULL DEFAULT 0,
    created_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at      TIMESTAMP WITH TIME ZONE
);

-- Audit history: one row per saga transition, including refused transitions.
CREATE TABLE saga_step_history (
    id             BIGSERIAL PRIMARY KEY,
    workflow_id    UUID REFERENCES workflow_executions(workflow_id) ON DELETE CASCADE,
    correlation_id VARCHAR(255) NOT NULL,
    step           VARCHAR(50) NOT NULL,     -- the event that drove the change
    from_state     VARCHAR(50),
    to_state       VARCHAR(50),
    outcome        VARCHAR(20) NOT NULL,     -- 'APPLIED' | 'IGNORED' | 'ERROR'
    detail         TEXT,
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- Inventory service
--
-- reservations is what makes RELEASE idempotent: releasing twice must not
-- credit stock twice.
-- ----------------------------------------------------------------------------
CREATE TABLE inventory (
    sku       VARCHAR(64) PRIMARY KEY,
    available INT NOT NULL CHECK (available >= 0),
    reserved  INT NOT NULL DEFAULT 0 CHECK (reserved >= 0)
);

CREATE TABLE reservations (
    correlation_id VARCHAR(255) PRIMARY KEY,
    order_id       VARCHAR(64) NOT NULL,
    items          JSONB NOT NULL,
    status         VARCHAR(20) NOT NULL CHECK (status IN ('RESERVED', 'RELEASED')),
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    released_at    TIMESTAMP WITH TIME ZONE
);

-- ----------------------------------------------------------------------------
-- Notification service
-- ----------------------------------------------------------------------------
CREATE TABLE notifications (
    id             BIGSERIAL PRIMARY KEY,
    correlation_id VARCHAR(255) NOT NULL UNIQUE,
    order_id       VARCHAR(64) NOT NULL,
    channel        VARCHAR(20) NOT NULL DEFAULT 'EMAIL',
    body           TEXT NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- Mock provider ledger (lives in the same DB for convenience; the provider
-- service is otherwise a completely separate system)
-- ----------------------------------------------------------------------------
CREATE TABLE provider_transactions (
    idempotency_key VARCHAR(255) PRIMARY KEY,
    txn_id          VARCHAR(64) NOT NULL,
    amount          BIGINT NOT NULL,
    outcome         VARCHAR(20) NOT NULL,  -- 'APPROVED' | 'DECLINED'
    response_sent   BOOLEAN NOT NULL DEFAULT TRUE,  -- false = the "lost response" case
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- Indexes
-- ----------------------------------------------------------------------------
CREATE INDEX idx_payments_status         ON payments(status);
CREATE INDEX idx_payments_order          ON payments(order_id);
CREATE INDEX idx_payment_events_payment  ON payment_events(payment_id);
CREATE INDEX idx_workflow_correlation    ON workflow_executions(correlation_id);
CREATE INDEX idx_workflow_state          ON workflow_executions(current_state);
CREATE INDEX idx_saga_history_corr       ON saga_step_history(correlation_id);
CREATE INDEX idx_outbox_pending          ON outbox_events(status, next_attempt_at)
                                         WHERE status = 'PENDING';

-- ----------------------------------------------------------------------------
-- Seed stock. SKU 'OUT_OF_STOCK' exists with zero availability so the
-- inventory-failure path can be triggered deterministically.
-- ----------------------------------------------------------------------------
INSERT INTO inventory (sku, available) VALUES
    ('SKU-1', 1000),
    ('SKU-2', 1000),
    ('SKU-3', 1000),
    ('OUT_OF_STOCK', 0);
