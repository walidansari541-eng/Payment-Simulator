# Payment Processing System — Saga Orchestration

A distributed payment system built to make the hard parts of distributed transactions **visible and testable**: Saga orchestration, compensation, the transactional Outbox, idempotent consumers, bounded retries, unknown provider outcomes, and reconciliation.

This is not a payment integration. It is a **simulator**. Six services coordinate a checkout across independent failure domains, and every failure mode you would otherwise spend a year meeting in production can be triggered on demand with one command..

---

## Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Node.js 22 (Alpine) |
| HTTP | Express 5 |
| Database | PostgreSQL 17 |
| Message broker | RabbitMQ 3 (management image) |
| DB driver | `pg` (connection pooling) |
| AMQP client | `amqplib` (confirm channels, per-message TTL, dead-letter exchanges) |
| Config | `dotenv` |
| Orchestration | Docker Compose — 8 containers |
| Tests | Bash + curl + psql assertions (`scripts/`) |
| Logging | Structured JSON to stdout, correlation ID on every line |

### Services

| Service | Host port | Responsibility |
|---|---|---|
| `api` | 3000 | Checkout, workflow/payment queries, admin, health, metrics |
| `orchestrator` | 9101 | Saga state machine + recovery jobs |
| `inventory-service` | 9102 | Reserve and release stock |
| `payment-worker` | 9103 | Talks to the external provider |
| `notification-service` | 9104 | Sends the receipt |
| `mock-provider` | 4000 | Fake card processor with injectable behaviour |
| `postgres` | 5442 | All state |
| `rabbitmq` | 5673 / 15673 | Broker + management UI |

All six application services run the same image with a different entrypoint, so any one of them can be killed mid-saga to observe recovery. Host ports for Postgres and RabbitMQ are offset so the stack runs alongside other local projects.

---

## Getting Started

### Prerequisites

- **Docker Desktop** (or Docker Engine + Compose v2)
- **bash**, **curl** — for the test scripts
- **jq** — for readable output in the examples (`brew install jq`)
- Node.js 22+ only if you intend to run a service outside Docker

### Run it locally

```bash
git clone <repo-url>
cd payment-system

cp .env.example .env          # local defaults, no real credentials

docker compose up -d --build
docker compose ps             # wait for postgres and rabbitmq to report healthy

curl -s localhost:3000/ready | jq
# -> { "ready": true, "checks": { "postgres": true, "rabbitmq": true } }
```

Run the full suite of failure scenarios:

## Architecture

Nothing publishes to RabbitMQ directly. Every outgoing message is first written to a database table **in the same transaction as the business change that caused it**, and a relay publishes it afterwards. Every consumer writes its reply back to that same table. The system is therefore a ring, and every hop passes through Postgres.

```
  [ Client ]
      │ POST /checkout
      ▼
  ( api ) ──── workflow + first command, ONE transaction ───► ▤ outbox_events
                                                                   │
                                                          claim PENDING (SKIP LOCKED)
                                                                   ▼
                                                            ( outbox relay )
                                                                   │ publish + confirm
                                                                   ▼
                                                            ▤ RabbitMQ queues
                                                                   │ deliver
            ┌──────────────────┬──────────────────┬───────────────┴──────────┐
            ▼                  ▼                  ▼                          ▼
    ( inventory )        ( payment worker )  ( notification )         ( orchestrator )
            │                  │                  │                          │
            │                  ▼                  │                 reads current state,
            │          [ Payment Provider ]       │                 decides next command
            │                  │                  │                          │
            └──────────────────┴──────────────────┴──────────────────────────┘
                                       │
                       outcome events and next commands
                                       ▼
                               ▤ outbox_events  ── back to the relay (ring closes)
```

The saga has four business steps — reserve inventory, charge the card, send the receipt, complete — and each one rides the same four mechanical steps: commit with an outbox row, relay publishes, consumer handles, consumer writes its reply to the outbox.

### Workflow state machine

```
                         RESERVING_INVENTORY
                          │                │
        INVENTORY_FAILED  │                │ INVENTORY_RESERVED
                          ▼                ▼
                      FAILED ✗      PROCESSING_PAYMENT
                                     │      │      │
                  PAYMENT_FAILED ────┘      │      └──── PAYMENT_UNKNOWN
                          │                 │                    │
                          ▼   PAYMENT_SUCCEEDED                  ▼
              COMPENSATING_INVENTORY │            AWAITING_RECONCILIATION
                    │        │       │                  │           │
   INVENTORY_       │        │       ▼          was charged      never charged
   RELEASED         │        │  SENDING_NOTIFICATION  │              │
        ▼           │        │       │                │              │
   COMPENSATED ✗    │        │       ▼  NOTIFICATION_SENT            │
                    │        └──►  COMPLETED ✗ ◄──────┘              │
     release exhausted                                               │
                    ▼                                                │
        COMPENSATION_FAILED ✗ ◄──────────────────────────────────────┘
                                                          (compensate instead)

   ✗ = terminal state
```

Three failure boundaries, three deliberately different answers. Inventory fails and nothing has happened yet, so the saga simply fails. Payment declines and stock is held but no money moved, so compensate. The payment outcome is **unknown** and money may have moved, so do neither — go and ask.

---

## API Reference

Base URL `http://localhost:3000` unless stated otherwise.

### POST /checkout

**Purpose:** Start a checkout saga. Returns immediately with a correlation ID; all downstream work happens asynchronously.

**Business logic**

1. **Idempotency claim** — The key is inserted with a conflict guard. If the row already exists, the request is a duplicate: a completed key replays its stored response, an in-flight key returns `409`, and a key reused with a *different* request body returns `422` rather than a confidently wrong replay.
3. **Correlation ID** — Taken from the request header if supplied, otherwise generated. This single value threads through every service, log line, database row and the provider's idempotency key.
4. **Saga creation** — In **one transaction**: the workflow row is created, the first command is written to the outbox, and the opening transition is recorded. No broker call happens inside the request.
5. **Response** — `202 Accepted` with the workflow ID, correlation ID and a poll URL. The response is stored against the idempotency key before returning.

**Error paths**

- `400` — missing `Idempotency-Key`, or missing/invalid body fields
- `409` — a request with this key is currently in flight
- `422` — this key was already used with a different request body
- `500` — persistence failure; the key is released so a retry can succeed

**Why the broker is never called here:** if the workflow committed and the publish then failed, the order would exist with nothing to move it forward. Writing the message as a database row in the same transaction removes that gap entirely — the relay retries until the broker confirms.

---

### POST /charge — mock provider (port 4000)

**Purpose:** Stand in for an external card processor, including the behaviours that make payments hard.

**Behaviour selection.** Either the order ID carries a trigger word, or a runtime override is set through `POST /admin/behavior` — so failure injection needs no restart.

**Business logic**

1. **Key required** — a request without an idempotency key is rejected with `400`.
2. **Ledger replay** — a key already present in the ledger returns its original outcome verbatim, whatever the current behaviour mode says. This is what protects a client that retries.
3. **Injected latency and failure rate** are applied.
4. **Behaviour branch** — approve, decline, return a transient error, or withhold the response entirely.
5. **Every decision is logged** with the key and order ID.

---

### GET /transactions — mock provider (port 4000)

**Purpose:** Let the reconciler discover what actually happened to a charge whose response was lost. This endpoint is the reason an unknown outcome is recoverable at all.

**Business logic**

1. Requires an idempotency key as a query parameter, otherwise `400`.
2. A matching ledger entry returns the outcome and transaction reference.
3. No entry returns `404` — an **authoritative** statement that no money moved.

---

### Read and operational endpoints

| Endpoint | Purpose | Tables read |
|---|---|---|
| `GET /workflows/:correlationId` | Full saga timeline: current state, the payment, the reservation, and every transition including ones that were *ignored* as duplicates | `workflow_executions`, `saga_step_history`, `payments`, `reservations` |
| `GET /payments/:id` | A payment and its auditable status history | `payments`, `payment_events` |
| `GET /payments` | List payments, filterable by status and order; capped page size | `payments` |
| `GET /admin/stuck` | First query of the incident runbook: non-terminal workflows past a threshold, unpublished outbox rows, and unresolved payments | `workflow_executions`, `outbox_events`, `payments` |
| `GET /health` | Liveness. Deliberately touches no dependency, so a database blip cannot make Docker kill a healthy container | — |
| `GET /ready` | Readiness. Verifies Postgres and RabbitMQ are both reachable; `503` when not | — |
| `GET /metrics` | Prometheus text format, including gauges collected from the database at scrape time | several, counts only |

Every application service exposes `/health`, `/ready` and `/metrics` on its own port (9101–9104), not just the API.

---
