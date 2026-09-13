# Notes App

A multi-user notes REST API built on Express 5. Each user registers, logs in for a session token, and manages their own private notes. The service demonstrates a full production-shaped stack: PostgreSQL for persistence, Redis for session storage and read-through caching, RabbitMQ for asynchronous notification delivery with retries and a dead-letter queue, and Nginx as a reverse proxy.

The whole stack runs from a single `docker compose up` on any machine with Docker installed.

## Tech Stack

| Layer            | Technology                          |
|------------------|-------------------------------------|
| Runtime          | Node.js 18 (Alpine, in Docker)      |
| Framework        | Express 5.x                         |
| Database         | PostgreSQL 15 (`pg` 8.x driver)     |
| Cache / Sessions | Redis 7 (`redis` 5.x client)        |
| Message Broker   | RabbitMQ 3 (`amqplib`)              |
| Reverse Proxy    | Nginx (alpine)                      |
| Password Hashing | bcrypt                              |
| Config           | dotenv                              |

## Getting Started

### Prerequisites

The remote machine needs only:

- Docker Engine 20.10+
- Docker Compose v2 (`docker compose`, bundled with modern Docker)
- Open inbound ports: `80` (Nginx), and optionally `3000`, `5433`, `6379`, `5672`, `15672` if you want to reach the individual services directly

No Node.js or database installation is required on the host — everything runs in containers.

### Deploy

```bash
# 1. Clone onto the remote machine
git clone <repo-url>
cd notes-app

# 2. Build and start the full stack in the background
docker compose up -d --build

# 3. Watch the logs until the app reports it is listening
docker compose logs -f app
```

Expect to see `Connected to Redis`, `Connected to RabbitMQ`, `SMS worker listening on notes-queue...` and `Server is running on port 3000`.

### What comes up

| Container        | Service   | Host port | Notes                                                    |
|------------------|-----------|-----------|----------------------------------------------------------|
| `notes_nginx`    | Nginx     | `80`      | Public entry point, proxies to the app                   |
| `notes_app`      | API       | `3000`    | Express server plus the inline SMS worker                |
| `notes_postgres` | Postgres  | `5433`    | Published on 5433 to avoid clashing with a native Postgres |
| `notes_redis`    | Redis     | `6379`    | Sessions and note cache                                  |
| `boot_rabbitmq`  | RabbitMQ  | `5672`, `15672` | `15672` serves the management UI                   |

Database tables are created automatically on first boot by the SQL in `init-scripts/`, which Postgres runs as an entrypoint script against the empty data volume.

### Configuration

All runtime configuration is supplied as environment variables in `docker-compose.yml` (database host/credentials, Redis host, RabbitMQ URL, `PORT`, `INLINE_WORKER`). Change them there before deploying to a real environment — the committed values are development defaults and must not be used in production. Running outside Compose reads the same variables from a local `.env`, which is gitignored and should never be committed.

### Stopping and resetting

```bash
docker compose down            # stop, keep data volumes
docker compose down -v         # stop and wipe Postgres/Redis/RabbitMQ data
docker compose restart app     # restart just the API after a config change
```

### Worker topology

`server.js` starts the RabbitMQ consumer inside the API process when `INLINE_WORKER` is not set to `false`. That is convenient for single-node deployments. To scale the worker separately, set `INLINE_WORKER=false` on the app service and run the dedicated worker entrypoint (`npm run worker:sms`) as its own container or process — otherwise the same message is consumed twice.

## Architecture Overview

```
                  ┌──────────┐
   Client ───────▶│  Nginx   │ :80
                  └────┬─────┘
                       │ proxy_pass
                       ▼
                 ┌───────────┐        ┌────────────┐
                 │  Express  │───────▶│ PostgreSQL │  users, notes
                 │   API     │        └────────────┘
                 │  :3000    │
                 │           │        ┌────────────┐
                 │           │───────▶│   Redis    │  session tokens, note cache
                 │           │        └────────────┘
                 │           │        ┌────────────────────────────┐
                 │           │───────▶│ RabbitMQ                   │
                 └───────────┘        │  notification-events (topic)│
                       ▲              │    └─▶ notes-queue          │
                       │              │          └─▶ dlx-exchange   │
                 ┌───────────┐        │                └─▶ notes-dlq│
                 │ SMS Worker│◀───────│                            │
                 │ (inline)  │        └────────────────────────────┘
                 └───────────┘
```

## Authentication Model

All `/api/notes` routes and `/api/health` sit behind `authGuard`. The guard reads the `Authorization` header (with or without a `Bearer ` prefix), looks the token up in Redis, and attaches the decoded user context to the request. Sessions are opaque random tokens stored in Redis with a one-hour expiry — there is no JWT and no refresh flow. A missing header is a `400`; a token that is absent or expired in Redis is a `401`.

Every note query is scoped to the authenticated user, so one user can never read or mutate another user's notes — a note belonging to someone else surfaces as `404`, not `403`.

## API Reference

Base path: `/api`. Through Nginx the public base URL is `http://<host>/api`.

---

### POST /api/register

**Purpose:** Create a user account and fire an asynchronous welcome notification.

**Business Logic:**

1. **Input validation** — Requires `username` and `password` in the body. Returns `400` if either is missing.
2. **Password hashing** — Hashes the password with bcrypt at 10 salt rounds. The plaintext is never stored.
3. **Account creation** — Writes a new user record and returns only the identifier and username; the hash is not returned to the client.
4. **Event publication** — Publishes a welcome SMS event to the `notification-events` topic exchange with a per-user routing key, a correlation id for tracing, and `persistent: true` so the message survives a broker restart. Publishing happens inside a try/catch: a broker failure is logged but does not fail registration.
5. **Backpressure handling** — If the channel's write buffer is full, the publish returns `false`; the message is still buffered locally but the condition is logged as broker backpressure.
6. **Response** — Returns `201` with the created user.

**Error paths:**
- `400` — Missing `username` or `password`
- `500` — Database failure (a duplicate username currently surfaces here)

### POST /api/login

**Purpose:** Exchange credentials for an opaque session token.

**Business Logic:**

1. **Input validation** — Requires `username` and `password`. Returns `400` otherwise.
2. **User lookup** — Reads the user record by username. Returns `401` if no account matches.
3. **Password verification** — Compares the supplied password against the stored bcrypt hash. Returns `401` on mismatch.
4. **Session creation** — Generates a 16-byte random hex token and stores the user context against it in Redis with a one-hour TTL.
5. **Response** — Returns `200` with the token. The client sends it back as the `Authorization` header on every protected route.

**Error paths:**
- `400` — Missing credentials
- `401` — Unknown user or wrong password
- `500` — Database or Redis failure

### GET /api/notes/:id

**Purpose:** Fetch a single note belonging to the authenticated user, served from cache when possible.

**Business Logic:**

1. **Authentication** — `authGuard` resolves the session token from Redis and attaches the user context.
2. **Cache lookup** — Checks a Redis key namespaced by user and note id. On a hit, the cached note is returned immediately and the database is never touched.
3. **Database read** — On a miss, reads the note scoped to both the note id and the owning user.
4. **Cache population** — If a row was found, caches it for 3600 seconds.
5. **Response** — Returns `200` with the note, or `404` when the note does not exist or belongs to someone else.

**Error paths:**
- `400` — Missing `Authorization` header
- `401` — Session expired or invalid token
- `404` — Note not found for this user
- `500` — Database or cache failure

### GET /api/notes

**Purpose:** List all notes owned by the authenticated user.

**Tables Involved:**

**Business Logic:** After `authGuard` resolves the session, the service reads all notes scoped to the user, ordered by creation time descending, and returns them as an array. This route bypasses the cache entirely — only single-note reads are cached.

**Error paths:** `400` missing header · `401` invalid session · `500` database failure.

---

### POST /api/notes

**Purpose:** Create a note owned by the authenticated user.


**Business Logic:** Takes `title` and `description` from the body and writes a new row linked to the authenticated user, with creation and update timestamps set server-side. Returns `201` with the created note. Note that field-level validation is not currently enforced in the service layer — a missing `title` is rejected by the database constraint and surfaces as `500`.

**Error paths:** `400` missing header · `401` invalid session · `500` database failure or constraint violation.

---

### PUT /api/notes/:id

**Purpose:** Update a note's title and description.

**Side effects:** Deletes the cached entry for that note so the next read repopulates it from the database.

**Business Logic:** The update is scoped to both note id and user id, so a note belonging to another user matches nothing and returns `404`. On a successful write the corresponding Redis cache key is invalidated. Returns `200` with the updated note.

**Error paths:** `400` missing header · `401` invalid session · `404` note not found for this user · `500` database failure.

---

### DELETE /api/notes/:id

**Purpose:** Delete a note.

**Side effects:** Deletes the cached entry for that note.

**Business Logic:** Deletes the note matching both the id and the authenticated user, invalidates its cache key, and returns `200` with a confirmation message. A note that does not exist or is not owned by the caller returns `404`.

**Error paths:** `400` missing header · `401` invalid session · `404` note not found for this user · `500` database failure.

---

### GET /api/health

**Purpose:** Liveness and dependency check.

**Business Logic:** Requires a valid session (it sits behind `authGuard`). Returns `200` with `status: "UP"`, a timestamp, and the database status, or `503` with `status: "DOWN"` and the error message when the dependency check fails.

## Message Queue Design

Registration publishes to the `notification-events` topic exchange with routing key `notification.sms.<userId>`. The durable `notes-queue` binds the pattern `notification.sms.*` and is declared with a dead-letter exchange, so rejected messages land in `notes-dlq`.

The consumer applies four production patterns:

| Pattern             | Implementation                                                                      |
|---------------------|-------------------------------------------------------------------------------------|
| **Fair dispatch**   | `prefetch(1)` — one unacknowledged message per worker at a time                      |
| **Idempotency**     | Processed message ids tracked in an in-process set; duplicates are acked and skipped |
| **Retry**           | Failures are republished to the exchange with an incremented `x-retry-count` header, up to 3 attempts |
| **Dead-lettering**  | Fatal errors (malformed payloads) and exhausted retries are `nack`ed without requeue, routing them to `notes-dlq` |

The idempotency store is in-memory and therefore per-process and non-durable; moving it to Redis is the natural next step before running multiple worker replicas.

## Testing

```bash
# 1. Register a user. Expect 201 and a JSON body with id and username.
#    Check `docker compose logs -f app` in another terminal: you should see a
#    [publish] line followed by a [FAKE SMS] line from the worker.
curl -s -X POST $HOST/api/register \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"secret123"}'

# 2. Log in and capture the session token.
TOKEN=$(curl -s -X POST $HOST/api/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"secret123"}' \
  | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
echo "$TOKEN"

# 3. Health check. Expect 200 with status UP.
curl -s $HOST/api/health -H "Authorization: Bearer $TOKEN"

# 4. Create a note. Expect 201 with the created note.
curl -s -X POST $HOST/api/notes \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"title":"First note","description":"hello world"}'

# 5. List notes. Expect an array containing the note above.
curl -s $HOST/api/notes -H "Authorization: Bearer $TOKEN"

# 6. Fetch one note by id. Run it twice — the second call is a cache hit,
#    visible as "Cache hit for note ID: 1" in the app logs.
curl -s $HOST/api/notes/1 -H "Authorization: Bearer $TOKEN"
curl -s $HOST/api/notes/1 -H "Authorization: Bearer $TOKEN"

# 7. Update the note. Expect 200 with the new values, and the cache entry
#    is invalidated — a following GET hits the database again.
curl -s -X PUT $HOST/api/notes/1 \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"title":"Updated title","description":"updated body"}'

# 8. Delete the note. Expect 200 with a confirmation message.
curl -s -X DELETE $HOST/api/notes/1 -H "Authorization: Bearer $TOKEN"

# 9. Fetch it again. Expect 404.
curl -s $HOST/api/notes/1 -H "Authorization: Bearer $TOKEN"
```

### Negative cases worth checking

```bash
# No Authorization header → 400
curl -i -s $HOST/api/notes

# Garbage token → 401 Unauthorized: Session expired
curl -i -s $HOST/api/notes -H "Authorization: Bearer not-a-real-token"

# Wrong password → 401
curl -i -s -X POST $HOST/api/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"wrong"}'

# Missing credentials → 400
curl -i -s -X POST $HOST/api/login \
  -H 'Content-Type: application/json' -d '{}'

# Cross-user isolation: register a second user, log in as them, and request
# the first user's note id. Expect 404, not the note.
```

### Checking the supporting services

```bash
docker compose ps                              # all containers healthy?
docker compose logs -f app                     # API, publisher, and worker output
docker compose exec redis redis-cli KEYS '*'   # session tokens and cached notes
docker compose exec postgres psql -U myuser -d notes_db -c '\dt'   # tables created?
```

The RabbitMQ management UI is available at `http://<host>:15672` using the broker credentials from `docker-compose.yml`. Use it to confirm that `notification-events`, `notes-queue`, `dlx-exchange`, and `notes-dlq` were declared, and to watch messages flow through on registration.
