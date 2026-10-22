# graceful-api

A small **job-queue service written in Go**: an HTTP API enqueues jobs into PostgreSQL, and concurrent worker goroutines claim and process them safely. The service shuts down gracefully, so a deploy or `Ctrl+C` does not lose work in progress.

> **Status: learning project, early stage.** The queue, concurrency and shutdown logic are implemented. The "work" a job performs is simulated (a 3-second wait). There are no automated tests yet and the service is not deployed anywhere. See [Known limitations](#known-limitations).

I am using this project to learn Go back-end fundamentals: goroutines, `context` cancellation, `sync.WaitGroup`, `net/http` and database-backed queues with `pgx`.

## What is implemented

- **HTTP API** (`net/http`): `POST /jobs?id=N` stores a job with payload `{"id": N}` and answers `202 Accepted`.
- **Persistent queue in PostgreSQL**: jobs live in a `jobs` table (`pending`, `processing`, `completed`, `failed`), so they survive restarts.
- **Concurrent workers**: two worker goroutines (`internal/jobs/worker.go`) poll the database, claim one job at a time and process it.
- **Safe claiming**: a single SQL statement with `FOR UPDATE SKIP LOCKED` lets several workers pull from the same table without taking the same job twice.
- **Leases**: a claimed job holds a 30-second lease. If a worker dies, the job becomes claimable again once the lease expires.
- **Retries with exponential backoff**: a failed job returns to `pending` with `available_at` pushed back by `2^attempts` seconds (capped at `2^6`), until `max_attempts` (default 3) is reached and it becomes `failed`.
- **Stale-worker protection**: `Complete` and `Fail` only update a row when the attempt number and the lease still match, so a worker that lost its lease cannot overwrite a newer attempt (`ErrLeaseLost`).
- **Idempotency**: an optional `Idempotency-Key` header is stored in a `UNIQUE` column, so the same request cannot create two jobs.
- **Graceful shutdown**: see below.

## Architecture

```text
            POST /jobs?id=N
 client ───────────────────────► HTTP server (net/http)
                                      │ Enqueue (INSERT)
                                      ▼
                          ┌──────────────────────┐
                          │ PostgreSQL: jobs     │
                          └──────────────────────┘
                              ▲ Claim / Complete / Fail
              ┌───────────────┴───────────────┐
         worker goroutine 1             worker goroutine 2
```

```text
cmd/api/main.go            entry point: wiring, signals, shutdown sequence
internal/config/env.go     minimal .env loader (does not override existing env vars)
internal/db/db.go          pgx connection pool + startup ping
internal/jobs/model.go     Job type and status constants
internal/jobs/repository.go  SQL: Enqueue, Claim (SKIP LOCKED), Complete, Fail
internal/jobs/worker.go    worker loop and job processing
internal/jobs/queue.go     first-version in-memory queue (kept as reference, unused)
internal/server/server.go  HTTP routes
migrations/                SQL files (see note on 002 below)
docker-compose.yml         local PostgreSQL 17
```

## PostgreSQL integration

- Driver and pool: [`pgx/v5`](https://github.com/jackc/pgx) with `pgxpool`, shared by the HTTP handler and the workers. The process pings the database before it starts serving.
- Configuration: `DATABASE_URL` (read from the environment or from a local `.env`, which is git-ignored).
- Schema: `migrations/001_jobs.sql` creates the `jobs` table with a `CHECK` on `status`, a `UNIQUE` idempotency key, `attempts` / `max_attempts`, `lease_until` and `available_at`.
- `migrations/002_candidate.sql` is **not** a schema migration. It is a manual script to run the claim query by hand in two `psql` sessions and observe `SKIP LOCKED` in action.
- Claiming is done in one atomic statement (`CTE` + `UPDATE ... FROM candidate ... RETURNING`) inside a transaction.

## Graceful shutdown

Implemented in `cmd/api/main.go`:

1. `signal.NotifyContext` listens for `SIGINT` and `SIGTERM`.
2. Workers get their own context. On shutdown it is cancelled so they stop claiming new jobs.
3. `http.Server.Shutdown` runs with a 10-second deadline so in-flight HTTP requests can finish. If it times out, the server is closed.
4. A `sync.WaitGroup` tracks the workers, and `main` waits for them (bounded by the same deadline).
5. A worker interrupted in the middle of a job calls `Fail` using a **fresh context** (the cancelled one cannot run the `UPDATE`), which returns the job to the queue with backoff so another worker can pick it up.

Why it matters: without this, a deploy or restart can kill a process while it holds a job, leaving it stuck as `processing` or cutting HTTP responses mid-flight. Here the job is returned to the queue and the HTTP server finishes active requests first.

Note: an interrupted job still consumes one attempt, because `attempts` is incremented at claim time.

## Running locally

Requirements: Go (version in `go.mod`), Docker.

```bash
# 1. Start PostgreSQL (host port 5433)
docker compose up -d

# 2. Create the schema
docker compose exec -T postgres psql -U graceful -d graceful < migrations/001_jobs.sql

# 3. Configure the connection (see the credentials in docker-compose.yml)
echo 'DATABASE_URL=postgres://graceful:<password-from-docker-compose>@127.0.0.1:5433/graceful' > .env

# 4. Run the API
go run ./cmd/api
```

Enqueue jobs and watch the workers in the logs:

```bash
curl -X POST "http://127.0.0.1:8080/jobs?id=1"
curl -X POST "http://127.0.0.1:8080/jobs?id=2" -H "Idempotency-Key: demo-2"
```

Try the shutdown: enqueue a few jobs, then press `Ctrl+C` while they are running.

## Known limitations

These are known and deliberately listed here rather than hidden:

- Job processing is simulated (a 3-second wait). There is no real task handler yet.
- No automated tests and no CI.
- No endpoint to read a job's status, and no `/health` endpoint.
- Repeating an `Idempotency-Key` currently returns `503` with the raw database error. It should return the original job (or `409`).
- No authentication, and `http.Server` has no read/write timeouts.
- The HTTP address is hard-coded to `127.0.0.1:8080`, and there is no `Dockerfile` for the API itself.
- Migrations are applied by hand; there is no migration tool.
- `docker-compose.yml` contains a fixed development database password and publishes the database port on all interfaces. Fine for local learning, **not** for a public demo.
- The lease is a fixed 30 seconds with no heartbeat, so jobs longer than that would be claimed twice.

## Possible next steps

Replace the simulated handler with a real one, add unit and integration tests (for example with `testcontainers-go`), return the existing job for repeated idempotency keys, add timeouts and a health endpoint, and move credentials to environment variables.
