# Trust Wallet ETL Take-Home

A small **extract → transform → load** service in Go. It polls a public REST API every 30 seconds, writes an immutable raw copy to Postgres and the local filesystem, normalizes the payload, then loads the curated records into Postgres and a processed lake path.

This repository is the submission for the Trust Wallet Data Engineer take-home.

## Why this API

Source: [Random User API](https://randomuser.me/) (`https://randomuser.me/api/`).

It is a better fit for this exercise than a flat todo list:

- Nested documents (name, location, login, pictures) force an explicit **schema** rather than `SELECT *`.
- Timestamps (`dob.date`, `registered.date`) need a consistent UTC / ISO-8601 contract.
- The payload includes credentials and national IDs, which should **never** land in a lake. Dropping them is a design choice, not an afterthought.
- `login.uuid` is a stable natural key, which is what you want for idempotent upserts later.

In a wallet company this maps cleanly onto an **identity / profile ingestion** path (app users, device accounts, KYC vendors) that sits beside on-chain event pipelines.

## Architecture

```
                    ┌──────────────────────┐
                    │  randomuser.me API   │
                    └──────────┬───────────┘
                               │ every 30s
                               ▼
                    ┌──────────────────────┐
                    │   Go ETL process     │
                    │  extractor           │
                    │  transformer         │
                    │  store + lake        │
                    │  /health  /metrics   │
                    └─────┬──────────┬─────┘
                          │          │
              append-only │          │ current snapshot
                          ▼          ▼
               ┌─────────────┐  ┌──────────────┐
               │  Postgres   │  │ Local lake   │
               │ raw_events  │  │ data/raw/    │
               │ processed_  │  │ data/processed/
               │   users     │  │ *.ndjson     │
               └─────────────┘  └──────────────┘
```

Each cycle:

1. **Extract** — HTTP GET with 3 retries and exponential backoff.
2. **Load raw** — insert every source document into `raw_events` (append-only) and append NDJSON under `data/raw/`.
3. **Transform** — drop secrets/PII, flatten nested fields, coerce timestamps to UTC RFC3339 / ISO-8601, wrap in an envelope (`meta` + `user`). Poison rows go to `data/dlq/` and the rest of the batch continues.
4. **Load processed** — upsert `processed_users` by `source_uuid`; append NDJSON under `data/processed/`.

Raw and processed are **two different contracts**:

| Layer | Postgres | Filesystem | Mutability |
| --- | --- | --- | --- |
| Raw | `raw_events` insert | `data/raw/dt=YYYY-MM-DD/events.ndjson` | Append only (replay log) |
| Processed | `processed_users` upsert | `data/processed/dt=YYYY-MM-DD/users.ndjson` | Table is current snapshot; lake keeps every version |

Lake files are **NDJSON** (one object per line) under Hive-style `dt=YYYY-MM-DD/` partitions. A JSON array cannot be appended safely without rewriting the whole file; NDJSON is the usual lake format and still satisfies “append, do not overwrite”.

## Project layout

```
cmd/etl/main.go              process entrypoint, graceful shutdown
internal/config              env-based configuration
internal/extractor           HTTP client + retries
internal/transformer         schema, field drops, timestamp coercion
internal/store               Postgres schema, insert, upsert
internal/lake                local data-lake writer
internal/pipeline            one ETL cycle
internal/observe             slog → logs/etl.log, /health, /metrics
```

## Data model

### Raw (`raw_events`)

The original API object is stored as `JSONB`. Nothing is interpreted except `source`, `batch_id`, and `ingested_at`. This is the replay surface.

### Processed envelope (`schema_version = 1.0.0`)

```json
{
  "meta": {
    "schema_version": "1.0.0",
    "source": "randomuser",
    "ingested_at": "2026-09-04T13:00:00Z",
    "batch_id": "…",
    "api_version": "1.4"
  },
  "user": {
    "source_uuid": "6106e1d9-dfea-45ae-8b0b-320e9861a898",
    "username": "angrypanda314",
    "email": "harry.burton@example.com",
    "name": { "title": "Mr", "first": "Harry", "last": "Burton", "display_name": "Harry Burton" },
    "location": { "city": "Boise", "country": "United States", "latitude": -73.22, "longitude": -106.75, "utc_offset": "-11:00" },
    "birth_at": "1951-10-14T04:21:30.137Z",
    "registered_at": "2002-09-22T08:20:58.921Z",
    "contact": { "phone": "6532655591", "cell": "4122182946" },
    "avatar_url": "https://randomuser.me/api/portraits/thumb/men/92.jpg"
  }
}
```

**Why this shape (and not a totally flat row):**

- `meta` is a shared header for every future source (chain indexer, price feed, mobile analytics). Consumers can filter on `source` + `schema_version` without knowing the payload.
- Nested `user.name` / `user.location` stay coherent objects. Flattening everything to `user_name_first` works until the next source adds a second address or a list of devices — then you regret it.
- `schema_version` lets a downstream job dual-read v1 and v2 during a migration.
- `source_uuid` is the idempotency key.

**Dropped on purpose:** `login.password`, `salt`, hashes, national ID / SSN, large/medium avatars, timezone description.

## Local run (without Docker for the app)

Requires Go 1.23+ and Postgres 16.

```bash
# Postgres
docker compose up -d postgres

export DATABASE_URL='postgres://etl:etl@localhost:5432/etl?sslmode=disable'
export DATA_DIR=./data
export LOG_PATH=./logs/etl.log
go test ./...
go run ./cmd/etl
```

The process listens on `:8080` and polls immediately, then every 30 seconds.

```bash
curl -s localhost:8080/health
curl -s localhost:8080/metrics | grep etl_
```

Stop with Ctrl+C; the HTTP server drains before exit.

## Docker

The image is a two-stage build: compile in `golang:1.23-alpine`, run a static binary on `alpine:3.20`.

### Compose (recommended)

Compose starts Postgres, waits until it accepts connections, then starts the ETL container with **bind mounts** for the lake and the log file, plus a named volume for Postgres.

```bash
docker compose up --build
```

| Host path | Container path | Why |
| --- | --- | --- |
| `./data` | `/app/data` | Durable raw + processed lake (`data/raw`, `data/processed`) |
| `./logs` | `/app/logs` | Durable `etl.log` |
| volume `postgres_data` | `/var/lib/postgresql/data` | Durable warehouse |

After the first cycle (immediately on boot):

```text
data/raw/dt=YYYY-MM-DD/events.ndjson
data/processed/dt=YYYY-MM-DD/users.ndjson
data/dlq/dt=YYYY-MM-DD/errors.ndjson
logs/etl.log
```

Tear down the app but **keep** Postgres data:

```bash
docker compose down          # keeps postgres_data volume
docker compose down -v       # wipe the warehouse too
```

### `docker run` equivalent (explicit mounts)

```bash
docker build -t trust-wallet-etl .

docker network create etl-net
docker run -d --name etl-pg --network etl-net \
  -e POSTGRES_USER=etl -e POSTGRES_PASSWORD=etl -e POSTGRES_DB=etl \
  -v postgres_data:/var/lib/postgresql/data \
  postgres:16-alpine

docker run --rm --name etl --network etl-net \
  -p 8080:8080 \
  -e DATABASE_URL='postgres://etl:etl@etl-pg:5432/etl?sslmode=disable' \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/logs:/app/logs" \
  trust-wallet-etl
```

The two `-v` flags are what make the lake and logs survive container replacement. Without them, `/app/data` and `/app/logs` die with the container.

## Observability

Logs are JSON via Go `log/slog`, duplicated to stdout (for `docker logs`) and `logs/etl.log`.

Events that the spec asked for:

| Event | Fields |
| --- | --- |
| API success | `msg="api request succeeded" records batch_id api_version` |
| API failure | `msg="api request failed" error` |
| Transform error | `msg="transformation error" error batch_id` (bad records are skipped; the batch continues) |
| Save success | `msg="data saved successfully" sink=postgres_raw\|lake_raw\|postgres_processed\|lake_processed` |

### HTTP

- `GET /health` — `200 {"status":"ok","postgres":"ok"}` or `503` if Postgres is down. Used as a liveness/readiness probe.
- `GET /metrics` — Prometheus text exposition.

### Metrics (and why these)

| Metric | Type | Why it exists |
| --- | --- | --- |
| `etl_extract_total{status}` | counter | Source availability vs. our client bugs. Alert on `failure` rate. |
| `etl_extract_duration_seconds` | histogram | Catch API latency before it blows the 30s budget. |
| `etl_records_extracted_total` | counter | Throughput; should track `FETCH_RESULTS` × successful cycles. |
| `etl_transform_errors_total` | counter | Schema drift. A sudden spike means the vendor changed the JSON. |
| `etl_records_loaded_total{sink}` | counter | Compare sinks. If `lake_raw` lags `postgres_raw`, the disk mount is the problem. |
| `etl_pipeline_runs_total{status}` | counter | End-to-end SLO: `success / (success+failure)`. |
| `etl_pipeline_duration_seconds` | histogram | Cycle time vs. poll interval (must stay ≪ 30s or you overlap). |
| `etl_last_success_timestamp_seconds` | gauge | Freshness. Alert if `time() - gauge > 90`. |

These are the same four questions a production pipeline pager cares about: **did it run, did the source work, did the schema hold, did the sinks accept the write**.

## Configuration

All knobs are environment variables (see `.env.example`).

| Variable | Default | Meaning |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | Bind address for `/health` and `/metrics` |
| `POLL_INTERVAL` | `30s` | Extract cadence |
| `FETCH_RESULTS` | `10` | Users per API call (1–50) |
| `API_URL` | `https://randomuser.me/api/` | Source |
| `API_TIMEOUT` | `15s` | Per-attempt HTTP timeout |
| `DATABASE_URL` | `postgres://etl:etl@localhost:5432/etl?sslmode=disable` | Postgres DSN |
| `DATA_DIR` | `./data` | Lake root |
| `LOG_PATH` | `./logs/etl.log` | Log file |
| `SOURCE_NAME` | `randomuser` | Written into `meta.source` |
| `SCHEMA_VERSION` | `1.0.0` | Written into `meta.schema_version` |

## How I would productionize this

The take-home is a single process writing local files. That is the right size for an interview repo; it is not how Trust Wallet would run user or chain data in production. Below is the path I would take, component by component.

### Ingestion

- **Replace the 30s poll** with an event-driven source wherever the vendor supports it (webhooks, Kafka, Kinesis, Pub/Sub). Polling a public HTTP API does not scale to chain data or high-cardinality wallet activity.
- For sources that remain pull-based, run the extractor as a **Kubernetes Deployment** (or Cloud Run / ECS service) with a leader-elected poll, not a CronJob: CronJobs overlap when a run exceeds the interval.
- Put a **queue** (Kafka / SQS / Pub/Sub) between extract and load. The extractor’s only job is “bytes from vendor → topic”. Transformers scale independently.
- Add **idempotency at the message layer** (`source` + `source_uuid` + `event_time`) so retries are safe. The processed table already upserts on `source_uuid`; keep that pattern.
- Circuit-break and **dead-letter** failed payloads (S3/GCS prefix or a Kafka DLQ topic) instead of blocking the batch. This repo already isolates transform errors per record; production should persist the poison payload.

### Transform

- Keep a **raw zone** untouched (this repo’s `raw_events` + `data/raw`). Replay is cheaper than begging the vendor for history.
- Move heavy transforms to **Spark / Flink / Dataflow** or **dbt** on the warehouse once volume leaves “small JSON files” territory. Go stays as the collector.
- Version schemas with a registry (Buf / JSON Schema / Avro). `schema_version` in `meta` is the foothold for that.
- Classify columns (public / internal / restricted). Anything like SSN or seed phrases is dropped or tokenized **before** the lake, which is what the transformer does with passwords and national IDs.

### Storage

| Take-home | Production |
| --- | --- |
| Local `data/raw` + `data/processed` NDJSON | Object storage: **GCS / S3 / Azure Blob**, partitioned `dt=YYYY-MM-DD/hr=HH/` |
| Append NDJSON | Convert to **Parquet** + **Apache Iceberg or Delta Lake** for schema evolution, time travel, and cheap scans |
| Postgres `raw_events` | Short-retention landing store, or skip and land directly in the lake if the queue is durable |
| Postgres `processed_users` | **Cloud SQL / RDS / AlloyDB** for serving; **BigQuery / Snowflake / Redshift** for analytics. CDC (Datastream / DMS / Debezium) from Postgres → warehouse if you still need an OLTP snapshot |

Postgres here is a serving layer (lookup user by uuid/email). It is not the lake. Mixing the two is the usual way teams paint themselves into a vacuum-and-bloat corner.

### Orchestration and compute

- **Airflow / Dagster / Prefect** for batch backfills and dbt.
- **Flink / Spark Structured Streaming / Dataflow** for the 30s (or sub-second) path.
- Kubernetes HPA / KEDA on Kafka consumer lag, not on CPU, for the transformers.
- For Trust Wallet specifically I would split pipelines by domain: **on-chain** (per-chain indexers → Kafka → lake), **off-chain product events** (mobile/analytics), **reference data** (token lists, FX, spam lists). They have different SLOs and PII profiles.

### Reliability

- **SLO:** freshness (`etl_last_success_timestamp_seconds` analog) and completeness (source count vs. loaded count).
- At-least-once delivery + idempotent upserts. Exactly-once is a warehouse/Iceberg problem, not an HTTP poll problem.
- Multi-AZ Postgres, object storage replication, and a second region for the lake if the pipeline is in the recovery path of a wallet app.
- Backfill runbooks: replay `raw_events` (or the raw lake prefix) through a new transformer version without touching the vendor.
- Secrets in a manager (GCP Secret Manager / AWS SM), never in compose files. Rotate the DB credential independently of the app image.

### Observability

- Keep Prometheus metrics; scrape via Grafana Alloy / OpenTelemetry Collector.
- Ship `etl.log` to Loki / Cloud Logging / ELK. Same JSON keys as this repo so the dashboards transfer.
- Trace each batch with `batch_id` as the trace/correlation id (already logged on every line).
- Alert: no successful cycle in 90s; transform error rate > 1%; sink mismatch (`extracted` vs `loaded`); disk/object write failures; Postgres saturation.
- Synthetic canary: inject a fixture record and assert it appears in the processed table within one interval.

### Security and compliance

- Network: extractor egress allow-list to the vendor; Postgres private IP; lake buckets no public ACL.
- Encryption at rest on the warehouse and the bucket; TLS on every hop.
- PII: the drop-list in the transformer is the start of a data-classification review. A wallet company will also need retention windows and a delete/anonymize path (GDPR) that can tombstone `processed_users` and expire raw objects.

### What I would *not* do

- Run this as a single binary that both polls and serves `/metrics` once there is more than one replica — split collector and HTTP, or use the Prometheus pushgateway / OTLP sidecars.
- Store the lake on a container overlay filesystem.
- Use `FETCH_RESULTS` as a substitute for backpressure. The queue is the buffer; the poller should slow down when lag is high.

## Tests

```bash
go test ./...
```

Coverage is on the pieces that encode design choices: field drops and timestamp coercion (`transformer`), append-not-overwrite (`lake`), and HTTP retries (`extractor`).

## Trade-offs I accepted for the take-home

- One binary, one replica, local disk instead of S3. Matches the brief; productionization is documented rather than implemented.
- Upsert processed users rather than append-only SQL. The **lake** is the history; Postgres is the lookup table.
- NDJSON over CSV. Nested envelopes round-trip without a custom dialect.
- Random User rather than OpenWeather. No API key, and the document is nested enough to justify a schema.
