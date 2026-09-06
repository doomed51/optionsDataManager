# Options Data Manager contributor guide

## Purpose and boundaries

This repository collects option data from Interactive Brokers (IBKR), calculates
implied volatility and Greeks, stores operational data in PostgreSQL, retrieves
historical option bars from ThetaData, and optionally translates the ThetaData
history into a Parquet analytics dataset.

Treat PostgreSQL as the system of record. The Parquet dataset is a rebuildable,
read-optimized projection of `thetadata_option_history`; do not add write paths
that make Parquet authoritative or mutate it in place from analytics code.

Never commit `.env`, API keys, database credentials, generated data, logs, or
local Parquet output. Use `.env.example` for new non-secret configuration.

## Repository map

| Area | Primary files | Responsibility |
| --- | --- | --- |
| Runtime configuration | `config.py`, `.env.example` | IBKR, database, collection symbols, tenor/delta, and ThetaData settings. |
| Persistence | `database.py`, `setup_database.py` | SQLAlchemy models, PostgreSQL connection/session lifecycle, schema creation. |
| IBKR collection | `option_data_collector.py`, `option_greeks_update.py` | Live/historical collection, market-hours scheduling, IV/Greeks, and tenor-delta snapshots. |
| ThetaData retrieval | `thetadata_collector.py`, `thetadata_repair.py` | Provider adapter, discovery, normalized history retrieval, retryable checkpoints, and repair workflows. |
| Parquet translation | `parquet_layer.py`, `parquet_analytics/` | PostgreSQL-to-Parquet extraction, validation, atomic publication, manifest/checkpoint maintenance, and scheduling. |
| Analytics UI/inspection | `parquet_dashboard.py`, `parquet_analytics/inspection.py` | Read-only dataset inspection and Streamlit dashboard. |
| Tests | `tests/` | Unit and integration-style contract tests for ThetaData and Parquet behavior. |

## Local workflow

- Use the repository virtual environment when present: `.\\venv\\Scripts\\python.exe`.
- Install runtime requirements with `pip install -r requirements.txt`; use
  `requirements-dev.txt` where DuckDB test support is required.
- Initialize or evolve tables through `DatabaseManager.create_tables()` / the
  setup workflow. Preserve the compatibility migration in `database.py` when
  changing existing deployments.
- Run focused tests while editing, then the applicable suite:

  ```powershell
  .\\venv\\Scripts\\python.exe -m unittest tests.test_thetadata_collector
  .\\venv\\Scripts\\python.exe -m unittest tests.test_parquet_layer
  .\\venv\\Scripts\\python.exe -m unittest tests.test_parquet_inspection
  ```

- For syntax-only checks, use
  `.\\venv\\Scripts\\python.exe -m py_compile <changed-python-files>`.
- Avoid live IBKR calls, ThetaData requests, production database writes, and
  `parquet_layer.py rebuild` unless the task explicitly calls for them and the
  required credentials/targets have been verified.

## Data and time conventions

- Keep source bar identity stable: ThetaData history is uniquely identified by
  `symbol`, `expiry`, `strike`, `right`, `interval`, and `timestamp`.
- Preserve the existing upsert and checkpoint behavior. A completed
  contract/day/interval/dataset checkpoint must make a rerun safe; unavailable
  provider endpoints must remain retryable with bounded backoff.
- ThetaData client responses are Polars frames. Normalize documented provider
  fields before persistence, retain nullable source values, and convert
  non-finite floats to database-safe nulls.
- Use `C`/`P` as stored option rights; convert to provider `call`/`put` only at
  the ThetaData endpoint boundary.
- Treat source datetimes as UTC. Do not silently reinterpret timestamps in a
  local timezone. The Parquet scheduler's 03:00 run is specifically in
  `America/Toronto`; preserve its DST-aware behavior.
- Calculate DTE from the option expiry and bar timestamp according to the
  existing collector contract. Do not substitute collection time for bar time.

## ThetaData retrieval rules

- `thetadata_collector.py` is the provider-facing adapter. Keep provider SDK
  specifics, endpoint availability checks, rate/backoff handling, and response
  normalization there instead of leaking them into database or UI code.
- Required history datasets are quote, OHLC, open interest, implied volatility,
  and first-order Greeks. Changes to one endpoint's schema must not discard
  valid fields from the others during merge/persistence.
- Discovery is vendor-driven: obtain available dates, expirations, and strikes
  from ThetaData and select the configured subset per trading day. Underlying
  OHLC first comes from the configured SQLite cache and may fall back to IBKR;
  preserve cache-before-network behavior.
- Bound concurrency and request retries. Do not remove endpoint preflight,
  coverage checks, checkpoint writes, or idempotent bulk upserts merely to make
  a backfill faster.
- Update `tests/test_thetadata_collector.py` for any provider argument,
  normalizer, merge, identity, checkpoint, or retry change. Mock the provider;
  tests must not require a real API key.

## Parquet translation and publication rules

- The only supported Parquet source is PostgreSQL
  `thetadata_option_history`. Keep the translation schema in
  `parquet_analytics/schema.py` source-compatible, including UTC timestamps and
  the database column named `lambda`.
- Preserve the Hive layout exactly:

  ```text
  symbol=<symbol>/interval=<interval>/year=YYYY/month=MM/part-NNNNN.parquet
  ```

- A refresh is incremental by `updated_at`; it does not promise to detect
  physical deletes from PostgreSQL. If semantics require delete propagation,
  design and document a new explicit reconciliation contract rather than
  silently changing refresh behavior.
- Writers must remain serialized by the PostgreSQL advisory lock. Stage complete
  monthly partitions, validate them against source rows, then publish with
  same-directory replacement. Preserve the maintenance marker
  `_PARQUET_REFRESH_IN_PROGRESS`, manifest, failure record, staging/backup
  recovery, and resumable rebuild checkpoints.
- Never let readers analyze a root while the maintenance marker exists. The
  dashboard and inspection layer must stay read-only and honor that guard.
- `PARQUET_ROOT` must be absolute and support writes plus same-directory atomic
  rename. `rebuild` can require roughly twice the live dataset's disk space;
  use `config-check`, `refresh`, or a narrow `reconcile` first when appropriate.
- Update Parquet-layer tests when changing partition keys, output schema,
  watermark logic, locking, publication/recovery, rebuild resumption, or
  scheduler timing. Keep DuckDB/R Arrow compatibility in mind.

## Change discipline

- Keep changes scoped to the owning subsystem; add a small interface rather
  than creating circular imports across collector, database, and analytics
  layers.
- Prefer explicit configuration and typed/validated boundaries over implicit
  environment reads scattered through the code.
- Retain existing logging and error context, but never log credentials or full
  secret-bearing connection strings.
- When a database model changes, update its unique constraints/indexes,
  serialization/schema mapping, affected collectors, and tests together.
- When a source field changes, trace it end-to-end: provider normalization ->
  database model/upsert -> Parquet schema/converter -> inspection/dashboard ->
  tests and documentation.