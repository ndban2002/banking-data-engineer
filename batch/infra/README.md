# Batch infrastructure: MinIO + Spark + Iceberg

This local Docker Compose stack provides:

- MinIO S3-compatible object storage (Chainguard image) with a persistent `minio-data` volume.
- Apache Iceberg REST fixture backed by file-based SQLite metadata persisted in `iceberg-catalog`.
- A Spark 3.5.6 Python-enabled standalone master and worker with Iceberg 1.9.2 runtime, PostgreSQL JDBC, and AWS S3 FileIO.
- A configured mount for a Bronze smoke SQL script; the script itself is still missing from the repository.

It includes an example JDBC job for loading `transactions` from the simulator into Bronze. The stack is a local learning environment, not production infrastructure. The sample MinIO credentials are for local development; override them in `.env` before exposing any port beyond localhost. Compose binds published ports to `127.0.0.1`.

Review status, 2026-11-11: Compose passes static configuration checks; runtime write/read has not been verified in this review. Before following the startup and smoke commands, add the missing `bronze-smoke-test.sql` referenced by Compose. This is the first task in [milestone M0](../../contracts/DATA_MODELING_AND_INGESTION.md#11-lộ-trình-học-và-tiêu-chí-nghiệm-thu).

## Start

From `batch/infra/`:

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
```

Open the MinIO console at <http://localhost:9001> (credentials from `.env`). Spark master UI is at <http://localhost:8080>; worker UI is at <http://localhost:8081>.

## Verify Iceberg Bronze write/read

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-sql \
  -f /opt/spark/work-dir/bronze-smoke-test.sql
```

The planned script should create a namespace/table, write a sample row with batch ID `local-smoke-001`, and read it back. Document its rerun behavior when implementing it; the command above cannot succeed until the script is added.

## Load simulator transactions to Bronze

Create `batch/infra/.env` from `.env.example`, then replace `SOURCE_PG_USER` and `SOURCE_PG_PASSWORD`. If PostgreSQL is reached through an SSH tunnel on your laptop, keep `SOURCE_PG_HOST=host.docker.internal` and set `SOURCE_PG_PORT` to the tunnel's local port (default `15432`). The loader does not print connection credentials.

Initial full snapshot:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/jobs/load_transactions_bronze.py \
  --mode full
```

Later incremental runs:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/jobs/load_transactions_bronze.py \
  --mode incremental --overlap-minutes 5
```

The job appends observations to `lakehouse.bronze.transactions`. Its watermark is stored in `lakehouse.bronze._ingestion_state` and advances only after the Bronze write succeeds. Repeated observations are expected; Silver deduplication is planned but not implemented. The loader reads a full source snapshot initially, then polls `updated_at`. A five-minute overlap cannot guarantee recovery of changes committed after that window.

The loader currently hardcodes `SOURCE_KEY = "core_sim.transactions"` and one destination table even though the source schema is configurable. Keep one source and one ingestion writer for now; isolate state and destinations before switching source databases/schemas. See the plan for the correction and replay tests.

Inspect the result with:

```sql
SELECT _batch_id, COUNT(*) AS rows, MAX(updated_at) AS latest_source_update
FROM lakehouse.bronze.transactions
GROUP BY _batch_id
ORDER BY _batch_id;
```

To inspect available tables interactively:

```bash
docker compose exec spark-master /opt/spark/bin/spark-sql
```

Then run:

```sql
SHOW NAMESPACES IN lakehouse;
SHOW TABLES IN lakehouse.bronze;
SELECT * FROM lakehouse.bronze.smoke_test;
```

## Stop and reset

```bash
docker compose down
```

The named volumes keep MinIO objects and catalog metadata across restarts. To delete all local lakehouse data and metadata:

```bash
docker compose down -v
```

## PostgreSQL source connectivity

If PostgreSQL is reached through a host SSH tunnel, the Spark container connects to `host.docker.internal:<local-tunnel-port>`; Linux also receives an explicit host-gateway mapping. Credentials belong in the ignored local `.env`, never in committed Compose files.
