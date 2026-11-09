# Batch infrastructure: MinIO + Spark + Iceberg

This local Docker Compose stack provides:

- MinIO S3-compatible object storage (Chainguard image) with a persistent `minio-data` volume.
- Apache Iceberg REST fixture backed by file-based SQLite metadata persisted in `iceberg-catalog`.
- A Spark 3.5.6 Python-enabled standalone master and worker with Iceberg 1.9.2 runtime, PostgreSQL JDBC, and AWS S3 FileIO.
- A smoke SQL script that creates a Bronze namespace/table, writes one sample row, then reads it back.

It includes an example JDBC job for loading `transactions` from the simulator into Bronze. The stack is a local learning environment, not production infrastructure. The sample MinIO credentials are for local development; override them in `.env` before exposing any port beyond localhost. Compose binds published ports to `127.0.0.1`.

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

The query should show the row with batch ID `local-smoke-001`. The script uses `CREATE TABLE IF NOT EXISTS` and appends one row each time it is run, so repeated runs intentionally add another smoke row.

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

The job appends observations to `lakehouse.bronze.transactions`. Its watermark is stored in `lakehouse.bronze._ingestion_state` and advances only after the Bronze write succeeds. The overlap makes extraction at-least-once: repeated observations are expected and Silver will deduplicate by transaction ID and `updated_at`. The loader reads a full source snapshot initially; incremental extraction uses the confirmed `updated_at` behavior.

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
