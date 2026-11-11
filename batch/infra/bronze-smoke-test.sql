-- Local write/read check; rerunning retains one row for this ID.
CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze;
CREATE TABLE IF NOT EXISTS lakehouse.bronze._infra_smoke_test (
    id STRING, message STRING, _batch_id STRING
) USING iceberg;

MERGE INTO lakehouse.bronze._infra_smoke_test t
USING (SELECT 'local-smoke-001' AS id, 'Iceberg write/read OK' AS message,
              'local-smoke-001' AS _batch_id) s
ON t.id = s.id
WHEN NOT MATCHED THEN INSERT (id, message, _batch_id) VALUES (s.id, s.message, s._batch_id);

SELECT * FROM lakehouse.bronze._infra_smoke_test WHERE id = 'local-smoke-001';
