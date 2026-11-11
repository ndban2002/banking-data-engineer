# Batch infrastructure: MinIO + Spark + Iceberg

Stack local gồm MinIO lưu object, Iceberg REST catalog lưu thông tin bảng bằng SQLite, Spark 3.5.6 standalone master/worker và các job JDBC nạp bốn bảng simulator vào Bronze. Image Spark có Python, Iceberg 1.9.2, AWS S3 FileIO và PostgreSQL JDBC driver.

Dữ liệu MinIO nằm trong volume `minio-data`; SQLite catalog nằm trong `iceberg-catalog`. Các cổng publish bind vào `127.0.0.1`. Đây là môi trường học local; thông tin kết nối PostgreSQL và credentials MinIO cấu hình trong `.env` được Git bỏ qua.

## Khởi động

Chạy các lệnh từ `batch/infra/`. Nếu chưa có `.env`, tạo từ [.env.example](.env.example) và điền `SOURCE_PG_USER`, `SOURCE_PG_PASSWORD`; giữ nguyên file `.env` đang dùng nếu đã có.

```bash
docker compose up --build -d
docker compose ps -a
```

`create-warehouse` là job tạo bucket một lần: kết thúc với mã `0` là bình thường. MinIO console ở <http://localhost:9001>, Spark master UI ở <http://localhost:8080>, worker UI ở <http://localhost:8081>.

Các file trong `jobs/` được bind mount vào container. Sửa Python job không cần build image lại. Khi sửa Dockerfile hoặc template Spark đã được `COPY` vào image, chạy lại `docker compose up --build -d`.

## Kiểm tra Iceberg đọc/ghi

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-sql \
  -f /opt/spark/work-dir/bronze-smoke-test.sql
```

Script dùng bảng riêng `lakehouse.bronze._infra_smoke_test`, tạo bảng nếu chưa có và `MERGE` dòng mẫu theo khóa ổn định. Chạy lại không cộng thêm bản sao của dòng mẫu; các bảng nguồn Bronze không bị thay đổi.

## Các job Bronze

| Nguồn PostgreSQL | Job | Bảng đích mặc định | Cách nạp tiếp theo |
| --- | --- | --- | --- |
| `customers` | `load_customers_bronze.py` | `lakehouse.bronze.customers` | Full snapshot mỗi lần |
| `merchants` | `load_merchants_bronze.py` | `lakehouse.bronze.merchants` | Full snapshot mỗi lần |
| `accounts` | `load_accounts_bronze.py` | `lakehouse.bronze.accounts` | Incremental theo `updated_at` |
| `transactions` | `load_transactions_bronze.py` | `lakehouse.bronze.transactions` | Incremental theo `updated_at` |

Bốn entrypoint dùng chung [bronze_loader.py](jobs/bronze_loader.py) cho JDBC, metadata, ghi Iceberg, manifest và watermark. Source-to-target mapping trong [contracts/mappings/postgresql](../../contracts/mappings/postgresql/) vẫn là đặc tả thiết kế; job chưa đọc YAML để tự sinh phép biến đổi.

`customers` và `merchants` không có `updated_at`, nên job mặc định `--mode full` và từ chối `--mode incremental`. Không dùng `created_at` làm watermark vì sẽ bỏ sót thay đổi thuộc tính của bản ghi cũ. Việc so sánh snapshot, SCD2 và phát hiện xóa sẽ được triển khai ở Silver.

`accounts` và `transactions` hỗ trợ `--mode full` hoặc `--mode incremental` (mặc định). Khi chưa có watermark, incremental đọc toàn bộ nguồn; sau đó lọc `updated_at >= watermark - overlap`. Overlap mặc định là 5 phút, có thể thay bằng `--overlap-minutes`.

**Full luôn append một lần quan sát mới**, không overwrite bảng Bronze. Số dòng toàn bảng Bronze có thể lớn hơn số dòng nguồn; phải chọn `_batch_id` khi đối chiếu một lần nạp.

## Nạp full lần đầu

Chạy tuần tự bốn job; vòng lặp dừng khi một job thất bại:

```bash
(
for table in customers merchants accounts transactions; do
  docker compose exec -T spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    "/opt/spark/work-dir/jobs/load_${table}_bronze.py" \
    --mode full || exit 1
done
)
```

Hoặc chạy từng bảng, ví dụ accounts:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/jobs/load_accounts_bronze.py \
  --mode full
```

## Các lần nạp tiếp theo

Customers và merchants tiếp tục đọc snapshot đầy đủ:

```bash
(
for table in customers merchants; do
  docker compose exec -T spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    "/opt/spark/work-dir/jobs/load_${table}_bronze.py" \
    --mode full || exit 1
done
)
```

Accounts và transactions chỉ đọc vùng thay đổi có overlap:

```bash
(
for table in accounts transactions; do
  docker compose exec -T spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    "/opt/spark/work-dir/jobs/load_${table}_bronze.py" \
    --mode incremental --overlap-minutes 5 || exit 1
done
)
```

Mỗi lần chạy job có một `_batch_id` và thời điểm `_ingested_at` chung cho toàn batch; `_ingestion_date` lấy từ thời điểm đó theo UTC. Các cột nguồn được giữ nguyên tên và giá trị; không đổi `balance`/`amount` sang số thực, không đổi `occurred_at` thành thời gian nạp.

## Manifest, watermark và chạy lại

Trình tự hoàn thành một batch:

1. Đọc nguồn qua JDBC và ghi batch vào bảng Bronze.
2. Đọc lại batch từ snapshot Iceberg đã commit và đối chiếu số dòng. Ghi manifest vào `lakehouse.bronze._ingestion_batches`, gồm định danh nguồn, batch, bảng đích, chế độ, số dòng, thời gian quan sát/hoàn thành, snapshot Iceberg và watermark của dữ liệu đã ghi.
3. Với accounts/transactions, cập nhật `lakehouse.bronze._ingestion_state` sau khi cả dữ liệu và manifest đã bền vững.

Snapshot rỗng vẫn cần manifest để phân biệt “đã đọc và không có dòng” với “job chưa chạy hoặc thất bại”. Customers/merchants không tạo watermark theo `created_at`.

Ba lần ghi Bronze, manifest và state là các commit riêng. Nếu job dừng giữa các bước, lần chạy tiếp có thể nạp lại dữ liệu đã ghi. Bronze dùng semantics **at-least-once**; chưa có exactly-once hoặc tự động dọn batch chưa có manifest. Silver phải đọc các batch có manifest và xử lý replay/deduplicate theo thiết kế. Chỉ nâng watermark khi các bước trước thành công, để lỗi ghi không đẩy checkpoint vượt dữ liệu đã lưu.

Mỗi namespace dùng **một nguồn PostgreSQL và một job ghi tại một thời điểm**. Các job trên bốn bảng chạy tuần tự; chưa có khóa điều phối nhiều writer. Bốn lần đọc JDBC cũng không phải một snapshot đồng thời trên cả PostgreSQL, nên đối soát liên bảng cần dừng simulator hoặc thống nhất mốc đối soát.

Overlap 5 phút là dung sai, không bảo đảm thu được mọi thay đổi: một transaction PostgreSQL commit muộn hơn cửa sổ có thể bị bỏ sót dù `updated_at` đã được gán trước đó. Cần full reconciliation định kỳ hoặc CDC để xử lý giới hạn này. Polling cũng không thu được mọi trạng thái trung gian hay hard delete.

## Phân biệt nguồn và namespace đích

Checkpoint key gồm `[system, database, schema, table]`, được mã hóa JSON trong cột `source_table` của `_ingestion_state`. Mỗi bảng có watermark riêng. Bảng Bronze mới được gắn thuộc tính định danh nguồn để phát hiện việc dùng nhầm cùng đích cho nguồn khác.

Để nạp dataset độc lập, dùng `--namespace`, ví dụ:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/jobs/load_customers_bronze.py \
  --mode full --namespace bronze_lab2
```

Cả bảng dữ liệu và bảng manifest/state sẽ nằm trong namespace đó. Khi chạy các bảng còn lại của cùng dataset, dùng cùng `--namespace`. Nếu đổi server PostgreSQL nhưng vẫn giữ database/schema trùng tên, cũng dùng namespace mới: checkpoint không dùng host/port vì địa chỉ SSH tunnel có thể thay đổi.

Loader transactions cũ ghi key `core_sim.transactions`. Loader mới không dùng lại key thiếu database/system này; lần incremental đầu tiên với key mới sẽ đọc full và append. Với bảng Bronze cũ chưa có thuộc tính định danh nguồn, giữ nguyên nguồn trong `.env` khi nâng cấp; job không thể suy ra database lịch sử từ metadata cũ. Nếu không chắc bảng cũ thuộc nguồn nào, dùng namespace mới.

Nếu bảng Bronze bị xóa nhưng watermark còn, incremental sẽ dừng để tránh dựng lại bảng chỉ với phần dữ liệu gần đây. Dùng `--mode full` để nạp lại toàn bộ cùng nguồn. Nếu nguồn đã được reset hoặc khôi phục từ backup cũ, dùng namespace mới vì watermark cũ có thể vượt thời điểm cập nhật trong bộ dữ liệu được khôi phục.

Manifest mới chỉ chứng nhận các batch do loader mới ghi; dữ liệu transactions cũ chưa có manifest sẽ không được coi là batch đã chấp nhận. Lần full đầu tiên lấy trạng thái hiện tại của nguồn, không tạo lại lịch sử quan sát đã có từ trước.

## Kiểm thử code và nghiệm thu runtime

Từ root repo, với Python 3.10+ và pytest đã cài:

```bash
python -m pytest batch/infra/tests -q
```

Bộ test kiểm tra chiến lược từng bảng, SQL incremental/UTC, định danh nguồn và thứ tự append → kiểm tra dữ liệu đã ghi → manifest → watermark, kể cả lỗi ghi hoặc snapshot rỗng. Các kiểm thử vòng đời dùng đối tượng Spark giả, không thay thế nghiệm thu JDBC/Iceberg. Docker daemon không chạy trong môi trường sửa code này nên chưa kiểm chứng runtime các job mới.

Khi stack chạy, dùng namespace thử nghiệm và chạy tuần tự: full bốn bảng → kiểm tra manifest/số dòng → sửa tên merchant hoặc region customer → full lại hai dimension → sinh giao dịch simulator → incremental accounts/transactions. Chạy lại để quan sát overlap, kiểm tra số dư VND vẫn là `BIGINT`, các trường chủ tài khoản null vẫn được giữ, và watermark riêng cho từng bảng. Dừng simulator trong lúc đối soát liên bảng. Với snapshot rỗng, dùng một schema nguồn thử nghiệm đầy đủ DDL nhưng chưa seed; không xóa dữ liệu đang dùng để thử.

## Xem kết quả

Mở Spark SQL:

```bash
docker compose exec spark-master /opt/spark/bin/spark-sql
```

```sql
SHOW TABLES IN lakehouse.bronze;

SELECT batch_id, source_key, target_table, mode, row_count,
       observed_at, completed_at, snapshot_id, watermark
FROM lakehouse.bronze._ingestion_batches
ORDER BY observed_at DESC, completed_at DESC;

SELECT source_table, watermark, updated_at
FROM lakehouse.bronze._ingestion_state
ORDER BY source_table;
```

Đối chiếu số dòng của từng batch accounts với manifest:

```sql
SELECT b.batch_id, b.mode, b.row_count AS extracted_rows,
       COUNT(a._batch_id) AS bronze_rows
FROM lakehouse.bronze._ingestion_batches b
LEFT JOIN lakehouse.bronze.accounts a ON a._batch_id = b.batch_id
WHERE b.target_table = 'lakehouse.bronze.accounts'
GROUP BY b.batch_id, b.mode, b.row_count, b.observed_at
ORDER BY b.observed_at DESC;
```

Đọc snapshot customers gần nhất bằng manifest; snapshot rỗng sẽ trả về 0 dòng:

```sql
WITH latest_batch AS (
  SELECT batch_id
  FROM lakehouse.bronze._ingestion_batches
  WHERE target_table = 'lakehouse.bronze.customers'
  ORDER BY observed_at DESC, completed_at DESC
  LIMIT 1
)
SELECT c.*
FROM lakehouse.bronze.customers c
JOIN latest_batch b ON c._batch_id = b.batch_id;
```

## Kết nối PostgreSQL qua SSH tunnel

Tunnel phải đang chạy trước khi submit job. Container kết nối tới `host.docker.internal:<SOURCE_PG_PORT>`; mặc định port là `15432`. Địa chỉ này trỏ về host Docker, khác với `localhost` trong container. Đảm bảo cả driver và worker truy cập được tunnel; trên Linux cần host-gateway mapping và địa chỉ lắng nghe của tunnel phù hợp mạng Docker. Credentials để trong `.env`, không ghi vào code hoặc commit lên Git.

## Dừng và reset

```bash
docker compose down
```

Các named volume giữ object MinIO và catalog qua lần dừng này. Lệnh sau xóa cả dữ liệu và metadata lakehouse local:

```bash
docker compose down -v
```
