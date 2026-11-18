# Batch infrastructure: MinIO + Spark + Iceberg

Stack local gồm MinIO lưu object, Iceberg REST catalog lưu thông tin bảng bằng SQLite, Spark 3.5.6 standalone master/worker, các job JDBC nạp bốn bảng simulator vào Bronze và các job Silver current state/quality. Image Spark có Python, Iceberg 1.9.2, AWS S3 FileIO và PostgreSQL JDBC driver.

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

`customers` và `merchants` không có `updated_at`, nên job mặc định `--mode full` và từ chối `--mode incremental`. Không dùng `created_at` làm watermark vì sẽ bỏ sót thay đổi thuộc tính của bản ghi cũ. Silver current state so sánh theo thời gian quan sát; SCD2 và phát hiện xóa chưa được triển khai.

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

## Silver current state

[silver_loader.py](jobs/silver_loader.py) đọc các batch có manifest từ Bronze và ghi bốn bảng Iceberg. Job không kết nối lại PostgreSQL. Chạy sau khi Bronze đã có ít nhất một batch được chấp nhận cho mỗi bảng:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/jobs/silver_loader.py \
  --table all --bronze-namespace bronze --silver-namespace silver
```

| Nguồn Bronze | Bảng Silver | Khóa | Phiên bản được giữ |
| --- | --- | --- | --- |
| `customers` | `lakehouse.silver.dim_customers` | `customer_id` | Thời gian quan sát batch mới hơn |
| `merchants` | `lakehouse.silver.dim_merchants` | `merchant_id` | Thời gian quan sát batch mới hơn |
| `accounts` | `lakehouse.silver.dim_accounts` | `account_id` | `updated_at` mới hơn |
| `transactions` | `lakehouse.silver.fact_transactions` | `transaction_id` | `updated_at` mới hơn |

Tên được trim, region được trim/uppercase, UUID chuyển lowercase. Tiền VND dùng `BIGINT`; `occurred_at` vẫn là thời gian nghiệp vụ. Giao dịch `PENDING` và `FAILED` hợp lệ vẫn được giữ; lỗi nghiệp vụ `INVALID_REFUND` không tự biến một dòng thành dữ liệu hỏng. Kiểm tra schema từ chối kiểu tiền float/decimal hoặc timestamp bị đổi kiểu, thay vì ép kiểu làm mất dữ liệu.

Mỗi batch được đọc từ đúng snapshot Iceberg ghi trong manifest, lọc `_batch_id` và kiểm tra provenance/số dòng. Batch chưa có manifest bị bỏ qua. Hai quan sát cùng khóa, cùng phiên bản nhưng khác nội dung bị quarantine; batch cũ không được ghi đè phiên bản mới. `request_id` phải duy nhất giữa các transaction và không được đổi cho transaction đã có.

Các bảng điều khiển:

- `lakehouse.silver._quarantine`: dòng không hợp lệ, JSON gốc, mã lỗi, batch và snapshot nguồn; ID ổn định giúp replay không nhân đôi lỗi.
- `lakehouse.silver._processed_batches`: checkpoint processing riêng với watermark Bronze, kèm fingerprint manifest, số dòng đầu vào/upsert/quarantine. `upsert_rows` là số dòng ghi trong lần xử lý đó, không phải tổng số dòng hợp lệ của nguồn.

Thứ tự commit là **Silver → quarantine → checkpoint**. Nếu ghi lỗi giữa chừng, chạy lại sẽ xử lý batch chưa có checkpoint và merge theo khóa. Batch có lỗi DQ vẫn được đánh dấu đã xử lý sau khi quarantine đã lưu. CLI trả mã `1` nếu có dòng bị quarantine trong batch xử lý ở lần chạy này; batch lịch sử được skip vẫn xuất trong báo cáo nhưng không làm lần chạy mới thất bại. Mã `0` không thay thế việc xem quarantine và chạy quality gate trước Gold.

Chạy lại mặc định chỉ xử lý batch chưa hoàn thành. Để kiểm tra replay một batch đã xử lý:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/jobs/silver_loader.py \
  --table transactions --batch-id '<batch_id_trong_manifest>' --replay
```

Chạy **một writer tại một thời điểm cho cả namespace Silver**; chưa có khóa điều phối. Mỗi Silver namespace gắn với một Bronze namespace và một dataset nguồn. Không chỉnh trực tiếp các bảng Silver/control; để rebuild hoặc đổi quy tắc xử lý, dùng namespace Silver mới và giữ đủ snapshot Bronze cần replay. Snapshot đã hết hạn khiến job dừng, không tự đọc dữ liệu hiện tại để thay thế.

Đây là **current state**: chưa có SCD2, chưa suy ra hard delete từ dòng vắng mặt trong full snapshot. Snapshot rỗng không xóa trạng thái đang có. Các commit trên bảng khác nhau không phải một transaction chung; downstream chỉ sử dụng kết quả sau khi hoàn tất nạp và kiểm tra chất lượng.

## Quality gate Silver

Sau khi dừng simulator, hoàn tất full/reconciliation Bronze và nạp đủ bốn bảng Silver, chạy:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/jobs/silver_quality.py --namespace silver
```

Gate chỉ đọc và cố định snapshot của từng bảng trong báo cáo: kiểm tra uniqueness, chủ sở hữu tài khoản, foreign key, hướng payment/refund thành công, một refund thành công cho mỗi payment và **đối soát số dư từng tài khoản** từ opening balance cộng/trừ giao dịch thành công. Cộng dồn dùng `DECIMAL(38,0)` để tránh tràn `BIGINT`. Có vi phạm thì trả mã `1`; bảng thiếu hoặc binding sai cũng làm job thất bại.

Kiểm tra liên bảng chạy riêng vì bốn lần JDBC không dùng chung snapshot nguồn. Gate không chứng minh được shared source cutoff, không xác minh channel lịch sử của merchant từ dimension hiện tại, và không tự phê duyệt lỗi quarantine lịch sử; các giới hạn này có trong `not_checked` của báo cáo. `PENDING` được đếm riêng để người chạy quyết định nguồn đã ổn định hay chưa.

Xem các batch lỗi và nguyên nhân trong Spark SQL:

```sql
SELECT bronze_table, batch_id, input_rows, upsert_rows, quarantined_rows
FROM lakehouse.silver._processed_batches
ORDER BY processed_at DESC;

SELECT source_table, batch_id, record_id, reason_codes
FROM lakehouse.silver._quarantine;
```

## Gold — daily merchant performance

[gold_daily_merchant.py](jobs/gold_daily_merchant.py) tạo bảng
`lakehouse.gold.daily_merchant_performance`, grain **merchant × ngày nghiệp vụ Việt Nam**.
Chạy từ `batch/infra/`, sau khi hoàn tất Silver; dừng simulator khi reconciliation
và không chạy Silver writer đồng thời với Gold:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/jobs/gold_daily_merchant.py \
  --silver-namespace silver --gold-namespace gold
```

Job cố định snapshot của bốn bảng Silver, chạy 16 kiểm tra chất lượng trên **đúng
các snapshot đó**, kiểm tra binding và checkpoint processing trước khi tính mart.
Quarantine Silver phải rỗng; lỗi lịch sử cũng chặn Gold trong baseline này. Sau khi
sửa quy tắc/nguồn, rebuild Silver trong namespace mới để giữ nguyên audit cũ rồi
chạy Gold với namespace mới tương ứng. Không xóa lỗi audit để vượt gate.
Dòng Silver đã ghi nhưng chưa có checkpoint cũng chặn Gold: retry Silver trước.

| Cột | Định nghĩa |
| --- | --- |
| `business_date` | DATE của `occurred_at` theo `Asia/Ho_Chi_Minh`, không dùng ngày ingest |
| `merchant_id` | PAYMENT: merchant của destination account; REFUND: merchant của source account |
| `payment_count`, `payment_amount_vnd` | Số transaction / tổng VND PAYMENT SUCCEEDED |
| `refund_count`, `refund_amount_vnd` | Số transaction / tổng VND REFUND SUCCEEDED, ghi ngày refund |
| `net_amount_vnd` | Payment amount trừ refund amount; có thể âm khi ngày chỉ có refund |
| `failed_payment_count`, `failed_refund_count` | Số transaction FAILED, không cộng tiền |
| `pending_payment_count`, `pending_refund_count` | Số transaction PENDING, không cộng tiền |
| `transaction_count` | Tổng số transaction mọi trạng thái của merchant/ngày |
| `_input_id`, `_source_snapshots` | Fingerprint và snapshot Silver đầu vào để tái hiện |

Silver bảo đảm transaction ID duy nhất; counts đo transaction, không đo lần gọi
retry request. Các tổng tiền là `DECIMAL(38,0)` để tổng vượt giới hạn BIGINT vẫn
chính xác; overflow làm job thất bại. Merchant không có hoạt động không sinh dòng
0. Chưa join tên/MCC/region vào mart để tránh thể hiện thuộc tính hiện tại như lịch
sử; có thể join dimension hiện tại khi hiển thị, với ý nghĩa được ghi rõ.

Một request FAILED/PENDING có thể sai hướng tài khoản nên không gán được merchant
theo quy tắc trên. Các dòng này vào bucket `merchant_id='__unattributed__'` theo
ngày thay vì bị inner join loại mất. Transaction SUCCEEDED không gán được merchant
làm job thất bại. Quality pass chưa chứng minh shared source cutoff; PENDING vẫn
được báo riêng và không tự chặn Gold.

**Baseline dùng full recompute**, chưa tối ưu chỉ xử lý ngày thay đổi. Mỗi lần
đọc lại toàn bộ fact current state, tính lại mọi merchant/ngày, đối chiếu với một
SQL aggregation riêng trên cùng input, rồi ghi toàn bộ mart bằng một Iceberg
overwrite. Vì vậy late arrival, thay đổi trạng thái/ngày, và nhóm cũ biến mất đều
được sửa; replay không cộng thêm tiền. Bảng partition theo `business_date`, nhưng
job không chỉ overwrite partition “hôm nay”. Giới hạn là chi phí scan toàn lịch sử;
bài tiếp theo mới tối ưu affected dates sau khi có thay đổi nguồn đáng tin cậy.
[Iceberg mô tả overwrite là thao tác nguyên tử trên bảng](https://iceberg.apache.org/docs/1.9.2/spark-writes/).

Sau ghi, job đọc lại đúng snapshot output và so với dữ liệu đã kiểm tra, rồi ghi
`lakehouse.gold._materializations`: run ID, input fingerprint, snapshot nguồn,
snapshot output, số dòng và tổng đối soát. Một writer cho mỗi Gold namespace.
Mart và ledger là **hai commit riêng**; nếu crash sau overwrite, dữ liệu mới có
thể đã thấy được nhưng chưa có run hoàn thành. Retry sẽ rebuild, không append
delta. Mỗi lần chạy thành công có một dòng ledger; cùng input có cùng `_input_id`.
Downstream publish dùng `output_snapshot_id` của run hoàn thành, không mặc định
snapshot mới nhất của mart là bản đã được ghi nhận trong ledger. Giữ các snapshot
cần dùng; chưa tự động hóa retention hoặc ClickHouse publication.

Ví dụ Spark SQL:

```sql
SELECT business_date, merchant_id, payment_count, refund_count,
       payment_amount_vnd, refund_amount_vnd, net_amount_vnd,
       failed_payment_count, failed_refund_count
FROM lakehouse.gold.daily_merchant_performance
ORDER BY business_date DESC, merchant_id;

SELECT run_id, input_id, row_count, output_snapshot_id, reconciliation
FROM lakehouse.gold._materializations
ORDER BY completed_at DESC;
```

Để đối chiếu PostgreSQL cùng cutoff, dùng SQL read-only
[daily_merchant_baseline.sql](../../data-source/sql/daily_merchant_baseline.sql).
Runtime test dùng fixture độc lập; nó không thay thế đối chiếu nguồn PostgreSQL
đang dùng, và job Gold không tự kết nối PostgreSQL.

## Kiểm thử code và nghiệm thu runtime

Từ root repo, với Python 3.10+ và pytest đã cài:

```bash
python -m pytest batch/infra/tests -q
```

Bộ test nhanh kiểm tra Bronze lifecycle, manifest/fingerprint Silver, schema và các quy tắc đối soát. Test runtime Silver mặc định được skip trong pytest; chạy riêng trên Spark/Iceberg thật bằng lệnh sau (từ `batch/infra/`):

```bash
docker compose exec -T spark-master sh -c 'cat > /tmp/test_silver_runtime.py' \
  < tests/test_silver_runtime.py
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master 'local[2]' /tmp/test_silver_runtime.py
```

Script tạo namespace riêng `silver_test_b_<uuid>` và `silver_test_s_<uuid>`, dùng dữ liệu tổng hợp rồi xóa đúng các bảng/namespace do test tạo. Không đọc PostgreSQL hoặc bảng nghiệp vụ hiện có. Các ca kiểm thử gồm accepted manifest, replay không lùi trạng thái, quarantine không trùng, snapshot rỗng, conflict cùng phiên bản, request ID trùng, thiếu snapshot, sai nguồn, lỗi trước checkpoint và quality gate bốn bảng. Đây là kiểm chứng Silver trên Iceberg; nghiệm thu JDBC từ nguồn thực vẫn cần quy trình bên dưới.

Test Gold dùng cùng fixture Bronze/Silver và namespace `gold_test_<uuid>` riêng.
Copy cả hai file vào cùng thư mục container rồi chạy:

```bash
docker compose exec -T spark-master sh -c 'cat > /tmp/test_silver_runtime.py' \
  < tests/test_silver_runtime.py
docker compose exec -T spark-master sh -c 'cat > /tmp/test_gold_runtime.py' \
  < tests/test_gold_runtime.py
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master 'local[2]' /tmp/test_gold_runtime.py
```

Các ca kiểm thử Gold có expected metric cụ thể: UTC sát nửa đêm Việt Nam,
payment ngày D/refund ngày D+1, FAILED/PENDING, unknown merchant, replay,
late arrival, nhóm ngày cũ biến mất, gate chặn publication, snapshot input cố
định, crash trước ledger, tổng tiền vượt BIGINT và empty input xóa các nhóm cũ. Test dọn đúng namespace
do nó tạo; không ghi vào dataset đang dùng.

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
