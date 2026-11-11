# Streaming — Phase 2

Thư mục dành cho phần xử lý realtime. Chưa có pipeline được triển khai.

Luồng dự kiến: PostgreSQL nguồn từ [simulator](../data-source/README.md) → **Debezium → Kafka → Spark Structured Streaming → ClickHouse analytics → Grafana**. Lưu raw CDC vào bảng Iceberg riêng để replay; không trộn observation JDBC và CDC envelope thành cùng một schema.

Chỉ bắt đầu sau khi batch đã có Silver/Gold, đối soát, kiểm thử replay và ClickHouse batch serving đã đạt milestone M5. Phần streaming là milestone M6; các job, cấu hình connector, dependency và test sẽ được bổ sung tại đây.

- CDC là đường sự kiện chuẩn đầu tiên; chưa thêm QR producer độc lập cho cùng giao dịch.
- Một transaction có nhiều change event. Không dedup chỉ theo `transaction_id` trước khi xử lý `PENDING → SUCCEEDED`, vì có thể loại mất kết quả cuối.
- `foreachBatch` có thể ghi lại batch sau lỗi. ClickHouse cần khóa/version ổn định từ CDC, không giả định có `ON CONFLICT` hoặc transaction như PostgreSQL. Nếu dùng `ReplacingMergeTree`, query phải lấy trạng thái cuối bằng `FINAL` hoặc `argMax` với thứ tự version xác định rồi mới lọc trạng thái/delete và tính tiền; không chờ background merge để bảo đảm kết quả.
- Không cộng mọi CDC event vào doanh thu bằng incremental materialized view: merge/dedup ở bảng nguồn không tự hoàn tác số tiền view đã cộng. Bài đầu tính metric từ latest-state query đã kiểm thử.
- Archive và serving có checkpoint riêng; nếu ghi nhiều sink trong một job phải xử lý và kiểm thử failure giữa hai commit. Không giả định có transaction xuyên Kafka, Iceberg và ClickHouse.
- Dữ liệu realtime là tạm tính. Batch marts và realtime tables trong ClickHouse tách riêng, cùng công thức và cùng cutoff khi đối soát; sửa lại ngày cũ nếu có dữ liệu đến muộn.
- Stream chạy như service có lifecycle/checkpoint riêng. Dagster điều phối batch, backfill và đối soát; không xử lý từng Kafka event.

Xem [plan và tiêu chí nghiệm thu](../contracts/DATA_MODELING_AND_INGESTION.md#11-lộ-trình-học-và-tiêu-chí-nghiệm-thu). Cơ chế checkpoint/`foreachBatch` theo [Spark 3.5.6](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch).

Ngữ nghĩa ClickHouse cần kiểm chứng theo [ReplacingMergeTree](https://clickhouse.com/docs/concepts/features/operations/update/replacing-merge-tree) và [incremental materialized views](https://clickhouse.com/docs/concepts/features/materialized-views/incremental-materialized-view). PostgreSQL vẫn là nguồn xử lý nghiệp vụ; ClickHouse phục vụ truy vấn phân tích.

Xem [tổng quan project](../README.md).
