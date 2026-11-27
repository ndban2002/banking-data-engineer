# Batch — Phase 1

Thư mục dành cho phase batch lakehouse. Hạ tầng local MinIO + Spark + Iceberg REST và các job nạp `customers`, `merchants`, `accounts`, `transactions` từ PostgreSQL vào Bronze nằm trong [infra](infra/README.md).

Luồng: PostgreSQL nguồn từ [simulator](../data-source/README.md) → Spark batch → Bronze → Silver current state → Gold daily merchant trên MinIO và Apache Iceberg.

Hiện có loader JDBC dùng chung cho bốn bảng: customers/merchants đọc full snapshot mỗi lần; accounts/transactions hỗ trợ full rồi incremental theo `updated_at`. Mỗi batch append vào Iceberg, thêm metadata, ghi manifest và chỉ cập nhật watermark sau khi dữ liệu cùng manifest đã lưu thành công. Silver chỉ đọc batch có manifest, chuẩn hóa và dedup vào `dim_customers`, `dim_merchants`, `dim_accounts`, `fact_transactions`; có quarantine, checkpoint riêng và quality gate đối soát từng tài khoản. Gold tính payment/refund/net theo merchant và ngày Việt Nam, kèm số đếm FAILED/PENDING; full rebuild xử lý late correction và replay. Xem [hướng dẫn chạy Gold và giới hạn](infra/README.md#gold--daily-merchant-performance) trước khi nghiệm thu pipeline.

[Dagster OSS](orchestrator/README.md) điều phối các Spark job Bronze → Silver → quality gate → Gold, ghi batch ID, snapshot và kết quả đối soát trong UI. Pipeline ghi Bronze, Silver và Gold vào Iceberg. Mỗi bảng có một pipeline chịu trách nhiệm ghi. ClickHouse và Grafana thuộc bước tiếp theo.

Hai [bảng Gold tổng hợp](infra/README.md#gold--tổng-quan-ngày-và-xếp-hạng-tháng) lấy cùng snapshot merchant/ngày đã đối soát để tạo KPI hệ thống theo ngày và xếp hạng merchant theo tháng.

Sau khi batch đạt nghiệm thu, publish một snapshot Gold đã kiểm tra vào bảng native ClickHouse để phục vụ dashboard. Bài đầu dùng mart nhỏ, nạp toàn bộ snapshot vào staging và kiểm tra trước khi đổi bảng phục vụ; kiểm thử retry/crash để tránh công bố dữ liệu nạp dở hoặc cộng trùng. ClickHouse chưa được cài hoặc có loader. Xem milestone M5 trong plan; chưa cần kết nối ClickHouse đọc trực tiếp Iceberg ở chặng này.

Tài liệu [Data Modeling & Ingestion Strategy](../contracts/DATA_MODELING_AND_INGESTION.md) có mô hình nguồn, chiến lược nạp và checklist học tập. Source-to-target mapping YAML theo từng bảng nằm trong [contracts/mappings/postgresql](../contracts/mappings/postgresql/). Mapping vẫn là đặc tả thiết kế, chưa được thực thi tự động từ YAML; Bronze và Silver current state của cả bốn bảng đã có code.

Xem [tổng quan project](../README.md).

Để học vai trò của Loader, Transform và Quality, mở [hướng dẫn Silver tương tác](docs/silver-layer-guide.html) bằng trình duyệt. File HTML chạy offline, có mô phỏng replay, quarantine, retry và đối soát số dư.
