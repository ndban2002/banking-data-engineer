# Batch — Phase 1

Thư mục dành cho phase batch lakehouse. Hạ tầng local MinIO + Spark + Iceberg REST và job nạp `transactions` từ PostgreSQL vào Bronze nằm trong [infra](infra/README.md).

Luồng dự kiến: PostgreSQL nguồn từ [simulator](../data-source/README.md) → Spark batch → Bronze → Silver → Gold trên MinIO và Apache Iceberg.

Hiện có loader JDBC cho `transactions`, full/incremental load, metadata và watermark sau khi ghi Bronze. Chưa có ingestion cho ba bảng còn lại, Silver, Gold, kiểm thử batch hoặc orchestrator. Compose và code chưa đồng nghĩa pipeline đã được nghiệm thu.

Hướng triển khai: Spark jobs chạy độc lập → Silver current state và một Gold mart có đối soát → **Dagster OSS assets**, quality gates, retry và backfill → **ClickHouse analytics → Grafana**. Dagster gọi Spark jobs; Bronze/Silver/Gold vẫn nằm trong Iceberg. Mỗi bảng chỉ có một pipeline chịu trách nhiệm ghi. Trino và dbt là các bài mở rộng tùy chọn.

Sau khi batch đạt nghiệm thu, publish một snapshot Gold đã kiểm tra vào bảng native ClickHouse để phục vụ dashboard. Bài đầu dùng mart nhỏ, nạp toàn bộ snapshot vào staging và kiểm tra trước khi đổi bảng phục vụ; kiểm thử retry/crash để tránh công bố dữ liệu nạp dở hoặc cộng trùng. ClickHouse chưa được cài hoặc có loader. Xem milestone M5 trong plan; chưa cần kết nối ClickHouse đọc trực tiếp Iceberg ở chặng này.

Tài liệu [Data Modeling & Ingestion Strategy](../contracts/DATA_MODELING_AND_INGESTION.md) có đánh giá hiện trạng, mô hình nguồn, chiến lược nạp và checklist học tập. Source-to-target mapping YAML theo từng bảng nằm trong [contracts/mappings/postgresql](../contracts/mappings/postgresql/). Mapping Silver/Gold vẫn là thiết kế; Bronze transactions đã có code.

Xem [tổng quan project](../README.md).
