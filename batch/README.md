# Batch — Phase 1

Thư mục dành cho phase batch lakehouse. Hạ tầng local MinIO + Spark + Iceberg REST và job nạp `transactions` từ PostgreSQL vào Bronze nằm trong [infra](infra/README.md).

Luồng dự kiến: PostgreSQL nguồn từ [simulator](../data-source/README.md) → Spark batch → Bronze → Silver → Gold trên MinIO và Apache Iceberg.

Các job trích xuất, model, DAG điều phối, dependency và test của phase này sẽ được bổ sung tại đây. Hạ tầng dùng chung với streaming sẽ được tổ chức riêng khi triển khai.

Tài liệu [Data Modeling & Ingestion Strategy](../contracts/DATA_MODELING_AND_INGESTION.md) mô tả mô hình nguồn và chiến lược nạp. Source-to-target mapping YAML theo từng bảng nằm trong [contracts/mappings/postgresql](../contracts/mappings/postgresql/). Đây là thiết kế; schema vật lý và pipeline chưa được triển khai.

Xem [tổng quan project](../README.md).
