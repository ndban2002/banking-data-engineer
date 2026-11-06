# Batch — Phase 1

Thư mục dành cho phần batch lakehouse. Chưa có pipeline được triển khai.

Luồng dự kiến: PostgreSQL nguồn từ [simulator](../data-source/README.md) → Spark batch → Bronze → Silver → Gold trên MinIO và Apache Iceberg.

Các job, SQL, DAG điều phối, dependency và test của phase này sẽ được bổ sung tại đây. Hạ tầng dùng chung với streaming sẽ được tổ chức riêng khi triển khai.

Xem [tổng quan project](../README.md).
