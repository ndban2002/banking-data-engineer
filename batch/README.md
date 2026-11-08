# Batch — Phase 1

Thư mục dành cho phần batch lakehouse. Chưa có pipeline được triển khai.

Luồng dự kiến: PostgreSQL nguồn từ [simulator](../data-source/README.md) → Spark batch → Bronze → Silver → Gold trên MinIO và Apache Iceberg.

Các job, SQL, DAG điều phối, dependency và test của phase này sẽ được bổ sung tại đây. Hạ tầng dùng chung với streaming sẽ được tổ chức riêng khi triển khai.

Tài liệu thiết kế mapping [Source-to-Target: Bronze → Gold](docs/source-to-target-mapping.md) mô tả grain, mapping cột, phép biến đổi và quy tắc đối soát mục tiêu. Đây là thiết kế; schema vật lý và pipeline chưa được triển khai.

Xem [tổng quan project](../README.md).
