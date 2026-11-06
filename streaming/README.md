# Streaming — Phase 2

Thư mục dành cho phần xử lý realtime. Chưa có pipeline được triển khai.

Luồng dự kiến: PostgreSQL nguồn từ [simulator](../data-source/README.md) → CDC → Kafka → Spark Structured Streaming → PostgreSQL serving.

Các job streaming, cấu hình connector, dependency và test của phase này sẽ được bổ sung tại đây. Phần streaming sẽ dùng cùng định nghĩa nghiệp vụ với batch để đối soát kết quả.

Xem [tổng quan project](../README.md).
