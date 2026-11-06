# Data contracts

Định nghĩa dữ liệu dùng chung giữa simulator, batch và streaming, theo [Open Data Contract Standard (ODCS) 3.2.0](https://bitol-io.github.io/open-data-contract-standard/v3.2.0/).

```text
contracts/
└── sources/
    └── postgresql/
        └── core_sim/
            └── accounts.yaml
```

Contract nguồn mô tả cấu trúc và ý nghĩa dữ liệu mà pipeline có thể kỳ vọng. DDL thực tế của simulator nằm trong [schema.sql](../data-source/src/bank_sim/schema.sql); khi thay đổi cấu trúc nguồn, cập nhật contract tương ứng.

## Contract `accounts`

- `customer_id` và `merchant_id` đều nullable; đúng một trong hai phải có giá trị. Quy tắc SQL `accounts_exactly_one_owner` đếm các hàng vi phạm và yêu cầu kết quả bằng 0.
- UUID dùng `logicalType: string` và `physicalType: uuid`; số tiền dùng `integer`/`bigint`; thời gian dùng `timestamp`/`timestamptz`.
- Số tiền là VND nguyên đồng, không âm. `opening_balance` là số dư khởi tạo; `balance` là số dư hiện tại.
- Hai cột chủ sở hữu là duy nhất khi có giá trị và tham chiếu bảng tương ứng; quan hệ này được mô tả trong contract và được PostgreSQL bảo vệ bằng UNIQUE/foreign key.
- `created_at` và `updated_at` là thời điểm ghi database, không phải thời điểm nghiệp vụ của giao dịch. Bảng hiện tại chưa cung cấp snapshot số dư lịch sử.

## Endpoint và cấu hình runtime

`servers` mô tả nguồn PostgreSQL. `POSTGRES_HOST` và `POSTGRES_PORT` là placeholder để công cụ sử dụng contract thay thế theo môi trường; giữ tên database `bank` từ contract ban đầu và điều chỉnh nếu môi trường của bạn dùng database khác.

Nếu công cụ chạy trên máy local qua SSH tunnel, endpoint có thể là `127.0.0.1:15432`; nếu công cụ chạy trên server, dùng endpoint PostgreSQL mà server đó truy cập được. Không lưu mật khẩu hoặc private key vào contract.

Simulator hiện vẫn kết nối bằng `DATABASE_URL` và chưa đọc contract hoặc các placeholder này. File `.env` không tự được nạp. Các quy tắc `quality` chỉ chạy khi có công cụ thực thi; validation YAML/JSON Schema chỉ xác nhận cấu trúc contract, không kiểm tra dữ liệu trong database.
