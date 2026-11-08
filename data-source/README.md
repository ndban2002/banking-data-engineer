# Banking Source Simulator

Bộ giả lập nguồn dữ liệu thanh toán QR/POS để tự học data engineering. Python tạo dữ liệu và xử lý nghiệp vụ trong PostgreSQL có sẵn; bạn có thể nối CDC → Kafka → Spark/lakehouse ở chặng tiếp theo.

Simulator nằm trong `data-source/` của repo. **Tất cả lệnh bên dưới chạy từ thư mục `data-source/`**; nếu đang ở root repo, chạy `cd data-source` trước. Xem [tổng quan project](../README.md) và các phần [batch](../batch/README.md), [streaming](../streaming/README.md).

## Chạy nhanh

Cần **Python 3.14+**, PostgreSQL **16+** và một database dành cho học tập. Tài khoản kết nối cần quyền `CONNECT`, `CREATE` schema trong database, và quyền sở hữu schema/bảng simulator. Project không tạo database hoặc user, không thay đổi cấu hình server.

```bash
python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'
cp config.example.toml config.toml

# Thay bằng kết nối PostgreSQL học tập của bạn; không commit thông tin kết nối.
export DATABASE_URL='postgresql://sim_user:your_password@localhost:5432/banking_lab'

bank-sim init-db
bank-sim seed
bank-sim generate --mode history \
  --start '2026-11-01T00:00:00+07:00' \
  --end '2026-11-04T00:00:00+07:00' \
  --count 10000
bank-sim generate --mode realtime --rate 1 --duration 60
bank-sim verify
```

`bank-sim generate --mode realtime` chạy liên tục ở mức mặc định 1 yêu cầu/giây. Nhấn **Ctrl+C** hoặc gửi SIGTERM để yêu cầu dừng sau giao dịch đang xử lý; CLI in thống kê khi kết thúc. Nếu bị kill đột ngột, lần `generate` tiếp theo xử lý các giao dịch `PENDING` trước khi sinh thêm.

Có thể dùng `python -m bank_sim` thay cho `bank-sim`. Đặt `--config` **trước** tên lệnh nếu dùng file khác:

```bash
bank-sim --config config.example.toml verify
```

CLI trả JSON. Mã thoát: `0` thành công; `1` đối soát phát hiện sai lệch hoặc còn `PENDING`; `2` lỗi cấu hình/database/tham số; `130` nếu ngắt ở bước không nằm trong vòng chạy có xử lý dừng. Lỗi database chỉ in SQLSTATE hoặc `CONNECTION_ERROR`, không in DSN, mật khẩu hoặc thông báo chi tiết từ server.

## Kết nối qua SSH tunnel và `.env`

File `.env` của simulator đặt tại `data-source/.env`, được `.gitignore` ở root bỏ qua. Với bản clone mới, sao chép [.env.example](.env.example) thành `.env` rồi điền cấu hình của bạn. Nếu đã có `.env`, giữ nguyên file đó.

CLI đọc biến môi trường, chưa tự nạp `.env` hoặc mở SSH tunnel. Trong terminal thứ nhất, từ `data-source/`:

```bash
set -a
source .env
set +a

ssh -N -T \
  -p "$SSH_PORT" \
  -i "$SSH_KEY_PATH" \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -L "127.0.0.1:${SSH_LOCAL_PORT}:${PG_REMOTE_HOST}:${PG_REMOTE_PORT}" \
  "${SSH_USER}@${SSH_HOST}"
```

Giữ tunnel chạy. Trong terminal thứ hai, cũng từ `data-source/`:

```bash
source .venv/bin/activate
set -a
source .env
set +a

bank-sim init-db
bank-sim seed
bank-sim generate --mode realtime --rate 1 --duration 300
```

`DATABASE_URL` trỏ vào đầu local của tunnel, ví dụ `127.0.0.1:15432`. `PG_REMOTE_HOST` là địa chỉ database nhìn từ máy SSH; dùng `127.0.0.1` nếu PostgreSQL nằm trên chính máy đó. SSH và PostgreSQL dùng tài khoản riêng. Chỉ lưu đường dẫn private key trong `.env`, giữ file key trong `~/.ssh/`.

## Cấu hình và ý nghĩa dữ liệu

Xem [config.example.toml](config.example.toml). Nếu không chỉ định file, CLI đọc `./config.toml` khi có, còn lại dùng mặc định:

| Tham số | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `database.schema` | `core_sim` | Schema riêng; tên chữ thường, không dùng `public` hoặc `pg_*` |
| `seed.random_seed` | 42 | Sinh định danh khách hàng/merchant ổn định và lựa chọn workload |
| `seed.customers` / `seed.merchants` | 100 / 20 | Mỗi chủ thể có một tài khoản |
| `seed.opening_balance` | 100.000.000 | VND mỗi khách hàng; merchant bắt đầu từ 0 |
| `generator.rate` | 1 | Mức yêu cầu/giây realtime; `--rate` ghi đè |
| `generator.amount_min` / `amount_max` | 10.000 / 500.000 | Khoảng tiền của thanh toán thường |
| `generator.scenarios` | 90 / 3 / 3 / 2 / 2 | Trọng số normal / insufficient_funds / decline / refund / retry |

Trọng số là số nguyên không âm, không bắt buộc tổng bằng 100, ít nhất một trọng số phải dương. File cấu hình và biến môi trường tách riêng; `DATABASE_URL` là nơi duy nhất nhận kết nối. Nếu cần TLS, cấu hình `sslmode` trong URL theo server của bạn.

- **normal**: chọn khách hàng và merchant, trả tiền nếu đủ số dư. Khi chạy lâu và tiền khách hàng cạn, giao dịch thường cũng có thể thất bại.
- **insufficient_funds**: số tiền bằng số dư hiện tại cộng 1 VND, vì vậy có thể vượt `amount_max`. Đây là thiếu số dư thật, không phải sửa trạng thái tùy ý.
- **decline**: từ chối giả lập với mã `SIMULATED_DECLINE`, không chuyển tiền.
- **refund**: chọn trong tối đa 1.000 thanh toán thành công chưa hoàn tiền, ưu tiên thứ tự thời gian; tạo hoàn tiền toàn phần. Nếu chưa có thanh toán phù hợp, tạo thanh toán thường.
- **retry**: gửi lại nguyên yêu cầu trong tối đa 1.000 yêu cầu gần nhất của lần chạy; nếu chưa có, tạo thanh toán thường. Retry thất bại vẫn trả về thất bại cũ.

`--count` tính **số lần gửi yêu cầu**, gồm retry; số bản ghi mới có thể nhỏ hơn. Thống kê `succeeded`/`failed` của generator cũng tính theo lần gửi. Dùng `verify` để xem số giao dịch thực trong database. Mỗi lần `generate` có `run_id` mới và sinh workload mới; chạy lại cùng khoảng lịch sử **có tạo thêm giao dịch**.

Giữ cùng random seed, cấu hình và trạng thái đầu để tái lập lựa chọn kịch bản/số tiền. UUID của lần sinh tải và thời gian thực không cố định. Seed khác tạo một tập chủ thể khác trong cùng schema. Giữ nguyên seed khi tiếp tục tập dữ liệu hiện tại.

`seed` chạy trong một transaction; chạy lại không xóa bản ghi, không đổi số dư ban đầu/hiện tại hoặc nạp thêm tiền. Tăng số lượng sẽ thêm chủ thể; giảm số lượng không xóa chủ thể cũ. `init-db` tạo các đối tượng còn thiếu; chưa phải công cụ migration cho phiên bản schema tương lai.

## Luồng nghiệp vụ

```mermaid
flowchart LR
    CLI[CLI + TOML] --> GEN[Workload generator]
    GEN --> SVC[Banking service]
    SVC --> INTENT[Commit yêu cầu PENDING]
    INTENT --> LOCK[Khóa giao dịch và tài khoản]
    LOCK --> FINAL[Commit số dư + trạng thái cuối]
    FINAL --> PG[(PostgreSQL)]
    PG -. Chặng tiếp theo .-> CDC[CDC → Kafka]
```

1. Lưu ý định giao dịch `PENDING` bằng mã yêu cầu duy nhất. Gửi lại mã đó với cùng nội dung trả về giao dịch cũ; thay đổi nội dung bị từ chối.
2. Khóa bản ghi giao dịch. Với hoàn tiền, khóa thêm thanh toán gốc; các hoàn tiền cùng thanh toán được xử lý tuần tự.
3. Khóa hai tài khoản theo thứ tự UUID, kiểm tra điều kiện. Thanh toán chỉ đi từ khách hàng sang merchant đúng kênh.
4. Trừ tiền, cộng tiền và chuyển trạng thái sang `SUCCEEDED` trong cùng transaction. Nếu không đủ số dư hoặc bị từ chối nghiệp vụ, ghi `FAILED` cùng mã lỗi, không thay đổi tiền.

Lỗi kỹ thuật trong bước xử lý rollback toàn bộ thay đổi số dư và để lại `PENDING` đã lưu ở bước đầu. Cả gửi lại yêu cầu và khôi phục khi khởi động đều đi qua cùng hàm xử lý, tránh chuyển tiền hai lần.

Phần xử lý dùng khóa `FOR NO KEY UPDATE`: số dư/trạng thái cần tuần tự hóa nhưng không đổi khóa chính. Loại khóa này tương thích với `KEY SHARE` khi PostgreSQL kiểm tra foreign key của intent mới; xem [tài liệu khóa PostgreSQL](https://www.postgresql.org/docs/16/explicit-locking.html#LOCKING-ROWS).

Hoàn tiền phải tham chiếu thanh toán `SUCCEEDED`, đảo đúng hai tài khoản, cùng kênh, cùng số tiền và thời điểm nghiệp vụ không trước thanh toán. Một thanh toán có tối đa một hoàn tiền thành công, được bảo vệ bằng khóa và unique index. Thanh toán gốc vẫn giữ trạng thái `SUCCEEDED`; doanh thu ròng phải trừ các bản ghi hoàn tiền thành công.

## Mô hình dữ liệu

```mermaid
erDiagram
    CUSTOMERS o|--|| ACCOUNTS : owns
    MERCHANTS o|--|| ACCOUNTS : owns
    ACCOUNTS ||--o{ TRANSACTIONS : source
    ACCOUNTS ||--o{ TRANSACTIONS : destination
    TRANSACTIONS o|--o{ TRANSACTIONS : refund_attempts
```

Mỗi account thuộc **một trong hai** loại chủ thể, không thuộc cả hai. `seed` tạo đúng một account cho mỗi chủ thể. Mọi khóa chính đều là UUID; tiền là `BIGINT` VND, không dùng float. Tổng tiền chỉ được đưa vào khi khởi tạo tài khoản; payment/refund chuyển tiền trong hệ thống nên tổng số dư được bảo toàn.

| Bảng | Trường đáng chú ý |
| --- | --- |
| `customers` | `id`, `name`, `region`, `created_at` |
| `merchants` | `id`, `name`, `mcc`, `region`, `channel`, `created_at` |
| `accounts` | `id`, `customer_id` hoặc `merchant_id`, `currency`, `opening_balance`, `balance`, `created_at`, `updated_at` |
| `transactions` | `id`, `request_id`, `kind`, hai account, `amount`, `currency`, `channel`, `status`, `error_code`, `original_transaction_id`, `requested_decline`, `occurred_at`, `created_at`, `updated_at` |

Trạng thái: `PENDING → SUCCEEDED | FAILED`. Mã lỗi nghiệp vụ: `INSUFFICIENT_FUNDS`, `SIMULATED_DECLINE`, `INVALID_ACCOUNTS`, `INVALID_CHANNEL`, `INVALID_REFUND`, `ALREADY_REFUNDED`. ID tài khoản/giao dịch tham chiếu không tồn tại bị foreign key từ chối trước khi lưu intent.

`occurred_at` là **thời điểm nghiệp vụ**. `created_at`/`updated_at` là **thời điểm thực ghi database**; đều dùng `TIMESTAMPTZ`, kết nối ứng dụng đặt timezone UTC. Chế độ lịch sử phân bố thời điểm nghiệp vụ tăng dần trong `[start, end)`, dùng cùng logic số dư như realtime, không ngủ. Tham số thời gian phải có timezone, ví dụ `+07:00` hoặc `Z`.

Dữ liệu lịch sử được ghi vào PostgreSQL ở hiện tại. Khi nối CDC sau này, thời điểm WAL không trở thành thời điểm nghiệp vụ lịch sử. Nên sinh lịch sử trước rồi chạy realtime để diễn biến số dư theo thời gian dễ hiểu. Tiền ban đầu được xem là có sẵn trước thời điểm nghiệp vụ đầu tiên; đây chưa phải hệ thống snapshot số dư theo ngày.

## Đối soát và truy vấn

```bash
bank-sim verify
psql "$DATABASE_URL" -f sql/examples.sql
```

`verify` đọc một snapshot nhất quán, kiểm tra tổng số dư, đối chiếu từng tài khoản với số dư ban đầu cộng/trừ giao dịch thành công, kiểm tra hoàn tiền và đếm `PENDING`. Báo cáo chỉ liệt kê tối đa 100 tài khoản sai lệch nhưng giữ tổng số tài khoản sai. Khi generator đang chạy, snapshot có thể bắt gặp intent `PENDING` hợp lệ giữa hai bước; hãy chạy lại sau khi workload dừng để nghiệm thu.

[sql/examples.sql](sql/examples.sql) có truy vấn giao dịch mới nhất, thống kê lỗi, doanh thu merchant theo phút và đối soát từng tài khoản. Các truy vấn dùng schema mặc định `core_sim`; sửa tên nếu cấu hình khác.

## Kiểm thử

```bash
# Chỉ unit test và kiểm tra tham số không cần database
python -m pytest -q -m 'not integration'

# Chạy toàn bộ; thay bằng database TEST bạn chuẩn bị
export TEST_DATABASE_URL='postgresql://sim_test:your_password@localhost:5432/banking_test'
python -m pytest -q
```

Integration test **chỉ đọc `TEST_DATABASE_URL`**, không lấy `DATABASE_URL`. Mỗi test tạo schema `bank_sim_test_<uuid>` mới rồi xóa đúng schema đó khi xong. Không có `TEST_DATABASE_URL` thì các test cần PostgreSQL được báo **skipped**, không được coi là đã kiểm chứng nghiệp vụ database.

Bộ test kiểm tra thanh toán/hoàn tiền, lỗi nghiệp vụ, seed idempotent, retry và tranh chấp đồng thời, rollback sau trừ tiền/trước đổi trạng thái, khôi phục pending, sinh lịch sử rồi realtime, dừng an toàn và phát hiện sai lệch từng account dù tổng tiền vẫn đúng.

## Cấu trúc và giới hạn phiên bản đầu

```text
src/bank_sim/
  cli.py          # argparse, cấu hình, mã thoát và JSON output
  config.py       # TOML và validation
  db.py           # connection, khởi tạo và seed
  schema.sql      # DDL template theo schema
  service.py      # nghiệp vụ, khóa, idempotency và recovery
  generator.py    # chọn kịch bản, history và realtime pacing
  verify.py       # đối soát bằng snapshot
tests/            # unit + integration test
sql/examples.sql  # truy vấn học tập
```

Một worker sinh tải; rate là mục tiêu tối đa theo thời điểm bắt đầu yêu cầu, không cam kết throughput khi database chậm. Các kiểm thử đồng thời gọi lớp nghiệp vụ bằng nhiều connection. Đây là bộ nguồn học tập, chưa có sổ cái kép, nạp/rút tiền, phí, lãi, đa tiền tệ, chuyển khoản, API/UI, CDC, Kafka, Spark hoặc Docker. Tiền khách hàng có thể cạn khi chạy lâu; muốn bắt đầu bộ dữ liệu độc lập, chọn schema mới rồi `init-db` và `seed`.
