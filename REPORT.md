
# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Văn Phong  **Lớp:** AICB-P2T2  **Ngày:** 17/8/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```

(.venv) phong@LAPTOP-UTMDTDIH:/mnt/c/Users/THIS PC/Desktop/IT/AI THUC CHIEN/Lesson/Lesson17/lab/Day17-2A202601241-NguyenVanPhong$ make verify

  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 60.0s
  run 2/3 … 41.5s
  run 3/3 … 41.1s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 132,832 (37.6×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt

(.venv) phong@LAPTOP-UTMDTDIH:/mnt/c/Users/THIS PC/Desktop/IT/AI THUC CHIEN/Lesson/Lesson17/lab/Day17-2A202601241-NguyenVanPhong$
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

Về câu hỏi A, hiệu suất truy vấn chậm do dữ liệu bị phân mảnh thành 5000 tệp nhỏ điều này tạo ra chi phí I/O cao và điều kiện lọc bao gồm strftime(thời gian sự kiện) bao quanh cột nên truy vấn không thể được tối ưu hóa và động cơ không thể sử dụng thống kê Min/Max để loại bỏ. Giải pháp là chạy một tập lệnh để nối tất cả các tệp, cắt phân vùng theo ngày (thời gian sự kiện),sắp xếp dữ liệu theo cụm theo ORDER BY tên khách hàng,thời gian sự kiện và viết lại truy vấn bằng cách thay thế strftime(thời gian sự kiện) bằng khoảng thời gian chuẩn (thời gian sự kiện >=...và <...). Điều này đã làm giảm đáng kể số lượng tệp từ 5000 xuống còn 14 và giảm 37,6% kích thước dữ liệu (số hàng quét) trong khi vẫn giữ kết quả như cũ. Về câu hỏi B, người tiêu dùng mất dữ liệu khi bị lỗi vì quá trình được cấu hình để hoạt động theo ngữ nghĩa At-most-once (xác nhận vị trí trước khi ghi dữ liệu). Do đó cần phải đảo ngược thứ tự của các

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

|                             |                                                                                                                                                                                                                                            |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Triệu chứng**     | Bảng`gold_training_set` bị phình to bất thường (38,750 dòng) sau mỗi lần bấm chạy lại (Clear Task) trên Airflow.                                                                                                            |
| **Nguyên nhân**     | 1. dbt model thiếu`unique_key` nên dùng chiến thuật `append` (chèn thêm) thay vì cập nhật trạng thái phiếu. 2. Airflow DAG bật `catchup=True` gây ra xung đột ghi (Race Condition) do kích hoạt chạy bù ồ ạt. |
| **Cách khắc phục** | -`gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`.- `ai_training_pipeline.py`: Đặt `catchup=False` và `max_active_runs=1`.                                                    |
| **Bằng chứng**      | trước: 38,750 hàng · sau: 12,480 hàng · checksum 3 lượt: 8622572a97 (Ổn định ✓)                                                                                                                                                |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

|                                     |                                                                                                                                                                                                                                           |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**             | Bảng`gold_feature_daily` bị thiếu hụt dữ liệu ở các ngày quá khứ.                                                                                                                                                            |
| **P99 độ trễ đo được** | **3 ngày** *(bắt buộc)*                                                                                                                                                                                                        |
| **Lookback đã chọn**       | 3 ngày — vì bao phủ được 99% dữ liệu đến muộn mà chi phí quét lại không quá cao.                                                                                                                                        |
| **Nguyên nhân**             | Dữ liệu đến muộn (Late-arriving data) bị bỏ qua do điều kiện quét`event_date > max(event_date)` không chịu quét lại những ngày đã qua.                                                                               |
| **Cách khắc phục**         | - Sửa điều kiện thành`>= max() - interval 3 day` để nới rộng cửa sổ quét thêm 3 ngày.- Thêm `unique_key = ['event_date', 'customer_id']` và chiến thuật `merge` để tránh trùng lặp dữ liệu khi quét lại. |
| **Bằng chứng**              | trước: 8,645 hàng · sau: 9,100 hàng                                                                                                                                                                                                  |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Chọn P99 giải quyết được 99% các trường hợp chậm trễ và vẫn có thể giữ được khoảng thời gian nhỏ (3 ngày) do đó tiết kiệm được chi phí vận hành dbt. Tuy nhiên việc chọn 'max' để giải quyết 100% các trường hợp chậm trễ (như 30 ngày) thì mỗi khi dbt chạy phải quét toàn bộ tập dữ liệu cho tháng đó để tính đến những trường hợp 1% còn lại. Điều này làm tăng chi phí tính toán rất nhiều nhưng không mang lại hiệu quả nào cả.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

|                                                                         |                                                                                                                                                                                                                                                                                                                                                                                         |
| ----------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**                                                 | Dữ liệu cột`priority` xuất hiện rất nhiều giá trị rác (`0`, `5`, `-1`) và nhãn chữ (`urgent`, `high`, `low`...). Trái với Data Contract ban đầu quy định từ 1 đến 4.                                                                                                                                                                                |
| **Nguyên nhân**                                                 | Đội Backend âm thầm thay đổi định dạng dữ liệu truyền xuống (Schema Evolution) mà không báo trước. Dữ liệu lẫn lộn cả giá trị hợp lệ (nhưng khác format) và rác thật sự.                                                                                                                                                                               |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | - Nhóm 1 (1, 2, 3, 4): Chuẩn cũ ➔ Giữ nguyên.- Nhóm 2 (urgent, high, medium, low): Hợp lệ nhưng khác vỏ ➔ Dùng lệnh CASE WHEN quy đổi về 1..4.- Nhóm 3 (0, -1, P1, rỗng): Rác thật sự ➔ Gán bằng NULL và đưa vào vùng cách ly Quarantine.                                                                                                               |
| **Cách khắc phục**                                             | -`normalize_priority.sql`: Viết khối CASE WHEN để xử lý 3 nhóm.- `silver_tickets.sql`: Đặt lính gác `where priority is not null` TRƯỚC hàm `row_number()` để chặn rác lọt vào.- `quarantine_tickets.sql`: Hứng rác bằng điều kiện `where priority is null`.- `schema.yml`: Bật `enforced: true` và thêm `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng**                                                  | `quarantine_tickets` = 312 hàng · `dbt test` 11/11 pass                                                                                                                                                                                                                                                                                                                           |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> - **Khó khăn ở mức Silver**: Vì bảng Bronze được thiết kế sao cho nó sẽ chứa 100% dữ liệu thô.Nếu chúng ta chặn bảng ở mức Bronze thì sẽ không còn dấu vết nào để phân tích những gì phía Backend gửi đi. **Đừng chặn dòng dữ liệu**: Vì chỉ có 312 bản ghi trong hàng ngàn bản ghi tốt đẹp không có quyền dừng quá trình này.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

### Bài A — Tối ưu Query Dashboard

|                             |                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**     | Truy vấn Dashboard truy xuất dữ liệu vô cùng chậm chạp.                                                                                                                                                                                                                                                                                                                                                                                         |
| **Nguyên nhân**     | 1. Dữ liệu bị phân mảnh thành 5.000 file quá nhỏ (Small-file problem). 2. Điều kiện lọc`WHERE` dùng hàm `strftime(event_time)` bọc bên ngoài cột, khiến truy vấn mất tính Sargable và engine không thể cắt tỉa (pruning) dựa trên thống kê Min/Max.                                                                                                                                                                  |
| **Cách khắc phục** | -**Sắp xếp lại file trên đĩa**: Chạy script `tools/compact.py` để gom file. Dùng `PARTITION BY (event_date)` để chia file gọn gàng, và `ORDER BY customer_name, event_time` để dồn dữ liệu của một khách vào một cụm.- **Sửa Query**: Thay vì dùng `strftime`, viết điều kiện theo khoảng thời gian (`event_time >= ... and event_time < ...`), đồng thời bật tính năng đọc partition. |
| **Bằng chứng**      | File giảm từ 5,000 xuống 14 file, rows scanned giảm 37.6 lần (từ 5,000,000 xuống 132,832), mã băm`result hash` không đổi.                                                                                                                                                                                                                                                                                                                 |

### Bài B — Consumer gặp sự cố giữa batch

|                             |                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**     | Nếu tiến trình Consumer bị crash đột ngột trong khi đang đọc luồng tin (stream), sẽ có nguyên một lô bản ghi bị mất tích vĩnh viễn (Data Loss).                                                                                                                                                                                                                                                                                |
| **Nguyên nhân**     | Tiến trình Consumer đang chạy ở ngữ nghĩa**At-most-once**: gọi `commit()` ghi nhận lưu lại offset TRƯỚC khi ghi dữ liệu. Do đó nếu vừa commit xong mà máy chết, lần khởi động lại nó sẽ không đọc lại luồng đó nữa vì ngỡ là đã xử lý xong.                                                                                                                                                      |
| **Cách khắc phục** | -**Đảo thứ tự thực hiện**: Đẩy lệnh ghi dữ liệu lên trước, ghi thành công rồi mới được gọi lệnh `commit()` (Chuyển sang ngữ nghĩa **At-least-once**).- **Chống trùng lặp (Idempotent)**: Đổi lệnh `INSERT` thường thành `INSERT ... ON CONFLICT (event_id) DO UPDATE SET ...` để dù có đọc đi đọc lại một sự kiện thì cũng chỉ cập nhật chứ không sinh ra hàng mới. |
| **Bằng chứng**      | Lệnh mô phỏng sự cố đứt ngầm`make crash-test` vượt qua 100% không mất hàng nào.                                                                                                                                                                                                                                                                                                                                                      |

> Lý do tại sao ta phải dùng câu lệnh DO UPDATE thay vì DO NOTHING?
> Trong cả hai trường hợp này đều không cho phép trùng lặp nhưng khi tin nhắn được gửi trở lại từ phía nguồn đã thay đổi nội dung (ví dụ như có lỗi chính tả đã được sửa chữa rồi) thì DO NOTHING sẽ giữ nguyên nội dung trước đó trong khi DO UPDATE sẽ thay đổi nó thành nội dung mới nhất.

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1 | Kiểm tra xem các bảng`incremental` đã được khai báo `unique_key` chưa, và Airflow có đang bật `catchup=True` gây rủi ro chạy bù ồ ạt không. |
| 2 | Kiểm tra xem hệ thống có đo lường độ trễ dữ liệu (P99) không, và các câu query có chừa khoảng lùi (Lookback window) để vét dữ liệu đến muộn chưa. |
| 3 | Kiểm tra Data Contract (`schema.yml`) xem có được bật `enforced: true` không, và có bảng Quarantine để hứng các dữ liệu rác ngoài luồng không. |
