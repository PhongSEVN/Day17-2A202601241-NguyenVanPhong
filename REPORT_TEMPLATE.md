# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** …  **Lớp:** AICB-P2T2  **Ngày:** …

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
(.venv) phong@LAPTOP-UTMDTDIH:/mnt/c/Users/THIS PC/Desktop/IT/AI THUC CHIEN/Lesson/Lesson17/lab/Day17-2A202601241-NguyenVanPhong$ make verify

  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 65.8s
  run 2/3 … 49.0s
  run 3/3 … 46.1s

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
Traceback (most recent call last):
  File "/mnt/c/Users/THIS PC/Desktop/IT/AI THUC CHIEN/Lesson/Lesson17/lab/Day17-2A202601241-NguyenVanPhong/tools/verify.py", line 287, in <module>
    sys.exit(main())
             ~~~~^^
  File "/mnt/c/Users/THIS PC/Desktop/IT/AI THUC CHIEN/Lesson/Lesson17/lab/Day17-2A202601241-NguyenVanPhong/tools/verify.py", line 231, in main
    d = dashboard_check() if BASELINE_FILE.exists() else None
        ~~~~~~~~~~~~~~~^^
  File "/mnt/c/Users/THIS PC/Desktop/IT/AI THUC CHIEN/Lesson/Lesson17/lab/Day17-2A202601241-NguyenVanPhong/tools/verify.py", line 131, in dashboard_check
    m = measure(read_query())
  File "/mnt/c/Users/THIS PC/Desktop/IT/AI THUC CHIEN/Lesson/Lesson17/lab/Day17-2A202601241-NguyenVanPhong/tools/explain.py", line 93, in measure
    rows = con.execute(sql).fetchall()
           ~~~~~~~~~~~^^^^^
_duckdb.IOException: IO Error: No files found that match the pattern "data/gold_events/*.parquet"

LINE 18: from read_parquet('data/gold_events/*.parquet')
              ^
make: *** [Makefile:41: verify] Error 1
(.venv) phong@LAPTOP-UTMDTDIH:/mnt/c/Users/THIS PC/Desktop/IT/AI THUC CHIEN/Lesson/Lesson17/lab/Day17-2A202601241-NguyenVanPhong$
```

</details>

Tổng kết: 3 **/ 4 tiêu chí đạt**

> **Giải thích về lỗi ở cuối `make verify` (Chỉ đạt 3/4 tiêu chí):**
> Lỗi `_duckdb.IOException: IO Error: No files found that match the pattern "data/gold_events/*.parquet"` xuất hiện ở cuối quá trình chấm điểm là do công cụ kiểm tra đang cố gắng chấm điểm cho **Nhiệm vụ 4 (Bài mở rộng - EXTRA.md)**.
> Vì em chưa thực hiện sinh dữ liệu phụ trợ cho bài mở rộng (bằng lệnh `python seed/generate.py --extra`), nên thư mục `data/gold_events/` đang trống, dẫn đến lỗi khi công cụ kiểm tra cố đọc file. Tuy nhiên, 3 nhiệm vụ cốt lõi bắt buộc (Training set, Feature daily, Quarantine) đều đã hoàn thành xuất sắc 100% (Pass toàn bộ test).
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

> Chọn P99 giúp vớt được 99% dữ liệu trễ với cửa sổ ngắn (3 ngày), tiết kiệm chi phí chạy dbt. Nếu chọn `max` (dữ liệu trễ kỷ lục, ví dụ 30 ngày), mỗi lần chạy dbt sẽ phải quét lại toàn bộ dữ liệu khổng lồ của cả tháng chỉ để vớt 1% cá biệt, làm tốn kém chi phí tính toán (compute cost) cực kỳ lớn một cách vô ích.

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

> - **Chặn ở Silver**: Vì tầng Bronze sinh ra với mục đích hứng dữ liệu thô (raw) nguyên bản 100%. Nếu chặn ở Bronze, chúng ta sẽ mất sạch dấu vết để điều tra xem Backend gửi sai cái gì.- **Không dừng Pipeline**: Vì 312 bản ghi lỗi (chiếm % cực nhỏ) không có quyền làm ngưng trệ hàng chục nghìn bản ghi khỏe mạnh đang phục vụ người dùng. Thay vào đó, ta gom chúng vào bảng Quarantine để kỹ sư vào xử lý riêng.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

|                             |                     |
| --------------------------- | ------------------- |
| **Bài đã làm**    | A / B / không làm |
| **Nguyên nhân**     |                     |
| **Cách khắc phục** |                     |
| **Bằng chứng**      |                     |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
| ---------- | ---------------------------------------------------------------------------------------- |
| 1          | Kiểm tra xem các bảng `incremental` đã được khai báo `unique_key` chưa, và Airflow có đang bật `catchup=True` gây rủi ro chạy bù ồ ạt không. |
| 2          | Kiểm tra xem hệ thống có đo lường độ trễ dữ liệu (P99) không, và các câu query có chừa khoảng lùi (Lookback window) để vét dữ liệu đến muộn chưa. |
| 3          | Kiểm tra Data Contract (`schema.yml`) xem có được bật `enforced: true` không, và có bảng Quarantine để hứng các dữ liệu rác ngoài luồng không. |
