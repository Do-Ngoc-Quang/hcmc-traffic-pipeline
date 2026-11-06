# Thiết kế triển khai: Pipeline thời tiết, AQI và giao thông thời gian thực (TP.HCM)

Nov 2, 2026 · @Quang

## 1. Tổng quan và giả định

Dự án xây một pipeline streaming thu thập thời tiết, AQI và giao thông cho TP.HCM, để trả lời hai câu hỏi: mưa và chất lượng không khí thay đổi thế nào theo giờ, và tốc độ di chuyển hiện tại ở các điểm đo so với tốc độ thông thoáng ra sao. Theo điều khoản TomTom (mục 11.4), dữ liệu giao thông chỉ được xử lý theo thời gian thực và không lưu lịch sử; chỉ thời tiết và AQI được lưu lâu dài. Toàn bộ chạy local bằng Docker Compose, không tốn phí dịch vụ.

**Mục tiêu**

- Thu thập đều đặn dữ liệu thời tiết, AQI và tốc độ giao thông tại các điểm đo đã chọn, trong khung ngân sách request miễn phí.
- Xử lý streaming có event time, watermark và join theo ô không gian H3 và theo thời gian.
- Lưu thời tiết và AQI dạng Parquet và ClickHouse, mô hình hoá bằng dbt; giao thông chỉ giữ ngắn hạn (30 phút) theo mục 11.4 của điều khoản TomTom.
- Dashboard hiển thị giao thông hiện tại cùng thời tiết, AQI và giám sát pipeline.

**Ngoài phạm vi**: dữ liệu thương mại, quét toàn thành phố, nowcasting từng phút, triển khai cloud, lưu lịch sử giao thông và mô hình dự báo kẹt xe (chỉ mở lại nếu TomTom xác nhận bằng văn bản).

**Giả định cần bạn xác nhận** (đổi giả định nào thì các phần sau cần chỉnh theo)

| Giả định | Giá trị tạm chọn | Ảnh hưởng nếu đổi |
| --- | --- | --- |
| Thành phố | TP.HCM | Danh sách điểm đo và toạ độ ở mục 4 |
| Môi trường chạy | Local, Docker Compose, máy 16 GB RAM | Cloud thì thêm object storage và managed Kafka |
| Mục đích | Học tập và portfolio, phi thương mại | Dùng thương mại phải đổi gói Open-Meteo và đọc lại điều khoản TomTom |
| Ngôn ngữ | Python 3.11, PySpark | Dùng Flink thì viết lại mục 7 |
| Nhà cung cấp giao thông | TomTom Flow Segment Data | HERE có hạn mức free khác, xem mục 2 |

## 2. Nguồn dữ liệu và ngân sách request

Giao thông là nút thắt duy nhất: TomTom chỉ cho 20.000 request Flow mỗi tháng (khoảng 660 mỗi ngày), nên cấu hình dưới đây dùng 10 điểm đo và lịch poll thích ứng, chiếm khoảng 83% hạn mức. Thời tiết và AQI dùng chưa tới 1% hạn mức của Open-Meteo.

**Số liệu đã kiểm chứng ngày 02/11/2026**

| Nguồn | Hạn mức free | Ghi chú |
| --- | --- | --- |
| [TomTom Traffic Flow Segment Data](https://docs.tomtom.com/pricing) | 20K request/tháng | Trang giá hiện ghi theo tháng, không còn 2.500/ngày như tài liệu cũ |
| [TomTom Traffic Incidents](https://docs.tomtom.com/pricing) | 2,5K request/tháng | Dùng cho sự kiện kẹt xe, tách khỏi luồng Flow |
| [Open-Meteo](https://www.itechguides.com/?p=876868) | 600/phút, 5.000/giờ, 10.000/ngày | Phi thương mại, dữ liệu CC BY 4.0 nên phải ghi nguồn |
| [Open-Meteo Air Quality](https://opennetzero.org/open-meteo/open-meteo-air-quality-api) | Cùng hạn mức trên | Dữ liệu mô hình CAMS, lưới thô: vùng toàn cầu 0,4° (khoảng 45 km), 3 giờ một mẫu, cập nhật 12 giờ một lần; cả TP.HCM nằm trong một vài ô lưới và đây không phải số đo trạm |

TomTom có Việt Nam trong bảng [Market Coverage](https://developer.tomtom.com/online-traffic/market-coverage) cho cả Flow và Incidents. Bảng này chỉ xác nhận quốc gia, chưa đảm bảo mọi tuyến đều có dữ liệu, nên mục 4 có bước kiểm tra từng điểm.

**Lịch poll giao thông (mỗi điểm)**

| Khung giờ | Chu kỳ | Số lần/ngày |
| --- | --- | --- |
| 06:30-09:30 và 16:30-19:30 (cao điểm) | 10 phút | 36 |
| 09:30-16:30 | 30 phút | 14 |
| 19:30-22:00 | 30 phút | 5 |
| **Tổng mỗi điểm** |  | **55** |

**Bảng ngân sách (30 ngày)**

| Luồng | Cấu hình | Request/ngày | Request/tháng | Hạn mức |
| --- | --- | --- | --- | --- |
| TomTom Flow | 10 điểm × 55 lần | 550 | 16.500 | 20.000 (83%) |
| TomTom Incidents | 1 bbox lõi thành phố, 30 phút, 06:00-22:00 | 32 | 960 | 2.500 (38%) |
| Open-Meteo thời tiết | 1 request nhiều toạ độ, mỗi giờ | 24 | 720 | 10.000/ngày |
| Open-Meteo AQI | 1 request, 1 toạ độ trung tâm TP, mỗi 3 giờ | 8 | 240 | 10.000/ngày |

Quy tắc quota guard (mục 6): dừng poll giao thông khi dùng đủ 95% hạn mức tháng, và ưu tiên điểm có ưu tiên cao hơn khi phải cắt. Cần xác nhận trong giai đoạn 2 rằng một lần gọi Flow Segment Data tính đúng 1 request, bằng cách đối chiếu bộ đếm của pipeline với bảng usage của TomTom sau 2-3 ngày chạy.

HERE là phương án dự phòng, nhưng các nguồn ghi hạn mức không thống nhất (từ 5.000 đến 250.000 giao dịch mỗi tháng tuỳ trang và năm), nên không dùng làm phương án chính khi chưa đọc lại trang giá hiện hành của HERE.

## 3. Kiến trúc tổng thể

Producer đẩy dữ liệu vào Kafka. Một job Spark ghi thời tiết và AQI ra Parquet, một job Spark tính chỉ số kẹt xe cho từng mẫu giao thông và join với thời tiết. ClickHouse đọc các topic, giữ thời tiết và AQI lâu dài, giữ giao thông 30 phút rồi tự xoá; dbt và Grafana đọc từ ClickHouse.

&#91;embedded content: kiến trúc pipeline · 8 thành phần\]

Mũi tên đi một chiều từ nguồn đến kho. Nhánh giao thông không ghi vào Parquet và không có thành phần dự báo.

| Thành phần | Lựa chọn | Lý do |
| --- | --- | --- |
| Thu thập | Python asyncio, httpx, APScheduler | Nhẹ; khối lượng khoảng 610 request/ngày không cần Airflow |
| Hàng đợi | Kafka chế độ KRaft, một node | Không cần ZooKeeper, đủ cho máy local |
| Streaming | Spark Structured Streaming (PySpark) | Có watermark và join stream-stream |
| Lưu thô | Parquet trên thư mục local | Cấu trúc giống S3, chuyển cloud sau này dễ |
| Kho truy vấn | ClickHouse | Kafka engine, `ReplacingMergeTree`, `ASOF JOIN` |
| Biến đổi | dbt với adapter ClickHouse | Test, tài liệu và lineage cho mô hình dữ liệu |
| Dashboard | Grafana | Geomap, cảnh báo, provision bằng file |
| Lưu giao thông | Kafka và ClickHouse có TTL 30 phút, không ghi Parquet | Tuân thủ mục 11.4 điều khoản TomTom: chỉ giữ ngắn hạn để hiển thị |

## 4. Điểm đo và lưới không gian H3

Chọn 10 điểm đo giao thông bằng cách thử thật với API trong giai đoạn 1, rồi gán mỗi điểm vào một ô H3 độ phân giải 8; thời tiết được join qua ô cha độ phân giải 6, còn AQI dùng một giá trị chung cho cả thành phố. Danh sách điểm nằm trong một file cấu hình, không viết cứng trong code.

**Cách chọn điểm**

1. Liệt kê khoảng 20 ứng viên là các trục chính và nút giao hay kẹt, rải đều các hướng của thành phố (trung tâm, Đông, Tây, Nam, Bắc). Tên đường cụ thể do bạn chọn trên bản đồ; mình không đưa toạ độ vì chưa kiểm chứng được từng điểm.
2. Với mỗi ứng viên, gọi Flow Segment Data 3 lần vào 3 khung giờ (cao điểm sáng, giữa trưa, cao điểm chiều). Chi phí khoảng 60 request, tức 0,3% hạn mức tháng.
3. Giữ điểm có `confidence` từ 0,7 trở lên ở cả 3 lần gọi và có `currentSpeed` thay đổi rõ giữa các khung giờ. Ngưỡng 0,7 là lựa chọn thiết kế, chỉnh theo kết quả thực tế.
4. Chọn 10 điểm cuối, mỗi vùng ít nhất 1 điểm và mỗi ô r8 chỉ chứa tối đa một điểm. Nếu hai ứng viên tốt nằm chung một ô thì giữ một điểm, hoặc dùng độ phân giải 9 (cạnh khoảng 0,17 km) cho riêng việc tách hai điểm đó. Ghi lý do chọn vào README.

**Vì sao H3 độ phân giải 8 và 6**

| Độ phân giải | Cạnh ô (xấp xỉ) | Dùng cho |
| --- | --- | --- |
| 8 | 0,46 km | Ô của điểm đo giao thông, đủ nhỏ để mỗi điểm nằm trong ô riêng |
| 6 | 3,2 km | Ô cha dùng để join với thời tiết, vì dữ liệu mô hình thô hơn nhiều so với 1 km, nên gộp nhiều ô r8 vào một ô để tránh gọi API lặp lại |

Bước join dùng `cell_to_parent(cell_r8, 6)`. Tập toạ độ gọi Open-Meteo cho thời tiết là các ô cha khác nhau của 10 điểm đo (lấy toạ độ tâm ô), nên mỗi giờ chỉ cần 1 request nhiều toạ độ. AQI chỉ cần 1 request cho một toạ độ ở trung tâm thành phố, vì dữ liệu gốc có ô lưới khoảng 45 km.

**File `config/points.yaml`** (mẫu cấu trúc, điền giá trị sau giai đoạn 1)

```yaml
city: hcmc
h3_res_traffic: 8
h3_res_weather: 6
incidents_bbox: [min_lon, min_lat, max_lon, max_lat]   # 1 bbox lõi thành phố
points:
  - id: p01
    name: <tên trục đường hoặc nút giao>
    lat: 0.0
    lon: 0.0
    zone: center            # center | north | south | east | west
    priority: 1             # 1 = bỏ cuối cùng khi cắt quota
    min_confidence: 0.7
    zoom: 10                # tham số zoom của Flow Segment Data, giữ cố định cho từng điểm
```

Quy ước: `id` không đổi sau khi đã có dữ liệu, vì nó là khoá trong mọi bảng và topic.

## 5. Kafka topics và schema message

Pipeline dùng 5 topic dữ liệu, 1 topic vận hành và các topic DLQ, tất cả message là JSON có chung một envelope. Dùng JSON kèm model pydantic trong repo thay vì Avro và Schema Registry, để Docker Compose nhẹ hơn; nếu sau này cần schema evolution chặt thì chuyển sang Avro mà không đổi envelope.

| Topic | Key | Nguồn | Partition (dev) | Retention |
| --- | --- | --- | --- | --- |
| `raw.traffic_flow` | `point_id` | TomTom Flow | 3 | 30 phút |
| `raw.traffic_incident` | `incident_id` | TomTom Incidents | 3 | 30 phút |
| `raw.weather` | `h3_r6` | Open-Meteo | 3 | 7 ngày |
| `raw.aqi` | `city` | Open-Meteo Air Quality | 3 | 7 ngày |
| `ops.api_usage` | `provider` | Producer | 1 | 30 ngày |
| `dlq.<topic>` | như topic gốc | Message lỗi parse hoặc schema; DLQ của topic giao thông giữ 30 phút như topic gốc | 1 | 30 ngày |

Replication factor là 1 vì chạy local một broker. Key theo `point_id` hoặc `h3_r6` để mọi bản ghi của cùng một điểm vào cùng partition và giữ thứ tự.

Hai topic giao thông (kèm DLQ của chúng) chỉ giữ 30 phút: `retention.ms` = 1800000 và `segment.ms` = 300000 để Kafka xoá đúng hạn. Mục 11.4 điều khoản TomTom chỉ cho cache theo header `cache-control` và không quá `max-age`, nên 30 phút là giá trị tạm; đặt lại theo header của response thật ở giai đoạn 1, và nếu response không có header thì giữ ngắn nhất có thể rồi hỏi TomTom.

**Envelope chung**

| Trường | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `event_id` | string | SHA-1 của `source`, khoá và `event_time`; dùng để khử trùng lặp |
| `source` | string | `tomtom_flow`, `tomtom_incident`, `open_meteo_weather`, `open_meteo_aqi` |
| `event_time` | timestamp UTC | Thời điểm dữ liệu mô tả |
| `ingest_time` | timestamp UTC | Thời điểm producer nhận response |
| `schema_version` | int | Bắt đầu từ 1 |
| `payload` | object | Theo từng topic, xem dưới |

`event_time` của thời tiết và AQI lấy từ mốc `time` trong response của Open-Meteo. Response của Flow Segment Data không có mốc thời gian đo (đã đối chiếu tài liệu), nên `event_time` của giao thông là mốc của slot poll (làm tròn xuống phút chia hết cho chu kỳ); ghi rõ điều này trong README, vì nó khiến event time và processing time chênh nhau tối đa vài giây. `frc` là cấp đường và `segment_hash` là mã băm từ toạ độ đầu và cuối của đoạn đường trả về, dùng để phát hiện khi API đổi sang đoạn đường khác cho cùng một điểm đo.

**Payload từng topic** (tên trường theo tài liệu API; đối chiếu với response thật ở giai đoạn 1 trước khi chốt)

| Topic | Trường chính |
| --- | --- |
| `raw.traffic_flow` | `point_id`, `h3_r8`, `frc`, `segment_hash`, `current_speed_kmh`, `free_flow_speed_kmh`, `current_travel_time_s`, `free_flow_travel_time_s`, `confidence`, `road_closure` |
| `raw.traffic_incident` | `incident_id`, `category`, `delay_s`, `length_m`, `lat`, `lon`, `h3_r8`, `start_time`, `end_time` |
| `raw.weather` | `h3_r6`, `temperature_c`, `precipitation_mm`, `humidity_pct`, `wind_speed_ms`, `weather_code` |
| `raw.aqi` | `city`, `pm2_5`, `pm10`, `us_aqi`, `european_aqi` |

Producer gọi Open-Meteo với `wind_speed_unit=ms` và `timezone=Asia/Bangkok`, nên `wind_speed_ms` đúng đơn vị m/s (mặc định của API là km/h). Theo tài liệu Open-Meteo, `precipitation` trong `current` là tổng của khoảng `interval` (900 giây) liền trước; ở TP.HCM dữ liệu 15 phút được nội suy từ dữ liệu theo giờ, nên lấy thời tiết mỗi giờ là đủ. Giá trị `frc` mang dạng chuỗi (ví dụ `FRC3`) và cần xác nhận lại với response thật.

Ví dụ một message `raw.traffic_flow`:

```json
{
  "event_id": "b1c7…",
  "source": "tomtom_flow",
  "event_time": "2026-11-05T01:30:00Z",
  "ingest_time": "2026-11-05T01:30:02Z",
  "schema_version": 1,
  "payload": {
    "point_id": "p03",
    "h3_r8": "88…",
    "frc": "FRC3",
    "segment_hash": "9f2c…",
    "current_speed_kmh": 18,
    "free_flow_speed_kmh": 42,
    "current_travel_time_s": 160,
    "free_flow_travel_time_s": 69,
    "confidence": 0.92,
    "road_closure": false
  }
}
```

Các giá trị trong ví dụ chỉ minh hoạ định dạng, không phải số đo thật. `ops.api_usage` mang `provider`, `endpoint`, `status_code`, `latency_ms`, `month_used`, `month_limit`, và là nguồn cho panel quota ở mục 9.

## 6. Producer: lịch poll, quota guard và xử lý lỗi

Một service Python (asyncio, httpx, APScheduler) chạy cả 4 luồng poll, không dùng Airflow, vì khối lượng chỉ khoảng 610 request mỗi ngày và Airflow tốn RAM mà không thêm giá trị. Mỗi lần poll đi qua cùng một chuỗi: kiểm quota, kiểm circuit breaker, gọi API, kiểm schema, gửi Kafka, ghi usage.

**Chuỗi xử lý một lần poll**

1. Tính `slot_time` từ lịch (mục 2), làm tròn xuống đúng chu kỳ. Mọi `event_id` dẫn xuất từ `slot_time` nên chạy lại cùng slot cho cùng `event_id`.
2. Quota guard đọc bộ đếm tháng; nếu đã dùng từ 95% hạn mức thì bỏ qua các điểm `priority` thấp, và dừng hẳn ở 100%.
3. Circuit breaker của nhà cung cấp: nếu đang mở thì bỏ qua và ghi `skipped` vào `ops.api_usage`.
4. Gọi API với timeout 10 giây.
5. Parse bằng model pydantic; lỗi parse đi vào `dlq.<topic>` kèm response gốc.
6. Gửi Kafka với `enable.idempotence=true` và `acks=all`.
7. Cập nhật bộ đếm quota và gửi một bản ghi `ops.api_usage`.

**Chính sách lỗi**

| Tình huống | Xử lý |
| --- | --- |
| Timeout, lỗi 5xx | Thử lại tối đa 3 lần, chờ luỹ thừa 2 có jitter (khoảng 1, 2, 4 giây), mỗi lần thử vẫn tính vào quota |
| 429 | Tôn trọng `Retry-After` nếu có, không thử lại quá 1 lần, rồi tính là một lỗi cho circuit breaker |
| 401, 403 | Không thử lại, mở circuit ngay và cảnh báo (khả năng sai key hoặc hết hạn mức) |
| Response sai schema | Đẩy vào DLQ, không thử lại |
| 5 lỗi liên tiếp của một nhà cung cấp | Mở circuit 5 phút, sau đó cho 1 request thăm dò (half-open) |
| Container khởi động lại | Đọc lại bộ đếm quota từ file, tiếp tục từ slot hiện tại |

Slot bỏ lỡ của giao thông không bù lại được vì không có API lịch sử; chỉ ghi `missed_slot` vào usage để dashboard hiện khoảng trống dữ liệu. Thời tiết có thể bù bằng endpoint lịch sử của Open-Meteo.

**Trạng thái quota**: lưu trong SQLite trên một Docker volume (`state/quota.db`), khoá theo `provider`, `endpoint` và tháng. Giả định TomTom reset theo tháng dương lịch UTC; xác nhận trong tài liệu hoặc bảng usage và chỉnh nếu khác.

**Cấu hình và bí mật**: toạ độ và lịch trong `config/`, key TomTom trong `.env` (đã có trong `.gitignore`), có `.env.example` làm mẫu. Open-Meteo không cần key.

**Khung mã chính**

```python
async def poll_traffic(slot_time):
    for p in points_by_priority():
        if not quota.allow("tomtom", "flow", p.priority):
            ops.skipped(p.id, "quota"); continue
        if breaker("tomtom").is_open():
            ops.skipped(p.id, "circuit_open"); continue
        try:
            raw = await fetch_flow(p)          # retry + timeout bên trong
            msg = TrafficFlow.model_validate(to_payload(raw, p, slot_time))
            producer.send("raw.traffic_flow", key=p.id, value=envelope(msg, slot_time))
            breaker("tomtom").success()
        except ValidationError:
            producer.send("dlq.raw.traffic_flow", key=p.id, value=raw)
        except ApiError as e:
            breaker("tomtom").failure(e)
        finally:
            quota.count("tomtom", "flow"); ops.usage(...)
```

## 7. Xử lý streaming: watermark và join

Hai job Spark Structured Streaming làm toàn bộ phần xử lý: `raw_to_parquet` ghi thời tiết và AQI ra Parquet (giao thông không ghi), và `live_enrich` tính chỉ số kẹt xe cho từng mẫu giao thông rồi join với thời tiết theo ô và theo giờ; AQI được join ở dbt (mục 8). Không gom cửa sổ 30 phút cho giao thông nữa, vì cửa sổ đòi giữ mẫu lâu hơn thời hạn cache mà mục 11.4 điều khoản TomTom cho phép; mỗi mẫu được xử lý riêng và chỉ nằm trong trạng thái Spark vài phút.

| Job | Đọc | Làm gì | Ghi |
| --- | --- | --- | --- |
| `raw_to_parquet` | `raw.weather`, `raw.aqi` | Parse, khử trùng lặp theo `event_id`, thêm cột `dt` và `hour` | Parquet theo `source/dt=YYYY-MM-DD/hour=HH` trên `data/lake/` |
| `live_enrich` | `raw.traffic_flow`, `raw.weather` | Tính chỉ số kẹt xe từng mẫu, join thời tiết theo ô r6 và giờ | Topic `curated.traffic_live` (giữ 30 phút) |

**Quy tắc tính**

- Chỉ số kẹt xe của một mẫu: `congestion = 1 - current_speed / free_flow_speed`, kẹp trong \[0, 1\]. Mẫu có `road_closure = true` hoặc `confidence` dưới `min_confidence` của điểm thì đánh dấu `valid = false` và vẫn xuất ra để dashboard hiển thị là không đủ tin cậy.
- Mỗi mẫu giao thông xuất ra: `point_id`, `event_time`, `current_speed_kmh`, `free_flow_speed_kmh`, `congestion`, `valid`, cùng các cột thời tiết và `weather_age_min`.
- Watermark 10 phút trên `event_time` cho cả hai luồng. Mẫu giao thông đến muộn hơn 10 phút bị bỏ vì không được lưu để tính lại; thời tiết đến muộn vẫn nằm trong Parquet nên dbt tính lại được (mục 8).
- Khử trùng lặp: `dropDuplicates(["event_id"])` có watermark, nên mỗi `event_id` chỉ được giữ trong trạng thái 10 phút.

**Join khác độ phân giải**

Thời tiết theo giờ, giao thông theo từng mẫu. Job join bằng điều kiện bằng nhau trên `h3_r6` (ô cha của ô r8) và `hour_bucket = date_trunc('hour', event_time)`, cộng ràng buộc khoảng thời gian để Spark giới hạn trạng thái. AQI không join ở đây vì chỉ đổi vài giờ một lần và dùng chung cho cả thành phố; dbt join AQI vào thời tiết bằng `ASOF JOIN` (mục 8). Dùng left outer join; khi thiếu thời tiết, cột thời tiết là null và `weather_age_min` là null thay vì chặn cả mẫu. Với mẫu có thời tiết, `weather_age_min` = phút từ đầu giờ đến `event_time`.

**Độ trễ chấp nhận được**: một mẫu giao thông được xuất ra sau tối đa khoảng 1 phút trigger; khi thiếu thời tiết, Spark chờ thêm tới watermark 10 phút mới xuất dòng với cột thời tiết null. Panel gần real-time của dashboard đọc thẳng bảng `raw_traffic_flow` (mục 8), không chờ job này.

**Vận hành**: trigger mỗi 1 phút; checkpoint trên volume `data/checkpoints/<job>`; ghim phiên bản Spark và gói `spark-sql-kafka` cùng phiên bản; mỗi job chạy trong một container riêng để khởi động lại độc lập. Nếu đổi schema hoặc logic xử lý thì xoá checkpoint của job đó và phát lại từ Kafka (topic giao thông giữ 30 phút; thời tiết phát lại được từ Parquet).

**Khung mã `live_enrich`**

```python
flow = (read_topic("raw.traffic_flow").select(parse_flow("value").alias("v")).select("v.*")
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["event_id"]))

live = (flow
        .withColumn("valid", ~F.col("road_closure") & (F.col("confidence") >= F.col("min_confidence")))
        .withColumn("congestion", F.greatest(F.lit(0.0), F.least(F.lit(1.0), 1 - F.col("current_speed_kmh")/F.col("free_flow_speed_kmh")))))

weather = read_weather().withWatermark("event_time", "10 minutes")   # hour_bucket = event_time đã cắt về giờ
joined = live.alias("t").join(weather.alias("w"),
    F.expr("t.h3_r6 = w.h3_r6 AND w.hour_bucket = date_trunc('hour', t.event_time) "
           "AND w.event_time BETWEEN t.event_time - interval 1 hour AND t.event_time + interval 1 hour"),
    "leftOuter")

(joined.select(to_json(struct("*")).alias("value"), F.col("t.point_id").alias("key"))
 .writeStream.format("kafka").option("topic", "curated.traffic_live")
 .option("checkpointLocation", "/data/checkpoints/live_enrich")
 .trigger(processingTime="1 minute").outputMode("append").start())
```

Khung mã trên là điểm xuất phát, chưa chạy thử: tên cột `min_confidence` cần được join từ cấu hình điểm đo (broadcast), và cú pháp join stream-stream outer cần thử với dữ liệu thật ở giai đoạn 2.

## 8. Lưu trữ và mô hình dữ liệu

Thời tiết và AQI đi vào hai nơi: Parquet thô làm nguồn sự thật để tính lại, và ClickHouse làm kho truy vấn được mô hình hoá bằng dbt. Giao thông chỉ vào ClickHouse với TTL 30 phút để dashboard hiển thị mẫu mới nhất, không ghi Parquet. Chọn ClickHouse thay TimescaleDB vì có engine đọc thẳng từ Kafka, có TTL theo hàng và có `ASOF JOIN` để join AQI theo giờ gần nhất; TimescaleDB vẫn là phương án thay thế hợp lệ nếu bạn quen PostgreSQL hơn.

| Lớp | Công nghệ | Nội dung | Ghi chú |
| --- | --- | --- | --- |
| Lake thô | Parquet trên `data/lake/` | Bản sao message `raw.weather` và `raw.aqi`, không có giao thông | Cấu trúc thư mục giống S3 để chuyển lên cloud sau này |
| Raw | ClickHouse, `ReplacingMergeTree` | `raw_weather`, `raw_aqi` (lưu lâu dài); `raw_traffic_flow`, `raw_traffic_incident` (TTL 30 phút) | Đổ từ Kafka bằng Kafka engine + materialized view; khoá sắp xếp làm dedup |
| Curated | ClickHouse | `curated_traffic_live` (TTL 30 phút) | Đổ từ topic `curated.traffic_live`, khoá `(point_id, event_time)` |
| Staging, mart | dbt | Xem bảng dưới | Chạy theo lịch hoặc sau mỗi giờ |

**DDL mẫu cho `raw_traffic_flow`** (chưa chạy thử, kiểm tra cú pháp khi dựng ở giai đoạn 3)

```sql
CREATE TABLE kafka_traffic_flow (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'raw.traffic_flow',
         kafka_group_name = 'ch_traffic_flow', kafka_format = 'JSONAsString';

CREATE TABLE raw_traffic_flow (
  event_id String,
  event_time DateTime('UTC'),
  ingest_time DateTime('UTC'),
  point_id LowCardinality(String),
  h3_r8 String,
  frc LowCardinality(String),
  segment_hash String,
  current_speed_kmh Float32,
  free_flow_speed_kmh Float32,
  current_travel_time_s UInt32,
  free_flow_travel_time_s UInt32,
  confidence Float32,
  road_closure UInt8
) ENGINE = ReplacingMergeTree(ingest_time)
ORDER BY (point_id, event_time)
TTL event_time + INTERVAL 30 MINUTE
SETTINGS merge_with_ttl_timeout = 300, ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW mv_traffic_flow TO raw_traffic_flow AS
SELECT JSONExtractString(raw, 'event_id') AS event_id,
       parseDateTimeBestEffort(JSONExtractString(raw, 'event_time')) AS event_time,
       parseDateTimeBestEffort(JSONExtractString(raw, 'ingest_time')) AS ingest_time,
       JSONExtractString(raw, 'payload', 'point_id') AS point_id,
       JSONExtractString(raw, 'payload', 'h3_r8') AS h3_r8,
       JSONExtractString(raw, 'payload', 'frc') AS frc,
       JSONExtractString(raw, 'payload', 'segment_hash') AS segment_hash,
       JSONExtractFloat(raw, 'payload', 'current_speed_kmh') AS current_speed_kmh,
       JSONExtractFloat(raw, 'payload', 'free_flow_speed_kmh') AS free_flow_speed_kmh,
       JSONExtractUInt(raw, 'payload', 'current_travel_time_s') AS current_travel_time_s,
       JSONExtractUInt(raw, 'payload', 'free_flow_travel_time_s') AS free_flow_travel_time_s,
       JSONExtractFloat(raw, 'payload', 'confidence') AS confidence,
       JSONExtractBool(raw, 'payload', 'road_closure') AS road_closure
FROM kafka_traffic_flow;
```

TTL của ClickHouse chỉ chạy khi merge nền, nên dòng cũ có thể nằm lại thêm vài phút; mọi truy vấn và dashboard đọc `raw_traffic_flow` phải lọc `event_time` trong 30 phút gần nhất. Bảng `raw_traffic_incident` và `curated_traffic_live` dùng cùng quy tắc TTL.

`ReplacingMergeTree` chỉ gộp bản trùng khi merge nền chạy, nên truy vấn cần dữ liệu đã khử trùng lặp phải dùng `FINAL` hoặc `argMax`. Với khoảng 610 dòng mỗi ngày, `FINAL` rẻ và là mặc định trong các model dbt.

**Các model dbt**

| Model | Lớp | Mục đích |
| --- | --- | --- |
| `stg_weather`, `stg_aqi` | staging | Đổi tên, ép kiểu, khử trùng lặp bằng `FINAL`, chuẩn múi giờ UTC+7 |
| `int_weather_aqi_asof` | intermediate | `ASOF JOIN` thời tiết theo giờ với AQI gần nhất của thành phố; là nơi duy nhất join AQI |
| `mart_weather_hourly` | mart | Một dòng cho mỗi ô r6 và mỗi giờ, gồm nhiệt độ, mưa, gió, AQI |
| `mart_rain_days` | mart | Số giờ mưa và tổng lượng mưa mỗi ngày theo ô r6 |
| `mart_aqi_profile` | mart | AQI trung bình theo giờ trong ngày và theo thứ (Hai đến Chủ nhật) |
| `dim_calendar` | dimension | Ngày, thứ, cờ lễ, cờ Tết; nguồn là seed CSV do bạn tự nhập ngày lễ |

Định nghĩa chỉ số chính của mục tiêu dự án: **giờ có mưa** là giờ có `precipitation_mm >= 0.5` ở ô r6; ngưỡng mặc định, chỉnh sau khi xem phân bố dữ liệu thật. Phân tích tác động của mưa lên tốc độ giao thông đã bị bỏ khỏi phạm vi vì cần lịch sử giao thông.

**Backfill**: thời tiết có thể nạp lịch sử từ endpoint archive của Open-Meteo (dữ liệu từ năm 1940), chạy một lần bằng script `scripts/backfill_weather.py` vào `raw_weather`. AQI toàn cầu của Open-Meteo chỉ có dữ liệu từ tháng 8/2022 và mình chưa kiểm tra endpoint lịch sử cho Việt Nam, nên coi như AQI chỉ có từ ngày bắt đầu thu thập. Giao thông không có lịch sử và không được lưu (mục 11.4 điều khoản TomTom).

## 9. Data quality và giám sát

Chất lượng dữ liệu được kiểm bằng test dbt, còn sức khoẻ pipeline được theo dõi bằng các chỉ số lấy từ topic `ops.api_usage` và từ độ trễ dữ liệu trong ClickHouse, hiển thị và cảnh báo trên Grafana. Các ngưỡng dưới đây là lựa chọn ban đầu, chỉnh sau 7-14 ngày có dữ liệu thật.

**Test dbt**

| Đối tượng | Test | Ngưỡng |
| --- | --- | --- |
| `raw_*` | `not_null` và `unique` trên `event_id` (sau `FINAL`) | Không có ngoại lệ |
| `current_speed_kmh` | Trong khoảng | 0 đến 150 |
| `confidence`, `congestion` | Trong khoảng | 0 đến 1 |
| `precipitation_mm` | Không âm | ≥ 0 |
| `us_aqi` | Trong khoảng | 0 đến 500 |
| `point_id` | `relationships` tới seed điểm đo | Mọi giá trị phải có trong seed |
| Số dòng raw\_weather mỗi ngày | Test tuỳ chỉnh | Gần 24 dòng mỗi ô r6 (±10%) |
| Nguồn | `dbt source freshness` | Cảnh báo sau 60 phút với giao thông, 120 phút với thời tiết, 8 giờ với AQI |

**Chỉ số vận hành và cảnh báo**

| Chỉ số | Cách tính | Cảnh báo |
| --- | --- | --- |
| Độ tươi dữ liệu giao thông | Hiện tại trừ `max(event_time)` của `raw_traffic_flow`, chỉ trong 06:00-22:00 | Hơn 35 phút |
| Độ tươi thời tiết, AQI | Như trên với `raw_weather`, `raw_aqi` | Thời tiết hơn 90 phút; AQI hơn 8 giờ |
| Mức dùng quota tháng | `month_used / month_limit` theo nhà cung cấp | 80% cảnh báo, 95% nghiêm trọng |
| Tỷ lệ lỗi API | Số lần gọi lỗi trên tổng, cửa sổ 15 phút | Trên 20% |
| Độ trễ API | p95 của `latency_ms` | Trên 5 giây |
| DLQ | Số dòng mới trong `dlq.*` trong 1 giờ | Lớn hơn 0 |
| Slot bỏ lỡ | Số `missed_slot` mỗi ngày | Lớn hơn 5% số slot kế hoạch |
| Độ trễ job streaming | Hiện tại trừ `max(event_time)` của `curated_traffic_live` | Hơn 10 phút |

Cảnh báo gửi qua contact point Grafana mà bạn chọn (email hoặc Telegram). Mọi cảnh báo phải kèm đường dẫn đến bảng điều khiển liên quan để xử lý nhanh.

**Runbook ngắn**: khi quota gần cạn thì giảm số điểm priority thấp trong `points.yaml` thay vì kéo giãn chu kỳ peak; khi circuit mở quá 30 phút thì kiểm tra key và trạng thái dịch vụ của nhà cung cấp; khi DLQ có dòng thì so response gốc với model pydantic để biết nhà cung cấp có đổi schema không.

## 10. Mô hình dự báo kẹt xe (bỏ khỏi phạm vi)

Mô hình dự báo kẹt xe đã bị bỏ khỏi phạm vi, vì điều khoản TomTom (mục 11.4 và 11.6.1) không cho lưu lịch sử giao thông để huấn luyện. Chỉ mở lại nếu TomTom xác nhận bằng văn bản; khi đó thiết kế cũ (LightGBM, hai mô hình nền, dự báo 30 và 60 phút) có thể lấy lại từ lịch sử phiên bản của tài liệu này.

## 11. Dashboard

Dùng Grafana với plugin nguồn dữ liệu ClickHouse, gồm 4 dashboard được provision bằng file trong repo để dựng lại được bằng một lệnh. Bản đồ dùng panel Geomap với toạ độ điểm đo lấy từ bảng `dim_points` (seed từ `points.yaml`), vì Grafana không vẽ trực tiếp ô H3, và vì mỗi giá trị chỉ mô tả một đoạn đường. Mỗi điểm đo được vẽ thành một chấm tại toạ độ của nó, màu theo mức kẹt xe; không tô cả ô lục giác, để người xem không hiểu nhầm rằng cả khu đang kẹt như vậy.

| Dashboard | Panel chính | Nguồn |
| --- | --- | --- |
| Tổng quan | Bản đồ 10 điểm đo dạng chấm, màu theo `congestion` của mẫu mới nhất; bảng tốc độ hiện tại; mưa theo thời gian; AQI chung của thành phố; số sự cố đang mở | `curated_traffic_live`, `raw_weather`, `raw_aqi`, `raw_traffic_incident` |
| Thời tiết | Mưa, nhiệt độ, gió theo giờ theo ô r6; số giờ mưa mỗi ngày | `mart_weather_hourly`, `mart_rain_days` |
| Chất lượng không khí | AQI và PM2,5 theo giờ; hồ sơ AQI theo giờ trong ngày và theo thứ | `mart_weather_hourly`, `mart_aqi_profile` |
| Vận hành | Độ tươi dữ liệu, gauge quota theo nhà cung cấp, tỷ lệ lỗi và độ trễ API, DLQ, slot bỏ lỡ | `raw_ops_api_usage` và các bảng raw |

Quy ước hiển thị: múi giờ dashboard là Asia/Ho\_Chi\_Minh, mọi thời điểm lưu UTC; thang màu kẹt xe cố định 0 đến 1 để so sánh giữa các ngày; chân trang ghi nguồn dữ liệu (Open-Meteo CC BY 4.0, TomTom) theo yêu cầu ghi nguồn; giữ nguyên logo và thông báo bản quyền của TomTom (mục 17.3 điều khoản). Dữ liệu giao thông hiển thị là mẫu mới nhất, không có đồ thị lịch sử.

**Tiêu chí hoàn thành**: dashboard Tổng quan cập nhật được bằng dữ liệu của chính giờ vừa qua, và dashboard Vận hành cho thấy rõ khi tắt producer 40 phút (cảnh báo độ tươi dữ liệu bật).

## 12. Cấu trúc repo, Docker Compose và CI/CD

Một monorepo, chạy toàn bộ bằng `make up`, với mỗi thành phần ở một thư mục riêng và một service riêng trong Compose. CI chỉ kiểm tra chất lượng mã và dbt, không triển khai gì lên cloud.

**Cây thư mục**

```text
hcmc-traffic-pipeline/
├── README.md                # quyết định thiết kế, ngân sách request, hạn chế
├── Makefile                 # up, down, logs, test, dbt-build, backfill
├── docker-compose.yml
├── .env.example
├── config/
│   └── points.yaml
├── producer/                # Python: scheduler, clients, quota, breaker, models
│   ├── app/
│   └── tests/
├── streaming/               # PySpark: raw_to_parquet.py, live_enrich.py
├── clickhouse/init/         # DDL: kafka_*, raw_*, mv_*, curated_traffic_live (TTL 30 phút)
├── dbt/                     # models/, seeds/ (dim_points, dim_calendar), tests/
├── grafana/provisioning/    # datasources, dashboards (JSON)
├── scripts/                 # backfill_weather.py, select_points.py
├── contracts/               # JSON mẫu cho từng topic
├── data/                    # lake/ (chỉ thời tiết, AQI), checkpoints/ (không commit)
└── .github/workflows/ci.yml
```

**Service trong Compose**

| Service | Vai trò | Ghi chú |
| --- | --- | --- |
| `kafka` | Broker KRaft một node | Tạo topic bằng service khởi tạo chạy một lần |
| `kafka-ui` | Xem topic và message | Tuỳ chọn, tắt khi cần tiết kiệm RAM |
| `producer` | 4 luồng poll | Mount `config/` và volume `state/` cho quota |
| `spark-raw`, `spark-enrich` | Hai job streaming | Mount `data/` cho Parquet và checkpoint |
| `clickhouse` | Kho truy vấn | Chạy các file trong `clickhouse/init/` khi tạo mới |
| `dbt` | Chạy theo lệnh | Profile `tools`, không chạy mặc định |
| `grafana` | Dashboard và cảnh báo | Provision từ thư mục repo |

Đặt `mem_limit` cho từng service và đo bằng `docker stats` trong giai đoạn 2 để biết máy 16 GB còn dư bao nhiêu; Spark là thành phần nặng nhất và nên được giới hạn trước. Mọi service có `healthcheck` và `restart: unless-stopped`; các service phụ thuộc dùng `depends_on` với điều kiện `service_healthy`.

**Biến môi trường** (`.env.example`): `TOMTOM_API_KEY`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `GRAFANA_ADMIN_PASSWORD`, `TZ=UTC`. Không commit `.env`.

**CI (GitHub Actions) trên mỗi pull request**

1. Lint và format Python bằng `ruff`.
2. Chạy `pytest` cho producer: parse response mẫu, quota guard, circuit breaker, tính `congestion`, tạo `event_id` ổn định.
3. `dbt parse` và `sqlfluff lint` cho các model.
4. Build image của producer và job Spark để bắt lỗi Dockerfile sớm.
5. Tuỳ chọn: một smoke test dựng Compose rút gọn (Kafka, producer với API giả) và kiểm tra có message trong topic.

Test không gọi API thật nên không tốn quota; dùng response mẫu trong `contracts/` và `producer/tests/fixtures/`.

## 13. Lộ trình 4 giai đoạn và tiêu chí hoàn thành

Bốn giai đoạn, mỗi giai đoạn kết thúc bằng một tiêu chí kiểm tra được. Collector thời tiết và AQI nên chạy liên tục từ cuối giai đoạn 1 để có lịch sử; giao thông chỉ chạy thời gian thực và không lưu lịch sử. Làm theo thứ tự các mục; hoàn thành tiêu chí của giai đoạn trước rồi mới sang giai đoạn sau.

**Giai đoạn 1: nền tảng và thu thập**

- [ ] Tạo repo, Compose với Kafka, ClickHouse, Grafana, và `.env.example`
- [ ] Đăng ký TomTom, đọc lại điều khoản bảng giá mới, ghi hạn mức tháng, cách reset và giá trị header cache-control của response thật vào README
- [ ] Chạy `select_points.py` cho khoảng 20 ứng viên, chọn 10 điểm, điền `points.yaml`
- [ ] Viết producer thời tiết và AQI (Open-Meteo), topic `raw.weather`, `raw.aqi`
- [ ] Viết producer giao thông cùng quota guard, retry, circuit breaker; chạy thật với lịch ở mục 2
- [ ] Job `raw_to_parquet` ghi lake

* Tiêu chí: sau 24 giờ producer gọi đủ 550 ± 5% request giao thông theo bộ đếm quota, topic `raw.traffic_flow` không giữ bản ghi nào cũ hơn 30 phút, `raw.weather` và `raw.aqi` có đủ bản ghi, không bản ghi DLQ ngoài ý muốn, bộ đếm quota khớp bảng usage của TomTom

**Giai đoạn 2: streaming và join**

- [ ] Producer sự cố giao thông (`raw.traffic_incident`)
- [ ] Job `live_enrich`: congestion từng mẫu, watermark, join thời tiết
- [ ] Topic `curated.traffic_live`, kiểm tra bằng consumer thử
- [ ] Kafka engine và bảng `raw_*` trong ClickHouse
- [ ] Đo bộ nhớ bằng `docker stats`, đặt `mem_limit`

* Tiêu chí: mỗi mẫu giao thông ra đúng một dòng; khi tắt producer thời tiết, dòng giao thông vẫn ra với cột thời tiết null

**Giai đoạn 3: kho dữ liệu, dbt và chất lượng**

- [ ] Seed `dim_points`, `dim_calendar` (nhập ngày lễ và Tết do bạn xác minh)
- [ ] Model staging, `int_weather_aqi_asof`, `mart_weather_hourly`, `mart_rain_days`, `mart_aqi_profile`
- [ ] Test dbt và source freshness theo mục 9
- [ ] Backfill thời tiết lịch sử bằng `backfill_weather.py`
- [ ] Kiểm tra TTL: không có dòng giao thông cũ hơn 45 phút trong ClickHouse

* Tiêu chí: `dbt build` xanh; truy vấn kiểm tra không thấy dòng giao thông cũ hơn 45 phút trong ClickHouse

**Giai đoạn 4: dashboard, hoàn thiện**

- [ ] 4 dashboard Grafana provision bằng file, cảnh báo theo mục 9
- [ ] Kiểm tra tuân thủ: không có lịch sử giao thông trong Parquet, ClickHouse và Kafka quá 45 phút
- [ ] GitHub Actions theo mục 12
- [ ] README: sơ đồ kiến trúc, ngân sách request, quyết định thiết kế, hạn chế, hướng dẫn chạy
- [ ] Diễn tập sự cố: tắt producer 40 phút, kiểm tra cảnh báo bật và dữ liệu hồi phục

* Tiêu chí: người khác clone repo, điền key và chạy `make up` là có dashboard; README ghi rõ giao thông chỉ là thời gian thực và lý do

**Tiêu chí tổng thể của dự án**

| Tiêu chí | Cách kiểm |
| --- | --- |
| Không vượt hạn mức miễn phí | Mức dùng quota tháng dưới 95% trên dashboard Vận hành |
| Không mất dữ liệu khi khởi động lại | Khởi động lại producer và Spark giữa chừng, không có `event_id` trùng hoặc khoảng trống ngoài slot bị bỏ |
| Dữ liệu đến muộn được xử lý đúng | Phát lại một batch có `event_time` cũ 5 phút và 15 phút, kiểm tra với thời tiết: batch 5 phút vào đúng giờ, batch 15 phút bị bỏ khỏi streaming nhưng còn trong Parquet |
| Tái lập được | `make up` trên máy sạch |
| Tuân thủ điều khoản TomTom | Không có bản ghi giao thông cũ hơn 45 phút ở Kafka, ClickHouse; không có giao thông trong Parquet |

## 14. Rủi ro và việc cần xác nhận trước khi code

Ba rủi ro lớn nhất là điều khoản lưu trữ của TomTom (đã xử lý bằng phương án chỉ thời gian thực), hạn mức TomTom (đổi theo tháng từ 01/07/2026) và độ phủ dữ liệu ở từng tuyến. Cả ba đều có bước giảm thiểu nằm trong giai đoạn 1.

| Rủi ro | Mức ảnh hưởng | Cách giảm thiểu |
| --- | --- | --- |
| Cách đếm request và điều khoản của bảng giá TomTom mới chưa được xác nhận đầy đủ | Cao: có thể làm cạn quota sớm | Đối chiếu bộ đếm với bảng usage sau 2-3 ngày; quota guard dừng ở 95% |
| Một số điểm đo có `confidence` thấp hoặc ít biến động | Trung bình | Bước chọn điểm bằng dữ liệu thật (mục 4); giữ danh sách ứng viên dự phòng |
| Hạn mức 20K/tháng không đủ nếu muốn thêm điểm | Trung bình | Lịch thích ứng, trường `priority`; thêm điểm chỉ khi giảm tần suất ở điểm khác |
| Không có mô hình dự báo vì không lưu lịch sử giao thông | Thấp: chấp nhận để tuân thủ điều khoản | Chỉ mở lại mô hình khi TomTom xác nhận bằng văn bản |
| Mùa mưa ở TP.HCM thường kết thúc khoảng cuối năm, nên dữ liệu thu từ tháng 11 có thể có ít ngày mưa (kiến thức chung, chưa kiểm chứng bằng dữ liệu) | Thấp: chỉ ảnh hưởng thống kê số giờ mưa | Dùng Open-Meteo lịch sử để biết số ngày mưa mỗi giai đoạn; nêu cỡ mẫu trong README |
| AQI từ mô hình lưới khoảng 45 km, cả thành phố gần như cùng một giá trị | Thấp đến trung bình | Ghi rõ trong dashboard; không diễn giải khác biệt giữa các ô gần nhau |
| Join stream-stream outer của Spark khó và dễ lỗi trạng thái | Trung bình | Join theo giờ như mục 7; thử với dữ liệu thật bằng consumer thử |
| Spark nặng so với khối lượng dữ liệu | Thấp | Chấp nhận như lựa chọn để thể hiện kỹ năng; ghi rõ trong README và nêu phương án consumer Python nhẹ |
| Lộ key API hoặc đưa dữ liệu thô lên repo công khai | Cao | `.env` ngoài git, `data/` trong `.gitignore`; đọc điều khoản TomTom về lưu trữ và chia sẻ dữ liệu trước khi công bố bất kỳ tập dữ liệu nào |
| Điều khoản TomTom (mục 11.4, 11.6.1, 11.6.3) cấm lưu Results ngoài cache phía client và cấm tạo cơ sở dữ liệu phái sinh; thiết kế đã đổi sang xử lý giao thông theo thời gian thực, không lưu lịch sử | Trung bình: cần đối chiếu max-age với response thật và xin TomTom xác nhận | Phương án A: Kafka giữ 30 phút, ClickHouse TTL 30 phút, không ghi Parquet; đặt thời hạn theo max-age của response thật và xin TomTom xác nhận |

**Việc cần bạn xác nhận hoặc kiểm tra trước khi viết dòng code đầu tiên**

- [ ] Thành phố là TP.HCM và chạy local, hay bạn muốn Hà Nội hoặc cloud (đổi mục 4 và mục 12)
- [ ] Điều khoản TomTom: lưu trữ dữ liệu lâu dài có được phép không, ghi nguồn thế nào, hạn mức reset khi nào. Đã đọc điều khoản ngày 05/11/2026: mục 11.4 cấm lưu Results, chỉ cho cache phía client theo header cache-control; mục 11.6.1 cấm tạo cơ sở dữ liệu phái sinh; mục 8.5 hạn mức miễn phí không cộng dồn sang tháng sau. Đã chọn phương án chỉ xử lý giao thông theo thời gian thực; còn cần kiểm tra header cache-control của response thật để đặt thời hạn giữ, và hỏi TomTom nếu muốn lưu lịch sử
- [ ] Một lần gọi Flow Segment Data tính là 1 request, và đoạn đường trả về ổn định với zoom cố định (chưa xác nhận: tài liệu chỉ nêu 20K request/tháng, không giải thích cách đếm; cần gọi thật bằng key và đối chiếu bảng usage, dùng segment\_hash để phát hiện đổi đoạn)
- [ ] Tên trường trong response thật của Flow Segment Data và Open-Meteo khớp mục 5 (đã đối chiếu tài liệu Open-Meteo: current có time và interval = 900, gió mặc định km/h; chưa xem response thật vì mạng của phiên làm việc bị chặn, và Flow Segment Data chưa gọi thật)
- [ ] Danh sách 10 điểm đo cuối cùng
- [ ] Ngày bắt đầu thu thập, để lên lịch giai đoạn 4

**Nguồn**

- [TomTom Pricing](https://docs.tomtom.com/pricing) và [Market Coverage của Traffic API](https://developer.tomtom.com/online-traffic/market-coverage)
- [TomTom Intermediate Traffic API](https://docs.tomtom.com/intermediate-traffic-service/documentation/introduction)
- [HERE Traffic Vector Tile coverage](https://docs.here.com/traffic-api/docs/traffic-vector-tile-traffic) và [HERE Base Plan Pricing](https://developers.here.com/plans)
- [Open-Meteo: giới hạn và giấy phép](https://www.itechguides.com/?p=876868), [Air Quality](https://opennetzero.org/open-meteo/open-meteo-air-quality-api)
- Số liệu kiểm chứng ngày 02/11/2026; các mục về cấu hình, DDL và mã nguồn là thiết kế đề xuất, chưa chạy thử; tài liệu Open-Meteo và FAQ TomTom được đối chiếu lại ngày 05/11/2026.
