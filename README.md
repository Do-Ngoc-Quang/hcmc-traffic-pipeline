# HCMC traffic pipeline

Pipeline streaming thu thập thời tiết, AQI và giao thông thời gian thực cho TP.HCM
(Kafka, Spark, ClickHouse, dbt, Grafana; chạy local bằng Docker Compose).

- Thiết kế đầy đủ: [docs/design.md](docs/design.md)
- Giao thông chỉ xử lý thời gian thực (TTL 30 phút, không lưu lịch sử) theo điều khoản TomTom mục 11.4.
- Nguồn dữ liệu: TomTom; Open-Meteo (CC BY 4.0).

Trạng thái: giai đoạn 1, mới có khung repo. Docker Compose chưa có.
