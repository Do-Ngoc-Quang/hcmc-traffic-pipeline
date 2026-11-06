#!/bin/bash
# Tạo topic theo mục 5 của thiết kế. Chạy lại nhiều lần vẫn an toàn (--if-not-exists).
set -euo pipefail
BS=kafka:9092
K=/opt/kafka/bin/kafka-topics.sh

# Topic giao thông và DLQ của chúng: giữ 30 phút (TomTom mục 11.4).
SHORT="--config retention.ms=1800000 --config segment.ms=300000"
for t in raw.traffic_flow raw.traffic_incident curated.traffic_live; do
  $K --bootstrap-server $BS --create --if-not-exists --topic $t --partitions 3 --replication-factor 1 $SHORT
done
for t in dlq.raw.traffic_flow dlq.raw.traffic_incident; do
  $K --bootstrap-server $BS --create --if-not-exists --topic $t --partitions 1 --replication-factor 1 $SHORT
done

# Thời tiết và AQI: 7 ngày.
for t in raw.weather raw.aqi; do
  $K --bootstrap-server $BS --create --if-not-exists --topic $t --partitions 3 --replication-factor 1 --config retention.ms=604800000
done
for t in dlq.raw.weather dlq.raw.aqi; do
  $K --bootstrap-server $BS --create --if-not-exists --topic $t --partitions 1 --replication-factor 1 --config retention.ms=2592000000
done

# Vận hành: 30 ngày.
$K --bootstrap-server $BS --create --if-not-exists --topic ops.api_usage --partitions 1 --replication-factor 1 --config retention.ms=2592000000

$K --bootstrap-server $BS --list
