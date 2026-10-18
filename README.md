# Bài thực hành số 2 – Thu thập, lưu trữ và tiền xử lý dữ liệu IoT (INT14149)
Sinh viên: Hoàng Thị Phương Thùy – B23DCCN806 – Lớp IoT-05-02

Phát triển từ Bài 1 (nút ESP32 + DHT22 + HC-SR04 + LED trên Wokwi, gửi telemetry lên ThingsBoard).
Bài 2 giữ nguyên mạch và các khóa telemetry, nhưng thay phần nền tảng bằng pipeline tự xây dựng:

```
Wokwi ESP32 → MQTT broker (HiveMQ) → Python Collector → InfluxDB (iot_raw) → Preprocessing → InfluxDB (iot_processed) → Streamlit
```

```
IoT_Practice_2/
├── wokwi/sketch.ino, diagram.json, libraries.txt   # copy vào dự án Wokwi (mạch giống Bài 1)
├── simulator/simulator.py                          # giả lập nút ESP32 khi không mở Wokwi
├── collector/mqtt_collector.py                     # subscribe + validate + ghi InfluxDB
├── preprocessing/preprocess.py                     # missing, outlier, resample, rolling, chuẩn hóa
├── dashboard/app.py                                # Streamlit dashboard (app ứng dụng)
├── scripts/setup_influx.py                         # tạo bucket + retention
├── config/config.example.env, mosquitto.conf
├── docker-compose.yml, requirements.txt
└── report/Bao_cao.docx
```

## 1. Kế thừa và mở rộng so với Bài 1
| | Bài 1 | Bài 2 |
|---|---|---|
| Thiết bị / mạch | ESP32, DHT22 (GPIO15), HC-SR04 (5/18), LED (2) | Giữ nguyên `diagram.json` |
| Telemetry | temperature, humidity, distance_cm, rssi, sequence, uptime_s | Giữ nguyên + `device_id`, `ts` (NTP, ms) |
| Chu kỳ gửi | 5 s | 2 s |
| Đích | ThingsBoard, topic `v1/devices/me/telemetry` | Broker MQTT, topic `iot/<site>/<device>/telemetry` |
| Lưu trữ / xem | ThingsBoard | InfluxDB 2.x + Streamlit |
| Kiểm tra dữ liệu | NaN trên ESP32 | + validate phía collector, loại trùng, mất gói, outlier |

## 2. Cài đặt (một lần)
```bash
python -m venv .venv && .venv\Scripts\activate        # Mac/Linux: source .venv/bin/activate
pip install -r requirements.txt
cp config/config.example.env config/.env              # Windows: copy ...
```
Sửa `config/.env` và `docker-compose.yml`:
- `INFLUX_TOKEN` (.env) = `DOCKER_INFLUXDB_INIT_ADMIN_TOKEN` (docker-compose) – đặt chuỗi riêng.
- `SITE` trong `wokwi/sketch.ino` và `MQTT_TOPIC` trong `.env` phải khớp (vd. `iot/b23dccn847/+/telemetry`). Nên đổi `b23dccn847` thành chuỗi khó đoán vì broker công cộng ai cũng đọc được.
```bash
docker compose up -d                                  # InfluxDB :8086 (admin / admin12345)
python scripts/setup_influx.py                        # bucket iot_raw 30 ngày, iot_processed 90 ngày
```

## 3. Chạy
1. **Wokwi**: tạo dự án ESP32 mới, dán nội dung `wokwi/diagram.json`, `libraries.txt`, `sketch.ino` → Start. Serial Monitor phải có `WiFi`, `MQTT connected!`, và dòng JSON.
   *(Không có Wokwi: `python simulator/simulator.py --site b23dccn847`)*
2. `python collector/mqtt_collector.py` → log "Ghi 1 điểm (xx ms)".
3. Để vài phút rồi: `python preprocessing/preprocess.py --loop 60 --freq 10s --method iqr`
4. `streamlit run dashboard/app.py` → http://localhost:8501

> Wokwi không truy cập được `localhost` của máy bạn, nên MQTT broker phải nằm trên Internet (HiveMQ công cộng hoặc HiveMQ Cloud). Collector chạy trên máy cá nhân và subscribe cùng broker đó.

## 4. Payload
```json
{"device_id":"esp32-01","ts":1760000000000,"sequence":12,"temperature":25.6,"humidity":50.0,"distance_cm":10.02,"rssi":-87,"uptime_s":24}
```
Nếu HC-SR04 timeout/ngoài 2–400 cm, ESP32 bỏ khóa `distance_cm` (Bài 1 gửi -1); collector cũng bỏ riêng trường này nếu gặp.

## 5. Schema InfluxDB
| Measurement | Tags | Fields | Bucket |
|---|---|---|---|
| `sensor_raw` | device_id, location(=site) | temperature, humidity, distance_cm, rssi, sequence, uptime_s, latency_ms | iot_raw (30 ngày) |
| `collector_stats` | collector | write_ms, batch | iot_raw |
| `sensor_processed` | device_id | {temperature, humidity, distance_cm} và `_outlier`, `_roll`, `_delta`, `_z`, `_minmax`, `n_raw` | iot_processed (90 ngày) |

## 6. Kiểm thử sự cố (mở rộng Thí nghiệm 4 của Bài 1)
| Tình huống | Cách tạo | Kỳ vọng |
|---|---|---|
| Mất Wi-Fi | Đổi SSID trong sketch | ESP32 lặp kết nối; dashboard báo "Dữ liệu cũ" > 15 s |
| Collector dừng | Ctrl+C rồi chạy lại | Broker không lưu QoS0; điểm mất thể hiện bằng khoảng trống `sequence` |
| InfluxDB tắt | `docker compose stop influxdb` | Collector log lỗi, giữ buffer, ghi bù khi DB bật lại |
| Payload hỏng / DHT NaN | simulator `--bad-rate` | Bị loại, `stats.invalid` tăng |
| Reset ESP32 | Stop/Start Wokwi | `uptime_s` về nhỏ → ghi nhận "khởi động lại", không tính là mất gói |
| Đặt khoảng cách ngoài miền | HC-SR04 > 400 cm | `distance_cm` bị bỏ, nhiệt độ/độ ẩm vẫn lưu |

## 7. Bảo mật
Không commit `config/.env` và không để access token ThingsBoard trong mã (sketch Bài 1 chứa token thật – hãy đổi token). Broker công cộng không có xác thực/TLS: chỉ dùng cho thực hành; khi triển khai dùng HiveMQ Cloud với `MQTT_PORT=8883`, `MQTT_TLS=true`, `MQTT_USER/PASS`.

## 8. Lấy số liệu cho báo cáo
Sau khi hệ thống chạy 20–30 phút: `python scripts/report_stats.py --hours 1` → in và lưu `report/report_stats.md`
(gói mất, số lần khởi động lại, latency/write_ms Min–P50–P95–Max, rssi, số điểm processed, số outlier).
Số gói lỗi định dạng và gói trùng lấy từ dòng "Dừng. Thống kê" khi nhấn Ctrl+C ở collector.
