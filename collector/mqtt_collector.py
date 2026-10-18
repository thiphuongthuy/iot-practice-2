"""Subscribe MQTT -> validate JSON -> ghi InfluxDB (measurement: sensor_raw).

Telemetry kế thừa Bài 1: temperature, humidity, distance_cm, rssi, sequence, uptime_s (+ device_id, ts).
Xử lý:
- Validate: JSON hợp lệ, đủ trường, đúng kiểu, nằm trong giới hạn vật lý (DHT22; HC-SR04 2–400 cm).
  distance_cm ngoài miền (vd -1 khi timeout) -> bỏ riêng trường đó, vẫn lưu nhiệt độ/độ ẩm.
- Trùng lặp: bỏ qua gói có cùng (device, sequence, ts); Influx cũng ghi đè điểm cùng series + timestamp.
- Mất dữ liệu: khoảng trống `sequence`; ESP32 khởi động lại (uptime_s giảm) được nhận diện, không tính là mất gói.
- Lỗi DB: đệm trong buffer và ghi lại ở lần flush sau; MQTT tự reconnect.
- Độ trễ: latency_ms = thời điểm nhận − ts thiết bị (NTP); write_ms = thời gian ghi DB.
"""
import json, logging, os, signal, ssl, sys, time
from collections import deque, OrderedDict
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / "config" / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("collector")

LIMITS = {"temperature": (-40.0, 80.0), "humidity": (0.0, 100.0)}      # bắt buộc
OPTIONAL_LIMITS = {"distance_cm": (2.0, 400.0)}                        # tùy chọn
MAX_BUFFER = 5000

influx = write_api = None
buffer: deque = deque(maxlen=MAX_BUFFER)
seen = OrderedDict()
state: dict = {}   # device -> (last_sequence, last_uptime)
stats = {"received": 0, "ok": 0, "invalid": 0, "duplicate": 0, "lost": 0,
         "restarts": 0, "distance_dropped": 0, "db_errors": 0}


class ValidationError(Exception):
    pass


def _num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and v == v


def validate(payload: bytes, topic: str) -> dict:
    try:
        d = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValidationError(f"JSON không hợp lệ: {e}")
    if not isinstance(d, dict):
        raise ValidationError("Payload không phải object")
    for k in ("temperature", "humidity", "ts"):
        if k not in d:
            raise ValidationError(f"Thiếu trường '{k}'")
    for k, (lo, hi) in LIMITS.items():
        if not _num(d[k]):
            raise ValidationError(f"'{k}' sai kiểu/NaN: {d[k]!r}")
        if not (lo <= d[k] <= hi):
            raise ValidationError(f"'{k}'={d[k]} ngoài giới hạn [{lo},{hi}]")
    if not _num(d["ts"]) or d["ts"] < 1.6e12:
        raise ValidationError("'ts' phải là epoch milliseconds")
    for k, (lo, hi) in OPTIONAL_LIMITS.items():
        if k in d and not (_num(d[k]) and lo <= d[k] <= hi):
            stats["distance_dropped"] += 1
            del d[k]                                   # bỏ riêng trường lỗi, giữ phần còn lại
    for k in ("rssi", "sequence", "uptime_s"):
        if k in d and not _num(d[k]):
            del d[k]
    parts = topic.split("/")                           # iot/<site>/<device>/telemetry
    d["device_id"] = str(d.get("device_id") or (parts[2] if len(parts) > 3 else "unknown"))
    d["location"] = parts[1] if len(parts) > 3 else "unknown"
    return d


def check_sequence(d: dict) -> str:
    """Trả về 'duplicate' | 'restart' | 'ok'; cập nhật thống kê mất gói."""
    if "sequence" not in d:
        return "ok"
    dev, seq, up = d["device_id"], int(d["sequence"]), d.get("uptime_s")
    key = (dev, seq, int(d["ts"]))
    if key in seen:
        return "duplicate"
    seen[key] = True
    if len(seen) > 10000:
        seen.popitem(last=False)
    prev = state.get(dev)
    status = "ok"
    if prev is not None:
        pseq, pup = prev
        if seq <= pseq and (up is None or pup is None or up < pup):
            stats["restarts"] += 1
            status = "restart"
            log.warning("%s khởi động lại (sequence %d -> %d)", dev, pseq, seq)
        elif seq > pseq + 1:
            gap = seq - pseq - 1
            stats["lost"] += gap
            log.warning("Mất %d gói của %s (sequence %d -> %d)", gap, dev, pseq, seq)
    state[dev] = (seq, up)
    return status


def to_point(d: dict, recv_ms: int):
    from influxdb_client import Point, WritePrecision
    p = (Point("sensor_raw").tag("device_id", d["device_id"]).tag("location", d["location"])
         .field("temperature", float(d["temperature"])).field("humidity", float(d["humidity"]))
         .field("latency_ms", float(recv_ms - d["ts"]))
         .time(int(d["ts"]), WritePrecision.MS))      # dùng thời gian thiết bị -> idempotent khi gửi lại
    if "distance_cm" in d:
        p = p.field("distance_cm", float(d["distance_cm"]))
    for k in ("rssi", "sequence", "uptime_s"):
        if k in d:
            p = p.field(k, int(d[k]))
    return p


def flush():
    from influxdb_client import Point
    if not buffer:
        return
    pts = list(buffer)
    t0 = time.perf_counter()
    try:
        write_api.write(bucket=os.getenv("RAW_BUCKET", "iot_raw"), record=pts)
        w_ms = (time.perf_counter() - t0) * 1000
        buffer.clear()
        stats["ok"] += len(pts)
        log.info("Ghi %d điểm (%.1f ms)", len(pts), w_ms)
        try:
            write_api.write(bucket=os.getenv("RAW_BUCKET", "iot_raw"), record=Point("collector_stats")
                            .tag("collector", "main").field("write_ms", w_ms).field("batch", len(pts)))
        except Exception:
            pass
    except Exception as e:
        stats["db_errors"] += 1
        log.error("Ghi DB lỗi (%s) – giữ %d điểm trong buffer", e, len(buffer))


def on_connect(client, userdata, flags, rc, props=None):
    if rc == 0:
        topic = os.getenv("MQTT_TOPIC", "iot/+/+/telemetry")
        log.info("Đã kết nối MQTT, subscribe %s", topic)
        client.subscribe(topic, qos=1)
    else:
        log.error("Kết nối MQTT thất bại: %s", rc)


def on_message(client, userdata, msg):
    recv_ms = int(time.time() * 1000)
    stats["received"] += 1
    try:
        d = validate(msg.payload, msg.topic)
    except ValidationError as e:
        stats["invalid"] += 1
        log.warning("Bỏ gói lỗi [%s]: %s", msg.topic, e)
        return
    if check_sequence(d) == "duplicate":
        stats["duplicate"] += 1
        return
    buffer.append(to_point(d, recv_ms))
    flush()


def main():
    global influx, write_api
    import paho.mqtt.client as mqtt
    from influxdb_client import InfluxDBClient
    from influxdb_client.client.write_api import SYNCHRONOUS
    influx = InfluxDBClient(url=os.getenv("INFLUX_URL"), token=os.getenv("INFLUX_TOKEN"), org=os.getenv("INFLUX_ORG"))
    write_api = influx.write_api(write_options=SYNCHRONOUS)

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"collector-{os.getpid()}")
    if os.getenv("MQTT_USER"):
        c.username_pw_set(os.getenv("MQTT_USER"), os.getenv("MQTT_PASS"))
    if os.getenv("MQTT_TLS", "false").lower() == "true":
        c.tls_set(cert_reqs=ssl.CERT_REQUIRED)
    c.on_connect, c.on_message = on_connect, on_message
    c.reconnect_delay_set(1, 30)
    c.connect(os.getenv("MQTT_HOST", "broker.hivemq.com"), int(os.getenv("MQTT_PORT", 1883)), 30)

    def stop(*_):
        log.info("Dừng. Thống kê: %s", stats)
        flush(); c.disconnect(); influx.close(); sys.exit(0)
    signal.signal(signal.SIGINT, stop); signal.signal(signal.SIGTERM, stop)
    c.loop_forever()


if __name__ == "__main__":
    main()
