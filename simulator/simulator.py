"""Giả lập nút ESP32 của Bài 1 khi không mở Wokwi: cùng payload (temperature, humidity, distance_cm, rssi,
sequence, uptime_s). Có chèn outlier, mất gói, gói trùng, payload lỗi, timeout HC-SR04 (-1), khởi động lại."""
import argparse, json, math, os, random, time
from pathlib import Path
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / "config" / ".env")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--site", default=os.getenv("SITE", "b23dccn847"))
    ap.add_argument("--device", default=os.getenv("DEVICE_ID", "sim-01"))
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--outlier-rate", type=float, default=0.03)
    ap.add_argument("--drop-rate", type=float, default=0.05)
    ap.add_argument("--dup-rate", type=float, default=0.02)
    ap.add_argument("--bad-rate", type=float, default=0.01)
    ap.add_argument("--restart-every", type=int, default=0, help="mô phỏng reset sau N gói (0=tắt)")
    a = ap.parse_args()

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"sim-{a.site}-{a.device}-{random.randint(0, 9999)}")
    if os.getenv("MQTT_USER"):
        c.username_pw_set(os.getenv("MQTT_USER"), os.getenv("MQTT_PASS"))
    c.connect(os.getenv("MQTT_HOST", "localhost"), int(os.getenv("MQTT_PORT", 1883)), 30)
    c.loop_start()
    topic = f"iot/{a.site}/{a.device}/telemetry"
    seq, boot = 0, time.time()
    t0 = time.time()
    print("Publishing to", topic)
    while True:
        x = time.time() - t0
        seq += 1
        if a.restart_every and seq > a.restart_every:
            seq, boot = 1, time.time()
        temp = 28 + 3 * math.sin(x / 120) + random.gauss(0, 0.2)
        hum = 70 - 8 * math.sin(x / 120) + random.gauss(0, 0.8)
        dist = 100 + 30 * math.sin(x / 60) + random.gauss(0, 0.5)
        if random.random() < a.outlier_rate:
            temp += random.choice([-1, 1]) * random.uniform(10, 20)
        if random.random() < a.outlier_rate:
            dist += random.uniform(150, 250)
        msg = {"device_id": a.device, "ts": int(time.time() * 1000), "sequence": seq,
               "temperature": round(temp, 2), "humidity": round(hum, 2),
               "rssi": random.randint(-90, -60), "uptime_s": int(time.time() - boot)}
        if random.random() < 0.03:
            pass                                    # HC-SR04 timeout: ESP32 không gửi distance_cm
        else:
            msg["distance_cm"] = round(dist, 2)
        r = random.random()
        if r < a.drop_rate:
            pass
        elif r < a.drop_rate + a.bad_rate:
            c.publish(topic, '{"temperature": "abc", broken')
        else:
            payload = json.dumps(msg)
            c.publish(topic, payload, qos=1)
            if random.random() < a.dup_rate:
                c.publish(topic, payload, qos=1)
        time.sleep(a.interval)


if __name__ == "__main__":
    main()
