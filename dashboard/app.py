"""Dashboard Streamlit (thay cho dashboard ThingsBoard của Bài 1):
dữ liệu real-time, dữ liệu đã tiền xử lý, độ trễ, chất lượng đường truyền (rssi, mất gói, khởi động lại).
Chạy: streamlit run dashboard/app.py
"""
import os, time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient

load_dotenv(Path(__file__).resolve().parents[1] / "config" / ".env")
RAW, PROC = os.getenv("RAW_BUCKET", "iot_raw"), os.getenv("PROC_BUCKET", "iot_processed")
RAW_FIELDS = ["temperature", "humidity", "distance_cm", "rssi", "sequence", "uptime_s", "latency_ms"]
PROC_FIELDS = ["temperature", "temperature_roll", "humidity", "humidity_roll", "distance_cm", "distance_cm_roll",
               "temperature_outlier", "humidity_outlier", "distance_cm_outlier"]

st.set_page_config(page_title="IoT Monitoring", layout="wide")
st.title("📡 Giám sát nút cảm biến ESP32 – Nhiệt độ, Độ ẩm, Khoảng cách")


@st.cache_resource
def client():
    return InfluxDBClient(url=os.getenv("INFLUX_URL"), token=os.getenv("INFLUX_TOKEN"),
                          org=os.getenv("INFLUX_ORG"), timeout=15_000)


def q(bucket, meas, fields, minutes, device):
    flt = " or ".join(f'r._field == "{f}"' for f in fields)
    flux = f'''from(bucket: "{bucket}") |> range(start: -{minutes}m)
      |> filter(fn: (r) => r._measurement == "{meas}" and r.device_id == "{device}")
      |> filter(fn: (r) => {flt})
      |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'''
    df = client().query_api().query_data_frame(flux)
    if isinstance(df, list):
        df = pd.concat(df, ignore_index=True) if df else pd.DataFrame()
    if df.empty:
        return df
    df = df.set_index(pd.to_datetime(df["_time"], utc=True)).sort_index()
    return df.reindex(columns=fields)            # cột nào chưa có dữ liệu -> NaN, không gây lỗi


def devices():
    flux = f'import "influxdata/influxdb/schema"\nschema.tagValues(bucket: "{RAW}", tag: "device_id")'
    try:
        return [r.get_value() for t in client().query_api().query(flux) for r in t.records] or ["esp32-01"]
    except Exception:
        return ["esp32-01"]


def fmt(v, nd=1):
    return "—" if pd.isna(v) else f"{v:.{nd}f}"


with st.sidebar:
    st.header("Cấu hình")
    device = st.selectbox("Thiết bị", devices())
    minutes = st.slider("Khoảng thời gian (phút)", 5, 240, 30)
    refresh = st.slider("Tự làm mới (giây)", 2, 30, 5)
    st.caption(f"Raw bucket: {RAW} · Processed bucket: {PROC}")

try:
    raw = q(RAW, "sensor_raw", RAW_FIELDS, minutes, device)
except Exception as e:
    st.error(f"Không kết nối được InfluxDB: {e}")
    raw = pd.DataFrame()

if raw.empty:
    st.warning("Chưa có dữ liệu. Hãy chạy collector và Wokwi/simulator.")
else:
    last = raw.iloc[-1]
    age = (datetime.now(timezone.utc) - raw.index[-1]).total_seconds()
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Nhiệt độ (°C)", fmt(last.temperature))
    c2.metric("Độ ẩm (%RH)", fmt(last.humidity))
    c3.metric("Khoảng cách (cm)", fmt(raw["distance_cm"].dropna().iloc[-1] if raw["distance_cm"].notna().any() else float("nan"), 2))
    c4.metric("RSSI (dBm)", fmt(last.rssi, 0))
    c5.metric("Uptime (s)", fmt(last.uptime_s, 0))
    c6.metric("Dữ liệu cũ (s)", f"{age:.0f}", delta="OFFLINE?" if age > 15 else "OK",
              delta_color="inverse" if age > 15 else "normal")

    st.subheader("Dữ liệu thô (real-time)")
    a, b, c = st.columns(3)
    a.line_chart(raw["temperature"], height=230)
    b.line_chart(raw["humidity"], height=230)
    c.line_chart(raw["distance_cm"], height=230)

    st.subheader("Dữ liệu sau tiền xử lý (resample + loại outlier + rolling mean)")
    try:
        proc = q(PROC, "sensor_processed", PROC_FIELDS, minutes, device)
    except Exception:
        proc = pd.DataFrame()
    if proc.empty:
        st.info("Chưa có dữ liệu đã xử lý. Chạy: python preprocessing/preprocess.py --loop 60")
    else:
        a, b, c = st.columns(3)
        a.line_chart(proc[["temperature", "temperature_roll"]], height=230)
        b.line_chart(proc[["humidity", "humidity_roll"]], height=230)
        c.line_chart(proc[["distance_cm", "distance_cm_roll"]], height=230)
        n_out = int(proc[["temperature_outlier", "humidity_outlier", "distance_cm_outlier"]].fillna(0).sum().sum())
        st.caption(f"Số outlier đã loại trong khoảng đang xem: {n_out}")

    st.subheader("Chất lượng truyền và độ trễ")
    seq = raw["sequence"].dropna()
    dseq = seq.diff().dropna()
    lost = int((dseq[dseq > 1] - 1).sum())
    restarts = int((dseq < 0).sum())
    lat = raw["latency_ms"].dropna()
    d1, d2, d3, d4, d5, d6 = st.columns(6)
    d1.metric("Gói nhận", len(raw)); d2.metric("Gói mất (sequence)", lost); d3.metric("Lần khởi động lại", restarts)
    d4.metric("Latency P50 (ms)", fmt(lat.quantile(.5), 0)); d5.metric("P95 (ms)", fmt(lat.quantile(.95), 0))
    d6.metric("Max (ms)", fmt(lat.max(), 0))
    a, b = st.columns(2)
    a.line_chart(lat, height=180)
    b.line_chart(raw["rssi"], height=180)
    st.caption("Độ trễ = (thời điểm collector nhận) − (ts trên ESP32, đồng bộ NTP). Cần đồng hồ máy tính chính xác.")
    with st.expander("Bảng dữ liệu mới nhất"):
        st.dataframe(raw.tail(30).iloc[::-1])

time.sleep(refresh)
st.rerun()
