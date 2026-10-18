"""Tiền xử lý (temperature, humidity, distance_cm): đọc sensor_raw -> missing -> outlier (IQR/Z-score) -> resample -> rolling/delta -> chuẩn hóa
-> ghi measurement `sensor_processed` (bucket iot_processed).

Chạy:  python preprocessing/preprocess.py --hours 1 --freq 10s --method iqr
       python preprocessing/preprocess.py --loop 60      # chạy lặp mỗi 60 giây
"""
import argparse, logging, os, time
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / "config" / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("preprocess")
COLS = ["temperature", "humidity", "distance_cm"]


# ---------- Các hàm thuần (dễ kiểm thử, không cần DB) ----------
def detect_outliers(s: pd.Series, method: str = "iqr", k: float = 1.5, z: float = 3.0) -> pd.Series:
    """Trả về mask True tại điểm bất thường."""
    s = s.astype(float)
    if method == "zscore":
        std = s.std(ddof=0)
        return pd.Series(False, index=s.index) if not std else ((s - s.mean()).abs() / std > z)
    q1, q3 = s.quantile(0.25), s.quantile(0.75)
    iqr = q3 - q1
    return (s < q1 - k * iqr) | (s > q3 + k * iqr)


def process(df: pd.DataFrame, freq: str = "10s", method: str = "iqr",
            window: int = 6, max_gap: int = 3) -> pd.DataFrame:
    """df: index = DatetimeIndex(UTC), cột temperature/humidity. Trả về DataFrame đã xử lý."""
    df = df.copy()
    for c in COLS:                                   # trường vắng mặt (vd. HC-SR04 luôn timeout) -> NaN
        if c not in df.columns:
            df[c] = np.nan
    df = df[~df.index.duplicated(keep="last")].sort_index()[COLS].astype(float)
    out = pd.DataFrame(index=df.index)
    for c in COLS:
        bad = detect_outliers(df[c], method)
        out[f"{c}_outlier"] = bad.astype(int)
        out[c] = df[c].mask(bad)                     # outlier -> NaN, sẽ nội suy bên dưới
    # Resample theo cửa sổ thời gian: trung bình giá trị sạch, đếm outlier trong cửa sổ
    agg = {c: "mean" for c in COLS}
    agg.update({f"{c}_outlier": "sum" for c in COLS})
    res = out.resample(freq).agg(agg)
    res["n_raw"] = df[COLS[0]].resample(freq).count()
    # Missing: nội suy theo thời gian, tối đa max_gap cửa sổ liên tiếp; phần còn lại giữ NaN
    for c in COLS:
        res[c] = res[c].interpolate(method="time", limit=max_gap, limit_area="inside")
    res = res.dropna(subset=COLS, how="all")
    for c in COLS:
        res[f"{c}_roll"] = res[c].rolling(window, min_periods=1).mean()   # trung bình trượt
        res[f"{c}_delta"] = res[c].diff()                                   # độ thay đổi
        mu, sd = res[c].mean(), res[c].std(ddof=0)
        res[f"{c}_z"] = (res[c] - mu) / sd if sd else 0.0                   # Z-score
        lo, hi = res[c].min(), res[c].max()
        res[f"{c}_minmax"] = (res[c] - lo) / (hi - lo) if hi > lo else 0.0  # Min-Max [0,1]
    return res


# ---------- Tương tác InfluxDB ----------
def fetch_raw(query_api, bucket: str, start: str, device: str | None = None) -> pd.DataFrame:
    dev = f'|> filter(fn: (r) => r.device_id == "{device}")' if device else ""
    q = f'''from(bucket: "{bucket}") |> range(start: {start})
      |> filter(fn: (r) => r._measurement == "sensor_raw")
      |> filter(fn: (r) => r._field == "temperature" or r._field == "humidity" or r._field == "distance_cm") {dev}
      |> pivot(rowKey: ["_time", "device_id"], columnKey: ["_field"], valueColumn: "_value")'''
    res = query_api.query_data_frame(q)
    if isinstance(res, list):
        res = pd.concat(res, ignore_index=True) if res else pd.DataFrame()
    return res


def write_processed(write_api, bucket: str, device: str, res: pd.DataFrame) -> int:
    from influxdb_client import Point, WritePrecision
    pts = []
    for ts, row in res.iterrows():
        p = Point("sensor_processed").tag("device_id", device).time(ts.to_pydatetime(), WritePrecision.S)
        for col, val in row.items():
            if pd.notna(val):
                p = p.field(col, float(val))
        pts.append(p)
    write_api.write(bucket=bucket, record=pts)
    return len(pts)


def run_once(args):
    from influxdb_client import InfluxDBClient
    from influxdb_client.client.write_api import SYNCHRONOUS
    with InfluxDBClient(url=os.getenv("INFLUX_URL"), token=os.getenv("INFLUX_TOKEN"),
                        org=os.getenv("INFLUX_ORG"), timeout=30_000) as cl:
        raw = fetch_raw(cl.query_api(), os.getenv("RAW_BUCKET", "iot_raw"), f"-{args.hours}h", args.device)
        if raw.empty:
            log.warning("Không có dữ liệu thô trong %s giờ qua", args.hours); return
        w = cl.write_api(write_options=SYNCHRONOUS)
        for dev, g in raw.groupby("device_id"):
            g = g.set_index(pd.to_datetime(g["_time"], utc=True))
            t0 = time.perf_counter()
            res = process(g, args.freq, args.method, args.window)
            n_out = int(res[[f"{c}_outlier" for c in COLS]].sum().sum())
            n = write_processed(w, os.getenv("PROC_BUCKET", "iot_processed"), dev, res)
            log.info("[%s] raw=%d -> processed=%d điểm, outlier=%d, thời gian=%.2fs",
                     dev, len(g), n, n_out, time.perf_counter() - t0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=1)
    ap.add_argument("--freq", default="10s", help="cửa sổ resample, vd 10s, 1min")
    ap.add_argument("--method", choices=["iqr", "zscore"], default="iqr")
    ap.add_argument("--window", type=int, default=6, help="kích thước cửa sổ rolling mean")
    ap.add_argument("--device", default=None)
    ap.add_argument("--loop", type=int, default=0, help="chạy lặp mỗi N giây (0 = chạy 1 lần)")
    a = ap.parse_args()
    while True:
        try:
            run_once(a)
        except Exception as e:
            log.error("Lỗi tiền xử lý: %s", e)
        if not a.loop:
            break
        time.sleep(a.loop)
