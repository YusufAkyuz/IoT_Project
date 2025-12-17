import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# DB: .../IoT_Project/storage/iot.db
DB_PATH = Path(__file__).resolve().parents[1] / "storage" / "iot.db"

DEFAULT_REFRESH_SEC = 2
DEFAULT_LAST_N = 3000
DEFAULT_WINDOW_MIN = 10


def connect():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=2000;")
    return con


def discover_schema():
    """
    DB şemasını keşfeder (kolon-bazlı telemetry).
    Beklenen tablo: telemetry
    Beklenen kolonlar: ts, device_id ve metric kolonları (achp, phr, ...)
    """
    con = connect()
    try:
        tables = [r["name"] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()]
        if not tables:
            return None

        def score(t: str) -> int:
            tl = t.lower()
            s = 0
            if "telemetry" in tl:
                s += 50
            if "reading" in tl:
                s += 30
            if "measurement" in tl:
                s += 30
            return s

        table = sorted(tables, key=lambda t: (score(t), t), reverse=True)[0]

        cols = con.execute(f"PRAGMA table_info('{table}')").fetchall()
        colnames = [c["name"] for c in cols]
        lower = [c.lower() for c in colnames]

        # ts col
        ts_candidates = ["ts_utc", "ts", "timestamp", "time", "created_at", "event_time"]
        ts_col = None
        for cand in ts_candidates:
            if cand in lower:
                ts_col = colnames[lower.index(cand)]
                break

        # device col
        dev_candidates = ["device_id", "device", "sensor_id", "node_id"]
        device_col = None
        for cand in dev_candidates:
            if cand in lower:
                device_col = colnames[lower.index(cand)]
                break

        # id col (opsiyonel)
        id_col = "id" if "id" in lower else None

        # metric cols = ts/device/id hariç her şey
        exclude = {c for c in [ts_col, device_col, id_col] if c}
        metric_cols = [c for c in colnames if c not in exclude]

        return {
            "table": table,
            "ts_col": ts_col,
            "device_col": device_col,
            "id_col": id_col,
            "metric_cols": metric_cols,
        }
    finally:
        con.close()


@st.cache_data(ttl=2, show_spinner=False)
def load_from_db(table, ts_col, device_col, id_col, metric_cols, limit: int, device: str | None) -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()

    cols = [c for c in [id_col, ts_col, device_col] if c] + list(metric_cols)
    cols_sql = ", ".join(cols)

    where = ""
    params = []
    if device_col and device:
        where = f"WHERE {device_col} = ?"
        params.append(device)

    q = f"""
    SELECT {cols_sql}
    FROM {table}
    {where}
    ORDER BY {id_col or ts_col} DESC
    LIMIT ?
    """
    params.append(int(limit))

    con = connect()
    try:
        rows = con.execute(q, tuple(params)).fetchall()
    finally:
        con.close()

    rows = list(reversed(rows))  # eski -> yeni

    records = []
    for r in rows:
        rec = {m: r[m] for m in metric_cols if m in r.keys()}
        rec["_ts_utc"] = r[ts_col] if ts_col else None
        if id_col:
            rec["_id"] = r[id_col]
        if device_col:
            rec["_device"] = r[device_col]
        records.append(rec)

    df = pd.DataFrame(records)
    if df.empty:
        return df

    df["_ts_utc"] = pd.to_datetime(df["_ts_utc"], utc=True, errors="coerce")
    df = df.sort_values("_ts_utc")
    return df


def adaptive_thresholds(series: pd.Series, rolling_k: int = 30, std_mult: float = 2.0):
    """
    Rolling mean/std ile adaptif eşikler.
    """
    s = pd.to_numeric(series, errors="coerce")
    minp = max(3, int(rolling_k) // 5)

    rmean = s.rolling(int(rolling_k), min_periods=minp).mean()
    rstd = s.rolling(int(rolling_k), min_periods=minp).std()

    upper = rmean + float(std_mult) * rstd
    lower = rmean - float(std_mult) * rstd

    valid = upper.notna() & lower.notna()
    is_anom = pd.Series(False, index=s.index)
    is_anom.loc[valid] = (s.loc[valid] > upper.loc[valid]) | (s.loc[valid] < lower.loc[valid])
    return is_anom, rmean, rstd, lower, upper


def calc_rpm(df: pd.DataFrame, window_sec: int = 300) -> float:
    if df.empty or "_ts_utc" not in df.columns:
        return 0.0
    mx = df["_ts_utc"].max()
    if pd.isna(mx):
        return 0.0
    cutoff = mx - pd.Timedelta(seconds=window_sec)
    c = (df["_ts_utc"] >= cutoff).sum()
    return float(c) / (window_sec / 60.0)


def downsample(df: pd.DataFrame, max_points: int) -> pd.DataFrame:
    """Grafik kalabalığını azaltmak için satır seyrelte."""
    if df is None or df.empty:
        return df
    n = len(df)
    if n <= max_points:
        return df
    step = max(1, n // max_points)
    return df.iloc[::step]


# ---------------- UI ----------------
st.set_page_config(page_title="Greenhouse Live Dashboard", layout="wide")
st.title("🌿 Greenhouse Live Dashboard (Industrial Streamlit)")

schema = discover_schema()
if not schema:
    st.error("No tables found in DB.")
    st.stop()

if not schema["ts_col"]:
    st.error(f"Telemetry table found ({schema['table']}) but ts column not detected.")
    st.write(schema)
    st.stop()

with st.sidebar:
    st.header("Settings")
    refresh_sec = st.number_input("Refresh (sec)", 1, 30, DEFAULT_REFRESH_SEC, 1)
    last_n = st.number_input("DB read limit (last N rows)", 200, 50000, DEFAULT_LAST_N, 100)
    window_min = st.number_input("Show last (minutes)", 1, 240, DEFAULT_WINDOW_MIN, 1)

    st.subheader("Chart readability")
    max_points = st.number_input("Max points on chart", min_value=100, max_value=5000, value=700, step=100)
    agg = st.selectbox("Resample", ["raw", "5s", "10s", "30s", "1min"], index=2)

    st.subheader("Adaptive Anomaly")
    std_mult = st.slider("STD multiplier", 0.5, 4.0, 2.0, 0.1)
    rolling_k = st.number_input("Rolling window (k)", 5, 500, 30, 5)

st_autorefresh(interval=int(refresh_sec * 1000), key="refresh")
st.caption(f"Auto refresh every {refresh_sec}s • table={schema['table']} • db={DB_PATH.name}")

# device select (optional)
device = None
if schema["device_col"]:
    con = connect()
    try:
        devs = [r["d"] for r in con.execute(
            f"SELECT DISTINCT {schema['device_col']} AS d FROM {schema['table']} ORDER BY d"
        ).fetchall() if r["d"] is not None]
    finally:
        con.close()

    device = st.sidebar.selectbox("Device", ["(all)"] + devs)
    if device == "(all)":
        device = None

df = load_from_db(
    schema["table"],
    schema["ts_col"],
    schema["device_col"],
    schema["id_col"],
    schema["metric_cols"],
    int(last_n),
    device
)

if df.empty:
    st.warning("No data in database yet. Start Edge + Simulator first.")
    st.stop()

# metric list (isteğe bağlı: is_anomaly'yi grafikten çıkar)
metric_keys = [m for m in schema["metric_cols"] if m in df.columns and not m.startswith("_")]
if "is_anomaly" in metric_keys:
    metric_keys.remove("is_anomaly")

if not metric_keys:
    st.error("No metric columns found to plot.")
    st.stop()

left_metric = st.sidebar.selectbox("Left metric", metric_keys, index=0)
right_metric = st.sidebar.selectbox("Right metric", metric_keys, index=min(1, len(metric_keys) - 1))

df[left_metric] = pd.to_numeric(df[left_metric], errors="coerce")
df[right_metric] = pd.to_numeric(df[right_metric], errors="coerce")

# time window
now_utc = pd.Timestamp.now(tz="UTC")
cutoff = now_utc - pd.Timedelta(minutes=float(window_min))
dfw = df[df["_ts_utc"] >= cutoff].copy()
if dfw.empty:
    st.warning("No data inside selected time window.")
    st.stop()

last_valid = dfw.dropna(subset=[left_metric, right_metric])
if last_valid.empty:
    st.warning("Latest rows contain NaNs for selected metrics.")
    st.stop()

last_row = last_valid.iloc[-1]
last_ts = pd.to_datetime(last_row["_ts_utc"], utc=True)
age_sec = (now_utc - last_ts).total_seconds()
is_live = age_sec <= max(10, refresh_sec * 3)

# RPM
rpm = calc_rpm(dfw, 300)

# Adaptive anomaly uses dfw[right_metric]
anom_mask, rmean, rstd, lower, upper = adaptive_thresholds(
    dfw[right_metric], rolling_k=int(rolling_k), std_mult=float(std_mult)
)
last_is_anom = bool(anom_mask.loc[last_row.name])

# banner
if not is_live:
    st.error(f"STALE DATA 🔴  Last update {int(age_sec)} sec ago.")
elif last_is_anom:
    st.error(f"ALERT 🔴  {right_metric} anomaly in latest reading!")
else:
    st.success("LIVE 🟢  Data stream is healthy.")

# KPIs
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric(f"Last {left_metric}", f"{last_row[left_metric]:.2f}")
c2.metric(f"Last {right_metric}", f"{last_row[right_metric]:.2f}")
c3.metric("Data age", f"{int(age_sec)} sec")
c4.metric("Throughput", f"{rpm:.1f} rows/min")
c5.metric(f"{right_metric} anomaly (adaptive)", "YES 🔴" if last_is_anom else "NO 🟢")

st.divider()

# --- Plot prep
plot_df = dfw.set_index("_ts_utc")[[left_metric, right_metric]].dropna()
if plot_df.empty:
    st.warning("No valid rows to plot after NaN filtering.")
    st.stop()

# Resample (readability)
if agg != "raw":
    rule = {"5s": "5S", "10s": "10S", "30s": "30S", "1min": "1T"}[agg]
    plot_df = plot_df.resample(rule).mean()

# Downsample (readability)
plot_df = downsample(plot_df, int(max_points))

# thresholds aligned to plot index (use original dfw alignment then reindex)
thr_df = pd.DataFrame(
    {"upper": upper.values, "lower": lower.values, "anom": anom_mask.values},
    index=dfw["_ts_utc"].values
).sort_index()
thr_df = thr_df.reindex(plot_df.index)

# ---- Charts (2 columns)
left_col, right_col = st.columns(2)

with left_col:
    st.subheader(f"{left_metric} (last {window_min} min) • view={agg}, pts≤{int(max_points)}")
    st.line_chart(plot_df[[left_metric]])

with right_col:
    st.subheader(f"{right_metric} (adaptive thresholds) • view={agg}, pts≤{int(max_points)}")
    chart_df = pd.DataFrame(index=plot_df.index)
    chart_df[right_metric] = plot_df[right_metric]
    chart_df["upper"] = thr_df["upper"]
    chart_df["lower"] = thr_df["lower"]
    st.line_chart(chart_df)

st.divider()

# DB flag anomalies
if "is_anomaly" in dfw.columns:
    st.subheader("Recent anomalies (DB flag: is_anomaly=1)")
    anom_db = dfw[dfw["is_anomaly"] == 1][["_ts_utc", right_metric, "is_anomaly"]].tail(50)
    if anom_db.empty:
        st.info("No DB-flag anomalies in the current window.")
    else:
        st.dataframe(anom_db, use_container_width=True)

# Adaptive anomalies list
st.subheader(f"Recent anomalies (adaptive) — {right_metric}")
anom_df = dfw[anom_mask][["_ts_utc", right_metric]].tail(50)
if anom_df.empty:
    st.info("No adaptive anomalies detected in the current window.")
else:
    st.dataframe(anom_df, use_container_width=True)

with st.expander("Debug / System Info"):
    st.write(f"DB Path: {DB_PATH}")
    st.write(schema)
    st.write(f"Rows in window: {len(dfw)}")
    st.write(f"Last update age (sec): {age_sec:.2f}")
    st.write(f"rolling_k={rolling_k}, std_mult={std_mult}, resample={agg}, max_points={max_points}")
