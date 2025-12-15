import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import paho.mqtt.client as mqtt

METRIC_COLS = ["ACHP", "PHR", "AWWGV", "PDMRG"]


def iso_utc_now() -> str:
    # Örn: 2025-01-01T12:00:01Z formatına yakın, UTC
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_class_map(series: pd.Series) -> dict:
    # SA, SB, ... gibi string class değerlerini 0..N-1'e çeviriyoruz
    labels = sorted(series.dropna().unique().tolist())
    return {label: i for i, label in enumerate(labels)}


def resolve_repo_root() -> Path:
    # simulator/simulator.py -> repo root: iot-greenhouse
    return Path(__file__).resolve().parents[1]


def main():
    repo_root = resolve_repo_root()
    default_csv = repo_root / "data" / "greenhouse.csv"

    parser = argparse.ArgumentParser(description="CSV -> MQTT telemetry simulator")
    parser.add_argument(
        "--csv",
        default=str(default_csv),
        help=f"CSV path (default: {default_csv})",
    )
    parser.add_argument("--host", default="localhost", help="MQTT host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT port")
    parser.add_argument("--topic", default="greenhouse/telemetry", help="MQTT topic")
    parser.add_argument("--device-id", default="gh_01", help="device_id field")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between rows")
    parser.add_argument("--rows", type=int, default=0, help="limit rows (0 = all)")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.is_absolute():
        # Eğer kullanıcı göreli path verdiyse repo köküne göre çöz
        csv_path = (repo_root / csv_path).resolve()

    if not csv_path.exists():
        raise SystemExit(
            f"CSV not found: {csv_path}\n"
            f"Hint: expected default at {default_csv}"
        )

    df = pd.read_csv(csv_path)

    # Kolon doğrulama
    missing = [c for c in METRIC_COLS + ["Class"] if c not in df.columns]
    if missing:
        raise SystemExit(f"CSV missing required columns: {missing}. Found: {list(df.columns)}")

    class_map = build_class_map(df["Class"])

    client = mqtt.Client()
    client.connect(args.host, args.port, keepalive=60)
    client.loop_start()

    total = len(df) if args.rows == 0 else min(args.rows, len(df))
    print(
        f"Publishing {total} rows from '{csv_path}' to mqtt://{args.host}:{args.port} "
        f"topic='{args.topic}' interval={args.interval}s"
    )

    try:
        for i in range(total):
            row = df.iloc[i]

            payload = {
                "device_id": args.device_id,
                "ts": iso_utc_now(),
                "metrics": {k: float(row[k]) for k in METRIC_COLS},
                "class": int(class_map.get(row["Class"], -1)),
            }

            client.publish(args.topic, json.dumps(payload), qos=0)
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
