import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

import paho.mqtt.client as mqtt

# CSV'den beklenen metrik kolonları (case-insensitive okunur)
REQUIRED_METRICS = ["ACHP", "PHR", "AWWGV", "PDMRG"]


def iso_utc_now() -> str:
    # 2025-01-01T12:00:01Z formatına yakın, UTC
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_repo_root() -> Path:
    # simulator/simulator.py -> repo root
    return Path(__file__).resolve().parents[1]


def normalize_key(k: str) -> str:
    return (k or "").strip().lower()


def build_row_mapping(fieldnames: Iterable[str]) -> Dict[str, str]:
    """
    CSV header -> normalized key map:
    örn "ACHP" -> "achp", "Timestamp" -> "timestamp"
    """
    mapping = {}
    for fn in fieldnames:
        mapping[normalize_key(fn)] = fn
    return mapping


def get_float(row: Dict[str, str], mapping: Dict[str, str], key: str) -> Optional[float]:
    """
    key: 'ACHP' gibi. CSV'de 'achp' ya da 'ACHP' olabilir.
    """
    k_norm = normalize_key(key)
    if k_norm not in mapping:
        return None
    raw = row.get(mapping[k_norm], None)
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def get_int_any(row: Dict[str, str], mapping: Dict[str, str], candidates) -> Optional[int]:
    for cand in candidates:
        k_norm = normalize_key(cand)
        if k_norm in mapping:
            raw = row.get(mapping[k_norm], None)
            if raw is None or str(raw).strip() == "":
                continue
            try:
                return int(float(raw))
            except ValueError:
                continue
    return None


def get_ts(row: Dict[str, str], mapping: Dict[str, str]) -> str:
    """
    CSV'de Timestamp/ts/time gibi bir alan varsa onu kullanır.
    Yoksa o anın UTC zamanını yazar.
    """
    ts_val = None
    for cand in ["ts", "timestamp", "time", "datetime", "date"]:
        k_norm = normalize_key(cand)
        if k_norm in mapping:
            ts_val = row.get(mapping[k_norm], None)
            if ts_val and str(ts_val).strip():
                break

    if not ts_val or not str(ts_val).strip():
        return iso_utc_now()

    ts_val = str(ts_val).strip()

    # Eğer zaten ISO benzeri ve Z yoksa eklemeye çalış
    # Edge processor parse_ts: Z veya +00:00 kabul ediyor.
    if ts_val.endswith("Z") or "+" in ts_val:
        return ts_val

    # "YYYY-MM-DD HH:MM:SS" gibi ise ISO'ya çevir
    try:
        dt = datetime.fromisoformat(ts_val.replace(" ", "T"))
        # timezone yoksa UTC varsay
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        # En kötü ihtimal: şu an
        return iso_utc_now()


def main():
    ap = argparse.ArgumentParser(description="Publish greenhouse telemetry rows over MQTT")
    ap.add_argument("--host", default="localhost", help="MQTT broker host")
    ap.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    ap.add_argument("--topic", default="greenhouse/telemetry", help="MQTT topic")
    ap.add_argument("--interval", type=float, default=1.0, help="Seconds between publishes")
    ap.add_argument("--device-id", default="gh_01", help="Device id to publish in payload")
    ap.add_argument("--csv", default="", help="CSV path (default: data/greenhouse.csv)")
    ap.add_argument("--max-rows", type=int, default=30000, help="How many rows to publish")
    ap.add_argument("--loop", action="store_true", help="Loop CSV if max-rows > file length")
    args = ap.parse_args()

    repo_root = resolve_repo_root()
    csv_path = Path(args.csv) if args.csv else (repo_root / "data" / "greenhouse.csv")
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    client = mqtt.Client(protocol=mqtt.MQTTv311)
    client.connect(args.host, args.port, keepalive=60)
    client.loop_start()

    sent = 0
    print(f"[SIM] Publishing {args.max_rows} rows from '{csv_path}' to mqtt://{args.host}:{args.port} topic='{args.topic}' interval={args.interval}s")

    # CSV okuma
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise SystemExit("CSV has no header/fieldnames.")

        mapping = build_row_mapping(reader.fieldnames)

        # Kolon var mı kontrol (case-insensitive)
        missing = [m for m in REQUIRED_METRICS if normalize_key(m) not in mapping]
        if missing:
            print(f"[WARN] CSV is missing required metric columns: {missing}")
            print(f"[INFO] Found columns: {reader.fieldnames}")
            raise SystemExit("Fix CSV header or update simulator metric mapping.")

        rows_cache = list(reader)

    if not rows_cache:
        raise SystemExit("CSV has no data rows.")

    idx = 0
    while sent < args.max_rows:
        if idx >= len(rows_cache):
            if args.loop:
                idx = 0
            else:
                break

        row = rows_cache[idx]
        idx += 1

        ts = get_ts(row, mapping)

        metrics = {}
        for m in REQUIRED_METRICS:
            v = get_float(row, mapping, m)
            # float parse edemediyse satırı atla (edge tarafında skip olmasın)
            if v is None:
                metrics = None
                break
            metrics[m] = v

        if metrics is None:
            continue

        cls = get_int_any(row, mapping, ["class", "label", "y", "target"])
        if cls is None:
            cls = 0

        payload = {
            "ts": ts,
            "device_id": args.device_id,
            "metrics": metrics,
            "class": cls,
        }

        client.publish(args.topic, json.dumps(payload, ensure_ascii=False))
        sent += 1
        if sent <= 20 or sent % 1000 == 0:
            print(f"[SIM] sent {sent}/{args.max_rows}")

        time.sleep(max(0.0, args.interval))

    client.loop_stop()
    client.disconnect()
    print(f"[SIM] Done. Sent {sent} messages.")


if __name__ == "__main__":
    main()
