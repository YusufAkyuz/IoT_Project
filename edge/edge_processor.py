import argparse
import json
import math
from typing import Any, Dict

import paho.mqtt.client as mqtt

from storage.db import init_db, insert_telemetry


METRIC_KEYS = ["ACHP", "PHR", "AWWGV", "PDMRG"]


def to_float(x: Any) -> float:
    """Tip dönüşümü + None/NaN kontrolü.
    Geçersizse ValueError fırlatır (kayıt atlanır).
    """
    if x is None:
        raise ValueError("value is None")
    v = float(x)
    if math.isnan(v) or math.isinf(v):
        raise ValueError("value is NaN/Inf")
    return v


def on_connect(client, userdata, connect_flags, reason_code, properties):
    rc = int(getattr(reason_code, "value", reason_code))
    if rc == 0:
        print(f"[MQTT] Connected. Subscribing to: {userdata['topic']}")
        client.subscribe(userdata["topic"])
    else:
        print(f"[MQTT] Connect failed reason_code={reason_code}")


def on_message(client, userdata, msg):
    db_path = userdata["db"]
    threshold = userdata["achp_threshold"]

    try:
        raw = msg.payload.decode("utf-8")
        data: Dict[str, Any] = json.loads(raw)

        # Zorunlu alanlar
        ts = data.get("ts")
        device_id = data.get("device_id")
        metrics = data.get("metrics", {})

        if not ts or not device_id or not isinstance(metrics, dict):
            raise ValueError("missing ts/device_id/metrics")

        # Pre-processing: tip dönüşümü + None/NaN kontrolü
        achp = to_float(metrics.get("ACHP"))
        phr = to_float(metrics.get("PHR"))
        awwgv = to_float(metrics.get("AWWGV"))
        pdmrg = to_float(metrics.get("PDMRG"))

        # Basit anomali kuralı (tek satır)
        is_anomaly = 1 if achp > threshold else 0

        insert_telemetry(
            db_path=db_path,
            ts=ts,
            device_id=device_id,
            achp=achp,
            phr=phr,
            awwgv=awwgv,
            pdmrg=pdmrg,
            is_anomaly=is_anomaly,
        )

        print(f"[DB] Inserted ts={ts} device={device_id} achp={achp:.3f} anomaly={is_anomaly}")

    except Exception as e:
        # Bozuk mesaj gelirse sistemi düşürmeyelim; loglayıp devam
        print(f"[WARN] Skipped message: {e}")


def main():
    parser = argparse.ArgumentParser(description="Edge processor: MQTT -> preprocess -> SQLite")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--topic", default="greenhouse/telemetry")
    parser.add_argument("--db", default="storage/iot.db")
    parser.add_argument("--achp-threshold", type=float, default=50.0)
    args = parser.parse_args()

    # DB hazırla
    init_db(args.db)
    print(f"[DB] Ready: {args.db}")

    userdata = {
        "topic": args.topic,
        "db": args.db,
        "achp_threshold": args.achp_threshold,
    }

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, userdata=userdata)
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"[MQTT] Connecting to {args.host}:{args.port} ...")
    client.connect(args.host, args.port, keepalive=60)
    client.loop_forever()


if __name__ == "__main__":
    main()
