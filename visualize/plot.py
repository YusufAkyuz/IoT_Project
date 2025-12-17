import argparse
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


def parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def fetch_last_rows(db_path: str, device_id: str, limit: int, dedup_ts: bool) -> List[Tuple[str, float, int]]:
    # Son N satırı al, sonra doğru çizim için ASC sıraya çevir.
    # rowid ile aynı ts tekrarlarında deterministik sıra sağlıyoruz.
    if dedup_ts:
        # Aynı ts’den birden fazla varsa en son rowid’yi al (tekilleştir)
        sql = """
        SELECT t.ts, t.achp, t.is_anomaly
        FROM telemetry t
        JOIN (
            SELECT ts, MAX(rowid) AS rid
            FROM telemetry
            WHERE device_id = ?
            GROUP BY ts
            ORDER BY ts DESC
            LIMIT ?
        ) x
        ON t.ts = x.ts AND t.rowid = x.rid
        WHERE t.device_id = ?
        ORDER BY t.ts ASC, t.rowid ASC
        """
        params = (device_id, limit, device_id)
    else:
        sql = """
        SELECT ts, achp, is_anomaly
        FROM (
            SELECT ts, achp, is_anomaly, rowid
            FROM telemetry
            WHERE device_id = ?
            ORDER BY ts DESC, rowid DESC
            LIMIT ?
        )
        ORDER BY ts ASC, rowid ASC
        """
        params = (device_id, limit)

    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        return cur.execute(sql, params).fetchall()


def main():
    parser = argparse.ArgumentParser(description="Plot ACHP trend (rolling mean) from SQLite telemetry")
    parser.add_argument("--db", default="storage/iot.db")
    parser.add_argument("--device-id", default="gh_01")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument(
        "--smooth-window",
        type=int,
        default=60,
        help="Rolling mean pencere boyu (ör: 10/30/60). Ne kadar büyükse o kadar yumuşak.",
    )
    parser.add_argument(
        "--dedup-ts",
        action="store_true",
        help="Aynı ts tekrar ediyorsa tekilleştir (en son kaydı tut)",
    )
    parser.add_argument("--out", default="", help="örn: visualize/achp_trend.png")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    rows = fetch_last_rows(str(db_path), args.device_id, args.limit, args.dedup_ts)
    if not rows:
        raise SystemExit("No telemetry rows found.")

    xs = [parse_ts(ts) for ts, _, _ in rows]
    ys = [achp for _, achp, _ in rows]

    # Anomali noktaları (varsa)
    anom_x = [parse_ts(ts) for ts, achp, is_anom in rows if int(is_anom) == 1]
    anom_y = [achp for ts, achp, is_anom in rows if int(is_anom) == 1]

    # Trend hesapla
    w = max(2, min(int(args.smooth_window), len(ys)))
    y_trend = pd.Series(ys).rolling(window=w, min_periods=1).mean().tolist()

    # --- Sunum için: edge-effect'i gizle (ilk w-1 nokta az veri ile hesaplanıyor) ---
    if len(xs) >= w:
        xs_plot = xs[w - 1 :]
        y_trend_plot = y_trend[w - 1 :]
    else:
        # çok az veri varsa yine de çiz
        xs_plot = xs
        y_trend_plot = y_trend

    # Plot
    plt.figure(figsize=(12, 5))
    plt.plot(xs_plot, y_trend_plot, linewidth=2.6, label=f"ACHP trend (rolling mean, w={w})")

    # Anomali varsa trend üstünde işaretle (ham veri çizmeden)
    if anom_x:
        plt.scatter(anom_x, anom_y, s=55, marker="x", linewidths=2.0, label="Anomaly")

    plt.title(f"ACHP Trend over time (device_id={args.device_id}, last {len(rows)} rows)")
    plt.xlabel("Time (UTC)")
    plt.ylabel("ACHP (trend)")

    # Y eksenini daralt: trend vurgusu artsın
    if y_trend_plot:
        y_min = min(y_trend_plot) - 0.2
        y_max = max(y_trend_plot) + 0.2
        if y_min < y_max:
            plt.ylim(y_min, y_max)

    # Zaman ekseni formatı
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    plt.xticks(rotation=30, ha="right")

    # Grid
    plt.grid(True, linestyle="--", alpha=0.25)

    plt.legend(frameon=False, loc="upper right")
    plt.tight_layout()

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out_path, dpi=220)
        print(f"Saved plot: {out_path}")

    plt.show()


if __name__ == "__main__":
    main()
