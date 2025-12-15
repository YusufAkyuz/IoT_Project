import argparse
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import matplotlib.pyplot as plt


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


def insert_gaps(xs, ys, gap_seconds: float):
    """Zaman farkı gap_seconds'tan büyükse çizgiyi kırmak için NaN ekler."""
    if gap_seconds <= 0 or len(xs) < 2:
        return xs, ys

    out_x, out_y = [xs[0]], [ys[0]]
    for i in range(1, len(xs)):
        dt = (xs[i] - xs[i - 1]).total_seconds()
        if dt > gap_seconds:
            out_x.append(xs[i])
            out_y.append(float("nan"))  # çizgiyi kır
        out_x.append(xs[i])
        out_y.append(ys[i])
    return out_x, out_y


def main():
    parser = argparse.ArgumentParser(description="Plot ACHP from SQLite telemetry")
    parser.add_argument("--db", default="storage/iot.db")
    parser.add_argument("--device-id", default="gh_01")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--mode", choices=["scatter", "line"], default="scatter",
                        help="scatter: noktalar, line: çizgi (gap kırma opsiyonel)")
    parser.add_argument("--gap-seconds", type=float, default=0,
                        help="line modunda, bu saniyeden büyük boşlukta çizgiyi kır (0=kapalı)")
    parser.add_argument("--dedup-ts", action="store_true",
                        help="Aynı ts tekrar ediyorsa tekilleştir (en son kaydı tut)")
    parser.add_argument("--out", default="", help="örn: visualize/achp.png")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    rows = fetch_last_rows(str(db_path), args.device_id, args.limit, args.dedup_ts)
    if not rows:
        raise SystemExit("No telemetry rows found.")

    xs = [parse_ts(ts) for ts, _, _ in rows]
    ys = [achp for _, achp, _ in rows]

    anom_x = [parse_ts(ts) for ts, achp, is_anom in rows if int(is_anom) == 1]
    anom_y = [achp for ts, achp, is_anom in rows if int(is_anom) == 1]

    plt.figure()

    if args.mode == "scatter":
        plt.scatter(xs, ys, label="ACHP")
    else:
        x_line, y_line = insert_gaps(xs, ys, args.gap_seconds)
        plt.plot(x_line, y_line, label="ACHP")

    if anom_x:
        plt.scatter(anom_x, anom_y, label="Anomaly (is_anomaly=1)")

    plt.title(f"ACHP over time (device_id={args.device_id}, last {len(rows)} rows)")
    plt.xlabel("Time (UTC)")
    plt.ylabel("ACHP")
    plt.legend()
    plt.gcf().autofmt_xdate()
    plt.tight_layout()

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out_path, dpi=150)
        print(f"Saved plot: {out_path}")

    plt.show()


if __name__ == "__main__":
    main()
