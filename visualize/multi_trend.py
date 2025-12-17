import argparse
import sqlite3
from datetime import datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


def parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def fetch_rows(db_path: str, device_id: str, limit: int):
    sql = """
    SELECT ts, achp, phr, awwgv, pdmrg, is_anomaly
    FROM (
        SELECT ts, achp, phr, awwgv, pdmrg, is_anomaly, rowid
        FROM telemetry
        WHERE device_id = ?
        ORDER BY ts DESC, rowid DESC
        LIMIT ?
    )
    ORDER BY ts ASC, rowid ASC
    """
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        return cur.execute(sql, (device_id, limit)).fetchall()


def main():
    p = argparse.ArgumentParser(description="Multi-metric trend plot with anomalies")
    p.add_argument("--db", default="storage/iot.db")
    p.add_argument("--device-id", default="gh_01")
    p.add_argument("--limit", type=int, default=1000)
    p.add_argument("--smooth-window", type=int, default=60)
    p.add_argument("--out", default="", help="örn: visualize/multi_trend.png")
    args = p.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    rows = fetch_rows(str(db_path), args.device_id, args.limit)
    if not rows:
        raise SystemExit("No telemetry rows found.")

    df = pd.DataFrame(
        rows,
        columns=["ts", "achp", "phr", "awwgv", "pdmrg", "is_anomaly"],
    )
    df["ts"] = df["ts"].apply(parse_ts)

    w = max(2, min(int(args.smooth_window), len(df)))
    metrics = ["achp", "phr", "awwgv", "pdmrg"]

    fig, axes = plt.subplots(nrows=4, ncols=1, figsize=(12, 9), sharex=True)

    for ax, m in zip(axes, metrics):
        trend = df[m].rolling(window=w, min_periods=1).mean()

        # edge-effect kırp
        if len(df) >= w:
            xs = df["ts"].iloc[w - 1 :]
            ys = trend.iloc[w - 1 :]
        else:
            xs = df["ts"]
            ys = trend

        ax.plot(xs, ys, linewidth=2.2, label=f"{m.upper()} trend (w={w})")

        # ---- ANOMALİLER ----
        anom = df[df["is_anomaly"] == 1]
        if not anom.empty:
            ax.scatter(
                anom["ts"],
                anom[m],
                color="red",
                marker="x",
                s=60,
                linewidths=2,
                label="Anomaly",
                zorder=5,
            )

        ax.set_ylabel(m.upper())
        ax.grid(True, linestyle="--", alpha=0.25)
        ax.legend(frameon=False, loc="upper right")

    # X ekseni (zaman)
    ax = axes[-1]
    ax.set_xlabel("Time (UTC)")
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    for tick in ax.get_xticklabels():
        tick.set_rotation(30)
        tick.set_ha("right")

    fig.suptitle(
        f"Multi-metric trends with anomalies (device_id={args.device_id}, last {len(df)} rows)",
        y=0.98,
    )
    fig.tight_layout()

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(out_path, dpi=220)
        print(f"Saved plot: {out_path}")

    plt.show()


if __name__ == "__main__":
    main()
