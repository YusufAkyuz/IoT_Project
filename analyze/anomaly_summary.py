import argparse
import sqlite3
from pathlib import Path
from datetime import datetime


def parse_ts(ts: str) -> str:
    # DB'de ISO gibi duruyor; sunumda okunur format için
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ts


def q_one(cur, sql, params=()):
    row = cur.execute(sql, params).fetchone()
    return row[0] if row else None


def main():
    p = argparse.ArgumentParser(description="Anomaly summary table from SQLite telemetry")
    p.add_argument("--db", default="storage/iot.db")
    p.add_argument("--device-id", default="", help="Boş bırakılırsa tüm cihazlar")
    p.add_argument("--top", type=int, default=10, help="En çok anomali olan top-N cihaz")
    args = p.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    where = ""
    params = ()
    if args.device_id:
        where = "WHERE device_id = ?"
        params = (args.device_id,)

    with sqlite3.connect(str(db_path)) as con:
        cur = con.cursor()

        total = q_one(cur, f"SELECT COUNT(*) FROM telemetry {where}", params) or 0
        anomalies = q_one(cur, f"SELECT COUNT(*) FROM telemetry {where} " + ("AND" if where else "WHERE") + " is_anomaly = 1", params) or 0
        ratio = (anomalies / total * 100.0) if total else 0.0

        first_anom = q_one(
            cur,
            f"SELECT ts FROM telemetry {where} " + ("AND" if where else "WHERE") + " is_anomaly = 1 ORDER BY ts ASC LIMIT 1",
            params,
        )
        last_anom = q_one(
            cur,
            f"SELECT ts FROM telemetry {where} " + ("AND" if where else "WHERE") + " is_anomaly = 1 ORDER BY ts DESC LIMIT 1",
            params,
        )

        # Cihaz bazında
        if args.device_id:
            by_device = [(args.device_id, anomalies)]
        else:
            by_device = cur.execute(
                """
                SELECT device_id,
                       SUM(CASE WHEN is_anomaly=1 THEN 1 ELSE 0 END) AS anomaly_cnt,
                       COUNT(*) AS total_cnt
                FROM telemetry
                GROUP BY device_id
                ORDER BY anomaly_cnt DESC, total_cnt DESC
                LIMIT ?
                """,
                (args.top,),
            ).fetchall()

        # Metrik bazında özet: normal vs anomaly (yalın ama çok faydalı)
        metric_summary = cur.execute(
            f"""
            SELECT
              AVG(achp), MIN(achp), MAX(achp),
              AVG(phr),  MIN(phr),  MAX(phr),
              AVG(awwgv),MIN(awwgv),MAX(awwgv),
              AVG(pdmrg),MIN(pdmrg),MAX(pdmrg)
            FROM telemetry
            {where}
            """,
            params,
        ).fetchone()

        metric_summary_anom = cur.execute(
            f"""
            SELECT
              AVG(achp), MIN(achp), MAX(achp),
              AVG(phr),  MIN(phr),  MAX(phr),
              AVG(awwgv),MIN(awwgv),MAX(awwgv),
              AVG(pdmrg),MIN(pdmrg),MAX(pdmrg)
            FROM telemetry
            {where} {"AND" if where else "WHERE"} is_anomaly = 1
            """,
            params,
        ).fetchone()

    scope = f"device_id={args.device_id}" if args.device_id else "ALL devices"

    print("\n==================== Anomaly Summary ====================")
    print(f"Scope            : {scope}")
    print(f"DB               : {db_path}")
    print("---------------------------------------------------------")
    print(f"Total rows       : {total}")
    print(f"Anomalies        : {anomalies}")
    print(f"Anomaly ratio    : {ratio:.2f}%")
    print(f"First anomaly ts : {parse_ts(first_anom) if first_anom else '-'}")
    print(f"Last anomaly ts  : {parse_ts(last_anom) if last_anom else '-'}")
    print("---------------------------------------------------------")

    print("\nTop devices by anomaly count:")
    if args.device_id:
        print(f"- {args.device_id}: {anomalies} anomalies")
    else:
        for dev, an_cnt, tot_cnt in by_device:
            pct = (an_cnt / tot_cnt * 100.0) if tot_cnt else 0.0
            print(f"- {dev}: anomalies={an_cnt}, total={tot_cnt}, ratio={pct:.2f}%")

    def fmt_triplet(row, i):
        # row contains groups of (avg,min,max) in order
        avg_, mn_, mx_ = row[i], row[i+1], row[i+2]
        if avg_ is None:
            return "-"
        return f"avg={avg_:.3f} | min={mn_:.3f} | max={mx_:.3f}"

    print("\nMetric summary (ALL rows):")
    if metric_summary and metric_summary[0] is not None:
        print(f"ACHP  : {fmt_triplet(metric_summary, 0)}")
        print(f"PHR   : {fmt_triplet(metric_summary, 3)}")
        print(f"AWWGV : {fmt_triplet(metric_summary, 6)}")
        print(f"PDMRG : {fmt_triplet(metric_summary, 9)}")
    else:
        print("- No data")

    print("\nMetric summary (ANOMALY rows only):")
    if metric_summary_anom and metric_summary_anom[0] is not None:
        print(f"ACHP  : {fmt_triplet(metric_summary_anom, 0)}")
        print(f"PHR   : {fmt_triplet(metric_summary_anom, 3)}")
        print(f"AWWGV : {fmt_triplet(metric_summary_anom, 6)}")
        print(f"PDMRG : {fmt_triplet(metric_summary_anom, 9)}")
    else:
        print("- No anomaly rows")

    print("=========================================================\n")


if __name__ == "__main__":
    main()
