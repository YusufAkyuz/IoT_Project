import argparse
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


METRICS = ["achp", "phr", "awwgv", "pdmrg"]


def parse_ts(ts: str) -> datetime:
    # 2025-12-15T19:43:20Z -> aware datetime
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def fmt_secs(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        return f"{seconds/60:.1f}m"
    return f"{seconds/3600:.1f}h"


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


@dataclass
class Snapshot:
    total_rows: int
    anomaly_rows: int
    last_ts: Optional[str]
    lag_seconds: Optional[float]
    rate_count: int
    dup_ts_count: int
    last_rows: List[Tuple]
    anomaly_rows_list: List[Tuple]
    window_rows: List[Tuple]  # metrics only


def connect_ro(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path, timeout=1)
    # Read-only davranışa yakın (SQLite query_only)
    try:
        con.execute("PRAGMA query_only=1;")
    except Exception:
        pass
    return con


def fetch_snapshot(
    con: sqlite3.Connection,
    device_id: str,
    last_n: int,
    window_n: int,
    anomalies_n: int,
    rate_seconds: int,
) -> Snapshot:
    cur = con.cursor()

    total_rows = cur.execute(
        "SELECT COUNT(*) FROM telemetry WHERE device_id=?",
        (device_id,),
    ).fetchone()[0]

    anomaly_rows = cur.execute(
        "SELECT COUNT(*) FROM telemetry WHERE device_id=? AND is_anomaly=1",
        (device_id,),
    ).fetchone()[0]

    last_ts_row = cur.execute(
        "SELECT ts FROM telemetry WHERE device_id=? ORDER BY ts DESC, rowid DESC LIMIT 1",
        (device_id,),
    ).fetchone()
    last_ts = last_ts_row[0] if last_ts_row else None

    # lag
    lag_seconds = None
    if last_ts:
        try:
            lag_seconds = (datetime.now(timezone.utc) - parse_ts(last_ts)).total_seconds()
        except Exception:
            lag_seconds = None

    # rate: son X saniyede gelen kayıt sayısı (ts text ISO olduğu için >= filtre işe yarar)
    since = (datetime.now(timezone.utc) - timedelta(seconds=rate_seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")
    rate_count = cur.execute(
        "SELECT COUNT(*) FROM telemetry WHERE device_id=? AND ts >= ?",
        (device_id, since),
    ).fetchone()[0]

    # duplicate ts: aynı ts'den kaç fazlalık var
    dup_ts_count = cur.execute(
        """
        SELECT COALESCE(SUM(c-1), 0)
        FROM (
          SELECT ts, COUNT(*) c
          FROM telemetry
          WHERE device_id=?
          GROUP BY ts
          HAVING c > 1
        )
        """,
        (device_id,),
    ).fetchone()[0]

    last_rows = cur.execute(
        """
        SELECT ts, achp, phr, awwgv, pdmrg, is_anomaly
        FROM telemetry
        WHERE device_id=?
        ORDER BY ts DESC, rowid DESC
        LIMIT ?
        """,
        (device_id, last_n),
    ).fetchall()

    anomaly_rows_list = cur.execute(
        """
        SELECT ts, achp, phr, awwgv, pdmrg
        FROM telemetry
        WHERE device_id=? AND is_anomaly=1
        ORDER BY ts DESC, rowid DESC
        LIMIT ?
        """,
        (device_id, anomalies_n),
    ).fetchall()

    window_rows = cur.execute(
        """
        SELECT achp, phr, awwgv, pdmrg
        FROM telemetry
        WHERE device_id=?
        ORDER BY ts DESC, rowid DESC
        LIMIT ?
        """,
        (device_id, window_n),
    ).fetchall()

    return Snapshot(
        total_rows=total_rows,
        anomaly_rows=anomaly_rows,
        last_ts=last_ts,
        lag_seconds=lag_seconds,
        rate_count=rate_count,
        dup_ts_count=dup_ts_count,
        last_rows=last_rows,
        anomaly_rows_list=anomaly_rows_list,
        window_rows=window_rows,
    )


def metric_stats(window_rows: List[Tuple], metric_index: int) -> Dict[str, Optional[float]]:
    vals = []
    for r in window_rows:
        v = safe_float(r[metric_index])
        if v is not None:
            vals.append(v)
    if not vals:
        return {"min": None, "mean": None, "max": None}
    return {
        "min": min(vals),
        "mean": sum(vals) / len(vals),
        "max": max(vals),
    }


def build_kpi_panel(s: Snapshot, device_id: str, rate_seconds: int) -> Panel:
    anomaly_pct = (100.0 * s.anomaly_rows / s.total_rows) if s.total_rows else 0.0
    kpi = Table.grid(padding=(0, 2))
    kpi.add_column(justify="left")
    kpi.add_column(justify="left")

    kpi.add_row("device_id", device_id)
    kpi.add_row("rows", str(s.total_rows))
    kpi.add_row("anomalies", f"{s.anomaly_rows} ({anomaly_pct:.1f}%)")
    kpi.add_row("last_ts", s.last_ts or "—")
    kpi.add_row("lag", fmt_secs(s.lag_seconds))
    kpi.add_row(f"last {rate_seconds}s", f"{s.rate_count} msgs")
    kpi.add_row("duplicate ts", str(s.dup_ts_count))

    title = Text("Live KPIs", style="bold")
    return Panel(kpi, title=title, border_style="cyan")


def build_last_rows_table(s: Snapshot) -> Panel:
    t = Table(show_lines=False)
    t.add_column("ts (UTC)", overflow="fold")
    t.add_column("ACHP", justify="right")
    t.add_column("PHR", justify="right")
    t.add_column("AWWGV", justify="right")
    t.add_column("PDMRG", justify="right")
    t.add_column("anom", justify="right")

    for ts, achp, phr, awwgv, pdmrg, is_anom in s.last_rows:
        is_anom = int(is_anom) if is_anom is not None else 0
        style = "bold red" if is_anom == 1 else ""
        t.add_row(
            str(ts),
            f"{safe_float(achp):.3f}" if safe_float(achp) is not None else "—",
            f"{safe_float(phr):.3f}" if safe_float(phr) is not None else "—",
            f"{safe_float(awwgv):.3f}" if safe_float(awwgv) is not None else "—",
            f"{safe_float(pdmrg):.3f}" if safe_float(pdmrg) is not None else "—",
            "1" if is_anom == 1 else "0",
            style=style,
        )

    return Panel(t, title=Text("Last rows (newest first)", style="bold"), border_style="green")


def build_window_stats_panel(s: Snapshot, window_n: int) -> Panel:
    # last değerleri: last_rows[0] en yeni
    last = s.last_rows[0] if s.last_rows else None

    t = Table(show_lines=False)
    t.add_column(f"Metric (last {window_n})")
    t.add_column("last", justify="right")
    t.add_column("min", justify="right")
    t.add_column("mean", justify="right")
    t.add_column("max", justify="right")

    # window_rows tuple: (achp, phr, awwgv, pdmrg)
    for idx, name in enumerate(METRICS):
        st = metric_stats(s.window_rows, idx)
        last_val = None
        if last:
            # last tuple: (ts, achp, phr, awwgv, pdmrg, is_anomaly)
            last_val = safe_float(last[idx + 1])

        t.add_row(
            name.upper(),
            f"{last_val:.3f}" if last_val is not None else "—",
            f"{st['min']:.3f}" if st["min"] is not None else "—",
            f"{st['mean']:.3f}" if st["mean"] is not None else "—",
            f"{st['max']:.3f}" if st["max"] is not None else "—",
        )

    return Panel(t, title=Text("Window stats", style="bold"), border_style="magenta")


def build_anomaly_panel(s: Snapshot) -> Panel:
    t = Table(show_lines=False)
    t.add_column("ts (UTC)", overflow="fold")
    t.add_column("ACHP", justify="right")
    t.add_column("PHR", justify="right")
    t.add_column("AWWGV", justify="right")
    t.add_column("PDMRG", justify="right")

    if not s.anomaly_rows_list:
        t.add_row("—", "—", "—", "—", "—")
        return Panel(t, title=Text("Recent anomalies", style="bold"), border_style="red")

    for ts, achp, phr, awwgv, pdmrg in s.anomaly_rows_list:
        t.add_row(
            str(ts),
            f"{safe_float(achp):.3f}" if safe_float(achp) is not None else "—",
            f"{safe_float(phr):.3f}" if safe_float(phr) is not None else "—",
            f"{safe_float(awwgv):.3f}" if safe_float(awwgv) is not None else "—",
            f"{safe_float(pdmrg):.3f}" if safe_float(pdmrg) is not None else "—",
            style="bold red",
        )

    return Panel(t, title=Text("Recent anomalies (newest first)", style="bold"), border_style="red")


def render_layout(db_path: Path, device_id: str, last_n: int, window_n: int, anomalies_n: int, rate_seconds: int) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="main", ratio=1),
        Layout(name="footer", size=10),
    )
    layout["main"].split_row(
        Layout(name="left", ratio=2),
        Layout(name="right", ratio=1),
    )
    layout["right"].split_column(
        Layout(name="kpi", size=10),
        Layout(name="stats", ratio=1),
    )

    # Header
    header_text = Text(f"IoT Greenhouse Live Dashboard  •  DB: {db_path}", style="bold")
    layout["header"].update(Panel(header_text, border_style="blue"))

    if not db_path.exists():
        layout["left"].update(Panel(f"Waiting for DB file: {db_path}\nStart edge processor.", border_style="yellow"))
        layout["right"].update(Panel("—", border_style="yellow"))
        layout["footer"].update(Panel("—", border_style="yellow"))
        return layout

    try:
        with connect_ro(str(db_path)) as con:
            s = fetch_snapshot(con, device_id, last_n, window_n, anomalies_n, rate_seconds)

        if s.total_rows == 0:
            layout["left"].update(Panel("DB exists but no rows yet.\nStart simulator.", border_style="yellow"))
            layout["right"]["kpi"].update(build_kpi_panel(s, device_id, rate_seconds))
            layout["right"]["stats"].update(build_window_stats_panel(s, window_n))
            layout["footer"].update(build_anomaly_panel(s))
            return layout

        layout["left"].update(build_last_rows_table(s))
        layout["right"]["kpi"].update(build_kpi_panel(s, device_id, rate_seconds))
        layout["right"]["stats"].update(build_window_stats_panel(s, window_n))
        layout["footer"].update(build_anomaly_panel(s))

        return layout

    except Exception as e:
        layout["left"].update(Panel(f"Error reading DB:\n{e}", border_style="red"))
        layout["right"].update(Panel("—", border_style="red"))
        layout["footer"].update(Panel("—", border_style="red"))
        return layout


def main():
    parser = argparse.ArgumentParser(description="Detailed live dashboard (SQLite -> terminal)")
    parser.add_argument("--db", default="storage/iot.db")
    parser.add_argument("--device-id", default="gh_01")
    parser.add_argument("--last", type=int, default=25, help="rows in live table")
    parser.add_argument("--window", type=int, default=200, help="rows for stats window")
    parser.add_argument("--anomalies", type=int, default=15, help="rows in anomaly panel")
    parser.add_argument("--rate-seconds", type=int, default=10, help="rate lookback seconds")
    parser.add_argument("--refresh", type=float, default=1.0, help="refresh interval seconds")
    args = parser.parse_args()

    console = Console()
    db_path = Path(args.db)

    with Live(
        render_layout(db_path, args.device_id, args.last, args.window, args.anomalies, args.rate_seconds),
        console=console,
        screen=True,
        refresh_per_second=4,
    ) as live:
        while True:
            live.update(
                render_layout(db_path, args.device_id, args.last, args.window, args.anomalies, args.rate_seconds),
                refresh=True,
            )
            time.sleep(args.refresh)


if __name__ == "__main__":
    main()
