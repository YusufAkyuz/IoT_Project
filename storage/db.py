from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS telemetry (
  ts TEXT,
  device_id TEXT,
  achp REAL,
  phr REAL,
  awwgv REAL,
  pdmrg REAL,
  is_anomaly INTEGER
);
"""


def init_db(db_path: str) -> None:
    """DB dosyasını oluşturur ve tabloyu garanti eder."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(path) as conn:
        conn.execute(SCHEMA_SQL)
        conn.commit()


def insert_telemetry(
    db_path: str,
    ts: str,
    device_id: str,
    achp: float,
    phr: float,
    awwgv: float,
    pdmrg: float,
    is_anomaly: int,
) -> None:
    """Tek bir telemetry kaydını DB'ye yazar."""
    path = Path(db_path)
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            INSERT INTO telemetry (ts, device_id, achp, phr, awwgv, pdmrg, is_anomaly)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, device_id, achp, phr, awwgv, pdmrg, int(is_anomaly)),
        )
        conn.commit()
