"""
SQLite storage for bus poll data.

Raw snapshots go into vehicle_polls. Run aggregate.py to build
stop_durations (time at each stop, how long) for easy reading.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import config


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(config.DATABASE_PATH)


def init_db() -> None:
    """Create tables if they do not exist."""
    path = Path(config.DATABASE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS vehicle_polls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                poll_utc TEXT NOT NULL,
                vehicle_name TEXT NOT NULL,
                lat REAL,
                lon REAL,
                speed REAL NOT NULL,
                time_at_stop_sec INTEGER NOT NULL,
                heading REAL,
                capacity INTEGER,
                occupation INTEGER,
                occupation_pct REAL
            )
        """)
        # Migrate older DBs
        for col, typ in [("heading", "REAL"), ("capacity", "INTEGER"),
                         ("occupation", "INTEGER"), ("occupation_pct", "REAL")]:
            try:
                conn.execute(f"ALTER TABLE vehicle_polls ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError:
                pass
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_vehicle_polls_vehicle_poll
            ON vehicle_polls(vehicle_name, poll_utc)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stop_durations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vehicle_name TEXT NOT NULL,
                stop_lat REAL,
                stop_lon REAL,
                arrived_utc TEXT NOT NULL,
                left_utc TEXT NOT NULL,
                duration_sec INTEGER NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_stop_durations_vehicle
            ON stop_durations(vehicle_name)
        """)
        conn.commit()
    finally:
        conn.close()


def insert_poll_rows(poll_utc: str, rows: list[dict[str, Any]]) -> None:
    """Insert one row per vehicle for this poll. time_at_stop is stored as seconds (int)."""
    if not rows:
        return
    conn = get_connection()
    try:
        for r in rows:
            conn.execute(
                """
                INSERT INTO vehicle_polls
                (poll_utc, vehicle_name, lat, lon, speed, time_at_stop_sec,
                 heading, capacity, occupation, occupation_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    poll_utc,
                    str(r.get("vehicle_id", "")),
                    r.get("lat"),
                    r.get("lon"),
                    float(r.get("speed", 0)),
                    int(r.get("time_at_stop_sec", 0)),
                    r.get("heading"),
                    r.get("capacity"),
                    r.get("occupation"),
                    r.get("occupation_pct"),
                ),
            )
        conn.commit()
    finally:
        conn.close()
