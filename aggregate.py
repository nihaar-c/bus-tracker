#!/usr/bin/env python3
"""
Aggregate raw vehicle_polls into stop_durations for easy reading.

A "stop" is a run of consecutive polls where speed = 0 for the same vehicle.
Output: time at each stop, how long (duration), and location.
Run after collecting data with poller.py:  python aggregate.py
"""

from __future__ import annotations

import sqlite3
from itertools import groupby

import config
from db import get_connection, init_db
from tabulate import tabulate


POLL_INTERVAL_SEC = getattr(config, "POLL_INTERVAL_SEC", 35)


def get_polls_by_vehicle(conn: sqlite3.Connection) -> list[tuple]:
    """Return (vehicle_name, poll_utc, lat, lon, speed) ordered by vehicle, time."""
    cur = conn.execute(
        """
        SELECT vehicle_name, poll_utc, lat, lon, speed
        FROM vehicle_polls
        ORDER BY vehicle_name, poll_utc
        """
    )
    return cur.fetchall()


def runs_of_stopped(rows: list[tuple]) -> list[dict]:
    """
    From ordered (vehicle_name, poll_utc, lat, lon, speed) rows for one vehicle,
    return list of stops: {vehicle_name, stop_lat, stop_lon, arrived_utc, left_utc, duration_sec}.
    """
    stops = []
    i = 0
    while i < len(rows):
        if rows[i][4] != 0:  # speed
            i += 1
            continue
        start = i
        while i < len(rows) and rows[i][4] == 0:
            i += 1
        count = i - start
        if count == 0:
            continue
        first = rows[start]
        last = rows[i - 1]
        stops.append({
            "vehicle_name": first[0],
            "stop_lat": first[2],
            "stop_lon": first[3],
            "arrived_utc": first[1],
            "left_utc": last[1],
            "duration_sec": count * POLL_INTERVAL_SEC,
        })
    return stops


def recompute_stop_durations(conn: sqlite3.Connection) -> int:
    """Rebuild stop_durations from vehicle_polls. Returns number of stops inserted."""
    conn.execute("DELETE FROM stop_durations")
    all_rows = get_polls_by_vehicle(conn)
    by_vehicle = []
    for key, group in groupby(all_rows, key=lambda r: r[0]):
        by_vehicle.append((key, list(group)))
    inserted = 0
    for vehicle_name, rows in by_vehicle:
        for stop in runs_of_stopped(rows):
            conn.execute(
                """
                INSERT INTO stop_durations
                (vehicle_name, stop_lat, stop_lon, arrived_utc, left_utc, duration_sec)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    stop["vehicle_name"],
                    stop["stop_lat"],
                    stop["stop_lon"],
                    stop["arrived_utc"],
                    stop["left_utc"],
                    stop["duration_sec"],
                ),
            )
            inserted += 1
    conn.commit()
    return inserted


def main() -> None:
    init_db()
    conn = get_connection()
    try:
        n = recompute_stop_durations(conn)
        print(f"Aggregated {n} stop(s) from vehicle_polls.\n")

        cur = conn.execute(
            """
            SELECT vehicle_name, stop_lat, stop_lon, arrived_utc, left_utc, duration_sec
            FROM stop_durations
            ORDER BY vehicle_name, arrived_utc
            """
        )
        rows = cur.fetchall()
        if not rows:
            print("No stop data yet. Run the poller to collect data, then run this again.")
            return

        # Human-friendly duration and times
        table = []
        for r in rows:
            vehicle_name, stop_lat, stop_lon, arrived, left, duration_sec = r
            duration_str = f"{duration_sec}s" if duration_sec < 60 else f"{duration_sec // 60}m {duration_sec % 60}s"
            # Shorten timestamps for display
            arrived_short = arrived[:19].replace("T", " ") if arrived else ""
            left_short = left[:19].replace("T", " ") if left else ""
            table.append([vehicle_name, stop_lat, stop_lon, arrived_short, left_short, duration_str])

        print("Time at each stop (vehicle, location, arrived, left, duration):")
        print(tabulate(table, headers=["vehicle", "lat", "lon", "arrived_utc", "left_utc", "duration"], tablefmt="simple"))
        print("\nQuery directly:  sqlite3 bus_data.db 'SELECT * FROM stop_durations ORDER BY vehicle_name, arrived_utc;'")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
