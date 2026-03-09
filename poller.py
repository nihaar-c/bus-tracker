#!/usr/bin/env python3
"""
Georgia Tech bus real-time poller.

Polls the TransLoc vehicle_statuses API, extracts key fields,
tracks time-at-stop via a simple state machine, prints a console
table, and appends raw JSON to a local JSONL log file.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
from tabulate import tabulate

import config

# ── URL helpers ───────────────────────────────────────────────────────

def build_url() -> str:
    if config.VEHICLE_STATUS_URL_OVERRIDE:
        return config.VEHICLE_STATUS_URL_OVERRIDE
    return f"{config.VEHICLE_STATUS_URL}?agencies={config.AGENCY_ID}"


# ── Network ───────────────────────────────────────────────────────────

def fetch_vehicle_statuses(url: str) -> dict[str, Any] | list[Any] | None:
    """GET the vehicle endpoint; return parsed JSON (dict or list) or None."""
    try:
        resp = requests.get(url, timeout=config.REQUEST_TIMEOUT_SEC)
        if resp.status_code == 429:
            print("[error] 429 Too Many Requests — skipping this poll, will retry after cooldown.")
            return None
        if resp.status_code == 503:
            print("[error] 503 Service Unavailable — skipping this poll, will retry after cooldown.")
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        print(f"[error] Request failed: {exc}")
        return None


def discover_endpoint(url: str) -> bool:
    """Validate that the endpoint is reachable and returns JSON."""
    print(f"[discovery] Testing endpoint: {url}")
    data = fetch_vehicle_statuses(url)
    if data is None:
        print("[discovery] Endpoint unreachable or returned an error.")
        return False
    if isinstance(data, list):
        print(f"[discovery] Endpoint is active. Response is a list of {len(data)} vehicle(s).")
    else:
        print("[discovery] Endpoint is active. Sample keys:", list(data.keys()))
    return True


# ── Extraction ────────────────────────────────────────────────────────

def extract_vehicles(raw: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    """
    Pull the vehicle list out of the API response.

    TransLoc v3 nests vehicles under a top-level key (commonly
    "vehicles" or "vehicle_statuses").  Adjust the key here if
    the first run's bus_logs.jsonl shows a different structure.
    """
    # GT GetMapVehiclePoints returns a top-level array
    if isinstance(raw, list):
        return raw

    for key in ("vehicles", "vehicle_statuses"):
        if key in raw and isinstance(raw[key], list):
            return raw[key]

    return []


def extract_vehicle_fields(vehicle: dict[str, Any]) -> dict[str, Any]:
    """
    Map a single raw vehicle dict to the canonical fields we care about.

    GT GetMapVehiclePoints uses PascalCase: Latitude, Longitude, VehicleID, GroundSpeed.
    No load/capacity field in the API; we show — when missing.
    """
    loc = vehicle.get("location")
    if isinstance(loc, dict) and loc:
        lat = loc.get("lat", loc.get("latitude", loc.get("Latitude")))
        lon = loc.get("lng", loc.get("lon", loc.get("longitude", loc.get("Longitude"))))
    else:
        # GT API returns flat Latitude, Longitude (PascalCase)
        lat = vehicle.get("Latitude", vehicle.get("latitude", vehicle.get("lat")))
        lon = vehicle.get("Longitude", vehicle.get("longitude", vehicle.get("lon", vehicle.get("lng"))))

    return {
        "vehicle_id": vehicle.get("Name", vehicle.get("VehicleID", vehicle.get("vehicle_id", vehicle.get("id")))),
        "lat": lat,
        "lon": lon,
        "load": vehicle.get("Load", vehicle.get("load", vehicle.get("passenger_load", vehicle.get("capacity")))),
        "speed": vehicle.get("GroundSpeed", vehicle.get("speed", 0)),
    }


# ── State machine (time-at-stop) ─────────────────────────────────────

# vehicle_id -> {"consecutive_zero": int, "time_at_stop_sec": int}
_vehicle_state: dict[str, dict[str, int]] = {}


def update_stop_state(vehicle_id: str, speed: float | int) -> int:
    """
    Track consecutive zero-speed polls for *vehicle_id*.

    Returns the current time_at_stop in seconds.
    """
    state = _vehicle_state.setdefault(
        str(vehicle_id), {"consecutive_zero": 0, "time_at_stop_sec": 0}
    )

    if speed is not None and float(speed) == 0:
        state["consecutive_zero"] += 1
    else:
        state["consecutive_zero"] = 0

    state["time_at_stop_sec"] = state["consecutive_zero"] * config.POLL_INTERVAL_SEC
    return state["time_at_stop_sec"]


# ── Output ────────────────────────────────────────────────────────────

def print_table(rows: list[dict[str, Any]], poll_time: str) -> None:
    """Print a pretty table to the console."""
    if not rows:
        print(f"\n[{poll_time}] No vehicles reported.\n")
        return

    headers = ["vehicle_id", "lat", "lon", "load", "speed", "time_at_stop"]
    table_data = [[r.get(h, "—") for h in headers] for r in rows]
    print(f"\n[{poll_time}] {len(rows)} vehicle(s)")
    print(tabulate(table_data, headers=headers, tablefmt="simple", floatfmt=".6f"))
    print()


def append_jsonl(raw: dict[str, Any], path: str) -> None:
    """Append one JSON line (the full raw API response) to *path*."""
    record = {
        "poll_utc": datetime.now(timezone.utc).isoformat(),
        "response": raw,
    }
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")


# ── Main loop ─────────────────────────────────────────────────────────

def poll_once(url: str) -> None:
    """Single poll iteration: fetch → extract → update state → output."""
    poll_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    raw = fetch_vehicle_statuses(url)
    if raw is None:
        print(f"[{poll_time}] Skipping this poll (request failed).\n")
        return

    append_jsonl(raw, config.JSONL_LOG_FILE)

    vehicles_raw = extract_vehicles(raw)
    rows: list[dict[str, Any]] = []
    for v in vehicles_raw:
        fields = extract_vehicle_fields(v)
        tas = update_stop_state(fields["vehicle_id"], fields["speed"])
        fields["time_at_stop"] = f"{tas}s"
        rows.append(fields)

    print_table(rows, poll_time)


def main() -> None:
    url = build_url()

    if not discover_endpoint(url):
        sys.exit(1)

    print(f"[poller] Polling every {config.POLL_INTERVAL_SEC}s  —  Ctrl-C to stop.\n")

    try:
        while True:
            poll_once(url)
            time.sleep(config.POLL_INTERVAL_SEC)
    except KeyboardInterrupt:
        print("\n[poller] Stopped.")


if __name__ == "__main__":
    main()
