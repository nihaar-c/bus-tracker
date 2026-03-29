#!/usr/bin/env python3
"""
Generate a self-contained HTML report from collected bus poll data.

Usage:
    python report.py          # writes report.html
    open report.html          # macOS
"""

from __future__ import annotations

import base64
import io
import math
import sqlite3
from collections import defaultdict
from datetime import datetime
from itertools import combinations, groupby
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import config
from db import get_connection, init_db

POLL_SEC = config.POLL_INTERVAL_SEC
OUTPUT_FILE = "report.html"

# ── Route helpers ─────────────────────────────────────────────────────

def _bus_to_route() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for route, buses in config.ROUTES.items():
        for b in buses:
            mapping[b] = route
    return mapping

BUS_TO_ROUTE = _bus_to_route()

def route_for(bus: str) -> str:
    return BUS_TO_ROUTE.get(bus, "Other")

ROUTE_COLORS = {
    "Red": "#c0392b",
    "Blue": "#2980b9",
    "Gold": "#d4a017",
    "Other": "#7f8c8d",
}

def route_color(route: str) -> str:
    return ROUTE_COLORS.get(route, "#7f8c8d")

# ── Data loading ──────────────────────────────────────────────────────

def load_polls(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.execute(
        "SELECT vehicle_name, poll_utc, lat, lon, speed, heading, "
        "capacity, occupation, occupation_pct "
        "FROM vehicle_polls ORDER BY vehicle_name, poll_utc"
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def polls_by_vehicle(polls: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    for p in polls:
        out[p["vehicle_name"]].append(p)
    return dict(out)

def group_by_route(by_vehicle: dict[str, list[dict]]) -> dict[str, dict[str, list[dict]]]:
    routes: dict[str, dict[str, list[dict]]] = defaultdict(dict)
    for bus, rows in sorted(by_vehicle.items()):
        routes[route_for(bus)][bus] = rows
    return dict(routes)

# ── Haversine ─────────────────────────────────────────────────────────

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371_000
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# ── Stop / break analysis ────────────────────────────────────────────

def runs_of_stopped(rows: list[dict]) -> list[dict]:
    stops: list[dict] = []
    i = 0
    while i < len(rows):
        if rows[i]["speed"] != 0:
            i += 1
            continue
        start = i
        while i < len(rows) and rows[i]["speed"] == 0:
            i += 1
        first, last = rows[start], rows[i - 1]
        stops.append({
            "lat": first["lat"],
            "lon": first["lon"],
            "arrived": first["poll_utc"],
            "left": last["poll_utc"],
            "duration_sec": (i - start) * POLL_SEC,
        })
    return stops

def break_stats(bus: str, rows: list[dict]) -> dict:
    stops = runs_of_stopped(rows)
    durations = [s["duration_sec"] for s in stops]
    total = sum(durations) if durations else 0
    return {
        "bus": bus,
        "num_stops": len(stops),
        "total_break_sec": total,
        "avg_break_sec": total // len(stops) if stops else 0,
        "max_break_sec": max(durations) if durations else 0,
        "stops": stops,
    }

# ── Speed stats ───────────────────────────────────────────────────────

def speed_stats(bus: str, rows: list[dict]) -> dict:
    moving = [r["speed"] for r in rows if r["speed"] and r["speed"] > 0]
    return {
        "bus": bus,
        "avg_speed": sum(moving) / len(moving) if moving else 0,
        "max_speed": max(moving) if moving else 0,
        "min_speed": min(moving) if moving else 0,
        "total_polls": len(rows),
        "moving_polls": len(moving),
        "stopped_polls": len(rows) - len(moving),
    }

# ── Heading helpers ───────────────────────────────────────────────────

def heading_diff(h1: float | None, h2: float | None) -> float | None:
    """Smallest angle between two compass headings (0-180). None if either is missing."""
    if h1 is None or h2 is None:
        return None
    diff = abs(h1 - h2) % 360
    return diff if diff <= 180 else 360 - diff

def classify_direction(h1: float | None, h2: float | None) -> str:
    """
    same     = heading difference ≤ 60°  (traveling roughly together)
    opposite = heading difference ≥ 120° (traveling toward or away from each other)
    angled   = everything in between
    unknown  = one or both headings missing / buses stopped (heading=0)
    """
    if h1 is None or h2 is None or h1 == 0 or h2 == 0:
        return "unknown"
    diff = heading_diff(h1, h2)
    if diff is None:
        return "unknown"
    if diff <= 60:
        return "same"
    if diff >= 120:
        return "opposite"
    return "angled"

DIRECTION_LABELS = {
    "same": "Same direction",
    "opposite": "Opposite direction",
    "angled": "Angled",
    "unknown": "Unknown (stopped)",
}

# ── Inter-bus distance ────────────────────────────────────────────────

def parse_ts(ts: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f+00:00", "%Y-%m-%dT%H:%M:%S+00:00",
                "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            continue
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def inter_bus_distances(buses_data: dict[str, list[dict]]) -> list[dict]:
    """For each poll timestamp, compute pairwise distances and heading relationship."""
    by_time: dict[str, dict[str, dict]] = defaultdict(dict)
    for bus, rows in buses_data.items():
        for r in rows:
            by_time[r["poll_utc"]][bus] = r

    results: list[dict] = []
    for ts in sorted(by_time.keys()):
        snapshot = by_time[ts]
        bus_list = sorted(snapshot.keys())
        if len(bus_list) < 2:
            continue
        for b1, b2 in combinations(bus_list, 2):
            r1, r2 = snapshot[b1], snapshot[b2]
            if r1["lat"] and r1["lon"] and r2["lat"] and r2["lon"]:
                d = haversine_m(r1["lat"], r1["lon"], r2["lat"], r2["lon"])
                h1, h2 = r1.get("heading"), r2.get("heading")
                direction = classify_direction(h1, h2)
                hdiff = heading_diff(h1, h2)
                results.append({
                    "ts": ts, "bus1": b1, "bus2": b2,
                    "distance_m": d,
                    "heading1": h1, "heading2": h2,
                    "heading_diff": hdiff,
                    "direction": direction,
                })
    return results

def spacing_stats(distances: list[dict]) -> dict:
    if not distances:
        return {"mean_m": 0, "min_m": 0, "max_m": 0}
    ds = [d["distance_m"] for d in distances]
    return {"mean_m": sum(ds) / len(ds), "min_m": min(ds), "max_m": max(ds)}

ALERT_WARN_PCT = 0.25   # 25% below pair mean → warning
ALERT_CRIT_PCT = 0.30   # 30% below pair mean → critical

def compute_spacing_alerts(distances: list[dict]) -> tuple[dict[str, float], list[dict]]:
    """
    Returns (pair_means, alerts).
    pair_means: {pair_label: mean_distance_m}
    alerts: list of dicts with ts, pair, distance_m, pair_mean, pct_below, severity
    """
    by_pair: dict[str, list[dict]] = defaultdict(list)
    for d in distances:
        pair = f"{d['bus1']} ↔ {d['bus2']}"
        by_pair[pair].append(d)

    pair_means: dict[str, float] = {}
    for pair, pts in by_pair.items():
        pair_means[pair] = sum(p["distance_m"] for p in pts) / len(pts)

    alerts: list[dict] = []
    for d in distances:
        # Only flag when buses are heading the same direction —
        # close distance while opposite is just a normal pass.
        if d.get("direction") != "same":
            continue
        pair = f"{d['bus1']} ↔ {d['bus2']}"
        mean = pair_means[pair]
        if mean == 0:
            continue
        pct_below = (mean - d["distance_m"]) / mean
        if pct_below >= ALERT_CRIT_PCT:
            severity = "critical"
        elif pct_below >= ALERT_WARN_PCT:
            severity = "warning"
        else:
            continue
        alerts.append({
            "ts": d["ts"],
            "pair": pair,
            "distance_m": d["distance_m"],
            "pair_mean": mean,
            "pct_below": pct_below,
            "severity": severity,
            "direction": "same",
        })
    alerts.sort(key=lambda a: a["ts"])
    return pair_means, alerts

# ── Chart helpers ─────────────────────────────────────────────────────

def fig_to_base64(fig: plt.Figure) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()

def make_speed_chart(route: str, buses_data: dict[str, list[dict]]) -> str:
    fig, ax = plt.subplots(figsize=(10, 3.5))
    for bus, rows in sorted(buses_data.items()):
        times = [parse_ts(r["poll_utc"]) for r in rows]
        speeds = [r["speed"] for r in rows]
        ax.plot(times, speeds, label=bus, linewidth=1.2, alpha=0.85)
    ax.set_title(f"{route} Route — Speed Over Time", fontsize=12)
    ax.set_ylabel("Speed (mph)")
    ax.set_xlabel("")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.legend(fontsize=8, loc="upper right")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    return fig_to_base64(fig)

def make_distance_chart(route: str, distances: list[dict],
                        pair_means: dict[str, float] | None = None,
                        alerts: list[dict] | None = None) -> str:
    fig, ax = plt.subplots(figsize=(10, 4))

    from collections import Counter
    pairs_per_ts = Counter(d["ts"] for d in distances)
    valid_ts = {ts for ts, cnt in pairs_per_ts.items() if cnt >= 2}
    distances = [d for d in distances if d["ts"] in valid_ts]

    by_pair: dict[str, list] = defaultdict(list)
    for d in distances:
        pair = f"{d['bus1']}↔{d['bus2']}"
        by_pair[pair].append(d)
    for pair, pts in sorted(by_pair.items()):
        times = [parse_ts(p["ts"]) for p in pts]
        dists = [p["distance_m"] for p in pts]
        ax.plot(times, dists, label=pair, linewidth=1.2, alpha=0.85)

    # Draw alert threshold line (route-wide mean × 0.75)
    if pair_means:
        route_mean = sum(pair_means.values()) / len(pair_means)
        warn_line = route_mean * (1 - ALERT_WARN_PCT)
        crit_line = route_mean * (1 - ALERT_CRIT_PCT)
        ax.axhline(y=warn_line, color="#e67e22", linestyle="--", linewidth=1, alpha=0.7, label=f"Warning ({ALERT_WARN_PCT:.0%} below avg)")
        ax.axhline(y=crit_line, color="#c0392b", linestyle="--", linewidth=1, alpha=0.7, label=f"Critical ({ALERT_CRIT_PCT:.0%} below avg)")

    # Plot alert markers
    if alerts:
        warn_alerts = [a for a in alerts if a["severity"] == "warning" and a["ts"] in valid_ts]
        crit_alerts = [a for a in alerts if a["severity"] == "critical" and a["ts"] in valid_ts]
        if warn_alerts:
            ax.scatter([parse_ts(a["ts"]) for a in warn_alerts],
                       [a["distance_m"] for a in warn_alerts],
                       color="#e67e22", marker="v", s=18, zorder=5, alpha=0.6)
        if crit_alerts:
            ax.scatter([parse_ts(a["ts"]) for a in crit_alerts],
                       [a["distance_m"] for a in crit_alerts],
                       color="#c0392b", marker="v", s=24, zorder=5, alpha=0.8)

    ax.set_title(f"{route} Route — Inter-Bus Distance Over Time", fontsize=12)
    ax.set_ylabel("Distance (meters)")
    ax.set_xlabel("")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.legend(fontsize=7, loc="upper right", ncol=2)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    return fig_to_base64(fig)

def make_occupation_chart(route: str, buses_data: dict[str, list[dict]]) -> str | None:
    """Occupation over time per bus. Returns None if no data."""
    has_data = False
    fig, ax = plt.subplots(figsize=(10, 3.5))
    for bus, rows in sorted(buses_data.items()):
        occ_rows = [(r["poll_utc"], r["occupation"]) for r in rows if r.get("occupation") is not None]
        if not occ_rows:
            continue
        has_data = True
        times = [parse_ts(ts) for ts, _ in occ_rows]
        occs = [o for _, o in occ_rows]
        ax.plot(times, occs, label=bus, linewidth=1.2, alpha=0.85)
    if not has_data:
        plt.close(fig)
        return None
    ax.set_title(f"{route} Route — Ridership Over Time", fontsize=12)
    ax.set_ylabel("Passengers")
    ax.set_xlabel("")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.legend(fontsize=8, loc="upper right")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    return fig_to_base64(fig)

# ── Formatting helpers ────────────────────────────────────────────────

def fmt_dur(sec: int) -> str:
    if sec < 60:
        return f"{sec}s"
    m, s = divmod(sec, 60)
    return f"{m}m {s}s" if s else f"{m}m"

def fmt_ts(ts: str) -> str:
    return ts[:19].replace("T", " ") if ts else ""

def fmt_speed(v: float) -> str:
    return f"{v:.1f}"

def fmt_dist(m: float) -> str:
    if m >= 1000:
        return f"{m / 1000:.2f} km"
    return f"{m:.0f} m"

# ── HTML generation ───────────────────────────────────────────────────

CSS = """
* { box-sizing: border-box; }
body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; margin: 0; padding: 24px 40px; background: #fafafa; color: #222; line-height: 1.5; }
h1 { font-size: 1.8rem; margin-bottom: 4px; }
h2 { font-size: 1.35rem; margin-top: 36px; padding-bottom: 6px; border-bottom: 3px solid; }
h3 { font-size: 1.1rem; margin-top: 24px; }
.subtitle { color: #666; font-size: 0.95rem; margin-bottom: 28px; }
table { border-collapse: collapse; width: 100%; margin: 12px 0 20px 0; font-size: 0.88rem; }
th { background: #eee; text-align: left; padding: 8px 12px; font-weight: 600; }
td { padding: 7px 12px; border-bottom: 1px solid #e0e0e0; }
tr:nth-child(even) td { background: #f7f7f7; }
.chart { margin: 16px 0; text-align: center; }
.chart img { max-width: 100%; border: 1px solid #ddd; border-radius: 6px; }
.route-tag { display: inline-block; padding: 2px 10px; border-radius: 4px; color: #fff; font-weight: 600; font-size: 0.85rem; vertical-align: middle; margin-right: 6px; }
.note { background: #fff3cd; border-left: 4px solid #d4a017; padding: 12px 16px; border-radius: 4px; margin: 16px 0; font-size: 0.9rem; }
.stat-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 12px; margin: 12px 0 20px 0; }
.stat-card { background: #fff; border: 1px solid #e0e0e0; border-radius: 6px; padding: 14px; text-align: center; }
.stat-card .value { font-size: 1.4rem; font-weight: 700; }
.stat-card .label { font-size: 0.8rem; color: #666; margin-top: 2px; }
"""

def tag(el: str, content: str, **attrs: str) -> str:
    a = "".join(f' {k.rstrip("_")}="{v}"' for k, v in attrs.items())
    return f"<{el}{a}>{content}</{el}>"

def table_html(headers: list[str], rows: list[list[str]], raw_html: bool = True) -> str:
    """Build an HTML table. Cell values may contain inline HTML when raw_html=True."""
    h = "".join(tag("th", h) for h in headers)
    body = ""
    for row in rows:
        cells = "".join(f"<td>{c}</td>" for c in row) if raw_html else "".join(tag("td", str(c)) for c in row)
        body += f"<tr>{cells}</tr>"
    return tag("table", tag("thead", tag("tr", h)) + tag("tbody", body))

def stat_card(value: str, label: str) -> str:
    return f'<div class="stat-card"><div class="value">{value}</div><div class="label">{label}</div></div>'

def chart_img(b64: str) -> str:
    return f'<div class="chart"><img src="data:image/png;base64,{b64}"></div>'

def route_badge(route: str) -> str:
    return f'<span class="route-tag" style="background:{route_color(route)}">{route}</span>'

# ── Report assembly ───────────────────────────────────────────────────

def build_report(conn: sqlite3.Connection) -> str:
    polls = load_polls(conn)
    if not polls:
        return "<html><body><h1>No data</h1><p>Run the poller first.</p></body></html>"

    by_vehicle = polls_by_vehicle(polls)
    by_route = group_by_route(by_vehicle)

    ordered_routes = [r for r in ("Red", "Blue", "Gold") if r in by_route]

    time_range_start = fmt_ts(polls[0]["poll_utc"])
    time_range_end = fmt_ts(polls[-1]["poll_utc"])
    total_polls = len(polls)
    total_buses = len(by_vehicle)

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>GT Bus Analytics Report</title><style>{CSS}</style></head><body>
<h1>Georgia Tech Bus Analytics Report</h1>
<div class="subtitle">Data from {time_range_start} to {time_range_end} &mdash; {total_polls:,} polls across {total_buses} buses</div>
"""

    # ── Section 1: Route Overview ─────────────────────────────────────
    html += tag("h2", "1. Route Overview", style="border-color:#444")
    overview_rows = []
    for route in ordered_routes:
        buses = sorted(by_route[route].keys())
        total = sum(len(by_route[route][b]) for b in buses)
        first = min(r["poll_utc"] for b in buses for r in by_route[route][b])
        last = max(r["poll_utc"] for b in buses for r in by_route[route][b])
        overview_rows.append([
            route_badge(route) + route,
            ", ".join(buses),
            str(len(buses)),
            str(total),
            fmt_ts(first),
            fmt_ts(last),
        ])
    html += table_html(["Route", "Buses", "# Buses", "Total Polls", "First Poll", "Last Poll"], overview_rows)

    # ── Section 2: Stop / Break Analysis ──────────────────────────────
    html += tag("h2", "2. Stop &amp; Break Analysis", style="border-color:#444")
    html += "<p>A 'stop' is a consecutive run of polls with speed = 0. Duration = count &times; 35s poll interval.</p>"

    for route in ordered_routes:
        html += tag("h3", route_badge(route) + f" {route} Route")
        buses_data = by_route[route]
        break_rows = []
        for bus in sorted(buses_data.keys()):
            bs = break_stats(bus, buses_data[bus])
            break_rows.append([
                bus,
                str(bs["num_stops"]),
                fmt_dur(bs["total_break_sec"]),
                fmt_dur(bs["avg_break_sec"]),
                fmt_dur(bs["max_break_sec"]),
            ])
        html += table_html(["Bus", "# Stops", "Total Break Time", "Avg Break", "Longest Break"], break_rows)

        # Detailed stops per bus
        for bus in sorted(buses_data.keys()):
            stops = runs_of_stopped(buses_data[bus])
            if not stops:
                continue
            html += f"<details><summary><strong>{bus}</strong> — {len(stops)} stop(s)</summary>"
            stop_rows = [[
                fmt_ts(s["arrived"]),
                fmt_ts(s["left"]),
                fmt_dur(s["duration_sec"]),
                f'{s["lat"]:.5f}, {s["lon"]:.5f}' if s["lat"] and s["lon"] else "—",
            ] for s in stops]
            html += table_html(["Arrived", "Left", "Duration", "Location"], stop_rows)
            html += "</details>"

    # ── Section 3: Speed Profiles ─────────────────────────────────────
    html += tag("h2", "3. Speed Profiles", style="border-color:#444")
    html += "<p>Average speed excludes polls where bus was stopped (speed = 0).</p>"

    for route in ordered_routes:
        html += tag("h3", route_badge(route) + f" {route} Route")
        buses_data = by_route[route]

        speed_rows = []
        for bus in sorted(buses_data.keys()):
            ss = speed_stats(bus, buses_data[bus])
            pct_moving = (ss["moving_polls"] / ss["total_polls"] * 100) if ss["total_polls"] else 0
            speed_rows.append([
                bus,
                fmt_speed(ss["avg_speed"]),
                fmt_speed(ss["max_speed"]),
                fmt_speed(ss["min_speed"]),
                f'{pct_moving:.0f}%',
                str(ss["total_polls"]),
            ])
        html += table_html(["Bus", "Avg Speed", "Max Speed", "Min Speed", "% Moving", "Total Polls"], speed_rows)

        b64 = make_speed_chart(route, buses_data)
        html += chart_img(b64)

    # ── Section 4: Inter-Bus Distance & Propagation Delay ─────────────
    html += tag("h2", "4. Inter-Bus Distance &amp; Propagation Delay", style="border-color:#444")
    html += "<p>Pairwise haversine distance between buses on the same route at each poll. "
    html += "The <strong>Heading</strong> field classifies each measurement:</p>"
    html += "<ul>"
    html += "<li><strong>Same direction</strong> (≤60° apart): buses traveling together — close distance = bunching</li>"
    html += "<li><strong>Opposite direction</strong> (≥120° apart): buses passing each other — close distance is expected</li>"
    html += "<li><strong>Angled</strong> (60°–120°): turning / diverging</li>"
    html += "<li><strong>Unknown</strong>: one or both buses stopped (heading = 0)</li>"
    html += "</ul>"

    any_multi = False
    for route in ordered_routes:
        buses_data = by_route[route]
        if len(buses_data) < 2:
            continue
        any_multi = True
        html += tag("h3", route_badge(route) + f" {route} Route")

        distances = inter_bus_distances(buses_data)
        if not distances:
            html += "<p>Not enough overlapping poll timestamps for distance calculation.</p>"
            continue

        # Summary table per pair
        by_pair: dict[str, list[dict]] = defaultdict(list)
        for d in distances:
            pair = f"{d['bus1']} ↔ {d['bus2']}"
            by_pair[pair].append(d)

        dist_rows = []
        for pair, pts in sorted(by_pair.items()):
            ds = [p["distance_m"] for p in pts]
            dist_rows.append([
                pair,
                fmt_dist(sum(ds) / len(ds)),
                fmt_dist(min(ds)),
                fmt_dist(max(ds)),
                str(len(ds)),
            ])
        html += table_html(["Bus Pair", "Mean Distance", "Min (bunching)", "Max", "Samples"], dist_rows)

        overall = spacing_stats(distances)
        pair_means, alerts = compute_spacing_alerts(distances)

        html += '<div class="stat-grid">'
        html += stat_card(fmt_dist(overall["mean_m"]), "Avg Spacing")
        html += stat_card(fmt_dist(overall["min_m"]), "Min Spacing (bunching)")
        html += stat_card(fmt_dist(overall["max_m"]), "Max Spacing")
        warn_count = sum(1 for a in alerts if a["severity"] == "warning")
        crit_count = sum(1 for a in alerts if a["severity"] == "critical")
        html += stat_card(str(warn_count), "Spacing Warnings (25%↓)")
        html += stat_card(f'<span style="color:#c0392b">{crit_count}</span>', "Spacing Critical (30%↓)")
        html += "</div>"

        b64 = make_distance_chart(route, distances, pair_means, alerts)
        html += chart_img(b64)

        # Spacing alerts log
        if alerts:
            html += tag("h4", "Spacing Alerts")
            html += f"<p>Timestamps where a bus pair's distance fell 25%+ below that pair's average. "
            html += f"Orange ▼ = warning (25-30% below), Red ▼ = critical (30%+ below).</p>"
            alert_rows = []
            for a in alerts:
                sev_label = (
                    f'<span style="color:#c0392b;font-weight:700">CRITICAL</span>'
                    if a["severity"] == "critical"
                    else '<span style="color:#e67e22;font-weight:700">WARNING</span>'
                )
                alert_rows.append([
                    fmt_ts(a["ts"]),
                    a["pair"],
                    fmt_dist(a["distance_m"]),
                    fmt_dist(a["pair_mean"]),
                    f'{a["pct_below"]:.0%}',
                    DIRECTION_LABELS.get(a["direction"], a["direction"]),
                    sev_label,
                ])
            html += table_html(
                ["Time", "Pair", "Distance", "Pair Avg", "% Below", "Direction", "Severity"],
                alert_rows,
            )
        else:
            html += '<div class="note">No spacing alerts — all pairs stayed within 25% of their average distance.</div>'

        # Direction breakdown
        html += tag("h4", "Heading Analysis")
        dir_counts: dict[str, int] = defaultdict(int)
        dir_dists: dict[str, list[float]] = defaultdict(list)
        for d in distances:
            dir_counts[d["direction"]] += 1
            dir_dists[d["direction"]].append(d["distance_m"])

        dir_rows = []
        for direction in ("same", "opposite", "angled", "unknown"):
            cnt = dir_counts.get(direction, 0)
            if cnt == 0:
                continue
            ds = dir_dists[direction]
            pct = cnt / len(distances) * 100
            dir_rows.append([
                DIRECTION_LABELS[direction],
                str(cnt),
                f"{pct:.0f}%",
                fmt_dist(sum(ds) / len(ds)),
                fmt_dist(min(ds)),
                fmt_dist(max(ds)),
            ])
        html += table_html(
            ["Direction", "Samples", "% of Total", "Avg Dist", "Min Dist", "Max Dist"],
            dir_rows,
        )

        # Same-direction close encounters = actual bunching
        same_dir = [d for d in distances if d["direction"] == "same"]
        if same_dir:
            bunching_threshold_m = 200
            bunching = [d for d in same_dir if d["distance_m"] <= bunching_threshold_m]
            html += '<div class="stat-grid">'
            html += stat_card(str(len(same_dir)), "Same-Dir Samples")
            sd_ds = [d["distance_m"] for d in same_dir]
            html += stat_card(fmt_dist(sum(sd_ds) / len(sd_ds)), "Avg Same-Dir Spacing")
            html += stat_card(str(len(bunching)), f"Bunching Events (≤{bunching_threshold_m}m, same dir)")
            html += "</div>"

            if bunching:
                html += f'<div class="note"><strong>{len(bunching)}</strong> times two buses were within '
                html += f'{bunching_threshold_m}m <em>and</em> heading the same direction — true bunching.</div>'
        else:
            html += '<div class="note">No heading data available yet for same-direction analysis. '
            html += 'Heading is recorded from new polls going forward.</div>'

    if not any_multi:
        html += "<p>No routes with 2+ buses found — inter-bus distance requires at least two active buses on the same route.</p>"

    single_routes = [r for r in ordered_routes if len(by_route[r]) < 2]
    if single_routes:
        names = ", ".join(single_routes)
        html += f'<div class="note">Routes with only 1 bus ({names}): inter-bus distance / propagation delay is not applicable.</div>'

    # ── Section 5: Capacity Usage ─────────────────────────────────────
    html += tag("h2", "5. Capacity &amp; Ridership", style="border-color:#444")
    html += "<p>Data from <code>GetVehicleCapacities</code>: current occupation vs. seat capacity per bus.</p>"

    # Check if any capacity data exists
    has_cap = any(p.get("occupation") is not None for p in polls)
    if not has_cap:
        html += '<div class="note">No capacity data recorded yet. '
        html += "Capacity is fetched alongside each poll going forward.</div>"
    else:
        for route in ordered_routes:
            html += tag("h3", route_badge(route) + f" {route} Route")
            buses_data = by_route[route]

            cap_rows = []
            for bus in sorted(buses_data.keys()):
                rows_with_occ = [r for r in buses_data[bus] if r.get("occupation") is not None]
                if not rows_with_occ:
                    cap_rows.append([bus, "—", "—", "—", "—", "—", "0"])
                    continue
                occs = [r["occupation"] for r in rows_with_occ]
                caps = [r["capacity"] for r in rows_with_occ if r.get("capacity")]
                cap_val = caps[0] if caps else "—"
                avg_occ = sum(occs) / len(occs)
                max_occ = max(occs)
                min_occ = min(occs)
                pct_vals = [r["occupation_pct"] for r in rows_with_occ if r.get("occupation_pct") is not None]
                avg_pct = (sum(pct_vals) / len(pct_vals) * 100) if pct_vals else 0
                peak_pct = (max(pct_vals) * 100) if pct_vals else 0
                cap_rows.append([
                    bus,
                    str(cap_val),
                    f"{avg_occ:.0f}",
                    str(max_occ),
                    str(min_occ),
                    f"{avg_pct:.0f}%",
                    f"{peak_pct:.0f}%",
                ])
            html += table_html(
                ["Bus", "Capacity", "Avg Riders", "Peak Riders", "Min Riders", "Avg Util%", "Peak Util%"],
                cap_rows,
            )

            # Occupation over time chart
            b64 = make_occupation_chart(route, buses_data)
            if b64:
                html += chart_img(b64)

    html += "</body></html>"
    return html


def main() -> None:
    init_db()
    conn = get_connection()
    try:
        html = build_report(conn)
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"Report written to {OUTPUT_FILE}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
