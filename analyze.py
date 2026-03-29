#!/usr/bin/env python3
"""
Spacing-capacity correlation and low-popularity stop analysis.

Reads from bus_data_v2.db and produces a self-contained analysis.html.

Usage:
    python analyze.py
    open analysis.html
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import config
from db import get_connection, init_db
from report import (
    CSS,
    POLL_SEC,
    chart_img,
    compute_spacing_alerts,
    fig_to_base64,
    fmt_dist,
    fmt_dur,
    fmt_ts,
    group_by_route,
    haversine_m,
    inter_bus_distances,
    load_polls,
    parse_ts,
    polls_by_vehicle,
    route_badge,
    route_for,
    stat_card,
    table_html,
    tag,
)

OUTPUT_FILE = "analysis.html"
IMBALANCE_THRESHOLD_PCT = 0.20  # 20 percentage-point utilization gap

# ── Part 1: Spacing–Capacity Correlation ──────────────────────────────

def _build_poll_index(polls: list[dict]) -> dict[str, dict[str, dict]]:
    """Index polls by (poll_utc, vehicle_name) for O(1) lookup."""
    idx: dict[str, dict[str, dict]] = defaultdict(dict)
    for p in polls:
        idx[p["poll_utc"]][p["vehicle_name"]] = p
    return dict(idx)


def enrich_alerts_with_capacity(
    alerts: list[dict], poll_index: dict[str, dict[str, dict]]
) -> list[dict]:
    """Attach occupation/capacity for both buses at each alert timestamp."""
    enriched: list[dict] = []
    for a in alerts:
        ts = a["ts"]
        pair = a["pair"]
        b1, b2 = [x.strip() for x in pair.split("↔")]
        snapshot = poll_index.get(ts, {})
        r1, r2 = snapshot.get(b1), snapshot.get(b2)
        if not r1 or not r2:
            continue
        occ1 = r1.get("occupation")
        occ2 = r2.get("occupation")
        cap1 = r1.get("capacity")
        cap2 = r2.get("capacity")
        if occ1 is None or occ2 is None:
            continue

        pct1 = (occ1 / cap1) if cap1 else 0
        pct2 = (occ2 / cap2) if cap2 else 0
        util_diff = abs(pct1 - pct2)
        combined = occ1 + occ2
        ideal = combined / 2 if combined else 0
        imbalance = util_diff >= IMBALANCE_THRESHOLD_PCT

        enriched.append({
            **a,
            "bus1": b1, "bus2": b2,
            "occ1": occ1, "occ2": occ2,
            "cap1": cap1, "cap2": cap2,
            "pct1": pct1, "pct2": pct2,
            "util_diff": util_diff,
            "occ_diff": abs(occ1 - occ2),
            "combined_occ": combined,
            "ideal_split": ideal,
            "imbalance": imbalance,
        })
    return enriched


def make_scatter_chart(enriched: list[dict]) -> str:
    """Scatter: X=distance, Y=occupation diff. Color by imbalance."""
    fig, ax = plt.subplots(figsize=(9, 5))
    normal = [e for e in enriched if not e["imbalance"]]
    bad = [e for e in enriched if e["imbalance"]]

    if normal:
        ax.scatter(
            [e["distance_m"] for e in normal],
            [e["occ_diff"] for e in normal],
            c="#3498db", alpha=0.5, s=30, label="Balanced (<20% gap)", edgecolors="none",
        )
    if bad:
        ax.scatter(
            [e["distance_m"] for e in bad],
            [e["occ_diff"] for e in bad],
            c="#c0392b", alpha=0.7, s=50, label="Imbalanced (>=20% gap)", edgecolors="none",
        )
    ax.set_xlabel("Pair Distance (meters)")
    ax.set_ylabel("Occupation Difference (riders)")
    ax.set_title("Spacing vs. Capacity Imbalance", fontsize=12)
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    return fig_to_base64(fig)


def make_imbalance_timeline(enriched: list[dict]) -> str:
    """Timeline: X=time, Y=util_diff, colored by severity."""
    fig, ax = plt.subplots(figsize=(10, 3.5))
    times = [parse_ts(e["ts"]) for e in enriched]
    diffs = [e["util_diff"] * 100 for e in enriched]
    colors = ["#c0392b" if e["imbalance"] else "#3498db" for e in enriched]
    ax.scatter(times, diffs, c=colors, s=20, alpha=0.6, edgecolors="none")
    ax.axhline(y=IMBALANCE_THRESHOLD_PCT * 100, color="#e67e22", linestyle="--",
               linewidth=1, alpha=0.7, label=f"Imbalance threshold ({IMBALANCE_THRESHOLD_PCT:.0%})")
    ax.set_ylabel("Utilization Gap (%)")
    ax.set_title("Capacity Imbalance Over Time", fontsize=12)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    return fig_to_base64(fig)


# ── Part 2: Low-Popularity Stop Discovery ─────────────────────────────

def detect_stop_events(by_vehicle: dict[str, list[dict]]) -> list[dict]:
    """
    Find stop events: consecutive speed=0 runs of 2+ polls (>=70s).
    Capture occupation before and after to measure rider change.
    """
    events: list[dict] = []
    for bus, rows in by_vehicle.items():
        i = 0
        while i < len(rows):
            if rows[i]["speed"] != 0:
                i += 1
                continue
            start = i
            while i < len(rows) and rows[i]["speed"] == 0:
                i += 1
            count = i - start
            if count < 2:
                continue

            first = rows[start]
            if not first.get("lat") or not first.get("lon"):
                continue

            occ_before = rows[start - 1].get("occupation") if start > 0 else None
            occ_after = rows[i].get("occupation") if i < len(rows) else None
            occ_change = None
            if occ_before is not None and occ_after is not None:
                occ_change = abs(occ_after - occ_before)

            # Filter: require occ_change data OR very long stop (3+ polls)
            if occ_change is None and count < 3:
                continue

            events.append({
                "bus": bus,
                "lat": first["lat"],
                "lon": first["lon"],
                "arrived": first["poll_utc"],
                "duration_sec": count * POLL_SEC,
                "polls": count,
                "occ_before": occ_before,
                "occ_after": occ_after,
                "occ_change": occ_change,
            })
    return events


def cluster_stops(events: list[dict], radius_m: float = 50) -> list[dict]:
    """Greedy distance-based clustering of stop events."""
    clusters: list[dict] = []  # {lat, lon, events: [...]}

    for ev in events:
        best_cluster = None
        best_dist = radius_m + 1
        for cl in clusters:
            d = haversine_m(ev["lat"], ev["lon"], cl["lat"], cl["lon"])
            if d < best_dist:
                best_dist = d
                best_cluster = cl
        if best_cluster and best_dist <= radius_m:
            best_cluster["events"].append(ev)
            n = len(best_cluster["events"])
            best_cluster["lat"] = (best_cluster["lat"] * (n - 1) + ev["lat"]) / n
            best_cluster["lon"] = (best_cluster["lon"] * (n - 1) + ev["lon"]) / n
        else:
            clusters.append({"lat": ev["lat"], "lon": ev["lon"], "events": [ev]})
    return clusters


def score_clusters(clusters: list[dict]) -> list[dict]:
    """Compute per-cluster metrics and sort by popularity score (ascending)."""
    scored: list[dict] = []
    for i, cl in enumerate(clusters):
        evts = cl["events"]
        visit_count = len(evts)
        unique_buses = len({e["bus"] for e in evts})
        durations = [e["duration_sec"] for e in evts]
        avg_dur = sum(durations) / len(durations) if durations else 0

        occ_changes = [e["occ_change"] for e in evts if e["occ_change"] is not None]
        avg_occ_change = sum(occ_changes) / len(occ_changes) if occ_changes else 0
        max_occ_change = max(occ_changes) if occ_changes else 0

        popularity = visit_count * avg_occ_change

        low_pop = avg_occ_change < 2 and unique_buses >= 3

        scored.append({
            "id": i + 1,
            "lat": cl["lat"],
            "lon": cl["lon"],
            "visit_count": visit_count,
            "unique_buses": unique_buses,
            "avg_dur_sec": avg_dur,
            "avg_occ_change": avg_occ_change,
            "max_occ_change": max_occ_change,
            "popularity": popularity,
            "low_pop": low_pop,
            "events": evts,
        })
    scored.sort(key=lambda s: s["popularity"])
    return scored


def make_stop_map(scored: list[dict]) -> str:
    """Scatter plot of discovered stops: size=visits, color=avg occ change."""
    fig, ax = plt.subplots(figsize=(9, 7))

    lats = [s["lat"] for s in scored]
    lons = [s["lon"] for s in scored]
    sizes = [max(s["visit_count"] * 8, 20) for s in scored]
    colors = [s["avg_occ_change"] for s in scored]

    sc = ax.scatter(lons, lats, s=sizes, c=colors, cmap="RdYlGn", alpha=0.7, edgecolors="#333", linewidths=0.5)
    cbar = fig.colorbar(sc, ax=ax, shrink=0.7)
    cbar.set_label("Avg Rider Change", fontsize=9)

    for s in scored:
        if s["low_pop"]:
            ax.annotate(f'#{s["id"]}', (s["lon"], s["lat"]),
                        fontsize=7, fontweight="bold", color="#c0392b",
                        textcoords="offset points", xytext=(5, 5))

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("Discovered Bus Stops — Size = Visits, Color = Avg Rider Change", fontsize=11)
    ax.grid(True, alpha=0.2)
    fig.tight_layout()
    return fig_to_base64(fig)


# ── HTML report assembly ──────────────────────────────────────────────

def build_analysis(conn) -> str:
    polls = load_polls(conn)
    if not polls:
        return "<html><body><h1>No data</h1></body></html>"

    by_vehicle = polls_by_vehicle(polls)
    by_route = group_by_route(by_vehicle)
    poll_index = _build_poll_index(polls)

    time_start = fmt_ts(polls[0]["poll_utc"])
    time_end = fmt_ts(polls[-1]["poll_utc"])

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>GT Bus Deep Analysis</title><style>{CSS}</style></head><body>
<h1>Spacing–Capacity Correlation &amp; Stop Popularity</h1>
<div class="subtitle">Data from {time_start} to {time_end} &mdash; {len(polls):,} polls, {len(by_vehicle)} buses</div>
"""

    # ── Part 1 ────────────────────────────────────────────────────────
    html += tag("h2", "1. Spacing–Capacity Imbalance", style="border-color:#c0392b")
    html += "<p>At each spacing alert (same-direction, 25%+ below pair mean), we compare "
    html += "the two buses' ridership. A <strong>capacity imbalance</strong> is flagged when "
    html += f"the utilization gap is &ge;{IMBALANCE_THRESHOLD_PCT:.0%} — one bus is significantly "
    html += "more loaded than the other, indicating bunching is hurting service distribution.</p>"

    ordered_routes = [r for r in ("Red", "Blue", "Gold") if r in by_route]
    if "Other" in by_route:
        ordered_routes.append("Other")

    all_enriched: list[dict] = []
    for route in ordered_routes:
        buses_data = by_route[route]
        if len(buses_data) < 2:
            continue
        distances = inter_bus_distances(buses_data)
        if not distances:
            continue
        _, alerts = compute_spacing_alerts(distances)
        enriched = enrich_alerts_with_capacity(alerts, poll_index)
        all_enriched.extend(enriched)

    if not all_enriched:
        html += '<div class="note">No spacing alerts with capacity data found. '
        html += "Need more polling data with heading + capacity.</div>"
    else:
        imb = [e for e in all_enriched if e["imbalance"]]
        total = len(all_enriched)
        worst = max(all_enriched, key=lambda e: e["util_diff"]) if all_enriched else None

        html += '<div class="stat-grid">'
        html += stat_card(str(total), "Spacing Alerts w/ Capacity")
        html += stat_card(f'<span style="color:#c0392b">{len(imb)}</span>', f"Imbalanced (&ge;{IMBALANCE_THRESHOLD_PCT:.0%} gap)")
        pct_imb = len(imb) / total * 100 if total else 0
        html += stat_card(f"{pct_imb:.0f}%", "Imbalance Rate")
        if worst:
            html += stat_card(f"{worst['util_diff']*100:.0f}%", "Worst Util Gap")
        html += "</div>"

        # Scatter: distance vs occupation diff
        b64 = make_scatter_chart(all_enriched)
        html += chart_img(b64)

        # Timeline
        b64 = make_imbalance_timeline(all_enriched)
        html += chart_img(b64)

        # Top offending pairs
        html += tag("h3", "Top Offending Pairs")
        pair_counts: dict[str, dict] = defaultdict(lambda: {"total": 0, "imbalanced": 0, "max_gap": 0.0})
        for e in all_enriched:
            pc = pair_counts[e["pair"]]
            pc["total"] += 1
            if e["imbalance"]:
                pc["imbalanced"] += 1
            pc["max_gap"] = max(pc["max_gap"], e["util_diff"])
        pair_rows = []
        for pair in sorted(pair_counts, key=lambda p: pair_counts[p]["imbalanced"], reverse=True):
            pc = pair_counts[pair]
            if pc["imbalanced"] == 0:
                continue
            pair_rows.append([
                pair,
                str(pc["total"]),
                f'<span style="color:#c0392b">{pc["imbalanced"]}</span>',
                f'{pc["imbalanced"] / pc["total"] * 100:.0f}%',
                f'{pc["max_gap"] * 100:.0f}%',
            ])
        if pair_rows:
            html += table_html(["Pair", "Alerts", "Imbalanced", "Imbalance Rate", "Worst Gap"], pair_rows)

        # Detailed imbalance log
        html += tag("h3", "Imbalance Events Log")
        html += f"<details><summary>Show all {len(imb)} imbalance events</summary>"
        log_rows = []
        for e in sorted(imb, key=lambda x: x["util_diff"], reverse=True):
            sev = (
                f'<span style="color:#c0392b;font-weight:700">CRITICAL</span>'
                if e["severity"] == "critical"
                else '<span style="color:#e67e22;font-weight:700">WARNING</span>'
            )
            log_rows.append([
                fmt_ts(e["ts"]),
                e["pair"],
                fmt_dist(e["distance_m"]),
                f'{e["bus1"]}: {e["occ1"]}/{e["cap1"]}',
                f'{e["bus2"]}: {e["occ2"]}/{e["cap2"]}',
                f'{e["occ_diff"]} riders',
                f'{e["util_diff"]*100:.0f}%',
                sev,
            ])
        html += table_html(
            ["Time", "Pair", "Distance", "Bus A Load", "Bus B Load", "Rider Diff", "Util Gap", "Spacing Severity"],
            log_rows,
        )
        html += "</details>"

    # ── Part 2 ────────────────────────────────────────────────────────
    html += tag("h2", "2. Low-Popularity Stop Discovery", style="border-color:#2980b9")
    html += "<p>Auto-discovered stops from GPS data. A 'stop' requires speed=0 for "
    html += "2+ consecutive polls (&ge;70s) to exclude red lights and crosswalks. "
    html += "Stops within 50m are clustered as the same physical stop. "
    html += "Rider change = |occupation after - occupation before| each visit.</p>"

    events = detect_stop_events(by_vehicle)
    if not events:
        html += '<div class="note">No qualifying stop events found.</div>'
    else:
        clusters = cluster_stops(events)
        scored = score_clusters(clusters)

        low_pop = [s for s in scored if s["low_pop"]]
        html += '<div class="stat-grid">'
        html += stat_card(str(len(scored)), "Stops Discovered")
        html += stat_card(str(len(events)), "Total Stop Events")
        html += stat_card(f'<span style="color:#c0392b">{len(low_pop)}</span>', "Low-Popularity Stops")
        avg_change_all = sum(s["avg_occ_change"] for s in scored) / len(scored) if scored else 0
        html += stat_card(f"{avg_change_all:.1f}", "Avg Rider Change (all stops)")
        html += "</div>"

        # Stop map
        b64 = make_stop_map(scored)
        html += chart_img(b64)

        # Low-popularity flagged stops
        if low_pop:
            html += tag("h3", "Flagged Low-Popularity Stops")
            html += "<p>Stops with avg rider change &lt;2 AND visited by 3+ different buses. "
            html += "These are candidates for schedule optimization or removal.</p>"
            flag_rows = []
            for s in low_pop:
                flag_rows.append([
                    f'<strong>#{s["id"]}</strong>',
                    f'{s["lat"]:.5f}, {s["lon"]:.5f}',
                    str(s["visit_count"]),
                    str(s["unique_buses"]),
                    f'{s["avg_occ_change"]:.1f}',
                    fmt_dur(int(s["avg_dur_sec"])),
                    f'{s["popularity"]:.1f}',
                ])
            html += table_html(
                ["Stop", "Location", "Visits", "Unique Buses", "Avg Rider Change", "Avg Dwell", "Popularity Score"],
                flag_rows,
            )

        # Full stop ranking table
        html += tag("h3", "All Discovered Stops (ranked by popularity, lowest first)")
        html += f"<details><summary>Show all {len(scored)} stops</summary>"
        all_rows = []
        for s in scored:
            style = ' style="color:#c0392b;font-weight:600"' if s["low_pop"] else ""
            name = f'<span{style}>#{s["id"]}</span>'
            if s["low_pop"]:
                name += ' <span style="color:#c0392b;font-size:0.8em">LOW</span>'
            all_rows.append([
                name,
                f'{s["lat"]:.5f}, {s["lon"]:.5f}',
                str(s["visit_count"]),
                str(s["unique_buses"]),
                f'{s["avg_occ_change"]:.1f}',
                f'{s["max_occ_change"]:.0f}' if s["max_occ_change"] else "—",
                fmt_dur(int(s["avg_dur_sec"])),
                f'{s["popularity"]:.1f}',
            ])
        html += table_html(
            ["Stop", "Location", "Visits", "Buses", "Avg Rider Chg", "Max Rider Chg", "Avg Dwell", "Score"],
            all_rows,
        )
        html += "</details>"

    html += "</body></html>"
    return html


def main() -> None:
    init_db()
    conn = get_connection()
    try:
        html = build_analysis(conn)
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"Analysis written to {OUTPUT_FILE}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
