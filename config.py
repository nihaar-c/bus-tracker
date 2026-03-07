# ── TransLoc API settings ─────────────────────────────────────────────
# Swap AGENCY_ID or VEHICLE_STATUS_URL if you discover a more specific
# endpoint in the Network tab of bus.gatech.edu.

AGENCY_ID: int = 647

VEHICLE_STATUS_URL: str = "https://feeds.transloc.com/3/vehicle_statuses"

# Set to a full URL to bypass the AGENCY_ID + base-URL construction.
# When non-empty, this is used as-is instead of building the URL.
# GT's live map uses this endpoint (from bus.gatech.edu Network tab):
VEHICLE_STATUS_URL_OVERRIDE: str = (
    "https://bus.gatech.edu/Services/JSONPRelay.svc/GetMapVehiclePoints"
    "?apiKey=8882812681&isPublicMap=true"
)

# ── Polling ───────────────────────────────────────────────────────────

POLL_INTERVAL_SEC: int = 35

REQUEST_TIMEOUT_SEC: int = 10

# ── Output ────────────────────────────────────────────────────────────

JSONL_LOG_FILE: str = "bus_logs.jsonl"
