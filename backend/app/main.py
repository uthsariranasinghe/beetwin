from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .config import (
    PRELOAD_BATCH_SIZE,
    PRELOAD_END_BUFFER_MINUTES,
    PRELOAD_HISTORY_PATH,
    PRELOAD_ON_STARTUP,
    load_kf_config,
)
from .db import get_conn, init_db
from .schemas import (
    AlertsResponse,
    HiveListResponse,
    HiveRegisterIn,
    HistoryResponse,
    LatestResponse,
)
from .services.history import (
    derive_status_from_latest,
    get_alerts,
    get_history,
    get_hive_status,
    get_latest_point,
    get_recent_history,
    list_hives,
    list_latest_points,
)
from .services.ingest import HiveStateRegistry
from .services.preload import preload_history_if_needed


app = FastAPI(
    title="Beehive Digital Twin API",
    version="2.2-replay",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Keep this only because preload may still depend on the registry.
# It is not used for live mode here.
registry: HiveStateRegistry | None = None


# Hives selected for dashboard preload
SELECTED_DASHBOARD_HIVES = [
    202039, 202040, 202043, 202045, 202046,
    202048, 202049, 202051, 202052, 202053,
    202054, 202055, 202056, 202060, 202061,
]


def to_utc_iso(ts: datetime) -> str:
    """
    Convert a datetime object into UTC ISO 8601 format.

    If the datetime is naive, it is treated as UTC.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def build_status_payload(hive_id: int) -> dict:
    """
    Build the status payload for one hive.

    First try to read the stored status from the database.
    If no stored status exists yet, derive a fallback status from the latest point.
    """
    status = get_hive_status(int(hive_id))

    if status is not None:
        return {
            "hive_id": int(hive_id),
            "status": status["status"],
            "status_reason": status["status_reason"],
            "last_ts": status["last_ts"],
            "alert_count": int(status["alert_count"]),
        }

    latest = get_latest_point(int(hive_id))
    derived = derive_status_from_latest(latest)

    return {
        "hive_id": int(hive_id),
        "status": derived["status"],
        "status_reason": derived["status_reason"],
        "last_ts": derived["last_ts"],
        "alert_count": 0,
    }


def build_snapshot_payload(hive_id: int) -> dict:
    """
    Build the full snapshot for one hive.

    A snapshot contains:
    - latest point
    - current hive status
    - active alerts
    """
    point = get_latest_point(int(hive_id))
    status = build_status_payload(int(hive_id))
    alerts = get_alerts(int(hive_id), active_only=True, limit=50)

    return {
        "hive_id": int(hive_id),
        "point": point,
        "status": status,
        "alerts": alerts,
    }


@app.on_event("startup")
async def startup_event() -> None:
    """
    Initialize the database and preload replay history on startup.
    """
    global registry

    init_db()

    # Kept only if preload_history_if_needed still expects a registry.
    kf_config = load_kf_config()
    registry = HiveStateRegistry(kf_config)

    if PRELOAD_ON_STARTUP:
        preload_history_if_needed(
            reg=registry,
            preload_path=PRELOAD_HISTORY_PATH,
            batch_size=PRELOAD_BATCH_SIZE,
            include_missing_rows=True,
            hive_ids=SELECTED_DASHBOARD_HIVES,
            end_buffer_minutes=PRELOAD_END_BUFFER_MINUTES,
        )


@app.get("/api/health")
def health():
    """Simple health-check endpoint."""
    return {"ok": True, "service": "beehive-digital-twin", "mode": "replay"}


@app.get("/api/hives", response_model=HiveListResponse)
def hives():
    """Return the list of registered hive IDs."""
    return {"hives": list_hives()}


@app.get("/api/hives/overview")
def hives_overview():
    """
    Return an overview of all hives with current status and latest point.
    """
    latest_points = {int(p["hive_id"]): p for p in list_latest_points()}
    hive_ids = list_hives()

    items = []
    for hive_id in hive_ids:
        status = build_status_payload(hive_id)
        items.append(
            {
                "hive_id": hive_id,
                "status": status,
                "latest_point": latest_points.get(hive_id),
            }
        )

    return {"items": items, "count": len(items)}


@app.post("/api/hives/register")
def register_hives(req: HiveRegisterIn):
    """
    Register hive IDs in the database if they do not already exist.
    """
    conn = get_conn()
    try:
        with conn:
            for hive_id in req.hive_ids:
                conn.execute(
                    "INSERT OR IGNORE INTO hives (hive_id) VALUES (?);",
                    (int(hive_id),),
                )

        return {
            "count": len(req.hive_ids),
            "hive_ids": [int(h) for h in req.hive_ids],
        }
    finally:
        conn.close()


@app.get("/api/hives/{hive_id}/history", response_model=HistoryResponse)
def hive_history(
    hive_id: int,
    limit: int = Query(500, ge=1, le=50000),
):
    """
    Return the most recent history points for one hive.
    """
    points = get_recent_history(hive_id=hive_id, limit=limit)
    return {"hive_id": hive_id, "points": points}


@app.get("/api/history", response_model=HistoryResponse)
def history(
    hive_id: int = Query(..., ge=0),
    ts_from: datetime = Query(...),
    ts_to: datetime = Query(...),
    limit: int = Query(5000, ge=1, le=50000),
):
    """
    Return history for one hive within a selected time range.
    """
    if ts_to < ts_from:
        raise HTTPException(status_code=400, detail="ts_to must be >= ts_from")

    points = get_history(
        hive_id=hive_id,
        ts_from=to_utc_iso(ts_from),
        ts_to=to_utc_iso(ts_to),
        limit=limit,
    )
    return {"hive_id": hive_id, "points": points}


@app.get("/api/latest", response_model=LatestResponse)
def latest(hive_id: int = Query(..., ge=0)):
    """
    Return the latest processed point for one hive.
    """
    point = get_latest_point(hive_id)
    return {"hive_id": hive_id, "point": point}


@app.get("/api/hives/{hive_id}/snapshot")
def hive_snapshot(hive_id: int):
    """
    Return a full snapshot for one hive.

    This combines latest point, status, and active alerts.
    """
    return build_snapshot_payload(hive_id)


@app.get("/api/hives/{hive_id}/status")
def hive_status(hive_id: int):
    """
    Return the current status for one hive.
    """
    hive_ids = set(list_hives())
    if hive_id not in hive_ids:
        raise HTTPException(status_code=404, detail="Hive not found")

    return build_status_payload(hive_id)


@app.get("/api/status")
def status_all():
    """
    Return the current status for all registered hives.
    """
    hive_ids = list_hives()
    items = [build_status_payload(h) for h in hive_ids]
    return {"items": items, "count": len(items)}


@app.get("/api/hives/{hive_id}/alerts", response_model=AlertsResponse)
def hive_alerts(
    hive_id: int,
    active_only: bool = Query(False),
    limit: int = Query(200, ge=1, le=5000),
    ts_from: datetime | None = Query(None),
    ts_to: datetime | None = Query(None),
):
    """
    Return alert records for one hive.

    Optional ts_from and ts_to allow filtering alerts by time range.
    """
    alerts = get_alerts(
        hive_id=hive_id,
        active_only=active_only,
        limit=limit,
        ts_from=to_utc_iso(ts_from) if ts_from else None,
        ts_to=to_utc_iso(ts_to) if ts_to else None,
    )

    return {"hive_id": hive_id, "alerts": alerts}


@app.get("/api/debug/hive_counts")
def hive_counts():
    """
    Debug endpoint that returns the number of stored measurements per hive.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT hive_id, COUNT(*) AS n
            FROM measurements
            GROUP BY hive_id
            ORDER BY n DESC;
            """
        )
        rows = cur.fetchall()

        return [{"hive_id": int(r["hive_id"]), "n": int(r["n"])} for r in rows]
    finally:
        conn.close()


@app.patch("/api/alerts/{alert_id}/resolve")
def resolve_alert(alert_id: int):
    """
    Mark an alert as resolved.

    This sets:
    - is_active = 0
    - resolved_at_utc = now
    """
    conn = get_conn()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            UPDATE alerts
            SET is_active = 0,
                resolved_at_utc = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ? AND is_active = 1;
            """,
            (alert_id,),
        )

        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Alert not found or already resolved")

        conn.commit()

        return {"status": "resolved", "alert_id": alert_id}

    finally:
        conn.close()


BASE_DIR = Path(__file__).resolve().parent.parent.parent
FRONTEND_DIST = BASE_DIR / "frontend" / "dist"

app.mount("/", StaticFiles(directory=str(FRONTEND_DIST), html=True), name="frontend")
