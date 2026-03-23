from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import numpy as np

from ..db import get_conn


ACCEPTED_INGESTION_STATUS = "accepted"
DEFAULT_STALE_AFTER_MINUTES = 60.0


def _chi2_ppf_wilson_hilferty(p: float, k: int) -> float:
    """
    Approximate the chi-square inverse CDF.

    This uses:
    - Acklam approximation for inverse normal CDF
    - Wilson–Hilferty transform for chi-square approximation

    It avoids requiring SciPy just for threshold lookup.
    """

    def inv_norm_cdf(prob: float) -> float:
        a = [
            -3.969683028665376e01,
            2.209460984245205e02,
            -2.759285104469687e02,
            1.383577518672690e02,
            -3.066479806614716e01,
            2.506628277459239e00,
        ]
        b = [
            -5.447609879822406e01,
            1.615858368580409e02,
            -1.556989798598866e02,
            6.680131188771972e01,
            -1.328068155288572e01,
        ]
        c = [
            -7.784894002430293e-03,
            -3.223964580411365e-01,
            -2.400758277161838e00,
            -2.549732539343734e00,
            4.374664141464968e00,
            2.938163982698783e00,
        ]
        d = [
            7.784695709041462e-03,
            3.224671290700398e-01,
            2.445134137142996e00,
            3.754408661907416e00,
        ]

        plow = 0.02425
        phigh = 1.0 - plow

        if prob < plow:
            q = np.sqrt(-2.0 * np.log(prob))
            return (
                (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
                / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
            )

        if prob > phigh:
            q = np.sqrt(-2.0 * np.log(1.0 - prob))
            return -(
                (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
                / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
            )

        q = prob - 0.5
        r = q * q
        return (
            (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
            / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
        )

    z = float(inv_norm_cdf(p))
    k = max(int(k), 1)
    return float(k * (1.0 - 2.0 / (9.0 * k) + z * np.sqrt(2.0 / (9.0 * k))) ** 3)


def chi2_thr(p: float, dof: int) -> float:
    """
    Return the approximate chi-square threshold for probability p and degrees of freedom.
    """
    if dof <= 0:
        return float("nan")
    return _chi2_ppf_wilson_hilferty(p, dof)


def list_hives() -> list[int]:
    """
    Return all registered hive IDs.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT hive_id FROM hives ORDER BY hive_id ASC;")
        return [int(row["hive_id"]) for row in cur.fetchall()]
    finally:
        conn.close()


def now_utc() -> datetime:
    """
    Return the current UTC time.
    """
    return datetime.now(timezone.utc)


def parse_utc_iso(ts: Optional[str]) -> Optional[datetime]:
    """
    Parse a UTC ISO timestamp safely.
    """
    if not ts:
        return None

    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except Exception:
        return None


def row_to_point(row) -> dict:
    """
    Convert one joined measurement + filter row into the API point structure.
    """
    return {
        "hive_id": int(row["hive_id"]),
        "ts": row["ts_utc"],
        "raw": {
            "temperature": row["temperature"],
            "humidity": row["humidity"],
            "audio_density": row["audio_density"],
        },
        "pred": {
            "temperature": row["x_pred_temperature"],
            "humidity": row["x_pred_humidity"],
            "audio_density": row["x_pred_audio_density"],
        },
        "filt": {
            "temperature": row["x_filt_temperature"],
            "humidity": row["x_filt_humidity"],
            "audio_density": row["x_filt_audio_density"],
        },
        "pred_std": {
            "temperature": row["pred_std_temperature"],
            "humidity": row["pred_std_humidity"],
            "audio_density": row["pred_std_audio_density"],
        },
        "nis": {
            "raw": row["nis_raw"],
            "norm": row["nis_norm"],
            "dof": int(row["nis_dof"] or 0),
        },
        "alerts": {
            "anomaly_p95": bool(row["anomaly_p95"] or 0),
            "anomaly_p99": bool(row["anomaly_p99"] or 0),
            "chi2_p95": bool(row["chi2_p95"] or 0),
            "chi2_p99": bool(row["chi2_p99"] or 0),
        },
        "adaptive_r": {
            "temperature": row["r_diag_temperature"],
            "humidity": row["r_diag_humidity"],
            "audio_density": row["r_diag_audio_density"],
        },
        "has_observation": bool(row["has_observation"] or 0),
        "ingestion_status": row["ingestion_status"],
    }


def base_point_select_sql() -> str:
    """
    Base SQL used to read measurements together with their Kalman filter output.
    """
    return """
        SELECT
            m.id AS measurement_id,
            m.hive_id,
            m.ts_utc,
            m.temperature,
            m.humidity,
            m.audio_density,
            m.dt_prev_min,
            m.has_observation,
            m.ingestion_status,

            k.x_pred_temperature,
            k.x_pred_humidity,
            k.x_pred_audio_density,

            k.x_filt_temperature,
            k.x_filt_humidity,
            k.x_filt_audio_density,

            k.pred_std_temperature,
            k.pred_std_humidity,
            k.pred_std_audio_density,

            k.nis_raw,
            k.nis_norm,
            k.nis_dof,

            k.anomaly_p95,
            k.anomaly_p99,
            k.chi2_p95,
            k.chi2_p99,

            k.r_diag_temperature,
            k.r_diag_humidity,
            k.r_diag_audio_density
        FROM measurements m
        LEFT JOIN kf_steps k
            ON k.measurement_id = m.id
    """


def accepted_filter_sql(alias: str = "m") -> str:
    """
    Reusable SQL fragment for accepted-only filtering.
    """
    return f"{alias}.ingestion_status = ?"


def count_active_alerts(conn, hive_id: int) -> int:
    """
    Count active alerts for one hive.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*) AS n
        FROM alerts
        WHERE hive_id = ?
          AND is_active = 1;
        """,
        (int(hive_id),),
    )
    row = cur.fetchone()
    return int(row["n"] or 0) if row is not None else 0


def get_history(hive_id: int, ts_from: str, ts_to: str, limit: int = 5000) -> list[dict]:
    """
    Return accepted points in the selected time range for one hive.

    LEFT JOIN is used so accepted rows with missing observations still appear.
    """
    limit = int(max(1, min(limit, 50000)))

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            base_point_select_sql()
            + f"""
            WHERE m.hive_id = ?
              AND {accepted_filter_sql("m")}
              AND m.ts_utc >= ?
              AND m.ts_utc <= ?
            ORDER BY m.ts_utc ASC
            LIMIT ?;
            """,
            (int(hive_id), ACCEPTED_INGESTION_STATUS, ts_from, ts_to, limit),
        )
        rows = cur.fetchall()
        return [row_to_point(row) for row in rows]
    finally:
        conn.close()


def get_recent_history(hive_id: int, limit: int = 500) -> list[dict]:
    """
    Return the most recent accepted points for one hive in ascending time order.
    """
    limit = int(max(1, min(limit, 50000)))

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            base_point_select_sql()
            + f"""
            WHERE m.hive_id = ?
              AND {accepted_filter_sql("m")}
            ORDER BY m.ts_utc DESC
            LIMIT ?;
            """,
            (int(hive_id), ACCEPTED_INGESTION_STATUS, limit),
        )
        rows = cur.fetchall()
        rows = list(rows)[::-1]
        return [row_to_point(row) for row in rows]
    finally:
        conn.close()


def get_latest_point(hive_id: int) -> Optional[dict]:
    """
    Return the most recent accepted point for one hive.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            base_point_select_sql()
            + f"""
            WHERE m.hive_id = ?
              AND {accepted_filter_sql("m")}
            ORDER BY m.ts_utc DESC
            LIMIT 1;
            """,
            (int(hive_id), ACCEPTED_INGESTION_STATUS),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return row_to_point(row)
    finally:
        conn.close()


def get_latest_accepted_point(hive_id: int) -> Optional[dict]:
    """
    Explicit alias for callers that want accepted-only latest point.
    """
    return get_latest_point(hive_id)


def derive_status_from_latest(
    latest_point: Optional[dict],
    stale_after_minutes: float = DEFAULT_STALE_AFTER_MINUTES,
) -> dict:
    """
    Derive a fallback hive status from the latest accepted point.

    Priority:
    offline > critical > warning > healthy
    """
    if latest_point is None:
        return {
            "status": "no_data",
            "status_reason": "No accepted measurements available",
            "last_ts": None,
        }

    ts = parse_utc_iso(latest_point.get("ts"))
    if ts is None:
        return {
            "status": "no_data",
            "status_reason": "Latest accepted timestamp is invalid",
            "last_ts": latest_point.get("ts"),
        }

    age_min = (now_utc() - ts).total_seconds() / 60.0

    alerts = latest_point.get("alerts", {}) or {}
    has_observation = bool(latest_point.get("has_observation", True))

    critical = bool(alerts.get("anomaly_p99")) or bool(alerts.get("chi2_p99"))
    warning = bool(alerts.get("anomaly_p95")) or bool(alerts.get("chi2_p95"))
    missing_observation = not has_observation


    if critical:
        return {
            "status": "critical",
            "status_reason": "Critical anomaly detected on latest accepted reading",
            "last_ts": latest_point.get("ts"),
        }

    if warning or missing_observation:
        reason = (
            "Latest accepted reading contains missing observations"
            if missing_observation and not warning
            else "Warning anomaly detected on latest accepted reading"
        )
        return {
            "status": "warning",
            "status_reason": reason,
            "last_ts": latest_point.get("ts"),
        }

    return {
        "status": "healthy",
        "status_reason": "Latest accepted readings within expected range",
        "last_ts": latest_point.get("ts"),
    }


def infer_runtime_status_from_latest(
    latest_point: Optional[dict],
    stale_after_minutes: float = DEFAULT_STALE_AFTER_MINUTES,
) -> dict:
    """
    Backward-compatible wrapper using the older function name.
    """
    return derive_status_from_latest(
        latest_point=latest_point,
        stale_after_minutes=stale_after_minutes,
    )


def get_hive_status(
    hive_id: int,
    stale_after_minutes: float = DEFAULT_STALE_AFTER_MINUTES,
) -> Optional[dict]:
    """
    Return the canonical hive status.

    Rules:
    - if there is no accepted latest point, return no_data
    - if latest data is stale, return offline
    - otherwise prefer the stored status table
    - alert count always comes from the live alert table
    """
    latest_point = get_latest_point(hive_id)
    derived = derive_status_from_latest(
        latest_point,
        stale_after_minutes=stale_after_minutes,
    )

    if latest_point is None:
        conn = get_conn()
        try:
            return {
                "hive_id": int(hive_id),
                "status": derived["status"],
                "status_reason": derived["status_reason"],
                "last_ts": derived["last_ts"],
                "alert_count": count_active_alerts(conn, hive_id),
            }
        finally:
            conn.close()

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT hive_id, status, status_reason, last_ts_utc, alert_count
            FROM hive_status
            WHERE hive_id = ?;
            """,
            (int(hive_id),),
        )
        row = cur.fetchone()

        real_alert_count = count_active_alerts(conn, hive_id)

        if row is None:
            return {
                "hive_id": int(hive_id),
                "status": derived["status"],
                "status_reason": derived["status_reason"],
                "last_ts": derived["last_ts"],
                "alert_count": real_alert_count,
            }

        if derived["status"] in {"offline", "no_data"}:
            return {
                "hive_id": int(hive_id),
                "status": derived["status"],
                "status_reason": derived["status_reason"],
                "last_ts": derived["last_ts"],
                "alert_count": real_alert_count,
            }

        stored_status = row["status"]
        valid_stored_statuses = {"healthy", "warning", "critical"}

        return {
            "hive_id": int(row["hive_id"]),
            "status": stored_status if stored_status in valid_stored_statuses else derived["status"],
            "status_reason": row["status_reason"] or derived["status_reason"],
            "last_ts": latest_point.get("ts"),
            "alert_count": real_alert_count,
        }
    finally:
        conn.close()


def list_hive_statuses(
    stale_after_minutes: float = DEFAULT_STALE_AFTER_MINUTES,
) -> list[dict]:
    """
    Return canonical status for all known hives.
    """
    return [
        get_hive_status(hive_id, stale_after_minutes=stale_after_minutes)
        for hive_id in list_hives()
    ]


def get_alerts(
    hive_id: int,
    active_only: bool = False,
    limit: int = 200,
    ts_from: Optional[str] = None,
    ts_to: Optional[str] = None,
) -> list[dict]:
    """
    Return alert records for one hive.

    Optional ts_from and ts_to allow filtering alerts by time range.
    """
    limit = int(max(1, min(limit, 5000)))

    conn = get_conn()
    try:
        cur = conn.cursor()

        sql = """
            SELECT
                id, hive_id, ts_utc, alert_type, severity,
                title, message, is_active, is_acknowledged
            FROM alerts
            WHERE hive_id = ?
        """
        params: list = [int(hive_id)]

        if active_only:
            sql += " AND is_active = 1"

        if ts_from is not None:
            sql += " AND ts_utc >= ?"
            params.append(ts_from)

        if ts_to is not None:
            sql += " AND ts_utc <= ?"
            params.append(ts_to)

        sql += """
            ORDER BY ts_utc DESC
            LIMIT ?;
        """
        params.append(limit)

        cur.execute(sql, tuple(params))
        rows = cur.fetchall()

        return [
            {
                "id": int(row["id"]),
                "hive_id": int(row["hive_id"]),
                "ts": row["ts_utc"],
                "alert_type": row["alert_type"],
                "severity": row["severity"],
                "title": row["title"],
                "message": row["message"],
                "is_active": bool(row["is_active"]),
                "is_acknowledged": bool(row["is_acknowledged"]),
            }
            for row in rows
        ]
    finally:
        conn.close()


def list_latest_points(limit_hives: Optional[int] = None) -> list[dict]:
    """
    Return the latest accepted point for each hive.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()

        sql = """
            WITH latest_measurement AS (
                SELECT hive_id, MAX(ts_utc) AS max_ts
                FROM measurements
                WHERE ingestion_status = ?
                GROUP BY hive_id
            )
        """ + base_point_select_sql() + """
            JOIN latest_measurement lm
              ON lm.hive_id = m.hive_id
             AND lm.max_ts = m.ts_utc
            WHERE m.ingestion_status = ?
            ORDER BY m.hive_id ASC
        """

        params: tuple = (
            ACCEPTED_INGESTION_STATUS,
            ACCEPTED_INGESTION_STATUS,
        )

        if limit_hives is not None:
            sql += " LIMIT ?"
            params = (
                ACCEPTED_INGESTION_STATUS,
                ACCEPTED_INGESTION_STATUS,
                int(max(1, limit_hives)),
            )

        sql += ";"
        cur.execute(sql, params)

        rows = cur.fetchall()
        return [row_to_point(row) for row in rows]
    finally:
        conn.close()
