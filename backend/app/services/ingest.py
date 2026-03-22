from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, Optional

import numpy as np

from ..db import get_conn
from ..kf.hive_filter import HiveKalmanFilter
from ..schemas import (
    AlertInfo,
    MeasurementIn,
    NISInfo,
    Observation,
    TwinPoint,
)
from ..services.history import chi2_thr, get_latest_point

logger = logging.getLogger(__name__)

OBS_COLS = ["temperature", "humidity", "audio_density"]

ACCEPTED_STATUS = "accepted"
IGNORED_OUT_OF_ORDER_STATUS = "ignored_out_of_order"
IGNORED_INVALID_FILTER_OUTPUT_STATUS = "ignored_invalid_filter_output"

STATUS_HEALTHY = "healthy"
STATUS_WARNING = "warning"
STATUS_CRITICAL = "critical"

ALERT_MISSING_DATA = "missing_data"
ALERT_WARNING_ANOMALY = "warning_anomaly"
ALERT_CRITICAL_ANOMALY = "critical_anomaly"

MIN_DT_MIN = 1e-6
MAX_DT_MIN = 24.0 * 60.0 * 7.0


class HiveLockRegistry:
    """
    Keep one lock per hive.

    This allows measurements from different hives to be processed in parallel,
    while still protecting each individual hive from race conditions.
    This approach is suitable for a single-process student project backend.
    """

    def __init__(self) -> None:
        self._master_lock = threading.Lock()
        self._locks: Dict[int, threading.Lock] = {}

    @contextmanager
    def hold(self, hive_id: int):
        with self._master_lock:
            lock = self._locks.setdefault(int(hive_id), threading.Lock())

        lock.acquire()
        try:
            yield
        finally:
            lock.release()


class HiveStateRegistry:
    """
    Store one Kalman filter instance per hive.

    Each hive has its own independent filter state.
    The registry also stores the shared KF configuration and threshold values.
    """

    def __init__(self, kf_config: dict):
        params = kf_config["params"]

        self.cfg = {
            "USE_DT_AWARE_Q": params["USE_DT_AWARE_Q"],
            "BASE_DT_MIN": params["BASE_DT_MIN"],
            "ADAPT_R": params["ADAPT_R"],
            "ALPHA_R": params["ALPHA_R"],
            "R_MIN_MULT": params["R_MIN_MULT"],
            "R_MAX_MULT": params["R_MAX_MULT"],
            "GATE_NIS_NORM": params["GATE_NIS_NORM"],
            "S_JITTER": params["S_JITTER"],
            "MISSING_Q_MULT": params["MISSING_Q_MULT"],
            "MISSING_STREAK_MAX_MULT": params["MISSING_STREAK_MAX_MULT"],
            "P_DIAG_MIN": params["P_DIAG_MIN"],
            "P_DIAG_MAX": params["P_DIAG_MAX"],
        }

        self.Q_diag = np.array(kf_config["Q_best_diag"], dtype=float)
        self.R0_diag = np.array(kf_config["R0_diag"], dtype=float)
        self.P0_diag = np.array(kf_config["P0_diag"], dtype=float)

        self.nis_p95 = float(kf_config["nis_norm_p95"])
        self.nis_p99 = float(kf_config["nis_norm_p99"])

        self.filters: Dict[int, HiveKalmanFilter] = {}
        self._filters_lock = threading.Lock()
        self._hive_locks = HiveLockRegistry()

    def _build_filter(self) -> HiveKalmanFilter:
        """
        Create a new Kalman filter instance using the loaded configuration.
        """
        return HiveKalmanFilter(
            Q_diag=self.Q_diag,
            R0_diag=self.R0_diag,
            P0_diag=self.P0_diag,
            cfg=self.cfg,
        )

    def hive_lock(self, hive_id: int):
        """
        Return the lock context manager for one hive.
        """
        return self._hive_locks.hold(int(hive_id))

    def get_filter(self, hive_id: int, conn=None) -> HiveKalmanFilter:
        """
        Return the filter instance for a hive.

        If the filter does not exist in memory yet, create it.
        If runtime state is stored in the database, restore it.
        """
        hive_id = int(hive_id)

        with self._filters_lock:
            existing_filter = self.filters.get(hive_id)
            if existing_filter is not None:
                return existing_filter

            kf = self._build_filter()

            if conn is not None:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT
                        initialized,
                        x_temperature, x_humidity, x_audio_density,
                        p_00, p_01, p_02,
                        p_10, p_11, p_12,
                        p_20, p_21, p_22,
                        r_diag_temperature, r_diag_humidity, r_diag_audio_density,
                        missing_streak
                    FROM hive_runtime_state
                    WHERE hive_id = ?;
                    """,
                    (hive_id,),
                )
                row = cur.fetchone()

                if row is not None:
                    try:
                        x = np.array(
                            [
                                row["x_temperature"],
                                row["x_humidity"],
                                row["x_audio_density"],
                            ],
                            dtype=float,
                        )

                        P = np.array(
                            [
                                [row["p_00"], row["p_01"], row["p_02"]],
                                [row["p_10"], row["p_11"], row["p_12"]],
                                [row["p_20"], row["p_21"], row["p_22"]],
                            ],
                            dtype=float,
                        )

                        R_diag_t = np.array(
                            [
                                row["r_diag_temperature"],
                                row["r_diag_humidity"],
                                row["r_diag_audio_density"],
                            ],
                            dtype=float,
                        )

                        kf.load_state(
                            x=x,
                            P=P,
                            R_diag_t=R_diag_t,
                            missing_streak=int(row["missing_streak"] or 0),
                            initialized=bool(row["initialized"]),
                        )
                    except Exception:
                        logger.exception(
                            "Failed to restore runtime state for hive %s",
                            hive_id,
                        )

            self.filters[hive_id] = kf
            return kf


def bool_to_int(value: bool) -> int:
    """
    Convert a Python boolean to SQLite-friendly integer.
    """
    return 1 if bool(value) else 0


def normalize_ts(ts: datetime) -> tuple[datetime, str]:
    """
    Convert a datetime into:
    - timezone-aware UTC datetime
    - UTC ISO string ending with Z
    """
    ts_dt = ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
    ts_dt = ts_dt.astimezone(timezone.utc)
    ts_utc = ts_dt.isoformat().replace("+00:00", "Z")
    return ts_dt, ts_utc


def parse_utc_iso(ts: Optional[str]) -> Optional[datetime]:
    """
    Parse a UTC ISO string safely.
    """
    if not ts:
        return None

    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except Exception:
        return None


def finite_or_none(value) -> Optional[float]:
    """
    Return a float only if it is finite.
    Otherwise return None.
    """
    try:
        out = float(value)
    except Exception:
        return None

    return out if np.isfinite(out) else None


def has_any_observation(measurement: MeasurementIn) -> bool:
    """
    Check whether the measurement contains at least one valid sensor value.
    """
    values = [
        measurement.temperature,
        measurement.humidity,
        measurement.audio_density,
    ]
    return any(finite_or_none(value) is not None for value in values)


def measurement_to_vector(measurement: MeasurementIn) -> np.ndarray:
    """
    Convert the incoming measurement into a NumPy vector for the Kalman filter.

    Missing values are represented using NaN.
    """
    return np.array(
        [
            np.nan
            if finite_or_none(measurement.temperature) is None
            else float(measurement.temperature),
            np.nan
            if finite_or_none(measurement.humidity) is None
            else float(measurement.humidity),
            np.nan
            if finite_or_none(measurement.audio_density) is None
            else float(measurement.audio_density),
        ],
        dtype=float,
    )


def point_from_latest(latest: dict) -> TwinPoint:
    """
    Rebuild a TwinPoint from the latest stored API response structure.
    """
    return TwinPoint(
        hive_id=latest["hive_id"],
        ts=datetime.fromisoformat(str(latest["ts"]).replace("Z", "+00:00")),
        raw=Observation(**latest["raw"]),
        pred=Observation(**latest["pred"]),
        filt=Observation(**latest["filt"]),
        pred_std=Observation(**latest["pred_std"]),
        nis=NISInfo(**latest["nis"]),
        alerts=AlertInfo(**latest["alerts"]),
        adaptive_r=Observation(**latest["adaptive_r"]),
        has_observation=bool(latest.get("has_observation", True)),
    )


def empty_point(
    measurement: MeasurementIn,
    ts_dt: datetime,
    has_observation_flag: bool,
) -> TwinPoint:
    """
    Build a minimal fallback point when no processed result is available yet.
    """
    return TwinPoint(
        hive_id=int(measurement.hive_id),
        ts=ts_dt,
        raw=Observation(
            temperature=finite_or_none(measurement.temperature),
            humidity=finite_or_none(measurement.humidity),
            audio_density=finite_or_none(measurement.audio_density),
        ),
        pred=Observation(),
        filt=Observation(),
        pred_std=Observation(),
        nis=NISInfo(raw=None, norm=None, dof=0),
        alerts=AlertInfo(),
        adaptive_r=Observation(),
        has_observation=bool(has_observation_flag),
    )


def latest_or_fallback(
    measurement: MeasurementIn,
    ts_dt: datetime,
    has_observation_flag: bool,
) -> TwinPoint:
    """
    Return the latest stored twin point if available.
    Otherwise return an empty fallback point.
    """
    latest = get_latest_point(int(measurement.hive_id))
    if latest is not None:
        return point_from_latest(latest)

    return empty_point(
        measurement=measurement,
        ts_dt=ts_dt,
        has_observation_flag=has_observation_flag,
    )


def serialize_step_to_point(
    hive_id: int,
    ts_dt: datetime,
    measurement: MeasurementIn,
    filter_output,
    anomaly_p95: bool,
    anomaly_p99: bool,
    chi2_p95: bool,
    chi2_p99: bool,
    has_observation_flag: bool,
) -> TwinPoint:
    """
    Convert one Kalman filter step output into the API TwinPoint structure.
    """
    return TwinPoint(
        hive_id=int(hive_id),
        ts=ts_dt,
        raw=Observation(
            temperature=finite_or_none(measurement.temperature),
            humidity=finite_or_none(measurement.humidity),
            audio_density=finite_or_none(measurement.audio_density),
        ),
        pred=Observation(
            temperature=finite_or_none(filter_output.x_pred[0]),
            humidity=finite_or_none(filter_output.x_pred[1]),
            audio_density=finite_or_none(filter_output.x_pred[2]),
        ),
        filt=Observation(
            temperature=finite_or_none(filter_output.x_filt[0]),
            humidity=finite_or_none(filter_output.x_filt[1]),
            audio_density=finite_or_none(filter_output.x_filt[2]),
        ),
        pred_std=Observation(
            temperature=finite_or_none(filter_output.pred_std[0]),
            humidity=finite_or_none(filter_output.pred_std[1]),
            audio_density=finite_or_none(filter_output.pred_std[2]),
        ),
        nis=NISInfo(
            raw=None if filter_output.nis_raw is None else float(filter_output.nis_raw),
            norm=None
            if filter_output.nis_norm is None
            else float(filter_output.nis_norm),
            dof=int(filter_output.nis_dof),
        ),
        alerts=AlertInfo(
            anomaly_p95=bool(anomaly_p95),
            anomaly_p99=bool(anomaly_p99),
            chi2_p95=bool(chi2_p95),
            chi2_p99=bool(chi2_p99),
        ),
        adaptive_r=Observation(
            temperature=finite_or_none(filter_output.R_diag[0]),
            humidity=finite_or_none(filter_output.R_diag[1]),
            audio_density=finite_or_none(filter_output.R_diag[2]),
        ),
        has_observation=bool(has_observation_flag),
    )


def upsert_runtime_state(
    conn,
    hive_id: int,
    ts_utc: str,
    kf: HiveKalmanFilter,
) -> None:
    """
    Save the latest Kalman filter runtime state for a hive.

    This allows the backend to continue from the previous state on the next step.
    """
    state = kf.export_state()
    x = state["x"]
    P = state["P"]
    R = state["R_diag_t"]

    conn.execute(
        """
        INSERT INTO hive_runtime_state (
            hive_id, last_ts_utc, initialized,
            x_temperature, x_humidity, x_audio_density,
            p_00, p_01, p_02,
            p_10, p_11, p_12,
            p_20, p_21, p_22,
            r_diag_temperature, r_diag_humidity, r_diag_audio_density,
            missing_streak, updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        ON CONFLICT(hive_id) DO UPDATE SET
            last_ts_utc = excluded.last_ts_utc,
            initialized = excluded.initialized,
            x_temperature = excluded.x_temperature,
            x_humidity = excluded.x_humidity,
            x_audio_density = excluded.x_audio_density,
            p_00 = excluded.p_00,
            p_01 = excluded.p_01,
            p_02 = excluded.p_02,
            p_10 = excluded.p_10,
            p_11 = excluded.p_11,
            p_12 = excluded.p_12,
            p_20 = excluded.p_20,
            p_21 = excluded.p_21,
            p_22 = excluded.p_22,
            r_diag_temperature = excluded.r_diag_temperature,
            r_diag_humidity = excluded.r_diag_humidity,
            r_diag_audio_density = excluded.r_diag_audio_density,
            missing_streak = excluded.missing_streak,
            updated_at_utc = excluded.updated_at_utc;
        """,
        (
            int(hive_id),
            ts_utc,
            bool_to_int(bool(state["initialized"])),
            float(x[0]),
            float(x[1]),
            float(x[2]),
            float(P[0, 0]),
            float(P[0, 1]),
            float(P[0, 2]),
            float(P[1, 0]),
            float(P[1, 1]),
            float(P[1, 2]),
            float(P[2, 0]),
            float(P[2, 1]),
            float(P[2, 2]),
            float(R[0]),
            float(R[1]),
            float(R[2]),
            int(state["missing_streak"]),
        ),
    )


def set_hive_status(
    conn,
    hive_id: int,
    ts_utc: str,
    status: str,
    reason: str,
    alert_count: int,
) -> None:
    """
    Update the current dashboard status for one hive.
    """
    conn.execute(
        """
        INSERT INTO hive_status (
            hive_id, status, status_reason, last_ts_utc, alert_count, updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        ON CONFLICT(hive_id) DO UPDATE SET
            status = excluded.status,
            status_reason = excluded.status_reason,
            last_ts_utc = excluded.last_ts_utc,
            alert_count = excluded.alert_count,
            updated_at_utc = excluded.updated_at_utc;
        """,
        (int(hive_id), status, reason, ts_utc, int(alert_count)),
    )


def resolve_active_alerts(conn, hive_id: int, alert_type: str, ts_utc: str) -> None:
    """
    Mark active alerts of a given type as resolved.
    """
    conn.execute(
        """
        UPDATE alerts
        SET is_active = 0,
            resolved_at_utc = COALESCE(resolved_at_utc, ?)
        WHERE hive_id = ?
          AND alert_type = ?
          AND is_active = 1;
        """,
        (ts_utc, int(hive_id), alert_type),
    )


def open_alert_if_needed(
    conn,
    hive_id: int,
    measurement_id: int,
    ts_utc: str,
    alert_type: str,
    severity: str,
    title: str,
    message: str,
) -> None:
    """
    Open a new alert only if there is not already an active alert of the same type.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM alerts
        WHERE hive_id = ?
          AND alert_type = ?
          AND is_active = 1
        ORDER BY id DESC
        LIMIT 1;
        """,
        (int(hive_id), alert_type),
    )
    existing = cur.fetchone()

    if existing is None:
        conn.execute(
            """
            INSERT INTO alerts (
                hive_id, measurement_id, ts_utc,
                alert_type, severity, title, message,
                is_active, is_acknowledged
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, 0);
            """,
            (
                int(hive_id),
                int(measurement_id),
                ts_utc,
                alert_type,
                severity,
                title,
                message,
            ),
        )


def update_alerts_and_status(
    conn,
    hive_id: int,
    measurement_id: int,
    ts_utc: str,
    has_observation_flag: bool,
    anomaly_p95: bool,
    anomaly_p99: bool,
    chi2_p95: bool,
    chi2_p99: bool,
) -> None:
    """
    Update alert lifecycle and derive the current hive status.

    Priority:
    critical anomaly > warning anomaly > missing data > healthy
    """
    active_types: list[str] = []

    if not has_observation_flag:
        open_alert_if_needed(
            conn,
            hive_id,
            measurement_id,
            ts_utc,
            ALERT_MISSING_DATA,
            "warning",
            "Missing observation",
            "Latest accepted measurement arrived without any valid sensor observation.",
        )
        active_types.append(ALERT_MISSING_DATA)
    else:
        resolve_active_alerts(conn, hive_id, ALERT_MISSING_DATA, ts_utc)

    if anomaly_p99 or chi2_p99:
        open_alert_if_needed(
            conn,
            hive_id,
            measurement_id,
            ts_utc,
            ALERT_CRITICAL_ANOMALY,
            "critical",
            "Critical anomaly detected",
            "Latest accepted reading exceeded the critical anomaly threshold.",
        )
        active_types.append(ALERT_CRITICAL_ANOMALY)
    else:
        resolve_active_alerts(conn, hive_id, ALERT_CRITICAL_ANOMALY, ts_utc)

    if (anomaly_p95 or chi2_p95) and not (anomaly_p99 or chi2_p99):
        open_alert_if_needed(
            conn,
            hive_id,
            measurement_id,
            ts_utc,
            ALERT_WARNING_ANOMALY,
            "warning",
            "Warning anomaly detected",
            "Latest accepted reading exceeded the warning anomaly threshold.",
        )
        active_types.append(ALERT_WARNING_ANOMALY)
    else:
        resolve_active_alerts(conn, hive_id, ALERT_WARNING_ANOMALY, ts_utc)

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
    alert_count = int(cur.fetchone()["n"])

    if ALERT_CRITICAL_ANOMALY in active_types:
        status = STATUS_CRITICAL
        reason = "Critical anomaly detected on latest accepted reading"
    elif ALERT_WARNING_ANOMALY in active_types:
        status = STATUS_WARNING
        reason = "Warning anomaly detected on latest accepted reading"
    elif ALERT_MISSING_DATA in active_types:
        status = STATUS_WARNING
        reason = "Latest accepted reading contains missing observations"
    else:
        status = STATUS_HEALTHY
        reason = "Latest accepted readings within expected range"

    set_hive_status(conn, hive_id, ts_utc, status, reason, alert_count)


def measurement_exists(conn, hive_id: int, ts_utc: str) -> bool:
    """
    Check whether a measurement with the same hive and timestamp already exists.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM measurements
        WHERE hive_id = ?
          AND ts_utc = ?
        LIMIT 1;
        """,
        (int(hive_id), ts_utc),
    )
    return cur.fetchone() is not None


def get_last_accepted_ts_for_hive(conn, hive_id: int) -> Optional[str]:
    """
    Return the timestamp of the latest accepted measurement for a hive.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT MAX(ts_utc) AS last_ts
        FROM measurements
        WHERE hive_id = ?
          AND ingestion_status = ?;
        """,
        (int(hive_id), ACCEPTED_STATUS),
    )
    row = cur.fetchone()

    if row is None:
        return None

    return row["last_ts"]


def compute_dt_prev_min(
    conn,
    hive_id: int,
    ts_dt: datetime,
    input_dt_prev_min: Optional[float],
) -> Optional[float]:
    """
    Compute the effective time gap in minutes.

    Priority:
    1. Calculate from the latest accepted timestamp already in the database
    2. Fall back to the incoming dt_prev_min value
    """
    last_ts_utc = get_last_accepted_ts_for_hive(conn, hive_id)
    last_ts_dt = parse_utc_iso(last_ts_utc)

    if last_ts_dt is not None:
        delta_min = (ts_dt - last_ts_dt).total_seconds() / 60.0
        if not np.isfinite(delta_min) or delta_min <= 0:
            return None
        return float(np.clip(delta_min, MIN_DT_MIN, MAX_DT_MIN))

    input_dt = finite_or_none(input_dt_prev_min)
    if input_dt is None or input_dt <= 0:
        return None

    return float(np.clip(input_dt, MIN_DT_MIN, MAX_DT_MIN))


def is_invalid_filter_output(filter_output) -> bool:
    """
    Check whether the Kalman filter output contains invalid numeric values.
    """
    valid_r = np.isfinite(filter_output.R_diag).all()
    valid_p = np.isfinite(filter_output.P_pred).all() or np.isnan(
        filter_output.P_pred
    ).all()
    valid_x_pred = np.isfinite(filter_output.x_pred).all() or np.isnan(
        filter_output.x_pred
    ).all()
    valid_x_filt = np.isfinite(filter_output.x_filt).all() or np.isnan(
        filter_output.x_filt
    ).all()
    valid_pred_std = np.isfinite(filter_output.pred_std).all() or np.isnan(
        filter_output.pred_std
    ).all()

    return not (valid_r and valid_p and valid_x_pred and valid_x_filt and valid_pred_std)


def insert_measurement(
    conn,
    measurement: MeasurementIn,
    ts_utc: str,
    has_observation_flag: bool,
    dt_prev_min: Optional[float],
    ingestion_status: str,
) -> int:
    """
    Insert the raw measurement row and return its database ID.
    """
    cur = conn.cursor()

    cur.execute(
        "INSERT OR IGNORE INTO hives (hive_id) VALUES (?);",
        (int(measurement.hive_id),),
    )

    cur.execute(
        """
        INSERT INTO measurements (
            hive_id, ts_utc, temperature, humidity, audio_density,
            dt_prev_min, has_observation, ingestion_status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(measurement.hive_id),
            ts_utc,
            finite_or_none(measurement.temperature),
            finite_or_none(measurement.humidity),
            finite_or_none(measurement.audio_density),
            None if dt_prev_min is None else float(dt_prev_min),
            bool_to_int(has_observation_flag),
            ingestion_status,
        ),
    )

    return int(cur.lastrowid)


def process_one_measurement(
    conn,
    registry: HiveStateRegistry,
    measurement: MeasurementIn,
) -> TwinPoint:
    """
    Process one measurement through the full ingestion pipeline.
    """
    ts_dt, ts_utc = normalize_ts(measurement.ts)
    hive_id = int(measurement.hive_id)
    has_observation_flag = has_any_observation(measurement)

    if measurement_exists(conn, hive_id, ts_utc):
        logger.info(
            "Duplicate measurement ignored for hive=%s ts=%s",
            hive_id,
            ts_utc,
        )
        return latest_or_fallback(
            measurement,
            ts_dt,
            has_observation_flag=has_observation_flag,
        )

    previous_ts_utc = get_last_accepted_ts_for_hive(conn, hive_id)
    if previous_ts_utc is not None and ts_utc <= previous_ts_utc:
        insert_measurement(
            conn=conn,
            measurement=measurement,
            ts_utc=ts_utc,
            has_observation_flag=has_observation_flag,
            dt_prev_min=None,
            ingestion_status=IGNORED_OUT_OF_ORDER_STATUS,
        )
        logger.warning(
            "Out-of-order measurement ignored for hive=%s ts=%s prev=%s",
            hive_id,
            ts_utc,
            previous_ts_utc,
        )
        return latest_or_fallback(
            measurement,
            ts_dt,
            has_observation_flag=has_observation_flag,
        )

    effective_dt_prev_min = compute_dt_prev_min(
        conn=conn,
        hive_id=hive_id,
        ts_dt=ts_dt,
        input_dt_prev_min=measurement.dt_prev_min,
    )

    measurement_id = insert_measurement(
        conn=conn,
        measurement=measurement,
        ts_utc=ts_utc,
        has_observation_flag=has_observation_flag,
        dt_prev_min=effective_dt_prev_min,
        ingestion_status=ACCEPTED_STATUS,
    )

    kf = registry.get_filter(hive_id, conn=conn)
    filter_output = kf.step(
        z=measurement_to_vector(measurement),
        dt_prev_min=effective_dt_prev_min,
    )

    if is_invalid_filter_output(filter_output):
        conn.execute(
            """
            UPDATE measurements
            SET ingestion_status = ?
            WHERE id = ?;
            """,
            (IGNORED_INVALID_FILTER_OUTPUT_STATUS, int(measurement_id)),
        )
        logger.error(
            "Invalid filter output for hive=%s measurement_id=%s",
            hive_id,
            measurement_id,
        )
        return latest_or_fallback(
            measurement,
            ts_dt,
            has_observation_flag=has_observation_flag,
        )

    dof = int(filter_output.nis_dof)
    nis_raw = filter_output.nis_raw
    nis_norm = filter_output.nis_norm

    anomaly_p95 = (
        dof > 0
        and nis_norm is not None
        and np.isfinite(nis_norm)
        and float(nis_norm) > registry.nis_p95
    )
    anomaly_p99 = (
        dof > 0
        and nis_norm is not None
        and np.isfinite(nis_norm)
        and float(nis_norm) > registry.nis_p99
    )

    chi2_p95 = False
    chi2_p99 = False

    if dof > 0 and nis_raw is not None and np.isfinite(nis_raw):
        threshold_95 = chi2_thr(0.95, dof)
        threshold_99 = chi2_thr(0.99, dof)
        chi2_p95 = np.isfinite(threshold_95) and float(nis_raw) > float(threshold_95)
        chi2_p99 = np.isfinite(threshold_99) and float(nis_raw) > float(threshold_99)

    conn.execute(
        """
        INSERT OR REPLACE INTO kf_steps (
            measurement_id,
            x_pred_temperature, x_pred_humidity, x_pred_audio_density,
            x_filt_temperature, x_filt_humidity, x_filt_audio_density,
            pred_std_temperature, pred_std_humidity, pred_std_audio_density,
            nis_raw, nis_norm, nis_dof,
            anomaly_p95, anomaly_p99, chi2_p95, chi2_p99,
            r_diag_temperature, r_diag_humidity, r_diag_audio_density
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(measurement_id),
            finite_or_none(filter_output.x_pred[0]),
            finite_or_none(filter_output.x_pred[1]),
            finite_or_none(filter_output.x_pred[2]),
            finite_or_none(filter_output.x_filt[0]),
            finite_or_none(filter_output.x_filt[1]),
            finite_or_none(filter_output.x_filt[2]),
            finite_or_none(filter_output.pred_std[0]),
            finite_or_none(filter_output.pred_std[1]),
            finite_or_none(filter_output.pred_std[2]),
            None if nis_raw is None else float(nis_raw),
            None if nis_norm is None else float(nis_norm),
            int(dof),
            bool_to_int(anomaly_p95),
            bool_to_int(anomaly_p99),
            bool_to_int(chi2_p95),
            bool_to_int(chi2_p99),
            finite_or_none(filter_output.R_diag[0]),
            finite_or_none(filter_output.R_diag[1]),
            finite_or_none(filter_output.R_diag[2]),
        ),
    )

    upsert_runtime_state(conn, hive_id, ts_utc, kf)

    update_alerts_and_status(
        conn=conn,
        hive_id=hive_id,
        measurement_id=int(measurement_id),
        ts_utc=ts_utc,
        has_observation_flag=has_observation_flag,
        anomaly_p95=bool(anomaly_p95),
        anomaly_p99=bool(anomaly_p99),
        chi2_p95=bool(chi2_p95),
        chi2_p99=bool(chi2_p99),
    )

    return serialize_step_to_point(
        hive_id=hive_id,
        ts_dt=ts_dt,
        measurement=measurement,
        filter_output=filter_output,
        anomaly_p95=bool(anomaly_p95),
        anomaly_p99=bool(anomaly_p99),
        chi2_p95=bool(chi2_p95),
        chi2_p99=bool(chi2_p99),
        has_observation_flag=has_observation_flag,
    )


def ingest_measurement(registry: HiveStateRegistry, measurement: MeasurementIn) -> TwinPoint:
    """
    Public entry point for ingesting a single measurement.
    """
    conn = get_conn()
    try:
        with registry.hive_lock(int(measurement.hive_id)):
            with conn:
                return process_one_measurement(conn, registry, measurement)
    finally:
        conn.close()


def ingest_measurements_batch(
    registry: HiveStateRegistry,
    items: list[MeasurementIn],
) -> list[TwinPoint]:
    """
    Public entry point for batch ingestion.

    Measurements are sorted by hive and timestamp so that each hive is processed
    in a stable chronological order.
    """
    if not items:
        return []

    items_sorted = sorted(
        items,
        key=lambda item: (int(item.hive_id), normalize_ts(item.ts)[1]),
    )

    conn = get_conn()
    current_lock = None
    current_lock_hive_id: Optional[int] = None

    try:
        output_points: list[TwinPoint] = []

        with conn:
            for measurement in items_sorted:
                hive_id = int(measurement.hive_id)

                if current_lock_hive_id != hive_id:
                    if current_lock is not None:
                        current_lock.__exit__(None, None, None)

                    current_lock = registry.hive_lock(hive_id)
                    current_lock.__enter__()
                    current_lock_hive_id = hive_id

                output_points.append(
                    process_one_measurement(conn, registry, measurement)
                )

        return output_points

    finally:
        if current_lock is not None:
            try:
                current_lock.__exit__(None, None, None)
            except Exception:
                pass
        conn.close()