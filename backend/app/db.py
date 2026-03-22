from __future__ import annotations

import sqlite3

from .config import DB_PATH


def get_conn() -> sqlite3.Connection:
    """
    Create and return a SQLite connection.

    The connection uses:
    - Row factory for dictionary-like access
    - Foreign keys for relational integrity
    - Busy timeout to reduce locking errors
    - Memory-backed temporary storage for better performance
    - Larger cache size for faster repeated queries
    """
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-20000;")

    return conn


def init_db() -> None:
    """
    Initialize the SQLite database schema.

    This function creates all required tables and indexes if they do not already exist.
    It is safe to run on every backend startup.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()

        # Write-Ahead Logging improves concurrent read and write behavior.
        # NORMAL synchronous mode gives a good balance between safety and speed
        # for this real-time monitoring prototype.
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")

        create_hives_table(cur)
        create_measurements_table(cur)
        create_kf_steps_table(cur)
        create_runtime_state_table(cur)
        create_hive_status_table(cur)
        create_alerts_table(cur)
        create_indexes(cur)

        conn.commit()

    finally:
        conn.close()


def create_hives_table(cur: sqlite3.Cursor) -> None:
    """
    Store the list of registered hive IDs.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS hives (
            hive_id INTEGER PRIMARY KEY
        );
        """
    )


def create_measurements_table(cur: sqlite3.Cursor) -> None:
    """
    Store raw incoming measurements after ingestion.

    This table keeps the original sensor values and ingestion metadata.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hive_id INTEGER NOT NULL,
            ts_utc TEXT NOT NULL,
            temperature REAL,
            humidity REAL,
            audio_density REAL,
            dt_prev_min REAL,
            has_observation INTEGER NOT NULL DEFAULT 0,
            ingestion_status TEXT NOT NULL DEFAULT 'accepted',
            created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            FOREIGN KEY(hive_id) REFERENCES hives(hive_id) ON DELETE CASCADE
        );
        """
    )


def create_kf_steps_table(cur: sqlite3.Cursor) -> None:
    """
    Store Kalman filter results for each measurement.

    This separates the raw sensor input from the filter output so that
    both can be inspected later for analysis and debugging.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS kf_steps (
            measurement_id INTEGER PRIMARY KEY,

            x_pred_temperature REAL,
            x_pred_humidity REAL,
            x_pred_audio_density REAL,

            x_filt_temperature REAL,
            x_filt_humidity REAL,
            x_filt_audio_density REAL,

            pred_std_temperature REAL,
            pred_std_humidity REAL,
            pred_std_audio_density REAL,

            nis_raw REAL,
            nis_norm REAL,
            nis_dof INTEGER NOT NULL DEFAULT 0,

            anomaly_p95 INTEGER NOT NULL DEFAULT 0,
            anomaly_p99 INTEGER NOT NULL DEFAULT 0,
            chi2_p95 INTEGER NOT NULL DEFAULT 0,
            chi2_p99 INTEGER NOT NULL DEFAULT 0,

            r_diag_temperature REAL,
            r_diag_humidity REAL,
            r_diag_audio_density REAL,

            FOREIGN KEY(measurement_id) REFERENCES measurements(id) ON DELETE CASCADE
        );
        """
    )


def create_runtime_state_table(cur: sqlite3.Cursor) -> None:
    """
    Store the persistent runtime state of the Kalman filter for each hive.

    This allows the backend to continue filtering from the latest known state
    instead of starting from scratch every time.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS hive_runtime_state (
            hive_id INTEGER PRIMARY KEY,
            last_ts_utc TEXT,
            initialized INTEGER NOT NULL DEFAULT 0,

            x_temperature REAL,
            x_humidity REAL,
            x_audio_density REAL,

            p_00 REAL, p_01 REAL, p_02 REAL,
            p_10 REAL, p_11 REAL, p_12 REAL,
            p_20 REAL, p_21 REAL, p_22 REAL,

            r_diag_temperature REAL,
            r_diag_humidity REAL,
            r_diag_audio_density REAL,

            missing_streak INTEGER NOT NULL DEFAULT 0,
            updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),

            FOREIGN KEY(hive_id) REFERENCES hives(hive_id) ON DELETE CASCADE
        );
        """
    )


def create_hive_status_table(cur: sqlite3.Cursor) -> None:
    """
    Store the latest dashboard-ready status for each hive.

    This avoids recalculating the status repeatedly for every UI request.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS hive_status (
            hive_id INTEGER PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'no_data',
            status_reason TEXT,
            last_ts_utc TEXT,
            alert_count INTEGER NOT NULL DEFAULT 0,
            updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            FOREIGN KEY(hive_id) REFERENCES hives(hive_id) ON DELETE CASCADE
        );
        """
    )


def create_alerts_table(cur: sqlite3.Cursor) -> None:
    """
    Store anomaly and alert records for each hive.

    Alerts can stay active until resolved, which supports dashboard monitoring.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hive_id INTEGER NOT NULL,
            measurement_id INTEGER,
            ts_utc TEXT NOT NULL,
            alert_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            title TEXT NOT NULL,
            message TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            is_acknowledged INTEGER NOT NULL DEFAULT 0,
            opened_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            resolved_at_utc TEXT,
            FOREIGN KEY(hive_id) REFERENCES hives(hive_id) ON DELETE CASCADE,
            FOREIGN KEY(measurement_id) REFERENCES measurements(id) ON DELETE SET NULL
        );
        """
    )


def create_indexes(cur: sqlite3.Cursor) -> None:
    """
    Create indexes that support the main backend query patterns.
    """
    # Prevent duplicate readings for the same hive and timestamp
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_measurements_hive_ts
        ON measurements(hive_id, ts_utc);
        """
    )

    # Support history lookups by hive and time
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_measurements_hive_ts
        ON measurements(hive_id, ts_utc);
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_measurements_ts
        ON measurements(ts_utc);
        """
    )

    # Support accepted-only measurement queries
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_measurements_hive_status_ts
        ON measurements(hive_id, ingestion_status, ts_utc DESC);
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_measurements_status_ts
        ON measurements(ingestion_status, ts_utc DESC);
        """
    )

    # Support anomaly lookup from Kalman filter outputs
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_kf_steps_alerts
        ON kf_steps(anomaly_p99, chi2_p99);
        """
    )

    # Support recent alert lookups and active alert queries
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_alerts_hive_ts
        ON alerts(hive_id, ts_utc DESC);
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_alerts_active
        ON alerts(hive_id, is_active, severity, ts_utc DESC);
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_alerts_measurement
        ON alerts(measurement_id);
        """
    )

    # Support dashboard status listing
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_hive_status_status
        ON hive_status(status, updated_at_utc DESC);
        """
    )