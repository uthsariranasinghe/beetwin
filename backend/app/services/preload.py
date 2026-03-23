from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from ..db import get_conn
from ..schemas import MeasurementIn
from ..services.ingest import HiveStateRegistry, ingest_measurements_batch


OBS_COLS = ["temperature", "humidity", "audio_density"]


def finite_or_none(value) -> Optional[float]:
    """
    Convert a value to float only if it is finite.
    Otherwise return None.
    """
    try:
        out = float(value)
    except Exception:
        return None

    return out if np.isfinite(out) else None


def has_any_observation(row) -> bool:
    """
    Return True if the row contains at least one valid sensor value.
    """
    for col in OBS_COLS:
        if hasattr(row, col) and finite_or_none(getattr(row, col)) is not None:
            return True
    return False


def row_has_any_observation_series(row: pd.Series) -> bool:
    """
    Return True if a pandas row contains at least one valid sensor value.
    """
    for col in OBS_COLS:
        if col in row and finite_or_none(row[col]) is not None:
            return True
    return False


def hive_group_has_any_valid_observation(group: pd.DataFrame) -> bool:
    """
    Keep only hives that have at least one valid observation somewhere
    in their historical data.

    This prevents preloading hives whose entire timeline is missing-only,
    which would create many timestamps but no usable chart values.
    """
    for col in OBS_COLS:
        values = pd.to_numeric(group[col], errors="coerce")
        if values.notna().any():
            return True
    return False


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize the historical dataframe.

    Steps:
    - parse timestamps
    - convert hive IDs to integers
    - convert observation columns to numeric
    - drop rows with invalid timestamp or hive ID
    - sort by hive and time
    """
    df = df.copy()

    df["published_at"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce")
    df["tag_number"] = pd.to_numeric(df["tag_number"], errors="coerce").astype("Int64")

    for col in OBS_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["published_at", "tag_number"])

    return df.sort_values(["tag_number", "published_at"]).reset_index(drop=True)


def db_has_measurements_for_hives(hive_ids: Optional[list[int]]) -> bool:
    """
    Check whether the database already contains measurements for the selected hives.

    This avoids skipping preload just because unrelated hive data already exists.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()

        if hive_ids:
            placeholders = ",".join(["?"] * len(hive_ids))
            cur.execute(
                f"""
                SELECT 1
                FROM measurements
                WHERE hive_id IN ({placeholders})
                LIMIT 1;
                """,
                tuple(int(hive_id) for hive_id in hive_ids),
            )
        else:
            cur.execute("SELECT 1 FROM measurements LIMIT 1;")

        return cur.fetchone() is not None

    finally:
        conn.close()


def register_hives_from_df(df: pd.DataFrame) -> list[int]:
    """
    Register all hive IDs found in the dataframe.
    """
    hive_ids = sorted(df["tag_number"].dropna().astype(int).unique().tolist())

    conn = get_conn()
    try:
        with conn:
            for hive_id in hive_ids:
                conn.execute(
                    "INSERT OR IGNORE INTO hives (hive_id) VALUES (?);",
                    (int(hive_id),),
                )
    finally:
        conn.close()

    return hive_ids


def build_measurement(row) -> MeasurementIn:
    """
    Convert one dataframe row into the MeasurementIn schema used by ingestion.
    """
    return MeasurementIn(
        hive_id=int(row.tag_number),
        ts=row.published_at.to_pydatetime(),
        temperature=finite_or_none(getattr(row, "temperature", None)),
        humidity=finite_or_none(getattr(row, "humidity", None)),
        audio_density=finite_or_none(getattr(row, "audio_density", None)),
        dt_prev_min=None,
    )


def preload_history_if_needed(
    reg: HiveStateRegistry,
    preload_path: Path,
    batch_size: int = 500,
    include_missing_rows: bool = False,
    hive_ids: Optional[list[int]] = None,
    end_buffer_minutes: int = 30,
) -> None:
    """
    Preload historical parquet data into the backend database.

    Main behavior:
    - preload runs only if matching hive data is not already present
    - original historical timestamps are preserved
    - data is replayed through the normal ingestion pipeline
    - optional filtering can restrict preload to selected hive IDs

    Missing-data behavior:
    - hives with no valid observations at all are removed completely
    - if include_missing_rows is False, rows with no valid observations are skipped
    - if include_missing_rows is True, missing rows are kept only for hives that
      have at least one real observation somewhere in their timeline
    """
    _ = end_buffer_minutes  # kept only so existing config calls do not break

    if db_has_measurements_for_hives(hive_ids):
        print("[preload] Measurements already exist for selected hives. Skipping preload.")
        return

    if not preload_path.exists():
        print(f"[preload] Preload file not found: {preload_path}. Skipping preload.")
        return

    print(f"[preload] Loading historical data from: {preload_path}")

    df = pd.read_parquet(preload_path)
    df = clean_df(df)

    if hive_ids:
        selected_hives = {int(hive_id) for hive_id in hive_ids}
        df = df[df["tag_number"].astype(int).isin(selected_hives)].copy()

    if df.empty:
        print("[preload] No valid rows found after cleaning and filtering. Skipping preload.")
        return

    original_start = df["published_at"].min()
    original_end = df["published_at"].max()

    # Remove hives whose entire history is missing-only.
    df = (
        df.groupby("tag_number", group_keys=False)
        .filter(hive_group_has_any_valid_observation)
        .reset_index(drop=True)
    )

    if df.empty:
        print("[preload] All candidate hives contained only missing rows. Skipping preload.")
        return

    original_hives_after_validation = sorted(
        df["tag_number"].dropna().astype(int).unique().tolist()
    )
    print(f"[preload] Hives with at least one valid observation: {original_hives_after_validation}")

    registered_hive_ids = register_hives_from_df(df)

    print(f"[preload] Registered hives: {registered_hive_ids}")
    print(f"[preload] Final range: {original_start} -> {original_end}")

    total_rows_after_filtering = len(df)
    total_missing_rows = int((~df.apply(row_has_any_observation_series, axis=1)).sum())
    total_non_missing_rows = int(total_rows_after_filtering - total_missing_rows)

    print(f"[preload] Rows after hive validation: {total_rows_after_filtering}")
    print(f"[preload] Rows with valid observations: {total_non_missing_rows}")
    print(f"[preload] Rows with missing-only observations: {total_missing_rows}")

    batch_items: list[MeasurementIn] = []
    inserted_total = 0
    skipped_missing_total = 0

    for row in df.itertuples(index=False):
        if (not include_missing_rows) and (not has_any_observation(row)):
            skipped_missing_total += 1
            continue

        batch_items.append(build_measurement(row))

        if len(batch_items) >= batch_size:
            ingest_measurements_batch(reg, batch_items)
            inserted_total += len(batch_items)
            print(f"[preload] Inserted {inserted_total} records...")
            batch_items.clear()

    if batch_items:
        ingest_measurements_batch(reg, batch_items)
        inserted_total += len(batch_items)

    print(f"[preload] Done. Total inserted records: {inserted_total}")
    if not include_missing_rows:
        print(f"[preload] Skipped missing-only rows: {skipped_missing_total}")
