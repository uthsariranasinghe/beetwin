from __future__ import annotations
import os
import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

BASE_API = os.getenv("https://believable-flexibility-production-77c5.up.railway.app/")

API_BATCH = f"{BASE_API}/api/measurements/batch"
API_REGISTER = f"{BASE_API}/api/hives/register"

BASE_DIR = Path(__file__).resolve().parent
SPLIT_DIR = BASE_DIR / "data" / "splits_70_15_15"
DEFAULT_STREAM_PATH = SPLIT_DIR / "test.parquet"

OBS_COLS = ["temperature", "humidity", "audio_density"]

SELECTED_DASHBOARD_HIVES = [
    202039, 202040, 202043, 202045,
    202048, 202049, 202051, 202053,
    202056, 202060,
]


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize the input dataframe before simulation.
    """
    df = df.copy()

    df["published_at"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce")
    df["tag_number"] = pd.to_numeric(df["tag_number"], errors="coerce").astype("Int64")

    for col in OBS_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["published_at", "tag_number"])

    return df.sort_values(["tag_number", "published_at"]).reset_index(drop=True)


def to_iso_z(ts: pd.Timestamp | datetime) -> str:
    """
    Convert a pandas or Python datetime into UTC ISO format ending with Z.
    """
    ts = pd.Timestamp(ts)

    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")

    return ts.to_pydatetime().isoformat().replace("+00:00", "Z")


def finite_or_none(value) -> Optional[float]:
    """
    Return a float if the value is finite, otherwise return None.
    """
    try:
        out = float(value)
    except Exception:
        return None

    return out if np.isfinite(out) else None


def has_any_observation(row: pd.Series | dict) -> bool:
    """
    Check whether at least one observation column contains a valid value.
    """
    for col in OBS_COLS:
        value = row[col] if isinstance(row, dict) else row.get(col)
        if finite_or_none(value) is not None:
            return True
    return False


def register_hives(hives: list[int], api_register: str = API_REGISTER) -> None:
    """
    Register hive IDs with the backend before streaming measurements.
    """
    payload = {"hive_ids": [int(hive_id) for hive_id in hives]}

    for attempt in range(1, 6):
        try:
            response = requests.post(api_register, json=payload, timeout=60)
            response.raise_for_status()
            return
        except requests.RequestException as e:
            print(f"[register_hives] attempt {attempt}/5 failed: {e}")
            if attempt < 5:
                time.sleep(5)
            else:
                raise


def post_batch(batch: list[dict], api_batch: str = API_BATCH) -> None:
    """
    Send one batch of measurements to the backend.

    Retry instead of crashing when the backend is temporarily slow.
    """
    if not batch:
        return

    max_retries = 5
    retry_delay_seconds = 5
    timeout_seconds = 120

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                api_batch,
                json={"items": batch},
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            return
        except requests.RequestException as e:
            print(f"[post_batch] attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                print(f"[post_batch] retrying in {retry_delay_seconds} seconds...")
                time.sleep(retry_delay_seconds)
            else:
                print("[post_batch] max retries reached. Skipping this batch and continuing.")


def choose_hives(
    df: pd.DataFrame,
    max_hives: int,
    hive_ids: Optional[list[int]],
) -> list[int]:
    """
    Select the hives that will be streamed.

    Priority:
    - explicitly requested hive IDs
    - otherwise the predefined dashboard hive list
    """
    available = sorted(df["tag_number"].dropna().astype(int).unique())
    available_set = set(available)

    if hive_ids:
        selected = [int(hive_id) for hive_id in hive_ids if int(hive_id) in available_set]
    else:
        default_selected = [
            int(hive_id)
            for hive_id in SELECTED_DASHBOARD_HIVES
            if int(hive_id) in available_set
        ]
        selected = default_selected[:max_hives] if max_hives > 0 else default_selected

    return sorted(selected)


def regularize_hive_to_interval(
    hive_df: pd.DataFrame,
    interval_minutes: int,
    aggregate_mode: str = "last",
) -> pd.DataFrame:
    """
    Convert one hive's irregular historical data into a regular time series.

    One row is produced per fixed interval.

    aggregate_mode:
    - "last": keep the last value in each interval
    - "mean": average values within each interval
    """
    hive_df = hive_df.copy().sort_values("published_at").reset_index(drop=True)
    hive_df = hive_df.set_index("published_at")

    rule = f"{int(interval_minutes)}min"

    if aggregate_mode == "mean":
        regular = hive_df[OBS_COLS].resample(rule).mean()
    else:
        regular = hive_df[OBS_COLS].resample(rule).last()

    regular["tag_number"] = int(hive_df["tag_number"].iloc[0])
    regular = regular.reset_index()

    return regular[["published_at", "tag_number", *OBS_COLS]]


def build_regular_streams(
    df: pd.DataFrame,
    hives: list[int],
    interval_minutes: int,
    aggregate_mode: str,
) -> dict[int, pd.DataFrame]:
    """
    Build one regularized stream per hive.
    """
    streams: dict[int, pd.DataFrame] = {}

    for hive_id in hives:
        hive_df = df[df["tag_number"].astype(int) == int(hive_id)].copy()
        if len(hive_df) == 0:
            continue

        regular = regularize_hive_to_interval(
            hive_df=hive_df,
            interval_minutes=interval_minutes,
            aggregate_mode=aggregate_mode,
        )

        if len(regular) > 0:
            streams[int(hive_id)] = regular.reset_index(drop=True)

    return streams


def advance_to_first_valid_row(
    hive_df: pd.DataFrame,
    start_idx: int,
) -> Optional[int]:
    """
    Find the first row with at least one valid observation.
    """
    idx = int(start_idx)

    while idx < len(hive_df):
        row = hive_df.loc[idx]
        if has_any_observation(row):
            return idx
        idx += 1

    return None


def build_live_timestamp(
    slot_index: int,
    live_anchor_ts: pd.Timestamp,
    interval_minutes: int,
) -> pd.Timestamp:
    """
    Build a simulated live timestamp.

    Each emitted row is separated by exactly interval_minutes in simulated time.
    """
    return live_anchor_ts + pd.Timedelta(minutes=slot_index * interval_minutes)


def maybe_add_noise(
    value: Optional[float],
    std: float,
    rng: np.random.Generator,
) -> Optional[float]:
    """
    Add Gaussian noise to a value.
    """
    if value is None:
        return None

    noisy = float(value) + float(rng.normal(0.0, std))
    return noisy if np.isfinite(noisy) else None


def maybe_add_spike(
    value: Optional[float],
    probability: float,
    magnitude: float,
    rng: np.random.Generator,
) -> Optional[float]:
    """
    Randomly inject a spike into a value.
    """
    if value is None:
        return None

    if probability <= 0:
        return value

    if float(rng.random()) < float(probability):
        sign = 1.0 if float(rng.random()) < 0.5 else -1.0
        return float(value) + sign * float(magnitude)

    return value


def maybe_drop_value(
    value: Optional[float],
    probability: float,
    rng: np.random.Generator,
) -> Optional[float]:
    """
    Randomly drop a value to simulate missing data.
    """
    if value is None:
        return None

    if probability <= 0:
        return value

    if float(rng.random()) < float(probability):
        return None

    return value


def ensure_not_fully_missing(
    original: dict,
    corrupted: dict,
    rng: np.random.Generator,
) -> dict:
    """
    Ensure the outgoing payload still has at least one observed value.
    """
    if has_any_observation(corrupted):
        return corrupted

    available_cols = [
        col for col in OBS_COLS if finite_or_none(original.get(col)) is not None
    ]

    if not available_cols:
        return corrupted

    chosen_col = available_cols[int(rng.integers(0, len(available_cols)))]
    corrupted = corrupted.copy()
    corrupted[chosen_col] = finite_or_none(original.get(chosen_col))
    return corrupted


def apply_demo_corruption(
    row: pd.Series,
    rng: np.random.Generator,
    noise_enabled: bool,
    temp_noise_std: float,
    hum_noise_std: float,
    audio_noise_std: float,
    spike_prob: float,
    temp_spike_mag: float,
    hum_spike_mag: float,
    audio_spike_mag: float,
    missing_prob: float,
    allow_fully_missing_payloads: bool,
) -> dict:
    """
    Apply optional demo corruption to one row.
    """
    original = {
        "temperature": finite_or_none(row.get("temperature")),
        "humidity": finite_or_none(row.get("humidity")),
        "audio_density": finite_or_none(row.get("audio_density")),
    }

    corrupted = original.copy()

    if noise_enabled:
        corrupted["temperature"] = maybe_add_noise(
            corrupted["temperature"], temp_noise_std, rng
        )
        corrupted["humidity"] = maybe_add_noise(
            corrupted["humidity"], hum_noise_std, rng
        )
        corrupted["audio_density"] = maybe_add_noise(
            corrupted["audio_density"], audio_noise_std, rng
        )

        corrupted["temperature"] = maybe_add_spike(
            corrupted["temperature"], spike_prob, temp_spike_mag, rng
        )
        corrupted["humidity"] = maybe_add_spike(
            corrupted["humidity"], spike_prob, hum_spike_mag, rng
        )
        corrupted["audio_density"] = maybe_add_spike(
            corrupted["audio_density"], spike_prob, audio_spike_mag, rng
        )

        corrupted["temperature"] = maybe_drop_value(
            corrupted["temperature"], missing_prob, rng
        )
        corrupted["humidity"] = maybe_drop_value(
            corrupted["humidity"], missing_prob, rng
        )
        corrupted["audio_density"] = maybe_drop_value(
            corrupted["audio_density"], missing_prob, rng
        )

        if not allow_fully_missing_payloads:
            corrupted = ensure_not_fully_missing(original, corrupted, rng)

    return corrupted


def payload_from_row(
    hive_id: int,
    row: pd.Series,
    slot_index: int,
    live_anchor_ts: pd.Timestamp,
    interval_minutes: int,
    rng: np.random.Generator,
    noise_enabled: bool,
    temp_noise_std: float,
    hum_noise_std: float,
    audio_noise_std: float,
    spike_prob: float,
    temp_spike_mag: float,
    hum_spike_mag: float,
    audio_spike_mag: float,
    missing_prob: float,
    allow_fully_missing_payloads: bool,
) -> dict:
    """
    Convert one regularized row into the API payload format.
    """
    live_ts = build_live_timestamp(
        slot_index=slot_index,
        live_anchor_ts=live_anchor_ts,
        interval_minutes=interval_minutes,
    )

    obs = apply_demo_corruption(
        row=row,
        rng=rng,
        noise_enabled=noise_enabled,
        temp_noise_std=temp_noise_std,
        hum_noise_std=hum_noise_std,
        audio_noise_std=audio_noise_std,
        spike_prob=spike_prob,
        temp_spike_mag=temp_spike_mag,
        hum_spike_mag=hum_spike_mag,
        audio_spike_mag=audio_spike_mag,
        missing_prob=missing_prob,
        allow_fully_missing_payloads=allow_fully_missing_payloads,
    )

    return {
        "hive_id": int(hive_id),
        "ts": to_iso_z(live_ts),
        "temperature": obs["temperature"],
        "humidity": obs["humidity"],
        "audio_density": obs["audio_density"],
    }


def find_first_valid_indices(streams: dict[int, pd.DataFrame], hives: list[int]) -> dict[int, int]:
    """
    Find the first valid row index for each hive.
    """
    result: dict[int, int] = {}

    for hive_id in hives:
        hive_df = streams[hive_id]
        idx = advance_to_first_valid_row(hive_df, 0)
        if idx is not None:
            result[hive_id] = idx

    return result


def build_startup_batch(
    streams: dict[int, pd.DataFrame],
    hives: list[int],
    first_valid_idx_map: dict[int, int],
    current_idx: dict[int, int],
    live_anchor: dict[int, pd.Timestamp],
    interval_minutes: int,
    rng: np.random.Generator,
    startup_first_valid_only: bool,
    include_missing_rows: bool,
    noise_enabled: bool,
    temp_noise_std: float,
    hum_noise_std: float,
    audio_noise_std: float,
    spike_prob: float,
    temp_spike_mag: float,
    hum_spike_mag: float,
    audio_spike_mag: float,
    missing_prob: float,
    allow_fully_missing_payloads: bool,
) -> list[dict]:
    """
    Build one startup batch for the current run or replay cycle.
    """
    startup_batch: list[dict] = []

    for hive_id in hives:
        hive_df = streams[hive_id]

        if startup_first_valid_only:
            first_idx = first_valid_idx_map[hive_id]
            row = hive_df.loc[first_idx]
            current_idx[hive_id] = first_idx + 1

            startup_batch.append(
                payload_from_row(
                    hive_id=hive_id,
                    row=row,
                    slot_index=0,
                    live_anchor_ts=live_anchor[hive_id],
                    interval_minutes=interval_minutes,
                    rng=rng,
                    noise_enabled=noise_enabled,
                    temp_noise_std=temp_noise_std,
                    hum_noise_std=hum_noise_std,
                    audio_noise_std=audio_noise_std,
                    spike_prob=spike_prob,
                    temp_spike_mag=temp_spike_mag,
                    hum_spike_mag=hum_spike_mag,
                    audio_spike_mag=audio_spike_mag,
                    missing_prob=missing_prob,
                    allow_fully_missing_payloads=allow_fully_missing_payloads,
                )
            )
        else:
            row = hive_df.loc[0]
            current_idx[hive_id] = 1

            if include_missing_rows or has_any_observation(row):
                startup_batch.append(
                    payload_from_row(
                        hive_id=hive_id,
                        row=row,
                        slot_index=0,
                        live_anchor_ts=live_anchor[hive_id],
                        interval_minutes=interval_minutes,
                        rng=rng,
                        noise_enabled=noise_enabled,
                        temp_noise_std=temp_noise_std,
                        hum_noise_std=hum_noise_std,
                        audio_noise_std=audio_noise_std,
                        spike_prob=spike_prob,
                        temp_spike_mag=temp_spike_mag,
                        hum_spike_mag=hum_spike_mag,
                        audio_spike_mag=audio_spike_mag,
                        missing_prob=missing_prob,
                        allow_fully_missing_payloads=allow_fully_missing_payloads,
                    )
                )

    return startup_batch


def run(
    cycle_seconds: float = 900.0,
    interval_minutes: int = 15,
    max_hives: int = 15,
    batch_size: int = 200,
    stream_path: Path = DEFAULT_STREAM_PATH,
    hive_ids: Optional[list[int]] = None,
    include_missing_rows: bool = True,
    startup_first_valid_only: bool = True,
    api_batch: str = API_BATCH,
    api_register: str = API_REGISTER,
    aggregate_mode: str = "last",
    noise_enabled: bool = False,
    temp_noise_std: float = 1.2,
    hum_noise_std: float = 2.5,
    audio_noise_std: float = 0.03,
    spike_prob: float = 0.02,
    temp_spike_mag: float = 8.0,
    hum_spike_mag: float = 12.0,
    audio_spike_mag: float = 0.12,
    missing_prob: float = 0.0,
    seed: int = 42,
    allow_fully_missing_payloads: bool = False,
) -> None:
    """
    Run the fixed-interval multi-hive simulator forever.

    Main behavior:
    - reads historical parquet data
    - regularizes it into fixed intervals
    - emits exactly one row per hive per cycle
    - maps old timestamps into a fresh live-looking timeline
    - optionally injects demo noise, spikes, and missingness
    - when the dataset ends, it restarts from the beginning and keeps going
    """
    if cycle_seconds <= 0:
        raise ValueError("cycle_seconds must be > 0")
    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be > 0")
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if aggregate_mode not in {"last", "mean"}:
        raise ValueError("aggregate_mode must be 'last' or 'mean'")
    if temp_noise_std < 0 or hum_noise_std < 0 or audio_noise_std < 0:
        raise ValueError("noise standard deviations must be >= 0")
    if not (0.0 <= spike_prob <= 1.0):
        raise ValueError("spike_prob must be in [0, 1]")
    if not (0.0 <= missing_prob <= 1.0):
        raise ValueError("missing_prob must be in [0, 1]")

    rng = np.random.default_rng(seed)

    df = clean_df(pd.read_parquet(stream_path))
    hives = choose_hives(df, max_hives=max_hives, hive_ids=hive_ids)

    if not hives:
        raise RuntimeError("No matching hives found in dataset.")

    streams = build_regular_streams(
        df=df,
        hives=hives,
        interval_minutes=interval_minutes,
        aggregate_mode=aggregate_mode,
    )

    hives = [hive_id for hive_id in hives if hive_id in streams]

    if not hives:
        raise RuntimeError("Selected hives have no rows after regularization.")

    first_valid_idx_map = find_first_valid_indices(streams, hives)

    if startup_first_valid_only:
        hives = [hive_id for hive_id in hives if hive_id in first_valid_idx_map]

    if not hives:
        raise RuntimeError("No selected hives contain any valid startup row.")

    print(f"Streaming from: {stream_path}")
    print(f"Streaming hives: {hives}")
    print(
        f"Config -> cycle_seconds={cycle_seconds}, "
        f"interval_minutes={interval_minutes}, "
        f"batch_size={batch_size}, "
        f"include_missing_rows={include_missing_rows}, "
        f"aggregate_mode={aggregate_mode}, "
        f"allow_fully_missing_payloads={allow_fully_missing_payloads}"
    )
    print("This simulator emits exactly one row per hive per cycle.")
    print(f"Each cycle represents one simulated {interval_minutes}-minute sensor interval.")
    print("This version never ends. When the dataset finishes, it restarts automatically.")
    print("Noise mode enabled:", noise_enabled)

    if noise_enabled:
        print(
            "Demo corruption -> "
            f"temp_noise_std={temp_noise_std}, "
            f"hum_noise_std={hum_noise_std}, "
            f"audio_noise_std={audio_noise_std}, "
            f"spike_prob={spike_prob}, "
            f"temp_spike_mag={temp_spike_mag}, "
            f"hum_spike_mag={hum_spike_mag}, "
            f"audio_spike_mag={audio_spike_mag}, "
            f"missing_prob={missing_prob}, "
            f"seed={seed}"
        )

    register_hives(hives, api_register=api_register)

    current_idx = {hive_id: 0 for hive_id in hives}
    live_anchor_base = pd.Timestamp.now(tz="UTC").floor("min")
    live_anchor = {hive_id: live_anchor_base for hive_id in hives}

    startup_batch = build_startup_batch(
        streams=streams,
        hives=hives,
        first_valid_idx_map=first_valid_idx_map,
        current_idx=current_idx,
        live_anchor=live_anchor,
        interval_minutes=interval_minutes,
        rng=rng,
        startup_first_valid_only=startup_first_valid_only,
        include_missing_rows=include_missing_rows,
        noise_enabled=noise_enabled,
        temp_noise_std=temp_noise_std,
        hum_noise_std=hum_noise_std,
        audio_noise_std=audio_noise_std,
        spike_prob=spike_prob,
        temp_spike_mag=temp_spike_mag,
        hum_spike_mag=hum_spike_mag,
        audio_spike_mag=audio_spike_mag,
        missing_prob=missing_prob,
        allow_fully_missing_payloads=allow_fully_missing_payloads,
    )

    if startup_batch:
        post_batch(startup_batch, api_batch=api_batch)

    cycle_index = 1

    while True:
        batch: list[dict] = []
        active_hives = 0

        for hive_id in hives:
            hive_df = streams[hive_id]

            if current_idx[hive_id] >= len(hive_df):
                continue

            active_hives += 1

            row = hive_df.loc[current_idx[hive_id]]
            current_idx[hive_id] += 1

            if (not include_missing_rows) and (not has_any_observation(row)):
                continue

            batch.append(
                payload_from_row(
                    hive_id=hive_id,
                    row=row,
                    slot_index=cycle_index,
                    live_anchor_ts=live_anchor[hive_id],
                    interval_minutes=interval_minutes,
                    rng=rng,
                    noise_enabled=noise_enabled,
                    temp_noise_std=temp_noise_std,
                    hum_noise_std=hum_noise_std,
                    audio_noise_std=audio_noise_std,
                    spike_prob=spike_prob,
                    temp_spike_mag=temp_spike_mag,
                    hum_spike_mag=hum_spike_mag,
                    audio_spike_mag=audio_spike_mag,
                    missing_prob=missing_prob,
                    allow_fully_missing_payloads=allow_fully_missing_payloads,
                )
            )

            if len(batch) >= batch_size:
                post_batch(batch, api_batch=api_batch)
                batch.clear()

        if batch:
            post_batch(batch, api_batch=api_batch)

        if active_hives == 0:
            print("Reached end of dataset. Restarting from beginning...")

            # Keep timeline continuous: the next cycle should be exactly one interval later.
            next_cycle_start_ts = build_live_timestamp(
                slot_index=cycle_index,
                live_anchor_ts=live_anchor_base,
                interval_minutes=interval_minutes,
            )

            current_idx = {hive_id: 0 for hive_id in hives}
            live_anchor_base = next_cycle_start_ts
            live_anchor = {hive_id: live_anchor_base for hive_id in hives}

            startup_batch = build_startup_batch(
                streams=streams,
                hives=hives,
                first_valid_idx_map=first_valid_idx_map,
                current_idx=current_idx,
                live_anchor=live_anchor,
                interval_minutes=interval_minutes,
                rng=rng,
                startup_first_valid_only=startup_first_valid_only,
                include_missing_rows=include_missing_rows,
                noise_enabled=noise_enabled,
                temp_noise_std=temp_noise_std,
                hum_noise_std=hum_noise_std,
                audio_noise_std=audio_noise_std,
                spike_prob=spike_prob,
                temp_spike_mag=temp_spike_mag,
                hum_spike_mag=hum_spike_mag,
                audio_spike_mag=audio_spike_mag,
                missing_prob=missing_prob,
                allow_fully_missing_payloads=allow_fully_missing_payloads,
            )

            if startup_batch:
                post_batch(startup_batch, api_batch=api_batch)

            cycle_index = 1
            time.sleep(cycle_seconds)
            continue

        cycle_index += 1
        time.sleep(cycle_seconds)


def main() -> None:
    """
    Command-line entry point.
    """
    parser = argparse.ArgumentParser(
        description="Fixed-interval multi-hive live simulator for the Beehive Digital Twin backend."
    )

    parser.add_argument(
        "--cycle-seconds",
        type=float,
        default=2.0,
        help="Real seconds between emission cycles.",
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=15,
        help="Simulated sensor interval in minutes.",
    )
    parser.add_argument(
        "--max-hives",
        type=int,
        default=15,
        help="Maximum number of hives to stream if hive IDs are not specified.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Batch size for POST requests.",
    )
    parser.add_argument(
        "--stream-path",
        type=str,
        default=str(DEFAULT_STREAM_PATH),
        help="Path to the parquet file used for streaming.",
    )
    parser.add_argument(
        "--hive-ids",
        type=str,
        default="",
        help="Comma-separated hive IDs to stream.",
    )
    parser.add_argument(
        "--skip-missing-rows",
        action="store_true",
        help="Skip regularized intervals where all observation columns are missing.",
    )
    parser.add_argument(
        "--send-first-row",
        action="store_true",
        help="Send the first chronological row immediately, even if it is missing.",
    )
    parser.add_argument(
        "--aggregate-mode",
        type=str,
        default="last",
        choices=["last", "mean"],
        help="How to regularize raw data into each fixed interval.",
    )
    parser.add_argument(
        "--api-batch",
        type=str,
        default=API_BATCH,
        help="Batch ingestion endpoint.",
    )
    parser.add_argument(
        "--api-register",
        type=str,
        default=API_REGISTER,
        help="Hive registration endpoint.",
    )
    parser.add_argument(
        "--enable-demo-noise",
        action="store_true",
        help="Add synthetic noise, spikes, and missingness on top of replayed data.",
    )
    parser.add_argument(
        "--temp-noise-std",
        type=float,
        default=1.2,
        help="Gaussian noise standard deviation for temperature.",
    )
    parser.add_argument(
        "--hum-noise-std",
        type=float,
        default=2.5,
        help="Gaussian noise standard deviation for humidity.",
    )
    parser.add_argument(
        "--audio-noise-std",
        type=float,
        default=0.03,
        help="Gaussian noise standard deviation for audio_density.",
    )
    parser.add_argument(
        "--spike-prob",
        type=float,
        default=0.02,
        help="Probability of injecting a spike into each observed variable.",
    )
    parser.add_argument(
        "--temp-spike-mag",
        type=float,
        default=8.0,
        help="Spike magnitude for temperature.",
    )
    parser.add_argument(
        "--hum-spike-mag",
        type=float,
        default=12.0,
        help="Spike magnitude for humidity.",
    )
    parser.add_argument(
        "--audio-spike-mag",
        type=float,
        default=0.12,
        help="Spike magnitude for audio_density.",
    )
    parser.add_argument(
        "--missing-prob",
        type=float,
        default=0.0,
        help="Probability of dropping an observed value after regularization.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible demo corruption.",
    )
    parser.add_argument(
        "--allow-fully-missing-payloads",
        action="store_true",
        help="Allow synthetic corruption to turn a row into a fully missing payload.",
    )

    args = parser.parse_args()

    hive_ids = None
    if args.hive_ids.strip():
        hive_ids = [int(part.strip()) for part in args.hive_ids.split(",") if part.strip()]

    run(
        cycle_seconds=args.cycle_seconds,
        interval_minutes=args.interval_minutes,
        max_hives=args.max_hives,
        batch_size=args.batch_size,
        stream_path=Path(args.stream_path),
        hive_ids=hive_ids,
        include_missing_rows=not args.skip_missing_rows,
        startup_first_valid_only=not args.send_first_row,
        api_batch=args.api_batch,
        api_register=args.api_register,
        aggregate_mode=args.aggregate_mode,
        noise_enabled=args.enable_demo_noise,
        temp_noise_std=args.temp_noise_std,
        hum_noise_std=args.hum_noise_std,
        audio_noise_std=args.audio_noise_std,
        spike_prob=args.spike_prob,
        temp_spike_mag=args.temp_spike_mag,
        hum_spike_mag=args.hum_spike_mag,
        audio_spike_mag=args.audio_spike_mag,
        missing_prob=args.missing_prob,
        seed=args.seed,
        allow_fully_missing_payloads=args.allow_fully_missing_payloads,
    )


if __name__ == "__main__":
    main()
