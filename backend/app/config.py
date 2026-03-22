from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


# Base project directory for the backend package
BASE_DIR = Path(__file__).resolve().parents[1]

# Data directory used for database files, config files, and preload data
DATA_DIR = Path(os.getenv("BEEHIVE_DATA_DIR", BASE_DIR / "data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

# SQLite database path
DB_PATH = Path(os.getenv("BEEHIVE_DB_PATH", DATA_DIR / "beehive_twin.db")).resolve()

# Kalman filter configuration file path
KF_CONFIG_PATH = Path(
    os.getenv("BEEHIVE_KF_CONFIG", DATA_DIR / "kf_config.json")
).resolve()


# Startup preload settings for historical parquet data
PRELOAD_ON_STARTUP = os.getenv("BEEHIVE_PRELOAD_ON_STARTUP", "true").lower() == "true"

PRELOAD_HISTORY_PATH = Path(
    os.getenv(
        "BEEHIVE_PRELOAD_HISTORY_PATH",
        DATA_DIR / "splits_70_15_15" / "val.parquet",
    )
).resolve()

PRELOAD_BATCH_SIZE = int(os.getenv("BEEHIVE_PRELOAD_BATCH_SIZE", "500"))

PRELOAD_END_BUFFER_MINUTES = int(
    os.getenv("BEEHIVE_PRELOAD_END_BUFFER_MINUTES", "30")
)


# Observation columns used by the Kalman filter
OBS_COLS = ["temperature", "humidity", "audio_density"]
OBS_DIM = 3


def require_key(config: dict[str, Any], key: str) -> Any:
    """
    Return a required key from a configuration dictionary.

    Raises a clear error if the key is missing.
    """
    if key not in config:
        raise ValueError(f"KF config missing required key: {key}")
    return config[key]


def as_float(field_name: str, value: Any) -> float:
    """
    Convert a value to float and ensure it is finite.
    """
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"KF config field '{field_name}' must be numeric.") from exc

    if not (result == result) or result in (float("inf"), float("-inf")):
        raise ValueError(f"KF config field '{field_name}' must be finite.")

    return result


def as_bool(field_name: str, value: Any) -> bool:
    """
    Convert a value to boolean when the value is already a bool
    or a numeric 0 or 1.
    """
    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)

    raise ValueError(f"KF config field '{field_name}' must be boolean.")


def as_positive_diag_vector(
    field_name: str,
    value: Any,
    expected_len: int = OBS_DIM,
) -> list[float]:
    """
    Validate a diagonal vector such as Q, R, or P.

    The vector must:
    - be a list
    - have the expected length
    - contain strictly positive numeric values
    """
    if not isinstance(value, list):
        raise ValueError(
            f"KF config field '{field_name}' must be a list of length {expected_len}."
        )

    if len(value) != expected_len:
        raise ValueError(
            f"KF config field '{field_name}' must have length {expected_len}, got {len(value)}."
        )

    result = [as_float(f"{field_name}[{i}]", item) for i, item in enumerate(value)]

    if any(item <= 0 for item in result):
        raise ValueError(
            f"KF config field '{field_name}' must contain strictly positive values."
        )

    return result


def validate_kf_params(params: Any) -> dict[str, Any]:
    """
    Validate the main Kalman filter parameter block.

    This function checks that all required parameters exist
    and that their numeric ranges are valid.
    """
    if not isinstance(params, dict):
        raise ValueError("KF config field 'params' must be an object.")

    required_keys = [
        "USE_DT_AWARE_Q",
        "BASE_DT_MIN",
        "ADAPT_R",
        "ALPHA_R",
        "R_MIN_MULT",
        "R_MAX_MULT",
        "GATE_NIS_NORM",
        "S_JITTER",
        "MISSING_Q_MULT",
        "MISSING_STREAK_MAX_MULT",
        "P_DIAG_MIN",
        "P_DIAG_MAX",
    ]

    for key in required_keys:
        if key not in params:
            raise ValueError(f"KF config params missing required key: {key}")

    clean_params = {
        "USE_DT_AWARE_Q": as_bool(
            "params.USE_DT_AWARE_Q",
            params["USE_DT_AWARE_Q"],
        ),
        "BASE_DT_MIN": as_float(
            "params.BASE_DT_MIN",
            params["BASE_DT_MIN"],
        ),
        "ADAPT_R": as_bool(
            "params.ADAPT_R",
            params["ADAPT_R"],
        ),
        "ALPHA_R": as_float(
            "params.ALPHA_R",
            params["ALPHA_R"],
        ),
        "R_MIN_MULT": as_float(
            "params.R_MIN_MULT",
            params["R_MIN_MULT"],
        ),
        "R_MAX_MULT": as_float(
            "params.R_MAX_MULT",
            params["R_MAX_MULT"],
        ),
        "GATE_NIS_NORM": as_float(
            "params.GATE_NIS_NORM",
            params["GATE_NIS_NORM"],
        ),
        "S_JITTER": as_float(
            "params.S_JITTER",
            params["S_JITTER"],
        ),
        "MISSING_Q_MULT": as_float(
            "params.MISSING_Q_MULT",
            params["MISSING_Q_MULT"],
        ),
        "MISSING_STREAK_MAX_MULT": as_float(
            "params.MISSING_STREAK_MAX_MULT",
            params["MISSING_STREAK_MAX_MULT"],
        ),
        "P_DIAG_MIN": as_float(
            "params.P_DIAG_MIN",
            params["P_DIAG_MIN"],
        ),
        "P_DIAG_MAX": as_float(
            "params.P_DIAG_MAX",
            params["P_DIAG_MAX"],
        ),
    }

    if clean_params["BASE_DT_MIN"] <= 0:
        raise ValueError("params.BASE_DT_MIN must be > 0.")

    if not (0.0 <= clean_params["ALPHA_R"] <= 1.0):
        raise ValueError("params.ALPHA_R must be in [0, 1].")

    if clean_params["R_MIN_MULT"] <= 0 or clean_params["R_MAX_MULT"] <= 0:
        raise ValueError("params.R_MIN_MULT and params.R_MAX_MULT must be > 0.")

    if clean_params["R_MIN_MULT"] > clean_params["R_MAX_MULT"]:
        raise ValueError("params.R_MIN_MULT must be <= params.R_MAX_MULT.")

    if clean_params["S_JITTER"] <= 0:
        raise ValueError("params.S_JITTER must be > 0.")

    if clean_params["MISSING_Q_MULT"] < 1.0:
        raise ValueError("params.MISSING_Q_MULT must be >= 1.0.")

    if clean_params["MISSING_STREAK_MAX_MULT"] < 1.0:
        raise ValueError("params.MISSING_STREAK_MAX_MULT must be >= 1.0.")

    if clean_params["P_DIAG_MIN"] <= 0 or clean_params["P_DIAG_MAX"] <= 0:
        raise ValueError("params.P_DIAG_MIN and params.P_DIAG_MAX must be > 0.")

    if clean_params["P_DIAG_MIN"] > clean_params["P_DIAG_MAX"]:
        raise ValueError("params.P_DIAG_MIN must be <= params.P_DIAG_MAX.")

    return clean_params


def load_kf_config() -> dict[str, Any]:
    """
    Load and validate the Kalman filter configuration JSON file.

    Returns a cleaned configuration dictionary that the rest of the
    backend can safely use.
    """
    if not KF_CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Missing KF config file: {KF_CONFIG_PATH}\n"
            "Create or export kf_config.json from your KF training pipeline."
        )

    with open(KF_CONFIG_PATH, "r", encoding="utf-8") as file:
        raw_config = json.load(file)

    if not isinstance(raw_config, dict):
        raise ValueError("KF config root must be a JSON object.")

    clean_params = validate_kf_params(require_key(raw_config, "params"))

    q_best_diag = as_positive_diag_vector(
        "Q_best_diag",
        require_key(raw_config, "Q_best_diag"),
    )

    r0_diag = as_positive_diag_vector(
        "R0_diag",
        require_key(raw_config, "R0_diag"),
    )

    p0_diag = as_positive_diag_vector(
        "P0_diag",
        require_key(raw_config, "P0_diag"),
    )

    nis_norm_p95 = as_float(
        "nis_norm_p95",
        require_key(raw_config, "nis_norm_p95"),
    )

    nis_norm_p99 = as_float(
        "nis_norm_p99",
        require_key(raw_config, "nis_norm_p99"),
    )

    if nis_norm_p95 <= 0 or nis_norm_p99 <= 0:
        raise ValueError("nis_norm thresholds must be > 0.")

    if nis_norm_p95 > nis_norm_p99:
        raise ValueError("nis_norm_p95 must be <= nis_norm_p99.")

    clean_config: dict[str, Any] = {
        "params": clean_params,
        "Q_best_diag": q_best_diag,
        "R0_diag": r0_diag,
        "P0_diag": p0_diag,
        "nis_norm_p95": nis_norm_p95,
        "nis_norm_p99": nis_norm_p99,
        "obs_cols": raw_config.get("obs_cols", OBS_COLS),
        "model_name": raw_config.get("model_name", "AdaptiveR_KF"),
        "config_version": raw_config.get("config_version", 1),
        "created_at_utc": raw_config.get("created_at_utc"),
    }

    obs_cols = clean_config["obs_cols"]

    if (
        not isinstance(obs_cols, list)
        or len(obs_cols) != OBS_DIM
        or any(not isinstance(col, str) or not col.strip() for col in obs_cols)
    ):
        raise ValueError(
            f"'obs_cols' must be a list of {OBS_DIM} non-empty strings."
        )

    return clean_config