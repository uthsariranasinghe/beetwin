from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np


# State dimension:
# [temperature, humidity, audio_density]
D = 3

# Identity transition model:
# x_t = x_(t-1) + process_noise
A = np.eye(D, dtype=float)

# Identity observation model:
# z_t directly observes the same state dimensions
H = np.eye(D, dtype=float)


@dataclass
class StepOut:
    """
    Output of one Kalman filter step.

    Contains:
    - prediction before measurement update
    - filtered state after measurement update
    - prediction uncertainty
    - anomaly statistics
    - current adaptive measurement noise
    """
    x_pred: np.ndarray
    P_pred: np.ndarray
    x_filt: np.ndarray
    pred_std: np.ndarray
    nis_raw: Optional[float]
    nis_norm: Optional[float]
    nis_dof: int
    R_diag: np.ndarray


class HiveKalmanFilter:
    """
    Adaptive Kalman filter for one hive.

    This filter tracks a 3-dimensional hive state:
    - temperature
    - humidity
    - audio_density

    It supports:
    - missing observations
    - adaptive measurement noise R
    - time-gap-aware process noise Q
    - covariance sanitization for numerical stability
    """

    def __init__(
        self,
        Q_diag: np.ndarray,
        R0_diag: np.ndarray,
        P0_diag: np.ndarray,
        cfg: dict,
    ):
        self.cfg = cfg
        self.Q_diag = np.asarray(Q_diag, dtype=float).reshape(D)
        self.R0_diag = np.asarray(R0_diag, dtype=float).reshape(D)
        self.P0_diag = np.asarray(P0_diag, dtype=float).reshape(D)

        self.reset()

    def reset(self) -> None:
        """
        Reset the filter to its initial uninitialized state.
        """
        self.x = np.zeros((D,), dtype=float)
        self.P = np.diag(self.P0_diag.copy())
        self.R_diag_t = self.R0_diag.copy()
        self.missing_streak = 0
        self.initialized = False

    def load_state(
        self,
        x: np.ndarray,
        P: np.ndarray,
        R_diag_t: np.ndarray,
        missing_streak: int,
        initialized: bool,
    ) -> None:
        """
        Restore the filter from previously saved runtime state.
        """
        self.x = np.asarray(x, dtype=float).reshape(D)
        self.P = np.asarray(P, dtype=float).reshape(D, D)
        self.P = self._sanitize_covariance(self.P, fallback_diag=self.P0_diag)

        self.R_diag_t = np.asarray(R_diag_t, dtype=float).reshape(D)
        self.R_diag_t = np.clip(
            self.R_diag_t,
            float(self.cfg["R_MIN_MULT"]) * self.R0_diag,
            float(self.cfg["R_MAX_MULT"]) * self.R0_diag,
        )

        self.missing_streak = max(0, int(missing_streak))
        self.initialized = bool(initialized)

    def export_state(self) -> dict:
        """
        Export the current filter state for persistence.
        """
        return {
            "x": self.x.copy(),
            "P": self.P.copy(),
            "R_diag_t": self.R_diag_t.copy(),
            "missing_streak": int(self.missing_streak),
            "initialized": bool(self.initialized),
        }

    def _sanitize_vector(self, x: np.ndarray, fallback: np.ndarray) -> np.ndarray:
        """
        Replace invalid numeric values in a vector using fallback values.
        """
        x = np.asarray(x, dtype=float).reshape(D)
        fallback = np.asarray(fallback, dtype=float).reshape(D)

        bad = ~np.isfinite(x)
        if np.any(bad):
            x = x.copy()
            x[bad] = fallback[bad]

        return x

    def _sanitize_covariance(
        self,
        Pmat: np.ndarray,
        fallback_diag: np.ndarray,
    ) -> np.ndarray:
        """
        Make sure the covariance matrix is usable.

        Steps:
        - force symmetry
        - replace invalid diagonal values
        - clip diagonal to safe bounds
        - if matrix is invalid or not positive definite, fall back to diagonal form
        """
        Pmat = np.asarray(Pmat, dtype=float).reshape(D, D)
        Pmat = 0.5 * (Pmat + Pmat.T)

        diag = np.diag(Pmat).copy()
        fallback_diag = np.asarray(fallback_diag, dtype=float).reshape(D)

        bad_diag = ~np.isfinite(diag)
        if np.any(bad_diag):
            diag[bad_diag] = fallback_diag[bad_diag]

        diag = np.clip(
            diag,
            float(self.cfg["P_DIAG_MIN"]),
            float(self.cfg["P_DIAG_MAX"]),
        )

        if not np.isfinite(Pmat).all():
            return np.diag(diag)

        Pmat = Pmat.copy()
        np.fill_diagonal(Pmat, diag)

        try:
            np.linalg.cholesky(Pmat + float(self.cfg["S_JITTER"]) * np.eye(D))
        except np.linalg.LinAlgError:
            return np.diag(diag)

        return Pmat

    def _init_from_first_obs(self, z: np.ndarray) -> None:
        """
        Initialize the filter state from the first valid observation.

        Only observed dimensions are used for initialization.
        """
        mask = np.isfinite(z)
        if mask.any():
            self.x[mask] = z[mask]
            self.initialized = True

    def _dt_scale(self, dt_prev_min: Optional[float]) -> float:
        """
        Compute a process-noise scale factor based on the time gap.

        If dt-aware Q is enabled, larger time gaps produce larger uncertainty.
        """
        scale = 1.0

        if bool(self.cfg["USE_DT_AWARE_Q"]):
            if dt_prev_min is not None:
                try:
                    dt = float(dt_prev_min)
                except (TypeError, ValueError):
                    dt = np.nan

                if np.isfinite(dt) and dt > 0:
                    scale = dt / float(self.cfg["BASE_DT_MIN"])
                    scale = float(np.clip(scale, 1.0, 16.0))

        return scale

    def step(self, z: np.ndarray, dt_prev_min: Optional[float]) -> StepOut:
        """
        Run one Kalman filter step.

        Input:
        - z: observation vector with NaN for missing values
        - dt_prev_min: time gap since previous accepted reading

        Output:
        - prediction before update
        - filtered state after update
        - uncertainty and anomaly statistics
        """
        z = np.asarray(z, dtype=float).reshape(D)

        # Initialize from the first observed measurement if possible
        if not self.initialized:
            self._init_from_first_obs(z)

        # If initialization is still impossible, return NaN outputs
        # so the dashboard does not display fake zero values.
        if not self.initialized:
            x_nan = np.full((D,), np.nan, dtype=float)
            P_nan = np.full((D, D), np.nan, dtype=float)
            pred_std_nan = np.full((D,), np.nan, dtype=float)

            return StepOut(
                x_pred=x_nan,
                P_pred=P_nan,
                x_filt=x_nan,
                pred_std=pred_std_nan,
                nis_raw=None,
                nis_norm=None,
                nis_dof=0,
                R_diag=self.R_diag_t.copy(),
            )

        # Sanitize internal state before prediction
        self.x = self._sanitize_vector(self.x, fallback=np.zeros(D, dtype=float))
        self.P = self._sanitize_covariance(self.P, fallback_diag=self.P0_diag)
        self.R_diag_t = self._sanitize_vector(self.R_diag_t, fallback=self.R0_diag)
        self.R_diag_t = np.clip(
            self.R_diag_t,
            float(self.cfg["R_MIN_MULT"]) * self.R0_diag,
            float(self.cfg["R_MAX_MULT"]) * self.R0_diag,
        )

        # Prediction step
        dt_scale = self._dt_scale(dt_prev_min)
        Q_t = np.diag(self.Q_diag * dt_scale)

        self.x = A @ self.x
        self.P = A @ self.P @ A.T + Q_t
        self.P = self._sanitize_covariance(self.P, fallback_diag=self.P0_diag)

        # Determine which observation dimensions are available
        mask = np.isfinite(z)
        observed_dim = int(mask.sum())

        # If all observations are missing, inflate uncertainty
        if observed_dim == 0:
            self.missing_streak += 1

            inflation = min(
                1.0 + (float(self.cfg["MISSING_Q_MULT"]) - 1.0) * self.missing_streak,
                float(self.cfg["MISSING_STREAK_MAX_MULT"]),
            )

            inflated_diag = np.clip(
                np.diag(self.P) * inflation,
                float(self.cfg["P_DIAG_MIN"]),
                float(self.cfg["P_DIAG_MAX"]),
            )

            self.P = np.diag(inflated_diag)
        else:
            self.missing_streak = 0

        # Save prediction before any measurement assimilation
        x_pred = self.x.copy()
        P_pred = self.P.copy()
        pred_std = np.sqrt(np.maximum(np.diag(P_pred), 0.0))

        # If no observations are available, stop after prediction
        if observed_dim == 0:
            return StepOut(
                x_pred=x_pred,
                P_pred=P_pred,
                x_filt=self.x.copy(),
                pred_std=pred_std,
                nis_raw=None,
                nis_norm=None,
                nis_dof=0,
                R_diag=self.R_diag_t.copy(),
            )

        # Innovation for observed dimensions
        H_obs = H[mask]
        innovation = z[mask] - (H_obs @ self.x)

        s_jitter = float(self.cfg["S_JITTER"])

        # Compute NIS using the current R before adaptive update
        R_current = np.diag(self.R_diag_t)[np.ix_(mask, mask)]
        S0 = H_obs @ self.P @ H_obs.T + R_current
        S0 = 0.5 * (S0 + S0.T) + s_jitter * np.eye(observed_dim)

        try:
            nis_raw = float(innovation.T @ np.linalg.solve(S0, innovation))
        except np.linalg.LinAlgError:
            nis_raw = float(innovation.T @ np.linalg.pinv(S0) @ innovation)

        nis_norm = nis_raw / max(observed_dim, 1)

        # Adaptive measurement noise update
        # Only update R if the normalized innovation is not too extreme
        if bool(self.cfg["ADAPT_R"]) and np.isfinite(nis_norm):
            if nis_norm <= float(self.cfg["GATE_NIS_NORM"]):
                idx = np.where(mask)[0]
                alpha_r = float(self.cfg["ALPHA_R"])

                self.R_diag_t[idx] = (
                    alpha_r * self.R_diag_t[idx]
                    + (1.0 - alpha_r) * (innovation ** 2)
                )

                self.R_diag_t = np.clip(
                    self.R_diag_t,
                    float(self.cfg["R_MIN_MULT"]) * self.R0_diag,
                    float(self.cfg["R_MAX_MULT"]) * self.R0_diag,
                )

        # Measurement update
        R_t = np.diag(self.R_diag_t)[np.ix_(mask, mask)]
        S = H_obs @ self.P @ H_obs.T + R_t
        S = 0.5 * (S + S.T) + s_jitter * np.eye(observed_dim)

        try:
            K = (self.P @ H_obs.T) @ np.linalg.solve(S, np.eye(observed_dim))
        except np.linalg.LinAlgError:
            K = (self.P @ H_obs.T) @ np.linalg.pinv(S)

        I = np.eye(D)

        self.x = self.x + K @ innovation

        # Joseph form covariance update for better numerical stability
        self.P = (I - K @ H_obs) @ self.P @ (I - K @ H_obs).T + K @ R_t @ K.T
        self.P = self._sanitize_covariance(self.P, fallback_diag=np.diag(P_pred))

        return StepOut(
            x_pred=x_pred,
            P_pred=P_pred,
            x_filt=self.x.copy(),
            pred_std=pred_std,
            nis_raw=nis_raw,
            nis_norm=nis_norm,
            nis_dof=observed_dim,
            R_diag=self.R_diag_t.copy(),
        )