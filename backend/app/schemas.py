from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field



# Enumerations used across the API


# Possible hive health states shown on the dashboard
HiveStatusValue = Literal["no_data", "offline", "healthy", "warning", "critical"]

# Alert severity levels
AlertSeverity = Literal["warning", "critical"]

# Types of alerts produced by the monitoring system
AlertType = Literal["missing_data", "warning_anomaly", "critical_anomaly"]


# Incoming API request models


class MeasurementIn(BaseModel):
    """
    Single sensor measurement received by the API.

    This represents one reading coming from a hive sensor device.
    """
    hive_id: int = Field(..., ge=0)
    ts: datetime

    temperature: Optional[float] = None
    humidity: Optional[float] = None
    audio_density: Optional[float] = None

    # Time difference from previous measurement (minutes)
    dt_prev_min: Optional[float] = Field(None, gt=0)


class MeasurementBatchIn(BaseModel):
    """
    Batch ingestion format used by the simulator.

    Allows multiple measurements to be inserted in one request.
    """
    items: List[MeasurementIn] = Field(..., min_length=1, max_length=5000)


class HiveRegisterIn(BaseModel):
    """
    Request payload for registering new hive IDs.
    """
    hive_ids: List[int] = Field(..., min_length=1)


# Basic reusable structures


class Observation(BaseModel):
    """
    Generic observation vector used for raw, predicted, and filtered values.
    """
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    audio_density: Optional[float] = None


class NISInfo(BaseModel):
    """
    Normalized Innovation Squared (NIS) statistics.

    Used for anomaly detection in the Kalman filter.
    """
    raw: Optional[float] = None
    norm: Optional[float] = None
    dof: int = Field(0, ge=0)


class AlertInfo(BaseModel):
    """
    Boolean flags produced by the anomaly detection logic.
    """
    anomaly_p95: bool = False
    anomaly_p99: bool = False
    chi2_p95: bool = False
    chi2_p99: bool = False



# Digital twin output structure


class TwinPoint(BaseModel):
    """
    Core digital twin data structure.

    Contains:
    - raw sensor data
    - Kalman prediction
    - filtered estimate
    - prediction uncertainty
    - anomaly metrics
    """
    hive_id: int = Field(..., ge=0)
    ts: datetime

    raw: Observation
    pred: Observation
    filt: Observation
    pred_std: Observation

    nis: NISInfo
    alerts: AlertInfo
    adaptive_r: Observation

    has_observation: bool = True


# Hive status models


class HiveStatus(BaseModel):
    """
    Current operational status of a hive.
    """
    hive_id: int = Field(..., ge=0)
    status: HiveStatusValue
    status_reason: Optional[str] = None
    last_ts: Optional[datetime] = None
    alert_count: int = Field(0, ge=0)


# Alert models


class AlertRecord(BaseModel):
    """
    Stored alert event produced by anomaly detection or system monitoring.
    """
    id: int = Field(..., ge=1)
    hive_id: int = Field(..., ge=0)
    ts: datetime

    alert_type: AlertType
    severity: AlertSeverity

    title: str
    message: Optional[str] = None

    is_active: bool
    is_acknowledged: bool



# Dashboard API response models


class HiveOverviewItem(BaseModel):
    """
    Dashboard overview entry for one hive.
    """
    hive_id: int = Field(..., ge=0)
    status: HiveStatus
    latest_point: Optional[TwinPoint] = None


class HiveOverviewResponse(BaseModel):
    """
    Response containing multiple hive overview entries.
    """
    items: List[HiveOverviewItem]
    count: int = Field(..., ge=0)


class StatusListResponse(BaseModel):
    """
    List of hive statuses.
    """
    items: List[HiveStatus]
    count: int = Field(..., ge=0)


class SnapshotResponse(BaseModel):
    """
    Full snapshot returned when a websocket client connects.

    Contains latest state of the hive.
    """
    hive_id: int = Field(..., ge=0)
    point: Optional[TwinPoint] = None
    status: HiveStatus
    alerts: List[AlertRecord]


class HistoryResponse(BaseModel):
    """
    Historical digital twin points for one hive.
    """
    hive_id: int = Field(..., ge=0)
    points: List[TwinPoint]


class LatestResponse(BaseModel):
    """
    Latest digital twin state for a hive.
    """
    hive_id: int = Field(..., ge=0)
    point: Optional[TwinPoint]


class HiveListResponse(BaseModel):
    """
    List of registered hive IDs.
    """
    hives: List[int]


class AlertsResponse(BaseModel):
    """
    Alert records returned by the alerts endpoint.
    """
    hive_id: int = Field(..., ge=0)
    alerts: List[AlertRecord]