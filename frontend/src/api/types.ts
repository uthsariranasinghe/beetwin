/* =========================================================
   Hive Status Values
   ========================================================= */

export type HiveStatusValue =
  | "no_data"
  | "offline"
  | "healthy"
  | "warning"
  | "critical";

/* =========================================================
   Alert Domain Types
   ========================================================= */

export type AlertType =
  | "missing_data"
  | "warning_anomaly"
  | "critical_anomaly";

export type AlertSeverity = "warning" | "critical";

/* =========================================================
   Sensor Observations
   ========================================================= */

export interface Observation {
  temperature: number | null;
  humidity: number | null;
  audio_density: number | null;
}

/* =========================================================
   Kalman Filter Statistics
   ========================================================= */

export interface NISInfo {
  raw: number | null;
  norm: number | null;
  dof: number;
}

/* =========================================================
   Internal Alert Flags
   ========================================================= */

export interface AlertInfo {
  anomaly_p95: boolean;
  anomaly_p99: boolean;
  chi2_p95: boolean;
  chi2_p99: boolean;
}

/* =========================================================
   Digital Twin Point
   ========================================================= */

export interface TwinPoint {
  hive_id: number;
  ts: string;

  raw: Observation;
  pred: Observation;
  filt: Observation;

  pred_std: Observation;
  adaptive_r: Observation;

  nis: NISInfo;
  alerts: AlertInfo;

  has_observation: boolean;
  ingestion_status?: string;
}

/* =========================================================
   Hive Status
   ========================================================= */

export interface HiveStatus {
  hive_id: number;
  status: HiveStatusValue;
  status_reason: string | null;
  last_ts: string | null;
  alert_count: number;
}

/* =========================================================
   Alert Records
   ========================================================= */

export interface AlertRecord {
  id: number;
  hive_id: number;
  ts: string;

  alert_type: AlertType;
  severity: AlertSeverity;

  title: string;
  message: string | null;

  is_active: boolean;
  is_acknowledged: boolean;
}

/* =========================================================
   API Response Structures
   ========================================================= */

export interface HiveListResponse {
  hives: number[];
}

export interface LatestResponse {
  hive_id: number;
  point: TwinPoint | null;
}

export interface HistoryResponse {
  hive_id: number;
  points: TwinPoint[];
}

export interface AlertsResponse {
  hive_id: number;
  alerts: AlertRecord[];
}

/* =========================================================
   Overview / Snapshot Responses
   ========================================================= */

export interface HiveOverviewItem {
  hive_id: number;
  status: HiveStatus;
  latest_point: TwinPoint | null;
}

export interface HiveOverviewResponse {
  items: HiveOverviewItem[];
  count: number;
}

export interface SnapshotResponse {
  hive_id: number;
  point: TwinPoint | null;
  status: HiveStatus;
  alerts: AlertRecord[];
}

export interface StatusListResponse {
  items: HiveStatus[];
  count: number;
}

/* =========================================================
   WebSocket Messages
   ========================================================= */

export type LiveMessage =
  | {
      type: "snapshot";
      hive_id: number;
      point: TwinPoint | null;
      status: HiveStatus;
      alerts: AlertRecord[];
    }
  | {
      type: "point";
      hive_id: number;
      point: TwinPoint;
    }
  | {
      type: "status";
      hive_id: number;
      status: HiveStatus;
    }
  | {
      type: "alerts";
      hive_id: number;
      alerts: AlertRecord[];
    }
  | {
      type: "heartbeat";
      hive_id: number;
    }
  | {
      type: "error";
      hive_id: number;
      detail: string;
    };

/* =========================================================
   Frontend UI Helper Types
   ========================================================= */

export type UncertaintyLevel = "low" | "moderate" | "high";

export interface UiStatusInfo {
  label: string;
  description: string;
}

export interface UiProblemInfo {
  title: string;
  description: string;
  severityLabel: string;
}