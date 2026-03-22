import type { HiveStatus, TwinPoint } from "../../api/types";

type Props = {
  status: HiveStatus | null;
  point: TwinPoint | null;
};

type MetricKey = "temperature" | "humidity" | "audio_density";

function formatValue(value: number | null | undefined, digits = 2) {
  if (value == null || Number.isNaN(value)) return "—";
  return Number(value).toFixed(digits);
}

function getMetricUnit(metric: MetricKey) {
  if (metric === "temperature") return "°C";
  if (metric === "humidity") return "%";
  return "";
}

function getDisplayMetricValue(point: TwinPoint | null, metric: MetricKey) {
  if (!point) return "—";

  const value =
    point.filt?.[metric] ??
    point.pred?.[metric] ??
    point.raw?.[metric] ??
    null;

  if (value == null || Number.isNaN(value)) return "—";

  const digits = metric === "audio_density" ? 3 : 2;
  const unit = getMetricUnit(metric);

  return `${formatValue(value, digits)} ${unit}`.trim();
}

function getStatusClass(status?: string | null) {
  switch (status) {
    case "healthy":
      return "status-pill healthy";
    case "warning":
      return "status-pill warning";
    case "critical":
      return "status-pill critical";
    case "offline":
      return "status-pill offline";
    default:
      return "status-pill no-data";
  }
}

function getStatusLabel(status?: string | null) {
  switch (status) {
    case "healthy":
      return "Healthy";
    case "warning":
      return "Warning";
    case "critical":
      return "Critical";
    case "offline":
      return "Offline";
    default:
      return "No Data";
  }
}

function getOverallUncertaintyLabel(point: TwinPoint | null) {
  if (!point) return "Unknown";

  const temperatureStd = point.pred_std.temperature;
  const humidityStd = point.pred_std.humidity;
  const activityStd = point.pred_std.audio_density;

  const levels: number[] = [];

  if (temperatureStd != null && !Number.isNaN(temperatureStd)) {
    if (temperatureStd < 1) levels.push(1);
    else if (temperatureStd < 3) levels.push(2);
    else levels.push(3);
  }

  if (humidityStd != null && !Number.isNaN(humidityStd)) {
    if (humidityStd < 3) levels.push(1);
    else if (humidityStd < 8) levels.push(2);
    else levels.push(3);
  }

  if (activityStd != null && !Number.isNaN(activityStd)) {
    if (activityStd < 0.03) levels.push(1);
    else if (activityStd < 0.08) levels.push(2);
    else levels.push(3);
  }

  if (levels.length === 0) return "Unknown";

  const worstLevel = Math.max(...levels);

  if (worstLevel === 1) return "Low";
  if (worstLevel === 2) return "Moderate";
  return "High";
}

function getStatusReason(status: HiveStatus | null) {
  if (!status) return "No current condition";
  if (status.status_reason && status.status_reason.trim().length > 0) {
    return status.status_reason;
  }

  switch (status.status) {
    case "healthy":
      return "Within expected range";
    case "warning":
      return "Unusual behaviour detected";
    case "critical":
      return "Urgent inspection recommended";
    case "offline":
      return "No recent live data";
    default:
      return "Condition unavailable";
  }
}

function getDataSourceLabel(point: TwinPoint | null) {
  if (!point) return "Unknown";
  return point.has_observation ? "Measured" : "Estimated";
}

function getLatestTimeLabel(point: TwinPoint | null) {
  if (!point) return "—";
  return new Date(point.ts).toLocaleString();
}

function MetricCard({
  label,
  value,
  helper,
  accent = "",
}: {
  label: string;
  value: string;
  helper: string;
  accent?: string;
}) {
  return (
    <div className={`card kpi-card ${accent}`.trim()}>
      <div className="card-label">{label}</div>
      <div className="kpi-value">{value}</div>
      <div className="kpi-helper">{helper}</div>
    </div>
  );
}

export default function KpiCards({ status, point }: Props) {
  const conditionLabel = getStatusLabel(status?.status);
  const conditionReason = getStatusReason(status);
  const dataConfidence = getOverallUncertaintyLabel(point);
  const latestTime = getLatestTimeLabel(point);
  const dataSource = getDataSourceLabel(point);

  return (
    <div className="kpi-grid">
      <div className="card kpi-card kpi-card-primary">
        <div className="card-label">Hive Condition</div>

        <div className="kpi-top-row">
          <div className={getStatusClass(status?.status)}>{conditionLabel}</div>
        </div>

        <div className="kpi-helper">{conditionReason}</div>
      </div>

      <MetricCard
        label="Temperature"
        value={getDisplayMetricValue(point, "temperature")}
        helper="Filtered live value"
      />

      <MetricCard
        label="Humidity"
        value={getDisplayMetricValue(point, "humidity")}
        helper="Filtered live value"
      />

      <MetricCard
        label="Activity"
        value={getDisplayMetricValue(point, "audio_density")}
        helper="Filtered live value"
      />

      <MetricCard
        label="Model Confidence"
        value={dataConfidence}
        helper="Based on prediction uncertainty"
      />

      <MetricCard
        label="Last Update"
        value={latestTime}
        helper={`Source: ${dataSource}`}
      />
    </div>
  );
}