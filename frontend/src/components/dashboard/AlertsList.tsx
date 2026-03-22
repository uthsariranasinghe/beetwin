import { useEffect, useState } from "react";
import type { AlertRecord } from "../../api/types";

type Props = {
  alerts: AlertRecord[];
  title?: string;
};

function severityClass(severity?: string) {
  switch (severity?.toLowerCase()) {
    case "critical":
      return "severity critical";
    case "warning":
      return "severity warning";
    default:
      return "severity normal";
  }
}

function severityLabel(severity?: string) {
  switch (severity?.toLowerCase()) {
    case "critical":
      return "Critical";
    case "warning":
      return "Warning";
    default:
      return "Info";
  }
}

function problemTypeLabel(alertType?: string) {
  switch (alertType) {
    case "missing_data":
      return "Missing sensor reading";
    case "warning_anomaly":
      return "Unusual hive behaviour";
    case "critical_anomaly":
      return "Serious hive change";
    default:
      return "Hive issue";
  }
}

function friendlyTitle(alert: AlertRecord) {
  switch (alert.alert_type) {
    case "missing_data":
      return "Sensor reading missing";
    case "warning_anomaly":
      return "Unusual hive pattern detected";
    case "critical_anomaly":
      return "Serious hive pattern change detected";
    default:
      return alert.title?.trim() || "Hive issue detected";
  }
}

function friendlyMessage(alert: AlertRecord) {
  if (alert.message && alert.message.trim().length > 0) {
    return alert.message;
  }

  switch (alert.alert_type) {
    case "missing_data":
      return "One or more sensor values were missing, so the system had to continue using estimated hive state information.";
    case "warning_anomaly":
      return "The hive moved outside its usual pattern. This may indicate early stress, environmental change, or unusual activity.";
    case "critical_anomaly":
      return "The hive moved far outside its expected pattern. This may indicate a serious condition that needs attention.";
    default:
      return "The monitoring system detected a condition that should be reviewed.";
  }
}

function meaningText(alert: AlertRecord) {
  switch (alert.alert_type) {
    case "missing_data":
      return "The latest reading was incomplete or unavailable.";
    case "warning_anomaly":
      return "The system noticed behaviour that is not normal for this hive.";
    case "critical_anomaly":
      return "The system noticed a strong abnormal change compared with the hive's normal behaviour.";
    default:
      return "The system detected a hive-related issue.";
  }
}

function actionText(alert: AlertRecord) {
  switch (alert.alert_type) {
    case "missing_data":
      return "Check sensor power, connection, and data flow. Confirm whether the latest reading resumed normally.";
    case "warning_anomaly":
      return "Inspect the hive soon and review temperature, humidity, and activity trends.";
    case "critical_anomaly":
      return "Inspect the hive immediately. Check colony condition, ventilation, moisture, and possible disturbance.";
    default:
      return "Review the latest hive condition and inspect if needed.";
  }
}

function statusLabel(alert: AlertRecord) {
  return alert.is_active ? "Mark as Resolved" : "Resolved";
}

function reviewLabel(alert: AlertRecord) {
  return alert.is_acknowledged ? "Reviewed" : "Needs review";
}

function getAlertTime(alert: AlertRecord): string | null {
  if (!alert.ts) return null;

  const parsed = new Date(alert.ts);
  if (Number.isNaN(parsed.getTime())) return null;

  return parsed.toLocaleString();
}

function sortAlertsNewestFirst(alerts: AlertRecord[]) {
  return [...alerts].sort((a, b) => {
    const aTime = new Date(a.ts ?? "").getTime();
    const bTime = new Date(b.ts ?? "").getTime();
    return bTime - aTime;
  });
}

export default function AlertsList({
  alerts,
  title = "Alerts",
}: Props) {
  const [localAlerts, setLocalAlerts] = useState<AlertRecord[]>(alerts);

  useEffect(() => {
    setLocalAlerts(alerts);
  }, [alerts]);

  async function handleResolve(id: number | string) {
  try {
    await fetch(`http://127.0.0.1:8000/api/alerts/${id}/resolve`, {
      method: "PATCH",
    });

    // remove from UI after backend success
    setLocalAlerts((prev) =>
      prev.filter((alert) => alert.id !== id)
    );
  } catch (error) {
    console.error("Failed to resolve alert:", error);
  }
}

  const sortedAlerts = sortAlertsNewestFirst(localAlerts);

  return (
    <div className="card">
      <div className="section-header">
        <h3>{title}</h3>
        <span className="muted">{sortedAlerts.length} items</span>
      </div>

      {sortedAlerts.length === 0 ? (
        <div className="empty-state">No alerts in the selected range.</div>
      ) : (
        <div className="alerts-list">
          {sortedAlerts.map((alert, index) => {
            const detectedAt = getAlertTime(alert);
            const newest = index === 0;

            return (
              <div
                className="alert-item"
                key={alert.id}
                style={{
                  border: newest ? "1px solid rgba(59, 130, 246, 0.35)" : undefined,
                  boxShadow: newest
                    ? "0 0 0 1px rgba(59, 130, 246, 0.08)"
                    : undefined,
                }}
              >
                <div className="alert-top">
                  <div
                    style={{
                      display: "flex",
                      gap: 8,
                      alignItems: "center",
                      flexWrap: "wrap",
                    }}
                  >
                    <span className={severityClass(alert.severity)}>
                      {severityLabel(alert.severity)}
                    </span>

                    {newest ? <span className="flag ack">Latest</span> : null}
                  </div>

                  <span className="muted">
                    {detectedAt ? detectedAt : "Time unavailable"}
                  </span>
                </div>

                <div className="alert-title">{friendlyTitle(alert)}</div>

                <div className="alert-meta">
                  Type: <strong>{problemTypeLabel(alert.alert_type)}</strong>
                </div>

                <div className="alert-message" style={{ marginTop: 8 }}>
                  {friendlyMessage(alert)}
                </div>

                <div
                  style={{
                    marginTop: 12,
                    padding: 12,
                    borderRadius: 10,
                    background: "rgba(148, 163, 184, 0.08)",
                  }}
                >
                  <div style={{ fontWeight: 600, marginBottom: 6 }}>
                    What this means
                  </div>
                  <div>{meaningText(alert)}</div>
                </div>

                <div
                  style={{
                    marginTop: 10,
                    padding: 12,
                    borderRadius: 10,
                    background: "rgba(59, 130, 246, 0.08)",
                  }}
                >
                  <div style={{ fontWeight: 600, marginBottom: 6 }}>
                    Recommended action
                  </div>
                  <div>{actionText(alert)}</div>
                </div>

                <div className="alert-flags" style={{ marginTop: 12 }}>
                  <span
                    className={alert.is_active ? "flag active" : "flag resolved"}
                    onClick={() => handleResolve(alert.id)}
                    style={{ cursor: "pointer" }}
                    title="Click to remove this alert"
                  >
                    {statusLabel(alert)}
                  </span>

                  <span className="flag ack">{reviewLabel(alert)}</span>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}