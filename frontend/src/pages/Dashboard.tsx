import logo from "../assets/logo.png";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  getAlerts,
  getHives,
  getOverview,
  getRangeHistory,
  getSnapshot,
} from "../api/http";
import { connectHiveLive } from "../api/ws";
import type {
  AlertRecord,
  HiveOverviewItem,
  HiveStatus,
  LiveMessage,
  TwinPoint,
} from "../api/types";
import AlertsList from "../components/dashboard/AlertsList";
import KpiCards from "../components/dashboard/KpiCards";
import TwinChart from "../components/dashboard/TwinChart";
import AppShell from "../components/layout/AppShell";
import TopBar, {
  type QuickRangeValue,
  type TopBarTab,
} from "../components/layout/TopBar";

type DashboardTab = TopBarTab;
type MetricKey = "temperature" | "humidity" | "audio_density";

type RangeBounds = {
  from: Date;
  to: Date;
};

function toDateInputValue(date: Date) {
  const local = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
  return local.toISOString().slice(0, 16);
}

function parseDateInput(value: string): Date | null {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date;
}

function getQuickRangeBounds(
  value: QuickRangeValue,
  now = new Date()
): RangeBounds | null {
  const to = new Date(now);
  let from = new Date(now);

  switch (value) {
    case "today":
      from = new Date(now);
      from.setHours(0, 0, 0, 0);
      return { from, to };
    case "yesterday": {
      const yesterday = new Date(now);
      yesterday.setDate(yesterday.getDate() - 1);
      yesterday.setHours(0, 0, 0, 0);

      const yesterdayEnd = new Date(yesterday);
      yesterdayEnd.setHours(23, 59, 59, 999);

      return { from: yesterday, to: yesterdayEnd };
    }
    case "last24h":
      from = new Date(now.getTime() - 24 * 60 * 60 * 1000);
      return { from, to };
    case "last7d":
      from = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
      return { from, to };
    case "last30d":
      from = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
      return { from, to };
    default:
      return null;
  }
}

function sortAndDedupePoints(points: TwinPoint[]) {
  const map = new Map<string, TwinPoint>();
  for (const point of points) map.set(point.ts, point);

  return Array.from(map.values()).sort(
    (a, b) => new Date(a.ts).getTime() - new Date(b.ts).getTime()
  );
}

function isPointWithinRange(
  point: TwinPoint,
  from: Date | null,
  to: Date | null
) {
  if (!from || !to) return false;
  const pointTime = new Date(point.ts).getTime();
  return pointTime >= from.getTime() && pointTime <= to.getTime();
}

function formatValue(value: number | null | undefined, digits = 2) {
  if (value == null || Number.isNaN(value)) return "—";
  return Number(value).toFixed(digits);
}

function formatStatusLabel(status?: string) {
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

function getMetricUnit(metric: MetricKey) {
  if (metric === "temperature") return "°C";
  if (metric === "humidity") return "%";
  return "";
}

function getMetricLabel(metric: MetricKey) {
  if (metric === "temperature") return "Temperature";
  if (metric === "humidity") return "Humidity";
  return "Hive Activity";
}

function hasRawObservation(point: TwinPoint) {
  return (
    point.raw.temperature != null ||
    point.raw.humidity != null ||
    point.raw.audio_density != null
  );
}

function hasPredictedOrFilteredValue(point: TwinPoint) {
  return (
    point.pred.temperature != null ||
    point.pred.humidity != null ||
    point.pred.audio_density != null ||
    point.filt.temperature != null ||
    point.filt.humidity != null ||
    point.filt.audio_density != null
  );
}

function hasAnyDisplayValue(point: TwinPoint) {
  return hasRawObservation(point) || hasPredictedOrFilteredValue(point);
}

function getLatestObservedPoint(points: TwinPoint[]) {
  for (let i = points.length - 1; i >= 0; i -= 1) {
    const point = points[i];
    if (point.has_observation && hasRawObservation(point)) return point;
  }
  return null;
}

function getLatestEstimatedPoint(points: TwinPoint[]) {
  for (let i = points.length - 1; i >= 0; i -= 1) {
    const point = points[i];
    if (hasAnyDisplayValue(point)) return point;
  }
  return null;
}

function getMetricDisplayValue(point: TwinPoint | null, metric: MetricKey) {
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

function getSimpleStatusSummary(status: HiveStatus | null) {
  if (!status) return "No current condition available.";
  switch (status.status) {
    case "healthy":
      return "Hive is stable.";
    case "warning":
      return "Hive shows unusual behaviour.";
    case "critical":
      return "Hive needs immediate attention.";
    case "offline":
      return "No recent live data is arriving.";
    default:
      return "Condition unavailable.";
  }
}

function getSimpleActionText(status: HiveStatus | null, alertsCount: number) {
  if (!status) return "Check hive and sensor availability.";

  if (status.status === "healthy") {
    return alertsCount > 0
      ? "Review recent alerts."
      : "No immediate action needed.";
  }

  if (status.status === "warning") {
    return "Inspect hive soon and review trend changes.";
  }

  if (status.status === "critical") {
    return "Immediate inspection recommended.";
  }

  if (status.status === "offline") {
    return "Check sensor power and connection.";
  }

  return "Review hive condition.";
}

function getMostRecentAlert(alerts: AlertRecord[]) {
  if (alerts.length === 0) return null;

  return [...alerts].sort(
    (a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime()
  )[0];
}

function getAlertSummaryText(alert: AlertRecord | null) {
  if (!alert) return "No alerts in selected range.";
  if (alert.message && String(alert.message).trim().length > 0) {
    return alert.message;
  }
  return "A hive condition needs review.";
}


function DashboardSidebar({
  activeTab,
  onChangeTab,
}: {
  activeTab: DashboardTab;
  onChangeTab: (tab: DashboardTab) => void;
}) {
  const items: { id: DashboardTab; title: string }[] = [
    { id: "overview", title: "Overview" },
    { id: "live", title: "Live Monitoring" },
    { id: "alerts", title: "Alerts" },
    { id: "history", title: "History" },
  ];

  return (
    <div className="sidebar">
      <div className="sidebar-brand">
  <img src={logo} alt="BeeTwin Logo" className="sidebar-logo-img" />

  <div className="sidebar-text">
    <div className="sidebar-subtitle">Digital Twin Beehive Dashboard</div>
  </div>
</div>

      <div style={{ marginTop: 20, display: "grid", gap: 10 }}>
        {items.map((item) => {
          const isActive = activeTab === item.id;

          return (
            <button
              key={item.id}
              onClick={() => onChangeTab(item.id)}
              style={{
                textAlign: "left",
                padding: 14,
                borderRadius: 12,
                border: isActive ? "1px solid #60a5fa" : "1px solid #334155",
                background: isActive ? "#1e293b" : "#0f172a",
                color: "white",
              }}
            >
              <div style={{ fontWeight: 700 }}>{item.title}</div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

export default function Dashboard() {
  const [hives, setHives] = useState<number[]>([]);
  const [overview, setOverview] = useState<HiveOverviewItem[]>([]);
  const [selectedHive, setSelectedHive] = useState<number | null>(null);

  const [liveStatus, setLiveStatus] = useState<HiveStatus | null>(null);
  const [livePoint, setLivePoint] = useState<TwinPoint | null>(null);
  const [liveAlerts, setLiveAlerts] = useState<AlertRecord[]>([]);

  const [rangeHistory, setRangeHistory] = useState<TwinPoint[]>([]);
  const [rangeAlertsAll, setRangeAlertsAll] = useState<AlertRecord[]>([]);

  const [loading, setLoading] = useState(true);
  const [rangeLoading, setRangeLoading] = useState(false);
  const [connectionLabel, setConnectionLabel] = useState("Disconnected");
  const [error, setError] = useState<string | null>(null);

  const [activeOnlyAlerts, setActiveOnlyAlerts] = useState(true);
  const [quickRange, setQuickRange] = useState<QuickRangeValue>("today");
  const [activeTab, setActiveTab] = useState<DashboardTab>("overview");
  const [focusMetric, setFocusMetric] = useState<MetricKey>("temperature");

  const initialFrom = toDateInputValue(
    new Date(new Date().setHours(0, 0, 0, 0))
  );
  const initialTo = toDateInputValue(new Date());

  const [draftFromInput, setDraftFromInput] = useState(initialFrom);
  const [draftToInput, setDraftToInput] = useState(initialTo);
  const [appliedFromInput, setAppliedFromInput] = useState(initialFrom);
  const [appliedToInput, setAppliedToInput] = useState(initialTo);

  const wsRef = useRef<WebSocket | null>(null);

  const selectedOverview = useMemo(
    () => overview.find((item) => item.hive_id === selectedHive) ?? null,
    [overview, selectedHive]
  );

  const appliedFrom = useMemo(() => parseDateInput(appliedFromInput), [appliedFromInput]);
  const appliedTo = useMemo(() => parseDateInput(appliedToInput), [appliedToInput]);

  const latestObservedPoint = useMemo(
    () => getLatestObservedPoint(rangeHistory),
    [rangeHistory]
  );

  const latestEstimatedPoint = useMemo(
    () => getLatestEstimatedPoint(rangeHistory) ?? livePoint,
    [rangeHistory, livePoint]
  );

  const currentLiveStatus = liveStatus ?? selectedOverview?.status ?? null;
  const currentLiveStatusText = formatStatusLabel(currentLiveStatus?.status);

  const rangeAlerts = useMemo(() => {
    if (!appliedFrom || !appliedTo) return [];

    return rangeAlertsAll.filter((alert) => {
      const alertTime = new Date(alert.ts);
      if (Number.isNaN(alertTime.getTime())) return true;
      const t = alertTime.getTime();
      return t >= appliedFrom.getTime() && t <= appliedTo.getTime();
    });
  }, [rangeAlertsAll, appliedFrom, appliedTo]);

  const latestRangeAlert = useMemo(() => getMostRecentAlert(rangeAlerts), [rangeAlerts]);

  useEffect(() => {
    async function loadInitial() {
      try {
        setLoading(true);
        setError(null);

        const [hivesRes, overviewRes] = await Promise.all([
          getHives(),
          getOverview(),
        ]);

        setHives(hivesRes.hives);
        setOverview(overviewRes.items);

        if (hivesRes.hives.length > 0) {
          setSelectedHive((prev) => prev ?? hivesRes.hives[0]);
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load dashboard");
      } finally {
        setLoading(false);
      }
    }

    loadInitial();
  }, []);

  useEffect(() => {
    if (quickRange === "custom") return;

    const bounds = getQuickRangeBounds(quickRange);
    if (!bounds) return;

    const fromValue = toDateInputValue(bounds.from);
    const toValue = toDateInputValue(bounds.to);

    setDraftFromInput(fromValue);
    setDraftToInput(toValue);
    setAppliedFromInput(fromValue);
    setAppliedToInput(toValue);
  }, [quickRange]);

  useEffect(() => {
    if (selectedHive == null) return;
    if (!appliedFrom || !appliedTo) return;
    if (appliedFrom.getTime() > appliedTo.getTime()) {
      setError("Start time must be earlier than end time.");
      return;
    }

    const hiveId = selectedHive;
    const fromIso = appliedFrom.toISOString();
    const toIso = appliedTo.toISOString();
    let cancelled = false;

    async function loadHiveData() {
      try {
        setRangeLoading(true);
        setError(null);

        const [snapshotRes, alertsRes, historyRes] = await Promise.all([
          getSnapshot(hiveId),
          getAlerts(hiveId, activeOnlyAlerts, 500, fromIso, toIso),
          getRangeHistory(hiveId, fromIso, toIso, 5000),
        ]);

        if (cancelled) return;

        setLivePoint(snapshotRes.point);
        setLiveStatus(snapshotRes.status);
        setLiveAlerts(alertsRes.alerts);
        setRangeAlertsAll(alertsRes.alerts);
        setRangeHistory(sortAndDedupePoints(historyRes.points));

        setOverview((prev) =>
          prev.map((item) =>
            item.hive_id === hiveId
              ? {
                  ...item,
                  status: snapshotRes.status,
                  latest_point: snapshotRes.point ?? item.latest_point,
                }
              : item
          )
        );
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load hive data");
        }
      } finally {
        if (!cancelled) setRangeLoading(false);
      }
    }

    loadHiveData();
    return () => {
      cancelled = true;
    };
  }, [selectedHive, activeOnlyAlerts, appliedFrom, appliedTo]);

  useEffect(() => {
    if (selectedHive == null) return;

    const hiveId = selectedHive;

    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }

    setConnectionLabel("Connecting...");

    const ws = connectHiveLive(
      hiveId,
      (msg: LiveMessage) => {
        if (msg.type === "snapshot") {
          setLivePoint(msg.point);
          setLiveStatus(msg.status);
          setLiveAlerts(msg.alerts);
          setConnectionLabel("Live");

          setOverview((prev) =>
            prev.map((item) =>
              item.hive_id === msg.hive_id
                ? { ...item, status: msg.status, latest_point: msg.point ?? item.latest_point }
                : item
            )
          );

          if (msg.point && isPointWithinRange(msg.point, appliedFrom, appliedTo)) {
  const point = msg.point;
  setRangeHistory((prev) => sortAndDedupePoints([...prev, point]));
}
          return;
        }

        if (msg.type === "point") {
          setLivePoint(msg.point);

          setOverview((prev) =>
            prev.map((item) =>
              item.hive_id === msg.hive_id
                ? { ...item, latest_point: msg.point }
                : item
            )
          );

          if (isPointWithinRange(msg.point, appliedFrom, appliedTo)) {
            setRangeHistory((prev) => sortAndDedupePoints([...prev, msg.point]));
          }
          return;
        }

        if (msg.type === "status") {
          setLiveStatus(msg.status);
          setOverview((prev) =>
            prev.map((item) =>
              item.hive_id === msg.hive_id ? { ...item, status: msg.status } : item
            )
          );
          return;
        }

        if (msg.type === "alerts") {
          setLiveAlerts(msg.alerts);
          setRangeAlertsAll((prev) =>
            [...msg.alerts, ...prev].sort(
              (a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime()
            )
          );
          return;
        }

        if (msg.type === "heartbeat") {
          setConnectionLabel("Live");
          return;
        }

        if (msg.type === "error") {
          setConnectionLabel("Error");
          setError(msg.detail);
        }
      },
      () => setConnectionLabel("Live"),
      () => setConnectionLabel("Disconnected"),
      () => setConnectionLabel("Error")
    );

    wsRef.current = ws;
    return () => ws.close();
  }, [selectedHive, appliedFrom, appliedTo]);

  async function refreshOverview() {
    try {
      const res = await getOverview();
      setOverview(res.items);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to refresh overview");
    }
  }

  function applyCustomRange() {
    const from = parseDateInput(draftFromInput);
    const to = parseDateInput(draftToInput);

    if (!from || !to) {
      setError("Please select a valid date range.");
      return;
    }

    if (from.getTime() > to.getTime()) {
      setError("Start time must be earlier than end time.");
      return;
    }

    setError(null);
    setQuickRange("custom");
    setAppliedFromInput(draftFromInput);
    setAppliedToInput(draftToInput);
  }

  function renderOverviewTab() {
    return (
      <>
        <KpiCards status={currentLiveStatus} point={latestEstimatedPoint} />

        

        <div className="dashboard-grid">
          <div className="charts-column">
            <div className="card">
              <div className="section-header section-header-space">
                <div>
                  <h3>Hive Overview</h3>
                  <div className="muted">Live condition and trend</div>
                </div>

                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  <button onClick={() => setFocusMetric("temperature")}>Temperature</button>
                  <button onClick={() => setFocusMetric("humidity")}>Humidity</button>
                  <button onClick={() => setFocusMetric("audio_density")}>Activity</button>
                </div>
              </div>

              <TwinChart
                title={getMetricLabel(focusMetric)}
                metric={focusMetric}
                points={rangeHistory}
                mode="simple"
                quickRange={quickRange}
              />
            </div>
          </div>

          <div className="side-column">
            <div className="card">
              <div className="section-header">
                <h3>Current Status</h3>
              </div>

              <div className="details-grid">
                <div>
                  <strong>Hive</strong>
                  <div>{selectedHive ? `Hive ${selectedHive}` : "—"}</div>
                </div>
                <div>
                  <strong>Status</strong>
                  <div>{currentLiveStatusText}</div>
                </div>
                <div>
                  <strong>Connection</strong>
                  <div>{connectionLabel}</div>
                </div>
                <div>
                  <strong>Summary</strong>
                  <div>{getSimpleStatusSummary(currentLiveStatus)}</div>
                </div>
                <div>
                  <strong>Action</strong>
                  <div>{getSimpleActionText(currentLiveStatus, rangeAlerts.length)}</div>
                </div>
                <div>
                  <strong>Latest Alert</strong>
                  <div>{getAlertSummaryText(latestRangeAlert)}</div>
                </div>
              </div>
            </div>

            <AlertsList alerts={rangeAlerts.slice(0, 5)} title="Recent Alerts" />
          </div>
        </div>
      </>
    );
  }

  function renderLiveTab() {
    return (
      <div className="chart-stack">
        <TwinChart
          title="Temperature"
          metric="temperature"
          points={rangeHistory}
          mode="technical"
          quickRange={quickRange}
        />
        <TwinChart
          title="Humidity"
          metric="humidity"
          points={rangeHistory}
          mode="technical"
          quickRange={quickRange}
        />
        <TwinChart
          title="Hive Activity"
          metric="audio_density"
          points={rangeHistory}
          mode="technical"
          quickRange={quickRange}
        />
      </div>
    );
  }

  function renderAlertsTab() {
    return (
      <div className="dashboard-grid">
        <div className="charts-column">
          <div className="card">
            <div className="section-header section-header-space">
              <div>
                <h3>Alerts</h3>
                <div className="muted">Problems and recommendations</div>
              </div>

              <label className="checkbox-row">
                <input
                  type="checkbox"
                  checked={activeOnlyAlerts}
                  onChange={(e) => setActiveOnlyAlerts(e.target.checked)}
                />
                Active only
              </label>
            </div>

            <AlertsList alerts={rangeAlerts} title="All Alerts" />
          </div>
        </div>

        <div className="side-column">
          <div className="card">
            <div className="section-header">
              <h3>Alert Summary</h3>
            </div>
            <div className="details-grid">
              <div>
                <strong>Current Status</strong>
                <div>{currentLiveStatusText}</div>
              </div>
              <div>
                <strong>Total Alerts</strong>
                <div>{rangeAlerts.length}</div>
              </div>
              <div>
                <strong>Live Alerts</strong>
                <div>{liveAlerts.length}</div>
              </div>
              <div>
                <strong>Action</strong>
                <div>{getSimpleActionText(currentLiveStatus, rangeAlerts.length)}</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  function renderHistoryTab() {
    return (
      <div className="dashboard-grid single-view">
        <div className="card">
          <div className="section-header">
            <h3>History</h3>
          </div>

          <div className="toolbar history-toolbar">
            <div className="toolbar-left">
              <label>
                From
                <input
                  type="datetime-local"
                  value={draftFromInput}
                  onChange={(e) => {
                    setQuickRange("custom");
                    setDraftFromInput(e.target.value);
                  }}
                />
              </label>

              <label>
                To
                <input
                  type="datetime-local"
                  value={draftToInput}
                  onChange={(e) => {
                    setQuickRange("custom");
                    setDraftToInput(e.target.value);
                  }}
                />
              </label>
            </div>

            <div className="toolbar-right">
              <button onClick={applyCustomRange}>Apply Range</button>
              <button onClick={refreshOverview}>Refresh</button>
            </div>
          </div>

          <div
            className="details-grid"
            style={{
              marginBottom: 16,
              gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
            }}
          >
            <div>
              <strong>Points Loaded</strong>
              <div>{rangeLoading ? "Loading..." : rangeHistory.length}</div>
            </div>
            <div>
              <strong>Observed Points</strong>
              <div>
                {rangeHistory.filter((p) => p.has_observation && hasRawObservation(p)).length}
              </div>
            </div>
            <div>
              <strong>Estimated State</strong>
              <div>
                {rangeHistory.filter((p) => hasPredictedOrFilteredValue(p)).length}
              </div>
            </div>
            <div>
              <strong>Latest Raw Time</strong>
              <div>
                {latestObservedPoint ? new Date(latestObservedPoint.ts).toLocaleString() : "—"}
              </div>
            </div>
          </div>

          <div className="chart-stack">
            <TwinChart title="Temperature History" metric="temperature" points={rangeHistory} mode="technical" quickRange={quickRange} />
            <TwinChart title="Humidity History" metric="humidity" points={rangeHistory} mode="technical" quickRange={quickRange} />
            <TwinChart title="Activity History" metric="audio_density" points={rangeHistory} mode="technical" quickRange={quickRange} />
          </div>
        </div>
      </div>
    );
  }

  function renderActiveTab() {
    switch (activeTab) {
      case "overview":
        return renderOverviewTab();
      case "live":
        return renderLiveTab();
      case "alerts":
        return renderAlertsTab();
      case "history":
        return renderHistoryTab();
      default:
        return renderOverviewTab();
    }
  }

  return (
    <AppShell
      sidebar={<DashboardSidebar activeTab={activeTab} onChangeTab={setActiveTab} />}
      topbar={
        <TopBar
  hives={hives}
  selectedHive={selectedHive}
  onSelectHive={setSelectedHive}
  connectionLabel={connectionLabel}
  quickRange={quickRange}
  onChangeQuickRange={setQuickRange}
  activeTab={activeTab}
/>
      }
    >
      {loading ? (
        <div className="page-state">Loading hive dashboard...</div>
      ) : hives.length === 0 ? (
        <div className="page-state">No hives available yet.</div>
      ) : (
        <>
          {error ? <div className="error-banner">{error}</div> : null}
          {renderActiveTab()}
        </>
      )}
    </AppShell>
  );
}