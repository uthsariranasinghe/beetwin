import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceDot,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { TwinPoint } from "../../api/types";

type MetricKey = "temperature" | "humidity" | "audio_density";
type ChartMode = "simple" | "technical";
type QuickRangeValue =
  | "today"
  | "yesterday"
  | "last24h"
  | "last7d"
  | "last30d"
  | "custom";

type Props = {
  title: string;
  metric: MetricKey;
  points: TwinPoint[];
  mode?: ChartMode;
  quickRange?: QuickRangeValue;
};

type ChartRow = {
  ts: string;
  fullTs: string;
  raw: number | null;
  predicted: number | null;
  filtered: number | null;
  hasObservation: boolean;
  warning: boolean;
  critical: boolean;
};

function metricUnit(metric: MetricKey) {
  if (metric === "temperature") return "°C";
  if (metric === "humidity") return "%";
  return "";
}

function formatValue(metric: MetricKey, value: number | null) {
  if (value == null || Number.isNaN(value)) return "—";
  if (metric === "audio_density") return value.toFixed(3);
  return `${value.toFixed(2)} ${metricUnit(metric)}`.trim();
}

function buildRows(points: TwinPoint[], metric: MetricKey): ChartRow[] {
  return points.map((p) => {
    const raw = p.raw?.[metric] ?? null;
    const pred = p.pred?.[metric] ?? null;
    const filt = p.filt?.[metric] ?? null;

    return {
      ts: p.ts,
      fullTs: new Date(p.ts).toLocaleString(),
      raw: typeof raw === "number" ? raw : null,
      predicted: typeof pred === "number" ? pred : null,
      filtered: typeof filt === "number" ? filt : null,
      hasObservation: Boolean(p.has_observation),
      warning: Boolean(p.alerts?.anomaly_p95 || p.alerts?.chi2_p95),
      critical: Boolean(p.alerts?.anomaly_p99 || p.alerts?.chi2_p99),
    };
  });
}

function latestValue(rows: ChartRow[]) {
  if (rows.length === 0) return null;
  return rows[rows.length - 1];
}

function formatXAxisTick(value: string, quickRange?: QuickRangeValue) {
  const date = new Date(value);

  if (
    quickRange === "today" ||
    quickRange === "yesterday" ||
    quickRange === "last24h"
  ) {
    return date.toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
    });
  }

  if (quickRange === "last7d" || quickRange === "last30d") {
    return date.toLocaleDateString([], {
      day: "2-digit",
      month: "short",
    });
  }

  return date.toLocaleDateString([], {
    day: "2-digit",
    month: "short",
  });
}

function StatChip({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div className="chart-stat-chip">
      <div className="chart-stat-chip-label">{label}</div>
      <div className="chart-stat-chip-value">{value}</div>
    </div>
  );
}

function CustomTooltip({
  active,
  payload,
  metric,
  mode,
}: {
  active?: boolean;
  payload?: Array<{ payload: ChartRow }>;
  metric: MetricKey;
  mode: ChartMode;
}) {
  if (!active || !payload || payload.length === 0) return null;

  const row = payload[0].payload;

  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-time">{row.fullTs}</div>

      {mode === "technical" && (
        <>
          <div>Raw: {formatValue(metric, row.raw)}</div>
          <div>Predicted: {formatValue(metric, row.predicted)}</div>
        </>
      )}

      <div>Filtered: {formatValue(metric, row.filtered)}</div>
      <div>Source: {row.hasObservation ? "Measured" : "Estimated"}</div>

      {row.critical ? (
        <div className="chart-tooltip-critical">Alert: Critical</div>
      ) : row.warning ? (
        <div className="chart-tooltip-warning">Alert: Warning</div>
      ) : null}
    </div>
  );
}

export default function TwinChart({
  title,
  metric,
  points,
  mode = "simple",
  quickRange,
}: Props) {
  const rows = buildRows(points, metric);
  const latest = latestValue(rows);

  const latestDisplayValue =
    latest?.filtered ?? latest?.predicted ?? latest?.raw ?? null;

  return (
    <div className="card chart-card">
      <div className="section-header section-header-space">
        <div>
          <h3>{title}</h3>
          <div className="muted">
            {mode === "simple"
              ? "Live filtered trend for monitoring"
              : "Raw, predicted, and Kalman filtered comparison"}
          </div>
        </div>

        <div className="chart-meta">
          <span className="chart-point-count">{rows.length} points</span>
          <span className={`chart-mode-badge ${mode}`}>
            {mode === "simple" ? "Simple View" : "Technical View"}
          </span>
        </div>
      </div>

      {latest ? (
        <div className="chart-stats-row">
          {mode === "technical" && (
            <>
              <StatChip label="Raw" value={formatValue(metric, latest.raw)} />
              <StatChip
                label="Predicted"
                value={formatValue(metric, latest.predicted)}
              />
            </>
          )}

          <StatChip
            label="Filtered"
            value={formatValue(metric, latest.filtered)}
          />
          <StatChip
            label="Current"
            value={formatValue(metric, latestDisplayValue)}
          />
          <StatChip
            label="Source"
            value={latest.hasObservation ? "Measured" : "Estimated"}
          />
          <StatChip label="Time" value={latest.fullTs} />
        </div>
      ) : null}

      <div className="chart-wrap">
        {rows.length === 0 ? (
          <div className="empty-state">No data available.</div>
        ) : (
          <ResponsiveContainer width="100%" height={320}>
            <LineChart data={rows} margin={{ top: 10, right: 12, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />

              <XAxis
                dataKey="ts"
                minTickGap={32}
                tickFormatter={(v) => formatXAxisTick(v, quickRange)}
              />

              <YAxis />

              <Tooltip content={<CustomTooltip metric={metric} mode={mode} />} />
              <Legend />

              {mode === "technical" && (
                <Line
                  type="monotone"
                  dataKey="raw"
                  name="Raw"
                  stroke="#94a3b8"
                  strokeDasharray="5 4"
                  dot={false}
                  strokeWidth={2}
                  connectNulls={false}
                />
              )}

              {mode === "technical" && (
                <Line
                  type="monotone"
                  dataKey="predicted"
                  name="Predicted"
                  stroke="#f59e0b"
                  strokeDasharray="6 4"
                  dot={false}
                  strokeWidth={2}
                  connectNulls={true}
                />
              )}

              <Line
                type="monotone"
                dataKey="filtered"
                name="Filtered"
                stroke="#2563eb"
                strokeWidth={3}
                dot={false}
                connectNulls={true}
              />

              {rows.map((r, i) =>
                r.critical && r.filtered != null ? (
                  <ReferenceDot
                    key={`critical-${i}`}
                    x={r.ts}
                    y={r.filtered}
                    r={5}
                    fill="#dc2626"
                    stroke="white"
                  />
                ) : r.warning && r.filtered != null ? (
                  <ReferenceDot
                    key={`warning-${i}`}
                    x={r.ts}
                    y={r.filtered}
                    r={4}
                    fill="#f59e0b"
                    stroke="white"
                  />
                ) : null
              )}
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}