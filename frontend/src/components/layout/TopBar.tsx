import { useEffect, useMemo, useState } from "react";

export type QuickRangeValue =
  | "today"
  | "yesterday"
  | "last24h"
  | "last7d"
  | "last30d"
  | "custom";

export type TopBarTab = "overview" | "live" | "alerts" | "history";
export type TopBarMode = "replay" | "live";

type Props = {
  hives: number[];
  selectedHive: number | null;
  onSelectHive: (hiveId: number) => void;
  connectionLabel: string;
  quickRange: QuickRangeValue;
  onChangeQuickRange: (value: QuickRangeValue) => void;
  activeTab: TopBarTab;
  mode: TopBarMode;
  onChangeMode: (mode: TopBarMode) => void;
};

function connectionText(label: string) {
  switch (label) {
    case "Live":
      return "Live";
    case "Live Mode":
      return "Live Mode";
    case "Replay Mode":
      return "Replay Mode";
    case "Connecting...":
      return "Connecting";
    case "Disconnected":
      return "Offline";
    case "Error":
      return "Error";
    default:
      return label;
  }
}

function connectionClass(label: string) {
  return `connection-pill ${label.toLowerCase().replace(/[^a-z]+/g, "-")}`;
}

function formatNow(date: Date) {
  return date.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function rangeLabel(value: QuickRangeValue) {
  switch (value) {
    case "today":
      return "Today";
    case "yesterday":
      return "Yesterday";
    case "last24h":
      return "Last 24 Hours";
    case "last7d":
      return "Last 7 Days";
    case "last30d":
      return "Last 30 Days";
    case "custom":
    default:
      return "Custom Range";
  }
}

function getPageTitle(tab: TopBarTab) {
  switch (tab) {
    case "overview":
      return "Overview";
    case "live":
      return "Live Monitoring";
    case "alerts":
      return "Alerts";
    case "history":
      return "History";
    default:
      return "Overview";
  }
}

function getTopbarSubtitle(
  selectedHive: number | null
) {
  return selectedHive == null
    ? "No hive selected"
    : `Hive ${selectedHive}`;
}

export default function TopBar({
  hives,
  selectedHive,
  onSelectHive,
  connectionLabel,
  quickRange,
  onChangeQuickRange,
  activeTab,
  mode,
  onChangeMode,
}: Props) {
  const [now, setNow] = useState(new Date());

  useEffect(() => {
    const timer = window.setInterval(() => {
      setNow(new Date());
    }, 60_000);

    return () => window.clearInterval(timer);
  }, []);

const subtitle = useMemo(() => {
  return getTopbarSubtitle(selectedHive);
}, [selectedHive]);
  return (
    <div className="topbar-inner">
      <div className="topbar-left">
        <div className="topbar-brand-block">
          <h1>{getPageTitle(activeTab)}</h1>
        </div>
        <div className="topbar-context">{subtitle}</div>
      </div>

      <div className="topbar-right">
        <div className="topbar-control-card">
          <span className="topbar-select-label">Mode</span>
          <select
            value={mode}
            onChange={(e) => onChangeMode(e.target.value as TopBarMode)}
          >
            <option value="replay">Replay Mode</option>
            <option value="live">Live Mode</option>
          </select>
        </div>

        <div className="topbar-control-card">
          <span className="topbar-select-label">Hive</span>
          <select
            value={selectedHive ?? ""}
            onChange={(e) => {
              const value = e.target.value;
              if (!value) return;
              onSelectHive(Number(value));
            }}
          >
            <option value="" disabled>
              Select hive
            </option>
            {hives.map((hiveId) => (
              <option key={hiveId} value={hiveId}>
                Hive {hiveId}
              </option>
            ))}
          </select>
        </div>

        <div className="topbar-control-card">
          <span className="topbar-select-label">Range</span>
          <select
            value={quickRange}
            onChange={(e) => onChangeQuickRange(e.target.value as QuickRangeValue)}
          >
            <option value="today">Today</option>
            <option value="yesterday">Yesterday</option>
            <option value="last24h">Last 24 Hours</option>
            <option value="last7d">Last 7 Days</option>
            <option value="last30d">Last 30 Days</option>
            <option value="custom">Custom Range</option>
          </select>
        </div>

        <div className="topbar-info-chip">
          <span className="topbar-select-label">Time</span>
          <span className="topbar-time-value">{formatNow(now)}</span>
        </div>

        <span className={connectionClass(connectionLabel)}>
          <span className="connection-dot" />
          {connectionText(connectionLabel)}
        </span>
      </div>
    </div>
  );
}
