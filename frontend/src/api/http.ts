import type {
  AlertsResponse,
  HiveListResponse,
  HiveOverviewResponse,
  HistoryResponse,
  LatestResponse,
  SnapshotResponse,
  StatusListResponse,
  HiveStatus,
} from "./types";

const API_BASE = (
  import.meta.env.VITE_API_BASE || "https://believable-flexibility-production-77c5.up.railway.app""
).replace(/\/$/, "");

async function httpGet<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`);

  if (!response.ok) {
    let message = `Request failed (${response.status})`;

    try {
      const contentType = response.headers.get("content-type") || "";

      if (contentType.includes("application/json")) {
        const body = await response.json();
        if (body?.detail) {
          message =
            typeof body.detail === "string"
              ? body.detail
              : JSON.stringify(body.detail);
        }
      } else {
        const text = await response.text();
        if (text) message = text;
      }
    } catch {
      // keep fallback message
    }

    throw new Error(message);
  }

  return response.json() as Promise<T>;
}

function buildQuery(params: Record<string, string | number | boolean | undefined | null>) {
  const searchParams = new URLSearchParams();

  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") return;
    searchParams.set(key, String(value));
  });

  const query = searchParams.toString();
  return query ? `?${query}` : "";
}

export function getHives(): Promise<HiveListResponse> {
  return httpGet<HiveListResponse>("/api/hives");
}

export function getOverview(): Promise<HiveOverviewResponse> {
  return httpGet<HiveOverviewResponse>("/api/hives/overview");
}

export function getLatest(hiveId: number): Promise<LatestResponse> {
  const query = buildQuery({
    hive_id: hiveId,
  });

  return httpGet<LatestResponse>(`/api/latest${query}`);
}

export function getStatus(hiveId: number): Promise<HiveStatus> {
  return httpGet<HiveStatus>(`/api/hives/${hiveId}/status`);
}

export function getAllStatuses(): Promise<StatusListResponse> {
  return httpGet<StatusListResponse>("/api/status");
}

export function getSnapshot(hiveId: number): Promise<SnapshotResponse> {
  return httpGet<SnapshotResponse>(`/api/hives/${hiveId}/snapshot`);
}

export function getRecentHistory(
  hiveId: number,
  limit = 300
): Promise<HistoryResponse> {
  const query = buildQuery({
    limit,
  });

  return httpGet<HistoryResponse>(`/api/hives/${hiveId}/history${query}`);
}

export function getRangeHistory(
  hiveId: number,
  fromIso: string,
  toIso: string,
  limit = 5000
): Promise<HistoryResponse> {
  const query = buildQuery({
    hive_id: hiveId,
    ts_from: fromIso,
    ts_to: toIso,
    limit,
  });

  return httpGet<HistoryResponse>(`/api/history${query}`);
}

export function getAlerts(
  hiveId: number,
  activeOnly = true,
  limit = 200,
  fromIso?: string,
  toIso?: string
): Promise<AlertsResponse> {
  const query = buildQuery({
    active_only: activeOnly,
    limit,
    ts_from: fromIso,
    ts_to: toIso,
  });

  return httpGet<AlertsResponse>(`/api/hives/${hiveId}/alerts${query}`);
}
