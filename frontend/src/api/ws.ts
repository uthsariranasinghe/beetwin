import type { LiveMessage } from "./types";

/* =========================================================
   WebSocket Base URL
   Uses environment variable when deployed
   ========================================================= */

const WS_BASE =
  import.meta.env.VITE_WS_BASE || "wss://believable-flexibility-production-77c5.up.railway.app";

/* =========================================================
   Connect to live hive stream
   ========================================================= */

export function connectHiveLive(
  hiveId: number,
  onMessage: (msg: LiveMessage) => void,
  onOpen?: () => void,
  onClose?: () => void,
  onError?: (ev: Event) => void
): WebSocket {
  let ws: WebSocket;
  let reconnectTimer: number | null = null;

  function connect() {
    ws = new WebSocket(`${WS_BASE}/api/live?hive_id=${hiveId}`);

    ws.onopen = () => {
      console.log("Live connection opened");
      onOpen?.();
    };

    ws.onmessage = (event) => {
      try {
        const parsed = JSON.parse(event.data) as LiveMessage;
        onMessage(parsed);
      } catch (err) {
        console.error("Failed to parse websocket message", err);
      }
    };

    ws.onerror = (ev) => {
      console.error("WebSocket error", ev);
      onError?.(ev);
    };

    ws.onclose = () => {
      console.warn("Live connection closed");

      onClose?.();

      // attempt reconnection
      if (!reconnectTimer) {
        reconnectTimer = window.setTimeout(() => {
          reconnectTimer = null;
          console.log("Reconnecting live stream...");
          connect();
        }, 3000);
      }
    };
  }

  connect();

  return ws!;
}
