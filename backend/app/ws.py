from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from typing import Dict, Optional, Set

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class WSManager:
    """
    Manage websocket clients grouped by hive ID.

    Main responsibilities:
    - register clients when they connect
    - remove clients when they disconnect
    - broadcast live updates to all clients of one hive
    - clean up dead sockets automatically
    - run an optional heartbeat loop
    """

    def __init__(self) -> None:
        # Maps each hive_id to the set of connected websocket clients
        self.clients_by_hive: Dict[int, Set[WebSocket]] = {}

        # Async lock protects shared websocket client state
        self._lock = asyncio.Lock()

        # Background heartbeat task
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._heartbeat_running = False

    async def connect(
        self,
        hive_id: int,
        ws: WebSocket,
        initial_payload: Optional[dict] = None,
    ) -> None:
        """
        Accept a websocket connection and register it under the given hive.

        If an initial payload is provided, send it immediately after connect.
        """
        hive_id = int(hive_id)
        await ws.accept()

        async with self._lock:
            self.clients_by_hive.setdefault(hive_id, set()).add(ws)

        if initial_payload is not None:
            try:
                await ws.send_json(initial_payload)
            except Exception:
                logger.exception(
                    "Failed to send initial websocket payload for hive %s",
                    hive_id,
                )
                await self.disconnect(hive_id, ws)

    async def disconnect(self, hive_id: int, ws: WebSocket) -> None:
        """
        Remove a websocket client from the hive subscription group.

        If the hive has no remaining clients, remove the hive entry too.
        """
        hive_id = int(hive_id)

        async with self._lock:
            clients = self.clients_by_hive.get(hive_id)
            if clients is not None:
                clients.discard(ws)
                if not clients:
                    self.clients_by_hive.pop(hive_id, None)

        with suppress(Exception):
            await ws.close()

    async def broadcast(self, hive_id: int, message: dict) -> None:
        """
        Broadcast one JSON message to all clients subscribed to a hive.

        Any client that fails during send is treated as dead and removed.
        """
        hive_id = int(hive_id)

        async with self._lock:
            clients = list(self.clients_by_hive.get(hive_id, set()))

        if not clients:
            return

        async def send_one(ws: WebSocket):
            try:
                await ws.send_json(message)
                return None
            except Exception:
                return ws

        dead_clients = await asyncio.gather(*(send_one(ws) for ws in clients))

        for dead_ws in dead_clients:
            if dead_ws is not None:
                await self.disconnect(hive_id, dead_ws)

    async def broadcast_point(self, hive_id: int, point_payload: dict) -> None:
        """
        Broadcast the latest twin point for one hive.
        """
        await self.broadcast(
            int(hive_id),
            {
                "type": "point",
                "hive_id": int(hive_id),
                "point": point_payload,
            },
        )

    async def broadcast_status(self, hive_id: int, status_payload: dict) -> None:
        """
        Broadcast the latest hive status for one hive.
        """
        await self.broadcast(
            int(hive_id),
            {
                "type": "status",
                "hive_id": int(hive_id),
                "status": status_payload,
            },
        )

    async def broadcast_alerts(self, hive_id: int, alerts_payload: list[dict]) -> None:
        """
        Broadcast the latest active alerts for one hive.
        """
        await self.broadcast(
            int(hive_id),
            {
                "type": "alerts",
                "hive_id": int(hive_id),
                "alerts": alerts_payload,
            },
        )

    async def heartbeat(self, hive_id: int) -> None:
        """
        Send a heartbeat message to all clients of one hive.
        """
        await self.broadcast(
            int(hive_id),
            {
                "type": "heartbeat",
                "hive_id": int(hive_id),
            },
        )

    async def heartbeat_all(self) -> None:
        """
        Send heartbeat messages to all subscribed hives.
        """
        async with self._lock:
            hive_ids = list(self.clients_by_hive.keys())

        for hive_id in hive_ids:
            await self.heartbeat(hive_id)

    async def start_heartbeat_loop(self, interval_seconds: float = 20.0) -> None:
        """
        Start a background task that periodically sends heartbeat messages.
        """
        if self._heartbeat_running:
            return

        self._heartbeat_running = True

        async def runner():
            try:
                while self._heartbeat_running:
                    await asyncio.sleep(interval_seconds)
                    await self.heartbeat_all()
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Heartbeat loop crashed")

        self._heartbeat_task = asyncio.create_task(runner())

    async def stop_heartbeat_loop(self) -> None:
        """
        Stop the background heartbeat task.
        """
        self._heartbeat_running = False

        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with suppress(Exception):
                await self._heartbeat_task
            self._heartbeat_task = None

    async def subscriber_count(self, hive_id: int) -> int:
        """
        Return the number of websocket clients subscribed to one hive.
        """
        async with self._lock:
            return len(self.clients_by_hive.get(int(hive_id), set()))

    async def total_subscribers(self) -> int:
        """
        Return the total number of connected websocket clients.
        """
        async with self._lock:
            return sum(len(clients) for clients in self.clients_by_hive.values())

    async def subscribed_hives(self) -> list[int]:
        """
        Return the sorted list of hive IDs that currently have subscribers.
        """
        async with self._lock:
            return sorted(self.clients_by_hive.keys())