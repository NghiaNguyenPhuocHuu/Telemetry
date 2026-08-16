import logging
from typing import Set
from fastapi import WebSocket

logger = logging.getLogger(__name__)

class ConnectionManager:
    """Manages active WebSocket connections for real-time telemetry broadcasting."""

    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"Client connected. Total active connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"Client disconnewcted. Total active connections: {len(self.active_connections)}")

    async def broadcast(self, data: dict):
        """Broadcasts a telemetry payload to all connected clients."""
        if not self.active_connections:
            return
            
        disconnected = set()
        for connection in self.active_connections:
            try:
                await connection.send_json(data)
            except Exception:
                disconnected.add(connection)

        # Clean up broken connections
        for dead_connection in disconnected:
            self.active_connections.remove(dead_connection)

# Global manager instance
manager = ConnectionManager()