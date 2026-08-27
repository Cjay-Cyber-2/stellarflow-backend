import pytest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import WebSocket
from app.websockets.manager import ConnectionManager, handle_websocket_session


@pytest.fixture
def mock_websocket():
    ws = AsyncMock(spec=WebSocket)
    ws.accept = AsyncMock()
    ws.send_text = AsyncMock()
    ws.send_json = AsyncMock()
    ws.receive_text = AsyncMock()
    return ws


@pytest.fixture
def manager():
    mgr = ConnectionManager(redis_url="redis://localhost:6379")
    mgr.pubsub = MagicMock()
    mgr.pubsub.subscribe = AsyncMock()
    mgr.pubsub.unsubscribe = AsyncMock()
    return mgr


@pytest.mark.asyncio
async def test_connect_and_disconnect(manager, mock_websocket):
    await manager.connect(mock_websocket)
    assert mock_websocket in manager.client_channels
    assert len(manager.client_channels[mock_websocket]) == 0

    manager.disconnect(mock_websocket)
    assert mock_websocket not in manager.client_channels


@pytest.mark.asyncio
async def test_subscribe_and_unsubscribe(manager, mock_websocket):
    await manager.connect(mock_websocket)
    channel = "subscribe:pool:XLM-USDC"

    # Subscribe
    manager.subscribe(mock_websocket, channel)
    assert channel in manager.subscriptions
    assert mock_websocket in manager.subscriptions[channel]
    assert channel in manager.client_channels[mock_websocket]

    # Unsubscribe
    manager.unsubscribe(mock_websocket, channel)
    assert channel not in manager.subscriptions
    assert channel not in manager.client_channels[mock_websocket]


@pytest.mark.asyncio
async def test_broadcast_to_channel(manager, mock_websocket):
    await manager.connect(mock_websocket)
    channel = "subscribe:pool:XLM-USDC"
    manager.subscribe(mock_websocket, channel)

    payload = {"price": 0.125, "volume": 15000}
    await manager.broadcast_to_channel(channel, payload)

    mock_websocket.send_text.assert_called_once_with(
        json.dumps({"channel": channel, "data": payload})
    )


@pytest.mark.asyncio
async def test_heartbeat_disconnects_stale_client(mock_websocket):
    # Mock WebSocket that fails on send_json during heartbeat ping
    mock_websocket.send_json.side_effect = Exception("Connection lost")
    mock_websocket.receive_text.side_effect = asyncio.CancelledError()

    with patch("app.websockets.manager.HEARTBEAT_INTERVAL", 0.01):
        with patch("app.websockets.manager.manager.connect", new_callable=AsyncMock) as mock_conn, \
             patch("app.websockets.manager.manager.disconnect") as mock_disc:
            
            try:
                await handle_websocket_session(mock_websocket)
            except asyncio.CancelledError:
                pass

            # Verify stale connection disconnect call was triggered
            mock_disc.assert_called_with(mock_websocket)