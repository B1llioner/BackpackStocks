import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from fastapi.testclient import TestClient

from backpack_stocks.api import create_app
from backpack_stocks.models import Policy, Settings


def test_authenticated_api_control_is_idempotent(tmp_path):
    replay = tmp_path / "replay.json"
    replay.write_text(json.dumps({"frames": [{"offset_s": 0, "ask": "100", "dex_bid": "1"}]}))
    config = Settings(
        database=str(tmp_path / "api.sqlite3"),
        replay_file=str(replay),
        policy=Policy(withdrawal_semantics="deduct"),
    )
    token = "test-token-for-api-only-123456"
    app = create_app(config, token=token)
    headers = {"Authorization": "Bearer " + token, "Idempotency-Key": "request-1"}
    with TestClient(app) as client:
        assert client.get("/v1/status").status_code == 401
        assert client.get("/v1/status", headers=headers).status_code == 200
        assert client.post("/v1/stop", headers={"Authorization": "Bearer " + token}).status_code == 400
        first = client.post("/v1/stop", headers=headers)
        assert first.status_code == 200 and first.json()["stopped"]
        assert client.post("/v1/stop", headers=headers).json() == first.json()
        assert not client.post("/v1/resume", headers=headers).json()["stopped"]
        assert client.get("/v1/events", headers=headers).status_code == 200
        negative = client.get("/v1/spread", headers=headers)
        assert negative.status_code == 409
        assert negative.json() == {"detail": "No positive spread after costs"}
        assert client.post("/v1/sign", headers=headers, json={"transaction": "anything"}).status_code == 404


def test_single_public_spread_uses_the_same_rfq_gate_as_multi_asset_reports():
    monitor = SimpleNamespace(report=Mock(return_value={"rows": []}), request_refresh=Mock())
    engine = SimpleNamespace(monitor=monitor, scan=AsyncMock(), tick=AsyncMock())

    @asynccontextmanager
    async def factory(settings):
        yield engine

    token = "test-token-for-api-only-123456"
    app = create_app(Settings(), runtime_factory=factory, token=token)
    with TestClient(app) as client:
        response = client.get("/v1/spread", headers={"Authorization": "Bearer " + token})
        assert response.status_code == 409 and "RFQ-validated" in response.json()["detail"]
        monitor.report.assert_called_once_with("WEN.US")
        monitor.request_refresh.assert_called_once_with("WEN.US")
        engine.scan.assert_not_called()
