from fastapi.testclient import TestClient

from binance_momentum_lab.api.app import create_app
from binance_momentum_lab.config import Settings


def test_health_and_public_config_are_safe() -> None:
    settings = Settings(_env_file=None, database_path=":memory:", binance_api_secret="never-show")
    app = create_app(settings, start_scanner=False)

    with TestClient(app) as client:
        health = client.get("/api/health")
        public_config = client.get("/api/config/public")

    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert health.json()["websocket"] == "disabled_for_test"
    assert "secret" not in str(public_config.json()).lower()
    assert public_config.json()["strategy_thresholds"]["max_spread_bps"] == "8"
    assert public_config.json()["paper_risk"]["network_latency_ms"] == 100


def test_paper_reset_and_emergency_stop_work_locally() -> None:
    app = create_app(Settings(_env_file=None, database_path=":memory:"), start_scanner=False)

    with TestClient(app) as client:
        assert client.post("/api/paper/reset", json={"confirm": False}).status_code == 400
        assert client.post("/api/emergency-stop", json={"confirm": False}).status_code == 400
        assert client.post("/api/paper/pause").json() == {"entries_paused": True}
        assert client.get("/api/health").json()["entries_paused"] is True
        assert client.post("/api/paper/resume").json() == {"entries_paused": False}
        assert client.post("/api/paper/reset", json={"confirm": True}).json() == {"reset": True}
        assert client.post("/api/emergency-stop", json={"confirm": True}).json() == {
            "emergency_stop": True
        }
        assert client.get("/api/health").json()["emergency_stop"] is True
        assert client.get("/api/orders").json() == []
        assert client.get("/api/trades").json() == []
        assert client.get("/api/positions").json() == []
        assert client.get("/api/performance").json()["equity"] == "10000"


def test_dashboard_snapshot_and_incremental_websocket_are_secret_free() -> None:
    settings = Settings(
        _env_file=None,
        database_path=":memory:",
        binance_api_key="abcd12345678wxyz",
        binance_api_secret="top-secret-value",
    )
    app = create_app(settings, start_scanner=False)

    with TestClient(app) as client:
        response = client.get("/api/dashboard")
        with client.websocket_connect("/ws/dashboard") as websocket:
            initial = websocket.receive_json()
            patch = websocket.receive_json()

    assert response.status_code == 200
    assert initial["type"] == "snapshot"
    assert initial["data"]["system"]["mode"] == "PAPER"
    assert patch["type"] == "patch"
    assert patch["revision"] == 2
    serialized = str(response.json()) + str(initial)
    assert "top-secret-value" not in serialized
    assert "abcd12345678wxyz" not in serialized
