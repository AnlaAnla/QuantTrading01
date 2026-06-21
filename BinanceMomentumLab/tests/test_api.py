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
    assert health.json()["websocket"] == "not_implemented_phase_one"
    assert "secret" not in str(public_config.json()).lower()


def test_paper_reset_is_explicitly_unavailable_and_emergency_stop_works() -> None:
    app = create_app(Settings(_env_file=None, database_path=":memory:"), start_scanner=False)

    with TestClient(app) as client:
        assert client.post("/api/paper/reset").status_code == 501
        assert client.post("/api/emergency-stop").json() == {"emergency_stop": True}
        assert client.get("/api/health").json()["emergency_stop"] is True
