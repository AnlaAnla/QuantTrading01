$ErrorActionPreference = "Stop"
uv run uvicorn binance_momentum_lab.api.app:create_app --factory --host 127.0.0.1 --port 8000

