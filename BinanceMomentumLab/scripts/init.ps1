$ErrorActionPreference = "Stop"
uv sync --python 3.12
if (-not (Test-Path -LiteralPath ".env")) {
    Copy-Item -LiteralPath ".env.example" -Destination ".env"
}
uv run python -m binance_momentum_lab.cli init-db

