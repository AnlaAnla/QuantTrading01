#!/usr/bin/env bash
set -euo pipefail
uv sync --python 3.12
if [[ ! -f .env ]]; then cp .env.example .env; fi
uv run python -m binance_momentum_lab.cli init-db

