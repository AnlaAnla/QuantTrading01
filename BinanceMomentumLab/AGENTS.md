# AGENTS.md

## Scope

This repository is a safety-first market-data research system. The REST and WebSocket market-data
layers contain no execution path. Never add real-money order placement, withdrawal, or transfer
behavior without a separately approved phase and explicit safety review. LIVE mode must remain
hard-disabled.

The separately approved DEMO adapter may only use `https://demo-fapi.binance.com` and
`wss://fstream.binancefuture.com`. It must never accept or derive a production trading endpoint.

## Engineering rules

- Target Python 3.12 and keep strict type checking green.
- Use `Decimal` for prices, quantities, quote volume, balances, and monetary calculations.
- Keep internal timestamps timezone-aware and in UTC.
- Keep market data, strategy, risk, and execution boundaries separate.
- Strategy output is research-only: structured signals must never submit or imply orders.
- PaperBroker must remain local-only and must never share code paths with authenticated Binance
  order endpoints.
- Paper fills may only use contemporaneous bookTicker/order-book liquidity after configured
  latency; never infer fills from future candles, highs, or lows.
- All exits are reduce-only. Never average down or reopen from an unfilled entry remainder after a
  protective exit.
- Keep every strategy threshold in `Settings` and `.env.example`.
- Preserve deterministic replay: never use wall-clock time or random IDs in feature/state logic.
- Inject network clients so tests remain offline and deterministic.
- Never log secrets. Public configuration, dashboard payloads, and browser pages must not expose
  API keys or API secrets, including masked values.
- Keep the dashboard dependency-free: native HTML, CSS, and JavaScript only. Dangerous paper
  controls require confirmation in both the browser and the API request.
- Add focused tests with every behavior change.
- Run `ruff check .`, `ruff format --check .`, `mypy`, and `pytest` before handoff.
