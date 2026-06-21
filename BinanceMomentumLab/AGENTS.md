# AGENTS.md

## Scope

This repository is a safety-first market-data research system. Phase one contains no execution
path. Never add real-money order placement, withdrawal, or transfer behavior without a separately
approved phase and explicit safety review. LIVE mode must remain hard-disabled.

## Engineering rules

- Target Python 3.12 and keep strict type checking green.
- Use `Decimal` for prices, quantities, quote volume, balances, and monetary calculations.
- Keep internal timestamps timezone-aware and in UTC.
- Keep market data, strategy, risk, and execution boundaries separate.
- Inject network clients so tests remain offline and deterministic.
- Never log secrets. API keys may only be shown as first four and last four characters.
- Add focused tests with every behavior change.
- Run `ruff check .`, `ruff format --check .`, `mypy`, and `pytest` before handoff.

