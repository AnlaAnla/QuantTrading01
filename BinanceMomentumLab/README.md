# BinanceMomentumLab

Binance USDⓈ-M USDT 永续合约异常动量监控实验室。第一阶段只提供公开行情采集、
全市场扫描、DuckDB 初始化和健康检查；不包含任何真实或模拟下单实现。

## 安全边界

- `MONITOR` 与 `PAPER` 是第一阶段仅有的可启动模式。
- `DEMO` 仅保留配置，启动会明确失败。
- `LIVE` 永久硬禁用，即使 `LIVE_TRADING_ENABLED=true` 也会明确失败。
- 当前代码不包含下单、提现、划转或签名交易请求。
- API Secret 不进入日志、API 响应或数据库。

## 快速开始

要求 Python 3.12；推荐使用 [uv](https://docs.astral.sh/uv/)。

Windows PowerShell：

```powershell
cd BinanceMomentumLab
.\scripts\init.ps1
.\scripts\run.ps1
```

Linux / WSL：

```bash
cd BinanceMomentumLab
bash scripts/init.sh
bash scripts/run.sh
```

打开 <http://127.0.0.1:8000>，健康检查位于
<http://127.0.0.1:8000/api/health>。

## 测试与质量门禁

```powershell
.\scripts\test.ps1
```

```bash
bash scripts/test.sh
```

脚本依次运行 `ruff check`、`ruff format --check`、`mypy` 和 `pytest`。所有测试均使用
离线 fixture，不依赖 Binance 实时网络。

## 扫描逻辑

扫描器按配置周期读取 `exchangeInfo` 与全市场 `24hr ticker`，仅保留
`status=TRADING`、`quoteAsset=USDT`、`contractType=PERPETUAL` 的合约。通过 24h
成交额与涨幅预筛后，受控并发拉取 1m Kline，计算最近 5 分钟涨幅与 5 分钟成交额
相对过去 60 个非重叠 5 分钟窗口的总体 Z-Score，最终按 Z-Score 和涨幅排序并截断。

阈值全部来自环境配置，详见 `.env.example`。

## 官方接口依据

- [USDⓈ-M Futures General Info](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info)
- [Exchange Information](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information)
- [24hr Ticker Price Change Statistics](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/24hr-Ticker-Price-Change-Statistics)
- [Kline/Candlestick Data](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data)

## 第一阶段以外

WebSocket 动态订阅、本地订单簿、实时特征、状态机、PaperBroker、风控和 DEMO 适配均未
在本阶段实现。这些能力不得从现有健康状态误判为已完成。
