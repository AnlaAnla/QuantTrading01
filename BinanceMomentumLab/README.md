# BinanceMomentumLab

Binance USDⓈ-M USDT 永续合约异常动量监控实验室。当前提供公开 REST 扫描、分路由
WebSocket 实时行情、本地订单簿、原始 Parquet 事件和健康检查；不包含任何真实或模拟
下单实现。

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

## 实时市场数据

候选集合变化会动态重建两条独立的 combined-stream 连接：

- `/market`：`aggTrade`、`markPrice@1s`、`forceOrder`、`kline_1m`
- `/public`：`bookTicker`、`depth@100ms`

连接自动响应服务端 ping，并以低频客户端 ping 验证反向链路；在 Binance 的 24 小时
强制断开前主动轮换。断线使用带随机抖动的指数退避。接收队列有界，满载时通过 await
施加背压。

本地订单簿先缓存 diff depth，再获取 `/fapi/v1/depth` 快照，以 `lastUpdateId` 对齐首个
`U/u` 区间；后续严格要求 `pu` 等于上一事件的 `u`。序列中断立即降级健康状态并重新
获取快照。数量为零的档位会被删除。

原始事件按 `date=YYYY-MM-DD/symbol=...` 写入 Parquet。`forceOrder` 只表示每个币种
每 1000ms 窗口中的最大爆仓订单快照，不能当作完整爆仓成交量。

## 官方接口依据

- [USDⓈ-M Futures General Info](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info)
- [Exchange Information](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information)
- [24hr Ticker Price Change Statistics](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/24hr-Ticker-Price-Change-Statistics)
- [Kline/Candlestick Data](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data)
- [WebSocket Market Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams)
- [Aggregate Trade Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams)
- [Liquidation Order Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams)
- [Local Order Book](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)

## 第一阶段以外

实时特征、状态机、PaperBroker、风控和 DEMO 下单适配均未实现。这些能力不得从现有
健康状态误判为已完成。
