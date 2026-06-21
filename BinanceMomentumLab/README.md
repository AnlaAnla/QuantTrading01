# BinanceMomentumLab

Binance USDⓈ-M USDT 永续合约异常动量监控实验室。当前提供公开 REST 扫描、分路由
WebSocket 实时行情、本地订单簿、原始 Parquet 事件、特征与策略状态机，以及完全本地的
PaperBroker、风险管理和浏览器监控面板；不包含任何真实或 Demo 下单实现。

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

## 特征与策略状态

新候选先由公开 REST Kline 预热，再由实时事件增量计算 1m/3m/5m/15m 收益、5m
成交额与 Z-Score、taker buy ratio、CVD 与斜率、事件锚定 VWAP、5m OI 变化、funding、
basis、spread、前 5/20 档盘口不平衡以及相对 BTC 的残差收益。OI 使用公开的
`GET /fapi/v1/openInterest` 定期采样。

策略状态依次描述 `NORMAL`、`WATCH`、`IGNITION`、`PULLBACK`、`CONTINUATION`、
`DISTRIBUTION`、`BREAKDOWN`、`COOLDOWN`。状态机只生成可解释的 LONG/SHORT 研究
信号，不连接任何 Broker。信号包含稳定 ID、参考入场/止损、完整特征快照、结构化
`reason_codes` 和结构化失效条件。同一历史事件序列会生成完全相同的状态与信号 ID。

所有数值阈值均位于 `.env.example` 和 `Settings`，百分比使用百分点单位，例如 3%
配置为 `3`。

## PaperBroker 与风险管理

`PAPER` 模式可以把研究信号交给本地 RiskManager 和 PaperBroker。系统绝不调用 Binance
账户或下单接口。开仓前会检查每日亏损、连续亏损冷却、行情新鲜度、WebSocket 健康、
订单簿同步、点差、最大仓位数和紧急停止；仓位按账户权益、单笔风险比例及信号止损距离
计算，并受最大名义金额限制。

撮合只使用订单延迟结束时可见的 bookTicker 和本地订单簿档位，不读取未来事件，也不以
K 线最高价/最低价推定成交。成交价先按可见档位计算 VWAP，再叠加由波动率、点差和订单
名义金额驱动的逆向滑点；每次成交均扣除手续费。Funding 只在明确的 funding 事件发生时
入账。

入场后生成 reduce-only 止损和止盈，另有时间止损；紧急停止只允许 reduce-only 平仓。
任何已有仓位都会阻止新的入场，部分入场后若先触发止损，会优先平仓并取消尚未成交的
入场余量，防止对亏损仓加仓。订单、成交、持仓和账户权益曲线写入 DuckDB。

离线测试包含三套固定行情：点火—回调—继续上涨、点火后立即失败、高位派发—跌破
VWAP—反抽失败。每套都验证状态、订单、成交和净盈亏。

## 浏览器监控面板

FastAPI 首页使用原生 HTML、CSS 和 JavaScript，不需要 React 或 Node 构建链。页面通过
`/ws/dashboard` 接收初始快照和按顶层数据域拆分的增量更新，展示系统健康、候选币、
特征、状态机、信号、模拟订单与成交、仓位、盈亏、权益曲线、最大回撤、胜率、
Profit Factor、手续费和错误日志。

`/api/paper/pause` 与 `/api/paper/resume` 控制是否允许新开仓。紧急停止和重置模拟账户
需要在浏览器中输入“确认”，后端也会独立校验请求体中的 `confirm=true`。公共配置接口和
面板数据均不返回 API Key 或 API Secret。

## 官方接口依据

- [USDⓈ-M Futures General Info](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info)
- [Exchange Information](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information)
- [24hr Ticker Price Change Statistics](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/24hr-Ticker-Price-Change-Statistics)
- [Kline/Candlestick Data](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data)
- [Open Interest](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest)
- [WebSocket Market Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams)
- [Aggregate Trade Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams)
- [Liquidation Order Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams)
- [Local Order Book](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)

## 第一阶段以外

真实交易和 DEMO 下单适配均未实现。PaperBroker 只存在于本地进程与 DuckDB 中，不得
被改造成真实订单适配器。
