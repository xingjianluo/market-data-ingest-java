# 本地金融行情事件摄取与聚合链路

本工程在 Spring Boot 3 / Java 21 骨架上实现了一条**完全本地**的行情事件摄取与聚合链路：
多源并发接收、幂等去重、事件时间窗口聚合、乱序/超期判定、结构演进兼容、断点恢复与全量重放。
所有状态保存在本地 JSON 快照与 JSONL 事件日志中，**不依赖任何外部服务、消息中间件或真实凭据**。

## 1. 目录结构

```
src/main/java/com/github/xingjianluo/marketdataingestjava/
├── protocol/   # 规范化事件、判定结论、拒绝原因枚举
├── codec/      # 接入协议解码器（v1/v2 兼容、缺省语义、非法识别）
├── window/     # 事件时间滚动窗口与迟到策略
├── aggregate/  # 单窗口 OHLC/成交量聚合与线程安全聚合表
├── dedup/      # (source,eventId) 幂等去重 + 最后位点判定缓存
├── offset/     # 每来源连续位点跟踪（回退/跳跃/重投识别）
├── state/      # 本地原子 JSON 快照
├── log/        # 本地 JSONL 事件日志（原始信封 + 判定结论）
├── engine/     # 摄取核心：解码→位点→去重→迟到→聚合→持久化
├── replay/     # 全量重放与逐条结论/聚合对账
├── config/     # Spring 本地装配
└── web/        # HTTP 接入接口
```

## 2. 摄取协议（本地 JSON）

`POST /api/ingest`，请求体为一条 JSON 信封：

```json
{
  "source": "A",
  "seq": 0,
  "eventId": "A-0",
  "schemaVersion": "v2",
  "payload": {
    "symbol": "BTCUSDT",
    "eventTime": 1700000000000,
    "price": 101.5,
    "size": 2,
    "side": "BUY"
  }
}
```

| 字段 | 要求 | 说明 |
|---|---|---|
| `source` | 必填，非空字符串 | 行情源标识，幂等键的一部分 |
| `seq` | 必填，非负整数（允许数字字符串） | **来源内**位点，从 0 开始、必须连续递增 |
| `eventId` | 必填，非空字符串 | 源内事件唯一标识，幂等键的一部分 |
| `schemaVersion` | 必填，当前支持 `v1`、`v2` | 不支持的版本明确拒绝 |
| `payload` | 必填 JSON 对象 | 业务负载 |
| `payload.symbol` | 必填，非空字符串 | 标的代码，聚合维度 |
| `payload.eventTime` | 必填，非负整数（允许数字字符串） | 事件时间，epoch millis |
| `payload.price` | 必填，非负数值（允许数字字符串） | 成交价 |
| `payload.size` | 必填，非负整数（允许整数字符串） | 成交量 |
| `payload.side` | 可选，`BUY`/`SELL` | **v2 新增字段**，见第 4 节 |

成功返回 `200`；任何“未计入”的事件返回 `422`，响应体仍是结构化判定结论，例如：

```json
{
  "accepted": false,
  "reason": "LATE",
  "source": "A",
  "seq": 2,
  "eventId": "A-2",
  "symbol": "BTCUSDT",
  "windowStart": 0,
  "watermark": 10000,
  "detail": "事件时间 7999 早于拒绝阈值 8000（watermark=10000, allowedLateness=2000），窗口 0 已封存"
}
```

其余接口：

- `GET /api/aggregations`：查询水位线、各来源已提交位点与全部窗口聚合（OHLC、总量、买卖/未知分量、笔数）。
- `POST /api/replay`：在独立的 `<stateDir>/replay/` 目录上做全量重放并返回对账报告。

## 3. 去重、迟到与位点判定规则

处理顺序固定，任何“未计入”都落到一种**互斥**原因上（`RejectReason`）：

1. **结构解码 `INVALID_EVENT`**：版本不支持、必填字段缺失、类型不匹配。
   不静默丢弃：返回明确原因；只要信封里的 `source/seq` 可信，位点照常推进（以拒绝结论占位），
   来源不会被一条坏消息卡死。缺少可信 `source/seq` 时只记录拒绝、无法推进位点。
2. **位点检查**
   - 新来源首条事件的 `seq` 必须是 0，否则按跳跃处理；
   - `seq == 已提交位点+1`：正常推进；
   - `seq == 已提交位点`：最后位点重投（at-least-once 重试），返回该位点上缓存的**原始判定**——
     原本被接受的事件重投得到幂等处理、原本被拒绝的事件重放得到相同拒绝原因；
   - `seq < 已提交位点`：`OFFSET_BACKWARD`，**位点回退被明确拒绝**，不会“从中间继续”；
   - `seq > 期望位点`：`OFFSET_GAP`，存在漏投，拒绝并提示补齐或全量重放。
3. **幂等去重 `DUPLICATE`**：幂等键为 `(source, eventId)`。
   即使重复事件使用新的连续位点、不同价格/成交量，也只判定为重复，**绝不重复计入**，但位点推进。
4. **迟到判定 `LATE`**：
   - 滚动窗口长度 `windowSizeMillis`，事件归属窗口起点为
     `floor(eventTime / windowSizeMillis) * windowSizeMillis`；
   - 水位线（watermark）= 所有**已接收**事件中观察到的最大事件时间，单调不减；
   - 当 `eventTime < watermark - allowedLatenessMillis` 时判定超期，所属窗口视为已封存，拒绝并入；
   - 未超过允许迟到窗口的乱序事件正常并入既有窗口。
5. **接受**：更新聚合、推进水位线（若需要）、推进位点。

聚合结果与事件**到达顺序无关**：`open/close` 按事件时间排序确定，同一事件时间以
`(source, eventId)` 字典序稳定打破平局；因此正序、逆序、乱序、并发提交，同一事件集合
必然得到同一聚合结果（可复现）。

默认窗口/迟到参数可在 `application.properties` 覆盖：

```properties
market.ingest.window-size-millis=60000
market.ingest.allowed-lateness-millis=5000
market.ingest.state-dir=data/ingest
```

## 4. 结构演进兼容范围

- **新增字段缺省语义确定且稳定**：`side` 是 v2 相对 v1 新增的可选字段。
  - v1 事件无 `side`：解码结果**稳定**为 `UNKNOWN`；
  - v2 事件缺省或显式为 `null`：同样为 `UNKNOWN`；
  - v2 显式 `BUY`/`SELL` 正常解析；取值非法（如 `WHALE`）属于结构非法，明确拒绝。
  - 聚合中分别累计 `buyVolume / sellVolume / unknownVolume`，v1 与 v2 事件在同一结果下可复现地合并。
- **前向宽容**：在受支持版本（v1/v2）内出现的未知字段一律忽略，不影响聚合。
- **不做静默兼容的变更**：不支持的 `schemaVersion`、必填字段缺失、字段类型不匹配
  （如 `price:"abc"`、`eventTime:"later"`、`size` 为小数/负数）一律以 `INVALID_EVENT` 拒绝并给出原因。
- 后续若引入 v3，应在解码器中显式登记版本与字段缺省策略，而非依赖隐式容错。

## 5. 中断恢复与全量重放

- 每条事件处理结束后都会：追加一行事件日志（原始信封 + 判定结论），并以
  “写临时文件 + 原子 move”的方式覆盖保存 `snapshot.json`（位点、水位线、幂等键、
  最后位点判定缓存、全部窗口聚合）。崩溃时只会留下旧快照或完整新快照。
- **断点续处理**：进程重启后从快照恢复，从各来源 `已提交位点+1` 继续。
  重放旧位点 → `OFFSET_BACKWARD` 明确拒绝；旧事件套上新位点 → 幂等键去重，不重复累计。
- **全量重放**：`ReplayService` 在**空状态**的新引擎（独立 `replay/` 目录，不触碰现网快照）上
  按事件日志顺序重放全部原始信封，并：
  - 逐条比对重放结论与首次处理结论（accepted/reason/source/seq/eventId/symbol/window/watermark）；
  - 对最终聚合表做逐窗口 OHLC/成交量/笔数对账；
  - 任一条不一致都会在日志与报告中显式标出。空状态起步 + 幂等键/位点保护保证**不重不漏**。

本地文件布局：

```
<state-dir>/
├── snapshot.json        # 原子快照（断点恢复）
├── events.jsonl         # 原始信封 + 判定结论（全量重放与对账）
└── replay/              # 全量重放的临时工作目录
    ├── snapshot.json
    └── events.jsonl
```

## 6. 验证方法

全部测试只依赖 JUnit 5 与 Spring Test，无外部服务：

```bash
mvn test
```

测试重点对应需求中的三类场景，且日志打印关键事件、处理位点与判定依据：

- `EventCodecTest`：v1/v2 缺省语义一致、未知字段宽容、缺字段/类型不匹配/版本不支持均被拒绝且原因明确。
- `IngestEngineCoreTest`：重复不重复计入、窗口内乱序并入、超期拒绝、位点回退/跳跃拒绝、
  非法事件推进位点，以及五类判定原因互斥可区分。
- `ConcurrentDedupTest`：多源在公共线程池并发提交、同一事件被多次并发重投，
  验证只接受一次、其余全部 `DUPLICATE`，聚合计数/成交量确定。
- `OutOfOrderDeterminismTest`：同一事件集合按正序、逆序、两种随机洗牌提交，
  OHLC 与分量成交量完全一致（顺序无关、可复现）。
- `RecoveryReplayTest`：从快照重启后位点/水位线/聚合完整恢复，续处理不重不漏，
  位点回退被拒；全量重放逐条结论与聚合对账一致，且重复重放仍然一致。
- `IngestWebTest`：HTTP 200/422 与结构化 `DUPLICATE` 原因可被调用方区分。

手工验证（启动后另开终端）：

```bash
mvn spring-boot:run

curl -s -X POST localhost:8080/api/ingest -H 'Content-Type: application/json' -d '{
  "source":"A","seq":0,"eventId":"A-0","schemaVersion":"v2",
  "payload":{"symbol":"BTCUSDT","eventTime":1000,"price":100,"size":2,"side":"BUY"}}'

curl -s localhost:8080/api/aggregations
curl -s -X POST localhost:8080/api/replay
```
