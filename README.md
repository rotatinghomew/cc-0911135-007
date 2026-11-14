# instrument-event-tap

仪器检测网关的长期采集通道：从 SSE 实时流接收样品事件，逐条持久化到
`events.jsonl`，用 `stream-status.json` 区分**已接收**、**已持久化**和
**已经连续覆盖**的进度。

原始网关、有限回放历史接口和演练控制面仍保留在本仓库，便于离线复核与故障演练。

## 环境与命令

Node.js 22.23.4，无第三方运行时依赖（`package-lock.json` 为空依赖树）。

```bash
npm ci          # 可选；本仓库没有外部依赖
npm run build   # 所有模块语法检查
npm test        # 单元 + 集成测试（含全部演练情景）
npm run drill   # 独立跑 9 个演练情景并逐项验收
npm run demo    # 真实采集一次，打印状态、审计与原始字节重放
npm start       # 长期采集（需 --source）
npm run verify -- var/tap/stream-status.json --with-events
```

### 接收命令（可停止并续接）

```bash
node src/main.mjs --source http://127.0.0.1:8080 --output var/tap
```

|选项|默认|说明|
|---|---|---|
|`--source <url>`|必填|网关地址|
|`--output <dir>`|`var/tap`|`events.jsonl`、`stream-status.json`、`audit.jsonl`、`raw/`|
|`--buffer <n>`|256|有界缓冲上限（条）|
|`--commit-delay <ms>`|0|人为放慢持久化，演练慢接收方|
|`--retain <n>`|20000|内存中保留的已提交内容索引条数（用于去重/冲突）|
|`--max-connections <n>`|不限|最多连接次数（演练用）|
|`--idle-stop <ms>`|0|连续空闲该时长后停止（演练用，0 表示不停）|
|`--reconnect-delay <ms>`|100|重连间隔|
|`--stall-timeout <ms>`|20000|背压等待上限，超时断线重连|

`Ctrl-C` / SIGINT / SIGTERM 会先让在途提交完成 fsync，再写出 `stopped`
状态并退出；退出码 `0` 表示正常停止（可续接）、`3` 表示停在
`needs-attention`（需人工处置）、`1` 表示致命错误。

再次用同一 `--output` 运行即自动续接：启动时读取 `events.jsonl` 的最后一条
**连续**记录，以它的 `evt-N` 作为第一次连接的 `Last-Event-ID`。
`events.jsonl` 尾部若残留崩溃时的半行，会在启动时被截断，并记录到审计日志。

## 三种进度互不替代

`stream-status.json` 里：

- `receivedSequence`：本次运行从**实时流**解析到的最大序号，可能尚未落盘；
- `committedSequence` / `contiguousThrough`：`events.jsonl` 中已 fsync 的
  **最大连续前缀**，也是续接游标；
- `nextExpectedSequence`：下一个唯一可提交的序号；
- `gaps`：已接收却被空洞挡住、尚未覆盖的区间（`from > committedSequence`）；
- `pendingCount` / `bufferedSequences`：有界缓冲中等待提交或等待补采的记录；
- `unfillable`：历史接口也无法补齐的区间，出现即将状态转入 `needs-attention`。

正常运行时 `receivedSequence >= committedSequence`；`receivedSequence`
只会因重复或乱序略微领先，不会把空洞“盖住”。收到注释心跳或 `notice`
不会推进任何进度。

## 正确性规则

1. **字节流解析**：SSE 按 WHATWG 规则增量解析。切分可以落在 UTF-8
   多字节字符、CRLF 的 CR 与 LF 之间、多行 `data` 之间；只有遇到空行
   才派发事件；未遇到空行的半帧整体丢弃。以 `:` 开头的注释是心跳。
2. **提交顺序**：只提交 `committedSequence + 1`。中间有洞时，洞后的记录
   留在有界缓冲，绝不提前提交。`events.jsonl` 因而总是严格递增的连续序列。
3. **重复与冲突**：已提交的 `eventId` 内容相同则忽略（计入
   `counters.eventsDuplicate`）；`eventId` 相同但业务内容不同（或同序号
   不同内容）则判为冲突：后来者一律**不写入**，状态转 `needs-attention`。
   已提交内容不会被后写覆盖。
4. **409 与回补**：网关回 `409` 时给出 `missingFrom`、`missingUntil`、
   `resumeFloor`、`observedTail`。接收端丢弃未提交的缓冲（它们会被回放），
   按分页 `GET /history?from=&until=&offset=` 回补到
   **固定上界 `resumeFloor - 1`**。补采期间实时尾部继续增长也不扩大本轮回补
   上界；连续提交到上界后，才以该上界的 `evt-N` 接回实时流，由实时流回放
   新尾部。绝不用“当前最大序号”当游标来掩盖中间空洞。
5. **无法补齐**：历史接口明确缺失、分页取不回、或补采失败留下的空洞，
   记为不可补齐并转 `needs-attention`，停止接回实时流等待人工处置。
6. **有界缓冲与背压**：缓冲区满时，连接在读取下一帧前等待空位，
   使 TCP 窗口收缩、网关 `write()` 返回 `false`——这是对源端的真实背压，
   同时计入 `counters.backpressure` 与审计日志。等待超过
   `--stall-timeout` 则主动断线，从**已提交游标**重连。被丢弃的只是
   未确认的缓冲，并会在 `audit.jsonl` 中记录 `buffer-discard`；不是静默丢弃。
7. **停机与崩溃**：每次提交在返回前 `fsync`。状态文件用临时文件 + `fsync`
   + `rename` 原子替换。任何时刻崩溃，最多丢掉未提交的缓冲，
   这些数据会由网关回放。

## 产物

`events.jsonl`：每行一条已持久提交的 observation，依次为
`sequence`、`eventId`、业务字段，以及本地元数据
`source`、`via`（`live`/`history`）、`connection`、`receivedAt`、
`committedAt`、`digest`（业务字段的稳定摘要，用于去重/冲突判定）。

`stream-status.json`：上面的进度与计数、缺口、冲突、回补状态。

`audit.jsonl`：按时间追加的生命周期事件——`started`、`connect`、
`commit`、`duplicate-*`、`conflict`、`gap-unfillable`、`backpressure`、
`buffer-discard`、`recovery-*`、`reconnect`、`stopped`。验收时用它证明
“何时断线、从哪个游标重连、丢弃了什么、补采用了哪个上界”。

`raw/conn-NNNNNN.sse` + `.meta.json`：每次连接的原始响应字节。可用

```bash
npm run replay -- var/tap/raw/conn-000001.sse --chunk 1
npm run replay -- var/tap/raw/conn-000001.sse --chunk 1 --mode char --events
```

离线重放。重放不经过业务状态：它证明保存的字节本身能重新解析出完整帧；
若录制尾部是半帧，退出码为 `1`，且该半帧**不会**出现在事件里。

## 数据校验

```bash
npm run verify -- var/tap/stream-status.json --with-events
```

独立复核 `stream-status.json` 的不变量，并用同目录 `events.jsonl`
交叉核对：事件条数、序号从 1 连续递增、`eventId` 与序号一致、
提交点等于条数。它也拒绝“跨过缺口的提交点”“缓冲峰值越限”、
“缺口未合并”“有缺口却报告 stopped”“有冲突却未转需处理”等状态。

## 演练

`npm run drill` 用本地合成网关跑完整矩阵，每个情景都走真实网络交互并
用上面的验收器复核：

|情景|设置|验收点|
|---|---|---|
|`small-chunks`|`chunkSize=1`、逐字符切分|UTF-8、CRLF、多行 data 完整；心跳与 notice 不落事件|
|`half-frame`|`cutOnce`|半帧不确认；重连 `Last-Event-ID` 等于已提交值|
|`duplicate`|`duplicate`|相同 ID 同内容只提交一次|
|`conflict`|`duplicate`、`conflict`|冲突副本不写入；状态 `needs-attention`|
|`floor-window`|`floor=6`|409 后回补 4..5（上界固定），从 `evt-5` 接回实时流|
|`history-gap`|`missing=3`、`floor=6`|提交停在 2；4、5 留在缓冲；缺口 3 不可补齐|
|`floor-multipage`|`floor=10`（已提交到 4）|跨两页回补 5..9；`via=history` 标记；从 `evt-9` 接回|
|`beyond-floor`|`floor=10`、`tail=8`|3..8 补齐；9 超出 `observedTail`，标记不可补齐并停止|
|`tail-growth`|补采期间 tail 由 12 涨到 20|补采上界固定为 5；新增尾部由实时流补齐到 20|
|`slow-consumer`|`bufferLimit=3`、慢持久化|峰值 ≤ 上限；背压断线有审计；最终连续补齐|
|`restart-resume`|先跑一次再续跑|第二次从第一次的提交点续接，无重复无跳号|

`npm run demo` 另行演示 5 个阶段（含原始字节重放），可用
`--keep` 把产物留在 `var/demo/` 供 `npm run verify` 复核。

## 网关与协议

`GET /stream` 接收 `Last-Event-ID: evt-N`，初次相当于 `evt-0`。
`event: observation` 的 `data` 是 JSON，`sequence` 连续，`eventId` 稳定。
注释心跳与 `event: notice` 不是样品事件；`notice` 用多行 data 演练拼接。
每次响应有限结束，客户端可继续连接。

`GET /history?from=N&until=M&offset=0` 每页三条，`nextOffset` 为 `null`
时结束。`POST /_control` 可调整演练开关：

|开关|含义|
|---|---|
|`tail`|当前最大序号|
|`floor`|保留窗下界；游标低于 `floor-1` 时回 409|
|`missing`|历史接口跳过的单个序号|
|`duplicate`|每次连接重放首个可用事件|
|`conflict`（配合 `duplicate`）|重放副本内容不同|
|`cutOnce`|下一次连接在半帧处断开|
|`chunkSize`|字节块大小（正整数）|
|`subdivide`|是否按 UTF-8 字符/CRLF 继续切分|
|`perConnection`|每次连接最多回放多少条|
|`slowWriteMs`|每块写出前等待的毫秒数|

`/_audit` 查看源端收到的请求证据；`/_control` 仅用于演练，不属于业务入口。
所有业务编号均为合成值，服务没有外网依赖。

## 目录

```
src/main.mjs      CLI：解析参数、信号处理、退出码
src/runtime.mjs   连接/重连/409/背压循环与停机收尾
src/connect.mjs   单次 /stream 连接：解析、录制、背压等待
src/sse.mjs       增量 SSE 字节解析器
src/collector.mjs 提交顺序、去重、冲突、缺口、有界缓冲、回补
src/store.mjs     追加式事件日志、原子状态、审计日志、原始录制
src/history.mjs   历史分页读取
src/site.mjs      演练网关（SSE、历史、控制面）
lib/verify.mjs    独立验收器
tools/drill.mjs   演练矩阵
tools/replay.mjs  原始字节重放
tools/demo.mjs    阶段演示
```

`docs/protocol.md` 记录协议字段，`docs/scenarios.md` 是演练对照表。
