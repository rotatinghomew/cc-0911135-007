# 协议与产物

## 网关接口

### GET /stream

请求头 `Last-Event-ID: evt-N`（缺省相当于 `evt-0`）。

- 正常：`200 text/event-stream`。帧格式

  ```
  id: evt-1\r\n
  event: observation\r\n
  data: {"sequence":1,"eventId":"evt-1","sample":"M-1","value":0.1,"label":"籽粒镍"}\r\n
  \r\n
  ```

  `sequence` 连续，`eventId` 稳定且等于 `evt-<sequence>`。
  注释行（`: heartbeat`）是心跳；`event: notice` 用多行 data 演练 SSE
  拼接。两者都不是样品事件，不推进任何确认点。
- 游标过旧：`409 application/json`

  ```json
  { "missingFrom": 2, "missingUntil": 5, "resumeFloor": 6, "observedTail": 12 }
  ```

  含义：`[missingFrom, missingUntil]` 已滑出保留窗，必须先用历史接口补齐，
  再从 `resumeFloor`（即 `evt-5`）接回实时流。`observedTail` 是 409 时刻
  的尾部，仅作参考，**不是**本轮补采上界。

每次响应有限结束。客户端以已持久提交的游标继续连接。

### GET /history?from=N&until=M&offset=0

返回 `{ events, nextOffset, from, until, total }`。每页最多三条；
`nextOffset` 为 `null` 表示结束。`from`/`until` 在整个补采过程中固定。
`missing` 演练开关会让某个序号**不出现在任何页**，从而制造不可补齐的缺口。

### POST /_control

合并演练开关，返回完整状态。见 README 的开关表。仅用于演练。

### GET /_audit

源端收到的请求证据：路径、方法、`Last-Event-ID`、时间戳。

## 接收端产物

### events.jsonl

每行一条已持久提交的 observation，字段顺序为原始业务字段在前，
本地元数据在后：

|字段|说明|
|---|---|
|`sequence`|源端连续序号|
|`eventId`|稳定 ID，`evt-<sequence>`|
|`sample`、`value`、`label`|业务字段（合成值）|
|`source`|固定 `gateway`|
|`via`|`live` 实时流，`history` 历史回补|
|`connection`|实时流来源连接序号；回补为 `null`|
|`receivedAt`|首次收到该事件的本地时间|
|`committedAt`|完成 fsync 的本地时间|
|`digest`|业务字段（除 `source`/`via`/`connection`/`receivedAt`/`committedAt`/`digest` 外）的稳定摘要|

`digest` 用于识别重复与冲突：同一 `eventId` 内容相同则跳过；不同则不写入
并转 `needs-attention`。

文件保证：序号从 1 连续递增、无重复、无跳号；启动时会截断崩溃残留的
半行。因此“第 N 行”就是 `sequence` 为 N 的记录。

### stream-status.json

```jsonc
{
  "version": 1,
  "generatedAt": "...",
  "startedAt": "...",
  "state": "running | stopped | needs-attention",
  "committedSequence": 12,          // 已 fsync 的最大连续序号
  "committedEventId": "evt-12",
  "committedCount": 12,
  "contiguousThrough": 12,          // 与 committedSequence 相同
  "nextExpectedSequence": 13,
  "receivedSequence": 14,           // 本次运行实时解析到的最大序号
  "receivedThisRun": 14,
  "pendingCount": 0,
  "bufferedSequences": [],
  "gaps": [],                       // 未覆盖区间，from > committedSequence
  "bufferLimit": 256,
  "maxBuffered": 3,
  "conflicts": [],                  // { eventId, sequence, reason, at }
  "unfillable": [],                 // 历史接口也补不到的区间
  "recovery": {                     // 最近一次回补
    "active": false, "reason": "保留窗外",
    "from": 4, "until": 5, "pages": 1, "repaired": 2,
    "observedTail": 12, "resumeFloor": 6, "resumeAt": 5
  },
  "counters": {
    "eventsReceived": 14, "eventsCommitted": 12, "eventsDuplicate": 2,
    "invalidFrames": 0, "connections": 3, "reconnects": 3,
    "backpressure": 0, "historyFetched": 2
  },
  "lastError": null
}
```

不变量（`npm run verify` 复核）：

- `committedEventId === "evt-" + committedSequence`；
- `receivedSequence >= committedSequence`；
- `contiguousThrough === committedSequence`，`nextExpectedSequence === committedSequence + 1`；
- `0 <= maxBuffered <= bufferLimit`；
- 每个 `gap.from > committedSequence`、`gap.until >= gap.from`，且缺口之间已合并；
- 有未覆盖缺口时 `state !== "stopped"`；
- 有 `conflicts` 或 `unfillable` 时 `state === "needs-attention"`；
- 配合 `events.jsonl` 时：条数等于 `committedCount`、第 N 条序号为 N、
  `committedSequence` 等于条数。

### audit.jsonl

按时间追加，每行 `{ at, type, ... }`。关键类型：

|type|含义|
|---|---|
|`started`|进程启动；`resumeSequence`/`resumeEventId` 为续接游标，`resumed` 表示是否续跑|
|`connect`|发起一次连接；`lastEventId` 是发送的游标|
|`commit` / `commit-history`|提交一条记录（实时 / 回补）|
|`duplicate-committed` / `duplicate-buffered`|重复到达|
|`conflict`|同 ID 或同序号内容冲突|
|`floor-409`|收到 409 及其四个字段|
|`recovery-start` / `history-page` / `recovery-done`|回补边界与分页证据|
|`gap-unfillable`|缺口无法补齐|
|`backpressure` / `buffer-discard`|达到上限、丢弃未确认缓冲|
|`reconnect`|断线原因与重连前游标|
|`recovered-partial-line`|启动时截断的崩溃残留字节数|
|`stopped`|停止原因与最终游标|

### raw/conn-NNNNNN.sse

单次连接收到的原始字节，逐字保存，可按任意粒度重放：

```bash
node tools/replay.mjs var/tap/raw/conn-000001.sse --chunk 1
node tools/replay.mjs var/tap/raw/conn-000001.sse --chunk 1 --mode char --events
```

退出码 `1` 表示录制尾部是半帧；这些字节不会被当作完整事件。
