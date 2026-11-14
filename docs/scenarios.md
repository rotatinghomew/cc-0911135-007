# 业务演练

全部使用本地合成数据，不访问外网。`npm run drill` 会依次运行下表情景，
每个情景都通过真实 HTTP 交互，并用 `lib/verify.mjs` 复核产物；
`npm test` 中也包含同一组断言。

|情景|流与历史设置|验收点|
|---|---|---|
|小字节块|`chunkSize=1`、逐字符切分|UTF-8、CRLF、多行 data 均正确；心跳与 notice 不落事件|
|半帧断开|`cutOnce=true`|未完成事件不确认；重连 `Last-Event-ID` 为已提交值；审计记 `reconnect.cut`|
|重复|`duplicate=true`|相同 ID 同内容只提交一次；`eventsDuplicate` 增长；无冲突|
|冲突|`duplicate=true`、`conflict=true`|通道转 `needs-attention`；后写不覆盖；`events.jsonl` 无副本内容|
|保留窗外|`floor=6`（已提交到 3）|409 后回补 4..5，上界固定为 `resumeFloor-1`；从 `evt-5` 接回实时流；最终连续到 12|
|多页回补|`floor=10`（已提交到 4）|回补 5..9 跨两页；`via=history` 标记 5..9，`via=live` 标记 10..12；从 `evt-9` 接回|
|上界超出可服务范围|`floor=10`、`tail=8`（已提交到 2）|3..8 可补部分仍补齐；序号 9 超出 `observedTail`，标记不可补齐并转 `needs-attention`|
|历史缺口|`missing=3`、`floor=6`（已提交到 2）|提交停在 2；4、5 留在缓冲；缺口 3 标记不可补齐；状态 `needs-attention`|
|慢接收方|`bufferLimit=3`、`commitDelayMs=4`|`maxBuffered <= bufferLimit`；`backpressure > 0`；`buffer-discard` 有审计；最终 40 条连续|
|尾部推进|补采期间 `tail` 12→20|`recovery-start.until` 固定为 5；新增尾部由实时流补齐到 20|
|停机恢复|先跑一次（停在 15），扩展尾部后同目录续跑|第二次 `started.resumeEventId` 为 `evt-15`；事件 1..30 无重复无跳号|

补充的自动化验证（`test/`）：

|测试文件|覆盖|
|---|---|
|`sse.test.mjs`|1/2/3/5/7/13/64 字节切分结果一致；CR/LF 跨块；BOM；NUL；半帧；多行 data|
|`collector.test.mjs`|上述 11 个情景 + 崩溃半行修复 + 原始字节离线重放 + 状态自洽|
|`collector-unit.test.mjs`|缺口按原因分段、补齐后收缩、缓冲上限拒绝、提交串行、重复/冲突判定、区间合并|
|`recovery.test.mjs`|已接收≠已持久化；提交途中断电修复；跨进程续接；`receivedSequence` 语义；多页回补来源标记；回补上界超出可服务范围|
|`verify.test.mjs`|验收器能拒绝跨缺口、越限、未合并缺口、冲突未转状态等情况|
|`fixture.test.mjs`|网关协议本身（心跳、409、历史分页、半帧、冲突帧）|

## 手工复核

```bash
# 1. 启动演练网关（打印 URL）
node -e 'import("./src/site.mjs").then(async ({createSite}) => { const s = await createSite(0, { tail: 12, floor: 6 }); console.log(s.url); process.on("SIGINT", async () => { await s.close(); process.exit(0); }); })'

# 2. 另一个终端采集
npm start -- --source http://127.0.0.1:端口 --output var/tap

# 3. Ctrl-C 后复核
npm run verify -- var/tap/stream-status.json --with-events
tail -1 var/tap/audit.jsonl

# 4. 原始字节重放
npm run replay -- var/tap/raw/conn-000001.sse --chunk 1
```

演练网关的 `/_control` 可以随时改变 `tail`、`floor`、`missing`、
`duplicate`、`conflict`、`cutOnce`、`chunkSize`、`perConnection`、
`slowWriteMs`，用来复现上表任一情景。
