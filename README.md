# instrument-event-tap

仪器事件网关及有限回放历史接口，提供原始 SSE 字节探针。

## 环境与命令

Node.js 22.23.4，依赖使用 package-lock.json 固定。准备阶段运行 `npm ci`。


```bash
npm run build
npm test
npm run demo
npm start
```

`npm start` 输出本次监听地址，另一个终端可运行 `npm run probe -- http://127.0.0.1:端口`。服务只监听本机。`PORT` 可指定端口，Ctrl-C 结束。

`npm run build` 检查所有模块的 JavaScript 语法，无需生成打包文件；`npm test` 检查源端协议、正常及异常响应。

## 数据与运行边界

GET /stream 接收 Last-Event-ID: evt-N；初次相当于 evt-0。样品事件为 event: observation，data 为 JSON，sequence 连续且 eventId 稳定。注释心跳和 event: notice 不是样品事件，notice 用多行 data 演练 SSE 拼接。每次响应有限结束，客户端可继续连接；演练到 observedTail 后可主动停止。

409 给 missingFrom、missingUntil、resumeFloor、observedTail。GET /history?from=N&until=M&offset=0 每页三条，nextOffset 为 null 时结束，from/until 固定补采界限。POST /_control 允许 tail 增长、floor 推进、missing 指定历史缺条、duplicate 重放、conflict 重复 ID 换内容、cutOnce 半帧断开、chunkSize 调整分块（应为正整数）。

events.jsonl 每行一个已持久提交 observation。stream-status.json：committedSequence、committedEventId、receivedSequence、gaps 数组、state（running/stopped/needs-attention）、maxBuffered、bufferLimit。连续提交不能跨过 gaps，收到 notice 不能更新确认点。

接口的完整字段及演练开关见 `docs/protocol.md`。业务编号均为合成值，不对应真实研究材料；服务没有外网依赖。`/_audit` 查看源端请求证据，`/_control` 只用于运行演练情景，不属于业务采集入口。

现有客户端只进行单次协议查看；尚不提供自动采集与持久运行调度。原始服务可单独运行，以便采集端在重启时保持源端状态。

## 数据校验

`npm run verify -- stream-status.json` 检查本地结果的基本业务不变量，缺少文件时返回非零状态。该命令不生成采集结果，也不替代网络交互和恢复测试。`npm run demo` 在临时本地服务上真实发起请求并完整打印响应，随后退出。日常演练均不需要安装依赖或访问研究站。
