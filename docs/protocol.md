GET /stream 接收 Last-Event-ID: evt-N；初次相当于 evt-0。样品事件为 event: observation，data 为 JSON，sequence 连续且 eventId 稳定。注释心跳和 event: notice 不是样品事件，notice 用多行 data 演练 SSE 拼接。每次响应有限结束，客户端可继续连接；演练到 observedTail 后可主动停止。

409 给 missingFrom、missingUntil、resumeFloor、observedTail。GET /history?from=N&until=M&offset=0 每页三条，nextOffset 为 null 时结束，from/until 固定补采界限。POST /_control 允许 tail 增长、floor 推进、missing 指定历史缺条、duplicate 重放、conflict 重复 ID 换内容、cutOnce 半帧断开、chunkSize 调整分块（应为正整数）。

events.jsonl 每行一个已持久提交 observation。stream-status.json：committedSequence、committedEventId、receivedSequence、gaps 数组、state（running/stopped/needs-attention）、maxBuffered、bufferLimit。连续提交不能跨过 gaps，收到 notice 不能更新确认点。
