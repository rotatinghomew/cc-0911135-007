import { SseParser } from './sse.mjs';
import { BackpressureError } from './collector.mjs';

export class RetryableError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = 'RetryableError';
    Object.assign(this, details);
  }
}

// 与 /stream 的一次连接。它只负责：发字节给解析器、把完整业务事件交给采集端、
// 把原始字节同步写进录制文件。断线与半帧不会在这里被包装成成功。
//
// 背压在这里可见：缓冲区满了就不再 read()，TCP 窗口随之收缩，网关
// 的 write() 会返回 false —— 这就是对源端的真实背压。等待超过上限
// 则主动断开，由运行层按已提交游标重连。
export async function runConnection({
  base, lastEventId, collector, capture, signal, timeoutMs, logger, stallTimeoutMs = 30000,
}) {
  const url = new URL('/stream', base);
  const abort = new AbortController();
  const onAbort = () => abort.abort();
  signal?.addEventListener('abort', onAbort, { once: true });
  const timer = timeoutMs ? setTimeout(() => abort.abort(new RetryableError('连接超时')), timeoutMs) : null;
  const parser = new SseParser();
  let frames = 0;
  let events = 0;
  let cut = false;

  try {
    const response = await fetch(url, {
      headers: lastEventId ? { 'last-event-id': lastEventId } : {},
      signal: abort.signal,
    });
    if (response.status === 409) {
      let body = null;
      try { body = await response.json(); } catch { /* 非 JSON 也按 409 处理 */ }
      return { kind: 'floor', status: 409, body };
    }
    if (!response.ok) throw new RetryableError(`/stream 返回 ${response.status}`);
    const contentType = response.headers.get('content-type') || '';
    if (!contentType.includes('text/event-stream')) {
      throw new RetryableError(`/stream 不是 SSE：${contentType}`);
    }

    const reader = response.body.getReader();
    for (;;) {
      const room = await collector.waitForRoom({ timeoutMs: stallTimeoutMs });
      if (room === 'stalled') {
        throw new BackpressureError(collector.bufferLimit, collector.pending.size);
      }
      if (room === 'stopped') return { kind: 'stopped', frames, events };

      const { done, value } = await reader.read();
      if (done) break;
      if (capture) await capture.write(value);
      parser.feed(value);
      for (let next = parser.next(); next; next = parser.next()) {
        frames += 1;
        if (capture) capture.countFrame();
        if (next.type !== 'observation') continue;
        events += 1;
        let value;
        try {
          value = JSON.parse(next.data);
        } catch {
          collector.counters.invalidFrames += 1;
          await collector.audit.append('invalid-event', { reason: 'JSON 解析失败', sample: next.data.slice(0, 200) });
          continue;
        }
        if (next.id && value.eventId !== next.id) {
          collector.counters.invalidFrames += 1;
          await collector.audit.append('invalid-event', {
            reason: 'id 与 eventId 不一致', id: next.id, eventId: value.eventId,
          });
          continue;
        }
        collector.receive(value, { via: 'live', connection: capture?.connection ?? null });
      }
    }
  } catch (error) {
    if (error instanceof BackpressureError) throw error;
    if (abort.signal.aborted && abort.signal.reason instanceof RetryableError) throw abort.signal.reason;
    if (error.name === 'AbortError') throw new RetryableError('连接被中止', { aborted: true, cut: true });
    cut = true;
    // 对端在帧中间断开时，fetch 会以 terminated / ECONNRESET 等形式抛错。
    // 半帧内容留在 parser 里，绝不派发。
    throw new RetryableError(`读取中断：${error.message}`, { cut: true });
  } finally {
    if (timer) clearTimeout(timer);
    signal?.removeEventListener('abort', onAbort);
  }

  const partial = parser.partialText();
  if (partial.trim()) cut = true;
  return { kind: cut ? 'cut' : 'ended', frames, events, partial: partial.length };
}
