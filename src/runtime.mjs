import path from 'node:path';
import { mkdir } from 'node:fs/promises';
import { EventLog, StatusWriter, JsonlLog, RawCapture } from './store.mjs';
import { Collector, BackpressureError, FatalError } from './collector.mjs';
import { createHistory, HistoryError } from './history.mjs';
import { runConnection, RetryableError } from './connect.mjs';

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

/**
 * 长期采集通道的运行循环。
 *
 * 每次连接都从“已持久提交”的 Last-Event-ID 开始；本地内存里的未提交缓冲
 * 在断线时一律丢弃，由源端回放补齐。这样任何时刻崩溃都不会造成
 * “确认了但没落盘”的静默丢失。
 *
 * 收到 409 时先按固定上界回补历史，再以回补后的连续提交点接回实时流。
 */
export async function runTap(options = {}) {
  const base = options.base;
  if (!base) throw new Error('缺少网关地址');
  const output = options.output ?? 'var/tap';
  const eventsPath = path.join(output, 'events.jsonl');
  const statusPath = path.join(output, 'stream-status.json');
  const auditPath = path.join(output, 'audit.jsonl');
  const rawDirectory = path.join(output, 'raw');

  const logger = options.logger ?? (value => console.log(JSON.stringify(value)));
  const maxConnections = options.maxConnections ?? Infinity;
  const reconnectDelay = options.reconnectDelay ?? 100;
  const idleStop = options.idleStop ?? 0;
  const bufferLimit = options.bufferLimit ?? 256;
  const connectTimeout = options.connectTimeout ?? 20000;
  const stallTimeout = options.stallTimeout ?? 20000;
  const commitDelayMs = options.commitDelayMs ?? 0;
  const maximumRecoveries = options.maxRecoveries ?? 200;

  await mkdir(output, { recursive: true });
  const log = await EventLog.open(eventsPath, { retain: options.retain });
  const statusWriter = new StatusWriter(statusPath);
  await statusWriter.removeTemporary();
  const audit = await JsonlLog.open(auditPath);
  const history = createHistory(base, { timeoutMs: options.historyTimeout ?? 15000 });

  const collector = new Collector({
    log, history, statusWriter, audit,
    bufferLimit, retain: options.retain, commitDelayMs,
  });

  const abort = new AbortController();
  let stopping = false;
  let captured = 0;
  let recoveries = 0;
  let idleSince = Date.now();

  const requestStop = reason => {
    if (stopping) return;
    stopping = true;
    collector.stopReason = reason;
    abort.abort();
  };
  const onSignal = () => requestStop('signal');
  if (options.signal) {
    if (options.signal.aborted) requestStop('signal');
    else options.signal.addEventListener('abort', onSignal, { once: true });
  }

  // 每次运行都是同一份日志的续写；已有记录时续接点就是最后一条连续记录。
  const resumed = collector.committedSequence > 0 || log.count > 0;

  if (log.truncatedBytes) await audit.append('recovered-partial-line', { bytes: log.truncatedBytes });
  await collector.flushStatus({ force: true });
  await audit.append('started', {
    base,
    resumed,
    resumeSequence: collector.committedSequence,
    resumeEventId: collector.committedEventId,
    bufferLimit,
    truncatedBytes: log.truncatedBytes,
  });

  try {
    for (let attempt = 0; !stopping && attempt < maxConnections; attempt += 1) {
      await collector.settle().catch(() => {});
      if (stopping) break;
      if (idleStop > 0 && Date.now() - idleSince >= idleStop) break;

      collector.counters.connections += 1;
      const lastEventId = collector.committedEventId;
      await collector.flushStatus({ force: true });
      await audit.append('connect', {
        attempt, lastEventId, sequence: collector.committedSequence,
      });

      const capture = await RawCapture.open(rawDirectory, ++captured, {
        base, lastEventId, sequence: collector.committedSequence,
      });

      let ended;
      let failure = null;
      try {
        ended = await runConnection({
          base, lastEventId, collector, capture,
          signal: abort.signal, timeoutMs: connectTimeout, stallTimeoutMs: stallTimeout,
        });
      } catch (error) {
        if (error instanceof BackpressureError) ended = { kind: 'backpressure', dropped: null };
        else failure = error;
        ended ??= { kind: 'error' };
      }

      if (stopping) {
        await capture.close({ status: 'stopped' });
        break;
      }

      if (failure instanceof HistoryError) throw failure;

      if (ended.kind === 'floor') {
        recoveries += 1;
        if (recoveries > maximumRecoveries) throw new FatalError('回补次数超过上限');
        const { missingFrom, missingUntil, resumeFloor, observedTail } = ended.body ?? {};
        await capture.close({ status: 'floor', body: ended.body });
        await audit.append('floor-409', { missingFrom, missingUntil, resumeFloor, observedTail });
        // 回补上界必须来自 409，而不是“当前最大的序号”：后者可能把洞盖住。
        const floorBound = Number.isInteger(resumeFloor) ? resumeFloor - 1 : missingUntil;
        if (!Number.isInteger(floorBound) || floorBound < 1) throw new FatalError('409 缺少可用的回补上界', ended.body);
        // 历史接口只能服务到 observedTail；超出部分无论怎样都补不到，
        // 先明确记为不可补齐，避免把可补的部分也一起放弃。
        const upper = Number.isInteger(observedTail) ? Math.min(floorBound, observedTail) : floorBound;
        if (floorBound > upper) {
          collector.markUnfillable(upper + 1, floorBound, '超出历史接口可服务范围', { observedTail, resumeFloor });
        }
        collector.discardPending('409 需要先回补历史');
        let result;
        try {
          result = await collector.recover({
            until: upper, resumeFloor, observedTail, reason: '保留窗外',
          });
        } catch (error) {
          // 历史接口不可用（例如上界超出其可服务范围），或回补本身触发背压：
          // 当前缺口无法安全补齐，转入需处理并停止，不能假装已恢复。
          await audit.append('recovery-error', { error: error.message, upper });
          requestStop('needs-attention');
          break;
        }
        await audit.append('recovery-result', {
          ...result, resumeCursor: collector.resumeCursor(), canResumeLive: collector.canResumeLive(),
        });
        if (!collector.canResumeLive()) {
          // 存在无法补齐的缺口：停止接回实时流，转入需处理状态等待人工处置。
          requestStop('needs-attention');
          break;
        }
        idleSince = Date.now();
        await sleep(reconnectDelay);
        continue;
      }

      if (ended.kind === 'backpressure') {
        const dropped = collector.discardPending('背压断线');
        await capture.close({ status: 'backpressure', dropped });
        collector.counters.reconnects += 1;
        await audit.append('reconnect', {
          reason: 'backpressure', lastEventId: collector.committedEventId, dropped,
        });
        idleSince = Date.now();
        await collector.settle().catch(() => {});
        await sleep(reconnectDelay);
        continue;
      }

      if (failure) {
        const kind = failure.cut ? 'cut' : 'error';
        await capture.close({ status: kind, error: failure.message });
        collector.counters.reconnects += 1;
        await audit.append(kind === 'cut' ? 'reconnect' : 'connection-error', {
          reason: kind, message: failure.message,
          lastEventId, aborted: Boolean(failure.aborted),
        });
        idleSince = Date.now();
        await sleep(reconnectDelay);
        continue;
      }

      // 正常结束、半帧断开、连接被中止：都从已提交游标重连。
      await capture.close({
        status: ended.kind, frames: ended.frames, events: ended.events,
        partialBytes: ended.partial ?? 0,
      });
      collector.counters.reconnects += 1;
      await audit.append('reconnect', {
        reason: ended.kind, lastEventId: collector.committedEventId,
        frames: ended.frames, events: ended.events, partialBytes: ended.partial ?? 0,
      });
      await collector.settle().catch(() => {});
      if (ended.events > 0) idleSince = Date.now();
      await sleep(reconnectDelay);
    }
  } finally {
    options.signal?.removeEventListener('abort', onSignal);
    await collector.settle().catch(() => {});
    await collector.stop(stopping ? (collector.stopReason === 'needs-attention' ? 'needs-attention' : 'stopped') : 'finished');
    await log.close();
    await audit.close();
  }

  const summary = {
    committedSequence: collector.committedSequence,
    committedEventId: collector.committedEventId,
    receivedSequence: collector.receivedSequence,
    contiguousThrough: collector.committedSequence,
    gaps: collector.gaps,
    state: collector.state,
    committedCount: log.count,
    output,
  };
  logger({ type: 'summary', ...summary });
  return summary;
}
