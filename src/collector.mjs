import { digestOf, validateObservation } from './store.mjs';

export class BackpressureError extends Error {
  constructor(limit, buffered) {
    super(`缓冲区达到上限 ${limit}`);
    this.name = 'BackpressureError';
    this.limit = limit;
    this.buffered = buffered;
  }
}

export class FatalError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = 'FatalError';
    this.details = details;
  }
}

const now = () => new Date().toISOString();
export const eventIdFor = sequence => `evt-${sequence}`;

export function mergeRanges(ranges) {
  const sorted = ranges.filter(item => item.until >= item.from)
    .sort((a, b) => a.from - b.from || a.until - b.until);
  const merged = [];
  for (const item of sorted) {
    const last = merged.at(-1);
    if (last && item.from <= last.until + 1) {
      last.until = Math.max(last.until, item.until);
      if (item.unfillable) last.unfillable = true;
      if (last.unfillable) last.reason = item.reason ?? last.reason;
    } else {
      merged.push({ ...item });
    }
  }
  return merged;
}

/**
 * 采集状态机。
 *
 * 三种进度互不替代：
 *   - receivedSequence：本次运行从实时流解析到的最大序号（可能尚未落盘）；
 *   - committedSequence / contiguousThrough：events.jsonl 中已 fsync 的最大连续前缀；
 *   - gaps：已接收却被空洞挡住、尚未覆盖的区间。
 *
 * 接收与持久化解耦：receive() 只登记进有界缓冲区并安排后台提交；
 * 提交串行推进 committedSequence，永远不跨过缺口。
 */
export class Collector {
  constructor(options) {
    this.log = options.log;
    this.history = options.history;
    this.statusWriter = options.statusWriter;
    this.audit = options.audit;
    this.bufferLimit = options.bufferLimit;
    this.retain = options.retain ?? 20000;
    this.commitDelayMs = options.commitDelayMs ?? 0;
    this.logger = options.logger || (() => {});

    const last = this.log.last;
    this.committedSequence = last ? last.sequence : 0;
    this.committedEventId = last ? last.eventId : eventIdFor(0);
    this.receivedSequence = this.committedSequence;

    this.seen = new Map();
    for (const record of this.log.recent) {
      this.seen.set(record.eventId, { sequence: record.sequence, digest: record.digest ?? digestOf(record) });
    }

    this.pending = new Map();
    this.roomWaiters = [];
    this.unfillable = Array.isArray(options.unfillable) ? options.unfillable.map(item => ({ ...item })) : [];
    this.gaps = [];
    this.conflicts = Array.isArray(options.conflicts) ? options.conflicts.map(item => ({ ...item })) : [];
    this.state = options.state === 'needs-attention' ? 'needs-attention' : 'running';
    this.lastError = options.lastError ?? null;
    this.maxBuffered = 0;
    this.recovery = {
      active: false, reason: null, from: null, until: null, pages: 0, repaired: 0, resumeAt: null,
    };
    this.counters = {
      eventsReceived: 0,
      eventsCommitted: 0,
      eventsDuplicate: 0,
      invalidFrames: 0,
      connections: 0,
      reconnects: 0,
      backpressure: 0,
      historyFetched: 0,
      ...options.counters,
    };
    this.startedAt = now();
    this.lastStatusAt = 0;
    this.statusChain = Promise.resolve();
    this.drainChain = Promise.resolve();
    this.draining = false;
    this.drainError = null;
    this.pruneUnfillable();
    this.reconcileGaps();
  }

  setHistory(history) {
    this.history = history;
  }

  seenDigest(eventId) {
    return this.seen.get(eventId) ?? null;
  }

  remember(record) {
    this.seen.set(record.eventId, { sequence: record.sequence, digest: record.digest ?? digestOf(record) });
    if (this.seen.size > this.retain) this.seen.delete(this.seen.keys().next().value);
  }

  statusSnapshot(extra = {}) {
    return {
      version: 1,
      generatedAt: now(),
      startedAt: this.startedAt,
      state: this.state,
      committedSequence: this.committedSequence,
      committedEventId: this.committedEventId,
      committedCount: this.log.count,
      contiguousThrough: this.committedSequence,
      nextExpectedSequence: this.committedSequence + 1,
      receivedSequence: Math.max(this.receivedSequence, this.committedSequence),
      receivedThisRun: this.counters.eventsReceived,
      pendingCount: this.pending.size,
      bufferedSequences: [...this.pending.keys()].sort((a, b) => a - b),
      gaps: this.gaps.map(gap => ({ ...gap })),
      bufferLimit: this.bufferLimit,
      maxBuffered: this.maxBuffered,
      conflicts: this.conflicts.map(item => ({ ...item })),
      unfillable: this.unfillable.map(item => ({ ...item })),
      recovery: { ...this.recovery },
      counters: { ...this.counters },
      lastError: this.lastError,
      ...extra,
    };
  }

  // 状态文件写入较慢；合并短时间内的多次请求，避免拖住接收路径。
  flushStatus({ force = false } = {}) {
    if (!force && Date.now() - this.lastStatusAt < 100) return this.statusChain;
    const value = this.statusSnapshot();
    this.lastStatusAt = Date.now();
    this.statusChain = this.statusChain.then(() => this.statusWriter.write(value), () => {});
    return this.statusChain;
  }

  // 接收路径：只登记、校验、入有界缓冲区。返回 true 表示已接收（含重复）。
  // 达到 bufferLimit 时抛 BackpressureError，由调用方断开连接并按已提交游标重连；
  // 缓冲中未提交的内容会由网关回放，因此不是静默丢弃。
  receive(value, { via = 'live', connection = null } = {}) {
    validateObservation(value);
    const digest = digestOf(value);
    const sequence = value.sequence;
    this.counters.eventsReceived += 1;

    if (via === 'live') {
      this.receivedSequence = Math.max(this.receivedSequence, sequence);
    }

    if (sequence <= this.committedSequence) {
      const known = this.seenDigest(value.eventId);
      if (!known || known.sequence !== sequence) {
        this.conflict(value, '已提交序号携带未知 eventId');
        return true;
      }
      if (known.digest !== digest) {
        this.conflict(value, '同一 eventId 内容不一致');
        return true;
      }
      this.counters.eventsDuplicate += 1;
      this.audit.append('duplicate-committed', { sequence, eventId: value.eventId, via }).catch(() => {});
      this.flushStatus();
      return true;
    }

    const buffered = this.pending.get(sequence);
    if (buffered) {
      if (buffered.digest !== digest) {
        this.conflict(value, '同一序号在缓冲区中内容不一致');
        return true;
      }
      this.counters.eventsDuplicate += 1;
      this.audit.append('duplicate-buffered', { sequence, eventId: value.eventId }).catch(() => {});
      if (via === 'live') this.startDrain();
      return true;
    }

    if (this.pending.size >= this.bufferLimit) {
      this.counters.backpressure += 1;
      this.lastError = `缓冲区达到上限 ${this.bufferLimit}`;
      this.audit.append('backpressure', {
        bufferLimit: this.bufferLimit, buffered: this.pending.size, atSequence: sequence,
      }).catch(() => {});
      this.flushStatus({ force: true });
      throw new BackpressureError(this.bufferLimit, this.pending.size);
    }

    this.pending.set(sequence, { value, digest, receivedAt: now(), via, connection });
    this.maxBuffered = Math.max(this.maxBuffered, this.pending.size);
    this.reconcileGaps();
    this.flushStatus();
    if (via === 'live') this.startDrain();
    else this.notifyRoom();
    return true;
  }

  // 后台串行提交；同一时刻只有一个提交者在推进。
  startDrain() {
    if (this.draining) return this.drainChain;
    this.draining = true;
    this.drainChain = (async () => {
      try {
        for (;;) {
          const next = this.committedSequence + 1;
          const entry = this.pending.get(next);
          if (!entry || entry.via !== 'live' || this.drainError) break;
          // 先持久化再移出缓冲：写入失败时记录仍留在有界缓冲里，
          // 游标不前进，重启后由源端回放，不会出现“确认了但没落盘”。
          await this.commit(entry);
          this.pending.delete(next);
          this.pruneUnfillable();
          this.reconcileGaps();
          this.flushStatus();
          this.notifyRoom();
        }
      } catch (error) {
        this.drainError = error;
        this.state = 'needs-attention';
        this.lastError = `持久化失败：${error.message}`;
        this.audit.append('commit-failed', { error: error.message }).catch(() => {});
        this.flushStatus({ force: true });
      } finally {
        this.draining = false;
      }
    })();
    return this.drainChain;
  }

  // 等到当前提交链走完；若期间又有新的连续记录进入缓冲，继续推进，
  // 直到没有可提交内容（或遇到缺口 / 持久化失败）。
  async settle() {
    for (;;) {
      await this.drainChain;
      if (this.drainError) throw this.drainError;
      const next = this.committedSequence + 1;
      const entry = this.pending.get(next);
      if (!entry || entry.via !== 'live' || this.draining) return;
      this.startDrain();
    }
  }

  async commit(entry) {
    if (this.commitDelayMs > 0) await new Promise(resolve => setTimeout(resolve, this.commitDelayMs));
    const value = entry.value;
    const record = {
      ...value,
      source: 'gateway',
      via: entry.via,
      connection: entry.connection,
      receivedAt: entry.receivedAt,
      committedAt: now(),
      digest: entry.digest,
    };
    await this.log.append(record);
    this.committedSequence = value.sequence;
    this.committedEventId = value.eventId;
    this.counters.eventsCommitted += 1;
    this.remember(record);
    this.audit.append(entry.via === 'history' ? 'commit-history' : 'commit', {
      sequence: value.sequence, eventId: value.eventId, via: entry.via,
    }).catch(() => {});
    this.notifyRoom();
  }

  // 背压：连接在读取下一帧前等待空位；只有真正提交或丢弃缓冲才会唤醒。
  waitForRoom({ timeoutMs = 0 } = {}) {
    if (this.pending.size < this.bufferLimit) return Promise.resolve('ready');
    return new Promise(resolve => {
      const waiter = { resolve, timer: null };
      if (timeoutMs > 0) {
        waiter.timer = setTimeout(() => {
          this.roomWaiters = this.roomWaiters.filter(item => item !== waiter);
          resolve('stalled');
        }, timeoutMs);
      }
      this.roomWaiters.push(waiter);
    });
  }

  notifyRoom() {
    if (this.pending.size >= this.bufferLimit) return;
    const waiters = this.roomWaiters;
    this.roomWaiters = [];
    for (const waiter of waiters) {
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.resolve('ready');
    }
  }

  conflict(value, reason) {
    this.state = 'needs-attention';
    this.lastError = reason;
    const item = { eventId: value.eventId, sequence: value.sequence, reason, at: now() };
    if (!this.conflicts.some(existing => existing.eventId === item.eventId
      && existing.sequence === item.sequence && existing.reason === item.reason)) {
      this.conflicts.push(item);
      this.audit.append('conflict', item).catch(() => {});
    }
    this.flushStatus({ force: true });
  }

  markUnfillable(from, until, reason, details = {}) {
    this.state = 'needs-attention';
    this.lastError = reason;
    this.unfillable = mergeRanges([...this.unfillable, { from, until, reason, unfillable: true, ...details }]);
    this.audit.append('gap-unfillable', { from, until, reason, ...details }).catch(() => {});
    this.reconcileGaps();
    this.flushStatus({ force: true });
  }

  pruneUnfillable() {
    const kept = this.unfillable
      .flatMap(item => {
        if (item.until <= this.committedSequence) return [];
        if (item.from <= this.committedSequence) return [{ ...item, from: this.committedSequence + 1 }];
        return [item];
      });
    this.unfillable = mergeRanges(kept);
    if (!this.unfillable.length && this.state === 'needs-attention'
      && !this.conflicts.length && !this.drainError) {
      this.state = 'running';
      this.lastError = null;
    }
  }

  // 缺口只由“已提交点之后实际缓冲了哪些序号”推导，再与不可补齐区间合并。
  // 合并后的每一段单独给出原因：与不可补齐区间重叠的段不可补齐，
  // 其余段仍只是“等待补采”。不会因为一段不可补齐而丢掉相邻的未覆盖段。
  reconcileGaps() {
    const waiting = [];
    const sorted = [...this.pending.keys()].sort((a, b) => a - b);
    let expected = this.committedSequence + 1;
    for (const sequence of sorted) {
      if (sequence < expected) continue;
      if (sequence > expected) waiting.push({ from: expected, until: sequence - 1 });
      expected = sequence + 1;
    }

    const regions = mergeRanges([
      ...waiting.map(item => ({ ...item, unfillable: false })),
      ...this.unfillable.map(item => ({ ...item, unfillable: true })),
    ]);

    const gaps = [];
    for (const region of regions) {
      const cuts = new Set([region.from, region.until + 1]);
      for (const item of this.unfillable) {
        if (item.from > region.from && item.from <= region.until) cuts.add(item.from);
        if (item.until + 1 >= region.from && item.until + 1 <= region.until) cuts.add(item.until + 1);
      }
      const points = [...cuts].sort((a, b) => a - b);
      for (let at = 0; at < points.length - 1; at += 1) {
        const from = points[at];
        const until = points[at + 1] - 1;
        if (until < from) continue;
        const cause = this.unfillable.find(item => item.from <= from && item.until >= until);
        gaps.push(cause
          ? { from, until, reason: cause.reason, unfillable: true }
          : { from, until, reason: '等待补采', unfillable: false });
      }
    }
    this.gaps = gaps;
  }

  // 补采上界在开始时固定（通常是 409 的 missingUntil，也就是可重新接流的
  // resumeFloor - 1）。补采期间实时尾部继续增长也不扩大本轮回补范围：
  // 回补到上界并连续提交后，用该上界作为 Last-Event-ID 接回实时流，
  // 由实时流负责回放保留窗内的新尾部。
  async recover({ until, resumeFloor, observedTail, reason = '保留窗外' } = {}) {
    if (!this.history) throw new FatalError('缺少历史接口配置');
    const from = this.committedSequence + 1;
    const upper = Number.isInteger(until) ? until : observedTail;
    if (!Number.isInteger(upper) || upper < from) {
      this.recovery = {
        active: false, reason, from, until: from - 1, pages: 0, repaired: 0,
        resumeAt: from - 1, observedTail,
      };
      await this.audit.append('recovery-skip', { from, until: from - 1, observedTail });
      await this.flushStatus({ force: true });
      return { from, until: from - 1, pages: 0, repaired: 0 };
    }

    this.recovery = {
      active: true, reason, from, until: upper, pages: 0, repaired: 0,
      observedTail, resumeFloor, resumeAt: null, startedAt: now(),
    };
    this.audit.append('recovery-start', { from, until: upper, resumeFloor, observedTail, reason }).catch(() => {});
    await this.flushStatus({ force: true });

    let repaired = 0;
    let aborted = null;
    try {
      for (let offset = 0; ;) {
        const page = await this.history.fetch({ from, until: upper, offset });
        this.recovery.pages += 1;
        await this.audit.append('history-page', {
          from, until: upper, offset, count: page.events.length, nextOffset: page.nextOffset,
        });
        // 先把本页内可推进的连续前缀提交掉，再判断本页是否留下了无法跨过的缺口。
        // 这样既不会把整页压在缓冲里，也能如实记录“已尝试取到哪些序号”。
        for (const value of page.events) {
          validateObservation(value);
          if (value.sequence < from || value.sequence > upper) continue;
          this.counters.historyFetched += 1;
          const had = this.pending.has(value.sequence);
          this.receive(value, { via: 'history' });
          if (!had && this.pending.has(value.sequence)) repaired += 1;
          for (;;) {
            const next = this.committedSequence + 1;
            const entry = this.pending.get(next);
            if (!entry) break;
            this.pending.delete(next);
            await this.commit(entry);
            this.pruneUnfillable();
          }
        }
        // 历史接口升序返回：下一个待提交序号若在本页缺席，后续页也不会再出现它。
        const expected = this.committedSequence + 1;
        if ([...this.pending.keys()].some(sequence => sequence > expected)) {
          aborted = { from: expected, until: this.missingUntil(expected, upper) };
          this.markUnfillable(aborted.from, aborted.until, '历史接口未能补齐');
          break;
        }
        if (page.nextOffset === null || page.events.length === 0) break;
        offset = page.nextOffset;
      }
    } catch (error) {
      this.recovery.active = false;
      this.recovery.resumeAt = null;
      if (error instanceof BackpressureError) {
        // 历史补采也受同一有界缓冲约束；这是本地容量问题而非数据缺失，如实记录后转人工。
        this.state = 'needs-attention';
        this.lastError = `历史回补触发背压：${error.message}`;
        await this.audit.append('recovery-backpressure', { from, until: upper, buffered: error.buffered });
      } else {
        this.markUnfillable(from, upper, `历史接口失败：${error.message}`);
      }
      await this.flushStatus({ force: true });
      throw error;
    }

    this.recovery.repaired = repaired;
    this.recovery.aborted = aborted;
    this.recovery.active = this.pending.size > 0;
    this.recovery.resumeAt = this.canResumeLive() ? this.committedSequence : null;
    this.reconcileGaps();
    await this.audit.append('recovery-done', {
      from, until: upper, repaired, committedSequence: this.committedSequence,
      awaiting: [...this.pending.keys()].sort((a, b) => a - b),
      aborted, resumeAt: this.recovery.resumeAt,
    });
    await this.flushStatus({ force: true });
    return { from, until: upper, pages: this.recovery.pages, repaired, aborted };
  }

  // 从 from 开始，缓冲中第一个不连续的位置；缺到 upper 为止。
  missingUntil(from, upper) {
    let last = from;
    for (const sequence of [...this.pending.keys()].sort((a, b) => a - b)) {
      if (sequence < last) continue;
      if (sequence > upper) break;
      if (sequence === last) last += 1;
      else break;
    }
    return Math.min(upper, Math.max(from, last - 1));
  }

  // 处于“下一个待提交序号”上的无法补齐缺口：此时无法安全接回实时流。
  blockingGap() {
    return this.unfillable.find(gap => gap.from <= this.committedSequence + 1 && gap.until >= this.committedSequence + 1) ?? null;
  }

  canResumeLive() {
    return !this.blockingGap();
  }

  resumeCursor() {
    return { sequence: this.committedSequence, lastEventId: this.committedEventId };
  }

  // 断线前丢弃所有未提交的实时缓冲；重连从已提交游标回放，不是静默丢弃。
  discardPending(reason) {
    if (!this.pending.size) return [];
    const dropped = [...this.pending.keys()].sort((a, b) => a - b);
    this.pending.clear();
    this.reconcileGaps();
    this.notifyRoom();
    this.audit.append('buffer-discard', { dropped, reason }).catch(() => {});
    this.flushStatus({ force: true });
    return dropped;
  }

  async stop(reason = 'stopped') {
    // 先放行阻塞在背压上的连接，再等待在途提交收尾。
    for (const waiter of this.roomWaiters) {
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.resolve('stopped');
    }
    this.roomWaiters = [];
    await this.settle().catch(() => {});
    if (this.state !== 'needs-attention') this.state = 'stopped';
    this.recovery.active = false;
    await this.audit.append('stopped', {
      reason, committedSequence: this.committedSequence, pending: this.pending.size,
    });
    await this.flushStatus({ force: true });
    await this.statusChain;
  }
}
