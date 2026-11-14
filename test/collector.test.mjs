import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, readdir, rm, writeFile, appendFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createSite, frame, event } from '../src/site.mjs';
import { runTap } from '../src/runtime.mjs';
import { EventLog } from '../src/store.mjs';

async function withSite(initial, operation) {
  const site = await createSite(0, initial);
  try { return await operation(site); } finally { await site.close(); }
}

async function withDirectory(operation) {
  const directory = await mkdtemp(path.join(tmpdir(), 'tap-'));
  try { return await operation(directory); } finally { await rm(directory, { recursive: true, force: true }); }
}

const control = (site, value) => fetch(site.url + '/_control', { method: 'POST', body: JSON.stringify(value) });

async function readEvents(directory) {
  try {
    const text = await readFile(path.join(directory, 'events.jsonl'), 'utf8');
    return text.trim() ? text.trim().split('\n').map(line => JSON.parse(line)) : [];
  } catch (error) {
    if (error.code === 'ENOENT') return [];
    throw error;
  }
}

async function readStatus(directory) {
  return JSON.parse(await readFile(path.join(directory, 'stream-status.json'), 'utf8'));
}

async function readAudit(directory) {
  const text = await readFile(path.join(directory, 'audit.jsonl'), 'utf8');
  return text.trim().split('\n').map(line => JSON.parse(line));
}

function options(site, output, extra = {}) {
  return {
    base: site.url, output, reconnectDelay: 5, connectTimeout: 4000, stallTimeout: 4000,
    logger: () => {}, ...extra,
  };
}

test('小字节块：UTF-8、CRLF、多行 data 与心跳都不影响落盘', async () => {
  await withSite({ chunkSize: 1, subdivide: true, tail: 9 }, async site => withDirectory(async directory => {
    const summary = await runTap(options(site, directory, { maxConnections: 3 }));
    const records = await readEvents(directory);
    assert.deepEqual(records.map(record => record.sequence), [1, 2, 3, 4, 5, 6, 7, 8, 9]);
    for (const record of records) {
      assert.equal(record.eventId, `evt-${record.sequence}`);
      assert.equal(record.label, '籽粒镍');
      assert.equal(record.value, record.sequence / 10);
    }
    assert.equal(summary.committedSequence, 9);
    const status = await readStatus(directory);
    assert.equal(status.committedSequence, 9);
    assert.equal(status.committedEventId, 'evt-9');
    assert.equal(status.state, 'stopped');
    assert.deepEqual(status.gaps, []);
    // 心跳与 notice 不落事件：12 条原始帧里只有 9 条 observation。
    assert.equal(status.counters.eventsReceived, 9);
  }));
});

test('半帧断开：未完成事件不确认，重连游标等于已持久提交值', async () => {
  await withSite({ cutOnce: true, tail: 10, chunkSize: 5 }, async site => withDirectory(async directory => {
    await control(site, { cutOnce: true });
    await runTap(options(site, directory, { maxConnections: 2 }));
    const records = await readEvents(directory);
    assert.deepEqual(records.map(record => record.sequence), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    const audit = await readAudit(directory);
    const firstCut = audit.find(item => item.type === 'reconnect' && item.reason === 'cut');
    assert.ok(firstCut, '应当记录一次半帧断线重连');
    // 第一次连接被切断，第二次连接从已提交游标继续；游标不越过已提交点。
    const connects = audit.filter(item => item.type === 'connect');
    assert.equal(connects.length, 2);
    assert.ok(connects.every(item => item.lastEventId === `evt-${item.sequence}`));
    // 重连时的 Last-Event-ID 必须等于当时已经提交的最大序号。
    assert.equal(connects[1].lastEventId, 'evt-3');
    assert.equal(connects[1].sequence, 3);
  }));
});

test('重复推送：相同 ID 同内容只提交一次', async () => {
  await withSite({ duplicate: true, tail: 8, chunkSize: 11 }, async site => withDirectory(async directory => {
    await control(site, { duplicate: true, perConnection: 8 });
    await runTap(options(site, directory, { maxConnections: 2 }));
    const records = await readEvents(directory);
    const sequences = records.map(record => record.sequence);
    assert.deepEqual(sequences, [1, 2, 3, 4, 5, 6, 7, 8]);
    assert.equal(new Set(records.map(record => record.eventId)).size, records.length);
    const status = await readStatus(directory);
    assert.ok(status.counters.eventsDuplicate >= 1, '重复到达应被识别');
    assert.deepEqual(status.conflicts, []);
    assert.notEqual(status.state, 'needs-attention');
  }));
});

test('冲突：同一 ID 内容不一致时通道转入需处理且不覆盖', async () => {
  await withSite({ duplicate: true, conflict: true, tail: 6 }, async site => withDirectory(async directory => {
    await control(site, { duplicate: true, conflict: true, perConnection: 6 });
    const summary = await runTap(options(site, directory, { maxConnections: 1 }));
    const records = await readEvents(directory);
    // 第一条已提交，冲突副本绝不写入。
    assert.ok(!records.some(record => record.value === -99));
    assert.deepEqual(records.map(record => record.sequence), [1, 2, 3, 4, 5, 6]);
    assert.equal(summary.state, 'needs-attention');
    const status = await readStatus(directory);
    assert.equal(status.state, 'needs-attention');
    assert.ok(status.conflicts.length >= 1);
    assert.equal(status.conflicts[0].eventId, 'evt-1');
    // 已提交的 evt-1 保持原值，后写没有覆盖。
    assert.equal(records.find(record => record.sequence === 1).value, 0.1);
  }));
});

test('保留窗外：409 后按固定上界回补，再安全接回实时流', async () => {
  await withSite({ tail: 12 }, async site => withDirectory(async directory => {
    // 先手动持久化 1..3，模拟旧游标；随后源端把 floor 推进到 6。
    const log = await EventLog.open(path.join(directory, 'events.jsonl'));
    for (const sequence of [1, 2, 3]) {
      await log.append({ ...event(sequence), source: 'gateway', via: 'live', digest: undefined });
    }
    await log.close();
    await control(site, { floor: 6, tail: 12, perConnection: 12 });

    const summary = await runTap(options(site, directory, { maxConnections: 2 }));
    const records = await readEvents(directory);
    assert.deepEqual(records.map(record => record.sequence), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
    assert.equal(summary.committedSequence, 12);
    const audit = await readAudit(directory);
    const floor = audit.find(item => item.type === 'floor-409');
    assert.deepEqual(
      { missingFrom: floor.missingFrom, missingUntil: floor.missingUntil, resumeFloor: floor.resumeFloor },
      { missingFrom: 4, missingUntil: 5, resumeFloor: 6 },
    );
    const start = audit.find(item => item.type === 'recovery-start');
    assert.equal(start.from, 4);
    assert.equal(start.until, 5); // 上界固定为 resumeFloor-1，不是当前最大序号
    const done = audit.find(item => item.type === 'recovery-done');
    assert.equal(done.resumeAt, 5);
    // 回补完成后从 evt-5 接回实时流。
    const connects = audit.filter(item => item.type === 'connect');
    assert.ok(connects.some(item => item.lastEventId === 'evt-5'));
    const status = await readStatus(directory);
    assert.deepEqual(status.gaps, []);
    assert.equal(status.counters.historyFetched >= 2, true);
  }));
});

test('历史缺口：无法补齐的序号使通道进入 needs-attention 而不是跨过缺口', async () => {
  await withSite({ tail: 12, floor: 6, missing: 3 }, async site => withDirectory(async directory => {
    const log = await EventLog.open(path.join(directory, 'events.jsonl'));
    for (const sequence of [1, 2]) {
      await log.append({ ...event(sequence), source: 'gateway', via: 'live' });
    }
    await log.close();
    await control(site, { floor: 6, missing: 3, tail: 12, perConnection: 12 });

    const summary = await runTap(options(site, directory, { maxConnections: 5 }));
    assert.equal(summary.state, 'needs-attention');
    const records = await readEvents(directory);
    const sequences = records.map(record => record.sequence);
    // 提交停在缺口前：3 缺失，4、5 即使能取到也不得跨过 3 提交。
    assert.deepEqual(sequences, [1, 2]);
    const status = await readStatus(directory);
    assert.equal(status.state, 'needs-attention');
    assert.equal(status.committedSequence, 2);
    assert.equal(status.counters.historyFetched, 2, '确实取到了 4、5');
    assert.deepEqual(status.bufferedSequences, [4, 5], '4、5 在缓冲中等待缺口修复');
    const gap = status.gaps.find(item => item.from === 3);
    assert.ok(gap, '缺口 3 应保留在状态中');
    assert.ok(gap.unfillable);
    assert.match(gap.reason, /未能补齐/);
    // 状态自洽：缺口必须位于已提交点之后。
    assert.ok(status.gaps.every(item => item.from > status.committedSequence));
    // 提交顺序绝不跨过缺口：events.jsonl 中不存在 3 之后的记录。
    assert.ok(!records.some(record => record.sequence > 2));
  }));
});

test('尾部推进：补采期间实时尾部增长也不扩大本轮回补上界', async () => {
  await withSite({ tail: 12 }, async site => withDirectory(async directory => {
    const log = await EventLog.open(path.join(directory, 'events.jsonl'));
    for (const sequence of [1, 2, 3]) {
      await log.append({ ...event(sequence), source: 'gateway', via: 'live' });
    }
    await log.close();
    await control(site, { floor: 6, tail: 12, perConnection: 12 });
    // 回补期间源端继续产生样品：tail 从 12 增长到 20。
    const timer = setInterval(() => { site.state.tail = Math.min(20, site.state.tail + 2); }, 5);
    try {
      const summary = await runTap(options(site, directory, { maxConnections: 3 }));
      assert.equal(summary.committedSequence, 20, '回补后实时流补上了新增尾部');
    } finally {
      clearInterval(timer);
    }
    const audit = await readAudit(directory);
    const start = audit.find(item => item.type === 'recovery-start');
    assert.equal(start.until, 5, '补采上界固定为 resumeFloor-1');
    const records = await readEvents(directory);
    assert.deepEqual(records.map(record => record.sequence), Array.from({ length: 20 }, (_, at) => at + 1));
  }));
});

test('慢接收方与有界缓冲：峰值不越限，背压断线后重连补齐且无静默丢弃', async () => {
  await withSite({ tail: 40, perConnection: 40, chunkSize: 64, slowWriteMs: 0 }, async site => withDirectory(async directory => {
    const summary = await runTap(options(site, directory, {
      bufferLimit: 3, commitDelayMs: 4, maxConnections: 60, reconnectDelay: 1,
    }));
    const status = await readStatus(directory);
    assert.ok(status.maxBuffered <= status.bufferLimit, `峰值 ${status.maxBuffered} 不应超过上限 ${status.bufferLimit}`);
    assert.ok(status.counters.backpressure >= 1, '应当观察到背压');
    // 断线重连把缓冲丢弃作为审计记录，而不是静默丢弃。
    const audit = await readAudit(directory);
    const discard = audit.find(item => item.type === 'buffer-discard');
    assert.ok(discard && discard.dropped.length > 0);
    const records = await readEvents(directory);
    assert.deepEqual(records.map(record => record.sequence), Array.from({ length: 40 }, (_, at) => at + 1));
    assert.equal(summary.committedSequence, 40);
    assert.deepEqual(status.gaps, []);
  }));
});

test('停机恢复：停止后重新运行从已提交游标续接，不重复不跳号', async () => {
  await withSite({ tail: 15, perConnection: 15 }, async site => withDirectory(async directory => {
    const first = await runTap(options(site, directory, { maxConnections: 1 }));
    assert.ok(first.committedSequence > 0);
    await control(site, { tail: 30, perConnection: 30 });
    const second = await runTap(options(site, directory, { maxConnections: 1 }));
    assert.equal(second.committedSequence, 30);
    const records = await readEvents(directory);
    assert.deepEqual(records.map(record => record.sequence), Array.from({ length: 30 }, (_, at) => at + 1));
    const audit = await readAudit(directory);
    const started = audit.filter(item => item.type === 'started');
    assert.equal(started.length, 2);
    assert.equal(started[1].resumeSequence, first.committedSequence);
    const status = await readStatus(directory);
    assert.equal(status.committedSequence, 30);
    assert.equal(status.state, 'stopped');
  }));
});

test('崩溃残留：events.jsonl 尾部半行被丢弃，已提交记录不重复', async () => {
  await withSite({ tail: 6 }, async site => withDirectory(async directory => {
    const log = await EventLog.open(path.join(directory, 'events.jsonl'));
    for (const sequence of [1, 2, 3]) await log.append({ ...event(sequence), source: 'gateway', via: 'live' });
    await log.close();
    await appendFile(path.join(directory, 'events.jsonl'), '{"sequence":4,"eventId":"evt-4"');
    const summary = await runTap(options(site, directory, { maxConnections: 1 }));
    assert.equal(summary.committedSequence, 6);
    const records = await readEvents(directory);
    assert.deepEqual(records.map(record => record.sequence), [1, 2, 3, 4, 5, 6]);
    const audit = await readAudit(directory);
    assert.ok(audit.some(item => item.type === 'recovered-partial-line'));
  }));
});

test('原始字节可离线重放：保存的 SSE 包含完整 observation 与 notice', async () => {
  await withSite({ tail: 5, chunkSize: 3 }, async site => withDirectory(async directory => {
    await runTap(options(site, directory, { maxConnections: 1 }));
    const rawDirectory = path.join(directory, 'raw');
    const names = (await readdir(rawDirectory)).filter(name => name.endsWith('.sse'));
    assert.equal(names.length, 1);
    const name = path.join(rawDirectory, names[0]);
    const bytes = await readFile(name);
    const { SseParser } = await import('../src/sse.mjs');
    const parser = new SseParser();
    const frames = [];
    for (let at = 0; at < bytes.length; at += 1) {
      parser.feed(bytes.subarray(at, at + 1));
      for (let next = parser.next(); next; next = parser.next()) frames.push(next);
    }
    const observations = frames.filter(item => item.type === 'observation');
    assert.deepEqual(observations.map(item => JSON.parse(item.data).sequence), [1, 2, 3, 4, 5]);
    assert.equal(parser.partialText(), '');
  }));
});

test('状态文件与事件日志始终自洽：提交序列严格递增且不跨过缺口', async () => {
  await withSite({ tail: 20, perConnection: 7 }, async site => withDirectory(async directory => {
    await runTap(options(site, directory, { maxConnections: 6 }));
    const records = await readEvents(directory);
    records.forEach((record, index) => {
      assert.equal(record.sequence, index + 1);
      assert.equal(record.eventId, `evt-${record.sequence}`);
    });
    const status = await readStatus(directory);
    assert.equal(status.committedSequence, records.length);
    assert.equal(status.committedEventId, `evt-${status.committedSequence}`);
    assert.ok(status.receivedSequence >= status.committedSequence);
    assert.ok(status.maxBuffered <= status.bufferLimit);
    assert.ok(status.gaps.every(gap => gap.from > status.committedSequence));
    assert.equal(status.contiguousThrough, status.committedSequence);
  }));
});
