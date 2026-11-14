import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, appendFile, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createSite, event } from '../src/site.mjs';
import { EventLog } from '../src/store.mjs';
import { Collector } from '../src/collector.mjs';
import { runTap } from '../src/runtime.mjs';

async function withDirectory(operation) {
  const directory = await mkdtemp(path.join(tmpdir(), 'recover-'));
  try { return await operation(directory); } finally { await rm(directory, { recursive: true, force: true }); }
}

const silent = () => {};
const auditStub = { append: async () => {} };
const statusStub = { write: async () => {} };

function makeCollector(log, extra = {}) {
  return new Collector({
    log,
    audit: auditStub,
    statusWriter: statusStub,
    bufferLimit: 8,
    history: null,
    ...extra,
  });
}

test('已接收但未持久化：receive 后崩溃，重启不把未落盘记录当成已确认', async () => {
  await withDirectory(async directory => {
    const filename = path.join(directory, 'events.jsonl');
    const log = await EventLog.open(filename);
    const collector = makeCollector(log);

    collector.receive(event(1));
    collector.receive(event(2));
    // 不调用 settle()，模拟在提交途中被杀。
    await collector.settle();
    assert.equal(collector.committedSequence, 2);
    assert.equal(collector.receivedSequence, 2);
    await log.close();

    // 再模拟只落了 buffer 未落盘的情形：人为放慢持久化，
    // 让“已接收 3”与“已提交 3”之间出现可观察的窗口。
    const reopened = await EventLog.open(filename);
    const restarted = makeCollector(reopened, { commitDelayMs: 400 });
    restarted.receive(event(3));
    // 记录此时只存在于内存：已接收前进到 3，已提交仍是 2。
    assert.equal(restarted.committedSequence, 2);
    assert.equal(restarted.receivedSequence, 3);
    const status = restarted.statusSnapshot();
    assert.equal(status.committedSequence, 2);
    assert.equal(status.receivedSequence, 3);
    assert.equal(status.pendingCount, 1);
    // 此刻真实文件里只有 1、2：接收过 3 不等于提交过 3。
    const before = (await readFile(filename, 'utf8')).trim().split('\n').map(line => JSON.parse(line));
    assert.deepEqual(before.map(record => record.sequence), [1, 2]);

    await restarted.settle();
    assert.equal(restarted.committedSequence, 3);
    await reopened.close();

    const lines = (await readFile(filename, 'utf8')).trim().split('\n').map(line => JSON.parse(line));
    assert.deepEqual(lines.map(record => record.sequence), [1, 2, 3]);
  });
});

test('提交途中断电：events.jsonl 尾部半行被修复，续写从下一序号开始', async () => {
  await withDirectory(async directory => {
    const filename = path.join(directory, 'events.jsonl');
    const log = await EventLog.open(filename);
    await log.append({ ...event(1), source: 'gateway', via: 'live' });
    await log.append({ ...event(2), source: 'gateway', via: 'live' });
    await log.close();
    // 模拟第 3 条写到一半进程被杀。
    await appendFile(filename, '{"sequence":3,"eventId":"evt-3","sam');

    const reopened = await EventLog.open(filename);
    assert.equal(reopened.last.sequence, 2);
    assert.equal(reopened.truncatedBytes > 0, true);
    await reopened.append({ ...event(3), source: 'gateway', via: 'live' });
    await reopened.append({ ...event(4), source: 'gateway', via: 'live' });
    await reopened.close();

    const lines = (await readFile(filename, 'utf8')).trim().split('\n').map(line => JSON.parse(line));
    assert.deepEqual(lines.map(record => record.sequence), [1, 2, 3, 4]);
    // 修复后的文件必须每行都是合法 JSON。
    for (const record of lines) assert.equal(typeof record.eventId, 'string');
  });
});

test('停机后同一目录续接：提交顺序不重复、不跳号', async () => {
  const site = await createSite(0, { tail: 12, perConnection: 12 });
  try {
    await withDirectory(async directory => {
      const first = await runTap({ base: site.url, output: directory, maxConnections: 1, reconnectDelay: 5, logger: silent });
      await fetch(site.url + '/_control', {
        method: 'POST', body: JSON.stringify({ tail: 25, perConnection: 25 }),
      });
      const second = await runTap({ base: site.url, output: directory, maxConnections: 1, reconnectDelay: 5, logger: silent });
      assert.equal(second.committedSequence, 25);
      const records = (await readFile(path.join(directory, 'events.jsonl'), 'utf8'))
        .trim().split('\n').map(line => JSON.parse(line));
      assert.deepEqual(records.map(record => record.sequence), Array.from({ length: 25 }, (_, at) => at + 1));
      const audit = (await readFile(path.join(directory, 'audit.jsonl'), 'utf8'))
        .trim().split('\n').map(line => JSON.parse(line));
      const starts = audit.filter(item => item.type === 'started');
      assert.equal(starts.length, 2);
      assert.equal(starts[1].resumeSequence, first.committedSequence);
      assert.equal(starts[1].resumeEventId, `evt-${first.committedSequence}`);
    });
  } finally {
    await site.close();
  }
});

test('409 回补跨多页：从 resumeFloor-1 接回，历史来源标记正确', async () => {
  const site = await createSite(0, { tail: 12, floor: 10, perConnection: 12 });
  try {
    await withDirectory(async directory => {
      const log = await EventLog.open(path.join(directory, 'events.jsonl'));
      for (const sequence of [1, 2, 3, 4]) {
        await log.append({ ...event(sequence), source: 'gateway', via: 'live' });
      }
      await log.close();
      const summary = await runTap({ base: site.url, output: directory, maxConnections: 2, reconnectDelay: 5, logger: silent });
      assert.equal(summary.committedSequence, 12);
      const records = (await readFile(path.join(directory, 'events.jsonl'), 'utf8'))
        .trim().split('\n').map(line => JSON.parse(line));
      assert.deepEqual(records.map(record => record.sequence), Array.from({ length: 12 }, (_, at) => at + 1));
      // 5..9 来自历史，10..12 来自实时流。
      assert.deepEqual(records.filter(record => record.via === 'history').map(record => record.sequence), [5, 6, 7, 8, 9]);
      assert.deepEqual(records.filter(record => record.via === 'live').map(record => record.sequence), [1, 2, 3, 4, 10, 11, 12]);
      const audit = (await readFile(path.join(directory, 'audit.jsonl'), 'utf8'))
        .trim().split('\n').map(line => JSON.parse(line));
      const start = audit.find(item => item.type === 'recovery-start');
      assert.equal(start.from, 5);
      assert.equal(start.until, 9);
      const connects = audit.filter(item => item.type === 'connect');
      assert.equal(connects.at(-1).lastEventId, 'evt-9');
    });
  } finally {
    await site.close();
  }
});

test('回补上界超出历史可服务范围：可补部分仍补齐，超出部分明确不可补齐', async () => {
  const site = await createSite(0, { tail: 8, floor: 10, perConnection: 8 });
  try {
    await withDirectory(async directory => {
      const log = await EventLog.open(path.join(directory, 'events.jsonl'));
      for (const sequence of [1, 2]) {
        await log.append({ ...event(sequence), source: 'gateway', via: 'live' });
      }
      await log.close();
      const summary = await runTap({ base: site.url, output: directory, maxConnections: 3, reconnectDelay: 5, logger: silent });
      // 3..8 可补，序号 9 超出 observedTail=8，无法补齐。
      assert.equal(summary.committedSequence, 8);
      assert.equal(summary.state, 'needs-attention');
      const status = JSON.parse(await readFile(path.join(directory, 'stream-status.json'), 'utf8'));
      assert.equal(status.state, 'needs-attention');
      assert.ok(status.gaps.some(gap => gap.from === 9 && gap.until === 9 && gap.unfillable));
      assert.match(status.gaps.find(gap => gap.from === 9).reason, /可服务范围/);
      // 没有任何记录越过不可补齐的 9。
      const records = (await readFile(path.join(directory, 'events.jsonl'), 'utf8'))
        .trim().split('\n').map(line => JSON.parse(line));
      assert.deepEqual(records.map(record => record.sequence), Array.from({ length: 8 }, (_, at) => at + 1));
    });
  } finally {
    await site.close();
  }
});

test('小缓冲配合历史回补：不因整页入缓冲而误触发背压', async () => {
  const site = await createSite(0, { tail: 12, floor: 6, perConnection: 12 });
  try {
    await withDirectory(async directory => {
      const log = await EventLog.open(path.join(directory, 'events.jsonl'));
      for (const sequence of [1, 2]) await log.append({ ...event(sequence), source: 'gateway', via: 'live' });
      await log.close();
      // bufferLimit 小于一页（每页 3 条）也能完成回补，因为连续前缀是逐条提交的。
      const summary = await runTap({
        base: site.url, output: directory, maxConnections: 2, reconnectDelay: 5,
        bufferLimit: 2, logger: silent,
      });
      assert.equal(summary.committedSequence, 12);
      assert.equal(summary.state, 'stopped');
      const status = JSON.parse(await readFile(path.join(directory, 'stream-status.json'), 'utf8'));
      assert.equal(status.counters.backpressure, 0, '回补不应触发背压');
      assert.ok(status.maxBuffered <= status.bufferLimit);
    });
  } finally {
    await site.close();
  }
});

test('receivedSequence 只统计本次运行：重启后从提交点重新计数', async () => {
  const site = await createSite(0, { tail: 6, perConnection: 6 });
  try {
    await withDirectory(async directory => {
      await mkdir(path.join(directory, 'raw'), { recursive: true });
      await runTap({ base: site.url, output: directory, maxConnections: 1, reconnectDelay: 5, logger: silent });
      await fetch(site.url + '/_control', { method: 'POST', body: JSON.stringify({ tail: 9, perConnection: 9 }) });
      await runTap({ base: site.url, output: directory, maxConnections: 1, reconnectDelay: 5, logger: silent });
      const status = JSON.parse(await readFile(path.join(directory, 'stream-status.json'), 'utf8'));
      assert.equal(status.committedSequence, 9);
      assert.equal(status.receivedSequence, 9, 'received 可以等于 committed，但不能倒退');
      assert.ok(status.receivedThisRun >= 3, '本次运行确实接收到了 7..9');
      assert.ok(status.receivedSequence >= status.committedSequence);
    });
  } finally {
    await site.close();
    await rm(path.join(tmpdir(), 'recover-'), { recursive: true, force: true }).catch(() => {});
  }
});
