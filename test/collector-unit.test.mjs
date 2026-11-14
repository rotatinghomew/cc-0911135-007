import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { EventLog } from '../src/store.mjs';
import { Collector, mergeRanges } from '../src/collector.mjs';
import { event } from '../src/site.mjs';

const audit = { append: async () => {} };
const statusWriter = { write: async () => {} };

async function withCollector(extra, operation) {
  const directory = await mkdtemp(path.join(tmpdir(), 'collector-'));
  const log = await EventLog.open(path.join(directory, 'events.jsonl'));
  try {
    return await operation(new Collector({ log, audit, statusWriter, bufferLimit: 32, history: null, ...extra }), log);
  } finally {
    await log.close();
    await rm(directory, { recursive: true, force: true });
  }
}

test('缺口按原因分段：不可补齐段不被相邻的等待段吞掉', async () => {
  await withCollector({}, collector => {
    collector.markUnfillable(3, 4, '历史缺失');
    collector.receive(event(6));
    collector.receive(event(7));
    collector.receive(event(8));
    assert.deepEqual(collector.gaps, [
      { from: 1, until: 2, reason: '等待补采', unfillable: false },
      { from: 3, until: 4, reason: '历史缺失', unfillable: true },
      { from: 5, until: 5, reason: '等待补采', unfillable: false },
    ]);
  });
});

test('缺口随提交推进而收缩，补齐后消失', async () => {
  await withCollector({}, async collector => {
    collector.receive(event(1));
    collector.receive(event(2));
    collector.receive(event(4));
    await collector.settle();
    assert.equal(collector.committedSequence, 2);
    assert.deepEqual(collector.gaps.map(gap => [gap.from, gap.until]), [[3, 3]]);
    collector.receive(event(3));
    await collector.settle();
    assert.equal(collector.committedSequence, 4);
    assert.deepEqual(collector.gaps, []);
    assert.equal(collector.state, 'running');
  });
});

test('有界缓冲满时拒绝新序号，但不推进确认点', async () => {
  await withCollector({ bufferLimit: 2, commitDelayMs: 1000 }, async collector => {
    collector.receive(event(1));
    collector.receive(event(2));
    // 提交被放慢，缓冲区已满：第 3 条必须触发背压而不是静默接受或丢弃已有数据。
    assert.throws(() => collector.receive(event(3)), /缓冲区达到上限/);
    assert.equal(collector.pending.size, 2);
    assert.equal(collector.committedSequence, 0);
    assert.equal(collector.counters.backpressure, 1);
    await collector.stop('test');
  });
});

test('提交串行且不跨过缺口，洞后的记录留在有界缓冲', async () => {
  await withCollector({}, async collector => {
    collector.receive(event(1));
    collector.receive(event(3));
    collector.receive(event(4));
    await collector.settle();
    assert.equal(collector.committedSequence, 1);
    assert.deepEqual([...collector.pending.keys()].sort((a, b) => a - b), [3, 4]);
    assert.deepEqual(collector.statusSnapshot().bufferedSequences, [3, 4]);
    assert.equal(collector.canResumeLive(), true, '缺口可补，仍允许接回实时流（补齐才会提交）');
  });
});

test('同一序号内容不同判为冲突；相同内容判为重复', async () => {
  await withCollector({}, async collector => {
    collector.receive(event(1));
    await collector.settle();
    collector.receive(event(1));
    assert.equal(collector.counters.eventsDuplicate, 1);
    assert.deepEqual(collector.conflicts, []);
    collector.receive({ ...event(1), value: -1 });
    assert.equal(collector.state, 'needs-attention');
    assert.equal(collector.conflicts.length, 1);
  });
});

test('区间合并只合并相邻或重叠，保留不连续段', () => {
  assert.deepEqual(mergeRanges([
    { from: 1, until: 2 }, { from: 3, until: 4 }, { from: 9, until: 9 },
  ]), [
    { from: 1, until: 4 }, { from: 9, until: 9 },
  ]);
});
