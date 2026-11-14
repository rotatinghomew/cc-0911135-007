import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { verify } from '../lib/verify.mjs';

function base(extra = {}) {
  return {
    version: 1,
    state: 'running',
    committedSequence: 5,
    committedEventId: 'evt-5',
    committedCount: 5,
    contiguousThrough: 5,
    nextExpectedSequence: 6,
    receivedSequence: 8,
    pendingCount: 2,
    bufferedSequences: [7, 8],
    gaps: [{ from: 6, until: 6, reason: '等待补采', unfillable: false }],
    bufferLimit: 4,
    maxBuffered: 3,
    conflicts: [],
    unfillable: [],
    counters: {},
    recovery: {},
    ...extra,
  };
}

test('正常状态通过校验', () => {
  assert.equal(verify(base()), true);
});

test('跨过缺口的提交状态无效', () => {
  assert.throws(() => verify(base({
    committedSequence: 5, gaps: [{ from: 3, until: 4, unfillable: false }],
  })), /缺口必须位于已提交点之后/);
});

test('缓冲峰值超过上限无效', () => {
  assert.throws(() => verify(base({ maxBuffered: 5, bufferLimit: 4 })), /缓冲峰值/);
});

test('committedEventId 与序号不一致无效', () => {
  assert.throws(() => verify(base({ committedEventId: 'evt-7' })), /committedEventId/);
});

test('连续覆盖点不能领先于已提交点', () => {
  assert.throws(() => verify(base({ contiguousThrough: 6 })), /连续覆盖点/);
  assert.throws(() => verify(base({ nextExpectedSequence: 8 })), /nextExpectedSequence/);
});

test('未覆盖缺口时不能报告 stopped', () => {
  assert.throws(() => verify(base({ state: 'stopped' })), /不能报告 stopped/);
});

test('存在冲突时必须处于需处理状态', () => {
  assert.throws(() => verify(base({ conflicts: [{ eventId: 'evt-5', sequence: 5 }] })), /需处理状态/);
  assert.equal(verify(base({ state: 'needs-attention', conflicts: [{ eventId: 'evt-5', sequence: 5 }] })), true);
});

test('缺口必须按序号递增且不重叠', () => {
  // 乱序：后一段的 from 不大于前一段。
  assert.throws(() => verify(base({
    gaps: [{ from: 8, until: 9, unfillable: false }, { from: 6, until: 6, unfillable: false }],
  })), /递增/);
  // 真重叠。
  assert.throws(() => verify(base({
    gaps: [{ from: 6, until: 9, unfillable: false }, { from: 8, until: 10, unfillable: false }],
  })), /重叠/);
  // 相邻但原因不同是允许的：同一段未覆盖区被拆成不可补齐 + 等待补采。
  assert.equal(verify(base({
    gaps: [
      { from: 6, until: 6, reason: '不可补齐', unfillable: true },
      { from: 7, until: 8, reason: '等待补采', unfillable: false },
    ],
  })), true);
});

test('事件日志交叉核对顺序与条数', () => {
  const events = [1, 2, 3, 4, 5].map(sequence => ({ sequence, eventId: `evt-${sequence}` }));
  assert.equal(verify(base(), { events }), true);
  assert.throws(() => verify(base(), {
    events: [...events.slice(0, 4), { sequence: 6, eventId: 'evt-6' }],
  }), /序号应为 5/);
  assert.throws(() => verify(base(), { events: events.slice(0, 4) }), /不一致/);
});

test('停机恢复后的事件日志可被独立验收', async () => {
  const directory = await mkdtemp(path.join(tmpdir(), 'verify-'));
  try {
    const statusPath = path.join(directory, 'stream-status.json');
    await writeFile(statusPath, JSON.stringify(base({ state: 'running', gaps: [] })));
    await writeFile(path.join(directory, 'events.jsonl'),
      [1, 2, 3, 4, 5].map(sequence => JSON.stringify({ sequence, eventId: `evt-${sequence}` })).join('\n') + '\n');
    const status = JSON.parse(await readFile(statusPath, 'utf8'));
    const text = await readFile(path.join(directory, 'events.jsonl'), 'utf8');
    const events = text.trim().split('\n').map(line => JSON.parse(line));
    assert.equal(verify(status, { events }), true);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
