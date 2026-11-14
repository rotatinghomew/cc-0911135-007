import assert from 'node:assert/strict';

/**
 * 校验 stream-status.json 的业务不变量，并可选地拿 events.jsonl 交叉核对。
 * 这里刻意重复一些采集端自己的断言：验收不应信任产出方。
 */
export function verify(value, { events = null } = {}) {
  assert.ok(value && typeof value === 'object', '状态必须是对象');
  assert.ok(Number.isInteger(value.committedSequence) && value.committedSequence >= 0, 'committedSequence 必须是非负整数');
  assert.equal(value.committedEventId, `evt-${value.committedSequence}`, 'committedEventId 必须与 committedSequence 一致');
  assert.ok(Number.isInteger(value.receivedSequence) && value.receivedSequence >= value.committedSequence,
    'receivedSequence 不能小于已提交点');
  assert.ok(Number.isInteger(value.contiguousThrough), 'contiguousThrough 必须是整数');
  assert.equal(value.contiguousThrough, value.committedSequence, '连续覆盖点必须等于已提交点');
  assert.equal(value.nextExpectedSequence, value.committedSequence + 1, 'nextExpectedSequence 必须紧随提交点');
  assert.ok(['running', 'stopped', 'needs-attention'].includes(value.state), `未知状态：${value.state}`);
  assert.ok(Number.isInteger(value.bufferLimit) && value.bufferLimit > 0, 'bufferLimit 必须是正整数');
  assert.ok(Number.isInteger(value.maxBuffered) && value.maxBuffered >= 0, 'maxBuffered 必须是非负整数');
  assert.ok(value.maxBuffered <= value.bufferLimit, `缓冲峰值 ${value.maxBuffered} 超过上限 ${value.bufferLimit}`);
  assert.ok(Number.isInteger(value.pendingCount) && value.pendingCount >= 0, 'pendingCount 必须是非负整数');
  assert.ok(Array.isArray(value.gaps), 'gaps 必须是数组');

  for (const gap of value.gaps) {
    assert.ok(Number.isInteger(gap.from) && Number.isInteger(gap.until), '缺口端点必须是整数');
    assert.ok(gap.from > value.committedSequence, '缺口必须位于已提交点之后');
    assert.ok(gap.until >= gap.from, '缺口区间不能倒置');
  }
  // 缺口之间不能重叠；相邻是允许的（同一段未覆盖区可能被拆成不同原因）。
  for (let at = 1; at < value.gaps.length; at += 1) {
    assert.ok(value.gaps[at].from > value.gaps[at - 1].from, '缺口必须按序号递增');
    assert.ok(value.gaps[at].from > value.gaps[at - 1].until, '缺口之间不能重叠');
  }

  if (value.gaps.length) assert.notEqual(value.state, 'stopped', '存在未覆盖缺口时不能报告 stopped');
  for (const item of value.conflicts ?? []) {
    assert.equal(value.state, 'needs-attention', '存在冲突时必须转入需处理状态');
    assert.ok(item.eventId, '冲突必须指明 eventId');
  }
  if (value.unfillable?.length) assert.equal(value.state, 'needs-attention', '不可补齐的缺口必须转入需处理状态');

  // 可选的交叉核对：事件日志必须恰好是 1..committedSequence 的连续序列，
  // 且首条之前没有空洞、eventId 与序号一致、提交顺序严格递增。
  if (events) {
    assert.equal(events.length, value.committedCount, `事件条数 ${events.length} 与 committedCount ${value.committedCount} 不一致`);
    events.forEach((record, at) => {
      assert.equal(record.sequence, at + 1, `第 ${at + 1} 条事件的序号应为 ${at + 1}，实际 ${record.sequence}`);
      assert.equal(record.eventId, `evt-${record.sequence}`, 'eventId 必须与序号一致');
    });
    assert.equal(value.committedSequence, events.length, '提交点必须等于事件条数');
  }

  assert.ok(value.counters && typeof value.counters === 'object', 'counters 必须存在');
  assert.ok(value.recovery && typeof value.recovery === 'object', 'recovery 必须存在');
  return true;
}
