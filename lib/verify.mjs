import assert from 'node:assert/strict';

export function verify(value) {
  assert.ok(Number.isInteger(value.committedSequence) && value.committedSequence >= 0);
  assert.equal(value.committedEventId, `evt-${value.committedSequence}`);
  assert.ok(value.receivedSequence >= value.committedSequence);
  assert.ok(value.bufferLimit > 0 && value.maxBuffered <= value.bufferLimit);
  assert.ok(Array.isArray(value.gaps));
  for (const gap of value.gaps) assert.ok(gap.from > value.committedSequence && gap.until >= gap.from);
  if (value.gaps.length) assert.notEqual(value.state, 'stopped');
}
