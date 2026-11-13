import test from 'node:test';
import assert from 'node:assert/strict';
import { createSite } from '../src/site.mjs';
import { verify } from '../lib/verify.mjs';

async function withSite(operation) {
  const site = await createSite();
  try { await operation(site); } finally { await site.close(); }
}
async function control(site, value) {
  return fetch(site.url + '/_control', { method: 'POST', body: JSON.stringify(value) });
}

test('SSE 响应携带心跳、多行 data 和 UTF-8', async () => withSite(async site => {
  const response = await fetch(site.url + '/stream');
  assert.match(response.headers.get('content-type'), /text\/event-stream/);
  const raw = await response.text();
  assert.match(raw, /籽粒镍/); assert.match(raw, /: heartbeat/); assert.match(raw, /event: notice\ndata:/);
}));
test('过期游标与历史分页缺条', async () => withSite(async site => {
  await control(site, { floor: 6, missing: 3 });
  const response = await fetch(site.url + '/stream', { headers: { 'last-event-id': 'evt-1' } });
  assert.equal(response.status, 409);
  assert.deepEqual(await response.json(), { missingFrom: 2, missingUntil: 5, resumeFloor: 6, observedTail: 12 });
  const history = await (await fetch(site.url + '/history?from=2&until=5&offset=0')).json();
  assert.deepEqual(history.events.map(value => value.sequence), [2, 4, 5]);
}));
test('跨过未补缺口的提交状态无效', () => assert.throws(() => verify({ committedSequence: 5, committedEventId: 'evt-5', receivedSequence: 8, gaps: [{ from: 3, until: 4 }], bufferLimit: 4, maxBuffered: 3, state: 'running' })));

test('冲突重复与半帧断流不会被源端伪装为成功', async () => withSite(async site => {
  await control(site, { duplicate: true, conflict: true });
  const wire = await (await fetch(site.url + '/stream')).text();
  assert.equal((wire.match(/id: evt-1\r\n/g) || []).length, 2);
  assert.match(wire, /"value":-99/);
  await control(site, { cutOnce: true });
  await assert.rejects(async () => { const response = await fetch(site.url + '/stream'); await response.text(); });
}));
