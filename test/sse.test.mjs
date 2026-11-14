import test from 'node:test';
import assert from 'node:assert/strict';
import { SseParser } from '../src/sse.mjs';

function parseAll(bytes, sizes) {
  const parser = new SseParser();
  const frames = [];
  let at = 0;
  let index = 0;
  while (at < bytes.length) {
    const size = sizes[index % sizes.length];
    index += 1;
    parser.feed(bytes.subarray(at, Math.min(bytes.length, at + size)));
    at += size;
    for (let next = parser.next(); next; next = parser.next()) frames.push(next);
  }
  return { frames, parser };
}

const WIRE = Buffer.from(
  ': heartbeat\r\n\r\n'
  + 'id: evt-1\r\nevent: observation\r\ndata: {"sequence":1,"eventId":"evt-1","label":"籽粒镍"}\r\n\r\n'
  + 'id: evt-2\r\nevent: notice\r\ndata: {"tail":\ndata: 12\ndata: }\r\n\r\n'
  + 'id: evt-3\r\nevent: observation\r\n\r\ndata: {"sequence":3,"eventId":"evt-3","label":"玉米"}\r\n\r\n',
);

test('任意字节切分都得到相同完整帧，含 UTF-8 '/', CRLF 与多行 data', () => {
  const baseline = parseAll(WIRE, [WIRE.length]).frames;
  assert.equal(baseline.length, 3);
  assert.equal(baseline[0].type, 'observation');
  assert.equal(baseline[0].id, 'evt-1');
  assert.equal(JSON.parse(baseline[0].data).label, '籽粒镍');
  assert.equal(baseline[1].type, 'notice');
  assert.equal(baseline[1].data, '{"tail":\n12\n}');
  assert.equal(baseline[2].data, '{"sequence":3,"eventId":"evt-3","label":"玉米"}');

  for (const size of [1, 2, 3, 5, 7, 13, 64]) {
    const { frames } = parseAll(WIRE, [size]);
    assert.deepEqual(frames, baseline, `按 ${size} 字节切分`);
  }
  // 混合切分：空块、超长块、单字节块交替。
  const mixed = parseAll(WIRE, [0, 1, 64, 2, 0, 3]).frames;
  assert.deepEqual(mixed, baseline);
});

test('CRLF 的 CR 与 LF 分属两块时不会多出一个空行', () => {
  const parser = new SseParser();
  parser.feed(Buffer.from('data: a\r'));
  parser.feed(Buffer.from('\n\r'));
  parser.feed(Buffer.from('\n'));
  const frames = [];
  for (let next = parser.next(); next; next = parser.next()) frames.push(next);
  assert.deepEqual(frames, [{ type: 'message', data: 'a', id: null }]);
});

test('注释心跳不产生事件，未以空行结束的半帧被丢弃', () => {
  const parser = new SseParser();
  parser.feed(Buffer.from(': ping\n\n: ping\n'));
  assert.equal(parser.next(), null);
  parser.feed(Buffer.from('id: evt-9\ndata: {"sequence":9}\n'));
  assert.equal(parser.next(), null);
  assert.match(parser.partialText(), /evt-9/);
  parser.feed(Buffer.from('\n'));
  assert.deepEqual(parser.next(), { type: 'message', data: '{"sequence":9}', id: 'evt-9' });
});

test('流首 BOM 被移除，id 中的 NUL 被忽略', () => {
  const parser = new SseParser();
  parser.feed(Buffer.from('﻿data: x\nid: bad\u0000id\ndata: y\n\n'));
  assert.deepEqual(parser.next(), { type: 'message', data: 'x\ny', id: null });
});

test('data 字段缺冒号时值为空字符串', () => {
  const parser = new SseParser();
  parser.feed(Buffer.from('data\ndata\ndata: z\n\n'));
  assert.deepEqual(parser.next(), { type: 'message', data: '\n\nz', id: null });
});
