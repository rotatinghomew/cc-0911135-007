import { parseArgs } from 'node:util';
import { readFile } from 'node:fs/promises';
import { SseParser } from '../src/sse.mjs';

// 从保存的原始字节重放 SSE：不连接网络、不依赖源端状态。
// 默认按字节块切割（模拟分包），也可以按字符切（暴露 UTF-8 / CRLF 边界）。
const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    chunk: { type: 'string', default: '5' },
    mode: { type: 'string', default: 'bytes' },
    events: { type: 'boolean', default: false },
    help: { type: 'boolean', short: 'h', default: false },
  },
});

if (values.help || !positionals[0]) {
  console.error(`用法：node tools/replay.mjs <conn-*.sse> [--chunk 5] [--mode bytes|char] [--events]

从保存的原始字节重放 SSE 帧，输出解析结果。退出码 1 表示录制的尾部是
半帧（断线时本来就不该被当作已提交事件），此时的完整帧仍会正常解析。`);
  process.exit(values.help ? 0 : 2);
}

const bytes = await readFile(positionals[0]);
const chunk = Math.max(1, Number(values.chunk) || 1);
const parser = new SseParser();
const frames = [];

function emit() {
  for (let next = parser.next(); next; next = parser.next()) frames.push(next);
}

if (values.mode === 'char') {
  // 逐字符切割，但按 UTF-8 编码还原为字节块。
  for (const character of bytes.toString('utf8')) {
    parser.feed(Buffer.from(character));
    emit();
  }
} else {
  for (let at = 0; at < bytes.length; at += chunk) {
    parser.feed(bytes.subarray(at, Math.min(bytes.length, at + chunk)));
    emit();
  }
}

const observations = frames.filter(frame => frame.type === 'observation');
const trailing = parser.partialText();
const report = {
  file: positionals[0],
  bytes: bytes.length,
  frames: frames.length,
  observations: observations.length,
  types: frames.reduce((counts, frame) => ({ ...counts, [frame.type]: (counts[frame.type] || 0) + 1 }), {}),
  partialBytes: Buffer.byteLength(trailing, 'utf8'),
  sequenceRange: observations.length
    ? [JSON.parse(observations[0].data).sequence, JSON.parse(observations.at(-1).data).sequence]
    : null,
};
if (values.events) report.events = observations.map(frame => ({ id: frame.id, type: frame.type, data: JSON.parse(frame.data) }));
console.log(JSON.stringify(report, null, 2));
process.exitCode = trailing.trim() ? 1 : 0;
