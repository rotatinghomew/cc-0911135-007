import { parseArgs } from 'node:util';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { runTap } from './runtime.mjs';

const usage = `用法：node src/main.mjs --source <网关地址> [选项]

长期采集通道：从网关 /stream 接收 SSE observation，逐条落盘到 events.jsonl。
进程收到 SIGINT/SIGTERM 时先完成在途提交，再写出 stopped 状态并退出；
再次运行会以 events.jsonl 中最后一条连续记录作为 Last-Event-ID 续接。

选项：
  --source <url>      网关地址（必填，例如 http://127.0.0.1:8080）
  --output <dir>      落盘目录，默认 var/tap
  --buffer <n>        有界缓冲上限（条），默认 256
  --retain <n>        内存中保留的已提交内容索引条数，默认 20000
  --commit-delay <ms> 人为放慢持久化，用于演练慢接收方
  --max-connections <n> 最多连接次数（演练用，默认不限）
  --idle-stop <ms>    连续空闲该时长后主动停止（演练用，默认 0 表示不停）
  --reconnect-delay <ms> 重连间隔，默认 100
  --connect-timeout <ms> 单次连接超时，默认 20000
  --stall-timeout <ms> 背压等待上限，超时断线重连，默认 20000
  --quiet             不打印每个连接/摘要的结构化日志
  -h, --help          显示本帮助
`;

let values;
try {
  ({ values } = parseArgs({
    options: {
      source: { type: 'string' },
      output: { type: 'string', default: 'var/tap' },
      buffer: { type: 'string', default: '256' },
      retain: { type: 'string', default: '20000' },
      'commit-delay': { type: 'string', default: '0' },
      'max-connections': { type: 'string', default: 'Infinity' },
      'idle-stop': { type: 'string', default: '0' },
      'reconnect-delay': { type: 'string', default: '100' },
      'connect-timeout': { type: 'string', default: '20000' },
      'stall-timeout': { type: 'string', default: '20000' },
      quiet: { type: 'boolean', default: false },
      help: { type: 'boolean', short: 'h', default: false },
    },
  }));
} catch (error) {
  console.error(error.message);
  console.error(usage);
  process.exit(2);
}

if (values.help) {
  console.log(usage);
  process.exit(0);
}
if (!values.source) {
  console.error('缺少 --source 网关地址');
  console.error(usage);
  process.exit(2);
}

function positive(value, name, { allowZero = false, integer = true } = {}) {
  const number = Number(value);
  if (!Number.isFinite(number) || number < (allowZero ? 0 : 1) || (integer && !Number.isInteger(number))) {
    console.error(`${name} 必须是${allowZero ? '非负' : '正'}${integer ? '整数' : '数'}：${value}`);
    process.exit(2);
  }
  return number;
}

const maxConnections = values['max-connections'] === 'Infinity'
  ? Infinity : positive(values['max-connections'], '--max-connections');

const abort = new AbortController();
const stop = () => abort.abort();
process.once('SIGINT', stop);
process.once('SIGTERM', stop);

const logger = values.quiet ? () => {} : value => console.log(JSON.stringify(value));

try {
  const summary = await runTap({
    base: values.source.replace(/\/$/, ''),
    output: path.resolve(values.output),
    bufferLimit: positive(values.buffer, '--buffer'),
    retain: positive(values.retain, '--retain'),
    commitDelayMs: positive(values['commit-delay'], '--commit-delay', { allowZero: true }),
    maxConnections,
    idleStop: positive(values['idle-stop'], '--idle-stop', { allowZero: true }),
    reconnectDelay: positive(values['reconnect-delay'], '--reconnect-delay', { allowZero: true }),
    connectTimeout: positive(values['connect-timeout'], '--connect-timeout'),
    stallTimeout: positive(values['stall-timeout'], '--stall-timeout'),
    signal: abort.signal,
    logger,
  });
  process.exitCode = summary.state === 'needs-attention' ? 3 : 0;
} catch (error) {
  console.error(JSON.stringify({ type: 'fatal', message: error.message, details: error.details ?? null }));
  process.exitCode = 1;
}

// 便于诊断：允许读取一些状态文件而不必再解析格式。
export async function readStatus(directory) {
  return JSON.parse(await readFile(path.join(directory, 'stream-status.json'), 'utf8'));
}
