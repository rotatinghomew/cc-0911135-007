import { parseArgs } from 'node:util';
import { mkdir, mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createSite, event } from '../src/site.mjs';
import { EventLog } from '../src/store.mjs';
import { runTap } from '../src/runtime.mjs';
import { SseParser } from '../src/sse.mjs';
import { verify } from '../lib/verify.mjs';

// 本地演示矩阵：每个阶段启动一个独立的合成网关，跑完后打印可复核的产物。
// 全部使用合成数据，不访问外网。
const { values } = parseArgs({ options: { keep: { type: 'boolean', default: false } } });
const root = values.keep ? path.join(process.cwd(), 'var', 'demo') : await mkdtemp(path.join(tmpdir(), 'tap-demo-'));
if (values.keep) await rm(root, { recursive: true, force: true });

const quiet = () => {};
const results = [];

async function readEvents(directory) {
  const text = await readFile(path.join(directory, 'events.jsonl'), 'utf8').catch(() => '');
  return text.trim() ? text.trim().split('\n').map(line => JSON.parse(line)) : [];
}

async function readStatus(directory) {
  return JSON.parse(await readFile(path.join(directory, 'stream-status.json'), 'utf8'));
}

async function stage(name, siteOptions, operation) {
  const directory = path.join(root, name);
  await mkdir(directory, { recursive: true });
  const site = await createSite(0, siteOptions);
  try {
    const detail = await operation(site, directory);
    const status = await readStatus(directory);
    const events = await readEvents(directory);
    verify({ ...status, committedCount: events.length }, { events });
    results.push({
      name, directory, committedSequence: status.committedSequence,
      state: status.state, events: events.map(record => record.sequence),
      gaps: status.gaps, conflicts: status.conflicts, detail,
    });
  } finally {
    await site.close();
  }
}

// 阶段 1：小字节块 + 半帧断开 + 重复推送。演示完整采集与原始字节重放。
await stage('01-happy', { tail: 8, chunkSize: 1, subdivide: true, duplicate: true, cutOnce: true, perConnection: 8 }, async (site, directory) => {
  await runTap({
    base: site.url, output: directory, maxConnections: 3, reconnectDelay: 10, logger: quiet,
  });
  const rawNames = (await readdir(path.join(directory, 'raw'))).filter(name => name.endsWith('.sse')).sort();
  const parser = new SseParser();
  const replayed = [];
  for (const name of rawNames) {
    const bytes = await readFile(path.join(directory, 'raw', name));
    for (let at = 0; at < bytes.length; at += 1) {
      parser.feed(bytes.subarray(at, at + 1));
      for (let next = parser.next(); next; next = parser.next()) {
        if (next.type === 'observation') replayed.push(JSON.parse(next.data).sequence);
      }
    }
  }
  assertRange(replayed, 1, 8, '重放', { unique: true });
  const audit = JSON.parse(`[${(await readFile(path.join(directory, 'audit.jsonl'), 'utf8')).trim().split('\n').join(',')}]`);
  return {
    rawFiles: rawNames.length, replayedObservations: replayed.length,
    uniqueReplayed: [...new Set(replayed)].length,
    cutReconnects: audit.filter(item => item.type === 'reconnect' && item.reason === 'cut').length,
    duplicates: (await readStatus(directory)).counters.eventsDuplicate,
  };
});

// 阶段 2：保留窗推进后的 409 回补。上界固定为 resumeFloor-1。
await stage('02-floor', { tail: 12, perConnection: 12 }, async (site, directory) => {
  const log = await EventLog.open(path.join(directory, 'events.jsonl'));
  for (const sequence of [1, 2, 3]) await log.append({ ...event(sequence), source: 'gateway', via: 'live' });
  await log.close();
  await (await fetch(site.url + '/_control', {
    method: 'POST', body: JSON.stringify({ floor: 6, tail: 12, perConnection: 12 }),
  })).text();
  await runTap({ base: site.url, output: directory, maxConnections: 2, reconnectDelay: 10, logger: quiet });
  const audit = JSON.parse(`[${(await readFile(path.join(directory, 'audit.jsonl'), 'utf8')).trim().split('\n').join(',')}]`);
  const start = audit.find(item => item.type === 'recovery-start');
  return { recoveryFrom: start?.from, recoveryUntil: start?.until, note: '上界为 resumeFloor-1，回补后从 evt-5 接回实时流' };
});

// 阶段 3：同一 ID 内容冲突。后写不得覆盖，通道转 needs-attention。
await stage('03-conflict', { tail: 5, duplicate: true, conflict: true, perConnection: 5 }, async (site, directory) => {
  await (await fetch(site.url + '/_control', {
    method: 'POST', body: JSON.stringify({ duplicate: true, conflict: true, perConnection: 5 }),
  })).text();
  await runTap({ base: site.url, output: directory, maxConnections: 1, reconnectDelay: 10, logger: quiet });
  return { note: '冲突副本未写入 events.jsonl，提交保持在原始内容' };
});

// 阶段 4：历史缺口无法补齐。提交停在缺口前，4、5 留在缓冲。
await stage('04-gap', { tail: 12, floor: 6, missing: 3 }, async (site, directory) => {
  const log = await EventLog.open(path.join(directory, 'events.jsonl'));
  for (const sequence of [1, 2]) await log.append({ ...event(sequence), source: 'gateway', via: 'live' });
  await log.close();
  await (await fetch(site.url + '/_control', {
    method: 'POST', body: JSON.stringify({ floor: 6, missing: 3, tail: 12, perConnection: 12 }),
  })).text();
  await runTap({ base: site.url, output: directory, maxConnections: 5, reconnectDelay: 10, logger: quiet });
  const status = await readStatus(directory);
  return { bufferedSequences: status.bufferedSequences, historyFetched: status.counters.historyFetched };
});

// 阶段 5：慢接收方与有界缓冲。背压断线重连，无静默丢弃。
await stage('05-backpressure', { tail: 24, perConnection: 24, chunkSize: 48 }, async (site, directory) => {
  await runTap({
    base: site.url, output: directory, maxConnections: 60, reconnectDelay: 1,
    bufferLimit: 3, commitDelayMs: 4, logger: quiet,
  });
  const status = await readStatus(directory);
  return {
    maxBuffered: status.maxBuffered, bufferLimit: status.bufferLimit,
    backpressureEvents: status.counters.backpressure,
  };
});

function assertRange(values, first, last, label, { unique = false } = {}) {
  const candidate = unique ? [...new Set(values)].sort((a, b) => a - b) : values;
  if (candidate.length !== last - first + 1 || candidate[0] !== first || candidate.at(-1) !== last
    || candidate.some((value, at) => value !== first + at)) {
    throw new Error(`${label} 的序号应为 ${first}..${last}，实际 ${candidate.join(',')}`);
  }
}

console.log(JSON.stringify({ phase: 'demo', root, stages: results }, null, 2));
console.log(JSON.stringify({
  phase: 'done',
  note: values.keep ? '产物保留在 var/demo，可运行 npm run verify -- var/demo/01-happy/stream-status.json' : '临时产物已清理',
}));
if (!values.keep) await rm(root, { recursive: true, force: true });
