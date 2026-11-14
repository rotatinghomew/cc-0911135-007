import { parseArgs } from 'node:util';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createSite, event } from '../src/site.mjs';
import { runTap } from '../src/runtime.mjs';
import { EventLog, validateObservation } from '../src/store.mjs';
import { verify } from '../lib/verify.mjs';

// 演练矩阵：每个情景启动真实的本地网关与本仓库的接收端，跑完后用验收器
// 复核 stream-status.json 与 events.jsonl。全部使用合成数据。
const scenarios = {
  'small-chunks': {
    site: { chunkSize: 1, subdivide: true, tail: 9 },
    run: { maxConnections: 3 },
    check: async ({ status, events }) => {
      assertEqual(events.map(item => item.sequence), [1, 2, 3, 4, 5, 6, 7, 8, 9]);
      assertEqual(status.counters.eventsReceived, 9);
      return 'UTF-8、CRLF、多行 data 均完整，心跳与 notice 未落事件';
    },
  },
  'half-frame': {
    site: { cutOnce: true, tail: 10, chunkSize: 5 },
    control: { cutOnce: true },
    run: { maxConnections: 2 },
    check: async ({ status, events, audit }) => {
      assertEqual(events.map(item => item.sequence), Array.from({ length: 10 }, (_, at) => at + 1));
      const cut = audit.find(item => item.type === 'reconnect' && item.reason === 'cut');
      if (!cut) throw new Error('未观察到半帧断线');
      const connects = audit.filter(item => item.type === 'connect');
      const second = connects[1];
      if (second.lastEventId !== `evt-${second.sequence}`) throw new Error('重连游标不是已提交值');
      return `半帧未确认，重连使用 ${second.lastEventId}`;
    },
  },
  duplicate: {
    site: { duplicate: true, tail: 8 },
    control: { duplicate: true, perConnection: 8 },
    run: { maxConnections: 2 },
    check: async ({ status, events }) => {
      assertEqual(events.map(item => item.sequence), Array.from({ length: 8 }, (_, at) => at + 1));
      if (status.counters.eventsDuplicate < 1) throw new Error('未识别重复');
      return `相同 ID 同内容只提交一次（识别 ${status.counters.eventsDuplicate} 次重复）`;
    },
  },
  conflict: {
    site: { duplicate: true, conflict: true, tail: 6 },
    control: { duplicate: true, conflict: true, perConnection: 6 },
    run: { maxConnections: 1 },
    check: async ({ status, events }) => {
      if (events.some(item => item.value === -99)) throw new Error('冲突内容被写入');
      if (status.state !== 'needs-attention') throw new Error('冲突未转入需处理状态');
      return '冲突未覆盖已提交内容，状态 needs-attention';
    },
  },
  'floor-window': {
    site: { tail: 12, floor: 1 },
    seed: 3,
    control: { floor: 6, tail: 12, perConnection: 12 },
    run: { maxConnections: 2 },
    check: async ({ status, events, audit }) => {
      assertEqual(events.map(item => item.sequence), Array.from({ length: 12 }, (_, at) => at + 1));
      const start = audit.find(item => item.type === 'recovery-start');
      if (!start) throw new Error('未触发回补');
      if (start.until !== 5) throw new Error(`补采上界应为 5，实际 ${start.until}`);
      return `409 后回补 4..5（上界固定），再从 evt-5 接回实时流，最终连续到 ${status.committedSequence}`;
    },
  },
  'history-gap': {
    site: { tail: 12, floor: 6, missing: 3 },
    seed: 2,
    control: { floor: 6, missing: 3, tail: 12, perConnection: 12 },
    run: { maxConnections: 5 },
    check: async ({ status, events }) => {
      if (status.state !== 'needs-attention') throw new Error('缺口未使通道转入需处理');
      if (status.committedSequence !== 2) throw new Error('提交跨过了缺口');
      if (!status.gaps.some(gap => gap.from === 3 && gap.unfillable)) throw new Error('缺口 3 未记录');
      if (events.some(item => item.sequence > 2)) throw new Error('事件日志跨过了缺口');
      if (status.counters.historyFetched < 2) throw new Error('未真正尝试补采 4、5');
      return '缺口 3 无法补齐：提交停在 2，4、5 留在缓冲，状态 needs-attention';
    },
  },
  'tail-growth': {
    site: { tail: 12 },
    seed: 3,
    control: { floor: 6, tail: 12, perConnection: 12 },
    grow: { everyMs: 5, step: 2, until: 20 },
    run: { maxConnections: 3 },
    check: async ({ status, events, audit }) => {
      const start = audit.find(item => item.type === 'recovery-start');
      if (start.until !== 5) throw new Error(`补采上界被尾部增长扩大：${start.until}`);
      assertEqual(events.map(item => item.sequence), Array.from({ length: 20 }, (_, at) => at + 1));
      return `补采上界固定为 5，尾部增长到 20 由实时流补齐（共 ${status.committedSequence} 条）`;
    },
  },
  'floor-multipage': {
    site: { tail: 12, floor: 10 },
    seed: 4,
    control: { floor: 10, tail: 12, perConnection: 12 },
    run: { maxConnections: 2 },
    check: async ({ status, events, audit }) => {
      assertEqual(events.map(item => item.sequence), Array.from({ length: 12 }, (_, at) => at + 1));
      const start = audit.find(item => item.type === 'recovery-start');
      if (start.from !== 5 || start.until !== 9) throw new Error(`回补区间应为 5..9，实际 ${start.from}..${start.until}`);
      const connects = audit.filter(item => item.type === 'connect');
      if (connects.at(-1).lastEventId !== 'evt-9') throw new Error('未从 resumeFloor-1 接回');
      return '多页回补 5..9，从 evt-9 接回实时流并补齐到 12';
    },
  },
  'beyond-floor': {
    site: { tail: 8, floor: 10 },
    seed: 2,
    control: { floor: 10, tail: 8, perConnection: 8 },
    run: { maxConnections: 3 },
    check: async ({ status, events }) => {
      if (status.committedSequence !== 8) throw new Error('可补区间未补齐');
      if (status.state !== 'needs-attention') throw new Error('超出部分未转需处理');
      if (!status.gaps.some(gap => gap.from === 9 && gap.until === 9 && gap.unfillable)) throw new Error('缺口 9 未标记不可补齐');
      return '3..8 可补部分已补齐；9 超出 observedTail，明确不可补齐并停止';
    },
  },
  'slow-consumer': {
    site: { tail: 40, perConnection: 40, chunkSize: 64 },
    run: { bufferLimit: 3, commitDelayMs: 4, maxConnections: 60, reconnectDelay: 1 },
    check: async ({ status, events, audit }) => {
      if (status.maxBuffered > status.bufferLimit) throw new Error('缓冲超过上限');
      if (status.counters.backpressure < 1) throw new Error('未观察到背压');
      const discard = audit.find(item => item.type === 'buffer-discard');
      if (!discard || !discard.dropped.length) throw new Error('背压断线未记录丢弃的未确认缓冲');
      assertEqual(events.map(item => item.sequence), Array.from({ length: 40 }, (_, at) => at + 1));
      return `峰值 ${status.maxBuffered}/${status.bufferLimit}，背压断线 ${status.counters.backpressure} 次后补齐全部 40 条`;
    },
  },
  'restart-resume': {
    site: { tail: 15, perConnection: 15 },
    restart: { tail: 30, perConnection: 30 },
    run: { maxConnections: 1 },
    check: async ({ status, events, audit }) => {
      const started = audit.filter(item => item.type === 'started');
      if (started.length !== 2) throw new Error('未观察到两次运行');
      assertEqual(events.map(item => item.sequence), Array.from({ length: 30 }, (_, at) => at + 1));
      return `第一次运行停在 ${started[1].resumeSequence}，第二次从 ${started[1].resumeEventId} 续接，无重复无跳号`;
    },
  },
};

function assertEqual(actual, expected) {
  const a = JSON.stringify(actual);
  const b = JSON.stringify(expected);
  if (a !== b) throw new Error(`期望 ${b}，实际 ${a}`);
}

async function readEvents(directory) {
  const text = await readFile(path.join(directory, 'events.jsonl'), 'utf8').catch(error => {
    if (error.code === 'ENOENT') return '';
    throw error;
  });
  return text.trim() ? text.trim().split('\n').map(line => JSON.parse(line)) : [];
}

async function readAudit(directory) {
  const text = await readFile(path.join(directory, 'audit.jsonl'), 'utf8').catch(() => '');
  return text.trim() ? text.trim().split('\n').map(line => JSON.parse(line)) : [];
}

async function seedLog(directory, count) {
  const log = await EventLog.open(path.join(directory, 'events.jsonl'));
  for (let sequence = 1; sequence <= count; sequence += 1) {
    await log.append({ ...event(sequence), source: 'gateway', via: 'live' });
  }
  await log.close();
}

async function runScenario(name) {
  const scenario = scenarios[name];
  const directory = await mkdtemp(path.join(tmpdir(), `drill-${name}-`));
  const site = await createSite(0, scenario.site);
  const growTimer = scenario.grow ? setInterval(() => {
    site.state.tail = Math.min(scenario.grow.until, site.state.tail + scenario.grow.step);
  }, scenario.grow.everyMs) : null;
  try {
    if (scenario.seed) await seedLog(directory, scenario.seed);
    if (scenario.control) {
      await fetch(site.url + '/_control', { method: 'POST', body: JSON.stringify(scenario.control) });
    }

    const runs = [];
    if (scenario.restart) {
      runs.push(await runTap({ base: site.url, output: directory, logger: () => {}, reconnectDelay: 5, ...scenario.run }));
      await fetch(site.url + '/_control', { method: 'POST', body: JSON.stringify(scenario.restart) });
      runs.push(await runTap({ base: site.url, output: directory, logger: () => {}, reconnectDelay: 5, ...scenario.run }));
    } else {
      runs.push(await runTap({ base: site.url, output: directory, logger: () => {}, reconnectDelay: 5, ...scenario.run }));
    }

    const status = JSON.parse(await readFile(path.join(directory, 'stream-status.json'), 'utf8'));
    const events = await readEvents(directory);
    const audit = await readAudit(directory);
    // 交叉验收：状态文件与事件日志必须互相印证。
    const crossCheck = events.every((record, at) => record.sequence === at + 1 && record.eventId === `evt-${at + 1}`);
    verify({ ...status, committedCount: events.length }, { events: crossCheck ? events : null });

    const note = await scenario.check({ status, events, audit, runs, directory });
    return { name, ok: true, note, committed: status.committedSequence, state: status.state };
  } finally {
    if (growTimer) clearInterval(growTimer);
    await site.close();
    await rm(directory, { recursive: true, force: true });
  }
}

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: { list: { type: 'boolean', default: false }, help: { type: 'boolean', short: 'h', default: false } },
});

if (values.help) {
  console.log(`用法：node tools/drill.mjs [情景名...] [--list]

可用情景：${Object.keys(scenarios).join(', ')}
不带情景名时运行全部。全部使用本地合成数据。`);
  process.exit(0);
}
if (values.list) {
  for (const name of Object.keys(scenarios)) console.log(name);
  process.exit(0);
}

const selected = positionals.length ? positionals : Object.keys(scenarios);
const unknown = selected.filter(name => !scenarios[name]);
if (unknown.length) {
  console.error(`未知情景：${unknown.join(', ')}`);
  process.exit(2);
}

let failures = 0;
for (const name of selected) {
  try {
    const result = await runScenario(name);
    console.log(JSON.stringify({ ...result, type: 'drill' }));
  } catch (error) {
    failures += 1;
    console.log(JSON.stringify({ name, ok: false, type: 'drill', error: error.message }));
    if (process.env.DRILL_VERBOSE) console.error(error);
  }
}
console.log(JSON.stringify({ type: 'drill-summary', scenarios: selected.length, failures }));
process.exitCode = failures ? 1 : 0;
