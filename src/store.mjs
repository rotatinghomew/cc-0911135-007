import { mkdir, open, readFile, readdir, rename, rm, stat, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import path from 'node:path';

// 本地记录中的元数据字段不参与内容摘要，因此源端重复内容与本地的
// 提交时间、来源通道无关。
const META_FIELDS = new Set(['source', 'committedAt', 'receivedAt', 'via', 'connection', 'digest']);
const DEFAULT_RETAIN = 20000;

function stableStringify(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return '[' + value.map(stableStringify).join(',') + ']';
  const keys = Object.keys(value).sort();
  return '{' + keys.map(key => JSON.stringify(key) + ':' + stableStringify(value[key])).join(',') + '}';
}

export function digestOf(value) {
  const business = {};
  for (const [key, item] of Object.entries(value)) if (!META_FIELDS.has(key)) business[key] = item;
  return createHash('sha256').update(stableStringify(business)).digest('hex').slice(0, 32);
}

export class DataError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = 'DataError';
    this.details = details;
  }
}

export function validateObservation(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new DataError('observation 必须是 JSON 对象');
  }
  if (!Number.isSafeInteger(value.sequence) || value.sequence < 1) {
    throw new DataError('observation.sequence 必须是正整数', { value });
  }
  if (typeof value.eventId !== 'string' || value.eventId.length === 0) {
    throw new DataError('observation.eventId 必须是非空字符串', { sequence: value.sequence });
  }
  if (value.eventId.includes('\u0000')) {
    throw new DataError('observation.eventId 不能包含 NUL', { sequence: value.sequence });
  }
  return value;
}

async function writeAll(handle, bytes) {
  let offset = 0;
  while (offset < bytes.length) {
    const { bytesWritten } = await handle.write(bytes, offset, bytes.length - offset);
    if (bytesWritten <= 0) throw new Error('写入未取得进展');
    offset += bytesWritten;
  }
}

async function syncDirectory(directory) {
  let handle;
  try {
    handle = await open(directory, 'r');
    await handle.sync();
  } catch (error) {
    // 少数文件系统不支持目录 fsync；rename 本身仍然是原子的。
    if (!['EINVAL', 'ENOTSUP', 'EISDIR', 'EPERM'].includes(error.code)) throw error;
  } finally {
    await handle?.close();
  }
}

// 追加式事件日志。append() 在返回前完成 fsync，因此“已持久化”不依赖内存状态。
// 打开时逐块流式校验：丢弃崩溃留下的半行，发现真正的坏行则拒绝启动，
// 但只保留最近 retain 条记录，内存不随历史长度增长。
export class EventLog {
  constructor(filename, handle, state, retain) {
    this.filename = filename;
    this.handle = handle;
    this.recent = state.recent;
    this.count = state.count;
    this.last = state.last;
    this.bytes = state.bytes;
    this.truncatedBytes = state.truncatedBytes;
    this.appends = 0;
    this.retain = retain;
  }

  static async open(filename, { retain = DEFAULT_RETAIN } = {}) {
    await mkdir(path.dirname(filename), { recursive: true });
    const state = { recent: [], count: 0, last: null, bytes: 0, truncatedBytes: 0 };
    let totalBytes = 0;
    let validBytes = 0;

    let source = null;
    try {
      source = await open(filename, 'r');
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    if (source) {
      try {
        const buffer = Buffer.allocUnsafe(64 * 1024);
        let position = 0;
        let trailing = Buffer.alloc(0);
        while (true) {
          const { bytesRead } = await source.read(buffer, 0, buffer.length, position);
          if (!bytesRead) break;
          position += bytesRead;
          totalBytes = position;
          const joined = trailing.length
            ? Buffer.concat([trailing, buffer.subarray(0, bytesRead)])
            : Buffer.from(buffer.subarray(0, bytesRead));
          let start = 0;
          for (;;) {
            const newlineAt = joined.indexOf(0x0a, start);
            if (newlineAt === -1) break;
            const line = joined.toString('utf8', start, newlineAt);
            start = newlineAt + 1;
            if (!line.trim()) continue;
            let record;
            try {
              record = JSON.parse(line);
            } catch {
              throw new DataError(`${filename} 存在无法解析的完整行`, { fromByte: validBytes });
            }
            validateObservation(record);
            if (record.digest && record.digest !== digestOf(record)) {
              throw new DataError(`${filename} 第 ${record.sequence} 行内容与摘要不符`, {
                sequence: record.sequence,
              });
            }
            if (state.last && record.sequence !== state.last.sequence + 1) {
              throw new DataError(`${filename} 的提交序列不连续`, {
                previous: state.last.sequence, next: record.sequence,
              });
            }
            state.last = record;
            state.count += 1;
            state.recent.push(record);
            if (state.recent.length > retain) state.recent.shift();
          }
          // 按原始字节累计，避免解码器跨块缓存多字节字符造成偏移漂移。
          validBytes += start;
          trailing = joined.subarray(start);
        }
        if (trailing.toString('utf8').trim()) validBytes = totalBytes - trailing.length;
      } finally {
        await source.close();
      }
    }

    if (totalBytes > validBytes) {
      const repair = await open(filename, 'r+');
      try {
        await repair.truncate(validBytes);
        await repair.sync();
      } finally {
        await repair.close();
      }
    }

    const handle = await open(filename, 'a');
    state.bytes = validBytes;
    state.truncatedBytes = totalBytes - validBytes;
    return new EventLog(filename, handle, state, retain);
  }

  async append(record, { fsync = true } = {}) {
    const line = Buffer.from(JSON.stringify(record) + '\n', 'utf8');
    await writeAll(this.handle, line);
    if (fsync) await this.handle.sync();
    this.bytes += line.length;
    this.count += 1;
    this.last = record;
    this.appends += 1;
    this.recent.push(record);
    if (this.recent.length > this.retain) this.recent.shift();
    return record;
  }

  async close() {
    if (!this.handle) return;
    await this.handle.sync().catch(() => {});
    await this.handle.close();
    this.handle = null;
  }
}

export class StatusWriter {
  constructor(filename) {
    this.filename = filename;
    this.counter = 0;
  }

  async read() {
    try {
      return JSON.parse(await readFile(this.filename, 'utf8'));
    } catch (error) {
      if (error.code === 'ENOENT') return null;
      throw error;
    }
  }

  // 临时文件 + fsync + rename：读者永远看不到半个状态文件。
  async write(value) {
    await mkdir(path.dirname(this.filename), { recursive: true });
    const temporary = `${this.filename}.tmp-${process.pid}-${this.counter++}`;
    const handle = await open(temporary, 'w');
    try {
      await handle.writeFile(JSON.stringify(value, null, 2) + '\n', 'utf8');
      await handle.sync();
    } finally {
      await handle.close();
    }
    await rename(temporary, this.filename);
    await syncDirectory(path.dirname(this.filename));
  }

  async removeTemporary() {
    const directory = path.dirname(this.filename);
    const base = path.basename(this.filename);
    let names;
    try {
      names = await readdir(directory);
    } catch (error) {
      if (error.code === 'ENOENT') return;
      throw error;
    }
    for (const name of names) {
      if (name.startsWith(base + '.tmp-')) await rm(path.join(directory, name), { force: true }).catch(() => {});
    }
  }
}

// 生命周期审计日志：连接、背压、提交、回补、冲突、停机都按顺序追加。
export class JsonlLog {
  constructor(filename, handle) {
    this.filename = filename;
    this.handle = handle;
  }

  static async open(filename) {
    await mkdir(path.dirname(filename), { recursive: true });
    return new JsonlLog(filename, await open(filename, 'a'));
  }

  async append(type, value = {}) {
    if (!this.handle) return;
    const line = Buffer.from(JSON.stringify({ at: new Date().toISOString(), type, ...value }) + '\n', 'utf8');
    await writeAll(this.handle, line);
  }

  async close() {
    if (!this.handle) return;
    await this.handle.sync().catch(() => {});
    await this.handle.close();
    this.handle = null;
  }
}

// 原始字节按连接落盘；重放工具只读这些文件，不经过任何业务状态。
export class RawCapture {
  static async open(directory, connection, meta = {}) {
    await mkdir(directory, { recursive: true });
    const file = path.join(directory, `conn-${String(connection).padStart(6, '0')}.sse`);
    const handle = await open(file, 'w');
    const capture = new RawCapture(file, handle, connection, meta);
    await capture.writeMeta({ status: 'open', startedAt: new Date().toISOString() });
    return capture;
  }

  constructor(file, handle, connection, meta) {
    this.file = file;
    this.handle = handle;
    this.connection = connection;
    this.meta = { ...meta };
    this.bytes = 0;
    this.frames = 0;
    this.closed = false;
  }

  get name() {
    return path.basename(this.file);
  }

  async write(chunk) {
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    await writeAll(this.handle, bytes);
    this.bytes += bytes.length;
  }

  countFrame() {
    this.frames += 1;
  }

  async writeMeta(extra = {}) {
    const value = {
      ...this.meta, ...extra,
      connection: this.connection,
      file: this.name,
      bytes: this.bytes,
      frames: this.frames,
    };
    this.meta = value;
    const target = `${this.file}.meta.json`;
    const temporary = `${target}.tmp`;
    await writeFile(temporary, JSON.stringify(value, null, 2) + '\n');
    await rename(temporary, target);
    return value;
  }

  record(value) {
    this.meta = { ...this.meta, ...value };
  }

  async close(extra = {}) {
    if (this.closed) return this.meta;
    this.closed = true;
    await this.handle.sync().catch(() => {});
    await this.handle.close();
    return this.writeMeta({ ...extra, status: extra.status || 'closed', endedAt: new Date().toISOString() });
  }
}

export async function fileExists(filename) {
  try { await stat(filename); return true; } catch { return false; }
}
