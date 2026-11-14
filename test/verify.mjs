import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { verify } from '../lib/verify.mjs';

const filename = process.argv[2];
if (!filename) throw new Error('请指定需要校验的结果文件');

const status = JSON.parse(await readFile(filename, 'utf8'));

// 若同目录存在 events.jsonl，一并交叉核对提交顺序与条数。
let events = null;
if (process.argv.includes('--with-events')) {
  const eventsPath = path.join(path.dirname(path.resolve(filename)), 'events.jsonl');
  const text = await readFile(eventsPath, 'utf8').catch(error => {
    if (error.code === 'ENOENT') return '';
    throw error;
  });
  events = text.trim() ? text.trim().split('\n').map(line => JSON.parse(line)) : [];
}

verify(status, { events });
console.log('结果结构和基本业务不变量校验通过');
