import { serve, json } from '../lib/http.mjs';

export const event = (sequence, value = sequence / 10) => ({
  sequence,
  eventId: `evt-${sequence}`,
  sample: `M-${sequence}`,
  value,
  label: '籽粒镍',
});

export function frame(value) {
  return `id: ${value.eventId}\r\nevent: observation\r\ndata: ${JSON.stringify(value)}\r\n\r\n`;
}

// 把一帧拆成“字节块不对应消息边界”的片段：先按 UTF-8 字符和 CRLF 切开，
// 保证多字节字符与 \r\n 确实可能落在不同块里；再由 chunkSize 合并。
function shards(wire, chunkSize, subdivide) {
  if (!subdivide) {
    const bytes = Buffer.from(wire);
    const pieces = [];
    for (let at = 0; at < bytes.length; at += chunkSize) pieces.push(bytes.subarray(at, Math.min(bytes.length, at + chunkSize)));
    return pieces;
  }
  // 按字符边界切，但把 \r 与 \n 拆到两个片段里（模拟 CRLF 跨块）。
  const pieces = [];
  for (const character of wire) {
    if (character === '\r') { pieces.push(Buffer.from('\r')); continue; }
    pieces.push(Buffer.from(character));
  }
  const merged = [];
  for (let at = 0; at < pieces.length; at += chunkSize) {
    merged.push(Buffer.concat(pieces.slice(at, at + chunkSize)));
  }
  return merged;
}

const DEFAULTS = {
  tail: 12,
  floor: 1,
  missing: null,
  duplicate: false,
  conflict: false,
  cutOnce: false,
  chunkSize: 7,
  subdivide: true,
  perConnection: 12,
  slowWriteMs: 0,
};

export async function createSite(port = 0, initial = {}) {
  return serve(async ({ request, response, url, state }) => {
    if (url.pathname === '/history') {
      const from = Number(url.searchParams.get('from'));
      const until = Number(url.searchParams.get('until'));
      const offset = Number(url.searchParams.get('offset') || 0);
      if (!Number.isInteger(from) || !Number.isInteger(until) || from < 1 || until > state.tail || from > until) {
        throw new Error('历史区间无效');
      }
      if (!Number.isInteger(offset) || offset < 0) throw new Error('offset 无效');
      if (offset > until - from + 1) throw new Error('offset 越界');
      if (!Number.isInteger(state.chunkSize) || state.chunkSize < 1) throw new Error('chunkSize 必须是正整数');
      const missing = state.missing == null ? [] : [state.missing];
      const all = Array.from({ length: until - from + 1 }, (_, at) => from + at)
        .filter(sequence => !missing.includes(sequence))
        .map(sequence => event(sequence));
      const page = all.slice(offset, offset + 3);
      json(response, 200, {
        events: page,
        nextOffset: offset + page.length < all.length ? offset + page.length : null,
        from,
        until,
        total: all.length,
      });
      return true;
    }
    if (url.pathname !== '/stream') return false;
    if (!Number.isInteger(state.chunkSize) || state.chunkSize < 1) throw new Error('chunkSize 必须是正整数');

    const identifier = request.headers['last-event-id'] || 'evt-0';
    const match = /^evt-(\d+)$/.exec(identifier);
    if (!match) throw new Error('非法 Last-Event-ID');
    const last = Number(match[1]);
    if (last < state.floor - 1) {
      json(response, 409, {
        missingFrom: last + 1,
        missingUntil: state.floor - 1,
        resumeFloor: state.floor,
        observedTail: state.tail,
      });
      return true;
    }

    // 有限回放：每次连接只回放 perConnection 条，之后的尾部由下一次连接接续。
    const upTo = state.perConnection > 0 ? Math.min(state.tail, last + state.perConnection) : state.tail;
    const values = Array.from({ length: Math.max(0, upTo - last) }, (_, at) => event(last + at + 1));
    const missing = state.missing == null ? [] : [state.missing];
    const sendable = values.filter(value => !missing.includes(value.sequence));
    if (state.duplicate && sendable.length) {
      const copy = { ...sendable[0], ...(state.conflict ? { value: -99, sample: 'CONFLICT' } : {}) };
      sendable.splice(1, 0, copy);
    }

    let wire = ': heartbeat\r\n\r\n' + sendable.map(value => frame(value)).join('');
    wire += `event: notice\ndata: {"tail":\ndata: ${state.tail}\ndata: }\n\n`;

    response.writeHead(200, {
      'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-cache',
      'x-observed-tail': String(state.tail),
      'x-resume-floor': String(state.floor),
      'x-last-event-id': identifier,
    });

    const pieces = shards(wire, state.chunkSize, state.subdivide);
    const cut = state.cutOnce;
    state.cutOnce = false;
    // 半帧断开：切在片段总数三分之一处，不保证落在消息边界上。
    const stopAt = cut ? Math.max(1, Math.floor(pieces.length / 3)) : pieces.length;

    for (let at = 0; at < stopAt; at += 1) {
      if (response.destroyed) break;
      if (state.slowWriteMs > 0) await new Promise(resolve => setTimeout(resolve, state.slowWriteMs));
      let accepted = false;
      try {
        accepted = response.write(pieces[at]);
      } catch {
        break;
      }
      if (!accepted) {
        await new Promise(resolve => { response.once('drain', resolve); response.once('close', resolve); });
      }
      await new Promise(resolve => setImmediate(resolve));
    }
    if (cut) response.destroy();
    else response.end();
    return true;
  }, { ...DEFAULTS, ...initial }, port);
}
