import { serve, json } from '../lib/http.mjs';
export const event = sequence => ({ sequence, eventId: `evt-${sequence}`, sample: `M-${sequence}`, value: sequence / 10, label: '籽粒镍' });
export function frame(value) {
  return `id: ${value.eventId}\r\nevent: observation\r\ndata: ${JSON.stringify(value)}\r\n\r\n`;
}
export async function createSite(port = 0) {
  return serve(async ({ request, response, url, state }) => {
    if (url.pathname === '/history') {
      const from = Number(url.searchParams.get('from'));
      const until = Number(url.searchParams.get('until'));
      const offset = Number(url.searchParams.get('offset') || 0);
      if (!Number.isInteger(from) || !Number.isInteger(until) || from < 1 || until > state.tail || from > until) throw new Error('历史区间无效');
      const all = Array.from({ length: until - from + 1 }, (_, position) => event(from + position)).filter(value => value.sequence !== state.missing);
      json(response, 200, { events: all.slice(offset, offset + 3), nextOffset: offset + 3 < all.length ? offset + 3 : null, from, until });
      return true;
    }
    if (url.pathname !== '/stream') return false;
    const identifier = request.headers['last-event-id'] || 'evt-0';
    const match = /^evt-(\d+)$/.exec(identifier);
    if (!match) throw new Error('非法 Last-Event-ID');
    const last = Number(match[1]);
    if (last < state.floor - 1) {
      json(response, 409, { missingFrom: last + 1, missingUntil: state.floor - 1, resumeFloor: state.floor, observedTail: state.tail }); return true;
    }
    response.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8', 'cache-control': 'no-cache' });
    const values = Array.from({ length: Math.max(0, state.tail - last) }, (_, position) => event(last + position + 1));
    if (state.duplicate && values.length) values.splice(1, 0, { ...values[0], ...(state.conflict ? { value: -99 } : {}) });
    let wire = ': heartbeat\r\n\r\n' + values.map(value => frame(value)).join('') + 'event: notice\ndata: {"tail":\ndata: ' + state.tail + '}\n\n';
    const bytes = Buffer.from(wire);
    const stop = state.cutOnce ? Math.floor(bytes.length / 3) : bytes.length;
    const cut = state.cutOnce; state.cutOnce = false;
    for (let offset = 0; offset < stop; offset += state.chunkSize) {
      if (response.destroyed) break;
      if (!response.write(bytes.subarray(offset, Math.min(stop, offset + state.chunkSize)))) {
        await new Promise(resolve => { response.once('drain', resolve); response.once('close', resolve); });
      }
      await new Promise(resolve => setImmediate(resolve));
    }
    if (cut) response.destroy(); else response.end();
    return true;
  }, { tail: 12, floor: 1, missing: null, duplicate: false, conflict: false, cutOnce: false, chunkSize: 7 }, port);
}
