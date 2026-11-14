export class HistoryError extends Error {
  constructor(message, status, body) {
    super(message);
    this.name = 'HistoryError';
    this.status = status;
    this.body = body;
  }
}

// 历史接口分页读取。from/until 在整个补采过程中固定，offset 只负责翻页；
// 调用方负责按页提交，避免把一整段历史同时压进内存。
export function createHistory(base, { timeoutMs = 15000 } = {}) {
  return {
    async fetch({ from, until, offset }) {
      const url = new URL('/history', base);
      url.searchParams.set('from', String(from));
      url.searchParams.set('until', String(until));
      url.searchParams.set('offset', String(offset));
      const response = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
      let value = null;
      try {
        value = await response.json();
      } catch {
        // 保持 body 为空，交给下面的状态码判断。
      }
      if (!response.ok) {
        throw new HistoryError(`历史接口返回 ${response.status}`, response.status, value);
      }
      if (!value || !Array.isArray(value.events)) {
        throw new HistoryError('历史接口响应缺少 events 数组', response.status, value);
      }
      for (const event of value.events) {
        if (!Number.isSafeInteger(event?.sequence)) {
          throw new HistoryError('历史接口返回了无序号的事件', response.status, event);
        }
      }
      const nextOffset = value.nextOffset ?? null;
      if (nextOffset !== null && (!Number.isSafeInteger(nextOffset) || nextOffset <= offset)) {
        throw new HistoryError('历史接口 nextOffset 未向前推进', response.status, value);
      }
      return {
        events: value.events,
        nextOffset,
        from: Number.isInteger(value.from) ? value.from : from,
        until: Number.isInteger(value.until) ? value.until : until,
      };
    },
  };
}
