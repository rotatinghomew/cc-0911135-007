// 一次性协议查看器：只为人工核对首段响应，不用于长期采集。
// 它读取有限字节后主动断开，避免把长流整段读进内存。
export async function inspect(base, { limitBytes = 64 * 1024, lastEventId = null, timeoutMs = 5000 } = {}) {
  const response = await fetch(base + '/stream', {
    headers: lastEventId ? { 'last-event-id': lastEventId } : {},
    signal: AbortSignal.timeout(timeoutMs),
  });
  const contentType = response.headers.get('content-type');
  if (!response.ok) {
    let body = null;
    try { body = await response.json(); } catch { /* 非 JSON */ }
    return { status: response.status, contentType, body };
  }
  const chunks = [];
  let bytes = 0;
  const reader = response.body.getReader();
  try {
    while (bytes < limitBytes) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(Buffer.from(value));
      bytes += value.length;
    }
  } catch (error) {
    // 超时或对端断开：返回已读到的部分，并注明原因。
    return {
      status: response.status, contentType, bytes,
      truncated: true, error: error.message,
      raw: Buffer.concat(chunks).toString('utf8'),
    };
  } finally {
    await reader.cancel().catch(() => {});
  }
  return {
    status: response.status, contentType, bytes,
    truncated: bytes >= limitBytes,
    raw: Buffer.concat(chunks).toString('utf8'),
  };
}
