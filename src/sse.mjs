// 字节流 SSE 解析器。
//
// 输入是任意切分的字节块（可能切在 UTF-8 字符、CRLF、多行 data 中间）。
// 解析规则按 WHATWG HTML 的 server-sent events 一节实现：
//   - 行结束符为 CRLF、CR 或 LF；CR 位于块尾时该行可以立即结束，
//     下一个块若以 LF 开头则消掉，因此 CRLF 跨块不会产生空行；
//   - BOM 只允许出现在流首；
//   - 空行才派发事件；
//   - 注释行（以 ":" 开头）只是心跳，不改变事件缓冲；
//   - data 多行以 "\n" 连接；行尾单个 CR/LF 在连接时剔除；
//   - id 值含 NUL 时忽略该字段；
//   - 未遇到空行就结束的帧整体丢弃，绝不当成完整事件。
// 本接收端只把 event 字段缺失或为 "observation" 的帧交给业务层；
// 其它命名事件（例如 notice）按协议忽略，但同样要求完整帧边界。

const LF = 0x0a;
const CR = 0x0d;
const COLON = 0x3a;
const SPACE = 0x20;

export class SseParser {
  #text = '';
  #started = false;
  #skipLeadingLf = false;
  #queue = [];
  // 解码器必须跨块保留内部状态；每次 feed 新建会把拆开的多字节字符变成乱码。
  #decoder = new TextDecoder();

  #data = [];
  #event = '';
  #id = '';
  #idPresent = false;
  // 尚未派发的原始行；只用于断线诊断，绝不参与业务。
  #raw = [];

  feed(chunk) {
    let text;
    if (chunk instanceof Uint8Array) {
      text = this.#decoder.decode(chunk, { stream: true });
    } else {
      text = String(chunk);
    }
    if (!this.#started && text.charCodeAt(0) === 0xfeff) text = text.slice(1);
    this.#started = true;
    this.#text += text;
    this.#drain();
  }

  #drain() {
    let text = this.#text;
    let start = 0;
    while (start < text.length) {
      if (this.#skipLeadingLf) {
        this.#skipLeadingLf = false;
        if (text.charCodeAt(start) === LF) { start += 1; continue; }
      }
      const rest = text.slice(start);
      const at = rest.search(/[\r\n]/);
      if (at === -1) break;
      this.#parseLine(rest.slice(0, at));
      start += at + 1;
      if (rest.charCodeAt(at) === CR) this.#skipLeadingLf = true;
    }
    this.#text = text.slice(start);
  }

  #parseLine(line) {
    if (line === '') {
      this.#dispatch();
      return;
    }
    this.#raw.push(line);
    if (line.charCodeAt(0) === COLON) return;
    const colonAt = line.indexOf(':');
    const name = colonAt === -1 ? line : line.slice(0, colonAt);
    const value = colonAt === -1 ? '' : line.slice(colonAt + 1).charCodeAt(0) === SPACE
      ? line.slice(colonAt + 2) : line.slice(colonAt + 1);
    switch (name) {
      case 'data': this.#data.push(value); break;
      case 'event': this.#event = value; break;
      case 'id': if (!value.includes('\u0000')) { this.#id = value; this.#idPresent = true; } break;
      case 'retry': break;
      default: break;
    }
  }

  #dispatch() {
    if (this.#data.length) {
      this.#queue.push({
        type: this.#event || 'message',
        data: this.#data.join('\n'),
        id: this.#idPresent ? this.#id : null,
      });
    }
    this.#data = [];
    this.#event = '';
    this.#raw = [];
  }

  // 取下一个完整派发的事件；半帧永远不在队列里。
  next() {
    return this.#queue.shift() ?? null;
  }

  // 尚未见到空行的残余内容；断线时用于日志/诊断，不会当成事件。
  partialText() {
    const pending = this.#raw.join('\n');
    if (!this.#text) return pending;
    return pending ? pending + '\n' + this.#text : this.#text;
  }

  resetConnection() {
    this.#text = '';
    this.#skipLeadingLf = false;
    this.#data = [];
    this.#event = '';
    this.#raw = [];
    this.#decoder = new TextDecoder();
    this.#queue = [];
  }
}
