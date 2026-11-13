export async function inspect(base) {
  const response = await fetch(base + '/stream');
  const bytes = await response.arrayBuffer();
  return { status: response.status, contentType: response.headers.get('content-type'), bytes: bytes.byteLength,
    raw: new TextDecoder().decode(bytes) };
}
