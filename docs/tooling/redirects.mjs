import { glob, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';

// Preserve bookmarks to the original HTML guides, including query strings and anchors.
const output = resolve('out');
for await (const file of glob('**/index.html', { cwd: output })) {
  const id = file.split('/').slice(0, -1).join('/');
  if (!id || id === 'api' || id === '404' || id === '_not-found') continue;
  const previous = id === 'reference/operations' ? 'api' : id;
  const url = `/${id}/`;
  const html = `<!doctype html><html lang="en"><meta charset="utf-8">
<title>Page moved · Fusee Docs</title><meta name="robots" content="noindex">
<script>location.replace(${JSON.stringify(url)} + location.search + location.hash)</script>
<meta http-equiv="refresh" content="0;url=${url}"><a href="${url}">Open the documentation</a></html>`;
  await writeFile(resolve(output, previous + '.html'), html);
}
