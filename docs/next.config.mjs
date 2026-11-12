import { createMDX } from 'fumadocs-mdx/next';
import { fileURLToPath } from 'node:url';
import { prepareContent } from './tooling/content.mjs';

prepareContent(process.env.NODE_ENV === 'development');

export default createMDX({ macro: false })({
  output: 'export',
  agentRules: false,
  trailingSlash: true,
  turbopack: { root: fileURLToPath(new URL('.', import.meta.url)) },
});
