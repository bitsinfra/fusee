// The SDKs, deployment guide, and examples each keep their README as the source.
export const externalPages = {
  'deploy/README.md': 'deploying',
  'examples/README.md': 'examples',
  'bindings/node/README.md': 'sdks/typescript',
  'bindings/python/README.md': 'sdks/python',
  'bindings/go/README.md': 'sdks/go',
  'bindings/java/README.md': 'sdks/java',
};

export function pageId(source) {
  if (externalPages[source]) return externalPages[source];
  if (source === 'docs/api.md') return 'reference/operations';
  if (source === 'docs/api/index.md') return 'api';
  return source.replace(/^docs\//, '').replace(/\.mdx?$/, '');
}

export const sidebar = [
  { label: 'Getting started', items: [
    { slug: 'index', label: 'Introduction' }, 'installation', 'tutorial-typescript',
    'tutorial-python', 'tutorial-rust', 'concepts', 'examples',
  ] },
  { label: 'Guides', items: [
    'guides/errors-and-retries', { slug: 'guides/sagas', label: 'Sagas and compensation' },
    { slug: 'guides/human-approval', label: 'Human approval' }, 'guides/agents',
    { slug: 'guides/agent-patterns', label: 'Agent patterns' },
    { slug: 'guides/coding-agents', label: 'Writing with an AI assistant' },
    'guides/schedules', 'testing-workflows', 'guides/versioning',
  ] },
  { label: 'API reference', collapsed: true, items: [
    'api', 'reference/operations', 'sdks/typescript', 'sdks/python', 'sdks/go',
    'sdks/java', 'features', 'errors', 'guarantees',
  ] },
  { label: 'Deployment and operations', collapsed: true, items: [
    'guides/running', 'durability', 'server-mode', 'replication', 'deploying', 'operations',
  ] },
  { label: 'Internals', collapsed: true, items: ['benchmark', 'testing'] },
];
