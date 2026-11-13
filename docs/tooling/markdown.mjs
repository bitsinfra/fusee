import { statSync } from 'node:fs';
import { dirname, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { visit } from 'unist-util-visit';
import { externalPages, pageId } from './navigation.mjs';

const repo = fileURLToPath(new URL('../../', import.meta.url));
const repoUrl = 'https://github.com/bitsinfra/fusee';

export function repositoryMarkdown() {
  return (tree, file) => {
    const heading = tree.children.findIndex((node) => node.type === 'heading' && node.depth === 1);
    if (heading !== -1) tree.children.splice(heading, 1);

    // Resolve Markdown links from their source files, including READMEs outside docs/.
    visit(tree, ['link', 'definition'], (node) => {
      const href = node.url;
      if (/^(?:[a-z][a-z0-9+.-]*:|[/#?])/i.test(href)) return;
      const [, path, suffix] = /^([^?#]+)(.*)$/.exec(href);
      const original = relative(resolve(repo, 'docs/.content'), file.path);
      const destination = resolve(repo, dirname(original), decodeURIComponent(path));
      const source = relative(repo, destination);
      if ((source.startsWith('docs/') && /\.mdx?$/.test(source)) || externalPages[source]) {
        const id = pageId(source);
        node.url = (id === 'index' ? '/' : `/${id}/`) + suffix;
      } else {
        const kind = statSync(destination, { throwIfNoEntry: false })?.isDirectory() ? 'tree' : 'blob';
        node.url = `${repoUrl}/${kind}/main/${source}${suffix}`;
      }
    });
  };
}
