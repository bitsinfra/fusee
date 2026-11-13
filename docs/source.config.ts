import { defineCollections, defineConfig } from 'fumadocs-mdx/config';
import { pageSchema } from 'fumadocs-core/source/schema';
import { repositoryMarkdown } from './tooling/markdown.mjs';

export const guides = defineCollections({ type: 'doc', dir: '.content', schema: pageSchema });

export default defineConfig({
  mdxOptions: {
    remarkPlugins: (plugins) => [repositoryMarkdown, ...plugins],
  },
});
