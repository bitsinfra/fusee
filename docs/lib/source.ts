import type { Root } from 'fumadocs-core/page-tree';
import { loader, type MetaData, type StaticSource } from 'fumadocs-core/source';
import { guides } from '../.source/server';
import { pageId, sidebar } from '../tooling/navigation.mjs';

const pages: StaticSource<{ pageData: (typeof guides)[number]; metaData: MetaData }> = {
  files: guides.map((data) => ({
    type: 'page',
    path: pageId(data.info.path) + '.mdx',
    data,
  })),
};

export const source = loader(pages, { baseUrl: '/' });

export const tree: Root = {
  name: 'Fusee Docs',
  children: sidebar.map((group) => ({
    type: 'folder',
    name: group.label,
    defaultOpen: !group.collapsed,
    children: group.items.map((item) => {
      const id = typeof item === 'string' ? item : item.slug;
      const page = source.getPage(id === 'index' ? [] : id.split('/'));
      if (!page) throw new Error(`Missing sidebar page: ${id}`);
      return { type: 'page', url: page.url, name: typeof item === 'string' ? page.data.title : item.label };
    }),
  })),
};
