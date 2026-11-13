import type { ReactNode } from 'react';
import { DocsLayout } from 'fumadocs-ui/layouts/docs';
import { tree } from '../../lib/source';

export default function GuideLayout({ children }: { children: ReactNode }) {
  return (
    <DocsLayout
      tree={tree}
      nav={{ url: '/', title: <><img src="/fusee.svg" width="26" height="21" alt="" /> Fusee Docs</> }}
      githubUrl="https://github.com/bitsinfra/fusee"
    >
      {children}
    </DocsLayout>
  );
}
