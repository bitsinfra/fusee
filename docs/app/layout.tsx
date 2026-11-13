import type { Metadata } from 'next';
import type { ReactNode } from 'react';
import { RootProvider } from 'fumadocs-ui/provider/next';
import './globals.css';

export const metadata: Metadata = {
  title: { default: 'Fusee Docs', template: '%s | Fusee Docs' },
  description: 'Build, test, and operate durable workflows with Fusee.',
  icons: { icon: '/fusee.svg' },
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className="flex min-h-screen flex-col">
        <RootProvider search={{ options: { type: 'static', api: '/search-index.json' } }}>
          {children}
        </RootProvider>
      </body>
    </html>
  );
}
