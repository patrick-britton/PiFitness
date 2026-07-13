import type { Metadata, Viewport } from "next";
import "./globals.css";
import Layout from "./components/Layout";
import Providers from "./providers";

export const metadata: Metadata = {
  title: "PiFitness",
  description: "Personal fitness & media dashboard",
};

// viewport-fit=cover allows env(safe-area-inset-*) to be used so the pinned
// mobile nav can clear the OS gesture bar / browser chrome
export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  viewportFit: "cover",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <script
          dangerouslySetInnerHTML={{
            __html: `
              (function() {
                try {
                  var theme = localStorage.getItem('pifitness-theme');
                  if (!theme) {
                    var isDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
                    theme = isDark ? 'dark' : 'light';
                    localStorage.setItem('pifitness-theme', theme);
                  }
                  if (theme === 'dark') {
                    document.documentElement.classList.add('dark');
                  }
                } catch (e) {}
              })();
            `,
          }}
        />
      </head>
      <body className="min-h-screen text-gray-900 antialiased">
        <Providers>
          <Layout>
            {children}
          </Layout>
        </Providers>
      </body>
    </html>
  );
}
