import type { Metadata, Viewport } from "next";
import Script from "next/script";
import "./globals.css";
import Layout from "./components/Layout";
import Providers from "./providers";
import ServiceWorkerRegister from "./components/ServiceWorkerRegister";

export const metadata: Metadata = {
  title: "PiFitness",
  description: "Personal fitness & media dashboard",
  manifest: "/manifest.json",
  appleWebApp: {
    capable: true,
    statusBarStyle: "default",
    title: "PiFitness",
  },
  icons: {
    icon: "/icon.svg",
    apple: "/icon-192.png",
  },
};

// viewport-fit=cover allows env(safe-area-inset-*) to be used so the pinned
// mobile nav can clear the OS gesture bar / browser chrome
export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  viewportFit: "cover",
  themeColor: [
    { media: "(prefers-color-scheme: light)", color: "#ffffff" },
    { media: "(prefers-color-scheme: dark)", color: "#1f2937" },
  ],
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <Script strategy="afterInteractive">
          {`
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
          `}
        </Script>
      </head>
      <body className="min-h-screen text-gray-900 antialiased">
        <ServiceWorkerRegister />
        <Providers>
          <Layout>
            {children}
          </Layout>
        </Providers>
      </body>
    </html>
  );
}
