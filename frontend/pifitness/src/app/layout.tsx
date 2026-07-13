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
    <html lang="en">
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
