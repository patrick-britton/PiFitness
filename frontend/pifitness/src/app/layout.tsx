import type { Metadata } from "next";
import "./globals.css";
import Layout from "./components/Layout";
import Providers from "./providers";

export const metadata: Metadata = {
  title: "PiFitness",
  description: "Personal fitness & media dashboard",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="light">
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
