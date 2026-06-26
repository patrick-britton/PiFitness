import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Server-side rendering for full functionality
  // Remove output: "export" to enable proper Next.js server

  // Dev-only: proxy /api/* requests to the FastAPI backend
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: "http://localhost:8000/api/:path*",
      },
    ];
  },
};

export default nextConfig;
