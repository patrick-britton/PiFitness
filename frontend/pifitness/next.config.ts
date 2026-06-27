import type { NextConfig } from "next";
import path from "path";

const nextConfig: NextConfig = {
  // Server-side rendering for full functionality
  // Remove output: "export" to enable proper Next.js server

  // Fix workspace root detection
  outputFileTracingRoot: path.join(__dirname, '../../'),

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
