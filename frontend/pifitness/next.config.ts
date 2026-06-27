import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Dev-only: proxy /api/* requests to the FastAPI backend
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: "http://localhost:8000/api/:path*",
      },
    ];
  },
  // Fix workspace root detection when multiple package-lock.json exist
  // Prevents Turbopack from inferring the wrong root and missing @/* alias
  turbopack: {
    root: process.cwd(),
  },
};

export default nextConfig;
