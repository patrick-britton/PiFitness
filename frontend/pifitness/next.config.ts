import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "export", // static export for production
  distDir: "out", // output directory (matches deployment script)

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