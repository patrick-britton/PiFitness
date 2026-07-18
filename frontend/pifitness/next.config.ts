import type { NextConfig } from "next";
import { readFileSync } from "fs";
import { resolve } from "path";

/**
 * Load the backend .env file and expose a small, safe subset of values to the
 * client bundle via NEXT_PUBLIC_* so the frontend can detect its environment
 * (local dev vs Pi 5 production) without leaking secrets.
 */
function loadPublicEnv() {
  const publicEnv: Record<string, string> = {};
  try {
    const envPath = resolve(process.cwd(), "..", "..", "backend", ".env");
    const raw = readFileSync(envPath, "utf-8");
    const boxMatch = raw.match(/^\s*BOX\s*=\s*(.+?)\s*$/m);
    if (boxMatch) {
      publicEnv.NEXT_PUBLIC_BOX = boxMatch[1];
    }
  } catch {
    // If the file can't be read, fall back to any existing process env value
    if (process.env.BOX) {
      publicEnv.NEXT_PUBLIC_BOX = process.env.BOX;
    }
  }
  return publicEnv;
}

const nextConfig: NextConfig = {
  env: loadPublicEnv(),
  // Dev-only: proxy /api/* requests to the FastAPI backend
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: "http://localhost:8000/api/:path*",
      },
    ];
  },
  // Redirect /running to /activities (module renamed)
  async redirects() {
    return [
      {
        source: "/running",
        destination: "/activities",
        permanent: true,
      },
      {
        source: "/running/:path*",
        destination: "/activities/:path*",
        permanent: true,
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