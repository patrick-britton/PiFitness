import type { NextConfig } from 'next';

const nextConfig: NextConfig = {
  output: 'export',  // static export
  distDir: 'out',    // output directory (matches deployment script)
};

export default nextConfig;