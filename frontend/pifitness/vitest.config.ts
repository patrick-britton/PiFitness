/**
 * Vitest configuration — component/behaviour tests (008-005 T10).
 *
 * Dev-only: these packages are `package.json` devDependencies and are never
 * installed or run on the Pi (the deployment requirements files are untouched).
 * jsdom has no layout engine, so this suite verifies behaviour (which branch
 * renders, what it says, what a click sends) — CSS layout and light/dark
 * appearance remain a human runtime check.
 */

import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';
import path from 'node:path';

export default defineConfig({
  plugins: [react()],
  resolve: {
    // Mirrors the `@/*` -> `./src/*` path alias in tsconfig.json
    alias: { '@': path.resolve(__dirname, './src') },
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.ts'],
    include: ['src/**/*.test.{ts,tsx}'],
    css: false,
  },
});