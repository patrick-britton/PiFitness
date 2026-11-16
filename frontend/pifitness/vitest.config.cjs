/**
 * Vitest configuration — component/behaviour tests (008-005 T10).
 *
 * This version is written in plain CommonJS so it works with Vite’s
 * future native config loader (which expects CommonJS).  No ESM syntax
 * (`import` / `export`) is used, therefore the `configLoader: 'native'`
 * warning disappears.
 */

const path = require('node:path');
const react = require('@vitejs/plugin-react');

module.exports = {
  plugins: [react()],
  resolve: {
    // Mirrors the `@/*` → `./src/*` alias from tsconfig.json
    alias: { '@': path.resolve(__dirname, './src') },
  },
  test: {
    environment: 'jsdom',          // keep the same jsdom environment
    setupFiles: ['./src/test/setup.ts'],
    include: ['src/**/*.test.{ts,tsx}'],
    css: false,
  },
};
