#!/usr/bin/env node
/**
 * Copy the ZXing wasm reader into `public/wasm/` (010-001 T05).
 *
 * `barcode-detector` fetches its `.wasm` at runtime and, by default, resolves it
 * to a jsDelivr CDN URL. This project serves it from our own origin instead (see
 * `.features/descriptions/selfhost_scanner.md`). This script runs on `postinstall`
 * so the binary is regenerated on every install and never committed
 * (`frontend/pifitness/public/wasm/` is gitignored).
 *
 * Plain `node:fs` (no shell `cp`) so it behaves identically on the Windows dev
 * box and the Raspberry Pi. It exits non-zero — loudly — if the source is
 * missing, because a silent failure would ship a scanner that hits the CDN.
 *
 * The source is the `zxing-wasm` that `barcode-detector` installs as its own
 * (pinned) dependency, so the `.wasm` always matches the JS that loads it. Never
 * install `zxing-wasm` separately at a different version.
 */
import { copyFileSync, existsSync, mkdirSync, readdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const projectRoot = resolve(here, '..');
const readerDir = resolve(projectRoot, 'node_modules', 'zxing-wasm', 'dist', 'reader');
const destDir = resolve(projectRoot, 'public', 'wasm');

function fail(message) {
  console.error(`[copy-zxing-wasm] ${message}`);
  process.exit(1);
}

if (!existsSync(readerDir)) {
  fail(
    `source directory not found: ${readerDir}\n` +
      '  Is "barcode-detector" installed? Run `npm install` and retry.',
  );
}

// Don't hardcode the filename — copy whatever `.wasm` the installed version ships,
// so a zxing-wasm layout change surfaces as an explicit failure instead of a bad path.
const wasmFiles = readdirSync(readerDir).filter((name) => name.endsWith('.wasm'));
if (wasmFiles.length === 0) {
  fail(`no .wasm file found in ${readerDir} — the installed zxing-wasm layout changed.`);
}

mkdirSync(destDir, { recursive: true });
for (const name of wasmFiles) {
  copyFileSync(resolve(readerDir, name), resolve(destDir, name));
  console.log(`[copy-zxing-wasm] ${name} -> public/wasm/${name}`);
}
console.log(`[copy-zxing-wasm] copied ${wasmFiles.length} file(s) to public/wasm/`);