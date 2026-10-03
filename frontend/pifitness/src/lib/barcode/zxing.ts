/**
 * ZXing wasm loader configuration for `barcode-detector` (010-001 T05).
 *
 * `barcode-detector` fetches its `.wasm` at runtime and, by default, resolves it
 * to a jsDelivr CDN URL. This project serves it from its own origin instead: the
 * binary is copied into `public/wasm/` by `scripts/copy-zxing-wasm.mjs` on
 * `postinstall` (see `.features/descriptions/selfhost_scanner.md`). Here we point
 * the loader at that same-origin static path so no scanner code path depends on a
 * third-party CDN.
 *
 * Browser-only. Import this module solely from client-only code — a `"use client"`
 * scanner component loaded via `next/dynamic` with `{ ssr: false }` (or a dynamic
 * import inside a `useEffect`) — and call `ensureZxingConfigured()` once before the
 * first `new BarcodeDetector(...)`.
 */
import { prepareZXingModule } from 'barcode-detector/ponyfill';

/**
 * Same-origin path prefix the wasm files are served from. `next.config.ts` sets no
 * `basePath`, so a literal `/wasm/` is correct; if a basePath is ever introduced,
 * it must be prepended here.
 */
export const ZXING_WASM_BASE_PATH = '/wasm';

let configured = false;

/**
 * Point the ZXing wasm loader at our own origin. Idempotent — safe to call on every
 * scanner mount.
 */
export function ensureZxingConfigured(): void {
  if (configured) return;
  // No DOM ⇒ no wasm fetch and nothing to configure (e.g. a stray SSR pass). Leave
  // `configured` false so the browser pass still applies the override.
  if (typeof window === 'undefined') return;

  prepareZXingModule({
    overrides: {
      // Resolve the `.wasm` to our own origin; leave anything else alone.
      locateFile: (path: string, prefix: string) =>
        path.endsWith('.wasm') ? `${ZXING_WASM_BASE_PATH}/${path}` : `${prefix}${path}`,
    },
  });
  configured = true;
}