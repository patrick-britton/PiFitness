/**
 * Barcode decoder selection (010-001 T06).
 *
 * Decoding is deliberately isolated from the UI so it can be mocked in tests and
 * swapped per browser capability:
 *   1. the native `BarcodeDetector`, but only when it reports support for *every*
 *      format we need, otherwise partially-supported browsers mis-decode; else
 *   2. the `barcode-detector` v3 ponyfill (ZXing-C++ WASM), whose `.wasm` is served
 *      from our own origin via the T05 loader (`src/lib/barcode/zxing.ts`).
 *
 * Both branches are loaded with a *dynamic* import so nothing is evaluated during
 * SSR — the scanner stays client-only.
 *
 * Each decoder also reports `kind`/`nativeFormats` so the scanner can tell the user
 * which backend is decoding, and why (010-001 T08 diagnostics).
 */

/** Retail EAN/UPC family — the only symbologies this dev tool scans. */
export const RETAIL_BARCODE_FORMATS = ['ean_13', 'ean_8', 'upc_a', 'upc_e'] as const;

export type RetailBarcodeFormat = (typeof RETAIL_BARCODE_FORMATS)[number];

export interface DetectedBarcode {
  rawValue: string;
  format: string;
}

/** Which backend actually performs decoding — shown by the dev diagnostics readout. */
export type DecoderKind = 'native' | 'ponyfill';

/**
 * What a decoder can be handed. Video for the live path; a canvas for the dev-only
 * self-test, which drives the same decoder with a locally drawn, known-good symbol.
 */
export type DetectSource = HTMLVideoElement | HTMLCanvasElement;

export interface BarcodeDecoder {
  detect(source: DetectSource): Promise<DetectedBarcode[]>;
  /**
   * Backend in use. Optional so injected test fakes stay valid; both real factories
   * set it, and the scanner hook reports it while it is seeking a barcode.
   */
  readonly kind?: DecoderKind;
  /**
   * Formats the browser's native `BarcodeDetector` advertised, or `null` when the API
   * is absent. Explains *why* the ponyfill fallback was chosen.
   */
  readonly nativeFormats?: readonly string[] | null;
}

/** Injectable decoder factory (tests pass a fake; the hook defaults to the real one). */
export type DecoderFactory = (formats: string[]) => Promise<BarcodeDecoder>;

interface NativeDetector {
  detect(source: DetectSource): Promise<DetectedBarcode[]>;
}

interface BarcodeDetectorCtor {
  new (options?: { formats?: string[] }): NativeDetector;
  getSupportedFormats?(): Promise<string[]>;
}

function nativeBarcodeDetector(): BarcodeDetectorCtor | null {
  const ctor = (globalThis as { BarcodeDetector?: unknown }).BarcodeDetector;
  return typeof ctor === 'function' ? (ctor as BarcodeDetectorCtor) : null;
}

/** Memoized native probe, keyed by the constructor instance the browser exposes. */
let probedCtor: BarcodeDetectorCtor | null = null;
let probedFormats: string[] = [];

/**
 * Formats the native `BarcodeDetector` advertises, or `null` when the API is absent.
 *
 * Memoized per constructor instance: the advertised set cannot change while the page
 * lives, so the decoder factories and the diagnostics readout share one probe instead
 * of re-running `getSupportedFormats()` on every camera start.
 */
export async function probeNativeFormats(): Promise<string[] | null> {
  const ctor = nativeBarcodeDetector();
  if (!ctor) return null;
  if (ctor === probedCtor) return probedFormats;
  try {
    probedFormats = (await ctor.getSupportedFormats?.()) ?? [];
  } catch {
    // A throwing probe means "advertises nothing usable".
    probedFormats = [];
  }
  probedCtor = ctor;
  return probedFormats;
}

/**
 * True only when the native API exists *and* advertises every requested format.
 * A browser that supports some-but-not-all would silently fail on the rest.
 */
export async function isNativeDecoderSufficient(
  formats: readonly string[] = RETAIL_BARCODE_FORMATS,
): Promise<boolean> {
  const supported = await probeNativeFormats();
  return supported !== null && formats.every((format) => supported.includes(format));
}

export const createNativeDecoder: DecoderFactory = async (formats) => {
  const ctor = nativeBarcodeDetector();
  if (!ctor) throw new Error('Native BarcodeDetector is unavailable.');
  const detector = new ctor({ formats });
  return {
    detect: (source) => detector.detect(source),
    kind: 'native',
    nativeFormats: await probeNativeFormats(),
  };
};

export const createPonyfillDecoder: DecoderFactory = async (formats) => {
  const [{ ensureZxingConfigured }, ponyfill] = await Promise.all([
    import('./zxing'),
    import('barcode-detector/ponyfill'),
  ]);
  // Points the wasm loader at our own origin before the first detection.
  ensureZxingConfigured();
  const ctor = ponyfill.BarcodeDetector as unknown as BarcodeDetectorCtor;
  const detector = new ctor({ formats });
  return {
    detect: (source) => detector.detect(source),
    kind: 'ponyfill',
    // Kept for the diagnostics readout: it shows *why* the fallback was chosen.
    nativeFormats: await probeNativeFormats(),
  };
};

/** Native when sufficient, otherwise the self-hosted ponyfill. */
export async function createBarcodeDecoder(
  formats: readonly string[] = RETAIL_BARCODE_FORMATS,
): Promise<BarcodeDecoder> {
  const requested = [...formats];
  if (await isNativeDecoderSufficient(requested)) return createNativeDecoder(requested);
  return createPonyfillDecoder(requested);
}