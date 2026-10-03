/**
 * Decoder selection (010-001 T06): native `BarcodeDetector` is used only when it
 * advertises *every* requested format — otherwise the self-hosted ponyfill wins.
 * Also pins the requested symbologies to the retail EAN/UPC family.
 */

import { afterEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  ponyfillCtor: vi.fn(),
  ensureZxingConfigured: vi.fn(),
}));

vi.mock('barcode-detector/ponyfill', () => ({
  BarcodeDetector: class {
    constructor(options?: { formats?: string[] }) {
      mocks.ponyfillCtor(options);
    }
    detect() {
      return Promise.resolve([]);
    }
  },
}));

vi.mock('../zxing', () => ({
  ensureZxingConfigured: mocks.ensureZxingConfigured,
}));

import {
  RETAIL_BARCODE_FORMATS,
  createBarcodeDecoder,
  isNativeDecoderSufficient,
} from '../decoder';

/** Install a fake native BarcodeDetector advertising `supported` formats. */
function installNative(supported: string[] | 'rejects') {
  const detect = vi.fn().mockResolvedValue([]);
  const ctorSpy = vi.fn();
  const formatsSpy = vi.fn(() =>
    supported === 'rejects' ? Promise.reject(new Error('unsupported')) : Promise.resolve(supported),
  );
  class FakeNative {
    constructor(options?: { formats?: string[] }) {
      ctorSpy(options);
    }
    static getSupportedFormats() {
      return formatsSpy();
    }
    detect(source: unknown) {
      return detect(source);
    }
  }
  (globalThis as { BarcodeDetector?: unknown }).BarcodeDetector = FakeNative;
  return { detect, ctorSpy, formatsSpy };
}

afterEach(() => {
  mocks.ponyfillCtor.mockClear();
  mocks.ensureZxingConfigured.mockClear();
  Reflect.deleteProperty(globalThis, 'BarcodeDetector');
});

describe('isNativeDecoderSufficient', () => {
  it('is false when the native API is absent', async () => {
    await expect(isNativeDecoderSufficient()).resolves.toBe(false);
  });

  it('is true only when every requested format is supported', async () => {
    installNative([...RETAIL_BARCODE_FORMATS]);
    await expect(isNativeDecoderSufficient()).resolves.toBe(true);

    installNative(['ean_13']);
    await expect(isNativeDecoderSufficient()).resolves.toBe(false);
  });

  it('is false when getSupportedFormats throws', async () => {
    installNative('rejects');
    await expect(isNativeDecoderSufficient()).resolves.toBe(false);
  });
});

describe('createBarcodeDecoder', () => {
  it('uses the native detector when it covers the retail formats', async () => {
    const { detect, ctorSpy } = installNative([...RETAIL_BARCODE_FORMATS]);
    const decoder = await createBarcodeDecoder();
    await decoder.detect({} as HTMLVideoElement);
    expect(ctorSpy).toHaveBeenCalledWith({ formats: [...RETAIL_BARCODE_FORMATS] });
    expect(detect).toHaveBeenCalledTimes(1);
    expect(mocks.ponyfillCtor).not.toHaveBeenCalled();
  });

  it('falls back to the ponyfill when the native API is absent', async () => {
    const decoder = await createBarcodeDecoder();
    expect(mocks.ensureZxingConfigured).toHaveBeenCalledTimes(1);
    expect(mocks.ponyfillCtor).toHaveBeenCalledWith({ formats: [...RETAIL_BARCODE_FORMATS] });
    await expect(decoder.detect({} as HTMLVideoElement)).resolves.toEqual([]);
  });

  it('falls back when the native API supports only some formats', async () => {
    installNative(['ean_13', 'ean_8']);
    await createBarcodeDecoder();
    expect(mocks.ponyfillCtor).toHaveBeenCalledTimes(1);
  });

  it('falls back when native format probing fails', async () => {
    installNative('rejects');
    await createBarcodeDecoder();
    expect(mocks.ponyfillCtor).toHaveBeenCalledTimes(1);
  });

  it('requests only the retail EAN/UPC family', () => {
    expect([...RETAIL_BARCODE_FORMATS]).toEqual(['ean_13', 'ean_8', 'upc_a', 'upc_e']);
  });

  it('reports which backend decodes and what the native API advertised', async () => {
    installNative([...RETAIL_BARCODE_FORMATS]);
    const native = await createBarcodeDecoder();
    expect(native.kind).toBe('native');
    expect(native.nativeFormats).toEqual([...RETAIL_BARCODE_FORMATS]);

    Reflect.deleteProperty(globalThis, 'BarcodeDetector');
    const ponyfill = await createBarcodeDecoder();
    expect(ponyfill.kind).toBe('ponyfill');
    expect(ponyfill.nativeFormats).toBeNull();
  });

  it('explains a partially-supporting native API in the fallback metadata', async () => {
    installNative(['ean_13']);
    const decoder = await createBarcodeDecoder();
    expect(decoder.kind).toBe('ponyfill');
    expect(decoder.nativeFormats).toEqual(['ean_13']);
  });

  it('probes the native format list once per browser instance', async () => {
    const { formatsSpy } = installNative([...RETAIL_BARCODE_FORMATS]);
    await createBarcodeDecoder();
    await createBarcodeDecoder();
    // BarcodeDetector's advertised formats cannot change mid-session, so repeated
    // camera starts must not re-probe (and the diagnostics reuse the same value).
    expect(formatsSpy).toHaveBeenCalledTimes(1);
  });
});