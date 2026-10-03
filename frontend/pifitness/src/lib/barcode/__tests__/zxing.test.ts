/**
 * ZXing wasm loader (010-001 T05/T06): the `.wasm` must resolve to our own origin
 * (`/wasm/…`) and the loader must be configured exactly once. This is the guard
 * against the default jsDelivr CDN path ever coming back.
 */

import { describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({ prepareZXingModule: vi.fn() }));

vi.mock('barcode-detector/ponyfill', () => ({
  prepareZXingModule: mocks.prepareZXingModule,
}));

import { ZXING_WASM_BASE_PATH, ensureZxingConfigured } from '../zxing';

describe('ensureZxingConfigured', () => {
  it('resolves the wasm to our own origin and configures only once', () => {
    ensureZxingConfigured();
    ensureZxingConfigured();

    expect(mocks.prepareZXingModule).toHaveBeenCalledTimes(1);

    const options = mocks.prepareZXingModule.mock.calls[0][0] as {
      overrides: { locateFile: (path: string, prefix: string) => string };
    };
    expect(ZXING_WASM_BASE_PATH).toBe('/wasm');
    // Would otherwise have been https://cdn.jsdelivr.net/.../zxing_reader.wasm
    expect(
      options.overrides.locateFile('zxing_reader.wasm', 'https://cdn.jsdelivr.net/npm/zxing-wasm@3/'),
    ).toBe('/wasm/zxing_reader.wasm');
    // Non-wasm assets keep their original prefix.
    expect(options.overrides.locateFile('other.js', 'https://example.test/')).toBe(
      'https://example.test/other.js',
    );
  });
});