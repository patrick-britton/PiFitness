/**
 * EAN-13 encoder (010-001 T09) — the decoder self-test's symbol must be mathematically
 * valid, so these pin the check digit, the module count, the guard patterns, and the
 * leading-digit L/G parity that makes the symbol decodable.
 */

import { describe, expect, it } from 'vitest';
import {
  EAN13_SYMBOL_MODULES,
  EAN13_TEST_CODE,
  ean13Canvas,
  ean13CheckDigit,
  ean13Modules,
} from '../ean13';

describe('ean13CheckDigit', () => {
  it('computes the check digit for the bundled test symbol', () => {
    // 4006381333931 — the last digit must agree with the first twelve.
    expect(ean13CheckDigit('400638133393')).toBe(1);
    expect(EAN13_TEST_CODE).toBe('4006381333931');
    expect(ean13CheckDigit('000000000000')).toBe(0);
  });

  it('rejects a non-12-digit input', () => {
    expect(() => ean13CheckDigit('123')).toThrow(/12 digits/);
    expect(() => ean13CheckDigit('40063813339X')).toThrow(/12 digits/);
  });
});

describe('ean13Modules', () => {
  it('builds a 95-module symbol with the three guard patterns', () => {
    const modules = ean13Modules(EAN13_TEST_CODE);
    expect(modules).toHaveLength(EAN13_SYMBOL_MODULES);
    expect(modules).toMatch(/^[01]+$/);
    expect(modules.slice(0, 3)).toBe('101'); // start guard
    expect(modules.slice(45, 50)).toBe('01010'); // centre guard
    expect(modules.slice(92)).toBe('101'); // end guard
  });

  it('uses the L/G parity selected by the leading digit', () => {
    // Digits: 4 0 0 6 3 8 | 1 3 3 3 9 3 | 1. Leading 4 → "LGLLGG", so the 2nd and 5th
    // left blocks are even-parity (G) and the rest are odd-parity (L).
    const modules = ean13Modules(EAN13_TEST_CODE);
    expect(modules.slice(3, 10)).toBe('0001101'); // L(0)
    expect(modules.slice(10, 17)).toBe('0100111'); // G(0)
    expect(modules.slice(17, 24)).toBe('0101111'); // L(6)
    expect(modules.slice(24, 31)).toBe('0111101'); // L(3)
    expect(modules.slice(31, 38)).toBe('0001001'); // G(8)
    expect(modules.slice(38, 45)).toBe('0110011'); // G(1)
  });

  it('encodes the right half in R (the complement of L)', () => {
    // Right digits are 3 3 3 9 3 1.
    const modules = ean13Modules(EAN13_TEST_CODE);
    expect(modules.slice(50, 57)).toBe('1000010'); // R(3)
    expect(modules.slice(57, 64)).toBe('1000010'); // R(3)
    expect(modules.slice(71, 78)).toBe('1110100'); // R(9)
    expect(modules.slice(85, 92)).toBe('1100110'); // R(1)
  });

  it('throws on a malformed or check-digit-invalid code', () => {
    expect(() => ean13Modules('400638133393')).toThrow(/13 digits/);
    expect(() => ean13Modules('4006381333930')).toThrow(/check digit/);
  });
});

describe('ean13Canvas', () => {
  it('degrades instead of throwing where the browser has no 2D context (jsdom)', () => {
    // jsdom provides no 2D context: the self-test must report "unavailable", not crash.
    expect(() => ean13Canvas(EAN13_TEST_CODE)).not.toThrow();
    expect(ean13Canvas(EAN13_TEST_CODE)).toBeFalsy();
    // A malformed symbol is still rejected before anything is drawn.
    expect(() => ean13Canvas('4006381333930')).toThrow(/check digit/);
  });
});
