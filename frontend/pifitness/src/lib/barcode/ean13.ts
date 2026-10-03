/**
 * Minimal EAN-13 encoder for the scanner's decoder self-test (010-001 T09).
 *
 * The dev page has to answer one question a camera cannot: *can this decoder read a
 * barcode at all, or is the camera image the problem?* Encoding a mathematically valid
 * symbol locally (no dependency, no network, no extra wasm) gives a pixel-perfect input
 * for the very same decoder instance the camera feeds, so a miss points at the decoder
 * and a hit points at the optics.
 *
 * Pure module-string functions are kept separate from the canvas helper so the encoding
 * is unit-testable in jsdom, which has no 2D canvas context.
 */

/** Digits 0-9, left half, odd parity ("L"). '1' = bar, '0' = space. */
const L_CODE = [
  '0001101', '0011001', '0010011', '0111101', '0100011',
  '0110001', '0101111', '0111011', '0110111', '0001011',
];

/** Digits 0-9, left half, even parity ("G"). */
const G_CODE = [
  '0100111', '0110011', '0011011', '0100001', '0011101',
  '0111001', '0000101', '0010001', '0001001', '0010111',
];

/** Digits 0-9, right half ("R") — the complement of {@link L_CODE}. */
const R_CODE = [
  '1110010', '1100110', '1101100', '1000010', '1011100',
  '1001110', '1010000', '1000100', '1001000', '1110100',
];

/** Left-half L/G parity pattern, selected by the symbol's first digit. */
const LEFT_PARITY = [
  'LLLLLL', 'LLGLGG', 'LLGGLG', 'LLGGGL', 'LGLLGG',
  'LGGLLG', 'LGGGLL', 'LGLGLG', 'LGLGGL', 'LGGLGL',
];

/** A valid, widely printed test symbol (EAN-13 of a Nutella jar). */
export const EAN13_TEST_CODE = '4006381333931';

/** Modules in a 13-digit symbol: 3 guard + 42 left + 5 centre + 42 right + 3 guard. */
export const EAN13_SYMBOL_MODULES = 95;

/** EAN-13 check digit for the first 12 digits; weights alternate 3,1 from the left. */
export function ean13CheckDigit(first12: string): number {
  if (!/^\d{12}$/.test(first12)) {
    throw new Error(`An EAN-13 check digit needs 12 digits, got "${first12}".`);
  }
  let sum = 0;
  for (let i = 0; i < 12; i += 1) sum += Number(first12[i]) * (i % 2 === 0 ? 1 : 3);
  return (10 - (sum % 10)) % 10;
}

/**
 * The 95-module bar pattern for a full 13-digit code ('1' = bar, '0' = space).
 * Throws when the code is malformed or its check digit is wrong, so a "known-good"
 * symbol can never quietly be an invalid one.
 */
export function ean13Modules(code: string): string {
  if (!/^\d{13}$/.test(code)) {
    throw new Error(`An EAN-13 needs 13 digits, got "${code}".`);
  }
  if (ean13CheckDigit(code.slice(0, 12)) !== Number(code[12])) {
    throw new Error(`Invalid EAN-13 check digit in "${code}".`);
  }
  const parity = LEFT_PARITY[Number(code[0])];
  let left = '';
  for (let i = 1; i <= 6; i += 1) {
    const digit = Number(code[i]);
    left += parity[i - 1] === 'L' ? L_CODE[digit] : G_CODE[digit];
  }
  let right = '';
  for (let i = 7; i <= 12; i += 1) right += R_CODE[Number(code[i])];
  return `101${left}01010${right}101`;
}

export interface Ean13CanvasOptions {
  /** Pixels per module (4 keeps the symbol sharp for ZXing). */
  moduleWidth?: number;
  height?: number;
  /** White space either side of the symbol; EAN requires a quiet zone. */
  quietModules?: number;
}

/**
 * Draw the symbol onto a canvas ready for `BarcodeDetector.detect`. Returns `null` when
 * there is no DOM or no 2D context (jsdom in unit tests, exotic browsers) so callers can
 * report "unavailable" instead of throwing.
 */
export function ean13Canvas(
  code: string,
  options: Ean13CanvasOptions = {},
): HTMLCanvasElement | null {
  const modules = ean13Modules(code);
  const { moduleWidth = 4, height = 200, quietModules = 12 } = options;
  if (typeof document === 'undefined') return null;
  const canvas = document.createElement('canvas');
  const width = (modules.length + quietModules * 2) * moduleWidth;
  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext('2d');
  if (!ctx) return null;
  ctx.fillStyle = '#ffffff';
  ctx.fillRect(0, 0, width, height);
  ctx.fillStyle = '#000000';
  for (let i = 0; i < modules.length; i += 1) {
    if (modules[i] === '1') ctx.fillRect((quietModules + i) * moduleWidth, 0, moduleWidth, height);
  }
  return canvas;
}
