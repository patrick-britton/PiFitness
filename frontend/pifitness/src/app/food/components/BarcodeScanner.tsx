'use client';

/**
 * BarcodeScanner — live camera barcode scanner (010-001 T06, diagnostics T08).
 *
 * A thin, presentational shell over {@link useBarcodeScanner}: rear-camera preview,
 * scan-area reticle, success flash, and the user-visible camera error states with a
 * retry. It owns *scanning only* — looking the code up is the caller's job, so this
 * component stays reusable and easy to mock.
 *
 * While scanning it also shows what the scanner is doing (decoder backend, native
 * format support, frames submitted, decode errors) so a camera that is seeking but
 * never matching a barcode is diagnosable on the device instead of silent.
 *
 * Client-only by construction: the decoder module is imported dynamically inside the
 * hook, and callers should also load this component with `next/dynamic` `{ ssr: false }`.
 */

import { useState } from 'react';
import { useBarcodeScanner, type ScannerDiagnostics } from '@/hooks/useBarcodeScanner';
import { EAN13_TEST_CODE, ean13Canvas } from '@/lib/barcode/ean13';
import type { ScannerErrorCode } from '@/lib/barcode/camera';
import type { DecoderFactory } from '@/lib/barcode/decoder';

export interface BarcodeScannerProps {
  /** Start/stop the camera. False releases the camera. */
  active: boolean;
  /** Symbologies to request (defaults to the retail EAN/UPC family). */
  formats?: readonly string[];
  onScan: (value: string, format: string) => void;
  onError?: (code: ScannerErrorCode) => void;
  /** Dev-only: when set, renders a "simulate scan" control. Never replaces the camera. */
  devSimulateValue?: string;
  /** Test seams — omitted in production. */
  requestStream?: () => Promise<MediaStream>;
  createDecoder?: DecoderFactory;
  debounceMs?: number;
  scanIntervalMs?: number;
}

const STATUS_TEXT: Record<string, string> = {
  idle: 'Camera off.',
  starting: 'Starting camera… grant permission if your browser asks.',
  scanning: 'Point the camera at a barcode — it scans automatically.',
  error: 'Camera unavailable. Use manual entry below.',
};

/** Plain-English decoder names for the diagnostics readout (dev-facing). */
const DECODER_LABELS: Record<string, string> = {
  native: 'browser-native BarcodeDetector',
  ponyfill: 'ZXing ponyfill (self-hosted wasm)',
};

/** What the browser's own BarcodeDetector advertised, and what that means here. */
function describeNativeSupport(nativeFormats: string[] | null): string {
  if (nativeFormats === null) return 'not available — using the wasm fallback';
  if (nativeFormats.length === 0) return 'present but reported no formats — using the wasm fallback';
  return nativeFormats.join(', ');
}

/**
 * One line telling the user what the numbers mean, in order of severity: a decoder
 * that is erroring outranks "nothing arrived", which outranks "arrived but empty".
 */
function describeHint(diagnostics: ScannerDiagnostics): string {
  if (diagnostics.decodeErrors > 0) {
    return 'The decoder is failing on this image — see the error below.';
  }
  if (diagnostics.framesScanned === 0) {
    return 'No frames have reached the decoder yet — the camera may still be warming up.';
  }
  if (diagnostics.frameWidth === 0) {
    return 'The camera image has no size yet, so there is nothing to decode — if this does not change, retry the camera.';
  }
  return 'Decoding is running but has not matched a barcode yet: fill the reticle with the barcode, hold steady, and avoid glare.';
}

/** Compact 2-column `text-xs` grid: readable in portrait without eating landscape height. */
function DiagnosticsReadout({ diagnostics }: { diagnostics: ScannerDiagnostics }) {
  const term = 'text-gray-500 dark:text-gray-400';
  const value = 'text-gray-900 dark:text-gray-100';
  return (
    <dl
      data-testid="scanner-diagnostics"
      className="grid grid-cols-2 gap-x-3 gap-y-1 rounded-md border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900 p-3 text-xs"
    >
      <dt className={term}>Decoder</dt>
      <dd data-testid="diag-decoder" className={value}>
        {diagnostics.decoderKind ? DECODER_LABELS[diagnostics.decoderKind] : 'not selected yet'}
      </dd>
      <dt className={term}>Native BarcodeDetector</dt>
      <dd data-testid="diag-native" className={value}>
        {describeNativeSupport(diagnostics.nativeFormats)}
      </dd>
      <dt className={term}>Requested formats</dt>
      <dd data-testid="diag-formats" className={value}>
        {diagnostics.requestedFormats.join(', ')}
      </dd>
      <dt className={term}>Frames decoded</dt>
      <dd data-testid="diag-frames" className={value}>
        {diagnostics.framesScanned.toLocaleString()} · {diagnostics.frameWidth}×
        {diagnostics.frameHeight} · {(diagnostics.elapsedMs / 1000).toFixed(1)}s
      </dd>
      <dt className={term}>Video</dt>
      <dd data-testid="diag-video" className={value}>
        {diagnostics.videoTime.toFixed(1)}s · {diagnostics.videoPaused ? 'PAUSED' : 'live'}
      </dd>
      <dt className={term}>Decode errors</dt>
      <dd data-testid="diag-errors" className={value}>
        {diagnostics.decodeErrors}
        {diagnostics.lastDecodeError ? ` — ${diagnostics.lastDecodeError}` : ''}
      </dd>
    </dl>
  );
}

export default function BarcodeScanner({
  active,
  formats,
  onScan,
  onError,
  devSimulateValue,
  requestStream,
  createDecoder,
  debounceMs,
  scanIntervalMs,
}: BarcodeScannerProps) {
  const { videoRef, status, errorMessage, errorDetail, lastScan, flash, diagnostics, probeDecode, restart } =
    useBarcodeScanner({
      active,
      formats,
      onScan,
      onError,
      requestStream,
      createDecoder,
      debounceMs,
      scanIntervalMs,
    });
  const [selfTest, setSelfTest] = useState<string | null>(null);

  /**
   * Dev-only: decode a locally drawn, check-digit-valid EAN-13 with the *live* decoder.
   * A hit means any code the camera misses is an image problem (focus, size, glare,
   * symbology); a miss means the decoder itself is the problem.
   */
  const runSelfTest = async () => {
    const canvas = ean13Canvas(EAN13_TEST_CODE);
    if (!canvas) {
      setSelfTest('Self-test unavailable: this browser gave no 2D canvas context.');
      return;
    }
    const results = await probeDecode(canvas);
    if (results === null) {
      setSelfTest('No decoder is running — start the camera, then try again.');
      return;
    }
    const hit = results.find((result) => result.rawValue);
    setSelfTest(
      hit
        ? `Decoder OK: read the known-good EAN-13 ${hit.rawValue} (${hit.format}). Whatever the camera misses is an image problem, not a decoder problem.`
        : 'Decoder could NOT read a perfect EAN-13 — the decoder/wasm path is the problem, not the camera image.',
    );
  };

  return (
    <div className="flex flex-col gap-2">
      <div className="relative overflow-hidden rounded-lg bg-black">
        <video
          ref={videoRef}
          className="aspect-video w-full object-cover"
          autoPlay
          muted
          playsInline
          aria-label="Camera preview for barcode scanning"
        />
        {/* Scan-area guide (reticle). No animation — honours prefers-reduced-motion. */}
        <div aria-hidden data-testid="scan-reticle" className="pointer-events-none absolute inset-0 flex items-center justify-center">
          <div
            className={`h-1/3 w-4/5 rounded-md border-2 ${
              flash ? 'border-emerald-400' : 'border-white/70'
            }`}
          >
            <div className="h-full w-full border-y-2 border-red-500/80" />
          </div>
        </div>
        {status === 'starting' && (
          <div className="absolute inset-0 flex items-center justify-center bg-black/50 text-xs text-white">
            Starting camera…
          </div>
        )}
      </div>

      <p role="status" aria-live="polite" className="text-xs text-gray-500 dark:text-gray-400">
        {STATUS_TEXT[status] ?? 'Camera off.'}
        {lastScan ? ` Last scan: ${lastScan.value} (${lastScan.format}).` : ''}
      </p>

      {status === 'scanning' && (
        <>
          <DiagnosticsReadout diagnostics={diagnostics} />
          <p className="text-xs text-gray-500 dark:text-gray-400" role="note">
            {describeHint(diagnostics)}
          </p>
        </>
      )}

      {status === 'scanning' && Boolean(devSimulateValue) && (
        <div className="flex flex-col gap-1">
          <button
            type="button"
            onClick={() => {
              void runSelfTest();
            }}
            className="self-start rounded-md border border-dashed border-amber-400 px-3 py-2 min-h-[44px] text-xs text-amber-700 dark:text-amber-300"
          >
            DEV: decode a known-good EAN-13
          </button>
          {selfTest && (
            <p
              data-testid="self-test-result"
              role="note"
              className="text-xs text-gray-500 dark:text-gray-400"
            >
              {selfTest}
            </p>
          )}
        </div>
      )}

      {errorMessage && (
        <div
          role="alert"
          className="rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-3 text-sm text-red-700 dark:text-red-300"
        >
          <p>{errorMessage}</p>
          {errorDetail && (
            <pre className="mt-1 whitespace-pre-wrap break-words font-mono text-xs">{errorDetail}</pre>
          )}
          <button
            type="button"
            onClick={restart}
            className="mt-2 rounded-md bg-blue-600 px-3 py-2 min-h-[44px] text-sm font-medium text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            Retry camera
          </button>
        </div>
      )}

      {devSimulateValue && (
        <button
          type="button"
          onClick={() => onScan(devSimulateValue, 'ean_13')}
          className="self-start rounded-md border border-dashed border-amber-400 px-3 py-2 min-h-[44px] text-xs text-amber-700 dark:text-amber-300"
        >
          DEV: simulate scan ({devSimulateValue})
        </button>
      )}
    </div>
  );
}