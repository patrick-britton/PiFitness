'use client';

/**
 * Camera barcode scanner hook (010-001 T06).
 *
 * Owns the browser-facing side of scanning and nothing else: requesting the rear
 * camera, driving a throttled `requestAnimationFrame` decode loop, suppressing
 * duplicate scans, giving success feedback, and — critically — releasing every
 * camera track on deactivate/close/unmount so the indicator light turns off.
 *
 * Decoding itself lives in `@/lib/barcode/decoder` and stream/error handling in
 * `@/lib/barcode/camera`, both of which can be injected here so the whole hook is
 * unit-testable without a real camera or wasm decoder.
 */

import { useCallback, useEffect, useRef, useState, type RefObject } from 'react';
import {
  SCANNER_ERROR_MESSAGES,
  classifyCameraError,
  describeError,
  requestRearCamera,
  stopMediaStream,
  type ScannerErrorCode,
} from '@/lib/barcode/camera';
import {
  RETAIL_BARCODE_FORMATS,
  createBarcodeDecoder,
  type BarcodeDecoder,
  type DecoderFactory,
  type DecoderKind,
  type DetectSource,
  type DetectedBarcode,
} from '@/lib/barcode/decoder';

/** ~15 decodes/second (the spec's 10–15/s band), not one decode per frame. */
export const DEFAULT_SCAN_INTERVAL_MS = 66;

/** Ignore the same code for this long so one physical barcode emits once. */
export const DEFAULT_SCAN_DEBOUNCE_MS = 1500;

/**
 * How often the diagnostics snapshot is published. The scanner's counters are *not*
 * React state — publishing per frame (~15/s) would re-render the readout constantly and
 * jank a Raspberry Pi 5 — so plain locals are sampled onto this interval instead.
 */
export const DEFAULT_DIAGNOSTICS_INTERVAL_MS = 500;

export type ScannerStatus = 'idle' | 'starting' | 'scanning' | 'error';

/**
 * What the scanner is actually doing while it seeks a barcode. A user watching a
 * scanner that never decodes can see whether frames reach the decoder, which backend is
 * decoding, and what is failing — instead of an indefinite "point at a barcode".
 */
export interface ScannerDiagnostics {
  decoderKind: DecoderKind | null;
  requestedFormats: string[];
  /** Formats the native API advertised, or `null` when the API is absent. */
  nativeFormats: string[] | null;
  /** Frames handed to the decoder (a frame of size 0 has nothing to decode). */
  framesScanned: number;
  decodeErrors: number;
  lastDecodeError: string | null;
  frameWidth: number;
  frameHeight: number;
  /**
   * The video element's own clock and play state. A frame can have a valid resolution
   * while the element is paused (or never started), so this is what actually proves
   * frames are advancing.
   */
  videoTime: number;
  videoPaused: boolean;
  elapsedMs: number;
}

function emptyDiagnostics(requestedFormats: string[]): ScannerDiagnostics {
  return {
    decoderKind: null,
    requestedFormats,
    nativeFormats: null,
    framesScanned: 0,
    decodeErrors: 0,
    lastDecodeError: null,
    frameWidth: 0,
    frameHeight: 0,
    videoTime: 0,
    videoPaused: false,
    elapsedMs: 0,
  };
}

export interface UseBarcodeScannerOptions {
  /** Start/stop the camera. False releases the camera. */
  active: boolean;
  /** Symbologies to request (defaults to the retail EAN/UPC family). */
  formats?: readonly string[];
  onScan: (value: string, format: string) => void;
  onError?: (code: ScannerErrorCode) => void;
  debounceMs?: number;
  scanIntervalMs?: number;
  /** How often the diagnostics snapshot is published. */
  diagnosticsIntervalMs?: number;
  /** Test seam — defaults to the real rear-camera request. */
  requestStream?: () => Promise<MediaStream>;
  /** Test seam — defaults to native-or-ponyfill selection. */
  createDecoder?: DecoderFactory;
}

export interface UseBarcodeScannerResult {
  /** Attach to the `<video autoplay muted playsinline>` element. */
  videoRef: RefObject<HTMLVideoElement | null>;
  status: ScannerStatus;
  errorCode: ScannerErrorCode | null;
  errorMessage: string | null;
  lastScan: { value: string; format: string } | null;
  /** True briefly after a successful decode (visual success feedback). */
  flash: boolean;
  /**
   * Raw text of the last failure (camera *or* decoder) so the UI can show the real
   * cause, not only the generic per-code message.
   */
  errorDetail: string | null;
  /** Live "what is the scanner doing" snapshot — see {@link ScannerDiagnostics}. */
  diagnostics: ScannerDiagnostics;
  /**
   * Dev-only self-test seam: run the *live* decoder against another source (e.g. a
   * locally drawn barcode canvas). Resolves `null` when no decoder is ready.
   */
  probeDecode: (source: DetectSource) => Promise<DetectedBarcode[] | null>;
  restart: () => void;
  stop: () => void;
}

export function useBarcodeScanner({
  active,
  formats = RETAIL_BARCODE_FORMATS,
  onScan,
  onError,
  debounceMs = DEFAULT_SCAN_DEBOUNCE_MS,
  scanIntervalMs = DEFAULT_SCAN_INTERVAL_MS,
  diagnosticsIntervalMs = DEFAULT_DIAGNOSTICS_INTERVAL_MS,
  requestStream = requestRearCamera,
  createDecoder = createBarcodeDecoder,
}: UseBarcodeScannerOptions): UseBarcodeScannerResult {
  const videoRef = useRef<HTMLVideoElement | null>(null);
  const [status, setStatus] = useState<ScannerStatus>('idle');
  const [errorCode, setErrorCode] = useState<ScannerErrorCode | null>(null);
  const [lastScan, setLastScan] = useState<{ value: string; format: string } | null>(null);
  const [flash, setFlash] = useState(false);
  const [errorDetail, setErrorDetail] = useState<string | null>(null);
  const [diagnostics, setDiagnostics] = useState<ScannerDiagnostics>(() =>
    emptyDiagnostics([...formats]),
  );
  const [retryNonce, setRetryNonce] = useState(0);

  // Keep callbacks in refs so changing them never restarts the camera.
  const onScanRef = useRef(onScan);
  const onErrorRef = useRef(onError);
  useEffect(() => {
    onScanRef.current = onScan;
    onErrorRef.current = onError;
  }, [onScan, onError]);

  const teardownRef = useRef<(() => void) | null>(null);
  // The live decoder, reachable from outside the start/stop effect so the dev self-test
  // can drive the very same instance (and wasm state) the camera feeds.
  const decoderRef = useRef<BarcodeDecoder | null>(null);

  // A joined key (not the array) keeps the effect from re-running on new array identity.
  const formatsKey = formats.join(',');

useEffect(() => {
    if (!active) {
      setStatus('idle');
      setErrorCode(null);
      setErrorDetail(null);
      return;
    }

    let cancelled = false;
    let stream: MediaStream | null = null;
    let decoder: BarcodeDecoder | null = null;
    let rafId = 0;
    let flashTimer: ReturnType<typeof setTimeout> | null = null;
    let diagnosticsTimer: ReturnType<typeof setInterval> | null = null;
    let decoding = false;
    let lastTick = 0;
    let lastValue = '';
    let lastValueAt = 0;

    // Diagnostics counters live in plain locals, never React state: a decode tick must
    // not re-render anything. `publish` samples them into state on a timer + key events.
    const requestedFormats = [...formats];
    let decoderKind: DecoderKind | null = null;
    let nativeFormats: string[] | null = null;
    let framesScanned = 0;
    let decodeErrors = 0;
    let lastDecodeError: string | null = null;
    let frameWidth = 0;
    let frameHeight = 0;
    let videoTime = 0;
    let videoPaused = false;
    let startedAt = 0;

    const publish = () => {
      setDiagnostics({
        decoderKind,
        requestedFormats,
        nativeFormats,
        framesScanned,
        decodeErrors,
        lastDecodeError,
        frameWidth,
        frameHeight,
        videoTime,
        videoPaused,
        elapsedMs: startedAt ? Date.now() - startedAt : 0,
      });
    };

    // A fresh run starts from zero so a retry never shows stale counters.
    setDiagnostics(emptyDiagnostics(requestedFormats));

    // Releases the camera and stops every timer/loop. Called on deactivate, on
    // `stop()`, and on unmount — the indicator light must go off in all three.
    const teardown = () => {
      cancelled = true;
      if (rafId) cancelAnimationFrame(rafId);
      rafId = 0;
      if (flashTimer) clearTimeout(flashTimer);
      flashTimer = null;
      if (diagnosticsTimer) clearInterval(diagnosticsTimer);
      diagnosticsTimer = null;
      decoding = false;
      decoderRef.current = null;
      stopMediaStream(stream);
      stream = null;
      const video = videoRef.current;
      if (video) {
        try {
          video.srcObject = null;
        } catch {
          // Detaching a stream some environments dislike must not block teardown.
        }
      }
    };
    teardownRef.current = teardown;

    const giveFeedback = () => {
      if (typeof navigator !== 'undefined' && typeof navigator.vibrate === 'function') {
        try {
          navigator.vibrate(80);
        } catch {
          // Vibration is best-effort.
        }
      }
      setFlash(true);
      if (flashTimer) clearTimeout(flashTimer);
      flashTimer = setTimeout(() => setFlash(false), 300);
    };

    const handleResults = (results: DetectedBarcode[]) => {
      const now = Date.now();
      for (const result of results) {
        const value = result.rawValue;
        if (!value) continue;
        // Duplicate suppression: exactly one scan event per distinct code.
        if (value === lastValue && now - lastValueAt < debounceMs) continue;
        lastValue = value;
        lastValueAt = now;
        setLastScan({ value, format: result.format });
        giveFeedback();
        publish();
        onScanRef.current(value, result.format);
        return;
      }
    };

    const tick = (timestamp: number) => {
      if (cancelled) return;
      rafId = requestAnimationFrame(tick);
      if (decoding || !decoder) return;
      // Throttle to ~10-15 checks/second rather than decoding every frame.
      if (timestamp - lastTick < scanIntervalMs) return;
      lastTick = timestamp;
      const video = videoRef.current;
      if (!video) return;
      // Counted (with its resolution) so the diagnostics can distinguish "the camera
      // image never reached the decoder" from "decoding runs but matches nothing". The
      // video clock/play state is sampled too: a paused element still reports a size.
      frameWidth = video.videoWidth;
      frameHeight = video.videoHeight;
      videoTime = video.currentTime;
      videoPaused = video.paused;
      framesScanned += 1;
      decoding = true;
      decoder
        .detect(video)
        .then((results) => {
          if (!cancelled) handleResults(results);
        })
        .catch((error: unknown) => {
          // A single failed frame is expected, but a decoder failing on *every* frame
          // must not look like a scanner that is merely still searching.
          decodeErrors += 1;
          lastDecodeError = describeError(error) ?? String(error);
          if (decodeErrors === 1) publish();
        })
        .finally(() => {
          decoding = false;
        });
    };

    const start = async () => {
      setErrorCode(null);
      setErrorDetail(null);
      setStatus('starting');
      try {
        stream = await requestStream();
        if (cancelled) {
          stopMediaStream(stream);
          return;
        }
        const video = videoRef.current;
        if (video) {
          video.srcObject = stream;
          try {
            await video.play();
          } catch {
            // `muted` + `playsinline` normally autoplay; ignore the rejection.
          }
        }
        try {
          decoder = await createDecoder(requestedFormats);
        } catch (error) {
          // The camera works — the *decoder* could not be created (missing dependency,
          // wasm fetch/instantiate failure, blocked dynamic import). Surface it as its
          // own state and release the camera: nothing can be scanned without it.
          if (cancelled) return;
          stopMediaStream(stream);
          stream = null;
          const video = videoRef.current;
          if (video) {
            try {
              video.srcObject = null;
            } catch {
              // Detaching is best-effort.
            }
          }
          setErrorDetail(describeError(error) ?? SCANNER_ERROR_MESSAGES.decoder);
          setErrorCode('decoder');
          setStatus('error');
          onErrorRef.current?.('decoder');
          return;
        }
        if (cancelled) return;
        decoderKind = decoder.kind ?? null;
        nativeFormats = decoder.nativeFormats ? [...decoder.nativeFormats] : null;
        decoderRef.current = decoder;
        startedAt = Date.now();
        publish();
        diagnosticsTimer = setInterval(publish, diagnosticsIntervalMs);
        setStatus('scanning');
        rafId = requestAnimationFrame(tick);
      } catch (error) {
        if (cancelled) return;
        const code = classifyCameraError(error);
        setErrorDetail(describeError(error));
        setErrorCode(code);
        setStatus('error');
        onErrorRef.current?.(code);
      }
    };

    void start();
    return teardown;
    // `formatsKey` stands in for the `formats` array identity.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    active,
    retryNonce,
    formatsKey,
    debounceMs,
    scanIntervalMs,
    diagnosticsIntervalMs,
    requestStream,
    createDecoder,
  ]);

  const stop = useCallback(() => {
    teardownRef.current?.();
    teardownRef.current = null;
    setStatus('idle');
  }, []);

  const restart = useCallback(() => {
    teardownRef.current?.();
    teardownRef.current = null;
    setErrorDetail(null);
    setRetryNonce((nonce) => nonce + 1);
  }, []);

  /**
   * Dev-only: drive the live decoder with an arbitrary source. Used by the scanner's
   * self-test button to prove the decoder can read *a* barcode independently of the
   * camera image. `null` means no decoder is running yet.
   */
  const probeDecode = useCallback(async (source: DetectSource): Promise<DetectedBarcode[] | null> => {
    const decoder = decoderRef.current;
    if (!decoder) return null;
    try {
      return await decoder.detect(source);
    } catch {
      // A decoder that throws on a synthetic frame reads as "could not decode".
      return [];
    }
  }, []);

  return {
    videoRef,
    status,
    errorCode,
    errorMessage: errorCode ? SCANNER_ERROR_MESSAGES[errorCode] : null,
    lastScan,
    flash,
    errorDetail,
    diagnostics,
    probeDecode,
    restart,
    stop,
  };
}