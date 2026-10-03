/**
 * Camera acquisition + error classification for the barcode scanner (010-001 T06).
 *
 * Pure-ish helpers kept separate from the React layer so the component/hook can be
 * driven with injected fakes in tests, and so the six user-visible camera failure
 * modes required by `.features/descriptions/camera-barcode-scanner.md` are classified
 * in one place instead of inline in JSX.
 */

/**
 * The six user-visible camera states, plus `decoder` (the camera is fine but the
 * decoding backend could not be created) and an `unknown` catch-all.
 */
export type ScannerErrorCode =
  | 'denied'
  | 'no-camera'
  | 'in-use'
  | 'unsupported'
  | 'insecure'
  | 'decoder'
  | 'unknown';

export const SCANNER_ERROR_MESSAGES: Record<ScannerErrorCode, string> = {
  denied: 'Camera permission was denied. Allow camera access in your browser, then retry.',
  'no-camera': 'No camera was found on this device. Enter the barcode manually instead.',
  'in-use': 'The camera appears to be in use by another app. Close it, then retry.',
  unsupported: 'This browser/device cannot scan barcodes. Enter the barcode manually instead.',
  insecure: 'The camera needs a secure context (HTTPS or localhost). Enter the barcode manually instead.',
  decoder:
    'The barcode decoder could not be loaded, so nothing can be scanned. Retry, or enter the barcode manually instead.',
  unknown: 'The camera could not be started. Enter the barcode manually instead.',
};

/**
 * Readable text for a caught error, so the scanner can show *why* it failed instead of
 * only the generic per-code message. `null` when there is nothing useful to show.
 */
export function describeError(error: unknown): string | null {
  if (error instanceof Error) {
    return error.message ? `${error.name}: ${error.message}` : error.name;
  }
  if (!error) return null;
  const text = String(error);
  return text && text !== '[object Object]' ? text : null;
}

/** Rear camera, 1280x720 ideal, no audio (per the scanner spec). */
export const CAMERA_CONSTRAINTS: MediaStreamConstraints = {
  video: { facingMode: { ideal: 'environment' }, width: { ideal: 1280 }, height: { ideal: 720 } },
  audio: false,
};

/** Is the camera API even present in this browser? */
export function isCameraSupported(): boolean {
  return (
    typeof navigator !== 'undefined' &&
    typeof navigator.mediaDevices?.getUserMedia === 'function'
  );
}

/**
 * `getUserMedia` only works on HTTPS or localhost. Testing on a phone via
 * `http://192.168.x.x` fails here rather than surfacing as a confusing DOM error.
 */
export function isSecureCameraContext(): boolean {
  if (typeof window === 'undefined') return false;
  return isSecureLocation(window.location.protocol, window.location.hostname, window.isSecureContext);
}

/** Pure core of {@link isSecureCameraContext}, so the rule is testable without a DOM. */
export function isSecureLocation(
  protocol: string,
  hostname: string,
  secureContextFlag?: boolean,
): boolean {
  if (secureContextFlag === true) return true;
  return protocol === 'https:' || hostname === 'localhost' || hostname === '127.0.0.1';
}

function namedError(name: string, message: string): Error {
  const error = new Error(message);
  error.name = name;
  return error;
}

/**
 * Request a rear-camera stream, failing with a *named* error so
 * {@link classifyCameraError} can map it to a user-visible state.
 */
export async function requestRearCamera(): Promise<MediaStream> {
  if (!isCameraSupported()) {
    throw namedError('TypeError', 'MediaDevices.getUserMedia is unavailable.');
  }
  if (!isSecureCameraContext()) {
    throw namedError('SecurityError', 'A secure context is required for camera access.');
  }
  return navigator.mediaDevices.getUserMedia(CAMERA_CONSTRAINTS);
}

/** Map a `getUserMedia` rejection (by DOMException name) to a user-visible state. */
export function classifyCameraError(error: unknown): ScannerErrorCode {
  const name = (error as { name?: unknown } | null | undefined)?.name;
  switch (typeof name === 'string' ? name : '') {
    case 'NotAllowedError':
    case 'PermissionDeniedError':
      return 'denied';
    case 'NotFoundError':
    case 'DevicesNotFoundError':
    case 'OverconstrainedError':
    case 'ConstraintNotSatisfiedError':
      return 'no-camera';
    case 'NotReadableError':
    case 'TrackStartError':
      return 'in-use';
    case 'SecurityError':
      return 'insecure';
    case 'TypeError':
      return 'unsupported';
    default:
      return 'unknown';
  }
}

/**
 * Stop every track so the camera indicator light turns off. Called on close,
 * deactivate, and unmount — the scanner must never hold the camera open.
 */
export function stopMediaStream(stream: MediaStream | null | undefined): void {
  if (!stream?.getTracks) return;
  for (const track of stream.getTracks()) {
    try {
      track.stop();
    } catch {
      // A track that is already ended throws in some browsers; nothing to do.
    }
  }
}