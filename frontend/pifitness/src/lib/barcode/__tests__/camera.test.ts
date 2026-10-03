/**
 * Camera acquisition + error classification (010-001 T06).
 * Covers the six user-visible camera states, the rear-camera constraints,
 * track cleanup (indicator light off), and the unsupported/insecure guards.
 */

import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  CAMERA_CONSTRAINTS,
  SCANNER_ERROR_MESSAGES,
  classifyCameraError,
  describeError,
  isCameraSupported,
  isSecureCameraContext,
  isSecureLocation,
  requestRearCamera,
  stopMediaStream,
  type ScannerErrorCode,
} from '../camera';

function named(name: string): Error {
  const error = new Error(name);
  error.name = name;
  return error;
}

function fakeStream() {
  const stopA = vi.fn();
  const stopB = vi.fn();
  const stream = {
    getTracks: () => [
      { stop: stopA } as unknown as MediaStreamTrack,
      { stop: stopB } as unknown as MediaStreamTrack,
    ],
  } as unknown as MediaStream;
  return { stream, stopA, stopB };
}

afterEach(() => {
  vi.unstubAllGlobals();
  Reflect.deleteProperty(navigator, 'mediaDevices');
});

describe('classifyCameraError', () => {
  it.each<[string, ScannerErrorCode]>([
    ['NotAllowedError', 'denied'],
    ['PermissionDeniedError', 'denied'],
    ['NotFoundError', 'no-camera'],
    ['DevicesNotFoundError', 'no-camera'],
    ['OverconstrainedError', 'no-camera'],
    ['NotReadableError', 'in-use'],
    ['TrackStartError', 'in-use'],
    ['SecurityError', 'insecure'],
    ['TypeError', 'unsupported'],
  ])('maps %s → %s', (name, expected) => {
    expect(classifyCameraError(named(name))).toBe(expected);
  });

  it('falls back to unknown for anything else', () => {
    expect(classifyCameraError(new Error('boom'))).toBe('unknown');
    expect(classifyCameraError(null)).toBe('unknown');
  });

  it('has a user-visible message for every code', () => {
    const codes: ScannerErrorCode[] = [
      'denied',
      'no-camera',
      'in-use',
      'unsupported',
      'insecure',
      'decoder',
      'unknown',
    ];
    for (const code of codes) {
      expect(SCANNER_ERROR_MESSAGES[code]).toMatch(/\S/);
    }
  });

  it('has a message for a decoder that could not be created', () => {
    // The camera can be perfectly healthy while the decoder is not (T08).
    expect(SCANNER_ERROR_MESSAGES.decoder).toMatch(/decoder could not be loaded/i);
    expect(SCANNER_ERROR_MESSAGES.decoder).toMatch(/manually/i);
  });
});

describe('describeError', () => {
  it('includes the error name and message', () => {
    expect(describeError(named('NotAllowedError'))).toBe('NotAllowedError: NotAllowedError');
    expect(describeError(new Error('wasm instantiate failed'))).toBe(
      'Error: wasm instantiate failed',
    );
  });

  it('falls back to the string form and ignores empty values', () => {
    expect(describeError('boom')).toBe('boom');
    expect(describeError(null)).toBeNull();
    expect(describeError(undefined)).toBeNull();
    expect(describeError('')).toBeNull();
    expect(describeError({})).toBeNull();
  });
});

describe('stopMediaStream', () => {
  it('stops every track so the camera indicator goes off', () => {
    const { stream, stopA, stopB } = fakeStream();
    stopMediaStream(stream);
    expect(stopA).toHaveBeenCalledTimes(1);
    expect(stopB).toHaveBeenCalledTimes(1);
  });

  it('is a no-op for a missing stream', () => {
    expect(() => stopMediaStream(null)).not.toThrow();
    expect(() => stopMediaStream(undefined)).not.toThrow();
  });
});

describe('isCameraSupported / isSecureCameraContext', () => {
  it('reports no camera when getUserMedia is absent', () => {
    expect(isCameraSupported()).toBe(false);
  });

  it('reports a camera once getUserMedia exists', () => {
    Object.defineProperty(navigator, 'mediaDevices', {
      value: { getUserMedia: vi.fn() },
      configurable: true,
    });
    expect(isCameraSupported()).toBe(true);
  });

  it('treats localhost/127.0.0.1 and https as secure', () => {
    expect(isSecureCameraContext()).toBe(true); // jsdom default origin is http://localhost
  });

  it('classifies contexts by protocol/hostname', () => {
    expect(isSecureLocation('https:', 'pifitness.duckdns.org', false)).toBe(true);
    expect(isSecureLocation('http:', 'localhost', false)).toBe(true);
    expect(isSecureLocation('http:', '127.0.0.1', false)).toBe(true);
    // The failure the spec warns about: a phone hitting the dev box over plain http.
    expect(isSecureLocation('http:', '192.168.1.50', false)).toBe(false);
    expect(isSecureLocation('http:', '192.168.1.50', undefined)).toBe(false);
    expect(isSecureLocation('http:', '192.168.1.50', true)).toBe(true);
  });
});

describe('requestRearCamera', () => {
  it('fails as unsupported when the browser has no camera API', async () => {
    await expect(requestRearCamera()).rejects.toMatchObject({ name: 'TypeError' });
  });

  it('asks for the rear camera at 1280x720 with no audio', async () => {
    const getUserMedia = vi.fn().mockResolvedValue(fakeStream().stream);
    Object.defineProperty(navigator, 'mediaDevices', {
      value: { getUserMedia },
      configurable: true,
    });
    await expect(requestRearCamera()).resolves.toBeTruthy();
    expect(getUserMedia).toHaveBeenCalledWith(CAMERA_CONSTRAINTS);
    expect(CAMERA_CONSTRAINTS).toMatchObject({ audio: false });
  });
});