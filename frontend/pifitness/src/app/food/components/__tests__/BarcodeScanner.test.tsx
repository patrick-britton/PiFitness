/**
 * BarcodeScanner (010-001 T06/T08) — camera scanning behaviour with a mocked camera
 * and a mocked decoder (jsdom cannot decode video): one scan per distinct code,
 * re-emit on a different code, the six error states, retry, camera release, and the
 * T08 diagnostics readout (decoder backend, frames, surfaced decode errors, and a
 * decoder that could not be created).
 */

import { describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import BarcodeScanner from '../BarcodeScanner';
import { RETAIL_BARCODE_FORMATS, type BarcodeDecoder } from '@/lib/barcode/decoder';

function fakeStream() {
  const stop = vi.fn();
  const stream = {
    getTracks: () => [{ stop } as unknown as MediaStreamTrack],
  } as unknown as MediaStream;
  return { stream, stop };
}

function named(name: string): Error {
  const error = new Error(name);
  error.name = name;
  return error;
}

/** Decoder factory whose `detect` yields `next()` results on every call. */
function decoder(next: () => { rawValue: string; format: string }[]) {
  return vi.fn(async (): Promise<BarcodeDecoder> => ({ detect: async () => next() }));
}

describe('BarcodeScanner', () => {
  // VERIFIED: Barcode scanner triggers onScan once per unique detected barcode and debounces repeated frames
  it('reports one scan per distinct code and ignores repeats', async () => {
    const { stream } = fakeStream();
    const onScan = vi.fn();
    const createDecoder = decoder(() => [{ rawValue: '4006381333931', format: 'ean_13' }]);

    render(
      <BarcodeScanner
        active
        onScan={onScan}
        requestStream={async () => stream}
        createDecoder={createDecoder}
        scanIntervalMs={1}
        debounceMs={5000}
      />,
    );

    await waitFor(() => expect(onScan).toHaveBeenCalledTimes(1));
    expect(onScan).toHaveBeenCalledWith('4006381333931', 'ean_13');
    // Requests only the retail family.
    expect(createDecoder).toHaveBeenCalledWith([...RETAIL_BARCODE_FORMATS]);
    // Scan-area guide + success readout are present.
    expect(screen.getByTestId('scan-reticle')).toBeInTheDocument();
    expect(await screen.findByText(/Last scan: 4006381333931/)).toBeInTheDocument();

    // Many more decode ticks with the same code must not re-emit.
    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(onScan).toHaveBeenCalledTimes(1);
  });

  // VERIFIED: Barcode scanner re-triggers onScan when a distinct second code enters the camera frame
  it('emits again for a different code', async () => {
    const { stream } = fakeStream();
    const onScan = vi.fn();
    let call = 0;
    const createDecoder = decoder(() => {
      call += 1;
      return call === 1
        ? [{ rawValue: '111111111111', format: 'ean_13' }]
        : [{ rawValue: '222222222222', format: 'ean_13' }];
    });

    render(
      <BarcodeScanner
        active
        onScan={onScan}
        requestStream={async () => stream}
        createDecoder={createDecoder}
        scanIntervalMs={1}
      />,
    );

    await waitFor(() => expect(onScan).toHaveBeenCalledWith('222222222222', 'ean_13'));
  });

  // VERIFIED: Camera permission rejection displays explanatory alert and offers retry action
  it('surfaces permission denial and offers a retry', async () => {
    const onError = vi.fn();
    render(
      <BarcodeScanner
        active
        onScan={vi.fn()}
        onError={onError}
        requestStream={async () => {
          throw named('NotAllowedError');
        }}
        createDecoder={decoder(() => [])}
      />,
    );

    expect(await screen.findByRole('alert')).toHaveTextContent(/permission was denied/i);
    expect(onError).toHaveBeenCalledWith('denied');
    expect(screen.getByRole('button', { name: /Retry camera/i })).toBeInTheDocument();
  });

  // VERIFIED: Missing camera hardware displays user alert and propagates no-camera error code
  it('surfaces a missing camera', async () => {
    const onError = vi.fn();
    render(
      <BarcodeScanner
        active
        onScan={vi.fn()}
        onError={onError}
        requestStream={async () => {
          throw named('NotFoundError');
        }}
        createDecoder={decoder(() => [])}
      />,
    );

    expect(await screen.findByRole('alert')).toHaveTextContent(/No camera was found/i);
    expect(onError).toHaveBeenCalledWith('no-camera');
  });

  // VERIFIED: Insecure non-HTTPS contexts display clear security warning alert
  it('surfaces an insecure context', async () => {
    render(
      <BarcodeScanner
        active
        onScan={vi.fn()}
        requestStream={async () => {
          throw named('SecurityError');
        }}
        createDecoder={decoder(() => [])}
      />,
    );

    expect(await screen.findByRole('alert')).toHaveTextContent(/secure context/i);
  });

  // VERIFIED: Browsers lacking mediaDevices support fail closed with unsupported alert
  it('reports an unsupported browser through the real camera path', async () => {
    // No requestStream fake: jsdom has no navigator.mediaDevices, so the real
    // requestRearCamera must fail closed as "unsupported".
    render(<BarcodeScanner active onScan={vi.fn()} />);
    expect(await screen.findByRole('alert')).toHaveTextContent(/cannot scan barcodes/i);
  });

  // VERIFIED: Unmounting scanner component terminates all active media stream tracks
  it('releases the camera on unmount', async () => {
    const { stream, stop } = fakeStream();
    const { unmount } = render(
      <BarcodeScanner
        active
        onScan={vi.fn()}
        requestStream={async () => stream}
        createDecoder={decoder(() => [])}
      />,
    );

    await waitFor(() =>
      expect(screen.getByRole('status')).toHaveTextContent(/Point the camera at a barcode/i),
    );
    unmount();
    expect(stop).toHaveBeenCalledTimes(1);
  });

  // VERIFIED: Clicking retry camera button requests fresh media stream after initial failure
  it('restarts the camera on retry', async () => {
    const user = userEvent.setup();
    const { stream } = fakeStream();
    const requestStream = vi
      .fn()
      .mockRejectedValueOnce(named('NotAllowedError'))
      .mockResolvedValue(stream);

    render(
      <BarcodeScanner
        active
        onScan={vi.fn()}
        requestStream={requestStream}
        createDecoder={decoder(() => [])}
      />,
    );

    await screen.findByRole('alert');
    await user.click(screen.getByRole('button', { name: /Retry camera/i }));
    await waitFor(() =>
      expect(screen.getByRole('status')).toHaveTextContent(/Point the camera at a barcode/i),
    );
    expect(requestStream).toHaveBeenCalledTimes(2);
  });

  // VERIFIED: Inactive scanner state prevents camera stream initialization
  it('does not touch the camera while inactive', () => {
    const requestStream = vi.fn();
    render(<BarcodeScanner active={false} onScan={vi.fn()} requestStream={requestStream} />);
    expect(requestStream).not.toHaveBeenCalled();
    expect(screen.getByRole('status')).toHaveTextContent(/Camera off/i);
  });

  // VERIFIED: Scanning state displays live diagnostics readout and orientation guidance
  it('shows what the scanner is doing while it seeks a barcode', async () => {
    const { stream } = fakeStream();
    const createDecoder = vi.fn(
      async (): Promise<BarcodeDecoder> => ({
        detect: async () => [],
        kind: 'ponyfill',
        nativeFormats: ['ean_13'],
      }),
    );

    render(
      <BarcodeScanner
        active
        onScan={vi.fn()}
        requestStream={async () => stream}
        createDecoder={createDecoder}
        scanIntervalMs={1}
      />,
    );

    expect(await screen.findByTestId('scanner-diagnostics')).toBeInTheDocument();
    expect(screen.getByTestId('diag-decoder')).toHaveTextContent(/ZXing ponyfill/i);
    expect(screen.getByTestId('diag-native')).toHaveTextContent('ean_13');
    expect(screen.getByTestId('diag-formats')).toHaveTextContent('ean_13, ean_8, upc_a, upc_e');
    expect(screen.getByTestId('diag-errors')).toHaveTextContent('0');
    // jsdom has no real video, so give the element the dimensions and clock a live camera
    // would report; the readout must then show the frames, resolution and liveness.
    const video = screen.getByLabelText(/camera preview/i);
    Object.defineProperty(video, 'videoWidth', { value: 1280, configurable: true });
    Object.defineProperty(video, 'videoHeight', { value: 720, configurable: true });
    Object.defineProperty(video, 'paused', { value: false, configurable: true });
    Object.defineProperty(video, 'currentTime', { value: 3.5, configurable: true });
    await waitFor(
      () => expect(screen.getByTestId('diag-frames')).toHaveTextContent('1280×720'),
      { timeout: 3000 },
    );
    // The video row proves the element is playing, not merely sized.
    expect(screen.getByTestId('diag-video')).toHaveTextContent(/live/);
    // Decoding runs but matches nothing: the hint says exactly that.
    expect(screen.getByText(/has not matched a barcode yet/i)).toBeInTheDocument();
  });

  // VERIFIED: Decoder errors encountered during frame processing surface diagnostic failure details
  it('surfaces per-frame decode errors instead of swallowing them', async () => {
    const { stream } = fakeStream();
    const createDecoder = vi.fn(
      async (): Promise<BarcodeDecoder> => ({
        detect: async () => {
          throw new Error('wasm instantiate failed');
        },
        kind: 'ponyfill',
      }),
    );

    render(
      <BarcodeScanner
        active
        onScan={vi.fn()}
        requestStream={async () => stream}
        createDecoder={createDecoder}
        scanIntervalMs={1}
      />,
    );

    await waitFor(
      () => expect(screen.getByTestId('diag-errors')).toHaveTextContent(/wasm instantiate failed/),
      { timeout: 3000 },
    );
    expect(screen.getByText(/the decoder is failing on this image/i)).toBeInTheDocument();
  });

  // VERIFIED: Decoder instantiation failure displays error alert with raw details and retry action
  it('reports a decoder that cannot be created, with the raw error', async () => {
    const { stream } = fakeStream();
    const onError = vi.fn();
    const createDecoder = vi
      .fn()
      .mockRejectedValue(new Error('Failed to load /wasm/zxing_reader.wasm'));

    render(
      <BarcodeScanner
        active
        onScan={vi.fn()}
        onError={onError}
        requestStream={async () => stream}
        createDecoder={createDecoder}
      />,
    );

    expect(await screen.findByRole('alert')).toHaveTextContent(/decoder could not be loaded/i);
    expect(screen.getByText(/Failed to load \/wasm\/zxing_reader\.wasm/)).toBeInTheDocument();
    expect(onError).toHaveBeenCalledWith('decoder');
    expect(screen.getByRole('button', { name: /Retry camera/i })).toBeInTheDocument();
  });
});