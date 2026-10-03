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
import { ean13Canvas } from '@/lib/barcode/ean13';

// The dev self-test draws a canvas; jsdom has no 2D context, so stub the drawing step
// (the encoder's own maths is covered by `lib/barcode/__tests__/ean13.test.ts`).
vi.mock('@/lib/barcode/ean13', () => ({
  EAN13_TEST_CODE: '4006381333931',
  ean13Canvas: vi.fn((): HTMLCanvasElement | null => ({}) as HTMLCanvasElement),
}));

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

  it('reports an unsupported browser through the real camera path', async () => {
    // No requestStream fake: jsdom has no navigator.mediaDevices, so the real
    // requestRearCamera must fail closed as "unsupported".
    render(<BarcodeScanner active onScan={vi.fn()} />);
    expect(await screen.findByRole('alert')).toHaveTextContent(/cannot scan barcodes/i);
  });

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

  it('does not touch the camera while inactive', () => {
    const requestStream = vi.fn();
    render(<BarcodeScanner active={false} onScan={vi.fn()} requestStream={requestStream} />);
    expect(requestStream).not.toHaveBeenCalled();
    expect(screen.getByRole('status')).toHaveTextContent(/Camera off/i);
  });

  it('supports a dev-only simulated scan without replacing the camera path', async () => {
    const user = userEvent.setup();
    const onScan = vi.fn();
    render(<BarcodeScanner active={false} onScan={onScan} devSimulateValue="737628064502" />);

    await user.click(screen.getByRole('button', { name: /simulate scan/i }));
    expect(onScan).toHaveBeenCalledWith('737628064502', 'ean_13');
  });

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

  it('dev self-test decodes a known-good EAN-13 through the live decoder', async () => {
    const user = userEvent.setup();
    const { stream } = fakeStream();
    const createDecoder = vi.fn(
      async (): Promise<BarcodeDecoder> => ({
        detect: async () => [{ rawValue: '4006381333931', format: 'ean_13' }],
        kind: 'ponyfill',
      }),
    );

    render(
      <BarcodeScanner
        active
        onScan={vi.fn()}
        requestStream={async () => stream}
        createDecoder={createDecoder}
        devSimulateValue="737628064502"
        debounceMs={60000}
      />,
    );

    await user.click(await screen.findByRole('button', { name: /known-good EAN-13/i }));
    await waitFor(() =>
      expect(screen.getByTestId('self-test-result')).toHaveTextContent(
        /Decoder OK: read the known-good EAN-13 4006381333931/,
      ),
    );
  });

  it('dev self-test blames the decoder when a perfect barcode cannot be read', async () => {
    const user = userEvent.setup();
    const { stream } = fakeStream();

    render(
      <BarcodeScanner
        active
        onScan={vi.fn()}
        requestStream={async () => stream}
        createDecoder={decoder(() => [])}
        devSimulateValue="737628064502"
      />,
    );

    await user.click(await screen.findByRole('button', { name: /known-good EAN-13/i }));
    await waitFor(() =>
      expect(screen.getByTestId('self-test-result')).toHaveTextContent(
        /could NOT read a perfect EAN-13/,
      ),
    );
  });

  it('dev self-test reports when the symbol cannot be drawn', async () => {
    const user = userEvent.setup();
    const { stream } = fakeStream();
    vi.mocked(ean13Canvas).mockReturnValueOnce(null);

    render(
      <BarcodeScanner
        active
        onScan={vi.fn()}
        requestStream={async () => stream}
        createDecoder={decoder(() => [])}
        devSimulateValue="737628064502"
      />,
    );

    await user.click(await screen.findByRole('button', { name: /known-good EAN-13/i }));
    await waitFor(() =>
      expect(screen.getByTestId('self-test-result')).toHaveTextContent(/no 2D canvas context/i),
    );
  });

  it('omits the dev self-test control without the dev flag', async () => {
    const { stream } = fakeStream();

    render(
      <BarcodeScanner
        active
        onScan={vi.fn()}
        requestStream={async () => stream}
        createDecoder={decoder(() => [])}
      />,
    );

    await waitFor(() => expect(screen.getByTestId('scanner-diagnostics')).toBeInTheDocument());
    expect(screen.queryByRole('button', { name: /known-good EAN-13/i })).not.toBeInTheDocument();
  });
});