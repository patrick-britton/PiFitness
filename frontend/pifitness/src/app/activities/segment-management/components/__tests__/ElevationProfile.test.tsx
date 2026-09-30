/**
 * ElevationProfile behaviour test (009-003 T28, AC-18).
 *
 * jsdom has no canvas, so react-chartjs-2's Line is stubbed to capture the
 * options object. What is asserted is the axis contract: the distance axis
 * terminates exactly at the last plotted distance (a 3,228 m route gets
 * `max: 3228`), instead of Chart.js rounding up to the next "nice" tick
 * (3,500) and drawing empty plot past the data.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render } from '@testing-library/react';
import type { ChartData, ChartOptions } from 'chart.js';

const captured = vi.hoisted(() => ({
  options: null as ChartOptions<'line'> | null,
  data: null as ChartData<'line'> | null,
}));

vi.mock('react-chartjs-2', () => ({
  Line: ({ options, data }: { options: ChartOptions<'line'>; data: ChartData<'line'> }) => {
    captured.options = options;
    captured.data = data;
    return <div data-testid="line" />;
  },
}));

import ElevationProfile from '../ElevationProfile';
import { cumulativeMeters } from '@/lib/segment-trim';
import { tokenRgb } from '@/lib/token-color';

/**
 * Click-only plot picks (009-010 T05, FR-2): the ONLY gesture that reports is
 * `onClick(event, elements, chart)` — the event's own `x` is already
 * chart-area pixels, so the report reads it directly. There is no `onHover`
 * path (hovering must never move the emphasized map markers) and no rAF
 * coalescing (nothing per-frame can exist without a hover source).
 */
beforeEach(() => {
  captured.options = null;
  captured.data = null;
});

afterEach(() => {
  document.documentElement.classList.remove('dark');
});

/**
 * A two-point due-north path whose cumulative distance is exactly `distanceM`:
 * pure-latitude arcs are linear in Δφ, so scaling the 0.001° probe step is
 * exact and independent of the haversine radius.
 */
function coordsEndingAt(distanceM: number): number[][] {
  const mPerStep = cumulativeMeters([[0, 0], [0, 0.001]])[1];
  return [[0, 0], [0, (distanceM / mPerStep) * 0.001]];
}

describe('ElevationProfile distance axis (AC-18)', () => {
  beforeEach(() => {
    captured.options = null;
    captured.data = null;
  });

  it('sets scales.x.max to the last data distance (3228, not a rounded-up tick)', () => {
    const coords = coordsEndingAt(3228);
    // Guard the fixture: the route really ends at 3,228 m after rounding.
    expect(Math.round(cumulativeMeters(coords).slice(-1)[0])).toBe(3228);

    render(<ElevationProfile coords={coords} elevations={[10, 25]} />);

    const x = captured.options?.scales?.x as { max?: number } | undefined;
    expect(x?.max).toBe(3228);
  });

  it('updates the axis max when the route changes', () => {
    const { rerender } = render(
      <ElevationProfile coords={coordsEndingAt(1000)} elevations={[5, 9]} />,
    );
    expect((captured.options?.scales?.x as { max?: number })?.max).toBe(1000);

    rerender(<ElevationProfile coords={coordsEndingAt(2500)} elevations={[5, 15]} />);
    expect((captured.options?.scales?.x as { max?: number })?.max).toBe(2500);
  });

  it('renders the empty state without chart options for a single-point route', () => {
    // A one-point path has no line to draw — the component shows its empty
    // message and never builds (or captures) chart options for it.
    const { getByText } = render(
      <ElevationProfile coords={[[0, 0]]} elevations={[10]} />,
    );
    expect(getByText('No elevation data for this range')).toBeTruthy();
    expect(captured.options).toBeNull();
  });
});

/**
 * Series colour/dash contract (009-003 T35, AC-24): the segment/course line is
 * `--chart-1` (blue) and solid, the comparison line is `--chart-2` (orange) and
 * dashed, in both themes. The dash is plot-only since 009-010 T11 retired the
 * map's `line-dasharray` (OQ-5) — see OQ-6 in the 009-010 design.
 */
describe('ElevationProfile series colours (AC-24)', () => {
  type Series = { label?: string; borderColor?: string; borderDash?: number[] };

  const series = () => (captured.data?.datasets ?? []) as unknown as Series[];
  const legendDisplay = () =>
    (captured.options?.plugins?.legend as { display?: boolean } | undefined)?.display;

  beforeEach(() => {
    captured.options = null;
    captured.data = null;
    document.documentElement.classList.remove('dark');
  });

  afterEach(() => {
    document.documentElement.classList.remove('dark');
  });

  it('draws a lone profile in --chart-1, solid, with no legend', () => {
    render(<ElevationProfile coords={coordsEndingAt(1000)} elevations={[5, 9]} />);

    expect(series()).toHaveLength(1);
    expect(series()[0].borderColor).toBe(tokenRgb('--chart-1', false));
    // Not the old hard-coded green: the profile matches the map's route colour.
    expect(series()[0].borderColor).not.toBe(tokenRgb('--chart-3', false));
    expect(series()[0].borderDash ?? []).toEqual([]);
    expect(series()[0].label).toBe('Elevation');
    expect(legendDisplay()).toBe(false);
  });

  it('draws the comparison series in --chart-2 dashed and names both in the legend', () => {
    render(
      <ElevationProfile
        coords={coordsEndingAt(1000)}
        elevations={[5, 9]}
        label="Test Loop"
        comparison={{ coords: coordsEndingAt(1200), elevations: [6, 11], label: 'Candidate 11' }}
      />,
    );

    expect(series()).toHaveLength(2);
    expect(series()[0].label).toBe('Test Loop');
    expect(series()[0].borderColor).toBe(tokenRgb('--chart-1', false));
    expect(series()[1].label).toBe('Candidate 11');
    expect(series()[1].borderColor).toBe(tokenRgb('--chart-2', false));
    expect(series()[1].borderDash?.length ?? 0).toBeGreaterThan(0);
    expect(legendDisplay()).toBe(true);
    // The axis still ends at the data limit — here the longer of the two series (AC-18).
    expect((captured.options?.scales?.x as { max?: number })?.max).toBe(1200);
  });

  it('re-reads both tokens for the dark theme', () => {
    document.documentElement.classList.add('dark');
    render(
      <ElevationProfile
        coords={coordsEndingAt(1000)}
        elevations={[5, 9]}
        comparison={{ coords: coordsEndingAt(1200), elevations: [6, 11], label: 'Candidate 11' }}
      />,
    );

    // Fixture guard: the two themes really do resolve to different colours.
    expect(tokenRgb('--chart-1', true)).not.toBe(tokenRgb('--chart-1', false));
    expect(series()[0].borderColor).toBe(tokenRgb('--chart-1', true));
    expect(series()[1].borderColor).toBe(tokenRgb('--chart-2', true));
  });
});

/**
 * Click-only pick contract (009-010 T05, FR-2, AC-3): the plot draws the
 * picked distance as a thin vertical indicator (a 2-point dataset, never a
 * counted series) and reports ONLY `onClick(event, elements, chart)` — the
 * event's own `x` is already chart-area pixels. There is deliberately no
 * `onHover` key (hovering must neither move the markers nor emit a report),
 * and no rAF coalescing (nothing per-frame can exist without a hover source).
 */
describe('ElevationProfile click-only pick (009-010 T05, FR-2)', () => {
  type PickDataset = { label?: string; data?: { x: number; y: number }[] };
  const picked: number[] = [];
  const report = (distanceM: number) => {
    picked.push(distanceM);
  };
  const datasets = () => (captured.data?.datasets ?? []) as unknown as PickDataset[];
  const indicator = () => datasets().find((d) => d.label === 'Picked point');
  /** Drive `onClick` the way Chart.js does: (event, elements, chart). */
  const fireClick = (pixelX: number, value: number) => {
    const options = captured.options as unknown as {
      onHover?: (...args: unknown[]) => void;
      onClick?: (
        event: { x: number },
        elements: unknown[],
        chart: { scales: { x: { getValueForPixel: (px: number) => number } } },
      ) => void;
    };
    options.onClick?.(
      { x: pixelX },
      [],
      { scales: { x: { getValueForPixel: (px: number) => (px === pixelX ? value : NaN) } } },
    );
    return value;
  };

  beforeEach(() => {
    captured.options = null;
    captured.data = null;
    picked.length = 0;
  });

  it('draws no indicator without pickDistance and reports a click from event.x', () => {
    render(
      <ElevationProfile
        coords={coordsEndingAt(1000)}
        elevations={[5, 9]}
        onPickDistance={report}
      />,
    );

    expect(indicator()).toBeUndefined();
    // The pixel IS the chart-area offset: 42 here would have been stablemate
    // `native.offsetX` under the old 2-arg reading — it must NOT be used.
    const x = fireClick(42, 420);
    expect(x).toBe(420);
    expect(picked).toEqual([420]);
  });

  it('exposes no onHover path: hovering the plot emits nothing', () => {
    render(
      <ElevationProfile
        coords={coordsEndingAt(1000)}
        elevations={[5, 9]}
        onPickDistance={report}
      />,
    );

    expect(captured.options).not.toHaveProperty('onHover');
    expect(picked).toEqual([]);
  });

  it('draws the indicator only when pickDistance is set', () => {
    const { rerender } = render(
      <ElevationProfile
        coords={coordsEndingAt(1000)}
        elevations={[5, 9]}
        pickDistance={250}
        onPickDistance={report}
      />,
    );

    const line = indicator();
    expect(line?.data).toEqual([
      { x: 250, y: 5 },
      { x: 250, y: 9 },
    ]);
    // Guide line only: never counted as a data series.
    expect(datasets()).toHaveLength(2);

    const clickX = fireClick(7, 700);
    expect(clickX).toBe(700);
    expect(picked).toEqual([700]);

    rerender(
      <ElevationProfile
        coords={coordsEndingAt(1000)}
        elevations={[5, 9]}
        pickDistance={null}
        onPickDistance={report}
      />,
    );
    expect(indicator()).toBeUndefined();
  });

  it('ignores a click whose event carries no chart-area x', () => {
    render(
      <ElevationProfile
        coords={coordsEndingAt(1000)}
        elevations={[5, 9]}
        pickDistance={null}
        onPickDistance={report}
      />,
    );
    const options = captured.options as unknown as {
      onClick?: (
        event: { x: number | null },
        elements: unknown[],
        chart: { scales: { x: { getValueForPixel: (px: number) => number } } },
      ) => void;
    };
    options.onClick?.(
      { x: null },
      [],
      { scales: { x: { getValueForPixel: () => 123 } } },
    );
    expect(picked).toEqual([]);
  });
});

/**
 * Deleted hover path (009-010 T05 supersedes 009-003 T42): the rAF-coalesced
 * `onHover` report, `requestAnimationFrame`/`cancelAnimationFrame` usage and
 * the T42 coalescing cases are gone with it — this describe asserts nothing
 * can reappear unnoticed.
 */
describe('ElevationProfile deleted hover path (009-010 T05 supersedes T42)', () => {
  it('schedules no animation frame and tolerates an unmount without one', () => {
    const frames = new Map<number, () => void>();
    let nextFrameId = 1;
    vi.stubGlobal('requestAnimationFrame', (cb: () => void) => {
      const id = nextFrameId;
      nextFrameId += 1;
      frames.set(id, cb);
      return id;
    });
    vi.stubGlobal('cancelAnimationFrame', (id: number) => {
      frames.delete(id);
    });
    try {
      const { unmount } = render(
        <ElevationProfile coords={coordsEndingAt(1000)} elevations={[5, 9]} />,
      );
      // Click-only: the component never touches rAF, so nothing is scheduled
      // and unmounting has no pending frame to flush.
      expect(frames.size).toBe(0);
      unmount();
      expect(frames.size).toBe(0);
    } finally {
      vi.unstubAllGlobals();
    }
  });
});

