/**
 * TrimRouteViews layout and gate-view zoom tests (009-003 T22-T25, FR-4).
 *
 * jsdom has no WebGL, so MapLibre is stubbed with a recorder. What is asserted
 * here is the view's own contract: the on-screen order (course map, gate
 * controls, gate maps, elevation profile), the reserved map heights per layout
 * variant, and the gate-view zoom rules — the fixed zoom is applied once to a
 * fresh map and never re-applied when a gate moves. Pixel rendering and
 * appearance stay a human runtime check.
 */

import { describe, expect, it, vi, beforeEach } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import type { ComponentProps } from 'react';

import TrimRouteViews from '../TrimRouteViews';
import { useViewportStore } from '@/stores/viewportStore';

interface RecordedMap {
  centers: number[][];
  zooms: number[];
  fits: unknown[];
}

const recorder = vi.hoisted(() => ({ maps: [] as RecordedMap[] }));

vi.mock('maplibre-gl', () => {
  class Map {
    centers: number[][] = [];
    zooms: number[] = [];
    fits: unknown[] = [];

    constructor() {
      recorder.maps.push(this as unknown as RecordedMap);
    }
    addControl() {}
    isStyleLoaded() {
      return true;
    }
    on() {}
    once() {}
    off() {}
    getSource() {
      return undefined;
    }
    addSource() {}
    addLayer() {}
    setPaintProperty() {}
    fitBounds(bounds: unknown) {
      this.fits.push(bounds);
    }
    setCenter(center: number[]) {
      this.centers.push(center);
    }
    setZoom(zoom: number) {
      this.zooms.push(zoom);
    }
    remove() {}
  }
  class NavigationControl {}
  return { default: { Map, NavigationControl } };
});

vi.mock('../MapStyleSelector', () => ({ default: () => <div data-testid="style-selector" /> }));
vi.mock('../ElevationProfile', () => ({ default: () => <div data-testid="elevation" /> }));

type ViewsProps = ComponentProps<typeof TrimRouteViews>;

/** 0.001° of latitude per point ≈ 111 m, so 11 points ≈ 1111 m. */
const COORDS: number[][] = Array.from({ length: 11 }, (_, i) => [0, i * 0.001]);
const ELEVATIONS: number[] = Array.from({ length: 11 }, (_, i) => 100 + i);

function renderViews(overrides: Partial<ViewsProps> = {}) {
  const props: ViewsProps = {
    coords: COORDS,
    elevations: ELEVATIONS,
    gates: { startM: 0, endM: 1111 },
    maxM: 1111,
    styleId: 'positron',
    token: null,
    onStyleChange: vi.fn(),
    onStartChange: vi.fn(),
    onEndChange: vi.fn(),
    ...overrides,
  };
  return { ...render(<TrimRouteViews {...props} />), props };
}

/** `first` must precede `second` in document order. */
function expectBefore(first: Element, second: Element) {
  expect(first.compareDocumentPosition(second) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
}

beforeEach(() => {
  recorder.maps.length = 0;
  act(() => useViewportStore.setState({ layoutVariant: 'portrait' }));
});

describe('TrimRouteViews layout (T22/T23)', () => {
  it('orders style selector, course map, gate controls, gate maps, elevation profile', () => {
    renderViews();

    const styleSelector = screen.getByTestId('style-selector');
    const courseMap = screen.getByRole('img', { name: /Trimmed route from 0 to 1111 meters/ });
    const startGate = screen.getByLabelText('Select start gate');
    const endGate = screen.getByLabelText('Select end gate');
    const range = screen.getByText(/Selected range: 0–1111 m/);
    const startMap = screen.getByRole('img', { name: /Route near the start gate/ });
    const endMap = screen.getByRole('img', { name: /Route near the end gate/ });
    const profile = screen.getByTestId('elevation');

    expectBefore(styleSelector, courseMap);
    expectBefore(courseMap, startGate);
    expectBefore(startGate, endGate);
    expectBefore(endGate, range);
    expectBefore(range, startMap);
    expectBefore(startMap, endMap);
    expectBefore(endMap, profile);
  });

  it('reserves taller gate maps than the course map in every layout variant (T22)', () => {
    const variants: { variant: 'desktop' | 'portrait' | 'landscape'; course: string; gate: string }[] = [
      { variant: 'desktop', course: '320px', gate: '280px' },
      { variant: 'portrait', course: '240px', gate: '230px' },
      { variant: 'landscape', course: '200px', gate: '170px' },
    ];

    for (const row of variants) {
      act(() => useViewportStore.setState({ layoutVariant: row.variant }));
      const { unmount } = renderViews();

      const courseMap = screen.getByRole('img', { name: /Trimmed route from/ });
      const gateMap = screen.getByRole('img', { name: /Route near the start gate/ });
      expect(courseMap).toHaveStyle({ height: row.course });
      expect(gateMap).toHaveStyle({ height: row.gate });

      unmount();
    }
  });

  it('reports gate edits to the owner and keeps 1 m steps (T19/T23)', () => {
    const onStartChange = vi.fn();
    renderViews({ onStartChange });

    const startGate = screen.getByLabelText('Select start gate');
    expect(startGate).toHaveAttribute('step', '1');
    expect(startGate).toHaveAttribute('max', '1111');

    fireEvent.change(startGate, { target: { value: '500' } });

    expect(onStartChange).toHaveBeenCalledWith(500);
  });
});

describe('TrimRouteViews gate-view zoom (T24/T25)', () => {
  it('opens both gate views at the +2 step zoom while the course map still fits bounds (T24)', () => {
    renderViews();

    const [courseMap, startGateMap, endGateMap] = recorder.maps;
    expect(startGateMap.zooms).toEqual([18]);
    expect(endGateMap.zooms).toEqual([18]);
    expect(startGateMap.centers).toEqual([[0, 0]]);
    expect(courseMap.zooms).toEqual([]);
    expect(courseMap.fits).toHaveLength(1);
  });

  it('re-centres on a moved gate without re-applying the fixed zoom (T25)', () => {
    const { rerender, props } = renderViews();
    const [courseMap, startGateMap] = recorder.maps;

    rerender(<TrimRouteViews {...props} gates={{ startM: 500, endM: 1111 }} />);

    // The view follows the moved gate...
    expect(startGateMap.centers).toEqual([
      [0, 0],
      [0, 0.005],
    ]);
    // ...the fixed zoom is not re-applied, and no map was rebuilt.
    expect(startGateMap.zooms).toEqual([18]);
    expect(recorder.maps).toHaveLength(3);
    // T15 preserved: the unchanged route array never reframes the course map.
    expect(courseMap.fits).toHaveLength(1);
  });
});


