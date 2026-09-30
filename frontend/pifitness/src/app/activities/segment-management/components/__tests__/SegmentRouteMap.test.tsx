/**
 * SegmentRouteMap regression tests (009-010 T01).
 *
 * jsdom has no WebGL, so MapLibre is stubbed with a handler recorder. The stub's
 * queryRenderedFeatures throws for any layer id it does not hold (mirroring
 * real MapLibre) so a future pick query fails loudly here. The component must
 * never call it: T01 deleted the map → elevation coupling, so only the exact
 * layer sets are asserted.
 */

import { describe, expect, it, vi, beforeEach } from 'vitest';
import { act, render } from '@testing-library/react';

import SegmentRouteMap, { type PickMarker, type RouteArrow } from '../SegmentRouteMap';

type Handler = (ev: {
  point: { x: number; y: number };
  lngLat: { lng: number; lat: number };
}) => void;

/** MapLibre stub: records `on` handlers, layers/sources, images. */
const recorder = vi.hoisted(() => ({
  instances: [] as {
    handlers: Record<string, Handler[]>;
    layers: { id: string; type: string; source?: string; layout?: unknown; paint?: unknown }[];
    sources: Record<string, { data: unknown; setData: (data: unknown) => void }>;
    images: Record<string, { width: number; height: number; data: unknown }>;
    fitBoundsCalls: number;
    queryResult: { layer?: { id?: string } }[];
  }[],
  queryResult: [] as { layer?: { id?: string } }[],
}));

vi.mock('maplibre-gl', () => {
  class Map {
    handlers: Record<string, Handler[]> = {};
    layers: { id: string; type: string; source?: string; layout?: unknown; paint?: unknown }[] = [];
    sources: Record<string, { data: unknown; setData: (data: unknown) => void }> = {};
    images: Record<string, { width: number; height: number; data: unknown }> = {};
    fitBoundsCalls = 0;
    queryResult: { layer?: { id?: string } }[] = [];
    queryCalls: unknown[][] = [];
    constructor() {
      recorder.instances.push(this as unknown as (typeof recorder.instances)[number]);
    }
    addControl() {}
    isStyleLoaded() {
      return true;
    }
    once() {}
    off(event: string, handler: Handler) {
      this.handlers[event] = (this.handlers[event] ?? []).filter((h) => h !== handler);
    }
    on(event: string, handler: Handler) {
      this.handlers[event] = [...(this.handlers[event] ?? []), handler];
    }
    queryRenderedFeatures(...args: unknown[]) {
      this.queryCalls.push(args);
      const opts = args[1] as { layers?: string[] } | undefined;
      const known = new Set(this.layers.map((l) => l.id));
      for (const id of opts?.layers ?? []) {
        if (!known.has(id)) {
          throw new Error(
            `The layer '${id}' does not exist in the map's style and cannot be queried for features.`,
          );
        }
      }
      return recorder.queryResult;
    }
    getSource(id: string) {
      return this.sources[id];
    }
    addSource(id: string, spec: { data: unknown }) {
      this.sources[id] = {
        data: spec.data,
        setData: (data: unknown) => {
          this.sources[id].data = data;
        },
      };
    }
    addLayer(spec: { id: string; type: string; source?: string; layout?: unknown; paint?: unknown }) {
      this.layers.push({
        id: spec.id,
        type: spec.type,
        source: spec.source,
        layout: spec.layout,
        paint: spec.paint,
      });
    }
    hasImage(id: string) {
      return id in this.images;
    }
    addImage(id: string, image: { width: number; height: number; data: unknown }) {
      this.images[id] = image;
    }
    removeImage(id: string) {
      delete this.images[id];
    }
    setPaintProperty() {}
    fitBounds() {
      this.fitBoundsCalls += 1;
    }
    setCenter() {}
    setZoom() {}
    remove() {}
  }
  class NavigationControl {}
  return { default: { Map, NavigationControl } };
});

const COORDS = [
  [0, 0],
  [0, 0.001],
];

function renderMap() {
  return render(
    <SegmentRouteMap
      coords={COORDS}
      styleId="positron"
      token={null}
      height="240px"
      ariaLabel="pick test map"
    />,
  );
}

beforeEach(() => {
  recorder.instances.length = 0;
  recorder.queryResult = [];
  // Theme is a global (`dark` on <html>) — never leak it between tests.
  document.documentElement.classList.remove('dark');
});

function renderMapWithMarkers(pickMarkers: PickMarker[]) {
  return render(
    <SegmentRouteMap
      coords={COORDS}
      styleId="positron"
      token={null}
      height="240px"
      ariaLabel="pick marker test map"
      pickMarkers={pickMarkers}
    />,
  );
}

function renderMapWithArrows(arrows: RouteArrow[]) {
  return render(
    <SegmentRouteMap
      coords={COORDS}
      styleId="positron"
      token={null}
      height="240px"
      ariaLabel="arrow test map"
      arrows={arrows}
    />,
  );
}

describe('map → elevation coupling deleted (009-010 T01)', () => {
  it('registers no pick listeners', () => {
    renderMap();
    expect(recorder.instances[0].handlers['mousemove'] ?? []).toHaveLength(0);
    expect(recorder.instances[0].handlers['click'] ?? []).toHaveLength(0);
  });
});

describe('picked-distance marker layers (009-010 T05: halo + per-series markers)', () => {
  it('adds the halo + marker circle layer pair, only for callers that pass markers', () => {
    const { unmount } = renderMap();
    // Layer set without the prop: exactly the route/overlay/gate layers.
    expect(recorder.instances[0].layers.map((l) => l.id)).toEqual([
      'route',
      'route-overlay',
      'gate-start',
      'gate-end',
    ]);
    unmount();

    renderMapWithMarkers([
      { series: 'route', point: [0, 0.001], label: 'Test Loop · 111 m' },
      { series: 'route-overlay', point: [1, 0.002], label: 'Candidate 11 · 222 m' },
    ]);

    const inst = recorder.instances[1];
    // Halo first, then its solid marker, per series — all circle layers.
    expect(inst.layers.map((l) => l.id)).toEqual([
      'route',
      'route-overlay',
      'gate-start',
      'gate-end',
      'pick-route-halo',
      'pick-route',
      'pick-overlay-halo',
      'pick-overlay',
    ]);
    expect(inst.layers.filter((l) => l.id.startsWith('pick-')).every((l) => l.type === 'circle')).toBe(
      true,
    );
    // Each layer is fed by its own GeoJSON source carrying its own point + label.
    expect(inst.sources['pick-route'].data).toMatchObject({
      features: [
        { properties: { label: 'Test Loop · 111 m' }, geometry: { coordinates: [0, 0.001] } },
      ],
    });
    expect(inst.sources['pick-overlay'].data).toMatchObject({
      features: [
        { properties: { label: 'Candidate 11 · 222 m' }, geometry: { coordinates: [1, 0.002] } },
      ],
    });
  });

  it('updates the pick layers in place without reframing the map', () => {
    const { rerender } = renderMapWithMarkers([
      { series: 'route', point: [0, 0.001], label: 'Test Loop · 111 m' },
    ]);
    const inst = recorder.instances[0];
    expect(inst.fitBoundsCalls).toBe(1);

    rerender(
      <SegmentRouteMap
        coords={COORDS}
        styleId="positron"
        token={null}
        height="240px"
        ariaLabel="pick marker test map"
        pickMarkers={[
          { series: 'route', point: [0, 0.001], label: 'Test Loop · 111 m' },
          { series: 'route-overlay', point: [1, 0.002], label: 'Candidate 11 · 222 m' },
        ]}
      />,
    );

    // Same instance, same layer set, refreshed data — and no second fit.
    expect(recorder.instances).toHaveLength(1);
    expect(inst.layers).toHaveLength(8);
    expect((inst.sources['pick-overlay'].data as { features: unknown[] }).features).toHaveLength(1);
    expect(inst.fitBoundsCalls).toBe(1);
  });
});

describe('directional arrow layers (009-010 T03, FR-3)', () => {
  const ARROWS: RouteArrow[] = [
    { series: 'route', lngLat: [0, 0.0005], bearing: 0 },
    { series: 'route-overlay', lngLat: [1, 0.0005], bearing: 90 },
  ];

  it('adds exactly the two symbol layers, each fed from its own bearing source', () => {
    renderMapWithArrows(ARROWS);
    const inst = recorder.instances[0];
    expect(inst.layers.map((l) => l.id)).toEqual([
      'route',
      'route-overlay',
      'gate-start',
      'gate-end',
      'route-arrows',
      'route-arrows-overlay',
    ]);
    const symbols = inst.layers.filter((l) => l.type === 'symbol');
    expect(symbols).toHaveLength(2);
    for (const s of symbols) {
      expect(s.layout).toMatchObject({
        'icon-rotate': ['get', 'bearing'],
        'icon-rotation-alignment': 'map',
        'icon-ignore-placement': true,
        'icon-allow-overlap': true,
      });
    }
    // Each layer is fed from its own source carrying bearing properties.
    expect(inst.sources['route-arrows'].data).toMatchObject({
      features: [{ properties: { bearing: 0 }, geometry: { coordinates: [0, 0.0005] } }],
    });
    expect(inst.sources['route-arrows-overlay'].data).toMatchObject({
      features: [{ properties: { bearing: 90 }, geometry: { coordinates: [1, 0.0005] } }],
    });
    // Both masks registered as images.
    expect(inst.images['route-arrows-icon']).toMatchObject({ width: 24, height: 24 });
    expect(inst.images['route-arrows-overlay-icon']).toMatchObject({ width: 24, height: 24 });
  });

  it('adds no arrow layers when the prop is absent and never reframes', () => {
    const { rerender } = renderMap();
    const inst = recorder.instances[0];
    expect(inst.layers.map((l) => l.id)).toEqual([
      'route',
      'route-overlay',
      'gate-start',
      'gate-end',
    ]);
    expect(inst.fitBoundsCalls).toBe(1);

    rerender(
      <SegmentRouteMap
        coords={COORDS}
        styleId="positron"
        token={null}
        height="240px"
        ariaLabel="arrow test map"
        arrows={ARROWS}
      />,
    );
    // Same instance, arrow layers added, still no second fit.
    expect(recorder.instances).toHaveLength(1);
    expect(inst.layers.map((l) => l.id)).toContain('route-arrows');
    expect(inst.layers.map((l) => l.id)).toContain('route-arrows-overlay');
    expect(inst.fitBoundsCalls).toBe(1);
  });

  it('re-registers the glyphs on the map instance rebuilt by a style swap', () => {
    const { rerender } = renderMapWithArrows(ARROWS);
    expect(recorder.instances).toHaveLength(1);

    rerender(
      <SegmentRouteMap
        coords={COORDS}
        styleId="dark-matter"
        token={null}
        height="240px"
        ariaLabel="arrow test map"
        arrows={ARROWS}
      />,
    );

    // A style swap re-creates the instance and images are style-scoped, so the
    // rebuilt map must carry the layers AND re-register both glyphs.
    expect(recorder.instances).toHaveLength(2);
    const inst = recorder.instances[1];
    expect(inst.layers.map((l) => l.id)).toContain('route-arrows');
    expect(inst.layers.map((l) => l.id)).toContain('route-arrows-overlay');
    expect(inst.images['route-arrows-icon']).toMatchObject({ width: 24, height: 24 });
    expect(inst.images['route-arrows-overlay-icon']).toMatchObject({ width: 24, height: 24 });
  });

  it('draws both series solid and bakes a halo into each glyph (009-010 T11, AC-6b)', () => {
    renderMapWithArrows(ARROWS);
    const inst = recorder.instances[0];
    const paintOf = (id: string): Record<string, unknown> =>
      (inst.layers.find((l) => l.id === id)?.paint ?? {}) as Record<string, unknown>;

    // OQ-5: the solid/dashed convention is retired — neither series line
    // carries a dash pattern, and each keeps its own chart token colour.
    expect(paintOf('route')['line-dasharray']).toBeUndefined();
    expect(paintOf('route-overlay')['line-dasharray']).toBeUndefined();
    expect(paintOf('route')['line-color']).toBe('rgb(0 114 178)');
    expect(paintOf('route-overlay')['line-color']).toBe('rgb(213 94 0)');

    // Both icons are baked with the halo (`--text`, near-black in light mode)
    // around their own colour, so neither can blend into the basemap or the
    // other series' glyphs.
    const channelsOf = (id: string): Uint8ClampedArray =>
      inst.images[id].data as Uint8ClampedArray;
    const hasChannel = (data: Uint8ClampedArray, rgb: [number, number, number]): boolean => {
      for (let i = 0; i < data.length; i += 4) {
        if (data[i + 3] > 0 && data[i] === rgb[0] && data[i + 1] === rgb[1] && data[i + 2] === rgb[2]) {
          return true;
        }
      }
      return false;
    };
    expect(hasChannel(channelsOf('route-arrows-icon'), [15, 23, 42])).toBe(true);
    expect(hasChannel(channelsOf('route-arrows-icon'), [0, 114, 178])).toBe(true);
    expect(hasChannel(channelsOf('route-arrows-overlay-icon'), [15, 23, 42])).toBe(true);
    expect(hasChannel(channelsOf('route-arrows-overlay-icon'), [213, 94, 0])).toBe(true);
  });

  it('recolours the halo with the theme', async () => {
    renderMapWithArrows(ARROWS);
    const inst = recorder.instances[0];
    const hasChannel = (rgb: [number, number, number]): boolean => {
      const data = inst.images['route-arrows-icon'].data as Uint8ClampedArray;
      for (let i = 3; i < data.length; i += 4) {
        if (data[i] > 0 && data[i - 3] === rgb[0] && data[i - 2] === rgb[1] && data[i - 1] === rgb[2]) {
          return true;
        }
      }
      return false;
    };
    // Light mode: halo is near-black `--text`.
    expect(hasChannel([15, 23, 42])).toBe(true);

    await act(async () => {
      document.documentElement.classList.add('dark');
    });

    // Dark mode: the same mask is re-baked with the near-white `--text` halo.
    expect(hasChannel([15, 23, 42])).toBe(false);
    expect(hasChannel([241, 245, 249])).toBe(true);
  });

  it('recolours the glyphs on a theme change without adding layers or reframing', async () => {
    renderMapWithArrows(ARROWS);
    const inst = recorder.instances[0];
    const light = inst.images['route-arrows-icon'].data as Uint8ClampedArray;
    // Light `--chart-1` channels; the mask paints a constant RGB per pixel.
    expect(Array.from(light.slice(0, 3))).toEqual([0, 114, 178]);

    await act(async () => {
      document.documentElement.classList.add('dark');
    });

    const dark = inst.images['route-arrows-icon'].data as Uint8ClampedArray;
    expect(dark).not.toBe(light);
    expect(Array.from(dark.slice(0, 3))).toEqual([96, 165, 250]);
    // Same instance, no extra layers: a theme toggle replaces the images only.
    // (The route/gate pass re-runs on a theme toggle too and refits callers that
    // pass no `fitKey` — pre-existing behaviour, not an arrow-driven refit.)
    expect(recorder.instances).toHaveLength(1);
    expect(inst.layers.filter((l) => l.type === 'symbol')).toHaveLength(2);
  });
});
