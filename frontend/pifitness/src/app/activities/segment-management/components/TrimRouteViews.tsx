'use client';

import { useMemo } from 'react';
import { useViewportStore } from '@/stores/viewportStore';
import { TrimGates, gateFocusWindow, indexWindow } from '@/lib/segment-trim';
import ElevationProfile from './ElevationProfile';
import MapStyleSelector from './MapStyleSelector';
import SegmentRouteMap, { gatePoint } from './SegmentRouteMap';

/**
 * Route, gate, and elevation views for the trimmed range (009-003 T08, FR-2/FR-4).
 *
 * The full activity route is fetched once per activity, and every view slices
 * from it client-side: the main map and profile show the gate-bounded range, and
 * the two gate views show the first/last 150 m of that range (the legacy focus
 * maps). Slicing locally keeps gate edits free of database round-trips on the
 * Pi; the creation call still sends the gate meters, so the stored segment is
 * trimmed server-side by the existing procedure.
 */

interface TrimRouteViewsProps {
  /** Full activity route as `[lon, lat]` pairs. */
  coords: number[][];
  /** Elevations aligned 1:1 with `coords`. */
  elevations: number[];
  /** Current gates, in meters. */
  gates: TrimGates;
  /** Selected basemap style id. */
  styleId: string;
  /** Mapbox token, or null when only token-free basemaps are available. */
  token: string | null;
  /** Called when the basemap style changes. */
  onStyleChange: (styleId: string) => void;
}

export default function TrimRouteViews({
  coords,
  elevations,
  gates,
  styleId,
  token,
  onStyleChange,
}: TrimRouteViewsProps) {
  const { layoutVariant } = useViewportStore();
  const isPortrait = layoutVariant === 'portrait';
  const isLandscape = layoutVariant === 'landscape';

  const mainWindow = useMemo(() => indexWindow(coords, gates.startM, gates.endM), [coords, gates]);
  const mainCoords = useMemo(
    () => (mainWindow ? coords.slice(mainWindow[0], mainWindow[1] + 1) : coords),
    [coords, mainWindow],
  );
  const mainElev = useMemo(
    () => (mainWindow ? elevations.slice(mainWindow[0], mainWindow[1] + 1) : elevations),
    [elevations, mainWindow],
  );

  const startWindow = useMemo(() => {
    const [from, to] = gateFocusWindow(gates, 'start');
    return indexWindow(coords, from, to);
  }, [coords, gates]);
  const endWindow = useMemo(() => {
    const [from, to] = gateFocusWindow(gates, 'end');
    return indexWindow(coords, from, to);
  }, [coords, gates]);

  const mainHeight = isLandscape ? '200px' : isPortrait ? '240px' : '320px';
  const gateHeight = isLandscape ? '130px' : isPortrait ? '150px' : '180px';

  return (
    <div className="space-y-3">
      <MapStyleSelector
        id="seg-map-style"
        value={styleId}
        onChange={onStyleChange}
        className="max-w-xs"
      />

      <SegmentRouteMap
        coords={mainCoords}
        styleId={styleId}
        token={token}
        height={mainHeight}
        ariaLabel={`Trimmed route from ${gates.startM} to ${gates.endM} meters`}
        gates={{ start: gatePoint(mainCoords, 'start'), end: gatePoint(mainCoords, 'end') }}
      />

      <div className={`grid gap-3 ${isPortrait ? 'grid-cols-1' : 'grid-cols-2'}`}>
        <div>
          <p className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
            Start gate · first {Math.min(150, Math.max(0, gates.endM - gates.startM))} m
          </p>
          <SegmentRouteMap
            coords={startWindow ? coords.slice(startWindow[0], startWindow[1] + 1) : coords}
            styleId={styleId}
            token={token}
            height={gateHeight}
            ariaLabel={`Route near the start gate at ${gates.startM} meters`}
            gates={{ start: gatePoint(mainCoords, 'start') }}
          />
        </div>
        <div>
          <p className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
            End gate · last {Math.min(150, Math.max(0, gates.endM - gates.startM))} m
          </p>
          <SegmentRouteMap
            coords={endWindow ? coords.slice(endWindow[0], endWindow[1] + 1) : coords}
            styleId={styleId}
            token={token}
            height={gateHeight}
            ariaLabel={`Route near the end gate at ${gates.endM} meters`}
            gates={{ end: gatePoint(mainCoords, 'end') }}
          />
        </div>
      </div>

      <ElevationProfile
        coords={mainCoords}
        elevations={mainElev}
        height={isLandscape ? '140px' : '200px'}
        ariaLabel={`Elevation profile between ${gates.startM} and ${gates.endM} meters`}
      />
    </div>
  );
}