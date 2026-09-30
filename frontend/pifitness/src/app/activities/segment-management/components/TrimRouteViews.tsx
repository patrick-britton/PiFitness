'use client';

import { useMemo } from 'react';
import { useViewportStore } from '@/stores/viewportStore';
import { TrimGates, gateFocusWindow, indexWindow, trimmedMeters } from '@/lib/segment-trim';
import { formatMiles } from '@/lib/date-format';
import ElevationProfile from './ElevationProfile';
import MapStyleSelector from './MapStyleSelector';
import SegmentRouteMap, { gatePoint } from './SegmentRouteMap';

/**
 * Route, gate, and elevation views for the trimmed range (009-003 T08/T22/T23,
 * FR-2/FR-4).
 *
 * The full activity route is fetched once per activity, and every view slices
 * from it client-side: the main map and profile show the gate-bounded range, and
 * the two gate views show the first/last 150 m of that range (the legacy focus
 * maps). Slicing locally keeps gate edits free of database round-trips on the
 * Pi; the creation call still sends the gate meters, so the stored segment is
 * trimmed server-side by the existing procedure.
 *
 * Screen order (T23, all three layout variants): basemap selector, whole-course
 * map, gate controls, gate maps, elevation profile. The gate inputs sit between
 * the two map rows so an edit is made directly above the views that answer it.
 */

/** Default zoom of the gate focus views. T24: +2 steps on the old 16 (MapLibre doubles the scale per step). */
const GATE_ZOOM = 18;

/** Reserved map heights per layout (px). Gate maps are the trimming surface, so they are the taller pair (T22). */
const MAIN_HEIGHT = { desktop: '320px', portrait: '240px', landscape: '200px' };
const GATE_HEIGHT = { desktop: '280px', portrait: '230px', landscape: '170px' };

interface TrimRouteViewsProps {
  /** Full activity route as `[lon, lat]` pairs. */
  coords: number[][];
  /** Elevations aligned 1:1 with `coords`. */
  elevations: number[];
  /** Current gates, in meters. */
  gates: TrimGates;
  /** Upper gate bound in meters (the activity's usable distance). */
  maxM: number;
  /** Selected basemap style id. */
  styleId: string;
  /** Mapbox token, or null when only token-free basemaps are available. */
  token: string | null;
  /** Called when the basemap style changes. */
  onStyleChange: (styleId: string) => void;
  /** Called when the start gate moves (whole meters). */
  onStartChange: (meters: number) => void;
  /** Called when the end gate moves (whole meters). */
  onEndChange: (meters: number) => void;
}

export default function TrimRouteViews({
  coords,
  elevations,
  gates,
  maxM,
  styleId,
  token,
  onStyleChange,
  onStartChange,
  onEndChange,
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

  const variant = isLandscape ? 'landscape' : isPortrait ? 'portrait' : 'desktop';
  const mainHeight = MAIN_HEIGHT[variant];
  const gateHeight = GATE_HEIGHT[variant];

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
        // T15: the full route array is the framing key — gate edits slice the
        // same array, so the map only reframes when a new activity route loads
        // (or the style rebuilds the map instance), never per gate step.
        fitKey={coords}
      />

      {/*
        T23: the gate controls sit between the whole-course map and the gate
        maps — the edit is made directly above the two views that answer it.
      */}
      <div className={`grid gap-3 ${isPortrait ? 'grid-cols-1' : 'grid-cols-2'}`}>
        <GateInput
          id="seg-gate-start"
          label="Select start gate"
          value={gates.startM}
          max={maxM}
          onChange={onStartChange}
        />
        <GateInput
          id="seg-gate-end"
          label="Select end gate"
          value={gates.endM}
          max={maxM}
          onChange={onEndChange}
        />
      </div>

      <p className="text-xs text-gray-500 dark:text-gray-400">
        Selected range: {gates.startM}–{gates.endM} m ({formatMiles(trimmedMeters(gates))} mi)
      </p>

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
            // T16: the start-gate view is centered on the gate itself and
            // follows it as the gate moves.
            centerOn={gatePoint(mainCoords, 'start')}
            centerZoom={GATE_ZOOM}
            fitKey={coords}
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
            // T16: the end-gate view is centered on the gate itself and
            // follows it as the gate moves.
            centerOn={gatePoint(mainCoords, 'end')}
            centerZoom={GATE_ZOOM}
            fitKey={coords}
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

/**
 * Whole-meter gate input bounded to the activity's distance (FR-4).
 * Moved here from SegmentTrimEditor in T23 so the controls render between the
 * course map and the gate maps.
 */
function GateInput({
  id,
  label,
  value,
  max,
  onChange,
  step = 1,
}: {
  id: string;
  label: string;
  value: number;
  max: number;
  /** Stepper increment; T19: 1 m per step. */
  step?: number;
  onChange: (value: number) => void;
}) {
  return (
    <div>
      <label htmlFor={id} className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
        {label}
      </label>
      <input
        id={id}
        type="number"
        min={0}
        max={max}
        step={step}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-1.5 text-sm tabular-nums focus:outline-none focus:ring-2 focus:ring-blue-500"
      />
    </div>
  );
}
