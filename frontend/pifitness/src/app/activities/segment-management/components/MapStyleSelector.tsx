'use client';

import { useEffect, useMemo, useState } from 'react';
import { API } from '@/lib/api-client';
import { MapStyle } from '@/lib/types/segment-management';

/**
 * Basemap style selector for the Segment Management maps (009-003 T08, FR-3).
 *
 * The catalogue is the backend's (`GET /api/segments/map-styles`, T04): labels
 * and per-style token gating come from there, so the client never invents a
 * basemap. Styles flagged `requires_satellite_token` are offered only when the
 * response reports `satellite_available`.
 *
 * Self-fetching and controlled (`value`/`onChange`), so both modes can embed the
 * same selector and share the merge logic with the theme default
 * (`mergeMapStyles`).
 */

interface MapStyleSelectorProps {
  /** Selected style id. */
  value: string;
  /** Called with a style id when the user picks one, or when `value` is unavailable. */
  onChange: (styleId: string) => void;
  /** DOM id for the label association. */
  id: string;
  /** Extra classes for the wrapping div. */
  className?: string;
}

/**
 * Options a response allows: token-gated styles are dropped when no token is
 * configured. Also re-points a now-unavailable selection at the first option,
 * so a token-less page cannot keep a Mapbox style selected.
 */
export function mergeMapStyles(
  value: string,
  catalogue: MapStyle[],
  satelliteAvailable: boolean,
): { options: MapStyle[]; value: string } {
  const options = catalogue.filter((s) => satelliteAvailable || !s.requires_satellite_token);
  if (options.length === 0) return { options, value };
  if (options.some((s) => s.id === value)) return { options, value };
  return { options, value: options[0].id };
}

export default function MapStyleSelector({ value, onChange, id, className = '' }: MapStyleSelectorProps) {
  const [catalogue, setCatalogue] = useState<MapStyle[] | null>(null);
  const [satelliteAvailable, setSatelliteAvailable] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    API.segments
      .getMapStyles()
      .then((res) => {
        if (!active) return;
        setCatalogue(res.data ?? []);
        setSatelliteAvailable(Boolean(res.satellite_available));
        setError(null);
      })
      .catch(() => {
        if (!active) return;
        setCatalogue([]);
        setError('Basemap list unavailable');
      });
    return () => {
      active = false;
    };
  }, []);

  const merged = useMemo(
    () => (catalogue ? mergeMapStyles(value, catalogue, satelliteAvailable) : null),
    [catalogue, value, satelliteAvailable],
  );

  // Keep the parent's style in sync with what the catalogue permits (e.g. drop
  // a Mapbox style once the token is known to be absent).
  useEffect(() => {
    if (merged && merged.value !== value) onChange(merged.value);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [merged?.value, value]);

  const loading = catalogue === null;

  return (
    <div className={className}>
      <label
        htmlFor={id}
        className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1"
      >
        Map Style
      </label>
      <select
        id={id}
        value={merged?.value ?? value}
        disabled={loading || (merged?.options.length ?? 0) === 0}
        onChange={(e) => onChange(e.target.value)}
        aria-describedby={error ? `${id}-error` : undefined}
        className="w-full min-h-[44px] rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-60"
      >
        {loading && <option value={value}>Loading basemaps…</option>}
        {!loading && (merged?.options.length ?? 0) === 0 && <option value={value}>No basemaps</option>}
        {merged?.options.map((s) => (
          <option key={s.id} value={s.id}>
            {s.name}
          </option>
        ))}
      </select>
      {error && (
        <p id={`${id}-error`} role="status" className="mt-1 text-xs text-gray-500 dark:text-gray-400">
          {error}
        </p>
      )}
    </div>
  );
}