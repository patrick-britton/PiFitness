'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { API } from '@/lib/api-client';
import { ActivityRoute, ActivitySummary } from '@/lib/types/segment-management';
import { cumulativeMeters, gateBound, normalizeGates } from '@/lib/segment-trim';
import { defaultStyleForTheme } from '@/lib/map-styles';
import { isDarkTheme } from '@/lib/theme-version';
import { formatIsoDate, formatMiles } from '@/lib/date-format';
import TrimRouteViews from './TrimRouteViews';

/**
 * Create Segment mode editor (009-003 T08, FR-2/FR-4/FR-5/FR-6).
 *
 * Shows the chosen activity's meta, the name field, the route/elevation views,
 * and the creation controls. The full route is fetched
 * ONCE per activity (`GET /api/segments/{id}/route`, no trim params) and the
 * gates slice it client-side, so moving a gate costs no database work on the
 * Pi; the gate meters are sent with the creation call and the stored segment is
 * trimmed server-side by the existing procedure.
 *
 * T23: the two trim-gate inputs and the selected-range line are rendered by
 * TrimRouteViews between the course map and the gate maps; the gate state stays
 * here because the creation call needs it.
 *
 * Creation offers course or segment (FR-5) and confirms the new id; the two
 * controls afterwards start a new segment from the same activity or return to
 * the activity list (FR-6).
 */

interface SegmentTrimEditorProps {
  /** The source activity chosen from the list. */
  activity: ActivitySummary;
  /** Return to the source-activity list (FR-6). */
  onChooseNewActivity: () => void;
}

export default function SegmentTrimEditor({ activity, onChooseNewActivity }: SegmentTrimEditorProps) {
  const [route, setRoute] = useState<ActivityRoute | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [mapboxToken, setMapboxToken] = useState<string | null>(null);
  const [mapStyle, setMapStyle] = useState<string>('');
  const [startM, setStartM] = useState(0);
  const [endM, setEndM] = useState(0);
  const [name, setName] = useState('');
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);
  const [created, setCreated] = useState<{ segment_id: number; is_course: boolean; name: string } | null>(null);

  const loadRoute = useCallback(async () => {
    setLoading(true);
    try {
      const res = await API.segments.getRoute(activity.activity_id);
      const data = res.data;
      setRoute(data ?? null);
      const pathLength = data?.path_coords?.length ? cumulativeMeters(data.path_coords).slice(-1)[0] : 0;
      setStartM(0);
      setEndM(gateBound(data?.distance_m, pathLength));
      setLoadError(null);
    } catch {
      setRoute(null);
      setLoadError('Could not load the activity route.');
    } finally {
      setLoading(false);
    }
  }, [activity.activity_id]);

  useEffect(() => {
    loadRoute();
  }, [loadRoute]);

  // Mapbox token availability gates the satellite basemaps (FR-3). The selector
  // itself takes the catalogue and gating flags from the backend.
  useEffect(() => {
    let active = true;
    API.config
      .getMapConfig()
      .then((c) => {
        if (!active) return;
        const token = c.mapboxToken || null;
        setMapboxToken(token);
        setMapStyle((prev) => (prev ? prev : defaultStyleForTheme(isDarkTheme(), token)));
      })
      .catch(() => {
        if (!active) return;
        setMapboxToken(null);
        setMapStyle((prev) => (prev ? prev : defaultStyleForTheme(isDarkTheme(), null)));
      });
    return () => {
      active = false;
    };
  }, []);

  const maxM = useMemo(() => {
    if (!route) return 0;
    const fromPath = route.path_coords?.length ? cumulativeMeters(route.path_coords).slice(-1)[0] : 0;
    return Math.max(1, gateBound(route.distance_m, fromPath));
  }, [route]);

  const gates = useMemo(() => normalizeGates(startM, endM, maxM), [startM, endM, maxM]);

  const submit = async (isCourse: boolean) => {
    const trimmed = name.trim();
    if (!trimmed) {
      setCreateError('Enter a name for the new course/segment.');
      return;
    }
    setCreating(true);
    setCreateError(null);
    try {
      const res = await API.segments.createSegment({
        activity_id: activity.activity_id,
        start_m: gates.startM,
        end_m: gates.endM,
        name: trimmed,
        is_course: isCourse,
        map_style: mapStyle,
      });
      setCreated({ segment_id: res.segment_id, is_course: isCourse, name: trimmed });
    } catch {
      setCreateError('Creation failed. Check the name and the trim range.');
    } finally {
      setCreating(false);
    }
  };

  const resetForSameActivity = () => {
    setName('');
    setCreated(null);
    setCreateError(null);
    setStartM(0);
    setEndM(maxM);
  };

  const hasRoute = Boolean(route && route.path_coords?.length);

  return (
    <div className="space-y-3">
      {/* Activity meta (AC-2) */}
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-3">
        <p className="text-sm text-gray-700 dark:text-gray-300">
          ID# <span className="font-semibold tabular-nums">{activity.activity_id}</span> @{' '}
          {formatIsoDate(activity.start_time_utc)} | {formatMiles(activity.distance_m)} mi
        </p>
      </div>

      <div className="max-w-md">
        <label htmlFor="seg-name" className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
          Segment Name
        </label>
        <input
          id="seg-name"
          type="text"
          value={name}
          maxLength={200}
          onChange={(e) => setName(e.target.value)}
          className="w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>

      {loading && (
        <p role="status" className="text-sm text-gray-500 dark:text-gray-400">
          Loading route…
        </p>
      )}

      {!loading && loadError && (
        <div className="rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4">
          <p role="alert" className="text-sm text-red-600 dark:text-red-400 mb-2">
            {loadError}
          </p>
          <button
            type="button"
            onClick={loadRoute}
            className="px-3 py-1.5 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            Retry
          </button>
        </div>
      )}

      {!loading && !loadError && !hasRoute && (
        <p role="status" className="text-sm text-gray-500 dark:text-gray-400">
          This activity has no GPS route data to create a course or segment from.
        </p>
      )}

      {!loading && !loadError && hasRoute && (
        <>
          {/*
            T23: the gate controls (start/end inputs + selected-range line) now
            render inside TrimRouteViews, between the course map and the gate
            maps. The create/new-activity controls below stay here.
          */}
          {route && mapStyle && (
            <TrimRouteViews
              coords={route.path_coords}
              elevations={route.elevations}
              gates={gates}
              maxM={maxM}
              styleId={mapStyle}
              token={mapboxToken}
              onStyleChange={setMapStyle}
              onStartChange={setStartM}
              onEndChange={setEndM}
            />
          )}

          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={() => submit(true)}
              disabled={creating}
              className="px-3 py-2 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {creating ? 'Creating…' : 'Create Course'}
            </button>
            <button
              type="button"
              onClick={() => submit(false)}
              disabled={creating}
              className="px-3 py-2 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {creating ? 'Creating…' : 'Create Segment'}
            </button>
            <button
              type="button"
              onClick={onChooseNewActivity}
              className="px-3 py-2 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              Choose new activity
            </button>
            <button
              type="button"
              onClick={resetForSameActivity}
              className="px-3 py-2 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              New segment from same activity
            </button>
          </div>

          {createError && (
            <p role="alert" className="text-sm text-red-600 dark:text-red-400">
              {createError}
            </p>
          )}

          {created && (
            <p
              role="status"
              className="text-sm text-green-700 dark:text-green-400 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-3"
            >
              {created.is_course ? 'Course' : 'Segment'} “{created.name}” created as ID#{' '}
              <span className="font-semibold tabular-nums">{created.segment_id}</span>
            </p>
          )}
        </>
      )}
    </div>
  );
}

