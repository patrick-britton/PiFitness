'use client';

import { useCallback, useEffect, useState } from 'react';
import { API } from '@/lib/api-client';
import { ActivityRoute, CourseInfo, CoursesResponse } from '@/lib/types/segment-management';
import { defaultStyleForTheme } from '@/lib/map-styles';
import { isDarkTheme } from '@/lib/theme-version';
import { useViewportStore } from '@/stores/viewportStore';
import { formatIsoDate } from '@/lib/date-format';
import CourseCard from './CourseCard';
import CourseFilters from './CourseFilters';
import CoursePager from './CoursePager';
import CourseRename from './CourseRename';
import ElevationProfile from './ElevationProfile';
import MapStyleSelector from './MapStyleSelector';
import ResetMatchTables from './ResetMatchTables';
import SegmentRouteMap from './SegmentRouteMap';

/**
 * Legacy parity (`backend_functions/viz_factory/run_list.py::render_course_list`):
 * name search, length min/max, total/new counts, wrap-around Prior/Next
 * paging through ONE course at a time, that course's map + elevation, and
 * rename-in-place. Deltas: paging is server-side (`page_size: 1`, one fetch
 * per course) instead of the legacy full-view read; course geometry comes
 * from the T02c reference pointers (`getRoute(activity_reference_id,
 * reference window)` — the legacy `segment_path` WKB is never shipped to
 * the client); the map/elevation are the module's MapLibre/Chart.js
 * components, not Plotly.
 *
 * One map instance at a time (the current course's): a second MapLibre
 * instance per course row is pure cost on a Pi-hosted phone view.
 */

const PAGE_SIZE = 1;

export default function CourseReviewMode() {
  const [nameFilter, setNameFilter] = useState('');
  const [minDistance, setMinDistance] = useState('');
  const [maxDistance, setMaxDistance] = useState('');
  const [page, setPage] = useState(1);

  const [courses, setCourses] = useState<CourseInfo[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [totalPages, setTotalPages] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [route, setRoute] = useState<ActivityRoute | null>(null);
  const [loadingRoute, setLoadingRoute] = useState(false);
  const [routeError, setRouteError] = useState<string | null>(null);
  const [mapboxToken, setMapboxToken] = useState<string | null>(null);
  const [mapStyle, setMapStyle] = useState('');

  const { layoutVariant } = useViewportStore();
  const isPortrait = layoutVariant === 'portrait';
  const isLandscape = layoutVariant === 'landscape';

  const course = courses[0] ?? null;

  const load = useCallback(
    async (pageNum: number) => {
      setLoading(true);
      try {
        const query: {
          name?: string;
          min_distance?: number;
          max_distance?: number;
          page: number;
          page_size: number;
        } = { page: pageNum, page_size: PAGE_SIZE };
        if (nameFilter) query.name = nameFilter;
        const min = minDistance === '' ? NaN : Number(minDistance);
        const max = maxDistance === '' ? NaN : Number(maxDistance);
        if (!Number.isNaN(min)) query.min_distance = min;
        if (!Number.isNaN(max)) query.max_distance = max;
        const res: CoursesResponse = await API.segments.getCourses(query);
        setCourses(res.data ?? []);
        setTotalCount(res.total_count ?? 0);
        setTotalPages(res.total_pages ?? 0);
        setPage(res.page ?? pageNum);
        setError(null);
      } catch {
        setError('Could not load courses.');
        setCourses([]);
        setTotalCount(0);
        setTotalPages(0);
      } finally {
        setLoading(false);
      }
    },
    [nameFilter, minDistance, maxDistance],
  );

  // Filter change resets to page 1 (legacy reset_idx semantics).
  useEffect(() => {
    load(1);
  }, [load]);

  // Mapbox token availability gates the satellite basemaps (FR-3); the style
  // catalogue and its gating flags come from the backend (T04).
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

  // Reference-geometry pointers (T02c/OQ-4). Absent on rows that predate the
  // field: the map is then hidden rather than fetched with a wrong id.
  const refActivityId = course?.activity_reference_id ?? null;
  const refStartM = course?.reference_start_point ?? null;
  const refEndM = course?.reference_end_point ?? null;
  const hasReference = refActivityId != null && refEndM != null;

  const loadRoute = useCallback(async () => {
    if (refActivityId == null || refEndM == null) {
      setRoute(null);
      setRouteError(null);
      setLoadingRoute(false);
      return;
    }
    setLoadingRoute(true);
    try {
      const res = await API.segments.getRoute(refActivityId, {
        start_m: refStartM ?? 0,
        end_m: refEndM,
      });
      setRoute(res.data ?? null);
      setRouteError(null);
    } catch {
      setRoute(null);
      setRouteError('Could not load the course route.');
    } finally {
      setLoadingRoute(false);
    }
  }, [refActivityId, refStartM, refEndM]);

  useEffect(() => {
    setRoute(null);
    setRouteError(null);
    if (course) loadRoute();
    else setLoadingRoute(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [course?.course_id]);

  const goPrior = useCallback(() => {
    if (totalPages <= 0) return;
    load(page <= 1 ? totalPages : page - 1);
  }, [load, page, totalPages]);

  const goNext = useCallback(() => {
    if (totalPages <= 0) return;
    load(page >= totalPages ? 1 : page + 1);
  }, [load, page, totalPages]);

  const onRenamed = useCallback(
    (newName: string) => {
      if (!course) return;
      setCourses((prev) =>
        prev.map((c) => (c.course_id === course.course_id ? { ...c, course_name: newName } : c)),
      );
    },
    [course],
  );

  const clearFilters = () => {
    setNameFilter('');
    setMinDistance('');
    setMaxDistance('');
  };

  const mapHeight = isLandscape ? '220px' : isPortrait ? '240px' : '320px';
  const profileHeight = isLandscape ? '140px' : '200px';
  const hasRoute = Boolean(route && route.path_coords?.length);
  const newOnPage = courses.filter((c) => c.is_new).length;

  return (
    <div className="space-y-3">
      <CourseFilters
        nameFilter={nameFilter}
        minDistance={minDistance}
        maxDistance={maxDistance}
        onNameChange={setNameFilter}
        onMinChange={setMinDistance}
        onMaxChange={setMaxDistance}
        onClear={clearFilters}
      />

      {loading && (
        <p role="status" className="text-sm text-gray-500 dark:text-gray-400 px-1">
          Loading courses…
        </p>
      )}

      {!loading && error && (
        <div className="rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4">
          <p role="alert" className="text-sm text-red-600 dark:text-red-400 mb-2">
            {error}
          </p>
          <button
            type="button"
            onClick={() => load(1)}
            className="px-3 py-1.5 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            Retry
          </button>
        </div>
      )}

      {!loading && !error && totalCount === 0 && (
        <p role="status" className="text-sm text-gray-500 dark:text-gray-400 px-1">
          No courses match your filters.
        </p>
      )}

      {!loading && !error && totalCount > 0 && (
        <>
          <p role="status" className="text-sm text-gray-700 dark:text-gray-300 px-1">
            <span className="font-semibold tabular-nums">{totalCount}</span> total course
            {totalCount === 1 ? '' : 's'}
            {newOnPage > 0 && <> · {newOnPage} new on this page</>}
          </p>
          <CoursePager page={page} totalPages={totalPages} onPrior={goPrior} onNext={goNext} />
        </>
      )}

      {!loading && !error && course && (
        <div className="space-y-3">
          <CourseCard course={course} />

          {loadingRoute && (
            <p role="status" className="text-sm text-gray-500 dark:text-gray-400">
              Loading the course route…
            </p>
          )}

          {!loadingRoute && !hasReference && (
            <p role="status" className="text-sm text-gray-500 dark:text-gray-400">
              This course records no reference activity and window, so its route cannot be drawn.
            </p>
          )}

          {!loadingRoute && hasReference && routeError && (
            <div className="rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4">
              <p role="alert" className="text-sm text-red-600 dark:text-red-400 mb-2">
                {routeError}
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

          {!loadingRoute && hasReference && !routeError && hasRoute && route && mapStyle && (
            <div className="space-y-3">
              <MapStyleSelector
                id="course-map-style"
                value={mapStyle}
                onChange={setMapStyle}
                className="max-w-xs"
              />
              <SegmentRouteMap
                coords={route.path_coords}
                styleId={mapStyle}
                token={mapboxToken}
                height={mapHeight}
                ariaLabel={`Route of course ${course.course_name}`}
              />
              <ElevationProfile
                coords={route.path_coords}
                elevations={route.elevations}
                height={profileHeight}
                ariaLabel={`Elevation profile of ${course.course_name}`}
              />
            </div>
          )}

          {!loadingRoute && hasReference && !routeError && !hasRoute && (
            <p role="status" className="text-sm text-gray-500 dark:text-gray-400">
              No GPS points fall inside this course&apos;s reference window, so there is nothing to
              draw.
            </p>
          )}

          <CourseRename course={course} onRenamed={onRenamed} />
        </div>
      )}

      <ResetMatchTables onReset={() => load(1)} />
    </div>
  );
}
