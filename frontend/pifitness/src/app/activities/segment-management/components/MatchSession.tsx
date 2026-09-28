'use client';

import { useCallback, useEffect, useState } from 'react';
import { API } from '@/lib/api-client';
import { ActivityRoute, CandidateEffort, SegmentListRow } from '@/lib/types/segment-management';
import { defaultStyleForTheme } from '@/lib/map-styles';
import { isDarkTheme } from '@/lib/theme-version';
import { useViewportStore } from '@/stores/viewportStore';
import CandidateTable from './CandidateTable';
import ConfirmDialog from './ConfirmDialog';
import ElevationProfile from './ElevationProfile';
import MapStyleSelector from './MapStyleSelector';
import SegmentRouteMap from './SegmentRouteMap';

/**
 * Match Activities session for one segment/course (009-003 T09, FR-8..FR-13).
 *
 * Geometry comes from the segment's reference activity and reference window
 * (T02b/OQ-3): `GET /api/segments/{id}/route` is keyed by activity_id, so the
 * segment's own GPS path is `getRoute(activity_reference_id, reference window)`
 * — the same source the legacy Streamlit map used. The segment and the selected
 * candidate effort share ONE map (candidate drawn dashed) because a second
 * MapLibre instance per screen is pure cost on a Pi-hosted phone view.
 *
 * The matching pipeline is synchronous (OQ-1), so Find matches blocks behind an
 * explicit spinner. Confirm/reject take the refreshed confirmed count from the
 * response and drop the acted row locally — no extra round trip; only bulk
 * confirm and the explicit Refresh re-read the candidate set, because a mass
 * approval pass rewrites it. Rename is FR-16 and lives in Course Review (T10).
 */

interface MatchSessionProps {
  /** Selected segment/course row from the picker (carries the reference-geometry pointers). */
  segment: SegmentListRow;
  /** Return to the picker (FR-13 "change segment"). */
  onChooseDifferentSegment: () => void;
}

export default function MatchSession({ segment, onChooseDifferentSegment }: MatchSessionProps) {
  const [route, setRoute] = useState<ActivityRoute | null>(null);
  const [loadingRoute, setLoadingRoute] = useState(true);
  const [routeError, setRouteError] = useState<string | null>(null);
  const [mapboxToken, setMapboxToken] = useState<string | null>(null);
  const [mapStyle, setMapStyle] = useState('');

  const [candidates, setCandidates] = useState<CandidateEffort[]>([]);
  const [loadingCandidates, setLoadingCandidates] = useState(true);
  const [candidatesError, setCandidatesError] = useState<string | null>(null);
  const [existingMatchCount, setExistingMatchCount] = useState(0);

  const [findLoading, setFindLoading] = useState(false);
  const [scoringKind, setScoringKind] = useState<'hausdorff' | 'frechet' | null>(null);
  const [bulkLoading, setBulkLoading] = useState(false);

  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [candidateRoute, setCandidateRoute] = useState<ActivityRoute | null>(null);
  const [candidateRouteError, setCandidateRouteError] = useState<string | null>(null);

  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [deleteLoading, setDeleteLoading] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);

  const { layoutVariant } = useViewportStore();
  const isPortrait = layoutVariant === 'portrait';
  const isLandscape = layoutVariant === 'landscape';

  // Reference-geometry pointers (T02b). Absent on rows that predate the field or
  // come from the 009-002 leaderboard list: the map is then hidden rather than
  // fetched with a wrong id.
  const refActivityId = segment.activity_reference_id ?? null;
  const refStartM = segment.reference_start_point ?? null;
  const refEndM = segment.reference_end_point ?? null;
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
      setRouteError('Could not load the segment route.');
    } finally {
      setLoadingRoute(false);
    }
  }, [refActivityId, refStartM, refEndM]);

  useEffect(() => {
    loadRoute();
  }, [loadRoute]);

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

  const loadCandidates = useCallback(async () => {
    setLoadingCandidates(true);
    try {
      const res = await API.segments.getCandidates(segment.segment_id);
      setCandidates(res.data ?? []);
      setExistingMatchCount(res.existing_match_count);
      setCandidatesError(null);
    } catch {
      setCandidates([]);
      setCandidatesError('Could not load candidate efforts.');
    } finally {
      setLoadingCandidates(false);
    }
  }, [segment.segment_id]);

  useEffect(() => {
    loadCandidates();
  }, [loadCandidates]);

  // A different segment means a different candidate set: never keep a stale
  // selection, message, or active candidate route.
  useEffect(() => {
    setSelectedId(null);
    setCandidateRoute(null);
    setCandidateRouteError(null);
    setStatusMessage(null);
    setActionError(null);
  }, [segment.segment_id]);

  const findMatches = useCallback(async () => {
    setFindLoading(true);
    setActionError(null);
    setStatusMessage(null);
    try {
      const res = await API.segments.findMatches(segment.segment_id);
      // The auto-approval pass can move candidates into confirmed matches, so
      // the candidate set and the existing-match count are both re-read here.
      await loadCandidates();
      setStatusMessage(res.message || 'Match search completed.');
    } catch {
      setActionError('Match search failed.');
    } finally {
      setFindLoading(false);
    }
  }, [segment.segment_id, loadCandidates]);

  const selectCandidate = useCallback(async (candidate: CandidateEffort) => {
    setSelectedId(candidate.activity_id);
    setCandidateRoute(null);
    setCandidateRouteError(null);
    try {
      const res = await API.segments.getRoute(candidate.activity_id, {
        start_m: candidate.best_start_dist,
        end_m: candidate.best_end_dist,
      });
      setCandidateRoute(res.data ?? null);
    } catch {
      setCandidateRouteError(`Could not load the route for candidate ${candidate.activity_id}.`);
    }
  }, []);
const finalize = useCallback(
    async (candidate: CandidateEffort, approved: boolean) => {
      setActionError(null);
      setStatusMessage(null);
      try {
        const res = approved
          ? await API.segments.confirmMatch(segment.segment_id, {
              activity_id: candidate.activity_id,
            })
          : await API.segments.rejectMatch(segment.segment_id, {
              activity_id: candidate.activity_id,
            });
        // The response carries the refreshed confirmed count and the acted row
        // left the pending set: update locally rather than paying for a re-read.
        setCandidates((prev) => prev.filter((c) => c.activity_id !== candidate.activity_id));
        setExistingMatchCount(res.segment?.matched_count ?? 0);
        if (selectedId === candidate.activity_id) {
          setSelectedId(null);
          setCandidateRoute(null);
        }
        setStatusMessage(res.message || (approved ? 'Match confirmed.' : 'Match rejected.'));
      } catch {
        setActionError(
          approved
            ? `Failed to confirm candidate ${candidate.activity_id}.`
            : `Failed to reject candidate ${candidate.activity_id}.`,
        );
      }
    },
    [segment.segment_id, selectedId],
  );

  const bulkConfirm = useCallback(async () => {
    setBulkLoading(true);
    setActionError(null);
    setStatusMessage(null);
    try {
      await API.segments.bulkConfirm(segment.segment_id, { confirm_all: true });
      setSelectedId(null);
      setCandidateRoute(null);
      await loadCandidates();
      setStatusMessage('Bulk confirm completed.');
    } catch {
      setActionError('Bulk confirm failed.');
    } finally {
      setBulkLoading(false);
    }
  }, [segment.segment_id, loadCandidates]);

  const runScoring = useCallback(
    async (kind: 'hausdorff' | 'frechet') => {
      setScoringKind(kind);
      setActionError(null);
      setStatusMessage(null);
      try {
        const res =
          kind === 'hausdorff'
            ? await API.segments.hausdorffScoring(segment.segment_id, { run_scoring: true })
            : await API.segments.frechetScoring(segment.segment_id, { run_scoring: true });
        // The scoring response already carries the rescored candidate set.
        setCandidates(res.candidates ?? []);
        setStatusMessage(res.message || `${kind} scoring completed.`);
      } catch {
        setActionError(
          kind === 'hausdorff' ? 'Hausdorff scoring failed.' : 'Fréchet scoring failed.',
        );
      } finally {
        setScoringKind(null);
      }
    },
    [segment.segment_id],
  );

  const deleteSegment = useCallback(async () => {
    setDeleteLoading(true);
    setDeleteError(null);
    try {
      await API.segments.deleteSegment(segment.segment_id);
      setDeleteOpen(false);
      onChooseDifferentSegment();
    } catch {
      setDeleteError('Failed to delete the segment.');
    } finally {
      setDeleteLoading(false);
    }
  }, [segment.segment_id, onChooseDifferentSegment]);

  const busy = findLoading || bulkLoading || scoringKind !== null;
  const mapHeight = isLandscape ? '220px' : isPortrait ? '240px' : '320px';
  const profileHeight = isLandscape ? '140px' : '200px';
  const hasRoute = Boolean(route && route.path_coords?.length);
  const candidateOverlay = candidateRoute?.path_coords?.length
    ? candidateRoute.path_coords
    : undefined;

  return (
    <div className="space-y-3">
      {/* Segment header (FR-8) + session controls (FR-13) */}
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-3">
        <div className="flex flex-wrap items-start justify-between gap-2">
          <div>
            <p className="text-sm font-semibold text-gray-900 dark:text-white">
              {segment.segment_name}
            </p>
            <p className="text-xs text-gray-600 dark:text-gray-400">
              {segment.is_course ? 'Course' : 'Segment'} ID#{' '}
              <span className="tabular-nums">{segment.segment_id}</span> ·{' '}
              {segment.distance_mi.toFixed(2)} mi
              {segment.elevation_gain != null ? ` · ${segment.elevation_gain} m elev` : ''}
              {segment.activity_type_name ? ` · ${segment.activity_type_name}` : ''}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={onChooseDifferentSegment}
              className="px-3 py-1.5 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              Change segment
            </button>
            <button
              type="button"
              onClick={() => {
                setDeleteError(null);
                setDeleteOpen(true);
              }}
              className="px-3 py-1.5 text-sm font-medium rounded-md border border-red-300 dark:border-red-700 text-red-700 dark:text-red-300 hover:bg-red-50 dark:hover:bg-red-900/20 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              Delete segment
            </button>
          </div>
        </div>
      </div>

      {loadingRoute && (
        <p role="status" className="text-sm text-gray-500 dark:text-gray-400">
          Loading the segment route…
        </p>
      )}

      {!loadingRoute && !hasReference && (
        <p role="status" className="text-sm text-gray-500 dark:text-gray-400">
          This segment records no reference activity and window, so its route cannot be drawn.
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
            id="match-map-style"
            value={mapStyle}
            onChange={setMapStyle}
            className="max-w-xs"
          />
          <SegmentRouteMap
            coords={route.path_coords}
            overlayCoords={candidateOverlay}
            styleId={mapStyle}
            token={mapboxToken}
            height={mapHeight}
            ariaLabel={`Route of ${segment.is_course ? 'course' : 'segment'} ${segment.segment_name}${
              candidateOverlay ? ', with the selected candidate effort drawn dashed' : ''
            }`}
          />
          {candidateOverlay && (
            <p className="text-xs text-gray-500 dark:text-gray-400">
              Map legend: solid line = the {segment.is_course ? 'course' : 'segment'} route; dashed
              line = candidate {selectedId} effort.
            </p>
          )}
          {candidateRouteError && (
            <p role="alert" className="text-sm text-red-600 dark:text-red-400">
              {candidateRouteError}
            </p>
          )}
          <ElevationProfile
            coords={route.path_coords}
            elevations={route.elevations}
            height={profileHeight}
            ariaLabel={`Elevation profile of ${segment.segment_name}`}
          />
        </div>
      )}

      {!loadingRoute && hasReference && !routeError && !hasRoute && (
        <p role="status" className="text-sm text-gray-500 dark:text-gray-400">
          No GPS points fall inside this segment&apos;s reference window, so there is nothing to draw.
        </p>
      )}
      {/* Matching controls (FR-9, FR-11, FR-12) */}
      <div className="flex flex-wrap items-center gap-2">
        <p className="rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 text-sm text-gray-700 dark:text-gray-300">
          Existing matches:{' '}
          <span className="font-semibold tabular-nums">{existingMatchCount}</span>
        </p>
        <p className="rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 text-sm text-gray-700 dark:text-gray-300">
          Potential matches:{' '}
          <span className="font-semibold tabular-nums">{candidates.length}</span>
        </p>
        <button
          type="button"
          onClick={findMatches}
          disabled={busy || loadingCandidates}
          className="px-3 py-2 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {findLoading ? 'Finding matches…' : 'Find matches'}
        </button>
        <button
          type="button"
          onClick={bulkConfirm}
          disabled={busy || loadingCandidates || candidates.length === 0}
          className="px-3 py-2 text-sm font-medium rounded-md bg-green-700 text-white hover:bg-green-800 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {bulkLoading ? 'Confirming all…' : 'Confirm all remaining'}
        </button>
        <button
          type="button"
          onClick={() => runScoring('hausdorff')}
          disabled={busy || loadingCandidates}
          className="px-3 py-2 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {scoringKind === 'hausdorff' ? 'Running Hausdorff…' : 'Run Hausdorff scoring'}
        </button>
        <button
          type="button"
          onClick={() => runScoring('frechet')}
          disabled={busy || loadingCandidates}
          className="px-3 py-2 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {scoringKind === 'frechet' ? 'Running Fréchet…' : 'Run Fréchet scoring'}
        </button>
        <button
          type="button"
          onClick={loadCandidates}
          disabled={busy || loadingCandidates}
          className="px-3 py-2 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {loadingCandidates ? 'Refreshing…' : 'Refresh candidates'}
        </button>
      </div>

      {findLoading && (
        <p role="status" className="text-sm text-gray-600 dark:text-gray-400">
          Running the matching pipeline (activity matching, pair generation, polygon scoring,
          auto-approval)… this can take a while.
        </p>
      )}

      {statusMessage && (
        <p
          role="status"
          className="text-sm text-green-700 dark:text-green-400 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md px-3 py-2"
        >
          {statusMessage}
        </p>
      )}

      {(actionError || candidatesError) && (
        <div className="rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 px-3 py-2">
          <p role="alert" className="text-sm text-red-700 dark:text-red-300">
            {actionError || candidatesError}
          </p>
          {candidatesError && (
            <button
              type="button"
              onClick={loadCandidates}
              className="mt-2 px-3 py-1.5 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              Retry
            </button>
          )}
        </div>
      )}

      {/* Potential matches (FR-9) and candidate review (FR-10, FR-11) */}
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md">
        <div className="px-3 py-2 border-b border-gray-200 dark:border-gray-700">
          <p className="text-sm font-medium text-gray-900 dark:text-white">Candidate efforts</p>
          <p className="text-xs text-gray-500 dark:text-gray-400">
            Metrics are scaled to the largest value in this candidate set. Selecting a candidate
            draws its effort route dashed on the map.
          </p>
        </div>
        <div className="p-3">
          {loadingCandidates ? (
            <p role="status" className="text-sm text-gray-500 dark:text-gray-400">
              Loading candidate efforts…
            </p>
          ) : candidates.length === 0 ? (
            <p role="status" className="text-sm text-gray-500 dark:text-gray-400">
              No candidate efforts on file. Run “Find matches” to search for them.
            </p>
          ) : (
            <CandidateTable
              rows={candidates}
              selectedActivityId={selectedId}
              busy={busy}
              onSelect={selectCandidate}
              onConfirm={(candidate) => finalize(candidate, true)}
              onReject={(candidate) => finalize(candidate, false)}
            />
          )}
        </div>
      </div>

      {deleteOpen && (
        <ConfirmDialog
          title={`Delete ${segment.is_course ? 'course' : 'segment'} “${segment.segment_name}”?`}
          message="This removes the segment together with its match rows, match exclusions, and segment-detail records. This cannot be undone."
          confirmLabel="Delete segment"
          busyLabel="Deleting…"
          busy={deleteLoading}
          error={deleteError}
          onCancel={() => setDeleteOpen(false)}
          onConfirm={deleteSegment}
        />
      )}
    </div>
  );
}