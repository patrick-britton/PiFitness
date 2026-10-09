'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { API } from '@/lib/api-client';
import { formatElapsedDuration, formatMiles } from '@/lib/date-format';
import {
  DEFAULT_ARROW_MIN_SEPARATION_M,
  DEFAULT_ARROW_SPACING_M,
  dedupeArrows,
  directionArrows,
  type DirectionArrow,
} from '@/lib/route-direction';
import { cumulativeMeters, nearestPointAtDistance } from '@/lib/segment-trim';
import { ActivityRoute, CandidateEffort, SegmentListRow } from '@/lib/types/segment-management';
import { defaultStyleForTheme } from '@/lib/map-styles';
import { isDarkTheme } from '@/lib/theme-version';
import { useViewportStore } from '@/stores/viewportStore';
import CandidateTable from './CandidateTable';
import ConfirmDialog from './ConfirmDialog';
import ElevationProfile, { type ElevationComparison } from './ElevationProfile';
import MapStyleSelector from './MapStyleSelector';
import MatchProgressSteps, { type MatchStepState } from './MatchProgressSteps';
import SegmentRouteMap, { type PickMarker, type RouteArrow } from './SegmentRouteMap';

/** The four find-matches pipeline steps, in backend order (T20). */
const FIND_STEPS: { step: number; label: string }[] = [
  { step: 1, label: 'Match candidate activities' },
  { step: 2, label: 'Generate activity pairs' },
  { step: 3, label: 'Score polygon deviations' },
  { step: 4, label: 'Auto-approve obvious matches' },
];

/**
 * Confidence above this is junk, not a user decision (009-003 T32/AC-20):
 * candidates arrive ordered best-first (T30) and anything over this never
 * reaches the table. Strict `>` — a row at exactly 200 survives.
 */
const AUTO_REJECT_CONFIDENCE_OVER = 200;

/**
 * Sub-metre slack when deciding whether a series reaches a picked distance
 * (009-003 T40): the elevation plot's x values are whole metres, so a pick on
 * the last plotted tick can exceed the raw cumulative length by up to 0.5 m
 * without being past the series' data.
 */
const PICK_EXTENT_TOLERANCE_M = 0.5;

/**
 * Shared empty glyph list (009-010 T04, FR-3): a route-less render returns this
 * same array, so no `directionArrows` pass runs for a route that is not loaded
 * and the map's `arrows` prop keeps a stable identity.
 */
const NO_ARROWS: DirectionArrow[] = [];

/** One series' outcome for a distance picked on the elevation plot (009-003 T40). */
interface PlotPickSide {
  series: PickMarker['series'];
  /** Legend name of the series (`segment.segment_name` / `Candidate <id>`). */
  label: string;
  /**
   * Marker + display values, or null when the series does not reach the picked
   * distance. `pct` is the pick's position as a percentage of this series' OWN
   * plotted length (009-010 T05, OQ-3 = B) — the fraction the marker was placed
   * at, stated in the legend so the marker and the axis cannot disagree.
   */
  mark: { point: number[]; distanceM: number; miles: string; elapsed: string; pct: number } | null;
  /** The series' own end distance in meters, named in the "outside" note. */
  extentM: number;
}

/** Two-series report for a plot pick: markers for the map plus the legend text. */
interface PlotPickReport {
  /** The picked distance in whole meters (the plot's own x convention). */
  targetM: number;
  /** The plot's x-axis span the pick divides by — the largest plotted extent. */
  spanM: number;
  /** The pick as a percentage of that span: the fraction every series is marked at. */
  pctOfSpan: number;
  sides: PlotPickSide[];
  /** Circle markers to hand to the map (only the sides that reach the distance). */
  markers: PickMarker[];
  /** Legend text naming each series, its pick fraction, distance and elapsed time. */
  legend: string;
}

/**
 * Resolve one series' marker for a pick on the elevation plot (009-003 T40;
 * fraction basis added by 009-010 T05, OQ-3 = B).
 *
 * Two rules, and the difference is the point of the loop comparison:
 *  - WHETHER a series is marked at all is decided by the CLICKED distance: a
 *    series whose plotted line ends before the click has no marker there and is
 *    named as missing instead (AC-4), so a single marker is never mistaken for
 *    "both activities are here".
 *  - WHERE it is marked is that fraction of the series' OWN plotted length
 *    (AC-10): `fraction × this series' extent`, resolved by the shared
 *    `nearestPointAtDistance` scan over this series' memoized cumulative array —
 *    the very array its line is plotted against. Two routes of different lengths
 *    therefore land at the SAME fraction of their own tracks (which is what makes
 *    loop direction comparable), while the fraction is stated in the legend so
 *    the marker and the axis reading cannot disagree.
 */
function buildPickSide(
  series: PickMarker['series'],
  label: string,
  coords: number[][],
  cumulative: number[],
  elapsed: number[] | undefined,
  clickedM: number,
  fraction: number,
): PlotPickSide {
  const extentM = cumulative.length > 0 ? Math.round(cumulative[cumulative.length - 1]) : 0;
  if (coords.length === 0 || clickedM - extentM > PICK_EXTENT_TOLERANCE_M) {
    return { series, label, mark: null, extentM };
  }
  const hit = nearestPointAtDistance(coords, fraction * extentM, cumulative);
  if (!hit) return { series, label, mark: null, extentM };
  const distanceM = Math.round(hit.distanceM);
  const elapsedS = elapsed?.[hit.index];
  return {
    series,
    label,
    mark: {
      point: coords[hit.index],
      distanceM,
      miles: formatMiles(distanceM),
      elapsed: formatElapsedDuration(typeof elapsedS === 'number' ? elapsedS : null),
      pct: Math.round(fraction * 100),
    },
    extentM,
  };
}

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
  /** Per-step progress for the find-matches pipeline (T20); null = idle. */
  const [findSteps, setFindSteps] = useState<MatchStepState[] | null>(null);
  const [scoringKind, setScoringKind] = useState<'hausdorff' | 'frechet' | null>(null);
  const [bulkLoading, setBulkLoading] = useState(false);
  /** True while the automatic >200-confidence sweep runs (T32): it disables the other match controls. */
  const [autoRejecting, setAutoRejecting] = useState(false);
  /** Deliberate manual reject-all dialog (T33/AC-22) + its in-flight/error state. */
  const [rejectAllOpen, setRejectAllOpen] = useState(false);
  const [rejectAllLoading, setRejectAllLoading] = useState(false);
  const [rejectAllError, setRejectAllError] = useState<string | null>(null);

  /**
   * Plot pick (009-010 T05, FR-2): the distance the elevation plot reported on a
   * CLICK, on the plot's own x basis. A click is the only plot gesture that sets
   * it (the 009-003 hover reporting is gone), so the emphasized map markers are
   * persistent: only a new click, a deselect, a segment change or the explicit
   * clear control moves or drops them. The map → elevation direction and its
   * readout were deleted in T01 with the bug they threw.
   */
  const [plotPickDistance, setPlotPickDistance] = useState<number | null>(null);

  /**
   * Cumulative distance per route point, memoized per route (009-003 T39b): the
   * pick maths and the plot's x axis share this one array, so resolving a click
   * costs one linear scan over already-held data — no recompute per pick and no
   * extra fetch on a Pi/phone.
   */
  const routeCumulative = useMemo(
    () => (route?.path_coords ? cumulativeMeters(route.path_coords) : []),
    [route?.path_coords],
  );

  const handlePlotPick = useCallback(
    (distanceM: number) => {
      if (!route || route.path_coords.length === 0) return;
      if (!Number.isFinite(distanceM)) return;
      setPlotPickDistance(distanceM);
    },
    [route],
  );

  /** Drop the plot pick (009-010 T01): the indicator and map markers clear. */
  const clearPick = useCallback(() => {
    setPlotPickDistance(null);
  }, []);

  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [candidateRoute, setCandidateRoute] = useState<ActivityRoute | null>(null);
  const [candidateRouteError, setCandidateRouteError] = useState<string | null>(null);
  /**
   * Cumulative distance of the selected candidate route, memoized per route
   * (009-003 T40): the comparison series' own x axis, so a picked distance
   * resolves against it exactly as its plotted line was drawn. Computed once per
   * selection, never per pick, so a hover frame costs only the linear scan.
   */
  const candidateCumulative = useMemo(
    () => (candidateRoute?.path_coords ? cumulativeMeters(candidateRoute.path_coords) : []),
    [candidateRoute?.path_coords],
  );

  /**
   * The plot's own x-axis span (009-010 T05, OQ-3 = B): the largest plotted
   * distance, computed exactly the way `ElevationProfile` computes its axis max
   * — each series' last plotted x, i.e. its end distance rounded to whole metres
   * as every plotted point is. A pick is divided by this number, so the fraction
   * a click yields is a fraction of the very axis the user saw. Memoized on the
   * two cumulative arrays: it changes only when a route does.
   */
  const plottedSpanM = useMemo(() => {
    const ends = [routeCumulative, candidateCumulative]
      .map((c) => (c.length > 0 ? Math.round(c[c.length - 1]) : null))
      .filter((m): m is number => m != null && m > 0);
    return ends.length > 0 ? Math.max(...ends) : 0;
  }, [routeCumulative, candidateCumulative]);

  /**
   * Direction glyphs, one pure pass per route (009-010 T04, FR-3): the segment's
   * reference window and — once a candidate is selected — that candidate's own
   * window, each sampled at the nominal 50 m spacing by `directionArrows`.
   *
   * Two memos keyed on their own coordinate array (the identity the cumulative
   * memos above use), so selecting a candidate re-samples only the new route and
   * a pick, a hover, or any state the routes do not carry costs nothing: the
   * passes run once per route load, never per frame and never per render. Each
   * series' glyphs are then tagged for the map (one symbol layer per series) and
   * merged through `dedupeArrows` (009-010 T11, AC-6b): glyphs from either
   * series closer than `DEFAULT_ARROW_MIN_SEPARATION_M` collapse to one — the
   * route's, being listed first — so the map never draws two arrows into an
   * unreadable blob, while a series that would lose every glyph keeps one.
   */
  const routeArrows = useMemo(
    () => (route?.path_coords?.length ? directionArrows(route.path_coords, DEFAULT_ARROW_SPACING_M) : NO_ARROWS),
    [route?.path_coords],
  );
  const candidateArrows = useMemo(
    () =>
      candidateRoute?.path_coords?.length
        ? directionArrows(candidateRoute.path_coords, DEFAULT_ARROW_SPACING_M)
        : NO_ARROWS,
    [candidateRoute?.path_coords],
  );
  const mapArrows = useMemo(
    (): RouteArrow[] =>
      dedupeArrows(
        [
          ...routeArrows.map((a) => ({
            series: 'route' as const,
            lngLat: a.lngLat,
            bearing: a.bearing,
          })),
          ...candidateArrows.map((a) => ({
            series: 'route-overlay' as const,
            lngLat: a.lngLat,
            bearing: a.bearing,
          })),
        ],
        DEFAULT_ARROW_MIN_SEPARATION_M,
        (arrow) => arrow.series,
      ),
    [routeArrows, candidateArrows],
  );

  /**
   * Drop the candidate selection together with its route and any plot pick that
   * described it (009-003 T40) — every path that deselects a candidate funnels
   * here, so a marker for an activity that is no longer selected cannot linger.
   */
  const clearSelection = useCallback(() => {
    setSelectedId(null);
    setCandidateRoute(null);
    setCandidateRouteError(null);
    setPlotPickDistance(null);
  }, []);

  /**
   * Two-series report for a pick on the elevation plot (009-003 T40, AC-27;
   * fraction basis 009-010 T05, OQ-3 = B).
   *
   * The clicked x is divided by the plot's own span to give a fraction; each
   * series that reaches the clicked distance is then resolved against its OWN
   * cumulative distance at that fraction of its own extent — the same convention
   * the two plotted lines use — producing up to two map markers (halo + circle
   * layers per series, distinct colours, sizes and weights) plus the legend text
   * that names each series with the fraction, its point's distance and elapsed
   * time, or names it as missing when the pick lies beyond that series' extent. A
   * series with no route (no comparison selected, or its route failed to load) is
   * named too, so a single marker is never mistaken for "both activities are
   * here". Pure state: no request, no per-frame work.
   */
  const plotPickReport = useMemo((): PlotPickReport | null => {
    if (plotPickDistance == null || !Number.isFinite(plotPickDistance)) return null;
    if (!route || route.path_coords.length === 0) return null;
    const targetM = Math.round(plotPickDistance);
    const spanM = plottedSpanM > 0 ? plottedSpanM : targetM;
    const fraction = spanM > 0 ? Math.min(1, Math.max(0, targetM / spanM)) : 0;
    const sides: PlotPickSide[] = [
      buildPickSide(
        'route',
        segment.segment_name,
        route.path_coords,
        routeCumulative,
        route.elapsed_s,
        targetM,
        fraction,
      ),
    ];
    const candidateCoords = candidateRoute?.path_coords ?? [];
    const comparisonUsable = selectedId != null && candidateCoords.length > 0;
    if (comparisonUsable) {
      sides.push(
        buildPickSide(
          'route-overlay',
          `Candidate ${selectedId}`,
          candidateCoords,
          candidateCumulative,
          candidateRoute?.elapsed_s,
          targetM,
          fraction,
        ),
      );
    }
    const parts = sides.map((side) =>
      side.mark
        ? `${side.label}: ${side.mark.pct}% of its length · ${side.mark.distanceM.toLocaleString('en-US')} m (${side.mark.miles} mi), elapsed ${side.mark.elapsed}`
        : `${side.label}: beyond its ${side.extentM.toLocaleString('en-US')} m extent, so no marker`,
    );
    if (!comparisonUsable) {
      const why =
        selectedId == null
          ? 'no comparison activity is selected'
          : candidateRouteError
            ? `candidate ${selectedId}'s route could not be loaded`
            : `candidate ${selectedId}'s route has no GPS points`;
      parts.push(`Candidate: ${why}, so no marker`);
    }
    const markers: PickMarker[] = [];
    for (const side of sides) {
      if (side.mark) {
        markers.push({
          series: side.series,
          point: side.mark.point,
          label: `${side.label} · ${side.mark.distanceM} m`,
        });
      }
    }
    return {
      targetM,
      spanM,
      pctOfSpan: Math.round(fraction * 100),
      sides,
      markers,
      legend: parts.join(' · '),
    };
  }, [
    plotPickDistance,
    plottedSpanM,
    route,
    routeCumulative,
    candidateRoute,
    candidateCumulative,
    candidateRouteError,
    selectedId,
    segment.segment_name,
  ]);

  /** Latest selection for the auto-reject sweep (T32 runs inside loadCandidates). */
  const selectedIdRef = useRef<number | null>(null);
  useEffect(() => {
    selectedIdRef.current = selectedId;
  }, [selectedId]);
  /** Latest confirmed-match count for scoring arrivals swept without a re-read. */
  const existingMatchCountRef = useRef(0);
  useEffect(() => {
    existingMatchCountRef.current = existingMatchCount;
  }, [existingMatchCount]);

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
      clearPick();
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
      clearPick();
    } catch {
      setRoute(null);
      setRouteError('Could not load the segment route.');
      clearPick();
    } finally {
      setLoadingRoute(false);
    }
  }, [refActivityId, refStartM, refEndM, clearPick]);

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

  const ingestCandidateSet = useCallback(
    async (initial?: { data?: CandidateEffort[]; existing_match_count: number }): Promise<number> => {
      // One shared sweep for every candidate-set arrival (initial load, find
      // completion, refresh, explicit scoring): rejected rows leave the
      // downselect view, so a second read after the sweep is clean and later
      // loads never re-fire. Returns the auto-rejected count.
      let res = initial ?? (await API.segments.getCandidates(segment.segment_id));
      let rows = res.data ?? [];
      let existing = res.existing_match_count;
      let autoRejected = 0;
      if (rows.some((c) => c.confidence > AUTO_REJECT_CONFIDENCE_OVER)) {
        setAutoRejecting(true);
        try {
          const rejected = await API.segments.bulkReject(segment.segment_id, {
            confidence_over: AUTO_REJECT_CONFIDENCE_OVER,
          });
          autoRejected = rejected.rejected ?? 0;
          if (selectedIdRef.current != null) {
            clearSelection();
          }
          res = await API.segments.getCandidates(segment.segment_id);
          rows = res.data ?? [];
          existing = res.existing_match_count;
        } catch {
          setActionError('Auto-reject of low-confidence matches failed.');
        } finally {
          setAutoRejecting(false);
        }
      }
      setCandidates(rows);
      setExistingMatchCount(existing);
      return autoRejected;
    },
    [segment.segment_id, clearSelection],
  );

  const loadCandidates = useCallback(
    async (opts?: { keepStatus?: boolean }): Promise<number> => {
      setLoadingCandidates(true);
      try {
        const autoRejected = await ingestCandidateSet();
        if (autoRejected > 0 && !opts?.keepStatus) {
          setStatusMessage(
            `Auto-rejected ${autoRejected} candidate${autoRejected === 1 ? '' : 's'} with confidence > ${AUTO_REJECT_CONFIDENCE_OVER}.`,
          );
        }
        setCandidatesError(null);
        return autoRejected;
      } catch {
        setCandidates([]);
        setCandidatesError('Could not load candidate efforts.');
        return 0;
      } finally {
        setLoadingCandidates(false);
      }
    },
    [ingestCandidateSet],
  );

  useEffect(() => {
    loadCandidates();
  }, [loadCandidates]);

  // A different segment means a different candidate set: never keep a stale
  // selection, message, active candidate route, or pick.
  useEffect(() => {
    clearSelection();
    setStatusMessage(null);
    setActionError(null);
    setFindSteps(null);
    setRejectAllOpen(false);
    setRejectAllError(null);
    clearPick();
  }, [segment.segment_id, clearSelection, clearPick]);

  const findMatches = useCallback(async () => {
    setFindLoading(true);
    setActionError(null);
    setStatusMessage(null);
    const sid = segment.segment_id;
    setFindSteps(
      FIND_STEPS.map(({ step, label }) => ({
        step,
        label,
        status: 'pending' as const,
        elapsedMs: null,
        error: null,
      })),
    );
    try {
      for (const { step, label } of FIND_STEPS) {
        const startedAtMs = Date.now();
        setFindSteps((prev) =>
          (prev ?? []).map((s) =>
            s.step === step ? { ...s, status: 'running', startedAtMs } : s,
          ),
        );
        try {
          await API.segments.findMatchesStep(sid, step);
          setFindSteps((prev) =>
            (prev ?? []).map((s) =>
              s.step === step
                ? { ...s, status: 'complete', elapsedMs: Date.now() - startedAtMs, startedAtMs: null }
                : s,
            ),
          );
        } catch (e) {
          // Surface which pipeline step failed and why (T20; Bug 009-003-21-1
          // made this visible — the raw 500 named the failing SP).
          const detail =
            e instanceof Error && e.message ? e.message.slice(0, 300) : 'unknown error';
          setFindSteps((prev) =>
            (prev ?? []).map((s) =>
              s.step === step
                ? { ...s, status: 'error', elapsedMs: Date.now() - startedAtMs, error: detail }
                : s,
            ),
          );
          setActionError(`Match search failed at step ${step} (${label}).`);
          return;
        }
      }
      // The auto-approval pass can move candidates into confirmed matches, so
      // the candidate set and the existing-match count are both re-read here
      // (through the auto-reject sweep, so junk never reaches the table).
      const autoRejected = await loadCandidates({ keepStatus: true });
      setStatusMessage(
        autoRejected > 0
          ? `Match search completed. Auto-rejected ${autoRejected} candidate${autoRejected === 1 ? '' : 's'} with confidence > ${AUTO_REJECT_CONFIDENCE_OVER}.`
          : 'Match search completed.',
      );
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
          clearSelection();
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
    [segment.segment_id, selectedId, clearSelection],
  );

  const bulkConfirm = useCallback(async () => {
    setBulkLoading(true);
    setActionError(null);
    setStatusMessage(null);
    try {
      await API.segments.bulkConfirm(segment.segment_id, { confirm_all: true });
      clearSelection();
      await loadCandidates();
      setStatusMessage('Bulk confirm completed.');
    } catch {
      setActionError('Bulk confirm failed.');
    } finally {
      setBulkLoading(false);
    }
  }, [segment.segment_id, loadCandidates, clearSelection]);

  /** Deliberate manual reject-all (T33/AC-22): fires only from the confirm dialog. */
  const bulkRejectAll = useCallback(async () => {
    setRejectAllLoading(true);
    setRejectAllError(null);
    try {
      const res = await API.segments.bulkReject(segment.segment_id, {});
      setRejectAllOpen(false);
      clearSelection();
      await loadCandidates({ keepStatus: true });
      const n = res.rejected ?? 0;
      setStatusMessage(
        `Rejected ${n} candidate${n === 1 ? '' : 's'}.`,
      );
    } catch {
      setRejectAllError('Failed to reject all remaining candidates.');
    } finally {
      setRejectAllLoading(false);
    }
  }, [segment.segment_id, loadCandidates, clearSelection]);

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
        // The scoring response carries the rescored set; run it through the
        // same auto-reject sweep so a rescored >200 row never reaches the table.
        const autoRejected = await ingestCandidateSet({
          data: res.candidates ?? [],
          existing_match_count: existingMatchCountRef.current,
        });
        setStatusMessage(
          autoRejected > 0
            ? `${res.message || `${kind} scoring completed.`} Auto-rejected ${autoRejected} candidate${autoRejected === 1 ? '' : 's'} with confidence > ${AUTO_REJECT_CONFIDENCE_OVER}.`
            : res.message || `${kind} scoring completed.`,
        );
      } catch {
        setActionError(
          kind === 'hausdorff' ? 'Hausdorff scoring failed.' : 'Fréchet scoring failed.',
        );
      } finally {
        setScoringKind(null);
      }
    },
    [segment.segment_id, ingestCandidateSet],
  );

  /**
   * Stable row-action adapters for the memoised candidate table (009-003 T42,
   * Bug 009-003-41-1): inline arrows would be new identities on every render, so
   * a pick change — which re-renders the session — would defeat `CandidateTable`'s
   * memo and re-render every candidate row. These depend only on `finalize`,
   * which changes when the segment or the selection changes (both of which
   * legitimately re-render the table).
   */
  const confirmCandidate = useCallback(
    (candidate: CandidateEffort) => finalize(candidate, true),
    [finalize],
  );
  const rejectCandidate = useCallback(
    (candidate: CandidateEffort) => finalize(candidate, false),
    [finalize],
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

  const busy = findLoading || bulkLoading || scoringKind !== null || autoRejecting || rejectAllLoading;
  const mapHeight = isLandscape ? '220px' : isPortrait ? '240px' : '320px';
  const profileHeight = isLandscape ? '140px' : '200px';
  const hasRoute = Boolean(route && route.path_coords?.length);
  const candidateOverlay = candidateRoute?.path_coords?.length
    ? candidateRoute.path_coords
    : undefined;
  /**
   * Selected candidate as the profile's comparison series (T34/T35, AC-23/AC-24):
   * `candidateRoute` is nulled on every clear path, so the series follows the
   * selection without extra state.
   */
  const candidateComparison: ElevationComparison | null =
    selectedId != null && candidateRoute && candidateRoute.elevations.length >= 2
      ? {
          coords: candidateRoute.path_coords,
          elevations: candidateRoute.elevations,
          label: `Candidate ${selectedId}`,
        }
      : null;

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
              {segment.distance_mi.toFixed(1)} mi
              {segment.elevation_gain != null ? ` · ${segment.elevation_gain} m elev` : ''}
              {segment.activity_type_name ? ` · ${segment.activity_type_name}` : ''}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={onChooseDifferentSegment}
              className="px-3 py-1.5 min-h-[44px] inline-flex items-center text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              Change segment
            </button>
            <button
              type="button"
              onClick={() => {
                setDeleteError(null);
                setDeleteOpen(true);
              }}
              className="px-3 py-1.5 min-h-[44px] inline-flex items-center text-sm font-medium rounded-md border border-red-300 dark:border-red-700 text-red-700 dark:text-red-300 hover:bg-red-50 dark:hover:bg-red-900/20 focus:outline-none focus:ring-2 focus:ring-blue-500"
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
            className="px-3 py-1.5 min-h-[44px] inline-flex items-center text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
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
              candidateOverlay ? ', with the selected candidate effort drawn in the comparison colour' : ''
            }${
              routeArrows.length > 0
                ? ', with filled triangle arrows on the route showing the direction it was travelled'
                : ''
            }${
              candidateOverlay && candidateArrows.length > 0
                ? ', with open chevron arrows on the candidate effort showing the direction it was travelled'
                : ''
            }${
              plotPickReport
                ? `, with picked points at ${plotPickReport.targetM.toLocaleString('en-US')} m marked for ${plotPickReport.sides
                    .filter((side) => side.mark)
                    .map((side) => side.label)
                    .join(' and ')}`
                : ''
            }`}
            pickMarkers={plotPickReport?.markers}
            arrows={mapArrows}
          />
          <p className="text-xs text-gray-500 dark:text-gray-400">
            Map legend: both lines are solid, told apart by colour — blue = the{' '}
            {segment.is_course ? 'course' : 'segment'} route
            {routeArrows.length > 0
              ? ', with filled triangle arrows pointing the way it was travelled'
              : ''}
            {candidateOverlay
              ? `; orange = candidate ${selectedId} effort${
                  candidateArrows.length > 0
                    ? ', with open chevron arrows pointing the way it was travelled'
                    : ''
                }`
              : ''}
            . One glyph is drawn every {DEFAULT_ARROW_SPACING_M} m of each path; an arrow
            within {DEFAULT_ARROW_MIN_SEPARATION_M} m of one already drawn is left out so the
            glyphs never merge into a blob.
          </p>
          {plotPickReport && (
            /*
              Two-series pick legend (T40, AC-27): names each activity with its
              point at the picked distance and its elapsed time (009-003 OQ-6 format),
              or names the side that cannot be marked — never colour alone, and
              the missing side is always stated rather than left to inference.
            */
            <p role="status" className="text-sm text-gray-700 dark:text-gray-300 tabular-nums">
              Picked {plotPickReport.targetM.toLocaleString('en-US')} m on the elevation plot (
              {plotPickReport.pctOfSpan}% of the {plotPickReport.spanM.toLocaleString('en-US')} m
              span) — {plotPickReport.legend}.
            </p>
          )}
          {plotPickReport && (
            <button
              type="button"
              onClick={clearPick}
              className="px-3 py-1.5 min-h-[44px] inline-flex items-center text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              Clear picked point
            </button>
          )}
          {candidateRouteError && (
            <p role="alert" className="text-sm text-red-600 dark:text-red-400">
              {candidateRouteError}
            </p>
          )}
          {/*
            Comparison elevation (T34/T35, AC-23/AC-24): the candidate route
            fetched for the map overlay already carries elevations, so the
            selected effort is handed to the shared profile as its dashed
            comparison series — the same orange + dashed semantics as the map
            overlay, so both activities read identically in either view. It
            disappears whenever the selection clears (finalize, bulk
            confirm/reject, segment change) or the candidate route failed,
            because `candidateRoute` is nulled on every one of those paths.
          */}
          <ElevationProfile
            coords={route.path_coords}
            elevations={route.elevations}
            height={profileHeight}
            label={segment.segment_name}
            ariaLabel={
              candidateComparison
                ? `Elevation profile of ${segment.segment_name} compared with candidate ${selectedId}`
                : `Elevation profile of ${segment.segment_name}`
            }
            comparison={candidateComparison}
            pickDistance={plotPickReport ? plotPickReport.targetM : null}
            onPickDistance={handlePlotPick}
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
        <p className="rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] inline-flex items-center text-sm text-gray-700 dark:text-gray-300">
          Existing matches:{' '}
          <span className="font-semibold tabular-nums">{existingMatchCount}</span>
        </p>
        <p className="rounded-md border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] inline-flex items-center text-sm text-gray-700 dark:text-gray-300">
          Potential matches:{' '}
          <span className="font-semibold tabular-nums">{candidates.length}</span>
        </p>
        <button
          type="button"
          onClick={findMatches}
          disabled={busy || loadingCandidates}
          className="px-3 py-1.5 min-h-[44px] inline-flex items-center text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {findLoading ? 'Finding matches…' : 'Find matches'}
        </button>
        <button
          type="button"
          onClick={bulkConfirm}
          disabled={busy || loadingCandidates || candidates.length === 0}
          className="px-3 py-1.5 min-h-[44px] inline-flex items-center text-sm font-medium rounded-md bg-green-700 text-white hover:bg-green-800 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {bulkLoading ? 'Confirming all…' : 'Confirm all remaining'}
        </button>
        <button
          type="button"
          onClick={() => {
            setRejectAllError(null);
            setRejectAllOpen(true);
          }}
          disabled={busy || loadingCandidates || candidates.length === 0}
          className="px-3 py-1.5 min-h-[44px] inline-flex items-center text-sm font-medium rounded-md bg-red-700 text-white hover:bg-red-800 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          Reject all remaining
        </button>
        <button
          type="button"
          onClick={() => runScoring('hausdorff')}
          disabled={busy || loadingCandidates}
          className="px-3 py-1.5 min-h-[44px] inline-flex items-center text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {scoringKind === 'hausdorff' ? 'Running Hausdorff…' : 'Run Hausdorff scoring'}
        </button>
        <button
          type="button"
          onClick={() => runScoring('frechet')}
          disabled={busy || loadingCandidates}
          className="px-3 py-1.5 min-h-[44px] inline-flex items-center text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {scoringKind === 'frechet' ? 'Running Fréchet…' : 'Run Fréchet scoring'}
        </button>
        <button
          type="button"
          onClick={() => loadCandidates()}
          disabled={busy || loadingCandidates}
          className="px-3 py-1.5 min-h-[44px] inline-flex items-center text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
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

      {findSteps && <MatchProgressSteps steps={findSteps} />}

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
              onClick={() => loadCandidates()}
              className="mt-2 px-3 py-1.5 min-h-[44px] inline-flex items-center text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
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
              onConfirm={confirmCandidate}
              onReject={rejectCandidate}
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

      {rejectAllOpen && (
        <ConfirmDialog
          title={`Reject all ${candidates.length} remaining candidate${candidates.length === 1 ? '' : 's'}?`}
          message="This rejects every remaining candidate effort for this segment through the existing finalize path. This cannot be undone."
          confirmLabel="Reject all remaining"
          busyLabel="Rejecting…"
          busy={rejectAllLoading}
          error={rejectAllError}
          onCancel={() => {
            if (!rejectAllLoading) setRejectAllOpen(false);
          }}
          onConfirm={bulkRejectAll}
        />
      )}
    </div>
  );
}