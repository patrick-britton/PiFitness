'use client';

import { useState, useEffect, useCallback } from 'react';
import { useUIStore } from '@/stores/uiStore';
import { API } from '@/lib/api-client';
import {
  ActivityReport,
  ActivityReportSegment,
} from '@/lib/types/activity-report';
import {
  CoursePathChart,
  ElevationChart,
  HeartrateChart,
} from './components/ActivityReportCharts';
import {
  EFFICIENCY_BULLETS,
  EfficiencyBullets,
} from './components/EfficiencyBullets';
import LeaderboardTable from '../leaderboards/components/LeaderboardTable';
import {
  formatAbsMSS,
  formatCardDelta,
  formatSegmentCount,
} from '@/lib/report-format';

/**
 * Recent Activity Report page (009-001, reworked 004-006).
 * Loads the newest qualifying activity (run / trail run / walk / hike, >1000 m,
 * non-null path) and renders the title (course name, else activity_name) with
 * prev/next arrows, a course-only info bar, a summary header, an optional
 * course section, and the list of crossed segments; a tapped segment card
 * swaps the whole view for the embedded segment leaderboard (004-006 T08,
 * AC-8). Layout-aware for desktop / portrait / landscape and themed via
 * Tailwind dark: tokens (design-system.md).
 */

/** Format an absolute delta + direction (e.g. 75 → "1:15 faster"). Shared with
 *  the course info bar; card rows use formatCardDelta (AC-7 thresholds).
 *  Deltas are reference-minus-current, so positive means this effort was faster. */
function formatInfoDelta(delta: number | null, noun: string): string {
  if (delta == null) return `— ${noun}`;
  if (delta === 0) return `0:00 ${noun}`;
  const label = delta > 0 ? 'faster' : 'slower';
  return `${formatAbsMSS(delta)} ${label} ${noun}`;
}

/**
 * Course-only info bar (AC-3). Exactly one variant:
 * - rank 1 → gold medal + "faster by m:ss vs best"
 *   (best excludes the current attempt, so best_delta_s > 0 / null on a record)
 * - rank 2–3 → silver/bronze medal + two-part text
 * - prior == best (equal deltas) → merged "than prior/best" variant
 * - otherwise → "#{rank} | m:ss … than prior, m:ss … than best"
 */
function CourseInfoBar({ course }: { course: ActivityReportSegment }) {
  const rank = course.all_time_rank;
  const medal = rank === 1 ? 'gold' : rank === 2 ? 'silver' : rank === 3 ? 'bronze' : null;
  const medalFill = medal === 'gold' ? '#D4A017' : medal === 'silver' ? '#9AA3B2' : '#B0703C';
  let text: string;
  if (rank === 1) {
    text = course.best_delta_s != null
      ? `faster by ${formatAbsMSS(course.best_delta_s)} vs best`
      : 'fastest attempt';
  } else if (
    course.prior_delta_s != null &&
    course.best_delta_s != null &&
    course.prior_delta_s === course.best_delta_s
  ) {
    text = `${formatAbsMSS(course.prior_delta_s)} ${course.prior_delta_s > 0 ? 'faster' : 'slower'} than prior/best`;
  } else {
    text = `${formatInfoDelta(course.prior_delta_s, 'than prior')}, ${formatInfoDelta(course.best_delta_s, 'than best')}`;
  }
  return (
    <p className="mt-1 text-sm text-gray-600 dark:text-gray-300 flex items-center gap-1.5">
      {medal && (
        <svg
          width="14"
          height="14"
          viewBox="0 0 16 16"
          aria-label={`${medal} medal`}
          role="img"
          className="shrink-0"
        >
          <circle cx="8" cy="8" r="7" fill={medalFill} />
          <text x="8" y="11.5" textAnchor="middle" fontSize="9" fontWeight="700" fill="#fff">
            {rank}
          </text>
        </svg>
      )}
      {rank !== 1 && (
        <span className="font-semibold tabular-nums">#{rank ?? '—'} | </span>
      )}
      <span>{text}</span>
    </p>
  );
}

/** Render an ISO-8601 UTC timestamp as `d-mmm h:mm am/pm` (local time). */
function formatStartShort(iso: string): string {
  const d = new Date(iso);
  const months = [
    'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
  ];
  let hours = d.getHours();
  const ampm = hours >= 12 ? 'pm' : 'am';
  hours = hours % 12;
  if (hours === 0) hours = 12;
  const mins = String(d.getMinutes()).padStart(2, '0');
  return `${d.getDate()} ${months[d.getMonth()]} ${hours}:${mins} ${ampm}`;
}

/** Format "A/B" rank; emits "—" when rank is missing. */
function formatRank(rank: number | null, total: number): string {
  if (rank == null) return '—';
  return `${rank}/${total}`;
}

function SegmentRow({
  seg,
  selected,
  onSelect,
}: {
  seg: ActivityReportSegment;
  selected: boolean;
  onSelect: (seg: ActivityReportSegment) => void;
}) {
  return (
    <button
      type="button"
      onClick={() => onSelect(seg)}
      className={`w-full text-left bg-white dark:bg-gray-800 border rounded-md p-3 transition-colors ${
        selected
          ? 'border-blue-500 dark:border-blue-400'
          : 'border-gray-200 dark:border-gray-700 hover:border-gray-300 dark:hover:border-gray-600'
      }`}
    >
      <div className="flex items-center justify-between gap-2">
        <p className="text-sm font-semibold text-gray-900 dark:text-white truncate">
          {seg.name}
        </p>
        <span className="shrink-0 text-xs font-medium text-gray-500 dark:text-gray-400">
          {formatRank(seg.all_time_rank, seg.total_attempts)}
        </span>
      </div>
      <p className="mt-1 text-xs text-gray-600 dark:text-gray-300">
        Prior: {formatCardDelta(seg.prior_delta_s)} · Best:{' '}
        {formatCardDelta(seg.best_delta_s)}
      </p>
    </button>
  );
}

/**
 * In-page leaderboard screen (004-006 T08 / OQ-2 A / AC-8): segment name with
 * an X that returns to the report, wrapping the SHARED `LeaderboardTable` in
 * `embedded` mode — the host renders the header, so future leaderboard changes
 * on the Leaderboards page flow to this host for free.
 */
function LeaderboardScreen({
  segment,
  onClose,
}: {
  segment: ActivityReportSegment;
  onClose: () => void;
}) {
  return (
    <div>
      <div className="flex items-center justify-between gap-3 mb-3 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-3">
        <p className="text-sm font-semibold text-gray-900 dark:text-white truncate">
          {segment.name}
        </p>
        <button
          type="button"
          onClick={onClose}
          aria-label={`Close ${segment.name} leaderboard and return to the report`}
          className="shrink-0 w-8 h-8 flex items-center justify-center text-lg font-medium rounded-md text-gray-500 dark:text-gray-400 border border-gray-300 dark:border-gray-600 hover:bg-gray-100 dark:hover:bg-gray-700 hover:text-gray-700 dark:hover:text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          ×
        </button>
      </div>
      <LeaderboardTable
        segmentId={segment.segment_id}
        segmentName={segment.name}
        embedded
      />
    </div>
  );
}

export default function RecentActivityPage() {
  const { setActiveSubPage } = useUIStore();

  const [report, setReport] = useState<ActivityReport | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedSegmentId, setSelectedSegmentId] = useState<number | null>(
    null,
  );

  const fetchReport = useCallback(async (activityId?: number) => {
    setLoading(true);
    setError(null);
    setReport(null);
    setSelectedSegmentId(null);
    try {
      const data = await API.activities.getReport(activityId);
      setReport(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load report');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    setActiveSubPage('recent-activity');
    fetchReport();
  }, [setActiveSubPage, fetchReport]);

  const handleSegmentSelect = useCallback((seg: ActivityReportSegment) => {
    setSelectedSegmentId((prev) =>
      prev === seg.segment_id ? null : seg.segment_id,
    );
  }, []);

  // The course is a card too (Bug 004-006-T07-1), so selection resolves
  // against the segments list first, then the course row.
  const selectedSegment =
    report?.segments.find((s) => s.segment_id === selectedSegmentId) ??
    (report?.course?.segment_id === selectedSegmentId ? report.course : undefined);

  const reportTitle = report?.course?.name ?? report?.header.activity_name ?? 'Recent Activity';

  const goNewer = useCallback(() => {
    const nextId = report?.nav.next_activity_id;
    if (nextId != null) fetchReport(nextId);
  }, [report, fetchReport]);

  const goOlder = useCallback(() => {
    const prevId = report?.nav.prev_activity_id;
    if (prevId != null) fetchReport(prevId);
  }, [report, fetchReport]);

  // Leaderboard screen (004-006 T08 / AC-8): a selected card replaces the
  // whole report view with the embedded leaderboard; X clears the selection
  // and returns. LeaderboardTable resets its per-segment state whenever it
  // remounts with a new segmentId (built-in effect), so screen changes never
  // leak range/type/selection state across segments.
  const closeLeaderboard = useCallback(() => setSelectedSegmentId(null), []);
  if (selectedSegment) {
    return (
      <div className="space-y-4">
        <LeaderboardScreen segment={selectedSegment} onClose={closeLeaderboard} />
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Title + prev/next nav (AC-1/AC-2) */}
      <div className="flex items-center justify-between gap-2">
        <h2 className="min-w-0 flex-1 truncate text-lg font-semibold text-gray-900 dark:text-white">
          {loading ? 'Loading…' : reportTitle}
        </h2>
        <div className="flex shrink-0 gap-2" role="group" aria-label="Activity navigation">
          <button
            type="button"
            onClick={goOlder}
            disabled={loading || report?.nav.prev_activity_id == null}
            aria-label="Older activity"
            className="w-11 h-11 flex items-center justify-center text-lg font-medium rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-700 disabled:opacity-40 disabled:cursor-not-allowed focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            ◀
          </button>
          <button
            type="button"
            onClick={goNewer}
            disabled={loading || report?.nav.next_activity_id == null}
            aria-label="Newer activity"
            className="w-11 h-11 flex items-center justify-center text-lg font-medium rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-700 disabled:opacity-40 disabled:cursor-not-allowed focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            ▶
          </button>
        </div>
      </div>

      {/* Loading */}
      {loading && (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-6 text-center">
          <p className="text-sm text-gray-500 dark:text-gray-400">
            Loading activity report…
          </p>
        </div>
      )}

      {/* Error */}
      {error && !loading && (
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-4">
          <p className="text-sm font-medium text-red-800 dark:text-red-200">
            Error loading report
          </p>
          <p className="mt-1 text-sm text-red-700 dark:text-red-300">{error}</p>
        </div>
      )}

      {/* Empty */}
      {report === null && !loading && !error && (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-6 text-center">
          <p className="text-sm text-gray-500 dark:text-gray-400">
            No recent activity found.
          </p>
        </div>
      )}

      {/* Report */}
      {report && !loading && (
        <div className="space-y-4">
          {/* Start line (009-006): italic, low emphasis — beneath the title/nav */}
          <p className="text-sm italic text-gray-500 dark:text-gray-400">
            {formatStartShort(report.header.start_utc)}
          </p>
          {/* Course-only info bar (AC-3) */}
          {report.course && <CourseInfoBar course={report.course} />}
          {/* Header band (004-006 T05): 2-column flex row — (1) stats text,
              (2) course plot filling the removed elevation slot. */}
          <div className="w-full flex flex-nowrap items-stretch gap-4">
            {/* (1) distance / time / pace text stats — view display strings
                (1-decimal), fixed height, vertically centered */}
            <div className="min-w-0 flex-1 h-28 flex flex-col justify-center text-xl font-semibold text-gray-900 dark:text-white space-y-0.5">
              <p>{report.header.distance_display}</p>
              <p>{report.header.total_time_display}</p>
              <p>{report.header.pace_display}</p>
            </div>
            {/* (2) activity shape visual — fills the removed elevation slot */}
            <div className="min-w-0 flex-[2] h-28" aria-label="Activity shape container">
              {report.course_path.length > 1 && (
                <CoursePathChart path={report.course_path} />
              )}
            </div>
          </div>

          {/* Heart-rate + elevation stacked chart (004-006 T05): HR full width
              with the elevation area directly beneath at 0 px gap — no axes or
              gridlines anywhere, so the pair reads as a single chart. Both
              x-axes fixed 0 → header.distance_m; y-scales (±10 pads) keep the
              HR line and elevation area from touching. */}
          {report.charts.heartrate.length > 0 && (
            <div className="w-full p-0 m-0 space-y-0 leading-none">
              <HeartrateChart
                points={report.charts.heartrate}
                xMax={report.header.distance_m}
              />
              {report.charts.elevation.length > 0 && (
                <div className="w-full p-0 m-0 leading-none overflow-hidden" style={{ height: 110 }} aria-label="Elevation profile container">
                  <ElevationChart
                    points={report.charts.elevation}
                    xMax={report.header.distance_m}
                  />
                </div>
              )}
            </div>
          )}

          {/* Running-efficiency bullets (004-006 T06): only for run/trail-run
              efforts on a course — backend gates efficiency != null. */}
          {report.efficiency && (
            <EfficiencyBullets
              data={EFFICIENCY_BULLETS.map((b) => ({
                label: b.label,
                metric: report.efficiency![b.key],
                unit: b.unit,
                format: b.format,
                higherBetter: b.higherBetter,
              }))}
            />
          )}

          {/* Course + segments list (004-006 T07, Bug 004-006-T07-1): the
              `{n} segment{s}:` header precedes ALL cards; the course is the
              first card and opens the embedded leaderboard like a segment. */}
          {(report.segments.length > 0 || report.course) && (
            <div>
              <h3 className="text-sm font-semibold text-gray-900 dark:text-white mb-2">
                {formatSegmentCount(
                  report.segments.length + (report.course ? 1 : 0),
                )}
              </h3>
              <div className="space-y-2">
                {report.course && (
                  <SegmentRow
                    key={`course-${report.course.segment_id}`}
                    seg={report.course}
                    selected={selectedSegmentId === report.course.segment_id}
                    onSelect={handleSegmentSelect}
                  />
                )}
                {report.segments.map((seg) => (
                  <SegmentRow
                    key={seg.segment_id}
                    seg={seg}
                    selected={selectedSegmentId === seg.segment_id}
                    onSelect={handleSegmentSelect}
                  />
                ))}
              </div>
            </div>
          )}

          {/* Empty state (004-006 T07 / AC-6): exact text + links to Segment
              Management and Activity Processing. */}
          {!report.has_segments && (
            <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-4 text-center">
              <p className="text-sm text-gray-500 dark:text-gray-400 mb-3">
                No Segments Found.
              </p>
              <div className="flex flex-wrap justify-center gap-2">
                <button
                  type="button"
                  onClick={() => setActiveSubPage('segment-management')}
                  className="px-4 py-2 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
                >
                  Go to Segment Management
                </button>
                <button
                  type="button"
                  onClick={() => setActiveSubPage('activity-processing')}
                  className="px-4 py-2 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 transition-colors"
                >
                  Go to Activity Processing
                </button>
              </div>
            </div>
          )}

          {/* Activity ID footer (FR-11) */}
          <footer className="pt-2 border-t border-gray-200 dark:border-gray-700">
            <p className="text-xs text-gray-400 dark:text-gray-500 text-center">
              ID #{report.activity_id}
            </p>
          </footer>
        </div>
      )}
    </div>
  );
}
