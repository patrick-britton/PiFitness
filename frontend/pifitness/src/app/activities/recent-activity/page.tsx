'use client';

import { useState, useEffect, useCallback } from 'react';
import { useViewportStore } from '@/stores/viewportStore';
import { useUIStore } from '@/stores/uiStore';
import { API } from '@/lib/api-client';
import {
  ActivityReport,
  ActivityReportType,
  ActivityReportSegment,
} from '@/lib/types/activity-report';

/**
 * Recent Activity Report page (009-001).
 * Run/Walk selection (default Run) loads the most recent activity of that type
 * and renders a summary header, an optional course section, and the list of
 * crossed segments. Layout-aware for desktop / portrait / landscape and themed
 * via Tailwind dark: tokens (design-system.md).
 */

/** Format a delta in seconds as "+Xs slower" / "-Xs faster" / "—". */
function formatDelta(delta: number | null): string {
  if (delta == null) return '—';
  if (delta === 0) return '0s';
  const sign = delta > 0 ? '+' : '';
  const label = delta > 0 ? 'slower' : 'faster';
  return `${sign}${delta}s ${label}`;
}

/** Render an ISO-8601 UTC timestamp as a local-time string. */
function formatStart(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleString();
}

/** Format "A/B" rank; emits "—" when rank is missing. */
function formatRank(rank: number | null, total: number): string {
  if (rank == null) return '—';
  return `${rank}/${total}`;
}

function Stat({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div className="bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-3">
      <p className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">
        {label}
      </p>
      <p className="mt-1 text-lg font-semibold text-gray-900 dark:text-white">
        {value}
      </p>
    </div>
  );
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
        Prior: {formatDelta(seg.prior_delta_s)} · Best:{' '}
        {formatDelta(seg.best_delta_s)}
      </p>
    </button>
  );
}

export default function RecentActivityPage() {
  const { layoutVariant } = useViewportStore();
  const { setActiveSubPage } = useUIStore();
  const isLandscape = layoutVariant === 'landscape';

  const [activityType, setActivityType] = useState<ActivityReportType>('Run');
  const [report, setReport] = useState<ActivityReport | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedSegmentId, setSelectedSegmentId] = useState<number | null>(
    null,
  );

  const fetchReport = useCallback(async (type: ActivityReportType) => {
    setLoading(true);
    setError(null);
    setReport(null);
    setSelectedSegmentId(null);
    try {
      const data = await API.activities.getReport(type);
      setReport(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load report');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchReport(activityType);
  }, [activityType, fetchReport]);

  const resetToSelection = useCallback(() => {
    setReport(null);
    setSelectedSegmentId(null);
    setError(null);
  }, []);

  const handleSegmentSelect = useCallback((seg: ActivityReportSegment) => {
    setSelectedSegmentId((prev) =>
      prev === seg.segment_id ? null : seg.segment_id,
    );
  }, []);

  const selectedSegment = report?.segments.find(
    (s) => s.segment_id === selectedSegmentId,
  );
  const leaderboardName = selectedSegment
    ? selectedSegment.name
    : report?.course?.name;

  const handleTypeChange = (type: ActivityReportType) => {
    setActivityType(type);
  };

  return (
    <div className="space-y-4">
      {/* Run / Walk selector */}
      <div
        className={`flex ${isLandscape ? 'gap-2' : 'gap-3'}`}
        role="tablist"
        aria-label="Activity type"
      >
        {(['Run', 'Walk'] as ActivityReportType[]).map((type) => (
          <button
            key={type}
            role="tab"
            aria-selected={activityType === type}
            onClick={() => handleTypeChange(type)}
            className={`flex-1 ${
              isLandscape ? 'py-1.5 text-sm' : 'py-2 text-sm'
            } font-medium rounded-md border transition-colors ${
              activityType === type
                ? 'bg-blue-600 text-white border-blue-600'
                : 'bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-200 border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700'
            }`}
          >
            {type}
          </button>
        ))}
      </div>

      {/* Loading */}
      {loading && (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-6 text-center">
          <p className="text-sm text-gray-500 dark:text-gray-400">
            Loading {activityType.toLowerCase()} report…
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
            No recent {activityType.toLowerCase()} activity found.
          </p>
        </div>
      )}

      {/* Report */}
      {report && !loading && (
        <div className="space-y-4">
          {/* Controls row: reset */}
          <div className="flex justify-end">
            <button
              type="button"
              onClick={resetToSelection}
              className="px-3 py-1.5 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
            >
              Reset
            </button>
          </div>
          {/* Summary header */}
          <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-4">
            <h2 className="text-lg font-bold text-gray-900 dark:text-white">
              {activityType} Summary
            </h2>
            <div
              className={`mt-3 grid gap-3 ${
                isLandscape
                  ? 'grid-cols-3'
                  : layoutVariant === 'desktop'
                  ? 'grid-cols-3 sm:grid-cols-4'
                  : 'grid-cols-2'
              }`}
            >
              <Stat label="Start" value={formatStart(report.header.start_utc)} />
              <Stat
                label="Distance"
                value={`${report.header.distance_mi.toFixed(2)} mi`}
              />
              <Stat label="Time" value={report.header.total_time_text} />
              <Stat label="Pace" value={report.header.pace_text} />
              <Stat
                label="Median HR"
                value={
                  report.header.hr_median != null
                    ? `${report.header.hr_median}`
                    : '—'
                }
              />
              <Stat
                label="75th % HR"
                value={
                  report.header.hr_p75 != null ? `${report.header.hr_p75}` : '—'
                }
              />
              <Stat
                label="Max HR"
                value={
                  report.header.hr_max != null ? `${report.header.hr_max}` : '—'
                }
              />
            </div>
          </div>

          {/* Running-efficiency placeholder — Run/Trail only */}
          {report.header.show_efficiency_placeholder && (
            <div className="bg-white dark:bg-gray-800 border border-dashed border-gray-300 dark:border-gray-600 rounded-md p-4 text-center">
              <p className="text-sm text-gray-500 dark:text-gray-400">
                Running efficiency metrics (coming soon)
              </p>
            </div>
          )}

          {/* Course section — collapse when a segment is selected */}
          {report.course && !selectedSegment && (
            <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-4">
              <div className="flex items-center justify-between gap-2">
                <h3 className="text-base font-semibold text-gray-900 dark:text-white">
                  {report.course.name}
                </h3>
                <span className="text-sm font-medium text-blue-600 dark:text-blue-400">
                  {formatRank(
                    report.course.all_time_rank,
                    report.course.total_attempts,
                  )}
                </span>
              </div>
              <p className="mt-1 text-sm text-gray-600 dark:text-gray-300">
                Prior: {formatDelta(report.course.prior_delta_s)} · Best:{' '}
                {formatDelta(report.course.best_delta_s)}
              </p>
            </div>
          )}

          {/* Segments list */}
          {report.segments.length > 0 && (
            <div>
              <h3 className="text-sm font-semibold text-gray-900 dark:text-white mb-2">
                Segments
              </h3>
              <div className="space-y-2">
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

          {/* Leaderboard placeholder region (FR-7) — only when a course or segment exists */}
          {report.has_segments && leaderboardName && (
            <div className="bg-white dark:bg-gray-800 border border-dashed border-gray-300 dark:border-gray-600 rounded-md p-4 text-center">
              <p className="text-sm text-gray-500 dark:text-gray-400">
                {selectedSegment
                  ? `placeholder for ${leaderboardName} leaderboard`
                  : `place holder for ${leaderboardName} leaderboard`}
              </p>
            </div>
          )}

          {/* Nav button to Activity Processing when no course/segments (FR-5) */}
          {!report.has_segments && (
            <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-4 text-center">
              <p className="text-sm text-gray-500 dark:text-gray-400 mb-3">
                No course or segments detected for this activity.
              </p>
              <button
                type="button"
                onClick={() => setActiveSubPage('activity-processing')}
                className="px-4 py-2 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 transition-colors"
              >
                Go to Activity Processing
              </button>
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
