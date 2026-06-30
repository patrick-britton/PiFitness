/**
 * DB Size View Component
 * Displays database size historical growth and current breakdown charts.
 * Uses Chart.js for visualization (per design doc).
 */

'use client';

import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { Bar } from 'react-chartjs-2';
import { API } from '@/lib/api-client';

// Register Chart.js components
ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend);

interface DbSizeChartRow {
  date_utc: string;
  table_size_mb: number | null;
  index_size_mb: number | null;
  other_size_mb: number | null;
}

interface DbSizeBreakdownRow {
  table_name: string;
  table_size_mb: number | null;
  index_size_mb: number | null;
  other_size_mb: number | null;
  total_size_mb: number | null;
}

function buildHistoricalChartData(rows: DbSizeChartRow[]) {
  if (!rows.length) return null;
  const labels = rows.map((row) => {
    try {
      return new Date(row.date_utc).toLocaleDateString(undefined, {
        month: 'short',
        day: 'numeric',
      });
    } catch {
      return row.date_utc;
    }
  });
  const tableSize = rows.map((row) => row.table_size_mb ?? 0);
  const indexSize = rows.map((row) => row.index_size_mb ?? 0);
  const otherSize = rows.map((row) => row.other_size_mb ?? 0);
  return {
    labels,
    datasets: [
      { label: 'Table Size (MB)', data: tableSize, backgroundColor: 'rgba(59, 130, 246, 0.8)' },
      { label: 'Index Size (MB)', data: indexSize, backgroundColor: 'rgba(240, 105, 15, 0.8)' },
      { label: 'Other Size (MB)', data: otherSize, backgroundColor: 'rgba(183, 183, 183, 0.8)' },
    ],
  };
}

function buildBreakdownChartData(rows: DbSizeBreakdownRow[]) {
  if (!rows.length) return null;
  const labels = rows.map((row) => row.table_name);
  const tableSize = rows.map((row) => row.table_size_mb ?? 0);
  const indexSize = rows.map((row) => row.index_size_mb ?? 0);
  const otherSize = rows.map((row) => row.other_size_mb ?? 0);
  return {
    labels,
    datasets: [
      { label: 'Table Size (MB)', data: tableSize, backgroundColor: 'rgba(59, 130, 246, 0.8)' },
      { label: 'Index Size (MB)', data: indexSize, backgroundColor: 'rgba(240, 105, 15, 0.8)' },
      { label: 'Other Size (MB)', data: otherSize, backgroundColor: 'rgba(183, 183, 183, 0.8)' },
    ],
  };
}

export default function DbSizeView() {
  const chart = useQuery({
    queryKey: ['admin', 'db-size-chart'],
    queryFn: () => API.admin.getDbSizeChart(),
    staleTime: 60_000,
  });

  const breakdown = useQuery({
    queryKey: ['admin', 'db-size-breakdown'],
    queryFn: () => API.admin.getDbSizeBreakdown(),
    staleTime: 60_000,
  });

  const chartData = useMemo<DbSizeChartRow[]>(() => chart.data?.data ?? [], [chart.data]);
  const breakdownData = useMemo<DbSizeBreakdownRow[]>(
    () => breakdown.data?.data ?? [],
    [breakdown.data]
  );

  const now = new Date();
  const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

  const last30Days = useMemo(() => {
    return chartData.filter((row) => new Date(row.date_utc) >= thirtyDaysAgo);
  }, [chartData]);

  const last12Months = useMemo(() => {
    const start = new Date(thirtyDaysAgo.getTime() - 12 * 30 * 24 * 60 * 60 * 1000);
    const filtered = chartData.filter((row) => new Date(row.date_utc) >= start);

    const months: DbSizeChartRow[] = [];
    for (const row of filtered) {
      const d = new Date(row.date_utc);
      const targetDay = d.getDate() <= 5 ? d.getDate() : null;
      const exists = months.some((m) => {
        const md = new Date(m.date_utc);
        return md.getFullYear() === d.getFullYear() && md.getMonth() === d.getMonth();
      });
      if (targetDay && !exists) {
        months.push(row);
      }
    }
    return months;
  }, [chartData]);

  const chart30 = useMemo(() => buildHistoricalChartData(last30Days), [last30Days]);
  const chart12 = useMemo(() => buildHistoricalChartData(last12Months), [last12Months]);

  const breakdownChart = useMemo(() => buildBreakdownChartData(breakdownData), [breakdownData]);

  const isDark = typeof document !== 'undefined' && document.documentElement.classList.contains('dark');
  const gridColor = isDark ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.1)';
  const textColor = isDark ? '#e5e7eb' : '#0f172a';

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        labels: {
          color: textColor,
        },
      },
      tooltip: {
        mode: 'index' as const,
        intersect: false,
      },
    },
    scales: {
      x: {
        stacked: true,
        ticks: { color: textColor },
        grid: { color: gridColor },
      },
      y: {
        stacked: true,
        ticks: { color: textColor },
        grid: { color: gridColor },
      },
    },
  };

  const breakdownOptions = {
    ...options,
    indexAxis: 'y' as const,
    scales: {
      x: {
        stacked: true,
        ticks: { color: textColor },
        grid: { color: gridColor },
      },
      y: {
        stacked: true,
        ticks: { color: textColor },
        grid: { color: gridColor },
      },
    },
  };

  if (chart.isLoading || breakdown.isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
      </div>
    );
  }

  if (chart.error || breakdown.error) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-4">
        <p className="text-red-700 dark:text-red-300">
          Failed to load DB size data. Please try again later.
        </p>
      </div>
    );
  }

  if (!chart30 && !chart12) {
    return (
      <div className="bg-gray-50 dark:bg-gray-800 rounded-md p-6 text-center">
        <p className="text-gray-500 dark:text-gray-400">No database size data available.</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">
          Database Size Overview
        </h2>

        <div className="flex flex-col lg:flex-row lg:gap-6 lg:max-h-[740px]">
          <div className="w-full lg:w-3/5 flex flex-col gap-6">
            <div className="bg-white dark:bg-gray-900 rounded-lg shadow-sm p-4 h-[340px]">
              <h3 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">
                Last 30 Days
              </h3>
              <div className="h-[292px]">
                {chart30 ? <Bar data={chart30} options={options} /> : <p className="text-sm text-gray-500 dark:text-gray-400">No recent data.</p>}
              </div>
            </div>
            <div className="bg-white dark:bg-gray-900 rounded-lg shadow-sm p-4 h-[340px]">
              <h3 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">
                Last 12 Months
              </h3>
              <div className="h-[292px]">
                {chart12 ? <Bar data={chart12} options={options} /> : <p className="text-sm text-gray-500 dark:text-gray-400">No monthly data.</p>}
              </div>
            </div>
          </div>

          <div className="w-full lg:w-2/5">
            <div className="bg-white dark:bg-gray-900 rounded-lg shadow-sm p-4 lg:h-[700px]">
              <h3 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">
                Current Breakdown
              </h3>
              <div className="h-[652px]">
                {breakdownChart ? <Bar data={breakdownChart} options={breakdownOptions} /> : <p className="text-sm text-gray-500 dark:text-gray-400">No breakdown data.</p>}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}