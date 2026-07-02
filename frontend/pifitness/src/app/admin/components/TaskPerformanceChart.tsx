/**
 * Task Performance Chart Component
 * Displays vertical stacked bar chart showing task execution times over time.
 * Each bar represents different timing categories with specific colors.
 */
'use client';

import { useMemo } from 'react';
import { Bar } from 'react-chartjs-2';
import { Chart as ChartJS, CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend, TimeScale, TimeSeriesScale, LinearScale as LinearScaleType } from 'chart.js';
import 'chartjs-adapter-date-fns';
import { useTaskPerformance } from '@/hooks/useAdmin';

// Register ChartJS components
ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend, TimeScale, TimeSeriesScale);

/**
 * Color scheme for different timing categories (from requirements)
 */
const TIMING_COLORS = {
  login_ms: 'rgba(200, 200, 200, 0.8)', // light gray
  extract_ms: 'rgba(100, 100, 100, 0.8)', // dark gray
  load_ms: 'rgba(255, 140, 0, 0.8)', // dark orange
  flatten_ms: 'rgba(255, 180, 100, 0.8)', // light orange
  parse_ms: 'rgba(100, 200, 255, 0.8)', // light blue
  interpolation_ms: 'rgba(0, 100, 200, 0.8)', // dark blue
  forecasting_ms: 'rgba(0, 80, 180, 0.8)', // dark blue
  python_ms: 'rgba(0, 150, 200, 0.8)', // blue
  admin_ms: 'rgba(220, 220, 220, 0.8)', // light gray
};

/**
 * Order of timing categories (from bottom to top as specified in requirements)
 */
const TIMING_ORDER = [
  'login_ms',
  'extract_ms',
  'load_ms',
  'flatten_ms',
  'parse_ms',
  'interpolation_ms',
  'forecasting_ms',
  'python_ms',
  'admin_ms'
];

/**
 * Task Performance Chart Component
 */
export default function TaskPerformanceChart({ taskId }: { taskId: number }) {
  const { data: performanceData, isLoading, error } = useTaskPerformance(taskId);

  // Extract and sort data for use in both chart data and options
  const sortedData = useMemo(() => {
    if (!performanceData?.data || performanceData.data.length === 0) {
      return null;
    }
    return [...performanceData.data].sort((a, b) =>
      new Date(a.event_date).getTime() - new Date(b.event_date).getTime()
    );
  }, [performanceData]);

  const chartData = useMemo(() => {
    if (!sortedData || sortedData.length === 0) {
      return null;
    }

    // Prepare datasets for each timing category
    const datasets = TIMING_ORDER
      .filter(category => {
        // Only include categories that have at least one non-null value
        return sortedData.some(item => item[category] !== null && item[category] !== undefined);
      })
      .map(category => ({
        label: category.replace('_ms', '').replace(/_/g, ' '),
        data: sortedData.map(item => ({
          x: item.event_date,
          y: item[category] || 0
        })),
        backgroundColor: TIMING_COLORS[category as keyof typeof TIMING_COLORS],
        borderColor: TIMING_COLORS[category as keyof typeof TIMING_COLORS],
        borderWidth: 1,
        stack: 'stack',
      }));

    return {
      datasets,
    };
  }, [sortedData]);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-8">
        <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600" />
        <span className="ml-2 text-sm text-gray-600 dark:text-gray-400">Loading performance data...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-4">
        <p className="text-red-700 dark:text-red-300 text-sm">
          Failed to load performance data: {String(error)}
        </p>
      </div>
    );
  }

  if (!chartData || chartData.datasets.length === 0) {
    return (
      <div className="bg-gray-50 dark:bg-gray-800 rounded-md p-6 text-center">
        <p className="text-gray-500 dark:text-gray-400 text-sm">
          No performance data available for this task.
        </p>
      </div>
    );
  }

  // Calculate time range: current timestamp - 30 days to current timestamp
  const now = new Date();
  const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'bottom' as const,
        labels: {
          boxWidth: 12,
          padding: 10,
          font: {
            size: 10,
          },
          usePointStyle: true,
        },
      },
      title: {
        display: false,
      },
      tooltip: {
        callbacks: {
          label: (context: any) => {
            const label = context.dataset.label || '';
            const value = context.raw?.y || 0;
            return `${label}: ${value}ms`;
          },
          afterLabel: (context: any) => {
            const dataPoint = context.raw;
            if (dataPoint?.is_failure) {
              return '⚠️ This interval had failures';
            }
            return '';
          },
        },
      },
    },
    scales: {
      x: {
        type: 'time' as const,
        time: {
          unit: 'day' as const,
          tooltipFormat: 'MMM dd, yyyy HH:mm',
          displayFormats: {
            day: 'MMM dd, yyyy'
          }
        },
        title: {
          display: true,
          text: 'Time Interval (30-minute buckets)',
          font: {
            size: 11,
          },
        },
        grid: {
          display: false,
        },
        ticks: {
          maxRotation: 45,
          minRotation: 45,
          font: {
            size: 10,
          },
          callback: function(value: any) {
            // Convert UTC timestamp to local time
            const date = new Date(value);
            return date.toLocaleString();
          }
        },
        min: thirtyDaysAgo.toISOString(),
        max: now.toISOString(),
      },
      y: {
        title: {
          display: true,
          text: 'Execution Time (ms)',
          font: {
            size: 11,
          },
        },
        beginAtZero: true,
        grid: {
          color: 'rgba(0, 0, 0, 0.05)',
        },
        ticks: {
          font: {
            size: 10,
          },
        },
      },
    },
  };

  return (
    <div className="mt-6 pt-6 border-t border-gray-200 dark:border-gray-700">
      <h4 className="text-sm font-medium text-gray-900 dark:text-white mb-3">
        Task Execution Performance Trend
      </h4>
      <div className="bg-white dark:bg-gray-900 rounded-lg p-4 shadow-sm border border-gray-200 dark:border-gray-700">
        <div className="h-96">
          <Bar data={chartData} options={chartOptions} />
        </div>
      </div>
      <div className="mt-3 text-xs text-gray-500 dark:text-gray-400">
        <p>Each stacked bar represents execution time breakdown for a 30-minute interval.</p>
      </div>
      </div>
    );
  }
