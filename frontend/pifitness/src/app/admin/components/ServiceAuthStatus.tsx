/**
 * ServiceAuthStatus Component
 * Displays clickable status indicators for Spotify and Garmin authentication.
 * Shows Red/Yellow/Green badges based on token status.
 * No auto-trigger on page load - purely informational with user-initiated actions.
 */

'use client';

import { useState } from 'react';
import { useAuthStatus, useTestSpotifyAuth, useTestGarminAuth } from '@/hooks/useAuth';

/**
 * Status badge component that displays the current auth state.
 */
function StatusBadge({
  service,
  status,
  onClick,
}: {
  service: 'Spotify' | 'Garmin';
  status: 'ok' | 'expired' | 'rate_limited' | 'error' | 'unknown';
  onClick: () => void;
}) {
  // Determine color based on status
  let bgColor = 'bg-gray-400';
  let textColor = 'text-white';
  let label = 'Unknown';

  if (status === 'ok') {
    bgColor = 'bg-green-500';
    label = 'Valid Token';
  } else if (status === 'rate_limited') {
    bgColor = 'bg-red-500';
    label = 'Rate Limited';
  } else if (status === 'expired' || status === 'error') {
    bgColor = 'bg-yellow-500';
    label = status === 'expired' ? 'Expired' : 'Error';
  } else if (status === 'unknown') {
    bgColor = 'bg-gray-500';
    label = 'Unknown';
  }

  return (
    <button
      onClick={onClick}
      className={`flex items-center gap-2 px-3 py-1.5 rounded-md text-sm font-medium transition-all hover:opacity-80 ${bgColor} ${textColor}`}
      title={`Click to test ${service} connection`}
    >
      <span className="w-2 h-2 rounded-full bg-current" />
      {service}: {label}
    </button>
  );
}

/**
 * ServiceAuthStatus Component
 * Shows status indicators at the top of the Services tab in Admin module.
 */
export default function ServiceAuthStatus() {
  const [showTestResult, setShowTestResult] = useState<{
    service: 'Spotify' | 'Garmin';
    result: string;
  } | null>(null);

  // Only fetch when explicitly enabled (user is viewing Services tab)
  const { data, isLoading, error, refetch } = useAuthStatus(true);
  const testSpotify = useTestSpotifyAuth();
  const testGarmin = useTestGarminAuth();

  // Get status from the response
  const spotifyStatus = data?.services?.Spotify?.status || 'unknown';
  const garminStatus = data?.services?.Garmin?.status || 'unknown';

  const handleTestSpotify = () => {
    testSpotify.mutate(undefined, {
      onSuccess: (response) => {
        setShowTestResult({ service: 'Spotify', result: response.message || response.status });
        // Refresh status after test
        refetch();
      },
      onError: (err: any) => {
        setShowTestResult({ service: 'Spotify', result: err.message || 'Test failed' });
      },
    });
  };

  const handleTestGarmin = () => {
    testGarmin.mutate(undefined, {
      onSuccess: (response) => {
        setShowTestResult({ service: 'Garmin', result: response.message || response.status });
        // Refresh status after test
        refetch();
      },
      onError: (err: any) => {
        setShowTestResult({ service: 'Garmin', result: err.message || 'Test failed' });
      },
    });
  };

  const handleRefresh = () => {
    refetch();
  };

  return (
    <div className="mb-6 p-4 bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-lg font-medium text-gray-900 dark:text-white">
          Service Authentication Status
        </h3>
        <button
          onClick={handleRefresh}
          disabled={isLoading}
          className="px-3 py-1.5 text-sm font-medium text-gray-600 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 rounded-md hover:bg-gray-200 dark:hover:bg-gray-600 disabled:opacity-50"
        >
          {isLoading ? 'Refreshing...' : 'Refresh Status'}
        </button>
      </div>

      <div className="flex flex-wrap gap-3">
        <StatusBadge
          service="Spotify"
          status={spotifyStatus as any}
          onClick={handleTestSpotify}
        />
        <StatusBadge
          service="Garmin"
          status={garminStatus as any}
          onClick={handleTestGarmin}
        />
      </div>

      {/* Test Result Toast */}
      {showTestResult && (
        <div className="mt-3 p-3 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-md">
          <p className="text-sm text-blue-700 dark:text-blue-300">
            <strong>{showTestResult.service} Test Result:</strong> {showTestResult.result}
          </p>
        </div>
      )}

      {/* Error Display */}
      {error && (
        <div className="mt-3 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
          <p className="text-sm text-red-700 dark:text-red-300">
            Failed to load auth status: {String(error)}
          </p>
        </div>
      )}

      {/* Token Expiry Info */}
      {data?.services?.Spotify?.token_expires_utc && (
        <p className="mt-2 text-xs text-gray-500 dark:text-gray-400">
          Spotify token expires: {data.services.Spotify.token_expires_utc}
        </p>
      )}
    </div>
  );
}