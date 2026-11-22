'use client';

import ThemeToggle from '@/app/components/ThemeToggle';

/**
 * ThemeSettings Component
 * Admin tab hosting the application theme toggle.
 */
export default function ThemeSettings() {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 max-w-md">
      <h2 className="text-lg font-medium text-gray-900 dark:text-white mb-4">Appearance</h2>
      <div className="flex items-center justify-between">
        <span className="text-sm text-gray-600 dark:text-gray-300">Theme</span>
        <ThemeToggle />
      </div>
    </div>
  );
}
