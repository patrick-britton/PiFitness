/**
 * Header Component
 * Top header with brand identity and optional controls
 */

'use client';

import { useUIStore } from '../../stores/uiStore';

export default function Header() {
  const { theme, toggleTheme } = useUIStore();

  return (
    <header className="h-16 flex items-center justify-between px-6 bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
      <div className="flex items-center gap-4">
        <h1 className="text-xl font-semibold text-gray-900 dark:text-white">
          PiFitness
        </h1>
      </div>
      
      <div className="flex items-center gap-2">
        <button
          onClick={toggleTheme}
          className="p-2 rounded-lg text-gray-600 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
          aria-label="Toggle theme"
        >
          {theme === 'light' ? (
            <span className="text-lg">🌙</span>
          ) : (
            <span className="text-lg">☀️</span>
          )}
        </button>
      </div>
    </header>
  );
}