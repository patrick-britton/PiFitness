/**
 * Header Component
 * Top header with brand identity and optional controls
 */

'use client';

import { useEffect, useState } from 'react';
import ThemeToggle from './ThemeToggle';

export default function Header() {
  const [viewportInfo, setViewportInfo] = useState('');

  useEffect(() => {
    if (typeof window !== 'undefined') {
      const updateViewport = () => {
        const width = window.innerWidth;
        const height = window.innerHeight;
        setViewportInfo(`(${width}x${height})`);
      };

      updateViewport();
      window.addEventListener('resize', updateViewport);
      return () => window.removeEventListener('resize', updateViewport);
    }
  }, []);

  return (
    <header className="h-16 flex items-center px-6 bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
      <div className="flex items-center gap-4">
        <h1 className="text-xl font-semibold text-gray-900 dark:text-white">
          PiFitness
          <span className="text-xs font-normal text-gray-500 dark:text-gray-400 ml-2">
            {viewportInfo}
          </span>
        </h1>
      </div>

      {/* Header controls */}
      <div className="flex items-center gap-1 ml-auto">
        <ThemeToggle />
        <button
          onClick={() => {
            if (typeof window !== 'undefined') {
              const currentState = localStorage.getItem('pifitness-debug');
              localStorage.setItem('pifitness-debug', currentState !== 'true' ? 'true' : 'false');
              window.location.reload(); // Force refresh to show/hide debug panel
            }
          }}
          className="p-2 rounded-lg text-gray-600 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
          aria-label="Toggle debug info"
        >
          <span className="text-sm">🐛</span>
        </button>
      </div>
    </header>
  );
}
