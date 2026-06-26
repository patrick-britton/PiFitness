/**
 * Header Component
 * Top header with brand identity and optional controls
 */

'use client';

import { useUIStore } from '../../stores/uiStore';
import { useEffect, useState } from 'react';

export default function Header() {
  const { theme, toggleTheme } = useUIStore();
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
    <header className="h-16 flex items-center justify-between px-6 bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
      <div className="flex items-center gap-4">
        <h1 className="text-xl font-semibold text-gray-900 dark:text-white">
          PiFitness
          <span className="text-xs font-normal text-gray-500 dark:text-gray-400 ml-2">
            {viewportInfo}
          </span>
        </h1>
      </div>

      {/* Theme toggle removed as requested */}
      <div className="flex items-center gap-2">
        {/* Empty div to maintain spacing */}
      </div>
    </header>
  );
}
