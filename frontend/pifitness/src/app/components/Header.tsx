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
          <span className="text-xs font-normal text-gray-500 dark:text-gray-400 ml-2">
            {typeof window !== 'undefined' ? `(${window.innerWidth}x${window.innerHeight})` : ''}
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