/**
 * NavBar Component
 * Mobile bottom navigation bar with Material UI icons
 * Supports three-state rendering: desktop/portrait show labels, landscape icon-only.
 */

'use client';

import { useRouter } from 'next/navigation';
import { NAVIGATION_MODULES } from '../../stores/uiStore';
import { useUIStore } from '../../stores/uiStore';
import * as Icons from '@mui/icons-material';

interface NavBarProps {
  /** Whether to show text labels alongside icons. False = icon-only (landscape). */
  showLabels?: boolean;
  /** Render vertically (left sidebar) instead of horizontal bottom bar. */
  vertical?: boolean;
}

export default function NavBar({ showLabels = true, vertical = false }: NavBarProps) {
  const router = useRouter();
  const { activeModule, setActiveModule, setActiveSubPage } = useUIStore();

  // Get the appropriate Material UI icon for each module
  const getIcon = (iconName: string) => {
    const IconComponent = (Icons as any)[iconName];
    return IconComponent ? <IconComponent className="w-6 h-6" /> : null;
  };

  const handleNavigation = (module: typeof NAVIGATION_MODULES[number]) => {
    setActiveModule(module.id);
    setActiveSubPage(null);
    router.push(module.path);
  };

  return (
    <nav className={`bg-white dark:bg-gray-800 ${vertical ? '' : 'border-t border-gray-200 dark:border-gray-700'}`}>
      <div className={vertical ? 'flex flex-col items-center py-2 space-y-2' : 'flex justify-around'}>
        {NAVIGATION_MODULES.map((module) => (
          <button
            key={module.id}
            onClick={() => handleNavigation(module)}
            className={`flex items-center justify-center ${
              vertical ? 'p-2 w-full' : 'flex-col p-3 flex-1'
            } ${
              activeModule === module.id
                ? 'text-blue-600 dark:text-blue-300'
                : 'text-gray-600 dark:text-gray-300'
            }`}
            title={!showLabels ? module.label : undefined}
          >
            <span className={showLabels && !vertical ? 'mb-1' : ''}>{getIcon(module.iconName)}</span>
            {showLabels && <span className="text-xs">{module.label}</span>}
          </button>
        ))}
      </div>
    </nav>
  );
}
