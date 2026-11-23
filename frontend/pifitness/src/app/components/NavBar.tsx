/**
 * NavBar Component
 * Adaptive mobile bottom navigation bar with Material UI icons:
 * - On 'home' (root): Renders the 7 primary modules.
 * - Inside a module: Contextually switches to [Home] + that module's sub-pages.
 * Supports three-state rendering: desktop/portrait show labels, landscape icon-only.
 */

'use client';

import { useMemo, useEffect } from 'react';
import { useRouter, usePathname } from 'next/navigation';
import { NAVIGATION_MODULES, MODULE_SUB_PAGES, useUIStore } from '../../stores/uiStore';
import * as Icons from '@mui/icons-material';

interface NavBarProps {
  /** Whether to show text labels alongside icons. False = icon-only (landscape). */
  showLabels?: boolean;
  /** Render vertically (left sidebar) instead of horizontal bottom bar. */
  vertical?: boolean;
}

export default function NavBar({ showLabels = true, vertical = false }: NavBarProps) {
  const router = useRouter();
  const pathname = usePathname();
  const { activeSubPage, setActiveModule, setActiveSubPage } = useUIStore();

  // Derive active module directly from current route path so SSR and client match identically
  const activeModule = useMemo(() => {
    if (!pathname || pathname === '/') return 'home';
    const found = NAVIGATION_MODULES.find(
      (m) => m.id !== 'home' && (pathname === m.path || pathname.startsWith(m.path + '/'))
    );
    return found ? found.id : 'home';
  }, [pathname]);

  // Keep Zustand activeModule in sync with the current route module
  useEffect(() => {
    setActiveModule(activeModule);
  }, [activeModule, setActiveModule]);

  // Get the appropriate Material UI icon for each module or sub-page
  const getIcon = (iconName: string) => {
    const IconComponent = (Icons as any)[iconName];
    return IconComponent ? <IconComponent className="w-5 h-5 sm:w-6 sm:h-6" /> : null;
  };

  const isHome = activeModule === 'home';
  const subPages = !isHome ? (MODULE_SUB_PAGES[activeModule] || []) : [];

  const handleModuleNavigation = (module: typeof NAVIGATION_MODULES[number]) => {
    setActiveModule(module.id);
    setActiveSubPage(null);
    router.push(module.path);
  };

  const handleReturnHome = () => {
    setActiveModule('home');
    setActiveSubPage(null);
    router.push('/');
  };

  const handleSubPageNavigation = (subPageId: string) => {
    setActiveSubPage(subPageId);
    const moduleConfig = NAVIGATION_MODULES.find(m => m.id === activeModule);
    if (moduleConfig && pathname !== moduleConfig.path) {
      router.push(moduleConfig.path);
    }
  };

  return (
    <nav
      className={`bg-white dark:bg-gray-800 ${vertical ? '' : 'border-t border-gray-200 dark:border-gray-700'}`}
      aria-label="Bottom Navigation"
    >
      {isHome ? (
        // Level 1: Home Mode — 7 primary navigation modules
        <div className={vertical ? 'flex flex-col items-center py-2 space-y-2' : 'flex justify-around'}>
          {NAVIGATION_MODULES.map((module) => (
            <button
              key={module.id}
              onClick={() => handleModuleNavigation(module)}
              className={`flex items-center justify-center rounded-lg transition-transform active:scale-95 active:bg-gray-100 dark:active:bg-gray-700/60 select-none ${
                vertical ? 'p-2 w-full min-h-[44px]' : 'flex-col p-2 sm:p-3 flex-1 min-h-[48px]'
              } ${
                activeModule === module.id
                  ? 'text-blue-600 dark:text-blue-300'
                  : 'text-gray-600 dark:text-gray-300'
              }`}
              title={!showLabels ? module.label : undefined}
            >
              <span className={showLabels && !vertical ? 'mb-1' : ''}>{getIcon(module.iconName)}</span>
              {showLabels && <span className="text-[10px] sm:text-xs font-medium truncate">{module.label}</span>}
            </button>
          ))}
        </div>
      ) : (
        // Level 2: Sub-Page Mode — [Home] + Active module's sub-pages
        <div className={vertical ? 'flex flex-col items-center py-2 space-y-1' : 'flex items-center'}>
          {/* Pinned Home / Back button */}
          <button
            onClick={handleReturnHome}
            className={`flex items-center justify-center rounded-lg transition-transform active:scale-95 active:bg-gray-100 dark:active:bg-gray-700/60 select-none ${
              vertical
                ? 'p-2 w-full min-h-[44px] text-gray-700 dark:text-gray-200'
                : 'flex-col py-2 px-3 flex-shrink-0 min-h-[48px] border-r border-gray-200 dark:border-gray-700 text-gray-700 dark:text-gray-300'
            }`}
            title="Return to Home"
            aria-label="Return to Home"
          >
            <span className={showLabels && !vertical ? 'mb-1' : ''}>{getIcon('Home')}</span>
            {showLabels && <span className="text-[10px] sm:text-xs font-semibold">Home</span>}
          </button>

          {vertical && <div className="w-8 border-b border-gray-200 dark:border-gray-700 my-1" />}

          {/* Sub-pages container */}
          <div
            className={
              vertical
                ? 'flex flex-col items-center w-full space-y-1'
                : subPages.length > 5
                ? 'flex-1 flex overflow-x-auto no-scrollbar scroll-smooth py-1 px-1'
                : 'flex-1 flex justify-around py-1'
            }
          >
            {subPages.map((subPage, index) => {
              const isSelected = activeSubPage ? activeSubPage === subPage.id : index === 0;
              return (
                <button
                  key={subPage.id}
                  onClick={() => handleSubPageNavigation(subPage.id)}
                  className={`flex items-center justify-center rounded-lg transition-transform active:scale-95 active:bg-gray-100 dark:active:bg-gray-700/60 select-none ${
                    vertical
                      ? 'p-2 w-full min-h-[44px]'
                      : subPages.length > 5
                      ? 'flex-col py-1 px-2.5 flex-shrink-0 min-w-[62px] min-h-[48px]'
                      : 'flex-col py-1 px-1 flex-1 min-h-[48px]'
                  } ${
                    isSelected
                      ? 'text-blue-600 dark:text-blue-400 font-semibold'
                      : 'text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200'
                  }`}
                  title={!showLabels ? subPage.label : undefined}
                >
                  <span className={showLabels && !vertical ? 'mb-0.5' : ''}>{getIcon(subPage.iconName)}</span>
                  {showLabels && (
                    <span className="text-[10px] sm:text-xs tracking-tight truncate max-w-[70px]">
                      {subPage.shortLabel || subPage.label}
                    </span>
                  )}
                </button>
              );
            })}
          </div>
        </div>
      )}
    </nav>
  );
}
