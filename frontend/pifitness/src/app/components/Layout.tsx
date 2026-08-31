/**
 * Main Layout Component
 * Responsive shell that switches between three states:
 * - desktop:  sidebar navigation (width ≥ 1024px)
 * - portrait: bottom navigation (width < 1024px, orientation portrait)
 * - landscape: left sidebar, icon-only navigation (width < 1024px, orientation landscape)
 */

'use client';

import { useEffect } from 'react';
import { usePathname } from 'next/navigation';
import { useViewportStore } from '../../stores/viewportStore';
import { useUIStore } from '../../stores/uiStore';
import Header from './Header';
import Sidebar from './Sidebar';
import NavBar from './NavBar';
import DebugPanel from './DebugPanel';

interface LayoutProps {
  children: React.ReactNode;
}

export default function Layout({ children }: LayoutProps) {
  const { initialize: initializeViewport, setViewport, layoutMode, layoutVariant } = useViewportStore();
  const { initialize: initializeUI, syncActiveModuleFromPath, syncActiveSubPageFromPath } = useUIStore();
  // Kiosk routes (006-002): the unlisted /beachchanger viewer renders without
  // the app chrome — no header, sidebar, or nav. DebugPanel intentionally
  // stays (dev-only toggle). Every other route is unchanged.
  const isKiosk = usePathname() === '/beachchanger';

  // Initialize stores on mount
  useEffect(() => {
    initializeViewport();
    initializeUI();
    // Sync navigation state from URL path on initial load
    syncActiveModuleFromPath();
    syncActiveSubPageFromPath();

    const handleResize = () => {
      // Only track the real window in native mode; forced modes keep their
      // simulated dimensions until the user returns to native.
      if (useViewportStore.getState().layoutMode === 'native') {
        setViewport(window.innerWidth, window.innerHeight);
      }
    };

    // Set initial size
    handleResize();

    // Add event listener
    window.addEventListener('resize', handleResize);

    // Cleanup
    return () => window.removeEventListener('resize', handleResize);
  }, [initializeViewport, initializeUI, setViewport, syncActiveModuleFromPath, syncActiveSubPageFromPath]);

  const { width, height } = useViewportStore();

  const isDesktop = layoutVariant === 'desktop';
  const isPortrait = layoutVariant === 'portrait';
  const isLandscape = layoutVariant === 'landscape';

  // In a forced mobile mode, render a centered phone frame so the simulated
  // viewport is visually obvious (addresses Bug 001.001 / developer toggle).
  const forcedMobile = layoutMode !== 'native';

  return (
    <div className="flex flex-col min-h-screen bg-gray-50 dark:bg-gray-900">
      {/* Header (hidden on kiosk routes) */}
      {!isKiosk && <Header />}

      {/* Main content area */}
      <div className="flex flex-1 overflow-hidden">
        {/* Sidebar for desktop - always visible */}
        {!isKiosk && isDesktop && (
          <div className="w-64 flex-shrink-0 border-r border-gray-200 dark:border-gray-700">
            <Sidebar />
          </div>
        )}

        {/* Left sidebar nav for landscape - icon-only, no text */}
        {!isKiosk && isLandscape && (
          <div className="w-16 flex-shrink-0 border-r border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800">
            <NavBar showLabels={false} vertical />
          </div>
        )}

        {/* Main content - add bottom padding on portrait so it clears the pinned nav */}
        <main className={`flex-1 overflow-auto ${isPortrait && !isKiosk ? 'pb-20' : ''}`}>
          {children}
        </main>
      </div>

      {/* Bottom navigation for portrait - pinned so the OS browser chrome cannot
          push it off-screen, with safe-area padding for the gesture bar */}
      {!isKiosk && isPortrait && (
        <div className="fixed bottom-0 inset-x-0 z-40 border-t border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 pb-[env(safe-area-inset-bottom)]">
          <NavBar showLabels={true} />
        </div>
      )}

      {/* Debug Panel - always rendered but conditionally visible */}
      <DebugPanel />

      {/* Centered device frame when a mobile layout is forced via the dev toggle */}
      {forcedMobile && (
        <div className="pointer-events-none fixed inset-0 z-30 flex items-center justify-center">
          <div className="border-4 border-gray-500 rounded-[2rem] shadow-2xl overflow-hidden bg-black/5">
            <div
              className="bg-gray-50 dark:bg-gray-900"
              style={{ width: `${width}px`, height: `${height}px` }}
            >
              {/* This frame is purely a visual indicator; the actual layout is
                  driven by the store's forced width/height. */}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}