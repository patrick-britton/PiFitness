/**
 * Main Layout Component
 * Responsive shell that switches between sidebar (≥768px) and bottom navigation (<768px)
 */

'use client';

import { useEffect } from 'react';
import { useViewportStore } from '../../stores/viewportStore';
import { useUIStore } from '../../stores/uiStore';
import Header from './Header';
import Sidebar from './Sidebar';
import NavBar from './NavBar';

interface LayoutProps {
  children: React.ReactNode;
}

export default function Layout({ children }: LayoutProps) {
  const { initialize: initializeViewport, setViewport } = useViewportStore();
  const { initialize: initializeUI } = useUIStore();

  // Initialize stores on mount
  useEffect(() => {
    initializeViewport();
    initializeUI();

    const handleResize = () => {
      setViewport(window.innerWidth, window.innerHeight);
    };

    // Set initial size
    handleResize();

    // Add event listener
    window.addEventListener('resize', handleResize);

    // Cleanup
    return () => window.removeEventListener('resize', handleResize);
  }, [initializeViewport, initializeUI, setViewport]);

  const { isMobile } = useViewportStore();

  return (
    <div className="flex flex-col min-h-screen bg-gray-50 dark:bg-gray-900">
      {/* Header */}
      <Header />

      {/* Main content area */}
      <div className="flex flex-1 overflow-hidden">
        {/* Sidebar for desktop - always visible */}
        {!isMobile && (
          <div className="w-64 flex-shrink-0 border-r border-gray-200 dark:border-gray-700">
            <Sidebar />
          </div>
        )}

        {/* Main content */}
        <main className="flex-1 overflow-auto">
          {children}
        </main>
      </div>

      {/* Bottom navigation for mobile */}
      {isMobile && (
        <div className="border-t border-gray-200 dark:border-gray-700">
          <NavBar />
        </div>
      )}
    </div>
  );
}