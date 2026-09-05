/**
 * Music Module Page
 * Inline tab navigation matching admin module pattern.
 * Sub-pages: Now Playing, Ratings, Playlist Config, ISRC Review, Playlist Sync
 */

'use client';

import { useState, useCallback, useEffect } from 'react';
import { usePathname } from 'next/navigation';
import { useViewportStore } from '../../stores/viewportStore';
import { useUIStore, MODULE_SUB_PAGES } from '../../stores/uiStore';
import { API } from '../../lib/api-client';
import * as Icons from '@mui/icons-material';
import NowPlayingView from './components/NowPlayingView';
import RatingsView from './components/RatingsView';
import PlaylistShuffleView from './components/PlaylistShuffleView';

const TABS = MODULE_SUB_PAGES.music;

type TabId = (typeof TABS)[number]['id'];

export default function MusicPage() {
  const { activeSubPage, setActiveSubPage } = useUIStore();
  const pathname = usePathname();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';
  const isPortrait = layoutVariant === 'portrait';
  const isLandscape = layoutVariant === 'landscape';

  // OQ-2 / FR-10 — entry-time-only landing. Each time the module is opened
  // from navigation (activeSubPage is null; NavBar/Sidebar clear it), route to
  // Ratings when any track awaits rating, else Now Playing. Never active-switch
  // a tab the user has selected mid-session (a non-null activeSubPage is left
  // untouched), and a direct sub-page URL (path !== '/music') is preserved.
  useEffect(() => {
    if (pathname !== '/music') return;
    if (activeSubPage !== null) return;

    let cancelled = false;
    API.music
      .getRatingsEligibleCount()
      .then((res) => {
        if (cancelled) return;
        const pending = res?.count ?? 0;
        setActiveSubPage(pending > 0 ? 'ratings' : 'now-playing');
      })
      .catch(() => {
        if (!cancelled) setActiveSubPage('now-playing'); // AC-1 default
      });
    return () => {
      cancelled = true;
    };
  }, [pathname, activeSubPage, setActiveSubPage]);

  const activeTab = activeSubPage || TABS[0].id;

  const handleTabChange = useCallback((tabId: TabId) => {
    setActiveSubPage(tabId);
    setMobileMenuOpen(false);
  }, [setActiveSubPage]);

  const getIcon = (iconName: string) => {
    const IconComponent = (Icons as any)[iconName];
    return IconComponent ? <IconComponent className="w-5 h-5" /> : null;
  };

  const activeTabConfig = TABS.find(t => t.id === activeTab) || TABS[0];

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      {/* Page Header */}
      <div className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 px-4 py-3 sm:px-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Music</h1>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-0.5">
              {activeTabConfig.label}
            </p>
          </div>

          {/* Mobile menu toggle */}
          <button
            onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
            className="sm:hidden p-2 rounded-md text-gray-500 hover:text-gray-700 hover:bg-gray-100 dark:text-gray-400 dark:hover:text-gray-200 dark:hover:bg-gray-700"
            aria-label="Toggle navigation"
          >
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              {mobileMenuOpen ? (
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              ) : (
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
              )}
            </svg>
          </button>
        </div>
      </div>

      {/* Tab Navigation — Desktop (full labels) */}
      {isDesktop && (
        <div className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
          <nav className="flex overflow-x-auto px-4 sm:px-6" aria-label="Music Tabs">
            {TABS.map((tab) => (
              <button
                key={tab.id}
                onClick={() => handleTabChange(tab.id)}
                className={`flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 whitespace-nowrap transition-colors ${
                  activeTab === tab.id
                    ? 'border-blue-500 text-blue-600 dark:text-blue-400 dark:border-blue-400'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300 dark:text-gray-400 dark:hover:text-gray-200 dark:hover:border-gray-600'
                }`}
              >
                <span className="text-base">{getIcon(tab.iconName)}</span>
                {tab.label}
              </button>
            ))}
          </nav>
        </div>
      )}

      {/* Tab Navigation — Portrait (accordion, toggled by hamburger) */}
      {isPortrait && mobileMenuOpen && (
        <div className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 shadow-lg">
          <div className="px-2 py-2 space-y-1">
            {TABS.map((tab) => (
              <button
                key={tab.id}
                onClick={() => handleTabChange(tab.id)}
                className={`flex items-center gap-3 w-full px-3 py-2.5 text-sm font-medium rounded-md ${
                  activeTab === tab.id
                    ? 'bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300'
                    : 'text-gray-600 hover:bg-gray-100 dark:text-gray-400 dark:hover:bg-gray-700'
                }`}
              >
                <span className="text-lg">{getIcon(tab.iconName)}</span>
                {tab.label}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Tab Navigation — Landscape (icon-only horizontal scroll) */}
      {isLandscape && (
        <div className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
          <nav className="flex overflow-x-auto px-2" aria-label="Music Tabs">
            {TABS.map((tab) => (
              <button
                key={tab.id}
                onClick={() => handleTabChange(tab.id)}
                className={`flex items-center justify-center px-3 py-2 text-sm font-medium border-b-2 whitespace-nowrap transition-colors ${
                  activeTab === tab.id
                    ? 'border-blue-500 text-blue-600 dark:text-blue-400 dark:border-blue-400'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300 dark:text-gray-400 dark:hover:text-gray-200 dark:hover:border-gray-600'
                }`}
                title={tab.label}
              >
                <span className="text-lg">{getIcon(tab.iconName)}</span>
              </button>
            ))}
          </nav>
        </div>
      )}

      {/* Active Tab Content */}
      <div className="p-4 sm:p-6">
        <div className="max-w-7xl mx-auto">
          {/* Mounted feature views */}
          {activeTab === 'now-playing' && <NowPlayingView />}
          {activeTab === 'ratings' && <RatingsView />}
          {activeTab === 'playlist-shuffle' && <PlaylistShuffleView />}

          {/* Placeholder for sub-pages not yet built */}
          {activeTab !== 'now-playing' && activeTab !== 'ratings' && activeTab !== 'playlist-shuffle' && (
            <div className="p-8 text-center">
              <p className="text-gray-500 dark:text-gray-400 text-lg">
                wow such empty
              </p>
              <p className="text-gray-400 dark:text-gray-500 mt-2">
                future home of {activeTabConfig.label}
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}