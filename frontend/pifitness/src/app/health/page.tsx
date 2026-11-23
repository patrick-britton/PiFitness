/**
 * Health Module Page
 * Inline tab navigation matching admin module pattern.
 * Sub-pages: Charting, Photo Intake, Dimension Intake, Weight Targets
 */

'use client';

import { useCallback } from 'react';
import { useViewportStore } from '../../stores/viewportStore';
import { useUIStore, MODULE_SUB_PAGES } from '../../stores/uiStore';
import * as Icons from '@mui/icons-material';

const TABS = MODULE_SUB_PAGES.health;

type TabId = (typeof TABS)[number]['id'];

export default function HealthPage() {
  const { activeSubPage, setActiveSubPage } = useUIStore();
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';
  const isLandscape = layoutVariant === 'landscape';

  const activeTab = activeSubPage || TABS[0].id;

  const handleTabChange = useCallback((tabId: TabId) => {
    setActiveSubPage(tabId);
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
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Health</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-0.5">
            {activeTabConfig.label}
          </p>
        </div>
      </div>

      {/* Tab Navigation — Desktop (full labels) */}
      {isDesktop && (
        <div className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
          <nav className="flex overflow-x-auto px-4 sm:px-6" aria-label="Health Tabs">
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



      {/* Tab Navigation — Landscape (icon-only horizontal scroll) */}
      {isLandscape && (
        <div className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
          <nav className="flex overflow-x-auto px-2" aria-label="Health Tabs">
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
          <div className="p-8 text-center">
            <p className="text-gray-500 dark:text-gray-400 text-lg">
              wow such empty
            </p>
            <p className="text-gray-400 dark:text-gray-500 mt-2">
              future home of {activeTabConfig.label}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}