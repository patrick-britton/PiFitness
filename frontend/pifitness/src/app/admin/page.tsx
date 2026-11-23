/**
 * Admin Page
 * Tabbed admin panel with modular components for:
 * - Task Monitoring (TaskList)
 * - Database Sessions (DBSessions)
 * - Event Search (EventHistory)
 * - Service Configuration (ServiceConfig)
 * - Credential Management (CredentialManager)
 * - Raw Log Viewer (RawLogViewer)
 *
 * Responsive layout: horizontal tabs on desktop, vertical accordion on mobile.
 */

'use client';

import { useCallback } from 'react';
import { useViewportStore } from '../../stores/viewportStore';
import { useUIStore } from '../../stores/uiStore';
import DBInfo from './components/DBInfo';
import TaskList from './components/TaskList';
import ServiceConfig from './components/ServiceConfig';
import CredentialManager from './components/CredentialManager';
import ThemeSettings from './components/ThemeSettings';

/**
 * Tab configuration
 */
const TABS = [
  { id: 'db-info', label: 'DB Info', icon: '📊', component: DBInfo },
  { id: 'task-management', label: 'Task Management', icon: '⚙️', component: TaskList },
  { id: 'services', label: 'Services', icon: '🔌', component: ServiceConfig },
  { id: 'credentials', label: 'Credentials', icon: '🔐', component: CredentialManager },
  { id: 'theme', label: 'Theme', icon: '🌓', component: ThemeSettings },
] as const;

type TabId = (typeof TABS)[number]['id'];

/**
 * Admin page with tab navigation
 */
export default function AdminPage() {
  const { activeSubPage, setActiveSubPage } = useUIStore();
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';
  const isLandscape = layoutVariant === 'landscape';

  const activeTab = (activeSubPage as TabId) || 'db-info';

  const handleTabChange = useCallback((tabId: TabId) => {
    setActiveSubPage(tabId);
  }, [setActiveSubPage]);

  // Find the active tab config
  const activeTabConfig = TABS.find((t) => t.id === activeTab) || TABS[0];
  const ActiveComponent = activeTabConfig.component;

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      {/* Page Header */}
      <div className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 px-4 py-3 sm:px-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Admin Panel</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-0.5">
            {activeTabConfig.label} — System administration, task monitoring, and configuration
          </p>
        </div>
      </div>

      {/* Tab Navigation — Desktop (full labels) */}
      {isDesktop && (
        <div className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
          <nav className="flex overflow-x-auto px-4 sm:px-6" aria-label="Admin Tabs">
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
                <span className="text-base">{tab.icon}</span>
                {tab.label}
              </button>
            ))}
          </nav>
        </div>
      )}



      {/* Tab Navigation — Landscape (icon-only horizontal scroll) */}
      {isLandscape && (
        <div className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
          <nav className="flex overflow-x-auto px-2" aria-label="Admin Tabs">
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
                <span className="text-lg">{tab.icon}</span>
              </button>
            ))}
          </nav>
        </div>
      )}

      {/* Active Tab Content */}
      <div className="p-4 sm:p-6">
        <div className="max-w-7xl mx-auto">
          <ActiveComponent />
        </div>
      </div>
    </div>
  );
}