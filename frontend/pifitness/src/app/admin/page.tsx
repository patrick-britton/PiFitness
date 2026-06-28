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

import { useState, useCallback } from 'react';
import TaskList from './components/TaskList';
import DBSessions from './components/DBSessions';
import EventHistory from './components/EventHistory';
import ServiceConfig from './components/ServiceConfig';
import CredentialManager from './components/CredentialManager';
import RawLogViewer from './components/RawLogViewer';

/**
 * Tab configuration
 */
const TABS = [
  { id: 'tasks', label: 'Tasks', icon: '⚙️', component: TaskList },
  { id: 'sessions', label: 'DB Sessions', icon: '🛢️', component: DBSessions },
  { id: 'events', label: 'Events', icon: '📋', component: EventHistory },
  { id: 'services', label: 'Services', icon: '🔌', component: ServiceConfig },
  { id: 'credentials', label: 'Credentials', icon: '🔐', component: CredentialManager },
  { id: 'logs', label: 'Raw Logs', icon: '📈', component: RawLogViewer },
] as const;

type TabId = (typeof TABS)[number]['id'];

/**
 * Admin page with tab navigation
 */
export default function AdminPage() {
  const [activeTab, setActiveTab] = useState<TabId>('tasks');
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const handleTabChange = useCallback((tabId: TabId) => {
    setActiveTab(tabId);
    setMobileMenuOpen(false);
  }, []);

  // Find the active tab config
  const activeTabConfig = TABS.find((t) => t.id === activeTab) || TABS[0];
  const ActiveComponent = activeTabConfig.component;

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      {/* Page Header */}
      <div className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 px-4 py-3 sm:px-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Admin Panel</h1>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-0.5">
              System administration, task monitoring, and configuration
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

      {/* Tab Navigation — Desktop */}
      <div className="hidden sm:block bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700">
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

      {/* Tab Navigation — Mobile Accordion */}
      {mobileMenuOpen && (
        <div className="sm:hidden bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 shadow-lg">
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
                <span className="text-lg">{tab.icon}</span>
                {tab.label}
              </button>
            ))}
          </div>
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