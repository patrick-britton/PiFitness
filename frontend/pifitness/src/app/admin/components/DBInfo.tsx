/**
 * DB Info Component
 * Sub-tabbed container for database information and inspection features:
 * - Task Summary: Table of tasks with status, timing, and execute capability
 * - DB Size: Historical growth and current breakdown charts
 * - Log Search: Filterable event history (reuses EventHistory filters)
 * - Active SQL: Active queries from pg_stat_activity with kill capability
 * - Raw Logs: Direct log table viewer (migrated from standalone tab)
 *
 * Responsive layout: horizontal sub-tabs on desktop, vertical accordion on mobile.
 */

'use client';

import { useState, useCallback } from 'react';
import TaskSummary from './TaskSummary';
import DbSizeView from './DbSizeView';
import EventHistory from './EventHistory';
import ActiveSqlView from './ActiveSqlView';
import RawLogViewer from './RawLogViewer';

/**
 * Sub-tab configuration for DB Info
 */
const SUB_TABS = [
  { id: 'task-summary', label: 'Task Summary' },
  { id: 'db-size', label: 'DB Size' },
  { id: 'log-search', label: 'Log Search' },
  { id: 'active-sql', label: 'Active SQL' },
  { id: 'raw-logs', label: 'Raw Logs' },
] as const;

type SubTabId = (typeof SUB_TABS)[number]['id'];

/**
 * DB Info component with sub-tab navigation
 */
export default function DBInfo() {
  const [activeSubTab, setActiveSubTab] = useState<SubTabId>('task-summary');
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const handleSubTabChange = useCallback((tabId: SubTabId) => {
    setActiveSubTab(tabId);
    setMobileMenuOpen(false);
  }, []);

  return (
    <div>
      {/* Sub-tab Navigation — Desktop */}
      <div className="hidden sm:block border-b border-gray-200 dark:border-gray-700 mb-4">
        <nav className="flex overflow-x-auto" aria-label="DB Info Sub-tabs">
          {SUB_TABS.map((tab) => (
            <button
              key={tab.id}
              onClick={() => handleSubTabChange(tab.id)}
              className={`px-4 py-2 text-sm font-medium border-b-2 whitespace-nowrap transition-colors ${
                activeSubTab === tab.id
                  ? 'border-blue-500 text-blue-600 dark:text-blue-400 dark:border-blue-400'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300 dark:text-gray-400 dark:hover:text-gray-200 dark:hover:border-gray-600'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </nav>
      </div>

      {/* Sub-tab Navigation — Mobile Accordion */}
      <div className="sm:hidden mb-4">
        <button
          onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
          className="w-full flex items-center justify-between px-4 py-2.5 text-sm font-medium bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md text-gray-700 dark:text-gray-300"
        >
          <span>{SUB_TABS.find((t) => t.id === activeSubTab)?.label || 'Select View'}</span>
          <svg
            className={`w-4 h-4 transition-transform ${mobileMenuOpen ? 'rotate-180' : ''}`}
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </button>
        {mobileMenuOpen && (
          <div className="mt-1 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md shadow-lg">
            {SUB_TABS.map((tab) => (
              <button
                key={tab.id}
                onClick={() => handleSubTabChange(tab.id)}
                className={`w-full text-left px-4 py-2.5 text-sm font-medium ${
                  activeSubTab === tab.id
                    ? 'bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300'
                    : 'text-gray-600 hover:bg-gray-100 dark:text-gray-400 dark:hover:bg-gray-700'
                } ${tab.id === SUB_TABS[0].id ? 'rounded-t-md' : ''} ${
                  tab.id === SUB_TABS[SUB_TABS.length - 1].id ? 'rounded-b-md' : ''
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Active Sub-tab Content */}
      <div>
        {activeSubTab === 'task-summary' && <TaskSummary />}
        {activeSubTab === 'db-size' && <DbSizeView />}
        {activeSubTab === 'log-search' && <EventHistory />}
        {activeSubTab === 'active-sql' && <ActiveSqlView />}
        {activeSubTab === 'raw-logs' && <RawLogViewer />}
      </div>
    </div>
  );
}