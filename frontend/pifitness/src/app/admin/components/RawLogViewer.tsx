/**
 * RawLogViewer Component
 * Dropdown-selected log table viewer with dynamic column rendering.
 * Fetches table list from logging schema and displays data in a grid.
 */

'use client';

import { useState } from 'react';
import { useLogTables, useLogData } from '@/hooks/useAdmin';

/**
 * RawLogViewer Component
 */
export default function RawLogViewer() {
  const [selectedTable, setSelectedTable] = useState('');
  const [rowLimit, setRowLimit] = useState(100);

  const { data: tablesData, isLoading: tablesLoading, error: tablesError } = useLogTables();
  const { data: logData, isLoading: dataLoading, error: dataError, refetch } = useLogData(selectedTable, rowLimit);

  const tables = tablesData?.data || [];

  const handleTableChange = (tableName: string) => {
    setSelectedTable(tableName);
  };

  const columns = logData?.data && logData.data.length > 0
    ? Object.keys(logData.data[0])
    : [];

  return (
    <div>
      <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">
        Raw Log Table Viewer
      </h2>

      {/* Table Selector */}
      <div className="flex flex-wrap items-end gap-3 mb-4">
        <div className="flex-1 min-w-[200px]">
          <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
            Select Log Table
          </label>
          {tablesLoading ? (
            <div className="flex items-center gap-2 text-sm text-gray-500 dark:text-gray-400">
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-600" />
              Loading tables...
            </div>
          ) : tablesError ? (
            <p className="text-sm text-red-500">Error loading tables: {String(tablesError)}</p>
          ) : (
            <select
              value={selectedTable}
              onChange={(e) => handleTableChange(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
            >
              <option value="">-- Select a table --</option>
              {tables.map((table: string) => (
                <option key={table} value={table}>{table}</option>
              ))}
            </select>
          )}
        </div>

        <div className="w-32">
          <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
            Row Limit
          </label>
          <input
            type="number"
            value={rowLimit}
            onChange={(e) => setRowLimit(Math.max(1, Math.min(1000, parseInt(e.target.value) || 100)))}
            min={1}
            max={1000}
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
          />
        </div>

        {selectedTable && (
          <button
            onClick={() => refetch()}
            className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700"
          >
            Refresh
          </button>
        )}
      </div>

      {/* Data Display */}
      {!selectedTable && (
        <div className="bg-gray-50 dark:bg-gray-800 rounded-md p-8 text-center">
          <p className="text-gray-500 dark:text-gray-400">Select a log table above to view its data.</p>
        </div>
      )}

      {dataLoading && (
        <div className="flex items-center justify-center py-12">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
          <span className="ml-3 text-sm text-gray-500 dark:text-gray-400">Loading data...</span>
        </div>
      )}

      {dataError && (
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-4">
          <p className="text-red-700 dark:text-red-300">Failed to load log data: {String(dataError)}</p>
        </div>
      )}

      {selectedTable && !dataLoading && !dataError && logData && (
        <div>
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm text-gray-500 dark:text-gray-400">
              {logData.count || 0} row{(logData.count || 0) !== 1 ? 's' : ''} from <strong className="text-gray-700 dark:text-gray-300">{selectedTable}</strong>
            </p>
          </div>

          {(!logData.data || logData.data.length === 0) ? (
            <div className="bg-gray-50 dark:bg-gray-800 rounded-md p-6 text-center">
              <p className="text-gray-500 dark:text-gray-400">No data in this table.</p>
            </div>
          ) : (
            <div className="overflow-x-auto max-h-[70vh] overflow-y-auto border border-gray-200 dark:border-gray-700 rounded-md">
              <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                <thead className="bg-gray-50 dark:bg-gray-800 sticky top-0 z-10">
                  <tr>
                    {columns.map((col) => (
                      <th
                        key={col}
                        className="px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider whitespace-nowrap"
                      >
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-800">
                  {logData.data.map((row: any, idx: number) => (
                    <tr key={idx} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                      {columns.map((col) => (
                        <td
                          key={col}
                          className="px-3 py-1.5 text-xs text-gray-700 dark:text-gray-300 whitespace-nowrap max-w-xs truncate"
                          title={String(row[col] ?? '')}
                        >
                          {formatCellValue(row[col])}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/**
 * Format a cell value for display
 */
function formatCellValue(value: any): string {
  if (value === null || value === undefined) return 'NULL';
  if (value instanceof Date) return value.toLocaleString();
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  return String(value);
}