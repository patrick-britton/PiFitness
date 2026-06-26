/**
 * Sidebar Component
 * Desktop navigation sidebar with Material UI icons
 */

'use client';

import { NAVIGATION_MODULES } from '../../stores/uiStore';
import { useUIStore } from '../../stores/uiStore';
import * as Icons from '@mui/icons-material';

export default function Sidebar() {
  const { activeModule, setActiveModule, sidebarOpen, toggleSidebar } = useUIStore();

  // Get the appropriate Material UI icon for each module
  const getIcon = (iconName: string) => {
    const IconComponent = (Icons as any)[iconName];
    return IconComponent ? <IconComponent className="w-5 h-5" /> : null;
  };

  return (
    <div className="h-full flex flex-col bg-white dark:bg-gray-800">
      {/* Header */}
      <div className="p-4 border-b border-gray-200 dark:border-gray-700">
        <h1 className="text-xl font-bold text-gray-900 dark:text-white">PiFitness</h1>
      </div>

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto">
        <ul className="p-2 space-y-1">
          {NAVIGATION_MODULES.map((module) => (
            <li key={module.id}>
              <button
                onClick={() => setActiveModule(module.id)}
                className={`w-full flex items-center p-2 rounded-lg text-left ${
                  activeModule === module.id
                    ? 'bg-blue-100 dark:bg-blue-900 text-blue-600 dark:text-blue-300'
                    : 'text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700'
                }`}
              >
                <span className="mr-3">{getIcon(module.iconName)}</span>
                <span>{module.label}</span>
              </button>
            </li>
          ))}
        </ul>
      </nav>

      {/* Footer with toggle button */}
      <div className="p-2 border-t border-gray-200 dark:border-gray-700">
        <button
          onClick={toggleSidebar}
          className="w-full flex items-center justify-center p-2 text-sm text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 rounded-lg"
        >
          <Icons.ChevronLeft className="w-5 h-5 mr-2" />
          {sidebarOpen ? 'Collapse' : 'Expand'}
        </button>
      </div>
    </div>
  );
}