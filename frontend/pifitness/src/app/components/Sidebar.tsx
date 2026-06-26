/**
 * Sidebar Component
 * Desktop navigation sidebar with Material UI icons
 */

'use client';

import { NAVIGATION_MODULES, NavigationModule } from '../../stores/uiStore';
import { useUIStore } from '../../stores/uiStore';
import * as Icons from '@mui/icons-material';
import { useRouter } from 'next/navigation';

export default function Sidebar() {
  const { activeModule, setActiveModule } = useUIStore();
  const router = useRouter();

  // Get the appropriate Material UI icon for each module
  const getIcon = (iconName: string) => {
    const IconComponent = (Icons as any)[iconName];
    return IconComponent ? <IconComponent className="w-5 h-5" /> : null;
  };

  const handleNavigation = (moduleId: NavigationModule) => {
    const module = NAVIGATION_MODULES.find(m => m.id === moduleId);
    if (module) {
      setActiveModule(moduleId);
      // Use window.location for static export compatibility
      if (typeof window !== 'undefined') {
        window.location.href = module.path;
      }
    }
  };

  return (
    <div className="h-full flex flex-col bg-white dark:bg-gray-800">
      {/* Header removed - sidebar starts directly with navigation buttons */}
      <div className="p-2 border-b border-gray-200 dark:border-gray-700">
        {/* Spacer to maintain layout */}
      </div>

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto p-2">
        <div className="space-y-2">
          {NAVIGATION_MODULES.map((module) => (
            <button
              key={module.id}
              onClick={() => handleNavigation(module.id)}
              className={`w-full flex items-center justify-center gap-3 p-3 rounded-lg border transition-colors ${
                activeModule === module.id
                  ? 'border-blue-300 bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300'
                  : 'border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 hover:border-gray-300 dark:hover:border-gray-500 hover:bg-gray-50 dark:hover:bg-gray-700/50'
              }`}
            >
              <span className="flex-shrink-0">{getIcon(module.iconName)}</span>
              <span className="font-medium">{module.label}</span>
            </button>
          ))}
        </div>
      </nav>
    </div>
  );
}
