/**
 * NavBar Component
 * Mobile bottom navigation bar with Material UI icons
 */

'use client';

import { useRouter } from 'next/navigation';
import { NAVIGATION_MODULES } from '../../stores/uiStore';
import { useUIStore } from '../../stores/uiStore';
import * as Icons from '@mui/icons-material';

export default function NavBar() {
  const router = useRouter();
  const { activeModule, setActiveModule } = useUIStore();

  // Get the appropriate Material UI icon for each module
  const getIcon = (iconName: string) => {
    const IconComponent = (Icons as any)[iconName];
    return IconComponent ? <IconComponent className="w-6 h-6" /> : null;
  };

  const handleNavigation = (module: typeof NAVIGATION_MODULES[number]) => {
    setActiveModule(module.id);
    router.push(module.path);
  };

  return (
    <nav className="bg-white dark:bg-gray-800 border-t border-gray-200 dark:border-gray-700">
      <div className="flex justify-around">
        {NAVIGATION_MODULES.map((module) => (
          <button
            key={module.id}
            onClick={() => handleNavigation(module)}
            className={`flex flex-col items-center justify-center p-3 flex-1 ${
              activeModule === module.id
                ? 'text-blue-600 dark:text-blue-300'
                : 'text-gray-600 dark:text-gray-300'
            }`}
          >
            <span className="mb-1">{getIcon(module.iconName)}</span>
            <span className="text-xs">{module.label}</span>
          </button>
        ))}
      </div>
    </nav>
  );
}
