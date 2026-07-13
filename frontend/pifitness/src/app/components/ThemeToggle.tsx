'use client';

import { useUIStore } from '../../stores/uiStore';
import WbSunnyIcon from '@mui/icons-material/WbSunny';

export default function ThemeToggle() {
  const { theme, toggleTheme } = useUIStore();

  const handleClick = () => {
    toggleTheme();
  };

  const isDark = theme === 'dark';

  return (
    <button
      type="button"
      onClick={handleClick}
      aria-label={`Switch to ${isDark ? 'light' : 'dark'} theme`}
      title={`Switch to ${isDark ? 'light' : 'dark'} theme`}
      className="inline-flex items-center justify-center h-11 w-11 rounded-full text-gray-900 dark:text-yellow-300 bg-gray-100 dark:bg-gray-800 hover:bg-gray-200 dark:hover:bg-gray-700 transition-colors"
    >
      <WbSunnyIcon className="text-[20px] pointer-events-none" />
    </button>
  );
}