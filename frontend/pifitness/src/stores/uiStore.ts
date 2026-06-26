/**
 * Zustand store for UI state management
 * Handles navigation, theme, and global UI state
 */

import { create } from 'zustand';

/**
 * Navigation module types
 */
type NavigationModule = 'home' | 'health' | 'music' | 'running' | 'admin';

/**
 * UI store state interface
 */
interface UIState {
  // Navigation state
  activeModule: NavigationModule;

  // Theme state
  theme: 'light' | 'dark';
  colorScheme: 'default' | 'high-contrast';

  // Initialize the store
  initialize: () => void;

  // Navigation actions
  setActiveModule: (module: NavigationModule) => void;

  // Theme actions
  toggleTheme: () => void;
  setTheme: (theme: 'light' | 'dark') => void;
  setColorScheme: (scheme: 'default' | 'high-contrast') => void;
}

/**
 * Create the UI store
 */
export const useUIStore = create<UIState>((set) => ({
  // Initial state
  activeModule: 'home',
  theme: 'light',
  colorScheme: 'default',

  /**
   * Initialize the store
   */
  initialize: () => {
    // Load preferences from localStorage if available
    if (typeof window !== 'undefined') {
      const savedTheme = localStorage.getItem('pifitness-theme') as 'light' | 'dark' | null;

      set({
        theme: savedTheme || 'light',
      });
    }
  },

  /**
   * Set the active navigation module
   */
  setActiveModule: (module: NavigationModule) => {
    set({ activeModule: module });
  },

  /**
   * Toggle between light and dark theme
   */
  toggleTheme: () => {
    set((state) => {
      const newTheme = state.theme === 'light' ? 'dark' : 'light';
      if (typeof window !== 'undefined') {
        localStorage.setItem('pifitness-theme', newTheme);
        document.documentElement.classList.remove(state.theme);
        document.documentElement.classList.add(newTheme);
      }
      return { theme: newTheme };
    });
  },

  /**
   * Set theme explicitly
   */
  setTheme: (theme: 'light' | 'dark') => {
    set({ theme });
    if (typeof window !== 'undefined') {
      localStorage.setItem('pifitness-theme', theme);
      document.documentElement.classList.remove(theme === 'light' ? 'dark' : 'light');
      document.documentElement.classList.add(theme);
    }
  },

  /**
   * Set color scheme
   */
  setColorScheme: (scheme: 'default' | 'high-contrast') => {
    set({ colorScheme: scheme });
    if (typeof window !== 'undefined') {
      document.documentElement.classList.remove(
        scheme === 'default' ? 'high-contrast' : 'default'
      );
      document.documentElement.classList.add(scheme);
    }
  },
}));

/**
 * Navigation configuration with Material UI icons
 */
export const NAVIGATION_MODULES: {
  id: NavigationModule;
  label: string;
  iconName: string;
  path: string;
}[] = [
  {
    id: 'home',
    label: 'Home',
    iconName: 'Home',
    path: '/',
  },
  {
    id: 'health',
    label: 'Health',
    iconName: 'Favorite',
    path: '/health',
  },
  {
    id: 'music',
    label: 'Music',
    iconName: 'MusicNote',
    path: '/music',
  },
  {
    id: 'running',
    label: 'Running',
    iconName: 'DirectionsRun',
    path: '/running',
  },
  {
    id: 'admin',
    label: 'Admin',
    iconName: 'Settings',
    path: '/admin',
  },
];