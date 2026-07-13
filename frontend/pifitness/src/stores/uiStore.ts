/**
 * Zustand store for UI state management
 * Handles navigation, theme, and global UI state
 * Note: Theme is managed by next-themes for SSR safety; this store syncs for compatibility
 */

import { create } from 'zustand';

/**
 * Navigation module types
 */
export type NavigationModule = 'home' | 'health' | 'music' | 'running' | 'admin';

/**
 * UI store state interface
 */
interface UIState {
  // Navigation state
  activeModule: NavigationModule;

  // Theme state (synced with next-themes)
  theme: 'light' | 'dark';
  colorScheme: 'default' | 'high-contrast';

  // Initialize the store
  initialize: () => void;

  // Navigation actions
  setActiveModule: (module: NavigationModule) => void;
  syncActiveModuleFromPath: () => void;

  // Theme actions (not used - next-themes handles this, kept for compatibility)
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
   * Note: Theme is handled by next-themes ThemeProvider for SSR safety.
   * This function only handles non-theme initialization.
   */
  initialize: () => {
    // Theme is now handled by next-themes in providers.tsx
    // This method is kept for any future non-theme initialization needs
  },

  /**
    * Set the active navigation module
    */
  setActiveModule: (module: NavigationModule) => {
    set({ activeModule: module });
  },

  /**
    * Sync activeModule from current URL path (for SPA navigation persistence)
    */
  syncActiveModuleFromPath: () => {
    if (typeof window !== 'undefined') {
      const path = window.location.pathname;
      const module = NAVIGATION_MODULES.find(m => m.path === path);
      if (module) {
        set({ activeModule: module.id });
      }
    }
  },

  /**
   * Set theme explicitly (deprecated - next-themes handles this)
   * Kept for backward compatibility if any component still uses it
   */
  setTheme: (theme: 'light' | 'dark') => {
    set({ theme });
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