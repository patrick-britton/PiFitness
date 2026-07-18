/**
 * Zustand store for UI state management
 * Handles navigation, theme, and global UI state
 * Note: Theme is managed by next-themes for SSR safety; this store syncs for compatibility
 */

import { create } from 'zustand';

/**
 * Navigation module types
 */
export type NavigationModule = 'home' | 'music' | 'activities' | 'health' | 'food' | 'exercises' | 'admin';

/**
 * Sub-navigation configuration for a single sub-page within a module
 */
export interface SubNavigationConfig {
  id: string;
  label: string;
  iconName: string;
  /** Path relative to the module root, e.g. '/now-playing' */
  path: string;
}

/**
 * UI store state interface
 */
interface UIState {
  // Navigation state
  activeModule: NavigationModule;
  activeSubPage: string | null;

  // Theme state (synced with next-themes)
  theme: 'light' | 'dark';
  colorScheme: 'default' | 'high-contrast';

  // Initialize the store
  initialize: () => void;

  // Navigation actions
  setActiveModule: (module: NavigationModule) => void;
  setActiveSubPage: (subPageId: string | null) => void;
  syncActiveModuleFromPath: () => void;
  syncActiveSubPageFromPath: () => void;

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
  activeSubPage: null,
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
   * Set the active sub-page within the current module
   */
  setActiveSubPage: (subPageId: string | null) => {
    set({ activeSubPage: subPageId });
  },

  /**
    * Sync activeModule from current URL path (for SPA navigation persistence)
    * Uses prefix matching so /music/now-playing resolves to 'music'
    */
  syncActiveModuleFromPath: () => {
    if (typeof window !== 'undefined') {
      const path = window.location.pathname;
      const module = NAVIGATION_MODULES.find(m => path === m.path || path.startsWith(m.path + '/'));
      if (module) {
        set({ activeModule: module.id });
      }
    }
  },

  /**
   * Sync activeSubPage from current URL path
   */
  syncActiveSubPageFromPath: () => {
    if (typeof window !== 'undefined') {
      const path = window.location.pathname;
      // Find which module this path belongs to
      const module = NAVIGATION_MODULES.find(m => path === m.path || path.startsWith(m.path + '/'));
      if (!module) {
        set({ activeSubPage: null });
        return;
      }
      // Get the sub-page config for this module
      const subPages = MODULE_SUB_PAGES[module.id];
      if (!subPages) {
        set({ activeSubPage: null });
        return;
      }
      // Find the matching sub-page
      const subPage = subPages.find(sp => path === module.path + sp.path);
      if (subPage) {
        set({ activeSubPage: subPage.id });
      } else {
        set({ activeSubPage: null });
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
    id: 'music',
    label: 'Music',
    iconName: 'MusicNote',
    path: '/music',
  },
  {
    id: 'activities',
    label: 'Activities',
    iconName: 'DirectionsRun',
    path: '/activities',
  },
  {
    id: 'health',
    label: 'Health',
    iconName: 'Favorite',
    path: '/health',
  },
  {
    id: 'food',
    label: 'Food',
    iconName: 'Restaurant',
    path: '/food',
  },
  {
    id: 'exercises',
    label: 'Exercises',
    iconName: 'FitnessCenter',
    path: '/exercises',
  },
  {
    id: 'admin',
    label: 'Admin',
    iconName: 'Settings',
    path: '/admin',
  },
];

/**
 * Sub-page configuration per module
 * Keyed by module id, each entry is an ordered array of sub-pages
 */
export const MODULE_SUB_PAGES: Record<NavigationModule, SubNavigationConfig[]> = {
  home: [],
  music: [
    { id: 'now-playing', label: 'Now Playing', iconName: 'PlayCircle', path: '/now-playing' },
    { id: 'ratings', label: 'Ratings', iconName: 'Star', path: '/ratings' },
    { id: 'playlist-shuffle', label: 'Playlist Shuffle', iconName: 'Shuffle', path: '/playlist-shuffle' },
    { id: 'isrc-review', label: 'ISRC Review', iconName: 'Search', path: '/isrc-review' },
    { id: 'playlist-config', label: 'Playlist Configuration', iconName: 'Tune', path: '/playlist-config' },
    { id: 'playlist-sync', label: 'Playlist Sync', iconName: 'Sync', path: '/playlist-sync' },
  ],
  activities: [
    { id: 'recent-activity', label: 'Recent Activity Report', iconName: 'Timeline', path: '/recent-activity' },
    { id: 'leaderboards', label: 'Leaderboards', iconName: 'Leaderboard', path: '/leaderboards' },
    { id: 'run-predictions', label: 'Run Predictions', iconName: 'Speed', path: '/run-predictions' },
    { id: 'segment-management', label: 'Segment Management', iconName: 'Route', path: '/segment-management' },
    { id: 'activity-processing', label: 'Activity Processing', iconName: 'Cached', path: '/activity-processing' },
    { id: 'training-cycles', label: 'Training Cycle Management', iconName: 'Repeat', path: '/training-cycles' },
  ],
  health: [
    { id: 'charting', label: 'Charting', iconName: 'BarChart', path: '/charting' },
    { id: 'photo-intake', label: 'Photo Intake', iconName: 'CameraAlt', path: '/photo-intake' },
    { id: 'dimension-intake', label: 'Dimension Intake', iconName: 'Straighten', path: '/dimension-intake' },
    { id: 'weight-targets', label: 'Weight Targets', iconName: 'TrackChanges', path: '/weight-targets' },
  ],
  food: [
    { id: 'logging', label: 'Food Logging', iconName: 'EditNote', path: '/logging' },
    { id: 'summary', label: 'Food Summary', iconName: 'Summarize', path: '/summary' },
    { id: 'recipe-selection', label: 'Recipe Selection', iconName: 'MenuBook', path: '/recipe-selection' },
    { id: 'recipe-creation', label: 'Recipe Creation', iconName: 'Create', path: '/recipe-creation' },
    { id: 'tri-tip-timer', label: 'Tri-tip Timer', iconName: 'Timer', path: '/tri-tip-timer' },
  ],
  exercises: [
    { id: 'timer-activation', label: 'Timer Activation', iconName: 'PlayArrow', path: '/timer-activation' },
    { id: 'timer-creation', label: 'Timer Creation', iconName: 'AddCircle', path: '/timer-creation' },
  ],
  admin: [],
};