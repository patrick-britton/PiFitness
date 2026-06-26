/**
 * Zustand store for viewport and responsive state management
 * Optimized for Pixel 9 XL mobile and 4K desktop displays
 */

import { create } from 'zustand';

/**
 * Viewport store state interface
 */
interface ViewportState {
  width: number;
  height: number;
  breakpoint: 'xs' | 'sm' | 'md' | 'lg' | 'xl';
  orientation: 'portrait' | 'landscape';
  isMobile: boolean;
  isPortrait: boolean;
  isLandscape: boolean;
  manualOrientation: 'portrait' | 'landscape' | null;
  isMobileOverride: boolean;
  initialize: () => void;
  setViewport: (width: number, height: number) => void;
  toggleOrientation: () => void;
  clearOrientationOverride: () => void;
  toggleMobileOverride: () => void;
}

/**
 * Create the viewport store
 */
export const useViewportStore = create<ViewportState>((set, get) => ({
  width: 0,
  height: 0,
  breakpoint: 'xs',
  orientation: 'portrait',
  isMobile: false,
  isPortrait: true,
  isLandscape: false,
  manualOrientation: null,
  isMobileOverride: false,

  /**
   * Initialize the store with current viewport values
   */
  initialize: () => {
    if (typeof window !== 'undefined') {
      const width = window.innerWidth;
      const height = window.innerHeight;

      // Calculate breakpoint
      const getBreakpoint = (w: number): 'xs' | 'sm' | 'md' | 'lg' | 'xl' => {
        if (w >= 1440) return 'xl';
        if (w >= 1024) return 'lg';
        if (w >= 768) return 'md';
        if (w >= 640) return 'sm';
        return 'xs';
      };

      // Calculate orientation
      const getOrientation = (w: number, h: number): 'portrait' | 'landscape' => {
        return w > h ? 'landscape' : 'portrait';
      };

      const breakpoint = getBreakpoint(width);
      const orientation = getOrientation(width, height);

      // Check URL for mobile parameter
      const isMobileFromURL = () => {
        const urlParams = new URLSearchParams(window.location.search);
        return urlParams.has('mobile') && (urlParams.get('mobile') === 'true' || urlParams.get('mobile') === '');
      };

      const isMobile = isMobileFromURL() || breakpoint === 'xs' || (breakpoint === 'sm' && orientation === 'portrait');

      set({
        width,
        height,
        breakpoint,
        orientation,
        isMobile,
        isPortrait: orientation === 'portrait',
        isLandscape: orientation === 'landscape',
      });
    }
  },

  /**
   * Update viewport dimensions and recalculate derived values
   */
  setViewport: (width: number, height: number) => {
    // Calculate breakpoint
    const getBreakpoint = (w: number): 'xs' | 'sm' | 'md' | 'lg' | 'xl' => {
      if (w >= 1440) return 'xl';
      if (w >= 1024) return 'lg';
      if (w >= 768) return 'md';
      if (w >= 640) return 'sm';
      return 'xs';
    };

    // Calculate orientation
    const getOrientation = (w: number, h: number): 'portrait' | 'landscape' => {
      const manualOrientation = get().manualOrientation;
      if (manualOrientation) return manualOrientation;
      return w > h ? 'landscape' : 'portrait';
    };

    const breakpoint = getBreakpoint(width);
    const orientation = getOrientation(width, height);
    const isMobileOverride = get().isMobileOverride;

    // Check URL for mobile parameter
    const isMobileFromURL = () => {
      if (typeof window !== 'undefined') {
        const urlParams = new URLSearchParams(window.location.search);
        return urlParams.has('mobile') && (urlParams.get('mobile') === 'true' || urlParams.get('mobile') === '');
      }
      return false;
    };

    const isMobile = isMobileOverride || isMobileFromURL() || breakpoint === 'xs' || (breakpoint === 'sm' && orientation === 'portrait');

    set({
      width,
      height,
      breakpoint,
      orientation,
      isMobile,
      isPortrait: orientation === 'portrait',
      isLandscape: orientation === 'landscape',
    });
  },

  /**
   * Toggle between portrait and landscape orientation
   */
  toggleOrientation: () => {
    const current = get().manualOrientation;
    const newOrientation = current === 'portrait' ? 'landscape' : 'portrait';
    set({ manualOrientation: newOrientation });

    // Recalculate mobile state
    const { width, height } = get();
    const isMobileFromURL = () => {
      if (typeof window !== 'undefined') {
        const urlParams = new URLSearchParams(window.location.search);
        return urlParams.has('mobile') && (urlParams.get('mobile') === 'true' || urlParams.get('mobile') === '');
      }
      return false;
    };

    const isMobile = get().isMobileOverride || isMobileFromURL() || get().breakpoint === 'xs' || (get().breakpoint === 'sm' && newOrientation === 'portrait');
    set({ isMobile, isPortrait: newOrientation === 'portrait', isLandscape: newOrientation === 'landscape' });
  },

  /**
   * Clear manual orientation override
   */
  clearOrientationOverride: () => {
    const { width, height } = get();
    const autoOrientation = width > height ? 'landscape' : 'portrait';
    set({ manualOrientation: null, orientation: autoOrientation, isPortrait: autoOrientation === 'portrait', isLandscape: autoOrientation === 'landscape' });
  },

  /**
   * Toggle mobile override
   */
  toggleMobileOverride: () => {
    const current = get().isMobileOverride;
    set({ isMobileOverride: !current, isMobile: !current });
  },
}));