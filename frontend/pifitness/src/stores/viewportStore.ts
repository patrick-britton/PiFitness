/**
 * Zustand store for viewport and responsive state management
 * Optimized for Pixel 9 XL mobile and 4K desktop displays
 */

import { create } from 'zustand';

/**
 * Layout mode for the developer toggle:
 * - native: use the real browser window dimensions (default)
 * - mobile-portrait: force 448x931, mobile layout
 * - mobile-landscape: force 931x448, mobile layout
 */
export type LayoutMode = 'native' | 'mobile-portrait' | 'mobile-landscape';

/**
 * Three-state layout variant resolved from viewport dimensions.
 * - desktop:  width >= 1024px
 * - portrait: width < 1024px, orientation portrait
 * - landscape: width < 1024px, orientation landscape
 */
export type LayoutVariant = 'desktop' | 'portrait' | 'landscape';

// Forced device dimensions used by the developer toggle
const FORCED_DIMENSIONS: Record<Exclude<LayoutMode, 'native'>, { width: number; height: number }> = {
  'mobile-portrait': { width: 448, height: 931 },
  'mobile-landscape': { width: 931, height: 448 },
};

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
  layoutVariant: LayoutVariant;
  manualOrientation: 'portrait' | 'landscape' | null;
  layoutMode: LayoutMode;
  initialize: () => void;
  setViewport: (width: number, height: number) => void;
  toggleOrientation: () => void;
  clearOrientationOverride: () => void;
  setLayoutMode: (mode: LayoutMode) => void;
}

/**
 * Resolve the three-state layout variant from width and orientation.
 */
function resolveLayoutVariant(width: number, orientation: 'portrait' | 'landscape'): LayoutVariant {
  if (width >= 1024) return 'desktop';
  return orientation === 'portrait' ? 'portrait' : 'landscape';
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
  layoutVariant: 'portrait',
  manualOrientation: null,
  layoutMode: 'native',

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
      const layoutVariant = resolveLayoutVariant(width, orientation);

      // Check URL for mobile parameter
      const isMobileFromURL = () => {
        const urlParams = new URLSearchParams(window.location.search);
        return urlParams.has('mobile') && (urlParams.get('mobile') === 'true' || urlParams.get('mobile') === '');
      };

      const isMobile = isMobileFromURL() || breakpoint === 'xs' || breakpoint === 'sm';

      set({
        width,
        height,
        breakpoint,
        orientation,
        isMobile,
        isPortrait: orientation === 'portrait',
        isLandscape: orientation === 'landscape',
        layoutVariant,
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
    const layoutVariant = resolveLayoutVariant(width, orientation);
    const layoutMode = get().layoutMode;

    // Check URL for mobile parameter
    const isMobileFromURL = () => {
      if (typeof window !== 'undefined') {
        const urlParams = new URLSearchParams(window.location.search);
        return urlParams.has('mobile') && (urlParams.get('mobile') === 'true' || urlParams.get('mobile') === '');
      }
      return false;
    };

    // In a forced layout mode, isMobile is always true
    const isMobile = layoutMode !== 'native' || isMobileFromURL() || breakpoint === 'xs' || breakpoint === 'sm';

    set({
      width,
      height,
      breakpoint,
      orientation,
      isMobile,
      isPortrait: orientation === 'portrait',
      isLandscape: orientation === 'landscape',
      layoutVariant,
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

    const isMobile = get().layoutMode !== 'native' || isMobileFromURL() || get().breakpoint === 'xs' || get().breakpoint === 'sm';
    const layoutVariant = resolveLayoutVariant(width, newOrientation);
    set({ isMobile, isPortrait: newOrientation === 'portrait', isLandscape: newOrientation === 'landscape', layoutVariant });
  },

  /**
   * Clear manual orientation override
   */
  clearOrientationOverride: () => {
    const { width, height } = get();
    const autoOrientation = width > height ? 'landscape' : 'portrait';
    const layoutVariant = resolveLayoutVariant(width, autoOrientation);
    set({ manualOrientation: null, orientation: autoOrientation, isPortrait: autoOrientation === 'portrait', isLandscape: autoOrientation === 'landscape', layoutVariant });
  },

  /**
   * Set the layout mode (developer toggle). Forces dimensions when not native.
   */
  setLayoutMode: (mode: LayoutMode) => {
    const prevMode = get().layoutMode;
    set({ layoutMode: mode });

    if (mode === 'native') {
      // Restore to real window size
      if (typeof window !== 'undefined') {
        get().setViewport(window.innerWidth, window.innerHeight);
      }
      return;
    }

    // Avoid a redundant recompute if already in the same forced mode
    if (prevMode === mode) return;

    const dims = FORCED_DIMENSIONS[mode];
    get().setViewport(dims.width, dims.height);
  },
}));