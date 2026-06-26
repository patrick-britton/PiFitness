/**
 * Custom hook for responsive viewport detection
 * Supports automatic detection, URL parameters, and manual toggle
 */

import { useState, useEffect } from 'react';

/**
 * Viewport breakpoints optimized for Pixel 9 XL and 4K displays
 */
const BREAKPOINTS = {
  xs: 0,      // Mobile portrait (Pixel 9 XL: 412px)
  sm: 640,    // Mobile landscape threshold
  md: 768,    // Tablet/desktop threshold (Pixel 9 XL landscape: 840px)
  lg: 1024,   // Larger desktop optimizations
  xl: 1440,   // 4K display optimizations
};

/**
 * Orientation detection for mobile devices
 */
const ORIENTATIONS = {
  PORTRAIT: 'portrait',
  LANDSCAPE: 'landscape',
} as const;

type Orientation = typeof ORIENTATIONS[keyof typeof ORIENTATIONS];

/**
 * Custom hook for responsive viewport detection
 * @returns Viewport information and orientation control
 */
export function useViewport() {
  const [width, setWidth] = useState<number>(typeof window !== 'undefined' ? window.innerWidth : 0);
  const [height, setHeight] = useState<number>(typeof window !== 'undefined' ? window.innerHeight : 0);
  const [manualOrientation, setManualOrientation] = useState<Orientation | null>(null);
  const [isMobileOverride, setIsMobileOverride] = useState<boolean>(false);

  // Check URL for mobile parameter (like legacy Streamlit)
  const isMobileFromURL = () => {
    if (typeof window !== 'undefined') {
      const urlParams = new URLSearchParams(window.location.search);
      return urlParams.has('mobile') && (urlParams.get('mobile') === 'true' || urlParams.get('mobile') === '');
    }
    return false;
  };

  // Detect current breakpoint
  const getCurrentBreakpoint = (currentWidth: number): keyof typeof BREAKPOINTS => {
    if (currentWidth >= BREAKPOINTS.xl) return 'xl';
    if (currentWidth >= BREAKPOINTS.lg) return 'lg';
    if (currentWidth >= BREAKPOINTS.md) return 'md';
    if (currentWidth >= BREAKPOINTS.sm) return 'sm';
    return 'xs';
  };

  // Detect orientation based on width/height ratio
  const detectOrientation = (w: number, h: number): Orientation => {
    return w > h ? ORIENTATIONS.LANDSCAPE : ORIENTATIONS.PORTRAIT;
  };

  // Current breakpoint
  const breakpoint = getCurrentBreakpoint(width);

  // Automatic orientation detection
  const autoOrientation = detectOrientation(width, height);

  // Final orientation (manual override or automatic)
  const orientation = manualOrientation || autoOrientation;

  // Determine if we should use mobile layout
  const isMobile = isMobileOverride || isMobileFromURL() || breakpoint === 'xs' || (breakpoint === 'sm' && orientation === ORIENTATIONS.PORTRAIT);

  // Handle window resize
  useEffect(() => {
    if (typeof window === 'undefined') return;

    const handleResize = () => {
      setWidth(window.innerWidth);
      setHeight(window.innerHeight);
    };

    // Set initial size
    handleResize();

    // Add event listener
    window.addEventListener('resize', handleResize);

    // Cleanup
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  // Toggle between portrait and landscape for mobile devices
  const toggleOrientation = () => {
    setManualOrientation(prev =>
      prev === ORIENTATIONS.PORTRAIT ? ORIENTATIONS.LANDSCAPE : ORIENTATIONS.PORTRAIT
    );
  };

  // Clear manual orientation override
  const clearOrientationOverride = () => {
    setManualOrientation(null);
  };

  // Toggle mobile override
  const toggleMobileOverride = () => {
    setIsMobileOverride(prev => !prev);
  };

  return {
    width,
    height,
    breakpoint,
    orientation,
    isMobile,
    isPortrait: orientation === ORIENTATIONS.PORTRAIT,
    isLandscape: orientation === ORIENTATIONS.LANDSCAPE,
    toggleOrientation,
    clearOrientationOverride,
    toggleMobileOverride,
    isMobileOverride,
    manualOrientation,
    BREAKPOINTS,
    ORIENTATIONS,
  };
}

/**
 * Type guard for orientation
 */
export function isOrientation(value: any): value is Orientation {
  return value === ORIENTATIONS.PORTRAIT || value === ORIENTATIONS.LANDSCAPE;
}