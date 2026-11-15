/**
 * Vitest global setup (008-005 T10).
 *
 * Adds @testing-library/jest-dom matchers to vitest's expect and unmounts
 * rendered trees between tests so queries never see stale DOM.
 */

import '@testing-library/jest-dom/vitest';
import { afterEach } from 'vitest';
import { cleanup } from '@testing-library/react';

// Minimal browser API shims for Vitest (jsdom) environment
Object.defineProperty(window.HTMLMediaElement.prototype, 'play', {
  configurable: true,
  value: () => Promise.resolve(),
});

Object.defineProperty(window.HTMLCanvasElement.prototype, 'getContext', {
  configurable: true,
  value: (type: string) => {
    // Return a simple mock object sufficient for test imports
    return {
      // mock methods can be added as needed
      fillRect: () => {},
      drawImage: () => {},
      // ...other canvas methods used in the code base can be stubbed here
    };
  },
});

afterEach(() => {
  cleanup();
});