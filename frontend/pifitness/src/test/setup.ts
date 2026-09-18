/**
 * Vitest global setup (008-005 T10).
 *
 * Adds @testing-library/jest-dom matchers to vitest's expect and unmounts
 * rendered trees between tests so queries never see stale DOM.
 */

import '@testing-library/jest-dom/vitest';
import { afterEach } from 'vitest';
import { cleanup } from '@testing-library/react';

afterEach(() => {
  cleanup();
});