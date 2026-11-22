'use client';

import { useEffect } from 'react';

/**
 * ServiceWorkerRegister Component
 * Registers the PWA service worker for offline caching per UI Principles R10.
 */
export default function ServiceWorkerRegister() {
  useEffect(() => {
    if (typeof window !== 'undefined' && 'serviceWorker' in navigator) {
      navigator.serviceWorker
        .register('/sw.js')
        .then((registration) => {
          // Listen for updates
          registration.onupdatefound = () => {
            const installingWorker = registration.installing;
            if (installingWorker) {
              installingWorker.onstatechange = () => {
                if (installingWorker.state === 'installed' && navigator.serviceWorker.controller) {
                  // New content is available
                }
              };
            }
          };
        })
        .catch(() => {
          // Service worker registration can fail in unsupported environments or security contexts
        });
    }
  }, []);

  return null;
}
