import { useEffect, useRef } from 'react';

interface UseBackGestureDismissOptions {
  /** Whether the modal, overlay, or sheet is currently open */
  isOpen: boolean;
  /** Callback to invoke when dismissed via back gesture, escape key, or backdrop */
  onDismiss: () => void;
  /** Optional identifier for debugging or nested tracking */
  id?: string;
  /** Whether escape key should also trigger dismissal (default: true) */
  enableEscape?: boolean;
}

/**
 * Custom hook providing Android back-gesture and history navigation integration
 * for modals, overlays, and bottom sheets per UI Principles R7.
 *
 * When an overlay opens, a synthetic history state is pushed.
 * If the user performs an Android back gesture or browser back,
 * the topmost modal/sheet is dismissed without leaving the app page.
 * When closed programmatically via UI, the history state is cleanly unwound.
 */
export function useBackGestureDismiss({
  isOpen,
  onDismiss,
  id = 'modal',
  enableEscape = true,
}: UseBackGestureDismissOptions) {
  const onDismissRef = useRef(onDismiss);
  onDismissRef.current = onDismiss;

  useEffect(() => {
    if (!isOpen || typeof window === 'undefined') return;

    let dismissedByPopState = false;
    const stateId = `${id}-${Date.now()}`;

    // Push state so back gesture pops this state instead of navigating away
    window.history.pushState({ modalOpen: true, modalId: stateId }, '');

    const handlePopState = () => {
      dismissedByPopState = true;
      onDismissRef.current();
    };

    const handleKeyDown = (e: KeyboardEvent) => {
      if (enableEscape && e.key === 'Escape') {
        onDismissRef.current();
      }
    };

    window.addEventListener('popstate', handlePopState);
    if (enableEscape) {
      window.addEventListener('keydown', handleKeyDown);
    }

    return () => {
      window.removeEventListener('popstate', handlePopState);
      if (enableEscape) {
        window.removeEventListener('keydown', handleKeyDown);
      }

      // If closed via UI (not via back button/gesture), pop the synthetic state
      if (!dismissedByPopState && window.history.state?.modalOpen) {
        window.history.back();
      }
    };
  }, [isOpen, id, enableEscape]);
}
