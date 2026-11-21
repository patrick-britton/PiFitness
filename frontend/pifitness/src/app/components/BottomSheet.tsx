'use client';

import React, { useRef, useState, useId, useCallback } from 'react';
import { useBackGestureDismiss } from '@/hooks/useBackGestureDismiss';
import CloseIcon from '@mui/icons-material/Close';

export interface BottomSheetProps {
  /** Whether the bottom sheet is open */
  isOpen: boolean;
  /** Callback invoked when the sheet is dismissed (backdrop tap, drag down, escape, back gesture) */
  onClose: () => void;
  /** Optional title for the header */
  title?: string;
  /** Optional custom accessible label */
  ariaLabel?: string;
  /** Content rendered inside the sheet */
  children: React.ReactNode;
  /** Optional extra classes for the sheet panel */
  className?: string;
  /** Identifier for history state tracking */
  id?: string;
  /** Whether tapping the backdrop dismisses the sheet (default: true) */
  dismissOnBackdrop?: boolean;
}

/**
 * BottomSheet Component
 * Implements mobile-first sliding bottom sheets for secondary flows per UI Principles R6,
 * with Android back-gesture dismissal per UI Principles R7, touch swipe-down to dismiss,
 * and tactile active pressed visual feedback per UI Principles R3.
 */
export default function BottomSheet({
  isOpen,
  onClose,
  title,
  ariaLabel,
  children,
  className = '',
  id = 'bottom-sheet',
  dismissOnBackdrop = true,
}: BottomSheetProps) {
  const titleId = useId();
  const [dragOffset, setDragOffset] = useState<number>(0);
  const touchStartY = useRef<number | null>(null);

  // Android back gesture & Escape key handling (UI Principles R7)
  useBackGestureDismiss({
    isOpen,
    onDismiss: onClose,
    id,
  });

  const handleTouchStart = useCallback((e: React.TouchEvent) => {
    touchStartY.current = e.touches[0].clientY;
  }, []);

  const handleTouchMove = useCallback((e: React.TouchEvent) => {
    if (touchStartY.current === null) return;
    const currentY = e.touches[0].clientY;
    const deltaY = currentY - touchStartY.current;
    if (deltaY > 0) {
      // Only permit dragging downwards
      setDragOffset(deltaY);
    }
  }, []);

  const handleTouchEnd = useCallback(() => {
    if (dragOffset > 75) {
      // Swiped down sufficiently to trigger dismissal
      onClose();
    }
    touchStartY.current = null;
    setDragOffset(0);
  }, [dragOffset, onClose]);

  if (!isOpen) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-end justify-center bg-black/50 backdrop-blur-xs transition-opacity sm:items-center sm:p-4"
      role="presentation"
      onClick={dismissOnBackdrop ? onClose : undefined}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby={title ? titleId : undefined}
        aria-label={!title ? (ariaLabel || 'Bottom Sheet') : undefined}
        onClick={(e) => e.stopPropagation()}
        style={{
          transform: dragOffset > 0 ? `translateY(${dragOffset}px)` : undefined,
          transition: dragOffset > 0 ? 'none' : 'transform 200ms ease-out',
        }}
        className={`w-full max-w-lg rounded-t-2xl sm:rounded-2xl border-t sm:border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 shadow-2xl overflow-hidden max-h-[90vh] flex flex-col pb-[calc(1rem+env(safe-area-inset-bottom,0px))] sm:pb-4 ${className}`}
      >
        {/* Drag handle header for touch gesture dismissal (R6) */}
        <div
          className="w-full flex flex-col items-center pt-2 pb-1 cursor-grab touch-none select-none"
          onTouchStart={handleTouchStart}
          onTouchMove={handleTouchMove}
          onTouchEnd={handleTouchEnd}
          aria-hidden="true"
        >
          <div className="w-12 h-1.5 rounded-full bg-gray-300 dark:bg-gray-600 mb-2" />
        </div>

        {/* Optional Title Bar with Dismiss Button */}
        {title && (
          <div className="flex items-center justify-between px-6 pb-3 border-b border-gray-100 dark:border-gray-700/60">
            <h2 id={titleId} className="text-lg font-semibold text-gray-900 dark:text-white">
              {title}
            </h2>
            <button
              type="button"
              onClick={onClose}
              aria-label="Close sheet"
              className="p-1 rounded-full text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 active:scale-95 active:bg-gray-200 dark:active:bg-gray-600 transition-all"
            >
              <CloseIcon className="w-5 h-5" />
            </button>
          </div>
        )}

        {/* Sheet Content Body */}
        <div className="px-6 py-4 overflow-y-auto flex-1">
          {children}
        </div>
      </div>
    </div>
  );
}
