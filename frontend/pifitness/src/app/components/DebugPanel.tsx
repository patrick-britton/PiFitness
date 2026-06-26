/**
 * Debug Panel Component
 * Toggleable debug information display
 */

'use client';

import { useState, useEffect } from 'react';
import { useViewportStore } from '../../stores/viewportStore';
import { useUIStore } from '../../stores/uiStore';

export default function DebugPanel() {
  const [isDebugVisible, setIsDebugVisible] = useState(false);
  const { width, height, breakpoint, orientation, isMobile, isMobileOverride } = useViewportStore();
  const { activeModule, theme } = useUIStore();
  const [environmentInfo, setEnvironmentInfo] = useState('');
  const [mobileDetectionReason, setMobileDetectionReason] = useState('');

  // Detect environment and mobile reasons
  useEffect(() => {
    if (typeof window !== 'undefined') {
      // Environment detection
      const isStaticExport = document.getElementById('__next') !== null;
      setEnvironmentInfo(isStaticExport ? 'Static Export' : 'Dev Server');

      // Mobile detection reasoning
      let reason = '';
      if (isMobileOverride) {
        reason = 'Manual override';
      } else if (breakpoint === 'xs') {
        reason = 'Breakpoint xs';
      } else if (breakpoint === 'sm' && orientation === 'portrait') {
        reason = 'Breakpoint sm + portrait';
      } else {
        const urlParams = new URLSearchParams(window.location.search);
        if (urlParams.has('mobile')) {
          reason = 'URL parameter ?mobile';
        } else {
          reason = 'Not mobile';
        }
      }
      setMobileDetectionReason(reason);
    }
  }, [breakpoint, orientation, isMobileOverride]);

  // Load debug visibility from localStorage
  useEffect(() => {
    if (typeof window !== 'undefined') {
      const savedDebugState = localStorage.getItem('pifitness-debug');
      if (savedDebugState === 'true') {
        setIsDebugVisible(true);
      }
    }
  }, []);

  // Save debug visibility to localStorage
  useEffect(() => {
    if (typeof window !== 'undefined') {
      localStorage.setItem('pifitness-debug', isDebugVisible.toString());
    }
  }, [isDebugVisible]);

  const toggleDebug = () => {
    setIsDebugVisible(!isDebugVisible);
  };

  if (!isDebugVisible) return null;

  return (
    <div className="fixed bottom-4 right-4 z-50 bg-gray-900 text-white p-4 rounded-lg shadow-lg max-w-sm text-xs font-mono">
      <div className="flex justify-between items-center mb-2">
        <h3 className="font-bold">DEBUG INFO</h3>
        <button
          onClick={toggleDebug}
          className="text-white hover:text-gray-300"
          aria-label="Close debug panel"
        >
          ✕
        </button>
      </div>

      <div className="space-y-1">
        <div><span className="text-gray-400">Environment:</span> {environmentInfo}</div>
        <div><span className="text-gray-400">Viewport:</span> {width}x{height}</div>
        <div><span className="text-gray-400">Breakpoint:</span> {breakpoint}</div>
        <div><span className="text-gray-400">Orientation:</span> {orientation}</div>
        <div><span className="text-gray-400">Mobile:</span> {isMobile ? 'YES' : 'NO'}</div>
        <div><span className="text-gray-400">Mobile Reason:</span> {mobileDetectionReason}</div>
        <div><span className="text-gray-400">Active Module:</span> {activeModule}</div>
        <div><span className="text-gray-400">Theme:</span> {theme}</div>
        <div><span className="text-gray-400">User Agent:</span> {typeof navigator !== 'undefined' ? navigator.userAgent.substring(0, 30) + '...' : 'N/A'}</div>
      </div>
    </div>
  );
}