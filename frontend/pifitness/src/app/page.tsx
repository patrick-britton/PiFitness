'use client';

/**
 * Home Page
 * Placeholder page for the root route '/'
 */

import React, { useState } from 'react';
import IsrcDupeHomeBanner from './components/IsrcDupeHomeBanner';
import {
  QUICK_LOG_BUTTONS,
  toLogEntryRequest,
} from '../lib/types/home-contract';
import { API } from '../lib/api-client';

export default function HomePage() {
  const [confirmMsg, setConfirmMsg] = useState<string | null>(null);

  const handleQuickLog = (button: typeof QUICK_LOG_BUTTONS[number]) => {
    const request = toLogEntryRequest(button);
    API.food.logEntry(request)
      .then(() => {
        setConfirmMsg(`Logged ${button.label}`);
        setTimeout(() => setConfirmMsg(null), 3000);
      })
      .catch(console.error);
  };

  return (
    <div className="flex flex-col items-center min-h-screen bg-gray-50 dark:bg-gray-900 px-4 py-8">
      <div className="w-full max-w-2xl">
        <IsrcDupeHomeBanner />
      </div>
      <h1 className="text-3xl font-bold text-gray-900 dark:text-white mt-8">
        Wow, such empty
      </h1>
      {confirmMsg && (
        <p className="mt-6 text-sm text-green-700 dark:text-green-400">
          {confirmMsg}
        </p>
      )}
      <div className="flex gap-4 mt-8">
        {QUICK_LOG_BUTTONS.map((button) => (
          <button
            key={button.id}
            onClick={() => handleQuickLog(button)}
            className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
          >
            {button.label}
          </button>
        ))}
      </div>
    </div>
  );
}