/**
 * Home Page
 * Placeholder page for the root route '/'
 */

import IsrcDupeHomeBanner from './components/IsrcDupeHomeBanner';

export default function HomePage() {
  return (
    <div className="flex flex-col items-center min-h-screen bg-gray-50 dark:bg-gray-900 px-4 py-8">
      <div className="w-full max-w-2xl">
        <IsrcDupeHomeBanner />
      </div>
      <h1 className="text-3xl font-bold text-gray-900 dark:text-white mt-8">
        Wow, such empty
      </h1>
    </div>
  );
}