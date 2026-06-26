export default function Home() {
  return (
    <main className="p-8">
      <h2 className="text-2xl font-semibold text-gray-900 dark:text-white mb-4">
        Dashboard
      </h2>
      <p className="text-gray-600 dark:text-gray-400">
        React frontend is working! 🚀
      </p>
      <p>
        <a
          href="/api/health"
          className="text-blue-600 dark:text-blue-400 hover:underline"
        >
          API Health Check
        </a>
      </p>
    </main>
  );
}
