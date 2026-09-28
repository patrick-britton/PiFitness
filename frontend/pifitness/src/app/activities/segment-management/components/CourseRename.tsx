'use client';

import { useState } from 'react';
import { API } from '@/lib/api-client';
import { CourseInfo } from '@/lib/types/segment-management';

interface CourseRenameProps {
  course: CourseInfo;
  onRenamed: (newName: string) => void;
}

/** Rename-in-place (FR-16, legacy update_course_name semantics). */
export default function CourseRename({ course, onRenamed }: CourseRenameProps) {
  const [newName, setNewName] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);

  const save = async () => {
    const trimmed = newName.trim();
    if (!trimmed) return;
    setLoading(true);
    setError(null);
    setMessage(null);
    try {
      const res = await API.segments.renameSegment(course.course_id, { new_name: trimmed });
      onRenamed(res.segment_name);
      setMessage('Name saved.');
      setNewName('');
    } catch {
      setError('Failed to rename the course.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md p-3 space-y-2">
      <label
        htmlFor="course-new-name"
        className="block text-xs font-medium text-gray-500 dark:text-gray-400"
      >
        Update name for course #{course.course_id}
      </label>
      <div className="flex flex-wrap items-center gap-2">
        <input
          id="course-new-name"
          type="text"
          value={newName}
          onChange={(e) => setNewName(e.target.value)}
          placeholder="New course name…"
          className="flex-1 min-w-[180px] rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
        <button
          type="button"
          onClick={save}
          disabled={loading || !newName.trim()}
          className="px-3 py-1.5 text-sm font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {loading ? 'Saving…' : 'Save name'}
        </button>
      </div>
      {message && (
        <p role="status" className="text-sm text-green-700 dark:text-green-400">
          {message}
        </p>
      )}
      {error && (
        <p role="alert" className="text-sm text-red-600 dark:text-red-400">
          {error}
        </p>
      )}
    </div>
  );
}
