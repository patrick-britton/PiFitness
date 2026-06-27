/**
 * CredentialManager Component
 * Secure credential input/update interface with status checking.
 * Uses the encrypted credential storage mechanism via the backend.
 */

'use client';

import { useState } from 'react';
import { useCredentialRequirements, useUpsertCredentials, useDeleteCredentials } from '@/hooks/useAdmin';

/**
 * Confirm Delete Dialog
 */
function ConfirmDeleteDialog({
  serviceName,
  onConfirm,
  onCancel,
}: {
  serviceName: string;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl p-6 w-full max-w-sm mx-4">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">Delete Credentials?</h3>
        <p className="text-sm text-gray-600 dark:text-gray-400 mb-6">
          This will permanently remove stored credentials for <strong>{serviceName}</strong>.
          Any services using these credentials will fail to authenticate until new credentials are provided.
        </p>
        <div className="flex justify-end gap-3">
          <button
            onClick={onCancel}
            className="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 rounded-md hover:bg-gray-200 dark:hover:bg-gray-600"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            className="px-4 py-2 text-sm font-medium text-white bg-red-600 rounded-md hover:bg-red-700"
          >
            Delete
          </button>
        </div>
      </div>
    </div>
  );
}

/**
 * Credential Form for a specific service
 */
function CredentialForm({
  serviceName,
  requirements,
  onSuccess,
}: {
  serviceName: string;
  requirements?: string;
  onSuccess: () => void;
}) {
  const [credentialsJson, setCredentialsJson] = useState('');
  const [showForm, setShowForm] = useState(false);
  const upsertCredentials = useUpsertCredentials();

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!credentialsJson.trim()) return;

    // Validate JSON
    try {
      JSON.parse(credentialsJson);
    } catch {
      alert('Invalid JSON. Please check your input.');
      return;
    }

    upsertCredentials.mutate(
      { serviceName, rawCredentialsJson: credentialsJson.trim() },
      {
        onSuccess: () => {
          setCredentialsJson('');
          setShowForm(false);
          onSuccess();
        },
      }
    );
  };

  if (!showForm) {
    return (
      <button
        onClick={() => setShowForm(true)}
        className="px-3 py-1.5 text-sm font-medium text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300 border border-blue-300 dark:border-blue-700 rounded-md hover:bg-blue-50 dark:hover:bg-blue-900/20"
      >
        Set Credentials
      </button>
    );
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-3 p-4 bg-gray-50 dark:bg-gray-800 rounded-md border border-gray-200 dark:border-gray-700">
      <div>
        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
          Credentials JSON for <span className="font-semibold">{serviceName}</span>
        </label>
        {requirements && (
          <p className="text-xs text-gray-500 dark:text-gray-400 mb-2 font-mono">
            Requirements hint: {requirements}
          </p>
        )}
        <textarea
          value={credentialsJson}
          onChange={(e) => setCredentialsJson(e.target.value)}
          placeholder='{"username": "...", "password": "..."}'
          rows={4}
          className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm font-mono"
        />
      </div>
      <div className="flex justify-end gap-2">
        <button
          type="button"
          onClick={() => setShowForm(false)}
          className="px-3 py-1.5 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 rounded-md hover:bg-gray-200 dark:hover:bg-gray-600"
        >
          Cancel
        </button>
        <button
          type="submit"
          disabled={!credentialsJson.trim() || upsertCredentials.isPending}
          className="px-3 py-1.5 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-50"
        >
          {upsertCredentials.isPending ? 'Encrypting & Saving...' : 'Save Credentials'}
        </button>
      </div>
    </form>
  );
}

/**
 * CredentialManager Component
 */
export default function CredentialManager() {
  const { data, isLoading, error } = useCredentialRequirements();
  const deleteCredentials = useDeleteCredentials();
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);

  const services = data?.data || [];

  const handleDelete = (serviceName: string) => {
    deleteCredentials.mutate(serviceName, {
      onSuccess: () => {
        setMessage(`Credentials for '${serviceName}' deleted.`);
        setConfirmDelete(null);
        setTimeout(() => setMessage(null), 3000);
      },
      onError: (err) => {
        setMessage(`Delete failed: ${err}`);
        setConfirmDelete(null);
        setTimeout(() => setMessage(null), 5000);
      },
    });
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-4">
        <p className="text-red-700 dark:text-red-300">Failed to load credential requirements: {String(error)}</p>
      </div>
    );
  }

  return (
    <div>
      <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">
        Credential Management
      </h2>

      {message && (
        <div className={`mb-4 px-4 py-2 rounded-md text-sm ${
          message.includes('Failed') || message.includes('failed')
            ? 'bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 border border-red-200 dark:border-red-800'
            : 'bg-green-50 dark:bg-green-900/20 text-green-700 dark:text-green-300 border border-green-200 dark:border-green-800'
        }`}>
          {message}
        </div>
      )}

      {services.length === 0 ? (
        <div className="bg-gray-50 dark:bg-gray-800 rounded-md p-6 text-center">
          <p className="text-gray-500 dark:text-gray-400">No API services found to configure credentials for.</p>
        </div>
      ) : (
        <div className="space-y-4">
          {services.map((svc: any) => {
            const name = svc.api_service_name;
            const hasRequirements = !!svc.api_credential_requirements;

            return (
              <div
                key={name}
                className="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded-lg p-4"
              >
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-3">
                    <h3 className="text-base font-medium text-gray-900 dark:text-white">{name}</h3>
                    {hasRequirements ? (
                      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300">
                        Has Requirements
                      </span>
                    ) : (
                      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-400">
                        No Requirements
                      </span>
                    )}
                  </div>
                  <button
                    onClick={() => setConfirmDelete(name)}
                    className="px-2 py-1 text-xs font-medium text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300"
                  >
                    Delete Credentials
                  </button>
                </div>

                {hasRequirements && (
                  <p className="text-xs text-gray-500 dark:text-gray-400 mb-3 font-mono">
                    {svc.api_credential_requirements}
                  </p>
                )}

                <CredentialForm
                  serviceName={name}
                  requirements={svc.api_credential_requirements}
                  onSuccess={() => setMessage(`Credentials for '${name}' saved successfully.`)}
                />
              </div>
            );
          })}
        </div>
      )}

      {/* Delete Confirmation */}
      {confirmDelete && (
        <ConfirmDeleteDialog
          serviceName={confirmDelete}
          onConfirm={() => handleDelete(confirmDelete)}
          onCancel={() => setConfirmDelete(null)}
        />
      )}
    </div>
  );
}