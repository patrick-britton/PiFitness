/**
 * ServiceConfig Component
 * Manages API service integrations and function library entries.
 * Dual-panel layout: API Services list + Function Library.
 * Includes ServiceAuthStatus indicators at the top.
 */

'use client';

import { useState } from 'react';
import { useServices, useAddService, useUpdateService, useDeleteService, useFunctions, useAddFunction, useUpdateFunction, useDeleteFunction } from '@/hooks/useAdmin';
import ServiceAuthStatus from './ServiceAuthStatus';

/**
 * Edit Service Form (Inline)
 */
function EditServiceForm({
  serviceName,
  initialCreds,
  onClose,
  onSave,
}: {
  serviceName: string;
  initialCreds: string;
  onClose: () => void;
  onSave: (creds: string) => void;
}) {
  const [credentialRequirements, setCredentialRequirements] = useState(initialCreds || '');

  return (
    <div className="space-y-2">
      <div>
        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
          Credential Requirements (comma-separated)
        </label>
        <input
          type="text"
          value={credentialRequirements}
          onChange={(e) => setCredentialRequirements(e.target.value)}
          onBlur={() => onSave(credentialRequirements.trim())}
          placeholder="e.g. client_id, client_secret"
          className="w-full px-2 py-1 text-xs border border-gray-300 dark:border-gray-600 rounded shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
          autoFocus
        />
      </div>
      <div className="flex justify-end gap-2">
        <button
          type="button"
          onClick={onClose}
          className="px-2 py-1 text-xs font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 rounded hover:bg-gray-200 dark:hover:bg-gray-600"
        >
          Cancel
        </button>
      </div>
    </div>
  );
}

/**
 * Add Service Form
 */
function AddServiceForm({ onClose }: { onClose: () => void }) {
  const [serviceName, setServiceName] = useState('');
  const [credentialRequirements, setCredentialRequirements] = useState('');
  const addService = useAddService();

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!serviceName.trim()) return;
    addService.mutate({ serviceName: serviceName.trim(), credentialRequirements: credentialRequirements.trim() || undefined }, {
      onSuccess: () => onClose(),
    });
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-3">
      <div>
        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
          Service Name
        </label>
        <input
          type="text"
          value={serviceName}
          onChange={(e) => setServiceName(e.target.value)}
          placeholder="e.g. garmin, spotify"
          className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
          autoFocus
        />
      </div>
      <div>
        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
          Credential Requirements (comma-separated)
        </label>
        <input
          type="text"
          value={credentialRequirements}
          onChange={(e) => setCredentialRequirements(e.target.value)}
          placeholder="e.g. client_id, client_secret"
          className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
        />
      </div>
      <div className="flex justify-end gap-2">
        <button
          type="button"
          onClick={onClose}
          className="px-3 py-1.5 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 rounded-md hover:bg-gray-200 dark:hover:bg-gray-600"
        >
          Cancel
        </button>
        <button
          type="submit"
          disabled={!serviceName.trim() || addService.isPending}
          className="px-3 py-1.5 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-50"
        >
          {addService.isPending ? 'Adding...' : 'Add Service'}
        </button>
      </div>
    </form>
  );
}

/**
 * Add/Edit Function Library Entry Form
 */
function FunctionForm({
  initial,
  onClose,
}: {
  initial?: { friendly_name: string; api_service_name: string; python_extraction_function: string; description?: string };
  onClose: () => void;
}) {
  const isEdit = !!initial;
  const [friendlyName, setFriendlyName] = useState(initial?.friendly_name || '');
  const [apiServiceName, setApiServiceName] = useState(initial?.api_service_name || '');
  const [pyFunction, setPyFunction] = useState(initial?.python_extraction_function || '');
  const [description, setDescription] = useState(initial?.description || '');

  const addFunction = useAddFunction();
  const updateFunction = useUpdateFunction();

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!friendlyName.trim() || !apiServiceName.trim() || !pyFunction.trim()) return;

    const entry = {
      friendly_name: friendlyName.trim(),
      api_service_name: apiServiceName.trim(),
      python_extraction_function: pyFunction.trim(),
      description: description.trim() || undefined,
    };

    if (isEdit && initial) {
      updateFunction.mutate({ friendlyName: initial.friendly_name, entry }, {
        onSuccess: () => onClose(),
      });
    } else {
      addFunction.mutate(entry, {
        onSuccess: () => onClose(),
      });
    }
  };

  const isPending = addFunction.isPending || updateFunction.isPending;

  return (
    <form onSubmit={handleSubmit} className="space-y-3">
      <div>
        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
          Friendly Name *
        </label>
        <input
          type="text"
          value={friendlyName}
          onChange={(e) => setFriendlyName(e.target.value)}
          className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
          disabled={isEdit}
          autoFocus
        />
      </div>
      <div>
        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
          API Service Name *
        </label>
        <input
          type="text"
          value={apiServiceName}
          onChange={(e) => setApiServiceName(e.target.value)}
          className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
        />
      </div>
      <div>
        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
          Python Extraction Function *
        </label>
        <input
          type="text"
          value={pyFunction}
          onChange={(e) => setPyFunction(e.target.value)}
          placeholder="e.g. extract_activities"
          className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
        />
      </div>
      <div>
        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
          Description
        </label>
        <textarea
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          rows={2}
          className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
        />
      </div>
      <div className="flex justify-end gap-2">
        <button
          type="button"
          onClick={onClose}
          className="px-3 py-1.5 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 rounded-md hover:bg-gray-200 dark:hover:bg-gray-600"
        >
          Cancel
        </button>
        <button
          type="submit"
          disabled={!friendlyName.trim() || !apiServiceName.trim() || !pyFunction.trim() || isPending}
          className="px-3 py-1.5 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-50"
        >
          {isPending ? 'Saving...' : isEdit ? 'Update' : 'Add'}
        </button>
      </div>
    </form>
  );
}

/**
 * Confirm Delete Dialog
 */
function ConfirmDelete({
  label,
  onConfirm,
  onCancel,
}: {
  label: string;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl p-6 w-full max-w-sm mx-4">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">Confirm Delete</h3>
        <p className="text-sm text-gray-600 dark:text-gray-400 mb-6">
          Are you sure you want to delete <strong>{label}</strong>?
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
 * ServiceConfig Component
 */
export default function ServiceConfig() {
  const { data: servicesData, isLoading: servicesLoading, error: servicesError } = useServices();
  const { data: functionsData, isLoading: functionsLoading, error: functionsError } = useFunctions();
  const deleteService = useDeleteService();
  const deleteFunction = useDeleteFunction();
  const updateService = useUpdateService();

  const [showAddService, setShowAddService] = useState(false);
  const [showAddFunction, setShowAddFunction] = useState(false);
  const [editingService, setEditingService] = useState<string | null>(null);
  const [editingFunction, setEditingFunction] = useState<any>(null);
  const [confirmDelete, setConfirmDelete] = useState<{ type: 'service' | 'function'; label: string; action: () => void } | null>(null);

  const services = servicesData?.data || [];
  const functions = functionsData?.data || [];

  return (
    <div>
      <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">
        Service & Function Library Configuration
      </h2>

      {/* Auth Status Indicators - At top of Services tab */}
      <ServiceAuthStatus />

      {/* API Services Panel */}
      <div className="mb-8">
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-lg font-medium text-gray-800 dark:text-gray-200">API Services</h3>
          <button
            onClick={() => setShowAddService(true)}
            className="px-3 py-1.5 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700"
          >
            + Add Service
          </button>
        </div>

        {servicesLoading ? (
          <div className="flex items-center justify-center py-6">
            <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600" />
          </div>
        ) : servicesError ? (
          <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-3">
            <p className="text-red-700 dark:text-red-300 text-sm">Error: {String(servicesError)}</p>
          </div>
        ) : services.length === 0 ? (
          <p className="text-gray-500 dark:text-gray-400 text-sm">No API services configured.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
              <thead className="bg-gray-50 dark:bg-gray-800">
                <tr>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Service Name</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Credential Requirements</th>
                  <th className="px-4 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Actions</th>
                </tr>
              </thead>
              <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-800">
                {services.map((svc: any) => {
                  const isEditing = editingService === (svc.api_service_name || svc.service_name);
                  return (
                    <tr key={svc.api_service_name || svc.service_name} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                      <td className="px-4 py-2 text-sm font-medium text-gray-900 dark:text-white">
                        {svc.api_service_name || svc.service_name}
                      </td>
                      <td className="px-4 py-2 text-xs">
                        {isEditing ? (
                          <EditServiceForm
                            serviceName={svc.api_service_name || svc.service_name}
                            initialCreds={svc.api_credential_requirements || ''}
                            onClose={() => setEditingService(null)}
                            onSave={(creds) => {
                              updateService.mutate({ 
                                serviceName: svc.api_service_name || svc.service_name, 
                                credentialRequirements: creds 
                              });
                              setEditingService(null);
                            }}
                          />
                        ) : (
                          <span className="text-gray-500 dark:text-gray-400 font-mono max-w-xs truncate cursor-pointer"
                                onClick={() => setEditingService(svc.api_service_name || svc.service_name)}>
                            {svc.api_credential_requirements || '-'}
                          </span>
                        )}
                      </td>
                      <td className="px-4 py-2 text-right">
                        <div className="flex justify-end gap-2">
                          {!isEditing && (
                            <button
                              onClick={() => setEditingService(svc.api_service_name || svc.service_name)}
                              className="px-2 py-1 text-xs font-medium text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300"
                            >
                              Edit
                            </button>
                          )}
                          <button
                            onClick={() => setConfirmDelete({
                              type: 'service',
                              label: svc.api_service_name || svc.service_name,
                              action: () => deleteService.mutate(svc.api_service_name || svc.service_name),
                            })}
                            className="px-2 py-1 text-xs font-medium text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300 disabled:opacity-50"
                            disabled={isEditing}
                          >
                            Delete
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        {/* Add Service Inline Form */}
        {showAddService && (
          <div className="mt-3 p-4 bg-gray-50 dark:bg-gray-800 rounded-md border border-gray-200 dark:border-gray-700">
            <AddServiceForm onClose={() => setShowAddService(false)} />
          </div>
        )}
      </div>

      {/* Function Library Panel */}
      <div>
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-lg font-medium text-gray-800 dark:text-gray-200">Function Library</h3>
          <button
            onClick={() => setShowAddFunction(true)}
            className="px-3 py-1.5 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700"
          >
            + Add Function
          </button>
        </div>

        {functionsLoading ? (
          <div className="flex items-center justify-center py-6">
            <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600" />
          </div>
        ) : functionsError ? (
          <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md p-3">
            <p className="text-red-700 dark:text-red-300 text-sm">Error: {String(functionsError)}</p>
          </div>
        ) : functions.length === 0 ? (
          <p className="text-gray-500 dark:text-gray-400 text-sm">No function library entries.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
              <thead className="bg-gray-50 dark:bg-gray-800">
                <tr>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Friendly Name</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">API Service</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Python Function</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Description</th>
                  <th className="px-4 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Actions</th>
                </tr>
              </thead>
              <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-800">
                {functions.map((fn: any) => (
                  <tr key={fn.friendly_name} className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                    <td className="px-4 py-2 text-sm font-medium text-gray-900 dark:text-white">
                      {fn.friendly_name}
                    </td>
                    <td className="px-4 py-2 text-sm text-gray-600 dark:text-gray-400">
                      {fn.api_service_name}
                    </td>
                    <td className="px-4 py-2 text-sm font-mono text-gray-600 dark:text-gray-400">
                      {fn.python_extraction_function}
                    </td>
                    <td className="px-4 py-2 text-xs text-gray-500 dark:text-gray-400 max-w-xs truncate">
                      {fn.description || '-'}
                    </td>
                    <td className="px-4 py-2 text-right">
                      <div className="flex justify-end gap-2">
                        <button
                          onClick={() => setEditingFunction(fn)}
                          className="px-2 py-1 text-xs font-medium text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300"
                        >
                          Edit
                        </button>
                        <button
                          onClick={() => setConfirmDelete({
                            type: 'function',
                            label: fn.friendly_name,
                            action: () => deleteFunction.mutate(fn.friendly_name),
                          })}
                          className="px-2 py-1 text-xs font-medium text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300"
                        >
                          Delete
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Add Function Inline Form */}
        {showAddFunction && (
          <div className="mt-3 p-4 bg-gray-50 dark:bg-gray-800 rounded-md border border-gray-200 dark:border-gray-700">
            <FunctionForm onClose={() => setShowAddFunction(false)} />
          </div>
        )}

        {/* Edit Function Form (Modal) */}
        {editingFunction && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl p-6 w-full max-w-lg mx-4 max-h-[90vh] overflow-y-auto">
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                Edit Function: {editingFunction.friendly_name}
              </h3>
              <FunctionForm
                initial={editingFunction}
                onClose={() => setEditingFunction(null)}
              />
            </div>
          </div>
        )}
      </div>

      {/* Delete Confirmation Modal */}
      {confirmDelete && (
        <ConfirmDelete
          label={confirmDelete.label}
          onConfirm={() => {
            confirmDelete.action();
            setConfirmDelete(null);
          }}
          onCancel={() => setConfirmDelete(null)}
        />
      )}
    </div>
  );
}