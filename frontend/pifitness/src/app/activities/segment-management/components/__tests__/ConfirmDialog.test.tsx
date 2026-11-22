import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import ConfirmDialog from '../ConfirmDialog';

describe('ConfirmDialog Component (UI Principles R3 & R7)', () => {
  it('renders title, message, and action buttons', () => {
    render(
      <ConfirmDialog
        title="Reset Tables"
        message="Are you sure you want to proceed?"
        confirmLabel="Reset"
        onCancel={vi.fn()}
        onConfirm={vi.fn()}
      />
    );

    expect(screen.getByRole('dialog', { name: /Reset Tables/i })).toBeInTheDocument();
    expect(screen.getByText('Are you sure you want to proceed?')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Cancel/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /^Reset$/i })).toBeInTheDocument();
  });

  it('triggers onConfirm when confirm button is clicked', async () => {
    const user = userEvent.setup();
    const handleConfirm = vi.fn();
    render(
      <ConfirmDialog
        title="Reset Tables"
        message="Message"
        confirmLabel="Reset"
        onCancel={vi.fn()}
        onConfirm={handleConfirm}
      />
    );

    await user.click(screen.getByRole('button', { name: /^Reset$/i }));
    expect(handleConfirm).toHaveBeenCalledTimes(1);
  });

  it('triggers onCancel when cancel button is clicked', async () => {
    const user = userEvent.setup();
    const handleCancel = vi.fn();
    render(
      <ConfirmDialog
        title="Reset Tables"
        message="Message"
        confirmLabel="Reset"
        onCancel={handleCancel}
        onConfirm={vi.fn()}
      />
    );

    await user.click(screen.getByRole('button', { name: /Cancel/i }));
    expect(handleCancel).toHaveBeenCalledTimes(1);
  });

  it('dismisses via Android back gesture (popstate) per UI Principles R7', () => {
    const handleCancel = vi.fn();
    render(
      <ConfirmDialog
        title="Reset Tables"
        message="Message"
        confirmLabel="Reset"
        onCancel={handleCancel}
        onConfirm={vi.fn()}
      />
    );

    fireEvent(window, new PopStateEvent('popstate', { state: null }));
    expect(handleCancel).toHaveBeenCalledTimes(1);
  });

  it('dismisses on Escape key', () => {
    const handleCancel = vi.fn();
    render(
      <ConfirmDialog
        title="Reset Tables"
        message="Message"
        confirmLabel="Reset"
        onCancel={handleCancel}
        onConfirm={vi.fn()}
      />
    );

    fireEvent.keyDown(window, { key: 'Escape' });
    expect(handleCancel).toHaveBeenCalledTimes(1);
  });

  it('has active pressed tactile styling on buttons per UI Principles R3', () => {
    render(
      <ConfirmDialog
        title="Reset Tables"
        message="Message"
        confirmLabel="Reset"
        onCancel={vi.fn()}
        onConfirm={vi.fn()}
      />
    );

    const cancelBtn = screen.getByRole('button', { name: /Cancel/i });
    const confirmBtn = screen.getByRole('button', { name: /^Reset$/i });

    expect(cancelBtn.className).toContain('active:scale-95');
    expect(confirmBtn.className).toContain('active:scale-95');
  });
});
