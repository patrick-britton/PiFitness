'use client';

import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi, describe, it, expect, beforeEach } from 'vitest';
import ResetMatchTables from '../ResetMatchTables';
import { API } from '@/lib/api-client';

vi.mock('@/lib/api-client', () => ({
  API: { segments: { resetMatchTables: vi.fn() } },
}));

const resetMock = API.segments.resetMatchTables as unknown as ReturnType<typeof vi.fn>;

// The opener carries aria-label="Reset match tables" (exact accessible name);
// the dialog confirm is "Reset match tables" without ellipsis. Scope dialog
// queries inside the dialog so the two never collide.
const OPENER_LABEL = 'Reset match tables';

describe('ResetMatchTables', () => {
  beforeEach(() => {
    resetMock.mockReset();
  });

  it('opens the confirmation dialog and only posts on confirm (OQ-2)', async () => {
    const user = userEvent.setup();
    resetMock.mockResolvedValue({ message: 'Reset 5 tables.', truncated_count: 5 });
    const onReset = vi.fn();
    render(<ResetMatchTables onReset={onReset} />);

    await user.click(screen.getByRole('button', { name: OPENER_LABEL }));
    const dialog = await screen.findByRole('dialog');
    expect(dialog).toBeInTheDocument();
    expect(resetMock).not.toHaveBeenCalled();

    await user.click(within(dialog).getByRole('button', { name: 'Reset match tables' }));
    expect(resetMock).toHaveBeenCalledWith({ confirm_reset: true });
    expect(await screen.findByText('Reset 5 tables.')).toBeInTheDocument();
    expect(onReset).toHaveBeenCalled();
  });

  it('cancel closes the dialog without posting', async () => {
    const user = userEvent.setup();
    render(<ResetMatchTables onReset={vi.fn()} />);

    await user.click(screen.getByRole('button', { name: OPENER_LABEL }));
    expect(await screen.findByRole('dialog')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(resetMock).not.toHaveBeenCalled();
  });

  it('reports a reset failure inside the dialog', async () => {
    const user = userEvent.setup();
    resetMock.mockRejectedValueOnce(new Error('boom'));
    render(<ResetMatchTables onReset={vi.fn()} />);

    await user.click(screen.getByRole('button', { name: OPENER_LABEL }));
    const failDialog = await screen.findByRole('dialog');
    await user.click(
      within(failDialog).getByRole('button', { name: 'Reset match tables' }),
    );

    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Failed to reset the match tables.',
    );
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });
});
