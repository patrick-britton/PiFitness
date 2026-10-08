/**
 * 004-005 T12 / AC-9 — a failing task row renders its `last_failure_message`
 * alongside the Error badge (not a bare badge).
 *
 * Run from frontend/pifitness:
 *   npx vitest run src/app/admin/components/__tests__
 */
import { render, screen } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import TaskSummary from '../TaskSummary';

vi.mock('@/hooks/useAdmin', () => ({
  useTaskSummaryChart: () => ({
    data: {
      data: [
        {
          task_name: 'Activity Details',
          is_active_failure: true,
          last_failure_message: '5 - No API response to load',
          last_executed_utc: new Date().toISOString(),
          next_planned_execution_utc: new Date().toISOString(),
          execution_count: 10,
          success_count: 5,
          login_ms: 1, extract_ms: 1, load_ms: 1, flatten_ms: 1,
          parse_ms: 1, interpolation_ms: 1, forecasting_ms: 1,
          python_ms: 1, admin_ms: 1,
        },
      ],
    },
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  }),
  useExecuteTask: () => ({ mutateAsync: vi.fn(), isPending: false }),
}));

describe('TaskSummary failure detail (AC-9)', () => {
  it('renders last_failure_message next to the Error badge', () => {
    render(<TaskSummary />);
    expect(screen.getByText('Error')).toBeInTheDocument();
    expect(screen.getByText('5 - No API response to load')).toBeInTheDocument();
  });
});
