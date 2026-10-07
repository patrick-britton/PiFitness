/** T07: RecipeSteps - one step at a time, timer math, portions, edit/remove. */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import RecipeSteps, {
  draftToStep, partsToSeconds, secondsToParts,
} from '../RecipeSteps';
import { useViewportStore } from '@/stores/viewportStore';
import type { RecipeStep } from '@/lib/types/food-contract';
import type { SessionIngredient } from '../RecipeCreate';

const LINES: SessionIngredient[] = [
  { food_id: 11, name: 'Oats', qty: 100, unit: 'g', kcalPer100g: 389 },
  { food_id: 22, name: 'Milk', qty: 200, unit: 'ml', kcalPer100g: 60 },
];

function Harness({ initial = [] as RecipeStep[] }: { initial?: RecipeStep[] }) {
  const [steps, setSteps] = useState<RecipeStep[]>(initial);
  return (
    <>
      <RecipeSteps lines={LINES} steps={steps} onChange={setSteps} />
      <output data-testid="steps-json">{JSON.stringify(steps)}</output>
    </>
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  useViewportStore.setState({ layoutVariant: 'desktop' });
});

describe('RecipeSteps', () => {
  it('renders in all three layouts', () => {
    for (const v of ['desktop', 'portrait', 'landscape'] as const) {
      useViewportStore.setState({ layoutVariant: v });
      const { unmount } = render(<Harness />);
      expect(screen.getByTestId('recipe-steps')).toBeInTheDocument();
      unmount();
    }
  });

  it('timer math: default 60s, min/sec map to int seconds', () => {
    expect(partsToSeconds(true, '1', '0')).toBe(60);
    expect(partsToSeconds(true, '2', '30')).toBe(150);
    expect(partsToSeconds(false, '5', '0')).toBeNull();
    expect(secondsToParts(150)).toEqual({ minutes: 2, seconds: 30 });
  });

  it('adds one step at a time with default 60s timer', async () => {
    const user = userEvent.setup();
    render(<Harness />);
    await user.type(screen.getByLabelText('Instruction'), 'Whisk it.');
    await user.click(screen.getByRole('button', { name: 'Add step 1' }));
    const steps = JSON.parse(screen.getByTestId('steps-json').textContent ?? '[]');
    expect(steps).toHaveLength(1);
    expect(steps[0]).toMatchObject({ step_no: 1, instruction: 'Whisk it.', timer_seconds: 60, foods: [] });
    // Skip path: a recipe with zero steps still saves (empty list valid).
    expect(steps[0].timer_seconds).toBe(60);
  });

  it('attaches the same ingredient to two steps with partial qty', async () => {
    const user = userEvent.setup();
    render(<Harness />);
    // Step 1: half the oats.
    await user.type(screen.getByLabelText('Instruction'), 'Mix half.');
    await user.selectOptions(screen.getByLabelText('Ingredient'), '11');
    await user.clear(screen.getByLabelText('Qty'));
    await user.type(screen.getByLabelText('Qty'), '50');
    await user.click(screen.getByRole('button', { name: 'Attach' }));
    await user.click(screen.getByRole('button', { name: 'Add step 1' }));
    // Step 2: the other half of the same ingredient.
    await user.type(screen.getByLabelText('Instruction'), 'Top it.');
    await user.selectOptions(screen.getByLabelText('Ingredient'), '11');
    await user.clear(screen.getByLabelText('Qty'));
    await user.type(screen.getByLabelText('Qty'), '50');
    await user.click(screen.getByRole('button', { name: 'Attach' }));
    await user.click(screen.getByRole('button', { name: 'Add step 2' }));
    const steps = JSON.parse(screen.getByTestId('steps-json').textContent ?? '[]');
    expect(steps).toHaveLength(2);
    expect(steps[0].foods).toEqual([{ food_id: 11, qty: 50, unit: 'g' }]);
    expect(steps[1].foods).toEqual([{ food_id: 11, qty: 50, unit: 'g' }]);
  });

  it('edits and removes a step, renumbering survivors', async () => {
    const user = userEvent.setup();
    render(<Harness />);
    await user.type(screen.getByLabelText('Instruction'), 'First.');
    await user.click(screen.getByRole('button', { name: 'Add step 1' }));
    await user.type(screen.getByLabelText('Instruction'), 'Second.');
    await user.click(screen.getByRole('button', { name: 'Add step 2' }));
    await user.click(screen.getByRole('button', { name: 'Remove step 1' }));
    let steps = JSON.parse(screen.getByTestId('steps-json').textContent ?? '[]');
    expect(steps).toHaveLength(1);
    expect(steps[0]).toMatchObject({ step_no: 1, instruction: 'Second.' });
    // Edit text + disable timer -> null.
    await user.click(screen.getByRole('button', { name: 'Edit step 1' }));
    await user.clear(screen.getByLabelText('Instruction'));
    await user.type(screen.getByLabelText('Instruction'), 'Second, revised.');
    await user.click(screen.getByLabelText('Timer'));
    await user.click(screen.getByRole('button', { name: 'Save step 1' }));
    steps = JSON.parse(screen.getByTestId('steps-json').textContent ?? '[]');
    expect(steps[0]).toMatchObject({ instruction: 'Second, revised.', timer_seconds: null });
  });

  it('draftToStep rejects empty instruction and bad portions', () => {
    expect(draftToStep(
      { instruction: '  ', timerEnabled: true, minutes: '1', seconds: '0', portions: [] }, 1,
    ).error).toMatch(/instruction/i);
    expect(draftToStep(
      {
        instruction: 'Stir.', timerEnabled: false, minutes: '0', seconds: '0',
        portions: [{ key: 1, food_id: 11, qty: '0', unit: 'g' }],
      }, 1,
    ).error).toMatch(/quantity/i);
  });
});
