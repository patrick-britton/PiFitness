import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import BottomSheet from '../BottomSheet';

describe('BottomSheet Component (UI Principles R6, R7, R3)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders nothing when isOpen is false', () => {
    render(
      <BottomSheet isOpen={false} onClose={vi.fn()} title="Test Sheet">
        <div>Sheet Content</div>
      </BottomSheet>
    );
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('renders dialog and content when isOpen is true', () => {
    render(
      <BottomSheet isOpen={true} onClose={vi.fn()} title="Test Sheet">
        <div>Sheet Content</div>
      </BottomSheet>
    );
    const dialog = screen.getByRole('dialog', { name: /Test Sheet/i });
    expect(dialog).toBeInTheDocument();
    expect(screen.getByText('Sheet Content')).toBeInTheDocument();
  });

  it('dismisses when close button is clicked', async () => {
    const user = userEvent.setup();
    const handleClose = vi.fn();
    render(
      <BottomSheet isOpen={true} onClose={handleClose} title="Test Sheet">
        <div>Sheet Content</div>
      </BottomSheet>
    );

    const closeBtn = screen.getByRole('button', { name: /Close sheet/i });
    await user.click(closeBtn);
    expect(handleClose).toHaveBeenCalledTimes(1);
  });

  it('dismisses on backdrop tap when dismissOnBackdrop is true (default)', () => {
    const handleClose = vi.fn();
    render(
      <BottomSheet isOpen={true} onClose={handleClose} title="Test Sheet">
        <div>Sheet Content</div>
      </BottomSheet>
    );

    fireEvent.click(screen.getByRole('presentation'));
    expect(handleClose).toHaveBeenCalledTimes(1);
  });

  it('does not dismiss on backdrop tap when dismissOnBackdrop is false', () => {
    const handleClose = vi.fn();
    render(
      <BottomSheet isOpen={true} onClose={handleClose} title="Test Sheet" dismissOnBackdrop={false}>
        <div>Sheet Content</div>
      </BottomSheet>
    );

    fireEvent.click(screen.getByRole('presentation'));
    expect(handleClose).not.toHaveBeenCalled();
  });

  it('dismisses when Escape key is pressed (UI Principles R7)', () => {
    const handleClose = vi.fn();
    render(
      <BottomSheet isOpen={true} onClose={handleClose} title="Test Sheet">
        <div>Sheet Content</div>
      </BottomSheet>
    );

    fireEvent.keyDown(window, { key: 'Escape' });
    expect(handleClose).toHaveBeenCalledTimes(1);
  });

  it('dismisses when Android back gesture triggers popstate (UI Principles R7 / B25)', () => {
    const handleClose = vi.fn();
    render(
      <BottomSheet isOpen={true} onClose={handleClose} title="Test Sheet">
        <div>Sheet Content</div>
      </BottomSheet>
    );

    // Simulate Android system back gesture triggering browser popstate
    fireEvent(window, new PopStateEvent('popstate', { state: null }));
    expect(handleClose).toHaveBeenCalledTimes(1);
  });

  it('dismisses when swiped down sufficiently (>75px) via touch gesture (UI Principles R6 / B24)', () => {
    const handleClose = vi.fn();
    render(
      <BottomSheet isOpen={true} onClose={handleClose} title="Test Sheet">
        <div>Sheet Content</div>
      </BottomSheet>
    );

    const dragHandleArea = screen.getByRole('dialog').firstElementChild as HTMLElement;
    // Simulate touch swipe down of 100px
    fireEvent.touchStart(dragHandleArea, {
      touches: [{ clientY: 100 } as Touch],
    });
    fireEvent.touchMove(dragHandleArea, {
      touches: [{ clientY: 200 } as Touch],
    });
    fireEvent.touchEnd(dragHandleArea);

    expect(handleClose).toHaveBeenCalledTimes(1);
  });

  it('does not dismiss when touch drag is below threshold (<75px)', () => {
    const handleClose = vi.fn();
    render(
      <BottomSheet isOpen={true} onClose={handleClose} title="Test Sheet">
        <div>Sheet Content</div>
      </BottomSheet>
    );

    const dragHandleArea = screen.getByRole('dialog').firstElementChild as HTMLElement;
    // Simulate small touch drag of only 30px
    fireEvent.touchStart(dragHandleArea, {
      touches: [{ clientY: 100 } as Touch],
    });
    fireEvent.touchMove(dragHandleArea, {
      touches: [{ clientY: 130 } as Touch],
    });
    fireEvent.touchEnd(dragHandleArea);

    expect(handleClose).not.toHaveBeenCalled();
  });
});
