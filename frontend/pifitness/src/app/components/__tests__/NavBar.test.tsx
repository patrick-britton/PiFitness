import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import NavBar from '../NavBar';

// Mock next/navigation useRouter
vi.mock('next/navigation', () => ({
  useRouter: () => ({
    push: vi.fn(),
  }),
}));

describe('NavBar Component (UI Principles R1, R2, R3)', () => {
  it('renders all navigation buttons with active pressed touch states (UI Principles R3)', () => {
    render(<NavBar />);

    const buttons = screen.getAllByRole('button');
    expect(buttons.length).toBeGreaterThanOrEqual(5);

    // Each button must have active pressed classes per UI Principles R3
    buttons.forEach((btn) => {
      expect(btn.className).toContain('active:scale-95');
    });
  });
});
