import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import NavBar from '../NavBar';
import { useUIStore } from '../../../stores/uiStore';

let mockPathname = '/';

// Mock next/navigation useRouter and usePathname
vi.mock('next/navigation', () => ({
  useRouter: () => ({
    push: vi.fn(),
  }),
  usePathname: () => mockPathname,
}));

describe('NavBar Component (UI Principles R1, R2, R3)', () => {
  beforeEach(() => {
    mockPathname = '/';
    useUIStore.setState({ activeModule: 'home', activeSubPage: null });
  });

  it('renders all 7 navigation modules with active pressed touch states on home', () => {
    render(<NavBar />);

    const buttons = screen.getAllByRole('button');
    expect(buttons.length).toBe(7);

    // Each button must have active pressed classes per UI Principles R3
    buttons.forEach((btn) => {
      expect(btn.className).toContain('active:scale-95');
    });
  });

  it('contextually switches to Home + sub-pages when inside a module', () => {
    mockPathname = '/music';
    useUIStore.setState({ activeModule: 'music', activeSubPage: 'now-playing' });
    render(<NavBar />);

    const buttons = screen.getAllByRole('button');
    // Music has 5 sub-pages + 1 Home button = 6 buttons
    expect(buttons.length).toBe(6);
    expect(screen.getByText('Home')).toBeDefined();
    expect(screen.getByText('Now Playing')).toBeDefined();
    expect(screen.getByText('Ratings')).toBeDefined();

    buttons.forEach((btn) => {
      expect(btn.className).toContain('active:scale-95');
    });
  });
});
