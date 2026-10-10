import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';

import {
  EfficiencyBullets,
  buildBulletChart,
  bulletDomain,
  bulletFrac,
  refFraction,
  refVisible,
  type EfficiencyBulletDatum,
} from '../EfficiencyBullets';

const PALETTE = {
  bar: 'rgb(0 114 178)',
  ref: 'rgb(202 138 4)',
  refOutline: 'rgb(15 23 42)',
  tick: 'rgb(100 116 139)',
  grid: 'rgb(226 232 240)',
};

function datum(
  overrides: Partial<EfficiencyBulletDatum> & {
    metric: EfficiencyBulletDatum['metric'];
  },
): EfficiencyBulletDatum {
  return {
    label: 'Cadence',
    unit: 'spm',
    format: (v) => v.toFixed(1),
    ...overrides,
  };
}

describe('bullet math helpers', () => {
  it('refVisible: present-and-positive only', () => {
    expect(refVisible(104.2)).toBe(true);
    expect(refVisible(null)).toBe(false);
    expect(refVisible(undefined)).toBe(false);
    expect(refVisible(-43.6375)).toBe(false);
    expect(refVisible(0)).toBe(false);
  });

  it('refFraction positions the reference within [0,1)', () => {
    expect(refFraction(171, 172.9)).toBeCloseTo(172.9 / (172.9 * 1.1), 6);
    expect(refFraction(null, 104.2)).toBeCloseTo(1 / 1.1, 6);
  });

  it('bulletDomain/bulletFrac stay non-negative and bounded', () => {
    expect(bulletDomain(null, -43.6)).toEqual([0, 1]);
    expect(bulletFrac(-43.6, 1)).toBe(0);
    expect(bulletFrac(500, 100)).toBe(1);
  });
});

describe('buildBulletChart spec (T06-1 Chart.js rebuild)', () => {
  it('builds a horizontal bar at the current value with x 0 to max', () => {
    const spec = buildBulletChart(171, 172.9, 190.19, PALETTE);
    expect(spec.options.indexAxis).toBe('y');
    expect(spec.options.scales?.x?.min).toBe(0);
    expect(spec.options.scales?.x?.max).toBe(190.19);
    expect(spec.data.datasets[0].data).toEqual([171]);
    expect(spec.data.datasets[0].backgroundColor).toBe(PALETTE.bar);
  });

  it('feeds an empty dataset for a null current so no origin stub draws', () => {
    const spec = buildBulletChart(null, 104.2, 114.62, PALETTE);
    expect(spec.data.datasets[0].data).toEqual([]);
    expect(spec.plugins).toHaveLength(1);
  });

  it('attaches the bulletRefLine plugin when the ref is usable', () => {
    const spec = buildBulletChart(171, 172.9, 190.19, PALETTE);
    expect(spec.plugins).toHaveLength(1);
    expect(spec.plugins[0].id).toBe('bulletRefLine');
  });

  it('omits the plugin when the ref is absent', () => {
    expect(buildBulletChart(171, null, 188.1, PALETTE).plugins).toEqual([]);
  });

  it('draws a constant-width vertical line at the ref x pixel', () => {
    const strokes: Array<{ style: unknown; width: unknown }> = [];
    const ctx = {
      save: vi.fn(),
      restore: vi.fn(),
      beginPath: vi.fn(),
      moveTo: vi.fn(),
      lineTo: vi.fn(),
      stroke: vi.fn(function (this: { strokeStyle: unknown; lineWidth: number }) {
        strokes.push({ style: this.strokeStyle, width: this.lineWidth });
      }),
      strokeStyle: 'init',
      lineWidth: 0,
    };
    const fakeChart = {
      ctx,
      chartArea: { top: 0, bottom: 24, left: 0, right: 200 },
      scales: { x: { getPixelForValue: (v: number) => v * 2 } },
    };
    const spec = buildBulletChart(171, 100, 190.19, PALETTE);
    expect(spec.plugins).toHaveLength(1);
    const hook = (
      spec.plugins[0] as unknown as { afterDatasetsDraw: (c: object) => void }
    ).afterDatasetsDraw;
    hook(fakeChart);
    expect(ctx.moveTo).toHaveBeenCalledWith(200, -2);
    expect(ctx.lineTo).toHaveBeenCalledWith(200, 26);
    expect(strokes).toEqual([
      { style: PALETTE.refOutline, width: 6 },
      { style: PALETTE.ref, width: 3 },
    ]);
  });
});

describe('EfficiencyBullets rendering', () => {
  it('places the short label LEFT of the Chart.js bar (T06-1 recheck)', () => {
    const { container } = render(
      <EfficiencyBullets
        data={[datum({ metric: { current: 171, prior10_avg: 172.9 } })]}
      />,
    );
    const row = screen.getByTestId('bullet-row');
    expect(row.className).toContain('grid-cols-[88px_minmax(0,1fr)]');
    const cells = Array.from(row.children);
    expect(cells).toHaveLength(3);
    expect(cells[0].textContent).toBe('Cadence');
    expect(cells[1].querySelector('canvas')).not.toBeNull();
    expect(container.querySelector('svg')).toBeNull();
  });

  it('marks the ref host plus honest text for a usable average (T06-1)', () => {
    render(
      <EfficiencyBullets
        data={[datum({ metric: { current: 171, prior10_avg: 172.9 } })]}
      />,
    );
    const host = screen.getByTestId('bullet-ref-line');
    expect(host.querySelector('canvas')).not.toBeNull();
    expect(screen.getByText(/prior-10 avg 172\.9 spm/)).toBeInTheDocument();
  });

  it('shows the marker and no reading when current is null', () => {
    render(
      <EfficiencyBullets
        data={[datum({ metric: { current: null, prior10_avg: 104.2 } })]}
      />,
    );
    expect(screen.getByTestId('bullet-ref-line')).toBeInTheDocument();
    expect(screen.getByText(/no reading/)).toBeInTheDocument();
    expect(screen.getByText(/prior-10 avg 104\.2 spm/)).toBeInTheDocument();
  });

  it('suppresses a sentinel-poisoned ref with honest text (T06-2)', () => {
    render(
      <EfficiencyBullets
        data={[datum({ metric: { current: null, prior10_avg: -43.6375 } })]}
      />,
    );
    expect(screen.queryByTestId('bullet-ref-line')).not.toBeInTheDocument();
    expect(screen.getByText(/prior-10 avg unavailable/)).toBeInTheDocument();
    expect(screen.queryByText(/-43\.6/)).not.toBeInTheDocument();
  });

  it('renders no prior attempts without a marker when ref is absent', () => {
    render(
      <EfficiencyBullets
        data={[datum({ metric: { current: 171, prior10_avg: null } })]}
      />,
    );
    expect(screen.queryByTestId('bullet-ref-line')).not.toBeInTheDocument();
    expect(screen.getByText(/no prior attempts/)).toBeInTheDocument();
  });

  it('carries the mt-4 boundary gap below the charts stack (T05-1)', () => {
    render(
      <EfficiencyBullets
        data={[datum({ metric: { current: 171, prior10_avg: 172.9 } })]}
      />,
    );
    expect(
      screen.getByRole('group', {
        name: 'Running efficiency vs prior 10 course attempts',
      }),
    ).toHaveClass('mt-4');
  });
});

