/**
 * Efficiency bar tests (004-007 T04).
 *
 * Pins the AC-1/AC-2 geometry: the three worked examples, the equal-value
 * suppression case, and the null-fallback cases — plus the Chart.js spec
 * (MRAV-centered axis, MRAV→PAV floating bar, square corners, no gridlines,
 * end-label plugin) and direction-aware colors.
 */

import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';

import {
  EfficiencyBullets,
  buildEfficiencyBar,
  refVisible,
  type EfficiencyBarDatum,
} from '../EfficiencyBullets';
import { efficiencyBarGeometry } from '@/lib/efficiency-bar-geometry';

const PALETTE = {
  good: 'rgb(22 163 74)',
  bad: 'rgb(220 38 38)',
  tick: 'rgb(100 116 139)',
};

function datum(
  overrides: Partial<EfficiencyBarDatum> & {
    metric: EfficiencyBarDatum['metric'];
  },
): EfficiencyBarDatum {
  return {
    label: 'Cadence',
    unit: 'spm',
    format: (v) => v.toFixed(1),
    higherBetter: true,
    ...overrides,
  };
}

describe('efficiencyBarGeometry (AC-1/AC-2 worked examples)', () => {
  it('cadence 6/7 → axis 4.5–7.5, bar 6→7, red (higher-better, worse)', () => {
    const geo = efficiencyBarGeometry(6, 7, true)!;
    expect(geo.min).toBeCloseTo(4.5, 6);
    expect(geo.max).toBeCloseTo(7.5, 6);
    expect(geo.spanStart).toBeCloseTo(0.5, 6);
    expect(geo.spanEnd).toBeCloseTo(5 / 6, 6);
    expect(geo.noChange).toBe(false);
    expect(geo.improved).toBe(false);
  });

  it('GCT 225/229 → axis 219–231, bar 225→229, green (lower-better, better)', () => {
    const geo = efficiencyBarGeometry(225, 229, false)!;
    expect(geo.min).toBeCloseTo(219, 6);
    expect(geo.max).toBeCloseTo(231, 6);
    expect(geo.improved).toBe(true);
  });

  it('Vert Ratio 12.1/11.9 → axis 11.8–12.4, bar 11.9→12.1, red', () => {
    const geo = efficiencyBarGeometry(12.1, 11.9, false)!;
    expect(geo.min).toBeCloseTo(11.8, 6);
    expect(geo.max).toBeCloseTo(12.4, 6);
    expect(geo.improved).toBe(false);
  });

  it('equal values suppress the bar (OQ-3)', () => {
    expect(efficiencyBarGeometry(100, 100, true)!.noChange).toBe(true);
  });

  it('null inputs return null (AC-3 fallback)', () => {
    expect(efficiencyBarGeometry(null, 100, true)).toBeNull();
    expect(efficiencyBarGeometry(100, null, true)).toBeNull();
  });

  it('refVisible: present-and-positive only (sentinel guard)', () => {
    expect(refVisible(104.2)).toBe(true);
    expect(refVisible(null)).toBe(false);
    expect(refVisible(undefined)).toBe(false);
    expect(refVisible(-43.6375)).toBe(false);
    expect(refVisible(0)).toBe(false);
  });
});

describe('buildEfficiencyBar spec', () => {
  it('builds an MRAV-centered horizontal bar spanning MRAV→PAV', () => {
    const geo = efficiencyBarGeometry(6, 7, true)!;
    const spec = buildEfficiencyBar(geo, 6, 7, (v) => v.toFixed(1), PALETTE);
    expect(spec.options.indexAxis).toBe('y');
    expect(spec.options.scales?.x?.min).toBeCloseTo(4.5, 6);
    expect(spec.options.scales?.x?.max).toBeCloseTo(7.5, 6);
    expect(spec.data.datasets[0].data).toEqual([[6, 7]]);
    expect(spec.data.datasets[0].borderRadius).toBe(0);
    expect(spec.options.scales?.x?.grid?.display).toBe(false);
    expect(spec.options.scales?.y?.display).toBe(false);
  });

  it('colors the bar red when worse, green when better', () => {
    const bad = buildEfficiencyBar(
      efficiencyBarGeometry(6, 7, true)!,
      6,
      7,
      (v) => v.toFixed(1),
      PALETTE,
    );
    expect(bad.data.datasets[0].backgroundColor).toBe(PALETTE.bad);
    const good = buildEfficiencyBar(
      efficiencyBarGeometry(225, 229, false)!,
      225,
      229,
      (v) => String(v),
      PALETTE,
    );
    expect(good.data.datasets[0].backgroundColor).toBe(PALETTE.good);
  });

  it('attaches the efficiencyEndLabels plugin', () => {
    const spec = buildEfficiencyBar(
      efficiencyBarGeometry(6, 7, true)!,
      6,
      7,
      (v) => v.toFixed(1),
      PALETTE,
    );
    expect(spec.plugins).toHaveLength(1);
    expect(spec.plugins[0].id).toBe('efficiencyEndLabels');
  });

  it('draws the lower value at the left end and the higher at the right', () => {
    const fills: Array<{ text: unknown; x: unknown }> = [];
    const ctx = {
      save: vi.fn(),
      restore: vi.fn(),
      fillText: vi.fn(function (this: { textAlign: unknown }, text: unknown, x: unknown) {
        fills.push({ text, x });
      }),
      font: '',
      textBaseline: '',
      textAlign: 'left' as string,
      fillStyle: '',
    };
    const fakeChart = {
      ctx,
      chartArea: { top: 0, bottom: 24, left: 0, right: 200 },
      scales: { x: { getPixelForValue: (v: number) => v * 20 } },
    };
    const spec = buildEfficiencyBar(
      efficiencyBarGeometry(6, 7, true)!,
      6,
      7,
      (v) => v.toFixed(1),
      PALETTE,
    );
    const hook = (
      spec.plugins[0] as unknown as { afterDatasetsDraw: (c: object) => void }
    ).afterDatasetsDraw;
    hook(fakeChart);
    expect(fills).toHaveLength(2);
    expect(fills[0].text).toBe('6.0');
    expect(fills[1].text).toBe('7.0');
    expect(Number(fills[0].x)).toBeLessThan(Number(fills[1].x));
  });
});


describe('EfficiencyBullets rendering', () => {
  it('places the label LEFT of the Chart.js bar', () => {
    const { container } = render(
      <EfficiencyBullets
        data={[datum({ metric: { current: 6, prior10_avg: 7 } })]}
      />,
    );
    const row = screen.getByTestId('efficiency-row');
    expect(row.className).toContain('grid-cols-[88px_minmax(0,1fr)]');
    const cells = Array.from(row.children);
    expect(cells[0].textContent).toBe('Cadence');
    expect(cells[1].querySelector('canvas')).not.toBeNull();
    expect(container.querySelector('svg')).toBeNull();
  });

  it('renders four rows with left-hand labels', () => {
    render(
      <EfficiencyBullets
        data={[
          datum({ label: 'Cadence', metric: { current: 6, prior10_avg: 7 } }),
          datum({ label: 'Stride Length', metric: { current: 100, prior10_avg: 110 } }),
          datum({ label: 'GCT', higherBetter: false, metric: { current: 225, prior10_avg: 229 } }),
          datum({ label: 'Vert Ratio', higherBetter: false, metric: { current: 12.1, prior10_avg: 11.9 } }),
        ]}
      />,
    );
    expect(screen.getAllByTestId('efficiency-row')).toHaveLength(4);
    for (const label of ['Cadence', 'Stride Length', 'GCT', 'Vert Ratio']) {
      expect(screen.getByText(label)).toBeInTheDocument();
    }
  });
  it('shows no value caption under a rendered bar (Bug 004-007-7-2)', () => {
    render(
      <EfficiencyBullets
        data={[datum({ metric: { current: 171, prior10_avg: 174.7 } })]}
      />,
    );
    expect(screen.getByTestId('efficiency-bar')).toBeInTheDocument();
    // End values live on the bar (canvas labels + aria-label); no duplicate
    // caption line may render beneath it.
    expect(screen.queryByText(/prior-10 avg 174\.7/)).not.toBeInTheDocument();
  });


  it('suppresses the bar and shows "No change vs prior" on equal values', () => {
    render(
      <EfficiencyBullets
        data={[datum({ metric: { current: 100, prior10_avg: 100 } })]}
      />,
    );
    expect(screen.queryByTestId('efficiency-bar')).not.toBeInTheDocument();
    expect(screen.getByText(/No change vs prior/)).toBeInTheDocument();
  });

  it('shows no reading with honest text when current is null', () => {
    render(
      <EfficiencyBullets
        data={[datum({ metric: { current: null, prior10_avg: 7 } })]}
      />,
    );
    expect(screen.queryByTestId('efficiency-bar')).not.toBeInTheDocument();
    expect(screen.getByText(/no reading/)).toBeInTheDocument();
    expect(screen.getByText(/prior-10 avg 7\.0 spm/)).toBeInTheDocument();
  });

  it('suppresses a sentinel-poisoned ref with honest text', () => {
    render(
      <EfficiencyBullets
        data={[datum({ metric: { current: null, prior10_avg: -43.6375 } })]}
      />,
    );
    expect(screen.queryByTestId('efficiency-bar')).not.toBeInTheDocument();
    expect(screen.getByText(/prior-10 avg unavailable/)).toBeInTheDocument();
    expect(screen.queryByText(/-43\.6/)).not.toBeInTheDocument();
  });

  it('renders no prior attempts without a bar when ref is absent', () => {
    render(
      <EfficiencyBullets
        data={[datum({ metric: { current: 6, prior10_avg: null } })]}
      />,
    );
    expect(screen.queryByTestId('efficiency-bar')).not.toBeInTheDocument();
    expect(screen.getByText(/no prior attempts/)).toBeInTheDocument();
  });

  it('carries the mt-4 boundary gap below the charts stack', () => {
    render(
      <EfficiencyBullets
        data={[datum({ metric: { current: 6, prior10_avg: 7 } })]}
      />,
    );
    expect(
      screen.getByRole('group', {
        name: 'Running efficiency vs prior 10 course attempts',
      }),
    ).toHaveClass('mt-4');
  });
});
