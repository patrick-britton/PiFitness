/**
 * MapStyleSelector behaviour tests (009-003 T08, FR-3).
 *
 * jsdom has no WebGL, so what is asserted here is the gating logic the backend
 * contract drives: token-gated styles appear only when the server reports the
 * satellite/token capability, and a selection the catalogue no longer allows is
 * re-pointed at an offered style. Map rendering itself stays a human check.
 */

import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';

import MapStyleSelector, { mergeMapStyles } from '../MapStyleSelector';
import { API } from '@/lib/api-client';
import type { MapStyle } from '@/lib/types/segment-management';

vi.mock('@/lib/api-client', () => ({
  API: { segments: { getMapStyles: vi.fn() } },
}));

const getMapStyles = vi.mocked(API.segments.getMapStyles);

const CATALOGUE: MapStyle[] = [
  { id: 'positron', name: 'Positron', requires_satellite_token: false },
  { id: 'darkmatter', name: 'Dark Matter', requires_satellite_token: false },
  { id: 'mb-dark', name: 'MB Dark', requires_satellite_token: true },
  { id: 'mb-satellite', name: 'Satellite', requires_satellite_token: true },
];

beforeEach(() => {
  getMapStyles.mockReset();
});

describe('mergeMapStyles', () => {
  it('drops token-gated styles when the token is unavailable', () => {
    const merged = mergeMapStyles('positron', CATALOGUE, false);
    expect(merged.options.map((s) => s.id)).toEqual(['positron', 'darkmatter']);
  });

  it('offers satellite styles once the server reports them available', () => {
    const merged = mergeMapStyles('positron', CATALOGUE, true);
    expect(merged.options.map((s) => s.id)).toEqual(['positron', 'darkmatter', 'mb-dark', 'mb-satellite']);
  });

  it('re-points an unavailable selection at the first offered style', () => {
    expect(mergeMapStyles('mb-satellite', CATALOGUE, false).value).toBe('positron');
  });

  it('keeps a selection the catalogue still allows', () => {
    expect(mergeMapStyles('mb-satellite', CATALOGUE, true).value).toBe('mb-satellite');
  });

  it('leaves the value alone when no style is offered at all', () => {
    const merged = mergeMapStyles('positron', [], false);
    expect(merged.options).toEqual([]);
    expect(merged.value).toBe('positron');
  });
});

describe('MapStyleSelector', () => {
  it('shows only free styles when no token is configured, and re-points a Mapbox selection', async () => {
    getMapStyles.mockResolvedValue({ data: CATALOGUE, satellite_available: false });
    const onChange = vi.fn();
    render(<MapStyleSelector id="style" value="mb-dark" onChange={onChange} />);

    await waitFor(() => expect(screen.getByRole('combobox')).not.toBeDisabled());
    const options = screen.getAllByRole('option').map((o) => o.textContent);
    expect(options).toEqual(['Positron', 'Dark Matter']);
    // The re-point runs in a passive effect ONE COMMIT after the catalogue
    // resolves, while the combobox is already enabled in that same commit — so
    // this call must be awaited, never asserted synchronously after the waitFor
    // above (Bug 009-010-11-3: a loaded CPU makes the one-tick race fail).
    await waitFor(() => expect(onChange).toHaveBeenCalledWith('positron'));
  });

  it('offers satellite styles when the token is available', async () => {
    getMapStyles.mockResolvedValue({ data: CATALOGUE, satellite_available: true });
    render(<MapStyleSelector id="style" value="positron" onChange={vi.fn()} />);

    await waitFor(() => expect(screen.getAllByRole('option')).toHaveLength(4));
    expect(screen.getByRole('option', { name: 'Satellite' })).toBeInTheDocument();
  });

  it('disables the selector and reports an error when the catalogue fails', async () => {
    getMapStyles.mockRejectedValue(new Error('nope'));
    render(<MapStyleSelector id="style" value="positron" onChange={vi.fn()} />);

    await waitFor(() => expect(screen.getByText('Basemap list unavailable')).toBeInTheDocument());
    expect(screen.getByRole('combobox')).toBeDisabled();
  });
});