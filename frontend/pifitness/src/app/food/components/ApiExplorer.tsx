/**
 * Food API Explorer — dev-only raw-JSON diagnostic viewer (010-001).
 * USDA text search + a live camera barcode scanner (OFF) sharing one pretty-print
 * <pre> display. Typed barcode entry exists only as the camera's fallback. OFF
 * status:0 shown as-is with an adjacent (not inline) hint. Three layouts;
 * loading/empty/error states; Tailwind theme classes only.
 */

'use client';

import { useCallback, useState } from 'react';
import dynamic from 'next/dynamic';
import { useViewportStore } from '../../../stores/viewportStore';
import { API } from '../../../lib/api-client';
import type { FoodLookupResponse } from '../../../lib/types/food-lookup';
import type { ScannerErrorCode } from '../../../lib/barcode/camera';

/**
 * The scanner is camera/wasm code, so it is loaded client-side only (`ssr: false`)
 * — nothing may execute during SSR. Its decoder is itself imported dynamically
 * inside the hook, which is the second half of that guarantee.
 */
const BarcodeScanner = dynamic(() => import('./BarcodeScanner'), {
  ssr: false,
  loading: () => (
    <p className="text-xs text-gray-500 dark:text-gray-400">Loading camera scanner…</p>
  ),
});

/** Dev-only simulate hook (never present in a production build). */
const DEV_SIMULATE_VALUE =
  process.env.NODE_ENV !== 'production' ? '737628064502' : undefined;

type PanelState =
  | { kind: 'empty' }
  | { kind: 'loading' }
  | { kind: 'error'; message: string }
  | { kind: 'result'; label: string; response: FoodLookupResponse };

function prettyPrint(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function isOffMiss(response: FoodLookupResponse): boolean {
  const body = response.body as { status?: unknown } | null;
  return typeof body === 'object' && body !== null && body.status === 0;
}

export function RawPanel({ state }: { state: PanelState }) {
  if (state.kind === 'empty') {
    return (
      <div className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4">
        <p className="text-sm text-gray-500 dark:text-gray-400">
          No lookup yet. Run a USDA search or a barcode lookup to see the raw JSON here.
        </p>
      </div>
    );
  }
  if (state.kind === 'loading') {
    return (
      <div className="animate-pulse space-y-3" aria-live="polite" aria-busy="true">
        <div className="h-5 bg-gray-200 dark:bg-gray-700 rounded-md w-48" />
        <div className="h-64 bg-gray-200 dark:bg-gray-700 rounded-lg" />
      </div>
    );
  }
  if (state.kind === 'error') {
    return (
      <div role="alert" className="rounded-lg border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-4 text-red-700 dark:text-red-300">
        <p className="font-medium">Lookup failed</p>
        <pre className="text-sm mt-2 whitespace-pre-wrap break-words font-mono">{state.message}</pre>
      </div>
    );
  }
  return (
    <div className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4">
      <div className="flex flex-wrap items-center gap-2">
        <p className="text-sm font-medium text-gray-900 dark:text-white">{state.label}</p>
        <span className="text-xs text-gray-500 dark:text-gray-400">upstream status: {state.response.upstreamStatus}</span>
        {state.response.rateLimitRemaining != null && (
          <span className="text-xs text-gray-500 dark:text-gray-400">rate-limit remaining: {state.response.rateLimitRemaining}</span>
        )}
      </div>
      {isOffMiss(state.response) && (
        <p className="mt-2 text-sm text-amber-700 dark:text-amber-300" role="note">
          Note: this barcode is not in Open Food Facts (body status is 0) — raw body shown as-is below.
        </p>
      )}
      <pre className="mt-2 max-h-[32rem] overflow-auto rounded-md bg-gray-50 dark:bg-gray-900 p-3 text-xs leading-relaxed text-gray-900 dark:text-gray-100 font-mono whitespace-pre-wrap break-words">
        {prettyPrint(state.response.body)}
      </pre>
    </div>
  );
}
export default function ApiExplorer() {
  const { layoutVariant } = useViewportStore();
  const isDesktop = layoutVariant === 'desktop';
  const isLandscape = layoutVariant === 'landscape';
  const [query, setQuery] = useState('');
  const [pageSize, setPageSize] = useState('10');
  const [dataType, setDataType] = useState('');
  const [barcode, setBarcode] = useState('');
  const [scannerActive, setScannerActive] = useState(false);
  const [scannerError, setScannerError] = useState<ScannerErrorCode | null>(null);
  const [manualMode, setManualMode] = useState(false);
  const [state, setState] = useState<PanelState>({ kind: 'empty' });

  const runUsda = useCallback(async () => {
    const q = query.trim();
    if (!q) { setState({ kind: 'error', message: 'Enter a food name first.' }); return; }
    const n = Math.max(1, Math.min(parseInt(pageSize, 10) || 10, 100));
    setState({ kind: 'loading' });
    try {
      const response = await API.foodLookup.searchUsda(q, n, dataType.trim() || undefined);
      setState({ kind: 'result', label: `USDA search: ${q}`, response });
    } catch (e) {
      setState({ kind: 'error', message: e instanceof Error ? e.message : 'USDA lookup failed.' });
    }
  }, [query, pageSize, dataType]);

  const runOff = useCallback(async (rawCode: string) => {
    const code = rawCode.trim();
    if (!code) { setState({ kind: 'error', message: 'Enter a barcode first.' }); return; }
    setState({ kind: 'loading' });
    try {
      const response = await API.foodLookup.lookupOff(code);
      setState({ kind: 'result', label: `Open Food Facts: ${code}`, response });
    } catch (e) {
      setState({ kind: 'error', message: e instanceof Error ? e.message : 'Barcode lookup failed.' });
    }
  }, []);

  const startScan = useCallback(() => {
    setScannerError(null);
    setManualMode(false);
    setScannerActive(true);
  }, []);

  const stopScan = useCallback(() => setScannerActive(false), []);

  // Manual entry is a *fallback*: revealed explicitly, or automatically once the
  // camera reports any of the six error states.
  const showManualEntry = manualMode || scannerError !== null;

  const formsClass = isDesktop ? 'grid grid-cols-2 gap-4' : isLandscape ? 'grid grid-cols-2 gap-3' : 'flex flex-col gap-3';
  const inputClass = 'w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 min-h-[44px] text-sm text-gray-900 dark:text-white placeholder:text-gray-400 dark:placeholder:text-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500';
  const buttonClass = 'rounded-md bg-blue-600 px-4 py-2 min-h-[44px] text-sm font-medium text-white hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50';
  const secondaryButtonClass = 'rounded-md border border-gray-300 dark:border-gray-600 px-3 py-2 min-h-[44px] text-sm font-medium text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500';

  return (
    <div className={isDesktop ? 'max-w-5xl mx-auto flex flex-col gap-4' : 'flex flex-col gap-3'}>
      <div role="note" className="rounded-lg border border-amber-300 dark:border-amber-700 bg-amber-50 dark:bg-amber-900/20 p-3 text-sm text-amber-800 dark:text-amber-200">
        <p className="font-semibold">DEV-ONLY diagnostic tool</p>
        <p className="mt-0.5">Queries external food APIs live and shows the raw JSON. Nothing is saved. Barcode lookup uses your camera (point it at an EAN-13/UPC); typed entry is only a fallback. Requires HTTPS or localhost.</p>
      </div>
      <div className={formsClass}>
        <form className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4 flex flex-col gap-2" onSubmit={(e) => { e.preventDefault(); void runUsda(); }}>
          <h2 className="text-base font-semibold text-gray-900 dark:text-white">USDA FoodData Central</h2>
          <label className="text-sm text-gray-600 dark:text-gray-300" htmlFor="api-explorer-query">Food name</label>
          <input id="api-explorer-query" className={inputClass} value={query} onChange={(e) => setQuery(e.target.value)} placeholder="e.g. banana" autoComplete="off" />
          <div className="flex gap-2">
            <div className="flex-1">
              <label className="text-sm text-gray-600 dark:text-gray-300" htmlFor="api-explorer-pagesize">Page size</label>
              <input id="api-explorer-pagesize" className={inputClass} value={pageSize} onChange={(e) => setPageSize(e.target.value)} inputMode="numeric" />
            </div>
            <div className="flex-[2]">
              <label className="text-sm text-gray-600 dark:text-gray-300" htmlFor="api-explorer-datatype">Data types (optional)</label>
              <input id="api-explorer-datatype" className={inputClass} value={dataType} onChange={(e) => setDataType(e.target.value)} placeholder="Foundation,SR Legacy" autoComplete="off" />
            </div>
          </div>
          <button type="submit" className={buttonClass}>Search USDA</button>
        </form>
        <div className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 p-4 flex flex-col gap-2">
          <h2 className="text-base font-semibold text-gray-900 dark:text-white">Open Food Facts (barcode)</h2>
          {scannerActive ? (
            <BarcodeScanner
              active={scannerActive}
              onScan={(value) => { void runOff(value); }}
              onError={(code) => setScannerError(code)}
              devSimulateValue={DEV_SIMULATE_VALUE}
            />
          ) : (
            <button type="button" onClick={startScan} className={buttonClass}>Start camera scan</button>
          )}
          {scannerActive && (
            <div className="flex flex-wrap gap-2">
              {!showManualEntry && (
                <button type="button" onClick={() => setManualMode(true)} className={secondaryButtonClass}>
                  Enter barcode manually
                </button>
              )}
              <button type="button" onClick={stopScan} className={secondaryButtonClass}>Stop camera</button>
            </div>
          )}
          {showManualEntry && (
            <form className="flex flex-col gap-2" onSubmit={(e) => { e.preventDefault(); void runOff(barcode); }}>
              <label className="text-sm text-gray-600 dark:text-gray-300" htmlFor="api-explorer-barcode">Barcode (manual fallback)</label>
              <input id="api-explorer-barcode" className={inputClass} value={barcode} onChange={(e) => setBarcode(e.target.value)} placeholder="e.g. 737628064502" autoComplete="off" inputMode="text" />
              <button type="submit" className={buttonClass}>Look up barcode</button>
            </form>
          )}
        </div>
      </div>
      <RawPanel state={state} />
    </div>
  );
}

