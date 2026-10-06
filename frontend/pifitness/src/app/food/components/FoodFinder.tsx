/** Shared finder T06: USDA+OFF via 010-001 proxy. */
'use client';
import { useCallback, useState } from 'react';
import dynamic from 'next/dynamic';
import { API } from '../../../lib/api-client';
const BarcodeScanner = dynamic(() => import('./BarcodeScanner'), { ssr: false });
const DEV = process.env.NODE_ENV !== 'production' ? '737628064502' : undefined;
export interface FinderPick { kind: 'usda' | 'off'; fdcId?: number; barcode?: string; name: string; }
function msg(e: unknown) { return e instanceof Error ? e.message : String(e); }
export default function FoodFinder({ onPick }: { onPick?: (p: FinderPick) => void }) {
  const [q, setQ] = useState('');
  const [res, setRes] = useState<{ fdcId?: number; description: string }[] | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [active, setActive] = useState(false);
  const [scanMsg, setScanMsg] = useState<string | null>(null);
  const [manual, setManual] = useState(false);
  const [code, setCode] = useState('');
  const run = useCallback(async () => {
    const query = q.trim(); if (!query) return;
    setBusy(true); setErr(null);
    try {
      const r = await API.foodLookup.searchUsda(query);
      const b = r.body as { foods?: { fdcId?: number; description?: string }[] } | null;
      setRes((b?.foods ?? []).map((f) => ({ fdcId: f.fdcId, description: f.description ?? '' })).filter((f) => f.description));
    } catch (e) { setErr(msg(e)); } finally { setBusy(false); }
  }, [q]);
  const scan = useCallback(async (v: string) => {
    setActive(false); setManual(false); setScanMsg(`Looking up barcode ${v}…`);
    try {
      const r = await API.foodLookup.lookupOff(v);
      const b = r.body as { status?: number; product?: { product_name?: string } } | null;
      const n = b?.product?.product_name?.trim();
      if (b?.status === 0 || !n) setScanMsg(`No product found for barcode ${v}.`);
      else { setScanMsg(`Scanned ${v}: ${n}`); onPick?.({ kind: 'off', barcode: v, name: n }); }
    } catch (e) { setScanMsg(`Barcode lookup failed: ${msg(e)}`); }
  }, [onPick]);
  return (
    <section aria-label="Find a food">
      <h2 className="text-base font-semibold text-gray-900 dark:text-white">Find a food</h2>
      <form onSubmit={(e) => { e.preventDefault(); void run(); }} className="mt-2 flex flex-wrap items-center gap-2">
        <label className="sr-only" htmlFor="finder-search">Food name</label>
        <input id="finder-search" value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search USDA (e.g. banana)" autoComplete="off"
          className="flex-1 min-w-48 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 text-sm text-gray-900 dark:text-white" />
        <button type="submit" className="rounded-md bg-blue-600 hover:bg-blue-700 px-3 py-2 text-sm text-white">{busy ? 'Searching…' : 'Search'}</button>
        {active ? <BarcodeScanner active={active} onScan={scan} onError={() => setActive(false)} devSimulateValue={DEV} />
          : <button type="button" onClick={() => setActive(true)} className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 text-sm text-gray-700 dark:text-gray-300">Scan barcode</button>}
      </form>
      {err && <p role="alert" className="text-xs text-red-700 dark:text-red-300">Search failed: {err}</p>}
      {res && (res.length === 0 ? <p className="text-xs text-gray-500 dark:text-gray-400">No matches.</p>
        : <ul className="mt-1 space-y-1 text-xs" aria-label="USDA search results">{res.map((f, i) => (
          <li key={`${f.fdcId ?? 'x'}-${i}`}><button type="button" onClick={() => onPick?.({ kind: 'usda', fdcId: f.fdcId, name: f.description })} className="text-left text-gray-700 dark:text-gray-300 hover:underline">{f.description}</button></li>))}</ul>)}
      {scanMsg && <p role="status" className="text-xs text-gray-700 dark:text-gray-300">{scanMsg}</p>}
      {manual
        ? <form onSubmit={(e) => { e.preventDefault(); if (code.trim()) void scan(code.trim()); }} className="mt-2 flex items-center gap-2">
          <label className="sr-only" htmlFor="finder-manual-barcode">Barcode</label>
          <input id="finder-manual-barcode" value={code} onChange={(e) => setCode(e.target.value)} placeholder="Enter barcode" inputMode="numeric" autoComplete="off"
            className="flex-1 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 text-sm text-gray-900 dark:text-white" />
          <button type="submit" className="rounded-md bg-blue-600 hover:bg-blue-700 px-3 py-2 text-sm text-white">Look up</button>
        </form>
        : <button type="button" onClick={() => setManual(true)} className="mt-2 text-xs text-gray-500 dark:text-gray-400 underline">Enter barcode manually</button>}
    </section>
  );
}
