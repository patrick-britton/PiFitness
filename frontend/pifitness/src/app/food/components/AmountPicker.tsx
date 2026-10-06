/** Shared amount picker T06: qty+unit once for diary/recipe logging. */
'use client';
import { useState } from 'react';
const UNITS = ['g', 'ml', 'tsp', 'tbsp', 'fl oz', 'cup', 'oz', 'serving'] as const;
export type AmountUnit = (typeof UNITS)[number];
export interface Amount { qty: number; unit: AmountUnit; }
export default function AmountPicker({ value, onChange }: { value?: Amount; onChange?: (a: Amount) => void }) {
  const [qty, setQty] = useState(value?.qty ?? 100);
  const [unit, setUnit] = useState<AmountUnit>(value?.unit ?? 'g');
  const emit = (nq: number, nu: AmountUnit) => onChange?.({ qty: nq, unit: nu });
  return (
    <div className="flex items-center gap-2" aria-label="Amount picker">
      <label className="sr-only" htmlFor="amount-qty">Quantity</label>
      <input id="amount-qty" type="number" min={0} step="any" value={qty}
        onChange={(e) => { const n = Number(e.target.value); setQty(n); emit(n, unit); }}
        className="w-24 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 text-sm text-gray-900 dark:text-white" />
      <label className="sr-only" htmlFor="amount-unit">Unit</label>
      <select id="amount-unit" value={unit} onChange={(e) => { const nu = e.target.value as AmountUnit; setUnit(nu); emit(qty, nu); }}
        className="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-3 py-2 text-sm text-gray-900 dark:text-white">
        {UNITS.map((u) => <option key={u} value={u}>{u}</option>)}
      </select>
    </div>
  );
}
