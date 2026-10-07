/**
 * FDA nutrition-label unit→gram/mL factors (010-003 T05).
 *
 * Mirrors `UNIT_TO_G_OR_ML` in `backend/schemas/food_contract_schemas.py` — the
 * server is authoritative for the kcal it stores (OQ-1); this table exists only
 * for the *live* calorie preview in the edit/confirm popups and is never
 * written to the database. `serving` resolves per-row from a food's
 * `serving_size`, so it is deliberately absent here.
 */
import type { NutrientSnapshot } from './types/food-contract';

export const UNIT_TO_G_OR_ML: Record<string, number> = {
  g: 1,
  ml: 1,
  tsp: 5,
  tbsp: 15,
  'fl oz': 30,
  cup: 240,
  oz: 28,
};

/**
 * Preview kcal for `qty` × `unit` from a per-100g figure, or `null` when it
 * cannot be computed (unit unknown to the table with no `ownUnit` match, or
 * no kcal figure). `ownUnit` is the row's own serving `{size, label}` so a
 * recipe's `pancake` resolves through its grams-per-unit exactly like the
 * server's `_kcal_for` — the popup number matches what will be saved.
 */
export function previewKcal(
  kcalPer100g: number | null | undefined,
  qty: number,
  unit: string,
  ownUnit?: { size: number; unit: string } | null,
): number | null {
  if (kcalPer100g == null || !Number.isFinite(qty) || qty <= 0) return null;
  const key = (unit || '').trim().toLowerCase();
  const gramsPerUnit = UNIT_TO_G_OR_ML[key];
  if (gramsPerUnit != null) return (kcalPer100g * (qty * gramsPerUnit)) / 100;
  // 010-005 T10 (OQ-2): a unit matching the row's own serving label (a
  // recipe's derived grams-per-unit) resolves via its size — mirrors the
  // server's `_kcal_for` unit==serving_unit branch.
  if (ownUnit != null && ownUnit.size > 0
    && key === ownUnit.unit.trim().toLowerCase()) {
    return (kcalPer100g * (qty * ownUnit.size)) / 100;
  }
  return null;
}

/** T10 (OQ-8): feed unit families the picker knows (mirrors the backend's
 * fail-closed `serving_fields`; anything else — e.g. upstream "MG" — is
 * rejected so the popup falls back to 100 g rather than guessing). */
const FEED_UNIT_FAMILY: Record<string, 'g' | 'ml'> = {
  g: 'g', gram: 'g', grams: 'g', grm: 'g',
  ml: 'ml', mlt: 'ml', milliliter: 'ml', millilitre: 'ml',
};

/**
 * Parse a feed serving (USDA `servingSize`/`servingSizeUnit`, OFF
 * `serving_quantity`/`serving_quantity_unit`) into a popup default, or
 * `null` when unusable (missing/non-positive/unfamiliar unit).
 */
export function feedServing(
  size: unknown,
  unit: unknown,
): { size: number; unit: 'g' | 'ml' } | null {
  const n = typeof size === 'number' ? size : Number(size);
  if (!Number.isFinite(n) || n <= 0) return null;
  const family = FEED_UNIT_FAMILY[String(unit ?? 'g').trim().toLowerCase()];
  return family ? { size: n, unit: family } : null;
}

/** T10 (OQ-8): OFF nutriments -> NutrientSnapshot. Key mapping only —
 * OFF `*_100g` values are already per-100g (no scaling; mirrors the
 * backend's OFF_TO_COL for the display panel; caffeine has no OFF field). */
const OFF_TO_COL: Record<string, keyof NutrientSnapshot> = {
  'energy-kcal_100g': 'kcal_per_100g',
  proteins_100g: 'protein_g',
  fat_100g: 'fat_g',
  carbohydrates_100g: 'carbs_g',
  fiber_100g: 'fiber_g',
  sugars_100g: 'sugars_g',
  sodium_100g: 'sodium_mg',
};

export function offNutrientsToSnapshot(nutriments: unknown): NutrientSnapshot {
  const out: NutrientSnapshot = {
    kcal_per_100g: null, protein_g: null, fat_g: null, carbs_g: null,
    fiber_g: null, sugars_g: null, sodium_mg: null, caffeine_mg: null,
  };
  if (nutriments == null || typeof nutriments !== 'object') return out;
  const src = nutriments as Record<string, unknown>;
  for (const [key, col] of Object.entries(OFF_TO_COL)) {
    const v = src[key];
    if (v == null) continue;
    const n = Number(v);
    if (Number.isFinite(n) && n >= 0) out[col] = n;
  }
  return out;
}