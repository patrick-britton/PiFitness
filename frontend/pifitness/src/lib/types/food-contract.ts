/**
 * Food Module Contract (010-002 T01, frozen)
 *
 * Module map: exactly 5 tabs in order — Diary, Recipe Box, Food Database,
 * Tri-tip Timer, API Explorer (DEV-ONLY). Recipe creation is a mode inside
 * Recipe Box, not a tab. Diary is the default tab, showing today.
 *
 * Snapshot semantics (OQ-3/OQ-6 Option A): diary entries and recipe foods
 * freeze name + nutrients at write time; history renders from the entry,
 * never joins food rows. Soft-delete (OQ-4): is_deleted rows hidden from
 * pickers/Box, cannot be re-logged; Food Database has show-deleted + restore.
 *
 * Shared reuse rule: search/scan delegate to the 010-001 proxy
 * (GET /api/food/lookup/usda, GET /api/food/lookup/off, raw passthrough).
 * The finder + amount-picker are defined once here and imported by Diary,
 * recipe-step ingredient add, and recipe logging — never duplicated.
 *
 * DB: existing `food` schema (OQ-2), alongside tri-tip tables.
 * Nutrient columns derive from .features/descriptions/json_food_responses
 * (USDA foodNutrients[].nutrientName/value/unitName + OFF nutriments *_100g).
 *
 * Endpoints (reads = stub GETs, writes = stub POSTs, all under /api/food):
 *   GET  /api/food/usuals?at=ISO-8601        -> UsualFood[]
 *   GET  /api/food/diary?day=YYYY-MM-DD      -> DayEntry[] (snapshot history)
 *   GET  /api/food/recipes                   -> RecipeSummary[]
 *   GET  /api/food/foods?q&sort&limit&cursor -> FoodListResponse (excludes deleted by default)
 *   POST /api/food/diary                     -> LogEntryRequest -> DayEntry
 *   POST /api/food/recipes                   -> SaveRecipeRequest -> RecipeSummary
 *
 * 010-003 additions (Diary feature):
 *   GET  /api/food/diary/stats?day=YYYY-MM-DD -> DiaryStats
 *     (day = device-local YYYY-MM-DD, treated literally; today_kcal = day
 *     00:00→now local; last_24h_kcal = now()-24h→now; avg_7d_kcal = mean of
 *     the 7 full days before `day`; scale_max = max of the three; NULL = no data)
 *   PATCH /api/food/diary/{entry_id} -> DiaryUpdateRequest -> DayEntry
 *     (qty/unit/logged_at editable; new logged_at moves the entry to that day)
 *   DELETE /api/food/diary/{entry_id} -> { success: true }
 *   GET  /api/food/suggest?at=ISO-8601 -> UsualFood[] (top 10, mode_qty/mode_unit)
 *   GET  /api/food/search?q&limit -> CombinedSearchResult[]
 *     (display top 8-10, show-more to ~30 from the same fetch)
 *   POST /api/food/diary also accepts LogApiEntryRequest (usda/off payload)
 *     -> persists to foods.foods then logs the snapshot entry.
 *
 * Unit conversion (OQ-1, FDA label column, authoritative for kcal math):
 *   UNIT_TO_G_OR_ML = { g: 1, ml: 1, oz: 28, serving: per-row serving_size,
 *     tsp: 5, tbsp: 15, 'fl oz': 30, cup: 240 }
 *   (exact US customary retained as reference only: oz 28.349523125,
 *   tsp 4.92892159375, tbsp 14.78676478125, fl oz 29.5735295625,
 *   cup 236.5882365). kcal = kcal_per_100g * grams_equiv / 100.
 *
 * Search ranking (OQ-4, backend-side): saved foods first, then relevance tier
 * (all query words > most > rest), then within-tier data-type prior +
 * completeness + plausibility + freshness + brand affinity, USDA score last.
 * Completeness gate: missing any of kcal/protein/fat/carbs sinks to bottom
 * (no kcal = unloggable). Completeness label: High/Medium/Low from count of
 * the other 11 label-panel nutrients. Plausibility: one penalty per failed
 * check (kcal 0.7-1.3x macros-implied; P+F+C <= 102g/100g; sugars <= carbs;
 * satfat <= fat; fibre <= carbs; kcal <= 902/100g).
 */

/** Where a food row came from. */
export type FoodSource = 'api' | 'db' | 'recipe';

/** Recipe category filter (Recipe Box). */
export type RecipeCategory =
  | 'Breakfast' | 'Lunch' | 'Dinner' | 'Dessert' | 'Drink' | 'Appetizer';

/** Canonical per-100g nutrient snapshot (frozen at write time; null = unreported, never 0). */
export interface NutrientSnapshot {
  kcal_per_100g: number | null;
  protein_g: number | null;
  fat_g: number | null;
  carbs_g: number | null;
  fiber_g: number | null;
  sugars_g: number | null;
  sodium_mg: number | null;
  caffeine_mg: number | null;
}

/** A usual food for the Diary usual-foods region (time-of-day weighted). */
export interface UsualFood {
  food_id: number | null;
  name: string;
  source: FoodSource;
  kcal_per_100g: number | null;
  quality_score: number;
  times_eaten: number;
  hour_weight: number;
  /** 010-003: most-logged qty + its unit for one-tap suggestion cards. */
  mode_qty: number | null;
  mode_unit: string | null;
}

/** A diary entry — snapshot history, never a live join (OQ-6). */
export interface DayEntry {
  entry_id: number;
  food_id: number | null;
  name_snapshot: string;
  nutrients_snapshot: NutrientSnapshot;
  is_partial: boolean;
  qty: number;
  unit: string;
  kcal: number | null;
  logged_at: string;
}

/** A recipe row summary for the Recipe Box list. */
export interface RecipeSummary {
  recipe_id: number;
  title: string;
  category: RecipeCategory;
  times_prepared: number;
}

/** A food database row (soft-delete via is_deleted). */
export interface FoodRow {
  food_id: number;
  name: string;
  nutrients: NutrientSnapshot;
  /** T10 (OQ-8): feed standard serving on the row (null = none/failed). */
  serving_size?: number | null;
  serving_unit?: string | null;
  serving_text?: string | null;
  is_deleted: boolean;
  deleted_at: string | null;
}

/** Paginated food list (keyset via cursor, excludes deleted by default). */
export interface FoodListResponse {
  data: FoodRow[];
  next_cursor: string | null;
}

/** POST /api/food/diary request — server snapshots name + nutrients. */
export interface LogEntryRequest {
  food_id: number | null;
  qty: number;
  unit: string;
  logged_at?: string;
  /** 010-003 tuning: pick telemetry echoed from the chosen search result. */
  query?: string;
  rank?: number;
  result_source?: string;
}

/** 010-003: diary stats for the three-bar header (device-local day). */
export interface DiaryStats {
  today_kcal: number | null;
  last_24h_kcal: number | null;
  avg_7d_kcal: number | null;
  scale_max: number | null;
}

/** 010-003: PATCH /api/food/diary/{entry_id} — new logged_at moves the entry. */
export interface DiaryUpdateRequest {
  qty?: number;
  unit?: string;
  logged_at?: string;
}

/** 010-003: data-quality label (never a raw score in the UI). */
export type DataQuality = 'High' | 'Medium' | 'Low';

/** 010-003: one combined search hit (local DB + USDA + OFF, tiered-ranked). */
export interface CombinedSearchResult {
  kind: 'db-food' | 'db-recipe' | 'usda' | 'off';
  food_id: number | null;
  name: string;
  mode_qty: number | null;
  mode_unit: string | null;
  kcal_preview: number | null;
  data_quality: DataQuality;
  /** Tuning log: final rank shown + which fetch supplied the hit. */
  rank: number;
  result_source: string;
  usda_score: number | null;
  /** T08 (OQ-6): natural keys + raw upstream hit carried on the hit so
   * confirm can persist without a re-fetch; mirrors the backend. Raw USDA
   * hit (foodNutrients + servingSize) or raw OFF product — never a
   * flattened per-100g map. */
  fdcId?: number | null;
  barcode?: string | null;
  nutrients_raw?: unknown | null;
  /** T10 (OQ-8): normalized per-100g panel + feed serving default so the
   * confirm popup can show exactly what will be saved with no re-fetch
   * (db rows from foods columns, usda from the ranking mapper; null =
   * unknown/failed-closed — 100 g popup fallback). */
  nutrients?: NutrientSnapshot | null;
  serving_size?: number | null;
  serving_unit?: string | null;
  serving_text?: string | null;
  /** 010-004: ranked-search fields (all optional/additive; backend populates,
   * frontend derives facet controls client-side from food_category +
   * serving_unit + serving_unit_facet). Sort: saved-first, then match_band
   * desc, is_single_ingredient, quality_percent desc, ingredient_count asc. */
  quality_percent?: number | null;
  match_pct?: number | null;
  match_band?: number | null;
  is_single_ingredient?: boolean | null;
  ingredient_count?: number | null;
  kcal_delta_pct?: number | null;
  excluded_kcal_mismatch?: boolean | null;
  food_category?: string | null;
  serving_unit_facet?: string | null;
}

/** 010-003: raw API hit carried through the confirm popup to persist-on-log. */
export interface ApiFoodPayload {
  provider: 'usda' | 'off';
  fdcId?: number;
  barcode?: string;
  name: string;
  nutrients_raw: unknown;
}

/** 010-003: POST /api/food/diary with an API hit — server persists to foods.foods first. */
export interface LogApiEntryRequest {
  usda_payload?: ApiFoodPayload;
  off_payload?: ApiFoodPayload;
  qty: number;
  unit: string;
  logged_at?: string;
  /** 010-003 tuning: pick telemetry echoed from the chosen search result. */
  query?: string;
  rank?: number;
  result_source?: string;
}

/** One recipe step (cook-mode checks are client-only, no writes). */
export interface RecipeStep {
  step_no: number;
  instruction: string;
  timer_seconds: number | null;
  foods: { food_id: number | null; qty: number; unit: string }[];
}

/** POST /api/food/recipes request — server snapshots combined nutrients. */
export interface SaveRecipeRequest {
  title: string;
  category: RecipeCategory;
  steps: RecipeStep[];
}
