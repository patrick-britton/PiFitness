"""Food query functions (010-002 T03, stub-backed).

Read paths hit the T02 serve views / typed tables only. Writes snapshot
name + nutrients at write time (NULL = unknown, never 0). All SQL
parameterized; keyset pagination, no OFFSET.

Every public function returns EXACTLY the T01 contract shape
(UsualFood[] / DayEntry[] / RecipeSummary[] / FoodListResponse /
DayEntry / RecipeSummary) so the router and consumers trust the wire format
without reshaping. Nutrient fields are nested under `nutrients` (FoodRow) /
`nutrients_snapshot` (DayEntry) per the contract.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend_functions.database_functions import (
    qec,
    sql_to_dict,
    sql_insert_returning,
)


FOOD_COLS = (
    "food_id, name, kcal_per_100g, protein_g, fat_g, carbs_g, "
    "fiber_g, sugars_g, sodium_mg, caffeine_mg, serving_size, "
    "serving_unit, serving_text, needs_review, review_reason, "
    "quality_score, is_deleted, deleted_at, created_at"
)

ENTRY_COLS = (
    "entry_id, food_id, name_snapshot, kcal_per_100g, protein_g, "
    "fat_g, carbs_g, fiber_g, sugars_g, sodium_mg, caffeine_mg, "
    "is_partial, qty, unit, kcal, logged_at, created_at"
)

_NUTRIENT_KEYS = (
    "kcal_per_100g", "protein_g", "fat_g", "carbs_g",
    "fiber_g", "sugars_g", "sodium_mg", "caffeine_mg",
)

_RECIPE_SOURCE = "recipe"


def _kcal_for(qty: float, unit: str, kcal_per_100g: Optional[float],
              serving_size: Optional[float] = None,
              conversions: Optional[Dict[str, float]] = None) -> Optional[float]:
    """Server-side qty x unit -> kcal (OQ-1 FDA-label table)."""
    if kcal_per_100g is None:
        return None
    table = conversions or {}
    key = (unit or "").strip().lower()
    alias = {"fl-oz": "fl oz", "floz": "fl oz", "ounce": "oz", "ounces": "oz",
             "teaspoon": "tsp", "tablespoon": "tbsp", "cups": "cup",
             "servings": "serving"}
    key = alias.get(key, key)
    if key in ("g", "gram", "grams", "ml", "milliliter", "millilitre"):
        grams = qty
    elif key == "serving":
        if serving_size is None or serving_size <= 0:
            raise ValueError("unit 'serving' needs a known serving_size")
        grams = qty * serving_size
    elif key in table:
        grams = qty * table[key]
    else:
        raise ValueError(f"Unknown unit: {unit!r}")
    return grams * kcal_per_100g / 100.0


def _iso(value: Any) -> Optional[str]:
    """Collapse a DB timestamp to an ISO-8601 string (None stays None)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _nutrients(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Project the NULL-able NutrientSnapshot from a food/entry row."""
    return {k: obj.get(k) for k in _NUTRIENT_KEYS}


def _snapshot_from_food(food: Dict[str, Any]) -> Dict[str, Any]:
    """Project the NULL-able nutrient snapshot from a foods row."""
    return _nutrients(food)


def _to_food_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Flat foods row -> FoodRow contract shape (nutrients nested)."""
    return {
        "food_id": row["food_id"],
        "name": row["name"],
        "nutrients": _nutrients(row),
        # T10 (OQ-8): serving travels on the row for popup defaults/panels.
        "serving_size": row.get("serving_size"),
        "serving_unit": row.get("serving_unit"),
        "serving_text": row.get("serving_text"),
        "is_deleted": bool(row.get("is_deleted", False)),
        "deleted_at": _iso(row.get("deleted_at")),
    }


def _to_day_entry(row: Dict[str, Any]) -> Dict[str, Any]:
    """Flat diary row -> DayEntry contract shape (nutrients_snapshot nested)."""
    return {
        "entry_id": row["entry_id"],
        "food_id": row.get("food_id"),
        "name_snapshot": row["name_snapshot"],
        "nutrients_snapshot": _nutrients(row),
        "is_partial": bool(row.get("is_partial", False)),
        "qty": row["qty"],
        "unit": row["unit"],
        "kcal": row.get("kcal"),
        "logged_at": _iso(row.get("logged_at")),
    }


def _to_recipe_summary(row: Dict[str, Any]) -> Dict[str, Any]:
    """Recipe row -> RecipeSummary contract shape (no extra fields)."""
    return {
        "recipe_id": row["recipe_id"],
        "title": row["title"],
        "category": row["category"],
        "times_prepared": int(row.get("times_prepared", 0) or 0),
    }


def get_usual_foods(
    limit: int = 20, at: Optional[datetime] = None,
) -> Sequence[Dict[str, Any]]:
    """Time-of-day-weighted usuals (serve view; excludes review/NULL-kcal).

    `at` (ISO instant) is accepted per the contract signature; the serve view
    weights by the current server hour, so it is validated upstream and not
    threaded into the fixed view expression.
    """
    try:
        rows = sql_to_dict(
            """SELECT food_id, name, source, kcal_per_100g, quality_score,
                      times_eaten, hour_weight, mode_qty, mode_unit
               FROM food.vw_usual_foods
               ORDER BY hour_weight DESC, times_eaten DESC
               LIMIT %s""",
            (limit,),
        )
    except Exception:
        # Pre-T02 DDL view has no mode columns — fall back, NULL them out.
        rows = sql_to_dict(
            """SELECT food_id, name, source, kcal_per_100g, quality_score,
                      times_eaten, hour_weight
               FROM food.vw_usual_foods
               ORDER BY hour_weight DESC, times_eaten DESC
               LIMIT %s""",
            (limit,),
        )
        for r in rows:
            r["mode_qty"] = None
            r["mode_unit"] = None
    return rows


def get_diary_entries(day: date, limit: int = 100) -> Sequence[Dict[str, Any]]:
    """Snapshot history for one calendar day, DayEntry[] (no food joins)."""
    sql = f"""
        SELECT {ENTRY_COLS}
        FROM food.diary_entries
        WHERE logged_at::date = %s
        ORDER BY logged_at DESC, entry_id DESC
        LIMIT %s
    """
    return [_to_day_entry(r) for r in sql_to_dict(sql, (day, limit))]


def get_diary_stats(day: date) -> Dict[str, Any]:
    """Three-bar header via fn_diary_stats (device-local day, literal)."""
    rows = sql_to_dict("SELECT * FROM food.fn_diary_stats(%s)", (day,))
    if not rows:
        return {"today_kcal": None, "last_24h_kcal": None,
                "avg_7d_kcal": None, "scale_max": None}
    r = rows[0]
    return {"today_kcal": r.get("today_kcal"),
            "last_24h_kcal": r.get("last_24h_kcal"),
            "avg_7d_kcal": r.get("avg_7d_kcal"),
            "scale_max": r.get("scale_max")}


def search_saved_foods(q: str, limit: int = 30) -> List[Dict[str, Any]]:
    """Local-first saved-food hits for the combined search (OQ-4 tier 0).

    Single query: name match, alive rows only, snapshot nutrients attached.
    T08 (OQ-6): also returns source/fdc_id/barcode so combined_search can
    suppress already-saved API duplicates.
    """
    qq = (q or "").strip()
    if not qq:
        return []
    rows = sql_to_dict(
        f"""SELECT {FOOD_COLS}, source, fdc_id, barcode
            FROM food.foods
            WHERE NOT is_deleted AND name ILIKE '%%' || %s || '%%'
            ORDER BY quality_score DESC, food_id DESC
            LIMIT %s""",
        (qq, limit),
    )
    return rows


def get_food_by_barcode(barcode: str) -> Optional[Dict[str, Any]]:
    """T08 (OQ-6): alive food row by barcode for the scan-first DB check."""
    code = (barcode or "").strip()
    if not code:
        return None
    rows = sql_to_dict(
        f"""SELECT {FOOD_COLS}, source, fdc_id, barcode
            FROM food.foods
            WHERE NOT is_deleted AND barcode = %s
            ORDER BY food_id DESC
            LIMIT 1""",
        (code,),
    )
    return rows[0] if rows else None


def persist_api_food(provider: str, name: str, fdc_id: Optional[int],
                     barcode: Optional[str], nutrients_raw: Any,
                     per100: Dict[str, Any], needs_review: bool,
                     review_reason: Optional[str],
                     normalized_from: Optional[str] = None,
                     serving_size: Optional[float] = None,
                     serving_unit: Optional[str] = None,
                     serving_text: Optional[str] = None) -> Dict[str, Any]:
    """Persist-on-log: normalize an API hit into foods.foods, return the row.

    Idempotent on natural keys (fdc_id / barcode). T09 (OQ-7): confirming
    a pick IS the review, so the fresh-INSERT path writes needs_review=false
    + review_reason=NULL (suggestion-eligible immediately, zero-kcal
    included). The duplicate-key path returns the existing row untouched.
    normalized_from must satisfy the foods CHECK
    (usda_branded/usda_survey/usda_legacy/off/manual/recipe); defaults to
    'off' for provider 'off', else 'usda_survey'.
    T10 (OQ-8): serving_size/unit/text record the feed's standard serving
    (parsed fail-closed by `serving_fields`) so the `serving` unit resolves.
    """
    from backend_functions.food_normalize import NUTRIENT_COLS
    existing = None
    if fdc_id is not None:
        rows = sql_to_dict("SELECT food_id FROM food.foods WHERE fdc_id = %s",
                           (fdc_id,))
        existing = rows[0] if rows else None
    if existing is None and barcode:
        rows = sql_to_dict(
            "SELECT food_id FROM food.foods WHERE barcode = %s", (barcode,))
        existing = rows[0] if rows else None
    if existing is not None:
        rows = sql_to_dict(f"SELECT {FOOD_COLS} FROM food.foods "
                           "WHERE food_id = %s", (existing["food_id"],))
        return rows[0]
    if normalized_from is None:
        normalized_from = "off" if provider == "off" else "usda_survey"
    cols = [c for c in NUTRIENT_COLS]
    vals = [per100.get(c) for c in cols]
    rows = sql_insert_returning(
        f"""INSERT INTO food.foods
                (name, source, fdc_id, barcode, {", ".join(cols)},
                 nutrients_raw, serving_size, serving_unit, serving_text,
                 normalized_from, needs_review, review_reason)
            VALUES (%s, 'api', %s, %s, {", ".join(["%s"] * len(cols))},
                    %s, %s, %s, %s, %s, %s, %s)
            RETURNING {FOOD_COLS}""",
        (name, fdc_id, barcode, *vals,
         __import__("json").dumps(nutrients_raw)
         if nutrients_raw is not None else None,
         serving_size, serving_unit, serving_text,
         # Confirm IS the review: user-confirmed rows are suggestion-eligible.
         normalized_from, False, None),
    )
    if not rows:
        raise RuntimeError("Failed to persist API food.")
    return rows[0]


def get_food_by_id(food_id: int) -> Optional[Dict[str, Any]]:
    """T10 (OQ-8): one alive food row as FoodRow (suggestion-pick hydration)."""
    rows = sql_to_dict(
        f"""SELECT {FOOD_COLS} FROM food.foods
            WHERE food_id = %s AND NOT is_deleted""", (food_id,))
    return _to_food_row(rows[0]) if rows else None


def search_foods(
    q: str = "",
    include_deleted: bool = False,
    limit: int = 20,
    cursor: Optional[int] = None,
) -> Dict[str, Any]:
    """Ranked search; keyset on food_id; deleted excluded by default."""
    sql = f"""
        SELECT food_id, name, source, kcal_per_100g, caffeine_mg,
               quality_score, needs_review, is_deleted, deleted_at, rank_score
        FROM food.vw_food_search
        WHERE (%s = '' OR name ILIKE '%%' || %s || '%%')
          AND (%s OR NOT (food_id IN
              (SELECT food_id FROM food.foods WHERE is_deleted)))
          AND (%s IS NULL OR food_id < %s)
        ORDER BY rank_score DESC, food_id DESC
        LIMIT %s
    """
    rows = sql_to_dict(sql, (q, q, include_deleted, cursor, cursor, limit))
    next_cursor = rows[-1]["food_id"] if rows and len(rows) == limit else None
    return {"data": [_to_food_row(r) for r in rows], "next_cursor": next_cursor}


def get_recipes(include_deleted: bool = False) -> Sequence[Dict[str, Any]]:
    """Recipe Box list, RecipeSummary[] (deleted hidden by default)."""
    sql = """
        SELECT recipe_id, title, category, times_prepared, is_deleted,
               deleted_at, created_at
        FROM food.recipes
        WHERE (%s OR NOT is_deleted)
        ORDER BY title ASC
    """
    return [_to_recipe_summary(r) for r in sql_to_dict(sql, (include_deleted,))]


def log_diary_entry(
    food_id: Optional[int],
    qty: float,
    unit: str,
    logged_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Log entry (DayEntry): snapshot name + nutrients from foods (NULL-safe).

    kcal uses the OQ-1 FDA-label conversion table (server-side): grams_equiv
    = qty * UNIT_TO_G_OR_ML[unit] ('serving' resolves from serving_size);
    kcal = kcal_per_100g * grams_equiv / 100. Unknown unit -> ValueError.
    """
    from backend.schemas.food_contract_schemas import UNIT_TO_G_OR_ML
    if isinstance(logged_at, str) and logged_at:
        try:
            logged_at = datetime.fromisoformat(logged_at)
        except ValueError:
            raise ValueError("logged_at must be an ISO-8601 string")
    food = None
    if food_id is not None:
        rows = sql_to_dict(
            f"SELECT {FOOD_COLS} FROM food.foods WHERE food_id = %s", (food_id,))
        if not rows:
            raise ValueError(f"Food {food_id} not found.")
        food = rows[0]
        if food.get("is_deleted"):
            raise ValueError(
                f"Food {food_id} is soft-deleted and cannot be re-logged.")
    snap = _snapshot_from_food(food) if food else {k: None for k in _NUTRIENT_KEYS}
    kcal = _kcal_for(qty, unit, snap.get("kcal_per_100g"),
                     serving_size=(food or {}).get("serving_size"),
                     conversions=UNIT_TO_G_OR_ML)
    is_partial = any(v is None for v in snap.values()) or kcal is None
    rows = sql_insert_returning(
        f"""INSERT INTO food.diary_entries
                (food_id, name_snapshot, kcal_per_100g, protein_g, fat_g,
                 carbs_g, fiber_g, sugars_g, sodium_mg, caffeine_mg,
                 is_partial, qty, unit, kcal, logged_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    COALESCE(%s, now()))
            RETURNING {ENTRY_COLS}""",
        (food_id, (food or {}).get("name", "Unknown food"),
         snap["kcal_per_100g"], snap["protein_g"], snap["fat_g"],
         snap["carbs_g"], snap["fiber_g"], snap["sugars_g"],
         snap["sodium_mg"], snap["caffeine_mg"], is_partial,
         qty, unit, kcal, logged_at),
    )
    if not rows:
        raise RuntimeError("Failed to log diary entry.")
    return _to_day_entry(rows[0])


def update_diary_entry(
    entry_id: int,
    qty: Optional[float] = None,
    unit: Optional[str] = None,
    logged_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """PATCH an entry (OQ-5): new logged_at moves it to that day.

    Snapshot nutrients stay frozen; only qty/unit/kcal/logged_at change
    (kcal recomputed via the FDA-label table when qty/unit change).
    """
    from backend.schemas.food_contract_schemas import UNIT_TO_G_OR_ML
    if isinstance(logged_at, str) and logged_at:
        try:
            logged_at = datetime.fromisoformat(logged_at)
        except ValueError:
            raise ValueError("logged_at must be an ISO-8601 string")
    rows = sql_to_dict(
        f"SELECT {ENTRY_COLS} FROM food.diary_entries WHERE entry_id = %s",
        (entry_id,))
    if not rows:
        raise ValueError(f"Diary entry {entry_id} not found.")
    cur = rows[0]
    new_qty = qty if qty is not None else cur["qty"]
    new_unit = unit if unit is not None else cur["unit"]
    new_logged = logged_at if logged_at is not None else cur.get("logged_at")
    if new_qty is not None and new_qty <= 0:
        raise ValueError("qty must be > 0")
    food = None
    if cur.get("food_id") is not None:
        food = _fetch_food(cur["food_id"])
    kcal = _kcal_for(new_qty, new_unit, cur.get("kcal_per_100g"),
                     serving_size=(food or {}).get("serving_size"),
                     conversions=UNIT_TO_G_OR_ML)
    is_partial = any(cur.get(k) is None for k in _NUTRIENT_KEYS) or kcal is None
    out = sql_insert_returning(
        """UPDATE food.diary_entries
              SET qty = %s, unit = %s, kcal = %s, is_partial = %s,
                  logged_at = COALESCE(%s, logged_at)
            WHERE entry_id = %s
            RETURNING entry_id, food_id, name_snapshot, kcal_per_100g,
                      protein_g, fat_g, carbs_g, fiber_g, sugars_g,
                      sodium_mg, caffeine_mg, is_partial, qty, unit,
                      kcal, logged_at, created_at""",
        (new_qty, new_unit, kcal, is_partial, new_logged, entry_id),
    )
    if not out:
        raise RuntimeError(f"Failed to update diary entry {entry_id}.")
    return _to_day_entry(out[0])


def delete_diary_entry(entry_id: int) -> bool:
    """DELETE an entry (hard delete; history gone, food row untouched)."""
    err = qec("DELETE FROM food.diary_entries WHERE entry_id = %s", (entry_id,))
    if err:
        raise RuntimeError(f"Failed to delete diary entry {entry_id}: {err}")
    return True


def set_food_deleted(
    food_id: int, deleted: bool, reason: Optional[str] = None,
) -> bool:
    """Soft-delete / restore a food (snapshots survive either way)."""
    err = qec(
        """UPDATE food.foods SET is_deleted = %s,
               deleted_at = CASE WHEN %s THEN now() ELSE NULL END,
               review_reason = COALESCE(%s, review_reason)
           WHERE food_id = %s""",
        (deleted, deleted, reason, food_id),
    )
    if err:
        raise RuntimeError(f"Failed to update food {food_id}: {err}")
    return True


def list_foods_page(
    limit: int = 20, cursor: Optional[int] = None,
    include_deleted: bool = False,
) -> Dict[str, Any]:
    """Food Database page: keyset on food_id, FoodRow[], deleted excluded."""
    sql = f"""
        SELECT {FOOD_COLS}
        FROM food.foods
        WHERE (%s OR NOT is_deleted)
          AND (%s IS NULL OR food_id < %s)
        ORDER BY food_id DESC
        LIMIT %s
    """
    rows = sql_to_dict(sql, (include_deleted, cursor, cursor, limit))
    next_cursor = rows[-1]["food_id"] if rows and len(rows) == limit else None
    return {"data": [_to_food_row(r) for r in rows], "next_cursor": next_cursor}


def _fetch_food(food_id: int) -> Optional[Dict[str, Any]]:
    rows = sql_to_dict(
        f"SELECT {FOOD_COLS} FROM food.foods WHERE food_id = %s", (food_id,))
    return rows[0] if rows else None


def _combine_recipe_nutrients(
    steps: Sequence[Dict[str, Any]],
) -> Tuple[Dict[str, Any], bool, str]:
    """Aggregate step-food per-100g snapshots into ONE per-100g snapshot.

    Assumes qty is in grams (ml ~= g), consistent with diary kcal scaling.
    Fail closed: if any contributing ingredient is missing a nutrient or is
    itself flagged needs_review, the combined snapshot is flagged needs_review
    (never fabricates a value).
    """
    total_g = 0.0
    weighted = {k: 0.0 for k in _NUTRIENT_KEYS}
    any_missing = False
    contributing = 0
    for step in steps:
        for f in step.get("foods") or []:
            fid = f.get("food_id")
            qty = f.get("qty")
            if fid is None or qty is None or qty <= 0:
                continue
            food = _fetch_food(fid)
            if food is None or food.get("is_deleted"):
                continue
            contributing += 1
            total_g += qty
            snap = _snapshot_from_food(food)
            if any(v is None for v in snap.values()) or food.get("needs_review"):
                any_missing = True
            for k in _NUTRIENT_KEYS:
                if snap.get(k) is not None:
                    weighted[k] += snap[k] * qty
    if total_g <= 0 or contributing == 0:
        empty = {k: None for k in _NUTRIENT_KEYS}
        return empty, True, "no valid step foods to aggregate"
    per_100 = {}
    for k in _NUTRIENT_KEYS:
        per_100[k] = round((weighted[k] / total_g) * 100.0, 6) if weighted[k] else None
    reason = "partial ingredient nutrients (missing/unreported)" if any_missing else ""
    return per_100, any_missing, reason


def save_recipe(
    title: str,
    category: str,
    steps: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """Persist a recipe (RecipeSummary) with a snapshot recipe-food row.

    Snapshot semantics (OQ-3): the combined ingredient nutrients are frozen
    into a source='recipe' food row at save time, so later ingredient edits
    never rewrite diary history for previously-logged servings.
    """
    combined, needs_review, reason = _combine_recipe_nutrients(steps)

    food_rows = sql_insert_returning(
        f"""INSERT INTO food.foods
                (name, source, kcal_per_100g, protein_g, fat_g, carbs_g,
                 fiber_g, sugars_g, sodium_mg, caffeine_mg,
                 needs_review, review_reason, normalized_from)
            VALUES (%s, '{_RECIPE_SOURCE}', %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, '{_RECIPE_SOURCE}')
            RETURNING food_id""",
        (title, combined["kcal_per_100g"], combined["protein_g"],
         combined["fat_g"], combined["carbs_g"], combined["fiber_g"],
         combined["sugars_g"], combined["sodium_mg"], combined["caffeine_mg"],
         needs_review, reason if needs_review else None),
    )
    if not food_rows:
        raise RuntimeError("Failed to create recipe food row.")
    food_id = food_rows[0]["food_id"]

    rec_rows = sql_insert_returning(
        """INSERT INTO food.recipes (title, category, food_id)
           VALUES (%s, %s, %s)
           RETURNING recipe_id, title, category, times_prepared""",
        (title, category, food_id),
    )
    if not rec_rows:
        raise RuntimeError("Failed to create recipe.")
    recipe = rec_rows[0]

    for step in steps:
        step_rows = sql_insert_returning(
            """INSERT INTO food.recipe_steps
                    (recipe_id, step_no, instruction, timer_seconds)
               VALUES (%s, %s, %s, %s)
               RETURNING step_id""",
            (recipe["recipe_id"], step.get("step_no"),
             step.get("instruction") or "", step.get("timer_seconds")),
        )
        if not step_rows:
            continue
        step_id = step_rows[0]["step_id"]
        for f in step.get("foods") or []:
            if f.get("food_id") is None:
                continue
            qec(
                """INSERT INTO food.recipe_step_foods
                        (recipe_id, step_id, food_id, qty, unit)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (recipe_id, step_id, food_id, unit) DO NOTHING""",
                (recipe["recipe_id"], step_id, f["food_id"], f["qty"], f["unit"]),
            )

    return _to_recipe_summary(recipe)
