"""Normalize USDA/OFF payloads to per-100g NULL-safe rows (010-002 T03).

T12 (Bug 010-003-T12-1): BOTH branded and survey/legacy search payloads carry
foodNutrients ALREADY per-100g (derivationCode LCCS = label per-serving value
converted upstream by USDA; proven on the almond fixture where 36/37 branded
rows have P+F+C grams exceeding serving grams — a per-serving basis is
physically impossible, e.g. fdcId 2642417 P+F+C=90g in a 30g serving).
The normalizer therefore passes values through WITHOUT any 100/grams rescale
(servingSize is for serving defaults + density checks only, never scaling).
Fail closed: unparseable serving -> serving_fields NULLs (never fabricated).
"""

from typing import Any, Dict, Optional, Tuple

USDA_TO_COL = {
    "energy": "kcal_per_100g",
    "protein": "protein_g",
    "total lipid (fat)": "fat_g",
    "carbohydrate, by difference": "carbs_g",
    "fiber, total dietary": "fiber_g",
    "total sugars": "sugars_g",
    "sugars, total": "sugars_g",
    "sodium, na": "sodium_mg",
    "caffeine": "caffeine_mg",
    "theobromine": None,  # tracked upstream but not a foods column
}

# T12 (Bug 010-003-T12-1): Foundation energy fallback by nutrientId —
# prefer 1008 (Energy), else 2048 (Atwater Specific Factors, e.g. almond
# flour fdcId 2261420 = 578), else 2047 (Atwater General Factors = 622).
# By-id so the two Atwater rows never both map to kcal with last-wins.
_ENERGY_IDS = (1008, 2048, 2047)

OFF_TO_COL = {
    "energy-kcal_100g": "kcal_per_100g",
    "proteins_100g": "protein_g",
    "fat_100g": "fat_g",
    "carbohydrates_100g": "carbs_g",
    "carbohydrates-total_100g": "carbs_g",
    "fiber_100g": "fiber_g",
    "sugars_100g": "sugars_g",
    "sodium_100g": "sodium_mg",
}

_G_PER_UNIT = {"g": 1.0, "gram": 1.0, "grm": 1.0, "ml": 1.0, "mlt": 1.0}

NUTRIENT_COLS = (
    "kcal_per_100g", "protein_g", "fat_g", "carbs_g",
    "fiber_g", "sugars_g", "sodium_mg", "caffeine_mg",
)


def parse_serving_grams(size: Any, unit: Any) -> Optional[float]:
    """Parse serving to grams; mL ~= g explicit; None when unparseable."""
    try:
        grams = float(size)
    except (TypeError, ValueError):
        return None
    if not grams or grams <= 0:
        return None
    u = str(unit or "g").strip().lower()
    factor = _G_PER_UNIT.get(u)
    if factor is None:
        return None
    return grams * factor


# T10 (OQ-8): unit families the AmountPicker knows; anything else fails closed.
_SERVING_G_FAMILY = {"g", "gram", "grams", "grm"}
_SERVING_ML_FAMILY = {"ml", "mlt", "milliliter", "millilitre"}


def serving_fields(size: Any, unit: Any,
                   text: Any = None) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    """T10 (OQ-8): parse a feed serving into foods.foods serving columns.

    Returns (size, unit, text): size via `parse_serving_grams` (fail-closed),
    unit = 'g' | 'ml' (what the picker/kcal table understands), text = the
    display string (householdServingFullText / OFF serving_size; literal
    "None" dropped). Free text is never parsed into numbers; messy upstream
    unit codes (e.g. "MG") yield (None, None, text).
    """
    raw_text = str(text).strip() if text is not None else None
    if raw_text and raw_text.lower() == "none":
        raw_text = None
    amount = parse_serving_grams(size, unit)
    u = str(unit or "g").strip().lower()
    if u in _SERVING_G_FAMILY:
        family = "g"
    elif u in _SERVING_ML_FAMILY:
        family = "ml"
    else:
        family = None
    if amount is None or family is None:
        return None, None, raw_text
    return amount, family, raw_text


def normalize_usda_branded(food: Dict[str, Any]) -> Tuple[Dict[str, Any], bool, str]:
    """Normalize a branded FDC food dict to per-100g columns (NULL-safe).

    T12 (Bug 010-003-T12-1): search-payload foodNutrients are ALREADY per-100g
    — pass values through with NO serving rescale (the old 100/grams factor
    manufactured phantom kcal, e.g. raw 571 on a 28 g row -> 2039).
    Unparseable serving no longer NULLs nutrients (they are usable as-is);
    it only yields a review note since serving defaults/density degrade.
    """
    raw = {str(n.get("nutrientName") or "").strip().lower():
           (n.get("value"), n.get("unitName"))
           for n in (food.get("foodNutrients") or [])}
    out: Dict[str, Any] = {c: None for c in NUTRIENT_COLS}
    for name, (val, _u) in raw.items():
        col = USDA_TO_COL.get(name)
        if not col or val is None:
            continue
        try:
            out[col] = float(val)
        except (TypeError, ValueError):
            continue
    notes = []
    size, unit = food.get("servingSize"), food.get("servingSizeUnit")
    if parse_serving_grams(size, unit) is None:
        notes.append(f"unparseable serving: {size} {unit}")
    missing = [c for c in NUTRIENT_COLS if out[c] is None]
    if missing:
        notes.append(f"unreported at source: {', '.join(missing)}")
    return out, bool(notes), "; ".join(notes)


def normalize_usda_survey(food: Dict[str, Any]) -> Dict[str, Any]:
    """Survey/Legacy/Foundation rows are already per-100g: extract, NULL when absent.

    T12: energy resolved by nutrientId — 1008 first, else 2048 (Atwater
    Specific), else 2047 (Atwater General). Foundation rows (e.g. almond
    flour fdcId 2261420) carry energy ONLY under the Atwater ids.
    """
    by_id = {n.get("nutrientId"): n.get("value")
             for n in (food.get("foodNutrients") or [])}
    raw = {str(n.get("nutrientName") or "").strip().lower(): n.get("value")
           for n in (food.get("foodNutrients") or [])}
    out: Dict[str, Any] = {}
    for usda_name, col in USDA_TO_COL.items():
        if not col:
            continue
        if col == "kcal_per_100g":
            val = next((by_id.get(i) for i in _ENERGY_IDS
                        if by_id.get(i) is not None), None)
            if val is None:
                val = raw.get(usda_name)
        else:
            val = raw.get(usda_name)
        try:
            out[col] = float(val) if val is not None else None
        except (TypeError, ValueError):
            out[col] = None
    return out


def normalize_off(nutriments: Dict[str, Any]) -> Dict[str, Any]:
    """OFF nutriments are *_100g already; caffeine always NULL (no field)."""
    out: Dict[str, Any] = {c: None for c in NUTRIENT_COLS}
    for off_key, col in OFF_TO_COL.items():
        val = (nutriments or {}).get(off_key)
        try:
            out[col] = float(val) if val is not None else None
        except (TypeError, ValueError):
            out[col] = None
    return out
