"""Food skeleton API (010-002 T03, stub-backed Option B).

Reads serve the T01 contract shapes from the T02 views/tables; writes
snapshot NULL-safe. Search/scan stay on the 010-001 raw-passthrough proxy
(food_lookup.py) — this router never duplicates them.

Endpoints (contract from T01; shapes served exactly):
    GET  /api/food/usuals?at=ISO-8601        -> UsualFood[]
    GET  /api/food/diary?day=YYYY-MM-DD      -> DayEntry[]
    GET  /api/food/recipes                   -> RecipeSummary[]
    GET  /api/food/foods?q&sort&limit&cursor -> FoodListResponse
    POST /api/food/diary                     -> DayEntry (snapshot write)
    POST /api/food/recipes                   -> RecipeSummary (snapshot write)
    POST /api/food/foods/{id}/delete         -> soft-delete
    POST /api/food/foods/{id}/restore        -> restore
"""

from datetime import date, datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from backend_functions.queries.food_queries import (
    get_food_by_barcode,
    get_food_by_id,
    get_usual_foods,
    get_diary_entries,
    get_diary_stats,
    get_recipes,
    list_foods_page,
    search_foods,
    search_saved_foods,
    persist_api_food,
    log_diary_entry,
    update_diary_entry,
    delete_diary_entry,
    save_recipe,
    set_food_deleted,
)
from backend.schemas.food_contract_schemas import (
    CombinedSearchResult,
    DiaryStats,
    DiaryUpdateRequest,
    FoodRow,
    LogApiEntryRequest,
    LogEntryRequest,
    SaveRecipeRequest,
)

router = APIRouter(prefix="/api/food", tags=["food-skeleton"])


def _nonneg(nuts: dict) -> dict:
    """T10: NutrientSnapshot is ge=0 — sink invalid values to None (never 0)."""
    return {k: (v if v is None or (isinstance(v, (int, float)) and not isinstance(
        v, bool) and v >= 0) else None) for k, v in nuts.items()}


def _log_pick_if_any(rank: Optional[int], result_source: Optional[str],
                     query: Optional[str]) -> None:
    """Tuning hook: record which search rank the user actually picked (OQ-4).

    Called on a successful confirm/log so the telemetry reflects real picks,
    never the ranks merely shown. No-op when the client sent no pick metadata.
    """
    if rank is None:
        return
    from backend_functions.food_search import log_pick
    log_pick(rank, result_source or "", query or "")


@router.get("/usuals")
async def usuals(
    at: Optional[str] = Query(None, description="ISO-8601 instant"),
    limit: int = Query(20, ge=1, le=100),
):
    """Time-of-day-weighted usuals (serve view).

    `at` is validated as ISO-8601 per the contract signature; the serve view
    weights by the server's current hour.
    """
    if at is not None:
        try:
            datetime.fromisoformat(at)
        except ValueError:
            raise HTTPException(status_code=400, detail="at must be ISO-8601")
    try:
        return list(get_usual_foods(limit=limit))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch usuals: {e}")


@router.get("/diary")
async def diary(day: str = Query(..., description="YYYY-MM-DD")):
    """Snapshot history for one day -> DayEntry[]. (no food joins)."""
    try:
        day_d = date.fromisoformat(day)
    except ValueError:
        raise HTTPException(status_code=400, detail="day must be YYYY-MM-DD")
    try:
        return list(get_diary_entries(day_d))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch diary: {e}")


@router.get("/diary/stats", response_model=DiaryStats)
async def diary_stats(day: str = Query(..., description="YYYY-MM-DD (device-local)")):
    """Three-bar header aggregates (device-local day treated literally)."""
    try:
        day_d = date.fromisoformat(day)
    except ValueError:
        raise HTTPException(status_code=400, detail="day must be YYYY-MM-DD")
    try:
        return get_diary_stats(day_d)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch stats: {e}")


@router.patch("/diary/{entry_id}")
async def patch_entry(entry_id: int, req: DiaryUpdateRequest):
    """Edit qty/unit/logged_at (new logged_at moves the entry — OQ-5)."""
    if req.qty is None and req.unit is None and req.logged_at is None:
        raise HTTPException(status_code=400, detail="nothing to update")
    try:
        return update_diary_entry(entry_id=entry_id, qty=req.qty,
                                  unit=req.unit, logged_at=req.logged_at)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update entry: {e}")


@router.delete("/diary/{entry_id}")
async def delete_entry(entry_id: int):
    """Delete a diary entry (history removed; the food row is untouched)."""
    try:
        delete_diary_entry(entry_id)
        return {"success": True, "entry_id": entry_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete entry: {e}")


@router.get("/suggest")
async def suggest(
    at: Optional[str] = Query(None, description="ISO-8601 instant"),
    limit: int = Query(10, ge=1, le=20),
):
    """Top-10 time-of-day suggestions (vw_usual_foods + mode qty/unit)."""
    if at is not None:
        try:
            datetime.fromisoformat(at)
        except ValueError:
            raise HTTPException(status_code=400, detail="at must be ISO-8601")
    try:
        return list(get_usual_foods(limit=limit))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch suggest: {e}")


@router.get("/search", response_model=List[CombinedSearchResult])
async def combined_search(
    q: str = Query(..., description="food text query"),
    limit: int = Query(30, ge=1, le=30),
):
    """Combined search: saved foods first, then USDA tiered-ranked (OQ-4).

    Display contract: frontend shows top 8-10, show-more reveals the rest
    (≤30) from this same fetch — never a second call.

    010-004 sort (AC-4): saved-first, then match_band desc,
    is_single_ingredient, quality_percent desc, ingredient_count asc.
    New optional fields (match_pct/match_band/quality_percent/
    is_single_ingredient/ingredient_count/kcal_delta_pct/
    excluded_kcal_mismatch/food_category/serving_unit_facet) ride on each
    hit; T02 populates them for USDA hits, saved rows carry nulls.

    T08 (OQ-6): hits carry fdcId + the raw USDA hit body so confirm can
    persist via the normalizer path without a re-fetch; USDA hits whose
    fdcId is already saved are suppressed (single saved DB row, ranked
    first). Raw bodies never reach the UI beyond nutrients_raw passthrough.
    """
    from backend_functions.food_search import _quality_label, fetch_usda_ranked
    query = (q or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="q must be non-empty")
    try:
        saved = search_saved_foods(q=query, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Saved search failed: {e}")
    out: List[dict] = []
    saved_fdc_ids = set()
    saved_barcodes = set()
    for row in saved:
        if row.get("fdc_id") is not None:
            saved_fdc_ids.add(row.get("fdc_id"))
        if row.get("barcode"):
            saved_barcodes.add(row.get("barcode"))
        nuts = {k: row.get(k) for k in (
            "kcal_per_100g", "protein_g", "fat_g", "carbs_g", "fiber_g",
            "sugars_g", "sodium_mg", "caffeine_mg")}
        gate_ok = all(nuts.get(k) is not None for k in (
            "kcal_per_100g", "protein_g", "fat_g", "carbs_g"))
        present = sum(1 for k in (
            "fiber_g", "sugars_g", "sodium_mg", "caffeine_mg") if nuts.get(k) is not None)
        out.append({
            "kind": "db-recipe" if row.get("source") == "recipe" else "db-food",
            "food_id": row.get("food_id"),
            "name": row.get("name"),
            "mode_qty": None, "mode_unit": None,
            "kcal_preview": nuts.get("kcal_per_100g"),
            "data_quality": _quality_label(present) if gate_ok else "Low",
            "rank": 0, "result_source": "saved",
            "usda_score": None,
            "fdcId": row.get("fdc_id"),
            "barcode": row.get("barcode"),
            # T10 (OQ-8): per-100g panel + feed serving (foods columns).
            "nutrients": _nonneg(nuts),
            "serving_size": row.get("serving_size"),
            "serving_unit": row.get("serving_unit"),
            "serving_text": row.get("serving_text"),
            # 010-004 T01: ranked-search fields are null on saved rows
            # (T02 populates USDA hits; facets derive client-side).
            "quality_percent": None,
            "match_pct": None,
            "match_band": None,
            "is_single_ingredient": None,
            "ingredient_count": None,
            "kcal_delta_pct": None,
            "excluded_kcal_mismatch": None,
            "food_category": None,
            "serving_unit_facet": None,
        })
    try:
        fetched = await fetch_usda_ranked(query)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"USDA search failed: {e}")
    for m in fetched["results"]:
        fdc_id = m.get("fdcId")
        if fdc_id is not None and fdc_id in saved_fdc_ids:
            continue
        out.append({
            "kind": "usda",
            "food_id": None,
            "name": m["name"],
            "mode_qty": None, "mode_unit": None,
            "kcal_preview": m["nuts"].get("kcal_per_100g"),
            "data_quality": _quality_label(m["present"])
            if m["gate_ok"] else "Low",
            "rank": 0, "result_source": f"usda:{m['dtype']}",
            "usda_score": m.get("score"),
            "fdcId": fdc_id,
            "barcode": None,
            # Raw hit body for persist-on-log (normalizer reads foodNutrients
            # + servingSize; a flattened per-100g map would fail closed).
            "nutrients_raw": m.get("raw") if isinstance(
                m.get("raw"), dict) else None,
            # T10 (OQ-8): normalized per-100g panel + feed serving default.
            "nutrients": _nonneg(m["nuts"]),
            "serving_size": m.get("serving_size"),
            "serving_unit": m.get("serving_unit"),
            "serving_text": m.get("serving_text"),
            # 010-004 T01: mapped fields pass through when T02 provides
            # them; legacy/trimmed maps land as null (no re-fetch).
            "quality_percent": m.get("quality_percent"),
            "match_pct": m.get("match_pct"),
            "match_band": m.get("match_band"),
            "is_single_ingredient": m.get("is_single_ingredient"),
            "ingredient_count": m.get("ingredient_count"),
            "kcal_delta_pct": m.get("kcal_delta_pct"),
            "excluded_kcal_mismatch": m.get("excluded_kcal_mismatch"),
            "food_category": m.get("food_category"),
            "serving_unit_facet": m.get("serving_unit_facet"),
        })
    # Saved-first, then mapped order (already tiered); assign final ranks.
    # Each hit carries `rank` + `result_source`; the confirm endpoints call
    # `log_pick` with the user's chosen rank for tuning (OQ-4).
    for i, r in enumerate(out[:limit]):
        r["rank"] = i
    return out[:limit]


@router.post("/diary/from-api", status_code=201)
async def log_api_entry(req: LogApiEntryRequest):
    """Persist-on-log: normalize an API hit into foods.foods, then log it."""
    from backend_functions.food_normalize import (
        normalize_off, normalize_usda_branded, normalize_usda_survey,
        serving_fields)
    payload = req.usda_payload or req.off_payload
    if payload is None:
        raise HTTPException(status_code=400, detail="usda/off payload required")
    provider = payload.provider
    try:
        raw = payload.nutrients_raw or {}
        # T10 (OQ-8): the feed's standard serving (fail-closed; survey rows
        # carry none). Display text is kept, never parsed.
        if provider == "usda":
            dtype = (raw.get("dataType") or "") if isinstance(raw, dict) else ""
            if dtype == "Branded":
                per100, needs_review, reason = normalize_usda_branded(raw)
                normalized_from = "usda_branded"
            else:
                per100 = normalize_usda_survey(raw)
                needs_review = any(v is None for v in per100.values())
                reason = "unreported at source" if needs_review else ""
                normalized_from = ("usda_legacy" if dtype in ("SR Legacy", "Foundation")
                                   else "usda_survey")
            fdc_id, barcode = payload.fdcId, None
            s_size, s_unit, s_text = serving_fields(
                raw.get("servingSize"), raw.get("servingSizeUnit"),
                raw.get("householdServingFullText")) if isinstance(raw, dict) else (None, None, None)
        else:
            nutr = raw.get("nutriments", raw) if isinstance(raw, dict) else {}
            per100 = normalize_off(nutr if isinstance(nutr, dict) else {})
            needs_review = any(v is None for v in per100.values())
            reason = "unreported at source" if needs_review else ""
            normalized_from = "off"
            fdc_id, barcode = None, payload.barcode
            s_size, s_unit, s_text = serving_fields(
                raw.get("serving_quantity"), raw.get("serving_quantity_unit"),
                raw.get("serving_size") if isinstance(
                    raw.get("serving_size"), str) else None
            ) if isinstance(raw, dict) else (None, None, None)
        row = persist_api_food(provider, payload.name, fdc_id, barcode,
                               raw, per100, needs_review, reason or None,
                               normalized_from=normalized_from,
                               serving_size=s_size, serving_unit=s_unit,
                               serving_text=s_text)
        entry = log_diary_entry(food_id=row["food_id"], qty=req.qty,
                                unit=req.unit, logged_at=req.logged_at)
        _log_pick_if_any(req.rank, req.result_source, req.query)
        return entry
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to log API entry: {e}")


@router.get("/by-barcode")
async def food_by_barcode(barcode: str = Query(..., description="packaging barcode")):
    """T08 (OQ-6): alive food row by barcode for the scan-first DB check.

    Returns the food row shape (with source/fdc_id/barcode) or
    `{found: false}` when unknown — the frontend then does the OFF lookup.
    """
    code = (barcode or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="barcode must be non-empty")
    try:
        row = get_food_by_barcode(code)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Barcode lookup failed: {e}")
    if row is None:
        return {"found": False, "barcode": code}
    nuts = {k: row.get(k) for k in (
        "kcal_per_100g", "protein_g", "fat_g", "carbs_g", "fiber_g",
        "sugars_g", "sodium_mg", "caffeine_mg")}
    return {"found": True, "food_id": row.get("food_id"),
            "name": row.get("name"),
            "source": row.get("source"), "fdc_id": row.get("fdc_id"),
            "barcode": row.get("barcode"), "nutrients": nuts,
            # T10 (OQ-8): scan-DB picks default + panel from the row serving.
            "serving_size": row.get("serving_size"),
            "serving_unit": row.get("serving_unit"),
            "serving_text": row.get("serving_text")}


@router.get("/recipes")
async def recipes():
    """Recipe Box list -> RecipeSummary[] (deleted hidden)."""
    try:
        return list(get_recipes())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch recipes: {e}")


@router.get("/foods")
async def foods(
    q: str = Query(""),
    limit: int = Query(20, ge=1, le=100),
    cursor: Optional[int] = Query(None),
    include_deleted: bool = Query(False),
):
    """Ranked search / page -> FoodListResponse; deleted excluded by default."""
    try:
        if q:
            return search_foods(q=q, include_deleted=include_deleted,
                                limit=limit, cursor=cursor)
        return list_foods_page(limit=limit, cursor=cursor,
                               include_deleted=include_deleted)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch foods: {e}")


@router.post("/diary", status_code=201)
async def log_entry(req: LogEntryRequest):
    """Log a diary entry (DayEntry snapshot; soft-deleted foods rejected)."""
    try:
        entry = log_diary_entry(food_id=req.food_id, qty=req.qty,
                                unit=req.unit, logged_at=req.logged_at)
        _log_pick_if_any(req.rank, req.result_source, req.query)
        return entry
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to log entry: {e}")


@router.post("/recipes", status_code=201)
async def create_recipe(req: SaveRecipeRequest):
    """Save a recipe -> RecipeSummary (snapshot recipe-food row)."""
    try:
        steps = [s.model_dump() for s in req.steps]
        return save_recipe(title=req.title, category=req.category, steps=steps)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save recipe: {e}")


@router.get("/foods/{food_id}", response_model=FoodRow)
async def food_by_id(food_id: int):
    """T10 (OQ-8): one alive food row — hydrates suggestion-pick panels.

    Single pk lookup (one indexed query); soft-deleted rows 404 like every
    other pick surface.
    """
    try:
        row = get_food_by_id(food_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Food fetch failed: {e}")
    if row is None:
        raise HTTPException(status_code=404, detail="food not found")
    return row


@router.post("/foods/{food_id}/delete")
async def delete_food(food_id: int):
    """Soft-delete (history snapshots survive; pickers exclude)."""
    try:
        set_food_deleted(food_id, True)
        return {"success": True, "food_id": food_id, "is_deleted": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete food: {e}")


@router.post("/foods/{food_id}/restore")
async def restore_food(food_id: int):
    """Restore via Food Database show-deleted toggle."""
    try:
        set_food_deleted(food_id, False)
        return {"success": True, "food_id": food_id, "is_deleted": False}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to restore food: {e}")
