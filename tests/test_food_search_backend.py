"""010-003 T04: backend search / suggest / persist-on-log tests (no DB, no net).

Env-ordering note (mirrors test_config): `food_search` imports `service_logins`
-> `credential_management`, whose module-level `load_key()` needs `KEY_PATH`.
Loading `backend/.env` first (as the app does) fixes the ordering. Upstream
USDA/OFF calls and DB helpers are monkeypatched, so no network/DB is touched.
"""

from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.testclient import TestClient

load_dotenv(Path(__file__).resolve().parents[1] / "backend" / ".env",
            override=False)

import backend.api.food as food_api  # noqa: E402
import backend_functions.food_search as fsearch  # noqa: E402
from backend_functions.food_normalize import (  # noqa: E402
    normalize_off, normalize_usda_branded,
)


def _make_app():
    app = FastAPI()
    app.include_router(food_api.router)
    return app


# ---------------------------------------------------------------------------
# Fixtures: USDA hit builders (survey rows are already per-100g).
# ---------------------------------------------------------------------------

_EXTRA_ALL = {
    "saturated fat": 1.0, "trans fat": 0.0, "cholesterol, total": 5.0,
    "sodium, na": 40.0, "fiber, total dietary": 0.0, "total sugars": 5.0,
    "added sugars": 0.0, "vitamin d": 1.0, "calcium, ca": 120.0,
    "iron, fe": 0.1, "potassium, k": 150.0,
}


def _survey(name, dtype, nutrients, extras=None, score=100.0, fdc=1):
    nuts = [{"nutrientName": k, "value": v} for k, v in nutrients.items()]
    for k, v in (extras or {}).items():
        nuts.append({"nutrientName": k, "value": v})
    return {"description": name, "dataType": dtype, "foodNutrients": nuts,
            "fdcId": fdc, "score": score}


def _complete():
    return {"Energy": 60.0, "Protein": 3.0, "Total lipid (fat)": 3.3,
            "Carbohydrate, by difference": 4.8}


def _partial():
    # missing Protein -> fails the kcal/P/F/C gate -> must sink.
    return {"Energy": 500.0, "Total lipid (fat)": 30.0,
            "Carbohydrate, by difference": 55.0}


# ---------------------------------------------------------------------------
# Ranking (OQ-4): gate sinks, relevance tier, quality label.
# ---------------------------------------------------------------------------

def test_rank_sinks_partial_and_orders_by_relevance_tier():
    hit_full = _survey("whole milk", "Foundation", _complete(), _EXTRA_ALL, fdc=1)
    hit_partial = _survey("milk chocolate", "Branded", _partial(), fdc=2)
    hit_most = _survey("milk", "SR Legacy", _complete(),
                       {"sodium, na": 40.0, "total sugars": 5.0,
                        "calcium, ca": 120.0, "potassium, k": 150.0}, fdc=3)

    ranked = fsearch.rank_usda_hits([hit_partial, hit_most, hit_full], "whole milk")

    assert [m["name"] for m in ranked] == ["whole milk", "milk", "milk chocolate"]
    assert ranked[-1]["gate_ok"] is False
    assert ranked[-1]["needs_review"] is True
    assert ranked[0]["gate_ok"] and ranked[1]["gate_ok"]


def test_quality_label_thresholds():
    # T09 (OQ-7): High >= 6 (survey rows max 6), Medium >= 3.
    assert fsearch._quality_label(11) == "High"
    assert fsearch._quality_label(6) == "High"
    assert fsearch._quality_label(5) == "Medium"
    assert fsearch._quality_label(3) == "Medium"
    assert fsearch._quality_label(2) == "Low"
    assert fsearch._quality_label(0) == "Low"


def test_name_bonus_orders_exact_first():
    """T09 (OQ-7): exact > starts-with > whole-word > contains, no extra fetches.

    Mirrors the live 'milk' report: "Milk, whole"-class hits must outrank
    "Crackers, milk"-class hits deterministically, not by fetch luck.
    """
    assert fsearch._name_bonus("milk", "milk") == 0
    assert fsearch._name_bonus("milk, whole", "milk") == 1
    assert fsearch._name_bonus("crackers, milk", "milk") == 2
    assert fsearch._name_bonus("buttermilk", "milk") == 3

    hits = [
        _survey("crackers, milk", "SR Legacy", _complete(), _EXTRA_ALL, fdc=1),
        _survey("buttermilk", "SR Legacy", _complete(), _EXTRA_ALL, fdc=2),
        _survey("milk, whole", "Foundation", _complete(), _EXTRA_ALL, fdc=3),
        _survey("milk", "SR Legacy", _complete(), _EXTRA_ALL, fdc=4),
    ]
    ranked = fsearch.rank_usda_hits(hits, "milk")
    assert [m["name"] for m in ranked] == [
        "milk", "milk, whole", "crackers, milk", "buttermilk"]


def test_quality_label_counts_extras():
    full = fsearch._map_usda_food(
        _survey("whole milk", "Foundation", _complete(), _EXTRA_ALL))
    assert fsearch._quality_label(full["present"]) == "High"
    assert full["present"] == 11


# ---------------------------------------------------------------------------
# Fetch budget (<=3) + ~1d normalized-query cache.
# ---------------------------------------------------------------------------

def test_fetch_budget_and_cache(monkeypatch):
    fsearch._cache.clear()
    fsearch._cache_order.clear()
    calls = []

    async def fake_fetch(client, query, page_size, data_types=None,
                         require_all_words=False):
        calls.append((page_size, data_types))
        if data_types and "Branded" in data_types:
            return []  # branded < 5 -> triggers exactly one fallback
        return [_survey("whole milk", "Foundation", _complete(), _EXTRA_ALL)]

    monkeypatch.setattr(fsearch, "_fetch_usda", fake_fetch)

    import asyncio
    first = asyncio.run(fsearch.fetch_usda_ranked("Whole  Milk"))
    assert first["fetches"] == 3            # 2 parallel + 1 fallback
    assert len(calls) == 3
    assert first["cached"] is False
    assert first["results"][0]["name"] == "whole milk"

    second = asyncio.run(fsearch.fetch_usda_ranked("whole milk"))
    assert second["cached"] is True         # cache hit, no new fetches
    assert len(calls) == 3
# ---------------------------------------------------------------------------
# GET /api/food/search -> CombinedSearchResult[] (saved-first, trimmed).
# ---------------------------------------------------------------------------

_SEARCH_KEYS = {"kind", "food_id", "name", "mode_qty", "mode_unit",
                "kcal_preview", "data_quality", "rank", "result_source",
                "usda_score", "fdcId", "barcode", "nutrients_raw",
                # T10 (OQ-8): panel nutrients + feed serving default.
                "nutrients", "serving_size", "serving_unit", "serving_text",
                # 010-004 T01: ranked-search fields (null until T02 populates).
                "quality_percent", "match_pct", "match_band",
                "is_single_ingredient", "ingredient_count", "kcal_delta_pct",
                "excluded_kcal_mismatch", "food_category",
                "serving_unit_facet"}

_SAVED_ROW = {
    "food_id": 7, "name": "Whole Milk", "source": "db",
    "kcal_per_100g": 60.0, "protein_g": 3.0, "fat_g": 3.3, "carbs_g": 4.8,
    "fiber_g": 0.0, "sugars_g": 5.0, "sodium_mg": 40.0, "caffeine_mg": None,
    # T10 (OQ-8): row serving travels for the popup default/panel.
    "serving_size": 240.0, "serving_unit": "ml", "serving_text": "1 cup",
}


def test_search_returns_trimmed_saved_first_results(monkeypatch):
    monkeypatch.setattr(food_api, "search_saved_foods",
                        lambda q, limit: [dict(_SAVED_ROW)])

    async def fake_ranked(query):
        return {"results": [{
            "name": "Milk, whole", "dtype": "SR Legacy",
            "nuts": {"kcal_per_100g": 61.0}, "present": 8,
            "gate_ok": True, "score": 99.0, "raw": {"huge": "payload"},
            "serving_size": 84.0, "serving_unit": "g",
            "serving_text": "0.5 cup",
        }], "fetches": 2, "cached": False}

    monkeypatch.setattr(fsearch, "fetch_usda_ranked", fake_ranked)

    client = TestClient(_make_app())
    res = client.get("/api/food/search?q=milk")
    assert res.status_code == 200
    body = res.json()
    assert len(body) == 2
    assert set(body[0].keys()) == _SEARCH_KEYS
    assert set(body[1].keys()) == _SEARCH_KEYS
    assert body[0]["kind"] == "db-food" and body[0]["rank"] == 0
    assert body[0]["result_source"] == "saved"
    # T10 (OQ-8): db hit carries its own nutrients + row serving.
    assert body[0]["nutrients"]["protein_g"] == 3.0
    assert body[0]["serving_size"] == 240.0
    assert body[0]["serving_unit"] == "ml"
    assert body[1]["kind"] == "usda" and body[1]["rank"] == 1
    assert body[1]["result_source"] == "usda:SR Legacy"
    # T10 (OQ-8): usda hit carries normalized nutrients + feed serving.
    assert body[1]["nutrients"]["kcal_per_100g"] == 61.0
    assert body[1]["serving_size"] == 84.0
    assert body[1]["serving_text"] == "0.5 cup"
    # Backend trims: the ranked-mapped raw key never crosses the wire, but
    # nutrients_raw carries the upstream hit for persist-on-log (T08).
    assert "raw" not in body[1] and "huge" not in body[1]


def test_search_suppresses_usda_hit_when_fdcid_saved(monkeypatch):
    saved = dict(_SAVED_ROW)
    saved["fdc_id"] = 12345
    monkeypatch.setattr(food_api, "search_saved_foods",
                        lambda q, limit: [saved])

    async def fake_ranked(query):
        return {"results": [
            {"name": "MILK, WHOLE", "dtype": "Branded", "fdcId": 12345,
             "nuts": {"kcal_per_100g": 61.0}, "present": 8,
             "gate_ok": True, "score": 99.0,
             "raw": {"fdcId": 12345, "description": "MILK, WHOLE"}},
            {"name": "Milk alternative", "dtype": "SR Legacy", "fdcId": 999,
             "nuts": {"kcal_per_100g": 40.0}, "present": 8,
             "gate_ok": True, "score": 50.0,
             "raw": {"fdcId": 999, "description": "Milk alternative"}},
        ], "fetches": 2, "cached": False}

    monkeypatch.setattr(fsearch, "fetch_usda_ranked", fake_ranked)

    client = TestClient(_make_app())
    res = client.get("/api/food/search?q=milk")
    assert res.status_code == 200
    body = res.json()
    # Saved row + only the unsaved USDA hit; the fdcId-12345 duplicate gone.
    assert [b["name"] for b in body] == ["Whole Milk", "Milk alternative"]
    assert body[1]["fdcId"] == 999
    assert body[1]["nutrients_raw"] == {"fdcId": 999,
                                       "description": "Milk alternative"}


# ---------------------------------------------------------------------------
# 010-004 T02: hybrid match bands, kcal hard-filter gate, presence percent,
# single-ingredient flag + AC-4 sort order.
# ---------------------------------------------------------------------------

def _branded(name, nutrients, ingredients, category="Beverages", fdc=1,
             serving=(100.0, "g"), alcohol=None, score=100.0):
    nuts = [{"nutrientName": k, "value": v}
            for k, v in nutrients.items()]
    if alcohol is not None:
        nuts.append({"nutrientName": "Alcohol, ethyl", "nutrientId": 1018,
                     "value": alcohol})
    size, unit = serving
    return {"description": name, "dataType": "Branded",
            "foodNutrients": nuts, "ingredients": ingredients,
            "foodCategory": category, "servingSize": size,
            "servingSizeUnit": unit, "fdcId": fdc, "score": score}


def test_match_pct_anchors_then_overlap():
    assert fsearch._match_pct("coffee", "coffee") == 100
    assert fsearch._match_pct("coffee ice cream", "coffee") == 90
    assert fsearch._match_pct("drip coffee", "coffee") == 80
    assert fsearch._match_pct("crackers, milk", "coffee") == 0
    assert fsearch._match_band(100) == 10
    assert fsearch._match_band(85) == 8
    assert fsearch._match_band(0) == 0


def test_kcal_gate_hard_filter_with_floor_and_alcohol():
    # Black-coffee class: huge relative error, tiny absolute -> kept.
    excluded, _ = fsearch._kcal_gate(
        {"kcal_per_100g": 2.0, "protein_g": 0.3, "fat_g": 0.0,
         "carbs_g": 0.0})
    assert excluded is False
    # Wildly misreported macros -> dropped.
    excluded, delta = fsearch._kcal_gate(
        {"kcal_per_100g": 500.0, "protein_g": 3.0, "fat_g": 3.0,
         "carbs_g": 5.0})
    assert excluded is True and delta is not None and delta > 15
    # Alcohol-aware: 7A closes the gap when present.
    kept, _ = fsearch._kcal_gate(
        {"kcal_per_100g": 100.0, "protein_g": 1.0, "fat_g": 0.5,
         "carbs_g": 5.0}, alcohol_g=10.0)
    assert kept is False
    # Missing macro -> gate does not apply (no drop).
    kept, delta = fsearch._kcal_gate({"kcal_per_100g": 50.0})
    assert kept is False and delta is None


def test_quality_percent_presence_and_single_ingredient():
    nuts = {"kcal_per_100g": 2.0, "protein_g": 0.3, "fat_g": 0.0,
            "carbs_g": 0.0, "fiber_g": 0.0, "sugars_g": 0.0,
            "sodium_mg": 5.0, "caffeine_mg": 40.0}
    assert fsearch._quality_percent(nuts, 240, "ml") == 100
    # Caffeine null always penalizes (9/10 -> 90).
    nuts["caffeine_mg"] = None
    assert fsearch._quality_percent(nuts, 240, "ml") == 90
    # Serving presence counts even when unparseable; multi-word query works.
    assert fsearch._quality_percent(nuts, None, None) == 70
    assert fsearch._ingredient_stats("drip coffee", "drip coffee") == (
        True, 2)
    assert fsearch._ingredient_stats("cream, sugar, coffee",
                                     "coffee") == (False, 3)
    assert fsearch._ingredient_stats(None, "coffee") == (False, None)


def test_rank_coffee_single_ingredient_first_mismatch_dropped():
    good = _branded("coffee", {"Energy": 2.0, "Protein": 0.3,
                               "Total lipid (fat)": 0.0,
                               "Carbohydrate, by difference": 0.0},
                    "coffee", fdc=1, score=10.0)
    dessert = _branded("coffee ice cream",
                       {"Energy": 200.0, "Protein": 3.0,
                        "Total lipid (fat)": 10.0,
                        "Carbohydrate, by difference": 25.0},
                       "cream, sugar, coffee powder", fdc=2, score=999.0)
    bad = _branded("coffee candy", {"Energy": 500.0, "Protein": 3.0,
                                    "Total lipid (fat)": 3.0,
                                    "Carbohydrate, by difference": 5.0},
                   "sugar, coffee", fdc=3, score=999.0)
    ranked = fsearch.rank_usda_hits([dessert, bad, good], "coffee")
    names = [m["name"] for m in ranked]
    assert "coffee candy" not in names  # kcal gate hard filter
    assert names[0] == "coffee"  # exact + single-ingredient first
    assert ranked[0]["match_band"] == 10
    assert ranked[0]["is_single_ingredient"] is True
    assert ranked[0]["quality_percent"] is not None
    assert ranked[0]["food_category"] == "Beverages"
    assert ranked[0]["serving_unit_facet"] == "g"


def test_rename_persisted_then_duplicate_routes_to_existing(monkeypatch):
    """T08 (OQ-6): custom name saved to foods row; duplicate fdcId keeps it."""
    saved_name = {}

    def fake_persist(provider, name, fdc_id, barcode, raw, per100,
                     needs_review, reason, normalized_from=None, **kwargs):
        saved_name["name"] = name
        saved_name["fdc_id"] = fdc_id
        saved_name["normalized_from"] = normalized_from
        return {"food_id": 42}

    monkeypatch.setattr(food_api, "persist_api_food", fake_persist)
    monkeypatch.setattr(food_api, "log_diary_entry",
                        lambda **kw: dict(SAMPLE_ENTRY))

    client = TestClient(_make_app())
    res = client.post("/api/food/diary/from-api", json={
        "usda_payload": {"provider": "usda", "fdcId": 5, "name": "Coffee",
                         "nutrients_raw": _survey("DARK ROAST COFFEE",
                                                 "Branded", _complete(),
                                                 _EXTRA_ALL,
                                                 ) | {"servingSize": 100.0,
                                                      "servingSizeUnit": "g"}},
        "qty": 100.0, "unit": "g",
    })
    assert res.status_code == 201
    # Edited name (not the upstream description) is what gets persisted,
    # with a CHECK-valid normalized_from (Bug T08-1: 'usda' violated it).
    assert saved_name == {"name": "Coffee", "fdc_id": 5,
                         "normalized_from": "usda_branded"}


# ---------------------------------------------------------------------------
# Persist-on-log: API hit -> foods.foods row -> diary entry.
# ---------------------------------------------------------------------------

SAMPLE_ENTRY = {
    "entry_id": 10, "food_id": 42, "name_snapshot": "Milk",
    "nutrients_snapshot": {"kcal_per_100g": 61.0, "protein_g": 3.0,
                           "fat_g": 3.3, "carbs_g": 4.8, "fiber_g": None,
                           "sugars_g": None, "sodium_mg": None,
                           "caffeine_mg": None},
    "is_partial": True, "qty": 100.0, "unit": "g", "kcal": 61.0,
    "logged_at": "2026-10-04T12:00:00+00:00",
}


def test_persist_on_log_creates_food_then_entry(monkeypatch):
    order = []

    def fake_persist(*a, **kw):
        order.append("persist")
        return {"food_id": 42}

    monkeypatch.setattr(food_api, "persist_api_food", fake_persist)

    def fake_log(**kw):
        order.append("log")
        assert kw["food_id"] == 42
        return dict(SAMPLE_ENTRY)
    monkeypatch.setattr(food_api, "log_diary_entry", fake_log)

    client = TestClient(_make_app())
    res = client.post("/api/food/diary/from-api", json={
        "usda_payload": {"provider": "usda", "fdcId": 5, "name": "Milk",
                         "nutrients_raw": _survey("Milk", "SR Legacy",
                                                  _complete())},
        "qty": 100.0, "unit": "g",
    })
    assert res.status_code == 201
    assert order == ["persist", "log"]      # foods row first, then the entry
    assert res.json()["food_id"] == 42


def test_persist_api_food_uses_check_valid_normalized_from(monkeypatch):
    """Bug T08-1: INSERT normalized_from must satisfy the foods CHECK."""
    import backend_functions.queries.food_queries as fq
    seen = {}
    CHECKED = {'usda_branded', 'usda_survey', 'usda_legacy',
               'off', 'manual', 'recipe'}

    def fake_insert(q, params):
        seen["params"] = params
        return [{"food_id": 42}]

    monkeypatch.setattr(fq, "sql_to_dict", lambda q, p=None: [])
    monkeypatch.setattr(fq, "sql_insert_returning", fake_insert)
    per100 = {"kcal_per_100g": 60.0, "protein_g": 3.0, "fat_g": 3.3,
              "carbs_g": 4.8, "fiber_g": None, "sugars_g": None,
              "sodium_mg": None, "caffeine_mg": None}
    row = fq.persist_api_food("usda", "Coffee", 5, None, {"raw": 1},
                              per100, True, "unreported",
                              normalized_from="usda_branded")
    assert row == {"food_id": 42}
    label = seen["params"][-3]
    assert label in CHECKED, label
    assert label == "usda_branded"
    # Default path (no label passed) also stays CHECK-valid.
    row = fq.persist_api_food("usda", "Coffee", 6, None, None,
                              per100, True, "unreported")
    assert seen["params"][-3] == "usda_survey"


def test_persist_clears_review_on_insert(monkeypatch):
    """T09 (OQ-7): confirming a pick IS the review — fresh INSERT clears
    needs_review + review_reason; duplicate key returns row untouched."""
    import backend_functions.queries.food_queries as fq
    seen = {"inserts": 0}
    per100 = {"kcal_per_100g": 0.0, "protein_g": None, "fat_g": None,
              "carbs_g": None, "fiber_g": None, "sugars_g": None,
              "sodium_mg": None, "caffeine_mg": None}

    def fake_insert(q, params):
        seen["inserts"] += 1
        seen["params"] = params
        return [{"food_id": 9}]

    monkeypatch.setattr(fq, "sql_to_dict", lambda q, p=None: [])
    monkeypatch.setattr(fq, "sql_insert_returning", fake_insert)
    # Caller-provided flags must NOT leak into the row: the confirm clears them.
    row = fq.persist_api_food("usda", "Coffee", 99, None, None, per100,
                              True, "unreported at source")
    assert row == {"food_id": 9}
    assert seen["params"][-2] is False      # needs_review
    assert seen["params"][-1] is None       # review_reason

    # Duplicate natural key: existing row returned as-is, no INSERT.
    existing = {"food_id": 7, "name": "Coffee", "needs_review": True,
                "review_reason": "unreported at source"}

    def fake_lookup(q, p=None):
        if "WHERE fdc_id" in q:
            return [{"food_id": 7}]
        return [existing]

    monkeypatch.setattr(fq, "sql_to_dict", fake_lookup)
    row = fq.persist_api_food("usda", "Coffee", 99, None, None, per100,
                              False, None)
    assert row == existing
    assert seen["inserts"] == 1


def test_log_api_entry_persists_feed_serving(monkeypatch):
    """T10 (OQ-8): branded USDA + OFF servings persist (fail-closed units)."""
    seen = {}

    def fake_persist(provider, name, fdc_id, barcode, raw, per100,
                     needs_review, reason, **kw):
        seen.update(kw)
        return {"food_id": 42}

    monkeypatch.setattr(food_api, "persist_api_food", fake_persist)
    monkeypatch.setattr(food_api, "log_diary_entry",
                        lambda **kw: dict(SAMPLE_ENTRY))
    client = TestClient(_make_app())

    # Branded USDA: servingSize 240 ml -> (240, 'ml', household text).
    branded = _survey("Cold Brew", "Branded", _complete(), _EXTRA_ALL, fdc=5)
    branded.update({"servingSize": 240.0, "servingSizeUnit": "ml",
                    "householdServingFullText": "1 cup"})
    res = client.post("/api/food/diary/from-api", json={
        "usda_payload": {"provider": "usda", "fdcId": 5, "name": "Cold Brew",
                         "nutrients_raw": branded},
        "qty": 240.0, "unit": "ml",
    })
    assert res.status_code == 201, res.text
    assert seen["serving_size"] == 240.0
    assert seen["serving_unit"] == "ml"
    assert seen["serving_text"] == "1 cup"

    # OFF: serving_quantity 40 g -> (40, 'g', string serving_size as text).
    seen.clear()
    res = client.post("/api/food/diary/from-api", json={
        "off_payload": {"provider": "off", "barcode": "12345", "name": "Bar",
                        "nutrients_raw": {
                            "nutriments": {"energy-kcal_100g": 400.0},
                            "serving_quantity": 40,
                            "serving_quantity_unit": "g",
                            "serving_size": "1 bar (40 g)"}},
        "qty": 1.0, "unit": "serving",
    })
    assert res.status_code == 201, res.text
    assert seen["serving_size"] == 40.0
    assert seen["serving_unit"] == "g"
    assert seen["serving_text"] == "1 bar (40 g)"

    # Messy unit code (upstream "MG") fails closed -> NULL serving columns.
    seen.clear()
    bad = _survey("Mystery", "Branded", _complete(), _EXTRA_ALL, fdc=6)
    bad.update({"servingSize": 28.0, "servingSizeUnit": "MG"})
    res = client.post("/api/food/diary/from-api", json={
        "usda_payload": {"provider": "usda", "fdcId": 6, "name": "Mystery",
                         "nutrients_raw": bad},
        "qty": 100.0, "unit": "g",
    })
    assert res.status_code == 201, res.text
    assert seen["serving_size"] is None
    assert seen["serving_unit"] is None


def test_persist_on_log_requires_payload():
    client = TestClient(_make_app())
    res = client.post("/api/food/diary/from-api",
                      json={"qty": 100.0, "unit": "g"})
    assert res.status_code == 400
# ---------------------------------------------------------------------------
# Pick logging (OQ-4 tuning): recorded on confirm only, with the picked rank.
# ---------------------------------------------------------------------------

def test_confirm_logs_picked_rank(monkeypatch):
    seen = []
    monkeypatch.setattr(fsearch, "log_pick",
                        lambda rank, src, q: seen.append((rank, src, q)))
    monkeypatch.setattr(food_api, "log_diary_entry",
                        lambda **kw: dict(SAMPLE_ENTRY))

    client = TestClient(_make_app())
    res = client.post("/api/food/diary", json={
        "food_id": 42, "qty": 100.0, "unit": "g",
        "rank": 3, "result_source": "usda:SR Legacy", "query": "milk",
    })
    assert res.status_code == 201
    assert seen == [(3, "usda:SR Legacy", "milk")]


def test_confirm_without_pick_does_not_log(monkeypatch):
    seen = []
    monkeypatch.setattr(fsearch, "log_pick", lambda *a: seen.append(a))
    monkeypatch.setattr(food_api, "log_diary_entry",
                        lambda **kw: dict(SAMPLE_ENTRY))

    client = TestClient(_make_app())
    res = client.post("/api/food/diary",
                      json={"food_id": 42, "qty": 100.0, "unit": "g"})
    assert res.status_code == 201
    assert seen == []


# ---------------------------------------------------------------------------
# Normalizers (persist-on-log correctness).
# ---------------------------------------------------------------------------

def test_normalize_usda_branded_passes_through_per_100g():
    # T12 (Bug 010-003-T12-1): branded search nutrients are ALREADY per-100g
    # (LCCS converted upstream) — no 100/grams rescale. Raw 30 on any
    # serving stays 30 (old code manufactured 30 x 100/50 = 60).
    food = {
        "servingSize": 50.0, "servingSizeUnit": "g",
        "foodNutrients": [
            {"nutrientName": "Energy", "value": 30.0},
            {"nutrientName": "Protein", "value": 1.5},
            {"nutrientName": "Total lipid (fat)", "value": 1.0},
            {"nutrientName": "Carbohydrate, by difference", "value": 3.0},
        ],
    }
    per100, needs_review, _reason = normalize_usda_branded(food)
    assert per100["kcal_per_100g"] == 30.0
    assert per100["protein_g"] == 1.5
    assert needs_review is True                # panel nutrients still missing


def test_normalize_usda_branded_keeps_nutrients_on_bad_serving():
    # T12: unparseable serving degrades serving defaults/density only —
    # nutrients pass through as-is with a review note (never NULLed).
    per100, needs_review, reason = normalize_usda_branded(
        {"servingSize": None, "servingSizeUnit": "g",
         "foodNutrients": [{"nutrientName": "Energy", "value": 571.0}]})
    assert per100["kcal_per_100g"] == 571.0
    assert needs_review is True and "unparseable serving" in reason


def test_normalize_off_maps_100g_fields():
    per100 = normalize_off({
        "energy-kcal_100g": 61.0, "proteins_100g": 3.0, "fat_100g": 3.3,
        "carbohydrates_100g": 4.8, "sugars_100g": 5.0, "sodium_100g": 0.04,
    })
    assert per100["kcal_per_100g"] == 61.0 and per100["protein_g"] == 3.0
    assert per100["caffeine_mg"] is None       # OFF has no caffeine field


# ---------------------------------------------------------------------------
# T12 (Bug 010-003-T12-1): almond fixture — pass-through, Atwater fallback,
# consensus/outlier/density flags, anchor-above-flagged order.
# ---------------------------------------------------------------------------

def _almond_foods():
    import json
    from pathlib import Path
    p = (Path(__file__).resolve().parents[1] / ".features" / "descriptions"
         / "json_food_responses" / "almond_responses.json")
    return json.loads(p.read_text())["foods"]


def test_t12_almond_raw_571_stays_571():
    foods = _almond_foods()
    raw28 = next(f for f in foods if f["fdcId"] == 2073909)
    per100, _, _ = normalize_usda_branded(raw28)
    assert per100["kcal_per_100g"] == 571.0    # not 571 x 100/28 = 2039


def test_t12_almond_branded_range_median():
    foods = _almond_foods()
    vals = sorted(per100["kcal_per_100g"]
                  for f in foods if f["dataType"] == "Branded"
                  for per100, _, _ in [normalize_usda_branded(f)]
                  if per100["kcal_per_100g"] is not None)
    assert (min(vals), max(vals)) == (247, 667)
    assert vals.count(571.0) == 8
    assert 571 <= vals[len(vals) // 2] <= 588


def test_t12_foundation_energy_prefers_2048_specific():
    from backend_functions.food_normalize import normalize_usda_survey
    foods = _almond_foods()
    flour = next(f for f in foods if f["fdcId"] == 2261420)
    assert normalize_usda_survey(flour)["kcal_per_100g"] == 578.0
    anchor = next(f for f in foods if f["fdcId"] == 170567)
    assert normalize_usda_survey(anchor)["kcal_per_100g"] == 579.0


def test_t12_flags_argires_post_schnucks_not_canonical():
    foods = _almond_foods()
    ranked = fsearch.rank_usda_hits(foods, "almonds")
    by_fdc = {m["fdcId"]: m for m in ranked}
    # Argires 247 kcal: 73 g for 22 nuts (~26 g expected) -> density flag.
    assert "implausible serving weight" in by_fdc[2045940]["review_reason"]
    # Post cereal: ALMONDS description but Cereal + multi-grain ingredients.
    assert "mismatch" in by_fdc[2653105]["review_reason"]
    # Schnucks: 28 g in 1/45 cup (~5.3 g/mL) -> density flag.
    assert "implausible density" in by_fdc[2621614]["review_reason"]
    # Canonical single-ingredient rows carry no junk flags.
    assert by_fdc[2073909]["penalty"] == 0
    assert "mismatch" not in by_fdc[2073909]["review_reason"]
    assert "implausible" not in by_fdc[2073909]["review_reason"]
    # Each flagged row is annotated (penalty + review note) — flags never
    # block, but they sink below same-tier canonical rows.
    for fdc in (2045940, 2653105, 2621614):
        assert by_fdc[fdc]["penalty"] >= 1
        assert by_fdc[fdc]["needs_review"] is True
    # Anchor is the diary-time reference (579): exact raw value, unflagged.
    assert by_fdc[170567]["nuts"]["kcal_per_100g"] == 579.0
    assert "mismatch" not in by_fdc[170567]["review_reason"]
    assert "implausible" not in by_fdc[170567]["review_reason"]


def test_ac1_almonds_plain_nuts_rank_above_butter():
    """010-004 AC-1 (almonds half): plain nuts above almond butter; exact
    single-ingredient anchor first. Fixture has no almond-extract row."""
    ranked = fsearch.rank_usda_hits(_almond_foods(), "almonds")
    names = [(m["name"] or "").lower().strip() for m in ranked]
    # Top hit: plain, single-ingredient almonds (exact anchor).
    assert ranked[0]["is_single_ingredient"] is True
    assert names[0] in ("almonds", "nuts, almonds")
    # Butter rows exist in the fixture and every plain single-ingredient
    # almonds row precedes the best butter row (match band dominates sort).
    butter = [i for i, n in enumerate(names) if "butter" in n]
    assert butter, "fixture must contain almond butter rows"
    plain = [i for i, m in enumerate(ranked)
             if m["is_single_ingredient"] and names[i] in ("almonds", "nuts, almonds")]
    assert plain and max(plain) < min(butter)