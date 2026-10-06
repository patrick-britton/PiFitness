"""010-002 T03: food skeleton route contract tests (no DB, no network).

Verifies `backend.api.food` serves EXACTLY the T01 wire contract over HTTP.
The query-layer DB functions are monkeypatched (fixture style, mirroring the
repo's other tests), so no PostgreSQL connection is required. Also locks the
flat-row -> contract-shape serializers in `food_queries`.
"""

from datetime import datetime
from fastapi import FastAPI
from fastapi.testclient import TestClient

import backend.api.food as food_api
from backend_functions import queries as fq


def _make_app():
    app = FastAPI()
    app.include_router(food_api.router)
    return app


SAMPLE_USUAL = {
    "food_id": 1, "name": "Avocado", "source": "db",
    "kcal_per_100g": 160.0, "quality_score": 0.9,
    "times_eaten": 4, "hour_weight": 0.8,
}

SAMPLE_ENTRY = {
    "entry_id": 10, "food_id": 1, "name_snapshot": "Avocado",
    "nutrients_snapshot": {
        "kcal_per_100g": 160.0, "protein_g": 2.0, "fat_g": 14.7,
        "carbs_g": 8.5, "fiber_g": 6.7, "sugars_g": 0.7,
        "sodium_mg": 7.0, "caffeine_mg": None,
    },
    "is_partial": False, "qty": 100.0, "unit": "g",
    "kcal": 160.0, "logged_at": "2026-10-04T12:00:00+00:00",
}

SAMPLE_RECIPE = {
    "recipe_id": 3, "title": "Guacamole", "category": "Appetizer",
    "times_prepared": 0,
}

SAMPLE_FOOD_ROW = {
    "food_id": 1, "name": "Avocado",
    "nutrients": {
        "kcal_per_100g": 160.0, "protein_g": 2.0, "fat_g": 14.7,
        "carbs_g": 8.5, "fiber_g": 6.7, "sugars_g": 0.7,
        "sodium_mg": 7.0, "caffeine_mg": None,
    },
    "is_deleted": False, "deleted_at": None,
}

SAMPLE_PAGE = {"data": [SAMPLE_FOOD_ROW], "next_cursor": 1}


def _patch_reads(monkeypatch):
    monkeypatch.setattr(food_api, "get_usual_foods",
                        lambda **kw: [SAMPLE_USUAL])
    monkeypatch.setattr(food_api, "get_diary_entries",
                        lambda *a, **kw: [SAMPLE_ENTRY])
    monkeypatch.setattr(food_api, "get_recipes",
                        lambda *a, **kw: [SAMPLE_RECIPE])
    monkeypatch.setattr(food_api, "search_foods",
                        lambda **kw: dict(SAMPLE_PAGE))
    monkeypatch.setattr(food_api, "list_foods_page",
                        lambda **kw: dict(SAMPLE_PAGE))


def test_route_table_matches_contract_exactly():
    """The contract endpoints + soft-delete helpers are present."""
    paths = {(r.path, tuple(sorted(getattr(r, "methods", []) or [])))
             for r in food_api.router.routes}
    for expected in [
        ("/api/food/usuals", ("GET",)),
        ("/api/food/diary", ("GET",)),
        ("/api/food/diary/stats", ("GET",)),
        ("/api/food/diary/{entry_id}", ("PATCH",)),
        ("/api/food/diary/{entry_id}", ("DELETE",)),
        ("/api/food/recipes", ("GET",)),
        ("/api/food/foods", ("GET",)),
        ("/api/food/foods/{food_id}", ("GET",)),
        ("/api/food/diary", ("POST",)),
        ("/api/food/recipes", ("POST",)),
        ("/api/food/foods/{food_id}/delete", ("POST",)),
        ("/api/food/foods/{food_id}/restore", ("POST",)),
    ]:
        assert expected in paths, f"missing route {expected}"


def test_get_usuals_returns_usual_food_shapes(monkeypatch):
    _patch_reads(monkeypatch)
    client = TestClient(_make_app())
    res = client.get("/api/food/usuals?at=2026-10-04T12:00:00")
    assert res.status_code == 200
    body = res.json()
    assert isinstance(body, list) and len(body) == 1
    item = body[0]
    assert set(item.keys()) == {
        "food_id", "name", "source", "kcal_per_100g",
        "quality_score", "times_eaten", "hour_weight",
    }


def test_get_diary_returns_day_entry_array(monkeypatch):
    _patch_reads(monkeypatch)
    client = TestClient(_make_app())
    res = client.get("/api/food/diary?day=2026-10-04")
    assert res.status_code == 200
    body = res.json()
    assert isinstance(body, list)
    entry = body[0]
    assert set(entry.keys()) == {
        "entry_id", "food_id", "name_snapshot", "nutrients_snapshot",
        "is_partial", "qty", "unit", "kcal", "logged_at",
    }
    assert set(entry["nutrients_snapshot"].keys()) == {
        "kcal_per_100g", "protein_g", "fat_g", "carbs_g",
        "fiber_g", "sugars_g", "sodium_mg", "caffeine_mg",
    }


def test_get_recipes_returns_recipe_summaries_only(monkeypatch):
    _patch_reads(monkeypatch)
    client = TestClient(_make_app())
    res = client.get("/api/food/recipes")
    assert res.status_code == 200
    recipe = res.json()[0]
    assert set(recipe.keys()) == {
        "recipe_id", "title", "category", "times_prepared",
    }


def test_get_foods_returns_food_list_response(monkeypatch):
    _patch_reads(monkeypatch)
    client = TestClient(_make_app())
    res = client.get("/api/food/foods?q=avo")
    assert res.status_code == 200
    body = res.json()
    assert set(body.keys()) == {"data", "next_cursor"}
    row = body["data"][0]
    assert set(row.keys()) == {
        "food_id", "name", "nutrients", "is_deleted", "deleted_at",
    }
    assert set(row["nutrients"].keys()) == {
        "kcal_per_100g", "protein_g", "fat_g", "carbs_g",
        "fiber_g", "sugars_g", "sodium_mg", "caffeine_mg",
    }
def test_post_diary_returns_day_entry(monkeypatch):
    monkeypatch.setattr(food_api, "log_diary_entry",
                        lambda **kw: dict(SAMPLE_ENTRY))
    client = TestClient(_make_app())
    res = client.post("/api/food/diary", json={
        "food_id": 1, "qty": 100.0, "unit": "g",
    })
    assert res.status_code == 201
    entry = res.json()
    assert "nutrients_snapshot" in entry and "name_snapshot" in entry


def test_post_recipes_returns_recipe_summary(monkeypatch):
    monkeypatch.setattr(food_api, "save_recipe", lambda **kw: dict(SAMPLE_RECIPE))
    client = TestClient(_make_app())
    res = client.post("/api/food/recipes", json={
        "title": "Guacamole", "category": "Appetizer",
        "steps": [{
            "step_no": 1, "instruction": "Mash avocados",
            "timer_seconds": None, "foods": [
                {"food_id": 1, "qty": 100.0, "unit": "g"},
            ],
        }],
    })
    assert res.status_code == 201
    assert set(res.json().keys()) == {
        "recipe_id", "title", "category", "times_prepared",
    }


def test_post_diary_rejects_deleted_food(monkeypatch):
    def _raise(**kw):
        raise ValueError("is soft-deleted and cannot be re-logged")
    monkeypatch.setattr(food_api, "log_diary_entry", _raise)
    client = TestClient(_make_app())
    res = client.post("/api/food/diary",
                      json={"food_id": 9, "qty": 100.0, "unit": "g"})
    assert res.status_code == 400


def test_diary_stats_shape(monkeypatch):
    monkeypatch.setattr(food_api, "get_diary_stats",
                        lambda *a, **kw: {"today_kcal": 500.0,
                                          "last_24h_kcal": 600.0,
                                          "avg_7d_kcal": 550.0,
                                          "scale_max": 600.0})
    client = TestClient(_make_app())
    res = client.get("/api/food/diary/stats?day=2026-10-04")
    assert res.status_code == 200
    assert res.json() == {"today_kcal": 500.0, "last_24h_kcal": 600.0,
                          "avg_7d_kcal": 550.0, "scale_max": 600.0}


def test_patch_entry_moves_day(monkeypatch):
    seen = {}

    def _upd(**kw):
        seen.update(kw)
        return dict(SAMPLE_ENTRY, logged_at=kw.get("logged_at"))
    monkeypatch.setattr(food_api, "update_diary_entry", _upd)
    client = TestClient(_make_app())
    res = client.patch("/api/food/diary/10",
                       json={"qty": 200.0, "logged_at": "2026-10-03T18:00:00"})
    assert res.status_code == 200
    assert seen["entry_id"] == 10 and seen["qty"] == 200.0
    assert res.json()["logged_at"] == "2026-10-03T18:00:00"


def test_patch_entry_empty_rejected():
    client = TestClient(_make_app())
    res = client.patch("/api/food/diary/10", json={})
    assert res.status_code == 400


def test_delete_entry(monkeypatch):
    monkeypatch.setattr(food_api, "delete_diary_entry", lambda *a, **kw: True)
    client = TestClient(_make_app())
    res = client.delete("/api/food/diary/10")
    assert res.status_code == 200
    assert res.json() == {"success": True, "entry_id": 10}


def test_kcal_for_fda_label_table():
    from backend.schemas.food_contract_schemas import UNIT_TO_G_OR_ML
    k = fq.food_queries._kcal_for
    assert k(100, "g", 160.0, conversions=UNIT_TO_G_OR_ML) == 160.0
    assert k(1, "cup", 100.0, conversions=UNIT_TO_G_OR_ML) == 240.0
    assert k(1, "oz", 100.0, conversions=UNIT_TO_G_OR_ML) == 28.0
    assert k(1, "tbsp", 100.0, conversions=UNIT_TO_G_OR_ML) == 15.0
    assert k(2, "serving", 100.0, serving_size=50.0,
             conversions=UNIT_TO_G_OR_ML) == 100.0
    assert k(100, "g", None, conversions=UNIT_TO_G_OR_ML) is None
    try:
        k(1, "pinch", 100.0, conversions=UNIT_TO_G_OR_ML)
    except ValueError:
        pass
    else:
        raise AssertionError("unknown unit must raise")


# ---------------------------------------------------------------------------
# Serializer (flat row -> contract shape) unit tests — no DB / no HTTP.
# ---------------------------------------------------------------------------

def _flat_food_row():
    return {
        "food_id": 1, "name": "Avocado", "kcal_per_100g": 160.0,
        "protein_g": 2.0, "fat_g": 14.7, "carbs_g": 8.5,
        "fiber_g": 6.7, "sugars_g": 0.7, "sodium_mg": 7.0,
        "caffeine_mg": None, "needs_review": False, "review_reason": None,
        "quality_score": 0.9, "is_deleted": False, "deleted_at": None,
    }


def test_to_food_row_nests_nutrients():
    row = fq.food_queries._to_food_row(_flat_food_row())
    assert row["food_id"] == 1 and row["name"] == "Avocado"
    assert row["nutrients"] == {
        "kcal_per_100g": 160.0, "protein_g": 2.0, "fat_g": 14.7,
        "carbs_g": 8.5, "fiber_g": 6.7, "sugars_g": 0.7,
        "sodium_mg": 7.0, "caffeine_mg": None,
    }
    assert row["is_deleted"] is False and row["deleted_at"] is None


def test_to_day_entry_nests_snapshot_and_iso_time():
    flat = dict(_flat_food_row())
    flat.update({
        "entry_id": 10, "name_snapshot": "Avocado", "is_partial": False,
        "qty": 100.0, "unit": "g", "kcal": 160.0,
        "logged_at": datetime(2026, 10, 4, 12, 0, 0),
    })
    entry = fq.food_queries._to_day_entry(flat)
    assert entry["logged_at"] == "2026-10-04T12:00:00"
    assert entry["nutrients_snapshot"]["kcal_per_100g"] == 160.0
    assert entry["name_snapshot"] == "Avocado"


def test_to_recipe_summary_preserves_exact_fields():
    row = {"recipe_id": 3, "title": "Guacamole", "category": "Appetizer",
           "times_prepared": 0, "is_deleted": False, "created_at": None}
    out = fq.food_queries._to_recipe_summary(row)
    assert out == {"recipe_id": 3, "title": "Guacamole",
                   "category": "Appetizer", "times_prepared": 0}


def test_food_by_id_returns_row_or_404(monkeypatch):
    """T10 (OQ-8): pk hydration endpoint serves FoodRow (+ serving), 404s."""
    row = _flat_food_row()
    row.update({"serving_size": 40.0, "serving_unit": "g",
                "serving_text": "1/2 cup"})

    def fake_get(food_id):
        return fq.food_queries._to_food_row(row) if food_id == 1 else None

    monkeypatch.setattr(food_api, "get_food_by_id", fake_get)
    client = TestClient(_make_app())
    res = client.get("/api/food/foods/1")
    assert res.status_code == 200
    body = res.json()
    assert body["food_id"] == 1 and body["name"] == "Avocado"
    assert body["nutrients"]["kcal_per_100g"] == 160.0
    assert body["serving_size"] == 40.0
    assert body["serving_unit"] == "g"
    assert body["serving_text"] == "1/2 cup"
    assert client.get("/api/food/foods/99").status_code == 404