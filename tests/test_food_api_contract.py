"""Food API wire contracts and query-layer logic with every database and network call
mocked.
"""

import pytest
import json
from datetime import datetime
from fastapi import FastAPI
from fastapi.testclient import TestClient

import importlib
import pifitness.food.router as food_api
food_api_module = importlib.import_module('pifitness.food.router')
import pifitness.food.queries as food_queries


def _make_app():
    app = FastAPI()
    app.include_router(food_api)
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
    "recipe_id": 3, "title": "Guacamole", "category": "Side",
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
    monkeypatch.setattr(food_api_module, "get_usual_foods",
                        lambda **kw: [SAMPLE_USUAL])
    monkeypatch.setattr(food_api_module, "get_diary_entries",
                        lambda *a, **kw: [SAMPLE_ENTRY])
    monkeypatch.setattr(food_api_module, "get_recipes",
                        lambda *a, **kw: [SAMPLE_RECIPE])
    monkeypatch.setattr(food_api_module, "search_foods",
                        lambda **kw: dict(SAMPLE_PAGE))
    monkeypatch.setattr(food_api_module, "list_foods_page",
                        lambda **kw: dict(SAMPLE_PAGE))


@pytest.mark.contract
def test_route_table_matches_contract_exactly():
    """The food router registers every contract endpoint by path and method, including
    the soft-delete and restore routes.
    """
    paths = {(r.path, tuple(sorted(getattr(r, "methods", []) or [])))
             for r in food_api.routes}
    for expected in [
        ("/api/food/usuals", ("GET",)),
        ("/api/food/diary", ("GET",)),
        ("/api/food/diary/stats", ("GET",)),
        ("/api/food/diary/{entry_id}", ("PATCH",)),
        ("/api/food/diary/{entry_id}", ("DELETE",)),
        ("/api/food/recipes", ("GET",)),
        ("/api/food/recipes/{recipe_id}", ("GET",)),
        ("/api/food/recipes/{recipe_id}", ("PUT",)),
        ("/api/food/recipes/{recipe_id}", ("DELETE",)),
        ("/api/food/recipes/{recipe_id}/complete", ("POST",)),
        ("/api/food/recipes/{recipe_id}/cook", ("GET",)),
        ("/api/food/recipes/{recipe_id}/cook", ("PUT",)),
        ("/api/food/recipes/{recipe_id}/cook", ("DELETE",)),
        ("/api/food/foods", ("GET",)),
        ("/api/food/foods/{food_id}", ("GET",)),
        ("/api/food/diary", ("POST",)),
        ("/api/food/recipes", ("POST",)),
        ("/api/food/persist", ("POST",)),
        ("/api/food/foods/{food_id}/delete", ("POST",)),
        ("/api/food/foods/{food_id}/restore", ("POST",)),
    ]:
        assert expected in paths, f"missing route {expected}"


@pytest.mark.contract
def test_get_usuals_returns_usual_food_shapes(monkeypatch):
    """GET /api/food/usuals returns a list of usual foods with exactly the UsualFood key
    set.
    """
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


@pytest.mark.contract
def test_get_diary_returns_day_entry_array(monkeypatch):
    """GET /api/food/diary returns a list of day entries with exactly the entry and
    nutrient-snapshot key sets.
    """
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


@pytest.mark.contract
def test_get_recipes_returns_recipe_summaries_only(monkeypatch):
    """GET /api/food/recipes returns recipe summaries carrying only recipe_id, title,
    category and times_prepared.
    """
    _patch_reads(monkeypatch)
    client = TestClient(_make_app())
    res = client.get("/api/food/recipes")
    assert res.status_code == 200
    recipe = res.json()[0]
    assert set(recipe.keys()) == {
        "recipe_id", "title", "category", "times_prepared",
    }


@pytest.mark.contract
def test_get_foods_returns_food_list_response(monkeypatch):
    """GET /api/food/foods returns data and next_cursor with each row's nutrients nested
    and the soft-delete fields present.
    """
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
@pytest.mark.contract
def test_post_diary_returns_day_entry(monkeypatch):
    """POST /api/food/diary returns 201 and a day entry carrying the name and nutrient
    snapshots.
    """
    monkeypatch.setattr(food_api_module, "log_diary_entry",
                        lambda **kw: dict(SAMPLE_ENTRY))
    client = TestClient(_make_app())
    res = client.post("/api/food/diary", json={
        "food_id": 1, "qty": 100.0, "unit": "g",
    })
    assert res.status_code == 201
    entry = res.json()
    assert "nutrients_snapshot" in entry and "name_snapshot" in entry


@pytest.mark.contract
def test_post_recipes_returns_recipe_summary(monkeypatch):
    """POST /api/food/recipes returns 201 and exactly the recipe summary key set."""
    monkeypatch.setattr(food_api_module, "save_recipe", lambda **kw: dict(SAMPLE_RECIPE))
    client = TestClient(_make_app())
    res = client.post("/api/food/recipes", json={
        "title": "Guacamole", "category": "Side",
        "ingredients": [{"food_id": 1, "qty": 100.0, "unit": "g"}],
        "servings_count": 4,
        "serving_unit": "serving",
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


@pytest.mark.contract
def test_post_diary_rejects_deleted_food(monkeypatch):
    """POST /api/food/diary returns 400 when the food has been soft-deleted."""
    def _raise(**kw):
        raise ValueError("is soft-deleted and cannot be re-logged")
    monkeypatch.setattr(food_api_module, "log_diary_entry", _raise)
    client = TestClient(_make_app())
    res = client.post("/api/food/diary",
                      json={"food_id": 9, "qty": 100.0, "unit": "g"})
    assert res.status_code == 400


@pytest.mark.contract
def test_diary_stats_shape(monkeypatch):
    """GET /api/food/diary/stats returns the four-field stats object unchanged."""
    monkeypatch.setattr(food_api_module, "get_diary_stats",
                        lambda *a, **kw: {"today_kcal": 500.0,
                                          "last_24h_kcal": 600.0,
                                          "avg_7d_kcal": 550.0,
                                          "scale_max": 600.0})
    client = TestClient(_make_app())
    res = client.get("/api/food/diary/stats?day=2026-10-04")
    assert res.status_code == 200
    assert res.json() == {"today_kcal": 500.0, "last_24h_kcal": 600.0,
                          "avg_7d_kcal": 550.0, "scale_max": 600.0}


@pytest.mark.contract
def test_patch_entry_moves_day(monkeypatch):
    """PATCH /api/food/diary/{id} forwards qty and logged_at to the update helper and
    echoes the moved logged_at.
    """
    seen = {}

    def _upd(**kw):
        seen.update(kw)
        return dict(SAMPLE_ENTRY, logged_at=kw.get("logged_at"))
    monkeypatch.setattr(food_api_module, "update_diary_entry", _upd)
    client = TestClient(_make_app())
    res = client.patch("/api/food/diary/10",
                       json={"qty": 200.0, "logged_at": "2026-10-03T18:00:00"})
    assert res.status_code == 200
    assert seen["entry_id"] == 10 and seen["qty"] == 200.0
    assert res.json()["logged_at"] == "2026-10-03T18:00:00"


@pytest.mark.contract
def test_patch_entry_empty_rejected():
    """PATCH /api/food/diary/{id} returns 400 for an empty body."""
    client = TestClient(_make_app())
    res = client.patch("/api/food/diary/10", json={})
    assert res.status_code == 400


@pytest.mark.contract
def test_delete_entry(monkeypatch):
    """DELETE /api/food/diary/{id} returns success true with the deleted entry_id."""
    monkeypatch.setattr(food_api_module, "delete_diary_entry", lambda *a, **kw: True)
    client = TestClient(_make_app())
    res = client.delete("/api/food/diary/10")
    assert res.status_code == 200
    assert res.json() == {"success": True, "entry_id": 10}


@pytest.mark.unit
def test_kcal_for_fda_label_table():
    """_kcal_for converts g, cup, oz, tbsp and serving amounts to kcal through the unit
    table, returns None without calories and raises ValueError for an unknown unit.
    """
    from pifitness.food.schemas import UNIT_TO_G_OR_ML
    k = food_queries._kcal_for
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


@pytest.mark.unit
def test_to_food_row_nests_nutrients():
    """_to_food_row nests the eight flat nutrient columns under nutrients and preserves
    the deletion flags.
    """
    row = food_queries._to_food_row(_flat_food_row())
    assert row["food_id"] == 1 and row["name"] == "Avocado"
    assert row["nutrients"] == {
        "kcal_per_100g": 160.0, "protein_g": 2.0, "fat_g": 14.7,
        "carbs_g": 8.5, "fiber_g": 6.7, "sugars_g": 0.7,
        "sodium_mg": 7.0, "caffeine_mg": None,
    }
    assert row["is_deleted"] is False and row["deleted_at"] is None


@pytest.mark.unit
def test_to_day_entry_nests_snapshot_and_iso_time():
    """_to_day_entry nests the nutrient snapshot and renders logged_at as an ISO-8601
    string.
    """
    flat = dict(_flat_food_row())
    flat.update({
        "entry_id": 10, "name_snapshot": "Avocado", "is_partial": False,
        "qty": 100.0, "unit": "g", "kcal": 160.0,
        "logged_at": datetime(2026, 10, 4, 12, 0, 0),
    })
    entry = food_queries._to_day_entry(flat)
    assert entry["logged_at"] == "2026-10-04T12:00:00"
    assert entry["nutrients_snapshot"]["kcal_per_100g"] == 160.0
    assert entry["name_snapshot"] == "Avocado"


@pytest.mark.unit
def test_to_recipe_summary_preserves_exact_fields():
    """_to_recipe_summary returns exactly recipe_id, title, category and times_prepared.
    """
    row = {"recipe_id": 3, "title": "Guacamole", "category": "Side",
           "times_prepared": 0, "is_deleted": False, "created_at": None}
    out = food_queries._to_recipe_summary(row)
    assert out == {"recipe_id": 3, "title": "Guacamole",
                   "category": "Side", "times_prepared": 0}


@pytest.mark.contract
def test_food_by_id_returns_row_or_404(monkeypatch):
    """GET /api/food/foods/{id} returns the food row with its serving fields, or 404
    when it does not exist.
    """
    row = _flat_food_row()
    row.update({"serving_size": 40.0, "serving_unit": "g",
                "serving_text": "1/2 cup"})

    def fake_get(food_id):
        return food_queries._to_food_row(row) if food_id == 1 else None

    monkeypatch.setattr(food_api_module, "get_food_by_id", fake_get)
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


# ---------------------------------------------------------------------------
# 010-005 T03: Recipe persistence v2 — aggregation, servings, CRUD routes.
# All mocked (monkeypatched SQL helpers); no DB, no network.
# ---------------------------------------------------------------------------

SAVE_PAYLOAD = {
    "title": "Guacamole", "category": "Side",
    "ingredients": [{"food_id": 1, "qty": 100.0, "unit": "g"}],
    "servings_count": 4, "serving_unit": "serving",
    "steps": [{"step_no": 1, "instruction": "Mash avocados",
               "timer_seconds": None,
               "foods": [{"food_id": 1, "qty": 100.0, "unit": "g"}]}],
}

SAMPLE_RECIPE_DETAIL = {
    "recipe_id": 3, "title": "Guacamole", "category": "Side",
    "times_prepared": 2, "food_id": 42,
    "servings_count": 4, "serving_unit": "serving",
    "ingredients": [{"food_id": 1, "qty": 100.0, "unit": "g",
                     "name": "Avocado"}],
    "steps": [{"step_no": 1, "instruction": "Mash", "timer_seconds": None,
               "foods": [{"food_id": 1, "qty": 100.0, "unit": "g",
                          "name": "Avocado"}]}],
}


def _food_fixture(**over):
    """Full-nutrient food row (all 8 present -> no accidental partial flags)."""
    base = {
        "food_id": 1, "name": "Oats", "source": "db",
        "kcal_per_100g": 380.0, "protein_g": 13.0, "fat_g": 6.5,
        "carbs_g": 66.0, "fiber_g": 10.0, "sugars_g": 0.7,
        "sodium_mg": 2.0, "caffeine_mg": 0.0,
        "serving_size": None, "serving_unit": None,
        "needs_review": False, "is_deleted": False,
    }
    base.update(over)
    return base


@pytest.mark.unit
def test_kcal_for_dynamic_recipe_serving_unit():
    """_kcal_for resolves a food's own serving unit through its serving size, falls back
    to the unit table on a mismatch, returns None when grams are unknown and raises
    ValueError for an unknown unit.
    """
    from pifitness.food.schemas import UNIT_TO_G_OR_ML
    k = food_queries._kcal_for
    assert k(3, "pancake", 200.0, serving_size=40.0, serving_unit="pancake",
             conversions=UNIT_TO_G_OR_ML) == 240.0
    assert k(3, "PANCAKE", 200.0, serving_size=40.0, serving_unit="pancake",
             conversions=UNIT_TO_G_OR_ML) == 240.0
    # mismatch falls through to the FDA label table
    assert k(1, "cup", 100.0, serving_size=30.0, serving_unit="slice",
             conversions=UNIT_TO_G_OR_ML) == 240.0
    # OQ-2 fallback: unknown grams -> partial (None), never a hard error
    assert k(2, "pancake", 200.0, serving_size=None, serving_unit="pancake",
             conversions=UNIT_TO_G_OR_ML) is None
    try:
        k(1, "pinch", 100.0, serving_unit="pancake",
          conversions=UNIT_TO_G_OR_ML)
    except ValueError:
        pass
    else:
        raise AssertionError("unknown unit must raise")


@pytest.mark.unit
def test_combine_ingredients_gram_weighted_math(monkeypatch):
    """_combine_ingredients computes per-100g nutrients weighted by the converted grams
    of each ingredient.
    """
    foods = {
        1: _food_fixture(food_id=1, name="Oats", kcal_per_100g=160.0,
                         protein_g=13.0),
        2: _food_fixture(food_id=2, name="Milk", kcal_per_100g=60.0,
                         protein_g=3.2),
    }
    monkeypatch.setattr(food_queries, "_fetch_foods", lambda ids: dict(foods))
    ings = [{"food_id": 1, "qty": 100.0, "unit": "g"},
            {"food_id": 2, "qty": 1.0, "unit": "cup"}]
    snap, needs_review, reason, total_g = food_queries._combine_ingredients(
        ings)
    # 100 g + 1 cup (240 g) = 340 g; per-100g = sum(snap*g) / sum(g)
    assert total_g == 340.0
    assert abs(snap["kcal_per_100g"] - (160.0 * 100 + 60.0 * 240) / 340.0) < 1e-6
    assert abs(snap["protein_g"] - (13.0 * 100 + 3.2 * 240) / 340.0) < 1e-6
    assert needs_review is False and reason == ""


@pytest.mark.unit
def test_combine_ingredients_recipe_as_ingredient(monkeypatch):
    """_combine_ingredients converts a recipe ingredient through its own serving unit
    and drops unconvertible amounts while flagging the result for review.
    """
    foods = {
        1: _food_fixture(food_id=1, name="Butter", kcal_per_100g=200.0),
        3: _food_fixture(food_id=3, name="Pancakes", source="recipe",
                         kcal_per_100g=250.0, serving_size=40.0,
                         serving_unit="pancake"),
    }
    monkeypatch.setattr(food_queries, "_fetch_foods", lambda ids: dict(foods))
    ings = [{"food_id": 1, "qty": 100.0, "unit": "g"},
            {"food_id": 3, "qty": 2.0, "unit": "pancake"}]
    snap, needs_review, reason, total_g = food_queries._combine_ingredients(
        ings)
    assert total_g == 180.0  # 100 g + 2 x 40 g-per-unit
    assert abs(snap["kcal_per_100g"] - (200.0 * 100 + 250.0 * 80) / 180.0) < 1e-6
    assert needs_review is False

    # unconvertible amount drops out, flags partial, contributes no grams
    snap2, nr2, reason2, total2 = food_queries._combine_ingredients(
        [{"food_id": 1, "qty": 1.0, "unit": "pinch"}])
    assert total2 is None and nr2 is True
    assert all(v is None for v in snap2.values())


@pytest.mark.contract
@pytest.mark.quarantine(
    reason="reads SQL parameters by position and counts placeholder groups",
    exit="rewrite_against_db",
)
def test_save_recipe_batches_fetch_and_ingredient_insert(monkeypatch):
    """save_recipe stores the recipe with grams-per-serving derived from the convertible
    ingredient grams and returns the recipe summary.
    """
    fetch_calls = []

    def fake_fetch(ids):
        fetch_calls.append(list(ids))
        return {
            1: _food_fixture(food_id=1, name="Oats", kcal_per_100g=160.0),
            2: _food_fixture(food_id=2, name="Milk", kcal_per_100g=60.0),
        }

    monkeypatch.setattr(food_queries, "_fetch_foods", fake_fetch)
    qec_calls = []
    monkeypatch.setattr(
        food_queries, "qec",
        lambda sql, params=None: qec_calls.append((sql, params)))
    insert_calls = []

    def fake_insert(sql, params=None):
        insert_calls.append((sql, params))
        if "INSERT INTO food.foods" in sql:
            return [{"food_id": 55}]
        if "INSERT INTO food.recipes" in sql:
            return [{"recipe_id": 7, "title": "Taco Night",
                     "category": "Side", "times_prepared": 0}]
        if "recipe_steps" in sql:
            return [{"step_id": 99}]
        return [{}]

    monkeypatch.setattr(food_queries, "sql_insert_returning", fake_insert)

    out = food_queries.save_recipe(
        title="Taco Night", category="Side",
        ingredients=[{"food_id": 1, "qty": 100.0, "unit": "g"},
                     {"food_id": 2, "qty": 1.0, "unit": "cup"}],
        servings_count=4, serving_unit="taco",
        steps=[{"step_no": 1, "instruction": "Mix", "timer_seconds": None,
                "foods": [{"food_id": 1, "qty": 50.0, "unit": "g"}]}],
    )
    assert set(out.keys()) == {"recipe_id", "title", "category",
                               "times_prepared"}
    assert fetch_calls == [[1, 2]]
    ing = [(s, p) for s, p in qec_calls if "recipe_ingredients" in s]
    assert len(ing) == 1  # exactly ONE multi-row insert for the list
    assert ing[0][0].count("(%s, %s, %s, %s)") == 2
    assert ing[0][1] == (7, 1, 100.0, "g", 7, 2, 1.0, "cup")
    food_sql, food_params = next(
        (s, p) for s, p in insert_calls if "INSERT INTO food.foods" in s)
    assert "serving_size" in food_sql
    # 340 convertible grams / 4 servings = 85.0 g-per-unit (OQ-2)
    assert food_params[9] == 85.0
    assert food_params[10] == "taco"
    rec_sql, rec_params = next(
        (s, p) for s, p in insert_calls if "INSERT INTO food.recipes" in s)
    assert "servings_count" in rec_sql and "serving_unit" in rec_sql
    assert rec_params == ("Taco Night", "Side", 55, 4, "taco")


@pytest.mark.contract
@pytest.mark.quarantine(
    reason="reads SQL parameters by position",
    exit="rewrite_against_db",
)
def test_save_recipe_unconvertible_yields_null_serving_size(monkeypatch):
    """save_recipe stores a null serving size and flags the food for review when no
    ingredient amount is convertible to grams.
    """
    monkeypatch.setattr(
        food_queries, "_fetch_foods",
        lambda ids: {1: _food_fixture(food_id=1, kcal_per_100g=160.0)})
    inserts = []

    def fake_insert(sql, params=None):
        inserts.append((sql, params))
        if "INSERT INTO food.foods" in sql:
            return [{"food_id": 55}]
        return [{"recipe_id": 7, "title": "Odd", "category": "Dessert",
                 "times_prepared": 0}]

    monkeypatch.setattr(food_queries, "sql_insert_returning", fake_insert)
    monkeypatch.setattr(food_queries, "qec", lambda *a, **k: None)
    food_queries.save_recipe(
        title="Odd", category="Dessert",
        ingredients=[{"food_id": 1, "qty": 1.0, "unit": "pinch"}],
        servings_count=2, serving_unit="slice", steps=[])
    food_params = inserts[0][1]
    assert food_params[9] is None    # grams-per-unit stays NULL
    assert food_params[10] == "slice"
    assert food_params[11] is True   # needs_review


@pytest.mark.regression
def test_save_recipe_rejects_step_food_outside_ingredient_list():
    """save_recipe raises ValueError, before touching the database, when a step uses a
    food that is not in the ingredient list.
    """
    try:
        food_queries.save_recipe(
            title="X", category="Dessert",
            ingredients=[{"food_id": 1, "qty": 1.0, "unit": "g"}],
            servings_count=2, serving_unit="slice",
            steps=[{"step_no": 1, "instruction": "s", "timer_seconds": None,
                    "foods": [{"food_id": 9, "qty": 1.0, "unit": "g"}]}],
        )
    except ValueError as e:
        assert "ingredient list" in str(e)
    else:
        raise AssertionError("step food outside the ingredient list must be "
                             "rejected before any DB touch")


@pytest.mark.contract
def test_get_recipe_detail_contract_shape(monkeypatch):
    """GET /api/food/recipes/{id} returns the RecipeDetail shape, or 404 when the recipe
    does not exist.
    """
    monkeypatch.setattr(food_api_module, "get_recipe",
                        lambda rid: dict(SAMPLE_RECIPE_DETAIL))
    client = TestClient(_make_app())
    res = client.get("/api/food/recipes/3")
    assert res.status_code == 200
    body = res.json()
    assert set(body.keys()) == {
        "recipe_id", "title", "category", "times_prepared", "food_id",
        "servings_count", "serving_unit", "ingredients", "steps"}
    assert body["servings_count"] == 4 and body["serving_unit"] == "serving"
    assert body["ingredients"][0]["name"] == "Avocado"
    assert body["steps"][0]["foods"][0]["food_id"] == 1

    monkeypatch.setattr(food_api_module, "get_recipe", lambda rid: None)
    assert client.get("/api/food/recipes/99").status_code == 404


@pytest.mark.contract
def test_put_recipe_passes_payload_and_404s(monkeypatch):
    """PUT /api/food/recipes/{id} forwards the request payload to the update helper and
    returns the recipe summary, or 404 when the recipe does not exist.
    """
    seen = {}

    def _upd(recipe_id, **kw):
        seen["recipe_id"] = recipe_id
        seen.update(kw)
        return dict(SAMPLE_RECIPE)

    monkeypatch.setattr(food_api_module, "update_recipe", _upd)
    client = TestClient(_make_app())
    res = client.put("/api/food/recipes/3", json=SAVE_PAYLOAD)
    assert res.status_code == 200
    assert seen["recipe_id"] == 3
    assert seen["servings_count"] == 4 and seen["serving_unit"] == "serving"
    # model_dump() adds the read-only `name: None` field; server derives names
    assert seen["ingredients"] == [
        dict(SAVE_PAYLOAD["ingredients"][0], name=None)]
    assert set(res.json().keys()) == {"recipe_id", "title", "category",
                                      "times_prepared"}

    monkeypatch.setattr(food_api_module, "update_recipe", lambda *a, **k: None)
    assert client.put("/api/food/recipes/9", json=SAVE_PAYLOAD).status_code == 404


@pytest.mark.contract
def test_delete_recipe_route_soft_delete_payload(monkeypatch):
    """DELETE /api/food/recipes/{id} returns success with the recipe_id and food_id, or
    404 when the recipe does not exist.
    """
    monkeypatch.setattr(
        food_api_module, "delete_recipe",
        lambda rid: {"success": True, "recipe_id": rid, "food_id": 42})
    client = TestClient(_make_app())
    res = client.delete("/api/food/recipes/3")
    assert res.status_code == 200
    assert res.json() == {"success": True, "recipe_id": 3, "food_id": 42}

    monkeypatch.setattr(food_api_module, "delete_recipe", lambda rid: None)
    assert client.delete("/api/food/recipes/9").status_code == 404


@pytest.mark.contract
def test_complete_recipe_route_increments(monkeypatch):
    """POST /api/food/recipes/{id}/complete returns the updated recipe summary, or 404
    when the recipe does not exist.
    """
    monkeypatch.setattr(food_api_module, "complete_recipe",
                        lambda rid: dict(SAMPLE_RECIPE, times_prepared=3))
    client = TestClient(_make_app())
    res = client.post("/api/food/recipes/3/complete")
    assert res.status_code == 200
    body = res.json()
    assert set(body.keys()) == {"recipe_id", "title", "category",
                                "times_prepared"}
    assert body["times_prepared"] == 3

    monkeypatch.setattr(food_api_module, "complete_recipe", lambda rid: None)
    assert client.post("/api/food/recipes/9/complete").status_code == 404


@pytest.mark.contract
@pytest.mark.quarantine(
    reason="asserts SQL fragments",
    exit="rewrite_against_db",
)
def test_complete_recipe_query_increments_and_clears_session(monkeypatch):
    """complete_recipe increments times_prepared, clears the recipe's cook session and
    returns None for a missing or deleted recipe.
    """
    calls = {"ret": [], "qec": []}

    def fake_insert(sql, params=None):
        calls["ret"].append((sql, params))
        return [{"recipe_id": 3, "title": "Guacamole", "category": "Side",
                 "times_prepared": 1}]

    monkeypatch.setattr(food_queries, "sql_insert_returning", fake_insert)
    monkeypatch.setattr(
        food_queries, "qec",
        lambda sql, params=None: calls["qec"].append((sql, params)))
    out = food_queries.complete_recipe(3)
    assert out == {"recipe_id": 3, "title": "Guacamole", "category": "Side",
                   "times_prepared": 1}
    assert "times_prepared = times_prepared + 1" in calls["ret"][0][0]
    assert "NOT is_deleted" in calls["ret"][0][0]
    assert any("recipe_cook_states" in s for s, _ in calls["qec"])

    monkeypatch.setattr(food_queries, "sql_insert_returning",
                        lambda *a, **k: None)
    assert food_queries.complete_recipe(99) is None


@pytest.mark.contract
@pytest.mark.quarantine(
    reason="asserts SQL fragments",
    exit="rewrite_against_db",
)
def test_delete_recipe_query_soft_deletes_both_rows(monkeypatch):
    """delete_recipe soft-deletes both the recipe and its food row, discards the cook
    session and returns None when the recipe is missing.
    """
    calls = {"reads": [], "qec": []}

    def fake_read(sql, params=None):
        calls["reads"].append(sql)
        return [{"recipe_id": 3, "food_id": 42}]

    monkeypatch.setattr(food_queries, "sql_to_dict", fake_read)
    monkeypatch.setattr(
        food_queries, "qec",
        lambda sql, params=None: calls["qec"].append((sql, params)))
    out = food_queries.delete_recipe(3)
    assert out == {"success": True, "recipe_id": 3, "food_id": 42}
    stmts = [s for s, _ in calls["qec"]]
    assert any("food.recipes" in s and "is_deleted = TRUE" in s for s in stmts)
    assert any("food.foods" in s and "is_deleted = TRUE" in s for s in stmts)
    assert any("recipe_cook_states" in s for s in stmts)

    monkeypatch.setattr(food_queries, "sql_to_dict", lambda *a, **k: [])
    assert food_queries.delete_recipe(99) is None


@pytest.mark.contract
def test_get_recipe_query_layer_shape(monkeypatch):
    """get_recipe assembles the RecipeDetail shape with ingredient and step-food names
    joined in, and returns None for a missing recipe.
    """
    def fake(sql, params=None):
        if "recipe_ingredients" in sql:
            return [{"food_id": 1, "qty": 100.0, "unit": "g",
                     "name": "Avocado"}]
        if "recipe_step_foods" in sql:
            return [{"step_no": 1, "food_id": 1, "qty": 100.0, "unit": "g",
                     "name": "Avocado"}]
        if "SELECT step_no, instruction, timer_seconds" in sql:
            return [{"step_no": 1, "instruction": "Mash",
                     "timer_seconds": None}]
        return [{"recipe_id": 3, "title": "Guacamole", "category": "Side",
                 "times_prepared": 2, "food_id": 42, "servings_count": 4,
                 "serving_unit": "serving"}]

    monkeypatch.setattr(food_queries, "sql_to_dict", fake)
    out = food_queries.get_recipe(3)
    assert set(out.keys()) == {
        "recipe_id", "title", "category", "times_prepared", "food_id",
        "servings_count", "serving_unit", "ingredients", "steps"}
    assert out["ingredients"] == [
        {"food_id": 1, "qty": 100.0, "unit": "g", "name": "Avocado"}]
    assert out["steps"][0]["foods"][0]["name"] == "Avocado"

    monkeypatch.setattr(food_queries, "sql_to_dict", lambda *a, **k: [])
    assert food_queries.get_recipe(99) is None


# ---------------------------------------------------------------------------
# 010-005 T04: Cook-state endpoints — one session per recipe, absolute
# deadlines. Round-trip test simulates the DB by json.loads-ing what the
# upsert stored (psycopg2 parses jsonb back to Python structures).
# ---------------------------------------------------------------------------

COOK_TIMERS = [{"step_no": 1, "ends_at": "2026-10-06T12:01:00+00:00"},
               {"step_no": 2, "ends_at": "2026-10-06T12:02:00+00:00"}]
COOK_FOODS = [{"step_no": 1, "food_id": 5, "unit": "g"}]


@pytest.mark.contract
@pytest.mark.quarantine(
    reason="round-trips through a fake database the test itself writes and asserts SQL text",
    exit="rewrite_against_db",
)
def test_cook_state_round_trip_preserves_deadlines(monkeypatch):
    """save_cook_state then get_cook_state returns the same timer deadlines and checked
    items, with updated_at as an ISO-8601 string.
    """
    stored = {}

    def fake_upsert(sql, params=None):
        assert "ON CONFLICT (recipe_id)" in sql          # one-row upsert
        assert sql.count("%s") == 4                      # fully parameterized
        assert "2026-10-06" not in sql                   # no literals inlined
        stored["steps"] = json.loads(params[1])
        stored["foods"] = json.loads(params[2])
        stored["timers"] = json.loads(params[3])
        return [{"recipe_id": params[0], "checked_steps": stored["steps"],
                 "checked_foods": stored["foods"], "timers": stored["timers"],
                 "updated_at": datetime(2026, 10, 6, 12, 0, 0)}]

    monkeypatch.setattr(food_queries, "sql_insert_returning", fake_upsert)
    monkeypatch.setattr(food_queries, "sql_to_dict",
                        lambda sql, params=None: [{"recipe_id": 3}])
    saved = food_queries.save_cook_state(
        3, checked_steps=[1, 2], checked_foods=COOK_FOODS, timers=COOK_TIMERS)
    assert saved["timers"] == COOK_TIMERS               # verbatim deadlines
    assert saved["checked_steps"] == [1, 2]
    assert saved["updated_at"] == "2026-10-06T12:00:00"

    # GET reads the stored row back (jsonb -> Python, as psycopg2 delivers)
    monkeypatch.setattr(
        food_queries, "sql_to_dict",
        lambda sql, params=None: [{"recipe_id": 3,
                                   "checked_steps": stored["steps"],
                                   "checked_foods": stored["foods"],
                                   "timers": stored["timers"],
                                   "updated_at":
                                       datetime(2026, 10, 6, 12, 0, 0)}])
    got = food_queries.get_cook_state(3)
    assert got["timers"] == COOK_TIMERS                 # round-trip identical
    assert len(got["timers"]) == 2                       # concurrent timers intact
    assert got["checked_foods"] == COOK_FOODS
    assert got["checked_steps"] == [1, 2]
    assert got["updated_at"] == "2026-10-06T12:00:00"


@pytest.mark.contract
def test_get_cook_state_empty_session(monkeypatch):
    """get_cook_state returns empty lists and a null updated_at when no session is
    stored.
    """
    monkeypatch.setattr(food_queries, "sql_to_dict", lambda *a, **k: [])
    assert food_queries.get_cook_state(3) == {
        "recipe_id": 3, "checked_steps": [], "checked_foods": [],
        "timers": [], "updated_at": None}


@pytest.mark.contract
def test_save_cook_state_missing_recipe_404_path(monkeypatch):
    """save_cook_state returns None when the recipe is missing or soft-deleted so the
    route can answer 404.
    """
    monkeypatch.setattr(food_queries, "sql_to_dict", lambda *a, **k: [])
    assert food_queries.save_cook_state(99, [], [], []) is None


@pytest.mark.contract
def test_cook_routes_round_trip_and_validation(monkeypatch):
    """PUT and GET /api/food/recipes/{id}/cook return the stored timers unchanged, PUT
    rejects a non-ISO-8601 timer instant with 400 before touching the database, and PUT
    returns 404 for a missing recipe.
    """
    state = {"recipe_id": 3, "checked_steps": [1, 2],
             "checked_foods": list(COOK_FOODS), "timers": list(COOK_TIMERS),
             "updated_at": "2026-10-06T12:00:00"}

    def fake_save(recipe_id, **kw):
        state["recipe_id"] = recipe_id
        state["checked_steps"] = kw["checked_steps"]
        state["checked_foods"] = kw["checked_foods"]
        state["timers"] = kw["timers"]
        return dict(state)

    monkeypatch.setattr(food_api_module, "save_cook_state", fake_save)
    monkeypatch.setattr(food_api_module, "get_cook_state",
                        lambda rid: dict(state, recipe_id=rid))
    client = TestClient(_make_app())

    res = client.put("/api/food/recipes/3/cook", json={
        "checked_steps": [1, 2], "checked_foods": COOK_FOODS,
        "timers": COOK_TIMERS})
    assert res.status_code == 200
    body = res.json()
    assert set(body.keys()) == {"recipe_id", "checked_steps",
                                "checked_foods", "timers", "updated_at"}
    assert body["timers"] == COOK_TIMERS        # both deadlines unchanged
    assert body["checked_steps"] == [1, 2]

    got = client.get("/api/food/recipes/3/cook")
    assert got.status_code == 200
    assert got.json()["timers"] == COOK_TIMERS  # get restores identical deadlines
    assert len(got.json()["timers"]) == 2        # concurrent-timer shape

    # bad instant -> 400 before any DB touch
    monkeypatch.setattr(
        food_api_module, "save_cook_state",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not hit DB")))
    res = client.put("/api/food/recipes/3/cook", json={
        "checked_steps": [], "checked_foods": [],
        "timers": [{"step_no": 1, "ends_at": "not-a-time"}]})
    assert res.status_code == 400
    assert "ISO-8601" in res.json()["detail"]

    # missing recipe -> 404
    monkeypatch.setattr(food_api_module, "save_cook_state", lambda *a, **k: None)
    res = client.put("/api/food/recipes/9/cook", json={
        "checked_steps": [], "checked_foods": [], "timers": []})
    assert res.status_code == 404


@pytest.mark.contract
def test_delete_cook_route_clears_session(monkeypatch):
    """DELETE /api/food/recipes/{id}/cook returns success true and clears that recipe's
    cook session.
    """
    calls = []
    monkeypatch.setattr(food_api_module, "clear_cook_state",
                        lambda rid: calls.append(rid) or True)
    client = TestClient(_make_app())
    res = client.delete("/api/food/recipes/3/cook")
    assert res.status_code == 200
    assert res.json() == {"success": True}
    assert calls == [3]


@pytest.mark.contract
def test_get_cook_route_empty_session_shape(monkeypatch):
    """GET /api/food/recipes/{id}/cook returns empty lists and a null updated_at when no
    session exists.
    """
    monkeypatch.setattr(food_api_module, "get_cook_state",
                        lambda rid: {"recipe_id": rid, "checked_steps": [],
                                     "checked_foods": [], "timers": [],
                                     "updated_at": None})
    client = TestClient(_make_app())
    res = client.get("/api/food/recipes/7/cook")
    assert res.status_code == 200
    assert res.json() == {"recipe_id": 7, "checked_steps": [],
                          "checked_foods": [], "timers": [],
                          "updated_at": None}


# ---------------------------------------------------------------------------
# 010-005 T12: persist-only endpoint (OQ-3) — recipe-create picks materialize
# an API hit into food.foods WITHOUT any diary write. Mocked; no DB.
# ---------------------------------------------------------------------------

def _persist_usda_body():
    return {"usda_payload": {
        "provider": "usda", "fdcId": 55, "name": "Rolled Oats",
        "nutrients_raw": {"dataType": "SR Legacy", "foodNutrients": []}}}


@pytest.mark.contract
def test_persist_route_returns_food_id_and_never_writes_diary(monkeypatch):
    """POST /api/food/persist returns 201 with the persisted food_id, passes the
    normalizer label and serving fields through, and never writes a diary entry.
    """
    seen = {}

    def fake_persist(*args, **kwargs):
        seen["args"] = args
        seen["kwargs"] = kwargs
        return {"food_id": 42}

    monkeypatch.setattr(food_api_module, "persist_api_food", fake_persist)
    monkeypatch.setattr(
        food_api_module, "log_diary_entry",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("persist-only must not write a diary entry")))
    client = TestClient(_make_app())
    res = client.post("/api/food/persist", json=_persist_usda_body())
    assert res.status_code == 201
    assert res.json() == {"food_id": 42}
    # Shared normalizer ran (survey/legacy path) with serving fields carried.
    assert seen["args"][0] == "usda"
    assert seen["args"][1] == "Rolled Oats"
    assert seen["args"][2] == 55
    assert seen["kwargs"]["normalized_from"] == "usda_legacy"
    assert seen["kwargs"]["serving_size"] is None


@pytest.mark.contract
def test_persist_route_off_payload_carries_serving_and_dedupe_id(monkeypatch):
    """POST /api/food/persist for an Open Food Facts payload forwards the feed serving
    and returns the existing food_id when the barcode is already saved.
    """
    seen = {}

    def fake_persist(*args, **kwargs):
        seen["args"] = args
        seen["kwargs"] = kwargs
        # persist_api_food returns the EXISTING row when the barcode matches.
        return {"food_id": 7}

    monkeypatch.setattr(food_api_module, "persist_api_food", fake_persist)
    client = TestClient(_make_app())
    res = client.post("/api/food/persist", json={"off_payload": {
        "provider": "off", "barcode": "12345", "name": "Bar",
        "nutrients_raw": {"nutriments": {"energy-kcal_100g": 100.0},
                          "serving_quantity": 40,
                          "serving_quantity_unit": "g"}}})
    assert res.status_code == 201
    assert res.json() == {"food_id": 7}
    assert seen["args"][0] == "off"
    assert seen["args"][3] == "12345"
    assert seen["kwargs"]["normalized_from"] == "off"
    assert seen["kwargs"]["serving_size"] == 40.0
    assert seen["kwargs"]["serving_unit"] == "g"


@pytest.mark.contract
def test_persist_route_requires_payload(monkeypatch):
    """POST /api/food/persist returns 400 when neither a USDA nor an Open Food Facts
    payload is supplied.
    """
    client = TestClient(_make_app())
    assert client.post("/api/food/persist", json={}).status_code == 400
