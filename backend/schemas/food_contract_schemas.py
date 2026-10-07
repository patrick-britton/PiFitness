"""
Food Module Contract (010-002 T01, frozen).

Mirrors frontend/pifitness/src/lib/types/food-contract.ts.
Module map: exactly 5 tabs (Diary, Recipe Box, Food Database, Tri-tip
Timer, API Explorer); recipe creation is a Recipe Box mode.
Snapshot semantics (OQ-3/OQ-6): diary entries freeze name + nutrients.
Soft-delete (OQ-4): is_deleted hidden from pickers, restore via Database.
DB: existing `food` schema alongside tri-tip tables.
010-003 (Diary): DiaryStats, DiaryUpdateRequest, CombinedSearchResult,
LogApiEntryRequest; FDA-label unit table; device-local day; entry-moves-day;
tiered ranking with completeness gate + plausibility penalties.
010-005 (Recipe Box): RecipeIngredient, RecipeStepDetail, RecipeDetail,
CookState (+Update); SaveRecipeRequest += ingredients/servings; category
set Breakfast|Lunch|Dinner|Side|Dessert|Drink ('Appetizer' removed, OQ-1);
grams-per-unit derived into food.foods.serving_size (OQ-2).
"""

from typing import List, Literal, Optional
from pydantic import BaseModel, Field


FoodSource = Literal["api", "db", "recipe"]
RecipeCategory = Literal[
    "Breakfast", "Lunch", "Dinner", "Side", "Dessert", "Drink"
]


class NutrientSnapshot(BaseModel):
    """Canonical per-100g nutrient snapshot, frozen at write time.

    All fields NULL-able: None = source did not report it (never 0).
    """

    kcal_per_100g: Optional[float] = Field(None, ge=0)
    protein_g: Optional[float] = Field(None, ge=0)
    fat_g: Optional[float] = Field(None, ge=0)
    carbs_g: Optional[float] = Field(None, ge=0)
    fiber_g: Optional[float] = Field(None, ge=0)
    sugars_g: Optional[float] = Field(None, ge=0)
    sodium_mg: Optional[float] = Field(None, ge=0)
    caffeine_mg: Optional[float] = Field(None, ge=0)


class UsualFood(BaseModel):
    food_id: Optional[int] = None
    name: str
    source: FoodSource
    kcal_per_100g: Optional[float] = Field(None, ge=0)
    quality_score: float = Field(..., ge=0)
    times_eaten: int = Field(..., ge=0)
    hour_weight: float = Field(..., ge=0)
    # 010-003: most-logged qty + unit for suggestion cards (NULL = unknown).
    mode_qty: Optional[float] = Field(None, gt=0)
    mode_unit: Optional[str] = None


class DayEntry(BaseModel):
    entry_id: int
    food_id: Optional[int] = None
    name_snapshot: str
    nutrients_snapshot: NutrientSnapshot
    is_partial: bool = False
    qty: float = Field(..., gt=0)
    unit: str
    kcal: Optional[float] = Field(None, ge=0)
    logged_at: str


class RecipeSummary(BaseModel):
    recipe_id: int
    title: str
    category: RecipeCategory
    times_prepared: int = Field(..., ge=0)


class FoodRow(BaseModel):
    food_id: int
    name: str
    nutrients: NutrientSnapshot
    # T10 (OQ-8): feed standard serving for popup defaults/panels
    # (NULL = the row carries no usable serving).
    serving_size: Optional[float] = Field(None, gt=0)
    serving_unit: Optional[str] = None
    serving_text: Optional[str] = None
    is_deleted: bool = False
    deleted_at: Optional[str] = None


class FoodListResponse(BaseModel):
    data: List[FoodRow] = Field(default_factory=list)
    next_cursor: Optional[str] = None


class LogEntryRequest(BaseModel):
    food_id: Optional[int] = None
    qty: float = Field(..., gt=0)
    unit: str
    logged_at: Optional[str] = None
    # 010-003 tuning: optional pick telemetry echoed from the search result
    # the user chose (OQ-4); backend logs it via log_pick on successful write.
    query: Optional[str] = None
    rank: Optional[int] = Field(None, ge=0)
    result_source: Optional[str] = None


class RecipeStepFood(BaseModel):
    food_id: Optional[int] = None
    qty: float = Field(..., gt=0)
    unit: str


class RecipeStep(BaseModel):
    step_no: int = Field(..., ge=1)
    instruction: str
    timer_seconds: Optional[int] = Field(None, ge=0)
    foods: List[RecipeStepFood] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# 010-005 (Recipe Box) additions — mirrors food-contract.ts.
# ---------------------------------------------------------------------------

class RecipeIngredient(BaseModel):
    """One line of a recipe's predefined ingredient list.

    `name` is read-side only (GET detail joins food.foods once); writes
    carry food_id + qty + unit and the server derives the name.
    """

    food_id: int
    qty: float = Field(..., gt=0)
    unit: str
    name: Optional[str] = None


class RecipeStepDetail(BaseModel):
    """A stored step as served to the cook/create screens (with names)."""

    step_no: int = Field(..., ge=1)
    instruction: str
    timer_seconds: Optional[int] = Field(None, ge=0)
    foods: List[RecipeIngredient] = Field(default_factory=list)


class RecipeDetail(BaseModel):
    """GET /api/food/recipes/{id} — full recipe for create-view/cook mode.

    servings_count/serving_unit are NULL only for rows predating the
    010-005 DDL; every new save requires them.
    """

    recipe_id: int
    title: str
    category: RecipeCategory
    times_prepared: int = Field(..., ge=0)
    food_id: Optional[int] = None
    servings_count: Optional[int] = Field(None, gt=0)
    serving_unit: Optional[str] = None
    ingredients: List[RecipeIngredient] = Field(default_factory=list)
    steps: List[RecipeStepDetail] = Field(default_factory=list)


class CookFoodCheck(BaseModel):
    """Identity of one checked ingredient line inside a step."""

    step_no: int = Field(..., ge=1)
    food_id: int
    unit: str


class CookTimer(BaseModel):
    """One running timer as an absolute deadline (survives sleep/off)."""

    step_no: int = Field(..., ge=1)
    ends_at: str  # ISO-8601 instant


class CookStateUpdate(BaseModel):
    """PUT body for /recipes/{id}/cook — full replace of the session."""

    checked_steps: List[int] = Field(default_factory=list)
    checked_foods: List[CookFoodCheck] = Field(default_factory=list)
    timers: List[CookTimer] = Field(default_factory=list)


class CookState(CookStateUpdate):
    """GET /api/food/recipes/{id}/cook — empty lists = no active session."""

    recipe_id: int
    updated_at: Optional[str] = None


class SaveRecipeRequest(BaseModel):
    title: str
    category: RecipeCategory
    ingredients: List[RecipeIngredient] = Field(min_length=1)
    servings_count: int = Field(..., gt=0)
    serving_unit: str = Field(..., min_length=1)
    steps: List[RecipeStep] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# 010-003 (Diary) additions — mirrors food-contract.ts.
# ---------------------------------------------------------------------------

# FDA nutrition-label conversion table (OQ-1, authoritative for kcal math).
# kcal = kcal_per_100g * grams_equiv / 100. `serving` resolves per-row from
# serving_size (grams); exact US customary kept as reference only.
UNIT_TO_G_OR_ML = {
    "g": 1.0,
    "ml": 1.0,
    "oz": 28.0,
    "tsp": 5.0,
    "tbsp": 15.0,
    "fl oz": 30.0,
    "cup": 240.0,
}

DataQuality = Literal["High", "Medium", "Low"]


class DiaryStats(BaseModel):
    """Three-bar header (device-local day treated literally; NULL = no data)."""

    today_kcal: Optional[float] = Field(None, ge=0)
    last_24h_kcal: Optional[float] = Field(None, ge=0)
    avg_7d_kcal: Optional[float] = Field(None, ge=0)
    scale_max: Optional[float] = Field(None, ge=0)


class DiaryUpdateRequest(BaseModel):
    """PATCH body — a new logged_at moves the entry to that day (OQ-5)."""

    qty: Optional[float] = Field(None, gt=0)
    unit: Optional[str] = None
    logged_at: Optional[str] = None


class CombinedSearchResult(BaseModel):
    kind: Literal["db-food", "db-recipe", "usda", "off"]
    food_id: Optional[int] = None
    name: str
    mode_qty: Optional[float] = Field(None, gt=0)
    mode_unit: Optional[str] = None
    kcal_preview: Optional[float] = Field(None, ge=0)
    data_quality: DataQuality
    rank: int = Field(..., ge=0)
    result_source: str
    usda_score: Optional[float] = None
    # T08 (OQ-6): natural keys + raw upstream hit body carried on the hit so
    # the confirm popup can persist without a re-fetch, and combined_search
    # can suppress already-saved API duplicates. nutrients_raw is the raw
    # USDA hit (foodNutrients + servingSize) or raw OFF product, matching
    # ApiFoodPayload — never a flattened per-100g map (the normalizers read
    # the upstream shapes).
    fdcId: Optional[int] = None
    barcode: Optional[str] = None
    nutrients_raw: Optional[dict] = None
    # T10 (OQ-8): normalized per-100g nutrients + the feed's standard
    # serving so the confirm popup can default + show exactly what will be
    # saved without a re-fetch (db rows from foods columns, usda from the
    # ranking mapper; null = unknown/failed-closed).
    nutrients: Optional[NutrientSnapshot] = None
    serving_size: Optional[float] = Field(None, gt=0)
    serving_unit: Optional[str] = None
    serving_text: Optional[str] = None
    # 010-004: ranked-search fields (all optional/additive; backend populates,
    # frontend derives facet controls client-side from food_category +
    # serving_unit + serving_unit_facet). Sort: saved-first, then match_band
    # desc, is_single_ingredient, quality_percent desc, ingredient_count asc.
    # kcal gate: computed = 4P + 4C + 9F + 7A (alcohol 0 when absent; gate
    # requires kcal+P+F+C); exclude when
    # abs(reported - computed) > max(0.15 * computed, 20).
    # quality_percent = round(100 * present / 10) over kcal, protein, fat,
    # carbs, fiber, sugars, sodium, caffeine, serving_size, serving unit
    # (each by presence; 0 counts, null does not).
    quality_percent: Optional[int] = Field(None, ge=0, le=100)
    match_pct: Optional[int] = Field(None, ge=0, le=100)
    match_band: Optional[int] = Field(None, ge=0, le=10)
    is_single_ingredient: Optional[bool] = None
    ingredient_count: Optional[int] = Field(None, ge=0)
    kcal_delta_pct: Optional[float] = None
    excluded_kcal_mismatch: Optional[bool] = None
    food_category: Optional[str] = None
    serving_unit_facet: Optional[str] = None


class ApiFoodPayload(BaseModel):
    provider: Literal["usda", "off"]
    fdcId: Optional[int] = None
    barcode: Optional[str] = None
    name: str
    nutrients_raw: Optional[dict] = None


class LogApiEntryRequest(BaseModel):
    usda_payload: Optional[ApiFoodPayload] = None
    off_payload: Optional[ApiFoodPayload] = None
    qty: float = Field(..., gt=0)
    unit: str
    logged_at: Optional[str] = None
    # 010-003 tuning: optional pick telemetry echoed from the search result.
    query: Optional[str] = None
    rank: Optional[int] = Field(None, ge=0)
    result_source: Optional[str] = None


# ---------------------------------------------------------------------------
# 010-005 T12 (OQ-3) additions — persist-only pick; mirrors food-contract.ts.
# ---------------------------------------------------------------------------


class PersistApiFoodRequest(BaseModel):
    """Payload half of LogApiEntryRequest — persist WITHOUT a diary write."""
    usda_payload: Optional[ApiFoodPayload] = None
    off_payload: Optional[ApiFoodPayload] = None


class PersistApiFoodResponse(BaseModel):
    food_id: int
