# BLOCKERS — test_food_api_contract.py (Phase 3 food-contract repair)

Scope: `tests/test_food_api_contract.py` after the approved Phase 3 fixes.

## Fixes applied (did not block)

- `tests/test_food_api_contract.py:21` — `include_router(food_api)` →
  `include_router(food_api.router)`.
- `tests/test_food_api_contract.py:246,373` — stale
  `backend_functions.schemas.food_contract_schemas` → `pifitness.food.schemas`.
- `backend_functions/queries/food_queries.py` (3 sites) — stale schema import →
  `pifitness.food.schemas` (CASE A; `router.py` imports
  `backend_functions.queries.food_queries`, so this is the canonical food_queries
  module for the router).

## Schema-import failures (resolved, did not block)

Fixed by the CASE A import change and now pass:

- test_combine_ingredients_gram_weighted_math
- test_combine_ingredients_recipe_as_ingredient
- test_save_recipe_batches_fetch_and_ingredient_insert
- test_save_recipe_unconvertible_yields_null_serving_size

## BLOCKED failures (Task 4 — non-schema, out of scope)

Per Amendment 3, Task 4's triage covers only the schema-import case
(`backend_functions.schemas.food_contract_schemas`). These 16 failures do NOT
reference that module in their tracebacks, so they are not actionable here and
are documented for a follow-up slice.

| # | test | failure (state) |
|---|------|-----------------|
| 1 | test_get_usuals_returns_usual_food_shapes | AssertionError: `assert (True and 13 == 1)` |
| 2 | test_get_diary_returns_day_entry_array | IndexError: list index out of range |
| 3 | test_get_foods_returns_food_list_response | assert 500 == 200 |
| 4 | test_post_diary_returns_day_entry | assert 500 == 201 |
| 5 | test_post_recipes_returns_recipe_summary | assert 500 == 201 |
| 6 | test_post_diary_rejects_deleted_food | assert 500 == 400 |
| 7 | test_diary_stats_shape | AssertionError: today_kcal / today_kcal_max mismatch |
| 8 | test_patch_entry_moves_day | assert 500 == 200 |
| 9 | test_food_by_id_returns_row_or_404 | assert 404 == 200 |
| 10 | test_get_recipe_detail_contract_shape | IndexError: list index out of range |
| 11 | test_put_recipe_passes_payload_and_404s | assert 500 == 200 |
| 12 | test_delete_recipe_route_soft_delete_payload | AssertionError: assert 200 == 404 |
| 13 | test_cook_routes_round_trip_and_validation | assert 500 == 404 |
| 14 | test_delete_cook_route_clears_session | assert [] == [3] |
| 15 | test_persist_route_returns_food_id_and_never_writes_diary | AssertionError: `{'food_id': 83} == {'food_id': 42}` |
| 16 | test_persist_route_off_payload_carries_serving_and_dedupe_id | AssertionError: `{'food_id': 80} == {'food_id': 7}` |

Source files involved (2 per failure):

- `tests/test_food_api_contract.py` — the test file
- `backend/pifitness/food/router.py` — route execution under test

(Note: full per-test tracebacks were captured in the run log
`$env:TEMP\food_contract_failures.txt`; only the failing assertion is shown
above to keep this file terse.)

## Summary

- Task 3 result: **20 failed, 22 passed** in `test_food_api_contract.py`.
- 4 failures blocked by Amendment 3 (non-schema).
- Remaining 16 fixed/blocked items require a separate slice beyond this commit.
