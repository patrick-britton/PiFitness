"""Diag: inspect saved Coffee row + live milk ranking (human-reported issues)."""
import sys
sys.path.insert(0, ".")
from backend_functions.database_functions import sql_to_dict

print("=== Coffee rows ===")
for r in sql_to_dict(
    "SELECT food_id, name, kcal_per_100g, protein_g, fat_g, carbs_g,"
    " fiber_g, sugars_g, sodium_mg, caffeine_mg, needs_review,"
    " review_reason, source, quality_score, fdc_id, barcode"
    " FROM food.foods WHERE name ILIKE '%%coffee%%' AND NOT is_deleted"
):
    print(r)

print("=== In vw_usual_foods? ===")
print(sql_to_dict(
    "SELECT food_id, name, times_eaten FROM food.vw_usual_foods"
    " WHERE name ILIKE '%%coffee%%' LIMIT 5"
))
