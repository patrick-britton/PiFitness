"""Diag: why 'almonds' search shows kcal 634-2168 per 100g (human report).

Read-only: fetches the live USDA ranking and prints, per hit, the raw Energy
entries (kcal vs kJ), the stated serving, and what our normalizer produced.
HOW TO RUN (repo root): .venv/Scripts/python.exe scripts/diag_almonds_rank.py
"""
import asyncio
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / "backend" / ".env",
            override=False)
sys.path.insert(0, ".")

from backend_functions import food_search as fsearch  # noqa: E402
from backend_functions.database_functions import sql_to_dict  # noqa: E402

print("=== Saved almond rows (food.foods) ===")
for r in sql_to_dict(
    "SELECT food_id, name, kcal_per_100g, serving_size, serving_unit,"
    " source, normalized_from FROM food.foods"
    " WHERE name ILIKE '%%almond%%' AND NOT is_deleted"
):
    print(" ", r)

print("=== Persisted rows exceeding the physical bound (kcal/100g > 902) ===")
print(" ", sql_to_dict(
    "SELECT count(*) AS n FROM food.foods WHERE kcal_per_100g > 902"))
for r in sql_to_dict(
    "SELECT food_id, name, kcal_per_100g, source, normalized_from"
    " FROM food.foods WHERE kcal_per_100g > 902 AND NOT is_deleted"
    " ORDER BY kcal_per_100g DESC LIMIT 20"
):
    print(" ", r)

out = asyncio.run(fsearch.fetch_usda_ranked("almonds"))
print(f"\n=== Live USDA ranking for 'almonds' (fetches={out['fetches']},"
      f" cached={out['cached']}, total={len(out['results'])}) ===")
for i, m in enumerate(out["results"][:12]):
    raw = m.get("raw") or {}
    energies = [(n.get("nutrientName"), n.get("unitName"), n.get("value"))
                for n in (raw.get("foodNutrients") or [])
                if str(n.get("nutrientName") or "").strip().lower() == "energy"]
    print(f"{i:2d} dtype={m['dtype']:<14} gate={m['gate_ok']}"
          f" pen={m['penalty']} present={m['present']}"
          f" kcal100={m['nuts'].get('kcal_per_100g')}")
    print(f"     servingSize={raw.get('servingSize')!r}"
          f" unit={raw.get('servingSizeUnit')!r}"
          f" dataSource={raw.get('dataSource')!r}"
          f" name={m['name'][:60]!r}")
    print(f"     energy entries: {energies}")
    if i == 2:
        print(f"     raw keys: {sorted(raw.keys())}")
        ln = raw.get("labelNutrients")
        if isinstance(ln, dict):
            print(f"     labelNutrients.energy: {ln.get('energy')}")

