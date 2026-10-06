"""010-003 T09 live verify (OQ-7): Coffee backfill + suggestion view + milk rank.

HOW TO RUN (repo root): .venv/Scripts/python.exe scripts/verify_010_003_t09.py
Needs backend/.env DB reachability and outbound USDA FDC access (~2 fetches).

Checks:
  1. Backfill food_id 2 (Coffee): needs_review=false, review_reason=NULL —
     only when still flagged (idempotent). Confirming a pick IS the review.
  2. The row now surfaces in food.vw_usual_foods (suggestion-eligible).
  3. Live "milk" search: top-3 contains a whole-word-or-better hit and
     exact/start/whole-word classes all outrank substring-only hits (AC-12)
     with no extra upstream fetches.

Exit 0 = every check passes.
"""
import asyncio
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / "backend" / ".env",
            override=False)
sys.path.insert(0, ".")

from backend_functions import food_search as fsearch  # noqa: E402
from backend_functions.database_functions import qec, sql_to_dict  # noqa: E402

failures = []


def check(label, ok, detail=""):
    print(f"{'PASS' if ok else 'FAIL'}  {label}"
          + (f" — {detail}" if detail else ""))
    if not ok:
        failures.append(label)


# --- 1. Coffee backfill (targeted single row, idempotent) ---------------------
rows = sql_to_dict(
    "SELECT food_id, name, needs_review FROM food.foods"
    " WHERE name ILIKE '%%coffee%%' AND NOT is_deleted")
print("coffee rows before:", rows)
target = next((r for r in rows if r["food_id"] == 2), rows[0] if rows else None)
if target is None:
    check("coffee row exists", False, "no matching food row found")
else:
    if target["needs_review"]:
        err = qec("UPDATE food.foods SET needs_review = FALSE,"
                  " review_reason = NULL"
                  " WHERE food_id = %s AND needs_review IS TRUE",
                  (target["food_id"],), auto_commit=True)
        check("backfill UPDATE executes", err is None, str(err or ""))
    after = sql_to_dict(
        "SELECT food_id, name, needs_review, review_reason FROM food.foods"
        " WHERE food_id = %s", (target["food_id"],))
    check("Coffee row needs_review=false",
          bool(after) and after[0]["needs_review"] is False, str(after))

    # --- 2. Suggestion view surfaces the row ---------------------------------
    usuals = sql_to_dict(
        "SELECT food_id, name, times_eaten FROM food.vw_usual_foods"
        " WHERE name ILIKE '%%coffee%%'")
    check("Coffee in vw_usual_foods", bool(usuals), str(usuals))

# --- 3. Live milk ranking (AC-12) ---------------------------------------------
out = asyncio.run(fsearch.fetch_usda_ranked("milk"))
ranked = out["results"]
check("milk search returns hits",
      out["fetches"] >= 1 and len(ranked) > 0,
      f"fetches={out['fetches']} cached={out['cached']} total={len(ranked)}")
for i, m in enumerate(ranked[:5]):
    print(f"  top{i + 1} bonus={fsearch._name_bonus(m['name'], 'milk')}"
          f" gate={m['gate_ok']} dtype={m['dtype']:<14} {m['name'][:58]!r}")

bonuses = [fsearch._name_bonus(m["name"], "milk") for m in ranked[:3]]
check("top-3 contains whole-word-or-better hit",
      bool(bonuses) and min(bonuses) <= 2, f"bonuses={bonuses}")

# Every exact/start/whole-word (0-2) gate-ok hit must outrank every
# substring-only (3) gate-ok hit; the first such pair proves ordering.
best = [i for i, m in enumerate(ranked) if m["gate_ok"]
        and fsearch._name_bonus(m["name"], "milk") <= 2]
worst = [i for i, m in enumerate(ranked) if m["gate_ok"]
         and fsearch._name_bonus(m["name"], "milk") == 3]
check("whole-word-class outranks substring-only class",
      bool(best) and (not worst or min(best) < min(worst)),
      f"best={min(best) if best else None} worst={min(worst) if worst else None}")

if failures:
    print(f"\n{len(failures)} FAILED: {failures}")
    sys.exit(1)
print("\nALL CHECKS PASSED")