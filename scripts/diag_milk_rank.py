"""Diag: live milk ranking — what USDA returns vs what we show (human report)."""
import sys, asyncio
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / "backend" / ".env",
             override=False)
sys.path.insert(0, ".")
from backend_functions import food_search as fsearch

out = asyncio.run(fsearch.fetch_usda_ranked("milk"))
print("fetches:", out["fetches"], "cached:", out["cached"])
print("total ranked:", len(out["results"]))
for i, m in enumerate(out["results"][:15]):
    print(f"{i:2d} gate={m['gate_ok']} dtype={m['dtype']:<15} present={m['present']}"
          f" pen={m['penalty']} score={m['score']} name={m['name'][:60]!r}")
