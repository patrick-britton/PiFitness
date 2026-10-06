"""Verify Bug 010-003-T06-1 fix: search_saved_foods SQL must escape LIKE % as %%."""
import sys
sys.path.insert(0, ".")
from unittest.mock import patch
from backend_functions.queries import food_queries as fq

captured = {}
def fake_sql_to_dict(q, params=None):
    captured["q"] = q
    captured["params"] = params
    return []

with patch.object(fq, "sql_to_dict", side_effect=fake_sql_to_dict):
    fq.search_saved_foods("drip coffee", 30)

q = captured["q"]
print("SQL:", " ".join(q.split()))
print("params:", captured["params"])
assert "ILIKE '%%'" in q, "literal % wildcards must be escaped as %% for psycopg2"
assert "ILIKE '%'" not in q.replace("ILIKE '%%'", ""), "no bare % may remain"
assert captured["params"] == ("drip coffee", 30)
# psycopg2 client-side check: bare % breaks placeholder parsing; %% parses clean
import re
placeholders = len(re.findall(r"(?<!%)%s", q))
percents = q.count("%")
print("placeholders=%s total_percent_chars=%s" % (placeholders, percents))
assert placeholders == 2, placeholders  # one for ILIKE operand, one for LIMIT
print("PASS: escaped SQL parses to 2 placeholders; bare-% 500 eliminated")
