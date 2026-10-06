"""010-003 T04 — backend search/suggest/persist-on-log (FastAPI + queries).

USDA candidate fetch (2 parallel + fallback) + OQ-4 tiered ranking, a ~1-day
normalized-query cache, and the pick-logging tuning hook. Payloads are trimmed
server-side; no Pandas, no raw-body forwarding.
"""

import logging
import re
import time
from typing import Any, Dict, List, Optional, Sequence

import httpx

from backend_functions.service_logins import load_api_credentials

logger = logging.getLogger("food_search")

_FDC_SEARCH_URL = "https://api.nal.usda.gov/fdc/v1/foods/search"
_TIMEOUT_S = 12.0

# Fetch budget: 2 parallel fetches per search, +1 fallback when branded < 5.
_GENERICS_PAGE = 25
_BRANDED_PAGE = 50
_FALLBACK_PAGE = 25

# Normalized-query cache (~1 day TTL, in-process, bounded).
_CACHE_TTL_S = 24 * 3600
_CACHE_MAX = 200
_cache: Dict[str, Any] = {}
_cache_order: List[str] = []


def _norm_query(q: str) -> str:
    return re.sub(r"\s+", " ", (q or "").strip().lower())


def _cache_get(key: str) -> Optional[Any]:
    hit = _cache.get(key)
    if not hit:
        return None
    if time.time() - hit["at"] > _CACHE_TTL_S:
        _cache.pop(key, None)
        return None
    return hit["value"]


def _cache_put(key: str, value: Any) -> None:
    if key in _cache:
        _cache[key] = {"at": time.time(), "value": value}
        return
    if len(_cache_order) >= _CACHE_MAX:
        _cache.pop(_cache_order.pop(0), None)
    _cache[key] = {"at": time.time(), "value": value}
    _cache_order.append(key)


def _usda_key() -> Optional[str]:
    try:
        creds = load_api_credentials("USDA")
    except Exception:
        return None
    if not isinstance(creds, dict):
        return None
    return creds.get("api_key") or None


async def _fetch_usda(client: httpx.AsyncClient, query: str,
                      page_size: int, data_types: Optional[str] = None,
                      require_all_words: bool = False) -> List[Dict[str, Any]]:
    key = _usda_key()
    if not key:
        return []
    params: Dict[str, Any] = {"api_key": key, "query": query,
                              "pageSize": page_size}
    if data_types:
        params["dataType"] = data_types
    if require_all_words:
        params["requireAllWords"] = "true"
    try:
        res = await client.get(_FDC_SEARCH_URL, params=params)
    except (httpx.TimeoutException, httpx.HTTPError):
        return []
    try:
        body = res.json()
    except Exception:
        return []
    return body.get("foods") or []


_GENERICS_TYPES = "Foundation,SR Legacy,Survey (FNDDS)"
_BRANDED_TYPES = "Branded"

# In-tier prior: generics first, branded with owner affinity next.
_DATA_TYPE_PRIOR = {"Foundation": 0, "SR Legacy": 1, "Survey (FNDDS)": 2,
                    "Branded": 3}


def _words(name: str) -> List[str]:
    return [w for w in re.split(r"[^a-z0-9]+", (name or "").lower()) if w]


def _relevance_tier(name: str, query_words: List[str]) -> int:
    if not query_words:
        return 2
    hit = sum(1 for w in query_words if w in (name or "").lower())
    if hit >= len(query_words):
        return 0
    if hit * 2 >= len(query_words):
        return 1
    return 2


def _name_bonus(name: str, query: str) -> int:
    """T09 (OQ-7): exact/word-boundary/start bonus for USDA ranking.

    0 = name equals query (case-insensitive); 1 = name starts with the
    query string; 2 = every query word is a whole word in the name;
    3 = substring containment only. Sorts before the relevance tier so
    "Milk, whole" beats "Crackers, milk" for query "milk" with no extra
    upstream fetches.
    """
    n = (name or "").strip().lower()
    q = (query or "").strip().lower()
    if not n or not q:
        return 3
    if n == q:
        return 0
    if n.startswith(q):
        return 1
    words = set(_words(n))
    if words and all(w in words for w in _words(q)):
        return 2
    return 3


def _plausibility_penalty(nuts: Dict[str, Any],
                          extra: Optional[Dict[str, Any]] = None) -> int:
    ex = extra or {}
    kcal = nuts.get("kcal_per_100g")
    p, f, c = nuts.get("protein_g"), nuts.get("fat_g"), nuts.get("carbs_g")
    pen = 0
    if None not in (kcal, p, f, c) and (4 * p + 9 * f + 4 * c) > 0:
        if not (0.7 <= kcal / (4 * p + 9 * f + 4 * c) <= 1.3):
            pen += 1
    if None not in (p, f, c) and (p + f + c) > 102:
        pen += 1
    if nuts.get("sugars_g") is not None and c is not None:
        pen += 0 if nuts["sugars_g"] <= c else 1
    if (ex.get("satfat") is not None and f is not None
            and ex["satfat"] > f):
        pen += 1
    if nuts.get("fiber_g") is not None and c is not None:
        pen += 0 if nuts["fiber_g"] <= c else 1
    if kcal is not None and kcal > 902:
        pen += 1
    return pen


# T12 (Bug 010-003-T12-1): consensus / outlier / density checks.
# Pure-Python, O(n) over the fetched hits, stdlib only (no pandas).
# All outcomes are penalty/review_reason annotations — never a block.

def _median(vals: List[float]) -> Optional[float]:
    s = sorted(vals)
    n = len(s)
    if not n:
        return None
    mid = n // 2
    return float(s[mid]) if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def _single_ingredient_match(ingredients: Any, query: str) -> bool:
    """True when the ingredient list is only the queried food itself.

    e.g. "ORGANIC ALMONDS." / "ALMONDS." for query "almonds" -> True;
    the Post cereal's 20-ingredient list -> False.
    """
    qw = _words(query)
    toks = _words(str(ingredients or ""))
    return bool(toks) and bool(qw) and all(t in qw for t in toks)


_HH_CUP_RE = re.compile(
    r"1\s*/\s*(\d+)\s*cup", re.IGNORECASE)
_HH_COUNT_RE = re.compile(
    r"(\d+)\s*(nuts?|pieces?|kernels?|almonds?)", re.IGNORECASE)

# Expected grams for count-based servings, e.g. ~1.2 g per almond
# (FNDDS: 1 nut ~= 1.2 g; Argires "22 NUTS" should weigh ~26 g, not 73 g).
_COUNT_G_EACH = {"nut": 1.2, "nuts": 1.2, "almond": 1.2, "almonds": 1.2}


def _density_flag(fd: Dict[str, Any]) -> Optional[str]:
    """Flag physically-implausible serving declarations, else None.

    Catches Schnucks-style "28 GRM in 1/45 CUP" (~5.3 g/mL, denser than
    lead) and Argires-style "73 g for 22 NUTS" (~3x the ~26 g real weight).
    Weight-only servings (no household volume/count text) cannot be
    checked and return None.
    """
    size, unit = fd.get("servingSize"), fd.get("servingSizeUnit")
    try:
        grams = float(size)
    except (TypeError, ValueError):
        return None
    if not grams or grams <= 0:
        return None
    u = str(unit or "").strip().lower()
    if u not in ("g", "gram", "grams", "grm", "ml", "mlt"):
        return None  # IU and friends stay fail-closed elsewhere
    text = str(fd.get("householdServingFullText") or "")
    m = _HH_CUP_RE.search(text)
    if m:
        try:
            cups = 1.0 / float(m.group(1))
        except (TypeError, ValueError, ZeroDivisionError):
            return None
        ml = cups * 236.6
        if ml > 0 and grams / ml > 2.0:  # densest real food ~1.5 g/mL
            return (f"implausible density: {grams:g} g in "
                    f"{cups:g} cup (~{grams / ml:.1f} g/mL)")
        return None
    m = _HH_COUNT_RE.search(text)
    if m:
        try:
            count = float(m.group(1))
        except (TypeError, ValueError):
            return None
        each = _COUNT_G_EACH.get(m.group(2).lower())
        if each and count > 0:
            expected = count * each
            if expected > 0 and grams / expected > 2.0:
                return (f"implausible serving weight: {grams:g} g for "
                        f"{m.group(0)} (~{expected:.0f} g expected)")
        return None
    return None


def _consensus_flags(mapped: List[Dict[str, Any]],
                     query: str) -> Dict[int, str]:
    """MAD outlier flags for the single-ingredient exact-name group.

    Group = gate-passing hits whose description is the query itself and
    whose ingredient list is only the query (19 almond rows). n>=5 ->
    median + MAD; flag beyond max(3x1.4826xMAD, 10% of median).
    Returns {id(m): reason}; empty when the group is too small.
    """
    group = [m for m in mapped
             if m.get("gate_ok")
             and (m.get("name") or "").strip().lower()
             == (query or "").strip().lower()
             and _single_ingredient_match((m.get("raw") or {}).get(
                 "ingredients"), query)
             and isinstance((m.get("nuts") or {}).get("kcal_per_100g"),
                            (int, float))]
    if len(group) < 5:
        return {}
    med = _median([m["nuts"]["kcal_per_100g"] for m in group])
    mad = _median([abs(m["nuts"]["kcal_per_100g"] - med) for m in group])
    tol = max(3 * 1.4826 * (mad or 0.0), 0.10 * (med or 0.0))
    return {id(m): (f"outlier vs single-ingredient consensus "
                    f"(kcal {m['nuts']['kcal_per_100g']:g} vs "
                    f"median {med:g})")
            for m in group if abs(m["nuts"]["kcal_per_100g"] - med) > tol}


def _category_mismatch(fd: Dict[str, Any], query: str) -> Optional[str]:
    """Flag description/category/ingredient disagreement (Post cereal case).

    Description says the food ("ALMONDS") AND the ingredient list is long
    and multi-item AND the category doesn't contain the query — all three
    must hold. Category alone is not enough: canonical single-ingredient
    rows live in "Popcorn, Peanuts, Seeds & Related Snacks", which does
    not contain "almonds" but is not a mismatch.
    """
    desc = (fd.get("description") or "").strip().lower()
    q = (query or "").strip().lower()
    if not desc or not q or desc != q:
        return None
    toks = _words(str(fd.get("ingredients") or ""))
    if len(toks) <= 3 or all(t in _words(q) for t in toks):
        return None
    cat = str(fd.get("foodCategory") or "")
    if q in cat.lower():
        return None
    return (f"category/ingredient mismatch: described as {q!r} "
            f"but category {cat!r} with multi-item ingredients")


# 11 label-panel nutrients beyond the 4 gate macros (OQ-4 completeness).
_PANEL_EXTRA = ("satfat", "transfat", "cholesterol", "sodium", "fiber",
                "sugars", "added_sugars", "vitd", "calcium", "iron",
                "potassium")


def _quality_label(extra_present: int) -> str:
    # T09 (OQ-7): survey/SR-Legacy rows top out at 6/11 extras, so High
    # starts at 6 — the old 8/4 made High unattainable for the best dtypes.
    if extra_present >= 6:
        return "High"
    if extra_present >= 3:
        return "Medium"
    return "Low"


# 010-004: calorie-reconciliation + ranked-search helpers.
# Pure-Python, O(n) over the mapped hits, stdlib only (no pandas).
# kcal gate (OQ-4): computed = 4P + 4C + 9F + 7A with alcohol defaulting to
# 0 when absent; gate requires kcal+P+F+C present. A hit is excluded only
# when abs(reported - computed) > max(0.15 * computed, 20) — the 20 kcal
# absolute floor protects trace-macro items like black coffee.
# quality_percent (OQ-5): round(100 * present / 10) over exactly kcal,
# protein, fat, carbs, fiber, sugars, sodium, caffeine, serving_size,
# serving unit — each by presence (0 counts, null does not); caffeine null
# always penalizes, including OFF rows. Serving counts by presence, not
# parseability. Match (OQ-2): exact name=100 / starts-with=90 /
# all-query-words-whole-word=80 anchors, else token-overlap percent
# 0-100; match_band = pct // 10 (10% bands). Single-ingredient (OQ-3):
# non-null bool, true only when the normalized ingredient text exactly
# equals the normalized (possibly multi-word) query; missing/empty -> False.
# ingredient_count is the ingredient token count (null when missing, sorts
# last ascending). Sort (AC-4): saved-first is handled by the caller;
# within USDA hits: match_band desc, is_single_ingredient,
# quality_percent desc, ingredient_count asc — ahead of the legacy
# tier/dtype/prior keys, which remain as tiebreaks.

_KCAL_ATWATER = {"protein_g": 4.0, "carbs_g": 4.0, "fat_g": 9.0,
                 "alcohol_g": 7.0}
_KCAL_GATE_PCT = 0.15
_KCAL_GATE_FLOOR = 20.0


def _norm_text(s: Any) -> str:
    return re.sub(r"\s+", " ", (s or "") if isinstance(s, str)
                  else str(s or "")).strip().lower()


def _match_pct(name: str, query: str) -> int:
    """Hybrid match percent 0-100 (anchors + token overlap, stdlib only)."""
    n = _norm_text(name)
    q = _norm_text(query)
    if not n or not q:
        return 0
    if n == q:
        return 100
    if n.startswith(q):
        return 90
    qw = _words(q)
    if qw and all(w in set(_words(n)) for w in qw):
        return 80
    # Token overlap below the anchors: Jaccard-style share of query words
    # hit as substrings, scaled into 0-79 so anchors always win.
    hit = sum(1 for w in qw if w and w in n)
    return int(79 * hit / len(qw)) if qw else 0


def _match_band(pct: int) -> int:
    return max(0, min(10, int(pct) // 10))


def _ingredient_stats(ingredients: Any, query: str) -> tuple:
    """(is_single_ingredient bool, ingredient_count int|None)."""
    toks = _words(str(ingredients or ""))
    if not toks:
        return False, None
    return _norm_text(ingredients) == _norm_text(query), len(toks)


def _quality_percent(nuts: Dict[str, Any],
                     serving_size: Any, serving_unit: Any) -> int:
    present = sum(1 for k in (
        "kcal_per_100g", "protein_g", "fat_g", "carbs_g", "fiber_g",
        "sugars_g", "sodium_mg", "caffeine_mg")
        if nuts.get(k) is not None)
    if serving_size is not None:
        try:
            if float(serving_size) > 0 or str(serving_size).strip():
                present += 1
        except (TypeError, ValueError):
            if str(serving_size).strip():
                present += 1
    if serving_unit is not None and str(serving_unit).strip():
        present += 1
    return int(round(100 * present / 10))


def _kcal_gate(nuts: Dict[str, Any],
               alcohol_g: Optional[float] = None) -> tuple:
    """(excluded bool, kcal_delta_pct float|None).

    Gate requires kcal+P+F+C; computed = 4P + 4C + 9F + 7A (alcohol 0 when
    absent). Excluded only when
    abs(reported - computed) > max(0.15 * computed, 20).
    """
    kcal = nuts.get("kcal_per_100g")
    p, f, c = (nuts.get("protein_g"), nuts.get("fat_g"),
               nuts.get("carbs_g"))
    if None in (kcal, p, f, c):
        return False, None
    try:
        a = float(alcohol_g) if alcohol_g is not None else 0.0
        computed = (4.0 * float(p) + 4.0 * float(c) + 9.0 * float(f)
                    + 7.0 * a)
        rep = float(kcal)
    except (TypeError, ValueError):
        return False, None
    delta = abs(rep - computed)
    delta_pct = (100.0 * delta / computed) if computed > 0 else None
    if computed <= 0:
        return False, delta_pct
    excluded = delta > max(_KCAL_GATE_PCT * computed, _KCAL_GATE_FLOOR)
    return excluded, delta_pct


def _map_usda_food(fd: Dict[str, Any]) -> Dict[str, Any]:
    from backend_functions.food_normalize import (
        normalize_usda_branded, normalize_usda_survey, serving_fields)
    dtype = fd.get("dataType") or ""
    nuts_raw = {str(n.get("nutrientName") or "").strip().lower(): n.get("value")
                for n in (fd.get("foodNutrients") or [])}
    extra = {k: nuts_raw.get(k) for k in (
        "saturated fat", "trans fat", "cholesterol, total", "sodium, na",
        "fiber, total dietary", "total sugars", "added sugars",
        "vitamin d", "calcium, ca", "iron, fe", "potassium, k")}
    if dtype == "Branded":
        nuts, needs_review, reason = normalize_usda_branded(fd)
        present = sum(1 for v in extra.values() if v is not None)
    else:
        nuts = normalize_usda_survey(fd)
        needs_review = any(v is None for v in nuts.values())
        reason = "unreported at source" if needs_review else ""
        present = sum(1 for v in extra.values() if v is not None)
    gate_ok = all(nuts.get(k) is not None
                  for k in ("kcal_per_100g", "protein_g", "fat_g", "carbs_g"))
    # T10 (OQ-8): feed serving for popup defaults + persistence (fail-closed;
    # survey/legacy rows carry no servingSize -> all-None, 100 g fallback).
    sv_size, sv_unit, sv_text = serving_fields(
        fd.get("servingSize"), fd.get("servingSizeUnit"),
        fd.get("householdServingFullText"))
    # T12: annotation-only quality flags (penalty/review_reason, never a block).
    t12_notes = []
    dens = _density_flag(fd)
    if dens:
        t12_notes.append(dens)
    if needs_review or not gate_ok:
        base_reason = reason
    else:
        base_reason = ""
    if base_reason:
        t12_notes.insert(0, base_reason)
    # 010-004: calorie-reconciliation gate + ranked-search fields. Alcohol
    # comes from the raw hit ("Alcohol, ethyl" by name or id 1018), 0 when
    # absent; excluded hits are hard-filtered in rank_usda_hits, but the
    # flag + delta travel on the map for contract/tests.
    alcohol = nuts_raw.get("alcohol, ethyl")
    if alcohol is None:
        for n in (fd.get("foodNutrients") or []):
            try:
                if int(n.get("nutrientId")) == 1018:
                    alcohol = n.get("value")
                    break
            except (TypeError, ValueError):
                continue
    try:
        alcohol_g = float(alcohol) if alcohol is not None else None
    except (TypeError, ValueError):
        alcohol_g = None
    excluded, delta_pct = _kcal_gate(nuts, alcohol_g)
    quality_percent = _quality_percent(nuts, fd.get("servingSize"),
                                       fd.get("servingSizeUnit"))
    is_single, ing_count = _ingredient_stats(fd.get("ingredients"), "")
    return {"dtype": dtype or "Unknown",
            "fdcId": fd.get("fdcId"),
            "name": fd.get("description") or "Unknown food",
            "nuts": nuts, "extra": extra, "present": present,
            "needs_review": needs_review or not gate_ok or bool(dens),
            "review_reason": "; ".join(t for t in t12_notes if t),
            "penalty": (_plausibility_penalty(nuts, {"satfat": extra.get(
                "saturated fat")}) + (1 if dens else 0)),
            "gate_ok": gate_ok,
            "score": fd.get("score"),
            "brand": (fd.get("brandOwner") or ""),
            "published": str(fd.get("publishedDate") or ""),
            "serving_size": sv_size, "serving_unit": sv_unit,
            "serving_text": sv_text,
            "raw": fd,
            "quality_percent": quality_percent,
            "is_single_ingredient": is_single,
            "ingredient_count": ing_count,
            "kcal_delta_pct": delta_pct,
            "excluded_kcal_mismatch": excluded,
            "food_category": fd.get("foodCategory"),
            "serving_unit_facet": (str(fd.get("servingSizeUnit") or "").strip()
                                   or None)}


def rank_usda_hits(hits: Sequence[Dict[str, Any]],
                   query: str) -> List[Dict[str, Any]]:
    """Tiered rank (OQ-4, T09 name bonus, T12 annotations, 010-004 fields).

    010-004 sort within USDA hits: match_band desc, is_single_ingredient,
    quality_percent desc, ingredient_count asc (nulls last) — ahead of the
    legacy tier/dtype/prior keys, which remain as tiebreaks. The kcal gate
    is a hard filter: excluded_kcal_mismatch hits are dropped (never merely
    sunk). Gate, name bonus, relevance tier, then anchor/data-type prior
    (T12: Foundation/SR Legacy lab-analysed rows outrank branded *within*
    the same tier — implemented as a (tier, prior) pair so the T09 tier
    order that existing tests pin is preserved), completeness, penalty,
    freshness, USDA score tiebreak. T12 consensus/outlier +
    category-mismatch flags add +1 penalty and a review_reason note each —
    annotation only, never a block.
    """
    qw = _words(query)
    mapped = [_map_usda_food(h) for h in hits]
    for m in mapped:
        pct = _match_pct(m["name"], query)
        m["match_pct"] = pct
        m["match_band"] = _match_band(pct)
        # Query-aware single-ingredient flag (mapper runs query-blind).
        is_single, _ = _ingredient_stats((m.get("raw") or {}).get(
            "ingredients"), query)
        m["is_single_ingredient"] = is_single
    # 010-004 kcal gate: hard filter (drop), never a sink.
    mapped = [m for m in mapped if not m.get("excluded_kcal_mismatch")]
    # T12: consensus + category-mismatch pass over the mapped hits.
    consensus = _consensus_flags(mapped, query)
    for m in mapped:
        extra_pen = 0
        notes = []
        if id(m) in consensus:
            extra_pen += 1
            notes.append(consensus[id(m)])
        mm = _category_mismatch(m.get("raw") or {}, query)
        if mm:
            extra_pen += 1
            notes.append(mm)
        if extra_pen:
            m["penalty"] = m.get("penalty", 0) + extra_pen
            m["needs_review"] = True
            prior = m.get("review_reason") or ""
            m["review_reason"] = "; ".join(
                t for t in [prior, *notes] if t)
    # Gate: missing any of kcal/P/F/C (or no kcal = unloggable) sinks.
    # 010-004 keys lead (band desc, single-ingredient, percent desc, count
    # asc with nulls last); legacy keys tiebreak below.
    mapped.sort(key=lambda m: (
        0 if m["gate_ok"] else 1,
        -m["match_band"],
        0 if m["is_single_ingredient"] else 1,
        -m["quality_percent"],
        (m["ingredient_count"] if m["ingredient_count"] is not None
         else 10**9),
        _name_bonus(m["name"], query),
        (_relevance_tier(m["name"], qw),
         _DATA_TYPE_PRIOR.get(m["dtype"], 4)),
        -m["present"],
        m["penalty"],
        m["published"],
        -(m["score"] or 0),
    ))
    return mapped


async def fetch_usda_ranked(query: str) -> Dict[str, Any]:
    """2 parallel fetches (~75 candidates) + fallback; cached ~1d.

    Returns {results: ranked-mapped, fetches: n, cached: bool}. Backend trims
    each hit to the ranking fields; the full body is never forwarded.
    """
    norm = _norm_query(query)
    if not norm:
        return {"results": [], "fetches": 0, "cached": False}
    hit = _cache_get(f"usda:{norm}")
    if hit is not None:
        hit["cached"] = True
        return hit
    fetches = 0
    generics: List[Dict[str, Any]] = []
    branded: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=_TIMEOUT_S) as client:
        import asyncio
        g, b = await asyncio.gather(
            _fetch_usda(client, query, _GENERICS_PAGE, _GENERICS_TYPES),
            _fetch_usda(client, query, _BRANDED_PAGE, _BRANDED_TYPES),
        )
        fetches += 2
        generics, branded = g, b
        if len(branded) < 5:
            extra = await _fetch_usda(client, query, _FALLBACK_PAGE,
                                      _BRANDED_TYPES, require_all_words=False)
            fetches += 1
            seen = {x.get("fdcId") for x in branded}
            branded = branded + [x for x in extra
                                 if x.get("fdcId") not in seen]
    ranked = rank_usda_hits(generics + branded, query)
    out = {"results": ranked, "fetches": fetches, "cached": False}
    _cache_put(f"usda:{norm}", out)
    return out


def log_pick(rank: int, result_source: str, query: str) -> None:
    """Tuning log: which rank the user picked + which fetch supplied it."""
    logger.info("food_pick query=%r rank=%s source=%s", _norm_query(query),
                rank, result_source)