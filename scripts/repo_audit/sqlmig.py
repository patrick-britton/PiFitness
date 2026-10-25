"""SQL/migrations: classify files, pair rollbacks, list DDL objects, and propose a global order
(from git first-commit dates) so migrations can be renumbered into db/migrations/NNNN_*.sql.
"""
from __future__ import annotations

import collections
import posixpath
import re

from .core import Section

DDL = re.compile(
    r"\b(CREATE|ALTER|DROP)\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?(TABLE|VIEW|FUNCTION|PROCEDURE|INDEX|TYPE|TRIGGER|SCHEMA|SEQUENCE)\s+"
    r"(?:IF\s+(?:NOT\s+)?EXISTS\s+)?([\w.\"]+)", re.I)
ROLLBACK = re.compile(r"(?i)(rollback|_down\b|revert|undo)")
FEATURE = re.compile(r"(\d{3}-\d{3})")


def kind_of(rel: str) -> str:
    low = rel.lower()
    if ROLLBACK.search(posixpath.basename(low)):
        return "rollback"
    if "/migrations/" in "/" + low:
        return "migration"
    if re.search(r"schema|reference|ddl", posixpath.basename(low)):
        return "schema"
    if low.startswith("scripts/"):
        return "adhoc"
    return "other"


def stem(rel: str) -> str:
    n = posixpath.basename(rel)[:-4]
    return re.sub(r"(?i)[_-]?(rollback|down|revert|undo)", "", n)


def analyze(repo) -> Section:
    sec = Section("sql")
    files = repo.by_ext(".sql")
    rows = []
    objs_by_file = {}
    for f in files:
        t = repo.text(f) or ""
        objs = sorted({f"{m.group(2).upper()} {m.group(3).strip(chr(34)).lower()}" for m in DDL.finditer(t)})
        objs_by_file[f] = objs
        rows.append({"path": f, "kind": kind_of(f), "lines": t.count("\n") + 1, "feature": (FEATURE.search(f) or [None, None])[1],
                     "objects": objs[:15], "n_objects": len(objs), "has_begin": bool(re.search(r"(?i)\bBEGIN\b", t)),
                     "has_commit": bool(re.search(r"(?i)\bCOMMIT\b", t))})
    migs = [r for r in rows if r["kind"] == "migration"]
    rbs = {stem(r["path"]): r["path"] for r in rows if r["kind"] == "rollback"}
    unpaired = [r["path"] for r in migs if stem(r["path"]) not in rbs and not r["path"].endswith("_body.sql")]
    orphan_rb = [p for s, p in rbs.items() if s not in {stem(m["path"]) for m in migs}]

    obj_touch = collections.defaultdict(list)
    for r in migs:
        for o in objs_by_file[r["path"]]:
            obj_touch[o].append(r["path"])
    multi = {o: sorted(set(p)) for o, p in obj_touch.items() if len(set(p)) > 1}

    # propose global order from git first-commit date, falling back to feature id then name
    dates = {}
    if repo.has_git:
        for r in migs:
            out = (repo.git("log", "--diff-filter=A", "--format=%aI", "--", r["path"]) or "").strip().splitlines()
            dates[r["path"]] = out[-1] if out else ""
    order = sorted(migs, key=lambda r: (dates.get(r["path"], "") or "9999", r["feature"] or "999-999", r["path"]))
    proposal = []
    for i, r in enumerate(order, 1):
        base = re.sub(r"^\d{3}-\d{3}_?", "", stem(r["path"])) or stem(r["path"])
        feat = r["feature"] or ""
        new = f"db/migrations/{i:04d}_{feat + '_' if feat else ''}{base}.sql".replace("__", "_")
        rb = rbs.get(stem(r["path"]))
        proposal.append({"old": r["path"], "new": new, "rollback_old": rb,
                         "rollback_new": new[:-4] + ".down.sql" if rb else None, "first_commit": dates.get(r["path"], "")})

    sec.data = {"files": rows, "migration_count": len(migs), "rollback_count": len(rbs),
                "migrations_without_rollback": unpaired, "rollbacks_without_migration": orphan_rb,
                "objects_touched_by_multiple_migrations": multi, "proposed_order": proposal,
                "note": "Proposed order comes from git first-commit dates; confirm against the real applied order in prod before renaming."}
    if migs and not any("db/migrations" in m["path"] for m in migs):
        sec.add("SQL-001", "high", "Migrations live outside a canonical db/migrations directory", count=len(migs),
                evidence=sorted({posixpath.dirname(m["path"]) for m in migs}),
                hint="Hidden dot-folders named by feature id have no global order; move to db/migrations/NNNN_*.sql with paired .down.sql.")
    if unpaired:
        sec.add("SQL-002", "low", "Migrations with no paired rollback", count=len(unpaired), evidence=unpaired)
    if multi:
        sec.add("SQL-003", "info", "DB objects altered by more than one migration (order matters)", count=len(multi),
                evidence=[f"{o}: {len(p)} files" for o, p in list(multi.items())[:20]])
    adhoc = [r["path"] for r in rows if r["kind"] == "adhoc"]
    if adhoc:
        sec.add("SQL-004", "low", "Ad-hoc SQL files under scripts/", evidence=adhoc)
    no_tx = [r["path"] for r in migs if not (r["has_begin"] and r["has_commit"])]
    if no_tx:
        sec.add("SQL-005", "low", "Migrations without an explicit BEGIN/COMMIT", count=len(no_tx), evidence=no_tx)
    sec.metrics = {"sql.files": len(rows), "sql.migrations": len(migs), "sql.migrations_unpaired": len(unpaired),
                   "sql.migrations_outside_db_dir": len([m for m in migs if not m["path"].startswith("db/migrations/")])}
    return sec
