"""Git history: churn hotspots, co-change coupling and how many folders/domains a typical change
touches. Co-change is the best empirical evidence for where vertical-slice boundaries belong."""
from __future__ import annotations

import collections
import datetime as dt
import itertools
import re

from .core import Section, domain_of, is_test_path

FEATURE_ID = re.compile(r"\b(\d{3}-\d{3})\b")


def analyze(repo) -> Section:
    sec = Section("git")
    if not repo.has_git:
        sec.data = {"note": "not a git repo or git unavailable"}
        return sec
    g = repo.cfg["git"]
    since = (dt.date.today() - dt.timedelta(days=30 * g["since_months"])).isoformat()
    out = repo.git("log", f"--since={since}", "--no-merges", "--name-only", "--pretty=format:@@%h|%aI|%s", timeout=180) or ""
    commits = []
    cur = None
    for ln in out.splitlines():
        if ln.startswith("@@"):
            h, d, s = (ln[2:].split("|", 2) + ["", ""])[:3]
            cur = {"h": h, "d": d, "s": s, "files": []}
            commits.append(cur)
        elif ln.strip() and cur is not None:
            cur["files"].append(ln.strip())
    domains = repo.cfg["domains"]
    live = repo.fileset

    churn = collections.Counter()
    last = {}
    for c in commits:
        for f in c["files"]:
            churn[f] += 1
            last.setdefault(f, c["d"][:10])

    pair = collections.Counter()
    single = collections.Counter()
    dir_counts, dom_counts, feat_dirs = [], [], collections.defaultdict(set)
    used_commits = 0
    for c in commits:
        fs = [f for f in c["files"] if f in live and not f.startswith((".audit/", "docs/generated/"))]
        if not fs or len(fs) > g["max_commit_files"]:
            continue
        used_commits += 1
        code = [f for f in fs if f.endswith((".py", ".ts", ".tsx", ".sql"))]
        if code:
            dir_counts.append(len({f.rsplit("/", 1)[0] if "/" in f else "." for f in code}))
            ds = {domain_of(f, domains) or "unassigned" for f in code if not is_test_path(f)}
            if ds:
                dom_counts.append(len(ds))
        for f in set(fs):
            single[f] += 1
        for a, b in itertools.combinations(sorted(set(fs)), 2):
            pair[(a, b)] += 1
        m = FEATURE_ID.search(c["s"])
        if m:
            feat_dirs[m.group(1)].update(f.split("/")[0] for f in fs)

    def area(f):
        p = f.split("/")
        return "/".join(p[:2]) if len(p) > 2 else p[0]

    cross = collections.Counter()
    for (a, b), n in pair.items():
        if n >= g["min_cochange"] and area(a) != area(b):
            cross[(area(a), area(b))] += n
    strong = [{"a": a, "b": b, "commits_together": n,
               "coupling": round(n / min(single[a], single[b]), 2)}
              for (a, b), n in pair.most_common(400) if n >= g["min_cochange"] and area(a) != area(b)][:30]

    avg = lambda xs: round(sum(xs) / len(xs), 2) if xs else 0
    sec.data = {
        "since": since, "commits_analyzed": len(commits), "commits_used_for_coupling": used_commits,
        "hotspots": [{"path": f, "commits": n, "last_change": last.get(f)} for f, n in churn.most_common(25) if f in live],
        "avg_code_dirs_per_commit": avg(dir_counts), "avg_domains_per_commit": avg(dom_counts),
        "folder_pair_cochange": [{"a": a, "b": b, "weight": n} for (a, b), n in cross.most_common(25)],
        "strong_cross_area_file_pairs": strong,
        "feature_footprint": {k: sorted(v) for k, v in sorted(feat_dirs.items())[:80]},
        "avg_top_dirs_per_feature": avg([len(v) for v in feat_dirs.values()]),
        "stale_files_never_changed_in_window": sorted(f for f in live if f not in churn and f.endswith((".py", ".ts", ".tsx")))[:60],
    }
    if strong:
        sec.add("GIT-001", "info", "File pairs in different folders that change together most (candidate same-slice files)",
                count=len(strong), evidence=[f"{s['commits_together']}x {s['a']} + {s['b']}" for s in strong[:12]],
                hint="Strong coupling across folders is the signal to colocate by domain.")
    if dir_counts:
        sec.add("GIT-002", "info", f"A typical code commit touches {avg(dir_counts)} directories and {avg(dom_counts)} domains",
                detail="Baseline for the reorg: the first number should drop after domain-sliced folders.")
    sec.metrics = {"git.avg_code_dirs_per_commit": avg(dir_counts), "git.avg_domains_per_commit": avg(dom_counts)}
    return sec
