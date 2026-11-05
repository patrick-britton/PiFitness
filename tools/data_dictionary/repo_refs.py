"""Find repository file/line references to database objects from schema.generated.md."""
from __future__ import annotations
import argparse
import re
from pathlib import Path
from schema_inventory import parse_schema
from common import write_json

EXTS = {'.py', '.sql', '.ts', '.tsx', '.js', '.jsx', '.json', '.yml', '.yaml', '.md', '.mdx', '.ps1', '.sh'}
SKIP = {'.git', '.venv', 'venv', 'node_modules', '.next', 'dist', 'build', '__pycache__', '.pytest_cache', '.mypy_cache', 'coverage', 'vendor'}


def iter_files(root: Path):
    for path in root.rglob('*'):
        if not path.is_file() or path.suffix.lower() not in EXTS:
            continue
        if any(part in SKIP for part in path.parts):
            continue
        try:
            rel = path.relative_to(root).as_posix()
            if rel in {'docs/data/schema.generated.md', 'docs/data/dictionary.md'} or rel.startswith('docs/data/_evidence/'):
                continue
            if path.stat().st_size > 2_000_000:
                continue
        except OSError:
            continue
        yield path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', default='.')
    parser.add_argument('--schema-file', default='docs/data/schema.generated.md')
    parser.add_argument('--out', default='docs/data/_evidence/repo_references.json')
    parser.add_argument('--max-hits-per-object', type=int, default=80)
    args = parser.parse_args()
    root = Path(args.root).resolve()
    objects = parse_schema(args.schema_file)['objects']
    term_targets: dict[str, set[str]] = {}
    for obj in objects:
        term_targets.setdefault(obj['qualified_name'], set()).add(obj['qualified_name'])
        # Preserve all matches where different schemas contain the same bare relation name.
        term_targets.setdefault(obj['name'], set()).add(obj['qualified_name'])
    hits = {obj['qualified_name']: [] for obj in objects}
    files = list(iter_files(root))
    terms = sorted(term_targets, key=len, reverse=True)
    combined = re.compile(r'(?<![A-Za-z0-9_])(?:' + '|'.join(re.escape(t) for t in terms) + r')(?![A-Za-z0-9_])', re.I)
    canonical = {term.lower(): term for term in terms}
    for file_index, path in enumerate(files, 1):
        try:
            lines = path.read_text(encoding='utf-8-sig', errors='replace').splitlines()
        except (OSError, UnicodeError):
            continue
        rel = path.relative_to(root).as_posix()
        for line_no, line in enumerate(lines, 1):
            for match in combined.finditer(line):
                term = canonical.get(match.group(0).lower(), match.group(0))
                strength = 'qualified' if '.' in term else 'unqualified_hint'
                for object_name in term_targets.get(term, set()):
                    if len(hits[object_name]) >= args.max_hits_per_object:
                        continue
                    hits[object_name].append({
                        'path': rel, 'line': line_no, 'matched_term': term,
                        'strength': strength, 'excerpt': line.strip()[:260],
                    })
        if file_index % 200 == 0:
            print(f'Scanned {file_index}/{len(files)} files')
    payload = {
        'root': str(root), 'files_scanned': len(files), 'object_count': len(objects),
        'references': hits, 'summary': {name: len(refs) for name, refs in hits.items()},
    }
    write_json(args.out, payload)
    print(f'Scanned {len(files)} files for {len(objects)} objects -> {args.out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
