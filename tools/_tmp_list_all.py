import sys
from pathlib import Path
sys.path.insert(0, 'tools')
import lint_comments as L
root = Path('backend')
out = []
for fp in sorted(root.rglob('*.py')):
    for key, detail in L.scan_file(fp):
        out.append(f'{fp.as_posix()}: {detail}')
Path('_violations.txt').write_text('\n'.join(out) + '\n', encoding='utf-8')
print('wrote', len(out), 'violations')
