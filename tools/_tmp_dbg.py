import sys
from pathlib import Path
sys.path.insert(0, 'tools')
import lint_comments as L
sys.path.insert(0, 'tools')
import _tmp_fix_tierb2 as F

fp = 'backend/pifitness/admin/services.py'
text = Path(fp).read_text(encoding='utf-8', errors='replace')
print('=== extract_comment_spans ===')
for ln, c in L.extract_comment_spans(text):
    print(ln, repr(c))
print('=== spans() ===')
for s, e, ln, c, kind, q in F.spans(text):
    print(ln, kind, q, repr(c), 'raw=', repr(text[s:e]))
