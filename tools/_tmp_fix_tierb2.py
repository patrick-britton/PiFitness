import re, sys
from pathlib import Path
sys.path.insert(0, 'tools')
import lint_comments as L

BUG_TASK_RE = L.BUG_TASK_RE
FEATURE_ID_RE = L.FEATURE_ID_RE
BUG_ID_RE = L.BUG_ID_RE
TASK_ID_RE = L.TASK_ID_RE
PLAN_TASK_ID_RE = L.PLAN_TASK_ID_RE
DATE_RE = L.DATE_RE
CHANGE_HISTORY_RE = L.CHANGE_HISTORY_RE
PATTERNS = [BUG_TASK_RE, FEATURE_ID_RE, BUG_ID_RE, TASK_ID_RE, PLAN_TASK_ID_RE, DATE_RE, CHANGE_HISTORY_RE]

def strip_markers(content):
    c = content
    for p in PATTERNS:
        c = p.sub('', c)
    return c

def clean_content(content, is_docstring):
    c = strip_markers(content)
    if is_docstring:
        lines = c.split('\n')
        lines = [re.sub(r'[ \t]+', ' ', ln).strip(' :;.,()-/') for ln in lines]
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        c = '\n'.join(lines)
    else:
        c = re.sub(r'[ \t]+', ' ', c).strip(' :;.,()-/')
    return c.strip()

def spans(text):
    """Mirror lint_comments.extract_comment_spans but with offsets.

    Yields (start, end, line_no, comment_text, kind, quote) where kind in
    {'hash','doc'} and comment_text excludes the comment/docstring delimiters.
    """
    lines = text.splitlines(keepends=True)
    offs = []
    o = 0
    for ln in lines:
        offs.append(o); o += len(ln)
    in_doc = False; quote = None; doc_start_line = None; doc_start_off = None
    for i, line in enumerate(lines):
        ln = i + 1; ls = offs[i]; st = line.strip()
        if in_doc:
            idx = st.find(quote)
            if idx >= 0:
                end = ls + idx + len(quote)
                yield (doc_start_off, end, doc_start_line,
                       text[doc_start_off + len(quote):end], 'doc', quote)
                in_doc = False; quote = None
            continue
        dq_start = -1; dq = None
        for q in ('"""', "'''"):
            idx = st.find(q)
            if idx >= 0 and q not in st[idx + len(q):]:
                dq_start = idx; dq = q; break
        if dq_start >= 0:
            content = st[dq_start + len(dq):]
            if dq in content:
                ci = content.find(dq)
                end = ls + dq_start + len(dq) + ci + len(dq)
                yield (ls + dq_start, end, ln, content[:ci], 'doc', dq)
            else:
                in_doc = True; quote = dq; doc_start_line = ln; doc_start_off = ls + dq_start
            continue
        cidx = st.find('#')
        if cidx >= 0:
            yield (ls + cidx, ls + len(st), ln, st[cidx + 1:].strip(), 'hash', None)

def rebuild(kind, quote, raw, new_content):
    if kind == 'doc':
        return quote + new_content + quote
    if new_content:
        return '# ' + new_content
    return None

def fix_file(path):
    text = path.read_text(encoding='utf-8', errors='replace')
    spans_list = list(spans(text))
    new_text = text
    changed = 0
    for start, end, line_no, comment, kind, quote in reversed(spans_list):
        if not any(p.search(comment) for p in PATTERNS):
            continue
        raw = text[start:end]
        new_content = clean_content(comment, kind == 'doc')
        new_raw = rebuild(kind, quote, raw, new_content)
        if new_raw is None:
            line_start = text.rfind('\n', 0, start) + 1
            line_end = text.find('\n', end)
            if line_end < 0: line_end = len(text)
            cut_end = line_end
            if cut_end < len(new_text) and new_text[cut_end] == '\r': cut_end += 1
            if cut_end < len(new_text) and new_text[cut_end] == '\n': cut_end += 1
            new_text = new_text[:line_start] + new_text[cut_end:]
        else:
            new_text = new_text[:start] + new_raw + new_text[end:]
        changed += 1
    if changed:
        path.write_text(new_text, encoding='utf-8')
    return changed

if __name__ == '__main__':
    target = sys.argv[1] if len(sys.argv) > 1 else None
    if target:
        n = fix_file(Path(target))
        print(f'{target}: {n}')
    else:
        root = Path('backend')
        total = 0
        for fp in sorted(root.rglob('*.py')):
            n = fix_file(fp)
            if n:
                print(f'{fp}: {n}')
                total += n
        print('TOTAL changed comments:', total)
