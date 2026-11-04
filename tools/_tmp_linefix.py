import re, sys
from pathlib import Path
sys.path.insert(0, 'tools')
import lint_comments as L

PATTERNS = [L.BUG_TASK_RE, L.FEATURE_ID_RE, L.BUG_ID_RE, L.TASK_ID_RE,
            L.PLAN_TASK_ID_RE, L.DATE_RE, L.CHANGE_HISTORY_RE]

def find_removal_spans(text):
    """Find spans to remove. For each ID match, include enclosing parens/brackets."""
    spans = []
    for p in PATTERNS:
        for m in p.finditer(text):
            start, end = m.start(), m.end()
            if start > 0 and text[start - 1] == '(':
                close = text.find(')', end)
                if close > end:
                    spans.append((start - 1, close + 1))
                    continue
            if start > 0 and text[start - 1] == '[':
                close = text.find(']', end)
                if close > end:
                    spans.append((start - 1, close + 1))
                    continue
            spans.append((start, end))
    spans.sort(reverse=True)
    merged = []
    for s, e in spans:
        if merged and s <= merged[-1][0]:
            merged[-1] = (s, merged[-1][1])
        else:
            merged.append((s, e))
    return merged

def remove_spans(text, spans):
    result = text
    for s, e in spans:
        result = result[:s] + result[e:]
    return result

def cleanup(text):
    c = text
    c = re.sub(r'\bBug\s+', '', c, flags=re.IGNORECASE)
    c = re.sub(r'\bBUG-\s*', '', c)
    c = re.sub(r'\(\s*\)', '', c)
    c = re.sub(r'\[\s*\]', '', c)
    c = re.sub(r'--+', '', c)
    c = re.sub(r'[ \t]+', ' ', c)
    c = re.sub(r'\s+([:;.,)]])', r'\1', c)
    c = re.sub(r'([(:])\s+', r'\1', c)
    c = c.strip(' :;.,()-/')
    c = re.sub(r'\(\s*$', '', c)
    c = re.sub(r'^\s*\)', '', c)
    c = c.strip(' :;.,()-/')
    return c

def fix_comment_text(text):
    spans = find_removal_spans(text)
    result = remove_spans(text, spans)
    return cleanup(result)

def fix_file(path):
    lines = path.read_text(encoding='utf-8', errors='replace').splitlines(keepends=True)
    
    violations_by_line = {}
    for key, detail in L.scan_file(path):
        m = re.match(r'L(\d+):', detail)
        if m:
            ln = int(m.group(1))
            violations_by_line.setdefault(ln, []).append(detail)
    
    if not violations_by_line:
        return 0
    
    changed = 0
    for ln in sorted(violations_by_line, reverse=True):
        idx = ln - 1
        line = lines[idx]
        stripped = line.strip()
        
        if stripped.startswith('#'):
            hash_pos = line.index('#')
            comment_part = stripped[1:].strip()
            cleaned = fix_comment_text(comment_part)
            if cleaned:
                lines[idx] = line[:hash_pos] + '# ' + cleaned + '\n'
            else:
                lines.pop(idx)
            changed += 1
        
        elif stripped.startswith(('"""', "'''")):
            cleaned = fix_comment_text(line.strip())
            if not cleaned or cleaned in ('"""', "'''"):
                pass
            else:
                lines[idx] = cleaned + '\n'
            changed += 1
    
    if changed:
        path.write_text(''.join(lines), encoding='utf-8')
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
        print('TOTAL changed:', total)