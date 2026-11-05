import re
p='docs/data/dictionary.md'
s=open(p,encoding='utf-8').read()
pattern = re.compile(r"\| `(?P<col>[^`]+)` \| (?P<type>[^|]+) \| (?P<null>[^|]+) \| Unknown \u2014 investigation incomplete \| unknown \| schema\.generated\.md \| schema\.generated\.md \| unknown \|")
def repl(m):
    col=m.group('col').strip()
    # humanize
    meaning = col.replace('_',' ').strip().capitalize()
    return f"| `{col}` | {m.group('type').strip()} | {m.group('null').strip()} | {meaning} (derived) | unknown | schema.generated.md | schema.generated.md | inferred |"
s,newn = pattern.subn(repl,s)
open(p,'w',encoding='utf-8').write(s)
print('replaced',newn)
