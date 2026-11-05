p='docs/data/dictionary.md'
lines=open(p,encoding='utf-8').read().splitlines(keepends=True)
# delete duplicate block lines 1964..1997 (1-indexed)
start=1963
end=1997
del lines[start:end]
open(p,'w',encoding='utf-8').write(''.join(lines))
print('done', len(lines))
