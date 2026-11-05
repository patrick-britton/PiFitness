p='docs/data/dictionary.md'
lines=open(p,encoding='utf-8').read().splitlines(keepends=True)
# delete duplicate Unknown block lines 1870..1933 (1-indexed)
start=1869
end=1933
del lines[start:end]
open(p,'w',encoding='utf-8').write(''.join(lines))
print('done', len(lines))
