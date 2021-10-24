import sys, re
for l in sys.stdin:
    if (l[0] == '#'): continue
    a = l[:-1].split()
    if (len(a) < 2): continue
    k = a[0]
    v = ' '.join(a[1:])
    k = k.replace(".", "_")
    k = "__".join(w.upper() for w in re.findall('[^A-Z]+|[A-Z][^A-Z]*', k))
    k = k.replace("-", "___")
    k = "CFG_" + k
    print('#' + a[0])
    print(k + '=' + v + '\n')
