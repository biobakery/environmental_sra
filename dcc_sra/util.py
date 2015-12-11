import re

def reportnum(fname):
    x = re.sub(r'\D+', '', fname)
    if x:
        return int(x)
    else:
        return 0
