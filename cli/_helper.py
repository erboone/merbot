from warnings import warn

def parse_kvpairs(raw_kvpairs:list[str]) -> dict:
    di = {}
    for kv in raw_kvpairs:
        splt = kv.split('=', 1)
        k = splt[0]
        v = splt[1]

        try:
            v = int(v)
        except:
            pass

        di[k] = v

    return di