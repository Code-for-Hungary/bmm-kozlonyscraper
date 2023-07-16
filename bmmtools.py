import re

def searchstringtofts(searchstring):    
    keresoszo = searchstring.strip()
    keresoszo = re.sub(r'\s+', ' ', keresoszo)
    keresoszo = re.sub(r'([()])', '', keresoszo)
    if keresoszo:
        if not re.search(r'(["+\-~*])', keresoszo):
            keresoszo = re.sub(r'([\s])', ' +', keresoszo) + '*'

    return keresoszo

def mnvtimestamp(tstamp):
    return int(tstamp) * 1000