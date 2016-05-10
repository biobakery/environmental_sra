import re
import string

def float_please(s):
    "s is a string, hopefully containing floats"
    try:
        thefloat = float(re.sub(r'[^.^0-9^+^-]', '', s))
    except ValueError:
        raise ValueError("No numbers found in `%s'"%(s))
    else:
        return thefloat

def halve_list(l):
    half = len(l)/2
    return l[:half], l[half:]

def spacejoin(l):
    return " ".join(l)

def parse_str(s):
    if not hasattr(s, "__iter__"): # it's a string
        tup = re.split(r'[,\s]+', s)
        if len(tup)%2 != 0: # doesn't split in two
            raise ValueError("Unable to split `%s' into coordinates"%(s))
        if len(tup) > 2:
            tup = map(spacejoin, halve_list(tup))
        s = tup
    return map(string.lower, map(str, s))
        
def _is_cardinal(lat, lon):
    return ("s" in lat or "n" in lat) and ("e" in lon or "w" in lon)

def is_cardinal(d):
    """d can be string or 2-tuple of string"""
    lat, lon = parse_str(d)
    return _is_cardinal(lat, lon)

def _reg_cardinal(lat, lon):
    lat_dir = "S" if "s" in lat else "N"
    lon_dir = "W" if "w" in lon else "E"
    return ("%.2f %s"%(float_please(lat), lat_dir),
            "%.2f %s"%(float_please(lon), lon_dir))

def reg_cardinal(d):
    lat, lon = parse_str(d)
    return _reg_cardinal(lat, lon)

def cardinal(d):
    """d can be string or 2-tuple of string or 2-tuple of float"""
    lat, lon = parse_str(d)
    if _is_cardinal(lat, lon):
        return _reg_cardinal(lat, lon)
    
    lat, lon = map(float_please, (lat, lon))
    lat_dir = "S" if lat < 0 else "N"
    lon_dir = "W" if lon < 0 else "E"
    return ("%f %s"%(abs(lat), lat_dir),
            "%f %s"%(abs(lon), lon_dir))
        
