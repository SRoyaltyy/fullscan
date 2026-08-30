"""Excel value model: errors, coercions, comparisons, array helpers."""
import math


class Err:
    """An Excel error value (propagates as a value, not an exception)."""
    __slots__ = ("code",)

    def __init__(self, code):
        self.code = code

    def __repr__(self):
        return f"Err({self.code})"

    def __eq__(self, other):
        return isinstance(other, Err) and other.code == self.code

    def __hash__(self):
        return hash(self.code)


E_VALUE = Err("#VALUE!")
E_REF = Err("#REF!")
E_NA = Err("#N/A")
E_DIV0 = Err("#DIV/0!")
E_NAME = Err("#NAME?")
E_NUM = Err("#NUM!")
E_CALC = Err("#CALC!")
E_NULL = Err("#NULL!")

ERROR_STRINGS = {"#VALUE!": E_VALUE, "#REF!": E_REF, "#N/A": E_NA,
                 "#DIV/0!": E_DIV0, "#NAME?": E_NAME, "#NUM!": E_NUM,
                 "#NULL!": E_NULL, "#CALC!": E_CALC, "#SPILL!": Err("#SPILL!")}


def is_err(x):
    return isinstance(x, Err)


def is_arr(x):
    return isinstance(x, list)  # arrays are list-of-rows (each row a list)


def arr_shape(a):
    return len(a), len(a[0]) if a else 0


def flatten(a):
    if not is_arr(a):
        yield a
        return
    for row in a:
        for v in row:
            yield v


def to_array(x):
    return x if is_arr(x) else [[x]]


def first(x):
    """Top-left element (single-cell CSE display semantics)."""
    return x[0][0] if is_arr(x) else x


def is_blank(x):
    return x is None


# ------------------------------------------------------------- coercions ---
def to_num(x, blank_as_zero=True):
    """Coerce to float with Excel arithmetic semantics."""
    if isinstance(x, Err):
        return x
    if x is None:
        return 0.0 if blank_as_zero else E_VALUE
    if isinstance(x, bool):
        return 1.0 if x else 0.0
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if s == "":
            return E_VALUE
        try:
            return float(s.replace(",", ""))
        except ValueError:
            return E_VALUE
    return E_VALUE


def to_text(x):
    """Excel & concatenation / text coercion."""
    if isinstance(x, Err):
        return x
    if x is None:
        return ""
    if isinstance(x, bool):
        return "TRUE" if x else "FALSE"
    if isinstance(x, float):
        if x == int(x) and abs(x) < 1e15:
            return str(int(x))
        # Excel "General" format: up to 15 significant digits
        s = format(x, ".15g")
        return s
    return str(x)


def to_bool(x):
    """Condition coercion for IF etc."""
    if isinstance(x, Err):
        return x
    if x is None:
        return False
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return x != 0
    if isinstance(x, str):
        u = x.upper()
        if u == "TRUE":
            return True
        if u == "FALSE":
            return False
        return E_VALUE
    return E_VALUE


# ------------------------------------------------------------ comparison ---
def _sort_key(x):
    """Excel sort order: numbers < text (case-insens) < FALSE < TRUE; blank last."""
    if x is None:
        return (4, 0)
    if isinstance(x, bool):
        return (3, 0 if not x else 1)
    if isinstance(x, (int, float)):
        return (1, x)
    if isinstance(x, str):
        return (2, x.upper())
    return (4, 0)


def compare(op, a, b):
    """Excel comparison semantics. Returns bool or Err."""
    if isinstance(a, Err):
        return a
    if isinstance(b, Err):
        return b
    # blank normalization: blank == 0, blank == "", blank == FALSE
    if a is None and b is None:
        av = bv = None
    elif a is None:
        a = _blank_as(b)
    elif b is None:
        b = _blank_as(a)
    if op in ("=", "<>"):
        eq = _eq(a, b)
        return eq if op == "=" else not eq
    ka, kb = _sort_key(a), _sort_key(b)
    if ka[0] != kb[0]:
        c = -1 if ka[0] < kb[0] else 1
    elif ka[0] == 2:
        c = (ka[1] > kb[1]) - (ka[1] < kb[1])
    else:
        c = (ka[1] > kb[1]) - (ka[1] < kb[1])
    return {"<": c < 0, "<=": c <= 0, ">": c > 0, ">=": c >= 0}[op]


def _blank_as(other):
    if isinstance(other, str):
        return ""
    if isinstance(other, bool):
        return False
    return 0


def _eq(a, b):
    if a is None and b is None:
        return True
    if isinstance(a, bool) or isinstance(b, bool):
        # bool only equals bool (after blank normalization)
        return isinstance(a, bool) and isinstance(b, bool) and a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    if isinstance(a, str) and isinstance(b, str):
        return a.upper() == b.upper()
    return False  # cross-type: number <> text


# ------------------------------------------------------------- broadcast ---
def broadcast_bin(fn, a, b):
    """Elementwise binary op. Scalars and 1x1 arrays broadcast; shapes must match."""
    if is_arr(a) and arr_shape(a) == (1, 1):
        a = a[0][0]
    if is_arr(b) and arr_shape(b) == (1, 1):
        b = b[0][0]
    if is_arr(a) and is_arr(b):
        ra, ca = arr_shape(a)
        rb, cb = arr_shape(b)
        rows, cols = max(ra, rb), max(ca, cb)
        out = []
        for i in range(rows):
            row = []
            for j in range(cols):
                x = a[i][j] if i < ra and j < ca else E_NA
                y = b[i][j] if i < rb and j < cb else E_NA
                row.append(fn(x, y) if not isinstance(x, Err) and not isinstance(y, Err)
                           else (x if isinstance(x, Err) else y))
            out.append(row)
        return out
    if is_arr(a):
        return [[fn(x, b) if not isinstance(x, Err) else x for x in row] for row in a]
    if is_arr(b):
        return [[fn(a, y) if not isinstance(y, Err) else y for y in row] for row in b]
    return fn(a, b)


def elementwise_un(fn, a):
    if is_arr(a):
        return [[fn(x) if not isinstance(x, Err) else x for x in row] for row in a]
    return fn(a)


# --------------------------------------------------------------- criteria --
import re as _re


def make_criteria(crit):
    """Compile COUNTIF/SUMIF/MATCH-style criteria into a predicate."""
    if isinstance(crit, Err):
        return lambda v: crit
    if is_arr(crit):
        crit = first(crit)
    if isinstance(crit, str):
        m = _re.match(r"^(<=|>=|<>|=|<|>)(.*)$", crit)
        if m:
            op, rhs = m.group(1), m.group(2)
            rhs_v = _parse_crit_value(rhs)
            return lambda v: _crit_compare(op, v, rhs_v)
        # plain text: wildcard match (case-insensitive), or number-as-text equality
        if "*" in crit or "?" in crit:
            pat = _wildcard_re(crit)
            return lambda v: isinstance(v, str) and bool(pat.match(v))
        num = _try_float(crit)
        if num is not None:
            return lambda v: isinstance(v, (int, float)) and not isinstance(v, bool) and v == num
        return lambda v: isinstance(v, str) and v.upper() == crit.upper()
    if crit is None:
        return lambda v: v is None or v == 0
    return lambda v: _eq(v, crit) if not isinstance(v, Err) else False


def _parse_crit_value(s):
    s = s.strip()
    if s == "":
        return None
    num = _try_float(s)
    if num is not None:
        return num
    return s


def _try_float(s):
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _crit_compare(op, v, rhs):
    if isinstance(v, Err):
        return False
    if rhs is None:
        # "<>""" means not empty; "=""" means empty
        is_blank = v is None or v == ""
        return {("<>", ): not is_blank, ("=", ): is_blank}.get((op,), False)
    if isinstance(rhs, (int, float)) and not isinstance(rhs, bool):
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            return False
        return {"<": v < rhs, "<=": v <= rhs, ">": v > rhs,
                ">=": v >= rhs, "=": v == rhs, "<>": v != rhs}[op]
    # text rhs with comparison operators (lexicographic, case-insensitive)
    if isinstance(v, str):
        c = (v.upper() > rhs.upper()) - (v.upper() < rhs.upper())
        return {"<": c < 0, "<=": c <= 0, ">": c > 0,
                ">=": c >= 0, "=": c == 0, "<>": c != 0}[op]
    if op == "<>":
        return True
    return False


def _wildcard_re(pat):
    out = []
    for ch in pat:
        if ch == "*":
            out.append(".*")
        elif ch == "?":
            out.append(".")
        elif ch == "~":
            continue
        else:
            out.append(_re.escape(ch))
    return _re.compile("^" + "".join(out) + "$", _re.IGNORECASE)
