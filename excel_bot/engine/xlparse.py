"""Tokenizer + Pratt parser for Excel formulas -> AST.

AST nodes are tuples:
  ("num", v) ("str", s) ("bool", b) ("err", code) ("empty",)
  ("ref", sheet, ext, col, row, abs_col, abs_row)          single cell
  ("range", ref1, ref2)                                    ref pair (nodes)
  ("call", name, [args...])
  ("bin", op, l, r) ("un", op, x) ("pct", x)
  ("array", [[row elements], ...])                          array literal
"""
import os
import pickle
import re

ERRORS = ("#NULL!", "#DIV/0!", "#VALUE!", "#REF!", "#NAME?", "#NUM!", "#N/A",
          "#GETTING_DATA", "#SPILL!", "#CALC!", "#FIELD!", "#BLOCKED!", "#UNKNOWN!")

TOKEN_RE = re.compile(r"""
    (?P<ws>\s+)
  | (?P<err>\#[A-Z0-9/]+[!?]?)
  | (?P<str>"(?:[^"]|"")*")
  | (?P<extref>\[\d+\](?:'[^']+'|[A-Za-z0-9_. ]+)!\$?[A-Z]{1,3}\$?\d+(?::\$?[A-Z]{1,3}\$?\d+)?)
  | (?P<extcol>\[\d+\](?:'[^']+'|[A-Za-z0-9_. ]+)!\$?[A-Z]{1,3}(?::\$?[A-Z]{1,3})?)
  | (?P<sheetref>(?:'[^']+'|[A-Za-z_][A-Za-z0-9_.]*)!\$?[A-Z]{1,3}\$?\d+(?::\$?[A-Z]{1,3}\$?\d+)?)
  | (?P<colrange>\$?[A-Z]{1,3}:\$?[A-Z]{1,3})
  | (?P<rowrange>\$?\d+:\$?\d+)
  | (?P<cell>\$?[A-Z]{1,3}\$?\d+)
  | (?P<num>\d+\.?\d*(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?)
  | (?P<name>[A-Za-z_\\][A-Za-z0-9_.\\]*)
  | (?P<op><=|>=|<>|[-+*/^&=<>(),;:{}%])
""", re.VERBOSE)

# precedence (higher binds tighter)
BINPREC = {"=": 10, "<>": 10, "<": 10, "<=": 10, ">": 10, ">=": 10,
           "&": 20, "+": 30, "-": 30, "*": 40, "/": 40, "^": 50}


class ParseError(Exception):
    pass


def tokenize(s):
    toks = []
    pos = 0
    for m in TOKEN_RE.finditer(s):
        if m.start() != pos:
            raise ParseError(f"cannot tokenize at {pos}: {s[pos:pos+15]!r}")
        pos = m.end()
        if m.lastgroup == "ws":
            continue
        toks.append((m.lastgroup, m.group()))
    if pos != len(s):
        raise ParseError(f"cannot tokenize at {pos}: {s[pos:pos+15]!r}")
    return toks


def split_ref(text):
    """'Sheet1'!$A$1 or $A$1 -> (sheet, col, row, abscol, absrow)"""
    sheet = None
    ext = None
    if "!" in text:
        sheetpart, text = text.rsplit("!", 1)
        m = re.match(r"^\[(\d+)\](.*)$", sheetpart)
        if m:
            ext = int(m.group(1))
            sheetpart = m.group(2)
        sheet = sheetpart.strip("'")
    m = re.match(r"^(\$?)([A-Z]{1,3})(\$?)(\d+)$", text)
    return sheet, ext, m.group(2), int(m.group(4)), m.group(1) == "$", m.group(3) == "$"


class Parser:
    def __init__(self, text):
        if text.startswith("="):
            text = text[1:]
        self.raw = [t for t in tokenize(text) if t[0] != "ws"]
        self.pos = 0

    def peek(self):
        return self.raw[self.pos] if self.pos < len(self.raw) else (None, None)

    def next(self):
        t = self.peek()
        self.pos += 1
        return t

    def expect(self, val):
        k, v = self.next()
        if v != val:
            raise ParseError(f"expected {val!r} got {v!r}")

    # ------------------------------------------------------------ grammar --
    def parse(self):
        node = self.expr(0)
        if self.pos != len(self.raw):
            raise ParseError(f"trailing tokens: {self.raw[self.pos:self.pos+4]}")
        return node

    def expr(self, minprec):
        node = self.unary()
        while True:
            k, v = self.peek()
            if k == "op" and v in BINPREC and BINPREC[v] >= minprec:
                prec = BINPREC[v]
                self.next()
                # ^ is right-associative
                rhs = self.expr(prec if v == "^" else prec + 1)
                node = ("bin", v, node, rhs)
            else:
                break
        return node

    def unary(self):
        k, v = self.peek()
        if k == "op" and v in ("+", "-"):
            self.next()
            return ("un", v, self.unary())
        node = self.postfix()
        return node

    def postfix(self):
        node = self.primary()
        while True:
            k, v = self.peek()
            if k == "op" and v == "%":
                self.next()
                node = ("pct", node)
            elif k == "op" and v == ":":
                # range join (col:col / row:row already single tokens)
                self.next()
                rhs = self.primary()
                node = ("range", node, rhs)
            else:
                break
        return node

    def primary(self):
        k, v = self.next()
        if k == "num":
            return ("num", float(v))
        if k == "str":
            return ("str", v[1:-1].replace('""', '"'))
        if k == "err":
            return ("err", v)
        if k == "bool":
            return ("bool", v == "TRUE")
        if k == "extref" or k == "sheetref":
            if ":" in v.split("!")[-1]:
                a, b = v.rsplit("!", 1)
                c1, c2 = b.split(":")
                r1 = self._mkref(a + "!" + c1)
                r2 = self._mkref(a + "!" + c2)
                return ("range", r1, r2)
            return self._mkref(v)
        if k == "cell":
            return self._mkref(v)
        if k == "colrange":
            c1, c2 = v.split(":")
            return ("colrange", c1.replace("$", ""), c2.replace("$", ""))
        if k == "rowrange":
            r1, r2 = v.split(":")
            return ("rowrange", int(r1.replace("$", "")), int(r2.replace("$", "")))
        if k == "extcol":
            a, b = v.rsplit("!", 1)
            c1, c2 = (b.split(":") + [b.split(":")[0]])[:2]
            m = re.match(r"^\[(\d+)\](.*)$", a)
            ext = int(m.group(1)) if m else None
            sheet = (m.group(2) if m else a).strip("'")
            return ("extcolrange", ext, sheet, c1.replace("$", ""), c2.replace("$", ""))
        if k == "name":
            u = v.upper()
            if u in ("TRUE", "FALSE"):
                return ("bool", u == "TRUE")
            if self.peek()[1] == "(":
                self.next()  # consume (
                args = []
                if self.peek()[1] != ")":
                    while True:
                        # empty argument (e.g. IF(x,,y))
                        if self.peek()[1] in (",", ")"):
                            args.append(("empty",))
                        else:
                            args.append(self.expr(0))
                        if self.peek()[1] == ",":
                            self.next()
                            continue
                        break
                self.expect(")")
                name = v
                for pfx in ("_xlfn._xlws.", "_xlfn.", "_xlws."):
                    if name.upper().startswith(pfx.upper()):
                        name = name[len(pfx):]
                        break
                if name.startswith("_xlpm."):
                    name = name[6:]
                return ("call", name.upper(), args)
            # bare name: LET parameter or defined name
            if v.startswith("_xlpm."):
                v = v[6:]
            return ("name", v)
        if k == "op" and v == "(":
            node = self.expr(0)
            self.expect(")")
            return node
        if k == "op" and v == "{":
            rows = []
            while True:
                row = [self.expr(0)]
                while self.peek()[1] == ",":
                    self.next()
                    row.append(self.expr(0))
                rows.append(row)
                if self.peek()[1] == ";":
                    self.next()
                    continue
                break
            self.expect("}")
            return ("array", rows)
        raise ParseError(f"unexpected token {k}:{v}")

    def _mkref(self, text):
        sheet, ext, col, row, abscol, absrow = split_ref(text)
        return ("ref", sheet, ext, col, row, abscol, absrow)


_PARSE_CACHE = {}
_CACHE_LOADED = False
CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          ".parse_cache.pkl")


def _load_disk_cache():
    """Load pre-warmed ASTs from disk once per process (read-only). The file
    is generated by engine/warm_parse_cache.py; if missing/stale, parsing
    simply happens on demand as before."""
    global _CACHE_LOADED
    if _CACHE_LOADED:
        return
    _CACHE_LOADED = True
    try:
        with open(CACHE_FILE, "rb") as fh:
            _PARSE_CACHE.update(pickle.load(fh))
    except OSError:
        pass


def parse(text):
    """Parse formula text to AST. Results are memoized globally by text --
    parse() is a pure function, and the same formulas are re-parsed for
    every trading day otherwise (dominant engine cost)."""
    if not _CACHE_LOADED:
        _load_disk_cache()
    ast = _PARSE_CACHE.get(text)
    if ast is None:
        ast = Parser(text).parse()
        _PARSE_CACHE[text] = ast
    return ast
