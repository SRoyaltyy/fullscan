"""Discovery probe for Finviz Elite endpoints. Prints URL shapes / headers only, never secrets."""
from __future__ import annotations
import re, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from src import finviz_session as fs  # noqa: E402

def show(sess, path, n=200):
    r = fs.get(sess, [path], timeout=40)
    if r is None:
        print(f"[probe] {path} -> None"); return ""
    t = r.text or ""
    print(f"[probe] {path} -> {r.status_code} ct={r.headers.get('content-type')} len={len(t)} head={t[:n]!r}")
    return t

def scan(tag, t):
    print(f"[probe] {tag} links:", sorted(set(re.findall(r'["\'](/?[a-z_\-/]*(?:export|api)[^"\' <>]{0,140})', t)))[:60])
    print(f"[probe] {tag} ty:", sorted(set(re.findall(r'[?&;]ty=([a-z0-9_]+)', t))))
    print(f"[probe] {tag} keys:", sorted(set(re.findall(r'"([a-zA-Z][a-zA-Z0-9]{2,30})":', t)))[:200])
    print(f"[probe] {tag} th:", sorted(set(re.findall(r'<th[^>]*>([^<]{2,40})<', t)))[:80])

def main():
    sess = fs.session(); print("auth:", sess.headers.get("X-Fullscan-Finviz"))
    if not fs.authed(sess): return
    q = show(sess, "/quote.ashx?t=SOUN"); scan("quote", q)
    for ty in ("oc", "si", "dl", "dilution", "ea"):
        scan(f"ty={ty}", show(sess, f"/quote.ashx?t=SOUN&ty={ty}"))
    scan("futures", show(sess, "/futures.ashx"))
    for p in ("/export/options?t=SOUN&ty=oc", "/export/options?t=SOUN&ty=oc&ov=greeks",
              "/export/options?t=SOUN&ty=ocg", "/export/short-interest?t=SOUN",
              "/api/short-interest?t=SOUN", "/export/dilution?t=SOUN",
              "/export/insidertrading?t=SOUN", "/export/insider?t=SOUN",
              "/export.ashx?v=111&t=SOUN", "/api/futures_all.ashx?timeframe=NO",
              "/export/futures", "/export/ratings?t=SOUN"):
        show(sess, p, 300)

if __name__ == "__main__":
    main()
