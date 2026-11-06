"""Discovery probe for Finviz Elite endpoints. Prints page structure only, never secrets."""
from __future__ import annotations
import re, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from src import finviz_session as fs  # noqa: E402

def main():
    sess = fs.session(); print("auth:", sess.headers.get("X-Fullscan-Finviz"))
    if not fs.authed(sess): return
    for t, ty in (("SOUN", "si"), ("SOUN", "di"), ("SOUN", "lf"), ("SOUN", "fc"), ("SOUN", "")):
        r = fs.get(sess, [f"/quote.ashx?t={t}&ty={ty}"], timeout=40); h = r.text if r else ""
        title = re.search(r"<title>(.*?)</title>", h)
        print(f"[probe] ty={ty} title={title.group(1) if title else None}")
        for m in re.finditer(r'<script([^>]*)>', h):
            attrs = m.group(1); end = h.find("</script>", m.end()); body = h[m.end():end]
            if len(body) > 300 and ("{" in body[:50] or "json" in attrs):
                print(f"[probe] ty={ty} script attrs={attrs.strip()[:120]!r} len={len(body)} head={body[:900]!r}")
        for kw in ("sharesShort", "settlement", "shortInterest", "dilution", "shelf", "riskScore", "atm", "runway", "Rating Change", "Insider Trading"):
            i = h.find(kw)
            if i >= 0: print(f"[probe] ty={ty} kw={kw} ctx={h[max(0,i-300):i+700]!r}")

if __name__ == "__main__":
    main()
