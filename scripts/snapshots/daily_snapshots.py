"""Add-only daily data snapshots (research only, nothing to Webull).

Writes ONLY under data/snapshots/<source>/<YYYY-MM-DD>/ ; never overwrites an
existing file. Every row carries captured_at_utc (capture time, not market date).
Sources:
  finviz  : Elite screener exports v=111,121,131,141,161,171 (full universe)
  yahoo   : option chains (nearest N expiries) for top-K names by volume
Usage: python scripts/snapshots/daily_snapshots.py [--finviz] [--yahoo] [--top 300]
"""
from __future__ import annotations
import argparse, csv, gzip, io, os, sys, time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "snapshots"
VIEWS = {"111": "overview", "121": "valuation", "131": "ownership",
         "141": "performance", "161": "financial", "171": "technical"}


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def day_dir(src: str) -> Path:
    d = OUT / src / datetime.now(timezone.utc).strftime("%Y-%m-%d")
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_rows(path: Path, header: list[str], rows: list[list]) -> bool:
    if path.exists():  # add-only: never overwrite
        print(f"[skip] exists {path}")
        return False
    with gzip.open(path, "wt", newline="") as f:
        w = csv.writer(f); w.writerow(header); w.writerows(rows)
    print(f"[ok] {path} rows={len(rows)}")
    return True


def finviz() -> Path | None:
    sys.path.insert(0, str(ROOT))
    from src import finviz_session as fs
    sess = fs.session()
    if sess is None or sess.headers.get("X-Fullscan-Finviz") == "skipped":
        print("[finviz] no elite session"); return None
    d = day_dir("finviz"); overview = None
    for v, name in VIEWS.items():
        cap = now_utc()
        r = fs.get(sess, [f"/export.ashx?v={v}"], timeout=60)
        txt = getattr(r, "text", "") or ""
        if not txt or txt.lstrip().startswith("<") or fs.looks_like_login_html(txt):
            print(f"[finviz] v={v} empty/login html"); continue
        rd = list(csv.reader(io.StringIO(txt)))
        if len(rd) < 2: continue
        p = d / f"v{v}_{name}.csv.gz"
        write_rows(p, ["captured_at_utc"] + rd[0], [[cap] + x for x in rd[1:]])
        if v == "111": overview = p
        time.sleep(3)
    return overview


def top_tickers(overview: Path | None, k: int) -> list[str]:
    base = ["SPY", "QQQ", "IWM", "DIA", "XLF", "XLE", "XLK", "SMH", "TLT", "GLD"]
    if overview is None:
        prev = sorted((OUT / "finviz").glob("*/v111_overview.csv.gz"))
        overview = prev[-1] if prev else None
    if overview is None: return base[:k]
    with gzip.open(overview, "rt") as f:
        rows = list(csv.DictReader(f))
    def num(x):
        try: return float(str(x).replace(",", ""))
        except Exception: return 0.0
    rows = [r for r in rows if num(r.get("Price")) >= 5]
    rows.sort(key=lambda r: num(r.get("Volume")) * num(r.get("Price")), reverse=True)
    out = base + [r["Ticker"] for r in rows if r.get("Ticker") not in base]
    return out[:k]


def yahoo(tickers: list[str], n_exp: int) -> None:
    import yfinance as yf
    d = day_dir("yahoo_options")
    p = d / "chains.csv.gz"
    if p.exists(): print(f"[skip] exists {p}"); return
    hdr = ["captured_at_utc", "underlying", "underlying_px", "expiration", "type",
           "contractSymbol", "strike", "lastPrice", "bid", "ask", "volume",
           "openInterest", "impliedVolatility", "inTheMoney", "lastTradeDate"]
    rows, fails = [], 0
    for t in tickers:
        try:
            tk = yf.Ticker(t)
            exps = list(tk.options)[:n_exp]
            px = None
            try: px = tk.fast_info.get("lastPrice")
            except Exception: pass
            for e in exps:
                cap = now_utc(); ch = tk.option_chain(e)
                for typ, df in (("C", ch.calls), ("P", ch.puts)):
                    for r in df.itertuples(index=False):
                        g = r._asdict()
                        rows.append([cap, t, px, e, typ] + [g.get(c) for c in hdr[5:]])
                time.sleep(0.4)
        except Exception as ex:  # noqa: BLE001
            fails += 1; print(f"[yahoo] {t}: {ex}")
    print(f"[yahoo] tickers={len(tickers)} fails={fails}")
    if rows: write_rows(p, hdr, rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--finviz", action="store_true"); ap.add_argument("--yahoo", action="store_true")
    ap.add_argument("--top", type=int, default=300); ap.add_argument("--expiries", type=int, default=4)
    a = ap.parse_args()
    ov = finviz() if a.finviz else None
    if a.yahoo: yahoo(top_tickers(ov, a.top), a.expiries)


if __name__ == "__main__":
    main()
