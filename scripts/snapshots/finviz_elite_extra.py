"""Add-only daily Finviz Elite extras (research only, nothing to Webull).

Writes ONLY data/snapshots/finviz_elite/<UTC date>/ ; never overwrites. Fail-soft:
every dataset and every ticker is wrapped; the script always exits 0.
Datasets (all rows carry captured_at_utc = save stamp):
  options_chain.csv.gz        /export/options?t=X&ty=oc (IV + greeks), expiries <= --max-dte days
  short_interest_history.csv.gz  quote ty=si route-init-data (full settlement history; first save = backfill)
  insider_trades.csv.gz       quote page insider table (Sale/Buy/Proposed Sale/Option Exercise ...)
  analyst_ratings.csv.gz      quote page ratings table (date, action, analyst, rating, PT change)
  dilution.jsonl.gz           quote ty=di for caps < $500M (json blobs + tables as found)
  futures.csv.gz              /api/futures_all.ashx (VIX, CL, ZN, GC, BTC ...)
  manifest.json               counts / failures
"""
from __future__ import annotations
import argparse, csv, gzip, html, json, re, sys, time
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts" / "snapshots"))
OUT = ROOT / "data" / "snapshots" / "finviz_elite"
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", s))).strip()


def write_csv(path: Path, header, rows) -> int:
    if path.exists():
        print(f"[skip] exists {path}"); return 0
    if not rows:
        print(f"[empty] {path.name}"); return 0
    with gzip.open(path, "wt", newline="") as f:
        w = csv.writer(f); w.writerow(header); w.writerows(rows)
    print(f"[ok] {path} rows={len(rows)}"); return len(rows)


def json_scripts(h: str) -> dict:
    out = {}
    for m in re.finditer(r'<script[^>]*id="([^"]+)"[^>]*type="application/json"[^>]*>(.*?)</script>', h, re.S):
        if m.group(1).startswith("income-breakdown"):
            continue
        try: out[m.group(1)] = json.loads(m.group(2))
        except Exception: pass
    return out


def tables_after(h: str, marker: str) -> list[list[str]]:
    i = h.find(marker)
    if i < 0: return []
    j = h.find("</table>", i)
    seg = h[i:j]
    rows = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", seg, re.S):
        cells = [clean(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
        if cells: rows.append(cells)
    return rows


def all_tables(h: str) -> list[list[list[str]]]:
    out = []
    for t in re.findall(r"<table[^>]*>(.*?)</table>", h, re.S):
        rows = [[clean(c) for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)]
                for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", t, re.S)]
        rows = [r for r in rows if any(r)]
        if len(rows) >= 2: out.append(rows)
    return out


def universe(top: int, small: int):
    from daily_snapshots import top_tickers, OUT as SOUT
    prev = sorted((SOUT / "finviz").glob("*/v111_overview.csv.gz"))
    ov = prev[-1] if prev else None
    tops = top_tickers(ov, top)
    smalls = []
    if ov:
        def num(x):
            try: return float(str(x).replace(",", ""))
            except Exception: return 0.0
        with gzip.open(ov, "rt") as f:
            rows = [r for r in csv.DictReader(f)
                    if 0 < num(r.get("Market Cap")) < 500 and num(r.get("Price")) >= 1]
        rows.sort(key=lambda r: num(r.get("Volume")) * num(r.get("Price")), reverse=True)
        smalls = [r["Ticker"] for r in rows[:small]]
    return tops, smalls


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=300)
    ap.add_argument("--small", type=int, default=150)
    ap.add_argument("--max-dte", type=int, default=30)
    ap.add_argument("--budget-min", type=float, default=75)
    a = ap.parse_args()
    t0 = time.time(); man = {"date": TODAY, "started": now_utc(), "counts": {}, "fails": {}}
    d = OUT / TODAY; d.mkdir(parents=True, exist_ok=True)
    try:
        from src import finviz_session as fs
        sess = fs.session()
        if not fs.authed(sess):
            print("[elite] no elite session; skip all"); man["blocked"] = "no elite session"; raise SystemExit
        tops, smalls = universe(a.top, a.small)
        print(f"[elite] top={len(tops)} small={len(smalls)}")
        over = lambda: (time.time() - t0) / 60 > a.budget_min

        # futures
        try:
            r = fs.get(sess, ["/api/futures_all.ashx?timeframe=NO"], timeout=30)
            fut = r.json() if r is not None else {}
            cap = now_utc(); cols = ["ticker", "label", "last", "change", "changeUsd", "prevClose", "high", "low"]
            man["counts"]["futures"] = write_csv(d / "futures.csv.gz", ["captured_at_utc"] + cols,
                [[cap] + [v.get(c) for c in cols] for v in fut.values() if isinstance(v, dict)])
        except Exception as e: man["fails"]["futures"] = str(e)[:200]

        # options (export CSV with greeks)
        oc_rows, oc_hdr, fails = [], None, []
        lim = (datetime.now(timezone.utc) + timedelta(days=a.max_dte)).date()
        for t in tops:
            if over(): fails.append("BUDGET"); break
            try:
                r = fs.get(sess, [f"/export/options?t={t}&ty=oc"], timeout=60)
                txt = getattr(r, "text", "") or ""
                rd = list(csv.reader(txt.splitlines()))
                if len(rd) < 2 or "Strike" not in rd[0]: fails.append(t); continue
                if oc_hdr is None: oc_hdr = rd[0]
                ie = rd[0].index("Expiry"); cap = now_utc()
                for x in rd[1:]:
                    try:
                        if datetime.strptime(x[ie], "%m/%d/%Y").date() > lim: continue
                    except Exception: pass
                    oc_rows.append([cap, t] + x)
            except Exception: fails.append(t)
        man["counts"]["options_chain"] = write_csv(d / "options_chain.csv.gz",
            ["captured_at_utc", "underlying"] + (oc_hdr or []), oc_rows)
        man["fails"]["options_chain"] = fails[:50]; man["counts"]["options_tickers"] = len(tops) - len(fails)

        # short interest history + quote page (insider, ratings)
        si_rows, si_keys, ins, rat, fails = [], None, [], [], []
        for t in tops:
            if over(): fails.append("BUDGET"); break
            try:
                r = fs.get(sess, [f"/quote.ashx?t={t}&ty=si"], timeout=60)
                js = json_scripts(r.text if r is not None else "").get("route-init-data")
                if isinstance(js, list) and js:
                    if si_keys is None: si_keys = [k for k in js[0].keys() if k != "ticker"]
                    cap = now_utc()
                    for x in js:
                        ts = x.get("timestamp")
                        sd = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d") if ts else ""
                        si_rows.append([cap, t, sd] + [x.get(k) for k in si_keys])
                else: fails.append(f"si:{t}")
            except Exception: fails.append(f"si:{t}")
            try:
                r = fs.get(sess, [f"/quote.ashx?t={t}"], timeout=60)
                h = r.text if r is not None else ""; cap = now_utc()
                for row in tables_after(h, ">Insider Trading</th>"):
                    ins.append([cap, t] + row[:9])
                for row in tables_after(h, ">Rating Change</th>"):
                    rat.append([cap, t] + row[:5])
            except Exception: fails.append(f"q:{t}")
        man["counts"]["short_interest_history"] = write_csv(d / "short_interest_history.csv.gz",
            ["captured_at_utc", "ticker", "settlement_date"] + (si_keys or []), si_rows)
        man["counts"]["insider_trades"] = write_csv(d / "insider_trades.csv.gz",
            ["captured_at_utc", "ticker", "insider", "relationship", "date", "transaction", "cost",
             "shares", "value_usd", "shares_total", "sec_form4"], ins)
        man["counts"]["analyst_ratings"] = write_csv(d / "analyst_ratings.csv.gz",
            ["captured_at_utc", "ticker", "date", "action", "analyst", "rating_change", "price_target_change"], rat)
        man["fails"]["si_quote"] = fails[:50]

        # dilution (< $500M caps)
        p = d / "dilution.jsonl.gz"; n = 0; fails = []
        if not p.exists():
            with gzip.open(p, "wt") as f:
                for t in smalls:
                    if over(): fails.append("BUDGET"); break
                    try:
                        r = fs.get(sess, [f"/quote.ashx?t={t}&ty=di"], timeout=60)
                        h = r.text if r is not None else ""
                        i = h.find('id="dilution'); i = i if i >= 0 else h.find("Dilution")
                        js = json_scripts(h)
                        rec = {"captured_at_utc": now_utc(), "ticker": t, "json": js,
                               "tables": [] if js else [tb for tb in all_tables(h) if len(tb) < 400][:20]}
                        if not rec["json"] and not rec["tables"]: fails.append(t)
                        f.write(json.dumps(rec) + "\n"); n += 1
                    except Exception: fails.append(t)
            print(f"[ok] {p} rows={n}")
        man["counts"]["dilution_tickers"] = n; man["fails"]["dilution_empty"] = fails[:50]
    except SystemExit: pass
    except Exception as e:  # noqa: BLE001
        man["fatal"] = str(e)[:300]; print(f"[elite] fatal (soft): {e}")
    man["finished"] = now_utc()
    mp = d / "manifest.json"
    if not mp.exists(): mp.write_text(json.dumps(man, indent=1))
    print(json.dumps(man["counts"]))


if __name__ == "__main__":
    main()
