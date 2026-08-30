"""Human viewer for grids/*.json -> one browsable HTML page (Excel-style colors).

  python engine/view_grids.py                     # latest 150 tickers
  python engine/view_grids.py --tickers AAPL MSFT # specific tickers
  python engine/view_grids.py --all               # everything (big file!)

Writes outputs/grid_browser.html — open it in any browser, pick a ticker.
"""
import argparse
import glob
import json
import os
from datetime import datetime, timedelta

HEADER = ["Date", "Close", "Open", "High", "Low", "Volume", "Vol ratio:",
          "Intraday:", "Daily:", "Range:", "Body/Candle:", "Boomerang?",
          "Yesterday?", "Volume?", "SPY"]


def s2d(n):
    return (datetime(1899, 12, 30) + timedelta(days=float(n))).date()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", nargs="*")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--limit", type=int, default=150)
    args = ap.parse_args()

    files = sorted(glob.glob("grids/*.json"), key=os.path.getmtime, reverse=True)
    files = [f for f in files if not f.endswith("_failed.json")]
    if args.tickers:
        want = {t.upper() for t in args.tickers}
        files = [f for f in files
                 if os.path.basename(f)[:-5].upper() in want]
    elif not args.all:
        files = files[:args.limit]

    data = {}
    for f in files:
        g = json.load(open(f))
        days = []
        for d in g["days"]:
            days.append({"d": str(s2d(d["date"])),
                         "v": [d["close"], d["open"], d["high"], d["low"],
                               d["volume"]],
                         "f": d["fills"]})
        data[g["ticker"]] = days

    payload = json.dumps(data, separators=(",", ":"))
    html = HTML_TMPL.replace("__DATA__", payload).replace(
        "__HEADER__", json.dumps(HEADER))
    os.makedirs("outputs", exist_ok=True)
    out = "outputs/grid_browser.html"
    open(out, "w", encoding="utf-8").write(html)
    print(f"{len(data)} tickers -> {out} ({os.path.getsize(out)//1024} KB)")


HTML_TMPL = """<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Grid browser</title><style>
body { font-family: Calibri, Arial, sans-serif; background:#222; color:#eee;
       padding:16px; }
select { font-size:15px; padding:4px 8px; margin-bottom:12px; }
table { border-collapse: collapse; font-size: 12px; }
td { border: 1px solid #999; padding: 1px 6px; text-align: right; color:#111;
     background:#fff; white-space: nowrap; }
td.h { background:#ddd; font-weight:bold; }
td.d { text-align:left; }
#meta { margin: 0 0 10px 8px; display:inline-block; color:#aaa; }
</style></head><body>
<select id="tk" onchange="render()"></select>
<span id="meta"></span>
<div id="out"></div>
<script>
const DATA = __DATA__;
const HEADER = __HEADER__;
const sel = document.getElementById('tk');
Object.keys(DATA).sort().forEach(t => {
  const o = document.createElement('option'); o.value = o.text = t;
  sel.appendChild(o);
});
function fmt(x, col) {
  if (x === null || x === undefined) return '';
  if (typeof x === 'string') return x;
  if (col === 0) return x;                       // date string already
  if (col === 5) return Math.round(x).toLocaleString('en-US');
  if (Math.abs(x) >= 100) return x.toFixed(2);
  if (Math.abs(x) >= 1) return (Math.round(x*1000)/1000).toString();
  return (Math.round(x*100000)/100000).toString();
}
function render() {
  const days = DATA[sel.value];
  document.getElementById('meta').textContent =
    days.length + ' days, ' + days[0].d + ' -> ' + days[days.length-1].d;
  let h = '<table><tr>' + HEADER.map(x =>
    '<td class="h d">'+x+'</td>').join('') + '</tr>';
  // row 0: header; then day rows. Columns: A date, B-F ohlcv, G-O extra
  // (G-O have no values in grid files - they hold colors only, shown blank)
  days.forEach(day => {
    h += '<tr>';
    const fills = day.f;
    const vals = [day.d, ...day.v];
    for (let c = 0; c < 15; c++) {
      const fill = fills[c] ? ' style="background:#'+fills[c]+'"' : '';
      const txt = c < 6 ? fmt(vals[c], c) : '';
      h += '<td'+fill+'>'+txt+'</td>';
    }
    h += '</tr>';
  });
  document.getElementById('out').innerHTML = h + '</table>';
}
render();
</script></body></html>"""

if __name__ == "__main__":
    main()
