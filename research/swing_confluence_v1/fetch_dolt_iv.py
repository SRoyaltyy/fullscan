"""python -m research.swing_confluence_v1.fetch_dolt_iv <YYYY-MM-DD> <outdir>  (free DoltHub SQL API; one file per date, resumable)
Queries volatility_history by date (fast; per-symbol range scans time out) and pages on act_symbol.
A row dated D is EOD (no capture time) -> usable only for the D+1 pre-open."""
import sys, os, json, time, urllib.parse, urllib.request
day, out = sys.argv[1:3]; f = f'{out}/{day}.json'
if os.path.exists(f): sys.exit()
URL = 'https://www.dolthub.com/api/v1alpha1/post-no-preference/options/master?q='
rows, last = [], ''
for _ in range(20):
    q = (f"select date,act_symbol,iv_current,iv_week_ago,iv_month_ago,iv_year_high,iv_year_low,hv_current from volatility_history "
         f"where date='{day}' and act_symbol>'{last}' order by act_symbol limit 400")
    for a in range(5):
        try:
            d = json.load(urllib.request.urlopen(URL + urllib.parse.quote(q), timeout=90))
            if d.get('query_execution_status') == 'Error': raise RuntimeError(d.get('query_execution_message'))
            break
        except Exception: time.sleep(4 * (a + 1))
    else: sys.exit(1)
    r = d.get('rows') or []; rows += r
    if not r: break
    last = r[-1]['act_symbol']
json.dump(rows, open(f + '.tmp', 'w')); os.replace(f + '.tmp', f)
