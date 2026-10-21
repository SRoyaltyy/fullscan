"""python -m research.swing_confluence_v1.fetch_earnings <tickers.txt> <out.csv>  (free yfinance; checkpointed, resumable)
Scheduled earnings dates are a CONTROL only (they are announced weeks ahead); never a signal."""
import sys, os, time, pandas as pd, yfinance as yf
src, out = sys.argv[1:3]
T = [t.strip() for t in open(src) if t.strip()]
done = set(pd.read_csv(out).ticker) if os.path.exists(out) else set()
for i, t in enumerate(T):
    if t in done: continue
    rows = []
    try:
        d = yf.Ticker(t).get_earnings_dates(limit=60)
        if d is not None:
            for ts in d.index:
                ts = pd.Timestamp(ts).tz_convert('America/New_York')
                rows.append((t, ts.date().isoformat(), int(ts.hour >= 12)))   # amc=1: after close -> move shows next session
    except Exception as e:
        print(t, 'ERR', str(e)[:80], flush=True)
    if not rows: rows = [(t, '', -1)]
    pd.DataFrame(rows, columns=['ticker', 'edate', 'amc']).to_csv(out, mode='a', header=not os.path.exists(out), index=False)
    if i % 50 == 0: print(i, len(T), flush=True)
    time.sleep(0.3)
print('done', flush=True)
