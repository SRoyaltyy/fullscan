"""Append-only day locks for updown_v4_preopen (same rule as v3: past rows never rewritten).

python -m research.updown_v4_preopen.lock add     # fingerprint any unlocked prediction day (today's day only before 09:25 ET)
python -m research.updown_v4_preopen.lock check   # fail if any locked day's rows changed or vanished
"""
import gzip, glob, hashlib, json, os, sys, datetime as dt
from zoneinfo import ZoneInfo
import pandas as pd
R = os.path.dirname(os.path.abspath(__file__)); L = os.path.join(R, 'LOCKS.json')

def _rows():
    fs = sorted(glob.glob(os.path.join(R, 'history', '*.csv.gz')))
    df = pd.concat([pd.read_csv(f, dtype=str) for f in fs], ignore_index=True)
    return df[[c for c in df.columns if not c.startswith('y_')]]  # outcomes fill in later; predictions are locked

def _fp(df):
    return {d: hashlib.sha256(g.sort_values(list(g.columns)).to_csv(index=False).encode()).hexdigest()
            for d, g in df.groupby(df.columns[0])}

def main(cmd):
    locks = json.load(open(L)) if os.path.exists(L) else {}
    fp = _fp(_rows())
    bad = [d for d, h in locks.items() if fp.get(d) != h]
    if bad: sys.exit(f'LOCK VIOLATION: locked days changed/missing: {bad[:10]}')
    if cmd == 'add':
        now = dt.datetime.now(ZoneInfo('America/New_York')); today = now.date().isoformat()
        for d in sorted(set(fp) - set(locks)):
            if d == today and now.time() >= dt.time(9, 25): print('skip', d, '(at/after 09:25 ET; draft only)'); continue
            if d > today: continue
            locks[d] = fp[d]
        json.dump(locks, open(L, 'w'), indent=0, sort_keys=True)
    print('ok', len(locks), 'locked days')

if __name__ == '__main__': main(sys.argv[1] if len(sys.argv) > 1 else 'check')
