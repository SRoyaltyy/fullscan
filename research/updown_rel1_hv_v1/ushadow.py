#!/usr/bin/env python3
"""updown_rel1_hv_v1 (paper/shadow only) + updown_expmove_v1 (descriptive risk column).

PAPER ONLY - failed multiple-testing haircut (t 2.02 vs 3.37). No broker wiring, no orders.

Commands (see README.md):
  gate      print what is due now (freeze / realize / none) as JSON
  run       missed-day markers -> freeze (if due) -> realize -> dashboard export -> lock check
  check     IRONCLAD lock check (optionally --base-ref REF for append-only prefix check)
  verify    re-score every locked day from its frozen features file and compare to the frozen picks
  dryrun    full freeze for --date into a temp dir (nothing in the repo changes)

Model rule (fixed; any change = new strategy name): both LightGBM models are FROZEN as trained once on L1000 rows dated
2018-03-29..2026-10-01 (before the 2026-10-07 start). Never retrained.
"""
from __future__ import annotations
import argparse, datetime as dt, gzip, hashlib, io, json, os, shutil, sys, tempfile, time
from pathlib import Path
from zoneinfo import ZoneInfo
import numpy as np, pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
NAME = 'updown_rel1_hv_v1'
EXPNAME = 'updown_expmove_v1'
LABEL = 'paper only \u2014 failed multiple-testing haircut (t 2.02 vs 3.37)'
START = pd.Timestamp('2026-10-07')
ET = ZoneInfo('America/New_York'); UTC = dt.timezone.utc; HKT = ZoneInfo('Asia/Hong_Kong')
FREEZE_CUTOFF = dt.time(9, 20)     # writer refuses to freeze day N at/after 09:20 ET on N
LOCK_DEADLINE = dt.time(9, 25)     # checker: every lock must be stamped before 09:25 ET on its day
PUSH_DEADLINE = dt.time(9, 24)     # workflow drops an unpushed freeze commit at/after this
DATA_READY = dt.time(17, 15)       # earliest freeze for N: 17:15 ET on the previous session (final daily bars)
REALIZE_FROM = dt.time(9, 45)      # nothing is written between 09:20 and 09:45 ET on a session day
FEES_BP = (5, 10, 15)
RANDOM_SEED, RANDOM_DRAWS = 20260813, 1000
KILL = dict(min_days=60, early_min_days=20, early_t=-2.5)
PROMOTE = dict(min_days=120, t_bar=3.37)

# NYSE full-day closures (past sessions come from SPY bars; this list only decides future sessions / today)
HOLIDAYS = {pd.Timestamp(d) for d in [
    '2025-01-01', '2025-01-09', '2025-01-20', '2025-02-17', '2025-04-18', '2025-05-26', '2025-06-19', '2025-07-04', '2025-09-01',
    '2025-11-27', '2025-12-25', '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03', '2026-05-25', '2026-06-19', '2026-07-03',
    '2026-09-07', '2026-11-26', '2026-12-25', '2027-01-01', '2027-01-18', '2027-02-15', '2027-03-26', '2027-05-31', '2027-06-18',
    '2027-07-05', '2027-09-06', '2027-11-25', '2027-12-24']}

F0 = ['ret1', 'ret3', 'ret5', 'ret10', 'ret20', 'ret60', 'ret120', 'ret250', 'o2c_prev', 'c2o_prev', 'range_prev', 'clv_prev', 'vol20',
      'vol60', 'ldv20', 'rvol1', 'rvol5', 'pos20', 'dd250', 'green10', 'o2c_mean10', 'c2o_mean10', 'dow', 'mkt_ret1', 'mkt_ret5', 'breadth1']
NEW = ['on_mean20', 'id_mean20', 'on_mean60', 'id_mean60', 'on_mean250', 'id_mean250', 'seesaw20', 'seesaw250', 'max20', 'min20', 'skew60',
       'pos250', 'dv5_60', 'dvshock1', 'beta60', 'res1', 'res5', 'res20', 'ivol60', 'ind_ret1', 'ind_ret5', 'ind_ret20', 'ind_ret60',
       'rel_ind20', 'rel_ind1', 'sec_ret1', 'sec_ret5', 'sec_ret20', 'sec_ret60', 'rel_sec20', 'rel_sec1', 'tdom', 'tdom_rev', 'dist_qrebal', 'month']
X3 = ['vol5', 'park5', 'park20', 'park60', 'absgap20', 'rng_ratio', 'hl1', 'e_days', 'e_upcoming', 'e_next', 'e_surp', 'vix', 'vix_rel',
      'spy_vol20', 'spy_trend200', 'spy_ret20']
FEATS = F0 + NEW + X3


class Paths:
    def __init__(self, repo: Path):
        self.repo = Path(repo)
        self.root = self.repo / 'research' / NAME
        self.days = self.root / 'days'
        self.locks = self.root / 'LOCKS.jsonl'
        self.missed = self.root / 'MISSED.jsonl'
        self.realized = self.root / 'realized'
        self.exp = self.repo / 'dashboard' / 'updown' / 'data' / 'expmove'
        self.exp_days = self.exp / 'days'
        self.exp_locks = self.exp / 'LOCKS.jsonl'
        self.exp_missed = self.exp / 'MISSED.jsonl'
        self.dash = self.repo / 'dashboard' / 'updown-shadow' / 'data'

    def rel(self, p: Path) -> str:
        return Path(p).resolve().relative_to(self.repo.resolve()).as_posix()


# ---------------------------------------------------------------------------------------------------------------- utils
def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def sha256_file(p: Path) -> str:
    return sha256_bytes(Path(p).read_bytes())


def now_utc() -> dt.datetime:
    o = os.environ.get('USHADOW_NOW')        # tests/dry-runs only
    return dt.datetime.fromisoformat(o).astimezone(UTC) if o else dt.datetime.now(UTC)


def et_at(d: pd.Timestamp, t: dt.time) -> dt.datetime:
    return dt.datetime.combine(pd.Timestamp(d).date(), t, ET)


def is_session(d) -> bool:
    d = pd.Timestamp(d).normalize()
    return d.dayofweek < 5 and d not in HOLIDAYS


def next_session(d) -> pd.Timestamp:
    d = pd.Timestamp(d).normalize() + pd.Timedelta(days=1)
    while not is_session(d):
        d += pd.Timedelta(days=1)
    return d


def prev_session(d) -> pd.Timestamp:
    d = pd.Timestamp(d).normalize() - pd.Timedelta(days=1)
    while not is_session(d):
        d -= pd.Timedelta(days=1)
    return d


def sessions_between(a, b):
    """calendar sessions in [a, b]"""
    out, d = [], pd.Timestamp(a).normalize()
    while d <= pd.Timestamp(b):
        if is_session(d):
            out.append(d)
        d += pd.Timedelta(days=1)
    return out


def ds(d) -> str:
    return pd.Timestamp(d).strftime('%Y-%m-%d')


def write_json(p: Path, obj):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=1, sort_keys=False, allow_nan=False) + '\n')


def clean(x):
    """json-safe floats"""
    if isinstance(x, dict):
        return {k: clean(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [clean(v) for v in x]
    if isinstance(x, (np.floating, float)):
        return None if not np.isfinite(x) else float(round(float(x), 10))
    if isinstance(x, np.integer):
        return int(x)
    return x


# ---------------------------------------------------------------------------------------------------------------- time gate
def due(now: dt.datetime | None = None) -> dict:
    """What may run now. Freeze target N: the next session whose previous session has closed (+data delay), only before
    09:20 ET on N. Between 09:20 and 09:45 ET on a session day nothing is written (keeps clear of the 09:30 path)."""
    now = (now or now_utc()).astimezone(ET)
    today = pd.Timestamp(now.date())
    t = now.time()
    quiet = is_session(today) and FREEZE_CUTOFF <= t < REALIZE_FROM
    if is_session(today) and t < FREEZE_CUTOFF:
        N = today
        ready = now >= et_at(prev_session(today), DATA_READY)
    elif is_session(today) and t < DATA_READY:
        N, ready = None, False
    else:
        N = next_session(today)
        ready = now >= et_at(prev_session(N), DATA_READY)
    return {'now_et': now.strftime('%Y-%m-%d %H:%M:%S %Z'), 'freeze_date': ds(N) if (N is not None and ready and N >= START) else None,
            'quiet': quiet, 'write_ok': not quiet}


# ---------------------------------------------------------------------------------------------------------------- data
def fetch_bars(tickers, period='2y', chunk=250, tries=3, log=print):
    import yfinance as yf
    out, spl, missing = [], [], []
    for i in range(0, len(tickers), chunk):
        ch = list(tickers[i:i + chunk]); ys = [t.replace('.', '-') for t in ch]
        df = None
        for a in range(tries):
            try:
                df = yf.download(ys, period=period, interval='1d', auto_adjust=False, actions=True, group_by='ticker',
                                 threads=True, progress=False, timeout=30)
                if df is not None and not df.empty:
                    break
            except Exception as e:  # noqa
                log(f'  yahoo chunk {i} try {a}: {e}')
            time.sleep(5 * (a + 1))
        if df is None or df.empty:
            missing += ch; continue
        lv0 = set(df.columns.get_level_values(0))
        for t, y in zip(ch, ys):
            if y not in lv0:
                missing.append(t); continue
            s = df[y]
            if 'Stock Splits' in s.columns:
                ss = s['Stock Splits'].fillna(0); ss = ss[ss > 0]
                for d, v in ss.items():
                    spl.append((t, pd.Timestamp(d).tz_localize(None).normalize(), float(v)))
            s = s.dropna(subset=['Open', 'Close'])
            s = s[(s.Open > 0) & (s.Close > 0)]
            if s.empty:
                missing.append(t); continue
            s = s.reset_index().rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
            s['ticker'] = t
            out.append(s[['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']])
        log(f'  yahoo {min(i + chunk, len(tickers))}/{len(tickers)} tickers')
    B = pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=['date', 'ticker', 'open', 'high', 'low', 'close', 'volume'])
    B['date'] = pd.to_datetime(B.date).dt.tz_localize(None).dt.normalize().astype('datetime64[ns]')
    for c in ['open', 'high', 'low', 'close', 'volume']:
        B[c] = B[c].astype('float64')
    S = pd.DataFrame(spl, columns=['ticker', 'date', 'ratio'])
    S['date'] = pd.to_datetime(S.date).astype('datetime64[ns]')
    return B.drop_duplicates(['ticker', 'date'], keep='last').reset_index(drop=True), S.drop_duplicates(), sorted(set(missing))


def parse_earn_json(j) -> list:
    rows = ((j or {}).get('data') or {}).get('rows') or []
    out = []
    for r in rows:
        s = str(r.get('surprise', '')).replace(',', '')
        try:
            s = float(s)
        except Exception:
            s = np.nan
        out.append((str(r['symbol']).replace('/', '.').strip(), s))
    return out


def fetch_earn_day(d, tries=2):
    import requests
    url = f'https://api.nasdaq.com/api/calendar/earnings?date={ds(d)}'
    h = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 'Accept': 'application/json, text/plain, */*'}
    for a in range(tries):
        try:
            r = requests.get(url, headers=h, timeout=15)
            if r.status_code == 200:
                return parse_earn_json(r.json())
        except Exception:
            pass
        time.sleep(1 + a)
    return None


def load_earn_seed(root: Path):
    p = root / 'static' / 'earn_seed.csv.gz'
    if not p.exists():
        return {}, None
    E = pd.read_csv(p, parse_dates=['edate'])
    meta = json.loads((root / 'static' / 'earn_seed_meta.json').read_text())
    return {d: list(zip(g.ticker, g.surp)) for d, g in E.groupby('edate')}, pd.Timestamp(meta['final_through'])


def earnings_for(N, root: Path, live=True, log=print):
    """Events (edate, ticker, surp) for weekdays N-100d..N+30d. Dates older than N-7d come from the committed seed when it
    has them as final (fetched after the fact); newer and future dates come live from Nasdaq, falling back to the seed."""
    seed, final_through = load_earn_seed(root)
    rows, src = [], {'seed_final': 0, 'live': 0, 'seed_fallback': 0, 'missing': 0}
    for d in pd.bdate_range(N - pd.Timedelta(days=100), N + pd.Timedelta(days=30)):
        got = None
        if final_through is not None and d <= final_through and d < N - pd.Timedelta(days=7) and d in seed:
            got, k = seed[d], 'seed_final'
        elif live:
            got = fetch_earn_day(d); k = 'live'
        if got is None and d in seed:
            got, k = seed[d], 'seed_fallback'
        if got is None:
            src['missing'] += 1; continue
        src[k] += 1
        rows += [(d, t, s) for t, s in got]
    E = pd.DataFrame(rows, columns=['edate', 'ticker', 'surp']).drop_duplicates(['edate', 'ticker'])
    E['edate'] = pd.to_datetime(E.edate).astype('datetime64[ns]')
    log(f'  earnings: {len(E)} events, sources {src}')
    return E, src


# ---------------------------------------------------------------------------------------------------------------- features
def _splitf(A, g, S):
    prev = g.date.shift(1); F = np.ones(len(A)); IDX = g.indices
    for t, s in S.sort_values('date').groupby('ticker'):
        if t not in IDX:
            continue
        rows = IDX[t]; sd = s.date.values; suf = np.append(np.cumprod(s.ratio.values[::-1])[::-1], 1.0)
        pdv = prev.values[rows]; k = np.searchsorted(sd, pdv, side='right')
        f = suf[k]; f[pd.isna(pdv)] = np.nan; F[rows] = f
    return F


def build_features(B, S, N, imap, spyvix, E, past_sessions):
    """Feature rows for day N (universe rows only). Uses bars dated < N only; day N gets a placeholder row with no prices.
    Mirrors research code: updown/features.py + features_v2.py, r2/features_r2.py, r2/earn_build.py, r3/build_r3.py."""
    N = pd.Timestamp(N)
    A = B[B.date < N][['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']].copy()
    last = A.groupby('ticker').date.max()
    live = last[last == past_sessions[-1]].index           # names with a bar on the previous session
    ph = pd.DataFrame({'date': N, 'ticker': live})
    A = pd.concat([A, ph], ignore_index=True).sort_values(['ticker', 'date']).reset_index(drop=True)
    for c in ['open', 'high', 'low', 'close', 'volume']:
        A[c] = A[c].astype('float64')
    g = A.groupby('ticker', sort=False); T = A.ticker
    P = pd.DataFrame({'date': A.date, 'ticker': A.ticker})
    pc = g.close.shift(1); po = g.open.shift(1); phh = g.high.shift(1); pl = g.low.shift(1); pv = g.volume.shift(1)
    P['prev_close'] = pc
    P['prev_close_raw'] = pc * _splitf(A, g, S)
    P['gap'] = A.open / pc - 1
    lr = np.log(A.close / g.close.shift(1)); lrp = lr.groupby(T).shift(1)
    P['ret1'] = lrp
    for n in [3, 5, 10, 20, 60, 120, 250]:
        P[f'ret{n}'] = np.log(pc / g.close.shift(n + 1))
    P['o2c_prev'] = pc / po - 1
    P['c2o_prev'] = po / g.close.shift(2) - 1
    P['range_prev'] = (phh - pl) / pc
    P['clv_prev'] = ((pc - pl) / (phh - pl)).where(phh > pl)
    P['vol20'] = lrp.groupby(T).transform(lambda s: s.rolling(20, min_periods=15).std())
    P['vol60'] = lrp.groupby(T).transform(lambda s: s.rolling(60, min_periods=40).std())
    dv = A.close * A.volume; dvp = dv.groupby(T).shift(1)
    P['ldv20'] = np.log1p(dvp.groupby(T).transform(lambda s: s.rolling(20, min_periods=15).mean()))
    vm20 = pv.groupby(T).transform(lambda s: s.rolling(20, min_periods=8).mean())
    P['rvol1'] = pv / vm20
    P['rvol5'] = pv.groupby(T).transform(lambda s: s.rolling(5).mean()) / vm20
    hi20 = phh.groupby(T).transform(lambda s: s.rolling(20, min_periods=15).max())
    lo20 = pl.groupby(T).transform(lambda s: s.rolling(20, min_periods=15).min())
    P['pos20'] = ((pc - lo20) / (hi20 - lo20)).where(hi20 > lo20)
    hi250 = phh.groupby(T).transform(lambda s: s.rolling(250, min_periods=120).max())
    P['dd250'] = pc / hi250 - 1
    green = (A.close > A.open).astype(float).groupby(T).shift(1)
    P['green10'] = green.groupby(T).transform(lambda s: s.rolling(10, min_periods=8).mean())
    o2cser = (A.close / A.open - 1).groupby(T).shift(1)
    P['o2c_mean10'] = o2cser.groupby(T).transform(lambda s: s.rolling(10, min_periods=8).mean())
    P['c2o_mean10'] = P['c2o_prev'].groupby(T).transform(lambda s: s.rolling(10, min_periods=8).mean())
    P['nbars'] = g.cumcount()
    P['dow'] = P.date.dt.dayofweek
    base = (P.nbars >= 60) & (P.prev_close_raw >= 1) & (P.ldv20 >= np.log1p(1e6))
    isN = (P.date == N).values
    # past rows: research universe (needs that day's open); day N: no open is known yet, so no gap filter
    U = (base & P.gap.notna() & np.isfinite(P.gap)) | (base & isN)
    P['U'] = U.values
    # r2: equal-weight market log return per day over the universe
    r_all = np.log(A.close / g.close.shift(1)).clip(-0.5, 0.5)
    mkt = r_all[U & ~isN].groupby(A.date[U & ~isN]).mean()
    s1 = lambda x: x.groupby(T).shift(1)
    roll = lambda x, w, mp, f: getattr(x.groupby(T).rolling(w, min_periods=mp), f)().reset_index(level=0, drop=True)
    r = r_all; on = (A.open / pc - 1).clip(-0.5, 0.5); idr = (A.close / A.open - 1).clip(-0.5, 0.5)
    for w in [20, 60, 250]:
        P[f'on_mean{w}'] = s1(roll(on, w, int(w * .75), 'mean')); P[f'id_mean{w}'] = s1(roll(idr, w, int(w * .75), 'mean'))
    P['seesaw20'] = P.on_mean20 - P.id_mean20; P['seesaw250'] = P.on_mean250 - P.id_mean250
    P['max20'] = s1(roll(r, 20, 15, 'max')); P['min20'] = s1(roll(r, 20, 15, 'min')); P['skew60'] = s1(roll(r, 60, 40, 'skew'))
    hi = s1(roll(A.high, 250, 120, 'max')); lo = s1(roll(A.low, 250, 120, 'min')); P['pos250'] = ((pc - lo) / (hi - lo)).where(hi > lo)
    m5 = s1(roll(dv, 5, 4, 'mean')); m60 = s1(roll(dv, 60, 40, 'mean'))
    P['dv5_60'] = np.log((m5 + 1) / (m60 + 1)); P['dvshock1'] = np.log((s1(dv) + 1) / (m60 + 1))
    m = A.date.map(mkt).astype('float64')
    rm = r * m; Er = roll(r, 60, 40, 'mean'); Em = roll(m, 60, 40, 'mean'); Erm = roll(rm, 60, 40, 'mean'); Em2 = roll(m * m, 60, 40, 'mean')
    beta = ((Erm - Er * Em) / (Em2 - Em * Em)).clip(-3, 5)
    P['beta60'] = s1(beta)
    e = r - s1(beta) * m
    P['res1'] = s1(e); P['res5'] = s1(roll(e, 5, 4, 'sum')); P['res20'] = s1(roll(e, 20, 15, 'sum')); P['ivol60'] = s1(roll(e, 60, 40, 'std'))
    # r3 extras
    on3 = np.log(A.open / pc).clip(-0.5, 0.5)
    hl = np.log(A.high / A.low).clip(0, 1); pk = hl ** 2 / (4 * np.log(2))
    P['vol5'] = s1(roll(r, 5, 4, 'std')); P['park5'] = np.sqrt(s1(roll(pk, 5, 4, 'mean'))); P['park20'] = np.sqrt(s1(roll(pk, 20, 15, 'mean')))
    P['park60'] = np.sqrt(s1(roll(pk, 60, 40, 'mean'))); P['absgap20'] = s1(roll(on3.abs(), 20, 15, 'mean')); P['rng_ratio'] = P.park5 / P.park20
    P['hl1'] = s1(hl)
    # earnings (r2/earn_build.py): per-ticker index over universe rows
    Q = P[P.U][['date', 'ticker']].copy().reset_index(drop=True)
    Q['sidx'] = Q.groupby('ticker').cumcount()
    cal = list(past_sessions) + sessions_between(N, N + pd.Timedelta(days=45))
    cal = np.array(sorted(set(pd.Timestamp(x) for x in cal)), dtype='datetime64[ns]')
    E = E.copy(); E['esess'] = cal[np.minimum(np.searchsorted(cal, E.edate.values), len(cal) - 1)]
    Ep = E.merge(Q.rename(columns={'date': 'esess'}), on=['esess', 'ticker'], how='inner')     # events on past/today universe rows
    QN = Q[Q.date == N][['ticker', 'sidx']]
    prior = Ep[Ep.esess < N].sort_values('esess').groupby('ticker').tail(1)[['ticker', 'sidx', 'surp']].rename(columns={'sidx': 'e_sidx'})
    QN = QN.merge(prior, on='ticker', how='left')
    QN['e_days'] = QN.sidx - QN.e_sidx
    ok = QN.e_days <= 63
    QN['e_surp'] = np.where(ok, QN.surp.clip(-200, 200), np.nan)
    QN['e_days'] = np.where(ok, QN.e_days, np.nan)
    ci = {pd.Timestamp(x): i for i, x in enumerate(cal)}
    fut = E[E.esess >= N].copy(); fut['nx'] = fut.esess.map(ci) - ci[N]
    nxt = fut.groupby('ticker').nx.min()
    QN['e_next'] = QN.ticker.map(nxt)
    QN['e_upcoming'] = ((QN.e_next >= 0) & (QN.e_next <= 5)).astype('float64')
    QN['e_next'] = QN.e_next.where(QN.e_next <= 20)
    # keep day N universe rows
    X = P[isN & P.U.values].copy().reset_index(drop=True)
    X = X.merge(QN[['ticker', 'e_days', 'e_upcoming', 'e_next', 'e_surp']], on='ticker', how='left')
    X['mkt_ret1'] = X.ret1.median(); X['mkt_ret5'] = X.ret5.median(); X['breadth1'] = (X.ret1 > 0).mean()
    X = X.merge(imap, on='ticker', how='left')
    for key, nm in [('Industry', 'ind'), ('Sector', 'sec')]:
        for c in ['ret1', 'ret5', 'ret20', 'ret60']:
            grp = X.groupby(key, observed=True)[c]; sm = grp.transform('sum'); n = grp.transform('count')
            X[f'{nm}_{c}'] = ((sm - X[c]) / (n - 1)).where(n >= 3)
        X[f'rel_{nm}20'] = X.ret20 - X[f'{nm}_ret20']; X[f'rel_{nm}1'] = X.ret1 - X[f'{nm}_ret1']
    # calendar
    mon = [x for x in cal if pd.Timestamp(x).to_period('M') == N.to_period('M')]
    X['tdom'] = [pd.Timestamp(x) for x in mon].index(N) + 1
    X['tdom_rev'] = len(mon) - [pd.Timestamp(x) for x in mon].index(N)

    def third_fri(y, mo):
        x = pd.Timestamp(y, mo, 15); return x + pd.Timedelta(days=(4 - x.dayofweek) % 7)
    pos = np.searchsorted(cal, np.array([third_fri(y, mo) for y in range(2017, 2029) for mo in (3, 6, 9, 12)], dtype='datetime64[ns]'))
    iN = ci[N]; X['dist_qrebal'] = float(min(30, min(abs(iN - p) for p in pos if p < len(cal))))
    X['month'] = N.month
    # SPY / VIX regime from closes strictly before N
    D = spyvix[spyvix.index < N].dropna().copy()
    lrs = np.log(D.spy).diff()
    D['spy_vol20'] = lrs.rolling(20).std() * np.sqrt(252); D['spy_trend200'] = D.spy / D.spy.rolling(200).mean() - 1
    D['spy_ret20'] = D.spy.pct_change(20); D['vix_med252'] = D.vix.rolling(252).median(); D['vix_rel'] = D.vix / D.vix_med252
    lastrow = D.iloc[-1]
    for c in ['vix', 'vix_rel', 'spy_vol20', 'spy_trend200', 'spy_ret20']:
        X[c] = float(lastrow[c])
    X['regime_asof'] = ds(D.index[-1])
    rk = X.ldv20.where(X.prev_close_raw >= 5).rank(ascending=False)
    X['L1000'] = (rk <= 1000).astype('int8')
    return X


# ---------------------------------------------------------------------------------------------------------------- scoring
def load_models(root: Path):
    import lightgbm as lgb
    meta = json.loads((root / 'models' / 'frozen_meta.json').read_text())
    assert meta['features'] == FEATS, 'feature list drift vs frozen_meta.json'
    out = {}
    for k in ['d1_rel_oo1', 'm1_abs1']:
        p = root / 'models' / f'{k}.txt'
        assert sha256_file(p) == meta['models'][k]['sha256'], f'model file {k} changed (frozen model rule)'
        out[k] = lgb.Booster(model_file=str(p))
    return out, meta


def to_f32_frame(X):
    F = X[FEATS].astype('float64').replace([np.inf, -np.inf], np.nan).astype('float32')
    return F


def score_and_pick(F: pd.DataFrame, tickers, models):
    """F: float32 feature frame of the L1000 rows. D8: keep M1 >= median, long top 10% / short bottom 10% of D1 within kept."""
    Xv = F[FEATS].to_numpy(dtype='float32')
    d1 = models['d1_rel_oo1'].predict(Xv); m1 = models['m1_abs1'].predict(Xv)
    S = pd.DataFrame({'ticker': list(tickers), 'd1': d1, 'm1': m1})
    S['m1_pct'] = S.m1.rank(pct=True)
    S['eligible'] = S.m1 >= S.m1.median()
    rk = S.d1.where(S.eligible).rank(pct=True)
    S['side'] = np.where(rk > 0.9, 'long', np.where(rk <= 0.1, 'short', ''))
    nl, ns = (S.side == 'long').sum(), (S.side == 'short').sum()
    S['w'] = np.where(S.side == 'long', 1.0 / max(nl, 1), np.where(S.side == 'short', -1.0 / max(ns, 1), 0.0))
    return S


def f32_csv(F: pd.DataFrame) -> bytes:
    b = io.StringIO(); F.to_csv(b, index=False, float_format='%.9g', lineterminator='\n')
    return gzip.compress(b.getvalue().encode(), mtime=0)


# ---------------------------------------------------------------------------------------------------------------- locks
def read_jsonl(p: Path):
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def append_lock(ledger: Path, entry: dict):
    prev = ledger.read_bytes() if ledger.exists() else b''
    entry = dict(entry); entry['prev_sha256'] = sha256_bytes(prev)
    line = (json.dumps(entry, sort_keys=True, separators=(',', ':')) + '\n').encode()
    ledger.parent.mkdir(parents=True, exist_ok=True)
    with open(ledger, 'ab') as f:
        f.write(line)


class LockError(Exception):
    pass


def check_ledger(paths: Paths, ledger: Path, day_dir: Path | None, base_bytes: bytes | None, missed: Path | None,
                 base_missed: bytes | None, errs: list):
    head = ledger.read_bytes() if ledger.exists() else b''
    if base_bytes is not None and not head.startswith(base_bytes):
        errs.append(f'{paths.rel(ledger)}: not an append-only extension of the base version (past lock lines changed)')
    prev = b''; seen = set(); last = None
    for ln in head.splitlines(keepends=True):
        if not ln.strip():
            continue
        e = json.loads(ln)
        d = e['date']
        if e.get('prev_sha256') != sha256_bytes(prev):
            errs.append(f'{paths.rel(ledger)}: hash chain broken at {d}')
        prev += ln
        if d in seen:
            errs.append(f'{paths.rel(ledger)}: duplicate date {d}')
        if last and d <= last:
            errs.append(f'{paths.rel(ledger)}: dates not increasing at {d}')
        seen.add(d); last = d
        fa = dt.datetime.fromisoformat(e['frozen_at_utc'])
        if fa >= et_at(pd.Timestamp(d), LOCK_DEADLINE):
            errs.append(f'{paths.rel(ledger)}: {d} stamped {e["frozen_at_utc"]}, not before 09:25 ET')
        if pd.Timestamp(d) < START:
            errs.append(f'{paths.rel(ledger)}: {d} is before the start date (no retroactive fingerprints)')
        for rp, h in e['files'].items():
            fp = paths.repo / rp
            if not fp.exists():
                errs.append(f'{rp}: locked file missing')
            elif sha256_file(fp) != h:
                errs.append(f'{rp}: locked file changed (sha256 mismatch)')
    if day_dir is not None and day_dir.exists():
        for x in sorted(day_dir.iterdir()):
            k = x.name[:10]
            if k not in seen:
                errs.append(f'{paths.rel(x)}: day output without a lock line')
    if missed is not None:
        mb = missed.read_bytes() if missed.exists() else b''
        if base_missed is not None and not mb.startswith(base_missed):
            errs.append(f'{paths.rel(missed)}: not append-only')
        for e in read_jsonl(missed):
            if e['date'] in seen:
                errs.append(f'{paths.rel(missed)}: {e["date"]} marked missed but also locked')
    return seen


def lock_check(paths: Paths, base_ref: str | None = None, log=print) -> list:
    def at_base(p):
        if not base_ref:
            return None
        import subprocess
        r = subprocess.run(['git', '-C', str(paths.repo), 'show', f'{base_ref}:{paths.rel(p)}'], capture_output=True)
        return r.stdout if r.returncode == 0 else b''
    errs = []
    check_ledger(paths, paths.locks, paths.days, at_base(paths.locks), paths.missed, at_base(paths.missed), errs)
    check_ledger(paths, paths.exp_locks, paths.exp_days, at_base(paths.exp_locks), paths.exp_missed, at_base(paths.exp_missed), errs)
    for e in errs:
        log('LOCK FAIL: ' + e)
    if not errs:
        log(f'lock check OK: {len(read_jsonl(paths.locks))} shadow days, {len(read_jsonl(paths.exp_locks))} expmove days locked'
            + (f' (append-only vs {base_ref})' if base_ref else ''))
    return errs


# ---------------------------------------------------------------------------------------------------------------- freeze
def fetch_inputs(N, paths: Paths, log=print, tickers=None):
    N = pd.Timestamp(N)
    tk = tickers or [t.strip() for t in (paths.root / 'static' / 'tickers.txt').read_text().split() if t.strip()]
    t0 = time.time()
    B, S, miss = fetch_bars(tk, log=log)
    log(f'  bars: {len(B)} rows, {B.ticker.nunique()} tickers, {time.time() - t0:.0f}s; missing {len(miss)}')
    I, _, _ = fetch_bars(['SPY', '^VIX', 'IWM'], period='3y', log=lambda *a: None)
    spy = I[I.ticker == 'SPY'].set_index('date').close; vix = I[I.ticker == '^VIX'].set_index('date').close
    spyvix = pd.DataFrame({'spy': spy, 'vix': vix}).dropna().sort_index()
    E, esrc = earnings_for(N, paths.root, log=log)
    return dict(B=B, S=S, miss=miss, spyvix=spyvix, spy_bars=I[I.ticker == 'SPY'], E=E, esrc=esrc, tickers=tk)


def readiness(inp, N):
    """Previous session's final bars must be in: SPY has it and >=95% of names with a bar 2 sessions back have it too."""
    N = pd.Timestamp(N); p1 = prev_session(N); p2 = prev_session(p1); B = inp['B']
    spy_ok = (inp['spy_bars'].date == p1).any() and inp['spyvix'].index.max() >= p1
    n1 = B[B.date == p1].ticker.nunique(); n2 = B[B.date == p2].ticker.nunique()
    future = int((B.date >= N).sum())
    return dict(ok=bool(spy_ok and n2 > 0 and n1 >= 0.95 * n2), prev_session=ds(p1), n_prev=int(n1), n_prev2=int(n2),
                spy_has_prev=bool(spy_ok), rows_dated_on_or_after_N_dropped=future)


def compute_day(N, inp, paths: Paths, models, log=print):
    N = pd.Timestamp(N)
    B = inp['B']; past = sorted(pd.Timestamp(x) for x in inp['spy_bars'].date.unique() if pd.Timestamp(x) < N)
    assert past[-1] == prev_session(N), f'SPY last session {past[-1]} != previous session {prev_session(N)}'
    imap = pd.read_csv(paths.root / 'static' / 'industry_map_finviz_2026-04-26.csv')
    X = build_features(B, inp['S'], N, imap, inp['spyvix'], inp['E'], past)
    L = X[X.L1000 == 1].sort_values('ticker').reset_index(drop=True)
    F = to_f32_frame(L)
    S = score_and_pick(F, L.ticker, models)
    S['beta60'] = L.beta60.values; S['prev_close_raw'] = L.prev_close_raw.values; S['ldv20'] = L.ldv20.values
    S['park20'] = L.park20.values
    log(f'  day {ds(N)}: universe {len(X)}, L1000 {len(L)}, eligible {int(S.eligible.sum())}, long {(S.side == "long").sum()}, short {(S.side == "short").sum()}')
    return X, L, F, S


def freeze_day(N, paths: Paths, out: Paths | None = None, inp=None, log=print, enforce_time=True):
    """Write and lock day N. `out` lets a dry-run write into a temp copy. Returns summary dict."""
    out = out or paths
    N = pd.Timestamp(N); dstr = ds(N)
    models, meta = load_models(paths.root)
    if (out.days / dstr).exists() or dstr in {e['date'] for e in read_jsonl(out.locks)}:
        raise LockError(f'{dstr} already frozen; refusing to touch it')
    if dstr in {e['date'] for e in read_jsonl(out.missed)}:
        raise LockError(f'{dstr} already marked missed')
    inp = inp or fetch_inputs(N, paths, log=log)
    rd = readiness(inp, N)
    log(f'  readiness {rd}')
    if not rd['ok']:
        raise LockError(f'inputs not ready for {dstr}: {rd}')
    X, L, F, S = compute_day(N, inp, paths, models, log=log)
    if len(L) < 900 or (S.side == 'long').sum() < 30 or (S.side == 'short').sum() < 30:
        raise LockError(f'too few names for {dstr} (L1000 {len(L)}); not freezing')
    t = now_utc()
    if enforce_time and t >= et_at(N, FREEZE_CUTOFF):
        raise LockError(f'past the 09:20 ET freeze cutoff for {dstr}; not freezing')
    dd = out.days / dstr; dd.mkdir(parents=True, exist_ok=False)
    # frozen inputs: scored feature matrix (float32), eligible/score table, dropped list, raw-input hashes
    feat_b = f32_csv(pd.concat([L[['ticker']], F], axis=1))
    (dd / 'features_L1000.csv.gz').write_bytes(feat_b)
    sc = S[['ticker', 'd1', 'm1', 'm1_pct', 'eligible', 'side', 'w', 'beta60', 'prev_close_raw', 'ldv20', 'park20']].copy()
    b = io.StringIO(); sc.to_csv(b, index=False, float_format='%.10g', lineterminator='\n')
    (dd / 'scores.csv').write_text(b.getvalue())
    raw = inp['B'][inp['B'].date < N].sort_values(['ticker', 'date'])
    raw_sha = sha256_bytes(pd.util.hash_pandas_object(raw, index=False).values.tobytes())
    ebuf = io.StringIO(); inp['E'].sort_values(['edate', 'ticker']).to_csv(ebuf, index=False, lineterminator='\n')
    (dd / 'earnings_used.csv.gz').write_bytes(gzip.compress(ebuf.getvalue().encode(), mtime=0))
    legs = lambda side: [clean({'ticker': r.ticker, 'w': r.w, 'd1': r.d1, 'm1': r.m1, 'beta60': r.beta60, 'prev_close_raw': r.prev_close_raw})
                         for r in S[S.side == side].sort_values('d1', ascending=(side == 'short')).itertuples()]
    picks = {
        'strategy': NAME, 'label': LABEL, 'date': dstr, 'frozen_at_utc': t.isoformat(timespec='seconds'),
        'frozen_at_et': t.astimezone(ET).strftime('%Y-%m-%d %H:%M:%S %Z'), 'frozen_at_hkt': t.astimezone(HKT).strftime('%Y-%m-%d %H:%M:%S HKT'),
        'what': 'D8: 1-day sector-relative LightGBM ranker on the top 1,000 names by 20-day dollar volume (actual prior close >= $5); '
                'trades only the half with the highest predicted 1-day move size; long top 10% / short bottom 10% of that half, equal weight.',
        'fill': 'enter at the 09:30 ET official open of this date, exit at the next session 09:30 open (Yahoo split-adjusted); daily rebalance',
        'inputs_rule': 'bars dated before this date only (no same-day open); SPY/VIX closes before this date; Nasdaq earnings calendar',
        'model_rule': 'frozen models trained once on L1000 rows 2018-03-29..2026-10-01; never retrained (any change = new name)',
        'model_sha256': {k: v['sha256'] for k, v in meta['models'].items()},
        'n_universe': int(len(X)), 'n_L1000': int(len(L)), 'n_eligible': int(S.eligible.sum()),
        'long': legs('long'), 'short': legs('short'),
        'ex_ante_beta': clean(float((S.w * S.beta60.fillna(1.0)).sum())),
        'random4': {'seed': RANDOM_SEED, 'draws': RANDOM_DRAWS, 'pool': 'eligible rows of scores.csv'},
        'paper_only': True, 'orders': 'none (no broker wiring)'}
    write_json(dd / 'picks.json', picks)
    manifest = {
        'date': dstr, 'frozen_at_utc': picks['frozen_at_utc'], 'readiness': rd,
        'raw_bars_sha256_hashpandas': raw_sha, 'raw_bars_rows': int(len(raw)), 'raw_bars_last_date': ds(raw.date.max()),
        'tickers_requested': len(inp['tickers']), 'tickers_dropped_no_yahoo': inp['miss'],
        'tickers_dropped_no_prev_session_bar': sorted(set(inp['B'].ticker) - set(inp['B'][inp['B'].date == prev_session(N)].ticker)),
        'earnings_sources': inp['esrc'], 'regime_asof': str(X.regime_asof.iloc[0]),
        'code_sha256': sha256_file(Path(__file__)),
        'files': {}}
    for f in ['features_L1000.csv.gz', 'scores.csv', 'earnings_used.csv.gz', 'picks.json']:
        manifest['files'][f] = sha256_file(dd / f)
    write_json(dd / 'manifest.json', clean(manifest))
    files = {out.rel(dd / f): sha256_file(dd / f) for f in ['features_L1000.csv.gz', 'scores.csv', 'earnings_used.csv.gz', 'picks.json', 'manifest.json']}
    # expmove forward day (descriptive): M1 predicted |open->next open| and its within-L1000 percentile
    ex = {'model': EXPNAME, 'date': dstr, 'frozen_at_utc': picks['frozen_at_utc'], 'kind': 'forward (frozen before 09:25 ET)',
          'note': 'Descriptive risk rank only, no trading claim. m1_score is unitless (model trained on per-day rank of |open->next open|); rank_pct is its percentile within the 1,000 names. Rank IC vs realized |open->next open| ~0.42 vs 0.38 for plain 20-day Parkinson vol.',
          'cols': ['ticker', 'm1_score', 'rank_pct', 'park20_rank_pct'],
          'rows': [clean([r.ticker, r.m1, round(r.m1_pct, 4), round(p, 4)]) for r, p in zip(S.itertuples(), S.park20.rank(pct=True))]}
    out.exp_days.mkdir(parents=True, exist_ok=True)
    ep = out.exp_days / f'{dstr}.json'
    if ep.exists():
        raise LockError(f'{ep} exists')
    write_json(ep, ex)
    # final time check right before locking
    t2 = now_utc()
    if enforce_time and t2 >= et_at(N, FREEZE_CUTOFF):
        shutil.rmtree(dd); ep.unlink()
        raise LockError('crossed the 09:20 ET cutoff while writing; rolled back, not locked')
    append_lock(out.locks, {'date': dstr, 'frozen_at_utc': picks['frozen_at_utc'], 'files': files})
    append_lock(out.exp_locks, {'date': dstr, 'frozen_at_utc': picks['frozen_at_utc'], 'files': {out.rel(ep): sha256_file(ep)}})
    log(f'  FROZEN {dstr}: {len(picks["long"])} long / {len(picks["short"])} short at {picks["frozen_at_et"]}')
    return picks


def missed_due(paths: Paths) -> bool:
    now = now_utc(); lk = {e['date'] for e in read_jsonl(paths.locks)} | {e['date'] for e in read_jsonl(paths.missed)}
    return any(ds(d) not in lk and now >= et_at(d, REALIZE_FROM) for d in sessions_between(START, now.astimezone(ET).date()))


def realize_due(paths: Paths) -> bool:
    """A locked day whose exit session has closed but is not yet in the realized ledger (or the ledger is missing)."""
    now = now_utc()
    if not (paths.realized / 'summary.json').exists() or not (paths.dash / 'shadow.json').exists():
        return True
    have = set()
    if (paths.realized / 'daily.csv').exists() and (paths.realized / 'daily.csv').stat().st_size > 5:
        have = set(pd.read_csv(paths.realized / 'daily.csv', usecols=['date']).date)
    fr = json.loads((paths.exp / 'forward_realized.json').read_text()) if (paths.exp / 'forward_realized.json').exists() else {}
    for e in read_jsonl(paths.locks):
        if e['date'] not in have and now >= et_at(next_session(e['date']), dt.time(16, 30)):
            return True
    for e in read_jsonl(paths.exp_locks):
        if e['date'] not in fr and now >= et_at(next_session(e['date']), dt.time(16, 30)):
            return True
    # dashboard needs the newest frozen day
    sj = json.loads((paths.dash / 'shadow.json').read_text())
    return [d['date'] for d in sj.get('days', [])] != [e['date'] for e in read_jsonl(paths.locks)]


def mark_missed(paths: Paths, log=print):
    """Sessions from START whose 09:25 ET deadline passed with no lock get an append-only MISSED line (never back-filled)."""
    now = now_utc()
    locked = {e['date'] for e in read_jsonl(paths.locks)}
    for ledger in [paths.missed, paths.exp_missed]:
        done = {e['date'] for e in read_jsonl(ledger)}
        lk = locked if ledger == paths.missed else {e['date'] for e in read_jsonl(paths.exp_locks)}
        for d in sessions_between(START, now.astimezone(ET).date()):
            k = ds(d)
            if k in lk or k in done or now < et_at(d, REALIZE_FROM):
                continue
            line = json.dumps({'date': k, 'recorded_at_utc': now.isoformat(timespec='seconds'), 'reason': 'not frozen before 09:25 ET'}, sort_keys=True)
            with open(ledger, 'a') as f:
                f.write(line + '\n')
            log(f'  MISSED {k} -> {paths.rel(ledger)}')


# ---------------------------------------------------------------------------------------------------------------- verify
def verify(paths: Paths, log=print) -> list:
    """Restart check: re-score each locked day from its frozen features with the frozen models; picks must be identical."""
    models, _ = load_models(paths.root); errs = []
    for e in read_jsonl(paths.locks):
        dd = paths.days / e['date']
        F = pd.read_csv(dd / 'features_L1000.csv.gz', dtype={c: 'float32' for c in FEATS})
        S = score_and_pick(F, F.ticker, models)
        P = json.loads((dd / 'picks.json').read_text())
        if sorted(x['ticker'] for x in P['long']) != sorted(S.ticker[S.side == 'long']) or \
           sorted(x['ticker'] for x in P['short']) != sorted(S.ticker[S.side == 'short']):
            errs.append(f'{e["date"]}: re-scored picks differ from frozen picks')
    log('verify OK' if not errs else '\n'.join('VERIFY FAIL: ' + x for x in errs))
    return errs


# ---------------------------------------------------------------------------------------------------------------- realize
def nw_t(x, lag=5):
    x = np.asarray(x, float); n = len(x)
    if n < 3 or np.std(x) == 0:
        return float('nan')
    m = x.mean(); u = x - m; v = (u @ u) / n
    for k in range(1, min(lag, n - 1) + 1):
        v += 2 * (1 - k / (lag + 1)) * (u[k:] @ u[:-k]) / n
    return float(m / np.sqrt(v / n))


def realize(paths: Paths, prices=None, log=print):
    """Separate realized ledger (recomputed from the frozen picks every run; pick files are never edited)."""
    L = read_jsonl(paths.locks)
    days = [pd.Timestamp(e['date']) for e in L]
    missed = [e['date'] for e in read_jsonl(paths.missed)]
    paths.realized.mkdir(parents=True, exist_ok=True)
    if not days:
        summ = {'strategy': NAME, 'label': LABEL, 'days_locked': 0, 'days_realized': 0, 'days_missed': len(missed), 'status': 'collecting',
                'rule': rule_text()}
        write_json(paths.realized / 'summary.json', summ); return summ
    picks = {ds(d): json.loads((paths.days / ds(d) / 'picks.json').read_text()) for d in days}
    scores = {ds(d): pd.read_csv(paths.days / ds(d) / 'scores.csv') for d in days}
    expd = {e['date']: json.loads((paths.exp_days / f"{e['date']}.json").read_text()) for e in read_jsonl(paths.exp_locks)}
    names = sorted({x['ticker'] for p in picks.values() for x in p['long'] + p['short']} |
                   {t for s in scores.values() for t in s.ticker} | {r[0] for j in expd.values() for r in j['rows']} | {'SPY', 'IWM'})
    if prices is None:
        first = days[0] - pd.Timedelta(days=7)
        per = '1mo' if (pd.Timestamp.now() - first).days < 25 else ('6mo' if (pd.Timestamp.now() - first).days < 170 else '2y')
        B, Sp, _ = fetch_bars(names, period=per, log=lambda *a: None)
    else:
        B, Sp = prices
    O = B.pivot_table(index='date', columns='ticker', values='open')
    now = now_utc()
    rows, contrib, cdaily = [], {}, {}
    rng = np.random.default_rng(RANDOM_SEED)
    prev_w, prev_rw, prev_d = None, None, None
    for d in days:
        k = ds(d); n1 = next_session(d)
        if d not in O.index or n1 not in O.index or now < et_at(n1, dt.time(16, 30)):
            continue   # realized after the close of the exit day only
        p = picks[k]
        w = pd.Series({x['ticker']: x['w'] for x in p['long'] + p['short']})
        r = (O.loc[n1, w.index] / O.loc[d, w.index] - 1)
        miss_fill = sorted(r[r.isna()].index)
        # >3x price jump with no split in between -> flagged (kept at 0 return, listed)
        spl_names = set(Sp[(Sp.date > d) & (Sp.date <= n1)].ticker) if len(Sp) else set()
        jump = sorted(t for t, v in r.items() if np.isfinite(v) and (v > 2 or v < -2 / 3) and t not in spl_names)
        r = r.fillna(0.0); r[jump] = 0.0
        cont = w * r
        for t, v in cont.items():
            contrib[t] = contrib.get(t, 0.0) + v
        cdaily[k] = cont
        consec = prev_d is not None and next_session(prev_d) == d
        pw = prev_w if (consec and prev_w is not None) else pd.Series(dtype=float)
        turn = float(w.sub(pw, fill_value=0).abs().sum())
        gross = float(cont.sum())
        beta = float(p['ex_ante_beta']) if p.get('ex_ante_beta') is not None else 1.0
        px = lambda t: float(O.loc[n1, t] / O.loc[d, t] - 1) if t in O.columns else float('nan')
        spy, iwm = px('SPY'), px('IWM')
        univ = scores[k].ticker.to_numpy()
        ur = (O.loc[n1].reindex(univ) / O.loc[d].reindex(univ) - 1)
        mkt = float(ur[(ur > -2 / 3) & (ur < 2)].mean())     # equal-weight L1000 open->next open (the market beta60 is measured against)
        row = {'date': k, 'exit_date': ds(n1), 'n_long': len(p['long']), 'n_short': len(p['short']), 'gross': gross, 'turnover': turn,
               'ex_ante_beta': beta, 'ew_l1000_oo1': mkt, 'hedged_gross': gross - beta * mkt, 'spy_oo1': spy, 'iwm_oo1': iwm,
               'missing_fills': ';'.join(miss_fill), 'jump_flags': ';'.join(jump)}
        for f in FEES_BP:
            row[f'net{f}'] = gross - f / 1e4 * turn
            row[f'hedged_net{f}'] = gross - beta * mkt - f / 1e4 * turn
        row['spy_hedged_net10'] = gross - beta * spy - 10 / 1e4 * turn
        # RANDOM4: 1000 seeded random long/short books of the same size from the frozen eligible pool, same fee model
        sc = scores[k]; pool = sc.ticker[sc.eligible].to_numpy(); rr = (O.loc[n1].reindex(pool) / O.loc[d].reindex(pool) - 1).fillna(0.0).to_numpy()
        nl, ns = len(p['long']), len(p['short']); g_r = np.empty(RANDOM_DRAWS); t_r = np.empty(RANDOM_DRAWS); rw_all = []
        rs = np.random.default_rng([RANDOM_SEED, int(k.replace('-', ''))])
        for i in range(RANDOM_DRAWS):
            idx = rs.permutation(len(pool))[:nl + ns]
            rw = pd.Series(np.r_[np.full(nl, 1 / nl), np.full(ns, -1 / ns)], index=pool[idx])
            g_r[i] = float(np.r_[np.full(nl, 1 / nl), np.full(ns, -1 / ns)] @ rr[idx])
            prw = prev_rw[i] if (consec and prev_rw is not None) else pd.Series(dtype=float)
            t_r[i] = float(rw.sub(prw, fill_value=0).abs().sum()); rw_all.append(rw)
        for f in FEES_BP:
            nr = g_r - f / 1e4 * t_r
            row[f'rand_med_net{f}'] = float(np.median(nr)); row[f'rand_p95_net{f}'] = float(np.quantile(nr, 0.95))
        row['rand_pctile_net10'] = float((g_r - 10 / 1e4 * t_r < row['net10']).mean())
        rows.append(row); prev_w, prev_rw, prev_d = w, rw_all, d
    R = pd.DataFrame(rows)
    b = io.StringIO(); R.to_csv(b, index=False, float_format='%.8g', lineterminator='\n'); (paths.realized / 'daily.csv').write_text(b.getvalue())
    if len(R) and contrib:
        best = max(contrib, key=contrib.get)
        R['net10_ex_best'] = [r.net10 - float(cdaily[r.date].get(best, 0.0)) for r in R.itertuples()]
        b = io.StringIO(); R.to_csv(b, index=False, float_format='%.8g', lineterminator='\n'); (paths.realized / 'daily.csv').write_text(b.getvalue())
    summ = summarize(R, contrib, len(days), missed)
    # updown_expmove_v1 forward days: realized |open->next open| and rank IC (descriptive)
    fr = {}
    for k, j in expd.items():
        d = pd.Timestamp(k); n1 = next_session(d)
        if d not in O.index or n1 not in O.index or now < et_at(n1, dt.time(16, 30)):
            continue
        X = pd.DataFrame(j['rows'], columns=j['cols']).set_index('ticker')
        a = (O.loc[n1].reindex(X.index) / O.loc[d].reindex(X.index) - 1).abs()
        ok = a.notna()
        fr[k] = {'n': int(ok.sum()), 'ic': float(X.m1_score[ok].corr(a[ok], method='spearman')),
                 'ic_park20': float(X.park20_rank_pct[ok].corr(a[ok], method='spearman')),
                 'abs': {t: int(round(v * 1e4)) for t, v in a[ok].items()}}
    write_json(paths.exp / 'forward_realized.json', clean(fr))
    write_json(paths.realized / 'summary.json', clean(summ))
    log(f'  realized {summ["days_realized"]} days; status {summ["status"]}')
    return summ


def rule_text():
    return {
        'kill': f'KILL if, after >= {KILL["min_days"]} realized locked days, cumulative net return at 10bp/side is negative; '
                f'early KILL if after >= {KILL["early_min_days"]} days the Newey-West t of daily net at 10bp/side is <= {KILL["early_t"]}. '
                'A killed strategy stops freezing new days; its record stays untouched.',
        'promote': f'PROMOTION REVIEW (by Cyrus; never automatic) only after >= {PROMOTE["min_days"]} realized locked days AND the live '
                   f'Newey-West t of daily net at 10bp/side >= {PROMOTE["t_bar"]} (the cumulative multiple-testing bar) AND it beats the '
                   'RANDOM4 median and IWM over the same days, also without its single best stock.',
        'missed': 'Missed days (not frozen before 09:25 ET) are recorded in MISSED.jsonl, never back-filled, and earn nothing.'}


def summarize(R, contrib, n_locked, missed):
    s = {'strategy': NAME, 'label': LABEL, 'days_locked': n_locked, 'days_realized': int(len(R)), 'days_missed': len(missed),
         'missed_dates': missed, 'rule': rule_text()}
    if len(R):
        for c in ['gross', 'net5', 'net10', 'net15', 'hedged_net10', 'iwm_oo1', 'rand_med_net10']:
            x = R[c].to_numpy(); s[c] = {'cum': float(np.prod(1 + x) - 1), 'mean_bp': float(x.mean() * 1e4), 't_nw': nw_t(x)}
        best = max(contrib, key=contrib.get) if contrib else None
        s['best_stock'] = best; s['best_stock_contrib'] = contrib.get(best)
        s['net10_ex_best_cum'] = float(np.prod(1 + R.net10_ex_best.to_numpy()) - 1) if 'net10_ex_best' in R else None
        s['mean_turnover'] = float(R.turnover.mean())
    n = len(R); cum10 = s.get('net10', {}).get('cum', 0.0); t10 = s.get('net10', {}).get('t_nw', float('nan'))
    if n >= KILL['min_days'] and cum10 < 0:
        st = 'KILLED (net at 10bp/side negative after >= 60 days)'
    elif n >= KILL['early_min_days'] and np.isfinite(t10) and t10 <= KILL['early_t']:
        st = 'KILLED (early: t <= -2.5 at 10bp/side)'
    elif n >= PROMOTE['min_days'] and np.isfinite(t10) and t10 >= PROMOTE['t_bar'] and cum10 > s.get('rand_med_net10', {}).get('cum', 0) \
            and cum10 > s.get('iwm_oo1', {}).get('cum', 0) and (s.get('net10_ex_best_cum') or -1) > 0:
        st = 'PROMOTION REVIEW DUE (Cyrus decides; still paper)'
    else:
        st = f'collecting ({n} realized days; kill check from day {KILL["early_min_days"]}/{KILL["min_days"]}, promotion review not before day {PROMOTE["min_days"]})'
    s['status'] = st
    s['killed'] = st.startswith('KILLED')
    return s


# ---------------------------------------------------------------------------------------------------------------- dashboard
def export_dashboard(paths: Paths, log=print):
    d = paths.dash; d.mkdir(parents=True, exist_ok=True)
    L = read_jsonl(paths.locks)
    days = []
    for e in L:
        p = json.loads((paths.days / e['date'] / 'picks.json').read_text())
        days.append({'date': e['date'], 'frozen_at_et': p['frozen_at_et'], 'frozen_at_hkt': p['frozen_at_hkt'],
                     'long': [[x['ticker'], x['d1'], x['m1']] for x in p['long']], 'short': [[x['ticker'], x['d1'], x['m1']] for x in p['short']],
                     'ex_ante_beta': p['ex_ante_beta'], 'n_L1000': p['n_L1000'], 'n_eligible': p['n_eligible'],
                     'files': e['files']})
    summ = json.loads((paths.realized / 'summary.json').read_text()) if (paths.realized / 'summary.json').exists() else {}
    R = pd.read_csv(paths.realized / 'daily.csv') if (paths.realized / 'daily.csv').exists() and (paths.realized / 'daily.csv').stat().st_size > 5 else pd.DataFrame()
    obj = {'strategy': NAME, 'label': LABEL, 'start': ds(START),
           'days': days, 'missed': read_jsonl(paths.missed), 'summary': summ,
           'realized_cols': list(R.columns), 'realized': R.replace({np.nan: None}).values.tolist() if len(R) else []}
    write_json(d / 'shadow.json', clean(obj))
    # expmove forward index (for dashboard/updown)
    ex = [e['date'] for e in read_jsonl(paths.exp_locks)]
    write_json(paths.exp / 'forward_index.json', {'model': EXPNAME, 'forward_days': ex, 'missed': read_jsonl(paths.exp_missed)})
    log(f'  dashboard data: {len(days)} days')


# ---------------------------------------------------------------------------------------------------------------- cli
def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('cmd', choices=['gate', 'run', 'check', 'verify', 'dryrun', 'realize', 'pushok'])
    ap.add_argument('--base-ref'); ap.add_argument('--date'); ap.add_argument('--no-freeze', action='store_true')
    ap.add_argument('--out')
    a = ap.parse_args(argv)
    paths = Paths(REPO)
    if a.cmd == 'gate':
        g = due(); summ = paths.realized / 'summary.json'
        g['killed'] = bool(json.loads(summ.read_text()).get('killed')) if summ.exists() else False
        g['already_frozen'] = bool(g['freeze_date'] and (paths.days / g['freeze_date']).exists())
        g['freeze_needed'] = bool(g['freeze_date'] and not g['already_frozen'] and not g['killed'])
        g['realize_needed'] = realize_due(paths); g['missed_needed'] = missed_due(paths)
        g['work'] = bool(g['write_ok'] and (g['freeze_needed'] or g['realize_needed'] or g['missed_needed']))
        print(json.dumps(g)); return 0
    if a.cmd == 'check':
        return 1 if lock_check(paths, a.base_ref) else 0
    if a.cmd == 'pushok':      # a freeze commit for --date may only land on main before 09:24 ET that day
        ok = now_utc() < et_at(pd.Timestamp(a.date), PUSH_DEADLINE)
        print(f'pushok {a.date}: {ok}'); return 0 if ok else 1
    if a.cmd == 'verify':
        return 1 if verify(paths) else 0
    if a.cmd == 'dryrun':
        N = pd.Timestamp(a.date) if a.date else pd.Timestamp(due()['freeze_date'] or next_session(pd.Timestamp.now().normalize()))
        tmp = Path(a.out or tempfile.mkdtemp(prefix='ushadow_dry_'))
        for sub in ['research/' + NAME, 'dashboard/updown/data/expmove']:
            src = paths.repo / sub
            (tmp / sub).mkdir(parents=True, exist_ok=True)
            for f in ['LOCKS.jsonl', 'MISSED.jsonl']:
                if (src / f).exists():
                    shutil.copy(src / f, tmp / sub / f)
        out = Paths(tmp)
        st = START
        globals()['START'] = min(START, N)   # dry-run may target a past date; never used for real locks
        try:
            freeze_day(N, paths, out=out, enforce_time=False)
        finally:
            globals()['START'] = st
        print(f'dry-run wrote {tmp}'); return 0
    if a.cmd in ('run', 'realize'):
        if lock_check(paths):
            return 1
        g = due()
        print(json.dumps(g))
        if not g['write_ok']:
            print('quiet window (09:20-09:45 ET on a session day): nothing written'); return 0
        mark_missed(paths)
        summ = json.loads((paths.realized / 'summary.json').read_text()) if (paths.realized / 'summary.json').exists() else {}
        froze = False
        if a.cmd == 'run' and not a.no_freeze and g['freeze_date'] and not summ.get('killed'):
            N = pd.Timestamp(g['freeze_date'])
            if not (paths.days / ds(N)).exists():
                try:
                    freeze_day(N, paths); froze = True
                except LockError as e:
                    print(f'freeze skipped: {e}')
        if froze:                      # push the freeze fast: no price fetch now, realized ledger updates on a later run
            export_dashboard(paths)
        elif realize_due(paths):
            try:
                realize(paths)
            except Exception as e:   # realized ledger is derived data; never blocks a freeze
                print(f'realize failed (will retry next run): {e!r}')
            export_dashboard(paths)
        return 1 if lock_check(paths) else 0


if __name__ == '__main__':
    sys.exit(main())
