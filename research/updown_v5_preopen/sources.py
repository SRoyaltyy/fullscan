"""updown_v5_preopen free-source blocks. Every function returns rows keyed by (feat_asof = bar date D, ticker) or feat_asof only,
holding information that was public before 09:25 ET on N = next session after D. Lags (conservative):
  iv      DoltHub volatility_history dated <= D (EOD, no capture time), as-of, max 5 calendar days stale (Mon/Wed/Fri before ~Oct 2024)
  short   FINRA Reg SHO CNMS daily file dated <= D (published evening of D)
  insider SEC Form 3/4/5 data sets, FILING_DATE <= D (a filing dated D was accepted by 22:00 ET on D; strictly before N 09:25)
  ftd     SEC fails-to-deliver, SETTLEMENT date <= D - 35 calendar days (files publish ~2-4 weeks after period end)
  regime  Cboe VVIX/SKEW/VIX3M dated <= D (final ~16:15 ET on D); FRED daily series dated <= D-2 sessions, NFCI <= D-7 days"""
import numpy as np, pandas as pd, os, glob, io, zipfile

BLOCKS = {
    'iv': ['l_iv', 'iv_rank', 'iv_minus_rv', 'iv_chg_wk', 'iv_chg_mo'],
    'short': ['short_ratio', 'short_ratio5', 'short_z20'],
    'insider': ['ins_buy21', 'ins_sell21', 'ins_buy63', 'ins_sell63', 'ins_net63'],
    'ftd': ['ftd_ratio', 'ftd_log'],
    'regime': ['vvix', 'skew_idx', 'vix_vix3m', 'hy_spread', 't10y2y', 'nfci'],
}

def asof(P, X, cols, date_col, by='ticker', tol=None):
    X = X.sort_values(date_col); P = P.sort_values('feat_asof')
    kw = dict(left_on='feat_asof', right_on=date_col, direction='backward')
    if by: kw['by'] = by
    if tol: kw['tolerance'] = pd.Timedelta(tol)
    out = pd.merge_asof(P, X[([by] if by else []) + [date_col] + cols], **kw)
    return out.drop(columns=[date_col])

def iv_block(IV, hv20=None):
    """IV: date, ticker, iv_current, iv_week_ago, iv_month_ago, iv_year_high, iv_year_low, hv_current."""
    X = IV.copy(); X['l_iv'] = np.log(X.iv_current.clip(lower=.01))
    X['iv_rank'] = (X.iv_current - X.iv_year_low) / (X.iv_year_high - X.iv_year_low).replace(0, np.nan)
    X['iv_minus_rv'] = X.iv_current - X.hv_current
    X['iv_chg_wk'] = np.log(X.iv_current / X.iv_week_ago); X['iv_chg_mo'] = np.log(X.iv_current / X.iv_month_ago)
    return X.rename(columns={'date': 'iv_date'})[['iv_date', 'ticker'] + BLOCKS['iv']].replace([np.inf, -np.inf], np.nan)

def short_block(F):
    """F: date, ticker, short, total (FINRA CNMS). Rolling per ticker over FINRA rows (dated <= D)."""
    F = F.sort_values(['ticker', 'date']).copy(); F['short_ratio'] = F.short / F.total.replace(0, np.nan)
    g = F.groupby('ticker').short_ratio
    F['short_ratio5'] = g.transform(lambda s: s.rolling(5, min_periods=3).mean())
    m = g.transform(lambda s: s.rolling(20, min_periods=10).mean()); sd = g.transform(lambda s: s.rolling(20, min_periods=10).std())
    F['short_z20'] = (F.short_ratio - m) / sd.replace(0, np.nan)
    return F.rename(columns={'date': 'sv_date'})[['sv_date', 'ticker'] + BLOCKS['short']]

def insider_block(T, days, tickers):
    """T: ticker, filing_date, code ('P' buy / 'S' sale). Counts of distinct filings with filing_date in (D-w sessions, D]."""
    di = pd.Series(np.arange(len(days)), index=days)
    T = T[T.ticker.isin(tickers)].copy()
    T['i'] = np.searchsorted(days, T.filing_date.values)        # first session >= filing date (same day if a session)
    T = T[T.i < len(days)]
    rows = []
    for (t, code), g in T.groupby(['ticker', 'code']):
        c = np.bincount(g.i.values, minlength=len(days)).astype(float)
        cs = np.cumsum(c)
        for w in (21, 63):
            v = cs - np.concatenate([np.zeros(w), cs[:-w]])
            rows.append(pd.DataFrame({'feat_asof': days, 'ticker': t, 'col': f"ins_{'buy' if code == 'P' else 'sell'}{w}", 'v': v}))
    if not rows: return pd.DataFrame(columns=['feat_asof', 'ticker'] + BLOCKS['insider'])
    X = pd.concat(rows).pivot_table(index=['feat_asof', 'ticker'], columns='col', values='v', fill_value=0).reset_index()
    for c in BLOCKS['insider'][:-1]:
        if c not in X: X[c] = 0.0
    X['ins_net63'] = X.ins_buy63 - X.ins_sell63
    return X[['feat_asof', 'ticker'] + BLOCKS['insider']]

def ftd_block(FT, adv):
    """FT: settle_date, ticker, qty. adv: ticker -> (date, vol20). Feature on D uses settle dates <= D - 35 days, summed over the
    last 10 available settle dates, divided by 20-day avg volume as of D."""
    FT = FT.sort_values(['ticker', 'settle_date']).copy()
    FT['ftd10'] = FT.groupby('ticker').qty.transform(lambda s: s.rolling(10, min_periods=1).sum())
    FT['avail'] = FT.settle_date + pd.Timedelta('35D')
    return FT[['avail', 'ticker', 'ftd10']]

def regime_block(cboe, fred, days):
    s = {k: v.set_index('date').close for k, v in cboe.items()}; d = pd.DataFrame(s).sort_index()
    o = pd.DataFrame(index=d.index); o['vvix'] = d.VVIX; o['skew_idx'] = d.SKEW; o['vix_vix3m'] = d.VIX / d.VIX3M
    o = o.reindex(days, method='ffill')
    for col, name, lag in (('hy_spread', 'BAMLH0A0HYM2', 2), ('t10y2y', 'T10Y2Y', 2)):
        x = fred[name].reindex(pd.DatetimeIndex(days), method='ffill'); o[col] = x.shift(lag).values   # dated <= D-2 sessions
    n = fred['NFCI']; n.index = n.index + pd.Timedelta('7D')                                           # weekly, released ~5 days after
    o['nfci'] = n.reindex(pd.DatetimeIndex(days), method='ffill').values
    return o.rename_axis('feat_asof').reset_index()

# ---- raw file loaders (box scratch) ----
def load_finra(d, tickers):
    out = []
    for f in sorted(glob.glob(f'{d}/*.txt.gz')):
        try: x = pd.read_csv(f, sep='|', usecols=['Date', 'Symbol', 'ShortVolume', 'TotalVolume'], dtype={'Date': str})
        except Exception: continue
        x = x[x.Symbol.isin(tickers)]
        out.append(pd.DataFrame({'date': pd.to_datetime(x.Date, format='%Y%m%d'), 'ticker': x.Symbol, 'short': x.ShortVolume.astype('float32'), 'total': x.TotalVolume.astype('float32')}))
    return pd.concat(out)

def load_ftd(d, tickers):
    out = []
    for f in sorted(glob.glob(f'{d}/*.zip')):
        try:
            z = zipfile.ZipFile(f); raw = z.read(z.namelist()[0]).decode('latin1')
            x = pd.read_csv(io.StringIO(raw), sep='|', dtype=str, on_bad_lines='skip')
        except Exception: continue
        x.columns = [c.strip().upper() for c in x.columns]
        qc = next((c for c in x.columns if c.startswith('QUANTITY')), None)
        if qc is None or 'SYMBOL' not in x: continue
        x = x[x.SYMBOL.isin(tickers)].rename(columns={qc: 'QUANTITY'})
        out.append(pd.DataFrame({'settle_date': pd.to_datetime(x['SETTLEMENT DATE'], format='%Y%m%d', errors='coerce'), 'ticker': x.SYMBOL,
                                 'qty': pd.to_numeric(x.QUANTITY.str.replace(r'\D', '', regex=True), errors='coerce')}))
    return pd.concat(out).dropna()

def load_form4(d, tickers):
    out = []
    for f in sorted(glob.glob(f'{d}/*.zip')):
        try:
            z = zipfile.ZipFile(f)
            s = pd.read_csv(z.open('SUBMISSION.tsv'), sep='\t', dtype=str, usecols=['ACCESSION_NUMBER', 'FILING_DATE', 'ISSUERTRADINGSYMBOL', 'DOCUMENT_TYPE'])
            t = pd.read_csv(z.open('NONDERIV_TRANS.tsv'), sep='\t', dtype=str, usecols=['ACCESSION_NUMBER', 'TRANS_CODE'])
        except Exception: continue
        t = t[t.TRANS_CODE.isin(['P', 'S'])].drop_duplicates()
        m = t.merge(s, on='ACCESSION_NUMBER'); m = m[m.DOCUMENT_TYPE.isin(['3', '4', '5', '4/A', '5/A', '3/A'])]
        m['ticker'] = m.ISSUERTRADINGSYMBOL.str.upper().str.strip()
        m = m[m.ticker.isin(tickers)]
        out.append(pd.DataFrame({'ticker': m.ticker, 'filing_date': pd.to_datetime(m.FILING_DATE, format='%d-%b-%Y', errors='coerce'), 'code': m.TRANS_CODE}))
    return pd.concat(out).dropna().drop_duplicates()
