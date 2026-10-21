"""Offline data prep (box). Bars = v3/v4 bars (bars_all + free yfinance top-up). Universe = v4's: price >= $5 and top 500 by
20-day dollar volume, both as of bar D (prior close)."""
import pandas as pd, numpy as np
TOP_N = 500

def load_bars(paths, end='2026-10-19'):
    B = pd.concat([pd.read_parquet(p) for p in paths]).drop_duplicates(['date', 'ticker'], keep='last')
    B = B[B.date <= end].dropna(subset=['close']).sort_values(['ticker', 'date']).reset_index(drop=True)
    return B

def universe(B, top_n=TOP_N):
    dv = (B.close * B.volume).groupby(B.ticker).transform(lambda s: s.rolling(20, min_periods=15).mean())
    U = B[['date', 'ticker']].assign(dv=dv.values, px=B.close.values)
    U = U[(U.px >= 5) & U.dv.notna()]
    U['rk'] = U.groupby('date').dv.rank(ascending=False, method='first')
    return U[U.rk <= top_n][['date', 'ticker', 'dv']]
