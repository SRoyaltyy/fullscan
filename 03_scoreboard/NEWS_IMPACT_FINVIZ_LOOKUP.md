# Finviz description pack + horizon-first

Cyrus 2026-09-22: 0-1d hygiene is not the product. Eligibility is **published before that session 09:30 ET (+30m leeway)**. Capture timestamp is metadata.

Primary grade windows stay **1-4w and 1-6m** (`horizons.CLASS_HORIZON` + `window_grid`). 0-1d is a footnote for inventory / hard CPI / same-day blast_ops only.

## Lookup

`src/news_impact/finviz_lookup.py` reads the latest `data/exports/finviz_*.csv` and returns ≤40 candidates (ticker, name, sector, industry, 220-char description) from Industry/Company/Description tokens.

Lane / any inference source should only emit tickers in that pack or `not_in_pack`.

Does not invent direction. Family analyzers still own winners/losers. The pack fills the **who even exists** hole (TSA title → CAR via "Rental & Leasing", TSV → COIN via Capital Markets).

No flatten / Webull / factor-mine / Supabase changes.
