# Data source inventory (research only)
Compiled 2026-10-21 HKT. Builds on /workspace/free_data_sources.md (V = verified by live fetch, U = from docs only).
"Pre-open usable" = value known and timestamped before 09:25 ET on the trading day. Cost: all $0 here (Finviz Elite already paid).

## Finviz Elite: what fullscan already uses vs not
Already used: `export.ashx?v=111` (overview price, src/elite_live_px.py), `v=151` custom export (finviz_learning_capture), `quote.ashx` pages, `insidertrading.ashx` (src/insider_fetch.py), financials export (collectors/finviz_financials, schedule disabled).

| Finviz field / export | Used now? | History? | Pre-open usable | Notes |
|---|---|---|---|---|
| v=121 valuation (P/E, fwd P/E, PEG, P/S, P/B, EPS growth) | partly via v=151 | No: snapshot only | Yes if captured after close D-1 | **Now snapshotted daily** |
| v=131 ownership (Short Float, Short Ratio, Inst Own/Trans, Insider Own/Trans, Float) | No | No (short float updates ~2×/month on FINRA dates) | Yes, but short float is ~2 wks stale | **Now snapshotted daily** |
| v=141 performance (W/M/Q/H/Y perf, volatility W/M, rel volume) | No | Rebuildable from prices | Yes | Snapshotted daily |
| v=161 financial (margins, ROE, debt, earnings date column) | No | No | Yes | Earnings date as a forward schedule; snapshotted daily |
| v=171 technical (beta, ATR, SMA20/50/200 dist, RSI14, 52w hi/lo, gap) | No | Rebuildable from prices | Yes | Snapshotted daily |
| Options / IV columns | n/a | **Finviz has no IV/options chain export** (only an "Optionable/Shortable" filter) | n/a | Use Yahoo/DoltHub instead |
| Analyst recom (1–5) + target price | v=151 partly | No (current value only); quote page shows recent upgrade/downgrade rows with dates | Yes for dated rows | Collect forward from snapshots; the recom history builds up |
| Insider trades (insidertrading.ashx) | Yes | Recent rows only (U) | Yes via SEC acceptance time | SEC Form 4 is the better history source |
| News timestamps (quote page / news export) | quote pages only | Recent only | Yes if headline time < 09:25 ET | Not added to snapshots yet (per-ticker scrape too heavy) |
| Institutional ownership | No | No (13F-derived, quarterly) | Yes, stale | In v=131 snapshot |
| All other screener fields (sector, industry, country, mkt cap, earnings-date filter, etc.) | partly | No | Yes | v=111 snapshot holds them daily |

## Free external sources
| Source | History depth | Pre-open usable (lag / timestamps) | Cost | Status |
|---|---|---|---|---|
| DoltHub post-no-preference/options (IV, greeks, IV rank) | 2019-02 → now (V), ~1.5k names, M/W/F before ~Oct 2024 | Yes for D+1 (EOD values, no capture time) | Free | Best free per-stock IV/skew history; **no OI/volume** |
| Yahoo option chains (yfinance) | **None** (today only) | Yes if captured after close | Free | **Now snapshotted daily**, top 300 names, 4 expiries, OI + volume + IV |
| FINRA Reg SHO daily short volume | 2018-08 → now (V); per-venue to 2009 (U) | Yes: published evening D, use D+1 | Free | Backfillable, no snapshot needed |
| SEC FTD | 2004 → now (V) | Lagged only (~2 wk publication) | Free (User-Agent required) | Backfillable |
| SEC Form 3/4/5 insider data sets | 2006Q1 → now (V) | Yes if EDGAR acceptance time < 09:25 ET | Free | Backfillable |
| SEC 13F data sets | 2013 → now (U) | 45-day lag, slow | Free | Backfillable |
| Cboe VIX/VIX9D/VVIX/SKEW/VIX3M CSV | 1990/2000s → now (V for VIX) | Yes (D-1 close) | Free | Market regime only; put/call archives are market-wide (U) |
| FRED | decades | Yes (D-1, some series lag) | Free API key | Regime only; already collect-macro-fred.yml |
| Nasdaq Data Link / Quandl | WIKI prices ended 2018 (U) | n/a | Free tier mostly dead | Not useful |
| Google Trends | daily only for ≤9-mo windows, needs stitching (U) | Partial (daily values settle late) | Free, rate-limited | Low priority |
| Paid (not bought): HistoricalData.net $99+, ORATS $199/mo, OptionMetrics (WRDS) | 2002 → | — | Paid | Skipped per rule |

## Collecting now (add-only)
Workflow `.github/workflows/data_snapshots_daily.yml`, ubuntu-latest, cron 21:40 UTC Mon–Fri (17:40 EDT / 16:40 EST), with an in-job 03:00–09:40 ET freeze guard. Writes only `data/snapshots/{finviz,yahoo_options}/YYYY-MM-DD/*.csv.gz`, refuses to overwrite and refuses any non-add git change. Every row starts with `captured_at_utc`.

## Top 3 most likely to help
1. **Yahoo option chains, archived daily**: the only free per-stock OI / put-call / volume path; it fits swing_confluence_v1 (move size vs IV). Value grows over time.
2. **DoltHub options IV history**: 7 years of per-stock IV/IV rank/skew for ~1.5k names, so move-size vs implied tests are possible now.
3. **FINRA daily short volume + SEC Form 4**: full universe, multi-year, timestamp-safe before the open.
