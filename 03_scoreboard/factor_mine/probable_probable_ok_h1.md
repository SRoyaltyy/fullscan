# Factor mine action — `probable_probable_ok_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-1.69%** ($9,831) · signal-only (no cash/fees) was +1.91%. Starts YES **7/17**. Fills 72 · skips 32 · realized $-613.49.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True,ret_5_max=10.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $116.96.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ANGX` | 464 | — | $4.31 | +0.00 | $4.37 | +27.84 | +27.84 | +0.00 | +27.84 |
| 2026-08-14 | `HYLN` | 478 | — | $4.18 | +0.00 | $4.06 | -57.36 | -57.36 | +0.00 | -57.36 |
| 2026-08-14 | `WDC` | 3 | — | $503.50 | +0.00 | $508.80 | +15.90 | +15.90 | +0.00 | +15.90 |
| 2026-08-14 | `ADUR` | 121 | — | $16.50 | +0.00 | $16.17 | -39.93 | -39.93 | +0.00 | -39.93 |
| 2026-08-14 | `ALGM` | 45 | — | $44.06 | +0.00 | $44.39 | +14.85 | +14.85 | +0.00 | +14.85 |
| 2026-08-17 | `ANGX` | 464 | $4.37 | $4.60 | +106.72 | — | +0.00 | +106.72 | +134.56 | — |
| 2026-08-17 | `HYLN` | 478 | $4.06 | $4.10 | +19.12 | — | +0.00 | +19.12 | -38.24 | — |
| 2026-08-17 | `WDC` | 3 | $508.80 | $525.53 | +50.19 | — | +0.00 | +50.19 | +66.09 | — |
| 2026-08-17 | `ADUR` | 121 | $16.17 | $15.73 | -53.24 | — | +0.00 | -53.24 | -93.17 | — |
| 2026-08-17 | `ALGM` | 45 | $44.39 | $45.32 | +41.85 | — | +0.00 | +41.85 | +56.70 | — |
| 2026-08-17 | `CDNL` | 42 | — | $39.85 | +0.00 | $39.23 | -26.04 | -26.04 | +0.00 | -26.04 |
| 2026-08-17 | `ABX` | 184 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `VERA` | 53 | — | $31.30 | +0.00 | $31.63 | +17.49 | +17.49 | +0.00 | +17.49 |
| 2026-08-17 | `CELC` | 18 | — | $92.99 | +0.00 | $92.44 | -9.90 | -9.90 | +0.00 | -9.90 |
| 2026-08-17 | `OCC` | 92 | — | $18.24 | +0.00 | $17.12 | -103.04 | -103.04 | +0.00 | -103.04 |
| 2026-08-17 | `ALM` | 103 | — | $16.20 | +0.00 | $16.36 | +16.48 | +16.48 | +0.00 | +16.48 |
| 2026-08-18 | `CDNL` | 42 | $39.23 | $41.57 | +98.28 | — | +0.00 | +98.28 | +72.24 | — |
| 2026-08-18 | `ABX` | 184 | $9.12 | $9.03 | -16.56 | — | +0.00 | -16.56 | -16.56 | — |
| 2026-08-18 | `VERA` | 53 | $31.63 | $31.31 | -16.96 | — | +0.00 | -16.96 | +0.53 | — |
| 2026-08-18 | `CELC` | 18 | $92.44 | $92.38 | -1.08 | — | +0.00 | -1.08 | -10.98 | — |
| 2026-08-18 | `OCC` | 92 | $17.12 | $16.20 | -84.64 | — | +0.00 | -84.64 | -187.68 | — |
| 2026-08-18 | `ALM` | 103 | $16.36 | $15.78 | -59.74 | — | +0.00 | -59.74 | -43.26 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `DNA` | 189 | — | $7.45 | +0.00 | $6.96 | -92.61 | -92.61 | +0.00 | -92.61 |
| 2026-08-20 | `MSTR` | 12 | — | $113.23 | +0.00 | $112.39 | -10.08 | -10.08 | +0.00 | -10.08 |
| 2026-08-20 | `EXK` | 130 | — | $10.77 | +0.00 | $10.97 | +26.00 | +26.00 | +0.00 | +26.00 |
| 2026-08-20 | `SCZM` | 149 | — | $9.46 | +0.00 | $9.76 | +44.70 | +44.70 | +0.00 | +44.70 |
| 2026-08-20 | `NG` | 168 | — | $8.38 | +0.00 | $8.66 | +47.04 | +47.04 | +0.00 | +47.04 |
| 2026-08-20 | `BLSH` | 48 | — | $29.20 | +0.00 | $28.44 | -36.48 | -36.48 | +0.00 | -36.48 |
| 2026-08-20 | `HYMC` | 51 | — | $27.25 | +0.00 | $26.14 | -56.61 | -56.61 | +0.00 | -56.61 |
| 2026-08-21 | `DNA` | 189 | $6.96 | $7.09 | +24.57 | — | +0.00 | +24.57 | -68.04 | — |
| 2026-08-21 | `MSTR` | 12 | $112.39 | $119.69 | +87.60 | — | +0.00 | +87.60 | +77.52 | — |
| 2026-08-21 | `EXK` | 130 | $10.97 | $11.34 | +48.10 | — | +0.00 | +48.10 | +74.10 | — |
| 2026-08-21 | `SCZM` | 149 | $9.76 | $10.26 | +74.50 | — | +0.00 | +74.50 | +119.20 | — |
| 2026-08-21 | `NG` | 168 | $8.66 | $9.02 | +60.48 | — | +0.00 | +60.48 | +107.52 | — |
| 2026-08-21 | `BLSH` | 48 | $28.44 | $29.75 | +62.88 | — | +0.00 | +62.88 | +26.40 | — |
| 2026-08-21 | `HYMC` | 51 | $26.14 | $27.40 | +64.26 | — | +0.00 | +64.26 | +7.65 | — |
| 2026-08-21 | `BTBT` | 1227 | — | $1.66 | +0.00 | $1.53 | -159.51 | -159.51 | +0.00 | -159.51 |
| 2026-08-21 | `DE` | 3 | — | $623.26 | +0.00 | $647.47 | +72.63 | +72.63 | +0.00 | +72.63 |
| 2026-08-21 | `QDEL` | 136 | — | $14.96 | +0.00 | $14.74 | -29.92 | -29.92 | +0.00 | -29.92 |
| 2026-08-21 | `ORBS` | 2358 | — | $0.86 | +0.00 | $0.88 | +37.73 | +37.73 | +0.00 | +37.73 |
| 2026-08-21 | `GORO` | 655 | — | $3.11 | +0.00 | $3.19 | +52.40 | +52.40 | +0.00 | +52.40 |
| 2026-08-24 | `BTBT` | 1227 | $1.53 | $1.55 | +24.54 | — | +0.00 | +24.54 | -134.97 | — |
| 2026-08-24 | `DE` | 3 | $647.47 | $653.62 | +18.45 | — | +0.00 | +18.45 | +91.08 | — |
| 2026-08-24 | `QDEL` | 136 | $14.74 | $14.71 | -4.08 | — | +0.00 | -4.08 | -34.00 | — |
| 2026-08-24 | `ORBS` | 2358 | $0.88 | $0.89 | +23.58 | — | +0.00 | +23.58 | +61.31 | — |
| 2026-08-24 | `GORO` | 655 | $3.19 | $3.20 | +6.55 | — | +0.00 | +6.55 | +58.95 | — |
| 2026-08-25 | `NPWR` | 1264 | — | $2.00 | +0.00 | $2.02 | +25.28 | +25.28 | +0.00 | +25.28 |
| 2026-08-25 | `ALVO` | 484 | — | $5.22 | +0.00 | $5.25 | +14.52 | +14.52 | +0.00 | +14.52 |
| 2026-08-25 | `ALIT` | 170 | — | $14.86 | +0.00 | $14.87 | +1.70 | +1.70 | +0.00 | +1.70 |
| 2026-08-25 | `ZURA` | 392 | — | $6.38 | +0.00 | $6.50 | +47.04 | +47.04 | +0.00 | +47.04 |
| 2026-08-26 | `NPWR` | 1264 | $2.02 | $2.02 | +0.00 | $2.02 | +0.00 | +0.00 | +25.28 | +25.28 |
| 2026-08-26 | `ALVO` | 484 | $5.25 | $5.25 | +0.00 | $5.25 | +0.00 | +0.00 | +14.52 | +14.52 |
| 2026-08-26 | `ALIT` | 170 | $14.87 | $14.87 | +0.00 | $14.87 | +0.00 | +0.00 | +1.70 | +1.70 |
| 2026-08-26 | `ZURA` | 392 | $6.50 | $6.50 | +0.00 | $6.50 | +0.00 | +0.00 | +47.04 | +47.04 |
| 2026-08-27 | `NPWR` | 1264 | $2.02 | $1.93 | -113.76 | — | +0.00 | -113.76 | -88.48 | — |
| 2026-08-27 | `ALVO` | 484 | $5.25 | $4.98 | -130.68 | — | +0.00 | -130.68 | -116.16 | — |
| 2026-08-27 | `ALIT` | 170 | $14.87 | $14.85 | -3.40 | — | +0.00 | -3.40 | -1.70 | — |
| 2026-08-27 | `ZURA` | 392 | $6.50 | $6.13 | -145.04 | — | +0.00 | -145.04 | -98.00 | — |
| 2026-08-28 | `ANF` | 13 | — | $144.70 | +0.00 | $145.75 | +13.65 | +13.65 | +0.00 | +13.65 |
| 2026-08-28 | `BHVN` | 115 | — | $16.95 | +0.00 | $16.12 | -95.45 | -95.45 | +0.00 | -95.45 |
| 2026-08-28 | `BZ` | 105 | — | $18.50 | +0.00 | $18.00 | -52.50 | -52.50 | +0.00 | -52.50 |
| 2026-08-28 | `LVWR` | 1413 | — | $1.38 | +0.00 | $1.36 | -28.26 | -28.26 | +0.00 | -28.26 |
| 2026-08-28 | `GRRR` | 122 | — | $15.94 | +0.00 | $15.66 | -34.16 | -34.16 | +0.00 | -34.16 |
| 2026-08-31 | `ANF` | 13 | $145.75 | $148.67 | +37.96 | — | +0.00 | +37.96 | +51.61 | — |
| 2026-08-31 | `BHVN` | 115 | $16.12 | $15.44 | -78.20 | — | +0.00 | -78.20 | -173.65 | — |
| 2026-08-31 | `BZ` | 105 | $18.00 | $17.89 | -11.55 | — | +0.00 | -11.55 | -64.05 | — |
| 2026-08-31 | `LVWR` | 1413 | $1.36 | $1.37 | +14.13 | — | +0.00 | +14.13 | -14.13 | — |
| 2026-08-31 | `GRRR` | 122 | $15.66 | $14.32 | -163.48 | — | +0.00 | -163.48 | -197.64 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 2540 | — | $1.22 | +0.00 | $1.69 | +1193.80 | +1193.80 | +0.00 | +1193.80 |
| 2026-09-03 | `CRK` | 197 | — | $15.70 | +0.00 | $15.54 | -31.52 | -31.52 | +0.00 | -31.52 |
| 2026-09-03 | `MMED` | 134 | — | $22.78 | +0.00 | $23.76 | +131.32 | +131.32 | +0.00 | +131.32 |
| 2026-09-04 | `GPRO` | 2540 | $1.69 | $1.78 | +228.60 | $1.39 | -990.60 | -762.00 | +1422.40 | +431.80 |
| 2026-09-04 | `CRK` | 197 | $15.54 | $15.45 | -17.73 | — | +0.00 | -17.73 | -49.25 | — |
| 2026-09-04 | `MMED` | 134 | $23.76 | $23.88 | +16.08 | — | +0.00 | +16.08 | +147.40 | — |
| 2026-09-04 | `BAK` | 1069 | — | $1.95 | +0.00 | $1.94 | -10.69 | -10.69 | +0.00 | -10.69 |
| 2026-09-04 | `EOSE` | 584 | — | $3.57 | +0.00 | $3.50 | -40.88 | -40.88 | +0.00 | -40.88 |
| 2026-09-04 | `DELL` | 4 | — | $486.31 | +0.00 | $516.39 | +120.32 | +120.32 | +0.00 | +120.32 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -38.70 | ANGX, HYLN, WDC, ADUR, ALGM | — | $493.79 | $9,942.67 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 |
| 2026-08-17 | +2.25 | $493.79 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 | $10,107.31 | +164.64 | -105.01 | CDNL, ABX, VERA, CELC, OCC, ALM | ANGX, HYLN, WDC, ADUR, ALGM | $43.81 | $9,969.98 | CDNL×42, ABX×184, VERA×53, CELC×18, OCC×92, ALM×103 |
| 2026-08-18 | -6.20 | $43.81 | CDNL×42, ABX×184, VERA×53, CELC×18, OCC×92, ALM×103 | $9,889.28 | -80.70 | +0.00 | — | CDNL, ABX, VERA, CELC, OCC, ALM | $9,875.70 | $9,875.70 | — |
| 2026-08-19 | -7.20 | $9,875.70 | — | $9,875.70 | -0.00 | +0.00 | — | — | $9,875.70 | $9,875.70 | — |
| 2026-08-20 | +1.12 | $9,875.70 | — | $9,875.70 | -0.00 | -78.04 | DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | — | $83.88 | $9,781.48 | DNA×189, MSTR×12, EXK×130, SCZM×149, NG×168, BLSH×48, HYMC×51 |
| 2026-08-21 | +3.25 | $83.88 | DNA×189, MSTR×12, EXK×130, SCZM×149, NG×168, BLSH×48, HYMC×51 | $10,203.87 | +422.39 | -26.67 | BTBT, DE, QDEL, ORBS, GORO | DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | $115.84 | $10,104.69 | BTBT×1227, DE×3, QDEL×136, ORBS×2358, GORO×655 |
| 2026-08-24 | -5.17 | $115.84 | BTBT×1227, DE×3, QDEL×136, ORBS×2358, GORO×655 | $10,173.73 | +69.04 | +0.00 | — | BTBT, DE, QDEL, ORBS, GORO | $10,116.18 | $10,116.18 | — |
| 2026-08-25 | +1.80 | $10,116.18 | — | $10,116.18 | +0.00 | +88.54 | NPWR, ALVO, ALIT, ZURA | — | $4.44 | $10,174.62 | NPWR×1264, ALVO×484, ALIT×170, ZURA×392 |
| 2026-08-26 | +2.02 | $4.44 | NPWR×1264, ALVO×484, ALIT×170, ZURA×392 | $10,174.62 | -0.00 | +0.00 | — | — | $4.44 | $10,174.62 | NPWR×1264, ALVO×484, ALIT×170, ZURA×392 |
| 2026-08-27 | — | $4.44 | NPWR×1264, ALVO×484, ALIT×170, ZURA×392 | $9,781.74 | -392.89 | +0.00 | — | NPWR, ALVO, ALIT, ZURA | $9,751.17 | $9,751.17 | — |
| 2026-08-28 | +0.75 | $9,751.17 | — | $9,751.17 | -0.00 | -196.72 | ANF, BHVN, BZ, LVWR, GRRR | — | $56.44 | $9,527.19 | ANF×13, BHVN×115, BZ×105, LVWR×1413, GRRR×122 |
| 2026-08-31 | -5.85 | $56.44 | ANF×13, BHVN×115, BZ×105, LVWR×1413, GRRR×122 | $9,326.05 | -201.14 | +0.00 | — | ANF, BHVN, BZ, LVWR, GRRR | $9,298.43 | $9,298.43 | — |
| 2026-09-01 | -6.30 | $9,298.43 | — | $9,298.43 | -0.00 | +0.00 | — | — | $9,298.43 | $9,298.43 | — |
| 2026-09-02 | -3.83 | $9,298.43 | — | $9,298.43 | -0.00 | +0.00 | — | — | $9,298.43 | $9,298.43 | — |
| 2026-09-03 | -0.90 | $9,298.43 | — | $9,298.43 | -0.00 | +1,293.60 | GPRO, CRK, MMED | — | $16.47 | $10,554.29 | GPRO×2540, CRK×197, MMED×134 |
| 2026-09-04 | — | $16.47 | GPRO×2540, CRK×197, MMED×134 | $10,781.24 | +226.95 | -921.85 | BAK, EOSE, DELL | CRK, MMED | $116.96 | $9,830.98 | GPRO×2540, BAK×1069, EOSE×584, DELL×4 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 478 | $4.18 | $6.17 | — | $5,989.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 3 | $503.50 | $2.00 | — | $4,477.47 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ⚪; ret5=+7.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 121 | $16.50 | $2.35 | — | $2,478.62 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 45 | $44.06 | $2.12 | — | $493.79 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $493.79 | ▼ close $9,942.67 vs 09:30 $10,000.00 (session -38.70) | 16:00 close · cash $493.79 · equity $9,942.67 vs 09:30 $10,000.00 (-57.33; session marks -38.70) · 5 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.31 → close $4.37 +27.84; HYLN×478 09:30 $4.18 → close $4.06 -57.36; WDC×3 09:30 $503.50 → close $508.80 +15.90; ADUR×121 09:30 $16.50 → close $16.17 -39.93; ALGM×45 09:30 $44.06 → close $44.39 +14.85 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $493.79 | ▲ 09:30 equity $10,107.31 vs yday $9,942.67 (+164.64) | 09:30 open · cash $493.79 (unchanged overnight, no fees) · equity $10,107.31 vs prior close $9,942.67 (+164.64) · 5 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; HYLN×478 yday $4.06 → 09:30 $4.10 +19.12; WDC×3 yday $508.80 → 09:30 $525.53 +50.19; ADUR×121 yday $16.17 → 09:30 $15.73 -53.24; ALGM×45 yday $44.39 → 09:30 $45.32 +41.85 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 464 | $4.60 | $6.08 | $+122.49 | $2,622.11 | ▲ +122.49 after sell → book $10,101.23; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 478 | $4.10 | $6.26 | $-50.67 | $4,575.65 | ▼ -50.67 after sell → book $10,094.97; vs 09:30 mark -6.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 3 | $525.53 | $2.02 | $+62.07 | $6,150.22 | ▲ +62.07 after sell → book $10,092.95; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 121 | $15.73 | $2.39 | $-97.91 | $8,051.16 | ▼ -97.91 after sell → book $10,090.56; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 45 | $45.32 | $2.15 | $+52.42 | $10,088.41 | ▲ +52.42 after sell → book $10,088.41; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 42 | $39.85 | $2.12 | — | $8,412.59 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1681.40 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 184 | $9.12 | $2.54 | — | $6,731.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1681.40 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 53 | $31.30 | $2.15 | — | $5,070.92 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-3.8; leftover $1681.40 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 18 | $92.99 | $2.04 | — | $3,395.06 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.8; leftover $1681.40 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 92 | $18.24 | $2.27 | — | $1,714.71 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1681.40 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 103 | $16.20 | $2.30 | — | $43.81 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1681.40 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.81 | ▼ close $9,969.98 vs 09:30 $10,107.31 (session -105.01) | 16:00 close · cash $43.81 · equity $9,969.98 vs 09:30 $10,107.31 (-137.33; session marks -105.01) · 6 name(s) marked open→close (per-name table). CDNL×42 09:30 $39.85 → close $39.23 -26.04; ABX×184 09:30 $9.12 → close $9.12 +0.00; VERA×53 09:30 $31.30 → close $31.63 +17.49; CELC×18 09:30 $92.99 → close $92.44 -9.90; OCC×92 09:30 $18.24 → close $17.12 -103.04; ALM×103 09:30 $16.20 → close $16.36 +16.48 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.81 | ▼ 09:30 equity $9,889.28 vs yday $9,969.98 (-80.70) | 09:30 open · cash $43.81 (unchanged overnight, no fees) · equity $9,889.28 vs prior close $9,969.98 (-80.70) · 6 name(s) re-marked at the open (per-name table). CDNL×42 yday $39.23 → 09:30 $41.57 +98.28; ABX×184 yday $9.12 → 09:30 $9.03 -16.56; VERA×53 yday $31.63 → 09:30 $31.31 -16.96; CELC×18 yday $92.44 → 09:30 $92.38 -1.08; OCC×92 yday $17.12 → 09:30 $16.20 -84.64; ALM×103 yday $16.36 → 09:30 $15.78 -59.74 | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 42 | $41.57 | $2.14 | $+67.98 | $1,787.61 | ▲ +67.98 after sell → book $9,887.14; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 184 | $9.03 | $2.59 | $-21.69 | $3,446.55 | ▼ -21.69 after sell → book $9,884.56; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 53 | $31.31 | $2.17 | $-3.79 | $5,103.81 | ▼ -3.79 after sell → book $9,882.39; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 18 | $92.38 | $2.07 | $-15.09 | $6,764.58 | ▼ -15.09 after sell → book $9,880.32; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 92 | $16.20 | $2.29 | $-192.24 | $8,252.68 | ▼ -192.24 after sell → book $9,878.02; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 103 | $15.78 | $2.33 | $-47.89 | $9,875.70 | ▼ -47.89 after sell → book $9,875.70; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,875.70 | ▲ close $9,875.70 vs 09:30 $9,889.28 (session +0.00) | 16:00 close · cash $9,875.70 · no lots left · equity $9,875.70. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,875.70 | ▲ 09:30 equity $9,875.70 vs yday $9,875.70 (-0.00) | 09:30 open · cash $9,875.70 · no holdings · equity $9,875.70 vs prior close $9,875.70 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,875.70 | ▲ close $9,875.70 vs 09:30 $9,875.70 (session +0.00) | 16:00 close · cash $9,875.70 · no lots left · equity $9,875.70. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,875.70 | ▲ 09:30 equity $9,875.70 vs yday $9,875.70 (-0.00) | 09:30 open · cash $9,875.70 · no holdings · equity $9,875.70 vs prior close $9,875.70 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 189 | $7.45 | $2.56 | — | $8,465.09 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1410.81 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 12 | $113.23 | $2.03 | — | $7,104.30 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1410.81 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 130 | $10.77 | $2.38 | — | $5,701.82 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1410.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 149 | $9.46 | $2.44 | — | $4,289.85 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1410.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 168 | $8.38 | $2.49 | — | $2,879.51 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1410.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 48 | $29.20 | $2.13 | — | $1,475.78 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1410.81 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HYMC` | 51 | $27.25 | $2.14 | — | $83.88 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+1.6; leftover $1410.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.88 | ▼ close $9,781.48 vs 09:30 $9,875.70 (session -78.04) | 16:00 close · cash $83.88 · equity $9,781.48 vs 09:30 $9,875.70 (-94.22; session marks -78.04) · 7 name(s) marked open→close (per-name table). DNA×189 09:30 $7.45 → close $6.96 -92.61; MSTR×12 09:30 $113.23 → close $112.39 -10.08; EXK×130 09:30 $10.77 → close $10.97 +26.00; SCZM×149 09:30 $9.46 → close $9.76 +44.70; NG×168 09:30 $8.38 → close $8.66 +47.04; BLSH×48 09:30 $29.20 → close $28.44 -36.48; HYMC×51 09:30 $27.25 → close $26.14 -56.61 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.88 | ▲ 09:30 equity $10,203.87 vs yday $9,781.48 (+422.39) | 09:30 open · cash $83.88 (unchanged overnight, no fees) · equity $10,203.87 vs prior close $9,781.48 (+422.39) · 7 name(s) re-marked at the open (per-name table). DNA×189 yday $6.96 → 09:30 $7.09 +24.57; MSTR×12 yday $112.39 → 09:30 $119.69 +87.60; EXK×130 yday $10.97 → 09:30 $11.34 +48.10; SCZM×149 yday $9.76 → 09:30 $10.26 +74.50; NG×168 yday $8.66 → 09:30 $9.02 +60.48; BLSH×48 yday $28.44 → 09:30 $29.75 +62.88; HYMC×51 yday $26.14 → 09:30 $27.40 +64.26 | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 189 | $7.09 | $2.60 | $-73.20 | $1,421.30 | ▼ -73.20 after sell → book $10,201.28; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 12 | $119.69 | $2.05 | $+73.45 | $2,855.53 | ▲ +73.45 after sell → book $10,199.23; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 130 | $11.34 | $2.41 | $+69.31 | $4,327.31 | ▲ +69.31 after sell → book $10,196.81; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 149 | $10.26 | $2.47 | $+114.29 | $5,853.58 | ▲ +114.29 after sell → book $10,194.34; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NG` | 168 | $9.02 | $2.53 | $+102.49 | $7,366.41 | ▲ +102.49 after sell → book $10,191.81; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 48 | $29.75 | $2.16 | $+22.11 | $8,792.25 | ▲ +22.11 after sell → book $10,189.65; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYMC` | 51 | $27.40 | $2.16 | $+3.34 | $10,187.49 | ▲ +3.34 after sell → book $10,187.49; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 1227 | $1.66 | $15.83 | — | $8,134.84 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $2037.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 3 | $623.26 | $2.00 | — | $6,263.06 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $2037.50 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 136 | $14.96 | $2.40 | — | $4,226.10 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-1.6; leftover $2037.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 2358 | $0.86 | $27.45 | — | $2,161.34 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $2037.50 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 655 | $3.11 | $8.45 | — | $115.84 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+7.1; leftover $2037.50 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $115.84 | ▼ close $10,104.69 vs 09:30 $10,203.87 (session -26.67) | 16:00 close · cash $115.84 · equity $10,104.69 vs 09:30 $10,203.87 (-99.18; session marks -26.67) · 5 name(s) marked open→close (per-name table). BTBT×1227 09:30 $1.66 → close $1.53 -159.51; DE×3 09:30 $623.26 → close $647.47 +72.63; QDEL×136 09:30 $14.96 → close $14.74 -29.92; ORBS×2358 09:30 $0.86 → close $0.88 +37.73; GORO×655 09:30 $3.11 → close $3.19 +52.40 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.84 | ▲ 09:30 equity $10,173.73 vs yday $10,104.69 (+69.04) | 09:30 open · cash $115.84 (unchanged overnight, no fees) · equity $10,173.73 vs prior close $10,104.69 (+69.04) · 5 name(s) re-marked at the open (per-name table). BTBT×1227 yday $1.53 → 09:30 $1.55 +24.54; DE×3 yday $647.47 → 09:30 $653.62 +18.45; QDEL×136 yday $14.74 → 09:30 $14.71 -4.08; ORBS×2358 yday $0.88 → 09:30 $0.89 +23.58; GORO×655 yday $3.19 → 09:30 $3.20 +6.55 | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 1227 | $1.55 | $16.05 | $-166.85 | $2,001.65 | ▼ -166.85 after sell → book $10,157.69; vs 09:30 mark -16.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 3 | $653.62 | $2.02 | $+87.06 | $3,960.48 | ▲ +87.06 after sell → book $10,155.66; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 136 | $14.71 | $2.44 | $-38.83 | $5,958.60 | ▼ -38.83 after sell → book $10,153.22; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 2358 | $0.89 | $28.47 | $+5.39 | $8,028.76 | ▲ +5.39 after sell → book $10,124.76; vs 09:30 mark -28.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 655 | $3.20 | $8.57 | $+41.93 | $10,116.18 | ▲ +41.93 after sell → book $10,116.18; vs 09:30 mark -8.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,116.18 | ▲ close $10,116.18 vs 09:30 $10,173.73 (session +0.00) | 16:00 close · cash $10,116.18 · no lots left · equity $10,116.18. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,116.18 | ▲ 09:30 equity $10,116.18 vs yday $10,116.18 (+0.00) | 09:30 open · cash $10,116.18 · no holdings · equity $10,116.18 vs prior close $10,116.18 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 1264 | $2.00 | $16.31 | — | $7,571.88 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $2529.05 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 484 | $5.22 | $6.24 | — | $5,039.15 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $2529.05 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 170 | $14.86 | $2.50 | — | $2,510.45 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $2529.05 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 392 | $6.38 | $5.06 | — | $4.44 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $2529.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.44 | ▲ close $10,174.62 vs 09:30 $10,116.18 (session +88.54) | 16:00 close · cash $4.44 · equity $10,174.62 vs 09:30 $10,116.18 (+58.44; session marks +88.54) · 4 name(s) marked open→close (per-name table). NPWR×1264 09:30 $2.00 → close $2.02 +25.28; ALVO×484 09:30 $5.22 → close $5.25 +14.52; ALIT×170 09:30 $14.86 → close $14.87 +1.70; ZURA×392 09:30 $6.38 → close $6.50 +47.04 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.44 | ▲ 09:30 equity $10,174.62 vs yday $10,174.62 (-0.00) | 09:30 open · cash $4.44 (unchanged overnight, no fees) · equity $10,174.62 vs prior close $10,174.62 (-0.00) · 4 name(s) re-marked at the open (per-name table). NPWR×1264 yday $2.02 → 09:30 $2.02 +0.00; ALVO×484 yday $5.25 → 09:30 $5.25 +0.00; ALIT×170 yday $14.87 → 09:30 $14.87 +0.00; ZURA×392 yday $6.50 → 09:30 $6.50 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.44 | ▲ close $10,174.62 vs 09:30 $10,174.62 (session +0.00) | 16:00 close · cash $4.44 · equity $10,174.62 vs 09:30 $10,174.62 (-0.00; session marks +0.00) · 4 name(s) marked open→close (per-name table). NPWR×1264 09:30 $2.02 → close $2.02 +0.00; ALVO×484 09:30 $5.25 → close $5.25 +0.00; ALIT×170 09:30 $14.87 → close $14.87 +0.00; ZURA×392 09:30 $6.50 → close $6.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.44 | ▼ 09:30 equity $9,781.74 vs yday $10,174.62 (-392.89) | 09:30 open · cash $4.44 (unchanged overnight, no fees) · equity $9,781.74 vs prior close $10,174.62 (-392.89) · 4 name(s) re-marked at the open (per-name table). NPWR×1264 yday $2.02 → 09:30 $1.93 -113.76; ALVO×484 yday $5.25 → 09:30 $4.98 -130.68; ALIT×170 yday $14.87 → 09:30 $14.85 -3.40; ZURA×392 yday $6.50 → 09:30 $6.13 -145.04 | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 1264 | $1.93 | $16.53 | $-121.32 | $2,427.42 | ▼ -121.32 after sell → book $9,765.20; vs 09:30 mark -16.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 484 | $4.98 | $6.34 | $-128.75 | $4,831.40 | ▼ -128.75 after sell → book $9,758.86; vs 09:30 mark -6.34 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALIT` | 170 | $14.85 | $2.55 | $-6.75 | $7,353.35 | ▼ -6.75 after sell → book $9,756.31; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 392 | $6.13 | $5.14 | $-108.20 | $9,751.17 | ▼ -108.20 after sell → book $9,751.17; vs 09:30 mark -5.14 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,751.17 | ▲ close $9,751.17 vs 09:30 $9,781.74 (session +0.00) | 16:00 close · cash $9,751.17 · no lots left · equity $9,751.17. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,751.17 | ▲ 09:30 equity $9,751.17 vs yday $9,751.17 (-0.00) | 09:30 open · cash $9,751.17 · no holdings · equity $9,751.17 vs prior close $9,751.17 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 13 | $144.70 | $2.03 | — | $7,868.04 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1950.23 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 115 | $16.95 | $2.33 | — | $5,916.45 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1950.23 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 105 | $18.50 | $2.31 | — | $3,971.65 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1950.23 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 1413 | $1.38 | $18.23 | — | $2,003.48 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1950.23 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 122 | $15.94 | $2.36 | — | $56.44 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1950.23 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.44 | ▼ close $9,527.19 vs 09:30 $9,751.17 (session -196.72) | 16:00 close · cash $56.44 · equity $9,527.19 vs 09:30 $9,751.17 (-223.98; session marks -196.72) · 5 name(s) marked open→close (per-name table). ANF×13 09:30 $144.70 → close $145.75 +13.65; BHVN×115 09:30 $16.95 → close $16.12 -95.45; BZ×105 09:30 $18.50 → close $18.00 -52.50; LVWR×1413 09:30 $1.38 → close $1.36 -28.26; GRRR×122 09:30 $15.94 → close $15.66 -34.16 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.44 | ▼ 09:30 equity $9,326.05 vs yday $9,527.19 (-201.14) | 09:30 open · cash $56.44 (unchanged overnight, no fees) · equity $9,326.05 vs prior close $9,527.19 (-201.14) · 5 name(s) re-marked at the open (per-name table). ANF×13 yday $145.75 → 09:30 $148.67 +37.96; BHVN×115 yday $16.12 → 09:30 $15.44 -78.20; BZ×105 yday $18.00 → 09:30 $17.89 -11.55; LVWR×1413 yday $1.36 → 09:30 $1.37 +14.13; GRRR×122 yday $15.66 → 09:30 $14.32 -163.48 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 13 | $148.67 | $2.05 | $+47.53 | $1,987.10 | ▲ +47.53 after sell → book $9,324.00; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 115 | $15.44 | $2.37 | $-178.35 | $3,760.33 | ▼ -178.35 after sell → book $9,321.63; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 105 | $17.89 | $2.34 | $-68.69 | $5,636.44 | ▼ -68.69 after sell → book $9,319.29; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 1413 | $1.37 | $18.48 | $-50.84 | $7,553.78 | ▼ -50.84 after sell → book $9,300.82; vs 09:30 mark -18.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 122 | $14.32 | $2.39 | $-202.39 | $9,298.43 | ▼ -202.39 after sell → book $9,298.43; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,298.43 | ▲ close $9,298.43 vs 09:30 $9,326.05 (session +0.00) | 16:00 close · cash $9,298.43 · no lots left · equity $9,298.43. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,298.43 | ▲ 09:30 equity $9,298.43 vs yday $9,298.43 (-0.00) | 09:30 open · cash $9,298.43 · no holdings · equity $9,298.43 vs prior close $9,298.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,298.43 | ▲ close $9,298.43 vs 09:30 $9,298.43 (session +0.00) | 16:00 close · cash $9,298.43 · no lots left · equity $9,298.43. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,298.43 | ▲ 09:30 equity $9,298.43 vs yday $9,298.43 (-0.00) | 09:30 open · cash $9,298.43 · no holdings · equity $9,298.43 vs prior close $9,298.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,298.43 | ▲ close $9,298.43 vs 09:30 $9,298.43 (session +0.00) | 16:00 close · cash $9,298.43 · no lots left · equity $9,298.43. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,298.43 | ▲ 09:30 equity $9,298.43 vs yday $9,298.43 (-0.00) | 09:30 open · cash $9,298.43 · no holdings · equity $9,298.43 vs prior close $9,298.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 2540 | $1.22 | $32.77 | — | $6,166.86 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $3099.48 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 197 | $15.70 | $2.58 | — | $3,071.38 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $3099.48 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 134 | $22.78 | $2.39 | — | $16.47 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $3099.48 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.47 | ▲ close $10,554.29 vs 09:30 $9,298.43 (session +1,293.60) | 16:00 close · cash $16.47 · equity $10,554.29 vs 09:30 $9,298.43 (+1255.86; session marks +1293.60) · 3 name(s) marked open→close (per-name table). GPRO×2540 09:30 $1.22 → close $1.69 +1193.80; CRK×197 09:30 $15.70 → close $15.54 -31.52; MMED×134 09:30 $22.78 → close $23.76 +131.32 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.47 | ▲ 09:30 equity $10,781.24 vs yday $10,554.29 (+226.95) | 09:30 open · cash $16.47 (unchanged overnight, no fees) · equity $10,781.24 vs prior close $10,554.29 (+226.95) · 3 name(s) re-marked at the open (per-name table). GPRO×2540 yday $1.69 → 09:30 $1.78 +228.60; CRK×197 yday $15.54 → 09:30 $15.45 -17.73; MMED×134 yday $23.76 → 09:30 $23.88 +16.08 | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 197 | $15.45 | $2.64 | $-54.47 | $3,057.48 | ▼ -54.47 after sell → book $10,778.60; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 134 | $23.88 | $2.44 | $+142.57 | $6,254.96 | ▲ +142.57 after sell → book $10,776.16; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1069 | $1.95 | $13.79 | — | $4,156.62 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $2084.99 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 584 | $3.57 | $7.53 | — | $2,064.21 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $2084.99 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 4 | $486.31 | $2.00 | — | $116.96 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $2084.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.96 | ▼ close $9,830.98 vs 09:30 $10,781.24 (session -921.85) | 16:00 close · cash $116.96 · equity $9,830.98 vs 09:30 $10,781.24 (-950.26; session marks -921.85) · 4 name(s) marked open→close (per-name table). GPRO×2540 09:30 $1.78 → close $1.39 -990.60; BAK×1069 09:30 $1.95 → close $1.94 -10.69; EOSE×584 09:30 $3.57 → close $3.50 -40.88; DELL×4 09:30 $486.31 → close $516.39 +120.32 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALIT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 2540 | 2026-09-03 @ $1.22 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $3099.48 |
| `BAK` | 1069 | 2026-09-04 @ $1.95 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $2084.99 |
| `EOSE` | 584 | 2026-09-04 @ $3.57 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $2084.99 |
| `DELL` | 4 | 2026-09-04 @ $486.31 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $2084.99 |
