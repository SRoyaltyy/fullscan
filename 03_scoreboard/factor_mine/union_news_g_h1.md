# Factor mine action — `union_news_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_g, no 🚨

Cash book **+3.57%** ($10,357) · signal-only (no cash/fees) was +3.34%. Starts YES **16/19**. Fills 127 · skips 56 · realized $+343.78.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the news camera (does the morning packet like the headline?) is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,100.07.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `TLN` | 3 | — | $359.83 | +0.00 | $362.74 | +8.73 | +8.73 | +0.00 | +8.73 |
| 2026-08-14 | `VST` | 8 | — | $146.90 | +0.00 | $148.13 | +9.84 | +9.84 | +0.00 | +9.84 |
| 2026-08-14 | `NRG` | 10 | — | $120.00 | +0.00 | $126.24 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `MH` | 92 | — | $13.55 | +0.00 | $13.10 | -41.40 | -41.40 | +0.00 | -41.40 |
| 2026-08-14 | `HLIT` | 94 | — | $13.18 | +0.00 | $13.92 | +69.56 | +69.56 | +0.00 | +69.56 |
| 2026-08-17 | `TLN` | 3 | $362.74 | $367.88 | +15.42 | — | +0.00 | +15.42 | +24.15 | — |
| 2026-08-17 | `VST` | 8 | $148.13 | $149.37 | +9.92 | — | +0.00 | +9.92 | +19.76 | — |
| 2026-08-17 | `NRG` | 10 | $126.24 | $127.40 | +11.60 | — | +0.00 | +11.60 | +74.00 | — |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `MH` | 92 | $13.10 | $13.16 | +5.52 | — | +0.00 | +5.52 | -35.88 | — |
| 2026-08-17 | `HLIT` | 94 | $13.92 | $13.84 | -7.52 | — | +0.00 | -7.52 | +62.04 | — |
| 2026-08-17 | `DVN` | 44 | — | $46.18 | +0.00 | $47.57 | +61.16 | +61.16 | +0.00 | +61.16 |
| 2026-08-17 | `EOG` | 14 | — | $142.77 | +0.00 | $146.15 | +47.32 | +47.32 | +0.00 | +47.32 |
| 2026-08-17 | `FANG` | 10 | — | $202.70 | +0.00 | $206.29 | +35.90 | +35.90 | +0.00 | +35.90 |
| 2026-08-17 | `CELC` | 21 | — | $92.99 | +0.00 | $92.44 | -11.55 | -11.55 | +0.00 | -11.55 |
| 2026-08-17 | `OUST` | 41 | — | $49.00 | +0.00 | $48.13 | -35.67 | -35.67 | +0.00 | -35.67 |
| 2026-08-18 | `DVN` | 44 | $47.57 | $48.00 | +18.92 | — | +0.00 | +18.92 | +80.08 | — |
| 2026-08-18 | `EOG` | 14 | $146.15 | $148.04 | +26.46 | — | +0.00 | +26.46 | +73.78 | — |
| 2026-08-18 | `FANG` | 10 | $206.29 | $208.93 | +26.40 | — | +0.00 | +26.40 | +62.30 | — |
| 2026-08-18 | `CELC` | 21 | $92.44 | $92.38 | -1.26 | — | +0.00 | -1.26 | -12.81 | — |
| 2026-08-18 | `OUST` | 41 | $48.13 | $45.09 | -124.64 | — | +0.00 | -124.64 | -160.31 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 14 | — | $91.01 | +0.00 | $93.63 | +36.68 | +36.68 | +0.00 | +36.68 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `HUMA` | 1806 | — | $0.71 | +0.00 | $0.68 | -46.96 | -46.96 | +0.00 | -46.96 |
| 2026-08-20 | `BTGO` | 193 | — | $6.61 | +0.00 | $6.60 | -0.97 | -0.97 | +0.00 | -0.97 |
| 2026-08-20 | `ASST` | 79 | — | $16.00 | +0.00 | $16.13 | +10.27 | +10.27 | +0.00 | +10.27 |
| 2026-08-20 | `ZLAB` | 48 | — | $26.57 | +0.00 | $26.02 | -26.40 | -26.40 | +0.00 | -26.40 |
| 2026-08-20 | `CRSP` | 21 | — | $58.73 | +0.00 | $58.12 | -12.81 | -12.81 | +0.00 | -12.81 |
| 2026-08-20 | `APA` | 28 | — | $44.76 | +0.00 | $44.39 | -10.36 | -10.36 | +0.00 | -10.36 |
| 2026-08-21 | `BHP` | 14 | $93.63 | $95.72 | +29.26 | — | +0.00 | +29.26 | +65.94 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `HUMA` | 1806 | $0.68 | $0.67 | -12.64 | — | +0.00 | -12.64 | -59.60 | — |
| 2026-08-21 | `BTGO` | 193 | $6.60 | $6.95 | +67.55 | — | +0.00 | +67.55 | +66.58 | — |
| 2026-08-21 | `ASST` | 79 | $16.13 | $17.66 | +120.87 | — | +0.00 | +120.87 | +131.14 | — |
| 2026-08-21 | `ZLAB` | 48 | $26.02 | $26.25 | +11.04 | — | +0.00 | +11.04 | -15.36 | — |
| 2026-08-21 | `CRSP` | 21 | $58.12 | $59.72 | +33.60 | $59.50 | -4.62 | +28.98 | +20.79 | +16.17 |
| 2026-08-21 | `APA` | 28 | $44.39 | $44.52 | +3.64 | — | +0.00 | +3.64 | -6.72 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUTL` | 518 | — | $2.47 | +0.00 | $2.41 | -31.08 | -31.08 | +0.00 | -31.08 |
| 2026-08-21 | `FUTU` | 11 | — | $115.18 | +0.00 | $123.64 | +93.06 | +93.06 | +0.00 | +93.06 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `MARA` | 109 | — | $11.70 | +0.00 | $11.26 | -47.96 | -47.96 | +0.00 | -47.96 |
| 2026-08-21 | `BTDR` | 115 | — | $11.10 | +0.00 | $11.37 | +31.62 | +31.62 | +0.00 | +31.62 |
| 2026-08-21 | `HIVE` | 395 | — | $3.24 | +0.00 | $3.03 | -82.95 | -82.95 | +0.00 | -82.95 |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.79 | -14.91 | $56.91 | -39.48 | -54.39 | +1.26 | -38.22 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.50 | -7.20 | — | +0.00 | -7.20 | +10.70 | — |
| 2026-08-24 | `AUTL` | 518 | $2.41 | $2.36 | -25.90 | — | +0.00 | -25.90 | -56.98 | — |
| 2026-08-24 | `FUTU` | 11 | $123.64 | $120.87 | -30.47 | — | +0.00 | -30.47 | +62.59 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.62 | +12.30 | — | +0.00 | +12.30 | +60.72 | — |
| 2026-08-24 | `MARA` | 109 | $11.26 | $11.18 | -8.72 | — | +0.00 | -8.72 | -56.68 | — |
| 2026-08-24 | `BTDR` | 115 | $11.37 | $11.49 | +13.80 | — | +0.00 | +13.80 | +45.42 | — |
| 2026-08-24 | `HIVE` | 395 | $3.03 | $2.98 | -19.75 | — | +0.00 | -19.75 | -102.70 | — |
| 2026-08-25 | `CRSP` | 21 | $56.91 | $57.00 | +1.89 | — | +0.00 | +1.89 | -36.33 | — |
| 2026-08-25 | `RUM` | 134 | — | $9.36 | +0.00 | $9.35 | -1.34 | -1.34 | +0.00 | -1.34 |
| 2026-08-25 | `EZPW` | 36 | — | $34.48 | +0.00 | $34.69 | +7.56 | +7.56 | +0.00 | +7.56 |
| 2026-08-25 | `REAX` | 52 | — | $24.00 | +0.00 | $24.00 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `TRLV` | 114 | — | $11.02 | +0.00 | $11.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `VIRT` | 19 | — | $66.29 | +0.00 | $66.29 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `HOOD` | 11 | — | $106.00 | +0.00 | $104.22 | -19.58 | -19.58 | +0.00 | -19.58 |
| 2026-08-25 | `ZYME` | 42 | — | $29.87 | +0.00 | $29.81 | -2.52 | -2.52 | +0.00 | -2.52 |
| 2026-08-25 | `BKKT` | 152 | — | $8.28 | +0.00 | $8.38 | +15.20 | +15.20 | +0.00 | +15.20 |
| 2026-08-26 | `RUM` | 134 | $9.35 | $9.35 | +0.00 | $9.35 | +0.00 | +0.00 | -1.34 | -1.34 |
| 2026-08-26 | `EZPW` | 36 | $34.69 | $34.69 | +0.00 | $34.69 | +0.00 | +0.00 | +7.56 | +7.56 |
| 2026-08-26 | `REAX` | 52 | $24.00 | $24.00 | +0.00 | $24.00 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `TRLV` | 114 | $11.02 | $11.02 | +0.00 | $11.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `VIRT` | 19 | $66.29 | $66.29 | +0.00 | $66.29 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `HOOD` | 11 | $104.22 | $104.22 | +0.00 | $104.22 | +0.00 | +0.00 | -19.58 | -19.58 |
| 2026-08-26 | `ZYME` | 42 | $29.81 | $29.81 | +0.00 | $29.81 | +0.00 | +0.00 | -2.52 | -2.52 |
| 2026-08-26 | `BKKT` | 152 | $8.38 | $8.38 | +0.00 | $8.38 | +0.00 | +0.00 | +15.20 | +15.20 |
| 2026-08-27 | `RUM` | 134 | $9.35 | $10.07 | +96.48 | — | +0.00 | +96.48 | +95.14 | — |
| 2026-08-27 | `EZPW` | 36 | $34.69 | $35.70 | +36.36 | — | +0.00 | +36.36 | +43.92 | — |
| 2026-08-27 | `REAX` | 52 | $24.00 | $26.61 | +135.72 | — | +0.00 | +135.72 | +135.72 | — |
| 2026-08-27 | `TRLV` | 114 | $11.02 | $11.22 | +22.80 | — | +0.00 | +22.80 | +22.80 | — |
| 2026-08-27 | `VIRT` | 19 | $66.29 | $64.92 | -26.03 | — | +0.00 | -26.03 | -26.03 | — |
| 2026-08-27 | `HOOD` | 11 | $104.22 | $110.11 | +64.79 | — | +0.00 | +64.79 | +45.21 | — |
| 2026-08-27 | `ZYME` | 42 | $29.81 | $27.56 | -94.50 | — | +0.00 | -94.50 | -97.02 | — |
| 2026-08-27 | `BKKT` | 152 | $8.38 | $8.38 | +0.00 | — | +0.00 | +0.00 | +15.20 | — |
| 2026-08-27 | `RRC` | 42 | — | $40.72 | +0.00 | $41.55 | +34.86 | +34.86 | +0.00 | +34.86 |
| 2026-08-27 | `ACMR` | 21 | — | $80.97 | +0.00 | $79.11 | -39.06 | -39.06 | +0.00 | -39.06 |
| 2026-08-27 | `MU` | 1 | — | $925.74 | +0.00 | $938.40 | +12.66 | +12.66 | +0.00 | +12.66 |
| 2026-08-27 | `LRCX` | 5 | — | $314.61 | +0.00 | $312.88 | -8.65 | -8.65 | +0.00 | -8.65 |
| 2026-08-27 | `NVDA` | 8 | — | $212.64 | +0.00 | $209.66 | -23.84 | -23.84 | +0.00 | -23.84 |
| 2026-08-28 | `RRC` | 42 | $41.55 | $41.44 | -4.62 | $41.64 | +8.40 | +3.78 | +30.24 | +38.64 |
| 2026-08-28 | `ACMR` | 21 | $79.11 | $81.65 | +53.34 | — | +0.00 | +53.34 | +14.28 | — |
| 2026-08-28 | `MU` | 1 | $938.40 | $967.01 | +28.61 | — | +0.00 | +28.61 | +41.27 | — |
| 2026-08-28 | `LRCX` | 5 | $312.88 | $318.88 | +30.00 | — | +0.00 | +30.00 | +21.35 | — |
| 2026-08-28 | `NVDA` | 8 | $209.66 | $222.86 | +105.60 | — | +0.00 | +105.60 | +81.76 | — |
| 2026-08-28 | `CAPR` | 135 | — | $9.19 | +0.00 | $10.06 | +117.45 | +117.45 | +0.00 | +117.45 |
| 2026-08-28 | `SEDG` | 36 | — | $33.78 | +0.00 | $33.51 | -9.72 | -9.72 | +0.00 | -9.72 |
| 2026-08-28 | `SMTC` | 8 | — | $149.40 | +0.00 | $142.43 | -55.76 | -55.76 | +0.00 | -55.76 |
| 2026-08-28 | `OPTX` | 145 | — | $8.57 | +0.00 | $8.73 | +23.20 | +23.20 | +0.00 | +23.20 |
| 2026-08-28 | `ERAS` | 64 | — | $19.30 | +0.00 | $19.49 | +12.16 | +12.16 | +0.00 | +12.16 |
| 2026-08-28 | `BBWI` | 66 | — | $18.68 | +0.00 | $18.65 | -1.98 | -1.98 | +0.00 | -1.98 |
| 2026-08-28 | `ZYME` | 42 | — | $29.33 | +0.00 | $29.01 | -13.44 | -13.44 | +0.00 | -13.44 |
| 2026-08-31 | `RRC` | 42 | $41.64 | $41.11 | -22.26 | — | +0.00 | -22.26 | +16.38 | — |
| 2026-08-31 | `CAPR` | 135 | $10.06 | $9.44 | -83.70 | — | +0.00 | -83.70 | +33.75 | — |
| 2026-08-31 | `SEDG` | 36 | $33.51 | $31.50 | -72.36 | — | +0.00 | -72.36 | -82.08 | — |
| 2026-08-31 | `SMTC` | 8 | $142.43 | $133.04 | -75.12 | — | +0.00 | -75.12 | -130.88 | — |
| 2026-08-31 | `OPTX` | 145 | $8.73 | $8.52 | -30.45 | — | +0.00 | -30.45 | -7.25 | — |
| 2026-08-31 | `ERAS` | 64 | $19.49 | $17.90 | -101.76 | — | +0.00 | -101.76 | -89.60 | — |
| 2026-08-31 | `BBWI` | 66 | $18.65 | $19.30 | +42.90 | — | +0.00 | +42.90 | +40.92 | — |
| 2026-08-31 | `ZYME` | 42 | $29.01 | $28.27 | -31.08 | $28.27 | +0.00 | -31.08 | -44.52 | -44.52 |
| 2026-09-01 | `ZYME` | 42 | $28.27 | $29.32 | +44.10 | — | +0.00 | +44.10 | -0.42 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `MMED` | 55 | — | $22.78 | +0.00 | $23.76 | +53.90 | +53.90 | +0.00 | +53.90 |
| 2026-09-03 | `CNXC` | 39 | — | $31.80 | +0.00 | $32.37 | +22.23 | +22.23 | +0.00 | +22.23 |
| 2026-09-03 | `OPTX` | 175 | — | $7.25 | +0.00 | $7.53 | +49.00 | +49.00 | +0.00 | +49.00 |
| 2026-09-03 | `TRLV` | 107 | — | $11.78 | +0.00 | $11.69 | -9.63 | -9.63 | +0.00 | -9.63 |
| 2026-09-03 | `TXG` | 21 | — | $60.24 | +0.00 | $61.65 | +29.61 | +29.61 | +0.00 | +29.61 |
| 2026-09-03 | `ZYME` | 42 | — | $30.00 | +0.00 | $31.05 | +44.10 | +44.10 | +0.00 | +44.10 |
| 2026-09-03 | `FCX` | 17 | — | $73.04 | +0.00 | $73.93 | +15.13 | +15.13 | +0.00 | +15.13 |
| 2026-09-03 | `AVGO` | 3 | — | $369.68 | +0.00 | $367.24 | -7.32 | -7.32 | +0.00 | -7.32 |
| 2026-09-04 | `MMED` | 55 | $23.76 | $23.88 | +6.60 | — | +0.00 | +6.60 | +60.50 | — |
| 2026-09-04 | `CNXC` | 39 | $32.37 | $32.88 | +19.89 | — | +0.00 | +19.89 | +42.12 | — |
| 2026-09-04 | `OPTX` | 175 | $7.53 | $7.59 | +10.50 | — | +0.00 | +10.50 | +59.50 | — |
| 2026-09-04 | `TRLV` | 107 | $11.69 | $11.89 | +21.40 | — | +0.00 | +21.40 | +11.77 | — |
| 2026-09-04 | `TXG` | 21 | $61.65 | $62.35 | +14.70 | $63.22 | +18.27 | +32.97 | +44.31 | +62.58 |
| 2026-09-04 | `ZYME` | 42 | $31.05 | $31.34 | +12.18 | $29.90 | -60.48 | -48.30 | +56.28 | -4.20 |
| 2026-09-04 | `FCX` | 17 | $73.93 | $75.34 | +23.97 | $72.56 | -47.26 | -23.29 | +39.10 | -8.16 |
| 2026-09-04 | `AVGO` | 3 | $367.24 | $351.74 | -46.50 | — | +0.00 | -46.50 | -53.82 | — |
| 2026-09-04 | `CRM` | 6 | — | $261.98 | +0.00 | $264.43 | +14.70 | +14.70 | +0.00 | +14.70 |
| 2026-09-04 | `BAK` | 832 | — | $1.95 | +0.00 | $1.94 | -8.32 | -8.32 | +0.00 | -8.32 |
| 2026-09-04 | `AMTX` | 849 | — | $1.91 | +0.00 | $1.83 | -67.92 | -67.92 | +0.00 | -67.92 |
| 2026-09-04 | `FRNM` | 102 | — | $15.87 | +0.00 | $16.90 | +105.06 | +105.06 | +0.00 | +105.06 |
| 2026-09-07 | `TXG` | 21 | $63.22 | $62.54 | -14.28 | $62.65 | +2.31 | -11.97 | +48.30 | +50.61 |
| 2026-09-07 | `ZYME` | 42 | $29.90 | $29.81 | -3.78 | $29.19 | -26.04 | -29.82 | -7.98 | -34.02 |
| 2026-09-07 | `FCX` | 17 | $72.56 | $71.72 | -14.28 | $72.73 | +17.17 | +2.89 | -22.44 | -5.27 |
| 2026-09-07 | `CRM` | 6 | $264.43 | $263.36 | -6.42 | — | +0.00 | -6.42 | +8.28 | — |
| 2026-09-07 | `BAK` | 832 | $1.94 | $1.94 | +0.00 | — | +0.00 | +0.00 | -8.32 | — |
| 2026-09-07 | `AMTX` | 849 | $1.83 | $1.83 | +0.00 | — | +0.00 | +0.00 | -67.92 | — |
| 2026-09-07 | `FRNM` | 102 | $16.90 | $16.40 | -51.00 | — | +0.00 | -51.00 | +54.06 | — |
| 2026-09-07 | `CHPT` | 138 | — | $9.28 | +0.00 | $9.89 | +84.18 | +84.18 | +0.00 | +84.18 |
| 2026-09-07 | `SMMT` | 75 | — | $16.93 | +0.00 | $17.60 | +50.25 | +50.25 | +0.00 | +50.25 |
| 2026-09-07 | `SNOW` | 3 | — | $353.63 | +0.00 | $337.18 | -49.35 | -49.35 | +0.00 | -49.35 |
| 2026-09-07 | `MSTR` | 9 | — | $137.35 | +0.00 | $142.80 | +49.05 | +49.05 | +0.00 | +49.05 |
| 2026-09-07 | `MRX` | 16 | — | $75.65 | +0.00 | $78.27 | +41.92 | +41.92 | +0.00 | +41.92 |
| 2026-09-08 | `TXG` | 21 | $62.65 | $63.18 | +11.13 | — | +0.00 | +11.13 | +61.74 | — |
| 2026-09-08 | `ZYME` | 42 | $29.19 | $28.70 | -20.58 | — | +0.00 | -20.58 | -54.60 | — |
| 2026-09-08 | `FCX` | 17 | $72.73 | $73.93 | +20.40 | $73.93 | +0.00 | +20.40 | +15.13 | +15.13 |
| 2026-09-08 | `CHPT` | 138 | $9.89 | $9.98 | +12.42 | — | +0.00 | +12.42 | +96.60 | — |
| 2026-09-08 | `SMMT` | 75 | $17.60 | $17.38 | -16.50 | — | +0.00 | -16.50 | +33.75 | — |
| 2026-09-08 | `SNOW` | 3 | $337.18 | $336.00 | -3.54 | — | +0.00 | -3.54 | -52.89 | — |
| 2026-09-08 | `MSTR` | 9 | $142.80 | $139.92 | -25.92 | — | +0.00 | -25.92 | +23.13 | — |
| 2026-09-08 | `MRX` | 16 | $78.27 | $79.95 | +26.88 | — | +0.00 | +26.88 | +68.80 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +127.16 | TLN, VST, NRG, ANGX, ARX, MH, HLIT | — | $1,560.49 | $10,110.67 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 |
| 2026-08-17 | +2.25 | $1,560.49 | TLN×3, VST×8, NRG×10, ANGX×290, ARX×63, MH×92, HLIT×94 | $10,211.68 | +101.01 | +97.16 | DVN, EOG, FANG, CELC, OUST | TLN, VST, NRG, ANGX, ARX, MH, HLIT | $165.17 | $10,281.82 | DVN×44, EOG×14, FANG×10, CELC×21, OUST×41 |
| 2026-08-18 | -6.20 | $165.17 | DVN×44, EOG×14, FANG×10, CELC×21, OUST×41 | $10,227.70 | -54.12 | +0.00 | — | DVN, EOG, FANG, CELC, OUST | $10,217.23 | $10,217.23 | — |
| 2026-08-19 | -7.20 | $10,217.23 | — | $10,217.23 | -0.00 | +0.00 | — | — | $10,217.23 | $10,217.23 | — |
| 2026-08-20 | +1.12 | $10,217.23 | — | $10,217.23 | -0.00 | -185.11 | BHP, MRNA, HUMA, BTGO, ASST, ZLAB, CRSP, APA | — | $131.10 | $9,998.84 | BHP×14, MRNA×8, HUMA×1806, BTGO×193, ASST×79, ZLAB×48, CRSP×21, APA×28 |
| 2026-08-21 | +3.25 | $131.10 | BHP×14, MRNA×8, HUMA×1806, BTGO×193, ASST×79, ZLAB×48, CRSP×21, APA×28 | $10,250.47 | +251.63 | +24.39 | AU, AUTL, FUTU, DE, MARA, BTDR, HIVE | BHP, MRNA, HUMA, BTGO, ASST, ZLAB, APA | $124.50 | $10,221.30 | CRSP×21, AU×10, AUTL×518, FUTU×11, DE×2, MARA×109, BTDR×115, HIVE×395 |
| 2026-08-24 | -5.17 | $124.50 | CRSP×21, AU×10, AUTL×518, FUTU×11, DE×2, MARA×109, BTDR×115, HIVE×395 | $10,140.45 | -80.85 | -39.48 | — | AU, AUTL, FUTU, DE, MARA, BTDR, HIVE | $8,883.10 | $10,078.21 | CRSP×21 |
| 2026-08-25 | +1.80 | $8,883.10 | CRSP×21 | $10,080.10 | +1.89 | -0.68 | RUM, EZPW, REAX, TRLV, VIRT, HOOD, ZYME, BKKT | CRSP | $122.02 | $10,059.75 | RUM×134, EZPW×36, REAX×52, TRLV×114, VIRT×19, HOOD×11, ZYME×42, BKKT×152 |
| 2026-08-26 | +2.02 | $122.02 | RUM×134, EZPW×36, REAX×52, TRLV×114, VIRT×19, HOOD×11, ZYME×42, BKKT×152 | $10,059.75 | +0.00 | +0.00 | — | — | $122.02 | $10,059.75 | RUM×134, EZPW×36, REAX×52, TRLV×114, VIRT×19, HOOD×11, ZYME×42, BKKT×152 |
| 2026-08-27 | — | $122.02 | RUM×134, EZPW×36, REAX×52, TRLV×114, VIRT×19, HOOD×11, ZYME×42, BKKT×152 | $10,295.37 | +235.62 | -24.03 | RRC, ACMR, MU, LRCX, NVDA | RUM, EZPW, REAX, TRLV, VIRT, HOOD, ZYME, BKKT | $2,656.87 | $10,243.36 | RRC×42, ACMR×21, MU×1, LRCX×5, NVDA×8 |
| 2026-08-28 | +0.75 | $2,656.87 | RRC×42, ACMR×21, MU×1, LRCX×5, NVDA×8 | $10,456.29 | +212.93 | +80.31 | CAPR, SEDG, SMTC, OPTX, ERAS, BBWI, ZYME | ACMR, MU, LRCX, NVDA | $97.72 | $10,513.03 | RRC×42, CAPR×135, SEDG×36, SMTC×8, OPTX×145, ERAS×64, BBWI×66, ZYME×42 |
| 2026-08-31 | -5.85 | $97.72 | RRC×42, CAPR×135, SEDG×36, SMTC×8, OPTX×145, ERAS×64, BBWI×66, ZYME×42 | $10,139.20 | -373.83 | +0.00 | — | RRC, CAPR, SEDG, SMTC, OPTX, ERAS, BBWI | $8,936.27 | $10,123.61 | ZYME×42 |
| 2026-09-01 | -6.30 | $8,936.27 | ZYME×42 | $10,167.71 | +44.10 | +0.00 | — | ZYME | $10,165.57 | $10,165.57 | — |
| 2026-09-02 | -3.83 | $10,165.57 | — | $10,165.57 | +0.00 | +0.00 | — | — | $10,165.57 | $10,165.57 | — |
| 2026-09-03 | -0.90 | $10,165.57 | — | $10,165.57 | +0.00 | +197.02 | MMED, CNXC, OPTX, TRLV, TXG, ZYME, FCX, AVGO | — | $250.20 | $10,345.29 | MMED×55, CNXC×39, OPTX×175, TRLV×107, TXG×21, ZYME×42, FCX×17, AVGO×3 |
| 2026-09-04 | +2.25 | $250.20 | MMED×55, CNXC×39, OPTX×175, TRLV×107, TXG×21, ZYME×42, FCX×17, AVGO×3 | $10,408.03 | +62.74 | -45.95 | CRM, BAK, AMTX, FRNM | MMED, CNXC, OPTX, TRLV, AVGO | $29.81 | $10,324.88 | TXG×21, ZYME×42, FCX×17, CRM×6, BAK×832, AMTX×849, FRNM×102 |
| 2026-09-07 | — | $29.81 | TXG×21, ZYME×42, FCX×17, CRM×6, BAK×832, AMTX×849, FRNM×102 | $10,235.12 | -89.76 | +169.49 | CHPT, SMMT, SNOW, MSTR, MRX | CRM, BAK, AMTX, FRNM | $355.67 | $10,367.59 | TXG×21, ZYME×42, FCX×17, CHPT×138, SMMT×75, SNOW×3, MSTR×9, MRX×16 |
| 2026-09-08 | -11.47 | $355.67 | TXG×21, ZYME×42, FCX×17, CHPT×138, SMMT×75, SNOW×3, MSTR×9, MRX×16 | $10,371.88 | +4.29 | +0.00 | — | TXG, ZYME, CHPT, SMMT, SNOW, MSTR, MRX | $9,100.07 | $10,356.88 | FCX×17 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $5,285.64 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $4,050.55 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $2,801.68 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $1,560.49 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,560.49 | ▲ close $10,110.67 vs 09:30 $10,000.00 (session +127.16) | 16:00 close · cash $1,560.49 · equity $10,110.67 vs 09:30 $10,000.00 (+110.67; session marks +127.16) · 7 name(s) marked open→close (per-name table). TLN×3 09:30 $359.83 → close $362.74 +8.73; VST×8 09:30 $146.90 → close $148.13 +9.84; NRG×10 09:30 $120.00 → close $126.24 +62.40; ANGX×290 09:30 $4.31 → close $4.37 +17.40; ARX×63 09:30 $19.57 → close $19.58 +0.63; MH×92 09:30 $13.55 → close $13.10 -41.40; HLIT×94 09:30 $13.18 → close $13.92 +69.56 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,560.49 | ▲ 09:30 equity $10,211.68 vs yday $10,110.67 (+101.01) | 09:30 open · cash $1,560.49 (unchanged overnight, no fees) · equity $10,211.68 vs prior close $10,110.67 (+101.01) · 7 name(s) re-marked at the open (per-name table). TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; MH×92 yday $13.10 → 09:30 $13.16 +5.52; HLIT×94 yday $13.92 → 09:30 $13.84 -7.52 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $2,662.11 | ▲ +20.13 after sell → book $10,209.66; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $3,855.04 | ▲ +15.71 after sell → book $10,207.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $5,127.00 | ▲ +69.94 after sell → book $10,205.59; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $6,457.20 | ▲ +76.56 after sell → book $10,201.79; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,687.91 | ▼ -4.38 after sell → book $10,199.59; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $8,896.34 | ▼ -40.44 after sell → book $10,197.30; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $10,195.00 | ▲ +57.47 after sell → book $10,195.00; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 44 | $46.18 | $2.12 | — | $8,160.96 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 14 | $142.77 | $2.03 | — | $6,160.14 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $4,131.12 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; leftover $2039.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 21 | $92.99 | $2.05 | — | $2,176.28 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; leftover $2039.00 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 41 | $49.00 | $2.11 | — | $165.17 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; leftover $2039.00 | join🟡 sector🟢 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $165.17 | ▲ close $10,281.82 vs 09:30 $10,211.68 (session +97.16) | 16:00 close · cash $165.17 · equity $10,281.82 vs 09:30 $10,211.68 (+70.14; session marks +97.16) · 5 name(s) marked open→close (per-name table). DVN×44 09:30 $46.18 → close $47.57 +61.16; EOG×14 09:30 $142.77 → close $146.15 +47.32; FANG×10 09:30 $202.70 → close $206.29 +35.90; CELC×21 09:30 $92.99 → close $92.44 -11.55; OUST×41 09:30 $49.00 → close $48.13 -35.67 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $165.17 | ▼ 09:30 equity $10,227.70 vs yday $10,281.82 (-54.12) | 09:30 open · cash $165.17 (unchanged overnight, no fees) · equity $10,227.70 vs prior close $10,281.82 (-54.12) · 5 name(s) re-marked at the open (per-name table). DVN×44 yday $47.57 → 09:30 $48.00 +18.92; EOG×14 yday $146.15 → 09:30 $148.04 +26.46; FANG×10 yday $206.29 → 09:30 $208.93 +26.40; CELC×21 yday $92.44 → 09:30 $92.38 -1.26; OUST×41 yday $48.13 → 09:30 $45.09 -124.64 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 44 | $48.00 | $2.15 | $+75.81 | $2,275.02 | ▲ +75.81 after sell → book $10,225.55; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 14 | $148.04 | $2.06 | $+69.69 | $4,345.52 | ▲ +69.69 after sell → book $10,223.49; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $6,432.77 | ▲ +58.23 after sell → book $10,221.44; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 21 | $92.38 | $2.08 | $-16.94 | $8,370.67 | ▼ -16.94 after sell → book $10,219.36; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 41 | $45.09 | $2.14 | $-164.56 | $10,217.23 | ▼ -164.56 after sell → book $10,217.23; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,217.23 | ▲ close $10,217.23 vs 09:30 $10,227.70 (session +0.00) | 16:00 close · cash $10,217.23 · no lots left · equity $10,217.23. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,217.23 | ▲ 09:30 equity $10,217.23 vs yday $10,217.23 (-0.00) | 09:30 open · cash $10,217.23 · no holdings · equity $10,217.23 vs prior close $10,217.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,217.23 | ▲ close $10,217.23 vs 09:30 $10,217.23 (session +0.00) | 16:00 close · cash $10,217.23 · no lots left · equity $10,217.23. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,217.23 | ▲ 09:30 equity $10,217.23 vs yday $10,217.23 (-0.00) | 09:30 open · cash $10,217.23 · no holdings · equity $10,217.23 vs prior close $10,217.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,941.05 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,737.92 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1277.15 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1806 | $0.71 | $18.19 | — | $6,442.89 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1277.15 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 193 | $6.61 | $2.57 | — | $5,165.56 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1277.15 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,899.33 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 48 | $26.57 | $2.13 | — | $2,621.84 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $1,386.45 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1277.15 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $131.10 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; leftover $1277.15 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.10 | ▼ close $9,998.84 vs 09:30 $10,217.23 (session -185.11) | 16:00 close · cash $131.10 · equity $9,998.84 vs 09:30 $10,217.23 (-218.39; session marks -185.11) · 8 name(s) marked open→close (per-name table). BHP×14 09:30 $91.01 → close $93.63 +36.68; MRNA×8 09:30 $150.14 → close $133.32 -134.56; HUMA×1806 09:30 $0.71 → close $0.68 -46.96; BTGO×193 09:30 $6.61 → close $6.60 -0.97; ASST×79 09:30 $16.00 → close $16.13 +10.27; ZLAB×48 09:30 $26.57 → close $26.02 -26.40; CRSP×21 09:30 $58.73 → close $58.12 -12.81; APA×28 09:30 $44.76 → close $44.39 -10.36 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.10 | ▲ 09:30 equity $10,250.47 vs yday $9,998.84 (+251.63) | 09:30 open · cash $131.10 (unchanged overnight, no fees) · equity $10,250.47 vs prior close $9,998.84 (+251.63) · 8 name(s) re-marked at the open (per-name table). BHP×14 yday $93.63 → 09:30 $95.72 +29.26; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; HUMA×1806 yday $0.68 → 09:30 $0.67 -12.64; BTGO×193 yday $6.60 → 09:30 $6.95 +67.55; ASST×79 yday $16.13 → 09:30 $17.66 +120.87; ZLAB×48 yday $26.02 → 09:30 $26.25 +11.04; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; APA×28 yday $44.39 → 09:30 $44.52 +3.64 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 14 | $95.72 | $2.05 | $+61.86 | $1,469.13 | ▲ +61.86 after sell → book $10,248.42; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $2,531.97 | ▼ -140.29 after sell → book $10,246.39; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1806 | $0.67 | $17.90 | $-95.68 | $3,731.32 | ▼ -95.68 after sell → book $10,228.49; vs 09:30 mark -17.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 193 | $6.95 | $2.61 | $+61.40 | $5,070.06 | ▲ +61.40 after sell → book $10,225.88; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $6,462.94 | ▲ +126.66 after sell → book $10,223.62; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 48 | $26.25 | $2.15 | $-19.65 | $7,720.79 | ▼ -19.65 after sell → book $10,221.47; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $8,965.26 | ▼ -10.89 after sell → book $10,219.38; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $7,768.94 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1280.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 518 | $2.47 | $6.68 | — | $6,482.79 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1280.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,213.79 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1280.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $3,965.28 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1280.75 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 109 | $11.70 | $2.32 | — | $2,687.66 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1280.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 115 | $11.10 | $2.33 | — | $1,409.40 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; leftover $1280.75 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 395 | $3.24 | $5.10 | — | $124.50 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1280.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.50 | ▲ close $10,221.30 vs 09:30 $10,250.47 (session +24.39) | 16:00 close · cash $124.50 · equity $10,221.30 vs 09:30 $10,250.47 (-29.17; session marks +24.39) · 8 name(s) marked open→close (per-name table). CRSP×21 09:30 $59.72 → close $59.50 -4.62; AU×10 09:30 $119.43 → close $121.22 +17.90; AUTL×518 09:30 $2.47 → close $2.41 -31.08; FUTU×11 09:30 $115.18 → close $123.64 +93.06; DE×2 09:30 $623.26 → close $647.47 +48.42; MARA×109 09:30 $11.70 → close $11.26 -47.96; BTDR×115 09:30 $11.10 → close $11.37 +31.62; HIVE×395 09:30 $3.24 → close $3.03 -82.95 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.50 | ▼ 09:30 equity $10,140.45 vs yday $10,221.30 (-80.85) | 09:30 open · cash $124.50 (unchanged overnight, no fees) · equity $10,140.45 vs prior close $10,221.30 (-80.85) · 8 name(s) re-marked at the open (per-name table). CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUTL×518 yday $2.41 → 09:30 $2.36 -25.90; FUTU×11 yday $123.64 → 09:30 $120.87 -30.47; DE×2 yday $647.47 → 09:30 $653.62 +12.30; MARA×109 yday $11.26 → 09:30 $11.18 -8.72; BTDR×115 yday $11.37 → 09:30 $11.49 +13.80; HIVE×395 yday $3.03 → 09:30 $2.98 -19.75 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,327.46 | ▲ +6.64 after sell → book $10,138.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 518 | $2.36 | $6.78 | $-70.44 | $2,543.16 | ▼ -70.44 after sell → book $10,131.63; vs 09:30 mark -6.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $120.87 | $2.04 | $+58.52 | $3,870.69 | ▲ +58.52 after sell → book $10,129.59; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $5,175.91 | ▲ +56.71 after sell → book $10,127.57; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 109 | $11.18 | $2.35 | $-61.34 | $6,392.19 | ▼ -61.34 after sell → book $10,125.23; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 115 | $11.49 | $2.36 | $+40.73 | $7,711.17 | ▲ +40.73 after sell → book $10,122.86; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 395 | $2.98 | $5.17 | $-112.97 | $8,883.10 | ▼ -112.97 after sell → book $10,117.69; vs 09:30 mark -5.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,883.10 | ▼ close $10,078.21 vs 09:30 $10,140.45 (session -39.48) | 16:00 close · cash $8,883.10 · equity $10,078.21 vs 09:30 $10,140.45 (-62.24; session marks -39.48) · 1 name(s) marked open→close (per-name table). CRSP×21 09:30 $58.79 → close $56.91 -39.48 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,883.10 | ▲ 09:30 equity $10,080.10 vs yday $10,078.21 (+1.89) | 09:30 open · cash $8,883.10 (unchanged overnight, no fees) · equity $10,080.10 vs prior close $10,078.21 (+1.89) · 1 name(s) re-marked at the open (per-name table). CRSP×21 yday $56.91 → 09:30 $57.00 +1.89 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.00 | $2.07 | $-40.46 | $10,078.03 | ▼ -40.46 after sell → book $10,078.03; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 134 | $9.36 | $2.39 | — | $8,821.40 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1259.75 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 36 | $34.48 | $2.10 | — | $7,578.02 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1259.75 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 52 | $24.00 | $2.15 | — | $6,327.87 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+10.0; leftover $1259.75 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 114 | $11.02 | $2.33 | — | $5,069.26 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.0; leftover $1259.75 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VIRT` | 19 | $66.29 | $2.05 | — | $3,807.71 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+13.2; leftover $1259.75 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HOOD` | 11 | $106.00 | $2.02 | — | $2,639.68 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+13.2; leftover $1259.75 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 42 | $29.87 | $2.12 | — | $1,383.03 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+14.1; leftover $1259.75 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BKKT` | 152 | $8.28 | $2.45 | — | $122.02 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+12.3; leftover $1259.75 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.02 | ▼ close $10,059.75 vs 09:30 $10,080.10 (session -0.68) | 16:00 close · cash $122.02 · equity $10,059.75 vs 09:30 $10,080.10 (-20.35; session marks -0.68) · 8 name(s) marked open→close (per-name table). RUM×134 09:30 $9.36 → close $9.35 -1.34; EZPW×36 09:30 $34.48 → close $34.69 +7.56; REAX×52 09:30 $24.00 → close $24.00 +0.00; TRLV×114 09:30 $11.02 → close $11.02 +0.00; VIRT×19 09:30 $66.29 → close $66.29 +0.00; HOOD×11 09:30 $106.00 → close $104.22 -19.58; ZYME×42 09:30 $29.87 → close $29.81 -2.52; BKKT×152 09:30 $8.28 → close $8.38 +15.20 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.02 | ▲ 09:30 equity $10,059.75 vs yday $10,059.75 (+0.00) | 09:30 open · cash $122.02 (unchanged overnight, no fees) · equity $10,059.75 vs prior close $10,059.75 (+0.00) · 8 name(s) re-marked at the open (per-name table). RUM×134 yday $9.35 → 09:30 $9.35 +0.00; EZPW×36 yday $34.69 → 09:30 $34.69 +0.00; REAX×52 yday $24.00 → 09:30 $24.00 +0.00; TRLV×114 yday $11.02 → 09:30 $11.02 +0.00; VIRT×19 yday $66.29 → 09:30 $66.29 +0.00; HOOD×11 yday $104.22 → 09:30 $104.22 +0.00; ZYME×42 yday $29.81 → 09:30 $29.81 +0.00; BKKT×152 yday $8.38 → 09:30 $8.38 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.02 | ▲ close $10,059.75 vs 09:30 $10,059.75 (session +0.00) | 16:00 close · cash $122.02 · equity $10,059.75 vs 09:30 $10,059.75 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). RUM×134 09:30 $9.35 → close $9.35 +0.00; EZPW×36 09:30 $34.69 → close $34.69 +0.00; REAX×52 09:30 $24.00 → close $24.00 +0.00; TRLV×114 09:30 $11.02 → close $11.02 +0.00; VIRT×19 09:30 $66.29 → close $66.29 +0.00; HOOD×11 09:30 $104.22 → close $104.22 +0.00; ZYME×42 09:30 $29.81 → close $29.81 +0.00; BKKT×152 09:30 $8.38 → close $8.38 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.02 | ▲ 09:30 equity $10,295.37 vs yday $10,059.75 (+235.62) | 09:30 open · cash $122.02 (unchanged overnight, no fees) · equity $10,295.37 vs prior close $10,059.75 (+235.62) · 8 name(s) re-marked at the open (per-name table). RUM×134 yday $9.35 → 09:30 $10.07 +96.48; EZPW×36 yday $34.69 → 09:30 $35.70 +36.36; REAX×52 yday $24.00 → 09:30 $26.61 +135.72; TRLV×114 yday $11.02 → 09:30 $11.22 +22.80; VIRT×19 yday $66.29 → 09:30 $64.92 -26.03; HOOD×11 yday $104.22 → 09:30 $110.11 +64.79; ZYME×42 yday $29.81 → 09:30 $27.56 -94.50; BKKT×152 yday $8.38 → 09:30 $8.38 +0.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `RUM` | 134 | $10.07 | $2.42 | $+90.32 | $1,468.98 | ▲ +90.32 after sell → book $10,292.95; vs 09:30 mark -2.42 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `EZPW` | 36 | $35.70 | $2.12 | $+39.70 | $2,752.06 | ▲ +39.70 after sell → book $10,290.83; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `REAX` | 52 | $26.61 | $2.17 | $+131.41 | $4,133.61 | ▲ +131.41 after sell → book $10,288.66; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 114 | $11.22 | $2.36 | $+18.11 | $5,410.33 | ▲ +18.11 after sell → book $10,286.30; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `VIRT` | 19 | $64.92 | $2.07 | $-30.14 | $6,641.74 | ▼ -30.14 after sell → book $10,284.23; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HOOD` | 11 | $110.11 | $2.04 | $+41.14 | $7,850.91 | ▲ +41.14 after sell → book $10,282.19; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZYME` | 42 | $27.56 | $2.14 | $-101.27 | $9,006.29 | ▼ -101.27 after sell → book $10,280.05; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BKKT` | 152 | $8.38 | $2.48 | $+10.27 | $10,277.57 | ▲ +10.27 after sell → book $10,277.57; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 42 | $40.72 | $2.12 | — | $8,565.22 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+1.8; leftover $1712.93 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 21 | $80.97 | $2.05 | — | $6,862.79 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-1.3; leftover $1712.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $5,935.06 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-0.5; leftover $1712.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 5 | $314.61 | $2.00 | — | $4,360.00 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-5.5; leftover $1712.93 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 8 | $212.64 | $2.01 | — | $2,656.87 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=-4.6; leftover $1712.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,656.87 | ▼ close $10,243.36 vs 09:30 $10,295.37 (session -24.03) | 16:00 close · cash $2,656.87 · equity $10,243.36 vs 09:30 $10,295.37 (-52.01; session marks -24.03) · 5 name(s) marked open→close (per-name table). RRC×42 09:30 $40.72 → close $41.55 +34.86; ACMR×21 09:30 $80.97 → close $79.11 -39.06; MU×1 09:30 $925.74 → close $938.40 +12.66; LRCX×5 09:30 $314.61 → close $312.88 -8.65; NVDA×8 09:30 $212.64 → close $209.66 -23.84 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,656.87 | ▲ 09:30 equity $10,456.29 vs yday $10,243.36 (+212.93) | 09:30 open · cash $2,656.87 (unchanged overnight, no fees) · equity $10,456.29 vs prior close $10,243.36 (+212.93) · 5 name(s) re-marked at the open (per-name table). RRC×42 yday $41.55 → 09:30 $41.44 -4.62; ACMR×21 yday $79.11 → 09:30 $81.65 +53.34; MU×1 yday $938.40 → 09:30 $967.01 +28.61; LRCX×5 yday $312.88 → 09:30 $318.88 +30.00; NVDA×8 yday $209.66 → 09:30 $222.86 +105.60 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 21 | $81.65 | $2.08 | $+10.15 | $4,369.44 | ▲ +10.15 after sell → book $10,454.21; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $5,334.44 | ▲ +37.26 after sell → book $10,452.20; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 5 | $318.88 | $2.03 | $+17.32 | $6,926.81 | ▲ +17.32 after sell → book $10,450.17; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 8 | $222.86 | $2.04 | $+77.71 | $8,707.65 | ▲ +77.71 after sell → book $10,448.13; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 135 | $9.19 | $2.40 | — | $7,464.61 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1243.95 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 36 | $33.78 | $2.10 | — | $6,246.43 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1243.95 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $5,049.22 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1243.95 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 145 | $8.57 | $2.42 | — | $3,804.14 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-3.4; leftover $1243.95 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 64 | $19.30 | $2.18 | — | $2,566.76 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-4.1; leftover $1243.95 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 66 | $18.68 | $2.19 | — | $1,331.69 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+0.2; leftover $1243.95 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 42 | $29.33 | $2.12 | — | $97.72 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1243.95 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.72 | ▲ close $10,513.03 vs 09:30 $10,456.29 (session +80.31) | 16:00 close · cash $97.72 · equity $10,513.03 vs 09:30 $10,456.29 (+56.74; session marks +80.31) · 8 name(s) marked open→close (per-name table). RRC×42 09:30 $41.44 → close $41.64 +8.40; CAPR×135 09:30 $9.19 → close $10.06 +117.45; SEDG×36 09:30 $33.78 → close $33.51 -9.72; SMTC×8 09:30 $149.40 → close $142.43 -55.76; OPTX×145 09:30 $8.57 → close $8.73 +23.20; ERAS×64 09:30 $19.30 → close $19.49 +12.16; BBWI×66 09:30 $18.68 → close $18.65 -1.98; ZYME×42 09:30 $29.33 → close $29.01 -13.44 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.72 | ▼ 09:30 equity $10,139.20 vs yday $10,513.03 (-373.83) | 09:30 open · cash $97.72 (unchanged overnight, no fees) · equity $10,139.20 vs prior close $10,513.03 (-373.83) · 8 name(s) re-marked at the open (per-name table). RRC×42 yday $41.64 → 09:30 $41.11 -22.26; CAPR×135 yday $10.06 → 09:30 $9.44 -83.70; SEDG×36 yday $33.51 → 09:30 $31.50 -72.36; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; OPTX×145 yday $8.73 → 09:30 $8.52 -30.45; ERAS×64 yday $19.49 → 09:30 $17.90 -101.76; BBWI×66 yday $18.65 → 09:30 $19.30 +42.90; ZYME×42 yday $29.01 → 09:30 $28.27 -31.08 | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 42 | $41.11 | $2.14 | $+12.12 | $1,822.20 | ▲ +12.12 after sell → book $10,137.06; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 135 | $9.44 | $2.43 | $+28.93 | $3,094.17 | ▲ +28.93 after sell → book $10,134.63; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 36 | $31.50 | $2.12 | $-86.30 | $4,226.05 | ▼ -86.30 after sell → book $10,132.51; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $133.04 | $2.03 | $-134.93 | $5,288.34 | ▼ -134.93 after sell → book $10,130.48; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 145 | $8.52 | $2.46 | $-12.13 | $6,521.28 | ▼ -12.13 after sell → book $10,128.02; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 64 | $17.90 | $2.20 | $-93.98 | $7,664.68 | ▼ -93.98 after sell → book $10,125.82; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 66 | $19.30 | $2.21 | $+36.52 | $8,936.27 | ▲ +36.52 after sell → book $10,123.61; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,936.27 | ▲ close $10,123.61 vs 09:30 $10,139.20 (session +0.00) | 16:00 close · cash $8,936.27 · equity $10,123.61 vs 09:30 $10,139.20 (-15.59; session marks +0.00) · 1 name(s) marked open→close (per-name table). ZYME×42 09:30 $28.27 → close $28.27 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,936.27 | ▲ 09:30 equity $10,167.71 vs yday $10,123.61 (+44.10) | 09:30 open · cash $8,936.27 (unchanged overnight, no fees) · equity $10,167.71 vs prior close $10,123.61 (+44.10) · 1 name(s) re-marked at the open (per-name table). ZYME×42 yday $28.27 → 09:30 $29.32 +44.10 | — |
| 2026-09-01 09:30 ET | **SELL** | `ZYME` | 42 | $29.32 | $2.14 | $-4.67 | $10,165.57 | ▼ -4.67 after sell → book $10,165.57; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,165.57 | ▲ close $10,165.57 vs 09:30 $10,167.71 (session +0.00) | 16:00 close · cash $10,165.57 · no lots left · equity $10,165.57. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,165.57 | ▲ 09:30 equity $10,165.57 vs yday $10,165.57 (+0.00) | 09:30 open · cash $10,165.57 · no holdings · equity $10,165.57 vs prior close $10,165.57 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,165.57 | ▲ close $10,165.57 vs 09:30 $10,165.57 (session +0.00) | 16:00 close · cash $10,165.57 · no lots left · equity $10,165.57. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,165.57 | ▲ 09:30 equity $10,165.57 vs yday $10,165.57 (+0.00) | 09:30 open · cash $10,165.57 · no holdings · equity $10,165.57 vs prior close $10,165.57 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 55 | $22.78 | $2.15 | — | $8,910.52 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1270.70 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 39 | $31.80 | $2.11 | — | $7,668.21 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+3.7; leftover $1270.70 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 175 | $7.25 | $2.52 | — | $6,396.94 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-3.4; leftover $1270.70 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TRLV` | 107 | $11.78 | $2.31 | — | $5,134.17 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.0; leftover $1270.70 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TXG` | 21 | $60.24 | $2.05 | — | $3,867.08 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.1; leftover $1270.70 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ZYME` | 42 | $30.00 | $2.12 | — | $2,604.96 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ⚪; ret5=+14.1; leftover $1270.70 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FCX` | 17 | $73.04 | $2.04 | — | $1,361.24 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $1270.70 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $369.68 | $2.00 | — | $250.20 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; leftover $1270.70 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $250.20 | ▲ close $10,345.29 vs 09:30 $10,165.57 (session +197.02) | 16:00 close · cash $250.20 · equity $10,345.29 vs 09:30 $10,165.57 (+179.72; session marks +197.02) · 8 name(s) marked open→close (per-name table). MMED×55 09:30 $22.78 → close $23.76 +53.90; CNXC×39 09:30 $31.80 → close $32.37 +22.23; OPTX×175 09:30 $7.25 → close $7.53 +49.00; TRLV×107 09:30 $11.78 → close $11.69 -9.63; TXG×21 09:30 $60.24 → close $61.65 +29.61; ZYME×42 09:30 $30.00 → close $31.05 +44.10; FCX×17 09:30 $73.04 → close $73.93 +15.13; AVGO×3 09:30 $369.68 → close $367.24 -7.32 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $250.20 | ▲ 09:30 equity $10,408.03 vs yday $10,345.29 (+62.74) | 09:30 open · cash $250.20 (unchanged overnight, no fees) · equity $10,408.03 vs prior close $10,345.29 (+62.74) · 8 name(s) re-marked at the open (per-name table). MMED×55 yday $23.76 → 09:30 $23.88 +6.60; CNXC×39 yday $32.37 → 09:30 $32.88 +19.89; OPTX×175 yday $7.53 → 09:30 $7.59 +10.50; TRLV×107 yday $11.69 → 09:30 $11.89 +21.40; TXG×21 yday $61.65 → 09:30 $62.35 +14.70; ZYME×42 yday $31.05 → 09:30 $31.34 +12.18; FCX×17 yday $73.93 → 09:30 $75.34 +23.97; AVGO×3 yday $367.24 → 09:30 $351.74 -46.50 | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 55 | $23.88 | $2.18 | $+56.17 | $1,561.43 | ▲ +56.17 after sell → book $10,405.86; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 39 | $32.88 | $2.13 | $+37.89 | $2,841.62 | ▲ +37.89 after sell → book $10,403.73; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 175 | $7.59 | $2.55 | $+54.43 | $4,167.32 | ▲ +54.43 after sell → book $10,401.18; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `TRLV` | 107 | $11.89 | $2.34 | $+7.12 | $5,437.21 | ▲ +7.12 after sell → book $10,398.84; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $351.74 | $2.02 | $-57.84 | $6,490.41 | ▼ -57.84 after sell → book $10,396.82; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 6 | $261.98 | $2.01 | — | $4,916.52 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1622.60 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 832 | $1.95 | $10.73 | — | $3,283.39 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1622.60 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMTX` | 849 | $1.91 | $10.95 | — | $1,650.85 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; leftover $1622.60 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 102 | $15.87 | $2.30 | — | $29.81 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+19.5; leftover $1622.60 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.81 | ▼ close $10,324.88 vs 09:30 $10,408.03 (session -45.95) | 16:00 close · cash $29.81 · equity $10,324.88 vs 09:30 $10,408.03 (-83.15; session marks -45.95) · 7 name(s) marked open→close (per-name table). TXG×21 09:30 $62.35 → close $63.22 +18.27; ZYME×42 09:30 $31.34 → close $29.90 -60.48; FCX×17 09:30 $75.34 → close $72.56 -47.26; CRM×6 09:30 $261.98 → close $264.43 +14.70; BAK×832 09:30 $1.95 → close $1.94 -8.32; AMTX×849 09:30 $1.91 → close $1.83 -67.92; FRNM×102 09:30 $15.87 → close $16.90 +105.06 | — |
| 2026-09-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.81 | ▼ 09:30 equity $10,235.12 vs yday $10,324.88 (-89.76) | 09:30 open · cash $29.81 (unchanged overnight, no fees) · equity $10,235.12 vs prior close $10,324.88 (-89.76) · 7 name(s) re-marked at the open (per-name table). TXG×21 yday $63.22 → 09:30 $62.54 -14.28; ZYME×42 yday $29.90 → 09:30 $29.81 -3.78; FCX×17 yday $72.56 → 09:30 $71.72 -14.28; CRM×6 yday $264.43 → 09:30 $263.36 -6.42; BAK×832 yday $1.94 → 09:30 $1.94 +0.00; AMTX×849 yday $1.83 → 09:30 $1.83 +0.00; FRNM×102 yday $16.90 → 09:30 $16.40 -51.00 | — |
| 2026-09-07 09:30 ET | **SELL** | `CRM` | 6 | $263.36 | $2.03 | $+4.24 | $1,607.94 | ▲ +4.24 after sell → book $10,233.09; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `BAK` | 832 | $1.94 | $10.88 | $-29.94 | $3,211.13 | ▼ -29.94 after sell → book $10,222.20; vs 09:30 mark -10.89 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `AMTX` | 849 | $1.83 | $11.11 | $-89.98 | $4,753.70 | ▼ -89.98 after sell → book $10,211.10; vs 09:30 mark -11.10 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-07 09:30 ET | **SELL** | `FRNM` | 102 | $16.40 | $2.33 | $+49.44 | $6,424.17 | ▲ +49.44 after sell → book $10,208.77; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `CHPT` | 138 | $9.28 | $2.40 | — | $5,141.13 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.1; leftover $1284.83 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `SMMT` | 75 | $16.93 | $2.21 | — | $3,869.16 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=-1.4; leftover $1284.83 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `SNOW` | 3 | $353.63 | $2.00 | — | $2,806.27 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; ret5=+1.2; leftover $1284.83 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `MSTR` | 9 | $137.35 | $2.02 | — | $1,568.11 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+28.2; leftover $1284.83 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `MRX` | 16 | $75.65 | $2.04 | — | $355.67 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+2.7; leftover $1284.83 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $355.67 | ▲ close $10,367.59 vs 09:30 $10,235.12 (session +169.49) | 16:00 close · cash $355.67 · equity $10,367.59 vs 09:30 $10,235.12 (+132.47; session marks +169.49) · 8 name(s) marked open→close (per-name table). TXG×21 09:30 $62.54 → close $62.65 +2.31; ZYME×42 09:30 $29.81 → close $29.19 -26.04; FCX×17 09:30 $71.72 → close $72.73 +17.17; CHPT×138 09:30 $9.28 → close $9.89 +84.18; SMMT×75 09:30 $16.93 → close $17.60 +50.25; SNOW×3 09:30 $353.63 → close $337.18 -49.35; MSTR×9 09:30 $137.35 → close $142.80 +49.05; MRX×16 09:30 $75.65 → close $78.27 +41.92 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $355.67 | ▲ 09:30 equity $10,371.88 vs yday $10,367.59 (+4.29) | 09:30 open · cash $355.67 (unchanged overnight, no fees) · equity $10,371.88 vs prior close $10,367.59 (+4.29) · 8 name(s) re-marked at the open (per-name table). TXG×21 yday $62.65 → 09:30 $63.18 +11.13; ZYME×42 yday $29.19 → 09:30 $28.70 -20.58; FCX×17 yday $72.73 → 09:30 $73.93 +20.40; CHPT×138 yday $9.89 → 09:30 $9.98 +12.42; SMMT×75 yday $17.60 → 09:30 $17.38 -16.50; SNOW×3 yday $337.18 → 09:30 $336.00 -3.54; MSTR×9 yday $142.80 → 09:30 $139.92 -25.92; MRX×16 yday $78.27 → 09:30 $79.95 +26.88 | — |
| 2026-09-08 09:30 ET | **SELL** | `TXG` | 21 | $63.18 | $2.07 | $+57.61 | $1,680.38 | ▲ +57.61 after sell → book $10,369.81; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `ZYME` | 42 | $28.70 | $2.14 | $-58.85 | $2,883.64 | ▼ -58.85 after sell → book $10,367.67; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `CHPT` | 138 | $9.98 | $2.44 | $+91.76 | $4,258.44 | ▲ +91.76 after sell → book $10,365.23; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `SMMT` | 75 | $17.38 | $2.24 | $+29.30 | $5,559.70 | ▲ +29.30 after sell → book $10,362.99; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `SNOW` | 3 | $336.00 | $2.02 | $-56.91 | $6,565.69 | ▼ -56.91 after sell → book $10,360.98; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MSTR` | 9 | $139.92 | $2.04 | $+19.08 | $7,822.93 | ▲ +19.08 after sell → book $10,358.94; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 16 | $79.95 | $2.06 | $+64.70 | $9,100.07 | ▲ +64.70 after sell → book $10,356.88; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,100.07 | ▲ close $10,356.88 vs 09:30 $10,371.88 (session +0.00) | 16:00 close · cash $9,100.07 · equity $10,356.88 vs 09:30 $10,371.88 (-15.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). FCX×17 09:30 $73.93 → close $73.93 +0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `RUM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `EZPW` | no_price | no 09:30 open — carry |
| 2026-08-26 | `REAX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `VIRT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `CAPR` | no_price | no 09:30 open |
| 2026-08-26 | `FWRD` | no_price | no 09:30 open |
| 2026-08-26 | `FCX` | no_price | no 09:30 open |
| 2026-08-27 | `ASML` | cash | leftover split 1712.93 < 1 share @ 1746.33 |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HOOD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BKKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEM` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TRLV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TXG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZYME` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `AIRS` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ALAB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `FWRD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TTMI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FCX` | 17 | 2026-09-03 @ $73.04 | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $1270.70 |
