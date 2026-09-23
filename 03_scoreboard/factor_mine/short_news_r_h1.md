# Factor mine action — `short_news_r_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · news🔴

Cash book **+2.54%** ($10,253) · signal-only (no cash/fees) was +15.09%. Starts YES **28/29**. Fills 109 · skips 23 · realized $+77.15.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the news camera (does the morning packet like the headline?) is red.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a SHORT sleeve: it borrows the name and profits if the price falls. Equity treats the short as a liability (must keep enough to cover).

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=bad` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $19,330.29.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `EU` | 1412 | — | $1.18 | +0.00 | $1.21 | -42.36 | -42.36 | -0.00 | -42.36 |
| 2026-08-14 | `LUNR` | 86 | — | $19.17 | +0.00 | $19.01 | +13.76 | +13.76 | -0.00 | +13.76 |
| 2026-08-14 | `OWL` | 131 | — | $12.70 | +0.00 | $12.22 | +62.22 | +62.22 | -0.00 | +62.22 |
| 2026-08-17 | `EU` | 1412 | $1.21 | $1.21 | +0.00 | — | +0.00 | +0.00 | -42.36 | — |
| 2026-08-17 | `LUNR` | 86 | $19.01 | $20.25 | -106.64 | — | +0.00 | -106.64 | -92.88 | — |
| 2026-08-17 | `OWL` | 131 | $12.22 | $12.12 | +13.10 | — | +0.00 | +13.10 | +75.33 | — |
| 2026-08-17 | `VERI` | 860 | — | $1.15 | +0.00 | $1.08 | +55.90 | +55.90 | -0.00 | +55.90 |
| 2026-08-17 | `ZNTL` | 277 | — | $3.56 | +0.00 | $3.71 | -40.17 | -40.17 | -0.00 | -40.17 |
| 2026-08-17 | `APMD` | 31 | — | $31.70 | +0.00 | $32.55 | -26.35 | -26.35 | -0.00 | -26.35 |
| 2026-08-17 | `HIVE` | 328 | — | $3.01 | +0.00 | $3.07 | -19.68 | -19.68 | -0.00 | -19.68 |
| 2026-08-17 | `RNW` | 145 | — | $6.80 | +0.00 | $6.82 | -2.90 | -2.90 | -0.00 | -2.90 |
| 2026-08-18 | `VERI` | 860 | $1.08 | $1.05 | +30.10 | — | +0.00 | +30.10 | +86.00 | — |
| 2026-08-18 | `ZNTL` | 277 | $3.71 | $3.75 | -12.46 | — | +0.00 | -12.46 | -52.63 | — |
| 2026-08-18 | `APMD` | 31 | $32.55 | $32.85 | -9.30 | — | +0.00 | -9.30 | -35.65 | — |
| 2026-08-18 | `HIVE` | 328 | $3.07 | $2.96 | +36.08 | — | +0.00 | +36.08 | +16.40 | — |
| 2026-08-18 | `RNW` | 145 | $6.82 | $6.83 | -1.45 | $6.82 | +1.45 | +0.00 | -4.35 | -2.90 |
| 2026-08-19 | `RNW` | 145 | $6.82 | $6.84 | -2.90 | — | +0.00 | -2.90 | -5.80 | — |
| 2026-08-20 | `AEM` | 3 | — | $204.45 | +0.00 | $212.04 | -22.77 | -22.77 | -0.00 | -22.77 |
| 2026-08-20 | `WYFI` | 28 | — | $21.40 | +0.00 | $21.16 | +6.72 | +6.72 | -0.00 | +6.72 |
| 2026-08-20 | `TOYO` | 139 | — | $4.43 | +0.00 | $4.51 | -11.81 | -11.81 | -0.00 | -11.81 |
| 2026-08-20 | `ABCL` | 52 | — | $11.81 | +0.00 | $11.57 | +12.74 | +12.74 | -0.00 | +12.74 |
| 2026-08-20 | `TEAM` | 3 | — | $173.90 | +0.00 | $174.91 | -3.03 | -3.03 | -0.00 | -3.03 |
| 2026-08-20 | `AAP` | 13 | — | $46.85 | +0.00 | $42.39 | +57.98 | +57.98 | -0.00 | +57.98 |
| 2026-08-20 | `WMT` | 5 | — | $106.38 | +0.00 | $103.84 | +12.70 | +12.70 | -0.00 | +12.70 |
| 2026-08-20 | `AQST` | 133 | — | $4.61 | +0.00 | $4.50 | +15.30 | +15.30 | -0.00 | +15.30 |
| 2026-08-21 | `AEM` | 3 | $212.04 | $216.30 | -12.78 | — | +0.00 | -12.78 | -35.55 | — |
| 2026-08-21 | `WYFI` | 28 | $21.16 | $21.54 | -10.64 | — | +0.00 | -10.64 | -3.92 | — |
| 2026-08-21 | `TOYO` | 139 | $4.51 | $4.68 | -22.94 | — | +0.00 | -22.94 | -34.75 | — |
| 2026-08-21 | `ABCL` | 52 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | +12.74 | — |
| 2026-08-21 | `TEAM` | 3 | $174.91 | $174.22 | +2.07 | — | +0.00 | +2.07 | -0.96 | — |
| 2026-08-21 | `AAP` | 13 | $42.39 | $42.41 | -0.26 | — | +0.00 | -0.26 | +57.72 | — |
| 2026-08-21 | `WMT` | 5 | $103.84 | $103.69 | +0.75 | — | +0.00 | +0.75 | +13.45 | — |
| 2026-08-21 | `AQST` | 133 | $4.50 | $4.54 | -5.98 | — | +0.00 | -5.98 | +9.31 | — |
| 2026-08-21 | `QTRX` | 263 | — | $3.11 | +0.00 | $2.99 | +31.56 | +31.56 | -0.00 | +31.56 |
| 2026-08-21 | `MRNA` | 6 | — | $133.11 | +0.00 | $145.13 | -72.12 | -72.12 | -0.00 | -72.12 |
| 2026-08-21 | `AUGO` | 9 | — | $89.10 | +0.00 | $87.26 | +16.56 | +16.56 | -0.00 | +16.56 |
| 2026-08-21 | `SSRM` | 21 | — | $38.40 | +0.00 | $37.77 | +13.23 | +13.23 | -0.00 | +13.23 |
| 2026-08-21 | `ARIS` | 39 | — | $20.90 | +0.00 | $20.86 | +1.56 | +1.56 | -0.00 | +1.56 |
| 2026-08-21 | `NOG` | 30 | — | $27.00 | +0.00 | $27.34 | -10.20 | -10.20 | -0.00 | -10.20 |
| 2026-08-24 | `QTRX` | 263 | $2.99 | $2.99 | +0.00 | — | +0.00 | +0.00 | +31.56 | — |
| 2026-08-24 | `MRNA` | 6 | $145.13 | $142.70 | +14.58 | — | +0.00 | +14.58 | -57.54 | — |
| 2026-08-24 | `AUGO` | 9 | $87.26 | $88.60 | -12.06 | — | +0.00 | -12.06 | +4.50 | — |
| 2026-08-24 | `SSRM` | 21 | $37.77 | $38.32 | -11.55 | — | +0.00 | -11.55 | +1.68 | — |
| 2026-08-24 | `ARIS` | 39 | $20.86 | $20.98 | -4.68 | — | +0.00 | -4.68 | -3.12 | — |
| 2026-08-24 | `NOG` | 30 | $27.34 | $27.12 | +6.60 | $26.84 | +8.40 | +15.00 | -3.60 | +4.80 |
| 2026-08-25 | `NOG` | 30 | $26.84 | $26.06 | +23.40 | — | +0.00 | +23.40 | +28.20 | — |
| 2026-08-25 | `AVAH` | 72 | — | $13.62 | +0.00 | $13.59 | +2.52 | +2.52 | -0.00 | +2.52 |
| 2026-08-25 | `SSRM` | 26 | — | $37.75 | +0.00 | $39.21 | -37.96 | -37.96 | -0.00 | -37.96 |
| 2026-08-25 | `ARE` | 18 | — | $54.51 | +0.00 | $52.90 | +28.98 | +28.98 | -0.00 | +28.98 |
| 2026-08-25 | `BMO` | 5 | — | $175.01 | +0.00 | $173.46 | +7.75 | +7.75 | -0.00 | +7.75 |
| 2026-08-25 | `INTU` | 2 | — | $364.35 | +0.00 | $357.46 | +13.78 | +13.78 | -0.00 | +13.78 |
| 2026-08-26 | `AVAH` | 72 | $13.59 | $13.65 | -4.32 | — | +0.00 | -4.32 | -1.80 | — |
| 2026-08-26 | `SSRM` | 26 | $39.21 | $38.41 | +20.80 | — | +0.00 | +20.80 | -17.16 | — |
| 2026-08-26 | `ARE` | 18 | $52.90 | $52.77 | +2.34 | — | +0.00 | +2.34 | +31.32 | — |
| 2026-08-26 | `BMO` | 5 | $173.46 | $173.22 | +1.20 | — | +0.00 | +1.20 | +8.95 | — |
| 2026-08-26 | `INTU` | 2 | $357.46 | $323.47 | +67.98 | — | +0.00 | +67.98 | +81.76 | — |
| 2026-08-26 | `BE` | 4 | — | $213.94 | +0.00 | $218.21 | -17.08 | -17.08 | -0.00 | -17.08 |
| 2026-08-26 | `ABCL` | 81 | — | $12.22 | +0.00 | $12.24 | -1.62 | -1.62 | -0.00 | -1.62 |
| 2026-08-26 | `AQST` | 194 | — | $5.08 | +0.00 | $5.39 | -60.14 | -60.14 | -0.00 | -60.14 |
| 2026-08-26 | `NEM` | 7 | — | $132.64 | +0.00 | $131.60 | +7.28 | +7.28 | -0.00 | +7.28 |
| 2026-08-26 | `CRM` | 4 | — | $199.94 | +0.00 | $205.62 | -22.72 | -22.72 | -0.00 | -22.72 |
| 2026-08-27 | `BE` | 4 | $218.21 | $227.10 | -35.56 | — | +0.00 | -35.56 | -52.64 | — |
| 2026-08-27 | `ABCL` | 81 | $12.24 | $12.25 | -0.81 | — | +0.00 | -0.81 | -2.43 | — |
| 2026-08-27 | `AQST` | 194 | $5.39 | $5.39 | +0.00 | $5.16 | +44.62 | +44.62 | -60.14 | -15.52 |
| 2026-08-27 | `NEM` | 7 | $131.60 | $131.02 | +4.06 | — | +0.00 | +4.06 | +11.34 | — |
| 2026-08-27 | `CRM` | 4 | $205.62 | $230.05 | -97.72 | — | +0.00 | -97.72 | -120.44 | — |
| 2026-08-27 | `INTU` | 4 | — | $353.54 | +0.00 | $348.00 | +22.16 | +22.16 | -0.00 | +22.16 |
| 2026-08-27 | `MT` | 21 | — | $74.54 | +0.00 | $74.63 | -1.89 | -1.89 | -0.00 | -1.89 |
| 2026-08-27 | `TX` | 29 | — | $55.25 | +0.00 | $55.83 | -16.82 | -16.82 | -0.00 | -16.82 |
| 2026-08-28 | `AQST` | 194 | $5.16 | $5.11 | +9.70 | — | +0.00 | +9.70 | -5.82 | — |
| 2026-08-28 | `INTU` | 4 | $348.00 | $347.82 | +0.72 | — | +0.00 | +0.72 | +22.88 | — |
| 2026-08-28 | `MT` | 21 | $74.63 | $75.39 | -15.96 | — | +0.00 | -15.96 | -17.85 | — |
| 2026-08-28 | `TX` | 29 | $55.83 | $55.97 | -4.06 | — | +0.00 | -4.06 | -20.88 | — |
| 2026-08-28 | `SIMO` | 9 | — | $252.24 | +0.00 | $245.81 | +57.87 | +57.87 | -0.00 | +57.87 |
| 2026-08-28 | `FIG` | 80 | — | $30.18 | +0.00 | $28.82 | +108.80 | +108.80 | -0.00 | +108.80 |
| 2026-08-31 | `SIMO` | 9 | $245.81 | $247.05 | -11.16 | — | +0.00 | -11.16 | +46.71 | — |
| 2026-08-31 | `FIG` | 80 | $28.82 | $27.60 | +97.60 | — | +0.00 | +97.60 | +206.40 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `SLN` | 167 | — | $14.85 | +0.00 | $14.79 | +10.02 | +10.02 | -0.00 | +10.02 |
| 2026-09-03 | `OPK` | 1450 | — | $1.71 | +0.00 | $1.61 | +145.00 | +145.00 | -0.00 | +145.00 |
| 2026-09-04 | `SLN` | 167 | $14.79 | $14.63 | +26.72 | — | +0.00 | +26.72 | +36.74 | — |
| 2026-09-04 | `OPK` | 1450 | $1.61 | $1.59 | +29.00 | $1.64 | -72.50 | -43.50 | +174.00 | +101.50 |
| 2026-09-04 | `GSM` | 541 | — | $4.67 | +0.00 | $4.67 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-09-04 | `PIPR` | 33 | — | $76.55 | +0.00 | $77.04 | -16.17 | -16.17 | -0.00 | -16.17 |
| 2026-09-08 | `OPK` | 1450 | $1.64 | $1.63 | +14.50 | — | +0.00 | +14.50 | +116.00 | — |
| 2026-09-08 | `GSM` | 541 | $4.67 | $4.75 | -43.28 | $4.52 | +124.43 | +81.15 | -43.28 | +81.15 |
| 2026-09-08 | `PIPR` | 33 | $77.04 | $76.64 | +13.20 | — | +0.00 | +13.20 | -2.97 | — |
| 2026-09-09 | `GSM` | 541 | $4.52 | $4.52 | +0.00 | — | +0.00 | +0.00 | +81.15 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `QRVO` | 11 | — | $112.83 | +0.00 | $116.65 | -41.97 | -41.97 | -0.00 | -41.97 |
| 2026-09-11 | `RWT` | 358 | — | $3.52 | +0.00 | $3.55 | -10.74 | -10.74 | -0.00 | -10.74 |
| 2026-09-11 | `CRDL` | 621 | — | $2.03 | +0.00 | $2.00 | +21.73 | +21.73 | -0.00 | +21.73 |
| 2026-09-11 | `MYGN` | 374 | — | $3.37 | +0.00 | $3.42 | -18.70 | -18.70 | -0.00 | -18.70 |
| 2026-09-14 | `QRVO` | 11 | $116.65 | $114.11 | +27.94 | — | +0.00 | +27.94 | -14.03 | — |
| 2026-09-14 | `RWT` | 358 | $3.55 | $3.53 | +7.16 | — | +0.00 | +7.16 | -3.58 | — |
| 2026-09-14 | `CRDL` | 621 | $2.00 | $1.98 | +9.32 | — | +0.00 | +9.32 | +31.05 | — |
| 2026-09-14 | `MYGN` | 374 | $3.42 | $3.43 | -3.74 | — | +0.00 | -3.74 | -22.44 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `BBNX` | 134 | — | $18.61 | +0.00 | $22.18 | -478.38 | -478.38 | -0.00 | -478.38 |
| 2026-09-16 | `GFR` | 367 | — | $6.83 | +0.00 | $6.49 | +124.78 | +124.78 | -0.00 | +124.78 |
| 2026-09-17 | `BBNX` | 134 | $22.18 | $22.46 | -37.52 | $21.43 | +138.02 | +100.50 | -515.90 | -377.88 |
| 2026-09-17 | `GFR` | 367 | $6.49 | $6.48 | +3.67 | — | +0.00 | +3.67 | +128.45 | — |
| 2026-09-17 | `BULL` | 303 | — | $7.95 | +0.00 | $7.71 | +72.72 | +72.72 | -0.00 | +72.72 |
| 2026-09-17 | `LEN` | 29 | — | $81.00 | +0.00 | $79.70 | +37.70 | +37.70 | -0.00 | +37.70 |
| 2026-09-18 | `BBNX` | 134 | $21.43 | $21.30 | +17.42 | — | +0.00 | +17.42 | -360.46 | — |
| 2026-09-18 | `BULL` | 303 | $7.71 | $7.85 | -42.42 | — | +0.00 | -42.42 | +30.30 | — |
| 2026-09-18 | `LEN` | 29 | $79.70 | $78.25 | +42.05 | — | +0.00 | +42.05 | +79.75 | — |
| 2026-09-18 | `FIVN` | 143 | — | $34.44 | +0.00 | $32.47 | +281.71 | +281.71 | -0.00 | +281.71 |
| 2026-09-21 | `FIVN` | 143 | $32.47 | $33.00 | -75.79 | — | +0.00 | -75.79 | +205.92 | — |
| 2026-09-21 | `AEHL` | 305 | — | $8.26 | +0.00 | $6.92 | +408.70 | +408.70 | -0.00 | +408.70 |
| 2026-09-21 | `AMD` | 4 | — | $583.88 | +0.00 | $615.52 | -126.56 | -126.56 | -0.00 | -126.56 |
| 2026-09-22 | `AEHL` | 305 | $6.92 | $7.13 | -64.05 | $7.61 | -146.40 | -210.45 | +344.65 | +198.25 |
| 2026-09-22 | `AMD` | 4 | $615.52 | $606.57 | +35.80 | — | +0.00 | +35.80 | -90.76 | — |
| 2026-09-22 | `USFD` | 18 | — | $94.22 | +0.00 | $94.22 | +0.00 | +0.00 | -0.00 | -0.00 |
| 2026-09-22 | `FIVN` | 46 | — | $37.35 | +0.00 | $37.20 | +6.90 | +6.90 | -0.00 | +6.90 |
| 2026-09-22 | `PAYX` | 14 | — | $115.35 | +0.00 | $115.21 | +1.96 | +1.96 | -0.00 | +1.96 |
| 2026-09-23 | `AEHL` | 305 | $7.61 | $7.62 | -3.05 | $8.22 | -184.52 | -187.57 | +195.20 | +10.68 |
| 2026-09-23 | `USFD` | 18 | $94.22 | $93.97 | +4.50 | — | +0.00 | +4.50 | +4.50 | — |
| 2026-09-23 | `FIVN` | 46 | $37.20 | $38.99 | -82.34 | $37.68 | +60.26 | -22.08 | -75.44 | -15.18 |
| 2026-09-23 | `PAYX` | 14 | $115.21 | $109.67 | +77.56 | — | +0.00 | +77.56 | +79.52 | — |
| 2026-09-23 | `HALO` | 43 | — | $116.84 | +0.00 | $112.44 | +189.42 | +189.42 | -0.00 | +189.42 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +33.62 | EU, LUNR, OWL | — | $14,954.53 | $10,010.33 | EU×1412, LUNR×86, OWL×131 |
| 2026-08-17 | +2.25 | $14,954.53 | EU×1412, LUNR×86, OWL×131 | $9,916.79 | -93.54 | -33.20 | VERI, ZNTL, APMD, HIVE, RNW | EU, LUNR, OWL | $14,801.18 | $9,836.88 | VERI×860, ZNTL×277, APMD×31, HIVE×328, RNW×145 |
| 2026-08-18 | -6.20 | $14,801.18 | VERI×860, ZNTL×277, APMD×31, HIVE×328, RNW×145 | $9,879.85 | +42.97 | +1.45 | — | VERI, ZNTL, APMD, HIVE | $10,849.22 | $9,860.32 | RNW×145 |
| 2026-08-19 | -7.20 | $10,849.22 | RNW×145 | $9,857.42 | -2.90 | +0.00 | — | RNW | $9,854.99 | $9,854.99 | — |
| 2026-08-20 | +1.12 | $9,854.99 | — | $9,854.99 | +0.00 | +67.83 | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | — | $14,556.11 | $9,905.45 | AEM×3, WYFI×28, TOYO×139, ABCL×52, TEAM×3, AAP×13, WMT×5, AQST×133 |
| 2026-08-21 | +3.25 | $14,556.11 | AEM×3, WYFI×28, TOYO×139, ABCL×52, TEAM×3, AAP×13, WMT×5, AQST×133 | $9,855.67 | -49.78 | -19.41 | QTRX, MRNA, AUGO, SSRM, ARIS, NOG | AEM, WYFI, TOYO, ABCL, TEAM, AAP, WMT, AQST | $14,674.67 | $9,805.27 | QTRX×263, MRNA×6, AUGO×9, SSRM×21, ARIS×39, NOG×30 |
| 2026-08-24 | -5.17 | $14,674.67 | QTRX×263, MRNA×6, AUGO×9, SSRM×21, ARIS×39, NOG×30 | $9,798.16 | -7.11 | +8.40 | — | QTRX, MRNA, AUGO, SSRM, ARIS | $10,600.18 | $9,794.98 | NOG×30 |
| 2026-08-25 | +1.80 | $10,600.18 | NOG×30 | $9,818.38 | +23.40 | +15.07 | AVAH, SSRM, ARE, BMO, INTU | NOG | $14,353.18 | $9,820.82 | AVAH×72, SSRM×26, ARE×18, BMO×5, INTU×2 |
| 2026-08-26 | +2.02 | $14,353.18 | AVAH×72, SSRM×26, ARE×18, BMO×5, INTU×2 | $9,908.82 | +88.00 | -94.28 | BE, ABCL, AQST, NEM, CRM | AVAH, SSRM, ARE, BMO, INTU | $14,446.77 | $9,793.15 | BE×4, ABCL×81, AQST×194, NEM×7, CRM×4 |
| 2026-08-27 | — | $14,446.77 | BE×4, ABCL×81, AQST×194, NEM×7, CRM×4 | $9,663.12 | -130.03 | +48.07 | INTU, MT, TX | BE, ABCL, NEM, CRM | $15,275.96 | $9,696.62 | AQST×194, INTU×4, MT×21, TX×29 |
| 2026-08-28 | +0.75 | $15,275.96 | AQST×194, INTU×4, MT×21, TX×29 | $9,687.02 | -9.60 | +166.67 | SIMO, FIG | AQST, INTU, MT, TX | $14,358.44 | $9,840.55 | SIMO×9, FIG×80 |
| 2026-08-31 | -5.85 | $14,358.44 | SIMO×9, FIG×80 | $9,926.99 | +86.44 | +0.00 | — | SIMO, FIG | $9,922.74 | $9,922.74 | — |
| 2026-09-01 | -6.30 | $9,922.74 | — | $9,922.74 | +0.00 | +0.00 | — | — | $9,922.74 | $9,922.74 | — |
| 2026-09-02 | -3.83 | $9,922.74 | — | $9,922.74 | +0.00 | +0.00 | — | — | $9,922.74 | $9,922.74 | — |
| 2026-09-03 | -0.90 | $9,922.74 | — | $9,922.74 | +0.00 | +155.02 | SLN, OPK | — | $14,860.55 | $10,056.12 | SLN×167, OPK×1450 |
| 2026-09-04 | +2.25 | $14,860.55 | SLN×167, OPK×1450 | $10,111.84 | +55.72 | -88.67 | GSM, PIPR | SLN | $17,458.13 | $10,011.34 | OPK×1450, GSM×541, PIPR×33 |
| 2026-09-08 | -11.47 | $17,458.13 | OPK×1450, GSM×541, PIPR×33 | $9,995.76 | -15.58 | +124.43 | — | OPK, PIPR | $12,544.71 | $10,099.39 | GSM×541 |
| 2026-09-09 | -13.95 | $12,544.71 | GSM×541 | $10,099.39 | +0.00 | +0.00 | — | GSM | $10,092.41 | $10,092.41 | — |
| 2026-09-10 | -13.28 | $10,092.41 | — | $10,092.41 | +0.00 | +0.00 | — | — | $10,092.41 | $10,092.41 | — |
| 2026-09-11 | +0.50 | $10,092.41 | — | $10,092.41 | +0.00 | -49.68 | QRVO, RWT, CRDL, MYGN | — | $15,094.88 | $10,022.85 | QRVO×11, RWT×358, CRDL×621, MYGN×374 |
| 2026-09-14 | -11.00 | $15,094.88 | QRVO×11, RWT×358, CRDL×621, MYGN×374 | $10,063.53 | +40.68 | +0.00 | — | QRVO, RWT, CRDL, MYGN | $10,044.05 | $10,044.05 | — |
| 2026-09-15 | -3.84 | $10,044.05 | — | $10,044.05 | +0.00 | +0.00 | — | — | $10,044.05 | $10,044.05 | — |
| 2026-09-16 | +5.30 | $10,044.05 | — | $10,044.05 | +0.00 | -353.60 | BBNX, GFR | — | $15,037.02 | $9,683.07 | BBNX×134, GFR×367 |
| 2026-09-17 | +7.38 | $15,037.02 | BBNX×134, GFR×367 | $9,649.22 | -33.85 | +248.44 | BULL, LEN | GFR | $17,405.76 | $9,886.71 | BBNX×134, BULL×303, LEN×29 |
| 2026-09-18 | +4.86 | $17,405.76 | BBNX×134, BULL×303, LEN×29 | $9,903.76 | +17.05 | +281.71 | FIVN | BBNX, BULL, LEN | $14,817.68 | $10,174.47 | FIVN×143 |
| 2026-09-21 | +12.87 | $14,817.68 | FIVN×143 | $10,098.68 | -75.79 | +282.14 | AEHL, AMD | FIVN | $14,944.92 | $10,372.24 | AEHL×305, AMD×4 |
| 2026-09-22 | -0.50 | $14,944.92 | AEHL×305, AMD×4 | $10,343.99 | -28.25 | -137.54 | USFD, FIVN, PAYX | AMD | $17,539.18 | $10,198.03 | AEHL×305, USFD×18, FIVN×46, PAYX×14 |
| 2026-09-23 | +2.29 | $17,539.18 | AEHL×305, USFD×18, FIVN×46, PAYX×14 | $10,194.70 | -3.33 | +65.16 | HALO | USFD, PAYX | $19,330.29 | $10,253.47 | AEHL×305, FIVN×46, HALO×43 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1412 | $1.18 | $18.51 | — | $11,647.65 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $13,293.95 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $14,954.53 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; leftover $1666.67 | join🟢 sector🟢 gen🟢 news🔴 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,954.53 | ▲ close $10,010.33 vs 09:30 $10,000.00 (session +33.62) | 16:00 close · cash $14,954.53 · equity $10,010.33 vs 09:30 $10,000.00 (+10.33; session marks +33.62) · 3 name(s) marked open→close (per-name table). EU×1412 09:30 $1.18 → close $1.21 -42.36; LUNR×86 09:30 $19.17 → close $19.01 +13.76; OWL×131 09:30 $12.70 → close $12.22 +62.22 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,954.53 | ▼ 09:30 equity $9,916.79 vs yday $10,010.33 (-93.54) | 09:30 open · cash $14,954.53 (unchanged overnight, no fees) · equity $9,916.79 vs prior close $10,010.33 (-93.54) · 3 name(s) re-marked at the open (per-name table). EU×1412 yday $1.21 → 09:30 $1.21 -0.00; LUNR×86 yday $19.01 → 09:30 $20.25 -106.64; OWL×131 yday $12.22 → 09:30 $12.12 +13.10 | — |
| 2026-08-17 09:30 ET | **COVER** | `EU` | 1412 | $1.21 | $18.21 | $-79.08 | $13,227.80 | ▼ -79.08 after sell → book $9,898.58; vs 09:30 mark -18.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `LUNR` | 86 | $20.25 | $2.25 | $-97.45 | $11,484.05 | ▼ -97.45 after sell → book $9,896.33; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **COVER** | `OWL` | 131 | $12.12 | $2.38 | $+70.48 | $9,893.95 | ▲ +70.48 after sell → book $9,893.95; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 860 | $1.15 | $11.27 | — | $10,871.67 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; leftover $989.39 | join🟡 sector🟢 gen🟢 news🔴 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 277 | $3.56 | $3.66 | — | $11,854.14 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; leftover $989.39 | join🟡 sector🔴 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 31 | $31.70 | $2.13 | — | $12,834.71 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; leftover $989.39 | join🟡 sector🔴 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 328 | $3.01 | $4.32 | — | $13,817.66 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; leftover $989.39 | join🟢 sector🟢 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 145 | $6.80 | $2.49 | — | $14,801.18 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; leftover $989.39 | join🟡 sector🟢 gen🟢 news🔴 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,801.18 | ▼ close $9,836.88 vs 09:30 $9,916.79 (session -33.20) | 16:00 close · cash $14,801.18 · equity $9,836.88 vs 09:30 $9,916.79 (-79.91; session marks -33.20) · 5 name(s) marked open→close (per-name table). VERI×860 09:30 $1.15 → close $1.08 +55.90; ZNTL×277 09:30 $3.56 → close $3.71 -40.17; APMD×31 09:30 $31.70 → close $32.55 -26.35; HIVE×328 09:30 $3.01 → close $3.07 -19.68; RNW×145 09:30 $6.80 → close $6.82 -2.90 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,801.18 | ▲ 09:30 equity $9,879.85 vs yday $9,836.88 (+42.97) | 09:30 open · cash $14,801.18 (unchanged overnight, no fees) · equity $9,879.85 vs prior close $9,836.88 (+42.97) · 5 name(s) re-marked at the open (per-name table). VERI×860 yday $1.08 → 09:30 $1.05 +30.10; ZNTL×277 yday $3.71 → 09:30 $3.75 -12.46; APMD×31 yday $32.55 → 09:30 $32.85 -9.30; HIVE×328 yday $3.07 → 09:30 $2.96 +36.08; RNW×145 yday $6.82 → 09:30 $6.83 -1.45 | — |
| 2026-08-18 09:30 ET | **COVER** | `VERI` | 860 | $1.05 | $11.09 | $+63.63 | $13,887.08 | ▲ +63.63 after sell → book $9,868.75; vs 09:30 mark -11.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `ZNTL` | 277 | $3.75 | $3.57 | $-59.86 | $12,844.76 | ▼ -59.86 after sell → book $9,865.18; vs 09:30 mark -3.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `APMD` | 31 | $32.85 | $2.08 | $-39.86 | $11,824.33 | ▼ -39.86 after sell → book $9,863.10; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `HIVE` | 328 | $2.96 | $4.23 | $+7.85 | $10,849.22 | ▲ +7.85 after sell → book $9,858.87; vs 09:30 mark -4.23 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,849.22 | ▲ close $9,860.32 vs 09:30 $9,879.85 (session +1.45) | 16:00 close · cash $10,849.22 · equity $9,860.32 vs 09:30 $9,879.85 (-19.53; session marks +1.45) · 1 name(s) marked open→close (per-name table). RNW×145 09:30 $6.83 → close $6.82 +1.45 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,849.22 | ▼ 09:30 equity $9,857.42 vs yday $9,860.32 (-2.90) | 09:30 open · cash $10,849.22 (unchanged overnight, no fees) · equity $9,857.42 vs prior close $9,860.32 (-2.90) · 1 name(s) re-marked at the open (per-name table). RNW×145 yday $6.82 → 09:30 $6.84 -2.90 | — |
| 2026-08-19 09:30 ET | **COVER** | `RNW` | 145 | $6.84 | $2.42 | $-10.71 | $9,854.99 | ▼ -10.71 after sell → book $9,854.99; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,854.99 | ▲ close $9,854.99 vs 09:30 $9,857.42 (session +0.00) | 16:00 close · cash $9,854.99 · no lots left · equity $9,854.99. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,854.99 | ▲ 09:30 equity $9,854.99 vs yday $9,854.99 (+0.00) | 09:30 open · cash $9,854.99 · no holdings · equity $9,854.99 vs prior close $9,854.99 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $10,466.31 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $615.94 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 28 | $21.40 | $2.11 | — | $11,063.40 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; leftover $615.94 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 139 | $4.43 | $2.46 | — | $11,676.71 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; leftover $615.94 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 52 | $11.81 | $2.18 | — | $12,288.91 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $615.94 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $12,808.57 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; leftover $615.94 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $13,415.56 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; leftover $615.94 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 5 | $106.38 | $2.04 | — | $13,945.42 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; leftover $615.94 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 133 | $4.61 | $2.44 | — | $14,556.11 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $615.94 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,556.11 | ▲ close $9,905.45 vs 09:30 $9,854.99 (session +67.83) | 16:00 close · cash $14,556.11 · equity $9,905.45 vs 09:30 $9,854.99 (+50.46; session marks +67.83) · 8 name(s) marked open→close (per-name table). AEM×3 09:30 $204.45 → close $212.04 -22.77; WYFI×28 09:30 $21.40 → close $21.16 +6.72; TOYO×139 09:30 $4.43 → close $4.51 -11.81; ABCL×52 09:30 $11.81 → close $11.57 +12.74; TEAM×3 09:30 $173.90 → close $174.91 -3.03; AAP×13 09:30 $46.85 → close $42.39 +57.98; WMT×5 09:30 $106.38 → close $103.84 +12.70; AQST×133 09:30 $4.61 → close $4.50 +15.30 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,556.11 | ▼ 09:30 equity $9,855.67 vs yday $9,905.45 (-49.78) | 09:30 open · cash $14,556.11 (unchanged overnight, no fees) · equity $9,855.67 vs prior close $9,905.45 (-49.78) · 8 name(s) re-marked at the open (per-name table). AEM×3 yday $212.04 → 09:30 $216.30 -12.78; WYFI×28 yday $21.16 → 09:30 $21.54 -10.64; TOYO×139 yday $4.51 → 09:30 $4.68 -22.94; ABCL×52 yday $11.57 → 09:30 $11.57 -0.00; TEAM×3 yday $174.91 → 09:30 $174.22 +2.07; AAP×13 yday $42.39 → 09:30 $42.41 -0.26; WMT×5 yday $103.84 → 09:30 $103.69 +0.75; AQST×133 yday $4.50 → 09:30 $4.54 -5.98 | — |
| 2026-08-21 09:30 ET | **COVER** | `AEM` | 3 | $216.30 | $2.00 | $-39.58 | $13,905.21 | ▼ -39.58 after sell → book $9,853.67; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `WYFI` | 28 | $21.54 | $2.07 | $-8.10 | $13,300.02 | ▼ -8.10 after sell → book $9,851.60; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TOYO` | 139 | $4.68 | $2.41 | $-39.61 | $12,647.09 | ▼ -39.61 after sell → book $9,849.19; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ABCL` | 52 | $11.57 | $2.15 | $+8.41 | $12,043.30 | ▲ +8.41 after sell → book $9,847.04; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TEAM` | 3 | $174.22 | $2.00 | $-4.99 | $11,518.64 | ▼ -4.99 after sell → book $9,845.04; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `AAP` | 13 | $42.41 | $2.03 | $+53.63 | $10,965.28 | ▲ +53.63 after sell → book $9,843.01; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `WMT` | 5 | $103.69 | $2.00 | $+9.41 | $10,444.83 | ▲ +9.41 after sell → book $9,841.01; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `AQST` | 133 | $4.54 | $2.39 | $+4.48 | $9,838.62 | ▲ +4.48 after sell → book $9,838.62; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 263 | $3.11 | $3.47 | — | $10,653.08 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $819.89 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $11,449.69 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $819.89 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $12,249.53 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $819.89 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 21 | $38.40 | $2.10 | — | $13,053.84 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $819.89 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 39 | $20.90 | $2.15 | — | $13,866.79 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $819.89 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 30 | $27.00 | $2.12 | — | $14,674.67 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; leftover $819.89 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,674.67 | ▼ close $9,805.27 vs 09:30 $9,855.67 (session -19.41) | 16:00 close · cash $14,674.67 · equity $9,805.27 vs 09:30 $9,855.67 (-50.40; session marks -19.41) · 6 name(s) marked open→close (per-name table). QTRX×263 09:30 $3.11 → close $2.99 +31.56; MRNA×6 09:30 $133.11 → close $145.13 -72.12; AUGO×9 09:30 $89.10 → close $87.26 +16.56; SSRM×21 09:30 $38.40 → close $37.77 +13.23; ARIS×39 09:30 $20.90 → close $20.86 +1.56; NOG×30 09:30 $27.00 → close $27.34 -10.20 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,674.67 | ▼ 09:30 equity $9,798.16 vs yday $9,805.27 (-7.11) | 09:30 open · cash $14,674.67 (unchanged overnight, no fees) · equity $9,798.16 vs prior close $9,805.27 (-7.11) · 6 name(s) re-marked at the open (per-name table). QTRX×263 yday $2.99 → 09:30 $2.99 -0.00; MRNA×6 yday $145.13 → 09:30 $142.70 +14.58; AUGO×9 yday $87.26 → 09:30 $88.60 -12.06; SSRM×21 yday $37.77 → 09:30 $38.32 -11.55; ARIS×39 yday $20.86 → 09:30 $20.98 -4.68; NOG×30 yday $27.34 → 09:30 $27.12 +6.60 | — |
| 2026-08-24 09:30 ET | **COVER** | `QTRX` | 263 | $2.99 | $3.39 | $+24.70 | $13,884.90 | ▲ +24.70 after sell → book $9,794.76; vs 09:30 mark -3.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `MRNA` | 6 | $142.70 | $2.01 | $-61.60 | $13,026.70 | ▼ -61.60 after sell → book $9,792.76; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AUGO` | 9 | $88.60 | $2.02 | $+0.42 | $12,227.28 | ▲ +0.42 after sell → book $9,790.74; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `SSRM` | 21 | $38.32 | $2.05 | $-2.47 | $11,420.51 | ▼ -2.47 after sell → book $9,788.69; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `ARIS` | 39 | $20.98 | $2.11 | $-7.38 | $10,600.18 | ▼ -7.38 after sell → book $9,786.58; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,600.18 | ▲ close $9,794.98 vs 09:30 $9,798.16 (session +8.40) | 16:00 close · cash $10,600.18 · equity $9,794.98 vs 09:30 $9,798.16 (-3.18; session marks +8.40) · 1 name(s) marked open→close (per-name table). NOG×30 09:30 $27.12 → close $26.84 +8.40 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,600.18 | ▲ 09:30 equity $9,818.38 vs yday $9,794.98 (+23.40) | 09:30 open · cash $10,600.18 (unchanged overnight, no fees) · equity $9,818.38 vs prior close $9,794.98 (+23.40) · 1 name(s) re-marked at the open (per-name table). NOG×30 yday $26.84 → 09:30 $26.06 +23.40 | — |
| 2026-08-25 09:30 ET | **COVER** | `NOG` | 30 | $26.06 | $2.08 | $+24.00 | $9,816.30 | ▲ +24.00 after sell → book $9,816.30; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 72 | $13.62 | $2.25 | — | $10,795.04 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $981.63 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `SSRM` | 26 | $37.75 | $2.11 | — | $11,774.43 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.8; leftover $981.63 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 18 | $54.51 | $2.09 | — | $12,753.52 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; leftover $981.63 | join🔴 sector🟡 gen🟡 news🔴 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 5 | $175.01 | $2.05 | — | $13,626.52 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; leftover $981.63 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 2 | $364.35 | $2.04 | — | $14,353.18 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $981.63 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,353.18 | ▲ close $9,820.82 vs 09:30 $9,818.38 (session +15.07) | 16:00 close · cash $14,353.18 · equity $9,820.82 vs 09:30 $9,818.38 (+2.44; session marks +15.07) · 5 name(s) marked open→close (per-name table). AVAH×72 09:30 $13.62 → close $13.59 +2.52; SSRM×26 09:30 $37.75 → close $39.21 -37.96; ARE×18 09:30 $54.51 → close $52.90 +28.98; BMO×5 09:30 $175.01 → close $173.46 +7.75; INTU×2 09:30 $364.35 → close $357.46 +13.78 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,353.18 | ▲ 09:30 equity $9,908.82 vs yday $9,820.82 (+88.00) | 09:30 open · cash $14,353.18 (unchanged overnight, no fees) · equity $9,908.82 vs prior close $9,820.82 (+88.00) · 5 name(s) re-marked at the open (per-name table). AVAH×72 yday $13.59 → 09:30 $13.65 -4.32; SSRM×26 yday $39.21 → 09:30 $38.41 +20.80; ARE×18 yday $52.90 → 09:30 $52.77 +2.34; BMO×5 yday $173.46 → 09:30 $173.22 +1.20; INTU×2 yday $357.46 → 09:30 $323.47 +67.98 | — |
| 2026-08-26 09:30 ET | **COVER** | `AVAH` | 72 | $13.65 | $2.21 | $-6.26 | $13,368.18 | ▼ -6.26 after sell → book $9,906.62; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 26 | $38.41 | $2.07 | $-21.34 | $12,367.45 | ▼ -21.34 after sell → book $9,904.55; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARE` | 18 | $52.77 | $2.04 | $+27.19 | $11,415.55 | ▲ +27.19 after sell → book $9,902.51; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `BMO` | 5 | $173.22 | $2.00 | $+4.90 | $10,547.44 | ▲ +4.90 after sell → book $9,900.50; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `INTU` | 2 | $323.47 | $2.00 | $+77.73 | $9,898.50 | ▲ +77.73 after sell → book $9,898.50; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 4 | $213.94 | $2.05 | — | $10,752.22 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $989.85 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 81 | $12.22 | $2.28 | — | $11,739.76 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; leftover $989.85 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 194 | $5.08 | $2.64 | — | $12,722.63 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; leftover $989.85 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 7 | $132.64 | $2.06 | — | $13,649.06 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; leftover $989.85 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 4 | $199.94 | $2.04 | — | $14,446.77 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; leftover $989.85 | join🟡 sector🔴 gen🟢 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 catal🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,446.77 | ▼ close $9,793.15 vs 09:30 $9,908.82 (session -94.28) | 16:00 close · cash $14,446.77 · equity $9,793.15 vs 09:30 $9,908.82 (-115.67; session marks -94.28) · 5 name(s) marked open→close (per-name table). BE×4 09:30 $213.94 → close $218.21 -17.08; ABCL×81 09:30 $12.22 → close $12.24 -1.62; AQST×194 09:30 $5.08 → close $5.39 -60.14; NEM×7 09:30 $132.64 → close $131.60 +7.28; CRM×4 09:30 $199.94 → close $205.62 -22.72 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,446.77 | ▼ 09:30 equity $9,663.12 vs yday $9,793.15 (-130.03) | 09:30 open · cash $14,446.77 (unchanged overnight, no fees) · equity $9,663.12 vs prior close $9,793.15 (-130.03) · 5 name(s) re-marked at the open (per-name table). BE×4 yday $218.21 → 09:30 $227.10 -35.56; ABCL×81 yday $12.24 → 09:30 $12.25 -0.81; AQST×194 yday $5.39 → 09:30 $5.39 -0.00; NEM×7 yday $131.60 → 09:30 $131.02 +4.06; CRM×4 yday $205.62 → 09:30 $230.05 -97.72 | — |
| 2026-08-27 09:30 ET | **COVER** | `BE` | 4 | $227.10 | $2.00 | $-56.69 | $13,536.37 | ▼ -56.69 after sell → book $9,661.12; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **COVER** | `ABCL` | 81 | $12.25 | $2.23 | $-6.95 | $12,541.89 | ▼ -6.95 after sell → book $9,658.89; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `NEM` | 7 | $131.02 | $2.01 | $+7.27 | $11,622.74 | ▲ +7.27 after sell → book $9,656.88; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `CRM` | 4 | $230.05 | $2.00 | $-124.49 | $10,700.54 | ▼ -124.49 after sell → book $9,654.88; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SHORT** | `INTU` | 4 | $353.54 | $2.06 | — | $12,112.63 | — | news🔴; gate news=bad; list earn_react; ret5=-4.6; leftover $1609.15 | join🔴 sector🟢 gen🟢 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 21 | $74.54 | $2.12 | — | $13,675.86 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; leftover $1609.15 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 29 | $55.25 | $2.14 | — | $15,275.96 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; leftover $1609.15 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,275.96 | ▲ close $9,696.62 vs 09:30 $9,663.12 (session +48.07) | 16:00 close · cash $15,275.96 · equity $9,696.62 vs 09:30 $9,663.12 (+33.50; session marks +48.07) · 4 name(s) marked open→close (per-name table). AQST×194 09:30 $5.39 → close $5.16 +44.62; INTU×4 09:30 $353.54 → close $348.00 +22.16; MT×21 09:30 $74.54 → close $74.63 -1.89; TX×29 09:30 $55.25 → close $55.83 -16.82 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,275.96 | ▼ 09:30 equity $9,687.02 vs yday $9,696.62 (-9.60) | 09:30 open · cash $15,275.96 (unchanged overnight, no fees) · equity $9,687.02 vs prior close $9,696.62 (-9.60) · 4 name(s) re-marked at the open (per-name table). AQST×194 yday $5.16 → 09:30 $5.11 +9.70; INTU×4 yday $348.00 → 09:30 $347.82 +0.72; MT×21 yday $74.63 → 09:30 $75.39 -15.96; TX×29 yday $55.83 → 09:30 $55.97 -4.06 | — |
| 2026-08-28 09:30 ET | **COVER** | `AQST` | 194 | $5.11 | $2.57 | $-11.03 | $14,282.05 | ▼ -11.03 after sell → book $9,684.45; vs 09:30 mark -2.57 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 4 | $347.82 | $2.00 | $+18.82 | $12,888.77 | ▲ +18.82 after sell → book $9,682.45; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `MT` | 21 | $75.39 | $2.05 | $-22.02 | $11,303.52 | ▼ -22.02 after sell → book $9,680.39; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `TX` | 29 | $55.97 | $2.08 | $-25.10 | $9,678.32 | ▼ -25.10 after sell → book $9,678.32; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 9 | $252.24 | $2.11 | — | $11,946.37 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $2419.58 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 80 | $30.18 | $2.33 | — | $14,358.44 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; leftover $2419.58 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,358.44 | ▲ close $9,840.55 vs 09:30 $9,687.02 (session +166.67) | 16:00 close · cash $14,358.44 · equity $9,840.55 vs 09:30 $9,687.02 (+153.53; session marks +166.67) · 2 name(s) marked open→close (per-name table). SIMO×9 09:30 $252.24 → close $245.81 +57.87; FIG×80 09:30 $30.18 → close $28.82 +108.80 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,358.44 | ▲ 09:30 equity $9,926.99 vs yday $9,840.55 (+86.44) | 09:30 open · cash $14,358.44 (unchanged overnight, no fees) · equity $9,926.99 vs prior close $9,840.55 (+86.44) · 2 name(s) re-marked at the open (per-name table). SIMO×9 yday $245.81 → 09:30 $247.05 -11.16; FIG×80 yday $28.82 → 09:30 $27.60 +97.60 | — |
| 2026-08-31 09:30 ET | **COVER** | `SIMO` | 9 | $247.05 | $2.02 | $+42.59 | $12,132.97 | ▲ +42.59 after sell → book $9,924.97; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `FIG` | 80 | $27.60 | $2.23 | $+201.84 | $9,922.74 | ▲ +201.84 after sell → book $9,922.74; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,922.74 | ▲ close $9,922.74 vs 09:30 $9,926.99 (session +0.00) | 16:00 close · cash $9,922.74 · no lots left · equity $9,922.74. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,922.74 | ▲ 09:30 equity $9,922.74 vs yday $9,922.74 (+0.00) | 09:30 open · cash $9,922.74 · no holdings · equity $9,922.74 vs prior close $9,922.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,922.74 | ▲ close $9,922.74 vs 09:30 $9,922.74 (session +0.00) | 16:00 close · cash $9,922.74 · no lots left · equity $9,922.74. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,922.74 | ▲ 09:30 equity $9,922.74 vs yday $9,922.74 (+0.00) | 09:30 open · cash $9,922.74 · no holdings · equity $9,922.74 vs prior close $9,922.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,922.74 | ▲ close $9,922.74 vs 09:30 $9,922.74 (session +0.00) | 16:00 close · cash $9,922.74 · no lots left · equity $9,922.74. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,922.74 | ▲ 09:30 equity $9,922.74 vs yday $9,922.74 (+0.00) | 09:30 open · cash $9,922.74 · no holdings · equity $9,922.74 vs prior close $9,922.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 167 | $14.85 | $2.61 | — | $12,400.09 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $2480.69 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1450 | $1.71 | $19.03 | — | $14,860.55 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $2480.69 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,860.55 | ▲ close $10,056.12 vs 09:30 $9,922.74 (session +155.02) | 16:00 close · cash $14,860.55 · equity $10,056.12 vs 09:30 $9,922.74 (+133.38; session marks +155.02) · 2 name(s) marked open→close (per-name table). SLN×167 09:30 $14.85 → close $14.79 +10.02; OPK×1450 09:30 $1.71 → close $1.61 +145.00 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,860.55 | ▲ 09:30 equity $10,111.84 vs yday $10,056.12 (+55.72) | 09:30 open · cash $14,860.55 (unchanged overnight, no fees) · equity $10,111.84 vs prior close $10,056.12 (+55.72) · 2 name(s) re-marked at the open (per-name table). SLN×167 yday $14.79 → 09:30 $14.63 +26.72; OPK×1450 yday $1.61 → 09:30 $1.59 +29.00 | — |
| 2026-09-04 09:30 ET | **COVER** | `SLN` | 167 | $14.63 | $2.49 | $+31.64 | $12,414.85 | ▲ +31.64 after sell → book $10,109.35; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 541 | $4.67 | $7.16 | — | $14,934.17 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; leftover $2527.34 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 33 | $76.55 | $2.19 | — | $17,458.13 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $2527.34 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,458.13 | ▼ close $10,011.34 vs 09:30 $10,111.84 (session -88.67) | 16:00 close · cash $17,458.13 · equity $10,011.34 vs 09:30 $10,111.84 (-100.50; session marks -88.67) · 3 name(s) marked open→close (per-name table). OPK×1450 09:30 $1.59 → close $1.64 -72.50; GSM×541 09:30 $4.67 → close $4.67 -0.00; PIPR×33 09:30 $76.55 → close $77.04 -16.17 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,458.13 | ▼ 09:30 equity $9,995.76 vs yday $10,011.34 (-15.58) | 09:30 open · cash $17,458.13 (unchanged overnight, no fees) · equity $9,995.76 vs prior close $10,011.34 (-15.58) · 3 name(s) re-marked at the open (per-name table). OPK×1450 yday $1.64 → 09:30 $1.63 +14.50; GSM×541 yday $4.67 → 09:30 $4.75 -43.28; PIPR×33 yday $77.04 → 09:30 $76.64 +13.20 | — |
| 2026-09-08 09:30 ET | **COVER** | `OPK` | 1450 | $1.63 | $18.70 | $+78.26 | $15,075.92 | ▲ +78.26 after sell → book $9,977.05; vs 09:30 mark -18.71 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `PIPR` | 33 | $76.64 | $2.09 | $-7.25 | $12,544.71 | ▼ -7.25 after sell → book $9,974.96; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,544.71 | ▲ close $10,099.39 vs 09:30 $9,995.76 (session +124.43) | 16:00 close · cash $12,544.71 · equity $10,099.39 vs 09:30 $9,995.76 (+103.63; session marks +124.43) · 1 name(s) marked open→close (per-name table). GSM×541 09:30 $4.75 → close $4.52 +124.43 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,544.71 | ▲ 09:30 equity $10,099.39 vs yday $10,099.39 (+0.00) | 09:30 open · cash $12,544.71 (unchanged overnight, no fees) · equity $10,099.39 vs prior close $10,099.39 (+0.00) · 1 name(s) re-marked at the open (per-name table). GSM×541 yday $4.52 → 09:30 $4.52 -0.00 | — |
| 2026-09-09 09:30 ET | **COVER** | `GSM` | 541 | $4.52 | $6.98 | $+67.01 | $10,092.41 | ▲ +67.01 after sell → book $10,092.41; vs 09:30 mark -6.98 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,092.41 | ▲ close $10,092.41 vs 09:30 $10,099.39 (session +0.00) | 16:00 close · cash $10,092.41 · no lots left · equity $10,092.41. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.41 | ▲ 09:30 equity $10,092.41 vs yday $10,092.41 (+0.00) | 09:30 open · cash $10,092.41 · no holdings · equity $10,092.41 vs prior close $10,092.41 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,092.41 | ▲ close $10,092.41 vs 09:30 $10,092.41 (session +0.00) | 16:00 close · cash $10,092.41 · no lots left · equity $10,092.41. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.41 | ▲ 09:30 equity $10,092.41 vs yday $10,092.41 (+0.00) | 09:30 open · cash $10,092.41 · no holdings · equity $10,092.41 vs prior close $10,092.41 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 11 | $112.83 | $2.08 | — | $11,331.52 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $1261.55 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 358 | $3.52 | $4.72 | — | $12,586.96 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; leftover $1261.55 | join🔴 sector🔴 gen🟡 news🔴 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 621 | $2.03 | $8.16 | — | $13,839.43 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; leftover $1261.55 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 judge🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 374 | $3.37 | $4.93 | — | $15,094.88 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; leftover $1261.55 | join🔴 sector🟡 gen🟡 news🔴 digest🔴 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,094.88 | ▼ close $10,022.85 vs 09:30 $10,092.41 (session -49.68) | 16:00 close · cash $15,094.88 · equity $10,022.85 vs 09:30 $10,092.41 (-69.56; session marks -49.68) · 4 name(s) marked open→close (per-name table). QRVO×11 09:30 $112.83 → close $116.65 -41.97; RWT×358 09:30 $3.52 → close $3.55 -10.74; CRDL×621 09:30 $2.03 → close $2.00 +21.73; MYGN×374 09:30 $3.37 → close $3.42 -18.70 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,094.88 | ▲ 09:30 equity $10,063.53 vs yday $10,022.85 (+40.68) | 09:30 open · cash $15,094.88 (unchanged overnight, no fees) · equity $10,063.53 vs prior close $10,022.85 (+40.68) · 4 name(s) re-marked at the open (per-name table). QRVO×11 yday $116.65 → 09:30 $114.11 +27.94; RWT×358 yday $3.55 → 09:30 $3.53 +7.16; CRDL×621 yday $2.00 → 09:30 $1.98 +9.32; MYGN×374 yday $3.42 → 09:30 $3.43 -3.74 | — |
| 2026-09-14 09:30 ET | **COVER** | `QRVO` | 11 | $114.11 | $2.02 | $-18.13 | $13,837.65 | ▼ -18.13 after sell → book $10,061.51; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **COVER** | `RWT` | 358 | $3.53 | $4.62 | $-12.92 | $12,569.29 | ▼ -12.92 after sell → book $10,056.89; vs 09:30 mark -4.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `CRDL` | 621 | $1.98 | $8.01 | $+14.88 | $11,331.70 | ▲ +14.88 after sell → book $10,048.88; vs 09:30 mark -8.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `MYGN` | 374 | $3.43 | $4.82 | $-32.20 | $10,044.05 | ▼ -32.20 after sell → book $10,044.05; vs 09:30 mark -4.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,044.05 | ▲ close $10,044.05 vs 09:30 $10,063.53 (session +0.00) | 16:00 close · cash $10,044.05 · no lots left · equity $10,044.05. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,044.05 | ▲ 09:30 equity $10,044.05 vs yday $10,044.05 (+0.00) | 09:30 open · cash $10,044.05 · no holdings · equity $10,044.05 vs prior close $10,044.05 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,044.05 | ▲ close $10,044.05 vs 09:30 $10,044.05 (session +0.00) | 16:00 close · cash $10,044.05 · no lots left · equity $10,044.05. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,044.05 | ▲ 09:30 equity $10,044.05 vs yday $10,044.05 (+0.00) | 09:30 open · cash $10,044.05 · no holdings · equity $10,044.05 vs prior close $10,044.05 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 134 | $18.61 | $2.50 | — | $12,535.29 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $2511.01 | join🟢 sector🟢 gen🟢 news🔴 digest🔴 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 367 | $6.83 | $4.88 | — | $15,037.02 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; leftover $2511.01 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,037.02 | ▼ close $9,683.07 vs 09:30 $10,044.05 (session -353.60) | 16:00 close · cash $15,037.02 · equity $9,683.07 vs 09:30 $10,044.05 (-360.98; session marks -353.60) · 2 name(s) marked open→close (per-name table). BBNX×134 09:30 $18.61 → close $22.18 -478.38; GFR×367 09:30 $6.83 → close $6.49 +124.78 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,037.02 | ▼ 09:30 equity $9,649.22 vs yday $9,683.07 (-33.85) | 09:30 open · cash $15,037.02 (unchanged overnight, no fees) · equity $9,649.22 vs prior close $9,683.07 (-33.85) · 2 name(s) re-marked at the open (per-name table). BBNX×134 yday $22.18 → 09:30 $22.46 -37.52; GFR×367 yday $6.49 → 09:30 $6.48 +3.67 | — |
| 2026-09-17 09:30 ET | **COVER** | `GFR` | 367 | $6.48 | $4.73 | $+118.83 | $12,654.12 | ▲ +118.83 after sell → book $9,644.48; vs 09:30 mark -4.74 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 303 | $7.95 | $4.04 | — | $15,058.93 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $2411.12 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 29 | $81.00 | $2.17 | — | $17,405.76 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; leftover $2411.12 | join🔴 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,405.76 | ▲ close $9,886.71 vs 09:30 $9,649.22 (session +248.44) | 16:00 close · cash $17,405.76 · equity $9,886.71 vs 09:30 $9,649.22 (+237.49; session marks +248.44) · 3 name(s) marked open→close (per-name table). BBNX×134 09:30 $22.46 → close $21.43 +138.02; BULL×303 09:30 $7.95 → close $7.71 +72.72; LEN×29 09:30 $81.00 → close $79.70 +37.70 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,405.76 | ▲ 09:30 equity $9,903.76 vs yday $9,886.71 (+17.05) | 09:30 open · cash $17,405.76 (unchanged overnight, no fees) · equity $9,903.76 vs prior close $9,886.71 (+17.05) · 3 name(s) re-marked at the open (per-name table). BBNX×134 yday $21.43 → 09:30 $21.30 +17.42; BULL×303 yday $7.71 → 09:30 $7.85 -42.42; LEN×29 yday $79.70 → 09:30 $78.25 +42.05 | — |
| 2026-09-18 09:30 ET | **COVER** | `BBNX` | 134 | $21.30 | $2.39 | $-365.35 | $14,549.17 | ▼ -365.35 after sell → book $9,901.37; vs 09:30 mark -2.39 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `BULL` | 303 | $7.85 | $3.91 | $+22.35 | $12,166.71 | ▲ +22.35 after sell → book $9,897.46; vs 09:30 mark -3.91 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `LEN` | 29 | $78.25 | $2.08 | $+75.50 | $9,895.38 | ▲ +75.50 after sell → book $9,895.38; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 143 | $34.44 | $2.62 | — | $14,817.68 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; leftover $4947.69 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,817.68 | ▲ close $10,174.47 vs 09:30 $9,903.76 (session +281.71) | 16:00 close · cash $14,817.68 · equity $10,174.47 vs 09:30 $9,903.76 (+270.71; session marks +281.71) · 1 name(s) marked open→close (per-name table). FIVN×143 09:30 $34.44 → close $32.47 +281.71 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,817.68 | ▼ 09:30 equity $10,098.68 vs yday $10,174.47 (-75.79) | 09:30 open · cash $14,817.68 (unchanged overnight, no fees) · equity $10,098.68 vs prior close $10,174.47 (-75.79) · 1 name(s) re-marked at the open (per-name table). FIVN×143 yday $32.47 → 09:30 $33.00 -75.79 | — |
| 2026-09-21 09:30 ET | **COVER** | `FIVN` | 143 | $33.00 | $2.42 | $+200.88 | $10,096.26 | ▲ +200.88 after sell → book $10,096.26; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 305 | $8.26 | $4.07 | — | $12,611.49 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; leftover $2524.07 | join🟡 sector🔴 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 4 | $583.88 | $2.09 | — | $14,944.92 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; leftover $2524.07 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,944.92 | ▲ close $10,372.24 vs 09:30 $10,098.68 (session +282.14) | 16:00 close · cash $14,944.92 · equity $10,372.24 vs 09:30 $10,098.68 (+273.56; session marks +282.14) · 2 name(s) marked open→close (per-name table). AEHL×305 09:30 $8.26 → close $6.92 +408.70; AMD×4 09:30 $583.88 → close $615.52 -126.56 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,944.92 | ▼ 09:30 equity $10,343.99 vs yday $10,372.24 (-28.25) | 09:30 open · cash $14,944.92 (unchanged overnight, no fees) · equity $10,343.99 vs prior close $10,372.24 (-28.25) · 2 name(s) re-marked at the open (per-name table). AEHL×305 yday $6.92 → 09:30 $7.13 -64.05; AMD×4 yday $615.52 → 09:30 $606.57 +35.80 | — |
| 2026-09-22 09:30 ET | **COVER** | `AMD` | 4 | $606.57 | $2.00 | $-94.86 | $12,516.63 | ▼ -94.86 after sell → book $10,341.98; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 18 | $94.22 | $2.11 | — | $14,210.48 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; leftover $1723.66 | join🟢 sector🟢 gen🔴 news🔴 digest🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-22 09:30 ET | **SHORT** | `FIVN` | 46 | $37.35 | $2.20 | — | $15,926.38 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $1723.66 | join🟢 sector🔴 gen🔴 news🔴 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-22 09:30 ET | **SHORT** | `PAYX` | 14 | $115.35 | $2.10 | — | $17,539.18 | — | news🔴; gate news=bad; list overnight; ret5=-8.2; leftover $1723.66 | join🟢 sector🔴 gen🔴 news🔴 digest🟡 ab🟢 peer🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,539.18 | ▼ close $10,198.03 vs 09:30 $10,343.99 (session -137.54) | 16:00 close · cash $17,539.18 · equity $10,198.03 vs 09:30 $10,343.99 (-145.96; session marks -137.54) · 4 name(s) marked open→close (per-name table). AEHL×305 09:30 $7.13 → close $7.61 -146.40; USFD×18 09:30 $94.22 → close $94.22 -0.00; FIVN×46 09:30 $37.35 → close $37.20 +6.90; PAYX×14 09:30 $115.35 → close $115.21 +1.96 | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,539.18 | ▼ 09:30 equity $10,194.70 vs yday $10,198.03 (-3.33) | 09:30 open · cash $17,539.18 (unchanged overnight, no fees) · equity $10,194.70 vs prior close $10,198.03 (-3.33) · 4 name(s) re-marked at the open (per-name table). AEHL×305 yday $7.61 → 09:30 $7.62 -3.05; USFD×18 yday $94.22 → 09:30 $93.97 +4.50; FIVN×46 yday $37.20 → 09:30 $38.99 -82.34; PAYX×14 yday $115.21 → 09:30 $109.67 +77.56 | — |
| 2026-09-23 09:30 ET | **COVER** | `USFD` | 18 | $93.97 | $2.04 | $+0.34 | $15,845.68 | ▲ +0.34 after sell → book $10,192.66; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **COVER** | `PAYX` | 14 | $109.67 | $2.03 | $+75.39 | $14,308.27 | ▲ +75.39 after sell → book $10,190.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 43 | $116.84 | $2.31 | — | $19,330.29 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+1.7; leftover $5095.31 | join🟢 sector🟢 gen🟢 news🔴 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,330.29 | ▲ close $10,253.47 vs 09:30 $10,194.70 (session +65.16) | 16:00 close · cash $19,330.29 · equity $10,253.47 vs 09:30 $10,194.70 (+58.77; session marks +65.16) · 3 name(s) marked open→close (per-name table). AEHL×305 09:30 $7.62 → close $8.22 -184.52; FIVN×46 09:30 $38.99 → close $37.68 +60.26; HALO×43 09:30 $116.84 → close $112.44 +189.42 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LUNR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SPCX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AEHL` | 305 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; leftover $2524.07 |
| `FIVN` | 46 | 2026-09-22 @ $37.35 | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $1723.66 |
| `HALO` | 43 | 2026-09-23 @ $116.84 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+1.7; leftover $5095.31 |
