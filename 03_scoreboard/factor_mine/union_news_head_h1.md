# Factor mine action — `union_news_head_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · prior-export headline🟢 only

Cash book **-6.21%** ($9,379) · signal-only (no cash/fees) was +13.01%. Starts YES **2/22**. Fills 108 · skips 48 · realized $-620.56.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the prior-export headline is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
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
- **Gate** `headline=good` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,379.44.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `HLIT` | 94 | — | $13.18 | +0.00 | $13.92 | +69.56 | +69.56 | +0.00 | +69.56 |
| 2026-08-14 | `MH` | 92 | — | $13.55 | +0.00 | $13.10 | -41.40 | -41.40 | +0.00 | -41.40 |
| 2026-08-14 | `VELO` | 81 | — | $15.38 | +0.00 | $16.16 | +63.18 | +63.18 | +0.00 | +63.18 |
| 2026-08-14 | `FWRD` | 62 | — | $20.11 | +0.00 | $18.96 | -71.30 | -71.30 | +0.00 | -71.30 |
| 2026-08-14 | `S` | 52 | — | $23.77 | +0.00 | $23.11 | -34.58 | -34.58 | +0.00 | -34.58 |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `HLIT` | 94 | $13.92 | $13.84 | -7.52 | — | +0.00 | -7.52 | +62.04 | — |
| 2026-08-17 | `MH` | 92 | $13.10 | $13.16 | +5.52 | — | +0.00 | +5.52 | -35.88 | — |
| 2026-08-17 | `VELO` | 81 | $16.16 | $16.05 | -8.91 | — | +0.00 | -8.91 | +54.27 | — |
| 2026-08-17 | `FWRD` | 62 | $18.96 | $19.20 | +14.88 | — | +0.00 | +14.88 | -56.42 | — |
| 2026-08-17 | `S` | 52 | $23.11 | $22.50 | -31.72 | — | +0.00 | -31.72 | -66.30 | — |
| 2026-08-17 | `OUST` | 102 | — | $49.00 | +0.00 | $48.13 | -88.74 | -88.74 | +0.00 | -88.74 |
| 2026-08-17 | `CELC` | 53 | — | $92.99 | +0.00 | $92.44 | -29.15 | -29.15 | +0.00 | -29.15 |
| 2026-08-18 | `OUST` | 102 | $48.13 | $45.09 | -310.08 | — | +0.00 | -310.08 | -398.82 | — |
| 2026-08-18 | `CELC` | 53 | $92.44 | $92.38 | -3.18 | — | +0.00 | -3.18 | -32.33 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `AUTL` | 484 | — | $2.47 | +0.00 | $2.46 | -4.84 | -4.84 | +0.00 | -4.84 |
| 2026-08-20 | `CRSP` | 20 | — | $58.73 | +0.00 | $58.12 | -12.20 | -12.20 | +0.00 | -12.20 |
| 2026-08-20 | `ASST` | 74 | — | $16.00 | +0.00 | $16.13 | +9.62 | +9.62 | +0.00 | +9.62 |
| 2026-08-20 | `MRNA` | 7 | — | $150.14 | +0.00 | $133.32 | -117.74 | -117.74 | +0.00 | -117.74 |
| 2026-08-20 | `ZLAB` | 45 | — | $26.57 | +0.00 | $26.02 | -24.75 | -24.75 | +0.00 | -24.75 |
| 2026-08-20 | `TEAM` | 6 | — | $173.90 | +0.00 | $174.91 | +6.06 | +6.06 | +0.00 | +6.06 |
| 2026-08-20 | `HUMA` | 1691 | — | $0.71 | +0.00 | $0.68 | -43.97 | -43.97 | +0.00 | -43.97 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `AUTL` | 484 | $2.46 | $2.47 | +4.84 | $2.41 | -29.04 | -24.20 | +0.00 | -29.04 |
| 2026-08-21 | `CRSP` | 20 | $58.12 | $59.72 | +32.00 | $59.50 | -4.40 | +27.60 | +19.80 | +15.40 |
| 2026-08-21 | `ASST` | 74 | $16.13 | $17.66 | +113.22 | — | +0.00 | +113.22 | +122.84 | — |
| 2026-08-21 | `MRNA` | 7 | $133.32 | $133.11 | -1.47 | — | +0.00 | -1.47 | -119.21 | — |
| 2026-08-21 | `ZLAB` | 45 | $26.02 | $26.25 | +10.35 | — | +0.00 | +10.35 | -14.40 | — |
| 2026-08-21 | `TEAM` | 6 | $174.91 | $174.22 | -4.14 | — | +0.00 | -4.14 | +1.92 | — |
| 2026-08-21 | `HUMA` | 1691 | $0.68 | $0.67 | -11.84 | — | +0.00 | -11.84 | -55.80 | — |
| 2026-08-21 | `FUTU` | 10 | — | $115.18 | +0.00 | $123.64 | +84.60 | +84.60 | +0.00 | +84.60 |
| 2026-08-21 | `GRAL` | 15 | — | $78.88 | +0.00 | $79.54 | +9.90 | +9.90 | +0.00 | +9.90 |
| 2026-08-21 | `ABTC` | 137 | — | $8.66 | +0.00 | $7.93 | -100.01 | -100.01 | +0.00 | -100.01 |
| 2026-08-21 | `HIVE` | 366 | — | $3.24 | +0.00 | $3.03 | -76.86 | -76.86 | +0.00 | -76.86 |
| 2026-08-21 | `MARA` | 101 | — | $11.70 | +0.00 | $11.26 | -44.44 | -44.44 | +0.00 | -44.44 |
| 2026-08-21 | `BTDR` | 107 | — | $11.10 | +0.00 | $11.37 | +29.42 | +29.42 | +0.00 | +29.42 |
| 2026-08-24 | `AUTL` | 484 | $2.41 | $2.40 | -4.84 | — | +0.00 | -4.84 | -33.88 | — |
| 2026-08-24 | `CRSP` | 20 | $59.50 | $58.75 | -15.00 | $57.08 | -33.50 | -48.50 | +0.40 | -33.10 |
| 2026-08-24 | `FUTU` | 10 | $123.64 | $121.00 | -26.40 | — | +0.00 | -26.40 | +58.20 | — |
| 2026-08-24 | `GRAL` | 15 | $79.54 | $81.87 | +34.95 | — | +0.00 | +34.95 | +44.85 | — |
| 2026-08-24 | `ABTC` | 137 | $7.93 | $8.00 | +9.59 | — | +0.00 | +9.59 | -90.42 | — |
| 2026-08-24 | `HIVE` | 366 | $3.03 | $2.99 | -14.64 | — | +0.00 | -14.64 | -91.50 | — |
| 2026-08-24 | `MARA` | 101 | $11.26 | $11.17 | -9.09 | — | +0.00 | -9.09 | -53.53 | — |
| 2026-08-24 | `BTDR` | 107 | $11.37 | $11.48 | +11.77 | — | +0.00 | +11.77 | +41.19 | — |
| 2026-08-25 | `CRSP` | 20 | $57.08 | $57.93 | +17.10 | — | +0.00 | +17.10 | -16.00 | — |
| 2026-08-25 | `EZPW` | 53 | — | $35.05 | +0.00 | $35.23 | +9.54 | +9.54 | +0.00 | +9.54 |
| 2026-08-25 | `RUM` | 197 | — | $9.42 | +0.00 | $10.23 | +159.57 | +159.57 | +0.00 | +159.57 |
| 2026-08-25 | `ZYME` | 64 | — | $28.86 | +0.00 | $27.47 | -88.96 | -88.96 | +0.00 | -88.96 |
| 2026-08-25 | `REAX` | 77 | — | $24.11 | +0.00 | $28.43 | +332.64 | +332.64 | +0.00 | +332.64 |
| 2026-08-25 | `EOLS` | 213 | — | $8.72 | +0.00 | $8.97 | +54.31 | +54.31 | +0.00 | +54.31 |
| 2026-08-26 | `EZPW` | 53 | $35.23 | $35.70 | +24.91 | — | +0.00 | +24.91 | +34.45 | — |
| 2026-08-26 | `RUM` | 197 | $10.23 | $10.07 | -31.52 | — | +0.00 | -31.52 | +128.05 | — |
| 2026-08-26 | `ZYME` | 64 | $27.47 | $27.56 | +5.76 | — | +0.00 | +5.76 | -83.20 | — |
| 2026-08-26 | `REAX` | 77 | $28.43 | $26.61 | -140.14 | — | +0.00 | -140.14 | +192.50 | — |
| 2026-08-26 | `EOLS` | 213 | $8.97 | $8.86 | -24.50 | — | +0.00 | -24.50 | +29.82 | — |
| 2026-08-26 | `FNV` | 7 | — | $267.02 | +0.00 | $267.37 | +2.45 | +2.45 | +0.00 | +2.45 |
| 2026-08-26 | `TRLV` | 171 | — | $11.22 | +0.00 | $11.43 | +35.91 | +35.91 | +0.00 | +35.91 |
| 2026-08-26 | `CAPR` | 231 | — | $8.29 | +0.00 | $9.36 | +247.17 | +247.17 | +0.00 | +247.17 |
| 2026-08-26 | `FWRD` | 110 | — | $17.41 | +0.00 | $17.63 | +24.20 | +24.20 | +0.00 | +24.20 |
| 2026-08-26 | `FLNC` | 172 | — | $11.12 | +0.00 | $11.08 | -6.88 | -6.88 | +0.00 | -6.88 |
| 2026-08-27 | `FNV` | 7 | $267.37 | $267.23 | -0.98 | — | +0.00 | -0.98 | +1.47 | — |
| 2026-08-27 | `TRLV` | 171 | $11.43 | $11.38 | -8.55 | — | +0.00 | -8.55 | +27.36 | — |
| 2026-08-27 | `CAPR` | 231 | $9.36 | $9.19 | -39.27 | — | +0.00 | -39.27 | +207.90 | — |
| 2026-08-27 | `FWRD` | 110 | $17.63 | $17.60 | -3.30 | — | +0.00 | -3.30 | +20.90 | — |
| 2026-08-27 | `FLNC` | 172 | $11.08 | $11.52 | +75.68 | — | +0.00 | +75.68 | +68.80 | — |
| 2026-08-28 | `SMTC` | 8 | — | $141.76 | +0.00 | $131.17 | -84.72 | -84.72 | +0.00 | -84.72 |
| 2026-08-28 | `SEDG` | 37 | — | $32.90 | +0.00 | $31.41 | -55.13 | -55.13 | +0.00 | -55.13 |
| 2026-08-28 | `TLS` | 256 | — | $4.82 | +0.00 | $4.79 | -7.68 | -7.68 | +0.00 | -7.68 |
| 2026-08-28 | `TH` | 65 | — | $19.00 | +0.00 | $18.55 | -29.25 | -29.25 | +0.00 | -29.25 |
| 2026-08-28 | `OPTX` | 143 | — | $8.61 | +0.00 | $8.52 | -12.87 | -12.87 | +0.00 | -12.87 |
| 2026-08-28 | `BBWI` | 65 | — | $18.75 | +0.00 | $19.22 | +30.55 | +30.55 | +0.00 | +30.55 |
| 2026-08-28 | `CAPR` | 127 | — | $9.73 | +0.00 | $9.59 | -17.78 | -17.78 | +0.00 | -17.78 |
| 2026-08-28 | `ERAS` | 64 | — | $19.25 | +0.00 | $18.03 | -78.08 | -78.08 | +0.00 | -78.08 |
| 2026-08-31 | `SMTC` | 8 | $131.17 | $132.30 | +9.04 | — | +0.00 | +9.04 | -75.68 | — |
| 2026-08-31 | `SEDG` | 37 | $31.41 | $31.15 | -9.62 | — | +0.00 | -9.62 | -64.75 | — |
| 2026-08-31 | `TLS` | 256 | $4.79 | $4.81 | +5.12 | — | +0.00 | +5.12 | -2.56 | — |
| 2026-08-31 | `TH` | 65 | $18.55 | $18.12 | -27.63 | $18.52 | +25.67 | -1.96 | -56.88 | -31.20 |
| 2026-08-31 | `OPTX` | 143 | $8.52 | $8.52 | +0.00 | — | +0.00 | +0.00 | -12.87 | — |
| 2026-08-31 | `BBWI` | 65 | $19.22 | $19.25 | +1.95 | — | +0.00 | +1.95 | +32.50 | — |
| 2026-08-31 | `CAPR` | 127 | $9.59 | $9.50 | -11.43 | — | +0.00 | -11.43 | -29.21 | — |
| 2026-08-31 | `ERAS` | 64 | $18.03 | $17.87 | -10.24 | — | +0.00 | -10.24 | -88.32 | — |
| 2026-09-01 | `TH` | 65 | $18.52 | $18.45 | -4.55 | — | +0.00 | -4.55 | -35.75 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CXW` | 49 | — | $32.31 | +0.00 | $33.66 | +66.15 | +66.15 | +0.00 | +66.15 |
| 2026-09-03 | `FRNM` | 100 | — | $15.87 | +0.00 | $16.90 | +103.00 | +103.00 | +0.00 | +103.00 |
| 2026-09-03 | `MMED` | 66 | — | $23.88 | +0.00 | $23.84 | -2.64 | -2.64 | +0.00 | -2.64 |
| 2026-09-03 | `DE` | 2 | — | $703.25 | +0.00 | $694.41 | -17.68 | -17.68 | +0.00 | -17.68 |
| 2026-09-03 | `CNXC` | 48 | — | $32.88 | +0.00 | $32.85 | -1.44 | -1.44 | +0.00 | -1.44 |
| 2026-09-03 | `OPTX` | 210 | — | $7.59 | +0.00 | $7.76 | +35.70 | +35.70 | +0.00 | +35.70 |
| 2026-09-04 | `CXW` | 49 | $33.66 | $33.46 | -9.80 | — | +0.00 | -9.80 | +56.35 | — |
| 2026-09-04 | `FRNM` | 100 | $16.90 | $16.40 | -50.00 | $16.31 | -9.00 | -59.00 | +53.00 | +44.00 |
| 2026-09-04 | `MMED` | 66 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.64 | — |
| 2026-09-04 | `DE` | 2 | $694.41 | $692.03 | -4.76 | — | +0.00 | -4.76 | -22.44 | — |
| 2026-09-04 | `CNXC` | 48 | $32.85 | $32.48 | -17.76 | — | +0.00 | -17.76 | -19.20 | — |
| 2026-09-04 | `OPTX` | 210 | $7.76 | $7.79 | +6.30 | — | +0.00 | +6.30 | +42.00 | — |
| 2026-09-04 | `MRX` | 53 | — | $75.65 | +0.00 | $78.27 | +138.86 | +138.86 | +0.00 | +138.86 |
| 2026-09-04 | `BAK` | 2056 | — | $1.94 | +0.00 | $1.89 | -102.80 | -102.80 | +0.00 | -102.80 |
| 2026-09-08 | `FRNM` | 100 | $16.31 | $16.74 | +43.00 | — | +0.00 | +43.00 | +87.00 | — |
| 2026-09-08 | `MRX` | 53 | $78.27 | $78.84 | +30.21 | $76.71 | -112.89 | -82.68 | +169.07 | +56.18 |
| 2026-09-08 | `BAK` | 2056 | $1.89 | $1.94 | +102.80 | — | +0.00 | +102.80 | +0.00 | — |
| 2026-09-09 | `MRX` | 53 | $76.71 | $76.60 | -5.83 | — | +0.00 | -5.83 | +50.35 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 11 | — | $164.43 | +0.00 | $150.28 | -155.65 | -155.65 | +0.00 | -155.65 |
| 2026-09-11 | `ADBE` | 8 | — | $242.17 | +0.00 | $252.23 | +80.48 | +80.48 | +0.00 | +80.48 |
| 2026-09-11 | `BAK` | 914 | — | $2.12 | +0.00 | $2.08 | -36.56 | -36.56 | +0.00 | -36.56 |
| 2026-09-11 | `AMTX` | 950 | — | $2.04 | +0.00 | $2.01 | -28.50 | -28.50 | +0.00 | -28.50 |
| 2026-09-11 | `RH` | 14 | — | $135.71 | +0.00 | $134.07 | -22.96 | -22.96 | +0.00 | -22.96 |
| 2026-09-14 | `ORCL` | 11 | $150.28 | $141.42 | -97.46 | — | +0.00 | -97.46 | -253.11 | — |
| 2026-09-14 | `ADBE` | 8 | $252.23 | $261.51 | +74.24 | — | +0.00 | +74.24 | +154.72 | — |
| 2026-09-14 | `BAK` | 914 | $2.08 | $2.05 | -27.42 | — | +0.00 | -27.42 | -63.98 | — |
| 2026-09-14 | `AMTX` | 950 | $2.01 | $2.01 | +0.00 | — | +0.00 | +0.00 | -28.50 | — |
| 2026-09-14 | `RH` | 14 | $134.07 | $131.40 | -37.38 | — | +0.00 | -37.38 | -60.34 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +3.49 | ANGX, ARX, HLIT, MH, VELO, FWRD, S | — | $1,285.76 | $9,986.48 | ANGX×290, ARX×63, HLIT×94, MH×92, VELO×81, FWRD×62, S×52 |
| 2026-08-17 | +2.25 | $1,285.76 | ANGX×290, ARX×63, HLIT×94, MH×92, VELO×81, FWRD×62, S×52 | $10,024.80 | +38.32 | -117.89 | OUST, CELC | ANGX, ARX, HLIT, MH, VELO, FWRD, S | $76.67 | $9,885.25 | OUST×102, CELC×53 |
| 2026-08-18 | -6.20 | $76.67 | OUST×102, CELC×53 | $9,571.99 | -313.26 | +0.00 | — | OUST, CELC | $9,567.45 | $9,567.45 | — |
| 2026-08-19 | -7.20 | $9,567.45 | — | $9,567.45 | -0.00 | +0.00 | — | — | $9,567.45 | $9,567.45 | — |
| 2026-08-20 | +1.12 | $9,567.45 | — | $9,567.45 | -0.00 | -153.76 | BHP, AUTL, CRSP, ASST, MRNA, ZLAB, TEAM, HUMA | — | $308.96 | $9,377.98 | BHP×13, AUTL×484, CRSP×20, ASST×74, MRNA×7, ZLAB×45, TEAM×6, HUMA×1691 |
| 2026-08-21 | +3.25 | $308.96 | BHP×13, AUTL×484, CRSP×20, ASST×74, MRNA×7, ZLAB×45, TEAM×6, HUMA×1691 | $9,548.12 | +170.14 | -130.83 | FUTU, GRAL, ABTC, HIVE, MARA, BTDR | BHP, ASST, MRNA, ZLAB, TEAM, HUMA | $39.08 | $9,374.26 | AUTL×484, CRSP×20, FUTU×10, GRAL×15, ABTC×137, HIVE×366, MARA×101, BTDR×107 |
| 2026-08-24 | -5.17 | $39.08 | AUTL×484, CRSP×20, FUTU×10, GRAL×15, ABTC×137, HIVE×366, MARA×101, BTDR×107 | $9,360.60 | -13.66 | -33.50 | — | AUTL, FUTU, GRAL, ABTC, HIVE, MARA, BTDR | $8,163.29 | $9,304.79 | CRSP×20 |
| 2026-08-25 | +1.80 | $8,163.29 | CRSP×20 | $9,321.89 | +17.10 | +467.10 | EZPW, RUM, ZYME, REAX, EOLS | CRSP | $33.68 | $9,775.04 | EZPW×53, RUM×197, ZYME×64, REAX×77, EOLS×213 |
| 2026-08-26 | +2.02 | $33.68 | EZPW×53, RUM×197, ZYME×64, REAX×77, EOLS×213 | $9,609.56 | -165.48 | +302.85 | FNV, TRLV, CAPR, FWRD, FLNC | EZPW, RUM, ZYME, REAX, EOLS | $54.69 | $9,888.03 | FNV×7, TRLV×171, CAPR×231, FWRD×110, FLNC×172 |
| 2026-08-27 | — | $54.69 | FNV×7, TRLV×171, CAPR×231, FWRD×110, FLNC×172 | $9,911.61 | +23.58 | +0.00 | — | FNV, TRLV, CAPR, FWRD, FLNC | $9,899.09 | $9,899.09 | — |
| 2026-08-28 | +0.75 | $9,899.09 | — | $9,899.09 | -0.00 | -254.96 | SMTC, SEDG, TLS, TH, OPTX, BBWI, CAPR, ERAS | — | $142.34 | $9,625.37 | SMTC×8, SEDG×37, TLS×256, TH×65, OPTX×143, BBWI×65, CAPR×127, ERAS×64 |
| 2026-08-31 | -5.85 | $142.34 | SMTC×8, SEDG×37, TLS×256, TH×65, OPTX×143, BBWI×65, CAPR×127, ERAS×64 | $9,582.56 | -42.81 | +25.67 | — | SMTC, SEDG, TLS, OPTX, BBWI, CAPR, ERAS | $8,387.66 | $9,591.46 | TH×65 |
| 2026-09-01 | -6.30 | $8,387.66 | TH×65 | $9,586.91 | -4.55 | +0.00 | — | TH | $9,584.71 | $9,584.71 | — |
| 2026-09-02 | -3.83 | $9,584.71 | — | $9,584.71 | -0.00 | +0.00 | — | — | $9,584.71 | $9,584.71 | — |
| 2026-09-03 | -0.90 | $9,584.71 | — | $9,584.71 | -0.00 | +183.09 | CXW, FRNM, MMED, DE, CNXC, OPTX | — | $246.34 | $9,754.34 | CXW×49, FRNM×100, MMED×66, DE×2, CNXC×48, OPTX×210 |
| 2026-09-04 | +2.25 | $246.34 | CXW×49, FRNM×100, MMED×66, DE×2, CNXC×48, OPTX×210 | $9,678.32 | -76.02 | +27.06 | MRX, BAK | CXW, MMED, DE, CNXC, OPTX | $0.26 | $9,665.41 | FRNM×100, MRX×53, BAK×2056 |
| 2026-09-08 | -11.47 | $0.26 | FRNM×100, MRX×53, BAK×2056 | $9,841.42 | +176.01 | -112.89 | — | FRNM, BAK | $5,633.68 | $9,699.31 | MRX×53 |
| 2026-09-09 | -13.95 | $5,633.68 | MRX×53 | $9,693.48 | -5.83 | +0.00 | — | MRX | $9,691.29 | $9,691.29 | — |
| 2026-09-10 | -13.28 | $9,691.29 | — | $9,691.29 | +0.00 | +0.00 | — | — | $9,691.29 | $9,691.29 | — |
| 2026-09-11 | +0.50 | $9,691.29 | — | $9,691.29 | +0.00 | -163.19 | ORCL, ADBE, BAK, AMTX, RH | — | $139.47 | $9,497.99 | ORCL×11, ADBE×8, BAK×914, AMTX×950, RH×14 |
| 2026-09-14 | -11.00 | $139.47 | ORCL×11, ADBE×8, BAK×914, AMTX×950, RH×14 | $9,409.97 | -88.02 | +0.00 | — | ORCL, ADBE, BAK, AMTX, RH | $9,379.44 | $9,379.44 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $8,746.36 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $7,511.27 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $6,270.08 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $3,773.20 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FWRD` | 62 | $20.11 | $2.18 | — | $2,524.20 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+13.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $1,285.76 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,285.76 | ▲ close $9,986.48 vs 09:30 $10,000.00 (session +3.49) | 16:00 close · cash $1,285.76 · equity $9,986.48 vs 09:30 $10,000.00 (-13.52; session marks +3.49) · 7 name(s) marked open→close (per-name table). ANGX×290 09:30 $4.31 → close $4.37 +17.40; ARX×63 09:30 $19.57 → close $19.58 +0.63; HLIT×94 09:30 $13.18 → close $13.92 +69.56; MH×92 09:30 $13.55 → close $13.10 -41.40; VELO×81 09:30 $15.38 → close $16.16 +63.18; FWRD×62 09:30 $20.11 → close $18.96 -71.30; S×52 09:30 $23.77 → close $23.11 -34.58 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,285.76 | ▲ 09:30 equity $10,024.80 vs yday $9,986.48 (+38.32) | 09:30 open · cash $1,285.76 (unchanged overnight, no fees) · equity $10,024.80 vs prior close $9,986.48 (+38.32) · 7 name(s) re-marked at the open (per-name table). ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; HLIT×94 yday $13.92 → 09:30 $13.84 -7.52; MH×92 yday $13.10 → 09:30 $13.16 +5.52; VELO×81 yday $16.16 → 09:30 $16.05 -8.91; FWRD×62 yday $18.96 → 09:30 $19.20 +14.88; S×52 yday $23.11 → 09:30 $22.50 -31.72 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $2,615.96 | ▲ +76.56 after sell → book $10,021.00; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $3,846.67 | ▼ -4.38 after sell → book $10,018.80; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $5,145.33 | ▲ +57.47 after sell → book $10,016.50; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $6,353.76 | ▼ -40.44 after sell → book $10,014.21; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 81 | $16.05 | $2.26 | $+49.78 | $7,651.55 | ▲ +49.78 after sell → book $10,011.95; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 judge🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `FWRD` | 62 | $19.20 | $2.20 | $-60.79 | $8,839.76 | ▼ -60.79 after sell → book $10,009.76; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `S` | 52 | $22.50 | $2.17 | $-70.61 | $10,007.59 | ▼ -70.61 after sell → book $10,007.59; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 102 | $49.00 | $2.30 | — | $5,007.29 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $5003.79 | join🟡 sector🟢 gen🟢 news🟢 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 53 | $92.99 | $2.15 | — | $76.67 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $5003.79 | join🟡 sector🔴 gen🟢 news🟢 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.67 | ▼ close $9,885.25 vs 09:30 $10,024.80 (session -117.89) | 16:00 close · cash $76.67 · equity $9,885.25 vs 09:30 $10,024.80 (-139.55; session marks -117.89) · 2 name(s) marked open→close (per-name table). OUST×102 09:30 $49.00 → close $48.13 -88.74; CELC×53 09:30 $92.99 → close $92.44 -29.15 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.67 | ▼ 09:30 equity $9,571.99 vs yday $9,885.25 (-313.26) | 09:30 open · cash $76.67 (unchanged overnight, no fees) · equity $9,571.99 vs prior close $9,885.25 (-313.26) · 2 name(s) re-marked at the open (per-name table). OUST×102 yday $48.13 → 09:30 $45.09 -310.08; CELC×53 yday $92.44 → 09:30 $92.38 -3.18 | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 102 | $45.09 | $2.35 | $-403.47 | $4,673.50 | ▼ -403.47 after sell → book $9,569.64; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 53 | $92.38 | $2.20 | $-36.68 | $9,567.45 | ▼ -36.68 after sell → book $9,567.45; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,567.45 | ▲ close $9,567.45 vs 09:30 $9,571.99 (session +0.00) | 16:00 close · cash $9,567.45 · no lots left · equity $9,567.45. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,567.45 | ▲ 09:30 equity $9,567.45 vs yday $9,567.45 (-0.00) | 09:30 open · cash $9,567.45 · no holdings · equity $9,567.45 vs prior close $9,567.45 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,567.45 | ▲ close $9,567.45 vs 09:30 $9,567.45 (session +0.00) | 16:00 close · cash $9,567.45 · no lots left · equity $9,567.45. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,567.45 | ▲ 09:30 equity $9,567.45 vs yday $9,567.45 (-0.00) | 09:30 open · cash $9,567.45 · no holdings · equity $9,567.45 vs prior close $9,567.45 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,382.29 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1195.93 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 484 | $2.47 | $6.24 | — | $7,180.56 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1195.93 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 20 | $58.73 | $2.05 | — | $6,003.91 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1195.93 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 74 | $16.00 | $2.21 | — | $4,817.70 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1195.93 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $3,764.71 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1195.93 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 45 | $26.57 | $2.12 | — | $2,566.94 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1195.93 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 6 | $173.90 | $2.01 | — | $1,521.53 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1195.93 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1691 | $0.71 | $17.03 | — | $308.96 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1195.93 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $308.96 | ▼ close $9,377.98 vs 09:30 $9,567.45 (session -153.76) | 16:00 close · cash $308.96 · equity $9,377.98 vs 09:30 $9,567.45 (-189.47; session marks -153.76) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; AUTL×484 09:30 $2.47 → close $2.46 -4.84; CRSP×20 09:30 $58.73 → close $58.12 -12.20; ASST×74 09:30 $16.00 → close $16.13 +9.62; MRNA×7 09:30 $150.14 → close $133.32 -117.74; ZLAB×45 09:30 $26.57 → close $26.02 -24.75; TEAM×6 09:30 $173.90 → close $174.91 +6.06; HUMA×1691 09:30 $0.71 → close $0.68 -43.97 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $308.96 | ▲ 09:30 equity $9,548.12 vs yday $9,377.98 (+170.14) | 09:30 open · cash $308.96 (unchanged overnight, no fees) · equity $9,548.12 vs prior close $9,377.98 (+170.14) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; AUTL×484 yday $2.46 → 09:30 $2.47 +4.84; CRSP×20 yday $58.12 → 09:30 $59.72 +32.00; ASST×74 yday $16.13 → 09:30 $17.66 +113.22; MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; ZLAB×45 yday $26.02 → 09:30 $26.25 +10.35; TEAM×6 yday $174.91 → 09:30 $174.22 -4.14; HUMA×1691 yday $0.68 → 09:30 $0.67 -11.84 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,551.27 | ▲ +57.15 after sell → book $9,546.07; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 74 | $17.66 | $2.23 | $+118.39 | $2,855.88 | ▲ +118.39 after sell → book $9,543.83; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 7 | $133.11 | $2.03 | $-123.25 | $3,785.62 | ▼ -123.25 after sell → book $9,541.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 45 | $26.25 | $2.15 | $-18.67 | $4,964.72 | ▼ -18.67 after sell → book $9,539.66; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TEAM` | 6 | $174.22 | $2.03 | $-2.12 | $6,008.01 | ▼ -2.12 after sell → book $9,537.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1691 | $0.67 | $16.76 | $-89.59 | $7,130.99 | ▼ -89.59 after sell → book $9,520.87; vs 09:30 mark -16.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $5,977.17 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1188.50 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 15 | $78.88 | $2.04 | — | $4,791.93 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1188.50 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 137 | $8.66 | $2.40 | — | $3,603.11 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1188.50 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 366 | $3.24 | $4.72 | — | $2,412.55 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1188.50 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 101 | $11.70 | $2.29 | — | $1,228.56 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1188.50 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 107 | $11.10 | $2.31 | — | $39.08 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=+19.1; leftover $1188.50 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.08 | ▼ close $9,374.26 vs 09:30 $9,548.12 (session -130.83) | 16:00 close · cash $39.08 · equity $9,374.26 vs 09:30 $9,548.12 (-173.86; session marks -130.83) · 8 name(s) marked open→close (per-name table). AUTL×484 09:30 $2.47 → close $2.41 -29.04; CRSP×20 09:30 $59.72 → close $59.50 -4.40; FUTU×10 09:30 $115.18 → close $123.64 +84.60; GRAL×15 09:30 $78.88 → close $79.54 +9.90; ABTC×137 09:30 $8.66 → close $7.93 -100.01; HIVE×366 09:30 $3.24 → close $3.03 -76.86; MARA×101 09:30 $11.70 → close $11.26 -44.44; BTDR×107 09:30 $11.10 → close $11.37 +29.42 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.08 | ▼ 09:30 equity $9,360.60 vs yday $9,374.26 (-13.66) | 09:30 open · cash $39.08 (unchanged overnight, no fees) · equity $9,360.60 vs prior close $9,374.26 (-13.66) · 8 name(s) re-marked at the open (per-name table). AUTL×484 yday $2.41 → 09:30 $2.40 -4.84; CRSP×20 yday $59.50 → 09:30 $58.75 -15.00; FUTU×10 yday $123.64 → 09:30 $121.00 -26.40; GRAL×15 yday $79.54 → 09:30 $81.87 +34.95; ABTC×137 yday $7.93 → 09:30 $8.00 +9.59; HIVE×366 yday $3.03 → 09:30 $2.99 -14.64; MARA×101 yday $11.26 → 09:30 $11.17 -9.09; BTDR×107 yday $11.37 → 09:30 $11.48 +11.77 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 484 | $2.40 | $6.33 | $-46.46 | $1,194.35 | ▼ -46.46 after sell → book $9,354.27; vs 09:30 mark -6.33 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $2,402.31 | ▲ +54.14 after sell → book $9,352.23; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 15 | $81.87 | $2.06 | $+40.76 | $3,628.30 | ▲ +40.76 after sell → book $9,350.17; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 137 | $8.00 | $2.43 | $-95.25 | $4,721.87 | ▼ -95.25 after sell → book $9,347.74; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 366 | $2.99 | $4.79 | $-101.01 | $5,811.42 | ▼ -101.01 after sell → book $9,342.95; vs 09:30 mark -4.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 101 | $11.17 | $2.32 | $-58.14 | $6,937.27 | ▼ -58.14 after sell → book $9,340.63; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 107 | $11.48 | $2.34 | $+36.55 | $8,163.29 | ▲ +36.55 after sell → book $9,338.29; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,163.29 | ▼ close $9,304.79 vs 09:30 $9,360.60 (session -33.50) | 16:00 close · cash $8,163.29 · equity $9,304.79 vs 09:30 $9,360.60 (-55.81; session marks -33.50) · 1 name(s) marked open→close (per-name table). CRSP×20 09:30 $58.75 → close $57.08 -33.50 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,163.29 | ▲ 09:30 equity $9,321.89 vs yday $9,304.79 (+17.10) | 09:30 open · cash $8,163.29 (unchanged overnight, no fees) · equity $9,321.89 vs prior close $9,304.79 (+17.10) · 1 name(s) re-marked at the open (per-name table). CRSP×20 yday $57.08 → 09:30 $57.93 +17.10 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.93 | $2.07 | $-20.12 | $9,319.82 | ▼ -20.12 after sell → book $9,319.82; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 53 | $35.05 | $2.15 | — | $7,460.02 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1863.96 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 197 | $9.42 | $2.58 | — | $5,601.70 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1863.96 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 64 | $28.86 | $2.18 | — | $3,752.48 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1863.96 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 77 | $24.11 | $2.22 | — | $1,893.78 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=+891.7; leftover $1863.96 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 213 | $8.72 | $2.75 | — | $33.68 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+13.0; leftover $1863.96 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.68 | ▲ close $9,775.04 vs 09:30 $9,321.89 (session +467.10) | 16:00 close · cash $33.68 · equity $9,775.04 vs 09:30 $9,321.89 (+453.15; session marks +467.10) · 5 name(s) marked open→close (per-name table). EZPW×53 09:30 $35.05 → close $35.23 +9.54; RUM×197 09:30 $9.42 → close $10.23 +159.57; ZYME×64 09:30 $28.86 → close $27.47 -88.96; REAX×77 09:30 $24.11 → close $28.43 +332.64; EOLS×213 09:30 $8.72 → close $8.97 +54.31 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.68 | ▼ 09:30 equity $9,609.56 vs yday $9,775.04 (-165.48) | 09:30 open · cash $33.68 (unchanged overnight, no fees) · equity $9,609.56 vs prior close $9,775.04 (-165.48) · 5 name(s) re-marked at the open (per-name table). EZPW×53 yday $35.23 → 09:30 $35.70 +24.91; RUM×197 yday $10.23 → 09:30 $10.07 -31.52; ZYME×64 yday $27.47 → 09:30 $27.56 +5.76; REAX×77 yday $28.43 → 09:30 $26.61 -140.14; EOLS×213 yday $8.97 → 09:30 $8.86 -24.50 | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 53 | $35.70 | $2.17 | $+30.13 | $1,923.60 | ▲ +30.13 after sell → book $9,607.38; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 197 | $10.07 | $2.63 | $+122.84 | $3,904.76 | ▲ +122.84 after sell → book $9,604.75; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 64 | $27.56 | $2.21 | $-87.59 | $5,666.40 | ▼ -87.59 after sell → book $9,602.55; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 77 | $26.61 | $2.25 | $+188.03 | $7,713.12 | ▲ +188.03 after sell → book $9,600.30; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 213 | $8.86 | $2.80 | $+24.27 | $9,597.50 | ▲ +24.27 after sell → book $9,597.50; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 7 | $267.02 | $2.01 | — | $7,726.35 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $1919.50 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 171 | $11.22 | $2.50 | — | $5,805.22 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $1919.50 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 231 | $8.29 | $2.98 | — | $3,887.25 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $1919.50 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 110 | $17.41 | $2.32 | — | $1,969.83 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-9.2; leftover $1919.50 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 172 | $11.12 | $2.51 | — | $54.69 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1919.50 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.69 | ▲ close $9,888.03 vs 09:30 $9,609.56 (session +302.85) | 16:00 close · cash $54.69 · equity $9,888.03 vs 09:30 $9,609.56 (+278.47; session marks +302.85) · 5 name(s) marked open→close (per-name table). FNV×7 09:30 $267.02 → close $267.37 +2.45; TRLV×171 09:30 $11.22 → close $11.43 +35.91; CAPR×231 09:30 $8.29 → close $9.36 +247.17; FWRD×110 09:30 $17.41 → close $17.63 +24.20; FLNC×172 09:30 $11.12 → close $11.08 -6.88 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.69 | ▲ 09:30 equity $9,911.61 vs yday $9,888.03 (+23.58) | 09:30 open · cash $54.69 (unchanged overnight, no fees) · equity $9,911.61 vs prior close $9,888.03 (+23.58) · 5 name(s) re-marked at the open (per-name table). FNV×7 yday $267.37 → 09:30 $267.23 -0.98; TRLV×171 yday $11.43 → 09:30 $11.38 -8.55; CAPR×231 yday $9.36 → 09:30 $9.19 -39.27; FWRD×110 yday $17.63 → 09:30 $17.60 -3.30; FLNC×172 yday $11.08 → 09:30 $11.52 +75.68 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 7 | $267.23 | $2.04 | $-2.58 | $1,923.26 | ▼ -2.58 after sell → book $9,909.57; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 171 | $11.38 | $2.55 | $+22.31 | $3,866.70 | ▲ +22.31 after sell → book $9,907.03; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 231 | $9.19 | $3.04 | $+201.88 | $5,986.55 | ▲ +201.88 after sell → book $9,903.99; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWRD` | 110 | $17.60 | $2.35 | $+16.23 | $7,920.20 | ▲ +16.23 after sell → book $9,901.64; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 172 | $11.52 | $2.55 | $+63.74 | $9,899.09 | ▲ +63.74 after sell → book $9,899.09; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,899.09 | ▲ close $9,899.09 vs 09:30 $9,911.61 (session +0.00) | 16:00 close · cash $9,899.09 · no lots left · equity $9,899.09. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,899.09 | ▲ 09:30 equity $9,899.09 vs yday $9,899.09 (-0.00) | 09:30 open · cash $9,899.09 · no holdings · equity $9,899.09 vs prior close $9,899.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $8,762.99 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1237.39 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 37 | $32.90 | $2.10 | — | $7,543.59 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1237.39 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 256 | $4.82 | $3.30 | — | $6,306.37 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1237.39 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 65 | $19.00 | $2.19 | — | $5,069.18 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+7.5; leftover $1237.39 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 143 | $8.61 | $2.42 | — | $3,835.53 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; ret5=-0.7; leftover $1237.39 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 65 | $18.75 | $2.19 | — | $2,614.60 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=-5.0; leftover $1237.39 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 127 | $9.73 | $2.37 | — | $1,376.52 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; ret5=+47.1; leftover $1237.39 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 64 | $19.25 | $2.18 | — | $142.34 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=+14.1; leftover $1237.39 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.34 | ▼ close $9,625.37 vs 09:30 $9,899.09 (session -254.96) | 16:00 close · cash $142.34 · equity $9,625.37 vs 09:30 $9,899.09 (-273.72; session marks -254.96) · 8 name(s) marked open→close (per-name table). SMTC×8 09:30 $141.76 → close $131.17 -84.72; SEDG×37 09:30 $32.90 → close $31.41 -55.13; TLS×256 09:30 $4.82 → close $4.79 -7.68; TH×65 09:30 $19.00 → close $18.55 -29.25; OPTX×143 09:30 $8.61 → close $8.52 -12.87; BBWI×65 09:30 $18.75 → close $19.22 +30.55; CAPR×127 09:30 $9.73 → close $9.59 -17.78; ERAS×64 09:30 $19.25 → close $18.03 -78.08 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.34 | ▼ 09:30 equity $9,582.56 vs yday $9,625.37 (-42.81) | 09:30 open · cash $142.34 (unchanged overnight, no fees) · equity $9,582.56 vs prior close $9,625.37 (-42.81) · 8 name(s) re-marked at the open (per-name table). SMTC×8 yday $131.17 → 09:30 $132.30 +9.04; SEDG×37 yday $31.41 → 09:30 $31.15 -9.62; TLS×256 yday $4.79 → 09:30 $4.81 +5.12; TH×65 yday $18.55 → 09:30 $18.12 -27.63; OPTX×143 yday $8.52 → 09:30 $8.52 +0.00; BBWI×65 yday $19.22 → 09:30 $19.25 +1.95; CAPR×127 yday $9.59 → 09:30 $9.50 -11.43; ERAS×64 yday $18.03 → 09:30 $17.87 -10.24 | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $132.30 | $2.03 | $-79.73 | $1,198.70 | ▼ -79.73 after sell → book $9,580.53; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 37 | $31.15 | $2.12 | $-68.97 | $2,349.13 | ▼ -68.97 after sell → book $9,578.41; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 256 | $4.81 | $3.35 | $-9.22 | $3,577.14 | ▼ -9.22 after sell → book $9,575.05; vs 09:30 mark -3.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 143 | $8.52 | $2.45 | $-17.74 | $4,793.04 | ▼ -17.74 after sell → book $9,572.60; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 65 | $19.25 | $2.21 | $+28.11 | $6,042.09 | ▲ +28.11 after sell → book $9,570.39; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 127 | $9.50 | $2.40 | $-33.98 | $7,246.19 | ▼ -33.98 after sell → book $9,567.99; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 64 | $17.87 | $2.20 | $-92.70 | $8,387.66 | ▼ -92.70 after sell → book $9,565.79; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,387.66 | ▲ close $9,591.46 vs 09:30 $9,582.56 (session +25.67) | 16:00 close · cash $8,387.66 · equity $9,591.46 vs 09:30 $9,582.56 (+8.90; session marks +25.67) · 1 name(s) marked open→close (per-name table). TH×65 09:30 $18.12 → close $18.52 +25.67 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,387.66 | ▼ 09:30 equity $9,586.91 vs yday $9,591.46 (-4.55) | 09:30 open · cash $8,387.66 (unchanged overnight, no fees) · equity $9,586.91 vs prior close $9,591.46 (-4.55) · 1 name(s) re-marked at the open (per-name table). TH×65 yday $18.52 → 09:30 $18.45 -4.55 | — |
| 2026-09-01 09:30 ET | **SELL** | `TH` | 65 | $18.45 | $2.21 | $-40.14 | $9,584.71 | ▼ -40.14 after sell → book $9,584.71; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,584.71 | ▲ close $9,584.71 vs 09:30 $9,586.91 (session +0.00) | 16:00 close · cash $9,584.71 · no lots left · equity $9,584.71. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,584.71 | ▲ 09:30 equity $9,584.71 vs yday $9,584.71 (-0.00) | 09:30 open · cash $9,584.71 · no holdings · equity $9,584.71 vs prior close $9,584.71 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,584.71 | ▲ close $9,584.71 vs 09:30 $9,584.71 (session +0.00) | 16:00 close · cash $9,584.71 · no lots left · equity $9,584.71. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,584.71 | ▲ 09:30 equity $9,584.71 vs yday $9,584.71 (-0.00) | 09:30 open · cash $9,584.71 · no holdings · equity $9,584.71 vs prior close $9,584.71 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 49 | $32.31 | $2.14 | — | $7,999.38 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1597.45 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 100 | $15.87 | $2.29 | — | $6,410.09 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1597.45 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 66 | $23.88 | $2.19 | — | $4,831.82 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1597.45 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $3,423.33 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1597.45 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 48 | $32.88 | $2.13 | — | $1,842.95 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1597.45 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 210 | $7.59 | $2.71 | — | $246.34 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-11.5; leftover $1597.45 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $246.34 | ▲ close $9,754.34 vs 09:30 $9,584.71 (session +183.09) | 16:00 close · cash $246.34 · equity $9,754.34 vs 09:30 $9,584.71 (+169.63; session marks +183.09) · 6 name(s) marked open→close (per-name table). CXW×49 09:30 $32.31 → close $33.66 +66.15; FRNM×100 09:30 $15.87 → close $16.90 +103.00; MMED×66 09:30 $23.88 → close $23.84 -2.64; DE×2 09:30 $703.25 → close $694.41 -17.68; CNXC×48 09:30 $32.88 → close $32.85 -1.44; OPTX×210 09:30 $7.59 → close $7.76 +35.70 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $246.34 | ▼ 09:30 equity $9,678.32 vs yday $9,754.34 (-76.02) | 09:30 open · cash $246.34 (unchanged overnight, no fees) · equity $9,678.32 vs prior close $9,754.34 (-76.02) · 6 name(s) re-marked at the open (per-name table). CXW×49 yday $33.66 → 09:30 $33.46 -9.80; FRNM×100 yday $16.90 → 09:30 $16.40 -50.00; MMED×66 yday $23.84 → 09:30 $23.84 +0.00; DE×2 yday $694.41 → 09:30 $692.03 -4.76; CNXC×48 yday $32.85 → 09:30 $32.48 -17.76; OPTX×210 yday $7.76 → 09:30 $7.79 +6.30 | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 49 | $33.46 | $2.16 | $+52.05 | $1,883.72 | ▲ +52.05 after sell → book $9,676.16; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 66 | $23.84 | $2.21 | $-7.04 | $3,454.95 | ▼ -7.04 after sell → book $9,673.95; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 2 | $692.03 | $2.02 | $-26.45 | $4,837.00 | ▼ -26.45 after sell → book $9,671.94; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 48 | $32.48 | $2.16 | $-23.49 | $6,393.88 | ▼ -23.49 after sell → book $9,669.78; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 210 | $7.79 | $2.76 | $+36.53 | $8,027.02 | ▲ +36.53 after sell → book $9,667.02; vs 09:30 mark -2.76 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 53 | $75.65 | $2.15 | — | $4,015.42 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $4013.51 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 2056 | $1.94 | $26.52 | — | $0.26 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $4013.51 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.26 | ▲ close $9,665.41 vs 09:30 $9,678.32 (session +27.06) | 16:00 close · cash $0.26 · equity $9,665.41 vs 09:30 $9,678.32 (-12.91; session marks +27.06) · 3 name(s) marked open→close (per-name table). FRNM×100 09:30 $16.40 → close $16.31 -9.00; MRX×53 09:30 $75.65 → close $78.27 +138.86; BAK×2056 09:30 $1.94 → close $1.89 -102.80 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.26 | ▲ 09:30 equity $9,841.42 vs yday $9,665.41 (+176.01) | 09:30 open · cash $0.26 (unchanged overnight, no fees) · equity $9,841.42 vs prior close $9,665.41 (+176.01) · 3 name(s) re-marked at the open (per-name table). FRNM×100 yday $16.31 → 09:30 $16.74 +43.00; MRX×53 yday $78.27 → 09:30 $78.84 +30.21; BAK×2056 yday $1.89 → 09:30 $1.94 +102.80 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 100 | $16.74 | $2.32 | $+82.39 | $1,671.94 | ▲ +82.39 after sell → book $9,839.10; vs 09:30 mark -2.32 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 2056 | $1.94 | $26.90 | $-53.42 | $5,633.68 | ▼ -53.42 after sell → book $9,812.20; vs 09:30 mark -26.90 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,633.68 | ▼ close $9,699.31 vs 09:30 $9,841.42 (session -112.89) | 16:00 close · cash $5,633.68 · equity $9,699.31 vs 09:30 $9,841.42 (-142.11; session marks -112.89) · 1 name(s) marked open→close (per-name table). MRX×53 09:30 $78.84 → close $76.71 -112.89 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,633.68 | ▼ 09:30 equity $9,693.48 vs yday $9,699.31 (-5.83) | 09:30 open · cash $5,633.68 (unchanged overnight, no fees) · equity $9,693.48 vs prior close $9,699.31 (-5.83) · 1 name(s) re-marked at the open (per-name table). MRX×53 yday $76.71 → 09:30 $76.60 -5.83 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 53 | $76.60 | $2.19 | $+46.01 | $9,691.29 | ▲ +46.01 after sell → book $9,691.29; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,691.29 | ▲ close $9,691.29 vs 09:30 $9,693.48 (session +0.00) | 16:00 close · cash $9,691.29 · no lots left · equity $9,691.29. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,691.29 | ▲ 09:30 equity $9,691.29 vs yday $9,691.29 (+0.00) | 09:30 open · cash $9,691.29 · no holdings · equity $9,691.29 vs prior close $9,691.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,691.29 | ▲ close $9,691.29 vs 09:30 $9,691.29 (session +0.00) | 16:00 close · cash $9,691.29 · no lots left · equity $9,691.29. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,691.29 | ▲ 09:30 equity $9,691.29 vs yday $9,691.29 (+0.00) | 09:30 open · cash $9,691.29 · no holdings · equity $9,691.29 vs prior close $9,691.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 11 | $164.43 | $2.02 | — | $7,880.54 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1938.26 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 8 | $242.17 | $2.01 | — | $5,941.17 | — | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react; ret5=-11.1; leftover $1938.26 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 914 | $2.12 | $11.79 | — | $3,991.70 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1938.26 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 950 | $2.04 | $12.26 | — | $2,041.44 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1938.26 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 14 | $135.71 | $2.03 | — | $139.47 | — | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react; ret5=-9.2; leftover $1938.26 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.47 | ▼ close $9,497.99 vs 09:30 $9,691.29 (session -163.19) | 16:00 close · cash $139.47 · equity $9,497.99 vs 09:30 $9,691.29 (-193.30; session marks -163.19) · 5 name(s) marked open→close (per-name table). ORCL×11 09:30 $164.43 → close $150.28 -155.65; ADBE×8 09:30 $242.17 → close $252.23 +80.48; BAK×914 09:30 $2.12 → close $2.08 -36.56; AMTX×950 09:30 $2.04 → close $2.01 -28.50; RH×14 09:30 $135.71 → close $134.07 -22.96 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.47 | ▼ 09:30 equity $9,409.97 vs yday $9,497.99 (-88.02) | 09:30 open · cash $139.47 (unchanged overnight, no fees) · equity $9,409.97 vs prior close $9,497.99 (-88.02) · 5 name(s) re-marked at the open (per-name table). ORCL×11 yday $150.28 → 09:30 $141.42 -97.46; ADBE×8 yday $252.23 → 09:30 $261.51 +74.24; BAK×914 yday $2.08 → 09:30 $2.05 -27.42; AMTX×950 yday $2.01 → 09:30 $2.01 +0.00; RH×14 yday $134.07 → 09:30 $131.40 -37.38 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 11 | $141.42 | $2.05 | $-257.18 | $1,693.04 | ▼ -257.18 after sell → book $9,407.92; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 8 | $261.51 | $2.04 | $+150.67 | $3,783.08 | ▲ +150.67 after sell → book $9,405.88; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 914 | $2.05 | $11.96 | $-87.73 | $5,644.83 | ▼ -87.73 after sell → book $9,393.93; vs 09:30 mark -11.95 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 950 | $2.01 | $12.43 | $-53.18 | $7,541.90 | ▼ -53.18 after sell → book $9,381.50; vs 09:30 mark -12.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 14 | $131.40 | $2.06 | $-64.43 | $9,379.44 | ▼ -64.43 after sell → book $9,379.44; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,379.44 | ▲ close $9,379.44 vs 09:30 $9,409.97 (session +0.00) | 16:00 close · cash $9,379.44 · no lots left · equity $9,379.44. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WLTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TXG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
