# Factor mine action — `union_news_head_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · prior-export headline🟢 only

Cash book **-7.85%** ($9,215) · signal-only (no cash/fees) was +12.99%. Starts YES **0/26**. Fills 124 · skips 51 · realized $-583.43.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $18.59.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 94 | — | $13.18 | +0.00 | $13.92 | +69.56 | +69.56 | +0.00 | +69.56 |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `MH` | 92 | — | $13.55 | +0.00 | $13.10 | -41.40 | -41.40 | +0.00 | -41.40 |
| 2026-08-14 | `VELO` | 81 | — | $15.38 | +0.00 | $16.16 | +63.18 | +63.18 | +0.00 | +63.18 |
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `S` | 52 | — | $23.77 | +0.00 | $23.11 | -34.58 | -34.58 | +0.00 | -34.58 |
| 2026-08-14 | `ZS` | 6 | — | $190.00 | +0.00 | $183.60 | -38.40 | -38.40 | +0.00 | -38.40 |
| 2026-08-17 | `HLIT` | 94 | $13.92 | $13.84 | -7.52 | — | +0.00 | -7.52 | +62.04 | — |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `MH` | 92 | $13.10 | $13.16 | +5.52 | — | +0.00 | +5.52 | -35.88 | — |
| 2026-08-17 | `VELO` | 81 | $16.16 | $16.05 | -8.91 | — | +0.00 | -8.91 | +54.27 | — |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `S` | 52 | $23.11 | $22.50 | -31.72 | — | +0.00 | -31.72 | -66.30 | — |
| 2026-08-17 | `ZS` | 6 | $183.60 | $188.38 | +28.65 | — | +0.00 | +28.65 | -9.75 | — |
| 2026-08-17 | `OUST` | 102 | — | $49.00 | +0.00 | $48.13 | -88.74 | -88.74 | +0.00 | -88.74 |
| 2026-08-17 | `CELC` | 54 | — | $92.99 | +0.00 | $92.44 | -29.70 | -29.70 | +0.00 | -29.70 |
| 2026-08-18 | `OUST` | 102 | $48.13 | $45.09 | -310.08 | — | +0.00 | -310.08 | -398.82 | — |
| 2026-08-18 | `CELC` | 54 | $92.44 | $92.38 | -3.24 | — | +0.00 | -3.24 | -32.94 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `AUTL` | 486 | — | $2.47 | +0.00 | $2.46 | -4.86 | -4.86 | +0.00 | -4.86 |
| 2026-08-20 | `CRSP` | 20 | — | $58.73 | +0.00 | $58.12 | -12.20 | -12.20 | +0.00 | -12.20 |
| 2026-08-20 | `ASST` | 75 | — | $16.00 | +0.00 | $16.13 | +9.75 | +9.75 | +0.00 | +9.75 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `ZLAB` | 45 | — | $26.57 | +0.00 | $26.02 | -24.75 | -24.75 | +0.00 | -24.75 |
| 2026-08-20 | `TEAM` | 6 | — | $173.90 | +0.00 | $174.91 | +6.06 | +6.06 | +0.00 | +6.06 |
| 2026-08-20 | `HUMA` | 1699 | — | $0.71 | +0.00 | $0.68 | -44.17 | -44.17 | +0.00 | -44.17 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `AUTL` | 486 | $2.46 | $2.47 | +4.86 | $2.41 | -29.16 | -24.30 | +0.00 | -29.16 |
| 2026-08-21 | `CRSP` | 20 | $58.12 | $59.72 | +32.00 | $59.50 | -4.40 | +27.60 | +19.80 | +15.40 |
| 2026-08-21 | `ASST` | 75 | $16.13 | $17.66 | +114.75 | — | +0.00 | +114.75 | +124.50 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `ZLAB` | 45 | $26.02 | $26.25 | +10.35 | — | +0.00 | +10.35 | -14.40 | — |
| 2026-08-21 | `TEAM` | 6 | $174.91 | $174.22 | -4.14 | — | +0.00 | -4.14 | +1.92 | — |
| 2026-08-21 | `HUMA` | 1699 | $0.68 | $0.67 | -11.89 | — | +0.00 | -11.89 | -56.07 | — |
| 2026-08-21 | `FUTU` | 10 | — | $115.18 | +0.00 | $123.64 | +84.60 | +84.60 | +0.00 | +84.60 |
| 2026-08-21 | `GRAL` | 15 | — | $78.88 | +0.00 | $79.54 | +9.90 | +9.90 | +0.00 | +9.90 |
| 2026-08-21 | `ABTC` | 137 | — | $8.66 | +0.00 | $7.93 | -100.01 | -100.01 | +0.00 | -100.01 |
| 2026-08-21 | `HIVE` | 368 | — | $3.24 | +0.00 | $3.03 | -77.28 | -77.28 | +0.00 | -77.28 |
| 2026-08-21 | `MARA` | 101 | — | $11.70 | +0.00 | $11.26 | -44.44 | -44.44 | +0.00 | -44.44 |
| 2026-08-21 | `BTDR` | 107 | — | $11.10 | +0.00 | $11.37 | +29.42 | +29.42 | +0.00 | +29.42 |
| 2026-08-24 | `AUTL` | 486 | $2.41 | $2.40 | -4.86 | — | +0.00 | -4.86 | -34.02 | — |
| 2026-08-24 | `CRSP` | 20 | $59.50 | $58.75 | -15.00 | $57.08 | -33.50 | -48.50 | +0.40 | -33.10 |
| 2026-08-24 | `FUTU` | 10 | $123.64 | $121.00 | -26.40 | — | +0.00 | -26.40 | +58.20 | — |
| 2026-08-24 | `GRAL` | 15 | $79.54 | $81.87 | +34.95 | — | +0.00 | +34.95 | +44.85 | — |
| 2026-08-24 | `ABTC` | 137 | $7.93 | $8.00 | +9.59 | — | +0.00 | +9.59 | -90.42 | — |
| 2026-08-24 | `HIVE` | 368 | $3.03 | $2.99 | -14.72 | — | +0.00 | -14.72 | -92.00 | — |
| 2026-08-24 | `MARA` | 101 | $11.26 | $11.17 | -9.09 | — | +0.00 | -9.09 | -53.53 | — |
| 2026-08-24 | `BTDR` | 107 | $11.37 | $11.48 | +11.77 | — | +0.00 | +11.77 | +41.19 | — |
| 2026-08-25 | `CRSP` | 20 | $57.08 | $57.93 | +17.10 | — | +0.00 | +17.10 | -16.00 | — |
| 2026-08-25 | `EZPW` | 53 | — | $35.05 | +0.00 | $35.23 | +9.54 | +9.54 | +0.00 | +9.54 |
| 2026-08-25 | `RUM` | 198 | — | $9.42 | +0.00 | $10.23 | +160.38 | +160.38 | +0.00 | +160.38 |
| 2026-08-25 | `ZYME` | 64 | — | $28.86 | +0.00 | $27.47 | -88.96 | -88.96 | +0.00 | -88.96 |
| 2026-08-25 | `REAX` | 77 | — | $24.11 | +0.00 | $28.43 | +332.64 | +332.64 | +0.00 | +332.64 |
| 2026-08-25 | `EOLS` | 214 | — | $8.72 | +0.00 | $8.97 | +54.57 | +54.57 | +0.00 | +54.57 |
| 2026-08-26 | `EZPW` | 53 | $35.23 | $35.70 | +24.91 | — | +0.00 | +24.91 | +34.45 | — |
| 2026-08-26 | `RUM` | 198 | $10.23 | $10.07 | -31.68 | — | +0.00 | -31.68 | +128.70 | — |
| 2026-08-26 | `ZYME` | 64 | $27.47 | $27.56 | +5.76 | — | +0.00 | +5.76 | -83.20 | — |
| 2026-08-26 | `REAX` | 77 | $28.43 | $26.61 | -140.14 | — | +0.00 | -140.14 | +192.50 | — |
| 2026-08-26 | `EOLS` | 214 | $8.97 | $8.86 | -24.61 | — | +0.00 | -24.61 | +29.96 | — |
| 2026-08-26 | `FNV` | 7 | — | $267.02 | +0.00 | $267.37 | +2.45 | +2.45 | +0.00 | +2.45 |
| 2026-08-26 | `TRLV` | 171 | — | $11.22 | +0.00 | $11.43 | +35.91 | +35.91 | +0.00 | +35.91 |
| 2026-08-26 | `CAPR` | 232 | — | $8.29 | +0.00 | $9.36 | +248.24 | +248.24 | +0.00 | +248.24 |
| 2026-08-26 | `FWRD` | 110 | — | $17.41 | +0.00 | $17.63 | +24.20 | +24.20 | +0.00 | +24.20 |
| 2026-08-26 | `FLNC` | 173 | — | $11.12 | +0.00 | $11.08 | -6.92 | -6.92 | +0.00 | -6.92 |
| 2026-08-27 | `FNV` | 7 | $267.37 | $267.23 | -0.98 | — | +0.00 | -0.98 | +1.47 | — |
| 2026-08-27 | `TRLV` | 171 | $11.43 | $11.38 | -8.55 | — | +0.00 | -8.55 | +27.36 | — |
| 2026-08-27 | `CAPR` | 232 | $9.36 | $9.19 | -39.44 | — | +0.00 | -39.44 | +208.80 | — |
| 2026-08-27 | `FWRD` | 110 | $17.63 | $17.60 | -3.30 | — | +0.00 | -3.30 | +20.90 | — |
| 2026-08-27 | `FLNC` | 173 | $11.08 | $11.52 | +76.12 | — | +0.00 | +76.12 | +69.20 | — |
| 2026-08-28 | `SMTC` | 8 | — | $141.76 | +0.00 | $131.17 | -84.72 | -84.72 | +0.00 | -84.72 |
| 2026-08-28 | `SEDG` | 37 | — | $32.90 | +0.00 | $31.41 | -55.13 | -55.13 | +0.00 | -55.13 |
| 2026-08-28 | `TLS` | 257 | — | $4.82 | +0.00 | $4.79 | -7.71 | -7.71 | +0.00 | -7.71 |
| 2026-08-28 | `TH` | 65 | — | $19.00 | +0.00 | $18.55 | -29.25 | -29.25 | +0.00 | -29.25 |
| 2026-08-28 | `OPTX` | 144 | — | $8.61 | +0.00 | $8.52 | -12.96 | -12.96 | +0.00 | -12.96 |
| 2026-08-28 | `BBWI` | 66 | — | $18.75 | +0.00 | $19.22 | +31.02 | +31.02 | +0.00 | +31.02 |
| 2026-08-28 | `CAPR` | 127 | — | $9.73 | +0.00 | $9.59 | -17.78 | -17.78 | +0.00 | -17.78 |
| 2026-08-28 | `ERAS` | 64 | — | $19.25 | +0.00 | $18.03 | -78.08 | -78.08 | +0.00 | -78.08 |
| 2026-08-31 | `SMTC` | 8 | $131.17 | $132.30 | +9.04 | — | +0.00 | +9.04 | -75.68 | — |
| 2026-08-31 | `SEDG` | 37 | $31.41 | $31.15 | -9.62 | — | +0.00 | -9.62 | -64.75 | — |
| 2026-08-31 | `TLS` | 257 | $4.79 | $4.81 | +5.14 | — | +0.00 | +5.14 | -2.57 | — |
| 2026-08-31 | `TH` | 65 | $18.55 | $18.12 | -27.63 | $18.52 | +25.67 | -1.96 | -56.88 | -31.20 |
| 2026-08-31 | `OPTX` | 144 | $8.52 | $8.52 | +0.00 | — | +0.00 | +0.00 | -12.96 | — |
| 2026-08-31 | `BBWI` | 66 | $19.22 | $19.25 | +1.98 | — | +0.00 | +1.98 | +33.00 | — |
| 2026-08-31 | `CAPR` | 127 | $9.59 | $9.50 | -11.43 | — | +0.00 | -11.43 | -29.21 | — |
| 2026-08-31 | `ERAS` | 64 | $18.03 | $17.87 | -10.24 | — | +0.00 | -10.24 | -88.32 | — |
| 2026-09-01 | `TH` | 65 | $18.52 | $18.45 | -4.55 | — | +0.00 | -4.55 | -35.75 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CXW` | 49 | — | $32.31 | +0.00 | $33.66 | +66.15 | +66.15 | +0.00 | +66.15 |
| 2026-09-03 | `FRNM` | 100 | — | $15.87 | +0.00 | $16.90 | +103.00 | +103.00 | +0.00 | +103.00 |
| 2026-09-03 | `MMED` | 67 | — | $23.88 | +0.00 | $23.84 | -2.68 | -2.68 | +0.00 | -2.68 |
| 2026-09-03 | `DE` | 2 | — | $703.25 | +0.00 | $694.41 | -17.68 | -17.68 | +0.00 | -17.68 |
| 2026-09-03 | `CNXC` | 48 | — | $32.88 | +0.00 | $32.85 | -1.44 | -1.44 | +0.00 | -1.44 |
| 2026-09-03 | `OPTX` | 211 | — | $7.59 | +0.00 | $7.76 | +35.87 | +35.87 | +0.00 | +35.87 |
| 2026-09-04 | `CXW` | 49 | $33.66 | $33.46 | -9.80 | — | +0.00 | -9.80 | +56.35 | — |
| 2026-09-04 | `FRNM` | 100 | $16.90 | $16.40 | -50.00 | $16.31 | -9.00 | -59.00 | +53.00 | +44.00 |
| 2026-09-04 | `MMED` | 67 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.68 | — |
| 2026-09-04 | `DE` | 2 | $694.41 | $692.03 | -4.76 | — | +0.00 | -4.76 | -22.44 | — |
| 2026-09-04 | `CNXC` | 48 | $32.85 | $32.48 | -17.76 | — | +0.00 | -17.76 | -19.20 | — |
| 2026-09-04 | `OPTX` | 211 | $7.76 | $7.79 | +6.33 | — | +0.00 | +6.33 | +42.20 | — |
| 2026-09-04 | `MRX` | 53 | — | $75.65 | +0.00 | $78.27 | +138.86 | +138.86 | +0.00 | +138.86 |
| 2026-09-04 | `BAK` | 2072 | — | $1.94 | +0.00 | $1.89 | -103.60 | -103.60 | +0.00 | -103.60 |
| 2026-09-08 | `FRNM` | 100 | $16.31 | $16.74 | +43.00 | — | +0.00 | +43.00 | +87.00 | — |
| 2026-09-08 | `MRX` | 53 | $78.27 | $78.84 | +30.21 | $76.71 | -112.89 | -82.68 | +169.07 | +56.18 |
| 2026-09-08 | `BAK` | 2072 | $1.89 | $1.94 | +103.60 | — | +0.00 | +103.60 | +0.00 | — |
| 2026-09-09 | `MRX` | 53 | $76.71 | $76.60 | -5.83 | — | +0.00 | -5.83 | +50.35 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 11 | — | $164.43 | +0.00 | $150.28 | -155.65 | -155.65 | +0.00 | -155.65 |
| 2026-09-11 | `ADBE` | 8 | — | $242.17 | +0.00 | $252.23 | +80.48 | +80.48 | +0.00 | +80.48 |
| 2026-09-11 | `BAK` | 917 | — | $2.12 | +0.00 | $2.08 | -36.68 | -36.68 | +0.00 | -36.68 |
| 2026-09-11 | `AMTX` | 953 | — | $2.04 | +0.00 | $2.01 | -28.59 | -28.59 | +0.00 | -28.59 |
| 2026-09-11 | `RH` | 14 | — | $135.71 | +0.00 | $134.07 | -22.96 | -22.96 | +0.00 | -22.96 |
| 2026-09-14 | `ORCL` | 11 | $150.28 | $141.42 | -97.46 | — | +0.00 | -97.46 | -253.11 | — |
| 2026-09-14 | `ADBE` | 8 | $252.23 | $261.51 | +74.24 | — | +0.00 | +74.24 | +154.72 | — |
| 2026-09-14 | `BAK` | 917 | $2.08 | $2.05 | -27.51 | — | +0.00 | -27.51 | -64.19 | — |
| 2026-09-14 | `AMTX` | 953 | $2.01 | $2.01 | +0.00 | — | +0.00 | +0.00 | -28.59 | — |
| 2026-09-14 | `RH` | 14 | $134.07 | $131.40 | -37.38 | — | +0.00 | -37.38 | -60.34 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 89 | — | $26.27 | +0.00 | $26.59 | +28.48 | +28.48 | +0.00 | +28.48 |
| 2026-09-16 | `CLS` | 7 | — | $320.20 | +0.00 | $323.83 | +25.41 | +25.41 | +0.00 | +25.41 |
| 2026-09-16 | `GME` | 109 | — | $21.48 | +0.00 | $21.98 | +54.50 | +54.50 | +0.00 | +54.50 |
| 2026-09-16 | `SION` | 338 | — | $6.95 | +0.00 | $7.04 | +30.42 | +30.42 | +0.00 | +30.42 |
| 2026-09-17 | `WAY` | 89 | $26.59 | $26.51 | -7.12 | — | +0.00 | -7.12 | +21.36 | — |
| 2026-09-17 | `CLS` | 7 | $323.83 | $337.75 | +97.44 | $329.94 | -54.67 | +42.77 | +122.85 | +68.18 |
| 2026-09-17 | `GME` | 109 | $21.98 | $22.12 | +15.26 | $22.77 | +70.85 | +86.11 | +69.76 | +140.61 |
| 2026-09-17 | `SION` | 338 | $7.04 | $7.27 | +77.74 | $7.18 | -30.42 | +47.32 | +108.16 | +77.74 |
| 2026-09-17 | `SMTC` | 3 | — | $170.85 | +0.00 | $178.19 | +22.02 | +22.02 | +0.00 | +22.02 |
| 2026-09-17 | `JBHT` | 2 | — | $238.60 | +0.00 | $236.80 | -3.60 | -3.60 | +0.00 | -3.60 |
| 2026-09-17 | `TNDM` | 35 | — | $17.72 | +0.00 | $17.23 | -17.15 | -17.15 | +0.00 | -17.15 |
| 2026-09-17 | `BAK` | 351 | — | $1.77 | +0.00 | $1.79 | +7.02 | +7.02 | +0.00 | +7.02 |
| 2026-09-18 | `CLS` | 7 | $329.94 | $332.06 | +14.84 | $332.63 | +3.99 | +18.83 | +83.02 | +87.01 |
| 2026-09-18 | `GME` | 109 | $22.77 | $22.90 | +14.17 | $22.64 | -28.34 | -14.17 | +154.78 | +126.44 |
| 2026-09-18 | `SION` | 338 | $7.18 | $7.12 | -20.28 | $5.96 | -392.08 | -412.36 | +57.46 | -334.62 |
| 2026-09-18 | `SMTC` | 3 | $178.19 | $182.33 | +12.42 | — | +0.00 | +12.42 | +34.44 | — |
| 2026-09-18 | `JBHT` | 2 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -3.60 | — |
| 2026-09-18 | `TNDM` | 35 | $17.23 | $17.13 | -3.50 | — | +0.00 | -3.50 | -20.65 | — |
| 2026-09-18 | `BAK` | 351 | $1.79 | $1.77 | -7.02 | — | +0.00 | -7.02 | +0.00 | — |
| 2026-09-18 | `RARE` | 55 | — | $14.79 | +0.00 | $14.51 | -15.40 | -15.40 | +0.00 | -15.40 |
| 2026-09-18 | `BHVN` | 58 | — | $14.07 | +0.00 | $13.62 | -26.10 | -26.10 | +0.00 | -26.10 |
| 2026-09-18 | `FLNC` | 109 | — | $7.54 | +0.00 | $7.32 | -23.43 | -23.43 | +0.00 | -23.43 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +36.39 | HLIT, ANGX, MH, VELO, ARX, S, ZS | — | $1,392.75 | $10,019.55 | HLIT×94, ANGX×290, MH×92, VELO×81, ARX×63, S×52, ZS×6 |
| 2026-08-17 | +2.25 | $1,392.75 | HLIT×94, ANGX×290, MH×92, VELO×81, ARX×63, S×52, ZS×6 | $10,071.64 | +52.09 | -118.44 | OUST, CELC | HLIT, ANGX, MH, VELO, ARX, S, ZS | $30.69 | $9,931.71 | OUST×102, CELC×54 |
| 2026-08-18 | -6.20 | $30.69 | OUST×102, CELC×54 | $9,618.39 | -313.32 | +0.00 | — | OUST, CELC | $9,613.84 | $9,613.84 | — |
| 2026-08-19 | -7.20 | $9,613.84 | — | $9,613.84 | -0.00 | +0.00 | — | — | $9,613.84 | $9,613.84 | — |
| 2026-08-20 | +1.12 | $9,613.84 | — | $9,613.84 | -0.00 | -170.67 | BHP, AUTL, CRSP, ASST, MRNA, ZLAB, TEAM, HUMA | — | $178.50 | $9,407.34 | BHP×13, AUTL×486, CRSP×20, ASST×75, MRNA×8, ZLAB×45, TEAM×6, HUMA×1699 |
| 2026-08-21 | +3.25 | $178.50 | BHP×13, AUTL×486, CRSP×20, ASST×75, MRNA×8, ZLAB×45, TEAM×6, HUMA×1699 | $9,578.76 | +171.42 | -131.37 | FUTU, GRAL, ABTC, HIVE, MARA, BTDR | BHP, ASST, MRNA, ZLAB, TEAM, HUMA | $58.19 | $9,404.25 | AUTL×486, CRSP×20, FUTU×10, GRAL×15, ABTC×137, HIVE×368, MARA×101, BTDR×107 |
| 2026-08-24 | -5.17 | $58.19 | AUTL×486, CRSP×20, FUTU×10, GRAL×15, ABTC×137, HIVE×368, MARA×101, BTDR×107 | $9,390.49 | -13.76 | -33.50 | — | AUTL, FUTU, GRAL, ABTC, HIVE, MARA, BTDR | $8,193.13 | $9,334.63 | CRSP×20 |
| 2026-08-25 | +1.80 | $8,193.13 | CRSP×20 | $9,351.73 | +17.10 | +468.17 | EZPW, RUM, ZYME, REAX, EOLS | CRSP | $45.36 | $9,805.93 | EZPW×53, RUM×198, ZYME×64, REAX×77, EOLS×214 |
| 2026-08-26 | +2.02 | $45.36 | EZPW×53, RUM×198, ZYME×64, REAX×77, EOLS×214 | $9,640.17 | -165.76 | +303.88 | FNV, TRLV, CAPR, FWRD, FLNC | EZPW, RUM, ZYME, REAX, EOLS | $65.86 | $9,919.64 | FNV×7, TRLV×171, CAPR×232, FWRD×110, FLNC×173 |
| 2026-08-27 | — | $65.86 | FNV×7, TRLV×171, CAPR×232, FWRD×110, FLNC×173 | $9,943.49 | +23.85 | +0.00 | — | FNV, TRLV, CAPR, FWRD, FLNC | $9,930.95 | $9,930.95 | — |
| 2026-08-28 | +0.75 | $9,930.95 | — | $9,930.95 | +0.00 | -254.61 | SMTC, SEDG, TLS, TH, OPTX, BBWI, CAPR, ERAS | — | $142.00 | $9,657.56 | SMTC×8, SEDG×37, TLS×257, TH×65, OPTX×144, BBWI×66, CAPR×127, ERAS×64 |
| 2026-08-31 | -5.85 | $142.00 | SMTC×8, SEDG×37, TLS×257, TH×65, OPTX×144, BBWI×66, CAPR×127, ERAS×64 | $9,614.81 | -42.75 | +25.67 | — | SMTC, SEDG, TLS, OPTX, BBWI, CAPR, ERAS | $8,419.89 | $9,623.69 | TH×65 |
| 2026-09-01 | -6.30 | $8,419.89 | TH×65 | $9,619.14 | -4.55 | +0.00 | — | TH | $9,616.93 | $9,616.93 | — |
| 2026-09-02 | -3.83 | $9,616.93 | — | $9,616.93 | +0.00 | +0.00 | — | — | $9,616.93 | $9,616.93 | — |
| 2026-09-03 | -0.90 | $9,616.93 | — | $9,616.93 | +0.00 | +183.22 | CXW, FRNM, MMED, DE, CNXC, OPTX | — | $247.08 | $9,786.68 | CXW×49, FRNM×100, MMED×67, DE×2, CNXC×48, OPTX×211 |
| 2026-09-04 | +2.25 | $247.08 | CXW×49, FRNM×100, MMED×67, DE×2, CNXC×48, OPTX×211 | $9,710.69 | -75.99 | +26.26 | MRX, BAK | CXW, MMED, DE, CNXC, OPTX | $1.37 | $9,696.76 | FRNM×100, MRX×53, BAK×2072 |
| 2026-09-08 | -11.47 | $1.37 | FRNM×100, MRX×53, BAK×2072 | $9,873.57 | +176.81 | -112.89 | — | FRNM, BAK | $5,665.62 | $9,731.25 | MRX×53 |
| 2026-09-09 | -13.95 | $5,665.62 | MRX×53 | $9,725.42 | -5.83 | +0.00 | — | MRX | $9,723.23 | $9,723.23 | — |
| 2026-09-10 | -13.28 | $9,723.23 | — | $9,723.23 | +0.00 | +0.00 | — | — | $9,723.23 | $9,723.23 | — |
| 2026-09-11 | +0.50 | $9,723.23 | — | $9,723.23 | +0.00 | -163.40 | ORCL, ADBE, BAK, AMTX, RH | — | $158.85 | $9,529.64 | ORCL×11, ADBE×8, BAK×917, AMTX×953, RH×14 |
| 2026-09-14 | -11.00 | $158.85 | ORCL×11, ADBE×8, BAK×917, AMTX×953, RH×14 | $9,441.53 | -88.11 | +0.00 | — | ORCL, ADBE, BAK, AMTX, RH | $9,410.92 | $9,410.92 | — |
| 2026-09-15 | -3.84 | $9,410.92 | — | $9,410.92 | +0.00 | +0.00 | — | — | $9,410.92 | $9,410.92 | — |
| 2026-09-16 | +5.30 | $9,410.92 | — | $9,410.92 | +0.00 | +138.81 | WAY, CLS, GME, SION | — | $130.13 | $9,538.79 | WAY×89, CLS×7, GME×109, SION×338 |
| 2026-09-17 | +7.38 | $130.13 | WAY×89, CLS×7, GME×109, SION×338 | $9,722.11 | +183.32 | -5.95 | SMTC, JBHT, TNDM, BAK | WAY | $245.39 | $9,703.25 | CLS×7, GME×109, SION×338, SMTC×3, JBHT×2, TNDM×35, BAK×351 |
| 2026-09-18 | +4.86 | $245.39 | CLS×7, GME×109, SION×338, SMTC×3, JBHT×2, TNDM×35, BAK×351 | $9,713.88 | +10.63 | -481.36 | RARE, BHVN, FLNC | SMTC, JBHT, TNDM, BAK | $18.59 | $9,215.13 | CLS×7, GME×109, SION×338, RARE×55, BHVN×58, FLNC×109 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $6,256.30 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $5,008.29 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $3,773.20 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $2,534.75 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZS` | 6 | $190.00 | $2.01 | — | $1,392.75 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+15.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,392.75 | ▲ close $10,019.55 vs 09:30 $10,000.00 (session +36.39) | 16:00 close · cash $1,392.75 · equity $10,019.55 vs 09:30 $10,000.00 (+19.55; session marks +36.39) · 7 name(s) marked open→close (per-name table). HLIT×94 09:30 $13.18 → close $13.92 +69.56; ANGX×290 09:30 $4.31 → close $4.37 +17.40; MH×92 09:30 $13.55 → close $13.10 -41.40; VELO×81 09:30 $15.38 → close $16.16 +63.18; ARX×63 09:30 $19.57 → close $19.58 +0.63; S×52 09:30 $23.77 → close $23.11 -34.58; ZS×6 09:30 $190.00 → close $183.60 -38.40 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,392.75 | ▲ 09:30 equity $10,071.64 vs yday $10,019.55 (+52.09) | 09:30 open · cash $1,392.75 (unchanged overnight, no fees) · equity $10,071.64 vs prior close $10,019.55 (+52.09) · 7 name(s) re-marked at the open (per-name table). HLIT×94 yday $13.92 → 09:30 $13.84 -7.52; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; MH×92 yday $13.10 → 09:30 $13.16 +5.52; VELO×81 yday $16.16 → 09:30 $16.05 -8.91; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; S×52 yday $23.11 → 09:30 $22.50 -31.72; ZS×6 yday $183.60 → 09:30 $188.38 +28.65 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $2,691.41 | ▲ +57.47 after sell → book $10,069.34; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $4,021.61 | ▲ +76.56 after sell → book $10,065.54; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $5,230.04 | ▼ -40.44 after sell → book $10,063.25; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 81 | $16.05 | $2.26 | $+49.78 | $6,527.83 | ▲ +49.78 after sell → book $10,060.99; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,758.54 | ▼ -4.38 after sell → book $10,058.79; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `S` | 52 | $22.50 | $2.17 | $-70.61 | $8,926.37 | ▼ -70.61 after sell → book $10,056.62; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZS` | 6 | $188.38 | $2.03 | $-13.79 | $10,054.60 | ▼ -13.79 after sell → book $10,054.60; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 102 | $49.00 | $2.30 | — | $5,054.30 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $5027.30 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 54 | $92.99 | $2.15 | — | $30.69 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $5027.30 | join🟡 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.69 | ▼ close $9,931.71 vs 09:30 $10,071.64 (session -118.44) | 16:00 close · cash $30.69 · equity $9,931.71 vs 09:30 $10,071.64 (-139.93; session marks -118.44) · 2 name(s) marked open→close (per-name table). OUST×102 09:30 $49.00 → close $48.13 -88.74; CELC×54 09:30 $92.99 → close $92.44 -29.70 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.69 | ▼ 09:30 equity $9,618.39 vs yday $9,931.71 (-313.32) | 09:30 open · cash $30.69 (unchanged overnight, no fees) · equity $9,618.39 vs prior close $9,931.71 (-313.32) · 2 name(s) re-marked at the open (per-name table). OUST×102 yday $48.13 → 09:30 $45.09 -310.08; CELC×54 yday $92.44 → 09:30 $92.38 -3.24 | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 102 | $45.09 | $2.35 | $-403.47 | $4,627.52 | ▼ -403.47 after sell → book $9,616.04; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 54 | $92.38 | $2.20 | $-37.29 | $9,613.84 | ▼ -37.29 after sell → book $9,613.84; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,613.84 | ▲ close $9,613.84 vs 09:30 $9,618.39 (session +0.00) | 16:00 close · cash $9,613.84 · no lots left · equity $9,613.84. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,613.84 | ▲ 09:30 equity $9,613.84 vs yday $9,613.84 (-0.00) | 09:30 open · cash $9,613.84 · no holdings · equity $9,613.84 vs prior close $9,613.84 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,613.84 | ▲ close $9,613.84 vs 09:30 $9,613.84 (session +0.00) | 16:00 close · cash $9,613.84 · no lots left · equity $9,613.84. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,613.84 | ▲ 09:30 equity $9,613.84 vs yday $9,613.84 (-0.00) | 09:30 open · cash $9,613.84 · no holdings · equity $9,613.84 vs prior close $9,613.84 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,428.68 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1201.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 486 | $2.47 | $6.27 | — | $7,221.99 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1201.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 20 | $58.73 | $2.05 | — | $6,045.34 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1201.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 75 | $16.00 | $2.21 | — | $4,843.12 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1201.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $3,639.99 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1201.73 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 45 | $26.57 | $2.12 | — | $2,442.21 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1201.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 6 | $173.90 | $2.01 | — | $1,396.81 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1201.73 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1699 | $0.71 | $17.11 | — | $178.50 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1201.73 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.50 | ▼ close $9,407.34 vs 09:30 $9,613.84 (session -170.67) | 16:00 close · cash $178.50 · equity $9,407.34 vs 09:30 $9,613.84 (-206.50; session marks -170.67) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; AUTL×486 09:30 $2.47 → close $2.46 -4.86; CRSP×20 09:30 $58.73 → close $58.12 -12.20; ASST×75 09:30 $16.00 → close $16.13 +9.75; MRNA×8 09:30 $150.14 → close $133.32 -134.56; ZLAB×45 09:30 $26.57 → close $26.02 -24.75; TEAM×6 09:30 $173.90 → close $174.91 +6.06; HUMA×1699 09:30 $0.71 → close $0.68 -44.17 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.50 | ▲ 09:30 equity $9,578.76 vs yday $9,407.34 (+171.42) | 09:30 open · cash $178.50 (unchanged overnight, no fees) · equity $9,578.76 vs prior close $9,407.34 (+171.42) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; AUTL×486 yday $2.46 → 09:30 $2.47 +4.86; CRSP×20 yday $58.12 → 09:30 $59.72 +32.00; ASST×75 yday $16.13 → 09:30 $17.66 +114.75; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ZLAB×45 yday $26.02 → 09:30 $26.25 +10.35; TEAM×6 yday $174.91 → 09:30 $174.22 -4.14; HUMA×1699 yday $0.68 → 09:30 $0.67 -11.89 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,420.81 | ▲ +57.15 after sell → book $9,576.71; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 75 | $17.66 | $2.24 | $+120.05 | $2,743.08 | ▲ +120.05 after sell → book $9,574.47; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $3,805.92 | ▼ -140.29 after sell → book $9,572.44; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 45 | $26.25 | $2.15 | $-18.67 | $4,985.03 | ▼ -18.67 after sell → book $9,570.29; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TEAM` | 6 | $174.22 | $2.03 | $-2.12 | $6,028.32 | ▼ -2.12 after sell → book $9,568.27; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1699 | $0.67 | $16.84 | $-90.02 | $7,156.61 | ▼ -90.02 after sell → book $9,551.43; vs 09:30 mark -16.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $6,002.79 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1192.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 15 | $78.88 | $2.04 | — | $4,817.55 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1192.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 137 | $8.66 | $2.40 | — | $3,628.73 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1192.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 368 | $3.24 | $4.75 | — | $2,431.66 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1192.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 101 | $11.70 | $2.29 | — | $1,247.67 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1192.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 107 | $11.10 | $2.31 | — | $58.19 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=+19.1; leftover $1192.77 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $58.19 | ▼ close $9,404.25 vs 09:30 $9,578.76 (session -131.37) | 16:00 close · cash $58.19 · equity $9,404.25 vs 09:30 $9,578.76 (-174.51; session marks -131.37) · 8 name(s) marked open→close (per-name table). AUTL×486 09:30 $2.47 → close $2.41 -29.16; CRSP×20 09:30 $59.72 → close $59.50 -4.40; FUTU×10 09:30 $115.18 → close $123.64 +84.60; GRAL×15 09:30 $78.88 → close $79.54 +9.90; ABTC×137 09:30 $8.66 → close $7.93 -100.01; HIVE×368 09:30 $3.24 → close $3.03 -77.28; MARA×101 09:30 $11.70 → close $11.26 -44.44; BTDR×107 09:30 $11.10 → close $11.37 +29.42 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $58.19 | ▼ 09:30 equity $9,390.49 vs yday $9,404.25 (-13.76) | 09:30 open · cash $58.19 (unchanged overnight, no fees) · equity $9,390.49 vs prior close $9,404.25 (-13.76) · 8 name(s) re-marked at the open (per-name table). AUTL×486 yday $2.41 → 09:30 $2.40 -4.86; CRSP×20 yday $59.50 → 09:30 $58.75 -15.00; FUTU×10 yday $123.64 → 09:30 $121.00 -26.40; GRAL×15 yday $79.54 → 09:30 $81.87 +34.95; ABTC×137 yday $7.93 → 09:30 $8.00 +9.59; HIVE×368 yday $3.03 → 09:30 $2.99 -14.72; MARA×101 yday $11.26 → 09:30 $11.17 -9.09; BTDR×107 yday $11.37 → 09:30 $11.48 +11.77 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 486 | $2.40 | $6.36 | $-46.65 | $1,218.23 | ▼ -46.65 after sell → book $9,384.13; vs 09:30 mark -6.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $2,426.19 | ▲ +54.14 after sell → book $9,382.09; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 15 | $81.87 | $2.06 | $+40.76 | $3,652.19 | ▲ +40.76 after sell → book $9,380.04; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 137 | $8.00 | $2.43 | $-95.25 | $4,745.75 | ▼ -95.25 after sell → book $9,377.60; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 368 | $2.99 | $4.82 | $-101.57 | $5,841.26 | ▼ -101.57 after sell → book $9,372.79; vs 09:30 mark -4.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 101 | $11.17 | $2.32 | $-58.14 | $6,967.11 | ▼ -58.14 after sell → book $9,370.47; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 107 | $11.48 | $2.34 | $+36.55 | $8,193.13 | ▲ +36.55 after sell → book $9,368.13; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,193.13 | ▼ close $9,334.63 vs 09:30 $9,390.49 (session -33.50) | 16:00 close · cash $8,193.13 · equity $9,334.63 vs 09:30 $9,390.49 (-55.86; session marks -33.50) · 1 name(s) marked open→close (per-name table). CRSP×20 09:30 $58.75 → close $57.08 -33.50 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,193.13 | ▲ 09:30 equity $9,351.73 vs yday $9,334.63 (+17.10) | 09:30 open · cash $8,193.13 (unchanged overnight, no fees) · equity $9,351.73 vs prior close $9,334.63 (+17.10) · 1 name(s) re-marked at the open (per-name table). CRSP×20 yday $57.08 → 09:30 $57.93 +17.10 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.93 | $2.07 | $-20.12 | $9,349.66 | ▼ -20.12 after sell → book $9,349.66; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 53 | $35.05 | $2.15 | — | $7,489.86 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1869.93 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 198 | $9.42 | $2.58 | — | $5,622.11 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1869.93 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 64 | $28.86 | $2.18 | — | $3,772.89 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1869.93 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 77 | $24.11 | $2.22 | — | $1,914.20 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=+891.7; leftover $1869.93 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 214 | $8.72 | $2.76 | — | $45.36 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+13.0; leftover $1869.93 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.36 | ▲ close $9,805.93 vs 09:30 $9,351.73 (session +468.17) | 16:00 close · cash $45.36 · equity $9,805.93 vs 09:30 $9,351.73 (+454.20; session marks +468.17) · 5 name(s) marked open→close (per-name table). EZPW×53 09:30 $35.05 → close $35.23 +9.54; RUM×198 09:30 $9.42 → close $10.23 +160.38; ZYME×64 09:30 $28.86 → close $27.47 -88.96; REAX×77 09:30 $24.11 → close $28.43 +332.64; EOLS×214 09:30 $8.72 → close $8.97 +54.57 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.36 | ▼ 09:30 equity $9,640.17 vs yday $9,805.93 (-165.76) | 09:30 open · cash $45.36 (unchanged overnight, no fees) · equity $9,640.17 vs prior close $9,805.93 (-165.76) · 5 name(s) re-marked at the open (per-name table). EZPW×53 yday $35.23 → 09:30 $35.70 +24.91; RUM×198 yday $10.23 → 09:30 $10.07 -31.68; ZYME×64 yday $27.47 → 09:30 $27.56 +5.76; REAX×77 yday $28.43 → 09:30 $26.61 -140.14; EOLS×214 yday $8.97 → 09:30 $8.86 -24.61 | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 53 | $35.70 | $2.17 | $+30.13 | $1,935.29 | ▲ +30.13 after sell → book $9,638.00; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 198 | $10.07 | $2.63 | $+123.48 | $3,926.51 | ▲ +123.48 after sell → book $9,635.36; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 64 | $27.56 | $2.21 | $-87.59 | $5,688.15 | ▼ -87.59 after sell → book $9,633.16; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 77 | $26.61 | $2.25 | $+188.03 | $7,734.87 | ▲ +188.03 after sell → book $9,630.91; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 214 | $8.86 | $2.81 | $+24.39 | $9,628.10 | ▲ +24.39 after sell → book $9,628.10; vs 09:30 mark -2.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 7 | $267.02 | $2.01 | — | $7,756.94 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $1925.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 171 | $11.22 | $2.50 | — | $5,835.82 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $1925.62 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 232 | $8.29 | $2.99 | — | $3,909.55 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $1925.62 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 110 | $17.41 | $2.32 | — | $1,992.13 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-9.2; leftover $1925.62 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 173 | $11.12 | $2.51 | — | $65.86 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1925.62 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.86 | ▲ close $9,919.64 vs 09:30 $9,640.17 (session +303.88) | 16:00 close · cash $65.86 · equity $9,919.64 vs 09:30 $9,640.17 (+279.47; session marks +303.88) · 5 name(s) marked open→close (per-name table). FNV×7 09:30 $267.02 → close $267.37 +2.45; TRLV×171 09:30 $11.22 → close $11.43 +35.91; CAPR×232 09:30 $8.29 → close $9.36 +248.24; FWRD×110 09:30 $17.41 → close $17.63 +24.20; FLNC×173 09:30 $11.12 → close $11.08 -6.92 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.86 | ▲ 09:30 equity $9,943.49 vs yday $9,919.64 (+23.85) | 09:30 open · cash $65.86 (unchanged overnight, no fees) · equity $9,943.49 vs prior close $9,919.64 (+23.85) · 5 name(s) re-marked at the open (per-name table). FNV×7 yday $267.37 → 09:30 $267.23 -0.98; TRLV×171 yday $11.43 → 09:30 $11.38 -8.55; CAPR×232 yday $9.36 → 09:30 $9.19 -39.44; FWRD×110 yday $17.63 → 09:30 $17.60 -3.30; FLNC×173 yday $11.08 → 09:30 $11.52 +76.12 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 7 | $267.23 | $2.04 | $-2.58 | $1,934.43 | ▼ -2.58 after sell → book $9,941.45; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 171 | $11.38 | $2.55 | $+22.31 | $3,877.87 | ▲ +22.31 after sell → book $9,938.91; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 232 | $9.19 | $3.05 | $+202.76 | $6,006.90 | ▲ +202.76 after sell → book $9,935.86; vs 09:30 mark -3.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWRD` | 110 | $17.60 | $2.35 | $+16.23 | $7,940.54 | ▲ +16.23 after sell → book $9,933.50; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 173 | $11.52 | $2.55 | $+64.14 | $9,930.95 | ▲ +64.14 after sell → book $9,930.95; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,930.95 | ▲ close $9,930.95 vs 09:30 $9,943.49 (session +0.00) | 16:00 close · cash $9,930.95 · no lots left · equity $9,930.95. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,930.95 | ▲ 09:30 equity $9,930.95 vs yday $9,930.95 (+0.00) | 09:30 open · cash $9,930.95 · no holdings · equity $9,930.95 vs prior close $9,930.95 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $8,794.86 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1241.37 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 37 | $32.90 | $2.10 | — | $7,575.46 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1241.37 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 257 | $4.82 | $3.32 | — | $6,333.40 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1241.37 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 65 | $19.00 | $2.19 | — | $5,096.22 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+7.5; leftover $1241.37 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 144 | $8.61 | $2.42 | — | $3,853.95 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; ret5=-0.7; leftover $1241.37 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 66 | $18.75 | $2.19 | — | $2,614.27 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=-5.0; leftover $1241.37 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 127 | $9.73 | $2.37 | — | $1,376.18 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; ret5=+47.1; leftover $1241.37 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 64 | $19.25 | $2.18 | — | $142.00 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=+14.1; leftover $1241.37 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.00 | ▼ close $9,657.56 vs 09:30 $9,930.95 (session -254.61) | 16:00 close · cash $142.00 · equity $9,657.56 vs 09:30 $9,930.95 (-273.39; session marks -254.61) · 8 name(s) marked open→close (per-name table). SMTC×8 09:30 $141.76 → close $131.17 -84.72; SEDG×37 09:30 $32.90 → close $31.41 -55.13; TLS×257 09:30 $4.82 → close $4.79 -7.71; TH×65 09:30 $19.00 → close $18.55 -29.25; OPTX×144 09:30 $8.61 → close $8.52 -12.96; BBWI×66 09:30 $18.75 → close $19.22 +31.02; CAPR×127 09:30 $9.73 → close $9.59 -17.78; ERAS×64 09:30 $19.25 → close $18.03 -78.08 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.00 | ▼ 09:30 equity $9,614.81 vs yday $9,657.56 (-42.75) | 09:30 open · cash $142.00 (unchanged overnight, no fees) · equity $9,614.81 vs prior close $9,657.56 (-42.75) · 8 name(s) re-marked at the open (per-name table). SMTC×8 yday $131.17 → 09:30 $132.30 +9.04; SEDG×37 yday $31.41 → 09:30 $31.15 -9.62; TLS×257 yday $4.79 → 09:30 $4.81 +5.14; TH×65 yday $18.55 → 09:30 $18.12 -27.63; OPTX×144 yday $8.52 → 09:30 $8.52 +0.00; BBWI×66 yday $19.22 → 09:30 $19.25 +1.98; CAPR×127 yday $9.59 → 09:30 $9.50 -11.43; ERAS×64 yday $18.03 → 09:30 $17.87 -10.24 | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $132.30 | $2.03 | $-79.73 | $1,198.37 | ▼ -79.73 after sell → book $9,612.77; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 37 | $31.15 | $2.12 | $-68.97 | $2,348.80 | ▼ -68.97 after sell → book $9,610.65; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 257 | $4.81 | $3.37 | $-9.25 | $3,581.60 | ▼ -9.25 after sell → book $9,607.28; vs 09:30 mark -3.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 144 | $8.52 | $2.46 | $-17.84 | $4,806.02 | ▼ -17.84 after sell → book $9,604.83; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 66 | $19.25 | $2.21 | $+28.60 | $6,074.31 | ▲ +28.60 after sell → book $9,602.62; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 127 | $9.50 | $2.40 | $-33.98 | $7,278.41 | ▼ -33.98 after sell → book $9,600.22; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 64 | $17.87 | $2.20 | $-92.70 | $8,419.89 | ▼ -92.70 after sell → book $9,598.02; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,419.89 | ▲ close $9,623.69 vs 09:30 $9,614.81 (session +25.67) | 16:00 close · cash $8,419.89 · equity $9,623.69 vs 09:30 $9,614.81 (+8.88; session marks +25.67) · 1 name(s) marked open→close (per-name table). TH×65 09:30 $18.12 → close $18.52 +25.67 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,419.89 | ▼ 09:30 equity $9,619.14 vs yday $9,623.69 (-4.55) | 09:30 open · cash $8,419.89 (unchanged overnight, no fees) · equity $9,619.14 vs prior close $9,623.69 (-4.55) · 1 name(s) re-marked at the open (per-name table). TH×65 yday $18.52 → 09:30 $18.45 -4.55 | — |
| 2026-09-01 09:30 ET | **SELL** | `TH` | 65 | $18.45 | $2.21 | $-40.14 | $9,616.93 | ▼ -40.14 after sell → book $9,616.93; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,616.93 | ▲ close $9,616.93 vs 09:30 $9,619.14 (session +0.00) | 16:00 close · cash $9,616.93 · no lots left · equity $9,616.93. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,616.93 | ▲ 09:30 equity $9,616.93 vs yday $9,616.93 (+0.00) | 09:30 open · cash $9,616.93 · no holdings · equity $9,616.93 vs prior close $9,616.93 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,616.93 | ▲ close $9,616.93 vs 09:30 $9,616.93 (session +0.00) | 16:00 close · cash $9,616.93 · no lots left · equity $9,616.93. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,616.93 | ▲ 09:30 equity $9,616.93 vs yday $9,616.93 (+0.00) | 09:30 open · cash $9,616.93 · no holdings · equity $9,616.93 vs prior close $9,616.93 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 49 | $32.31 | $2.14 | — | $8,031.61 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1602.82 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 100 | $15.87 | $2.29 | — | $6,442.32 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1602.82 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 67 | $23.88 | $2.19 | — | $4,840.17 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1602.82 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $3,431.67 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1602.82 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 48 | $32.88 | $2.13 | — | $1,851.30 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1602.82 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 211 | $7.59 | $2.72 | — | $247.08 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-11.5; leftover $1602.82 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $247.08 | ▲ close $9,786.68 vs 09:30 $9,616.93 (session +183.22) | 16:00 close · cash $247.08 · equity $9,786.68 vs 09:30 $9,616.93 (+169.75; session marks +183.22) · 6 name(s) marked open→close (per-name table). CXW×49 09:30 $32.31 → close $33.66 +66.15; FRNM×100 09:30 $15.87 → close $16.90 +103.00; MMED×67 09:30 $23.88 → close $23.84 -2.68; DE×2 09:30 $703.25 → close $694.41 -17.68; CNXC×48 09:30 $32.88 → close $32.85 -1.44; OPTX×211 09:30 $7.59 → close $7.76 +35.87 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $247.08 | ▼ 09:30 equity $9,710.69 vs yday $9,786.68 (-75.99) | 09:30 open · cash $247.08 (unchanged overnight, no fees) · equity $9,710.69 vs prior close $9,786.68 (-75.99) · 6 name(s) re-marked at the open (per-name table). CXW×49 yday $33.66 → 09:30 $33.46 -9.80; FRNM×100 yday $16.90 → 09:30 $16.40 -50.00; MMED×67 yday $23.84 → 09:30 $23.84 +0.00; DE×2 yday $694.41 → 09:30 $692.03 -4.76; CNXC×48 yday $32.85 → 09:30 $32.48 -17.76; OPTX×211 yday $7.76 → 09:30 $7.79 +6.33 | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 49 | $33.46 | $2.16 | $+52.05 | $1,884.46 | ▲ +52.05 after sell → book $9,708.53; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 67 | $23.84 | $2.21 | $-7.09 | $3,479.53 | ▼ -7.09 after sell → book $9,706.32; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 2 | $692.03 | $2.02 | $-26.45 | $4,861.57 | ▼ -26.45 after sell → book $9,704.30; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 48 | $32.48 | $2.16 | $-23.49 | $6,418.46 | ▼ -23.49 after sell → book $9,702.15; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 211 | $7.79 | $2.77 | $+36.71 | $8,059.38 | ▲ +36.71 after sell → book $9,699.38; vs 09:30 mark -2.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 53 | $75.65 | $2.15 | — | $4,047.78 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $4029.69 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 2072 | $1.94 | $26.73 | — | $1.37 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $4029.69 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.37 | ▲ close $9,696.76 vs 09:30 $9,710.69 (session +26.26) | 16:00 close · cash $1.37 · equity $9,696.76 vs 09:30 $9,710.69 (-13.93; session marks +26.26) · 3 name(s) marked open→close (per-name table). FRNM×100 09:30 $16.40 → close $16.31 -9.00; MRX×53 09:30 $75.65 → close $78.27 +138.86; BAK×2072 09:30 $1.94 → close $1.89 -103.60 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.37 | ▲ 09:30 equity $9,873.57 vs yday $9,696.76 (+176.81) | 09:30 open · cash $1.37 (unchanged overnight, no fees) · equity $9,873.57 vs prior close $9,696.76 (+176.81) · 3 name(s) re-marked at the open (per-name table). FRNM×100 yday $16.31 → 09:30 $16.74 +43.00; MRX×53 yday $78.27 → 09:30 $78.84 +30.21; BAK×2072 yday $1.89 → 09:30 $1.94 +103.60 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 100 | $16.74 | $2.32 | $+82.39 | $1,673.05 | ▲ +82.39 after sell → book $9,871.25; vs 09:30 mark -2.32 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 2072 | $1.94 | $27.10 | $-53.83 | $5,665.62 | ▼ -53.83 after sell → book $9,844.14; vs 09:30 mark -27.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,665.62 | ▼ close $9,731.25 vs 09:30 $9,873.57 (session -112.89) | 16:00 close · cash $5,665.62 · equity $9,731.25 vs 09:30 $9,873.57 (-142.32; session marks -112.89) · 1 name(s) marked open→close (per-name table). MRX×53 09:30 $78.84 → close $76.71 -112.89 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,665.62 | ▼ 09:30 equity $9,725.42 vs yday $9,731.25 (-5.83) | 09:30 open · cash $5,665.62 (unchanged overnight, no fees) · equity $9,725.42 vs prior close $9,731.25 (-5.83) · 1 name(s) re-marked at the open (per-name table). MRX×53 yday $76.71 → 09:30 $76.60 -5.83 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 53 | $76.60 | $2.19 | $+46.01 | $9,723.23 | ▲ +46.01 after sell → book $9,723.23; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,723.23 | ▲ close $9,723.23 vs 09:30 $9,725.42 (session +0.00) | 16:00 close · cash $9,723.23 · no lots left · equity $9,723.23. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,723.23 | ▲ 09:30 equity $9,723.23 vs yday $9,723.23 (+0.00) | 09:30 open · cash $9,723.23 · no holdings · equity $9,723.23 vs prior close $9,723.23 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,723.23 | ▲ close $9,723.23 vs 09:30 $9,723.23 (session +0.00) | 16:00 close · cash $9,723.23 · no lots left · equity $9,723.23. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,723.23 | ▲ 09:30 equity $9,723.23 vs yday $9,723.23 (+0.00) | 09:30 open · cash $9,723.23 · no holdings · equity $9,723.23 vs prior close $9,723.23 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 11 | $164.43 | $2.02 | — | $7,912.48 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1944.65 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 8 | $242.17 | $2.01 | — | $5,973.10 | — | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react; ret5=-11.1; leftover $1944.65 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 917 | $2.12 | $11.83 | — | $4,017.24 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1944.65 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 953 | $2.04 | $12.29 | — | $2,060.82 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1944.65 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 14 | $135.71 | $2.03 | — | $158.85 | — | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react; ret5=-9.2; leftover $1944.65 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.85 | ▼ close $9,529.64 vs 09:30 $9,723.23 (session -163.40) | 16:00 close · cash $158.85 · equity $9,529.64 vs 09:30 $9,723.23 (-193.59; session marks -163.40) · 5 name(s) marked open→close (per-name table). ORCL×11 09:30 $164.43 → close $150.28 -155.65; ADBE×8 09:30 $242.17 → close $252.23 +80.48; BAK×917 09:30 $2.12 → close $2.08 -36.68; AMTX×953 09:30 $2.04 → close $2.01 -28.59; RH×14 09:30 $135.71 → close $134.07 -22.96 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.85 | ▼ 09:30 equity $9,441.53 vs yday $9,529.64 (-88.11) | 09:30 open · cash $158.85 (unchanged overnight, no fees) · equity $9,441.53 vs prior close $9,529.64 (-88.11) · 5 name(s) re-marked at the open (per-name table). ORCL×11 yday $150.28 → 09:30 $141.42 -97.46; ADBE×8 yday $252.23 → 09:30 $261.51 +74.24; BAK×917 yday $2.08 → 09:30 $2.05 -27.51; AMTX×953 yday $2.01 → 09:30 $2.01 +0.00; RH×14 yday $134.07 → 09:30 $131.40 -37.38 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 11 | $141.42 | $2.05 | $-257.18 | $1,712.42 | ▼ -257.18 after sell → book $9,439.48; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 8 | $261.51 | $2.04 | $+150.67 | $3,802.46 | ▲ +150.67 after sell → book $9,437.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 917 | $2.05 | $12.00 | $-88.02 | $5,670.32 | ▼ -88.02 after sell → book $9,425.45; vs 09:30 mark -11.99 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 953 | $2.01 | $12.47 | $-53.35 | $7,573.38 | ▼ -53.35 after sell → book $9,412.98; vs 09:30 mark -12.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 14 | $131.40 | $2.06 | $-64.43 | $9,410.92 | ▼ -64.43 after sell → book $9,410.92; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,410.92 | ▲ close $9,410.92 vs 09:30 $9,441.53 (session +0.00) | 16:00 close · cash $9,410.92 · no lots left · equity $9,410.92. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,410.92 | ▲ 09:30 equity $9,410.92 vs yday $9,410.92 (+0.00) | 09:30 open · cash $9,410.92 · no holdings · equity $9,410.92 vs prior close $9,410.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,410.92 | ▲ close $9,410.92 vs 09:30 $9,410.92 (session +0.00) | 16:00 close · cash $9,410.92 · no lots left · equity $9,410.92. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,410.92 | ▲ 09:30 equity $9,410.92 vs yday $9,410.92 (+0.00) | 09:30 open · cash $9,410.92 · no holdings · equity $9,410.92 vs prior close $9,410.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 89 | $26.27 | $2.26 | — | $7,070.64 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=-7.4; leftover $2352.73 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `CLS` | 7 | $320.20 | $2.01 | — | $4,827.23 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+10.2; leftover $2352.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `GME` | 109 | $21.48 | $2.32 | — | $2,483.59 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+10.0; leftover $2352.73 | join🔴 sector🔴 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 338 | $6.95 | $4.36 | — | $130.13 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover,ohlc_hot; ret5=+16.3; leftover $2352.73 | join🔴 sector🟢 gen🟢 news🟢 digest🔴 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.13 | ▲ close $9,538.79 vs 09:30 $9,410.92 (session +138.81) | 16:00 close · cash $130.13 · equity $9,538.79 vs 09:30 $9,410.92 (+127.87; session marks +138.81) · 4 name(s) marked open→close (per-name table). WAY×89 09:30 $26.27 → close $26.59 +28.48; CLS×7 09:30 $320.20 → close $323.83 +25.41; GME×109 09:30 $21.48 → close $21.98 +54.50; SION×338 09:30 $6.95 → close $7.04 +30.42 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.13 | ▲ 09:30 equity $9,722.11 vs yday $9,538.79 (+183.32) | 09:30 open · cash $130.13 (unchanged overnight, no fees) · equity $9,722.11 vs prior close $9,538.79 (+183.32) · 4 name(s) re-marked at the open (per-name table). WAY×89 yday $26.59 → 09:30 $26.51 -7.12; CLS×7 yday $323.83 → 09:30 $337.75 +97.44; GME×109 yday $21.98 → 09:30 $22.12 +15.26; SION×338 yday $7.04 → 09:30 $7.27 +77.74 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 89 | $26.51 | $2.29 | $+16.81 | $2,487.23 | ▲ +16.81 after sell → book $9,719.82; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 3 | $170.85 | $2.00 | — | $1,972.68 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+23.5; leftover $621.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 2 | $238.60 | $2.00 | — | $1,493.48 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=+3.5; leftover $621.81 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 35 | $17.72 | $2.10 | — | $871.19 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=-16.3; leftover $621.81 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 351 | $1.77 | $4.53 | — | $245.39 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=+7.2; leftover $621.81 | join🔴 sector🟡 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $245.39 | ▼ close $9,703.25 vs 09:30 $9,722.11 (session -5.95) | 16:00 close · cash $245.39 · equity $9,703.25 vs 09:30 $9,722.11 (-18.86; session marks -5.95) · 7 name(s) marked open→close (per-name table). CLS×7 09:30 $337.75 → close $329.94 -54.67; GME×109 09:30 $22.12 → close $22.77 +70.85; SION×338 09:30 $7.27 → close $7.18 -30.42; SMTC×3 09:30 $170.85 → close $178.19 +22.02; JBHT×2 09:30 $238.60 → close $236.80 -3.60; TNDM×35 09:30 $17.72 → close $17.23 -17.15; BAK×351 09:30 $1.77 → close $1.79 +7.02 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $245.39 | ▲ 09:30 equity $9,713.88 vs yday $9,703.25 (+10.63) | 09:30 open · cash $245.39 (unchanged overnight, no fees) · equity $9,713.88 vs prior close $9,703.25 (+10.63) · 7 name(s) re-marked at the open (per-name table). CLS×7 yday $329.94 → 09:30 $332.06 +14.84; GME×109 yday $22.77 → 09:30 $22.90 +14.17; SION×338 yday $7.18 → 09:30 $7.12 -20.28; SMTC×3 yday $178.19 → 09:30 $182.33 +12.42; JBHT×2 yday $236.80 → 09:30 $236.80 +0.00; TNDM×35 yday $17.23 → 09:30 $17.13 -3.50; BAK×351 yday $1.79 → 09:30 $1.77 -7.02 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 3 | $182.33 | $2.02 | $+30.42 | $790.36 | ▲ +30.42 after sell → book $9,711.86; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 2 | $236.80 | $2.02 | $-7.61 | $1,261.94 | ▼ -7.61 after sell → book $9,709.84; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 35 | $17.13 | $2.12 | $-24.86 | $1,859.38 | ▼ -24.86 after sell → book $9,707.73; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 351 | $1.77 | $4.60 | $-9.12 | $2,476.05 | ▼ -9.12 after sell → book $9,703.13; vs 09:30 mark -4.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 55 | $14.79 | $2.15 | — | $1,660.45 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=-3.7; leftover $825.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 58 | $14.07 | $2.16 | — | $842.22 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=-19.9; leftover $825.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 109 | $7.54 | $2.32 | — | $18.59 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; 🔵; ret5=-2.6; leftover $825.35 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.59 | ▼ close $9,215.13 vs 09:30 $9,713.88 (session -481.36) | 16:00 close · cash $18.59 · equity $9,215.13 vs 09:30 $9,713.88 (-498.75; session marks -481.36) · 6 name(s) marked open→close (per-name table). CLS×7 09:30 $332.06 → close $332.63 +3.99; GME×109 09:30 $22.90 → close $22.64 -28.34; SION×338 09:30 $7.12 → close $5.96 -392.08; RARE×55 09:30 $14.79 → close $14.51 -15.40; BHVN×58 09:30 $14.07 → close $13.62 -26.10; FLNC×109 09:30 $7.54 → close $7.32 -23.43 | — |

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
| 2026-09-14 | `AVTR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CLS` | 7 | 2026-09-16 @ $320.20 | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+10.2; leftover $2352.73 |
| `GME` | 109 | 2026-09-16 @ $21.48 | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+10.0; leftover $2352.73 |
| `SION` | 338 | 2026-09-16 @ $6.95 | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover,ohlc_hot; ret5=+16.3; leftover $2352.73 |
| `RARE` | 55 | 2026-09-18 @ $14.79 | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=-3.7; leftover $825.35 |
| `BHVN` | 58 | 2026-09-18 @ $14.07 | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=-19.9; leftover $825.35 |
| `FLNC` | 109 | 2026-09-18 @ $7.54 | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; 🔵; ret5=-2.6; leftover $825.35 |
