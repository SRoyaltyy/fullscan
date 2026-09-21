# Factor mine action — `union_news_head_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · prior-export headline🟢 only

Cash book **-4.85%** ($9,515) · signal-only (no cash/fees) was +16.70%. Starts YES **5/27**. Fills 141 · skips 52 · realized $-225.48.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $84.49.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 94 | — | $13.18 | +0.00 | $13.92 | +69.56 | +69.56 | +0.00 | +69.56 |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `MH` | 92 | — | $13.55 | +0.00 | $13.10 | -41.40 | -41.40 | +0.00 | -41.40 |
| 2026-08-14 | `VELO` | 81 | — | $15.38 | +0.00 | $16.16 | +63.18 | +63.18 | +0.00 | +63.18 |
| 2026-08-14 | `S` | 52 | — | $23.77 | +0.00 | $23.11 | -34.58 | -34.58 | +0.00 | -34.58 |
| 2026-08-14 | `ZS` | 6 | — | $190.00 | +0.00 | $183.60 | -38.40 | -38.40 | +0.00 | -38.40 |
| 2026-08-17 | `HLIT` | 94 | $13.92 | $13.84 | -7.52 | — | +0.00 | -7.52 | +62.04 | — |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `MH` | 92 | $13.10 | $13.16 | +5.52 | — | +0.00 | +5.52 | -35.88 | — |
| 2026-08-17 | `VELO` | 81 | $16.16 | $16.05 | -8.91 | — | +0.00 | -8.91 | +54.27 | — |
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
| 2026-08-27 | `TRLV` | 171 | $11.43 | $11.38 | -8.55 | $11.03 | -59.85 | -68.40 | +27.36 | -32.49 |
| 2026-08-27 | `CAPR` | 232 | $9.36 | $9.19 | -39.44 | $10.06 | +201.84 | +162.40 | +208.80 | +410.64 |
| 2026-08-27 | `FWRD` | 110 | $17.63 | $17.60 | -3.30 | $17.70 | +11.00 | +7.70 | +20.90 | +31.90 |
| 2026-08-27 | `FLNC` | 173 | $11.08 | $11.52 | +76.12 | $11.43 | -15.57 | +60.55 | +69.20 | +53.63 |
| 2026-08-27 | `GEN` | 21 | — | $29.83 | +0.00 | $30.50 | +14.07 | +14.07 | +0.00 | +14.07 |
| 2026-08-27 | `SRRK` | 10 | — | $60.00 | +0.00 | $59.26 | -7.40 | -7.40 | +0.00 | -7.40 |
| 2026-08-28 | `TRLV` | 171 | $11.03 | $11.00 | -5.13 | — | +0.00 | -5.13 | -37.62 | — |
| 2026-08-28 | `CAPR` | 232 | $10.06 | $9.73 | -76.56 | $9.59 | -32.48 | -109.04 | +334.08 | +301.60 |
| 2026-08-28 | `FWRD` | 110 | $17.70 | $17.70 | +0.00 | — | +0.00 | +0.00 | +31.90 | — |
| 2026-08-28 | `FLNC` | 173 | $11.43 | $11.27 | -27.68 | — | +0.00 | -27.68 | +25.95 | — |
| 2026-08-28 | `GEN` | 21 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +14.07 | — |
| 2026-08-28 | `SRRK` | 10 | $59.26 | $58.75 | -5.10 | — | +0.00 | -5.10 | -12.50 | — |
| 2026-08-28 | `SMTC` | 7 | — | $141.76 | +0.00 | $131.17 | -74.13 | -74.13 | +0.00 | -74.13 |
| 2026-08-28 | `SEDG` | 33 | — | $32.90 | +0.00 | $31.41 | -49.17 | -49.17 | +0.00 | -49.17 |
| 2026-08-28 | `TLS` | 228 | — | $4.82 | +0.00 | $4.79 | -6.84 | -6.84 | +0.00 | -6.84 |
| 2026-08-28 | `TH` | 57 | — | $19.00 | +0.00 | $18.55 | -25.65 | -25.65 | +0.00 | -25.65 |
| 2026-08-28 | `OPTX` | 127 | — | $8.61 | +0.00 | $8.52 | -11.43 | -11.43 | +0.00 | -11.43 |
| 2026-08-28 | `BBWI` | 58 | — | $18.75 | +0.00 | $19.22 | +27.26 | +27.26 | +0.00 | +27.26 |
| 2026-08-28 | `ERAS` | 57 | — | $19.25 | +0.00 | $18.03 | -69.54 | -69.54 | +0.00 | -69.54 |
| 2026-08-31 | `CAPR` | 232 | $9.59 | $9.50 | -20.88 | — | +0.00 | -20.88 | +280.72 | — |
| 2026-08-31 | `SMTC` | 7 | $131.17 | $132.30 | +7.91 | — | +0.00 | +7.91 | -66.22 | — |
| 2026-08-31 | `SEDG` | 33 | $31.41 | $31.15 | -8.58 | — | +0.00 | -8.58 | -57.75 | — |
| 2026-08-31 | `TLS` | 228 | $4.79 | $4.81 | +4.56 | — | +0.00 | +4.56 | -2.28 | — |
| 2026-08-31 | `TH` | 57 | $18.55 | $18.12 | -24.23 | $18.52 | +22.51 | -1.72 | -49.88 | -27.36 |
| 2026-08-31 | `OPTX` | 127 | $8.52 | $8.52 | +0.00 | — | +0.00 | +0.00 | -11.43 | — |
| 2026-08-31 | `BBWI` | 58 | $19.22 | $19.25 | +1.74 | — | +0.00 | +1.74 | +29.00 | — |
| 2026-08-31 | `ERAS` | 57 | $18.03 | $17.87 | -9.12 | — | +0.00 | -9.12 | -78.66 | — |
| 2026-09-01 | `TH` | 57 | $18.52 | $18.45 | -3.99 | — | +0.00 | -3.99 | -31.35 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CXW` | 49 | — | $32.31 | +0.00 | $33.66 | +66.15 | +66.15 | +0.00 | +66.15 |
| 2026-09-03 | `FRNM` | 101 | — | $15.87 | +0.00 | $16.90 | +104.03 | +104.03 | +0.00 | +104.03 |
| 2026-09-03 | `MMED` | 67 | — | $23.88 | +0.00 | $23.84 | -2.68 | -2.68 | +0.00 | -2.68 |
| 2026-09-03 | `DE` | 2 | — | $703.25 | +0.00 | $694.41 | -17.68 | -17.68 | +0.00 | -17.68 |
| 2026-09-03 | `CNXC` | 48 | — | $32.88 | +0.00 | $32.85 | -1.44 | -1.44 | +0.00 | -1.44 |
| 2026-09-03 | `OPTX` | 211 | — | $7.59 | +0.00 | $7.76 | +35.87 | +35.87 | +0.00 | +35.87 |
| 2026-09-04 | `CXW` | 49 | $33.66 | $33.46 | -9.80 | — | +0.00 | -9.80 | +56.35 | — |
| 2026-09-04 | `FRNM` | 101 | $16.90 | $16.40 | -50.50 | $16.31 | -9.09 | -59.59 | +53.53 | +44.44 |
| 2026-09-04 | `MMED` | 67 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.68 | — |
| 2026-09-04 | `DE` | 2 | $694.41 | $692.03 | -4.76 | — | +0.00 | -4.76 | -22.44 | — |
| 2026-09-04 | `CNXC` | 48 | $32.85 | $32.48 | -17.76 | — | +0.00 | -17.76 | -19.20 | — |
| 2026-09-04 | `OPTX` | 211 | $7.76 | $7.79 | +6.33 | — | +0.00 | +6.33 | +42.20 | — |
| 2026-09-04 | `MRX` | 53 | — | $75.65 | +0.00 | $78.27 | +138.86 | +138.86 | +0.00 | +138.86 |
| 2026-09-04 | `BAK` | 2080 | — | $1.94 | +0.00 | $1.89 | -104.00 | -104.00 | +0.00 | -104.00 |
| 2026-09-08 | `FRNM` | 101 | $16.31 | $16.74 | +43.43 | — | +0.00 | +43.43 | +87.87 | — |
| 2026-09-08 | `MRX` | 53 | $78.27 | $78.84 | +30.21 | $76.71 | -112.89 | -82.68 | +169.07 | +56.18 |
| 2026-09-08 | `BAK` | 2080 | $1.89 | $1.94 | +104.00 | — | +0.00 | +104.00 | +0.00 | — |
| 2026-09-09 | `MRX` | 53 | $76.71 | $76.60 | -5.83 | — | +0.00 | -5.83 | +50.35 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 11 | — | $164.43 | +0.00 | $150.28 | -155.65 | -155.65 | +0.00 | -155.65 |
| 2026-09-11 | `ADBE` | 8 | — | $242.17 | +0.00 | $252.23 | +80.48 | +80.48 | +0.00 | +80.48 |
| 2026-09-11 | `BAK` | 920 | — | $2.12 | +0.00 | $2.08 | -36.80 | -36.80 | +0.00 | -36.80 |
| 2026-09-11 | `AMTX` | 956 | — | $2.04 | +0.00 | $2.01 | -28.68 | -28.68 | +0.00 | -28.68 |
| 2026-09-11 | `RH` | 14 | — | $135.71 | +0.00 | $134.07 | -22.96 | -22.96 | +0.00 | -22.96 |
| 2026-09-14 | `ORCL` | 11 | $150.28 | $141.42 | -97.46 | — | +0.00 | -97.46 | -253.11 | — |
| 2026-09-14 | `ADBE` | 8 | $252.23 | $261.51 | +74.24 | — | +0.00 | +74.24 | +154.72 | — |
| 2026-09-14 | `BAK` | 920 | $2.08 | $2.05 | -27.60 | — | +0.00 | -27.60 | -64.40 | — |
| 2026-09-14 | `AMTX` | 956 | $2.01 | $2.01 | +0.00 | — | +0.00 | +0.00 | -28.68 | — |
| 2026-09-14 | `RH` | 14 | $134.07 | $131.40 | -37.38 | — | +0.00 | -37.38 | -60.34 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 119 | — | $26.27 | +0.00 | $26.59 | +38.08 | +38.08 | +0.00 | +38.08 |
| 2026-09-16 | `CLS` | 9 | — | $320.20 | +0.00 | $323.83 | +32.67 | +32.67 | +0.00 | +32.67 |
| 2026-09-16 | `SION` | 452 | — | $6.95 | +0.00 | $7.04 | +40.68 | +40.68 | +0.00 | +40.68 |
| 2026-09-17 | `WAY` | 119 | $26.59 | $26.51 | -9.52 | — | +0.00 | -9.52 | +28.56 | — |
| 2026-09-17 | `CLS` | 9 | $323.83 | $337.75 | +125.28 | $329.94 | -70.29 | +54.99 | +157.95 | +87.66 |
| 2026-09-17 | `SION` | 452 | $7.04 | $7.27 | +103.96 | — | +0.00 | +103.96 | +144.64 | — |
| 2026-09-17 | `SMTC` | 7 | — | $170.85 | +0.00 | $178.19 | +51.38 | +51.38 | +0.00 | +51.38 |
| 2026-09-17 | `GME` | 60 | — | $22.12 | +0.00 | $22.77 | +39.00 | +39.00 | +0.00 | +39.00 |
| 2026-09-17 | `JBHT` | 5 | — | $238.60 | +0.00 | $236.80 | -9.00 | -9.00 | +0.00 | -9.00 |
| 2026-09-17 | `TNDM` | 75 | — | $17.72 | +0.00 | $17.23 | -36.75 | -36.75 | +0.00 | -36.75 |
| 2026-09-17 | `BAK` | 758 | — | $1.77 | +0.00 | $1.79 | +15.16 | +15.16 | +0.00 | +15.16 |
| 2026-09-18 | `CLS` | 9 | $329.94 | $332.06 | +19.08 | $332.63 | +5.13 | +24.21 | +106.74 | +111.87 |
| 2026-09-18 | `SMTC` | 7 | $178.19 | $182.33 | +28.98 | — | +0.00 | +28.98 | +80.36 | — |
| 2026-09-18 | `GME` | 60 | $22.77 | $22.90 | +7.80 | $22.64 | -15.60 | -7.80 | +46.80 | +31.20 |
| 2026-09-18 | `JBHT` | 5 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -9.00 | — |
| 2026-09-18 | `TNDM` | 75 | $17.23 | $17.13 | -7.50 | — | +0.00 | -7.50 | -44.25 | — |
| 2026-09-18 | `BAK` | 758 | $1.79 | $1.77 | -15.16 | — | +0.00 | -15.16 | +0.00 | — |
| 2026-09-18 | `TH` | 64 | — | $20.91 | +0.00 | $21.19 | +17.92 | +17.92 | +0.00 | +17.92 |
| 2026-09-18 | `RARE` | 90 | — | $14.79 | +0.00 | $14.51 | -25.20 | -25.20 | +0.00 | -25.20 |
| 2026-09-18 | `BHVN` | 95 | — | $14.07 | +0.00 | $13.62 | -42.75 | -42.75 | +0.00 | -42.75 |
| 2026-09-18 | `FLNC` | 178 | — | $7.54 | +0.00 | $7.32 | -38.27 | -38.27 | +0.00 | -38.27 |
| 2026-09-21 | `CLS` | 9 | $332.63 | $341.45 | +79.38 | — | +0.00 | +79.38 | +191.25 | — |
| 2026-09-21 | `GME` | 60 | $22.64 | $22.78 | +8.40 | — | +0.00 | +8.40 | +39.60 | — |
| 2026-09-21 | `TH` | 64 | $21.19 | $21.55 | +23.04 | — | +0.00 | +23.04 | +40.96 | — |
| 2026-09-21 | `RARE` | 90 | $14.51 | $14.60 | +8.10 | — | +0.00 | +8.10 | -17.10 | — |
| 2026-09-21 | `BHVN` | 95 | $13.62 | $13.90 | +26.60 | — | +0.00 | +26.60 | -16.15 | — |
| 2026-09-21 | `FLNC` | 178 | $7.32 | $7.36 | +7.12 | — | +0.00 | +7.12 | -31.15 | — |
| 2026-09-21 | `VICR` | 6 | — | $230.25 | +0.00 | $223.90 | -38.10 | -38.10 | +0.00 | -38.10 |
| 2026-09-21 | `SMTC` | 7 | — | $190.30 | +0.00 | $177.37 | -90.51 | -90.51 | +0.00 | -90.51 |
| 2026-09-21 | `ALVO` | 235 | — | $5.92 | +0.00 | $5.88 | -9.40 | -9.40 | +0.00 | -9.40 |
| 2026-09-21 | `SION` | 233 | — | $5.99 | +0.00 | $6.00 | +2.33 | +2.33 | +0.00 | +2.33 |
| 2026-09-21 | `GLXY` | 53 | — | $25.95 | +0.00 | $26.07 | +6.36 | +6.36 | +0.00 | +6.36 |
| 2026-09-21 | `MARA` | 100 | — | $13.96 | +0.00 | $13.28 | -68.00 | -68.00 | +0.00 | -68.00 |
| 2026-09-21 | `AMTX` | 649 | — | $2.15 | +0.00 | $2.09 | -38.94 | -38.94 | +0.00 | -38.94 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +36.39 | HLIT, ANGX, ARX, MH, VELO, S, ZS | — | $1,392.75 | $10,019.55 | HLIT×94, ANGX×290, ARX×63, MH×92, VELO×81, S×52, ZS×6 |
| 2026-08-17 | +2.25 | $1,392.75 | HLIT×94, ANGX×290, ARX×63, MH×92, VELO×81, S×52, ZS×6 | $10,071.64 | +52.09 | -118.44 | OUST, CELC | HLIT, ANGX, ARX, MH, VELO, S, ZS | $30.69 | $9,931.71 | OUST×102, CELC×54 |
| 2026-08-18 | -6.20 | $30.69 | OUST×102, CELC×54 | $9,618.39 | -313.32 | +0.00 | — | OUST, CELC | $9,613.84 | $9,613.84 | — |
| 2026-08-19 | -7.20 | $9,613.84 | — | $9,613.84 | -0.00 | +0.00 | — | — | $9,613.84 | $9,613.84 | — |
| 2026-08-20 | +1.12 | $9,613.84 | — | $9,613.84 | -0.00 | -170.67 | BHP, AUTL, CRSP, ASST, MRNA, ZLAB, TEAM, HUMA | — | $178.50 | $9,407.34 | BHP×13, AUTL×486, CRSP×20, ASST×75, MRNA×8, ZLAB×45, TEAM×6, HUMA×1699 |
| 2026-08-21 | +3.25 | $178.50 | BHP×13, AUTL×486, CRSP×20, ASST×75, MRNA×8, ZLAB×45, TEAM×6, HUMA×1699 | $9,578.76 | +171.42 | -131.37 | FUTU, GRAL, ABTC, HIVE, MARA, BTDR | BHP, ASST, MRNA, ZLAB, TEAM, HUMA | $58.19 | $9,404.25 | AUTL×486, CRSP×20, FUTU×10, GRAL×15, ABTC×137, HIVE×368, MARA×101, BTDR×107 |
| 2026-08-24 | -5.17 | $58.19 | AUTL×486, CRSP×20, FUTU×10, GRAL×15, ABTC×137, HIVE×368, MARA×101, BTDR×107 | $9,390.49 | -13.76 | -33.50 | — | AUTL, FUTU, GRAL, ABTC, HIVE, MARA, BTDR | $8,193.13 | $9,334.63 | CRSP×20 |
| 2026-08-25 | +1.80 | $8,193.13 | CRSP×20 | $9,351.73 | +17.10 | +468.17 | EZPW, RUM, ZYME, REAX, EOLS | CRSP | $45.36 | $9,805.93 | EZPW×53, RUM×198, ZYME×64, REAX×77, EOLS×214 |
| 2026-08-26 | +2.02 | $45.36 | EZPW×53, RUM×198, ZYME×64, REAX×77, EOLS×214 | $9,640.17 | -165.76 | +303.88 | FNV, TRLV, CAPR, FWRD, FLNC | EZPW, RUM, ZYME, REAX, EOLS | $65.86 | $9,919.64 | FNV×7, TRLV×171, CAPR×232, FWRD×110, FLNC×173 |
| 2026-08-27 | — | $65.86 | FNV×7, TRLV×171, CAPR×232, FWRD×110, FLNC×173 | $9,943.49 | +23.85 | +144.09 | GEN, SRRK | FNV | $703.93 | $10,081.47 | TRLV×171, CAPR×232, FWRD×110, FLNC×173, GEN×21, SRRK×10 |
| 2026-08-28 | +0.75 | $703.93 | TRLV×171, CAPR×232, FWRD×110, FLNC×173, GEN×21, SRRK×10 | $9,967.00 | -114.47 | -241.98 | SMTC, SEDG, TLS, TH, OPTX, BBWI, ERAS | TRLV, FWRD, FLNC, GEN, SRRK | $143.98 | $9,697.56 | CAPR×232, SMTC×7, SEDG×33, TLS×228, TH×57, OPTX×127, BBWI×58, ERAS×57 |
| 2026-08-31 | -5.85 | $143.98 | CAPR×232, SMTC×7, SEDG×33, TLS×228, TH×57, OPTX×127, BBWI×58, ERAS×57 | $9,648.96 | -48.60 | +22.51 | — | CAPR, SMTC, SEDG, TLS, OPTX, BBWI, ERAS | $8,598.89 | $9,654.53 | TH×57 |
| 2026-09-01 | -6.30 | $8,598.89 | TH×57 | $9,650.54 | -3.99 | +0.00 | — | TH | $9,648.36 | $9,648.36 | — |
| 2026-09-02 | -3.83 | $9,648.36 | — | $9,648.36 | +0.00 | +0.00 | — | — | $9,648.36 | $9,648.36 | — |
| 2026-09-03 | -0.90 | $9,648.36 | — | $9,648.36 | +0.00 | +184.25 | CXW, FRNM, MMED, DE, CNXC, OPTX | — | $262.64 | $9,819.14 | CXW×49, FRNM×101, MMED×67, DE×2, CNXC×48, OPTX×211 |
| 2026-09-04 | +2.25 | $262.64 | CXW×49, FRNM×101, MMED×67, DE×2, CNXC×48, OPTX×211 | $9,742.65 | -76.49 | +25.77 | MRX, BAK | CXW, MMED, DE, CNXC, OPTX | $1.30 | $9,728.12 | FRNM×101, MRX×53, BAK×2080 |
| 2026-09-08 | -11.47 | $1.30 | FRNM×101, MRX×53, BAK×2080 | $9,905.76 | +177.64 | -112.89 | — | FRNM, BAK | $5,697.70 | $9,763.33 | MRX×53 |
| 2026-09-09 | -13.95 | $5,697.70 | MRX×53 | $9,757.50 | -5.83 | +0.00 | — | MRX | $9,755.31 | $9,755.31 | — |
| 2026-09-10 | -13.28 | $9,755.31 | — | $9,755.31 | +0.00 | +0.00 | — | — | $9,755.31 | $9,755.31 | — |
| 2026-09-11 | +0.50 | $9,755.31 | — | $9,755.31 | +0.00 | -163.61 | ORCL, ADBE, BAK, AMTX, RH | — | $178.37 | $9,561.43 | ORCL×11, ADBE×8, BAK×920, AMTX×956, RH×14 |
| 2026-09-14 | -11.00 | $178.37 | ORCL×11, ADBE×8, BAK×920, AMTX×956, RH×14 | $9,473.23 | -88.20 | +0.00 | — | ORCL, ADBE, BAK, AMTX, RH | $9,442.55 | $9,442.55 | — |
| 2026-09-15 | -3.84 | $9,442.55 | — | $9,442.55 | -0.00 | +0.00 | — | — | $9,442.55 | $9,442.55 | — |
| 2026-09-16 | +5.30 | $9,442.55 | — | $9,442.55 | -0.00 | +111.43 | WAY, CLS, SION | — | $283.02 | $9,543.78 | WAY×119, CLS×9, SION×452 |
| 2026-09-17 | +7.38 | $283.02 | WAY×119, CLS×9, SION×452 | $9,763.50 | +219.72 | -10.50 | SMTC, GME, JBHT, TNDM, BAK | WAY, SION | $310.44 | $9,726.50 | CLS×9, SMTC×7, GME×60, JBHT×5, TNDM×75, BAK×758 |
| 2026-09-18 | +4.86 | $310.44 | CLS×9, SMTC×7, GME×60, JBHT×5, TNDM×75, BAK×758 | $9,759.70 | +33.20 | -98.77 | TH, RARE, BHVN, FLNC | SMTC, JBHT, TNDM, BAK | $24.49 | $9,635.48 | CLS×9, GME×60, TH×64, RARE×90, BHVN×95, FLNC×178 |
| 2026-09-21 | +12.87 | $24.49 | CLS×9, GME×60, TH×64, RARE×90, BHVN×95, FLNC×178 | $9,788.12 | +152.64 | -236.26 | VICR, SMTC, ALVO, SION, GLXY, MARA, AMTX | CLS, GME, TH, RARE, BHVN, FLNC | $84.49 | $9,515.40 | VICR×6, SMTC×7, ALVO×235, SION×233, GLXY×53, MARA×100, AMTX×649 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $6,270.08 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $3,773.20 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $2,534.75 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZS` | 6 | $190.00 | $2.01 | — | $1,392.75 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+15.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,392.75 | ▲ close $10,019.55 vs 09:30 $10,000.00 (session +36.39) | 16:00 close · cash $1,392.75 · equity $10,019.55 vs 09:30 $10,000.00 (+19.55; session marks +36.39) · 7 name(s) marked open→close (per-name table). HLIT×94 09:30 $13.18 → close $13.92 +69.56; ANGX×290 09:30 $4.31 → close $4.37 +17.40; ARX×63 09:30 $19.57 → close $19.58 +0.63; MH×92 09:30 $13.55 → close $13.10 -41.40; VELO×81 09:30 $15.38 → close $16.16 +63.18; S×52 09:30 $23.77 → close $23.11 -34.58; ZS×6 09:30 $190.00 → close $183.60 -38.40 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,392.75 | ▲ 09:30 equity $10,071.64 vs yday $10,019.55 (+52.09) | 09:30 open · cash $1,392.75 (unchanged overnight, no fees) · equity $10,071.64 vs prior close $10,019.55 (+52.09) · 7 name(s) re-marked at the open (per-name table). HLIT×94 yday $13.92 → 09:30 $13.84 -7.52; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; MH×92 yday $13.10 → 09:30 $13.16 +5.52; VELO×81 yday $16.16 → 09:30 $16.05 -8.91; S×52 yday $23.11 → 09:30 $22.50 -31.72; ZS×6 yday $183.60 → 09:30 $188.38 +28.65 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $2,691.41 | ▲ +57.47 after sell → book $10,069.34; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $4,021.61 | ▲ +76.56 after sell → book $10,065.54; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,252.32 | ▼ -4.38 after sell → book $10,063.34; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $6,460.75 | ▼ -40.44 after sell → book $10,061.05; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 81 | $16.05 | $2.26 | $+49.78 | $7,758.54 | ▲ +49.78 after sell → book $10,058.79; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟡 |
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
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 21 | $29.83 | $2.05 | — | $1,305.95 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $644.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 10 | $60.00 | $2.02 | — | $703.93 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+6.2; leftover $644.81 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $703.93 | ▲ close $10,081.47 vs 09:30 $9,943.49 (session +144.09) | 16:00 close · cash $703.93 · equity $10,081.47 vs 09:30 $9,943.49 (+137.98; session marks +144.09) · 6 name(s) marked open→close (per-name table). TRLV×171 09:30 $11.38 → close $11.03 -59.85; CAPR×232 09:30 $9.19 → close $10.06 +201.84; FWRD×110 09:30 $17.60 → close $17.70 +11.00; FLNC×173 09:30 $11.52 → close $11.43 -15.57; GEN×21 09:30 $29.83 → close $30.50 +14.07; SRRK×10 09:30 $60.00 → close $59.26 -7.40 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $703.93 | ▼ 09:30 equity $9,967.00 vs yday $10,081.47 (-114.47) | 09:30 open · cash $703.93 (unchanged overnight, no fees) · equity $9,967.00 vs prior close $10,081.47 (-114.47) · 6 name(s) re-marked at the open (per-name table). TRLV×171 yday $11.03 → 09:30 $11.00 -5.13; CAPR×232 yday $10.06 → 09:30 $9.73 -76.56; FWRD×110 yday $17.70 → 09:30 $17.70 +0.00; FLNC×173 yday $11.43 → 09:30 $11.27 -27.68; GEN×21 yday $30.50 → 09:30 $30.50 +0.00; SRRK×10 yday $59.26 → 09:30 $58.75 -5.10 | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 171 | $11.00 | $2.55 | $-42.67 | $2,582.38 | ▼ -42.67 after sell → book $9,964.45; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWRD` | 110 | $17.70 | $2.35 | $+27.23 | $4,527.03 | ▲ +27.23 after sell → book $9,962.10; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 173 | $11.27 | $2.55 | $+20.89 | $6,474.19 | ▲ +20.89 after sell → book $9,959.55; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 21 | $30.50 | $2.07 | $+9.94 | $7,112.61 | ▲ +9.94 after sell → book $9,957.47; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 10 | $58.75 | $2.04 | $-16.56 | $7,698.07 | ▼ -16.56 after sell → book $9,955.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $6,703.74 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1099.72 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 33 | $32.90 | $2.09 | — | $5,615.95 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1099.72 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 228 | $4.82 | $2.94 | — | $4,514.05 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1099.72 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 57 | $19.00 | $2.16 | — | $3,428.89 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+7.5; leftover $1099.72 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 127 | $8.61 | $2.37 | — | $2,333.05 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; ret5=-0.7; leftover $1099.72 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 58 | $18.75 | $2.16 | — | $1,243.39 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=-5.0; leftover $1099.72 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 57 | $19.25 | $2.16 | — | $143.98 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=+14.1; leftover $1099.72 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $143.98 | ▼ close $9,697.56 vs 09:30 $9,967.00 (session -241.98) | 16:00 close · cash $143.98 · equity $9,697.56 vs 09:30 $9,967.00 (-269.44; session marks -241.98) · 8 name(s) marked open→close (per-name table). CAPR×232 09:30 $9.73 → close $9.59 -32.48; SMTC×7 09:30 $141.76 → close $131.17 -74.13; SEDG×33 09:30 $32.90 → close $31.41 -49.17; TLS×228 09:30 $4.82 → close $4.79 -6.84; TH×57 09:30 $19.00 → close $18.55 -25.65; OPTX×127 09:30 $8.61 → close $8.52 -11.43; BBWI×58 09:30 $18.75 → close $19.22 +27.26; ERAS×57 09:30 $19.25 → close $18.03 -69.54 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $143.98 | ▼ 09:30 equity $9,648.96 vs yday $9,697.56 (-48.60) | 09:30 open · cash $143.98 (unchanged overnight, no fees) · equity $9,648.96 vs prior close $9,697.56 (-48.60) · 8 name(s) re-marked at the open (per-name table). CAPR×232 yday $9.59 → 09:30 $9.50 -20.88; SMTC×7 yday $131.17 → 09:30 $132.30 +7.91; SEDG×33 yday $31.41 → 09:30 $31.15 -8.58; TLS×228 yday $4.79 → 09:30 $4.81 +4.56; TH×57 yday $18.55 → 09:30 $18.12 -24.23; OPTX×127 yday $8.52 → 09:30 $8.52 +0.00; BBWI×58 yday $19.22 → 09:30 $19.25 +1.74; ERAS×57 yday $18.03 → 09:30 $17.87 -9.12 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 232 | $9.50 | $3.05 | $+274.68 | $2,344.93 | ▲ +274.68 after sell → book $9,645.91; vs 09:30 mark -3.05 | dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 7 | $132.30 | $2.03 | $-70.26 | $3,269.00 | ▼ -70.26 after sell → book $9,643.88; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 33 | $31.15 | $2.11 | $-61.95 | $4,294.84 | ▼ -61.95 after sell → book $9,641.77; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 228 | $4.81 | $2.99 | $-8.21 | $5,388.53 | ▼ -8.21 after sell → book $9,638.78; vs 09:30 mark -2.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 127 | $8.52 | $2.40 | $-16.20 | $6,468.17 | ▼ -16.20 after sell → book $9,636.38; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 58 | $19.25 | $2.18 | $+24.65 | $7,582.48 | ▲ +24.65 after sell → book $9,634.20; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 57 | $17.87 | $2.18 | $-83.00 | $8,598.89 | ▼ -83.00 after sell → book $9,632.02; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,598.89 | ▲ close $9,654.53 vs 09:30 $9,648.96 (session +22.51) | 16:00 close · cash $8,598.89 · equity $9,654.53 vs 09:30 $9,648.96 (+5.57; session marks +22.51) · 1 name(s) marked open→close (per-name table). TH×57 09:30 $18.12 → close $18.52 +22.51 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,598.89 | ▼ 09:30 equity $9,650.54 vs yday $9,654.53 (-3.99) | 09:30 open · cash $8,598.89 (unchanged overnight, no fees) · equity $9,650.54 vs prior close $9,654.53 (-3.99) · 1 name(s) re-marked at the open (per-name table). TH×57 yday $18.52 → 09:30 $18.45 -3.99 | — |
| 2026-09-01 09:30 ET | **SELL** | `TH` | 57 | $18.45 | $2.18 | $-35.69 | $9,648.36 | ▼ -35.69 after sell → book $9,648.36; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,648.36 | ▲ close $9,648.36 vs 09:30 $9,650.54 (session +0.00) | 16:00 close · cash $9,648.36 · no lots left · equity $9,648.36. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,648.36 | ▲ 09:30 equity $9,648.36 vs yday $9,648.36 (+0.00) | 09:30 open · cash $9,648.36 · no holdings · equity $9,648.36 vs prior close $9,648.36 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,648.36 | ▲ close $9,648.36 vs 09:30 $9,648.36 (session +0.00) | 16:00 close · cash $9,648.36 · no lots left · equity $9,648.36. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,648.36 | ▲ 09:30 equity $9,648.36 vs yday $9,648.36 (+0.00) | 09:30 open · cash $9,648.36 · no holdings · equity $9,648.36 vs prior close $9,648.36 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 49 | $32.31 | $2.14 | — | $8,063.03 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1608.06 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 101 | $15.87 | $2.29 | — | $6,457.87 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1608.06 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 67 | $23.88 | $2.19 | — | $4,855.72 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1608.06 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $3,447.22 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1608.06 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 48 | $32.88 | $2.13 | — | $1,866.85 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1608.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 211 | $7.59 | $2.72 | — | $262.64 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-11.5; leftover $1608.06 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $262.64 | ▲ close $9,819.14 vs 09:30 $9,648.36 (session +184.25) | 16:00 close · cash $262.64 · equity $9,819.14 vs 09:30 $9,648.36 (+170.78; session marks +184.25) · 6 name(s) marked open→close (per-name table). CXW×49 09:30 $32.31 → close $33.66 +66.15; FRNM×101 09:30 $15.87 → close $16.90 +104.03; MMED×67 09:30 $23.88 → close $23.84 -2.68; DE×2 09:30 $703.25 → close $694.41 -17.68; CNXC×48 09:30 $32.88 → close $32.85 -1.44; OPTX×211 09:30 $7.59 → close $7.76 +35.87 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $262.64 | ▼ 09:30 equity $9,742.65 vs yday $9,819.14 (-76.49) | 09:30 open · cash $262.64 (unchanged overnight, no fees) · equity $9,742.65 vs prior close $9,819.14 (-76.49) · 6 name(s) re-marked at the open (per-name table). CXW×49 yday $33.66 → 09:30 $33.46 -9.80; FRNM×101 yday $16.90 → 09:30 $16.40 -50.50; MMED×67 yday $23.84 → 09:30 $23.84 +0.00; DE×2 yday $694.41 → 09:30 $692.03 -4.76; CNXC×48 yday $32.85 → 09:30 $32.48 -17.76; OPTX×211 yday $7.76 → 09:30 $7.79 +6.33 | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 49 | $33.46 | $2.16 | $+52.05 | $1,900.02 | ▲ +52.05 after sell → book $9,740.49; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 67 | $23.84 | $2.21 | $-7.09 | $3,495.08 | ▼ -7.09 after sell → book $9,738.27; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 2 | $692.03 | $2.02 | $-26.45 | $4,877.13 | ▼ -26.45 after sell → book $9,736.26; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 48 | $32.48 | $2.16 | $-23.49 | $6,434.01 | ▼ -23.49 after sell → book $9,734.10; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 211 | $7.79 | $2.77 | $+36.71 | $8,074.93 | ▲ +36.71 after sell → book $9,731.33; vs 09:30 mark -2.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 53 | $75.65 | $2.15 | — | $4,063.33 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $4037.46 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 2080 | $1.94 | $26.83 | — | $1.30 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $4037.46 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.30 | ▲ close $9,728.12 vs 09:30 $9,742.65 (session +25.77) | 16:00 close · cash $1.30 · equity $9,728.12 vs 09:30 $9,742.65 (-14.53; session marks +25.77) · 3 name(s) marked open→close (per-name table). FRNM×101 09:30 $16.40 → close $16.31 -9.09; MRX×53 09:30 $75.65 → close $78.27 +138.86; BAK×2080 09:30 $1.94 → close $1.89 -104.00 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.30 | ▲ 09:30 equity $9,905.76 vs yday $9,728.12 (+177.64) | 09:30 open · cash $1.30 (unchanged overnight, no fees) · equity $9,905.76 vs prior close $9,728.12 (+177.64) · 3 name(s) re-marked at the open (per-name table). FRNM×101 yday $16.31 → 09:30 $16.74 +43.43; MRX×53 yday $78.27 → 09:30 $78.84 +30.21; BAK×2080 yday $1.89 → 09:30 $1.94 +104.00 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 101 | $16.74 | $2.32 | $+83.25 | $1,689.71 | ▲ +83.25 after sell → book $9,903.43; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 2080 | $1.94 | $27.21 | $-54.04 | $5,697.70 | ▼ -54.04 after sell → book $9,876.22; vs 09:30 mark -27.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,697.70 | ▼ close $9,763.33 vs 09:30 $9,905.76 (session -112.89) | 16:00 close · cash $5,697.70 · equity $9,763.33 vs 09:30 $9,905.76 (-142.43; session marks -112.89) · 1 name(s) marked open→close (per-name table). MRX×53 09:30 $78.84 → close $76.71 -112.89 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,697.70 | ▼ 09:30 equity $9,757.50 vs yday $9,763.33 (-5.83) | 09:30 open · cash $5,697.70 (unchanged overnight, no fees) · equity $9,757.50 vs prior close $9,763.33 (-5.83) · 1 name(s) re-marked at the open (per-name table). MRX×53 yday $76.71 → 09:30 $76.60 -5.83 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 53 | $76.60 | $2.19 | $+46.01 | $9,755.31 | ▲ +46.01 after sell → book $9,755.31; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,755.31 | ▲ close $9,755.31 vs 09:30 $9,757.50 (session +0.00) | 16:00 close · cash $9,755.31 · no lots left · equity $9,755.31. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,755.31 | ▲ 09:30 equity $9,755.31 vs yday $9,755.31 (+0.00) | 09:30 open · cash $9,755.31 · no holdings · equity $9,755.31 vs prior close $9,755.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,755.31 | ▲ close $9,755.31 vs 09:30 $9,755.31 (session +0.00) | 16:00 close · cash $9,755.31 · no lots left · equity $9,755.31. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,755.31 | ▲ 09:30 equity $9,755.31 vs yday $9,755.31 (+0.00) | 09:30 open · cash $9,755.31 · no holdings · equity $9,755.31 vs prior close $9,755.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 11 | $164.43 | $2.02 | — | $7,944.56 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1951.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 8 | $242.17 | $2.01 | — | $6,005.19 | — | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react; ret5=-11.1; leftover $1951.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 920 | $2.12 | $11.87 | — | $4,042.92 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1951.06 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 956 | $2.04 | $12.33 | — | $2,080.35 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1951.06 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 14 | $135.71 | $2.03 | — | $178.37 | — | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react; ret5=-9.2; leftover $1951.06 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.37 | ▼ close $9,561.43 vs 09:30 $9,755.31 (session -163.61) | 16:00 close · cash $178.37 · equity $9,561.43 vs 09:30 $9,755.31 (-193.88; session marks -163.61) · 5 name(s) marked open→close (per-name table). ORCL×11 09:30 $164.43 → close $150.28 -155.65; ADBE×8 09:30 $242.17 → close $252.23 +80.48; BAK×920 09:30 $2.12 → close $2.08 -36.80; AMTX×956 09:30 $2.04 → close $2.01 -28.68; RH×14 09:30 $135.71 → close $134.07 -22.96 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.37 | ▼ 09:30 equity $9,473.23 vs yday $9,561.43 (-88.20) | 09:30 open · cash $178.37 (unchanged overnight, no fees) · equity $9,473.23 vs prior close $9,561.43 (-88.20) · 5 name(s) re-marked at the open (per-name table). ORCL×11 yday $150.28 → 09:30 $141.42 -97.46; ADBE×8 yday $252.23 → 09:30 $261.51 +74.24; BAK×920 yday $2.08 → 09:30 $2.05 -27.60; AMTX×956 yday $2.01 → 09:30 $2.01 +0.00; RH×14 yday $134.07 → 09:30 $131.40 -37.38 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 11 | $141.42 | $2.05 | $-257.18 | $1,731.95 | ▼ -257.18 after sell → book $9,471.19; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 8 | $261.51 | $2.04 | $+150.67 | $3,821.99 | ▲ +150.67 after sell → book $9,469.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 920 | $2.05 | $12.04 | $-88.30 | $5,695.95 | ▼ -88.30 after sell → book $9,457.11; vs 09:30 mark -12.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 956 | $2.01 | $12.51 | $-53.52 | $7,605.01 | ▼ -53.52 after sell → book $9,444.61; vs 09:30 mark -12.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 14 | $131.40 | $2.06 | $-64.43 | $9,442.55 | ▼ -64.43 after sell → book $9,442.55; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,442.55 | ▲ close $9,442.55 vs 09:30 $9,473.23 (session +0.00) | 16:00 close · cash $9,442.55 · no lots left · equity $9,442.55. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,442.55 | ▲ 09:30 equity $9,442.55 vs yday $9,442.55 (-0.00) | 09:30 open · cash $9,442.55 · no holdings · equity $9,442.55 vs prior close $9,442.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,442.55 | ▲ close $9,442.55 vs 09:30 $9,442.55 (session +0.00) | 16:00 close · cash $9,442.55 · no lots left · equity $9,442.55. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,442.55 | ▲ 09:30 equity $9,442.55 vs yday $9,442.55 (-0.00) | 09:30 open · cash $9,442.55 · no holdings · equity $9,442.55 vs prior close $9,442.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 119 | $26.27 | $2.35 | — | $6,314.07 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $3147.52 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `CLS` | 9 | $320.20 | $2.02 | — | $3,430.25 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+10.2; leftover $3147.52 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 452 | $6.95 | $5.83 | — | $283.02 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-5.8; leftover $3147.52 | join🔴 sector🟢 gen🟢 news🟢 digest🔴 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $283.02 | ▲ close $9,543.78 vs 09:30 $9,442.55 (session +111.43) | 16:00 close · cash $283.02 · equity $9,543.78 vs 09:30 $9,442.55 (+101.23; session marks +111.43) · 3 name(s) marked open→close (per-name table). WAY×119 09:30 $26.27 → close $26.59 +38.08; CLS×9 09:30 $320.20 → close $323.83 +32.67; SION×452 09:30 $6.95 → close $7.04 +40.68 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $283.02 | ▲ 09:30 equity $9,763.50 vs yday $9,543.78 (+219.72) | 09:30 open · cash $283.02 (unchanged overnight, no fees) · equity $9,763.50 vs prior close $9,543.78 (+219.72) · 3 name(s) re-marked at the open (per-name table). WAY×119 yday $26.59 → 09:30 $26.51 -9.52; CLS×9 yday $323.83 → 09:30 $337.75 +125.28; SION×452 yday $7.04 → 09:30 $7.27 +103.96 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 119 | $26.51 | $2.39 | $+23.82 | $3,435.32 | ▲ +23.82 after sell → book $9,761.11; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 452 | $7.27 | $5.93 | $+132.88 | $6,715.43 | ▲ +132.88 after sell → book $9,755.18; vs 09:30 mark -5.93 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $5,517.47 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1343.09 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 60 | $22.12 | $2.17 | — | $4,188.10 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1343.09 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 5 | $238.60 | $2.00 | — | $2,993.09 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-11.6; leftover $1343.09 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 75 | $17.72 | $2.21 | — | $1,661.88 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=-8.3; leftover $1343.09 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 758 | $1.77 | $9.78 | — | $310.44 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-10.2; leftover $1343.09 | join🔴 sector🟡 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $310.44 | ▼ close $9,726.50 vs 09:30 $9,763.50 (session -10.50) | 16:00 close · cash $310.44 · equity $9,726.50 vs 09:30 $9,763.50 (-37.00; session marks -10.50) · 6 name(s) marked open→close (per-name table). CLS×9 09:30 $337.75 → close $329.94 -70.29; SMTC×7 09:30 $170.85 → close $178.19 +51.38; GME×60 09:30 $22.12 → close $22.77 +39.00; JBHT×5 09:30 $238.60 → close $236.80 -9.00; TNDM×75 09:30 $17.72 → close $17.23 -36.75; BAK×758 09:30 $1.77 → close $1.79 +15.16 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $310.44 | ▲ 09:30 equity $9,759.70 vs yday $9,726.50 (+33.20) | 09:30 open · cash $310.44 (unchanged overnight, no fees) · equity $9,759.70 vs prior close $9,726.50 (+33.20) · 6 name(s) re-marked at the open (per-name table). CLS×9 yday $329.94 → 09:30 $332.06 +19.08; SMTC×7 yday $178.19 → 09:30 $182.33 +28.98; GME×60 yday $22.77 → 09:30 $22.90 +7.80; JBHT×5 yday $236.80 → 09:30 $236.80 +0.00; TNDM×75 yday $17.23 → 09:30 $17.13 -7.50; BAK×758 yday $1.79 → 09:30 $1.77 -15.16 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $1,584.72 | ▲ +76.32 after sell → book $9,757.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 5 | $236.80 | $2.02 | $-13.03 | $2,766.69 | ▼ -13.03 after sell → book $9,755.64; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 75 | $17.13 | $2.24 | $-48.70 | $4,049.21 | ▼ -48.70 after sell → book $9,753.41; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 758 | $1.77 | $9.91 | $-19.69 | $5,380.95 | ▼ -19.69 after sell → book $9,743.49; vs 09:30 mark -9.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 64 | $20.91 | $2.18 | — | $4,040.53 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1345.24 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 90 | $14.79 | $2.26 | — | $2,707.17 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1345.24 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 95 | $14.07 | $2.27 | — | $1,368.25 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1345.24 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 178 | $7.54 | $2.52 | — | $24.49 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; 🔵; ret5=-20.9; leftover $1345.24 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.49 | ▼ close $9,635.48 vs 09:30 $9,759.70 (session -98.77) | 16:00 close · cash $24.49 · equity $9,635.48 vs 09:30 $9,759.70 (-124.22; session marks -98.77) · 6 name(s) marked open→close (per-name table). CLS×9 09:30 $332.06 → close $332.63 +5.13; GME×60 09:30 $22.90 → close $22.64 -15.60; TH×64 09:30 $20.91 → close $21.19 +17.92; RARE×90 09:30 $14.79 → close $14.51 -25.20; BHVN×95 09:30 $14.07 → close $13.62 -42.75; FLNC×178 09:30 $7.54 → close $7.32 -38.27 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.49 | ▲ 09:30 equity $9,788.12 vs yday $9,635.48 (+152.64) | 09:30 open · cash $24.49 (unchanged overnight, no fees) · equity $9,788.12 vs prior close $9,635.48 (+152.64) · 6 name(s) re-marked at the open (per-name table). CLS×9 yday $332.63 → 09:30 $341.45 +79.38; GME×60 yday $22.64 → 09:30 $22.78 +8.40; TH×64 yday $21.19 → 09:30 $21.55 +23.04; RARE×90 yday $14.51 → 09:30 $14.60 +8.10; BHVN×95 yday $13.62 → 09:30 $13.90 +26.60; FLNC×178 yday $7.32 → 09:30 $7.36 +7.12 | — |
| 2026-09-21 09:30 ET | **SELL** | `CLS` | 9 | $341.45 | $2.05 | $+187.18 | $3,095.49 | ▲ +187.18 after sell → book $9,786.07; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 60 | $22.78 | $2.19 | $+35.24 | $4,460.10 | ▲ +35.24 after sell → book $9,783.88; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 64 | $21.55 | $2.20 | $+36.57 | $5,837.09 | ▲ +36.57 after sell → book $9,781.67; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 90 | $14.60 | $2.29 | $-21.65 | $7,148.81 | ▼ -21.65 after sell → book $9,779.39; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 95 | $13.90 | $2.30 | $-20.73 | $8,467.01 | ▼ -20.73 after sell → book $9,777.09; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 178 | $7.36 | $2.56 | $-36.24 | $9,774.52 | ▼ -36.24 after sell → book $9,774.52; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 6 | $230.25 | $2.01 | — | $8,391.02 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+12.5; leftover $1396.36 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 7 | $190.30 | $2.01 | — | $7,056.91 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+10.6; leftover $1396.36 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `ALVO` | 235 | $5.92 | $3.03 | — | $5,662.67 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+11.0; leftover $1396.36 | join🔴 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 233 | $5.99 | $3.01 | — | $4,264.00 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; 🔵; ret5=-24.1; leftover $1396.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 53 | $25.95 | $2.15 | — | $2,886.50 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=-7.9; leftover $1396.36 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 100 | $13.96 | $2.29 | — | $1,488.21 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $1396.36 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 649 | $2.15 | $8.37 | — | $84.49 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1396.36 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.49 | ▼ close $9,515.40 vs 09:30 $9,788.12 (session -236.26) | 16:00 close · cash $84.49 · equity $9,515.40 vs 09:30 $9,788.12 (-272.72; session marks -236.26) · 7 name(s) marked open→close (per-name table). VICR×6 09:30 $230.25 → close $223.90 -38.10; SMTC×7 09:30 $190.30 → close $177.37 -90.51; ALVO×235 09:30 $5.92 → close $5.88 -9.40; SION×233 09:30 $5.99 → close $6.00 +2.33; GLXY×53 09:30 $25.95 → close $26.07 +6.36; MARA×100 09:30 $13.96 → close $13.28 -68.00; AMTX×649 09:30 $2.15 → close $2.09 -38.94 | — |

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
| 2026-08-27 | `ASML` | cash | leftover split 644.81 < 1 share @ 1746.53 |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new buys |
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
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
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
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `VICR` | 6 | 2026-09-21 @ $230.25 | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+12.5; leftover $1396.36 |
| `SMTC` | 7 | 2026-09-21 @ $190.30 | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+10.6; leftover $1396.36 |
| `ALVO` | 235 | 2026-09-21 @ $5.92 | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+11.0; leftover $1396.36 |
| `SION` | 233 | 2026-09-21 @ $5.99 | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; 🔵; ret5=-24.1; leftover $1396.36 |
| `GLXY` | 53 | 2026-09-21 @ $25.95 | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=-7.9; leftover $1396.36 |
| `MARA` | 100 | 2026-09-21 @ $13.96 | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $1396.36 |
| `AMTX` | 649 | 2026-09-21 @ $2.15 | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1396.36 |
