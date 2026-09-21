# Factor mine action — `union_macd_xup_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ macd_xup, no 🚨

Cash book **-12.47%** ($8,753) · signal-only (no cash/fees) was -15.61%. Starts YES **8/27**. Fills 141 · skips 53 · realized $-1132.66.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: MACD histogram just crossed from ≤0 to >0 on the last finished bar.
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
- **Gate** `macd_cross_up=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $1,214.10.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `BCAR` | 1638 | — | $6.09 | +0.00 | $5.83 | -425.88 | -425.88 | +0.00 | -425.88 |
| 2026-08-17 | `BCAR` | 1638 | $5.83 | $5.99 | +262.08 | — | +0.00 | +262.08 | -163.80 | — |
| 2026-08-17 | `RDDT` | 55 | — | $177.51 | +0.00 | $164.50 | -715.55 | -715.55 | +0.00 | -715.55 |
| 2026-08-18 | `RDDT` | 55 | $164.50 | $166.10 | +88.00 | — | +0.00 | +88.00 | -627.55 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BNTX` | 16 | — | $109.06 | +0.00 | $110.89 | +29.28 | +29.28 | +0.00 | +29.28 |
| 2026-08-20 | `HUMA` | 2591 | — | $0.71 | +0.00 | $0.68 | -67.37 | -67.37 | +0.00 | -67.37 |
| 2026-08-20 | `EL` | 18 | — | $97.43 | +0.00 | $96.15 | -23.04 | -23.04 | +0.00 | -23.04 |
| 2026-08-20 | `SBET` | 242 | — | $7.55 | +0.00 | $7.59 | +9.68 | +9.68 | +0.00 | +9.68 |
| 2026-08-20 | `BMNR` | 85 | — | $21.46 | +0.00 | $21.57 | +9.35 | +9.35 | +0.00 | +9.35 |
| 2026-08-21 | `BNTX` | 16 | $110.89 | $110.92 | +0.48 | — | +0.00 | +0.48 | +29.76 | — |
| 2026-08-21 | `HUMA` | 2591 | $0.68 | $0.67 | -18.14 | — | +0.00 | -18.14 | -85.50 | — |
| 2026-08-21 | `EL` | 18 | $96.15 | $96.75 | +10.80 | — | +0.00 | +10.80 | -12.24 | — |
| 2026-08-21 | `SBET` | 242 | $7.59 | $7.87 | +67.76 | — | +0.00 | +67.76 | +77.44 | — |
| 2026-08-21 | `BMNR` | 85 | $21.57 | $22.25 | +57.80 | — | +0.00 | +57.80 | +67.15 | — |
| 2026-08-21 | `CF` | 14 | — | $127.43 | +0.00 | $129.60 | +30.38 | +30.38 | +0.00 | +30.38 |
| 2026-08-21 | `INDP` | 1319 | — | $1.39 | +0.00 | $1.29 | -131.90 | -131.90 | +0.00 | -131.90 |
| 2026-08-21 | `MRVI` | 221 | — | $8.28 | +0.00 | $8.64 | +79.56 | +79.56 | +0.00 | +79.56 |
| 2026-08-21 | `MARA` | 156 | — | $11.70 | +0.00 | $11.26 | -68.64 | -68.64 | +0.00 | -68.64 |
| 2026-08-21 | `ILMN` | 8 | — | $212.40 | +0.00 | $219.40 | +56.00 | +56.00 | +0.00 | +56.00 |
| 2026-08-24 | `CF` | 14 | $129.60 | $129.99 | +5.46 | — | +0.00 | +5.46 | +35.84 | — |
| 2026-08-24 | `INDP` | 1319 | $1.29 | $1.24 | -65.95 | — | +0.00 | -65.95 | -197.85 | — |
| 2026-08-24 | `MRVI` | 221 | $8.64 | $8.59 | -11.05 | — | +0.00 | -11.05 | +68.51 | — |
| 2026-08-24 | `MARA` | 156 | $11.26 | $11.17 | -14.04 | — | +0.00 | -14.04 | -82.68 | — |
| 2026-08-24 | `ILMN` | 8 | $219.40 | $215.98 | -27.36 | — | +0.00 | -27.36 | +28.64 | — |
| 2026-08-25 | `ZURA` | 703 | — | $6.37 | +0.00 | $6.32 | -35.15 | -35.15 | +0.00 | -35.15 |
| 2026-08-25 | `RHI` | 102 | — | $43.76 | +0.00 | $44.90 | +116.28 | +116.28 | +0.00 | +116.28 |
| 2026-08-26 | `ZURA` | 703 | $6.32 | $6.13 | -133.57 | — | +0.00 | -133.57 | -168.72 | — |
| 2026-08-26 | `RHI` | 102 | $44.90 | $44.33 | -58.14 | — | +0.00 | -58.14 | +58.14 | — |
| 2026-08-26 | `AVBP` | 94 | — | $31.21 | +0.00 | $31.14 | -6.58 | -6.58 | +0.00 | -6.58 |
| 2026-08-26 | `FLNC` | 264 | — | $11.12 | +0.00 | $11.08 | -10.56 | -10.56 | +0.00 | -10.56 |
| 2026-08-26 | `BE` | 13 | — | $213.94 | +0.00 | $218.21 | +55.51 | +55.51 | +0.00 | +55.51 |
| 2026-08-27 | `AVBP` | 94 | $31.14 | $30.79 | -32.90 | — | +0.00 | -32.90 | -39.48 | — |
| 2026-08-27 | `FLNC` | 264 | $11.08 | $11.52 | +116.16 | — | +0.00 | +116.16 | +105.60 | — |
| 2026-08-27 | `BE` | 13 | $218.21 | $227.10 | +115.57 | — | +0.00 | +115.57 | +171.08 | — |
| 2026-08-27 | `BZ` | 69 | — | $18.50 | +0.00 | $18.00 | -34.50 | -34.50 | +0.00 | -34.50 |
| 2026-08-27 | `VYX` | 144 | — | $8.95 | +0.00 | $9.18 | +33.12 | +33.12 | +0.00 | +33.12 |
| 2026-08-27 | `GAP` | 62 | — | $20.75 | +0.00 | $20.79 | +2.48 | +2.48 | +0.00 | +2.48 |
| 2026-08-27 | `AEO` | 74 | — | $17.27 | +0.00 | $16.69 | -42.92 | -42.92 | +0.00 | -42.92 |
| 2026-08-27 | `SMTC` | 8 | — | $149.40 | +0.00 | $142.43 | -55.76 | -55.76 | +0.00 | -55.76 |
| 2026-08-27 | `GEN` | 43 | — | $29.83 | +0.00 | $30.50 | +28.81 | +28.81 | +0.00 | +28.81 |
| 2026-08-27 | `PGY` | 56 | — | $22.93 | +0.00 | $23.26 | +18.48 | +18.48 | +0.00 | +18.48 |
| 2026-08-28 | `BZ` | 69 | $18.00 | $18.15 | +10.35 | — | +0.00 | +10.35 | -24.15 | — |
| 2026-08-28 | `VYX` | 144 | $9.18 | $9.13 | -7.20 | — | +0.00 | -7.20 | +25.92 | — |
| 2026-08-28 | `GAP` | 62 | $20.79 | $24.69 | +241.80 | — | +0.00 | +241.80 | +244.28 | — |
| 2026-08-28 | `AEO` | 74 | $16.69 | $17.06 | +27.38 | — | +0.00 | +27.38 | -15.54 | — |
| 2026-08-28 | `SMTC` | 8 | $142.43 | $141.76 | -5.36 | — | +0.00 | -5.36 | -61.12 | — |
| 2026-08-28 | `GEN` | 43 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +28.81 | — |
| 2026-08-28 | `PGY` | 56 | $23.26 | $23.21 | -2.80 | — | +0.00 | -2.80 | +15.68 | — |
| 2026-08-28 | `LVWR` | 830 | — | $1.39 | +0.00 | $1.35 | -33.20 | -33.20 | +0.00 | -33.20 |
| 2026-08-28 | `TTMI` | 9 | — | $122.81 | +0.00 | $118.65 | -37.44 | -37.44 | +0.00 | -37.44 |
| 2026-08-28 | `ERAS` | 59 | — | $19.25 | +0.00 | $18.03 | -71.98 | -71.98 | +0.00 | -71.98 |
| 2026-08-28 | `NEO` | 62 | — | $18.36 | +0.00 | $18.05 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-28 | `FTNT` | 6 | — | $172.58 | +0.00 | $166.00 | -39.48 | -39.48 | +0.00 | -39.48 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `RBRK` | 11 | — | $98.95 | +0.00 | $93.05 | -64.90 | -64.90 | +0.00 | -64.90 |
| 2026-08-28 | `ULTA` | 2 | — | $542.00 | +0.00 | $517.50 | -49.00 | -49.00 | +0.00 | -49.00 |
| 2026-08-31 | `LVWR` | 830 | $1.35 | $1.30 | -41.50 | — | +0.00 | -41.50 | -74.70 | — |
| 2026-08-31 | `TTMI` | 9 | $118.65 | $118.83 | +1.62 | — | +0.00 | +1.62 | -35.82 | — |
| 2026-08-31 | `ERAS` | 59 | $18.03 | $17.87 | -9.44 | — | +0.00 | -9.44 | -81.42 | — |
| 2026-08-31 | `NEO` | 62 | $18.05 | $17.77 | -17.36 | — | +0.00 | -17.36 | -36.58 | — |
| 2026-08-31 | `FTNT` | 6 | $166.00 | $166.60 | +3.60 | — | +0.00 | +3.60 | -35.88 | — |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | — | +0.00 | -11.80 | -13.80 | — |
| 2026-08-31 | `RBRK` | 11 | $93.05 | $92.83 | -2.42 | — | +0.00 | -2.42 | -67.32 | — |
| 2026-08-31 | `ULTA` | 2 | $517.50 | $521.10 | +7.20 | — | +0.00 | +7.20 | -41.80 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 27 | — | $52.88 | +0.00 | $52.46 | -11.34 | -11.34 | +0.00 | -11.34 |
| 2026-09-03 | `VSTM` | 182 | — | $8.03 | +0.00 | $7.98 | -9.10 | -9.10 | +0.00 | -9.10 |
| 2026-09-03 | `PYXS` | 395 | — | $3.71 | +0.00 | $3.56 | -57.28 | -57.28 | +0.00 | -57.28 |
| 2026-09-03 | `MLYS` | 50 | — | $29.15 | +0.00 | $28.27 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-09-03 | `HP` | 30 | — | $47.74 | +0.00 | $45.02 | -81.60 | -81.60 | +0.00 | -81.60 |
| 2026-09-03 | `RSKD` | 219 | — | $6.68 | +0.00 | $6.93 | +54.75 | +54.75 | +0.00 | +54.75 |
| 2026-09-04 | `ATRC` | 27 | $52.46 | $52.03 | -11.61 | — | +0.00 | -11.61 | -22.95 | — |
| 2026-09-04 | `VSTM` | 182 | $7.98 | $7.91 | -12.74 | — | +0.00 | -12.74 | -21.84 | — |
| 2026-09-04 | `PYXS` | 395 | $3.56 | $3.53 | -13.83 | — | +0.00 | -13.83 | -71.10 | — |
| 2026-09-04 | `MLYS` | 50 | $28.27 | $28.00 | -13.50 | — | +0.00 | -13.50 | -57.50 | — |
| 2026-09-04 | `HP` | 30 | $45.02 | $44.59 | -12.90 | — | +0.00 | -12.90 | -94.50 | — |
| 2026-09-04 | `RSKD` | 219 | $6.93 | $6.84 | -19.71 | — | +0.00 | -19.71 | +35.04 | — |
| 2026-09-04 | `EOSE` | 484 | — | $3.52 | +0.00 | $3.88 | +174.24 | +174.24 | +0.00 | +174.24 |
| 2026-09-04 | `DELL` | 3 | — | $513.78 | +0.00 | $524.14 | +31.08 | +31.08 | +0.00 | +31.08 |
| 2026-09-04 | `GSM` | 365 | — | $4.67 | +0.00 | $4.67 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-04 | `RNG` | 22 | — | $75.35 | +0.00 | $73.39 | -43.12 | -43.12 | +0.00 | -43.12 |
| 2026-09-04 | `LULU` | 17 | — | $98.15 | +0.00 | $100.61 | +41.82 | +41.82 | +0.00 | +41.82 |
| 2026-09-08 | `EOSE` | 484 | $3.88 | $3.99 | +53.24 | — | +0.00 | +53.24 | +227.48 | — |
| 2026-09-08 | `DELL` | 3 | $524.14 | $521.15 | -8.97 | — | +0.00 | -8.97 | +22.11 | — |
| 2026-09-08 | `GSM` | 365 | $4.67 | $4.75 | +29.20 | — | +0.00 | +29.20 | +29.20 | — |
| 2026-09-08 | `RNG` | 22 | $73.39 | $72.07 | -29.04 | — | +0.00 | -29.04 | -72.16 | — |
| 2026-09-08 | `LULU` | 17 | $100.61 | $100.58 | -0.51 | — | +0.00 | -0.51 | +41.31 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `DBI` | 493 | — | $5.91 | +0.00 | $5.88 | -14.79 | -14.79 | +0.00 | -14.79 |
| 2026-09-11 | `APPS` | 245 | — | $11.88 | +0.00 | $11.81 | -17.15 | -17.15 | +0.00 | -17.15 |
| 2026-09-11 | `INSP` | 41 | — | $69.88 | +0.00 | $73.00 | +127.92 | +127.92 | +0.00 | +127.92 |
| 2026-09-14 | `DBI` | 493 | $5.88 | $5.86 | -9.86 | — | +0.00 | -9.86 | -24.65 | — |
| 2026-09-14 | `APPS` | 245 | $11.81 | $11.75 | -14.70 | — | +0.00 | -14.70 | -31.85 | — |
| 2026-09-14 | `INSP` | 41 | $73.00 | $72.14 | -35.26 | — | +0.00 | -35.26 | +92.66 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `HLP` | 608 | — | $1.80 | +0.00 | $2.07 | +164.16 | +164.16 | +0.00 | +164.16 |
| 2026-09-16 | `FTRE` | 55 | — | $19.75 | +0.00 | $19.97 | +12.10 | +12.10 | +0.00 | +12.10 |
| 2026-09-16 | `SDGR` | 47 | — | $23.29 | +0.00 | $23.93 | +30.08 | +30.08 | +0.00 | +30.08 |
| 2026-09-16 | `RVTY` | 7 | — | $140.88 | +0.00 | $145.73 | +33.95 | +33.95 | +0.00 | +33.95 |
| 2026-09-16 | `TENB` | 29 | — | $36.86 | +0.00 | $36.37 | -14.21 | -14.21 | +0.00 | -14.21 |
| 2026-09-16 | `MRCY` | 12 | — | $87.52 | +0.00 | $87.25 | -3.24 | -3.24 | +0.00 | -3.24 |
| 2026-09-16 | `RBRK` | 10 | — | $101.97 | +0.00 | $104.98 | +30.10 | +30.10 | +0.00 | +30.10 |
| 2026-09-16 | `DOCU` | 15 | — | $70.60 | +0.00 | $70.40 | -3.00 | -3.00 | +0.00 | -3.00 |
| 2026-09-17 | `HLP` | 608 | $2.07 | $2.10 | +18.24 | — | +0.00 | +18.24 | +182.40 | — |
| 2026-09-17 | `FTRE` | 55 | $19.97 | $20.31 | +18.70 | — | +0.00 | +18.70 | +30.80 | — |
| 2026-09-17 | `SDGR` | 47 | $23.93 | $24.09 | +7.52 | — | +0.00 | +7.52 | +37.60 | — |
| 2026-09-17 | `RVTY` | 7 | $145.73 | $147.61 | +13.16 | — | +0.00 | +13.16 | +47.11 | — |
| 2026-09-17 | `TENB` | 29 | $36.37 | $35.89 | -13.92 | — | +0.00 | -13.92 | -28.13 | — |
| 2026-09-17 | `MRCY` | 12 | $87.25 | $89.27 | +24.24 | — | +0.00 | +24.24 | +21.00 | — |
| 2026-09-17 | `RBRK` | 10 | $104.98 | $102.56 | -24.20 | — | +0.00 | -24.20 | +5.90 | — |
| 2026-09-17 | `DOCU` | 15 | $70.40 | $69.16 | -18.60 | — | +0.00 | -18.60 | -21.60 | — |
| 2026-09-17 | `IOVA` | 175 | — | $10.25 | +0.00 | $10.02 | -40.25 | -40.25 | +0.00 | -40.25 |
| 2026-09-17 | `AMN` | 51 | — | $34.93 | +0.00 | $34.55 | -19.38 | -19.38 | +0.00 | -19.38 |
| 2026-09-17 | `SABR` | 749 | — | $2.40 | +0.00 | $2.32 | -59.92 | -59.92 | +0.00 | -59.92 |
| 2026-09-17 | `BRKR` | 29 | — | $61.90 | +0.00 | $63.05 | +33.35 | +33.35 | +0.00 | +33.35 |
| 2026-09-17 | `ADPT` | 63 | — | $28.23 | +0.00 | $28.55 | +20.16 | +20.16 | +0.00 | +20.16 |
| 2026-09-18 | `IOVA` | 175 | $10.02 | $10.12 | +17.50 | — | +0.00 | +17.50 | -22.75 | — |
| 2026-09-18 | `AMN` | 51 | $34.55 | $34.52 | -1.53 | — | +0.00 | -1.53 | -20.91 | — |
| 2026-09-18 | `SABR` | 749 | $2.32 | $2.29 | -22.47 | — | +0.00 | -22.47 | -82.39 | — |
| 2026-09-18 | `BRKR` | 29 | $63.05 | $63.37 | +9.28 | — | +0.00 | +9.28 | +42.63 | — |
| 2026-09-18 | `ADPT` | 63 | $28.55 | $28.55 | +0.00 | — | +0.00 | +0.00 | +20.16 | — |
| 2026-09-18 | `GNRC` | 5 | — | $209.52 | +0.00 | $207.44 | -10.40 | -10.40 | +0.00 | -10.40 |
| 2026-09-18 | `LVWR` | 745 | — | $1.49 | +0.00 | $1.63 | +104.30 | +104.30 | +0.00 | +104.30 |
| 2026-09-18 | `TEM` | 13 | — | $81.40 | +0.00 | $77.84 | -46.28 | -46.28 | +0.00 | -46.28 |
| 2026-09-18 | `FCEL` | 62 | — | $17.80 | +0.00 | $18.11 | +19.22 | +19.22 | +0.00 | +19.22 |
| 2026-09-18 | `PGEN` | 139 | — | $7.98 | +0.00 | $7.78 | -27.80 | -27.80 | +0.00 | -27.80 |
| 2026-09-18 | `SATL` | 202 | — | $5.49 | +0.00 | $5.11 | -75.75 | -75.75 | +0.00 | -75.75 |
| 2026-09-18 | `LTRX` | 188 | — | $5.89 | +0.00 | $5.85 | -7.52 | -7.52 | +0.00 | -7.52 |
| 2026-09-18 | `VOYG` | 29 | — | $37.16 | +0.00 | $35.87 | -37.41 | -37.41 | +0.00 | -37.41 |
| 2026-09-21 | `GNRC` | 5 | $207.44 | $210.00 | +12.80 | — | +0.00 | +12.80 | +2.40 | — |
| 2026-09-21 | `LVWR` | 745 | $1.63 | $1.65 | +14.90 | — | +0.00 | +14.90 | +119.20 | — |
| 2026-09-21 | `TEM` | 13 | $77.84 | $78.99 | +14.95 | — | +0.00 | +14.95 | -31.33 | — |
| 2026-09-21 | `FCEL` | 62 | $18.11 | $18.25 | +8.93 | — | +0.00 | +8.93 | +28.15 | — |
| 2026-09-21 | `PGEN` | 139 | $7.78 | $7.84 | +8.34 | — | +0.00 | +8.34 | -19.46 | — |
| 2026-09-21 | `SATL` | 202 | $5.11 | $5.21 | +20.20 | — | +0.00 | +20.20 | -55.55 | — |
| 2026-09-21 | `LTRX` | 188 | $5.85 | $5.96 | +20.68 | — | +0.00 | +20.68 | +13.16 | — |
| 2026-09-21 | `VOYG` | 29 | $35.87 | $36.21 | +9.86 | — | +0.00 | +9.86 | -27.55 | — |
| 2026-09-21 | `BTBT` | 607 | — | $1.82 | +0.00 | $1.82 | -3.03 | -3.03 | +0.00 | -3.03 |
| 2026-09-21 | `GEMI` | 191 | — | $5.80 | +0.00 | $5.99 | +36.29 | +36.29 | +0.00 | +36.29 |
| 2026-09-21 | `MSTR` | 6 | — | $164.58 | +0.00 | $168.50 | +23.52 | +23.52 | +0.00 | +23.52 |
| 2026-09-21 | `AMTX` | 515 | — | $2.15 | +0.00 | $2.09 | -30.90 | -30.90 | +0.00 | -30.90 |
| 2026-09-21 | `ABTC` | 103 | — | $10.71 | +0.00 | $10.30 | -42.23 | -42.23 | +0.00 | -42.23 |
| 2026-09-21 | `ASST` | 35 | — | $31.64 | +0.00 | $30.33 | -45.85 | -45.85 | +0.00 | -45.85 |
| 2026-09-21 | `ABSI` | 110 | — | $10.06 | +0.00 | $9.82 | -26.40 | -26.40 | +0.00 | -26.40 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -425.88 | BCAR | — | $3.45 | $9,552.99 | BCAR×1638 |
| 2026-08-17 | +2.25 | $3.45 | BCAR×1638 | $9,815.07 | +262.08 | -715.55 | RDDT | BCAR | $28.38 | $9,075.88 | RDDT×55 |
| 2026-08-18 | -6.20 | $28.38 | RDDT×55 | $9,163.88 | +88.00 | +0.00 | — | RDDT | $9,161.65 | $9,161.65 | — |
| 2026-08-19 | -7.20 | $9,161.65 | — | $9,161.65 | -0.00 | +0.00 | — | — | $9,161.65 | $9,161.65 | — |
| 2026-08-20 | +1.12 | $9,161.65 | — | $9,161.65 | -0.00 | -42.10 | BNTX, HUMA, EL, SBET, BMNR | — | $144.37 | $9,084.01 | BNTX×16, HUMA×2591, EL×18, SBET×242, BMNR×85 |
| 2026-08-21 | +3.25 | $144.37 | BNTX×16, HUMA×2591, EL×18, SBET×242, BMNR×85 | $9,202.71 | +118.70 | -34.60 | CF, INDP, MRVI, MARA, ILMN | BNTX, HUMA, EL, SBET, BMNR | $169.37 | $9,106.48 | CF×14, INDP×1319, MRVI×221, MARA×156, ILMN×8 |
| 2026-08-24 | -5.17 | $169.37 | CF×14, INDP×1319, MRVI×221, MARA×156, ILMN×8 | $8,993.54 | -112.94 | +0.00 | — | CF, INDP, MRVI, MARA, ILMN | $8,966.80 | $8,966.80 | — |
| 2026-08-25 | +1.80 | $8,966.80 | — | $8,966.80 | -0.00 | +81.13 | ZURA, RHI | — | $13.80 | $9,036.56 | ZURA×703, RHI×102 |
| 2026-08-26 | +2.02 | $13.80 | ZURA×703, RHI×102 | $8,844.85 | -191.71 | +38.37 | AVBP, FLNC, BE | ZURA, RHI | $174.94 | $8,863.95 | AVBP×94, FLNC×264, BE×13 |
| 2026-08-27 | — | $174.94 | AVBP×94, FLNC×264, BE×13 | $9,062.78 | +198.83 | -50.29 | BZ, VYX, GAP, AEO, SMTC, GEN, PGY | AVBP, FLNC, BE | $147.88 | $8,989.34 | BZ×69, VYX×144, GAP×62, AEO×74, SMTC×8, GEN×43, PGY×56 |
| 2026-08-28 | +0.75 | $147.88 | BZ×69, VYX×144, GAP×62, AEO×74, SMTC×8, GEN×43, PGY×56 | $9,253.51 | +264.17 | -317.22 | LVWR, TTMI, ERAS, NEO, FTNT, ADSK, RBRK, ULTA | BZ, VYX, GAP, AEO, SMTC, GEN, PGY | $427.33 | $8,895.74 | LVWR×830, TTMI×9, ERAS×59, NEO×62, FTNT×6, ADSK×4, RBRK×11, ULTA×2 |
| 2026-08-31 | -5.85 | $427.33 | LVWR×830, TTMI×9, ERAS×59, NEO×62, FTNT×6, ADSK×4, RBRK×11, ULTA×2 | $8,825.64 | -70.10 | +0.00 | — | LVWR, TTMI, ERAS, NEO, FTNT, ADSK, RBRK, ULTA | $8,800.25 | $8,800.25 | — |
| 2026-09-01 | -6.30 | $8,800.25 | — | $8,800.25 | +0.00 | +0.00 | — | — | $8,800.25 | $8,800.25 | — |
| 2026-09-02 | -3.83 | $8,800.25 | — | $8,800.25 | +0.00 | +0.00 | — | — | $8,800.25 | $8,800.25 | — |
| 2026-09-03 | -0.90 | $8,800.25 | — | $8,800.25 | +0.00 | -148.57 | ATRC, VSTM, PYXS, MLYS, HP, RSKD | — | $76.22 | $8,634.94 | ATRC×27, VSTM×182, PYXS×395, MLYS×50, HP×30, RSKD×219 |
| 2026-09-04 | +2.25 | $76.22 | ATRC×27, VSTM×182, PYXS×395, MLYS×50, HP×30, RSKD×219 | $8,550.66 | -84.28 | +204.02 | EOSE, DELL, GSM, RNG, LULU | ATRC, VSTM, PYXS, MLYS, HP, RSKD | $240.81 | $8,720.65 | EOSE×484, DELL×3, GSM×365, RNG×22, LULU×17 |
| 2026-09-08 | -11.47 | $240.81 | EOSE×484, DELL×3, GSM×365, RNG×22, LULU×17 | $8,764.57 | +43.92 | +0.00 | — | EOSE, DELL, GSM, RNG, LULU | $8,747.28 | $8,747.28 | — |
| 2026-09-09 | -13.95 | $8,747.28 | — | $8,747.28 | +0.00 | +0.00 | — | — | $8,747.28 | $8,747.28 | — |
| 2026-09-10 | -13.28 | $8,747.28 | — | $8,747.28 | +0.00 | +0.00 | — | — | $8,747.28 | $8,747.28 | — |
| 2026-09-11 | +0.50 | $8,747.28 | — | $8,747.28 | +0.00 | +95.98 | DBI, APPS, INSP | — | $46.34 | $8,831.63 | DBI×493, APPS×245, INSP×41 |
| 2026-09-14 | -11.00 | $46.34 | DBI×493, APPS×245, INSP×41 | $8,771.81 | -59.82 | +0.00 | — | DBI, APPS, INSP | $8,759.97 | $8,759.97 | — |
| 2026-09-15 | -3.84 | $8,759.97 | — | $8,759.97 | +0.00 | +0.00 | — | — | $8,759.97 | $8,759.97 | — |
| 2026-09-16 | +5.30 | $8,759.97 | — | $8,759.97 | +0.00 | +249.94 | HLP, FTRE, SDGR, RVTY, TENB, MRCY, RBRK, DOCU | — | $278.36 | $8,987.62 | HLP×608, FTRE×55, SDGR×47, RVTY×7, TENB×29, MRCY×12, RBRK×10, DOCU×15 |
| 2026-09-17 | +7.38 | $278.36 | HLP×608, FTRE×55, SDGR×47, RVTY×7, TENB×29, MRCY×12, RBRK×10, DOCU×15 | $9,012.76 | +25.14 | -66.04 | IOVA, AMN, SABR, BRKR, ADPT | HLP, FTRE, SDGR, RVTY, TENB, MRCY, RBRK, DOCU | $25.26 | $8,905.59 | IOVA×175, AMN×51, SABR×749, BRKR×29, ADPT×63 |
| 2026-09-18 | +4.86 | $25.26 | IOVA×175, AMN×51, SABR×749, BRKR×29, ADPT×63 | $8,908.37 | +2.78 | -81.64 | GNRC, LVWR, TEM, FCEL, PGEN, SATL, LTRX, VOYG | IOVA, AMN, SABR, BRKR, ADPT | $142.48 | $8,782.44 | GNRC×5, LVWR×745, TEM×13, FCEL×62, PGEN×139, SATL×202, LTRX×188, VOYG×29 |
| 2026-09-21 | +12.87 | $142.48 | GNRC×5, LVWR×745, TEM×13, FCEL×62, PGEN×139, SATL×202, LTRX×188, VOYG×29 | $8,893.09 | +110.65 | -88.60 | BTBT, GEMI, MSTR, AMTX, ABTC, ASST, ABSI | GNRC, LVWR, TEM, FCEL, PGEN, SATL, LTRX, VOYG | $1,214.10 | $8,752.93 | BTBT×607, GEMI×191, MSTR×6, AMTX×515, ABTC×103, ASST×35, ABSI×110 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BCAR` | 1638 | $6.09 | $21.13 | — | $3.45 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ⚪; ret5=+27.6; leftover $10000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.45 | ▼ close $9,552.99 vs 09:30 $10,000.00 (session -425.88) | 16:00 close · cash $3.45 · equity $9,552.99 vs 09:30 $10,000.00 (-447.01; session marks -425.88) · 1 name(s) marked open→close (per-name table). BCAR×1638 09:30 $6.09 → close $5.83 -425.88 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.45 | ▲ 09:30 equity $9,815.07 vs yday $9,552.99 (+262.08) | 09:30 open · cash $3.45 (unchanged overnight, no fees) · equity $9,815.07 vs prior close $9,552.99 (+262.08) · 1 name(s) re-marked at the open (per-name table). BCAR×1638 yday $5.83 → 09:30 $5.99 +262.08 | — |
| 2026-08-17 09:30 ET | **SELL** | `BCAR` | 1638 | $5.99 | $21.48 | $-206.41 | $9,793.59 | ▼ -206.41 after sell → book $9,793.59; vs 09:30 mark -21.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `RDDT` | 55 | $177.51 | $2.15 | — | $28.38 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ⚪; ret5=+10.1; leftover $9793.59 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.38 | ▼ close $9,075.88 vs 09:30 $9,815.07 (session -715.55) | 16:00 close · cash $28.38 · equity $9,075.88 vs 09:30 $9,815.07 (-739.19; session marks -715.55) · 1 name(s) marked open→close (per-name table). RDDT×55 09:30 $177.51 → close $164.50 -715.55 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.38 | ▲ 09:30 equity $9,163.88 vs yday $9,075.88 (+88.00) | 09:30 open · cash $28.38 (unchanged overnight, no fees) · equity $9,163.88 vs prior close $9,075.88 (+88.00) · 1 name(s) re-marked at the open (per-name table). RDDT×55 yday $164.50 → 09:30 $166.10 +88.00 | — |
| 2026-08-18 09:30 ET | **SELL** | `RDDT` | 55 | $166.10 | $2.24 | $-631.94 | $9,161.65 | ▼ -631.94 after sell → book $9,161.65; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,161.65 | ▲ close $9,161.65 vs 09:30 $9,163.88 (session +0.00) | 16:00 close · cash $9,161.65 · no lots left · equity $9,161.65. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,161.65 | ▲ 09:30 equity $9,161.65 vs yday $9,161.65 (-0.00) | 09:30 open · cash $9,161.65 · no holdings · equity $9,161.65 vs prior close $9,161.65 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,161.65 | ▲ close $9,161.65 vs 09:30 $9,161.65 (session +0.00) | 16:00 close · cash $9,161.65 · no lots left · equity $9,161.65. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,161.65 | ▲ 09:30 equity $9,161.65 vs yday $9,161.65 (-0.00) | 09:30 open · cash $9,161.65 · no holdings · equity $9,161.65 vs prior close $9,161.65 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 16 | $109.06 | $2.04 | — | $7,414.65 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1832.33 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 2591 | $0.71 | $26.09 | — | $5,556.72 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1832.33 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 18 | $97.43 | $2.04 | — | $3,800.94 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $1832.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SBET` | 242 | $7.55 | $3.12 | — | $1,970.71 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+14.6; leftover $1832.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BMNR` | 85 | $21.46 | $2.25 | — | $144.37 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ⚪; ret5=+13.1; leftover $1832.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $144.37 | ▼ close $9,084.01 vs 09:30 $9,161.65 (session -42.10) | 16:00 close · cash $144.37 · equity $9,084.01 vs 09:30 $9,161.65 (-77.64; session marks -42.10) · 5 name(s) marked open→close (per-name table). BNTX×16 09:30 $109.06 → close $110.89 +29.28; HUMA×2591 09:30 $0.71 → close $0.68 -67.37; EL×18 09:30 $97.43 → close $96.15 -23.04; SBET×242 09:30 $7.55 → close $7.59 +9.68; BMNR×85 09:30 $21.46 → close $21.57 +9.35 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $144.37 | ▲ 09:30 equity $9,202.71 vs yday $9,084.01 (+118.70) | 09:30 open · cash $144.37 (unchanged overnight, no fees) · equity $9,202.71 vs prior close $9,084.01 (+118.70) · 5 name(s) re-marked at the open (per-name table). BNTX×16 yday $110.89 → 09:30 $110.92 +0.48; HUMA×2591 yday $0.68 → 09:30 $0.67 -18.14; EL×18 yday $96.15 → 09:30 $96.75 +10.80; SBET×242 yday $7.59 → 09:30 $7.87 +67.76; BMNR×85 yday $21.57 → 09:30 $22.25 +57.80 | — |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 16 | $110.92 | $2.06 | $+25.66 | $1,917.03 | ▲ +25.66 after sell → book $9,200.65; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 2591 | $0.67 | $25.68 | $-137.27 | $3,637.68 | ▼ -137.27 after sell → book $9,174.97; vs 09:30 mark -25.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 18 | $96.75 | $2.07 | $-16.35 | $5,377.11 | ▼ -16.35 after sell → book $9,172.90; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SBET` | 242 | $7.87 | $3.18 | $+71.14 | $7,278.48 | ▲ +71.14 after sell → book $9,169.73; vs 09:30 mark -3.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BMNR` | 85 | $22.25 | $2.27 | $+62.63 | $9,167.45 | ▲ +62.63 after sell → book $9,167.45; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 14 | $127.43 | $2.03 | — | $7,381.40 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable; 🔵; ⚪; ret5=+7.9; leftover $1833.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 1319 | $1.39 | $17.02 | — | $5,530.97 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $1833.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 221 | $8.28 | $2.85 | — | $3,698.24 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1833.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 156 | $11.70 | $2.46 | — | $1,870.58 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1833.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ILMN` | 8 | $212.40 | $2.01 | — | $169.37 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list mover_buy; 🔵; ⚪; ret5=+10.7; leftover $1833.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.37 | ▼ close $9,106.48 vs 09:30 $9,202.71 (session -34.60) | 16:00 close · cash $169.37 · equity $9,106.48 vs 09:30 $9,202.71 (-96.23; session marks -34.60) · 5 name(s) marked open→close (per-name table). CF×14 09:30 $127.43 → close $129.60 +30.38; INDP×1319 09:30 $1.39 → close $1.29 -131.90; MRVI×221 09:30 $8.28 → close $8.64 +79.56; MARA×156 09:30 $11.70 → close $11.26 -68.64; ILMN×8 09:30 $212.40 → close $219.40 +56.00 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.37 | ▼ 09:30 equity $8,993.54 vs yday $9,106.48 (-112.94) | 09:30 open · cash $169.37 (unchanged overnight, no fees) · equity $8,993.54 vs prior close $9,106.48 (-112.94) · 5 name(s) re-marked at the open (per-name table). CF×14 yday $129.60 → 09:30 $129.99 +5.46; INDP×1319 yday $1.29 → 09:30 $1.24 -65.95; MRVI×221 yday $8.64 → 09:30 $8.59 -11.05; MARA×156 yday $11.26 → 09:30 $11.17 -14.04; ILMN×8 yday $219.40 → 09:30 $215.98 -27.36 | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 14 | $129.99 | $2.06 | $+31.75 | $1,987.17 | ▲ +31.75 after sell → book $8,991.48; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 1319 | $1.24 | $17.25 | $-232.11 | $3,605.49 | ▼ -232.11 after sell → book $8,974.24; vs 09:30 mark -17.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 221 | $8.59 | $2.90 | $+62.76 | $5,500.97 | ▲ +62.76 after sell → book $8,971.33; vs 09:30 mark -2.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 156 | $11.17 | $2.50 | $-87.64 | $7,241.00 | ▼ -87.64 after sell → book $8,968.84; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ILMN` | 8 | $215.98 | $2.04 | $+24.59 | $8,966.80 | ▲ +24.59 after sell → book $8,966.80; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,966.80 | ▲ close $8,966.80 vs 09:30 $8,993.54 (session +0.00) | 16:00 close · cash $8,966.80 · no lots left · equity $8,966.80. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,966.80 | ▲ 09:30 equity $8,966.80 vs yday $8,966.80 (-0.00) | 09:30 open · cash $8,966.80 · no holdings · equity $8,966.80 vs prior close $8,966.80 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 703 | $6.37 | $9.07 | — | $4,479.62 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $4483.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 102 | $43.76 | $2.30 | — | $13.80 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $4483.40 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.80 | ▲ close $9,036.56 vs 09:30 $8,966.80 (session +81.13) | 16:00 close · cash $13.80 · equity $9,036.56 vs 09:30 $8,966.80 (+69.76; session marks +81.13) · 2 name(s) marked open→close (per-name table). ZURA×703 09:30 $6.37 → close $6.32 -35.15; RHI×102 09:30 $43.76 → close $44.90 +116.28 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.80 | ▼ 09:30 equity $8,844.85 vs yday $9,036.56 (-191.71) | 09:30 open · cash $13.80 (unchanged overnight, no fees) · equity $8,844.85 vs prior close $9,036.56 (-191.71) · 2 name(s) re-marked at the open (per-name table). ZURA×703 yday $6.32 → 09:30 $6.13 -133.57; RHI×102 yday $44.90 → 09:30 $44.33 -58.14 | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 703 | $6.13 | $9.22 | $-187.01 | $4,313.97 | ▼ -187.01 after sell → book $8,835.63; vs 09:30 mark -9.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 102 | $44.33 | $2.35 | $+53.49 | $8,833.29 | ▲ +53.49 after sell → book $8,833.29; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 94 | $31.21 | $2.27 | — | $5,897.27 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $2944.43 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 264 | $11.12 | $3.41 | — | $2,958.19 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $2944.43 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 13 | $213.94 | $2.03 | — | $174.94 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $2944.43 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.94 | ▲ close $8,863.95 vs 09:30 $8,844.85 (session +38.37) | 16:00 close · cash $174.94 · equity $8,863.95 vs 09:30 $8,844.85 (+19.10; session marks +38.37) · 3 name(s) marked open→close (per-name table). AVBP×94 09:30 $31.21 → close $31.14 -6.58; FLNC×264 09:30 $11.12 → close $11.08 -10.56; BE×13 09:30 $213.94 → close $218.21 +55.51 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.94 | ▲ 09:30 equity $9,062.78 vs yday $8,863.95 (+198.83) | 09:30 open · cash $174.94 (unchanged overnight, no fees) · equity $9,062.78 vs prior close $8,863.95 (+198.83) · 3 name(s) re-marked at the open (per-name table). AVBP×94 yday $31.14 → 09:30 $30.79 -32.90; FLNC×264 yday $11.08 → 09:30 $11.52 +116.16; BE×13 yday $218.21 → 09:30 $227.10 +115.57 | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 94 | $30.79 | $2.31 | $-44.06 | $3,066.89 | ▼ -44.06 after sell → book $9,060.47; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 264 | $11.52 | $3.47 | $+98.72 | $6,104.69 | ▲ +98.72 after sell → book $9,056.99; vs 09:30 mark -3.48 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BE` | 13 | $227.10 | $2.06 | $+166.99 | $9,054.93 | ▲ +166.99 after sell → book $9,054.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 69 | $18.50 | $2.20 | — | $7,776.23 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+17.2; leftover $1293.56 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `VYX` | 144 | $8.95 | $2.42 | — | $6,485.01 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+16.2; leftover $1293.56 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GAP` | 62 | $20.75 | $2.18 | — | $5,196.34 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot,overnight; ret5=+5.2; leftover $1293.56 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `AEO` | 74 | $17.27 | $2.21 | — | $3,916.14 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+5.5; leftover $1293.56 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $2,718.93 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; 🔵; ret5=+12.3; leftover $1293.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 43 | $29.83 | $2.12 | — | $1,434.12 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list mover_buy; 🔵; ret5=+7.6; leftover $1293.56 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 56 | $22.93 | $2.16 | — | $147.88 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list mover_buy; 🔵; ret5=+9.5; leftover $1293.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $147.88 | ▼ close $8,989.34 vs 09:30 $9,062.78 (session -50.29) | 16:00 close · cash $147.88 · equity $8,989.34 vs 09:30 $9,062.78 (-73.44; session marks -50.29) · 7 name(s) marked open→close (per-name table). BZ×69 09:30 $18.50 → close $18.00 -34.50; VYX×144 09:30 $8.95 → close $9.18 +33.12; GAP×62 09:30 $20.75 → close $20.79 +2.48; AEO×74 09:30 $17.27 → close $16.69 -42.92; SMTC×8 09:30 $149.40 → close $142.43 -55.76; GEN×43 09:30 $29.83 → close $30.50 +28.81; PGY×56 09:30 $22.93 → close $23.26 +18.48 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $147.88 | ▲ 09:30 equity $9,253.51 vs yday $8,989.34 (+264.17) | 09:30 open · cash $147.88 (unchanged overnight, no fees) · equity $9,253.51 vs prior close $8,989.34 (+264.17) · 7 name(s) re-marked at the open (per-name table). BZ×69 yday $18.00 → 09:30 $18.15 +10.35; VYX×144 yday $9.18 → 09:30 $9.13 -7.20; GAP×62 yday $20.79 → 09:30 $24.69 +241.80; AEO×74 yday $16.69 → 09:30 $17.06 +27.38; SMTC×8 yday $142.43 → 09:30 $141.76 -5.36; GEN×43 yday $30.50 → 09:30 $30.50 +0.00; PGY×56 yday $23.26 → 09:30 $23.21 -2.80 | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 69 | $18.15 | $2.22 | $-28.57 | $1,398.01 | ▼ -28.57 after sell → book $9,251.29; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `VYX` | 144 | $9.13 | $2.46 | $+21.04 | $2,710.28 | ▲ +21.04 after sell → book $9,248.84; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `GAP` | 62 | $24.69 | $2.20 | $+239.91 | $4,238.86 | ▲ +239.91 after sell → book $9,246.64; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AEO` | 74 | $17.06 | $2.23 | $-19.99 | $5,499.07 | ▼ -19.99 after sell → book $9,244.41; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SMTC` | 8 | $141.76 | $2.03 | $-65.17 | $6,631.11 | ▼ -65.17 after sell → book $9,242.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 43 | $30.50 | $2.14 | $+24.55 | $7,940.47 | ▲ +24.55 after sell → book $9,240.23; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 56 | $23.21 | $2.18 | $+11.34 | $9,238.05 | ▲ +11.34 after sell → book $9,238.05; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 830 | $1.39 | $10.71 | — | $8,073.65 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+20.4; leftover $1154.76 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 9 | $122.81 | $2.02 | — | $6,966.34 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1154.76 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 59 | $19.25 | $2.17 | — | $5,828.42 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; ret5=+14.1; leftover $1154.76 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 62 | $18.36 | $2.18 | — | $4,687.93 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+12.8; leftover $1154.76 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FTNT` | 6 | $172.58 | $2.01 | — | $3,650.44 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+14.6; leftover $1154.76 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $2,603.80 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+7.8; leftover $1154.76 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 11 | $98.95 | $2.02 | — | $1,513.32 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+9.7; leftover $1154.76 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 2 | $542.00 | $2.00 | — | $427.33 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+4.8; leftover $1154.76 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $427.33 | ▼ close $8,895.74 vs 09:30 $9,253.51 (session -317.22) | 16:00 close · cash $427.33 · equity $8,895.74 vs 09:30 $9,253.51 (-357.77; session marks -317.22) · 8 name(s) marked open→close (per-name table). LVWR×830 09:30 $1.39 → close $1.35 -33.20; TTMI×9 09:30 $122.81 → close $118.65 -37.44; ERAS×59 09:30 $19.25 → close $18.03 -71.98; NEO×62 09:30 $18.36 → close $18.05 -19.22; FTNT×6 09:30 $172.58 → close $166.00 -39.48; ADSK×4 09:30 $261.16 → close $260.66 -2.00; RBRK×11 09:30 $98.95 → close $93.05 -64.90; ULTA×2 09:30 $542.00 → close $517.50 -49.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $427.33 | ▼ 09:30 equity $8,825.64 vs yday $8,895.74 (-70.10) | 09:30 open · cash $427.33 (unchanged overnight, no fees) · equity $8,825.64 vs prior close $8,895.74 (-70.10) · 8 name(s) re-marked at the open (per-name table). LVWR×830 yday $1.35 → 09:30 $1.30 -41.50; TTMI×9 yday $118.65 → 09:30 $118.83 +1.62; ERAS×59 yday $18.03 → 09:30 $17.87 -9.44; NEO×62 yday $18.05 → 09:30 $17.77 -17.36; FTNT×6 yday $166.00 → 09:30 $166.60 +3.60; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; RBRK×11 yday $93.05 → 09:30 $92.83 -2.42; ULTA×2 yday $517.50 → 09:30 $521.10 +7.20 | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 830 | $1.30 | $10.85 | $-96.26 | $1,495.47 | ▼ -96.26 after sell → book $8,814.78; vs 09:30 mark -10.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 9 | $118.83 | $2.04 | $-39.87 | $2,562.91 | ▼ -39.87 after sell → book $8,812.75; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 59 | $17.87 | $2.19 | $-85.77 | $3,615.05 | ▼ -85.77 after sell → book $8,810.56; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NEO` | 62 | $17.77 | $2.20 | $-40.95 | $4,714.59 | ▼ -40.95 after sell → book $8,808.36; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FTNT` | 6 | $166.60 | $2.03 | $-39.92 | $5,712.16 | ▼ -39.92 after sell → book $8,806.33; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $6,740.98 | ▼ -17.82 after sell → book $8,804.31; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RBRK` | 11 | $92.83 | $2.04 | $-71.39 | $7,760.07 | ▼ -71.39 after sell → book $8,802.27; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 2 | $521.10 | $2.02 | $-45.81 | $8,800.25 | ▼ -45.81 after sell → book $8,800.25; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,800.25 | ▲ close $8,800.25 vs 09:30 $8,825.64 (session +0.00) | 16:00 close · cash $8,800.25 · no lots left · equity $8,800.25. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,800.25 | ▲ 09:30 equity $8,800.25 vs yday $8,800.25 (+0.00) | 09:30 open · cash $8,800.25 · no holdings · equity $8,800.25 vs prior close $8,800.25 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,800.25 | ▲ close $8,800.25 vs 09:30 $8,800.25 (session +0.00) | 16:00 close · cash $8,800.25 · no lots left · equity $8,800.25. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,800.25 | ▲ 09:30 equity $8,800.25 vs yday $8,800.25 (+0.00) | 09:30 open · cash $8,800.25 · no holdings · equity $8,800.25 vs prior close $8,800.25 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,800.25 | ▲ close $8,800.25 vs 09:30 $8,800.25 (session +0.00) | 16:00 close · cash $8,800.25 · no lots left · equity $8,800.25. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,800.25 | ▲ 09:30 equity $8,800.25 vs yday $8,800.25 (+0.00) | 09:30 open · cash $8,800.25 · no holdings · equity $8,800.25 vs prior close $8,800.25 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $52.88 | $2.07 | — | $7,370.42 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1466.71 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 182 | $8.03 | $2.54 | — | $5,906.43 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1466.71 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `PYXS` | 395 | $3.71 | $5.10 | — | $4,435.88 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+12.3; leftover $1466.71 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MLYS` | 50 | $29.15 | $2.14 | — | $2,976.24 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+12.2; leftover $1466.71 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HP` | 30 | $47.74 | $2.08 | — | $1,541.96 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+15.1; leftover $1466.71 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RSKD` | 219 | $6.68 | $2.83 | — | $76.22 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+11.4; leftover $1466.71 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.22 | ▼ close $8,634.94 vs 09:30 $8,800.25 (session -148.57) | 16:00 close · cash $76.22 · equity $8,634.94 vs 09:30 $8,800.25 (-165.31; session marks -148.57) · 6 name(s) marked open→close (per-name table). ATRC×27 09:30 $52.88 → close $52.46 -11.34; VSTM×182 09:30 $8.03 → close $7.98 -9.10; PYXS×395 09:30 $3.71 → close $3.56 -57.28; MLYS×50 09:30 $29.15 → close $28.27 -44.00; HP×30 09:30 $47.74 → close $45.02 -81.60; RSKD×219 09:30 $6.68 → close $6.93 +54.75 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.22 | ▼ 09:30 equity $8,550.66 vs yday $8,634.94 (-84.28) | 09:30 open · cash $76.22 (unchanged overnight, no fees) · equity $8,550.66 vs prior close $8,634.94 (-84.28) · 6 name(s) re-marked at the open (per-name table). ATRC×27 yday $52.46 → 09:30 $52.03 -11.61; VSTM×182 yday $7.98 → 09:30 $7.91 -12.74; PYXS×395 yday $3.56 → 09:30 $3.53 -13.83; MLYS×50 yday $28.27 → 09:30 $28.00 -13.50; HP×30 yday $45.02 → 09:30 $44.59 -12.90; RSKD×219 yday $6.93 → 09:30 $6.84 -19.71 | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 27 | $52.03 | $2.09 | $-27.11 | $1,478.93 | ▼ -27.11 after sell → book $8,548.56; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 182 | $7.91 | $2.58 | $-26.95 | $2,915.98 | ▼ -26.95 after sell → book $8,545.99; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PYXS` | 395 | $3.53 | $5.17 | $-81.37 | $4,305.15 | ▼ -81.37 after sell → book $8,540.81; vs 09:30 mark -5.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MLYS` | 50 | $28.00 | $2.16 | $-61.80 | $5,702.99 | ▼ -61.80 after sell → book $8,538.65; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `HP` | 30 | $44.59 | $2.10 | $-98.68 | $7,038.59 | ▼ -98.68 after sell → book $8,536.55; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RSKD` | 219 | $6.84 | $2.87 | $+29.34 | $8,533.68 | ▲ +29.34 after sell → book $8,533.68; vs 09:30 mark -2.87 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 484 | $3.52 | $6.24 | — | $6,823.75 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1706.74 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 3 | $513.78 | $2.00 | — | $5,280.42 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1706.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GSM` | 365 | $4.67 | $4.71 | — | $3,571.16 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; ret5=+11.9; leftover $1706.74 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `RNG` | 22 | $75.35 | $2.06 | — | $1,911.40 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+11.4; leftover $1706.74 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 17 | $98.15 | $2.04 | — | $240.81 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+5.9; leftover $1706.74 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $240.81 | ▲ close $8,720.65 vs 09:30 $8,550.66 (session +204.02) | 16:00 close · cash $240.81 · equity $8,720.65 vs 09:30 $8,550.66 (+169.99; session marks +204.02) · 5 name(s) marked open→close (per-name table). EOSE×484 09:30 $3.52 → close $3.88 +174.24; DELL×3 09:30 $513.78 → close $524.14 +31.08; GSM×365 09:30 $4.67 → close $4.67 +0.00; RNG×22 09:30 $75.35 → close $73.39 -43.12; LULU×17 09:30 $98.15 → close $100.61 +41.82 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $240.81 | ▲ 09:30 equity $8,764.57 vs yday $8,720.65 (+43.92) | 09:30 open · cash $240.81 (unchanged overnight, no fees) · equity $8,764.57 vs prior close $8,720.65 (+43.92) · 5 name(s) re-marked at the open (per-name table). EOSE×484 yday $3.88 → 09:30 $3.99 +53.24; DELL×3 yday $524.14 → 09:30 $521.15 -8.97; GSM×365 yday $4.67 → 09:30 $4.75 +29.20; RNG×22 yday $73.39 → 09:30 $72.07 -29.04; LULU×17 yday $100.61 → 09:30 $100.58 -0.51 | — |
| 2026-09-08 09:30 ET | **SELL** | `EOSE` | 484 | $3.99 | $6.34 | $+214.90 | $2,165.63 | ▲ +214.90 after sell → book $8,758.23; vs 09:30 mark -6.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 3 | $521.15 | $2.02 | $+18.09 | $3,727.06 | ▲ +18.09 after sell → book $8,756.21; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `GSM` | 365 | $4.75 | $4.78 | $+19.71 | $5,456.03 | ▲ +19.71 after sell → book $8,751.43; vs 09:30 mark -4.78 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `RNG` | 22 | $72.07 | $2.08 | $-76.29 | $7,039.49 | ▼ -76.29 after sell → book $8,749.35; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 17 | $100.58 | $2.06 | $+37.20 | $8,747.28 | ▲ +37.20 after sell → book $8,747.28; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,747.28 | ▲ close $8,747.28 vs 09:30 $8,764.57 (session +0.00) | 16:00 close · cash $8,747.28 · no lots left · equity $8,747.28. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,747.28 | ▲ 09:30 equity $8,747.28 vs yday $8,747.28 (+0.00) | 09:30 open · cash $8,747.28 · no holdings · equity $8,747.28 vs prior close $8,747.28 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,747.28 | ▲ close $8,747.28 vs 09:30 $8,747.28 (session +0.00) | 16:00 close · cash $8,747.28 · no lots left · equity $8,747.28. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,747.28 | ▲ 09:30 equity $8,747.28 vs yday $8,747.28 (+0.00) | 09:30 open · cash $8,747.28 · no holdings · equity $8,747.28 vs prior close $8,747.28 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,747.28 | ▲ close $8,747.28 vs 09:30 $8,747.28 (session +0.00) | 16:00 close · cash $8,747.28 · no lots left · equity $8,747.28. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,747.28 | ▲ 09:30 equity $8,747.28 vs yday $8,747.28 (+0.00) | 09:30 open · cash $8,747.28 · no holdings · equity $8,747.28 vs prior close $8,747.28 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 493 | $5.91 | $6.36 | — | $5,827.29 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $2915.76 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `APPS` | 245 | $11.88 | $3.16 | — | $2,913.53 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+20.7; leftover $2915.76 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INSP` | 41 | $69.88 | $2.11 | — | $46.34 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+8.0; leftover $2915.76 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.34 | ▲ close $8,831.63 vs 09:30 $8,747.28 (session +95.98) | 16:00 close · cash $46.34 · equity $8,831.63 vs 09:30 $8,747.28 (+84.35; session marks +95.98) · 3 name(s) marked open→close (per-name table). DBI×493 09:30 $5.91 → close $5.88 -14.79; APPS×245 09:30 $11.88 → close $11.81 -17.15; INSP×41 09:30 $69.88 → close $73.00 +127.92 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.34 | ▼ 09:30 equity $8,771.81 vs yday $8,831.63 (-59.82) | 09:30 open · cash $46.34 (unchanged overnight, no fees) · equity $8,771.81 vs prior close $8,831.63 (-59.82) · 3 name(s) re-marked at the open (per-name table). DBI×493 yday $5.88 → 09:30 $5.86 -9.86; APPS×245 yday $11.81 → 09:30 $11.75 -14.70; INSP×41 yday $73.00 → 09:30 $72.14 -35.26 | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 493 | $5.86 | $6.46 | $-37.47 | $2,928.86 | ▼ -37.47 after sell → book $8,765.35; vs 09:30 mark -6.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `APPS` | 245 | $11.75 | $3.22 | $-38.23 | $5,804.38 | ▼ -38.23 after sell → book $8,762.12; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INSP` | 41 | $72.14 | $2.15 | $+88.40 | $8,759.97 | ▲ +88.40 after sell → book $8,759.97; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,759.97 | ▲ close $8,759.97 vs 09:30 $8,771.81 (session +0.00) | 16:00 close · cash $8,759.97 · no lots left · equity $8,759.97. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,759.97 | ▲ 09:30 equity $8,759.97 vs yday $8,759.97 (+0.00) | 09:30 open · cash $8,759.97 · no holdings · equity $8,759.97 vs prior close $8,759.97 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,759.97 | ▲ close $8,759.97 vs 09:30 $8,759.97 (session +0.00) | 16:00 close · cash $8,759.97 · no lots left · equity $8,759.97. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,759.97 | ▲ 09:30 equity $8,759.97 vs yday $8,759.97 (+0.00) | 09:30 open · cash $8,759.97 · no holdings · equity $8,759.97 vs prior close $8,759.97 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 608 | $1.80 | $7.84 | — | $7,657.73 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $1095.00 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `FTRE` | 55 | $19.75 | $2.15 | — | $6,569.33 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $1095.00 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 47 | $23.29 | $2.13 | — | $5,472.57 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+16.1; leftover $1095.00 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 7 | $140.88 | $2.01 | — | $4,484.39 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $1095.00 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `TENB` | 29 | $36.86 | $2.08 | — | $3,413.38 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; ret5=+13.0; leftover $1095.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 12 | $87.52 | $2.03 | — | $2,361.11 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+4.3; leftover $1095.00 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RBRK` | 10 | $101.97 | $2.02 | — | $1,339.39 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+13.0; leftover $1095.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `DOCU` | 15 | $70.60 | $2.04 | — | $278.36 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+10.4; leftover $1095.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $278.36 | ▲ close $8,987.62 vs 09:30 $8,759.97 (session +249.94) | 16:00 close · cash $278.36 · equity $8,987.62 vs 09:30 $8,759.97 (+227.65; session marks +249.94) · 8 name(s) marked open→close (per-name table). HLP×608 09:30 $1.80 → close $2.07 +164.16; FTRE×55 09:30 $19.75 → close $19.97 +12.10; SDGR×47 09:30 $23.29 → close $23.93 +30.08; RVTY×7 09:30 $140.88 → close $145.73 +33.95; TENB×29 09:30 $36.86 → close $36.37 -14.21; MRCY×12 09:30 $87.52 → close $87.25 -3.24; RBRK×10 09:30 $101.97 → close $104.98 +30.10; DOCU×15 09:30 $70.60 → close $70.40 -3.00 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $278.36 | ▲ 09:30 equity $9,012.76 vs yday $8,987.62 (+25.14) | 09:30 open · cash $278.36 (unchanged overnight, no fees) · equity $9,012.76 vs prior close $8,987.62 (+25.14) · 8 name(s) re-marked at the open (per-name table). HLP×608 yday $2.07 → 09:30 $2.10 +18.24; FTRE×55 yday $19.97 → 09:30 $20.31 +18.70; SDGR×47 yday $23.93 → 09:30 $24.09 +7.52; RVTY×7 yday $145.73 → 09:30 $147.61 +13.16; TENB×29 yday $36.37 → 09:30 $35.89 -13.92; MRCY×12 yday $87.25 → 09:30 $89.27 +24.24; RBRK×10 yday $104.98 → 09:30 $102.56 -24.20; DOCU×15 yday $70.40 → 09:30 $69.16 -18.60 | — |
| 2026-09-17 09:30 ET | **SELL** | `HLP` | 608 | $2.10 | $7.95 | $+166.60 | $1,547.20 | ▲ +166.60 after sell → book $9,004.80; vs 09:30 mark -7.96 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `FTRE` | 55 | $20.31 | $2.17 | $+26.47 | $2,662.08 | ▲ +26.47 after sell → book $9,002.63; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 47 | $24.09 | $2.15 | $+33.32 | $3,792.16 | ▲ +33.32 after sell → book $9,000.48; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RVTY` | 7 | $147.61 | $2.03 | $+43.07 | $4,823.40 | ▲ +43.07 after sell → book $8,998.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `TENB` | 29 | $35.89 | $2.10 | $-32.30 | $5,862.11 | ▼ -32.30 after sell → book $8,996.35; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `MRCY` | 12 | $89.27 | $2.05 | $+16.93 | $6,931.30 | ▲ +16.93 after sell → book $8,994.30; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RBRK` | 10 | $102.56 | $2.04 | $+1.84 | $7,954.86 | ▲ +1.84 after sell → book $8,992.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `DOCU` | 15 | $69.16 | $2.06 | $-25.69 | $8,990.21 | ▼ -25.69 after sell → book $8,990.21; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 175 | $10.25 | $2.52 | — | $7,193.94 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1798.04 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 51 | $34.93 | $2.14 | — | $5,410.37 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; ret5=+1.6; leftover $1798.04 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 749 | $2.40 | $9.66 | — | $3,603.11 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1798.04 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 29 | $61.90 | $2.08 | — | $1,805.93 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; leftover $1798.04 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ADPT` | 63 | $28.23 | $2.18 | — | $25.26 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+13.3; leftover $1798.04 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.26 | ▼ close $8,905.59 vs 09:30 $9,012.76 (session -66.04) | 16:00 close · cash $25.26 · equity $8,905.59 vs 09:30 $9,012.76 (-107.17; session marks -66.04) · 5 name(s) marked open→close (per-name table). IOVA×175 09:30 $10.25 → close $10.02 -40.25; AMN×51 09:30 $34.93 → close $34.55 -19.38; SABR×749 09:30 $2.40 → close $2.32 -59.92; BRKR×29 09:30 $61.90 → close $63.05 +33.35; ADPT×63 09:30 $28.23 → close $28.55 +20.16 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.26 | ▲ 09:30 equity $8,908.37 vs yday $8,905.59 (+2.78) | 09:30 open · cash $25.26 (unchanged overnight, no fees) · equity $8,908.37 vs prior close $8,905.59 (+2.78) · 5 name(s) re-marked at the open (per-name table). IOVA×175 yday $10.02 → 09:30 $10.12 +17.50; AMN×51 yday $34.55 → 09:30 $34.52 -1.53; SABR×749 yday $2.32 → 09:30 $2.29 -22.47; BRKR×29 yday $63.05 → 09:30 $63.37 +9.28; ADPT×63 yday $28.55 → 09:30 $28.55 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 175 | $10.12 | $2.56 | $-27.82 | $1,793.70 | ▼ -27.82 after sell → book $8,905.81; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 51 | $34.52 | $2.17 | $-25.22 | $3,552.06 | ▼ -25.22 after sell → book $8,903.65; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 749 | $2.29 | $9.80 | $-101.85 | $5,257.47 | ▼ -101.85 after sell → book $8,893.85; vs 09:30 mark -9.80 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 29 | $63.37 | $2.10 | $+38.45 | $7,093.09 | ▲ +38.45 after sell → book $8,891.74; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `ADPT` | 63 | $28.55 | $2.20 | $+15.78 | $8,889.54 | ▲ +15.78 after sell → book $8,889.54; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $7,839.93 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1111.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `LVWR` | 745 | $1.49 | $9.61 | — | $6,720.27 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+25.7; leftover $1111.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 13 | $81.40 | $2.03 | — | $5,660.05 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $1111.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FCEL` | 62 | $17.80 | $2.18 | — | $4,554.27 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+13.6; leftover $1111.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `PGEN` | 139 | $7.98 | $2.41 | — | $3,442.64 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $1111.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `SATL` | 202 | $5.49 | $2.61 | — | $2,332.07 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+17.2; leftover $1111.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `LTRX` | 188 | $5.89 | $2.55 | — | $1,222.19 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+15.0; leftover $1111.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `VOYG` | 29 | $37.16 | $2.08 | — | $142.48 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+11.0; leftover $1111.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.48 | ▼ close $8,782.44 vs 09:30 $8,908.37 (session -81.64) | 16:00 close · cash $142.48 · equity $8,782.44 vs 09:30 $8,908.37 (-125.93; session marks -81.64) · 8 name(s) marked open→close (per-name table). GNRC×5 09:30 $209.52 → close $207.44 -10.40; LVWR×745 09:30 $1.49 → close $1.63 +104.30; TEM×13 09:30 $81.40 → close $77.84 -46.28; FCEL×62 09:30 $17.80 → close $18.11 +19.22; PGEN×139 09:30 $7.98 → close $7.78 -27.80; SATL×202 09:30 $5.49 → close $5.11 -75.75; LTRX×188 09:30 $5.89 → close $5.85 -7.52; VOYG×29 09:30 $37.16 → close $35.87 -37.41 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.48 | ▲ 09:30 equity $8,893.09 vs yday $8,782.44 (+110.65) | 09:30 open · cash $142.48 (unchanged overnight, no fees) · equity $8,893.09 vs prior close $8,782.44 (+110.65) · 8 name(s) re-marked at the open (per-name table). GNRC×5 yday $207.44 → 09:30 $210.00 +12.80; LVWR×745 yday $1.63 → 09:30 $1.65 +14.90; TEM×13 yday $77.84 → 09:30 $78.99 +14.95; FCEL×62 yday $18.11 → 09:30 $18.25 +8.93; PGEN×139 yday $7.78 → 09:30 $7.84 +8.34; SATL×202 yday $5.11 → 09:30 $5.21 +20.20; LTRX×188 yday $5.85 → 09:30 $5.96 +20.68; VOYG×29 yday $35.87 → 09:30 $36.21 +9.86 | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 5 | $210.00 | $2.02 | $-1.63 | $1,190.45 | ▼ -1.63 after sell → book $8,891.07; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `LVWR` | 745 | $1.65 | $9.74 | $+99.85 | $2,409.96 | ▲ +99.85 after sell → book $8,881.32; vs 09:30 mark -9.75 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 13 | $78.99 | $2.05 | $-35.41 | $3,434.78 | ▼ -35.41 after sell → book $8,879.28; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FCEL` | 62 | $18.25 | $2.20 | $+23.78 | $4,564.33 | ▲ +23.78 after sell → book $8,877.08; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `PGEN` | 139 | $7.84 | $2.44 | $-24.31 | $5,651.65 | ▼ -24.31 after sell → book $8,874.64; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `SATL` | 202 | $5.21 | $2.65 | $-60.81 | $6,701.42 | ▼ -60.81 after sell → book $8,871.99; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `LTRX` | 188 | $5.96 | $2.60 | $+8.01 | $7,819.30 | ▲ +8.01 after sell → book $8,869.39; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VOYG` | 29 | $36.21 | $2.10 | $-31.72 | $8,867.30 | ▼ -31.72 after sell → book $8,867.30; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 607 | $1.82 | $7.83 | — | $7,751.69 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1108.41 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 191 | $5.80 | $2.56 | — | $6,641.33 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+30.3; leftover $1108.41 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `MSTR` | 6 | $164.58 | $2.01 | — | $5,651.84 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.5; leftover $1108.41 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 515 | $2.15 | $6.64 | — | $4,537.95 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1108.41 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `ABTC` | 103 | $10.71 | $2.30 | — | $3,432.52 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $1108.41 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `ASST` | 35 | $31.64 | $2.10 | — | $2,323.02 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+8.9; leftover $1108.41 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `ABSI` | 110 | $10.06 | $2.32 | — | $1,214.10 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ⚪; ret5=+17.5; leftover $1108.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,214.10 | ▼ close $8,752.93 vs 09:30 $8,893.09 (session -88.60) | 16:00 close · cash $1,214.10 · equity $8,752.93 vs 09:30 $8,893.09 (-140.16; session marks -88.60) · 7 name(s) marked open→close (per-name table). BTBT×607 09:30 $1.82 → close $1.82 -3.03; GEMI×191 09:30 $5.80 → close $5.99 +36.29; MSTR×6 09:30 $164.58 → close $168.50 +23.52; AMTX×515 09:30 $2.15 → close $2.09 -30.90; ABTC×103 09:30 $10.71 → close $10.30 -42.23; ASST×35 09:30 $31.64 → close $30.33 -45.85; ABSI×110 09:30 $10.06 → close $9.82 -26.40 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DVLT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATAT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DK` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `XRX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `WFRD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ZETA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `EYPT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DOCN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ANGX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VSAT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ARQQ` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHLD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COHU` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `IMSR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-21 | `SNDK` | cash | leftover split 1108.41 < 1 share @ 1826.66 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BTBT` | 607 | 2026-09-21 @ $1.82 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1108.41 |
| `GEMI` | 191 | 2026-09-21 @ $5.80 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+30.3; leftover $1108.41 |
| `MSTR` | 6 | 2026-09-21 @ $164.58 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.5; leftover $1108.41 |
| `AMTX` | 515 | 2026-09-21 @ $2.15 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1108.41 |
| `ABTC` | 103 | 2026-09-21 @ $10.71 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $1108.41 |
| `ASST` | 35 | 2026-09-21 @ $31.64 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+8.9; leftover $1108.41 |
| `ABSI` | 110 | 2026-09-21 @ $10.06 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ⚪; ret5=+17.5; leftover $1108.41 |
