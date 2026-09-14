# Factor mine action — `union_macd_xup_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ macd_xup, no 🚨

Cash book **-12.77%** ($8,723) · signal-only (no cash/fees) was -18.33%. Starts YES **5/22**. Fills 82 · skips 45 · realized $-1276.91.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,723.07.

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
| 2026-08-27 | `GEN` | 151 | — | $29.83 | +0.00 | $30.50 | +101.17 | +101.17 | +0.00 | +101.17 |
| 2026-08-27 | `PGY` | 197 | — | $22.93 | +0.00 | $23.26 | +65.01 | +65.01 | +0.00 | +65.01 |
| 2026-08-28 | `GEN` | 151 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +101.17 | — |
| 2026-08-28 | `PGY` | 197 | $23.26 | $23.21 | -9.85 | — | +0.00 | -9.85 | +55.16 | — |
| 2026-08-28 | `LVWR` | 827 | — | $1.39 | +0.00 | $1.35 | -33.08 | -33.08 | +0.00 | -33.08 |
| 2026-08-28 | `TTMI` | 9 | — | $122.81 | +0.00 | $118.65 | -37.44 | -37.44 | +0.00 | -37.44 |
| 2026-08-28 | `ERAS` | 59 | — | $19.25 | +0.00 | $18.03 | -71.98 | -71.98 | +0.00 | -71.98 |
| 2026-08-28 | `NEO` | 62 | — | $18.36 | +0.00 | $18.05 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-28 | `FTNT` | 6 | — | $172.58 | +0.00 | $166.00 | -39.48 | -39.48 | +0.00 | -39.48 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `RBRK` | 11 | — | $98.95 | +0.00 | $93.05 | -64.90 | -64.90 | +0.00 | -64.90 |
| 2026-08-28 | `ULTA` | 2 | — | $542.00 | +0.00 | $517.50 | -49.00 | -49.00 | +0.00 | -49.00 |
| 2026-08-31 | `LVWR` | 827 | $1.35 | $1.30 | -41.35 | — | +0.00 | -41.35 | -74.43 | — |
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
| 2026-09-03 | `VSTM` | 181 | — | $8.03 | +0.00 | $7.98 | -9.05 | -9.05 | +0.00 | -9.05 |
| 2026-09-03 | `PYXS` | 393 | — | $3.71 | +0.00 | $3.56 | -56.99 | -56.99 | +0.00 | -56.99 |
| 2026-09-03 | `MLYS` | 50 | — | $29.15 | +0.00 | $28.27 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-09-03 | `HP` | 30 | — | $47.74 | +0.00 | $45.02 | -81.60 | -81.60 | +0.00 | -81.60 |
| 2026-09-03 | `RSKD` | 218 | — | $6.68 | +0.00 | $6.93 | +54.50 | +54.50 | +0.00 | +54.50 |
| 2026-09-04 | `ATRC` | 27 | $52.46 | $52.03 | -11.61 | — | +0.00 | -11.61 | -22.95 | — |
| 2026-09-04 | `VSTM` | 181 | $7.98 | $7.91 | -12.67 | — | +0.00 | -12.67 | -21.72 | — |
| 2026-09-04 | `PYXS` | 393 | $3.56 | $3.53 | -13.76 | — | +0.00 | -13.76 | -70.74 | — |
| 2026-09-04 | `MLYS` | 50 | $28.27 | $28.00 | -13.50 | — | +0.00 | -13.50 | -57.50 | — |
| 2026-09-04 | `HP` | 30 | $45.02 | $44.59 | -12.90 | — | +0.00 | -12.90 | -94.50 | — |
| 2026-09-04 | `RSKD` | 218 | $6.93 | $6.84 | -19.62 | — | +0.00 | -19.62 | +34.88 | — |
| 2026-09-04 | `EOSE` | 482 | — | $3.52 | +0.00 | $3.88 | +173.52 | +173.52 | +0.00 | +173.52 |
| 2026-09-04 | `DELL` | 3 | — | $513.78 | +0.00 | $524.14 | +31.08 | +31.08 | +0.00 | +31.08 |
| 2026-09-04 | `GSM` | 363 | — | $4.67 | +0.00 | $4.67 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-04 | `RNG` | 22 | — | $75.35 | +0.00 | $73.39 | -43.12 | -43.12 | +0.00 | -43.12 |
| 2026-09-04 | `LULU` | 17 | — | $98.15 | +0.00 | $100.61 | +41.82 | +41.82 | +0.00 | +41.82 |
| 2026-09-08 | `EOSE` | 482 | $3.88 | $3.99 | +53.02 | — | +0.00 | +53.02 | +226.54 | — |
| 2026-09-08 | `DELL` | 3 | $524.14 | $521.15 | -8.97 | — | +0.00 | -8.97 | +22.11 | — |
| 2026-09-08 | `GSM` | 363 | $4.67 | $4.75 | +29.04 | — | +0.00 | +29.04 | +29.04 | — |
| 2026-09-08 | `RNG` | 22 | $73.39 | $72.07 | -29.04 | — | +0.00 | -29.04 | -72.16 | — |
| 2026-09-08 | `LULU` | 17 | $100.61 | $100.58 | -0.51 | — | +0.00 | -0.51 | +41.31 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `DBI` | 491 | — | $5.91 | +0.00 | $5.88 | -14.73 | -14.73 | +0.00 | -14.73 |
| 2026-09-11 | `APPS` | 244 | — | $11.88 | +0.00 | $11.81 | -17.08 | -17.08 | +0.00 | -17.08 |
| 2026-09-11 | `INSP` | 41 | — | $69.88 | +0.00 | $73.00 | +127.92 | +127.92 | +0.00 | +127.92 |
| 2026-09-14 | `DBI` | 491 | $5.88 | $5.86 | -9.82 | — | +0.00 | -9.82 | -24.55 | — |
| 2026-09-14 | `APPS` | 244 | $11.81 | $11.75 | -14.64 | — | +0.00 | -14.64 | -31.72 | — |
| 2026-09-14 | `INSP` | 41 | $73.00 | $72.14 | -35.26 | — | +0.00 | -35.26 | +92.66 | — |

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
| 2026-08-27 | — | $174.94 | AVBP×94, FLNC×264, BE×13 | $9,062.78 | +198.83 | +166.18 | GEN, PGY | AVBP, FLNC, BE | $28.37 | $9,216.09 | GEN×151, PGY×197 |
| 2026-08-28 | +0.75 | $28.37 | GEN×151, PGY×197 | $9,206.24 | -9.85 | -317.10 | LVWR, TTMI, ERAS, NEO, FTNT, ADSK, RBRK, ULTA | GEN, PGY | $394.56 | $8,858.92 | LVWR×827, TTMI×9, ERAS×59, NEO×62, FTNT×6, ADSK×4, RBRK×11, ULTA×2 |
| 2026-08-31 | -5.85 | $394.56 | LVWR×827, TTMI×9, ERAS×59, NEO×62, FTNT×6, ADSK×4, RBRK×11, ULTA×2 | $8,788.97 | -69.95 | +0.00 | — | LVWR, TTMI, ERAS, NEO, FTNT, ADSK, RBRK, ULTA | $8,763.63 | $8,763.63 | — |
| 2026-09-01 | -6.30 | $8,763.63 | — | $8,763.63 | +0.00 | +0.00 | — | — | $8,763.63 | $8,763.63 | — |
| 2026-09-02 | -3.83 | $8,763.63 | — | $8,763.63 | +0.00 | +0.00 | — | — | $8,763.63 | $8,763.63 | — |
| 2026-09-03 | -0.90 | $8,763.63 | — | $8,763.63 | +0.00 | -148.48 | ATRC, VSTM, PYXS, MLYS, HP, RSKD | — | $61.76 | $8,598.45 | ATRC×27, VSTM×181, PYXS×393, MLYS×50, HP×30, RSKD×218 |
| 2026-09-04 | +2.25 | $61.76 | ATRC×27, VSTM×181, PYXS×393, MLYS×50, HP×30, RSKD×218 | $8,514.39 | -84.06 | +203.30 | EOSE, DELL, GSM, RNG, LULU | ATRC, VSTM, PYXS, MLYS, HP, RSKD | $221.02 | $8,683.76 | EOSE×482, DELL×3, GSM×363, RNG×22, LULU×17 |
| 2026-09-08 | -11.47 | $221.02 | EOSE×482, DELL×3, GSM×363, RNG×22, LULU×17 | $8,727.30 | +43.54 | +0.00 | — | EOSE, DELL, GSM, RNG, LULU | $8,710.07 | $8,710.07 | — |
| 2026-09-09 | -13.95 | $8,710.07 | — | $8,710.07 | -0.00 | +0.00 | — | — | $8,710.07 | $8,710.07 | — |
| 2026-09-10 | -13.28 | $8,710.07 | — | $8,710.07 | -0.00 | +0.00 | — | — | $8,710.07 | $8,710.07 | — |
| 2026-09-11 | +0.50 | $8,710.07 | — | $8,710.07 | -0.00 | +96.11 | DBI, APPS, INSP | — | $32.86 | $8,794.58 | DBI×491, APPS×244, INSP×41 |
| 2026-09-14 | -11.00 | $32.86 | DBI×491, APPS×244, INSP×41 | $8,734.86 | -59.72 | +0.00 | — | DBI, APPS, INSP | $8,723.07 | $8,723.07 | — |

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
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 94 | $30.79 | $2.31 | $-44.06 | $3,066.89 | ▼ -44.06 after sell → book $9,060.47; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 264 | $11.52 | $3.47 | $+98.72 | $6,104.69 | ▲ +98.72 after sell → book $9,056.99; vs 09:30 mark -3.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BE` | 13 | $227.10 | $2.06 | $+166.99 | $9,054.93 | ▲ +166.99 after sell → book $9,054.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 151 | $29.83 | $2.44 | — | $4,548.16 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list mover_buy; 🔵; ret5=+7.6; leftover $4527.47 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 197 | $22.93 | $2.58 | — | $28.37 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list mover_buy; 🔵; ret5=+9.5; leftover $4527.47 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.37 | ▲ close $9,216.09 vs 09:30 $9,062.78 (session +166.18) | 16:00 close · cash $28.37 · equity $9,216.09 vs 09:30 $9,062.78 (+153.31; session marks +166.18) · 2 name(s) marked open→close (per-name table). GEN×151 09:30 $29.83 → close $30.50 +101.17; PGY×197 09:30 $22.93 → close $23.26 +65.01 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.37 | ▼ 09:30 equity $9,206.24 vs yday $9,216.09 (-9.85) | 09:30 open · cash $28.37 (unchanged overnight, no fees) · equity $9,206.24 vs prior close $9,216.09 (-9.85) · 2 name(s) re-marked at the open (per-name table). GEN×151 yday $30.50 → 09:30 $30.50 +0.00; PGY×197 yday $23.26 → 09:30 $23.21 -9.85 | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 151 | $30.50 | $2.50 | $+96.22 | $4,631.36 | ▲ +96.22 after sell → book $9,203.73; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 197 | $23.21 | $2.65 | $+49.93 | $9,201.08 | ▲ +49.93 after sell → book $9,201.08; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 827 | $1.39 | $10.67 | — | $8,040.88 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+20.4; leftover $1150.14 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 9 | $122.81 | $2.02 | — | $6,933.58 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1150.14 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 59 | $19.25 | $2.17 | — | $5,795.66 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; ret5=+14.1; leftover $1150.14 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 62 | $18.36 | $2.18 | — | $4,655.16 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+12.8; leftover $1150.14 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FTNT` | 6 | $172.58 | $2.01 | — | $3,617.68 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+14.6; leftover $1150.14 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $2,571.03 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+7.8; leftover $1150.14 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 11 | $98.95 | $2.02 | — | $1,480.56 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+9.7; leftover $1150.14 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 2 | $542.00 | $2.00 | — | $394.56 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+4.8; leftover $1150.14 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $394.56 | ▼ close $8,858.92 vs 09:30 $9,206.24 (session -317.10) | 16:00 close · cash $394.56 · equity $8,858.92 vs 09:30 $9,206.24 (-347.32; session marks -317.10) · 8 name(s) marked open→close (per-name table). LVWR×827 09:30 $1.39 → close $1.35 -33.08; TTMI×9 09:30 $122.81 → close $118.65 -37.44; ERAS×59 09:30 $19.25 → close $18.03 -71.98; NEO×62 09:30 $18.36 → close $18.05 -19.22; FTNT×6 09:30 $172.58 → close $166.00 -39.48; ADSK×4 09:30 $261.16 → close $260.66 -2.00; RBRK×11 09:30 $98.95 → close $93.05 -64.90; ULTA×2 09:30 $542.00 → close $517.50 -49.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $394.56 | ▼ 09:30 equity $8,788.97 vs yday $8,858.92 (-69.95) | 09:30 open · cash $394.56 (unchanged overnight, no fees) · equity $8,788.97 vs prior close $8,858.92 (-69.95) · 8 name(s) re-marked at the open (per-name table). LVWR×827 yday $1.35 → 09:30 $1.30 -41.35; TTMI×9 yday $118.65 → 09:30 $118.83 +1.62; ERAS×59 yday $18.03 → 09:30 $17.87 -9.44; NEO×62 yday $18.05 → 09:30 $17.77 -17.36; FTNT×6 yday $166.00 → 09:30 $166.60 +3.60; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; RBRK×11 yday $93.05 → 09:30 $92.83 -2.42; ULTA×2 yday $517.50 → 09:30 $521.10 +7.20 | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 827 | $1.30 | $10.82 | $-95.91 | $1,458.85 | ▼ -95.91 after sell → book $8,778.16; vs 09:30 mark -10.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 9 | $118.83 | $2.04 | $-39.87 | $2,526.28 | ▼ -39.87 after sell → book $8,776.12; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 59 | $17.87 | $2.19 | $-85.77 | $3,578.43 | ▼ -85.77 after sell → book $8,773.94; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NEO` | 62 | $17.77 | $2.20 | $-40.95 | $4,677.97 | ▼ -40.95 after sell → book $8,771.74; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FTNT` | 6 | $166.60 | $2.03 | $-39.92 | $5,675.54 | ▼ -39.92 after sell → book $8,769.71; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $6,704.36 | ▼ -17.82 after sell → book $8,767.69; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RBRK` | 11 | $92.83 | $2.04 | $-71.39 | $7,723.45 | ▼ -71.39 after sell → book $8,765.65; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 2 | $521.10 | $2.02 | $-45.81 | $8,763.63 | ▼ -45.81 after sell → book $8,763.63; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,763.63 | ▲ close $8,763.63 vs 09:30 $8,788.97 (session +0.00) | 16:00 close · cash $8,763.63 · no lots left · equity $8,763.63. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,763.63 | ▲ 09:30 equity $8,763.63 vs yday $8,763.63 (+0.00) | 09:30 open · cash $8,763.63 · no holdings · equity $8,763.63 vs prior close $8,763.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,763.63 | ▲ close $8,763.63 vs 09:30 $8,763.63 (session +0.00) | 16:00 close · cash $8,763.63 · no lots left · equity $8,763.63. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,763.63 | ▲ 09:30 equity $8,763.63 vs yday $8,763.63 (+0.00) | 09:30 open · cash $8,763.63 · no holdings · equity $8,763.63 vs prior close $8,763.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,763.63 | ▲ close $8,763.63 vs 09:30 $8,763.63 (session +0.00) | 16:00 close · cash $8,763.63 · no lots left · equity $8,763.63. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,763.63 | ▲ 09:30 equity $8,763.63 vs yday $8,763.63 (+0.00) | 09:30 open · cash $8,763.63 · no holdings · equity $8,763.63 vs prior close $8,763.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $52.88 | $2.07 | — | $7,333.80 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1460.60 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 181 | $8.03 | $2.53 | — | $5,877.84 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1460.60 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `PYXS` | 393 | $3.71 | $5.07 | — | $4,414.74 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+12.3; leftover $1460.60 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MLYS` | 50 | $29.15 | $2.14 | — | $2,955.10 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+12.2; leftover $1460.60 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HP` | 30 | $47.74 | $2.08 | — | $1,520.82 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+15.1; leftover $1460.60 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RSKD` | 218 | $6.68 | $2.81 | — | $61.76 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+11.4; leftover $1460.60 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.76 | ▼ close $8,598.45 vs 09:30 $8,763.63 (session -148.48) | 16:00 close · cash $61.76 · equity $8,598.45 vs 09:30 $8,763.63 (-165.18; session marks -148.48) · 6 name(s) marked open→close (per-name table). ATRC×27 09:30 $52.88 → close $52.46 -11.34; VSTM×181 09:30 $8.03 → close $7.98 -9.05; PYXS×393 09:30 $3.71 → close $3.56 -56.99; MLYS×50 09:30 $29.15 → close $28.27 -44.00; HP×30 09:30 $47.74 → close $45.02 -81.60; RSKD×218 09:30 $6.68 → close $6.93 +54.50 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.76 | ▼ 09:30 equity $8,514.39 vs yday $8,598.45 (-84.06) | 09:30 open · cash $61.76 (unchanged overnight, no fees) · equity $8,514.39 vs prior close $8,598.45 (-84.06) · 6 name(s) re-marked at the open (per-name table). ATRC×27 yday $52.46 → 09:30 $52.03 -11.61; VSTM×181 yday $7.98 → 09:30 $7.91 -12.67; PYXS×393 yday $3.56 → 09:30 $3.53 -13.76; MLYS×50 yday $28.27 → 09:30 $28.00 -13.50; HP×30 yday $45.02 → 09:30 $44.59 -12.90; RSKD×218 yday $6.93 → 09:30 $6.84 -19.62 | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 27 | $52.03 | $2.09 | $-27.11 | $1,464.48 | ▼ -27.11 after sell → book $8,512.30; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 181 | $7.91 | $2.57 | $-26.83 | $2,893.62 | ▼ -26.83 after sell → book $8,509.73; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PYXS` | 393 | $3.53 | $5.15 | $-80.96 | $4,275.76 | ▼ -80.96 after sell → book $8,504.58; vs 09:30 mark -5.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MLYS` | 50 | $28.00 | $2.16 | $-61.80 | $5,673.60 | ▼ -61.80 after sell → book $8,502.42; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `HP` | 30 | $44.59 | $2.10 | $-98.68 | $7,009.20 | ▼ -98.68 after sell → book $8,500.32; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RSKD` | 218 | $6.84 | $2.86 | $+29.21 | $8,497.46 | ▲ +29.21 after sell → book $8,497.46; vs 09:30 mark -2.86 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 482 | $3.52 | $6.22 | — | $6,794.60 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1699.49 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 3 | $513.78 | $2.00 | — | $5,251.26 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1699.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GSM` | 363 | $4.67 | $4.68 | — | $3,551.37 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; ret5=+11.9; leftover $1699.49 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `RNG` | 22 | $75.35 | $2.06 | — | $1,891.61 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+11.4; leftover $1699.49 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 17 | $98.15 | $2.04 | — | $221.02 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+5.9; leftover $1699.49 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $221.02 | ▲ close $8,683.76 vs 09:30 $8,514.39 (session +203.30) | 16:00 close · cash $221.02 · equity $8,683.76 vs 09:30 $8,514.39 (+169.37; session marks +203.30) · 5 name(s) marked open→close (per-name table). EOSE×482 09:30 $3.52 → close $3.88 +173.52; DELL×3 09:30 $513.78 → close $524.14 +31.08; GSM×363 09:30 $4.67 → close $4.67 +0.00; RNG×22 09:30 $75.35 → close $73.39 -43.12; LULU×17 09:30 $98.15 → close $100.61 +41.82 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $221.02 | ▲ 09:30 equity $8,727.30 vs yday $8,683.76 (+43.54) | 09:30 open · cash $221.02 (unchanged overnight, no fees) · equity $8,727.30 vs prior close $8,683.76 (+43.54) · 5 name(s) re-marked at the open (per-name table). EOSE×482 yday $3.88 → 09:30 $3.99 +53.02; DELL×3 yday $524.14 → 09:30 $521.15 -8.97; GSM×363 yday $4.67 → 09:30 $4.75 +29.04; RNG×22 yday $73.39 → 09:30 $72.07 -29.04; LULU×17 yday $100.61 → 09:30 $100.58 -0.51 | — |
| 2026-09-08 09:30 ET | **SELL** | `EOSE` | 482 | $3.99 | $6.31 | $+214.01 | $2,137.89 | ▲ +214.01 after sell → book $8,720.99; vs 09:30 mark -6.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 3 | $521.15 | $2.02 | $+18.09 | $3,699.32 | ▲ +18.09 after sell → book $8,718.97; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `GSM` | 363 | $4.75 | $4.76 | $+19.60 | $5,418.81 | ▲ +19.60 after sell → book $8,714.21; vs 09:30 mark -4.76 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `RNG` | 22 | $72.07 | $2.08 | $-76.29 | $7,002.27 | ▼ -76.29 after sell → book $8,712.13; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 17 | $100.58 | $2.06 | $+37.20 | $8,710.07 | ▲ +37.20 after sell → book $8,710.07; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,710.07 | ▲ close $8,710.07 vs 09:30 $8,727.30 (session +0.00) | 16:00 close · cash $8,710.07 · no lots left · equity $8,710.07. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,710.07 | ▲ 09:30 equity $8,710.07 vs yday $8,710.07 (-0.00) | 09:30 open · cash $8,710.07 · no holdings · equity $8,710.07 vs prior close $8,710.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,710.07 | ▲ close $8,710.07 vs 09:30 $8,710.07 (session +0.00) | 16:00 close · cash $8,710.07 · no lots left · equity $8,710.07. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,710.07 | ▲ 09:30 equity $8,710.07 vs yday $8,710.07 (-0.00) | 09:30 open · cash $8,710.07 · no holdings · equity $8,710.07 vs prior close $8,710.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,710.07 | ▲ close $8,710.07 vs 09:30 $8,710.07 (session +0.00) | 16:00 close · cash $8,710.07 · no lots left · equity $8,710.07. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,710.07 | ▲ 09:30 equity $8,710.07 vs yday $8,710.07 (-0.00) | 09:30 open · cash $8,710.07 · no holdings · equity $8,710.07 vs prior close $8,710.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 491 | $5.91 | $6.33 | — | $5,801.92 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $2903.36 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `APPS` | 244 | $11.88 | $3.15 | — | $2,900.06 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+20.7; leftover $2903.36 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INSP` | 41 | $69.88 | $2.11 | — | $32.86 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+8.0; leftover $2903.36 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.86 | ▲ close $8,794.58 vs 09:30 $8,710.07 (session +96.11) | 16:00 close · cash $32.86 · equity $8,794.58 vs 09:30 $8,710.07 (+84.51; session marks +96.11) · 3 name(s) marked open→close (per-name table). DBI×491 09:30 $5.91 → close $5.88 -14.73; APPS×244 09:30 $11.88 → close $11.81 -17.08; INSP×41 09:30 $69.88 → close $73.00 +127.92 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.86 | ▼ 09:30 equity $8,734.86 vs yday $8,794.58 (-59.72) | 09:30 open · cash $32.86 (unchanged overnight, no fees) · equity $8,734.86 vs prior close $8,794.58 (-59.72) · 3 name(s) re-marked at the open (per-name table). DBI×491 yday $5.88 → 09:30 $5.86 -9.82; APPS×244 yday $11.81 → 09:30 $11.75 -14.64; INSP×41 yday $73.00 → 09:30 $72.14 -35.26 | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 491 | $5.86 | $6.44 | $-37.32 | $2,903.68 | ▼ -37.32 after sell → book $8,728.42; vs 09:30 mark -6.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `APPS` | 244 | $11.75 | $3.21 | $-38.08 | $5,767.47 | ▼ -38.08 after sell → book $8,725.21; vs 09:30 mark -3.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INSP` | 41 | $72.14 | $2.15 | $+88.40 | $8,723.07 | ▲ +88.40 after sell → book $8,723.07; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,723.07 | ▲ close $8,723.07 vs 09:30 $8,734.86 (session +0.00) | 16:00 close · cash $8,723.07 · no lots left · equity $8,723.07. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
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
| 2026-09-14 | `VICR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `IMSR` | hard_red | hard-red S=-11.00 sit; no new buys |
