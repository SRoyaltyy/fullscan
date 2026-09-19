# Factor mine action — `union_macd_xup_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ macd_xup, no 🚨

Cash book **-8.60%** ($9,140) · signal-only (no cash/fees) was -14.92%. Starts YES **5/26**. Fills 144 · skips 55 · realized $-717.03.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $291.14.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `BCAR` | 821 | — | $6.09 | +0.00 | $5.83 | -213.46 | -213.46 | +0.00 | -213.46 |
| 2026-08-14 | `ZIM` | 183 | — | $27.25 | +0.00 | $28.14 | +162.87 | +162.87 | +0.00 | +162.87 |
| 2026-08-17 | `BCAR` | 821 | $5.83 | $5.99 | +131.36 | — | +0.00 | +131.36 | -82.10 | — |
| 2026-08-17 | `ZIM` | 183 | $28.14 | $28.83 | +126.27 | — | +0.00 | +126.27 | +289.14 | — |
| 2026-08-17 | `RDDT` | 57 | — | $177.51 | +0.00 | $164.50 | -741.57 | -741.57 | +0.00 | -741.57 |
| 2026-08-18 | `RDDT` | 57 | $164.50 | $166.10 | +91.20 | — | +0.00 | +91.20 | -650.37 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BNTX` | 12 | — | $109.06 | +0.00 | $110.89 | +21.96 | +21.96 | +0.00 | +21.96 |
| 2026-08-20 | `HUMA` | 1924 | — | $0.71 | +0.00 | $0.68 | -50.02 | -50.02 | +0.00 | -50.02 |
| 2026-08-20 | `EL` | 13 | — | $97.43 | +0.00 | $96.15 | -16.64 | -16.64 | +0.00 | -16.64 |
| 2026-08-20 | `SBET` | 180 | — | $7.55 | +0.00 | $7.59 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-08-20 | `BMNR` | 63 | — | $21.46 | +0.00 | $21.57 | +6.93 | +6.93 | +0.00 | +6.93 |
| 2026-08-20 | `SUI` | 11 | — | $121.21 | +0.00 | $122.29 | +11.88 | +11.88 | +0.00 | +11.88 |
| 2026-08-20 | `WEAV` | 185 | — | $7.32 | +0.00 | $7.29 | -5.55 | -5.55 | +0.00 | -5.55 |
| 2026-08-21 | `BNTX` | 12 | $110.89 | $110.92 | +0.36 | — | +0.00 | +0.36 | +22.32 | — |
| 2026-08-21 | `HUMA` | 1924 | $0.68 | $0.67 | -13.47 | — | +0.00 | -13.47 | -63.49 | — |
| 2026-08-21 | `EL` | 13 | $96.15 | $96.75 | +7.80 | — | +0.00 | +7.80 | -8.84 | — |
| 2026-08-21 | `SBET` | 180 | $7.59 | $7.87 | +50.40 | — | +0.00 | +50.40 | +57.60 | — |
| 2026-08-21 | `BMNR` | 63 | $21.57 | $22.25 | +42.84 | — | +0.00 | +42.84 | +49.77 | — |
| 2026-08-21 | `SUI` | 11 | $122.29 | $122.41 | +1.32 | — | +0.00 | +1.32 | +13.20 | — |
| 2026-08-21 | `WEAV` | 185 | $7.29 | $7.30 | +1.85 | — | +0.00 | +1.85 | -3.70 | — |
| 2026-08-21 | `CF` | 14 | — | $127.43 | +0.00 | $129.60 | +30.38 | +30.38 | +0.00 | +30.38 |
| 2026-08-21 | `INDP` | 1370 | — | $1.39 | +0.00 | $1.29 | -137.00 | -137.00 | +0.00 | -137.00 |
| 2026-08-21 | `MRVI` | 230 | — | $8.28 | +0.00 | $8.64 | +82.80 | +82.80 | +0.00 | +82.80 |
| 2026-08-21 | `MARA` | 162 | — | $11.70 | +0.00 | $11.26 | -71.28 | -71.28 | +0.00 | -71.28 |
| 2026-08-21 | `ILMN` | 8 | — | $212.40 | +0.00 | $219.40 | +56.00 | +56.00 | +0.00 | +56.00 |
| 2026-08-24 | `CF` | 14 | $129.60 | $129.99 | +5.46 | — | +0.00 | +5.46 | +35.84 | — |
| 2026-08-24 | `INDP` | 1370 | $1.29 | $1.24 | -68.50 | — | +0.00 | -68.50 | -205.50 | — |
| 2026-08-24 | `MRVI` | 230 | $8.64 | $8.59 | -11.50 | — | +0.00 | -11.50 | +71.30 | — |
| 2026-08-24 | `MARA` | 162 | $11.26 | $11.17 | -14.58 | — | +0.00 | -14.58 | -85.86 | — |
| 2026-08-24 | `ILMN` | 8 | $219.40 | $215.98 | -27.36 | — | +0.00 | -27.36 | +28.64 | — |
| 2026-08-25 | `ZURA` | 731 | — | $6.37 | +0.00 | $6.32 | -36.55 | -36.55 | +0.00 | -36.55 |
| 2026-08-25 | `RHI` | 106 | — | $43.76 | +0.00 | $44.90 | +120.84 | +120.84 | +0.00 | +120.84 |
| 2026-08-26 | `ZURA` | 731 | $6.32 | $6.13 | -138.89 | — | +0.00 | -138.89 | -175.44 | — |
| 2026-08-26 | `RHI` | 106 | $44.90 | $44.33 | -60.42 | — | +0.00 | -60.42 | +60.42 | — |
| 2026-08-26 | `AVBP` | 98 | — | $31.21 | +0.00 | $31.14 | -6.86 | -6.86 | +0.00 | -6.86 |
| 2026-08-26 | `FLNC` | 275 | — | $11.12 | +0.00 | $11.08 | -11.00 | -11.00 | +0.00 | -11.00 |
| 2026-08-26 | `BE` | 14 | — | $213.94 | +0.00 | $218.21 | +59.78 | +59.78 | +0.00 | +59.78 |
| 2026-08-27 | `AVBP` | 98 | $31.14 | $30.79 | -34.30 | — | +0.00 | -34.30 | -41.16 | — |
| 2026-08-27 | `FLNC` | 275 | $11.08 | $11.52 | +121.00 | — | +0.00 | +121.00 | +110.00 | — |
| 2026-08-27 | `BE` | 14 | $218.21 | $227.10 | +124.46 | — | +0.00 | +124.46 | +184.24 | — |
| 2026-08-27 | `BZ` | 72 | — | $18.50 | +0.00 | $18.00 | -36.00 | -36.00 | +0.00 | -36.00 |
| 2026-08-27 | `VYX` | 150 | — | $8.95 | +0.00 | $9.18 | +34.50 | +34.50 | +0.00 | +34.50 |
| 2026-08-27 | `GAP` | 64 | — | $20.75 | +0.00 | $20.79 | +2.56 | +2.56 | +0.00 | +2.56 |
| 2026-08-27 | `AEO` | 77 | — | $17.27 | +0.00 | $16.69 | -44.66 | -44.66 | +0.00 | -44.66 |
| 2026-08-27 | `SMTC` | 9 | — | $149.40 | +0.00 | $142.43 | -62.73 | -62.73 | +0.00 | -62.73 |
| 2026-08-27 | `GEN` | 45 | — | $29.83 | +0.00 | $30.50 | +30.15 | +30.15 | +0.00 | +30.15 |
| 2026-08-27 | `PGY` | 58 | — | $22.93 | +0.00 | $23.26 | +19.14 | +19.14 | +0.00 | +19.14 |
| 2026-08-28 | `BZ` | 72 | $18.00 | $18.15 | +10.80 | — | +0.00 | +10.80 | -25.20 | — |
| 2026-08-28 | `VYX` | 150 | $9.18 | $9.13 | -7.50 | — | +0.00 | -7.50 | +27.00 | — |
| 2026-08-28 | `GAP` | 64 | $20.79 | $24.69 | +249.60 | — | +0.00 | +249.60 | +252.16 | — |
| 2026-08-28 | `AEO` | 77 | $16.69 | $17.06 | +28.49 | — | +0.00 | +28.49 | -16.17 | — |
| 2026-08-28 | `SMTC` | 9 | $142.43 | $141.76 | -6.03 | — | +0.00 | -6.03 | -68.76 | — |
| 2026-08-28 | `GEN` | 45 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +30.15 | — |
| 2026-08-28 | `PGY` | 58 | $23.26 | $23.21 | -2.90 | — | +0.00 | -2.90 | +16.24 | — |
| 2026-08-28 | `LVWR` | 863 | — | $1.39 | +0.00 | $1.35 | -34.52 | -34.52 | +0.00 | -34.52 |
| 2026-08-28 | `TTMI` | 9 | — | $122.81 | +0.00 | $118.65 | -37.44 | -37.44 | +0.00 | -37.44 |
| 2026-08-28 | `ERAS` | 62 | — | $19.25 | +0.00 | $18.03 | -75.64 | -75.64 | +0.00 | -75.64 |
| 2026-08-28 | `NEO` | 65 | — | $18.36 | +0.00 | $18.05 | -20.15 | -20.15 | +0.00 | -20.15 |
| 2026-08-28 | `FTNT` | 6 | — | $172.58 | +0.00 | $166.00 | -39.48 | -39.48 | +0.00 | -39.48 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `RBRK` | 12 | — | $98.95 | +0.00 | $93.05 | -70.80 | -70.80 | +0.00 | -70.80 |
| 2026-08-28 | `ULTA` | 2 | — | $542.00 | +0.00 | $517.50 | -49.00 | -49.00 | +0.00 | -49.00 |
| 2026-08-31 | `LVWR` | 863 | $1.35 | $1.30 | -43.15 | — | +0.00 | -43.15 | -77.67 | — |
| 2026-08-31 | `TTMI` | 9 | $118.65 | $118.83 | +1.62 | — | +0.00 | +1.62 | -35.82 | — |
| 2026-08-31 | `ERAS` | 62 | $18.03 | $17.87 | -9.92 | — | +0.00 | -9.92 | -85.56 | — |
| 2026-08-31 | `NEO` | 65 | $18.05 | $17.77 | -18.20 | — | +0.00 | -18.20 | -38.35 | — |
| 2026-08-31 | `FTNT` | 6 | $166.00 | $166.60 | +3.60 | — | +0.00 | +3.60 | -35.88 | — |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | — | +0.00 | -11.80 | -13.80 | — |
| 2026-08-31 | `RBRK` | 12 | $93.05 | $92.83 | -2.64 | — | +0.00 | -2.64 | -73.44 | — |
| 2026-08-31 | `ULTA` | 2 | $517.50 | $521.10 | +7.20 | — | +0.00 | +7.20 | -41.80 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 28 | — | $52.88 | +0.00 | $52.46 | -11.76 | -11.76 | +0.00 | -11.76 |
| 2026-09-03 | `VSTM` | 189 | — | $8.03 | +0.00 | $7.98 | -9.45 | -9.45 | +0.00 | -9.45 |
| 2026-09-03 | `PYXS` | 410 | — | $3.71 | +0.00 | $3.56 | -59.45 | -59.45 | +0.00 | -59.45 |
| 2026-09-03 | `MLYS` | 52 | — | $29.15 | +0.00 | $28.27 | -45.76 | -45.76 | +0.00 | -45.76 |
| 2026-09-03 | `HP` | 31 | — | $47.74 | +0.00 | $45.02 | -84.32 | -84.32 | +0.00 | -84.32 |
| 2026-09-03 | `RSKD` | 228 | — | $6.68 | +0.00 | $6.93 | +57.00 | +57.00 | +0.00 | +57.00 |
| 2026-09-04 | `ATRC` | 28 | $52.46 | $52.03 | -12.04 | — | +0.00 | -12.04 | -23.80 | — |
| 2026-09-04 | `VSTM` | 189 | $7.98 | $7.91 | -13.23 | — | +0.00 | -13.23 | -22.68 | — |
| 2026-09-04 | `PYXS` | 410 | $3.56 | $3.53 | -14.35 | — | +0.00 | -14.35 | -73.80 | — |
| 2026-09-04 | `MLYS` | 52 | $28.27 | $28.00 | -14.04 | — | +0.00 | -14.04 | -59.80 | — |
| 2026-09-04 | `HP` | 31 | $45.02 | $44.59 | -13.33 | — | +0.00 | -13.33 | -97.65 | — |
| 2026-09-04 | `RSKD` | 228 | $6.93 | $6.84 | -20.52 | — | +0.00 | -20.52 | +36.48 | — |
| 2026-09-04 | `EOSE` | 315 | — | $3.52 | +0.00 | $3.88 | +113.40 | +113.40 | +0.00 | +113.40 |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-04 | `GSM` | 237 | — | $4.67 | +0.00 | $4.67 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-04 | `RNG` | 14 | — | $75.35 | +0.00 | $73.39 | -27.44 | -27.44 | +0.00 | -27.44 |
| 2026-09-04 | `LULU` | 11 | — | $98.15 | +0.00 | $100.61 | +27.06 | +27.06 | +0.00 | +27.06 |
| 2026-09-04 | `CHPT` | 119 | — | $9.28 | +0.00 | $9.89 | +72.59 | +72.59 | +0.00 | +72.59 |
| 2026-09-04 | `AMX` | 48 | — | $23.03 | +0.00 | $23.00 | -1.44 | -1.44 | +0.00 | -1.44 |
| 2026-09-04 | `PFG` | 9 | — | $117.03 | +0.00 | $116.68 | -3.15 | -3.15 | +0.00 | -3.15 |
| 2026-09-08 | `EOSE` | 315 | $3.88 | $3.99 | +34.65 | — | +0.00 | +34.65 | +148.05 | — |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-08 | `GSM` | 237 | $4.67 | $4.75 | +18.96 | — | +0.00 | +18.96 | +18.96 | — |
| 2026-09-08 | `RNG` | 14 | $73.39 | $72.07 | -18.48 | — | +0.00 | -18.48 | -45.92 | — |
| 2026-09-08 | `LULU` | 11 | $100.61 | $100.58 | -0.33 | — | +0.00 | -0.33 | +26.73 | — |
| 2026-09-08 | `CHPT` | 119 | $9.89 | $9.91 | +2.38 | — | +0.00 | +2.38 | +74.97 | — |
| 2026-09-08 | `AMX` | 48 | $23.00 | $23.15 | +7.20 | — | +0.00 | +7.20 | +5.76 | — |
| 2026-09-08 | `PFG` | 9 | $116.68 | $115.81 | -7.83 | — | +0.00 | -7.83 | -10.98 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `DBI` | 306 | — | $5.91 | +0.00 | $5.88 | -9.18 | -9.18 | +0.00 | -9.18 |
| 2026-09-11 | `APPS` | 152 | — | $11.88 | +0.00 | $11.81 | -10.64 | -10.64 | +0.00 | -10.64 |
| 2026-09-11 | `INSP` | 25 | — | $69.88 | +0.00 | $73.00 | +78.00 | +78.00 | +0.00 | +78.00 |
| 2026-09-11 | `WCC` | 5 | — | $350.21 | +0.00 | $356.32 | +30.55 | +30.55 | +0.00 | +30.55 |
| 2026-09-11 | `OBE` | 144 | — | $12.55 | +0.00 | $12.97 | +60.48 | +60.48 | +0.00 | +60.48 |
| 2026-09-14 | `DBI` | 306 | $5.88 | $5.86 | -6.12 | — | +0.00 | -6.12 | -15.30 | — |
| 2026-09-14 | `APPS` | 152 | $11.81 | $11.75 | -9.12 | — | +0.00 | -9.12 | -19.76 | — |
| 2026-09-14 | `INSP` | 25 | $73.00 | $72.14 | -21.50 | — | +0.00 | -21.50 | +56.50 | — |
| 2026-09-14 | `WCC` | 5 | $356.32 | $342.12 | -71.00 | $336.73 | -26.95 | -97.95 | -40.45 | -67.40 |
| 2026-09-14 | `OBE` | 144 | $12.97 | $13.57 | +86.40 | — | +0.00 | +86.40 | +146.88 | — |
| 2026-09-15 | `WCC` | 5 | $336.73 | $338.28 | +7.75 | — | +0.00 | +7.75 | -59.65 | — |
| 2026-09-16 | `HLP` | 635 | — | $1.80 | +0.00 | $2.07 | +171.45 | +171.45 | +0.00 | +171.45 |
| 2026-09-16 | `FTRE` | 57 | — | $19.75 | +0.00 | $19.97 | +12.54 | +12.54 | +0.00 | +12.54 |
| 2026-09-16 | `SDGR` | 49 | — | $23.29 | +0.00 | $23.93 | +31.36 | +31.36 | +0.00 | +31.36 |
| 2026-09-16 | `RVTY` | 8 | — | $140.88 | +0.00 | $145.73 | +38.80 | +38.80 | +0.00 | +38.80 |
| 2026-09-16 | `TENB` | 31 | — | $36.86 | +0.00 | $36.37 | -15.19 | -15.19 | +0.00 | -15.19 |
| 2026-09-16 | `MRCY` | 13 | — | $87.52 | +0.00 | $87.25 | -3.51 | -3.51 | +0.00 | -3.51 |
| 2026-09-16 | `RBRK` | 11 | — | $101.97 | +0.00 | $104.98 | +33.11 | +33.11 | +0.00 | +33.11 |
| 2026-09-16 | `DOCU` | 16 | — | $70.60 | +0.00 | $70.40 | -3.20 | -3.20 | +0.00 | -3.20 |
| 2026-09-17 | `HLP` | 635 | $2.07 | $2.10 | +19.05 | — | +0.00 | +19.05 | +190.50 | — |
| 2026-09-17 | `FTRE` | 57 | $19.97 | $20.31 | +19.38 | — | +0.00 | +19.38 | +31.92 | — |
| 2026-09-17 | `SDGR` | 49 | $23.93 | $24.09 | +7.84 | — | +0.00 | +7.84 | +39.20 | — |
| 2026-09-17 | `RVTY` | 8 | $145.73 | $147.61 | +15.04 | — | +0.00 | +15.04 | +53.84 | — |
| 2026-09-17 | `TENB` | 31 | $36.37 | $35.89 | -14.88 | — | +0.00 | -14.88 | -30.07 | — |
| 2026-09-17 | `MRCY` | 13 | $87.25 | $89.27 | +26.26 | — | +0.00 | +26.26 | +22.75 | — |
| 2026-09-17 | `RBRK` | 11 | $104.98 | $102.56 | -26.62 | — | +0.00 | -26.62 | +6.49 | — |
| 2026-09-17 | `DOCU` | 16 | $70.40 | $69.16 | -19.84 | — | +0.00 | -19.84 | -23.04 | — |
| 2026-09-17 | `IOVA` | 152 | — | $10.25 | +0.00 | $10.02 | -34.96 | -34.96 | +0.00 | -34.96 |
| 2026-09-17 | `AMN` | 44 | — | $34.93 | +0.00 | $34.55 | -16.72 | -16.72 | +0.00 | -16.72 |
| 2026-09-17 | `SABR` | 652 | — | $2.40 | +0.00 | $2.32 | -52.16 | -52.16 | +0.00 | -52.16 |
| 2026-09-17 | `BRKR` | 25 | — | $61.90 | +0.00 | $63.05 | +28.75 | +28.75 | +0.00 | +28.75 |
| 2026-09-17 | `ADPT` | 55 | — | $28.23 | +0.00 | $28.55 | +17.60 | +17.60 | +0.00 | +17.60 |
| 2026-09-17 | `WCC` | 4 | — | $344.29 | +0.00 | $339.32 | -19.88 | -19.88 | +0.00 | -19.88 |
| 2026-09-18 | `IOVA` | 152 | $10.02 | $10.12 | +15.20 | — | +0.00 | +15.20 | -19.76 | — |
| 2026-09-18 | `AMN` | 44 | $34.55 | $34.52 | -1.32 | — | +0.00 | -1.32 | -18.04 | — |
| 2026-09-18 | `SABR` | 652 | $2.32 | $2.29 | -19.56 | — | +0.00 | -19.56 | -71.72 | — |
| 2026-09-18 | `BRKR` | 25 | $63.05 | $63.37 | +8.00 | — | +0.00 | +8.00 | +36.75 | — |
| 2026-09-18 | `ADPT` | 55 | $28.55 | $28.55 | +0.00 | — | +0.00 | +0.00 | +17.60 | — |
| 2026-09-18 | `WCC` | 4 | $339.32 | $340.45 | +4.52 | — | +0.00 | +4.52 | -15.36 | — |
| 2026-09-18 | `ILMN` | 4 | — | $249.13 | +0.00 | $239.62 | -38.04 | -38.04 | +0.00 | -38.04 |
| 2026-09-18 | `LVWR` | 778 | — | $1.49 | +0.00 | $1.63 | +108.92 | +108.92 | +0.00 | +108.92 |
| 2026-09-18 | `GNRC` | 5 | — | $209.52 | +0.00 | $207.44 | -10.40 | -10.40 | +0.00 | -10.40 |
| 2026-09-18 | `TEM` | 14 | — | $81.40 | +0.00 | $77.84 | -49.84 | -49.84 | +0.00 | -49.84 |
| 2026-09-18 | `FCEL` | 65 | — | $17.80 | +0.00 | $18.11 | +20.15 | +20.15 | +0.00 | +20.15 |
| 2026-09-18 | `PGEN` | 145 | — | $7.98 | +0.00 | $7.78 | -29.00 | -29.00 | +0.00 | -29.00 |
| 2026-09-18 | `SATL` | 211 | — | $5.49 | +0.00 | $5.11 | -79.12 | -79.12 | +0.00 | -79.12 |
| 2026-09-18 | `VOYG` | 31 | — | $37.16 | +0.00 | $35.87 | -39.99 | -39.99 | +0.00 | -39.99 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -50.59 | BCAR, ZIM | — | $0.23 | $9,936.28 | BCAR×821, ZIM×183 |
| 2026-08-17 | +2.25 | $0.23 | BCAR×821, ZIM×183 | $10,193.91 | +257.63 | -741.57 | RDDT | BCAR, ZIM | $60.30 | $9,436.80 | RDDT×57 |
| 2026-08-18 | -6.20 | $60.30 | RDDT×57 | $9,528.00 | +91.20 | +0.00 | — | RDDT | $9,525.75 | $9,525.75 | — |
| 2026-08-19 | -7.20 | $9,525.75 | — | $9,525.75 | +0.00 | +0.00 | — | — | $9,525.75 | $9,525.75 | — |
| 2026-08-20 | +1.12 | $9,525.75 | — | $9,525.75 | +0.00 | -24.24 | BNTX, HUMA, EL, SBET, BMNR, SUI, WEAV | — | $158.98 | $9,468.80 | BNTX×12, HUMA×1924, EL×13, SBET×180, BMNR×63, SUI×11, WEAV×185 |
| 2026-08-21 | +3.25 | $158.98 | BNTX×12, HUMA×1924, EL×13, SBET×180, BMNR×63, SUI×11, WEAV×185 | $9,559.91 | +91.11 | -39.10 | CF, INDP, MRVI, MARA, ILMN | BNTX, HUMA, EL, SBET, BMNR, SUI, WEAV | $312.86 | $9,461.08 | CF×14, INDP×1370, MRVI×230, MARA×162, ILMN×8 |
| 2026-08-24 | -5.17 | $312.86 | CF×14, INDP×1370, MRVI×230, MARA×162, ILMN×8 | $9,344.60 | -116.48 | +0.00 | — | CF, INDP, MRVI, MARA, ILMN | $9,317.05 | $9,317.05 | — |
| 2026-08-25 | +1.80 | $9,317.05 | — | $9,317.05 | -0.00 | +84.29 | ZURA, RHI | — | $10.28 | $9,389.60 | ZURA×731, RHI×106 |
| 2026-08-26 | +2.02 | $10.28 | ZURA×731, RHI×106 | $9,190.29 | -199.31 | +41.92 | AVBP, FLNC, BE | ZURA, RHI | $58.74 | $9,212.40 | AVBP×98, FLNC×275, BE×14 |
| 2026-08-27 | — | $58.74 | AVBP×98, FLNC×275, BE×14 | $9,423.56 | +211.16 | -57.04 | BZ, VYX, GAP, AEO, SMTC, GEN, PGY | AVBP, FLNC, BE | $51.01 | $9,343.15 | BZ×72, VYX×150, GAP×64, AEO×77, SMTC×9, GEN×45, PGY×58 |
| 2026-08-28 | +0.75 | $51.01 | BZ×72, VYX×150, GAP×64, AEO×77, SMTC×9, GEN×45, PGY×58 | $9,615.61 | +272.46 | -329.03 | LVWR, TTMI, ERAS, NEO, FTNT, ADSK, RBRK, ULTA | BZ, VYX, GAP, AEO, SMTC, GEN, PGY | $531.27 | $9,245.52 | LVWR×863, TTMI×9, ERAS×62, NEO×65, FTNT×6, ADSK×4, RBRK×12, ULTA×2 |
| 2026-08-31 | -5.85 | $531.27 | LVWR×863, TTMI×9, ERAS×62, NEO×65, FTNT×6, ADSK×4, RBRK×12, ULTA×2 | $9,172.23 | -73.29 | +0.00 | — | LVWR, TTMI, ERAS, NEO, FTNT, ADSK, RBRK, ULTA | $9,146.39 | $9,146.39 | — |
| 2026-09-01 | -6.30 | $9,146.39 | — | $9,146.39 | +0.00 | +0.00 | — | — | $9,146.39 | $9,146.39 | — |
| 2026-09-02 | -3.83 | $9,146.39 | — | $9,146.39 | +0.00 | +0.00 | — | — | $9,146.39 | $9,146.39 | — |
| 2026-09-03 | -0.90 | $9,146.39 | — | $9,146.39 | +0.00 | -153.74 | ATRC, VSTM, PYXS, MLYS, HP, RSKD | — | $91.11 | $8,975.56 | ATRC×28, VSTM×189, PYXS×410, MLYS×52, HP×31, RSKD×228 |
| 2026-09-04 | +2.25 | $91.11 | ATRC×28, VSTM×189, PYXS×410, MLYS×52, HP×31, RSKD×228 | $8,888.05 | -87.51 | +201.74 | EOSE, DELL, GSM, RNG, LULU, CHPT, AMX, PFG | ATRC, VSTM, PYXS, MLYS, HP, RSKD | $210.32 | $9,052.79 | EOSE×315, DELL×2, GSM×237, RNG×14, LULU×11, CHPT×119, AMX×48, PFG×9 |
| 2026-09-08 | -11.47 | $210.32 | EOSE×315, DELL×2, GSM×237, RNG×14, LULU×11, CHPT×119, AMX×48, PFG×9 | $9,083.36 | +30.57 | +0.00 | — | EOSE, DELL, GSM, RNG, LULU, CHPT, AMX, PFG | $9,063.45 | $9,063.45 | — |
| 2026-09-09 | -13.95 | $9,063.45 | — | $9,063.45 | +0.00 | +0.00 | — | — | $9,063.45 | $9,063.45 | — |
| 2026-09-10 | -13.28 | $9,063.45 | — | $9,063.45 | +0.00 | +0.00 | — | — | $9,063.45 | $9,063.45 | — |
| 2026-09-11 | +0.50 | $9,063.45 | — | $9,063.45 | +0.00 | +149.21 | DBI, APPS, INSP, WCC, OBE | — | $131.10 | $9,199.78 | DBI×306, APPS×152, INSP×25, WCC×5, OBE×144 |
| 2026-09-14 | -11.00 | $131.10 | DBI×306, APPS×152, INSP×25, WCC×5, OBE×144 | $9,178.44 | -21.34 | -26.95 | — | DBI, APPS, INSP, OBE | $7,456.79 | $9,140.44 | WCC×5 |
| 2026-09-15 | -3.84 | $7,456.79 | WCC×5 | $9,148.19 | +7.75 | +0.00 | — | WCC | $9,146.16 | $9,146.16 | — |
| 2026-09-16 | +5.30 | $9,146.16 | — | $9,146.16 | -0.00 | +265.36 | HLP, FTRE, SDGR, RVTY, TENB, MRCY, RBRK, DOCU | — | $54.79 | $9,388.84 | HLP×635, FTRE×57, SDGR×49, RVTY×8, TENB×31, MRCY×13, RBRK×11, DOCU×16 |
| 2026-09-17 | +7.38 | $54.79 | HLP×635, FTRE×57, SDGR×49, RVTY×8, TENB×31, MRCY×13, RBRK×11, DOCU×16 | $9,415.07 | +26.23 | -77.37 | IOVA, AMN, SABR, BRKR, ADPT, WCC | HLP, FTRE, SDGR, RVTY, TENB, MRCY, RBRK, DOCU | $235.91 | $9,295.57 | IOVA×152, AMN×44, SABR×652, BRKR×25, ADPT×55, WCC×4 |
| 2026-09-18 | +4.86 | $235.91 | IOVA×152, AMN×44, SABR×652, BRKR×25, ADPT×55, WCC×4 | $9,302.41 | +6.84 | -117.32 | ILMN, LVWR, GNRC, TEM, FCEL, PGEN, SATL, VOYG | IOVA, AMN, SABR, BRKR, ADPT, WCC | $291.14 | $9,140.15 | ILMN×4, LVWR×778, GNRC×5, TEM×14, FCEL×65, PGEN×145, SATL×211, VOYG×31 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BCAR` | 821 | $6.09 | $10.59 | — | $4,989.52 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ⚪; ret5=+27.6; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZIM` | 183 | $27.25 | $2.54 | — | $0.23 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list oppset; 🔵; ⚪; ret5=+1.7; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.23 | ▼ close $9,936.28 vs 09:30 $10,000.00 (session -50.59) | 16:00 close · cash $0.23 · equity $9,936.28 vs 09:30 $10,000.00 (-63.72; session marks -50.59) · 2 name(s) marked open→close (per-name table). BCAR×821 09:30 $6.09 → close $5.83 -213.46; ZIM×183 09:30 $27.25 → close $28.14 +162.87 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.23 | ▲ 09:30 equity $10,193.91 vs yday $9,936.28 (+257.63) | 09:30 open · cash $0.23 (unchanged overnight, no fees) · equity $10,193.91 vs prior close $9,936.28 (+257.63) · 2 name(s) re-marked at the open (per-name table). BCAR×821 yday $5.83 → 09:30 $5.99 +131.36; ZIM×183 yday $28.14 → 09:30 $28.83 +126.27 | — |
| 2026-08-17 09:30 ET | **SELL** | `BCAR` | 821 | $5.99 | $10.77 | $-103.46 | $4,907.25 | ▼ -103.46 after sell → book $10,183.14; vs 09:30 mark -10.77 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZIM` | 183 | $28.83 | $2.61 | $+283.99 | $10,180.53 | ▲ +283.99 after sell → book $10,180.53; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `RDDT` | 57 | $177.51 | $2.16 | — | $60.30 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,oppset; ⚪; ret5=+10.1; leftover $10180.53 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.30 | ▼ close $9,436.80 vs 09:30 $10,193.91 (session -741.57) | 16:00 close · cash $60.30 · equity $9,436.80 vs 09:30 $10,193.91 (-757.11; session marks -741.57) · 1 name(s) marked open→close (per-name table). RDDT×57 09:30 $177.51 → close $164.50 -741.57 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.30 | ▲ 09:30 equity $9,528.00 vs yday $9,436.80 (+91.20) | 09:30 open · cash $60.30 (unchanged overnight, no fees) · equity $9,528.00 vs prior close $9,436.80 (+91.20) · 1 name(s) re-marked at the open (per-name table). RDDT×57 yday $164.50 → 09:30 $166.10 +91.20 | — |
| 2026-08-18 09:30 ET | **SELL** | `RDDT` | 57 | $166.10 | $2.25 | $-654.78 | $9,525.75 | ▼ -654.78 after sell → book $9,525.75; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,525.75 | ▲ close $9,525.75 vs 09:30 $9,528.00 (session +0.00) | 16:00 close · cash $9,525.75 · no lots left · equity $9,525.75. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,525.75 | ▲ 09:30 equity $9,525.75 vs yday $9,525.75 (+0.00) | 09:30 open · cash $9,525.75 · no holdings · equity $9,525.75 vs prior close $9,525.75 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,525.75 | ▲ close $9,525.75 vs 09:30 $9,525.75 (session +0.00) | 16:00 close · cash $9,525.75 · no lots left · equity $9,525.75. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,525.75 | ▲ 09:30 equity $9,525.75 vs yday $9,525.75 (+0.00) | 09:30 open · cash $9,525.75 · no holdings · equity $9,525.75 vs prior close $9,525.75 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 12 | $109.06 | $2.03 | — | $8,215.01 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,oppset; 🔵; ret5=+22.0; leftover $1360.82 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1924 | $0.71 | $19.37 | — | $6,835.37 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1360.82 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 13 | $97.43 | $2.03 | — | $5,566.75 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+11.8; leftover $1360.82 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SBET` | 180 | $7.55 | $2.53 | — | $4,205.22 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+14.6; leftover $1360.82 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BMNR` | 63 | $21.46 | $2.18 | — | $2,851.06 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ⚪; ret5=+13.1; leftover $1360.82 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SUI` | 11 | $121.21 | $2.02 | — | $1,515.72 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list oppset; 🔵; ret5=+3.1; leftover $1360.82 | join🔴 sector🔴 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WEAV` | 185 | $7.32 | $2.54 | — | $158.98 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list oppset; 🔵; ret5=+45.9; leftover $1360.82 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.98 | ▼ close $9,468.80 vs 09:30 $9,525.75 (session -24.24) | 16:00 close · cash $158.98 · equity $9,468.80 vs 09:30 $9,525.75 (-56.95; session marks -24.24) · 7 name(s) marked open→close (per-name table). BNTX×12 09:30 $109.06 → close $110.89 +21.96; HUMA×1924 09:30 $0.71 → close $0.68 -50.02; EL×13 09:30 $97.43 → close $96.15 -16.64; SBET×180 09:30 $7.55 → close $7.59 +7.20; BMNR×63 09:30 $21.46 → close $21.57 +6.93; SUI×11 09:30 $121.21 → close $122.29 +11.88; WEAV×185 09:30 $7.32 → close $7.29 -5.55 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.98 | ▲ 09:30 equity $9,559.91 vs yday $9,468.80 (+91.11) | 09:30 open · cash $158.98 (unchanged overnight, no fees) · equity $9,559.91 vs prior close $9,468.80 (+91.11) · 7 name(s) re-marked at the open (per-name table). BNTX×12 yday $110.89 → 09:30 $110.92 +0.36; HUMA×1924 yday $0.68 → 09:30 $0.67 -13.47; EL×13 yday $96.15 → 09:30 $96.75 +7.80; SBET×180 yday $7.59 → 09:30 $7.87 +50.40; BMNR×63 yday $21.57 → 09:30 $22.25 +42.84; SUI×11 yday $122.29 → 09:30 $122.41 +1.32; WEAV×185 yday $7.29 → 09:30 $7.30 +1.85 | — |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 12 | $110.92 | $2.05 | $+18.25 | $1,487.97 | ▲ +18.25 after sell → book $9,557.86; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1924 | $0.67 | $19.07 | $-101.94 | $2,765.68 | ▼ -101.94 after sell → book $9,538.79; vs 09:30 mark -19.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 13 | $96.75 | $2.05 | $-12.92 | $4,021.38 | ▼ -12.92 after sell → book $9,536.74; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SBET` | 180 | $7.87 | $2.57 | $+52.50 | $5,435.41 | ▲ +52.50 after sell → book $9,534.17; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BMNR` | 63 | $22.25 | $2.20 | $+45.39 | $6,834.96 | ▲ +45.39 after sell → book $9,531.97; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `SUI` | 11 | $122.41 | $2.04 | $+9.13 | $8,179.42 | ▲ +9.13 after sell → book $9,529.92; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WEAV` | 185 | $7.30 | $2.59 | $-8.83 | $9,527.34 | ▼ -8.83 after sell → book $9,527.34; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 14 | $127.43 | $2.03 | — | $7,741.29 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable; 🔵; ⚪; ret5=+7.9; leftover $1905.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 1370 | $1.39 | $17.67 | — | $5,819.31 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $1905.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 230 | $8.28 | $2.97 | — | $3,911.95 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.6; leftover $1905.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 162 | $11.70 | $2.48 | — | $2,014.07 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1905.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ILMN` | 8 | $212.40 | $2.01 | — | $312.86 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list mover_buy; 🔵; ⚪; ret5=+10.7; leftover $1905.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $312.86 | ▼ close $9,461.08 vs 09:30 $9,559.91 (session -39.10) | 16:00 close · cash $312.86 · equity $9,461.08 vs 09:30 $9,559.91 (-98.83; session marks -39.10) · 5 name(s) marked open→close (per-name table). CF×14 09:30 $127.43 → close $129.60 +30.38; INDP×1370 09:30 $1.39 → close $1.29 -137.00; MRVI×230 09:30 $8.28 → close $8.64 +82.80; MARA×162 09:30 $11.70 → close $11.26 -71.28; ILMN×8 09:30 $212.40 → close $219.40 +56.00 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $312.86 | ▼ 09:30 equity $9,344.60 vs yday $9,461.08 (-116.48) | 09:30 open · cash $312.86 (unchanged overnight, no fees) · equity $9,344.60 vs prior close $9,461.08 (-116.48) · 5 name(s) re-marked at the open (per-name table). CF×14 yday $129.60 → 09:30 $129.99 +5.46; INDP×1370 yday $1.29 → 09:30 $1.24 -68.50; MRVI×230 yday $8.64 → 09:30 $8.59 -11.50; MARA×162 yday $11.26 → 09:30 $11.17 -14.58; ILMN×8 yday $219.40 → 09:30 $215.98 -27.36 | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 14 | $129.99 | $2.06 | $+31.75 | $2,130.66 | ▲ +31.75 after sell → book $9,342.54; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 1370 | $1.24 | $17.91 | $-241.09 | $3,811.55 | ▼ -241.09 after sell → book $9,324.63; vs 09:30 mark -17.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 230 | $8.59 | $3.02 | $+65.31 | $5,784.22 | ▲ +65.31 after sell → book $9,321.60; vs 09:30 mark -3.03 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 162 | $11.17 | $2.52 | $-90.85 | $7,591.25 | ▼ -90.85 after sell → book $9,319.09; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ILMN` | 8 | $215.98 | $2.04 | $+24.59 | $9,317.05 | ▲ +24.59 after sell → book $9,317.05; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,317.05 | ▲ close $9,317.05 vs 09:30 $9,344.60 (session +0.00) | 16:00 close · cash $9,317.05 · no lots left · equity $9,317.05. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,317.05 | ▲ 09:30 equity $9,317.05 vs yday $9,317.05 (-0.00) | 09:30 open · cash $9,317.05 · no holdings · equity $9,317.05 vs prior close $9,317.05 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 731 | $6.37 | $9.43 | — | $4,651.15 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,oppset; 🔵; ⚪; ret5=+10.9; leftover $4658.52 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 106 | $43.76 | $2.31 | — | $10.28 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $4658.52 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▲ close $9,389.60 vs 09:30 $9,317.05 (session +84.29) | 16:00 close · cash $10.28 · equity $9,389.60 vs 09:30 $9,317.05 (+72.55; session marks +84.29) · 2 name(s) marked open→close (per-name table). ZURA×731 09:30 $6.37 → close $6.32 -36.55; RHI×106 09:30 $43.76 → close $44.90 +120.84 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,190.29 vs yday $9,389.60 (-199.31) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,190.29 vs prior close $9,389.60 (-199.31) · 2 name(s) re-marked at the open (per-name table). ZURA×731 yday $6.32 → 09:30 $6.13 -138.89; RHI×106 yday $44.90 → 09:30 $44.33 -60.42 | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 731 | $6.13 | $9.59 | $-194.46 | $4,481.72 | ▼ -194.46 after sell → book $9,180.70; vs 09:30 mark -9.59 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 106 | $44.33 | $2.36 | $+55.75 | $9,178.34 | ▲ +55.75 after sell → book $9,178.34; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 98 | $31.21 | $2.28 | — | $6,117.48 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $3059.45 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 275 | $11.12 | $3.55 | — | $3,055.93 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $3059.45 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BE` | 14 | $213.94 | $2.03 | — | $58.74 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $3059.45 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $58.74 | ▲ close $9,212.40 vs 09:30 $9,190.29 (session +41.92) | 16:00 close · cash $58.74 · equity $9,212.40 vs 09:30 $9,190.29 (+22.11; session marks +41.92) · 3 name(s) marked open→close (per-name table). AVBP×98 09:30 $31.21 → close $31.14 -6.86; FLNC×275 09:30 $11.12 → close $11.08 -11.00; BE×14 09:30 $213.94 → close $218.21 +59.78 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $58.74 | ▲ 09:30 equity $9,423.56 vs yday $9,212.40 (+211.16) | 09:30 open · cash $58.74 (unchanged overnight, no fees) · equity $9,423.56 vs prior close $9,212.40 (+211.16) · 3 name(s) re-marked at the open (per-name table). AVBP×98 yday $31.14 → 09:30 $30.79 -34.30; FLNC×275 yday $11.08 → 09:30 $11.52 +121.00; BE×14 yday $218.21 → 09:30 $227.10 +124.46 | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 98 | $30.79 | $2.32 | $-45.77 | $3,073.83 | ▼ -45.77 after sell → book $9,421.23; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 275 | $11.52 | $3.62 | $+102.83 | $6,238.21 | ▲ +102.83 after sell → book $9,417.61; vs 09:30 mark -3.62 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BE` | 14 | $227.10 | $2.07 | $+180.14 | $9,415.55 | ▲ +180.14 after sell → book $9,415.55; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 72 | $18.50 | $2.21 | — | $8,081.34 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+17.2; leftover $1345.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `VYX` | 150 | $8.95 | $2.44 | — | $6,736.40 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+16.2; leftover $1345.08 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GAP` | 64 | $20.75 | $2.18 | — | $5,406.22 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+5.2; leftover $1345.08 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `AEO` | 77 | $17.27 | $2.22 | — | $4,074.21 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+5.5; leftover $1345.08 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SMTC` | 9 | $149.40 | $2.02 | — | $2,727.59 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; 🔵; ret5=+12.3; leftover $1345.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 45 | $29.83 | $2.12 | — | $1,383.12 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list mover_buy; 🔵; ret5=+7.6; leftover $1345.08 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 58 | $22.93 | $2.16 | — | $51.01 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list mover_buy; 🔵; ret5=+9.5; leftover $1345.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.01 | ▼ close $9,343.15 vs 09:30 $9,423.56 (session -57.04) | 16:00 close · cash $51.01 · equity $9,343.15 vs 09:30 $9,423.56 (-80.41; session marks -57.04) · 7 name(s) marked open→close (per-name table). BZ×72 09:30 $18.50 → close $18.00 -36.00; VYX×150 09:30 $8.95 → close $9.18 +34.50; GAP×64 09:30 $20.75 → close $20.79 +2.56; AEO×77 09:30 $17.27 → close $16.69 -44.66; SMTC×9 09:30 $149.40 → close $142.43 -62.73; GEN×45 09:30 $29.83 → close $30.50 +30.15; PGY×58 09:30 $22.93 → close $23.26 +19.14 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.01 | ▲ 09:30 equity $9,615.61 vs yday $9,343.15 (+272.46) | 09:30 open · cash $51.01 (unchanged overnight, no fees) · equity $9,615.61 vs prior close $9,343.15 (+272.46) · 7 name(s) re-marked at the open (per-name table). BZ×72 yday $18.00 → 09:30 $18.15 +10.80; VYX×150 yday $9.18 → 09:30 $9.13 -7.50; GAP×64 yday $20.79 → 09:30 $24.69 +249.60; AEO×77 yday $16.69 → 09:30 $17.06 +28.49; SMTC×9 yday $142.43 → 09:30 $141.76 -6.03; GEN×45 yday $30.50 → 09:30 $30.50 +0.00; PGY×58 yday $23.26 → 09:30 $23.21 -2.90 | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 72 | $18.15 | $2.23 | $-29.63 | $1,355.58 | ▼ -29.63 after sell → book $9,613.38; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `VYX` | 150 | $9.13 | $2.48 | $+22.08 | $2,722.61 | ▲ +22.08 after sell → book $9,610.91; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `GAP` | 64 | $24.69 | $2.21 | $+247.77 | $4,300.56 | ▲ +247.77 after sell → book $9,608.70; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AEO` | 77 | $17.06 | $2.24 | $-20.64 | $5,611.94 | ▼ -20.64 after sell → book $9,606.46; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SMTC` | 9 | $141.76 | $2.04 | $-72.81 | $6,885.74 | ▼ -72.81 after sell → book $9,604.42; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 45 | $30.50 | $2.15 | $+25.88 | $8,256.10 | ▲ +25.88 after sell → book $9,602.28; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 58 | $23.21 | $2.18 | $+11.89 | $9,600.09 | ▲ +11.89 after sell → book $9,600.09; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 863 | $1.39 | $11.13 | — | $8,389.39 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; ret5=+20.4; leftover $1200.01 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 9 | $122.81 | $2.02 | — | $7,282.08 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1200.01 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 62 | $19.25 | $2.18 | — | $6,086.40 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; ret5=+14.1; leftover $1200.01 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 65 | $18.36 | $2.19 | — | $4,890.82 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+12.8; leftover $1200.01 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FTNT` | 6 | $172.58 | $2.01 | — | $3,853.33 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+14.6; leftover $1200.01 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $2,806.69 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+7.8; leftover $1200.01 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 12 | $98.95 | $2.03 | — | $1,617.26 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+9.7; leftover $1200.01 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 2 | $542.00 | $2.00 | — | $531.27 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react; ret5=+4.8; leftover $1200.01 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $531.27 | ▼ close $9,245.52 vs 09:30 $9,615.61 (session -329.03) | 16:00 close · cash $531.27 · equity $9,245.52 vs 09:30 $9,615.61 (-370.09; session marks -329.03) · 8 name(s) marked open→close (per-name table). LVWR×863 09:30 $1.39 → close $1.35 -34.52; TTMI×9 09:30 $122.81 → close $118.65 -37.44; ERAS×62 09:30 $19.25 → close $18.03 -75.64; NEO×65 09:30 $18.36 → close $18.05 -20.15; FTNT×6 09:30 $172.58 → close $166.00 -39.48; ADSK×4 09:30 $261.16 → close $260.66 -2.00; RBRK×12 09:30 $98.95 → close $93.05 -70.80; ULTA×2 09:30 $542.00 → close $517.50 -49.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $531.27 | ▼ 09:30 equity $9,172.23 vs yday $9,245.52 (-73.29) | 09:30 open · cash $531.27 (unchanged overnight, no fees) · equity $9,172.23 vs prior close $9,245.52 (-73.29) · 8 name(s) re-marked at the open (per-name table). LVWR×863 yday $1.35 → 09:30 $1.30 -43.15; TTMI×9 yday $118.65 → 09:30 $118.83 +1.62; ERAS×62 yday $18.03 → 09:30 $17.87 -9.92; NEO×65 yday $18.05 → 09:30 $17.77 -18.20; FTNT×6 yday $166.00 → 09:30 $166.60 +3.60; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; RBRK×12 yday $93.05 → 09:30 $92.83 -2.64; ULTA×2 yday $517.50 → 09:30 $521.10 +7.20 | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 863 | $1.30 | $11.29 | $-100.09 | $1,641.88 | ▼ -100.09 after sell → book $9,160.94; vs 09:30 mark -11.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 9 | $118.83 | $2.04 | $-39.87 | $2,709.31 | ▼ -39.87 after sell → book $9,158.90; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 62 | $17.87 | $2.20 | $-89.93 | $3,815.06 | ▼ -89.93 after sell → book $9,156.71; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NEO` | 65 | $17.77 | $2.21 | $-42.74 | $4,967.90 | ▼ -42.74 after sell → book $9,154.50; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FTNT` | 6 | $166.60 | $2.03 | $-39.92 | $5,965.47 | ▼ -39.92 after sell → book $9,152.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $6,994.29 | ▼ -17.82 after sell → book $9,150.45; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RBRK` | 12 | $92.83 | $2.05 | $-77.51 | $8,106.21 | ▼ -77.51 after sell → book $9,148.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 2 | $521.10 | $2.02 | $-45.81 | $9,146.39 | ▼ -45.81 after sell → book $9,146.39; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,146.39 | ▲ close $9,146.39 vs 09:30 $9,172.23 (session +0.00) | 16:00 close · cash $9,146.39 · no lots left · equity $9,146.39. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,146.39 | ▲ 09:30 equity $9,146.39 vs yday $9,146.39 (+0.00) | 09:30 open · cash $9,146.39 · no holdings · equity $9,146.39 vs prior close $9,146.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,146.39 | ▲ close $9,146.39 vs 09:30 $9,146.39 (session +0.00) | 16:00 close · cash $9,146.39 · no lots left · equity $9,146.39. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,146.39 | ▲ 09:30 equity $9,146.39 vs yday $9,146.39 (+0.00) | 09:30 open · cash $9,146.39 · no holdings · equity $9,146.39 vs prior close $9,146.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,146.39 | ▲ close $9,146.39 vs 09:30 $9,146.39 (session +0.00) | 16:00 close · cash $9,146.39 · no lots left · equity $9,146.39. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,146.39 | ▲ 09:30 equity $9,146.39 vs yday $9,146.39 (+0.00) | 09:30 open · cash $9,146.39 · no holdings · equity $9,146.39 vs prior close $9,146.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 28 | $52.88 | $2.07 | — | $7,663.68 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1524.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 189 | $8.03 | $2.56 | — | $6,143.45 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1524.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `PYXS` | 410 | $3.71 | $5.29 | — | $4,617.06 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+12.3; leftover $1524.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MLYS` | 52 | $29.15 | $2.15 | — | $3,099.11 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+12.2; leftover $1524.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HP` | 31 | $47.74 | $2.08 | — | $1,617.09 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+15.1; leftover $1524.40 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RSKD` | 228 | $6.68 | $2.94 | — | $91.11 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot,oppset; 🔵; ret5=+11.4; leftover $1524.40 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.11 | ▼ close $8,975.56 vs 09:30 $9,146.39 (session -153.74) | 16:00 close · cash $91.11 · equity $8,975.56 vs 09:30 $9,146.39 (-170.83; session marks -153.74) · 6 name(s) marked open→close (per-name table). ATRC×28 09:30 $52.88 → close $52.46 -11.76; VSTM×189 09:30 $8.03 → close $7.98 -9.45; PYXS×410 09:30 $3.71 → close $3.56 -59.45; MLYS×52 09:30 $29.15 → close $28.27 -45.76; HP×31 09:30 $47.74 → close $45.02 -84.32; RSKD×228 09:30 $6.68 → close $6.93 +57.00 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.11 | ▼ 09:30 equity $8,888.05 vs yday $8,975.56 (-87.51) | 09:30 open · cash $91.11 (unchanged overnight, no fees) · equity $8,888.05 vs prior close $8,975.56 (-87.51) · 6 name(s) re-marked at the open (per-name table). ATRC×28 yday $52.46 → 09:30 $52.03 -12.04; VSTM×189 yday $7.98 → 09:30 $7.91 -13.23; PYXS×410 yday $3.56 → 09:30 $3.53 -14.35; MLYS×52 yday $28.27 → 09:30 $28.00 -14.04; HP×31 yday $45.02 → 09:30 $44.59 -13.33; RSKD×228 yday $6.93 → 09:30 $6.84 -20.52 | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 28 | $52.03 | $2.10 | $-27.97 | $1,545.85 | ▼ -27.97 after sell → book $8,885.95; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 189 | $7.91 | $2.60 | $-27.84 | $3,038.24 | ▼ -27.84 after sell → book $8,883.35; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PYXS` | 410 | $3.53 | $5.37 | $-84.46 | $4,480.18 | ▼ -84.46 after sell → book $8,877.99; vs 09:30 mark -5.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MLYS` | 52 | $28.00 | $2.17 | $-64.11 | $5,934.01 | ▼ -64.11 after sell → book $8,875.82; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `HP` | 31 | $44.59 | $2.10 | $-101.84 | $7,314.19 | ▼ -101.84 after sell → book $8,873.71; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RSKD` | 228 | $6.84 | $2.99 | $+30.55 | $8,870.72 | ▲ +30.55 after sell → book $8,870.72; vs 09:30 mark -2.99 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 315 | $3.52 | $4.06 | — | $7,757.86 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1108.84 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $6,728.30 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy,oppset; 🔵; ⚪; ret5=+9.3; leftover $1108.84 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GSM` | 237 | $4.67 | $3.06 | — | $5,618.46 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; ret5=+11.9; leftover $1108.84 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `RNG` | 14 | $75.35 | $2.03 | — | $4,561.52 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+11.4; leftover $1108.84 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 11 | $98.15 | $2.02 | — | $3,479.85 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list earn_react,oppset; ret5=+5.9; leftover $1108.84 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CHPT` | 119 | $9.28 | $2.35 | — | $2,373.18 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list oppset; 🔵; ret5=+55.7; leftover $1108.84 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMX` | 48 | $23.03 | $2.13 | — | $1,265.61 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list oppset; 🔵; ret5=-1.4; leftover $1108.84 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `PFG` | 9 | $117.03 | $2.02 | — | $210.32 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list oppset; 🔵; ret5=+5.7; leftover $1108.84 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $210.32 | ▲ close $9,052.79 vs 09:30 $8,888.05 (session +201.74) | 16:00 close · cash $210.32 · equity $9,052.79 vs 09:30 $8,888.05 (+164.74; session marks +201.74) · 8 name(s) marked open→close (per-name table). EOSE×315 09:30 $3.52 → close $3.88 +113.40; DELL×2 09:30 $513.78 → close $524.14 +20.72; GSM×237 09:30 $4.67 → close $4.67 +0.00; RNG×14 09:30 $75.35 → close $73.39 -27.44; LULU×11 09:30 $98.15 → close $100.61 +27.06; CHPT×119 09:30 $9.28 → close $9.89 +72.59; AMX×48 09:30 $23.03 → close $23.00 -1.44; PFG×9 09:30 $117.03 → close $116.68 -3.15 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $210.32 | ▲ 09:30 equity $9,083.36 vs yday $9,052.79 (+30.57) | 09:30 open · cash $210.32 (unchanged overnight, no fees) · equity $9,083.36 vs prior close $9,052.79 (+30.57) · 8 name(s) re-marked at the open (per-name table). EOSE×315 yday $3.88 → 09:30 $3.99 +34.65; DELL×2 yday $524.14 → 09:30 $521.15 -5.98; GSM×237 yday $4.67 → 09:30 $4.75 +18.96; RNG×14 yday $73.39 → 09:30 $72.07 -18.48; LULU×11 yday $100.61 → 09:30 $100.58 -0.33; CHPT×119 yday $9.89 → 09:30 $9.91 +2.38; AMX×48 yday $23.00 → 09:30 $23.15 +7.20; PFG×9 yday $116.68 → 09:30 $115.81 -7.83 | — |
| 2026-09-08 09:30 ET | **SELL** | `EOSE` | 315 | $3.99 | $4.13 | $+139.86 | $1,463.05 | ▲ +139.86 after sell → book $9,079.24; vs 09:30 mark -4.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $2,503.33 | ▲ +10.73 after sell → book $9,077.22; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `GSM` | 237 | $4.75 | $3.11 | $+12.80 | $3,625.97 | ▲ +12.80 after sell → book $9,074.11; vs 09:30 mark -3.11 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `RNG` | 14 | $72.07 | $2.05 | $-50.00 | $4,632.90 | ▼ -50.00 after sell → book $9,072.06; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 11 | $100.58 | $2.04 | $+22.66 | $5,737.24 | ▲ +22.66 after sell → book $9,070.02; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `CHPT` | 119 | $9.91 | $2.38 | $+70.25 | $6,914.15 | ▲ +70.25 after sell → book $9,067.64; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `AMX` | 48 | $23.15 | $2.15 | $+1.47 | $8,023.20 | ▲ +1.47 after sell → book $9,065.49; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `PFG` | 9 | $115.81 | $2.04 | $-15.03 | $9,063.45 | ▼ -15.03 after sell → book $9,063.45; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,063.45 | ▲ close $9,063.45 vs 09:30 $9,083.36 (session +0.00) | 16:00 close · cash $9,063.45 · no lots left · equity $9,063.45. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,063.45 | ▲ 09:30 equity $9,063.45 vs yday $9,063.45 (+0.00) | 09:30 open · cash $9,063.45 · no holdings · equity $9,063.45 vs prior close $9,063.45 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,063.45 | ▲ close $9,063.45 vs 09:30 $9,063.45 (session +0.00) | 16:00 close · cash $9,063.45 · no lots left · equity $9,063.45. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,063.45 | ▲ 09:30 equity $9,063.45 vs yday $9,063.45 (+0.00) | 09:30 open · cash $9,063.45 · no holdings · equity $9,063.45 vs prior close $9,063.45 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,063.45 | ▲ close $9,063.45 vs 09:30 $9,063.45 (session +0.00) | 16:00 close · cash $9,063.45 · no lots left · equity $9,063.45. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,063.45 | ▲ 09:30 equity $9,063.45 vs yday $9,063.45 (+0.00) | 09:30 open · cash $9,063.45 · no holdings · equity $9,063.45 vs prior close $9,063.45 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 306 | $5.91 | $3.95 | — | $7,251.04 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,ohlc_hot,oppset; ret5=+14.1; leftover $1812.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `APPS` | 152 | $11.88 | $2.45 | — | $5,442.84 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+20.7; leftover $1812.69 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INSP` | 25 | $69.88 | $2.06 | — | $3,693.77 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+8.0; leftover $1812.69 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `WCC` | 5 | $350.21 | $2.00 | — | $1,940.72 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+5.8; leftover $1812.69 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `OBE` | 144 | $12.55 | $2.42 | — | $131.10 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list oppset; ret5=+5.5; leftover $1812.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.10 | ▲ close $9,199.78 vs 09:30 $9,063.45 (session +149.21) | 16:00 close · cash $131.10 · equity $9,199.78 vs 09:30 $9,063.45 (+136.33; session marks +149.21) · 5 name(s) marked open→close (per-name table). DBI×306 09:30 $5.91 → close $5.88 -9.18; APPS×152 09:30 $11.88 → close $11.81 -10.64; INSP×25 09:30 $69.88 → close $73.00 +78.00; WCC×5 09:30 $350.21 → close $356.32 +30.55; OBE×144 09:30 $12.55 → close $12.97 +60.48 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.10 | ▼ 09:30 equity $9,178.44 vs yday $9,199.78 (-21.34) | 09:30 open · cash $131.10 (unchanged overnight, no fees) · equity $9,178.44 vs prior close $9,199.78 (-21.34) · 5 name(s) re-marked at the open (per-name table). DBI×306 yday $5.88 → 09:30 $5.86 -6.12; APPS×152 yday $11.81 → 09:30 $11.75 -9.12; INSP×25 yday $73.00 → 09:30 $72.14 -21.50; WCC×5 yday $356.32 → 09:30 $342.12 -71.00; OBE×144 yday $12.97 → 09:30 $13.57 +86.40 | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 306 | $5.86 | $4.01 | $-23.26 | $1,920.24 | ▼ -23.26 after sell → book $9,174.42; vs 09:30 mark -4.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `APPS` | 152 | $11.75 | $2.49 | $-24.69 | $3,703.76 | ▼ -24.69 after sell → book $9,171.94; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INSP` | 25 | $72.14 | $2.09 | $+52.35 | $5,505.17 | ▲ +52.35 after sell → book $9,169.85; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `OBE` | 144 | $13.57 | $2.46 | $+142.00 | $7,456.79 | ▲ +142.00 after sell → book $9,167.39; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,456.79 | ▼ close $9,140.44 vs 09:30 $9,178.44 (session -26.95) | 16:00 close · cash $7,456.79 · equity $9,140.44 vs 09:30 $9,178.44 (-38.00; session marks -26.95) · 1 name(s) marked open→close (per-name table). WCC×5 09:30 $342.12 → close $336.73 -26.95 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,456.79 | ▲ 09:30 equity $9,148.19 vs yday $9,140.44 (+7.75) | 09:30 open · cash $7,456.79 (unchanged overnight, no fees) · equity $9,148.19 vs prior close $9,140.44 (+7.75) · 1 name(s) re-marked at the open (per-name table). WCC×5 yday $336.73 → 09:30 $338.28 +7.75 | — |
| 2026-09-15 09:30 ET | **SELL** | `WCC` | 5 | $338.28 | $2.03 | $-63.68 | $9,146.16 | ▼ -63.68 after sell → book $9,146.16; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,146.16 | ▲ close $9,146.16 vs 09:30 $9,148.19 (session +0.00) | 16:00 close · cash $9,146.16 · no lots left · equity $9,146.16. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,146.16 | ▲ 09:30 equity $9,146.16 vs yday $9,146.16 (-0.00) | 09:30 open · cash $9,146.16 · no holdings · equity $9,146.16 vs prior close $9,146.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 635 | $1.80 | $8.19 | — | $7,994.97 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $1143.27 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `FTRE` | 57 | $19.75 | $2.16 | — | $6,867.06 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $1143.27 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 49 | $23.29 | $2.14 | — | $5,723.71 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,oppset; 🔵; ret5=+16.1; leftover $1143.27 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 8 | $140.88 | $2.01 | — | $4,594.66 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $1143.27 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `TENB` | 31 | $36.86 | $2.08 | — | $3,449.91 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; ret5=+13.0; leftover $1143.27 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 13 | $87.52 | $2.03 | — | $2,310.12 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer; 🔵; ret5=+4.3; leftover $1143.27 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RBRK` | 11 | $101.97 | $2.02 | — | $1,186.43 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+13.0; leftover $1143.27 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `DOCU` | 16 | $70.60 | $2.04 | — | $54.79 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+10.4; leftover $1143.27 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.79 | ▲ close $9,388.84 vs 09:30 $9,146.16 (session +265.36) | 16:00 close · cash $54.79 · equity $9,388.84 vs 09:30 $9,146.16 (+242.68; session marks +265.36) · 8 name(s) marked open→close (per-name table). HLP×635 09:30 $1.80 → close $2.07 +171.45; FTRE×57 09:30 $19.75 → close $19.97 +12.54; SDGR×49 09:30 $23.29 → close $23.93 +31.36; RVTY×8 09:30 $140.88 → close $145.73 +38.80; TENB×31 09:30 $36.86 → close $36.37 -15.19; MRCY×13 09:30 $87.52 → close $87.25 -3.51; RBRK×11 09:30 $101.97 → close $104.98 +33.11; DOCU×16 09:30 $70.60 → close $70.40 -3.20 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.79 | ▲ 09:30 equity $9,415.07 vs yday $9,388.84 (+26.23) | 09:30 open · cash $54.79 (unchanged overnight, no fees) · equity $9,415.07 vs prior close $9,388.84 (+26.23) · 8 name(s) re-marked at the open (per-name table). HLP×635 yday $2.07 → 09:30 $2.10 +19.05; FTRE×57 yday $19.97 → 09:30 $20.31 +19.38; SDGR×49 yday $23.93 → 09:30 $24.09 +7.84; RVTY×8 yday $145.73 → 09:30 $147.61 +15.04; TENB×31 yday $36.37 → 09:30 $35.89 -14.88; MRCY×13 yday $87.25 → 09:30 $89.27 +26.26; RBRK×11 yday $104.98 → 09:30 $102.56 -26.62; DOCU×16 yday $70.40 → 09:30 $69.16 -19.84 | — |
| 2026-09-17 09:30 ET | **SELL** | `HLP` | 635 | $2.10 | $8.31 | $+174.00 | $1,379.98 | ▲ +174.00 after sell → book $9,406.76; vs 09:30 mark -8.31 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `FTRE` | 57 | $20.31 | $2.18 | $+27.58 | $2,535.47 | ▲ +27.58 after sell → book $9,404.58; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 49 | $24.09 | $2.16 | $+34.91 | $3,713.73 | ▲ +34.91 after sell → book $9,402.43; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RVTY` | 8 | $147.61 | $2.03 | $+49.79 | $4,892.57 | ▲ +49.79 after sell → book $9,400.39; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `TENB` | 31 | $35.89 | $2.10 | $-34.26 | $6,003.06 | ▼ -34.26 after sell → book $9,398.29; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `MRCY` | 13 | $89.27 | $2.05 | $+18.67 | $7,161.52 | ▲ +18.67 after sell → book $9,396.24; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RBRK` | 11 | $102.56 | $2.04 | $+2.42 | $8,287.64 | ▲ +2.42 after sell → book $9,394.20; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `DOCU` | 16 | $69.16 | $2.06 | $-27.14 | $9,392.14 | ▼ -27.14 after sell → book $9,392.14; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 152 | $10.25 | $2.45 | — | $7,831.69 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1565.36 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 44 | $34.93 | $2.12 | — | $6,292.65 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; ret5=+1.6; leftover $1565.36 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 652 | $2.40 | $8.41 | — | $4,719.44 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1565.36 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 25 | $61.90 | $2.06 | — | $3,169.88 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; leftover $1565.36 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ADPT` | 55 | $28.23 | $2.15 | — | $1,615.07 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; ret5=+13.3; leftover $1565.36 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `WCC` | 4 | $344.29 | $2.00 | — | $235.91 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+5.8; leftover $1565.36 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $235.91 | ▼ close $9,295.57 vs 09:30 $9,415.07 (session -77.37) | 16:00 close · cash $235.91 · equity $9,295.57 vs 09:30 $9,415.07 (-119.50; session marks -77.37) · 6 name(s) marked open→close (per-name table). IOVA×152 09:30 $10.25 → close $10.02 -34.96; AMN×44 09:30 $34.93 → close $34.55 -16.72; SABR×652 09:30 $2.40 → close $2.32 -52.16; BRKR×25 09:30 $61.90 → close $63.05 +28.75; ADPT×55 09:30 $28.23 → close $28.55 +17.60; WCC×4 09:30 $344.29 → close $339.32 -19.88 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $235.91 | ▲ 09:30 equity $9,302.41 vs yday $9,295.57 (+6.84) | 09:30 open · cash $235.91 (unchanged overnight, no fees) · equity $9,302.41 vs prior close $9,295.57 (+6.84) · 6 name(s) re-marked at the open (per-name table). IOVA×152 yday $10.02 → 09:30 $10.12 +15.20; AMN×44 yday $34.55 → 09:30 $34.52 -1.32; SABR×652 yday $2.32 → 09:30 $2.29 -19.56; BRKR×25 yday $63.05 → 09:30 $63.37 +8.00; ADPT×55 yday $28.55 → 09:30 $28.55 +0.00; WCC×4 yday $339.32 → 09:30 $340.45 +4.52 | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 152 | $10.12 | $2.48 | $-24.69 | $1,771.67 | ▼ -24.69 after sell → book $9,299.93; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMN` | 44 | $34.52 | $2.14 | $-22.31 | $3,288.40 | ▼ -22.31 after sell → book $9,297.78; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 652 | $2.29 | $8.53 | $-88.66 | $4,772.95 | ▼ -88.66 after sell → book $9,289.25; vs 09:30 mark -8.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 25 | $63.37 | $2.09 | $+32.60 | $6,355.11 | ▲ +32.60 after sell → book $9,287.16; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `ADPT` | 55 | $28.55 | $2.18 | $+13.27 | $7,923.18 | ▲ +13.27 after sell → book $9,284.98; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `WCC` | 4 | $340.45 | $2.02 | $-19.38 | $9,282.96 | ▼ -19.38 after sell → book $9,282.96; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `ILMN` | 4 | $249.13 | $2.00 | — | $8,284.44 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; 🔵; ⚪; ret5=+21.8; leftover $1160.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `LVWR` | 778 | $1.49 | $10.04 | — | $7,115.18 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+25.7; leftover $1160.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $6,065.58 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,oppset; 🔵; ret5=+14.1; leftover $1160.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 14 | $81.40 | $2.03 | — | $4,923.95 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.8; leftover $1160.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FCEL` | 65 | $17.80 | $2.19 | — | $3,764.76 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+13.6; leftover $1160.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `PGEN` | 145 | $7.98 | $2.42 | — | $2,605.24 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $1160.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `SATL` | 211 | $5.49 | $2.72 | — | $1,445.18 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+17.2; leftover $1160.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `VOYG` | 31 | $37.16 | $2.08 | — | $291.14 | — | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+11.0; leftover $1160.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $291.14 | ▼ close $9,140.15 vs 09:30 $9,302.41 (session -117.32) | 16:00 close · cash $291.14 · equity $9,140.15 vs 09:30 $9,302.41 (-162.26; session marks -117.32) · 8 name(s) marked open→close (per-name table). ILMN×4 09:30 $249.13 → close $239.62 -38.04; LVWR×778 09:30 $1.49 → close $1.63 +108.92; GNRC×5 09:30 $209.52 → close $207.44 -10.40; TEM×14 09:30 $81.40 → close $77.84 -49.84; FCEL×65 09:30 $17.80 → close $18.11 +20.15; PGEN×145 09:30 $7.98 → close $7.78 -29.00; SATL×211 09:30 $5.49 → close $5.11 -79.12; VOYG×31 09:30 $37.16 → close $35.87 -39.99 | — |

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
| 2026-08-24 | `AMX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SOLS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ESTC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SGI` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-02 | `MMED` | hard_red | hard-red S=-3.83 sit; no new buys |
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
| 2026-09-14 | `RLMD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `IMSR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `OKLO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `INIO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TAC` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ILMN` | 4 | 2026-09-18 @ $249.13 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list flatten; 🔵; ⚪; ret5=+21.8; leftover $1160.37 |
| `LVWR` | 778 | 2026-09-18 @ $1.49 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+25.7; leftover $1160.37 |
| `GNRC` | 5 | 2026-09-18 @ $209.52 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,oppset; 🔵; ret5=+14.1; leftover $1160.37 |
| `TEM` | 14 | 2026-09-18 @ $81.40 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.8; leftover $1160.37 |
| `FCEL` | 65 | 2026-09-18 @ $17.80 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list yday_gainer,yday_mover; 🔵; ret5=+13.6; leftover $1160.37 |
| `PGEN` | 145 | 2026-09-18 @ $7.98 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $1160.37 |
| `SATL` | 211 | 2026-09-18 @ $5.49 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+17.2; leftover $1160.37 |
| `VOYG` | 31 | 2026-09-18 @ $37.16 | union ∩ macd_xup, no 🚨; gate macd_cross_up=True; list ohlc_hot; 🔵; ret5=+11.0; leftover $1160.37 |
