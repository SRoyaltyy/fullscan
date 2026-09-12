# Factor mine action — `union_white_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-5.87%** ($9,414) · signal-only (no cash/fees) was -2.55%. Starts YES **0/21**. Fills 129 · skips 3 · realized $-585.77.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: no morning camera is red (the 'white' / all-clear row).
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-have: prior relative volume is at most 2.2 (not a blow-off).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

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
- **Gate** `zero_red=True,ret_5_max=10.0,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,065.70.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 27 | — | $59.80 | +0.00 | $60.23 | +11.61 | +11.61 | +0.00 | +11.61 |
| 2026-08-13 | `TPG` | 32 | — | $50.62 | +0.00 | $54.62 | +127.90 | +127.90 | +0.00 | +127.90 |
| 2026-08-13 | `TGTX` | 33 | — | $49.70 | +0.00 | $47.94 | -58.08 | -58.08 | +0.00 | -58.08 |
| 2026-08-13 | `SLS` | 142 | — | $11.70 | +0.00 | $12.36 | +93.72 | +93.72 | +0.00 | +93.72 |
| 2026-08-13 | `HIMS` | 56 | — | $29.74 | +0.00 | $28.77 | -54.32 | -54.32 | +0.00 | -54.32 |
| 2026-08-13 | `VOR` | 75 | — | $22.01 | +0.00 | $23.29 | +96.00 | +96.00 | +0.00 | +96.00 |
| 2026-08-14 | `BTSG` | 27 | $60.23 | $59.65 | -15.66 | — | +0.00 | -15.66 | -4.05 | — |
| 2026-08-14 | `TPG` | 32 | $54.62 | $55.29 | +21.44 | — | +0.00 | +21.44 | +149.34 | — |
| 2026-08-14 | `TGTX` | 33 | $47.94 | $47.27 | -22.11 | — | +0.00 | -22.11 | -80.19 | — |
| 2026-08-14 | `SLS` | 142 | $12.36 | $12.40 | +5.68 | — | +0.00 | +5.68 | +99.40 | — |
| 2026-08-14 | `HIMS` | 56 | $28.77 | $29.15 | +21.28 | — | +0.00 | +21.28 | -33.04 | — |
| 2026-08-14 | `VOR` | 75 | $23.29 | $23.33 | +3.00 | — | +0.00 | +3.00 | +99.00 | — |
| 2026-08-14 | `DAVE` | 3 | — | $330.91 | +0.00 | $334.57 | +10.98 | +10.98 | +0.00 | +10.98 |
| 2026-08-14 | `SLG` | 22 | — | $57.61 | +0.00 | $56.09 | -33.44 | -33.44 | +0.00 | -33.44 |
| 2026-08-14 | `MARA` | 141 | — | $9.01 | +0.00 | $9.20 | +26.79 | +26.79 | +0.00 | +26.79 |
| 2026-08-14 | `LDI` | 1361 | — | $0.94 | +0.00 | $0.90 | -54.44 | -54.44 | +0.00 | -54.44 |
| 2026-08-14 | `BTBT` | 850 | — | $1.50 | +0.00 | $1.57 | +59.50 | +59.50 | +0.00 | +59.50 |
| 2026-08-14 | `BETR` | 86 | — | $14.80 | +0.00 | $13.73 | -92.02 | -92.02 | +0.00 | -92.02 |
| 2026-08-14 | `ANGX` | 295 | — | $4.31 | +0.00 | $4.37 | +17.70 | +17.70 | +0.00 | +17.70 |
| 2026-08-14 | `HYLN` | 305 | — | $4.18 | +0.00 | $4.06 | -36.60 | -36.60 | +0.00 | -36.60 |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `SLG` | 22 | $56.09 | $55.37 | -15.84 | — | +0.00 | -15.84 | -49.28 | — |
| 2026-08-17 | `MARA` | 141 | $9.20 | $9.22 | +2.82 | — | +0.00 | +2.82 | +29.61 | — |
| 2026-08-17 | `LDI` | 1361 | $0.90 | $0.91 | +13.61 | — | +0.00 | +13.61 | -40.83 | — |
| 2026-08-17 | `BTBT` | 850 | $1.57 | $1.52 | -42.50 | — | +0.00 | -42.50 | +17.00 | — |
| 2026-08-17 | `BETR` | 86 | $13.73 | $13.67 | -5.16 | — | +0.00 | -5.16 | -97.18 | — |
| 2026-08-17 | `ANGX` | 295 | $4.37 | $4.60 | +67.85 | — | +0.00 | +67.85 | +85.55 | — |
| 2026-08-17 | `HYLN` | 305 | $4.06 | $4.10 | +12.20 | — | +0.00 | +12.20 | -24.40 | — |
| 2026-08-17 | `TMC` | 310 | — | $4.05 | +0.00 | $3.77 | -86.80 | -86.80 | +0.00 | -86.80 |
| 2026-08-17 | `TGB` | 148 | — | $8.46 | +0.00 | $8.77 | +45.88 | +45.88 | +0.00 | +45.88 |
| 2026-08-17 | `DNN` | 387 | — | $3.24 | +0.00 | $3.19 | -19.35 | -19.35 | +0.00 | -19.35 |
| 2026-08-17 | `CDNL` | 31 | — | $39.85 | +0.00 | $39.23 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-17 | `ABX` | 137 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `OCC` | 68 | — | $18.24 | +0.00 | $17.12 | -76.16 | -76.16 | +0.00 | -76.16 |
| 2026-08-17 | `ALM` | 77 | — | $16.20 | +0.00 | $16.36 | +12.32 | +12.32 | +0.00 | +12.32 |
| 2026-08-17 | `MRLN` | 335 | — | $3.75 | +0.00 | $3.54 | -72.02 | -72.02 | +0.00 | -72.02 |
| 2026-08-18 | `TMC` | 310 | $3.77 | $3.72 | -15.50 | — | +0.00 | -15.50 | -102.30 | — |
| 2026-08-18 | `TGB` | 148 | $8.77 | $8.55 | -32.56 | — | +0.00 | -32.56 | +13.32 | — |
| 2026-08-18 | `DNN` | 387 | $3.19 | $3.11 | -30.96 | — | +0.00 | -30.96 | -50.31 | — |
| 2026-08-18 | `CDNL` | 31 | $39.23 | $41.57 | +72.54 | — | +0.00 | +72.54 | +53.32 | — |
| 2026-08-18 | `ABX` | 137 | $9.12 | $9.03 | -12.33 | — | +0.00 | -12.33 | -12.33 | — |
| 2026-08-18 | `OCC` | 68 | $17.12 | $16.20 | -62.56 | — | +0.00 | -62.56 | -138.72 | — |
| 2026-08-18 | `ALM` | 77 | $16.36 | $15.78 | -44.66 | — | +0.00 | -44.66 | -32.34 | — |
| 2026-08-18 | `MRLN` | 335 | $3.54 | $3.50 | -11.73 | — | +0.00 | -11.73 | -83.75 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 58 | — | $20.55 | +0.00 | $21.19 | +37.12 | +37.12 | +0.00 | +37.12 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `HDSN` | 209 | — | $5.77 | +0.00 | $5.57 | -41.80 | -41.80 | +0.00 | -41.80 |
| 2026-08-20 | `IAG` | 61 | — | $19.63 | +0.00 | $20.50 | +53.07 | +53.07 | +0.00 | +53.07 |
| 2026-08-20 | `KGC` | 40 | — | $29.63 | +0.00 | $31.43 | +72.00 | +72.00 | +0.00 | +72.00 |
| 2026-08-20 | `NFGC` | 689 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `MRVI` | 162 | — | $7.44 | +0.00 | $8.29 | +137.70 | +137.70 | +0.00 | +137.70 |
| 2026-08-20 | `SCZM` | 127 | — | $9.46 | +0.00 | $9.76 | +38.10 | +38.10 | +0.00 | +38.10 |
| 2026-08-21 | `AG` | 58 | $21.19 | $21.90 | +41.18 | — | +0.00 | +41.18 | +78.30 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `HDSN` | 209 | $5.57 | $5.67 | +20.90 | — | +0.00 | +20.90 | -20.90 | — |
| 2026-08-21 | `IAG` | 61 | $20.50 | $21.17 | +40.87 | — | +0.00 | +40.87 | +93.94 | — |
| 2026-08-21 | `KGC` | 40 | $31.43 | $32.17 | +29.60 | — | +0.00 | +29.60 | +101.60 | — |
| 2026-08-21 | `NFGC` | 689 | $1.75 | $1.79 | +27.56 | — | +0.00 | +27.56 | +27.56 | — |
| 2026-08-21 | `MRVI` | 162 | $8.29 | $8.28 | -1.62 | — | +0.00 | -1.62 | +136.08 | — |
| 2026-08-21 | `SCZM` | 127 | $9.76 | $10.26 | +63.50 | — | +0.00 | +63.50 | +101.60 | — |
| 2026-08-21 | `CRSP` | 28 | — | $59.72 | +0.00 | $59.50 | -6.16 | -6.16 | +0.00 | -6.16 |
| 2026-08-21 | `CF` | 13 | — | $127.43 | +0.00 | $129.60 | +28.21 | +28.21 | +0.00 | +28.21 |
| 2026-08-21 | `EMBC` | 312 | — | $5.43 | +0.00 | $5.23 | -62.40 | -62.40 | +0.00 | -62.40 |
| 2026-08-21 | `TXG` | 26 | — | $64.39 | +0.00 | $65.12 | +18.98 | +18.98 | +0.00 | +18.98 |
| 2026-08-21 | `BEKE` | 94 | — | $17.93 | +0.00 | $17.75 | -17.39 | -17.39 | +0.00 | -17.39 |
| 2026-08-21 | `HITI` | 698 | — | $2.43 | +0.00 | $2.45 | +13.96 | +13.96 | +0.00 | +13.96 |
| 2026-08-24 | `CRSP` | 28 | $59.50 | $58.75 | -21.00 | — | +0.00 | -21.00 | -27.16 | — |
| 2026-08-24 | `CF` | 13 | $129.60 | $129.99 | +5.07 | — | +0.00 | +5.07 | +33.28 | — |
| 2026-08-24 | `EMBC` | 312 | $5.23 | $5.20 | -10.92 | — | +0.00 | -10.92 | -73.32 | — |
| 2026-08-24 | `TXG` | 26 | $65.12 | $63.15 | -51.22 | — | +0.00 | -51.22 | -32.24 | — |
| 2026-08-24 | `BEKE` | 94 | $17.75 | $18.05 | +28.67 | — | +0.00 | +28.67 | +11.28 | — |
| 2026-08-24 | `HITI` | 698 | $2.45 | $2.45 | +0.00 | — | +0.00 | +0.00 | +13.96 | — |
| 2026-08-25 | `CRMD` | 150 | — | $8.35 | +0.00 | $8.56 | +31.50 | +31.50 | +0.00 | +31.50 |
| 2026-08-25 | `ELMT` | 70 | — | $17.89 | +0.00 | $17.75 | -9.80 | -9.80 | +0.00 | -9.80 |
| 2026-08-25 | `AMTX` | 662 | — | $1.90 | +0.00 | $1.91 | +6.62 | +6.62 | +0.00 | +6.62 |
| 2026-08-25 | `BZ` | 82 | — | $15.28 | +0.00 | $16.29 | +82.82 | +82.82 | +0.00 | +82.82 |
| 2026-08-25 | `VIPS` | 90 | — | $13.96 | +0.00 | $14.16 | +18.00 | +18.00 | +0.00 | +18.00 |
| 2026-08-25 | `RHI` | 28 | — | $43.76 | +0.00 | $44.90 | +31.92 | +31.92 | +0.00 | +31.92 |
| 2026-08-25 | `VALE` | 83 | — | $15.01 | +0.00 | $15.33 | +26.56 | +26.56 | +0.00 | +26.56 |
| 2026-08-25 | `AYA` | 47 | — | $26.21 | +0.00 | $27.71 | +70.50 | +70.50 | +0.00 | +70.50 |
| 2026-08-26 | `CRMD` | 150 | $8.56 | $8.60 | +6.00 | — | +0.00 | +6.00 | +37.50 | — |
| 2026-08-26 | `ELMT` | 70 | $17.75 | $17.82 | +4.90 | — | +0.00 | +4.90 | -4.90 | — |
| 2026-08-26 | `AMTX` | 662 | $1.91 | $1.91 | +0.00 | — | +0.00 | +0.00 | +6.62 | — |
| 2026-08-26 | `BZ` | 82 | $16.29 | $16.77 | +39.36 | — | +0.00 | +39.36 | +122.18 | — |
| 2026-08-26 | `VIPS` | 90 | $14.16 | $14.00 | -14.40 | — | +0.00 | -14.40 | +3.60 | — |
| 2026-08-26 | `RHI` | 28 | $44.90 | $44.33 | -15.96 | — | +0.00 | -15.96 | +15.96 | — |
| 2026-08-26 | `VALE` | 83 | $15.33 | $15.37 | +3.32 | — | +0.00 | +3.32 | +29.88 | — |
| 2026-08-26 | `AYA` | 47 | $27.71 | $27.24 | -22.09 | — | +0.00 | -22.09 | +48.41 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | `TTMI` | 10 | — | $122.81 | +0.00 | $118.65 | -41.60 | -41.60 | +0.00 | -41.60 |
| 2026-08-28 | `KEYS` | 3 | — | $324.41 | +0.00 | $319.97 | -13.32 | -13.32 | +0.00 | -13.32 |
| 2026-08-28 | `AVT` | 14 | — | $91.49 | +0.00 | $88.63 | -40.04 | -40.04 | +0.00 | -40.04 |
| 2026-08-28 | `CGNX` | 20 | — | $62.82 | +0.00 | $60.46 | -47.20 | -47.20 | +0.00 | -47.20 |
| 2026-08-28 | `COHR` | 4 | — | $289.44 | +0.00 | $279.20 | -40.96 | -40.96 | +0.00 | -40.96 |
| 2026-08-28 | `LSCC` | 10 | — | $119.76 | +0.00 | $114.40 | -53.60 | -53.60 | +0.00 | -53.60 |
| 2026-08-28 | `MTSI` | 4 | — | $275.20 | +0.00 | $265.27 | -39.72 | -39.72 | +0.00 | -39.72 |
| 2026-08-28 | `OLED` | 15 | — | $85.02 | +0.00 | $82.98 | -30.60 | -30.60 | +0.00 | -30.60 |
| 2026-08-31 | `TTMI` | 10 | $118.65 | $118.83 | +1.80 | — | +0.00 | +1.80 | -39.80 | — |
| 2026-08-31 | `KEYS` | 3 | $319.97 | $322.49 | +7.56 | — | +0.00 | +7.56 | -5.76 | — |
| 2026-08-31 | `AVT` | 14 | $88.63 | $89.39 | +10.64 | — | +0.00 | +10.64 | -29.40 | — |
| 2026-08-31 | `CGNX` | 20 | $60.46 | $60.46 | +0.00 | — | +0.00 | +0.00 | -47.20 | — |
| 2026-08-31 | `COHR` | 4 | $279.20 | $280.25 | +4.20 | — | +0.00 | +4.20 | -36.76 | — |
| 2026-08-31 | `LSCC` | 10 | $114.40 | $115.56 | +11.60 | — | +0.00 | +11.60 | -42.00 | — |
| 2026-08-31 | `MTSI` | 4 | $265.27 | $266.96 | +6.76 | — | +0.00 | +6.76 | -32.96 | — |
| 2026-08-31 | `OLED` | 15 | $82.98 | $83.28 | +4.50 | — | +0.00 | +4.50 | -26.10 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 23 | — | $52.88 | +0.00 | $52.46 | -9.66 | -9.66 | +0.00 | -9.66 |
| 2026-09-03 | `HRMY` | 29 | — | $42.93 | +0.00 | $41.86 | -31.03 | -31.03 | +0.00 | -31.03 |
| 2026-09-03 | `CABA` | 343 | — | $3.63 | +0.00 | $3.48 | -51.45 | -51.45 | +0.00 | -51.45 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `ARCT` | 74 | — | $16.77 | +0.00 | $15.56 | -89.54 | -89.54 | +0.00 | -89.54 |
| 2026-09-03 | `CRDL` | 572 | — | $2.18 | +0.00 | $2.16 | -11.44 | -11.44 | +0.00 | -11.44 |
| 2026-09-03 | `SDGR` | 59 | — | $21.03 | +0.00 | $20.71 | -18.88 | -18.88 | +0.00 | -18.88 |
| 2026-09-03 | `VIR` | 108 | — | $11.54 | +0.00 | $11.45 | -9.72 | -9.72 | +0.00 | -9.72 |
| 2026-09-04 | `ATRC` | 23 | $52.46 | $52.03 | -9.89 | $51.52 | -11.73 | -21.62 | -19.55 | -31.28 |
| 2026-09-04 | `HRMY` | 29 | $41.86 | $41.50 | -10.44 | $42.25 | +21.75 | +11.31 | -41.47 | -19.72 |
| 2026-09-04 | `CABA` | 343 | $3.48 | $3.46 | -6.86 | $3.47 | +3.43 | -3.43 | -58.31 | -54.88 |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `ARCT` | 74 | $15.56 | $15.61 | +3.70 | — | +0.00 | +3.70 | -85.84 | — |
| 2026-09-04 | `CRDL` | 572 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.44 | — |
| 2026-09-04 | `SDGR` | 59 | $20.71 | $20.58 | -7.67 | — | +0.00 | -7.67 | -26.55 | — |
| 2026-09-04 | `VIR` | 108 | $11.45 | $11.31 | -15.12 | $11.38 | +8.10 | -7.02 | -24.84 | -16.74 |
| 2026-09-04 | `ALEC` | 480 | — | $2.52 | +0.00 | $2.46 | -28.80 | -28.80 | +0.00 | -28.80 |
| 2026-09-04 | `BHC` | 180 | — | $6.71 | +0.00 | $6.56 | -27.00 | -27.00 | +0.00 | -27.00 |
| 2026-09-04 | `OABI` | 253 | — | $4.78 | +0.00 | $4.33 | -113.85 | -113.85 | +0.00 | -113.85 |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-08 | `ATRC` | 23 | $51.52 | $54.31 | +64.17 | — | +0.00 | +64.17 | +32.89 | — |
| 2026-09-08 | `HRMY` | 29 | $42.25 | $42.20 | -1.45 | — | +0.00 | -1.45 | -21.17 | — |
| 2026-09-08 | `CABA` | 343 | $3.47 | $3.43 | -13.72 | — | +0.00 | -13.72 | -68.60 | — |
| 2026-09-08 | `VIR` | 108 | $11.38 | $11.22 | -17.82 | — | +0.00 | -17.82 | -34.56 | — |
| 2026-09-08 | `ALEC` | 480 | $2.46 | $2.38 | -38.40 | — | +0.00 | -38.40 | -67.20 | — |
| 2026-09-08 | `BHC` | 180 | $6.56 | $6.57 | +1.80 | — | +0.00 | +1.80 | -25.20 | — |
| 2026-09-08 | `OABI` | 253 | $4.33 | $4.30 | -7.59 | — | +0.00 | -7.59 | -121.44 | — |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `PAGS` | 232 | — | $10.11 | +0.00 | $10.12 | +2.32 | +2.32 | +0.00 | +2.32 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +216.83 | BTSG, TPG, TGTX, SLS, HIMS, VOR | — | $134.73 | $10,203.79 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75 |
| 2026-08-14 | +5.50 | $134.73 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75 | $10,217.42 | +13.63 | -101.53 | DAVE, SLG, MARA, LDI, BTBT, BETR, ANGX, HYLN | BTSG, TPG, TGTX, SLS, HIMS, VOR | $260.01 | $10,058.44 | DAVE×3, SLG×22, MARA×141, LDI×1361, BTBT×850, BETR×86, ANGX×295, HYLN×305 |
| 2026-08-17 | +2.25 | $260.01 | DAVE×3, SLG×22, MARA×141, LDI×1361, BTBT×850, BETR×86, ANGX×295, HYLN×305 | $10,098.53 | +40.09 | -215.35 | TMC, TGB, DNN, CDNL, ABX, OCC, ALM, MRLN | DAVE, SLG, MARA, LDI, BTBT, BETR, ANGX, HYLN | $39.21 | $9,814.08 | TMC×310, TGB×148, DNN×387, CDNL×31, ABX×137, OCC×68, ALM×77, MRLN×335 |
| 2026-08-18 | -6.20 | $39.21 | TMC×310, TGB×148, DNN×387, CDNL×31, ABX×137, OCC×68, ALM×77, MRLN×335 | $9,676.32 | -137.76 | +0.00 | — | TMC, TGB, DNN, CDNL, ABX, OCC, ALM, MRLN | $9,651.35 | $9,651.35 | — |
| 2026-08-19 | -7.20 | $9,651.35 | — | $9,651.35 | -0.00 | +0.00 | — | — | $9,651.35 | $9,651.35 | — |
| 2026-08-20 | +1.12 | $9,651.35 | — | $9,651.35 | -0.00 | +330.25 | AG, BHP, HDSN, IAG, KGC, NFGC, MRVI, SCZM | — | $50.40 | $9,956.69 | AG×58, BHP×13, HDSN×209, IAG×61, KGC×40, NFGC×689, MRVI×162, SCZM×127 |
| 2026-08-21 | +3.25 | $50.40 | AG×58, BHP×13, HDSN×209, IAG×61, KGC×40, NFGC×689, MRVI×162, SCZM×127 | $10,205.85 | +249.16 | -24.80 | CRSP, CF, EMBC, TXG, BEKE, HITI | AG, BHP, HDSN, IAG, KGC, NFGC, MRVI, SCZM | $80.07 | $10,134.35 | CRSP×28, CF×13, EMBC×312, TXG×26, BEKE×94, HITI×698 |
| 2026-08-24 | -5.17 | $80.07 | CRSP×28, CF×13, EMBC×312, TXG×26, BEKE×94, HITI×698 | $10,084.95 | -49.40 | +0.00 | — | CRSP, CF, EMBC, TXG, BEKE, HITI | $10,063.18 | $10,063.18 | — |
| 2026-08-25 | +1.80 | $10,063.18 | — | $10,063.18 | +0.00 | +258.12 | CRMD, ELMT, AMTX, BZ, VIPS, RHI, VALE, AYA | — | $64.12 | $10,297.18 | CRMD×150, ELMT×70, AMTX×662, BZ×82, VIPS×90, RHI×28, VALE×83, AYA×47 |
| 2026-08-26 | +2.02 | $64.12 | CRMD×150, ELMT×70, AMTX×662, BZ×82, VIPS×90, RHI×28, VALE×83, AYA×47 | $10,298.31 | +1.13 | +0.00 | — | CRMD, ELMT, AMTX, BZ, VIPS, RHI, VALE, AYA | $10,273.90 | $10,273.90 | — |
| 2026-08-27 | — | $10,273.90 | — | $10,273.90 | +0.00 | +0.00 | — | — | $10,273.90 | $10,273.90 | — |
| 2026-08-28 | +0.75 | $10,273.90 | — | $10,273.90 | +0.00 | -307.04 | TTMI, KEYS, AVT, CGNX, COHR, LSCC, MTSI, OLED | — | $787.69 | $9,950.70 | TTMI×10, KEYS×3, AVT×14, CGNX×20, COHR×4, LSCC×10, MTSI×4, OLED×15 |
| 2026-08-31 | -5.85 | $787.69 | TTMI×10, KEYS×3, AVT×14, CGNX×20, COHR×4, LSCC×10, MTSI×4, OLED×15 | $9,997.76 | +47.06 | +0.00 | — | TTMI, KEYS, AVT, CGNX, COHR, LSCC, MTSI, OLED | $9,981.44 | $9,981.44 | — |
| 2026-09-01 | -6.30 | $9,981.44 | — | $9,981.44 | +0.00 | +0.00 | — | — | $9,981.44 | $9,981.44 | — |
| 2026-09-02 | -3.83 | $9,981.44 | — | $9,981.44 | +0.00 | +0.00 | — | — | $9,981.44 | $9,981.44 | — |
| 2026-09-03 | -0.90 | $9,981.44 | — | $9,981.44 | +0.00 | -238.10 | ATRC, HRMY, CABA, RVTY, ARCT, CRDL, SDGR, VIR | — | $83.41 | $9,718.69 | ATRC×23, HRMY×29, CABA×343, RVTY×9, ARCT×74, CRDL×572, SDGR×59, VIR×108 |
| 2026-09-04 | +2.25 | $83.41 | ATRC×23, HRMY×29, CABA×343, RVTY×9, ARCT×74, CRDL×572, SDGR×59, VIR×108 | $9,667.01 | -51.68 | -164.62 | ALEC, BHC, OABI, CRM | RVTY, ARCT, CRDL, SDGR | $150.45 | $9,474.47 | ATRC×23, HRMY×29, CABA×343, VIR×108, ALEC×480, BHC×180, OABI×253, CRM×4 |
| 2026-09-08 | -11.47 | $150.45 | ATRC×23, HRMY×29, CABA×343, VIR×108, ALEC×480, BHC×180, OABI×253, CRM×4 | $9,439.41 | -35.06 | +0.00 | — | ATRC, HRMY, CABA, VIR, ALEC, BHC, OABI, CRM | $9,414.22 | $9,414.22 | — |
| 2026-09-09 | -13.95 | $9,414.22 | — | $9,414.22 | -0.00 | +0.00 | — | — | $9,414.22 | $9,414.22 | — |
| 2026-09-10 | -13.28 | $9,414.22 | — | $9,414.22 | -0.00 | +0.00 | — | — | $9,414.22 | $9,414.22 | — |
| 2026-09-11 | +0.50 | $9,414.22 | — | $9,414.22 | -0.00 | +2.32 | PAGS | — | $7,065.70 | $9,413.54 | PAGS×232 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 27 | $59.80 | $2.07 | — | $8,383.33 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $6,761.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 33 | $49.70 | $2.09 | — | $5,119.11 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 142 | $11.70 | $2.42 | — | $3,455.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 56 | $29.74 | $2.16 | — | $1,787.70 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 75 | $22.01 | $2.21 | — | $134.73 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.73 | ▲ close $10,203.79 vs 09:30 $10,000.00 (session +216.83) | 16:00 close · cash $134.73 · equity $10,203.79 vs 09:30 $10,000.00 (+203.79; session marks +216.83) · 6 name(s) marked open→close (per-name table). BTSG×27 09:30 $59.80 → close $60.23 +11.61; TPG×32 09:30 $50.62 → close $54.62 +127.90; TGTX×33 09:30 $49.70 → close $47.94 -58.08; SLS×142 09:30 $11.70 → close $12.36 +93.72; HIMS×56 09:30 $29.74 → close $28.77 -54.32; VOR×75 09:30 $22.01 → close $23.29 +96.00 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.73 | ▲ 09:30 equity $10,217.42 vs yday $10,203.79 (+13.63) | 09:30 open · cash $134.73 (unchanged overnight, no fees) · equity $10,217.42 vs prior close $10,203.79 (+13.63) · 6 name(s) re-marked at the open (per-name table). BTSG×27 yday $60.23 → 09:30 $59.65 -15.66; TPG×32 yday $54.62 → 09:30 $55.29 +21.44; TGTX×33 yday $47.94 → 09:30 $47.27 -22.11; SLS×142 yday $12.36 → 09:30 $12.40 +5.68; HIMS×56 yday $28.77 → 09:30 $29.15 +21.28; VOR×75 yday $23.29 → 09:30 $23.33 +3.00 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 27 | $59.65 | $2.09 | $-8.21 | $1,743.19 | ▼ -8.21 after sell → book $10,215.33; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $3,510.36 | ▲ +145.14 after sell → book $10,213.22; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 33 | $47.27 | $2.11 | $-84.39 | $5,068.16 | ▼ -84.39 after sell → book $10,211.11; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 142 | $12.40 | $2.45 | $+94.53 | $6,826.50 | ▲ +94.53 after sell → book $10,208.65; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 56 | $29.15 | $2.18 | $-37.38 | $8,456.72 | ▼ -37.38 after sell → book $10,206.47; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 75 | $23.33 | $2.24 | $+94.54 | $10,204.23 | ▲ +94.54 after sell → book $10,204.23; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $9,209.50 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $7,940.03 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 141 | $9.01 | $2.41 | — | $6,667.20 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1361 | $0.94 | $16.84 | — | $5,375.11 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 850 | $1.50 | $10.96 | — | $4,089.15 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 86 | $14.80 | $2.25 | — | $2,814.10 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 295 | $4.31 | $3.81 | — | $1,538.84 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 305 | $4.18 | $3.93 | — | $260.01 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $260.01 | ▼ close $10,058.44 vs 09:30 $10,217.42 (session -101.53) | 16:00 close · cash $260.01 · equity $10,058.44 vs 09:30 $10,217.42 (-158.98; session marks -101.53) · 8 name(s) marked open→close (per-name table). DAVE×3 09:30 $330.91 → close $334.57 +10.98; SLG×22 09:30 $57.61 → close $56.09 -33.44; MARA×141 09:30 $9.01 → close $9.20 +26.79; LDI×1361 09:30 $0.94 → close $0.90 -54.44; BTBT×850 09:30 $1.50 → close $1.57 +59.50; BETR×86 09:30 $14.80 → close $13.73 -92.02; ANGX×295 09:30 $4.31 → close $4.37 +17.70; HYLN×305 09:30 $4.18 → close $4.06 -36.60 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $260.01 | ▲ 09:30 equity $10,098.53 vs yday $10,058.44 (+40.09) | 09:30 open · cash $260.01 (unchanged overnight, no fees) · equity $10,098.53 vs prior close $10,058.44 (+40.09) · 8 name(s) re-marked at the open (per-name table). DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×22 yday $56.09 → 09:30 $55.37 -15.84; MARA×141 yday $9.20 → 09:30 $9.22 +2.82; LDI×1361 yday $0.90 → 09:30 $0.91 +13.61; BTBT×850 yday $1.57 → 09:30 $1.52 -42.50; BETR×86 yday $13.73 → 09:30 $13.67 -5.16; ANGX×295 yday $4.37 → 09:30 $4.60 +67.85; HYLN×305 yday $4.06 → 09:30 $4.10 +12.20 | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $1,268.81 | ▲ +14.07 after sell → book $10,096.52; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $2,484.87 | ▼ -53.41 after sell → book $10,094.44; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 141 | $9.22 | $2.45 | $+24.75 | $3,782.45 | ▲ +24.75 after sell → book $10,091.99; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1361 | $0.91 | $16.66 | $-74.33 | $5,000.21 | ▼ -74.33 after sell → book $10,075.33; vs 09:30 mark -16.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 850 | $1.52 | $11.12 | $-5.08 | $6,281.09 | ▼ -5.08 after sell → book $10,064.21; vs 09:30 mark -11.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 86 | $13.67 | $2.27 | $-101.70 | $7,454.44 | ▼ -101.70 after sell → book $10,061.94; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 295 | $4.60 | $3.87 | $+77.88 | $8,807.58 | ▲ +77.88 after sell → book $10,058.08; vs 09:30 mark -3.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 305 | $4.10 | $4.00 | $-32.33 | $10,054.08 | ▼ -32.33 after sell → book $10,054.08; vs 09:30 mark -4.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 310 | $4.05 | $4.00 | — | $8,794.58 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1256.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 148 | $8.46 | $2.43 | — | $7,540.07 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1256.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 387 | $3.24 | $4.99 | — | $6,281.19 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $1256.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $5,043.76 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1256.76 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 137 | $9.12 | $2.40 | — | $3,791.92 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1256.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 68 | $18.24 | $2.19 | — | $2,549.41 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1256.76 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $1,299.79 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1256.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `MRLN` | 335 | $3.75 | $4.32 | — | $39.21 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-15.4; leftover $1256.76 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.21 | ▼ close $9,814.08 vs 09:30 $10,098.53 (session -215.35) | 16:00 close · cash $39.21 · equity $9,814.08 vs 09:30 $10,098.53 (-284.45; session marks -215.35) · 8 name(s) marked open→close (per-name table). TMC×310 09:30 $4.05 → close $3.77 -86.80; TGB×148 09:30 $8.46 → close $8.77 +45.88; DNN×387 09:30 $3.24 → close $3.19 -19.35; CDNL×31 09:30 $39.85 → close $39.23 -19.22; ABX×137 09:30 $9.12 → close $9.12 +0.00; OCC×68 09:30 $18.24 → close $17.12 -76.16; ALM×77 09:30 $16.20 → close $16.36 +12.32; MRLN×335 09:30 $3.75 → close $3.54 -72.02 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.21 | ▼ 09:30 equity $9,676.32 vs yday $9,814.08 (-137.76) | 09:30 open · cash $39.21 (unchanged overnight, no fees) · equity $9,676.32 vs prior close $9,814.08 (-137.76) · 8 name(s) re-marked at the open (per-name table). TMC×310 yday $3.77 → 09:30 $3.72 -15.50; TGB×148 yday $8.77 → 09:30 $8.55 -32.56; DNN×387 yday $3.19 → 09:30 $3.11 -30.96; CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; ABX×137 yday $9.12 → 09:30 $9.03 -12.33; OCC×68 yday $17.12 → 09:30 $16.20 -62.56; ALM×77 yday $16.36 → 09:30 $15.78 -44.66; MRLN×335 yday $3.54 → 09:30 $3.50 -11.73 | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 310 | $3.72 | $4.06 | $-110.36 | $1,188.35 | ▼ -110.36 after sell → book $9,672.26; vs 09:30 mark -4.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 148 | $8.55 | $2.47 | $+8.42 | $2,451.29 | ▲ +8.42 after sell → book $9,669.80; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 387 | $3.11 | $5.07 | $-60.37 | $3,649.79 | ▼ -60.37 after sell → book $9,664.73; vs 09:30 mark -5.07 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $4,936.36 | ▲ +49.13 after sell → book $9,662.63; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 137 | $9.03 | $2.43 | $-17.16 | $6,171.03 | ▼ -17.16 after sell → book $9,660.19; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 68 | $16.20 | $2.22 | $-143.13 | $7,270.42 | ▼ -143.13 after sell → book $9,657.98; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $8,483.23 | ▼ -36.80 after sell → book $9,655.73; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `MRLN` | 335 | $3.50 | $4.39 | $-92.46 | $9,651.35 | ▼ -92.46 after sell → book $9,651.35; vs 09:30 mark -4.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,651.35 | ▲ close $9,651.35 vs 09:30 $9,676.32 (session +0.00) | 16:00 close · cash $9,651.35 · no lots left · equity $9,651.35. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,651.35 | ▲ 09:30 equity $9,651.35 vs yday $9,651.35 (-0.00) | 09:30 open · cash $9,651.35 · no holdings · equity $9,651.35 vs prior close $9,651.35 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,651.35 | ▲ close $9,651.35 vs 09:30 $9,651.35 (session +0.00) | 16:00 close · cash $9,651.35 · no lots left · equity $9,651.35. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,651.35 | ▲ 09:30 equity $9,651.35 vs yday $9,651.35 (-0.00) | 09:30 open · cash $9,651.35 · no holdings · equity $9,651.35 vs prior close $9,651.35 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,457.28 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1206.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,272.12 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1206.42 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 209 | $5.77 | $2.70 | — | $6,063.50 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1206.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $4,863.89 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1206.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $3,676.58 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1206.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 689 | $1.75 | $8.89 | — | $2,461.95 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1206.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 162 | $7.44 | $2.48 | — | $1,254.19 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1206.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 127 | $9.46 | $2.37 | — | $50.40 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1206.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.40 | ▲ close $9,956.69 vs 09:30 $9,651.35 (session +330.25) | 16:00 close · cash $50.40 · equity $9,956.69 vs 09:30 $9,651.35 (+305.34; session marks +330.25) · 8 name(s) marked open→close (per-name table). AG×58 09:30 $20.55 → close $21.19 +37.12; BHP×13 09:30 $91.01 → close $93.63 +34.06; HDSN×209 09:30 $5.77 → close $5.57 -41.80; IAG×61 09:30 $19.63 → close $20.50 +53.07; KGC×40 09:30 $29.63 → close $31.43 +72.00; NFGC×689 09:30 $1.75 → close $1.75 +0.00; MRVI×162 09:30 $7.44 → close $8.29 +137.70; SCZM×127 09:30 $9.46 → close $9.76 +38.10 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.40 | ▲ 09:30 equity $10,205.85 vs yday $9,956.69 (+249.16) | 09:30 open · cash $50.40 (unchanged overnight, no fees) · equity $10,205.85 vs prior close $9,956.69 (+249.16) · 8 name(s) re-marked at the open (per-name table). AG×58 yday $21.19 → 09:30 $21.90 +41.18; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; HDSN×209 yday $5.57 → 09:30 $5.67 +20.90; IAG×61 yday $20.50 → 09:30 $21.17 +40.87; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×689 yday $1.75 → 09:30 $1.79 +27.56; MRVI×162 yday $8.29 → 09:30 $8.28 -1.62; SCZM×127 yday $9.76 → 09:30 $10.26 +63.50 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,318.41 | ▲ +73.95 after sell → book $10,203.66; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,560.73 | ▲ +57.15 after sell → book $10,201.61; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 209 | $5.67 | $2.74 | $-26.34 | $3,743.01 | ▼ -26.34 after sell → book $10,198.87; vs 09:30 mark -2.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $5,032.19 | ▲ +89.57 after sell → book $10,196.68; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $6,316.86 | ▲ +97.36 after sell → book $10,194.55; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 689 | $1.79 | $9.01 | $+9.66 | $7,541.16 | ▲ +9.66 after sell → book $10,185.54; vs 09:30 mark -9.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 162 | $8.28 | $2.51 | $+131.09 | $8,880.00 | ▲ +131.09 after sell → book $10,183.02; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 127 | $10.26 | $2.40 | $+96.83 | $10,180.62 | ▲ +96.83 after sell → book $10,180.62; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 28 | $59.72 | $2.07 | — | $8,506.39 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1696.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 13 | $127.43 | $2.03 | — | $6,847.77 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1696.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 312 | $5.43 | $4.02 | — | $5,149.58 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1696.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 26 | $64.39 | $2.07 | — | $3,473.38 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1696.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 94 | $17.93 | $2.27 | — | $1,785.21 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $1696.77 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 698 | $2.43 | $9.00 | — | $80.07 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $1696.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.07 | ▼ close $10,134.35 vs 09:30 $10,205.85 (session -24.80) | 16:00 close · cash $80.07 · equity $10,134.35 vs 09:30 $10,205.85 (-71.50; session marks -24.80) · 6 name(s) marked open→close (per-name table). CRSP×28 09:30 $59.72 → close $59.50 -6.16; CF×13 09:30 $127.43 → close $129.60 +28.21; EMBC×312 09:30 $5.43 → close $5.23 -62.40; TXG×26 09:30 $64.39 → close $65.12 +18.98; BEKE×94 09:30 $17.93 → close $17.75 -17.39; HITI×698 09:30 $2.43 → close $2.45 +13.96 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.07 | ▼ 09:30 equity $10,084.95 vs yday $10,134.35 (-49.40) | 09:30 open · cash $80.07 (unchanged overnight, no fees) · equity $10,084.95 vs prior close $10,134.35 (-49.40) · 6 name(s) re-marked at the open (per-name table). CRSP×28 yday $59.50 → 09:30 $58.75 -21.00; CF×13 yday $129.60 → 09:30 $129.99 +5.07; EMBC×312 yday $5.23 → 09:30 $5.20 -10.92; TXG×26 yday $65.12 → 09:30 $63.15 -51.22; BEKE×94 yday $17.75 → 09:30 $18.05 +28.67; HITI×698 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 28 | $58.75 | $2.10 | $-31.33 | $1,722.97 | ▼ -31.33 after sell → book $10,082.85; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 13 | $129.99 | $2.05 | $+29.20 | $3,410.79 | ▲ +29.20 after sell → book $10,080.80; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 312 | $5.20 | $4.09 | $-81.43 | $5,027.54 | ▼ -81.43 after sell → book $10,076.71; vs 09:30 mark -4.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 26 | $63.15 | $2.09 | $-36.40 | $6,667.35 | ▼ -36.40 after sell → book $10,074.62; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 94 | $18.05 | $2.30 | $+6.71 | $8,362.22 | ▲ +6.71 after sell → book $10,072.32; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 698 | $2.45 | $9.13 | $-4.18 | $10,063.18 | ▼ -4.18 after sell → book $10,063.18; vs 09:30 mark -9.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,063.18 | ▲ close $10,063.18 vs 09:30 $10,084.95 (session +0.00) | 16:00 close · cash $10,063.18 · no lots left · equity $10,063.18. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,063.18 | ▲ 09:30 equity $10,063.18 vs yday $10,063.18 (+0.00) | 09:30 open · cash $10,063.18 · no holdings · equity $10,063.18 vs prior close $10,063.18 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 150 | $8.35 | $2.44 | — | $8,808.24 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1257.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ELMT` | 70 | $17.89 | $2.20 | — | $7,553.74 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-7.5; leftover $1257.90 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 662 | $1.90 | $8.54 | — | $6,287.40 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=+5.0; leftover $1257.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 82 | $15.28 | $2.24 | — | $5,032.21 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $1257.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 90 | $13.96 | $2.26 | — | $3,773.55 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.0; leftover $1257.90 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 28 | $43.76 | $2.07 | — | $2,546.19 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1257.90 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VALE` | 83 | $15.01 | $2.24 | — | $1,298.13 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; ⚪; ret5=+9.4; leftover $1257.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AYA` | 47 | $26.21 | $2.13 | — | $64.12 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; ⚪; ret5=-0.3; leftover $1257.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.12 | ▲ close $10,297.18 vs 09:30 $10,063.18 (session +258.12) | 16:00 close · cash $64.12 · equity $10,297.18 vs 09:30 $10,063.18 (+234.00; session marks +258.12) · 8 name(s) marked open→close (per-name table). CRMD×150 09:30 $8.35 → close $8.56 +31.50; ELMT×70 09:30 $17.89 → close $17.75 -9.80; AMTX×662 09:30 $1.90 → close $1.91 +6.62; BZ×82 09:30 $15.28 → close $16.29 +82.82; VIPS×90 09:30 $13.96 → close $14.16 +18.00; RHI×28 09:30 $43.76 → close $44.90 +31.92; VALE×83 09:30 $15.01 → close $15.33 +26.56; AYA×47 09:30 $26.21 → close $27.71 +70.50 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.12 | ▲ 09:30 equity $10,298.31 vs yday $10,297.18 (+1.13) | 09:30 open · cash $64.12 (unchanged overnight, no fees) · equity $10,298.31 vs prior close $10,297.18 (+1.13) · 8 name(s) re-marked at the open (per-name table). CRMD×150 yday $8.56 → 09:30 $8.60 +6.00; ELMT×70 yday $17.75 → 09:30 $17.82 +4.90; AMTX×662 yday $1.91 → 09:30 $1.91 +0.00; BZ×82 yday $16.29 → 09:30 $16.77 +39.36; VIPS×90 yday $14.16 → 09:30 $14.00 -14.40; RHI×28 yday $44.90 → 09:30 $44.33 -15.96; VALE×83 yday $15.33 → 09:30 $15.37 +3.32; AYA×47 yday $27.71 → 09:30 $27.24 -22.09 | — |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 150 | $8.60 | $2.48 | $+32.58 | $1,351.65 | ▲ +32.58 after sell → book $10,295.84; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `ELMT` | 70 | $17.82 | $2.22 | $-9.32 | $2,596.83 | ▼ -9.32 after sell → book $10,293.62; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AMTX` | 662 | $1.91 | $8.66 | $-10.58 | $3,852.59 | ▼ -10.58 after sell → book $10,284.96; vs 09:30 mark -8.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BZ` | 82 | $16.77 | $2.26 | $+117.68 | $5,225.47 | ▲ +117.68 after sell → book $10,282.70; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `VIPS` | 90 | $14.00 | $2.29 | $-0.95 | $6,483.18 | ▼ -0.95 after sell → book $10,280.41; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 28 | $44.33 | $2.09 | $+11.79 | $7,722.33 | ▲ +11.79 after sell → book $10,278.32; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VALE` | 83 | $15.37 | $2.26 | $+25.38 | $8,995.78 | ▲ +25.38 after sell → book $10,276.06; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AYA` | 47 | $27.24 | $2.15 | $+44.13 | $10,273.90 | ▲ +44.13 after sell → book $10,273.90; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,273.90 | ▲ close $10,273.90 vs 09:30 $10,298.31 (session +0.00) | 16:00 close · cash $10,273.90 · no lots left · equity $10,273.90. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,273.90 | ▲ 09:30 equity $10,273.90 vs yday $10,273.90 (+0.00) | 09:30 open · cash $10,273.90 · no holdings · equity $10,273.90 vs prior close $10,273.90 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,273.90 | ▲ close $10,273.90 vs 09:30 $10,273.90 (session +0.00) | 16:00 close · cash $10,273.90 · no lots left · equity $10,273.90. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,273.90 | ▲ 09:30 equity $10,273.90 vs yday $10,273.90 (+0.00) | 09:30 open · cash $10,273.90 · no holdings · equity $10,273.90 vs prior close $10,273.90 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $9,043.78 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1284.24 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $8,068.56 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1284.24 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $6,785.66 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1284.24 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.82 | $2.05 | — | $5,527.21 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1284.24 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $4,367.45 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1284.24 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $119.76 | $2.02 | — | $3,167.83 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1284.24 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MTSI` | 4 | $275.20 | $2.00 | — | $2,065.03 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+4.1; leftover $1284.24 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OLED` | 15 | $85.02 | $2.04 | — | $787.69 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-1.9; leftover $1284.24 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $787.69 | ▼ close $9,950.70 vs 09:30 $10,273.90 (session -307.04) | 16:00 close · cash $787.69 · equity $9,950.70 vs 09:30 $10,273.90 (-323.20; session marks -307.04) · 8 name(s) marked open→close (per-name table). TTMI×10 09:30 $122.81 → close $118.65 -41.60; KEYS×3 09:30 $324.41 → close $319.97 -13.32; AVT×14 09:30 $91.49 → close $88.63 -40.04; CGNX×20 09:30 $62.82 → close $60.46 -47.20; COHR×4 09:30 $289.44 → close $279.20 -40.96; LSCC×10 09:30 $119.76 → close $114.40 -53.60; MTSI×4 09:30 $275.20 → close $265.27 -39.72; OLED×15 09:30 $85.02 → close $82.98 -30.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $787.69 | ▲ 09:30 equity $9,997.76 vs yday $9,950.70 (+47.06) | 09:30 open · cash $787.69 (unchanged overnight, no fees) · equity $9,997.76 vs prior close $9,950.70 (+47.06) · 8 name(s) re-marked at the open (per-name table). TTMI×10 yday $118.65 → 09:30 $118.83 +1.80; KEYS×3 yday $319.97 → 09:30 $322.49 +7.56; AVT×14 yday $88.63 → 09:30 $89.39 +10.64; CGNX×20 yday $60.46 → 09:30 $60.46 +0.00; COHR×4 yday $279.20 → 09:30 $280.25 +4.20; LSCC×10 yday $114.40 → 09:30 $115.56 +11.60; MTSI×4 yday $265.27 → 09:30 $266.96 +6.76; OLED×15 yday $82.98 → 09:30 $83.28 +4.50 | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $118.83 | $2.04 | $-43.86 | $1,973.95 | ▼ -43.86 after sell → book $9,995.72; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 3 | $322.49 | $2.02 | $-9.78 | $2,939.41 | ▼ -9.78 after sell → book $9,993.71; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $4,188.81 | ▼ -33.48 after sell → book $9,991.65; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 20 | $60.46 | $2.07 | $-51.32 | $5,395.94 | ▼ -51.32 after sell → book $9,989.58; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $6,514.92 | ▼ -40.78 after sell → book $9,987.56; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 10 | $115.56 | $2.04 | $-46.06 | $7,668.48 | ▼ -46.06 after sell → book $9,985.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MTSI` | 4 | $266.96 | $2.02 | $-36.98 | $8,734.30 | ▼ -36.98 after sell → book $9,983.50; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OLED` | 15 | $83.28 | $2.06 | $-30.19 | $9,981.44 | ▼ -30.19 after sell → book $9,981.44; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,981.44 | ▲ close $9,981.44 vs 09:30 $9,997.76 (session +0.00) | 16:00 close · cash $9,981.44 · no lots left · equity $9,981.44. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,981.44 | ▲ 09:30 equity $9,981.44 vs yday $9,981.44 (+0.00) | 09:30 open · cash $9,981.44 · no holdings · equity $9,981.44 vs prior close $9,981.44 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,981.44 | ▲ close $9,981.44 vs 09:30 $9,981.44 (session +0.00) | 16:00 close · cash $9,981.44 · no lots left · equity $9,981.44. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,981.44 | ▲ 09:30 equity $9,981.44 vs yday $9,981.44 (+0.00) | 09:30 open · cash $9,981.44 · no holdings · equity $9,981.44 vs prior close $9,981.44 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,981.44 | ▲ close $9,981.44 vs 09:30 $9,981.44 (session +0.00) | 16:00 close · cash $9,981.44 · no lots left · equity $9,981.44. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,981.44 | ▲ 09:30 equity $9,981.44 vs yday $9,981.44 (+0.00) | 09:30 open · cash $9,981.44 · no holdings · equity $9,981.44 vs prior close $9,981.44 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,763.15 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1247.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,516.10 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1247.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 343 | $3.63 | $4.42 | — | $6,266.58 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1247.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $5,072.52 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1247.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $3,829.32 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1247.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 572 | $2.18 | $7.38 | — | $2,574.99 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1247.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SDGR` | 59 | $21.03 | $2.17 | — | $1,332.05 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1247.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 108 | $11.54 | $2.31 | — | $83.41 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $1247.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.41 | ▼ close $9,718.69 vs 09:30 $9,981.44 (session -238.10) | 16:00 close · cash $83.41 · equity $9,718.69 vs 09:30 $9,981.44 (-262.75; session marks -238.10) · 8 name(s) marked open→close (per-name table). ATRC×23 09:30 $52.88 → close $52.46 -9.66; HRMY×29 09:30 $42.93 → close $41.86 -31.03; CABA×343 09:30 $3.63 → close $3.48 -51.45; RVTY×9 09:30 $132.45 → close $130.63 -16.38; ARCT×74 09:30 $16.77 → close $15.56 -89.54; CRDL×572 09:30 $2.18 → close $2.16 -11.44; SDGR×59 09:30 $21.03 → close $20.71 -18.88; VIR×108 09:30 $11.54 → close $11.45 -9.72 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.41 | ▼ 09:30 equity $9,667.01 vs yday $9,718.69 (-51.68) | 09:30 open · cash $83.41 (unchanged overnight, no fees) · equity $9,667.01 vs prior close $9,718.69 (-51.68) · 8 name(s) re-marked at the open (per-name table). ATRC×23 yday $52.46 → 09:30 $52.03 -9.89; HRMY×29 yday $41.86 → 09:30 $41.50 -10.44; CABA×343 yday $3.48 → 09:30 $3.46 -6.86; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; ARCT×74 yday $15.56 → 09:30 $15.61 +3.70; CRDL×572 yday $2.16 → 09:30 $2.16 +0.00; SDGR×59 yday $20.71 → 09:30 $20.58 -7.67; VIR×108 yday $11.45 → 09:30 $11.31 -15.12 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $1,251.65 | ▼ -25.83 after sell → book $9,664.98; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 74 | $15.61 | $2.23 | $-90.29 | $2,404.55 | ▼ -90.29 after sell → book $9,662.74; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 572 | $2.16 | $7.48 | $-26.30 | $3,632.59 | ▼ -26.30 after sell → book $9,655.26; vs 09:30 mark -7.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SDGR` | 59 | $20.58 | $2.19 | $-30.90 | $4,844.62 | ▼ -30.90 after sell → book $9,653.07; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 480 | $2.52 | $6.19 | — | $3,628.83 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1211.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 180 | $6.71 | $2.53 | — | $2,418.50 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1211.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 253 | $4.78 | $3.26 | — | $1,205.90 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1211.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $150.45 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1211.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $150.45 | ▼ close $9,474.47 vs 09:30 $9,667.01 (session -164.62) | 16:00 close · cash $150.45 · equity $9,474.47 vs 09:30 $9,667.01 (-192.55; session marks -164.62) · 8 name(s) marked open→close (per-name table). ATRC×23 09:30 $52.03 → close $51.52 -11.73; HRMY×29 09:30 $41.50 → close $42.25 +21.75; CABA×343 09:30 $3.46 → close $3.47 +3.43; VIR×108 09:30 $11.31 → close $11.38 +8.10; ALEC×480 09:30 $2.52 → close $2.46 -28.80; BHC×180 09:30 $6.71 → close $6.56 -27.00; OABI×253 09:30 $4.78 → close $4.33 -113.85; CRM×4 09:30 $263.36 → close $259.23 -16.52 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $150.45 | ▼ 09:30 equity $9,439.41 vs yday $9,474.47 (-35.06) | 09:30 open · cash $150.45 (unchanged overnight, no fees) · equity $9,439.41 vs prior close $9,474.47 (-35.06) · 8 name(s) re-marked at the open (per-name table). ATRC×23 yday $51.52 → 09:30 $54.31 +64.17; HRMY×29 yday $42.25 → 09:30 $42.20 -1.45; CABA×343 yday $3.47 → 09:30 $3.43 -13.72; VIR×108 yday $11.38 → 09:30 $11.22 -17.82; ALEC×480 yday $2.46 → 09:30 $2.38 -38.40; BHC×180 yday $6.56 → 09:30 $6.57 +1.80; OABI×253 yday $4.33 → 09:30 $4.30 -7.59; CRM×4 yday $259.23 → 09:30 $253.72 -22.04 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 23 | $54.31 | $2.08 | $+28.75 | $1,397.51 | ▲ +28.75 after sell → book $9,437.34; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 29 | $42.20 | $2.10 | $-25.34 | $2,619.21 | ▼ -25.34 after sell → book $9,435.24; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 343 | $3.43 | $4.49 | $-77.52 | $3,791.21 | ▼ -77.52 after sell → book $9,430.75; vs 09:30 mark -4.49 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 108 | $11.22 | $2.34 | $-39.22 | $5,000.63 | ▼ -39.22 after sell → book $9,428.41; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 480 | $2.38 | $6.28 | $-79.67 | $6,136.74 | ▼ -79.67 after sell → book $9,422.12; vs 09:30 mark -6.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 180 | $6.57 | $2.57 | $-30.30 | $7,316.77 | ▼ -30.30 after sell → book $9,419.55; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 253 | $4.30 | $3.32 | $-128.02 | $8,401.36 | ▼ -128.02 after sell → book $9,416.24; vs 09:30 mark -3.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $9,414.22 | ▼ -42.58 after sell → book $9,414.22; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,414.22 | ▲ close $9,414.22 vs 09:30 $9,439.41 (session +0.00) | 16:00 close · cash $9,414.22 · no lots left · equity $9,414.22. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,414.22 | ▲ 09:30 equity $9,414.22 vs yday $9,414.22 (-0.00) | 09:30 open · cash $9,414.22 · no holdings · equity $9,414.22 vs prior close $9,414.22 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,414.22 | ▲ close $9,414.22 vs 09:30 $9,414.22 (session +0.00) | 16:00 close · cash $9,414.22 · no lots left · equity $9,414.22. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,414.22 | ▲ 09:30 equity $9,414.22 vs yday $9,414.22 (-0.00) | 09:30 open · cash $9,414.22 · no holdings · equity $9,414.22 vs prior close $9,414.22 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,414.22 | ▲ close $9,414.22 vs 09:30 $9,414.22 (session +0.00) | 16:00 close · cash $9,414.22 · no lots left · equity $9,414.22. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,414.22 | ▲ 09:30 equity $9,414.22 vs yday $9,414.22 (-0.00) | 09:30 open · cash $9,414.22 · no holdings · equity $9,414.22 vs prior close $9,414.22 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 232 | $10.11 | $2.99 | — | $7,065.70 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; ⚪; ret5=+5.1; leftover $2353.55 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,065.70 | ▲ close $9,413.54 vs 09:30 $9,414.22 (session +2.32) | 16:00 close · cash $7,065.70 · equity $9,413.54 vs 09:30 $9,414.22 (-0.68; session marks +2.32) · 1 name(s) marked open→close (per-name table). PAGS×232 09:30 $10.11 → close $10.12 +2.32 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-09-11 | `ORCL` | no_price | no 09:30 open |
| 2026-09-11 | `BAND` | no_price | no 09:30 open |
| 2026-09-11 | `ZSQR` | no_price | no 09:30 open |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `PAGS` | 232 | 2026-09-11 @ $10.11 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; ⚪; ret5=+5.1; leftover $2353.55 |
