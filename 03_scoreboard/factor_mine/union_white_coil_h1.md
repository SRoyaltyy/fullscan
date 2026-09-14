# Factor mine action — `union_white_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-5.70%** ($9,430) · signal-only (no cash/fees) was -1.83%. Starts YES **4/22**. Fills 132 · skips 0 · realized $-569.75.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,430.24.

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
| 2026-08-17 | `TMC` | 354 | — | $4.05 | +0.00 | $3.77 | -99.12 | -99.12 | +0.00 | -99.12 |
| 2026-08-17 | `TGB` | 169 | — | $8.46 | +0.00 | $8.77 | +52.39 | +52.39 | +0.00 | +52.39 |
| 2026-08-17 | `CDNL` | 36 | — | $39.85 | +0.00 | $39.23 | -22.32 | -22.32 | +0.00 | -22.32 |
| 2026-08-17 | `ABX` | 157 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `OCC` | 78 | — | $18.24 | +0.00 | $17.12 | -87.36 | -87.36 | +0.00 | -87.36 |
| 2026-08-17 | `ALM` | 88 | — | $16.20 | +0.00 | $16.36 | +14.08 | +14.08 | +0.00 | +14.08 |
| 2026-08-17 | `MRLN` | 383 | — | $3.75 | +0.00 | $3.54 | -82.34 | -82.34 | +0.00 | -82.34 |
| 2026-08-18 | `TMC` | 354 | $3.77 | $3.72 | -17.70 | — | +0.00 | -17.70 | -116.82 | — |
| 2026-08-18 | `TGB` | 169 | $8.77 | $8.55 | -37.18 | — | +0.00 | -37.18 | +15.21 | — |
| 2026-08-18 | `CDNL` | 36 | $39.23 | $41.57 | +84.24 | — | +0.00 | +84.24 | +61.92 | — |
| 2026-08-18 | `ABX` | 157 | $9.12 | $9.03 | -14.13 | — | +0.00 | -14.13 | -14.13 | — |
| 2026-08-18 | `OCC` | 78 | $17.12 | $16.20 | -71.76 | — | +0.00 | -71.76 | -159.12 | — |
| 2026-08-18 | `ALM` | 88 | $16.36 | $15.78 | -51.04 | — | +0.00 | -51.04 | -36.96 | — |
| 2026-08-18 | `MRLN` | 383 | $3.54 | $3.50 | -13.41 | — | +0.00 | -13.41 | -95.75 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 58 | — | $20.55 | +0.00 | $21.19 | +37.12 | +37.12 | +0.00 | +37.12 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `HDSN` | 209 | — | $5.77 | +0.00 | $5.57 | -41.80 | -41.80 | +0.00 | -41.80 |
| 2026-08-20 | `IAG` | 61 | — | $19.63 | +0.00 | $20.50 | +53.07 | +53.07 | +0.00 | +53.07 |
| 2026-08-20 | `KGC` | 40 | — | $29.63 | +0.00 | $31.43 | +72.00 | +72.00 | +0.00 | +72.00 |
| 2026-08-20 | `NFGC` | 690 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `MRVI` | 162 | — | $7.44 | +0.00 | $8.29 | +137.70 | +137.70 | +0.00 | +137.70 |
| 2026-08-20 | `SCZM` | 127 | — | $9.46 | +0.00 | $9.76 | +38.10 | +38.10 | +0.00 | +38.10 |
| 2026-08-21 | `AG` | 58 | $21.19 | $21.90 | +41.18 | — | +0.00 | +41.18 | +78.30 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `HDSN` | 209 | $5.57 | $5.67 | +20.90 | — | +0.00 | +20.90 | -20.90 | — |
| 2026-08-21 | `IAG` | 61 | $20.50 | $21.17 | +40.87 | — | +0.00 | +40.87 | +93.94 | — |
| 2026-08-21 | `KGC` | 40 | $31.43 | $32.17 | +29.60 | — | +0.00 | +29.60 | +101.60 | — |
| 2026-08-21 | `NFGC` | 690 | $1.75 | $1.79 | +27.60 | — | +0.00 | +27.60 | +27.60 | — |
| 2026-08-21 | `MRVI` | 162 | $8.29 | $8.28 | -1.62 | — | +0.00 | -1.62 | +136.08 | — |
| 2026-08-21 | `SCZM` | 127 | $9.76 | $10.26 | +63.50 | — | +0.00 | +63.50 | +101.60 | — |
| 2026-08-21 | `CRSP` | 28 | — | $59.72 | +0.00 | $59.50 | -6.16 | -6.16 | +0.00 | -6.16 |
| 2026-08-21 | `CF` | 13 | — | $127.43 | +0.00 | $129.60 | +28.21 | +28.21 | +0.00 | +28.21 |
| 2026-08-21 | `EMBC` | 312 | — | $5.43 | +0.00 | $5.23 | -62.40 | -62.40 | +0.00 | -62.40 |
| 2026-08-21 | `TXG` | 26 | — | $64.39 | +0.00 | $65.12 | +18.98 | +18.98 | +0.00 | +18.98 |
| 2026-08-21 | `BEKE` | 94 | — | $17.93 | +0.00 | $17.75 | -17.39 | -17.39 | +0.00 | -17.39 |
| 2026-08-21 | `HITI` | 699 | — | $2.43 | +0.00 | $2.45 | +13.98 | +13.98 | +0.00 | +13.98 |
| 2026-08-24 | `CRSP` | 28 | $59.50 | $58.75 | -21.00 | — | +0.00 | -21.00 | -27.16 | — |
| 2026-08-24 | `CF` | 13 | $129.60 | $129.99 | +5.07 | — | +0.00 | +5.07 | +33.28 | — |
| 2026-08-24 | `EMBC` | 312 | $5.23 | $5.20 | -10.92 | — | +0.00 | -10.92 | -73.32 | — |
| 2026-08-24 | `TXG` | 26 | $65.12 | $63.15 | -51.22 | — | +0.00 | -51.22 | -32.24 | — |
| 2026-08-24 | `BEKE` | 94 | $17.75 | $18.05 | +28.67 | — | +0.00 | +28.67 | +11.28 | — |
| 2026-08-24 | `HITI` | 699 | $2.45 | $2.45 | +0.00 | — | +0.00 | +0.00 | +13.98 | — |
| 2026-08-25 | `CRMD` | 150 | — | $8.35 | +0.00 | $8.56 | +31.50 | +31.50 | +0.00 | +31.50 |
| 2026-08-25 | `ELMT` | 70 | — | $17.89 | +0.00 | $17.75 | -9.80 | -9.80 | +0.00 | -9.80 |
| 2026-08-25 | `AMTX` | 663 | — | $1.90 | +0.00 | $1.91 | +6.63 | +6.63 | +0.00 | +6.63 |
| 2026-08-25 | `BZ` | 82 | — | $15.28 | +0.00 | $16.29 | +82.82 | +82.82 | +0.00 | +82.82 |
| 2026-08-25 | `VIPS` | 90 | — | $13.96 | +0.00 | $14.16 | +18.00 | +18.00 | +0.00 | +18.00 |
| 2026-08-25 | `RHI` | 28 | — | $43.76 | +0.00 | $44.90 | +31.92 | +31.92 | +0.00 | +31.92 |
| 2026-08-25 | `VALE` | 83 | — | $15.01 | +0.00 | $15.33 | +26.56 | +26.56 | +0.00 | +26.56 |
| 2026-08-25 | `AYA` | 48 | — | $26.21 | +0.00 | $27.71 | +72.00 | +72.00 | +0.00 | +72.00 |
| 2026-08-26 | `CRMD` | 150 | $8.56 | $8.60 | +6.00 | — | +0.00 | +6.00 | +37.50 | — |
| 2026-08-26 | `ELMT` | 70 | $17.75 | $17.82 | +4.90 | — | +0.00 | +4.90 | -4.90 | — |
| 2026-08-26 | `AMTX` | 663 | $1.91 | $1.91 | +0.00 | — | +0.00 | +0.00 | +6.63 | — |
| 2026-08-26 | `BZ` | 82 | $16.29 | $16.77 | +39.36 | — | +0.00 | +39.36 | +122.18 | — |
| 2026-08-26 | `VIPS` | 90 | $14.16 | $14.00 | -14.40 | — | +0.00 | -14.40 | +3.60 | — |
| 2026-08-26 | `RHI` | 28 | $44.90 | $44.33 | -15.96 | — | +0.00 | -15.96 | +15.96 | — |
| 2026-08-26 | `VALE` | 83 | $15.33 | $15.37 | +3.32 | — | +0.00 | +3.32 | +29.88 | — |
| 2026-08-26 | `AYA` | 48 | $27.71 | $27.24 | -22.56 | — | +0.00 | -22.56 | +49.44 | — |
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
| 2026-09-03 | `CABA` | 344 | — | $3.63 | +0.00 | $3.48 | -51.60 | -51.60 | +0.00 | -51.60 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `ARCT` | 74 | — | $16.77 | +0.00 | $15.56 | -89.54 | -89.54 | +0.00 | -89.54 |
| 2026-09-03 | `CRDL` | 573 | — | $2.18 | +0.00 | $2.16 | -11.46 | -11.46 | +0.00 | -11.46 |
| 2026-09-03 | `SDGR` | 59 | — | $21.03 | +0.00 | $20.71 | -18.88 | -18.88 | +0.00 | -18.88 |
| 2026-09-03 | `VIR` | 108 | — | $11.54 | +0.00 | $11.45 | -9.72 | -9.72 | +0.00 | -9.72 |
| 2026-09-04 | `ATRC` | 23 | $52.46 | $52.03 | -9.89 | $51.52 | -11.73 | -21.62 | -19.55 | -31.28 |
| 2026-09-04 | `HRMY` | 29 | $41.86 | $41.50 | -10.44 | $42.25 | +21.75 | +11.31 | -41.47 | -19.72 |
| 2026-09-04 | `CABA` | 344 | $3.48 | $3.46 | -6.88 | $3.47 | +3.44 | -3.44 | -58.48 | -55.04 |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `ARCT` | 74 | $15.56 | $15.61 | +3.70 | — | +0.00 | +3.70 | -85.84 | — |
| 2026-09-04 | `CRDL` | 573 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.46 | — |
| 2026-09-04 | `SDGR` | 59 | $20.71 | $20.58 | -7.67 | — | +0.00 | -7.67 | -26.55 | — |
| 2026-09-04 | `VIR` | 108 | $11.45 | $11.31 | -15.12 | $11.38 | +8.10 | -7.02 | -24.84 | -16.74 |
| 2026-09-04 | `ALEC` | 481 | — | $2.52 | +0.00 | $2.46 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-09-04 | `BHC` | 180 | — | $6.71 | +0.00 | $6.56 | -27.00 | -27.00 | +0.00 | -27.00 |
| 2026-09-04 | `OABI` | 254 | — | $4.78 | +0.00 | $4.33 | -114.30 | -114.30 | +0.00 | -114.30 |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-08 | `ATRC` | 23 | $51.52 | $54.31 | +64.17 | — | +0.00 | +64.17 | +32.89 | — |
| 2026-09-08 | `HRMY` | 29 | $42.25 | $42.20 | -1.45 | — | +0.00 | -1.45 | -21.17 | — |
| 2026-09-08 | `CABA` | 344 | $3.47 | $3.43 | -13.76 | — | +0.00 | -13.76 | -68.80 | — |
| 2026-09-08 | `VIR` | 108 | $11.38 | $11.22 | -17.82 | — | +0.00 | -17.82 | -34.56 | — |
| 2026-09-08 | `ALEC` | 481 | $2.46 | $2.38 | -38.48 | — | +0.00 | -38.48 | -67.34 | — |
| 2026-09-08 | `BHC` | 180 | $6.56 | $6.57 | +1.80 | — | +0.00 | +1.80 | -25.20 | — |
| 2026-09-08 | `OABI` | 254 | $4.33 | $4.30 | -7.62 | — | +0.00 | -7.62 | -121.92 | — |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `BAND` | 59 | — | $52.55 | +0.00 | $56.87 | +254.88 | +254.88 | +0.00 | +254.88 |
| 2026-09-11 | `PAGS` | 310 | — | $10.11 | +0.00 | $10.12 | +3.10 | +3.10 | +0.00 | +3.10 |
| 2026-09-11 | `ZSQR` | 967 | — | $3.25 | +0.00 | $3.07 | -174.06 | -174.06 | +0.00 | -174.06 |
| 2026-09-14 | `BAND` | 59 | $56.87 | $56.90 | +1.77 | — | +0.00 | +1.77 | +256.65 | — |
| 2026-09-14 | `PAGS` | 310 | $10.12 | $10.00 | -37.20 | — | +0.00 | -37.20 | -34.10 | — |
| 2026-09-14 | `ZSQR` | 967 | $3.07 | $3.06 | -9.67 | — | +0.00 | -9.67 | -183.73 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +216.83 | BTSG, TPG, TGTX, SLS, HIMS, VOR | — | $134.73 | $10,203.79 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75 |
| 2026-08-14 | +5.50 | $134.73 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75 | $10,217.42 | +13.63 | -101.53 | DAVE, SLG, MARA, LDI, BTBT, BETR, ANGX, HYLN | BTSG, TPG, TGTX, SLS, HIMS, VOR | $260.01 | $10,058.44 | DAVE×3, SLG×22, MARA×141, LDI×1361, BTBT×850, BETR×86, ANGX×295, HYLN×305 |
| 2026-08-17 | +2.25 | $260.01 | DAVE×3, SLG×22, MARA×141, LDI×1361, BTBT×850, BETR×86, ANGX×295, HYLN×305 | $10,098.53 | +40.09 | -224.67 | TMC, TGB, CDNL, ABX, OCC, ALM, MRLN | DAVE, SLG, MARA, LDI, BTBT, BETR, ANGX, HYLN | $18.59 | $9,808.36 | TMC×354, TGB×169, CDNL×36, ABX×157, OCC×78, ALM×88, MRLN×383 |
| 2026-08-18 | -6.20 | $18.59 | TMC×354, TGB×169, CDNL×36, ABX×157, OCC×78, ALM×88, MRLN×383 | $9,687.39 | -120.97 | +0.00 | — | TMC, TGB, CDNL, ABX, OCC, ALM, MRLN | $9,666.06 | $9,666.06 | — |
| 2026-08-19 | -7.20 | $9,666.06 | — | $9,666.06 | -0.00 | +0.00 | — | — | $9,666.06 | $9,666.06 | — |
| 2026-08-20 | +1.12 | $9,666.06 | — | $9,666.06 | -0.00 | +330.25 | AG, BHP, HDSN, IAG, KGC, NFGC, MRVI, SCZM | — | $63.35 | $9,971.39 | AG×58, BHP×13, HDSN×209, IAG×61, KGC×40, NFGC×690, MRVI×162, SCZM×127 |
| 2026-08-21 | +3.25 | $63.35 | AG×58, BHP×13, HDSN×209, IAG×61, KGC×40, NFGC×690, MRVI×162, SCZM×127 | $10,220.59 | +249.20 | -24.78 | CRSP, CF, EMBC, TXG, BEKE, HITI | AG, BHP, HDSN, IAG, KGC, NFGC, MRVI, SCZM | $92.35 | $10,149.08 | CRSP×28, CF×13, EMBC×312, TXG×26, BEKE×94, HITI×699 |
| 2026-08-24 | -5.17 | $92.35 | CRSP×28, CF×13, EMBC×312, TXG×26, BEKE×94, HITI×699 | $10,099.68 | -49.40 | +0.00 | — | CRSP, CF, EMBC, TXG, BEKE, HITI | $10,077.90 | $10,077.90 | — |
| 2026-08-25 | +1.80 | $10,077.90 | — | $10,077.90 | +0.00 | +259.63 | CRMD, ELMT, AMTX, BZ, VIPS, RHI, VALE, AYA | — | $50.72 | $10,313.40 | CRMD×150, ELMT×70, AMTX×663, BZ×82, VIPS×90, RHI×28, VALE×83, AYA×48 |
| 2026-08-26 | +2.02 | $50.72 | CRMD×150, ELMT×70, AMTX×663, BZ×82, VIPS×90, RHI×28, VALE×83, AYA×48 | $10,314.06 | +0.66 | +0.00 | — | CRMD, ELMT, AMTX, BZ, VIPS, RHI, VALE, AYA | $10,289.63 | $10,289.63 | — |
| 2026-08-27 | — | $10,289.63 | — | $10,289.63 | +0.00 | +0.00 | — | — | $10,289.63 | $10,289.63 | — |
| 2026-08-28 | +0.75 | $10,289.63 | — | $10,289.63 | +0.00 | -307.04 | TTMI, KEYS, AVT, CGNX, COHR, LSCC, MTSI, OLED | — | $803.42 | $9,966.43 | TTMI×10, KEYS×3, AVT×14, CGNX×20, COHR×4, LSCC×10, MTSI×4, OLED×15 |
| 2026-08-31 | -5.85 | $803.42 | TTMI×10, KEYS×3, AVT×14, CGNX×20, COHR×4, LSCC×10, MTSI×4, OLED×15 | $10,013.49 | +47.06 | +0.00 | — | TTMI, KEYS, AVT, CGNX, COHR, LSCC, MTSI, OLED | $9,997.17 | $9,997.17 | — |
| 2026-09-01 | -6.30 | $9,997.17 | — | $9,997.17 | +0.00 | +0.00 | — | — | $9,997.17 | $9,997.17 | — |
| 2026-09-02 | -3.83 | $9,997.17 | — | $9,997.17 | +0.00 | +0.00 | — | — | $9,997.17 | $9,997.17 | — |
| 2026-09-03 | -0.90 | $9,997.17 | — | $9,997.17 | +0.00 | -238.27 | ATRC, HRMY, CABA, RVTY, ARCT, CRDL, SDGR, VIR | — | $93.31 | $9,734.23 | ATRC×23, HRMY×29, CABA×344, RVTY×9, ARCT×74, CRDL×573, SDGR×59, VIR×108 |
| 2026-09-04 | +2.25 | $93.31 | ATRC×23, HRMY×29, CABA×344, RVTY×9, ARCT×74, CRDL×573, SDGR×59, VIR×108 | $9,682.53 | -51.70 | -165.12 | ALEC, BHC, OABI, CRM | RVTY, ARCT, CRDL, SDGR | $155.17 | $9,489.44 | ATRC×23, HRMY×29, CABA×344, VIR×108, ALEC×481, BHC×180, OABI×254, CRM×4 |
| 2026-09-08 | -11.47 | $155.17 | ATRC×23, HRMY×29, CABA×344, VIR×108, ALEC×481, BHC×180, OABI×254, CRM×4 | $9,454.24 | -35.20 | +0.00 | — | ATRC, HRMY, CABA, VIR, ALEC, BHC, OABI, CRM | $9,429.00 | $9,429.00 | — |
| 2026-09-09 | -13.95 | $9,429.00 | — | $9,429.00 | -0.00 | +0.00 | — | — | $9,429.00 | $9,429.00 | — |
| 2026-09-10 | -13.28 | $9,429.00 | — | $9,429.00 | -0.00 | +0.00 | — | — | $9,429.00 | $9,429.00 | — |
| 2026-09-11 | +0.50 | $9,429.00 | — | $9,429.00 | -0.00 | +83.92 | BAND, PAGS, ZSQR | — | $33.06 | $9,494.28 | BAND×59, PAGS×310, ZSQR×967 |
| 2026-09-14 | -11.00 | $33.06 | BAND×59, PAGS×310, ZSQR×967 | $9,449.18 | -45.10 | +0.00 | — | BAND, PAGS, ZSQR | $9,430.24 | $9,430.24 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 27 | $59.80 | $2.07 | — | $8,383.33 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟡 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $6,761.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 33 | $49.70 | $2.09 | — | $5,119.11 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟡 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 142 | $11.70 | $2.42 | — | $3,455.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟡 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 56 | $29.74 | $2.16 | — | $1,787.70 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟡 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 75 | $22.01 | $2.21 | — | $134.73 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟡 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.73 | ▲ close $10,203.79 vs 09:30 $10,000.00 (session +216.83) | 16:00 close · cash $134.73 · equity $10,203.79 vs 09:30 $10,000.00 (+203.79; session marks +216.83) · 6 name(s) marked open→close (per-name table). BTSG×27 09:30 $59.80 → close $60.23 +11.61; TPG×32 09:30 $50.62 → close $54.62 +127.90; TGTX×33 09:30 $49.70 → close $47.94 -58.08; SLS×142 09:30 $11.70 → close $12.36 +93.72; HIMS×56 09:30 $29.74 → close $28.77 -54.32; VOR×75 09:30 $22.01 → close $23.29 +96.00 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.73 | ▲ 09:30 equity $10,217.42 vs yday $10,203.79 (+13.63) | 09:30 open · cash $134.73 (unchanged overnight, no fees) · equity $10,217.42 vs prior close $10,203.79 (+13.63) · 6 name(s) re-marked at the open (per-name table). BTSG×27 yday $60.23 → 09:30 $59.65 -15.66; TPG×32 yday $54.62 → 09:30 $55.29 +21.44; TGTX×33 yday $47.94 → 09:30 $47.27 -22.11; SLS×142 yday $12.36 → 09:30 $12.40 +5.68; HIMS×56 yday $28.77 → 09:30 $29.15 +21.28; VOR×75 yday $23.29 → 09:30 $23.33 +3.00 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 27 | $59.65 | $2.09 | $-8.21 | $1,743.19 | ▼ -8.21 after sell → book $10,215.33; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $3,510.36 | ▲ +145.14 after sell → book $10,213.22; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 33 | $47.27 | $2.11 | $-84.39 | $5,068.16 | ▼ -84.39 after sell → book $10,211.11; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 142 | $12.40 | $2.45 | $+94.53 | $6,826.50 | ▲ +94.53 after sell → book $10,208.65; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 56 | $29.15 | $2.18 | $-37.38 | $8,456.72 | ▼ -37.38 after sell → book $10,206.47; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 75 | $23.33 | $2.24 | $+94.54 | $10,204.23 | ▲ +94.54 after sell → book $10,204.23; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $9,209.50 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $7,940.03 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 141 | $9.01 | $2.41 | — | $6,667.20 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1361 | $0.94 | $16.84 | — | $5,375.11 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 850 | $1.50 | $10.96 | — | $4,089.15 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 86 | $14.80 | $2.25 | — | $2,814.10 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 295 | $4.31 | $3.81 | — | $1,538.84 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 305 | $4.18 | $3.93 | — | $260.01 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
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
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 354 | $4.05 | $4.57 | — | $8,615.81 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1436.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 169 | $8.46 | $2.50 | — | $7,183.58 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1436.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 36 | $39.85 | $2.10 | — | $5,746.88 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1436.30 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 157 | $9.12 | $2.46 | — | $4,312.58 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1436.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 78 | $18.24 | $2.22 | — | $2,887.63 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1436.30 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 88 | $16.20 | $2.25 | — | $1,459.78 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1436.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `MRLN` | 383 | $3.75 | $4.94 | — | $18.59 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-15.4; leftover $1436.30 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.59 | ▼ close $9,808.36 vs 09:30 $10,098.53 (session -224.67) | 16:00 close · cash $18.59 · equity $9,808.36 vs 09:30 $10,098.53 (-290.17; session marks -224.67) · 7 name(s) marked open→close (per-name table). TMC×354 09:30 $4.05 → close $3.77 -99.12; TGB×169 09:30 $8.46 → close $8.77 +52.39; CDNL×36 09:30 $39.85 → close $39.23 -22.32; ABX×157 09:30 $9.12 → close $9.12 +0.00; OCC×78 09:30 $18.24 → close $17.12 -87.36; ALM×88 09:30 $16.20 → close $16.36 +14.08; MRLN×383 09:30 $3.75 → close $3.54 -82.34 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.59 | ▼ 09:30 equity $9,687.39 vs yday $9,808.36 (-120.97) | 09:30 open · cash $18.59 (unchanged overnight, no fees) · equity $9,687.39 vs prior close $9,808.36 (-120.97) · 7 name(s) re-marked at the open (per-name table). TMC×354 yday $3.77 → 09:30 $3.72 -17.70; TGB×169 yday $8.77 → 09:30 $8.55 -37.18; CDNL×36 yday $39.23 → 09:30 $41.57 +84.24; ABX×157 yday $9.12 → 09:30 $9.03 -14.13; OCC×78 yday $17.12 → 09:30 $16.20 -71.76; ALM×88 yday $16.36 → 09:30 $15.78 -51.04; MRLN×383 yday $3.54 → 09:30 $3.50 -13.41 | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 354 | $3.72 | $4.64 | $-126.02 | $1,330.83 | ▼ -126.02 after sell → book $9,682.75; vs 09:30 mark -4.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 169 | $8.55 | $2.54 | $+10.18 | $2,773.25 | ▲ +10.18 after sell → book $9,680.22; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 36 | $41.57 | $2.12 | $+57.70 | $4,267.65 | ▲ +57.70 after sell → book $9,678.10; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 157 | $9.03 | $2.50 | $-19.09 | $5,682.86 | ▼ -19.09 after sell → book $9,675.60; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 78 | $16.20 | $2.25 | $-163.59 | $6,944.21 | ▼ -163.59 after sell → book $9,673.35; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 88 | $15.78 | $2.28 | $-41.49 | $8,330.57 | ▼ -41.49 after sell → book $9,671.07; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `MRLN` | 383 | $3.50 | $5.01 | $-105.71 | $9,666.06 | ▼ -105.71 after sell → book $9,666.06; vs 09:30 mark -5.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,666.06 | ▲ close $9,666.06 vs 09:30 $9,687.39 (session +0.00) | 16:00 close · cash $9,666.06 · no lots left · equity $9,666.06. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,666.06 | ▲ 09:30 equity $9,666.06 vs yday $9,666.06 (-0.00) | 09:30 open · cash $9,666.06 · no holdings · equity $9,666.06 vs prior close $9,666.06 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,666.06 | ▲ close $9,666.06 vs 09:30 $9,666.06 (session +0.00) | 16:00 close · cash $9,666.06 · no lots left · equity $9,666.06. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,666.06 | ▲ 09:30 equity $9,666.06 vs yday $9,666.06 (-0.00) | 09:30 open · cash $9,666.06 · no holdings · equity $9,666.06 vs prior close $9,666.06 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,471.99 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1208.26 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,286.83 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1208.26 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 209 | $5.77 | $2.70 | — | $6,078.21 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1208.26 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $4,878.60 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1208.26 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $3,691.29 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1208.26 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 690 | $1.75 | $8.90 | — | $2,474.89 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1208.26 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 162 | $7.44 | $2.48 | — | $1,267.14 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1208.26 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 127 | $9.46 | $2.37 | — | $63.35 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1208.26 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.35 | ▲ close $9,971.39 vs 09:30 $9,666.06 (session +330.25) | 16:00 close · cash $63.35 · equity $9,971.39 vs 09:30 $9,666.06 (+305.33; session marks +330.25) · 8 name(s) marked open→close (per-name table). AG×58 09:30 $20.55 → close $21.19 +37.12; BHP×13 09:30 $91.01 → close $93.63 +34.06; HDSN×209 09:30 $5.77 → close $5.57 -41.80; IAG×61 09:30 $19.63 → close $20.50 +53.07; KGC×40 09:30 $29.63 → close $31.43 +72.00; NFGC×690 09:30 $1.75 → close $1.75 +0.00; MRVI×162 09:30 $7.44 → close $8.29 +137.70; SCZM×127 09:30 $9.46 → close $9.76 +38.10 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.35 | ▲ 09:30 equity $10,220.59 vs yday $9,971.39 (+249.20) | 09:30 open · cash $63.35 (unchanged overnight, no fees) · equity $10,220.59 vs prior close $9,971.39 (+249.20) · 8 name(s) re-marked at the open (per-name table). AG×58 yday $21.19 → 09:30 $21.90 +41.18; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; HDSN×209 yday $5.57 → 09:30 $5.67 +20.90; IAG×61 yday $20.50 → 09:30 $21.17 +40.87; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×690 yday $1.75 → 09:30 $1.79 +27.60; MRVI×162 yday $8.29 → 09:30 $8.28 -1.62; SCZM×127 yday $9.76 → 09:30 $10.26 +63.50 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,331.36 | ▲ +73.95 after sell → book $10,218.40; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,573.67 | ▲ +57.15 after sell → book $10,216.35; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 209 | $5.67 | $2.74 | $-26.34 | $3,755.96 | ▼ -26.34 after sell → book $10,213.61; vs 09:30 mark -2.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $5,045.14 | ▲ +89.57 after sell → book $10,211.42; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $6,329.81 | ▲ +97.36 after sell → book $10,209.29; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 690 | $1.79 | $9.03 | $+9.67 | $7,555.88 | ▲ +9.67 after sell → book $10,200.26; vs 09:30 mark -9.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 162 | $8.28 | $2.51 | $+131.09 | $8,894.73 | ▲ +131.09 after sell → book $10,197.75; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 127 | $10.26 | $2.40 | $+96.83 | $10,195.35 | ▲ +96.83 after sell → book $10,195.35; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 28 | $59.72 | $2.07 | — | $8,521.11 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1699.22 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 13 | $127.43 | $2.03 | — | $6,862.49 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1699.22 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 312 | $5.43 | $4.02 | — | $5,164.31 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1699.22 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 26 | $64.39 | $2.07 | — | $3,488.10 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1699.22 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 94 | $17.93 | $2.27 | — | $1,799.94 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $1699.22 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 699 | $2.43 | $9.02 | — | $92.35 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $1699.22 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.35 | ▼ close $10,149.08 vs 09:30 $10,220.59 (session -24.78) | 16:00 close · cash $92.35 · equity $10,149.08 vs 09:30 $10,220.59 (-71.51; session marks -24.78) · 6 name(s) marked open→close (per-name table). CRSP×28 09:30 $59.72 → close $59.50 -6.16; CF×13 09:30 $127.43 → close $129.60 +28.21; EMBC×312 09:30 $5.43 → close $5.23 -62.40; TXG×26 09:30 $64.39 → close $65.12 +18.98; BEKE×94 09:30 $17.93 → close $17.75 -17.39; HITI×699 09:30 $2.43 → close $2.45 +13.98 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.35 | ▼ 09:30 equity $10,099.68 vs yday $10,149.08 (-49.40) | 09:30 open · cash $92.35 (unchanged overnight, no fees) · equity $10,099.68 vs prior close $10,149.08 (-49.40) · 6 name(s) re-marked at the open (per-name table). CRSP×28 yday $59.50 → 09:30 $58.75 -21.00; CF×13 yday $129.60 → 09:30 $129.99 +5.07; EMBC×312 yday $5.23 → 09:30 $5.20 -10.92; TXG×26 yday $65.12 → 09:30 $63.15 -51.22; BEKE×94 yday $17.75 → 09:30 $18.05 +28.67; HITI×699 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 28 | $58.75 | $2.10 | $-31.33 | $1,735.25 | ▼ -31.33 after sell → book $10,097.58; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 13 | $129.99 | $2.05 | $+29.20 | $3,423.07 | ▲ +29.20 after sell → book $10,095.53; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 312 | $5.20 | $4.09 | $-81.43 | $5,039.82 | ▼ -81.43 after sell → book $10,091.44; vs 09:30 mark -4.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 26 | $63.15 | $2.09 | $-36.40 | $6,679.63 | ▼ -36.40 after sell → book $10,089.35; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 94 | $18.05 | $2.30 | $+6.71 | $8,374.50 | ▲ +6.71 after sell → book $10,087.05; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 699 | $2.45 | $9.15 | $-4.18 | $10,077.90 | ▼ -4.18 after sell → book $10,077.90; vs 09:30 mark -9.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,077.90 | ▲ close $10,077.90 vs 09:30 $10,099.68 (session +0.00) | 16:00 close · cash $10,077.90 · no lots left · equity $10,077.90. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,077.90 | ▲ 09:30 equity $10,077.90 vs yday $10,077.90 (+0.00) | 09:30 open · cash $10,077.90 · no holdings · equity $10,077.90 vs prior close $10,077.90 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 150 | $8.35 | $2.44 | — | $8,822.96 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1259.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ELMT` | 70 | $17.89 | $2.20 | — | $7,568.46 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-7.5; leftover $1259.74 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 663 | $1.90 | $8.55 | — | $6,300.21 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=+5.0; leftover $1259.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 82 | $15.28 | $2.24 | — | $5,045.01 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $1259.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 90 | $13.96 | $2.26 | — | $3,786.35 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.0; leftover $1259.74 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 28 | $43.76 | $2.07 | — | $2,559.00 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1259.74 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VALE` | 83 | $15.01 | $2.24 | — | $1,310.93 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; ⚪; ret5=+9.4; leftover $1259.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AYA` | 48 | $26.21 | $2.13 | — | $50.72 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; ⚪; ret5=-0.3; leftover $1259.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.72 | ▲ close $10,313.40 vs 09:30 $10,077.90 (session +259.63) | 16:00 close · cash $50.72 · equity $10,313.40 vs 09:30 $10,077.90 (+235.50; session marks +259.63) · 8 name(s) marked open→close (per-name table). CRMD×150 09:30 $8.35 → close $8.56 +31.50; ELMT×70 09:30 $17.89 → close $17.75 -9.80; AMTX×663 09:30 $1.90 → close $1.91 +6.63; BZ×82 09:30 $15.28 → close $16.29 +82.82; VIPS×90 09:30 $13.96 → close $14.16 +18.00; RHI×28 09:30 $43.76 → close $44.90 +31.92; VALE×83 09:30 $15.01 → close $15.33 +26.56; AYA×48 09:30 $26.21 → close $27.71 +72.00 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.72 | ▲ 09:30 equity $10,314.06 vs yday $10,313.40 (+0.66) | 09:30 open · cash $50.72 (unchanged overnight, no fees) · equity $10,314.06 vs prior close $10,313.40 (+0.66) · 8 name(s) re-marked at the open (per-name table). CRMD×150 yday $8.56 → 09:30 $8.60 +6.00; ELMT×70 yday $17.75 → 09:30 $17.82 +4.90; AMTX×663 yday $1.91 → 09:30 $1.91 +0.00; BZ×82 yday $16.29 → 09:30 $16.77 +39.36; VIPS×90 yday $14.16 → 09:30 $14.00 -14.40; RHI×28 yday $44.90 → 09:30 $44.33 -15.96; VALE×83 yday $15.33 → 09:30 $15.37 +3.32; AYA×48 yday $27.71 → 09:30 $27.24 -22.56 | — |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 150 | $8.60 | $2.48 | $+32.58 | $1,338.24 | ▲ +32.58 after sell → book $10,311.58; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `ELMT` | 70 | $17.82 | $2.22 | $-9.32 | $2,583.42 | ▼ -9.32 after sell → book $10,309.36; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AMTX` | 663 | $1.91 | $8.67 | $-10.60 | $3,841.08 | ▼ -10.60 after sell → book $10,300.69; vs 09:30 mark -8.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BZ` | 82 | $16.77 | $2.26 | $+117.68 | $5,213.96 | ▲ +117.68 after sell → book $10,298.43; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `VIPS` | 90 | $14.00 | $2.29 | $-0.95 | $6,471.67 | ▼ -0.95 after sell → book $10,296.14; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 28 | $44.33 | $2.09 | $+11.79 | $7,710.82 | ▲ +11.79 after sell → book $10,294.05; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VALE` | 83 | $15.37 | $2.26 | $+25.38 | $8,984.27 | ▲ +25.38 after sell → book $10,291.79; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AYA` | 48 | $27.24 | $2.15 | $+45.15 | $10,289.63 | ▲ +45.15 after sell → book $10,289.63; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,289.63 | ▲ close $10,289.63 vs 09:30 $10,314.06 (session +0.00) | 16:00 close · cash $10,289.63 · no lots left · equity $10,289.63. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,289.63 | ▲ 09:30 equity $10,289.63 vs yday $10,289.63 (+0.00) | 09:30 open · cash $10,289.63 · no holdings · equity $10,289.63 vs prior close $10,289.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,289.63 | ▲ close $10,289.63 vs 09:30 $10,289.63 (session +0.00) | 16:00 close · cash $10,289.63 · no lots left · equity $10,289.63. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,289.63 | ▲ 09:30 equity $10,289.63 vs yday $10,289.63 (+0.00) | 09:30 open · cash $10,289.63 · no holdings · equity $10,289.63 vs prior close $10,289.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $9,059.51 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1286.20 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $8,084.28 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1286.20 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $6,801.39 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1286.20 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.82 | $2.05 | — | $5,542.94 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1286.20 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $4,383.18 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1286.20 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $119.76 | $2.02 | — | $3,183.56 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1286.20 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MTSI` | 4 | $275.20 | $2.00 | — | $2,080.76 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+4.1; leftover $1286.20 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OLED` | 15 | $85.02 | $2.04 | — | $803.42 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-1.9; leftover $1286.20 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $803.42 | ▼ close $9,966.43 vs 09:30 $10,289.63 (session -307.04) | 16:00 close · cash $803.42 · equity $9,966.43 vs 09:30 $10,289.63 (-323.20; session marks -307.04) · 8 name(s) marked open→close (per-name table). TTMI×10 09:30 $122.81 → close $118.65 -41.60; KEYS×3 09:30 $324.41 → close $319.97 -13.32; AVT×14 09:30 $91.49 → close $88.63 -40.04; CGNX×20 09:30 $62.82 → close $60.46 -47.20; COHR×4 09:30 $289.44 → close $279.20 -40.96; LSCC×10 09:30 $119.76 → close $114.40 -53.60; MTSI×4 09:30 $275.20 → close $265.27 -39.72; OLED×15 09:30 $85.02 → close $82.98 -30.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $803.42 | ▲ 09:30 equity $10,013.49 vs yday $9,966.43 (+47.06) | 09:30 open · cash $803.42 (unchanged overnight, no fees) · equity $10,013.49 vs prior close $9,966.43 (+47.06) · 8 name(s) re-marked at the open (per-name table). TTMI×10 yday $118.65 → 09:30 $118.83 +1.80; KEYS×3 yday $319.97 → 09:30 $322.49 +7.56; AVT×14 yday $88.63 → 09:30 $89.39 +10.64; CGNX×20 yday $60.46 → 09:30 $60.46 +0.00; COHR×4 yday $279.20 → 09:30 $280.25 +4.20; LSCC×10 yday $114.40 → 09:30 $115.56 +11.60; MTSI×4 yday $265.27 → 09:30 $266.96 +6.76; OLED×15 yday $82.98 → 09:30 $83.28 +4.50 | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $118.83 | $2.04 | $-43.86 | $1,989.68 | ▼ -43.86 after sell → book $10,011.45; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 3 | $322.49 | $2.02 | $-9.78 | $2,955.13 | ▼ -9.78 after sell → book $10,009.43; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $4,204.54 | ▼ -33.48 after sell → book $10,007.38; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 20 | $60.46 | $2.07 | $-51.32 | $5,411.67 | ▼ -51.32 after sell → book $10,005.31; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $6,530.65 | ▼ -40.78 after sell → book $10,003.29; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 10 | $115.56 | $2.04 | $-46.06 | $7,684.21 | ▼ -46.06 after sell → book $10,001.25; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MTSI` | 4 | $266.96 | $2.02 | $-36.98 | $8,750.03 | ▼ -36.98 after sell → book $9,999.23; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OLED` | 15 | $83.28 | $2.06 | $-30.19 | $9,997.17 | ▼ -30.19 after sell → book $9,997.17; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,997.17 | ▲ close $9,997.17 vs 09:30 $10,013.49 (session +0.00) | 16:00 close · cash $9,997.17 · no lots left · equity $9,997.17. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,997.17 | ▲ 09:30 equity $9,997.17 vs yday $9,997.17 (+0.00) | 09:30 open · cash $9,997.17 · no holdings · equity $9,997.17 vs prior close $9,997.17 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,997.17 | ▲ close $9,997.17 vs 09:30 $9,997.17 (session +0.00) | 16:00 close · cash $9,997.17 · no lots left · equity $9,997.17. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,997.17 | ▲ 09:30 equity $9,997.17 vs yday $9,997.17 (+0.00) | 09:30 open · cash $9,997.17 · no holdings · equity $9,997.17 vs prior close $9,997.17 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,997.17 | ▲ close $9,997.17 vs 09:30 $9,997.17 (session +0.00) | 16:00 close · cash $9,997.17 · no lots left · equity $9,997.17. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,997.17 | ▲ 09:30 equity $9,997.17 vs yday $9,997.17 (+0.00) | 09:30 open · cash $9,997.17 · no holdings · equity $9,997.17 vs prior close $9,997.17 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,778.87 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1249.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,531.82 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1249.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 344 | $3.63 | $4.44 | — | $6,278.67 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1249.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $5,084.60 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1249.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $3,841.41 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1249.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 573 | $2.18 | $7.39 | — | $2,584.88 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1249.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SDGR` | 59 | $21.03 | $2.17 | — | $1,341.94 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1249.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 108 | $11.54 | $2.31 | — | $93.31 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $1249.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.31 | ▼ close $9,734.23 vs 09:30 $9,997.17 (session -238.27) | 16:00 close · cash $93.31 · equity $9,734.23 vs 09:30 $9,997.17 (-262.94; session marks -238.27) · 8 name(s) marked open→close (per-name table). ATRC×23 09:30 $52.88 → close $52.46 -9.66; HRMY×29 09:30 $42.93 → close $41.86 -31.03; CABA×344 09:30 $3.63 → close $3.48 -51.60; RVTY×9 09:30 $132.45 → close $130.63 -16.38; ARCT×74 09:30 $16.77 → close $15.56 -89.54; CRDL×573 09:30 $2.18 → close $2.16 -11.46; SDGR×59 09:30 $21.03 → close $20.71 -18.88; VIR×108 09:30 $11.54 → close $11.45 -9.72 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.31 | ▼ 09:30 equity $9,682.53 vs yday $9,734.23 (-51.70) | 09:30 open · cash $93.31 (unchanged overnight, no fees) · equity $9,682.53 vs prior close $9,734.23 (-51.70) · 8 name(s) re-marked at the open (per-name table). ATRC×23 yday $52.46 → 09:30 $52.03 -9.89; HRMY×29 yday $41.86 → 09:30 $41.50 -10.44; CABA×344 yday $3.48 → 09:30 $3.46 -6.88; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; ARCT×74 yday $15.56 → 09:30 $15.61 +3.70; CRDL×573 yday $2.16 → 09:30 $2.16 +0.00; SDGR×59 yday $20.71 → 09:30 $20.58 -7.67; VIR×108 yday $11.45 → 09:30 $11.31 -15.12 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $1,261.54 | ▼ -25.83 after sell → book $9,680.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 74 | $15.61 | $2.23 | $-90.29 | $2,414.44 | ▼ -90.29 after sell → book $9,678.25; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 573 | $2.16 | $7.50 | $-26.35 | $3,644.63 | ▼ -26.35 after sell → book $9,670.76; vs 09:30 mark -7.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SDGR` | 59 | $20.58 | $2.19 | $-30.90 | $4,856.66 | ▼ -30.90 after sell → book $9,668.57; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 481 | $2.52 | $6.20 | — | $3,638.34 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1214.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 180 | $6.71 | $2.53 | — | $2,428.01 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1214.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 254 | $4.78 | $3.28 | — | $1,210.61 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1214.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $155.17 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1214.17 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $155.17 | ▼ close $9,489.44 vs 09:30 $9,682.53 (session -165.12) | 16:00 close · cash $155.17 · equity $9,489.44 vs 09:30 $9,682.53 (-193.09; session marks -165.12) · 8 name(s) marked open→close (per-name table). ATRC×23 09:30 $52.03 → close $51.52 -11.73; HRMY×29 09:30 $41.50 → close $42.25 +21.75; CABA×344 09:30 $3.46 → close $3.47 +3.44; VIR×108 09:30 $11.31 → close $11.38 +8.10; ALEC×481 09:30 $2.52 → close $2.46 -28.86; BHC×180 09:30 $6.71 → close $6.56 -27.00; OABI×254 09:30 $4.78 → close $4.33 -114.30; CRM×4 09:30 $263.36 → close $259.23 -16.52 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $155.17 | ▼ 09:30 equity $9,454.24 vs yday $9,489.44 (-35.20) | 09:30 open · cash $155.17 (unchanged overnight, no fees) · equity $9,454.24 vs prior close $9,489.44 (-35.20) · 8 name(s) re-marked at the open (per-name table). ATRC×23 yday $51.52 → 09:30 $54.31 +64.17; HRMY×29 yday $42.25 → 09:30 $42.20 -1.45; CABA×344 yday $3.47 → 09:30 $3.43 -13.76; VIR×108 yday $11.38 → 09:30 $11.22 -17.82; ALEC×481 yday $2.46 → 09:30 $2.38 -38.48; BHC×180 yday $6.56 → 09:30 $6.57 +1.80; OABI×254 yday $4.33 → 09:30 $4.30 -7.62; CRM×4 yday $259.23 → 09:30 $253.72 -22.04 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 23 | $54.31 | $2.08 | $+28.75 | $1,402.22 | ▲ +28.75 after sell → book $9,452.16; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 29 | $42.20 | $2.10 | $-25.34 | $2,623.92 | ▼ -25.34 after sell → book $9,450.06; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 344 | $3.43 | $4.50 | $-77.74 | $3,799.34 | ▼ -77.74 after sell → book $9,445.56; vs 09:30 mark -4.50 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 108 | $11.22 | $2.34 | $-39.22 | $5,008.75 | ▼ -39.22 after sell → book $9,443.21; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 481 | $2.38 | $6.29 | $-79.84 | $6,147.24 | ▼ -79.84 after sell → book $9,436.92; vs 09:30 mark -6.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 180 | $6.57 | $2.57 | $-30.30 | $7,327.27 | ▼ -30.30 after sell → book $9,434.35; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 254 | $4.30 | $3.33 | $-128.53 | $8,416.14 | ▼ -128.53 after sell → book $9,431.02; vs 09:30 mark -3.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $9,429.00 | ▼ -42.58 after sell → book $9,429.00; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,429.00 | ▲ close $9,429.00 vs 09:30 $9,454.24 (session +0.00) | 16:00 close · cash $9,429.00 · no lots left · equity $9,429.00. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,429.00 | ▲ 09:30 equity $9,429.00 vs yday $9,429.00 (-0.00) | 09:30 open · cash $9,429.00 · no holdings · equity $9,429.00 vs prior close $9,429.00 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,429.00 | ▲ close $9,429.00 vs 09:30 $9,429.00 (session +0.00) | 16:00 close · cash $9,429.00 · no lots left · equity $9,429.00. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,429.00 | ▲ 09:30 equity $9,429.00 vs yday $9,429.00 (-0.00) | 09:30 open · cash $9,429.00 · no holdings · equity $9,429.00 vs prior close $9,429.00 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,429.00 | ▲ close $9,429.00 vs 09:30 $9,429.00 (session +0.00) | 16:00 close · cash $9,429.00 · no lots left · equity $9,429.00. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,429.00 | ▲ 09:30 equity $9,429.00 vs yday $9,429.00 (-0.00) | 09:30 open · cash $9,429.00 · no holdings · equity $9,429.00 vs prior close $9,429.00 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 59 | $52.55 | $2.17 | — | $6,326.38 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $3143.00 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 310 | $10.11 | $4.00 | — | $3,188.28 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $3143.00 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 967 | $3.25 | $12.47 | — | $33.06 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+4.7; leftover $3143.00 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.06 | ▲ close $9,494.28 vs 09:30 $9,429.00 (session +83.92) | 16:00 close · cash $33.06 · equity $9,494.28 vs 09:30 $9,429.00 (+65.28; session marks +83.92) · 3 name(s) marked open→close (per-name table). BAND×59 09:30 $52.55 → close $56.87 +254.88; PAGS×310 09:30 $10.11 → close $10.12 +3.10; ZSQR×967 09:30 $3.25 → close $3.07 -174.06 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.06 | ▼ 09:30 equity $9,449.18 vs yday $9,494.28 (-45.10) | 09:30 open · cash $33.06 (unchanged overnight, no fees) · equity $9,449.18 vs prior close $9,494.28 (-45.10) · 3 name(s) re-marked at the open (per-name table). BAND×59 yday $56.87 → 09:30 $56.90 +1.77; PAGS×310 yday $10.12 → 09:30 $10.00 -37.20; ZSQR×967 yday $3.07 → 09:30 $3.06 -9.67 | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 59 | $56.90 | $2.20 | $+252.28 | $3,387.95 | ▲ +252.28 after sell → book $9,446.97; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 310 | $10.00 | $4.08 | $-42.17 | $6,483.88 | ▼ -42.17 after sell → book $9,442.90; vs 09:30 mark -4.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ZSQR` | 967 | $3.06 | $12.66 | $-208.86 | $9,430.24 | ▼ -208.86 after sell → book $9,430.24; vs 09:30 mark -12.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,430.24 | ▲ close $9,430.24 vs 09:30 $9,449.18 (session +0.00) | 16:00 close · cash $9,430.24 · no lots left · equity $9,430.24. | — |
