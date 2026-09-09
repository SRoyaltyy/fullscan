# Factor mine action — `union_white_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-4.94%** ($9,506) · signal-only (no cash/fees) was -2.19%. Starts YES **0/18**. Fills 128 · skips 0 · realized $-493.56.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,506.43.

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
| 2026-08-14 | `MARA` | 141 | — | $9.01 | +0.00 | $9.20 | +26.79 | +26.79 | +0.00 | +26.79 |
| 2026-08-14 | `LDI` | 1361 | — | $0.94 | +0.00 | $0.90 | -54.44 | -54.44 | +0.00 | -54.44 |
| 2026-08-14 | `BTBT` | 850 | — | $1.50 | +0.00 | $1.57 | +59.50 | +59.50 | +0.00 | +59.50 |
| 2026-08-14 | `BETR` | 86 | — | $14.80 | +0.00 | $13.73 | -92.02 | -92.02 | +0.00 | -92.02 |
| 2026-08-14 | `ANGX` | 295 | — | $4.31 | +0.00 | $4.37 | +17.70 | +17.70 | +0.00 | +17.70 |
| 2026-08-14 | `HYLN` | 305 | — | $4.18 | +0.00 | $4.06 | -36.60 | -36.60 | +0.00 | -36.60 |
| 2026-08-14 | `WDC` | 2 | — | $503.50 | +0.00 | $508.80 | +10.60 | +10.60 | +0.00 | +10.60 |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `MARA` | 141 | $9.20 | $9.22 | +2.82 | — | +0.00 | +2.82 | +29.61 | — |
| 2026-08-17 | `LDI` | 1361 | $0.90 | $0.91 | +13.61 | — | +0.00 | +13.61 | -40.83 | — |
| 2026-08-17 | `BTBT` | 850 | $1.57 | $1.52 | -42.50 | — | +0.00 | -42.50 | +17.00 | — |
| 2026-08-17 | `BETR` | 86 | $13.73 | $13.67 | -5.16 | — | +0.00 | -5.16 | -97.18 | — |
| 2026-08-17 | `ANGX` | 295 | $4.37 | $4.60 | +67.85 | — | +0.00 | +67.85 | +85.55 | — |
| 2026-08-17 | `HYLN` | 305 | $4.06 | $4.10 | +12.20 | — | +0.00 | +12.20 | -24.40 | — |
| 2026-08-17 | `WDC` | 2 | $508.80 | $525.53 | +33.46 | — | +0.00 | +33.46 | +44.06 | — |
| 2026-08-17 | `TMC` | 313 | — | $4.05 | +0.00 | $3.77 | -87.64 | -87.64 | +0.00 | -87.64 |
| 2026-08-17 | `TGB` | 149 | — | $8.46 | +0.00 | $8.77 | +46.19 | +46.19 | +0.00 | +46.19 |
| 2026-08-17 | `DNN` | 391 | — | $3.24 | +0.00 | $3.19 | -19.55 | -19.55 | +0.00 | -19.55 |
| 2026-08-17 | `CDNL` | 31 | — | $39.85 | +0.00 | $39.23 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-17 | `ABX` | 139 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `OCC` | 69 | — | $18.24 | +0.00 | $17.12 | -77.28 | -77.28 | +0.00 | -77.28 |
| 2026-08-17 | `ALM` | 78 | — | $16.20 | +0.00 | $16.36 | +12.48 | +12.48 | +0.00 | +12.48 |
| 2026-08-17 | `MRLN` | 338 | — | $3.75 | +0.00 | $3.54 | -72.67 | -72.67 | +0.00 | -72.67 |
| 2026-08-18 | `TMC` | 313 | $3.77 | $3.72 | -15.65 | — | +0.00 | -15.65 | -103.29 | — |
| 2026-08-18 | `TGB` | 149 | $8.77 | $8.55 | -32.78 | — | +0.00 | -32.78 | +13.41 | — |
| 2026-08-18 | `DNN` | 391 | $3.19 | $3.11 | -31.28 | — | +0.00 | -31.28 | -50.83 | — |
| 2026-08-18 | `CDNL` | 31 | $39.23 | $41.57 | +72.54 | — | +0.00 | +72.54 | +53.32 | — |
| 2026-08-18 | `ABX` | 139 | $9.12 | $9.03 | -12.51 | — | +0.00 | -12.51 | -12.51 | — |
| 2026-08-18 | `OCC` | 69 | $17.12 | $16.20 | -63.48 | — | +0.00 | -63.48 | -140.76 | — |
| 2026-08-18 | `ALM` | 78 | $16.36 | $15.78 | -45.24 | — | +0.00 | -45.24 | -32.76 | — |
| 2026-08-18 | `MRLN` | 338 | $3.54 | $3.50 | -11.83 | — | +0.00 | -11.83 | -84.50 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 59 | — | $20.55 | +0.00 | $21.19 | +37.76 | +37.76 | +0.00 | +37.76 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `HDSN` | 210 | — | $5.77 | +0.00 | $5.57 | -42.00 | -42.00 | +0.00 | -42.00 |
| 2026-08-20 | `IAG` | 62 | — | $19.63 | +0.00 | $20.50 | +53.94 | +53.94 | +0.00 | +53.94 |
| 2026-08-20 | `KGC` | 41 | — | $29.63 | +0.00 | $31.43 | +73.80 | +73.80 | +0.00 | +73.80 |
| 2026-08-20 | `NFGC` | 695 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `MRVI` | 163 | — | $7.44 | +0.00 | $8.29 | +138.55 | +138.55 | +0.00 | +138.55 |
| 2026-08-20 | `SCZM` | 128 | — | $9.46 | +0.00 | $9.76 | +38.40 | +38.40 | +0.00 | +38.40 |
| 2026-08-21 | `AG` | 59 | $21.19 | $21.90 | +41.89 | — | +0.00 | +41.89 | +79.65 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `HDSN` | 210 | $5.57 | $5.67 | +21.00 | — | +0.00 | +21.00 | -21.00 | — |
| 2026-08-21 | `IAG` | 62 | $20.50 | $21.17 | +41.54 | — | +0.00 | +41.54 | +95.48 | — |
| 2026-08-21 | `KGC` | 41 | $31.43 | $32.17 | +30.34 | — | +0.00 | +30.34 | +104.14 | — |
| 2026-08-21 | `NFGC` | 695 | $1.75 | $1.79 | +27.80 | — | +0.00 | +27.80 | +27.80 | — |
| 2026-08-21 | `MRVI` | 163 | $8.29 | $8.28 | -1.63 | — | +0.00 | -1.63 | +136.92 | — |
| 2026-08-21 | `SCZM` | 128 | $9.76 | $10.26 | +64.00 | — | +0.00 | +64.00 | +102.40 | — |
| 2026-08-21 | `CRSP` | 28 | — | $59.72 | +0.00 | $59.50 | -6.16 | -6.16 | +0.00 | -6.16 |
| 2026-08-21 | `CF` | 13 | — | $127.43 | +0.00 | $129.60 | +28.21 | +28.21 | +0.00 | +28.21 |
| 2026-08-21 | `EMBC` | 315 | — | $5.43 | +0.00 | $5.23 | -63.00 | -63.00 | +0.00 | -63.00 |
| 2026-08-21 | `TXG` | 26 | — | $64.39 | +0.00 | $65.12 | +18.98 | +18.98 | +0.00 | +18.98 |
| 2026-08-21 | `BEKE` | 95 | — | $17.93 | +0.00 | $17.75 | -17.57 | -17.57 | +0.00 | -17.57 |
| 2026-08-21 | `HITI` | 704 | — | $2.43 | +0.00 | $2.45 | +14.08 | +14.08 | +0.00 | +14.08 |
| 2026-08-24 | `CRSP` | 28 | $59.50 | $58.75 | -21.00 | — | +0.00 | -21.00 | -27.16 | — |
| 2026-08-24 | `CF` | 13 | $129.60 | $129.99 | +5.07 | — | +0.00 | +5.07 | +33.28 | — |
| 2026-08-24 | `EMBC` | 315 | $5.23 | $5.20 | -11.03 | — | +0.00 | -11.03 | -74.02 | — |
| 2026-08-24 | `TXG` | 26 | $65.12 | $63.15 | -51.22 | — | +0.00 | -51.22 | -32.24 | — |
| 2026-08-24 | `BEKE` | 95 | $17.75 | $18.05 | +28.97 | — | +0.00 | +28.97 | +11.40 | — |
| 2026-08-24 | `HITI` | 704 | $2.45 | $2.45 | +0.00 | — | +0.00 | +0.00 | +14.08 | — |
| 2026-08-25 | `CRMD` | 152 | — | $8.35 | +0.00 | $8.56 | +31.92 | +31.92 | +0.00 | +31.92 |
| 2026-08-25 | `ELMT` | 70 | — | $17.89 | +0.00 | $17.75 | -9.80 | -9.80 | +0.00 | -9.80 |
| 2026-08-25 | `AMTX` | 668 | — | $1.90 | +0.00 | $1.91 | +6.68 | +6.68 | +0.00 | +6.68 |
| 2026-08-25 | `BZ` | 83 | — | $15.28 | +0.00 | $16.29 | +83.83 | +83.83 | +0.00 | +83.83 |
| 2026-08-25 | `VIPS` | 90 | — | $13.96 | +0.00 | $14.16 | +18.00 | +18.00 | +0.00 | +18.00 |
| 2026-08-25 | `RHI` | 29 | — | $43.76 | +0.00 | $44.90 | +33.06 | +33.06 | +0.00 | +33.06 |
| 2026-08-25 | `VALE` | 84 | — | $15.01 | +0.00 | $15.33 | +26.88 | +26.88 | +0.00 | +26.88 |
| 2026-08-25 | `AYA` | 48 | — | $26.21 | +0.00 | $27.71 | +72.00 | +72.00 | +0.00 | +72.00 |
| 2026-08-26 | `CRMD` | 152 | $8.56 | $8.60 | +6.08 | — | +0.00 | +6.08 | +38.00 | — |
| 2026-08-26 | `ELMT` | 70 | $17.75 | $17.82 | +4.90 | — | +0.00 | +4.90 | -4.90 | — |
| 2026-08-26 | `AMTX` | 668 | $1.91 | $1.91 | +0.00 | — | +0.00 | +0.00 | +6.68 | — |
| 2026-08-26 | `BZ` | 83 | $16.29 | $16.77 | +39.84 | — | +0.00 | +39.84 | +123.67 | — |
| 2026-08-26 | `VIPS` | 90 | $14.16 | $14.00 | -14.40 | — | +0.00 | -14.40 | +3.60 | — |
| 2026-08-26 | `RHI` | 29 | $44.90 | $44.33 | -16.53 | — | +0.00 | -16.53 | +16.53 | — |
| 2026-08-26 | `VALE` | 84 | $15.33 | $15.37 | +3.36 | — | +0.00 | +3.36 | +30.24 | — |
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
| 2026-09-03 | `CABA` | 347 | — | $3.63 | +0.00 | $3.48 | -52.05 | -52.05 | +0.00 | -52.05 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `ARCT` | 75 | — | $16.77 | +0.00 | $15.56 | -90.75 | -90.75 | +0.00 | -90.75 |
| 2026-09-03 | `CRDL` | 577 | — | $2.18 | +0.00 | $2.16 | -11.54 | -11.54 | +0.00 | -11.54 |
| 2026-09-03 | `SDGR` | 59 | — | $21.03 | +0.00 | $20.71 | -18.88 | -18.88 | +0.00 | -18.88 |
| 2026-09-03 | `VIR` | 109 | — | $11.54 | +0.00 | $11.45 | -9.81 | -9.81 | +0.00 | -9.81 |
| 2026-09-04 | `ATRC` | 23 | $52.46 | $52.03 | -9.89 | $51.52 | -11.73 | -21.62 | -19.55 | -31.28 |
| 2026-09-04 | `HRMY` | 29 | $41.86 | $41.50 | -10.44 | $42.25 | +21.75 | +11.31 | -41.47 | -19.72 |
| 2026-09-04 | `CABA` | 347 | $3.48 | $3.46 | -6.94 | $3.47 | +3.47 | -3.47 | -58.99 | -55.52 |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `ARCT` | 75 | $15.56 | $15.61 | +3.75 | — | +0.00 | +3.75 | -87.00 | — |
| 2026-09-04 | `CRDL` | 577 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.54 | — |
| 2026-09-04 | `SDGR` | 59 | $20.71 | $20.58 | -7.67 | — | +0.00 | -7.67 | -26.55 | — |
| 2026-09-04 | `VIR` | 109 | $11.45 | $11.31 | -15.26 | $11.38 | +8.17 | -7.09 | -25.07 | -16.89 |
| 2026-09-04 | `ALEC` | 487 | — | $2.52 | +0.00 | $2.46 | -29.22 | -29.22 | +0.00 | -29.22 |
| 2026-09-04 | `BHC` | 183 | — | $6.71 | +0.00 | $6.56 | -27.45 | -27.45 | +0.00 | -27.45 |
| 2026-09-04 | `OABI` | 257 | — | $4.78 | +0.00 | $4.33 | -115.65 | -115.65 | +0.00 | -115.65 |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-08 | `ATRC` | 23 | $51.52 | $54.31 | +64.17 | — | +0.00 | +64.17 | +32.89 | — |
| 2026-09-08 | `HRMY` | 29 | $42.25 | $42.20 | -1.45 | — | +0.00 | -1.45 | -21.17 | — |
| 2026-09-08 | `CABA` | 347 | $3.47 | $3.43 | -13.88 | — | +0.00 | -13.88 | -69.40 | — |
| 2026-09-08 | `VIR` | 109 | $11.38 | $11.22 | -17.98 | — | +0.00 | -17.98 | -34.88 | — |
| 2026-09-08 | `ALEC` | 487 | $2.46 | $2.38 | -38.96 | — | +0.00 | -38.96 | -68.18 | — |
| 2026-09-08 | `BHC` | 183 | $6.56 | $6.57 | +1.83 | — | +0.00 | +1.83 | -25.62 | — |
| 2026-09-08 | `OABI` | 257 | $4.33 | $4.30 | -7.71 | — | +0.00 | -7.71 | -123.36 | — |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +216.83 | BTSG, TPG, TGTX, SLS, HIMS, VOR | — | $134.73 | $10,203.79 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75 |
| 2026-08-14 | +5.50 | $134.73 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75 | $10,217.42 | +13.63 | -57.49 | DAVE, MARA, LDI, BTBT, BETR, ANGX, HYLN, WDC | BTSG, TPG, TGTX, SLS, HIMS, VOR | $520.49 | $10,102.54 | DAVE×3, MARA×141, LDI×1361, BTBT×850, BETR×86, ANGX×295, HYLN×305, WDC×2 |
| 2026-08-17 | +2.25 | $520.49 | DAVE×3, MARA×141, LDI×1361, BTBT×850, BETR×86, ANGX×295, HYLN×305, WDC×2 | $10,191.93 | +89.39 | -217.69 | TMC, TGB, DNN, CDNL, ABX, OCC, ALM, MRLN | DAVE, MARA, LDI, BTBT, BETR, ANGX, HYLN, WDC | $35.03 | $9,905.06 | TMC×313, TGB×149, DNN×391, CDNL×31, ABX×139, OCC×69, ALM×78, MRLN×338 |
| 2026-08-18 | -6.20 | $35.03 | TMC×313, TGB×149, DNN×391, CDNL×31, ABX×139, OCC×69, ALM×78, MRLN×338 | $9,764.83 | -140.23 | +0.00 | — | TMC, TGB, DNN, CDNL, ABX, OCC, ALM, MRLN | $9,739.70 | $9,739.70 | — |
| 2026-08-19 | -7.20 | $9,739.70 | — | $9,739.70 | +0.00 | +0.00 | — | — | $9,739.70 | $9,739.70 | — |
| 2026-08-20 | +1.12 | $9,739.70 | — | $9,739.70 | +0.00 | +334.51 | AG, BHP, HDSN, IAG, KGC, NFGC, MRVI, SCZM | — | $35.67 | $10,049.20 | AG×59, BHP×13, HDSN×210, IAG×62, KGC×41, NFGC×695, MRVI×163, SCZM×128 |
| 2026-08-21 | +3.25 | $35.67 | AG×59, BHP×13, HDSN×210, IAG×62, KGC×41, NFGC×695, MRVI×163, SCZM×128 | $10,301.31 | +252.11 | -25.46 | CRSP, CF, EMBC, TXG, BEKE, HITI | AG, BHP, HDSN, IAG, KGC, NFGC, MRVI, SCZM | $126.50 | $10,228.92 | CRSP×28, CF×13, EMBC×315, TXG×26, BEKE×95, HITI×704 |
| 2026-08-24 | -5.17 | $126.50 | CRSP×28, CF×13, EMBC×315, TXG×26, BEKE×95, HITI×704 | $10,179.72 | -49.20 | +0.00 | — | CRSP, CF, EMBC, TXG, BEKE, HITI | $10,157.84 | $10,157.84 | — |
| 2026-08-25 | +1.80 | $10,157.84 | — | $10,157.84 | -0.00 | +262.57 | CRMD, ELMT, AMTX, BZ, VIPS, RHI, VALE, AYA | — | $30.32 | $10,396.19 | CRMD×152, ELMT×70, AMTX×668, BZ×83, VIPS×90, RHI×29, VALE×84, AYA×48 |
| 2026-08-26 | +2.02 | $30.32 | CRMD×152, ELMT×70, AMTX×668, BZ×83, VIPS×90, RHI×29, VALE×84, AYA×48 | $10,396.88 | +0.69 | +0.00 | — | CRMD, ELMT, AMTX, BZ, VIPS, RHI, VALE, AYA | $10,372.37 | $10,372.37 | — |
| 2026-08-27 | — | $10,372.37 | — | $10,372.37 | +0.00 | +0.00 | — | — | $10,372.37 | $10,372.37 | — |
| 2026-08-28 | +0.75 | $10,372.37 | — | $10,372.37 | +0.00 | -307.04 | TTMI, KEYS, AVT, CGNX, COHR, LSCC, MTSI, OLED | — | $886.16 | $10,049.17 | TTMI×10, KEYS×3, AVT×14, CGNX×20, COHR×4, LSCC×10, MTSI×4, OLED×15 |
| 2026-08-31 | -5.85 | $886.16 | TTMI×10, KEYS×3, AVT×14, CGNX×20, COHR×4, LSCC×10, MTSI×4, OLED×15 | $10,096.23 | +47.06 | +0.00 | — | TTMI, KEYS, AVT, CGNX, COHR, LSCC, MTSI, OLED | $10,079.91 | $10,079.91 | — |
| 2026-09-01 | -6.30 | $10,079.91 | — | $10,079.91 | +0.00 | +0.00 | — | — | $10,079.91 | $10,079.91 | — |
| 2026-09-02 | -3.83 | $10,079.91 | — | $10,079.91 | +0.00 | +0.00 | — | — | $10,079.91 | $10,079.91 | — |
| 2026-09-03 | -0.90 | $10,079.91 | — | $10,079.91 | +0.00 | -240.10 | ATRC, HRMY, CABA, RVTY, ARCT, CRDL, SDGR, VIR | — | $128.03 | $9,815.04 | ATRC×23, HRMY×29, CABA×347, RVTY×9, ARCT×75, CRDL×577, SDGR×59, VIR×109 |
| 2026-09-04 | +2.25 | $128.03 | ATRC×23, HRMY×29, CABA×347, RVTY×9, ARCT×75, CRDL×577, SDGR×59, VIR×109 | $9,763.19 | -51.85 | -167.18 | ALEC, BHC, OABI, CRM | RVTY, ARCT, CRDL, SDGR | $164.37 | $9,567.87 | ATRC×23, HRMY×29, CABA×347, VIR×109, ALEC×487, BHC×183, OABI×257, CRM×4 |
| 2026-09-08 | -11.47 | $164.37 | ATRC×23, HRMY×29, CABA×347, VIR×109, ALEC×487, BHC×183, OABI×257, CRM×4 | $9,531.84 | -36.03 | +0.00 | — | ATRC, HRMY, CABA, VIR, ALEC, BHC, OABI, CRM | $9,506.43 | $9,506.43 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 27 | $59.80 | $2.07 | — | $8,383.33 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $6,761.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 33 | $49.70 | $2.09 | — | $5,119.11 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 142 | $11.70 | $2.42 | — | $3,455.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 56 | $29.74 | $2.16 | — | $1,787.70 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 75 | $22.01 | $2.21 | — | $134.73 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.73 | ▲ close $10,203.79 vs 09:30 $10,000.00 (session +216.83) | 16:00 close · cash $134.73 · equity $10,203.79 vs 09:30 $10,000.00 (+203.79; session marks +216.83) · 6 name(s) marked open→close (per-name table). BTSG×27 09:30 $59.80 → close $60.23 +11.61; TPG×32 09:30 $50.62 → close $54.62 +127.90; TGTX×33 09:30 $49.70 → close $47.94 -58.08; SLS×142 09:30 $11.70 → close $12.36 +93.72; HIMS×56 09:30 $29.74 → close $28.77 -54.32; VOR×75 09:30 $22.01 → close $23.29 +96.00 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.73 | ▲ 09:30 equity $10,217.42 vs yday $10,203.79 (+13.63) | 09:30 open · cash $134.73 (unchanged overnight, no fees) · equity $10,217.42 vs prior close $10,203.79 (+13.63) · 6 name(s) re-marked at the open (per-name table). BTSG×27 yday $60.23 → 09:30 $59.65 -15.66; TPG×32 yday $54.62 → 09:30 $55.29 +21.44; TGTX×33 yday $47.94 → 09:30 $47.27 -22.11; SLS×142 yday $12.36 → 09:30 $12.40 +5.68; HIMS×56 yday $28.77 → 09:30 $29.15 +21.28; VOR×75 yday $23.29 → 09:30 $23.33 +3.00 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 27 | $59.65 | $2.09 | $-8.21 | $1,743.19 | ▼ -8.21 after sell → book $10,215.33; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $3,510.36 | ▲ +145.14 after sell → book $10,213.22; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 33 | $47.27 | $2.11 | $-84.39 | $5,068.16 | ▼ -84.39 after sell → book $10,211.11; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 142 | $12.40 | $2.45 | $+94.53 | $6,826.50 | ▲ +94.53 after sell → book $10,208.65; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 56 | $29.15 | $2.18 | $-37.38 | $8,456.72 | ▼ -37.38 after sell → book $10,206.47; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 75 | $23.33 | $2.24 | $+94.54 | $10,204.23 | ▲ +94.54 after sell → book $10,204.23; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $9,209.50 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 141 | $9.01 | $2.41 | — | $7,936.68 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1361 | $0.94 | $16.84 | — | $6,644.59 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 850 | $1.50 | $10.96 | — | $5,358.62 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 86 | $14.80 | $2.25 | — | $4,083.57 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 295 | $4.31 | $3.81 | — | $2,808.32 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 305 | $4.18 | $3.93 | — | $1,529.48 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $520.49 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $520.49 | ▼ close $10,102.54 vs 09:30 $10,217.42 (session -57.49) | 16:00 close · cash $520.49 · equity $10,102.54 vs 09:30 $10,217.42 (-114.88; session marks -57.49) · 8 name(s) marked open→close (per-name table). DAVE×3 09:30 $330.91 → close $334.57 +10.98; MARA×141 09:30 $9.01 → close $9.20 +26.79; LDI×1361 09:30 $0.94 → close $0.90 -54.44; BTBT×850 09:30 $1.50 → close $1.57 +59.50; BETR×86 09:30 $14.80 → close $13.73 -92.02; ANGX×295 09:30 $4.31 → close $4.37 +17.70; HYLN×305 09:30 $4.18 → close $4.06 -36.60; WDC×2 09:30 $503.50 → close $508.80 +10.60 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $520.49 | ▲ 09:30 equity $10,191.93 vs yday $10,102.54 (+89.39) | 09:30 open · cash $520.49 (unchanged overnight, no fees) · equity $10,191.93 vs prior close $10,102.54 (+89.39) · 8 name(s) re-marked at the open (per-name table). DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; MARA×141 yday $9.20 → 09:30 $9.22 +2.82; LDI×1361 yday $0.90 → 09:30 $0.91 +13.61; BTBT×850 yday $1.57 → 09:30 $1.52 -42.50; BETR×86 yday $13.73 → 09:30 $13.67 -5.16; ANGX×295 yday $4.37 → 09:30 $4.60 +67.85; HYLN×305 yday $4.06 → 09:30 $4.10 +12.20; WDC×2 yday $508.80 → 09:30 $525.53 +33.46 | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $1,529.29 | ▲ +14.07 after sell → book $10,189.92; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 141 | $9.22 | $2.45 | $+24.75 | $2,826.86 | ▲ +24.75 after sell → book $10,187.47; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1361 | $0.91 | $16.66 | $-74.33 | $4,044.63 | ▼ -74.33 after sell → book $10,170.81; vs 09:30 mark -16.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 850 | $1.52 | $11.12 | $-5.08 | $5,325.51 | ▼ -5.08 after sell → book $10,159.69; vs 09:30 mark -11.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 86 | $13.67 | $2.27 | $-101.70 | $6,498.86 | ▼ -101.70 after sell → book $10,157.42; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 295 | $4.60 | $3.87 | $+77.88 | $7,851.99 | ▲ +77.88 after sell → book $10,153.55; vs 09:30 mark -3.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 305 | $4.10 | $4.00 | $-32.33 | $9,098.50 | ▼ -32.33 after sell → book $10,149.56; vs 09:30 mark -3.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $10,147.54 | ▲ +40.05 after sell → book $10,147.54; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $8,875.85 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1268.44 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 149 | $8.46 | $2.44 | — | $7,612.88 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1268.44 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $6,340.99 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $1268.44 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $5,103.56 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1268.44 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 139 | $9.12 | $2.41 | — | $3,833.47 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1268.44 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 69 | $18.24 | $2.20 | — | $2,572.71 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1268.44 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 78 | $16.20 | $2.22 | — | $1,306.89 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1268.44 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `MRLN` | 338 | $3.75 | $4.36 | — | $35.03 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-15.4; leftover $1268.44 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.03 | ▼ close $9,905.06 vs 09:30 $10,191.93 (session -217.69) | 16:00 close · cash $35.03 · equity $9,905.06 vs 09:30 $10,191.93 (-286.87; session marks -217.69) · 8 name(s) marked open→close (per-name table). TMC×313 09:30 $4.05 → close $3.77 -87.64; TGB×149 09:30 $8.46 → close $8.77 +46.19; DNN×391 09:30 $3.24 → close $3.19 -19.55; CDNL×31 09:30 $39.85 → close $39.23 -19.22; ABX×139 09:30 $9.12 → close $9.12 +0.00; OCC×69 09:30 $18.24 → close $17.12 -77.28; ALM×78 09:30 $16.20 → close $16.36 +12.48; MRLN×338 09:30 $3.75 → close $3.54 -72.67 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.03 | ▼ 09:30 equity $9,764.83 vs yday $9,905.06 (-140.23) | 09:30 open · cash $35.03 (unchanged overnight, no fees) · equity $9,764.83 vs prior close $9,905.06 (-140.23) · 8 name(s) re-marked at the open (per-name table). TMC×313 yday $3.77 → 09:30 $3.72 -15.65; TGB×149 yday $8.77 → 09:30 $8.55 -32.78; DNN×391 yday $3.19 → 09:30 $3.11 -31.28; CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; ABX×139 yday $9.12 → 09:30 $9.03 -12.51; OCC×69 yday $17.12 → 09:30 $16.20 -63.48; ALM×78 yday $16.36 → 09:30 $15.78 -45.24; MRLN×338 yday $3.54 → 09:30 $3.50 -11.83 | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $1,195.29 | ▼ -111.43 after sell → book $9,760.73; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 149 | $8.55 | $2.47 | $+8.50 | $2,466.77 | ▲ +8.50 after sell → book $9,758.26; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 391 | $3.11 | $5.12 | $-60.99 | $3,677.66 | ▼ -60.99 after sell → book $9,753.14; vs 09:30 mark -5.12 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $4,964.23 | ▲ +49.13 after sell → book $9,751.04; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 139 | $9.03 | $2.44 | $-17.36 | $6,216.96 | ▼ -17.36 after sell → book $9,748.60; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 69 | $16.20 | $2.22 | $-145.18 | $7,332.54 | ▼ -145.18 after sell → book $9,746.38; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 78 | $15.78 | $2.25 | $-37.23 | $8,561.13 | ▼ -37.23 after sell → book $9,744.13; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `MRLN` | 338 | $3.50 | $4.43 | $-93.29 | $9,739.70 | ▼ -93.29 after sell → book $9,739.70; vs 09:30 mark -4.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,739.70 | ▲ close $9,739.70 vs 09:30 $9,764.83 (session +0.00) | 16:00 close · cash $9,739.70 · no lots left · equity $9,739.70. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,739.70 | ▲ 09:30 equity $9,739.70 vs yday $9,739.70 (+0.00) | 09:30 open · cash $9,739.70 · no holdings · equity $9,739.70 vs prior close $9,739.70 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,739.70 | ▲ close $9,739.70 vs 09:30 $9,739.70 (session +0.00) | 16:00 close · cash $9,739.70 · no lots left · equity $9,739.70. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,739.70 | ▲ 09:30 equity $9,739.70 vs yday $9,739.70 (+0.00) | 09:30 open · cash $9,739.70 · no holdings · equity $9,739.70 vs prior close $9,739.70 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $8,525.09 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,339.93 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 210 | $5.77 | $2.71 | — | $6,125.52 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 62 | $19.63 | $2.18 | — | $4,906.28 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $3,689.34 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 695 | $1.75 | $8.97 | — | $2,464.13 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 163 | $7.44 | $2.48 | — | $1,248.93 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 128 | $9.46 | $2.37 | — | $35.67 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.67 | ▲ close $10,049.20 vs 09:30 $9,739.70 (session +334.51) | 16:00 close · cash $35.67 · equity $10,049.20 vs 09:30 $9,739.70 (+309.50; session marks +334.51) · 8 name(s) marked open→close (per-name table). AG×59 09:30 $20.55 → close $21.19 +37.76; BHP×13 09:30 $91.01 → close $93.63 +34.06; HDSN×210 09:30 $5.77 → close $5.57 -42.00; IAG×62 09:30 $19.63 → close $20.50 +53.94; KGC×41 09:30 $29.63 → close $31.43 +73.80; NFGC×695 09:30 $1.75 → close $1.75 +0.00; MRVI×163 09:30 $7.44 → close $8.29 +138.55; SCZM×128 09:30 $9.46 → close $9.76 +38.40 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.67 | ▲ 09:30 equity $10,301.31 vs yday $10,049.20 (+252.11) | 09:30 open · cash $35.67 (unchanged overnight, no fees) · equity $10,301.31 vs prior close $10,049.20 (+252.11) · 8 name(s) re-marked at the open (per-name table). AG×59 yday $21.19 → 09:30 $21.90 +41.89; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; HDSN×210 yday $5.57 → 09:30 $5.67 +21.00; IAG×62 yday $20.50 → 09:30 $21.17 +41.54; KGC×41 yday $31.43 → 09:30 $32.17 +30.34; NFGC×695 yday $1.75 → 09:30 $1.79 +27.80; MRVI×163 yday $8.29 → 09:30 $8.28 -1.63; SCZM×128 yday $9.76 → 09:30 $10.26 +64.00 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 59 | $21.90 | $2.19 | $+75.30 | $1,325.59 | ▲ +75.30 after sell → book $10,299.13; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,567.90 | ▲ +57.15 after sell → book $10,297.08; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 210 | $5.67 | $2.75 | $-26.46 | $3,755.84 | ▼ -26.46 after sell → book $10,294.32; vs 09:30 mark -2.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 62 | $21.17 | $2.20 | $+91.11 | $5,066.19 | ▲ +91.11 after sell → book $10,292.13; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $6,383.02 | ▲ +99.89 after sell → book $10,289.99; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 695 | $1.79 | $9.09 | $+9.74 | $7,617.98 | ▲ +9.74 after sell → book $10,280.90; vs 09:30 mark -9.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRVI` | 163 | $8.28 | $2.52 | $+131.92 | $8,965.10 | ▲ +131.92 after sell → book $10,278.38; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 128 | $10.26 | $2.41 | $+97.62 | $10,275.98 | ▲ +97.62 after sell → book $10,275.98; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 28 | $59.72 | $2.07 | — | $8,601.74 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1712.66 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 13 | $127.43 | $2.03 | — | $6,943.13 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1712.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 315 | $5.43 | $4.06 | — | $5,228.61 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1712.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 26 | $64.39 | $2.07 | — | $3,552.40 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1712.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 95 | $17.93 | $2.27 | — | $1,846.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $1712.66 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 704 | $2.43 | $9.08 | — | $126.50 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $1712.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.50 | ▼ close $10,228.92 vs 09:30 $10,301.31 (session -25.46) | 16:00 close · cash $126.50 · equity $10,228.92 vs 09:30 $10,301.31 (-72.39; session marks -25.46) · 6 name(s) marked open→close (per-name table). CRSP×28 09:30 $59.72 → close $59.50 -6.16; CF×13 09:30 $127.43 → close $129.60 +28.21; EMBC×315 09:30 $5.43 → close $5.23 -63.00; TXG×26 09:30 $64.39 → close $65.12 +18.98; BEKE×95 09:30 $17.93 → close $17.75 -17.57; HITI×704 09:30 $2.43 → close $2.45 +14.08 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.50 | ▼ 09:30 equity $10,179.72 vs yday $10,228.92 (-49.20) | 09:30 open · cash $126.50 (unchanged overnight, no fees) · equity $10,179.72 vs prior close $10,228.92 (-49.20) · 6 name(s) re-marked at the open (per-name table). CRSP×28 yday $59.50 → 09:30 $58.75 -21.00; CF×13 yday $129.60 → 09:30 $129.99 +5.07; EMBC×315 yday $5.23 → 09:30 $5.20 -11.03; TXG×26 yday $65.12 → 09:30 $63.15 -51.22; BEKE×95 yday $17.75 → 09:30 $18.05 +28.97; HITI×704 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 28 | $58.75 | $2.10 | $-31.33 | $1,769.40 | ▼ -31.33 after sell → book $10,177.62; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 13 | $129.99 | $2.05 | $+29.20 | $3,457.22 | ▲ +29.20 after sell → book $10,175.57; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 315 | $5.20 | $4.13 | $-82.22 | $5,089.52 | ▼ -82.22 after sell → book $10,171.44; vs 09:30 mark -4.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 26 | $63.15 | $2.09 | $-36.40 | $6,729.33 | ▼ -36.40 after sell → book $10,169.35; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 95 | $18.05 | $2.30 | $+6.82 | $8,442.25 | ▲ +6.82 after sell → book $10,167.05; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 704 | $2.45 | $9.21 | $-4.21 | $10,157.84 | ▼ -4.21 after sell → book $10,157.84; vs 09:30 mark -9.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,157.84 | ▲ close $10,157.84 vs 09:30 $10,179.72 (session +0.00) | 16:00 close · cash $10,157.84 · no lots left · equity $10,157.84. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,157.84 | ▲ 09:30 equity $10,157.84 vs yday $10,157.84 (-0.00) | 09:30 open · cash $10,157.84 · no holdings · equity $10,157.84 vs prior close $10,157.84 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 152 | $8.35 | $2.45 | — | $8,886.19 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1269.73 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ELMT` | 70 | $17.89 | $2.20 | — | $7,631.69 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-7.5; leftover $1269.73 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 668 | $1.90 | $8.62 | — | $6,353.87 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=+5.0; leftover $1269.73 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 83 | $15.28 | $2.24 | — | $5,083.39 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=-0.7; leftover $1269.73 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 90 | $13.96 | $2.26 | — | $3,824.73 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.0; leftover $1269.73 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 29 | $43.76 | $2.08 | — | $2,553.62 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1269.73 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VALE` | 84 | $15.01 | $2.24 | — | $1,290.53 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; ⚪; ret5=+9.4; leftover $1269.73 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AYA` | 48 | $26.21 | $2.13 | — | $30.32 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; ⚪; ret5=-0.3; leftover $1269.73 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.32 | ▲ close $10,396.19 vs 09:30 $10,157.84 (session +262.57) | 16:00 close · cash $30.32 · equity $10,396.19 vs 09:30 $10,157.84 (+238.35; session marks +262.57) · 8 name(s) marked open→close (per-name table). CRMD×152 09:30 $8.35 → close $8.56 +31.92; ELMT×70 09:30 $17.89 → close $17.75 -9.80; AMTX×668 09:30 $1.90 → close $1.91 +6.68; BZ×83 09:30 $15.28 → close $16.29 +83.83; VIPS×90 09:30 $13.96 → close $14.16 +18.00; RHI×29 09:30 $43.76 → close $44.90 +33.06; VALE×84 09:30 $15.01 → close $15.33 +26.88; AYA×48 09:30 $26.21 → close $27.71 +72.00 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.32 | ▲ 09:30 equity $10,396.88 vs yday $10,396.19 (+0.69) | 09:30 open · cash $30.32 (unchanged overnight, no fees) · equity $10,396.88 vs prior close $10,396.19 (+0.69) · 8 name(s) re-marked at the open (per-name table). CRMD×152 yday $8.56 → 09:30 $8.60 +6.08; ELMT×70 yday $17.75 → 09:30 $17.82 +4.90; AMTX×668 yday $1.91 → 09:30 $1.91 +0.00; BZ×83 yday $16.29 → 09:30 $16.77 +39.84; VIPS×90 yday $14.16 → 09:30 $14.00 -14.40; RHI×29 yday $44.90 → 09:30 $44.33 -16.53; VALE×84 yday $15.33 → 09:30 $15.37 +3.36; AYA×48 yday $27.71 → 09:30 $27.24 -22.56 | — |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 152 | $8.60 | $2.48 | $+33.07 | $1,335.04 | ▲ +33.07 after sell → book $10,394.40; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `ELMT` | 70 | $17.82 | $2.22 | $-9.32 | $2,580.22 | ▼ -9.32 after sell → book $10,392.18; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AMTX` | 668 | $1.91 | $8.74 | $-10.68 | $3,847.36 | ▼ -10.68 after sell → book $10,383.44; vs 09:30 mark -8.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BZ` | 83 | $16.77 | $2.26 | $+119.17 | $5,237.00 | ▲ +119.17 after sell → book $10,381.17; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `VIPS` | 90 | $14.00 | $2.29 | $-0.95 | $6,494.72 | ▼ -0.95 after sell → book $10,378.89; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 29 | $44.33 | $2.10 | $+12.36 | $7,778.19 | ▲ +12.36 after sell → book $10,376.79; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VALE` | 84 | $15.37 | $2.27 | $+25.73 | $9,067.01 | ▲ +25.73 after sell → book $10,374.53; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AYA` | 48 | $27.24 | $2.15 | $+45.15 | $10,372.37 | ▲ +45.15 after sell → book $10,372.37; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,372.37 | ▲ close $10,372.37 vs 09:30 $10,396.88 (session +0.00) | 16:00 close · cash $10,372.37 · no lots left · equity $10,372.37. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,372.37 | ▲ 09:30 equity $10,372.37 vs yday $10,372.37 (+0.00) | 09:30 open · cash $10,372.37 · no holdings · equity $10,372.37 vs prior close $10,372.37 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,372.37 | ▲ close $10,372.37 vs 09:30 $10,372.37 (session +0.00) | 16:00 close · cash $10,372.37 · no lots left · equity $10,372.37. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,372.37 | ▲ 09:30 equity $10,372.37 vs yday $10,372.37 (+0.00) | 09:30 open · cash $10,372.37 · no holdings · equity $10,372.37 vs prior close $10,372.37 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $9,142.25 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1296.55 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $8,167.02 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1296.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $6,884.13 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1296.55 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.82 | $2.05 | — | $5,625.68 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1296.55 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $4,465.92 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1296.55 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $119.76 | $2.02 | — | $3,266.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1296.55 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MTSI` | 4 | $275.20 | $2.00 | — | $2,163.50 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+4.1; leftover $1296.55 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OLED` | 15 | $85.02 | $2.04 | — | $886.16 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-1.9; leftover $1296.55 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $886.16 | ▼ close $10,049.17 vs 09:30 $10,372.37 (session -307.04) | 16:00 close · cash $886.16 · equity $10,049.17 vs 09:30 $10,372.37 (-323.20; session marks -307.04) · 8 name(s) marked open→close (per-name table). TTMI×10 09:30 $122.81 → close $118.65 -41.60; KEYS×3 09:30 $324.41 → close $319.97 -13.32; AVT×14 09:30 $91.49 → close $88.63 -40.04; CGNX×20 09:30 $62.82 → close $60.46 -47.20; COHR×4 09:30 $289.44 → close $279.20 -40.96; LSCC×10 09:30 $119.76 → close $114.40 -53.60; MTSI×4 09:30 $275.20 → close $265.27 -39.72; OLED×15 09:30 $85.02 → close $82.98 -30.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $886.16 | ▲ 09:30 equity $10,096.23 vs yday $10,049.17 (+47.06) | 09:30 open · cash $886.16 (unchanged overnight, no fees) · equity $10,096.23 vs prior close $10,049.17 (+47.06) · 8 name(s) re-marked at the open (per-name table). TTMI×10 yday $118.65 → 09:30 $118.83 +1.80; KEYS×3 yday $319.97 → 09:30 $322.49 +7.56; AVT×14 yday $88.63 → 09:30 $89.39 +10.64; CGNX×20 yday $60.46 → 09:30 $60.46 +0.00; COHR×4 yday $279.20 → 09:30 $280.25 +4.20; LSCC×10 yday $114.40 → 09:30 $115.56 +11.60; MTSI×4 yday $265.27 → 09:30 $266.96 +6.76; OLED×15 yday $82.98 → 09:30 $83.28 +4.50 | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $118.83 | $2.04 | $-43.86 | $2,072.42 | ▼ -43.86 after sell → book $10,094.19; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 3 | $322.49 | $2.02 | $-9.78 | $3,037.87 | ▼ -9.78 after sell → book $10,092.17; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $4,287.28 | ▼ -33.48 after sell → book $10,090.12; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 20 | $60.46 | $2.07 | $-51.32 | $5,494.41 | ▼ -51.32 after sell → book $10,088.05; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $6,613.39 | ▼ -40.78 after sell → book $10,086.03; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 10 | $115.56 | $2.04 | $-46.06 | $7,766.95 | ▼ -46.06 after sell → book $10,083.99; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MTSI` | 4 | $266.96 | $2.02 | $-36.98 | $8,832.77 | ▼ -36.98 after sell → book $10,081.97; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OLED` | 15 | $83.28 | $2.06 | $-30.19 | $10,079.91 | ▼ -30.19 after sell → book $10,079.91; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,079.91 | ▲ close $10,079.91 vs 09:30 $10,096.23 (session +0.00) | 16:00 close · cash $10,079.91 · no lots left · equity $10,079.91. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,079.91 | ▲ 09:30 equity $10,079.91 vs yday $10,079.91 (+0.00) | 09:30 open · cash $10,079.91 · no holdings · equity $10,079.91 vs prior close $10,079.91 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,079.91 | ▲ close $10,079.91 vs 09:30 $10,079.91 (session +0.00) | 16:00 close · cash $10,079.91 · no lots left · equity $10,079.91. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,079.91 | ▲ 09:30 equity $10,079.91 vs yday $10,079.91 (+0.00) | 09:30 open · cash $10,079.91 · no holdings · equity $10,079.91 vs prior close $10,079.91 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,079.91 | ▲ close $10,079.91 vs 09:30 $10,079.91 (session +0.00) | 16:00 close · cash $10,079.91 · no lots left · equity $10,079.91. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,079.91 | ▲ 09:30 equity $10,079.91 vs yday $10,079.91 (+0.00) | 09:30 open · cash $10,079.91 · no holdings · equity $10,079.91 vs prior close $10,079.91 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,861.61 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1259.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,614.57 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1259.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 347 | $3.63 | $4.48 | — | $6,350.48 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1259.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $5,156.41 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1259.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 75 | $16.77 | $2.21 | — | $3,896.45 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1259.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 577 | $2.18 | $7.44 | — | $2,631.14 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1259.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SDGR` | 59 | $21.03 | $2.17 | — | $1,388.21 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1259.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 109 | $11.54 | $2.32 | — | $128.03 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $1259.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.03 | ▼ close $9,815.04 vs 09:30 $10,079.91 (session -240.10) | 16:00 close · cash $128.03 · equity $9,815.04 vs 09:30 $10,079.91 (-264.87; session marks -240.10) · 8 name(s) marked open→close (per-name table). ATRC×23 09:30 $52.88 → close $52.46 -9.66; HRMY×29 09:30 $42.93 → close $41.86 -31.03; CABA×347 09:30 $3.63 → close $3.48 -52.05; RVTY×9 09:30 $132.45 → close $130.63 -16.38; ARCT×75 09:30 $16.77 → close $15.56 -90.75; CRDL×577 09:30 $2.18 → close $2.16 -11.54; SDGR×59 09:30 $21.03 → close $20.71 -18.88; VIR×109 09:30 $11.54 → close $11.45 -9.81 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.03 | ▼ 09:30 equity $9,763.19 vs yday $9,815.04 (-51.85) | 09:30 open · cash $128.03 (unchanged overnight, no fees) · equity $9,763.19 vs prior close $9,815.04 (-51.85) · 8 name(s) re-marked at the open (per-name table). ATRC×23 yday $52.46 → 09:30 $52.03 -9.89; HRMY×29 yday $41.86 → 09:30 $41.50 -10.44; CABA×347 yday $3.48 → 09:30 $3.46 -6.94; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; ARCT×75 yday $15.56 → 09:30 $15.61 +3.75; CRDL×577 yday $2.16 → 09:30 $2.16 +0.00; SDGR×59 yday $20.71 → 09:30 $20.58 -7.67; VIR×109 yday $11.45 → 09:30 $11.31 -15.26 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $1,296.26 | ▼ -25.83 after sell → book $9,761.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 75 | $15.61 | $2.24 | $-91.45 | $2,464.78 | ▼ -91.45 after sell → book $9,758.92; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 577 | $2.16 | $7.55 | $-26.53 | $3,703.55 | ▼ -26.53 after sell → book $9,751.37; vs 09:30 mark -7.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SDGR` | 59 | $20.58 | $2.19 | $-30.90 | $4,915.58 | ▼ -30.90 after sell → book $9,749.18; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 487 | $2.52 | $6.28 | — | $3,682.06 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1228.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 183 | $6.71 | $2.54 | — | $2,451.59 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1228.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 257 | $4.78 | $3.32 | — | $1,219.81 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1228.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $164.37 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1228.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $164.37 | ▼ close $9,567.87 vs 09:30 $9,763.19 (session -167.18) | 16:00 close · cash $164.37 · equity $9,567.87 vs 09:30 $9,763.19 (-195.32; session marks -167.18) · 8 name(s) marked open→close (per-name table). ATRC×23 09:30 $52.03 → close $51.52 -11.73; HRMY×29 09:30 $41.50 → close $42.25 +21.75; CABA×347 09:30 $3.46 → close $3.47 +3.47; VIR×109 09:30 $11.31 → close $11.38 +8.17; ALEC×487 09:30 $2.52 → close $2.46 -29.22; BHC×183 09:30 $6.71 → close $6.56 -27.45; OABI×257 09:30 $4.78 → close $4.33 -115.65; CRM×4 09:30 $263.36 → close $259.23 -16.52 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $164.37 | ▼ 09:30 equity $9,531.84 vs yday $9,567.87 (-36.03) | 09:30 open · cash $164.37 (unchanged overnight, no fees) · equity $9,531.84 vs prior close $9,567.87 (-36.03) · 8 name(s) re-marked at the open (per-name table). ATRC×23 yday $51.52 → 09:30 $54.31 +64.17; HRMY×29 yday $42.25 → 09:30 $42.20 -1.45; CABA×347 yday $3.47 → 09:30 $3.43 -13.88; VIR×109 yday $11.38 → 09:30 $11.22 -17.98; ALEC×487 yday $2.46 → 09:30 $2.38 -38.96; BHC×183 yday $6.56 → 09:30 $6.57 +1.83; OABI×257 yday $4.33 → 09:30 $4.30 -7.71; CRM×4 yday $259.23 → 09:30 $253.72 -22.04 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 23 | $54.31 | $2.08 | $+28.75 | $1,411.42 | ▲ +28.75 after sell → book $9,529.76; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 29 | $42.20 | $2.10 | $-25.34 | $2,633.13 | ▼ -25.34 after sell → book $9,527.67; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 347 | $3.43 | $4.54 | $-78.42 | $3,818.79 | ▼ -78.42 after sell → book $9,523.12; vs 09:30 mark -4.55 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 109 | $11.22 | $2.35 | $-39.54 | $5,039.43 | ▼ -39.54 after sell → book $9,520.78; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 487 | $2.38 | $6.37 | $-80.84 | $6,192.11 | ▼ -80.84 after sell → book $9,514.40; vs 09:30 mark -6.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 183 | $6.57 | $2.58 | $-30.74 | $7,391.84 | ▼ -30.74 after sell → book $9,511.82; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 257 | $4.30 | $3.37 | $-130.04 | $8,493.58 | ▼ -130.04 after sell → book $9,508.46; vs 09:30 mark -3.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $9,506.43 | ▼ -42.58 after sell → book $9,506.43; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,506.43 | ▲ close $9,506.43 vs 09:30 $9,531.84 (session +0.00) | 16:00 close · cash $9,506.43 · no lots left · equity $9,506.43. | — |
