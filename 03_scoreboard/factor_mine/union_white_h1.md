# Factor mine action — `union_white_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ white, no 🚨

Cash book **+9.28%** ($10,928) · signal-only (no cash/fees) was +2.04%. Starts YES **10/18**. Fills 142 · skips 0 · realized $+928.08.

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
- **Gate** `zero_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,928.07.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-13 | `TPG` | 24 | — | $50.62 | +0.00 | $54.62 | +95.92 | +95.92 | +0.00 | +95.92 |
| 2026-08-13 | `TGTX` | 25 | — | $49.70 | +0.00 | $47.94 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | -40.74 | -40.74 | +0.00 | -40.74 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | — | +0.00 | -11.60 | -3.00 | — |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | — | +0.00 | -18.09 | -51.03 | — |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | — | +0.00 | +16.08 | +112.00 | — |
| 2026-08-14 | `TGTX` | 25 | $47.94 | $47.27 | -16.75 | — | +0.00 | -16.75 | -60.75 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | — | +0.00 | +4.24 | +74.20 | — |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | — | +0.00 | +15.96 | -24.78 | — |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | — | +0.00 | +46.29 | +185.16 | — |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | — | +0.00 | -11.13 | -21.73 | — |
| 2026-08-14 | `DAVE` | 3 | — | $330.91 | +0.00 | $334.57 | +10.98 | +10.98 | +0.00 | +10.98 |
| 2026-08-14 | `MARA` | 140 | — | $9.01 | +0.00 | $9.20 | +26.60 | +26.60 | +0.00 | +26.60 |
| 2026-08-14 | `LDI` | 1353 | — | $0.94 | +0.00 | $0.90 | -54.12 | -54.12 | +0.00 | -54.12 |
| 2026-08-14 | `BTBT` | 845 | — | $1.50 | +0.00 | $1.57 | +59.15 | +59.15 | +0.00 | +59.15 |
| 2026-08-14 | `BETR` | 85 | — | $14.80 | +0.00 | $13.73 | -90.95 | -90.95 | +0.00 | -90.95 |
| 2026-08-14 | `ANGX` | 294 | — | $4.31 | +0.00 | $4.37 | +17.64 | +17.64 | +0.00 | +17.64 |
| 2026-08-14 | `HYLN` | 303 | — | $4.18 | +0.00 | $4.06 | -36.36 | -36.36 | +0.00 | -36.36 |
| 2026-08-14 | `WDC` | 2 | — | $503.50 | +0.00 | $508.80 | +10.60 | +10.60 | +0.00 | +10.60 |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `MARA` | 140 | $9.20 | $9.22 | +2.80 | — | +0.00 | +2.80 | +29.40 | — |
| 2026-08-17 | `LDI` | 1353 | $0.90 | $0.91 | +13.53 | — | +0.00 | +13.53 | -40.59 | — |
| 2026-08-17 | `BTBT` | 845 | $1.57 | $1.52 | -42.25 | — | +0.00 | -42.25 | +16.90 | — |
| 2026-08-17 | `BETR` | 85 | $13.73 | $13.67 | -5.10 | — | +0.00 | -5.10 | -96.05 | — |
| 2026-08-17 | `ANGX` | 294 | $4.37 | $4.60 | +67.62 | — | +0.00 | +67.62 | +85.26 | — |
| 2026-08-17 | `HYLN` | 303 | $4.06 | $4.10 | +12.12 | — | +0.00 | +12.12 | -24.24 | — |
| 2026-08-17 | `WDC` | 2 | $508.80 | $525.53 | +33.46 | — | +0.00 | +33.46 | +44.06 | — |
| 2026-08-17 | `TMC` | 311 | — | $4.05 | +0.00 | $3.77 | -87.08 | -87.08 | +0.00 | -87.08 |
| 2026-08-17 | `TGB` | 149 | — | $8.46 | +0.00 | $8.77 | +46.19 | +46.19 | +0.00 | +46.19 |
| 2026-08-17 | `DNN` | 389 | — | $3.24 | +0.00 | $3.19 | -19.45 | -19.45 | +0.00 | -19.45 |
| 2026-08-17 | `CDNL` | 31 | — | $39.85 | +0.00 | $39.23 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-17 | `ABX` | 138 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `OCC` | 69 | — | $18.24 | +0.00 | $17.12 | -77.28 | -77.28 | +0.00 | -77.28 |
| 2026-08-17 | `ALM` | 77 | — | $16.20 | +0.00 | $16.36 | +12.32 | +12.32 | +0.00 | +12.32 |
| 2026-08-17 | `UMAC` | 38 | — | $32.55 | +0.00 | $30.15 | -91.20 | -91.20 | +0.00 | -91.20 |
| 2026-08-18 | `TMC` | 311 | $3.77 | $3.72 | -15.55 | — | +0.00 | -15.55 | -102.63 | — |
| 2026-08-18 | `TGB` | 149 | $8.77 | $8.55 | -32.78 | — | +0.00 | -32.78 | +13.41 | — |
| 2026-08-18 | `DNN` | 389 | $3.19 | $3.11 | -31.12 | — | +0.00 | -31.12 | -50.57 | — |
| 2026-08-18 | `CDNL` | 31 | $39.23 | $41.57 | +72.54 | — | +0.00 | +72.54 | +53.32 | — |
| 2026-08-18 | `ABX` | 138 | $9.12 | $9.03 | -12.42 | — | +0.00 | -12.42 | -12.42 | — |
| 2026-08-18 | `OCC` | 69 | $17.12 | $16.20 | -63.48 | — | +0.00 | -63.48 | -140.76 | — |
| 2026-08-18 | `ALM` | 77 | $16.36 | $15.78 | -44.66 | — | +0.00 | -44.66 | -32.34 | — |
| 2026-08-18 | `UMAC` | 38 | $30.15 | $28.59 | -59.28 | — | +0.00 | -59.28 | -150.48 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 58 | — | $20.55 | +0.00 | $21.19 | +37.12 | +37.12 | +0.00 | +37.12 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 58 | — | $20.65 | +0.00 | $21.11 | +26.68 | +26.68 | +0.00 | +26.68 |
| 2026-08-20 | `HDSN` | 208 | — | $5.77 | +0.00 | $5.57 | -41.60 | -41.60 | +0.00 | -41.60 |
| 2026-08-20 | `IAG` | 61 | — | $19.63 | +0.00 | $20.50 | +53.07 | +53.07 | +0.00 | +53.07 |
| 2026-08-20 | `KGC` | 40 | — | $29.63 | +0.00 | $31.43 | +72.00 | +72.00 | +0.00 | +72.00 |
| 2026-08-20 | `NFGC` | 687 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 58 | $21.19 | $21.90 | +41.18 | — | +0.00 | +41.18 | +78.30 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 58 | $21.11 | $21.75 | +37.12 | — | +0.00 | +37.12 | +63.80 | — |
| 2026-08-21 | `HDSN` | 208 | $5.57 | $5.67 | +20.80 | — | +0.00 | +20.80 | -20.80 | — |
| 2026-08-21 | `IAG` | 61 | $20.50 | $21.17 | +40.87 | — | +0.00 | +40.87 | +93.94 | — |
| 2026-08-21 | `KGC` | 40 | $31.43 | $32.17 | +29.60 | — | +0.00 | +29.60 | +101.60 | — |
| 2026-08-21 | `NFGC` | 687 | $1.75 | $1.79 | +27.48 | — | +0.00 | +27.48 | +27.48 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 73 | — | $17.20 | +0.00 | $16.65 | -40.15 | -40.15 | +0.00 | -40.15 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 112 | — | $11.13 | +0.00 | $13.45 | +259.84 | +259.84 | +0.00 | +259.84 |
| 2026-08-21 | `AUTL` | 509 | — | $2.47 | +0.00 | $2.41 | -30.54 | -30.54 | +0.00 | -30.54 |
| 2026-08-21 | `CRDL` | 651 | — | $1.93 | +0.00 | $1.86 | -45.57 | -45.57 | +0.00 | -45.57 |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `CYPH` | 952 | — | $1.32 | +0.00 | $1.42 | +95.20 | +95.20 | +0.00 | +95.20 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 73 | $16.65 | $16.57 | -5.84 | — | +0.00 | -5.84 | -45.99 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 112 | $13.45 | $13.33 | -13.44 | — | +0.00 | -13.44 | +246.40 | — |
| 2026-08-24 | `AUTL` | 509 | $2.41 | $2.40 | -5.09 | — | +0.00 | -5.09 | -35.63 | — |
| 2026-08-24 | `CRDL` | 651 | $1.86 | $1.88 | +13.02 | — | +0.00 | +13.02 | -32.55 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | — | +0.00 | -15.75 | -20.37 | — |
| 2026-08-24 | `CYPH` | 952 | $1.42 | $1.83 | +390.32 | — | +0.00 | +390.32 | +485.52 | — |
| 2026-08-25 | `MOS` | 55 | — | $23.77 | +0.00 | $24.27 | +27.50 | +27.50 | +0.00 | +27.50 |
| 2026-08-25 | `CRMD` | 158 | — | $8.35 | +0.00 | $8.56 | +33.18 | +33.18 | +0.00 | +33.18 |
| 2026-08-25 | `BMEA` | 812 | — | $1.63 | +0.00 | $1.73 | +81.20 | +81.20 | +0.00 | +81.20 |
| 2026-08-25 | `ALVO` | 252 | — | $5.24 | +0.00 | $5.05 | -47.88 | -47.88 | +0.00 | -47.88 |
| 2026-08-25 | `SUJA` | 150 | — | $8.79 | +0.00 | $9.33 | +81.00 | +81.00 | +0.00 | +81.00 |
| 2026-08-25 | `CYPH` | 848 | — | $1.56 | +0.00 | $1.64 | +67.84 | +67.84 | +0.00 | +67.84 |
| 2026-08-25 | `DEFT` | 2136 | — | $0.62 | +0.00 | $0.60 | -34.18 | -34.18 | +0.00 | -34.18 |
| 2026-08-25 | `ZURA` | 204 | — | $6.37 | +0.00 | $6.32 | -10.20 | -10.20 | +0.00 | -10.20 |
| 2026-08-26 | `MOS` | 55 | $24.27 | $24.84 | +31.35 | — | +0.00 | +31.35 | +58.85 | — |
| 2026-08-26 | `CRMD` | 158 | $8.56 | $8.60 | +6.32 | — | +0.00 | +6.32 | +39.50 | — |
| 2026-08-26 | `BMEA` | 812 | $1.73 | $1.75 | +20.30 | — | +0.00 | +20.30 | +101.50 | — |
| 2026-08-26 | `ALVO` | 252 | $5.05 | $4.98 | -17.64 | — | +0.00 | -17.64 | -65.52 | — |
| 2026-08-26 | `SUJA` | 150 | $9.33 | $9.39 | +9.00 | — | +0.00 | +9.00 | +90.00 | — |
| 2026-08-26 | `CYPH` | 848 | $1.64 | $1.60 | -33.92 | — | +0.00 | -33.92 | +33.92 | — |
| 2026-08-26 | `DEFT` | 2136 | $0.60 | $0.60 | -12.82 | — | +0.00 | -12.82 | -46.99 | — |
| 2026-08-26 | `ZURA` | 204 | $6.32 | $6.13 | -38.76 | — | +0.00 | -38.76 | -48.96 | — |
| 2026-08-26 | `USDE` | 1828 | — | $5.81 | +0.00 | $5.98 | +310.76 | +310.76 | +0.00 | +310.76 |
| 2026-08-27 | `USDE` | 1828 | $5.98 | $6.50 | +950.56 | — | +0.00 | +950.56 | +1261.32 | — |
| 2026-08-28 | `SIMO` | 5 | — | $252.24 | +0.00 | $245.81 | -32.15 | -32.15 | +0.00 | -32.15 |
| 2026-08-28 | `SMTC` | 10 | — | $141.76 | +0.00 | $131.17 | -105.90 | -105.90 | +0.00 | -105.90 |
| 2026-08-28 | `TTMI` | 12 | — | $122.81 | +0.00 | $118.65 | -49.92 | -49.92 | +0.00 | -49.92 |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `AVT` | 16 | — | $91.49 | +0.00 | $88.63 | -45.76 | -45.76 | +0.00 | -45.76 |
| 2026-08-28 | `CGNX` | 23 | — | $62.82 | +0.00 | $60.46 | -54.28 | -54.28 | +0.00 | -54.28 |
| 2026-08-28 | `COHR` | 5 | — | $289.44 | +0.00 | $279.20 | -51.20 | -51.20 | +0.00 | -51.20 |
| 2026-08-28 | `LSCC` | 12 | — | $119.76 | +0.00 | $114.40 | -64.32 | -64.32 | +0.00 | -64.32 |
| 2026-08-31 | `SIMO` | 5 | $245.81 | $247.05 | +6.20 | — | +0.00 | +6.20 | -25.95 | — |
| 2026-08-31 | `SMTC` | 10 | $131.17 | $132.30 | +11.30 | — | +0.00 | +11.30 | -94.60 | — |
| 2026-08-31 | `TTMI` | 12 | $118.65 | $118.83 | +2.16 | — | +0.00 | +2.16 | -47.76 | — |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `AVT` | 16 | $88.63 | $89.39 | +12.16 | — | +0.00 | +12.16 | -33.60 | — |
| 2026-08-31 | `CGNX` | 23 | $60.46 | $60.46 | +0.00 | — | +0.00 | +0.00 | -54.28 | — |
| 2026-08-31 | `COHR` | 5 | $279.20 | $280.25 | +5.25 | — | +0.00 | +5.25 | -45.95 | — |
| 2026-08-31 | `LSCC` | 12 | $114.40 | $115.56 | +13.92 | — | +0.00 | +13.92 | -50.40 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 27 | — | $52.88 | +0.00 | $52.46 | -11.34 | -11.34 | +0.00 | -11.34 |
| 2026-09-03 | `HRMY` | 33 | — | $42.93 | +0.00 | $41.86 | -35.31 | -35.31 | +0.00 | -35.31 |
| 2026-09-03 | `CABA` | 394 | — | $3.63 | +0.00 | $3.48 | -59.10 | -59.10 | +0.00 | -59.10 |
| 2026-09-03 | `VSTM` | 178 | — | $8.03 | +0.00 | $7.98 | -8.90 | -8.90 | +0.00 | -8.90 |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `ARCT` | 85 | — | $16.77 | +0.00 | $15.56 | -102.85 | -102.85 | +0.00 | -102.85 |
| 2026-09-03 | `SLN` | 96 | — | $14.85 | +0.00 | $14.79 | -5.76 | -5.76 | +0.00 | -5.76 |
| 2026-09-03 | `CRDL` | 657 | — | $2.18 | +0.00 | $2.16 | -13.14 | -13.14 | +0.00 | -13.14 |
| 2026-09-04 | `ATRC` | 27 | $52.46 | $52.03 | -11.61 | $51.52 | -13.77 | -25.38 | -22.95 | -36.72 |
| 2026-09-04 | `HRMY` | 33 | $41.86 | $41.50 | -11.88 | — | +0.00 | -11.88 | -47.19 | — |
| 2026-09-04 | `CABA` | 394 | $3.48 | $3.46 | -7.88 | $3.47 | +3.94 | -3.94 | -66.98 | -63.04 |
| 2026-09-04 | `VSTM` | 178 | $7.98 | $7.91 | -12.46 | — | +0.00 | -12.46 | -21.36 | — |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `ARCT` | 85 | $15.56 | $15.61 | +4.25 | — | +0.00 | +4.25 | -98.60 | — |
| 2026-09-04 | `SLN` | 96 | $14.79 | $14.63 | -15.36 | — | +0.00 | -15.36 | -21.12 | — |
| 2026-09-04 | `CRDL` | 657 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -13.14 | — |
| 2026-09-04 | `ALEC` | 551 | — | $2.52 | +0.00 | $2.46 | -33.06 | -33.06 | +0.00 | -33.06 |
| 2026-09-04 | `BHC` | 207 | — | $6.71 | +0.00 | $6.56 | -31.05 | -31.05 | +0.00 | -31.05 |
| 2026-09-04 | `BMEA` | 731 | — | $1.90 | +0.00 | $2.03 | +95.03 | +95.03 | +0.00 | +95.03 |
| 2026-09-04 | `OABI` | 290 | — | $4.78 | +0.00 | $4.33 | -130.50 | -130.50 | +0.00 | -130.50 |
| 2026-09-04 | `OPK` | 874 | — | $1.59 | +0.00 | $1.64 | +43.70 | +43.70 | +0.00 | +43.70 |
| 2026-09-04 | `VIR` | 120 | — | $11.31 | +0.00 | $11.38 | +9.00 | +9.00 | +0.00 | +9.00 |
| 2026-09-08 | `ATRC` | 27 | $51.52 | $54.31 | +75.33 | — | +0.00 | +75.33 | +38.61 | — |
| 2026-09-08 | `CABA` | 394 | $3.47 | $3.43 | -15.76 | — | +0.00 | -15.76 | -78.80 | — |
| 2026-09-08 | `ALEC` | 551 | $2.46 | $2.38 | -44.08 | — | +0.00 | -44.08 | -77.14 | — |
| 2026-09-08 | `BHC` | 207 | $6.56 | $6.57 | +2.07 | — | +0.00 | +2.07 | -28.98 | — |
| 2026-09-08 | `BMEA` | 731 | $2.03 | $2.00 | -21.93 | — | +0.00 | -21.93 | +73.10 | — |
| 2026-09-08 | `OABI` | 290 | $4.33 | $4.30 | -8.70 | — | +0.00 | -8.70 | -139.20 | — |
| 2026-09-08 | `OPK` | 874 | $1.64 | $1.63 | -8.74 | — | +0.00 | -8.74 | +34.96 | — |
| 2026-09-08 | `VIR` | 120 | $11.38 | $11.22 | -19.80 | — | +0.00 | -19.80 | -10.80 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +185.07 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | -56.46 | DAVE, MARA, LDI, BTBT, BETR, ANGX, HYLN, WDC | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $511.85 | $10,043.46 | DAVE×3, MARA×140, LDI×1353, BTBT×845, BETR×85, ANGX×294, HYLN×303, WDC×2 |
| 2026-08-17 | +2.25 | $511.85 | DAVE×3, MARA×140, LDI×1353, BTBT×845, BETR×85, ANGX×294, HYLN×303, WDC×2 | $10,132.75 | +89.29 | -235.72 | TMC, TGB, DNN, CDNL, ABX, OCC, ALM, UMAC | DAVE, MARA, LDI, BTBT, BETR, ANGX, HYLN, WDC | $48.87 | $9,830.37 | TMC×311, TGB×149, DNN×389, CDNL×31, ABX×138, OCC×69, ALM×77, UMAC×38 |
| 2026-08-18 | -6.20 | $48.87 | TMC×311, TGB×149, DNN×389, CDNL×31, ABX×138, OCC×69, ALM×77, UMAC×38 | $9,643.62 | -186.75 | +0.00 | — | TMC, TGB, DNN, CDNL, ABX, OCC, ALM, UMAC | $9,620.85 | $9,620.85 | — |
| 2026-08-19 | -7.20 | $9,620.85 | — | $9,620.85 | +0.00 | +0.00 | — | — | $9,620.85 | $9,620.85 | — |
| 2026-08-20 | +1.12 | $9,620.85 | — | $9,620.85 | +0.00 | +227.01 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $82.57 | $9,823.67 | AG×58, BHP×13, CDE×58, HDSN×208, IAG×61, KGC×40, NFGC×687, WPM×8 |
| 2026-08-21 | +3.25 | $82.57 | AG×58, BHP×13, CDE×58, HDSN×208, IAG×61, KGC×40, NFGC×687, WPM×8 | $10,083.49 | +259.82 | +250.86 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $218.76 | $10,272.00 | AU×10, AUPH×73, AEM×5, ARCT×112, AUTL×509, CRDL×651, CRSP×21, CYPH×952 |
| 2026-08-24 | -5.17 | $218.76 | AU×10, AUPH×73, AEM×5, ARCT×112, AUTL×509, CRDL×651, CRSP×21, CYPH×952 | $10,632.97 | +360.97 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,594.61 | $10,594.61 | — |
| 2026-08-25 | +1.80 | $10,594.61 | — | $10,594.61 | +0.00 | +198.46 | MOS, CRMD, BMEA, ALVO, SUJA, CYPH, DEFT, ZURA | — | $4.74 | $10,739.07 | MOS×55, CRMD×158, BMEA×812, ALVO×252, SUJA×150, CYPH×848, DEFT×2136, ZURA×204 |
| 2026-08-26 | +2.02 | $4.74 | MOS×55, CRMD×158, BMEA×812, ALVO×252, SUJA×150, CYPH×848, DEFT×2136, ZURA×204 | $10,702.90 | -36.17 | +310.76 | USDE | MOS, CRMD, BMEA, ALVO, SUJA, CYPH, DEFT, ZURA | $4.25 | $10,935.69 | USDE×1828 |
| 2026-08-27 | — | $4.25 | USDE×1828 | $11,886.25 | +950.56 | +0.00 | — | USDE | $11,862.27 | $11,862.27 | — |
| 2026-08-28 | +0.75 | $11,862.27 | — | $11,862.27 | +0.00 | -421.29 | SIMO, SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC | — | $602.91 | $11,424.80 | SIMO×5, SMTC×10, TTMI×12, KEYS×4, AVT×16, CGNX×23, COHR×5, LSCC×12 |
| 2026-08-31 | -5.85 | $602.91 | SIMO×5, SMTC×10, TTMI×12, KEYS×4, AVT×16, CGNX×23, COHR×5, LSCC×12 | $11,485.87 | +61.07 | +0.00 | — | SIMO, SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC | $11,469.53 | $11,469.53 | — |
| 2026-09-01 | -6.30 | $11,469.53 | — | $11,469.53 | -0.00 | +0.00 | — | — | $11,469.53 | $11,469.53 | — |
| 2026-09-02 | -3.83 | $11,469.53 | — | $11,469.53 | -0.00 | +0.00 | — | — | $11,469.53 | $11,469.53 | — |
| 2026-09-03 | -0.90 | $11,469.53 | — | $11,469.53 | -0.00 | -254.60 | ATRC, HRMY, CABA, VSTM, RVTY, ARCT, SLN, CRDL | — | $130.92 | $11,188.14 | ATRC×27, HRMY×33, CABA×394, VSTM×178, RVTY×10, ARCT×85, SLN×96, CRDL×657 |
| 2026-09-04 | +2.25 | $130.92 | ATRC×27, HRMY×33, CABA×394, VSTM×178, RVTY×10, ARCT×85, SLN×96, CRDL×657 | $11,127.20 | -60.94 | -56.71 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY, ARCT, SLN, CRDL | $3.24 | $11,014.03 | ATRC×27, CABA×394, ALEC×551, BHC×207, BMEA×731, OABI×290, OPK×874, VIR×120 |
| 2026-09-08 | -11.47 | $3.24 | ATRC×27, CABA×394, ALEC×551, BHC×207, BMEA×731, OABI×290, OPK×874, VIR×120 | $10,972.42 | -41.61 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $10,928.07 | $10,928.07 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | 16:00 close · cash $97.53 · equity $10,153.12 vs 09:30 $10,000.00 (+153.12; session marks +185.07) · 8 name(s) marked open→close (per-name table). BTSG×20 09:30 $59.80 → close $60.23 +8.60; IREN×27 09:30 $45.98 → close $44.76 -32.94; TPG×24 09:30 $50.62 → close $54.62 +95.92; TGTX×25 09:30 $49.70 → close $47.94 -44.00; SLS×106 09:30 $11.70 → close $12.36 +69.96; HIMS×42 09:30 $29.74 → close $28.77 -40.74; INO×1543 09:30 $0.81 → close $0.90 +138.87; TNDM×53 09:30 $23.33 → close $23.13 -10.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) · 8 name(s) re-marked at the open (per-name table). BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▼ -7.12 after sell → book $10,176.05; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | ▼ -55.19 after sell → book $10,173.96; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | ▲ +107.86 after sell → book $10,171.88; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | ▼ -64.90 after sell → book $10,169.80; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | ▲ +69.56 after sell → book $10,167.46; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | ▼ -29.03 after sell → book $10,165.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | ▲ +148.79 after sell → book $10,146.08; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | ▼ -26.05 after sell → book $10,143.91; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $9,149.18 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $7,885.37 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $6,600.87 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $5,322.47 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 85 | $14.80 | $2.25 | — | $4,062.23 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 294 | $4.31 | $3.79 | — | $2,791.29 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 303 | $4.18 | $3.91 | — | $1,520.85 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $511.85 | — | union ∩ white, no 🚨; gate zero_red=True; list probable; 🔵; ⚪; ret5=+7.9; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $511.85 | ▼ close $10,043.46 vs 09:30 $10,178.12 (session -56.46) | 16:00 close · cash $511.85 · equity $10,043.46 vs 09:30 $10,178.12 (-134.66; session marks -56.46) · 8 name(s) marked open→close (per-name table). DAVE×3 09:30 $330.91 → close $334.57 +10.98; MARA×140 09:30 $9.01 → close $9.20 +26.60; LDI×1353 09:30 $0.94 → close $0.90 -54.12; BTBT×845 09:30 $1.50 → close $1.57 +59.15; BETR×85 09:30 $14.80 → close $13.73 -90.95; ANGX×294 09:30 $4.31 → close $4.37 +17.64; HYLN×303 09:30 $4.18 → close $4.06 -36.36; WDC×2 09:30 $503.50 → close $508.80 +10.60 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $511.85 | ▲ 09:30 equity $10,132.75 vs yday $10,043.46 (+89.29) | 09:30 open · cash $511.85 (unchanged overnight, no fees) · equity $10,132.75 vs prior close $10,043.46 (+89.29) · 8 name(s) re-marked at the open (per-name table). DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; MARA×140 yday $9.20 → 09:30 $9.22 +2.80; LDI×1353 yday $0.90 → 09:30 $0.91 +13.53; BTBT×845 yday $1.57 → 09:30 $1.52 -42.25; BETR×85 yday $13.73 → 09:30 $13.67 -5.10; ANGX×294 yday $4.37 → 09:30 $4.60 +67.62; HYLN×303 yday $4.06 → 09:30 $4.10 +12.12; WDC×2 yday $508.80 → 09:30 $525.53 +33.46 | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $1,520.65 | ▲ +14.07 after sell → book $10,130.73; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $2,809.01 | ▲ +24.55 after sell → book $10,128.29; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1353 | $0.91 | $16.57 | $-73.89 | $4,019.61 | ▼ -73.89 after sell → book $10,111.72; vs 09:30 mark -16.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 845 | $1.52 | $11.05 | $-5.05 | $5,292.96 | ▼ -5.05 after sell → book $10,100.67; vs 09:30 mark -11.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 85 | $13.67 | $2.27 | $-100.56 | $6,452.64 | ▼ -100.56 after sell → book $10,098.40; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 294 | $4.60 | $3.85 | $+77.62 | $7,801.19 | ▲ +77.62 after sell → book $10,094.55; vs 09:30 mark -3.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 303 | $4.10 | $3.97 | $-32.12 | $9,039.52 | ▼ -32.12 after sell → book $10,090.58; vs 09:30 mark -3.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $10,088.57 | ▲ +40.05 after sell → book $10,088.57; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 311 | $4.05 | $4.01 | — | $8,825.00 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1261.07 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 149 | $8.46 | $2.44 | — | $7,562.03 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1261.07 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 389 | $3.24 | $5.02 | — | $6,296.65 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+0.3; leftover $1261.07 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $5,059.22 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1261.07 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 138 | $9.12 | $2.40 | — | $3,798.25 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1261.07 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 69 | $18.24 | $2.20 | — | $2,537.49 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1261.07 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $1,287.87 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1261.07 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 38 | $32.55 | $2.10 | — | $48.87 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1261.07 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.87 | ▼ close $9,830.37 vs 09:30 $10,132.75 (session -235.72) | 16:00 close · cash $48.87 · equity $9,830.37 vs 09:30 $10,132.75 (-302.38; session marks -235.72) · 8 name(s) marked open→close (per-name table). TMC×311 09:30 $4.05 → close $3.77 -87.08; TGB×149 09:30 $8.46 → close $8.77 +46.19; DNN×389 09:30 $3.24 → close $3.19 -19.45; CDNL×31 09:30 $39.85 → close $39.23 -19.22; ABX×138 09:30 $9.12 → close $9.12 +0.00; OCC×69 09:30 $18.24 → close $17.12 -77.28; ALM×77 09:30 $16.20 → close $16.36 +12.32; UMAC×38 09:30 $32.55 → close $30.15 -91.20 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.87 | ▼ 09:30 equity $9,643.62 vs yday $9,830.37 (-186.75) | 09:30 open · cash $48.87 (unchanged overnight, no fees) · equity $9,643.62 vs prior close $9,830.37 (-186.75) · 8 name(s) re-marked at the open (per-name table). TMC×311 yday $3.77 → 09:30 $3.72 -15.55; TGB×149 yday $8.77 → 09:30 $8.55 -32.78; DNN×389 yday $3.19 → 09:30 $3.11 -31.12; CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; ABX×138 yday $9.12 → 09:30 $9.03 -12.42; OCC×69 yday $17.12 → 09:30 $16.20 -63.48; ALM×77 yday $16.36 → 09:30 $15.78 -44.66; UMAC×38 yday $30.15 → 09:30 $28.59 -59.28 | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 311 | $3.72 | $4.07 | $-110.72 | $1,201.72 | ▼ -110.72 after sell → book $9,639.55; vs 09:30 mark -4.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 149 | $8.55 | $2.47 | $+8.50 | $2,473.19 | ▲ +8.50 after sell → book $9,637.07; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 389 | $3.11 | $5.09 | $-60.68 | $3,677.89 | ▼ -60.68 after sell → book $9,631.98; vs 09:30 mark -5.09 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $4,964.46 | ▲ +49.13 after sell → book $9,629.88; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 138 | $9.03 | $2.44 | $-17.26 | $6,208.16 | ▼ -17.26 after sell → book $9,627.44; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 69 | $16.20 | $2.22 | $-145.18 | $7,323.74 | ▼ -145.18 after sell → book $9,625.22; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $8,536.56 | ▼ -36.80 after sell → book $9,622.98; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 38 | $28.59 | $2.12 | $-154.71 | $9,620.85 | ▼ -154.71 after sell → book $9,620.85; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,620.85 | ▲ close $9,620.85 vs 09:30 $9,643.62 (session +0.00) | 16:00 close · cash $9,620.85 · no lots left · equity $9,620.85. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,620.85 | ▲ 09:30 equity $9,620.85 vs yday $9,620.85 (+0.00) | 09:30 open · cash $9,620.85 · no holdings · equity $9,620.85 vs prior close $9,620.85 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,620.85 | ▲ close $9,620.85 vs 09:30 $9,620.85 (session +0.00) | 16:00 close · cash $9,620.85 · no lots left · equity $9,620.85. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,620.85 | ▲ 09:30 equity $9,620.85 vs yday $9,620.85 (+0.00) | 09:30 open · cash $9,620.85 · no holdings · equity $9,620.85 vs prior close $9,620.85 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,426.79 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,241.63 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $6,041.77 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 208 | $5.77 | $2.68 | — | $4,838.92 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $3,639.32 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $2,452.01 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 687 | $1.75 | $8.86 | — | $1,240.90 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $82.57 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.57 | ▲ close $9,823.67 vs 09:30 $9,620.85 (session +227.01) | 16:00 close · cash $82.57 · equity $9,823.67 vs 09:30 $9,620.85 (+202.82; session marks +227.01) · 8 name(s) marked open→close (per-name table). AG×58 09:30 $20.55 → close $21.19 +37.12; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×58 09:30 $20.65 → close $21.11 +26.68; HDSN×208 09:30 $5.77 → close $5.57 -41.60; IAG×61 09:30 $19.63 → close $20.50 +53.07; KGC×40 09:30 $29.63 → close $31.43 +72.00; NFGC×687 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.57 | ▲ 09:30 equity $10,083.49 vs yday $9,823.67 (+259.82) | 09:30 open · cash $82.57 (unchanged overnight, no fees) · equity $10,083.49 vs prior close $9,823.67 (+259.82) · 8 name(s) re-marked at the open (per-name table). AG×58 yday $21.19 → 09:30 $21.90 +41.18; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×58 yday $21.11 → 09:30 $21.75 +37.12; HDSN×208 yday $5.57 → 09:30 $5.67 +20.80; IAG×61 yday $20.50 → 09:30 $21.17 +40.87; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×687 yday $1.75 → 09:30 $1.79 +27.48; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,350.58 | ▲ +73.95 after sell → book $10,081.30; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,592.89 | ▲ +57.15 after sell → book $10,079.25; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 58 | $21.75 | $2.18 | $+59.45 | $3,852.21 | ▲ +59.45 after sell → book $10,077.07; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 208 | $5.67 | $2.73 | $-26.21 | $5,028.84 | ▼ -26.21 after sell → book $10,074.34; vs 09:30 mark -2.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $6,318.02 | ▲ +89.57 after sell → book $10,072.15; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $7,602.69 | ▲ +97.36 after sell → book $10,070.02; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 687 | $1.79 | $8.99 | $+9.63 | $8,823.43 | ▲ +9.63 after sell → book $10,061.03; vs 09:30 mark -8.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,059.00 | ▲ +77.23 after sell → book $10,059.00; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,862.68 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 73 | $17.20 | $2.21 | — | $7,604.87 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,521.36 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 112 | $11.13 | $2.33 | — | $5,272.48 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 509 | $2.47 | $6.57 | — | $4,008.68 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 651 | $1.93 | $8.40 | — | $2,743.85 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,487.68 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 952 | $1.32 | $12.28 | — | $218.76 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $218.76 | ▲ close $10,272.00 vs 09:30 $10,083.49 (session +250.86) | 16:00 close · cash $218.76 · equity $10,272.00 vs 09:30 $10,083.49 (+188.51; session marks +250.86) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×73 09:30 $17.20 → close $16.65 -40.15; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×112 09:30 $11.13 → close $13.45 +259.84; AUTL×509 09:30 $2.47 → close $2.41 -30.54; CRDL×651 09:30 $1.93 → close $1.86 -45.57; CRSP×21 09:30 $59.72 → close $59.50 -4.62; CYPH×952 09:30 $1.32 → close $1.42 +95.20 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $218.76 | ▲ 09:30 equity $10,632.97 vs yday $10,272.00 (+360.97) | 09:30 open · cash $218.76 (unchanged overnight, no fees) · equity $10,632.97 vs prior close $10,272.00 (+360.97) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×73 yday $16.65 → 09:30 $16.57 -5.84; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×112 yday $13.45 → 09:30 $13.33 -13.44; AUTL×509 yday $2.41 → 09:30 $2.40 -5.09; CRDL×651 yday $1.86 → 09:30 $1.88 +13.02; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; CYPH×952 yday $1.42 → 09:30 $1.83 +390.32 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,421.82 | ▲ +6.74 after sell → book $10,630.93; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 73 | $16.57 | $2.23 | $-50.43 | $2,629.20 | ▼ -50.43 after sell → book $10,628.70; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,712.32 | ▼ -0.38 after sell → book $10,626.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 112 | $13.33 | $2.36 | $+241.72 | $5,202.93 | ▲ +241.72 after sell → book $10,624.32; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 509 | $2.40 | $6.66 | $-48.86 | $6,417.86 | ▼ -48.86 after sell → book $10,617.65; vs 09:30 mark -6.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 651 | $1.88 | $8.52 | $-49.46 | $7,633.23 | ▼ -49.46 after sell → book $10,609.14; vs 09:30 mark -8.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $8,864.91 | ▼ -24.50 after sell → book $10,607.07; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 952 | $1.83 | $12.45 | $+460.79 | $10,594.61 | ▲ +460.79 after sell → book $10,594.61; vs 09:30 mark -12.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,594.61 | ▲ close $10,594.61 vs 09:30 $10,632.97 (session +0.00) | 16:00 close · cash $10,594.61 · no lots left · equity $10,594.61. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,594.61 | ▲ 09:30 equity $10,594.61 vs yday $10,594.61 (+0.00) | 09:30 open · cash $10,594.61 · no holdings · equity $10,594.61 vs prior close $10,594.61 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $23.77 | $2.15 | — | $9,285.11 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.0; leftover $1324.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 158 | $8.35 | $2.46 | — | $7,963.34 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1324.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 812 | $1.63 | $10.47 | — | $6,629.31 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1324.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 252 | $5.24 | $3.25 | — | $5,305.58 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1324.33 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 150 | $8.79 | $2.44 | — | $3,984.64 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1324.33 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 848 | $1.56 | $10.94 | — | $2,650.82 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1324.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2136 | $0.62 | $19.65 | — | $1,306.85 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1324.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 204 | $6.37 | $2.63 | — | $4.74 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1324.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.74 | ▲ close $10,739.07 vs 09:30 $10,594.61 (session +198.46) | 16:00 close · cash $4.74 · equity $10,739.07 vs 09:30 $10,594.61 (+144.46; session marks +198.46) · 8 name(s) marked open→close (per-name table). MOS×55 09:30 $23.77 → close $24.27 +27.50; CRMD×158 09:30 $8.35 → close $8.56 +33.18; BMEA×812 09:30 $1.63 → close $1.73 +81.20; ALVO×252 09:30 $5.24 → close $5.05 -47.88; SUJA×150 09:30 $8.79 → close $9.33 +81.00; CYPH×848 09:30 $1.56 → close $1.64 +67.84; DEFT×2136 09:30 $0.62 → close $0.60 -34.18; ZURA×204 09:30 $6.37 → close $6.32 -10.20 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.74 | ▼ 09:30 equity $10,702.90 vs yday $10,739.07 (-36.17) | 09:30 open · cash $4.74 (unchanged overnight, no fees) · equity $10,702.90 vs prior close $10,739.07 (-36.17) · 8 name(s) re-marked at the open (per-name table). MOS×55 yday $24.27 → 09:30 $24.84 +31.35; CRMD×158 yday $8.56 → 09:30 $8.60 +6.32; BMEA×812 yday $1.73 → 09:30 $1.75 +20.30; ALVO×252 yday $5.05 → 09:30 $4.98 -17.64; SUJA×150 yday $9.33 → 09:30 $9.39 +9.00; CYPH×848 yday $1.64 → 09:30 $1.60 -33.92; DEFT×2136 yday $0.60 → 09:30 $0.60 -12.82; ZURA×204 yday $6.32 → 09:30 $6.13 -38.76 | — |
| 2026-08-26 09:30 ET | **SELL** | `MOS` | 55 | $24.84 | $2.18 | $+54.52 | $1,368.76 | ▲ +54.52 after sell → book $10,700.73; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 158 | $8.60 | $2.50 | $+34.53 | $2,725.06 | ▲ +34.53 after sell → book $10,698.23; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 812 | $1.75 | $10.62 | $+80.40 | $4,139.50 | ▲ +80.40 after sell → book $10,687.61; vs 09:30 mark -10.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 252 | $4.98 | $3.30 | $-72.07 | $5,391.16 | ▼ -72.07 after sell → book $10,684.30; vs 09:30 mark -3.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUJA` | 150 | $9.39 | $2.48 | $+85.08 | $6,797.18 | ▲ +85.08 after sell → book $10,681.83; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 848 | $1.60 | $11.09 | $+11.89 | $8,142.89 | ▲ +11.89 after sell → book $10,670.74; vs 09:30 mark -11.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DEFT` | 2136 | $0.60 | $19.55 | $-86.19 | $9,400.67 | ▼ -86.19 after sell → book $10,651.19; vs 09:30 mark -19.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 204 | $6.13 | $2.68 | $-54.27 | $10,648.52 | ▼ -54.27 after sell → book $10,648.52; vs 09:30 mark -2.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 1828 | $5.81 | $23.58 | — | $4.25 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $10648.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.25 | ▲ close $10,935.69 vs 09:30 $10,702.90 (session +310.76) | 16:00 close · cash $4.25 · equity $10,935.69 vs 09:30 $10,702.90 (+232.79; session marks +310.76) · 1 name(s) marked open→close (per-name table). USDE×1828 09:30 $5.81 → close $5.98 +310.76 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.25 | ▲ 09:30 equity $11,886.25 vs yday $10,935.69 (+950.56) | 09:30 open · cash $4.25 (unchanged overnight, no fees) · equity $11,886.25 vs prior close $10,935.69 (+950.56) · 1 name(s) re-marked at the open (per-name table). USDE×1828 yday $5.98 → 09:30 $6.50 +950.56 | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 1828 | $6.50 | $23.98 | $+1213.76 | $11,862.27 | ▲ +1,213.76 after sell → book $11,862.27; vs 09:30 mark -23.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,862.27 | ▲ close $11,862.27 vs 09:30 $11,886.25 (session +0.00) | 16:00 close · cash $11,862.27 · no lots left · equity $11,862.27. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,862.27 | ▲ 09:30 equity $11,862.27 vs yday $11,862.27 (+0.00) | 09:30 open · cash $11,862.27 · no holdings · equity $11,862.27 vs prior close $11,862.27 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $10,599.07 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1482.78 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 10 | $141.76 | $2.02 | — | $9,179.45 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1482.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 12 | $122.81 | $2.03 | — | $7,703.70 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1482.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $6,404.06 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1482.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 16 | $91.49 | $2.04 | — | $4,938.18 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1482.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 23 | $62.82 | $2.06 | — | $3,491.26 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1482.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 5 | $289.44 | $2.00 | — | $2,042.06 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1482.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 12 | $119.76 | $2.03 | — | $602.91 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1482.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $602.91 | ▼ close $11,424.80 vs 09:30 $11,862.27 (session -421.29) | 16:00 close · cash $602.91 · equity $11,424.80 vs 09:30 $11,862.27 (-437.47; session marks -421.29) · 8 name(s) marked open→close (per-name table). SIMO×5 09:30 $252.24 → close $245.81 -32.15; SMTC×10 09:30 $141.76 → close $131.17 -105.90; TTMI×12 09:30 $122.81 → close $118.65 -49.92; KEYS×4 09:30 $324.41 → close $319.97 -17.76; AVT×16 09:30 $91.49 → close $88.63 -45.76; CGNX×23 09:30 $62.82 → close $60.46 -54.28; COHR×5 09:30 $289.44 → close $279.20 -51.20; LSCC×12 09:30 $119.76 → close $114.40 -64.32 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $602.91 | ▲ 09:30 equity $11,485.87 vs yday $11,424.80 (+61.07) | 09:30 open · cash $602.91 (unchanged overnight, no fees) · equity $11,485.87 vs prior close $11,424.80 (+61.07) · 8 name(s) re-marked at the open (per-name table). SIMO×5 yday $245.81 → 09:30 $247.05 +6.20; SMTC×10 yday $131.17 → 09:30 $132.30 +11.30; TTMI×12 yday $118.65 → 09:30 $118.83 +2.16; KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; AVT×16 yday $88.63 → 09:30 $89.39 +12.16; CGNX×23 yday $60.46 → 09:30 $60.46 +0.00; COHR×5 yday $279.20 → 09:30 $280.25 +5.25; LSCC×12 yday $114.40 → 09:30 $115.56 +13.92 | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $1,836.14 | ▼ -29.98 after sell → book $11,483.85; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 10 | $132.30 | $2.04 | $-98.66 | $3,157.10 | ▼ -98.66 after sell → book $11,481.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 12 | $118.83 | $2.05 | $-51.83 | $4,581.01 | ▼ -51.83 after sell → book $11,479.76; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $5,868.95 | ▼ -11.70 after sell → book $11,477.74; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 16 | $89.39 | $2.06 | $-37.70 | $7,297.13 | ▼ -37.70 after sell → book $11,475.68; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 23 | $60.46 | $2.08 | $-58.42 | $8,685.63 | ▼ -58.42 after sell → book $11,473.60; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 5 | $280.25 | $2.03 | $-49.98 | $10,084.85 | ▼ -49.98 after sell → book $11,471.57; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 12 | $115.56 | $2.05 | $-54.47 | $11,469.53 | ▼ -54.47 after sell → book $11,469.53; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,469.53 | ▲ close $11,469.53 vs 09:30 $11,485.87 (session +0.00) | 16:00 close · cash $11,469.53 · no lots left · equity $11,469.53. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,469.53 | ▲ 09:30 equity $11,469.53 vs yday $11,469.53 (-0.00) | 09:30 open · cash $11,469.53 · no holdings · equity $11,469.53 vs prior close $11,469.53 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,469.53 | ▲ close $11,469.53 vs 09:30 $11,469.53 (session +0.00) | 16:00 close · cash $11,469.53 · no lots left · equity $11,469.53. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,469.53 | ▲ 09:30 equity $11,469.53 vs yday $11,469.53 (-0.00) | 09:30 open · cash $11,469.53 · no holdings · equity $11,469.53 vs prior close $11,469.53 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,469.53 | ▲ close $11,469.53 vs 09:30 $11,469.53 (session +0.00) | 16:00 close · cash $11,469.53 · no lots left · equity $11,469.53. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,469.53 | ▲ 09:30 equity $11,469.53 vs yday $11,469.53 (-0.00) | 09:30 open · cash $11,469.53 · no holdings · equity $11,469.53 vs prior close $11,469.53 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $52.88 | $2.07 | — | $10,039.69 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1433.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 33 | $42.93 | $2.09 | — | $8,620.92 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1433.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 394 | $3.63 | $5.08 | — | $7,185.61 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1433.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 178 | $8.03 | $2.52 | — | $5,753.75 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1433.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,427.23 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1433.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 85 | $16.77 | $2.25 | — | $2,999.53 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1433.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 96 | $14.85 | $2.28 | — | $1,571.66 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1433.69 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 657 | $2.18 | $8.48 | — | $130.92 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1433.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.92 | ▼ close $11,188.14 vs 09:30 $11,469.53 (session -254.60) | 16:00 close · cash $130.92 · equity $11,188.14 vs 09:30 $11,469.53 (-281.39; session marks -254.60) · 8 name(s) marked open→close (per-name table). ATRC×27 09:30 $52.88 → close $52.46 -11.34; HRMY×33 09:30 $42.93 → close $41.86 -35.31; CABA×394 09:30 $3.63 → close $3.48 -59.10; VSTM×178 09:30 $8.03 → close $7.98 -8.90; RVTY×10 09:30 $132.45 → close $130.63 -18.20; ARCT×85 09:30 $16.77 → close $15.56 -102.85; SLN×96 09:30 $14.85 → close $14.79 -5.76; CRDL×657 09:30 $2.18 → close $2.16 -13.14 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.92 | ▼ 09:30 equity $11,127.20 vs yday $11,188.14 (-60.94) | 09:30 open · cash $130.92 (unchanged overnight, no fees) · equity $11,127.20 vs prior close $11,188.14 (-60.94) · 8 name(s) re-marked at the open (per-name table). ATRC×27 yday $52.46 → 09:30 $52.03 -11.61; HRMY×33 yday $41.86 → 09:30 $41.50 -11.88; CABA×394 yday $3.48 → 09:30 $3.46 -7.88; VSTM×178 yday $7.98 → 09:30 $7.91 -12.46; RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; ARCT×85 yday $15.56 → 09:30 $15.61 +4.25; SLN×96 yday $14.79 → 09:30 $14.63 -15.36; CRDL×657 yday $2.16 → 09:30 $2.16 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 33 | $41.50 | $2.11 | $-51.39 | $1,498.31 | ▼ -51.39 after sell → book $11,125.09; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 178 | $7.91 | $2.56 | $-26.45 | $2,903.73 | ▼ -26.45 after sell → book $11,122.53; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $4,201.99 | ▼ -28.26 after sell → book $11,120.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 85 | $15.61 | $2.27 | $-103.11 | $5,526.57 | ▼ -103.11 after sell → book $11,118.22; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 96 | $14.63 | $2.31 | $-25.70 | $6,928.74 | ▼ -25.70 after sell → book $11,115.91; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 657 | $2.16 | $8.60 | $-30.21 | $8,339.26 | ▼ -30.21 after sell → book $11,107.31; vs 09:30 mark -8.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 551 | $2.52 | $7.11 | — | $6,943.64 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1389.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 207 | $6.71 | $2.67 | — | $5,552.00 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1389.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 731 | $1.90 | $9.43 | — | $4,153.67 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1389.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 290 | $4.78 | $3.74 | — | $2,763.73 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1389.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 874 | $1.59 | $11.27 | — | $1,362.79 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1389.88 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 120 | $11.31 | $2.35 | — | $3.24 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1389.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.24 | ▼ close $11,014.03 vs 09:30 $11,127.20 (session -56.71) | 16:00 close · cash $3.24 · equity $11,014.03 vs 09:30 $11,127.20 (-113.17; session marks -56.71) · 8 name(s) marked open→close (per-name table). ATRC×27 09:30 $52.03 → close $51.52 -13.77; CABA×394 09:30 $3.46 → close $3.47 +3.94; ALEC×551 09:30 $2.52 → close $2.46 -33.06; BHC×207 09:30 $6.71 → close $6.56 -31.05; BMEA×731 09:30 $1.90 → close $2.03 +95.03; OABI×290 09:30 $4.78 → close $4.33 -130.50; OPK×874 09:30 $1.59 → close $1.64 +43.70; VIR×120 09:30 $11.31 → close $11.38 +9.00 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.24 | ▼ 09:30 equity $10,972.42 vs yday $11,014.03 (-41.61) | 09:30 open · cash $3.24 (unchanged overnight, no fees) · equity $10,972.42 vs prior close $11,014.03 (-41.61) · 8 name(s) re-marked at the open (per-name table). ATRC×27 yday $51.52 → 09:30 $54.31 +75.33; CABA×394 yday $3.47 → 09:30 $3.43 -15.76; ALEC×551 yday $2.46 → 09:30 $2.38 -44.08; BHC×207 yday $6.56 → 09:30 $6.57 +2.07; BMEA×731 yday $2.03 → 09:30 $2.00 -21.93; OABI×290 yday $4.33 → 09:30 $4.30 -8.70; OPK×874 yday $1.64 → 09:30 $1.63 -8.74; VIR×120 yday $11.38 → 09:30 $11.22 -19.80 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 27 | $54.31 | $2.09 | $+34.45 | $1,467.52 | ▲ +34.45 after sell → book $10,970.33; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 394 | $3.43 | $5.16 | $-89.04 | $2,813.78 | ▼ -89.04 after sell → book $10,965.17; vs 09:30 mark -5.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 551 | $2.38 | $7.21 | $-91.46 | $4,117.95 | ▼ -91.46 after sell → book $10,957.96; vs 09:30 mark -7.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 207 | $6.57 | $2.72 | $-34.37 | $5,475.22 | ▼ -34.37 after sell → book $10,955.24; vs 09:30 mark -2.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 731 | $2.00 | $9.56 | $+54.11 | $6,927.66 | ▲ +54.11 after sell → book $10,945.68; vs 09:30 mark -9.56 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 290 | $4.30 | $3.80 | $-146.74 | $8,170.86 | ▼ -146.74 after sell → book $10,941.88; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 874 | $1.63 | $11.43 | $+12.25 | $9,584.05 | ▲ +12.25 after sell → book $10,930.45; vs 09:30 mark -11.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 120 | $11.22 | $2.38 | $-15.53 | $10,928.07 | ▼ -15.53 after sell → book $10,928.07; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,928.07 | ▲ close $10,928.07 vs 09:30 $10,972.42 (session +0.00) | 16:00 close · cash $10,928.07 · no lots left · equity $10,928.07. | — |
