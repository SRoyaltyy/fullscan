# Factor mine action — `union_white_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ white, no 🚨

Cash book **+8.27%** ($10,827) · signal-only (no cash/fees) was +1.57%. Starts YES **10/19**. Fills 142 · skips 0 · realized $+826.86.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,826.85.

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
| 2026-08-14 | `SLG` | 22 | — | $57.61 | +0.00 | $56.09 | -33.44 | -33.44 | +0.00 | -33.44 |
| 2026-08-14 | `MARA` | 140 | — | $9.01 | +0.00 | $9.20 | +26.60 | +26.60 | +0.00 | +26.60 |
| 2026-08-14 | `LDI` | 1353 | — | $0.94 | +0.00 | $0.90 | -54.12 | -54.12 | +0.00 | -54.12 |
| 2026-08-14 | `BTBT` | 845 | — | $1.50 | +0.00 | $1.57 | +59.15 | +59.15 | +0.00 | +59.15 |
| 2026-08-14 | `BETR` | 85 | — | $14.80 | +0.00 | $13.73 | -90.95 | -90.95 | +0.00 | -90.95 |
| 2026-08-14 | `ANGX` | 294 | — | $4.31 | +0.00 | $4.37 | +17.64 | +17.64 | +0.00 | +17.64 |
| 2026-08-14 | `HYLN` | 303 | — | $4.18 | +0.00 | $4.06 | -36.36 | -36.36 | +0.00 | -36.36 |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `SLG` | 22 | $56.09 | $55.37 | -15.84 | — | +0.00 | -15.84 | -49.28 | — |
| 2026-08-17 | `MARA` | 140 | $9.20 | $9.22 | +2.80 | — | +0.00 | +2.80 | +29.40 | — |
| 2026-08-17 | `LDI` | 1353 | $0.90 | $0.91 | +13.53 | — | +0.00 | +13.53 | -40.59 | — |
| 2026-08-17 | `BTBT` | 845 | $1.57 | $1.52 | -42.25 | — | +0.00 | -42.25 | +16.90 | — |
| 2026-08-17 | `BETR` | 85 | $13.73 | $13.67 | -5.10 | — | +0.00 | -5.10 | -96.05 | — |
| 2026-08-17 | `ANGX` | 294 | $4.37 | $4.60 | +67.62 | — | +0.00 | +67.62 | +85.26 | — |
| 2026-08-17 | `HYLN` | 303 | $4.06 | $4.10 | +12.12 | — | +0.00 | +12.12 | -24.24 | — |
| 2026-08-17 | `TMC` | 308 | — | $4.05 | +0.00 | $3.77 | -86.24 | -86.24 | +0.00 | -86.24 |
| 2026-08-17 | `TGB` | 147 | — | $8.46 | +0.00 | $8.77 | +45.57 | +45.57 | +0.00 | +45.57 |
| 2026-08-17 | `DNN` | 385 | — | $3.24 | +0.00 | $3.19 | -19.25 | -19.25 | +0.00 | -19.25 |
| 2026-08-17 | `CDNL` | 31 | — | $39.85 | +0.00 | $39.23 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-17 | `ABX` | 136 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `OCC` | 68 | — | $18.24 | +0.00 | $17.12 | -76.16 | -76.16 | +0.00 | -76.16 |
| 2026-08-17 | `ALM` | 77 | — | $16.20 | +0.00 | $16.36 | +12.32 | +12.32 | +0.00 | +12.32 |
| 2026-08-17 | `UMAC` | 38 | — | $32.55 | +0.00 | $30.15 | -91.20 | -91.20 | +0.00 | -91.20 |
| 2026-08-18 | `TMC` | 308 | $3.77 | $3.72 | -15.40 | — | +0.00 | -15.40 | -101.64 | — |
| 2026-08-18 | `TGB` | 147 | $8.77 | $8.55 | -32.34 | — | +0.00 | -32.34 | +13.23 | — |
| 2026-08-18 | `DNN` | 385 | $3.19 | $3.11 | -30.80 | — | +0.00 | -30.80 | -50.05 | — |
| 2026-08-18 | `CDNL` | 31 | $39.23 | $41.57 | +72.54 | — | +0.00 | +72.54 | +53.32 | — |
| 2026-08-18 | `ABX` | 136 | $9.12 | $9.03 | -12.24 | — | +0.00 | -12.24 | -12.24 | — |
| 2026-08-18 | `OCC` | 68 | $17.12 | $16.20 | -62.56 | — | +0.00 | -62.56 | -138.72 | — |
| 2026-08-18 | `ALM` | 77 | $16.36 | $15.78 | -44.66 | — | +0.00 | -44.66 | -32.34 | — |
| 2026-08-18 | `UMAC` | 38 | $30.15 | $28.59 | -59.28 | — | +0.00 | -59.28 | -150.48 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 57 | — | $20.55 | +0.00 | $21.19 | +36.48 | +36.48 | +0.00 | +36.48 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 57 | — | $20.65 | +0.00 | $21.11 | +26.22 | +26.22 | +0.00 | +26.22 |
| 2026-08-20 | `HDSN` | 206 | — | $5.77 | +0.00 | $5.57 | -41.20 | -41.20 | +0.00 | -41.20 |
| 2026-08-20 | `IAG` | 60 | — | $19.63 | +0.00 | $20.50 | +52.20 | +52.20 | +0.00 | +52.20 |
| 2026-08-20 | `KGC` | 40 | — | $29.63 | +0.00 | $31.43 | +72.00 | +72.00 | +0.00 | +72.00 |
| 2026-08-20 | `NFGC` | 680 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 57 | $21.19 | $21.90 | +40.47 | — | +0.00 | +40.47 | +76.95 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 57 | $21.11 | $21.75 | +36.48 | — | +0.00 | +36.48 | +62.70 | — |
| 2026-08-21 | `HDSN` | 206 | $5.57 | $5.67 | +20.60 | — | +0.00 | +20.60 | -20.60 | — |
| 2026-08-21 | `IAG` | 60 | $20.50 | $21.17 | +40.20 | — | +0.00 | +40.20 | +92.40 | — |
| 2026-08-21 | `KGC` | 40 | $31.43 | $32.17 | +29.60 | — | +0.00 | +29.60 | +101.60 | — |
| 2026-08-21 | `NFGC` | 680 | $1.75 | $1.79 | +27.20 | — | +0.00 | +27.20 | +27.20 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 72 | — | $17.20 | +0.00 | $16.65 | -39.60 | -39.60 | +0.00 | -39.60 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 111 | — | $11.13 | +0.00 | $13.45 | +257.52 | +257.52 | +0.00 | +257.52 |
| 2026-08-21 | `AUTL` | 504 | — | $2.47 | +0.00 | $2.41 | -30.24 | -30.24 | +0.00 | -30.24 |
| 2026-08-21 | `CRDL` | 645 | — | $1.93 | +0.00 | $1.86 | -45.15 | -45.15 | +0.00 | -45.15 |
| 2026-08-21 | `CRSP` | 20 | — | $59.72 | +0.00 | $59.50 | -4.40 | -4.40 | +0.00 | -4.40 |
| 2026-08-21 | `CYPH` | 943 | — | $1.32 | +0.00 | $1.42 | +94.30 | +94.30 | +0.00 | +94.30 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 72 | $16.65 | $16.57 | -5.76 | — | +0.00 | -5.76 | -45.36 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 111 | $13.45 | $13.33 | -13.32 | — | +0.00 | -13.32 | +244.20 | — |
| 2026-08-24 | `AUTL` | 504 | $2.41 | $2.40 | -5.04 | — | +0.00 | -5.04 | -35.28 | — |
| 2026-08-24 | `CRDL` | 645 | $1.86 | $1.88 | +12.90 | — | +0.00 | +12.90 | -32.25 | — |
| 2026-08-24 | `CRSP` | 20 | $59.50 | $58.75 | -15.00 | — | +0.00 | -15.00 | -19.40 | — |
| 2026-08-24 | `CYPH` | 943 | $1.42 | $1.83 | +386.63 | — | +0.00 | +386.63 | +480.93 | — |
| 2026-08-25 | `MOS` | 55 | — | $23.77 | +0.00 | $24.27 | +27.50 | +27.50 | +0.00 | +27.50 |
| 2026-08-25 | `CRMD` | 157 | — | $8.35 | +0.00 | $8.56 | +32.97 | +32.97 | +0.00 | +32.97 |
| 2026-08-25 | `BMEA` | 804 | — | $1.63 | +0.00 | $1.73 | +80.40 | +80.40 | +0.00 | +80.40 |
| 2026-08-25 | `ALVO` | 250 | — | $5.24 | +0.00 | $5.05 | -47.50 | -47.50 | +0.00 | -47.50 |
| 2026-08-25 | `SUJA` | 149 | — | $8.79 | +0.00 | $9.33 | +80.46 | +80.46 | +0.00 | +80.46 |
| 2026-08-25 | `CYPH` | 841 | — | $1.56 | +0.00 | $1.64 | +67.28 | +67.28 | +0.00 | +67.28 |
| 2026-08-25 | `DEFT` | 2116 | — | $0.62 | +0.00 | $0.60 | -33.86 | -33.86 | +0.00 | -33.86 |
| 2026-08-25 | `ZURA` | 199 | — | $6.37 | +0.00 | $6.32 | -9.95 | -9.95 | +0.00 | -9.95 |
| 2026-08-26 | `MOS` | 55 | $24.27 | $24.84 | +31.35 | — | +0.00 | +31.35 | +58.85 | — |
| 2026-08-26 | `CRMD` | 157 | $8.56 | $8.60 | +6.28 | — | +0.00 | +6.28 | +39.25 | — |
| 2026-08-26 | `BMEA` | 804 | $1.73 | $1.75 | +20.10 | — | +0.00 | +20.10 | +100.50 | — |
| 2026-08-26 | `ALVO` | 250 | $5.05 | $4.98 | -17.50 | — | +0.00 | -17.50 | -65.00 | — |
| 2026-08-26 | `SUJA` | 149 | $9.33 | $9.39 | +8.94 | — | +0.00 | +8.94 | +89.40 | — |
| 2026-08-26 | `CYPH` | 841 | $1.64 | $1.60 | -33.64 | — | +0.00 | -33.64 | +33.64 | — |
| 2026-08-26 | `DEFT` | 2116 | $0.60 | $0.60 | -12.70 | — | +0.00 | -12.70 | -46.55 | — |
| 2026-08-26 | `ZURA` | 199 | $6.32 | $6.13 | -37.81 | — | +0.00 | -37.81 | -47.76 | — |
| 2026-08-26 | `USDE` | 1812 | — | $5.81 | +0.00 | $5.98 | +308.04 | +308.04 | +0.00 | +308.04 |
| 2026-08-27 | `USDE` | 1812 | $5.98 | $6.50 | +942.24 | — | +0.00 | +942.24 | +1250.28 | — |
| 2026-08-28 | `SIMO` | 5 | — | $252.24 | +0.00 | $245.81 | -32.15 | -32.15 | +0.00 | -32.15 |
| 2026-08-28 | `SMTC` | 10 | — | $141.76 | +0.00 | $131.17 | -105.90 | -105.90 | +0.00 | -105.90 |
| 2026-08-28 | `TTMI` | 11 | — | $122.81 | +0.00 | $118.65 | -45.76 | -45.76 | +0.00 | -45.76 |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `AVT` | 16 | — | $91.49 | +0.00 | $88.63 | -45.76 | -45.76 | +0.00 | -45.76 |
| 2026-08-28 | `CGNX` | 23 | — | $62.82 | +0.00 | $60.46 | -54.28 | -54.28 | +0.00 | -54.28 |
| 2026-08-28 | `COHR` | 5 | — | $289.44 | +0.00 | $279.20 | -51.20 | -51.20 | +0.00 | -51.20 |
| 2026-08-28 | `LSCC` | 12 | — | $119.76 | +0.00 | $114.40 | -64.32 | -64.32 | +0.00 | -64.32 |
| 2026-08-31 | `SIMO` | 5 | $245.81 | $247.05 | +6.20 | — | +0.00 | +6.20 | -25.95 | — |
| 2026-08-31 | `SMTC` | 10 | $131.17 | $132.30 | +11.30 | — | +0.00 | +11.30 | -94.60 | — |
| 2026-08-31 | `TTMI` | 11 | $118.65 | $118.83 | +1.98 | — | +0.00 | +1.98 | -43.78 | — |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `AVT` | 16 | $88.63 | $89.39 | +12.16 | — | +0.00 | +12.16 | -33.60 | — |
| 2026-08-31 | `CGNX` | 23 | $60.46 | $60.46 | +0.00 | — | +0.00 | +0.00 | -54.28 | — |
| 2026-08-31 | `COHR` | 5 | $279.20 | $280.25 | +5.25 | — | +0.00 | +5.25 | -45.95 | — |
| 2026-08-31 | `LSCC` | 12 | $114.40 | $115.56 | +13.92 | — | +0.00 | +13.92 | -50.40 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 26 | — | $52.88 | +0.00 | $52.46 | -10.92 | -10.92 | +0.00 | -10.92 |
| 2026-09-03 | `HRMY` | 33 | — | $42.93 | +0.00 | $41.86 | -35.31 | -35.31 | +0.00 | -35.31 |
| 2026-09-03 | `CABA` | 391 | — | $3.63 | +0.00 | $3.48 | -58.65 | -58.65 | +0.00 | -58.65 |
| 2026-09-03 | `VSTM` | 176 | — | $8.03 | +0.00 | $7.98 | -8.80 | -8.80 | +0.00 | -8.80 |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `ARCT` | 84 | — | $16.77 | +0.00 | $15.56 | -101.64 | -101.64 | +0.00 | -101.64 |
| 2026-09-03 | `SLN` | 95 | — | $14.85 | +0.00 | $14.79 | -5.70 | -5.70 | +0.00 | -5.70 |
| 2026-09-03 | `CRDL` | 651 | — | $2.18 | +0.00 | $2.16 | -13.02 | -13.02 | +0.00 | -13.02 |
| 2026-09-04 | `ATRC` | 26 | $52.46 | $52.03 | -11.18 | $51.52 | -13.26 | -24.44 | -22.10 | -35.36 |
| 2026-09-04 | `HRMY` | 33 | $41.86 | $41.50 | -11.88 | — | +0.00 | -11.88 | -47.19 | — |
| 2026-09-04 | `CABA` | 391 | $3.48 | $3.46 | -7.82 | $3.47 | +3.91 | -3.91 | -66.47 | -62.56 |
| 2026-09-04 | `VSTM` | 176 | $7.98 | $7.91 | -12.32 | — | +0.00 | -12.32 | -21.12 | — |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `ARCT` | 84 | $15.56 | $15.61 | +4.20 | — | +0.00 | +4.20 | -97.44 | — |
| 2026-09-04 | `SLN` | 95 | $14.79 | $14.63 | -15.20 | — | +0.00 | -15.20 | -20.90 | — |
| 2026-09-04 | `CRDL` | 651 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -13.02 | — |
| 2026-09-04 | `ALEC` | 549 | — | $2.52 | +0.00 | $2.46 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-09-04 | `BHC` | 206 | — | $6.71 | +0.00 | $6.56 | -30.90 | -30.90 | +0.00 | -30.90 |
| 2026-09-04 | `BMEA` | 728 | — | $1.90 | +0.00 | $2.03 | +94.64 | +94.64 | +0.00 | +94.64 |
| 2026-09-04 | `OABI` | 289 | — | $4.78 | +0.00 | $4.33 | -130.05 | -130.05 | +0.00 | -130.05 |
| 2026-09-04 | `OPK` | 870 | — | $1.59 | +0.00 | $1.64 | +43.50 | +43.50 | +0.00 | +43.50 |
| 2026-09-04 | `VIR` | 119 | — | $11.31 | +0.00 | $11.38 | +8.92 | +8.92 | +0.00 | +8.92 |
| 2026-09-08 | `ATRC` | 26 | $51.52 | $54.31 | +72.54 | — | +0.00 | +72.54 | +37.18 | — |
| 2026-09-08 | `CABA` | 391 | $3.47 | $3.43 | -15.64 | — | +0.00 | -15.64 | -78.20 | — |
| 2026-09-08 | `ALEC` | 549 | $2.46 | $2.38 | -43.92 | — | +0.00 | -43.92 | -76.86 | — |
| 2026-09-08 | `BHC` | 206 | $6.56 | $6.57 | +2.06 | — | +0.00 | +2.06 | -28.84 | — |
| 2026-09-08 | `BMEA` | 728 | $2.03 | $2.00 | -21.84 | — | +0.00 | -21.84 | +72.80 | — |
| 2026-09-08 | `OABI` | 289 | $4.33 | $4.30 | -8.67 | — | +0.00 | -8.67 | -138.72 | — |
| 2026-09-08 | `OPK` | 870 | $1.64 | $1.63 | -8.70 | — | +0.00 | -8.70 | +34.80 | — |
| 2026-09-08 | `VIR` | 119 | $11.38 | $11.22 | -19.63 | — | +0.00 | -19.63 | -10.71 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +185.07 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | -100.50 | DAVE, SLG, MARA, LDI, BTBT, BETR, ANGX, HYLN | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $251.37 | $9,999.36 | DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845, BETR×85, ANGX×294, HYLN×303 |
| 2026-08-17 | +2.25 | $251.37 | DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845, BETR×85, ANGX×294, HYLN×303 | $10,039.35 | +39.99 | -234.18 | TMC, TGB, DNN, CDNL, ABX, OCC, ALM, UMAC | DAVE, SLG, MARA, LDI, BTBT, BETR, ANGX, HYLN | $34.02 | $9,738.55 | TMC×308, TGB×147, DNN×385, CDNL×31, ABX×136, OCC×68, ALM×77, UMAC×38 |
| 2026-08-18 | -6.20 | $34.02 | TMC×308, TGB×147, DNN×385, CDNL×31, ABX×136, OCC×68, ALM×77, UMAC×38 | $9,553.81 | -184.74 | +0.00 | — | TMC, TGB, DNN, CDNL, ABX, OCC, ALM, UMAC | $9,531.16 | $9,531.16 | — |
| 2026-08-19 | -7.20 | $9,531.16 | — | $9,531.16 | -0.00 | +0.00 | — | — | $9,531.16 | $9,531.16 | — |
| 2026-08-20 | +1.12 | $9,531.16 | — | $9,531.16 | -0.00 | +225.44 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $77.61 | $9,732.52 | AG×57, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×680, WPM×8 |
| 2026-08-21 | +3.25 | $77.61 | AG×57, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×680, WPM×8 | $9,989.84 | +257.32 | +249.13 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $249.37 | $10,177.02 | AU×10, AUPH×72, AEM×5, ARCT×111, AUTL×504, CRDL×645, CRSP×20, CYPH×943 |
| 2026-08-24 | -5.17 | $249.37 | AU×10, AUPH×72, AEM×5, ARCT×111, AUTL×504, CRDL×645, CRSP×20, CYPH×943 | $10,535.18 | +358.16 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,497.10 | $10,497.10 | — |
| 2026-08-25 | +1.80 | $10,497.10 | — | $10,497.10 | -0.00 | +197.30 | MOS, CRMD, BMEA, ALVO, SUJA, CYPH, DEFT, ZURA | — | $3.50 | $10,640.85 | MOS×55, CRMD×157, BMEA×804, ALVO×250, SUJA×149, CYPH×841, DEFT×2116, ZURA×199 |
| 2026-08-26 | +2.02 | $3.50 | MOS×55, CRMD×157, BMEA×804, ALVO×250, SUJA×149, CYPH×841, DEFT×2116, ZURA×199 | $10,605.87 | -34.98 | +308.04 | USDE | MOS, CRMD, BMEA, ALVO, SUJA, CYPH, DEFT, ZURA | $0.84 | $10,836.60 | USDE×1812 |
| 2026-08-27 | — | $0.84 | USDE×1812 | $11,778.84 | +942.24 | +0.00 | — | USDE | $11,755.07 | $11,755.07 | — |
| 2026-08-28 | +0.75 | $11,755.07 | — | $11,755.07 | +0.00 | -417.13 | SIMO, SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC | — | $618.53 | $11,321.77 | SIMO×5, SMTC×10, TTMI×11, KEYS×4, AVT×16, CGNX×23, COHR×5, LSCC×12 |
| 2026-08-31 | -5.85 | $618.53 | SIMO×5, SMTC×10, TTMI×11, KEYS×4, AVT×16, CGNX×23, COHR×5, LSCC×12 | $11,382.66 | +60.89 | +0.00 | — | SIMO, SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC | $11,366.31 | $11,366.31 | — |
| 2026-09-01 | -6.30 | $11,366.31 | — | $11,366.31 | +0.00 | +0.00 | — | — | $11,366.31 | $11,366.31 | — |
| 2026-09-02 | -3.83 | $11,366.31 | — | $11,366.31 | +0.00 | +0.00 | — | — | $11,366.31 | $11,366.31 | — |
| 2026-09-03 | -0.90 | $11,366.31 | — | $11,366.31 | +0.00 | -252.24 | ATRC, HRMY, CABA, VSTM, RVTY, ARCT, SLN, CRDL | — | $152.37 | $11,087.42 | ATRC×26, HRMY×33, CABA×391, VSTM×176, RVTY×10, ARCT×84, SLN×95, CRDL×651 |
| 2026-09-04 | +2.25 | $152.37 | ATRC×26, HRMY×33, CABA×391, VSTM×176, RVTY×10, ARCT×84, SLN×95, CRDL×651 | $11,027.22 | -60.20 | -56.18 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY, ARCT, SLN, CRDL | $5.81 | $10,914.82 | ATRC×26, CABA×391, ALEC×549, BHC×206, BMEA×728, OABI×289, OPK×870, VIR×119 |
| 2026-09-08 | -11.47 | $5.81 | ATRC×26, CABA×391, ALEC×549, BHC×206, BMEA×728, OABI×289, OPK×870, VIR×119 | $10,871.02 | -43.80 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $10,826.85 | $10,826.85 | — |
| 2026-09-09 | -13.95 | $10,826.85 | — | $10,826.85 | +0.00 | +0.00 | — | — | $10,826.85 | $10,826.85 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | 16:00 close · cash $97.53 · equity $10,153.12 vs 09:30 $10,000.00 (+153.12; session marks +185.07) · 8 name(s) marked open→close (per-name table). BTSG×20 09:30 $59.80 → close $60.23 +8.60; IREN×27 09:30 $45.98 → close $44.76 -32.94; TPG×24 09:30 $50.62 → close $54.62 +95.92; TGTX×25 09:30 $49.70 → close $47.94 -44.00; SLS×106 09:30 $11.70 → close $12.36 +69.96; HIMS×42 09:30 $29.74 → close $28.77 -40.74; INO×1543 09:30 $0.81 → close $0.90 +138.87; TNDM×53 09:30 $23.33 → close $23.13 -10.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) · 8 name(s) re-marked at the open (per-name table). BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▼ -7.12 after sell → book $10,176.05; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | ▼ -55.19 after sell → book $10,173.96; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | ▲ +107.86 after sell → book $10,171.88; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | ▼ -64.90 after sell → book $10,169.80; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | ▲ +69.56 after sell → book $10,167.46; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | ▼ -29.03 after sell → book $10,165.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | ▲ +148.79 after sell → book $10,146.08; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | ▼ -26.05 after sell → book $10,143.91; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $9,149.18 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $7,879.70 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $6,615.89 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $5,331.40 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $4,053.00 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 85 | $14.80 | $2.25 | — | $2,792.75 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 294 | $4.31 | $3.79 | — | $1,521.82 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 303 | $4.18 | $3.91 | — | $251.37 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.37 | ▼ close $9,999.36 vs 09:30 $10,178.12 (session -100.50) | 16:00 close · cash $251.37 · equity $9,999.36 vs 09:30 $10,178.12 (-178.76; session marks -100.50) · 8 name(s) marked open→close (per-name table). DAVE×3 09:30 $330.91 → close $334.57 +10.98; SLG×22 09:30 $57.61 → close $56.09 -33.44; MARA×140 09:30 $9.01 → close $9.20 +26.60; LDI×1353 09:30 $0.94 → close $0.90 -54.12; BTBT×845 09:30 $1.50 → close $1.57 +59.15; BETR×85 09:30 $14.80 → close $13.73 -90.95; ANGX×294 09:30 $4.31 → close $4.37 +17.64; HYLN×303 09:30 $4.18 → close $4.06 -36.36 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.37 | ▲ 09:30 equity $10,039.35 vs yday $9,999.36 (+39.99) | 09:30 open · cash $251.37 (unchanged overnight, no fees) · equity $10,039.35 vs prior close $9,999.36 (+39.99) · 8 name(s) re-marked at the open (per-name table). DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×22 yday $56.09 → 09:30 $55.37 -15.84; MARA×140 yday $9.20 → 09:30 $9.22 +2.80; LDI×1353 yday $0.90 → 09:30 $0.91 +13.53; BTBT×845 yday $1.57 → 09:30 $1.52 -42.25; BETR×85 yday $13.73 → 09:30 $13.67 -5.10; ANGX×294 yday $4.37 → 09:30 $4.60 +67.62; HYLN×303 yday $4.06 → 09:30 $4.10 +12.12 | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $1,260.17 | ▲ +14.07 after sell → book $10,037.33; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $2,476.23 | ▼ -53.41 after sell → book $10,035.26; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $3,764.59 | ▲ +24.55 after sell → book $10,032.81; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1353 | $0.91 | $16.57 | $-73.89 | $4,975.20 | ▼ -73.89 after sell → book $10,016.25; vs 09:30 mark -16.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 845 | $1.52 | $11.05 | $-5.05 | $6,248.55 | ▼ -5.05 after sell → book $10,005.20; vs 09:30 mark -11.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 85 | $13.67 | $2.27 | $-100.56 | $7,408.23 | ▼ -100.56 after sell → book $10,002.93; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 294 | $4.60 | $3.85 | $+77.62 | $8,756.77 | ▲ +77.62 after sell → book $9,999.07; vs 09:30 mark -3.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 303 | $4.10 | $3.97 | $-32.12 | $9,995.11 | ▼ -32.12 after sell → book $9,995.11; vs 09:30 mark -3.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 308 | $4.05 | $3.97 | — | $8,743.73 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1249.39 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 147 | $8.46 | $2.43 | — | $7,497.68 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1249.39 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 385 | $3.24 | $4.97 | — | $6,245.31 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+0.3; leftover $1249.39 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $5,007.88 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1249.39 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 136 | $9.12 | $2.40 | — | $3,765.16 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1249.39 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 68 | $18.24 | $2.19 | — | $2,522.65 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1249.39 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $1,273.03 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1249.39 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 38 | $32.55 | $2.10 | — | $34.02 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1249.39 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.02 | ▼ close $9,738.55 vs 09:30 $10,039.35 (session -234.18) | 16:00 close · cash $34.02 · equity $9,738.55 vs 09:30 $10,039.35 (-300.80; session marks -234.18) · 8 name(s) marked open→close (per-name table). TMC×308 09:30 $4.05 → close $3.77 -86.24; TGB×147 09:30 $8.46 → close $8.77 +45.57; DNN×385 09:30 $3.24 → close $3.19 -19.25; CDNL×31 09:30 $39.85 → close $39.23 -19.22; ABX×136 09:30 $9.12 → close $9.12 +0.00; OCC×68 09:30 $18.24 → close $17.12 -76.16; ALM×77 09:30 $16.20 → close $16.36 +12.32; UMAC×38 09:30 $32.55 → close $30.15 -91.20 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.02 | ▼ 09:30 equity $9,553.81 vs yday $9,738.55 (-184.74) | 09:30 open · cash $34.02 (unchanged overnight, no fees) · equity $9,553.81 vs prior close $9,738.55 (-184.74) · 8 name(s) re-marked at the open (per-name table). TMC×308 yday $3.77 → 09:30 $3.72 -15.40; TGB×147 yday $8.77 → 09:30 $8.55 -32.34; DNN×385 yday $3.19 → 09:30 $3.11 -30.80; CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; ABX×136 yday $9.12 → 09:30 $9.03 -12.24; OCC×68 yday $17.12 → 09:30 $16.20 -62.56; ALM×77 yday $16.36 → 09:30 $15.78 -44.66; UMAC×38 yday $30.15 → 09:30 $28.59 -59.28 | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 308 | $3.72 | $4.03 | $-109.65 | $1,175.75 | ▼ -109.65 after sell → book $9,549.78; vs 09:30 mark -4.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 147 | $8.55 | $2.47 | $+8.33 | $2,430.13 | ▲ +8.33 after sell → book $9,547.31; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 385 | $3.11 | $5.04 | $-60.06 | $3,622.44 | ▼ -60.06 after sell → book $9,542.27; vs 09:30 mark -5.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $4,909.01 | ▲ +49.13 after sell → book $9,540.17; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 136 | $9.03 | $2.43 | $-17.07 | $6,134.66 | ▼ -17.07 after sell → book $9,537.74; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 68 | $16.20 | $2.22 | $-143.13 | $7,234.05 | ▼ -143.13 after sell → book $9,535.53; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $8,446.86 | ▼ -36.80 after sell → book $9,533.28; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 38 | $28.59 | $2.12 | $-154.71 | $9,531.16 | ▼ -154.71 after sell → book $9,531.16; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,531.16 | ▲ close $9,531.16 vs 09:30 $9,553.81 (session +0.00) | 16:00 close · cash $9,531.16 · no lots left · equity $9,531.16. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,531.16 | ▲ 09:30 equity $9,531.16 vs yday $9,531.16 (-0.00) | 09:30 open · cash $9,531.16 · no holdings · equity $9,531.16 vs prior close $9,531.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,531.16 | ▲ close $9,531.16 vs 09:30 $9,531.16 (session +0.00) | 16:00 close · cash $9,531.16 · no lots left · equity $9,531.16. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,531.16 | ▲ 09:30 equity $9,531.16 vs yday $9,531.16 (-0.00) | 09:30 open · cash $9,531.16 · no holdings · equity $9,531.16 vs prior close $9,531.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,357.65 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1191.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,172.49 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1191.39 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $5,993.28 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1191.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 206 | $5.77 | $2.66 | — | $4,802.00 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1191.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $3,622.03 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1191.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $2,434.72 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1191.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 680 | $1.75 | $8.77 | — | $1,235.95 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1191.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $77.61 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1191.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $77.61 | ▲ close $9,732.52 vs 09:30 $9,531.16 (session +225.44) | 16:00 close · cash $77.61 · equity $9,732.52 vs 09:30 $9,531.16 (+201.36; session marks +225.44) · 8 name(s) marked open→close (per-name table). AG×57 09:30 $20.55 → close $21.19 +36.48; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×57 09:30 $20.65 → close $21.11 +26.22; HDSN×206 09:30 $5.77 → close $5.57 -41.20; IAG×60 09:30 $19.63 → close $20.50 +52.20; KGC×40 09:30 $29.63 → close $31.43 +72.00; NFGC×680 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.61 | ▲ 09:30 equity $9,989.84 vs yday $9,732.52 (+257.32) | 09:30 open · cash $77.61 (unchanged overnight, no fees) · equity $9,989.84 vs prior close $9,732.52 (+257.32) · 8 name(s) re-marked at the open (per-name table). AG×57 yday $21.19 → 09:30 $21.90 +40.47; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×57 yday $21.11 → 09:30 $21.75 +36.48; HDSN×206 yday $5.57 → 09:30 $5.67 +20.60; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×680 yday $1.75 → 09:30 $1.79 +27.20; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 57 | $21.90 | $2.18 | $+72.61 | $1,323.73 | ▲ +72.61 after sell → book $9,987.66; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,566.04 | ▲ +57.15 after sell → book $9,985.61; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 57 | $21.75 | $2.18 | $+58.36 | $3,803.61 | ▲ +58.36 after sell → book $9,983.43; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 206 | $5.67 | $2.70 | $-25.96 | $4,968.93 | ▼ -25.96 after sell → book $9,980.73; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 60 | $21.17 | $2.19 | $+88.04 | $6,236.94 | ▲ +88.04 after sell → book $9,978.54; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $7,521.61 | ▲ +97.36 after sell → book $9,976.41; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 680 | $1.79 | $8.89 | $+9.53 | $8,729.91 | ▲ +9.53 after sell → book $9,967.51; vs 09:30 mark -8.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,965.48 | ▲ +77.23 after sell → book $9,965.48; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,769.16 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1245.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 72 | $17.20 | $2.21 | — | $7,528.55 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1245.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,445.05 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1245.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 111 | $11.13 | $2.32 | — | $5,207.30 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1245.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 504 | $2.47 | $6.50 | — | $3,955.92 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1245.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 645 | $1.93 | $8.32 | — | $2,702.74 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1245.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $1,506.29 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1245.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 943 | $1.32 | $12.16 | — | $249.37 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1245.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $249.37 | ▲ close $10,177.02 vs 09:30 $9,989.84 (session +249.13) | 16:00 close · cash $249.37 · equity $10,177.02 vs 09:30 $9,989.84 (+187.18; session marks +249.13) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×72 09:30 $17.20 → close $16.65 -39.60; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×111 09:30 $11.13 → close $13.45 +257.52; AUTL×504 09:30 $2.47 → close $2.41 -30.24; CRDL×645 09:30 $1.93 → close $1.86 -45.15; CRSP×20 09:30 $59.72 → close $59.50 -4.40; CYPH×943 09:30 $1.32 → close $1.42 +94.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $249.37 | ▲ 09:30 equity $10,535.18 vs yday $10,177.02 (+358.16) | 09:30 open · cash $249.37 (unchanged overnight, no fees) · equity $10,535.18 vs prior close $10,177.02 (+358.16) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×72 yday $16.65 → 09:30 $16.57 -5.76; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×111 yday $13.45 → 09:30 $13.33 -13.32; AUTL×504 yday $2.41 → 09:30 $2.40 -5.04; CRDL×645 yday $1.86 → 09:30 $1.88 +12.90; CRSP×20 yday $59.50 → 09:30 $58.75 -15.00; CYPH×943 yday $1.42 → 09:30 $1.83 +386.63 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,452.43 | ▲ +6.74 after sell → book $10,533.14; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 72 | $16.57 | $2.23 | $-49.79 | $2,643.24 | ▼ -49.79 after sell → book $10,530.91; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,726.37 | ▼ -0.38 after sell → book $10,528.89; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 111 | $13.33 | $2.35 | $+239.52 | $5,203.64 | ▲ +239.52 after sell → book $10,526.53; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 504 | $2.40 | $6.60 | $-48.38 | $6,406.65 | ▼ -48.38 after sell → book $10,519.94; vs 09:30 mark -6.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 645 | $1.88 | $8.44 | $-49.01 | $7,610.81 | ▼ -49.01 after sell → book $10,511.50; vs 09:30 mark -8.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 20 | $58.75 | $2.07 | $-23.52 | $8,783.74 | ▼ -23.52 after sell → book $10,509.43; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 943 | $1.83 | $12.34 | $+456.43 | $10,497.10 | ▲ +456.43 after sell → book $10,497.10; vs 09:30 mark -12.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,497.10 | ▲ close $10,497.10 vs 09:30 $10,535.18 (session +0.00) | 16:00 close · cash $10,497.10 · no lots left · equity $10,497.10. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,497.10 | ▲ 09:30 equity $10,497.10 vs yday $10,497.10 (-0.00) | 09:30 open · cash $10,497.10 · no holdings · equity $10,497.10 vs prior close $10,497.10 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $23.77 | $2.15 | — | $9,187.59 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.0; leftover $1312.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 157 | $8.35 | $2.46 | — | $7,874.18 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1312.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 804 | $1.63 | $10.37 | — | $6,553.29 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1312.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 250 | $5.24 | $3.23 | — | $5,240.06 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1312.14 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 149 | $8.79 | $2.44 | — | $3,927.92 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1312.14 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 841 | $1.56 | $10.85 | — | $2,605.11 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1312.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2116 | $0.62 | $19.47 | — | $1,273.72 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1312.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 199 | $6.37 | $2.59 | — | $3.50 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1312.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.50 | ▲ close $10,640.85 vs 09:30 $10,497.10 (session +197.30) | 16:00 close · cash $3.50 · equity $10,640.85 vs 09:30 $10,497.10 (+143.75; session marks +197.30) · 8 name(s) marked open→close (per-name table). MOS×55 09:30 $23.77 → close $24.27 +27.50; CRMD×157 09:30 $8.35 → close $8.56 +32.97; BMEA×804 09:30 $1.63 → close $1.73 +80.40; ALVO×250 09:30 $5.24 → close $5.05 -47.50; SUJA×149 09:30 $8.79 → close $9.33 +80.46; CYPH×841 09:30 $1.56 → close $1.64 +67.28; DEFT×2116 09:30 $0.62 → close $0.60 -33.86; ZURA×199 09:30 $6.37 → close $6.32 -9.95 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.50 | ▼ 09:30 equity $10,605.87 vs yday $10,640.85 (-34.98) | 09:30 open · cash $3.50 (unchanged overnight, no fees) · equity $10,605.87 vs prior close $10,640.85 (-34.98) · 8 name(s) re-marked at the open (per-name table). MOS×55 yday $24.27 → 09:30 $24.84 +31.35; CRMD×157 yday $8.56 → 09:30 $8.60 +6.28; BMEA×804 yday $1.73 → 09:30 $1.75 +20.10; ALVO×250 yday $5.05 → 09:30 $4.98 -17.50; SUJA×149 yday $9.33 → 09:30 $9.39 +8.94; CYPH×841 yday $1.64 → 09:30 $1.60 -33.64; DEFT×2116 yday $0.60 → 09:30 $0.60 -12.70; ZURA×199 yday $6.32 → 09:30 $6.13 -37.81 | — |
| 2026-08-26 09:30 ET | **SELL** | `MOS` | 55 | $24.84 | $2.18 | $+54.52 | $1,367.53 | ▲ +54.52 after sell → book $10,603.70; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 157 | $8.60 | $2.50 | $+34.29 | $2,715.23 | ▲ +34.29 after sell → book $10,601.20; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 804 | $1.75 | $10.52 | $+79.61 | $4,115.73 | ▲ +79.61 after sell → book $10,590.68; vs 09:30 mark -10.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 250 | $4.98 | $3.28 | $-71.50 | $5,357.46 | ▼ -71.50 after sell → book $10,587.40; vs 09:30 mark -3.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUJA` | 149 | $9.39 | $2.47 | $+84.49 | $6,754.09 | ▲ +84.49 after sell → book $10,584.93; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 841 | $1.60 | $11.00 | $+11.79 | $8,088.69 | ▲ +11.79 after sell → book $10,573.93; vs 09:30 mark -11.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DEFT` | 2116 | $0.60 | $19.36 | $-85.38 | $9,334.70 | ▼ -85.38 after sell → book $10,554.57; vs 09:30 mark -19.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 199 | $6.13 | $2.63 | $-52.98 | $10,551.94 | ▼ -52.98 after sell → book $10,551.94; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 1812 | $5.81 | $23.37 | — | $0.84 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $10551.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.84 | ▲ close $10,836.60 vs 09:30 $10,605.87 (session +308.04) | 16:00 close · cash $0.84 · equity $10,836.60 vs 09:30 $10,605.87 (+230.73; session marks +308.04) · 1 name(s) marked open→close (per-name table). USDE×1812 09:30 $5.81 → close $5.98 +308.04 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.84 | ▲ 09:30 equity $11,778.84 vs yday $10,836.60 (+942.24) | 09:30 open · cash $0.84 (unchanged overnight, no fees) · equity $11,778.84 vs prior close $10,836.60 (+942.24) · 1 name(s) re-marked at the open (per-name table). USDE×1812 yday $5.98 → 09:30 $6.50 +942.24 | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 1812 | $6.50 | $23.77 | $+1203.14 | $11,755.07 | ▲ +1,203.14 after sell → book $11,755.07; vs 09:30 mark -23.77 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,755.07 | ▲ close $11,755.07 vs 09:30 $11,778.84 (session +0.00) | 16:00 close · cash $11,755.07 · no lots left · equity $11,755.07. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,755.07 | ▲ 09:30 equity $11,755.07 vs yday $11,755.07 (+0.00) | 09:30 open · cash $11,755.07 · no holdings · equity $11,755.07 vs prior close $11,755.07 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $10,491.87 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1469.38 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 10 | $141.76 | $2.02 | — | $9,072.25 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1469.38 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 11 | $122.81 | $2.02 | — | $7,719.32 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1469.38 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $6,419.67 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1469.38 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 16 | $91.49 | $2.04 | — | $4,953.80 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1469.38 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 23 | $62.82 | $2.06 | — | $3,506.88 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1469.38 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 5 | $289.44 | $2.00 | — | $2,057.67 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1469.38 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 12 | $119.76 | $2.03 | — | $618.53 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1469.38 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $618.53 | ▼ close $11,321.77 vs 09:30 $11,755.07 (session -417.13) | 16:00 close · cash $618.53 · equity $11,321.77 vs 09:30 $11,755.07 (-433.30; session marks -417.13) · 8 name(s) marked open→close (per-name table). SIMO×5 09:30 $252.24 → close $245.81 -32.15; SMTC×10 09:30 $141.76 → close $131.17 -105.90; TTMI×11 09:30 $122.81 → close $118.65 -45.76; KEYS×4 09:30 $324.41 → close $319.97 -17.76; AVT×16 09:30 $91.49 → close $88.63 -45.76; CGNX×23 09:30 $62.82 → close $60.46 -54.28; COHR×5 09:30 $289.44 → close $279.20 -51.20; LSCC×12 09:30 $119.76 → close $114.40 -64.32 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $618.53 | ▲ 09:30 equity $11,382.66 vs yday $11,321.77 (+60.89) | 09:30 open · cash $618.53 (unchanged overnight, no fees) · equity $11,382.66 vs prior close $11,321.77 (+60.89) · 8 name(s) re-marked at the open (per-name table). SIMO×5 yday $245.81 → 09:30 $247.05 +6.20; SMTC×10 yday $131.17 → 09:30 $132.30 +11.30; TTMI×11 yday $118.65 → 09:30 $118.83 +1.98; KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; AVT×16 yday $88.63 → 09:30 $89.39 +12.16; CGNX×23 yday $60.46 → 09:30 $60.46 +0.00; COHR×5 yday $279.20 → 09:30 $280.25 +5.25; LSCC×12 yday $114.40 → 09:30 $115.56 +13.92 | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $1,851.75 | ▼ -29.98 after sell → book $11,380.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 10 | $132.30 | $2.04 | $-98.66 | $3,172.71 | ▼ -98.66 after sell → book $11,378.59; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 11 | $118.83 | $2.04 | $-47.85 | $4,477.80 | ▼ -47.85 after sell → book $11,376.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $5,765.74 | ▼ -11.70 after sell → book $11,374.53; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 16 | $89.39 | $2.06 | $-37.70 | $7,193.92 | ▼ -37.70 after sell → book $11,372.47; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 23 | $60.46 | $2.08 | $-58.42 | $8,582.42 | ▼ -58.42 after sell → book $11,370.39; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 5 | $280.25 | $2.03 | $-49.98 | $9,981.64 | ▼ -49.98 after sell → book $11,368.36; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 12 | $115.56 | $2.05 | $-54.47 | $11,366.31 | ▼ -54.47 after sell → book $11,366.31; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,366.31 | ▲ close $11,366.31 vs 09:30 $11,382.66 (session +0.00) | 16:00 close · cash $11,366.31 · no lots left · equity $11,366.31. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,366.31 | ▲ 09:30 equity $11,366.31 vs yday $11,366.31 (+0.00) | 09:30 open · cash $11,366.31 · no holdings · equity $11,366.31 vs prior close $11,366.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,366.31 | ▲ close $11,366.31 vs 09:30 $11,366.31 (session +0.00) | 16:00 close · cash $11,366.31 · no lots left · equity $11,366.31. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,366.31 | ▲ 09:30 equity $11,366.31 vs yday $11,366.31 (+0.00) | 09:30 open · cash $11,366.31 · no holdings · equity $11,366.31 vs prior close $11,366.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,366.31 | ▲ close $11,366.31 vs 09:30 $11,366.31 (session +0.00) | 16:00 close · cash $11,366.31 · no lots left · equity $11,366.31. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,366.31 | ▲ 09:30 equity $11,366.31 vs yday $11,366.31 (+0.00) | 09:30 open · cash $11,366.31 · no holdings · equity $11,366.31 vs prior close $11,366.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $52.88 | $2.07 | — | $9,989.36 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1420.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 33 | $42.93 | $2.09 | — | $8,570.59 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1420.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 391 | $3.63 | $5.04 | — | $7,146.21 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1420.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 176 | $8.03 | $2.52 | — | $5,730.41 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1420.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,403.89 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1420.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 84 | $16.77 | $2.24 | — | $2,992.97 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1420.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 95 | $14.85 | $2.27 | — | $1,579.95 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1420.79 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 651 | $2.18 | $8.40 | — | $152.37 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1420.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.37 | ▼ close $11,087.42 vs 09:30 $11,366.31 (session -252.24) | 16:00 close · cash $152.37 · equity $11,087.42 vs 09:30 $11,366.31 (-278.89; session marks -252.24) · 8 name(s) marked open→close (per-name table). ATRC×26 09:30 $52.88 → close $52.46 -10.92; HRMY×33 09:30 $42.93 → close $41.86 -35.31; CABA×391 09:30 $3.63 → close $3.48 -58.65; VSTM×176 09:30 $8.03 → close $7.98 -8.80; RVTY×10 09:30 $132.45 → close $130.63 -18.20; ARCT×84 09:30 $16.77 → close $15.56 -101.64; SLN×95 09:30 $14.85 → close $14.79 -5.70; CRDL×651 09:30 $2.18 → close $2.16 -13.02 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.37 | ▼ 09:30 equity $11,027.22 vs yday $11,087.42 (-60.20) | 09:30 open · cash $152.37 (unchanged overnight, no fees) · equity $11,027.22 vs prior close $11,087.42 (-60.20) · 8 name(s) re-marked at the open (per-name table). ATRC×26 yday $52.46 → 09:30 $52.03 -11.18; HRMY×33 yday $41.86 → 09:30 $41.50 -11.88; CABA×391 yday $3.48 → 09:30 $3.46 -7.82; VSTM×176 yday $7.98 → 09:30 $7.91 -12.32; RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; ARCT×84 yday $15.56 → 09:30 $15.61 +4.20; SLN×95 yday $14.79 → 09:30 $14.63 -15.20; CRDL×651 yday $2.16 → 09:30 $2.16 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 33 | $41.50 | $2.11 | $-51.39 | $1,519.76 | ▼ -51.39 after sell → book $11,025.11; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 176 | $7.91 | $2.56 | $-26.20 | $2,909.36 | ▼ -26.20 after sell → book $11,022.55; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $4,207.62 | ▼ -28.26 after sell → book $11,020.51; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 84 | $15.61 | $2.27 | $-101.95 | $5,516.59 | ▼ -101.95 after sell → book $11,018.24; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 95 | $14.63 | $2.30 | $-25.48 | $6,904.14 | ▼ -25.48 after sell → book $11,015.94; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 651 | $2.16 | $8.52 | $-29.94 | $8,301.78 | ▼ -29.94 after sell → book $11,007.42; vs 09:30 mark -8.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 549 | $2.52 | $7.08 | — | $6,911.22 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1383.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 206 | $6.71 | $2.66 | — | $5,526.30 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1383.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 728 | $1.90 | $9.39 | — | $4,133.71 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1383.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 289 | $4.78 | $3.73 | — | $2,748.57 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1383.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 870 | $1.59 | $11.22 | — | $1,354.04 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1383.63 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 119 | $11.31 | $2.35 | — | $5.81 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1383.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.81 | ▼ close $10,914.82 vs 09:30 $11,027.22 (session -56.18) | 16:00 close · cash $5.81 · equity $10,914.82 vs 09:30 $11,027.22 (-112.40; session marks -56.18) · 8 name(s) marked open→close (per-name table). ATRC×26 09:30 $52.03 → close $51.52 -13.26; CABA×391 09:30 $3.46 → close $3.47 +3.91; ALEC×549 09:30 $2.52 → close $2.46 -32.94; BHC×206 09:30 $6.71 → close $6.56 -30.90; BMEA×728 09:30 $1.90 → close $2.03 +94.64; OABI×289 09:30 $4.78 → close $4.33 -130.05; OPK×870 09:30 $1.59 → close $1.64 +43.50; VIR×119 09:30 $11.31 → close $11.38 +8.92 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.81 | ▼ 09:30 equity $10,871.02 vs yday $10,914.82 (-43.80) | 09:30 open · cash $5.81 (unchanged overnight, no fees) · equity $10,871.02 vs prior close $10,914.82 (-43.80) · 8 name(s) re-marked at the open (per-name table). ATRC×26 yday $51.52 → 09:30 $54.31 +72.54; CABA×391 yday $3.47 → 09:30 $3.43 -15.64; ALEC×549 yday $2.46 → 09:30 $2.38 -43.92; BHC×206 yday $6.56 → 09:30 $6.57 +2.06; BMEA×728 yday $2.03 → 09:30 $2.00 -21.84; OABI×289 yday $4.33 → 09:30 $4.30 -8.67; OPK×870 yday $1.64 → 09:30 $1.63 -8.70; VIR×119 yday $11.38 → 09:30 $11.22 -19.63 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 26 | $54.31 | $2.09 | $+33.02 | $1,415.78 | ▲ +33.02 after sell → book $10,868.93; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 391 | $3.43 | $5.12 | $-88.36 | $2,751.79 | ▼ -88.36 after sell → book $10,863.81; vs 09:30 mark -5.12 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 549 | $2.38 | $7.18 | $-91.13 | $4,051.22 | ▼ -91.13 after sell → book $10,856.62; vs 09:30 mark -7.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 206 | $6.57 | $2.70 | $-34.20 | $5,401.94 | ▼ -34.20 after sell → book $10,853.92; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 728 | $2.00 | $9.52 | $+53.89 | $6,848.42 | ▲ +53.89 after sell → book $10,844.40; vs 09:30 mark -9.52 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 289 | $4.30 | $3.79 | $-146.23 | $8,087.33 | ▼ -146.23 after sell → book $10,840.61; vs 09:30 mark -3.79 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 870 | $1.63 | $11.38 | $+12.20 | $9,494.05 | ▲ +12.20 after sell → book $10,829.23; vs 09:30 mark -11.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 119 | $11.22 | $2.38 | $-15.43 | $10,826.85 | ▼ -15.43 after sell → book $10,826.85; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,826.85 | ▲ close $10,826.85 vs 09:30 $10,871.02 (session +0.00) | 16:00 close · cash $10,826.85 · no lots left · equity $10,826.85. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,826.85 | ▲ 09:30 equity $10,826.85 vs yday $10,826.85 (+0.00) | 09:30 open · cash $10,826.85 · no holdings · equity $10,826.85 vs prior close $10,826.85 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,826.85 | ▲ close $10,826.85 vs 09:30 $10,826.85 (session +0.00) | 16:00 close · cash $10,826.85 · no lots left · equity $10,826.85. | — |
