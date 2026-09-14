# Factor mine action — `union_white_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ white, no 🚨

Cash book **+3.97%** ($10,397) · signal-only (no cash/fees) was -0.52%. Starts YES **9/22**. Fills 152 · skips 0 · realized $+397.39.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,397.36.

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
| 2026-08-17 | `CDNL` | 31 | — | $39.85 | +0.00 | $39.23 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-17 | `ABX` | 136 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `OCC` | 68 | — | $18.24 | +0.00 | $17.12 | -76.16 | -76.16 | +0.00 | -76.16 |
| 2026-08-17 | `ALM` | 77 | — | $16.20 | +0.00 | $16.36 | +12.32 | +12.32 | +0.00 | +12.32 |
| 2026-08-17 | `UMAC` | 38 | — | $32.55 | +0.00 | $30.15 | -91.20 | -91.20 | +0.00 | -91.20 |
| 2026-08-17 | `NPWR` | 650 | — | $1.92 | +0.00 | $1.73 | -123.50 | -123.50 | +0.00 | -123.50 |
| 2026-08-18 | `TMC` | 308 | $3.77 | $3.72 | -15.40 | — | +0.00 | -15.40 | -101.64 | — |
| 2026-08-18 | `TGB` | 147 | $8.77 | $8.55 | -32.34 | — | +0.00 | -32.34 | +13.23 | — |
| 2026-08-18 | `CDNL` | 31 | $39.23 | $41.57 | +72.54 | — | +0.00 | +72.54 | +53.32 | — |
| 2026-08-18 | `ABX` | 136 | $9.12 | $9.03 | -12.24 | — | +0.00 | -12.24 | -12.24 | — |
| 2026-08-18 | `OCC` | 68 | $17.12 | $16.20 | -62.56 | — | +0.00 | -62.56 | -138.72 | — |
| 2026-08-18 | `ALM` | 77 | $16.36 | $15.78 | -44.66 | — | +0.00 | -44.66 | -32.34 | — |
| 2026-08-18 | `UMAC` | 38 | $30.15 | $28.59 | -59.28 | — | +0.00 | -59.28 | -150.48 | — |
| 2026-08-18 | `NPWR` | 650 | $1.73 | $1.70 | -19.50 | — | +0.00 | -19.50 | -143.00 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 57 | — | $20.55 | +0.00 | $21.19 | +36.48 | +36.48 | +0.00 | +36.48 |
| 2026-08-20 | `BHP` | 12 | — | $91.01 | +0.00 | $93.63 | +31.44 | +31.44 | +0.00 | +31.44 |
| 2026-08-20 | `CDE` | 57 | — | $20.65 | +0.00 | $21.11 | +26.22 | +26.22 | +0.00 | +26.22 |
| 2026-08-20 | `HDSN` | 204 | — | $5.77 | +0.00 | $5.57 | -40.80 | -40.80 | +0.00 | -40.80 |
| 2026-08-20 | `IAG` | 60 | — | $19.63 | +0.00 | $20.50 | +52.20 | +52.20 | +0.00 | +52.20 |
| 2026-08-20 | `KGC` | 39 | — | $29.63 | +0.00 | $31.43 | +70.20 | +70.20 | +0.00 | +70.20 |
| 2026-08-20 | `NFGC` | 673 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 57 | $21.19 | $21.90 | +40.47 | — | +0.00 | +40.47 | +76.95 | — |
| 2026-08-21 | `BHP` | 12 | $93.63 | $95.72 | +25.08 | — | +0.00 | +25.08 | +56.52 | — |
| 2026-08-21 | `CDE` | 57 | $21.11 | $21.75 | +36.48 | — | +0.00 | +36.48 | +62.70 | — |
| 2026-08-21 | `HDSN` | 204 | $5.57 | $5.67 | +20.40 | — | +0.00 | +20.40 | -20.40 | — |
| 2026-08-21 | `IAG` | 60 | $20.50 | $21.17 | +40.20 | — | +0.00 | +40.20 | +92.40 | — |
| 2026-08-21 | `KGC` | 39 | $31.43 | $32.17 | +28.86 | — | +0.00 | +28.86 | +99.06 | — |
| 2026-08-21 | `NFGC` | 673 | $1.75 | $1.79 | +26.92 | — | +0.00 | +26.92 | +26.92 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 71 | — | $17.20 | +0.00 | $16.65 | -39.05 | -39.05 | +0.00 | -39.05 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 110 | — | $11.13 | +0.00 | $13.45 | +255.20 | +255.20 | +0.00 | +255.20 |
| 2026-08-21 | `AUTL` | 498 | — | $2.47 | +0.00 | $2.41 | -29.88 | -29.88 | +0.00 | -29.88 |
| 2026-08-21 | `CRDL` | 638 | — | $1.93 | +0.00 | $1.86 | -44.66 | -44.66 | +0.00 | -44.66 |
| 2026-08-21 | `CRSP` | 20 | — | $59.72 | +0.00 | $59.50 | -4.40 | -4.40 | +0.00 | -4.40 |
| 2026-08-21 | `CYPH` | 933 | — | $1.32 | +0.00 | $1.42 | +93.30 | +93.30 | +0.00 | +93.30 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 71 | $16.65 | $16.57 | -5.68 | — | +0.00 | -5.68 | -44.73 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 110 | $13.45 | $13.33 | -13.20 | — | +0.00 | -13.20 | +242.00 | — |
| 2026-08-24 | `AUTL` | 498 | $2.41 | $2.40 | -4.98 | — | +0.00 | -4.98 | -34.86 | — |
| 2026-08-24 | `CRDL` | 638 | $1.86 | $1.88 | +12.76 | — | +0.00 | +12.76 | -31.90 | — |
| 2026-08-24 | `CRSP` | 20 | $59.50 | $58.75 | -15.00 | — | +0.00 | -15.00 | -19.40 | — |
| 2026-08-24 | `CYPH` | 933 | $1.42 | $1.83 | +382.53 | — | +0.00 | +382.53 | +475.83 | — |
| 2026-08-25 | `MOS` | 54 | — | $23.77 | +0.00 | $24.27 | +27.00 | +27.00 | +0.00 | +27.00 |
| 2026-08-25 | `CRMD` | 155 | — | $8.35 | +0.00 | $8.56 | +32.55 | +32.55 | +0.00 | +32.55 |
| 2026-08-25 | `BMEA` | 796 | — | $1.63 | +0.00 | $1.73 | +79.60 | +79.60 | +0.00 | +79.60 |
| 2026-08-25 | `ALVO` | 247 | — | $5.24 | +0.00 | $5.05 | -46.93 | -46.93 | +0.00 | -46.93 |
| 2026-08-25 | `SUJA` | 147 | — | $8.79 | +0.00 | $9.33 | +79.38 | +79.38 | +0.00 | +79.38 |
| 2026-08-25 | `CYPH` | 832 | — | $1.56 | +0.00 | $1.64 | +66.56 | +66.56 | +0.00 | +66.56 |
| 2026-08-25 | `DEFT` | 2093 | — | $0.62 | +0.00 | $0.60 | -33.49 | -33.49 | +0.00 | -33.49 |
| 2026-08-25 | `ZURA` | 200 | — | $6.37 | +0.00 | $6.32 | -10.00 | -10.00 | +0.00 | -10.00 |
| 2026-08-26 | `MOS` | 54 | $24.27 | $24.84 | +30.78 | — | +0.00 | +30.78 | +57.78 | — |
| 2026-08-26 | `CRMD` | 155 | $8.56 | $8.60 | +6.20 | — | +0.00 | +6.20 | +38.75 | — |
| 2026-08-26 | `BMEA` | 796 | $1.73 | $1.75 | +19.90 | — | +0.00 | +19.90 | +99.50 | — |
| 2026-08-26 | `ALVO` | 247 | $5.05 | $4.98 | -17.29 | — | +0.00 | -17.29 | -64.22 | — |
| 2026-08-26 | `SUJA` | 147 | $9.33 | $9.39 | +8.82 | — | +0.00 | +8.82 | +88.20 | — |
| 2026-08-26 | `CYPH` | 832 | $1.64 | $1.60 | -33.28 | — | +0.00 | -33.28 | +33.28 | — |
| 2026-08-26 | `DEFT` | 2093 | $0.60 | $0.60 | -12.56 | — | +0.00 | -12.56 | -46.05 | — |
| 2026-08-26 | `ZURA` | 200 | $6.32 | $6.13 | -38.00 | — | +0.00 | -38.00 | -48.00 | — |
| 2026-08-26 | `USDE` | 1792 | — | $5.81 | +0.00 | $5.98 | +304.64 | +304.64 | +0.00 | +304.64 |
| 2026-08-27 | `USDE` | 1792 | $5.98 | $6.50 | +931.84 | — | +0.00 | +931.84 | +1236.48 | — |
| 2026-08-28 | `SIMO` | 5 | — | $252.24 | +0.00 | $245.81 | -32.15 | -32.15 | +0.00 | -32.15 |
| 2026-08-28 | `SMTC` | 10 | — | $141.76 | +0.00 | $131.17 | -105.90 | -105.90 | +0.00 | -105.90 |
| 2026-08-28 | `TTMI` | 11 | — | $122.81 | +0.00 | $118.65 | -45.76 | -45.76 | +0.00 | -45.76 |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `AVT` | 15 | — | $91.49 | +0.00 | $88.63 | -42.90 | -42.90 | +0.00 | -42.90 |
| 2026-08-28 | `CGNX` | 23 | — | $62.82 | +0.00 | $60.46 | -54.28 | -54.28 | +0.00 | -54.28 |
| 2026-08-28 | `COHR` | 5 | — | $289.44 | +0.00 | $279.20 | -51.20 | -51.20 | +0.00 | -51.20 |
| 2026-08-28 | `LSCC` | 12 | — | $119.76 | +0.00 | $114.40 | -64.32 | -64.32 | +0.00 | -64.32 |
| 2026-08-31 | `SIMO` | 5 | $245.81 | $247.05 | +6.20 | — | +0.00 | +6.20 | -25.95 | — |
| 2026-08-31 | `SMTC` | 10 | $131.17 | $132.30 | +11.30 | — | +0.00 | +11.30 | -94.60 | — |
| 2026-08-31 | `TTMI` | 11 | $118.65 | $118.83 | +1.98 | — | +0.00 | +1.98 | -43.78 | — |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `AVT` | 15 | $88.63 | $89.39 | +11.40 | — | +0.00 | +11.40 | -31.50 | — |
| 2026-08-31 | `CGNX` | 23 | $60.46 | $60.46 | +0.00 | — | +0.00 | +0.00 | -54.28 | — |
| 2026-08-31 | `COHR` | 5 | $279.20 | $280.25 | +5.25 | — | +0.00 | +5.25 | -45.95 | — |
| 2026-08-31 | `LSCC` | 12 | $114.40 | $115.56 | +13.92 | — | +0.00 | +13.92 | -50.40 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 26 | — | $52.88 | +0.00 | $52.46 | -10.92 | -10.92 | +0.00 | -10.92 |
| 2026-09-03 | `HRMY` | 32 | — | $42.93 | +0.00 | $41.86 | -34.24 | -34.24 | +0.00 | -34.24 |
| 2026-09-03 | `CABA` | 387 | — | $3.63 | +0.00 | $3.48 | -58.05 | -58.05 | +0.00 | -58.05 |
| 2026-09-03 | `VSTM` | 174 | — | $8.03 | +0.00 | $7.98 | -8.70 | -8.70 | +0.00 | -8.70 |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `ARCT` | 83 | — | $16.77 | +0.00 | $15.56 | -100.43 | -100.43 | +0.00 | -100.43 |
| 2026-09-03 | `SLN` | 94 | — | $14.85 | +0.00 | $14.79 | -5.64 | -5.64 | +0.00 | -5.64 |
| 2026-09-03 | `CRDL` | 644 | — | $2.18 | +0.00 | $2.16 | -12.88 | -12.88 | +0.00 | -12.88 |
| 2026-09-04 | `ATRC` | 26 | $52.46 | $52.03 | -11.18 | $51.52 | -13.26 | -24.44 | -22.10 | -35.36 |
| 2026-09-04 | `HRMY` | 32 | $41.86 | $41.50 | -11.52 | — | +0.00 | -11.52 | -45.76 | — |
| 2026-09-04 | `CABA` | 387 | $3.48 | $3.46 | -7.74 | $3.47 | +3.87 | -3.87 | -65.79 | -61.92 |
| 2026-09-04 | `VSTM` | 174 | $7.98 | $7.91 | -12.18 | — | +0.00 | -12.18 | -20.88 | — |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `ARCT` | 83 | $15.56 | $15.61 | +4.15 | — | +0.00 | +4.15 | -96.28 | — |
| 2026-09-04 | `SLN` | 94 | $14.79 | $14.63 | -15.04 | — | +0.00 | -15.04 | -20.68 | — |
| 2026-09-04 | `CRDL` | 644 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -12.88 | — |
| 2026-09-04 | `ALEC` | 541 | — | $2.52 | +0.00 | $2.46 | -32.46 | -32.46 | +0.00 | -32.46 |
| 2026-09-04 | `BHC` | 203 | — | $6.71 | +0.00 | $6.56 | -30.45 | -30.45 | +0.00 | -30.45 |
| 2026-09-04 | `BMEA` | 718 | — | $1.90 | +0.00 | $2.03 | +93.34 | +93.34 | +0.00 | +93.34 |
| 2026-09-04 | `OABI` | 285 | — | $4.78 | +0.00 | $4.33 | -128.25 | -128.25 | +0.00 | -128.25 |
| 2026-09-04 | `OPK` | 858 | — | $1.59 | +0.00 | $1.64 | +42.90 | +42.90 | +0.00 | +42.90 |
| 2026-09-04 | `VIR` | 118 | — | $11.31 | +0.00 | $11.38 | +8.85 | +8.85 | +0.00 | +8.85 |
| 2026-09-08 | `ATRC` | 26 | $51.52 | $54.31 | +72.54 | — | +0.00 | +72.54 | +37.18 | — |
| 2026-09-08 | `CABA` | 387 | $3.47 | $3.43 | -15.48 | — | +0.00 | -15.48 | -77.40 | — |
| 2026-09-08 | `ALEC` | 541 | $2.46 | $2.38 | -43.28 | — | +0.00 | -43.28 | -75.74 | — |
| 2026-09-08 | `BHC` | 203 | $6.56 | $6.57 | +2.03 | — | +0.00 | +2.03 | -28.42 | — |
| 2026-09-08 | `BMEA` | 718 | $2.03 | $2.00 | -21.54 | — | +0.00 | -21.54 | +71.80 | — |
| 2026-09-08 | `OABI` | 285 | $4.33 | $4.30 | -8.55 | — | +0.00 | -8.55 | -136.80 | — |
| 2026-09-08 | `OPK` | 858 | $1.64 | $1.63 | -8.58 | — | +0.00 | -8.58 | +34.32 | — |
| 2026-09-08 | `VIR` | 118 | $11.38 | $11.22 | -19.47 | — | +0.00 | -19.47 | -10.62 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 13 | — | $164.43 | +0.00 | $150.28 | -183.95 | -183.95 | +0.00 | -183.95 |
| 2026-09-11 | `BAND` | 40 | — | $52.55 | +0.00 | $56.87 | +172.80 | +172.80 | +0.00 | +172.80 |
| 2026-09-11 | `PAGS` | 211 | — | $10.11 | +0.00 | $10.12 | +2.11 | +2.11 | +0.00 | +2.11 |
| 2026-09-11 | `ZSQR` | 658 | — | $3.25 | +0.00 | $3.07 | -118.44 | -118.44 | +0.00 | -118.44 |
| 2026-09-11 | `PAYP` | 117 | — | $18.30 | +0.00 | $18.45 | +17.55 | +17.55 | +0.00 | +17.55 |
| 2026-09-14 | `ORCL` | 13 | $150.28 | $141.42 | -115.18 | — | +0.00 | -115.18 | -299.13 | — |
| 2026-09-14 | `BAND` | 40 | $56.87 | $56.90 | +1.20 | — | +0.00 | +1.20 | +174.00 | — |
| 2026-09-14 | `PAGS` | 211 | $10.12 | $10.00 | -25.32 | — | +0.00 | -25.32 | -23.21 | — |
| 2026-09-14 | `ZSQR` | 658 | $3.07 | $3.06 | -6.58 | — | +0.00 | -6.58 | -125.02 | — |
| 2026-09-14 | `PAYP` | 117 | $18.45 | $18.28 | -19.89 | — | +0.00 | -19.89 | -2.34 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +185.07 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | -100.50 | DAVE, SLG, MARA, LDI, BTBT, BETR, ANGX, HYLN | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $251.37 | $9,999.36 | DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845, BETR×85, ANGX×294, HYLN×303 |
| 2026-08-17 | +2.25 | $251.37 | DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845, BETR×85, ANGX×294, HYLN×303 | $10,039.35 | +39.99 | -338.43 | TMC, TGB, CDNL, ABX, OCC, ALM, UMAC, NPWR | DAVE, SLG, MARA, LDI, BTBT, BETR, ANGX, HYLN | $30.01 | $9,630.89 | TMC×308, TGB×147, CDNL×31, ABX×136, OCC×68, ALM×77, UMAC×38, NPWR×650 |
| 2026-08-18 | -6.20 | $30.01 | TMC×308, TGB×147, CDNL×31, ABX×136, OCC×68, ALM×77, UMAC×38, NPWR×650 | $9,457.45 | -173.44 | +0.00 | — | TMC, TGB, CDNL, ABX, OCC, ALM, UMAC, NPWR | $9,431.33 | $9,431.33 | — |
| 2026-08-19 | -7.20 | $9,431.33 | — | $9,431.33 | -0.00 | +0.00 | — | — | $9,431.33 | $9,431.33 | — |
| 2026-08-20 | +1.12 | $9,431.33 | — | $9,431.33 | -0.00 | +221.42 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $122.33 | $9,628.79 | AG×57, BHP×12, CDE×57, HDSN×204, IAG×60, KGC×39, NFGC×673, WPM×8 |
| 2026-08-21 | +3.25 | $122.33 | AG×57, BHP×12, CDE×57, HDSN×204, IAG×60, KGC×39, NFGC×673, WPM×8 | $9,882.80 | +254.01 | +247.21 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $212.62 | $10,068.49 | AU×10, AUPH×71, AEM×5, ARCT×110, AUTL×498, CRDL×638, CRSP×20, CYPH×933 |
| 2026-08-24 | -5.17 | $212.62 | AU×10, AUPH×71, AEM×5, ARCT×110, AUTL×498, CRDL×638, CRSP×20, CYPH×933 | $10,422.67 | +354.18 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,384.89 | $10,384.89 | — |
| 2026-08-25 | +1.80 | $10,384.89 | — | $10,384.89 | +0.00 | +194.67 | MOS, CRMD, BMEA, ALVO, SUJA, CYPH, DEFT, ZURA | — | $0.52 | $10,526.49 | MOS×54, CRMD×155, BMEA×796, ALVO×247, SUJA×147, CYPH×832, DEFT×2093, ZURA×200 |
| 2026-08-26 | +2.02 | $0.52 | MOS×54, CRMD×155, BMEA×796, ALVO×247, SUJA×147, CYPH×832, DEFT×2093, ZURA×200 | $10,491.06 | -35.43 | +304.64 | USDE | MOS, CRMD, BMEA, ALVO, SUJA, CYPH, DEFT, ZURA | $2.98 | $10,719.14 | USDE×1792 |
| 2026-08-27 | — | $2.98 | USDE×1792 | $11,650.98 | +931.84 | +0.00 | — | USDE | $11,627.47 | $11,627.47 | — |
| 2026-08-28 | +0.75 | $11,627.47 | — | $11,627.47 | +0.00 | -414.27 | SIMO, SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC | — | $582.42 | $11,197.03 | SIMO×5, SMTC×10, TTMI×11, KEYS×4, AVT×15, CGNX×23, COHR×5, LSCC×12 |
| 2026-08-31 | -5.85 | $582.42 | SIMO×5, SMTC×10, TTMI×11, KEYS×4, AVT×15, CGNX×23, COHR×5, LSCC×12 | $11,257.16 | +60.13 | +0.00 | — | SIMO, SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC | $11,240.82 | $11,240.82 | — |
| 2026-09-01 | -6.30 | $11,240.82 | — | $11,240.82 | -0.00 | +0.00 | — | — | $11,240.82 | $11,240.82 | — |
| 2026-09-02 | -3.83 | $11,240.82 | — | $11,240.82 | -0.00 | +0.00 | — | — | $11,240.82 | $11,240.82 | — |
| 2026-09-03 | -0.90 | $11,240.82 | — | $11,240.82 | -0.00 | -249.06 | ATRC, HRMY, CABA, VSTM, RVTY, ARCT, SLN, CRDL | — | $147.42 | $10,965.26 | ATRC×26, HRMY×32, CABA×387, VSTM×174, RVTY×10, ARCT×83, SLN×94, CRDL×644 |
| 2026-09-04 | +2.25 | $147.42 | ATRC×26, HRMY×32, CABA×387, VSTM×174, RVTY×10, ARCT×83, SLN×94, CRDL×644 | $10,905.75 | -59.51 | -55.46 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY, ARCT, SLN, CRDL | $7.57 | $10,794.66 | ATRC×26, CABA×387, ALEC×541, BHC×203, BMEA×718, OABI×285, OPK×858, VIR×118 |
| 2026-09-08 | -11.47 | $7.57 | ATRC×26, CABA×387, ALEC×541, BHC×203, BMEA×718, OABI×285, OPK×858, VIR×118 | $10,752.33 | -42.33 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $10,708.70 | $10,708.70 | — |
| 2026-09-09 | -13.95 | $10,708.70 | — | $10,708.70 | +0.00 | +0.00 | — | — | $10,708.70 | $10,708.70 | — |
| 2026-09-10 | -13.28 | $10,708.70 | — | $10,708.70 | +0.00 | +0.00 | — | — | $10,708.70 | $10,708.70 | — |
| 2026-09-11 | +0.50 | $10,708.70 | — | $10,708.70 | +0.00 | -109.93 | ORCL, BAND, PAGS, ZSQR, PAYP | — | $38.61 | $10,581.08 | ORCL×13, BAND×40, PAGS×211, ZSQR×658, PAYP×117 |
| 2026-09-14 | -11.00 | $38.61 | ORCL×13, BAND×40, PAGS×211, ZSQR×658, PAYP×117 | $10,415.31 | -165.77 | +0.00 | — | ORCL, BAND, PAGS, ZSQR, PAYP | $10,397.36 | $10,397.36 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟡 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟡 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟡 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟡 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟡 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟡 |
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
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $9,149.18 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
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
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 308 | $4.05 | $3.97 | — | $8,743.73 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1249.39 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 147 | $8.46 | $2.43 | — | $7,497.68 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1249.39 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $6,260.25 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1249.39 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 136 | $9.12 | $2.40 | — | $5,017.53 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1249.39 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 68 | $18.24 | $2.19 | — | $3,775.02 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1249.39 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $2,525.39 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1249.39 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 38 | $32.55 | $2.10 | — | $1,286.39 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1249.39 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 650 | $1.92 | $8.38 | — | $30.01 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1249.39 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.01 | ▼ close $9,630.89 vs 09:30 $10,039.35 (session -338.43) | 16:00 close · cash $30.01 · equity $9,630.89 vs 09:30 $10,039.35 (-408.46; session marks -338.43) · 8 name(s) marked open→close (per-name table). TMC×308 09:30 $4.05 → close $3.77 -86.24; TGB×147 09:30 $8.46 → close $8.77 +45.57; CDNL×31 09:30 $39.85 → close $39.23 -19.22; ABX×136 09:30 $9.12 → close $9.12 +0.00; OCC×68 09:30 $18.24 → close $17.12 -76.16; ALM×77 09:30 $16.20 → close $16.36 +12.32; UMAC×38 09:30 $32.55 → close $30.15 -91.20; NPWR×650 09:30 $1.92 → close $1.73 -123.50 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.01 | ▼ 09:30 equity $9,457.45 vs yday $9,630.89 (-173.44) | 09:30 open · cash $30.01 (unchanged overnight, no fees) · equity $9,457.45 vs prior close $9,630.89 (-173.44) · 8 name(s) re-marked at the open (per-name table). TMC×308 yday $3.77 → 09:30 $3.72 -15.40; TGB×147 yday $8.77 → 09:30 $8.55 -32.34; CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; ABX×136 yday $9.12 → 09:30 $9.03 -12.24; OCC×68 yday $17.12 → 09:30 $16.20 -62.56; ALM×77 yday $16.36 → 09:30 $15.78 -44.66; UMAC×38 yday $30.15 → 09:30 $28.59 -59.28; NPWR×650 yday $1.73 → 09:30 $1.70 -19.50 | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 308 | $3.72 | $4.03 | $-109.65 | $1,171.73 | ▼ -109.65 after sell → book $9,453.41; vs 09:30 mark -4.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 147 | $8.55 | $2.47 | $+8.33 | $2,426.12 | ▲ +8.33 after sell → book $9,450.95; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $3,712.68 | ▲ +49.13 after sell → book $9,448.84; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 136 | $9.03 | $2.43 | $-17.07 | $4,938.33 | ▼ -17.07 after sell → book $9,446.41; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 68 | $16.20 | $2.22 | $-143.13 | $6,037.72 | ▼ -143.13 after sell → book $9,444.20; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $7,250.53 | ▼ -36.80 after sell → book $9,441.95; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 38 | $28.59 | $2.12 | $-154.71 | $8,334.83 | ▼ -154.71 after sell → book $9,439.83; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 650 | $1.70 | $8.50 | $-159.89 | $9,431.33 | ▼ -159.89 after sell → book $9,431.33; vs 09:30 mark -8.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.33 | ▲ close $9,431.33 vs 09:30 $9,457.45 (session +0.00) | 16:00 close · cash $9,431.33 · no lots left · equity $9,431.33. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,431.33 | ▲ 09:30 equity $9,431.33 vs yday $9,431.33 (-0.00) | 09:30 open · cash $9,431.33 · no holdings · equity $9,431.33 vs prior close $9,431.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.33 | ▲ close $9,431.33 vs 09:30 $9,431.33 (session +0.00) | 16:00 close · cash $9,431.33 · no lots left · equity $9,431.33. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,431.33 | ▲ 09:30 equity $9,431.33 vs yday $9,431.33 (-0.00) | 09:30 open · cash $9,431.33 · no holdings · equity $9,431.33 vs prior close $9,431.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,257.82 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1178.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $7,163.67 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1178.92 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $5,984.46 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1178.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 204 | $5.77 | $2.63 | — | $4,804.75 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1178.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $3,624.78 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1178.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $2,467.10 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1178.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 673 | $1.75 | $8.68 | — | $1,280.67 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1178.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $122.33 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1178.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.33 | ▲ close $9,628.79 vs 09:30 $9,431.33 (session +221.42) | 16:00 close · cash $122.33 · equity $9,628.79 vs 09:30 $9,431.33 (+197.46; session marks +221.42) · 8 name(s) marked open→close (per-name table). AG×57 09:30 $20.55 → close $21.19 +36.48; BHP×12 09:30 $91.01 → close $93.63 +31.44; CDE×57 09:30 $20.65 → close $21.11 +26.22; HDSN×204 09:30 $5.77 → close $5.57 -40.80; IAG×60 09:30 $19.63 → close $20.50 +52.20; KGC×39 09:30 $29.63 → close $31.43 +70.20; NFGC×673 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.33 | ▲ 09:30 equity $9,882.80 vs yday $9,628.79 (+254.01) | 09:30 open · cash $122.33 (unchanged overnight, no fees) · equity $9,882.80 vs prior close $9,628.79 (+254.01) · 8 name(s) re-marked at the open (per-name table). AG×57 yday $21.19 → 09:30 $21.90 +40.47; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×57 yday $21.11 → 09:30 $21.75 +36.48; HDSN×204 yday $5.57 → 09:30 $5.67 +20.40; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×673 yday $1.75 → 09:30 $1.79 +26.92; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 57 | $21.90 | $2.18 | $+72.61 | $1,368.45 | ▲ +72.61 after sell → book $9,880.62; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 12 | $95.72 | $2.05 | $+52.45 | $2,515.05 | ▲ +52.45 after sell → book $9,878.58; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 57 | $21.75 | $2.18 | $+58.36 | $3,752.62 | ▲ +58.36 after sell → book $9,876.40; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 204 | $5.67 | $2.68 | $-25.71 | $4,906.62 | ▼ -25.71 after sell → book $9,873.72; vs 09:30 mark -2.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 60 | $21.17 | $2.19 | $+88.04 | $6,174.63 | ▲ +88.04 after sell → book $9,871.53; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 39 | $32.17 | $2.13 | $+94.83 | $7,427.13 | ▲ +94.83 after sell → book $9,869.40; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 673 | $1.79 | $8.80 | $+9.43 | $8,623.00 | ▲ +9.43 after sell → book $9,860.60; vs 09:30 mark -8.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,858.57 | ▲ +77.23 after sell → book $9,858.57; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,662.25 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1232.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 71 | $17.20 | $2.20 | — | $7,438.84 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1232.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,355.34 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1232.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 110 | $11.13 | $2.32 | — | $5,128.72 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1232.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 498 | $2.47 | $6.42 | — | $3,892.23 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1232.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 638 | $1.93 | $8.23 | — | $2,652.66 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1232.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $1,456.21 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1232.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 933 | $1.32 | $12.04 | — | $212.62 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1232.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $212.62 | ▲ close $10,068.49 vs 09:30 $9,882.80 (session +247.21) | 16:00 close · cash $212.62 · equity $10,068.49 vs 09:30 $9,882.80 (+185.69; session marks +247.21) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×71 09:30 $17.20 → close $16.65 -39.05; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×110 09:30 $11.13 → close $13.45 +255.20; AUTL×498 09:30 $2.47 → close $2.41 -29.88; CRDL×638 09:30 $1.93 → close $1.86 -44.66; CRSP×20 09:30 $59.72 → close $59.50 -4.40; CYPH×933 09:30 $1.32 → close $1.42 +93.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.62 | ▲ 09:30 equity $10,422.67 vs yday $10,068.49 (+354.18) | 09:30 open · cash $212.62 (unchanged overnight, no fees) · equity $10,422.67 vs prior close $10,068.49 (+354.18) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×71 yday $16.65 → 09:30 $16.57 -5.68; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×110 yday $13.45 → 09:30 $13.33 -13.20; AUTL×498 yday $2.41 → 09:30 $2.40 -4.98; CRDL×638 yday $1.86 → 09:30 $1.88 +12.76; CRSP×20 yday $59.50 → 09:30 $58.75 -15.00; CYPH×933 yday $1.42 → 09:30 $1.83 +382.53 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,415.68 | ▲ +6.74 after sell → book $10,420.63; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 71 | $16.57 | $2.22 | $-49.16 | $2,589.92 | ▼ -49.16 after sell → book $10,418.40; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,673.05 | ▼ -0.38 after sell → book $10,416.38; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 110 | $13.33 | $2.35 | $+237.33 | $5,137.00 | ▲ +237.33 after sell → book $10,414.03; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 498 | $2.40 | $6.52 | $-47.80 | $6,325.68 | ▼ -47.80 after sell → book $10,407.51; vs 09:30 mark -6.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 638 | $1.88 | $8.35 | $-48.48 | $7,516.77 | ▼ -48.48 after sell → book $10,399.16; vs 09:30 mark -8.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 20 | $58.75 | $2.07 | $-23.52 | $8,689.70 | ▼ -23.52 after sell → book $10,397.09; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 933 | $1.83 | $12.20 | $+451.59 | $10,384.89 | ▲ +451.59 after sell → book $10,384.89; vs 09:30 mark -12.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,384.89 | ▲ close $10,384.89 vs 09:30 $10,422.67 (session +0.00) | 16:00 close · cash $10,384.89 · no lots left · equity $10,384.89. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,384.89 | ▲ 09:30 equity $10,384.89 vs yday $10,384.89 (+0.00) | 09:30 open · cash $10,384.89 · no holdings · equity $10,384.89 vs prior close $10,384.89 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 54 | $23.77 | $2.15 | — | $9,099.16 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.0; leftover $1298.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 155 | $8.35 | $2.46 | — | $7,802.45 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1298.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 796 | $1.63 | $10.27 | — | $6,494.71 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1298.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 247 | $5.24 | $3.19 | — | $5,197.24 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1298.11 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 147 | $8.79 | $2.43 | — | $3,902.68 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1298.11 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 832 | $1.56 | $10.73 | — | $2,594.03 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1298.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2093 | $0.62 | $19.26 | — | $1,277.11 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1298.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 200 | $6.37 | $2.59 | — | $0.52 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1298.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.52 | ▲ close $10,526.49 vs 09:30 $10,384.89 (session +194.67) | 16:00 close · cash $0.52 · equity $10,526.49 vs 09:30 $10,384.89 (+141.60; session marks +194.67) · 8 name(s) marked open→close (per-name table). MOS×54 09:30 $23.77 → close $24.27 +27.00; CRMD×155 09:30 $8.35 → close $8.56 +32.55; BMEA×796 09:30 $1.63 → close $1.73 +79.60; ALVO×247 09:30 $5.24 → close $5.05 -46.93; SUJA×147 09:30 $8.79 → close $9.33 +79.38; CYPH×832 09:30 $1.56 → close $1.64 +66.56; DEFT×2093 09:30 $0.62 → close $0.60 -33.49; ZURA×200 09:30 $6.37 → close $6.32 -10.00 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.52 | ▼ 09:30 equity $10,491.06 vs yday $10,526.49 (-35.43) | 09:30 open · cash $0.52 (unchanged overnight, no fees) · equity $10,491.06 vs prior close $10,526.49 (-35.43) · 8 name(s) re-marked at the open (per-name table). MOS×54 yday $24.27 → 09:30 $24.84 +30.78; CRMD×155 yday $8.56 → 09:30 $8.60 +6.20; BMEA×796 yday $1.73 → 09:30 $1.75 +19.90; ALVO×247 yday $5.05 → 09:30 $4.98 -17.29; SUJA×147 yday $9.33 → 09:30 $9.39 +8.82; CYPH×832 yday $1.64 → 09:30 $1.60 -33.28; DEFT×2093 yday $0.60 → 09:30 $0.60 -12.56; ZURA×200 yday $6.32 → 09:30 $6.13 -38.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `MOS` | 54 | $24.84 | $2.17 | $+53.46 | $1,339.71 | ▲ +53.46 after sell → book $10,488.89; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 155 | $8.60 | $2.49 | $+33.80 | $2,670.22 | ▲ +33.80 after sell → book $10,486.40; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 796 | $1.75 | $10.41 | $+78.82 | $4,056.78 | ▲ +78.82 after sell → book $10,475.99; vs 09:30 mark -10.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 247 | $4.98 | $3.24 | $-70.64 | $5,283.61 | ▼ -70.64 after sell → book $10,472.75; vs 09:30 mark -3.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUJA` | 147 | $9.39 | $2.47 | $+83.30 | $6,661.47 | ▲ +83.30 after sell → book $10,470.28; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 832 | $1.60 | $10.88 | $+11.67 | $7,981.79 | ▲ +11.67 after sell → book $10,459.40; vs 09:30 mark -10.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DEFT` | 2093 | $0.60 | $19.15 | $-84.45 | $9,214.25 | ▼ -84.45 after sell → book $10,440.25; vs 09:30 mark -19.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 200 | $6.13 | $2.63 | $-53.22 | $10,437.62 | ▼ -53.22 after sell → book $10,437.62; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 1792 | $5.81 | $23.12 | — | $2.98 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $10437.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.98 | ▲ close $10,719.14 vs 09:30 $10,491.06 (session +304.64) | 16:00 close · cash $2.98 · equity $10,719.14 vs 09:30 $10,491.06 (+228.08; session marks +304.64) · 1 name(s) marked open→close (per-name table). USDE×1792 09:30 $5.81 → close $5.98 +304.64 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.98 | ▲ 09:30 equity $11,650.98 vs yday $10,719.14 (+931.84) | 09:30 open · cash $2.98 (unchanged overnight, no fees) · equity $11,650.98 vs prior close $10,719.14 (+931.84) · 1 name(s) re-marked at the open (per-name table). USDE×1792 yday $5.98 → 09:30 $6.50 +931.84 | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 1792 | $6.50 | $23.51 | $+1189.86 | $11,627.47 | ▲ +1,189.86 after sell → book $11,627.47; vs 09:30 mark -23.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,627.47 | ▲ close $11,627.47 vs 09:30 $11,650.98 (session +0.00) | 16:00 close · cash $11,627.47 · no lots left · equity $11,627.47. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,627.47 | ▲ 09:30 equity $11,627.47 vs yday $11,627.47 (+0.00) | 09:30 open · cash $11,627.47 · no holdings · equity $11,627.47 vs prior close $11,627.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $10,364.27 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1453.43 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 10 | $141.76 | $2.02 | — | $8,944.65 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1453.43 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 11 | $122.81 | $2.02 | — | $7,591.71 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1453.43 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $6,292.07 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1453.43 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 15 | $91.49 | $2.04 | — | $4,917.69 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1453.43 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 23 | $62.82 | $2.06 | — | $3,470.77 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1453.43 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 5 | $289.44 | $2.00 | — | $2,021.56 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1453.43 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 12 | $119.76 | $2.03 | — | $582.42 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1453.43 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $582.42 | ▼ close $11,197.03 vs 09:30 $11,627.47 (session -414.27) | 16:00 close · cash $582.42 · equity $11,197.03 vs 09:30 $11,627.47 (-430.44; session marks -414.27) · 8 name(s) marked open→close (per-name table). SIMO×5 09:30 $252.24 → close $245.81 -32.15; SMTC×10 09:30 $141.76 → close $131.17 -105.90; TTMI×11 09:30 $122.81 → close $118.65 -45.76; KEYS×4 09:30 $324.41 → close $319.97 -17.76; AVT×15 09:30 $91.49 → close $88.63 -42.90; CGNX×23 09:30 $62.82 → close $60.46 -54.28; COHR×5 09:30 $289.44 → close $279.20 -51.20; LSCC×12 09:30 $119.76 → close $114.40 -64.32 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $582.42 | ▲ 09:30 equity $11,257.16 vs yday $11,197.03 (+60.13) | 09:30 open · cash $582.42 (unchanged overnight, no fees) · equity $11,257.16 vs prior close $11,197.03 (+60.13) · 8 name(s) re-marked at the open (per-name table). SIMO×5 yday $245.81 → 09:30 $247.05 +6.20; SMTC×10 yday $131.17 → 09:30 $132.30 +11.30; TTMI×11 yday $118.65 → 09:30 $118.83 +1.98; KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; AVT×15 yday $88.63 → 09:30 $89.39 +11.40; CGNX×23 yday $60.46 → 09:30 $60.46 +0.00; COHR×5 yday $279.20 → 09:30 $280.25 +5.25; LSCC×12 yday $114.40 → 09:30 $115.56 +13.92 | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $1,815.64 | ▼ -29.98 after sell → book $11,255.13; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 10 | $132.30 | $2.04 | $-98.66 | $3,136.60 | ▼ -98.66 after sell → book $11,253.09; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 11 | $118.83 | $2.04 | $-47.85 | $4,441.69 | ▼ -47.85 after sell → book $11,251.05; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $5,729.63 | ▼ -11.70 after sell → book $11,249.03; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 15 | $89.39 | $2.06 | $-35.59 | $7,068.42 | ▼ -35.59 after sell → book $11,246.97; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 23 | $60.46 | $2.08 | $-58.42 | $8,456.92 | ▼ -58.42 after sell → book $11,244.89; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 5 | $280.25 | $2.03 | $-49.98 | $9,856.14 | ▼ -49.98 after sell → book $11,242.86; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 12 | $115.56 | $2.05 | $-54.47 | $11,240.82 | ▼ -54.47 after sell → book $11,240.82; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,240.82 | ▲ close $11,240.82 vs 09:30 $11,257.16 (session +0.00) | 16:00 close · cash $11,240.82 · no lots left · equity $11,240.82. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,240.82 | ▲ 09:30 equity $11,240.82 vs yday $11,240.82 (-0.00) | 09:30 open · cash $11,240.82 · no holdings · equity $11,240.82 vs prior close $11,240.82 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,240.82 | ▲ close $11,240.82 vs 09:30 $11,240.82 (session +0.00) | 16:00 close · cash $11,240.82 · no lots left · equity $11,240.82. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,240.82 | ▲ 09:30 equity $11,240.82 vs yday $11,240.82 (-0.00) | 09:30 open · cash $11,240.82 · no holdings · equity $11,240.82 vs prior close $11,240.82 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,240.82 | ▲ close $11,240.82 vs 09:30 $11,240.82 (session +0.00) | 16:00 close · cash $11,240.82 · no lots left · equity $11,240.82. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,240.82 | ▲ 09:30 equity $11,240.82 vs yday $11,240.82 (-0.00) | 09:30 open · cash $11,240.82 · no holdings · equity $11,240.82 vs prior close $11,240.82 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $52.88 | $2.07 | — | $9,863.87 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1405.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $42.93 | $2.09 | — | $8,488.02 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1405.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 387 | $3.63 | $4.99 | — | $7,078.22 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1405.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 174 | $8.03 | $2.51 | — | $5,678.49 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1405.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,351.97 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1405.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 83 | $16.77 | $2.24 | — | $2,957.82 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1405.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 94 | $14.85 | $2.27 | — | $1,559.65 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1405.10 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 644 | $2.18 | $8.31 | — | $147.42 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1405.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $147.42 | ▼ close $10,965.26 vs 09:30 $11,240.82 (session -249.06) | 16:00 close · cash $147.42 · equity $10,965.26 vs 09:30 $11,240.82 (-275.56; session marks -249.06) · 8 name(s) marked open→close (per-name table). ATRC×26 09:30 $52.88 → close $52.46 -10.92; HRMY×32 09:30 $42.93 → close $41.86 -34.24; CABA×387 09:30 $3.63 → close $3.48 -58.05; VSTM×174 09:30 $8.03 → close $7.98 -8.70; RVTY×10 09:30 $132.45 → close $130.63 -18.20; ARCT×83 09:30 $16.77 → close $15.56 -100.43; SLN×94 09:30 $14.85 → close $14.79 -5.64; CRDL×644 09:30 $2.18 → close $2.16 -12.88 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $147.42 | ▼ 09:30 equity $10,905.75 vs yday $10,965.26 (-59.51) | 09:30 open · cash $147.42 (unchanged overnight, no fees) · equity $10,905.75 vs prior close $10,965.26 (-59.51) · 8 name(s) re-marked at the open (per-name table). ATRC×26 yday $52.46 → 09:30 $52.03 -11.18; HRMY×32 yday $41.86 → 09:30 $41.50 -11.52; CABA×387 yday $3.48 → 09:30 $3.46 -7.74; VSTM×174 yday $7.98 → 09:30 $7.91 -12.18; RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; ARCT×83 yday $15.56 → 09:30 $15.61 +4.15; SLN×94 yday $14.79 → 09:30 $14.63 -15.04; CRDL×644 yday $2.16 → 09:30 $2.16 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 32 | $41.50 | $2.11 | $-49.95 | $1,473.31 | ▼ -49.95 after sell → book $10,903.64; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 174 | $7.91 | $2.55 | $-25.94 | $2,847.10 | ▼ -25.94 after sell → book $10,901.09; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $4,145.36 | ▼ -28.26 after sell → book $10,899.05; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 83 | $15.61 | $2.26 | $-100.78 | $5,438.73 | ▼ -100.78 after sell → book $10,896.79; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 94 | $14.63 | $2.30 | $-25.25 | $6,811.65 | ▼ -25.25 after sell → book $10,894.49; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 644 | $2.16 | $8.43 | $-29.61 | $8,194.26 | ▼ -29.61 after sell → book $10,886.06; vs 09:30 mark -8.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 541 | $2.52 | $6.98 | — | $6,823.96 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1365.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 203 | $6.71 | $2.62 | — | $5,459.22 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1365.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 718 | $1.90 | $9.26 | — | $4,085.75 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1365.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 285 | $4.78 | $3.68 | — | $2,719.78 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1365.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 858 | $1.59 | $11.07 | — | $1,344.49 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1365.71 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 118 | $11.31 | $2.34 | — | $7.57 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1365.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.57 | ▼ close $10,794.66 vs 09:30 $10,905.75 (session -55.46) | 16:00 close · cash $7.57 · equity $10,794.66 vs 09:30 $10,905.75 (-111.09; session marks -55.46) · 8 name(s) marked open→close (per-name table). ATRC×26 09:30 $52.03 → close $51.52 -13.26; CABA×387 09:30 $3.46 → close $3.47 +3.87; ALEC×541 09:30 $2.52 → close $2.46 -32.46; BHC×203 09:30 $6.71 → close $6.56 -30.45; BMEA×718 09:30 $1.90 → close $2.03 +93.34; OABI×285 09:30 $4.78 → close $4.33 -128.25; OPK×858 09:30 $1.59 → close $1.64 +42.90; VIR×118 09:30 $11.31 → close $11.38 +8.85 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.57 | ▼ 09:30 equity $10,752.33 vs yday $10,794.66 (-42.33) | 09:30 open · cash $7.57 (unchanged overnight, no fees) · equity $10,752.33 vs prior close $10,794.66 (-42.33) · 8 name(s) re-marked at the open (per-name table). ATRC×26 yday $51.52 → 09:30 $54.31 +72.54; CABA×387 yday $3.47 → 09:30 $3.43 -15.48; ALEC×541 yday $2.46 → 09:30 $2.38 -43.28; BHC×203 yday $6.56 → 09:30 $6.57 +2.03; BMEA×718 yday $2.03 → 09:30 $2.00 -21.54; OABI×285 yday $4.33 → 09:30 $4.30 -8.55; OPK×858 yday $1.64 → 09:30 $1.63 -8.58; VIR×118 yday $11.38 → 09:30 $11.22 -19.47 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 26 | $54.31 | $2.09 | $+33.02 | $1,417.54 | ▲ +33.02 after sell → book $10,750.24; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 387 | $3.43 | $5.07 | $-87.46 | $2,739.88 | ▼ -87.46 after sell → book $10,745.17; vs 09:30 mark -5.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 541 | $2.38 | $7.08 | $-89.80 | $4,020.38 | ▼ -89.80 after sell → book $10,738.09; vs 09:30 mark -7.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 203 | $6.57 | $2.66 | $-33.70 | $5,351.43 | ▼ -33.70 after sell → book $10,735.43; vs 09:30 mark -2.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 718 | $2.00 | $9.39 | $+53.14 | $6,778.03 | ▲ +53.14 after sell → book $10,726.03; vs 09:30 mark -9.40 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 285 | $4.30 | $3.73 | $-144.21 | $7,999.80 | ▼ -144.21 after sell → book $10,722.30; vs 09:30 mark -3.73 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 858 | $1.63 | $11.22 | $+12.03 | $9,387.12 | ▲ +12.03 after sell → book $10,711.08; vs 09:30 mark -11.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 118 | $11.22 | $2.37 | $-15.34 | $10,708.70 | ▼ -15.34 after sell → book $10,708.70; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,708.70 | ▲ close $10,708.70 vs 09:30 $10,752.33 (session +0.00) | 16:00 close · cash $10,708.70 · no lots left · equity $10,708.70. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,708.70 | ▲ 09:30 equity $10,708.70 vs yday $10,708.70 (+0.00) | 09:30 open · cash $10,708.70 · no holdings · equity $10,708.70 vs prior close $10,708.70 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,708.70 | ▲ close $10,708.70 vs 09:30 $10,708.70 (session +0.00) | 16:00 close · cash $10,708.70 · no lots left · equity $10,708.70. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,708.70 | ▲ 09:30 equity $10,708.70 vs yday $10,708.70 (+0.00) | 09:30 open · cash $10,708.70 · no holdings · equity $10,708.70 vs prior close $10,708.70 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,708.70 | ▲ close $10,708.70 vs 09:30 $10,708.70 (session +0.00) | 16:00 close · cash $10,708.70 · no lots left · equity $10,708.70. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,708.70 | ▲ 09:30 equity $10,708.70 vs yday $10,708.70 (+0.00) | 09:30 open · cash $10,708.70 · no holdings · equity $10,708.70 vs prior close $10,708.70 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 13 | $164.43 | $2.03 | — | $8,569.09 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,earn_react; ⚪; ret5=+4.9; leftover $2141.74 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 40 | $52.55 | $2.11 | — | $6,464.98 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $2141.74 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 211 | $10.11 | $2.72 | — | $4,329.04 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $2141.74 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 658 | $3.25 | $8.49 | — | $2,182.05 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+4.7; leftover $2141.74 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 117 | $18.30 | $2.34 | — | $38.61 | — | union ∩ white, no 🚨; gate zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $2141.74 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.61 | ▼ close $10,581.08 vs 09:30 $10,708.70 (session -109.93) | 16:00 close · cash $38.61 · equity $10,581.08 vs 09:30 $10,708.70 (-127.62; session marks -109.93) · 5 name(s) marked open→close (per-name table). ORCL×13 09:30 $164.43 → close $150.28 -183.95; BAND×40 09:30 $52.55 → close $56.87 +172.80; PAGS×211 09:30 $10.11 → close $10.12 +2.11; ZSQR×658 09:30 $3.25 → close $3.07 -118.44; PAYP×117 09:30 $18.30 → close $18.45 +17.55 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.61 | ▼ 09:30 equity $10,415.31 vs yday $10,581.08 (-165.77) | 09:30 open · cash $38.61 (unchanged overnight, no fees) · equity $10,415.31 vs prior close $10,581.08 (-165.77) · 5 name(s) re-marked at the open (per-name table). ORCL×13 yday $150.28 → 09:30 $141.42 -115.18; BAND×40 yday $56.87 → 09:30 $56.90 +1.20; PAGS×211 yday $10.12 → 09:30 $10.00 -25.32; ZSQR×658 yday $3.07 → 09:30 $3.06 -6.58; PAYP×117 yday $18.45 → 09:30 $18.28 -19.89 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 13 | $141.42 | $2.05 | $-303.21 | $1,875.02 | ▼ -303.21 after sell → book $10,413.26; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 40 | $56.90 | $2.14 | $+169.75 | $4,148.88 | ▲ +169.75 after sell → book $10,411.12; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 211 | $10.00 | $2.77 | $-28.71 | $6,256.11 | ▼ -28.71 after sell → book $10,408.35; vs 09:30 mark -2.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ZSQR` | 658 | $3.06 | $8.61 | $-142.12 | $8,260.97 | ▼ -142.12 after sell → book $10,399.73; vs 09:30 mark -8.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 117 | $18.28 | $2.38 | $-7.06 | $10,397.36 | ▼ -7.06 after sell → book $10,397.36; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,397.36 | ▲ close $10,397.36 vs 09:30 $10,415.31 (session +0.00) | 16:00 close · cash $10,397.36 · no lots left · equity $10,397.36. | — |
