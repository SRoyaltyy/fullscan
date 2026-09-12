# Factor mine action — `union_break10_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ break10, no 🚨

Cash book **-7.82%** ($9,218) · signal-only (no cash/fees) was +6.01%. Starts YES **5/21**. Fills 152 · skips 73 · realized $-625.40.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the name broke its prior 10-session range.
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
- **Gate** `break_10=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $105.98.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `IREN` | 217 | — | $45.98 | +0.00 | $44.76 | -264.74 | -264.74 | +0.00 | -264.74 |
| 2026-08-14 | `IREN` | 217 | $44.76 | $44.09 | -145.39 | — | +0.00 | -145.39 | -410.13 | — |
| 2026-08-14 | `SLG` | 20 | — | $57.61 | +0.00 | $56.09 | -30.40 | -30.40 | +0.00 | -30.40 |
| 2026-08-14 | `ADUR` | 72 | — | $16.50 | +0.00 | $16.17 | -23.76 | -23.76 | +0.00 | -23.76 |
| 2026-08-14 | `ARX` | 61 | — | $19.57 | +0.00 | $19.58 | +0.61 | +0.61 | +0.00 | +0.61 |
| 2026-08-14 | `AIRO` | 107 | — | $11.12 | +0.00 | $9.57 | -165.85 | -165.85 | +0.00 | -165.85 |
| 2026-08-14 | `TBBB` | 24 | — | $48.82 | +0.00 | $47.79 | -24.72 | -24.72 | +0.00 | -24.72 |
| 2026-08-14 | `AMPY` | 242 | — | $4.94 | +0.00 | $4.78 | -38.72 | -38.72 | +0.00 | -38.72 |
| 2026-08-14 | `MH` | 88 | — | $13.55 | +0.00 | $13.10 | -39.60 | -39.60 | +0.00 | -39.60 |
| 2026-08-17 | `SLG` | 20 | $56.09 | $55.37 | -14.40 | — | +0.00 | -14.40 | -44.80 | — |
| 2026-08-17 | `ADUR` | 72 | $16.17 | $15.73 | -31.68 | — | +0.00 | -31.68 | -55.44 | — |
| 2026-08-17 | `ARX` | 61 | $19.58 | $19.57 | -0.61 | — | +0.00 | -0.61 | +0.00 | — |
| 2026-08-17 | `AIRO` | 107 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -165.85 | — |
| 2026-08-17 | `TBBB` | 24 | $47.79 | $47.39 | -9.60 | — | +0.00 | -9.60 | -34.32 | — |
| 2026-08-17 | `AMPY` | 242 | $4.78 | $4.86 | +19.36 | — | +0.00 | +19.36 | -19.36 | — |
| 2026-08-17 | `MH` | 88 | $13.10 | $13.16 | +5.28 | — | +0.00 | +5.28 | -34.32 | — |
| 2026-08-17 | `DVN` | 24 | — | $46.18 | +0.00 | $47.57 | +33.36 | +33.36 | +0.00 | +33.36 |
| 2026-08-17 | `OCC` | 63 | — | $18.24 | +0.00 | $17.12 | -70.56 | -70.56 | +0.00 | -70.56 |
| 2026-08-17 | `ALM` | 70 | — | $16.20 | +0.00 | $16.36 | +11.20 | +11.20 | +0.00 | +11.20 |
| 2026-08-17 | `CAPR` | 167 | — | $6.87 | +0.00 | $7.45 | +96.86 | +96.86 | +0.00 | +96.86 |
| 2026-08-17 | `HTFL` | 27 | — | $41.23 | +0.00 | $41.94 | +19.17 | +19.17 | +0.00 | +19.17 |
| 2026-08-17 | `UMAC` | 35 | — | $32.55 | +0.00 | $30.15 | -84.00 | -84.00 | +0.00 | -84.00 |
| 2026-08-17 | `NPWR` | 598 | — | $1.92 | +0.00 | $1.73 | -113.62 | -113.62 | +0.00 | -113.62 |
| 2026-08-17 | `LPTH` | 76 | — | $14.94 | +0.00 | $14.80 | -10.64 | -10.64 | +0.00 | -10.64 |
| 2026-08-18 | `DVN` | 24 | $47.57 | $48.00 | +10.32 | — | +0.00 | +10.32 | +43.68 | — |
| 2026-08-18 | `OCC` | 63 | $17.12 | $16.20 | -57.96 | — | +0.00 | -57.96 | -128.52 | — |
| 2026-08-18 | `ALM` | 70 | $16.36 | $15.78 | -40.60 | — | +0.00 | -40.60 | -29.40 | — |
| 2026-08-18 | `CAPR` | 167 | $7.45 | $7.50 | +8.35 | — | +0.00 | +8.35 | +105.21 | — |
| 2026-08-18 | `HTFL` | 27 | $41.94 | $41.50 | -11.88 | — | +0.00 | -11.88 | +7.29 | — |
| 2026-08-18 | `UMAC` | 35 | $30.15 | $28.59 | -54.60 | — | +0.00 | -54.60 | -138.60 | — |
| 2026-08-18 | `NPWR` | 598 | $1.73 | $1.70 | -17.94 | — | +0.00 | -17.94 | -131.56 | — |
| 2026-08-18 | `LPTH` | 76 | $14.80 | $14.01 | -60.04 | — | +0.00 | -60.04 | -70.68 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 53 | — | $20.55 | +0.00 | $21.19 | +33.92 | +33.92 | +0.00 | +33.92 |
| 2026-08-20 | `BHP` | 12 | — | $91.01 | +0.00 | $93.63 | +31.44 | +31.44 | +0.00 | +31.44 |
| 2026-08-20 | `CDE` | 53 | — | $20.65 | +0.00 | $21.11 | +24.38 | +24.38 | +0.00 | +24.38 |
| 2026-08-20 | `IAG` | 56 | — | $19.63 | +0.00 | $20.50 | +48.72 | +48.72 | +0.00 | +48.72 |
| 2026-08-20 | `KGC` | 37 | — | $29.63 | +0.00 | $31.43 | +66.60 | +66.60 | +0.00 | +66.60 |
| 2026-08-20 | `NFGC` | 629 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 7 | — | $144.54 | +0.00 | $150.25 | +39.97 | +39.97 | +0.00 | +39.97 |
| 2026-08-20 | `ABUS` | 223 | — | $4.92 | +0.00 | $4.77 | -33.45 | -33.45 | +0.00 | -33.45 |
| 2026-08-21 | `AG` | 53 | $21.19 | $21.90 | +37.63 | — | +0.00 | +37.63 | +71.55 | — |
| 2026-08-21 | `BHP` | 12 | $93.63 | $95.72 | +25.08 | — | +0.00 | +25.08 | +56.52 | — |
| 2026-08-21 | `CDE` | 53 | $21.11 | $21.75 | +33.92 | — | +0.00 | +33.92 | +58.30 | — |
| 2026-08-21 | `IAG` | 56 | $20.50 | $21.17 | +37.52 | — | +0.00 | +37.52 | +86.24 | — |
| 2026-08-21 | `KGC` | 37 | $31.43 | $32.17 | +27.38 | — | +0.00 | +27.38 | +93.98 | — |
| 2026-08-21 | `NFGC` | 629 | $1.75 | $1.79 | +25.16 | — | +0.00 | +25.16 | +25.16 | — |
| 2026-08-21 | `WPM` | 7 | $150.25 | $154.70 | +31.15 | — | +0.00 | +31.15 | +71.12 | — |
| 2026-08-21 | `ABUS` | 223 | $4.77 | $5.20 | +95.89 | — | +0.00 | +95.89 | +62.44 | — |
| 2026-08-21 | `AU` | 9 | — | $119.43 | +0.00 | $121.22 | +16.11 | +16.11 | +0.00 | +16.11 |
| 2026-08-21 | `AUPH` | 67 | — | $17.20 | +0.00 | $16.65 | -36.85 | -36.85 | +0.00 | -36.85 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `CYPH` | 879 | — | $1.32 | +0.00 | $1.42 | +87.90 | +87.90 | +0.00 | +87.90 |
| 2026-08-21 | `ORBS` | 1343 | — | $0.86 | +0.00 | $0.88 | +21.49 | +21.49 | +0.00 | +21.49 |
| 2026-08-21 | `CF` | 9 | — | $127.43 | +0.00 | $129.60 | +19.53 | +19.53 | +0.00 | +19.53 |
| 2026-08-21 | `CAN` | 3948 | — | $0.29 | +0.00 | $0.35 | +240.83 | +240.83 | +0.00 | +240.83 |
| 2026-08-21 | `MRVI` | 140 | — | $8.28 | +0.00 | $8.64 | +50.40 | +50.40 | +0.00 | +50.40 |
| 2026-08-24 | `AU` | 9 | $121.22 | $120.51 | -6.39 | — | +0.00 | -6.39 | +9.72 | — |
| 2026-08-24 | `AUPH` | 67 | $16.65 | $16.57 | -5.36 | — | +0.00 | -5.36 | -42.21 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `CYPH` | 879 | $1.42 | $1.83 | +360.39 | — | +0.00 | +360.39 | +448.29 | — |
| 2026-08-24 | `ORBS` | 1343 | $0.88 | $0.89 | +13.43 | — | +0.00 | +13.43 | +34.92 | — |
| 2026-08-24 | `CF` | 9 | $129.60 | $129.99 | +3.51 | — | +0.00 | +3.51 | +23.04 | — |
| 2026-08-24 | `CAN` | 3948 | $0.35 | $0.38 | +110.54 | — | +0.00 | +110.54 | +351.37 | — |
| 2026-08-24 | `MRVI` | 140 | $8.64 | $8.59 | -7.00 | — | +0.00 | -7.00 | +43.40 | — |
| 2026-08-25 | `LIFE` | 33 | — | $36.96 | +0.00 | $38.56 | +52.80 | +52.80 | +0.00 | +52.80 |
| 2026-08-25 | `BMEA` | 769 | — | $1.63 | +0.00 | $1.73 | +76.90 | +76.90 | +0.00 | +76.90 |
| 2026-08-25 | `ALVO` | 239 | — | $5.24 | +0.00 | $5.05 | -45.41 | -45.41 | +0.00 | -45.41 |
| 2026-08-25 | `SUJA` | 142 | — | $8.79 | +0.00 | $9.33 | +76.68 | +76.68 | +0.00 | +76.68 |
| 2026-08-25 | `CYPH` | 803 | — | $1.56 | +0.00 | $1.64 | +64.24 | +64.24 | +0.00 | +64.24 |
| 2026-08-25 | `FWDI` | 219 | — | $5.71 | +0.00 | $6.05 | +74.46 | +74.46 | +0.00 | +74.46 |
| 2026-08-25 | `GORO` | 353 | — | $3.55 | +0.00 | $3.87 | +112.96 | +112.96 | +0.00 | +112.96 |
| 2026-08-25 | `ASST` | 65 | — | $19.04 | +0.00 | $21.39 | +152.75 | +152.75 | +0.00 | +152.75 |
| 2026-08-26 | `LIFE` | 33 | $38.56 | $38.24 | -10.56 | — | +0.00 | -10.56 | +42.24 | — |
| 2026-08-26 | `BMEA` | 769 | $1.73 | $1.75 | +19.22 | — | +0.00 | +19.22 | +96.12 | — |
| 2026-08-26 | `ALVO` | 239 | $5.05 | $4.98 | -16.73 | — | +0.00 | -16.73 | -62.14 | — |
| 2026-08-26 | `SUJA` | 142 | $9.33 | $9.39 | +8.52 | $9.44 | +7.10 | +15.62 | +85.20 | +92.30 |
| 2026-08-26 | `CYPH` | 803 | $1.64 | $1.60 | -32.12 | — | +0.00 | -32.12 | +32.12 | — |
| 2026-08-26 | `FWDI` | 219 | $6.05 | $5.97 | -17.52 | — | +0.00 | -17.52 | +56.94 | — |
| 2026-08-26 | `GORO` | 353 | $3.87 | $3.77 | -35.30 | — | +0.00 | -35.30 | +77.66 | — |
| 2026-08-26 | `ASST` | 65 | $21.39 | $20.72 | -43.55 | — | +0.00 | -43.55 | +109.20 | — |
| 2026-08-26 | `ABX` | 131 | — | $9.83 | +0.00 | $9.78 | -6.55 | -6.55 | +0.00 | -6.55 |
| 2026-08-26 | `KURA` | 94 | — | $13.63 | +0.00 | $13.06 | -53.58 | -53.58 | +0.00 | -53.58 |
| 2026-08-26 | `ACRS` | 198 | — | $6.53 | +0.00 | $6.19 | -67.32 | -67.32 | +0.00 | -67.32 |
| 2026-08-26 | `CNTN` | 565 | — | $2.29 | +0.00 | $2.23 | -33.90 | -33.90 | +0.00 | -33.90 |
| 2026-08-26 | `FIGR` | 31 | — | $40.50 | +0.00 | $37.08 | -106.02 | -106.02 | +0.00 | -106.02 |
| 2026-08-26 | `MNRO` | 92 | — | $14.00 | +0.00 | $12.61 | -127.88 | -127.88 | +0.00 | -127.88 |
| 2026-08-26 | `FUTU` | 10 | — | $124.67 | +0.00 | $127.34 | +26.70 | +26.70 | +0.00 | +26.70 |
| 2026-08-27 | `SUJA` | 142 | $9.44 | $9.41 | -4.26 | — | +0.00 | -4.26 | +88.04 | — |
| 2026-08-27 | `ABX` | 131 | $9.78 | $9.68 | -13.10 | — | +0.00 | -13.10 | -19.65 | — |
| 2026-08-27 | `KURA` | 94 | $13.06 | $12.98 | -7.52 | — | +0.00 | -7.52 | -61.10 | — |
| 2026-08-27 | `ACRS` | 198 | $6.19 | $6.15 | -7.92 | — | +0.00 | -7.92 | -75.24 | — |
| 2026-08-27 | `CNTN` | 565 | $2.23 | $2.21 | -11.30 | — | +0.00 | -11.30 | -45.20 | — |
| 2026-08-27 | `FIGR` | 31 | $37.08 | $37.42 | +10.54 | — | +0.00 | +10.54 | -95.48 | — |
| 2026-08-27 | `MNRO` | 92 | $12.61 | $12.56 | -4.60 | — | +0.00 | -4.60 | -132.48 | — |
| 2026-08-27 | `FUTU` | 10 | $127.34 | $128.00 | +6.60 | — | +0.00 | +6.60 | +33.30 | — |
| 2026-08-27 | `RRC` | 120 | — | $41.44 | +0.00 | $41.64 | +24.00 | +24.00 | +0.00 | +24.00 |
| 2026-08-27 | `SLI` | 1907 | — | $2.60 | +0.00 | $2.64 | +76.28 | +76.28 | +0.00 | +76.28 |
| 2026-08-28 | `RRC` | 120 | $41.64 | $41.74 | +12.00 | — | +0.00 | +12.00 | +36.00 | — |
| 2026-08-28 | `SLI` | 1907 | $2.64 | $2.68 | +76.28 | $2.55 | -247.91 | -171.63 | +152.56 | -95.35 |
| 2026-08-28 | `CAPR` | 73 | — | $9.73 | +0.00 | $9.59 | -10.22 | -10.22 | +0.00 | -10.22 |
| 2026-08-28 | `VYX` | 78 | — | $9.13 | +0.00 | $8.78 | -27.30 | -27.30 | +0.00 | -27.30 |
| 2026-08-28 | `SNPS` | 1 | — | $461.85 | +0.00 | $442.61 | -19.24 | -19.24 | +0.00 | -19.24 |
| 2026-08-28 | `SRPT` | 33 | — | $21.49 | +0.00 | $20.86 | -20.79 | -20.79 | +0.00 | -20.79 |
| 2026-08-28 | `NEO` | 38 | — | $18.36 | +0.00 | $18.05 | -11.78 | -11.78 | +0.00 | -11.78 |
| 2026-08-28 | `NCNO` | 30 | — | $23.30 | +0.00 | $22.99 | -9.30 | -9.30 | +0.00 | -9.30 |
| 2026-08-28 | `DJT` | 73 | — | $9.72 | +0.00 | $9.63 | -6.21 | -6.21 | +0.00 | -6.21 |
| 2026-08-31 | `SLI` | 1907 | $2.55 | $2.58 | +57.21 | — | +0.00 | +57.21 | -38.14 | — |
| 2026-08-31 | `CAPR` | 73 | $9.59 | $9.50 | -6.57 | — | +0.00 | -6.57 | -16.79 | — |
| 2026-08-31 | `VYX` | 78 | $8.78 | $8.66 | -9.36 | — | +0.00 | -9.36 | -36.66 | — |
| 2026-08-31 | `SNPS` | 1 | $442.61 | $437.95 | -4.66 | — | +0.00 | -4.66 | -23.90 | — |
| 2026-08-31 | `SRPT` | 33 | $20.86 | $20.56 | -9.90 | — | +0.00 | -9.90 | -30.69 | — |
| 2026-08-31 | `NEO` | 38 | $18.05 | $17.77 | -10.64 | — | +0.00 | -10.64 | -22.42 | — |
| 2026-08-31 | `NCNO` | 30 | $22.99 | $22.66 | -9.90 | — | +0.00 | -9.90 | -19.20 | — |
| 2026-08-31 | `DJT` | 73 | $9.63 | $9.55 | -6.20 | — | +0.00 | -6.20 | -12.41 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 22 | — | $52.88 | +0.00 | $52.46 | -9.24 | -9.24 | +0.00 | -9.24 |
| 2026-09-03 | `HRMY` | 28 | — | $42.93 | +0.00 | $41.86 | -29.96 | -29.96 | +0.00 | -29.96 |
| 2026-09-03 | `CABA` | 334 | — | $3.63 | +0.00 | $3.48 | -50.10 | -50.10 | +0.00 | -50.10 |
| 2026-09-03 | `VSTM` | 151 | — | $8.03 | +0.00 | $7.98 | -7.55 | -7.55 | +0.00 | -7.55 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `GPRO` | 681 | — | $1.78 | +0.00 | $1.39 | -265.59 | -265.59 | +0.00 | -265.59 |
| 2026-09-03 | `MMED` | 50 | — | $23.88 | +0.00 | $23.84 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-09-03 | `SID` | 892 | — | $1.36 | +0.00 | $1.26 | -89.20 | -89.20 | +0.00 | -89.20 |
| 2026-09-04 | `ATRC` | 22 | $52.46 | $52.03 | -9.46 | — | +0.00 | -9.46 | -18.70 | — |
| 2026-09-04 | `HRMY` | 28 | $41.86 | $41.50 | -10.08 | — | +0.00 | -10.08 | -40.04 | — |
| 2026-09-04 | `CABA` | 334 | $3.48 | $3.46 | -6.68 | — | +0.00 | -6.68 | -56.78 | — |
| 2026-09-04 | `VSTM` | 151 | $7.98 | $7.91 | -10.57 | — | +0.00 | -10.57 | -18.12 | — |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `GPRO` | 681 | $1.39 | $1.48 | +61.29 | — | +0.00 | +61.29 | -204.30 | — |
| 2026-09-04 | `MMED` | 50 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.00 | — |
| 2026-09-04 | `SID` | 892 | $1.26 | $1.23 | -26.76 | — | +0.00 | -26.76 | -115.96 | — |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-04 | `IRD` | 252 | — | $4.53 | +0.00 | $4.67 | +35.28 | +35.28 | +0.00 | +35.28 |
| 2026-09-04 | `TARS` | 13 | — | $82.70 | +0.00 | $90.78 | +105.04 | +105.04 | +0.00 | +105.04 |
| 2026-09-04 | `BRR` | 456 | — | $2.51 | +0.00 | $2.66 | +68.40 | +68.40 | +0.00 | +68.40 |
| 2026-09-04 | `LENZ` | 199 | — | $5.75 | +0.00 | $5.96 | +41.79 | +41.79 | +0.00 | +41.79 |
| 2026-09-04 | `SCZM` | 114 | — | $10.03 | +0.00 | $9.94 | -10.26 | -10.26 | +0.00 | -10.26 |
| 2026-09-04 | `ASST` | 45 | — | $25.18 | +0.00 | $27.14 | +88.20 | +88.20 | +0.00 | +88.20 |
| 2026-09-04 | `DFDV` | 197 | — | $5.79 | +0.00 | $5.87 | +15.76 | +15.76 | +0.00 | +15.76 |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-08 | `IRD` | 252 | $4.67 | $4.53 | -35.28 | — | +0.00 | -35.28 | +0.00 | — |
| 2026-09-08 | `TARS` | 13 | $90.78 | $89.67 | -14.43 | — | +0.00 | -14.43 | +90.61 | — |
| 2026-09-08 | `BRR` | 456 | $2.66 | $2.66 | +0.00 | — | +0.00 | +0.00 | +68.40 | — |
| 2026-09-08 | `LENZ` | 199 | $5.96 | $5.95 | -1.99 | — | +0.00 | -1.99 | +39.80 | — |
| 2026-09-08 | `SCZM` | 114 | $9.94 | $9.90 | -4.56 | — | +0.00 | -4.56 | -14.82 | — |
| 2026-09-08 | `ASST` | 45 | $27.14 | $26.44 | -31.50 | — | +0.00 | -31.50 | +56.70 | — |
| 2026-09-08 | `DFDV` | 197 | $5.87 | $5.81 | -11.82 | — | +0.00 | -11.82 | +3.94 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 7 | — | $164.43 | +0.00 | $150.28 | -99.05 | -99.05 | +0.00 | -99.05 |
| 2026-09-11 | `NVT` | 7 | — | $157.78 | +0.00 | $162.38 | +32.20 | +32.20 | +0.00 | +32.20 |
| 2026-09-11 | `AMTX` | 574 | — | $2.04 | +0.00 | $2.01 | -17.22 | -17.22 | +0.00 | -17.22 |
| 2026-09-11 | `BAK` | 552 | — | $2.12 | +0.00 | $2.08 | -22.08 | -22.08 | +0.00 | -22.08 |
| 2026-09-11 | `QRVO` | 10 | — | $112.83 | +0.00 | $116.65 | +38.15 | +38.15 | +0.00 | +38.15 |
| 2026-09-11 | `INDP` | 434 | — | $2.70 | +0.00 | $2.77 | +30.38 | +30.38 | +0.00 | +30.38 |
| 2026-09-11 | `WLTH` | 107 | — | $10.95 | +0.00 | $10.38 | -60.99 | -60.99 | +0.00 | -60.99 |
| 2026-09-11 | `BNC` | 238 | — | $4.91 | +0.00 | $4.80 | -26.18 | -26.18 | +0.00 | -26.18 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | -264.74 | IREN | — | $19.54 | $9,732.46 | IREN×217 |
| 2026-08-14 | +5.50 | $19.54 | IREN×217 | $9,587.07 | -145.39 | -322.44 | SLG, ADUR, ARX, AIRO, TBBB, AMPY, MH | IREN | $1,284.61 | $9,245.54 | SLG×20, ADUR×72, ARX×61, AIRO×107, TBBB×24, AMPY×242, MH×88 |
| 2026-08-17 | +2.25 | $1,284.61 | SLG×20, ADUR×72, ARX×61, AIRO×107, TBBB×24, AMPY×242, MH×88 | $9,213.89 | -31.65 | -118.23 | DVN, OCC, ALM, CAPR, HTFL, UMAC, NPWR, LPTH | SLG, ADUR, ARX, AIRO, TBBB, AMPY, MH | $99.71 | $9,056.27 | DVN×24, OCC×63, ALM×70, CAPR×167, HTFL×27, UMAC×35, NPWR×598, LPTH×76 |
| 2026-08-18 | -6.20 | $99.71 | DVN×24, OCC×63, ALM×70, CAPR×167, HTFL×27, UMAC×35, NPWR×598, LPTH×76 | $8,831.92 | -224.35 | +0.00 | — | DVN, OCC, ALM, CAPR, HTFL, UMAC, NPWR, LPTH | $8,808.62 | $8,808.62 | — |
| 2026-08-19 | -7.20 | $8,808.62 | — | $8,808.62 | -0.00 | +0.00 | — | — | $8,808.62 | $8,808.62 | — |
| 2026-08-20 | +1.12 | $8,808.62 | — | $8,808.62 | -0.00 | +211.58 | AG, BHP, CDE, IAG, KGC, NFGC, WPM, ABUS | — | $104.03 | $8,996.61 | AG×53, BHP×12, CDE×53, IAG×56, KGC×37, NFGC×629, WPM×7, ABUS×223 |
| 2026-08-21 | +3.25 | $104.03 | AG×53, BHP×12, CDE×53, IAG×56, KGC×37, NFGC×629, WPM×7, ABUS×223 | $9,310.34 | +313.73 | +398.21 | AU, AUPH, AEM, CYPH, ORBS, CF, CAN, MRVI | AG, BHP, CDE, IAG, KGC, NFGC, WPM, ABUS | $129.23 | $9,623.62 | AU×9, AUPH×67, AEM×5, CYPH×879, ORBS×1343, CF×9, CAN×3948, MRVI×140 |
| 2026-08-24 | -5.17 | $129.23 | AU×9, AUPH×67, AEM×5, CYPH×879, ORBS×1343, CF×9, CAN×3948, MRVI×140 | $10,097.59 | +473.97 | +0.00 | — | AU, AUPH, AEM, CYPH, ORBS, CF, CAN, MRVI | $10,031.49 | $10,031.49 | — |
| 2026-08-25 | +1.80 | $10,031.49 | — | $10,031.49 | +0.00 | +565.38 | LIFE, BMEA, ALVO, SUJA, CYPH, FWDI, GORO, ASST | — | $26.45 | $10,559.44 | LIFE×33, BMEA×769, ALVO×239, SUJA×142, CYPH×803, FWDI×219, GORO×353, ASST×65 |
| 2026-08-26 | +2.02 | $26.45 | LIFE×33, BMEA×769, ALVO×239, SUJA×142, CYPH×803, FWDI×219, GORO×353, ASST×65 | $10,431.41 | -128.03 | -361.45 | ABX, KURA, ACRS, CNTN, FIGR, MNRO, FUTU | LIFE, BMEA, ALVO, CYPH, FWDI, GORO, ASST | $95.69 | $10,013.56 | SUJA×142, ABX×131, KURA×94, ACRS×198, CNTN×565, FIGR×31, MNRO×92, FUTU×10 |
| 2026-08-27 | — | $95.69 | SUJA×142, ABX×131, KURA×94, ACRS×198, CNTN×565, FIGR×31, MNRO×92, FUTU×10 | $9,982.00 | -31.56 | +100.28 | RRC, SLI | SUJA, ABX, KURA, ACRS, CNTN, FIGR, MNRO, FUTU | $0.43 | $10,031.71 | RRC×120, SLI×1907 |
| 2026-08-28 | +0.75 | $0.43 | RRC×120, SLI×1907 | $10,119.99 | +88.28 | -352.75 | CAPR, VYX, SNPS, SRPT, NEO, NCNO, DJT | RRC | $292.22 | $9,749.93 | SLI×1907, CAPR×73, VYX×78, SNPS×1, SRPT×33, NEO×38, NCNO×30, DJT×73 |
| 2026-08-31 | -5.85 | $292.22 | SLI×1907, CAPR×73, VYX×78, SNPS×1, SRPT×33, NEO×38, NCNO×30, DJT×73 | $9,749.90 | -0.03 | +0.00 | — | SLI, CAPR, VYX, SNPS, SRPT, NEO, NCNO, DJT | $9,709.89 | $9,709.89 | — |
| 2026-09-01 | -6.30 | $9,709.89 | — | $9,709.89 | +0.00 | +0.00 | — | — | $9,709.89 | $9,709.89 | — |
| 2026-09-02 | -3.83 | $9,709.89 | — | $9,709.89 | +0.00 | +0.00 | — | — | $9,709.89 | $9,709.89 | — |
| 2026-09-03 | -0.90 | $9,709.89 | — | $9,709.89 | +0.00 | -470.02 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, MMED, SID | — | $72.86 | $9,204.54 | ATRC×22, HRMY×28, CABA×334, VSTM×151, RVTY×9, GPRO×681, MMED×50, SID×892 |
| 2026-09-04 | +2.25 | $72.86 | ATRC×22, HRMY×28, CABA×334, VSTM×151, RVTY×9, GPRO×681, MMED×50, SID×892 | $9,196.88 | -7.66 | +364.93 | DELL, IRD, TARS, BRR, LENZ, SCZM, ASST, DFDV | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, MMED, SID | $188.13 | $9,503.24 | DELL×2, IRD×252, TARS×13, BRR×456, LENZ×199, SCZM×114, ASST×45, DFDV×197 |
| 2026-09-08 | -11.47 | $188.13 | DELL×2, IRD×252, TARS×13, BRR×456, LENZ×199, SCZM×114, ASST×45, DFDV×197 | $9,397.68 | -105.56 | +0.00 | — | DELL, IRD, TARS, BRR, LENZ, SCZM, ASST, DFDV | $9,374.58 | $9,374.58 | — |
| 2026-09-09 | -13.95 | $9,374.58 | — | $9,374.58 | +0.00 | +0.00 | — | — | $9,374.58 | $9,374.58 | — |
| 2026-09-10 | -13.28 | $9,374.58 | — | $9,374.58 | +0.00 | +0.00 | — | — | $9,374.58 | $9,374.58 | — |
| 2026-09-11 | +0.50 | $9,374.58 | — | $9,374.58 | +0.00 | -124.79 | ORCL, NVT, AMTX, BAK, QRVO, INDP, WLTH, BNC | — | $105.98 | $9,218.24 | ORCL×7, NVT×7, AMTX×574, BAK×552, QRVO×10, INDP×434, WLTH×107, BNC×238 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 217 | $45.98 | $2.80 | — | $19.54 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ⚪; ret5=+12.3; leftover $10000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.54 | ▼ close $9,732.46 vs 09:30 $10,000.00 (session -264.74) | 16:00 close · cash $19.54 · equity $9,732.46 vs 09:30 $10,000.00 (-267.54; session marks -264.74) · 1 name(s) marked open→close (per-name table). IREN×217 09:30 $45.98 → close $44.76 -264.74 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.54 | ▼ 09:30 equity $9,587.07 vs yday $9,732.46 (-145.39) | 09:30 open · cash $19.54 (unchanged overnight, no fees) · equity $9,587.07 vs prior close $9,732.46 (-145.39) · 1 name(s) re-marked at the open (per-name table). IREN×217 yday $44.76 → 09:30 $44.09 -145.39 | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 217 | $44.09 | $2.91 | $-415.84 | $9,584.16 | ▼ -415.84 after sell → book $9,584.16; vs 09:30 mark -2.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 20 | $57.61 | $2.05 | — | $8,429.91 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1198.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 72 | $16.50 | $2.21 | — | $7,239.70 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1198.02 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 61 | $19.57 | $2.17 | — | $6,043.76 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1198.02 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 107 | $11.12 | $2.31 | — | $4,851.61 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1198.02 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 24 | $48.82 | $2.06 | — | $3,677.87 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1198.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMPY` | 242 | $4.94 | $3.12 | — | $2,479.27 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $1198.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 88 | $13.55 | $2.25 | — | $1,284.61 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1198.02 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,284.61 | ▼ close $9,245.54 vs 09:30 $9,587.07 (session -322.44) | 16:00 close · cash $1,284.61 · equity $9,245.54 vs 09:30 $9,587.07 (-341.53; session marks -322.44) · 7 name(s) marked open→close (per-name table). SLG×20 09:30 $57.61 → close $56.09 -30.40; ADUR×72 09:30 $16.50 → close $16.17 -23.76; ARX×61 09:30 $19.57 → close $19.58 +0.61; AIRO×107 09:30 $11.12 → close $9.57 -165.85; TBBB×24 09:30 $48.82 → close $47.79 -24.72; AMPY×242 09:30 $4.94 → close $4.78 -38.72; MH×88 09:30 $13.55 → close $13.10 -39.60 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,284.61 | ▼ 09:30 equity $9,213.89 vs yday $9,245.54 (-31.65) | 09:30 open · cash $1,284.61 (unchanged overnight, no fees) · equity $9,213.89 vs prior close $9,245.54 (-31.65) · 7 name(s) re-marked at the open (per-name table). SLG×20 yday $56.09 → 09:30 $55.37 -14.40; ADUR×72 yday $16.17 → 09:30 $15.73 -31.68; ARX×61 yday $19.58 → 09:30 $19.57 -0.61; AIRO×107 yday $9.57 → 09:30 $9.57 +0.00; TBBB×24 yday $47.79 → 09:30 $47.39 -9.60; AMPY×242 yday $4.78 → 09:30 $4.86 +19.36; MH×88 yday $13.10 → 09:30 $13.16 +5.28 | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 20 | $55.37 | $2.07 | $-48.92 | $2,389.94 | ▼ -48.92 after sell → book $9,211.82; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 72 | $15.73 | $2.23 | $-59.87 | $3,520.27 | ▼ -59.87 after sell → book $9,209.59; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 61 | $19.57 | $2.19 | $-4.37 | $4,711.85 | ▼ -4.37 after sell → book $9,207.40; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 107 | $9.57 | $2.34 | $-170.50 | $5,733.50 | ▼ -170.50 after sell → book $9,205.06; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 24 | $47.39 | $2.08 | $-38.46 | $6,868.78 | ▼ -38.46 after sell → book $9,202.98; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMPY` | 242 | $4.86 | $3.17 | $-25.65 | $8,041.73 | ▼ -25.65 after sell → book $9,199.81; vs 09:30 mark -3.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 88 | $13.16 | $2.28 | $-38.85 | $9,197.53 | ▼ -38.85 after sell → book $9,197.53; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 24 | $46.18 | $2.06 | — | $8,087.15 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ret5=+6.7; leftover $1149.69 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 63 | $18.24 | $2.18 | — | $6,935.85 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1149.69 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 70 | $16.20 | $2.20 | — | $5,799.65 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1149.69 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 167 | $6.87 | $2.49 | — | $4,649.87 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+62.6; leftover $1149.69 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 27 | $41.23 | $2.07 | — | $3,534.59 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+46.0; leftover $1149.69 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 35 | $32.55 | $2.10 | — | $2,393.24 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1149.69 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 598 | $1.92 | $7.71 | — | $1,237.37 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1149.69 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 76 | $14.94 | $2.22 | — | $99.71 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1149.69 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.71 | ▼ close $9,056.27 vs 09:30 $9,213.89 (session -118.23) | 16:00 close · cash $99.71 · equity $9,056.27 vs 09:30 $9,213.89 (-157.62; session marks -118.23) · 8 name(s) marked open→close (per-name table). DVN×24 09:30 $46.18 → close $47.57 +33.36; OCC×63 09:30 $18.24 → close $17.12 -70.56; ALM×70 09:30 $16.20 → close $16.36 +11.20; CAPR×167 09:30 $6.87 → close $7.45 +96.86; HTFL×27 09:30 $41.23 → close $41.94 +19.17; UMAC×35 09:30 $32.55 → close $30.15 -84.00; NPWR×598 09:30 $1.92 → close $1.73 -113.62; LPTH×76 09:30 $14.94 → close $14.80 -10.64 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.71 | ▼ 09:30 equity $8,831.92 vs yday $9,056.27 (-224.35) | 09:30 open · cash $99.71 (unchanged overnight, no fees) · equity $8,831.92 vs prior close $9,056.27 (-224.35) · 8 name(s) re-marked at the open (per-name table). DVN×24 yday $47.57 → 09:30 $48.00 +10.32; OCC×63 yday $17.12 → 09:30 $16.20 -57.96; ALM×70 yday $16.36 → 09:30 $15.78 -40.60; CAPR×167 yday $7.45 → 09:30 $7.50 +8.35; HTFL×27 yday $41.94 → 09:30 $41.50 -11.88; UMAC×35 yday $30.15 → 09:30 $28.59 -54.60; NPWR×598 yday $1.73 → 09:30 $1.70 -17.94; LPTH×76 yday $14.80 → 09:30 $14.01 -60.04 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 24 | $48.00 | $2.08 | $+39.54 | $1,249.63 | ▲ +39.54 after sell → book $8,829.84; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 63 | $16.20 | $2.20 | $-132.90 | $2,268.03 | ▼ -132.90 after sell → book $8,827.64; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 70 | $15.78 | $2.22 | $-33.82 | $3,370.41 | ▼ -33.82 after sell → book $8,825.42; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `CAPR` | 167 | $7.50 | $2.53 | $+100.19 | $4,620.38 | ▲ +100.19 after sell → book $8,822.89; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 27 | $41.50 | $2.09 | $+3.13 | $5,738.79 | ▲ +3.13 after sell → book $8,820.80; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 35 | $28.59 | $2.12 | $-142.81 | $6,737.32 | ▼ -142.81 after sell → book $8,818.68; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 598 | $1.70 | $7.82 | $-147.10 | $7,746.10 | ▼ -147.10 after sell → book $8,810.86; vs 09:30 mark -7.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 76 | $14.01 | $2.24 | $-75.14 | $8,808.62 | ▼ -75.14 after sell → book $8,808.62; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,808.62 | ▲ close $8,808.62 vs 09:30 $8,831.92 (session +0.00) | 16:00 close · cash $8,808.62 · no lots left · equity $8,808.62. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,808.62 | ▲ 09:30 equity $8,808.62 vs yday $8,808.62 (-0.00) | 09:30 open · cash $8,808.62 · no holdings · equity $8,808.62 vs prior close $8,808.62 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,808.62 | ▲ close $8,808.62 vs 09:30 $8,808.62 (session +0.00) | 16:00 close · cash $8,808.62 · no lots left · equity $8,808.62. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,808.62 | ▲ 09:30 equity $8,808.62 vs yday $8,808.62 (-0.00) | 09:30 open · cash $8,808.62 · no holdings · equity $8,808.62 vs prior close $8,808.62 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 53 | $20.55 | $2.15 | — | $7,717.32 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $6,623.17 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 53 | $20.65 | $2.15 | — | $5,526.57 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 56 | $19.63 | $2.16 | — | $4,425.13 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 37 | $29.63 | $2.10 | — | $3,326.72 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 629 | $1.75 | $8.11 | — | $2,217.86 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $1,204.07 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 223 | $4.92 | $2.88 | — | $104.03 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.03 | ▲ close $8,996.61 vs 09:30 $8,808.62 (session +211.58) | 16:00 close · cash $104.03 · equity $8,996.61 vs 09:30 $8,808.62 (+187.99; session marks +211.58) · 8 name(s) marked open→close (per-name table). AG×53 09:30 $20.55 → close $21.19 +33.92; BHP×12 09:30 $91.01 → close $93.63 +31.44; CDE×53 09:30 $20.65 → close $21.11 +24.38; IAG×56 09:30 $19.63 → close $20.50 +48.72; KGC×37 09:30 $29.63 → close $31.43 +66.60; NFGC×629 09:30 $1.75 → close $1.75 +0.00; WPM×7 09:30 $144.54 → close $150.25 +39.97; ABUS×223 09:30 $4.92 → close $4.77 -33.45 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.03 | ▲ 09:30 equity $9,310.34 vs yday $8,996.61 (+313.73) | 09:30 open · cash $104.03 (unchanged overnight, no fees) · equity $9,310.34 vs prior close $8,996.61 (+313.73) · 8 name(s) re-marked at the open (per-name table). AG×53 yday $21.19 → 09:30 $21.90 +37.63; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×53 yday $21.11 → 09:30 $21.75 +33.92; IAG×56 yday $20.50 → 09:30 $21.17 +37.52; KGC×37 yday $31.43 → 09:30 $32.17 +27.38; NFGC×629 yday $1.75 → 09:30 $1.79 +25.16; WPM×7 yday $150.25 → 09:30 $154.70 +31.15; ABUS×223 yday $4.77 → 09:30 $5.20 +95.89 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 53 | $21.90 | $2.17 | $+67.23 | $1,262.56 | ▲ +67.23 after sell → book $9,308.17; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 12 | $95.72 | $2.05 | $+52.45 | $2,409.16 | ▲ +52.45 after sell → book $9,306.13; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 53 | $21.75 | $2.17 | $+53.98 | $3,559.74 | ▲ +53.98 after sell → book $9,303.96; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 56 | $21.17 | $2.18 | $+81.90 | $4,743.08 | ▲ +81.90 after sell → book $9,301.78; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 37 | $32.17 | $2.12 | $+89.76 | $5,931.25 | ▲ +89.76 after sell → book $9,299.66; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 629 | $1.79 | $8.23 | $+8.82 | $7,048.93 | ▲ +8.82 after sell → book $9,291.43; vs 09:30 mark -8.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 7 | $154.70 | $2.03 | $+67.08 | $8,129.80 | ▲ +67.08 after sell → book $9,289.40; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 223 | $5.20 | $2.92 | $+56.64 | $9,286.48 | ▲ +56.64 after sell → book $9,286.48; vs 09:30 mark -2.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $8,209.59 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1160.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 67 | $17.20 | $2.19 | — | $7,055.00 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1160.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $5,971.49 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1160.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 879 | $1.32 | $11.34 | — | $4,799.87 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1160.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1343 | $0.86 | $15.63 | — | $3,623.89 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1160.81 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 9 | $127.43 | $2.02 | — | $2,475.00 | — | union ∩ break10, no 🚨; gate break_10=True; list probable; 🔵; ⚪; ret5=+7.9; leftover $1160.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 3948 | $0.29 | $23.45 | — | $1,290.84 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1160.81 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 140 | $8.28 | $2.41 | — | $129.23 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1160.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.23 | ▲ close $9,623.62 vs 09:30 $9,310.34 (session +398.21) | 16:00 close · cash $129.23 · equity $9,623.62 vs 09:30 $9,310.34 (+313.28; session marks +398.21) · 8 name(s) marked open→close (per-name table). AU×9 09:30 $119.43 → close $121.22 +16.11; AUPH×67 09:30 $17.20 → close $16.65 -36.85; AEM×5 09:30 $216.30 → close $216.06 -1.20; CYPH×879 09:30 $1.32 → close $1.42 +87.90; ORBS×1343 09:30 $0.86 → close $0.88 +21.49; CF×9 09:30 $127.43 → close $129.60 +19.53; CAN×3948 09:30 $0.29 → close $0.35 +240.83; MRVI×140 09:30 $8.28 → close $8.64 +50.40 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.23 | ▲ 09:30 equity $10,097.59 vs yday $9,623.62 (+473.97) | 09:30 open · cash $129.23 (unchanged overnight, no fees) · equity $10,097.59 vs prior close $9,623.62 (+473.97) · 8 name(s) re-marked at the open (per-name table). AU×9 yday $121.22 → 09:30 $120.51 -6.39; AUPH×67 yday $16.65 → 09:30 $16.57 -5.36; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; CYPH×879 yday $1.42 → 09:30 $1.83 +360.39; ORBS×1343 yday $0.88 → 09:30 $0.89 +13.43; CF×9 yday $129.60 → 09:30 $129.99 +3.51; CAN×3948 yday $0.35 → 09:30 $0.38 +110.54; MRVI×140 yday $8.64 → 09:30 $8.59 -7.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 9 | $120.51 | $2.04 | $+5.67 | $1,211.78 | ▲ +5.67 after sell → book $10,095.56; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 67 | $16.57 | $2.21 | $-46.61 | $2,319.76 | ▼ -46.61 after sell → book $10,093.34; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,402.88 | ▼ -0.38 after sell → book $10,091.32; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 879 | $1.83 | $11.50 | $+425.45 | $4,999.96 | ▲ +425.45 after sell → book $10,079.82; vs 09:30 mark -11.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1343 | $0.89 | $16.21 | $+3.07 | $6,179.01 | ▲ +3.07 after sell → book $10,063.61; vs 09:30 mark -16.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 9 | $129.99 | $2.04 | $+18.99 | $7,346.89 | ▲ +18.99 after sell → book $10,061.57; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 3948 | $0.38 | $27.63 | $+300.29 | $8,831.34 | ▲ +300.29 after sell → book $10,033.94; vs 09:30 mark -27.63 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 140 | $8.59 | $2.44 | $+38.55 | $10,031.49 | ▲ +38.55 after sell → book $10,031.49; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,031.49 | ▲ close $10,031.49 vs 09:30 $10,097.59 (session +0.00) | 16:00 close · cash $10,031.49 · no lots left · equity $10,031.49. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,031.49 | ▲ 09:30 equity $10,031.49 vs yday $10,031.49 (+0.00) | 09:30 open · cash $10,031.49 · no holdings · equity $10,031.49 vs prior close $10,031.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 33 | $36.96 | $2.09 | — | $8,809.72 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1253.94 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 769 | $1.63 | $9.92 | — | $7,546.33 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1253.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 239 | $5.24 | $3.08 | — | $6,290.89 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1253.94 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 142 | $8.79 | $2.42 | — | $5,040.30 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1253.94 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 803 | $1.56 | $10.36 | — | $3,777.26 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1253.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 219 | $5.71 | $2.83 | — | $2,523.94 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1253.94 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 353 | $3.55 | $4.55 | — | $1,266.24 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+27.9; leftover $1253.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 65 | $19.04 | $2.19 | — | $26.45 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+49.5; leftover $1253.94 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.45 | ▲ close $10,559.44 vs 09:30 $10,031.49 (session +565.38) | 16:00 close · cash $26.45 · equity $10,559.44 vs 09:30 $10,031.49 (+527.95; session marks +565.38) · 8 name(s) marked open→close (per-name table). LIFE×33 09:30 $36.96 → close $38.56 +52.80; BMEA×769 09:30 $1.63 → close $1.73 +76.90; ALVO×239 09:30 $5.24 → close $5.05 -45.41; SUJA×142 09:30 $8.79 → close $9.33 +76.68; CYPH×803 09:30 $1.56 → close $1.64 +64.24; FWDI×219 09:30 $5.71 → close $6.05 +74.46; GORO×353 09:30 $3.55 → close $3.87 +112.96; ASST×65 09:30 $19.04 → close $21.39 +152.75 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.45 | ▼ 09:30 equity $10,431.41 vs yday $10,559.44 (-128.03) | 09:30 open · cash $26.45 (unchanged overnight, no fees) · equity $10,431.41 vs prior close $10,559.44 (-128.03) · 8 name(s) re-marked at the open (per-name table). LIFE×33 yday $38.56 → 09:30 $38.24 -10.56; BMEA×769 yday $1.73 → 09:30 $1.75 +19.22; ALVO×239 yday $5.05 → 09:30 $4.98 -16.73; SUJA×142 yday $9.33 → 09:30 $9.39 +8.52; CYPH×803 yday $1.64 → 09:30 $1.60 -32.12; FWDI×219 yday $6.05 → 09:30 $5.97 -17.52; GORO×353 yday $3.87 → 09:30 $3.77 -35.30; ASST×65 yday $21.39 → 09:30 $20.72 -43.55 | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 33 | $38.24 | $2.11 | $+38.04 | $1,286.26 | ▲ +38.04 after sell → book $10,429.30; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 769 | $1.75 | $10.06 | $+76.15 | $2,625.80 | ▲ +76.15 after sell → book $10,419.24; vs 09:30 mark -10.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 239 | $4.98 | $3.13 | $-68.36 | $3,812.89 | ▼ -68.36 after sell → book $10,416.11; vs 09:30 mark -3.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 803 | $1.60 | $10.50 | $+11.26 | $5,087.19 | ▲ +11.26 after sell → book $10,405.61; vs 09:30 mark -10.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FWDI` | 219 | $5.97 | $2.87 | $+51.24 | $6,391.74 | ▲ +51.24 after sell → book $10,402.73; vs 09:30 mark -2.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 353 | $3.77 | $4.62 | $+68.48 | $7,717.93 | ▲ +68.48 after sell → book $10,398.11; vs 09:30 mark -4.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 65 | $20.72 | $2.21 | $+104.81 | $9,062.52 | ▲ +104.81 after sell → book $10,395.90; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 131 | $9.83 | $2.38 | — | $7,772.41 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $1294.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `KURA` | 94 | $13.63 | $2.27 | — | $6,488.92 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $1294.65 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 198 | $6.53 | $2.58 | — | $5,193.39 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+3.6; leftover $1294.65 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CNTN` | 565 | $2.29 | $7.29 | — | $3,892.26 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; 🔵; ret5=+14.9; leftover $1294.65 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 31 | $40.50 | $2.08 | — | $2,634.67 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ret5=+15.8; leftover $1294.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 92 | $14.00 | $2.27 | — | $1,344.41 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+17.8; leftover $1294.65 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 10 | $124.67 | $2.02 | — | $95.69 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ret5=+15.7; leftover $1294.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.69 | ▼ close $10,013.56 vs 09:30 $10,431.41 (session -361.45) | 16:00 close · cash $95.69 · equity $10,013.56 vs 09:30 $10,431.41 (-417.85; session marks -361.45) · 8 name(s) marked open→close (per-name table). SUJA×142 09:30 $9.39 → close $9.44 +7.10; ABX×131 09:30 $9.83 → close $9.78 -6.55; KURA×94 09:30 $13.63 → close $13.06 -53.58; ACRS×198 09:30 $6.53 → close $6.19 -67.32; CNTN×565 09:30 $2.29 → close $2.23 -33.90; FIGR×31 09:30 $40.50 → close $37.08 -106.02; MNRO×92 09:30 $14.00 → close $12.61 -127.88; FUTU×10 09:30 $124.67 → close $127.34 +26.70 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.69 | ▼ 09:30 equity $9,982.00 vs yday $10,013.56 (-31.56) | 09:30 open · cash $95.69 (unchanged overnight, no fees) · equity $9,982.00 vs prior close $10,013.56 (-31.56) · 8 name(s) re-marked at the open (per-name table). SUJA×142 yday $9.44 → 09:30 $9.41 -4.26; ABX×131 yday $9.78 → 09:30 $9.68 -13.10; KURA×94 yday $13.06 → 09:30 $12.98 -7.52; ACRS×198 yday $6.19 → 09:30 $6.15 -7.92; CNTN×565 yday $2.23 → 09:30 $2.21 -11.30; FIGR×31 yday $37.08 → 09:30 $37.42 +10.54; MNRO×92 yday $12.61 → 09:30 $12.56 -4.60; FUTU×10 yday $127.34 → 09:30 $128.00 +6.60 | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 142 | $9.41 | $2.45 | $+83.17 | $1,429.46 | ▲ +83.17 after sell → book $9,979.55; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 131 | $9.68 | $2.41 | $-24.45 | $2,695.12 | ▼ -24.45 after sell → book $9,977.13; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `KURA` | 94 | $12.98 | $2.30 | $-65.67 | $3,912.94 | ▼ -65.67 after sell → book $9,974.83; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ACRS` | 198 | $6.15 | $2.63 | $-80.45 | $5,128.02 | ▼ -80.45 after sell → book $9,972.21; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CNTN` | 565 | $2.21 | $7.39 | $-59.88 | $6,369.28 | ▼ -59.88 after sell → book $9,964.82; vs 09:30 mark -7.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 31 | $37.42 | $2.10 | $-99.67 | $7,527.19 | ▼ -99.67 after sell → book $9,962.71; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 92 | $12.56 | $2.29 | $-137.04 | $8,680.42 | ▼ -137.04 after sell → book $9,960.42; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 10 | $128.00 | $2.04 | $+29.24 | $9,958.38 | ▲ +29.24 after sell → book $9,958.38; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 120 | $41.44 | $2.35 | — | $4,983.23 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ret5=+3.1; leftover $4979.19 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1907 | $2.60 | $24.60 | — | $0.43 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ret5=+13.0; leftover $4979.19 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.43 | ▲ close $10,031.71 vs 09:30 $9,982.00 (session +100.28) | 16:00 close · cash $0.43 · equity $10,031.71 vs 09:30 $9,982.00 (+49.71; session marks +100.28) · 2 name(s) marked open→close (per-name table). RRC×120 09:30 $41.44 → close $41.64 +24.00; SLI×1907 09:30 $2.60 → close $2.64 +76.28 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.43 | ▲ 09:30 equity $10,119.99 vs yday $10,031.71 (+88.28) | 09:30 open · cash $0.43 (unchanged overnight, no fees) · equity $10,119.99 vs prior close $10,031.71 (+88.28) · 2 name(s) re-marked at the open (per-name table). RRC×120 yday $41.64 → 09:30 $41.74 +12.00; SLI×1907 yday $2.64 → 09:30 $2.68 +76.28 | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 120 | $41.74 | $2.41 | $+31.24 | $5,006.82 | ▲ +31.24 after sell → book $10,117.58; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 73 | $9.73 | $2.21 | — | $4,294.32 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+47.1; leftover $715.26 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 78 | $9.13 | $2.22 | — | $3,579.96 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+20.0; leftover $715.26 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 1 | $461.85 | $1.99 | — | $3,116.11 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+16.8; leftover $715.26 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SRPT` | 33 | $21.49 | $2.09 | — | $2,404.86 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+12.3; leftover $715.26 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 38 | $18.36 | $2.10 | — | $1,705.07 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+12.8; leftover $715.26 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 30 | $23.30 | $2.08 | — | $1,003.99 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $715.26 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DJT` | 73 | $9.72 | $2.21 | — | $292.22 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+14.8; leftover $715.26 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $292.22 | ▼ close $9,749.93 vs 09:30 $10,119.99 (session -352.75) | 16:00 close · cash $292.22 · equity $9,749.93 vs 09:30 $10,119.99 (-370.06; session marks -352.75) · 8 name(s) marked open→close (per-name table). SLI×1907 09:30 $2.68 → close $2.55 -247.91; CAPR×73 09:30 $9.73 → close $9.59 -10.22; VYX×78 09:30 $9.13 → close $8.78 -27.30; SNPS×1 09:30 $461.85 → close $442.61 -19.24; SRPT×33 09:30 $21.49 → close $20.86 -20.79; NEO×38 09:30 $18.36 → close $18.05 -11.78; NCNO×30 09:30 $23.30 → close $22.99 -9.30; DJT×73 09:30 $9.72 → close $9.63 -6.21 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $292.22 | ▼ 09:30 equity $9,749.90 vs yday $9,749.93 (-0.03) | 09:30 open · cash $292.22 (unchanged overnight, no fees) · equity $9,749.90 vs prior close $9,749.93 (-0.03) · 8 name(s) re-marked at the open (per-name table). SLI×1907 yday $2.55 → 09:30 $2.58 +57.21; CAPR×73 yday $9.59 → 09:30 $9.50 -6.57; VYX×78 yday $8.78 → 09:30 $8.66 -9.36; SNPS×1 yday $442.61 → 09:30 $437.95 -4.66; SRPT×33 yday $20.86 → 09:30 $20.56 -9.90; NEO×38 yday $18.05 → 09:30 $17.77 -10.64; NCNO×30 yday $22.99 → 09:30 $22.66 -9.90; DJT×73 yday $9.63 → 09:30 $9.55 -6.20 | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 1907 | $2.58 | $24.96 | $-87.70 | $5,187.33 | ▼ -87.70 after sell → book $9,724.95; vs 09:30 mark -24.95 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 73 | $9.50 | $2.23 | $-21.23 | $5,878.60 | ▼ -21.23 after sell → book $9,722.72; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 78 | $8.66 | $2.25 | $-41.13 | $6,551.83 | ▼ -41.13 after sell → book $9,720.47; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SNPS` | 1 | $437.95 | $2.01 | $-27.91 | $6,987.77 | ▼ -27.91 after sell → book $9,718.46; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `SRPT` | 33 | $20.56 | $2.11 | $-34.89 | $7,664.14 | ▼ -34.89 after sell → book $9,716.35; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `NEO` | 38 | $17.77 | $2.12 | $-26.65 | $8,337.27 | ▼ -26.65 after sell → book $9,714.22; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 30 | $22.66 | $2.10 | $-23.38 | $9,014.97 | ▼ -23.38 after sell → book $9,712.12; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `DJT` | 73 | $9.55 | $2.23 | $-16.85 | $9,709.89 | ▼ -16.85 after sell → book $9,709.89; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,709.89 | ▲ close $9,709.89 vs 09:30 $9,749.90 (session +0.00) | 16:00 close · cash $9,709.89 · no lots left · equity $9,709.89. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,709.89 | ▲ 09:30 equity $9,709.89 vs yday $9,709.89 (+0.00) | 09:30 open · cash $9,709.89 · no holdings · equity $9,709.89 vs prior close $9,709.89 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,709.89 | ▲ close $9,709.89 vs 09:30 $9,709.89 (session +0.00) | 16:00 close · cash $9,709.89 · no lots left · equity $9,709.89. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,709.89 | ▲ 09:30 equity $9,709.89 vs yday $9,709.89 (+0.00) | 09:30 open · cash $9,709.89 · no holdings · equity $9,709.89 vs prior close $9,709.89 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,709.89 | ▲ close $9,709.89 vs 09:30 $9,709.89 (session +0.00) | 16:00 close · cash $9,709.89 · no lots left · equity $9,709.89. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,709.89 | ▲ 09:30 equity $9,709.89 vs yday $9,709.89 (+0.00) | 09:30 open · cash $9,709.89 · no holdings · equity $9,709.89 vs prior close $9,709.89 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 22 | $52.88 | $2.06 | — | $8,544.48 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1213.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $7,340.36 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1213.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 334 | $3.63 | $4.31 | — | $6,123.63 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1213.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 151 | $8.03 | $2.44 | — | $4,908.66 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1213.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,714.59 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1213.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 681 | $1.78 | $8.78 | — | $2,493.63 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+183.1; leftover $1213.74 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 50 | $23.88 | $2.14 | — | $1,297.49 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1213.74 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 892 | $1.36 | $11.51 | — | $72.86 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1213.74 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.86 | ▼ close $9,204.54 vs 09:30 $9,709.89 (session -470.02) | 16:00 close · cash $72.86 · equity $9,204.54 vs 09:30 $9,709.89 (-505.35; session marks -470.02) · 8 name(s) marked open→close (per-name table). ATRC×22 09:30 $52.88 → close $52.46 -9.24; HRMY×28 09:30 $42.93 → close $41.86 -29.96; CABA×334 09:30 $3.63 → close $3.48 -50.10; VSTM×151 09:30 $8.03 → close $7.98 -7.55; RVTY×9 09:30 $132.45 → close $130.63 -16.38; GPRO×681 09:30 $1.78 → close $1.39 -265.59; MMED×50 09:30 $23.88 → close $23.84 -2.00; SID×892 09:30 $1.36 → close $1.26 -89.20 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.86 | ▼ 09:30 equity $9,196.88 vs yday $9,204.54 (-7.66) | 09:30 open · cash $72.86 (unchanged overnight, no fees) · equity $9,196.88 vs prior close $9,204.54 (-7.66) · 8 name(s) re-marked at the open (per-name table). ATRC×22 yday $52.46 → 09:30 $52.03 -9.46; HRMY×28 yday $41.86 → 09:30 $41.50 -10.08; CABA×334 yday $3.48 → 09:30 $3.46 -6.68; VSTM×151 yday $7.98 → 09:30 $7.91 -10.57; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; GPRO×681 yday $1.39 → 09:30 $1.48 +61.29; MMED×50 yday $23.84 → 09:30 $23.84 +0.00; SID×892 yday $1.26 → 09:30 $1.23 -26.76 | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 22 | $52.03 | $2.08 | $-22.83 | $1,215.44 | ▼ -22.83 after sell → book $9,194.80; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 28 | $41.50 | $2.09 | $-44.21 | $2,375.35 | ▼ -44.21 after sell → book $9,192.71; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 334 | $3.46 | $4.37 | $-65.46 | $3,526.62 | ▼ -65.46 after sell → book $9,188.34; vs 09:30 mark -4.37 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 151 | $7.91 | $2.48 | $-23.04 | $4,718.55 | ▼ -23.04 after sell → book $9,185.86; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $5,886.78 | ▼ -25.83 after sell → book $9,183.82; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `GPRO` | 681 | $1.48 | $8.91 | $-221.99 | $6,885.75 | ▼ -221.99 after sell → book $9,174.91; vs 09:30 mark -8.91 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 50 | $23.84 | $2.16 | $-6.30 | $8,075.59 | ▼ -6.30 after sell → book $9,172.75; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SID` | 892 | $1.23 | $11.66 | $-139.13 | $9,161.09 | ▼ -139.13 after sell → book $9,161.09; vs 09:30 mark -11.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $8,131.53 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1145.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 252 | $4.53 | $3.25 | — | $6,986.72 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $1145.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 13 | $82.70 | $2.03 | — | $5,909.59 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1145.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 456 | $2.51 | $5.88 | — | $4,759.15 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1145.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 199 | $5.75 | $2.59 | — | $3,612.31 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $1145.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SCZM` | 114 | $10.03 | $2.33 | — | $2,466.56 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; ret5=+4.0; leftover $1145.14 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 45 | $25.18 | $2.12 | — | $1,331.34 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ret5=+16.0; leftover $1145.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 197 | $5.79 | $2.58 | — | $188.13 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1145.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.13 | ▲ close $9,503.24 vs 09:30 $9,196.88 (session +364.93) | 16:00 close · cash $188.13 · equity $9,503.24 vs 09:30 $9,196.88 (+306.36; session marks +364.93) · 8 name(s) marked open→close (per-name table). DELL×2 09:30 $513.78 → close $524.14 +20.72; IRD×252 09:30 $4.53 → close $4.67 +35.28; TARS×13 09:30 $82.70 → close $90.78 +105.04; BRR×456 09:30 $2.51 → close $2.66 +68.40; LENZ×199 09:30 $5.75 → close $5.96 +41.79; SCZM×114 09:30 $10.03 → close $9.94 -10.26; ASST×45 09:30 $25.18 → close $27.14 +88.20; DFDV×197 09:30 $5.79 → close $5.87 +15.76 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.13 | ▼ 09:30 equity $9,397.68 vs yday $9,503.24 (-105.56) | 09:30 open · cash $188.13 (unchanged overnight, no fees) · equity $9,397.68 vs prior close $9,503.24 (-105.56) · 8 name(s) re-marked at the open (per-name table). DELL×2 yday $524.14 → 09:30 $521.15 -5.98; IRD×252 yday $4.67 → 09:30 $4.53 -35.28; TARS×13 yday $90.78 → 09:30 $89.67 -14.43; BRR×456 yday $2.66 → 09:30 $2.66 +0.00; LENZ×199 yday $5.96 → 09:30 $5.95 -1.99; SCZM×114 yday $9.94 → 09:30 $9.90 -4.56; ASST×45 yday $27.14 → 09:30 $26.44 -31.50; DFDV×197 yday $5.87 → 09:30 $5.81 -11.82 | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $1,228.41 | ▲ +10.73 after sell → book $9,395.66; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 252 | $4.53 | $3.30 | $-6.55 | $2,366.67 | ▼ -6.55 after sell → book $9,392.36; vs 09:30 mark -3.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 13 | $89.67 | $2.05 | $+86.53 | $3,530.33 | ▲ +86.53 after sell → book $9,390.31; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 456 | $2.66 | $5.97 | $+56.55 | $4,737.32 | ▲ +56.55 after sell → book $9,384.34; vs 09:30 mark -5.97 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 199 | $5.95 | $2.63 | $+34.58 | $5,918.74 | ▲ +34.58 after sell → book $9,381.71; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `SCZM` | 114 | $9.90 | $2.36 | $-19.51 | $7,044.98 | ▼ -19.51 after sell → book $9,379.35; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 45 | $26.44 | $2.15 | $+52.43 | $8,232.63 | ▲ +52.43 after sell → book $9,377.20; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 197 | $5.81 | $2.62 | $-1.26 | $9,374.58 | ▼ -1.26 after sell → book $9,374.58; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,374.58 | ▲ close $9,374.58 vs 09:30 $9,397.68 (session +0.00) | 16:00 close · cash $9,374.58 · no lots left · equity $9,374.58. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,374.58 | ▲ 09:30 equity $9,374.58 vs yday $9,374.58 (+0.00) | 09:30 open · cash $9,374.58 · no holdings · equity $9,374.58 vs prior close $9,374.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,374.58 | ▲ close $9,374.58 vs 09:30 $9,374.58 (session +0.00) | 16:00 close · cash $9,374.58 · no lots left · equity $9,374.58. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,374.58 | ▲ 09:30 equity $9,374.58 vs yday $9,374.58 (+0.00) | 09:30 open · cash $9,374.58 · no holdings · equity $9,374.58 vs prior close $9,374.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,374.58 | ▲ close $9,374.58 vs 09:30 $9,374.58 (session +0.00) | 16:00 close · cash $9,374.58 · no lots left · equity $9,374.58. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,374.58 | ▲ 09:30 equity $9,374.58 vs yday $9,374.58 (+0.00) | 09:30 open · cash $9,374.58 · no holdings · equity $9,374.58 vs prior close $9,374.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $8,221.56 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; leftover $1171.82 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $7,115.09 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ret5=+7.8; leftover $1171.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 574 | $2.04 | $7.40 | — | $5,936.72 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1171.82 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 552 | $2.12 | $7.12 | — | $4,759.36 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1171.82 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `QRVO` | 10 | $112.83 | $2.02 | — | $3,628.99 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1171.82 | join🟢 sector🟢 gen🟡 news🔴 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 434 | $2.70 | $5.60 | — | $2,451.59 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1171.82 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 107 | $10.95 | $2.31 | — | $1,277.63 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+4.6; leftover $1171.82 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 238 | $4.91 | $3.07 | — | $105.98 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+69.9; leftover $1171.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $105.98 | ▼ close $9,218.24 vs 09:30 $9,374.58 (session -124.79) | 16:00 close · cash $105.98 · equity $9,218.24 vs 09:30 $9,374.58 (-156.34; session marks -124.79) · 8 name(s) marked open→close (per-name table). ORCL×7 09:30 $164.43 → close $150.28 -99.05; NVT×7 09:30 $157.78 → close $162.38 +32.20; AMTX×574 09:30 $2.04 → close $2.01 -17.22; BAK×552 09:30 $2.12 → close $2.08 -22.08; QRVO×10 09:30 $112.83 → close $116.65 +38.15; INDP×434 09:30 $2.70 → close $2.77 +30.38; WLTH×107 09:30 $10.95 → close $10.38 -60.99; BNC×238 09:30 $4.91 → close $4.80 -26.18 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1198.02 < 1 share @ 1646.93 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALEC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `APPN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CXM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CMRC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GWRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `METC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SID` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SECZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SKYX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GSM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SSL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SLDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SKHY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ORCL` | 7 | 2026-09-11 @ $164.43 | union ∩ break10, no 🚨; gate break_10=True; list flatten,earn_react; 🔵; ⚪; ret5=+9.0; leftover $1171.82 |
| `NVT` | 7 | 2026-09-11 @ $157.78 | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ret5=+7.8; leftover $1171.82 |
| `AMTX` | 574 | 2026-09-11 @ $2.04 | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.8; leftover $1171.82 |
| `BAK` | 552 | 2026-09-11 @ $2.12 | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1171.82 |
| `QRVO` | 10 | 2026-09-11 @ $112.83 | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1171.82 |
| `INDP` | 434 | 2026-09-11 @ $2.70 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1171.82 |
| `WLTH` | 107 | 2026-09-11 @ $10.95 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+4.6; leftover $1171.82 |
| `BNC` | 238 | 2026-09-11 @ $4.91 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+69.9; leftover $1171.82 |
