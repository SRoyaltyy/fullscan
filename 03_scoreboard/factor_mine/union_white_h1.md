# Factor mine action — `union_white_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ white, no 🚨

Cash book **+3.30%** ($10,330) · signal-only (no cash/fees) was -1.22%. Starts YES **9/23**. Fills 150 · skips 0 · realized $+330.26.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,330.23.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 23 | — | $59.80 | +0.00 | $60.23 | +9.89 | +9.89 | +0.00 | +9.89 |
| 2026-08-13 | `TGTX` | 28 | — | $49.70 | +0.00 | $47.94 | -49.28 | -49.28 | +0.00 | -49.28 |
| 2026-08-13 | `SLS` | 122 | — | $11.70 | +0.00 | $12.36 | +80.52 | +80.52 | +0.00 | +80.52 |
| 2026-08-13 | `HIMS` | 48 | — | $29.74 | +0.00 | $28.77 | -46.56 | -46.56 | +0.00 | -46.56 |
| 2026-08-13 | `INO` | 1763 | — | $0.81 | +0.00 | $0.90 | +158.67 | +158.67 | +0.00 | +158.67 |
| 2026-08-13 | `TNDM` | 61 | — | $23.33 | +0.00 | $23.13 | -12.20 | -12.20 | +0.00 | -12.20 |
| 2026-08-13 | `VOR` | 64 | — | $22.01 | +0.00 | $23.29 | +81.92 | +81.92 | +0.00 | +81.92 |
| 2026-08-14 | `BTSG` | 23 | $60.23 | $59.65 | -13.34 | — | +0.00 | -13.34 | -3.45 | — |
| 2026-08-14 | `TGTX` | 28 | $47.94 | $47.27 | -18.76 | — | +0.00 | -18.76 | -68.04 | — |
| 2026-08-14 | `SLS` | 122 | $12.36 | $12.40 | +4.88 | — | +0.00 | +4.88 | +85.40 | — |
| 2026-08-14 | `HIMS` | 48 | $28.77 | $29.15 | +18.24 | — | +0.00 | +18.24 | -28.32 | — |
| 2026-08-14 | `INO` | 1763 | $0.90 | $0.93 | +52.89 | — | +0.00 | +52.89 | +211.56 | — |
| 2026-08-14 | `TNDM` | 61 | $23.13 | $22.92 | -12.81 | — | +0.00 | -12.81 | -25.01 | — |
| 2026-08-14 | `VOR` | 64 | $23.29 | $23.33 | +2.56 | — | +0.00 | +2.56 | +84.48 | — |
| 2026-08-14 | `DAVE` | 3 | — | $330.91 | +0.00 | $334.57 | +10.98 | +10.98 | +0.00 | +10.98 |
| 2026-08-14 | `SLG` | 22 | — | $57.61 | +0.00 | $56.09 | -33.44 | -33.44 | +0.00 | -33.44 |
| 2026-08-14 | `ANGX` | 295 | — | $4.31 | +0.00 | $4.37 | +17.70 | +17.70 | +0.00 | +17.70 |
| 2026-08-14 | `HYLN` | 304 | — | $4.18 | +0.00 | $4.06 | -36.48 | -36.48 | +0.00 | -36.48 |
| 2026-08-14 | `WDC` | 2 | — | $503.50 | +0.00 | $508.80 | +10.60 | +10.60 | +0.00 | +10.60 |
| 2026-08-14 | `ADUR` | 77 | — | $16.50 | +0.00 | $16.17 | -25.41 | -25.41 | +0.00 | -25.41 |
| 2026-08-14 | `AIRO` | 114 | — | $11.12 | +0.00 | $9.57 | -176.70 | -176.70 | +0.00 | -176.70 |
| 2026-08-14 | `NCMI` | 473 | — | $2.69 | +0.00 | $2.86 | +80.41 | +80.41 | +0.00 | +80.41 |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `SLG` | 22 | $56.09 | $55.37 | -15.84 | — | +0.00 | -15.84 | -49.28 | — |
| 2026-08-17 | `ANGX` | 295 | $4.37 | $4.60 | +67.85 | — | +0.00 | +67.85 | +85.55 | — |
| 2026-08-17 | `HYLN` | 304 | $4.06 | $4.10 | +12.16 | — | +0.00 | +12.16 | -24.32 | — |
| 2026-08-17 | `WDC` | 2 | $508.80 | $525.53 | +33.46 | — | +0.00 | +33.46 | +44.06 | — |
| 2026-08-17 | `ADUR` | 77 | $16.17 | $15.73 | -33.88 | — | +0.00 | -33.88 | -59.29 | — |
| 2026-08-17 | `AIRO` | 114 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -176.70 | — |
| 2026-08-17 | `NCMI` | 473 | $2.86 | $2.80 | -28.38 | — | +0.00 | -28.38 | +52.03 | — |
| 2026-08-17 | `DNN` | 386 | — | $3.24 | +0.00 | $3.19 | -19.30 | -19.30 | +0.00 | -19.30 |
| 2026-08-17 | `CDNL` | 31 | — | $39.85 | +0.00 | $39.23 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-17 | `OCC` | 68 | — | $18.24 | +0.00 | $17.12 | -76.16 | -76.16 | +0.00 | -76.16 |
| 2026-08-17 | `UMAC` | 38 | — | $32.55 | +0.00 | $30.15 | -91.20 | -91.20 | +0.00 | -91.20 |
| 2026-08-17 | `NPWR` | 652 | — | $1.92 | +0.00 | $1.73 | -123.88 | -123.88 | +0.00 | -123.88 |
| 2026-08-17 | `LPTH` | 83 | — | $14.94 | +0.00 | $14.80 | -11.62 | -11.62 | +0.00 | -11.62 |
| 2026-08-17 | `NMAX` | 114 | — | $10.97 | +0.00 | $10.36 | -69.54 | -69.54 | +0.00 | -69.54 |
| 2026-08-17 | `AAOI` | 8 | — | $152.64 | +0.00 | $154.89 | +18.00 | +18.00 | +0.00 | +18.00 |
| 2026-08-18 | `DNN` | 386 | $3.19 | $3.11 | -30.88 | — | +0.00 | -30.88 | -50.18 | — |
| 2026-08-18 | `CDNL` | 31 | $39.23 | $41.57 | +72.54 | — | +0.00 | +72.54 | +53.32 | — |
| 2026-08-18 | `OCC` | 68 | $17.12 | $16.20 | -62.56 | — | +0.00 | -62.56 | -138.72 | — |
| 2026-08-18 | `UMAC` | 38 | $30.15 | $28.59 | -59.28 | — | +0.00 | -59.28 | -150.48 | — |
| 2026-08-18 | `NPWR` | 652 | $1.73 | $1.70 | -19.56 | — | +0.00 | -19.56 | -143.44 | — |
| 2026-08-18 | `LPTH` | 83 | $14.80 | $14.01 | -65.57 | — | +0.00 | -65.57 | -77.19 | — |
| 2026-08-18 | `NMAX` | 114 | $10.36 | $10.31 | -5.70 | — | +0.00 | -5.70 | -75.24 | — |
| 2026-08-18 | `AAOI` | 8 | $154.89 | $146.20 | -69.52 | — | +0.00 | -69.52 | -51.52 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 56 | — | $20.55 | +0.00 | $21.19 | +35.84 | +35.84 | +0.00 | +35.84 |
| 2026-08-20 | `BHP` | 12 | — | $91.01 | +0.00 | $93.63 | +31.44 | +31.44 | +0.00 | +31.44 |
| 2026-08-20 | `CDE` | 56 | — | $20.65 | +0.00 | $21.11 | +25.76 | +25.76 | +0.00 | +25.76 |
| 2026-08-20 | `HDSN` | 202 | — | $5.77 | +0.00 | $5.57 | -40.40 | -40.40 | +0.00 | -40.40 |
| 2026-08-20 | `IAG` | 59 | — | $19.63 | +0.00 | $20.50 | +51.33 | +51.33 | +0.00 | +51.33 |
| 2026-08-20 | `KGC` | 39 | — | $29.63 | +0.00 | $31.43 | +70.20 | +70.20 | +0.00 | +70.20 |
| 2026-08-20 | `NFGC` | 667 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 56 | $21.19 | $21.90 | +39.76 | — | +0.00 | +39.76 | +75.60 | — |
| 2026-08-21 | `BHP` | 12 | $93.63 | $95.72 | +25.08 | — | +0.00 | +25.08 | +56.52 | — |
| 2026-08-21 | `CDE` | 56 | $21.11 | $21.75 | +35.84 | — | +0.00 | +35.84 | +61.60 | — |
| 2026-08-21 | `HDSN` | 202 | $5.57 | $5.67 | +20.20 | — | +0.00 | +20.20 | -20.20 | — |
| 2026-08-21 | `IAG` | 59 | $20.50 | $21.17 | +39.53 | — | +0.00 | +39.53 | +90.86 | — |
| 2026-08-21 | `KGC` | 39 | $31.43 | $32.17 | +28.86 | — | +0.00 | +28.86 | +99.06 | — |
| 2026-08-21 | `NFGC` | 667 | $1.75 | $1.79 | +26.68 | — | +0.00 | +26.68 | +26.68 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 70 | — | $17.20 | +0.00 | $16.65 | -38.50 | -38.50 | +0.00 | -38.50 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 109 | — | $11.13 | +0.00 | $13.45 | +252.88 | +252.88 | +0.00 | +252.88 |
| 2026-08-21 | `AUTL` | 494 | — | $2.47 | +0.00 | $2.41 | -29.64 | -29.64 | +0.00 | -29.64 |
| 2026-08-21 | `CRDL` | 632 | — | $1.93 | +0.00 | $1.86 | -44.24 | -44.24 | +0.00 | -44.24 |
| 2026-08-21 | `CRSP` | 20 | — | $59.72 | +0.00 | $59.50 | -4.40 | -4.40 | +0.00 | -4.40 |
| 2026-08-21 | `CYPH` | 924 | — | $1.32 | +0.00 | $1.42 | +92.40 | +92.40 | +0.00 | +92.40 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 70 | $16.65 | $16.57 | -5.60 | — | +0.00 | -5.60 | -44.10 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 109 | $13.45 | $13.33 | -13.08 | — | +0.00 | -13.08 | +239.80 | — |
| 2026-08-24 | `AUTL` | 494 | $2.41 | $2.40 | -4.94 | — | +0.00 | -4.94 | -34.58 | — |
| 2026-08-24 | `CRDL` | 632 | $1.86 | $1.88 | +12.64 | — | +0.00 | +12.64 | -31.60 | — |
| 2026-08-24 | `CRSP` | 20 | $59.50 | $58.75 | -15.00 | — | +0.00 | -15.00 | -19.40 | — |
| 2026-08-24 | `CYPH` | 924 | $1.42 | $1.83 | +378.84 | — | +0.00 | +378.84 | +471.24 | — |
| 2026-08-25 | `MOS` | 54 | — | $23.77 | +0.00 | $24.27 | +27.00 | +27.00 | +0.00 | +27.00 |
| 2026-08-25 | `CRMD` | 154 | — | $8.35 | +0.00 | $8.56 | +32.34 | +32.34 | +0.00 | +32.34 |
| 2026-08-25 | `BMEA` | 788 | — | $1.63 | +0.00 | $1.73 | +78.80 | +78.80 | +0.00 | +78.80 |
| 2026-08-25 | `ALVO` | 245 | — | $5.24 | +0.00 | $5.05 | -46.55 | -46.55 | +0.00 | -46.55 |
| 2026-08-25 | `SUJA` | 146 | — | $8.79 | +0.00 | $9.33 | +78.84 | +78.84 | +0.00 | +78.84 |
| 2026-08-25 | `CYPH` | 824 | — | $1.56 | +0.00 | $1.64 | +65.92 | +65.92 | +0.00 | +65.92 |
| 2026-08-25 | `DEFT` | 2074 | — | $0.62 | +0.00 | $0.60 | -33.18 | -33.18 | +0.00 | -33.18 |
| 2026-08-25 | `ZURA` | 195 | — | $6.37 | +0.00 | $6.32 | -9.75 | -9.75 | +0.00 | -9.75 |
| 2026-08-26 | `MOS` | 54 | $24.27 | $24.84 | +30.78 | — | +0.00 | +30.78 | +57.78 | — |
| 2026-08-26 | `CRMD` | 154 | $8.56 | $8.60 | +6.16 | — | +0.00 | +6.16 | +38.50 | — |
| 2026-08-26 | `BMEA` | 788 | $1.73 | $1.75 | +19.70 | — | +0.00 | +19.70 | +98.50 | — |
| 2026-08-26 | `ALVO` | 245 | $5.05 | $4.98 | -17.15 | — | +0.00 | -17.15 | -63.70 | — |
| 2026-08-26 | `SUJA` | 146 | $9.33 | $9.39 | +8.76 | — | +0.00 | +8.76 | +87.60 | — |
| 2026-08-26 | `CYPH` | 824 | $1.64 | $1.60 | -32.96 | — | +0.00 | -32.96 | +32.96 | — |
| 2026-08-26 | `DEFT` | 2074 | $0.60 | $0.60 | -12.44 | — | +0.00 | -12.44 | -45.63 | — |
| 2026-08-26 | `ZURA` | 195 | $6.32 | $6.13 | -37.05 | — | +0.00 | -37.05 | -46.80 | — |
| 2026-08-26 | `USDE` | 1776 | — | $5.81 | +0.00 | $5.98 | +301.92 | +301.92 | +0.00 | +301.92 |
| 2026-08-27 | `USDE` | 1776 | $5.98 | $6.50 | +923.52 | — | +0.00 | +923.52 | +1225.44 | — |
| 2026-08-28 | `SIMO` | 5 | — | $252.24 | +0.00 | $245.81 | -32.15 | -32.15 | +0.00 | -32.15 |
| 2026-08-28 | `SMTC` | 10 | — | $141.76 | +0.00 | $131.17 | -105.90 | -105.90 | +0.00 | -105.90 |
| 2026-08-28 | `TTMI` | 11 | — | $122.81 | +0.00 | $118.65 | -45.76 | -45.76 | +0.00 | -45.76 |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `AVT` | 15 | — | $91.49 | +0.00 | $88.63 | -42.90 | -42.90 | +0.00 | -42.90 |
| 2026-08-28 | `CGNX` | 22 | — | $62.82 | +0.00 | $60.46 | -51.92 | -51.92 | +0.00 | -51.92 |
| 2026-08-28 | `COHR` | 4 | — | $289.44 | +0.00 | $279.20 | -40.96 | -40.96 | +0.00 | -40.96 |
| 2026-08-28 | `LSCC` | 12 | — | $119.76 | +0.00 | $114.40 | -64.32 | -64.32 | +0.00 | -64.32 |
| 2026-08-31 | `SIMO` | 5 | $245.81 | $247.05 | +6.20 | — | +0.00 | +6.20 | -25.95 | — |
| 2026-08-31 | `SMTC` | 10 | $131.17 | $132.30 | +11.30 | — | +0.00 | +11.30 | -94.60 | — |
| 2026-08-31 | `TTMI` | 11 | $118.65 | $118.83 | +1.98 | — | +0.00 | +1.98 | -43.78 | — |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `AVT` | 15 | $88.63 | $89.39 | +11.40 | — | +0.00 | +11.40 | -31.50 | — |
| 2026-08-31 | `CGNX` | 22 | $60.46 | $60.46 | +0.00 | — | +0.00 | +0.00 | -51.92 | — |
| 2026-08-31 | `COHR` | 4 | $279.20 | $280.25 | +4.20 | — | +0.00 | +4.20 | -36.76 | — |
| 2026-08-31 | `LSCC` | 12 | $114.40 | $115.56 | +13.92 | — | +0.00 | +13.92 | -50.40 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 26 | — | $52.88 | +0.00 | $52.46 | -10.92 | -10.92 | +0.00 | -10.92 |
| 2026-09-03 | `HRMY` | 32 | — | $42.93 | +0.00 | $41.86 | -34.24 | -34.24 | +0.00 | -34.24 |
| 2026-09-03 | `CABA` | 383 | — | $3.63 | +0.00 | $3.48 | -57.45 | -57.45 | +0.00 | -57.45 |
| 2026-09-03 | `VSTM` | 173 | — | $8.03 | +0.00 | $7.98 | -8.65 | -8.65 | +0.00 | -8.65 |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `ARCT` | 83 | — | $16.77 | +0.00 | $15.56 | -100.43 | -100.43 | +0.00 | -100.43 |
| 2026-09-03 | `SLN` | 93 | — | $14.85 | +0.00 | $14.79 | -5.58 | -5.58 | +0.00 | -5.58 |
| 2026-09-03 | `CRDL` | 639 | — | $2.18 | +0.00 | $2.16 | -12.78 | -12.78 | +0.00 | -12.78 |
| 2026-09-04 | `ATRC` | 26 | $52.46 | $52.03 | -11.18 | $51.52 | -13.26 | -24.44 | -22.10 | -35.36 |
| 2026-09-04 | `HRMY` | 32 | $41.86 | $41.50 | -11.52 | — | +0.00 | -11.52 | -45.76 | — |
| 2026-09-04 | `CABA` | 383 | $3.48 | $3.46 | -7.66 | $3.47 | +3.83 | -3.83 | -65.11 | -61.28 |
| 2026-09-04 | `VSTM` | 173 | $7.98 | $7.91 | -12.11 | — | +0.00 | -12.11 | -20.76 | — |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `ARCT` | 83 | $15.56 | $15.61 | +4.15 | — | +0.00 | +4.15 | -96.28 | — |
| 2026-09-04 | `SLN` | 93 | $14.79 | $14.63 | -14.88 | — | +0.00 | -14.88 | -20.46 | — |
| 2026-09-04 | `CRDL` | 639 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -12.78 | — |
| 2026-09-04 | `ALEC` | 536 | — | $2.52 | +0.00 | $2.46 | -32.16 | -32.16 | +0.00 | -32.16 |
| 2026-09-04 | `BHC` | 201 | — | $6.71 | +0.00 | $6.56 | -30.15 | -30.15 | +0.00 | -30.15 |
| 2026-09-04 | `BMEA` | 711 | — | $1.90 | +0.00 | $2.03 | +92.43 | +92.43 | +0.00 | +92.43 |
| 2026-09-04 | `OABI` | 282 | — | $4.78 | +0.00 | $4.33 | -126.90 | -126.90 | +0.00 | -126.90 |
| 2026-09-04 | `OPK` | 850 | — | $1.59 | +0.00 | $1.64 | +42.50 | +42.50 | +0.00 | +42.50 |
| 2026-09-04 | `VIR` | 117 | — | $11.31 | +0.00 | $11.38 | +8.77 | +8.77 | +0.00 | +8.77 |
| 2026-09-08 | `ATRC` | 26 | $51.52 | $54.31 | +72.54 | — | +0.00 | +72.54 | +37.18 | — |
| 2026-09-08 | `CABA` | 383 | $3.47 | $3.43 | -15.32 | — | +0.00 | -15.32 | -76.60 | — |
| 2026-09-08 | `ALEC` | 536 | $2.46 | $2.38 | -42.88 | — | +0.00 | -42.88 | -75.04 | — |
| 2026-09-08 | `BHC` | 201 | $6.56 | $6.57 | +2.01 | — | +0.00 | +2.01 | -28.14 | — |
| 2026-09-08 | `BMEA` | 711 | $2.03 | $2.00 | -21.33 | — | +0.00 | -21.33 | +71.10 | — |
| 2026-09-08 | `OABI` | 282 | $4.33 | $4.30 | -8.46 | — | +0.00 | -8.46 | -135.36 | — |
| 2026-09-08 | `OPK` | 850 | $1.64 | $1.63 | -8.50 | — | +0.00 | -8.50 | +34.00 | — |
| 2026-09-08 | `VIR` | 117 | $11.38 | $11.22 | -19.30 | — | +0.00 | -19.30 | -10.53 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 12 | — | $164.43 | +0.00 | $150.28 | -169.80 | -169.80 | +0.00 | -169.80 |
| 2026-09-11 | `BAND` | 40 | — | $52.55 | +0.00 | $56.87 | +172.80 | +172.80 | +0.00 | +172.80 |
| 2026-09-11 | `PAGS` | 210 | — | $10.11 | +0.00 | $10.12 | +2.10 | +2.10 | +0.00 | +2.10 |
| 2026-09-11 | `ZSQR` | 653 | — | $3.25 | +0.00 | $3.07 | -117.54 | -117.54 | +0.00 | -117.54 |
| 2026-09-11 | `PAYP` | 116 | — | $18.30 | +0.00 | $18.45 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-09-14 | `ORCL` | 12 | $150.28 | $141.42 | -106.32 | — | +0.00 | -106.32 | -276.12 | — |
| 2026-09-14 | `BAND` | 40 | $56.87 | $56.90 | +1.20 | — | +0.00 | +1.20 | +174.00 | — |
| 2026-09-14 | `PAGS` | 210 | $10.12 | $10.00 | -25.20 | — | +0.00 | -25.20 | -23.10 | — |
| 2026-09-14 | `ZSQR` | 653 | $3.07 | $3.06 | -6.53 | — | +0.00 | -6.53 | -124.07 | — |
| 2026-09-14 | `PAYP` | 116 | $18.45 | $18.28 | -19.72 | — | +0.00 | -19.72 | -2.32 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +222.96 | BTSG, TGTX, SLS, HIMS, INO, TNDM, VOR | — | $85.73 | $10,190.41 | BTSG×23, TGTX×28, SLS×122, HIMS×48, INO×1763, TNDM×61, VOR×64 |
| 2026-08-14 | +5.50 | $85.73 | BTSG×23, TGTX×28, SLS×122, HIMS×48, INO×1763, TNDM×61, VOR×64 | $10,224.07 | +33.66 | -152.34 | DAVE, SLG, ANGX, HYLN, WDC, ADUR, AIRO, NCMI | BTSG, TGTX, SLS, HIMS, INO, TNDM, VOR | $544.66 | $10,012.19 | DAVE×3, SLG×22, ANGX×295, HYLN×304, WDC×2, ADUR×77, AIRO×114, NCMI×473 |
| 2026-08-17 | +2.25 | $544.66 | DAVE×3, SLG×22, ANGX×295, HYLN×304, WDC×2, ADUR×77, AIRO×114, NCMI×473 | $10,054.67 | +42.48 | -392.92 | DNN, CDNL, OCC, UMAC, NPWR, LPTH, NMAX, AAOI | DAVE, SLG, ANGX, HYLN, WDC, ADUR, AIRO, NCMI | $76.79 | $9,610.64 | DNN×386, CDNL×31, OCC×68, UMAC×38, NPWR×652, LPTH×83, NMAX×114, AAOI×8 |
| 2026-08-18 | -6.20 | $76.79 | DNN×386, CDNL×31, OCC×68, UMAC×38, NPWR×652, LPTH×83, NMAX×114, AAOI×8 | $9,370.11 | -240.53 | +0.00 | — | DNN, CDNL, OCC, UMAC, NPWR, LPTH, NMAX, AAOI | $9,343.43 | $9,343.43 | — |
| 2026-08-19 | -7.20 | $9,343.43 | — | $9,343.43 | -0.00 | +0.00 | — | — | $9,343.43 | $9,343.43 | — |
| 2026-08-20 | +1.12 | $9,343.43 | — | $9,343.43 | -0.00 | +219.85 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $117.42 | $9,539.44 | AG×56, BHP×12, CDE×56, HDSN×202, IAG×59, KGC×39, NFGC×667, WPM×8 |
| 2026-08-21 | +3.25 | $117.42 | AG×56, BHP×12, CDE×56, HDSN×202, IAG×59, KGC×39, NFGC×667, WPM×8 | $9,790.99 | +251.55 | +245.20 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $182.84 | $9,975.03 | AU×10, AUPH×70, AEM×5, ARCT×109, AUTL×494, CRDL×632, CRSP×20, CYPH×924 |
| 2026-08-24 | -5.17 | $182.84 | AU×10, AUPH×70, AEM×5, ARCT×109, AUTL×494, CRDL×632, CRSP×20, CYPH×924 | $10,325.64 | +350.61 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,288.12 | $10,288.12 | — |
| 2026-08-25 | +1.80 | $10,288.12 | — | $10,288.12 | -0.00 | +193.42 | MOS, CRMD, BMEA, ALVO, SUJA, CYPH, DEFT, ZURA | — | $0.94 | $10,428.89 | MOS×54, CRMD×154, BMEA×788, ALVO×245, SUJA×146, CYPH×824, DEFT×2074, ZURA×195 |
| 2026-08-26 | +2.02 | $0.94 | MOS×54, CRMD×154, BMEA×788, ALVO×245, SUJA×146, CYPH×824, DEFT×2074, ZURA×195 | $10,394.68 | -34.21 | +301.92 | USDE | MOS, CRMD, BMEA, ALVO, SUJA, CYPH, DEFT, ZURA | $0.20 | $10,620.68 | USDE×1776 |
| 2026-08-27 | — | $0.20 | USDE×1776 | $11,544.20 | +923.52 | +0.00 | — | USDE | $11,520.90 | $11,520.90 | — |
| 2026-08-28 | +0.75 | $11,520.90 | — | $11,520.90 | +0.00 | -401.67 | SIMO, SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC | — | $828.11 | $11,103.06 | SIMO×5, SMTC×10, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×12 |
| 2026-08-31 | -5.85 | $828.11 | SIMO×5, SMTC×10, TTMI×11, KEYS×4, AVT×15, CGNX×22, COHR×4, LSCC×12 | $11,162.14 | +59.08 | +0.00 | — | SIMO, SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC | $11,145.81 | $11,145.81 | — |
| 2026-09-01 | -6.30 | $11,145.81 | — | $11,145.81 | -0.00 | +0.00 | — | — | $11,145.81 | $11,145.81 | — |
| 2026-09-02 | -3.83 | $11,145.81 | — | $11,145.81 | -0.00 | +0.00 | — | — | $11,145.81 | $11,145.81 | — |
| 2026-09-03 | -0.90 | $11,145.81 | — | $11,145.81 | -0.00 | -248.25 | ATRC, HRMY, CABA, VSTM, RVTY, ARCT, SLN, CRDL | — | $100.83 | $10,871.18 | ATRC×26, HRMY×32, CABA×383, VSTM×173, RVTY×10, ARCT×83, SLN×93, CRDL×639 |
| 2026-09-04 | +2.25 | $100.83 | ATRC×26, HRMY×32, CABA×383, VSTM×173, RVTY×10, ARCT×83, SLN×93, CRDL×639 | $10,811.98 | -59.20 | -54.94 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY, ARCT, SLN, CRDL | $5.72 | $10,701.81 | ATRC×26, CABA×383, ALEC×536, BHC×201, BMEA×711, OABI×282, OPK×850, VIR×117 |
| 2026-09-08 | -11.47 | $5.72 | ATRC×26, CABA×383, ALEC×536, BHC×201, BMEA×711, OABI×282, OPK×850, VIR×117 | $10,660.56 | -41.25 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $10,617.32 | $10,617.32 | — |
| 2026-09-09 | -13.95 | $10,617.32 | — | $10,617.32 | -0.00 | +0.00 | — | — | $10,617.32 | $10,617.32 | — |
| 2026-09-10 | -13.28 | $10,617.32 | — | $10,617.32 | -0.00 | +0.00 | — | — | $10,617.32 | $10,617.32 | — |
| 2026-09-11 | +0.50 | $10,617.32 | — | $10,617.32 | -0.00 | -95.04 | ORCL, BAND, PAGS, ZSQR, PAYP | — | $156.40 | $10,504.67 | ORCL×12, BAND×40, PAGS×210, ZSQR×653, PAYP×116 |
| 2026-09-14 | -11.00 | $156.40 | ORCL×12, BAND×40, PAGS×210, ZSQR×653, PAYP×116 | $10,348.10 | -156.57 | +0.00 | — | ORCL, BAND, PAGS, ZSQR, PAYP | $10,330.23 | $10,330.23 | — |
| 2026-09-15 | -3.84 | $10,330.23 | — | $10,330.23 | +0.00 | +0.00 | — | — | $10,330.23 | $10,330.23 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 23 | $59.80 | $2.06 | — | $8,622.54 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1428.57 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 28 | $49.70 | $2.07 | — | $7,228.87 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1428.57 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 122 | $11.70 | $2.36 | — | $5,799.11 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1428.57 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 48 | $29.74 | $2.13 | — | $4,369.46 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1428.57 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1763 | $0.81 | $19.57 | — | $2,921.86 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.2; leftover $1428.57 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 61 | $23.33 | $2.17 | — | $1,496.55 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+19.7; leftover $1428.57 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 64 | $22.01 | $2.18 | — | $85.73 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+0.3; leftover $1428.57 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.73 | ▲ close $10,190.41 vs 09:30 $10,000.00 (session +222.96) | 16:00 close · cash $85.73 · equity $10,190.41 vs 09:30 $10,000.00 (+190.41; session marks +222.96) · 7 name(s) marked open→close (per-name table). BTSG×23 09:30 $59.80 → close $60.23 +9.89; TGTX×28 09:30 $49.70 → close $47.94 -49.28; SLS×122 09:30 $11.70 → close $12.36 +80.52; HIMS×48 09:30 $29.74 → close $28.77 -46.56; INO×1763 09:30 $0.81 → close $0.90 +158.67; TNDM×61 09:30 $23.33 → close $23.13 -12.20; VOR×64 09:30 $22.01 → close $23.29 +81.92 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.73 | ▲ 09:30 equity $10,224.07 vs yday $10,190.41 (+33.66) | 09:30 open · cash $85.73 (unchanged overnight, no fees) · equity $10,224.07 vs prior close $10,190.41 (+33.66) · 7 name(s) re-marked at the open (per-name table). BTSG×23 yday $60.23 → 09:30 $59.65 -13.34; TGTX×28 yday $47.94 → 09:30 $47.27 -18.76; SLS×122 yday $12.36 → 09:30 $12.40 +4.88; HIMS×48 yday $28.77 → 09:30 $29.15 +18.24; INO×1763 yday $0.90 → 09:30 $0.93 +52.89; TNDM×61 yday $23.13 → 09:30 $22.92 -12.81; VOR×64 yday $23.29 → 09:30 $23.33 +2.56 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 23 | $59.65 | $2.08 | $-7.59 | $1,455.60 | ▼ -7.59 after sell → book $10,221.99; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 28 | $47.27 | $2.09 | $-72.21 | $2,777.07 | ▼ -72.21 after sell → book $10,219.90; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 122 | $12.40 | $2.39 | $+80.66 | $4,287.48 | ▲ +80.66 after sell → book $10,217.51; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 48 | $29.15 | $2.16 | $-32.61 | $5,684.52 | ▼ -32.61 after sell → book $10,215.35; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1763 | $0.93 | $21.99 | $+170.00 | $7,302.12 | ▲ +170.00 after sell → book $10,193.36; vs 09:30 mark -21.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 61 | $22.92 | $2.19 | $-29.38 | $8,698.05 | ▼ -29.38 after sell → book $10,191.17; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 64 | $23.33 | $2.20 | $+80.09 | $10,188.96 | ▲ +80.09 after sell → book $10,188.96; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $9,194.24 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1273.62 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $7,924.76 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1273.62 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 295 | $4.31 | $3.81 | — | $6,649.50 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1273.62 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 304 | $4.18 | $3.92 | — | $5,374.86 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1273.62 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $4,365.87 | — | union ∩ white, no 🚨; gate zero_red=True; list probable; 🔵; ⚪; ret5=+7.9; leftover $1273.62 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 77 | $16.50 | $2.22 | — | $3,093.15 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1273.62 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 114 | $11.12 | $2.33 | — | $1,823.13 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1273.62 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 473 | $2.69 | $6.10 | — | $544.66 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1273.62 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $544.66 | ▼ close $10,012.19 vs 09:30 $10,224.07 (session -152.34) | 16:00 close · cash $544.66 · equity $10,012.19 vs 09:30 $10,224.07 (-211.88; session marks -152.34) · 8 name(s) marked open→close (per-name table). DAVE×3 09:30 $330.91 → close $334.57 +10.98; SLG×22 09:30 $57.61 → close $56.09 -33.44; ANGX×295 09:30 $4.31 → close $4.37 +17.70; HYLN×304 09:30 $4.18 → close $4.06 -36.48; WDC×2 09:30 $503.50 → close $508.80 +10.60; ADUR×77 09:30 $16.50 → close $16.17 -25.41; AIRO×114 09:30 $11.12 → close $9.57 -176.70; NCMI×473 09:30 $2.69 → close $2.86 +80.41 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $544.66 | ▲ 09:30 equity $10,054.67 vs yday $10,012.19 (+42.48) | 09:30 open · cash $544.66 (unchanged overnight, no fees) · equity $10,054.67 vs prior close $10,012.19 (+42.48) · 8 name(s) re-marked at the open (per-name table). DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×22 yday $56.09 → 09:30 $55.37 -15.84; ANGX×295 yday $4.37 → 09:30 $4.60 +67.85; HYLN×304 yday $4.06 → 09:30 $4.10 +12.16; WDC×2 yday $508.80 → 09:30 $525.53 +33.46; ADUR×77 yday $16.17 → 09:30 $15.73 -33.88; AIRO×114 yday $9.57 → 09:30 $9.57 +0.00; NCMI×473 yday $2.86 → 09:30 $2.80 -28.38 | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $1,553.46 | ▲ +14.07 after sell → book $10,052.65; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $2,769.53 | ▼ -53.41 after sell → book $10,050.58; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 295 | $4.60 | $3.87 | $+77.88 | $4,122.66 | ▲ +77.88 after sell → book $10,046.71; vs 09:30 mark -3.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 304 | $4.10 | $3.98 | $-32.22 | $5,365.08 | ▼ -32.22 after sell → book $10,042.73; vs 09:30 mark -3.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $6,414.12 | ▲ +40.05 after sell → book $10,040.71; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 77 | $15.73 | $2.24 | $-63.75 | $7,623.09 | ▼ -63.75 after sell → book $10,038.47; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 114 | $9.57 | $2.36 | $-181.39 | $8,711.71 | ▼ -181.39 after sell → book $10,036.11; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 473 | $2.80 | $6.19 | $+39.74 | $10,029.92 | ▲ +39.74 after sell → book $10,029.92; vs 09:30 mark -6.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 386 | $3.24 | $4.98 | — | $8,774.30 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+0.3; leftover $1253.74 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $7,536.87 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1253.74 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 68 | $18.24 | $2.19 | — | $6,294.35 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1253.74 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 38 | $32.55 | $2.10 | — | $5,055.35 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1253.74 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 652 | $1.92 | $8.41 | — | $3,795.10 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1253.74 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 83 | $14.94 | $2.24 | — | $2,552.84 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1253.74 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 114 | $10.97 | $2.33 | — | $1,299.93 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $1253.74 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 8 | $152.64 | $2.01 | — | $76.79 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $1253.74 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.79 | ▼ close $9,610.64 vs 09:30 $10,054.67 (session -392.92) | 16:00 close · cash $76.79 · equity $9,610.64 vs 09:30 $10,054.67 (-444.03; session marks -392.92) · 8 name(s) marked open→close (per-name table). DNN×386 09:30 $3.24 → close $3.19 -19.30; CDNL×31 09:30 $39.85 → close $39.23 -19.22; OCC×68 09:30 $18.24 → close $17.12 -76.16; UMAC×38 09:30 $32.55 → close $30.15 -91.20; NPWR×652 09:30 $1.92 → close $1.73 -123.88; LPTH×83 09:30 $14.94 → close $14.80 -11.62; NMAX×114 09:30 $10.97 → close $10.36 -69.54; AAOI×8 09:30 $152.64 → close $154.89 +18.00 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.79 | ▼ 09:30 equity $9,370.11 vs yday $9,610.64 (-240.53) | 09:30 open · cash $76.79 (unchanged overnight, no fees) · equity $9,370.11 vs prior close $9,610.64 (-240.53) · 8 name(s) re-marked at the open (per-name table). DNN×386 yday $3.19 → 09:30 $3.11 -30.88; CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; OCC×68 yday $17.12 → 09:30 $16.20 -62.56; UMAC×38 yday $30.15 → 09:30 $28.59 -59.28; NPWR×652 yday $1.73 → 09:30 $1.70 -19.56; LPTH×83 yday $14.80 → 09:30 $14.01 -65.57; NMAX×114 yday $10.36 → 09:30 $10.31 -5.70; AAOI×8 yday $154.89 → 09:30 $146.20 -69.52 | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 386 | $3.11 | $5.05 | $-60.21 | $1,272.20 | ▼ -60.21 after sell → book $9,365.06; vs 09:30 mark -5.05 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $2,558.77 | ▲ +49.13 after sell → book $9,362.96; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 68 | $16.20 | $2.22 | $-143.13 | $3,658.15 | ▼ -143.13 after sell → book $9,360.74; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 38 | $28.59 | $2.12 | $-154.71 | $4,742.45 | ▼ -154.71 after sell → book $9,358.62; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 652 | $1.70 | $8.53 | $-160.38 | $5,842.32 | ▼ -160.38 after sell → book $9,350.09; vs 09:30 mark -8.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 83 | $14.01 | $2.26 | $-81.69 | $7,002.88 | ▼ -81.69 after sell → book $9,347.82; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 114 | $10.31 | $2.36 | $-79.93 | $8,175.86 | ▼ -79.93 after sell → book $9,345.46; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `AAOI` | 8 | $146.20 | $2.03 | $-55.57 | $9,343.43 | ▼ -55.57 after sell → book $9,343.43; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,343.43 | ▲ close $9,343.43 vs 09:30 $9,370.11 (session +0.00) | 16:00 close · cash $9,343.43 · no lots left · equity $9,343.43. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,343.43 | ▲ 09:30 equity $9,343.43 vs yday $9,343.43 (-0.00) | 09:30 open · cash $9,343.43 · no holdings · equity $9,343.43 vs prior close $9,343.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,343.43 | ▲ close $9,343.43 vs 09:30 $9,343.43 (session +0.00) | 16:00 close · cash $9,343.43 · no lots left · equity $9,343.43. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,343.43 | ▲ 09:30 equity $9,343.43 vs yday $9,343.43 (-0.00) | 09:30 open · cash $9,343.43 · no holdings · equity $9,343.43 vs prior close $9,343.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 56 | $20.55 | $2.16 | — | $8,190.47 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1167.93 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $7,096.33 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1167.93 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $5,937.77 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1167.93 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 202 | $5.77 | $2.61 | — | $4,769.62 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1167.93 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $3,609.28 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1167.93 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $2,451.61 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1167.93 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 667 | $1.75 | $8.60 | — | $1,275.75 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1167.93 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $117.42 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1167.93 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.42 | ▲ close $9,539.44 vs 09:30 $9,343.43 (session +219.85) | 16:00 close · cash $117.42 · equity $9,539.44 vs 09:30 $9,343.43 (+196.01; session marks +219.85) · 8 name(s) marked open→close (per-name table). AG×56 09:30 $20.55 → close $21.19 +35.84; BHP×12 09:30 $91.01 → close $93.63 +31.44; CDE×56 09:30 $20.65 → close $21.11 +25.76; HDSN×202 09:30 $5.77 → close $5.57 -40.40; IAG×59 09:30 $19.63 → close $20.50 +51.33; KGC×39 09:30 $29.63 → close $31.43 +70.20; NFGC×667 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.42 | ▲ 09:30 equity $9,790.99 vs yday $9,539.44 (+251.55) | 09:30 open · cash $117.42 (unchanged overnight, no fees) · equity $9,790.99 vs prior close $9,539.44 (+251.55) · 8 name(s) re-marked at the open (per-name table). AG×56 yday $21.19 → 09:30 $21.90 +39.76; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×56 yday $21.11 → 09:30 $21.75 +35.84; HDSN×202 yday $5.57 → 09:30 $5.67 +20.20; IAG×59 yday $20.50 → 09:30 $21.17 +39.53; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×667 yday $1.75 → 09:30 $1.79 +26.68; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 56 | $21.90 | $2.18 | $+71.26 | $1,341.64 | ▲ +71.26 after sell → book $9,788.81; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 12 | $95.72 | $2.05 | $+52.45 | $2,488.23 | ▲ +52.45 after sell → book $9,786.76; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 56 | $21.75 | $2.18 | $+57.26 | $3,704.06 | ▲ +57.26 after sell → book $9,784.59; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 202 | $5.67 | $2.65 | $-25.46 | $4,846.75 | ▼ -25.46 after sell → book $9,781.94; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 59 | $21.17 | $2.19 | $+86.51 | $6,093.59 | ▲ +86.51 after sell → book $9,779.75; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 39 | $32.17 | $2.13 | $+94.83 | $7,346.09 | ▲ +94.83 after sell → book $9,777.62; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 667 | $1.79 | $8.72 | $+9.35 | $8,531.30 | ▲ +9.35 after sell → book $9,768.90; vs 09:30 mark -8.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,766.86 | ▲ +77.23 after sell → book $9,766.86; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,570.54 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1220.86 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 70 | $17.20 | $2.20 | — | $7,364.34 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1220.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,280.84 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1220.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 109 | $11.13 | $2.32 | — | $5,065.35 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1220.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 494 | $2.47 | $6.37 | — | $3,838.80 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1220.86 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 632 | $1.93 | $8.15 | — | $2,610.89 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1220.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $1,414.44 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1220.86 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 924 | $1.32 | $11.92 | — | $182.84 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1220.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $182.84 | ▲ close $9,975.03 vs 09:30 $9,790.99 (session +245.20) | 16:00 close · cash $182.84 · equity $9,975.03 vs 09:30 $9,790.99 (+184.04; session marks +245.20) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×70 09:30 $17.20 → close $16.65 -38.50; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×109 09:30 $11.13 → close $13.45 +252.88; AUTL×494 09:30 $2.47 → close $2.41 -29.64; CRDL×632 09:30 $1.93 → close $1.86 -44.24; CRSP×20 09:30 $59.72 → close $59.50 -4.40; CYPH×924 09:30 $1.32 → close $1.42 +92.40 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $182.84 | ▲ 09:30 equity $10,325.64 vs yday $9,975.03 (+350.61) | 09:30 open · cash $182.84 (unchanged overnight, no fees) · equity $10,325.64 vs prior close $9,975.03 (+350.61) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×70 yday $16.65 → 09:30 $16.57 -5.60; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×109 yday $13.45 → 09:30 $13.33 -13.08; AUTL×494 yday $2.41 → 09:30 $2.40 -4.94; CRDL×632 yday $1.86 → 09:30 $1.88 +12.64; CRSP×20 yday $59.50 → 09:30 $58.75 -15.00; CYPH×924 yday $1.42 → 09:30 $1.83 +378.84 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,385.90 | ▲ +6.74 after sell → book $10,323.60; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 70 | $16.57 | $2.22 | $-48.52 | $2,543.58 | ▼ -48.52 after sell → book $10,321.38; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,626.70 | ▼ -0.38 after sell → book $10,319.35; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 109 | $13.33 | $2.35 | $+235.14 | $5,077.32 | ▲ +235.14 after sell → book $10,317.00; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 494 | $2.40 | $6.46 | $-47.42 | $6,256.46 | ▼ -47.42 after sell → book $10,310.54; vs 09:30 mark -6.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 632 | $1.88 | $8.27 | $-48.02 | $7,436.35 | ▼ -48.02 after sell → book $10,302.27; vs 09:30 mark -8.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 20 | $58.75 | $2.07 | $-23.52 | $8,609.28 | ▼ -23.52 after sell → book $10,300.20; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 924 | $1.83 | $12.09 | $+447.23 | $10,288.12 | ▲ +447.23 after sell → book $10,288.12; vs 09:30 mark -12.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,288.12 | ▲ close $10,288.12 vs 09:30 $10,325.64 (session +0.00) | 16:00 close · cash $10,288.12 · no lots left · equity $10,288.12. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,288.12 | ▲ 09:30 equity $10,288.12 vs yday $10,288.12 (-0.00) | 09:30 open · cash $10,288.12 · no holdings · equity $10,288.12 vs prior close $10,288.12 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 54 | $23.77 | $2.15 | — | $9,002.38 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.0; leftover $1286.01 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 154 | $8.35 | $2.45 | — | $7,714.03 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1286.01 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 788 | $1.63 | $10.17 | — | $6,419.43 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1286.01 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 245 | $5.24 | $3.16 | — | $5,132.47 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1286.01 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 146 | $8.79 | $2.43 | — | $3,846.70 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1286.01 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 824 | $1.56 | $10.63 | — | $2,550.63 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1286.01 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2074 | $0.62 | $19.08 | — | $1,245.67 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1286.01 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 195 | $6.37 | $2.58 | — | $0.94 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1286.01 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.94 | ▲ close $10,428.89 vs 09:30 $10,288.12 (session +193.42) | 16:00 close · cash $0.94 · equity $10,428.89 vs 09:30 $10,288.12 (+140.77; session marks +193.42) · 8 name(s) marked open→close (per-name table). MOS×54 09:30 $23.77 → close $24.27 +27.00; CRMD×154 09:30 $8.35 → close $8.56 +32.34; BMEA×788 09:30 $1.63 → close $1.73 +78.80; ALVO×245 09:30 $5.24 → close $5.05 -46.55; SUJA×146 09:30 $8.79 → close $9.33 +78.84; CYPH×824 09:30 $1.56 → close $1.64 +65.92; DEFT×2074 09:30 $0.62 → close $0.60 -33.18; ZURA×195 09:30 $6.37 → close $6.32 -9.75 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.94 | ▼ 09:30 equity $10,394.68 vs yday $10,428.89 (-34.21) | 09:30 open · cash $0.94 (unchanged overnight, no fees) · equity $10,394.68 vs prior close $10,428.89 (-34.21) · 8 name(s) re-marked at the open (per-name table). MOS×54 yday $24.27 → 09:30 $24.84 +30.78; CRMD×154 yday $8.56 → 09:30 $8.60 +6.16; BMEA×788 yday $1.73 → 09:30 $1.75 +19.70; ALVO×245 yday $5.05 → 09:30 $4.98 -17.15; SUJA×146 yday $9.33 → 09:30 $9.39 +8.76; CYPH×824 yday $1.64 → 09:30 $1.60 -32.96; DEFT×2074 yday $0.60 → 09:30 $0.60 -12.44; ZURA×195 yday $6.32 → 09:30 $6.13 -37.05 | — |
| 2026-08-26 09:30 ET | **SELL** | `MOS` | 54 | $24.84 | $2.17 | $+53.46 | $1,340.13 | ▲ +53.46 after sell → book $10,392.51; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 154 | $8.60 | $2.49 | $+33.56 | $2,662.04 | ▲ +33.56 after sell → book $10,390.02; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 788 | $1.75 | $10.31 | $+78.03 | $4,034.67 | ▲ +78.03 after sell → book $10,379.72; vs 09:30 mark -10.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 245 | $4.98 | $3.21 | $-70.07 | $5,251.56 | ▼ -70.07 after sell → book $10,376.51; vs 09:30 mark -3.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUJA` | 146 | $9.39 | $2.46 | $+82.71 | $6,620.04 | ▲ +82.71 after sell → book $10,374.04; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 824 | $1.60 | $10.78 | $+11.55 | $7,927.66 | ▲ +11.55 after sell → book $10,363.26; vs 09:30 mark -10.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DEFT` | 2074 | $0.60 | $18.98 | $-83.69 | $9,148.94 | ▼ -83.69 after sell → book $10,344.29; vs 09:30 mark -18.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 195 | $6.13 | $2.62 | $-51.99 | $10,341.67 | ▼ -51.99 after sell → book $10,341.67; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 1776 | $5.81 | $22.91 | — | $0.20 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $10341.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.20 | ▲ close $10,620.68 vs 09:30 $10,394.68 (session +301.92) | 16:00 close · cash $0.20 · equity $10,620.68 vs 09:30 $10,394.68 (+226.00; session marks +301.92) · 1 name(s) marked open→close (per-name table). USDE×1776 09:30 $5.81 → close $5.98 +301.92 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.20 | ▲ 09:30 equity $11,544.20 vs yday $10,620.68 (+923.52) | 09:30 open · cash $0.20 (unchanged overnight, no fees) · equity $11,544.20 vs prior close $10,620.68 (+923.52) · 1 name(s) re-marked at the open (per-name table). USDE×1776 yday $5.98 → 09:30 $6.50 +923.52 | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 1776 | $6.50 | $23.30 | $+1179.23 | $11,520.90 | ▲ +1,179.23 after sell → book $11,520.90; vs 09:30 mark -23.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,520.90 | ▲ close $11,520.90 vs 09:30 $11,544.20 (session +0.00) | 16:00 close · cash $11,520.90 · no lots left · equity $11,520.90. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,520.90 | ▲ 09:30 equity $11,520.90 vs yday $11,520.90 (+0.00) | 09:30 open · cash $11,520.90 · no holdings · equity $11,520.90 vs prior close $11,520.90 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $10,257.70 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1440.11 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 10 | $141.76 | $2.02 | — | $8,838.08 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1440.11 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 11 | $122.81 | $2.02 | — | $7,485.14 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1440.11 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $6,185.50 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1440.11 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 15 | $91.49 | $2.04 | — | $4,811.12 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1440.11 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 22 | $62.82 | $2.06 | — | $3,427.02 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1440.11 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $2,267.26 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1440.11 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 12 | $119.76 | $2.03 | — | $828.11 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1440.11 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $828.11 | ▼ close $11,103.06 vs 09:30 $11,520.90 (session -401.67) | 16:00 close · cash $828.11 · equity $11,103.06 vs 09:30 $11,520.90 (-417.84; session marks -401.67) · 8 name(s) marked open→close (per-name table). SIMO×5 09:30 $252.24 → close $245.81 -32.15; SMTC×10 09:30 $141.76 → close $131.17 -105.90; TTMI×11 09:30 $122.81 → close $118.65 -45.76; KEYS×4 09:30 $324.41 → close $319.97 -17.76; AVT×15 09:30 $91.49 → close $88.63 -42.90; CGNX×22 09:30 $62.82 → close $60.46 -51.92; COHR×4 09:30 $289.44 → close $279.20 -40.96; LSCC×12 09:30 $119.76 → close $114.40 -64.32 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $828.11 | ▲ 09:30 equity $11,162.14 vs yday $11,103.06 (+59.08) | 09:30 open · cash $828.11 (unchanged overnight, no fees) · equity $11,162.14 vs prior close $11,103.06 (+59.08) · 8 name(s) re-marked at the open (per-name table). SIMO×5 yday $245.81 → 09:30 $247.05 +6.20; SMTC×10 yday $131.17 → 09:30 $132.30 +11.30; TTMI×11 yday $118.65 → 09:30 $118.83 +1.98; KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; AVT×15 yday $88.63 → 09:30 $89.39 +11.40; CGNX×22 yday $60.46 → 09:30 $60.46 +0.00; COHR×4 yday $279.20 → 09:30 $280.25 +4.20; LSCC×12 yday $114.40 → 09:30 $115.56 +13.92 | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $2,061.34 | ▼ -29.98 after sell → book $11,160.12; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 10 | $132.30 | $2.04 | $-98.66 | $3,382.30 | ▼ -98.66 after sell → book $11,158.08; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 11 | $118.83 | $2.04 | $-47.85 | $4,687.38 | ▼ -47.85 after sell → book $11,156.03; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $5,975.32 | ▼ -11.70 after sell → book $11,154.01; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 15 | $89.39 | $2.06 | $-35.59 | $7,314.11 | ▼ -35.59 after sell → book $11,151.95; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 22 | $60.46 | $2.08 | $-56.05 | $8,642.16 | ▼ -56.05 after sell → book $11,149.88; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $9,761.14 | ▼ -40.78 after sell → book $11,147.86; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 12 | $115.56 | $2.05 | $-54.47 | $11,145.81 | ▼ -54.47 after sell → book $11,145.81; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,145.81 | ▲ close $11,145.81 vs 09:30 $11,162.14 (session +0.00) | 16:00 close · cash $11,145.81 · no lots left · equity $11,145.81. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,145.81 | ▲ 09:30 equity $11,145.81 vs yday $11,145.81 (-0.00) | 09:30 open · cash $11,145.81 · no holdings · equity $11,145.81 vs prior close $11,145.81 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,145.81 | ▲ close $11,145.81 vs 09:30 $11,145.81 (session +0.00) | 16:00 close · cash $11,145.81 · no lots left · equity $11,145.81. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,145.81 | ▲ 09:30 equity $11,145.81 vs yday $11,145.81 (-0.00) | 09:30 open · cash $11,145.81 · no holdings · equity $11,145.81 vs prior close $11,145.81 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,145.81 | ▲ close $11,145.81 vs 09:30 $11,145.81 (session +0.00) | 16:00 close · cash $11,145.81 · no lots left · equity $11,145.81. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,145.81 | ▲ 09:30 equity $11,145.81 vs yday $11,145.81 (-0.00) | 09:30 open · cash $11,145.81 · no holdings · equity $11,145.81 vs prior close $11,145.81 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $52.88 | $2.07 | — | $9,768.86 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1393.23 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $42.93 | $2.09 | — | $8,393.01 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1393.23 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 383 | $3.63 | $4.94 | — | $6,997.78 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1393.23 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 173 | $8.03 | $2.51 | — | $5,606.09 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1393.23 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,279.57 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1393.23 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 83 | $16.77 | $2.24 | — | $2,885.42 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1393.23 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 93 | $14.85 | $2.27 | — | $1,502.10 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1393.23 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 639 | $2.18 | $8.24 | — | $100.83 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1393.23 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $100.83 | ▼ close $10,871.18 vs 09:30 $11,145.81 (session -248.25) | 16:00 close · cash $100.83 · equity $10,871.18 vs 09:30 $11,145.81 (-274.63; session marks -248.25) · 8 name(s) marked open→close (per-name table). ATRC×26 09:30 $52.88 → close $52.46 -10.92; HRMY×32 09:30 $42.93 → close $41.86 -34.24; CABA×383 09:30 $3.63 → close $3.48 -57.45; VSTM×173 09:30 $8.03 → close $7.98 -8.65; RVTY×10 09:30 $132.45 → close $130.63 -18.20; ARCT×83 09:30 $16.77 → close $15.56 -100.43; SLN×93 09:30 $14.85 → close $14.79 -5.58; CRDL×639 09:30 $2.18 → close $2.16 -12.78 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $100.83 | ▼ 09:30 equity $10,811.98 vs yday $10,871.18 (-59.20) | 09:30 open · cash $100.83 (unchanged overnight, no fees) · equity $10,811.98 vs prior close $10,871.18 (-59.20) · 8 name(s) re-marked at the open (per-name table). ATRC×26 yday $52.46 → 09:30 $52.03 -11.18; HRMY×32 yday $41.86 → 09:30 $41.50 -11.52; CABA×383 yday $3.48 → 09:30 $3.46 -7.66; VSTM×173 yday $7.98 → 09:30 $7.91 -12.11; RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; ARCT×83 yday $15.56 → 09:30 $15.61 +4.15; SLN×93 yday $14.79 → 09:30 $14.63 -14.88; CRDL×639 yday $2.16 → 09:30 $2.16 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 32 | $41.50 | $2.11 | $-49.95 | $1,426.73 | ▼ -49.95 after sell → book $10,809.88; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 173 | $7.91 | $2.55 | $-25.82 | $2,792.61 | ▼ -25.82 after sell → book $10,807.33; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $4,090.87 | ▼ -28.26 after sell → book $10,805.29; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 83 | $15.61 | $2.26 | $-100.78 | $5,384.24 | ▼ -100.78 after sell → book $10,803.03; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 93 | $14.63 | $2.30 | $-25.02 | $6,742.53 | ▼ -25.02 after sell → book $10,800.73; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 639 | $2.16 | $8.36 | $-29.38 | $8,114.41 | ▼ -29.38 after sell → book $10,792.37; vs 09:30 mark -8.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 536 | $2.52 | $6.91 | — | $6,756.78 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1352.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 201 | $6.71 | $2.60 | — | $5,405.47 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1352.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 711 | $1.90 | $9.17 | — | $4,045.40 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1352.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 282 | $4.78 | $3.64 | — | $2,693.80 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1352.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 850 | $1.59 | $10.96 | — | $1,331.33 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1352.40 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 117 | $11.31 | $2.34 | — | $5.72 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1352.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.72 | ▼ close $10,701.81 vs 09:30 $10,811.98 (session -54.94) | 16:00 close · cash $5.72 · equity $10,701.81 vs 09:30 $10,811.98 (-110.17; session marks -54.94) · 8 name(s) marked open→close (per-name table). ATRC×26 09:30 $52.03 → close $51.52 -13.26; CABA×383 09:30 $3.46 → close $3.47 +3.83; ALEC×536 09:30 $2.52 → close $2.46 -32.16; BHC×201 09:30 $6.71 → close $6.56 -30.15; BMEA×711 09:30 $1.90 → close $2.03 +92.43; OABI×282 09:30 $4.78 → close $4.33 -126.90; OPK×850 09:30 $1.59 → close $1.64 +42.50; VIR×117 09:30 $11.31 → close $11.38 +8.77 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.72 | ▼ 09:30 equity $10,660.56 vs yday $10,701.81 (-41.25) | 09:30 open · cash $5.72 (unchanged overnight, no fees) · equity $10,660.56 vs prior close $10,701.81 (-41.25) · 8 name(s) re-marked at the open (per-name table). ATRC×26 yday $51.52 → 09:30 $54.31 +72.54; CABA×383 yday $3.47 → 09:30 $3.43 -15.32; ALEC×536 yday $2.46 → 09:30 $2.38 -42.88; BHC×201 yday $6.56 → 09:30 $6.57 +2.01; BMEA×711 yday $2.03 → 09:30 $2.00 -21.33; OABI×282 yday $4.33 → 09:30 $4.30 -8.46; OPK×850 yday $1.64 → 09:30 $1.63 -8.50; VIR×117 yday $11.38 → 09:30 $11.22 -19.30 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 26 | $54.31 | $2.09 | $+33.02 | $1,415.69 | ▲ +33.02 after sell → book $10,658.47; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 383 | $3.43 | $5.01 | $-86.56 | $2,724.37 | ▼ -86.56 after sell → book $10,653.46; vs 09:30 mark -5.01 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 536 | $2.38 | $7.01 | $-88.97 | $3,993.03 | ▼ -88.97 after sell → book $10,646.44; vs 09:30 mark -7.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 201 | $6.57 | $2.64 | $-33.38 | $5,310.96 | ▼ -33.38 after sell → book $10,643.80; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 711 | $2.00 | $9.30 | $+52.63 | $6,723.66 | ▲ +52.63 after sell → book $10,634.50; vs 09:30 mark -9.30 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 282 | $4.30 | $3.69 | $-142.69 | $7,932.57 | ▼ -142.69 after sell → book $10,630.81; vs 09:30 mark -3.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 850 | $1.63 | $11.12 | $+11.92 | $9,306.95 | ▲ +11.92 after sell → book $10,619.69; vs 09:30 mark -11.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 117 | $11.22 | $2.37 | $-15.24 | $10,617.32 | ▼ -15.24 after sell → book $10,617.32; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,617.32 | ▲ close $10,617.32 vs 09:30 $10,660.56 (session +0.00) | 16:00 close · cash $10,617.32 · no lots left · equity $10,617.32. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,617.32 | ▲ 09:30 equity $10,617.32 vs yday $10,617.32 (-0.00) | 09:30 open · cash $10,617.32 · no holdings · equity $10,617.32 vs prior close $10,617.32 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,617.32 | ▲ close $10,617.32 vs 09:30 $10,617.32 (session +0.00) | 16:00 close · cash $10,617.32 · no lots left · equity $10,617.32. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,617.32 | ▲ 09:30 equity $10,617.32 vs yday $10,617.32 (-0.00) | 09:30 open · cash $10,617.32 · no holdings · equity $10,617.32 vs prior close $10,617.32 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,617.32 | ▲ close $10,617.32 vs 09:30 $10,617.32 (session +0.00) | 16:00 close · cash $10,617.32 · no lots left · equity $10,617.32. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,617.32 | ▲ 09:30 equity $10,617.32 vs yday $10,617.32 (-0.00) | 09:30 open · cash $10,617.32 · no holdings · equity $10,617.32 vs prior close $10,617.32 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 12 | $164.43 | $2.03 | — | $8,642.13 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,earn_react; ⚪; ret5=+4.9; leftover $2123.46 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 40 | $52.55 | $2.11 | — | $6,538.02 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $2123.46 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 210 | $10.11 | $2.71 | — | $4,412.21 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $2123.46 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 653 | $3.25 | $8.42 | — | $2,281.54 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $2123.46 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 116 | $18.30 | $2.34 | — | $156.40 | — | union ∩ white, no 🚨; gate zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $2123.46 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.40 | ▼ close $10,504.67 vs 09:30 $10,617.32 (session -95.04) | 16:00 close · cash $156.40 · equity $10,504.67 vs 09:30 $10,617.32 (-112.65; session marks -95.04) · 5 name(s) marked open→close (per-name table). ORCL×12 09:30 $164.43 → close $150.28 -169.80; BAND×40 09:30 $52.55 → close $56.87 +172.80; PAGS×210 09:30 $10.11 → close $10.12 +2.10; ZSQR×653 09:30 $3.25 → close $3.07 -117.54; PAYP×116 09:30 $18.30 → close $18.45 +17.40 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.40 | ▼ 09:30 equity $10,348.10 vs yday $10,504.67 (-156.57) | 09:30 open · cash $156.40 (unchanged overnight, no fees) · equity $10,348.10 vs prior close $10,504.67 (-156.57) · 5 name(s) re-marked at the open (per-name table). ORCL×12 yday $150.28 → 09:30 $141.42 -106.32; BAND×40 yday $56.87 → 09:30 $56.90 +1.20; PAGS×210 yday $10.12 → 09:30 $10.00 -25.20; ZSQR×653 yday $3.07 → 09:30 $3.06 -6.53; PAYP×116 yday $18.45 → 09:30 $18.28 -19.72 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 12 | $141.42 | $2.05 | $-280.20 | $1,851.39 | ▼ -280.20 after sell → book $10,346.05; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 40 | $56.90 | $2.14 | $+169.75 | $4,125.25 | ▲ +169.75 after sell → book $10,343.91; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 210 | $10.00 | $2.76 | $-28.57 | $6,222.49 | ▼ -28.57 after sell → book $10,341.15; vs 09:30 mark -2.76 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ZSQR` | 653 | $3.06 | $8.55 | $-141.04 | $8,212.12 | ▼ -141.04 after sell → book $10,332.60; vs 09:30 mark -8.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 116 | $18.28 | $2.37 | $-7.03 | $10,330.23 | ▼ -7.03 after sell → book $10,330.23; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,330.23 | ▲ close $10,330.23 vs 09:30 $10,348.10 (session +0.00) | 16:00 close · cash $10,330.23 · no lots left · equity $10,330.23. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,330.23 | ▲ 09:30 equity $10,330.23 vs yday $10,330.23 (+0.00) | 09:30 open · cash $10,330.23 · no holdings · equity $10,330.23 vs prior close $10,330.23 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,330.23 | ▲ close $10,330.23 vs 09:30 $10,330.23 (session +0.00) | 16:00 close · cash $10,330.23 · no lots left · equity $10,330.23. | — |
