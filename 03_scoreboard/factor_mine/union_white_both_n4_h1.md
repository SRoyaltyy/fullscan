# Factor mine action — `union_white_both_n4_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `list` · size `leftover` · sell `list` · S-boost `none` · −0 red + yday up AND catalyst, top 4 by Score

Cash book **+13.12%** ($11,312) · signal-only (no cash/fees) was +9.03%. Starts YES **7/26**. Fills 84 · skips 0 · realized $+1563.20.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: the morning-board Score (100 minus list rank) — only after the pool is chosen.
- Must-have: at most 0 red cameras (the −R half of +G −R; 🚨 is not counted here).
- Must-have: yesterday's session was up AND a major good catalyst.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by the morning-board Score (100 minus list rank) — only after the pool is chosen and keep the top 4.
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
- **Gate** `cam_bad_max=0,yday_and_catalyst=True` · **rank** `list` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $190.65.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 83 | — | $59.80 | +0.00 | $60.23 | +35.69 | +35.69 | +0.00 | +35.69 |
| 2026-08-13 | `TPG` | 98 | — | $50.62 | +0.00 | $54.62 | +391.69 | +391.69 | +0.00 | +391.69 |
| 2026-08-14 | `BTSG` | 83 | $60.23 | $59.65 | -48.14 | — | +0.00 | -48.14 | -12.45 | — |
| 2026-08-14 | `TPG` | 98 | $54.62 | $55.29 | +65.66 | — | +0.00 | +65.66 | +457.35 | — |
| 2026-08-14 | `SLG` | 45 | — | $57.61 | +0.00 | $56.09 | -68.40 | -68.40 | +0.00 | -68.40 |
| 2026-08-14 | `BETR` | 176 | — | $14.80 | +0.00 | $13.73 | -188.32 | -188.32 | +0.00 | -188.32 |
| 2026-08-14 | `ANGX` | 605 | — | $4.31 | +0.00 | $4.37 | +36.30 | +36.30 | +0.00 | +36.30 |
| 2026-08-14 | `WDC` | 5 | — | $503.50 | +0.00 | $508.80 | +26.50 | +26.50 | +0.00 | +26.50 |
| 2026-08-17 | `SLG` | 45 | $56.09 | $55.37 | -32.40 | — | +0.00 | -32.40 | -100.80 | — |
| 2026-08-17 | `BETR` | 176 | $13.73 | $13.67 | -10.56 | — | +0.00 | -10.56 | -198.88 | — |
| 2026-08-17 | `ANGX` | 605 | $4.37 | $4.60 | +139.15 | — | +0.00 | +139.15 | +175.45 | — |
| 2026-08-17 | `WDC` | 5 | $508.80 | $525.53 | +83.65 | — | +0.00 | +83.65 | +110.15 | — |
| 2026-08-17 | `ABX` | 284 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `ALM` | 160 | — | $16.20 | +0.00 | $16.36 | +25.60 | +25.60 | +0.00 | +25.60 |
| 2026-08-17 | `NMAX` | 236 | — | $10.97 | +0.00 | $10.36 | -143.96 | -143.96 | +0.00 | -143.96 |
| 2026-08-17 | `AAOI` | 17 | — | $152.64 | +0.00 | $154.89 | +38.25 | +38.25 | +0.00 | +38.25 |
| 2026-08-18 | `ABX` | 284 | $9.12 | $9.03 | -25.56 | — | +0.00 | -25.56 | -25.56 | — |
| 2026-08-18 | `ALM` | 160 | $16.36 | $15.78 | -92.80 | — | +0.00 | -92.80 | -67.20 | — |
| 2026-08-18 | `NMAX` | 236 | $10.36 | $10.31 | -11.80 | — | +0.00 | -11.80 | -155.76 | — |
| 2026-08-18 | `AAOI` | 17 | $154.89 | $146.20 | -147.73 | — | +0.00 | -147.73 | -109.48 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 27 | — | $91.01 | +0.00 | $93.63 | +70.74 | +70.74 | +0.00 | +70.74 |
| 2026-08-20 | `KGC` | 84 | — | $29.63 | +0.00 | $31.43 | +151.20 | +151.20 | +0.00 | +151.20 |
| 2026-08-20 | `WPM` | 17 | — | $144.54 | +0.00 | $150.25 | +97.07 | +97.07 | +0.00 | +97.07 |
| 2026-08-20 | `CYPH` | 2176 | — | $1.15 | +0.00 | $1.19 | +87.04 | +87.04 | +0.00 | +87.04 |
| 2026-08-21 | `BHP` | 27 | $93.63 | $95.72 | +56.43 | — | +0.00 | +56.43 | +127.17 | — |
| 2026-08-21 | `KGC` | 84 | $31.43 | $32.17 | +62.16 | — | +0.00 | +62.16 | +213.36 | — |
| 2026-08-21 | `WPM` | 17 | $150.25 | $154.70 | +75.65 | — | +0.00 | +75.65 | +172.72 | — |
| 2026-08-21 | `CYPH` | 2176 | $1.19 | $1.32 | +282.88 | $1.42 | +217.60 | +500.48 | +369.92 | +587.52 |
| 2026-08-21 | `AUPH` | 154 | — | $17.20 | +0.00 | $16.65 | -84.70 | -84.70 | +0.00 | -84.70 |
| 2026-08-21 | `AEM` | 12 | — | $216.30 | +0.00 | $216.06 | -2.88 | -2.88 | +0.00 | -2.88 |
| 2026-08-21 | `ARCT` | 239 | — | $11.13 | +0.00 | $13.45 | +554.48 | +554.48 | +0.00 | +554.48 |
| 2026-08-24 | `CYPH` | 2176 | $1.42 | $1.83 | +892.16 | — | +0.00 | +892.16 | +1479.68 | — |
| 2026-08-24 | `AUPH` | 154 | $16.65 | $16.57 | -12.32 | — | +0.00 | -12.32 | -97.02 | — |
| 2026-08-24 | `AEM` | 12 | $216.06 | $217.03 | +11.64 | — | +0.00 | +11.64 | +8.76 | — |
| 2026-08-24 | `ARCT` | 239 | $13.45 | $13.33 | -28.68 | — | +0.00 | -28.68 | +525.80 | — |
| 2026-08-25 | `CRMD` | 369 | — | $8.35 | +0.00 | $8.56 | +77.49 | +77.49 | +0.00 | +77.49 |
| 2026-08-25 | `BMEA` | 1895 | — | $1.63 | +0.00 | $1.73 | +189.50 | +189.50 | +0.00 | +189.50 |
| 2026-08-25 | `CYPH` | 1980 | — | $1.56 | +0.00 | $1.64 | +158.40 | +158.40 | +0.00 | +158.40 |
| 2026-08-25 | `EZPW` | 86 | — | $35.05 | +0.00 | $35.23 | +15.48 | +15.48 | +0.00 | +15.48 |
| 2026-08-26 | `CRMD` | 369 | $8.56 | $8.60 | +14.76 | — | +0.00 | +14.76 | +92.25 | — |
| 2026-08-26 | `BMEA` | 1895 | $1.73 | $1.75 | +47.37 | — | +0.00 | +47.37 | +236.88 | — |
| 2026-08-26 | `CYPH` | 1980 | $1.64 | $1.60 | -79.20 | — | +0.00 | -79.20 | +79.20 | — |
| 2026-08-26 | `EZPW` | 86 | $35.23 | $35.70 | +40.42 | — | +0.00 | +40.42 | +55.90 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | `SMTC` | 22 | — | $141.76 | +0.00 | $131.17 | -232.98 | -232.98 | +0.00 | -232.98 |
| 2026-08-28 | `TTMI` | 25 | — | $122.81 | +0.00 | $118.65 | -104.00 | -104.00 | +0.00 | -104.00 |
| 2026-08-28 | `KEYS` | 9 | — | $324.41 | +0.00 | $319.97 | -39.96 | -39.96 | +0.00 | -39.96 |
| 2026-08-28 | `AVT` | 34 | — | $91.49 | +0.00 | $88.63 | -97.24 | -97.24 | +0.00 | -97.24 |
| 2026-08-31 | `SMTC` | 22 | $131.17 | $132.30 | +24.86 | — | +0.00 | +24.86 | -208.12 | — |
| 2026-08-31 | `TTMI` | 25 | $118.65 | $118.83 | +4.50 | — | +0.00 | +4.50 | -99.50 | — |
| 2026-08-31 | `KEYS` | 9 | $319.97 | $322.49 | +22.68 | — | +0.00 | +22.68 | -17.28 | — |
| 2026-08-31 | `AVT` | 34 | $88.63 | $89.39 | +25.84 | — | +0.00 | +25.84 | -71.40 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 58 | — | $52.88 | +0.00 | $52.46 | -24.36 | -24.36 | +0.00 | -24.36 |
| 2026-09-03 | `HRMY` | 71 | — | $42.93 | +0.00 | $41.86 | -75.97 | -75.97 | +0.00 | -75.97 |
| 2026-09-03 | `CABA` | 846 | — | $3.63 | +0.00 | $3.48 | -126.90 | -126.90 | +0.00 | -126.90 |
| 2026-09-03 | `VSTM` | 382 | — | $8.03 | +0.00 | $7.98 | -19.10 | -19.10 | +0.00 | -19.10 |
| 2026-09-04 | `ATRC` | 58 | $52.46 | $52.03 | -24.94 | — | +0.00 | -24.94 | -49.30 | — |
| 2026-09-04 | `HRMY` | 71 | $41.86 | $41.50 | -25.56 | — | +0.00 | -25.56 | -101.53 | — |
| 2026-09-04 | `CABA` | 846 | $3.48 | $3.46 | -16.92 | — | +0.00 | -16.92 | -143.82 | — |
| 2026-09-04 | `VSTM` | 382 | $7.98 | $7.91 | -26.74 | — | +0.00 | -26.74 | -45.84 | — |
| 2026-09-04 | `CRM` | 11 | — | $263.36 | +0.00 | $259.23 | -45.43 | -45.43 | +0.00 | -45.43 |
| 2026-09-04 | `DELL` | 5 | — | $513.78 | +0.00 | $524.14 | +51.80 | +51.80 | +0.00 | +51.80 |
| 2026-09-04 | `IRD` | 657 | — | $4.53 | +0.00 | $4.67 | +91.98 | +91.98 | +0.00 | +91.98 |
| 2026-09-04 | `LENZ` | 517 | — | $5.75 | +0.00 | $5.96 | +108.57 | +108.57 | +0.00 | +108.57 |
| 2026-09-08 | `CRM` | 11 | $259.23 | $253.72 | -60.61 | — | +0.00 | -60.61 | -106.04 | — |
| 2026-09-08 | `DELL` | 5 | $524.14 | $521.15 | -14.95 | — | +0.00 | -14.95 | +36.85 | — |
| 2026-09-08 | `IRD` | 657 | $4.67 | $4.53 | -91.98 | — | +0.00 | -91.98 | +0.00 | — |
| 2026-09-08 | `LENZ` | 517 | $5.96 | $5.95 | -5.17 | — | +0.00 | -5.17 | +103.40 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `BAND` | 75 | — | $52.55 | +0.00 | $56.87 | +324.00 | +324.00 | +0.00 | +324.00 |
| 2026-09-11 | `PAYP` | 216 | — | $18.30 | +0.00 | $18.45 | +32.40 | +32.40 | +0.00 | +32.40 |
| 2026-09-11 | `SEDG` | 107 | — | $36.78 | +0.00 | $34.68 | -224.70 | -224.70 | +0.00 | -224.70 |
| 2026-09-14 | `BAND` | 75 | $56.87 | $56.90 | +2.25 | — | +0.00 | +2.25 | +326.25 | — |
| 2026-09-14 | `PAYP` | 216 | $18.45 | $18.28 | -36.72 | — | +0.00 | -36.72 | -4.32 | — |
| 2026-09-14 | `SEDG` | 107 | $34.68 | $33.64 | -111.28 | — | +0.00 | -111.28 | -335.98 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `SWKS` | 66 | — | $89.38 | +0.00 | $85.59 | -250.14 | -250.14 | +0.00 | -250.14 |
| 2026-09-16 | `QRVO` | 50 | — | $118.18 | +0.00 | $113.97 | -210.50 | -210.50 | +0.00 | -210.50 |
| 2026-09-17 | `SWKS` | 66 | $85.59 | $86.76 | +77.22 | — | +0.00 | +77.22 | -172.92 | — |
| 2026-09-17 | `QRVO` | 50 | $113.97 | $114.90 | +46.50 | — | +0.00 | +46.50 | -164.00 | — |
| 2026-09-17 | `VOD` | 328 | — | $17.56 | +0.00 | $17.52 | -13.12 | -13.12 | +0.00 | -13.12 |
| 2026-09-17 | `ASAN` | 603 | — | $9.55 | +0.00 | $10.09 | +325.62 | +325.62 | +0.00 | +325.62 |
| 2026-09-18 | `VOD` | 328 | $17.52 | $16.73 | -259.12 | — | +0.00 | -259.12 | -272.24 | — |
| 2026-09-18 | `ASAN` | 603 | $10.09 | $10.09 | +0.00 | — | +0.00 | +0.00 | +325.62 | — |
| 2026-09-18 | `ILMN` | 11 | — | $249.13 | +0.00 | $239.62 | -104.61 | -104.61 | +0.00 | -104.61 |
| 2026-09-18 | `SDGR` | 98 | — | $29.32 | +0.00 | $29.02 | -29.40 | -29.40 | +0.00 | -29.40 |
| 2026-09-18 | `ARQT` | 110 | — | $26.14 | +0.00 | $25.38 | -83.60 | -83.60 | +0.00 | -83.60 |
| 2026-09-18 | `FTRE` | 143 | — | $20.10 | +0.00 | $19.93 | -24.31 | -24.31 | +0.00 | -24.31 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +427.38 | BTSG, TPG | — | $71.00 | $10,422.85 | BTSG×83, TPG×98 |
| 2026-08-14 | +5.50 | $71.00 | BTSG×83, TPG×98 | $10,440.37 | +17.52 | -193.92 | SLG, BETR, ANGX, WDC | BTSG, TPG | $98.98 | $10,227.36 | SLG×45, BETR×176, ANGX×605, WDC×5 |
| 2026-08-17 | +2.25 | $98.98 | SLG×45, BETR×176, ANGX×605, WDC×5 | $10,407.20 | +179.84 | -80.11 | ABX, ALM, NMAX, AAOI | SLG, BETR, ANGX, WDC | $15.42 | $10,301.19 | ABX×284, ALM×160, NMAX×236, AAOI×17 |
| 2026-08-18 | -6.20 | $15.42 | ABX×284, ALM×160, NMAX×236, AAOI×17 | $10,023.30 | -277.89 | +0.00 | — | ABX, ALM, NMAX, AAOI | $10,011.88 | $10,011.88 | — |
| 2026-08-19 | -7.20 | $10,011.88 | — | $10,011.88 | -0.00 | +0.00 | — | — | $10,011.88 | $10,011.88 | — |
| 2026-08-20 | +1.12 | $10,011.88 | — | $10,011.88 | -0.00 | +406.05 | BHP, KGC, WPM, CYPH | — | $71.68 | $10,383.50 | BHP×27, KGC×84, WPM×17, CYPH×2176 |
| 2026-08-21 | +3.25 | $71.68 | BHP×27, KGC×84, WPM×17, CYPH×2176 | $10,860.62 | +477.12 | +684.50 | AUPH, AEM, ARCT | BHP, KGC, WPM | $69.82 | $11,531.11 | CYPH×2176, AUPH×154, AEM×12, ARCT×239 |
| 2026-08-24 | -5.17 | $69.82 | CYPH×2176, AUPH×154, AEM×12, ARCT×239 | $12,393.91 | +862.80 | +0.00 | — | CYPH, AUPH, AEM, ARCT | $12,357.75 | $12,357.75 | — |
| 2026-08-25 | +1.80 | $12,357.75 | — | $12,357.75 | -0.00 | +440.87 | CRMD, BMEA, CYPH, EZPW | — | $27.65 | $12,741.62 | CRMD×369, BMEA×1895, CYPH×1980, EZPW×86 |
| 2026-08-26 | +2.02 | $27.65 | CRMD×369, BMEA×1895, CYPH×1980, EZPW×86 | $12,764.98 | +23.36 | +0.00 | — | CRMD, BMEA, CYPH, EZPW | $12,707.16 | $12,707.16 | — |
| 2026-08-27 | — | $12,707.16 | — | $12,707.16 | -0.00 | +0.00 | — | — | $12,707.16 | $12,707.16 | — |
| 2026-08-28 | +0.75 | $12,707.16 | — | $12,707.16 | -0.00 | -474.18 | SMTC, TTMI, KEYS, AVT | — | $479.61 | $12,224.75 | SMTC×22, TTMI×25, KEYS×9, AVT×34 |
| 2026-08-31 | -5.85 | $479.61 | SMTC×22, TTMI×25, KEYS×9, AVT×34 | $12,302.63 | +77.88 | +0.00 | — | SMTC, TTMI, KEYS, AVT | $12,294.26 | $12,294.26 | — |
| 2026-09-01 | -6.30 | $12,294.26 | — | $12,294.26 | +0.00 | +0.00 | — | — | $12,294.26 | $12,294.26 | — |
| 2026-09-02 | -3.83 | $12,294.26 | — | $12,294.26 | +0.00 | +0.00 | — | — | $12,294.26 | $12,294.26 | — |
| 2026-09-03 | -0.90 | $12,294.26 | — | $12,294.26 | +0.00 | -246.33 | ATRC, HRMY, CABA, VSTM | — | $20.55 | $12,027.73 | ATRC×58, HRMY×71, CABA×846, VSTM×382 |
| 2026-09-04 | +2.25 | $20.55 | ATRC×58, HRMY×71, CABA×846, VSTM×382 | $11,933.57 | -94.16 | +206.92 | CRM, DELL, IRD, LENZ | ATRC, HRMY, CABA, VSTM | $479.04 | $12,100.78 | CRM×11, DELL×5, IRD×657, LENZ×517 |
| 2026-09-08 | -11.47 | $479.04 | CRM×11, DELL×5, IRD×657, LENZ×517 | $11,928.07 | -172.71 | +0.00 | — | CRM, DELL, IRD, LENZ | $11,908.60 | $11,908.60 | — |
| 2026-09-09 | -13.95 | $11,908.60 | — | $11,908.60 | -0.00 | +0.00 | — | — | $11,908.60 | $11,908.60 | — |
| 2026-09-10 | -13.28 | $11,908.60 | — | $11,908.60 | -0.00 | +0.00 | — | — | $11,908.60 | $11,908.60 | — |
| 2026-09-11 | +0.50 | $11,908.60 | — | $11,908.60 | -0.00 | +131.70 | BAND, PAYP, SEDG | — | $71.77 | $12,032.98 | BAND×75, PAYP×216, SEDG×107 |
| 2026-09-14 | -11.00 | $71.77 | BAND×75, PAYP×216, SEDG×107 | $11,887.23 | -145.75 | +0.00 | — | BAND, PAYP, SEDG | $11,879.76 | $11,879.76 | — |
| 2026-09-15 | -3.84 | $11,879.76 | — | $11,879.76 | +0.00 | +0.00 | — | — | $11,879.76 | $11,879.76 | — |
| 2026-09-16 | +5.30 | $11,879.76 | — | $11,879.76 | +0.00 | -460.64 | SWKS, QRVO | — | $67.35 | $11,414.79 | SWKS×66, QRVO×50 |
| 2026-09-17 | +7.38 | $67.35 | SWKS×66, QRVO×50 | $11,538.51 | +123.72 | +312.50 | VOD, ASAN | SWKS, QRVO | $3.73 | $11,834.56 | VOD×328, ASAN×603 |
| 2026-09-18 | +4.86 | $3.73 | VOD×328, ASAN×603 | $11,575.44 | -259.12 | -241.92 | ILMN, SDGR, ARQT, FTRE | VOD, ASAN | $190.65 | $11,312.22 | ILMN×11, SDGR×98, ARQT×110, FTRE×143 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 83 | $59.80 | $2.24 | — | $5,034.36 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=-5.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $71.00 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.00 | ▲ close $10,422.85 vs 09:30 $10,000.00 (session +427.38) | 16:00 close · cash $71.00 · equity $10,422.85 vs 09:30 $10,000.00 (+422.85; session marks +427.38) · 2 name(s) marked open→close (per-name table). BTSG×83 09:30 $59.80 → close $60.23 +35.69; TPG×98 09:30 $50.62 → close $54.62 +391.69 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.00 | ▲ 09:30 equity $10,440.37 vs yday $10,422.85 (+17.52) | 09:30 open · cash $71.00 (unchanged overnight, no fees) · equity $10,440.37 vs prior close $10,422.85 (+17.52) · 2 name(s) re-marked at the open (per-name table). BTSG×83 yday $60.23 → 09:30 $59.65 -48.14; TPG×98 yday $54.62 → 09:30 $55.29 +65.66 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 83 | $59.65 | $2.29 | $-16.98 | $5,019.66 | ▼ -16.98 after sell → book $10,438.08; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 98 | $55.29 | $2.34 | $+452.72 | $10,435.74 | ▲ +452.72 after sell → book $10,435.74; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 45 | $57.61 | $2.12 | — | $7,841.16 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.7; leftover $2608.93 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 176 | $14.80 | $2.52 | — | $5,233.84 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=-9.9; leftover $2608.93 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 605 | $4.31 | $7.80 | — | $2,618.49 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2608.93 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 5 | $503.50 | $2.00 | — | $98.98 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable; 🔵; ⚪; ret5=+7.9; leftover $2608.93 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.98 | ▼ close $10,227.36 vs 09:30 $10,440.37 (session -193.92) | 16:00 close · cash $98.98 · equity $10,227.36 vs 09:30 $10,440.37 (-213.01; session marks -193.92) · 4 name(s) marked open→close (per-name table). SLG×45 09:30 $57.61 → close $56.09 -68.40; BETR×176 09:30 $14.80 → close $13.73 -188.32; ANGX×605 09:30 $4.31 → close $4.37 +36.30; WDC×5 09:30 $503.50 → close $508.80 +26.50 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.98 | ▲ 09:30 equity $10,407.20 vs yday $10,227.36 (+179.84) | 09:30 open · cash $98.98 (unchanged overnight, no fees) · equity $10,407.20 vs prior close $10,227.36 (+179.84) · 4 name(s) re-marked at the open (per-name table). SLG×45 yday $56.09 → 09:30 $55.37 -32.40; BETR×176 yday $13.73 → 09:30 $13.67 -10.56; ANGX×605 yday $4.37 → 09:30 $4.60 +139.15; WDC×5 yday $508.80 → 09:30 $525.53 +83.65 | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 45 | $55.37 | $2.15 | $-105.08 | $2,588.48 | ▼ -105.08 after sell → book $10,405.05; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 176 | $13.67 | $2.57 | $-203.96 | $4,991.83 | ▼ -203.96 after sell → book $10,402.48; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 605 | $4.60 | $7.93 | $+159.72 | $7,766.91 | ▲ +159.72 after sell → book $10,394.56; vs 09:30 mark -7.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 5 | $525.53 | $2.04 | $+106.11 | $10,392.52 | ▲ +106.11 after sell → book $10,392.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 284 | $9.12 | $3.66 | — | $7,798.78 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $2598.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 160 | $16.20 | $2.47 | — | $5,204.31 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $2598.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 236 | $10.97 | $3.04 | — | $2,612.34 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,oppset; ⚪; ret5=+21.2; leftover $2598.13 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 17 | $152.64 | $2.04 | — | $15.42 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $2598.13 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.42 | ▼ close $10,301.19 vs 09:30 $10,407.20 (session -80.11) | 16:00 close · cash $15.42 · equity $10,301.19 vs 09:30 $10,407.20 (-106.01; session marks -80.11) · 4 name(s) marked open→close (per-name table). ABX×284 09:30 $9.12 → close $9.12 +0.00; ALM×160 09:30 $16.20 → close $16.36 +25.60; NMAX×236 09:30 $10.97 → close $10.36 -143.96; AAOI×17 09:30 $152.64 → close $154.89 +38.25 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.42 | ▼ 09:30 equity $10,023.30 vs yday $10,301.19 (-277.89) | 09:30 open · cash $15.42 (unchanged overnight, no fees) · equity $10,023.30 vs prior close $10,301.19 (-277.89) · 4 name(s) re-marked at the open (per-name table). ABX×284 yday $9.12 → 09:30 $9.03 -25.56; ALM×160 yday $16.36 → 09:30 $15.78 -92.80; NMAX×236 yday $10.36 → 09:30 $10.31 -11.80; AAOI×17 yday $154.89 → 09:30 $146.20 -147.73 | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 284 | $9.03 | $3.73 | $-32.95 | $2,576.21 | ▼ -32.95 after sell → book $10,019.57; vs 09:30 mark -3.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 160 | $15.78 | $2.52 | $-72.19 | $5,098.49 | ▼ -72.19 after sell → book $10,017.05; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 236 | $10.31 | $3.10 | $-161.91 | $7,528.55 | ▼ -161.91 after sell → book $10,013.95; vs 09:30 mark -3.10 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `AAOI` | 17 | $146.20 | $2.07 | $-113.59 | $10,011.88 | ▼ -113.59 after sell → book $10,011.88; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,011.88 | ▲ close $10,011.88 vs 09:30 $10,023.30 (session +0.00) | 16:00 close · cash $10,011.88 · no lots left · equity $10,011.88. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,011.88 | ▲ 09:30 equity $10,011.88 vs yday $10,011.88 (-0.00) | 09:30 open · cash $10,011.88 · no holdings · equity $10,011.88 vs prior close $10,011.88 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,011.88 | ▲ close $10,011.88 vs 09:30 $10,011.88 (session +0.00) | 16:00 close · cash $10,011.88 · no lots left · equity $10,011.88. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,011.88 | ▲ 09:30 equity $10,011.88 vs yday $10,011.88 (-0.00) | 09:30 open · cash $10,011.88 · no holdings · equity $10,011.88 vs prior close $10,011.88 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 27 | $91.01 | $2.07 | — | $7,552.54 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2502.97 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 84 | $29.63 | $2.24 | — | $5,061.38 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $2502.97 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 17 | $144.54 | $2.04 | — | $2,602.16 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy,oppset; 🔵; ⚪; ret5=+9.2; leftover $2502.97 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2176 | $1.15 | $28.07 | — | $71.68 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2502.97 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.68 | ▲ close $10,383.50 vs 09:30 $10,011.88 (session +406.05) | 16:00 close · cash $71.68 · equity $10,383.50 vs 09:30 $10,011.88 (+371.62; session marks +406.05) · 4 name(s) marked open→close (per-name table). BHP×27 09:30 $91.01 → close $93.63 +70.74; KGC×84 09:30 $29.63 → close $31.43 +151.20; WPM×17 09:30 $144.54 → close $150.25 +97.07; CYPH×2176 09:30 $1.15 → close $1.19 +87.04 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.68 | ▲ 09:30 equity $10,860.62 vs yday $10,383.50 (+477.12) | 09:30 open · cash $71.68 (unchanged overnight, no fees) · equity $10,860.62 vs prior close $10,383.50 (+477.12) · 4 name(s) re-marked at the open (per-name table). BHP×27 yday $93.63 → 09:30 $95.72 +56.43; KGC×84 yday $31.43 → 09:30 $32.17 +62.16; WPM×17 yday $150.25 → 09:30 $154.70 +75.65; CYPH×2176 yday $1.19 → 09:30 $1.32 +282.88 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 27 | $95.72 | $2.10 | $+123.00 | $2,654.02 | ▲ +123.00 after sell → book $10,858.52; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 84 | $32.17 | $2.28 | $+208.84 | $5,354.03 | ▲ +208.84 after sell → book $10,856.25; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 17 | $154.70 | $2.07 | $+168.61 | $7,981.85 | ▲ +168.61 after sell → book $10,854.17; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 154 | $17.20 | $2.45 | — | $5,330.60 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $2660.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 12 | $216.30 | $2.03 | — | $2,732.98 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $2660.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 239 | $11.13 | $3.08 | — | $69.82 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,yday_gainer,mover_buy,oppset; 🔵; ⚪; ret5=+39.8; leftover $2660.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.82 | ▲ close $11,531.11 vs 09:30 $10,860.62 (session +684.50) | 16:00 close · cash $69.82 · equity $11,531.11 vs 09:30 $10,860.62 (+670.49; session marks +684.50) · 4 name(s) marked open→close (per-name table). CYPH×2176 09:30 $1.32 → close $1.42 +217.60; AUPH×154 09:30 $17.20 → close $16.65 -84.70; AEM×12 09:30 $216.30 → close $216.06 -2.88; ARCT×239 09:30 $11.13 → close $13.45 +554.48 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.82 | ▲ 09:30 equity $12,393.91 vs yday $11,531.11 (+862.80) | 09:30 open · cash $69.82 (unchanged overnight, no fees) · equity $12,393.91 vs prior close $11,531.11 (+862.80) · 4 name(s) re-marked at the open (per-name table). CYPH×2176 yday $1.42 → 09:30 $1.83 +892.16; AUPH×154 yday $16.65 → 09:30 $16.57 -12.32; AEM×12 yday $216.06 → 09:30 $217.03 +11.64; ARCT×239 yday $13.45 → 09:30 $13.33 -28.68 | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 2176 | $1.83 | $28.46 | $+1423.15 | $4,023.44 | ▲ +1,423.15 after sell → book $12,365.45; vs 09:30 mark -28.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 154 | $16.57 | $2.50 | $-101.97 | $6,572.72 | ▼ -101.97 after sell → book $12,362.95; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 12 | $217.03 | $2.06 | $+4.68 | $9,175.02 | ▲ +4.68 after sell → book $12,360.89; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 239 | $13.33 | $3.15 | $+519.57 | $12,357.75 | ▲ +519.57 after sell → book $12,357.75; vs 09:30 mark -3.14 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,357.75 | ▲ close $12,357.75 vs 09:30 $12,393.91 (session +0.00) | 16:00 close · cash $12,357.75 · no lots left · equity $12,357.75. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,357.75 | ▲ 09:30 equity $12,357.75 vs yday $12,357.75 (-0.00) | 09:30 open · cash $12,357.75 · no holdings · equity $12,357.75 vs prior close $12,357.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 369 | $8.35 | $4.76 | — | $9,271.84 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+8.0; leftover $3089.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 1895 | $1.63 | $24.45 | — | $6,158.54 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $3089.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1980 | $1.56 | $25.54 | — | $3,044.20 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $3089.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 86 | $35.05 | $2.25 | — | $27.65 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,oppset; 🔵; ⚪; ret5=+19.7; leftover $3089.44 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.65 | ▲ close $12,741.62 vs 09:30 $12,357.75 (session +440.87) | 16:00 close · cash $27.65 · equity $12,741.62 vs 09:30 $12,357.75 (+383.87; session marks +440.87) · 4 name(s) marked open→close (per-name table). CRMD×369 09:30 $8.35 → close $8.56 +77.49; BMEA×1895 09:30 $1.63 → close $1.73 +189.50; CYPH×1980 09:30 $1.56 → close $1.64 +158.40; EZPW×86 09:30 $35.05 → close $35.23 +15.48 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.65 | ▲ 09:30 equity $12,764.98 vs yday $12,741.62 (+23.36) | 09:30 open · cash $27.65 (unchanged overnight, no fees) · equity $12,764.98 vs prior close $12,741.62 (+23.36) · 4 name(s) re-marked at the open (per-name table). CRMD×369 yday $8.56 → 09:30 $8.60 +14.76; BMEA×1895 yday $1.73 → 09:30 $1.75 +47.37; CYPH×1980 yday $1.64 → 09:30 $1.60 -79.20; EZPW×86 yday $35.23 → 09:30 $35.70 +40.42 | — |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 369 | $8.60 | $4.85 | $+82.64 | $3,196.20 | ▲ +82.64 after sell → book $12,760.13; vs 09:30 mark -4.85 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 1895 | $1.75 | $24.79 | $+187.64 | $6,497.14 | ▲ +187.64 after sell → book $12,735.34; vs 09:30 mark -24.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1980 | $1.60 | $25.90 | $+27.76 | $9,639.25 | ▲ +27.76 after sell → book $12,709.45; vs 09:30 mark -25.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 86 | $35.70 | $2.29 | $+51.37 | $12,707.16 | ▲ +51.37 after sell → book $12,707.16; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,707.16 | ▲ close $12,707.16 vs 09:30 $12,764.98 (session +0.00) | 16:00 close · cash $12,707.16 · no lots left · equity $12,707.16. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,707.16 | ▲ 09:30 equity $12,707.16 vs yday $12,707.16 (-0.00) | 09:30 open · cash $12,707.16 · no holdings · equity $12,707.16 vs prior close $12,707.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,707.16 | ▲ close $12,707.16 vs 09:30 $12,707.16 (session +0.00) | 16:00 close · cash $12,707.16 · no lots left · equity $12,707.16. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,707.16 | ▲ 09:30 equity $12,707.16 vs yday $12,707.16 (-0.00) | 09:30 open · cash $12,707.16 · no holdings · equity $12,707.16 vs prior close $12,707.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 22 | $141.76 | $2.06 | — | $9,586.38 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $3176.79 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 25 | $122.81 | $2.06 | — | $6,514.07 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $3176.79 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 9 | $324.41 | $2.02 | — | $3,592.36 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $3176.79 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 34 | $91.49 | $2.09 | — | $479.61 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $3176.79 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $479.61 | ▼ close $12,224.75 vs 09:30 $12,707.16 (session -474.18) | 16:00 close · cash $479.61 · equity $12,224.75 vs 09:30 $12,707.16 (-482.41; session marks -474.18) · 4 name(s) marked open→close (per-name table). SMTC×22 09:30 $141.76 → close $131.17 -232.98; TTMI×25 09:30 $122.81 → close $118.65 -104.00; KEYS×9 09:30 $324.41 → close $319.97 -39.96; AVT×34 09:30 $91.49 → close $88.63 -97.24 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $479.61 | ▲ 09:30 equity $12,302.63 vs yday $12,224.75 (+77.88) | 09:30 open · cash $479.61 (unchanged overnight, no fees) · equity $12,302.63 vs prior close $12,224.75 (+77.88) · 4 name(s) re-marked at the open (per-name table). SMTC×22 yday $131.17 → 09:30 $132.30 +24.86; TTMI×25 yday $118.65 → 09:30 $118.83 +4.50; KEYS×9 yday $319.97 → 09:30 $322.49 +22.68; AVT×34 yday $88.63 → 09:30 $89.39 +25.84 | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 22 | $132.30 | $2.09 | $-212.27 | $3,388.12 | ▼ -212.27 after sell → book $12,300.54; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 25 | $118.83 | $2.10 | $-103.66 | $6,356.77 | ▼ -103.66 after sell → book $12,298.44; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 9 | $322.49 | $2.05 | $-21.35 | $9,257.13 | ▼ -21.35 after sell → book $12,296.39; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 34 | $89.39 | $2.13 | $-75.62 | $12,294.26 | ▼ -75.62 after sell → book $12,294.26; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,294.26 | ▲ close $12,294.26 vs 09:30 $12,302.63 (session +0.00) | 16:00 close · cash $12,294.26 · no lots left · equity $12,294.26. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,294.26 | ▲ 09:30 equity $12,294.26 vs yday $12,294.26 (+0.00) | 09:30 open · cash $12,294.26 · no holdings · equity $12,294.26 vs prior close $12,294.26 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,294.26 | ▲ close $12,294.26 vs 09:30 $12,294.26 (session +0.00) | 16:00 close · cash $12,294.26 · no lots left · equity $12,294.26. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,294.26 | ▲ 09:30 equity $12,294.26 vs yday $12,294.26 (+0.00) | 09:30 open · cash $12,294.26 · no holdings · equity $12,294.26 vs prior close $12,294.26 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,294.26 | ▲ close $12,294.26 vs 09:30 $12,294.26 (session +0.00) | 16:00 close · cash $12,294.26 · no lots left · equity $12,294.26. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,294.26 | ▲ 09:30 equity $12,294.26 vs yday $12,294.26 (+0.00) | 09:30 open · cash $12,294.26 · no holdings · equity $12,294.26 vs prior close $12,294.26 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 58 | $52.88 | $2.16 | — | $9,225.06 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $3073.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 71 | $42.93 | $2.20 | — | $6,174.83 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $3073.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 846 | $3.63 | $10.91 | — | $3,092.93 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $3073.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 382 | $8.03 | $4.93 | — | $20.55 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $3073.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.55 | ▼ close $12,027.73 vs 09:30 $12,294.26 (session -246.33) | 16:00 close · cash $20.55 · equity $12,027.73 vs 09:30 $12,294.26 (-266.53; session marks -246.33) · 4 name(s) marked open→close (per-name table). ATRC×58 09:30 $52.88 → close $52.46 -24.36; HRMY×71 09:30 $42.93 → close $41.86 -75.97; CABA×846 09:30 $3.63 → close $3.48 -126.90; VSTM×382 09:30 $8.03 → close $7.98 -19.10 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.55 | ▼ 09:30 equity $11,933.57 vs yday $12,027.73 (-94.16) | 09:30 open · cash $20.55 (unchanged overnight, no fees) · equity $11,933.57 vs prior close $12,027.73 (-94.16) · 4 name(s) re-marked at the open (per-name table). ATRC×58 yday $52.46 → 09:30 $52.03 -24.94; HRMY×71 yday $41.86 → 09:30 $41.50 -25.56; CABA×846 yday $3.48 → 09:30 $3.46 -16.92; VSTM×382 yday $7.98 → 09:30 $7.91 -26.74 | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 58 | $52.03 | $2.20 | $-53.66 | $3,036.09 | ▼ -53.66 after sell → book $11,931.37; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 71 | $41.50 | $2.24 | $-105.97 | $5,980.35 | ▼ -105.97 after sell → book $11,929.13; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 846 | $3.46 | $11.08 | $-165.81 | $8,896.43 | ▼ -165.81 after sell → book $11,918.05; vs 09:30 mark -11.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 382 | $7.91 | $5.02 | $-55.78 | $11,913.04 | ▼ -55.78 after sell → book $11,913.04; vs 09:30 mark -5.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 11 | $263.36 | $2.02 | — | $9,014.05 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2978.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 5 | $513.78 | $2.00 | — | $6,443.15 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy,oppset; 🔵; ⚪; ret5=+9.3; leftover $2978.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 657 | $4.53 | $8.48 | — | $3,458.46 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $2978.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 517 | $5.75 | $6.67 | — | $479.04 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $2978.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $479.04 | ▲ close $12,100.78 vs 09:30 $11,933.57 (session +206.92) | 16:00 close · cash $479.04 · equity $12,100.78 vs 09:30 $11,933.57 (+167.21; session marks +206.92) · 4 name(s) marked open→close (per-name table). CRM×11 09:30 $263.36 → close $259.23 -45.43; DELL×5 09:30 $513.78 → close $524.14 +51.80; IRD×657 09:30 $4.53 → close $4.67 +91.98; LENZ×517 09:30 $5.75 → close $5.96 +108.57 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $479.04 | ▼ 09:30 equity $11,928.07 vs yday $12,100.78 (-172.71) | 09:30 open · cash $479.04 (unchanged overnight, no fees) · equity $11,928.07 vs prior close $12,100.78 (-172.71) · 4 name(s) re-marked at the open (per-name table). CRM×11 yday $259.23 → 09:30 $253.72 -60.61; DELL×5 yday $524.14 → 09:30 $521.15 -14.95; IRD×657 yday $4.67 → 09:30 $4.53 -91.98; LENZ×517 yday $5.96 → 09:30 $5.95 -5.17 | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 11 | $253.72 | $2.06 | $-110.12 | $3,267.91 | ▼ -110.12 after sell → book $11,926.02; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 5 | $521.15 | $2.04 | $+32.81 | $5,871.62 | ▲ +32.81 after sell → book $11,923.98; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 657 | $4.53 | $8.61 | $-17.08 | $8,839.23 | ▼ -17.08 after sell → book $11,915.38; vs 09:30 mark -8.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 517 | $5.95 | $6.78 | $+89.95 | $11,908.60 | ▲ +89.95 after sell → book $11,908.60; vs 09:30 mark -6.78 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,908.60 | ▲ close $11,908.60 vs 09:30 $11,928.07 (session +0.00) | 16:00 close · cash $11,908.60 · no lots left · equity $11,908.60. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,908.60 | ▲ 09:30 equity $11,908.60 vs yday $11,908.60 (-0.00) | 09:30 open · cash $11,908.60 · no holdings · equity $11,908.60 vs prior close $11,908.60 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,908.60 | ▲ close $11,908.60 vs 09:30 $11,908.60 (session +0.00) | 16:00 close · cash $11,908.60 · no lots left · equity $11,908.60. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,908.60 | ▲ 09:30 equity $11,908.60 vs yday $11,908.60 (-0.00) | 09:30 open · cash $11,908.60 · no holdings · equity $11,908.60 vs prior close $11,908.60 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,908.60 | ▲ close $11,908.60 vs 09:30 $11,908.60 (session +0.00) | 16:00 close · cash $11,908.60 · no lots left · equity $11,908.60. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,908.60 | ▲ 09:30 equity $11,908.60 vs yday $11,908.60 (-0.00) | 09:30 open · cash $11,908.60 · no holdings · equity $11,908.60 vs prior close $11,908.60 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 75 | $52.55 | $2.21 | — | $7,965.13 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $3969.53 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 216 | $18.30 | $2.79 | — | $4,009.54 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $3969.53 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SEDG` | 107 | $36.78 | $2.31 | — | $71.77 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list oppset; 🔵; ⚪; ret5=+8.2; leftover $3969.53 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.77 | ▲ close $12,032.98 vs 09:30 $11,908.60 (session +131.70) | 16:00 close · cash $71.77 · equity $12,032.98 vs 09:30 $11,908.60 (+124.38; session marks +131.70) · 3 name(s) marked open→close (per-name table). BAND×75 09:30 $52.55 → close $56.87 +324.00; PAYP×216 09:30 $18.30 → close $18.45 +32.40; SEDG×107 09:30 $36.78 → close $34.68 -224.70 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.77 | ▼ 09:30 equity $11,887.23 vs yday $12,032.98 (-145.75) | 09:30 open · cash $71.77 (unchanged overnight, no fees) · equity $11,887.23 vs prior close $12,032.98 (-145.75) · 3 name(s) re-marked at the open (per-name table). BAND×75 yday $56.87 → 09:30 $56.90 +2.25; PAYP×216 yday $18.45 → 09:30 $18.28 -36.72; SEDG×107 yday $34.68 → 09:30 $33.64 -111.28 | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 75 | $56.90 | $2.26 | $+321.77 | $4,337.01 | ▲ +321.77 after sell → book $11,884.97; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 216 | $18.28 | $2.85 | $-9.96 | $8,282.64 | ▼ -9.96 after sell → book $11,882.12; vs 09:30 mark -2.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SEDG` | 107 | $33.64 | $2.36 | $-340.65 | $11,879.76 | ▼ -340.65 after sell → book $11,879.76; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,879.76 | ▲ close $11,879.76 vs 09:30 $11,887.23 (session +0.00) | 16:00 close · cash $11,879.76 · no lots left · equity $11,879.76. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,879.76 | ▲ 09:30 equity $11,879.76 vs yday $11,879.76 (+0.00) | 09:30 open · cash $11,879.76 · no holdings · equity $11,879.76 vs prior close $11,879.76 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,879.76 | ▲ close $11,879.76 vs 09:30 $11,879.76 (session +0.00) | 16:00 close · cash $11,879.76 · no lots left · equity $11,879.76. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,879.76 | ▲ 09:30 equity $11,879.76 vs yday $11,879.76 (+0.00) | 09:30 open · cash $11,879.76 · no holdings · equity $11,879.76 vs prior close $11,879.76 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 66 | $89.38 | $2.19 | — | $5,978.49 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $5939.88 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 50 | $118.18 | $2.14 | — | $67.35 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $5939.88 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.35 | ▼ close $11,414.79 vs 09:30 $11,879.76 (session -460.64) | 16:00 close · cash $67.35 · equity $11,414.79 vs 09:30 $11,879.76 (-464.97; session marks -460.64) · 2 name(s) marked open→close (per-name table). SWKS×66 09:30 $89.38 → close $85.59 -250.14; QRVO×50 09:30 $118.18 → close $113.97 -210.50 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.35 | ▲ 09:30 equity $11,538.51 vs yday $11,414.79 (+123.72) | 09:30 open · cash $67.35 (unchanged overnight, no fees) · equity $11,538.51 vs prior close $11,414.79 (+123.72) · 2 name(s) re-marked at the open (per-name table). SWKS×66 yday $85.59 → 09:30 $86.76 +77.22; QRVO×50 yday $113.97 → 09:30 $114.90 +46.50 | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 66 | $86.76 | $2.24 | $-177.35 | $5,791.27 | ▼ -177.35 after sell → book $11,536.27; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 50 | $114.90 | $2.20 | $-168.34 | $11,534.07 | ▼ -168.34 after sell → book $11,534.07; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `VOD` | 328 | $17.56 | $4.23 | — | $5,770.16 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $5767.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ASAN` | 603 | $9.55 | $7.78 | — | $3.73 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list oppset; 🔵; ⚪; ret5=+17.0; leftover $5767.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.73 | ▲ close $11,834.56 vs 09:30 $11,538.51 (session +312.50) | 16:00 close · cash $3.73 · equity $11,834.56 vs 09:30 $11,538.51 (+296.05; session marks +312.50) · 2 name(s) marked open→close (per-name table). VOD×328 09:30 $17.56 → close $17.52 -13.12; ASAN×603 09:30 $9.55 → close $10.09 +325.62 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.73 | ▼ 09:30 equity $11,575.44 vs yday $11,834.56 (-259.12) | 09:30 open · cash $3.73 (unchanged overnight, no fees) · equity $11,575.44 vs prior close $11,834.56 (-259.12) · 2 name(s) re-marked at the open (per-name table). VOD×328 yday $17.52 → 09:30 $16.73 -259.12; ASAN×603 yday $10.09 → 09:30 $10.09 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `VOD` | 328 | $16.73 | $4.33 | $-280.80 | $5,486.84 | ▼ -280.80 after sell → book $11,571.11; vs 09:30 mark -4.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ASAN` | 603 | $10.09 | $7.93 | $+309.91 | $11,563.18 | ▲ +309.91 after sell → book $11,563.18; vs 09:30 mark -7.93 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `ILMN` | 11 | $249.13 | $2.02 | — | $8,820.73 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+21.8; leftover $2890.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 98 | $29.32 | $2.28 | — | $5,945.09 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $2890.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ARQT` | 110 | $26.14 | $2.32 | — | $3,067.37 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,oppset; 🔵; ⚪; ret5=+13.2; leftover $2890.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FTRE` | 143 | $20.10 | $2.42 | — | $190.65 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+19.2; leftover $2890.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $190.65 | ▼ close $11,312.22 vs 09:30 $11,575.44 (session -241.92) | 16:00 close · cash $190.65 · equity $11,312.22 vs 09:30 $11,575.44 (-263.22; session marks -241.92) · 4 name(s) marked open→close (per-name table). ILMN×11 09:30 $249.13 → close $239.62 -104.61; SDGR×98 09:30 $29.32 → close $29.02 -29.40; ARQT×110 09:30 $26.14 → close $25.38 -83.60; FTRE×143 09:30 $20.10 → close $19.93 -24.31 | — |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ILMN` | 11 | 2026-09-18 @ $249.13 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+21.8; leftover $2890.80 |
| `SDGR` | 98 | 2026-09-18 @ $29.32 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $2890.80 |
| `ARQT` | 110 | 2026-09-18 @ $26.14 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,oppset; 🔵; ⚪; ret5=+13.2; leftover $2890.80 |
| `FTRE` | 143 | 2026-09-18 @ $20.10 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+19.2; leftover $2890.80 |
