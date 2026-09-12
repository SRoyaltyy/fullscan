# Factor mine action — `union_vol_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+8.74%** ($10,874) · signal-only (no cash/fees) was -2.21%. Starts YES **15/21**. Fills 136 · skips 47 · realized $+718.13.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the volume camera (is this name unusually active?) is green.
- Must-have: the last finished bar was green (closed up).
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
- **Gate** `vol=good,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $64.09.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `BTBT` | 833 | — | $1.50 | +0.00 | $1.57 | +58.31 | +58.31 | +0.00 | +58.31 |
| 2026-08-14 | `BETR` | 84 | — | $14.80 | +0.00 | $13.73 | -89.88 | -89.88 | +0.00 | -89.88 |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `HYLN` | 299 | — | $4.18 | +0.00 | $4.06 | -35.88 | -35.88 | +0.00 | -35.88 |
| 2026-08-14 | `ADUR` | 75 | — | $16.50 | +0.00 | $16.17 | -24.75 | -24.75 | +0.00 | -24.75 |
| 2026-08-14 | `AIRO` | 112 | — | $11.12 | +0.00 | $9.57 | -173.60 | -173.60 | +0.00 | -173.60 |
| 2026-08-14 | `NCMI` | 464 | — | $2.69 | +0.00 | $2.86 | +78.88 | +78.88 | +0.00 | +78.88 |
| 2026-08-14 | `QMLS` | 170 | — | $7.29 | +0.00 | $7.32 | +5.10 | +5.10 | +0.00 | +5.10 |
| 2026-08-17 | `BTBT` | 833 | $1.57 | $1.52 | -41.65 | — | +0.00 | -41.65 | +16.66 | — |
| 2026-08-17 | `BETR` | 84 | $13.73 | $13.67 | -5.04 | — | +0.00 | -5.04 | -94.92 | — |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `HYLN` | 299 | $4.06 | $4.10 | +11.96 | — | +0.00 | +11.96 | -23.92 | — |
| 2026-08-17 | `ADUR` | 75 | $16.17 | $15.73 | -33.00 | — | +0.00 | -33.00 | -57.75 | — |
| 2026-08-17 | `AIRO` | 112 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -173.60 | — |
| 2026-08-17 | `NCMI` | 464 | $2.86 | $2.80 | -27.84 | — | +0.00 | -27.84 | +51.04 | — |
| 2026-08-17 | `QMLS` | 170 | $7.32 | $7.24 | -13.60 | — | +0.00 | -13.60 | -8.50 | — |
| 2026-08-17 | `CDNL` | 30 | — | $39.85 | +0.00 | $39.23 | -18.60 | -18.60 | +0.00 | -18.60 |
| 2026-08-17 | `ABX` | 133 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `VERA` | 38 | — | $31.30 | +0.00 | $31.63 | +12.54 | +12.54 | +0.00 | +12.54 |
| 2026-08-17 | `HTFL` | 29 | — | $41.23 | +0.00 | $41.94 | +20.59 | +20.59 | +0.00 | +20.59 |
| 2026-08-17 | `UMAC` | 37 | — | $32.55 | +0.00 | $30.15 | -88.80 | -88.80 | +0.00 | -88.80 |
| 2026-08-17 | `NPWR` | 633 | — | $1.92 | +0.00 | $1.73 | -120.27 | -120.27 | +0.00 | -120.27 |
| 2026-08-17 | `LPTH` | 81 | — | $14.94 | +0.00 | $14.80 | -11.34 | -11.34 | +0.00 | -11.34 |
| 2026-08-17 | `NMAX` | 110 | — | $10.97 | +0.00 | $10.36 | -67.10 | -67.10 | +0.00 | -67.10 |
| 2026-08-18 | `CDNL` | 30 | $39.23 | $41.57 | +70.20 | — | +0.00 | +70.20 | +51.60 | — |
| 2026-08-18 | `ABX` | 133 | $9.12 | $9.03 | -11.97 | — | +0.00 | -11.97 | -11.97 | — |
| 2026-08-18 | `VERA` | 38 | $31.63 | $31.31 | -12.16 | — | +0.00 | -12.16 | +0.38 | — |
| 2026-08-18 | `HTFL` | 29 | $41.94 | $41.50 | -12.76 | — | +0.00 | -12.76 | +7.83 | — |
| 2026-08-18 | `UMAC` | 37 | $30.15 | $28.59 | -57.72 | — | +0.00 | -57.72 | -146.52 | — |
| 2026-08-18 | `NPWR` | 633 | $1.73 | $1.70 | -18.99 | — | +0.00 | -18.99 | -139.26 | — |
| 2026-08-18 | `LPTH` | 81 | $14.80 | $14.01 | -63.99 | — | +0.00 | -63.99 | -75.33 | — |
| 2026-08-18 | `NMAX` | 110 | $10.36 | $10.31 | -5.50 | — | +0.00 | -5.50 | -72.60 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 56 | — | $20.55 | +0.00 | $21.19 | +35.84 | +35.84 | +0.00 | +35.84 |
| 2026-08-20 | `CDE` | 56 | — | $20.65 | +0.00 | $21.11 | +25.76 | +25.76 | +0.00 | +25.76 |
| 2026-08-20 | `HDSN` | 201 | — | $5.77 | +0.00 | $5.57 | -40.20 | -40.20 | +0.00 | -40.20 |
| 2026-08-20 | `IAG` | 59 | — | $19.63 | +0.00 | $20.50 | +51.33 | +51.33 | +0.00 | +51.33 |
| 2026-08-20 | `KGC` | 39 | — | $29.63 | +0.00 | $31.43 | +70.20 | +70.20 | +0.00 | +70.20 |
| 2026-08-20 | `NFGC` | 663 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-20 | `ABUS` | 236 | — | $4.92 | +0.00 | $4.77 | -35.40 | -35.40 | +0.00 | -35.40 |
| 2026-08-21 | `AG` | 56 | $21.19 | $21.90 | +39.76 | — | +0.00 | +39.76 | +75.60 | — |
| 2026-08-21 | `CDE` | 56 | $21.11 | $21.75 | +35.84 | — | +0.00 | +35.84 | +61.60 | — |
| 2026-08-21 | `HDSN` | 201 | $5.57 | $5.67 | +20.10 | — | +0.00 | +20.10 | -20.10 | — |
| 2026-08-21 | `IAG` | 59 | $20.50 | $21.17 | +39.53 | — | +0.00 | +39.53 | +90.86 | — |
| 2026-08-21 | `KGC` | 39 | $31.43 | $32.17 | +28.86 | — | +0.00 | +28.86 | +99.06 | — |
| 2026-08-21 | `NFGC` | 663 | $1.75 | $1.79 | +26.52 | — | +0.00 | +26.52 | +26.52 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `ABUS` | 236 | $4.77 | $5.20 | +101.48 | — | +0.00 | +101.48 | +66.08 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 70 | — | $17.20 | +0.00 | $16.65 | -38.50 | -38.50 | +0.00 | -38.50 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 109 | — | $11.13 | +0.00 | $13.45 | +252.88 | +252.88 | +0.00 | +252.88 |
| 2026-08-21 | `CYPH` | 920 | — | $1.32 | +0.00 | $1.42 | +92.00 | +92.00 | +0.00 | +92.00 |
| 2026-08-21 | `BTBT` | 732 | — | $1.66 | +0.00 | $1.53 | -95.16 | -95.16 | +0.00 | -95.16 |
| 2026-08-21 | `DE` | 1 | — | $623.26 | +0.00 | $647.47 | +24.21 | +24.21 | +0.00 | +24.21 |
| 2026-08-21 | `GORO` | 390 | — | $3.11 | +0.00 | $3.19 | +31.20 | +31.20 | +0.00 | +31.20 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 70 | $16.65 | $16.57 | -5.60 | — | +0.00 | -5.60 | -44.10 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 109 | $13.45 | $13.33 | -13.08 | — | +0.00 | -13.08 | +239.80 | — |
| 2026-08-24 | `CYPH` | 920 | $1.42 | $1.83 | +377.20 | — | +0.00 | +377.20 | +469.20 | — |
| 2026-08-24 | `BTBT` | 732 | $1.53 | $1.55 | +14.64 | — | +0.00 | +14.64 | -80.52 | — |
| 2026-08-24 | `DE` | 1 | $647.47 | $653.04 | +5.57 | — | +0.00 | +5.57 | +29.78 | — |
| 2026-08-24 | `GORO` | 390 | $3.19 | $3.20 | +3.90 | — | +0.00 | +3.90 | +35.10 | — |
| 2026-08-25 | `KURA` | 94 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CCOI` | 135 | — | $9.49 | +0.00 | $9.88 | +52.65 | +52.65 | +0.00 | +52.65 |
| 2026-08-25 | `LIFE` | 34 | — | $36.96 | +0.00 | $38.56 | +54.40 | +54.40 | +0.00 | +54.40 |
| 2026-08-25 | `ZIP` | 283 | — | $4.55 | +0.00 | $4.35 | -56.60 | -56.60 | +0.00 | -56.60 |
| 2026-08-25 | `BMEA` | 790 | — | $1.63 | +0.00 | $1.73 | +79.00 | +79.00 | +0.00 | +79.00 |
| 2026-08-25 | `NPWR` | 644 | — | $2.00 | +0.00 | $1.95 | -32.20 | -32.20 | +0.00 | -32.20 |
| 2026-08-25 | `ALVO` | 246 | — | $5.24 | +0.00 | $5.05 | -46.74 | -46.74 | +0.00 | -46.74 |
| 2026-08-25 | `SUJA` | 146 | — | $8.79 | +0.00 | $9.33 | +78.84 | +78.84 | +0.00 | +78.84 |
| 2026-08-26 | `KURA` | 94 | $13.59 | $13.63 | +3.76 | — | +0.00 | +3.76 | +3.76 | — |
| 2026-08-26 | `CCOI` | 135 | $9.88 | $9.89 | +1.35 | — | +0.00 | +1.35 | +54.00 | — |
| 2026-08-26 | `LIFE` | 34 | $38.56 | $38.24 | -10.88 | — | +0.00 | -10.88 | +43.52 | — |
| 2026-08-26 | `ZIP` | 283 | $4.35 | $4.31 | -11.32 | — | +0.00 | -11.32 | -67.92 | — |
| 2026-08-26 | `BMEA` | 790 | $1.73 | $1.75 | +19.75 | — | +0.00 | +19.75 | +98.75 | — |
| 2026-08-26 | `NPWR` | 644 | $1.95 | $1.93 | -12.88 | — | +0.00 | -12.88 | -45.08 | — |
| 2026-08-26 | `ALVO` | 246 | $5.05 | $4.98 | -17.22 | — | +0.00 | -17.22 | -63.96 | — |
| 2026-08-26 | `SUJA` | 146 | $9.33 | $9.39 | +8.76 | — | +0.00 | +8.76 | +87.60 | — |
| 2026-08-26 | `USDE` | 1778 | — | $5.81 | +0.00 | $5.98 | +302.26 | +302.26 | +0.00 | +302.26 |
| 2026-08-27 | `USDE` | 1778 | $5.98 | $6.50 | +924.56 | — | +0.00 | +924.56 | +1226.82 | — |
| 2026-08-28 | `ANF` | 11 | — | $146.07 | +0.00 | $148.42 | +25.85 | +25.85 | +0.00 | +25.85 |
| 2026-08-28 | `CAPR` | 169 | — | $9.73 | +0.00 | $9.59 | -23.66 | -23.66 | +0.00 | -23.66 |
| 2026-08-28 | `ERAS` | 85 | — | $19.25 | +0.00 | $18.03 | -103.70 | -103.70 | +0.00 | -103.70 |
| 2026-08-28 | `SYRE` | 17 | — | $91.75 | +0.00 | $90.36 | -23.63 | -23.63 | +0.00 | -23.63 |
| 2026-08-28 | `NCNO` | 70 | — | $23.30 | +0.00 | $22.99 | -21.70 | -21.70 | +0.00 | -21.70 |
| 2026-08-28 | `TH` | 86 | — | $19.00 | +0.00 | $18.55 | -38.70 | -38.70 | +0.00 | -38.70 |
| 2026-08-28 | `GAP` | 66 | — | $24.69 | +0.00 | $23.48 | -79.86 | -79.86 | +0.00 | -79.86 |
| 2026-08-31 | `ANF` | 11 | $148.42 | $148.03 | -4.29 | — | +0.00 | -4.29 | +21.56 | — |
| 2026-08-31 | `CAPR` | 169 | $9.59 | $9.50 | -15.21 | — | +0.00 | -15.21 | -38.87 | — |
| 2026-08-31 | `ERAS` | 85 | $18.03 | $17.87 | -13.60 | — | +0.00 | -13.60 | -117.30 | — |
| 2026-08-31 | `SYRE` | 17 | $90.36 | $89.15 | -20.57 | — | +0.00 | -20.57 | -44.20 | — |
| 2026-08-31 | `NCNO` | 70 | $22.99 | $22.66 | -23.10 | — | +0.00 | -23.10 | -44.80 | — |
| 2026-08-31 | `TH` | 86 | $18.55 | $18.12 | -36.55 | — | +0.00 | -36.55 | -75.25 | — |
| 2026-08-31 | `GAP` | 66 | $23.48 | $22.98 | -33.00 | — | +0.00 | -33.00 | -112.86 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `ARCT` | 82 | — | $16.77 | +0.00 | $15.56 | -99.22 | -99.22 | +0.00 | -99.22 |
| 2026-09-03 | `CRDL` | 635 | — | $2.18 | +0.00 | $2.16 | -12.70 | -12.70 | +0.00 | -12.70 |
| 2026-09-03 | `GPRO` | 778 | — | $1.78 | +0.00 | $1.39 | -303.42 | -303.42 | +0.00 | -303.42 |
| 2026-09-03 | `MMED` | 58 | — | $23.88 | +0.00 | $23.84 | -2.32 | -2.32 | +0.00 | -2.32 |
| 2026-09-03 | `NVAX` | 133 | — | $10.42 | +0.00 | $10.34 | -10.64 | -10.64 | +0.00 | -10.64 |
| 2026-09-03 | `BMEA` | 718 | — | $1.93 | +0.00 | $1.91 | -14.36 | -14.36 | +0.00 | -14.36 |
| 2026-09-03 | `DUOL` | 8 | — | $161.54 | +0.00 | $158.82 | -21.76 | -21.76 | +0.00 | -21.76 |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `ARCT` | 82 | $15.56 | $15.61 | +4.10 | — | +0.00 | +4.10 | -95.12 | — |
| 2026-09-04 | `CRDL` | 635 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -12.70 | — |
| 2026-09-04 | `GPRO` | 778 | $1.39 | $1.48 | +70.02 | — | +0.00 | +70.02 | -233.40 | — |
| 2026-09-04 | `MMED` | 58 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.32 | — |
| 2026-09-04 | `NVAX` | 133 | $10.34 | $10.50 | +21.28 | — | +0.00 | +21.28 | +10.64 | — |
| 2026-09-04 | `BMEA` | 718 | $1.91 | $1.90 | -7.18 | — | +0.00 | -7.18 | -21.54 | — |
| 2026-09-04 | `DUOL` | 8 | $158.82 | $157.46 | -10.88 | — | +0.00 | -10.88 | -32.64 | — |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-04 | `TARS` | 16 | — | $82.70 | +0.00 | $90.78 | +129.28 | +129.28 | +0.00 | +129.28 |
| 2026-09-04 | `BRR` | 528 | — | $2.51 | +0.00 | $2.66 | +79.20 | +79.20 | +0.00 | +79.20 |
| 2026-09-04 | `MDB` | 3 | — | $378.34 | +0.00 | $368.74 | -28.80 | -28.80 | +0.00 | -28.80 |
| 2026-09-04 | `ASST` | 52 | — | $25.18 | +0.00 | $27.14 | +101.92 | +101.92 | +0.00 | +101.92 |
| 2026-09-04 | `DFDV` | 228 | — | $5.79 | +0.00 | $5.87 | +18.24 | +18.24 | +0.00 | +18.24 |
| 2026-09-04 | `RSKD` | 193 | — | $6.84 | +0.00 | $6.51 | -63.69 | -63.69 | +0.00 | -63.69 |
| 2026-09-04 | `TDS` | 35 | — | $37.44 | +0.00 | $37.83 | +13.65 | +13.65 | +0.00 | +13.65 |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-08 | `TARS` | 16 | $90.78 | $89.67 | -17.76 | — | +0.00 | -17.76 | +111.52 | — |
| 2026-09-08 | `BRR` | 528 | $2.66 | $2.66 | +0.00 | — | +0.00 | +0.00 | +79.20 | — |
| 2026-09-08 | `MDB` | 3 | $368.74 | $360.75 | -23.97 | — | +0.00 | -23.97 | -52.77 | — |
| 2026-09-08 | `ASST` | 52 | $27.14 | $26.44 | -36.40 | — | +0.00 | -36.40 | +65.52 | — |
| 2026-09-08 | `DFDV` | 228 | $5.87 | $5.81 | -13.68 | — | +0.00 | -13.68 | +4.56 | — |
| 2026-09-08 | `RSKD` | 193 | $6.51 | $6.46 | -9.65 | — | +0.00 | -9.65 | -73.34 | — |
| 2026-09-08 | `TDS` | 35 | $37.83 | $37.75 | -2.80 | — | +0.00 | -2.80 | +10.85 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `CMRC` | 428 | — | $3.13 | +0.00 | $3.50 | +160.50 | +160.50 | +0.00 | +160.50 |
| 2026-09-11 | `INDP` | 496 | — | $2.70 | +0.00 | $2.77 | +34.72 | +34.72 | +0.00 | +34.72 |
| 2026-09-11 | `SWKS` | 15 | — | $84.27 | +0.00 | $88.35 | +61.20 | +61.20 | +0.00 | +61.20 |
| 2026-09-11 | `ANGX` | 249 | — | $5.38 | +0.00 | $5.45 | +17.43 | +17.43 | +0.00 | +17.43 |
| 2026-09-11 | `TSSI` | 149 | — | $8.98 | +0.00 | $8.93 | -7.45 | -7.45 | +0.00 | -7.45 |
| 2026-09-11 | `LDI` | 1576 | — | $0.85 | +0.00 | $0.83 | -23.64 | -23.64 | +0.00 | -23.64 |
| 2026-09-11 | `IRD` | 217 | — | $6.16 | +0.00 | $6.04 | -26.04 | -26.04 | +0.00 | -26.04 |
| 2026-09-11 | `VIST` | 17 | — | $77.33 | +0.00 | $76.27 | -18.02 | -18.02 | +0.00 | -18.02 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -164.42 | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | — | $3.57 | $9,801.97 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 |
| 2026-08-17 | +2.25 | $3.57 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | $9,759.50 | -42.47 | -272.98 | CDNL, ABX, VERA, HTFL, UMAC, NPWR, LPTH, NMAX | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | $71.87 | $9,428.97 | CDNL×30, ABX×133, VERA×38, HTFL×29, UMAC×37, NPWR×633, LPTH×81, NMAX×110 |
| 2026-08-18 | -6.20 | $71.87 | CDNL×30, ABX×133, VERA×38, HTFL×29, UMAC×37, NPWR×633, LPTH×81, NMAX×110 | $9,316.08 | -112.89 | +0.00 | — | CDNL, ABX, VERA, HTFL, UMAC, NPWR, LPTH, NMAX | $9,292.33 | $9,292.33 | — |
| 2026-08-19 | -7.20 | $9,292.33 | — | $9,292.33 | -0.00 | +0.00 | — | — | $9,292.33 | $9,292.33 | — |
| 2026-08-20 | +1.12 | $9,292.33 | — | $9,292.33 | -0.00 | +153.21 | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $9.13 | $9,420.74 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×663, WPM×8, ABUS×236 |
| 2026-08-21 | +3.25 | $9.13 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×663, WPM×8, ABUS×236 | $9,748.43 | +327.69 | +283.33 | AU, AUPH, AEM, ARCT, CYPH, BTBT, DE, GORO | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $727.79 | $9,969.77 | AU×10, AUPH×70, AEM×5, ARCT×109, CYPH×920, BTBT×732, DE×1, GORO×390 |
| 2026-08-24 | -5.17 | $727.79 | AU×10, AUPH×70, AEM×5, ARCT×109, CYPH×920, BTBT×732, DE×1, GORO×390 | $10,350.15 | +380.38 | +0.00 | — | AU, AUPH, AEM, ARCT, CYPH, BTBT, DE, GORO | $10,312.79 | $10,312.79 | — |
| 2026-08-25 | +1.80 | $10,312.79 | — | $10,312.79 | +0.00 | +129.35 | KURA, CCOI, LIFE, ZIP, BMEA, NPWR, ALVO, SUJA | — | $27.30 | $10,407.63 | KURA×94, CCOI×135, LIFE×34, ZIP×283, BMEA×790, NPWR×644, ALVO×246, SUJA×146 |
| 2026-08-26 | +2.02 | $27.30 | KURA×94, CCOI×135, LIFE×34, ZIP×283, BMEA×790, NPWR×644, ALVO×246, SUJA×146 | $10,388.95 | -18.68 | +302.26 | USDE | KURA, CCOI, LIFE, ZIP, BMEA, NPWR, ALVO, SUJA | $0.84 | $10,633.28 | USDE×1778 |
| 2026-08-27 | — | $0.84 | USDE×1778 | $11,557.84 | +924.56 | +0.00 | — | USDE | $11,534.52 | $11,534.52 | — |
| 2026-08-28 | +0.75 | $11,534.52 | — | $11,534.52 | +0.00 | -265.40 | ANF, CAPR, ERAS, SYRE, NCNO, TH, GAP | — | $177.40 | $11,253.68 | ANF×11, CAPR×169, ERAS×85, SYRE×17, NCNO×70, TH×86, GAP×66 |
| 2026-08-31 | -5.85 | $177.40 | ANF×11, CAPR×169, ERAS×85, SYRE×17, NCNO×70, TH×86, GAP×66 | $11,107.36 | -146.32 | +0.00 | — | ANF, CAPR, ERAS, SYRE, NCNO, TH, GAP | $11,091.73 | $11,091.73 | — |
| 2026-09-01 | -6.30 | $11,091.73 | — | $11,091.73 | +0.00 | +0.00 | — | — | $11,091.73 | $11,091.73 | — |
| 2026-09-02 | -3.83 | $11,091.73 | — | $11,091.73 | +0.00 | +0.00 | — | — | $11,091.73 | $11,091.73 | — |
| 2026-09-03 | -0.90 | $11,091.73 | — | $11,091.73 | +0.00 | -482.62 | RVTY, ARCT, CRDL, GPRO, MMED, NVAX, BMEA, DUOL | — | $135.68 | $10,570.80 | RVTY×10, ARCT×82, CRDL×635, GPRO×778, MMED×58, NVAX×133, BMEA×718, DUOL×8 |
| 2026-09-04 | +2.25 | $135.68 | RVTY×10, ARCT×82, CRDL×635, GPRO×778, MMED×58, NVAX×133, BMEA×718, DUOL×8 | $10,642.14 | +71.34 | +270.52 | DELL, TARS, BRR, MDB, ASST, DFDV, RSKD, TDS | RVTY, ARCT, CRDL, GPRO, MMED, NVAX, BMEA, DUOL | $509.66 | $10,851.24 | DELL×2, TARS×16, BRR×528, MDB×3, ASST×52, DFDV×228, RSKD×193, TDS×35 |
| 2026-09-08 | -11.47 | $509.66 | DELL×2, TARS×16, BRR×528, MDB×3, ASST×52, DFDV×228, RSKD×193, TDS×35 | $10,741.00 | -110.24 | +0.00 | — | DELL, TARS, BRR, MDB, ASST, DFDV, RSKD, TDS | $10,718.12 | $10,718.12 | — |
| 2026-09-09 | -13.95 | $10,718.12 | — | $10,718.12 | -0.00 | +0.00 | — | — | $10,718.12 | $10,718.12 | — |
| 2026-09-10 | -13.28 | $10,718.12 | — | $10,718.12 | -0.00 | +0.00 | — | — | $10,718.12 | $10,718.12 | — |
| 2026-09-11 | +0.50 | $10,718.12 | — | $10,718.12 | -0.00 | +198.70 | CMRC, INDP, SWKS, ANGX, TSSI, LDI, IRD, VIST | — | $64.09 | $10,874.25 | CMRC×428, INDP×496, SWKS×15, ANGX×249, TSSI×149, LDI×1576, IRD×217, VIST×17 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | combo gate; gate vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | combo gate; gate vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | combo gate; gate vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $2,499.51 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $1,245.37 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 170 | $7.29 | $2.50 | — | $3.57 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.57 | ▼ close $9,801.97 vs 09:30 $10,000.00 (session -164.42) | 16:00 close · cash $3.57 · equity $9,801.97 vs 09:30 $10,000.00 (-198.03; session marks -164.42) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.50 → close $1.57 +58.31; BETR×84 09:30 $14.80 → close $13.73 -89.88; ANGX×290 09:30 $4.31 → close $4.37 +17.40; HYLN×299 09:30 $4.18 → close $4.06 -35.88; ADUR×75 09:30 $16.50 → close $16.17 -24.75; AIRO×112 09:30 $11.12 → close $9.57 -173.60; NCMI×464 09:30 $2.69 → close $2.86 +78.88; QMLS×170 09:30 $7.29 → close $7.32 +5.10 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,759.50 vs yday $9,801.97 (-42.47) | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,759.50 vs prior close $9,801.97 (-42.47) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84; QMLS×170 yday $7.32 → 09:30 $7.24 -13.60 | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $1,258.83 | ▼ -4.98 after sell → book $9,748.60; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $2,404.85 | ▼ -99.43 after sell → book $9,746.34; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,735.05 | ▲ +76.56 after sell → book $9,742.54; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,957.03 | ▼ -31.69 after sell → book $9,738.62; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $6,134.54 | ▼ -62.20 after sell → book $9,736.38; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $7,204.03 | ▼ -178.28 after sell → book $9,734.03; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $8,497.16 | ▲ +38.98 after sell → book $9,727.96; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 170 | $7.24 | $2.54 | $-13.54 | $9,725.42 | ▼ -13.54 after sell → book $9,725.42; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 30 | $39.85 | $2.08 | — | $8,527.84 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1215.68 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 133 | $9.12 | $2.39 | — | $7,312.49 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1215.68 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 38 | $31.30 | $2.10 | — | $6,120.98 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=-3.8; leftover $1215.68 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,923.24 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+46.0; leftover $1215.68 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $3,716.79 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1215.68 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 633 | $1.92 | $8.17 | — | $2,493.26 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1215.68 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 81 | $14.94 | $2.23 | — | $1,280.89 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1215.68 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 110 | $10.97 | $2.32 | — | $71.87 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $1215.68 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.87 | ▼ close $9,428.97 vs 09:30 $9,759.50 (session -272.98) | 16:00 close · cash $71.87 · equity $9,428.97 vs 09:30 $9,759.50 (-330.53; session marks -272.98) · 8 name(s) marked open→close (per-name table). CDNL×30 09:30 $39.85 → close $39.23 -18.60; ABX×133 09:30 $9.12 → close $9.12 +0.00; VERA×38 09:30 $31.30 → close $31.63 +12.54; HTFL×29 09:30 $41.23 → close $41.94 +20.59; UMAC×37 09:30 $32.55 → close $30.15 -88.80; NPWR×633 09:30 $1.92 → close $1.73 -120.27; LPTH×81 09:30 $14.94 → close $14.80 -11.34; NMAX×110 09:30 $10.97 → close $10.36 -67.10 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.87 | ▼ 09:30 equity $9,316.08 vs yday $9,428.97 (-112.89) | 09:30 open · cash $71.87 (unchanged overnight, no fees) · equity $9,316.08 vs prior close $9,428.97 (-112.89) · 8 name(s) re-marked at the open (per-name table). CDNL×30 yday $39.23 → 09:30 $41.57 +70.20; ABX×133 yday $9.12 → 09:30 $9.03 -11.97; VERA×38 yday $31.63 → 09:30 $31.31 -12.16; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72; NPWR×633 yday $1.73 → 09:30 $1.70 -18.99; LPTH×81 yday $14.80 → 09:30 $14.01 -63.99; NMAX×110 yday $10.36 → 09:30 $10.31 -5.50 | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 30 | $41.57 | $2.10 | $+47.42 | $1,316.87 | ▲ +47.42 after sell → book $9,313.98; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 133 | $9.03 | $2.42 | $-16.78 | $2,515.44 | ▼ -16.78 after sell → book $9,311.56; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 38 | $31.31 | $2.12 | $-3.85 | $3,703.09 | ▼ -3.85 after sell → book $9,309.43; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $4,904.50 | ▲ +3.66 after sell → book $9,307.34; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $5,960.21 | ▼ -150.74 after sell → book $9,305.22; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 633 | $1.70 | $8.28 | $-155.71 | $7,028.02 | ▼ -155.71 after sell → book $9,296.93; vs 09:30 mark -8.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 81 | $14.01 | $2.26 | $-79.82 | $8,160.58 | ▼ -79.82 after sell → book $9,294.68; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 110 | $10.31 | $2.35 | $-77.27 | $9,292.33 | ▼ -77.27 after sell → book $9,292.33; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,292.33 | ▲ close $9,292.33 vs 09:30 $9,316.08 (session +0.00) | 16:00 close · cash $9,292.33 · no lots left · equity $9,292.33. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,292.33 | ▲ 09:30 equity $9,292.33 vs yday $9,292.33 (-0.00) | 09:30 open · cash $9,292.33 · no holdings · equity $9,292.33 vs prior close $9,292.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,292.33 | ▲ close $9,292.33 vs 09:30 $9,292.33 (session +0.00) | 16:00 close · cash $9,292.33 · no lots left · equity $9,292.33. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,292.33 | ▲ 09:30 equity $9,292.33 vs yday $9,292.33 (-0.00) | 09:30 open · cash $9,292.33 · no holdings · equity $9,292.33 vs prior close $9,292.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 56 | $20.55 | $2.16 | — | $8,139.37 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $6,980.81 | — | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 201 | $5.77 | $2.60 | — | $5,818.45 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $4,658.11 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,500.43 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 663 | $1.75 | $8.55 | — | $2,331.63 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,173.29 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 236 | $4.92 | $3.04 | — | $9.13 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.13 | ▲ close $9,420.74 vs 09:30 $9,292.33 (session +153.21) | 16:00 close · cash $9.13 · equity $9,420.74 vs 09:30 $9,292.33 (+128.41; session marks +153.21) · 8 name(s) marked open→close (per-name table). AG×56 09:30 $20.55 → close $21.19 +35.84; CDE×56 09:30 $20.65 → close $21.11 +25.76; HDSN×201 09:30 $5.77 → close $5.57 -40.20; IAG×59 09:30 $19.63 → close $20.50 +51.33; KGC×39 09:30 $29.63 → close $31.43 +70.20; NFGC×663 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68; ABUS×236 09:30 $4.92 → close $4.77 -35.40 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.13 | ▲ 09:30 equity $9,748.43 vs yday $9,420.74 (+327.69) | 09:30 open · cash $9.13 (unchanged overnight, no fees) · equity $9,748.43 vs prior close $9,420.74 (+327.69) · 8 name(s) re-marked at the open (per-name table). AG×56 yday $21.19 → 09:30 $21.90 +39.76; CDE×56 yday $21.11 → 09:30 $21.75 +35.84; HDSN×201 yday $5.57 → 09:30 $5.67 +20.10; IAG×59 yday $20.50 → 09:30 $21.17 +39.53; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×663 yday $1.75 → 09:30 $1.79 +26.52; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×236 yday $4.77 → 09:30 $5.20 +101.48 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 56 | $21.90 | $2.18 | $+71.26 | $1,233.35 | ▲ +71.26 after sell → book $9,746.25; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 56 | $21.75 | $2.18 | $+57.26 | $2,449.17 | ▲ +57.26 after sell → book $9,744.07; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 201 | $5.67 | $2.64 | $-25.34 | $3,586.20 | ▼ -25.34 after sell → book $9,741.43; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 59 | $21.17 | $2.19 | $+86.51 | $4,833.05 | ▲ +86.51 after sell → book $9,739.25; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 39 | $32.17 | $2.13 | $+94.83 | $6,085.55 | ▲ +94.83 after sell → book $9,737.12; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 663 | $1.79 | $8.67 | $+9.29 | $7,263.65 | ▲ +9.29 after sell → book $9,728.45; vs 09:30 mark -8.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $8,499.21 | ▲ +77.23 after sell → book $9,726.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 236 | $5.20 | $3.09 | $+59.94 | $9,723.32 | ▲ +59.94 after sell → book $9,723.32; vs 09:30 mark -3.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,527.00 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 70 | $17.20 | $2.20 | — | $7,320.80 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,237.29 | — | combo gate; gate vol=good,last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 109 | $11.13 | $2.32 | — | $5,021.81 | — | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 920 | $1.32 | $11.87 | — | $3,795.54 | — | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 732 | $1.66 | $9.44 | — | $2,570.98 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $1,945.72 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1215.41 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 390 | $3.11 | $5.03 | — | $727.79 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=+7.1; leftover $1215.41 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $727.79 | ▲ close $9,969.77 vs 09:30 $9,748.43 (session +283.33) | 16:00 close · cash $727.79 · equity $9,969.77 vs 09:30 $9,748.43 (+221.34; session marks +283.33) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×70 09:30 $17.20 → close $16.65 -38.50; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×109 09:30 $11.13 → close $13.45 +252.88; CYPH×920 09:30 $1.32 → close $1.42 +92.00; BTBT×732 09:30 $1.66 → close $1.53 -95.16; DE×1 09:30 $623.26 → close $647.47 +24.21; GORO×390 09:30 $3.11 → close $3.19 +31.20 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $727.79 | ▲ 09:30 equity $10,350.15 vs yday $9,969.77 (+380.38) | 09:30 open · cash $727.79 (unchanged overnight, no fees) · equity $10,350.15 vs prior close $9,969.77 (+380.38) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×70 yday $16.65 → 09:30 $16.57 -5.60; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×109 yday $13.45 → 09:30 $13.33 -13.08; CYPH×920 yday $1.42 → 09:30 $1.83 +377.20; BTBT×732 yday $1.53 → 09:30 $1.55 +14.64; DE×1 yday $647.47 → 09:30 $653.04 +5.57; GORO×390 yday $3.19 → 09:30 $3.20 +3.90 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,930.85 | ▲ +6.74 after sell → book $10,348.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 70 | $16.57 | $2.22 | $-48.52 | $3,088.53 | ▼ -48.52 after sell → book $10,345.89; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $4,171.66 | ▼ -0.38 after sell → book $10,343.87; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 109 | $13.33 | $2.35 | $+235.14 | $5,622.28 | ▲ +235.14 after sell → book $10,341.52; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 920 | $1.83 | $12.03 | $+445.30 | $7,293.84 | ▲ +445.30 after sell → book $10,329.48; vs 09:30 mark -12.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 732 | $1.55 | $9.57 | $-99.54 | $8,418.87 | ▼ -99.54 after sell → book $10,319.91; vs 09:30 mark -9.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 1 | $653.04 | $2.01 | $+25.77 | $9,069.90 | ▲ +25.77 after sell → book $10,317.90; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 390 | $3.20 | $5.11 | $+24.96 | $10,312.79 | ▲ +24.96 after sell → book $10,312.79; vs 09:30 mark -5.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,312.79 | ▲ close $10,312.79 vs 09:30 $10,350.15 (session +0.00) | 16:00 close · cash $10,312.79 · no lots left · equity $10,312.79. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,312.79 | ▲ 09:30 equity $10,312.79 vs yday $10,312.79 (+0.00) | 09:30 open · cash $10,312.79 · no holdings · equity $10,312.79 vs prior close $10,312.79 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 94 | $13.59 | $2.27 | — | $9,033.06 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1289.10 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 135 | $9.49 | $2.40 | — | $7,749.51 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1289.10 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 34 | $36.96 | $2.09 | — | $6,490.78 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1289.10 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 283 | $4.55 | $3.65 | — | $5,199.48 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1289.10 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 790 | $1.63 | $10.19 | — | $3,901.59 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1289.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 644 | $2.00 | $8.31 | — | $2,605.28 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $1289.10 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 246 | $5.24 | $3.17 | — | $1,313.07 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1289.10 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 146 | $8.79 | $2.43 | — | $27.30 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1289.10 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.30 | ▲ close $10,407.63 vs 09:30 $10,312.79 (session +129.35) | 16:00 close · cash $27.30 · equity $10,407.63 vs 09:30 $10,312.79 (+94.84; session marks +129.35) · 8 name(s) marked open→close (per-name table). KURA×94 09:30 $13.59 → close $13.59 +0.00; CCOI×135 09:30 $9.49 → close $9.88 +52.65; LIFE×34 09:30 $36.96 → close $38.56 +54.40; ZIP×283 09:30 $4.55 → close $4.35 -56.60; BMEA×790 09:30 $1.63 → close $1.73 +79.00; NPWR×644 09:30 $2.00 → close $1.95 -32.20; ALVO×246 09:30 $5.24 → close $5.05 -46.74; SUJA×146 09:30 $8.79 → close $9.33 +78.84 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.30 | ▼ 09:30 equity $10,388.95 vs yday $10,407.63 (-18.68) | 09:30 open · cash $27.30 (unchanged overnight, no fees) · equity $10,388.95 vs prior close $10,407.63 (-18.68) · 8 name(s) re-marked at the open (per-name table). KURA×94 yday $13.59 → 09:30 $13.63 +3.76; CCOI×135 yday $9.88 → 09:30 $9.89 +1.35; LIFE×34 yday $38.56 → 09:30 $38.24 -10.88; ZIP×283 yday $4.35 → 09:30 $4.31 -11.32; BMEA×790 yday $1.73 → 09:30 $1.75 +19.75; NPWR×644 yday $1.95 → 09:30 $1.93 -12.88; ALVO×246 yday $5.05 → 09:30 $4.98 -17.22; SUJA×146 yday $9.33 → 09:30 $9.39 +8.76 | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 94 | $13.63 | $2.30 | $-0.81 | $1,306.22 | ▼ -0.81 after sell → book $10,386.65; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 135 | $9.89 | $2.43 | $+49.18 | $2,638.95 | ▲ +49.18 after sell → book $10,384.23; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 34 | $38.24 | $2.11 | $+39.32 | $3,936.99 | ▲ +39.32 after sell → book $10,382.11; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 283 | $4.31 | $3.71 | $-75.28 | $5,153.02 | ▼ -75.28 after sell → book $10,378.41; vs 09:30 mark -3.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 790 | $1.75 | $10.33 | $+78.23 | $6,529.13 | ▲ +78.23 after sell → book $10,368.07; vs 09:30 mark -10.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `NPWR` | 644 | $1.93 | $8.42 | $-61.81 | $7,763.63 | ▼ -61.81 after sell → book $10,359.65; vs 09:30 mark -8.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 246 | $4.98 | $3.22 | $-70.36 | $8,985.48 | ▼ -70.36 after sell → book $10,356.42; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUJA` | 146 | $9.39 | $2.46 | $+82.71 | $10,353.96 | ▲ +82.71 after sell → book $10,353.96; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 1778 | $5.81 | $22.94 | — | $0.84 | — | combo gate; gate vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $10353.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.84 | ▲ close $10,633.28 vs 09:30 $10,388.95 (session +302.26) | 16:00 close · cash $0.84 · equity $10,633.28 vs 09:30 $10,388.95 (+244.33; session marks +302.26) · 1 name(s) marked open→close (per-name table). USDE×1778 09:30 $5.81 → close $5.98 +302.26 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.84 | ▲ 09:30 equity $11,557.84 vs yday $10,633.28 (+924.56) | 09:30 open · cash $0.84 (unchanged overnight, no fees) · equity $11,557.84 vs prior close $10,633.28 (+924.56) · 1 name(s) re-marked at the open (per-name table). USDE×1778 yday $5.98 → 09:30 $6.50 +924.56 | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 1778 | $6.50 | $23.32 | $+1180.56 | $11,534.52 | ▲ +1,180.56 after sell → book $11,534.52; vs 09:30 mark -23.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,534.52 | ▲ close $11,534.52 vs 09:30 $11,557.84 (session +0.00) | 16:00 close · cash $11,534.52 · no lots left · equity $11,534.52. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,534.52 | ▲ 09:30 equity $11,534.52 vs yday $11,534.52 (+0.00) | 09:30 open · cash $11,534.52 · no holdings · equity $11,534.52 vs prior close $11,534.52 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 11 | $146.07 | $2.02 | — | $9,925.73 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1647.79 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 169 | $9.73 | $2.50 | — | $8,278.86 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+47.1; leftover $1647.79 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 85 | $19.25 | $2.25 | — | $6,640.37 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; ret5=+14.1; leftover $1647.79 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SYRE` | 17 | $91.75 | $2.04 | — | $5,078.57 | — | combo gate; gate vol=good,last_green=True; list yday_mover; ret5=-13.2; leftover $1647.79 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 70 | $23.30 | $2.20 | — | $3,445.37 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $1647.79 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 86 | $19.00 | $2.25 | — | $1,809.13 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; ret5=+7.5; leftover $1647.79 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 66 | $24.69 | $2.19 | — | $177.40 | — | combo gate; gate vol=good,last_green=True; list earn_react; ret5=+5.8; leftover $1647.79 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $177.40 | ▼ close $11,253.68 vs 09:30 $11,534.52 (session -265.40) | 16:00 close · cash $177.40 · equity $11,253.68 vs 09:30 $11,534.52 (-280.84; session marks -265.40) · 7 name(s) marked open→close (per-name table). ANF×11 09:30 $146.07 → close $148.42 +25.85; CAPR×169 09:30 $9.73 → close $9.59 -23.66; ERAS×85 09:30 $19.25 → close $18.03 -103.70; SYRE×17 09:30 $91.75 → close $90.36 -23.63; NCNO×70 09:30 $23.30 → close $22.99 -21.70; TH×86 09:30 $19.00 → close $18.55 -38.70; GAP×66 09:30 $24.69 → close $23.48 -79.86 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $177.40 | ▼ 09:30 equity $11,107.36 vs yday $11,253.68 (-146.32) | 09:30 open · cash $177.40 (unchanged overnight, no fees) · equity $11,107.36 vs prior close $11,253.68 (-146.32) · 7 name(s) re-marked at the open (per-name table). ANF×11 yday $148.42 → 09:30 $148.03 -4.29; CAPR×169 yday $9.59 → 09:30 $9.50 -15.21; ERAS×85 yday $18.03 → 09:30 $17.87 -13.60; SYRE×17 yday $90.36 → 09:30 $89.15 -20.57; NCNO×70 yday $22.99 → 09:30 $22.66 -23.10; TH×86 yday $18.55 → 09:30 $18.12 -36.55; GAP×66 yday $23.48 → 09:30 $22.98 -33.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 11 | $148.03 | $2.05 | $+17.49 | $1,803.68 | ▲ +17.49 after sell → book $11,105.31; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 169 | $9.50 | $2.54 | $-43.90 | $3,406.64 | ▼ -43.90 after sell → book $11,102.77; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 85 | $17.87 | $2.27 | $-121.82 | $4,923.32 | ▼ -121.82 after sell → book $11,100.50; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SYRE` | 17 | $89.15 | $2.06 | $-48.30 | $6,436.81 | ▼ -48.30 after sell → book $11,098.44; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 70 | $22.66 | $2.22 | $-49.22 | $8,020.79 | ▼ -49.22 after sell → book $11,096.22; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 86 | $18.12 | $2.27 | $-79.77 | $9,577.26 | ▼ -79.77 after sell → book $11,093.94; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 66 | $22.98 | $2.21 | $-117.26 | $11,091.73 | ▼ -117.26 after sell → book $11,091.73; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,091.73 | ▲ close $11,091.73 vs 09:30 $11,107.36 (session +0.00) | 16:00 close · cash $11,091.73 · no lots left · equity $11,091.73. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,091.73 | ▲ 09:30 equity $11,091.73 vs yday $11,091.73 (+0.00) | 09:30 open · cash $11,091.73 · no holdings · equity $11,091.73 vs prior close $11,091.73 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,091.73 | ▲ close $11,091.73 vs 09:30 $11,091.73 (session +0.00) | 16:00 close · cash $11,091.73 · no lots left · equity $11,091.73. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,091.73 | ▲ 09:30 equity $11,091.73 vs yday $11,091.73 (+0.00) | 09:30 open · cash $11,091.73 · no holdings · equity $11,091.73 vs prior close $11,091.73 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,091.73 | ▲ close $11,091.73 vs 09:30 $11,091.73 (session +0.00) | 16:00 close · cash $11,091.73 · no lots left · equity $11,091.73. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,091.73 | ▲ 09:30 equity $11,091.73 vs yday $11,091.73 (+0.00) | 09:30 open · cash $11,091.73 · no holdings · equity $11,091.73 vs prior close $11,091.73 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $9,765.21 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1386.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 82 | $16.77 | $2.24 | — | $8,387.83 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1386.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 635 | $2.18 | $8.19 | — | $6,995.34 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1386.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 778 | $1.78 | $10.04 | — | $5,600.47 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+183.1; leftover $1386.47 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 58 | $23.88 | $2.16 | — | $4,213.26 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1386.47 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 133 | $10.42 | $2.39 | — | $2,825.01 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1386.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 718 | $1.93 | $9.26 | — | $1,430.01 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1386.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 8 | $161.54 | $2.01 | — | $135.68 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; leftover $1386.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $135.68 | ▼ close $10,570.80 vs 09:30 $11,091.73 (session -482.62) | 16:00 close · cash $135.68 · equity $10,570.80 vs 09:30 $11,091.73 (-520.93; session marks -482.62) · 8 name(s) marked open→close (per-name table). RVTY×10 09:30 $132.45 → close $130.63 -18.20; ARCT×82 09:30 $16.77 → close $15.56 -99.22; CRDL×635 09:30 $2.18 → close $2.16 -12.70; GPRO×778 09:30 $1.78 → close $1.39 -303.42; MMED×58 09:30 $23.88 → close $23.84 -2.32; NVAX×133 09:30 $10.42 → close $10.34 -10.64; BMEA×718 09:30 $1.93 → close $1.91 -14.36; DUOL×8 09:30 $161.54 → close $158.82 -21.76 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $135.68 | ▲ 09:30 equity $10,642.14 vs yday $10,570.80 (+71.34) | 09:30 open · cash $135.68 (unchanged overnight, no fees) · equity $10,642.14 vs prior close $10,570.80 (+71.34) · 8 name(s) re-marked at the open (per-name table). RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; ARCT×82 yday $15.56 → 09:30 $15.61 +4.10; CRDL×635 yday $2.16 → 09:30 $2.16 +0.00; GPRO×778 yday $1.39 → 09:30 $1.48 +70.02; MMED×58 yday $23.84 → 09:30 $23.84 +0.00; NVAX×133 yday $10.34 → 09:30 $10.50 +21.28; BMEA×718 yday $1.91 → 09:30 $1.90 -7.18; DUOL×8 yday $158.82 → 09:30 $157.46 -10.88 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $1,433.94 | ▼ -28.26 after sell → book $10,640.10; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 82 | $15.61 | $2.26 | $-99.62 | $2,711.70 | ▼ -99.62 after sell → book $10,637.84; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 635 | $2.16 | $8.31 | $-29.20 | $4,074.99 | ▼ -29.20 after sell → book $10,629.53; vs 09:30 mark -8.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GPRO` | 778 | $1.48 | $10.18 | $-253.61 | $5,216.25 | ▼ -253.61 after sell → book $10,619.35; vs 09:30 mark -10.18 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 58 | $23.84 | $2.19 | $-6.67 | $6,596.79 | ▼ -6.67 after sell → book $10,617.17; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 133 | $10.50 | $2.42 | $+5.83 | $7,990.87 | ▲ +5.83 after sell → book $10,614.75; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 718 | $1.90 | $9.39 | $-40.19 | $9,345.67 | ▼ -40.19 after sell → book $10,605.35; vs 09:30 mark -9.40 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 8 | $157.46 | $2.03 | $-36.69 | $10,603.32 | ▼ -36.69 after sell → book $10,603.32; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $9,573.76 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1325.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 16 | $82.70 | $2.04 | — | $8,248.53 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1325.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 528 | $2.51 | $6.81 | — | $6,916.43 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1325.41 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 3 | $378.34 | $2.00 | — | $5,779.42 | — | combo gate; gate vol=good,last_green=True; list yday_mover; ret5=-12.7; leftover $1325.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 52 | $25.18 | $2.15 | — | $4,467.91 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.0; leftover $1325.41 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 228 | $5.79 | $2.94 | — | $3,144.85 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1325.41 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `RSKD` | 193 | $6.84 | $2.57 | — | $1,822.16 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; ret5=+13.2; leftover $1325.41 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 35 | $37.44 | $2.10 | — | $509.66 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; leftover $1325.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $509.66 | ▲ close $10,851.24 vs 09:30 $10,642.14 (session +270.52) | 16:00 close · cash $509.66 · equity $10,851.24 vs 09:30 $10,642.14 (+209.10; session marks +270.52) · 8 name(s) marked open→close (per-name table). DELL×2 09:30 $513.78 → close $524.14 +20.72; TARS×16 09:30 $82.70 → close $90.78 +129.28; BRR×528 09:30 $2.51 → close $2.66 +79.20; MDB×3 09:30 $378.34 → close $368.74 -28.80; ASST×52 09:30 $25.18 → close $27.14 +101.92; DFDV×228 09:30 $5.79 → close $5.87 +18.24; RSKD×193 09:30 $6.84 → close $6.51 -63.69; TDS×35 09:30 $37.44 → close $37.83 +13.65 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $509.66 | ▼ 09:30 equity $10,741.00 vs yday $10,851.24 (-110.24) | 09:30 open · cash $509.66 (unchanged overnight, no fees) · equity $10,741.00 vs prior close $10,851.24 (-110.24) · 8 name(s) re-marked at the open (per-name table). DELL×2 yday $524.14 → 09:30 $521.15 -5.98; TARS×16 yday $90.78 → 09:30 $89.67 -17.76; BRR×528 yday $2.66 → 09:30 $2.66 +0.00; MDB×3 yday $368.74 → 09:30 $360.75 -23.97; ASST×52 yday $27.14 → 09:30 $26.44 -36.40; DFDV×228 yday $5.87 → 09:30 $5.81 -13.68; RSKD×193 yday $6.51 → 09:30 $6.46 -9.65; TDS×35 yday $37.83 → 09:30 $37.75 -2.80 | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $1,549.95 | ▲ +10.73 after sell → book $10,738.99; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 16 | $89.67 | $2.06 | $+107.42 | $2,982.61 | ▲ +107.42 after sell → book $10,736.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 528 | $2.66 | $6.91 | $+65.48 | $4,380.18 | ▲ +65.48 after sell → book $10,730.02; vs 09:30 mark -6.91 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 3 | $360.75 | $2.02 | $-56.79 | $5,460.41 | ▼ -56.79 after sell → book $10,728.00; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 52 | $26.44 | $2.17 | $+61.21 | $6,833.12 | ▲ +61.21 after sell → book $10,725.83; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 228 | $5.81 | $2.99 | $-1.37 | $8,154.81 | ▼ -1.37 after sell → book $10,722.84; vs 09:30 mark -2.99 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `RSKD` | 193 | $6.46 | $2.61 | $-78.52 | $9,398.98 | ▼ -78.52 after sell → book $10,720.23; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 35 | $37.75 | $2.12 | $+6.64 | $10,718.12 | ▲ +6.64 after sell → book $10,718.12; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.12 | ▲ close $10,718.12 vs 09:30 $10,741.00 (session +0.00) | 16:00 close · cash $10,718.12 · no lots left · equity $10,718.12. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.12 | ▲ 09:30 equity $10,718.12 vs yday $10,718.12 (-0.00) | 09:30 open · cash $10,718.12 · no holdings · equity $10,718.12 vs prior close $10,718.12 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.12 | ▲ close $10,718.12 vs 09:30 $10,718.12 (session +0.00) | 16:00 close · cash $10,718.12 · no lots left · equity $10,718.12. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.12 | ▲ 09:30 equity $10,718.12 vs yday $10,718.12 (-0.00) | 09:30 open · cash $10,718.12 · no holdings · equity $10,718.12 vs prior close $10,718.12 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.12 | ▲ close $10,718.12 vs 09:30 $10,718.12 (session +0.00) | 16:00 close · cash $10,718.12 · no lots left · equity $10,718.12. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.12 | ▲ 09:30 equity $10,718.12 vs yday $10,718.12 (-0.00) | 09:30 open · cash $10,718.12 · no holdings · equity $10,718.12 vs prior close $10,718.12 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 428 | $3.13 | $5.52 | — | $9,372.96 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1339.76 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 496 | $2.70 | $6.40 | — | $8,027.36 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1339.76 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 15 | $84.27 | $2.04 | — | $6,761.27 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+12.5; leftover $1339.76 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ANGX` | 249 | $5.38 | $3.21 | — | $5,418.44 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ret5=+19.8; leftover $1339.76 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `TSSI` | 149 | $8.98 | $2.44 | — | $4,077.98 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ret5=+10.0; leftover $1339.76 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `LDI` | 1576 | $0.85 | $18.12 | — | $2,720.26 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ret5=-7.8; leftover $1339.76 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 217 | $6.16 | $2.80 | — | $1,380.74 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ret5=+36.4; leftover $1339.76 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 17 | $77.33 | $2.04 | — | $64.09 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; ret5=+6.2; leftover $1339.76 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.09 | ▲ close $10,874.25 vs 09:30 $10,718.12 (session +198.70) | 16:00 close · cash $64.09 · equity $10,874.25 vs 09:30 $10,718.12 (+156.13; session marks +198.70) · 8 name(s) marked open→close (per-name table). CMRC×428 09:30 $3.13 → close $3.50 +160.50; INDP×496 09:30 $2.70 → close $2.77 +34.72; SWKS×15 09:30 $84.27 → close $88.35 +61.20; ANGX×249 09:30 $5.38 → close $5.45 +17.43; TSSI×149 09:30 $8.98 → close $8.93 -7.45; LDI×1576 09:30 $0.85 → close $0.83 -23.64; IRD×217 09:30 $6.16 → close $6.04 -26.04; VIST×17 09:30 $77.33 → close $76.27 -18.02 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `CHRS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CMRC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BTBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `BIDU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SID` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CMRC` | 428 | 2026-09-11 @ $3.13 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+6.2; leftover $1339.76 |
| `INDP` | 496 | 2026-09-11 @ $2.70 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1339.76 |
| `SWKS` | 15 | 2026-09-11 @ $84.27 | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+12.5; leftover $1339.76 |
| `ANGX` | 249 | 2026-09-11 @ $5.38 | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ret5=+19.8; leftover $1339.76 |
| `TSSI` | 149 | 2026-09-11 @ $8.98 | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ret5=+10.0; leftover $1339.76 |
| `LDI` | 1576 | 2026-09-11 @ $0.85 | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ret5=-7.8; leftover $1339.76 |
| `IRD` | 217 | 2026-09-11 @ $6.16 | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ret5=+36.4; leftover $1339.76 |
| `VIST` | 17 | 2026-09-11 @ $77.33 | combo gate; gate vol=good,last_green=True; list yday_gainer; ret5=+6.2; leftover $1339.76 |
