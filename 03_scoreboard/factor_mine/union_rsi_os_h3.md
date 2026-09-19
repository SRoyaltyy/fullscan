# Factor mine action — `union_rsi_os_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ rsi_os, no 🚨

Cash book **-20.99%** ($7,901) · signal-only (no cash/fees) was -46.46%. Starts YES **5/26**. Fills 60 · skips 104 · realized $-1929.99.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior RSI is oversold (≤30) — Finviz prior export, else computed on prior bars.
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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `rsi_os=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $3.13.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `NCMI` | 929 | — | $2.69 | +0.00 | $2.86 | +157.93 | +157.93 | +0.00 | +157.93 |
| 2026-08-14 | `QMLS` | 342 | — | $7.29 | +0.00 | $7.32 | +10.26 | +10.26 | +0.00 | +10.26 |
| 2026-08-14 | `CLBT` | 230 | — | $10.83 | +0.00 | $11.14 | +71.30 | +71.30 | +0.00 | +71.30 |
| 2026-08-14 | `YSS` | 247 | — | $10.06 | +0.00 | $10.93 | +214.89 | +214.89 | +0.00 | +214.89 |
| 2026-08-17 | `NCMI` | 929 | $2.86 | $2.80 | -55.74 | $2.73 | -65.03 | -120.77 | +102.19 | +37.16 |
| 2026-08-17 | `QMLS` | 342 | $7.32 | $7.24 | -27.36 | $7.14 | -34.20 | -61.56 | -17.10 | -51.30 |
| 2026-08-17 | `CLBT` | 230 | $11.14 | $11.19 | +11.50 | $10.44 | -172.50 | -161.00 | +82.80 | -89.70 |
| 2026-08-17 | `YSS` | 247 | $10.93 | $10.36 | -140.79 | $10.66 | +74.10 | -66.69 | +74.10 | +148.20 |
| 2026-08-18 | `NCMI` | 929 | $2.73 | $2.71 | -18.58 | $2.52 | -176.51 | -195.09 | +18.58 | -157.93 |
| 2026-08-18 | `QMLS` | 342 | $7.14 | $6.85 | -99.18 | $6.74 | -37.62 | -136.80 | -150.48 | -188.10 |
| 2026-08-18 | `CLBT` | 230 | $10.44 | $10.44 | +0.00 | $11.00 | +128.80 | +128.80 | -89.70 | +39.10 |
| 2026-08-18 | `YSS` | 247 | $10.66 | $10.24 | -103.74 | $10.42 | +44.46 | -59.28 | +44.46 | +88.92 |
| 2026-08-19 | `NCMI` | 929 | $2.52 | $2.56 | +37.16 | — | +0.00 | +37.16 | -120.77 | — |
| 2026-08-19 | `QMLS` | 342 | $6.74 | $6.74 | +0.00 | — | +0.00 | +0.00 | -188.10 | — |
| 2026-08-19 | `CLBT` | 230 | $11.00 | $10.85 | -34.50 | $11.24 | +89.70 | +55.20 | +4.60 | +94.30 |
| 2026-08-19 | `YSS` | 247 | $10.42 | $10.32 | -24.70 | — | +0.00 | -24.70 | +64.22 | — |
| 2026-08-20 | `CLBT` | 230 | $11.24 | $11.20 | -9.20 | — | +0.00 | -9.20 | +85.10 | — |
| 2026-08-20 | `LZB` | 145 | — | $33.61 | +0.00 | $33.65 | +5.80 | +5.80 | +0.00 | +5.80 |
| 2026-08-20 | `BHF` | 91 | — | $53.27 | +0.00 | $53.22 | -4.55 | -4.55 | +0.00 | -4.55 |
| 2026-08-21 | `LZB` | 145 | $33.65 | $33.63 | -2.90 | $33.71 | +11.60 | +8.70 | +2.90 | +14.50 |
| 2026-08-21 | `BHF` | 91 | $53.22 | $53.10 | -10.92 | $52.27 | -75.53 | -86.45 | -15.47 | -91.00 |
| 2026-08-21 | `WB` | 1 | — | $7.13 | +0.00 | $7.03 | -0.10 | -0.10 | +0.00 | -0.10 |
| 2026-08-24 | `LZB` | 145 | $33.71 | $33.55 | -23.20 | $32.12 | -207.35 | -230.55 | -8.70 | -216.05 |
| 2026-08-24 | `BHF` | 91 | $52.27 | $52.83 | +50.96 | $51.90 | -84.63 | -33.67 | -40.04 | -124.67 |
| 2026-08-24 | `WB` | 1 | $7.03 | $6.98 | -0.05 | $7.00 | +0.02 | -0.03 | -0.15 | -0.13 |
| 2026-08-25 | `LZB` | 145 | $32.12 | $32.33 | +30.45 | — | +0.00 | +30.45 | -185.60 | — |
| 2026-08-25 | `BHF` | 91 | $51.90 | $52.17 | +24.57 | — | +0.00 | +24.57 | -100.10 | — |
| 2026-08-25 | `WB` | 1 | $7.00 | $7.00 | +0.00 | $7.12 | +0.12 | +0.12 | -0.13 | -0.01 |
| 2026-08-25 | `QFIN` | 427 | — | $11.09 | +0.00 | $11.53 | +187.88 | +187.88 | +0.00 | +187.88 |
| 2026-08-25 | `CRI` | 138 | — | $34.24 | +0.00 | $33.91 | -45.54 | -45.54 | +0.00 | -45.54 |
| 2026-08-26 | `WB` | 1 | $7.12 | $7.10 | -0.02 | $7.04 | -0.06 | -0.08 | -0.03 | -0.09 |
| 2026-08-26 | `QFIN` | 427 | $11.53 | $9.76 | -755.79 | $9.35 | -175.07 | -930.86 | -567.91 | -742.98 |
| 2026-08-26 | `CRI` | 138 | $33.91 | $33.68 | -31.74 | $34.28 | +82.80 | +51.06 | -77.28 | +5.52 |
| 2026-08-26 | `QMLS` | 1 | — | $6.47 | +0.00 | $6.10 | -0.37 | -0.37 | +0.00 | -0.37 |
| 2026-08-27 | `WB` | 1 | $7.04 | $7.04 | +0.00 | — | +0.00 | +0.00 | -0.09 | — |
| 2026-08-27 | `QFIN` | 427 | $9.35 | $9.42 | +29.89 | $9.17 | -106.75 | -76.86 | -713.09 | -819.84 |
| 2026-08-27 | `CRI` | 138 | $34.28 | $33.92 | -49.68 | $33.34 | -80.04 | -129.72 | -44.16 | -124.20 |
| 2026-08-27 | `QMLS` | 1 | $6.10 | $6.33 | +0.23 | $6.47 | +0.14 | +0.37 | -0.14 | +0.00 |
| 2026-08-28 | `QFIN` | 427 | $9.17 | $9.15 | -8.54 | $8.80 | -149.45 | -157.99 | -828.38 | -977.83 |
| 2026-08-28 | `CRI` | 138 | $33.34 | $33.68 | +46.92 | — | +0.00 | +46.92 | -77.28 | — |
| 2026-08-28 | `QMLS` | 1 | $6.47 | $6.27 | -0.20 | $6.11 | -0.16 | -0.36 | -0.20 | -0.36 |
| 2026-08-28 | `DY` | 7 | — | $306.34 | +0.00 | $294.34 | -84.00 | -84.00 | +0.00 | -84.00 |
| 2026-08-28 | `BURL` | 8 | — | $291.30 | +0.00 | $272.95 | -146.80 | -146.80 | +0.00 | -146.80 |
| 2026-08-31 | `QFIN` | 427 | $8.80 | $8.70 | -42.70 | — | +0.00 | -42.70 | -1020.53 | — |
| 2026-08-31 | `QMLS` | 1 | $6.11 | $5.95 | -0.16 | — | +0.00 | -0.16 | -0.52 | — |
| 2026-08-31 | `DY` | 7 | $294.34 | $298.01 | +25.69 | $291.21 | -47.60 | -21.91 | -58.31 | -105.91 |
| 2026-08-31 | `BURL` | 8 | $272.95 | $270.50 | -19.60 | $259.76 | -85.92 | -105.52 | -166.40 | -252.32 |
| 2026-09-01 | `DY` | 7 | $291.21 | $289.16 | -14.35 | $287.30 | -13.02 | -27.37 | -120.26 | -133.28 |
| 2026-09-01 | `BURL` | 8 | $259.76 | $256.00 | -30.08 | $262.15 | +49.20 | +19.12 | -282.40 | -233.20 |
| 2026-09-02 | `DY` | 7 | $287.30 | $287.99 | +4.83 | — | +0.00 | +4.83 | -128.45 | — |
| 2026-09-02 | `BURL` | 8 | $262.15 | $262.01 | -1.12 | — | +0.00 | -1.12 | -234.32 | — |
| 2026-09-03 | `SION` | 156 | — | $7.31 | +0.00 | $6.75 | -87.36 | -87.36 | +0.00 | -87.36 |
| 2026-09-03 | `ALMS` | 110 | — | $10.38 | +0.00 | $11.36 | +108.35 | +108.35 | +0.00 | +108.35 |
| 2026-09-03 | `LX` | 1384 | — | $0.83 | +0.00 | $0.85 | +34.60 | +34.60 | +0.00 | +34.60 |
| 2026-09-03 | `EVTL` | 1788 | — | $0.64 | +0.00 | $0.60 | -71.52 | -71.52 | +0.00 | -71.52 |
| 2026-09-03 | `FJET` | 403 | — | $2.84 | +0.00 | $2.78 | -24.18 | -24.18 | +0.00 | -24.18 |
| 2026-09-03 | `OSW` | 52 | — | $22.00 | +0.00 | $22.27 | +14.04 | +14.04 | +0.00 | +14.04 |
| 2026-09-03 | `PCG` | 83 | — | $13.35 | +0.00 | $13.96 | +50.63 | +50.63 | +0.00 | +50.63 |
| 2026-09-04 | `SION` | 156 | $6.75 | $6.68 | -10.92 | $7.18 | +78.00 | +67.08 | -98.28 | -20.28 |
| 2026-09-04 | `ALMS` | 110 | $11.36 | $11.23 | -14.30 | $11.10 | -14.30 | -28.60 | +94.05 | +79.75 |
| 2026-09-04 | `LX` | 1384 | $0.85 | $0.86 | +8.30 | $0.88 | +33.22 | +41.52 | +42.90 | +76.12 |
| 2026-09-04 | `EVTL` | 1788 | $0.60 | $0.60 | +0.00 | $0.60 | -3.58 | -3.58 | -71.52 | -75.10 |
| 2026-09-04 | `FJET` | 403 | $2.78 | $2.80 | +8.06 | $2.80 | +0.00 | +8.06 | -16.12 | -16.12 |
| 2026-09-04 | `OSW` | 52 | $22.27 | $22.27 | +0.00 | $22.40 | +6.76 | +6.76 | +14.04 | +20.80 |
| 2026-09-04 | `PCG` | 83 | $13.96 | $13.80 | -13.28 | $14.30 | +41.50 | +28.22 | +37.35 | +78.85 |
| 2026-09-08 | `SION` | 156 | $7.18 | $7.13 | -7.80 | $7.30 | +26.52 | +18.72 | -28.08 | -1.56 |
| 2026-09-08 | `ALMS` | 110 | $11.10 | $11.05 | -5.50 | $10.58 | -51.70 | -57.20 | +74.25 | +22.55 |
| 2026-09-08 | `LX` | 1384 | $0.88 | $0.90 | +24.91 | $0.85 | -65.05 | -40.14 | +101.03 | +35.98 |
| 2026-09-08 | `EVTL` | 1788 | $0.60 | $0.60 | +0.00 | $0.59 | -14.30 | -14.30 | -75.10 | -89.40 |
| 2026-09-08 | `FJET` | 403 | $2.80 | $2.80 | +0.00 | $2.63 | -68.51 | -68.51 | -16.12 | -84.63 |
| 2026-09-08 | `OSW` | 52 | $22.40 | $22.29 | -5.72 | $22.12 | -8.84 | -14.56 | +15.08 | +6.24 |
| 2026-09-08 | `PCG` | 83 | $14.30 | $14.26 | -3.32 | $14.82 | +46.48 | +43.16 | +75.53 | +122.01 |
| 2026-09-09 | `SION` | 156 | $7.30 | $7.27 | -4.68 | — | +0.00 | -4.68 | -6.24 | — |
| 2026-09-09 | `ALMS` | 110 | $10.58 | $10.49 | -9.90 | — | +0.00 | -9.90 | +12.65 | — |
| 2026-09-09 | `LX` | 1384 | $0.85 | $0.83 | -24.91 | — | +0.00 | -24.91 | +11.07 | — |
| 2026-09-09 | `EVTL` | 1788 | $0.59 | $0.59 | +7.15 | — | +0.00 | +7.15 | -82.25 | — |
| 2026-09-09 | `FJET` | 403 | $2.63 | $2.63 | +0.00 | — | +0.00 | +0.00 | -84.63 | — |
| 2026-09-09 | `OSW` | 52 | $22.12 | $21.86 | -13.52 | — | +0.00 | -13.52 | -7.28 | — |
| 2026-09-09 | `PCG` | 83 | $14.82 | $14.83 | +0.83 | — | +0.00 | +0.83 | +122.84 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `NAVN` | 54 | — | $20.61 | +0.00 | $21.02 | +22.14 | +22.14 | +0.00 | +22.14 |
| 2026-09-11 | `RWT` | 320 | — | $3.52 | +0.00 | $3.55 | +9.60 | +9.60 | +0.00 | +9.60 |
| 2026-09-11 | `COO` | 20 | — | $54.66 | +0.00 | $53.91 | -15.00 | -15.00 | +0.00 | -15.00 |
| 2026-09-11 | `TTAN` | 20 | — | $55.91 | +0.00 | $54.68 | -24.60 | -24.60 | +0.00 | -24.60 |
| 2026-09-11 | `ENB` | 23 | — | $48.37 | +0.00 | $47.76 | -14.03 | -14.03 | +0.00 | -14.03 |
| 2026-09-11 | `WAFD` | 32 | — | $34.44 | +0.00 | $33.88 | -17.92 | -17.92 | +0.00 | -17.92 |
| 2026-09-11 | `KMB` | 11 | — | $99.24 | +0.00 | $98.15 | -11.99 | -11.99 | +0.00 | -11.99 |
| 2026-09-14 | `NAVN` | 54 | $21.02 | $21.10 | +4.32 | $21.37 | +14.58 | +18.90 | +26.46 | +41.04 |
| 2026-09-14 | `RWT` | 320 | $3.55 | $3.53 | -6.40 | $3.83 | +96.00 | +89.60 | +3.20 | +99.20 |
| 2026-09-14 | `COO` | 20 | $53.91 | $54.78 | +17.40 | $54.22 | -11.20 | +6.20 | +2.40 | -8.80 |
| 2026-09-14 | `TTAN` | 20 | $54.68 | $54.77 | +1.80 | $58.99 | +84.40 | +86.20 | -22.80 | +61.60 |
| 2026-09-14 | `ENB` | 23 | $47.76 | $47.85 | +2.07 | $48.16 | +7.13 | +9.20 | -11.96 | -4.83 |
| 2026-09-14 | `WAFD` | 32 | $33.88 | $33.88 | +0.00 | $33.61 | -8.64 | -8.64 | -17.92 | -26.56 |
| 2026-09-14 | `KMB` | 11 | $98.15 | $99.18 | +11.33 | $99.09 | -0.99 | +10.34 | -0.66 | -1.65 |
| 2026-09-15 | `NAVN` | 54 | $21.37 | $21.32 | -2.70 | $22.42 | +59.40 | +56.70 | +38.34 | +97.74 |
| 2026-09-15 | `RWT` | 320 | $3.83 | $3.80 | -9.60 | $3.91 | +35.20 | +25.60 | +89.60 | +124.80 |
| 2026-09-15 | `COO` | 20 | $54.22 | $54.40 | +3.60 | $53.27 | -22.60 | -19.00 | -5.20 | -27.80 |
| 2026-09-15 | `TTAN` | 20 | $58.99 | $57.78 | -24.20 | $58.60 | +16.40 | -7.80 | +37.40 | +53.80 |
| 2026-09-15 | `ENB` | 23 | $48.16 | $48.58 | +9.66 | $48.36 | -5.06 | +4.60 | +4.83 | -0.23 |
| 2026-09-15 | `WAFD` | 32 | $33.61 | $33.59 | -0.64 | $33.01 | -18.56 | -19.20 | -27.20 | -45.76 |
| 2026-09-15 | `KMB` | 11 | $99.09 | $98.35 | -8.14 | $98.68 | +3.63 | -4.51 | -9.79 | -6.16 |
| 2026-09-16 | `NAVN` | 54 | $22.42 | $22.50 | +4.32 | — | +0.00 | +4.32 | +102.06 | — |
| 2026-09-16 | `RWT` | 320 | $3.91 | $3.98 | +22.40 | — | +0.00 | +22.40 | +147.20 | — |
| 2026-09-16 | `COO` | 20 | $53.27 | $54.37 | +22.00 | — | +0.00 | +22.00 | -5.80 | — |
| 2026-09-16 | `TTAN` | 20 | $58.60 | $57.04 | -31.20 | — | +0.00 | -31.20 | +22.60 | — |
| 2026-09-16 | `ENB` | 23 | $48.36 | $48.36 | +0.00 | — | +0.00 | +0.00 | -0.23 | — |
| 2026-09-16 | `WAFD` | 32 | $33.01 | $33.09 | +2.56 | — | +0.00 | +2.56 | -43.20 | — |
| 2026-09-16 | `KMB` | 11 | $98.68 | $98.68 | +0.00 | — | +0.00 | +0.00 | -6.16 | — |
| 2026-09-16 | `ALHC` | 97 | — | $10.30 | +0.00 | $8.71 | -154.23 | -154.23 | +0.00 | -154.23 |
| 2026-09-16 | `PLAY` | 147 | — | $6.86 | +0.00 | $6.86 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-16 | `DVLT` | 6304 | — | $0.16 | +0.00 | $0.18 | +126.08 | +126.08 | +0.00 | +126.08 |
| 2026-09-16 | `NMRA` | 1195 | — | $0.84 | +0.00 | $0.77 | -84.84 | -84.84 | +0.00 | -84.84 |
| 2026-09-16 | `ZSQR` | 431 | — | $2.34 | +0.00 | $2.29 | -21.55 | -21.55 | +0.00 | -21.55 |
| 2026-09-16 | `CTMX` | 370 | — | $2.72 | +0.00 | $2.77 | +16.65 | +16.65 | +0.00 | +16.65 |
| 2026-09-16 | `CRBP` | 147 | — | $6.86 | +0.00 | $7.26 | +58.80 | +58.80 | +0.00 | +58.80 |
| 2026-09-16 | `EYPT` | 261 | — | $3.66 | +0.00 | $3.45 | -54.81 | -54.81 | +0.00 | -54.81 |
| 2026-09-17 | `ALHC` | 97 | $8.71 | $8.58 | -12.61 | $8.70 | +11.64 | -0.97 | -166.84 | -155.20 |
| 2026-09-17 | `PLAY` | 147 | $6.86 | $6.96 | +14.70 | $6.54 | -61.74 | -47.04 | +14.70 | -47.04 |
| 2026-09-17 | `DVLT` | 6304 | $0.18 | $0.17 | -63.04 | $0.16 | -63.04 | -126.08 | +63.04 | +0.00 |
| 2026-09-17 | `NMRA` | 1195 | $0.77 | $0.78 | +8.37 | $0.75 | -35.85 | -27.48 | -76.48 | -112.33 |
| 2026-09-17 | `ZSQR` | 431 | $2.29 | $2.35 | +25.86 | $2.54 | +81.89 | +107.75 | +4.31 | +86.20 |
| 2026-09-17 | `CTMX` | 370 | $2.77 | $2.83 | +24.05 | $2.82 | -3.70 | +20.35 | +40.70 | +37.00 |
| 2026-09-17 | `CRBP` | 147 | $7.26 | $7.26 | +0.00 | $7.26 | +0.00 | +0.00 | +58.80 | +58.80 |
| 2026-09-17 | `EYPT` | 261 | $3.45 | $3.57 | +31.32 | $3.99 | +109.62 | +140.94 | -23.49 | +86.13 |
| 2026-09-18 | `ALHC` | 97 | $8.70 | $8.68 | -1.94 | $8.35 | -32.01 | -33.95 | -157.14 | -189.15 |
| 2026-09-18 | `PLAY` | 147 | $6.54 | $6.64 | +14.70 | $6.73 | +13.23 | +27.93 | -32.34 | -19.11 |
| 2026-09-18 | `DVLT` | 6304 | $0.16 | $0.17 | +63.04 | $0.15 | -126.08 | -63.04 | +63.04 | -63.04 |
| 2026-09-18 | `NMRA` | 1195 | $0.75 | $0.74 | -10.76 | $0.73 | -13.15 | -23.91 | -123.08 | -136.23 |
| 2026-09-18 | `ZSQR` | 431 | $2.54 | $2.50 | -17.24 | $2.68 | +77.58 | +60.34 | +68.96 | +146.54 |
| 2026-09-18 | `CTMX` | 370 | $2.82 | $2.83 | +3.70 | $2.76 | -25.90 | -22.20 | +40.70 | +14.80 |
| 2026-09-18 | `CRBP` | 147 | $7.26 | $7.22 | -5.88 | $7.48 | +38.22 | +32.34 | +52.92 | +91.14 |
| 2026-09-18 | `EYPT` | 261 | $3.99 | $3.95 | -10.44 | $3.85 | -26.10 | -36.54 | +75.69 | +49.59 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +454.38 | NCMI, QMLS, CLBT, YSS | — | $9.54 | $10,431.83 | NCMI×929, QMLS×342, CLBT×230, YSS×247 |
| 2026-08-17 | +2.25 | $9.54 | NCMI×929, QMLS×342, CLBT×230, YSS×247 | $10,219.44 | -212.39 | -197.63 | — | — | $9.54 | $10,021.81 | NCMI×929, QMLS×342, CLBT×230, YSS×247 |
| 2026-08-18 | -6.20 | $9.54 | NCMI×929, QMLS×342, CLBT×230, YSS×247 | $9,800.31 | -221.50 | -40.87 | — | — | $9.54 | $9,759.44 | NCMI×929, QMLS×342, CLBT×230, YSS×247 |
| 2026-08-19 | -7.20 | $9.54 | NCMI×929, QMLS×342, CLBT×230, YSS×247 | $9,737.40 | -22.04 | +89.70 | — | NCMI, QMLS, YSS | $7,222.01 | $9,807.21 | CLBT×230 |
| 2026-08-20 | +1.12 | $7,222.01 | CLBT×230 | $9,798.01 | -9.20 | +1.25 | LZB, BHF | CLBT | $69.27 | $9,791.55 | LZB×145, BHF×91 |
| 2026-08-21 | +3.25 | $69.27 | LZB×145, BHF×91 | $9,777.73 | -13.82 | -64.03 | WB | — | $62.07 | $9,713.62 | LZB×145, BHF×91, WB×1 |
| 2026-08-24 | -5.17 | $62.07 | LZB×145, BHF×91, WB×1 | $9,741.33 | +27.71 | -291.96 | — | — | $62.07 | $9,449.37 | LZB×145, BHF×91, WB×1 |
| 2026-08-25 | +1.80 | $62.07 | LZB×145, BHF×91, WB×1 | $9,504.39 | +55.02 | +142.46 | QFIN, CRI | LZB, BHF | $24.13 | $9,634.14 | WB×1, QFIN×427, CRI×138 |
| 2026-08-26 | +2.02 | $24.13 | WB×1, QFIN×427, CRI×138 | $8,846.59 | -787.55 | -92.70 | QMLS | — | $17.59 | $8,753.82 | WB×1, QFIN×427, CRI×138, QMLS×1 |
| 2026-08-27 | — | $17.59 | WB×1, QFIN×427, CRI×138, QMLS×1 | $8,734.26 | -19.56 | -186.65 | — | WB | $24.53 | $8,547.51 | QFIN×427, CRI×138, QMLS×1 |
| 2026-08-28 | +0.75 | $24.53 | QFIN×427, CRI×138, QMLS×1 | $8,585.69 | +38.18 | -380.41 | DY, BURL | CRI | $191.11 | $8,198.80 | QFIN×427, QMLS×1, DY×7, BURL×8 |
| 2026-08-31 | -5.85 | $191.11 | QFIN×427, QMLS×1, DY×7, BURL×8 | $8,162.03 | -36.77 | -133.52 | — | QFIN, QMLS | $3,906.26 | $8,022.81 | DY×7, BURL×8 |
| 2026-09-01 | -6.30 | $3,906.26 | DY×7, BURL×8 | $7,978.38 | -44.43 | +36.18 | — | — | $3,906.26 | $8,014.56 | DY×7, BURL×8 |
| 2026-09-02 | -3.83 | $3,906.26 | DY×7, BURL×8 | $8,018.27 | +3.71 | +0.00 | — | DY, BURL | $8,014.20 | $8,014.20 | — |
| 2026-09-03 | -0.90 | $8,014.20 | — | $8,014.20 | -0.00 | +24.56 | SION, ALMS, LX, EVTL, FJET, OSW, PCG | — | $0.36 | $7,991.99 | SION×156, ALMS×110, LX×1384, EVTL×1788, FJET×403, OSW×52, PCG×83 |
| 2026-09-04 | +2.25 | $0.36 | SION×156, ALMS×110, LX×1384, EVTL×1788, FJET×403, OSW×52, PCG×83 | $7,969.85 | -22.14 | +141.60 | — | — | $0.36 | $8,111.45 | SION×156, ALMS×110, LX×1384, EVTL×1788, FJET×403, OSW×52, PCG×83 |
| 2026-09-08 | -11.47 | $0.36 | SION×156, ALMS×110, LX×1384, EVTL×1788, FJET×403, OSW×52, PCG×83 | $8,114.03 | +2.58 | -135.40 | — | — | $0.36 | $7,978.62 | SION×156, ALMS×110, LX×1384, EVTL×1788, FJET×403, OSW×52, PCG×83 |
| 2026-09-09 | -13.95 | $0.36 | SION×156, ALMS×110, LX×1384, EVTL×1788, FJET×403, OSW×52, PCG×83 | $7,933.59 | -45.03 | +0.00 | — | SION, ALMS, LX, EVTL, FJET, OSW, PCG | $7,886.81 | $7,886.81 | — |
| 2026-09-10 | -13.28 | $7,886.81 | — | $7,886.81 | -0.00 | +0.00 | — | — | $7,886.81 | $7,886.81 | — |
| 2026-09-11 | +0.50 | $7,886.81 | — | $7,886.81 | -0.00 | -51.80 | NAVN, RWT, COO, TTAN, ENB, WAFD, KMB | — | $113.29 | $7,818.46 | NAVN×54, RWT×320, COO×20, TTAN×20, ENB×23, WAFD×32, KMB×11 |
| 2026-09-14 | -11.00 | $113.29 | NAVN×54, RWT×320, COO×20, TTAN×20, ENB×23, WAFD×32, KMB×11 | $7,848.98 | +30.52 | +181.28 | — | — | $113.29 | $8,030.26 | NAVN×54, RWT×320, COO×20, TTAN×20, ENB×23, WAFD×32, KMB×11 |
| 2026-09-15 | -3.84 | $113.29 | NAVN×54, RWT×320, COO×20, TTAN×20, ENB×23, WAFD×32, KMB×11 | $7,998.24 | -32.02 | +68.41 | — | — | $113.29 | $8,066.65 | NAVN×54, RWT×320, COO×20, TTAN×20, ENB×23, WAFD×32, KMB×11 |
| 2026-09-16 | +5.30 | $113.29 | NAVN×54, RWT×320, COO×20, TTAN×20, ENB×23, WAFD×32, KMB×11 | $8,086.73 | +20.08 | -113.90 | ALHC, PLAY, DVLT, NMRA, ZSQR, CTMX, CRBP, EYPT | NAVN, RWT, COO, TTAN, ENB, WAFD, KMB | $3.13 | $7,892.58 | ALHC×97, PLAY×147, DVLT×6304, NMRA×1195, ZSQR×431, CTMX×370, CRBP×147, EYPT×261 |
| 2026-09-17 | +7.38 | $3.13 | ALHC×97, PLAY×147, DVLT×6304, NMRA×1195, ZSQR×431, CTMX×370, CRBP×147, EYPT×261 | $7,921.23 | +28.65 | +38.82 | — | — | $3.13 | $7,960.05 | ALHC×97, PLAY×147, DVLT×6304, NMRA×1195, ZSQR×431, CTMX×370, CRBP×147, EYPT×261 |
| 2026-09-18 | +4.86 | $3.13 | ALHC×97, PLAY×147, DVLT×6304, NMRA×1195, ZSQR×431, CTMX×370, CRBP×147, EYPT×261 | $7,995.23 | +35.18 | -94.21 | — | — | $3.13 | $7,901.03 | ALHC×97, PLAY×147, DVLT×6304, NMRA×1195, ZSQR×431, CTMX×370, CRBP×147, EYPT×261 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 929 | $2.69 | $11.98 | — | $7,489.01 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 342 | $7.29 | $4.41 | — | $4,991.41 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 230 | $10.83 | $2.97 | — | $2,497.55 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; 🔵; ⚪; ret5=-30.1; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `YSS` | 247 | $10.06 | $3.19 | — | $9.54 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; 🔵; ⚪; ret5=+5.7; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.54 | ▲ close $10,431.83 vs 09:30 $10,000.00 (session +454.38) | 16:00 close · cash $9.54 · equity $10,431.83 vs 09:30 $10,000.00 (+431.83; session marks +454.38) · 4 name(s) marked open→close (per-name table). NCMI×929 09:30 $2.69 → close $2.86 +157.93; QMLS×342 09:30 $7.29 → close $7.32 +10.26; CLBT×230 09:30 $10.83 → close $11.14 +71.30; YSS×247 09:30 $10.06 → close $10.93 +214.89 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.54 | ▼ 09:30 equity $10,219.44 vs yday $10,431.83 (-212.39) | 09:30 open · cash $9.54 (unchanged overnight, no fees) · equity $10,219.44 vs prior close $10,431.83 (-212.39) · 4 name(s) re-marked at the open (per-name table). NCMI×929 yday $2.86 → 09:30 $2.80 -55.74; QMLS×342 yday $7.32 → 09:30 $7.24 -27.36; CLBT×230 yday $11.14 → 09:30 $11.19 +11.50; YSS×247 yday $10.93 → 09:30 $10.36 -140.79 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.54 | ▼ close $10,021.81 vs 09:30 $10,219.44 (session -197.63) | 16:00 close · cash $9.54 · equity $10,021.81 vs 09:30 $10,219.44 (-197.63; session marks -197.63) · 4 name(s) marked open→close (per-name table). NCMI×929 09:30 $2.80 → close $2.73 -65.03; QMLS×342 09:30 $7.24 → close $7.14 -34.20; CLBT×230 09:30 $11.19 → close $10.44 -172.50; YSS×247 09:30 $10.36 → close $10.66 +74.10 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.54 | ▼ 09:30 equity $9,800.31 vs yday $10,021.81 (-221.50) | 09:30 open · cash $9.54 (unchanged overnight, no fees) · equity $9,800.31 vs prior close $10,021.81 (-221.50) · 4 name(s) re-marked at the open (per-name table). NCMI×929 yday $2.73 → 09:30 $2.71 -18.58; QMLS×342 yday $7.14 → 09:30 $6.85 -99.18; CLBT×230 yday $10.44 → 09:30 $10.44 +0.00; YSS×247 yday $10.66 → 09:30 $10.24 -103.74 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.54 | ▼ close $9,759.44 vs 09:30 $9,800.31 (session -40.87) | 16:00 close · cash $9.54 · equity $9,759.44 vs 09:30 $9,800.31 (-40.87; session marks -40.87) · 4 name(s) marked open→close (per-name table). NCMI×929 09:30 $2.71 → close $2.52 -176.51; QMLS×342 09:30 $6.85 → close $6.74 -37.62; CLBT×230 09:30 $10.44 → close $11.00 +128.80; YSS×247 09:30 $10.24 → close $10.42 +44.46 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.54 | ▼ 09:30 equity $9,737.40 vs yday $9,759.44 (-22.04) | 09:30 open · cash $9.54 (unchanged overnight, no fees) · equity $9,737.40 vs prior close $9,759.44 (-22.04) · 4 name(s) re-marked at the open (per-name table). NCMI×929 yday $2.52 → 09:30 $2.56 +37.16; QMLS×342 yday $6.74 → 09:30 $6.74 +0.00; CLBT×230 yday $11.00 → 09:30 $10.85 -34.50; YSS×247 yday $10.42 → 09:30 $10.32 -24.70 | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 929 | $2.56 | $12.16 | $-144.91 | $2,375.62 | ▼ -144.91 after sell → book $9,725.24; vs 09:30 mark -12.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `QMLS` | 342 | $6.74 | $4.49 | $-197.00 | $4,676.22 | ▼ -197.00 after sell → book $9,720.76; vs 09:30 mark -4.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `YSS` | 247 | $10.32 | $3.25 | $+57.79 | $7,222.01 | ▲ +57.79 after sell → book $9,717.51; vs 09:30 mark -3.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,222.01 | ▲ close $9,807.21 vs 09:30 $9,737.40 (session +89.70) | 16:00 close · cash $7,222.01 · equity $9,807.21 vs 09:30 $9,737.40 (+69.81; session marks +89.70) · 1 name(s) marked open→close (per-name table). CLBT×230 09:30 $10.85 → close $11.24 +89.70 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,222.01 | ▼ 09:30 equity $9,798.01 vs yday $9,807.21 (-9.20) | 09:30 open · cash $7,222.01 (unchanged overnight, no fees) · equity $9,798.01 vs prior close $9,807.21 (-9.20) · 1 name(s) re-marked at the open (per-name table). CLBT×230 yday $11.24 → 09:30 $11.20 -9.20 | — |
| 2026-08-20 09:30 ET | **SELL** | `CLBT` | 230 | $11.20 | $3.03 | $+79.11 | $9,794.98 | ▲ +79.11 after sell → book $9,794.98; vs 09:30 mark -3.03 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `LZB` | 145 | $33.61 | $2.42 | — | $4,919.11 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; 🔵; ret5=-17.4; leftover $4897.49 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHF` | 91 | $53.27 | $2.26 | — | $69.27 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-10.9; leftover $4897.49 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.27 | ▲ close $9,791.55 vs 09:30 $9,798.01 (session +1.25) | 16:00 close · cash $69.27 · equity $9,791.55 vs 09:30 $9,798.01 (-6.47; session marks +1.25) · 2 name(s) marked open→close (per-name table). LZB×145 09:30 $33.61 → close $33.65 +5.80; BHF×91 09:30 $53.27 → close $53.22 -4.55 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.27 | ▼ 09:30 equity $9,777.73 vs yday $9,791.55 (-13.82) | 09:30 open · cash $69.27 (unchanged overnight, no fees) · equity $9,777.73 vs prior close $9,791.55 (-13.82) · 2 name(s) re-marked at the open (per-name table). LZB×145 yday $33.65 → 09:30 $33.63 -2.90; BHF×91 yday $53.22 → 09:30 $53.10 -10.92 | — |
| 2026-08-21 09:30 ET | **BUY** | `WB` | 1 | $7.13 | $0.07 | — | $62.07 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; ret5=-5.9; leftover $9.90 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.07 | ▼ close $9,713.62 vs 09:30 $9,777.73 (session -64.03) | 16:00 close · cash $62.07 · equity $9,713.62 vs 09:30 $9,777.73 (-64.11; session marks -64.03) · 3 name(s) marked open→close (per-name table). LZB×145 09:30 $33.63 → close $33.71 +11.60; BHF×91 09:30 $53.10 → close $52.27 -75.53; WB×1 09:30 $7.13 → close $7.03 -0.10 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.07 | ▲ 09:30 equity $9,741.33 vs yday $9,713.62 (+27.71) | 09:30 open · cash $62.07 (unchanged overnight, no fees) · equity $9,741.33 vs prior close $9,713.62 (+27.71) · 3 name(s) re-marked at the open (per-name table). LZB×145 yday $33.71 → 09:30 $33.55 -23.20; BHF×91 yday $52.27 → 09:30 $52.83 +50.96; WB×1 yday $7.03 → 09:30 $6.98 -0.05 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.07 | ▼ close $9,449.37 vs 09:30 $9,741.33 (session -291.96) | 16:00 close · cash $62.07 · equity $9,449.37 vs 09:30 $9,741.33 (-291.96; session marks -291.96) · 3 name(s) marked open→close (per-name table). LZB×145 09:30 $33.55 → close $32.12 -207.35; BHF×91 09:30 $52.83 → close $51.90 -84.63; WB×1 09:30 $6.98 → close $7.00 +0.02 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.07 | ▲ 09:30 equity $9,504.39 vs yday $9,449.37 (+55.02) | 09:30 open · cash $62.07 (unchanged overnight, no fees) · equity $9,504.39 vs prior close $9,449.37 (+55.02) · 3 name(s) re-marked at the open (per-name table). LZB×145 yday $32.12 → 09:30 $32.33 +30.45; BHF×91 yday $51.90 → 09:30 $52.17 +24.57; WB×1 yday $7.00 → 09:30 $7.00 +0.00 | — |
| 2026-08-25 09:30 ET | **SELL** | `LZB` | 145 | $32.33 | $2.49 | $-190.51 | $4,747.43 | ▼ -190.51 after sell → book $9,501.90; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHF` | 91 | $52.17 | $2.32 | $-104.68 | $9,492.59 | ▼ -104.68 after sell → book $9,499.59; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `QFIN` | 427 | $11.09 | $5.51 | — | $4,751.65 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-8.0; leftover $4746.29 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CRI` | 138 | $34.24 | $2.40 | — | $24.13 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-9.2; leftover $4746.29 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.13 | ▲ close $9,634.14 vs 09:30 $9,504.39 (session +142.46) | 16:00 close · cash $24.13 · equity $9,634.14 vs 09:30 $9,504.39 (+129.75; session marks +142.46) · 3 name(s) marked open→close (per-name table). WB×1 09:30 $7.00 → close $7.12 +0.12; QFIN×427 09:30 $11.09 → close $11.53 +187.88; CRI×138 09:30 $34.24 → close $33.91 -45.54 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.13 | ▼ 09:30 equity $8,846.59 vs yday $9,634.14 (-787.55) | 09:30 open · cash $24.13 (unchanged overnight, no fees) · equity $8,846.59 vs prior close $9,634.14 (-787.55) · 3 name(s) re-marked at the open (per-name table). WB×1 yday $7.12 → 09:30 $7.10 -0.02; QFIN×427 yday $11.53 → 09:30 $9.76 -755.79; CRI×138 yday $33.91 → 09:30 $33.68 -31.74 | — |
| 2026-08-26 09:30 ET | **BUY** | `QMLS` | 1 | $6.47 | $0.07 | — | $17.59 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; 🔵; ret5=-7.0; leftover $12.06 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.59 | ▼ close $8,753.82 vs 09:30 $8,846.59 (session -92.70) | 16:00 close · cash $17.59 · equity $8,753.82 vs 09:30 $8,846.59 (-92.77; session marks -92.70) · 4 name(s) marked open→close (per-name table). WB×1 09:30 $7.10 → close $7.04 -0.06; QFIN×427 09:30 $9.76 → close $9.35 -175.07; CRI×138 09:30 $33.68 → close $34.28 +82.80; QMLS×1 09:30 $6.47 → close $6.10 -0.37 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.59 | ▼ 09:30 equity $8,734.26 vs yday $8,753.82 (-19.56) | 09:30 open · cash $17.59 (unchanged overnight, no fees) · equity $8,734.26 vs prior close $8,753.82 (-19.56) · 4 name(s) re-marked at the open (per-name table). WB×1 yday $7.04 → 09:30 $7.04 +0.00; QFIN×427 yday $9.35 → 09:30 $9.42 +29.89; CRI×138 yday $34.28 → 09:30 $33.92 -49.68; QMLS×1 yday $6.10 → 09:30 $6.33 +0.23 | — |
| 2026-08-27 09:30 ET | **SELL** | `WB` | 1 | $7.04 | $0.09 | $-0.26 | $24.53 | ▼ -0.26 after sell → book $8,734.16; vs 09:30 mark -0.10 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.53 | ▼ close $8,547.51 vs 09:30 $8,734.26 (session -186.65) | 16:00 close · cash $24.53 · equity $8,547.51 vs 09:30 $8,734.26 (-186.75; session marks -186.65) · 3 name(s) marked open→close (per-name table). QFIN×427 09:30 $9.42 → close $9.17 -106.75; CRI×138 09:30 $33.92 → close $33.34 -80.04; QMLS×1 09:30 $6.33 → close $6.47 +0.14 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.53 | ▲ 09:30 equity $8,585.69 vs yday $8,547.51 (+38.18) | 09:30 open · cash $24.53 (unchanged overnight, no fees) · equity $8,585.69 vs prior close $8,547.51 (+38.18) · 3 name(s) re-marked at the open (per-name table). QFIN×427 yday $9.17 → 09:30 $9.15 -8.54; CRI×138 yday $33.34 → 09:30 $33.68 +46.92; QMLS×1 yday $6.47 → 09:30 $6.27 -0.20 | — |
| 2026-08-28 09:30 ET | **SELL** | `CRI` | 138 | $33.68 | $2.46 | $-82.15 | $4,669.91 | ▼ -82.15 after sell → book $8,583.23; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 7 | $306.34 | $2.01 | — | $2,523.52 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; ret5=-23.0; leftover $2334.96 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BURL` | 8 | $291.30 | $2.01 | — | $191.11 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-13.0; leftover $2334.96 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.11 | ▼ close $8,198.80 vs 09:30 $8,585.69 (session -380.41) | 16:00 close · cash $191.11 · equity $8,198.80 vs 09:30 $8,585.69 (-386.89; session marks -380.41) · 4 name(s) marked open→close (per-name table). QFIN×427 09:30 $9.15 → close $8.80 -149.45; QMLS×1 09:30 $6.27 → close $6.11 -0.16; DY×7 09:30 $306.34 → close $294.34 -84.00; BURL×8 09:30 $291.30 → close $272.95 -146.80 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.11 | ▼ 09:30 equity $8,162.03 vs yday $8,198.80 (-36.77) | 09:30 open · cash $191.11 (unchanged overnight, no fees) · equity $8,162.03 vs prior close $8,198.80 (-36.77) · 4 name(s) re-marked at the open (per-name table). QFIN×427 yday $8.80 → 09:30 $8.70 -42.70; QMLS×1 yday $6.11 → 09:30 $5.95 -0.16; DY×7 yday $294.34 → 09:30 $298.01 +25.69; BURL×8 yday $272.95 → 09:30 $270.50 -19.60 | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 427 | $8.70 | $5.61 | $-1031.65 | $3,900.40 | ▼ -1,031.65 after sell → book $8,156.42; vs 09:30 mark -5.61 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `QMLS` | 1 | $5.95 | $0.08 | $-0.67 | $3,906.26 | ▼ -0.67 after sell → book $8,156.33; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,906.26 | ▼ close $8,022.81 vs 09:30 $8,162.03 (session -133.52) | 16:00 close · cash $3,906.26 · equity $8,022.81 vs 09:30 $8,162.03 (-139.22; session marks -133.52) · 2 name(s) marked open→close (per-name table). DY×7 09:30 $298.01 → close $291.21 -47.60; BURL×8 09:30 $270.50 → close $259.76 -85.92 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,906.26 | ▼ 09:30 equity $7,978.38 vs yday $8,022.81 (-44.43) | 09:30 open · cash $3,906.26 (unchanged overnight, no fees) · equity $7,978.38 vs prior close $8,022.81 (-44.43) · 2 name(s) re-marked at the open (per-name table). DY×7 yday $291.21 → 09:30 $289.16 -14.35; BURL×8 yday $259.76 → 09:30 $256.00 -30.08 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,906.26 | ▲ close $8,014.56 vs 09:30 $7,978.38 (session +36.18) | 16:00 close · cash $3,906.26 · equity $8,014.56 vs 09:30 $7,978.38 (+36.18; session marks +36.18) · 2 name(s) marked open→close (per-name table). DY×7 09:30 $289.16 → close $287.30 -13.02; BURL×8 09:30 $256.00 → close $262.15 +49.20 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,906.26 | ▲ 09:30 equity $8,018.27 vs yday $8,014.56 (+3.71) | 09:30 open · cash $3,906.26 (unchanged overnight, no fees) · equity $8,018.27 vs prior close $8,014.56 (+3.71) · 2 name(s) re-marked at the open (per-name table). DY×7 yday $287.30 → 09:30 $287.99 +4.83; BURL×8 yday $262.15 → 09:30 $262.01 -1.12 | — |
| 2026-09-02 09:30 ET | **SELL** | `DY` | 7 | $287.99 | $2.04 | $-132.50 | $5,920.16 | ▼ -132.50 after sell → book $8,016.24; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BURL` | 8 | $262.01 | $2.04 | $-238.37 | $8,014.20 | ▼ -238.37 after sell → book $8,014.20; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,014.20 | ▲ close $8,014.20 vs 09:30 $8,018.27 (session +0.00) | 16:00 close · cash $8,014.20 · no lots left · equity $8,014.20. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,014.20 | ▲ 09:30 equity $8,014.20 vs yday $8,014.20 (-0.00) | 09:30 open · cash $8,014.20 · no holdings · equity $8,014.20 vs prior close $8,014.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 156 | $7.31 | $2.46 | — | $6,871.38 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer; 🔵; ret5=+18.5; leftover $1144.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 110 | $10.38 | $2.32 | — | $5,727.81 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; 🔵; ret5=-56.2; leftover $1144.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `LX` | 1384 | $0.83 | $15.60 | — | $4,567.64 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-30.4; leftover $1144.89 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EVTL` | 1788 | $0.64 | $16.81 | — | $3,406.52 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.0; leftover $1144.89 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FJET` | 403 | $2.84 | $5.20 | — | $2,256.80 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.9; leftover $1144.89 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OSW` | 52 | $22.00 | $2.15 | — | $1,110.65 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-17.3; leftover $1144.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PCG` | 83 | $13.35 | $2.24 | — | $0.36 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; ret5=-26.8; leftover $1144.89 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.36 | ▲ close $7,991.99 vs 09:30 $8,014.20 (session +24.56) | 16:00 close · cash $0.36 · equity $7,991.99 vs 09:30 $8,014.20 (-22.21; session marks +24.56) · 7 name(s) marked open→close (per-name table). SION×156 09:30 $7.31 → close $6.75 -87.36; ALMS×110 09:30 $10.38 → close $11.36 +108.35; LX×1384 09:30 $0.83 → close $0.85 +34.60; EVTL×1788 09:30 $0.64 → close $0.60 -71.52; FJET×403 09:30 $2.84 → close $2.78 -24.18; OSW×52 09:30 $22.00 → close $22.27 +14.04; PCG×83 09:30 $13.35 → close $13.96 +50.63 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.36 | ▼ 09:30 equity $7,969.85 vs yday $7,991.99 (-22.14) | 09:30 open · cash $0.36 (unchanged overnight, no fees) · equity $7,969.85 vs prior close $7,991.99 (-22.14) · 7 name(s) re-marked at the open (per-name table). SION×156 yday $6.75 → 09:30 $6.68 -10.92; ALMS×110 yday $11.36 → 09:30 $11.23 -14.30; LX×1384 yday $0.85 → 09:30 $0.86 +8.30; EVTL×1788 yday $0.60 → 09:30 $0.60 +0.00; FJET×403 yday $2.78 → 09:30 $2.80 +8.06; OSW×52 yday $22.27 → 09:30 $22.27 +0.00; PCG×83 yday $13.96 → 09:30 $13.80 -13.28 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.36 | ▲ close $8,111.45 vs 09:30 $7,969.85 (session +141.60) | 16:00 close · cash $0.36 · equity $8,111.45 vs 09:30 $7,969.85 (+141.60; session marks +141.60) · 7 name(s) marked open→close (per-name table). SION×156 09:30 $6.68 → close $7.18 +78.00; ALMS×110 09:30 $11.23 → close $11.10 -14.30; LX×1384 09:30 $0.86 → close $0.88 +33.22; EVTL×1788 09:30 $0.60 → close $0.60 -3.58; FJET×403 09:30 $2.80 → close $2.80 +0.00; OSW×52 09:30 $22.27 → close $22.40 +6.76; PCG×83 09:30 $13.80 → close $14.30 +41.50 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.36 | ▲ 09:30 equity $8,114.03 vs yday $8,111.45 (+2.58) | 09:30 open · cash $0.36 (unchanged overnight, no fees) · equity $8,114.03 vs prior close $8,111.45 (+2.58) · 7 name(s) re-marked at the open (per-name table). SION×156 yday $7.18 → 09:30 $7.13 -7.80; ALMS×110 yday $11.10 → 09:30 $11.05 -5.50; LX×1384 yday $0.88 → 09:30 $0.90 +24.91; EVTL×1788 yday $0.60 → 09:30 $0.60 +0.00; FJET×403 yday $2.80 → 09:30 $2.80 +0.00; OSW×52 yday $22.40 → 09:30 $22.29 -5.72; PCG×83 yday $14.30 → 09:30 $14.26 -3.32 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.36 | ▼ close $7,978.62 vs 09:30 $8,114.03 (session -135.40) | 16:00 close · cash $0.36 · equity $7,978.62 vs 09:30 $8,114.03 (-135.41; session marks -135.40) · 7 name(s) marked open→close (per-name table). SION×156 09:30 $7.13 → close $7.30 +26.52; ALMS×110 09:30 $11.05 → close $10.58 -51.70; LX×1384 09:30 $0.90 → close $0.85 -65.05; EVTL×1788 09:30 $0.60 → close $0.59 -14.30; FJET×403 09:30 $2.80 → close $2.63 -68.51; OSW×52 09:30 $22.29 → close $22.12 -8.84; PCG×83 09:30 $14.26 → close $14.82 +46.48 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.36 | ▼ 09:30 equity $7,933.59 vs yday $7,978.62 (-45.03) | 09:30 open · cash $0.36 (unchanged overnight, no fees) · equity $7,933.59 vs prior close $7,978.62 (-45.03) · 7 name(s) re-marked at the open (per-name table). SION×156 yday $7.30 → 09:30 $7.27 -4.68; ALMS×110 yday $10.58 → 09:30 $10.49 -9.90; LX×1384 yday $0.85 → 09:30 $0.83 -24.91; EVTL×1788 yday $0.59 → 09:30 $0.59 +7.15; FJET×403 yday $2.63 → 09:30 $2.63 +0.00; OSW×52 yday $22.12 → 09:30 $21.86 -13.52; PCG×83 yday $14.82 → 09:30 $14.83 +0.83 | — |
| 2026-09-09 09:30 ET | **SELL** | `SION` | 156 | $7.27 | $2.49 | $-11.19 | $1,131.99 | ▼ -11.19 after sell → book $7,931.10; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ALMS` | 110 | $10.49 | $2.35 | $+7.98 | $2,283.54 | ▲ +7.98 after sell → book $7,928.75; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `LX` | 1384 | $0.83 | $15.95 | $-20.47 | $3,423.23 | ▼ -20.47 after sell → book $7,912.80; vs 09:30 mark -15.95 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `EVTL` | 1788 | $0.59 | $16.29 | $-115.35 | $4,469.01 | ▼ -115.35 after sell → book $7,896.51; vs 09:30 mark -16.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FJET` | 403 | $2.63 | $5.28 | $-95.10 | $5,523.63 | ▼ -95.10 after sell → book $7,891.24; vs 09:30 mark -5.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `OSW` | 52 | $21.86 | $2.17 | $-11.59 | $6,658.18 | ▼ -11.59 after sell → book $7,889.07; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PCG` | 83 | $14.83 | $2.26 | $+118.34 | $7,886.81 | ▲ +118.34 after sell → book $7,886.81; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟡 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,886.81 | ▲ close $7,886.81 vs 09:30 $7,933.59 (session +0.00) | 16:00 close · cash $7,886.81 · no lots left · equity $7,886.81. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,886.81 | ▲ 09:30 equity $7,886.81 vs yday $7,886.81 (-0.00) | 09:30 open · cash $7,886.81 · no holdings · equity $7,886.81 vs prior close $7,886.81 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,886.81 | ▲ close $7,886.81 vs 09:30 $7,886.81 (session +0.00) | 16:00 close · cash $7,886.81 · no lots left · equity $7,886.81. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,886.81 | ▲ 09:30 equity $7,886.81 vs yday $7,886.81 (-0.00) | 09:30 open · cash $7,886.81 · no holdings · equity $7,886.81 vs prior close $7,886.81 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 54 | $20.61 | $2.15 | — | $6,771.72 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; 🔵; ret5=-24.7; leftover $1126.69 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RWT` | 320 | $3.52 | $4.13 | — | $5,641.19 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-19.2; leftover $1126.69 | join🔴 sector🔴 gen🟡 news🔴 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 20 | $54.66 | $2.05 | — | $4,545.94 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; ret5=-22.3; leftover $1126.69 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `TTAN` | 20 | $55.91 | $2.05 | — | $3,425.69 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-39.2; leftover $1126.69 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ENB` | 23 | $48.37 | $2.06 | — | $2,311.12 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; ret5=-0.2; leftover $1126.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `WAFD` | 32 | $34.44 | $2.09 | — | $1,206.95 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; ret5=-4.1; leftover $1126.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `KMB` | 11 | $99.24 | $2.02 | — | $113.29 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-4.7; leftover $1126.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.29 | ▼ close $7,818.46 vs 09:30 $7,886.81 (session -51.80) | 16:00 close · cash $113.29 · equity $7,818.46 vs 09:30 $7,886.81 (-68.35; session marks -51.80) · 7 name(s) marked open→close (per-name table). NAVN×54 09:30 $20.61 → close $21.02 +22.14; RWT×320 09:30 $3.52 → close $3.55 +9.60; COO×20 09:30 $54.66 → close $53.91 -15.00; TTAN×20 09:30 $55.91 → close $54.68 -24.60; ENB×23 09:30 $48.37 → close $47.76 -14.03; WAFD×32 09:30 $34.44 → close $33.88 -17.92; KMB×11 09:30 $99.24 → close $98.15 -11.99 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.29 | ▲ 09:30 equity $7,848.98 vs yday $7,818.46 (+30.52) | 09:30 open · cash $113.29 (unchanged overnight, no fees) · equity $7,848.98 vs prior close $7,818.46 (+30.52) · 7 name(s) re-marked at the open (per-name table). NAVN×54 yday $21.02 → 09:30 $21.10 +4.32; RWT×320 yday $3.55 → 09:30 $3.53 -6.40; COO×20 yday $53.91 → 09:30 $54.78 +17.40; TTAN×20 yday $54.68 → 09:30 $54.77 +1.80; ENB×23 yday $47.76 → 09:30 $47.85 +2.07; WAFD×32 yday $33.88 → 09:30 $33.88 +0.00; KMB×11 yday $98.15 → 09:30 $99.18 +11.33 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.29 | ▲ close $8,030.26 vs 09:30 $7,848.98 (session +181.28) | 16:00 close · cash $113.29 · equity $8,030.26 vs 09:30 $7,848.98 (+181.28; session marks +181.28) · 7 name(s) marked open→close (per-name table). NAVN×54 09:30 $21.10 → close $21.37 +14.58; RWT×320 09:30 $3.53 → close $3.83 +96.00; COO×20 09:30 $54.78 → close $54.22 -11.20; TTAN×20 09:30 $54.77 → close $58.99 +84.40; ENB×23 09:30 $47.85 → close $48.16 +7.13; WAFD×32 09:30 $33.88 → close $33.61 -8.64; KMB×11 09:30 $99.18 → close $99.09 -0.99 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.29 | ▼ 09:30 equity $7,998.24 vs yday $8,030.26 (-32.02) | 09:30 open · cash $113.29 (unchanged overnight, no fees) · equity $7,998.24 vs prior close $8,030.26 (-32.02) · 7 name(s) re-marked at the open (per-name table). NAVN×54 yday $21.37 → 09:30 $21.32 -2.70; RWT×320 yday $3.83 → 09:30 $3.80 -9.60; COO×20 yday $54.22 → 09:30 $54.40 +3.60; TTAN×20 yday $58.99 → 09:30 $57.78 -24.20; ENB×23 yday $48.16 → 09:30 $48.58 +9.66; WAFD×32 yday $33.61 → 09:30 $33.59 -0.64; KMB×11 yday $99.09 → 09:30 $98.35 -8.14 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.29 | ▲ close $8,066.65 vs 09:30 $7,998.24 (session +68.41) | 16:00 close · cash $113.29 · equity $8,066.65 vs 09:30 $7,998.24 (+68.41; session marks +68.41) · 7 name(s) marked open→close (per-name table). NAVN×54 09:30 $21.32 → close $22.42 +59.40; RWT×320 09:30 $3.80 → close $3.91 +35.20; COO×20 09:30 $54.40 → close $53.27 -22.60; TTAN×20 09:30 $57.78 → close $58.60 +16.40; ENB×23 09:30 $48.58 → close $48.36 -5.06; WAFD×32 09:30 $33.59 → close $33.01 -18.56; KMB×11 09:30 $98.35 → close $98.68 +3.63 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.29 | ▲ 09:30 equity $8,086.73 vs yday $8,066.65 (+20.08) | 09:30 open · cash $113.29 (unchanged overnight, no fees) · equity $8,086.73 vs prior close $8,066.65 (+20.08) · 7 name(s) re-marked at the open (per-name table). NAVN×54 yday $22.42 → 09:30 $22.50 +4.32; RWT×320 yday $3.91 → 09:30 $3.98 +22.40; COO×20 yday $53.27 → 09:30 $54.37 +22.00; TTAN×20 yday $58.60 → 09:30 $57.04 -31.20; ENB×23 yday $48.36 → 09:30 $48.36 +0.00; WAFD×32 yday $33.01 → 09:30 $33.09 +2.56; KMB×11 yday $98.68 → 09:30 $98.68 +0.00 | — |
| 2026-09-16 09:30 ET | **SELL** | `NAVN` | 54 | $22.50 | $2.17 | $+97.74 | $1,326.12 | ▲ +97.74 after sell → book $8,084.56; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RWT` | 320 | $3.98 | $4.19 | $+138.88 | $2,595.53 | ▲ +138.88 after sell → book $8,080.37; vs 09:30 mark -4.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COO` | 20 | $54.37 | $2.07 | $-9.92 | $3,680.86 | ▼ -9.92 after sell → book $8,078.30; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `TTAN` | 20 | $57.04 | $2.07 | $+18.48 | $4,819.59 | ▲ +18.48 after sell → book $8,076.23; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ENB` | 23 | $48.36 | $2.08 | $-4.37 | $5,929.79 | ▼ -4.37 after sell → book $8,074.15; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `WAFD` | 32 | $33.09 | $2.11 | $-47.39 | $6,986.56 | ▼ -47.39 after sell → book $8,072.04; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KMB` | 11 | $98.68 | $2.04 | $-10.23 | $8,070.00 | ▼ -10.23 after sell → book $8,070.00; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 97 | $10.30 | $2.28 | — | $7,068.62 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; ret5=-23.0; leftover $1008.75 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `PLAY` | 147 | $6.86 | $2.43 | — | $6,057.77 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; ret5=-22.4; leftover $1008.75 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `DVLT` | 6304 | $0.16 | $29.00 | — | $5,020.13 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-23.8; leftover $1008.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `NMRA` | 1195 | $0.84 | $13.67 | — | $3,997.88 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-33.5; leftover $1008.75 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ZSQR` | 431 | $2.34 | $5.56 | — | $2,983.78 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-25.2; leftover $1008.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `CTMX` | 370 | $2.72 | $4.77 | — | $1,972.60 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.3; leftover $1008.75 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `CRBP` | 147 | $6.86 | $2.43 | — | $961.75 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-34.7; leftover $1008.75 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `EYPT` | 261 | $3.66 | $3.37 | — | $3.13 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-19.7; leftover $1008.75 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.13 | ▼ close $7,892.58 vs 09:30 $8,086.73 (session -113.90) | 16:00 close · cash $3.13 · equity $7,892.58 vs 09:30 $8,086.73 (-194.15; session marks -113.90) · 8 name(s) marked open→close (per-name table). ALHC×97 09:30 $10.30 → close $8.71 -154.23; PLAY×147 09:30 $6.86 → close $6.86 +0.00; DVLT×6304 09:30 $0.16 → close $0.18 +126.08; NMRA×1195 09:30 $0.84 → close $0.77 -84.84; ZSQR×431 09:30 $2.34 → close $2.29 -21.55; CTMX×370 09:30 $2.72 → close $2.77 +16.65; CRBP×147 09:30 $6.86 → close $7.26 +58.80; EYPT×261 09:30 $3.66 → close $3.45 -54.81 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.13 | ▲ 09:30 equity $7,921.23 vs yday $7,892.58 (+28.65) | 09:30 open · cash $3.13 (unchanged overnight, no fees) · equity $7,921.23 vs prior close $7,892.58 (+28.65) · 8 name(s) re-marked at the open (per-name table). ALHC×97 yday $8.71 → 09:30 $8.58 -12.61; PLAY×147 yday $6.86 → 09:30 $6.96 +14.70; DVLT×6304 yday $0.18 → 09:30 $0.17 -63.04; NMRA×1195 yday $0.77 → 09:30 $0.78 +8.37; ZSQR×431 yday $2.29 → 09:30 $2.35 +25.86; CTMX×370 yday $2.77 → 09:30 $2.83 +24.05; CRBP×147 yday $7.26 → 09:30 $7.26 +0.00; EYPT×261 yday $3.45 → 09:30 $3.57 +31.32 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.13 | ▲ close $7,960.05 vs 09:30 $7,921.23 (session +38.82) | 16:00 close · cash $3.13 · equity $7,960.05 vs 09:30 $7,921.23 (+38.82; session marks +38.82) · 8 name(s) marked open→close (per-name table). ALHC×97 09:30 $8.58 → close $8.70 +11.64; PLAY×147 09:30 $6.96 → close $6.54 -61.74; DVLT×6304 09:30 $0.17 → close $0.16 -63.04; NMRA×1195 09:30 $0.78 → close $0.75 -35.85; ZSQR×431 09:30 $2.35 → close $2.54 +81.89; CTMX×370 09:30 $2.83 → close $2.82 -3.70; CRBP×147 09:30 $7.26 → close $7.26 +0.00; EYPT×261 09:30 $3.57 → close $3.99 +109.62 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.13 | ▲ 09:30 equity $7,995.23 vs yday $7,960.05 (+35.18) | 09:30 open · cash $3.13 (unchanged overnight, no fees) · equity $7,995.23 vs prior close $7,960.05 (+35.18) · 8 name(s) re-marked at the open (per-name table). ALHC×97 yday $8.70 → 09:30 $8.68 -1.94; PLAY×147 yday $6.54 → 09:30 $6.64 +14.70; DVLT×6304 yday $0.16 → 09:30 $0.17 +63.04; NMRA×1195 yday $0.75 → 09:30 $0.74 -10.76; ZSQR×431 yday $2.54 → 09:30 $2.50 -17.24; CTMX×370 yday $2.82 → 09:30 $2.83 +3.70; CRBP×147 yday $7.26 → 09:30 $7.22 -5.88; EYPT×261 yday $3.99 → 09:30 $3.95 -10.44 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.13 | ▼ close $7,901.03 vs 09:30 $7,995.23 (session -94.21) | 16:00 close · cash $3.13 · equity $7,901.03 vs 09:30 $7,995.23 (-94.20; session marks -94.21) · 8 name(s) marked open→close (per-name table). ALHC×97 09:30 $8.68 → close $8.35 -32.01; PLAY×147 09:30 $6.64 → close $6.73 +13.23; DVLT×6304 09:30 $0.17 → close $0.15 -126.08; NMRA×1195 09:30 $0.74 → close $0.73 -13.15; ZSQR×431 09:30 $2.50 → close $2.68 +77.58; CTMX×370 09:30 $2.83 → close $2.76 -25.90; CRBP×147 09:30 $7.22 → close $7.48 +38.22; EYPT×261 09:30 $3.95 → close $3.85 -26.10 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 1.59 < 1 share @ 39.85 |
| 2026-08-17 | `INV` | cash | leftover split 1.59 < 1 share @ 1.62 |
| 2026-08-17 | `KLC` | cash | leftover split 1.59 < 1 share @ 2.62 |
| 2026-08-17 | `CSAN` | cash | leftover split 1.59 < 1 share @ 2.50 |
| 2026-08-17 | `VIV` | cash | leftover split 1.59 < 1 share @ 11.55 |
| 2026-08-17 | `RBA` | cash | leftover split 1.59 < 1 share @ 84.07 |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `YSS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `EYPT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `BHF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MNSO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BHF` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OPLN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `LZB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | cash | leftover split 9.90 < 1 share @ 42.41 |
| 2026-08-21 | `WMT` | cash | leftover split 9.90 < 1 share @ 103.69 |
| 2026-08-21 | `ALH` | cash | leftover split 9.90 < 1 share @ 23.33 |
| 2026-08-21 | `DKS` | cash | leftover split 9.90 < 1 share @ 181.25 |
| 2026-08-21 | `BBAR` | cash | leftover split 9.90 < 1 share @ 14.29 |
| 2026-08-21 | `KLAR` | cash | leftover split 9.90 < 1 share @ 14.14 |
| 2026-08-24 | `LZB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `WB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CRI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DKS` | cash | leftover split 12.06 < 1 share @ 121.87 |
| 2026-08-27 | `CRI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DKS` | cash | leftover split 24.53 < 1 share @ 128.73 |
| 2026-08-28 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `DY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BURL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BURL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `EIX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PCG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `COLL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `LX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EVTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FJET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `OSW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PCG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AIIO` | cash | leftover split 0.12 < 1 share @ 1.75 |
| 2026-09-04 | `SWBI` | cash | leftover split 0.12 < 1 share @ 14.12 |
| 2026-09-04 | `AMX` | cash | leftover split 0.12 < 1 share @ 23.03 |
| 2026-09-08 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `LX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EVTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FJET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `OSW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PCG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SWBI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XPOF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SFD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `NAVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RWT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ENB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `WAFD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `KMB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RCUS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CLX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `NAVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RWT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ENB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `WAFD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `KMB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `NMRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ZSQR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CRBP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `MRLN` | cash | leftover split 1.56 < 1 share @ 2.27 |
| 2026-09-17 | `HBAN` | cash | leftover split 1.56 < 1 share @ 15.90 |
| 2026-09-18 | `PLAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `NMRA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ZSQR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CTMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CRBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RARE` | cash | leftover split 0.63 < 1 share @ 14.79 |
| 2026-09-18 | `FLNC` | cash | leftover split 0.63 < 1 share @ 7.54 |
| 2026-09-18 | `MNR` | cash | leftover split 0.63 < 1 share @ 10.95 |
| 2026-09-18 | `MTZ` | cash | leftover split 0.63 < 1 share @ 210.53 |
| 2026-09-18 | `TTAN` | cash | leftover split 0.63 < 1 share @ 53.53 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ALHC` | 97 | 2026-09-16 @ $10.30 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; ret5=-23.0; leftover $1008.75 |
| `PLAY` | 147 | 2026-09-16 @ $6.86 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; ret5=-22.4; leftover $1008.75 |
| `DVLT` | 6304 | 2026-09-16 @ $0.16 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-23.8; leftover $1008.75 |
| `NMRA` | 1195 | 2026-09-16 @ $0.84 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-33.5; leftover $1008.75 |
| `ZSQR` | 431 | 2026-09-16 @ $2.34 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-25.2; leftover $1008.75 |
| `CTMX` | 370 | 2026-09-16 @ $2.72 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.3; leftover $1008.75 |
| `CRBP` | 147 | 2026-09-16 @ $6.86 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-34.7; leftover $1008.75 |
| `EYPT` | 261 | 2026-09-16 @ $3.66 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-19.7; leftover $1008.75 |
