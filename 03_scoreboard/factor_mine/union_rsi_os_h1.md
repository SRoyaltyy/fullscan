# Factor mine action — `union_rsi_os_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ rsi_os, no 🚨

Cash book **-22.67%** ($7,733) · signal-only (no cash/fees) was +1.00%. Starts YES **0/26**. Fills 110 · skips 24 · realized $-2074.44.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `rsi_os=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $154.57.

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
| 2026-08-17 | `NCMI` | 929 | $2.86 | $2.80 | -55.74 | — | +0.00 | -55.74 | +102.19 | — |
| 2026-08-17 | `QMLS` | 342 | $7.32 | $7.24 | -27.36 | — | +0.00 | -27.36 | -17.10 | — |
| 2026-08-17 | `CLBT` | 230 | $11.14 | $11.19 | +11.50 | — | +0.00 | +11.50 | +82.80 | — |
| 2026-08-17 | `YSS` | 247 | $10.93 | $10.36 | -140.79 | $10.66 | +74.10 | -66.69 | +74.10 | +148.20 |
| 2026-08-17 | `CDNL` | 31 | — | $39.85 | +0.00 | $39.23 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-17 | `INV` | 786 | — | $1.62 | +0.00 | $1.39 | -184.71 | -184.71 | +0.00 | -184.71 |
| 2026-08-17 | `KLC` | 486 | — | $2.62 | +0.00 | $2.56 | -29.16 | -29.16 | +0.00 | -29.16 |
| 2026-08-17 | `CSAN` | 509 | — | $2.50 | +0.00 | $2.52 | +10.18 | +10.18 | +0.00 | +10.18 |
| 2026-08-17 | `VIV` | 110 | — | $11.55 | +0.00 | $11.40 | -16.50 | -16.50 | +0.00 | -16.50 |
| 2026-08-17 | `RBA` | 15 | — | $84.07 | +0.00 | $82.11 | -29.40 | -29.40 | +0.00 | -29.40 |
| 2026-08-18 | `YSS` | 247 | $10.66 | $10.24 | -103.74 | — | +0.00 | -103.74 | +44.46 | — |
| 2026-08-18 | `CDNL` | 31 | $39.23 | $41.57 | +72.54 | — | +0.00 | +72.54 | +53.32 | — |
| 2026-08-18 | `INV` | 786 | $1.39 | $1.32 | -47.16 | — | +0.00 | -47.16 | -231.87 | — |
| 2026-08-18 | `KLC` | 486 | $2.56 | $2.52 | -19.44 | — | +0.00 | -19.44 | -48.60 | — |
| 2026-08-18 | `CSAN` | 509 | $2.52 | $2.51 | -5.09 | — | +0.00 | -5.09 | +5.09 | — |
| 2026-08-18 | `VIV` | 110 | $11.40 | $11.45 | +5.50 | — | +0.00 | +5.50 | -11.00 | — |
| 2026-08-18 | `RBA` | 15 | $82.11 | $82.48 | +5.55 | — | +0.00 | +5.55 | -23.85 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `LZB` | 146 | — | $33.61 | +0.00 | $33.65 | +5.84 | +5.84 | +0.00 | +5.84 |
| 2026-08-20 | `BHF` | 92 | — | $53.27 | +0.00 | $53.22 | -4.60 | -4.60 | +0.00 | -4.60 |
| 2026-08-21 | `LZB` | 146 | $33.65 | $33.63 | -2.92 | — | +0.00 | -2.92 | +2.92 | — |
| 2026-08-21 | `BHF` | 92 | $53.22 | $53.10 | -11.04 | — | +0.00 | -11.04 | -15.64 | — |
| 2026-08-21 | `AAP` | 33 | — | $42.41 | +0.00 | $42.58 | +5.61 | +5.61 | +0.00 | +5.61 |
| 2026-08-21 | `WMT` | 13 | — | $103.69 | +0.00 | $103.70 | +0.13 | +0.13 | +0.00 | +0.13 |
| 2026-08-21 | `ALH` | 60 | — | $23.33 | +0.00 | $23.47 | +8.40 | +8.40 | +0.00 | +8.40 |
| 2026-08-21 | `WB` | 196 | — | $7.13 | +0.00 | $7.03 | -19.60 | -19.60 | +0.00 | -19.60 |
| 2026-08-21 | `DKS` | 7 | — | $181.25 | +0.00 | $183.23 | +13.86 | +13.86 | +0.00 | +13.86 |
| 2026-08-21 | `BBAR` | 98 | — | $14.29 | +0.00 | $14.05 | -23.52 | -23.52 | +0.00 | -23.52 |
| 2026-08-21 | `KLAR` | 99 | — | $14.14 | +0.00 | $14.33 | +18.81 | +18.81 | +0.00 | +18.81 |
| 2026-08-24 | `AAP` | 33 | $42.58 | $43.05 | +15.51 | — | +0.00 | +15.51 | +21.12 | — |
| 2026-08-24 | `WMT` | 13 | $103.70 | $104.14 | +5.72 | — | +0.00 | +5.72 | +5.85 | — |
| 2026-08-24 | `ALH` | 60 | $23.47 | $23.66 | +11.40 | — | +0.00 | +11.40 | +19.80 | — |
| 2026-08-24 | `WB` | 196 | $7.03 | $6.98 | -9.80 | — | +0.00 | -9.80 | -29.40 | — |
| 2026-08-24 | `DKS` | 7 | $183.23 | $184.79 | +10.92 | — | +0.00 | +10.92 | +24.78 | — |
| 2026-08-24 | `BBAR` | 98 | $14.05 | $14.05 | +0.00 | — | +0.00 | +0.00 | -23.52 | — |
| 2026-08-24 | `KLAR` | 99 | $14.33 | $14.24 | -8.91 | — | +0.00 | -8.91 | +9.90 | — |
| 2026-08-25 | `QFIN` | 443 | — | $11.09 | +0.00 | $11.53 | +194.92 | +194.92 | +0.00 | +194.92 |
| 2026-08-25 | `CRI` | 143 | — | $34.24 | +0.00 | $33.91 | -47.19 | -47.19 | +0.00 | -47.19 |
| 2026-08-26 | `QFIN` | 443 | $11.53 | $9.76 | -784.11 | $9.35 | -181.63 | -965.74 | -589.19 | -770.82 |
| 2026-08-26 | `CRI` | 143 | $33.91 | $33.68 | -32.89 | — | +0.00 | -32.89 | -80.08 | — |
| 2026-08-26 | `DKS` | 13 | — | $121.87 | +0.00 | $129.66 | +101.27 | +101.27 | +0.00 | +101.27 |
| 2026-08-26 | `QMLS` | 248 | — | $6.47 | +0.00 | $6.10 | -91.76 | -91.76 | +0.00 | -91.76 |
| 2026-08-26 | `WB` | 226 | — | $7.10 | +0.00 | $7.04 | -13.56 | -13.56 | +0.00 | -13.56 |
| 2026-08-27 | `QFIN` | 443 | $9.35 | $9.42 | +31.01 | $9.17 | -110.75 | -79.74 | -739.81 | -850.56 |
| 2026-08-27 | `DKS` | 13 | $129.66 | $128.73 | -12.09 | $131.77 | +39.52 | +27.43 | +89.18 | +128.70 |
| 2026-08-27 | `QMLS` | 248 | $6.10 | $6.33 | +57.04 | $6.47 | +34.72 | +91.76 | -34.72 | +0.00 |
| 2026-08-27 | `WB` | 226 | $7.04 | $7.04 | +0.00 | — | +0.00 | +0.00 | -13.56 | — |
| 2026-08-28 | `QFIN` | 443 | $9.17 | $9.15 | -8.86 | $8.80 | -155.05 | -163.91 | -859.42 | -1014.47 |
| 2026-08-28 | `DKS` | 13 | $131.77 | $132.80 | +13.39 | — | +0.00 | +13.39 | +142.09 | — |
| 2026-08-28 | `QMLS` | 248 | $6.47 | $6.27 | -49.60 | — | +0.00 | -49.60 | -49.60 | — |
| 2026-08-28 | `DY` | 7 | — | $306.34 | +0.00 | $294.34 | -84.00 | -84.00 | +0.00 | -84.00 |
| 2026-08-28 | `BURL` | 8 | — | $291.30 | +0.00 | $272.95 | -146.80 | -146.80 | +0.00 | -146.80 |
| 2026-08-31 | `QFIN` | 443 | $8.80 | $8.70 | -44.30 | — | +0.00 | -44.30 | -1058.77 | — |
| 2026-08-31 | `DY` | 7 | $294.34 | $298.01 | +25.69 | — | +0.00 | +25.69 | -58.31 | — |
| 2026-08-31 | `BURL` | 8 | $272.95 | $270.50 | -19.60 | — | +0.00 | -19.60 | -166.40 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `SION` | 166 | — | $7.31 | +0.00 | $6.75 | -92.96 | -92.96 | +0.00 | -92.96 |
| 2026-09-03 | `ALMS` | 117 | — | $10.38 | +0.00 | $11.36 | +115.24 | +115.24 | +0.00 | +115.24 |
| 2026-09-03 | `LX` | 1468 | — | $0.83 | +0.00 | $0.85 | +36.70 | +36.70 | +0.00 | +36.70 |
| 2026-09-03 | `EVTL` | 1897 | — | $0.64 | +0.00 | $0.60 | -75.88 | -75.88 | +0.00 | -75.88 |
| 2026-09-03 | `FJET` | 427 | — | $2.84 | +0.00 | $2.78 | -25.62 | -25.62 | +0.00 | -25.62 |
| 2026-09-03 | `OSW` | 55 | — | $22.00 | +0.00 | $22.27 | +14.85 | +14.85 | +0.00 | +14.85 |
| 2026-09-03 | `PCG` | 87 | — | $13.35 | +0.00 | $13.96 | +53.07 | +53.07 | +0.00 | +53.07 |
| 2026-09-04 | `SION` | 166 | $6.75 | $6.68 | -11.62 | $7.18 | +83.00 | +71.38 | -104.58 | -21.58 |
| 2026-09-04 | `ALMS` | 117 | $11.36 | $11.23 | -15.21 | $11.10 | -15.21 | -30.42 | +100.04 | +84.82 |
| 2026-09-04 | `LX` | 1468 | $0.85 | $0.86 | +8.81 | — | +0.00 | +8.81 | +45.51 | — |
| 2026-09-04 | `EVTL` | 1897 | $0.60 | $0.60 | +0.00 | — | +0.00 | +0.00 | -75.88 | — |
| 2026-09-04 | `FJET` | 427 | $2.78 | $2.80 | +8.54 | — | +0.00 | +8.54 | -17.08 | — |
| 2026-09-04 | `OSW` | 55 | $22.27 | $22.27 | +0.00 | — | +0.00 | +0.00 | +14.85 | — |
| 2026-09-04 | `PCG` | 87 | $13.96 | $13.80 | -13.92 | — | +0.00 | -13.92 | +39.15 | — |
| 2026-09-04 | `AIIO` | 1140 | — | $1.75 | +0.00 | $1.84 | +102.60 | +102.60 | +0.00 | +102.60 |
| 2026-09-04 | `SWBI` | 141 | — | $14.12 | +0.00 | $12.89 | -173.43 | -173.43 | +0.00 | -173.43 |
| 2026-09-04 | `AMX` | 85 | — | $23.03 | +0.00 | $23.00 | -2.55 | -2.55 | +0.00 | -2.55 |
| 2026-09-08 | `SION` | 166 | $7.18 | $7.13 | -8.30 | — | +0.00 | -8.30 | -29.88 | — |
| 2026-09-08 | `ALMS` | 117 | $11.10 | $11.05 | -5.85 | — | +0.00 | -5.85 | +78.98 | — |
| 2026-09-08 | `AIIO` | 1140 | $1.84 | $1.81 | -34.20 | — | +0.00 | -34.20 | +68.40 | — |
| 2026-09-08 | `SWBI` | 141 | $12.89 | $12.51 | -53.58 | $13.23 | +101.52 | +47.94 | -227.01 | -125.49 |
| 2026-09-08 | `AMX` | 85 | $23.00 | $23.15 | +12.75 | — | +0.00 | +12.75 | +10.20 | — |
| 2026-09-09 | `SWBI` | 141 | $13.23 | $13.12 | -15.51 | — | +0.00 | -15.51 | -141.00 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `NAVN` | 57 | — | $20.61 | +0.00 | $21.02 | +23.37 | +23.37 | +0.00 | +23.37 |
| 2026-09-11 | `RWT` | 339 | — | $3.52 | +0.00 | $3.55 | +10.17 | +10.17 | +0.00 | +10.17 |
| 2026-09-11 | `COO` | 21 | — | $54.66 | +0.00 | $53.91 | -15.75 | -15.75 | +0.00 | -15.75 |
| 2026-09-11 | `TTAN` | 21 | — | $55.91 | +0.00 | $54.68 | -25.83 | -25.83 | +0.00 | -25.83 |
| 2026-09-11 | `ENB` | 24 | — | $48.37 | +0.00 | $47.76 | -14.64 | -14.64 | +0.00 | -14.64 |
| 2026-09-11 | `WAFD` | 34 | — | $34.44 | +0.00 | $33.88 | -19.04 | -19.04 | +0.00 | -19.04 |
| 2026-09-11 | `KMB` | 12 | — | $99.24 | +0.00 | $98.15 | -13.08 | -13.08 | +0.00 | -13.08 |
| 2026-09-14 | `NAVN` | 57 | $21.02 | $21.10 | +4.56 | — | +0.00 | +4.56 | +27.93 | — |
| 2026-09-14 | `RWT` | 339 | $3.55 | $3.53 | -6.78 | — | +0.00 | -6.78 | +3.39 | — |
| 2026-09-14 | `COO` | 21 | $53.91 | $54.78 | +18.27 | $54.22 | -11.76 | +6.51 | +2.52 | -9.24 |
| 2026-09-14 | `TTAN` | 21 | $54.68 | $54.77 | +1.89 | $58.99 | +88.62 | +90.51 | -23.94 | +64.68 |
| 2026-09-14 | `ENB` | 24 | $47.76 | $47.85 | +2.16 | — | +0.00 | +2.16 | -12.48 | — |
| 2026-09-14 | `WAFD` | 34 | $33.88 | $33.88 | +0.00 | — | +0.00 | +0.00 | -19.04 | — |
| 2026-09-14 | `KMB` | 12 | $98.15 | $99.18 | +12.36 | — | +0.00 | +12.36 | -0.72 | — |
| 2026-09-15 | `COO` | 21 | $54.22 | $54.40 | +3.78 | — | +0.00 | +3.78 | -5.46 | — |
| 2026-09-15 | `TTAN` | 21 | $58.99 | $57.78 | -25.41 | $58.60 | +17.22 | -8.19 | +39.27 | +56.49 |
| 2026-09-16 | `TTAN` | 21 | $58.60 | $57.04 | -32.76 | — | +0.00 | -32.76 | +23.73 | — |
| 2026-09-16 | `ALHC` | 101 | — | $10.30 | +0.00 | $8.71 | -160.59 | -160.59 | +0.00 | -160.59 |
| 2026-09-16 | `PLAY` | 151 | — | $6.86 | +0.00 | $6.86 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-16 | `DVLT` | 6515 | — | $0.16 | +0.00 | $0.18 | +130.30 | +130.30 | +0.00 | +130.30 |
| 2026-09-16 | `NMRA` | 1235 | — | $0.84 | +0.00 | $0.77 | -87.68 | -87.68 | +0.00 | -87.68 |
| 2026-09-16 | `ZSQR` | 445 | — | $2.34 | +0.00 | $2.29 | -22.25 | -22.25 | +0.00 | -22.25 |
| 2026-09-16 | `CTMX` | 383 | — | $2.72 | +0.00 | $2.77 | +17.23 | +17.23 | +0.00 | +17.23 |
| 2026-09-16 | `CRBP` | 151 | — | $6.86 | +0.00 | $7.26 | +60.40 | +60.40 | +0.00 | +60.40 |
| 2026-09-16 | `EYPT` | 271 | — | $3.66 | +0.00 | $3.45 | -56.91 | -56.91 | +0.00 | -56.91 |
| 2026-09-17 | `ALHC` | 101 | $8.71 | $8.58 | -13.13 | $8.70 | +12.12 | -1.01 | -173.72 | -161.60 |
| 2026-09-17 | `PLAY` | 151 | $6.86 | $6.96 | +15.10 | $6.54 | -63.42 | -48.32 | +15.10 | -48.32 |
| 2026-09-17 | `DVLT` | 6515 | $0.18 | $0.17 | -65.15 | — | +0.00 | -65.15 | +65.15 | — |
| 2026-09-17 | `NMRA` | 1235 | $0.77 | $0.78 | +8.65 | — | +0.00 | +8.65 | -79.04 | — |
| 2026-09-17 | `ZSQR` | 445 | $2.29 | $2.35 | +26.70 | — | +0.00 | +26.70 | +4.45 | — |
| 2026-09-17 | `CTMX` | 383 | $2.77 | $2.83 | +24.89 | — | +0.00 | +24.89 | +42.13 | — |
| 2026-09-17 | `CRBP` | 151 | $7.26 | $7.26 | +0.00 | — | +0.00 | +0.00 | +60.40 | — |
| 2026-09-17 | `EYPT` | 271 | $3.45 | $3.57 | +32.52 | — | +0.00 | +32.52 | -24.39 | — |
| 2026-09-17 | `MRLN` | 1366 | — | $2.27 | +0.00 | $2.06 | -286.86 | -286.86 | +0.00 | -286.86 |
| 2026-09-17 | `HBAN` | 193 | — | $15.90 | +0.00 | $15.88 | -3.86 | -3.86 | +0.00 | -3.86 |
| 2026-09-18 | `ALHC` | 101 | $8.70 | $8.68 | -2.02 | $8.35 | -33.33 | -35.35 | -163.62 | -196.95 |
| 2026-09-18 | `PLAY` | 151 | $6.54 | $6.64 | +15.10 | — | +0.00 | +15.10 | -33.22 | — |
| 2026-09-18 | `MRLN` | 1366 | $2.06 | $2.07 | +13.66 | — | +0.00 | +13.66 | -273.20 | — |
| 2026-09-18 | `HBAN` | 193 | $15.88 | $15.86 | -3.86 | — | +0.00 | -3.86 | -7.72 | — |
| 2026-09-18 | `RARE` | 93 | — | $14.79 | +0.00 | $14.51 | -26.04 | -26.04 | +0.00 | -26.04 |
| 2026-09-18 | `FLNC` | 182 | — | $7.54 | +0.00 | $7.32 | -39.13 | -39.13 | +0.00 | -39.13 |
| 2026-09-18 | `MNR` | 125 | — | $10.95 | +0.00 | $11.13 | +22.50 | +22.50 | +0.00 | +22.50 |
| 2026-09-18 | `MTZ` | 6 | — | $210.53 | +0.00 | $214.38 | +23.10 | +23.10 | +0.00 | +23.10 |
| 2026-09-18 | `TTAN` | 25 | — | $53.53 | +0.00 | $55.04 | +37.63 | +37.63 | +0.00 | +37.63 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +454.38 | NCMI, QMLS, CLBT, YSS | — | $9.54 | $10,431.83 | NCMI×929, QMLS×342, CLBT×230, YSS×247 |
| 2026-08-17 | +2.25 | $9.54 | NCMI×929, QMLS×342, CLBT×230, YSS×247 | $10,219.44 | -212.39 | -194.71 | CDNL, INV, KLC, CSAN, VIV, RBA | NCMI, QMLS, CLBT | $25.39 | $9,975.64 | YSS×247, CDNL×31, INV×786, KLC×486, CSAN×509, VIV×110, RBA×15 |
| 2026-08-18 | -6.20 | $25.39 | YSS×247, CDNL×31, INV×786, KLC×486, CSAN×509, VIV×110, RBA×15 | $9,883.80 | -91.84 | +0.00 | — | YSS, CDNL, INV, KLC, CSAN, VIV, RBA | $9,850.75 | $9,850.75 | — |
| 2026-08-19 | -7.20 | $9,850.75 | — | $9,850.75 | -0.00 | +0.00 | — | — | $9,850.75 | $9,850.75 | — |
| 2026-08-20 | +1.12 | $9,850.75 | — | $9,850.75 | -0.00 | +1.24 | LZB, BHF | — | $38.16 | $9,847.30 | LZB×146, BHF×92 |
| 2026-08-21 | +3.25 | $38.16 | LZB×146, BHF×92 | $9,833.34 | -13.96 | +3.69 | AAP, WMT, ALH, WB, DKS, BBAR, KLAR | LZB, BHF | $199.27 | $9,816.77 | AAP×33, WMT×13, ALH×60, WB×196, DKS×7, BBAR×98, KLAR×99 |
| 2026-08-24 | -5.17 | $199.27 | AAP×33, WMT×13, ALH×60, WB×196, DKS×7, BBAR×98, KLAR×99 | $9,841.61 | +24.84 | +0.00 | — | AAP, WMT, ALH, WB, DKS, BBAR, KLAR | $9,825.98 | $9,825.98 | — |
| 2026-08-25 | +1.80 | $9,825.98 | — | $9,825.98 | -0.00 | +147.73 | QFIN, CRI | — | $8.65 | $9,965.57 | QFIN×443, CRI×143 |
| 2026-08-26 | +2.02 | $8.65 | QFIN×443, CRI×143 | $9,148.57 | -817.00 | -185.68 | DKS, QMLS, WB | CRI | $20.80 | $8,952.27 | QFIN×443, DKS×13, QMLS×248, WB×226 |
| 2026-08-27 | — | $20.80 | QFIN×443, DKS×13, QMLS×248, WB×226 | $9,028.23 | +75.96 | -36.51 | — | WB | $1,608.87 | $8,988.75 | QFIN×443, DKS×13, QMLS×248 |
| 2026-08-28 | +0.75 | $1,608.87 | QFIN×443, DKS×13, QMLS×248 | $8,943.68 | -45.07 | -385.85 | DY, BURL | DKS, QMLS | $406.12 | $8,548.50 | QFIN×443, DY×7, BURL×8 |
| 2026-08-31 | -5.85 | $406.12 | QFIN×443, DY×7, BURL×8 | $8,510.29 | -38.21 | +0.00 | — | QFIN, DY, BURL | $8,500.39 | $8,500.39 | — |
| 2026-09-01 | -6.30 | $8,500.39 | — | $8,500.39 | +0.00 | +0.00 | — | — | $8,500.39 | $8,500.39 | — |
| 2026-09-02 | -3.83 | $8,500.39 | — | $8,500.39 | +0.00 | +0.00 | — | — | $8,500.39 | $8,500.39 | — |
| 2026-09-03 | -0.90 | $8,500.39 | — | $8,500.39 | +0.00 | +25.40 | SION, ALMS, LX, EVTL, FJET, OSW, PCG | — | $11.69 | $8,476.68 | SION×166, ALMS×117, LX×1468, EVTL×1897, FJET×427, OSW×55, PCG×87 |
| 2026-09-04 | +2.25 | $11.69 | SION×166, ALMS×117, LX×1468, EVTL×1897, FJET×427, OSW×55, PCG×87 | $8,453.28 | -23.40 | -5.59 | AIIO, SWBI, AMX | LX, EVTL, FJET, OSW, PCG | $22.96 | $8,383.63 | SION×166, ALMS×117, AIIO×1140, SWBI×141, AMX×85 |
| 2026-09-08 | -11.47 | $22.96 | SION×166, ALMS×117, AIIO×1140, SWBI×141, AMX×85 | $8,294.45 | -89.18 | +101.52 | — | SION, ALMS, AIIO, AMX | $6,508.46 | $8,373.89 | SWBI×141 |
| 2026-09-09 | -13.95 | $6,508.46 | SWBI×141 | $8,358.38 | -15.51 | +0.00 | — | SWBI | $8,355.93 | $8,355.93 | — |
| 2026-09-10 | -13.28 | $8,355.93 | — | $8,355.93 | -0.00 | +0.00 | — | — | $8,355.93 | $8,355.93 | — |
| 2026-09-11 | +0.50 | $8,355.93 | — | $8,355.93 | -0.00 | -54.80 | NAVN, RWT, COO, TTAN, ENB, WAFD, KMB | — | $126.37 | $8,284.31 | NAVN×57, RWT×339, COO×21, TTAN×21, ENB×24, WAFD×34, KMB×12 |
| 2026-09-14 | -11.00 | $126.37 | NAVN×57, RWT×339, COO×21, TTAN×21, ENB×24, WAFD×34, KMB×12 | $8,316.77 | +32.46 | +76.86 | — | NAVN, RWT, ENB, WAFD, KMB | $6,003.36 | $8,380.77 | COO×21, TTAN×21 |
| 2026-09-15 | -3.84 | $6,003.36 | COO×21, TTAN×21 | $8,359.14 | -21.63 | +17.22 | — | COO | $7,143.68 | $8,374.28 | TTAN×21 |
| 2026-09-16 | +5.30 | $7,143.68 | TTAN×21 | $8,341.52 | -32.76 | -119.50 | ALHC, PLAY, DVLT, NMRA, ZSQR, CTMX, CRBP, EYPT | TTAN | $2.32 | $8,154.50 | ALHC×101, PLAY×151, DVLT×6515, NMRA×1235, ZSQR×445, CTMX×383, CRBP×151, EYPT×271 |
| 2026-09-17 | +7.38 | $2.32 | ALHC×101, PLAY×151, DVLT×6515, NMRA×1235, ZSQR×445, CTMX×383, CRBP×151, EYPT×271 | $8,184.08 | +29.58 | -342.02 | MRLN, HBAN | DVLT, NMRA, ZSQR, CTMX, CRBP, EYPT | $14.69 | $7,759.73 | ALHC×101, PLAY×151, MRLN×1366, HBAN×193 |
| 2026-09-18 | +4.86 | $14.69 | ALHC×101, PLAY×151, MRLN×1366, HBAN×193 | $7,782.61 | +22.88 | -15.27 | RARE, FLNC, MNR, MTZ, TTAN | PLAY, MRLN, HBAN | $154.57 | $7,733.12 | ALHC×101, RARE×93, FLNC×182, MNR×125, MTZ×6, TTAN×25 |

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
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 929 | $2.80 | $12.16 | $+78.05 | $2,598.58 | ▲ +78.05 after sell → book $10,207.28; vs 09:30 mark -12.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 342 | $7.24 | $4.49 | $-26.00 | $5,070.17 | ▼ -26.00 after sell → book $10,202.79; vs 09:30 mark -4.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 230 | $11.19 | $3.03 | $+76.81 | $7,640.85 | ▲ +76.81 after sell → book $10,199.77; vs 09:30 mark -3.02 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $6,403.41 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1273.47 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 786 | $1.62 | $10.14 | — | $5,119.96 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $1273.47 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 486 | $2.62 | $6.27 | — | $3,840.37 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $1273.47 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CSAN` | 509 | $2.50 | $6.57 | — | $2,561.30 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; 🔵; ⚪; ret5=-12.5; leftover $1273.47 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VIV` | 110 | $11.55 | $2.32 | — | $1,288.48 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; ⚪; ret5=-5.0; leftover $1273.47 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `RBA` | 15 | $84.07 | $2.04 | — | $25.39 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; ⚪; ret5=-11.0; leftover $1273.47 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.39 | ▼ close $9,975.64 vs 09:30 $10,219.44 (session -194.71) | 16:00 close · cash $25.39 · equity $9,975.64 vs 09:30 $10,219.44 (-243.80; session marks -194.71) · 7 name(s) marked open→close (per-name table). YSS×247 09:30 $10.36 → close $10.66 +74.10; CDNL×31 09:30 $39.85 → close $39.23 -19.22; INV×786 09:30 $1.62 → close $1.39 -184.71; KLC×486 09:30 $2.62 → close $2.56 -29.16; CSAN×509 09:30 $2.50 → close $2.52 +10.18; VIV×110 09:30 $11.55 → close $11.40 -16.50; RBA×15 09:30 $84.07 → close $82.11 -29.40 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.39 | ▼ 09:30 equity $9,883.80 vs yday $9,975.64 (-91.84) | 09:30 open · cash $25.39 (unchanged overnight, no fees) · equity $9,883.80 vs prior close $9,975.64 (-91.84) · 7 name(s) re-marked at the open (per-name table). YSS×247 yday $10.66 → 09:30 $10.24 -103.74; CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; INV×786 yday $1.39 → 09:30 $1.32 -47.16; KLC×486 yday $2.56 → 09:30 $2.52 -19.44; CSAN×509 yday $2.52 → 09:30 $2.51 -5.09; VIV×110 yday $11.40 → 09:30 $11.45 +5.50; RBA×15 yday $82.11 → 09:30 $82.48 +5.55 | — |
| 2026-08-18 09:30 ET | **SELL** | `YSS` | 247 | $10.24 | $3.25 | $+38.03 | $2,551.43 | ▲ +38.03 after sell → book $9,880.56; vs 09:30 mark -3.24 | dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $3,837.99 | ▲ +49.13 after sell → book $9,878.45; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 786 | $1.32 | $10.28 | $-252.29 | $4,869.16 | ▼ -252.29 after sell → book $9,868.17; vs 09:30 mark -10.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `KLC` | 486 | $2.52 | $6.36 | $-61.23 | $6,087.52 | ▼ -61.23 after sell → book $9,861.81; vs 09:30 mark -6.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CSAN` | 509 | $2.51 | $6.66 | $-8.14 | $7,358.45 | ▼ -8.14 after sell → book $9,855.15; vs 09:30 mark -6.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VIV` | 110 | $11.45 | $2.35 | $-15.67 | $8,615.60 | ▼ -15.67 after sell → book $9,852.80; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `RBA` | 15 | $82.48 | $2.06 | $-27.94 | $9,850.75 | ▼ -27.94 after sell → book $9,850.75; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,850.75 | ▲ close $9,850.75 vs 09:30 $9,883.80 (session +0.00) | 16:00 close · cash $9,850.75 · no lots left · equity $9,850.75. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,850.75 | ▲ 09:30 equity $9,850.75 vs yday $9,850.75 (-0.00) | 09:30 open · cash $9,850.75 · no holdings · equity $9,850.75 vs prior close $9,850.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,850.75 | ▲ close $9,850.75 vs 09:30 $9,850.75 (session +0.00) | 16:00 close · cash $9,850.75 · no lots left · equity $9,850.75. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,850.75 | ▲ 09:30 equity $9,850.75 vs yday $9,850.75 (-0.00) | 09:30 open · cash $9,850.75 · no holdings · equity $9,850.75 vs prior close $9,850.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `LZB` | 146 | $33.61 | $2.43 | — | $4,941.26 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; 🔵; ret5=-17.4; leftover $4925.37 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHF` | 92 | $53.27 | $2.27 | — | $38.16 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-10.9; leftover $4925.37 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.16 | ▲ close $9,847.30 vs 09:30 $9,850.75 (session +1.24) | 16:00 close · cash $38.16 · equity $9,847.30 vs 09:30 $9,850.75 (-3.45; session marks +1.24) · 2 name(s) marked open→close (per-name table). LZB×146 09:30 $33.61 → close $33.65 +5.84; BHF×92 09:30 $53.27 → close $53.22 -4.60 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.16 | ▼ 09:30 equity $9,833.34 vs yday $9,847.30 (-13.96) | 09:30 open · cash $38.16 (unchanged overnight, no fees) · equity $9,833.34 vs prior close $9,847.30 (-13.96) · 2 name(s) re-marked at the open (per-name table). LZB×146 yday $33.65 → 09:30 $33.63 -2.92; BHF×92 yday $53.22 → 09:30 $53.10 -11.04 | — |
| 2026-08-21 09:30 ET | **SELL** | `LZB` | 146 | $33.63 | $2.49 | $-2.00 | $4,945.64 | ▼ -2.00 after sell → book $9,830.84; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHF` | 92 | $53.10 | $2.32 | $-20.23 | $9,828.52 | ▼ -20.23 after sell → book $9,828.52; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AAP` | 33 | $42.41 | $2.09 | — | $8,426.90 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; ret5=-26.1; leftover $1404.07 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 13 | $103.69 | $2.03 | — | $7,076.91 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; ret5=-10.3; leftover $1404.07 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ALH` | 60 | $23.33 | $2.17 | — | $5,674.94 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-10.0; leftover $1404.07 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `WB` | 196 | $7.13 | $2.58 | — | $4,274.88 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; ret5=-5.9; leftover $1404.07 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DKS` | 7 | $181.25 | $2.01 | — | $3,004.12 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-11.3; leftover $1404.07 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BBAR` | 98 | $14.29 | $2.28 | — | $1,601.41 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-12.1; leftover $1404.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `KLAR` | 99 | $14.14 | $2.29 | — | $199.27 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ⚪; ret5=-32.3; leftover $1404.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $199.27 | ▲ close $9,816.77 vs 09:30 $9,833.34 (session +3.69) | 16:00 close · cash $199.27 · equity $9,816.77 vs 09:30 $9,833.34 (-16.57; session marks +3.69) · 7 name(s) marked open→close (per-name table). AAP×33 09:30 $42.41 → close $42.58 +5.61; WMT×13 09:30 $103.69 → close $103.70 +0.13; ALH×60 09:30 $23.33 → close $23.47 +8.40; WB×196 09:30 $7.13 → close $7.03 -19.60; DKS×7 09:30 $181.25 → close $183.23 +13.86; BBAR×98 09:30 $14.29 → close $14.05 -23.52; KLAR×99 09:30 $14.14 → close $14.33 +18.81 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $199.27 | ▲ 09:30 equity $9,841.61 vs yday $9,816.77 (+24.84) | 09:30 open · cash $199.27 (unchanged overnight, no fees) · equity $9,841.61 vs prior close $9,816.77 (+24.84) · 7 name(s) re-marked at the open (per-name table). AAP×33 yday $42.58 → 09:30 $43.05 +15.51; WMT×13 yday $103.70 → 09:30 $104.14 +5.72; ALH×60 yday $23.47 → 09:30 $23.66 +11.40; WB×196 yday $7.03 → 09:30 $6.98 -9.80; DKS×7 yday $183.23 → 09:30 $184.79 +10.92; BBAR×98 yday $14.05 → 09:30 $14.05 +0.00; KLAR×99 yday $14.33 → 09:30 $14.24 -8.91 | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 33 | $43.05 | $2.11 | $+16.92 | $1,617.81 | ▲ +16.92 after sell → book $9,839.50; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 13 | $104.14 | $2.05 | $+1.77 | $2,969.58 | ▲ +1.77 after sell → book $9,837.45; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ALH` | 60 | $23.66 | $2.19 | $+15.44 | $4,386.98 | ▲ +15.44 after sell → book $9,835.25; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🔴 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `WB` | 196 | $6.98 | $2.62 | $-34.60 | $5,752.44 | ▼ -34.60 after sell → book $9,832.63; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DKS` | 7 | $184.79 | $2.03 | $+20.74 | $7,043.94 | ▲ +20.74 after sell → book $9,830.60; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BBAR` | 98 | $14.05 | $2.31 | $-28.12 | $8,418.53 | ▼ -28.12 after sell → book $9,828.29; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `KLAR` | 99 | $14.24 | $2.31 | $+5.30 | $9,825.98 | ▲ +5.30 after sell → book $9,825.98; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,825.98 | ▲ close $9,825.98 vs 09:30 $9,841.61 (session +0.00) | 16:00 close · cash $9,825.98 · no lots left · equity $9,825.98. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,825.98 | ▲ 09:30 equity $9,825.98 vs yday $9,825.98 (-0.00) | 09:30 open · cash $9,825.98 · no holdings · equity $9,825.98 vs prior close $9,825.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `QFIN` | 443 | $11.09 | $5.71 | — | $4,907.39 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-8.0; leftover $4912.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CRI` | 143 | $34.24 | $2.42 | — | $8.65 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-9.2; leftover $4912.99 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.65 | ▲ close $9,965.57 vs 09:30 $9,825.98 (session +147.73) | 16:00 close · cash $8.65 · equity $9,965.57 vs 09:30 $9,825.98 (+139.59; session marks +147.73) · 2 name(s) marked open→close (per-name table). QFIN×443 09:30 $11.09 → close $11.53 +194.92; CRI×143 09:30 $34.24 → close $33.91 -47.19 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.65 | ▼ 09:30 equity $9,148.57 vs yday $9,965.57 (-817.00) | 09:30 open · cash $8.65 (unchanged overnight, no fees) · equity $9,148.57 vs prior close $9,965.57 (-817.00) · 2 name(s) re-marked at the open (per-name table). QFIN×443 yday $11.53 → 09:30 $9.76 -784.11; CRI×143 yday $33.91 → 09:30 $33.68 -32.89 | — |
| 2026-08-26 09:30 ET | **SELL** | `CRI` | 143 | $33.68 | $2.48 | $-84.98 | $4,822.41 | ▼ -84.98 after sell → book $9,146.09; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 13 | $121.87 | $2.03 | — | $3,236.07 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; 🔵; ret5=-35.1; leftover $1607.47 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `QMLS` | 248 | $6.47 | $3.20 | — | $1,628.31 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; 🔵; ret5=-7.0; leftover $1607.47 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `WB` | 226 | $7.10 | $2.92 | — | $20.80 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-6.2; leftover $1607.47 | join🟡 sector🟡 gen🟢 news🔴 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.80 | ▼ close $8,952.27 vs 09:30 $9,148.57 (session -185.68) | 16:00 close · cash $20.80 · equity $8,952.27 vs 09:30 $9,148.57 (-196.30; session marks -185.68) · 4 name(s) marked open→close (per-name table). QFIN×443 09:30 $9.76 → close $9.35 -181.63; DKS×13 09:30 $121.87 → close $129.66 +101.27; QMLS×248 09:30 $6.47 → close $6.10 -91.76; WB×226 09:30 $7.10 → close $7.04 -13.56 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.80 | ▲ 09:30 equity $9,028.23 vs yday $8,952.27 (+75.96) | 09:30 open · cash $20.80 (unchanged overnight, no fees) · equity $9,028.23 vs prior close $8,952.27 (+75.96) · 4 name(s) re-marked at the open (per-name table). QFIN×443 yday $9.35 → 09:30 $9.42 +31.01; DKS×13 yday $129.66 → 09:30 $128.73 -12.09; QMLS×248 yday $6.10 → 09:30 $6.33 +57.04; WB×226 yday $7.04 → 09:30 $7.04 +0.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `WB` | 226 | $7.04 | $2.97 | $-19.44 | $1,608.87 | ▼ -19.44 after sell → book $9,025.26; vs 09:30 mark -2.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,608.87 | ▼ close $8,988.75 vs 09:30 $9,028.23 (session -36.51) | 16:00 close · cash $1,608.87 · equity $8,988.75 vs 09:30 $9,028.23 (-39.48; session marks -36.51) · 3 name(s) marked open→close (per-name table). QFIN×443 09:30 $9.42 → close $9.17 -110.75; DKS×13 09:30 $128.73 → close $131.77 +39.52; QMLS×248 09:30 $6.33 → close $6.47 +34.72 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,608.87 | ▼ 09:30 equity $8,943.68 vs yday $8,988.75 (-45.07) | 09:30 open · cash $1,608.87 (unchanged overnight, no fees) · equity $8,943.68 vs prior close $8,988.75 (-45.07) · 3 name(s) re-marked at the open (per-name table). QFIN×443 yday $9.17 → 09:30 $9.15 -8.86; DKS×13 yday $131.77 → 09:30 $132.80 +13.39; QMLS×248 yday $6.47 → 09:30 $6.27 -49.60 | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 13 | $132.80 | $2.05 | $+138.01 | $3,333.22 | ▲ +138.01 after sell → book $8,941.63; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `QMLS` | 248 | $6.27 | $3.25 | $-56.05 | $4,884.93 | ▼ -56.05 after sell → book $8,938.38; vs 09:30 mark -3.25 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 7 | $306.34 | $2.01 | — | $2,738.53 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; ret5=-23.0; leftover $2442.46 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BURL` | 8 | $291.30 | $2.01 | — | $406.12 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-13.0; leftover $2442.46 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $406.12 | ▼ close $8,548.50 vs 09:30 $8,943.68 (session -385.85) | 16:00 close · cash $406.12 · equity $8,548.50 vs 09:30 $8,943.68 (-395.18; session marks -385.85) · 3 name(s) marked open→close (per-name table). QFIN×443 09:30 $9.15 → close $8.80 -155.05; DY×7 09:30 $306.34 → close $294.34 -84.00; BURL×8 09:30 $291.30 → close $272.95 -146.80 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $406.12 | ▼ 09:30 equity $8,510.29 vs yday $8,548.50 (-38.21) | 09:30 open · cash $406.12 (unchanged overnight, no fees) · equity $8,510.29 vs prior close $8,548.50 (-38.21) · 3 name(s) re-marked at the open (per-name table). QFIN×443 yday $8.80 → 09:30 $8.70 -44.30; DY×7 yday $294.34 → 09:30 $298.01 +25.69; BURL×8 yday $272.95 → 09:30 $270.50 -19.60 | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 443 | $8.70 | $5.82 | $-1070.30 | $4,254.40 | ▼ -1,070.30 after sell → book $8,504.47; vs 09:30 mark -5.82 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 7 | $298.01 | $2.04 | $-62.36 | $6,338.43 | ▼ -62.36 after sell → book $8,502.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BURL` | 8 | $270.50 | $2.04 | $-170.46 | $8,500.39 | ▼ -170.46 after sell → book $8,500.39; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,500.39 | ▲ close $8,500.39 vs 09:30 $8,510.29 (session +0.00) | 16:00 close · cash $8,500.39 · no lots left · equity $8,500.39. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,500.39 | ▲ 09:30 equity $8,500.39 vs yday $8,500.39 (+0.00) | 09:30 open · cash $8,500.39 · no holdings · equity $8,500.39 vs prior close $8,500.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,500.39 | ▲ close $8,500.39 vs 09:30 $8,500.39 (session +0.00) | 16:00 close · cash $8,500.39 · no lots left · equity $8,500.39. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,500.39 | ▲ 09:30 equity $8,500.39 vs yday $8,500.39 (+0.00) | 09:30 open · cash $8,500.39 · no holdings · equity $8,500.39 vs prior close $8,500.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,500.39 | ▲ close $8,500.39 vs 09:30 $8,500.39 (session +0.00) | 16:00 close · cash $8,500.39 · no lots left · equity $8,500.39. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,500.39 | ▲ 09:30 equity $8,500.39 vs yday $8,500.39 (+0.00) | 09:30 open · cash $8,500.39 · no holdings · equity $8,500.39 vs prior close $8,500.39 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 166 | $7.31 | $2.49 | — | $7,284.44 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer; 🔵; ret5=+18.5; leftover $1214.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 117 | $10.38 | $2.34 | — | $6,068.23 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; 🔵; ret5=-56.2; leftover $1214.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `LX` | 1468 | $0.83 | $16.54 | — | $4,837.65 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-30.4; leftover $1214.34 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EVTL` | 1897 | $0.64 | $17.83 | — | $3,605.74 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.0; leftover $1214.34 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FJET` | 427 | $2.84 | $5.51 | — | $2,387.55 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.9; leftover $1214.34 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OSW` | 55 | $22.00 | $2.15 | — | $1,175.39 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-17.3; leftover $1214.34 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PCG` | 87 | $13.35 | $2.25 | — | $11.69 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; ret5=-26.8; leftover $1214.34 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.69 | ▲ close $8,476.68 vs 09:30 $8,500.39 (session +25.40) | 16:00 close · cash $11.69 · equity $8,476.68 vs 09:30 $8,500.39 (-23.71; session marks +25.40) · 7 name(s) marked open→close (per-name table). SION×166 09:30 $7.31 → close $6.75 -92.96; ALMS×117 09:30 $10.38 → close $11.36 +115.24; LX×1468 09:30 $0.83 → close $0.85 +36.70; EVTL×1897 09:30 $0.64 → close $0.60 -75.88; FJET×427 09:30 $2.84 → close $2.78 -25.62; OSW×55 09:30 $22.00 → close $22.27 +14.85; PCG×87 09:30 $13.35 → close $13.96 +53.07 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.69 | ▼ 09:30 equity $8,453.28 vs yday $8,476.68 (-23.40) | 09:30 open · cash $11.69 (unchanged overnight, no fees) · equity $8,453.28 vs prior close $8,476.68 (-23.40) · 7 name(s) re-marked at the open (per-name table). SION×166 yday $6.75 → 09:30 $6.68 -11.62; ALMS×117 yday $11.36 → 09:30 $11.23 -15.21; LX×1468 yday $0.85 → 09:30 $0.86 +8.81; EVTL×1897 yday $0.60 → 09:30 $0.60 +0.00; FJET×427 yday $2.78 → 09:30 $2.80 +8.54; OSW×55 yday $22.27 → 09:30 $22.27 +0.00; PCG×87 yday $13.96 → 09:30 $13.80 -13.92 | — |
| 2026-09-04 09:30 ET | **SELL** | `LX` | 1468 | $0.86 | $17.25 | $+11.71 | $1,253.98 | ▲ +11.71 after sell → book $8,436.02; vs 09:30 mark -17.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EVTL` | 1897 | $0.60 | $17.40 | $-111.11 | $2,374.78 | ▼ -111.11 after sell → book $8,418.62; vs 09:30 mark -17.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FJET` | 427 | $2.80 | $5.59 | $-28.18 | $3,564.80 | ▼ -28.18 after sell → book $8,413.04; vs 09:30 mark -5.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OSW` | 55 | $22.27 | $2.17 | $+10.52 | $4,787.47 | ▲ +10.52 after sell → book $8,410.86; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PCG` | 87 | $13.80 | $2.28 | $+34.62 | $5,985.80 | ▲ +34.62 after sell → book $8,408.59; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AIIO` | 1140 | $1.75 | $14.71 | — | $3,976.09 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.6; leftover $1995.27 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SWBI` | 141 | $14.12 | $2.41 | — | $1,982.76 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; ret5=-6.1; leftover $1995.27 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMX` | 85 | $23.03 | $2.25 | — | $22.96 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-1.4; leftover $1995.27 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.96 | ▼ close $8,383.63 vs 09:30 $8,453.28 (session -5.59) | 16:00 close · cash $22.96 · equity $8,383.63 vs 09:30 $8,453.28 (-69.65; session marks -5.59) · 5 name(s) marked open→close (per-name table). SION×166 09:30 $6.68 → close $7.18 +83.00; ALMS×117 09:30 $11.23 → close $11.10 -15.21; AIIO×1140 09:30 $1.75 → close $1.84 +102.60; SWBI×141 09:30 $14.12 → close $12.89 -173.43; AMX×85 09:30 $23.03 → close $23.00 -2.55 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.96 | ▼ 09:30 equity $8,294.45 vs yday $8,383.63 (-89.18) | 09:30 open · cash $22.96 (unchanged overnight, no fees) · equity $8,294.45 vs prior close $8,383.63 (-89.18) · 5 name(s) re-marked at the open (per-name table). SION×166 yday $7.18 → 09:30 $7.13 -8.30; ALMS×117 yday $11.10 → 09:30 $11.05 -5.85; AIIO×1140 yday $1.84 → 09:30 $1.81 -34.20; SWBI×141 yday $12.89 → 09:30 $12.51 -53.58; AMX×85 yday $23.00 → 09:30 $23.15 +12.75 | — |
| 2026-09-08 09:30 ET | **SELL** | `SION` | 166 | $7.13 | $2.53 | $-34.89 | $1,204.02 | ▼ -34.89 after sell → book $8,291.93; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALMS` | 117 | $11.05 | $2.37 | $+74.26 | $2,494.49 | ▲ +74.26 after sell → book $8,289.55; vs 09:30 mark -2.38 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AIIO` | 1140 | $1.81 | $14.91 | $+38.78 | $4,542.98 | ▲ +38.78 after sell → book $8,274.64; vs 09:30 mark -14.91 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AMX` | 85 | $23.15 | $2.27 | $+5.68 | $6,508.46 | ▲ +5.68 after sell → book $8,272.37; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,508.46 | ▲ close $8,373.89 vs 09:30 $8,294.45 (session +101.52) | 16:00 close · cash $6,508.46 · equity $8,373.89 vs 09:30 $8,294.45 (+79.44; session marks +101.52) · 1 name(s) marked open→close (per-name table). SWBI×141 09:30 $12.51 → close $13.23 +101.52 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,508.46 | ▼ 09:30 equity $8,358.38 vs yday $8,373.89 (-15.51) | 09:30 open · cash $6,508.46 (unchanged overnight, no fees) · equity $8,358.38 vs prior close $8,373.89 (-15.51) · 1 name(s) re-marked at the open (per-name table). SWBI×141 yday $13.23 → 09:30 $13.12 -15.51 | — |
| 2026-09-09 09:30 ET | **SELL** | `SWBI` | 141 | $13.12 | $2.45 | $-145.86 | $8,355.93 | ▼ -145.86 after sell → book $8,355.93; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,355.93 | ▲ close $8,355.93 vs 09:30 $8,358.38 (session +0.00) | 16:00 close · cash $8,355.93 · no lots left · equity $8,355.93. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,355.93 | ▲ 09:30 equity $8,355.93 vs yday $8,355.93 (-0.00) | 09:30 open · cash $8,355.93 · no holdings · equity $8,355.93 vs prior close $8,355.93 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,355.93 | ▲ close $8,355.93 vs 09:30 $8,355.93 (session +0.00) | 16:00 close · cash $8,355.93 · no lots left · equity $8,355.93. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,355.93 | ▲ 09:30 equity $8,355.93 vs yday $8,355.93 (-0.00) | 09:30 open · cash $8,355.93 · no holdings · equity $8,355.93 vs prior close $8,355.93 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 57 | $20.61 | $2.16 | — | $7,179.00 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; 🔵; ret5=-24.7; leftover $1193.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RWT` | 339 | $3.52 | $4.37 | — | $5,981.34 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-19.2; leftover $1193.70 | join🔴 sector🔴 gen🟡 news🔴 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 21 | $54.66 | $2.05 | — | $4,831.43 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; ret5=-22.3; leftover $1193.70 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `TTAN` | 21 | $55.91 | $2.05 | — | $3,655.27 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-39.2; leftover $1193.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ENB` | 24 | $48.37 | $2.06 | — | $2,492.32 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; ret5=-0.2; leftover $1193.70 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `WAFD` | 34 | $34.44 | $2.09 | — | $1,319.27 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; ret5=-4.1; leftover $1193.70 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `KMB` | 12 | $99.24 | $2.03 | — | $126.37 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=-4.7; leftover $1193.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.37 | ▼ close $8,284.31 vs 09:30 $8,355.93 (session -54.80) | 16:00 close · cash $126.37 · equity $8,284.31 vs 09:30 $8,355.93 (-71.62; session marks -54.80) · 7 name(s) marked open→close (per-name table). NAVN×57 09:30 $20.61 → close $21.02 +23.37; RWT×339 09:30 $3.52 → close $3.55 +10.17; COO×21 09:30 $54.66 → close $53.91 -15.75; TTAN×21 09:30 $55.91 → close $54.68 -25.83; ENB×24 09:30 $48.37 → close $47.76 -14.64; WAFD×34 09:30 $34.44 → close $33.88 -19.04; KMB×12 09:30 $99.24 → close $98.15 -13.08 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.37 | ▲ 09:30 equity $8,316.77 vs yday $8,284.31 (+32.46) | 09:30 open · cash $126.37 (unchanged overnight, no fees) · equity $8,316.77 vs prior close $8,284.31 (+32.46) · 7 name(s) re-marked at the open (per-name table). NAVN×57 yday $21.02 → 09:30 $21.10 +4.56; RWT×339 yday $3.55 → 09:30 $3.53 -6.78; COO×21 yday $53.91 → 09:30 $54.78 +18.27; TTAN×21 yday $54.68 → 09:30 $54.77 +1.89; ENB×24 yday $47.76 → 09:30 $47.85 +2.16; WAFD×34 yday $33.88 → 09:30 $33.88 +0.00; KMB×12 yday $98.15 → 09:30 $99.18 +12.36 | — |
| 2026-09-14 09:30 ET | **SELL** | `NAVN` | 57 | $21.10 | $2.18 | $+23.59 | $1,326.89 | ▲ +23.59 after sell → book $8,314.59; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RWT` | 339 | $3.53 | $4.44 | $-5.42 | $2,519.12 | ▼ -5.42 after sell → book $8,310.15; vs 09:30 mark -4.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ENB` | 24 | $47.85 | $2.08 | $-16.62 | $3,665.43 | ▼ -16.62 after sell → book $8,308.06; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WAFD` | 34 | $33.88 | $2.11 | $-23.24 | $4,815.24 | ▼ -23.24 after sell → book $8,305.95; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `KMB` | 12 | $99.18 | $2.05 | $-4.79 | $6,003.36 | ▼ -4.79 after sell → book $8,303.91; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,003.36 | ▲ close $8,380.77 vs 09:30 $8,316.77 (session +76.86) | 16:00 close · cash $6,003.36 · equity $8,380.77 vs 09:30 $8,316.77 (+64.00; session marks +76.86) · 2 name(s) marked open→close (per-name table). COO×21 09:30 $54.78 → close $54.22 -11.76; TTAN×21 09:30 $54.77 → close $58.99 +88.62 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,003.36 | ▼ 09:30 equity $8,359.14 vs yday $8,380.77 (-21.63) | 09:30 open · cash $6,003.36 (unchanged overnight, no fees) · equity $8,359.14 vs prior close $8,380.77 (-21.63) · 2 name(s) re-marked at the open (per-name table). COO×21 yday $54.22 → 09:30 $54.40 +3.78; TTAN×21 yday $58.99 → 09:30 $57.78 -25.41 | — |
| 2026-09-15 09:30 ET | **SELL** | `COO` | 21 | $54.40 | $2.07 | $-9.59 | $7,143.68 | ▼ -9.59 after sell → book $8,357.06; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,143.68 | ▲ close $8,374.28 vs 09:30 $8,359.14 (session +17.22) | 16:00 close · cash $7,143.68 · equity $8,374.28 vs 09:30 $8,359.14 (+15.14; session marks +17.22) · 1 name(s) marked open→close (per-name table). TTAN×21 09:30 $57.78 → close $58.60 +17.22 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,143.68 | ▼ 09:30 equity $8,341.52 vs yday $8,374.28 (-32.76) | 09:30 open · cash $7,143.68 (unchanged overnight, no fees) · equity $8,341.52 vs prior close $8,374.28 (-32.76) · 1 name(s) re-marked at the open (per-name table). TTAN×21 yday $58.60 → 09:30 $57.04 -32.76 | — |
| 2026-09-16 09:30 ET | **SELL** | `TTAN` | 21 | $57.04 | $2.07 | $+19.60 | $8,339.45 | ▲ +19.60 after sell → book $8,339.45; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 101 | $10.30 | $2.29 | — | $7,296.86 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; ret5=-23.0; leftover $1042.43 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `PLAY` | 151 | $6.86 | $2.44 | — | $6,258.55 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; ret5=-22.4; leftover $1042.43 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `DVLT` | 6515 | $0.16 | $29.97 | — | $5,186.19 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-23.8; leftover $1042.43 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `NMRA` | 1235 | $0.84 | $14.13 | — | $4,129.72 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-33.5; leftover $1042.43 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ZSQR` | 445 | $2.34 | $5.74 | — | $3,082.68 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-25.2; leftover $1042.43 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `CTMX` | 383 | $2.72 | $4.94 | — | $2,035.98 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.3; leftover $1042.43 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `CRBP` | 151 | $6.86 | $2.44 | — | $997.67 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-34.7; leftover $1042.43 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `EYPT` | 271 | $3.66 | $3.50 | — | $2.32 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-19.7; leftover $1042.43 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.32 | ▼ close $8,154.50 vs 09:30 $8,341.52 (session -119.50) | 16:00 close · cash $2.32 · equity $8,154.50 vs 09:30 $8,341.52 (-187.02; session marks -119.50) · 8 name(s) marked open→close (per-name table). ALHC×101 09:30 $10.30 → close $8.71 -160.59; PLAY×151 09:30 $6.86 → close $6.86 +0.00; DVLT×6515 09:30 $0.16 → close $0.18 +130.30; NMRA×1235 09:30 $0.84 → close $0.77 -87.68; ZSQR×445 09:30 $2.34 → close $2.29 -22.25; CTMX×383 09:30 $2.72 → close $2.77 +17.23; CRBP×151 09:30 $6.86 → close $7.26 +60.40; EYPT×271 09:30 $3.66 → close $3.45 -56.91 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.32 | ▲ 09:30 equity $8,184.08 vs yday $8,154.50 (+29.58) | 09:30 open · cash $2.32 (unchanged overnight, no fees) · equity $8,184.08 vs prior close $8,154.50 (+29.58) · 8 name(s) re-marked at the open (per-name table). ALHC×101 yday $8.71 → 09:30 $8.58 -13.13; PLAY×151 yday $6.86 → 09:30 $6.96 +15.10; DVLT×6515 yday $0.18 → 09:30 $0.17 -65.15; NMRA×1235 yday $0.77 → 09:30 $0.78 +8.65; ZSQR×445 yday $2.29 → 09:30 $2.35 +26.70; CTMX×383 yday $2.77 → 09:30 $2.83 +24.89; CRBP×151 yday $7.26 → 09:30 $7.26 +0.00; EYPT×271 yday $3.45 → 09:30 $3.57 +32.52 | — |
| 2026-09-17 09:30 ET | **SELL** | `DVLT` | 6515 | $0.17 | $31.71 | $+3.47 | $1,078.16 | ▲ +3.47 after sell → book $8,152.37; vs 09:30 mark -31.71 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `NMRA` | 1235 | $0.78 | $13.55 | $-106.72 | $2,027.90 | ▼ -106.72 after sell → book $8,138.81; vs 09:30 mark -13.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ZSQR` | 445 | $2.35 | $5.82 | $-7.11 | $3,067.83 | ▼ -7.11 after sell → book $8,132.99; vs 09:30 mark -5.82 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CTMX` | 383 | $2.83 | $5.01 | $+32.17 | $4,146.70 | ▲ +32.17 after sell → book $8,127.97; vs 09:30 mark -5.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CRBP` | 151 | $7.26 | $2.48 | $+55.48 | $5,240.49 | ▲ +55.48 after sell → book $8,125.50; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `EYPT` | 271 | $3.57 | $3.55 | $-31.44 | $6,204.40 | ▼ -31.44 after sell → book $8,121.94; vs 09:30 mark -3.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `MRLN` | 1366 | $2.27 | $17.62 | — | $3,085.96 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-29.6; leftover $3102.20 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `HBAN` | 193 | $15.90 | $2.57 | — | $14.69 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=+0.4; leftover $3102.20 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.69 | ▼ close $7,759.73 vs 09:30 $8,184.08 (session -342.02) | 16:00 close · cash $14.69 · equity $7,759.73 vs 09:30 $8,184.08 (-424.35; session marks -342.02) · 4 name(s) marked open→close (per-name table). ALHC×101 09:30 $8.58 → close $8.70 +12.12; PLAY×151 09:30 $6.96 → close $6.54 -63.42; MRLN×1366 09:30 $2.27 → close $2.06 -286.86; HBAN×193 09:30 $15.90 → close $15.88 -3.86 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.69 | ▲ 09:30 equity $7,782.61 vs yday $7,759.73 (+22.88) | 09:30 open · cash $14.69 (unchanged overnight, no fees) · equity $7,782.61 vs prior close $7,759.73 (+22.88) · 4 name(s) re-marked at the open (per-name table). ALHC×101 yday $8.70 → 09:30 $8.68 -2.02; PLAY×151 yday $6.54 → 09:30 $6.64 +15.10; MRLN×1366 yday $2.06 → 09:30 $2.07 +13.66; HBAN×193 yday $15.88 → 09:30 $15.86 -3.86 | — |
| 2026-09-18 09:30 ET | **SELL** | `PLAY` | 151 | $6.64 | $2.48 | $-38.14 | $1,014.86 | ▼ -38.14 after sell → book $7,780.14; vs 09:30 mark -2.47 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `MRLN` | 1366 | $2.07 | $17.87 | $-308.69 | $3,824.61 | ▼ -308.69 after sell → book $7,762.27; vs 09:30 mark -17.87 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `HBAN` | 193 | $15.86 | $2.63 | $-12.91 | $6,882.96 | ▼ -12.91 after sell → book $7,759.64; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 93 | $14.79 | $2.27 | — | $5,505.22 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1376.59 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 182 | $7.54 | $2.54 | — | $4,131.31 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; 🔵; ret5=-20.9; leftover $1376.59 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `MNR` | 125 | $10.95 | $2.37 | — | $2,760.20 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; ret5=+1.7; leftover $1376.59 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `MTZ` | 6 | $210.53 | $2.01 | — | $1,495.01 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=+3.5; leftover $1376.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TTAN` | 25 | $53.53 | $2.06 | — | $154.57 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; ⚪; ret5=-41.3; leftover $1376.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.57 | ▼ close $7,733.12 vs 09:30 $7,782.61 (session -15.27) | 16:00 close · cash $154.57 · equity $7,733.12 vs 09:30 $7,782.61 (-49.49; session marks -15.27) · 6 name(s) marked open→close (per-name table). ALHC×101 09:30 $8.68 → close $8.35 -33.33; RARE×93 09:30 $14.79 → close $14.51 -26.04; FLNC×182 09:30 $7.54 → close $7.32 -39.13; MNR×125 09:30 $10.95 → close $11.13 +22.50; MTZ×6 09:30 $210.53 → close $214.38 +23.10; TTAN×25 09:30 $53.53 → close $55.04 +37.63 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `EYPT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `BHF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MNSO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BHF` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `CLBT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OPLN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-02 | `EIX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PCG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `COLL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XPOF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SFD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `RCUS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CLX` | hard_red | hard-red S=-11.00 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ALHC` | 101 | 2026-09-16 @ $10.30 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; ret5=-23.0; leftover $1042.43 |
| `RARE` | 93 | 2026-09-18 @ $14.79 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1376.59 |
| `FLNC` | 182 | 2026-09-18 @ $7.54 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover,oppset; 🔵; ret5=-20.9; leftover $1376.59 |
| `MNR` | 125 | 2026-09-18 @ $10.95 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; ret5=+1.7; leftover $1376.59 |
| `MTZ` | 6 | 2026-09-18 @ $210.53 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; 🔵; ret5=+3.5; leftover $1376.59 |
| `TTAN` | 25 | 2026-09-18 @ $53.53 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list oppset; ⚪; ret5=-41.3; leftover $1376.59 |
