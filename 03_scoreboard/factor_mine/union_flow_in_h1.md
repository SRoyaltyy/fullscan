# Factor mine action — `union_flow_in_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ flow_in, no 🚨

Cash book **+4.73%** ($10,473) · signal-only (no cash/fees) was +18.57%. Starts YES **6/26**. Fills 138 · skips 46 · realized $+835.01.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: money came in (prior rel vol ≥ 1.5) but price barely moved (|1-day| ≤ 1.2%).
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
- **Gate** `flow_in=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $3.89.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TPG` | 197 | — | $50.62 | +0.00 | $54.62 | +787.37 | +787.37 | +0.00 | +787.37 |
| 2026-08-14 | `TPG` | 197 | $54.62 | $55.29 | +131.99 | — | +0.00 | +131.99 | +919.36 | — |
| 2026-08-14 | `SPHR` | 7 | — | $176.68 | +0.00 | $168.00 | -60.76 | -60.76 | +0.00 | -60.76 |
| 2026-08-14 | `KULR` | 545 | — | $2.50 | +0.00 | $2.64 | +76.30 | +76.30 | +0.00 | +76.30 |
| 2026-08-14 | `NPWR` | 874 | — | $1.56 | +0.00 | $1.95 | +340.86 | +340.86 | +0.00 | +340.86 |
| 2026-08-14 | `RLX` | 737 | — | $1.85 | +0.00 | $1.94 | +66.33 | +66.33 | +0.00 | +66.33 |
| 2026-08-14 | `BSBR` | 235 | — | $5.79 | +0.00 | $5.77 | -4.70 | -4.70 | +0.00 | -4.70 |
| 2026-08-14 | `ENB` | 26 | — | $51.15 | +0.00 | $50.91 | -6.24 | -6.24 | +0.00 | -6.24 |
| 2026-08-14 | `RUM` | 181 | — | $7.51 | +0.00 | $7.46 | -9.05 | -9.05 | +0.00 | -9.05 |
| 2026-08-14 | `JBTM` | 11 | — | $117.42 | +0.00 | $120.18 | +30.36 | +30.36 | +0.00 | +30.36 |
| 2026-08-17 | `SPHR` | 7 | $168.00 | $168.10 | +0.70 | — | +0.00 | +0.70 | -60.06 | — |
| 2026-08-17 | `KULR` | 545 | $2.64 | $2.63 | -5.45 | — | +0.00 | -5.45 | +70.85 | — |
| 2026-08-17 | `NPWR` | 874 | $1.95 | $1.92 | -26.22 | — | +0.00 | -26.22 | +314.64 | — |
| 2026-08-17 | `RLX` | 737 | $1.94 | $1.92 | -14.74 | — | +0.00 | -14.74 | +51.59 | — |
| 2026-08-17 | `BSBR` | 235 | $5.77 | $5.78 | +2.35 | — | +0.00 | +2.35 | -2.35 | — |
| 2026-08-17 | `ENB` | 26 | $50.91 | $50.80 | -2.86 | — | +0.00 | -2.86 | -9.10 | — |
| 2026-08-17 | `RUM` | 181 | $7.46 | $7.43 | -5.43 | — | +0.00 | -5.43 | -14.48 | — |
| 2026-08-17 | `JBTM` | 11 | $120.18 | $119.18 | -11.00 | — | +0.00 | -11.00 | +19.36 | — |
| 2026-08-17 | `VIV` | 485 | — | $11.55 | +0.00 | $11.40 | -72.75 | -72.75 | +0.00 | -72.75 |
| 2026-08-17 | `WBS` | 70 | — | $79.00 | +0.00 | $78.69 | -21.70 | -21.70 | +0.00 | -21.70 |
| 2026-08-18 | `VIV` | 485 | $11.40 | $11.45 | +24.25 | — | +0.00 | +24.25 | -48.50 | — |
| 2026-08-18 | `WBS` | 70 | $78.69 | $78.52 | -11.90 | — | +0.00 | -11.90 | -33.60 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `FUTU` | 11 | — | $117.65 | +0.00 | $112.73 | -54.12 | -54.12 | +0.00 | -54.12 |
| 2026-08-20 | `WMT` | 13 | — | $106.38 | +0.00 | $103.84 | -33.02 | -33.02 | +0.00 | -33.02 |
| 2026-08-20 | `WBS` | 17 | — | $77.57 | +0.00 | $77.57 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `ALH` | 58 | — | $23.72 | +0.00 | $23.18 | -31.32 | -31.32 | +0.00 | -31.32 |
| 2026-08-20 | `NTST` | 67 | — | $20.47 | +0.00 | $20.61 | +9.38 | +9.38 | +0.00 | +9.38 |
| 2026-08-20 | `ADC` | 18 | — | $74.37 | +0.00 | $74.45 | +1.44 | +1.44 | +0.00 | +1.44 |
| 2026-08-20 | `SAN` | 97 | — | $14.30 | +0.00 | $14.21 | -8.73 | -8.73 | +0.00 | -8.73 |
| 2026-08-20 | `FBRX` | 18 | — | $76.93 | +0.00 | $76.89 | -0.72 | -0.72 | +0.00 | -0.72 |
| 2026-08-21 | `FUTU` | 11 | $112.73 | $115.18 | +26.95 | — | +0.00 | +26.95 | -27.17 | — |
| 2026-08-21 | `WMT` | 13 | $103.84 | $103.69 | -1.95 | — | +0.00 | -1.95 | -34.97 | — |
| 2026-08-21 | `WBS` | 17 | $77.57 | $77.57 | +0.00 | — | +0.00 | +0.00 | +0.00 | — |
| 2026-08-21 | `ALH` | 58 | $23.18 | $23.33 | +8.70 | — | +0.00 | +8.70 | -22.62 | — |
| 2026-08-21 | `NTST` | 67 | $20.61 | $20.66 | +3.35 | — | +0.00 | +3.35 | +12.73 | — |
| 2026-08-21 | `ADC` | 18 | $74.45 | $74.60 | +2.70 | — | +0.00 | +2.70 | +4.14 | — |
| 2026-08-21 | `SAN` | 97 | $14.21 | $14.55 | +32.98 | — | +0.00 | +32.98 | +24.25 | — |
| 2026-08-21 | `FBRX` | 18 | $76.89 | $76.94 | +0.90 | — | +0.00 | +0.90 | +0.18 | — |
| 2026-08-21 | `BJ` | 39 | — | $93.98 | +0.00 | $96.42 | +95.16 | +95.16 | +0.00 | +95.16 |
| 2026-08-21 | `HITI` | 1512 | — | $2.43 | +0.00 | $2.45 | +30.24 | +30.24 | +0.00 | +30.24 |
| 2026-08-21 | `VIK` | 40 | — | $91.00 | +0.00 | $92.79 | +71.60 | +71.60 | +0.00 | +71.60 |
| 2026-08-24 | `BJ` | 39 | $96.42 | $97.02 | +23.40 | — | +0.00 | +23.40 | +118.56 | — |
| 2026-08-24 | `HITI` | 1512 | $2.45 | $2.45 | +0.00 | — | +0.00 | +0.00 | +30.24 | — |
| 2026-08-24 | `VIK` | 40 | $92.79 | $93.06 | +10.80 | — | +0.00 | +10.80 | +82.40 | — |
| 2026-08-25 | `ZYME` | 55 | — | $28.86 | +0.00 | $27.47 | -76.45 | -76.45 | +0.00 | -76.45 |
| 2026-08-25 | `RHI` | 36 | — | $43.76 | +0.00 | $44.90 | +41.04 | +41.04 | +0.00 | +41.04 |
| 2026-08-25 | `ABUS` | 305 | — | $5.25 | +0.00 | $5.20 | -15.25 | -15.25 | +0.00 | -15.25 |
| 2026-08-25 | `AMX` | 67 | — | $23.80 | +0.00 | $23.75 | -3.35 | -3.35 | +0.00 | -3.35 |
| 2026-08-25 | `SAN` | 109 | — | $14.65 | +0.00 | $14.63 | -2.18 | -2.18 | +0.00 | -2.18 |
| 2026-08-25 | `DBRG` | 100 | — | $15.98 | +0.00 | $15.97 | -1.00 | -1.00 | +0.00 | -1.00 |
| 2026-08-25 | `ASO` | 35 | — | $44.57 | +0.00 | $43.48 | -38.32 | -38.32 | +0.00 | -38.32 |
| 2026-08-26 | `ZYME` | 55 | $27.47 | $27.56 | +4.95 | — | +0.00 | +4.95 | -71.50 | — |
| 2026-08-26 | `RHI` | 36 | $44.90 | $44.33 | -20.52 | — | +0.00 | -20.52 | +20.52 | — |
| 2026-08-26 | `ABUS` | 305 | $5.20 | $5.19 | -3.05 | $5.19 | +0.00 | -3.05 | -18.30 | -18.30 |
| 2026-08-26 | `AMX` | 67 | $23.75 | $23.75 | +0.00 | — | +0.00 | +0.00 | -3.35 | — |
| 2026-08-26 | `SAN` | 109 | $14.63 | $14.82 | +20.71 | — | +0.00 | +20.71 | +18.53 | — |
| 2026-08-26 | `DBRG` | 100 | $15.97 | $15.97 | +0.00 | $15.97 | +0.00 | +0.00 | -1.00 | -1.00 |
| 2026-08-26 | `ASO` | 35 | $43.48 | $43.40 | -2.63 | — | +0.00 | -2.63 | -40.95 | — |
| 2026-08-26 | `NCNO` | 68 | — | $19.33 | +0.00 | $21.51 | +148.24 | +148.24 | +0.00 | +148.24 |
| 2026-08-26 | `PLAB` | 35 | — | $37.26 | +0.00 | $30.25 | -245.35 | -245.35 | +0.00 | -245.35 |
| 2026-08-26 | `SJM` | 9 | — | $134.80 | +0.00 | $130.90 | -35.10 | -35.10 | +0.00 | -35.10 |
| 2026-08-26 | `SLF` | 16 | — | $79.20 | +0.00 | $79.05 | -2.40 | -2.40 | +0.00 | -2.40 |
| 2026-08-26 | `VIPS` | 94 | — | $14.00 | +0.00 | $14.08 | +7.52 | +7.52 | +0.00 | +7.52 |
| 2026-08-26 | `ATHM` | 60 | — | $21.74 | +0.00 | $22.17 | +25.80 | +25.80 | +0.00 | +25.80 |
| 2026-08-27 | `ABUS` | 305 | $5.19 | $5.16 | -9.15 | — | +0.00 | -9.15 | -27.45 | — |
| 2026-08-27 | `DBRG` | 100 | $15.97 | $15.96 | -1.00 | — | +0.00 | -1.00 | -2.00 | — |
| 2026-08-27 | `NCNO` | 68 | $21.51 | $22.03 | +35.36 | — | +0.00 | +35.36 | +183.60 | — |
| 2026-08-27 | `PLAB` | 35 | $30.25 | $30.12 | -4.55 | — | +0.00 | -4.55 | -249.90 | — |
| 2026-08-27 | `SJM` | 9 | $130.90 | $130.29 | -5.49 | — | +0.00 | -5.49 | -40.59 | — |
| 2026-08-27 | `SLF` | 16 | $79.05 | $78.45 | -9.60 | — | +0.00 | -9.60 | -12.00 | — |
| 2026-08-27 | `VIPS` | 94 | $14.08 | $14.00 | -7.52 | — | +0.00 | -7.52 | +0.00 | — |
| 2026-08-27 | `ATHM` | 60 | $22.17 | $22.08 | -5.40 | — | +0.00 | -5.40 | +20.40 | — |
| 2026-08-27 | `TRLV` | 480 | — | $11.38 | +0.00 | $11.03 | -168.00 | -168.00 | +0.00 | -168.00 |
| 2026-08-27 | `BOX` | 161 | — | $33.79 | +0.00 | $34.74 | +152.95 | +152.95 | +0.00 | +152.95 |
| 2026-08-28 | `TRLV` | 480 | $11.03 | $11.00 | -14.40 | — | +0.00 | -14.40 | -182.40 | — |
| 2026-08-28 | `BOX` | 161 | $34.74 | $34.75 | +1.61 | — | +0.00 | +1.61 | +154.56 | — |
| 2026-08-28 | `JKS` | 135 | — | $13.37 | +0.00 | $13.54 | +22.95 | +22.95 | +0.00 | +22.95 |
| 2026-08-28 | `DY` | 5 | — | $306.34 | +0.00 | $294.34 | -60.00 | -60.00 | +0.00 | -60.00 |
| 2026-08-28 | `ULTA` | 3 | — | $542.00 | +0.00 | $517.50 | -73.50 | -73.50 | +0.00 | -73.50 |
| 2026-08-28 | `PLAB` | 60 | — | $30.01 | +0.00 | $27.73 | -136.80 | -136.80 | +0.00 | -136.80 |
| 2026-08-28 | `JAZZ` | 7 | — | $249.48 | +0.00 | $244.54 | -34.58 | -34.58 | +0.00 | -34.58 |
| 2026-08-28 | `WSM` | 7 | — | $235.67 | +0.00 | $235.09 | -4.06 | -4.06 | +0.00 | -4.06 |
| 2026-08-31 | `JKS` | 135 | $13.54 | $13.54 | +0.00 | — | +0.00 | +0.00 | +22.95 | — |
| 2026-08-31 | `DY` | 5 | $294.34 | $298.01 | +18.35 | — | +0.00 | +18.35 | -41.65 | — |
| 2026-08-31 | `ULTA` | 3 | $517.50 | $521.10 | +10.80 | — | +0.00 | +10.80 | -62.70 | — |
| 2026-08-31 | `PLAB` | 60 | $27.73 | $28.04 | +18.60 | — | +0.00 | +18.60 | -118.20 | — |
| 2026-08-31 | `JAZZ` | 7 | $244.54 | $241.39 | -22.05 | — | +0.00 | -22.05 | -56.63 | — |
| 2026-08-31 | `WSM` | 7 | $235.09 | $232.06 | -21.21 | — | +0.00 | -21.21 | -25.27 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `VIR` | 114 | — | $11.54 | +0.00 | $11.45 | -10.26 | -10.26 | +0.00 | -10.26 |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `FIVE` | 5 | — | $257.00 | +0.00 | $239.96 | -85.20 | -85.20 | +0.00 | -85.20 |
| 2026-09-03 | `MOMO` | 240 | — | $5.50 | +0.00 | $5.10 | -96.00 | -96.00 | +0.00 | -96.00 |
| 2026-09-03 | `PVH` | 17 | — | $74.96 | +0.00 | $72.46 | -42.50 | -42.50 | +0.00 | -42.50 |
| 2026-09-03 | `VSXY` | 17 | — | $76.86 | +0.00 | $73.64 | -54.74 | -54.74 | +0.00 | -54.74 |
| 2026-09-03 | `GBTG` | 139 | — | $9.49 | +0.00 | $9.49 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `DAKT` | 69 | — | $19.08 | +0.00 | $19.48 | +27.60 | +27.60 | +0.00 | +27.60 |
| 2026-09-04 | `VIR` | 114 | $11.45 | $11.31 | -15.96 | — | +0.00 | -15.96 | -26.22 | — |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | — | +0.00 | +7.62 | +23.88 | — |
| 2026-09-04 | `FIVE` | 5 | $239.96 | $238.88 | -5.40 | — | +0.00 | -5.40 | -90.60 | — |
| 2026-09-04 | `MOMO` | 240 | $5.10 | $5.13 | +7.20 | — | +0.00 | +7.20 | -88.80 | — |
| 2026-09-04 | `PVH` | 17 | $72.46 | $72.79 | +5.61 | — | +0.00 | +5.61 | -36.89 | — |
| 2026-09-04 | `VSXY` | 17 | $73.64 | $73.63 | -0.17 | — | +0.00 | -0.17 | -54.91 | — |
| 2026-09-04 | `GBTG` | 139 | $9.49 | $9.49 | +0.00 | — | +0.00 | +0.00 | +0.00 | — |
| 2026-09-04 | `DAKT` | 69 | $19.48 | $19.47 | -0.69 | — | +0.00 | -0.69 | +26.91 | — |
| 2026-09-04 | `ATRC` | 24 | — | $52.03 | +0.00 | $51.52 | -12.24 | -12.24 | +0.00 | -12.24 |
| 2026-09-04 | `WNC` | 90 | — | $14.17 | +0.00 | $14.31 | +12.60 | +12.60 | +0.00 | +12.60 |
| 2026-09-04 | `CRDO` | 7 | — | $162.10 | +0.00 | $170.57 | +59.29 | +59.29 | +0.00 | +59.29 |
| 2026-09-04 | `ADCT` | 991 | — | $1.30 | +0.00 | $1.36 | +59.46 | +59.46 | +0.00 | +59.46 |
| 2026-09-04 | `HAFN` | 144 | — | $8.94 | +0.00 | $9.22 | +40.32 | +40.32 | +0.00 | +40.32 |
| 2026-09-04 | `DOCU` | 18 | — | $68.52 | +0.00 | $68.41 | -1.98 | -1.98 | +0.00 | -1.98 |
| 2026-09-04 | `XP` | 65 | — | $19.67 | +0.00 | $19.86 | +12.35 | +12.35 | +0.00 | +12.35 |
| 2026-09-04 | `MMED` | 54 | — | $23.84 | +0.00 | $23.29 | -29.70 | -29.70 | +0.00 | -29.70 |
| 2026-09-08 | `ATRC` | 24 | $51.52 | $54.31 | +66.96 | — | +0.00 | +66.96 | +54.72 | — |
| 2026-09-08 | `WNC` | 90 | $14.31 | $14.22 | -8.10 | — | +0.00 | -8.10 | +4.50 | — |
| 2026-09-08 | `CRDO` | 7 | $170.57 | $170.54 | -0.18 | — | +0.00 | -0.18 | +59.11 | — |
| 2026-09-08 | `ADCT` | 991 | $1.36 | $1.33 | -29.73 | — | +0.00 | -29.73 | +29.73 | — |
| 2026-09-08 | `HAFN` | 144 | $9.22 | $8.81 | -59.04 | — | +0.00 | -59.04 | -18.72 | — |
| 2026-09-08 | `DOCU` | 18 | $68.41 | $67.05 | -24.48 | — | +0.00 | -24.48 | -26.46 | — |
| 2026-09-08 | `XP` | 65 | $19.86 | $20.46 | +39.00 | — | +0.00 | +39.00 | +51.35 | — |
| 2026-09-08 | `MMED` | 54 | $23.29 | $23.16 | -7.02 | — | +0.00 | -7.02 | -36.72 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `SSL` | 361 | — | $14.35 | +0.00 | $14.59 | +86.64 | +86.64 | +0.00 | +86.64 |
| 2026-09-11 | `GFR` | 836 | — | $6.19 | +0.00 | $6.52 | +275.88 | +275.88 | +0.00 | +275.88 |
| 2026-09-14 | `SSL` | 361 | $14.59 | $14.69 | +36.10 | — | +0.00 | +36.10 | +122.74 | — |
| 2026-09-14 | `GFR` | 836 | $6.52 | $6.60 | +66.88 | $6.62 | +16.72 | +83.60 | +342.76 | +359.48 |
| 2026-09-15 | `GFR` | 836 | $6.62 | $6.61 | -8.36 | $6.93 | +267.52 | +259.16 | +351.12 | +618.64 |
| 2026-09-16 | `GFR` | 836 | $6.93 | $6.83 | -83.60 | $6.49 | -284.24 | -367.84 | +535.04 | +250.80 |
| 2026-09-16 | `ATRC` | 23 | — | $55.66 | +0.00 | $57.14 | +34.04 | +34.04 | +0.00 | +34.04 |
| 2026-09-16 | `TCOM` | 32 | — | $40.93 | +0.00 | $40.43 | -16.00 | -16.00 | +0.00 | -16.00 |
| 2026-09-16 | `VERA` | 39 | — | $33.22 | +0.00 | $31.77 | -56.55 | -56.55 | +0.00 | -56.55 |
| 2026-09-16 | `GNW` | 132 | — | $10.00 | +0.00 | $10.09 | +11.88 | +11.88 | +0.00 | +11.88 |
| 2026-09-17 | `GFR` | 836 | $6.49 | $6.48 | -8.36 | — | +0.00 | -8.36 | +242.44 | — |
| 2026-09-17 | `ATRC` | 23 | $57.14 | $57.96 | +18.86 | — | +0.00 | +18.86 | +52.90 | — |
| 2026-09-17 | `TCOM` | 32 | $40.43 | $40.79 | +11.52 | — | +0.00 | +11.52 | -4.48 | — |
| 2026-09-17 | `VERA` | 39 | $31.77 | $32.50 | +28.47 | — | +0.00 | +28.47 | -28.08 | — |
| 2026-09-17 | `GNW` | 132 | $10.09 | $10.12 | +3.96 | — | +0.00 | +3.96 | +15.84 | — |
| 2026-09-17 | `CBC` | 113 | — | $31.60 | +0.00 | $31.67 | +7.91 | +7.91 | +0.00 | +7.91 |
| 2026-09-17 | `PLAY` | 513 | — | $6.96 | +0.00 | $6.54 | -215.46 | -215.46 | +0.00 | -215.46 |
| 2026-09-17 | `GRAL` | 47 | — | $75.29 | +0.00 | $79.95 | +219.02 | +219.02 | +0.00 | +219.02 |
| 2026-09-18 | `CBC` | 113 | $31.67 | $31.64 | -3.39 | — | +0.00 | -3.39 | +4.52 | — |
| 2026-09-18 | `PLAY` | 513 | $6.54 | $6.64 | +51.30 | — | +0.00 | +51.30 | -164.16 | — |
| 2026-09-18 | `GRAL` | 47 | $79.95 | $81.48 | +71.91 | — | +0.00 | +71.91 | +290.93 | — |
| 2026-09-18 | `S` | 234 | — | $23.13 | +0.00 | $22.51 | -145.08 | -145.08 | +0.00 | -145.08 |
| 2026-09-18 | `ALHC` | 623 | — | $8.68 | +0.00 | $8.35 | -205.59 | -205.59 | +0.00 | -205.59 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +787.37 | TPG | — | $24.65 | $10,784.79 | TPG×197 |
| 2026-08-14 | +5.50 | $24.65 | TPG×197 | $10,916.78 | +131.99 | +433.10 | SPHR, KULR, NPWR, RLX, BSBR, ENB, RUM, JBTM | TPG | $206.97 | $11,307.70 | SPHR×7, KULR×545, NPWR×874, RLX×737, BSBR×235, ENB×26, RUM×181, JBTM×11 |
| 2026-08-17 | +2.25 | $206.97 | SPHR×7, KULR×545, NPWR×874, RLX×737, BSBR×235, ENB×26, RUM×181, JBTM×11 | $11,245.05 | -62.65 | -94.45 | VIV, WBS | SPHR, KULR, NPWR, RLX, BSBR, ENB, RUM, JBTM | $64.82 | $11,102.12 | VIV×485, WBS×70 |
| 2026-08-18 | -6.20 | $64.82 | VIV×485, WBS×70 | $11,114.47 | +12.35 | +0.00 | — | VIV, WBS | $11,105.83 | $11,105.83 | — |
| 2026-08-19 | -7.20 | $11,105.83 | — | $11,105.83 | +0.00 | +0.00 | — | — | $11,105.83 | $11,105.83 | — |
| 2026-08-20 | +1.12 | $11,105.83 | — | $11,105.83 | +0.00 | -117.09 | FUTU, WMT, WBS, ALH, NTST, ADC, SAN, FBRX | — | $235.48 | $10,971.92 | FUTU×11, WMT×13, WBS×17, ALH×58, NTST×67, ADC×18, SAN×97, FBRX×18 |
| 2026-08-21 | +3.25 | $235.48 | FUTU×11, WMT×13, WBS×17, ALH×58, NTST×67, ADC×18, SAN×97, FBRX×18 | $11,045.55 | +73.63 | +197.00 | BJ, HITI, VIK | FUTU, WMT, WBS, ALH, NTST, ADC, SAN, FBRX | $25.46 | $11,201.84 | BJ×39, HITI×1512, VIK×40 |
| 2026-08-24 | -5.17 | $25.46 | BJ×39, HITI×1512, VIK×40 | $11,236.04 | +34.20 | +0.00 | — | BJ, HITI, VIK | $11,211.96 | $11,211.96 | — |
| 2026-08-25 | +1.80 | $11,211.96 | — | $11,211.96 | -0.00 | -95.51 | ZYME, RHI, ABUS, AMX, SAN, DBRG, ASO | — | $81.57 | $11,099.36 | ZYME×55, RHI×36, ABUS×305, AMX×67, SAN×109, DBRG×100, ASO×35 |
| 2026-08-26 | +2.02 | $81.57 | ZYME×55, RHI×36, ABUS×305, AMX×67, SAN×109, DBRG×100, ASO×35 | $11,098.83 | -0.53 | -101.29 | NCNO, PLAB, SJM, SLF, VIPS, ATHM | ZYME, RHI, AMX, SAN, ASO | $175.77 | $10,973.77 | ABUS×305, DBRG×100, NCNO×68, PLAB×35, SJM×9, SLF×16, VIPS×94, ATHM×60 |
| 2026-08-27 | — | $175.77 | ABUS×305, DBRG×100, NCNO×68, PLAB×35, SJM×9, SLF×16, VIPS×94, ATHM×60 | $10,966.42 | -7.35 | -15.05 | TRLV, BOX | ABUS, DBRG, NCNO, PLAB, SJM, SLF, VIPS, ATHM | $35.94 | $10,923.48 | TRLV×480, BOX×161 |
| 2026-08-28 | +0.75 | $35.94 | TRLV×480, BOX×161 | $10,910.69 | -12.79 | -285.99 | JKS, DY, ULTA, PLAB, JAZZ, WSM | TRLV, BOX | $729.94 | $10,603.25 | JKS×135, DY×5, ULTA×3, PLAB×60, JAZZ×7, WSM×7 |
| 2026-08-31 | -5.85 | $729.94 | JKS×135, DY×5, ULTA×3, PLAB×60, JAZZ×7, WSM×7 | $10,607.74 | +4.49 | +0.00 | — | JKS, DY, ULTA, PLAB, JAZZ, WSM | $10,594.99 | $10,594.99 | — |
| 2026-09-01 | -6.30 | $10,594.99 | — | $10,594.99 | +0.00 | +0.00 | — | — | $10,594.99 | $10,594.99 | — |
| 2026-09-02 | -3.83 | $10,594.99 | — | $10,594.99 | +0.00 | +0.00 | — | — | $10,594.99 | $10,594.99 | — |
| 2026-09-03 | -0.90 | $10,594.99 | — | $10,594.99 | +0.00 | -244.84 | VIR, AVGO, FIVE, MOMO, PVH, VSXY, GBTG, DAKT | — | $384.53 | $10,332.04 | VIR×114, AVGO×3, FIVE×5, MOMO×240, PVH×17, VSXY×17, GBTG×139, DAKT×69 |
| 2026-09-04 | +2.25 | $384.53 | VIR×114, AVGO×3, FIVE×5, MOMO×240, PVH×17, VSXY×17, GBTG×139, DAKT×69 | $10,330.25 | -1.79 | +140.10 | ATRC, WNC, CRDO, ADCT, HAFN, DOCU, XP, MMED | VIR, AVGO, FIVE, MOMO, PVH, VSXY, GBTG, DAKT | $250.34 | $10,424.09 | ATRC×24, WNC×90, CRDO×7, ADCT×991, HAFN×144, DOCU×18, XP×65, MMED×54 |
| 2026-09-08 | -11.47 | $250.34 | ATRC×24, WNC×90, CRDO×7, ADCT×991, HAFN×144, DOCU×18, XP×65, MMED×54 | $10,401.51 | -22.58 | +0.00 | — | ATRC, WNC, CRDO, ADCT, HAFN, DOCU, XP, MMED | $10,373.25 | $10,373.25 | — |
| 2026-09-09 | -13.95 | $10,373.25 | — | $10,373.25 | +0.00 | +0.00 | — | — | $10,373.25 | $10,373.25 | — |
| 2026-09-10 | -13.28 | $10,373.25 | — | $10,373.25 | +0.00 | +0.00 | — | — | $10,373.25 | $10,373.25 | — |
| 2026-09-11 | +0.50 | $10,373.25 | — | $10,373.25 | +0.00 | +362.52 | SSL, GFR | — | $2.62 | $10,720.33 | SSL×361, GFR×836 |
| 2026-09-14 | -11.00 | $2.62 | SSL×361, GFR×836 | $10,823.31 | +102.98 | +16.72 | — | SSL | $5,300.95 | $10,835.27 | GFR×836 |
| 2026-09-15 | -3.84 | $5,300.95 | GFR×836 | $10,826.91 | -8.36 | +267.52 | — | — | $5,300.95 | $11,094.43 | GFR×836 |
| 2026-09-16 | +5.30 | $5,300.95 | GFR×836 | $11,010.83 | -83.60 | -310.87 | ATRC, TCOM, VERA, GNW | — | $86.79 | $10,691.32 | GFR×836, ATRC×23, TCOM×32, VERA×39, GNW×132 |
| 2026-09-17 | +7.38 | $86.79 | GFR×836, ATRC×23, TCOM×32, VERA×39, GNW×132 | $10,745.77 | +54.45 | +11.47 | CBC, PLAY, GRAL | GFR, ATRC, TCOM, VERA, GNW | $35.09 | $10,726.47 | CBC×113, PLAY×513, GRAL×47 |
| 2026-09-18 | +4.86 | $35.09 | CBC×113, PLAY×513, GRAL×47 | $10,846.29 | +119.82 | -350.67 | S, ALHC | CBC, PLAY, GRAL | $3.89 | $10,473.28 | S×234, ALHC×623 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | 16:00 close · cash $24.65 · equity $10,784.79 vs 09:30 $10,000.00 (+784.79; session marks +787.37) · 1 name(s) marked open→close (per-name table). TPG×197 09:30 $50.62 → close $54.62 +787.37 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | 09:30 open · cash $24.65 (unchanged overnight, no fees) · equity $10,916.78 vs prior close $10,784.79 (+131.99) · 1 name(s) re-marked at the open (per-name table). TPG×197 yday $54.62 → 09:30 $55.29 +131.99 | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 197 | $55.29 | $2.70 | $+914.08 | $10,914.08 | ▲ +914.08 after sell → book $10,914.08; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SPHR` | 7 | $176.68 | $2.01 | — | $9,675.31 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ⚪; ret5=+14.4; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 545 | $2.50 | $7.03 | — | $8,305.78 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NPWR` | 874 | $1.56 | $11.27 | — | $6,931.06 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+8.0; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `RLX` | 737 | $1.85 | $9.51 | — | $5,558.10 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ⚪; ret5=+0.5; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BSBR` | 235 | $5.79 | $3.03 | — | $4,194.42 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ⚪; ret5=-0.3; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ENB` | 26 | $51.15 | $2.07 | — | $2,862.45 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ⚪; ret5=-0.8; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `RUM` | 181 | $7.51 | $2.53 | — | $1,500.61 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ⚪; ret5=+21.6; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `JBTM` | 11 | $117.42 | $2.02 | — | $206.97 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ⚪; ret5=-3.7; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $206.97 | ▲ close $11,307.70 vs 09:30 $10,916.78 (session +433.10) | 16:00 close · cash $206.97 · equity $11,307.70 vs 09:30 $10,916.78 (+390.92; session marks +433.10) · 8 name(s) marked open→close (per-name table). SPHR×7 09:30 $176.68 → close $168.00 -60.76; KULR×545 09:30 $2.50 → close $2.64 +76.30; NPWR×874 09:30 $1.56 → close $1.95 +340.86; RLX×737 09:30 $1.85 → close $1.94 +66.33; BSBR×235 09:30 $5.79 → close $5.77 -4.70; ENB×26 09:30 $51.15 → close $50.91 -6.24; RUM×181 09:30 $7.51 → close $7.46 -9.05; JBTM×11 09:30 $117.42 → close $120.18 +30.36 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $206.97 | ▼ 09:30 equity $11,245.05 vs yday $11,307.70 (-62.65) | 09:30 open · cash $206.97 (unchanged overnight, no fees) · equity $11,245.05 vs prior close $11,307.70 (-62.65) · 8 name(s) re-marked at the open (per-name table). SPHR×7 yday $168.00 → 09:30 $168.10 +0.70; KULR×545 yday $2.64 → 09:30 $2.63 -5.45; NPWR×874 yday $1.95 → 09:30 $1.92 -26.22; RLX×737 yday $1.94 → 09:30 $1.92 -14.74; BSBR×235 yday $5.77 → 09:30 $5.78 +2.35; ENB×26 yday $50.91 → 09:30 $50.80 -2.86; RUM×181 yday $7.46 → 09:30 $7.43 -5.43; JBTM×11 yday $120.18 → 09:30 $119.18 -11.00 | — |
| 2026-08-17 09:30 ET | **SELL** | `SPHR` | 7 | $168.10 | $2.03 | $-64.10 | $1,381.64 | ▼ -64.10 after sell → book $11,243.02; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `KULR` | 545 | $2.63 | $7.13 | $+56.69 | $2,807.86 | ▲ +56.69 after sell → book $11,235.89; vs 09:30 mark -7.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NPWR` | 874 | $1.92 | $11.43 | $+291.93 | $4,474.50 | ▲ +291.93 after sell → book $11,224.45; vs 09:30 mark -11.44 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `RLX` | 737 | $1.92 | $9.64 | $+32.44 | $5,879.90 | ▲ +32.44 after sell → book $11,214.81; vs 09:30 mark -9.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BSBR` | 235 | $5.78 | $3.08 | $-8.46 | $7,235.12 | ▼ -8.46 after sell → book $11,211.73; vs 09:30 mark -3.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `ENB` | 26 | $50.80 | $2.09 | $-13.26 | $8,553.83 | ▼ -13.26 after sell → book $11,209.64; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `RUM` | 181 | $7.43 | $2.57 | $-19.59 | $9,896.09 | ▼ -19.59 after sell → book $11,207.07; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `JBTM` | 11 | $119.18 | $2.04 | $+15.29 | $11,205.02 | ▲ +15.29 after sell → book $11,205.02; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `VIV` | 485 | $11.55 | $6.26 | — | $5,597.02 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; ⚪; ret5=-5.0; leftover $5602.51 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `WBS` | 70 | $79.00 | $2.20 | — | $64.82 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; ⚪; ret5=+0.5; leftover $5602.51 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.82 | ▼ close $11,102.12 vs 09:30 $11,245.05 (session -94.45) | 16:00 close · cash $64.82 · equity $11,102.12 vs 09:30 $11,245.05 (-142.93; session marks -94.45) · 2 name(s) marked open→close (per-name table). VIV×485 09:30 $11.55 → close $11.40 -72.75; WBS×70 09:30 $79.00 → close $78.69 -21.70 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.82 | ▲ 09:30 equity $11,114.47 vs yday $11,102.12 (+12.35) | 09:30 open · cash $64.82 (unchanged overnight, no fees) · equity $11,114.47 vs prior close $11,102.12 (+12.35) · 2 name(s) re-marked at the open (per-name table). VIV×485 yday $11.40 → 09:30 $11.45 +24.25; WBS×70 yday $78.69 → 09:30 $78.52 -11.90 | — |
| 2026-08-18 09:30 ET | **SELL** | `VIV` | 485 | $11.45 | $6.38 | $-61.14 | $5,611.69 | ▼ -61.14 after sell → book $11,108.09; vs 09:30 mark -6.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `WBS` | 70 | $78.52 | $2.26 | $-38.06 | $11,105.83 | ▼ -38.06 after sell → book $11,105.83; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,105.83 | ▲ close $11,105.83 vs 09:30 $11,114.47 (session +0.00) | 16:00 close · cash $11,105.83 · no lots left · equity $11,105.83. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,105.83 | ▲ 09:30 equity $11,105.83 vs yday $11,105.83 (+0.00) | 09:30 open · cash $11,105.83 · no holdings · equity $11,105.83 vs prior close $11,105.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,105.83 | ▲ close $11,105.83 vs 09:30 $11,105.83 (session +0.00) | 16:00 close · cash $11,105.83 · no lots left · equity $11,105.83. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,105.83 | ▲ 09:30 equity $11,105.83 vs yday $11,105.83 (+0.00) | 09:30 open · cash $11,105.83 · no holdings · equity $11,105.83 vs prior close $11,105.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 11 | $117.65 | $2.02 | — | $9,809.66 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+4.1; leftover $1388.23 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WMT` | 13 | $106.38 | $2.03 | — | $8,424.69 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-1.7; leftover $1388.23 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WBS` | 17 | $77.57 | $2.04 | — | $7,103.96 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ⚪; ret5=-1.9; leftover $1388.23 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALH` | 58 | $23.72 | $2.16 | — | $5,726.03 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=-7.6; leftover $1388.23 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NTST` | 67 | $20.47 | $2.19 | — | $4,352.35 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=+0.0; leftover $1388.23 | join🟢 sector🔴 gen🟢 news🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ADC` | 18 | $74.37 | $2.04 | — | $3,011.65 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=+0.0; leftover $1388.23 | join🟡 sector🔴 gen🟢 news🟡 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SAN` | 97 | $14.30 | $2.28 | — | $1,622.27 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=-4.4; leftover $1388.23 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `FBRX` | 18 | $76.93 | $2.04 | — | $235.48 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=+0.2; leftover $1388.23 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟡 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $235.48 | ▼ close $10,971.92 vs 09:30 $11,105.83 (session -117.09) | 16:00 close · cash $235.48 · equity $10,971.92 vs 09:30 $11,105.83 (-133.91; session marks -117.09) · 8 name(s) marked open→close (per-name table). FUTU×11 09:30 $117.65 → close $112.73 -54.12; WMT×13 09:30 $106.38 → close $103.84 -33.02; WBS×17 09:30 $77.57 → close $77.57 +0.00; ALH×58 09:30 $23.72 → close $23.18 -31.32; NTST×67 09:30 $20.47 → close $20.61 +9.38; ADC×18 09:30 $74.37 → close $74.45 +1.44; SAN×97 09:30 $14.30 → close $14.21 -8.73; FBRX×18 09:30 $76.93 → close $76.89 -0.72 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $235.48 | ▲ 09:30 equity $11,045.55 vs yday $10,971.92 (+73.63) | 09:30 open · cash $235.48 (unchanged overnight, no fees) · equity $11,045.55 vs prior close $10,971.92 (+73.63) · 8 name(s) re-marked at the open (per-name table). FUTU×11 yday $112.73 → 09:30 $115.18 +26.95; WMT×13 yday $103.84 → 09:30 $103.69 -1.95; WBS×17 yday $77.57 → 09:30 $77.57 +0.00; ALH×58 yday $23.18 → 09:30 $23.33 +8.70; NTST×67 yday $20.61 → 09:30 $20.66 +3.35; ADC×18 yday $74.45 → 09:30 $74.60 +2.70; SAN×97 yday $14.21 → 09:30 $14.55 +32.98; FBRX×18 yday $76.89 → 09:30 $76.94 +0.90 | — |
| 2026-08-21 09:30 ET | **SELL** | `FUTU` | 11 | $115.18 | $2.04 | $-31.24 | $1,500.42 | ▼ -31.24 after sell → book $11,043.51; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `WMT` | 13 | $103.69 | $2.05 | $-39.05 | $2,846.34 | ▼ -39.05 after sell → book $11,041.46; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `WBS` | 17 | $77.57 | $2.06 | $-4.10 | $4,162.97 | ▼ -4.10 after sell → book $11,039.40; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALH` | 58 | $23.33 | $2.18 | $-26.97 | $5,513.92 | ▼ -26.97 after sell → book $11,037.21; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NTST` | 67 | $20.66 | $2.21 | $+8.33 | $6,895.93 | ▲ +8.33 after sell → book $11,035.00; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ADC` | 18 | $74.60 | $2.06 | $+0.03 | $8,236.67 | ▲ +0.03 after sell → book $11,032.94; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SAN` | 97 | $14.55 | $2.31 | $+19.66 | $9,645.71 | ▲ +19.66 after sell → book $11,030.63; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `FBRX` | 18 | $76.94 | $2.07 | $-3.93 | $11,028.56 | ▼ -3.93 after sell → book $11,028.56; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 39 | $93.98 | $2.11 | — | $7,361.24 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=-2.4; leftover $3676.19 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 1512 | $2.43 | $19.50 | — | $3,667.57 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $3676.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `VIK` | 40 | $91.00 | $2.11 | — | $25.46 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; ret5=-14.7; leftover $3676.19 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.46 | ▲ close $11,201.84 vs 09:30 $11,045.55 (session +197.00) | 16:00 close · cash $25.46 · equity $11,201.84 vs 09:30 $11,045.55 (+156.29; session marks +197.00) · 3 name(s) marked open→close (per-name table). BJ×39 09:30 $93.98 → close $96.42 +95.16; HITI×1512 09:30 $2.43 → close $2.45 +30.24; VIK×40 09:30 $91.00 → close $92.79 +71.60 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.46 | ▲ 09:30 equity $11,236.04 vs yday $11,201.84 (+34.20) | 09:30 open · cash $25.46 (unchanged overnight, no fees) · equity $11,236.04 vs prior close $11,201.84 (+34.20) · 3 name(s) re-marked at the open (per-name table). BJ×39 yday $96.42 → 09:30 $97.02 +23.40; HITI×1512 yday $2.45 → 09:30 $2.45 +0.00; VIK×40 yday $92.79 → 09:30 $93.06 +10.80 | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 39 | $97.02 | $2.15 | $+114.31 | $3,807.09 | ▲ +114.31 after sell → book $11,233.89; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 1512 | $2.45 | $19.79 | $-9.05 | $7,491.71 | ▼ -9.05 after sell → book $11,214.11; vs 09:30 mark -19.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `VIK` | 40 | $93.06 | $2.15 | $+78.14 | $11,211.96 | ▲ +78.14 after sell → book $11,211.96; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,211.96 | ▲ close $11,211.96 vs 09:30 $11,236.04 (session +0.00) | 16:00 close · cash $11,211.96 · no lots left · equity $11,211.96. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,211.96 | ▲ 09:30 equity $11,211.96 vs yday $11,211.96 (-0.00) | 09:30 open · cash $11,211.96 · no holdings · equity $11,211.96 vs prior close $11,211.96 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 55 | $28.86 | $2.15 | — | $9,622.50 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+13.7; leftover $1601.71 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 36 | $43.76 | $2.10 | — | $8,045.05 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1601.71 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 305 | $5.25 | $3.93 | — | $6,439.86 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy,oppset; 🔵; ⚪; ret5=+10.4; leftover $1601.71 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 67 | $23.80 | $2.19 | — | $4,843.07 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=+0.5; leftover $1601.71 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAN` | 109 | $14.65 | $2.32 | — | $3,243.90 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=+0.9; leftover $1601.71 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DBRG` | 100 | $15.98 | $2.29 | — | $1,643.61 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ⚪; ret5=+0.4; leftover $1601.71 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASO` | 35 | $44.57 | $2.10 | — | $81.57 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=+0.1; leftover $1601.71 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.57 | ▼ close $11,099.36 vs 09:30 $11,211.96 (session -95.51) | 16:00 close · cash $81.57 · equity $11,099.36 vs 09:30 $11,211.96 (-112.60; session marks -95.51) · 7 name(s) marked open→close (per-name table). ZYME×55 09:30 $28.86 → close $27.47 -76.45; RHI×36 09:30 $43.76 → close $44.90 +41.04; ABUS×305 09:30 $5.25 → close $5.20 -15.25; AMX×67 09:30 $23.80 → close $23.75 -3.35; SAN×109 09:30 $14.65 → close $14.63 -2.18; DBRG×100 09:30 $15.98 → close $15.97 -1.00; ASO×35 09:30 $44.57 → close $43.48 -38.32 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.57 | ▼ 09:30 equity $11,098.83 vs yday $11,099.36 (-0.53) | 09:30 open · cash $81.57 (unchanged overnight, no fees) · equity $11,098.83 vs prior close $11,099.36 (-0.53) · 7 name(s) re-marked at the open (per-name table). ZYME×55 yday $27.47 → 09:30 $27.56 +4.95; RHI×36 yday $44.90 → 09:30 $44.33 -20.52; ABUS×305 yday $5.20 → 09:30 $5.19 -3.05; AMX×67 yday $23.75 → 09:30 $23.75 +0.00; SAN×109 yday $14.63 → 09:30 $14.82 +20.71; DBRG×100 yday $15.97 → 09:30 $15.97 +0.00; ASO×35 yday $43.48 → 09:30 $43.40 -2.63 | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 55 | $27.56 | $2.18 | $-75.83 | $1,595.19 | ▼ -75.83 after sell → book $11,096.65; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 36 | $44.33 | $2.12 | $+16.30 | $3,188.95 | ▲ +16.30 after sell → book $11,094.53; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AMX` | 67 | $23.75 | $2.21 | $-7.76 | $4,777.99 | ▼ -7.76 after sell → book $11,092.32; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SAN` | 109 | $14.82 | $2.35 | $+13.87 | $6,391.02 | ▲ +13.87 after sell → book $11,089.97; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASO` | 35 | $43.40 | $2.12 | $-45.16 | $7,907.90 | ▼ -45.16 after sell → book $11,087.85; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 68 | $19.33 | $2.19 | — | $6,591.27 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=+3.0; leftover $1317.98 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `PLAB` | 35 | $37.26 | $2.10 | — | $5,285.07 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-8.0; leftover $1317.98 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SJM` | 9 | $134.80 | $2.02 | — | $4,069.85 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+5.9; leftover $1317.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SLF` | 16 | $79.20 | $2.04 | — | $2,800.62 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=-0.5; leftover $1317.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `VIPS` | 94 | $14.00 | $2.27 | — | $1,482.34 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; ret5=-0.4; leftover $1317.98 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ATHM` | 60 | $21.74 | $2.17 | — | $175.77 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=-1.1; leftover $1317.98 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $175.77 | ▼ close $10,973.77 vs 09:30 $11,098.83 (session -101.29) | 16:00 close · cash $175.77 · equity $10,973.77 vs 09:30 $11,098.83 (-125.06; session marks -101.29) · 8 name(s) marked open→close (per-name table). ABUS×305 09:30 $5.19 → close $5.19 +0.00; DBRG×100 09:30 $15.97 → close $15.97 +0.00; NCNO×68 09:30 $19.33 → close $21.51 +148.24; PLAB×35 09:30 $37.26 → close $30.25 -245.35; SJM×9 09:30 $134.80 → close $130.90 -35.10; SLF×16 09:30 $79.20 → close $79.05 -2.40; VIPS×94 09:30 $14.00 → close $14.08 +7.52; ATHM×60 09:30 $21.74 → close $22.17 +25.80 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $175.77 | ▼ 09:30 equity $10,966.42 vs yday $10,973.77 (-7.35) | 09:30 open · cash $175.77 (unchanged overnight, no fees) · equity $10,966.42 vs prior close $10,973.77 (-7.35) · 8 name(s) re-marked at the open (per-name table). ABUS×305 yday $5.19 → 09:30 $5.16 -9.15; DBRG×100 yday $15.97 → 09:30 $15.96 -1.00; NCNO×68 yday $21.51 → 09:30 $22.03 +35.36; PLAB×35 yday $30.25 → 09:30 $30.12 -4.55; SJM×9 yday $130.90 → 09:30 $130.29 -5.49; SLF×16 yday $79.05 → 09:30 $78.45 -9.60; VIPS×94 yday $14.08 → 09:30 $14.00 -7.52; ATHM×60 yday $22.17 → 09:30 $22.08 -5.40 | — |
| 2026-08-27 09:30 ET | **SELL** | `ABUS` | 305 | $5.16 | $4.00 | $-35.38 | $1,745.58 | ▼ -35.38 after sell → book $10,962.43; vs 09:30 mark -3.99 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DBRG` | 100 | $15.96 | $2.32 | $-6.61 | $3,339.26 | ▼ -6.61 after sell → book $10,960.11; vs 09:30 mark -2.32 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 68 | $22.03 | $2.22 | $+179.19 | $4,835.08 | ▲ +179.19 after sell → book $10,957.89; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `PLAB` | 35 | $30.12 | $2.12 | $-254.11 | $5,887.16 | ▼ -254.11 after sell → book $10,955.77; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SJM` | 9 | $130.29 | $2.04 | $-44.64 | $7,057.74 | ▼ -44.64 after sell → book $10,953.74; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `SLF` | 16 | $78.45 | $2.06 | $-16.10 | $8,310.88 | ▼ -16.10 after sell → book $10,951.68; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `VIPS` | 94 | $14.00 | $2.30 | $-4.57 | $9,624.58 | ▼ -4.57 after sell → book $10,949.38; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ATHM` | 60 | $22.08 | $2.19 | $+16.04 | $10,947.19 | ▲ +16.04 after sell → book $10,947.19; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `TRLV` | 480 | $11.38 | $6.19 | — | $5,478.60 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+13.3; leftover $5473.60 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BOX` | 161 | $33.79 | $2.47 | — | $35.94 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+0.8; leftover $5473.60 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.94 | ▼ close $10,923.48 vs 09:30 $10,966.42 (session -15.05) | 16:00 close · cash $35.94 · equity $10,923.48 vs 09:30 $10,966.42 (-42.94; session marks -15.05) · 2 name(s) marked open→close (per-name table). TRLV×480 09:30 $11.38 → close $11.03 -168.00; BOX×161 09:30 $33.79 → close $34.74 +152.95 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.94 | ▼ 09:30 equity $10,910.69 vs yday $10,923.48 (-12.79) | 09:30 open · cash $35.94 (unchanged overnight, no fees) · equity $10,910.69 vs prior close $10,923.48 (-12.79) · 2 name(s) re-marked at the open (per-name table). TRLV×480 yday $11.03 → 09:30 $11.00 -14.40; BOX×161 yday $34.74 → 09:30 $34.75 +1.61 | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 480 | $11.00 | $6.31 | $-194.91 | $5,309.62 | ▼ -194.91 after sell → book $10,904.37; vs 09:30 mark -6.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BOX` | 161 | $34.75 | $2.54 | $+149.54 | $10,901.83 | ▲ +149.54 after sell → book $10,901.83; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `JKS` | 135 | $13.37 | $2.40 | — | $9,094.48 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover,oppset; ret5=-14.9; leftover $1816.97 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 5 | $306.34 | $2.00 | — | $7,560.78 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover,oppset; ret5=-23.0; leftover $1816.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 3 | $542.00 | $2.00 | — | $5,932.78 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=+4.8; leftover $1816.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 60 | $30.01 | $2.17 | — | $4,130.01 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=-0.9; leftover $1816.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `JAZZ` | 7 | $249.48 | $2.01 | — | $2,381.64 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; ret5=+1.0; leftover $1816.97 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `WSM` | 7 | $235.67 | $2.01 | — | $729.94 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; ret5=+1.1; leftover $1816.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $729.94 | ▼ close $10,603.25 vs 09:30 $10,910.69 (session -285.99) | 16:00 close · cash $729.94 · equity $10,603.25 vs 09:30 $10,910.69 (-307.44; session marks -285.99) · 6 name(s) marked open→close (per-name table). JKS×135 09:30 $13.37 → close $13.54 +22.95; DY×5 09:30 $306.34 → close $294.34 -60.00; ULTA×3 09:30 $542.00 → close $517.50 -73.50; PLAB×60 09:30 $30.01 → close $27.73 -136.80; JAZZ×7 09:30 $249.48 → close $244.54 -34.58; WSM×7 09:30 $235.67 → close $235.09 -4.06 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $729.94 | ▲ 09:30 equity $10,607.74 vs yday $10,603.25 (+4.49) | 09:30 open · cash $729.94 (unchanged overnight, no fees) · equity $10,607.74 vs prior close $10,603.25 (+4.49) · 6 name(s) re-marked at the open (per-name table). JKS×135 yday $13.54 → 09:30 $13.54 +0.00; DY×5 yday $294.34 → 09:30 $298.01 +18.35; ULTA×3 yday $517.50 → 09:30 $521.10 +10.80; PLAB×60 yday $27.73 → 09:30 $28.04 +18.60; JAZZ×7 yday $244.54 → 09:30 $241.39 -22.05; WSM×7 yday $235.09 → 09:30 $232.06 -21.21 | — |
| 2026-08-31 09:30 ET | **SELL** | `JKS` | 135 | $13.54 | $2.43 | $+18.12 | $2,555.40 | ▲ +18.12 after sell → book $10,605.30; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 5 | $298.01 | $2.03 | $-45.68 | $4,043.43 | ▼ -45.68 after sell → book $10,603.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 3 | $521.10 | $2.02 | $-66.72 | $5,604.71 | ▼ -66.72 after sell → book $10,601.26; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 60 | $28.04 | $2.19 | $-122.56 | $7,284.91 | ▼ -122.56 after sell → book $10,599.06; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `JAZZ` | 7 | $241.39 | $2.03 | $-60.68 | $8,972.61 | ▼ -60.68 after sell → book $10,597.03; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `WSM` | 7 | $232.06 | $2.03 | $-29.31 | $10,594.99 | ▼ -29.31 after sell → book $10,594.99; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,594.99 | ▲ close $10,594.99 vs 09:30 $10,607.74 (session +0.00) | 16:00 close · cash $10,594.99 · no lots left · equity $10,594.99. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,594.99 | ▲ 09:30 equity $10,594.99 vs yday $10,594.99 (+0.00) | 09:30 open · cash $10,594.99 · no holdings · equity $10,594.99 vs prior close $10,594.99 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,594.99 | ▲ close $10,594.99 vs 09:30 $10,594.99 (session +0.00) | 16:00 close · cash $10,594.99 · no lots left · equity $10,594.99. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,594.99 | ▲ 09:30 equity $10,594.99 vs yday $10,594.99 (+0.00) | 09:30 open · cash $10,594.99 · no holdings · equity $10,594.99 vs prior close $10,594.99 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,594.99 | ▲ close $10,594.99 vs 09:30 $10,594.99 (session +0.00) | 16:00 close · cash $10,594.99 · no lots left · equity $10,594.99. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,594.99 | ▲ 09:30 equity $10,594.99 vs yday $10,594.99 (+0.00) | 09:30 open · cash $10,594.99 · no holdings · equity $10,594.99 vs prior close $10,594.99 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 114 | $11.54 | $2.33 | — | $9,277.10 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $1324.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,219.88 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+3.3; leftover $1324.37 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 5 | $257.00 | $2.00 | — | $6,932.88 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-5.5; leftover $1324.37 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 240 | $5.50 | $3.10 | — | $5,609.78 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-4.8; leftover $1324.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PVH` | 17 | $74.96 | $2.04 | — | $4,333.42 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-7.9; leftover $1324.37 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 17 | $76.86 | $2.04 | — | $3,024.76 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-6.6; leftover $1324.37 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GBTG` | 139 | $9.49 | $2.41 | — | $1,703.24 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=+0.2; leftover $1324.37 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DAKT` | 69 | $19.08 | $2.20 | — | $384.53 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=-1.9; leftover $1324.37 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $384.53 | ▼ close $10,332.04 vs 09:30 $10,594.99 (session -244.84) | 16:00 close · cash $384.53 · equity $10,332.04 vs 09:30 $10,594.99 (-262.95; session marks -244.84) · 8 name(s) marked open→close (per-name table). VIR×114 09:30 $11.54 → close $11.45 -10.26; AVGO×3 09:30 $351.74 → close $357.16 +16.26; FIVE×5 09:30 $257.00 → close $239.96 -85.20; MOMO×240 09:30 $5.50 → close $5.10 -96.00; PVH×17 09:30 $74.96 → close $72.46 -42.50; VSXY×17 09:30 $76.86 → close $73.64 -54.74; GBTG×139 09:30 $9.49 → close $9.49 +0.00; DAKT×69 09:30 $19.08 → close $19.48 +27.60 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $384.53 | ▼ 09:30 equity $10,330.25 vs yday $10,332.04 (-1.79) | 09:30 open · cash $384.53 (unchanged overnight, no fees) · equity $10,330.25 vs prior close $10,332.04 (-1.79) · 8 name(s) re-marked at the open (per-name table). VIR×114 yday $11.45 → 09:30 $11.31 -15.96; AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; FIVE×5 yday $239.96 → 09:30 $238.88 -5.40; MOMO×240 yday $5.10 → 09:30 $5.13 +7.20; PVH×17 yday $72.46 → 09:30 $72.79 +5.61; VSXY×17 yday $73.64 → 09:30 $73.63 -0.17; GBTG×139 yday $9.49 → 09:30 $9.49 +0.00; DAKT×69 yday $19.48 → 09:30 $19.47 -0.69 | — |
| 2026-09-04 09:30 ET | **SELL** | `VIR` | 114 | $11.31 | $2.36 | $-30.91 | $1,671.51 | ▼ -30.91 after sell → book $10,327.89; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,748.59 | ▲ +19.86 after sell → book $10,325.87; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 5 | $238.88 | $2.02 | $-94.63 | $3,940.96 | ▼ -94.63 after sell → book $10,323.84; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MOMO` | 240 | $5.13 | $3.15 | $-95.04 | $5,169.02 | ▼ -95.04 after sell → book $10,320.70; vs 09:30 mark -3.14 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `PVH` | 17 | $72.79 | $2.06 | $-40.99 | $6,404.38 | ▼ -40.99 after sell → book $10,318.63; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `VSXY` | 17 | $73.63 | $2.06 | $-59.01 | $7,654.03 | ▼ -59.01 after sell → book $10,316.57; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `GBTG` | 139 | $9.49 | $2.44 | $-4.85 | $8,970.70 | ▼ -4.85 after sell → book $10,314.13; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DAKT` | 69 | $19.47 | $2.22 | $+22.49 | $10,311.91 | ▲ +22.49 after sell → book $10,311.91; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 24 | $52.03 | $2.06 | — | $9,061.13 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $1288.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `WNC` | 90 | $14.17 | $2.26 | — | $7,783.57 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ret5=+7.9; leftover $1288.99 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRDO` | 7 | $162.10 | $2.01 | — | $6,646.86 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; 🔵; ret5=-31.7; leftover $1288.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ADCT` | 991 | $1.30 | $12.78 | — | $5,345.78 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ⚪; ret5=+17.9; leftover $1288.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 144 | $8.94 | $2.42 | — | $4,055.99 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+7.7; leftover $1288.99 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 18 | $68.52 | $2.04 | — | $2,820.59 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+3.4; leftover $1288.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `XP` | 65 | $19.67 | $2.19 | — | $1,539.86 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+13.1; leftover $1288.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MMED` | 54 | $23.84 | $2.15 | — | $250.34 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ⚪; ret5=+22.2; leftover $1288.99 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $250.34 | ▲ close $10,424.09 vs 09:30 $10,330.25 (session +140.10) | 16:00 close · cash $250.34 · equity $10,424.09 vs 09:30 $10,330.25 (+93.84; session marks +140.10) · 8 name(s) marked open→close (per-name table). ATRC×24 09:30 $52.03 → close $51.52 -12.24; WNC×90 09:30 $14.17 → close $14.31 +12.60; CRDO×7 09:30 $162.10 → close $170.57 +59.29; ADCT×991 09:30 $1.30 → close $1.36 +59.46; HAFN×144 09:30 $8.94 → close $9.22 +40.32; DOCU×18 09:30 $68.52 → close $68.41 -1.98; XP×65 09:30 $19.67 → close $19.86 +12.35; MMED×54 09:30 $23.84 → close $23.29 -29.70 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $250.34 | ▼ 09:30 equity $10,401.51 vs yday $10,424.09 (-22.58) | 09:30 open · cash $250.34 (unchanged overnight, no fees) · equity $10,401.51 vs prior close $10,424.09 (-22.58) · 8 name(s) re-marked at the open (per-name table). ATRC×24 yday $51.52 → 09:30 $54.31 +66.96; WNC×90 yday $14.31 → 09:30 $14.22 -8.10; CRDO×7 yday $170.57 → 09:30 $170.54 -0.18; ADCT×991 yday $1.36 → 09:30 $1.33 -29.73; HAFN×144 yday $9.22 → 09:30 $8.81 -59.04; DOCU×18 yday $68.41 → 09:30 $67.05 -24.48; XP×65 yday $19.86 → 09:30 $20.46 +39.00; MMED×54 yday $23.29 → 09:30 $23.16 -7.02 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 24 | $54.31 | $2.08 | $+50.58 | $1,551.70 | ▲ +50.58 after sell → book $10,399.43; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `WNC` | 90 | $14.22 | $2.29 | $-0.05 | $2,829.22 | ▼ -0.05 after sell → book $10,397.14; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRDO` | 7 | $170.54 | $2.03 | $+55.07 | $4,021.00 | ▲ +55.07 after sell → book $10,395.11; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ADCT` | 991 | $1.33 | $12.96 | $+3.99 | $5,326.07 | ▲ +3.99 after sell → book $10,382.15; vs 09:30 mark -12.96 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HAFN` | 144 | $8.81 | $2.46 | $-23.60 | $6,592.25 | ▼ -23.60 after sell → book $10,379.69; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 18 | $67.05 | $2.06 | $-30.57 | $7,797.09 | ▼ -30.57 after sell → book $10,377.63; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `XP` | 65 | $20.46 | $2.21 | $+46.96 | $9,124.78 | ▲ +46.96 after sell → book $10,375.42; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 54 | $23.16 | $2.17 | $-41.04 | $10,373.25 | ▼ -41.04 after sell → book $10,373.25; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.25 | ▲ close $10,373.25 vs 09:30 $10,401.51 (session +0.00) | 16:00 close · cash $10,373.25 · no lots left · equity $10,373.25. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,373.25 | ▲ 09:30 equity $10,373.25 vs yday $10,373.25 (+0.00) | 09:30 open · cash $10,373.25 · no holdings · equity $10,373.25 vs prior close $10,373.25 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.25 | ▲ close $10,373.25 vs 09:30 $10,373.25 (session +0.00) | 16:00 close · cash $10,373.25 · no lots left · equity $10,373.25. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,373.25 | ▲ 09:30 equity $10,373.25 vs yday $10,373.25 (+0.00) | 09:30 open · cash $10,373.25 · no holdings · equity $10,373.25 vs prior close $10,373.25 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,373.25 | ▲ close $10,373.25 vs 09:30 $10,373.25 (session +0.00) | 16:00 close · cash $10,373.25 · no lots left · equity $10,373.25. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,373.25 | ▲ 09:30 equity $10,373.25 vs yday $10,373.25 (+0.00) | 09:30 open · cash $10,373.25 · no holdings · equity $10,373.25 vs prior close $10,373.25 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `SSL` | 361 | $14.35 | $4.66 | — | $5,188.25 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+15.5; leftover $5186.63 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `GFR` | 836 | $6.19 | $10.78 | — | $2.62 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; ret5=+1.1; leftover $5186.63 | join🔴 sector🔴 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.62 | ▲ close $10,720.33 vs 09:30 $10,373.25 (session +362.52) | 16:00 close · cash $2.62 · equity $10,720.33 vs 09:30 $10,373.25 (+347.08; session marks +362.52) · 2 name(s) marked open→close (per-name table). SSL×361 09:30 $14.35 → close $14.59 +86.64; GFR×836 09:30 $6.19 → close $6.52 +275.88 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.62 | ▲ 09:30 equity $10,823.31 vs yday $10,720.33 (+102.98) | 09:30 open · cash $2.62 (unchanged overnight, no fees) · equity $10,823.31 vs prior close $10,720.33 (+102.98) · 2 name(s) re-marked at the open (per-name table). SSL×361 yday $14.59 → 09:30 $14.69 +36.10; GFR×836 yday $6.52 → 09:30 $6.60 +66.88 | — |
| 2026-09-14 09:30 ET | **SELL** | `SSL` | 361 | $14.69 | $4.76 | $+113.32 | $5,300.95 | ▲ +113.32 after sell → book $10,818.55; vs 09:30 mark -4.76 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,300.95 | ▲ close $10,835.27 vs 09:30 $10,823.31 (session +16.72) | 16:00 close · cash $5,300.95 · equity $10,835.27 vs 09:30 $10,823.31 (+11.96; session marks +16.72) · 1 name(s) marked open→close (per-name table). GFR×836 09:30 $6.60 → close $6.62 +16.72 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,300.95 | ▼ 09:30 equity $10,826.91 vs yday $10,835.27 (-8.36) | 09:30 open · cash $5,300.95 (unchanged overnight, no fees) · equity $10,826.91 vs prior close $10,835.27 (-8.36) · 1 name(s) re-marked at the open (per-name table). GFR×836 yday $6.62 → 09:30 $6.61 -8.36 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,300.95 | ▲ close $11,094.43 vs 09:30 $10,826.91 (session +267.52) | 16:00 close · cash $5,300.95 · equity $11,094.43 vs 09:30 $10,826.91 (+267.52; session marks +267.52) · 1 name(s) marked open→close (per-name table). GFR×836 09:30 $6.61 → close $6.93 +267.52 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,300.95 | ▼ 09:30 equity $11,010.83 vs yday $11,094.43 (-83.60) | 09:30 open · cash $5,300.95 (unchanged overnight, no fees) · equity $11,010.83 vs prior close $11,094.43 (-83.60) · 1 name(s) re-marked at the open (per-name table). GFR×836 yday $6.93 → 09:30 $6.83 -83.60 | — |
| 2026-09-16 09:30 ET | **BUY** | `ATRC` | 23 | $55.66 | $2.06 | — | $4,018.71 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+4.6; leftover $1325.24 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 32 | $40.93 | $2.09 | — | $2,706.87 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=-3.1; leftover $1325.24 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `VERA` | 39 | $33.22 | $2.11 | — | $1,409.18 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; ret5=-4.6; leftover $1325.24 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `GNW` | 132 | $10.00 | $2.39 | — | $86.79 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=+4.8; leftover $1325.24 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.79 | ▼ close $10,691.32 vs 09:30 $11,010.83 (session -310.87) | 16:00 close · cash $86.79 · equity $10,691.32 vs 09:30 $11,010.83 (-319.51; session marks -310.87) · 5 name(s) marked open→close (per-name table). GFR×836 09:30 $6.83 → close $6.49 -284.24; ATRC×23 09:30 $55.66 → close $57.14 +34.04; TCOM×32 09:30 $40.93 → close $40.43 -16.00; VERA×39 09:30 $33.22 → close $31.77 -56.55; GNW×132 09:30 $10.00 → close $10.09 +11.88 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.79 | ▲ 09:30 equity $10,745.77 vs yday $10,691.32 (+54.45) | 09:30 open · cash $86.79 (unchanged overnight, no fees) · equity $10,745.77 vs prior close $10,691.32 (+54.45) · 5 name(s) re-marked at the open (per-name table). GFR×836 yday $6.49 → 09:30 $6.48 -8.36; ATRC×23 yday $57.14 → 09:30 $57.96 +18.86; TCOM×32 yday $40.43 → 09:30 $40.79 +11.52; VERA×39 yday $31.77 → 09:30 $32.50 +28.47; GNW×132 yday $10.09 → 09:30 $10.12 +3.96 | — |
| 2026-09-17 09:30 ET | **SELL** | `GFR` | 836 | $6.48 | $10.97 | $+220.69 | $5,493.11 | ▲ +220.69 after sell → book $10,734.81; vs 09:30 mark -10.96 | dropped from list after 4 sess (min 1) | join🔴 sector🔴 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `ATRC` | 23 | $57.96 | $2.08 | $+48.76 | $6,824.11 | ▲ +48.76 after sell → book $10,732.73; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `TCOM` | 32 | $40.79 | $2.11 | $-8.67 | $8,127.28 | ▼ -8.67 after sell → book $10,730.62; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VERA` | 39 | $32.50 | $2.13 | $-32.31 | $9,392.65 | ▼ -32.31 after sell → book $10,728.49; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `GNW` | 132 | $10.12 | $2.42 | $+11.04 | $10,726.08 | ▲ +11.04 after sell → book $10,726.08; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `CBC` | 113 | $31.60 | $2.33 | — | $7,152.95 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; ret5=+1.3; leftover $3575.36 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `PLAY` | 513 | $6.96 | $6.62 | — | $3,575.85 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=-16.2; leftover $3575.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GRAL` | 47 | $75.29 | $2.13 | — | $35.09 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; ret5=-3.1; leftover $3575.36 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.09 | ▲ close $10,726.47 vs 09:30 $10,745.77 (session +11.47) | 16:00 close · cash $35.09 · equity $10,726.47 vs 09:30 $10,745.77 (-19.30; session marks +11.47) · 3 name(s) marked open→close (per-name table). CBC×113 09:30 $31.60 → close $31.67 +7.91; PLAY×513 09:30 $6.96 → close $6.54 -215.46; GRAL×47 09:30 $75.29 → close $79.95 +219.02 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.09 | ▲ 09:30 equity $10,846.29 vs yday $10,726.47 (+119.82) | 09:30 open · cash $35.09 (unchanged overnight, no fees) · equity $10,846.29 vs prior close $10,726.47 (+119.82) · 3 name(s) re-marked at the open (per-name table). CBC×113 yday $31.67 → 09:30 $31.64 -3.39; PLAY×513 yday $6.54 → 09:30 $6.64 +51.30; GRAL×47 yday $79.95 → 09:30 $81.48 +71.91 | — |
| 2026-09-18 09:30 ET | **SELL** | `CBC` | 113 | $31.64 | $2.38 | $-0.19 | $3,608.03 | ▼ -0.19 after sell → book $10,843.91; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `PLAY` | 513 | $6.64 | $6.73 | $-177.51 | $7,007.62 | ▼ -177.51 after sell → book $10,837.18; vs 09:30 mark -6.73 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `GRAL` | 47 | $81.48 | $2.17 | $+286.63 | $10,835.01 | ▲ +286.63 after sell → book $10,835.01; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `S` | 234 | $23.13 | $3.02 | — | $5,419.57 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $5417.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ALHC` | 623 | $8.68 | $8.04 | — | $3.89 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=-31.8; leftover $5417.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.89 | ▼ close $10,473.28 vs 09:30 $10,846.29 (session -350.67) | 16:00 close · cash $3.89 · equity $10,473.28 vs 09:30 $10,846.29 (-373.01; session marks -350.67) · 2 name(s) marked open→close (per-name table). S×234 09:30 $23.13 → close $22.51 -145.08; ALHC×623 09:30 $8.68 → close $8.35 -205.59 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SHC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTFL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `EIX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WB` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WBS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `WSC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `AFRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `GLPI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `STNE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ROIV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CRS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CSGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `LULU` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HASI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CNO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IHS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TXNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AMBA` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `STAA` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CBC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NEOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FNV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ANAB` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AXGN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `RH` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVAV` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `COO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `S` | 234 | 2026-09-18 @ $23.13 | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $5417.50 |
| `ALHC` | 623 | 2026-09-18 @ $8.68 | union ∩ flow_in, no 🚨; gate flow_in=True; list oppset; 🔵; ret5=-31.8; leftover $5417.50 |
