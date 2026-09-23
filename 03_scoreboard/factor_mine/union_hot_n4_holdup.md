# Factor mine action — `union_hot_n4_holdup`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `hot_score` · size `leftover` · sell `list` · S-boost `holdup` · hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book

Cash book **+62.41%** ($16,240) · signal-only (no cash/fees) was +76.30%. Starts YES **28/28**. Fills 81 · skips 73 · realized $+5290.87.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how hot the prior tape looked.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by how hot the prior tape looked and keep the top 4.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- On an UP morning (S > 0), each new long lot stays through the next 09:30 so the overnight gap is marked. Highly positive days in this window were mostly that gap; a same-day 09:30→16:00 book cannot harvest them.
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
- **Gate** `none (list as ranked)` · **rank** `hot_score` · **top_n** 4 (S≥+5 may raise this when S-boost is `holdup`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $67.41.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `IREN` | 54 | — | $45.98 | +0.00 | $44.76 | -65.88 | -65.88 | +0.00 | -65.88 |
| 2026-08-13 | `TNDM` | 107 | — | $23.33 | +0.00 | $23.13 | -21.40 | -21.40 | +0.00 | -21.40 |
| 2026-08-13 | `TPG` | 49 | — | $50.62 | +0.00 | $54.62 | +195.84 | +195.84 | +0.00 | +195.84 |
| 2026-08-13 | `INO` | 3085 | — | $0.81 | +0.00 | $0.90 | +277.65 | +277.65 | +0.00 | +277.65 |
| 2026-08-14 | `IREN` | 54 | $44.76 | $44.09 | -36.18 | $44.06 | -1.62 | -37.80 | -102.06 | -103.68 |
| 2026-08-14 | `TNDM` | 107 | $23.13 | $22.92 | -22.47 | $22.72 | -21.40 | -43.87 | -43.87 | -65.27 |
| 2026-08-14 | `TPG` | 49 | $54.62 | $55.29 | +32.83 | $53.03 | -110.74 | -77.91 | +228.67 | +117.93 |
| 2026-08-14 | `INO` | 3085 | $0.90 | $0.93 | +92.55 | $1.09 | +493.60 | +586.15 | +370.20 | +863.80 |
| 2026-08-17 | `IREN` | 54 | $44.06 | $45.23 | +63.18 | — | +0.00 | +63.18 | -40.50 | — |
| 2026-08-17 | `TNDM` | 107 | $22.72 | $22.50 | -23.54 | — | +0.00 | -23.54 | -88.81 | — |
| 2026-08-17 | `TPG` | 49 | $53.03 | $52.67 | -17.64 | — | +0.00 | -17.64 | +100.29 | — |
| 2026-08-17 | `INO` | 3085 | $1.09 | $1.07 | -61.70 | — | +0.00 | -61.70 | +802.10 | — |
| 2026-08-17 | `XHG` | 637 | — | $4.19 | +0.00 | $3.91 | -178.36 | -178.36 | +0.00 | -178.36 |
| 2026-08-17 | `CAPR` | 388 | — | $6.87 | +0.00 | $7.45 | +225.04 | +225.04 | +0.00 | +225.04 |
| 2026-08-17 | `STDN` | 195 | — | $13.64 | +0.00 | $13.31 | -64.35 | -64.35 | +0.00 | -64.35 |
| 2026-08-17 | `HTFL` | 64 | — | $41.23 | +0.00 | $41.94 | +45.44 | +45.44 | +0.00 | +45.44 |
| 2026-08-18 | `XHG` | 637 | $3.91 | $3.94 | +19.11 | $4.28 | +216.58 | +235.69 | -159.25 | +57.33 |
| 2026-08-18 | `CAPR` | 388 | $7.45 | $7.50 | +19.40 | $7.08 | -162.96 | -143.56 | +244.44 | +81.48 |
| 2026-08-18 | `STDN` | 195 | $13.31 | $13.31 | +0.00 | $12.20 | -216.45 | -216.45 | -64.35 | -280.80 |
| 2026-08-18 | `HTFL` | 64 | $41.94 | $41.50 | -28.16 | $45.23 | +238.72 | +210.56 | +17.28 | +256.00 |
| 2026-08-19 | `XHG` | 637 | $4.28 | $4.32 | +25.48 | — | +0.00 | +25.48 | +82.81 | — |
| 2026-08-19 | `CAPR` | 388 | $7.08 | $7.19 | +42.68 | — | +0.00 | +42.68 | +124.16 | — |
| 2026-08-19 | `STDN` | 195 | $12.20 | $12.35 | +29.25 | — | +0.00 | +29.25 | -251.55 | — |
| 2026-08-19 | `HTFL` | 64 | $45.23 | $46.02 | +50.56 | — | +0.00 | +50.56 | +306.56 | — |
| 2026-08-20 | `MRNA` | 18 | — | $150.14 | +0.00 | $133.32 | -302.76 | -302.76 | +0.00 | -302.76 |
| 2026-08-20 | `CYPH` | 2371 | — | $1.15 | +0.00 | $1.19 | +94.84 | +94.84 | +0.00 | +94.84 |
| 2026-08-20 | `ABCL` | 230 | — | $11.81 | +0.00 | $11.57 | -56.35 | -56.35 | +0.00 | -56.35 |
| 2026-08-20 | `AZI` | 1973 | — | $1.37 | +0.00 | $1.44 | +138.11 | +138.11 | +0.00 | +138.11 |
| 2026-08-21 | `MRNA` | 18 | $133.32 | $133.11 | -3.78 | $145.13 | +216.36 | +212.58 | -306.54 | -90.18 |
| 2026-08-21 | `CYPH` | 2371 | $1.19 | $1.32 | +308.23 | $1.42 | +237.10 | +545.33 | +403.07 | +640.17 |
| 2026-08-21 | `ABCL` | 230 | $11.57 | $11.57 | +0.00 | $11.32 | -57.50 | -57.50 | -56.35 | -113.85 |
| 2026-08-21 | `AZI` | 1973 | $1.44 | $1.46 | +39.46 | $1.45 | -19.73 | +19.73 | +177.57 | +157.84 |
| 2026-08-24 | `MRNA` | 18 | $145.13 | $142.70 | -43.74 | — | +0.00 | -43.74 | -133.92 | — |
| 2026-08-24 | `CYPH` | 2371 | $1.42 | $1.83 | +972.11 | — | +0.00 | +972.11 | +1612.28 | — |
| 2026-08-24 | `ABCL` | 230 | $11.32 | $10.97 | -80.50 | — | +0.00 | -80.50 | -194.35 | — |
| 2026-08-24 | `AZI` | 1973 | $1.45 | $1.46 | +19.73 | — | +0.00 | +19.73 | +177.57 | — |
| 2026-08-25 | `REAX` | 127 | — | $24.11 | +0.00 | $28.43 | +548.64 | +548.64 | +0.00 | +548.64 |
| 2026-08-25 | `CYPH` | 1963 | — | $1.56 | +0.00 | $1.64 | +157.04 | +157.04 | +0.00 | +157.04 |
| 2026-08-25 | `XHG` | 752 | — | $4.07 | +0.00 | $4.02 | -37.60 | -37.60 | +0.00 | -37.60 |
| 2026-08-25 | `ASST` | 158 | — | $19.04 | +0.00 | $21.39 | +371.30 | +371.30 | +0.00 | +371.30 |
| 2026-08-26 | `REAX` | 127 | $28.43 | $26.61 | -231.14 | $26.59 | -2.54 | -233.68 | +317.50 | +314.96 |
| 2026-08-26 | `CYPH` | 1963 | $1.64 | $1.60 | -78.52 | $1.63 | +58.89 | -19.63 | +78.52 | +137.41 |
| 2026-08-26 | `XHG` | 752 | $4.02 | $3.81 | -157.92 | $4.06 | +188.00 | +30.08 | -195.52 | -7.52 |
| 2026-08-26 | `ASST` | 158 | $21.39 | $20.72 | -105.86 | $21.50 | +123.24 | +17.38 | +265.44 | +388.68 |
| 2026-08-27 | `REAX` | 127 | $26.59 | $25.91 | -86.36 | — | +0.00 | -86.36 | +228.60 | — |
| 2026-08-27 | `CYPH` | 1963 | $1.63 | $1.75 | +235.56 | — | +0.00 | +235.56 | +372.97 | — |
| 2026-08-27 | `XHG` | 752 | $4.06 | $4.06 | +0.00 | $3.80 | -195.52 | -195.52 | -7.52 | -203.04 |
| 2026-08-27 | `ASST` | 158 | $21.50 | $22.45 | +150.10 | — | +0.00 | +150.10 | +538.78 | — |
| 2026-08-27 | `CAPR` | 372 | — | $9.19 | +0.00 | $10.06 | +323.64 | +323.64 | +0.00 | +323.64 |
| 2026-08-27 | `MRNA` | 23 | — | $144.18 | +0.00 | $142.77 | -32.43 | -32.43 | +0.00 | -32.43 |
| 2026-08-27 | `BZ` | 184 | — | $18.50 | +0.00 | $18.00 | -92.00 | -92.00 | +0.00 | -92.00 |
| 2026-08-28 | `XHG` | 752 | $3.80 | $3.69 | -82.72 | — | +0.00 | -82.72 | -285.76 | — |
| 2026-08-28 | `CAPR` | 372 | $10.06 | $9.73 | -122.76 | $9.59 | -52.08 | -174.84 | +200.88 | +148.80 |
| 2026-08-28 | `MRNA` | 23 | $142.77 | $137.19 | -128.34 | $137.99 | +18.40 | -109.94 | -160.77 | -142.37 |
| 2026-08-28 | `BZ` | 184 | $18.00 | $18.15 | +27.60 | — | +0.00 | +27.60 | -64.40 | — |
| 2026-08-28 | `BYND` | 221 | — | $14.00 | +0.00 | $13.86 | -30.94 | -30.94 | +0.00 | -30.94 |
| 2026-08-28 | `ANF` | 21 | — | $146.07 | +0.00 | $148.42 | +49.35 | +49.35 | +0.00 | +49.35 |
| 2026-08-31 | `CAPR` | 372 | $9.59 | $9.50 | -33.48 | — | +0.00 | -33.48 | +115.32 | — |
| 2026-08-31 | `MRNA` | 23 | $137.99 | $134.10 | -89.47 | — | +0.00 | -89.47 | -231.84 | — |
| 2026-08-31 | `BYND` | 221 | $13.86 | $13.81 | -11.05 | $13.30 | -112.71 | -123.76 | -41.99 | -154.70 |
| 2026-08-31 | `ANF` | 21 | $148.42 | $148.03 | -8.19 | $143.08 | -103.95 | -112.14 | +41.16 | -62.79 |
| 2026-09-01 | `BYND` | 221 | $13.30 | $13.04 | -57.46 | — | +0.00 | -57.46 | -212.16 | — |
| 2026-09-01 | `ANF` | 21 | $143.08 | $142.00 | -22.68 | — | +0.00 | -22.68 | -85.47 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 1757 | — | $1.78 | +0.00 | $1.39 | -685.23 | -685.23 | +0.00 | -685.23 |
| 2026-09-03 | `REAX` | 170 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CNH` | 228 | — | $13.71 | +0.00 | $13.84 | +29.64 | +29.64 | +0.00 | +29.64 |
| 2026-09-03 | `MMED` | 130 | — | $23.88 | +0.00 | $23.84 | -5.20 | -5.20 | +0.00 | -5.20 |
| 2026-09-04 | `GPRO` | 1757 | $1.39 | $1.48 | +158.13 | $1.70 | +386.54 | +544.67 | -527.10 | -140.56 |
| 2026-09-04 | `REAX` | 170 | $18.40 | $18.15 | -42.50 | — | +0.00 | -42.50 | -42.50 | — |
| 2026-09-04 | `CNH` | 228 | $13.84 | $13.89 | +11.40 | — | +0.00 | +11.40 | +41.04 | — |
| 2026-09-04 | `MMED` | 130 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -5.20 | — |
| 2026-09-04 | `ASST` | 123 | — | $25.18 | +0.00 | $27.14 | +241.08 | +241.08 | +0.00 | +241.08 |
| 2026-09-04 | `USDE` | 395 | — | $7.87 | +0.00 | $7.93 | +23.70 | +23.70 | +0.00 | +23.70 |
| 2026-09-04 | `DFDV` | 537 | — | $5.79 | +0.00 | $5.87 | +42.96 | +42.96 | +0.00 | +42.96 |
| 2026-09-08 | `GPRO` | 1757 | $1.70 | $1.56 | -237.20 | — | +0.00 | -237.20 | -377.76 | — |
| 2026-09-08 | `ASST` | 123 | $27.14 | $26.44 | -86.10 | $27.16 | +88.56 | +2.46 | +154.98 | +243.54 |
| 2026-09-08 | `USDE` | 395 | $7.93 | $7.76 | -67.15 | $7.61 | -59.25 | -126.40 | -43.45 | -102.70 |
| 2026-09-08 | `DFDV` | 537 | $5.87 | $5.81 | -32.22 | $5.99 | +96.66 | +64.44 | +10.74 | +107.40 |
| 2026-09-09 | `ASST` | 123 | $27.16 | $28.00 | +103.32 | — | +0.00 | +103.32 | +346.86 | — |
| 2026-09-09 | `USDE` | 395 | $7.61 | $8.01 | +158.00 | — | +0.00 | +158.00 | +55.30 | — |
| 2026-09-09 | `DFDV` | 537 | $5.99 | $6.02 | +16.11 | — | +0.00 | +16.11 | +123.51 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `INDP` | 1163 | — | $2.70 | +0.00 | $2.77 | +81.41 | +81.41 | +0.00 | +81.41 |
| 2026-09-11 | `BNC` | 639 | — | $4.91 | +0.00 | $4.80 | -70.29 | -70.29 | +0.00 | -70.29 |
| 2026-09-11 | `IRD` | 510 | — | $6.16 | +0.00 | $6.04 | -61.20 | -61.20 | +0.00 | -61.20 |
| 2026-09-11 | `CMRC` | 992 | — | $3.13 | +0.00 | $3.50 | +372.00 | +372.00 | +0.00 | +372.00 |
| 2026-09-14 | `INDP` | 1163 | $2.77 | $2.80 | +34.89 | $3.14 | +395.42 | +430.31 | +116.30 | +511.72 |
| 2026-09-14 | `BNC` | 639 | $4.80 | $5.03 | +146.97 | $5.27 | +153.36 | +300.33 | +76.68 | +230.04 |
| 2026-09-14 | `IRD` | 510 | $6.04 | $6.02 | -10.20 | $5.97 | -25.50 | -35.70 | -71.40 | -96.90 |
| 2026-09-14 | `CMRC` | 992 | $3.50 | $3.51 | +4.96 | $3.64 | +128.96 | +133.92 | +376.96 | +505.92 |
| 2026-09-15 | `INDP` | 1163 | $3.14 | $3.40 | +302.38 | $3.64 | +279.12 | +581.50 | +814.10 | +1093.22 |
| 2026-09-15 | `BNC` | 639 | $5.27 | $5.11 | -102.24 | — | +0.00 | -102.24 | +127.80 | — |
| 2026-09-15 | `IRD` | 510 | $5.97 | $5.94 | -15.30 | — | +0.00 | -15.30 | -112.20 | — |
| 2026-09-15 | `CMRC` | 992 | $3.64 | $3.64 | +0.00 | — | +0.00 | +0.00 | +505.92 | — |
| 2026-09-16 | `INDP` | 1163 | $3.64 | $3.66 | +23.26 | $3.21 | -523.35 | -500.09 | +1116.48 | +593.13 |
| 2026-09-16 | `HLP` | 1829 | — | $1.80 | +0.00 | $2.07 | +493.83 | +493.83 | +0.00 | +493.83 |
| 2026-09-16 | `SDGR` | 141 | — | $23.29 | +0.00 | $23.93 | +90.24 | +90.24 | +0.00 | +90.24 |
| 2026-09-16 | `SSL` | 223 | — | $14.62 | +0.00 | $14.29 | -73.59 | -73.59 | +0.00 | -73.59 |
| 2026-09-17 | `INDP` | 1163 | $3.21 | $3.30 | +104.67 | $3.93 | +732.69 | +837.36 | +697.80 | +1430.49 |
| 2026-09-17 | `HLP` | 1829 | $2.07 | $2.10 | +54.87 | $2.02 | -146.32 | -91.45 | +548.70 | +402.38 |
| 2026-09-17 | `SDGR` | 141 | $23.93 | $24.09 | +22.56 | $30.24 | +867.15 | +889.71 | +112.80 | +979.95 |
| 2026-09-17 | `SSL` | 223 | $14.29 | $13.77 | -115.96 | $14.14 | +82.51 | -33.45 | -189.55 | -107.04 |
| 2026-09-18 | `INDP` | 1163 | $3.93 | $3.85 | -93.04 | $3.55 | -348.90 | -441.94 | +1337.45 | +988.55 |
| 2026-09-18 | `HLP` | 1829 | $2.02 | $1.96 | -109.74 | — | +0.00 | -109.74 | +292.64 | — |
| 2026-09-18 | `SDGR` | 141 | $30.24 | $29.32 | -129.72 | $29.02 | -42.30 | -172.02 | +850.23 | +807.93 |
| 2026-09-18 | `SSL` | 223 | $14.14 | $13.93 | -46.83 | — | +0.00 | -46.83 | -153.87 | — |
| 2026-09-18 | `CYPH` | 1100 | — | $3.04 | +0.00 | $3.60 | +621.50 | +621.50 | +0.00 | +621.50 |
| 2026-09-18 | `TEM` | 40 | — | $81.40 | +0.00 | $77.84 | -142.40 | -142.40 | +0.00 | -142.40 |
| 2026-09-21 | `INDP` | 1163 | $3.55 | $3.55 | +0.00 | — | +0.00 | +0.00 | +988.55 | — |
| 2026-09-21 | `SDGR` | 141 | $29.02 | $29.43 | +57.81 | — | +0.00 | +57.81 | +865.74 | — |
| 2026-09-21 | `CYPH` | 1100 | $3.60 | $4.00 | +440.00 | $3.40 | -660.00 | -220.00 | +1061.50 | +401.50 |
| 2026-09-21 | `TEM` | 40 | $77.84 | $79.08 | +49.60 | $78.03 | -42.00 | +7.60 | -92.80 | -134.80 |
| 2026-09-21 | `FEAM` | 842 | — | $2.47 | +0.00 | $2.48 | +8.42 | +8.42 | +0.00 | +8.42 |
| 2026-09-21 | `TJGC` | 123 | — | $16.91 | +0.00 | $17.58 | +82.41 | +82.41 | +0.00 | +82.41 |
| 2026-09-21 | `LVWR` | 1261 | — | $1.65 | +0.00 | $1.53 | -151.32 | -151.32 | +0.00 | -151.32 |
| 2026-09-21 | `SECZ` | 176 | — | $11.67 | +0.00 | $13.50 | +322.08 | +322.08 | +0.00 | +322.08 |
| 2026-09-22 | `CYPH` | 1100 | $3.40 | $3.51 | +121.00 | — | +0.00 | +121.00 | +522.50 | — |
| 2026-09-22 | `TEM` | 40 | $78.03 | $77.99 | -1.60 | — | +0.00 | -1.60 | -136.40 | — |
| 2026-09-22 | `FEAM` | 842 | $2.48 | $2.47 | -8.42 | $2.94 | +395.74 | +387.32 | +0.00 | +395.74 |
| 2026-09-22 | `TJGC` | 123 | $17.58 | $17.58 | +0.00 | $16.90 | -83.64 | -83.64 | +82.41 | -1.23 |
| 2026-09-22 | `LVWR` | 1261 | $1.53 | $1.50 | -37.83 | $1.45 | -63.05 | -100.88 | -189.15 | -252.20 |
| 2026-09-22 | `SECZ` | 176 | $13.50 | $12.96 | -95.04 | $13.00 | +7.04 | -88.00 | +227.04 | +234.08 |
| 2026-09-22 | `GRAL` | 21 | — | $106.75 | +0.00 | $108.52 | +37.17 | +37.17 | +0.00 | +37.17 |
| 2026-09-22 | `NUAI` | 321 | — | $7.23 | +0.00 | $6.97 | -81.86 | -81.86 | +0.00 | -81.86 |
| 2026-09-22 | `INDP` | 748 | — | $3.10 | +0.00 | $3.99 | +665.72 | +665.72 | +0.00 | +665.72 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +386.21 | IREN, TNDM, TPG, INO | — | $0.54 | $10,345.37 | IREN×54, TNDM×107, TPG×49, INO×3085 |
| 2026-08-14 | +5.50 | $0.54 | IREN×54, TNDM×107, TPG×49, INO×3085 | $10,412.10 | +66.73 | +359.84 | — | — | $0.54 | $10,771.94 | IREN×54, TNDM×107, TPG×49, INO×3085 |
| 2026-08-17 | +2.25 | $0.54 | IREN×54, TNDM×107, TPG×49, INO×3085 | $10,732.24 | -39.70 | +27.77 | XHG, CAPR, STDN, HTFL | IREN, TNDM, TPG, INO | $34.12 | $10,695.00 | XHG×637, CAPR×388, STDN×195, HTFL×64 |
| 2026-08-18 | -6.20 | $34.12 | XHG×637, CAPR×388, STDN×195, HTFL×64 | $10,705.35 | +10.35 | +75.89 | — | — | $34.12 | $10,781.24 | XHG×637, CAPR×388, STDN×195, HTFL×64 |
| 2026-08-19 | -7.20 | $34.12 | XHG×637, CAPR×388, STDN×195, HTFL×64 | $10,929.21 | +147.97 | +0.00 | — | XHG, CAPR, STDN, HTFL | $10,910.93 | $10,910.93 | — |
| 2026-08-20 | +1.12 | $10,910.93 | — | $10,910.93 | -0.00 | -126.16 | MRNA, CYPH, ABCL, AZI | — | $0.25 | $10,723.72 | MRNA×18, CYPH×2371, ABCL×230, AZI×1973 |
| 2026-08-21 | +3.25 | $0.25 | MRNA×18, CYPH×2371, ABCL×230, AZI×1973 | $11,067.63 | +343.91 | +376.23 | — | — | $0.25 | $11,443.86 | MRNA×18, CYPH×2371, ABCL×230, AZI×1973 |
| 2026-08-24 | -5.17 | $0.25 | MRNA×18, CYPH×2371, ABCL×230, AZI×1973 | $12,311.46 | +867.60 | +0.00 | — | MRNA, CYPH, ABCL, AZI | $12,249.54 | $12,249.54 | — |
| 2026-08-25 | +1.80 | $12,249.54 | — | $12,249.54 | +0.00 | +1,039.38 | REAX, CYPH, XHG, ASST | — | $16.47 | $13,249.06 | REAX×127, CYPH×1963, XHG×752, ASST×158 |
| 2026-08-26 | +2.02 | $16.47 | REAX×127, CYPH×1963, XHG×752, ASST×158 | $12,675.62 | -573.44 | +367.59 | — | — | $16.47 | $13,043.21 | REAX×127, CYPH×1963, XHG×752, ASST×158 |
| 2026-08-27 | — | $16.47 | REAX×127, CYPH×1963, XHG×752, ASST×158 | $13,342.51 | +299.30 | +3.69 | CAPR, MRNA, BZ | REAX, CYPH, ASST | $110.56 | $13,306.19 | XHG×752, CAPR×372, MRNA×23, BZ×184 |
| 2026-08-28 | +0.75 | $110.56 | XHG×752, CAPR×372, MRNA×23, BZ×184 | $12,999.97 | -306.22 | -15.27 | BYND, ANF | XHG, BZ | $46.22 | $12,967.35 | CAPR×372, MRNA×23, BYND×221, ANF×21 |
| 2026-08-31 | -5.85 | $46.22 | CAPR×372, MRNA×23, BYND×221, ANF×21 | $12,825.16 | -142.19 | -216.66 | — | CAPR, MRNA | $6,657.54 | $12,601.52 | BYND×221, ANF×21 |
| 2026-09-01 | -6.30 | $6,657.54 | BYND×221, ANF×21 | $12,521.38 | -80.14 | +0.00 | — | BYND, ANF | $12,516.38 | $12,516.38 | — |
| 2026-09-02 | -3.83 | $12,516.38 | — | $12,516.38 | +0.00 | +0.00 | — | — | $12,516.38 | $12,516.38 | — |
| 2026-09-03 | -0.90 | $12,516.38 | — | $12,516.38 | +0.00 | -660.79 | GPRO, REAX, CNH, MMED | — | $0.15 | $11,825.10 | GPRO×1757, REAX×170, CNH×228, MMED×130 |
| 2026-09-04 | +2.25 | $0.15 | GPRO×1757, REAX×170, CNH×228, MMED×130 | $11,952.13 | +127.03 | +694.28 | ASST, USDE, DFDV | REAX, CNH, MMED | $14.39 | $12,624.05 | GPRO×1757, ASST×123, USDE×395, DFDV×537 |
| 2026-09-08 | -11.47 | $14.39 | GPRO×1757, ASST×123, USDE×395, DFDV×537 | $12,201.38 | -422.67 | +125.97 | — | GPRO | $2,741.12 | $12,304.38 | ASST×123, USDE×395, DFDV×537 |
| 2026-09-09 | -13.95 | $2,741.12 | ASST×123, USDE×395, DFDV×537 | $12,581.81 | +277.43 | +0.00 | — | ASST, USDE, DFDV | $12,567.17 | $12,567.17 | — |
| 2026-09-10 | -13.28 | $12,567.17 | — | $12,567.17 | -0.00 | +0.00 | — | — | $12,567.17 | $12,567.17 | — |
| 2026-09-11 | +0.50 | $12,567.17 | — | $12,567.17 | -0.00 | +321.92 | INDP, BNC, IRD, CMRC | — | $0.40 | $12,846.47 | INDP×1163, BNC×639, IRD×510, CMRC×992 |
| 2026-09-14 | -11.00 | $0.40 | INDP×1163, BNC×639, IRD×510, CMRC×992 | $13,023.09 | +176.62 | +652.24 | — | — | $0.40 | $13,675.33 | INDP×1163, BNC×639, IRD×510, CMRC×992 |
| 2026-09-15 | -3.84 | $0.40 | INDP×1163, BNC×639, IRD×510, CMRC×992 | $13,860.17 | +184.84 | +279.12 | — | BNC, IRD, CMRC | $9,877.91 | $14,111.23 | INDP×1163 |
| 2026-09-16 | +5.30 | $9,877.91 | INDP×1163 | $14,134.49 | +23.26 | -12.87 | HLP, SDGR, SSL | — | $12.68 | $14,092.74 | INDP×1163, HLP×1829, SDGR×141, SSL×223 |
| 2026-09-17 | +7.38 | $12.68 | INDP×1163, HLP×1829, SDGR×141, SSL×223 | $14,158.88 | +66.14 | +1,536.03 | — | — | $12.68 | $15,694.91 | INDP×1163, HLP×1829, SDGR×141, SSL×223 |
| 2026-09-18 | +4.86 | $12.68 | INDP×1163, HLP×1829, SDGR×141, SSL×223 | $15,315.58 | -379.33 | +87.90 | CYPH, TEM | HLP, SSL | $66.25 | $15,360.32 | INDP×1163, SDGR×141, CYPH×1100, TEM×40 |
| 2026-09-21 | +12.87 | $66.25 | INDP×1163, SDGR×141, CYPH×1100, TEM×40 | $15,907.73 | +547.41 | -440.41 | FEAM, TJGC, LVWR, SECZ | INDP, SDGR | $0.58 | $15,417.61 | CYPH×1100, TEM×40, FEAM×842, TJGC×123, LVWR×1261, SECZ×176 |
| 2026-09-22 | -0.50 | $0.58 | CYPH×1100, TEM×40, FEAM×842, TJGC×123, LVWR×1261, SECZ×176 | $15,395.72 | -21.89 | +877.12 | GRAL, NUAI, INDP | CYPH, TEM | $67.41 | $16,240.45 | FEAM×842, TJGC×123, LVWR×1261, SECZ×176, GRAL×21, NUAI×321, INDP×748 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $7,514.93 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 107 | $23.33 | $2.31 | — | $5,016.31 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $2,533.63 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3085 | $0.81 | $34.24 | — | $0.54 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▲ close $10,345.37 vs 09:30 $10,000.00 (session +386.21) | 16:00 close · cash $0.54 · equity $10,345.37 vs 09:30 $10,000.00 (+345.37; session marks +386.21) · 4 name(s) marked open→close (per-name table). IREN×54 09:30 $45.98 → close $44.76 -65.88; TNDM×107 09:30 $23.33 → close $23.13 -21.40; TPG×49 09:30 $50.62 → close $54.62 +195.84; INO×3085 09:30 $0.81 → close $0.90 +277.65 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▲ 09:30 equity $10,412.10 vs yday $10,345.37 (+66.73) | 09:30 open · cash $0.54 (unchanged overnight, no fees) · equity $10,412.10 vs prior close $10,345.37 (+66.73) · 4 name(s) re-marked at the open (per-name table). IREN×54 yday $44.76 → 09:30 $44.09 -36.18; TNDM×107 yday $23.13 → 09:30 $22.92 -22.47; TPG×49 yday $54.62 → 09:30 $55.29 +32.83; INO×3085 yday $0.90 → 09:30 $0.93 +92.55 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▲ close $10,771.94 vs 09:30 $10,412.10 (session +359.84) | 16:00 close · cash $0.54 · equity $10,771.94 vs 09:30 $10,412.10 (+359.84; session marks +359.84) · 4 name(s) marked open→close (per-name table). IREN×54 09:30 $44.09 → close $44.06 -1.62; TNDM×107 09:30 $22.92 → close $22.72 -21.40; TPG×49 09:30 $55.29 → close $53.03 -110.74; INO×3085 09:30 $0.93 → close $1.09 +493.60 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▼ 09:30 equity $10,732.24 vs yday $10,771.94 (-39.70) | 09:30 open · cash $0.54 (unchanged overnight, no fees) · equity $10,732.24 vs prior close $10,771.94 (-39.70) · 4 name(s) re-marked at the open (per-name table). IREN×54 yday $44.06 → 09:30 $45.23 +63.18; TNDM×107 yday $22.72 → 09:30 $22.50 -23.54; TPG×49 yday $53.03 → 09:30 $52.67 -17.64; INO×3085 yday $1.09 → 09:30 $1.07 -61.70 | — |
| 2026-08-17 09:30 ET | **SELL** | `IREN` | 54 | $45.23 | $2.18 | $-44.83 | $2,440.78 | ▼ -44.83 after sell → book $10,730.06; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **SELL** | `TNDM` | 107 | $22.50 | $2.35 | $-93.47 | $4,845.93 | ▼ -93.47 after sell → book $10,727.71; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **SELL** | `TPG` | 49 | $52.67 | $2.17 | $+95.99 | $7,424.59 | ▲ +95.99 after sell → book $10,725.54; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **SELL** | `INO` | 3085 | $1.07 | $40.34 | $+727.52 | $10,685.21 | ▲ +727.52 after sell → book $10,685.21; vs 09:30 mark -40.33 | dropped from list after 2 sess (min 2) | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 637 | $4.19 | $8.22 | — | $8,007.96 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $2671.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 388 | $6.87 | $5.01 | — | $5,337.40 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $2671.30 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 195 | $13.64 | $2.58 | — | $2,675.02 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $2671.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 64 | $41.23 | $2.18 | — | $34.12 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $2671.30 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.12 | ▲ close $10,695.00 vs 09:30 $10,732.24 (session +27.77) | 16:00 close · cash $34.12 · equity $10,695.00 vs 09:30 $10,732.24 (-37.24; session marks +27.77) · 4 name(s) marked open→close (per-name table). XHG×637 09:30 $4.19 → close $3.91 -178.36; CAPR×388 09:30 $6.87 → close $7.45 +225.04; STDN×195 09:30 $13.64 → close $13.31 -64.35; HTFL×64 09:30 $41.23 → close $41.94 +45.44 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.12 | ▲ 09:30 equity $10,705.35 vs yday $10,695.00 (+10.35) | 09:30 open · cash $34.12 (unchanged overnight, no fees) · equity $10,705.35 vs prior close $10,695.00 (+10.35) · 4 name(s) re-marked at the open (per-name table). XHG×637 yday $3.91 → 09:30 $3.94 +19.11; CAPR×388 yday $7.45 → 09:30 $7.50 +19.40; STDN×195 yday $13.31 → 09:30 $13.31 +0.00; HTFL×64 yday $41.94 → 09:30 $41.50 -28.16 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.12 | ▲ close $10,781.24 vs 09:30 $10,705.35 (session +75.89) | 16:00 close · cash $34.12 · equity $10,781.24 vs 09:30 $10,705.35 (+75.89; session marks +75.89) · 4 name(s) marked open→close (per-name table). XHG×637 09:30 $3.94 → close $4.28 +216.58; CAPR×388 09:30 $7.50 → close $7.08 -162.96; STDN×195 09:30 $13.31 → close $12.20 -216.45; HTFL×64 09:30 $41.50 → close $45.23 +238.72 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.12 | ▲ 09:30 equity $10,929.21 vs yday $10,781.24 (+147.97) | 09:30 open · cash $34.12 (unchanged overnight, no fees) · equity $10,929.21 vs prior close $10,781.24 (+147.97) · 4 name(s) re-marked at the open (per-name table). XHG×637 yday $4.28 → 09:30 $4.32 +25.48; CAPR×388 yday $7.08 → 09:30 $7.19 +42.68; STDN×195 yday $12.20 → 09:30 $12.35 +29.25; HTFL×64 yday $45.23 → 09:30 $46.02 +50.56 | — |
| 2026-08-19 09:30 ET | **SELL** | `XHG` | 637 | $4.32 | $8.35 | $+66.25 | $2,777.61 | ▲ +66.25 after sell → book $10,920.86; vs 09:30 mark -8.35 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 388 | $7.19 | $5.09 | $+114.06 | $5,562.24 | ▲ +114.06 after sell → book $10,915.77; vs 09:30 mark -5.09 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `STDN` | 195 | $12.35 | $2.63 | $-256.75 | $7,967.86 | ▼ -256.75 after sell → book $10,913.14; vs 09:30 mark -2.63 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `HTFL` | 64 | $46.02 | $2.22 | $+302.16 | $10,910.93 | ▲ +302.16 after sell → book $10,910.93; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,910.93 | ▲ close $10,910.93 vs 09:30 $10,929.21 (session +0.00) | 16:00 close · cash $10,910.93 · no lots left · equity $10,910.93. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,910.93 | ▲ 09:30 equity $10,910.93 vs yday $10,910.93 (-0.00) | 09:30 open · cash $10,910.93 · no holdings · equity $10,910.93 vs prior close $10,910.93 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 18 | $150.14 | $2.04 | — | $8,206.36 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $2727.73 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2371 | $1.15 | $30.59 | — | $5,449.13 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2727.73 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 230 | $11.81 | $2.97 | — | $2,728.71 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $2727.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 1973 | $1.37 | $25.45 | — | $0.25 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $2727.73 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.25 | ▼ close $10,723.72 vs 09:30 $10,910.93 (session -126.16) | 16:00 close · cash $0.25 · equity $10,723.72 vs 09:30 $10,910.93 (-187.21; session marks -126.16) · 4 name(s) marked open→close (per-name table). MRNA×18 09:30 $150.14 → close $133.32 -302.76; CYPH×2371 09:30 $1.15 → close $1.19 +94.84; ABCL×230 09:30 $11.81 → close $11.57 -56.35; AZI×1973 09:30 $1.37 → close $1.44 +138.11 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.25 | ▲ 09:30 equity $11,067.63 vs yday $10,723.72 (+343.91) | 09:30 open · cash $0.25 (unchanged overnight, no fees) · equity $11,067.63 vs prior close $10,723.72 (+343.91) · 4 name(s) re-marked at the open (per-name table). MRNA×18 yday $133.32 → 09:30 $133.11 -3.78; CYPH×2371 yday $1.19 → 09:30 $1.32 +308.23; ABCL×230 yday $11.57 → 09:30 $11.57 +0.00; AZI×1973 yday $1.44 → 09:30 $1.46 +39.46 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.25 | ▲ close $11,443.86 vs 09:30 $11,067.63 (session +376.23) | 16:00 close · cash $0.25 · equity $11,443.86 vs 09:30 $11,067.63 (+376.23; session marks +376.23) · 4 name(s) marked open→close (per-name table). MRNA×18 09:30 $133.11 → close $145.13 +216.36; CYPH×2371 09:30 $1.32 → close $1.42 +237.10; ABCL×230 09:30 $11.57 → close $11.32 -57.50; AZI×1973 09:30 $1.46 → close $1.45 -19.73 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.25 | ▲ 09:30 equity $12,311.46 vs yday $11,443.86 (+867.60) | 09:30 open · cash $0.25 (unchanged overnight, no fees) · equity $12,311.46 vs prior close $11,443.86 (+867.60) · 4 name(s) re-marked at the open (per-name table). MRNA×18 yday $145.13 → 09:30 $142.70 -43.74; CYPH×2371 yday $1.42 → 09:30 $1.83 +972.11; ABCL×230 yday $11.32 → 09:30 $10.97 -80.50; AZI×1973 yday $1.45 → 09:30 $1.46 +19.73 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 18 | $142.70 | $2.07 | $-138.04 | $2,566.78 | ▼ -138.04 after sell → book $12,309.39; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 2371 | $1.83 | $31.01 | $+1550.68 | $6,874.69 | ▲ +1,550.68 after sell → book $12,278.37; vs 09:30 mark -31.02 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABCL` | 230 | $10.97 | $3.03 | $-200.34 | $9,394.77 | ▼ -200.34 after sell → book $12,275.35; vs 09:30 mark -3.02 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `AZI` | 1973 | $1.46 | $25.80 | $+126.32 | $12,249.54 | ▲ +126.32 after sell → book $12,249.54; vs 09:30 mark -25.81 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,249.54 | ▲ close $12,249.54 vs 09:30 $12,311.46 (session +0.00) | 16:00 close · cash $12,249.54 · no lots left · equity $12,249.54. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,249.54 | ▲ 09:30 equity $12,249.54 vs yday $12,249.54 (+0.00) | 09:30 open · cash $12,249.54 · no holdings · equity $12,249.54 vs prior close $12,249.54 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 127 | $24.11 | $2.37 | — | $9,185.20 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_mover; ret5=+891.7; leftover $3062.39 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1963 | $1.56 | $25.32 | — | $6,097.60 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $3062.39 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 752 | $4.07 | $9.70 | — | $3,027.26 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; leftover $3062.39 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 158 | $19.04 | $2.46 | — | $16.47 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; leftover $3062.39 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.47 | ▲ close $13,249.06 vs 09:30 $12,249.54 (session +1,039.38) | 16:00 close · cash $16.47 · equity $13,249.06 vs 09:30 $12,249.54 (+999.52; session marks +1039.38) · 4 name(s) marked open→close (per-name table). REAX×127 09:30 $24.11 → close $28.43 +548.64; CYPH×1963 09:30 $1.56 → close $1.64 +157.04; XHG×752 09:30 $4.07 → close $4.02 -37.60; ASST×158 09:30 $19.04 → close $21.39 +371.30 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.47 | ▼ 09:30 equity $12,675.62 vs yday $13,249.06 (-573.44) | 09:30 open · cash $16.47 (unchanged overnight, no fees) · equity $12,675.62 vs prior close $13,249.06 (-573.44) · 4 name(s) re-marked at the open (per-name table). REAX×127 yday $28.43 → 09:30 $26.61 -231.14; CYPH×1963 yday $1.64 → 09:30 $1.60 -78.52; XHG×752 yday $4.02 → 09:30 $3.81 -157.92; ASST×158 yday $21.39 → 09:30 $20.72 -105.86 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.47 | ▲ close $13,043.21 vs 09:30 $12,675.62 (session +367.59) | 16:00 close · cash $16.47 · equity $13,043.21 vs 09:30 $12,675.62 (+367.59; session marks +367.59) · 4 name(s) marked open→close (per-name table). REAX×127 09:30 $26.61 → close $26.59 -2.54; CYPH×1963 09:30 $1.60 → close $1.63 +58.89; XHG×752 09:30 $3.81 → close $4.06 +188.00; ASST×158 09:30 $20.72 → close $21.50 +123.24 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.47 | ▲ 09:30 equity $13,342.51 vs yday $13,043.21 (+299.30) | 09:30 open · cash $16.47 (unchanged overnight, no fees) · equity $13,342.51 vs prior close $13,043.21 (+299.30) · 4 name(s) re-marked at the open (per-name table). REAX×127 yday $26.59 → 09:30 $25.91 -86.36; CYPH×1963 yday $1.63 → 09:30 $1.75 +235.56; XHG×752 yday $4.06 → 09:30 $4.06 +0.00; ASST×158 yday $21.50 → 09:30 $22.45 +150.10 | — |
| 2026-08-27 09:30 ET | **SELL** | `REAX` | 127 | $25.91 | $2.42 | $+223.81 | $3,304.63 | ▲ +223.81 after sell → book $13,340.10; vs 09:30 mark -2.41 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1963 | $1.75 | $25.68 | $+321.97 | $6,714.20 | ▲ +321.97 after sell → book $13,314.42; vs 09:30 mark -25.68 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 158 | $22.45 | $2.52 | $+533.80 | $10,258.78 | ▲ +533.80 after sell → book $13,311.90; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 372 | $9.19 | $4.80 | — | $6,835.30 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $3419.59 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 23 | $144.18 | $2.06 | — | $3,517.10 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; ret5=-14.2; leftover $3419.59 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 184 | $18.50 | $2.54 | — | $110.56 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; leftover $3419.59 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.56 | ▲ close $13,306.19 vs 09:30 $13,342.51 (session +3.69) | 16:00 close · cash $110.56 · equity $13,306.19 vs 09:30 $13,342.51 (-36.32; session marks +3.69) · 4 name(s) marked open→close (per-name table). XHG×752 09:30 $4.06 → close $3.80 -195.52; CAPR×372 09:30 $9.19 → close $10.06 +323.64; MRNA×23 09:30 $144.18 → close $142.77 -32.43; BZ×184 09:30 $18.50 → close $18.00 -92.00 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.56 | ▼ 09:30 equity $12,999.97 vs yday $13,306.19 (-306.22) | 09:30 open · cash $110.56 (unchanged overnight, no fees) · equity $12,999.97 vs prior close $13,306.19 (-306.22) · 4 name(s) re-marked at the open (per-name table). XHG×752 yday $3.80 → 09:30 $3.69 -82.72; CAPR×372 yday $10.06 → 09:30 $9.73 -122.76; MRNA×23 yday $142.77 → 09:30 $137.19 -128.34; BZ×184 yday $18.00 → 09:30 $18.15 +27.60 | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 752 | $3.69 | $9.85 | $-305.31 | $2,875.59 | ▼ -305.31 after sell → book $12,990.12; vs 09:30 mark -9.85 | dropped from list after 3 sess (min 2) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 184 | $18.15 | $2.60 | $-69.54 | $6,212.59 | ▼ -69.54 after sell → book $12,987.52; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 221 | $14.00 | $2.85 | — | $3,115.74 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; ret5=-3.3; leftover $3106.30 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 21 | $146.07 | $2.05 | — | $46.22 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $3106.30 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.22 | ▼ close $12,967.35 vs 09:30 $12,999.97 (session -15.27) | 16:00 close · cash $46.22 · equity $12,967.35 vs 09:30 $12,999.97 (-32.62; session marks -15.27) · 4 name(s) marked open→close (per-name table). CAPR×372 09:30 $9.73 → close $9.59 -52.08; MRNA×23 09:30 $137.19 → close $137.99 +18.40; BYND×221 09:30 $14.00 → close $13.86 -30.94; ANF×21 09:30 $146.07 → close $148.42 +49.35 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.22 | ▼ 09:30 equity $12,825.16 vs yday $12,967.35 (-142.19) | 09:30 open · cash $46.22 (unchanged overnight, no fees) · equity $12,825.16 vs prior close $12,967.35 (-142.19) · 4 name(s) re-marked at the open (per-name table). CAPR×372 yday $9.59 → 09:30 $9.50 -33.48; MRNA×23 yday $137.99 → 09:30 $134.10 -89.47; BYND×221 yday $13.86 → 09:30 $13.81 -11.05; ANF×21 yday $148.42 → 09:30 $148.03 -8.19 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 372 | $9.50 | $4.89 | $+105.63 | $3,575.33 | ▲ +105.63 after sell → book $12,820.27; vs 09:30 mark -4.89 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 23 | $134.10 | $2.09 | $-235.99 | $6,657.54 | ▼ -235.99 after sell → book $12,818.18; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,657.54 | ▼ close $12,601.52 vs 09:30 $12,825.16 (session -216.66) | 16:00 close · cash $6,657.54 · equity $12,601.52 vs 09:30 $12,825.16 (-223.64; session marks -216.66) · 2 name(s) marked open→close (per-name table). BYND×221 09:30 $13.81 → close $13.30 -112.71; ANF×21 09:30 $148.03 → close $143.08 -103.95 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,657.54 | ▼ 09:30 equity $12,521.38 vs yday $12,601.52 (-80.14) | 09:30 open · cash $6,657.54 (unchanged overnight, no fees) · equity $12,521.38 vs prior close $12,601.52 (-80.14) · 2 name(s) re-marked at the open (per-name table). BYND×221 yday $13.30 → 09:30 $13.04 -57.46; ANF×21 yday $143.08 → 09:30 $142.00 -22.68 | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 221 | $13.04 | $2.91 | $-217.92 | $9,536.47 | ▼ -217.92 after sell → book $12,518.47; vs 09:30 mark -2.91 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 09:30 ET | **SELL** | `ANF` | 21 | $142.00 | $2.09 | $-89.61 | $12,516.38 | ▼ -89.61 after sell → book $12,516.38; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,516.38 | ▲ close $12,516.38 vs 09:30 $12,521.38 (session +0.00) | 16:00 close · cash $12,516.38 · no lots left · equity $12,516.38. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,516.38 | ▲ 09:30 equity $12,516.38 vs yday $12,516.38 (+0.00) | 09:30 open · cash $12,516.38 · no holdings · equity $12,516.38 vs prior close $12,516.38 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,516.38 | ▲ close $12,516.38 vs 09:30 $12,516.38 (session +0.00) | 16:00 close · cash $12,516.38 · no lots left · equity $12,516.38. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,516.38 | ▲ 09:30 equity $12,516.38 vs yday $12,516.38 (+0.00) | 09:30 open · cash $12,516.38 · no holdings · equity $12,516.38 vs prior close $12,516.38 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1757 | $1.78 | $22.67 | — | $9,366.26 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; leftover $3129.10 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 170 | $18.40 | $2.50 | — | $6,235.76 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; leftover $3129.10 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 228 | $13.71 | $2.94 | — | $3,106.93 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; leftover $3129.10 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 130 | $23.88 | $2.38 | — | $0.15 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $3129.10 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.15 | ▼ close $11,825.10 vs 09:30 $12,516.38 (session -660.79) | 16:00 close · cash $0.15 · equity $11,825.10 vs 09:30 $12,516.38 (-691.28; session marks -660.79) · 4 name(s) marked open→close (per-name table). GPRO×1757 09:30 $1.78 → close $1.39 -685.23; REAX×170 09:30 $18.40 → close $18.40 +0.00; CNH×228 09:30 $13.71 → close $13.84 +29.64; MMED×130 09:30 $23.88 → close $23.84 -5.20 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.15 | ▲ 09:30 equity $11,952.13 vs yday $11,825.10 (+127.03) | 09:30 open · cash $0.15 (unchanged overnight, no fees) · equity $11,952.13 vs prior close $11,825.10 (+127.03) · 4 name(s) re-marked at the open (per-name table). GPRO×1757 yday $1.39 → 09:30 $1.48 +158.13; REAX×170 yday $18.40 → 09:30 $18.15 -42.50; CNH×228 yday $13.84 → 09:30 $13.89 +11.40; MMED×130 yday $23.84 → 09:30 $23.84 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 170 | $18.15 | $2.55 | $-47.55 | $3,083.10 | ▼ -47.55 after sell → book $11,949.58; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 228 | $13.89 | $3.00 | $+35.09 | $6,247.02 | ▲ +35.09 after sell → book $11,946.58; vs 09:30 mark -3.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 130 | $23.84 | $2.43 | $-10.01 | $9,343.79 | ▼ -10.01 after sell → book $11,944.15; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 123 | $25.18 | $2.36 | — | $6,244.29 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; leftover $3114.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 395 | $7.87 | $5.10 | — | $3,130.55 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; leftover $3114.60 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 537 | $5.79 | $6.93 | — | $14.39 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $3114.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.39 | ▲ close $12,624.05 vs 09:30 $11,952.13 (session +694.28) | 16:00 close · cash $14.39 · equity $12,624.05 vs 09:30 $11,952.13 (+671.92; session marks +694.28) · 4 name(s) marked open→close (per-name table). GPRO×1757 09:30 $1.48 → close $1.70 +386.54; ASST×123 09:30 $25.18 → close $27.14 +241.08; USDE×395 09:30 $7.87 → close $7.93 +23.70; DFDV×537 09:30 $5.79 → close $5.87 +42.96 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.39 | ▼ 09:30 equity $12,201.38 vs yday $12,624.05 (-422.67) | 09:30 open · cash $14.39 (unchanged overnight, no fees) · equity $12,201.38 vs prior close $12,624.05 (-422.67) · 4 name(s) re-marked at the open (per-name table). GPRO×1757 yday $1.70 → 09:30 $1.56 -237.20; ASST×123 yday $27.14 → 09:30 $26.44 -86.10; USDE×395 yday $7.93 → 09:30 $7.76 -67.15; DFDV×537 yday $5.87 → 09:30 $5.81 -32.22 | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 1757 | $1.56 | $22.98 | $-423.40 | $2,741.12 | ▼ -423.40 after sell → book $12,178.41; vs 09:30 mark -22.97 | dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,741.12 | ▲ close $12,304.38 vs 09:30 $12,201.38 (session +125.97) | 16:00 close · cash $2,741.12 · equity $12,304.38 vs 09:30 $12,201.38 (+103.00; session marks +125.97) · 3 name(s) marked open→close (per-name table). ASST×123 09:30 $26.44 → close $27.16 +88.56; USDE×395 09:30 $7.76 → close $7.61 -59.25; DFDV×537 09:30 $5.81 → close $5.99 +96.66 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,741.12 | ▲ 09:30 equity $12,581.81 vs yday $12,304.38 (+277.43) | 09:30 open · cash $2,741.12 (unchanged overnight, no fees) · equity $12,581.81 vs prior close $12,304.38 (+277.43) · 3 name(s) re-marked at the open (per-name table). ASST×123 yday $27.16 → 09:30 $28.00 +103.32; USDE×395 yday $7.61 → 09:30 $8.01 +158.00; DFDV×537 yday $5.99 → 09:30 $6.02 +16.11 | — |
| 2026-09-09 09:30 ET | **SELL** | `ASST` | 123 | $28.00 | $2.41 | $+342.09 | $6,182.71 | ▲ +342.09 after sell → book $12,579.40; vs 09:30 mark -2.41 | dropped from list after 2 sess (min 2) | — |
| 2026-09-09 09:30 ET | **SELL** | `USDE` | 395 | $8.01 | $5.19 | $+45.02 | $9,341.47 | ▲ +45.02 after sell → book $12,574.21; vs 09:30 mark -5.19 | dropped from list after 2 sess (min 2) | — |
| 2026-09-09 09:30 ET | **SELL** | `DFDV` | 537 | $6.02 | $7.04 | $+109.54 | $12,567.17 | ▲ +109.54 after sell → book $12,567.17; vs 09:30 mark -7.04 | dropped from list after 2 sess (min 2) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,567.17 | ▲ close $12,567.17 vs 09:30 $12,581.81 (session +0.00) | 16:00 close · cash $12,567.17 · no lots left · equity $12,567.17. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,567.17 | ▲ 09:30 equity $12,567.17 vs yday $12,567.17 (-0.00) | 09:30 open · cash $12,567.17 · no holdings · equity $12,567.17 vs prior close $12,567.17 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,567.17 | ▲ close $12,567.17 vs 09:30 $12,567.17 (session +0.00) | 16:00 close · cash $12,567.17 · no lots left · equity $12,567.17. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,567.17 | ▲ 09:30 equity $12,567.17 vs yday $12,567.17 (-0.00) | 09:30 open · cash $12,567.17 · no holdings · equity $12,567.17 vs prior close $12,567.17 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 1163 | $2.70 | $15.00 | — | $9,412.07 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $3141.79 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 639 | $4.91 | $8.24 | — | $6,266.33 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $3141.79 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 510 | $6.16 | $6.58 | — | $3,118.15 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; leftover $3141.79 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 992 | $3.13 | $12.80 | — | $0.40 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; leftover $3141.79 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.40 | ▲ close $12,846.47 vs 09:30 $12,567.17 (session +321.92) | 16:00 close · cash $0.40 · equity $12,846.47 vs 09:30 $12,567.17 (+279.30; session marks +321.92) · 4 name(s) marked open→close (per-name table). INDP×1163 09:30 $2.70 → close $2.77 +81.41; BNC×639 09:30 $4.91 → close $4.80 -70.29; IRD×510 09:30 $6.16 → close $6.04 -61.20; CMRC×992 09:30 $3.13 → close $3.50 +372.00 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.40 | ▲ 09:30 equity $13,023.09 vs yday $12,846.47 (+176.62) | 09:30 open · cash $0.40 (unchanged overnight, no fees) · equity $13,023.09 vs prior close $12,846.47 (+176.62) · 4 name(s) re-marked at the open (per-name table). INDP×1163 yday $2.77 → 09:30 $2.80 +34.89; BNC×639 yday $4.80 → 09:30 $5.03 +146.97; IRD×510 yday $6.04 → 09:30 $6.02 -10.20; CMRC×992 yday $3.50 → 09:30 $3.51 +4.96 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.40 | ▲ close $13,675.33 vs 09:30 $13,023.09 (session +652.24) | 16:00 close · cash $0.40 · equity $13,675.33 vs 09:30 $13,023.09 (+652.24; session marks +652.24) · 4 name(s) marked open→close (per-name table). INDP×1163 09:30 $2.80 → close $3.14 +395.42; BNC×639 09:30 $5.03 → close $5.27 +153.36; IRD×510 09:30 $6.02 → close $5.97 -25.50; CMRC×992 09:30 $3.51 → close $3.64 +128.96 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.40 | ▲ 09:30 equity $13,860.17 vs yday $13,675.33 (+184.84) | 09:30 open · cash $0.40 (unchanged overnight, no fees) · equity $13,860.17 vs prior close $13,675.33 (+184.84) · 4 name(s) re-marked at the open (per-name table). INDP×1163 yday $3.14 → 09:30 $3.40 +302.38; BNC×639 yday $5.27 → 09:30 $5.11 -102.24; IRD×510 yday $5.97 → 09:30 $5.94 -15.30; CMRC×992 yday $3.64 → 09:30 $3.64 +0.00 | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 639 | $5.11 | $8.38 | $+111.18 | $3,257.31 | ▲ +111.18 after sell → book $13,851.79; vs 09:30 mark -8.38 | dropped from list after 2 sess (min 2) | — |
| 2026-09-15 09:30 ET | **SELL** | `IRD` | 510 | $5.94 | $6.69 | $-125.47 | $6,280.02 | ▼ -125.47 after sell → book $13,845.10; vs 09:30 mark -6.69 | dropped from list after 2 sess (min 2) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 992 | $3.64 | $12.99 | $+480.13 | $9,877.91 | ▲ +480.13 after sell → book $13,832.11; vs 09:30 mark -12.99 | dropped from list after 2 sess (min 2) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,877.91 | ▲ close $14,111.23 vs 09:30 $13,860.17 (session +279.12) | 16:00 close · cash $9,877.91 · equity $14,111.23 vs 09:30 $13,860.17 (+251.06; session marks +279.12) · 1 name(s) marked open→close (per-name table). INDP×1163 09:30 $3.40 → close $3.64 +279.12 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,877.91 | ▲ 09:30 equity $14,134.49 vs yday $14,111.23 (+23.26) | 09:30 open · cash $9,877.91 (unchanged overnight, no fees) · equity $14,134.49 vs prior close $14,111.23 (+23.26) · 1 name(s) re-marked at the open (per-name table). INDP×1163 yday $3.64 → 09:30 $3.66 +23.26 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 1829 | $1.80 | $23.59 | — | $6,562.12 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $3292.64 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 141 | $23.29 | $2.41 | — | $3,275.82 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; leftover $3292.64 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 223 | $14.62 | $2.88 | — | $12.68 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; leftover $3292.64 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.68 | ▼ close $14,092.74 vs 09:30 $14,134.49 (session -12.87) | 16:00 close · cash $12.68 · equity $14,092.74 vs 09:30 $14,134.49 (-41.75; session marks -12.87) · 4 name(s) marked open→close (per-name table). INDP×1163 09:30 $3.66 → close $3.21 -523.35; HLP×1829 09:30 $1.80 → close $2.07 +493.83; SDGR×141 09:30 $23.29 → close $23.93 +90.24; SSL×223 09:30 $14.62 → close $14.29 -73.59 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.68 | ▲ 09:30 equity $14,158.88 vs yday $14,092.74 (+66.14) | 09:30 open · cash $12.68 (unchanged overnight, no fees) · equity $14,158.88 vs prior close $14,092.74 (+66.14) · 4 name(s) re-marked at the open (per-name table). INDP×1163 yday $3.21 → 09:30 $3.30 +104.67; HLP×1829 yday $2.07 → 09:30 $2.10 +54.87; SDGR×141 yday $23.93 → 09:30 $24.09 +22.56; SSL×223 yday $14.29 → 09:30 $13.77 -115.96 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.68 | ▲ close $15,694.91 vs 09:30 $14,158.88 (session +1,536.03) | 16:00 close · cash $12.68 · equity $15,694.91 vs 09:30 $14,158.88 (+1536.03; session marks +1536.03) · 4 name(s) marked open→close (per-name table). INDP×1163 09:30 $3.30 → close $3.93 +732.69; HLP×1829 09:30 $2.10 → close $2.02 -146.32; SDGR×141 09:30 $24.09 → close $30.24 +867.15; SSL×223 09:30 $13.77 → close $14.14 +82.51 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.68 | ▼ 09:30 equity $15,315.58 vs yday $15,694.91 (-379.33) | 09:30 open · cash $12.68 (unchanged overnight, no fees) · equity $15,315.58 vs prior close $15,694.91 (-379.33) · 4 name(s) re-marked at the open (per-name table). INDP×1163 yday $3.93 → 09:30 $3.85 -93.04; HLP×1829 yday $2.02 → 09:30 $1.96 -109.74; SDGR×141 yday $30.24 → 09:30 $29.32 -129.72; SSL×223 yday $14.14 → 09:30 $13.93 -46.83 | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 1829 | $1.96 | $23.93 | $+245.12 | $3,573.59 | ▲ +245.12 after sell → book $15,291.65; vs 09:30 mark -23.93 | dropped from list after 2 sess (min 2) | — |
| 2026-09-18 09:30 ET | **SELL** | `SSL` | 223 | $13.93 | $2.94 | $-159.69 | $6,677.05 | ▼ -159.69 after sell → book $15,288.72; vs 09:30 mark -2.93 | dropped from list after 2 sess (min 2) | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 1100 | $3.04 | $14.19 | — | $3,324.36 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $3338.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 40 | $81.40 | $2.11 | — | $66.25 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $3338.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $66.25 | ▲ close $15,360.32 vs 09:30 $15,315.58 (session +87.90) | 16:00 close · cash $66.25 · equity $15,360.32 vs 09:30 $15,315.58 (+44.74; session marks +87.90) · 4 name(s) marked open→close (per-name table). INDP×1163 09:30 $3.85 → close $3.55 -348.90; SDGR×141 09:30 $29.32 → close $29.02 -42.30; CYPH×1100 09:30 $3.04 → close $3.60 +621.50; TEM×40 09:30 $81.40 → close $77.84 -142.40 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $66.25 | ▲ 09:30 equity $15,907.73 vs yday $15,360.32 (+547.41) | 09:30 open · cash $66.25 (unchanged overnight, no fees) · equity $15,907.73 vs prior close $15,360.32 (+547.41) · 4 name(s) re-marked at the open (per-name table). INDP×1163 yday $3.55 → 09:30 $3.55 +0.00; SDGR×141 yday $29.02 → 09:30 $29.43 +57.81; CYPH×1100 yday $3.60 → 09:30 $4.00 +440.00; TEM×40 yday $77.84 → 09:30 $79.08 +49.60 | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 1163 | $3.55 | $15.23 | $+958.32 | $4,179.67 | ▲ +958.32 after sell → book $15,892.50; vs 09:30 mark -15.23 | dropped from list after 6 sess (min 2) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 141 | $29.43 | $2.47 | $+860.86 | $8,326.83 | ▲ +860.86 after sell → book $15,890.03; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 2) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 842 | $2.47 | $10.86 | — | $6,236.23 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; leftover $2081.71 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 123 | $16.91 | $2.36 | — | $4,153.94 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; leftover $2081.71 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 1261 | $1.65 | $16.27 | — | $2,057.02 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; leftover $2081.71 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 176 | $11.67 | $2.52 | — | $0.58 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; leftover $2081.71 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.58 | ▼ close $15,417.61 vs 09:30 $15,907.73 (session -440.41) | 16:00 close · cash $0.58 · equity $15,417.61 vs 09:30 $15,907.73 (-490.12; session marks -440.41) · 6 name(s) marked open→close (per-name table). CYPH×1100 09:30 $4.00 → close $3.40 -660.00; TEM×40 09:30 $79.08 → close $78.03 -42.00; FEAM×842 09:30 $2.47 → close $2.48 +8.42; TJGC×123 09:30 $16.91 → close $17.58 +82.41; LVWR×1261 09:30 $1.65 → close $1.53 -151.32; SECZ×176 09:30 $11.67 → close $13.50 +322.08 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.58 | ▼ 09:30 equity $15,395.72 vs yday $15,417.61 (-21.89) | 09:30 open · cash $0.58 (unchanged overnight, no fees) · equity $15,395.72 vs prior close $15,417.61 (-21.89) · 6 name(s) re-marked at the open (per-name table). CYPH×1100 yday $3.40 → 09:30 $3.51 +121.00; TEM×40 yday $78.03 → 09:30 $77.99 -1.60; FEAM×842 yday $2.48 → 09:30 $2.47 -8.42; TJGC×123 yday $17.58 → 09:30 $17.58 +0.00; LVWR×1261 yday $1.53 → 09:30 $1.50 -37.83; SECZ×176 yday $13.50 → 09:30 $12.96 -95.04 | — |
| 2026-09-22 09:30 ET | **SELL** | `CYPH` | 1100 | $3.51 | $14.40 | $+493.91 | $3,847.18 | ▲ +493.91 after sell → book $15,381.32; vs 09:30 mark -14.40 | dropped from list after 2 sess (min 2) | — |
| 2026-09-22 09:30 ET | **SELL** | `TEM` | 40 | $77.99 | $2.15 | $-140.66 | $6,964.63 | ▼ -140.66 after sell → book $15,379.17; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 2) | — |
| 2026-09-22 09:30 ET | **BUY** | `GRAL` | 21 | $106.75 | $2.05 | — | $4,720.83 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+42.9; leftover $2321.54 | join🔴 sector🔴 gen🔴 news🟢 digest🟡 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 321 | $7.23 | $4.14 | — | $2,395.86 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; leftover $2321.54 | join🟡 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-22 09:30 ET | **BUY** | `INDP` | 748 | $3.10 | $9.65 | — | $67.41 | — | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; ret5=-1.6; leftover $2321.54 | join🔴 sector🔴 gen🔴 news🟡 digest🟡 judge🔴 ab🟡 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.41 | ▲ close $16,240.45 vs 09:30 $15,395.72 (session +877.12) | 16:00 close · cash $67.41 · equity $16,240.45 vs 09:30 $15,395.72 (+844.73; session marks +877.12) · 7 name(s) marked open→close (per-name table). FEAM×842 09:30 $2.47 → close $2.94 +395.74; TJGC×123 09:30 $17.58 → close $16.90 -83.64; LVWR×1261 09:30 $1.50 → close $1.45 -63.05; SECZ×176 09:30 $12.96 → close $13.00 +7.04; GRAL×21 09:30 $106.75 → close $108.52 +37.17; NUAI×321 09:30 $7.23 → close $6.97 -81.86; INDP×748 09:30 $3.10 → close $3.99 +665.72 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `QMCO` | cash | leftover split 0.13 < 1 share @ 24.68 |
| 2026-08-14 | `ARX` | cash | leftover split 0.13 < 1 share @ 19.57 |
| 2026-08-14 | `ZENA` | cash | leftover split 0.13 < 1 share @ 2.20 |
| 2026-08-14 | `AIRO` | cash | leftover split 0.13 < 1 share @ 11.12 |
| 2026-08-18 | `XHG` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `STDN` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `HTFL` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `AZI` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `XHG` | cash | leftover split 0.12 < 1 share @ 4.49 |
| 2026-08-21 | `CAPR` | cash | leftover split 0.12 < 1 share @ 6.81 |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `ASST` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `BYND` | cash | leftover split 5.49 < 1 share @ 14.11 |
| 2026-08-26 | `USDE` | cash | leftover split 5.49 < 1 share @ 5.81 |
| 2026-08-26 | `PURR` | cash | leftover split 5.49 < 1 share @ 11.59 |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `ASST` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-08 | `USDE` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-08 | `DFDV` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `IRD` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `XHLD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `SDGR` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-17 | `SSL` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-17 | `BBNX` | cash | leftover split 6.34 < 1 share @ 22.46 |
| 2026-09-17 | `FPS` | cash | leftover split 6.34 < 1 share @ 36.76 |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-21 | `TEM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-22 | `FEAM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-22 | `TJGC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-22 | `LVWR` | min_hold | dropped but min-hold 1/2 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FEAM` | 842 | 2026-09-21 @ $2.47 | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; leftover $2081.71 |
| `TJGC` | 123 | 2026-09-21 @ $16.91 | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; leftover $2081.71 |
| `LVWR` | 1261 | 2026-09-21 @ $1.65 | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; leftover $2081.71 |
| `SECZ` | 176 | 2026-09-21 @ $11.67 | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; leftover $2081.71 |
| `GRAL` | 21 | 2026-09-22 @ $106.75 | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+42.9; leftover $2321.54 |
| `NUAI` | 321 | 2026-09-22 @ $7.23 | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list yday_gainer,yday_mover; ret5=+36.6; leftover $2321.54 |
| `INDP` | 748 | 2026-09-22 @ $3.10 | hot4; S>0 lots stay through the next 09:30 so the overnight gap is in the book; rank hot_score; list ohlc_hot; ret5=-1.6; leftover $2321.54 |
