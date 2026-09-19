# Factor mine action — `union_white_both_n4_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `list` · size `leftover` · sell `list` · S-boost `none` · −0 red + yday up AND catalyst, top 4 by Score

Cash book **+19.41%** ($11,941) · signal-only (no cash/fees) was +18.10%. Starts YES **7/26**. Fills 63 · skips 124 · realized $+2046.66.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `cam_bad_max=0,yday_and_catalyst=True` · **rank** `list` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $195.74.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 83 | — | $59.80 | +0.00 | $60.23 | +35.69 | +35.69 | +0.00 | +35.69 |
| 2026-08-13 | `TPG` | 98 | — | $50.62 | +0.00 | $54.62 | +391.69 | +391.69 | +0.00 | +391.69 |
| 2026-08-14 | `BTSG` | 83 | $60.23 | $59.65 | -48.14 | $61.71 | +170.98 | +122.84 | -12.45 | +158.53 |
| 2026-08-14 | `TPG` | 98 | $54.62 | $55.29 | +65.66 | $53.03 | -221.48 | -155.82 | +457.35 | +235.87 |
| 2026-08-14 | `BETR` | 1 | — | $14.80 | +0.00 | $13.73 | -1.07 | -1.07 | +0.00 | -1.07 |
| 2026-08-14 | `ANGX` | 4 | — | $4.31 | +0.00 | $4.37 | +0.24 | +0.24 | +0.00 | +0.24 |
| 2026-08-17 | `BTSG` | 83 | $61.71 | $61.69 | -1.66 | $60.38 | -108.73 | -110.39 | +156.87 | +48.14 |
| 2026-08-17 | `TPG` | 98 | $53.03 | $52.67 | -35.28 | $51.77 | -88.20 | -123.48 | +200.59 | +112.39 |
| 2026-08-17 | `BETR` | 1 | $13.73 | $13.67 | -0.06 | $13.54 | -0.13 | -0.19 | -1.13 | -1.26 |
| 2026-08-17 | `ANGX` | 4 | $4.37 | $4.60 | +0.92 | $4.71 | +0.44 | +1.36 | +1.16 | +1.60 |
| 2026-08-17 | `ABX` | 1 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-18 | `BTSG` | 83 | $60.38 | $60.00 | -31.54 | $59.50 | -41.50 | -73.04 | +16.60 | -24.90 |
| 2026-08-18 | `TPG` | 98 | $51.77 | $51.77 | +0.00 | $52.02 | +24.50 | +24.50 | +112.39 | +136.89 |
| 2026-08-18 | `BETR` | 1 | $13.54 | $13.21 | -0.33 | $13.05 | -0.16 | -0.49 | -1.59 | -1.75 |
| 2026-08-18 | `ANGX` | 4 | $4.71 | $4.79 | +0.32 | $4.85 | +0.24 | +0.56 | +1.92 | +2.16 |
| 2026-08-18 | `ABX` | 1 | $9.12 | $9.03 | -0.09 | $9.01 | -0.02 | -0.11 | -0.09 | -0.11 |
| 2026-08-19 | `BTSG` | 83 | $59.50 | $60.15 | +53.95 | $59.33 | -68.06 | -14.11 | +29.05 | -39.01 |
| 2026-08-19 | `TPG` | 98 | $52.02 | $52.26 | +23.52 | $53.18 | +90.16 | +113.68 | +160.41 | +250.57 |
| 2026-08-19 | `BETR` | 1 | $13.05 | $13.03 | -0.02 | $13.03 | +0.00 | -0.02 | -1.77 | -1.77 |
| 2026-08-19 | `ANGX` | 4 | $4.85 | $4.79 | -0.24 | $4.60 | -0.76 | -1.00 | +1.92 | +1.16 |
| 2026-08-19 | `ABX` | 1 | $9.01 | $9.08 | +0.07 | $9.15 | +0.07 | +0.14 | -0.04 | +0.03 |
| 2026-08-20 | `BTSG` | 83 | $59.33 | $58.64 | -57.27 | — | +0.00 | -57.27 | -96.28 | — |
| 2026-08-20 | `TPG` | 98 | $53.18 | $53.06 | -11.76 | — | +0.00 | -11.76 | +238.81 | — |
| 2026-08-20 | `BETR` | 1 | $13.03 | $12.95 | -0.08 | $11.60 | -1.35 | -1.43 | -1.85 | -3.20 |
| 2026-08-20 | `ANGX` | 4 | $4.60 | $4.57 | -0.12 | $4.37 | -0.80 | -0.92 | +1.04 | +0.24 |
| 2026-08-20 | `ABX` | 1 | $9.15 | $9.13 | -0.02 | $9.16 | +0.03 | +0.01 | +0.01 | +0.04 |
| 2026-08-20 | `BHP` | 27 | — | $91.01 | +0.00 | $93.63 | +70.74 | +70.74 | +0.00 | +70.74 |
| 2026-08-20 | `KGC` | 85 | — | $29.63 | +0.00 | $31.43 | +153.00 | +153.00 | +0.00 | +153.00 |
| 2026-08-20 | `WPM` | 17 | — | $144.54 | +0.00 | $150.25 | +97.07 | +97.07 | +0.00 | +97.07 |
| 2026-08-20 | `CYPH` | 2193 | — | $1.15 | +0.00 | $1.19 | +87.72 | +87.72 | +0.00 | +87.72 |
| 2026-08-21 | `BETR` | 1 | $11.60 | $11.73 | +0.13 | — | +0.00 | +0.13 | -3.07 | — |
| 2026-08-21 | `ANGX` | 4 | $4.37 | $4.43 | +0.24 | — | +0.00 | +0.24 | +0.48 | — |
| 2026-08-21 | `ABX` | 1 | $9.16 | $9.13 | -0.03 | $9.66 | +0.53 | +0.50 | +0.01 | +0.54 |
| 2026-08-21 | `BHP` | 27 | $93.63 | $95.72 | +56.43 | $97.03 | +35.37 | +91.80 | +127.17 | +162.54 |
| 2026-08-21 | `KGC` | 85 | $31.43 | $32.17 | +62.90 | $32.76 | +50.15 | +113.05 | +215.90 | +266.05 |
| 2026-08-21 | `WPM` | 17 | $150.25 | $154.70 | +75.65 | $157.78 | +52.36 | +128.01 | +172.72 | +225.08 |
| 2026-08-21 | `CYPH` | 2193 | $1.19 | $1.32 | +285.09 | $1.42 | +219.30 | +504.39 | +372.81 | +592.11 |
| 2026-08-21 | `AUPH` | 2 | — | $17.20 | +0.00 | $16.65 | -1.10 | -1.10 | +0.00 | -1.10 |
| 2026-08-21 | `ARCT` | 3 | — | $11.13 | +0.00 | $13.45 | +6.96 | +6.96 | +0.00 | +6.96 |
| 2026-08-24 | `ABX` | 1 | $9.66 | $9.76 | +0.10 | — | +0.00 | +0.10 | +0.64 | — |
| 2026-08-24 | `BHP` | 27 | $97.03 | $97.31 | +7.56 | $97.13 | -4.86 | +2.70 | +170.10 | +165.24 |
| 2026-08-24 | `KGC` | 85 | $32.76 | $33.03 | +22.95 | $32.98 | -4.25 | +18.70 | +289.00 | +284.75 |
| 2026-08-24 | `WPM` | 17 | $157.78 | $159.50 | +29.24 | $160.19 | +11.73 | +40.97 | +254.32 | +266.05 |
| 2026-08-24 | `CYPH` | 2193 | $1.42 | $1.83 | +899.13 | $1.68 | -328.95 | +570.18 | +1491.24 | +1162.29 |
| 2026-08-24 | `AUPH` | 2 | $16.65 | $16.57 | -0.16 | $16.57 | +0.00 | -0.16 | -1.26 | -1.26 |
| 2026-08-24 | `ARCT` | 3 | $13.45 | $13.33 | -0.36 | $14.34 | +3.03 | +2.67 | +6.60 | +9.63 |
| 2026-08-25 | `BHP` | 27 | $97.13 | $95.86 | -34.29 | $98.69 | +76.41 | +42.12 | +130.95 | +207.36 |
| 2026-08-25 | `KGC` | 85 | $32.98 | $32.32 | -56.10 | $33.48 | +98.60 | +42.50 | +228.65 | +327.25 |
| 2026-08-25 | `WPM` | 17 | $160.19 | $156.51 | -62.56 | $163.72 | +122.57 | +60.01 | +203.49 | +326.06 |
| 2026-08-25 | `CYPH` | 2193 | $1.68 | $1.56 | -263.16 | $1.64 | +175.44 | -87.72 | +899.13 | +1074.57 |
| 2026-08-25 | `AUPH` | 2 | $16.57 | $16.63 | +0.12 | $16.75 | +0.24 | +0.36 | -1.14 | -0.90 |
| 2026-08-25 | `ARCT` | 3 | $14.34 | $14.12 | -0.66 | $15.44 | +3.96 | +3.30 | +8.97 | +12.93 |
| 2026-08-25 | `CRMD` | 2 | — | $8.35 | +0.00 | $8.56 | +0.42 | +0.42 | +0.00 | +0.42 |
| 2026-08-25 | `BMEA` | 14 | — | $1.63 | +0.00 | $1.73 | +1.40 | +1.40 | +0.00 | +1.40 |
| 2026-08-26 | `BHP` | 27 | $98.69 | $96.99 | -45.90 | $96.33 | -17.82 | -63.72 | +161.46 | +143.64 |
| 2026-08-26 | `KGC` | 85 | $33.48 | $32.90 | -49.30 | $32.32 | -49.30 | -98.60 | +277.95 | +228.65 |
| 2026-08-26 | `WPM` | 17 | $163.72 | $160.93 | -47.43 | $156.02 | -83.47 | -130.90 | +278.63 | +195.16 |
| 2026-08-26 | `CYPH` | 2193 | $1.64 | $1.60 | -87.72 | $1.63 | +65.79 | -21.93 | +986.85 | +1052.64 |
| 2026-08-26 | `AUPH` | 2 | $16.75 | $16.60 | -0.30 | $16.54 | -0.12 | -0.42 | -1.20 | -1.32 |
| 2026-08-26 | `ARCT` | 3 | $15.44 | $15.35 | -0.27 | $15.83 | +1.44 | +1.17 | +12.66 | +14.10 |
| 2026-08-26 | `CRMD` | 2 | $8.56 | $8.60 | +0.08 | $8.39 | -0.42 | -0.34 | +0.50 | +0.08 |
| 2026-08-26 | `BMEA` | 14 | $1.73 | $1.75 | +0.35 | $1.71 | -0.63 | -0.28 | +1.75 | +1.12 |
| 2026-08-27 | `BHP` | 27 | $96.33 | $95.52 | -21.87 | — | +0.00 | -21.87 | +121.77 | — |
| 2026-08-27 | `KGC` | 85 | $32.32 | $32.32 | +0.00 | — | +0.00 | +0.00 | +228.65 | — |
| 2026-08-27 | `WPM` | 17 | $156.02 | $155.89 | -2.21 | — | +0.00 | -2.21 | +192.95 | — |
| 2026-08-27 | `CYPH` | 2193 | $1.63 | $1.75 | +263.16 | — | +0.00 | +263.16 | +1315.80 | — |
| 2026-08-27 | `AUPH` | 2 | $16.54 | $16.47 | -0.14 | $16.48 | +0.01 | -0.13 | -1.46 | -1.45 |
| 2026-08-27 | `ARCT` | 3 | $15.83 | $15.74 | -0.27 | $16.17 | +1.29 | +1.02 | +13.83 | +15.12 |
| 2026-08-27 | `CRMD` | 2 | $8.39 | $8.49 | +0.20 | $8.31 | -0.36 | -0.16 | +0.28 | -0.08 |
| 2026-08-27 | `BMEA` | 14 | $1.71 | $1.74 | +0.42 | $1.68 | -0.84 | -0.42 | +1.54 | +0.70 |
| 2026-08-28 | `AUPH` | 2 | $16.48 | $16.44 | -0.07 | — | +0.00 | -0.07 | -1.52 | — |
| 2026-08-28 | `ARCT` | 3 | $16.17 | $15.43 | -2.22 | — | +0.00 | -2.22 | +12.90 | — |
| 2026-08-28 | `CRMD` | 2 | $8.31 | $8.28 | -0.06 | $8.30 | +0.04 | -0.02 | -0.14 | -0.10 |
| 2026-08-28 | `BMEA` | 14 | $1.68 | $1.69 | +0.14 | $1.73 | +0.56 | +0.70 | +0.84 | +1.40 |
| 2026-08-28 | `SMTC` | 20 | — | $141.76 | +0.00 | $131.17 | -211.80 | -211.80 | +0.00 | -211.80 |
| 2026-08-28 | `TTMI` | 24 | — | $122.81 | +0.00 | $118.65 | -99.84 | -99.84 | +0.00 | -99.84 |
| 2026-08-28 | `KEYS` | 9 | — | $324.41 | +0.00 | $319.97 | -39.96 | -39.96 | +0.00 | -39.96 |
| 2026-08-28 | `AVT` | 32 | — | $91.49 | +0.00 | $88.63 | -91.52 | -91.52 | +0.00 | -91.52 |
| 2026-08-31 | `CRMD` | 2 | $8.30 | $8.26 | -0.08 | $8.26 | +0.00 | -0.08 | -0.18 | -0.18 |
| 2026-08-31 | `BMEA` | 14 | $1.73 | $1.72 | -0.14 | $1.68 | -0.56 | -0.70 | +1.26 | +0.70 |
| 2026-08-31 | `SMTC` | 20 | $131.17 | $132.30 | +22.60 | $132.96 | +13.20 | +35.80 | -189.20 | -176.00 |
| 2026-08-31 | `TTMI` | 24 | $118.65 | $118.83 | +4.32 | $118.92 | +2.16 | +6.48 | -95.52 | -93.36 |
| 2026-08-31 | `KEYS` | 9 | $319.97 | $322.49 | +22.68 | $322.70 | +1.89 | +24.57 | -17.28 | -15.39 |
| 2026-08-31 | `AVT` | 32 | $88.63 | $89.39 | +24.32 | $89.56 | +5.44 | +29.76 | -67.20 | -61.76 |
| 2026-09-01 | `CRMD` | 2 | $8.26 | $8.25 | -0.02 | — | +0.00 | -0.02 | -0.20 | — |
| 2026-09-01 | `BMEA` | 14 | $1.68 | $1.68 | +0.00 | — | +0.00 | +0.00 | +0.70 | — |
| 2026-09-01 | `SMTC` | 20 | $132.96 | $127.63 | -106.60 | $132.27 | +92.80 | -13.80 | -282.60 | -189.80 |
| 2026-09-01 | `TTMI` | 24 | $118.92 | $116.68 | -53.76 | $115.33 | -32.40 | -86.16 | -147.12 | -179.52 |
| 2026-09-01 | `KEYS` | 9 | $322.70 | $321.47 | -11.07 | $319.27 | -19.80 | -30.87 | -26.46 | -46.26 |
| 2026-09-01 | `AVT` | 32 | $89.56 | $88.58 | -31.36 | $89.39 | +25.92 | -5.44 | -93.12 | -67.20 |
| 2026-09-02 | `SMTC` | 20 | $132.27 | $133.00 | +14.60 | $133.85 | +17.00 | +31.60 | -175.20 | -158.20 |
| 2026-09-02 | `TTMI` | 24 | $115.33 | $114.22 | -26.64 | $115.60 | +33.12 | +6.48 | -206.16 | -173.04 |
| 2026-09-02 | `KEYS` | 9 | $319.27 | $318.04 | -11.07 | $321.58 | +31.86 | +20.79 | -57.33 | -25.47 |
| 2026-09-02 | `AVT` | 32 | $89.39 | $89.39 | +0.00 | $89.99 | +19.20 | +19.20 | -67.20 | -48.00 |
| 2026-09-03 | `SMTC` | 20 | $133.85 | $133.10 | -15.00 | $135.40 | +46.00 | +31.00 | -173.20 | -127.20 |
| 2026-09-03 | `TTMI` | 24 | $115.60 | $115.21 | -9.36 | $115.39 | +4.32 | -5.04 | -182.40 | -178.08 |
| 2026-09-03 | `KEYS` | 9 | $321.58 | $319.09 | -22.41 | $322.62 | +31.77 | +9.36 | -47.88 | -16.11 |
| 2026-09-03 | `AVT` | 32 | $89.99 | $89.02 | -31.04 | $90.82 | +57.60 | +26.56 | -79.04 | -21.44 |
| 2026-09-03 | `ATRC` | 1 | — | $52.88 | +0.00 | $52.46 | -0.42 | -0.42 | +0.00 | -0.42 |
| 2026-09-03 | `HRMY` | 1 | — | $42.93 | +0.00 | $41.86 | -1.07 | -1.07 | +0.00 | -1.07 |
| 2026-09-03 | `CABA` | 20 | — | $3.63 | +0.00 | $3.48 | -3.00 | -3.00 | +0.00 | -3.00 |
| 2026-09-03 | `VSTM` | 9 | — | $8.03 | +0.00 | $7.98 | -0.45 | -0.45 | +0.00 | -0.45 |
| 2026-09-04 | `SMTC` | 20 | $135.40 | $138.71 | +66.20 | — | +0.00 | +66.20 | -61.00 | — |
| 2026-09-04 | `TTMI` | 24 | $115.39 | $118.58 | +76.56 | — | +0.00 | +76.56 | -101.52 | — |
| 2026-09-04 | `KEYS` | 9 | $322.62 | $326.10 | +31.32 | — | +0.00 | +31.32 | +15.21 | — |
| 2026-09-04 | `AVT` | 32 | $90.82 | $91.02 | +6.40 | — | +0.00 | +6.40 | -15.04 | — |
| 2026-09-04 | `ATRC` | 1 | $52.46 | $52.03 | -0.43 | $51.52 | -0.51 | -0.94 | -0.85 | -1.36 |
| 2026-09-04 | `HRMY` | 1 | $41.86 | $41.50 | -0.36 | $42.25 | +0.75 | +0.39 | -1.43 | -0.68 |
| 2026-09-04 | `CABA` | 20 | $3.48 | $3.46 | -0.40 | $3.47 | +0.20 | -0.20 | -3.40 | -3.20 |
| 2026-09-04 | `VSTM` | 9 | $7.98 | $7.91 | -0.63 | $8.20 | +2.61 | +1.98 | -1.08 | +1.53 |
| 2026-09-04 | `CRM` | 10 | — | $263.36 | +0.00 | $259.23 | -41.30 | -41.30 | +0.00 | -41.30 |
| 2026-09-04 | `DELL` | 5 | — | $513.78 | +0.00 | $524.14 | +51.80 | +51.80 | +0.00 | +51.80 |
| 2026-09-04 | `IRD` | 635 | — | $4.53 | +0.00 | $4.67 | +88.90 | +88.90 | +0.00 | +88.90 |
| 2026-09-04 | `LENZ` | 500 | — | $5.75 | +0.00 | $5.96 | +105.00 | +105.00 | +0.00 | +105.00 |
| 2026-09-08 | `ATRC` | 1 | $51.52 | $54.31 | +2.79 | $53.73 | -0.58 | +2.21 | +1.43 | +0.85 |
| 2026-09-08 | `HRMY` | 1 | $42.25 | $42.20 | -0.05 | $42.07 | -0.13 | -0.18 | -0.73 | -0.86 |
| 2026-09-08 | `CABA` | 20 | $3.47 | $3.43 | -0.80 | $3.27 | -3.20 | -4.00 | -4.00 | -7.20 |
| 2026-09-08 | `VSTM` | 9 | $8.20 | $8.20 | +0.00 | $8.08 | -1.08 | -1.08 | +1.53 | +0.45 |
| 2026-09-08 | `CRM` | 10 | $259.23 | $253.72 | -55.10 | $249.12 | -46.00 | -101.10 | -96.40 | -142.40 |
| 2026-09-08 | `DELL` | 5 | $524.14 | $521.15 | -14.95 | $533.88 | +63.65 | +48.70 | +36.85 | +100.50 |
| 2026-09-08 | `IRD` | 635 | $4.67 | $4.53 | -88.90 | $4.34 | -120.65 | -209.55 | +0.00 | -120.65 |
| 2026-09-08 | `LENZ` | 500 | $5.96 | $5.95 | -5.00 | $5.33 | -310.00 | -315.00 | +100.00 | -210.00 |
| 2026-09-09 | `ATRC` | 1 | $53.73 | $53.16 | -0.57 | $53.03 | -0.13 | -0.70 | +0.28 | +0.15 |
| 2026-09-09 | `HRMY` | 1 | $42.07 | $42.01 | -0.06 | $41.62 | -0.39 | -0.45 | -0.92 | -1.31 |
| 2026-09-09 | `CABA` | 20 | $3.27 | $3.28 | +0.20 | $2.91 | -7.40 | -7.20 | -7.00 | -14.40 |
| 2026-09-09 | `VSTM` | 9 | $8.08 | $8.01 | -0.63 | $7.94 | -0.63 | -1.26 | -0.18 | -0.81 |
| 2026-09-09 | `CRM` | 10 | $249.12 | $249.78 | +6.60 | $244.16 | -56.20 | -49.60 | -135.80 | -192.00 |
| 2026-09-09 | `DELL` | 5 | $533.88 | $538.47 | +22.95 | $535.25 | -16.10 | +6.85 | +123.45 | +107.35 |
| 2026-09-09 | `IRD` | 635 | $4.34 | $5.31 | +615.95 | $5.73 | +266.70 | +882.65 | +495.30 | +762.00 |
| 2026-09-09 | `LENZ` | 500 | $5.33 | $5.31 | -10.00 | $4.91 | -200.00 | -210.00 | -220.00 | -420.00 |
| 2026-09-10 | `ATRC` | 1 | $53.03 | $52.31 | -0.72 | $52.96 | +0.65 | -0.07 | -0.57 | +0.08 |
| 2026-09-10 | `HRMY` | 1 | $41.62 | $41.26 | -0.36 | $41.10 | -0.16 | -0.52 | -1.67 | -1.83 |
| 2026-09-10 | `CABA` | 20 | $2.91 | $2.85 | -1.20 | $2.74 | -2.20 | -3.40 | -15.60 | -17.80 |
| 2026-09-10 | `VSTM` | 9 | $7.94 | $7.91 | -0.27 | $7.62 | -2.61 | -2.88 | -1.08 | -3.69 |
| 2026-09-10 | `CRM` | 10 | $244.16 | $245.35 | +11.90 | $243.00 | -23.50 | -11.60 | -180.10 | -203.60 |
| 2026-09-10 | `DELL` | 5 | $535.25 | $523.83 | -57.10 | $506.62 | -86.05 | -143.15 | +50.25 | -35.80 |
| 2026-09-10 | `IRD` | 635 | $5.73 | $5.87 | +88.90 | $6.07 | +127.00 | +215.90 | +850.90 | +977.90 |
| 2026-09-10 | `LENZ` | 500 | $4.91 | $4.85 | -30.00 | $4.72 | -65.00 | -95.00 | -450.00 | -515.00 |
| 2026-09-11 | `ATRC` | 1 | $52.96 | $53.53 | +0.57 | — | +0.00 | +0.57 | +0.65 | — |
| 2026-09-11 | `HRMY` | 1 | $41.10 | $41.30 | +0.20 | — | +0.00 | +0.20 | -1.63 | — |
| 2026-09-11 | `CABA` | 20 | $2.74 | $2.77 | +0.60 | — | +0.00 | +0.60 | -17.20 | — |
| 2026-09-11 | `VSTM` | 9 | $7.62 | $7.70 | +0.72 | — | +0.00 | +0.72 | -2.97 | — |
| 2026-09-11 | `CRM` | 10 | $243.00 | $242.02 | -9.80 | $247.72 | +57.00 | +47.20 | -213.40 | -156.40 |
| 2026-09-11 | `DELL` | 5 | $506.62 | $518.07 | +57.25 | $567.29 | +246.10 | +303.35 | +21.45 | +267.55 |
| 2026-09-11 | `IRD` | 635 | $6.07 | $6.16 | +57.15 | $6.04 | -76.20 | -19.05 | +1035.05 | +958.85 |
| 2026-09-11 | `LENZ` | 500 | $4.72 | $4.71 | -5.00 | $4.52 | -95.00 | -100.00 | -520.00 | -615.00 |
| 2026-09-11 | `BAND` | 4 | — | $52.55 | +0.00 | $56.87 | +17.28 | +17.28 | +0.00 | +17.28 |
| 2026-09-11 | `PAYP` | 13 | — | $18.30 | +0.00 | $18.45 | +1.95 | +1.95 | +0.00 | +1.95 |
| 2026-09-11 | `SEDG` | 6 | — | $36.78 | +0.00 | $34.68 | -12.60 | -12.60 | +0.00 | -12.60 |
| 2026-09-14 | `CRM` | 10 | $247.72 | $255.75 | +80.30 | — | +0.00 | +80.30 | -76.10 | — |
| 2026-09-14 | `DELL` | 5 | $567.29 | $538.57 | -143.60 | — | +0.00 | -143.60 | +123.95 | — |
| 2026-09-14 | `IRD` | 635 | $6.04 | $6.02 | -12.70 | — | +0.00 | -12.70 | +946.15 | — |
| 2026-09-14 | `LENZ` | 500 | $4.52 | $4.53 | +5.00 | — | +0.00 | +5.00 | -610.00 | — |
| 2026-09-14 | `BAND` | 4 | $56.87 | $56.90 | +0.12 | $48.97 | -31.72 | -31.60 | +17.40 | -14.32 |
| 2026-09-14 | `PAYP` | 13 | $18.45 | $18.28 | -2.21 | $18.68 | +5.20 | +2.99 | -0.26 | +4.94 |
| 2026-09-14 | `SEDG` | 6 | $34.68 | $33.64 | -6.24 | $35.33 | +10.14 | +3.90 | -18.84 | -8.70 |
| 2026-09-15 | `BAND` | 4 | $48.97 | $49.51 | +2.16 | $50.08 | +2.28 | +4.44 | -12.16 | -9.88 |
| 2026-09-15 | `PAYP` | 13 | $18.68 | $18.30 | -4.94 | $17.82 | -6.24 | -11.18 | +0.00 | -6.24 |
| 2026-09-15 | `SEDG` | 6 | $35.33 | $35.24 | -0.54 | $35.26 | +0.12 | -0.42 | -9.24 | -9.12 |
| 2026-09-16 | `BAND` | 4 | $50.08 | $48.60 | -5.92 | $48.93 | +1.32 | -4.60 | -15.80 | -14.48 |
| 2026-09-16 | `PAYP` | 13 | $17.82 | $17.73 | -1.17 | $17.65 | -1.04 | -2.21 | -7.41 | -8.45 |
| 2026-09-16 | `SEDG` | 6 | $35.26 | $35.93 | +4.02 | $34.68 | -7.50 | -3.48 | -5.10 | -12.60 |
| 2026-09-16 | `SWKS` | 63 | — | $89.38 | +0.00 | $85.59 | -238.77 | -238.77 | +0.00 | -238.77 |
| 2026-09-16 | `QRVO` | 48 | — | $118.18 | +0.00 | $113.97 | -202.08 | -202.08 | +0.00 | -202.08 |
| 2026-09-17 | `BAND` | 4 | $48.93 | $49.85 | +3.68 | $50.51 | +2.64 | +6.32 | -10.80 | -8.16 |
| 2026-09-17 | `PAYP` | 13 | $17.65 | $17.85 | +2.60 | $17.91 | +0.78 | +3.38 | -5.85 | -5.07 |
| 2026-09-17 | `SEDG` | 6 | $34.68 | $35.30 | +3.72 | $36.00 | +4.20 | +7.92 | -8.88 | -4.68 |
| 2026-09-17 | `SWKS` | 63 | $85.59 | $86.76 | +73.71 | $91.32 | +287.28 | +360.99 | -165.06 | +122.22 |
| 2026-09-17 | `QRVO` | 48 | $113.97 | $114.90 | +44.64 | $119.51 | +221.28 | +265.92 | -157.44 | +63.84 |
| 2026-09-17 | `VOD` | 2 | — | $17.56 | +0.00 | $17.52 | -0.08 | -0.08 | +0.00 | -0.08 |
| 2026-09-17 | `ASAN` | 4 | — | $9.55 | +0.00 | $10.09 | +2.16 | +2.16 | +0.00 | +2.16 |
| 2026-09-18 | `BAND` | 4 | $50.51 | $51.19 | +2.70 | — | +0.00 | +2.70 | -5.46 | — |
| 2026-09-18 | `PAYP` | 13 | $17.91 | $17.91 | +0.00 | — | +0.00 | +0.00 | -5.07 | — |
| 2026-09-18 | `SEDG` | 6 | $36.00 | $36.54 | +3.24 | — | +0.00 | +3.24 | -1.44 | — |
| 2026-09-18 | `SWKS` | 63 | $91.32 | $92.05 | +45.99 | $88.76 | -207.27 | -161.28 | +168.21 | -39.06 |
| 2026-09-18 | `QRVO` | 48 | $119.51 | $120.76 | +60.00 | $117.18 | -171.84 | -111.84 | +123.84 | -48.00 |
| 2026-09-18 | `VOD` | 2 | $17.52 | $16.73 | -1.58 | $16.95 | +0.44 | -1.14 | -1.66 | -1.22 |
| 2026-09-18 | `ASAN` | 4 | $10.09 | $10.09 | +0.00 | $9.51 | -2.32 | -2.32 | +2.16 | -0.16 |
| 2026-09-18 | `SDGR` | 5 | — | $29.32 | +0.00 | $29.02 | -1.50 | -1.50 | +0.00 | -1.50 |
| 2026-09-18 | `ARQT` | 6 | — | $26.14 | +0.00 | $25.38 | -4.56 | -4.56 | +0.00 | -4.56 |
| 2026-09-18 | `FTRE` | 8 | — | $20.10 | +0.00 | $19.93 | -1.36 | -1.36 | +0.00 | -1.36 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +427.38 | BTSG, TPG | — | $71.00 | $10,422.85 | BTSG×83, TPG×98 |
| 2026-08-14 | +5.50 | $71.00 | BTSG×83, TPG×98 | $10,440.37 | +17.52 | -51.33 | BETR, ANGX | — | $38.63 | $10,388.71 | BTSG×83, TPG×98, BETR×1, ANGX×4 |
| 2026-08-17 | +2.25 | $38.63 | BTSG×83, TPG×98, BETR×1, ANGX×4 | $10,352.63 | -36.08 | -196.62 | ABX | — | $29.41 | $10,155.91 | BTSG×83, TPG×98, BETR×1, ANGX×4, ABX×1 |
| 2026-08-18 | -6.20 | $29.41 | BTSG×83, TPG×98, BETR×1, ANGX×4, ABX×1 | $10,124.27 | -31.64 | -16.94 | — | — | $29.41 | $10,107.33 | BTSG×83, TPG×98, BETR×1, ANGX×4, ABX×1 |
| 2026-08-19 | -7.20 | $29.41 | BTSG×83, TPG×98, BETR×1, ANGX×4, ABX×1 | $10,184.61 | +77.28 | +21.41 | — | — | $29.41 | $10,206.02 | BTSG×83, TPG×98, BETR×1, ANGX×4, ABX×1 |
| 2026-08-20 | +1.12 | $29.41 | BTSG×83, TPG×98, BETR×1, ANGX×4, ABX×1 | $10,136.77 | -69.25 | +406.41 | BHP, KGC, WPM, CYPH | BTSG, TPG | $102.18 | $10,503.90 | BETR×1, ANGX×4, ABX×1, BHP×27, KGC×85, WPM×17, CYPH×2193 |
| 2026-08-21 | +3.25 | $102.18 | BETR×1, ANGX×4, ABX×1, BHP×27, KGC×85, WPM×17, CYPH×2193 | $10,984.31 | +480.41 | +363.57 | AUPH, ARCT | BETR, ANGX | $62.80 | $11,346.84 | ABX×1, BHP×27, KGC×85, WPM×17, CYPH×2193, AUPH×2, ARCT×3 |
| 2026-08-24 | -5.17 | $62.80 | ABX×1, BHP×27, KGC×85, WPM×17, CYPH×2193, AUPH×2, ARCT×3 | $12,305.30 | +958.46 | -323.30 | — | ABX | $72.44 | $11,981.88 | BHP×27, KGC×85, WPM×17, CYPH×2193, AUPH×2, ARCT×3 |
| 2026-08-25 | +1.80 | $72.44 | BHP×27, KGC×85, WPM×17, CYPH×2193, AUPH×2, ARCT×3 | $11,565.23 | -416.65 | +479.04 | CRMD, BMEA | — | $32.48 | $12,043.83 | BHP×27, KGC×85, WPM×17, CYPH×2193, AUPH×2, ARCT×3, CRMD×2, BMEA×14 |
| 2026-08-26 | +2.02 | $32.48 | BHP×27, KGC×85, WPM×17, CYPH×2193, AUPH×2, ARCT×3, CRMD×2, BMEA×14 | $11,813.34 | -230.49 | -84.53 | — | — | $32.48 | $11,728.81 | BHP×27, KGC×85, WPM×17, CYPH×2193, AUPH×2, ARCT×3, CRMD×2, BMEA×14 |
| 2026-08-27 | — | $32.48 | BHP×27, KGC×85, WPM×17, CYPH×2193, AUPH×2, ARCT×3, CRMD×2, BMEA×14 | $11,968.10 | +239.29 | +0.10 | — | BHP, KGC, WPM, CYPH | $11,811.46 | $11,933.06 | AUPH×2, ARCT×3, CRMD×2, BMEA×14 |
| 2026-08-28 | +0.75 | $11,811.46 | AUPH×2, ARCT×3, CRMD×2, BMEA×14 | $11,930.85 | -2.21 | -442.52 | SMTC, TTMI, KEYS, AVT | AUPH, ARCT | $251.56 | $11,479.27 | CRMD×2, BMEA×14, SMTC×20, TTMI×24, KEYS×9, AVT×32 |
| 2026-08-31 | -5.85 | $251.56 | CRMD×2, BMEA×14, SMTC×20, TTMI×24, KEYS×9, AVT×32 | $11,552.97 | +73.70 | +22.13 | — | — | $251.56 | $11,575.10 | CRMD×2, BMEA×14, SMTC×20, TTMI×24, KEYS×9, AVT×32 |
| 2026-09-01 | -6.30 | $251.56 | CRMD×2, BMEA×14, SMTC×20, TTMI×24, KEYS×9, AVT×32 | $11,372.29 | -202.81 | +66.52 | — | CRMD, BMEA | $291.09 | $11,438.32 | SMTC×20, TTMI×24, KEYS×9, AVT×32 |
| 2026-09-02 | -3.83 | $291.09 | SMTC×20, TTMI×24, KEYS×9, AVT×32 | $11,415.21 | -23.11 | +101.18 | — | — | $291.09 | $11,516.39 | SMTC×20, TTMI×24, KEYS×9, AVT×32 |
| 2026-09-03 | -0.90 | $291.09 | SMTC×20, TTMI×24, KEYS×9, AVT×32 | $11,438.58 | -77.81 | +134.75 | ATRC, HRMY, CABA, VSTM | — | $47.91 | $11,570.83 | SMTC×20, TTMI×24, KEYS×9, AVT×32, ATRC×1, HRMY×1, CABA×20, VSTM×9 |
| 2026-09-04 | +2.25 | $47.91 | SMTC×20, TTMI×24, KEYS×9, AVT×32, ATRC×1, HRMY×1, CABA×20, VSTM×9 | $11,749.49 | +178.66 | +207.45 | CRM, DELL, IRD, LENZ | SMTC, TTMI, KEYS, AVT | $534.50 | $11,929.92 | ATRC×1, HRMY×1, CABA×20, VSTM×9, CRM×10, DELL×5, IRD×635, LENZ×500 |
| 2026-09-08 | -11.47 | $534.50 | ATRC×1, HRMY×1, CABA×20, VSTM×9, CRM×10, DELL×5, IRD×635, LENZ×500 | $11,767.91 | -162.01 | -417.99 | — | — | $534.50 | $11,349.92 | ATRC×1, HRMY×1, CABA×20, VSTM×9, CRM×10, DELL×5, IRD×635, LENZ×500 |
| 2026-09-09 | -13.95 | $534.50 | ATRC×1, HRMY×1, CABA×20, VSTM×9, CRM×10, DELL×5, IRD×635, LENZ×500 | $11,984.36 | +634.44 | -14.15 | — | — | $534.50 | $11,970.21 | ATRC×1, HRMY×1, CABA×20, VSTM×9, CRM×10, DELL×5, IRD×635, LENZ×500 |
| 2026-09-10 | -13.28 | $534.50 | ATRC×1, HRMY×1, CABA×20, VSTM×9, CRM×10, DELL×5, IRD×635, LENZ×500 | $11,981.36 | +11.16 | -51.87 | — | — | $534.50 | $11,929.49 | ATRC×1, HRMY×1, CABA×20, VSTM×9, CRM×10, DELL×5, IRD×635, LENZ×500 |
| 2026-09-11 | +0.50 | $534.50 | ATRC×1, HRMY×1, CABA×20, VSTM×9, CRM×10, DELL×5, IRD×635, LENZ×500 | $12,031.18 | +101.69 | +138.53 | BAND, PAYP, SEDG | ATRC, HRMY, CABA, VSTM | $76.85 | $12,161.31 | CRM×10, DELL×5, IRD×635, LENZ×500, BAND×4, PAYP×13, SEDG×6 |
| 2026-09-14 | -11.00 | $76.85 | CRM×10, DELL×5, IRD×635, LENZ×500, BAND×4, PAYP×13, SEDG×6 | $12,081.98 | -79.33 | -16.38 | — | CRM, DELL, IRD, LENZ | $11,395.93 | $12,046.63 | BAND×4, PAYP×13, SEDG×6 |
| 2026-09-15 | -3.84 | $11,395.93 | BAND×4, PAYP×13, SEDG×6 | $12,043.31 | -3.32 | -3.84 | — | — | $11,395.93 | $12,039.47 | BAND×4, PAYP×13, SEDG×6 |
| 2026-09-16 | +5.30 | $11,395.93 | BAND×4, PAYP×13, SEDG×6 | $12,036.40 | -3.07 | -448.07 | SWKS, QRVO | — | $88.04 | $11,584.02 | BAND×4, PAYP×13, SEDG×6, SWKS×63, QRVO×48 |
| 2026-09-17 | +7.38 | $88.04 | BAND×4, PAYP×13, SEDG×6, SWKS×63, QRVO×48 | $11,712.37 | +128.35 | +518.26 | VOD, ASAN | — | $13.97 | $12,229.88 | BAND×4, PAYP×13, SEDG×6, SWKS×63, QRVO×48, VOD×2, ASAN×4 |
| 2026-09-18 | +4.86 | $13.97 | BAND×4, PAYP×13, SEDG×6, SWKS×63, QRVO×48, VOD×2, ASAN×4 | $12,340.23 | +110.35 | -388.41 | SDGR, ARQT, FTRE | BAND, PAYP, SEDG | $195.74 | $11,941.02 | SWKS×63, QRVO×48, VOD×2, ASAN×4, SDGR×5, ARQT×6, FTRE×8 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 83 | $59.80 | $2.24 | — | $5,034.36 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=-5.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $71.00 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.00 | ▲ close $10,422.85 vs 09:30 $10,000.00 (session +427.38) | 16:00 close · cash $71.00 · equity $10,422.85 vs 09:30 $10,000.00 (+422.85; session marks +427.38) · 2 name(s) marked open→close (per-name table). BTSG×83 09:30 $59.80 → close $60.23 +35.69; TPG×98 09:30 $50.62 → close $54.62 +391.69 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.00 | ▲ 09:30 equity $10,440.37 vs yday $10,422.85 (+17.52) | 09:30 open · cash $71.00 (unchanged overnight, no fees) · equity $10,440.37 vs prior close $10,422.85 (+17.52) · 2 name(s) re-marked at the open (per-name table). BTSG×83 yday $60.23 → 09:30 $59.65 -48.14; TPG×98 yday $54.62 → 09:30 $55.29 +65.66 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 1 | $14.80 | $0.15 | — | $56.05 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=-9.9; leftover $17.75 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 4 | $4.31 | $0.18 | — | $38.63 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $17.75 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.63 | ▼ close $10,388.71 vs 09:30 $10,440.37 (session -51.33) | 16:00 close · cash $38.63 · equity $10,388.71 vs 09:30 $10,440.37 (-51.66; session marks -51.33) · 4 name(s) marked open→close (per-name table). BTSG×83 09:30 $59.65 → close $61.71 +170.98; TPG×98 09:30 $55.29 → close $53.03 -221.48; BETR×1 09:30 $14.80 → close $13.73 -1.07; ANGX×4 09:30 $4.31 → close $4.37 +0.24 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.63 | ▼ 09:30 equity $10,352.63 vs yday $10,388.71 (-36.08) | 09:30 open · cash $38.63 (unchanged overnight, no fees) · equity $10,352.63 vs prior close $10,388.71 (-36.08) · 4 name(s) re-marked at the open (per-name table). BTSG×83 yday $61.71 → 09:30 $61.69 -1.66; TPG×98 yday $53.03 → 09:30 $52.67 -35.28; BETR×1 yday $13.73 → 09:30 $13.67 -0.06; ANGX×4 yday $4.37 → 09:30 $4.60 +0.92 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 1 | $9.12 | $0.09 | — | $29.41 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $9.66 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.41 | ▼ close $10,155.91 vs 09:30 $10,352.63 (session -196.62) | 16:00 close · cash $29.41 · equity $10,155.91 vs 09:30 $10,352.63 (-196.72; session marks -196.62) · 5 name(s) marked open→close (per-name table). BTSG×83 09:30 $61.69 → close $60.38 -108.73; TPG×98 09:30 $52.67 → close $51.77 -88.20; BETR×1 09:30 $13.67 → close $13.54 -0.13; ANGX×4 09:30 $4.60 → close $4.71 +0.44; ABX×1 09:30 $9.12 → close $9.12 +0.00 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.41 | ▼ 09:30 equity $10,124.27 vs yday $10,155.91 (-31.64) | 09:30 open · cash $29.41 (unchanged overnight, no fees) · equity $10,124.27 vs prior close $10,155.91 (-31.64) · 5 name(s) re-marked at the open (per-name table). BTSG×83 yday $60.38 → 09:30 $60.00 -31.54; TPG×98 yday $51.77 → 09:30 $51.77 +0.00; BETR×1 yday $13.54 → 09:30 $13.21 -0.33; ANGX×4 yday $4.71 → 09:30 $4.79 +0.32; ABX×1 yday $9.12 → 09:30 $9.03 -0.09 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.41 | ▼ close $10,107.33 vs 09:30 $10,124.27 (session -16.94) | 16:00 close · cash $29.41 · equity $10,107.33 vs 09:30 $10,124.27 (-16.94; session marks -16.94) · 5 name(s) marked open→close (per-name table). BTSG×83 09:30 $60.00 → close $59.50 -41.50; TPG×98 09:30 $51.77 → close $52.02 +24.50; BETR×1 09:30 $13.21 → close $13.05 -0.16; ANGX×4 09:30 $4.79 → close $4.85 +0.24; ABX×1 09:30 $9.03 → close $9.01 -0.02 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.41 | ▲ 09:30 equity $10,184.61 vs yday $10,107.33 (+77.28) | 09:30 open · cash $29.41 (unchanged overnight, no fees) · equity $10,184.61 vs prior close $10,107.33 (+77.28) · 5 name(s) re-marked at the open (per-name table). BTSG×83 yday $59.50 → 09:30 $60.15 +53.95; TPG×98 yday $52.02 → 09:30 $52.26 +23.52; BETR×1 yday $13.05 → 09:30 $13.03 -0.02; ANGX×4 yday $4.85 → 09:30 $4.79 -0.24; ABX×1 yday $9.01 → 09:30 $9.08 +0.07 | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.41 | ▲ close $10,206.02 vs 09:30 $10,184.61 (session +21.41) | 16:00 close · cash $29.41 · equity $10,206.02 vs 09:30 $10,184.61 (+21.41; session marks +21.41) · 5 name(s) marked open→close (per-name table). BTSG×83 09:30 $60.15 → close $59.33 -68.06; TPG×98 09:30 $52.26 → close $53.18 +90.16; BETR×1 09:30 $13.03 → close $13.03 +0.00; ANGX×4 09:30 $4.79 → close $4.60 -0.76; ABX×1 09:30 $9.08 → close $9.15 +0.07 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.41 | ▼ 09:30 equity $10,136.77 vs yday $10,206.02 (-69.25) | 09:30 open · cash $29.41 (unchanged overnight, no fees) · equity $10,136.77 vs prior close $10,206.02 (-69.25) · 5 name(s) re-marked at the open (per-name table). BTSG×83 yday $59.33 → 09:30 $58.64 -57.27; TPG×98 yday $53.18 → 09:30 $53.06 -11.76; BETR×1 yday $13.03 → 09:30 $12.95 -0.08; ANGX×4 yday $4.60 → 09:30 $4.57 -0.12; ABX×1 yday $9.15 → 09:30 $9.13 -0.02 | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 83 | $58.64 | $2.29 | $-100.81 | $4,894.24 | ▼ -100.81 after sell → book $10,134.48; vs 09:30 mark -2.29 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 98 | $53.06 | $2.34 | $+234.18 | $10,091.78 | ▲ +234.18 after sell → book $10,132.14; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 27 | $91.01 | $2.07 | — | $7,632.44 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2522.95 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 85 | $29.63 | $2.25 | — | $5,111.64 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $2522.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 17 | $144.54 | $2.04 | — | $2,652.42 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy,oppset; 🔵; ⚪; ret5=+9.2; leftover $2522.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2193 | $1.15 | $28.29 | — | $102.18 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2522.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.18 | ▲ close $10,503.90 vs 09:30 $10,136.77 (session +406.41) | 16:00 close · cash $102.18 · equity $10,503.90 vs 09:30 $10,136.77 (+367.13; session marks +406.41) · 7 name(s) marked open→close (per-name table). BETR×1 09:30 $12.95 → close $11.60 -1.35; ANGX×4 09:30 $4.57 → close $4.37 -0.80; ABX×1 09:30 $9.13 → close $9.16 +0.03; BHP×27 09:30 $91.01 → close $93.63 +70.74; KGC×85 09:30 $29.63 → close $31.43 +153.00; WPM×17 09:30 $144.54 → close $150.25 +97.07; CYPH×2193 09:30 $1.15 → close $1.19 +87.72 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.18 | ▲ 09:30 equity $10,984.31 vs yday $10,503.90 (+480.41) | 09:30 open · cash $102.18 (unchanged overnight, no fees) · equity $10,984.31 vs prior close $10,503.90 (+480.41) · 7 name(s) re-marked at the open (per-name table). BETR×1 yday $11.60 → 09:30 $11.73 +0.13; ANGX×4 yday $4.37 → 09:30 $4.43 +0.24; ABX×1 yday $9.16 → 09:30 $9.13 -0.03; BHP×27 yday $93.63 → 09:30 $95.72 +56.43; KGC×85 yday $31.43 → 09:30 $32.17 +62.90; WPM×17 yday $150.25 → 09:30 $154.70 +75.65; CYPH×2193 yday $1.19 → 09:30 $1.32 +285.09 | — |
| 2026-08-21 09:30 ET | **SELL** | `BETR` | 1 | $11.73 | $0.14 | $-3.36 | $113.77 | ▼ -3.36 after sell → book $10,984.17; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 4 | $4.43 | $0.21 | $+0.09 | $131.28 | ▲ +0.09 after sell → book $10,983.96; vs 09:30 mark -0.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 2 | $17.20 | $0.35 | — | $96.53 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $43.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 3 | $11.13 | $0.34 | — | $62.80 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,yday_gainer,mover_buy,oppset; 🔵; ⚪; ret5=+39.8; leftover $43.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.80 | ▲ close $11,346.84 vs 09:30 $10,984.31 (session +363.57) | 16:00 close · cash $62.80 · equity $11,346.84 vs 09:30 $10,984.31 (+362.53; session marks +363.57) · 7 name(s) marked open→close (per-name table). ABX×1 09:30 $9.13 → close $9.66 +0.53; BHP×27 09:30 $95.72 → close $97.03 +35.37; KGC×85 09:30 $32.17 → close $32.76 +50.15; WPM×17 09:30 $154.70 → close $157.78 +52.36; CYPH×2193 09:30 $1.32 → close $1.42 +219.30; AUPH×2 09:30 $17.20 → close $16.65 -1.10; ARCT×3 09:30 $11.13 → close $13.45 +6.96 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.80 | ▲ 09:30 equity $12,305.30 vs yday $11,346.84 (+958.46) | 09:30 open · cash $62.80 (unchanged overnight, no fees) · equity $12,305.30 vs prior close $11,346.84 (+958.46) · 7 name(s) re-marked at the open (per-name table). ABX×1 yday $9.66 → 09:30 $9.76 +0.10; BHP×27 yday $97.03 → 09:30 $97.31 +7.56; KGC×85 yday $32.76 → 09:30 $33.03 +22.95; WPM×17 yday $157.78 → 09:30 $159.50 +29.24; CYPH×2193 yday $1.42 → 09:30 $1.83 +899.13; AUPH×2 yday $16.65 → 09:30 $16.57 -0.16; ARCT×3 yday $13.45 → 09:30 $13.33 -0.36 | — |
| 2026-08-24 09:30 ET | **SELL** | `ABX` | 1 | $9.76 | $0.12 | $+0.43 | $72.44 | ▲ +0.43 after sell → book $12,305.18; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.44 | ▼ close $11,981.88 vs 09:30 $12,305.30 (session -323.30) | 16:00 close · cash $72.44 · equity $11,981.88 vs 09:30 $12,305.30 (-323.42; session marks -323.30) · 6 name(s) marked open→close (per-name table). BHP×27 09:30 $97.31 → close $97.13 -4.86; KGC×85 09:30 $33.03 → close $32.98 -4.25; WPM×17 09:30 $159.50 → close $160.19 +11.73; CYPH×2193 09:30 $1.83 → close $1.68 -328.95; AUPH×2 09:30 $16.57 → close $16.57 +0.00; ARCT×3 09:30 $13.33 → close $14.34 +3.03 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.44 | ▼ 09:30 equity $11,565.23 vs yday $11,981.88 (-416.65) | 09:30 open · cash $72.44 (unchanged overnight, no fees) · equity $11,565.23 vs prior close $11,981.88 (-416.65) · 6 name(s) re-marked at the open (per-name table). BHP×27 yday $97.13 → 09:30 $95.86 -34.29; KGC×85 yday $32.98 → 09:30 $32.32 -56.10; WPM×17 yday $160.19 → 09:30 $156.51 -62.56; CYPH×2193 yday $1.68 → 09:30 $1.56 -263.16; AUPH×2 yday $16.57 → 09:30 $16.63 +0.12; ARCT×3 yday $14.34 → 09:30 $14.12 -0.66 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 2 | $8.35 | $0.17 | — | $55.57 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+8.0; leftover $24.15 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 14 | $1.63 | $0.27 | — | $32.48 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $24.15 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.48 | ▲ close $12,043.83 vs 09:30 $11,565.23 (session +479.04) | 16:00 close · cash $32.48 · equity $12,043.83 vs 09:30 $11,565.23 (+478.60; session marks +479.04) · 8 name(s) marked open→close (per-name table). BHP×27 09:30 $95.86 → close $98.69 +76.41; KGC×85 09:30 $32.32 → close $33.48 +98.60; WPM×17 09:30 $156.51 → close $163.72 +122.57; CYPH×2193 09:30 $1.56 → close $1.64 +175.44; AUPH×2 09:30 $16.63 → close $16.75 +0.24; ARCT×3 09:30 $14.12 → close $15.44 +3.96; CRMD×2 09:30 $8.35 → close $8.56 +0.42; BMEA×14 09:30 $1.63 → close $1.73 +1.40 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.48 | ▼ 09:30 equity $11,813.34 vs yday $12,043.83 (-230.49) | 09:30 open · cash $32.48 (unchanged overnight, no fees) · equity $11,813.34 vs prior close $12,043.83 (-230.49) · 8 name(s) re-marked at the open (per-name table). BHP×27 yday $98.69 → 09:30 $96.99 -45.90; KGC×85 yday $33.48 → 09:30 $32.90 -49.30; WPM×17 yday $163.72 → 09:30 $160.93 -47.43; CYPH×2193 yday $1.64 → 09:30 $1.60 -87.72; AUPH×2 yday $16.75 → 09:30 $16.60 -0.30; ARCT×3 yday $15.44 → 09:30 $15.35 -0.27; CRMD×2 yday $8.56 → 09:30 $8.60 +0.08; BMEA×14 yday $1.73 → 09:30 $1.75 +0.35 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.48 | ▼ close $11,728.81 vs 09:30 $11,813.34 (session -84.53) | 16:00 close · cash $32.48 · equity $11,728.81 vs 09:30 $11,813.34 (-84.53; session marks -84.53) · 8 name(s) marked open→close (per-name table). BHP×27 09:30 $96.99 → close $96.33 -17.82; KGC×85 09:30 $32.90 → close $32.32 -49.30; WPM×17 09:30 $160.93 → close $156.02 -83.47; CYPH×2193 09:30 $1.60 → close $1.63 +65.79; AUPH×2 09:30 $16.60 → close $16.54 -0.12; ARCT×3 09:30 $15.35 → close $15.83 +1.44; CRMD×2 09:30 $8.60 → close $8.39 -0.42; BMEA×14 09:30 $1.75 → close $1.71 -0.63 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.48 | ▲ 09:30 equity $11,968.10 vs yday $11,728.81 (+239.29) | 09:30 open · cash $32.48 (unchanged overnight, no fees) · equity $11,968.10 vs prior close $11,728.81 (+239.29) · 8 name(s) re-marked at the open (per-name table). BHP×27 yday $96.33 → 09:30 $95.52 -21.87; KGC×85 yday $32.32 → 09:30 $32.32 +0.00; WPM×17 yday $156.02 → 09:30 $155.89 -2.21; CYPH×2193 yday $1.63 → 09:30 $1.75 +263.16; AUPH×2 yday $16.54 → 09:30 $16.47 -0.14; ARCT×3 yday $15.83 → 09:30 $15.74 -0.27; CRMD×2 yday $8.39 → 09:30 $8.49 +0.20; BMEA×14 yday $1.71 → 09:30 $1.74 +0.42 | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 27 | $95.52 | $2.10 | $+117.60 | $2,609.42 | ▲ +117.60 after sell → book $11,966.00; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 85 | $32.32 | $2.28 | $+224.12 | $5,354.33 | ▲ +224.12 after sell → book $11,963.71; vs 09:30 mark -2.29 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 17 | $155.89 | $2.07 | $+188.84 | $8,002.39 | ▲ +188.84 after sell → book $11,961.64; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 2193 | $1.75 | $28.68 | $+1258.83 | $11,811.46 | ▲ +1,258.83 after sell → book $11,932.96; vs 09:30 mark -28.68 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,811.46 | ▲ close $11,933.06 vs 09:30 $11,968.10 (session +0.10) | 16:00 close · cash $11,811.46 · equity $11,933.06 vs 09:30 $11,968.10 (-35.04; session marks +0.10) · 4 name(s) marked open→close (per-name table). AUPH×2 09:30 $16.47 → close $16.48 +0.01; ARCT×3 09:30 $15.74 → close $16.17 +1.29; CRMD×2 09:30 $8.49 → close $8.31 -0.36; BMEA×14 09:30 $1.74 → close $1.68 -0.84 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,811.46 | ▼ 09:30 equity $11,930.85 vs yday $11,933.06 (-2.21) | 09:30 open · cash $11,811.46 (unchanged overnight, no fees) · equity $11,930.85 vs prior close $11,933.06 (-2.21) · 4 name(s) re-marked at the open (per-name table). AUPH×2 yday $16.48 → 09:30 $16.44 -0.07; ARCT×3 yday $16.17 → 09:30 $15.43 -2.22; CRMD×2 yday $8.31 → 09:30 $8.28 -0.06; BMEA×14 yday $1.68 → 09:30 $1.69 +0.14 | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 2 | $16.44 | $0.35 | $-2.22 | $11,843.98 | ▼ -2.22 after sell → book $11,930.49; vs 09:30 mark -0.36 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 3 | $15.43 | $0.49 | $+12.07 | $11,889.78 | ▲ +12.07 after sell → book $11,930.00; vs 09:30 mark -0.49 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 20 | $141.76 | $2.05 | — | $9,052.53 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $2972.45 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 24 | $122.81 | $2.06 | — | $6,103.03 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $2972.45 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 9 | $324.41 | $2.02 | — | $3,181.32 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $2972.45 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 32 | $91.49 | $2.09 | — | $251.56 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $2972.45 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.56 | ▼ close $11,479.27 vs 09:30 $11,930.85 (session -442.52) | 16:00 close · cash $251.56 · equity $11,479.27 vs 09:30 $11,930.85 (-451.58; session marks -442.52) · 6 name(s) marked open→close (per-name table). CRMD×2 09:30 $8.28 → close $8.30 +0.04; BMEA×14 09:30 $1.69 → close $1.73 +0.56; SMTC×20 09:30 $141.76 → close $131.17 -211.80; TTMI×24 09:30 $122.81 → close $118.65 -99.84; KEYS×9 09:30 $324.41 → close $319.97 -39.96; AVT×32 09:30 $91.49 → close $88.63 -91.52 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.56 | ▲ 09:30 equity $11,552.97 vs yday $11,479.27 (+73.70) | 09:30 open · cash $251.56 (unchanged overnight, no fees) · equity $11,552.97 vs prior close $11,479.27 (+73.70) · 6 name(s) re-marked at the open (per-name table). CRMD×2 yday $8.30 → 09:30 $8.26 -0.08; BMEA×14 yday $1.73 → 09:30 $1.72 -0.14; SMTC×20 yday $131.17 → 09:30 $132.30 +22.60; TTMI×24 yday $118.65 → 09:30 $118.83 +4.32; KEYS×9 yday $319.97 → 09:30 $322.49 +22.68; AVT×32 yday $88.63 → 09:30 $89.39 +24.32 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.56 | ▲ close $11,575.10 vs 09:30 $11,552.97 (session +22.13) | 16:00 close · cash $251.56 · equity $11,575.10 vs 09:30 $11,552.97 (+22.13; session marks +22.13) · 6 name(s) marked open→close (per-name table). CRMD×2 09:30 $8.26 → close $8.26 +0.00; BMEA×14 09:30 $1.72 → close $1.68 -0.56; SMTC×20 09:30 $132.30 → close $132.96 +13.20; TTMI×24 09:30 $118.83 → close $118.92 +2.16; KEYS×9 09:30 $322.49 → close $322.70 +1.89; AVT×32 09:30 $89.39 → close $89.56 +5.44 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.56 | ▼ 09:30 equity $11,372.29 vs yday $11,575.10 (-202.81) | 09:30 open · cash $251.56 (unchanged overnight, no fees) · equity $11,372.29 vs prior close $11,575.10 (-202.81) · 6 name(s) re-marked at the open (per-name table). CRMD×2 yday $8.26 → 09:30 $8.25 -0.02; BMEA×14 yday $1.68 → 09:30 $1.68 +0.00; SMTC×20 yday $132.96 → 09:30 $127.63 -106.60; TTMI×24 yday $118.92 → 09:30 $116.68 -53.76; KEYS×9 yday $322.70 → 09:30 $321.47 -11.07; AVT×32 yday $89.56 → 09:30 $88.58 -31.36 | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 2 | $8.25 | $0.19 | $-0.56 | $267.87 | ▼ -0.56 after sell → book $11,372.10; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 14 | $1.68 | $0.30 | $+0.13 | $291.09 | ▲ +0.13 after sell → book $11,371.80; vs 09:30 mark -0.30 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $291.09 | ▲ close $11,438.32 vs 09:30 $11,372.29 (session +66.52) | 16:00 close · cash $291.09 · equity $11,438.32 vs 09:30 $11,372.29 (+66.03; session marks +66.52) · 4 name(s) marked open→close (per-name table). SMTC×20 09:30 $127.63 → close $132.27 +92.80; TTMI×24 09:30 $116.68 → close $115.33 -32.40; KEYS×9 09:30 $321.47 → close $319.27 -19.80; AVT×32 09:30 $88.58 → close $89.39 +25.92 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $291.09 | ▼ 09:30 equity $11,415.21 vs yday $11,438.32 (-23.11) | 09:30 open · cash $291.09 (unchanged overnight, no fees) · equity $11,415.21 vs prior close $11,438.32 (-23.11) · 4 name(s) re-marked at the open (per-name table). SMTC×20 yday $132.27 → 09:30 $133.00 +14.60; TTMI×24 yday $115.33 → 09:30 $114.22 -26.64; KEYS×9 yday $319.27 → 09:30 $318.04 -11.07; AVT×32 yday $89.39 → 09:30 $89.39 +0.00 | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $291.09 | ▲ close $11,516.39 vs 09:30 $11,415.21 (session +101.18) | 16:00 close · cash $291.09 · equity $11,516.39 vs 09:30 $11,415.21 (+101.18; session marks +101.18) · 4 name(s) marked open→close (per-name table). SMTC×20 09:30 $133.00 → close $133.85 +17.00; TTMI×24 09:30 $114.22 → close $115.60 +33.12; KEYS×9 09:30 $318.04 → close $321.58 +31.86; AVT×32 09:30 $89.39 → close $89.99 +19.20 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $291.09 | ▼ 09:30 equity $11,438.58 vs yday $11,516.39 (-77.81) | 09:30 open · cash $291.09 (unchanged overnight, no fees) · equity $11,438.58 vs prior close $11,516.39 (-77.81) · 4 name(s) re-marked at the open (per-name table). SMTC×20 yday $133.85 → 09:30 $133.10 -15.00; TTMI×24 yday $115.60 → 09:30 $115.21 -9.36; KEYS×9 yday $321.58 → 09:30 $319.09 -22.41; AVT×32 yday $89.99 → 09:30 $89.02 -31.04 | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 1 | $52.88 | $0.53 | — | $237.68 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $72.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 1 | $42.93 | $0.43 | — | $194.31 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $72.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 20 | $3.63 | $0.79 | — | $120.93 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $72.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 9 | $8.03 | $0.75 | — | $47.91 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $72.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.91 | ▲ close $11,570.83 vs 09:30 $11,438.58 (session +134.75) | 16:00 close · cash $47.91 · equity $11,570.83 vs 09:30 $11,438.58 (+132.25; session marks +134.75) · 8 name(s) marked open→close (per-name table). SMTC×20 09:30 $133.10 → close $135.40 +46.00; TTMI×24 09:30 $115.21 → close $115.39 +4.32; KEYS×9 09:30 $319.09 → close $322.62 +31.77; AVT×32 09:30 $89.02 → close $90.82 +57.60; ATRC×1 09:30 $52.88 → close $52.46 -0.42; HRMY×1 09:30 $42.93 → close $41.86 -1.07; CABA×20 09:30 $3.63 → close $3.48 -3.00; VSTM×9 09:30 $8.03 → close $7.98 -0.45 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.91 | ▲ 09:30 equity $11,749.49 vs yday $11,570.83 (+178.66) | 09:30 open · cash $47.91 (unchanged overnight, no fees) · equity $11,749.49 vs prior close $11,570.83 (+178.66) · 8 name(s) re-marked at the open (per-name table). SMTC×20 yday $135.40 → 09:30 $138.71 +66.20; TTMI×24 yday $115.39 → 09:30 $118.58 +76.56; KEYS×9 yday $322.62 → 09:30 $326.10 +31.32; AVT×32 yday $90.82 → 09:30 $91.02 +6.40; ATRC×1 yday $52.46 → 09:30 $52.03 -0.43; HRMY×1 yday $41.86 → 09:30 $41.50 -0.36; CABA×20 yday $3.48 → 09:30 $3.46 -0.40; VSTM×9 yday $7.98 → 09:30 $7.91 -0.63 | — |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 20 | $138.71 | $2.08 | $-65.13 | $2,820.03 | ▼ -65.13 after sell → book $11,747.41; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `TTMI` | 24 | $118.58 | $2.09 | $-105.68 | $5,663.85 | ▼ -105.68 after sell → book $11,745.31; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `KEYS` | 9 | $326.10 | $2.05 | $+11.14 | $8,596.70 | ▲ +11.14 after sell → book $11,743.26; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVT` | 32 | $91.02 | $2.12 | $-19.25 | $11,507.22 | ▼ -19.25 after sell → book $11,741.14; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 10 | $263.36 | $2.02 | — | $8,871.60 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2876.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 5 | $513.78 | $2.00 | — | $6,300.70 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy,oppset; 🔵; ⚪; ret5=+9.3; leftover $2876.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 635 | $4.53 | $8.19 | — | $3,415.95 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $2876.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 500 | $5.75 | $6.45 | — | $534.50 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $2876.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $534.50 | ▲ close $11,929.92 vs 09:30 $11,749.49 (session +207.45) | 16:00 close · cash $534.50 · equity $11,929.92 vs 09:30 $11,749.49 (+180.43; session marks +207.45) · 8 name(s) marked open→close (per-name table). ATRC×1 09:30 $52.03 → close $51.52 -0.51; HRMY×1 09:30 $41.50 → close $42.25 +0.75; CABA×20 09:30 $3.46 → close $3.47 +0.20; VSTM×9 09:30 $7.91 → close $8.20 +2.61; CRM×10 09:30 $263.36 → close $259.23 -41.30; DELL×5 09:30 $513.78 → close $524.14 +51.80; IRD×635 09:30 $4.53 → close $4.67 +88.90; LENZ×500 09:30 $5.75 → close $5.96 +105.00 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $534.50 | ▼ 09:30 equity $11,767.91 vs yday $11,929.92 (-162.01) | 09:30 open · cash $534.50 (unchanged overnight, no fees) · equity $11,767.91 vs prior close $11,929.92 (-162.01) · 8 name(s) re-marked at the open (per-name table). ATRC×1 yday $51.52 → 09:30 $54.31 +2.79; HRMY×1 yday $42.25 → 09:30 $42.20 -0.05; CABA×20 yday $3.47 → 09:30 $3.43 -0.80; VSTM×9 yday $8.20 → 09:30 $8.20 +0.00; CRM×10 yday $259.23 → 09:30 $253.72 -55.10; DELL×5 yday $524.14 → 09:30 $521.15 -14.95; IRD×635 yday $4.67 → 09:30 $4.53 -88.90; LENZ×500 yday $5.96 → 09:30 $5.95 -5.00 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $534.50 | ▼ close $11,349.92 vs 09:30 $11,767.91 (session -417.99) | 16:00 close · cash $534.50 · equity $11,349.92 vs 09:30 $11,767.91 (-417.99; session marks -417.99) · 8 name(s) marked open→close (per-name table). ATRC×1 09:30 $54.31 → close $53.73 -0.58; HRMY×1 09:30 $42.20 → close $42.07 -0.13; CABA×20 09:30 $3.43 → close $3.27 -3.20; VSTM×9 09:30 $8.20 → close $8.08 -1.08; CRM×10 09:30 $253.72 → close $249.12 -46.00; DELL×5 09:30 $521.15 → close $533.88 +63.65; IRD×635 09:30 $4.53 → close $4.34 -120.65; LENZ×500 09:30 $5.95 → close $5.33 -310.00 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $534.50 | ▲ 09:30 equity $11,984.36 vs yday $11,349.92 (+634.44) | 09:30 open · cash $534.50 (unchanged overnight, no fees) · equity $11,984.36 vs prior close $11,349.92 (+634.44) · 8 name(s) re-marked at the open (per-name table). ATRC×1 yday $53.73 → 09:30 $53.16 -0.57; HRMY×1 yday $42.07 → 09:30 $42.01 -0.06; CABA×20 yday $3.27 → 09:30 $3.28 +0.20; VSTM×9 yday $8.08 → 09:30 $8.01 -0.63; CRM×10 yday $249.12 → 09:30 $249.78 +6.60; DELL×5 yday $533.88 → 09:30 $538.47 +22.95; IRD×635 yday $4.34 → 09:30 $5.31 +615.95; LENZ×500 yday $5.33 → 09:30 $5.31 -10.00 | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $534.50 | ▼ close $11,970.21 vs 09:30 $11,984.36 (session -14.15) | 16:00 close · cash $534.50 · equity $11,970.21 vs 09:30 $11,984.36 (-14.15; session marks -14.15) · 8 name(s) marked open→close (per-name table). ATRC×1 09:30 $53.16 → close $53.03 -0.13; HRMY×1 09:30 $42.01 → close $41.62 -0.39; CABA×20 09:30 $3.28 → close $2.91 -7.40; VSTM×9 09:30 $8.01 → close $7.94 -0.63; CRM×10 09:30 $249.78 → close $244.16 -56.20; DELL×5 09:30 $538.47 → close $535.25 -16.10; IRD×635 09:30 $5.31 → close $5.73 +266.70; LENZ×500 09:30 $5.31 → close $4.91 -200.00 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $534.50 | ▲ 09:30 equity $11,981.36 vs yday $11,970.21 (+11.16) | 09:30 open · cash $534.50 (unchanged overnight, no fees) · equity $11,981.36 vs prior close $11,970.21 (+11.16) · 8 name(s) re-marked at the open (per-name table). ATRC×1 yday $53.03 → 09:30 $52.31 -0.72; HRMY×1 yday $41.62 → 09:30 $41.26 -0.36; CABA×20 yday $2.91 → 09:30 $2.85 -1.20; VSTM×9 yday $7.94 → 09:30 $7.91 -0.27; CRM×10 yday $244.16 → 09:30 $245.35 +11.90; DELL×5 yday $535.25 → 09:30 $523.83 -57.10; IRD×635 yday $5.73 → 09:30 $5.87 +88.90; LENZ×500 yday $4.91 → 09:30 $4.85 -30.00 | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $534.50 | ▼ close $11,929.49 vs 09:30 $11,981.36 (session -51.87) | 16:00 close · cash $534.50 · equity $11,929.49 vs 09:30 $11,981.36 (-51.87; session marks -51.87) · 8 name(s) marked open→close (per-name table). ATRC×1 09:30 $52.31 → close $52.96 +0.65; HRMY×1 09:30 $41.26 → close $41.10 -0.16; CABA×20 09:30 $2.85 → close $2.74 -2.20; VSTM×9 09:30 $7.91 → close $7.62 -2.61; CRM×10 09:30 $245.35 → close $243.00 -23.50; DELL×5 09:30 $523.83 → close $506.62 -86.05; IRD×635 09:30 $5.87 → close $6.07 +127.00; LENZ×500 09:30 $4.85 → close $4.72 -65.00 | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $534.50 | ▲ 09:30 equity $12,031.18 vs yday $11,929.49 (+101.69) | 09:30 open · cash $534.50 (unchanged overnight, no fees) · equity $12,031.18 vs prior close $11,929.49 (+101.69) · 8 name(s) re-marked at the open (per-name table). ATRC×1 yday $52.96 → 09:30 $53.53 +0.57; HRMY×1 yday $41.10 → 09:30 $41.30 +0.20; CABA×20 yday $2.74 → 09:30 $2.77 +0.60; VSTM×9 yday $7.62 → 09:30 $7.70 +0.72; CRM×10 yday $243.00 → 09:30 $242.02 -9.80; DELL×5 yday $506.62 → 09:30 $518.07 +57.25; IRD×635 yday $6.07 → 09:30 $6.16 +57.15; LENZ×500 yday $4.72 → 09:30 $4.71 -5.00 | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 1 | $53.53 | $0.56 | $-0.44 | $587.48 | ▼ -0.44 after sell → book $12,030.63; vs 09:30 mark -0.56 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 1 | $41.30 | $0.44 | $-2.50 | $628.34 | ▼ -2.50 after sell → book $12,030.19; vs 09:30 mark -0.44 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 20 | $2.77 | $0.63 | $-18.62 | $683.11 | ▼ -18.62 after sell → book $12,029.56; vs 09:30 mark -0.63 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 9 | $7.70 | $0.74 | $-4.46 | $751.67 | ▼ -4.46 after sell → book $12,028.82; vs 09:30 mark -0.74 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 4 | $52.55 | $2.00 | — | $539.46 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $250.56 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 13 | $18.30 | $2.03 | — | $299.54 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $250.56 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SEDG` | 6 | $36.78 | $2.01 | — | $76.85 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list oppset; 🔵; ⚪; ret5=+8.2; leftover $250.56 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.85 | ▲ close $12,161.31 vs 09:30 $12,031.18 (session +138.53) | 16:00 close · cash $76.85 · equity $12,161.31 vs 09:30 $12,031.18 (+130.13; session marks +138.53) · 7 name(s) marked open→close (per-name table). CRM×10 09:30 $242.02 → close $247.72 +57.00; DELL×5 09:30 $518.07 → close $567.29 +246.10; IRD×635 09:30 $6.16 → close $6.04 -76.20; LENZ×500 09:30 $4.71 → close $4.52 -95.00; BAND×4 09:30 $52.55 → close $56.87 +17.28; PAYP×13 09:30 $18.30 → close $18.45 +1.95; SEDG×6 09:30 $36.78 → close $34.68 -12.60 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.85 | ▼ 09:30 equity $12,081.98 vs yday $12,161.31 (-79.33) | 09:30 open · cash $76.85 (unchanged overnight, no fees) · equity $12,081.98 vs prior close $12,161.31 (-79.33) · 7 name(s) re-marked at the open (per-name table). CRM×10 yday $247.72 → 09:30 $255.75 +80.30; DELL×5 yday $567.29 → 09:30 $538.57 -143.60; IRD×635 yday $6.04 → 09:30 $6.02 -12.70; LENZ×500 yday $4.52 → 09:30 $4.53 +5.00; BAND×4 yday $56.87 → 09:30 $56.90 +0.12; PAYP×13 yday $18.45 → 09:30 $18.28 -2.21; SEDG×6 yday $34.68 → 09:30 $33.64 -6.24 | — |
| 2026-09-14 09:30 ET | **SELL** | `CRM` | 10 | $255.75 | $2.05 | $-80.17 | $2,632.30 | ▼ -80.17 after sell → book $12,079.93; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `DELL` | 5 | $538.57 | $2.04 | $+119.91 | $5,323.11 | ▲ +119.91 after sell → book $12,077.89; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 635 | $6.02 | $8.33 | $+929.63 | $9,137.48 | ▲ +929.63 after sell → book $12,069.56; vs 09:30 mark -8.33 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `LENZ` | 500 | $4.53 | $6.55 | $-623.00 | $11,395.93 | ▼ -623.00 after sell → book $12,063.01; vs 09:30 mark -6.55 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,395.93 | ▼ close $12,046.63 vs 09:30 $12,081.98 (session -16.38) | 16:00 close · cash $11,395.93 · equity $12,046.63 vs 09:30 $12,081.98 (-35.35; session marks -16.38) · 3 name(s) marked open→close (per-name table). BAND×4 09:30 $56.90 → close $48.97 -31.72; PAYP×13 09:30 $18.28 → close $18.68 +5.20; SEDG×6 09:30 $33.64 → close $35.33 +10.14 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,395.93 | ▼ 09:30 equity $12,043.31 vs yday $12,046.63 (-3.32) | 09:30 open · cash $11,395.93 (unchanged overnight, no fees) · equity $12,043.31 vs prior close $12,046.63 (-3.32) · 3 name(s) re-marked at the open (per-name table). BAND×4 yday $48.97 → 09:30 $49.51 +2.16; PAYP×13 yday $18.68 → 09:30 $18.30 -4.94; SEDG×6 yday $35.33 → 09:30 $35.24 -0.54 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,395.93 | ▼ close $12,039.47 vs 09:30 $12,043.31 (session -3.84) | 16:00 close · cash $11,395.93 · equity $12,039.47 vs 09:30 $12,043.31 (-3.84; session marks -3.84) · 3 name(s) marked open→close (per-name table). BAND×4 09:30 $49.51 → close $50.08 +2.28; PAYP×13 09:30 $18.30 → close $17.82 -6.24; SEDG×6 09:30 $35.24 → close $35.26 +0.12 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,395.93 | ▼ 09:30 equity $12,036.40 vs yday $12,039.47 (-3.07) | 09:30 open · cash $11,395.93 (unchanged overnight, no fees) · equity $12,036.40 vs prior close $12,039.47 (-3.07) · 3 name(s) re-marked at the open (per-name table). BAND×4 yday $50.08 → 09:30 $48.60 -5.92; PAYP×13 yday $17.82 → 09:30 $17.73 -1.17; SEDG×6 yday $35.26 → 09:30 $35.93 +4.02 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 63 | $89.38 | $2.18 | — | $5,762.81 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $5697.97 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 48 | $118.18 | $2.13 | — | $88.04 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $5697.97 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.04 | ▼ close $11,584.02 vs 09:30 $12,036.40 (session -448.07) | 16:00 close · cash $88.04 · equity $11,584.02 vs 09:30 $12,036.40 (-452.38; session marks -448.07) · 5 name(s) marked open→close (per-name table). BAND×4 09:30 $48.60 → close $48.93 +1.32; PAYP×13 09:30 $17.73 → close $17.65 -1.04; SEDG×6 09:30 $35.93 → close $34.68 -7.50; SWKS×63 09:30 $89.38 → close $85.59 -238.77; QRVO×48 09:30 $118.18 → close $113.97 -202.08 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.04 | ▲ 09:30 equity $11,712.37 vs yday $11,584.02 (+128.35) | 09:30 open · cash $88.04 (unchanged overnight, no fees) · equity $11,712.37 vs prior close $11,584.02 (+128.35) · 5 name(s) re-marked at the open (per-name table). BAND×4 yday $48.93 → 09:30 $49.85 +3.68; PAYP×13 yday $17.65 → 09:30 $17.85 +2.60; SEDG×6 yday $34.68 → 09:30 $35.30 +3.72; SWKS×63 yday $85.59 → 09:30 $86.76 +73.71; QRVO×48 yday $113.97 → 09:30 $114.90 +44.64 | — |
| 2026-09-17 09:30 ET | **BUY** | `VOD` | 2 | $17.56 | $0.36 | — | $52.56 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $44.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ASAN` | 4 | $9.55 | $0.39 | — | $13.97 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list oppset; 🔵; ⚪; ret5=+17.0; leftover $44.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.97 | ▲ close $12,229.88 vs 09:30 $11,712.37 (session +518.26) | 16:00 close · cash $13.97 · equity $12,229.88 vs 09:30 $11,712.37 (+517.51; session marks +518.26) · 7 name(s) marked open→close (per-name table). BAND×4 09:30 $49.85 → close $50.51 +2.64; PAYP×13 09:30 $17.85 → close $17.91 +0.78; SEDG×6 09:30 $35.30 → close $36.00 +4.20; SWKS×63 09:30 $86.76 → close $91.32 +287.28; QRVO×48 09:30 $114.90 → close $119.51 +221.28; VOD×2 09:30 $17.56 → close $17.52 -0.08; ASAN×4 09:30 $9.55 → close $10.09 +2.16 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.97 | ▲ 09:30 equity $12,340.23 vs yday $12,229.88 (+110.35) | 09:30 open · cash $13.97 (unchanged overnight, no fees) · equity $12,340.23 vs prior close $12,229.88 (+110.35) · 7 name(s) re-marked at the open (per-name table). BAND×4 yday $50.51 → 09:30 $51.19 +2.70; PAYP×13 yday $17.91 → 09:30 $17.91 +0.00; SEDG×6 yday $36.00 → 09:30 $36.54 +3.24; SWKS×63 yday $91.32 → 09:30 $92.05 +45.99; QRVO×48 yday $119.51 → 09:30 $120.76 +60.00; VOD×2 yday $17.52 → 09:30 $16.73 -1.58; ASAN×4 yday $10.09 → 09:30 $10.09 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `BAND` | 4 | $51.19 | $2.02 | $-9.48 | $216.69 | ▼ -9.48 after sell → book $12,338.21; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `PAYP` | 13 | $17.91 | $2.05 | $-9.15 | $447.47 | ▼ -9.15 after sell → book $12,336.16; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SEDG` | 6 | $36.54 | $2.03 | $-5.48 | $664.68 | ▼ -5.48 after sell → book $12,334.13; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 5 | $29.32 | $1.48 | — | $516.60 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $166.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ARQT` | 6 | $26.14 | $1.59 | — | $358.17 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,oppset; 🔵; ⚪; ret5=+13.2; leftover $166.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FTRE` | 8 | $20.10 | $1.63 | — | $195.74 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+19.2; leftover $166.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $195.74 | ▼ close $11,941.02 vs 09:30 $12,340.23 (session -388.41) | 16:00 close · cash $195.74 · equity $11,941.02 vs 09:30 $12,340.23 (-399.21; session marks -388.41) · 7 name(s) marked open→close (per-name table). SWKS×63 09:30 $92.05 → close $88.76 -207.27; QRVO×48 09:30 $120.76 → close $117.18 -171.84; VOD×2 09:30 $16.73 → close $16.95 +0.44; ASAN×4 09:30 $10.09 → close $9.51 -2.32; SDGR×5 09:30 $29.32 → close $29.02 -1.50; ARQT×6 09:30 $26.14 → close $25.38 -4.56; FTRE×8 09:30 $20.10 → close $19.93 -1.36 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `SLG` | cash | leftover split 17.75 < 1 share @ 57.61 |
| 2026-08-14 | `WDC` | cash | leftover split 17.75 < 1 share @ 503.50 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ALM` | cash | leftover split 9.66 < 1 share @ 16.20 |
| 2026-08-17 | `NMAX` | cash | leftover split 9.66 < 1 share @ 10.97 |
| 2026-08-17 | `AAOI` | cash | leftover split 9.66 < 1 share @ 152.64 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-19 | `BTSG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `BETR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-20 | `BETR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ABX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `ABX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AEM` | cash | leftover split 43.76 < 1 share @ 216.30 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WPM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `EZPW` | cash | leftover split 24.15 < 1 share @ 35.05 |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-02 | `SMTC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `TTMI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `KEYS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `AVT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-03 | `SMTC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `TTMI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `KEYS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `AVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `IRD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `LENZ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `VSTM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `DELL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `IRD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `LENZ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `HRMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSTM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CRM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `DELL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `IRD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `LENZ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-11 | `CRM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `DELL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `IRD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `LENZ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `SEDG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `PAYP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `SEDG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-16 | `BAND` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `PAYP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `SEDG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-17 | `BAND` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `PAYP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SEDG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `QRVO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `VOD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `ASAN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `ILMN` | cash | leftover split 166.17 < 1 share @ 249.13 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `SWKS` | 63 | 2026-09-16 @ $89.38 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $5697.97 |
| `QRVO` | 48 | 2026-09-16 @ $118.18 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $5697.97 |
| `VOD` | 2 | 2026-09-17 @ $17.56 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $44.02 |
| `ASAN` | 4 | 2026-09-17 @ $9.55 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list oppset; 🔵; ⚪; ret5=+17.0; leftover $44.02 |
| `SDGR` | 5 | 2026-09-18 @ $29.32 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $166.17 |
| `ARQT` | 6 | 2026-09-18 @ $26.14 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,oppset; 🔵; ⚪; ret5=+13.2; leftover $166.17 |
| `FTRE` | 8 | 2026-09-18 @ $20.10 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+19.2; leftover $166.17 |
