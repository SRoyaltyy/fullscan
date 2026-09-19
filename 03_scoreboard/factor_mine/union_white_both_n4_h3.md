# Factor mine action — `union_white_both_n4_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `list` · size `leftover` · sell `list` · S-boost `none` · −0 red + yday up AND catalyst, top 4 by Score

Cash book **+1.69%** ($10,169) · signal-only (no cash/fees) was +21.99%. Starts YES **4/26**. Fills 58 · skips 71 · realized $+253.64.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `cam_bad_max=0,yday_and_catalyst=True` · **rank** `list` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8.54.

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
| 2026-08-18 | `BTSG` | 83 | $60.38 | $60.00 | -31.54 | — | +0.00 | -31.54 | +16.60 | — |
| 2026-08-18 | `TPG` | 98 | $51.77 | $51.77 | +0.00 | — | +0.00 | +0.00 | +112.39 | — |
| 2026-08-18 | `BETR` | 1 | $13.54 | $13.21 | -0.33 | $13.05 | -0.16 | -0.49 | -1.59 | -1.75 |
| 2026-08-18 | `ANGX` | 4 | $4.71 | $4.79 | +0.32 | $4.85 | +0.24 | +0.56 | +1.92 | +2.16 |
| 2026-08-18 | `ABX` | 1 | $9.12 | $9.03 | -0.09 | $9.01 | -0.02 | -0.11 | -0.09 | -0.11 |
| 2026-08-19 | `BETR` | 1 | $13.05 | $13.03 | -0.02 | — | +0.00 | -0.02 | -1.77 | — |
| 2026-08-19 | `ANGX` | 4 | $4.85 | $4.79 | -0.24 | — | +0.00 | -0.24 | +1.92 | — |
| 2026-08-19 | `ABX` | 1 | $9.01 | $9.08 | +0.07 | $9.15 | +0.07 | +0.14 | -0.04 | +0.03 |
| 2026-08-20 | `ABX` | 1 | $9.15 | $9.13 | -0.02 | — | +0.00 | -0.02 | +0.01 | — |
| 2026-08-20 | `BHP` | 27 | — | $91.01 | +0.00 | $93.63 | +70.74 | +70.74 | +0.00 | +70.74 |
| 2026-08-20 | `KGC` | 85 | — | $29.63 | +0.00 | $31.43 | +153.00 | +153.00 | +0.00 | +153.00 |
| 2026-08-20 | `WPM` | 17 | — | $144.54 | +0.00 | $150.25 | +97.07 | +97.07 | +0.00 | +97.07 |
| 2026-08-20 | `CYPH` | 2199 | — | $1.15 | +0.00 | $1.19 | +87.96 | +87.96 | +0.00 | +87.96 |
| 2026-08-21 | `BHP` | 27 | $93.63 | $95.72 | +56.43 | $97.03 | +35.37 | +91.80 | +127.17 | +162.54 |
| 2026-08-21 | `KGC` | 85 | $31.43 | $32.17 | +62.90 | $32.76 | +50.15 | +113.05 | +215.90 | +266.05 |
| 2026-08-21 | `WPM` | 17 | $150.25 | $154.70 | +75.65 | $157.78 | +52.36 | +128.01 | +172.72 | +225.08 |
| 2026-08-21 | `CYPH` | 2199 | $1.19 | $1.32 | +285.87 | $1.42 | +219.90 | +505.77 | +373.83 | +593.73 |
| 2026-08-21 | `AUPH` | 2 | — | $17.20 | +0.00 | $16.65 | -1.10 | -1.10 | +0.00 | -1.10 |
| 2026-08-21 | `ARCT` | 3 | — | $11.13 | +0.00 | $13.45 | +6.96 | +6.96 | +0.00 | +6.96 |
| 2026-08-24 | `BHP` | 27 | $97.03 | $97.31 | +7.56 | $97.13 | -4.86 | +2.70 | +170.10 | +165.24 |
| 2026-08-24 | `KGC` | 85 | $32.76 | $33.03 | +22.95 | $32.98 | -4.25 | +18.70 | +289.00 | +284.75 |
| 2026-08-24 | `WPM` | 17 | $157.78 | $159.50 | +29.24 | $160.19 | +11.73 | +40.97 | +254.32 | +266.05 |
| 2026-08-24 | `CYPH` | 2199 | $1.42 | $1.83 | +901.59 | $1.68 | -329.85 | +571.74 | +1495.32 | +1165.47 |
| 2026-08-24 | `AUPH` | 2 | $16.65 | $16.57 | -0.16 | $16.57 | +0.00 | -0.16 | -1.26 | -1.26 |
| 2026-08-24 | `ARCT` | 3 | $13.45 | $13.33 | -0.36 | $14.34 | +3.03 | +2.67 | +6.60 | +9.63 |
| 2026-08-25 | `BHP` | 27 | $97.13 | $95.86 | -34.29 | — | +0.00 | -34.29 | +130.95 | — |
| 2026-08-25 | `KGC` | 85 | $32.98 | $32.32 | -56.10 | — | +0.00 | -56.10 | +228.65 | — |
| 2026-08-25 | `WPM` | 17 | $160.19 | $156.51 | -62.56 | — | +0.00 | -62.56 | +203.49 | — |
| 2026-08-25 | `CYPH` | 2199 | $1.68 | $1.56 | -263.88 | $1.64 | +175.92 | -87.96 | +901.59 | +1077.51 |
| 2026-08-25 | `AUPH` | 2 | $16.57 | $16.63 | +0.12 | $16.75 | +0.24 | +0.36 | -1.14 | -0.90 |
| 2026-08-25 | `ARCT` | 3 | $14.34 | $14.12 | -0.66 | $15.44 | +3.96 | +3.30 | +8.97 | +12.93 |
| 2026-08-25 | `CRMD` | 321 | — | $8.35 | +0.00 | $8.56 | +67.41 | +67.41 | +0.00 | +67.41 |
| 2026-08-25 | `BMEA` | 1644 | — | $1.63 | +0.00 | $1.73 | +164.40 | +164.40 | +0.00 | +164.40 |
| 2026-08-25 | `EZPW` | 75 | — | $35.05 | +0.00 | $35.23 | +13.50 | +13.50 | +0.00 | +13.50 |
| 2026-08-26 | `CYPH` | 2199 | $1.64 | $1.60 | -87.96 | — | +0.00 | -87.96 | +989.55 | — |
| 2026-08-26 | `AUPH` | 2 | $16.75 | $16.60 | -0.30 | — | +0.00 | -0.30 | -1.20 | — |
| 2026-08-26 | `ARCT` | 3 | $15.44 | $15.35 | -0.27 | — | +0.00 | -0.27 | +12.66 | — |
| 2026-08-26 | `CRMD` | 321 | $8.56 | $8.60 | +12.84 | $8.39 | -67.41 | -54.57 | +80.25 | +12.84 |
| 2026-08-26 | `BMEA` | 1644 | $1.73 | $1.75 | +41.10 | $1.71 | -73.98 | -32.88 | +205.50 | +131.52 |
| 2026-08-26 | `EZPW` | 75 | $35.23 | $35.70 | +35.25 | $33.90 | -135.00 | -99.75 | +48.75 | -86.25 |
| 2026-08-27 | `CRMD` | 321 | $8.39 | $8.49 | +32.10 | $8.31 | -57.78 | -25.68 | +44.94 | -12.84 |
| 2026-08-27 | `BMEA` | 1644 | $1.71 | $1.74 | +49.32 | $1.68 | -98.64 | -49.32 | +180.84 | +82.20 |
| 2026-08-27 | `EZPW` | 75 | $33.90 | $33.50 | -30.00 | $34.41 | +68.25 | +38.25 | -116.25 | -48.00 |
| 2026-08-28 | `CRMD` | 321 | $8.31 | $8.28 | -9.63 | — | +0.00 | -9.63 | -22.47 | — |
| 2026-08-28 | `BMEA` | 1644 | $1.68 | $1.69 | +16.44 | — | +0.00 | +16.44 | +98.64 | — |
| 2026-08-28 | `EZPW` | 75 | $34.41 | $34.50 | +6.75 | — | +0.00 | +6.75 | -41.25 | — |
| 2026-08-28 | `SMTC` | 20 | — | $141.76 | +0.00 | $131.17 | -211.80 | -211.80 | +0.00 | -211.80 |
| 2026-08-28 | `TTMI` | 23 | — | $122.81 | +0.00 | $118.65 | -95.68 | -95.68 | +0.00 | -95.68 |
| 2026-08-28 | `KEYS` | 8 | — | $324.41 | +0.00 | $319.97 | -35.52 | -35.52 | +0.00 | -35.52 |
| 2026-08-28 | `AVT` | 31 | — | $91.49 | +0.00 | $88.63 | -88.66 | -88.66 | +0.00 | -88.66 |
| 2026-08-31 | `SMTC` | 20 | $131.17 | $132.30 | +22.60 | $132.96 | +13.20 | +35.80 | -189.20 | -176.00 |
| 2026-08-31 | `TTMI` | 23 | $118.65 | $118.83 | +4.14 | $118.92 | +2.07 | +6.21 | -91.54 | -89.47 |
| 2026-08-31 | `KEYS` | 8 | $319.97 | $322.49 | +20.16 | $322.70 | +1.68 | +21.84 | -15.36 | -13.68 |
| 2026-08-31 | `AVT` | 31 | $88.63 | $89.39 | +23.56 | $89.56 | +5.27 | +28.83 | -65.10 | -59.83 |
| 2026-09-01 | `SMTC` | 20 | $132.96 | $127.63 | -106.60 | $132.27 | +92.80 | -13.80 | -282.60 | -189.80 |
| 2026-09-01 | `TTMI` | 23 | $118.92 | $116.68 | -51.52 | $115.33 | -31.05 | -82.57 | -140.99 | -172.04 |
| 2026-09-01 | `KEYS` | 8 | $322.70 | $321.47 | -9.84 | $319.27 | -17.60 | -27.44 | -23.52 | -41.12 |
| 2026-09-01 | `AVT` | 31 | $89.56 | $88.58 | -30.38 | $89.39 | +25.11 | -5.27 | -90.21 | -65.10 |
| 2026-09-02 | `SMTC` | 20 | $132.27 | $133.00 | +14.60 | — | +0.00 | +14.60 | -175.20 | — |
| 2026-09-02 | `TTMI` | 23 | $115.33 | $114.22 | -25.53 | — | +0.00 | -25.53 | -197.57 | — |
| 2026-09-02 | `KEYS` | 8 | $319.27 | $318.04 | -9.84 | — | +0.00 | -9.84 | -50.96 | — |
| 2026-09-02 | `AVT` | 31 | $89.39 | $89.39 | +0.00 | — | +0.00 | +0.00 | -65.10 | — |
| 2026-09-03 | `ATRC` | 52 | — | $52.88 | +0.00 | $52.46 | -21.84 | -21.84 | +0.00 | -21.84 |
| 2026-09-03 | `HRMY` | 64 | — | $42.93 | +0.00 | $41.86 | -68.48 | -68.48 | +0.00 | -68.48 |
| 2026-09-03 | `CABA` | 763 | — | $3.63 | +0.00 | $3.48 | -114.45 | -114.45 | +0.00 | -114.45 |
| 2026-09-03 | `VSTM` | 345 | — | $8.03 | +0.00 | $7.98 | -17.25 | -17.25 | +0.00 | -17.25 |
| 2026-09-04 | `ATRC` | 52 | $52.46 | $52.03 | -22.36 | $51.52 | -26.52 | -48.88 | -44.20 | -70.72 |
| 2026-09-04 | `HRMY` | 64 | $41.86 | $41.50 | -23.04 | $42.25 | +48.00 | +24.96 | -91.52 | -43.52 |
| 2026-09-04 | `CABA` | 763 | $3.48 | $3.46 | -15.26 | $3.47 | +7.63 | -7.63 | -129.71 | -122.08 |
| 2026-09-04 | `VSTM` | 345 | $7.98 | $7.91 | -24.15 | $8.20 | +100.05 | +75.90 | -41.40 | +58.65 |
| 2026-09-04 | `IRD` | 1 | — | $4.53 | +0.00 | $4.67 | +0.14 | +0.14 | +0.00 | +0.14 |
| 2026-09-04 | `LENZ` | 1 | — | $5.75 | +0.00 | $5.96 | +0.21 | +0.21 | +0.00 | +0.21 |
| 2026-09-08 | `ATRC` | 52 | $51.52 | $54.31 | +145.08 | $53.73 | -30.16 | +114.92 | +74.36 | +44.20 |
| 2026-09-08 | `HRMY` | 64 | $42.25 | $42.20 | -3.20 | $42.07 | -8.32 | -11.52 | -46.72 | -55.04 |
| 2026-09-08 | `CABA` | 763 | $3.47 | $3.43 | -30.52 | $3.27 | -122.08 | -152.60 | -152.60 | -274.68 |
| 2026-09-08 | `VSTM` | 345 | $8.20 | $8.20 | +0.00 | $8.08 | -41.40 | -41.40 | +58.65 | +17.25 |
| 2026-09-08 | `IRD` | 1 | $4.67 | $4.53 | -0.14 | $4.34 | -0.19 | -0.33 | +0.00 | -0.19 |
| 2026-09-08 | `LENZ` | 1 | $5.96 | $5.95 | -0.01 | $5.33 | -0.62 | -0.63 | +0.20 | -0.42 |
| 2026-09-09 | `ATRC` | 52 | $53.73 | $53.16 | -29.64 | — | +0.00 | -29.64 | +14.56 | — |
| 2026-09-09 | `HRMY` | 64 | $42.07 | $42.01 | -3.84 | — | +0.00 | -3.84 | -58.88 | — |
| 2026-09-09 | `CABA` | 763 | $3.27 | $3.28 | +7.63 | — | +0.00 | +7.63 | -267.05 | — |
| 2026-09-09 | `VSTM` | 345 | $8.08 | $8.01 | -24.15 | — | +0.00 | -24.15 | -6.90 | — |
| 2026-09-09 | `IRD` | 1 | $4.34 | $5.31 | +0.97 | $5.73 | +0.42 | +1.39 | +0.78 | +1.20 |
| 2026-09-09 | `LENZ` | 1 | $5.33 | $5.31 | -0.02 | $4.91 | -0.40 | -0.42 | -0.44 | -0.84 |
| 2026-09-10 | `IRD` | 1 | $5.73 | $5.87 | +0.14 | — | +0.00 | +0.14 | +1.34 | — |
| 2026-09-10 | `LENZ` | 1 | $4.91 | $4.85 | -0.06 | — | +0.00 | -0.06 | -0.90 | — |
| 2026-09-11 | `BAND` | 68 | — | $52.55 | +0.00 | $56.87 | +293.76 | +293.76 | +0.00 | +293.76 |
| 2026-09-11 | `PAYP` | 195 | — | $18.30 | +0.00 | $18.45 | +29.25 | +29.25 | +0.00 | +29.25 |
| 2026-09-11 | `SEDG` | 97 | — | $36.78 | +0.00 | $34.68 | -203.70 | -203.70 | +0.00 | -203.70 |
| 2026-09-14 | `BAND` | 68 | $56.87 | $56.90 | +2.04 | $48.97 | -539.24 | -537.20 | +295.80 | -243.44 |
| 2026-09-14 | `PAYP` | 195 | $18.45 | $18.28 | -33.15 | $18.68 | +78.00 | +44.85 | -3.90 | +74.10 |
| 2026-09-14 | `SEDG` | 97 | $34.68 | $33.64 | -100.88 | $35.33 | +163.93 | +63.05 | -304.58 | -140.65 |
| 2026-09-15 | `BAND` | 68 | $48.97 | $49.51 | +36.72 | $50.08 | +38.76 | +75.48 | -206.72 | -167.96 |
| 2026-09-15 | `PAYP` | 195 | $18.68 | $18.30 | -74.10 | $17.82 | -93.60 | -167.70 | +0.00 | -93.60 |
| 2026-09-15 | `SEDG` | 97 | $35.33 | $35.24 | -8.73 | $35.26 | +1.94 | -6.79 | -149.38 | -147.44 |
| 2026-09-16 | `BAND` | 68 | $50.08 | $48.60 | -100.64 | — | +0.00 | -100.64 | -268.60 | — |
| 2026-09-16 | `PAYP` | 195 | $17.82 | $17.73 | -17.55 | — | +0.00 | -17.55 | -111.15 | — |
| 2026-09-16 | `SEDG` | 97 | $35.26 | $35.93 | +64.99 | — | +0.00 | +64.99 | -82.45 | — |
| 2026-09-16 | `SWKS` | 57 | — | $89.38 | +0.00 | $85.59 | -216.03 | -216.03 | +0.00 | -216.03 |
| 2026-09-16 | `QRVO` | 43 | — | $118.18 | +0.00 | $113.97 | -181.03 | -181.03 | +0.00 | -181.03 |
| 2026-09-17 | `SWKS` | 57 | $85.59 | $86.76 | +66.69 | $91.32 | +259.92 | +326.61 | -149.34 | +110.58 |
| 2026-09-17 | `QRVO` | 43 | $113.97 | $114.90 | +39.99 | $119.51 | +198.23 | +238.22 | -141.04 | +57.19 |
| 2026-09-17 | `VOD` | 2 | — | $17.56 | +0.00 | $17.52 | -0.08 | -0.08 | +0.00 | -0.08 |
| 2026-09-17 | `ASAN` | 3 | — | $9.55 | +0.00 | $10.09 | +1.62 | +1.62 | +0.00 | +1.62 |
| 2026-09-18 | `SWKS` | 57 | $91.32 | $92.05 | +41.61 | $88.76 | -187.53 | -145.92 | +152.19 | -35.34 |
| 2026-09-18 | `QRVO` | 43 | $119.51 | $120.76 | +53.75 | $117.18 | -153.94 | -100.19 | +110.94 | -43.00 |
| 2026-09-18 | `VOD` | 2 | $17.52 | $16.73 | -1.58 | $16.95 | +0.44 | -1.14 | -1.66 | -1.22 |
| 2026-09-18 | `ASAN` | 3 | $10.09 | $10.09 | +0.00 | $9.51 | -1.74 | -1.74 | +1.62 | -0.12 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +427.38 | BTSG, TPG | — | $71.00 | $10,422.85 | BTSG×83, TPG×98 |
| 2026-08-14 | +5.50 | $71.00 | BTSG×83, TPG×98 | $10,440.37 | +17.52 | -51.33 | BETR, ANGX | — | $38.63 | $10,388.71 | BTSG×83, TPG×98, BETR×1, ANGX×4 |
| 2026-08-17 | +2.25 | $38.63 | BTSG×83, TPG×98, BETR×1, ANGX×4 | $10,352.63 | -36.08 | -196.62 | ABX | — | $29.41 | $10,155.91 | BTSG×83, TPG×98, BETR×1, ANGX×4, ABX×1 |
| 2026-08-18 | -6.20 | $29.41 | BTSG×83, TPG×98, BETR×1, ANGX×4, ABX×1 | $10,124.27 | -31.64 | +0.06 | — | BTSG, TPG | $10,078.24 | $10,119.70 | BETR×1, ANGX×4, ABX×1 |
| 2026-08-19 | -7.20 | $10,078.24 | BETR×1, ANGX×4, ABX×1 | $10,119.51 | -0.19 | +0.07 | — | BETR, ANGX | $10,110.05 | $10,119.20 | ABX×1 |
| 2026-08-20 | +1.12 | $10,110.05 | ABX×1 | $10,119.18 | -0.02 | +408.77 | BHP, KGC, WPM, CYPH | ABX | $122.50 | $10,493.12 | BHP×27, KGC×85, WPM×17, CYPH×2199 |
| 2026-08-21 | +3.25 | $122.50 | BHP×27, KGC×85, WPM×17, CYPH×2199 | $10,973.97 | +480.85 | +363.64 | AUPH, ARCT | — | $54.01 | $11,336.91 | BHP×27, KGC×85, WPM×17, CYPH×2199, AUPH×2, ARCT×3 |
| 2026-08-24 | -5.17 | $54.01 | BHP×27, KGC×85, WPM×17, CYPH×2199, AUPH×2, ARCT×3 | $12,297.73 | +960.82 | -324.20 | — | — | $54.01 | $11,973.53 | BHP×27, KGC×85, WPM×17, CYPH×2199, AUPH×2, ARCT×3 |
| 2026-08-25 | +1.80 | $54.01 | BHP×27, KGC×85, WPM×17, CYPH×2199, AUPH×2, ARCT×3 | $11,556.16 | -417.37 | +425.43 | CRMD, BMEA, EZPW | BHP, KGC, WPM | $27.26 | $11,947.57 | CYPH×2199, AUPH×2, ARCT×3, CRMD×321, BMEA×1644, EZPW×75 |
| 2026-08-26 | +2.02 | $27.26 | CYPH×2199, AUPH×2, ARCT×3, CRMD×321, BMEA×1644, EZPW×75 | $11,948.23 | +0.66 | -276.39 | — | CYPH, AUPH, ARCT | $3,595.31 | $11,642.24 | CRMD×321, BMEA×1644, EZPW×75 |
| 2026-08-27 | — | $3,595.31 | CRMD×321, BMEA×1644, EZPW×75 | $11,693.66 | +51.42 | -88.17 | — | — | $3,595.31 | $11,605.49 | CRMD×321, BMEA×1644, EZPW×75 |
| 2026-08-28 | +0.75 | $3,595.31 | CRMD×321, BMEA×1644, EZPW×75 | $11,619.05 | +13.56 | -431.66 | SMTC, TTMI, KEYS, AVT | CRMD, BMEA, EZPW | $491.57 | $11,151.21 | SMTC×20, TTMI×23, KEYS×8, AVT×31 |
| 2026-08-31 | -5.85 | $491.57 | SMTC×20, TTMI×23, KEYS×8, AVT×31 | $11,221.67 | +70.46 | +22.22 | — | — | $491.57 | $11,243.89 | SMTC×20, TTMI×23, KEYS×8, AVT×31 |
| 2026-09-01 | -6.30 | $491.57 | SMTC×20, TTMI×23, KEYS×8, AVT×31 | $11,045.55 | -198.34 | +69.26 | — | — | $491.57 | $11,114.81 | SMTC×20, TTMI×23, KEYS×8, AVT×31 |
| 2026-09-02 | -3.83 | $491.57 | SMTC×20, TTMI×23, KEYS×8, AVT×31 | $11,094.04 | -20.77 | +0.00 | — | SMTC, TTMI, KEYS, AVT | $11,085.71 | $11,085.71 | — |
| 2026-09-03 | -0.90 | $11,085.71 | — | $11,085.71 | +0.00 | -222.02 | ATRC, HRMY, CABA, VSTM | — | $29.77 | $10,845.07 | ATRC×52, HRMY×64, CABA×763, VSTM×345 |
| 2026-09-04 | +2.25 | $29.77 | ATRC×52, HRMY×64, CABA×763, VSTM×345 | $10,760.26 | -84.81 | +129.51 | IRD, LENZ | — | $19.38 | $10,889.66 | ATRC×52, HRMY×64, CABA×763, VSTM×345, IRD×1, LENZ×1 |
| 2026-09-08 | -11.47 | $19.38 | ATRC×52, HRMY×64, CABA×763, VSTM×345, IRD×1, LENZ×1 | $11,000.87 | +111.21 | -202.77 | — | — | $19.38 | $10,798.10 | ATRC×52, HRMY×64, CABA×763, VSTM×345, IRD×1, LENZ×1 |
| 2026-09-09 | -13.95 | $19.38 | ATRC×52, HRMY×64, CABA×763, VSTM×345, IRD×1, LENZ×1 | $10,749.05 | -49.05 | +0.02 | — | ATRC, HRMY, CABA, VSTM | $10,719.52 | $10,730.16 | IRD×1, LENZ×1 |
| 2026-09-10 | -13.28 | $10,719.52 | IRD×1, LENZ×1 | $10,730.24 | +0.08 | +0.00 | — | IRD, LENZ | $10,730.09 | $10,730.09 | — |
| 2026-09-11 | +0.50 | $10,730.09 | — | $10,730.09 | -0.00 | +119.31 | BAND, PAYP, SEDG | — | $13.48 | $10,842.35 | BAND×68, PAYP×195, SEDG×97 |
| 2026-09-14 | -11.00 | $13.48 | BAND×68, PAYP×195, SEDG×97 | $10,710.36 | -131.99 | -297.31 | — | — | $13.48 | $10,413.05 | BAND×68, PAYP×195, SEDG×97 |
| 2026-09-15 | -3.84 | $13.48 | BAND×68, PAYP×195, SEDG×97 | $10,366.94 | -46.11 | -52.90 | — | — | $13.48 | $10,314.04 | BAND×68, PAYP×195, SEDG×97 |
| 2026-09-16 | +5.30 | $13.48 | BAND×68, PAYP×195, SEDG×97 | $10,260.84 | -53.20 | -397.06 | SWKS, QRVO | BAND, PAYP, SEDG | $72.97 | $9,852.31 | SWKS×57, QRVO×43 |
| 2026-09-17 | +7.38 | $72.97 | SWKS×57, QRVO×43 | $9,958.99 | +106.68 | +459.69 | VOD, ASAN | — | $8.54 | $10,418.02 | SWKS×57, QRVO×43, VOD×2, ASAN×3 |
| 2026-09-18 | +4.86 | $8.54 | SWKS×57, QRVO×43, VOD×2, ASAN×3 | $10,511.80 | +93.78 | -342.77 | — | — | $8.54 | $10,169.03 | SWKS×57, QRVO×43, VOD×2, ASAN×3 |

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
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 83 | $60.00 | $2.29 | $+12.07 | $5,007.12 | ▲ +12.07 after sell → book $10,121.98; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 98 | $51.77 | $2.34 | $+107.76 | $10,078.24 | ▲ +107.76 after sell → book $10,119.64; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,078.24 | ▲ close $10,119.70 vs 09:30 $10,124.27 (session +0.06) | 16:00 close · cash $10,078.24 · equity $10,119.70 vs 09:30 $10,124.27 (-4.57; session marks +0.06) · 3 name(s) marked open→close (per-name table). BETR×1 09:30 $13.21 → close $13.05 -0.16; ANGX×4 09:30 $4.79 → close $4.85 +0.24; ABX×1 09:30 $9.03 → close $9.01 -0.02 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,078.24 | ▼ 09:30 equity $10,119.51 vs yday $10,119.70 (-0.19) | 09:30 open · cash $10,078.24 (unchanged overnight, no fees) · equity $10,119.51 vs prior close $10,119.70 (-0.19) · 3 name(s) re-marked at the open (per-name table). BETR×1 yday $13.05 → 09:30 $13.03 -0.02; ANGX×4 yday $4.85 → 09:30 $4.79 -0.24; ABX×1 yday $9.01 → 09:30 $9.08 +0.07 | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 1 | $13.03 | $0.15 | $-2.07 | $10,091.12 | ▼ -2.07 after sell → book $10,119.36; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 4 | $4.79 | $0.22 | $+1.51 | $10,110.05 | ▲ +1.51 after sell → book $10,119.13; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,110.05 | ▲ close $10,119.20 vs 09:30 $10,119.51 (session +0.07) | 16:00 close · cash $10,110.05 · equity $10,119.20 vs 09:30 $10,119.51 (-0.31; session marks +0.07) · 1 name(s) marked open→close (per-name table). ABX×1 09:30 $9.08 → close $9.15 +0.07 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,110.05 | ▼ 09:30 equity $10,119.18 vs yday $10,119.20 (-0.02) | 09:30 open · cash $10,110.05 (unchanged overnight, no fees) · equity $10,119.18 vs prior close $10,119.20 (-0.02) · 1 name(s) re-marked at the open (per-name table). ABX×1 yday $9.15 → 09:30 $9.13 -0.02 | — |
| 2026-08-20 09:30 ET | **SELL** | `ABX` | 1 | $9.13 | $0.11 | $-0.20 | $10,119.07 | ▼ -0.20 after sell → book $10,119.07; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 27 | $91.01 | $2.07 | — | $7,659.73 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2529.77 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 85 | $29.63 | $2.25 | — | $5,138.93 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $2529.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 17 | $144.54 | $2.04 | — | $2,679.71 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy,oppset; 🔵; ⚪; ret5=+9.2; leftover $2529.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2199 | $1.15 | $28.37 | — | $122.50 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2529.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.50 | ▲ close $10,493.12 vs 09:30 $10,119.18 (session +408.77) | 16:00 close · cash $122.50 · equity $10,493.12 vs 09:30 $10,119.18 (+373.94; session marks +408.77) · 4 name(s) marked open→close (per-name table). BHP×27 09:30 $91.01 → close $93.63 +70.74; KGC×85 09:30 $29.63 → close $31.43 +153.00; WPM×17 09:30 $144.54 → close $150.25 +97.07; CYPH×2199 09:30 $1.15 → close $1.19 +87.96 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.50 | ▲ 09:30 equity $10,973.97 vs yday $10,493.12 (+480.85) | 09:30 open · cash $122.50 (unchanged overnight, no fees) · equity $10,973.97 vs prior close $10,493.12 (+480.85) · 4 name(s) re-marked at the open (per-name table). BHP×27 yday $93.63 → 09:30 $95.72 +56.43; KGC×85 yday $31.43 → 09:30 $32.17 +62.90; WPM×17 yday $150.25 → 09:30 $154.70 +75.65; CYPH×2199 yday $1.19 → 09:30 $1.32 +285.87 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 2 | $17.20 | $0.35 | — | $87.75 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $40.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 3 | $11.13 | $0.34 | — | $54.01 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,yday_gainer,mover_buy,oppset; 🔵; ⚪; ret5=+39.8; leftover $40.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.01 | ▲ close $11,336.91 vs 09:30 $10,973.97 (session +363.64) | 16:00 close · cash $54.01 · equity $11,336.91 vs 09:30 $10,973.97 (+362.94; session marks +363.64) · 6 name(s) marked open→close (per-name table). BHP×27 09:30 $95.72 → close $97.03 +35.37; KGC×85 09:30 $32.17 → close $32.76 +50.15; WPM×17 09:30 $154.70 → close $157.78 +52.36; CYPH×2199 09:30 $1.32 → close $1.42 +219.90; AUPH×2 09:30 $17.20 → close $16.65 -1.10; ARCT×3 09:30 $11.13 → close $13.45 +6.96 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.01 | ▲ 09:30 equity $12,297.73 vs yday $11,336.91 (+960.82) | 09:30 open · cash $54.01 (unchanged overnight, no fees) · equity $12,297.73 vs prior close $11,336.91 (+960.82) · 6 name(s) re-marked at the open (per-name table). BHP×27 yday $97.03 → 09:30 $97.31 +7.56; KGC×85 yday $32.76 → 09:30 $33.03 +22.95; WPM×17 yday $157.78 → 09:30 $159.50 +29.24; CYPH×2199 yday $1.42 → 09:30 $1.83 +901.59; AUPH×2 yday $16.65 → 09:30 $16.57 -0.16; ARCT×3 yday $13.45 → 09:30 $13.33 -0.36 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.01 | ▼ close $11,973.53 vs 09:30 $12,297.73 (session -324.20) | 16:00 close · cash $54.01 · equity $11,973.53 vs 09:30 $12,297.73 (-324.20; session marks -324.20) · 6 name(s) marked open→close (per-name table). BHP×27 09:30 $97.31 → close $97.13 -4.86; KGC×85 09:30 $33.03 → close $32.98 -4.25; WPM×17 09:30 $159.50 → close $160.19 +11.73; CYPH×2199 09:30 $1.83 → close $1.68 -329.85; AUPH×2 09:30 $16.57 → close $16.57 +0.00; ARCT×3 09:30 $13.33 → close $14.34 +3.03 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.01 | ▼ 09:30 equity $11,556.16 vs yday $11,973.53 (-417.37) | 09:30 open · cash $54.01 (unchanged overnight, no fees) · equity $11,556.16 vs prior close $11,973.53 (-417.37) · 6 name(s) re-marked at the open (per-name table). BHP×27 yday $97.13 → 09:30 $95.86 -34.29; KGC×85 yday $32.98 → 09:30 $32.32 -56.10; WPM×17 yday $160.19 → 09:30 $156.51 -62.56; CYPH×2199 yday $1.68 → 09:30 $1.56 -263.88; AUPH×2 yday $16.57 → 09:30 $16.63 +0.12; ARCT×3 yday $14.34 → 09:30 $14.12 -0.66 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 27 | $95.86 | $2.10 | $+126.78 | $2,640.13 | ▲ +126.78 after sell → book $11,554.06; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 85 | $32.32 | $2.28 | $+224.12 | $5,385.05 | ▲ +224.12 after sell → book $11,551.78; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 17 | $156.51 | $2.07 | $+199.38 | $8,043.65 | ▲ +199.38 after sell → book $11,549.71; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 321 | $8.35 | $4.14 | — | $5,359.16 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+8.0; leftover $2681.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 1644 | $1.63 | $21.21 | — | $2,658.23 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $2681.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 75 | $35.05 | $2.21 | — | $27.26 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,oppset; 🔵; ⚪; ret5=+19.7; leftover $2681.22 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.26 | ▲ close $11,947.57 vs 09:30 $11,556.16 (session +425.43) | 16:00 close · cash $27.26 · equity $11,947.57 vs 09:30 $11,556.16 (+391.41; session marks +425.43) · 6 name(s) marked open→close (per-name table). CYPH×2199 09:30 $1.56 → close $1.64 +175.92; AUPH×2 09:30 $16.63 → close $16.75 +0.24; ARCT×3 09:30 $14.12 → close $15.44 +3.96; CRMD×321 09:30 $8.35 → close $8.56 +67.41; BMEA×1644 09:30 $1.63 → close $1.73 +164.40; EZPW×75 09:30 $35.05 → close $35.23 +13.50 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.26 | ▲ 09:30 equity $11,948.23 vs yday $11,947.57 (+0.66) | 09:30 open · cash $27.26 (unchanged overnight, no fees) · equity $11,948.23 vs prior close $11,947.57 (+0.66) · 6 name(s) re-marked at the open (per-name table). CYPH×2199 yday $1.64 → 09:30 $1.60 -87.96; AUPH×2 yday $16.75 → 09:30 $16.60 -0.30; ARCT×3 yday $15.44 → 09:30 $15.35 -0.27; CRMD×321 yday $8.56 → 09:30 $8.60 +12.84; BMEA×1644 yday $1.73 → 09:30 $1.75 +41.10; EZPW×75 yday $35.23 → 09:30 $35.70 +35.25 | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 2199 | $1.60 | $28.76 | $+932.42 | $3,516.90 | ▲ +932.42 after sell → book $11,919.47; vs 09:30 mark -28.76 | dropped from list after 4 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 2 | $16.60 | $0.36 | $-1.91 | $3,549.75 | ▼ -1.91 after sell → book $11,919.12; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 3 | $15.35 | $0.49 | $+11.83 | $3,595.31 | ▲ +11.83 after sell → book $11,918.63; vs 09:30 mark -0.49 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,595.31 | ▼ close $11,642.24 vs 09:30 $11,948.23 (session -276.39) | 16:00 close · cash $3,595.31 · equity $11,642.24 vs 09:30 $11,948.23 (-305.99; session marks -276.39) · 3 name(s) marked open→close (per-name table). CRMD×321 09:30 $8.60 → close $8.39 -67.41; BMEA×1644 09:30 $1.75 → close $1.71 -73.98; EZPW×75 09:30 $35.70 → close $33.90 -135.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,595.31 | ▲ 09:30 equity $11,693.66 vs yday $11,642.24 (+51.42) | 09:30 open · cash $3,595.31 (unchanged overnight, no fees) · equity $11,693.66 vs prior close $11,642.24 (+51.42) · 3 name(s) re-marked at the open (per-name table). CRMD×321 yday $8.39 → 09:30 $8.49 +32.10; BMEA×1644 yday $1.71 → 09:30 $1.74 +49.32; EZPW×75 yday $33.90 → 09:30 $33.50 -30.00 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,595.31 | ▼ close $11,605.49 vs 09:30 $11,693.66 (session -88.17) | 16:00 close · cash $3,595.31 · equity $11,605.49 vs 09:30 $11,693.66 (-88.17; session marks -88.17) · 3 name(s) marked open→close (per-name table). CRMD×321 09:30 $8.49 → close $8.31 -57.78; BMEA×1644 09:30 $1.74 → close $1.68 -98.64; EZPW×75 09:30 $33.50 → close $34.41 +68.25 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,595.31 | ▲ 09:30 equity $11,619.05 vs yday $11,605.49 (+13.56) | 09:30 open · cash $3,595.31 (unchanged overnight, no fees) · equity $11,619.05 vs prior close $11,605.49 (+13.56) · 3 name(s) re-marked at the open (per-name table). CRMD×321 yday $8.31 → 09:30 $8.28 -9.63; BMEA×1644 yday $1.68 → 09:30 $1.69 +16.44; EZPW×75 yday $34.41 → 09:30 $34.50 +6.75 | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 321 | $8.28 | $4.22 | $-30.83 | $6,248.97 | ▼ -30.83 after sell → book $11,614.83; vs 09:30 mark -4.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 1644 | $1.69 | $21.50 | $+55.93 | $9,005.83 | ▲ +55.93 after sell → book $11,593.33; vs 09:30 mark -21.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 75 | $34.50 | $2.25 | $-45.71 | $11,591.08 | ▼ -45.71 after sell → book $11,591.08; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 20 | $141.76 | $2.05 | — | $8,753.83 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $2897.77 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 23 | $122.81 | $2.06 | — | $5,927.14 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $2897.77 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 8 | $324.41 | $2.01 | — | $3,329.85 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $2897.77 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 31 | $91.49 | $2.08 | — | $491.57 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $2897.77 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $491.57 | ▼ close $11,151.21 vs 09:30 $11,619.05 (session -431.66) | 16:00 close · cash $491.57 · equity $11,151.21 vs 09:30 $11,619.05 (-467.84; session marks -431.66) · 4 name(s) marked open→close (per-name table). SMTC×20 09:30 $141.76 → close $131.17 -211.80; TTMI×23 09:30 $122.81 → close $118.65 -95.68; KEYS×8 09:30 $324.41 → close $319.97 -35.52; AVT×31 09:30 $91.49 → close $88.63 -88.66 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $491.57 | ▲ 09:30 equity $11,221.67 vs yday $11,151.21 (+70.46) | 09:30 open · cash $491.57 (unchanged overnight, no fees) · equity $11,221.67 vs prior close $11,151.21 (+70.46) · 4 name(s) re-marked at the open (per-name table). SMTC×20 yday $131.17 → 09:30 $132.30 +22.60; TTMI×23 yday $118.65 → 09:30 $118.83 +4.14; KEYS×8 yday $319.97 → 09:30 $322.49 +20.16; AVT×31 yday $88.63 → 09:30 $89.39 +23.56 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $491.57 | ▲ close $11,243.89 vs 09:30 $11,221.67 (session +22.22) | 16:00 close · cash $491.57 · equity $11,243.89 vs 09:30 $11,221.67 (+22.22; session marks +22.22) · 4 name(s) marked open→close (per-name table). SMTC×20 09:30 $132.30 → close $132.96 +13.20; TTMI×23 09:30 $118.83 → close $118.92 +2.07; KEYS×8 09:30 $322.49 → close $322.70 +1.68; AVT×31 09:30 $89.39 → close $89.56 +5.27 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $491.57 | ▼ 09:30 equity $11,045.55 vs yday $11,243.89 (-198.34) | 09:30 open · cash $491.57 (unchanged overnight, no fees) · equity $11,045.55 vs prior close $11,243.89 (-198.34) · 4 name(s) re-marked at the open (per-name table). SMTC×20 yday $132.96 → 09:30 $127.63 -106.60; TTMI×23 yday $118.92 → 09:30 $116.68 -51.52; KEYS×8 yday $322.70 → 09:30 $321.47 -9.84; AVT×31 yday $89.56 → 09:30 $88.58 -30.38 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $491.57 | ▲ close $11,114.81 vs 09:30 $11,045.55 (session +69.26) | 16:00 close · cash $491.57 · equity $11,114.81 vs 09:30 $11,045.55 (+69.26; session marks +69.26) · 4 name(s) marked open→close (per-name table). SMTC×20 09:30 $127.63 → close $132.27 +92.80; TTMI×23 09:30 $116.68 → close $115.33 -31.05; KEYS×8 09:30 $321.47 → close $319.27 -17.60; AVT×31 09:30 $88.58 → close $89.39 +25.11 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $491.57 | ▼ 09:30 equity $11,094.04 vs yday $11,114.81 (-20.77) | 09:30 open · cash $491.57 (unchanged overnight, no fees) · equity $11,094.04 vs prior close $11,114.81 (-20.77) · 4 name(s) re-marked at the open (per-name table). SMTC×20 yday $132.27 → 09:30 $133.00 +14.60; TTMI×23 yday $115.33 → 09:30 $114.22 -25.53; KEYS×8 yday $319.27 → 09:30 $318.04 -9.84; AVT×31 yday $89.39 → 09:30 $89.39 +0.00 | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 20 | $133.00 | $2.08 | $-179.33 | $3,149.49 | ▼ -179.33 after sell → book $11,091.96; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 23 | $114.22 | $2.09 | $-201.72 | $5,774.46 | ▼ -201.72 after sell → book $11,089.87; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 8 | $318.04 | $2.04 | $-55.02 | $8,316.74 | ▼ -55.02 after sell → book $11,087.83; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AVT` | 31 | $89.39 | $2.12 | $-69.30 | $11,085.71 | ▼ -69.30 after sell → book $11,085.71; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,085.71 | ▲ close $11,085.71 vs 09:30 $11,094.04 (session +0.00) | 16:00 close · cash $11,085.71 · no lots left · equity $11,085.71. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,085.71 | ▲ 09:30 equity $11,085.71 vs yday $11,085.71 (+0.00) | 09:30 open · cash $11,085.71 · no holdings · equity $11,085.71 vs prior close $11,085.71 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 52 | $52.88 | $2.15 | — | $8,333.81 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $2771.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 64 | $42.93 | $2.18 | — | $5,584.10 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $2771.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 763 | $3.63 | $9.84 | — | $2,804.57 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $2771.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 345 | $8.03 | $4.45 | — | $29.77 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $2771.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.77 | ▼ close $10,845.07 vs 09:30 $11,085.71 (session -222.02) | 16:00 close · cash $29.77 · equity $10,845.07 vs 09:30 $11,085.71 (-240.64; session marks -222.02) · 4 name(s) marked open→close (per-name table). ATRC×52 09:30 $52.88 → close $52.46 -21.84; HRMY×64 09:30 $42.93 → close $41.86 -68.48; CABA×763 09:30 $3.63 → close $3.48 -114.45; VSTM×345 09:30 $8.03 → close $7.98 -17.25 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.77 | ▼ 09:30 equity $10,760.26 vs yday $10,845.07 (-84.81) | 09:30 open · cash $29.77 (unchanged overnight, no fees) · equity $10,760.26 vs prior close $10,845.07 (-84.81) · 4 name(s) re-marked at the open (per-name table). ATRC×52 yday $52.46 → 09:30 $52.03 -22.36; HRMY×64 yday $41.86 → 09:30 $41.50 -23.04; CABA×763 yday $3.48 → 09:30 $3.46 -15.26; VSTM×345 yday $7.98 → 09:30 $7.91 -24.15 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 1 | $4.53 | $0.05 | — | $25.19 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $7.44 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 1 | $5.75 | $0.06 | — | $19.38 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $7.44 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.38 | ▲ close $10,889.66 vs 09:30 $10,760.26 (session +129.51) | 16:00 close · cash $19.38 · equity $10,889.66 vs 09:30 $10,760.26 (+129.40; session marks +129.51) · 6 name(s) marked open→close (per-name table). ATRC×52 09:30 $52.03 → close $51.52 -26.52; HRMY×64 09:30 $41.50 → close $42.25 +48.00; CABA×763 09:30 $3.46 → close $3.47 +7.63; VSTM×345 09:30 $7.91 → close $8.20 +100.05; IRD×1 09:30 $4.53 → close $4.67 +0.14; LENZ×1 09:30 $5.75 → close $5.96 +0.21 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.38 | ▲ 09:30 equity $11,000.87 vs yday $10,889.66 (+111.21) | 09:30 open · cash $19.38 (unchanged overnight, no fees) · equity $11,000.87 vs prior close $10,889.66 (+111.21) · 6 name(s) re-marked at the open (per-name table). ATRC×52 yday $51.52 → 09:30 $54.31 +145.08; HRMY×64 yday $42.25 → 09:30 $42.20 -3.20; CABA×763 yday $3.47 → 09:30 $3.43 -30.52; VSTM×345 yday $8.20 → 09:30 $8.20 +0.00; IRD×1 yday $4.67 → 09:30 $4.53 -0.14; LENZ×1 yday $5.96 → 09:30 $5.95 -0.01 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.38 | ▼ close $10,798.10 vs 09:30 $11,000.87 (session -202.77) | 16:00 close · cash $19.38 · equity $10,798.10 vs 09:30 $11,000.87 (-202.77; session marks -202.77) · 6 name(s) marked open→close (per-name table). ATRC×52 09:30 $54.31 → close $53.73 -30.16; HRMY×64 09:30 $42.20 → close $42.07 -8.32; CABA×763 09:30 $3.43 → close $3.27 -122.08; VSTM×345 09:30 $8.20 → close $8.08 -41.40; IRD×1 09:30 $4.53 → close $4.34 -0.19; LENZ×1 09:30 $5.95 → close $5.33 -0.62 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.38 | ▼ 09:30 equity $10,749.05 vs yday $10,798.10 (-49.05) | 09:30 open · cash $19.38 (unchanged overnight, no fees) · equity $10,749.05 vs prior close $10,798.10 (-49.05) · 6 name(s) re-marked at the open (per-name table). ATRC×52 yday $53.73 → 09:30 $53.16 -29.64; HRMY×64 yday $42.07 → 09:30 $42.01 -3.84; CABA×763 yday $3.27 → 09:30 $3.28 +7.63; VSTM×345 yday $8.08 → 09:30 $8.01 -24.15; IRD×1 yday $4.34 → 09:30 $5.31 +0.97; LENZ×1 yday $5.33 → 09:30 $5.31 -0.02 | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 52 | $53.16 | $2.18 | $+10.24 | $2,781.52 | ▲ +10.24 after sell → book $10,746.87; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 64 | $42.01 | $2.21 | $-63.28 | $5,467.95 | ▼ -63.28 after sell → book $10,744.66; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 763 | $3.28 | $9.99 | $-286.88 | $7,960.60 | ▼ -286.88 after sell → book $10,734.67; vs 09:30 mark -9.99 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 345 | $8.01 | $4.53 | $-15.88 | $10,719.52 | ▼ -15.88 after sell → book $10,730.14; vs 09:30 mark -4.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,719.52 | ▲ close $10,730.16 vs 09:30 $10,749.05 (session +0.02) | 16:00 close · cash $10,719.52 · equity $10,730.16 vs 09:30 $10,749.05 (-18.89; session marks +0.02) · 2 name(s) marked open→close (per-name table). IRD×1 09:30 $5.31 → close $5.73 +0.42; LENZ×1 09:30 $5.31 → close $4.91 -0.40 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,719.52 | ▲ 09:30 equity $10,730.24 vs yday $10,730.16 (+0.08) | 09:30 open · cash $10,719.52 (unchanged overnight, no fees) · equity $10,730.24 vs prior close $10,730.16 (+0.08) · 2 name(s) re-marked at the open (per-name table). IRD×1 yday $5.73 → 09:30 $5.87 +0.14; LENZ×1 yday $4.91 → 09:30 $4.85 -0.06 | — |
| 2026-09-10 09:30 ET | **SELL** | `IRD` | 1 | $5.87 | $0.08 | $+1.21 | $10,725.31 | ▲ +1.21 after sell → book $10,730.16; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🔴 judge🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-10 09:30 ET | **SELL** | `LENZ` | 1 | $4.85 | $0.07 | $-1.03 | $10,730.09 | ▼ -1.03 after sell → book $10,730.09; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,730.09 | ▲ close $10,730.09 vs 09:30 $10,730.24 (session +0.00) | 16:00 close · cash $10,730.09 · no lots left · equity $10,730.09. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,730.09 | ▲ 09:30 equity $10,730.09 vs yday $10,730.09 (-0.00) | 09:30 open · cash $10,730.09 · no holdings · equity $10,730.09 vs prior close $10,730.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 68 | $52.55 | $2.19 | — | $7,154.49 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $3576.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 195 | $18.30 | $2.58 | — | $3,583.42 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $3576.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SEDG` | 97 | $36.78 | $2.28 | — | $13.48 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list oppset; 🔵; ⚪; ret5=+8.2; leftover $3576.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.48 | ▲ close $10,842.35 vs 09:30 $10,730.09 (session +119.31) | 16:00 close · cash $13.48 · equity $10,842.35 vs 09:30 $10,730.09 (+112.26; session marks +119.31) · 3 name(s) marked open→close (per-name table). BAND×68 09:30 $52.55 → close $56.87 +293.76; PAYP×195 09:30 $18.30 → close $18.45 +29.25; SEDG×97 09:30 $36.78 → close $34.68 -203.70 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.48 | ▼ 09:30 equity $10,710.36 vs yday $10,842.35 (-131.99) | 09:30 open · cash $13.48 (unchanged overnight, no fees) · equity $10,710.36 vs prior close $10,842.35 (-131.99) · 3 name(s) re-marked at the open (per-name table). BAND×68 yday $56.87 → 09:30 $56.90 +2.04; PAYP×195 yday $18.45 → 09:30 $18.28 -33.15; SEDG×97 yday $34.68 → 09:30 $33.64 -100.88 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.48 | ▼ close $10,413.05 vs 09:30 $10,710.36 (session -297.31) | 16:00 close · cash $13.48 · equity $10,413.05 vs 09:30 $10,710.36 (-297.31; session marks -297.31) · 3 name(s) marked open→close (per-name table). BAND×68 09:30 $56.90 → close $48.97 -539.24; PAYP×195 09:30 $18.28 → close $18.68 +78.00; SEDG×97 09:30 $33.64 → close $35.33 +163.93 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.48 | ▼ 09:30 equity $10,366.94 vs yday $10,413.05 (-46.11) | 09:30 open · cash $13.48 (unchanged overnight, no fees) · equity $10,366.94 vs prior close $10,413.05 (-46.11) · 3 name(s) re-marked at the open (per-name table). BAND×68 yday $48.97 → 09:30 $49.51 +36.72; PAYP×195 yday $18.68 → 09:30 $18.30 -74.10; SEDG×97 yday $35.33 → 09:30 $35.24 -8.73 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.48 | ▼ close $10,314.04 vs 09:30 $10,366.94 (session -52.90) | 16:00 close · cash $13.48 · equity $10,314.04 vs 09:30 $10,366.94 (-52.90; session marks -52.90) · 3 name(s) marked open→close (per-name table). BAND×68 09:30 $49.51 → close $50.08 +38.76; PAYP×195 09:30 $18.30 → close $17.82 -93.60; SEDG×97 09:30 $35.24 → close $35.26 +1.94 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.48 | ▼ 09:30 equity $10,260.84 vs yday $10,314.04 (-53.20) | 09:30 open · cash $13.48 (unchanged overnight, no fees) · equity $10,260.84 vs prior close $10,314.04 (-53.20) · 3 name(s) re-marked at the open (per-name table). BAND×68 yday $50.08 → 09:30 $48.60 -100.64; PAYP×195 yday $17.82 → 09:30 $17.73 -17.55; SEDG×97 yday $35.26 → 09:30 $35.93 +64.99 | — |
| 2026-09-16 09:30 ET | **SELL** | `BAND` | 68 | $48.60 | $2.23 | $-273.03 | $3,316.05 | ▼ -273.03 after sell → book $10,258.61; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAYP` | 195 | $17.73 | $2.63 | $-116.36 | $6,770.76 | ▼ -116.36 after sell → book $10,255.97; vs 09:30 mark -2.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `SEDG` | 97 | $35.93 | $2.33 | $-87.06 | $10,253.65 | ▼ -87.06 after sell → book $10,253.65; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 57 | $89.38 | $2.16 | — | $5,156.83 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $5126.82 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 43 | $118.18 | $2.12 | — | $72.97 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $5126.82 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.97 | ▼ close $9,852.31 vs 09:30 $10,260.84 (session -397.06) | 16:00 close · cash $72.97 · equity $9,852.31 vs 09:30 $10,260.84 (-408.53; session marks -397.06) · 2 name(s) marked open→close (per-name table). SWKS×57 09:30 $89.38 → close $85.59 -216.03; QRVO×43 09:30 $118.18 → close $113.97 -181.03 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.97 | ▲ 09:30 equity $9,958.99 vs yday $9,852.31 (+106.68) | 09:30 open · cash $72.97 (unchanged overnight, no fees) · equity $9,958.99 vs prior close $9,852.31 (+106.68) · 2 name(s) re-marked at the open (per-name table). SWKS×57 yday $85.59 → 09:30 $86.76 +66.69; QRVO×43 yday $113.97 → 09:30 $114.90 +39.99 | — |
| 2026-09-17 09:30 ET | **BUY** | `VOD` | 2 | $17.56 | $0.36 | — | $37.49 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $36.48 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ASAN` | 3 | $9.55 | $0.30 | — | $8.54 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list oppset; 🔵; ⚪; ret5=+17.0; leftover $36.48 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.54 | ▲ close $10,418.02 vs 09:30 $9,958.99 (session +459.69) | 16:00 close · cash $8.54 · equity $10,418.02 vs 09:30 $9,958.99 (+459.03; session marks +459.69) · 4 name(s) marked open→close (per-name table). SWKS×57 09:30 $86.76 → close $91.32 +259.92; QRVO×43 09:30 $114.90 → close $119.51 +198.23; VOD×2 09:30 $17.56 → close $17.52 -0.08; ASAN×3 09:30 $9.55 → close $10.09 +1.62 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.54 | ▲ 09:30 equity $10,511.80 vs yday $10,418.02 (+93.78) | 09:30 open · cash $8.54 (unchanged overnight, no fees) · equity $10,511.80 vs prior close $10,418.02 (+93.78) · 4 name(s) re-marked at the open (per-name table). SWKS×57 yday $91.32 → 09:30 $92.05 +41.61; QRVO×43 yday $119.51 → 09:30 $120.76 +53.75; VOD×2 yday $17.52 → 09:30 $16.73 -1.58; ASAN×3 yday $10.09 → 09:30 $10.09 +0.00 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.54 | ▼ close $10,169.03 vs 09:30 $10,511.80 (session -342.77) | 16:00 close · cash $8.54 · equity $10,169.03 vs 09:30 $10,511.80 (-342.77; session marks -342.77) · 4 name(s) marked open→close (per-name table). SWKS×57 09:30 $92.05 → close $88.76 -187.53; QRVO×43 09:30 $120.76 → close $117.18 -153.94; VOD×2 09:30 $16.73 → close $16.95 +0.44; ASAN×3 09:30 $10.09 → close $9.51 -1.74 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLG` | cash | leftover split 17.75 < 1 share @ 57.61 |
| 2026-08-14 | `WDC` | cash | leftover split 17.75 < 1 share @ 503.50 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ALM` | cash | leftover split 9.66 < 1 share @ 16.20 |
| 2026-08-17 | `NMAX` | cash | leftover split 9.66 < 1 share @ 10.97 |
| 2026-08-17 | `AAOI` | cash | leftover split 9.66 < 1 share @ 152.64 |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AEM` | cash | leftover split 40.83 < 1 share @ 216.30 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 7.44 < 1 share @ 263.36 |
| 2026-09-04 | `DELL` | cash | leftover split 7.44 < 1 share @ 513.78 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `LENZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `IRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LENZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAYP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `VOD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ASAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ILMN` | cash | leftover split 2.14 < 1 share @ 249.13 |
| 2026-09-18 | `SDGR` | cash | leftover split 2.14 < 1 share @ 29.32 |
| 2026-09-18 | `ARQT` | cash | leftover split 2.14 < 1 share @ 26.14 |
| 2026-09-18 | `FTRE` | cash | leftover split 2.14 < 1 share @ 20.10 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `SWKS` | 57 | 2026-09-16 @ $89.38 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $5126.82 |
| `QRVO` | 43 | 2026-09-16 @ $118.18 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $5126.82 |
| `VOD` | 2 | 2026-09-17 @ $17.56 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $36.48 |
| `ASAN` | 3 | 2026-09-17 @ $9.55 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list oppset; 🔵; ⚪; ret5=+17.0; leftover $36.48 |
