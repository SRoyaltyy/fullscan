# Factor mine action — `union_news_pack_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · morning packet news🟢 only (not the merged box)

Cash book **-14.29%** ($8,571) · signal-only (no cash/fees) was -10.21%. Starts YES **7/26**. Fills 49 · skips 94 · realized $-926.55.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the morning news packet box is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
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
- **Gate** `news_box=good` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $216.64.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `NRG` | 27 | — | $120.00 | +0.00 | $126.24 | +168.48 | +168.48 | +0.00 | +168.48 |
| 2026-08-14 | `TLN` | 9 | — | $359.83 | +0.00 | $362.74 | +26.19 | +26.19 | +0.00 | +26.19 |
| 2026-08-14 | `VST` | 22 | — | $146.90 | +0.00 | $148.13 | +27.06 | +27.06 | +0.00 | +27.06 |
| 2026-08-17 | `NRG` | 27 | $126.24 | $127.40 | +31.32 | $122.37 | -135.81 | -104.49 | +199.80 | +63.99 |
| 2026-08-17 | `TLN` | 9 | $362.74 | $367.88 | +46.26 | $356.92 | -98.64 | -52.38 | +72.45 | -26.19 |
| 2026-08-17 | `VST` | 22 | $148.13 | $149.37 | +27.28 | $146.11 | -71.72 | -44.44 | +54.34 | -17.38 |
| 2026-08-17 | `DVN` | 2 | — | $46.18 | +0.00 | $47.57 | +2.78 | +2.78 | +0.00 | +2.78 |
| 2026-08-18 | `NRG` | 27 | $122.37 | $121.92 | -12.15 | $115.56 | -171.72 | -183.87 | +51.84 | -119.88 |
| 2026-08-18 | `TLN` | 9 | $356.92 | $350.89 | -54.27 | $317.66 | -299.07 | -353.34 | -80.46 | -379.53 |
| 2026-08-18 | `VST` | 22 | $146.11 | $144.50 | -35.42 | $140.52 | -87.56 | -122.98 | -52.80 | -140.36 |
| 2026-08-18 | `DVN` | 2 | $47.57 | $48.00 | +0.86 | $47.83 | -0.34 | +0.52 | +3.64 | +3.30 |
| 2026-08-19 | `NRG` | 27 | $115.56 | $116.20 | +17.28 | — | +0.00 | +17.28 | -102.60 | — |
| 2026-08-19 | `TLN` | 9 | $317.66 | $321.00 | +30.06 | — | +0.00 | +30.06 | -349.47 | — |
| 2026-08-19 | `VST` | 22 | $140.52 | $140.74 | +4.84 | — | +0.00 | +4.84 | -135.52 | — |
| 2026-08-19 | `DVN` | 2 | $47.83 | $48.22 | +0.78 | $48.19 | -0.06 | +0.72 | +4.08 | +4.02 |
| 2026-08-20 | `DVN` | 2 | $48.19 | $49.02 | +1.66 | — | +0.00 | +1.66 | +5.68 | — |
| 2026-08-20 | `APA` | 210 | — | $44.76 | +0.00 | $44.39 | -77.70 | -77.70 | +0.00 | -77.70 |
| 2026-08-21 | `APA` | 210 | $44.39 | $44.52 | +27.30 | $43.39 | -237.30 | -210.00 | -50.40 | -287.70 |
| 2026-08-24 | `APA` | 210 | $43.39 | $42.93 | -96.60 | $42.96 | +6.30 | -90.30 | -384.30 | -378.00 |
| 2026-08-25 | `APA` | 210 | $42.96 | $41.38 | -331.80 | — | +0.00 | -331.80 | -709.80 | — |
| 2026-08-25 | `AU` | 24 | — | $118.52 | +0.00 | $123.39 | +116.88 | +116.88 | +0.00 | +116.88 |
| 2026-08-25 | `FCX` | 37 | — | $77.13 | +0.00 | $79.91 | +102.86 | +102.86 | +0.00 | +102.86 |
| 2026-08-25 | `AMX` | 121 | — | $23.80 | +0.00 | $23.75 | -6.05 | -6.05 | +0.00 | -6.05 |
| 2026-08-26 | `AU` | 24 | $123.39 | $119.80 | -86.16 | $118.11 | -40.56 | -126.72 | +30.72 | -9.84 |
| 2026-08-26 | `FCX` | 37 | $79.91 | $79.34 | -21.09 | $79.00 | -12.58 | -33.67 | +81.77 | +69.19 |
| 2026-08-26 | `AMX` | 121 | $23.75 | $23.75 | +0.00 | $23.62 | -15.73 | -15.73 | -6.05 | -21.78 |
| 2026-08-27 | `AU` | 24 | $118.11 | $117.41 | -16.80 | $118.40 | +23.76 | +6.96 | -26.64 | -2.88 |
| 2026-08-27 | `FCX` | 37 | $79.00 | $78.83 | -6.29 | $78.42 | -15.17 | -21.46 | +62.90 | +47.73 |
| 2026-08-27 | `AMX` | 121 | $23.62 | $23.77 | +18.15 | $23.50 | -32.67 | -14.52 | -3.63 | -36.30 |
| 2026-08-28 | `AU` | 24 | $118.40 | $119.19 | +18.96 | — | +0.00 | +18.96 | +16.08 | — |
| 2026-08-28 | `FCX` | 37 | $78.42 | $78.57 | +5.55 | — | +0.00 | +5.55 | +53.28 | — |
| 2026-08-28 | `AMX` | 121 | $23.50 | $23.64 | +16.94 | — | +0.00 | +16.94 | -19.36 | — |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 6 | — | $240.22 | +0.00 | $236.98 | -19.44 | -19.44 | +0.00 | -19.44 |
| 2026-08-28 | `ADSK` | 5 | — | $261.16 | +0.00 | $260.66 | -2.50 | -2.50 | +0.00 | -2.50 |
| 2026-08-28 | `RRC` | 34 | — | $41.74 | +0.00 | $41.46 | -9.52 | -9.52 | +0.00 | -9.52 |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | $322.70 | +0.84 | +10.92 | -7.68 | -6.84 |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | $382.80 | +13.08 | +13.08 | -65.94 | -52.86 |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | $1267.77 | +5.87 | +11.51 | -44.13 | -38.26 |
| 2026-08-31 | `DDOG` | 6 | $236.98 | $233.97 | -18.09 | $237.04 | +18.45 | +0.36 | -37.53 | -19.08 |
| 2026-08-31 | `ADSK` | 5 | $260.66 | $257.71 | -14.75 | $258.53 | +4.10 | -10.65 | -17.25 | -13.15 |
| 2026-08-31 | `RRC` | 34 | $41.46 | $42.00 | +18.36 | $41.32 | -23.12 | -4.76 | +8.84 | -14.28 |
| 2026-09-01 | `KEYS` | 4 | $322.70 | $321.47 | -4.92 | $319.27 | -8.80 | -13.72 | -11.76 | -20.56 |
| 2026-09-01 | `CIEN` | 3 | $382.80 | $376.89 | -17.73 | $360.33 | -49.68 | -67.41 | -70.59 | -120.27 |
| 2026-09-01 | `MPWR` | 1 | $1267.77 | $1245.11 | -22.66 | $1225.96 | -19.15 | -41.81 | -60.92 | -80.07 |
| 2026-09-01 | `DDOG` | 6 | $237.04 | $232.88 | -24.96 | $223.84 | -54.24 | -79.20 | -44.04 | -98.28 |
| 2026-09-01 | `ADSK` | 5 | $258.53 | $253.48 | -25.25 | $247.69 | -28.95 | -54.20 | -38.40 | -67.35 |
| 2026-09-01 | `RRC` | 34 | $41.32 | $41.94 | +21.08 | $42.40 | +15.64 | +36.72 | +6.80 | +22.44 |
| 2026-09-02 | `KEYS` | 4 | $319.27 | $318.04 | -4.92 | — | +0.00 | -4.92 | -25.48 | — |
| 2026-09-02 | `CIEN` | 3 | $360.33 | $357.25 | -9.24 | — | +0.00 | -9.24 | -129.51 | — |
| 2026-09-02 | `MPWR` | 1 | $1225.96 | $1224.92 | -1.04 | — | +0.00 | -1.04 | -81.11 | — |
| 2026-09-02 | `DDOG` | 6 | $223.84 | $219.46 | -26.28 | — | +0.00 | -26.28 | -124.56 | — |
| 2026-09-02 | `ADSK` | 5 | $247.69 | $246.70 | -4.95 | — | +0.00 | -4.95 | -72.30 | — |
| 2026-09-02 | `RRC` | 34 | $42.40 | $42.10 | -10.20 | — | +0.00 | -10.20 | +12.24 | — |
| 2026-09-03 | `AVGO` | 5 | — | $351.74 | +0.00 | $357.16 | +27.10 | +27.10 | +0.00 | +27.10 |
| 2026-09-03 | `DELL` | 4 | — | $486.31 | +0.00 | $516.39 | +120.32 | +120.32 | +0.00 | +120.32 |
| 2026-09-03 | `HPE` | 43 | — | $47.60 | +0.00 | $54.44 | +294.12 | +294.12 | +0.00 | +294.12 |
| 2026-09-03 | `CIEN` | 5 | — | $354.49 | +0.00 | $317.46 | -185.15 | -185.15 | +0.00 | -185.15 |
| 2026-09-04 | `AVGO` | 5 | $357.16 | $359.70 | +12.70 | $357.90 | -9.00 | +3.70 | +39.80 | +30.80 |
| 2026-09-04 | `DELL` | 4 | $516.39 | $513.78 | -10.44 | $524.14 | +41.44 | +31.00 | +109.88 | +151.32 |
| 2026-09-04 | `HPE` | 43 | $54.44 | $53.85 | -25.37 | $52.00 | -79.55 | -104.92 | +268.75 | +189.20 |
| 2026-09-04 | `CIEN` | 5 | $317.46 | $321.67 | +21.05 | $321.00 | -3.35 | +17.70 | -164.10 | -167.45 |
| 2026-09-04 | `AMX` | 8 | — | $23.03 | +0.00 | $23.00 | -0.24 | -0.24 | +0.00 | -0.24 |
| 2026-09-04 | `MSTR` | 1 | — | $137.35 | +0.00 | $142.80 | +5.45 | +5.45 | +0.00 | +5.45 |
| 2026-09-08 | `AVGO` | 5 | $357.90 | $363.68 | +28.90 | $368.56 | +24.40 | +53.30 | +59.70 | +84.10 |
| 2026-09-08 | `DELL` | 4 | $524.14 | $521.15 | -11.96 | $533.88 | +50.92 | +38.96 | +139.36 | +190.28 |
| 2026-09-08 | `HPE` | 43 | $52.00 | $52.29 | +12.47 | $56.03 | +160.82 | +173.29 | +201.67 | +362.49 |
| 2026-09-08 | `CIEN` | 5 | $321.00 | $327.42 | +32.10 | $341.29 | +69.35 | +101.45 | -135.35 | -66.00 |
| 2026-09-08 | `AMX` | 8 | $23.00 | $23.15 | +1.20 | $23.00 | -1.20 | +0.00 | +0.96 | -0.24 |
| 2026-09-08 | `MSTR` | 1 | $142.80 | $137.62 | -5.18 | $136.52 | -1.10 | -6.28 | +0.27 | -0.83 |
| 2026-09-09 | `AVGO` | 5 | $368.56 | $366.23 | -11.65 | — | +0.00 | -11.65 | +72.45 | — |
| 2026-09-09 | `DELL` | 4 | $533.88 | $538.47 | +18.36 | — | +0.00 | +18.36 | +208.64 | — |
| 2026-09-09 | `HPE` | 43 | $56.03 | $56.94 | +39.13 | — | +0.00 | +39.13 | +401.62 | — |
| 2026-09-09 | `CIEN` | 5 | $341.29 | $341.90 | +3.05 | — | +0.00 | +3.05 | -62.95 | — |
| 2026-09-09 | `AMX` | 8 | $23.00 | $22.88 | -0.96 | $23.10 | +1.76 | +0.80 | -1.20 | +0.56 |
| 2026-09-09 | `MSTR` | 1 | $136.52 | $141.82 | +5.30 | $132.70 | -9.12 | -3.82 | +4.47 | -4.65 |
| 2026-09-10 | `AMX` | 8 | $23.10 | $23.05 | -0.40 | — | +0.00 | -0.40 | +0.16 | — |
| 2026-09-10 | `MSTR` | 1 | $132.70 | $128.44 | -4.26 | — | +0.00 | -4.26 | -8.91 | — |
| 2026-09-11 | `BTI` | 52 | — | $56.03 | +0.00 | $55.24 | -41.08 | -41.08 | +0.00 | -41.08 |
| 2026-09-11 | `ADBE` | 12 | — | $242.17 | +0.00 | $252.23 | +120.72 | +120.72 | +0.00 | +120.72 |
| 2026-09-11 | `CNQ` | 59 | — | $49.94 | +0.00 | $50.07 | +7.67 | +7.67 | +0.00 | +7.67 |
| 2026-09-14 | `BTI` | 52 | $55.24 | $57.12 | +97.76 | $57.29 | +8.84 | +106.60 | +56.68 | +65.52 |
| 2026-09-14 | `ADBE` | 12 | $252.23 | $261.51 | +111.36 | $265.60 | +49.08 | +160.44 | +232.08 | +281.16 |
| 2026-09-14 | `CNQ` | 59 | $50.07 | $50.76 | +40.71 | $50.32 | -25.96 | +14.75 | +48.38 | +22.42 |
| 2026-09-15 | `BTI` | 52 | $57.29 | $56.46 | -43.16 | $56.52 | +3.12 | -40.04 | +22.36 | +25.48 |
| 2026-09-15 | `ADBE` | 12 | $265.60 | $261.70 | -46.80 | $257.76 | -47.28 | -94.08 | +234.36 | +187.08 |
| 2026-09-15 | `CNQ` | 59 | $50.32 | $50.45 | +7.67 | $51.55 | +64.90 | +72.57 | +30.09 | +94.99 |
| 2026-09-16 | `BTI` | 52 | $56.52 | $56.54 | +1.04 | — | +0.00 | +1.04 | +26.52 | — |
| 2026-09-16 | `ADBE` | 12 | $257.76 | $253.34 | -53.04 | — | +0.00 | -53.04 | +134.04 | — |
| 2026-09-16 | `CNQ` | 59 | $51.55 | $50.91 | -37.76 | — | +0.00 | -37.76 | +57.23 | — |
| 2026-09-16 | `QCOM` | 15 | — | $189.17 | +0.00 | $184.84 | -64.95 | -64.95 | +0.00 | -64.95 |
| 2026-09-16 | `SM` | 75 | — | $39.99 | +0.00 | $38.16 | -137.25 | -137.25 | +0.00 | -137.25 |
| 2026-09-16 | `AMX` | 130 | — | $23.18 | +0.00 | $22.98 | -26.00 | -26.00 | +0.00 | -26.00 |
| 2026-09-17 | `QCOM` | 15 | $184.84 | $190.35 | +82.65 | $188.71 | -24.60 | +58.05 | +17.70 | -6.90 |
| 2026-09-17 | `SM` | 75 | $38.16 | $37.57 | -44.25 | $36.97 | -45.00 | -89.25 | -181.50 | -226.50 |
| 2026-09-17 | `AMX` | 130 | $22.98 | $23.09 | +14.30 | $23.03 | -7.80 | +6.50 | -11.70 | -19.50 |
| 2026-09-18 | `QCOM` | 15 | $188.71 | $191.34 | +39.45 | $177.72 | -204.30 | -164.85 | +32.55 | -171.75 |
| 2026-09-18 | `SM` | 75 | $36.97 | $36.87 | -7.50 | $36.97 | +7.50 | +0.00 | -234.00 | -226.50 |
| 2026-09-18 | `AMX` | 130 | $23.03 | $22.90 | -16.90 | $22.43 | -61.10 | -78.00 | -36.40 | -97.50 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +221.73 | NRG, TLN, VST | — | $283.59 | $10,215.59 | NRG×27, TLN×9, VST×22 |
| 2026-08-17 | +2.25 | $283.59 | NRG×27, TLN×9, VST×22 | $10,320.45 | +104.86 | -303.39 | DVN | — | $190.30 | $10,016.13 | NRG×27, TLN×9, VST×22, DVN×2 |
| 2026-08-18 | -6.20 | $190.30 | NRG×27, TLN×9, VST×22, DVN×2 | $9,915.15 | -100.98 | -558.69 | — | — | $190.30 | $9,356.46 | NRG×27, TLN×9, VST×22, DVN×2 |
| 2026-08-19 | -7.20 | $190.30 | NRG×27, TLN×9, VST×22, DVN×2 | $9,409.42 | +52.96 | -0.06 | — | NRG, TLN, VST | $9,306.73 | $9,403.11 | DVN×2 |
| 2026-08-20 | +1.12 | $9,306.73 | DVN×2 | $9,404.77 | +1.66 | -77.70 | APA | DVN | $1.45 | $9,323.35 | APA×210 |
| 2026-08-21 | +3.25 | $1.45 | APA×210 | $9,350.65 | +27.30 | -237.30 | — | — | $1.45 | $9,113.35 | APA×210 |
| 2026-08-24 | -5.17 | $1.45 | APA×210 | $9,016.75 | -96.60 | +6.30 | — | — | $1.45 | $9,023.05 | APA×210 |
| 2026-08-25 | +1.80 | $1.45 | APA×210 | $8,691.25 | -331.80 | +213.69 | AU, FCX, AMX | APA | $103.83 | $8,895.61 | AU×24, FCX×37, AMX×121 |
| 2026-08-26 | +2.02 | $103.83 | AU×24, FCX×37, AMX×121 | $8,788.36 | -107.25 | -68.87 | — | — | $103.83 | $8,719.49 | AU×24, FCX×37, AMX×121 |
| 2026-08-27 | — | $103.83 | AU×24, FCX×37, AMX×121 | $8,714.55 | -4.94 | -24.08 | — | — | $103.83 | $8,690.47 | AU×24, FCX×37, AMX×121 |
| 2026-08-28 | +0.75 | $103.83 | AU×24, FCX×37, AMX×121 | $8,731.92 | +41.45 | -164.93 | KEYS, CIEN, MPWR, DDOG, ADSK, RRC | AU, FCX, AMX | $741.99 | $8,548.27 | KEYS×4, CIEN×3, MPWR×1, DDOG×6, ADSK×5, RRC×34 |
| 2026-08-31 | -5.85 | $741.99 | KEYS×4, CIEN×3, MPWR×1, DDOG×6, ADSK×5, RRC×34 | $8,549.51 | +1.24 | +19.22 | — | — | $741.99 | $8,568.73 | KEYS×4, CIEN×3, MPWR×1, DDOG×6, ADSK×5, RRC×34 |
| 2026-09-01 | -6.30 | $741.99 | KEYS×4, CIEN×3, MPWR×1, DDOG×6, ADSK×5, RRC×34 | $8,494.29 | -74.44 | -145.18 | — | — | $741.99 | $8,349.11 | KEYS×4, CIEN×3, MPWR×1, DDOG×6, ADSK×5, RRC×34 |
| 2026-09-02 | -3.83 | $741.99 | KEYS×4, CIEN×3, MPWR×1, DDOG×6, ADSK×5, RRC×34 | $8,292.48 | -56.63 | +0.00 | — | KEYS, CIEN, MPWR, DDOG, ADSK, RRC | $8,280.26 | $8,280.26 | — |
| 2026-09-03 | -0.90 | $8,280.26 | — | $8,280.26 | -0.00 | +256.39 | AVGO, DELL, HPE, CIEN | — | $748.94 | $8,528.52 | AVGO×5, DELL×4, HPE×43, CIEN×5 |
| 2026-09-04 | +2.25 | $748.94 | AVGO×5, DELL×4, HPE×43, CIEN×5 | $8,526.46 | -2.06 | -45.25 | AMX, MSTR | — | $424.11 | $8,477.97 | AVGO×5, DELL×4, HPE×43, CIEN×5, AMX×8, MSTR×1 |
| 2026-09-08 | -11.47 | $424.11 | AVGO×5, DELL×4, HPE×43, CIEN×5, AMX×8, MSTR×1 | $8,535.50 | +57.53 | +303.19 | — | — | $424.11 | $8,838.69 | AVGO×5, DELL×4, HPE×43, CIEN×5, AMX×8, MSTR×1 |
| 2026-09-09 | -13.95 | $424.11 | AVGO×5, DELL×4, HPE×43, CIEN×5, AMX×8, MSTR×1 | $8,891.92 | +53.23 | -7.36 | — | AVGO, DELL, HPE, CIEN | $8,558.82 | $8,876.32 | AMX×8, MSTR×1 |
| 2026-09-10 | -13.28 | $8,558.82 | AMX×8, MSTR×1 | $8,871.66 | -4.66 | +0.00 | — | AMX, MSTR | $8,868.46 | $8,868.46 | — |
| 2026-09-11 | +0.50 | $8,868.46 | — | $8,868.46 | +0.00 | +87.31 | BTI, ADBE, CNQ | — | $96.06 | $8,949.43 | BTI×52, ADBE×12, CNQ×59 |
| 2026-09-14 | -11.00 | $96.06 | BTI×52, ADBE×12, CNQ×59 | $9,199.26 | +249.83 | +31.96 | — | — | $96.06 | $9,231.22 | BTI×52, ADBE×12, CNQ×59 |
| 2026-09-15 | -3.84 | $96.06 | BTI×52, ADBE×12, CNQ×59 | $9,148.93 | -82.29 | +20.74 | — | — | $96.06 | $9,169.67 | BTI×52, ADBE×12, CNQ×59 |
| 2026-09-16 | +5.30 | $96.06 | BTI×52, ADBE×12, CNQ×59 | $9,079.91 | -89.76 | -228.20 | QCOM, SM, AMX | BTI, ADBE, CNQ | $216.64 | $8,838.64 | QCOM×15, SM×75, AMX×130 |
| 2026-09-17 | +7.38 | $216.64 | QCOM×15, SM×75, AMX×130 | $8,891.34 | +52.70 | -77.40 | — | — | $216.64 | $8,813.94 | QCOM×15, SM×75, AMX×130 |
| 2026-09-18 | +4.86 | $216.64 | QCOM×15, SM×75, AMX×130 | $8,828.99 | +15.05 | -257.90 | — | — | $216.64 | $8,571.09 | QCOM×15, SM×75, AMX×130 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 27 | $120.00 | $2.07 | — | $6,757.93 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+0.6; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 9 | $359.83 | $2.02 | — | $3,517.44 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+5.9; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 22 | $146.90 | $2.06 | — | $283.59 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+3.6; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $283.59 | ▲ close $10,215.59 vs 09:30 $10,000.00 (session +221.73) | 16:00 close · cash $283.59 · equity $10,215.59 vs 09:30 $10,000.00 (+215.59; session marks +221.73) · 3 name(s) marked open→close (per-name table). NRG×27 09:30 $120.00 → close $126.24 +168.48; TLN×9 09:30 $359.83 → close $362.74 +26.19; VST×22 09:30 $146.90 → close $148.13 +27.06 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $283.59 | ▲ 09:30 equity $10,320.45 vs yday $10,215.59 (+104.86) | 09:30 open · cash $283.59 (unchanged overnight, no fees) · equity $10,320.45 vs prior close $10,215.59 (+104.86) · 3 name(s) re-marked at the open (per-name table). NRG×27 yday $126.24 → 09:30 $127.40 +31.32; TLN×9 yday $362.74 → 09:30 $367.88 +46.26; VST×22 yday $148.13 → 09:30 $149.37 +27.28 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 2 | $46.18 | $0.93 | — | $190.30 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $94.53 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $190.30 | ▼ close $10,016.13 vs 09:30 $10,320.45 (session -303.39) | 16:00 close · cash $190.30 · equity $10,016.13 vs 09:30 $10,320.45 (-304.32; session marks -303.39) · 4 name(s) marked open→close (per-name table). NRG×27 09:30 $127.40 → close $122.37 -135.81; TLN×9 09:30 $367.88 → close $356.92 -98.64; VST×22 09:30 $149.37 → close $146.11 -71.72; DVN×2 09:30 $46.18 → close $47.57 +2.78 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.30 | ▼ 09:30 equity $9,915.15 vs yday $10,016.13 (-100.98) | 09:30 open · cash $190.30 (unchanged overnight, no fees) · equity $9,915.15 vs prior close $10,016.13 (-100.98) · 4 name(s) re-marked at the open (per-name table). NRG×27 yday $122.37 → 09:30 $121.92 -12.15; TLN×9 yday $356.92 → 09:30 $350.89 -54.27; VST×22 yday $146.11 → 09:30 $144.50 -35.42; DVN×2 yday $47.57 → 09:30 $48.00 +0.86 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $190.30 | ▼ close $9,356.46 vs 09:30 $9,915.15 (session -558.69) | 16:00 close · cash $190.30 · equity $9,356.46 vs 09:30 $9,915.15 (-558.69; session marks -558.69) · 4 name(s) marked open→close (per-name table). NRG×27 09:30 $121.92 → close $115.56 -171.72; TLN×9 09:30 $350.89 → close $317.66 -299.07; VST×22 09:30 $144.50 → close $140.52 -87.56; DVN×2 09:30 $48.00 → close $47.83 -0.34 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.30 | ▲ 09:30 equity $9,409.42 vs yday $9,356.46 (+52.96) | 09:30 open · cash $190.30 (unchanged overnight, no fees) · equity $9,409.42 vs prior close $9,356.46 (+52.96) · 4 name(s) re-marked at the open (per-name table). NRG×27 yday $115.56 → 09:30 $116.20 +17.28; TLN×9 yday $317.66 → 09:30 $321.00 +30.06; VST×22 yday $140.52 → 09:30 $140.74 +4.84; DVN×2 yday $47.83 → 09:30 $48.22 +0.78 | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 27 | $116.20 | $2.11 | $-106.78 | $3,325.59 | ▼ -106.78 after sell → book $9,407.31; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `TLN` | 9 | $321.00 | $2.05 | $-353.54 | $6,212.54 | ▼ -353.54 after sell → book $9,405.26; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `VST` | 22 | $140.74 | $2.09 | $-139.67 | $9,306.73 | ▼ -139.67 after sell → book $9,403.17; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,306.73 | ▼ close $9,403.11 vs 09:30 $9,409.42 (session -0.06) | 16:00 close · cash $9,306.73 · equity $9,403.11 vs 09:30 $9,409.42 (-6.31; session marks -0.06) · 1 name(s) marked open→close (per-name table). DVN×2 09:30 $48.22 → close $48.19 -0.06 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,306.73 | ▲ 09:30 equity $9,404.77 vs yday $9,403.11 (+1.66) | 09:30 open · cash $9,306.73 (unchanged overnight, no fees) · equity $9,404.77 vs prior close $9,403.11 (+1.66) · 1 name(s) re-marked at the open (per-name table). DVN×2 yday $48.19 → 09:30 $49.02 +1.66 | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 2 | $49.02 | $1.01 | $+3.74 | $9,403.76 | ▲ +3.74 after sell → book $9,403.76; vs 09:30 mark -1.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 210 | $44.76 | $2.71 | — | $1.45 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $9403.76 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.45 | ▼ close $9,323.35 vs 09:30 $9,404.77 (session -77.70) | 16:00 close · cash $1.45 · equity $9,323.35 vs 09:30 $9,404.77 (-81.42; session marks -77.70) · 1 name(s) marked open→close (per-name table). APA×210 09:30 $44.76 → close $44.39 -77.70 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.45 | ▲ 09:30 equity $9,350.65 vs yday $9,323.35 (+27.30) | 09:30 open · cash $1.45 (unchanged overnight, no fees) · equity $9,350.65 vs prior close $9,323.35 (+27.30) · 1 name(s) re-marked at the open (per-name table). APA×210 yday $44.39 → 09:30 $44.52 +27.30 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.45 | ▼ close $9,113.35 vs 09:30 $9,350.65 (session -237.30) | 16:00 close · cash $1.45 · equity $9,113.35 vs 09:30 $9,350.65 (-237.30; session marks -237.30) · 1 name(s) marked open→close (per-name table). APA×210 09:30 $44.52 → close $43.39 -237.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.45 | ▼ 09:30 equity $9,016.75 vs yday $9,113.35 (-96.60) | 09:30 open · cash $1.45 (unchanged overnight, no fees) · equity $9,016.75 vs prior close $9,113.35 (-96.60) · 1 name(s) re-marked at the open (per-name table). APA×210 yday $43.39 → 09:30 $42.93 -96.60 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.45 | ▲ close $9,023.05 vs 09:30 $9,016.75 (session +6.30) | 16:00 close · cash $1.45 · equity $9,023.05 vs 09:30 $9,016.75 (+6.30; session marks +6.30) · 1 name(s) marked open→close (per-name table). APA×210 09:30 $42.93 → close $42.96 +6.30 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.45 | ▼ 09:30 equity $8,691.25 vs yday $9,023.05 (-331.80) | 09:30 open · cash $1.45 (unchanged overnight, no fees) · equity $8,691.25 vs prior close $9,023.05 (-331.80) · 1 name(s) re-marked at the open (per-name table). APA×210 yday $42.96 → 09:30 $41.38 -331.80 | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 210 | $41.38 | $2.81 | $-715.32 | $8,688.44 | ▼ -715.32 after sell → book $8,688.44; vs 09:30 mark -2.81 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 24 | $118.52 | $2.06 | — | $5,841.90 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $2896.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 37 | $77.13 | $2.10 | — | $2,985.99 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $2896.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 121 | $23.80 | $2.35 | — | $103.83 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list oppset; 🔵; ret5=+0.5; leftover $2896.15 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.83 | ▲ close $8,895.61 vs 09:30 $8,691.25 (session +213.69) | 16:00 close · cash $103.83 · equity $8,895.61 vs 09:30 $8,691.25 (+204.36; session marks +213.69) · 3 name(s) marked open→close (per-name table). AU×24 09:30 $118.52 → close $123.39 +116.88; FCX×37 09:30 $77.13 → close $79.91 +102.86; AMX×121 09:30 $23.80 → close $23.75 -6.05 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.83 | ▼ 09:30 equity $8,788.36 vs yday $8,895.61 (-107.25) | 09:30 open · cash $103.83 (unchanged overnight, no fees) · equity $8,788.36 vs prior close $8,895.61 (-107.25) · 3 name(s) re-marked at the open (per-name table). AU×24 yday $123.39 → 09:30 $119.80 -86.16; FCX×37 yday $79.91 → 09:30 $79.34 -21.09; AMX×121 yday $23.75 → 09:30 $23.75 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.83 | ▼ close $8,719.49 vs 09:30 $8,788.36 (session -68.87) | 16:00 close · cash $103.83 · equity $8,719.49 vs 09:30 $8,788.36 (-68.87; session marks -68.87) · 3 name(s) marked open→close (per-name table). AU×24 09:30 $119.80 → close $118.11 -40.56; FCX×37 09:30 $79.34 → close $79.00 -12.58; AMX×121 09:30 $23.75 → close $23.62 -15.73 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.83 | ▼ 09:30 equity $8,714.55 vs yday $8,719.49 (-4.94) | 09:30 open · cash $103.83 (unchanged overnight, no fees) · equity $8,714.55 vs prior close $8,719.49 (-4.94) · 3 name(s) re-marked at the open (per-name table). AU×24 yday $118.11 → 09:30 $117.41 -16.80; FCX×37 yday $79.00 → 09:30 $78.83 -6.29; AMX×121 yday $23.62 → 09:30 $23.77 +18.15 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.83 | ▼ close $8,690.47 vs 09:30 $8,714.55 (session -24.08) | 16:00 close · cash $103.83 · equity $8,690.47 vs 09:30 $8,714.55 (-24.08; session marks -24.08) · 3 name(s) marked open→close (per-name table). AU×24 09:30 $117.41 → close $118.40 +23.76; FCX×37 09:30 $78.83 → close $78.42 -15.17; AMX×121 09:30 $23.77 → close $23.50 -32.67 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.83 | ▲ 09:30 equity $8,731.92 vs yday $8,690.47 (+41.45) | 09:30 open · cash $103.83 (unchanged overnight, no fees) · equity $8,731.92 vs prior close $8,690.47 (+41.45) · 3 name(s) re-marked at the open (per-name table). AU×24 yday $118.40 → 09:30 $119.19 +18.96; FCX×37 yday $78.42 → 09:30 $78.57 +5.55; AMX×121 yday $23.50 → 09:30 $23.64 +16.94 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 24 | $119.19 | $2.09 | $+11.92 | $2,962.30 | ▲ +11.92 after sell → book $8,729.83; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 37 | $78.57 | $2.13 | $+49.04 | $5,867.26 | ▲ +49.04 after sell → book $8,727.70; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AMX` | 121 | $23.64 | $2.40 | $-24.11 | $8,725.30 | ▼ -24.11 after sell → book $8,725.30; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $7,425.66 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1454.22 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,222.40 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1454.22 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,914.38 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1454.22 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 6 | $240.22 | $2.01 | — | $3,471.05 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1454.22 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $2,163.24 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=+7.8; leftover $1454.22 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 34 | $41.74 | $2.09 | — | $741.99 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; ret5=+2.4; leftover $1454.22 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $741.99 | ▼ close $8,548.27 vs 09:30 $8,731.92 (session -164.93) | 16:00 close · cash $741.99 · equity $8,548.27 vs 09:30 $8,731.92 (-183.65; session marks -164.93) · 6 name(s) marked open→close (per-name table). KEYS×4 09:30 $324.41 → close $319.97 -17.76; CIEN×3 09:30 $400.42 → close $378.44 -65.94; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×6 09:30 $240.22 → close $236.98 -19.44; ADSK×5 09:30 $261.16 → close $260.66 -2.50; RRC×34 09:30 $41.74 → close $41.46 -9.52 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $741.99 | ▲ 09:30 equity $8,549.51 vs yday $8,548.27 (+1.24) | 09:30 open · cash $741.99 (unchanged overnight, no fees) · equity $8,549.51 vs prior close $8,548.27 (+1.24) · 6 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×6 yday $236.98 → 09:30 $233.97 -18.09; ADSK×5 yday $260.66 → 09:30 $257.71 -14.75; RRC×34 yday $41.46 → 09:30 $42.00 +18.36 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $741.99 | ▲ close $8,568.73 vs 09:30 $8,549.51 (session +19.22) | 16:00 close · cash $741.99 · equity $8,568.73 vs 09:30 $8,549.51 (+19.22; session marks +19.22) · 6 name(s) marked open→close (per-name table). KEYS×4 09:30 $322.49 → close $322.70 +0.84; CIEN×3 09:30 $378.44 → close $382.80 +13.08; MPWR×1 09:30 $1261.90 → close $1267.77 +5.87; DDOG×6 09:30 $233.97 → close $237.04 +18.45; ADSK×5 09:30 $257.71 → close $258.53 +4.10; RRC×34 09:30 $42.00 → close $41.32 -23.12 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $741.99 | ▼ 09:30 equity $8,494.29 vs yday $8,568.73 (-74.44) | 09:30 open · cash $741.99 (unchanged overnight, no fees) · equity $8,494.29 vs prior close $8,568.73 (-74.44) · 6 name(s) re-marked at the open (per-name table). KEYS×4 yday $322.70 → 09:30 $321.47 -4.92; CIEN×3 yday $382.80 → 09:30 $376.89 -17.73; MPWR×1 yday $1267.77 → 09:30 $1245.11 -22.66; DDOG×6 yday $237.04 → 09:30 $232.88 -24.96; ADSK×5 yday $258.53 → 09:30 $253.48 -25.25; RRC×34 yday $41.32 → 09:30 $41.94 +21.08 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $741.99 | ▼ close $8,349.11 vs 09:30 $8,494.29 (session -145.18) | 16:00 close · cash $741.99 · equity $8,349.11 vs 09:30 $8,494.29 (-145.18; session marks -145.18) · 6 name(s) marked open→close (per-name table). KEYS×4 09:30 $321.47 → close $319.27 -8.80; CIEN×3 09:30 $376.89 → close $360.33 -49.68; MPWR×1 09:30 $1245.11 → close $1225.96 -19.15; DDOG×6 09:30 $232.88 → close $223.84 -54.24; ADSK×5 09:30 $253.48 → close $247.69 -28.95; RRC×34 09:30 $41.94 → close $42.40 +15.64 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $741.99 | ▼ 09:30 equity $8,292.48 vs yday $8,349.11 (-56.63) | 09:30 open · cash $741.99 (unchanged overnight, no fees) · equity $8,292.48 vs prior close $8,349.11 (-56.63) · 6 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.27 → 09:30 $318.04 -4.92; CIEN×3 yday $360.33 → 09:30 $357.25 -9.24; MPWR×1 yday $1225.96 → 09:30 $1224.92 -1.04; DDOG×6 yday $223.84 → 09:30 $219.46 -26.28; ADSK×5 yday $247.69 → 09:30 $246.70 -4.95; RRC×34 yday $42.40 → 09:30 $42.10 -10.20 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $318.04 | $2.02 | $-29.50 | $2,012.13 | ▼ -29.50 after sell → book $8,290.46; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 3 | $357.25 | $2.02 | $-133.53 | $3,081.86 | ▼ -133.53 after sell → book $8,288.44; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1224.92 | $2.01 | $-85.12 | $4,304.77 | ▼ -85.12 after sell → book $8,286.43; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 6 | $219.46 | $2.03 | $-128.60 | $5,619.50 | ▼ -128.60 after sell → book $8,284.40; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 5 | $246.70 | $2.02 | $-76.33 | $6,850.97 | ▼ -76.33 after sell → book $8,282.37; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 34 | $42.10 | $2.11 | $+8.03 | $8,280.26 | ▲ +8.03 after sell → book $8,280.26; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,280.26 | ▲ close $8,280.26 vs 09:30 $8,292.48 (session +0.00) | 16:00 close · cash $8,280.26 · no lots left · equity $8,280.26. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,280.26 | ▲ 09:30 equity $8,280.26 vs yday $8,280.26 (-0.00) | 09:30 open · cash $8,280.26 · no holdings · equity $8,280.26 vs prior close $8,280.26 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 5 | $351.74 | $2.00 | — | $6,519.55 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $2070.06 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 4 | $486.31 | $2.00 | — | $4,572.31 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $2070.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 43 | $47.60 | $2.12 | — | $2,523.39 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react,oppset; 🔵; ret5=-6.2; leftover $2070.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 5 | $354.49 | $2.00 | — | $748.94 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=-12.3; leftover $2070.06 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $748.94 | ▲ close $8,528.52 vs 09:30 $8,280.26 (session +256.39) | 16:00 close · cash $748.94 · equity $8,528.52 vs 09:30 $8,280.26 (+248.26; session marks +256.39) · 4 name(s) marked open→close (per-name table). AVGO×5 09:30 $351.74 → close $357.16 +27.10; DELL×4 09:30 $486.31 → close $516.39 +120.32; HPE×43 09:30 $47.60 → close $54.44 +294.12; CIEN×5 09:30 $354.49 → close $317.46 -185.15 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $748.94 | ▼ 09:30 equity $8,526.46 vs yday $8,528.52 (-2.06) | 09:30 open · cash $748.94 (unchanged overnight, no fees) · equity $8,526.46 vs prior close $8,528.52 (-2.06) · 4 name(s) re-marked at the open (per-name table). AVGO×5 yday $357.16 → 09:30 $359.70 +12.70; DELL×4 yday $516.39 → 09:30 $513.78 -10.44; HPE×43 yday $54.44 → 09:30 $53.85 -25.37; CIEN×5 yday $317.46 → 09:30 $321.67 +21.05 | — |
| 2026-09-04 09:30 ET | **BUY** | `AMX` | 8 | $23.03 | $1.87 | — | $562.83 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list oppset; 🔵; ret5=-1.4; leftover $187.23 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 1 | $137.35 | $1.38 | — | $424.11 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+5.4; leftover $187.23 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $424.11 | ▼ close $8,477.97 vs 09:30 $8,526.46 (session -45.25) | 16:00 close · cash $424.11 · equity $8,477.97 vs 09:30 $8,526.46 (-48.49; session marks -45.25) · 6 name(s) marked open→close (per-name table). AVGO×5 09:30 $359.70 → close $357.90 -9.00; DELL×4 09:30 $513.78 → close $524.14 +41.44; HPE×43 09:30 $53.85 → close $52.00 -79.55; CIEN×5 09:30 $321.67 → close $321.00 -3.35; AMX×8 09:30 $23.03 → close $23.00 -0.24; MSTR×1 09:30 $137.35 → close $142.80 +5.45 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $424.11 | ▲ 09:30 equity $8,535.50 vs yday $8,477.97 (+57.53) | 09:30 open · cash $424.11 (unchanged overnight, no fees) · equity $8,535.50 vs prior close $8,477.97 (+57.53) · 6 name(s) re-marked at the open (per-name table). AVGO×5 yday $357.90 → 09:30 $363.68 +28.90; DELL×4 yday $524.14 → 09:30 $521.15 -11.96; HPE×43 yday $52.00 → 09:30 $52.29 +12.47; CIEN×5 yday $321.00 → 09:30 $327.42 +32.10; AMX×8 yday $23.00 → 09:30 $23.15 +1.20; MSTR×1 yday $142.80 → 09:30 $137.62 -5.18 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $424.11 | ▲ close $8,838.69 vs 09:30 $8,535.50 (session +303.19) | 16:00 close · cash $424.11 · equity $8,838.69 vs 09:30 $8,535.50 (+303.19; session marks +303.19) · 6 name(s) marked open→close (per-name table). AVGO×5 09:30 $363.68 → close $368.56 +24.40; DELL×4 09:30 $521.15 → close $533.88 +50.92; HPE×43 09:30 $52.29 → close $56.03 +160.82; CIEN×5 09:30 $327.42 → close $341.29 +69.35; AMX×8 09:30 $23.15 → close $23.00 -1.20; MSTR×1 09:30 $137.62 → close $136.52 -1.10 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $424.11 | ▲ 09:30 equity $8,891.92 vs yday $8,838.69 (+53.23) | 09:30 open · cash $424.11 (unchanged overnight, no fees) · equity $8,891.92 vs prior close $8,838.69 (+53.23) · 6 name(s) re-marked at the open (per-name table). AVGO×5 yday $368.56 → 09:30 $366.23 -11.65; DELL×4 yday $533.88 → 09:30 $538.47 +18.36; HPE×43 yday $56.03 → 09:30 $56.94 +39.13; CIEN×5 yday $341.29 → 09:30 $341.90 +3.05; AMX×8 yday $23.00 → 09:30 $22.88 -0.96; MSTR×1 yday $136.52 → 09:30 $141.82 +5.30 | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 5 | $366.23 | $2.03 | $+68.42 | $2,253.23 | ▲ +68.42 after sell → book $8,889.89; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 4 | $538.47 | $2.03 | $+204.61 | $4,405.08 | ▲ +204.61 after sell → book $8,887.86; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 43 | $56.94 | $2.15 | $+397.35 | $6,851.35 | ▲ +397.35 after sell → book $8,885.71; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 5 | $341.90 | $2.03 | $-66.98 | $8,558.82 | ▼ -66.98 after sell → book $8,883.68; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,558.82 | ▼ close $8,876.32 vs 09:30 $8,891.92 (session -7.36) | 16:00 close · cash $8,558.82 · equity $8,876.32 vs 09:30 $8,891.92 (-15.60; session marks -7.36) · 2 name(s) marked open→close (per-name table). AMX×8 09:30 $22.88 → close $23.10 +1.76; MSTR×1 09:30 $141.82 → close $132.70 -9.12 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,558.82 | ▼ 09:30 equity $8,871.66 vs yday $8,876.32 (-4.66) | 09:30 open · cash $8,558.82 (unchanged overnight, no fees) · equity $8,871.66 vs prior close $8,876.32 (-4.66) · 2 name(s) re-marked at the open (per-name table). AMX×8 yday $23.10 → 09:30 $23.05 -0.40; MSTR×1 yday $132.70 → 09:30 $128.44 -4.26 | — |
| 2026-09-10 09:30 ET | **SELL** | `AMX` | 8 | $23.05 | $1.89 | $-3.59 | $8,741.33 | ▼ -3.59 after sell → book $8,869.77; vs 09:30 mark -1.89 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MSTR` | 1 | $128.44 | $1.31 | $-11.59 | $8,868.46 | ▼ -11.59 after sell → book $8,868.46; vs 09:30 mark -1.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,868.46 | ▲ close $8,868.46 vs 09:30 $8,871.66 (session +0.00) | 16:00 close · cash $8,868.46 · no lots left · equity $8,868.46. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,868.46 | ▲ 09:30 equity $8,868.46 vs yday $8,868.46 (+0.00) | 09:30 open · cash $8,868.46 · no holdings · equity $8,868.46 vs prior close $8,868.46 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 52 | $56.03 | $2.15 | — | $5,952.76 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list oppset; 🔵; ret5=-0.8; leftover $2956.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 12 | $242.17 | $2.03 | — | $3,044.69 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=-11.1; leftover $2956.15 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CNQ` | 59 | $49.94 | $2.17 | — | $96.06 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list oppset; ret5=+1.7; leftover $2956.15 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.06 | ▲ close $8,949.43 vs 09:30 $8,868.46 (session +87.31) | 16:00 close · cash $96.06 · equity $8,949.43 vs 09:30 $8,868.46 (+80.97; session marks +87.31) · 3 name(s) marked open→close (per-name table). BTI×52 09:30 $56.03 → close $55.24 -41.08; ADBE×12 09:30 $242.17 → close $252.23 +120.72; CNQ×59 09:30 $49.94 → close $50.07 +7.67 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.06 | ▲ 09:30 equity $9,199.26 vs yday $8,949.43 (+249.83) | 09:30 open · cash $96.06 (unchanged overnight, no fees) · equity $9,199.26 vs prior close $8,949.43 (+249.83) · 3 name(s) re-marked at the open (per-name table). BTI×52 yday $55.24 → 09:30 $57.12 +97.76; ADBE×12 yday $252.23 → 09:30 $261.51 +111.36; CNQ×59 yday $50.07 → 09:30 $50.76 +40.71 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.06 | ▲ close $9,231.22 vs 09:30 $9,199.26 (session +31.96) | 16:00 close · cash $96.06 · equity $9,231.22 vs 09:30 $9,199.26 (+31.96; session marks +31.96) · 3 name(s) marked open→close (per-name table). BTI×52 09:30 $57.12 → close $57.29 +8.84; ADBE×12 09:30 $261.51 → close $265.60 +49.08; CNQ×59 09:30 $50.76 → close $50.32 -25.96 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.06 | ▼ 09:30 equity $9,148.93 vs yday $9,231.22 (-82.29) | 09:30 open · cash $96.06 (unchanged overnight, no fees) · equity $9,148.93 vs prior close $9,231.22 (-82.29) · 3 name(s) re-marked at the open (per-name table). BTI×52 yday $57.29 → 09:30 $56.46 -43.16; ADBE×12 yday $265.60 → 09:30 $261.70 -46.80; CNQ×59 yday $50.32 → 09:30 $50.45 +7.67 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.06 | ▲ close $9,169.67 vs 09:30 $9,148.93 (session +20.74) | 16:00 close · cash $96.06 · equity $9,169.67 vs 09:30 $9,148.93 (+20.74; session marks +20.74) · 3 name(s) marked open→close (per-name table). BTI×52 09:30 $56.46 → close $56.52 +3.12; ADBE×12 09:30 $261.70 → close $257.76 -47.28; CNQ×59 09:30 $50.45 → close $51.55 +64.90 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.06 | ▼ 09:30 equity $9,079.91 vs yday $9,169.67 (-89.76) | 09:30 open · cash $96.06 (unchanged overnight, no fees) · equity $9,079.91 vs prior close $9,169.67 (-89.76) · 3 name(s) re-marked at the open (per-name table). BTI×52 yday $56.52 → 09:30 $56.54 +1.04; ADBE×12 yday $257.76 → 09:30 $253.34 -53.04; CNQ×59 yday $51.55 → 09:30 $50.91 -37.76 | — |
| 2026-09-16 09:30 ET | **SELL** | `BTI` | 52 | $56.54 | $2.18 | $+22.19 | $3,033.97 | ▲ +22.19 after sell → book $9,077.74; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 12 | $253.34 | $2.06 | $+129.95 | $6,071.99 | ▲ +129.95 after sell → book $9,075.68; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CNQ` | 59 | $50.91 | $2.20 | $+52.86 | $9,073.47 | ▲ +52.86 after sell → book $9,073.47; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 15 | $189.17 | $2.04 | — | $6,233.89 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $3024.49 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 75 | $39.99 | $2.21 | — | $3,232.42 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $3024.49 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AMX` | 130 | $23.18 | $2.38 | — | $216.64 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list oppset; 🔵; ret5=-0.2; leftover $3024.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $216.64 | ▼ close $8,838.64 vs 09:30 $9,079.91 (session -228.20) | 16:00 close · cash $216.64 · equity $8,838.64 vs 09:30 $9,079.91 (-241.27; session marks -228.20) · 3 name(s) marked open→close (per-name table). QCOM×15 09:30 $189.17 → close $184.84 -64.95; SM×75 09:30 $39.99 → close $38.16 -137.25; AMX×130 09:30 $23.18 → close $22.98 -26.00 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $216.64 | ▲ 09:30 equity $8,891.34 vs yday $8,838.64 (+52.70) | 09:30 open · cash $216.64 (unchanged overnight, no fees) · equity $8,891.34 vs prior close $8,838.64 (+52.70) · 3 name(s) re-marked at the open (per-name table). QCOM×15 yday $184.84 → 09:30 $190.35 +82.65; SM×75 yday $38.16 → 09:30 $37.57 -44.25; AMX×130 yday $22.98 → 09:30 $23.09 +14.30 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $216.64 | ▼ close $8,813.94 vs 09:30 $8,891.34 (session -77.40) | 16:00 close · cash $216.64 · equity $8,813.94 vs 09:30 $8,891.34 (-77.40; session marks -77.40) · 3 name(s) marked open→close (per-name table). QCOM×15 09:30 $190.35 → close $188.71 -24.60; SM×75 09:30 $37.57 → close $36.97 -45.00; AMX×130 09:30 $23.09 → close $23.03 -7.80 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $216.64 | ▲ 09:30 equity $8,828.99 vs yday $8,813.94 (+15.05) | 09:30 open · cash $216.64 (unchanged overnight, no fees) · equity $8,828.99 vs prior close $8,813.94 (+15.05) · 3 name(s) re-marked at the open (per-name table). QCOM×15 yday $188.71 → 09:30 $191.34 +39.45; SM×75 yday $36.97 → 09:30 $36.87 -7.50; AMX×130 yday $23.03 → 09:30 $22.90 -16.90 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $216.64 | ▼ close $8,571.09 vs 09:30 $8,828.99 (session -257.90) | 16:00 close · cash $216.64 · equity $8,571.09 vs 09:30 $8,828.99 (-257.90; session marks -257.90) · 3 name(s) marked open→close (per-name table). QCOM×15 09:30 $191.34 → close $177.72 -204.30; SM×75 09:30 $36.87 → close $36.97 +7.50; AMX×130 09:30 $22.90 → close $22.43 -61.10 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `EOG` | cash | leftover split 94.53 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 94.53 < 1 share @ 202.70 |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 0.48 < 1 share @ 119.43 |
| 2026-08-21 | `MFC` | cash | leftover split 0.48 < 1 share @ 42.48 |
| 2026-08-21 | `DE` | cash | leftover split 0.48 < 1 share @ 623.26 |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AMX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 103.83 < 1 share @ 267.02 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 12.98 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 12.98 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 12.98 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 12.98 < 1 share @ 118.77 |
| 2026-08-27 | `LRCX` | cash | leftover split 12.98 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 12.98 < 1 share @ 222.86 |
| 2026-08-27 | `AXTI` | cash | leftover split 12.98 < 1 share @ 70.30 |
| 2026-08-27 | `RRC` | cash | leftover split 12.98 < 1 share @ 41.44 |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DDOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DDOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 187.23 < 1 share @ 263.36 |
| 2026-09-04 | `BE` | cash | leftover split 187.23 < 1 share @ 236.82 |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FNV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BTI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CNQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QCOM` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `BTI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CNQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `LITE` | cash | leftover split 108.32 < 1 share @ 934.88 |
| 2026-09-17 | `FANG` | cash | leftover split 108.32 < 1 share @ 191.08 |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AMX` | min_hold | dropped but min-hold 2/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `QCOM` | 15 | 2026-09-16 @ $189.17 | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $3024.49 |
| `SM` | 75 | 2026-09-16 @ $39.99 | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $3024.49 |
| `AMX` | 130 | 2026-09-16 @ $23.18 | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list oppset; 🔵; ret5=-0.2; leftover $3024.49 |
