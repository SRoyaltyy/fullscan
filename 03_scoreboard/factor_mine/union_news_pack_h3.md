# Factor mine action — `union_news_pack_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · morning packet news🟢 only (not the merged box)

Cash book **-13.06%** ($8,694) · signal-only (no cash/fees) was -16.54%. Starts YES **11/30**. Fills 45 · skips 85 · realized $-1342.35.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $194.00.

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
| 2026-08-25 | `AU` | 36 | — | $118.52 | +0.00 | $123.39 | +175.32 | +175.32 | +0.00 | +175.32 |
| 2026-08-25 | `FCX` | 56 | — | $77.13 | +0.00 | $79.91 | +155.68 | +155.68 | +0.00 | +155.68 |
| 2026-08-26 | `AU` | 36 | $123.39 | $119.80 | -129.24 | $118.11 | -60.84 | -190.08 | +46.08 | -14.76 |
| 2026-08-26 | `FCX` | 56 | $79.91 | $79.34 | -31.92 | $79.00 | -19.04 | -50.96 | +123.76 | +104.72 |
| 2026-08-27 | `AU` | 36 | $118.11 | $117.41 | -25.20 | $118.40 | +35.64 | +10.44 | -39.96 | -4.32 |
| 2026-08-27 | `FCX` | 56 | $79.00 | $78.83 | -9.52 | $78.42 | -22.96 | -32.48 | +95.20 | +72.24 |
| 2026-08-28 | `AU` | 36 | $118.40 | $119.19 | +28.44 | — | +0.00 | +28.44 | +24.12 | — |
| 2026-08-28 | `FCX` | 56 | $78.42 | $78.57 | +8.40 | — | +0.00 | +8.40 | +80.64 | — |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 6 | — | $240.22 | +0.00 | $236.98 | -19.44 | -19.44 | +0.00 | -19.44 |
| 2026-08-28 | `ADSK` | 5 | — | $261.16 | +0.00 | $260.66 | -2.50 | -2.50 | +0.00 | -2.50 |
| 2026-08-28 | `RRC` | 35 | — | $41.74 | +0.00 | $41.46 | -9.80 | -9.80 | +0.00 | -9.80 |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | $322.70 | +0.84 | +10.92 | -7.68 | -6.84 |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | $382.80 | +13.08 | +13.08 | -65.94 | -52.86 |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | $1267.77 | +5.87 | +11.51 | -44.13 | -38.26 |
| 2026-08-31 | `DDOG` | 6 | $236.98 | $233.97 | -18.09 | $237.04 | +18.45 | +0.36 | -37.53 | -19.08 |
| 2026-08-31 | `ADSK` | 5 | $260.66 | $257.71 | -14.75 | $258.53 | +4.10 | -10.65 | -17.25 | -13.15 |
| 2026-08-31 | `RRC` | 35 | $41.46 | $42.00 | +18.90 | $41.32 | -23.80 | -4.90 | +9.10 | -14.70 |
| 2026-09-01 | `KEYS` | 4 | $322.70 | $321.47 | -4.92 | $319.27 | -8.80 | -13.72 | -11.76 | -20.56 |
| 2026-09-01 | `CIEN` | 3 | $382.80 | $376.89 | -17.73 | $360.33 | -49.68 | -67.41 | -70.59 | -120.27 |
| 2026-09-01 | `MPWR` | 1 | $1267.77 | $1245.11 | -22.66 | $1225.96 | -19.15 | -41.81 | -60.92 | -80.07 |
| 2026-09-01 | `DDOG` | 6 | $237.04 | $232.88 | -24.96 | $223.84 | -54.24 | -79.20 | -44.04 | -98.28 |
| 2026-09-01 | `ADSK` | 5 | $258.53 | $253.48 | -25.25 | $247.69 | -28.95 | -54.20 | -38.40 | -67.35 |
| 2026-09-01 | `RRC` | 35 | $41.32 | $41.94 | +21.70 | $42.40 | +16.10 | +37.80 | +7.00 | +23.10 |
| 2026-09-02 | `KEYS` | 4 | $319.27 | $318.04 | -4.92 | — | +0.00 | -4.92 | -25.48 | — |
| 2026-09-02 | `CIEN` | 3 | $360.33 | $357.25 | -9.24 | $354.16 | -9.27 | -18.51 | -129.51 | -138.78 |
| 2026-09-02 | `MPWR` | 1 | $1225.96 | $1224.92 | -1.04 | — | +0.00 | -1.04 | -81.11 | — |
| 2026-09-02 | `DDOG` | 6 | $223.84 | $219.46 | -26.28 | — | +0.00 | -26.28 | -124.56 | — |
| 2026-09-02 | `ADSK` | 5 | $247.69 | $246.70 | -4.95 | — | +0.00 | -4.95 | -72.30 | — |
| 2026-09-02 | `RRC` | 35 | $42.40 | $42.10 | -10.50 | — | +0.00 | -10.50 | +12.60 | — |
| 2026-09-03 | `CIEN` | 3 | $354.16 | $354.49 | +0.99 | $317.46 | -111.09 | -110.10 | -137.79 | -248.88 |
| 2026-09-03 | `AVGO` | 6 | — | $351.74 | +0.00 | $357.16 | +32.52 | +32.52 | +0.00 | +32.52 |
| 2026-09-03 | `DELL` | 4 | — | $486.31 | +0.00 | $516.39 | +120.32 | +120.32 | +0.00 | +120.32 |
| 2026-09-03 | `HPE` | 50 | — | $47.60 | +0.00 | $54.44 | +342.00 | +342.00 | +0.00 | +342.00 |
| 2026-09-04 | `CIEN` | 3 | $317.46 | $321.67 | +12.63 | — | +0.00 | +12.63 | -236.25 | — |
| 2026-09-04 | `AVGO` | 6 | $357.16 | $359.70 | +15.24 | $357.90 | -10.80 | +4.44 | +47.76 | +36.96 |
| 2026-09-04 | `DELL` | 4 | $516.39 | $513.78 | -10.44 | $524.14 | +41.44 | +31.00 | +109.88 | +151.32 |
| 2026-09-04 | `HPE` | 50 | $54.44 | $53.85 | -29.50 | $52.00 | -92.50 | -122.00 | +312.50 | +220.00 |
| 2026-09-04 | `CRM` | 2 | — | $263.36 | +0.00 | $259.23 | -8.26 | -8.26 | +0.00 | -8.26 |
| 2026-09-04 | `BE` | 2 | — | $236.82 | +0.00 | $252.87 | +32.10 | +32.10 | +0.00 | +32.10 |
| 2026-09-04 | `MSTR` | 4 | — | $137.35 | +0.00 | $142.80 | +21.80 | +21.80 | +0.00 | +21.80 |
| 2026-09-08 | `AVGO` | 6 | $357.90 | $363.68 | +34.68 | $368.56 | +29.28 | +63.96 | +71.64 | +100.92 |
| 2026-09-08 | `DELL` | 4 | $524.14 | $521.15 | -11.96 | $533.88 | +50.92 | +38.96 | +139.36 | +190.28 |
| 2026-09-08 | `HPE` | 50 | $52.00 | $52.29 | +14.50 | $56.03 | +187.00 | +201.50 | +234.50 | +421.50 |
| 2026-09-08 | `CRM` | 2 | $259.23 | $253.72 | -11.02 | $249.12 | -9.20 | -20.22 | -19.28 | -28.48 |
| 2026-09-08 | `BE` | 2 | $252.87 | $267.76 | +29.78 | $277.22 | +18.92 | +48.70 | +61.88 | +80.80 |
| 2026-09-08 | `MSTR` | 4 | $142.80 | $137.62 | -20.72 | $136.52 | -4.40 | -25.12 | +1.08 | -3.32 |
| 2026-09-09 | `AVGO` | 6 | $368.56 | $366.23 | -13.98 | — | +0.00 | -13.98 | +86.94 | — |
| 2026-09-09 | `DELL` | 4 | $533.88 | $538.47 | +18.36 | — | +0.00 | +18.36 | +208.64 | — |
| 2026-09-09 | `HPE` | 50 | $56.03 | $56.94 | +45.50 | — | +0.00 | +45.50 | +467.00 | — |
| 2026-09-09 | `CRM` | 2 | $249.12 | $249.78 | +1.32 | $244.16 | -11.24 | -9.92 | -27.16 | -38.40 |
| 2026-09-09 | `BE` | 2 | $277.22 | $272.99 | -8.46 | $269.28 | -7.42 | -15.88 | +72.34 | +64.92 |
| 2026-09-09 | `MSTR` | 4 | $136.52 | $141.82 | +21.20 | $132.70 | -36.48 | -15.28 | +17.88 | -18.60 |
| 2026-09-10 | `CRM` | 2 | $244.16 | $245.35 | +2.38 | — | +0.00 | +2.38 | -36.02 | — |
| 2026-09-10 | `BE` | 2 | $269.28 | $260.71 | -17.14 | — | +0.00 | -17.14 | +47.78 | — |
| 2026-09-10 | `MSTR` | 4 | $132.70 | $128.44 | -17.04 | — | +0.00 | -17.04 | -35.64 | — |
| 2026-09-11 | `ADBE` | 36 | — | $242.17 | +0.00 | $252.23 | +362.16 | +362.16 | +0.00 | +362.16 |
| 2026-09-14 | `ADBE` | 36 | $252.23 | $261.51 | +334.08 | $265.60 | +147.24 | +481.32 | +696.24 | +843.48 |
| 2026-09-15 | `ADBE` | 36 | $265.60 | $261.70 | -140.40 | $257.76 | -141.84 | -282.24 | +703.08 | +561.24 |
| 2026-09-16 | `ADBE` | 36 | $257.76 | $253.34 | -159.12 | — | +0.00 | -159.12 | +402.12 | — |
| 2026-09-16 | `QCOM` | 24 | — | $189.17 | +0.00 | $184.84 | -103.92 | -103.92 | +0.00 | -103.92 |
| 2026-09-16 | `SM` | 116 | — | $39.99 | +0.00 | $38.16 | -212.28 | -212.28 | +0.00 | -212.28 |
| 2026-09-17 | `QCOM` | 24 | $184.84 | $190.35 | +132.24 | $188.71 | -39.36 | +92.88 | +28.32 | -11.04 |
| 2026-09-17 | `SM` | 116 | $38.16 | $37.57 | -68.44 | $36.97 | -69.60 | -138.04 | -280.72 | -350.32 |
| 2026-09-18 | `QCOM` | 24 | $188.71 | $191.34 | +63.12 | $177.72 | -326.88 | -263.76 | +52.08 | -274.80 |
| 2026-09-18 | `SM` | 116 | $36.97 | $36.87 | -11.60 | $36.97 | +11.60 | +0.00 | -361.92 | -350.32 |
| 2026-09-21 | `QCOM` | 24 | $177.72 | $180.61 | +69.36 | — | +0.00 | +69.36 | -205.44 | — |
| 2026-09-21 | `SM` | 116 | $36.97 | $35.91 | -122.96 | — | +0.00 | -122.96 | -473.28 | — |
| 2026-09-22 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-23 | `CTAS` | 43 | — | $196.78 | +0.00 | $191.97 | -206.83 | -206.83 | +0.00 | -206.83 |
| 2026-09-24 | `CTAS` | 43 | $191.97 | $192.26 | +12.47 | $197.68 | +233.06 | +245.53 | -194.36 | +38.70 |

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
| 2026-08-25 | +1.80 | $1.45 | APA×210 | $8,691.25 | -331.80 | +331.00 | AU, FCX | APA | $98.18 | $9,015.18 | AU×36, FCX×56 |
| 2026-08-26 | +2.02 | $98.18 | AU×36, FCX×56 | $8,854.02 | -161.16 | -79.88 | — | — | $98.18 | $8,774.14 | AU×36, FCX×56 |
| 2026-08-27 | — | $98.18 | AU×36, FCX×56 | $8,739.42 | -34.72 | +12.68 | — | — | $98.18 | $8,752.10 | AU×36, FCX×56 |
| 2026-08-28 | +0.75 | $98.18 | AU×36, FCX×56 | $8,788.94 | +36.84 | -165.21 | KEYS, CIEN, MPWR, DDOG, ADSK, RRC | AU, FCX | $759.55 | $8,607.29 | KEYS×4, CIEN×3, MPWR×1, DDOG×6, ADSK×5, RRC×35 |
| 2026-08-31 | -5.85 | $759.55 | KEYS×4, CIEN×3, MPWR×1, DDOG×6, ADSK×5, RRC×35 | $8,609.07 | +1.78 | +18.54 | — | — | $759.55 | $8,627.61 | KEYS×4, CIEN×3, MPWR×1, DDOG×6, ADSK×5, RRC×35 |
| 2026-09-01 | -6.30 | $759.55 | KEYS×4, CIEN×3, MPWR×1, DDOG×6, ADSK×5, RRC×35 | $8,553.79 | -73.82 | -144.72 | — | — | $759.55 | $8,409.07 | KEYS×4, CIEN×3, MPWR×1, DDOG×6, ADSK×5, RRC×35 |
| 2026-09-02 | -3.83 | $759.55 | KEYS×4, CIEN×3, MPWR×1, DDOG×6, ADSK×5, RRC×35 | $8,352.14 | -56.93 | -9.27 | — | KEYS, MPWR, DDOG, ADSK, RRC | $7,270.18 | $8,332.66 | CIEN×3 |
| 2026-09-03 | -0.90 | $7,270.18 | CIEN×3 | $8,333.65 | +0.99 | +383.75 | AVGO, DELL, HPE | — | $828.35 | $8,711.25 | CIEN×3, AVGO×6, DELL×4, HPE×50 |
| 2026-09-04 | +2.25 | $828.35 | CIEN×3, AVGO×6, DELL×4, HPE×50 | $8,699.18 | -12.07 | -16.22 | CRM, BE, MSTR | CIEN | $235.59 | $8,674.95 | AVGO×6, DELL×4, HPE×50, CRM×2, BE×2, MSTR×4 |
| 2026-09-08 | -11.47 | $235.59 | AVGO×6, DELL×4, HPE×50, CRM×2, BE×2, MSTR×4 | $8,710.21 | +35.26 | +272.52 | — | — | $235.59 | $8,982.73 | AVGO×6, DELL×4, HPE×50, CRM×2, BE×2, MSTR×4 |
| 2026-09-09 | -13.95 | $235.59 | AVGO×6, DELL×4, HPE×50, CRM×2, BE×2, MSTR×4 | $9,046.67 | +63.94 | -55.14 | — | AVGO, DELL, HPE | $7,427.61 | $8,985.29 | CRM×2, BE×2, MSTR×4 |
| 2026-09-10 | -13.28 | $7,427.61 | CRM×2, BE×2, MSTR×4 | $8,953.49 | -31.80 | +0.00 | — | CRM, BE, MSTR | $8,947.43 | $8,947.43 | — |
| 2026-09-11 | +0.50 | $8,947.43 | — | $8,947.43 | +0.00 | +362.16 | ADBE | — | $227.22 | $9,307.50 | ADBE×36 |
| 2026-09-14 | -11.00 | $227.22 | ADBE×36 | $9,641.58 | +334.08 | +147.24 | — | — | $227.22 | $9,788.82 | ADBE×36 |
| 2026-09-15 | -3.84 | $227.22 | ADBE×36 | $9,648.42 | -140.40 | -141.84 | — | — | $227.22 | $9,506.58 | ADBE×36 |
| 2026-09-16 | +5.30 | $227.22 | ADBE×36 | $9,347.46 | -159.12 | -316.20 | QCOM, SM | ADBE | $161.95 | $9,024.67 | QCOM×24, SM×116 |
| 2026-09-17 | +7.38 | $161.95 | QCOM×24, SM×116 | $9,088.47 | +63.80 | -108.96 | — | — | $161.95 | $8,979.51 | QCOM×24, SM×116 |
| 2026-09-18 | +4.86 | $161.95 | QCOM×24, SM×116 | $9,031.03 | +51.52 | -315.28 | — | — | $161.95 | $8,715.75 | QCOM×24, SM×116 |
| 2026-09-21 | +12.87 | $161.95 | QCOM×24, SM×116 | $8,662.15 | -53.60 | +0.00 | — | QCOM, SM | $8,657.66 | $8,657.66 | — |
| 2026-09-22 | -0.50 | $8,657.66 | — | $8,657.66 | -0.00 | +0.00 | — | — | $8,657.66 | $8,657.66 | — |
| 2026-09-23 | +2.29 | $8,657.66 | — | $8,657.66 | -0.00 | -206.83 | CTAS | — | $194.00 | $8,448.71 | CTAS×43 |
| 2026-09-24 | -7.66 | $194.00 | CTAS×43 | $8,461.18 | +12.47 | +233.06 | — | — | $194.00 | $8,694.24 | CTAS×43 |

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
| 2026-08-19 09:30 ET | **SELL** | `TLN` | 9 | $321.00 | $2.05 | $-353.54 | $6,212.54 | ▼ -353.54 after sell → book $9,405.26; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
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
| 2026-08-25 09:30 ET | **BUY** | `AU` | 36 | $118.52 | $2.10 | — | $4,419.62 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $4344.22 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 56 | $77.13 | $2.16 | — | $98.18 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $4344.22 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.18 | ▲ close $9,015.18 vs 09:30 $8,691.25 (session +331.00) | 16:00 close · cash $98.18 · equity $9,015.18 vs 09:30 $8,691.25 (+323.93; session marks +331.00) · 2 name(s) marked open→close (per-name table). AU×36 09:30 $118.52 → close $123.39 +175.32; FCX×56 09:30 $77.13 → close $79.91 +155.68 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.18 | ▼ 09:30 equity $8,854.02 vs yday $9,015.18 (-161.16) | 09:30 open · cash $98.18 (unchanged overnight, no fees) · equity $8,854.02 vs prior close $9,015.18 (-161.16) · 2 name(s) re-marked at the open (per-name table). AU×36 yday $123.39 → 09:30 $119.80 -129.24; FCX×56 yday $79.91 → 09:30 $79.34 -31.92 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.18 | ▼ close $8,774.14 vs 09:30 $8,854.02 (session -79.88) | 16:00 close · cash $98.18 · equity $8,774.14 vs 09:30 $8,854.02 (-79.88; session marks -79.88) · 2 name(s) marked open→close (per-name table). AU×36 09:30 $119.80 → close $118.11 -60.84; FCX×56 09:30 $79.34 → close $79.00 -19.04 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.18 | ▼ 09:30 equity $8,739.42 vs yday $8,774.14 (-34.72) | 09:30 open · cash $98.18 (unchanged overnight, no fees) · equity $8,739.42 vs prior close $8,774.14 (-34.72) · 2 name(s) re-marked at the open (per-name table). AU×36 yday $118.11 → 09:30 $117.41 -25.20; FCX×56 yday $79.00 → 09:30 $78.83 -9.52 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.18 | ▲ close $8,752.10 vs 09:30 $8,739.42 (session +12.68) | 16:00 close · cash $98.18 · equity $8,752.10 vs 09:30 $8,739.42 (+12.68; session marks +12.68) · 2 name(s) marked open→close (per-name table). AU×36 09:30 $117.41 → close $118.40 +35.64; FCX×56 09:30 $78.83 → close $78.42 -22.96 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.18 | ▲ 09:30 equity $8,788.94 vs yday $8,752.10 (+36.84) | 09:30 open · cash $98.18 (unchanged overnight, no fees) · equity $8,788.94 vs prior close $8,752.10 (+36.84) · 2 name(s) re-marked at the open (per-name table). AU×36 yday $118.40 → 09:30 $119.19 +28.44; FCX×56 yday $78.42 → 09:30 $78.57 +8.40 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 36 | $119.19 | $2.14 | $+19.88 | $4,386.88 | ▲ +19.88 after sell → book $8,786.80; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 56 | $78.57 | $2.20 | $+76.28 | $8,784.60 | ▲ +76.28 after sell → book $8,784.60; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $7,484.96 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1464.10 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,281.70 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1464.10 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,973.68 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1464.10 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 6 | $240.22 | $2.01 | — | $3,530.35 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1464.10 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $2,222.54 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=+7.8; leftover $1464.10 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 35 | $41.74 | $2.10 | — | $759.55 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; ret5=+2.4; leftover $1464.10 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $759.55 | ▼ close $8,607.29 vs 09:30 $8,788.94 (session -165.21) | 16:00 close · cash $759.55 · equity $8,607.29 vs 09:30 $8,788.94 (-181.65; session marks -165.21) · 6 name(s) marked open→close (per-name table). KEYS×4 09:30 $324.41 → close $319.97 -17.76; CIEN×3 09:30 $400.42 → close $378.44 -65.94; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×6 09:30 $240.22 → close $236.98 -19.44; ADSK×5 09:30 $261.16 → close $260.66 -2.50; RRC×35 09:30 $41.74 → close $41.46 -9.80 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $759.55 | ▲ 09:30 equity $8,609.07 vs yday $8,607.29 (+1.78) | 09:30 open · cash $759.55 (unchanged overnight, no fees) · equity $8,609.07 vs prior close $8,607.29 (+1.78) · 6 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×6 yday $236.98 → 09:30 $233.97 -18.09; ADSK×5 yday $260.66 → 09:30 $257.71 -14.75; RRC×35 yday $41.46 → 09:30 $42.00 +18.90 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $759.55 | ▲ close $8,627.61 vs 09:30 $8,609.07 (session +18.54) | 16:00 close · cash $759.55 · equity $8,627.61 vs 09:30 $8,609.07 (+18.54; session marks +18.54) · 6 name(s) marked open→close (per-name table). KEYS×4 09:30 $322.49 → close $322.70 +0.84; CIEN×3 09:30 $378.44 → close $382.80 +13.08; MPWR×1 09:30 $1261.90 → close $1267.77 +5.87; DDOG×6 09:30 $233.97 → close $237.04 +18.45; ADSK×5 09:30 $257.71 → close $258.53 +4.10; RRC×35 09:30 $42.00 → close $41.32 -23.80 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $759.55 | ▼ 09:30 equity $8,553.79 vs yday $8,627.61 (-73.82) | 09:30 open · cash $759.55 (unchanged overnight, no fees) · equity $8,553.79 vs prior close $8,627.61 (-73.82) · 6 name(s) re-marked at the open (per-name table). KEYS×4 yday $322.70 → 09:30 $321.47 -4.92; CIEN×3 yday $382.80 → 09:30 $376.89 -17.73; MPWR×1 yday $1267.77 → 09:30 $1245.11 -22.66; DDOG×6 yday $237.04 → 09:30 $232.88 -24.96; ADSK×5 yday $258.53 → 09:30 $253.48 -25.25; RRC×35 yday $41.32 → 09:30 $41.94 +21.70 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $759.55 | ▼ close $8,409.07 vs 09:30 $8,553.79 (session -144.72) | 16:00 close · cash $759.55 · equity $8,409.07 vs 09:30 $8,553.79 (-144.72; session marks -144.72) · 6 name(s) marked open→close (per-name table). KEYS×4 09:30 $321.47 → close $319.27 -8.80; CIEN×3 09:30 $376.89 → close $360.33 -49.68; MPWR×1 09:30 $1245.11 → close $1225.96 -19.15; DDOG×6 09:30 $232.88 → close $223.84 -54.24; ADSK×5 09:30 $253.48 → close $247.69 -28.95; RRC×35 09:30 $41.94 → close $42.40 +16.10 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $759.55 | ▼ 09:30 equity $8,352.14 vs yday $8,409.07 (-56.93) | 09:30 open · cash $759.55 (unchanged overnight, no fees) · equity $8,352.14 vs prior close $8,409.07 (-56.93) · 6 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.27 → 09:30 $318.04 -4.92; CIEN×3 yday $360.33 → 09:30 $357.25 -9.24; MPWR×1 yday $1225.96 → 09:30 $1224.92 -1.04; DDOG×6 yday $223.84 → 09:30 $219.46 -26.28; ADSK×5 yday $247.69 → 09:30 $246.70 -4.95; RRC×35 yday $42.40 → 09:30 $42.10 -10.50 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $318.04 | $2.02 | $-29.50 | $2,029.68 | ▼ -29.50 after sell → book $8,350.11; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1224.92 | $2.01 | $-85.12 | $3,252.59 | ▼ -85.12 after sell → book $8,348.10; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 6 | $219.46 | $2.03 | $-128.60 | $4,567.32 | ▼ -128.60 after sell → book $8,346.07; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 5 | $246.70 | $2.02 | $-76.33 | $5,798.80 | ▼ -76.33 after sell → book $8,344.05; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 35 | $42.10 | $2.12 | $+8.39 | $7,270.18 | ▲ +8.39 after sell → book $8,341.93; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,270.18 | ▼ close $8,332.66 vs 09:30 $8,352.14 (session -9.27) | 16:00 close · cash $7,270.18 · equity $8,332.66 vs 09:30 $8,352.14 (-19.48; session marks -9.27) · 1 name(s) marked open→close (per-name table). CIEN×3 09:30 $357.25 → close $354.16 -9.27 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,270.18 | ▲ 09:30 equity $8,333.65 vs yday $8,332.66 (+0.99) | 09:30 open · cash $7,270.18 (unchanged overnight, no fees) · equity $8,333.65 vs prior close $8,332.66 (+0.99) · 1 name(s) re-marked at the open (per-name table). CIEN×3 yday $354.16 → 09:30 $354.49 +0.99 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 6 | $351.74 | $2.01 | — | $5,157.73 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $2423.39 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 4 | $486.31 | $2.00 | — | $3,210.49 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $2423.39 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 50 | $47.60 | $2.14 | — | $828.35 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $2423.39 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $828.35 | ▲ close $8,711.25 vs 09:30 $8,333.65 (session +383.75) | 16:00 close · cash $828.35 · equity $8,711.25 vs 09:30 $8,333.65 (+377.60; session marks +383.75) · 4 name(s) marked open→close (per-name table). CIEN×3 09:30 $354.49 → close $317.46 -111.09; AVGO×6 09:30 $351.74 → close $357.16 +32.52; DELL×4 09:30 $486.31 → close $516.39 +120.32; HPE×50 09:30 $47.60 → close $54.44 +342.00 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $828.35 | ▼ 09:30 equity $8,699.18 vs yday $8,711.25 (-12.07) | 09:30 open · cash $828.35 (unchanged overnight, no fees) · equity $8,699.18 vs prior close $8,711.25 (-12.07) · 4 name(s) re-marked at the open (per-name table). CIEN×3 yday $317.46 → 09:30 $321.67 +12.63; AVGO×6 yday $357.16 → 09:30 $359.70 +15.24; DELL×4 yday $516.39 → 09:30 $513.78 -10.44; HPE×50 yday $54.44 → 09:30 $53.85 -29.50 | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $321.67 | $2.02 | $-240.27 | $1,791.34 | ▼ -240.27 after sell → book $8,697.16; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 3) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 2 | $263.36 | $2.00 | — | $1,262.63 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $597.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 2 | $236.82 | $2.00 | — | $786.99 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+8.1; leftover $597.11 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 4 | $137.35 | $2.00 | — | $235.59 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+5.4; leftover $597.11 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $235.59 | ▼ close $8,674.95 vs 09:30 $8,699.18 (session -16.22) | 16:00 close · cash $235.59 · equity $8,674.95 vs 09:30 $8,699.18 (-24.23; session marks -16.22) · 6 name(s) marked open→close (per-name table). AVGO×6 09:30 $359.70 → close $357.90 -10.80; DELL×4 09:30 $513.78 → close $524.14 +41.44; HPE×50 09:30 $53.85 → close $52.00 -92.50; CRM×2 09:30 $263.36 → close $259.23 -8.26; BE×2 09:30 $236.82 → close $252.87 +32.10; MSTR×4 09:30 $137.35 → close $142.80 +21.80 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $235.59 | ▲ 09:30 equity $8,710.21 vs yday $8,674.95 (+35.26) | 09:30 open · cash $235.59 (unchanged overnight, no fees) · equity $8,710.21 vs prior close $8,674.95 (+35.26) · 6 name(s) re-marked at the open (per-name table). AVGO×6 yday $357.90 → 09:30 $363.68 +34.68; DELL×4 yday $524.14 → 09:30 $521.15 -11.96; HPE×50 yday $52.00 → 09:30 $52.29 +14.50; CRM×2 yday $259.23 → 09:30 $253.72 -11.02; BE×2 yday $252.87 → 09:30 $267.76 +29.78; MSTR×4 yday $142.80 → 09:30 $137.62 -20.72 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $235.59 | ▲ close $8,982.73 vs 09:30 $8,710.21 (session +272.52) | 16:00 close · cash $235.59 · equity $8,982.73 vs 09:30 $8,710.21 (+272.52; session marks +272.52) · 6 name(s) marked open→close (per-name table). AVGO×6 09:30 $363.68 → close $368.56 +29.28; DELL×4 09:30 $521.15 → close $533.88 +50.92; HPE×50 09:30 $52.29 → close $56.03 +187.00; CRM×2 09:30 $253.72 → close $249.12 -9.20; BE×2 09:30 $267.76 → close $277.22 +18.92; MSTR×4 09:30 $137.62 → close $136.52 -4.40 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $235.59 | ▲ 09:30 equity $9,046.67 vs yday $8,982.73 (+63.94) | 09:30 open · cash $235.59 (unchanged overnight, no fees) · equity $9,046.67 vs prior close $8,982.73 (+63.94) · 6 name(s) re-marked at the open (per-name table). AVGO×6 yday $368.56 → 09:30 $366.23 -13.98; DELL×4 yday $533.88 → 09:30 $538.47 +18.36; HPE×50 yday $56.03 → 09:30 $56.94 +45.50; CRM×2 yday $249.12 → 09:30 $249.78 +1.32; BE×2 yday $277.22 → 09:30 $272.99 -8.46; MSTR×4 yday $136.52 → 09:30 $141.82 +21.20 | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 6 | $366.23 | $2.04 | $+82.90 | $2,430.93 | ▲ +82.90 after sell → book $9,044.63; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 4 | $538.47 | $2.03 | $+204.61 | $4,582.78 | ▲ +204.61 after sell → book $9,042.60; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 50 | $56.94 | $2.17 | $+462.69 | $7,427.61 | ▲ +462.69 after sell → book $9,040.43; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,427.61 | ▼ close $8,985.29 vs 09:30 $9,046.67 (session -55.14) | 16:00 close · cash $7,427.61 · equity $8,985.29 vs 09:30 $9,046.67 (-61.38; session marks -55.14) · 3 name(s) marked open→close (per-name table). CRM×2 09:30 $249.78 → close $244.16 -11.24; BE×2 09:30 $272.99 → close $269.28 -7.42; MSTR×4 09:30 $141.82 → close $132.70 -36.48 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,427.61 | ▼ 09:30 equity $8,953.49 vs yday $8,985.29 (-31.80) | 09:30 open · cash $7,427.61 (unchanged overnight, no fees) · equity $8,953.49 vs prior close $8,985.29 (-31.80) · 3 name(s) re-marked at the open (per-name table). CRM×2 yday $244.16 → 09:30 $245.35 +2.38; BE×2 yday $269.28 → 09:30 $260.71 -17.14; MSTR×4 yday $132.70 → 09:30 $128.44 -17.04 | — |
| 2026-09-10 09:30 ET | **SELL** | `CRM` | 2 | $245.35 | $2.02 | $-40.03 | $7,916.30 | ▼ -40.03 after sell → book $8,951.47; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BE` | 2 | $260.71 | $2.02 | $+43.76 | $8,435.70 | ▲ +43.76 after sell → book $8,949.46; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MSTR` | 4 | $128.44 | $2.02 | $-39.66 | $8,947.43 | ▼ -39.66 after sell → book $8,947.43; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,947.43 | ▲ close $8,947.43 vs 09:30 $8,953.49 (session +0.00) | 16:00 close · cash $8,947.43 · no lots left · equity $8,947.43. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,947.43 | ▲ 09:30 equity $8,947.43 vs yday $8,947.43 (+0.00) | 09:30 open · cash $8,947.43 · no holdings · equity $8,947.43 vs prior close $8,947.43 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 36 | $242.17 | $2.10 | — | $227.22 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=-11.1; leftover $8947.43 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.22 | ▲ close $9,307.50 vs 09:30 $8,947.43 (session +362.16) | 16:00 close · cash $227.22 · equity $9,307.50 vs 09:30 $8,947.43 (+360.07; session marks +362.16) · 1 name(s) marked open→close (per-name table). ADBE×36 09:30 $242.17 → close $252.23 +362.16 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.22 | ▲ 09:30 equity $9,641.58 vs yday $9,307.50 (+334.08) | 09:30 open · cash $227.22 (unchanged overnight, no fees) · equity $9,641.58 vs prior close $9,307.50 (+334.08) · 1 name(s) re-marked at the open (per-name table). ADBE×36 yday $252.23 → 09:30 $261.51 +334.08 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.22 | ▲ close $9,788.82 vs 09:30 $9,641.58 (session +147.24) | 16:00 close · cash $227.22 · equity $9,788.82 vs 09:30 $9,641.58 (+147.24; session marks +147.24) · 1 name(s) marked open→close (per-name table). ADBE×36 09:30 $261.51 → close $265.60 +147.24 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.22 | ▼ 09:30 equity $9,648.42 vs yday $9,788.82 (-140.40) | 09:30 open · cash $227.22 (unchanged overnight, no fees) · equity $9,648.42 vs prior close $9,788.82 (-140.40) · 1 name(s) re-marked at the open (per-name table). ADBE×36 yday $265.60 → 09:30 $261.70 -140.40 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.22 | ▼ close $9,506.58 vs 09:30 $9,648.42 (session -141.84) | 16:00 close · cash $227.22 · equity $9,506.58 vs 09:30 $9,648.42 (-141.84; session marks -141.84) · 1 name(s) marked open→close (per-name table). ADBE×36 09:30 $261.70 → close $257.76 -141.84 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.22 | ▼ 09:30 equity $9,347.46 vs yday $9,506.58 (-159.12) | 09:30 open · cash $227.22 (unchanged overnight, no fees) · equity $9,347.46 vs prior close $9,506.58 (-159.12) · 1 name(s) re-marked at the open (per-name table). ADBE×36 yday $257.76 → 09:30 $253.34 -159.12 | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 36 | $253.34 | $2.18 | $+397.84 | $9,345.27 | ▲ +397.84 after sell → book $9,345.27; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 24 | $189.17 | $2.06 | — | $4,803.13 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $4672.64 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 116 | $39.99 | $2.34 | — | $161.95 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $4672.64 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.95 | ▼ close $9,024.67 vs 09:30 $9,347.46 (session -316.20) | 16:00 close · cash $161.95 · equity $9,024.67 vs 09:30 $9,347.46 (-322.79; session marks -316.20) · 2 name(s) marked open→close (per-name table). QCOM×24 09:30 $189.17 → close $184.84 -103.92; SM×116 09:30 $39.99 → close $38.16 -212.28 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.95 | ▲ 09:30 equity $9,088.47 vs yday $9,024.67 (+63.80) | 09:30 open · cash $161.95 (unchanged overnight, no fees) · equity $9,088.47 vs prior close $9,024.67 (+63.80) · 2 name(s) re-marked at the open (per-name table). QCOM×24 yday $184.84 → 09:30 $190.35 +132.24; SM×116 yday $38.16 → 09:30 $37.57 -68.44 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.95 | ▼ close $8,979.51 vs 09:30 $9,088.47 (session -108.96) | 16:00 close · cash $161.95 · equity $8,979.51 vs 09:30 $9,088.47 (-108.96; session marks -108.96) · 2 name(s) marked open→close (per-name table). QCOM×24 09:30 $190.35 → close $188.71 -39.36; SM×116 09:30 $37.57 → close $36.97 -69.60 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.95 | ▲ 09:30 equity $9,031.03 vs yday $8,979.51 (+51.52) | 09:30 open · cash $161.95 (unchanged overnight, no fees) · equity $9,031.03 vs prior close $8,979.51 (+51.52) · 2 name(s) re-marked at the open (per-name table). QCOM×24 yday $188.71 → 09:30 $191.34 +63.12; SM×116 yday $36.97 → 09:30 $36.87 -11.60 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.95 | ▼ close $8,715.75 vs 09:30 $9,031.03 (session -315.28) | 16:00 close · cash $161.95 · equity $8,715.75 vs 09:30 $9,031.03 (-315.28; session marks -315.28) · 2 name(s) marked open→close (per-name table). QCOM×24 09:30 $191.34 → close $177.72 -326.88; SM×116 09:30 $36.87 → close $36.97 +11.60 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.95 | ▼ 09:30 equity $8,662.15 vs yday $8,715.75 (-53.60) | 09:30 open · cash $161.95 (unchanged overnight, no fees) · equity $8,662.15 vs prior close $8,715.75 (-53.60) · 2 name(s) re-marked at the open (per-name table). QCOM×24 yday $177.72 → 09:30 $180.61 +69.36; SM×116 yday $36.97 → 09:30 $35.91 -122.96 | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 24 | $180.61 | $2.11 | $-209.61 | $4,494.49 | ▼ -209.61 after sell → book $8,660.05; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 116 | $35.91 | $2.39 | $-478.01 | $8,657.66 | ▼ -478.01 after sell → book $8,657.66; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,657.66 | ▲ close $8,657.66 vs 09:30 $8,662.15 (session +0.00) | 16:00 close · cash $8,657.66 · no lots left · equity $8,657.66. | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,657.66 | ▲ 09:30 equity $8,657.66 vs yday $8,657.66 (-0.00) | 09:30 open · cash $8,657.66 · no holdings · equity $8,657.66 vs prior close $8,657.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,657.66 | ▲ close $8,657.66 vs 09:30 $8,657.66 (session +0.00) | 16:00 close · cash $8,657.66 · no lots left · equity $8,657.66. | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,657.66 | ▲ 09:30 equity $8,657.66 vs yday $8,657.66 (-0.00) | 09:30 open · cash $8,657.66 · no holdings · equity $8,657.66 vs prior close $8,657.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 43 | $196.78 | $2.12 | — | $194.00 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $8657.66 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $194.00 | ▼ close $8,448.71 vs 09:30 $8,657.66 (session -206.83) | 16:00 close · cash $194.00 · equity $8,448.71 vs 09:30 $8,657.66 (-208.95; session marks -206.83) · 1 name(s) marked open→close (per-name table). CTAS×43 09:30 $196.78 → close $191.97 -206.83 | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $194.00 | ▲ 09:30 equity $8,461.18 vs yday $8,448.71 (+12.47) | 09:30 open · cash $194.00 (unchanged overnight, no fees) · equity $8,461.18 vs prior close $8,448.71 (+12.47) · 1 name(s) re-marked at the open (per-name table). CTAS×43 yday $191.97 → 09:30 $192.26 +12.47 | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $194.00 | ▲ close $8,694.24 vs 09:30 $8,461.18 (session +233.06) | 16:00 close · cash $194.00 · equity $8,694.24 vs 09:30 $8,461.18 (+233.06; session marks +233.06) · 1 name(s) marked open→close (per-name table). CTAS×43 09:30 $192.26 → close $197.68 +233.06 | — |

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
| 2026-08-21 | `AU` | cash | leftover split 0.73 < 1 share @ 119.43 |
| 2026-08-21 | `DE` | cash | leftover split 0.73 < 1 share @ 623.26 |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 49.09 < 1 share @ 267.02 |
| 2026-08-26 | `CM` | cash | leftover split 49.09 < 1 share @ 118.50 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 12.27 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 12.27 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 12.27 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 12.27 < 1 share @ 118.77 |
| 2026-08-27 | `LRCX` | cash | leftover split 12.27 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 12.27 < 1 share @ 222.86 |
| 2026-08-27 | `ADSK` | cash | leftover split 12.27 < 1 share @ 261.47 |
| 2026-08-27 | `AXTI` | cash | leftover split 12.27 < 1 share @ 70.30 |
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
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `LITE` | cash | leftover split 161.95 < 1 share @ 934.88 |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `CTAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CTAS` | 43 | 2026-09-23 @ $196.78 | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $8657.66 |
