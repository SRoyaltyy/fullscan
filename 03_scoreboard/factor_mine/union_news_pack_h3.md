# Factor mine action — `union_news_pack_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · morning packet news🟢 only (not the merged box)

Cash book **-13.79%** ($8,621) · signal-only (no cash/fees) was -10.79%. Starts YES **6/26**. Fills 43 · skips 75 · realized $-672.59.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6.74.

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
| 2026-09-02 | `CIEN` | 3 | $360.33 | $357.25 | -9.24 | — | +0.00 | -9.24 | -129.51 | — |
| 2026-09-02 | `MPWR` | 1 | $1225.96 | $1224.92 | -1.04 | — | +0.00 | -1.04 | -81.11 | — |
| 2026-09-02 | `DDOG` | 6 | $223.84 | $219.46 | -26.28 | — | +0.00 | -26.28 | -124.56 | — |
| 2026-09-02 | `ADSK` | 5 | $247.69 | $246.70 | -4.95 | — | +0.00 | -4.95 | -72.30 | — |
| 2026-09-02 | `RRC` | 35 | $42.40 | $42.10 | -10.50 | — | +0.00 | -10.50 | +12.60 | — |
| 2026-09-03 | `AVGO` | 5 | — | $351.74 | +0.00 | $357.16 | +27.10 | +27.10 | +0.00 | +27.10 |
| 2026-09-03 | `DELL` | 4 | — | $486.31 | +0.00 | $516.39 | +120.32 | +120.32 | +0.00 | +120.32 |
| 2026-09-03 | `HPE` | 43 | — | $47.60 | +0.00 | $54.44 | +294.12 | +294.12 | +0.00 | +294.12 |
| 2026-09-03 | `CIEN` | 5 | — | $354.49 | +0.00 | $317.46 | -185.15 | -185.15 | +0.00 | -185.15 |
| 2026-09-04 | `AVGO` | 5 | $357.16 | $359.70 | +12.70 | $357.90 | -9.00 | +3.70 | +39.80 | +30.80 |
| 2026-09-04 | `DELL` | 4 | $516.39 | $513.78 | -10.44 | $524.14 | +41.44 | +31.00 | +109.88 | +151.32 |
| 2026-09-04 | `HPE` | 43 | $54.44 | $53.85 | -25.37 | $52.00 | -79.55 | -104.92 | +268.75 | +189.20 |
| 2026-09-04 | `CIEN` | 5 | $317.46 | $321.67 | +21.05 | $321.00 | -3.35 | +17.70 | -164.10 | -167.45 |
| 2026-09-04 | `CRM` | 1 | — | $263.36 | +0.00 | $259.23 | -4.13 | -4.13 | +0.00 | -4.13 |
| 2026-09-04 | `BE` | 1 | — | $236.82 | +0.00 | $252.87 | +16.05 | +16.05 | +0.00 | +16.05 |
| 2026-09-04 | `MSTR` | 1 | — | $137.35 | +0.00 | $142.80 | +5.45 | +5.45 | +0.00 | +5.45 |
| 2026-09-08 | `AVGO` | 5 | $357.90 | $363.68 | +28.90 | $368.56 | +24.40 | +53.30 | +59.70 | +84.10 |
| 2026-09-08 | `DELL` | 4 | $524.14 | $521.15 | -11.96 | $533.88 | +50.92 | +38.96 | +139.36 | +190.28 |
| 2026-09-08 | `HPE` | 43 | $52.00 | $52.29 | +12.47 | $56.03 | +160.82 | +173.29 | +201.67 | +362.49 |
| 2026-09-08 | `CIEN` | 5 | $321.00 | $327.42 | +32.10 | $341.29 | +69.35 | +101.45 | -135.35 | -66.00 |
| 2026-09-08 | `CRM` | 1 | $259.23 | $253.72 | -5.51 | $249.12 | -4.60 | -10.11 | -9.64 | -14.24 |
| 2026-09-08 | `BE` | 1 | $252.87 | $267.76 | +14.89 | $277.22 | +9.46 | +24.35 | +30.94 | +40.40 |
| 2026-09-08 | `MSTR` | 1 | $142.80 | $137.62 | -5.18 | $136.52 | -1.10 | -6.28 | +0.27 | -0.83 |
| 2026-09-09 | `AVGO` | 5 | $368.56 | $366.23 | -11.65 | — | +0.00 | -11.65 | +72.45 | — |
| 2026-09-09 | `DELL` | 4 | $533.88 | $538.47 | +18.36 | — | +0.00 | +18.36 | +208.64 | — |
| 2026-09-09 | `HPE` | 43 | $56.03 | $56.94 | +39.13 | — | +0.00 | +39.13 | +401.62 | — |
| 2026-09-09 | `CIEN` | 5 | $341.29 | $341.90 | +3.05 | — | +0.00 | +3.05 | -62.95 | — |
| 2026-09-09 | `CRM` | 1 | $249.12 | $249.78 | +0.66 | $244.16 | -5.62 | -4.96 | -13.58 | -19.20 |
| 2026-09-09 | `BE` | 1 | $277.22 | $272.99 | -4.23 | $269.28 | -3.71 | -7.94 | +36.17 | +32.46 |
| 2026-09-09 | `MSTR` | 1 | $136.52 | $141.82 | +5.30 | $132.70 | -9.12 | -3.82 | +4.47 | -4.65 |
| 2026-09-10 | `CRM` | 1 | $244.16 | $245.35 | +1.19 | — | +0.00 | +1.19 | -18.01 | — |
| 2026-09-10 | `BE` | 1 | $269.28 | $260.71 | -8.57 | — | +0.00 | -8.57 | +23.89 | — |
| 2026-09-10 | `MSTR` | 1 | $132.70 | $128.44 | -4.26 | — | +0.00 | -4.26 | -8.91 | — |
| 2026-09-11 | `ADBE` | 36 | — | $242.17 | +0.00 | $252.23 | +362.16 | +362.16 | +0.00 | +362.16 |
| 2026-09-14 | `ADBE` | 36 | $252.23 | $261.51 | +334.08 | $265.60 | +147.24 | +481.32 | +696.24 | +843.48 |
| 2026-09-15 | `ADBE` | 36 | $265.60 | $261.70 | -140.40 | $257.76 | -141.84 | -282.24 | +703.08 | +561.24 |
| 2026-09-16 | `ADBE` | 36 | $257.76 | $253.34 | -159.12 | — | +0.00 | -159.12 | +402.12 | — |
| 2026-09-16 | `SM` | 233 | — | $39.99 | +0.00 | $38.16 | -426.39 | -426.39 | +0.00 | -426.39 |
| 2026-09-17 | `SM` | 233 | $38.16 | $37.57 | -137.47 | $36.97 | -139.80 | -277.27 | -563.86 | -703.66 |
| 2026-09-18 | `SM` | 233 | $36.97 | $36.87 | -23.30 | $36.97 | +23.30 | +0.00 | -726.96 | -703.66 |

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
| 2026-09-02 | -3.83 | $759.55 | KEYS×4, CIEN×3, MPWR×1, DDOG×6, ADSK×5, RRC×35 | $8,352.14 | -56.93 | +0.00 | — | KEYS, CIEN, MPWR, DDOG, ADSK, RRC | $8,339.91 | $8,339.91 | — |
| 2026-09-03 | -0.90 | $8,339.91 | — | $8,339.91 | +0.00 | +256.39 | AVGO, DELL, HPE, CIEN | — | $808.59 | $8,588.17 | AVGO×5, DELL×4, HPE×43, CIEN×5 |
| 2026-09-04 | +2.25 | $808.59 | AVGO×5, DELL×4, HPE×43, CIEN×5 | $8,586.11 | -2.06 | -33.09 | CRM, BE, MSTR | — | $165.70 | $8,547.66 | AVGO×5, DELL×4, HPE×43, CIEN×5, CRM×1, BE×1, MSTR×1 |
| 2026-09-08 | -11.47 | $165.70 | AVGO×5, DELL×4, HPE×43, CIEN×5, CRM×1, BE×1, MSTR×1 | $8,613.37 | +65.71 | +309.25 | — | — | $165.70 | $8,922.62 | AVGO×5, DELL×4, HPE×43, CIEN×5, CRM×1, BE×1, MSTR×1 |
| 2026-09-09 | -13.95 | $165.70 | AVGO×5, DELL×4, HPE×43, CIEN×5, CRM×1, BE×1, MSTR×1 | $8,973.24 | +50.62 | -18.45 | — | AVGO, DELL, HPE, CIEN | $8,300.41 | $8,946.55 | CRM×1, BE×1, MSTR×1 |
| 2026-09-10 | -13.28 | $8,300.41 | CRM×1, BE×1, MSTR×1 | $8,934.91 | -11.64 | +0.00 | — | CRM, BE, MSTR | $8,929.58 | $8,929.58 | — |
| 2026-09-11 | +0.50 | $8,929.58 | — | $8,929.58 | -0.00 | +362.16 | ADBE | — | $209.36 | $9,289.64 | ADBE×36 |
| 2026-09-14 | -11.00 | $209.36 | ADBE×36 | $9,623.72 | +334.08 | +147.24 | — | — | $209.36 | $9,770.96 | ADBE×36 |
| 2026-09-15 | -3.84 | $209.36 | ADBE×36 | $9,630.56 | -140.40 | -141.84 | — | — | $209.36 | $9,488.72 | ADBE×36 |
| 2026-09-16 | +5.30 | $209.36 | ADBE×36 | $9,329.60 | -159.12 | -426.39 | SM | ADBE | $6.74 | $8,898.02 | SM×233 |
| 2026-09-17 | +7.38 | $6.74 | SM×233 | $8,760.55 | -137.47 | -139.80 | — | — | $6.74 | $8,620.75 | SM×233 |
| 2026-09-18 | +4.86 | $6.74 | SM×233 | $8,597.45 | -23.30 | +23.30 | — | — | $6.74 | $8,620.75 | SM×233 |

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
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 3 | $357.25 | $2.02 | $-133.53 | $3,099.42 | ▼ -133.53 after sell → book $8,348.10; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1224.92 | $2.01 | $-85.12 | $4,322.32 | ▼ -85.12 after sell → book $8,346.08; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 6 | $219.46 | $2.03 | $-128.60 | $5,637.05 | ▼ -128.60 after sell → book $8,344.05; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 5 | $246.70 | $2.02 | $-76.33 | $6,868.53 | ▼ -76.33 after sell → book $8,342.03; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 35 | $42.10 | $2.12 | $+8.39 | $8,339.91 | ▲ +8.39 after sell → book $8,339.91; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,339.91 | ▲ close $8,339.91 vs 09:30 $8,352.14 (session +0.00) | 16:00 close · cash $8,339.91 · no lots left · equity $8,339.91. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,339.91 | ▲ 09:30 equity $8,339.91 vs yday $8,339.91 (+0.00) | 09:30 open · cash $8,339.91 · no holdings · equity $8,339.91 vs prior close $8,339.91 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 5 | $351.74 | $2.00 | — | $6,579.21 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $2084.98 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 4 | $486.31 | $2.00 | — | $4,631.97 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $2084.98 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 43 | $47.60 | $2.12 | — | $2,583.05 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $2084.98 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 5 | $354.49 | $2.00 | — | $808.59 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=-12.3; leftover $2084.98 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $808.59 | ▲ close $8,588.17 vs 09:30 $8,339.91 (session +256.39) | 16:00 close · cash $808.59 · equity $8,588.17 vs 09:30 $8,339.91 (+248.26; session marks +256.39) · 4 name(s) marked open→close (per-name table). AVGO×5 09:30 $351.74 → close $357.16 +27.10; DELL×4 09:30 $486.31 → close $516.39 +120.32; HPE×43 09:30 $47.60 → close $54.44 +294.12; CIEN×5 09:30 $354.49 → close $317.46 -185.15 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $808.59 | ▼ 09:30 equity $8,586.11 vs yday $8,588.17 (-2.06) | 09:30 open · cash $808.59 (unchanged overnight, no fees) · equity $8,586.11 vs prior close $8,588.17 (-2.06) · 4 name(s) re-marked at the open (per-name table). AVGO×5 yday $357.16 → 09:30 $359.70 +12.70; DELL×4 yday $516.39 → 09:30 $513.78 -10.44; HPE×43 yday $54.44 → 09:30 $53.85 -25.37; CIEN×5 yday $317.46 → 09:30 $321.67 +21.05 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 1 | $263.36 | $1.99 | — | $543.24 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $269.53 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 1 | $236.82 | $1.99 | — | $304.43 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+8.1; leftover $269.53 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 1 | $137.35 | $1.38 | — | $165.70 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+5.4; leftover $269.53 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $165.70 | ▼ close $8,547.66 vs 09:30 $8,586.11 (session -33.09) | 16:00 close · cash $165.70 · equity $8,547.66 vs 09:30 $8,586.11 (-38.45; session marks -33.09) · 7 name(s) marked open→close (per-name table). AVGO×5 09:30 $359.70 → close $357.90 -9.00; DELL×4 09:30 $513.78 → close $524.14 +41.44; HPE×43 09:30 $53.85 → close $52.00 -79.55; CIEN×5 09:30 $321.67 → close $321.00 -3.35; CRM×1 09:30 $263.36 → close $259.23 -4.13; BE×1 09:30 $236.82 → close $252.87 +16.05; MSTR×1 09:30 $137.35 → close $142.80 +5.45 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $165.70 | ▲ 09:30 equity $8,613.37 vs yday $8,547.66 (+65.71) | 09:30 open · cash $165.70 (unchanged overnight, no fees) · equity $8,613.37 vs prior close $8,547.66 (+65.71) · 7 name(s) re-marked at the open (per-name table). AVGO×5 yday $357.90 → 09:30 $363.68 +28.90; DELL×4 yday $524.14 → 09:30 $521.15 -11.96; HPE×43 yday $52.00 → 09:30 $52.29 +12.47; CIEN×5 yday $321.00 → 09:30 $327.42 +32.10; CRM×1 yday $259.23 → 09:30 $253.72 -5.51; BE×1 yday $252.87 → 09:30 $267.76 +14.89; MSTR×1 yday $142.80 → 09:30 $137.62 -5.18 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $165.70 | ▲ close $8,922.62 vs 09:30 $8,613.37 (session +309.25) | 16:00 close · cash $165.70 · equity $8,922.62 vs 09:30 $8,613.37 (+309.25; session marks +309.25) · 7 name(s) marked open→close (per-name table). AVGO×5 09:30 $363.68 → close $368.56 +24.40; DELL×4 09:30 $521.15 → close $533.88 +50.92; HPE×43 09:30 $52.29 → close $56.03 +160.82; CIEN×5 09:30 $327.42 → close $341.29 +69.35; CRM×1 09:30 $253.72 → close $249.12 -4.60; BE×1 09:30 $267.76 → close $277.22 +9.46; MSTR×1 09:30 $137.62 → close $136.52 -1.10 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $165.70 | ▲ 09:30 equity $8,973.24 vs yday $8,922.62 (+50.62) | 09:30 open · cash $165.70 (unchanged overnight, no fees) · equity $8,973.24 vs prior close $8,922.62 (+50.62) · 7 name(s) re-marked at the open (per-name table). AVGO×5 yday $368.56 → 09:30 $366.23 -11.65; DELL×4 yday $533.88 → 09:30 $538.47 +18.36; HPE×43 yday $56.03 → 09:30 $56.94 +39.13; CIEN×5 yday $341.29 → 09:30 $341.90 +3.05; CRM×1 yday $249.12 → 09:30 $249.78 +0.66; BE×1 yday $277.22 → 09:30 $272.99 -4.23; MSTR×1 yday $136.52 → 09:30 $141.82 +5.30 | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 5 | $366.23 | $2.03 | $+68.42 | $1,994.82 | ▲ +68.42 after sell → book $8,971.21; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 4 | $538.47 | $2.03 | $+204.61 | $4,146.67 | ▲ +204.61 after sell → book $8,969.18; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 43 | $56.94 | $2.15 | $+397.35 | $6,592.94 | ▲ +397.35 after sell → book $8,967.03; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 5 | $341.90 | $2.03 | $-66.98 | $8,300.41 | ▼ -66.98 after sell → book $8,965.00; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,300.41 | ▼ close $8,946.55 vs 09:30 $8,973.24 (session -18.45) | 16:00 close · cash $8,300.41 · equity $8,946.55 vs 09:30 $8,973.24 (-26.69; session marks -18.45) · 3 name(s) marked open→close (per-name table). CRM×1 09:30 $249.78 → close $244.16 -5.62; BE×1 09:30 $272.99 → close $269.28 -3.71; MSTR×1 09:30 $141.82 → close $132.70 -9.12 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,300.41 | ▼ 09:30 equity $8,934.91 vs yday $8,946.55 (-11.64) | 09:30 open · cash $8,300.41 (unchanged overnight, no fees) · equity $8,934.91 vs prior close $8,946.55 (-11.64) · 3 name(s) re-marked at the open (per-name table). CRM×1 yday $244.16 → 09:30 $245.35 +1.19; BE×1 yday $269.28 → 09:30 $260.71 -8.57; MSTR×1 yday $132.70 → 09:30 $128.44 -4.26 | — |
| 2026-09-10 09:30 ET | **SELL** | `CRM` | 1 | $245.35 | $2.01 | $-22.02 | $8,543.75 | ▼ -22.02 after sell → book $8,932.90; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BE` | 1 | $260.71 | $2.01 | $+19.88 | $8,802.44 | ▲ +19.88 after sell → book $8,930.89; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MSTR` | 1 | $128.44 | $1.31 | $-11.59 | $8,929.58 | ▼ -11.59 after sell → book $8,929.58; vs 09:30 mark -1.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,929.58 | ▲ close $8,929.58 vs 09:30 $8,934.91 (session +0.00) | 16:00 close · cash $8,929.58 · no lots left · equity $8,929.58. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,929.58 | ▲ 09:30 equity $8,929.58 vs yday $8,929.58 (-0.00) | 09:30 open · cash $8,929.58 · no holdings · equity $8,929.58 vs prior close $8,929.58 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 36 | $242.17 | $2.10 | — | $209.36 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=-11.1; leftover $8929.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.36 | ▲ close $9,289.64 vs 09:30 $8,929.58 (session +362.16) | 16:00 close · cash $209.36 · equity $9,289.64 vs 09:30 $8,929.58 (+360.06; session marks +362.16) · 1 name(s) marked open→close (per-name table). ADBE×36 09:30 $242.17 → close $252.23 +362.16 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.36 | ▲ 09:30 equity $9,623.72 vs yday $9,289.64 (+334.08) | 09:30 open · cash $209.36 (unchanged overnight, no fees) · equity $9,623.72 vs prior close $9,289.64 (+334.08) · 1 name(s) re-marked at the open (per-name table). ADBE×36 yday $252.23 → 09:30 $261.51 +334.08 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.36 | ▲ close $9,770.96 vs 09:30 $9,623.72 (session +147.24) | 16:00 close · cash $209.36 · equity $9,770.96 vs 09:30 $9,623.72 (+147.24; session marks +147.24) · 1 name(s) marked open→close (per-name table). ADBE×36 09:30 $261.51 → close $265.60 +147.24 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.36 | ▼ 09:30 equity $9,630.56 vs yday $9,770.96 (-140.40) | 09:30 open · cash $209.36 (unchanged overnight, no fees) · equity $9,630.56 vs prior close $9,770.96 (-140.40) · 1 name(s) re-marked at the open (per-name table). ADBE×36 yday $265.60 → 09:30 $261.70 -140.40 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.36 | ▼ close $9,488.72 vs 09:30 $9,630.56 (session -141.84) | 16:00 close · cash $209.36 · equity $9,488.72 vs 09:30 $9,630.56 (-141.84; session marks -141.84) · 1 name(s) marked open→close (per-name table). ADBE×36 09:30 $261.70 → close $257.76 -141.84 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.36 | ▼ 09:30 equity $9,329.60 vs yday $9,488.72 (-159.12) | 09:30 open · cash $209.36 (unchanged overnight, no fees) · equity $9,329.60 vs prior close $9,488.72 (-159.12) · 1 name(s) re-marked at the open (per-name table). ADBE×36 yday $257.76 → 09:30 $253.34 -159.12 | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 36 | $253.34 | $2.18 | $+397.84 | $9,327.42 | ▲ +397.84 after sell → book $9,327.42; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 233 | $39.99 | $3.01 | — | $6.74 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $9327.42 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.74 | ▼ close $8,898.02 vs 09:30 $9,329.60 (session -426.39) | 16:00 close · cash $6.74 · equity $8,898.02 vs 09:30 $9,329.60 (-431.58; session marks -426.39) · 1 name(s) marked open→close (per-name table). SM×233 09:30 $39.99 → close $38.16 -426.39 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.74 | ▼ 09:30 equity $8,760.55 vs yday $8,898.02 (-137.47) | 09:30 open · cash $6.74 (unchanged overnight, no fees) · equity $8,760.55 vs prior close $8,898.02 (-137.47) · 1 name(s) re-marked at the open (per-name table). SM×233 yday $38.16 → 09:30 $37.57 -137.47 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.74 | ▼ close $8,620.75 vs 09:30 $8,760.55 (session -139.80) | 16:00 close · cash $6.74 · equity $8,620.75 vs 09:30 $8,760.55 (-139.80; session marks -139.80) · 1 name(s) marked open→close (per-name table). SM×233 09:30 $37.57 → close $36.97 -139.80 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.74 | ▼ 09:30 equity $8,597.45 vs yday $8,620.75 (-23.30) | 09:30 open · cash $6.74 (unchanged overnight, no fees) · equity $8,597.45 vs prior close $8,620.75 (-23.30) · 1 name(s) re-marked at the open (per-name table). SM×233 yday $36.97 → 09:30 $36.87 -23.30 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.74 | ▲ close $8,620.75 vs 09:30 $8,597.45 (session +23.30) | 16:00 close · cash $6.74 · equity $8,620.75 vs 09:30 $8,597.45 (+23.30; session marks +23.30) · 1 name(s) marked open→close (per-name table). SM×233 09:30 $36.87 → close $36.97 +23.30 | — |

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
| 2026-08-26 | `FNV` | cash | leftover split 98.18 < 1 share @ 267.02 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 16.36 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 16.36 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 16.36 < 1 share @ 1746.53 |
| 2026-08-27 | `LRCX` | cash | leftover split 16.36 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 16.36 < 1 share @ 222.86 |
| 2026-08-27 | `RRC` | cash | leftover split 16.36 < 1 share @ 41.44 |
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
| 2026-09-04 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `LITE` | cash | leftover split 6.74 < 1 share @ 934.88 |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `SM` | 233 | 2026-09-16 @ $39.99 | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $9327.42 |
