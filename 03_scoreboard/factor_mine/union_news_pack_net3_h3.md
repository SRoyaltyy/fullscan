# Factor mine action — `union_news_pack_net3_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 and camera net ≥ 3

Cash book **-15.53%** ($8,447) · signal-only (no cash/fees) was -16.71%. Starts YES **7/27**. Fills 38 · skips 71 · realized $-1552.71.

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
- Must-have: camera net (+G −R) is at least 3.
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
- **Gate** `news_box=good,cam_net_min=3` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,447.31.

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
| 2026-08-28 | `KEYS` | 5 | — | $324.41 | +0.00 | $319.97 | -22.20 | -22.20 | +0.00 | -22.20 |
| 2026-08-28 | `CIEN` | 4 | — | $400.42 | +0.00 | $378.44 | -87.92 | -87.92 | +0.00 | -87.92 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 7 | — | $240.22 | +0.00 | $236.98 | -22.68 | -22.68 | +0.00 | -22.68 |
| 2026-08-28 | `ADSK` | 6 | — | $261.16 | +0.00 | $260.66 | -3.00 | -3.00 | +0.00 | -3.00 |
| 2026-08-31 | `KEYS` | 5 | $319.97 | $322.49 | +12.60 | $322.70 | +1.05 | +13.65 | -9.60 | -8.55 |
| 2026-08-31 | `CIEN` | 4 | $378.44 | $378.44 | +0.00 | $382.80 | +17.44 | +17.44 | -87.92 | -70.48 |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | $1267.77 | +5.87 | +11.51 | -44.13 | -38.26 |
| 2026-08-31 | `DDOG` | 7 | $236.98 | $233.97 | -21.10 | $237.04 | +21.52 | +0.42 | -43.78 | -22.26 |
| 2026-08-31 | `ADSK` | 6 | $260.66 | $257.71 | -17.70 | $258.53 | +4.92 | -12.78 | -20.70 | -15.78 |
| 2026-09-01 | `KEYS` | 5 | $322.70 | $321.47 | -6.15 | $319.27 | -11.00 | -17.15 | -14.70 | -25.70 |
| 2026-09-01 | `CIEN` | 4 | $382.80 | $376.89 | -23.64 | $360.33 | -66.24 | -89.88 | -94.12 | -160.36 |
| 2026-09-01 | `MPWR` | 1 | $1267.77 | $1245.11 | -22.66 | $1225.96 | -19.15 | -41.81 | -60.92 | -80.07 |
| 2026-09-01 | `DDOG` | 7 | $237.04 | $232.88 | -29.12 | $223.84 | -63.28 | -92.40 | -51.38 | -114.66 |
| 2026-09-01 | `ADSK` | 6 | $258.53 | $253.48 | -30.30 | $247.69 | -34.74 | -65.04 | -46.08 | -80.82 |
| 2026-09-02 | `KEYS` | 5 | $319.27 | $318.04 | -6.15 | — | +0.00 | -6.15 | -31.85 | — |
| 2026-09-02 | `CIEN` | 4 | $360.33 | $357.25 | -12.32 | — | +0.00 | -12.32 | -172.68 | — |
| 2026-09-02 | `MPWR` | 1 | $1225.96 | $1224.92 | -1.04 | — | +0.00 | -1.04 | -81.11 | — |
| 2026-09-02 | `DDOG` | 7 | $223.84 | $219.46 | -30.66 | — | +0.00 | -30.66 | -145.32 | — |
| 2026-09-02 | `ADSK` | 6 | $247.69 | $246.70 | -5.94 | — | +0.00 | -5.94 | -86.76 | — |
| 2026-09-03 | `AVGO` | 7 | — | $351.74 | +0.00 | $357.16 | +37.94 | +37.94 | +0.00 | +37.94 |
| 2026-09-03 | `DELL` | 5 | — | $486.31 | +0.00 | $516.39 | +150.40 | +150.40 | +0.00 | +150.40 |
| 2026-09-03 | `HPE` | 57 | — | $47.60 | +0.00 | $54.44 | +389.88 | +389.88 | +0.00 | +389.88 |
| 2026-09-04 | `AVGO` | 7 | $357.16 | $359.70 | +17.78 | $357.90 | -12.60 | +5.18 | +55.72 | +43.12 |
| 2026-09-04 | `DELL` | 5 | $516.39 | $513.78 | -13.05 | $524.14 | +51.80 | +38.75 | +137.35 | +189.15 |
| 2026-09-04 | `HPE` | 57 | $54.44 | $53.85 | -33.63 | $52.00 | -105.45 | -139.08 | +356.25 | +250.80 |
| 2026-09-04 | `CRM` | 1 | — | $263.36 | +0.00 | $259.23 | -4.13 | -4.13 | +0.00 | -4.13 |
| 2026-09-04 | `BE` | 1 | — | $236.82 | +0.00 | $252.87 | +16.05 | +16.05 | +0.00 | +16.05 |
| 2026-09-08 | `AVGO` | 7 | $357.90 | $363.68 | +40.46 | $368.56 | +34.16 | +74.62 | +83.58 | +117.74 |
| 2026-09-08 | `DELL` | 5 | $524.14 | $521.15 | -14.95 | $533.88 | +63.65 | +48.70 | +174.20 | +237.85 |
| 2026-09-08 | `HPE` | 57 | $52.00 | $52.29 | +16.53 | $56.03 | +213.18 | +229.71 | +267.33 | +480.51 |
| 2026-09-08 | `CRM` | 1 | $259.23 | $253.72 | -5.51 | $249.12 | -4.60 | -10.11 | -9.64 | -14.24 |
| 2026-09-08 | `BE` | 1 | $252.87 | $267.76 | +14.89 | $277.22 | +9.46 | +24.35 | +30.94 | +40.40 |
| 2026-09-09 | `AVGO` | 7 | $368.56 | $366.23 | -16.31 | — | +0.00 | -16.31 | +101.43 | — |
| 2026-09-09 | `DELL` | 5 | $533.88 | $538.47 | +22.95 | — | +0.00 | +22.95 | +260.80 | — |
| 2026-09-09 | `HPE` | 57 | $56.03 | $56.94 | +51.87 | — | +0.00 | +51.87 | +532.38 | — |
| 2026-09-09 | `CRM` | 1 | $249.12 | $249.78 | +0.66 | $244.16 | -5.62 | -4.96 | -13.58 | -19.20 |
| 2026-09-09 | `BE` | 1 | $277.22 | $272.99 | -4.23 | $269.28 | -3.71 | -7.94 | +36.17 | +32.46 |
| 2026-09-10 | `CRM` | 1 | $244.16 | $245.35 | +1.19 | — | +0.00 | +1.19 | -18.01 | — |
| 2026-09-10 | `BE` | 1 | $269.28 | $260.71 | -8.57 | — | +0.00 | -8.57 | +23.89 | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `QCOM` | 24 | — | $189.17 | +0.00 | $184.84 | -103.92 | -103.92 | +0.00 | -103.92 |
| 2026-09-16 | `SM` | 114 | — | $39.99 | +0.00 | $38.16 | -208.62 | -208.62 | +0.00 | -208.62 |
| 2026-09-17 | `QCOM` | 24 | $184.84 | $190.35 | +132.24 | $188.71 | -39.36 | +92.88 | +28.32 | -11.04 |
| 2026-09-17 | `SM` | 114 | $38.16 | $37.57 | -67.26 | $36.97 | -68.40 | -135.66 | -275.88 | -344.28 |
| 2026-09-18 | `QCOM` | 24 | $188.71 | $191.34 | +63.12 | $177.72 | -326.88 | -263.76 | +52.08 | -274.80 |
| 2026-09-18 | `SM` | 114 | $36.97 | $36.87 | -11.40 | $36.97 | +11.40 | +0.00 | -355.68 | -344.28 |
| 2026-09-21 | `QCOM` | 24 | $177.72 | $180.61 | +69.36 | — | +0.00 | +69.36 | -205.44 | — |
| 2026-09-21 | `SM` | 114 | $36.97 | $35.91 | -120.84 | — | +0.00 | -120.84 | -465.12 | — |

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
| 2026-08-28 | +0.75 | $98.18 | AU×36, FCX×56 | $8,788.94 | +36.84 | -185.57 | KEYS, CIEN, MPWR, DDOG, ADSK | AU, FCX | $996.32 | $8,589.01 | KEYS×5, CIEN×4, MPWR×1, DDOG×7, ADSK×6 |
| 2026-08-31 | -5.85 | $996.32 | KEYS×5, CIEN×4, MPWR×1, DDOG×7, ADSK×6 | $8,568.45 | -20.56 | +50.80 | — | — | $996.32 | $8,619.25 | KEYS×5, CIEN×4, MPWR×1, DDOG×7, ADSK×6 |
| 2026-09-01 | -6.30 | $996.32 | KEYS×5, CIEN×4, MPWR×1, DDOG×7, ADSK×6 | $8,507.38 | -111.87 | -194.41 | — | — | $996.32 | $8,312.97 | KEYS×5, CIEN×4, MPWR×1, DDOG×7, ADSK×6 |
| 2026-09-02 | -3.83 | $996.32 | KEYS×5, CIEN×4, MPWR×1, DDOG×7, ADSK×6 | $8,256.86 | -56.11 | +0.00 | — | KEYS, CIEN, MPWR, DDOG, ADSK | $8,246.73 | $8,246.73 | — |
| 2026-09-03 | -0.90 | $8,246.73 | — | $8,246.73 | +0.00 | +578.22 | AVGO, DELL, HPE | — | $633.63 | $8,818.78 | AVGO×7, DELL×5, HPE×57 |
| 2026-09-04 | +2.25 | $633.63 | AVGO×7, DELL×5, HPE×57 | $8,789.88 | -28.90 | -54.33 | CRM, BE | — | $129.46 | $8,731.56 | AVGO×7, DELL×5, HPE×57, CRM×1, BE×1 |
| 2026-09-08 | -11.47 | $129.46 | AVGO×7, DELL×5, HPE×57, CRM×1, BE×1 | $8,782.98 | +51.42 | +315.85 | — | — | $129.46 | $9,098.83 | AVGO×7, DELL×5, HPE×57, CRM×1, BE×1 |
| 2026-09-09 | -13.95 | $129.46 | AVGO×7, DELL×5, HPE×57, CRM×1, BE×1 | $9,153.77 | +54.94 | -9.33 | — | AVGO, DELL, HPE | $8,624.72 | $9,138.16 | CRM×1, BE×1 |
| 2026-09-10 | -13.28 | $8,624.72 | CRM×1, BE×1 | $9,130.78 | -7.38 | +0.00 | — | CRM, BE | $9,126.76 | $9,126.76 | — |
| 2026-09-11 | +0.50 | $9,126.76 | — | $9,126.76 | -0.00 | +0.00 | — | — | $9,126.76 | $9,126.76 | — |
| 2026-09-14 | -11.00 | $9,126.76 | — | $9,126.76 | -0.00 | +0.00 | — | — | $9,126.76 | $9,126.76 | — |
| 2026-09-15 | -3.84 | $9,126.76 | — | $9,126.76 | -0.00 | +0.00 | — | — | $9,126.76 | $9,126.76 | — |
| 2026-09-16 | +5.30 | $9,126.76 | — | $9,126.76 | -0.00 | -312.54 | QCOM, SM | — | $23.42 | $8,809.82 | QCOM×24, SM×114 |
| 2026-09-17 | +7.38 | $23.42 | QCOM×24, SM×114 | $8,874.80 | +64.98 | -107.76 | — | — | $23.42 | $8,767.04 | QCOM×24, SM×114 |
| 2026-09-18 | +4.86 | $23.42 | QCOM×24, SM×114 | $8,818.76 | +51.72 | -315.48 | — | — | $23.42 | $8,503.28 | QCOM×24, SM×114 |
| 2026-09-21 | +12.87 | $23.42 | QCOM×24, SM×114 | $8,451.80 | -51.48 | +0.00 | — | QCOM, SM | $8,447.31 | $8,447.31 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 27 | $120.00 | $2.07 | — | $6,757.93 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+0.6; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 9 | $359.83 | $2.02 | — | $3,517.44 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+5.9; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 22 | $146.90 | $2.06 | — | $283.59 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+3.6; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $283.59 | ▲ close $10,215.59 vs 09:30 $10,000.00 (session +221.73) | 16:00 close · cash $283.59 · equity $10,215.59 vs 09:30 $10,000.00 (+215.59; session marks +221.73) · 3 name(s) marked open→close (per-name table). NRG×27 09:30 $120.00 → close $126.24 +168.48; TLN×9 09:30 $359.83 → close $362.74 +26.19; VST×22 09:30 $146.90 → close $148.13 +27.06 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $283.59 | ▲ 09:30 equity $10,320.45 vs yday $10,215.59 (+104.86) | 09:30 open · cash $283.59 (unchanged overnight, no fees) · equity $10,320.45 vs prior close $10,215.59 (+104.86) · 3 name(s) re-marked at the open (per-name table). NRG×27 yday $126.24 → 09:30 $127.40 +31.32; TLN×9 yday $362.74 → 09:30 $367.88 +46.26; VST×22 yday $148.13 → 09:30 $149.37 +27.28 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 2 | $46.18 | $0.93 | — | $190.30 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+6.7; leftover $94.53 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
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
| 2026-08-20 09:30 ET | **BUY** | `APA` | 210 | $44.76 | $2.71 | — | $1.45 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $9403.76 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.45 | ▼ close $9,323.35 vs 09:30 $9,404.77 (session -77.70) | 16:00 close · cash $1.45 · equity $9,323.35 vs 09:30 $9,404.77 (-81.42; session marks -77.70) · 1 name(s) marked open→close (per-name table). APA×210 09:30 $44.76 → close $44.39 -77.70 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.45 | ▲ 09:30 equity $9,350.65 vs yday $9,323.35 (+27.30) | 09:30 open · cash $1.45 (unchanged overnight, no fees) · equity $9,350.65 vs prior close $9,323.35 (+27.30) · 1 name(s) re-marked at the open (per-name table). APA×210 yday $44.39 → 09:30 $44.52 +27.30 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.45 | ▼ close $9,113.35 vs 09:30 $9,350.65 (session -237.30) | 16:00 close · cash $1.45 · equity $9,113.35 vs 09:30 $9,350.65 (-237.30; session marks -237.30) · 1 name(s) marked open→close (per-name table). APA×210 09:30 $44.52 → close $43.39 -237.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.45 | ▼ 09:30 equity $9,016.75 vs yday $9,113.35 (-96.60) | 09:30 open · cash $1.45 (unchanged overnight, no fees) · equity $9,016.75 vs prior close $9,113.35 (-96.60) · 1 name(s) re-marked at the open (per-name table). APA×210 yday $43.39 → 09:30 $42.93 -96.60 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.45 | ▲ close $9,023.05 vs 09:30 $9,016.75 (session +6.30) | 16:00 close · cash $1.45 · equity $9,023.05 vs 09:30 $9,016.75 (+6.30; session marks +6.30) · 1 name(s) marked open→close (per-name table). APA×210 09:30 $42.93 → close $42.96 +6.30 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.45 | ▼ 09:30 equity $8,691.25 vs yday $9,023.05 (-331.80) | 09:30 open · cash $1.45 (unchanged overnight, no fees) · equity $8,691.25 vs prior close $9,023.05 (-331.80) · 1 name(s) re-marked at the open (per-name table). APA×210 yday $42.96 → 09:30 $41.38 -331.80 | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 210 | $41.38 | $2.81 | $-715.32 | $8,688.44 | ▼ -715.32 after sell → book $8,688.44; vs 09:30 mark -2.81 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 36 | $118.52 | $2.10 | — | $4,419.62 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $4344.22 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 56 | $77.13 | $2.16 | — | $98.18 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $4344.22 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.18 | ▲ close $9,015.18 vs 09:30 $8,691.25 (session +331.00) | 16:00 close · cash $98.18 · equity $9,015.18 vs 09:30 $8,691.25 (+323.93; session marks +331.00) · 2 name(s) marked open→close (per-name table). AU×36 09:30 $118.52 → close $123.39 +175.32; FCX×56 09:30 $77.13 → close $79.91 +155.68 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.18 | ▼ 09:30 equity $8,854.02 vs yday $9,015.18 (-161.16) | 09:30 open · cash $98.18 (unchanged overnight, no fees) · equity $8,854.02 vs prior close $9,015.18 (-161.16) · 2 name(s) re-marked at the open (per-name table). AU×36 yday $123.39 → 09:30 $119.80 -129.24; FCX×56 yday $79.91 → 09:30 $79.34 -31.92 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.18 | ▼ close $8,774.14 vs 09:30 $8,854.02 (session -79.88) | 16:00 close · cash $98.18 · equity $8,774.14 vs 09:30 $8,854.02 (-79.88; session marks -79.88) · 2 name(s) marked open→close (per-name table). AU×36 09:30 $119.80 → close $118.11 -60.84; FCX×56 09:30 $79.34 → close $79.00 -19.04 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.18 | ▼ 09:30 equity $8,739.42 vs yday $8,774.14 (-34.72) | 09:30 open · cash $98.18 (unchanged overnight, no fees) · equity $8,739.42 vs prior close $8,774.14 (-34.72) · 2 name(s) re-marked at the open (per-name table). AU×36 yday $118.11 → 09:30 $117.41 -25.20; FCX×56 yday $79.00 → 09:30 $78.83 -9.52 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.18 | ▲ close $8,752.10 vs 09:30 $8,739.42 (session +12.68) | 16:00 close · cash $98.18 · equity $8,752.10 vs 09:30 $8,739.42 (+12.68; session marks +12.68) · 2 name(s) marked open→close (per-name table). AU×36 09:30 $117.41 → close $118.40 +35.64; FCX×56 09:30 $78.83 → close $78.42 -22.96 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.18 | ▲ 09:30 equity $8,788.94 vs yday $8,752.10 (+36.84) | 09:30 open · cash $98.18 (unchanged overnight, no fees) · equity $8,788.94 vs prior close $8,752.10 (+36.84) · 2 name(s) re-marked at the open (per-name table). AU×36 yday $118.40 → 09:30 $119.19 +28.44; FCX×56 yday $78.42 → 09:30 $78.57 +8.40 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 36 | $119.19 | $2.14 | $+19.88 | $4,386.88 | ▲ +19.88 after sell → book $8,786.80; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 56 | $78.57 | $2.20 | $+76.28 | $8,784.60 | ▲ +76.28 after sell → book $8,784.60; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 5 | $324.41 | $2.00 | — | $7,160.54 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1756.92 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 4 | $400.42 | $2.00 | — | $5,556.86 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1756.92 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,248.84 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1756.92 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 7 | $240.22 | $2.01 | — | $2,565.29 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1756.92 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 6 | $261.16 | $2.01 | — | $996.32 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; ret5=+7.8; leftover $1756.92 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $996.32 | ▼ close $8,589.01 vs 09:30 $8,788.94 (session -185.57) | 16:00 close · cash $996.32 · equity $8,589.01 vs 09:30 $8,788.94 (-199.93; session marks -185.57) · 5 name(s) marked open→close (per-name table). KEYS×5 09:30 $324.41 → close $319.97 -22.20; CIEN×4 09:30 $400.42 → close $378.44 -87.92; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×7 09:30 $240.22 → close $236.98 -22.68; ADSK×6 09:30 $261.16 → close $260.66 -3.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $996.32 | ▼ 09:30 equity $8,568.45 vs yday $8,589.01 (-20.56) | 09:30 open · cash $996.32 (unchanged overnight, no fees) · equity $8,568.45 vs prior close $8,589.01 (-20.56) · 5 name(s) re-marked at the open (per-name table). KEYS×5 yday $319.97 → 09:30 $322.49 +12.60; CIEN×4 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×7 yday $236.98 → 09:30 $233.97 -21.10; ADSK×6 yday $260.66 → 09:30 $257.71 -17.70 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $996.32 | ▲ close $8,619.25 vs 09:30 $8,568.45 (session +50.80) | 16:00 close · cash $996.32 · equity $8,619.25 vs 09:30 $8,568.45 (+50.80; session marks +50.80) · 5 name(s) marked open→close (per-name table). KEYS×5 09:30 $322.49 → close $322.70 +1.05; CIEN×4 09:30 $378.44 → close $382.80 +17.44; MPWR×1 09:30 $1261.90 → close $1267.77 +5.87; DDOG×7 09:30 $233.97 → close $237.04 +21.52; ADSK×6 09:30 $257.71 → close $258.53 +4.92 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $996.32 | ▼ 09:30 equity $8,507.38 vs yday $8,619.25 (-111.87) | 09:30 open · cash $996.32 (unchanged overnight, no fees) · equity $8,507.38 vs prior close $8,619.25 (-111.87) · 5 name(s) re-marked at the open (per-name table). KEYS×5 yday $322.70 → 09:30 $321.47 -6.15; CIEN×4 yday $382.80 → 09:30 $376.89 -23.64; MPWR×1 yday $1267.77 → 09:30 $1245.11 -22.66; DDOG×7 yday $237.04 → 09:30 $232.88 -29.12; ADSK×6 yday $258.53 → 09:30 $253.48 -30.30 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $996.32 | ▼ close $8,312.97 vs 09:30 $8,507.38 (session -194.41) | 16:00 close · cash $996.32 · equity $8,312.97 vs 09:30 $8,507.38 (-194.41; session marks -194.41) · 5 name(s) marked open→close (per-name table). KEYS×5 09:30 $321.47 → close $319.27 -11.00; CIEN×4 09:30 $376.89 → close $360.33 -66.24; MPWR×1 09:30 $1245.11 → close $1225.96 -19.15; DDOG×7 09:30 $232.88 → close $223.84 -63.28; ADSK×6 09:30 $253.48 → close $247.69 -34.74 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $996.32 | ▼ 09:30 equity $8,256.86 vs yday $8,312.97 (-56.11) | 09:30 open · cash $996.32 (unchanged overnight, no fees) · equity $8,256.86 vs prior close $8,312.97 (-56.11) · 5 name(s) re-marked at the open (per-name table). KEYS×5 yday $319.27 → 09:30 $318.04 -6.15; CIEN×4 yday $360.33 → 09:30 $357.25 -12.32; MPWR×1 yday $1225.96 → 09:30 $1224.92 -1.04; DDOG×7 yday $223.84 → 09:30 $219.46 -30.66; ADSK×6 yday $247.69 → 09:30 $246.70 -5.94 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 5 | $318.04 | $2.03 | $-35.88 | $2,584.49 | ▼ -35.88 after sell → book $8,254.83; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 4 | $357.25 | $2.02 | $-176.71 | $4,011.47 | ▼ -176.71 after sell → book $8,252.81; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1224.92 | $2.01 | $-85.12 | $5,234.38 | ▼ -85.12 after sell → book $8,250.80; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 7 | $219.46 | $2.03 | $-149.36 | $6,768.56 | ▼ -149.36 after sell → book $8,248.76; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 6 | $246.70 | $2.03 | $-90.80 | $8,246.73 | ▼ -90.80 after sell → book $8,246.73; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,246.73 | ▲ close $8,246.73 vs 09:30 $8,256.86 (session +0.00) | 16:00 close · cash $8,246.73 · no lots left · equity $8,246.73. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,246.73 | ▲ 09:30 equity $8,246.73 vs yday $8,246.73 (+0.00) | 09:30 open · cash $8,246.73 · no holdings · equity $8,246.73 vs prior close $8,246.73 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 7 | $351.74 | $2.01 | — | $5,782.54 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $2748.91 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 5 | $486.31 | $2.00 | — | $3,348.99 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $2748.91 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 57 | $47.60 | $2.16 | — | $633.63 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $2748.91 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $633.63 | ▲ close $8,818.78 vs 09:30 $8,246.73 (session +578.22) | 16:00 close · cash $633.63 · equity $8,818.78 vs 09:30 $8,246.73 (+572.05; session marks +578.22) · 3 name(s) marked open→close (per-name table). AVGO×7 09:30 $351.74 → close $357.16 +37.94; DELL×5 09:30 $486.31 → close $516.39 +150.40; HPE×57 09:30 $47.60 → close $54.44 +389.88 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $633.63 | ▼ 09:30 equity $8,789.88 vs yday $8,818.78 (-28.90) | 09:30 open · cash $633.63 (unchanged overnight, no fees) · equity $8,789.88 vs prior close $8,818.78 (-28.90) · 3 name(s) re-marked at the open (per-name table). AVGO×7 yday $357.16 → 09:30 $359.70 +17.78; DELL×5 yday $516.39 → 09:30 $513.78 -13.05; HPE×57 yday $54.44 → 09:30 $53.85 -33.63 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 1 | $263.36 | $1.99 | — | $368.27 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $316.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 1 | $236.82 | $1.99 | — | $129.46 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list ohlc_hot; ret5=+8.1; leftover $316.81 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.46 | ▼ close $8,731.56 vs 09:30 $8,789.88 (session -54.33) | 16:00 close · cash $129.46 · equity $8,731.56 vs 09:30 $8,789.88 (-58.32; session marks -54.33) · 5 name(s) marked open→close (per-name table). AVGO×7 09:30 $359.70 → close $357.90 -12.60; DELL×5 09:30 $513.78 → close $524.14 +51.80; HPE×57 09:30 $53.85 → close $52.00 -105.45; CRM×1 09:30 $263.36 → close $259.23 -4.13; BE×1 09:30 $236.82 → close $252.87 +16.05 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.46 | ▲ 09:30 equity $8,782.98 vs yday $8,731.56 (+51.42) | 09:30 open · cash $129.46 (unchanged overnight, no fees) · equity $8,782.98 vs prior close $8,731.56 (+51.42) · 5 name(s) re-marked at the open (per-name table). AVGO×7 yday $357.90 → 09:30 $363.68 +40.46; DELL×5 yday $524.14 → 09:30 $521.15 -14.95; HPE×57 yday $52.00 → 09:30 $52.29 +16.53; CRM×1 yday $259.23 → 09:30 $253.72 -5.51; BE×1 yday $252.87 → 09:30 $267.76 +14.89 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.46 | ▲ close $9,098.83 vs 09:30 $8,782.98 (session +315.85) | 16:00 close · cash $129.46 · equity $9,098.83 vs 09:30 $8,782.98 (+315.85; session marks +315.85) · 5 name(s) marked open→close (per-name table). AVGO×7 09:30 $363.68 → close $368.56 +34.16; DELL×5 09:30 $521.15 → close $533.88 +63.65; HPE×57 09:30 $52.29 → close $56.03 +213.18; CRM×1 09:30 $253.72 → close $249.12 -4.60; BE×1 09:30 $267.76 → close $277.22 +9.46 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.46 | ▲ 09:30 equity $9,153.77 vs yday $9,098.83 (+54.94) | 09:30 open · cash $129.46 (unchanged overnight, no fees) · equity $9,153.77 vs prior close $9,098.83 (+54.94) · 5 name(s) re-marked at the open (per-name table). AVGO×7 yday $368.56 → 09:30 $366.23 -16.31; DELL×5 yday $533.88 → 09:30 $538.47 +22.95; HPE×57 yday $56.03 → 09:30 $56.94 +51.87; CRM×1 yday $249.12 → 09:30 $249.78 +0.66; BE×1 yday $277.22 → 09:30 $272.99 -4.23 | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 7 | $366.23 | $2.04 | $+97.38 | $2,691.03 | ▲ +97.38 after sell → book $9,151.73; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 5 | $538.47 | $2.04 | $+256.76 | $5,381.34 | ▲ +256.76 after sell → book $9,149.69; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 57 | $56.94 | $2.20 | $+528.02 | $8,624.72 | ▲ +528.02 after sell → book $9,147.49; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,624.72 | ▼ close $9,138.16 vs 09:30 $9,153.77 (session -9.33) | 16:00 close · cash $8,624.72 · equity $9,138.16 vs 09:30 $9,153.77 (-15.61; session marks -9.33) · 2 name(s) marked open→close (per-name table). CRM×1 09:30 $249.78 → close $244.16 -5.62; BE×1 09:30 $272.99 → close $269.28 -3.71 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,624.72 | ▼ 09:30 equity $9,130.78 vs yday $9,138.16 (-7.38) | 09:30 open · cash $8,624.72 (unchanged overnight, no fees) · equity $9,130.78 vs prior close $9,138.16 (-7.38) · 2 name(s) re-marked at the open (per-name table). CRM×1 yday $244.16 → 09:30 $245.35 +1.19; BE×1 yday $269.28 → 09:30 $260.71 -8.57 | — |
| 2026-09-10 09:30 ET | **SELL** | `CRM` | 1 | $245.35 | $2.01 | $-22.02 | $8,868.06 | ▼ -22.02 after sell → book $9,128.77; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BE` | 1 | $260.71 | $2.01 | $+19.88 | $9,126.76 | ▲ +19.88 after sell → book $9,126.76; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,126.76 | ▲ close $9,126.76 vs 09:30 $9,130.78 (session +0.00) | 16:00 close · cash $9,126.76 · no lots left · equity $9,126.76. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,126.76 | ▲ 09:30 equity $9,126.76 vs yday $9,126.76 (-0.00) | 09:30 open · cash $9,126.76 · no holdings · equity $9,126.76 vs prior close $9,126.76 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,126.76 | ▲ close $9,126.76 vs 09:30 $9,126.76 (session +0.00) | 16:00 close · cash $9,126.76 · no lots left · equity $9,126.76. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,126.76 | ▲ 09:30 equity $9,126.76 vs yday $9,126.76 (-0.00) | 09:30 open · cash $9,126.76 · no holdings · equity $9,126.76 vs prior close $9,126.76 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,126.76 | ▲ close $9,126.76 vs 09:30 $9,126.76 (session +0.00) | 16:00 close · cash $9,126.76 · no lots left · equity $9,126.76. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,126.76 | ▲ 09:30 equity $9,126.76 vs yday $9,126.76 (-0.00) | 09:30 open · cash $9,126.76 · no holdings · equity $9,126.76 vs prior close $9,126.76 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,126.76 | ▲ close $9,126.76 vs 09:30 $9,126.76 (session +0.00) | 16:00 close · cash $9,126.76 · no lots left · equity $9,126.76. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,126.76 | ▲ 09:30 equity $9,126.76 vs yday $9,126.76 (-0.00) | 09:30 open · cash $9,126.76 · no holdings · equity $9,126.76 vs prior close $9,126.76 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 24 | $189.17 | $2.06 | — | $4,584.61 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $4563.38 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 114 | $39.99 | $2.33 | — | $23.42 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $4563.38 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.42 | ▼ close $8,809.82 vs 09:30 $9,126.76 (session -312.54) | 16:00 close · cash $23.42 · equity $8,809.82 vs 09:30 $9,126.76 (-316.94; session marks -312.54) · 2 name(s) marked open→close (per-name table). QCOM×24 09:30 $189.17 → close $184.84 -103.92; SM×114 09:30 $39.99 → close $38.16 -208.62 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.42 | ▲ 09:30 equity $8,874.80 vs yday $8,809.82 (+64.98) | 09:30 open · cash $23.42 (unchanged overnight, no fees) · equity $8,874.80 vs prior close $8,809.82 (+64.98) · 2 name(s) re-marked at the open (per-name table). QCOM×24 yday $184.84 → 09:30 $190.35 +132.24; SM×114 yday $38.16 → 09:30 $37.57 -67.26 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.42 | ▼ close $8,767.04 vs 09:30 $8,874.80 (session -107.76) | 16:00 close · cash $23.42 · equity $8,767.04 vs 09:30 $8,874.80 (-107.76; session marks -107.76) · 2 name(s) marked open→close (per-name table). QCOM×24 09:30 $190.35 → close $188.71 -39.36; SM×114 09:30 $37.57 → close $36.97 -68.40 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.42 | ▲ 09:30 equity $8,818.76 vs yday $8,767.04 (+51.72) | 09:30 open · cash $23.42 (unchanged overnight, no fees) · equity $8,818.76 vs prior close $8,767.04 (+51.72) · 2 name(s) re-marked at the open (per-name table). QCOM×24 yday $188.71 → 09:30 $191.34 +63.12; SM×114 yday $36.97 → 09:30 $36.87 -11.40 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.42 | ▼ close $8,503.28 vs 09:30 $8,818.76 (session -315.48) | 16:00 close · cash $23.42 · equity $8,503.28 vs 09:30 $8,818.76 (-315.48; session marks -315.48) · 2 name(s) marked open→close (per-name table). QCOM×24 09:30 $191.34 → close $177.72 -326.88; SM×114 09:30 $36.87 → close $36.97 +11.40 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.42 | ▼ 09:30 equity $8,451.80 vs yday $8,503.28 (-51.48) | 09:30 open · cash $23.42 (unchanged overnight, no fees) · equity $8,451.80 vs prior close $8,503.28 (-51.48) · 2 name(s) re-marked at the open (per-name table). QCOM×24 yday $177.72 → 09:30 $180.61 +69.36; SM×114 yday $36.97 → 09:30 $35.91 -120.84 | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 24 | $180.61 | $2.11 | $-209.61 | $4,355.96 | ▼ -209.61 after sell → book $8,449.70; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 114 | $35.91 | $2.38 | $-469.84 | $8,447.31 | ▼ -469.84 after sell → book $8,447.31; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,447.31 | ▲ close $8,447.31 vs 09:30 $8,451.80 (session +0.00) | 16:00 close · cash $8,447.31 · no lots left · equity $8,447.31. | — |

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
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
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
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `LITE` | cash | leftover split 23.42 < 1 share @ 934.88 |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
