# Factor mine action — `union_news_pack_net3_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 and camera net ≥ 3

Cash book **+8.99%** ($10,899) · signal-only (no cash/fees) was +6.42%. Starts YES **15/26**. Fills 74 · skips 21 · realized $+898.87.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news_box=good,cam_net_min=3` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,898.87.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `NRG` | 27 | — | $120.00 | +0.00 | $126.24 | +168.48 | +168.48 | +0.00 | +168.48 |
| 2026-08-14 | `TLN` | 9 | — | $359.83 | +0.00 | $362.74 | +26.19 | +26.19 | +0.00 | +26.19 |
| 2026-08-14 | `VST` | 22 | — | $146.90 | +0.00 | $148.13 | +27.06 | +27.06 | +0.00 | +27.06 |
| 2026-08-17 | `NRG` | 27 | $126.24 | $127.40 | +31.32 | — | +0.00 | +31.32 | +199.80 | — |
| 2026-08-17 | `TLN` | 9 | $362.74 | $367.88 | +46.26 | — | +0.00 | +46.26 | +72.45 | — |
| 2026-08-17 | `VST` | 22 | $148.13 | $149.37 | +27.28 | — | +0.00 | +27.28 | +54.34 | — |
| 2026-08-17 | `DVN` | 74 | — | $46.18 | +0.00 | $47.57 | +102.86 | +102.86 | +0.00 | +102.86 |
| 2026-08-17 | `EOG` | 24 | — | $142.77 | +0.00 | $146.15 | +81.12 | +81.12 | +0.00 | +81.12 |
| 2026-08-17 | `FANG` | 16 | — | $202.70 | +0.00 | $206.29 | +57.44 | +57.44 | +0.00 | +57.44 |
| 2026-08-18 | `DVN` | 74 | $47.57 | $48.00 | +31.82 | — | +0.00 | +31.82 | +134.68 | — |
| 2026-08-18 | `EOG` | 24 | $146.15 | $148.04 | +45.36 | — | +0.00 | +45.36 | +126.48 | — |
| 2026-08-18 | `FANG` | 16 | $206.29 | $208.93 | +42.24 | — | +0.00 | +42.24 | +99.68 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `APA` | 238 | — | $44.76 | +0.00 | $44.39 | -88.06 | -88.06 | +0.00 | -88.06 |
| 2026-08-21 | `APA` | 238 | $44.39 | $44.52 | +30.94 | — | +0.00 | +30.94 | -57.12 | — |
| 2026-08-21 | `AU` | 29 | — | $119.43 | +0.00 | $121.22 | +51.91 | +51.91 | +0.00 | +51.91 |
| 2026-08-21 | `MFC` | 83 | — | $42.48 | +0.00 | $42.51 | +2.49 | +2.49 | +0.00 | +2.49 |
| 2026-08-21 | `DE` | 5 | — | $623.26 | +0.00 | $647.47 | +121.05 | +121.05 | +0.00 | +121.05 |
| 2026-08-24 | `AU` | 29 | $121.22 | $120.51 | -20.59 | — | +0.00 | -20.59 | +31.32 | — |
| 2026-08-24 | `MFC` | 83 | $42.51 | $42.31 | -16.60 | — | +0.00 | -16.60 | -14.11 | — |
| 2026-08-24 | `DE` | 5 | $647.47 | $653.04 | +27.85 | — | +0.00 | +27.85 | +148.90 | — |
| 2026-08-25 | `AU` | 30 | — | $118.52 | +0.00 | $123.39 | +146.10 | +146.10 | +0.00 | +146.10 |
| 2026-08-25 | `FCX` | 46 | — | $77.13 | +0.00 | $79.91 | +127.88 | +127.88 | +0.00 | +127.88 |
| 2026-08-25 | `AMX` | 150 | — | $23.80 | +0.00 | $23.75 | -7.50 | -7.50 | +0.00 | -7.50 |
| 2026-08-26 | `AU` | 30 | $123.39 | $119.80 | -107.70 | — | +0.00 | -107.70 | +38.40 | — |
| 2026-08-26 | `FCX` | 46 | $79.91 | $79.34 | -26.22 | — | +0.00 | -26.22 | +101.66 | — |
| 2026-08-26 | `AMX` | 150 | $23.75 | $23.75 | +0.00 | $23.62 | -19.50 | -19.50 | -7.50 | -27.00 |
| 2026-08-26 | `FNV` | 27 | — | $267.02 | +0.00 | $267.37 | +9.45 | +9.45 | +0.00 | +9.45 |
| 2026-08-27 | `AMX` | 150 | $23.62 | $23.77 | +22.50 | — | +0.00 | +22.50 | -4.50 | — |
| 2026-08-27 | `FNV` | 27 | $267.37 | $267.23 | -3.78 | — | +0.00 | -3.78 | +5.67 | — |
| 2026-08-27 | `ACMR` | 19 | — | $81.65 | +0.00 | $80.49 | -22.04 | -22.04 | +0.00 | -22.04 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `CM` | 13 | — | $118.77 | +0.00 | $114.84 | -51.09 | -51.09 | +0.00 | -51.09 |
| 2026-08-27 | `LRCX` | 4 | — | $318.88 | +0.00 | $318.58 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-27 | `NVDA` | 6 | — | $222.86 | +0.00 | $227.98 | +30.72 | +30.72 | +0.00 | +30.72 |
| 2026-08-27 | `AXTI` | 22 | — | $70.30 | +0.00 | $66.92 | -74.36 | -74.36 | +0.00 | -74.36 |
| 2026-08-28 | `ACMR` | 19 | $80.49 | $79.27 | -23.18 | — | +0.00 | -23.18 | -45.22 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `CM` | 13 | $114.84 | $115.66 | +10.66 | — | +0.00 | +10.66 | -40.43 | — |
| 2026-08-28 | `LRCX` | 4 | $318.58 | $318.03 | -2.20 | — | +0.00 | -2.20 | -3.40 | — |
| 2026-08-28 | `NVDA` | 6 | $227.98 | $227.36 | -3.72 | — | +0.00 | -3.72 | +27.00 | — |
| 2026-08-28 | `AXTI` | 22 | $66.92 | $65.29 | -35.86 | — | +0.00 | -35.86 | -110.22 | — |
| 2026-08-28 | `KEYS` | 6 | — | $324.41 | +0.00 | $319.97 | -26.64 | -26.64 | +0.00 | -26.64 |
| 2026-08-28 | `CIEN` | 5 | — | $400.42 | +0.00 | $378.44 | -109.90 | -109.90 | +0.00 | -109.90 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 8 | — | $240.22 | +0.00 | $236.98 | -25.92 | -25.92 | +0.00 | -25.92 |
| 2026-08-28 | `ADSK` | 8 | — | $261.16 | +0.00 | $260.66 | -4.00 | -4.00 | +0.00 | -4.00 |
| 2026-08-31 | `KEYS` | 6 | $319.97 | $322.49 | +15.12 | — | +0.00 | +15.12 | -11.52 | — |
| 2026-08-31 | `CIEN` | 5 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -109.90 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `DDOG` | 8 | $236.98 | $233.97 | -24.12 | — | +0.00 | -24.12 | -50.04 | — |
| 2026-08-31 | `ADSK` | 8 | $260.66 | $257.71 | -23.60 | — | +0.00 | -23.60 | -27.60 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 9 | — | $351.74 | +0.00 | $357.16 | +48.78 | +48.78 | +0.00 | +48.78 |
| 2026-09-03 | `DELL` | 7 | — | $486.31 | +0.00 | $516.39 | +210.56 | +210.56 | +0.00 | +210.56 |
| 2026-09-03 | `HPE` | 72 | — | $47.60 | +0.00 | $54.44 | +492.48 | +492.48 | +0.00 | +492.48 |
| 2026-09-04 | `AVGO` | 9 | $357.16 | $359.70 | +22.86 | — | +0.00 | +22.86 | +71.64 | — |
| 2026-09-04 | `DELL` | 7 | $516.39 | $513.78 | -18.27 | — | +0.00 | -18.27 | +192.29 | — |
| 2026-09-04 | `HPE` | 72 | $54.44 | $53.85 | -42.48 | $52.00 | -133.20 | -175.68 | +450.00 | +316.80 |
| 2026-09-04 | `CRM` | 6 | — | $263.36 | +0.00 | $259.23 | -24.78 | -24.78 | +0.00 | -24.78 |
| 2026-09-04 | `BE` | 7 | — | $236.82 | +0.00 | $252.87 | +112.35 | +112.35 | +0.00 | +112.35 |
| 2026-09-04 | `AMX` | 78 | — | $23.03 | +0.00 | $23.00 | -2.34 | -2.34 | +0.00 | -2.34 |
| 2026-09-04 | `CIEN` | 5 | — | $321.67 | +0.00 | $321.00 | -3.35 | -3.35 | +0.00 | -3.35 |
| 2026-09-08 | `HPE` | 72 | $52.00 | $52.29 | +20.88 | — | +0.00 | +20.88 | +337.68 | — |
| 2026-09-08 | `CRM` | 6 | $259.23 | $253.72 | -33.06 | — | +0.00 | -33.06 | -57.84 | — |
| 2026-09-08 | `BE` | 7 | $252.87 | $267.76 | +104.23 | — | +0.00 | +104.23 | +216.58 | — |
| 2026-09-08 | `AMX` | 78 | $23.00 | $23.15 | +11.70 | — | +0.00 | +11.70 | +9.36 | — |
| 2026-09-08 | `CIEN` | 5 | $321.00 | $327.42 | +32.10 | — | +0.00 | +32.10 | +28.75 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `BTI` | 198 | — | $56.03 | +0.00 | $55.24 | -156.42 | -156.42 | +0.00 | -156.42 |
| 2026-09-14 | `BTI` | 198 | $55.24 | $57.12 | +372.24 | — | +0.00 | +372.24 | +215.82 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `QCOM` | 19 | — | $189.17 | +0.00 | $184.84 | -82.27 | -82.27 | +0.00 | -82.27 |
| 2026-09-16 | `SM` | 94 | — | $39.99 | +0.00 | $38.16 | -172.02 | -172.02 | +0.00 | -172.02 |
| 2026-09-16 | `AMX` | 163 | — | $23.18 | +0.00 | $22.98 | -32.60 | -32.60 | +0.00 | -32.60 |
| 2026-09-17 | `QCOM` | 19 | $184.84 | $190.35 | +104.69 | — | +0.00 | +104.69 | +22.42 | — |
| 2026-09-17 | `SM` | 94 | $38.16 | $37.57 | -55.46 | — | +0.00 | -55.46 | -227.48 | — |
| 2026-09-17 | `AMX` | 163 | $22.98 | $23.09 | +17.93 | — | +0.00 | +17.93 | -14.67 | — |
| 2026-09-17 | `LITE` | 11 | — | $934.88 | +0.00 | $893.61 | -453.97 | -453.97 | +0.00 | -453.97 |
| 2026-09-18 | `LITE` | 11 | $893.61 | $915.66 | +242.55 | — | +0.00 | +242.55 | -211.42 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +221.73 | NRG, TLN, VST | — | $283.59 | $10,215.59 | NRG×27, TLN×9, VST×22 |
| 2026-08-17 | +2.25 | $283.59 | NRG×27, TLN×9, VST×22 | $10,320.45 | +104.86 | +241.42 | DVN, EOG, FANG | NRG, TLN, VST | $220.88 | $10,549.30 | DVN×74, EOG×24, FANG×16 |
| 2026-08-18 | -6.20 | $220.88 | DVN×74, EOG×24, FANG×16 | $10,668.72 | +119.42 | +0.00 | — | DVN, EOG, FANG | $10,662.29 | $10,662.29 | — |
| 2026-08-19 | -7.20 | $10,662.29 | — | $10,662.29 | +0.00 | +0.00 | — | — | $10,662.29 | $10,662.29 | — |
| 2026-08-20 | +1.12 | $10,662.29 | — | $10,662.29 | +0.00 | -88.06 | APA | — | $6.34 | $10,571.16 | APA×238 |
| 2026-08-21 | +3.25 | $6.34 | APA×238 | $10,602.10 | +30.94 | +175.45 | AU, MFC, DE | APA | $486.98 | $10,768.04 | AU×29, MFC×83, DE×5 |
| 2026-08-24 | -5.17 | $486.98 | AU×29, MFC×83, DE×5 | $10,758.70 | -9.34 | +0.00 | — | AU, MFC, DE | $10,752.26 | $10,752.26 | — |
| 2026-08-25 | +1.80 | $10,752.26 | — | $10,752.26 | -0.00 | +266.48 | AU, FCX, AMX | — | $72.03 | $11,012.09 | AU×30, FCX×46, AMX×150 |
| 2026-08-26 | +2.02 | $72.03 | AU×30, FCX×46, AMX×150 | $10,878.17 | -133.92 | -10.05 | FNV | AU, FCX | $99.77 | $10,861.76 | AMX×150, FNV×27 |
| 2026-08-27 | — | $99.77 | AMX×150, FNV×27 | $10,880.48 | +18.72 | -149.59 | ACMR, MU, CM, LRCX, NVDA, AXTI | AMX, FNV | $2,642.07 | $10,714.13 | ACMR×19, MU×1, CM×13, LRCX×4, NVDA×6, AXTI×22 |
| 2026-08-28 | +0.75 | $2,642.07 | ACMR×19, MU×1, CM×13, LRCX×4, NVDA×6, AXTI×22 | $10,643.73 | -70.40 | -216.23 | KEYS, CIEN, MPWR, DDOG, ADSK | ACMR, MU, CM, LRCX, NVDA, AXTI | $1,355.80 | $10,405.20 | KEYS×6, CIEN×5, MPWR×1, DDOG×8, ADSK×8 |
| 2026-08-31 | -5.85 | $1,355.80 | KEYS×6, CIEN×5, MPWR×1, DDOG×8, ADSK×8 | $10,378.24 | -26.96 | +0.00 | — | KEYS, CIEN, MPWR, DDOG, ADSK | $10,368.09 | $10,368.09 | — |
| 2026-09-01 | -6.30 | $10,368.09 | — | $10,368.09 | -0.00 | +0.00 | — | — | $10,368.09 | $10,368.09 | — |
| 2026-09-02 | -3.83 | $10,368.09 | — | $10,368.09 | -0.00 | +0.00 | — | — | $10,368.09 | $10,368.09 | — |
| 2026-09-03 | -0.90 | $10,368.09 | — | $10,368.09 | -0.00 | +751.82 | AVGO, DELL, HPE | — | $364.82 | $11,113.67 | AVGO×9, DELL×7, HPE×72 |
| 2026-09-04 | +2.25 | $364.82 | AVGO×9, DELL×7, HPE×72 | $11,075.78 | -37.89 | -51.32 | CRM, BE, AMX, CIEN | AVGO, DELL | $543.64 | $11,012.11 | HPE×72, CRM×6, BE×7, AMX×78, CIEN×5 |
| 2026-09-08 | -11.47 | $543.64 | HPE×72, CRM×6, BE×7, AMX×78, CIEN×5 | $11,147.96 | +135.85 | +0.00 | — | HPE, CRM, BE, AMX, CIEN | $11,137.37 | $11,137.37 | — |
| 2026-09-09 | -13.95 | $11,137.37 | — | $11,137.37 | -0.00 | +0.00 | — | — | $11,137.37 | $11,137.37 | — |
| 2026-09-10 | -13.28 | $11,137.37 | — | $11,137.37 | -0.00 | +0.00 | — | — | $11,137.37 | $11,137.37 | — |
| 2026-09-11 | +0.50 | $11,137.37 | — | $11,137.37 | -0.00 | -156.42 | BTI | — | $40.84 | $10,978.36 | BTI×198 |
| 2026-09-14 | -11.00 | $40.84 | BTI×198 | $11,350.60 | +372.24 | +0.00 | — | BTI | $11,347.90 | $11,347.90 | — |
| 2026-09-15 | -3.84 | $11,347.90 | — | $11,347.90 | -0.00 | +0.00 | — | — | $11,347.90 | $11,347.90 | — |
| 2026-09-16 | +5.30 | $11,347.90 | — | $11,347.90 | -0.00 | -286.89 | QCOM, SM, AMX | — | $209.47 | $11,054.21 | QCOM×19, SM×94, AMX×163 |
| 2026-09-17 | +7.38 | $209.47 | QCOM×19, SM×94, AMX×163 | $11,121.37 | +67.16 | -453.97 | LITE | QCOM, SM, AMX | $828.73 | $10,658.44 | LITE×11 |
| 2026-09-18 | +4.86 | $828.73 | LITE×11 | $10,900.99 | +242.55 | +0.00 | — | LITE | $10,898.87 | $10,898.87 | — |

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
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 27 | $127.40 | $2.11 | $+195.62 | $3,721.28 | ▲ +195.62 after sell → book $10,318.34; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 9 | $367.88 | $2.05 | $+68.38 | $7,030.14 | ▲ +68.38 after sell → book $10,316.28; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 22 | $149.37 | $2.09 | $+50.19 | $10,314.19 | ▲ +50.19 after sell → book $10,314.19; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 74 | $46.18 | $2.21 | — | $6,894.66 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+6.7; leftover $3438.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 24 | $142.77 | $2.06 | — | $3,466.12 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3438.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 16 | $202.70 | $2.04 | — | $220.88 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+8.3; leftover $3438.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $220.88 | ▲ close $10,549.30 vs 09:30 $10,320.45 (session +241.42) | 16:00 close · cash $220.88 · equity $10,549.30 vs 09:30 $10,320.45 (+228.85; session marks +241.42) · 3 name(s) marked open→close (per-name table). DVN×74 09:30 $46.18 → close $47.57 +102.86; EOG×24 09:30 $142.77 → close $146.15 +81.12; FANG×16 09:30 $202.70 → close $206.29 +57.44 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $220.88 | ▲ 09:30 equity $10,668.72 vs yday $10,549.30 (+119.42) | 09:30 open · cash $220.88 (unchanged overnight, no fees) · equity $10,668.72 vs prior close $10,549.30 (+119.42) · 3 name(s) re-marked at the open (per-name table). DVN×74 yday $47.57 → 09:30 $48.00 +31.82; EOG×24 yday $146.15 → 09:30 $148.04 +45.36; FANG×16 yday $206.29 → 09:30 $208.93 +42.24 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 74 | $48.00 | $2.25 | $+130.22 | $3,770.63 | ▲ +130.22 after sell → book $10,666.47; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 24 | $148.04 | $2.10 | $+122.32 | $7,321.49 | ▲ +122.32 after sell → book $10,664.37; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 16 | $208.93 | $2.07 | $+95.57 | $10,662.29 | ▲ +95.57 after sell → book $10,662.29; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.29 | ▲ close $10,662.29 vs 09:30 $10,668.72 (session +0.00) | 16:00 close · cash $10,662.29 · no lots left · equity $10,662.29. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.29 | ▲ 09:30 equity $10,662.29 vs yday $10,662.29 (+0.00) | 09:30 open · cash $10,662.29 · no holdings · equity $10,662.29 vs prior close $10,662.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.29 | ▲ close $10,662.29 vs 09:30 $10,662.29 (session +0.00) | 16:00 close · cash $10,662.29 · no lots left · equity $10,662.29. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.29 | ▲ 09:30 equity $10,662.29 vs yday $10,662.29 (+0.00) | 09:30 open · cash $10,662.29 · no holdings · equity $10,662.29 vs prior close $10,662.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 238 | $44.76 | $3.07 | — | $6.34 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $10662.29 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.34 | ▼ close $10,571.16 vs 09:30 $10,662.29 (session -88.06) | 16:00 close · cash $6.34 · equity $10,571.16 vs 09:30 $10,662.29 (-91.13; session marks -88.06) · 1 name(s) marked open→close (per-name table). APA×238 09:30 $44.76 → close $44.39 -88.06 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.34 | ▲ 09:30 equity $10,602.10 vs yday $10,571.16 (+30.94) | 09:30 open · cash $6.34 (unchanged overnight, no fees) · equity $10,602.10 vs prior close $10,571.16 (+30.94) · 1 name(s) re-marked at the open (per-name table). APA×238 yday $44.39 → 09:30 $44.52 +30.94 | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 238 | $44.52 | $3.19 | $-63.38 | $10,598.91 | ▼ -63.38 after sell → book $10,598.91; vs 09:30 mark -3.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 29 | $119.43 | $2.08 | — | $7,133.36 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $3532.97 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MFC` | 83 | $42.48 | $2.24 | — | $3,605.28 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list oppset; 🔵; ⚪; ret5=-3.3; leftover $3532.97 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 5 | $623.26 | $2.00 | — | $486.98 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $3532.97 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $486.98 | ▲ close $10,768.04 vs 09:30 $10,602.10 (session +175.45) | 16:00 close · cash $486.98 · equity $10,768.04 vs 09:30 $10,602.10 (+165.94; session marks +175.45) · 3 name(s) marked open→close (per-name table). AU×29 09:30 $119.43 → close $121.22 +51.91; MFC×83 09:30 $42.48 → close $42.51 +2.49; DE×5 09:30 $623.26 → close $647.47 +121.05 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $486.98 | ▼ 09:30 equity $10,758.70 vs yday $10,768.04 (-9.34) | 09:30 open · cash $486.98 (unchanged overnight, no fees) · equity $10,758.70 vs prior close $10,768.04 (-9.34) · 3 name(s) re-marked at the open (per-name table). AU×29 yday $121.22 → 09:30 $120.51 -20.59; MFC×83 yday $42.51 → 09:30 $42.31 -16.60; DE×5 yday $647.47 → 09:30 $653.04 +27.85 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 29 | $120.51 | $2.12 | $+27.13 | $3,979.65 | ▲ +27.13 after sell → book $10,756.58; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MFC` | 83 | $42.31 | $2.28 | $-18.63 | $7,489.10 | ▼ -18.63 after sell → book $10,754.30; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 5 | $653.04 | $2.04 | $+144.85 | $10,752.26 | ▲ +144.85 after sell → book $10,752.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,752.26 | ▲ close $10,752.26 vs 09:30 $10,758.70 (session +0.00) | 16:00 close · cash $10,752.26 · no lots left · equity $10,752.26. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,752.26 | ▲ 09:30 equity $10,752.26 vs yday $10,752.26 (-0.00) | 09:30 open · cash $10,752.26 · no holdings · equity $10,752.26 vs prior close $10,752.26 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 30 | $118.52 | $2.08 | — | $7,194.58 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3584.09 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 46 | $77.13 | $2.13 | — | $3,644.47 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3584.09 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 150 | $23.80 | $2.44 | — | $72.03 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list oppset; 🔵; ret5=+0.5; leftover $3584.09 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.03 | ▲ close $11,012.09 vs 09:30 $10,752.26 (session +266.48) | 16:00 close · cash $72.03 · equity $11,012.09 vs 09:30 $10,752.26 (+259.83; session marks +266.48) · 3 name(s) marked open→close (per-name table). AU×30 09:30 $118.52 → close $123.39 +146.10; FCX×46 09:30 $77.13 → close $79.91 +127.88; AMX×150 09:30 $23.80 → close $23.75 -7.50 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.03 | ▼ 09:30 equity $10,878.17 vs yday $11,012.09 (-133.92) | 09:30 open · cash $72.03 (unchanged overnight, no fees) · equity $10,878.17 vs prior close $11,012.09 (-133.92) · 3 name(s) re-marked at the open (per-name table). AU×30 yday $123.39 → 09:30 $119.80 -107.70; FCX×46 yday $79.91 → 09:30 $79.34 -26.22; AMX×150 yday $23.75 → 09:30 $23.75 +0.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 30 | $119.80 | $2.12 | $+34.20 | $3,663.91 | ▲ +34.20 after sell → book $10,876.05; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 46 | $79.34 | $2.17 | $+97.36 | $7,311.39 | ▲ +97.36 after sell → book $10,873.89; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 27 | $267.02 | $2.07 | — | $99.77 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $7311.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.77 | ▼ close $10,861.76 vs 09:30 $10,878.17 (session -10.05) | 16:00 close · cash $99.77 · equity $10,861.76 vs 09:30 $10,878.17 (-16.41; session marks -10.05) · 2 name(s) marked open→close (per-name table). AMX×150 09:30 $23.75 → close $23.62 -19.50; FNV×27 09:30 $267.02 → close $267.37 +9.45 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.77 | ▲ 09:30 equity $10,880.48 vs yday $10,861.76 (+18.72) | 09:30 open · cash $99.77 (unchanged overnight, no fees) · equity $10,880.48 vs prior close $10,861.76 (+18.72) · 2 name(s) re-marked at the open (per-name table). AMX×150 yday $23.62 → 09:30 $23.77 +22.50; FNV×27 yday $267.37 → 09:30 $267.23 -3.78 | — |
| 2026-08-27 09:30 ET | **SELL** | `AMX` | 150 | $23.77 | $2.49 | $-9.43 | $3,662.78 | ▼ -9.43 after sell → book $10,877.99; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 27 | $267.23 | $2.14 | $+1.46 | $10,875.85 | ▲ +1.46 after sell → book $10,875.85; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 19 | $81.65 | $2.05 | — | $9,322.46 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1553.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $8,353.45 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1553.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 13 | $118.77 | $2.03 | — | $6,807.41 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; ret5=+0.3; leftover $1553.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 4 | $318.88 | $2.00 | — | $5,529.89 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1553.69 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 6 | $222.86 | $2.01 | — | $4,190.72 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $1553.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 22 | $70.30 | $2.06 | — | $2,642.07 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=-11.2; leftover $1553.69 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,642.07 | ▼ close $10,714.13 vs 09:30 $10,880.48 (session -149.59) | 16:00 close · cash $2,642.07 · equity $10,714.13 vs 09:30 $10,880.48 (-166.35; session marks -149.59) · 6 name(s) marked open→close (per-name table). ACMR×19 09:30 $81.65 → close $80.49 -22.04; MU×1 09:30 $967.01 → close $935.39 -31.62; CM×13 09:30 $118.77 → close $114.84 -51.09; LRCX×4 09:30 $318.88 → close $318.58 -1.20; NVDA×6 09:30 $222.86 → close $227.98 +30.72; AXTI×22 09:30 $70.30 → close $66.92 -74.36 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,642.07 | ▼ 09:30 equity $10,643.73 vs yday $10,714.13 (-70.40) | 09:30 open · cash $2,642.07 (unchanged overnight, no fees) · equity $10,643.73 vs prior close $10,714.13 (-70.40) · 6 name(s) re-marked at the open (per-name table). ACMR×19 yday $80.49 → 09:30 $79.27 -23.18; MU×1 yday $935.39 → 09:30 $919.29 -16.10; CM×13 yday $114.84 → 09:30 $115.66 +10.66; LRCX×4 yday $318.58 → 09:30 $318.03 -2.20; NVDA×6 yday $227.98 → 09:30 $227.36 -3.72; AXTI×22 yday $66.92 → 09:30 $65.29 -35.86 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 19 | $79.27 | $2.07 | $-49.34 | $4,146.13 | ▼ -49.34 after sell → book $10,641.66; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $5,063.41 | ▼ -51.73 after sell → book $10,639.65; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 13 | $115.66 | $2.05 | $-44.51 | $6,564.93 | ▼ -44.51 after sell → book $10,637.59; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 4 | $318.03 | $2.02 | $-7.42 | $7,835.03 | ▼ -7.42 after sell → book $10,635.57; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 6 | $227.36 | $2.03 | $+22.96 | $9,197.16 | ▲ +22.96 after sell → book $10,633.54; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 22 | $65.29 | $2.08 | $-114.35 | $10,631.47 | ▼ -114.35 after sell → book $10,631.47; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 6 | $324.41 | $2.01 | — | $8,683.00 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $2126.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 5 | $400.42 | $2.00 | — | $6,678.89 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2126.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $5,370.87 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $2126.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 8 | $240.22 | $2.01 | — | $3,447.10 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $2126.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 8 | $261.16 | $2.01 | — | $1,355.80 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; ret5=+7.8; leftover $2126.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,355.80 | ▼ close $10,405.20 vs 09:30 $10,643.73 (session -216.23) | 16:00 close · cash $1,355.80 · equity $10,405.20 vs 09:30 $10,643.73 (-238.53; session marks -216.23) · 5 name(s) marked open→close (per-name table). KEYS×6 09:30 $324.41 → close $319.97 -26.64; CIEN×5 09:30 $400.42 → close $378.44 -109.90; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×8 09:30 $240.22 → close $236.98 -25.92; ADSK×8 09:30 $261.16 → close $260.66 -4.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,355.80 | ▼ 09:30 equity $10,378.24 vs yday $10,405.20 (-26.96) | 09:30 open · cash $1,355.80 (unchanged overnight, no fees) · equity $10,378.24 vs prior close $10,405.20 (-26.96) · 5 name(s) re-marked at the open (per-name table). KEYS×6 yday $319.97 → 09:30 $322.49 +15.12; CIEN×5 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×8 yday $236.98 → 09:30 $233.97 -24.12; ADSK×8 yday $260.66 → 09:30 $257.71 -23.60 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 6 | $322.49 | $2.03 | $-15.56 | $3,288.71 | ▼ -15.56 after sell → book $10,376.21; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 5 | $378.44 | $2.03 | $-113.94 | $5,178.88 | ▼ -113.94 after sell → book $10,374.18; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $6,438.76 | ▼ -48.14 after sell → book $10,372.16; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 8 | $233.97 | $2.04 | $-54.09 | $8,308.45 | ▼ -54.09 after sell → book $10,370.13; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 8 | $257.71 | $2.04 | $-31.65 | $10,368.09 | ▼ -31.65 after sell → book $10,368.09; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,368.09 | ▲ close $10,368.09 vs 09:30 $10,378.24 (session +0.00) | 16:00 close · cash $10,368.09 · no lots left · equity $10,368.09. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,368.09 | ▲ 09:30 equity $10,368.09 vs yday $10,368.09 (-0.00) | 09:30 open · cash $10,368.09 · no holdings · equity $10,368.09 vs prior close $10,368.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,368.09 | ▲ close $10,368.09 vs 09:30 $10,368.09 (session +0.00) | 16:00 close · cash $10,368.09 · no lots left · equity $10,368.09. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,368.09 | ▲ 09:30 equity $10,368.09 vs yday $10,368.09 (-0.00) | 09:30 open · cash $10,368.09 · no holdings · equity $10,368.09 vs prior close $10,368.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,368.09 | ▲ close $10,368.09 vs 09:30 $10,368.09 (session +0.00) | 16:00 close · cash $10,368.09 · no lots left · equity $10,368.09. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,368.09 | ▲ 09:30 equity $10,368.09 vs yday $10,368.09 (-0.00) | 09:30 open · cash $10,368.09 · no holdings · equity $10,368.09 vs prior close $10,368.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 9 | $351.74 | $2.02 | — | $7,200.41 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $3456.03 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 7 | $486.31 | $2.01 | — | $3,794.23 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $3456.03 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 72 | $47.60 | $2.21 | — | $364.82 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react,oppset; 🔵; ret5=-6.2; leftover $3456.03 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $364.82 | ▲ close $11,113.67 vs 09:30 $10,368.09 (session +751.82) | 16:00 close · cash $364.82 · equity $11,113.67 vs 09:30 $10,368.09 (+745.58; session marks +751.82) · 3 name(s) marked open→close (per-name table). AVGO×9 09:30 $351.74 → close $357.16 +48.78; DELL×7 09:30 $486.31 → close $516.39 +210.56; HPE×72 09:30 $47.60 → close $54.44 +492.48 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $364.82 | ▼ 09:30 equity $11,075.78 vs yday $11,113.67 (-37.89) | 09:30 open · cash $364.82 (unchanged overnight, no fees) · equity $11,075.78 vs prior close $11,113.67 (-37.89) · 3 name(s) re-marked at the open (per-name table). AVGO×9 yday $357.16 → 09:30 $359.70 +22.86; DELL×7 yday $516.39 → 09:30 $513.78 -18.27; HPE×72 yday $54.44 → 09:30 $53.85 -42.48 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 9 | $359.70 | $2.05 | $+67.57 | $3,600.07 | ▲ +67.57 after sell → book $11,073.73; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 7 | $513.78 | $2.05 | $+188.23 | $7,194.48 | ▲ +188.23 after sell → book $11,071.68; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 6 | $263.36 | $2.01 | — | $5,612.31 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1798.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 7 | $236.82 | $2.01 | — | $3,952.56 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list ohlc_hot; ret5=+8.1; leftover $1798.62 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMX` | 78 | $23.03 | $2.22 | — | $2,154.00 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list oppset; 🔵; ret5=-1.4; leftover $1798.62 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CIEN` | 5 | $321.67 | $2.00 | — | $543.64 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list oppset; 🔵; ret5=-20.6; leftover $1798.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $543.64 | ▼ close $11,012.11 vs 09:30 $11,075.78 (session -51.32) | 16:00 close · cash $543.64 · equity $11,012.11 vs 09:30 $11,075.78 (-63.67; session marks -51.32) · 5 name(s) marked open→close (per-name table). HPE×72 09:30 $53.85 → close $52.00 -133.20; CRM×6 09:30 $263.36 → close $259.23 -24.78; BE×7 09:30 $236.82 → close $252.87 +112.35; AMX×78 09:30 $23.03 → close $23.00 -2.34; CIEN×5 09:30 $321.67 → close $321.00 -3.35 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $543.64 | ▲ 09:30 equity $11,147.96 vs yday $11,012.11 (+135.85) | 09:30 open · cash $543.64 (unchanged overnight, no fees) · equity $11,147.96 vs prior close $11,012.11 (+135.85) · 5 name(s) re-marked at the open (per-name table). HPE×72 yday $52.00 → 09:30 $52.29 +20.88; CRM×6 yday $259.23 → 09:30 $253.72 -33.06; BE×7 yday $252.87 → 09:30 $267.76 +104.23; AMX×78 yday $23.00 → 09:30 $23.15 +11.70; CIEN×5 yday $321.00 → 09:30 $327.42 +32.10 | — |
| 2026-09-08 09:30 ET | **SELL** | `HPE` | 72 | $52.29 | $2.25 | $+333.23 | $4,306.27 | ▲ +333.23 after sell → book $11,145.71; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 6 | $253.72 | $2.03 | $-61.88 | $5,826.56 | ▼ -61.88 after sell → book $11,143.68; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 7 | $267.76 | $2.04 | $+212.53 | $7,698.85 | ▲ +212.53 after sell → book $11,141.65; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AMX` | 78 | $23.15 | $2.25 | $+4.88 | $9,502.29 | ▲ +4.88 after sell → book $11,139.39; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CIEN` | 5 | $327.42 | $2.03 | $+24.72 | $11,137.37 | ▲ +24.72 after sell → book $11,137.37; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,137.37 | ▲ close $11,137.37 vs 09:30 $11,147.96 (session +0.00) | 16:00 close · cash $11,137.37 · no lots left · equity $11,137.37. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,137.37 | ▲ 09:30 equity $11,137.37 vs yday $11,137.37 (-0.00) | 09:30 open · cash $11,137.37 · no holdings · equity $11,137.37 vs prior close $11,137.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,137.37 | ▲ close $11,137.37 vs 09:30 $11,137.37 (session +0.00) | 16:00 close · cash $11,137.37 · no lots left · equity $11,137.37. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,137.37 | ▲ 09:30 equity $11,137.37 vs yday $11,137.37 (-0.00) | 09:30 open · cash $11,137.37 · no holdings · equity $11,137.37 vs prior close $11,137.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,137.37 | ▲ close $11,137.37 vs 09:30 $11,137.37 (session +0.00) | 16:00 close · cash $11,137.37 · no lots left · equity $11,137.37. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,137.37 | ▲ 09:30 equity $11,137.37 vs yday $11,137.37 (-0.00) | 09:30 open · cash $11,137.37 · no holdings · equity $11,137.37 vs prior close $11,137.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 198 | $56.03 | $2.58 | — | $40.84 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list oppset; 🔵; ret5=-0.8; leftover $11137.37 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.84 | ▼ close $10,978.36 vs 09:30 $11,137.37 (session -156.42) | 16:00 close · cash $40.84 · equity $10,978.36 vs 09:30 $11,137.37 (-159.01; session marks -156.42) · 1 name(s) marked open→close (per-name table). BTI×198 09:30 $56.03 → close $55.24 -156.42 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.84 | ▲ 09:30 equity $11,350.60 vs yday $10,978.36 (+372.24) | 09:30 open · cash $40.84 (unchanged overnight, no fees) · equity $11,350.60 vs prior close $10,978.36 (+372.24) · 1 name(s) re-marked at the open (per-name table). BTI×198 yday $55.24 → 09:30 $57.12 +372.24 | — |
| 2026-09-14 09:30 ET | **SELL** | `BTI` | 198 | $57.12 | $2.71 | $+210.53 | $11,347.90 | ▲ +210.53 after sell → book $11,347.90; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,347.90 | ▲ close $11,347.90 vs 09:30 $11,350.60 (session +0.00) | 16:00 close · cash $11,347.90 · no lots left · equity $11,347.90. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,347.90 | ▲ 09:30 equity $11,347.90 vs yday $11,347.90 (-0.00) | 09:30 open · cash $11,347.90 · no holdings · equity $11,347.90 vs prior close $11,347.90 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,347.90 | ▲ close $11,347.90 vs 09:30 $11,347.90 (session +0.00) | 16:00 close · cash $11,347.90 · no lots left · equity $11,347.90. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,347.90 | ▲ 09:30 equity $11,347.90 vs yday $11,347.90 (-0.00) | 09:30 open · cash $11,347.90 · no holdings · equity $11,347.90 vs prior close $11,347.90 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 19 | $189.17 | $2.05 | — | $7,751.62 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $3782.63 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 94 | $39.99 | $2.27 | — | $3,990.29 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $3782.63 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AMX` | 163 | $23.18 | $2.48 | — | $209.47 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list oppset; 🔵; ret5=-0.2; leftover $3782.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.47 | ▼ close $11,054.21 vs 09:30 $11,347.90 (session -286.89) | 16:00 close · cash $209.47 · equity $11,054.21 vs 09:30 $11,347.90 (-293.69; session marks -286.89) · 3 name(s) marked open→close (per-name table). QCOM×19 09:30 $189.17 → close $184.84 -82.27; SM×94 09:30 $39.99 → close $38.16 -172.02; AMX×163 09:30 $23.18 → close $22.98 -32.60 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.47 | ▲ 09:30 equity $11,121.37 vs yday $11,054.21 (+67.16) | 09:30 open · cash $209.47 (unchanged overnight, no fees) · equity $11,121.37 vs prior close $11,054.21 (+67.16) · 3 name(s) re-marked at the open (per-name table). QCOM×19 yday $184.84 → 09:30 $190.35 +104.69; SM×94 yday $38.16 → 09:30 $37.57 -55.46; AMX×163 yday $22.98 → 09:30 $23.09 +17.93 | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 19 | $190.35 | $2.09 | $+18.29 | $3,824.03 | ▲ +18.29 after sell → book $11,119.28; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 94 | $37.57 | $2.32 | $-232.07 | $7,353.30 | ▼ -232.07 after sell → book $11,116.97; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AMX` | 163 | $23.09 | $2.54 | $-19.69 | $11,114.43 | ▼ -19.69 after sell → book $11,114.43; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 11 | $934.88 | $2.02 | — | $828.73 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list yday_gainer; ret5=-7.0; leftover $11114.43 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $828.73 | ▼ close $10,658.44 vs 09:30 $11,121.37 (session -453.97) | 16:00 close · cash $828.73 · equity $10,658.44 vs 09:30 $11,121.37 (-462.93; session marks -453.97) · 1 name(s) marked open→close (per-name table). LITE×11 09:30 $934.88 → close $893.61 -453.97 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $828.73 | ▲ 09:30 equity $10,900.99 vs yday $10,658.44 (+242.55) | 09:30 open · cash $828.73 (unchanged overnight, no fees) · equity $10,900.99 vs prior close $10,658.44 (+242.55) · 1 name(s) re-marked at the open (per-name table). LITE×11 yday $893.61 → 09:30 $915.66 +242.55 | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 11 | $915.66 | $2.11 | $-215.56 | $10,898.87 | ▼ -215.56 after sell → book $10,898.87; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,898.87 | ▲ close $10,898.87 vs 09:30 $10,900.99 (session +0.00) | 16:00 close · cash $10,898.87 · no lots left · equity $10,898.87. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1553.69 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FNV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SM` | hard_red | hard-red S=-3.84 sit; no new buys |
