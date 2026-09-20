# Factor mine action — `union_news_pack_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · morning packet news🟢 only (not the merged box)

Cash book **+10.42%** ($11,042) · signal-only (no cash/fees) was +15.48%. Starts YES **22/26**. Fills 84 · skips 29 · realized $+1041.66.

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
- **Gate** `news_box=good` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,041.65.

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
| 2026-08-27 | `ACMR` | 16 | — | $81.65 | +0.00 | $80.49 | -18.56 | -18.56 | +0.00 | -18.56 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `CM` | 11 | — | $118.77 | +0.00 | $114.84 | -43.23 | -43.23 | +0.00 | -43.23 |
| 2026-08-27 | `LRCX` | 4 | — | $318.88 | +0.00 | $318.58 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-27 | `NVDA` | 6 | — | $222.86 | +0.00 | $227.98 | +30.72 | +30.72 | +0.00 | +30.72 |
| 2026-08-27 | `AXTI` | 19 | — | $70.30 | +0.00 | $66.92 | -64.22 | -64.22 | +0.00 | -64.22 |
| 2026-08-27 | `RRC` | 32 | — | $41.44 | +0.00 | $41.64 | +6.40 | +6.40 | +0.00 | +6.40 |
| 2026-08-28 | `ACMR` | 16 | $80.49 | $79.27 | -19.52 | — | +0.00 | -19.52 | -38.08 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `CM` | 11 | $114.84 | $115.66 | +9.02 | — | +0.00 | +9.02 | -34.21 | — |
| 2026-08-28 | `LRCX` | 4 | $318.58 | $318.03 | -2.20 | — | +0.00 | -2.20 | -3.40 | — |
| 2026-08-28 | `NVDA` | 6 | $227.98 | $227.36 | -3.72 | — | +0.00 | -3.72 | +27.00 | — |
| 2026-08-28 | `AXTI` | 19 | $66.92 | $65.29 | -30.97 | — | +0.00 | -30.97 | -95.19 | — |
| 2026-08-28 | `RRC` | 32 | $41.64 | $41.74 | +3.20 | $41.46 | -8.96 | -5.76 | +9.60 | +0.64 |
| 2026-08-28 | `KEYS` | 5 | — | $324.41 | +0.00 | $319.97 | -22.20 | -22.20 | +0.00 | -22.20 |
| 2026-08-28 | `CIEN` | 4 | — | $400.42 | +0.00 | $378.44 | -87.92 | -87.92 | +0.00 | -87.92 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 7 | — | $240.22 | +0.00 | $236.98 | -22.68 | -22.68 | +0.00 | -22.68 |
| 2026-08-28 | `ADSK` | 7 | — | $261.16 | +0.00 | $260.66 | -3.50 | -3.50 | +0.00 | -3.50 |
| 2026-08-31 | `RRC` | 32 | $41.46 | $42.00 | +17.28 | — | +0.00 | +17.28 | +17.92 | — |
| 2026-08-31 | `KEYS` | 5 | $319.97 | $322.49 | +12.60 | — | +0.00 | +12.60 | -9.60 | — |
| 2026-08-31 | `CIEN` | 4 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -87.92 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `DDOG` | 7 | $236.98 | $233.97 | -21.10 | — | +0.00 | -21.10 | -43.78 | — |
| 2026-08-31 | `ADSK` | 7 | $260.66 | $257.71 | -20.65 | — | +0.00 | -20.65 | -24.15 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 7 | — | $351.74 | +0.00 | $357.16 | +37.94 | +37.94 | +0.00 | +37.94 |
| 2026-09-03 | `DELL` | 5 | — | $486.31 | +0.00 | $516.39 | +150.40 | +150.40 | +0.00 | +150.40 |
| 2026-09-03 | `HPE` | 54 | — | $47.60 | +0.00 | $54.44 | +369.36 | +369.36 | +0.00 | +369.36 |
| 2026-09-03 | `CIEN` | 7 | — | $354.49 | +0.00 | $317.46 | -259.21 | -259.21 | +0.00 | -259.21 |
| 2026-09-04 | `AVGO` | 7 | $357.16 | $359.70 | +17.78 | — | +0.00 | +17.78 | +55.72 | — |
| 2026-09-04 | `DELL` | 5 | $516.39 | $513.78 | -13.05 | — | +0.00 | -13.05 | +137.35 | — |
| 2026-09-04 | `HPE` | 54 | $54.44 | $53.85 | -31.86 | $52.00 | -99.90 | -131.76 | +337.50 | +237.60 |
| 2026-09-04 | `CIEN` | 7 | $317.46 | $321.67 | +29.47 | $321.00 | -4.69 | +24.78 | -229.74 | -234.43 |
| 2026-09-04 | `CRM` | 5 | — | $263.36 | +0.00 | $259.23 | -20.65 | -20.65 | +0.00 | -20.65 |
| 2026-09-04 | `BE` | 5 | — | $236.82 | +0.00 | $252.87 | +80.25 | +80.25 | +0.00 | +80.25 |
| 2026-09-04 | `AMX` | 60 | — | $23.03 | +0.00 | $23.00 | -1.80 | -1.80 | +0.00 | -1.80 |
| 2026-09-04 | `MSTR` | 10 | — | $137.35 | +0.00 | $142.80 | +54.50 | +54.50 | +0.00 | +54.50 |
| 2026-09-08 | `HPE` | 54 | $52.00 | $52.29 | +15.66 | — | +0.00 | +15.66 | +253.26 | — |
| 2026-09-08 | `CIEN` | 7 | $321.00 | $327.42 | +44.94 | — | +0.00 | +44.94 | -189.49 | — |
| 2026-09-08 | `CRM` | 5 | $259.23 | $253.72 | -27.55 | — | +0.00 | -27.55 | -48.20 | — |
| 2026-09-08 | `BE` | 5 | $252.87 | $267.76 | +74.45 | — | +0.00 | +74.45 | +154.70 | — |
| 2026-09-08 | `AMX` | 60 | $23.00 | $23.15 | +9.00 | — | +0.00 | +9.00 | +7.20 | — |
| 2026-09-08 | `MSTR` | 10 | $142.80 | $137.62 | -51.80 | $136.52 | -11.00 | -62.80 | +2.70 | -8.30 |
| 2026-09-09 | `MSTR` | 10 | $136.52 | $141.82 | +53.00 | — | +0.00 | +53.00 | +44.70 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `BTI` | 64 | — | $56.03 | +0.00 | $55.24 | -50.56 | -50.56 | +0.00 | -50.56 |
| 2026-09-11 | `ADBE` | 14 | — | $242.17 | +0.00 | $252.23 | +140.84 | +140.84 | +0.00 | +140.84 |
| 2026-09-11 | `CNQ` | 72 | — | $49.94 | +0.00 | $50.07 | +9.36 | +9.36 | +0.00 | +9.36 |
| 2026-09-14 | `BTI` | 64 | $55.24 | $57.12 | +120.32 | — | +0.00 | +120.32 | +69.76 | — |
| 2026-09-14 | `ADBE` | 14 | $252.23 | $261.51 | +129.92 | — | +0.00 | +129.92 | +270.76 | — |
| 2026-09-14 | `CNQ` | 72 | $50.07 | $50.76 | +49.68 | — | +0.00 | +49.68 | +59.04 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `QCOM` | 19 | — | $189.17 | +0.00 | $184.84 | -82.27 | -82.27 | +0.00 | -82.27 |
| 2026-09-16 | `SM` | 93 | — | $39.99 | +0.00 | $38.16 | -170.19 | -170.19 | +0.00 | -170.19 |
| 2026-09-16 | `AMX` | 161 | — | $23.18 | +0.00 | $22.98 | -32.20 | -32.20 | +0.00 | -32.20 |
| 2026-09-17 | `QCOM` | 19 | $184.84 | $190.35 | +104.69 | — | +0.00 | +104.69 | +22.42 | — |
| 2026-09-17 | `SM` | 93 | $38.16 | $37.57 | -54.87 | — | +0.00 | -54.87 | -225.06 | — |
| 2026-09-17 | `AMX` | 161 | $22.98 | $23.09 | +17.71 | — | +0.00 | +17.71 | -14.49 | — |
| 2026-09-17 | `LITE` | 5 | — | $934.88 | +0.00 | $893.61 | -206.35 | -206.35 | +0.00 | -206.35 |
| 2026-09-17 | `FANG` | 28 | — | $191.08 | +0.00 | $196.94 | +164.08 | +164.08 | +0.00 | +164.08 |
| 2026-09-18 | `LITE` | 5 | $893.61 | $915.66 | +110.25 | — | +0.00 | +110.25 | -96.10 | — |
| 2026-09-18 | `FANG` | 28 | $196.94 | $196.94 | +0.00 | — | +0.00 | +0.00 | +164.08 | — |

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
| 2026-08-27 | — | $99.77 | AMX×150, FNV×27 | $10,880.48 | +18.72 | -121.71 | ACMR, MU, CM, LRCX, NVDA, AXTI, RRC | AMX, FNV | $2,007.32 | $10,739.95 | ACMR×16, MU×1, CM×11, LRCX×4, NVDA×6, AXTI×19, RRC×32 |
| 2026-08-28 | +0.75 | $2,007.32 | ACMR×16, MU×1, CM×11, LRCX×4, NVDA×6, AXTI×19, RRC×32 | $10,679.66 | -60.29 | -195.03 | KEYS, CIEN, MPWR, DDOG, ADSK | ACMR, MU, CM, LRCX, NVDA, AXTI | $1,282.30 | $10,462.37 | RRC×32, KEYS×5, CIEN×4, MPWR×1, DDOG×7, ADSK×7 |
| 2026-08-31 | -5.85 | $1,282.30 | RRC×32, KEYS×5, CIEN×4, MPWR×1, DDOG×7, ADSK×7 | $10,456.14 | -6.23 | +0.00 | — | RRC, KEYS, CIEN, MPWR, DDOG, ADSK | $10,443.89 | $10,443.89 | — |
| 2026-09-01 | -6.30 | $10,443.89 | — | $10,443.89 | +0.00 | +0.00 | — | — | $10,443.89 | $10,443.89 | — |
| 2026-09-02 | -3.83 | $10,443.89 | — | $10,443.89 | +0.00 | +0.00 | — | — | $10,443.89 | $10,443.89 | — |
| 2026-09-03 | -0.90 | $10,443.89 | — | $10,443.89 | +0.00 | +298.49 | AVGO, DELL, HPE, CIEN | — | $490.16 | $10,734.21 | AVGO×7, DELL×5, HPE×54, CIEN×7 |
| 2026-09-04 | +2.25 | $490.16 | AVGO×7, DELL×5, HPE×54, CIEN×7 | $10,736.55 | +2.34 | +7.71 | CRM, BE, AMX, MSTR | AVGO, DELL | $308.48 | $10,731.98 | HPE×54, CIEN×7, CRM×5, BE×5, AMX×60, MSTR×10 |
| 2026-09-08 | -11.47 | $308.48 | HPE×54, CIEN×7, CRM×5, BE×5, AMX×60, MSTR×10 | $10,796.68 | +64.70 | -11.00 | — | HPE, CIEN, CRM, BE, AMX | $9,410.01 | $10,775.21 | MSTR×10 |
| 2026-09-09 | -13.95 | $9,410.01 | MSTR×10 | $10,828.21 | +53.00 | +0.00 | — | MSTR | $10,826.17 | $10,826.17 | — |
| 2026-09-10 | -13.28 | $10,826.17 | — | $10,826.17 | +0.00 | +0.00 | — | — | $10,826.17 | $10,826.17 | — |
| 2026-09-11 | +0.50 | $10,826.17 | — | $10,826.17 | +0.00 | +99.64 | BTI, ADBE, CNQ | — | $247.77 | $10,919.39 | BTI×64, ADBE×14, CNQ×72 |
| 2026-09-14 | -11.00 | $247.77 | BTI×64, ADBE×14, CNQ×72 | $11,219.31 | +299.92 | +0.00 | — | BTI, ADBE, CNQ | $11,212.77 | $11,212.77 | — |
| 2026-09-15 | -3.84 | $11,212.77 | — | $11,212.77 | +0.00 | +0.00 | — | — | $11,212.77 | $11,212.77 | — |
| 2026-09-16 | +5.30 | $11,212.77 | — | $11,212.77 | +0.00 | -284.66 | QCOM, SM, AMX | — | $160.70 | $10,921.32 | QCOM×19, SM×93, AMX×161 |
| 2026-09-17 | +7.38 | $160.70 | QCOM×19, SM×93, AMX×161 | $10,988.85 | +67.53 | -42.27 | LITE, FANG | QCOM, SM, AMX | $953.21 | $10,935.58 | LITE×5, FANG×28 |
| 2026-09-18 | +4.86 | $953.21 | LITE×5, FANG×28 | $11,045.83 | +110.25 | +0.00 | — | LITE, FANG | $11,041.65 | $11,041.65 | — |

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
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 27 | $127.40 | $2.11 | $+195.62 | $3,721.28 | ▲ +195.62 after sell → book $10,318.34; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 9 | $367.88 | $2.05 | $+68.38 | $7,030.14 | ▲ +68.38 after sell → book $10,316.28; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 22 | $149.37 | $2.09 | $+50.19 | $10,314.19 | ▲ +50.19 after sell → book $10,314.19; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 74 | $46.18 | $2.21 | — | $6,894.66 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $3438.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 24 | $142.77 | $2.06 | — | $3,466.12 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3438.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 16 | $202.70 | $2.04 | — | $220.88 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+8.3; leftover $3438.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $220.88 | ▲ close $10,549.30 vs 09:30 $10,320.45 (session +241.42) | 16:00 close · cash $220.88 · equity $10,549.30 vs 09:30 $10,320.45 (+228.85; session marks +241.42) · 3 name(s) marked open→close (per-name table). DVN×74 09:30 $46.18 → close $47.57 +102.86; EOG×24 09:30 $142.77 → close $146.15 +81.12; FANG×16 09:30 $202.70 → close $206.29 +57.44 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $220.88 | ▲ 09:30 equity $10,668.72 vs yday $10,549.30 (+119.42) | 09:30 open · cash $220.88 (unchanged overnight, no fees) · equity $10,668.72 vs prior close $10,549.30 (+119.42) · 3 name(s) re-marked at the open (per-name table). DVN×74 yday $47.57 → 09:30 $48.00 +31.82; EOG×24 yday $146.15 → 09:30 $148.04 +45.36; FANG×16 yday $206.29 → 09:30 $208.93 +42.24 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 74 | $48.00 | $2.25 | $+130.22 | $3,770.63 | ▲ +130.22 after sell → book $10,666.47; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 24 | $148.04 | $2.10 | $+122.32 | $7,321.49 | ▲ +122.32 after sell → book $10,664.37; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 16 | $208.93 | $2.07 | $+95.57 | $10,662.29 | ▲ +95.57 after sell → book $10,662.29; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.29 | ▲ close $10,662.29 vs 09:30 $10,668.72 (session +0.00) | 16:00 close · cash $10,662.29 · no lots left · equity $10,662.29. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.29 | ▲ 09:30 equity $10,662.29 vs yday $10,662.29 (+0.00) | 09:30 open · cash $10,662.29 · no holdings · equity $10,662.29 vs prior close $10,662.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.29 | ▲ close $10,662.29 vs 09:30 $10,662.29 (session +0.00) | 16:00 close · cash $10,662.29 · no lots left · equity $10,662.29. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.29 | ▲ 09:30 equity $10,662.29 vs yday $10,662.29 (+0.00) | 09:30 open · cash $10,662.29 · no holdings · equity $10,662.29 vs prior close $10,662.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 238 | $44.76 | $3.07 | — | $6.34 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $10662.29 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.34 | ▼ close $10,571.16 vs 09:30 $10,662.29 (session -88.06) | 16:00 close · cash $6.34 · equity $10,571.16 vs 09:30 $10,662.29 (-91.13; session marks -88.06) · 1 name(s) marked open→close (per-name table). APA×238 09:30 $44.76 → close $44.39 -88.06 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.34 | ▲ 09:30 equity $10,602.10 vs yday $10,571.16 (+30.94) | 09:30 open · cash $6.34 (unchanged overnight, no fees) · equity $10,602.10 vs prior close $10,571.16 (+30.94) · 1 name(s) re-marked at the open (per-name table). APA×238 yday $44.39 → 09:30 $44.52 +30.94 | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 238 | $44.52 | $3.19 | $-63.38 | $10,598.91 | ▼ -63.38 after sell → book $10,598.91; vs 09:30 mark -3.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 29 | $119.43 | $2.08 | — | $7,133.36 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $3532.97 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MFC` | 83 | $42.48 | $2.24 | — | $3,605.28 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list oppset; 🔵; ⚪; ret5=-3.3; leftover $3532.97 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 5 | $623.26 | $2.00 | — | $486.98 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $3532.97 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $486.98 | ▲ close $10,768.04 vs 09:30 $10,602.10 (session +175.45) | 16:00 close · cash $486.98 · equity $10,768.04 vs 09:30 $10,602.10 (+165.94; session marks +175.45) · 3 name(s) marked open→close (per-name table). AU×29 09:30 $119.43 → close $121.22 +51.91; MFC×83 09:30 $42.48 → close $42.51 +2.49; DE×5 09:30 $623.26 → close $647.47 +121.05 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $486.98 | ▼ 09:30 equity $10,758.70 vs yday $10,768.04 (-9.34) | 09:30 open · cash $486.98 (unchanged overnight, no fees) · equity $10,758.70 vs prior close $10,768.04 (-9.34) · 3 name(s) re-marked at the open (per-name table). AU×29 yday $121.22 → 09:30 $120.51 -20.59; MFC×83 yday $42.51 → 09:30 $42.31 -16.60; DE×5 yday $647.47 → 09:30 $653.04 +27.85 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 29 | $120.51 | $2.12 | $+27.13 | $3,979.65 | ▲ +27.13 after sell → book $10,756.58; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MFC` | 83 | $42.31 | $2.28 | $-18.63 | $7,489.10 | ▼ -18.63 after sell → book $10,754.30; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 5 | $653.04 | $2.04 | $+144.85 | $10,752.26 | ▲ +144.85 after sell → book $10,752.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,752.26 | ▲ close $10,752.26 vs 09:30 $10,758.70 (session +0.00) | 16:00 close · cash $10,752.26 · no lots left · equity $10,752.26. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,752.26 | ▲ 09:30 equity $10,752.26 vs yday $10,752.26 (-0.00) | 09:30 open · cash $10,752.26 · no holdings · equity $10,752.26 vs prior close $10,752.26 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 30 | $118.52 | $2.08 | — | $7,194.58 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3584.09 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 46 | $77.13 | $2.13 | — | $3,644.47 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3584.09 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 150 | $23.80 | $2.44 | — | $72.03 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list oppset; 🔵; ret5=+0.5; leftover $3584.09 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.03 | ▲ close $11,012.09 vs 09:30 $10,752.26 (session +266.48) | 16:00 close · cash $72.03 · equity $11,012.09 vs 09:30 $10,752.26 (+259.83; session marks +266.48) · 3 name(s) marked open→close (per-name table). AU×30 09:30 $118.52 → close $123.39 +146.10; FCX×46 09:30 $77.13 → close $79.91 +127.88; AMX×150 09:30 $23.80 → close $23.75 -7.50 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.03 | ▼ 09:30 equity $10,878.17 vs yday $11,012.09 (-133.92) | 09:30 open · cash $72.03 (unchanged overnight, no fees) · equity $10,878.17 vs prior close $11,012.09 (-133.92) · 3 name(s) re-marked at the open (per-name table). AU×30 yday $123.39 → 09:30 $119.80 -107.70; FCX×46 yday $79.91 → 09:30 $79.34 -26.22; AMX×150 yday $23.75 → 09:30 $23.75 +0.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 30 | $119.80 | $2.12 | $+34.20 | $3,663.91 | ▲ +34.20 after sell → book $10,876.05; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 46 | $79.34 | $2.17 | $+97.36 | $7,311.39 | ▲ +97.36 after sell → book $10,873.89; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 27 | $267.02 | $2.07 | — | $99.77 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $7311.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.77 | ▼ close $10,861.76 vs 09:30 $10,878.17 (session -10.05) | 16:00 close · cash $99.77 · equity $10,861.76 vs 09:30 $10,878.17 (-16.41; session marks -10.05) · 2 name(s) marked open→close (per-name table). AMX×150 09:30 $23.75 → close $23.62 -19.50; FNV×27 09:30 $267.02 → close $267.37 +9.45 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.77 | ▲ 09:30 equity $10,880.48 vs yday $10,861.76 (+18.72) | 09:30 open · cash $99.77 (unchanged overnight, no fees) · equity $10,880.48 vs prior close $10,861.76 (+18.72) · 2 name(s) re-marked at the open (per-name table). AMX×150 yday $23.62 → 09:30 $23.77 +22.50; FNV×27 yday $267.37 → 09:30 $267.23 -3.78 | — |
| 2026-08-27 09:30 ET | **SELL** | `AMX` | 150 | $23.77 | $2.49 | $-9.43 | $3,662.78 | ▼ -9.43 after sell → book $10,877.99; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 27 | $267.23 | $2.14 | $+1.46 | $10,875.85 | ▲ +1.46 after sell → book $10,875.85; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $81.65 | $2.04 | — | $9,567.41 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1359.48 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $8,598.41 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1359.48 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 11 | $118.77 | $2.02 | — | $7,289.92 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=+0.3; leftover $1359.48 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 4 | $318.88 | $2.00 | — | $6,012.40 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1359.48 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 6 | $222.86 | $2.01 | — | $4,673.23 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $1359.48 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 19 | $70.30 | $2.05 | — | $3,335.48 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=-11.2; leftover $1359.48 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 32 | $41.44 | $2.09 | — | $2,007.32 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; ret5=+3.1; leftover $1359.48 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,007.32 | ▼ close $10,739.95 vs 09:30 $10,880.48 (session -121.71) | 16:00 close · cash $2,007.32 · equity $10,739.95 vs 09:30 $10,880.48 (-140.53; session marks -121.71) · 7 name(s) marked open→close (per-name table). ACMR×16 09:30 $81.65 → close $80.49 -18.56; MU×1 09:30 $967.01 → close $935.39 -31.62; CM×11 09:30 $118.77 → close $114.84 -43.23; LRCX×4 09:30 $318.88 → close $318.58 -1.20; NVDA×6 09:30 $222.86 → close $227.98 +30.72; AXTI×19 09:30 $70.30 → close $66.92 -64.22; RRC×32 09:30 $41.44 → close $41.64 +6.40 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,007.32 | ▼ 09:30 equity $10,679.66 vs yday $10,739.95 (-60.29) | 09:30 open · cash $2,007.32 (unchanged overnight, no fees) · equity $10,679.66 vs prior close $10,739.95 (-60.29) · 7 name(s) re-marked at the open (per-name table). ACMR×16 yday $80.49 → 09:30 $79.27 -19.52; MU×1 yday $935.39 → 09:30 $919.29 -16.10; CM×11 yday $114.84 → 09:30 $115.66 +9.02; LRCX×4 yday $318.58 → 09:30 $318.03 -2.20; NVDA×6 yday $227.98 → 09:30 $227.36 -3.72; AXTI×19 yday $66.92 → 09:30 $65.29 -30.97; RRC×32 yday $41.64 → 09:30 $41.74 +3.20 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $79.27 | $2.06 | $-42.18 | $3,273.58 | ▼ -42.18 after sell → book $10,677.60; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $4,190.85 | ▼ -51.73 after sell → book $10,675.58; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 11 | $115.66 | $2.04 | $-38.28 | $5,461.07 | ▼ -38.28 after sell → book $10,673.54; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 4 | $318.03 | $2.02 | $-7.42 | $6,731.17 | ▼ -7.42 after sell → book $10,671.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 6 | $227.36 | $2.03 | $+22.96 | $8,093.30 | ▲ +22.96 after sell → book $10,669.49; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 19 | $65.29 | $2.07 | $-99.30 | $9,331.74 | ▼ -99.30 after sell → book $10,667.42; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 5 | $324.41 | $2.00 | — | $7,707.69 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1866.35 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 4 | $400.42 | $2.00 | — | $6,104.01 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1866.35 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,795.98 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1866.35 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 7 | $240.22 | $2.01 | — | $3,112.43 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1866.35 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 7 | $261.16 | $2.01 | — | $1,282.30 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=+7.8; leftover $1866.35 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,282.30 | ▼ close $10,462.37 vs 09:30 $10,679.66 (session -195.03) | 16:00 close · cash $1,282.30 · equity $10,462.37 vs 09:30 $10,679.66 (-217.29; session marks -195.03) · 6 name(s) marked open→close (per-name table). RRC×32 09:30 $41.74 → close $41.46 -8.96; KEYS×5 09:30 $324.41 → close $319.97 -22.20; CIEN×4 09:30 $400.42 → close $378.44 -87.92; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×7 09:30 $240.22 → close $236.98 -22.68; ADSK×7 09:30 $261.16 → close $260.66 -3.50 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,282.30 | ▼ 09:30 equity $10,456.14 vs yday $10,462.37 (-6.23) | 09:30 open · cash $1,282.30 (unchanged overnight, no fees) · equity $10,456.14 vs prior close $10,462.37 (-6.23) · 6 name(s) re-marked at the open (per-name table). RRC×32 yday $41.46 → 09:30 $42.00 +17.28; KEYS×5 yday $319.97 → 09:30 $322.49 +12.60; CIEN×4 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×7 yday $236.98 → 09:30 $233.97 -21.10; ADSK×7 yday $260.66 → 09:30 $257.71 -20.65 | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 32 | $42.00 | $2.11 | $+13.73 | $2,624.19 | ▲ +13.73 after sell → book $10,454.03; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 5 | $322.49 | $2.03 | $-13.63 | $4,234.62 | ▼ -13.63 after sell → book $10,452.00; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 4 | $378.44 | $2.02 | $-91.95 | $5,746.35 | ▼ -91.95 after sell → book $10,449.98; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $7,006.24 | ▼ -48.14 after sell → book $10,447.96; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 7 | $233.97 | $2.03 | $-47.83 | $8,641.96 | ▼ -47.83 after sell → book $10,445.93; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 7 | $257.71 | $2.04 | $-28.20 | $10,443.89 | ▼ -28.20 after sell → book $10,443.89; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,443.89 | ▲ close $10,443.89 vs 09:30 $10,456.14 (session +0.00) | 16:00 close · cash $10,443.89 · no lots left · equity $10,443.89. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,443.89 | ▲ 09:30 equity $10,443.89 vs yday $10,443.89 (+0.00) | 09:30 open · cash $10,443.89 · no holdings · equity $10,443.89 vs prior close $10,443.89 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,443.89 | ▲ close $10,443.89 vs 09:30 $10,443.89 (session +0.00) | 16:00 close · cash $10,443.89 · no lots left · equity $10,443.89. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,443.89 | ▲ 09:30 equity $10,443.89 vs yday $10,443.89 (+0.00) | 09:30 open · cash $10,443.89 · no holdings · equity $10,443.89 vs prior close $10,443.89 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,443.89 | ▲ close $10,443.89 vs 09:30 $10,443.89 (session +0.00) | 16:00 close · cash $10,443.89 · no lots left · equity $10,443.89. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,443.89 | ▲ 09:30 equity $10,443.89 vs yday $10,443.89 (+0.00) | 09:30 open · cash $10,443.89 · no holdings · equity $10,443.89 vs prior close $10,443.89 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 7 | $351.74 | $2.01 | — | $7,979.70 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $2610.97 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 5 | $486.31 | $2.00 | — | $5,546.15 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $2610.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 54 | $47.60 | $2.15 | — | $2,973.60 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react,oppset; 🔵; ret5=-6.2; leftover $2610.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 7 | $354.49 | $2.01 | — | $490.16 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=-12.3; leftover $2610.97 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $490.16 | ▲ close $10,734.21 vs 09:30 $10,443.89 (session +298.49) | 16:00 close · cash $490.16 · equity $10,734.21 vs 09:30 $10,443.89 (+290.32; session marks +298.49) · 4 name(s) marked open→close (per-name table). AVGO×7 09:30 $351.74 → close $357.16 +37.94; DELL×5 09:30 $486.31 → close $516.39 +150.40; HPE×54 09:30 $47.60 → close $54.44 +369.36; CIEN×7 09:30 $354.49 → close $317.46 -259.21 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $490.16 | ▲ 09:30 equity $10,736.55 vs yday $10,734.21 (+2.34) | 09:30 open · cash $490.16 (unchanged overnight, no fees) · equity $10,736.55 vs prior close $10,734.21 (+2.34) · 4 name(s) re-marked at the open (per-name table). AVGO×7 yday $357.16 → 09:30 $359.70 +17.78; DELL×5 yday $516.39 → 09:30 $513.78 -13.05; HPE×54 yday $54.44 → 09:30 $53.85 -31.86; CIEN×7 yday $317.46 → 09:30 $321.67 +29.47 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 7 | $359.70 | $2.04 | $+51.67 | $3,006.01 | ▲ +51.67 after sell → book $10,734.50; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 5 | $513.78 | $2.04 | $+133.31 | $5,572.88 | ▲ +133.31 after sell → book $10,732.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 5 | $263.36 | $2.00 | — | $4,254.07 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1393.22 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 5 | $236.82 | $2.00 | — | $3,067.97 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+8.1; leftover $1393.22 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AMX` | 60 | $23.03 | $2.17 | — | $1,684.00 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list oppset; 🔵; ret5=-1.4; leftover $1393.22 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 10 | $137.35 | $2.02 | — | $308.48 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+5.4; leftover $1393.22 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $308.48 | ▲ close $10,731.98 vs 09:30 $10,736.55 (session +7.71) | 16:00 close · cash $308.48 · equity $10,731.98 vs 09:30 $10,736.55 (-4.57; session marks +7.71) · 6 name(s) marked open→close (per-name table). HPE×54 09:30 $53.85 → close $52.00 -99.90; CIEN×7 09:30 $321.67 → close $321.00 -4.69; CRM×5 09:30 $263.36 → close $259.23 -20.65; BE×5 09:30 $236.82 → close $252.87 +80.25; AMX×60 09:30 $23.03 → close $23.00 -1.80; MSTR×10 09:30 $137.35 → close $142.80 +54.50 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $308.48 | ▲ 09:30 equity $10,796.68 vs yday $10,731.98 (+64.70) | 09:30 open · cash $308.48 (unchanged overnight, no fees) · equity $10,796.68 vs prior close $10,731.98 (+64.70) · 6 name(s) re-marked at the open (per-name table). HPE×54 yday $52.00 → 09:30 $52.29 +15.66; CIEN×7 yday $321.00 → 09:30 $327.42 +44.94; CRM×5 yday $259.23 → 09:30 $253.72 -27.55; BE×5 yday $252.87 → 09:30 $267.76 +74.45; AMX×60 yday $23.00 → 09:30 $23.15 +9.00; MSTR×10 yday $142.80 → 09:30 $137.62 -51.80 | — |
| 2026-09-08 09:30 ET | **SELL** | `HPE` | 54 | $52.29 | $2.18 | $+248.92 | $3,129.95 | ▲ +248.92 after sell → book $10,794.49; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CIEN` | 7 | $327.42 | $2.04 | $-193.54 | $5,419.85 | ▼ -193.54 after sell → book $10,792.45; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 5 | $253.72 | $2.03 | $-52.23 | $6,686.43 | ▼ -52.23 after sell → book $10,790.43; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 5 | $267.76 | $2.03 | $+150.67 | $8,023.20 | ▲ +150.67 after sell → book $10,788.40; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AMX` | 60 | $23.15 | $2.19 | $+2.84 | $9,410.01 | ▲ +2.84 after sell → book $10,786.21; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,410.01 | ▼ close $10,775.21 vs 09:30 $10,796.68 (session -11.00) | 16:00 close · cash $9,410.01 · equity $10,775.21 vs 09:30 $10,796.68 (-21.47; session marks -11.00) · 1 name(s) marked open→close (per-name table). MSTR×10 09:30 $137.62 → close $136.52 -11.00 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,410.01 | ▲ 09:30 equity $10,828.21 vs yday $10,775.21 (+53.00) | 09:30 open · cash $9,410.01 (unchanged overnight, no fees) · equity $10,828.21 vs prior close $10,775.21 (+53.00) · 1 name(s) re-marked at the open (per-name table). MSTR×10 yday $136.52 → 09:30 $141.82 +53.00 | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 10 | $141.82 | $2.04 | $+40.64 | $10,826.17 | ▲ +40.64 after sell → book $10,826.17; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,826.17 | ▲ close $10,826.17 vs 09:30 $10,828.21 (session +0.00) | 16:00 close · cash $10,826.17 · no lots left · equity $10,826.17. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,826.17 | ▲ 09:30 equity $10,826.17 vs yday $10,826.17 (+0.00) | 09:30 open · cash $10,826.17 · no holdings · equity $10,826.17 vs prior close $10,826.17 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,826.17 | ▲ close $10,826.17 vs 09:30 $10,826.17 (session +0.00) | 16:00 close · cash $10,826.17 · no lots left · equity $10,826.17. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,826.17 | ▲ 09:30 equity $10,826.17 vs yday $10,826.17 (+0.00) | 09:30 open · cash $10,826.17 · no holdings · equity $10,826.17 vs prior close $10,826.17 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 64 | $56.03 | $2.18 | — | $7,238.07 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list oppset; 🔵; ret5=-0.8; leftover $3608.72 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 14 | $242.17 | $2.03 | — | $3,845.66 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=-11.1; leftover $3608.72 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CNQ` | 72 | $49.94 | $2.21 | — | $247.77 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list oppset; ret5=+1.7; leftover $3608.72 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $247.77 | ▲ close $10,919.39 vs 09:30 $10,826.17 (session +99.64) | 16:00 close · cash $247.77 · equity $10,919.39 vs 09:30 $10,826.17 (+93.22; session marks +99.64) · 3 name(s) marked open→close (per-name table). BTI×64 09:30 $56.03 → close $55.24 -50.56; ADBE×14 09:30 $242.17 → close $252.23 +140.84; CNQ×72 09:30 $49.94 → close $50.07 +9.36 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $247.77 | ▲ 09:30 equity $11,219.31 vs yday $10,919.39 (+299.92) | 09:30 open · cash $247.77 (unchanged overnight, no fees) · equity $11,219.31 vs prior close $10,919.39 (+299.92) · 3 name(s) re-marked at the open (per-name table). BTI×64 yday $55.24 → 09:30 $57.12 +120.32; ADBE×14 yday $252.23 → 09:30 $261.51 +129.92; CNQ×72 yday $50.07 → 09:30 $50.76 +49.68 | — |
| 2026-09-14 09:30 ET | **SELL** | `BTI` | 64 | $57.12 | $2.22 | $+65.36 | $3,901.23 | ▲ +65.36 after sell → book $11,217.09; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 14 | $261.51 | $2.07 | $+266.66 | $7,560.30 | ▲ +266.66 after sell → book $11,215.02; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CNQ` | 72 | $50.76 | $2.25 | $+54.59 | $11,212.77 | ▲ +54.59 after sell → book $11,212.77; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,212.77 | ▲ close $11,212.77 vs 09:30 $11,219.31 (session +0.00) | 16:00 close · cash $11,212.77 · no lots left · equity $11,212.77. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,212.77 | ▲ 09:30 equity $11,212.77 vs yday $11,212.77 (+0.00) | 09:30 open · cash $11,212.77 · no holdings · equity $11,212.77 vs prior close $11,212.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,212.77 | ▲ close $11,212.77 vs 09:30 $11,212.77 (session +0.00) | 16:00 close · cash $11,212.77 · no lots left · equity $11,212.77. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,212.77 | ▲ 09:30 equity $11,212.77 vs yday $11,212.77 (+0.00) | 09:30 open · cash $11,212.77 · no holdings · equity $11,212.77 vs prior close $11,212.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 19 | $189.17 | $2.05 | — | $7,616.49 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $3737.59 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 93 | $39.99 | $2.27 | — | $3,895.16 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $3737.59 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AMX` | 161 | $23.18 | $2.47 | — | $160.70 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list oppset; 🔵; ret5=-0.2; leftover $3737.59 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.70 | ▼ close $10,921.32 vs 09:30 $11,212.77 (session -284.66) | 16:00 close · cash $160.70 · equity $10,921.32 vs 09:30 $11,212.77 (-291.45; session marks -284.66) · 3 name(s) marked open→close (per-name table). QCOM×19 09:30 $189.17 → close $184.84 -82.27; SM×93 09:30 $39.99 → close $38.16 -170.19; AMX×161 09:30 $23.18 → close $22.98 -32.20 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.70 | ▲ 09:30 equity $10,988.85 vs yday $10,921.32 (+67.53) | 09:30 open · cash $160.70 (unchanged overnight, no fees) · equity $10,988.85 vs prior close $10,921.32 (+67.53) · 3 name(s) re-marked at the open (per-name table). QCOM×19 yday $184.84 → 09:30 $190.35 +104.69; SM×93 yday $38.16 → 09:30 $37.57 -54.87; AMX×161 yday $22.98 → 09:30 $23.09 +17.71 | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 19 | $190.35 | $2.09 | $+18.29 | $3,775.27 | ▲ +18.29 after sell → book $10,986.77; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 93 | $37.57 | $2.31 | $-229.64 | $7,266.96 | ▼ -229.64 after sell → book $10,984.45; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AMX` | 161 | $23.09 | $2.53 | $-19.49 | $10,981.92 | ▼ -19.49 after sell → book $10,981.92; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 5 | $934.88 | $2.00 | — | $6,305.52 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list yday_gainer; ret5=-7.0; leftover $5490.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FANG` | 28 | $191.08 | $2.07 | — | $953.21 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list oppset; ret5=-4.0; leftover $5490.96 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $953.21 | ▼ close $10,935.58 vs 09:30 $10,988.85 (session -42.27) | 16:00 close · cash $953.21 · equity $10,935.58 vs 09:30 $10,988.85 (-53.27; session marks -42.27) · 2 name(s) marked open→close (per-name table). LITE×5 09:30 $934.88 → close $893.61 -206.35; FANG×28 09:30 $191.08 → close $196.94 +164.08 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $953.21 | ▲ 09:30 equity $11,045.83 vs yday $10,935.58 (+110.25) | 09:30 open · cash $953.21 (unchanged overnight, no fees) · equity $11,045.83 vs prior close $10,935.58 (+110.25) · 2 name(s) re-marked at the open (per-name table). LITE×5 yday $893.61 → 09:30 $915.66 +110.25; FANG×28 yday $196.94 → 09:30 $196.94 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 5 | $915.66 | $2.05 | $-100.16 | $5,529.45 | ▼ -100.16 after sell → book $11,043.77; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FANG` | 28 | $196.94 | $2.13 | $+159.88 | $11,041.65 | ▲ +159.88 after sell → book $11,041.65; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,041.65 | ▲ close $11,041.65 vs 09:30 $11,045.83 (session +0.00) | 16:00 close · cash $11,041.65 · no lots left · equity $11,041.65. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AMX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1359.48 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FNV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QCOM` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
