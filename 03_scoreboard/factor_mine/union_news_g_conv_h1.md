# Factor mine action — `union_news_g_conv_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `conviction` · sell `list` · S-boost `none` · merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5

Cash book **+5.00%** ($10,500) · signal-only (no cash/fees) was +7.97%. Starts YES **8/26**. Fills 92 · skips 46 · realized $+470.54.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the news camera (does the morning packet like the headline?) is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 4.
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
- **Gate** `news=good` · **rank** `cond` · **top_n** 4.
- **Size** `conviction` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9.05.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 531 | — | $13.18 | +0.00 | $13.92 | +392.94 | +392.94 | +0.00 | +392.94 |
| 2026-08-14 | `ANGX` | 232 | — | $4.31 | +0.00 | $4.37 | +13.92 | +13.92 | +0.00 | +13.92 |
| 2026-08-14 | `ARX` | 51 | — | $19.57 | +0.00 | $19.58 | +0.51 | +0.51 | +0.00 | +0.51 |
| 2026-08-17 | `HLIT` | 531 | $13.92 | $13.84 | -42.48 | — | +0.00 | -42.48 | +350.46 | — |
| 2026-08-17 | `ANGX` | 232 | $4.37 | $4.60 | +53.36 | — | +0.00 | +53.36 | +67.28 | — |
| 2026-08-17 | `ARX` | 51 | $19.58 | $19.57 | -0.51 | — | +0.00 | -0.51 | +0.00 | — |
| 2026-08-17 | `DVN` | 90 | — | $46.18 | +0.00 | $47.57 | +125.10 | +125.10 | +0.00 | +125.10 |
| 2026-08-17 | `EOG` | 21 | — | $142.77 | +0.00 | $146.15 | +70.98 | +70.98 | +0.00 | +70.98 |
| 2026-08-17 | `FANG` | 10 | — | $202.70 | +0.00 | $206.29 | +35.90 | +35.90 | +0.00 | +35.90 |
| 2026-08-17 | `GLOB` | 27 | — | $37.18 | +0.00 | $36.26 | -24.84 | -24.84 | +0.00 | -24.84 |
| 2026-08-18 | `DVN` | 90 | $47.57 | $48.00 | +38.70 | — | +0.00 | +38.70 | +163.80 | — |
| 2026-08-18 | `EOG` | 21 | $146.15 | $148.04 | +39.69 | — | +0.00 | +39.69 | +110.67 | — |
| 2026-08-18 | `FANG` | 10 | $206.29 | $208.93 | +26.40 | — | +0.00 | +26.40 | +62.30 | — |
| 2026-08-18 | `GLOB` | 27 | $36.26 | $36.98 | +19.44 | — | +0.00 | +19.44 | -5.40 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 82 | — | $91.01 | +0.00 | $93.63 | +214.84 | +214.84 | +0.00 | +214.84 |
| 2026-08-20 | `APA` | 23 | — | $44.76 | +0.00 | $44.39 | -8.51 | -8.51 | +0.00 | -8.51 |
| 2026-08-20 | `AUTL` | 433 | — | $2.47 | +0.00 | $2.46 | -4.33 | -4.33 | +0.00 | -4.33 |
| 2026-08-20 | `CRSP` | 18 | — | $58.73 | +0.00 | $58.12 | -10.98 | -10.98 | +0.00 | -10.98 |
| 2026-08-21 | `BHP` | 82 | $93.63 | $95.72 | +171.38 | — | +0.00 | +171.38 | +386.22 | — |
| 2026-08-21 | `APA` | 23 | $44.39 | $44.52 | +2.99 | — | +0.00 | +2.99 | -5.52 | — |
| 2026-08-21 | `AUTL` | 433 | $2.46 | $2.47 | +4.33 | $2.41 | -25.98 | -21.65 | +0.00 | -25.98 |
| 2026-08-21 | `CRSP` | 18 | $58.12 | $59.72 | +28.80 | $59.50 | -3.96 | +24.84 | +17.82 | +13.86 |
| 2026-08-21 | `AU` | 52 | — | $119.43 | +0.00 | $121.22 | +93.08 | +93.08 | +0.00 | +93.08 |
| 2026-08-21 | `FUTU` | 23 | — | $115.18 | +0.00 | $123.64 | +194.58 | +194.58 | +0.00 | +194.58 |
| 2026-08-24 | `AUTL` | 433 | $2.41 | $2.40 | -4.33 | — | +0.00 | -4.33 | -30.31 | — |
| 2026-08-24 | `CRSP` | 18 | $59.50 | $58.75 | -13.50 | $57.08 | -30.15 | -43.65 | +0.36 | -29.79 |
| 2026-08-24 | `AU` | 52 | $121.22 | $120.51 | -36.92 | — | +0.00 | -36.92 | +56.16 | — |
| 2026-08-24 | `FUTU` | 23 | $123.64 | $121.00 | -60.72 | — | +0.00 | -60.72 | +133.86 | — |
| 2026-08-25 | `CRSP` | 18 | $57.08 | $57.93 | +15.39 | — | +0.00 | +15.39 | -14.40 | — |
| 2026-08-25 | `AU` | 66 | — | $118.52 | +0.00 | $123.39 | +321.42 | +321.42 | +0.00 | +321.42 |
| 2026-08-25 | `FCX` | 14 | — | $77.13 | +0.00 | $79.91 | +38.92 | +38.92 | +0.00 | +38.92 |
| 2026-08-25 | `EZPW` | 31 | — | $35.05 | +0.00 | $35.23 | +5.58 | +5.58 | +0.00 | +5.58 |
| 2026-08-25 | `AMX` | 47 | — | $23.80 | +0.00 | $23.75 | -2.35 | -2.35 | +0.00 | -2.35 |
| 2026-08-26 | `AU` | 66 | $123.39 | $119.80 | -236.94 | — | +0.00 | -236.94 | +84.48 | — |
| 2026-08-26 | `FCX` | 14 | $79.91 | $79.34 | -7.98 | — | +0.00 | -7.98 | +30.94 | — |
| 2026-08-26 | `EZPW` | 31 | $35.23 | $35.70 | +14.57 | — | +0.00 | +14.57 | +20.15 | — |
| 2026-08-26 | `AMX` | 47 | $23.75 | $23.75 | +0.00 | $23.62 | -6.11 | -6.11 | -2.35 | -8.46 |
| 2026-08-26 | `FNV` | 26 | — | $267.02 | +0.00 | $267.37 | +9.10 | +9.10 | +0.00 | +9.10 |
| 2026-08-26 | `ASST` | 73 | — | $20.72 | +0.00 | $21.50 | +56.94 | +56.94 | +0.00 | +56.94 |
| 2026-08-26 | `ZYME` | 55 | — | $27.56 | +0.00 | $29.30 | +95.98 | +95.98 | +0.00 | +95.98 |
| 2026-08-27 | `AMX` | 47 | $23.62 | $23.77 | +7.05 | — | +0.00 | +7.05 | -1.41 | — |
| 2026-08-27 | `FNV` | 26 | $267.37 | $267.23 | -3.64 | — | +0.00 | -3.64 | +5.46 | — |
| 2026-08-27 | `ASST` | 73 | $21.50 | $22.45 | +69.35 | — | +0.00 | +69.35 | +126.29 | — |
| 2026-08-27 | `ZYME` | 55 | $29.30 | $29.33 | +1.37 | — | +0.00 | +1.37 | +97.35 | — |
| 2026-08-27 | `ACMR` | 98 | — | $81.65 | +0.00 | $80.49 | -113.68 | -113.68 | +0.00 | -113.68 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `CM` | 9 | — | $118.77 | +0.00 | $114.84 | -35.37 | -35.37 | +0.00 | -35.37 |
| 2026-08-28 | `ACMR` | 98 | $80.49 | $79.27 | -119.56 | — | +0.00 | -119.56 | -233.24 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `CM` | 9 | $114.84 | $115.66 | +7.38 | — | +0.00 | +7.38 | -27.99 | — |
| 2026-08-28 | `KEYS` | 24 | — | $324.41 | +0.00 | $319.97 | -106.56 | -106.56 | +0.00 | -106.56 |
| 2026-08-28 | `SMTC` | 7 | — | $141.76 | +0.00 | $131.17 | -74.13 | -74.13 | +0.00 | -74.13 |
| 2026-08-28 | `CIEN` | 2 | — | $400.42 | +0.00 | $378.44 | -43.96 | -43.96 | +0.00 | -43.96 |
| 2026-08-31 | `KEYS` | 24 | $319.97 | $322.49 | +60.48 | — | +0.00 | +60.48 | -46.08 | — |
| 2026-08-31 | `SMTC` | 7 | $131.17 | $132.30 | +7.91 | — | +0.00 | +7.91 | -66.22 | — |
| 2026-08-31 | `CIEN` | 2 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -43.96 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 21 | — | $351.74 | +0.00 | $357.16 | +113.82 | +113.82 | +0.00 | +113.82 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 34 | — | $32.31 | +0.00 | $33.66 | +45.90 | +45.90 | +0.00 | +45.90 |
| 2026-09-03 | `FRNM` | 69 | — | $15.87 | +0.00 | $16.90 | +71.07 | +71.07 | +0.00 | +71.07 |
| 2026-09-04 | `AVGO` | 21 | $357.16 | $359.70 | +53.34 | — | +0.00 | +53.34 | +167.16 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `CXW` | 34 | $33.66 | $33.46 | -6.80 | — | +0.00 | -6.80 | +39.10 | — |
| 2026-09-04 | `FRNM` | 69 | $16.90 | $16.40 | -34.50 | $16.31 | -6.21 | -40.71 | +36.57 | +30.36 |
| 2026-09-04 | `CRM` | 27 | — | $263.36 | +0.00 | $259.23 | -111.51 | -111.51 | +0.00 | -111.51 |
| 2026-09-04 | `MMED` | 64 | — | $23.84 | +0.00 | $23.29 | -35.20 | -35.20 | +0.00 | -35.20 |
| 2026-09-04 | `HPE` | 28 | — | $53.85 | +0.00 | $52.00 | -51.80 | -51.80 | +0.00 | -51.80 |
| 2026-09-08 | `FRNM` | 69 | $16.31 | $16.74 | +29.67 | — | +0.00 | +29.67 | +60.03 | — |
| 2026-09-08 | `CRM` | 27 | $259.23 | $253.72 | -148.77 | — | +0.00 | -148.77 | -260.28 | — |
| 2026-09-08 | `MMED` | 64 | $23.29 | $23.16 | -8.32 | — | +0.00 | -8.32 | -43.52 | — |
| 2026-09-08 | `HPE` | 28 | $52.00 | $52.29 | +8.12 | — | +0.00 | +8.12 | -43.68 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 46 | — | $164.43 | +0.00 | $150.28 | -650.90 | -650.90 | +0.00 | -650.90 |
| 2026-09-11 | `BTI` | 19 | — | $56.03 | +0.00 | $55.24 | -15.01 | -15.01 | +0.00 | -15.01 |
| 2026-09-11 | `ADBE` | 4 | — | $242.17 | +0.00 | $252.23 | +40.24 | +40.24 | +0.00 | +40.24 |
| 2026-09-11 | `CNQ` | 22 | — | $49.94 | +0.00 | $50.07 | +2.86 | +2.86 | +0.00 | +2.86 |
| 2026-09-14 | `ORCL` | 46 | $150.28 | $141.42 | -407.56 | — | +0.00 | -407.56 | -1058.46 | — |
| 2026-09-14 | `BTI` | 19 | $55.24 | $57.12 | +35.72 | — | +0.00 | +35.72 | +20.71 | — |
| 2026-09-14 | `ADBE` | 4 | $252.23 | $261.51 | +37.12 | — | +0.00 | +37.12 | +77.36 | — |
| 2026-09-14 | `CNQ` | 22 | $50.07 | $50.76 | +15.18 | — | +0.00 | +15.18 | +18.04 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 267 | — | $26.27 | +0.00 | $26.59 | +85.44 | +85.44 | +0.00 | +85.44 |
| 2026-09-16 | `QCOM` | 5 | — | $189.17 | +0.00 | $184.84 | -21.65 | -21.65 | +0.00 | -21.65 |
| 2026-09-16 | `SM` | 25 | — | $39.99 | +0.00 | $38.16 | -45.75 | -45.75 | +0.00 | -45.75 |
| 2026-09-16 | `AMX` | 43 | — | $23.18 | +0.00 | $22.98 | -8.60 | -8.60 | +0.00 | -8.60 |
| 2026-09-17 | `WAY` | 267 | $26.59 | $26.51 | -21.36 | — | +0.00 | -21.36 | +64.08 | — |
| 2026-09-17 | `QCOM` | 5 | $184.84 | $190.35 | +27.55 | — | +0.00 | +27.55 | +5.90 | — |
| 2026-09-17 | `SM` | 25 | $38.16 | $37.57 | -14.75 | — | +0.00 | -14.75 | -60.50 | — |
| 2026-09-17 | `AMX` | 43 | $22.98 | $23.09 | +4.73 | — | +0.00 | +4.73 | -3.87 | — |
| 2026-09-17 | `SMTC` | 41 | — | $170.85 | +0.00 | $178.19 | +300.94 | +300.94 | +0.00 | +300.94 |
| 2026-09-17 | `AVTR` | 63 | — | $15.81 | +0.00 | $15.86 | +3.15 | +3.15 | +0.00 | +3.15 |
| 2026-09-17 | `GME` | 45 | — | $22.12 | +0.00 | $22.77 | +29.25 | +29.25 | +0.00 | +29.25 |
| 2026-09-17 | `JBHT` | 4 | — | $238.60 | +0.00 | $236.80 | -7.20 | -7.20 | +0.00 | -7.20 |
| 2026-09-18 | `SMTC` | 41 | $178.19 | $182.33 | +169.74 | — | +0.00 | +169.74 | +470.68 | — |
| 2026-09-18 | `AVTR` | 63 | $15.86 | $15.87 | +0.63 | — | +0.00 | +0.63 | +3.78 | — |
| 2026-09-18 | `GME` | 45 | $22.77 | $22.90 | +5.85 | $22.64 | -11.70 | -5.85 | +35.10 | +23.40 |
| 2026-09-18 | `JBHT` | 4 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -7.20 | — |
| 2026-09-18 | `TH` | 317 | — | $20.91 | +0.00 | $21.19 | +88.76 | +88.76 | +0.00 | +88.76 |
| 2026-09-18 | `RARE` | 96 | — | $14.79 | +0.00 | $14.51 | -26.88 | -26.88 | +0.00 | -26.88 |
| 2026-09-18 | `BHVN` | 100 | — | $14.07 | +0.00 | $13.62 | -45.00 | -45.00 | +0.00 | -45.00 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +407.37 | HLIT, ANGX, ARX | — | $991.44 | $10,395.38 | HLIT×531, ANGX×232, ARX×51 |
| 2026-08-17 | +2.25 | $991.44 | HLIT×531, ANGX×232, ARX×51 | $10,405.75 | +10.37 | +207.14 | DVN, EOG, FANG, GLOB | HLIT, ANGX, ARX | $199.92 | $10,592.29 | DVN×90, EOG×21, FANG×10, GLOB×27 |
| 2026-08-18 | -6.20 | $199.92 | DVN×90, EOG×21, FANG×10, GLOB×27 | $10,716.52 | +124.23 | +0.00 | — | DVN, EOG, FANG, GLOB | $10,707.98 | $10,707.98 | — |
| 2026-08-19 | -7.20 | $10,707.98 | — | $10,707.98 | +0.00 | +0.00 | — | — | $10,707.98 | $10,707.98 | — |
| 2026-08-20 | +1.12 | $10,707.98 | — | $10,707.98 | +0.00 | +191.02 | BHP, APA, AUTL, CRSP | — | $77.11 | $10,887.08 | BHP×82, APA×23, AUTL×433, CRSP×18 |
| 2026-08-21 | +3.25 | $77.11 | BHP×82, APA×23, AUTL×433, CRSP×18 | $11,094.58 | +207.50 | +257.72 | AU, FUTU | BHP, APA | $82.01 | $11,343.70 | AUTL×433, CRSP×18, AU×52, FUTU×23 |
| 2026-08-24 | -5.17 | $82.01 | AUTL×433, CRSP×18, AU×52, FUTU×23 | $11,228.23 | -115.47 | -30.15 | — | AUTL, AU, FUTU | $10,160.77 | $11,188.12 | CRSP×18 |
| 2026-08-25 | +1.80 | $10,160.77 | CRSP×18 | $11,203.51 | +15.39 | +363.57 | AU, FCX, EZPW, AMX | CRSP | $85.72 | $11,556.58 | AU×66, FCX×14, EZPW×31, AMX×47 |
| 2026-08-26 | +2.02 | $85.72 | AU×66, FCX×14, EZPW×31, AMX×47 | $11,326.23 | -230.35 | +155.91 | FNV, ASST, ZYME | AU, FCX, EZPW | $226.25 | $11,469.29 | AMX×47, FNV×26, ASST×73, ZYME×55 |
| 2026-08-27 | — | $226.25 | AMX×47, FNV×26, ASST×73, ZYME×55 | $11,543.42 | +74.13 | -180.67 | ACMR, MU, CM | AMX, FNV, ASST, ZYME | $1,490.79 | $11,347.76 | ACMR×98, MU×1, CM×9 |
| 2026-08-28 | +0.75 | $1,490.79 | ACMR×98, MU×1, CM×9 | $11,219.48 | -128.28 | -224.65 | KEYS, SMTC, CIEN | ACMR, MU, CM | $1,628.00 | $10,982.35 | KEYS×24, SMTC×7, CIEN×2 |
| 2026-08-31 | -5.85 | $1,628.00 | KEYS×24, SMTC×7, CIEN×2 | $11,050.74 | +68.39 | +0.00 | — | KEYS, SMTC, CIEN | $11,044.56 | $11,044.56 | — |
| 2026-09-01 | -6.30 | $11,044.56 | — | $11,044.56 | -0.00 | +0.00 | — | — | $11,044.56 | $11,044.56 | — |
| 2026-09-02 | -3.83 | $11,044.56 | — | $11,044.56 | -0.00 | +0.00 | — | — | $11,044.56 | $11,044.56 | — |
| 2026-09-03 | -0.90 | $11,044.56 | — | $11,044.56 | -0.00 | +290.95 | AVGO, DELL, CXW, FRNM | — | $483.49 | $11,327.17 | AVGO×21, DELL×2, CXW×34, FRNM×69 |
| 2026-09-04 | +2.25 | $483.49 | AVGO×21, DELL×2, CXW×34, FRNM×69 | $11,333.99 | +6.82 | -204.72 | CRM, MMED, HPE | AVGO, DELL, CXW | $45.53 | $11,116.69 | FRNM×69, CRM×27, MMED×64, HPE×28 |
| 2026-09-08 | -11.47 | $45.53 | FRNM×69, CRM×27, MMED×64, HPE×28 | $10,997.39 | -119.30 | +0.00 | — | FRNM, CRM, MMED, HPE | $10,988.74 | $10,988.74 | — |
| 2026-09-09 | -13.95 | $10,988.74 | — | $10,988.74 | -0.00 | +0.00 | — | — | $10,988.74 | $10,988.74 | — |
| 2026-09-10 | -13.28 | $10,988.74 | — | $10,988.74 | -0.00 | +0.00 | — | — | $10,988.74 | $10,988.74 | — |
| 2026-09-11 | +0.50 | $10,988.74 | — | $10,988.74 | -0.00 | -622.81 | ORCL, BTI, ADBE, CNQ | — | $284.79 | $10,357.69 | ORCL×46, BTI×19, ADBE×4, CNQ×22 |
| 2026-09-14 | -11.00 | $284.79 | ORCL×46, BTI×19, ADBE×4, CNQ×22 | $10,038.15 | -319.54 | +0.00 | — | ORCL, BTI, ADBE, CNQ | $10,029.80 | $10,029.80 | — |
| 2026-09-15 | -3.84 | $10,029.80 | — | $10,029.80 | -0.00 | +0.00 | — | — | $10,029.80 | $10,029.80 | — |
| 2026-09-16 | +5.30 | $10,029.80 | — | $10,029.80 | -0.00 | +9.44 | WAY, QCOM, SM, AMX | — | $63.74 | $10,029.61 | WAY×267, QCOM×5, SM×25, AMX×43 |
| 2026-09-17 | +7.38 | $63.74 | WAY×267, QCOM×5, SM×25, AMX×43 | $10,025.78 | -3.83 | +326.14 | SMTC, AVTR, GME, JBHT | WAY, QCOM, SM, AMX | $56.88 | $10,333.70 | SMTC×41, AVTR×63, GME×45, JBHT×4 |
| 2026-09-18 | +4.86 | $56.88 | SMTC×41, AVTR×63, GME×45, JBHT×4 | $10,509.92 | +176.22 | +5.18 | TH, RARE, BHVN | SMTC, AVTR, JBHT | $9.05 | $10,500.04 | GME×45, TH×317, RARE×96, BHVN×100 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 531 | $13.18 | $6.85 | — | $2,994.57 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $7000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 232 | $4.31 | $2.99 | — | $1,991.66 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 51 | $19.57 | $2.14 | — | $991.44 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+58.7; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $991.44 | ▲ close $10,395.38 vs 09:30 $10,000.00 (session +407.37) | 16:00 close · cash $991.44 · equity $10,395.38 vs 09:30 $10,000.00 (+395.38; session marks +407.37) · 3 name(s) marked open→close (per-name table). HLIT×531 09:30 $13.18 → close $13.92 +392.94; ANGX×232 09:30 $4.31 → close $4.37 +13.92; ARX×51 09:30 $19.57 → close $19.58 +0.51 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $991.44 | ▲ 09:30 equity $10,405.75 vs yday $10,395.38 (+10.37) | 09:30 open · cash $991.44 (unchanged overnight, no fees) · equity $10,405.75 vs prior close $10,395.38 (+10.37) · 3 name(s) re-marked at the open (per-name table). HLIT×531 yday $13.92 → 09:30 $13.84 -42.48; ANGX×232 yday $4.37 → 09:30 $4.60 +53.36; ARX×51 yday $19.58 → 09:30 $19.57 -0.51 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 531 | $13.84 | $7.00 | $+336.61 | $8,333.49 | ▲ +336.61 after sell → book $10,398.76; vs 09:30 mark -6.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 232 | $4.60 | $3.04 | $+61.25 | $9,397.65 | ▲ +61.25 after sell → book $10,395.72; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 51 | $19.57 | $2.16 | $-4.31 | $10,393.55 | ▼ -4.31 after sell → book $10,393.55; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 90 | $46.18 | $2.26 | — | $6,235.09 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $4157.42 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 21 | $142.77 | $2.05 | — | $3,234.87 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3118.07 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $1,205.85 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2078.71 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `GLOB` | 27 | $37.18 | $2.07 | — | $199.92 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; ⚪; ret5=-0.1; leftover $1039.36 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $199.92 | ▲ close $10,592.29 vs 09:30 $10,405.75 (session +207.14) | 16:00 close · cash $199.92 · equity $10,592.29 vs 09:30 $10,405.75 (+186.54; session marks +207.14) · 4 name(s) marked open→close (per-name table). DVN×90 09:30 $46.18 → close $47.57 +125.10; EOG×21 09:30 $142.77 → close $146.15 +70.98; FANG×10 09:30 $202.70 → close $206.29 +35.90; GLOB×27 09:30 $37.18 → close $36.26 -24.84 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $199.92 | ▲ 09:30 equity $10,716.52 vs yday $10,592.29 (+124.23) | 09:30 open · cash $199.92 (unchanged overnight, no fees) · equity $10,716.52 vs prior close $10,592.29 (+124.23) · 4 name(s) re-marked at the open (per-name table). DVN×90 yday $47.57 → 09:30 $48.00 +38.70; EOG×21 yday $146.15 → 09:30 $148.04 +39.69; FANG×10 yday $206.29 → 09:30 $208.93 +26.40; GLOB×27 yday $36.26 → 09:30 $36.98 +19.44 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 90 | $48.00 | $2.31 | $+159.23 | $4,517.61 | ▲ +159.23 after sell → book $10,714.21; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 21 | $148.04 | $2.09 | $+106.53 | $7,624.36 | ▲ +106.53 after sell → book $10,712.12; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $9,711.62 | ▲ +58.23 after sell → book $10,710.08; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `GLOB` | 27 | $36.98 | $2.09 | $-9.56 | $10,707.98 | ▼ -9.56 after sell → book $10,707.98; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,707.98 | ▲ close $10,707.98 vs 09:30 $10,716.52 (session +0.00) | 16:00 close · cash $10,707.98 · no lots left · equity $10,707.98. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,707.98 | ▲ 09:30 equity $10,707.98 vs yday $10,707.98 (+0.00) | 09:30 open · cash $10,707.98 · no holdings · equity $10,707.98 vs prior close $10,707.98 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,707.98 | ▲ close $10,707.98 vs 09:30 $10,707.98 (session +0.00) | 16:00 close · cash $10,707.98 · no lots left · equity $10,707.98. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,707.98 | ▲ 09:30 equity $10,707.98 vs yday $10,707.98 (+0.00) | 09:30 open · cash $10,707.98 · no holdings · equity $10,707.98 vs prior close $10,707.98 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 82 | $91.01 | $2.24 | — | $3,242.93 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $7495.59 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 23 | $44.76 | $2.06 | — | $2,211.39 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1070.80 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 433 | $2.47 | $5.59 | — | $1,136.29 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1070.80 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 18 | $58.73 | $2.04 | — | $77.11 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1070.80 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $77.11 | ▲ close $10,887.08 vs 09:30 $10,707.98 (session +191.02) | 16:00 close · cash $77.11 · equity $10,887.08 vs 09:30 $10,707.98 (+179.10; session marks +191.02) · 4 name(s) marked open→close (per-name table). BHP×82 09:30 $91.01 → close $93.63 +214.84; APA×23 09:30 $44.76 → close $44.39 -8.51; AUTL×433 09:30 $2.47 → close $2.46 -4.33; CRSP×18 09:30 $58.73 → close $58.12 -10.98 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.11 | ▲ 09:30 equity $11,094.58 vs yday $10,887.08 (+207.50) | 09:30 open · cash $77.11 (unchanged overnight, no fees) · equity $11,094.58 vs prior close $10,887.08 (+207.50) · 4 name(s) re-marked at the open (per-name table). BHP×82 yday $93.63 → 09:30 $95.72 +171.38; APA×23 yday $44.39 → 09:30 $44.52 +2.99; AUTL×433 yday $2.46 → 09:30 $2.47 +4.33; CRSP×18 yday $58.12 → 09:30 $59.72 +28.80 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 82 | $95.72 | $2.31 | $+381.67 | $7,923.84 | ▲ +381.67 after sell → book $11,092.27; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 23 | $44.52 | $2.08 | $-9.66 | $8,945.72 | ▼ -9.66 after sell → book $11,090.19; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 52 | $119.43 | $2.15 | — | $2,733.21 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $6262.00 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 23 | $115.18 | $2.06 | — | $82.01 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $2683.72 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.01 | ▲ close $11,343.70 vs 09:30 $11,094.58 (session +257.72) | 16:00 close · cash $82.01 · equity $11,343.70 vs 09:30 $11,094.58 (+249.12; session marks +257.72) · 4 name(s) marked open→close (per-name table). AUTL×433 09:30 $2.47 → close $2.41 -25.98; CRSP×18 09:30 $59.72 → close $59.50 -3.96; AU×52 09:30 $119.43 → close $121.22 +93.08; FUTU×23 09:30 $115.18 → close $123.64 +194.58 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.01 | ▼ 09:30 equity $11,228.23 vs yday $11,343.70 (-115.47) | 09:30 open · cash $82.01 (unchanged overnight, no fees) · equity $11,228.23 vs prior close $11,343.70 (-115.47) · 4 name(s) re-marked at the open (per-name table). AUTL×433 yday $2.41 → 09:30 $2.40 -4.33; CRSP×18 yday $59.50 → 09:30 $58.75 -13.50; AU×52 yday $121.22 → 09:30 $120.51 -36.92; FUTU×23 yday $123.64 → 09:30 $121.00 -60.72 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 433 | $2.40 | $5.67 | $-41.56 | $1,115.55 | ▼ -41.56 after sell → book $11,222.57; vs 09:30 mark -5.66 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 52 | $120.51 | $2.21 | $+51.81 | $7,379.86 | ▲ +51.81 after sell → book $11,220.36; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 23 | $121.00 | $2.09 | $+129.71 | $10,160.77 | ▲ +129.71 after sell → book $11,218.27; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,160.77 | ▼ close $11,188.12 vs 09:30 $11,228.23 (session -30.15) | 16:00 close · cash $10,160.77 · equity $11,188.12 vs 09:30 $11,228.23 (-40.11; session marks -30.15) · 1 name(s) marked open→close (per-name table). CRSP×18 09:30 $58.75 → close $57.08 -30.15 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,160.77 | ▲ 09:30 equity $11,203.51 vs yday $11,188.12 (+15.39) | 09:30 open · cash $10,160.77 (unchanged overnight, no fees) · equity $11,203.51 vs prior close $11,188.12 (+15.39) · 1 name(s) re-marked at the open (per-name table). CRSP×18 yday $57.08 → 09:30 $57.93 +15.39 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 18 | $57.93 | $2.06 | $-18.51 | $11,201.44 | ▼ -18.51 after sell → book $11,201.44; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 66 | $118.52 | $2.19 | — | $3,376.94 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $7841.01 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 14 | $77.13 | $2.03 | — | $2,295.08 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1120.14 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 31 | $35.05 | $2.08 | — | $1,206.45 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,oppset; 🔵; ⚪; ret5=+19.7; leftover $1120.14 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 47 | $23.80 | $2.13 | — | $85.72 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; 🔵; ret5=+0.5; leftover $1120.14 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.72 | ▲ close $11,556.58 vs 09:30 $11,203.51 (session +363.57) | 16:00 close · cash $85.72 · equity $11,556.58 vs 09:30 $11,203.51 (+353.07; session marks +363.57) · 4 name(s) marked open→close (per-name table). AU×66 09:30 $118.52 → close $123.39 +321.42; FCX×14 09:30 $77.13 → close $79.91 +38.92; EZPW×31 09:30 $35.05 → close $35.23 +5.58; AMX×47 09:30 $23.80 → close $23.75 -2.35 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.72 | ▼ 09:30 equity $11,326.23 vs yday $11,556.58 (-230.35) | 09:30 open · cash $85.72 (unchanged overnight, no fees) · equity $11,326.23 vs prior close $11,556.58 (-230.35) · 4 name(s) re-marked at the open (per-name table). AU×66 yday $123.39 → 09:30 $119.80 -236.94; FCX×14 yday $79.91 → 09:30 $79.34 -7.98; EZPW×31 yday $35.23 → 09:30 $35.70 +14.57; AMX×47 yday $23.75 → 09:30 $23.75 +0.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 66 | $119.80 | $2.26 | $+80.03 | $7,990.26 | ▲ +80.03 after sell → book $11,323.97; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 14 | $79.34 | $2.05 | $+26.86 | $9,098.97 | ▲ +26.86 after sell → book $11,321.92; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 31 | $35.70 | $2.10 | $+15.96 | $10,203.56 | ▲ +15.96 after sell → book $11,319.81; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 26 | $267.02 | $2.07 | — | $3,258.97 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $7142.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 73 | $20.72 | $2.21 | — | $1,744.21 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; 🔵; ret5=+67.1; leftover $1530.53 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ZYME` | 55 | $27.56 | $2.15 | — | $226.25 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; 🔵; ret5=+5.4; leftover $1530.53 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $226.25 | ▲ close $11,469.29 vs 09:30 $11,326.23 (session +155.91) | 16:00 close · cash $226.25 · equity $11,469.29 vs 09:30 $11,326.23 (+143.06; session marks +155.91) · 4 name(s) marked open→close (per-name table). AMX×47 09:30 $23.75 → close $23.62 -6.11; FNV×26 09:30 $267.02 → close $267.37 +9.10; ASST×73 09:30 $20.72 → close $21.50 +56.94; ZYME×55 09:30 $27.56 → close $29.30 +95.98 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.25 | ▲ 09:30 equity $11,543.42 vs yday $11,469.29 (+74.13) | 09:30 open · cash $226.25 (unchanged overnight, no fees) · equity $11,543.42 vs prior close $11,469.29 (+74.13) · 4 name(s) re-marked at the open (per-name table). AMX×47 yday $23.62 → 09:30 $23.77 +7.05; FNV×26 yday $267.37 → 09:30 $267.23 -3.64; ASST×73 yday $21.50 → 09:30 $22.45 +69.35; ZYME×55 yday $29.30 → 09:30 $29.33 +1.37 | — |
| 2026-08-27 09:30 ET | **SELL** | `AMX` | 47 | $23.77 | $2.15 | $-5.69 | $1,341.29 | ▼ -5.69 after sell → book $11,541.27; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 26 | $267.23 | $2.13 | $+1.26 | $8,287.14 | ▲ +1.26 after sell → book $11,539.14; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 73 | $22.45 | $2.23 | $+121.85 | $9,923.75 | ▲ +121.85 after sell → book $11,536.90; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZYME` | 55 | $29.33 | $2.18 | $+93.02 | $11,534.72 | ▲ +93.02 after sell → book $11,534.72; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 98 | $81.65 | $2.28 | — | $3,530.74 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $8074.31 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $2,561.74 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1153.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 9 | $118.77 | $2.02 | — | $1,490.79 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; ret5=+0.3; leftover $1153.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,490.79 | ▼ close $11,347.76 vs 09:30 $11,543.42 (session -180.67) | 16:00 close · cash $1,490.79 · equity $11,347.76 vs 09:30 $11,543.42 (-195.66; session marks -180.67) · 3 name(s) marked open→close (per-name table). ACMR×98 09:30 $81.65 → close $80.49 -113.68; MU×1 09:30 $967.01 → close $935.39 -31.62; CM×9 09:30 $118.77 → close $114.84 -35.37 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,490.79 | ▼ 09:30 equity $11,219.48 vs yday $11,347.76 (-128.28) | 09:30 open · cash $1,490.79 (unchanged overnight, no fees) · equity $11,219.48 vs prior close $11,347.76 (-128.28) · 3 name(s) re-marked at the open (per-name table). ACMR×98 yday $80.49 → 09:30 $79.27 -119.56; MU×1 yday $935.39 → 09:30 $919.29 -16.10; CM×9 yday $114.84 → 09:30 $115.66 +7.38 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 98 | $79.27 | $2.36 | $-237.89 | $9,256.89 | ▼ -237.89 after sell → book $11,217.12; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $10,174.16 | ▼ -51.73 after sell → book $11,215.10; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 9 | $115.66 | $2.04 | $-32.04 | $11,213.07 | ▼ -32.04 after sell → book $11,213.07; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 24 | $324.41 | $2.06 | — | $3,425.17 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $7849.15 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $2,430.83 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1121.31 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $1,628.00 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1121.31 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,628.00 | ▼ close $10,982.35 vs 09:30 $11,219.48 (session -224.65) | 16:00 close · cash $1,628.00 · equity $10,982.35 vs 09:30 $11,219.48 (-237.13; session marks -224.65) · 3 name(s) marked open→close (per-name table). KEYS×24 09:30 $324.41 → close $319.97 -106.56; SMTC×7 09:30 $141.76 → close $131.17 -74.13; CIEN×2 09:30 $400.42 → close $378.44 -43.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,628.00 | ▲ 09:30 equity $11,050.74 vs yday $10,982.35 (+68.39) | 09:30 open · cash $1,628.00 (unchanged overnight, no fees) · equity $11,050.74 vs prior close $10,982.35 (+68.39) · 3 name(s) re-marked at the open (per-name table). KEYS×24 yday $319.97 → 09:30 $322.49 +60.48; SMTC×7 yday $131.17 → 09:30 $132.30 +7.91; CIEN×2 yday $378.44 → 09:30 $378.44 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 24 | $322.49 | $2.13 | $-50.28 | $9,365.62 | ▼ -50.28 after sell → book $11,048.60; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 7 | $132.30 | $2.03 | $-70.26 | $10,289.69 | ▼ -70.26 after sell → book $11,046.57; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 2 | $378.44 | $2.02 | $-47.97 | $11,044.56 | ▼ -47.97 after sell → book $11,044.56; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,044.56 | ▲ close $11,044.56 vs 09:30 $11,050.74 (session +0.00) | 16:00 close · cash $11,044.56 · no lots left · equity $11,044.56. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,044.56 | ▲ 09:30 equity $11,044.56 vs yday $11,044.56 (-0.00) | 09:30 open · cash $11,044.56 · no holdings · equity $11,044.56 vs prior close $11,044.56 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,044.56 | ▲ close $11,044.56 vs 09:30 $11,044.56 (session +0.00) | 16:00 close · cash $11,044.56 · no lots left · equity $11,044.56. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,044.56 | ▲ 09:30 equity $11,044.56 vs yday $11,044.56 (-0.00) | 09:30 open · cash $11,044.56 · no holdings · equity $11,044.56 vs prior close $11,044.56 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,044.56 | ▲ close $11,044.56 vs 09:30 $11,044.56 (session +0.00) | 16:00 close · cash $11,044.56 · no lots left · equity $11,044.56. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,044.56 | ▲ 09:30 equity $11,044.56 vs yday $11,044.56 (-0.00) | 09:30 open · cash $11,044.56 · no holdings · equity $11,044.56 vs prior close $11,044.56 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 21 | $351.74 | $2.05 | — | $3,655.96 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $7731.19 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $2,681.35 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $1104.46 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 34 | $32.31 | $2.09 | — | $1,580.72 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1104.46 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 69 | $15.87 | $2.20 | — | $483.49 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1104.46 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $483.49 | ▲ close $11,327.17 vs 09:30 $11,044.56 (session +290.95) | 16:00 close · cash $483.49 · equity $11,327.17 vs 09:30 $11,044.56 (+282.61; session marks +290.95) · 4 name(s) marked open→close (per-name table). AVGO×21 09:30 $351.74 → close $357.16 +113.82; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×34 09:30 $32.31 → close $33.66 +45.90; FRNM×69 09:30 $15.87 → close $16.90 +71.07 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $483.49 | ▲ 09:30 equity $11,333.99 vs yday $11,327.17 (+6.82) | 09:30 open · cash $483.49 (unchanged overnight, no fees) · equity $11,333.99 vs prior close $11,327.17 (+6.82) · 4 name(s) re-marked at the open (per-name table). AVGO×21 yday $357.16 → 09:30 $359.70 +53.34; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×34 yday $33.66 → 09:30 $33.46 -6.80; FRNM×69 yday $16.90 → 09:30 $16.40 -34.50 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 21 | $359.70 | $2.12 | $+162.98 | $8,035.07 | ▲ +162.98 after sell → book $11,331.87; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $9,060.61 | ▲ +50.93 after sell → book $11,329.85; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 34 | $33.46 | $2.11 | $+34.90 | $10,196.14 | ▲ +34.90 after sell → book $11,327.74; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 27 | $263.36 | $2.07 | — | $3,083.35 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $7137.30 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MMED` | 64 | $23.84 | $2.18 | — | $1,555.41 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; 🔵; ⚪; ret5=+22.2; leftover $1529.42 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HPE` | 28 | $53.85 | $2.07 | — | $45.53 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; 🔵; ret5=+0.1; leftover $1529.42 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟡 vol🟢 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.53 | ▼ close $11,116.69 vs 09:30 $11,333.99 (session -204.72) | 16:00 close · cash $45.53 · equity $11,116.69 vs 09:30 $11,333.99 (-217.30; session marks -204.72) · 4 name(s) marked open→close (per-name table). FRNM×69 09:30 $16.40 → close $16.31 -6.21; CRM×27 09:30 $263.36 → close $259.23 -111.51; MMED×64 09:30 $23.84 → close $23.29 -35.20; HPE×28 09:30 $53.85 → close $52.00 -51.80 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.53 | ▼ 09:30 equity $10,997.39 vs yday $11,116.69 (-119.30) | 09:30 open · cash $45.53 (unchanged overnight, no fees) · equity $10,997.39 vs prior close $11,116.69 (-119.30) · 4 name(s) re-marked at the open (per-name table). FRNM×69 yday $16.31 → 09:30 $16.74 +29.67; CRM×27 yday $259.23 → 09:30 $253.72 -148.77; MMED×64 yday $23.29 → 09:30 $23.16 -8.32; HPE×28 yday $52.00 → 09:30 $52.29 +8.12 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 69 | $16.74 | $2.22 | $+55.61 | $1,198.37 | ▲ +55.61 after sell → book $10,995.17; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 27 | $253.72 | $2.14 | $-264.49 | $8,046.68 | ▼ -264.49 after sell → book $10,993.04; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 64 | $23.16 | $2.20 | $-47.91 | $9,526.71 | ▼ -47.91 after sell → book $10,990.83; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HPE` | 28 | $52.29 | $2.10 | $-47.85 | $10,988.74 | ▼ -47.85 after sell → book $10,988.74; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,988.74 | ▲ close $10,988.74 vs 09:30 $10,997.39 (session +0.00) | 16:00 close · cash $10,988.74 · no lots left · equity $10,988.74. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,988.74 | ▲ 09:30 equity $10,988.74 vs yday $10,988.74 (-0.00) | 09:30 open · cash $10,988.74 · no holdings · equity $10,988.74 vs prior close $10,988.74 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,988.74 | ▲ close $10,988.74 vs 09:30 $10,988.74 (session +0.00) | 16:00 close · cash $10,988.74 · no lots left · equity $10,988.74. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,988.74 | ▲ 09:30 equity $10,988.74 vs yday $10,988.74 (-0.00) | 09:30 open · cash $10,988.74 · no holdings · equity $10,988.74 vs prior close $10,988.74 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,988.74 | ▲ close $10,988.74 vs 09:30 $10,988.74 (session +0.00) | 16:00 close · cash $10,988.74 · no lots left · equity $10,988.74. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,988.74 | ▲ 09:30 equity $10,988.74 vs yday $10,988.74 (-0.00) | 09:30 open · cash $10,988.74 · no holdings · equity $10,988.74 vs prior close $10,988.74 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 46 | $164.43 | $2.13 | — | $3,422.83 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $7692.12 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 19 | $56.03 | $2.05 | — | $2,356.21 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; 🔵; ret5=-0.8; leftover $1098.87 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 4 | $242.17 | $2.00 | — | $1,385.53 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; ret5=-11.1; leftover $1098.87 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CNQ` | 22 | $49.94 | $2.06 | — | $284.79 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; ret5=+1.7; leftover $1098.87 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $284.79 | ▼ close $10,357.69 vs 09:30 $10,988.74 (session -622.81) | 16:00 close · cash $284.79 · equity $10,357.69 vs 09:30 $10,988.74 (-631.05; session marks -622.81) · 4 name(s) marked open→close (per-name table). ORCL×46 09:30 $164.43 → close $150.28 -650.90; BTI×19 09:30 $56.03 → close $55.24 -15.01; ADBE×4 09:30 $242.17 → close $252.23 +40.24; CNQ×22 09:30 $49.94 → close $50.07 +2.86 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $284.79 | ▼ 09:30 equity $10,038.15 vs yday $10,357.69 (-319.54) | 09:30 open · cash $284.79 (unchanged overnight, no fees) · equity $10,038.15 vs prior close $10,357.69 (-319.54) · 4 name(s) re-marked at the open (per-name table). ORCL×46 yday $150.28 → 09:30 $141.42 -407.56; BTI×19 yday $55.24 → 09:30 $57.12 +35.72; ADBE×4 yday $252.23 → 09:30 $261.51 +37.12; CNQ×22 yday $50.07 → 09:30 $50.76 +15.18 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 46 | $141.42 | $2.19 | $-1062.78 | $6,787.92 | ▼ -1,062.78 after sell → book $10,035.96; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `BTI` | 19 | $57.12 | $2.07 | $+16.60 | $7,871.14 | ▲ +16.60 after sell → book $10,033.90; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 4 | $261.51 | $2.02 | $+73.34 | $8,915.15 | ▲ +73.34 after sell → book $10,031.87; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CNQ` | 22 | $50.76 | $2.08 | $+13.91 | $10,029.80 | ▲ +13.91 after sell → book $10,029.80; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,029.80 | ▲ close $10,029.80 vs 09:30 $10,038.15 (session +0.00) | 16:00 close · cash $10,029.80 · no lots left · equity $10,029.80. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,029.80 | ▲ 09:30 equity $10,029.80 vs yday $10,029.80 (-0.00) | 09:30 open · cash $10,029.80 · no holdings · equity $10,029.80 vs prior close $10,029.80 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,029.80 | ▲ close $10,029.80 vs 09:30 $10,029.80 (session +0.00) | 16:00 close · cash $10,029.80 · no lots left · equity $10,029.80. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,029.80 | ▲ 09:30 equity $10,029.80 vs yday $10,029.80 (-0.00) | 09:30 open · cash $10,029.80 · no holdings · equity $10,029.80 vs prior close $10,029.80 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 267 | $26.27 | $3.44 | — | $3,012.26 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $7020.86 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 5 | $189.17 | $2.00 | — | $2,064.41 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $1002.98 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 25 | $39.99 | $2.06 | — | $1,062.59 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1002.98 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AMX` | 43 | $23.18 | $2.12 | — | $63.74 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list oppset; 🔵; ret5=-0.2; leftover $1002.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.74 | ▲ close $10,029.61 vs 09:30 $10,029.80 (session +9.44) | 16:00 close · cash $63.74 · equity $10,029.61 vs 09:30 $10,029.80 (-0.19; session marks +9.44) · 4 name(s) marked open→close (per-name table). WAY×267 09:30 $26.27 → close $26.59 +85.44; QCOM×5 09:30 $189.17 → close $184.84 -21.65; SM×25 09:30 $39.99 → close $38.16 -45.75; AMX×43 09:30 $23.18 → close $22.98 -8.60 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.74 | ▼ 09:30 equity $10,025.78 vs yday $10,029.61 (-3.83) | 09:30 open · cash $63.74 (unchanged overnight, no fees) · equity $10,025.78 vs prior close $10,029.61 (-3.83) · 4 name(s) re-marked at the open (per-name table). WAY×267 yday $26.59 → 09:30 $26.51 -21.36; QCOM×5 yday $184.84 → 09:30 $190.35 +27.55; SM×25 yday $38.16 → 09:30 $37.57 -14.75; AMX×43 yday $22.98 → 09:30 $23.09 +4.73 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 267 | $26.51 | $3.55 | $+57.09 | $7,138.36 | ▲ +57.09 after sell → book $10,022.23; vs 09:30 mark -3.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 5 | $190.35 | $2.02 | $+1.87 | $8,088.09 | ▲ +1.87 after sell → book $10,020.21; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 25 | $37.57 | $2.08 | $-64.65 | $9,025.25 | ▼ -64.65 after sell → book $10,018.12; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AMX` | 43 | $23.09 | $2.14 | $-8.13 | $10,015.98 | ▼ -8.13 after sell → book $10,015.98; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 41 | $170.85 | $2.11 | — | $3,009.02 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $7011.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 63 | $15.81 | $2.18 | — | $2,010.81 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1001.60 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 45 | $22.12 | $2.12 | — | $1,013.28 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1001.60 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 4 | $238.60 | $2.00 | — | $56.88 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_mover,oppset; ret5=-11.6; leftover $1001.60 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.88 | ▲ close $10,333.70 vs 09:30 $10,025.78 (session +326.14) | 16:00 close · cash $56.88 · equity $10,333.70 vs 09:30 $10,025.78 (+307.92; session marks +326.14) · 4 name(s) marked open→close (per-name table). SMTC×41 09:30 $170.85 → close $178.19 +300.94; AVTR×63 09:30 $15.81 → close $15.86 +3.15; GME×45 09:30 $22.12 → close $22.77 +29.25; JBHT×4 09:30 $238.60 → close $236.80 -7.20 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.88 | ▲ 09:30 equity $10,509.92 vs yday $10,333.70 (+176.22) | 09:30 open · cash $56.88 (unchanged overnight, no fees) · equity $10,509.92 vs prior close $10,333.70 (+176.22) · 4 name(s) re-marked at the open (per-name table). SMTC×41 yday $178.19 → 09:30 $182.33 +169.74; AVTR×63 yday $15.86 → 09:30 $15.87 +0.63; GME×45 yday $22.77 → 09:30 $22.90 +5.85; JBHT×4 yday $236.80 → 09:30 $236.80 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 41 | $182.33 | $2.18 | $+466.38 | $7,530.23 | ▲ +466.38 after sell → book $10,507.74; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 63 | $15.87 | $2.20 | $-0.60 | $8,527.84 | ▼ -0.60 after sell → book $10,505.54; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 4 | $236.80 | $2.02 | $-11.22 | $9,473.02 | ▼ -11.22 after sell → book $10,503.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 317 | $20.91 | $4.09 | — | $2,840.46 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $6631.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 96 | $14.79 | $2.28 | — | $1,418.34 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1420.95 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 100 | $14.07 | $2.29 | — | $9.05 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1420.95 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.05 | ▲ close $10,500.04 vs 09:30 $10,509.92 (session +5.18) | 16:00 close · cash $9.05 · equity $10,500.04 vs 09:30 $10,509.92 (-9.88; session marks +5.18) · 4 name(s) marked open→close (per-name table). GME×45 09:30 $22.90 → close $22.64 -11.70; TH×317 09:30 $20.91 → close $21.19 +88.76; RARE×96 09:30 $14.79 → close $14.51 -26.88; BHVN×100 09:30 $14.07 → close $13.62 -45.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1000.00 < 1 share @ 1646.93 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HDSN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1153.47 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1121.31 < 1 share @ 1306.03 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `RPRX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QCOM` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SRRK` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GME` | 45 | 2026-09-17 @ $22.12 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1001.60 |
| `TH` | 317 | 2026-09-18 @ $20.91 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $6631.11 |
| `RARE` | 96 | 2026-09-18 @ $14.79 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1420.95 |
| `BHVN` | 100 | 2026-09-18 @ $14.07 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1420.95 |
