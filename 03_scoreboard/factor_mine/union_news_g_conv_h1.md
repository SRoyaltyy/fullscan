# Factor mine action — `union_news_g_conv_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `conviction` · sell `list` · S-boost `none` · merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5

Cash book **+5.01%** ($10,501) · signal-only (no cash/fees) was +12.75%. Starts YES **7/27**. Fills 96 · skips 41 · realized $+784.85.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $304.79.

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
| 2026-08-17 | `OUST` | 21 | — | $49.00 | +0.00 | $48.13 | -18.27 | -18.27 | +0.00 | -18.27 |
| 2026-08-18 | `DVN` | 90 | $47.57 | $48.00 | +38.70 | — | +0.00 | +38.70 | +163.80 | — |
| 2026-08-18 | `EOG` | 21 | $146.15 | $148.04 | +39.69 | — | +0.00 | +39.69 | +110.67 | — |
| 2026-08-18 | `FANG` | 10 | $206.29 | $208.93 | +26.40 | — | +0.00 | +26.40 | +62.30 | — |
| 2026-08-18 | `OUST` | 21 | $48.13 | $45.09 | -63.84 | — | +0.00 | -63.84 | -82.11 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 81 | — | $91.01 | +0.00 | $93.63 | +212.22 | +212.22 | +0.00 | +212.22 |
| 2026-08-20 | `APA` | 23 | — | $44.76 | +0.00 | $44.39 | -8.51 | -8.51 | +0.00 | -8.51 |
| 2026-08-20 | `AUTL` | 430 | — | $2.47 | +0.00 | $2.46 | -4.30 | -4.30 | +0.00 | -4.30 |
| 2026-08-20 | `CRSP` | 18 | — | $58.73 | +0.00 | $58.12 | -10.98 | -10.98 | +0.00 | -10.98 |
| 2026-08-21 | `BHP` | 81 | $93.63 | $95.72 | +169.29 | — | +0.00 | +169.29 | +381.51 | — |
| 2026-08-21 | `APA` | 23 | $44.39 | $44.52 | +2.99 | — | +0.00 | +2.99 | -5.52 | — |
| 2026-08-21 | `AUTL` | 430 | $2.46 | $2.47 | +4.30 | $2.41 | -25.80 | -21.50 | +0.00 | -25.80 |
| 2026-08-21 | `CRSP` | 18 | $58.12 | $59.72 | +28.80 | $59.50 | -3.96 | +24.84 | +17.82 | +13.86 |
| 2026-08-21 | `AU` | 51 | — | $119.43 | +0.00 | $121.22 | +91.29 | +91.29 | +0.00 | +91.29 |
| 2026-08-21 | `FUTU` | 23 | — | $115.18 | +0.00 | $123.64 | +194.58 | +194.58 | +0.00 | +194.58 |
| 2026-08-24 | `AUTL` | 430 | $2.41 | $2.40 | -4.30 | — | +0.00 | -4.30 | -30.10 | — |
| 2026-08-24 | `CRSP` | 18 | $59.50 | $58.75 | -13.50 | $57.08 | -30.15 | -43.65 | +0.36 | -29.79 |
| 2026-08-24 | `AU` | 51 | $121.22 | $120.51 | -36.21 | — | +0.00 | -36.21 | +55.08 | — |
| 2026-08-24 | `FUTU` | 23 | $123.64 | $121.00 | -60.72 | — | +0.00 | -60.72 | +133.86 | — |
| 2026-08-25 | `CRSP` | 18 | $57.08 | $57.93 | +15.39 | — | +0.00 | +15.39 | -14.40 | — |
| 2026-08-25 | `AU` | 65 | — | $118.52 | +0.00 | $123.39 | +316.55 | +316.55 | +0.00 | +316.55 |
| 2026-08-25 | `FCX` | 14 | — | $77.13 | +0.00 | $79.91 | +38.92 | +38.92 | +0.00 | +38.92 |
| 2026-08-25 | `EZPW` | 31 | — | $35.05 | +0.00 | $35.23 | +5.58 | +5.58 | +0.00 | +5.58 |
| 2026-08-25 | `RUM` | 118 | — | $9.42 | +0.00 | $10.23 | +95.58 | +95.58 | +0.00 | +95.58 |
| 2026-08-26 | `AU` | 65 | $123.39 | $119.80 | -233.35 | — | +0.00 | -233.35 | +83.20 | — |
| 2026-08-26 | `FCX` | 14 | $79.91 | $79.34 | -7.98 | — | +0.00 | -7.98 | +30.94 | — |
| 2026-08-26 | `EZPW` | 31 | $35.23 | $35.70 | +14.57 | — | +0.00 | +14.57 | +20.15 | — |
| 2026-08-26 | `RUM` | 118 | $10.23 | $10.07 | -18.88 | — | +0.00 | -18.88 | +76.70 | — |
| 2026-08-26 | `FNV` | 29 | — | $267.02 | +0.00 | $267.37 | +10.15 | +10.15 | +0.00 | +10.15 |
| 2026-08-26 | `CM` | 9 | — | $118.50 | +0.00 | $118.20 | -2.70 | -2.70 | +0.00 | -2.70 |
| 2026-08-26 | `TRLV` | 100 | — | $11.22 | +0.00 | $11.43 | +21.00 | +21.00 | +0.00 | +21.00 |
| 2026-08-26 | `CAPR` | 136 | — | $8.29 | +0.00 | $9.36 | +145.52 | +145.52 | +0.00 | +145.52 |
| 2026-08-27 | `FNV` | 29 | $267.37 | $267.23 | -4.06 | — | +0.00 | -4.06 | +6.09 | — |
| 2026-08-27 | `CM` | 9 | $118.20 | $118.77 | +5.13 | $114.84 | -35.37 | -30.24 | +2.43 | -32.94 |
| 2026-08-27 | `TRLV` | 100 | $11.43 | $11.38 | -5.00 | — | +0.00 | -5.00 | +16.00 | — |
| 2026-08-27 | `CAPR` | 136 | $9.36 | $9.19 | -23.12 | — | +0.00 | -23.12 | +122.40 | — |
| 2026-08-27 | `ACMR` | 88 | — | $81.65 | +0.00 | $80.49 | -102.08 | -102.08 | +0.00 | -102.08 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-28 | `CM` | 9 | $114.84 | $115.66 | +7.38 | — | +0.00 | +7.38 | -25.56 | — |
| 2026-08-28 | `ACMR` | 88 | $80.49 | $79.27 | -107.36 | — | +0.00 | -107.36 | -209.44 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
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
| 2026-09-03 | `CXW` | 33 | — | $32.31 | +0.00 | $33.66 | +44.55 | +44.55 | +0.00 | +44.55 |
| 2026-09-03 | `FRNM` | 69 | — | $15.87 | +0.00 | $16.90 | +71.07 | +71.07 | +0.00 | +71.07 |
| 2026-09-04 | `AVGO` | 21 | $357.16 | $359.70 | +53.34 | — | +0.00 | +53.34 | +167.16 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `CXW` | 33 | $33.66 | $33.46 | -6.60 | — | +0.00 | -6.60 | +37.95 | — |
| 2026-09-04 | `FRNM` | 69 | $16.90 | $16.40 | -34.50 | $16.31 | -6.21 | -40.71 | +36.57 | +30.36 |
| 2026-09-04 | `CRM` | 26 | — | $263.36 | +0.00 | $259.23 | -107.38 | -107.38 | +0.00 | -107.38 |
| 2026-09-04 | `MRX` | 20 | — | $75.65 | +0.00 | $78.27 | +52.40 | +52.40 | +0.00 | +52.40 |
| 2026-09-04 | `BE` | 6 | — | $236.82 | +0.00 | $252.87 | +96.30 | +96.30 | +0.00 | +96.30 |
| 2026-09-08 | `FRNM` | 69 | $16.31 | $16.74 | +29.67 | — | +0.00 | +29.67 | +60.03 | — |
| 2026-09-08 | `CRM` | 26 | $259.23 | $253.72 | -143.26 | — | +0.00 | -143.26 | -250.64 | — |
| 2026-09-08 | `MRX` | 20 | $78.27 | $78.84 | +11.40 | $76.71 | -42.60 | -31.20 | +63.80 | +21.20 |
| 2026-09-08 | `BE` | 6 | $252.87 | $267.76 | +89.34 | — | +0.00 | +89.34 | +185.64 | — |
| 2026-09-09 | `MRX` | 20 | $76.71 | $76.60 | -2.20 | — | +0.00 | -2.20 | +19.00 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 47 | — | $164.43 | +0.00 | $150.28 | -665.05 | -665.05 | +0.00 | -665.05 |
| 2026-09-11 | `ADBE` | 4 | — | $242.17 | +0.00 | $252.23 | +40.24 | +40.24 | +0.00 | +40.24 |
| 2026-09-11 | `BAK` | 529 | — | $2.12 | +0.00 | $2.08 | -21.16 | -21.16 | +0.00 | -21.16 |
| 2026-09-11 | `AMTX` | 550 | — | $2.04 | +0.00 | $2.01 | -16.50 | -16.50 | +0.00 | -16.50 |
| 2026-09-14 | `ORCL` | 47 | $150.28 | $141.42 | -416.42 | — | +0.00 | -416.42 | -1081.47 | — |
| 2026-09-14 | `ADBE` | 4 | $252.23 | $261.51 | +37.12 | — | +0.00 | +37.12 | +77.36 | — |
| 2026-09-14 | `BAK` | 529 | $2.08 | $2.05 | -15.87 | — | +0.00 | -15.87 | -37.03 | — |
| 2026-09-14 | `AMTX` | 550 | $2.01 | $2.01 | +0.00 | — | +0.00 | +0.00 | -16.50 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 269 | — | $26.27 | +0.00 | $26.59 | +86.08 | +86.08 | +0.00 | +86.08 |
| 2026-09-16 | `QCOM` | 5 | — | $189.17 | +0.00 | $184.84 | -21.65 | -21.65 | +0.00 | -21.65 |
| 2026-09-16 | `SM` | 25 | — | $39.99 | +0.00 | $38.16 | -45.75 | -45.75 | +0.00 | -45.75 |
| 2026-09-16 | `CLS` | 3 | — | $320.20 | +0.00 | $323.83 | +10.89 | +10.89 | +0.00 | +10.89 |
| 2026-09-17 | `WAY` | 269 | $26.59 | $26.51 | -21.52 | — | +0.00 | -21.52 | +64.56 | — |
| 2026-09-17 | `QCOM` | 5 | $184.84 | $190.35 | +27.55 | — | +0.00 | +27.55 | +5.90 | — |
| 2026-09-17 | `SM` | 25 | $38.16 | $37.57 | -14.75 | — | +0.00 | -14.75 | -60.50 | — |
| 2026-09-17 | `CLS` | 3 | $323.83 | $337.75 | +41.76 | $329.94 | -23.43 | +18.33 | +52.65 | +29.22 |
| 2026-09-17 | `SMTC` | 37 | — | $170.85 | +0.00 | $178.19 | +271.58 | +271.58 | +0.00 | +271.58 |
| 2026-09-17 | `GME` | 62 | — | $22.12 | +0.00 | $22.77 | +40.30 | +40.30 | +0.00 | +40.30 |
| 2026-09-17 | `JBHT` | 5 | — | $238.60 | +0.00 | $236.80 | -9.00 | -9.00 | +0.00 | -9.00 |
| 2026-09-18 | `CLS` | 3 | $329.94 | $332.06 | +6.36 | $332.63 | +1.71 | +8.07 | +35.58 | +37.29 |
| 2026-09-18 | `SMTC` | 37 | $178.19 | $182.33 | +153.18 | — | +0.00 | +153.18 | +424.76 | — |
| 2026-09-18 | `GME` | 62 | $22.77 | $22.90 | +8.06 | $22.64 | -16.12 | -8.06 | +48.36 | +32.24 |
| 2026-09-18 | `JBHT` | 5 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -9.00 | — |
| 2026-09-18 | `TH` | 274 | — | $20.91 | +0.00 | $21.19 | +76.72 | +76.72 | +0.00 | +76.72 |
| 2026-09-18 | `RARE` | 166 | — | $14.79 | +0.00 | $14.51 | -46.48 | -46.48 | +0.00 | -46.48 |
| 2026-09-21 | `CLS` | 3 | $332.63 | $341.45 | +26.46 | — | +0.00 | +26.46 | +63.75 | — |
| 2026-09-21 | `GME` | 62 | $22.64 | $22.78 | +8.68 | — | +0.00 | +8.68 | +40.92 | — |
| 2026-09-21 | `TH` | 274 | $21.19 | $21.65 | +126.04 | — | +0.00 | +126.04 | +202.76 | — |
| 2026-09-21 | `RARE` | 166 | $14.51 | $14.58 | +11.62 | — | +0.00 | +11.62 | -34.86 | — |
| 2026-09-21 | `VICR` | 32 | — | $230.25 | +0.00 | $223.90 | -203.20 | -203.20 | +0.00 | -203.20 |
| 2026-09-21 | `SMTC` | 5 | — | $190.30 | +0.00 | $177.37 | -64.65 | -64.65 | +0.00 | -64.65 |
| 2026-09-21 | `ALVO` | 182 | — | $5.92 | +0.00 | $5.88 | -7.28 | -7.28 | +0.00 | -7.28 |
| 2026-09-21 | `SION` | 179 | — | $6.00 | +0.00 | $6.00 | +0.00 | +0.00 | +0.00 | +0.00 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +407.37 | HLIT, ANGX, ARX | — | $991.44 | $10,395.38 | HLIT×531, ANGX×232, ARX×51 |
| 2026-08-17 | +2.25 | $991.44 | HLIT×531, ANGX×232, ARX×51 | $10,405.75 | +10.37 | +213.71 | DVN, EOG, FANG, OUST | HLIT, ANGX, ARX | $174.80 | $10,598.88 | DVN×90, EOG×21, FANG×10, OUST×21 |
| 2026-08-18 | -6.20 | $174.80 | DVN×90, EOG×21, FANG×10, OUST×21 | $10,639.83 | +40.95 | +0.00 | — | DVN, EOG, FANG, OUST | $10,631.31 | $10,631.31 | — |
| 2026-08-19 | -7.20 | $10,631.31 | — | $10,631.31 | +0.00 | +0.00 | — | — | $10,631.31 | $10,631.31 | — |
| 2026-08-20 | +1.12 | $10,631.31 | — | $10,631.31 | +0.00 | +188.43 | BHP, APA, AUTL, CRSP | — | $98.90 | $10,807.86 | BHP×81, APA×23, AUTL×430, CRSP×18 |
| 2026-08-21 | +3.25 | $98.90 | BHP×81, APA×23, AUTL×430, CRSP×18 | $11,013.24 | +205.38 | +256.11 | AU, FUTU | BHP, APA | $127.52 | $11,260.76 | AUTL×430, CRSP×18, AU×51, FUTU×23 |
| 2026-08-24 | -5.17 | $127.52 | AUTL×430, CRSP×18, AU×51, FUTU×23 | $11,146.03 | -114.73 | -30.15 | — | AUTL, AU, FUTU | $10,078.61 | $11,105.96 | CRSP×18 |
| 2026-08-25 | +1.80 | $10,078.61 | CRSP×18 | $11,121.35 | +15.39 | +456.63 | AU, FCX, EZPW, RUM | CRSP | $128.91 | $11,567.27 | AU×65, FCX×14, EZPW×31, RUM×118 |
| 2026-08-26 | +2.02 | $128.91 | AU×65, FCX×14, EZPW×31, RUM×118 | $11,321.63 | -245.64 | +173.97 | FNV, CM, TRLV, CAPR | AU, FCX, EZPW, RUM | $244.54 | $11,478.03 | FNV×29, CM×9, TRLV×100, CAPR×136 |
| 2026-08-27 | — | $244.54 | FNV×29, CM×9, TRLV×100, CAPR×136 | $11,450.98 | -27.05 | -169.07 | ACMR, MU | FNV, TRLV, CAPR | $2,218.70 | $11,270.77 | CM×9, ACMR×88, MU×1 |
| 2026-08-28 | +0.75 | $2,218.70 | CM×9, ACMR×88, MU×1 | $11,154.69 | -116.08 | -224.65 | KEYS, SMTC, CIEN | CM, ACMR, MU | $1,563.24 | $10,917.59 | KEYS×24, SMTC×7, CIEN×2 |
| 2026-08-31 | -5.85 | $1,563.24 | KEYS×24, SMTC×7, CIEN×2 | $10,985.98 | +68.39 | +0.00 | — | KEYS, SMTC, CIEN | $10,979.80 | $10,979.80 | — |
| 2026-09-01 | -6.30 | $10,979.80 | — | $10,979.80 | +0.00 | +0.00 | — | — | $10,979.80 | $10,979.80 | — |
| 2026-09-02 | -3.83 | $10,979.80 | — | $10,979.80 | +0.00 | +0.00 | — | — | $10,979.80 | $10,979.80 | — |
| 2026-09-03 | -0.90 | $10,979.80 | — | $10,979.80 | +0.00 | +289.60 | AVGO, DELL, CXW, FRNM | — | $451.05 | $11,261.07 | AVGO×21, DELL×2, CXW×33, FRNM×69 |
| 2026-09-04 | +2.25 | $451.05 | AVGO×21, DELL×2, CXW×33, FRNM×69 | $11,268.09 | +7.02 | +35.11 | CRM, MRX, BE | AVGO, DELL, CXW | $342.83 | $11,290.82 | FRNM×69, CRM×26, MRX×20, BE×6 |
| 2026-09-08 | -11.47 | $342.83 | FRNM×69, CRM×26, MRX×20, BE×6 | $11,277.97 | -12.85 | -42.60 | — | FRNM, CRM, BE | $9,694.79 | $11,228.99 | MRX×20 |
| 2026-09-09 | -13.95 | $9,694.79 | MRX×20 | $11,226.79 | -2.20 | +0.00 | — | MRX | $11,224.72 | $11,224.72 | — |
| 2026-09-10 | -13.28 | $11,224.72 | — | $11,224.72 | -0.00 | +0.00 | — | — | $11,224.72 | $11,224.72 | — |
| 2026-09-11 | +0.50 | $11,224.72 | — | $11,224.72 | -0.00 | -662.47 | ORCL, ADBE, BAK, AMTX | — | $266.30 | $10,544.20 | ORCL×47, ADBE×4, BAK×529, AMTX×550 |
| 2026-09-14 | -11.00 | $266.30 | ORCL×47, ADBE×4, BAK×529, AMTX×550 | $10,149.03 | -395.17 | +0.00 | — | ORCL, ADBE, BAK, AMTX | $10,130.69 | $10,130.69 | — |
| 2026-09-15 | -3.84 | $10,130.69 | — | $10,130.69 | +0.00 | +0.00 | — | — | $10,130.69 | $10,130.69 | — |
| 2026-09-16 | +5.30 | $10,130.69 | — | $10,130.69 | +0.00 | +29.57 | WAY, QCOM, SM, CLS | — | $148.32 | $10,150.72 | WAY×269, QCOM×5, SM×25, CLS×3 |
| 2026-09-17 | +7.38 | $148.32 | WAY×269, QCOM×5, SM×25, CLS×3 | $10,183.76 | +33.04 | +279.45 | SMTC, GME, JBHT | WAY, QCOM, SM | $270.66 | $10,449.25 | CLS×3, SMTC×37, GME×62, JBHT×5 |
| 2026-09-18 | +4.86 | $270.66 | CLS×3, SMTC×37, GME×62, JBHT×5 | $10,616.85 | +167.60 | +15.83 | TH, RARE | SMTC, JBHT | $6.18 | $10,622.47 | CLS×3, GME×62, TH×274, RARE×166 |
| 2026-09-21 | +12.87 | $6.18 | CLS×3, GME×62, TH×274, RARE×166 | $10,795.27 | +172.80 | -275.13 | VICR, SMTC, ALVO, SION | CLS, GME, TH, RARE | $304.79 | $10,500.60 | VICR×32, SMTC×5, ALVO×182, SION×179 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 531 | $13.18 | $6.85 | — | $2,994.57 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $7000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 232 | $4.31 | $2.99 | — | $1,991.66 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 51 | $19.57 | $2.14 | — | $991.44 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $991.44 | ▲ close $10,395.38 vs 09:30 $10,000.00 (session +407.37) | 16:00 close · cash $991.44 · equity $10,395.38 vs 09:30 $10,000.00 (+395.38; session marks +407.37) · 3 name(s) marked open→close (per-name table). HLIT×531 09:30 $13.18 → close $13.92 +392.94; ANGX×232 09:30 $4.31 → close $4.37 +13.92; ARX×51 09:30 $19.57 → close $19.58 +0.51 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $991.44 | ▲ 09:30 equity $10,405.75 vs yday $10,395.38 (+10.37) | 09:30 open · cash $991.44 (unchanged overnight, no fees) · equity $10,405.75 vs prior close $10,395.38 (+10.37) · 3 name(s) re-marked at the open (per-name table). HLIT×531 yday $13.92 → 09:30 $13.84 -42.48; ANGX×232 yday $4.37 → 09:30 $4.60 +53.36; ARX×51 yday $19.58 → 09:30 $19.57 -0.51 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 531 | $13.84 | $7.00 | $+336.61 | $8,333.49 | ▲ +336.61 after sell → book $10,398.76; vs 09:30 mark -6.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 232 | $4.60 | $3.04 | $+61.25 | $9,397.65 | ▲ +61.25 after sell → book $10,395.72; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 51 | $19.57 | $2.16 | $-4.31 | $10,393.55 | ▼ -4.31 after sell → book $10,393.55; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 90 | $46.18 | $2.26 | — | $6,235.09 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $4157.42 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 21 | $142.77 | $2.05 | — | $3,234.87 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3118.07 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $1,205.85 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2078.71 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 21 | $49.00 | $2.05 | — | $174.80 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $1039.36 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.80 | ▲ close $10,598.88 vs 09:30 $10,405.75 (session +213.71) | 16:00 close · cash $174.80 · equity $10,598.88 vs 09:30 $10,405.75 (+193.13; session marks +213.71) · 4 name(s) marked open→close (per-name table). DVN×90 09:30 $46.18 → close $47.57 +125.10; EOG×21 09:30 $142.77 → close $146.15 +70.98; FANG×10 09:30 $202.70 → close $206.29 +35.90; OUST×21 09:30 $49.00 → close $48.13 -18.27 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.80 | ▲ 09:30 equity $10,639.83 vs yday $10,598.88 (+40.95) | 09:30 open · cash $174.80 (unchanged overnight, no fees) · equity $10,639.83 vs prior close $10,598.88 (+40.95) · 4 name(s) re-marked at the open (per-name table). DVN×90 yday $47.57 → 09:30 $48.00 +38.70; EOG×21 yday $146.15 → 09:30 $148.04 +39.69; FANG×10 yday $206.29 → 09:30 $208.93 +26.40; OUST×21 yday $48.13 → 09:30 $45.09 -63.84 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 90 | $48.00 | $2.31 | $+159.23 | $4,492.49 | ▲ +159.23 after sell → book $10,637.52; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 21 | $148.04 | $2.09 | $+106.53 | $7,599.24 | ▲ +106.53 after sell → book $10,635.43; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $9,686.49 | ▲ +58.23 after sell → book $10,633.38; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 21 | $45.09 | $2.07 | $-86.24 | $10,631.31 | ▼ -86.24 after sell → book $10,631.31; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,631.31 | ▲ close $10,631.31 vs 09:30 $10,639.83 (session +0.00) | 16:00 close · cash $10,631.31 · no lots left · equity $10,631.31. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,631.31 | ▲ 09:30 equity $10,631.31 vs yday $10,631.31 (+0.00) | 09:30 open · cash $10,631.31 · no holdings · equity $10,631.31 vs prior close $10,631.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,631.31 | ▲ close $10,631.31 vs 09:30 $10,631.31 (session +0.00) | 16:00 close · cash $10,631.31 · no lots left · equity $10,631.31. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,631.31 | ▲ 09:30 equity $10,631.31 vs yday $10,631.31 (+0.00) | 09:30 open · cash $10,631.31 · no holdings · equity $10,631.31 vs prior close $10,631.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 81 | $91.01 | $2.23 | — | $3,257.27 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $7441.92 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 23 | $44.76 | $2.06 | — | $2,225.73 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1063.13 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 430 | $2.47 | $5.55 | — | $1,158.08 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1063.13 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 18 | $58.73 | $2.04 | — | $98.90 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1063.13 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.90 | ▲ close $10,807.86 vs 09:30 $10,631.31 (session +188.43) | 16:00 close · cash $98.90 · equity $10,807.86 vs 09:30 $10,631.31 (+176.55; session marks +188.43) · 4 name(s) marked open→close (per-name table). BHP×81 09:30 $91.01 → close $93.63 +212.22; APA×23 09:30 $44.76 → close $44.39 -8.51; AUTL×430 09:30 $2.47 → close $2.46 -4.30; CRSP×18 09:30 $58.73 → close $58.12 -10.98 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.90 | ▲ 09:30 equity $11,013.24 vs yday $10,807.86 (+205.38) | 09:30 open · cash $98.90 (unchanged overnight, no fees) · equity $11,013.24 vs prior close $10,807.86 (+205.38) · 4 name(s) re-marked at the open (per-name table). BHP×81 yday $93.63 → 09:30 $95.72 +169.29; APA×23 yday $44.39 → 09:30 $44.52 +2.99; AUTL×430 yday $2.46 → 09:30 $2.47 +4.30; CRSP×18 yday $58.12 → 09:30 $59.72 +28.80 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 81 | $95.72 | $2.31 | $+376.97 | $7,849.91 | ▲ +376.97 after sell → book $11,010.93; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 23 | $44.52 | $2.08 | $-9.66 | $8,871.79 | ▼ -9.66 after sell → book $11,008.85; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 51 | $119.43 | $2.14 | — | $2,778.72 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $6210.25 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 23 | $115.18 | $2.06 | — | $127.52 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $2661.54 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.52 | ▲ close $11,260.76 vs 09:30 $11,013.24 (session +256.11) | 16:00 close · cash $127.52 · equity $11,260.76 vs 09:30 $11,013.24 (+247.52; session marks +256.11) · 4 name(s) marked open→close (per-name table). AUTL×430 09:30 $2.47 → close $2.41 -25.80; CRSP×18 09:30 $59.72 → close $59.50 -3.96; AU×51 09:30 $119.43 → close $121.22 +91.29; FUTU×23 09:30 $115.18 → close $123.64 +194.58 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.52 | ▼ 09:30 equity $11,146.03 vs yday $11,260.76 (-114.73) | 09:30 open · cash $127.52 (unchanged overnight, no fees) · equity $11,146.03 vs prior close $11,260.76 (-114.73) · 4 name(s) re-marked at the open (per-name table). AUTL×430 yday $2.41 → 09:30 $2.40 -4.30; CRSP×18 yday $59.50 → 09:30 $58.75 -13.50; AU×51 yday $121.22 → 09:30 $120.51 -36.21; FUTU×23 yday $123.64 → 09:30 $121.00 -60.72 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 430 | $2.40 | $5.63 | $-41.28 | $1,153.89 | ▼ -41.28 after sell → book $11,140.40; vs 09:30 mark -5.63 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 51 | $120.51 | $2.20 | $+50.73 | $7,297.70 | ▲ +50.73 after sell → book $11,138.20; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 23 | $121.00 | $2.09 | $+129.71 | $10,078.61 | ▲ +129.71 after sell → book $11,136.11; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,078.61 | ▼ close $11,105.96 vs 09:30 $11,146.03 (session -30.15) | 16:00 close · cash $10,078.61 · equity $11,105.96 vs 09:30 $11,146.03 (-40.07; session marks -30.15) · 1 name(s) marked open→close (per-name table). CRSP×18 09:30 $58.75 → close $57.08 -30.15 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,078.61 | ▲ 09:30 equity $11,121.35 vs yday $11,105.96 (+15.39) | 09:30 open · cash $10,078.61 (unchanged overnight, no fees) · equity $11,121.35 vs prior close $11,105.96 (+15.39) · 1 name(s) re-marked at the open (per-name table). CRSP×18 yday $57.08 → 09:30 $57.93 +15.39 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 18 | $57.93 | $2.06 | $-18.51 | $11,119.28 | ▼ -18.51 after sell → book $11,119.28; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 65 | $118.52 | $2.19 | — | $3,413.30 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $7783.50 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 14 | $77.13 | $2.03 | — | $2,331.44 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1111.93 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 31 | $35.05 | $2.08 | — | $1,242.81 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1111.93 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 118 | $9.42 | $2.34 | — | $128.91 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1111.93 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.91 | ▲ close $11,567.27 vs 09:30 $11,121.35 (session +456.63) | 16:00 close · cash $128.91 · equity $11,567.27 vs 09:30 $11,121.35 (+445.92; session marks +456.63) · 4 name(s) marked open→close (per-name table). AU×65 09:30 $118.52 → close $123.39 +316.55; FCX×14 09:30 $77.13 → close $79.91 +38.92; EZPW×31 09:30 $35.05 → close $35.23 +5.58; RUM×118 09:30 $9.42 → close $10.23 +95.58 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.91 | ▼ 09:30 equity $11,321.63 vs yday $11,567.27 (-245.64) | 09:30 open · cash $128.91 (unchanged overnight, no fees) · equity $11,321.63 vs prior close $11,567.27 (-245.64) · 4 name(s) re-marked at the open (per-name table). AU×65 yday $123.39 → 09:30 $119.80 -233.35; FCX×14 yday $79.91 → 09:30 $79.34 -7.98; EZPW×31 yday $35.23 → 09:30 $35.70 +14.57; RUM×118 yday $10.23 → 09:30 $10.07 -18.88 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 65 | $119.80 | $2.26 | $+78.76 | $7,913.65 | ▲ +78.76 after sell → book $11,319.37; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 14 | $79.34 | $2.05 | $+26.86 | $9,022.36 | ▲ +26.86 after sell → book $11,317.32; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 31 | $35.70 | $2.10 | $+15.96 | $10,126.95 | ▲ +15.96 after sell → book $11,315.21; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 118 | $10.07 | $2.37 | $+71.98 | $11,312.84 | ▲ +71.98 after sell → book $11,312.84; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 29 | $267.02 | $2.08 | — | $3,567.18 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $7918.99 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 9 | $118.50 | $2.02 | — | $2,498.67 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $1131.28 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 100 | $11.22 | $2.29 | — | $1,374.38 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $1131.28 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 136 | $8.29 | $2.40 | — | $244.54 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $1131.28 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $244.54 | ▲ close $11,478.03 vs 09:30 $11,321.63 (session +173.97) | 16:00 close · cash $244.54 · equity $11,478.03 vs 09:30 $11,321.63 (+156.40; session marks +173.97) · 4 name(s) marked open→close (per-name table). FNV×29 09:30 $267.02 → close $267.37 +10.15; CM×9 09:30 $118.50 → close $118.20 -2.70; TRLV×100 09:30 $11.22 → close $11.43 +21.00; CAPR×136 09:30 $8.29 → close $9.36 +145.52 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $244.54 | ▼ 09:30 equity $11,450.98 vs yday $11,478.03 (-27.05) | 09:30 open · cash $244.54 (unchanged overnight, no fees) · equity $11,450.98 vs prior close $11,478.03 (-27.05) · 4 name(s) re-marked at the open (per-name table). FNV×29 yday $267.37 → 09:30 $267.23 -4.06; CM×9 yday $118.20 → 09:30 $118.77 +5.13; TRLV×100 yday $11.43 → 09:30 $11.38 -5.00; CAPR×136 yday $9.36 → 09:30 $9.19 -23.12 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 29 | $267.23 | $2.15 | $+1.86 | $7,992.06 | ▲ +1.86 after sell → book $11,448.83; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 100 | $11.38 | $2.32 | $+11.39 | $9,127.74 | ▲ +11.39 after sell → book $11,446.51; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 136 | $9.19 | $2.43 | $+117.57 | $10,375.15 | ▲ +117.57 after sell → book $11,444.08; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 88 | $81.65 | $2.25 | — | $3,187.70 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $7262.61 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $2,218.70 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1556.27 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,218.70 | ▼ close $11,270.77 vs 09:30 $11,450.98 (session -169.07) | 16:00 close · cash $2,218.70 · equity $11,270.77 vs 09:30 $11,450.98 (-180.21; session marks -169.07) · 3 name(s) marked open→close (per-name table). CM×9 09:30 $118.77 → close $114.84 -35.37; ACMR×88 09:30 $81.65 → close $80.49 -102.08; MU×1 09:30 $967.01 → close $935.39 -31.62 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,218.70 | ▼ 09:30 equity $11,154.69 vs yday $11,270.77 (-116.08) | 09:30 open · cash $2,218.70 (unchanged overnight, no fees) · equity $11,154.69 vs prior close $11,270.77 (-116.08) · 3 name(s) re-marked at the open (per-name table). CM×9 yday $114.84 → 09:30 $115.66 +7.38; ACMR×88 yday $80.49 → 09:30 $79.27 -107.36; MU×1 yday $935.39 → 09:30 $919.29 -16.10 | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 9 | $115.66 | $2.04 | $-29.61 | $3,257.60 | ▼ -29.61 after sell → book $11,152.65; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 88 | $79.27 | $2.32 | $-214.02 | $10,231.03 | ▼ -214.02 after sell → book $11,150.32; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $11,148.31 | ▼ -51.73 after sell → book $11,148.31; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 24 | $324.41 | $2.06 | — | $3,360.41 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $7803.82 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $2,366.08 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1114.83 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $1,563.24 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1114.83 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,563.24 | ▼ close $10,917.59 vs 09:30 $11,154.69 (session -224.65) | 16:00 close · cash $1,563.24 · equity $10,917.59 vs 09:30 $11,154.69 (-237.10; session marks -224.65) · 3 name(s) marked open→close (per-name table). KEYS×24 09:30 $324.41 → close $319.97 -106.56; SMTC×7 09:30 $141.76 → close $131.17 -74.13; CIEN×2 09:30 $400.42 → close $378.44 -43.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,563.24 | ▲ 09:30 equity $10,985.98 vs yday $10,917.59 (+68.39) | 09:30 open · cash $1,563.24 (unchanged overnight, no fees) · equity $10,985.98 vs prior close $10,917.59 (+68.39) · 3 name(s) re-marked at the open (per-name table). KEYS×24 yday $319.97 → 09:30 $322.49 +60.48; SMTC×7 yday $131.17 → 09:30 $132.30 +7.91; CIEN×2 yday $378.44 → 09:30 $378.44 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 24 | $322.49 | $2.13 | $-50.28 | $9,300.87 | ▼ -50.28 after sell → book $10,983.85; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 7 | $132.30 | $2.03 | $-70.26 | $10,224.94 | ▼ -70.26 after sell → book $10,981.82; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 2 | $378.44 | $2.02 | $-47.97 | $10,979.80 | ▼ -47.97 after sell → book $10,979.80; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,979.80 | ▲ close $10,979.80 vs 09:30 $10,985.98 (session +0.00) | 16:00 close · cash $10,979.80 · no lots left · equity $10,979.80. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,979.80 | ▲ 09:30 equity $10,979.80 vs yday $10,979.80 (+0.00) | 09:30 open · cash $10,979.80 · no holdings · equity $10,979.80 vs prior close $10,979.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,979.80 | ▲ close $10,979.80 vs 09:30 $10,979.80 (session +0.00) | 16:00 close · cash $10,979.80 · no lots left · equity $10,979.80. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,979.80 | ▲ 09:30 equity $10,979.80 vs yday $10,979.80 (+0.00) | 09:30 open · cash $10,979.80 · no holdings · equity $10,979.80 vs prior close $10,979.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,979.80 | ▲ close $10,979.80 vs 09:30 $10,979.80 (session +0.00) | 16:00 close · cash $10,979.80 · no lots left · equity $10,979.80. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,979.80 | ▲ 09:30 equity $10,979.80 vs yday $10,979.80 (+0.00) | 09:30 open · cash $10,979.80 · no holdings · equity $10,979.80 vs prior close $10,979.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 21 | $351.74 | $2.05 | — | $3,591.21 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $7685.86 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $2,616.59 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1097.98 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 33 | $32.31 | $2.09 | — | $1,548.27 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1097.98 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 69 | $15.87 | $2.20 | — | $451.05 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1097.98 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $451.05 | ▲ close $11,261.07 vs 09:30 $10,979.80 (session +289.60) | 16:00 close · cash $451.05 · equity $11,261.07 vs 09:30 $10,979.80 (+281.27; session marks +289.60) · 4 name(s) marked open→close (per-name table). AVGO×21 09:30 $351.74 → close $357.16 +113.82; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×33 09:30 $32.31 → close $33.66 +44.55; FRNM×69 09:30 $15.87 → close $16.90 +71.07 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $451.05 | ▲ 09:30 equity $11,268.09 vs yday $11,261.07 (+7.02) | 09:30 open · cash $451.05 (unchanged overnight, no fees) · equity $11,268.09 vs prior close $11,261.07 (+7.02) · 4 name(s) re-marked at the open (per-name table). AVGO×21 yday $357.16 → 09:30 $359.70 +53.34; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×33 yday $33.66 → 09:30 $33.46 -6.60; FRNM×69 yday $16.90 → 09:30 $16.40 -34.50 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 21 | $359.70 | $2.12 | $+162.98 | $8,002.62 | ▲ +162.98 after sell → book $11,265.96; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $9,028.17 | ▲ +50.93 after sell → book $11,263.95; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 33 | $33.46 | $2.11 | $+33.75 | $10,130.24 | ▲ +33.75 after sell → book $11,261.84; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 26 | $263.36 | $2.07 | — | $3,280.81 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $7091.17 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 20 | $75.65 | $2.05 | — | $1,765.76 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1519.54 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 6 | $236.82 | $2.01 | — | $342.83 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+8.1; leftover $1519.54 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $342.83 | ▲ close $11,290.82 vs 09:30 $11,268.09 (session +35.11) | 16:00 close · cash $342.83 · equity $11,290.82 vs 09:30 $11,268.09 (+22.73; session marks +35.11) · 4 name(s) marked open→close (per-name table). FRNM×69 09:30 $16.40 → close $16.31 -6.21; CRM×26 09:30 $263.36 → close $259.23 -107.38; MRX×20 09:30 $75.65 → close $78.27 +52.40; BE×6 09:30 $236.82 → close $252.87 +96.30 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $342.83 | ▼ 09:30 equity $11,277.97 vs yday $11,290.82 (-12.85) | 09:30 open · cash $342.83 (unchanged overnight, no fees) · equity $11,277.97 vs prior close $11,290.82 (-12.85) · 4 name(s) re-marked at the open (per-name table). FRNM×69 yday $16.31 → 09:30 $16.74 +29.67; CRM×26 yday $259.23 → 09:30 $253.72 -143.26; MRX×20 yday $78.27 → 09:30 $78.84 +11.40; BE×6 yday $252.87 → 09:30 $267.76 +89.34 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 69 | $16.74 | $2.22 | $+55.61 | $1,495.67 | ▲ +55.61 after sell → book $11,275.75; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 26 | $253.72 | $2.13 | $-254.84 | $8,090.26 | ▼ -254.84 after sell → book $11,273.62; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 6 | $267.76 | $2.03 | $+181.60 | $9,694.79 | ▲ +181.60 after sell → book $11,271.59; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,694.79 | ▼ close $11,228.99 vs 09:30 $11,277.97 (session -42.60) | 16:00 close · cash $9,694.79 · equity $11,228.99 vs 09:30 $11,277.97 (-48.98; session marks -42.60) · 1 name(s) marked open→close (per-name table). MRX×20 09:30 $78.84 → close $76.71 -42.60 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,694.79 | ▼ 09:30 equity $11,226.79 vs yday $11,228.99 (-2.20) | 09:30 open · cash $9,694.79 (unchanged overnight, no fees) · equity $11,226.79 vs prior close $11,228.99 (-2.20) · 1 name(s) re-marked at the open (per-name table). MRX×20 yday $76.71 → 09:30 $76.60 -2.20 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 20 | $76.60 | $2.07 | $+14.88 | $11,224.72 | ▲ +14.88 after sell → book $11,224.72; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,224.72 | ▲ close $11,224.72 vs 09:30 $11,226.79 (session +0.00) | 16:00 close · cash $11,224.72 · no lots left · equity $11,224.72. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,224.72 | ▲ 09:30 equity $11,224.72 vs yday $11,224.72 (-0.00) | 09:30 open · cash $11,224.72 · no holdings · equity $11,224.72 vs prior close $11,224.72 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,224.72 | ▲ close $11,224.72 vs 09:30 $11,224.72 (session +0.00) | 16:00 close · cash $11,224.72 · no lots left · equity $11,224.72. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,224.72 | ▲ 09:30 equity $11,224.72 vs yday $11,224.72 (-0.00) | 09:30 open · cash $11,224.72 · no holdings · equity $11,224.72 vs prior close $11,224.72 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 47 | $164.43 | $2.13 | — | $3,494.38 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $7857.30 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 4 | $242.17 | $2.00 | — | $2,523.70 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; ret5=-11.1; leftover $1122.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 529 | $2.12 | $6.82 | — | $1,395.39 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1122.47 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 550 | $2.04 | $7.09 | — | $266.30 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1122.47 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $266.30 | ▼ close $10,544.20 vs 09:30 $11,224.72 (session -662.47) | 16:00 close · cash $266.30 · equity $10,544.20 vs 09:30 $11,224.72 (-680.52; session marks -662.47) · 4 name(s) marked open→close (per-name table). ORCL×47 09:30 $164.43 → close $150.28 -665.05; ADBE×4 09:30 $242.17 → close $252.23 +40.24; BAK×529 09:30 $2.12 → close $2.08 -21.16; AMTX×550 09:30 $2.04 → close $2.01 -16.50 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $266.30 | ▼ 09:30 equity $10,149.03 vs yday $10,544.20 (-395.17) | 09:30 open · cash $266.30 (unchanged overnight, no fees) · equity $10,149.03 vs prior close $10,544.20 (-395.17) · 4 name(s) re-marked at the open (per-name table). ORCL×47 yday $150.28 → 09:30 $141.42 -416.42; ADBE×4 yday $252.23 → 09:30 $261.51 +37.12; BAK×529 yday $2.08 → 09:30 $2.05 -15.87; AMTX×550 yday $2.01 → 09:30 $2.01 +0.00 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 47 | $141.42 | $2.19 | $-1085.80 | $6,910.84 | ▼ -1,085.80 after sell → book $10,146.83; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 4 | $261.51 | $2.02 | $+73.34 | $7,954.86 | ▲ +73.34 after sell → book $10,144.81; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 529 | $2.05 | $6.92 | $-50.78 | $9,032.39 | ▼ -50.78 after sell → book $10,137.89; vs 09:30 mark -6.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 550 | $2.01 | $7.20 | $-30.79 | $10,130.69 | ▼ -30.79 after sell → book $10,130.69; vs 09:30 mark -7.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,130.69 | ▲ close $10,130.69 vs 09:30 $10,149.03 (session +0.00) | 16:00 close · cash $10,130.69 · no lots left · equity $10,130.69. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,130.69 | ▲ 09:30 equity $10,130.69 vs yday $10,130.69 (+0.00) | 09:30 open · cash $10,130.69 · no holdings · equity $10,130.69 vs prior close $10,130.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,130.69 | ▲ close $10,130.69 vs 09:30 $10,130.69 (session +0.00) | 16:00 close · cash $10,130.69 · no lots left · equity $10,130.69. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,130.69 | ▲ 09:30 equity $10,130.69 vs yday $10,130.69 (+0.00) | 09:30 open · cash $10,130.69 · no holdings · equity $10,130.69 vs prior close $10,130.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 269 | $26.27 | $3.47 | — | $3,060.59 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $7091.49 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 5 | $189.17 | $2.00 | — | $2,112.74 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $1013.07 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 25 | $39.99 | $2.06 | — | $1,110.92 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1013.07 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `CLS` | 3 | $320.20 | $2.00 | — | $148.32 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+10.2; leftover $1013.07 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.32 | ▲ close $10,150.72 vs 09:30 $10,130.69 (session +29.57) | 16:00 close · cash $148.32 · equity $10,150.72 vs 09:30 $10,130.69 (+20.03; session marks +29.57) · 4 name(s) marked open→close (per-name table). WAY×269 09:30 $26.27 → close $26.59 +86.08; QCOM×5 09:30 $189.17 → close $184.84 -21.65; SM×25 09:30 $39.99 → close $38.16 -45.75; CLS×3 09:30 $320.20 → close $323.83 +10.89 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.32 | ▲ 09:30 equity $10,183.76 vs yday $10,150.72 (+33.04) | 09:30 open · cash $148.32 (unchanged overnight, no fees) · equity $10,183.76 vs prior close $10,150.72 (+33.04) · 4 name(s) re-marked at the open (per-name table). WAY×269 yday $26.59 → 09:30 $26.51 -21.52; QCOM×5 yday $184.84 → 09:30 $190.35 +27.55; SM×25 yday $38.16 → 09:30 $37.57 -14.75; CLS×3 yday $323.83 → 09:30 $337.75 +41.76 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 269 | $26.51 | $3.57 | $+57.52 | $7,275.94 | ▲ +57.52 after sell → book $10,180.19; vs 09:30 mark -3.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 5 | $190.35 | $2.02 | $+1.87 | $8,225.67 | ▲ +1.87 after sell → book $10,178.17; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 25 | $37.57 | $2.08 | $-64.65 | $9,162.83 | ▼ -64.65 after sell → book $10,176.08; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 37 | $170.85 | $2.10 | — | $2,839.28 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $6413.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 62 | $22.12 | $2.18 | — | $1,465.67 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1374.42 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 5 | $238.60 | $2.00 | — | $270.66 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_mover; ret5=-11.6; leftover $1374.42 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $270.66 | ▲ close $10,449.25 vs 09:30 $10,183.76 (session +279.45) | 16:00 close · cash $270.66 · equity $10,449.25 vs 09:30 $10,183.76 (+265.49; session marks +279.45) · 4 name(s) marked open→close (per-name table). CLS×3 09:30 $337.75 → close $329.94 -23.43; SMTC×37 09:30 $170.85 → close $178.19 +271.58; GME×62 09:30 $22.12 → close $22.77 +40.30; JBHT×5 09:30 $238.60 → close $236.80 -9.00 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $270.66 | ▲ 09:30 equity $10,616.85 vs yday $10,449.25 (+167.60) | 09:30 open · cash $270.66 (unchanged overnight, no fees) · equity $10,616.85 vs prior close $10,449.25 (+167.60) · 4 name(s) re-marked at the open (per-name table). CLS×3 yday $329.94 → 09:30 $332.06 +6.36; SMTC×37 yday $178.19 → 09:30 $182.33 +153.18; GME×62 yday $22.77 → 09:30 $22.90 +8.06; JBHT×5 yday $236.80 → 09:30 $236.80 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 37 | $182.33 | $2.17 | $+420.49 | $7,014.71 | ▲ +420.49 after sell → book $10,614.69; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 5 | $236.80 | $2.02 | $-13.03 | $8,196.68 | ▼ -13.03 after sell → book $10,612.66; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 274 | $20.91 | $3.53 | — | $2,463.81 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $5737.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 166 | $14.79 | $2.49 | — | $6.18 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $2459.00 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.18 | ▲ close $10,622.47 vs 09:30 $10,616.85 (session +15.83) | 16:00 close · cash $6.18 · equity $10,622.47 vs 09:30 $10,616.85 (+5.62; session marks +15.83) · 4 name(s) marked open→close (per-name table). CLS×3 09:30 $332.06 → close $332.63 +1.71; GME×62 09:30 $22.90 → close $22.64 -16.12; TH×274 09:30 $20.91 → close $21.19 +76.72; RARE×166 09:30 $14.79 → close $14.51 -46.48 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.18 | ▲ 09:30 equity $10,795.27 vs yday $10,622.47 (+172.80) | 09:30 open · cash $6.18 (unchanged overnight, no fees) · equity $10,795.27 vs prior close $10,622.47 (+172.80) · 4 name(s) re-marked at the open (per-name table). CLS×3 yday $332.63 → 09:30 $341.45 +26.46; GME×62 yday $22.64 → 09:30 $22.78 +8.68; TH×274 yday $21.19 → 09:30 $21.65 +126.04; RARE×166 yday $14.51 → 09:30 $14.58 +11.62 | — |
| 2026-09-21 09:30 ET | **SELL** | `CLS` | 3 | $341.45 | $2.02 | $+59.73 | $1,028.51 | ▲ +59.73 after sell → book $10,793.25; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 62 | $22.78 | $2.20 | $+36.55 | $2,438.67 | ▲ +36.55 after sell → book $10,791.05; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 274 | $21.65 | $3.63 | $+195.60 | $8,367.14 | ▲ +195.60 after sell → book $10,787.42; vs 09:30 mark -3.63 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 166 | $14.58 | $2.53 | $-39.88 | $10,784.89 | ▼ -39.88 after sell → book $10,784.89; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 32 | $230.25 | $2.09 | — | $3,414.80 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+12.5; leftover $7549.42 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 5 | $190.30 | $2.00 | — | $2,461.30 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+10.6; leftover $1078.49 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `ALVO` | 182 | $5.92 | $2.54 | — | $1,381.32 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+11.0; leftover $1078.49 | join🔴 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 179 | $6.00 | $2.53 | — | $304.79 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_mover; 🔵; ret5=-24.1; leftover $1078.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $304.79 | ▼ close $10,500.60 vs 09:30 $10,795.27 (session -275.13) | 16:00 close · cash $304.79 · equity $10,500.60 vs 09:30 $10,795.27 (-294.67; session marks -275.13) · 4 name(s) marked open→close (per-name table). VICR×32 09:30 $230.25 → close $223.90 -203.20; SMTC×5 09:30 $190.30 → close $177.37 -64.65; ALVO×182 09:30 $5.92 → close $5.88 -7.28; SION×179 09:30 $6.00 → close $6.00 +0.00 | — |

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
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1556.27 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1114.83 < 1 share @ 1306.03 |
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
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `VICR` | 32 | 2026-09-21 @ $230.25 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+12.5; leftover $7549.42 |
| `SMTC` | 5 | 2026-09-21 @ $190.30 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+10.6; leftover $1078.49 |
| `ALVO` | 182 | 2026-09-21 @ $5.92 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+11.0; leftover $1078.49 |
| `SION` | 179 | 2026-09-21 @ $6.00 | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_mover; 🔵; ret5=-24.1; leftover $1078.49 |
