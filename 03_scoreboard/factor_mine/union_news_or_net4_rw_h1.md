# Factor mine action — `union_news_or_net4_rw_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `rank_w` · sell `list` · S-boost `none` · OR news + net≥4; leftover weighted by camera rank

Cash book **-0.57%** ($9,943) · signal-only (no cash/fees) was -1.38%. Starts YES **4/26**. Fills 90 · skips 23 · realized $-52.78.

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
- Must-have: the morning news packet OR the prior-export headline is green.
- Must-have: camera net (+G −R) is at least 4.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 4.
- Split leftover cash by rank (first name gets the biggest slice).
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
- **Gate** `news_or_headline=True,cam_net_min=4` · **rank** `cond` · **top_n** 4.
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $20.46.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 303 | — | $13.18 | +0.00 | $13.92 | +224.22 | +224.22 | +0.00 | +224.22 |
| 2026-08-14 | `SNDK` | 1 | — | $1646.93 | +0.00 | $1641.11 | -5.82 | -5.82 | +0.00 | -5.82 |
| 2026-08-14 | `ANGX` | 464 | — | $4.31 | +0.00 | $4.37 | +27.84 | +27.84 | +0.00 | +27.84 |
| 2026-08-14 | `ARX` | 51 | — | $19.57 | +0.00 | $19.58 | +0.51 | +0.51 | +0.00 | +0.51 |
| 2026-08-17 | `HLIT` | 303 | $13.92 | $13.84 | -24.24 | — | +0.00 | -24.24 | +199.98 | — |
| 2026-08-17 | `SNDK` | 1 | $1641.11 | $1700.74 | +59.63 | — | +0.00 | +59.63 | +53.81 | — |
| 2026-08-17 | `ANGX` | 464 | $4.37 | $4.60 | +106.72 | — | +0.00 | +106.72 | +134.56 | — |
| 2026-08-17 | `ARX` | 51 | $19.58 | $19.57 | -0.51 | — | +0.00 | -0.51 | +0.00 | — |
| 2026-08-17 | `DVN` | 89 | — | $46.18 | +0.00 | $47.57 | +123.71 | +123.71 | +0.00 | +123.71 |
| 2026-08-17 | `EOG` | 21 | — | $142.77 | +0.00 | $146.15 | +70.98 | +70.98 | +0.00 | +70.98 |
| 2026-08-17 | `FANG` | 10 | — | $202.70 | +0.00 | $206.29 | +35.90 | +35.90 | +0.00 | +35.90 |
| 2026-08-17 | `GLOB` | 27 | — | $37.18 | +0.00 | $36.26 | -24.84 | -24.84 | +0.00 | -24.84 |
| 2026-08-18 | `DVN` | 89 | $47.57 | $48.00 | +38.27 | — | +0.00 | +38.27 | +161.98 | — |
| 2026-08-18 | `EOG` | 21 | $146.15 | $148.04 | +39.69 | — | +0.00 | +39.69 | +110.67 | — |
| 2026-08-18 | `FANG` | 10 | $206.29 | $208.93 | +26.40 | — | +0.00 | +26.40 | +62.30 | — |
| 2026-08-18 | `GLOB` | 27 | $36.26 | $36.98 | +19.44 | — | +0.00 | +19.44 | -5.40 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 46 | — | $91.01 | +0.00 | $93.63 | +120.52 | +120.52 | +0.00 | +120.52 |
| 2026-08-20 | `APA` | 71 | — | $44.76 | +0.00 | $44.39 | -26.27 | -26.27 | +0.00 | -26.27 |
| 2026-08-20 | `AUTL` | 864 | — | $2.47 | +0.00 | $2.46 | -8.64 | -8.64 | +0.00 | -8.64 |
| 2026-08-20 | `CRSP` | 18 | — | $58.73 | +0.00 | $58.12 | -10.98 | -10.98 | +0.00 | -10.98 |
| 2026-08-21 | `BHP` | 46 | $93.63 | $95.72 | +96.14 | — | +0.00 | +96.14 | +216.66 | — |
| 2026-08-21 | `APA` | 71 | $44.39 | $44.52 | +9.23 | — | +0.00 | +9.23 | -17.04 | — |
| 2026-08-21 | `AUTL` | 864 | $2.46 | $2.47 | +8.64 | $2.41 | -51.84 | -43.20 | +0.00 | -51.84 |
| 2026-08-21 | `CRSP` | 18 | $58.12 | $59.72 | +28.80 | $59.50 | -3.96 | +24.84 | +17.82 | +13.86 |
| 2026-08-21 | `AU` | 42 | — | $119.43 | +0.00 | $121.22 | +75.18 | +75.18 | +0.00 | +75.18 |
| 2026-08-21 | `FUTU` | 22 | — | $115.18 | +0.00 | $123.64 | +186.12 | +186.12 | +0.00 | +186.12 |
| 2026-08-24 | `AUTL` | 864 | $2.41 | $2.40 | -8.64 | — | +0.00 | -8.64 | -60.48 | — |
| 2026-08-24 | `CRSP` | 18 | $59.50 | $58.75 | -13.50 | $57.08 | -30.15 | -43.65 | +0.36 | -29.79 |
| 2026-08-24 | `AU` | 42 | $121.22 | $120.51 | -29.82 | — | +0.00 | -29.82 | +45.36 | — |
| 2026-08-24 | `FUTU` | 22 | $123.64 | $121.00 | -58.08 | — | +0.00 | -58.08 | +128.04 | — |
| 2026-08-25 | `CRSP` | 18 | $57.08 | $57.93 | +15.39 | — | +0.00 | +15.39 | -14.40 | — |
| 2026-08-25 | `AU` | 36 | — | $118.52 | +0.00 | $123.39 | +175.32 | +175.32 | +0.00 | +175.32 |
| 2026-08-25 | `FCX` | 42 | — | $77.13 | +0.00 | $79.91 | +116.76 | +116.76 | +0.00 | +116.76 |
| 2026-08-25 | `EZPW` | 62 | — | $35.05 | +0.00 | $35.23 | +11.16 | +11.16 | +0.00 | +11.16 |
| 2026-08-25 | `AMX` | 45 | — | $23.80 | +0.00 | $23.75 | -2.25 | -2.25 | +0.00 | -2.25 |
| 2026-08-26 | `AU` | 36 | $123.39 | $119.80 | -129.24 | — | +0.00 | -129.24 | +46.08 | — |
| 2026-08-26 | `FCX` | 42 | $79.91 | $79.34 | -23.94 | — | +0.00 | -23.94 | +92.82 | — |
| 2026-08-26 | `EZPW` | 62 | $35.23 | $35.70 | +29.14 | — | +0.00 | +29.14 | +40.30 | — |
| 2026-08-26 | `AMX` | 45 | $23.75 | $23.75 | +0.00 | $23.62 | -5.85 | -5.85 | -2.25 | -8.10 |
| 2026-08-26 | `FNV` | 25 | — | $267.02 | +0.00 | $267.37 | +8.75 | +8.75 | +0.00 | +8.75 |
| 2026-08-26 | `ASST` | 161 | — | $20.72 | +0.00 | $21.50 | +125.58 | +125.58 | +0.00 | +125.58 |
| 2026-08-27 | `AMX` | 45 | $23.62 | $23.77 | +6.75 | — | +0.00 | +6.75 | -1.35 | — |
| 2026-08-27 | `FNV` | 25 | $267.37 | $267.23 | -3.50 | — | +0.00 | -3.50 | +5.25 | — |
| 2026-08-27 | `ASST` | 161 | $21.50 | $22.45 | +152.95 | — | +0.00 | +152.95 | +278.53 | — |
| 2026-08-27 | `ACMR` | 55 | — | $81.65 | +0.00 | $80.49 | -63.80 | -63.80 | +0.00 | -63.80 |
| 2026-08-27 | `MU` | 3 | — | $967.01 | +0.00 | $935.39 | -94.86 | -94.86 | +0.00 | -94.86 |
| 2026-08-27 | `ASML` | 1 | — | $1746.53 | +0.00 | $1735.01 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-08-27 | `CM` | 9 | — | $118.77 | +0.00 | $114.84 | -35.37 | -35.37 | +0.00 | -35.37 |
| 2026-08-28 | `ACMR` | 55 | $80.49 | $79.27 | -67.10 | — | +0.00 | -67.10 | -130.90 | — |
| 2026-08-28 | `MU` | 3 | $935.39 | $919.29 | -48.30 | — | +0.00 | -48.30 | -143.16 | — |
| 2026-08-28 | `ASML` | 1 | $1735.01 | $1734.75 | -0.26 | — | +0.00 | -0.26 | -11.78 | — |
| 2026-08-28 | `CM` | 9 | $114.84 | $115.66 | +7.38 | — | +0.00 | +7.38 | -27.99 | — |
| 2026-08-28 | `KEYS` | 13 | — | $324.41 | +0.00 | $319.97 | -57.72 | -57.72 | +0.00 | -57.72 |
| 2026-08-28 | `SMTC` | 23 | — | $141.76 | +0.00 | $131.17 | -243.57 | -243.57 | +0.00 | -243.57 |
| 2026-08-28 | `CIEN` | 5 | — | $400.42 | +0.00 | $378.44 | -109.90 | -109.90 | +0.00 | -109.90 |
| 2026-08-31 | `KEYS` | 13 | $319.97 | $322.49 | +32.76 | — | +0.00 | +32.76 | -24.96 | — |
| 2026-08-31 | `SMTC` | 23 | $131.17 | $132.30 | +25.99 | — | +0.00 | +25.99 | -217.58 | — |
| 2026-08-31 | `CIEN` | 5 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -109.90 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 12 | — | $351.74 | +0.00 | $357.16 | +65.04 | +65.04 | +0.00 | +65.04 |
| 2026-09-03 | `DELL` | 6 | — | $486.31 | +0.00 | $516.39 | +180.48 | +180.48 | +0.00 | +180.48 |
| 2026-09-03 | `CXW` | 66 | — | $32.31 | +0.00 | $33.66 | +89.10 | +89.10 | +0.00 | +89.10 |
| 2026-09-03 | `FRNM` | 67 | — | $15.87 | +0.00 | $16.90 | +69.01 | +69.01 | +0.00 | +69.01 |
| 2026-09-04 | `AVGO` | 12 | $357.16 | $359.70 | +30.48 | — | +0.00 | +30.48 | +95.52 | — |
| 2026-09-04 | `DELL` | 6 | $516.39 | $513.78 | -15.66 | — | +0.00 | -15.66 | +164.82 | — |
| 2026-09-04 | `CXW` | 66 | $33.66 | $33.46 | -13.20 | — | +0.00 | -13.20 | +75.90 | — |
| 2026-09-04 | `FRNM` | 67 | $16.90 | $16.40 | -33.50 | $16.31 | -6.03 | -39.53 | +35.51 | +29.48 |
| 2026-09-04 | `CRM` | 18 | — | $263.36 | +0.00 | $259.23 | -74.34 | -74.34 | +0.00 | -74.34 |
| 2026-09-04 | `MMED` | 138 | — | $23.84 | +0.00 | $23.29 | -75.90 | -75.90 | +0.00 | -75.90 |
| 2026-09-04 | `HPE` | 30 | — | $53.85 | +0.00 | $52.00 | -55.50 | -55.50 | +0.00 | -55.50 |
| 2026-09-08 | `FRNM` | 67 | $16.31 | $16.74 | +28.81 | — | +0.00 | +28.81 | +58.29 | — |
| 2026-09-08 | `CRM` | 18 | $259.23 | $253.72 | -99.18 | — | +0.00 | -99.18 | -173.52 | — |
| 2026-09-08 | `MMED` | 138 | $23.29 | $23.16 | -17.94 | — | +0.00 | -17.94 | -93.84 | — |
| 2026-09-08 | `HPE` | 30 | $52.00 | $52.29 | +8.70 | — | +0.00 | +8.70 | -46.80 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 43 | — | $164.43 | +0.00 | $150.28 | -608.45 | -608.45 | +0.00 | -608.45 |
| 2026-09-11 | `BTI` | 63 | — | $56.03 | +0.00 | $55.24 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-09-14 | `ORCL` | 43 | $150.28 | $141.42 | -380.98 | — | +0.00 | -380.98 | -989.43 | — |
| 2026-09-14 | `BTI` | 63 | $55.24 | $57.12 | +118.44 | — | +0.00 | +118.44 | +68.67 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 149 | — | $26.27 | +0.00 | $26.59 | +47.68 | +47.68 | +0.00 | +47.68 |
| 2026-09-16 | `QCOM` | 15 | — | $189.17 | +0.00 | $184.84 | -64.95 | -64.95 | +0.00 | -64.95 |
| 2026-09-16 | `SM` | 48 | — | $39.99 | +0.00 | $38.16 | -87.84 | -87.84 | +0.00 | -87.84 |
| 2026-09-16 | `AMX` | 42 | — | $23.18 | +0.00 | $22.98 | -8.40 | -8.40 | +0.00 | -8.40 |
| 2026-09-17 | `WAY` | 149 | $26.59 | $26.51 | -11.92 | — | +0.00 | -11.92 | +35.76 | — |
| 2026-09-17 | `QCOM` | 15 | $184.84 | $190.35 | +82.65 | — | +0.00 | +82.65 | +17.70 | — |
| 2026-09-17 | `SM` | 48 | $38.16 | $37.57 | -28.32 | — | +0.00 | -28.32 | -116.16 | — |
| 2026-09-17 | `AMX` | 42 | $22.98 | $23.09 | +4.62 | — | +0.00 | +4.62 | -3.78 | — |
| 2026-09-17 | `SMTC` | 22 | — | $170.85 | +0.00 | $178.19 | +161.48 | +161.48 | +0.00 | +161.48 |
| 2026-09-17 | `AVTR` | 184 | — | $15.81 | +0.00 | $15.86 | +9.20 | +9.20 | +0.00 | +9.20 |
| 2026-09-17 | `GME` | 87 | — | $22.12 | +0.00 | $22.77 | +56.55 | +56.55 | +0.00 | +56.55 |
| 2026-09-17 | `JBHT` | 4 | — | $238.60 | +0.00 | $236.80 | -7.20 | -7.20 | +0.00 | -7.20 |
| 2026-09-18 | `SMTC` | 22 | $178.19 | $182.33 | +91.08 | — | +0.00 | +91.08 | +252.56 | — |
| 2026-09-18 | `AVTR` | 184 | $15.86 | $15.87 | +1.84 | — | +0.00 | +1.84 | +11.04 | — |
| 2026-09-18 | `GME` | 87 | $22.77 | $22.90 | +11.31 | $22.64 | -22.62 | -11.31 | +67.86 | +45.24 |
| 2026-09-18 | `JBHT` | 4 | $236.80 | $236.80 | +0.00 | — | +0.00 | +0.00 | -7.20 | — |
| 2026-09-18 | `TH` | 191 | — | $20.91 | +0.00 | $21.19 | +53.48 | +53.48 | +0.00 | +53.48 |
| 2026-09-18 | `RARE` | 180 | — | $14.79 | +0.00 | $14.51 | -50.40 | -50.40 | +0.00 | -50.40 |
| 2026-09-18 | `BHVN` | 95 | — | $14.07 | +0.00 | $13.62 | -42.75 | -42.75 | +0.00 | -42.75 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +246.75 | HLIT, SNDK, ANGX, ARX | — | $1,347.59 | $10,232.72 | HLIT×303, SNDK×1, ANGX×464, ARX×51 |
| 2026-08-17 | +2.25 | $1,347.59 | HLIT×303, SNDK×1, ANGX×464, ARX×51 | $10,374.32 | +141.60 | +205.75 | DVN, EOG, FANG, GLOB | HLIT, SNDK, ANGX, ARX | $212.62 | $10,557.42 | DVN×89, EOG×21, FANG×10, GLOB×27 |
| 2026-08-18 | -6.20 | $212.62 | DVN×89, EOG×21, FANG×10, GLOB×27 | $10,681.22 | +123.80 | +0.00 | — | DVN, EOG, FANG, GLOB | $10,672.69 | $10,672.69 | — |
| 2026-08-19 | -7.20 | $10,672.69 | — | $10,672.69 | +0.00 | +0.00 | — | — | $10,672.69 | $10,672.69 | — |
| 2026-08-20 | +1.12 | $10,672.69 | — | $10,672.69 | +0.00 | +74.63 | BHP, APA, AUTL, CRSP | — | $99.53 | $10,729.80 | BHP×46, APA×71, AUTL×864, CRSP×18 |
| 2026-08-21 | +3.25 | $99.53 | BHP×46, APA×71, AUTL×864, CRSP×18 | $10,872.61 | +142.81 | +205.50 | AU, FUTU | BHP, APA | $104.96 | $11,069.52 | AUTL×864, CRSP×18, AU×42, FUTU×22 |
| 2026-08-24 | -5.17 | $104.96 | AUTL×864, CRSP×18, AU×42, FUTU×22 | $10,959.48 | -110.04 | -30.15 | — | AUTL, AU, FUTU | $9,886.42 | $10,913.77 | CRSP×18 |
| 2026-08-25 | +1.80 | $9,886.42 | CRSP×18 | $10,929.16 | +15.39 | +300.99 | AU, FCX, EZPW, AMX | CRSP | $168.31 | $11,219.58 | AU×36, FCX×42, EZPW×62, AMX×45 |
| 2026-08-26 | +2.02 | $168.31 | AU×36, FCX×42, EZPW×62, AMX×45 | $11,095.54 | -124.04 | +128.48 | FNV, ASST | AU, FCX, EZPW | $4.33 | $11,212.98 | AMX×45, FNV×25, ASST×161 |
| 2026-08-27 | — | $4.33 | AMX×45, FNV×25, ASST×161 | $11,369.18 | +156.20 | -205.55 | ACMR, MU, ASML, CM | AMX, FNV, ASST | $1,146.97 | $11,148.66 | ACMR×55, MU×3, ASML×1, CM×9 |
| 2026-08-28 | +0.75 | $1,146.97 | ACMR×55, MU×3, ASML×1, CM×9 | $11,040.38 | -108.28 | -411.19 | KEYS, SMTC, CIEN | ACMR, MU, ASML, CM | $1,546.09 | $10,614.81 | KEYS×13, SMTC×23, CIEN×5 |
| 2026-08-31 | -5.85 | $1,546.09 | KEYS×13, SMTC×23, CIEN×5 | $10,673.56 | +58.75 | +0.00 | — | KEYS, SMTC, CIEN | $10,667.37 | $10,667.37 | — |
| 2026-09-01 | -6.30 | $10,667.37 | — | $10,667.37 | -0.00 | +0.00 | — | — | $10,667.37 | $10,667.37 | — |
| 2026-09-02 | -3.83 | $10,667.37 | — | $10,667.37 | -0.00 | +0.00 | — | — | $10,667.37 | $10,667.37 | — |
| 2026-09-03 | -0.90 | $10,667.37 | — | $10,667.37 | -0.00 | +403.63 | AVGO, DELL, CXW, FRNM | — | $324.47 | $11,062.59 | AVGO×12, DELL×6, CXW×66, FRNM×67 |
| 2026-09-04 | +2.25 | $324.47 | AVGO×12, DELL×6, CXW×66, FRNM×67 | $11,030.71 | -31.88 | -211.77 | CRM, MMED, HPE | AVGO, DELL, CXW | $273.15 | $10,806.08 | FRNM×67, CRM×18, MMED×138, HPE×30 |
| 2026-09-08 | -11.47 | $273.15 | FRNM×67, CRM×18, MMED×138, HPE×30 | $10,726.47 | -79.61 | +0.00 | — | FRNM, CRM, MMED, HPE | $10,717.61 | $10,717.61 | — |
| 2026-09-09 | -13.95 | $10,717.61 | — | $10,717.61 | +0.00 | +0.00 | — | — | $10,717.61 | $10,717.61 | — |
| 2026-09-10 | -13.28 | $10,717.61 | — | $10,717.61 | +0.00 | +0.00 | — | — | $10,717.61 | $10,717.61 | — |
| 2026-09-11 | +0.50 | $10,717.61 | — | $10,717.61 | +0.00 | -658.22 | ORCL, BTI | — | $112.93 | $10,055.09 | ORCL×43, BTI×63 |
| 2026-09-14 | -11.00 | $112.93 | ORCL×43, BTI×63 | $9,792.55 | -262.54 | +0.00 | — | ORCL, BTI | $9,788.16 | $9,788.16 | — |
| 2026-09-15 | -3.84 | $9,788.16 | — | $9,788.16 | -0.00 | +0.00 | — | — | $9,788.16 | $9,788.16 | — |
| 2026-09-16 | +5.30 | $9,788.16 | — | $9,788.16 | -0.00 | -113.51 | WAY, QCOM, SM, AMX | — | $134.57 | $9,665.92 | WAY×149, QCOM×15, SM×48, AMX×42 |
| 2026-09-17 | +7.38 | $134.57 | WAY×149, QCOM×15, SM×48, AMX×42 | $9,712.95 | +47.03 | +220.03 | SMTC, AVTR, GME, JBHT | WAY, QCOM, SM, AMX | $148.67 | $9,915.28 | SMTC×22, AVTR×184, GME×87, JBHT×4 |
| 2026-09-18 | +4.86 | $148.67 | SMTC×22, AVTR×184, GME×87, JBHT×4 | $10,019.51 | +104.23 | -62.29 | TH, RARE, BHVN | SMTC, AVTR, JBHT | $20.46 | $9,943.13 | GME×87, TH×191, RARE×180, BHVN×95 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 303 | $13.18 | $3.91 | — | $6,002.55 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $4000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $4,353.63 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $3000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $2,347.80 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 51 | $19.57 | $2.14 | — | $1,347.59 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+58.7; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,347.59 | ▲ close $10,232.72 vs 09:30 $10,000.00 (session +246.75) | 16:00 close · cash $1,347.59 · equity $10,232.72 vs 09:30 $10,000.00 (+232.72; session marks +246.75) · 4 name(s) marked open→close (per-name table). HLIT×303 09:30 $13.18 → close $13.92 +224.22; SNDK×1 09:30 $1646.93 → close $1641.11 -5.82; ANGX×464 09:30 $4.31 → close $4.37 +27.84; ARX×51 09:30 $19.57 → close $19.58 +0.51 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,347.59 | ▲ 09:30 equity $10,374.32 vs yday $10,232.72 (+141.60) | 09:30 open · cash $1,347.59 (unchanged overnight, no fees) · equity $10,374.32 vs prior close $10,232.72 (+141.60) · 4 name(s) re-marked at the open (per-name table). HLIT×303 yday $13.92 → 09:30 $13.84 -24.24; SNDK×1 yday $1641.11 → 09:30 $1700.74 +59.63; ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; ARX×51 yday $19.58 → 09:30 $19.57 -0.51 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 303 | $13.84 | $3.99 | $+192.08 | $5,537.12 | ▲ +192.08 after sell → book $10,370.33; vs 09:30 mark -3.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SNDK` | 1 | $1700.74 | $2.02 | $+49.81 | $7,235.85 | ▲ +49.81 after sell → book $10,368.32; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 464 | $4.60 | $6.08 | $+122.49 | $9,364.17 | ▲ +122.49 after sell → book $10,362.24; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 51 | $19.57 | $2.16 | $-4.31 | $10,360.07 | ▼ -4.31 after sell → book $10,360.07; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 89 | $46.18 | $2.26 | — | $6,247.80 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+6.7; leftover $4144.03 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 21 | $142.77 | $2.05 | — | $3,247.57 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3108.02 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $1,218.55 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2072.01 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `GLOB` | 27 | $37.18 | $2.07 | — | $212.62 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; ⚪; ret5=-0.1; leftover $1036.01 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $212.62 | ▲ close $10,557.42 vs 09:30 $10,374.32 (session +205.75) | 16:00 close · cash $212.62 · equity $10,557.42 vs 09:30 $10,374.32 (+183.10; session marks +205.75) · 4 name(s) marked open→close (per-name table). DVN×89 09:30 $46.18 → close $47.57 +123.71; EOG×21 09:30 $142.77 → close $146.15 +70.98; FANG×10 09:30 $202.70 → close $206.29 +35.90; GLOB×27 09:30 $37.18 → close $36.26 -24.84 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.62 | ▲ 09:30 equity $10,681.22 vs yday $10,557.42 (+123.80) | 09:30 open · cash $212.62 (unchanged overnight, no fees) · equity $10,681.22 vs prior close $10,557.42 (+123.80) · 4 name(s) re-marked at the open (per-name table). DVN×89 yday $47.57 → 09:30 $48.00 +38.27; EOG×21 yday $146.15 → 09:30 $148.04 +39.69; FANG×10 yday $206.29 → 09:30 $208.93 +26.40; GLOB×27 yday $36.26 → 09:30 $36.98 +19.44 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 89 | $48.00 | $2.31 | $+157.42 | $4,482.32 | ▲ +157.42 after sell → book $10,678.92; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 21 | $148.04 | $2.09 | $+106.53 | $7,589.07 | ▲ +106.53 after sell → book $10,676.83; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $9,676.32 | ▲ +58.23 after sell → book $10,674.78; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `GLOB` | 27 | $36.98 | $2.09 | $-9.56 | $10,672.69 | ▼ -9.56 after sell → book $10,672.69; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,672.69 | ▲ close $10,672.69 vs 09:30 $10,681.22 (session +0.00) | 16:00 close · cash $10,672.69 · no lots left · equity $10,672.69. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,672.69 | ▲ 09:30 equity $10,672.69 vs yday $10,672.69 (+0.00) | 09:30 open · cash $10,672.69 · no holdings · equity $10,672.69 vs prior close $10,672.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,672.69 | ▲ close $10,672.69 vs 09:30 $10,672.69 (session +0.00) | 16:00 close · cash $10,672.69 · no lots left · equity $10,672.69. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,672.69 | ▲ 09:30 equity $10,672.69 vs yday $10,672.69 (+0.00) | 09:30 open · cash $10,672.69 · no holdings · equity $10,672.69 vs prior close $10,672.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 46 | $91.01 | $2.13 | — | $6,484.10 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $4269.08 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 71 | $44.76 | $2.20 | — | $3,303.94 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $3201.81 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 864 | $2.47 | $11.15 | — | $1,158.71 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $2134.54 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 18 | $58.73 | $2.04 | — | $99.53 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1067.27 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.53 | ▲ close $10,729.80 vs 09:30 $10,672.69 (session +74.63) | 16:00 close · cash $99.53 · equity $10,729.80 vs 09:30 $10,672.69 (+57.11; session marks +74.63) · 4 name(s) marked open→close (per-name table). BHP×46 09:30 $91.01 → close $93.63 +120.52; APA×71 09:30 $44.76 → close $44.39 -26.27; AUTL×864 09:30 $2.47 → close $2.46 -8.64; CRSP×18 09:30 $58.73 → close $58.12 -10.98 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.53 | ▲ 09:30 equity $10,872.61 vs yday $10,729.80 (+142.81) | 09:30 open · cash $99.53 (unchanged overnight, no fees) · equity $10,872.61 vs prior close $10,729.80 (+142.81) · 4 name(s) re-marked at the open (per-name table). BHP×46 yday $93.63 → 09:30 $95.72 +96.14; APA×71 yday $44.39 → 09:30 $44.52 +9.23; AUTL×864 yday $2.46 → 09:30 $2.47 +8.64; CRSP×18 yday $58.12 → 09:30 $59.72 +28.80 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 46 | $95.72 | $2.17 | $+212.36 | $4,500.48 | ▲ +212.36 after sell → book $10,870.44; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 71 | $44.52 | $2.24 | $-21.48 | $7,659.16 | ▼ -21.48 after sell → book $10,868.20; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 42 | $119.43 | $2.12 | — | $2,640.98 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $5106.10 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 22 | $115.18 | $2.06 | — | $104.96 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $2553.05 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.96 | ▲ close $11,069.52 vs 09:30 $10,872.61 (session +205.50) | 16:00 close · cash $104.96 · equity $11,069.52 vs 09:30 $10,872.61 (+196.91; session marks +205.50) · 4 name(s) marked open→close (per-name table). AUTL×864 09:30 $2.47 → close $2.41 -51.84; CRSP×18 09:30 $59.72 → close $59.50 -3.96; AU×42 09:30 $119.43 → close $121.22 +75.18; FUTU×22 09:30 $115.18 → close $123.64 +186.12 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.96 | ▼ 09:30 equity $10,959.48 vs yday $11,069.52 (-110.04) | 09:30 open · cash $104.96 (unchanged overnight, no fees) · equity $10,959.48 vs prior close $11,069.52 (-110.04) · 4 name(s) re-marked at the open (per-name table). AUTL×864 yday $2.41 → 09:30 $2.40 -8.64; CRSP×18 yday $59.50 → 09:30 $58.75 -13.50; AU×42 yday $121.22 → 09:30 $120.51 -29.82; FUTU×22 yday $123.64 → 09:30 $121.00 -58.08 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 864 | $2.40 | $11.31 | $-82.93 | $2,167.26 | ▼ -82.93 after sell → book $10,948.18; vs 09:30 mark -11.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 42 | $120.51 | $2.17 | $+41.08 | $7,226.51 | ▲ +41.08 after sell → book $10,946.01; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 22 | $121.00 | $2.09 | $+123.90 | $9,886.42 | ▲ +123.90 after sell → book $10,943.92; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,886.42 | ▼ close $10,913.77 vs 09:30 $10,959.48 (session -30.15) | 16:00 close · cash $9,886.42 · equity $10,913.77 vs 09:30 $10,959.48 (-45.71; session marks -30.15) · 1 name(s) marked open→close (per-name table). CRSP×18 09:30 $58.75 → close $57.08 -30.15 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,886.42 | ▲ 09:30 equity $10,929.16 vs yday $10,913.77 (+15.39) | 09:30 open · cash $9,886.42 (unchanged overnight, no fees) · equity $10,929.16 vs prior close $10,913.77 (+15.39) · 1 name(s) re-marked at the open (per-name table). CRSP×18 yday $57.08 → 09:30 $57.93 +15.39 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 18 | $57.93 | $2.06 | $-18.51 | $10,927.10 | ▼ -18.51 after sell → book $10,927.10; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 36 | $118.52 | $2.10 | — | $6,658.28 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $4370.84 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 42 | $77.13 | $2.12 | — | $3,416.71 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3278.13 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 62 | $35.05 | $2.18 | — | $1,241.43 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,oppset; 🔵; ⚪; ret5=+19.7; leftover $2185.42 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 45 | $23.80 | $2.12 | — | $168.31 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=+0.5; leftover $1092.71 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.31 | ▲ close $11,219.58 vs 09:30 $10,929.16 (session +300.99) | 16:00 close · cash $168.31 · equity $11,219.58 vs 09:30 $10,929.16 (+290.42; session marks +300.99) · 4 name(s) marked open→close (per-name table). AU×36 09:30 $118.52 → close $123.39 +175.32; FCX×42 09:30 $77.13 → close $79.91 +116.76; EZPW×62 09:30 $35.05 → close $35.23 +11.16; AMX×45 09:30 $23.80 → close $23.75 -2.25 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $168.31 | ▼ 09:30 equity $11,095.54 vs yday $11,219.58 (-124.04) | 09:30 open · cash $168.31 (unchanged overnight, no fees) · equity $11,095.54 vs prior close $11,219.58 (-124.04) · 4 name(s) re-marked at the open (per-name table). AU×36 yday $123.39 → 09:30 $119.80 -129.24; FCX×42 yday $79.91 → 09:30 $79.34 -23.94; EZPW×62 yday $35.23 → 09:30 $35.70 +29.14; AMX×45 yday $23.75 → 09:30 $23.75 +0.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 36 | $119.80 | $2.14 | $+41.84 | $4,478.96 | ▲ +41.84 after sell → book $11,093.39; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 42 | $79.34 | $2.15 | $+88.55 | $7,809.09 | ▲ +88.55 after sell → book $11,091.24; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 62 | $35.70 | $2.20 | $+35.92 | $10,020.29 | ▲ +35.92 after sell → book $11,089.04; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 25 | $267.02 | $2.06 | — | $3,342.72 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $6680.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 161 | $20.72 | $2.47 | — | $4.33 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=+67.1; leftover $3340.10 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.33 | ▲ close $11,212.98 vs 09:30 $11,095.54 (session +128.48) | 16:00 close · cash $4.33 · equity $11,212.98 vs 09:30 $11,095.54 (+117.44; session marks +128.48) · 3 name(s) marked open→close (per-name table). AMX×45 09:30 $23.75 → close $23.62 -5.85; FNV×25 09:30 $267.02 → close $267.37 +8.75; ASST×161 09:30 $20.72 → close $21.50 +125.58 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.33 | ▲ 09:30 equity $11,369.18 vs yday $11,212.98 (+156.20) | 09:30 open · cash $4.33 (unchanged overnight, no fees) · equity $11,369.18 vs prior close $11,212.98 (+156.20) · 3 name(s) re-marked at the open (per-name table). AMX×45 yday $23.62 → 09:30 $23.77 +6.75; FNV×25 yday $267.37 → 09:30 $267.23 -3.50; ASST×161 yday $21.50 → 09:30 $22.45 +152.95 | — |
| 2026-08-27 09:30 ET | **SELL** | `AMX` | 45 | $23.77 | $2.15 | $-5.62 | $1,071.83 | ▼ -5.62 after sell → book $11,367.03; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 25 | $267.23 | $2.13 | $+1.06 | $7,750.46 | ▲ +1.06 after sell → book $11,364.91; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 161 | $22.45 | $2.53 | $+273.53 | $11,362.38 | ▲ +273.53 after sell → book $11,362.38; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 55 | $81.65 | $2.15 | — | $6,869.47 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $4544.95 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 3 | $967.01 | $2.00 | — | $3,966.44 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $3408.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ASML` | 1 | $1746.53 | $1.99 | — | $2,217.92 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=-0.3; leftover $2272.48 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 9 | $118.77 | $2.02 | — | $1,146.97 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; ret5=+0.3; leftover $1136.24 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,146.97 | ▼ close $11,148.66 vs 09:30 $11,369.18 (session -205.55) | 16:00 close · cash $1,146.97 · equity $11,148.66 vs 09:30 $11,369.18 (-220.52; session marks -205.55) · 4 name(s) marked open→close (per-name table). ACMR×55 09:30 $81.65 → close $80.49 -63.80; MU×3 09:30 $967.01 → close $935.39 -94.86; ASML×1 09:30 $1746.53 → close $1735.01 -11.52; CM×9 09:30 $118.77 → close $114.84 -35.37 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,146.97 | ▼ 09:30 equity $11,040.38 vs yday $11,148.66 (-108.28) | 09:30 open · cash $1,146.97 (unchanged overnight, no fees) · equity $11,040.38 vs prior close $11,148.66 (-108.28) · 4 name(s) re-marked at the open (per-name table). ACMR×55 yday $80.49 → 09:30 $79.27 -67.10; MU×3 yday $935.39 → 09:30 $919.29 -48.30; ASML×1 yday $1735.01 → 09:30 $1734.75 -0.26; CM×9 yday $114.84 → 09:30 $115.66 +7.38 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 55 | $79.27 | $2.20 | $-135.25 | $5,504.62 | ▼ -135.25 after sell → book $11,038.18; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 3 | $919.29 | $2.03 | $-147.19 | $8,260.46 | ▼ -147.19 after sell → book $11,036.15; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASML` | 1 | $1734.75 | $2.02 | $-15.79 | $9,993.19 | ▼ -15.79 after sell → book $11,034.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 9 | $115.66 | $2.04 | $-32.04 | $11,032.10 | ▼ -32.04 after sell → book $11,032.10; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 13 | $324.41 | $2.03 | — | $6,812.74 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $4412.84 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 23 | $141.76 | $2.06 | — | $3,550.20 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $3309.63 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 5 | $400.42 | $2.00 | — | $1,546.09 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2206.42 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,546.09 | ▼ close $10,614.81 vs 09:30 $11,040.38 (session -411.19) | 16:00 close · cash $1,546.09 · equity $10,614.81 vs 09:30 $11,040.38 (-425.57; session marks -411.19) · 3 name(s) marked open→close (per-name table). KEYS×13 09:30 $324.41 → close $319.97 -57.72; SMTC×23 09:30 $141.76 → close $131.17 -243.57; CIEN×5 09:30 $400.42 → close $378.44 -109.90 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,546.09 | ▲ 09:30 equity $10,673.56 vs yday $10,614.81 (+58.75) | 09:30 open · cash $1,546.09 (unchanged overnight, no fees) · equity $10,673.56 vs prior close $10,614.81 (+58.75) · 3 name(s) re-marked at the open (per-name table). KEYS×13 yday $319.97 → 09:30 $322.49 +32.76; SMTC×23 yday $131.17 → 09:30 $132.30 +25.99; CIEN×5 yday $378.44 → 09:30 $378.44 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 13 | $322.49 | $2.07 | $-29.06 | $5,736.39 | ▼ -29.06 after sell → book $10,671.49; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 23 | $132.30 | $2.09 | $-221.73 | $8,777.20 | ▼ -221.73 after sell → book $10,669.40; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 5 | $378.44 | $2.03 | $-113.94 | $10,667.37 | ▼ -113.94 after sell → book $10,667.37; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,667.37 | ▲ close $10,667.37 vs 09:30 $10,673.56 (session +0.00) | 16:00 close · cash $10,667.37 · no lots left · equity $10,667.37. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,667.37 | ▲ 09:30 equity $10,667.37 vs yday $10,667.37 (-0.00) | 09:30 open · cash $10,667.37 · no holdings · equity $10,667.37 vs prior close $10,667.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,667.37 | ▲ close $10,667.37 vs 09:30 $10,667.37 (session +0.00) | 16:00 close · cash $10,667.37 · no lots left · equity $10,667.37. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,667.37 | ▲ 09:30 equity $10,667.37 vs yday $10,667.37 (-0.00) | 09:30 open · cash $10,667.37 · no holdings · equity $10,667.37 vs prior close $10,667.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,667.37 | ▲ close $10,667.37 vs 09:30 $10,667.37 (session +0.00) | 16:00 close · cash $10,667.37 · no lots left · equity $10,667.37. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,667.37 | ▲ 09:30 equity $10,667.37 vs yday $10,667.37 (-0.00) | 09:30 open · cash $10,667.37 · no holdings · equity $10,667.37 vs prior close $10,667.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 12 | $351.74 | $2.03 | — | $6,444.46 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $4266.95 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 6 | $486.31 | $2.01 | — | $3,524.59 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $3200.21 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 66 | $32.31 | $2.19 | — | $1,389.95 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $2133.47 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 67 | $15.87 | $2.19 | — | $324.47 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1066.74 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $324.47 | ▲ close $11,062.59 vs 09:30 $10,667.37 (session +403.63) | 16:00 close · cash $324.47 · equity $11,062.59 vs 09:30 $10,667.37 (+395.22; session marks +403.63) · 4 name(s) marked open→close (per-name table). AVGO×12 09:30 $351.74 → close $357.16 +65.04; DELL×6 09:30 $486.31 → close $516.39 +180.48; CXW×66 09:30 $32.31 → close $33.66 +89.10; FRNM×67 09:30 $15.87 → close $16.90 +69.01 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $324.47 | ▼ 09:30 equity $11,030.71 vs yday $11,062.59 (-31.88) | 09:30 open · cash $324.47 (unchanged overnight, no fees) · equity $11,030.71 vs prior close $11,062.59 (-31.88) · 4 name(s) re-marked at the open (per-name table). AVGO×12 yday $357.16 → 09:30 $359.70 +30.48; DELL×6 yday $516.39 → 09:30 $513.78 -15.66; CXW×66 yday $33.66 → 09:30 $33.46 -13.20; FRNM×67 yday $16.90 → 09:30 $16.40 -33.50 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 12 | $359.70 | $2.07 | $+91.42 | $4,638.80 | ▲ +91.42 after sell → book $11,028.64; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 6 | $513.78 | $2.04 | $+160.77 | $7,719.43 | ▲ +160.77 after sell → book $11,026.59; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 66 | $33.46 | $2.22 | $+71.50 | $9,925.58 | ▲ +71.50 after sell → book $11,024.38; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 18 | $263.36 | $2.04 | — | $5,183.05 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $4962.79 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MMED` | 138 | $23.84 | $2.40 | — | $1,890.73 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ⚪; ret5=+22.2; leftover $3308.53 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HPE` | 30 | $53.85 | $2.08 | — | $273.15 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=+0.1; leftover $1654.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟡 vol🟢 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $273.15 | ▼ close $10,806.08 vs 09:30 $11,030.71 (session -211.77) | 16:00 close · cash $273.15 · equity $10,806.08 vs 09:30 $11,030.71 (-224.63; session marks -211.77) · 4 name(s) marked open→close (per-name table). FRNM×67 09:30 $16.40 → close $16.31 -6.03; CRM×18 09:30 $263.36 → close $259.23 -74.34; MMED×138 09:30 $23.84 → close $23.29 -75.90; HPE×30 09:30 $53.85 → close $52.00 -55.50 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $273.15 | ▼ 09:30 equity $10,726.47 vs yday $10,806.08 (-79.61) | 09:30 open · cash $273.15 (unchanged overnight, no fees) · equity $10,726.47 vs prior close $10,806.08 (-79.61) · 4 name(s) re-marked at the open (per-name table). FRNM×67 yday $16.31 → 09:30 $16.74 +28.81; CRM×18 yday $259.23 → 09:30 $253.72 -99.18; MMED×138 yday $23.29 → 09:30 $23.16 -17.94; HPE×30 yday $52.00 → 09:30 $52.29 +8.70 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 67 | $16.74 | $2.21 | $+53.89 | $1,392.52 | ▲ +53.89 after sell → book $10,724.26; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 18 | $253.72 | $2.09 | $-177.65 | $5,957.39 | ▼ -177.65 after sell → book $10,722.17; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 138 | $23.16 | $2.45 | $-98.70 | $9,151.01 | ▼ -98.70 after sell → book $10,719.71; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HPE` | 30 | $52.29 | $2.10 | $-50.98 | $10,717.61 | ▼ -50.98 after sell → book $10,717.61; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,717.61 | ▲ close $10,717.61 vs 09:30 $10,726.47 (session +0.00) | 16:00 close · cash $10,717.61 · no lots left · equity $10,717.61. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,717.61 | ▲ 09:30 equity $10,717.61 vs yday $10,717.61 (+0.00) | 09:30 open · cash $10,717.61 · no holdings · equity $10,717.61 vs prior close $10,717.61 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,717.61 | ▲ close $10,717.61 vs 09:30 $10,717.61 (session +0.00) | 16:00 close · cash $10,717.61 · no lots left · equity $10,717.61. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,717.61 | ▲ 09:30 equity $10,717.61 vs yday $10,717.61 (+0.00) | 09:30 open · cash $10,717.61 · no holdings · equity $10,717.61 vs prior close $10,717.61 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,717.61 | ▲ close $10,717.61 vs 09:30 $10,717.61 (session +0.00) | 16:00 close · cash $10,717.61 · no lots left · equity $10,717.61. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,717.61 | ▲ 09:30 equity $10,717.61 vs yday $10,717.61 (+0.00) | 09:30 open · cash $10,717.61 · no holdings · equity $10,717.61 vs prior close $10,717.61 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 43 | $164.43 | $2.12 | — | $3,645.00 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $7145.07 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 63 | $56.03 | $2.18 | — | $112.93 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=-0.8; leftover $3572.54 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $112.93 | ▼ close $10,055.09 vs 09:30 $10,717.61 (session -658.22) | 16:00 close · cash $112.93 · equity $10,055.09 vs 09:30 $10,717.61 (-662.52; session marks -658.22) · 2 name(s) marked open→close (per-name table). ORCL×43 09:30 $164.43 → close $150.28 -608.45; BTI×63 09:30 $56.03 → close $55.24 -49.77 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $112.93 | ▼ 09:30 equity $9,792.55 vs yday $10,055.09 (-262.54) | 09:30 open · cash $112.93 (unchanged overnight, no fees) · equity $9,792.55 vs prior close $10,055.09 (-262.54) · 2 name(s) re-marked at the open (per-name table). ORCL×43 yday $150.28 → 09:30 $141.42 -380.98; BTI×63 yday $55.24 → 09:30 $57.12 +118.44 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 43 | $141.42 | $2.18 | $-993.73 | $6,191.81 | ▼ -993.73 after sell → book $9,790.37; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `BTI` | 63 | $57.12 | $2.22 | $+64.27 | $9,788.16 | ▲ +64.27 after sell → book $9,788.16; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,788.16 | ▲ close $9,788.16 vs 09:30 $9,792.55 (session +0.00) | 16:00 close · cash $9,788.16 · no lots left · equity $9,788.16. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,788.16 | ▲ 09:30 equity $9,788.16 vs yday $9,788.16 (-0.00) | 09:30 open · cash $9,788.16 · no holdings · equity $9,788.16 vs prior close $9,788.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,788.16 | ▲ close $9,788.16 vs 09:30 $9,788.16 (session +0.00) | 16:00 close · cash $9,788.16 · no lots left · equity $9,788.16. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,788.16 | ▲ 09:30 equity $9,788.16 vs yday $9,788.16 (-0.00) | 09:30 open · cash $9,788.16 · no holdings · equity $9,788.16 vs prior close $9,788.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 149 | $26.27 | $2.44 | — | $5,871.49 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $3915.26 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 15 | $189.17 | $2.04 | — | $3,031.90 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $2936.45 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 48 | $39.99 | $2.13 | — | $1,110.25 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1957.63 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `AMX` | 42 | $23.18 | $2.12 | — | $134.57 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list oppset; 🔵; ret5=-0.2; leftover $978.82 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.57 | ▼ close $9,665.92 vs 09:30 $9,788.16 (session -113.51) | 16:00 close · cash $134.57 · equity $9,665.92 vs 09:30 $9,788.16 (-122.24; session marks -113.51) · 4 name(s) marked open→close (per-name table). WAY×149 09:30 $26.27 → close $26.59 +47.68; QCOM×15 09:30 $189.17 → close $184.84 -64.95; SM×48 09:30 $39.99 → close $38.16 -87.84; AMX×42 09:30 $23.18 → close $22.98 -8.40 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.57 | ▲ 09:30 equity $9,712.95 vs yday $9,665.92 (+47.03) | 09:30 open · cash $134.57 (unchanged overnight, no fees) · equity $9,712.95 vs prior close $9,665.92 (+47.03) · 4 name(s) re-marked at the open (per-name table). WAY×149 yday $26.59 → 09:30 $26.51 -11.92; QCOM×15 yday $184.84 → 09:30 $190.35 +82.65; SM×48 yday $38.16 → 09:30 $37.57 -28.32; AMX×42 yday $22.98 → 09:30 $23.09 +4.62 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 149 | $26.51 | $2.49 | $+30.83 | $4,082.07 | ▲ +30.83 after sell → book $9,710.46; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 15 | $190.35 | $2.07 | $+13.60 | $6,935.25 | ▲ +13.60 after sell → book $9,708.39; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 48 | $37.57 | $2.16 | $-120.45 | $8,736.46 | ▼ -120.45 after sell → book $9,706.24; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `AMX` | 42 | $23.09 | $2.14 | $-8.03 | $9,704.10 | ▼ -8.03 after sell → book $9,704.10; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 22 | $170.85 | $2.06 | — | $5,943.34 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $3881.64 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 184 | $15.81 | $2.54 | — | $3,031.76 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $2911.23 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 87 | $22.12 | $2.25 | — | $1,105.07 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1940.82 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 4 | $238.60 | $2.00 | — | $148.67 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_mover,oppset; ret5=-11.6; leftover $970.41 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.67 | ▲ close $9,915.28 vs 09:30 $9,712.95 (session +220.03) | 16:00 close · cash $148.67 · equity $9,915.28 vs 09:30 $9,712.95 (+202.33; session marks +220.03) · 4 name(s) marked open→close (per-name table). SMTC×22 09:30 $170.85 → close $178.19 +161.48; AVTR×184 09:30 $15.81 → close $15.86 +9.20; GME×87 09:30 $22.12 → close $22.77 +56.55; JBHT×4 09:30 $238.60 → close $236.80 -7.20 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.67 | ▲ 09:30 equity $10,019.51 vs yday $9,915.28 (+104.23) | 09:30 open · cash $148.67 (unchanged overnight, no fees) · equity $10,019.51 vs prior close $9,915.28 (+104.23) · 4 name(s) re-marked at the open (per-name table). SMTC×22 yday $178.19 → 09:30 $182.33 +91.08; AVTR×184 yday $15.86 → 09:30 $15.87 +1.84; GME×87 yday $22.77 → 09:30 $22.90 +11.31; JBHT×4 yday $236.80 → 09:30 $236.80 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 22 | $182.33 | $2.10 | $+248.41 | $4,157.83 | ▲ +248.41 after sell → book $10,017.41; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 184 | $15.87 | $2.60 | $+5.90 | $7,075.31 | ▲ +5.90 after sell → book $10,014.81; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 4 | $236.80 | $2.02 | $-11.22 | $8,020.49 | ▼ -11.22 after sell → book $10,012.79; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 191 | $20.91 | $2.56 | — | $4,024.12 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $4010.25 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 180 | $14.79 | $2.53 | — | $1,359.39 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $2673.50 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 95 | $14.07 | $2.27 | — | $20.46 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1336.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.46 | ▼ close $9,943.13 vs 09:30 $10,019.51 (session -62.29) | 16:00 close · cash $20.46 · equity $9,943.13 vs 09:30 $10,019.51 (-76.38; session marks -62.29) · 4 name(s) marked open→close (per-name table). GME×87 09:30 $22.90 → close $22.64 -22.62; TH×191 09:30 $20.91 → close $21.19 +53.48; RARE×180 09:30 $14.79 → close $14.51 -50.40; BHVN×95 09:30 $14.07 → close $13.62 -42.75 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-28 | `MPWR` | cash | leftover split 1103.21 < 1 share @ 1306.03 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CVE` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GME` | 87 | 2026-09-17 @ $22.12 | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1940.82 |
| `TH` | 191 | 2026-09-18 @ $20.91 | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $4010.25 |
| `RARE` | 180 | 2026-09-18 @ $14.79 | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $2673.50 |
| `BHVN` | 95 | 2026-09-18 @ $14.07 | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1336.75 |
