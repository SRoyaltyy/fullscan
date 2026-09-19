# Factor mine action — `union_news_g_cam61_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢 and cameras +6 −≤1

Cash book **-3.08%** ($9,692) · signal-only (no cash/fees) was -3.06%. Starts YES **4/26**. Fills 90 · skips 8 · realized $-290.77.

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
- Must-have: the news camera (does the morning packet like the headline?) is green.
- Must-have: at least 6 green cameras (the +G half of +G −R).
- Must-have: at most 1 red cameras (the −R half of +G −R; 🚨 is not counted here).
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
- **Gate** `news=good,n_pos_min=6,cam_bad_max=1` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6.83.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 21 | — | $91.01 | +0.00 | $93.63 | +55.02 | +55.02 | +0.00 | +55.02 |
| 2026-08-20 | `APA` | 44 | — | $44.76 | +0.00 | $44.39 | -16.28 | -16.28 | +0.00 | -16.28 |
| 2026-08-20 | `AUTL` | 809 | — | $2.47 | +0.00 | $2.46 | -8.09 | -8.09 | +0.00 | -8.09 |
| 2026-08-20 | `CRSP` | 34 | — | $58.73 | +0.00 | $58.12 | -20.74 | -20.74 | +0.00 | -20.74 |
| 2026-08-20 | `MRK` | 13 | — | $150.78 | +0.00 | $148.99 | -23.27 | -23.27 | +0.00 | -23.27 |
| 2026-08-21 | `BHP` | 21 | $93.63 | $95.72 | +43.89 | — | +0.00 | +43.89 | +98.91 | — |
| 2026-08-21 | `APA` | 44 | $44.39 | $44.52 | +5.72 | — | +0.00 | +5.72 | -10.56 | — |
| 2026-08-21 | `AUTL` | 809 | $2.46 | $2.47 | +8.09 | $2.41 | -48.54 | -40.45 | +0.00 | -48.54 |
| 2026-08-21 | `CRSP` | 34 | $58.12 | $59.72 | +54.40 | $59.50 | -7.48 | +46.92 | +33.66 | +26.18 |
| 2026-08-21 | `MRK` | 13 | $148.99 | $149.12 | +1.69 | — | +0.00 | +1.69 | -21.58 | — |
| 2026-08-21 | `AU` | 8 | — | $119.43 | +0.00 | $121.22 | +14.32 | +14.32 | +0.00 | +14.32 |
| 2026-08-21 | `FUTU` | 8 | — | $115.18 | +0.00 | $123.64 | +67.68 | +67.68 | +0.00 | +67.68 |
| 2026-08-21 | `GRAL` | 12 | — | $78.88 | +0.00 | $79.54 | +7.92 | +7.92 | +0.00 | +7.92 |
| 2026-08-21 | `VIRT` | 16 | — | $60.66 | +0.00 | $67.93 | +116.32 | +116.32 | +0.00 | +116.32 |
| 2026-08-21 | `MFC` | 23 | — | $42.48 | +0.00 | $42.51 | +0.69 | +0.69 | +0.00 | +0.69 |
| 2026-08-21 | `ABTC` | 116 | — | $8.66 | +0.00 | $7.93 | -84.68 | -84.68 | +0.00 | -84.68 |
| 2026-08-24 | `AUTL` | 809 | $2.41 | $2.40 | -8.09 | — | +0.00 | -8.09 | -56.63 | — |
| 2026-08-24 | `CRSP` | 34 | $59.50 | $58.75 | -25.50 | $57.08 | -56.95 | -82.45 | +0.68 | -56.27 |
| 2026-08-24 | `AU` | 8 | $121.22 | $120.51 | -5.68 | — | +0.00 | -5.68 | +8.64 | — |
| 2026-08-24 | `FUTU` | 8 | $123.64 | $121.00 | -21.12 | — | +0.00 | -21.12 | +46.56 | — |
| 2026-08-24 | `GRAL` | 12 | $79.54 | $81.87 | +27.96 | — | +0.00 | +27.96 | +35.88 | — |
| 2026-08-24 | `VIRT` | 16 | $67.93 | $66.80 | -18.08 | — | +0.00 | -18.08 | +98.24 | — |
| 2026-08-24 | `MFC` | 23 | $42.51 | $42.31 | -4.60 | — | +0.00 | -4.60 | -3.91 | — |
| 2026-08-24 | `ABTC` | 116 | $7.93 | $8.00 | +8.12 | — | +0.00 | +8.12 | -76.56 | — |
| 2026-08-25 | `CRSP` | 34 | $57.08 | $57.93 | +29.07 | — | +0.00 | +29.07 | -27.20 | — |
| 2026-08-25 | `AU` | 42 | — | $118.52 | +0.00 | $123.39 | +204.54 | +204.54 | +0.00 | +204.54 |
| 2026-08-25 | `FCX` | 65 | — | $77.13 | +0.00 | $79.91 | +180.70 | +180.70 | +0.00 | +180.70 |
| 2026-08-26 | `AU` | 42 | $123.39 | $119.80 | -150.78 | — | +0.00 | -150.78 | +53.76 | — |
| 2026-08-26 | `FCX` | 65 | $79.91 | $79.34 | -37.05 | — | +0.00 | -37.05 | +143.65 | — |
| 2026-08-26 | `ASST` | 246 | — | $20.72 | +0.00 | $21.50 | +191.88 | +191.88 | +0.00 | +191.88 |
| 2026-08-26 | `AMX` | 215 | — | $23.75 | +0.00 | $23.62 | -27.95 | -27.95 | +0.00 | -27.95 |
| 2026-08-27 | `ASST` | 246 | $21.50 | $22.45 | +233.70 | — | +0.00 | +233.70 | +425.58 | — |
| 2026-08-27 | `AMX` | 215 | $23.62 | $23.77 | +32.25 | — | +0.00 | +32.25 | +4.30 | — |
| 2026-08-27 | `ACMR` | 18 | — | $81.65 | +0.00 | $80.49 | -20.88 | -20.88 | +0.00 | -20.88 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `CM` | 12 | — | $118.77 | +0.00 | $114.84 | -47.16 | -47.16 | +0.00 | -47.16 |
| 2026-08-27 | `GEN` | 50 | — | $29.83 | +0.00 | $30.50 | +33.50 | +33.50 | +0.00 | +33.50 |
| 2026-08-27 | `LRCX` | 4 | — | $318.88 | +0.00 | $318.58 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-27 | `NVDA` | 6 | — | $222.86 | +0.00 | $227.98 | +30.72 | +30.72 | +0.00 | +30.72 |
| 2026-08-28 | `ACMR` | 18 | $80.49 | $79.27 | -21.96 | — | +0.00 | -21.96 | -42.84 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `CM` | 12 | $114.84 | $115.66 | +9.84 | — | +0.00 | +9.84 | -37.32 | — |
| 2026-08-28 | `GEN` | 50 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +33.50 | — |
| 2026-08-28 | `LRCX` | 4 | $318.58 | $318.03 | -2.20 | — | +0.00 | -2.20 | -3.40 | — |
| 2026-08-28 | `NVDA` | 6 | $227.98 | $227.36 | -3.72 | — | +0.00 | -3.72 | +27.00 | — |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `SMTC` | 10 | — | $141.76 | +0.00 | $131.17 | -105.90 | -105.90 | +0.00 | -105.90 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 6 | — | $240.22 | +0.00 | $236.98 | -19.44 | -19.44 | +0.00 | -19.44 |
| 2026-08-28 | `PLAB` | 50 | — | $30.01 | +0.00 | $27.73 | -114.00 | -114.00 | +0.00 | -114.00 |
| 2026-08-28 | `SEDG` | 45 | — | $32.90 | +0.00 | $31.41 | -67.05 | -67.05 | +0.00 | -67.05 |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `SMTC` | 10 | $131.17 | $132.30 | +11.30 | — | +0.00 | +11.30 | -94.60 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `DDOG` | 6 | $236.98 | $233.97 | -18.09 | — | +0.00 | -18.09 | -37.53 | — |
| 2026-08-31 | `PLAB` | 50 | $27.73 | $28.04 | +15.50 | — | +0.00 | +15.50 | -98.50 | — |
| 2026-08-31 | `SEDG` | 45 | $31.41 | $31.15 | -11.70 | — | +0.00 | -11.70 | -78.75 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 5 | — | $351.74 | +0.00 | $357.16 | +27.10 | +27.10 | +0.00 | +27.10 |
| 2026-09-03 | `DELL` | 4 | — | $486.31 | +0.00 | $516.39 | +120.32 | +120.32 | +0.00 | +120.32 |
| 2026-09-03 | `CXW` | 62 | — | $32.31 | +0.00 | $33.66 | +83.70 | +83.70 | +0.00 | +83.70 |
| 2026-09-03 | `FRNM` | 127 | — | $15.87 | +0.00 | $16.90 | +130.81 | +130.81 | +0.00 | +130.81 |
| 2026-09-03 | `MMED` | 84 | — | $23.88 | +0.00 | $23.84 | -3.36 | -3.36 | +0.00 | -3.36 |
| 2026-09-04 | `AVGO` | 5 | $357.16 | $359.70 | +12.70 | — | +0.00 | +12.70 | +39.80 | — |
| 2026-09-04 | `DELL` | 4 | $516.39 | $513.78 | -10.44 | — | +0.00 | -10.44 | +109.88 | — |
| 2026-09-04 | `CXW` | 62 | $33.66 | $33.46 | -12.40 | — | +0.00 | -12.40 | +71.30 | — |
| 2026-09-04 | `FRNM` | 127 | $16.90 | $16.40 | -63.50 | $16.31 | -11.43 | -74.93 | +67.31 | +55.88 |
| 2026-09-04 | `MMED` | 84 | $23.84 | $23.84 | +0.00 | $23.29 | -46.20 | -46.20 | -3.36 | -49.56 |
| 2026-09-04 | `CRM` | 7 | — | $263.36 | +0.00 | $259.23 | -28.91 | -28.91 | +0.00 | -28.91 |
| 2026-09-04 | `HPE` | 38 | — | $53.85 | +0.00 | $52.00 | -70.30 | -70.30 | +0.00 | -70.30 |
| 2026-09-04 | `MRX` | 27 | — | $75.65 | +0.00 | $78.27 | +70.74 | +70.74 | +0.00 | +70.74 |
| 2026-09-08 | `FRNM` | 127 | $16.31 | $16.74 | +54.61 | — | +0.00 | +54.61 | +110.49 | — |
| 2026-09-08 | `MMED` | 84 | $23.29 | $23.16 | -10.92 | — | +0.00 | -10.92 | -60.48 | — |
| 2026-09-08 | `CRM` | 7 | $259.23 | $253.72 | -38.57 | — | +0.00 | -38.57 | -67.48 | — |
| 2026-09-08 | `HPE` | 38 | $52.00 | $52.29 | +11.02 | — | +0.00 | +11.02 | -59.28 | — |
| 2026-09-08 | `MRX` | 27 | $78.27 | $78.84 | +15.39 | — | +0.00 | +15.39 | +86.13 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 31 | — | $164.43 | +0.00 | $150.28 | -438.65 | -438.65 | +0.00 | -438.65 |
| 2026-09-11 | `BTI` | 91 | — | $56.03 | +0.00 | $55.24 | -71.89 | -71.89 | +0.00 | -71.89 |
| 2026-09-14 | `ORCL` | 31 | $150.28 | $141.42 | -274.66 | — | +0.00 | -274.66 | -713.31 | — |
| 2026-09-14 | `BTI` | 91 | $55.24 | $57.12 | +171.08 | — | +0.00 | +171.08 | +99.19 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 122 | — | $26.27 | +0.00 | $26.59 | +39.04 | +39.04 | +0.00 | +39.04 |
| 2026-09-16 | `QCOM` | 17 | — | $189.17 | +0.00 | $184.84 | -73.61 | -73.61 | +0.00 | -73.61 |
| 2026-09-16 | `SM` | 80 | — | $39.99 | +0.00 | $38.16 | -146.40 | -146.40 | +0.00 | -146.40 |
| 2026-09-17 | `WAY` | 122 | $26.59 | $26.51 | -9.76 | — | +0.00 | -9.76 | +29.28 | — |
| 2026-09-17 | `QCOM` | 17 | $184.84 | $190.35 | +93.67 | — | +0.00 | +93.67 | +20.06 | — |
| 2026-09-17 | `SM` | 80 | $38.16 | $37.57 | -47.20 | — | +0.00 | -47.20 | -193.60 | — |
| 2026-09-17 | `SMTC` | 18 | — | $170.85 | +0.00 | $178.19 | +132.12 | +132.12 | +0.00 | +132.12 |
| 2026-09-17 | `AVTR` | 200 | — | $15.81 | +0.00 | $15.86 | +10.00 | +10.00 | +0.00 | +10.00 |
| 2026-09-17 | `GME` | 143 | — | $22.12 | +0.00 | $22.77 | +92.95 | +92.95 | +0.00 | +92.95 |
| 2026-09-18 | `SMTC` | 18 | $178.19 | $182.33 | +74.52 | — | +0.00 | +74.52 | +206.64 | — |
| 2026-09-18 | `AVTR` | 200 | $15.86 | $15.87 | +2.00 | — | +0.00 | +2.00 | +12.00 | — |
| 2026-09-18 | `GME` | 143 | $22.77 | $22.90 | +18.59 | $22.64 | -37.18 | -18.59 | +111.54 | +74.36 |
| 2026-09-18 | `TH` | 104 | — | $20.91 | +0.00 | $21.19 | +29.12 | +29.12 | +0.00 | +29.12 |
| 2026-09-18 | `RARE` | 147 | — | $14.79 | +0.00 | $14.51 | -41.16 | -41.16 | +0.00 | -41.16 |
| 2026-09-18 | `BHVN` | 155 | — | $14.07 | +0.00 | $13.62 | -69.75 | -69.75 | +0.00 | -69.75 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | -13.36 | BHP, APA, AUTL, CRSP, MRK | — | $145.43 | $9,967.91 | BHP×21, APA×44, AUTL×809, CRSP×34, MRK×13 |
| 2026-08-21 | +3.25 | $145.43 | BHP×21, APA×44, AUTL×809, CRSP×34, MRK×13 | $10,081.70 | +113.79 | +66.23 | AU, FUTU, GRAL, VIRT, MFC, ABTC | BHP, APA, MRK | $258.62 | $10,129.16 | AUTL×809, CRSP×34, AU×8, FUTU×8, GRAL×12, VIRT×16, MFC×23, ABTC×116 |
| 2026-08-24 | -5.17 | $258.62 | AUTL×809, CRSP×34, AU×8, FUTU×8, GRAL×12, VIRT×16, MFC×23, ABTC×116 | $10,082.17 | -46.99 | -56.95 | — | AUTL, AU, FUTU, GRAL, VIRT, MFC, ABTC | $8,061.46 | $10,002.01 | CRSP×34 |
| 2026-08-25 | +1.80 | $8,061.46 | CRSP×34 | $10,031.08 | +29.07 | +385.24 | AU, FCX | CRSP | $33.37 | $10,409.90 | AU×42, FCX×65 |
| 2026-08-26 | +2.02 | $33.37 | AU×42, FCX×65 | $10,222.07 | -187.83 | +163.93 | ASST, AMX | AU, FCX | $8.35 | $10,375.65 | ASST×246, AMX×215 |
| 2026-08-27 | — | $8.35 | ASST×246, AMX×215 | $10,641.60 | +265.95 | -36.64 | ACMR, MU, CM, GEN, LRCX, NVDA | ASST, AMX | $2,657.15 | $10,586.64 | ACMR×18, MU×1, CM×12, GEN×50, LRCX×4, NVDA×6 |
| 2026-08-28 | +0.75 | $2,657.15 | ACMR×18, MU×1, CM×12, GEN×50, LRCX×4, NVDA×6 | $10,552.50 | -34.14 | -439.86 | KEYS, SMTC, CIEN, MPWR, DDOG, PLAB, SEDG | ACMR, MU, CM, GEN, LRCX, NVDA | $881.03 | $10,086.02 | KEYS×4, SMTC×10, CIEN×3, MPWR×1, DDOG×6, PLAB×50, SEDG×45 |
| 2026-08-31 | -5.85 | $881.03 | KEYS×4, SMTC×10, CIEN×3, MPWR×1, DDOG×6, PLAB×50, SEDG×45 | $10,098.75 | +12.73 | +0.00 | — | KEYS, SMTC, CIEN, MPWR, DDOG, PLAB, SEDG | $10,084.32 | $10,084.32 | — |
| 2026-09-01 | -6.30 | $10,084.32 | — | $10,084.32 | -0.00 | +0.00 | — | — | $10,084.32 | $10,084.32 | — |
| 2026-09-02 | -3.83 | $10,084.32 | — | $10,084.32 | -0.00 | +0.00 | — | — | $10,084.32 | $10,084.32 | — |
| 2026-09-03 | -0.90 | $10,084.32 | — | $10,084.32 | -0.00 | +358.57 | AVGO, DELL, CXW, FRNM, MMED | — | $344.95 | $10,432.09 | AVGO×5, DELL×4, CXW×62, FRNM×127, MMED×84 |
| 2026-09-04 | +2.25 | $344.95 | AVGO×5, DELL×4, CXW×62, FRNM×127, MMED×84 | $10,358.45 | -73.64 | -86.10 | CRM, HPE, MRX | AVGO, DELL, CXW | $328.27 | $10,259.90 | FRNM×127, MMED×84, CRM×7, HPE×38, MRX×27 |
| 2026-09-08 | -11.47 | $328.27 | FRNM×127, MMED×84, CRM×7, HPE×38, MRX×27 | $10,291.43 | +31.53 | +0.00 | — | FRNM, MMED, CRM, HPE, MRX | $10,280.49 | $10,280.49 | — |
| 2026-09-09 | -13.95 | $10,280.49 | — | $10,280.49 | -0.00 | +0.00 | — | — | $10,280.49 | $10,280.49 | — |
| 2026-09-10 | -13.28 | $10,280.49 | — | $10,280.49 | -0.00 | +0.00 | — | — | $10,280.49 | $10,280.49 | — |
| 2026-09-11 | +0.50 | $10,280.49 | — | $10,280.49 | -0.00 | -510.54 | ORCL, BTI | — | $80.08 | $9,765.60 | ORCL×31, BTI×91 |
| 2026-09-14 | -11.00 | $80.08 | ORCL×31, BTI×91 | $9,662.02 | -103.58 | +0.00 | — | ORCL, BTI | $9,657.58 | $9,657.58 | — |
| 2026-09-15 | -3.84 | $9,657.58 | — | $9,657.58 | -0.00 | +0.00 | — | — | $9,657.58 | $9,657.58 | — |
| 2026-09-16 | +5.30 | $9,657.58 | — | $9,657.58 | -0.00 | -180.97 | WAY, QCOM, SM | — | $30.92 | $9,469.98 | WAY×122, QCOM×17, SM×80 |
| 2026-09-17 | +7.38 | $30.92 | WAY×122, QCOM×17, SM×80 | $9,506.69 | +36.71 | +235.07 | SMTC, AVTR, GME | WAY, QCOM, SM | $92.43 | $9,727.96 | SMTC×18, AVTR×200, GME×143 |
| 2026-09-18 | +4.86 | $92.43 | SMTC×18, AVTR×200, GME×143 | $9,823.07 | +95.11 | -118.97 | TH, RARE, BHVN | SMTC, AVTR | $6.83 | $9,692.18 | GME×143, TH×104, RARE×147, BHVN×155 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 21 | $91.01 | $2.05 | — | $8,086.74 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2000.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 44 | $44.76 | $2.12 | — | $6,115.18 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $2000.00 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 809 | $2.47 | $10.44 | — | $4,106.51 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $2000.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 34 | $58.73 | $2.09 | — | $2,107.60 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $2000.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRK` | 13 | $150.78 | $2.03 | — | $145.43 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list oppset; 🔵; ⚪; ret5=+14.5; leftover $2000.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.43 | ▼ close $9,967.91 vs 09:30 $10,000.00 (session -13.36) | 16:00 close · cash $145.43 · equity $9,967.91 vs 09:30 $10,000.00 (-32.09; session marks -13.36) · 5 name(s) marked open→close (per-name table). BHP×21 09:30 $91.01 → close $93.63 +55.02; APA×44 09:30 $44.76 → close $44.39 -16.28; AUTL×809 09:30 $2.47 → close $2.46 -8.09; CRSP×34 09:30 $58.73 → close $58.12 -20.74; MRK×13 09:30 $150.78 → close $148.99 -23.27 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.43 | ▲ 09:30 equity $10,081.70 vs yday $9,967.91 (+113.79) | 09:30 open · cash $145.43 (unchanged overnight, no fees) · equity $10,081.70 vs prior close $9,967.91 (+113.79) · 5 name(s) re-marked at the open (per-name table). BHP×21 yday $93.63 → 09:30 $95.72 +43.89; APA×44 yday $44.39 → 09:30 $44.52 +5.72; AUTL×809 yday $2.46 → 09:30 $2.47 +8.09; CRSP×34 yday $58.12 → 09:30 $59.72 +54.40; MRK×13 yday $148.99 → 09:30 $149.12 +1.69 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 21 | $95.72 | $2.08 | $+94.78 | $2,153.47 | ▲ +94.78 after sell → book $10,079.62; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 44 | $44.52 | $2.15 | $-14.83 | $4,110.20 | ▼ -14.83 after sell → book $10,077.47; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRK` | 13 | $149.12 | $2.05 | $-25.66 | $6,046.71 | ▼ -25.66 after sell → book $10,075.42; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 8 | $119.43 | $2.01 | — | $5,089.25 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1007.78 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 8 | $115.18 | $2.01 | — | $4,165.80 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1007.78 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 12 | $78.88 | $2.03 | — | $3,217.21 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1007.78 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `VIRT` | 16 | $60.66 | $2.04 | — | $2,244.61 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list oppset; 🔵; ⚪; ret5=+7.0; leftover $1007.78 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MFC` | 23 | $42.48 | $2.06 | — | $1,265.52 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list oppset; 🔵; ⚪; ret5=-3.3; leftover $1007.78 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 116 | $8.66 | $2.34 | — | $258.62 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1007.78 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $258.62 | ▲ close $10,129.16 vs 09:30 $10,081.70 (session +66.23) | 16:00 close · cash $258.62 · equity $10,129.16 vs 09:30 $10,081.70 (+47.46; session marks +66.23) · 8 name(s) marked open→close (per-name table). AUTL×809 09:30 $2.47 → close $2.41 -48.54; CRSP×34 09:30 $59.72 → close $59.50 -7.48; AU×8 09:30 $119.43 → close $121.22 +14.32; FUTU×8 09:30 $115.18 → close $123.64 +67.68; GRAL×12 09:30 $78.88 → close $79.54 +7.92; VIRT×16 09:30 $60.66 → close $67.93 +116.32; MFC×23 09:30 $42.48 → close $42.51 +0.69; ABTC×116 09:30 $8.66 → close $7.93 -84.68 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $258.62 | ▼ 09:30 equity $10,082.17 vs yday $10,129.16 (-46.99) | 09:30 open · cash $258.62 (unchanged overnight, no fees) · equity $10,082.17 vs prior close $10,129.16 (-46.99) · 8 name(s) re-marked at the open (per-name table). AUTL×809 yday $2.41 → 09:30 $2.40 -8.09; CRSP×34 yday $59.50 → 09:30 $58.75 -25.50; AU×8 yday $121.22 → 09:30 $120.51 -5.68; FUTU×8 yday $123.64 → 09:30 $121.00 -21.12; GRAL×12 yday $79.54 → 09:30 $81.87 +27.96; VIRT×16 yday $67.93 → 09:30 $66.80 -18.08; MFC×23 yday $42.51 → 09:30 $42.31 -4.60; ABTC×116 yday $7.93 → 09:30 $8.00 +8.12 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 809 | $2.40 | $10.59 | $-77.65 | $2,189.63 | ▼ -77.65 after sell → book $10,071.58; vs 09:30 mark -10.59 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 8 | $120.51 | $2.03 | $+4.59 | $3,151.68 | ▲ +4.59 after sell → book $10,069.55; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 8 | $121.00 | $2.03 | $+42.51 | $4,117.64 | ▲ +42.51 after sell → book $10,067.51; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 12 | $81.87 | $2.05 | $+31.81 | $5,098.04 | ▲ +31.81 after sell → book $10,065.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `VIRT` | 16 | $66.80 | $2.06 | $+94.14 | $6,164.78 | ▲ +94.14 after sell → book $10,063.41; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MFC` | 23 | $42.31 | $2.08 | $-8.05 | $7,135.83 | ▼ -8.05 after sell → book $10,061.33; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 116 | $8.00 | $2.37 | $-81.27 | $8,061.46 | ▼ -81.27 after sell → book $10,058.96; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,061.46 | ▼ close $10,002.01 vs 09:30 $10,082.17 (session -56.95) | 16:00 close · cash $8,061.46 · equity $10,002.01 vs 09:30 $10,082.17 (-80.16; session marks -56.95) · 1 name(s) marked open→close (per-name table). CRSP×34 09:30 $58.75 → close $57.08 -56.95 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,061.46 | ▲ 09:30 equity $10,031.08 vs yday $10,002.01 (+29.07) | 09:30 open · cash $8,061.46 (unchanged overnight, no fees) · equity $10,031.08 vs prior close $10,002.01 (+29.07) · 1 name(s) re-marked at the open (per-name table). CRSP×34 yday $57.08 → 09:30 $57.93 +29.07 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 34 | $57.93 | $2.12 | $-31.41 | $10,028.97 | ▼ -31.41 after sell → book $10,028.97; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 42 | $118.52 | $2.12 | — | $5,049.01 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $5014.48 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 65 | $77.13 | $2.19 | — | $33.37 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $5014.48 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.37 | ▲ close $10,409.90 vs 09:30 $10,031.08 (session +385.24) | 16:00 close · cash $33.37 · equity $10,409.90 vs 09:30 $10,031.08 (+378.82; session marks +385.24) · 2 name(s) marked open→close (per-name table). AU×42 09:30 $118.52 → close $123.39 +204.54; FCX×65 09:30 $77.13 → close $79.91 +180.70 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.37 | ▼ 09:30 equity $10,222.07 vs yday $10,409.90 (-187.83) | 09:30 open · cash $33.37 (unchanged overnight, no fees) · equity $10,222.07 vs prior close $10,409.90 (-187.83) · 2 name(s) re-marked at the open (per-name table). AU×42 yday $123.39 → 09:30 $119.80 -150.78; FCX×65 yday $79.91 → 09:30 $79.34 -37.05 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 42 | $119.80 | $2.17 | $+49.48 | $5,062.81 | ▲ +49.48 after sell → book $10,219.91; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 65 | $79.34 | $2.24 | $+139.23 | $10,217.67 | ▲ +139.23 after sell → book $10,217.67; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 246 | $20.72 | $3.17 | — | $5,117.38 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=+67.1; leftover $5108.84 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AMX` | 215 | $23.75 | $2.77 | — | $8.35 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=+0.5; leftover $5108.84 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.35 | ▲ close $10,375.65 vs 09:30 $10,222.07 (session +163.93) | 16:00 close · cash $8.35 · equity $10,375.65 vs 09:30 $10,222.07 (+153.58; session marks +163.93) · 2 name(s) marked open→close (per-name table). ASST×246 09:30 $20.72 → close $21.50 +191.88; AMX×215 09:30 $23.75 → close $23.62 -27.95 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.35 | ▲ 09:30 equity $10,641.60 vs yday $10,375.65 (+265.95) | 09:30 open · cash $8.35 (unchanged overnight, no fees) · equity $10,641.60 vs prior close $10,375.65 (+265.95) · 2 name(s) re-marked at the open (per-name table). ASST×246 yday $21.50 → 09:30 $22.45 +233.70; AMX×215 yday $23.62 → 09:30 $23.77 +32.25 | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 246 | $22.45 | $3.26 | $+419.15 | $5,527.80 | ▲ +419.15 after sell → book $10,638.35; vs 09:30 mark -3.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AMX` | 215 | $23.77 | $2.85 | $-1.32 | $10,635.50 | ▼ -1.32 after sell → book $10,635.50; vs 09:30 mark -2.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 18 | $81.65 | $2.04 | — | $9,163.75 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1519.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $8,194.75 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1519.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 12 | $118.77 | $2.03 | — | $6,767.48 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list earn_react; ret5=+0.3; leftover $1519.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 50 | $29.83 | $2.14 | — | $5,273.84 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $1519.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 4 | $318.88 | $2.00 | — | $3,996.32 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1519.36 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 6 | $222.86 | $2.01 | — | $2,657.15 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $1519.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,657.15 | ▼ close $10,586.64 vs 09:30 $10,641.60 (session -36.64) | 16:00 close · cash $2,657.15 · equity $10,586.64 vs 09:30 $10,641.60 (-54.96; session marks -36.64) · 6 name(s) marked open→close (per-name table). ACMR×18 09:30 $81.65 → close $80.49 -20.88; MU×1 09:30 $967.01 → close $935.39 -31.62; CM×12 09:30 $118.77 → close $114.84 -47.16; GEN×50 09:30 $29.83 → close $30.50 +33.50; LRCX×4 09:30 $318.88 → close $318.58 -1.20; NVDA×6 09:30 $222.86 → close $227.98 +30.72 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,657.15 | ▼ 09:30 equity $10,552.50 vs yday $10,586.64 (-34.14) | 09:30 open · cash $2,657.15 (unchanged overnight, no fees) · equity $10,552.50 vs prior close $10,586.64 (-34.14) · 6 name(s) re-marked at the open (per-name table). ACMR×18 yday $80.49 → 09:30 $79.27 -21.96; MU×1 yday $935.39 → 09:30 $919.29 -16.10; CM×12 yday $114.84 → 09:30 $115.66 +9.84; GEN×50 yday $30.50 → 09:30 $30.50 +0.00; LRCX×4 yday $318.58 → 09:30 $318.03 -2.20; NVDA×6 yday $227.98 → 09:30 $227.36 -3.72 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 18 | $79.27 | $2.07 | $-46.95 | $4,081.95 | ▼ -46.95 after sell → book $10,550.44; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $4,999.22 | ▼ -51.73 after sell → book $10,548.42; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 12 | $115.66 | $2.05 | $-41.39 | $6,385.10 | ▼ -41.39 after sell → book $10,546.38; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 50 | $30.50 | $2.16 | $+29.20 | $7,907.94 | ▲ +29.20 after sell → book $10,544.22; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 4 | $318.03 | $2.02 | $-7.42 | $9,178.03 | ▼ -7.42 after sell → book $10,542.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 6 | $227.36 | $2.03 | $+22.96 | $10,540.16 | ▲ +22.96 after sell → book $10,540.16; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,240.52 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1505.74 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 10 | $141.76 | $2.02 | — | $7,820.90 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1505.74 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,617.64 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1505.74 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $5,309.62 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1505.74 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 6 | $240.22 | $2.01 | — | $3,866.29 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1505.74 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 50 | $30.01 | $2.14 | — | $2,363.65 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=-0.9; leftover $1505.74 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 45 | $32.90 | $2.12 | — | $881.03 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1505.74 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $881.03 | ▼ close $10,086.02 vs 09:30 $10,552.50 (session -439.86) | 16:00 close · cash $881.03 · equity $10,086.02 vs 09:30 $10,552.50 (-466.48; session marks -439.86) · 7 name(s) marked open→close (per-name table). KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×10 09:30 $141.76 → close $131.17 -105.90; CIEN×3 09:30 $400.42 → close $378.44 -65.94; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×6 09:30 $240.22 → close $236.98 -19.44; PLAB×50 09:30 $30.01 → close $27.73 -114.00; SEDG×45 09:30 $32.90 → close $31.41 -67.05 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $881.03 | ▲ 09:30 equity $10,098.75 vs yday $10,086.02 (+12.73) | 09:30 open · cash $881.03 (unchanged overnight, no fees) · equity $10,098.75 vs prior close $10,086.02 (+12.73) · 7 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×10 yday $131.17 → 09:30 $132.30 +11.30; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×6 yday $236.98 → 09:30 $233.97 -18.09; PLAB×50 yday $27.73 → 09:30 $28.04 +15.50; SEDG×45 yday $31.41 → 09:30 $31.15 -11.70 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $2,168.96 | ▼ -11.70 after sell → book $10,096.72; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 10 | $132.30 | $2.04 | $-98.66 | $3,489.92 | ▼ -98.66 after sell → book $10,094.68; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $4,623.23 | ▼ -69.96 after sell → book $10,092.67; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $5,883.11 | ▼ -48.14 after sell → book $10,090.65; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 6 | $233.97 | $2.03 | $-41.57 | $7,284.87 | ▼ -41.57 after sell → book $10,088.62; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 50 | $28.04 | $2.16 | $-102.80 | $8,684.71 | ▼ -102.80 after sell → book $10,086.46; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 45 | $31.15 | $2.15 | $-83.02 | $10,084.32 | ▼ -83.02 after sell → book $10,084.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,084.32 | ▲ close $10,084.32 vs 09:30 $10,098.75 (session +0.00) | 16:00 close · cash $10,084.32 · no lots left · equity $10,084.32. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,084.32 | ▲ 09:30 equity $10,084.32 vs yday $10,084.32 (-0.00) | 09:30 open · cash $10,084.32 · no holdings · equity $10,084.32 vs prior close $10,084.32 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,084.32 | ▲ close $10,084.32 vs 09:30 $10,084.32 (session +0.00) | 16:00 close · cash $10,084.32 · no lots left · equity $10,084.32. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,084.32 | ▲ 09:30 equity $10,084.32 vs yday $10,084.32 (-0.00) | 09:30 open · cash $10,084.32 · no holdings · equity $10,084.32 vs prior close $10,084.32 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,084.32 | ▲ close $10,084.32 vs 09:30 $10,084.32 (session +0.00) | 16:00 close · cash $10,084.32 · no lots left · equity $10,084.32. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,084.32 | ▲ 09:30 equity $10,084.32 vs yday $10,084.32 (-0.00) | 09:30 open · cash $10,084.32 · no holdings · equity $10,084.32 vs prior close $10,084.32 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 5 | $351.74 | $2.00 | — | $8,323.61 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $2016.86 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 4 | $486.31 | $2.00 | — | $6,376.37 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $2016.86 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 62 | $32.31 | $2.18 | — | $4,370.97 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $2016.86 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 127 | $15.87 | $2.37 | — | $2,353.11 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $2016.86 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 84 | $23.88 | $2.24 | — | $344.95 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+21.9; leftover $2016.86 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $344.95 | ▲ close $10,432.09 vs 09:30 $10,084.32 (session +358.57) | 16:00 close · cash $344.95 · equity $10,432.09 vs 09:30 $10,084.32 (+347.77; session marks +358.57) · 5 name(s) marked open→close (per-name table). AVGO×5 09:30 $351.74 → close $357.16 +27.10; DELL×4 09:30 $486.31 → close $516.39 +120.32; CXW×62 09:30 $32.31 → close $33.66 +83.70; FRNM×127 09:30 $15.87 → close $16.90 +130.81; MMED×84 09:30 $23.88 → close $23.84 -3.36 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $344.95 | ▼ 09:30 equity $10,358.45 vs yday $10,432.09 (-73.64) | 09:30 open · cash $344.95 (unchanged overnight, no fees) · equity $10,358.45 vs prior close $10,432.09 (-73.64) · 5 name(s) re-marked at the open (per-name table). AVGO×5 yday $357.16 → 09:30 $359.70 +12.70; DELL×4 yday $516.39 → 09:30 $513.78 -10.44; CXW×62 yday $33.66 → 09:30 $33.46 -12.40; FRNM×127 yday $16.90 → 09:30 $16.40 -63.50; MMED×84 yday $23.84 → 09:30 $23.84 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 5 | $359.70 | $2.03 | $+35.77 | $2,141.42 | ▲ +35.77 after sell → book $10,356.42; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 4 | $513.78 | $2.03 | $+105.85 | $4,194.51 | ▲ +105.85 after sell → book $10,354.39; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 62 | $33.46 | $2.20 | $+66.92 | $6,266.83 | ▲ +66.92 after sell → book $10,352.19; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 7 | $263.36 | $2.01 | — | $4,421.30 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2088.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `HPE` | 38 | $53.85 | $2.10 | — | $2,372.89 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=+0.1; leftover $2088.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟡 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 27 | $75.65 | $2.07 | — | $328.27 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $2088.94 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $328.27 | ▼ close $10,259.90 vs 09:30 $10,358.45 (session -86.10) | 16:00 close · cash $328.27 · equity $10,259.90 vs 09:30 $10,358.45 (-98.55; session marks -86.10) · 5 name(s) marked open→close (per-name table). FRNM×127 09:30 $16.40 → close $16.31 -11.43; MMED×84 09:30 $23.84 → close $23.29 -46.20; CRM×7 09:30 $263.36 → close $259.23 -28.91; HPE×38 09:30 $53.85 → close $52.00 -70.30; MRX×27 09:30 $75.65 → close $78.27 +70.74 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $328.27 | ▲ 09:30 equity $10,291.43 vs yday $10,259.90 (+31.53) | 09:30 open · cash $328.27 (unchanged overnight, no fees) · equity $10,291.43 vs prior close $10,259.90 (+31.53) · 5 name(s) re-marked at the open (per-name table). FRNM×127 yday $16.31 → 09:30 $16.74 +54.61; MMED×84 yday $23.29 → 09:30 $23.16 -10.92; CRM×7 yday $259.23 → 09:30 $253.72 -38.57; HPE×38 yday $52.00 → 09:30 $52.29 +11.02; MRX×27 yday $78.27 → 09:30 $78.84 +15.39 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 127 | $16.74 | $2.41 | $+105.71 | $2,451.84 | ▲ +105.71 after sell → book $10,289.02; vs 09:30 mark -2.41 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 84 | $23.16 | $2.27 | $-64.99 | $4,395.01 | ▼ -64.99 after sell → book $10,286.75; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 7 | $253.72 | $2.04 | $-71.53 | $6,169.02 | ▼ -71.53 after sell → book $10,284.72; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HPE` | 38 | $52.29 | $2.13 | $-63.51 | $8,153.91 | ▼ -63.51 after sell → book $10,282.59; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 27 | $78.84 | $2.10 | $+81.96 | $10,280.49 | ▲ +81.96 after sell → book $10,280.49; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,280.49 | ▲ close $10,280.49 vs 09:30 $10,291.43 (session +0.00) | 16:00 close · cash $10,280.49 · no lots left · equity $10,280.49. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,280.49 | ▲ 09:30 equity $10,280.49 vs yday $10,280.49 (-0.00) | 09:30 open · cash $10,280.49 · no holdings · equity $10,280.49 vs prior close $10,280.49 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,280.49 | ▲ close $10,280.49 vs 09:30 $10,280.49 (session +0.00) | 16:00 close · cash $10,280.49 · no lots left · equity $10,280.49. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,280.49 | ▲ 09:30 equity $10,280.49 vs yday $10,280.49 (-0.00) | 09:30 open · cash $10,280.49 · no holdings · equity $10,280.49 vs prior close $10,280.49 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,280.49 | ▲ close $10,280.49 vs 09:30 $10,280.49 (session +0.00) | 16:00 close · cash $10,280.49 · no lots left · equity $10,280.49. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,280.49 | ▲ 09:30 equity $10,280.49 vs yday $10,280.49 (-0.00) | 09:30 open · cash $10,280.49 · no holdings · equity $10,280.49 vs prior close $10,280.49 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 31 | $164.43 | $2.08 | — | $5,181.08 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $5140.24 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BTI` | 91 | $56.03 | $2.26 | — | $80.08 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=-0.8; leftover $5140.24 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.08 | ▼ close $9,765.60 vs 09:30 $10,280.49 (session -510.54) | 16:00 close · cash $80.08 · equity $9,765.60 vs 09:30 $10,280.49 (-514.89; session marks -510.54) · 2 name(s) marked open→close (per-name table). ORCL×31 09:30 $164.43 → close $150.28 -438.65; BTI×91 09:30 $56.03 → close $55.24 -71.89 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.08 | ▼ 09:30 equity $9,662.02 vs yday $9,765.60 (-103.58) | 09:30 open · cash $80.08 (unchanged overnight, no fees) · equity $9,662.02 vs prior close $9,765.60 (-103.58) · 2 name(s) re-marked at the open (per-name table). ORCL×31 yday $150.28 → 09:30 $141.42 -274.66; BTI×91 yday $55.24 → 09:30 $57.12 +171.08 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 31 | $141.42 | $2.13 | $-717.52 | $4,461.98 | ▼ -717.52 after sell → book $9,659.90; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `BTI` | 91 | $57.12 | $2.32 | $+94.61 | $9,657.58 | ▲ +94.61 after sell → book $9,657.58; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,657.58 | ▲ close $9,657.58 vs 09:30 $9,662.02 (session +0.00) | 16:00 close · cash $9,657.58 · no lots left · equity $9,657.58. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,657.58 | ▲ 09:30 equity $9,657.58 vs yday $9,657.58 (-0.00) | 09:30 open · cash $9,657.58 · no holdings · equity $9,657.58 vs prior close $9,657.58 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,657.58 | ▲ close $9,657.58 vs 09:30 $9,657.58 (session +0.00) | 16:00 close · cash $9,657.58 · no lots left · equity $9,657.58. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,657.58 | ▲ 09:30 equity $9,657.58 vs yday $9,657.58 (-0.00) | 09:30 open · cash $9,657.58 · no holdings · equity $9,657.58 vs prior close $9,657.58 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 122 | $26.27 | $2.36 | — | $6,450.28 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $3219.19 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 17 | $189.17 | $2.04 | — | $3,232.35 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $3219.19 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 80 | $39.99 | $2.23 | — | $30.92 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $3219.19 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.92 | ▼ close $9,469.98 vs 09:30 $9,657.58 (session -180.97) | 16:00 close · cash $30.92 · equity $9,469.98 vs 09:30 $9,657.58 (-187.60; session marks -180.97) · 3 name(s) marked open→close (per-name table). WAY×122 09:30 $26.27 → close $26.59 +39.04; QCOM×17 09:30 $189.17 → close $184.84 -73.61; SM×80 09:30 $39.99 → close $38.16 -146.40 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.92 | ▲ 09:30 equity $9,506.69 vs yday $9,469.98 (+36.71) | 09:30 open · cash $30.92 (unchanged overnight, no fees) · equity $9,506.69 vs prior close $9,469.98 (+36.71) · 3 name(s) re-marked at the open (per-name table). WAY×122 yday $26.59 → 09:30 $26.51 -9.76; QCOM×17 yday $184.84 → 09:30 $190.35 +93.67; SM×80 yday $38.16 → 09:30 $37.57 -47.20 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 122 | $26.51 | $2.40 | $+24.52 | $3,262.74 | ▲ +24.52 after sell → book $9,504.29; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 17 | $190.35 | $2.08 | $+15.94 | $6,496.61 | ▲ +15.94 after sell → book $9,502.21; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 80 | $37.57 | $2.27 | $-198.10 | $9,499.94 | ▼ -198.10 after sell → book $9,499.94; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 18 | $170.85 | $2.04 | — | $6,422.60 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $3166.65 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 200 | $15.81 | $2.59 | — | $3,258.01 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $3166.65 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 143 | $22.12 | $2.42 | — | $92.43 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $3166.65 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.43 | ▲ close $9,727.96 vs 09:30 $9,506.69 (session +235.07) | 16:00 close · cash $92.43 · equity $9,727.96 vs 09:30 $9,506.69 (+221.27; session marks +235.07) · 3 name(s) marked open→close (per-name table). SMTC×18 09:30 $170.85 → close $178.19 +132.12; AVTR×200 09:30 $15.81 → close $15.86 +10.00; GME×143 09:30 $22.12 → close $22.77 +92.95 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.43 | ▲ 09:30 equity $9,823.07 vs yday $9,727.96 (+95.11) | 09:30 open · cash $92.43 (unchanged overnight, no fees) · equity $9,823.07 vs prior close $9,727.96 (+95.11) · 3 name(s) re-marked at the open (per-name table). SMTC×18 yday $178.19 → 09:30 $182.33 +74.52; AVTR×200 yday $15.86 → 09:30 $15.87 +2.00; GME×143 yday $22.77 → 09:30 $22.90 +18.59 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 18 | $182.33 | $2.08 | $+202.52 | $3,372.29 | ▲ +202.52 after sell → book $9,820.99; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 200 | $15.87 | $2.65 | $+6.76 | $6,543.64 | ▲ +6.76 after sell → book $9,818.34; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 104 | $20.91 | $2.30 | — | $4,366.70 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2181.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 147 | $14.79 | $2.43 | — | $2,190.14 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $2181.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 155 | $14.07 | $2.46 | — | $6.83 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2181.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.83 | ▼ close $9,692.18 vs 09:30 $9,823.07 (session -118.97) | 16:00 close · cash $6.83 · equity $9,692.18 vs 09:30 $9,823.07 (-130.89; session marks -118.97) · 4 name(s) marked open→close (per-name table). GME×143 09:30 $22.90 → close $22.64 -37.18; TH×104 09:30 $20.91 → close $21.19 +29.12; RARE×147 09:30 $14.79 → close $14.51 -41.16; BHVN×155 09:30 $14.07 → close $13.62 -69.75 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1519.36 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GME` | 143 | 2026-09-17 @ $22.12 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $3166.65 |
| `TH` | 104 | 2026-09-18 @ $20.91 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2181.21 |
| `RARE` | 147 | 2026-09-18 @ $14.79 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $2181.21 |
| `BHVN` | 155 | 2026-09-18 @ $14.07 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2181.21 |
