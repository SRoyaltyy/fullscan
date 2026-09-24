# Factor mine action — `union_news_g_cam71_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢 and cameras +7 −≤1

Cash book **-9.11%** ($9,089) · signal-only (no cash/fees) was -11.63%. Starts YES **4/30**. Fills 66 · skips 12 · realized $-910.91.

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
- Must-have: at least 7 green cameras (the +G half of +G −R).
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
- **Gate** `news=good,n_pos_min=7,cam_bad_max=1` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,089.10.

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
| 2026-08-20 | `BHP` | 54 | — | $91.01 | +0.00 | $93.63 | +141.48 | +141.48 | +0.00 | +141.48 |
| 2026-08-20 | `APA` | 111 | — | $44.76 | +0.00 | $44.39 | -41.07 | -41.07 | +0.00 | -41.07 |
| 2026-08-21 | `BHP` | 54 | $93.63 | $95.72 | +112.86 | — | +0.00 | +112.86 | +254.34 | — |
| 2026-08-21 | `APA` | 111 | $44.39 | $44.52 | +14.43 | — | +0.00 | +14.43 | -26.64 | — |
| 2026-08-21 | `AU` | 17 | — | $119.43 | +0.00 | $121.22 | +30.43 | +30.43 | +0.00 | +30.43 |
| 2026-08-21 | `AUTL` | 827 | — | $2.47 | +0.00 | $2.41 | -49.62 | -49.62 | +0.00 | -49.62 |
| 2026-08-21 | `CRSP` | 34 | — | $59.72 | +0.00 | $59.50 | -7.48 | -7.48 | +0.00 | -7.48 |
| 2026-08-21 | `FUTU` | 17 | — | $115.18 | +0.00 | $123.64 | +143.82 | +143.82 | +0.00 | +143.82 |
| 2026-08-21 | `GRAL` | 25 | — | $78.88 | +0.00 | $79.54 | +16.50 | +16.50 | +0.00 | +16.50 |
| 2026-08-24 | `AU` | 17 | $121.22 | $120.51 | -12.07 | — | +0.00 | -12.07 | +18.36 | — |
| 2026-08-24 | `AUTL` | 827 | $2.41 | $2.40 | -8.27 | — | +0.00 | -8.27 | -57.89 | — |
| 2026-08-24 | `CRSP` | 34 | $59.50 | $58.75 | -25.50 | — | +0.00 | -25.50 | -32.98 | — |
| 2026-08-24 | `FUTU` | 17 | $123.64 | $121.00 | -44.88 | — | +0.00 | -44.88 | +98.94 | — |
| 2026-08-24 | `GRAL` | 25 | $79.54 | $81.87 | +58.25 | — | +0.00 | +58.25 | +74.75 | — |
| 2026-08-25 | `AU` | 43 | — | $118.52 | +0.00 | $123.39 | +209.41 | +209.41 | +0.00 | +209.41 |
| 2026-08-25 | `FCX` | 66 | — | $77.13 | +0.00 | $79.91 | +183.48 | +183.48 | +0.00 | +183.48 |
| 2026-08-26 | `AU` | 43 | $123.39 | $119.80 | -154.37 | — | +0.00 | -154.37 | +55.04 | — |
| 2026-08-26 | `FCX` | 66 | $79.91 | $79.34 | -37.62 | — | +0.00 | -37.62 | +145.86 | — |
| 2026-08-26 | `CM` | 88 | — | $118.50 | +0.00 | $118.20 | -26.40 | -26.40 | +0.00 | -26.40 |
| 2026-08-27 | `CM` | 88 | $118.20 | $118.77 | +50.16 | $114.84 | -345.84 | -295.68 | +23.76 | -322.08 |
| 2026-08-28 | `CM` | 88 | $114.84 | $115.66 | +72.16 | — | +0.00 | +72.16 | -249.92 | — |
| 2026-08-28 | `KEYS` | 6 | — | $324.41 | +0.00 | $319.97 | -26.64 | -26.64 | +0.00 | -26.64 |
| 2026-08-28 | `SMTC` | 14 | — | $141.76 | +0.00 | $131.17 | -148.26 | -148.26 | +0.00 | -148.26 |
| 2026-08-28 | `CIEN` | 5 | — | $400.42 | +0.00 | $378.44 | -109.90 | -109.90 | +0.00 | -109.90 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 8 | — | $240.22 | +0.00 | $236.98 | -25.92 | -25.92 | +0.00 | -25.92 |
| 2026-08-31 | `KEYS` | 6 | $319.97 | $322.49 | +15.12 | — | +0.00 | +15.12 | -11.52 | — |
| 2026-08-31 | `SMTC` | 14 | $131.17 | $132.30 | +15.82 | — | +0.00 | +15.82 | -132.44 | — |
| 2026-08-31 | `CIEN` | 5 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -109.90 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `DDOG` | 8 | $236.98 | $233.97 | -24.12 | — | +0.00 | -24.12 | -50.04 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 14 | — | $351.74 | +0.00 | $357.16 | +75.88 | +75.88 | +0.00 | +75.88 |
| 2026-09-03 | `DELL` | 10 | — | $486.31 | +0.00 | $516.39 | +300.80 | +300.80 | +0.00 | +300.80 |
| 2026-09-04 | `AVGO` | 14 | $357.16 | $359.70 | +35.56 | — | +0.00 | +35.56 | +111.44 | — |
| 2026-09-04 | `DELL` | 10 | $516.39 | $513.78 | -26.10 | — | +0.00 | -26.10 | +274.70 | — |
| 2026-09-04 | `CRM` | 12 | — | $263.36 | +0.00 | $259.23 | -49.56 | -49.56 | +0.00 | -49.56 |
| 2026-09-04 | `FRNM` | 207 | — | $16.40 | +0.00 | $16.31 | -18.63 | -18.63 | +0.00 | -18.63 |
| 2026-09-04 | `MRX` | 45 | — | $75.65 | +0.00 | $78.27 | +117.90 | +117.90 | +0.00 | +117.90 |
| 2026-09-08 | `CRM` | 12 | $259.23 | $253.72 | -66.12 | — | +0.00 | -66.12 | -115.68 | — |
| 2026-09-08 | `FRNM` | 207 | $16.31 | $16.74 | +89.01 | — | +0.00 | +89.01 | +70.38 | — |
| 2026-09-08 | `MRX` | 45 | $78.27 | $78.84 | +25.65 | — | +0.00 | +25.65 | +143.55 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 62 | — | $164.43 | +0.00 | $150.28 | -877.30 | -877.30 | +0.00 | -877.30 |
| 2026-09-14 | `ORCL` | 62 | $150.28 | $141.42 | -549.32 | — | +0.00 | -549.32 | -1426.62 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 112 | — | $26.27 | +0.00 | $26.59 | +35.84 | +35.84 | +0.00 | +35.84 |
| 2026-09-16 | `QCOM` | 15 | — | $189.17 | +0.00 | $184.84 | -64.95 | -64.95 | +0.00 | -64.95 |
| 2026-09-16 | `SM` | 74 | — | $39.99 | +0.00 | $38.16 | -135.42 | -135.42 | +0.00 | -135.42 |
| 2026-09-17 | `WAY` | 112 | $26.59 | $26.51 | -8.96 | — | +0.00 | -8.96 | +26.88 | — |
| 2026-09-17 | `QCOM` | 15 | $184.84 | $190.35 | +82.65 | — | +0.00 | +82.65 | +17.70 | — |
| 2026-09-17 | `SM` | 74 | $38.16 | $37.57 | -43.66 | — | +0.00 | -43.66 | -179.08 | — |
| 2026-09-17 | `SMTC` | 51 | — | $170.85 | +0.00 | $178.19 | +374.34 | +374.34 | +0.00 | +374.34 |
| 2026-09-18 | `SMTC` | 51 | $178.19 | $182.33 | +211.14 | — | +0.00 | +211.14 | +585.48 | — |
| 2026-09-18 | `TH` | 148 | — | $20.91 | +0.00 | $21.19 | +41.44 | +41.44 | +0.00 | +41.44 |
| 2026-09-18 | `GME` | 135 | — | $22.90 | +0.00 | $22.64 | -35.10 | -35.10 | +0.00 | -35.10 |
| 2026-09-18 | `RARE` | 209 | — | $14.79 | +0.00 | $14.51 | -58.52 | -58.52 | +0.00 | -58.52 |
| 2026-09-21 | `TH` | 148 | $21.19 | $21.65 | +68.08 | — | +0.00 | +68.08 | +109.52 | — |
| 2026-09-21 | `GME` | 135 | $22.64 | $22.78 | +18.90 | — | +0.00 | +18.90 | -16.20 | — |
| 2026-09-21 | `RARE` | 209 | $14.51 | $14.58 | +14.63 | — | +0.00 | +14.63 | -43.89 | — |
| 2026-09-21 | `VICR` | 20 | — | $230.25 | +0.00 | $223.90 | -127.00 | -127.00 | +0.00 | -127.00 |
| 2026-09-21 | `SMTC` | 24 | — | $190.30 | +0.00 | $177.37 | -310.32 | -310.32 | +0.00 | -310.32 |
| 2026-09-22 | `VICR` | 20 | $223.90 | $223.90 | +0.00 | $223.90 | +0.00 | +0.00 | -127.00 | -127.00 |
| 2026-09-22 | `SMTC` | 24 | $177.37 | $177.37 | +0.00 | $177.37 | +0.00 | +0.00 | -310.32 | -310.32 |
| 2026-09-23 | `VICR` | 20 | $223.90 | $266.50 | +852.00 | — | +0.00 | +852.00 | +725.00 | — |
| 2026-09-23 | `SMTC` | 24 | $177.37 | $174.50 | -68.88 | — | +0.00 | -68.88 | -379.20 | — |
| 2026-09-23 | `CTAS` | 16 | — | $196.78 | +0.00 | $191.97 | -76.96 | -76.96 | +0.00 | -76.96 |
| 2026-09-23 | `PGEN` | 406 | — | $7.95 | +0.00 | $7.44 | -207.06 | -207.06 | +0.00 | -207.06 |
| 2026-09-23 | `SGRY` | 205 | — | $15.72 | +0.00 | $14.56 | -237.80 | -237.80 | +0.00 | -237.80 |
| 2026-09-24 | `CTAS` | 16 | $191.97 | $192.26 | +4.64 | — | +0.00 | +4.64 | -72.32 | — |
| 2026-09-24 | `PGEN` | 406 | $7.44 | $7.38 | -24.36 | — | +0.00 | -24.36 | -231.42 | — |
| 2026-09-24 | `SGRY` | 205 | $14.56 | $14.38 | -36.90 | — | +0.00 | -36.90 | -274.70 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +100.41 | BHP, APA | — | $112.62 | $10,095.93 | BHP×54, APA×111 |
| 2026-08-21 | +3.25 | $112.62 | BHP×54, APA×111 | $10,223.23 | +127.30 | +133.65 | AU, AUTL, CRSP, FUTU, GRAL | BHP, APA | $166.19 | $10,333.38 | AU×17, AUTL×827, CRSP×34, FUTU×17, GRAL×25 |
| 2026-08-24 | -5.17 | $166.19 | AU×17, AUTL×827, CRSP×34, FUTU×17, GRAL×25 | $10,300.91 | -32.47 | +0.00 | — | AU, AUTL, CRSP, FUTU, GRAL | $10,281.75 | $10,281.75 | — |
| 2026-08-25 | +1.80 | $10,281.75 | — | $10,281.75 | -0.00 | +392.89 | AU, FCX | — | $90.50 | $10,670.33 | AU×43, FCX×66 |
| 2026-08-26 | +2.02 | $90.50 | AU×43, FCX×66 | $10,478.34 | -191.99 | -26.40 | CM | AU, FCX | $43.68 | $10,445.28 | CM×88 |
| 2026-08-27 | — | $43.68 | CM×88 | $10,495.44 | +50.16 | -345.84 | — | — | $43.68 | $10,149.60 | CM×88 |
| 2026-08-28 | +0.75 | $43.68 | CM×88 | $10,221.76 | +72.16 | -360.49 | KEYS, SMTC, CIEN, MPWR, DDOG | CM | $1,048.36 | $9,848.86 | KEYS×6, SMTC×14, CIEN×5, MPWR×1, DDOG×8 |
| 2026-08-31 | -5.85 | $1,048.36 | KEYS×6, SMTC×14, CIEN×5, MPWR×1, DDOG×8 | $9,861.32 | +12.46 | +0.00 | — | KEYS, SMTC, CIEN, MPWR, DDOG | $9,851.15 | $9,851.15 | — |
| 2026-09-01 | -6.30 | $9,851.15 | — | $9,851.15 | +0.00 | +0.00 | — | — | $9,851.15 | $9,851.15 | — |
| 2026-09-02 | -3.83 | $9,851.15 | — | $9,851.15 | +0.00 | +0.00 | — | — | $9,851.15 | $9,851.15 | — |
| 2026-09-03 | -0.90 | $9,851.15 | — | $9,851.15 | +0.00 | +376.68 | AVGO, DELL | — | $59.64 | $10,223.78 | AVGO×14, DELL×10 |
| 2026-09-04 | +2.25 | $59.64 | AVGO×14, DELL×10 | $10,233.24 | +9.46 | +49.71 | CRM, FRNM, MRX | AVGO, DELL | $262.89 | $10,271.97 | CRM×12, FRNM×207, MRX×45 |
| 2026-09-08 | -11.47 | $262.89 | CRM×12, FRNM×207, MRX×45 | $10,320.51 | +48.54 | +0.00 | — | CRM, FRNM, MRX | $10,313.56 | $10,313.56 | — |
| 2026-09-09 | -13.95 | $10,313.56 | — | $10,313.56 | -0.00 | +0.00 | — | — | $10,313.56 | $10,313.56 | — |
| 2026-09-10 | -13.28 | $10,313.56 | — | $10,313.56 | -0.00 | +0.00 | — | — | $10,313.56 | $10,313.56 | — |
| 2026-09-11 | +0.50 | $10,313.56 | — | $10,313.56 | -0.00 | -877.30 | ORCL | — | $116.72 | $9,434.08 | ORCL×62 |
| 2026-09-14 | -11.00 | $116.72 | ORCL×62 | $8,884.76 | -549.32 | +0.00 | — | ORCL | $8,882.51 | $8,882.51 | — |
| 2026-09-15 | -3.84 | $8,882.51 | — | $8,882.51 | -0.00 | +0.00 | — | — | $8,882.51 | $8,882.51 | — |
| 2026-09-16 | +5.30 | $8,882.51 | — | $8,882.51 | -0.00 | -164.53 | WAY, QCOM, SM | — | $136.88 | $8,711.40 | WAY×112, QCOM×15, SM×74 |
| 2026-09-17 | +7.38 | $136.88 | WAY×112, QCOM×15, SM×74 | $8,741.43 | +30.03 | +374.34 | SMTC | WAY, QCOM, SM | $19.26 | $9,106.95 | SMTC×51 |
| 2026-09-18 | +4.86 | $19.26 | SMTC×51 | $9,318.09 | +211.14 | -52.18 | TH, GME, RARE | SMTC | $31.04 | $9,256.15 | TH×148, GME×135, RARE×209 |
| 2026-09-21 | +12.87 | $31.04 | TH×148, GME×135, RARE×209 | $9,357.76 | +101.61 | -437.32 | VICR, SMTC | TH, GME, RARE | $173.77 | $8,908.65 | VICR×20, SMTC×24 |
| 2026-09-22 | -0.50 | $173.77 | VICR×20, SMTC×24 | $8,908.65 | +0.00 | +0.00 | — | — | $173.77 | $8,908.65 | VICR×20, SMTC×24 |
| 2026-09-23 | +2.29 | $173.77 | VICR×20, SMTC×24 | $9,691.77 | +783.12 | -521.82 | CTAS, PGEN, SGRY | VICR, SMTC | $78.86 | $9,155.82 | CTAS×16, PGEN×406, SGRY×205 |
| 2026-09-24 | -7.66 | $78.86 | CTAS×16, PGEN×406, SGRY×205 | $9,099.20 | -56.62 | +0.00 | — | CTAS, PGEN, SGRY | $9,089.10 | $9,089.10 | — |

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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 54 | $91.01 | $2.15 | — | $5,083.31 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $5000.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 111 | $44.76 | $2.32 | — | $112.62 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $5000.00 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $112.62 | ▲ close $10,095.93 vs 09:30 $10,000.00 (session +100.41) | 16:00 close · cash $112.62 · equity $10,095.93 vs 09:30 $10,000.00 (+95.93; session marks +100.41) · 2 name(s) marked open→close (per-name table). BHP×54 09:30 $91.01 → close $93.63 +141.48; APA×111 09:30 $44.76 → close $44.39 -41.07 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $112.62 | ▲ 09:30 equity $10,223.23 vs yday $10,095.93 (+127.30) | 09:30 open · cash $112.62 (unchanged overnight, no fees) · equity $10,223.23 vs prior close $10,095.93 (+127.30) · 2 name(s) re-marked at the open (per-name table). BHP×54 yday $93.63 → 09:30 $95.72 +112.86; APA×111 yday $44.39 → 09:30 $44.52 +14.43 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 54 | $95.72 | $2.20 | $+249.98 | $5,279.30 | ▲ +249.98 after sell → book $10,221.02; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 111 | $44.52 | $2.38 | $-31.34 | $10,218.64 | ▼ -31.34 after sell → book $10,218.64; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 17 | $119.43 | $2.04 | — | $8,186.29 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $2043.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 827 | $2.47 | $10.67 | — | $6,132.93 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $2043.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 34 | $59.72 | $2.09 | — | $4,100.36 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $2043.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 17 | $115.18 | $2.04 | — | $2,140.26 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $2043.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 25 | $78.88 | $2.06 | — | $166.19 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $2043.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $166.19 | ▲ close $10,333.38 vs 09:30 $10,223.23 (session +133.65) | 16:00 close · cash $166.19 · equity $10,333.38 vs 09:30 $10,223.23 (+110.15; session marks +133.65) · 5 name(s) marked open→close (per-name table). AU×17 09:30 $119.43 → close $121.22 +30.43; AUTL×827 09:30 $2.47 → close $2.41 -49.62; CRSP×34 09:30 $59.72 → close $59.50 -7.48; FUTU×17 09:30 $115.18 → close $123.64 +143.82; GRAL×25 09:30 $78.88 → close $79.54 +16.50 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.19 | ▼ 09:30 equity $10,300.91 vs yday $10,333.38 (-32.47) | 09:30 open · cash $166.19 (unchanged overnight, no fees) · equity $10,300.91 vs prior close $10,333.38 (-32.47) · 5 name(s) re-marked at the open (per-name table). AU×17 yday $121.22 → 09:30 $120.51 -12.07; AUTL×827 yday $2.41 → 09:30 $2.40 -8.27; CRSP×34 yday $59.50 → 09:30 $58.75 -25.50; FUTU×17 yday $123.64 → 09:30 $121.00 -44.88; GRAL×25 yday $79.54 → 09:30 $81.87 +58.25 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 17 | $120.51 | $2.07 | $+14.25 | $2,212.80 | ▲ +14.25 after sell → book $10,298.85; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 827 | $2.40 | $10.82 | $-79.38 | $4,186.77 | ▼ -79.38 after sell → book $10,288.02; vs 09:30 mark -10.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 34 | $58.75 | $2.12 | $-37.19 | $6,182.16 | ▼ -37.19 after sell → book $10,285.91; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 17 | $121.00 | $2.07 | $+94.83 | $8,237.09 | ▲ +94.83 after sell → book $10,283.84; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 25 | $81.87 | $2.09 | $+70.59 | $10,281.75 | ▲ +70.59 after sell → book $10,281.75; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,281.75 | ▲ close $10,281.75 vs 09:30 $10,300.91 (session +0.00) | 16:00 close · cash $10,281.75 · no lots left · equity $10,281.75. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,281.75 | ▲ 09:30 equity $10,281.75 vs yday $10,281.75 (-0.00) | 09:30 open · cash $10,281.75 · no holdings · equity $10,281.75 vs prior close $10,281.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 43 | $118.52 | $2.12 | — | $5,183.27 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $5140.87 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 66 | $77.13 | $2.19 | — | $90.50 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $5140.87 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.50 | ▲ close $10,670.33 vs 09:30 $10,281.75 (session +392.89) | 16:00 close · cash $90.50 · equity $10,670.33 vs 09:30 $10,281.75 (+388.58; session marks +392.89) · 2 name(s) marked open→close (per-name table). AU×43 09:30 $118.52 → close $123.39 +209.41; FCX×66 09:30 $77.13 → close $79.91 +183.48 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.50 | ▼ 09:30 equity $10,478.34 vs yday $10,670.33 (-191.99) | 09:30 open · cash $90.50 (unchanged overnight, no fees) · equity $10,478.34 vs prior close $10,670.33 (-191.99) · 2 name(s) re-marked at the open (per-name table). AU×43 yday $123.39 → 09:30 $119.80 -154.37; FCX×66 yday $79.91 → 09:30 $79.34 -37.62 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 43 | $119.80 | $2.17 | $+50.75 | $5,239.73 | ▲ +50.75 after sell → book $10,476.17; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 66 | $79.34 | $2.24 | $+141.43 | $10,473.93 | ▲ +141.43 after sell → book $10,473.93; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 88 | $118.50 | $2.25 | — | $43.68 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $10473.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.68 | ▼ close $10,445.28 vs 09:30 $10,478.34 (session -26.40) | 16:00 close · cash $43.68 · equity $10,445.28 vs 09:30 $10,478.34 (-33.06; session marks -26.40) · 1 name(s) marked open→close (per-name table). CM×88 09:30 $118.50 → close $118.20 -26.40 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.68 | ▲ 09:30 equity $10,495.44 vs yday $10,445.28 (+50.16) | 09:30 open · cash $43.68 (unchanged overnight, no fees) · equity $10,495.44 vs prior close $10,445.28 (+50.16) · 1 name(s) re-marked at the open (per-name table). CM×88 yday $118.20 → 09:30 $118.77 +50.16 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.68 | ▼ close $10,149.60 vs 09:30 $10,495.44 (session -345.84) | 16:00 close · cash $43.68 · equity $10,149.60 vs 09:30 $10,495.44 (-345.84; session marks -345.84) · 1 name(s) marked open→close (per-name table). CM×88 09:30 $118.77 → close $114.84 -345.84 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.68 | ▲ 09:30 equity $10,221.76 vs yday $10,149.60 (+72.16) | 09:30 open · cash $43.68 (unchanged overnight, no fees) · equity $10,221.76 vs prior close $10,149.60 (+72.16) · 1 name(s) re-marked at the open (per-name table). CM×88 yday $114.84 → 09:30 $115.66 +72.16 | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 88 | $115.66 | $2.35 | $-254.52 | $10,219.41 | ▼ -254.52 after sell → book $10,219.41; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 6 | $324.41 | $2.01 | — | $8,270.94 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $2043.88 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 14 | $141.76 | $2.03 | — | $6,284.27 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $2043.88 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 5 | $400.42 | $2.00 | — | $4,280.16 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2043.88 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $2,972.14 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $2043.88 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 8 | $240.22 | $2.01 | — | $1,048.36 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $2043.88 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,048.36 | ▼ close $9,848.86 vs 09:30 $10,221.76 (session -360.49) | 16:00 close · cash $1,048.36 · equity $9,848.86 vs 09:30 $10,221.76 (-372.90; session marks -360.49) · 5 name(s) marked open→close (per-name table). KEYS×6 09:30 $324.41 → close $319.97 -26.64; SMTC×14 09:30 $141.76 → close $131.17 -148.26; CIEN×5 09:30 $400.42 → close $378.44 -109.90; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×8 09:30 $240.22 → close $236.98 -25.92 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,048.36 | ▲ 09:30 equity $9,861.32 vs yday $9,848.86 (+12.46) | 09:30 open · cash $1,048.36 (unchanged overnight, no fees) · equity $9,861.32 vs prior close $9,848.86 (+12.46) · 5 name(s) re-marked at the open (per-name table). KEYS×6 yday $319.97 → 09:30 $322.49 +15.12; SMTC×14 yday $131.17 → 09:30 $132.30 +15.82; CIEN×5 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×8 yday $236.98 → 09:30 $233.97 -24.12 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 6 | $322.49 | $2.03 | $-15.56 | $2,981.27 | ▼ -15.56 after sell → book $9,859.29; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 14 | $132.30 | $2.06 | $-136.53 | $4,831.41 | ▼ -136.53 after sell → book $9,857.23; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 5 | $378.44 | $2.03 | $-113.94 | $6,721.58 | ▼ -113.94 after sell → book $9,855.20; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $7,981.47 | ▼ -48.14 after sell → book $9,853.19; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 8 | $233.97 | $2.04 | $-54.09 | $9,851.15 | ▼ -54.09 after sell → book $9,851.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,851.15 | ▲ close $9,851.15 vs 09:30 $9,861.32 (session +0.00) | 16:00 close · cash $9,851.15 · no lots left · equity $9,851.15. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,851.15 | ▲ 09:30 equity $9,851.15 vs yday $9,851.15 (+0.00) | 09:30 open · cash $9,851.15 · no holdings · equity $9,851.15 vs prior close $9,851.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,851.15 | ▲ close $9,851.15 vs 09:30 $9,851.15 (session +0.00) | 16:00 close · cash $9,851.15 · no lots left · equity $9,851.15. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,851.15 | ▲ 09:30 equity $9,851.15 vs yday $9,851.15 (+0.00) | 09:30 open · cash $9,851.15 · no holdings · equity $9,851.15 vs prior close $9,851.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,851.15 | ▲ close $9,851.15 vs 09:30 $9,851.15 (session +0.00) | 16:00 close · cash $9,851.15 · no lots left · equity $9,851.15. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,851.15 | ▲ 09:30 equity $9,851.15 vs yday $9,851.15 (+0.00) | 09:30 open · cash $9,851.15 · no holdings · equity $9,851.15 vs prior close $9,851.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 14 | $351.74 | $2.03 | — | $4,924.76 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $4925.58 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 10 | $486.31 | $2.02 | — | $59.64 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $4925.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.64 | ▲ close $10,223.78 vs 09:30 $9,851.15 (session +376.68) | 16:00 close · cash $59.64 · equity $10,223.78 vs 09:30 $9,851.15 (+372.63; session marks +376.68) · 2 name(s) marked open→close (per-name table). AVGO×14 09:30 $351.74 → close $357.16 +75.88; DELL×10 09:30 $486.31 → close $516.39 +300.80 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.64 | ▲ 09:30 equity $10,233.24 vs yday $10,223.78 (+9.46) | 09:30 open · cash $59.64 (unchanged overnight, no fees) · equity $10,233.24 vs prior close $10,223.78 (+9.46) · 2 name(s) re-marked at the open (per-name table). AVGO×14 yday $357.16 → 09:30 $359.70 +35.56; DELL×10 yday $516.39 → 09:30 $513.78 -26.10 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 14 | $359.70 | $2.08 | $+107.33 | $5,093.36 | ▲ +107.33 after sell → book $10,231.16; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 10 | $513.78 | $2.07 | $+270.61 | $10,229.09 | ▲ +270.61 after sell → book $10,229.09; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 12 | $263.36 | $2.03 | — | $7,066.74 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $3409.70 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 207 | $16.40 | $2.67 | — | $3,669.27 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $3409.70 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 45 | $75.65 | $2.12 | — | $262.89 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $3409.70 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $262.89 | ▲ close $10,271.97 vs 09:30 $10,233.24 (session +49.71) | 16:00 close · cash $262.89 · equity $10,271.97 vs 09:30 $10,233.24 (+38.73; session marks +49.71) · 3 name(s) marked open→close (per-name table). CRM×12 09:30 $263.36 → close $259.23 -49.56; FRNM×207 09:30 $16.40 → close $16.31 -18.63; MRX×45 09:30 $75.65 → close $78.27 +117.90 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $262.89 | ▲ 09:30 equity $10,320.51 vs yday $10,271.97 (+48.54) | 09:30 open · cash $262.89 (unchanged overnight, no fees) · equity $10,320.51 vs prior close $10,271.97 (+48.54) · 3 name(s) re-marked at the open (per-name table). CRM×12 yday $259.23 → 09:30 $253.72 -66.12; FRNM×207 yday $16.31 → 09:30 $16.74 +89.01; MRX×45 yday $78.27 → 09:30 $78.84 +25.65 | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 12 | $253.72 | $2.06 | $-119.77 | $3,305.47 | ▼ -119.77 after sell → book $10,318.45; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 207 | $16.74 | $2.73 | $+64.98 | $6,767.92 | ▲ +64.98 after sell → book $10,315.72; vs 09:30 mark -2.73 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 45 | $78.84 | $2.16 | $+139.26 | $10,313.56 | ▲ +139.26 after sell → book $10,313.56; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,313.56 | ▲ close $10,313.56 vs 09:30 $10,320.51 (session +0.00) | 16:00 close · cash $10,313.56 · no lots left · equity $10,313.56. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,313.56 | ▲ 09:30 equity $10,313.56 vs yday $10,313.56 (-0.00) | 09:30 open · cash $10,313.56 · no holdings · equity $10,313.56 vs prior close $10,313.56 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,313.56 | ▲ close $10,313.56 vs 09:30 $10,313.56 (session +0.00) | 16:00 close · cash $10,313.56 · no lots left · equity $10,313.56. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,313.56 | ▲ 09:30 equity $10,313.56 vs yday $10,313.56 (-0.00) | 09:30 open · cash $10,313.56 · no holdings · equity $10,313.56 vs prior close $10,313.56 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,313.56 | ▲ close $10,313.56 vs 09:30 $10,313.56 (session +0.00) | 16:00 close · cash $10,313.56 · no lots left · equity $10,313.56. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,313.56 | ▲ 09:30 equity $10,313.56 vs yday $10,313.56 (-0.00) | 09:30 open · cash $10,313.56 · no holdings · equity $10,313.56 vs prior close $10,313.56 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 62 | $164.43 | $2.18 | — | $116.72 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10313.56 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.72 | ▼ close $9,434.08 vs 09:30 $10,313.56 (session -877.30) | 16:00 close · cash $116.72 · equity $9,434.08 vs 09:30 $10,313.56 (-879.48; session marks -877.30) · 1 name(s) marked open→close (per-name table). ORCL×62 09:30 $164.43 → close $150.28 -877.30 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.72 | ▼ 09:30 equity $8,884.76 vs yday $9,434.08 (-549.32) | 09:30 open · cash $116.72 (unchanged overnight, no fees) · equity $8,884.76 vs prior close $9,434.08 (-549.32) · 1 name(s) re-marked at the open (per-name table). ORCL×62 yday $150.28 → 09:30 $141.42 -549.32 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 62 | $141.42 | $2.26 | $-1431.05 | $8,882.51 | ▼ -1,431.05 after sell → book $8,882.51; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,882.51 | ▲ close $8,882.51 vs 09:30 $8,884.76 (session +0.00) | 16:00 close · cash $8,882.51 · no lots left · equity $8,882.51. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,882.51 | ▲ 09:30 equity $8,882.51 vs yday $8,882.51 (-0.00) | 09:30 open · cash $8,882.51 · no holdings · equity $8,882.51 vs prior close $8,882.51 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,882.51 | ▲ close $8,882.51 vs 09:30 $8,882.51 (session +0.00) | 16:00 close · cash $8,882.51 · no lots left · equity $8,882.51. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,882.51 | ▲ 09:30 equity $8,882.51 vs yday $8,882.51 (-0.00) | 09:30 open · cash $8,882.51 · no holdings · equity $8,882.51 vs prior close $8,882.51 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 112 | $26.27 | $2.33 | — | $5,937.94 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2960.84 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 15 | $189.17 | $2.04 | — | $3,098.35 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2960.84 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 74 | $39.99 | $2.21 | — | $136.88 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2960.84 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $136.88 | ▼ close $8,711.40 vs 09:30 $8,882.51 (session -164.53) | 16:00 close · cash $136.88 · equity $8,711.40 vs 09:30 $8,882.51 (-171.11; session marks -164.53) · 3 name(s) marked open→close (per-name table). WAY×112 09:30 $26.27 → close $26.59 +35.84; QCOM×15 09:30 $189.17 → close $184.84 -64.95; SM×74 09:30 $39.99 → close $38.16 -135.42 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.88 | ▲ 09:30 equity $8,741.43 vs yday $8,711.40 (+30.03) | 09:30 open · cash $136.88 (unchanged overnight, no fees) · equity $8,741.43 vs prior close $8,711.40 (+30.03) · 3 name(s) re-marked at the open (per-name table). WAY×112 yday $26.59 → 09:30 $26.51 -8.96; QCOM×15 yday $184.84 → 09:30 $190.35 +82.65; SM×74 yday $38.16 → 09:30 $37.57 -43.66 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 112 | $26.51 | $2.37 | $+22.19 | $3,103.63 | ▲ +22.19 after sell → book $8,739.06; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 15 | $190.35 | $2.07 | $+13.60 | $5,956.82 | ▲ +13.60 after sell → book $8,737.00; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 74 | $37.57 | $2.25 | $-183.54 | $8,734.75 | ▼ -183.54 after sell → book $8,734.75; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 51 | $170.85 | $2.14 | — | $19.26 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $8734.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.26 | ▲ close $9,106.95 vs 09:30 $8,741.43 (session +374.34) | 16:00 close · cash $19.26 · equity $9,106.95 vs 09:30 $8,741.43 (+365.52; session marks +374.34) · 1 name(s) marked open→close (per-name table). SMTC×51 09:30 $170.85 → close $178.19 +374.34 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.26 | ▲ 09:30 equity $9,318.09 vs yday $9,106.95 (+211.14) | 09:30 open · cash $19.26 (unchanged overnight, no fees) · equity $9,318.09 vs prior close $9,106.95 (+211.14) · 1 name(s) re-marked at the open (per-name table). SMTC×51 yday $178.19 → 09:30 $182.33 +211.14 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 51 | $182.33 | $2.23 | $+581.11 | $9,315.86 | ▲ +581.11 after sell → book $9,315.86; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 148 | $20.91 | $2.43 | — | $6,218.75 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $3105.29 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 135 | $22.90 | $2.40 | — | $3,124.85 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $3105.29 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 209 | $14.79 | $2.70 | — | $31.04 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $3105.29 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.04 | ▼ close $9,256.15 vs 09:30 $9,318.09 (session -52.18) | 16:00 close · cash $31.04 · equity $9,256.15 vs 09:30 $9,318.09 (-61.94; session marks -52.18) · 3 name(s) marked open→close (per-name table). TH×148 09:30 $20.91 → close $21.19 +41.44; GME×135 09:30 $22.90 → close $22.64 -35.10; RARE×209 09:30 $14.79 → close $14.51 -58.52 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.04 | ▲ 09:30 equity $9,357.76 vs yday $9,256.15 (+101.61) | 09:30 open · cash $31.04 (unchanged overnight, no fees) · equity $9,357.76 vs prior close $9,256.15 (+101.61) · 3 name(s) re-marked at the open (per-name table). TH×148 yday $21.19 → 09:30 $21.65 +68.08; GME×135 yday $22.64 → 09:30 $22.78 +18.90; RARE×209 yday $14.51 → 09:30 $14.58 +14.63 | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 148 | $21.65 | $2.48 | $+104.60 | $3,232.76 | ▲ +104.60 after sell → book $9,355.28; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 135 | $22.78 | $2.44 | $-21.04 | $6,305.62 | ▼ -21.04 after sell → book $9,352.84; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 209 | $14.58 | $2.76 | $-49.34 | $9,350.08 | ▼ -49.34 after sell → book $9,350.08; vs 09:30 mark -2.76 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 20 | $230.25 | $2.05 | — | $4,743.03 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+12.5; leftover $4675.04 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 24 | $190.30 | $2.06 | — | $173.77 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+10.6; leftover $4675.04 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.77 | ▼ close $8,908.65 vs 09:30 $9,357.76 (session -437.32) | 16:00 close · cash $173.77 · equity $8,908.65 vs 09:30 $9,357.76 (-449.11; session marks -437.32) · 2 name(s) marked open→close (per-name table). VICR×20 09:30 $230.25 → close $223.90 -127.00; SMTC×24 09:30 $190.30 → close $177.37 -310.32 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.77 | ▲ 09:30 equity $8,908.65 vs yday $8,908.65 (+0.00) | 09:30 open · cash $173.77 (unchanged overnight, no fees) · equity $8,908.65 vs prior close $8,908.65 (+0.00) · 2 name(s) re-marked at the open (per-name table). VICR×20 yday $223.90 → 09:30 $223.90 +0.00; SMTC×24 yday $177.37 → 09:30 $177.37 +0.00 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.77 | ▲ close $8,908.65 vs 09:30 $8,908.65 (session +0.00) | 16:00 close · cash $173.77 · equity $8,908.65 vs 09:30 $8,908.65 (+0.00; session marks +0.00) · 2 name(s) marked open→close (per-name table). VICR×20 09:30 $223.90 → close $223.90 +0.00; SMTC×24 09:30 $177.37 → close $177.37 +0.00 | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.77 | ▲ 09:30 equity $9,691.77 vs yday $8,908.65 (+783.12) | 09:30 open · cash $173.77 (unchanged overnight, no fees) · equity $9,691.77 vs prior close $8,908.65 (+783.12) · 2 name(s) re-marked at the open (per-name table). VICR×20 yday $223.90 → 09:30 $266.50 +852.00; SMTC×24 yday $177.37 → 09:30 $174.50 -68.88 | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 20 | $266.50 | $2.10 | $+720.85 | $5,501.67 | ▲ +720.85 after sell → book $9,689.67; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 24 | $174.50 | $2.11 | $-383.37 | $9,687.56 | ▼ -383.37 after sell → book $9,687.56; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 16 | $196.78 | $2.04 | — | $6,537.05 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $3229.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 406 | $7.95 | $5.24 | — | $3,304.11 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $3229.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 205 | $15.72 | $2.64 | — | $78.86 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $3229.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.86 | ▼ close $9,155.82 vs 09:30 $9,691.77 (session -521.82) | 16:00 close · cash $78.86 · equity $9,155.82 vs 09:30 $9,691.77 (-535.95; session marks -521.82) · 3 name(s) marked open→close (per-name table). CTAS×16 09:30 $196.78 → close $191.97 -76.96; PGEN×406 09:30 $7.95 → close $7.44 -207.06; SGRY×205 09:30 $15.72 → close $14.56 -237.80 | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.86 | ▼ 09:30 equity $9,099.20 vs yday $9,155.82 (-56.62) | 09:30 open · cash $78.86 (unchanged overnight, no fees) · equity $9,099.20 vs prior close $9,155.82 (-56.62) · 3 name(s) re-marked at the open (per-name table). CTAS×16 yday $191.97 → 09:30 $192.26 +4.64; PGEN×406 yday $7.44 → 09:30 $7.38 -24.36; SGRY×205 yday $14.56 → 09:30 $14.38 -36.90 | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 16 | $192.26 | $2.07 | $-76.43 | $3,152.95 | ▼ -76.43 after sell → book $9,097.13; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 406 | $7.38 | $5.33 | $-241.99 | $6,143.90 | ▼ -241.99 after sell → book $9,091.80; vs 09:30 mark -5.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 205 | $14.38 | $2.70 | $-280.05 | $9,089.10 | ▼ -280.05 after sell → book $9,089.10; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,089.10 | ▲ close $9,089.10 vs 09:30 $9,099.20 (session +0.00) | 16:00 close · cash $9,089.10 · no lots left · equity $9,089.10. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ACMR` | cash | leftover split 7.28 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 7.28 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 7.28 < 1 share @ 1746.53 |
| 2026-08-27 | `GEN` | cash | leftover split 7.28 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 7.28 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 7.28 < 1 share @ 222.86 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
