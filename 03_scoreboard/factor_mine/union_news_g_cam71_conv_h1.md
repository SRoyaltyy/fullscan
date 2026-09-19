# Factor mine action — `union_news_g_cam71_conv_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `conviction` · sell `list` · S-boost `none` · news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5

Cash book **+0.00%** ($10,000) · signal-only (no cash/fees) was -3.23%. Starts YES **5/26**. Fills 55 · skips 4 · realized $-39.26.

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
- Must-have: at least 7 green cameras (the +G half of +G −R).
- Must-have: at most 1 red cameras (the −R half of +G −R; 🚨 is not counted here).
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
- **Gate** `news=good,n_pos_min=7,cam_bad_max=1` · **rank** `cond` · **top_n** 4.
- **Size** `conviction` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6.64.

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
| 2026-08-20 | `BHP` | 76 | — | $91.01 | +0.00 | $93.63 | +199.12 | +199.12 | +0.00 | +199.12 |
| 2026-08-20 | `APA` | 67 | — | $44.76 | +0.00 | $44.39 | -24.79 | -24.79 | +0.00 | -24.79 |
| 2026-08-21 | `BHP` | 76 | $93.63 | $95.72 | +158.84 | — | +0.00 | +158.84 | +357.96 | — |
| 2026-08-21 | `APA` | 67 | $44.39 | $44.52 | +8.71 | — | +0.00 | +8.71 | -16.08 | — |
| 2026-08-21 | `AU` | 60 | — | $119.43 | +0.00 | $121.22 | +107.40 | +107.40 | +0.00 | +107.40 |
| 2026-08-21 | `AUTL` | 418 | — | $2.47 | +0.00 | $2.41 | -25.08 | -25.08 | +0.00 | -25.08 |
| 2026-08-21 | `CRSP` | 17 | — | $59.72 | +0.00 | $59.50 | -3.74 | -3.74 | +0.00 | -3.74 |
| 2026-08-21 | `FUTU` | 8 | — | $115.18 | +0.00 | $123.64 | +67.68 | +67.68 | +0.00 | +67.68 |
| 2026-08-24 | `AU` | 60 | $121.22 | $120.51 | -42.60 | — | +0.00 | -42.60 | +64.80 | — |
| 2026-08-24 | `AUTL` | 418 | $2.41 | $2.40 | -4.18 | — | +0.00 | -4.18 | -29.26 | — |
| 2026-08-24 | `CRSP` | 17 | $59.50 | $58.75 | -12.75 | — | +0.00 | -12.75 | -16.49 | — |
| 2026-08-24 | `FUTU` | 8 | $123.64 | $121.00 | -21.12 | — | +0.00 | -21.12 | +46.56 | — |
| 2026-08-25 | `AU` | 61 | — | $118.52 | +0.00 | $123.39 | +297.07 | +297.07 | +0.00 | +297.07 |
| 2026-08-25 | `FCX` | 40 | — | $77.13 | +0.00 | $79.91 | +111.20 | +111.20 | +0.00 | +111.20 |
| 2026-08-26 | `AU` | 61 | $123.39 | $119.80 | -218.99 | — | +0.00 | -218.99 | +78.08 | — |
| 2026-08-26 | `FCX` | 40 | $79.91 | $79.34 | -22.80 | — | +0.00 | -22.80 | +88.40 | — |
| 2026-08-26 | `ASST` | 508 | — | $20.72 | +0.00 | $21.50 | +396.24 | +396.24 | +0.00 | +396.24 |
| 2026-08-27 | `ASST` | 508 | $21.50 | $22.45 | +482.60 | — | +0.00 | +482.60 | +878.84 | — |
| 2026-08-27 | `ACMR` | 97 | — | $81.65 | +0.00 | $80.49 | -112.52 | -112.52 | +0.00 | -112.52 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `CM` | 9 | — | $118.77 | +0.00 | $114.84 | -35.37 | -35.37 | +0.00 | -35.37 |
| 2026-08-28 | `ACMR` | 97 | $80.49 | $79.27 | -118.34 | — | +0.00 | -118.34 | -230.86 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `CM` | 9 | $114.84 | $115.66 | +7.38 | — | +0.00 | +7.38 | -27.99 | — |
| 2026-08-28 | `KEYS` | 23 | — | $324.41 | +0.00 | $319.97 | -102.12 | -102.12 | +0.00 | -102.12 |
| 2026-08-28 | `SMTC` | 7 | — | $141.76 | +0.00 | $131.17 | -74.13 | -74.13 | +0.00 | -74.13 |
| 2026-08-28 | `CIEN` | 2 | — | $400.42 | +0.00 | $378.44 | -43.96 | -43.96 | +0.00 | -43.96 |
| 2026-08-31 | `KEYS` | 23 | $319.97 | $322.49 | +57.96 | — | +0.00 | +57.96 | -44.16 | — |
| 2026-08-31 | `SMTC` | 7 | $131.17 | $132.30 | +7.91 | — | +0.00 | +7.91 | -66.22 | — |
| 2026-08-31 | `CIEN` | 2 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -43.96 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 21 | — | $351.74 | +0.00 | $357.16 | +113.82 | +113.82 | +0.00 | +113.82 |
| 2026-09-03 | `DELL` | 6 | — | $486.31 | +0.00 | $516.39 | +180.48 | +180.48 | +0.00 | +180.48 |
| 2026-09-04 | `AVGO` | 21 | $357.16 | $359.70 | +53.34 | — | +0.00 | +53.34 | +167.16 | — |
| 2026-09-04 | `DELL` | 6 | $516.39 | $513.78 | -15.66 | — | +0.00 | -15.66 | +164.82 | — |
| 2026-09-04 | `CRM` | 29 | — | $263.36 | +0.00 | $259.23 | -119.77 | -119.77 | +0.00 | -119.77 |
| 2026-09-04 | `FRNM` | 68 | — | $16.40 | +0.00 | $16.31 | -6.12 | -6.12 | +0.00 | -6.12 |
| 2026-09-04 | `MMED` | 47 | — | $23.84 | +0.00 | $23.29 | -25.85 | -25.85 | +0.00 | -25.85 |
| 2026-09-04 | `HPE` | 20 | — | $53.85 | +0.00 | $52.00 | -37.00 | -37.00 | +0.00 | -37.00 |
| 2026-09-08 | `CRM` | 29 | $259.23 | $253.72 | -159.79 | — | +0.00 | -159.79 | -279.56 | — |
| 2026-09-08 | `FRNM` | 68 | $16.31 | $16.74 | +29.24 | — | +0.00 | +29.24 | +23.12 | — |
| 2026-09-08 | `MMED` | 47 | $23.29 | $23.16 | -6.11 | — | +0.00 | -6.11 | -31.96 | — |
| 2026-09-08 | `HPE` | 20 | $52.00 | $52.29 | +5.80 | — | +0.00 | +5.80 | -31.20 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 66 | — | $164.43 | +0.00 | $150.28 | -933.90 | -933.90 | +0.00 | -933.90 |
| 2026-09-14 | `ORCL` | 66 | $150.28 | $141.42 | -584.76 | — | +0.00 | -584.76 | -1518.66 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 249 | — | $26.27 | +0.00 | $26.59 | +79.68 | +79.68 | +0.00 | +79.68 |
| 2026-09-16 | `QCOM` | 7 | — | $189.17 | +0.00 | $184.84 | -30.31 | -30.31 | +0.00 | -30.31 |
| 2026-09-16 | `SM` | 35 | — | $39.99 | +0.00 | $38.16 | -64.05 | -64.05 | +0.00 | -64.05 |
| 2026-09-17 | `WAY` | 249 | $26.59 | $26.51 | -19.92 | — | +0.00 | -19.92 | +59.76 | — |
| 2026-09-17 | `QCOM` | 7 | $184.84 | $190.35 | +38.57 | — | +0.00 | +38.57 | +8.26 | — |
| 2026-09-17 | `SM` | 35 | $38.16 | $37.57 | -20.65 | — | +0.00 | -20.65 | -84.70 | — |
| 2026-09-17 | `SMTC` | 54 | — | $170.85 | +0.00 | $178.19 | +396.36 | +396.36 | +0.00 | +396.36 |
| 2026-09-18 | `SMTC` | 54 | $178.19 | $182.33 | +223.56 | — | +0.00 | +223.56 | +619.92 | — |
| 2026-09-18 | `TH` | 333 | — | $20.91 | +0.00 | $21.19 | +93.24 | +93.24 | +0.00 | +93.24 |
| 2026-09-18 | `GME` | 65 | — | $22.90 | +0.00 | $22.64 | -16.90 | -16.90 | +0.00 | -16.90 |
| 2026-09-18 | `RARE` | 101 | — | $14.79 | +0.00 | $14.51 | -28.28 | -28.28 | +0.00 | -28.28 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +174.33 | BHP, APA | — | $79.91 | $10,169.92 | BHP×76, APA×67 |
| 2026-08-21 | +3.25 | $79.91 | BHP×76, APA×67 | $10,337.47 | +167.55 | +146.26 | AU, AUTL, CRSP, FUTU | BHP, APA | $186.40 | $10,467.60 | AU×60, AUTL×418, CRSP×17, FUTU×8 |
| 2026-08-24 | -5.17 | $186.40 | AU×60, AUTL×418, CRSP×17, FUTU×8 | $10,386.95 | -80.65 | +0.00 | — | AU, AUTL, CRSP, FUTU | $10,375.14 | $10,375.14 | — |
| 2026-08-25 | +1.80 | $10,375.14 | — | $10,375.14 | +0.00 | +408.27 | AU, FCX | — | $55.94 | $10,779.13 | AU×61, FCX×40 |
| 2026-08-26 | +2.02 | $55.94 | AU×61, FCX×40 | $10,537.34 | -241.79 | +396.24 | ASST | AU, FCX | $0.64 | $10,922.64 | ASST×508 |
| 2026-08-27 | — | $0.64 | ASST×508 | $11,405.24 | +482.60 | -179.51 | ACMR, MU, CM | ASST | $1,436.23 | $11,212.71 | ACMR×97, MU×1, CM×9 |
| 2026-08-28 | +0.75 | $1,436.23 | ACMR×97, MU×1, CM×9 | $11,085.65 | -127.06 | -220.21 | KEYS, SMTC, CIEN | ACMR, MU, CM | $1,818.59 | $10,852.97 | KEYS×23, SMTC×7, CIEN×2 |
| 2026-08-31 | -5.85 | $1,818.59 | KEYS×23, SMTC×7, CIEN×2 | $10,918.84 | +65.87 | +0.00 | — | KEYS, SMTC, CIEN | $10,912.66 | $10,912.66 | — |
| 2026-09-01 | -6.30 | $10,912.66 | — | $10,912.66 | +0.00 | +0.00 | — | — | $10,912.66 | $10,912.66 | — |
| 2026-09-02 | -3.83 | $10,912.66 | — | $10,912.66 | +0.00 | +0.00 | — | — | $10,912.66 | $10,912.66 | — |
| 2026-09-03 | -0.90 | $10,912.66 | — | $10,912.66 | +0.00 | +294.30 | AVGO, DELL | — | $604.20 | $11,202.90 | AVGO×21, DELL×6 |
| 2026-09-04 | +2.25 | $604.20 | AVGO×21, DELL×6 | $11,240.58 | +37.68 | -188.74 | CRM, FRNM, MMED, HPE | AVGO, DELL | $277.84 | $11,039.22 | CRM×29, FRNM×68, MMED×47, HPE×20 |
| 2026-09-08 | -11.47 | $277.84 | CRM×29, FRNM×68, MMED×47, HPE×20 | $10,908.36 | -130.86 | +0.00 | — | CRM, FRNM, MMED, HPE | $10,899.78 | $10,899.78 | — |
| 2026-09-09 | -13.95 | $10,899.78 | — | $10,899.78 | +0.00 | +0.00 | — | — | $10,899.78 | $10,899.78 | — |
| 2026-09-10 | -13.28 | $10,899.78 | — | $10,899.78 | +0.00 | +0.00 | — | — | $10,899.78 | $10,899.78 | — |
| 2026-09-11 | +0.50 | $10,899.78 | — | $10,899.78 | +0.00 | -933.90 | ORCL | — | $45.21 | $9,963.69 | ORCL×66 |
| 2026-09-14 | -11.00 | $45.21 | ORCL×66 | $9,378.93 | -584.76 | +0.00 | — | ORCL | $9,376.66 | $9,376.66 | — |
| 2026-09-15 | -3.84 | $9,376.66 | — | $9,376.66 | -0.00 | +0.00 | — | — | $9,376.66 | $9,376.66 | — |
| 2026-09-16 | +5.30 | $9,376.66 | — | $9,376.66 | -0.00 | -14.68 | WAY, QCOM, SM | — | $104.27 | $9,354.66 | WAY×249, QCOM×7, SM×35 |
| 2026-09-17 | +7.38 | $104.27 | WAY×249, QCOM×7, SM×35 | $9,352.66 | -2.00 | +396.36 | SMTC | WAY, QCOM, SM | $117.16 | $9,739.42 | SMTC×54 |
| 2026-09-18 | +4.86 | $117.16 | SMTC×54 | $9,962.98 | +223.56 | +48.06 | TH, GME, RARE | SMTC | $6.64 | $10,000.02 | TH×333, GME×65, RARE×101 |

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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 76 | $91.01 | $2.22 | — | $3,081.02 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $7000.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 67 | $44.76 | $2.19 | — | $79.91 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $3000.00 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.91 | ▲ close $10,169.92 vs 09:30 $10,000.00 (session +174.33) | 16:00 close · cash $79.91 · equity $10,169.92 vs 09:30 $10,000.00 (+169.92; session marks +174.33) · 2 name(s) marked open→close (per-name table). BHP×76 09:30 $91.01 → close $93.63 +199.12; APA×67 09:30 $44.76 → close $44.39 -24.79 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.91 | ▲ 09:30 equity $10,337.47 vs yday $10,169.92 (+167.55) | 09:30 open · cash $79.91 (unchanged overnight, no fees) · equity $10,337.47 vs prior close $10,169.92 (+167.55) · 2 name(s) re-marked at the open (per-name table). BHP×76 yday $93.63 → 09:30 $95.72 +158.84; APA×67 yday $44.39 → 09:30 $44.52 +8.71 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 76 | $95.72 | $2.29 | $+353.45 | $7,352.34 | ▲ +353.45 after sell → book $10,335.18; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 67 | $44.52 | $2.23 | $-20.50 | $10,332.96 | ▼ -20.50 after sell → book $10,332.96; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 60 | $119.43 | $2.17 | — | $3,164.99 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $7233.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 418 | $2.47 | $5.39 | — | $2,127.13 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1033.30 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 17 | $59.72 | $2.04 | — | $1,109.85 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1033.30 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 8 | $115.18 | $2.01 | — | $186.40 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1033.30 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.40 | ▲ close $10,467.60 vs 09:30 $10,337.47 (session +146.26) | 16:00 close · cash $186.40 · equity $10,467.60 vs 09:30 $10,337.47 (+130.13; session marks +146.26) · 4 name(s) marked open→close (per-name table). AU×60 09:30 $119.43 → close $121.22 +107.40; AUTL×418 09:30 $2.47 → close $2.41 -25.08; CRSP×17 09:30 $59.72 → close $59.50 -3.74; FUTU×8 09:30 $115.18 → close $123.64 +67.68 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.40 | ▼ 09:30 equity $10,386.95 vs yday $10,467.60 (-80.65) | 09:30 open · cash $186.40 (unchanged overnight, no fees) · equity $10,386.95 vs prior close $10,467.60 (-80.65) · 4 name(s) re-marked at the open (per-name table). AU×60 yday $121.22 → 09:30 $120.51 -42.60; AUTL×418 yday $2.41 → 09:30 $2.40 -4.18; CRSP×17 yday $59.50 → 09:30 $58.75 -12.75; FUTU×8 yday $123.64 → 09:30 $121.00 -21.12 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 60 | $120.51 | $2.24 | $+60.39 | $7,414.76 | ▲ +60.39 after sell → book $10,384.71; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 418 | $2.40 | $5.47 | $-40.12 | $8,412.49 | ▼ -40.12 after sell → book $10,379.24; vs 09:30 mark -5.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 17 | $58.75 | $2.06 | $-20.59 | $9,409.18 | ▼ -20.59 after sell → book $10,377.18; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 8 | $121.00 | $2.03 | $+42.51 | $10,375.14 | ▲ +42.51 after sell → book $10,375.14; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,375.14 | ▲ close $10,375.14 vs 09:30 $10,386.95 (session +0.00) | 16:00 close · cash $10,375.14 · no lots left · equity $10,375.14. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,375.14 | ▲ 09:30 equity $10,375.14 vs yday $10,375.14 (+0.00) | 09:30 open · cash $10,375.14 · no holdings · equity $10,375.14 vs prior close $10,375.14 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 61 | $118.52 | $2.17 | — | $3,143.25 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $7262.60 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 40 | $77.13 | $2.11 | — | $55.94 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3112.54 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.94 | ▲ close $10,779.13 vs 09:30 $10,375.14 (session +408.27) | 16:00 close · cash $55.94 · equity $10,779.13 vs 09:30 $10,375.14 (+403.99; session marks +408.27) · 2 name(s) marked open→close (per-name table). AU×61 09:30 $118.52 → close $123.39 +297.07; FCX×40 09:30 $77.13 → close $79.91 +111.20 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.94 | ▼ 09:30 equity $10,537.34 vs yday $10,779.13 (-241.79) | 09:30 open · cash $55.94 (unchanged overnight, no fees) · equity $10,537.34 vs prior close $10,779.13 (-241.79) · 2 name(s) re-marked at the open (per-name table). AU×61 yday $123.39 → 09:30 $119.80 -218.99; FCX×40 yday $79.91 → 09:30 $79.34 -22.80 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 61 | $119.80 | $2.24 | $+73.67 | $7,361.50 | ▲ +73.67 after sell → book $10,535.10; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 40 | $79.34 | $2.15 | $+84.14 | $10,532.95 | ▲ +84.14 after sell → book $10,532.95; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 508 | $20.72 | $6.55 | — | $0.64 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=+67.1; leftover $10532.95 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.64 | ▲ close $10,922.64 vs 09:30 $10,537.34 (session +396.24) | 16:00 close · cash $0.64 · equity $10,922.64 vs 09:30 $10,537.34 (+385.30; session marks +396.24) · 1 name(s) marked open→close (per-name table). ASST×508 09:30 $20.72 → close $21.50 +396.24 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.64 | ▲ 09:30 equity $11,405.24 vs yday $10,922.64 (+482.60) | 09:30 open · cash $0.64 (unchanged overnight, no fees) · equity $11,405.24 vs prior close $10,922.64 (+482.60) · 1 name(s) re-marked at the open (per-name table). ASST×508 yday $21.50 → 09:30 $22.45 +482.60 | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 508 | $22.45 | $6.73 | $+865.56 | $11,398.51 | ▲ +865.56 after sell → book $11,398.51; vs 09:30 mark -6.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 97 | $81.65 | $2.28 | — | $3,476.18 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $7978.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $2,507.18 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1139.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 9 | $118.77 | $2.02 | — | $1,436.23 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; ret5=+0.3; leftover $1139.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,436.23 | ▼ close $11,212.71 vs 09:30 $11,405.24 (session -179.51) | 16:00 close · cash $1,436.23 · equity $11,212.71 vs 09:30 $11,405.24 (-192.53; session marks -179.51) · 3 name(s) marked open→close (per-name table). ACMR×97 09:30 $81.65 → close $80.49 -112.52; MU×1 09:30 $967.01 → close $935.39 -31.62; CM×9 09:30 $118.77 → close $114.84 -35.37 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,436.23 | ▼ 09:30 equity $11,085.65 vs yday $11,212.71 (-127.06) | 09:30 open · cash $1,436.23 (unchanged overnight, no fees) · equity $11,085.65 vs prior close $11,212.71 (-127.06) · 3 name(s) re-marked at the open (per-name table). ACMR×97 yday $80.49 → 09:30 $79.27 -118.34; MU×1 yday $935.39 → 09:30 $919.29 -16.10; CM×9 yday $114.84 → 09:30 $115.66 +7.38 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 97 | $79.27 | $2.36 | $-235.50 | $9,123.06 | ▼ -235.50 after sell → book $11,083.29; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $10,040.34 | ▼ -51.73 after sell → book $11,081.28; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 9 | $115.66 | $2.04 | $-32.04 | $11,079.24 | ▼ -32.04 after sell → book $11,079.24; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 23 | $324.41 | $2.06 | — | $3,615.75 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $7755.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $2,621.42 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1107.92 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $1,818.59 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1107.92 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,818.59 | ▼ close $10,852.97 vs 09:30 $11,085.65 (session -220.21) | 16:00 close · cash $1,818.59 · equity $10,852.97 vs 09:30 $11,085.65 (-232.68; session marks -220.21) · 3 name(s) marked open→close (per-name table). KEYS×23 09:30 $324.41 → close $319.97 -102.12; SMTC×7 09:30 $141.76 → close $131.17 -74.13; CIEN×2 09:30 $400.42 → close $378.44 -43.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,818.59 | ▲ 09:30 equity $10,918.84 vs yday $10,852.97 (+65.87) | 09:30 open · cash $1,818.59 (unchanged overnight, no fees) · equity $10,918.84 vs prior close $10,852.97 (+65.87) · 3 name(s) re-marked at the open (per-name table). KEYS×23 yday $319.97 → 09:30 $322.49 +57.96; SMTC×7 yday $131.17 → 09:30 $132.30 +7.91; CIEN×2 yday $378.44 → 09:30 $378.44 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 23 | $322.49 | $2.13 | $-48.35 | $9,233.73 | ▼ -48.35 after sell → book $10,916.71; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 7 | $132.30 | $2.03 | $-70.26 | $10,157.80 | ▼ -70.26 after sell → book $10,914.68; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 2 | $378.44 | $2.02 | $-47.97 | $10,912.66 | ▼ -47.97 after sell → book $10,912.66; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,912.66 | ▲ close $10,912.66 vs 09:30 $10,918.84 (session +0.00) | 16:00 close · cash $10,912.66 · no lots left · equity $10,912.66. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,912.66 | ▲ 09:30 equity $10,912.66 vs yday $10,912.66 (+0.00) | 09:30 open · cash $10,912.66 · no holdings · equity $10,912.66 vs prior close $10,912.66 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,912.66 | ▲ close $10,912.66 vs 09:30 $10,912.66 (session +0.00) | 16:00 close · cash $10,912.66 · no lots left · equity $10,912.66. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,912.66 | ▲ 09:30 equity $10,912.66 vs yday $10,912.66 (+0.00) | 09:30 open · cash $10,912.66 · no holdings · equity $10,912.66 vs prior close $10,912.66 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,912.66 | ▲ close $10,912.66 vs 09:30 $10,912.66 (session +0.00) | 16:00 close · cash $10,912.66 · no lots left · equity $10,912.66. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,912.66 | ▲ 09:30 equity $10,912.66 vs yday $10,912.66 (+0.00) | 09:30 open · cash $10,912.66 · no holdings · equity $10,912.66 vs prior close $10,912.66 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 21 | $351.74 | $2.05 | — | $3,524.07 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $7638.86 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 6 | $486.31 | $2.01 | — | $604.20 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $3273.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $604.20 | ▲ close $11,202.90 vs 09:30 $10,912.66 (session +294.30) | 16:00 close · cash $604.20 · equity $11,202.90 vs 09:30 $10,912.66 (+290.24; session marks +294.30) · 2 name(s) marked open→close (per-name table). AVGO×21 09:30 $351.74 → close $357.16 +113.82; DELL×6 09:30 $486.31 → close $516.39 +180.48 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $604.20 | ▲ 09:30 equity $11,240.58 vs yday $11,202.90 (+37.68) | 09:30 open · cash $604.20 (unchanged overnight, no fees) · equity $11,240.58 vs prior close $11,202.90 (+37.68) · 2 name(s) re-marked at the open (per-name table). AVGO×21 yday $357.16 → 09:30 $359.70 +53.34; DELL×6 yday $516.39 → 09:30 $513.78 -15.66 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 21 | $359.70 | $2.12 | $+162.98 | $8,155.78 | ▲ +162.98 after sell → book $11,238.46; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 6 | $513.78 | $2.04 | $+160.77 | $11,236.41 | ▲ +160.77 after sell → book $11,236.41; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 29 | $263.36 | $2.08 | — | $3,596.90 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $7865.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 68 | $16.40 | $2.19 | — | $2,479.50 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1123.64 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MMED` | 47 | $23.84 | $2.13 | — | $1,356.89 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list oppset; 🔵; ⚪; ret5=+22.2; leftover $1123.64 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HPE` | 20 | $53.85 | $2.05 | — | $277.84 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=+0.1; leftover $1123.64 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟡 vol🟢 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $277.84 | ▼ close $11,039.22 vs 09:30 $11,240.58 (session -188.74) | 16:00 close · cash $277.84 · equity $11,039.22 vs 09:30 $11,240.58 (-201.36; session marks -188.74) · 4 name(s) marked open→close (per-name table). CRM×29 09:30 $263.36 → close $259.23 -119.77; FRNM×68 09:30 $16.40 → close $16.31 -6.12; MMED×47 09:30 $23.84 → close $23.29 -25.85; HPE×20 09:30 $53.85 → close $52.00 -37.00 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $277.84 | ▼ 09:30 equity $10,908.36 vs yday $11,039.22 (-130.86) | 09:30 open · cash $277.84 (unchanged overnight, no fees) · equity $10,908.36 vs prior close $11,039.22 (-130.86) · 4 name(s) re-marked at the open (per-name table). CRM×29 yday $259.23 → 09:30 $253.72 -159.79; FRNM×68 yday $16.31 → 09:30 $16.74 +29.24; MMED×47 yday $23.29 → 09:30 $23.16 -6.11; HPE×20 yday $52.00 → 09:30 $52.29 +5.80 | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 29 | $253.72 | $2.15 | $-283.78 | $7,633.58 | ▼ -283.78 after sell → book $10,906.22; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 68 | $16.74 | $2.22 | $+18.71 | $8,769.68 | ▲ +18.71 after sell → book $10,904.00; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 47 | $23.16 | $2.15 | $-36.24 | $9,856.05 | ▼ -36.24 after sell → book $10,901.85; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HPE` | 20 | $52.29 | $2.07 | $-35.32 | $10,899.78 | ▼ -35.32 after sell → book $10,899.78; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,899.78 | ▲ close $10,899.78 vs 09:30 $10,908.36 (session +0.00) | 16:00 close · cash $10,899.78 · no lots left · equity $10,899.78. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,899.78 | ▲ 09:30 equity $10,899.78 vs yday $10,899.78 (+0.00) | 09:30 open · cash $10,899.78 · no holdings · equity $10,899.78 vs prior close $10,899.78 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,899.78 | ▲ close $10,899.78 vs 09:30 $10,899.78 (session +0.00) | 16:00 close · cash $10,899.78 · no lots left · equity $10,899.78. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,899.78 | ▲ 09:30 equity $10,899.78 vs yday $10,899.78 (+0.00) | 09:30 open · cash $10,899.78 · no holdings · equity $10,899.78 vs prior close $10,899.78 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,899.78 | ▲ close $10,899.78 vs 09:30 $10,899.78 (session +0.00) | 16:00 close · cash $10,899.78 · no lots left · equity $10,899.78. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,899.78 | ▲ 09:30 equity $10,899.78 vs yday $10,899.78 (+0.00) | 09:30 open · cash $10,899.78 · no holdings · equity $10,899.78 vs prior close $10,899.78 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 66 | $164.43 | $2.19 | — | $45.21 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10899.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.21 | ▼ close $9,963.69 vs 09:30 $10,899.78 (session -933.90) | 16:00 close · cash $45.21 · equity $9,963.69 vs 09:30 $10,899.78 (-936.09; session marks -933.90) · 1 name(s) marked open→close (per-name table). ORCL×66 09:30 $164.43 → close $150.28 -933.90 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.21 | ▼ 09:30 equity $9,378.93 vs yday $9,963.69 (-584.76) | 09:30 open · cash $45.21 (unchanged overnight, no fees) · equity $9,378.93 vs prior close $9,963.69 (-584.76) · 1 name(s) re-marked at the open (per-name table). ORCL×66 yday $150.28 → 09:30 $141.42 -584.76 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 66 | $141.42 | $2.27 | $-1523.12 | $9,376.66 | ▼ -1,523.12 after sell → book $9,376.66; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,376.66 | ▲ close $9,376.66 vs 09:30 $9,378.93 (session +0.00) | 16:00 close · cash $9,376.66 · no lots left · equity $9,376.66. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,376.66 | ▲ 09:30 equity $9,376.66 vs yday $9,376.66 (-0.00) | 09:30 open · cash $9,376.66 · no holdings · equity $9,376.66 vs prior close $9,376.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,376.66 | ▲ close $9,376.66 vs 09:30 $9,376.66 (session +0.00) | 16:00 close · cash $9,376.66 · no lots left · equity $9,376.66. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,376.66 | ▲ 09:30 equity $9,376.66 vs yday $9,376.66 (-0.00) | 09:30 open · cash $9,376.66 · no holdings · equity $9,376.66 vs prior close $9,376.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 249 | $26.27 | $3.21 | — | $2,832.22 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $6563.66 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 7 | $189.17 | $2.01 | — | $1,506.02 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $1406.50 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 35 | $39.99 | $2.10 | — | $104.27 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1406.50 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.27 | ▼ close $9,354.66 vs 09:30 $9,376.66 (session -14.68) | 16:00 close · cash $104.27 · equity $9,354.66 vs 09:30 $9,376.66 (-22.00; session marks -14.68) · 3 name(s) marked open→close (per-name table). WAY×249 09:30 $26.27 → close $26.59 +79.68; QCOM×7 09:30 $189.17 → close $184.84 -30.31; SM×35 09:30 $39.99 → close $38.16 -64.05 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.27 | ▼ 09:30 equity $9,352.66 vs yday $9,354.66 (-2.00) | 09:30 open · cash $104.27 (unchanged overnight, no fees) · equity $9,352.66 vs prior close $9,354.66 (-2.00) · 3 name(s) re-marked at the open (per-name table). WAY×249 yday $26.59 → 09:30 $26.51 -19.92; QCOM×7 yday $184.84 → 09:30 $190.35 +38.57; SM×35 yday $38.16 → 09:30 $37.57 -20.65 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 249 | $26.51 | $3.31 | $+53.24 | $6,701.95 | ▲ +53.24 after sell → book $9,349.35; vs 09:30 mark -3.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 7 | $190.35 | $2.03 | $+4.22 | $8,032.37 | ▲ +4.22 after sell → book $9,347.32; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 35 | $37.57 | $2.12 | $-88.91 | $9,345.21 | ▼ -88.91 after sell → book $9,345.21; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 54 | $170.85 | $2.15 | — | $117.16 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $9345.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.16 | ▲ close $9,739.42 vs 09:30 $9,352.66 (session +396.36) | 16:00 close · cash $117.16 · equity $9,739.42 vs 09:30 $9,352.66 (+386.76; session marks +396.36) · 1 name(s) marked open→close (per-name table). SMTC×54 09:30 $170.85 → close $178.19 +396.36 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.16 | ▲ 09:30 equity $9,962.98 vs yday $9,739.42 (+223.56) | 09:30 open · cash $117.16 (unchanged overnight, no fees) · equity $9,962.98 vs prior close $9,739.42 (+223.56) · 1 name(s) re-marked at the open (per-name table). SMTC×54 yday $178.19 → 09:30 $182.33 +223.56 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 54 | $182.33 | $2.24 | $+615.53 | $9,960.73 | ▲ +615.53 after sell → book $9,960.73; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 333 | $20.91 | $4.30 | — | $2,993.41 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $6972.51 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 65 | $22.90 | $2.19 | — | $1,502.72 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1494.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 101 | $14.79 | $2.29 | — | $6.64 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1494.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.64 | ▲ close $10,000.02 vs 09:30 $9,962.98 (session +48.06) | 16:00 close · cash $6.64 · equity $10,000.02 vs 09:30 $9,962.98 (+37.04; session marks +48.06) · 3 name(s) marked open→close (per-name table). TH×333 09:30 $20.91 → close $21.19 +93.24; GME×65 09:30 $22.90 → close $22.64 -16.90; RARE×101 09:30 $14.79 → close $14.51 -28.28 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1139.85 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1107.92 < 1 share @ 1306.03 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TH` | 333 | 2026-09-18 @ $20.91 | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $6972.51 |
| `GME` | 65 | 2026-09-18 @ $22.90 | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1494.11 |
| `RARE` | 101 | 2026-09-18 @ $14.79 | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1494.11 |
