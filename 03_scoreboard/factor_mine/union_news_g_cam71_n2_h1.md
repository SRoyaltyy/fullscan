# Factor mine action — `union_news_g_cam71_n2_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 2 · rank `cond` · size `topheavy` · sell `list` · S-boost `none` · topheavy leftover on news🟢 +7 −≤1

Cash book **-13.78%** ($8,622) · signal-only (no cash/fees) was -9.09%. Starts YES **5/27**. Fills 44 · skips 2 · realized $-1378.15.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 2 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Sort the keepers by how many morning cameras are green vs red and keep the top 2.
- Give about 40% of leftover cash to the first name; split the rest.
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
- **Gate** `news=good,n_pos_min=7,cam_bad_max=1` · **rank** `cond` · **top_n** 2.
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,621.84.

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
| 2026-08-20 | `BHP` | 43 | — | $91.01 | +0.00 | $93.63 | +112.66 | +112.66 | +0.00 | +112.66 |
| 2026-08-20 | `APA` | 134 | — | $44.76 | +0.00 | $44.39 | -49.58 | -49.58 | +0.00 | -49.58 |
| 2026-08-21 | `BHP` | 43 | $93.63 | $95.72 | +89.87 | — | +0.00 | +89.87 | +202.53 | — |
| 2026-08-21 | `APA` | 134 | $44.39 | $44.52 | +17.42 | — | +0.00 | +17.42 | -32.16 | — |
| 2026-08-21 | `AU` | 34 | — | $119.43 | +0.00 | $121.22 | +60.86 | +60.86 | +0.00 | +60.86 |
| 2026-08-21 | `AUTL` | 2456 | — | $2.47 | +0.00 | $2.41 | -147.36 | -147.36 | +0.00 | -147.36 |
| 2026-08-24 | `AU` | 34 | $121.22 | $120.51 | -24.14 | — | +0.00 | -24.14 | +36.72 | — |
| 2026-08-24 | `AUTL` | 2456 | $2.41 | $2.40 | -24.56 | — | +0.00 | -24.56 | -171.92 | — |
| 2026-08-25 | `AU` | 33 | — | $118.52 | +0.00 | $123.39 | +160.71 | +160.71 | +0.00 | +160.71 |
| 2026-08-25 | `FCX` | 77 | — | $77.13 | +0.00 | $79.91 | +214.06 | +214.06 | +0.00 | +214.06 |
| 2026-08-26 | `AU` | 33 | $123.39 | $119.80 | -118.47 | — | +0.00 | -118.47 | +42.24 | — |
| 2026-08-26 | `FCX` | 77 | $79.91 | $79.34 | -43.89 | — | +0.00 | -43.89 | +170.17 | — |
| 2026-08-26 | `CM` | 85 | — | $118.50 | +0.00 | $118.20 | -25.50 | -25.50 | +0.00 | -25.50 |
| 2026-08-27 | `CM` | 85 | $118.20 | $118.77 | +48.45 | — | +0.00 | +48.45 | +22.95 | — |
| 2026-08-27 | `ACMR` | 49 | — | $81.65 | +0.00 | $80.49 | -56.84 | -56.84 | +0.00 | -56.84 |
| 2026-08-27 | `MU` | 6 | — | $967.01 | +0.00 | $935.39 | -189.72 | -189.72 | +0.00 | -189.72 |
| 2026-08-28 | `ACMR` | 49 | $80.49 | $79.27 | -59.78 | — | +0.00 | -59.78 | -116.62 | — |
| 2026-08-28 | `MU` | 6 | $935.39 | $919.29 | -96.60 | — | +0.00 | -96.60 | -286.32 | — |
| 2026-08-28 | `KEYS` | 12 | — | $324.41 | +0.00 | $319.97 | -53.28 | -53.28 | +0.00 | -53.28 |
| 2026-08-28 | `SMTC` | 41 | — | $141.76 | +0.00 | $131.17 | -434.19 | -434.19 | +0.00 | -434.19 |
| 2026-08-31 | `KEYS` | 12 | $319.97 | $322.49 | +30.24 | — | +0.00 | +30.24 | -23.04 | — |
| 2026-08-31 | `SMTC` | 41 | $131.17 | $132.30 | +46.33 | — | +0.00 | +46.33 | -387.86 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 10 | — | $351.74 | +0.00 | $357.16 | +54.20 | +54.20 | +0.00 | +54.20 |
| 2026-09-03 | `DELL` | 11 | — | $486.31 | +0.00 | $516.39 | +330.88 | +330.88 | +0.00 | +330.88 |
| 2026-09-04 | `AVGO` | 10 | $357.16 | $359.70 | +25.40 | — | +0.00 | +25.40 | +79.60 | — |
| 2026-09-04 | `DELL` | 11 | $516.39 | $513.78 | -28.71 | — | +0.00 | -28.71 | +302.17 | — |
| 2026-09-04 | `CRM` | 14 | — | $263.36 | +0.00 | $259.23 | -57.82 | -57.82 | +0.00 | -57.82 |
| 2026-09-04 | `FRNM` | 355 | — | $16.40 | +0.00 | $16.31 | -31.95 | -31.95 | +0.00 | -31.95 |
| 2026-09-08 | `CRM` | 14 | $259.23 | $253.72 | -77.14 | — | +0.00 | -77.14 | -134.96 | — |
| 2026-09-08 | `FRNM` | 355 | $16.31 | $16.74 | +152.65 | — | +0.00 | +152.65 | +120.70 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 58 | — | $164.43 | +0.00 | $150.28 | -820.70 | -820.70 | +0.00 | -820.70 |
| 2026-09-14 | `ORCL` | 58 | $150.28 | $141.42 | -513.88 | — | +0.00 | -513.88 | -1334.58 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 127 | — | $26.27 | +0.00 | $26.59 | +40.64 | +40.64 | +0.00 | +40.64 |
| 2026-09-16 | `QCOM` | 26 | — | $189.17 | +0.00 | $184.84 | -112.58 | -112.58 | +0.00 | -112.58 |
| 2026-09-17 | `WAY` | 127 | $26.59 | $26.51 | -10.16 | — | +0.00 | -10.16 | +30.48 | — |
| 2026-09-17 | `QCOM` | 26 | $184.84 | $190.35 | +143.26 | — | +0.00 | +143.26 | +30.68 | — |
| 2026-09-17 | `SMTC` | 19 | — | $170.85 | +0.00 | $178.19 | +139.46 | +139.46 | +0.00 | +139.46 |
| 2026-09-17 | `CLS` | 14 | — | $337.75 | +0.00 | $329.94 | -109.34 | -109.34 | +0.00 | -109.34 |
| 2026-09-18 | `SMTC` | 19 | $178.19 | $182.33 | +78.66 | — | +0.00 | +78.66 | +218.12 | — |
| 2026-09-18 | `CLS` | 14 | $329.94 | $332.06 | +29.68 | — | +0.00 | +29.68 | -79.66 | — |
| 2026-09-18 | `TH` | 163 | — | $20.91 | +0.00 | $21.19 | +45.64 | +45.64 | +0.00 | +45.64 |
| 2026-09-18 | `GME` | 223 | — | $22.90 | +0.00 | $22.64 | -57.98 | -57.98 | +0.00 | -57.98 |
| 2026-09-21 | `TH` | 163 | $21.19 | $21.65 | +74.98 | — | +0.00 | +74.98 | +120.62 | — |
| 2026-09-21 | `GME` | 223 | $22.64 | $22.78 | +31.22 | — | +0.00 | +31.22 | -26.76 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +63.08 | BHP, APA | — | $84.22 | $10,058.57 | BHP×43, APA×134 |
| 2026-08-21 | +3.25 | $84.22 | BHP×43, APA×134 | $10,165.86 | +107.29 | -86.50 | AU, AUTL | BHP, APA | $0.52 | $10,040.96 | AU×34, AUTL×2456 |
| 2026-08-24 | -5.17 | $0.52 | AU×34, AUTL×2456 | $9,992.26 | -48.70 | +0.00 | — | AU, AUTL | $9,957.99 | $9,957.99 | — |
| 2026-08-25 | +1.80 | $9,957.99 | — | $9,957.99 | -0.00 | +374.77 | AU, FCX | — | $103.51 | $10,328.45 | AU×33, FCX×77 |
| 2026-08-26 | +2.02 | $103.51 | AU×33, FCX×77 | $10,166.09 | -162.36 | -25.50 | CM | AU, FCX | $86.93 | $10,133.93 | CM×85 |
| 2026-08-27 | — | $86.93 | CM×85 | $10,182.38 | +48.45 | -246.56 | ACMR, MU | CM | $372.99 | $9,929.34 | ACMR×49, MU×6 |
| 2026-08-28 | +0.75 | $372.99 | ACMR×49, MU×6 | $9,772.96 | -156.38 | -487.47 | KEYS, SMTC | ACMR, MU | $59.50 | $9,277.11 | KEYS×12, SMTC×41 |
| 2026-08-31 | -5.85 | $59.50 | KEYS×12, SMTC×41 | $9,353.68 | +76.57 | +0.00 | — | KEYS, SMTC | $9,349.44 | $9,349.44 | — |
| 2026-09-01 | -6.30 | $9,349.44 | — | $9,349.44 | +0.00 | +0.00 | — | — | $9,349.44 | $9,349.44 | — |
| 2026-09-02 | -3.83 | $9,349.44 | — | $9,349.44 | +0.00 | +0.00 | — | — | $9,349.44 | $9,349.44 | — |
| 2026-09-03 | -0.90 | $9,349.44 | — | $9,349.44 | +0.00 | +385.08 | AVGO, DELL | — | $478.59 | $9,730.48 | AVGO×10, DELL×11 |
| 2026-09-04 | +2.25 | $478.59 | AVGO×10, DELL×11 | $9,727.17 | -3.31 | -89.77 | CRM, FRNM | AVGO, DELL | $207.38 | $9,626.65 | CRM×14, FRNM×355 |
| 2026-09-08 | -11.47 | $207.38 | CRM×14, FRNM×355 | $9,702.16 | +75.51 | +0.00 | — | CRM, FRNM | $9,695.40 | $9,695.40 | — |
| 2026-09-09 | -13.95 | $9,695.40 | — | $9,695.40 | +0.00 | +0.00 | — | — | $9,695.40 | $9,695.40 | — |
| 2026-09-10 | -13.28 | $9,695.40 | — | $9,695.40 | +0.00 | +0.00 | — | — | $9,695.40 | $9,695.40 | — |
| 2026-09-11 | +0.50 | $9,695.40 | — | $9,695.40 | +0.00 | -820.70 | ORCL | — | $156.30 | $8,872.54 | ORCL×58 |
| 2026-09-14 | -11.00 | $156.30 | ORCL×58 | $8,358.66 | -513.88 | +0.00 | — | ORCL | $8,356.42 | $8,356.42 | — |
| 2026-09-15 | -3.84 | $8,356.42 | — | $8,356.42 | +0.00 | +0.00 | — | — | $8,356.42 | $8,356.42 | — |
| 2026-09-16 | +5.30 | $8,356.42 | — | $8,356.42 | +0.00 | -71.94 | WAY, QCOM | — | $97.27 | $8,280.04 | WAY×127, QCOM×26 |
| 2026-09-17 | +7.38 | $97.27 | WAY×127, QCOM×26 | $8,413.14 | +133.10 | +30.12 | SMTC, CLS | WAY, QCOM | $429.88 | $8,434.65 | SMTC×19, CLS×14 |
| 2026-09-18 | +4.86 | $429.88 | SMTC×19, CLS×14 | $8,542.99 | +108.34 | -12.34 | TH, GME | SMTC, CLS | $18.44 | $8,521.13 | TH×163, GME×223 |
| 2026-09-21 | +12.87 | $18.44 | TH×163, GME×223 | $8,627.33 | +106.20 | +0.00 | — | TH, GME | $8,621.84 | $8,621.84 | — |

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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 43 | $91.01 | $2.12 | — | $6,084.45 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $4000.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 134 | $44.76 | $2.39 | — | $84.22 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $6000.00 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.22 | ▲ close $10,058.57 vs 09:30 $10,000.00 (session +63.08) | 16:00 close · cash $84.22 · equity $10,058.57 vs 09:30 $10,000.00 (+58.57; session marks +63.08) · 2 name(s) marked open→close (per-name table). BHP×43 09:30 $91.01 → close $93.63 +112.66; APA×134 09:30 $44.76 → close $44.39 -49.58 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.22 | ▲ 09:30 equity $10,165.86 vs yday $10,058.57 (+107.29) | 09:30 open · cash $84.22 (unchanged overnight, no fees) · equity $10,165.86 vs prior close $10,058.57 (+107.29) · 2 name(s) re-marked at the open (per-name table). BHP×43 yday $93.63 → 09:30 $95.72 +89.87; APA×134 yday $44.39 → 09:30 $44.52 +17.42 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 43 | $95.72 | $2.16 | $+198.25 | $4,198.02 | ▲ +198.25 after sell → book $10,163.70; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 134 | $44.52 | $2.46 | $-37.01 | $10,161.24 | ▼ -37.01 after sell → book $10,161.24; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 34 | $119.43 | $2.09 | — | $6,098.52 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $4064.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 2456 | $2.47 | $31.68 | — | $0.52 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $6096.74 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.52 | ▼ close $10,040.96 vs 09:30 $10,165.86 (session -86.50) | 16:00 close · cash $0.52 · equity $10,040.96 vs 09:30 $10,165.86 (-124.90; session marks -86.50) · 2 name(s) marked open→close (per-name table). AU×34 09:30 $119.43 → close $121.22 +60.86; AUTL×2456 09:30 $2.47 → close $2.41 -147.36 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.52 | ▼ 09:30 equity $9,992.26 vs yday $10,040.96 (-48.70) | 09:30 open · cash $0.52 (unchanged overnight, no fees) · equity $9,992.26 vs prior close $10,040.96 (-48.70) · 2 name(s) re-marked at the open (per-name table). AU×34 yday $121.22 → 09:30 $120.51 -24.14; AUTL×2456 yday $2.41 → 09:30 $2.40 -24.56 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 34 | $120.51 | $2.13 | $+32.49 | $4,095.73 | ▲ +32.49 after sell → book $9,990.13; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 2456 | $2.40 | $32.14 | $-235.74 | $9,957.99 | ▼ -235.74 after sell → book $9,957.99; vs 09:30 mark -32.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,957.99 | ▲ close $9,957.99 vs 09:30 $9,992.26 (session +0.00) | 16:00 close · cash $9,957.99 · no lots left · equity $9,957.99. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,957.99 | ▲ 09:30 equity $9,957.99 vs yday $9,957.99 (-0.00) | 09:30 open · cash $9,957.99 · no holdings · equity $9,957.99 vs prior close $9,957.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 33 | $118.52 | $2.09 | — | $6,044.74 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3983.20 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 77 | $77.13 | $2.22 | — | $103.51 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $5974.79 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.51 | ▲ close $10,328.45 vs 09:30 $9,957.99 (session +374.77) | 16:00 close · cash $103.51 · equity $10,328.45 vs 09:30 $9,957.99 (+370.46; session marks +374.77) · 2 name(s) marked open→close (per-name table). AU×33 09:30 $118.52 → close $123.39 +160.71; FCX×77 09:30 $77.13 → close $79.91 +214.06 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.51 | ▼ 09:30 equity $10,166.09 vs yday $10,328.45 (-162.36) | 09:30 open · cash $103.51 (unchanged overnight, no fees) · equity $10,166.09 vs prior close $10,328.45 (-162.36) · 2 name(s) re-marked at the open (per-name table). AU×33 yday $123.39 → 09:30 $119.80 -118.47; FCX×77 yday $79.91 → 09:30 $79.34 -43.89 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 33 | $119.80 | $2.13 | $+38.02 | $4,054.78 | ▲ +38.02 after sell → book $10,163.96; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 77 | $79.34 | $2.28 | $+165.67 | $10,161.68 | ▲ +165.67 after sell → book $10,161.68; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 85 | $118.50 | $2.25 | — | $86.93 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $10161.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.93 | ▼ close $10,133.93 vs 09:30 $10,166.09 (session -25.50) | 16:00 close · cash $86.93 · equity $10,133.93 vs 09:30 $10,166.09 (-32.16; session marks -25.50) · 1 name(s) marked open→close (per-name table). CM×85 09:30 $118.50 → close $118.20 -25.50 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.93 | ▲ 09:30 equity $10,182.38 vs yday $10,133.93 (+48.45) | 09:30 open · cash $86.93 (unchanged overnight, no fees) · equity $10,182.38 vs prior close $10,133.93 (+48.45) · 1 name(s) re-marked at the open (per-name table). CM×85 yday $118.20 → 09:30 $118.77 +48.45 | — |
| 2026-08-27 09:30 ET | **SELL** | `CM` | 85 | $118.77 | $2.34 | $+18.37 | $10,180.04 | ▲ +18.37 after sell → book $10,180.04; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 49 | $81.65 | $2.14 | — | $6,177.05 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $4072.02 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 6 | $967.01 | $2.01 | — | $372.99 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $6108.02 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $372.99 | ▼ close $9,929.34 vs 09:30 $10,182.38 (session -246.56) | 16:00 close · cash $372.99 · equity $9,929.34 vs 09:30 $10,182.38 (-253.04; session marks -246.56) · 2 name(s) marked open→close (per-name table). ACMR×49 09:30 $81.65 → close $80.49 -56.84; MU×6 09:30 $967.01 → close $935.39 -189.72 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $372.99 | ▼ 09:30 equity $9,772.96 vs yday $9,929.34 (-156.38) | 09:30 open · cash $372.99 (unchanged overnight, no fees) · equity $9,772.96 vs prior close $9,929.34 (-156.38) · 2 name(s) re-marked at the open (per-name table). ACMR×49 yday $80.49 → 09:30 $79.27 -59.78; MU×6 yday $935.39 → 09:30 $919.29 -96.60 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 49 | $79.27 | $2.18 | $-120.94 | $4,255.04 | ▼ -120.94 after sell → book $9,770.78; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 6 | $919.29 | $2.06 | $-290.39 | $9,768.72 | ▼ -290.39 after sell → book $9,768.72; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 12 | $324.41 | $2.03 | — | $5,873.77 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $3907.49 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 41 | $141.76 | $2.11 | — | $59.50 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $5861.23 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.50 | ▼ close $9,277.11 vs 09:30 $9,772.96 (session -487.47) | 16:00 close · cash $59.50 · equity $9,277.11 vs 09:30 $9,772.96 (-495.85; session marks -487.47) · 2 name(s) marked open→close (per-name table). KEYS×12 09:30 $324.41 → close $319.97 -53.28; SMTC×41 09:30 $141.76 → close $131.17 -434.19 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.50 | ▲ 09:30 equity $9,353.68 vs yday $9,277.11 (+76.57) | 09:30 open · cash $59.50 (unchanged overnight, no fees) · equity $9,353.68 vs prior close $9,277.11 (+76.57) · 2 name(s) re-marked at the open (per-name table). KEYS×12 yday $319.97 → 09:30 $322.49 +30.24; SMTC×41 yday $131.17 → 09:30 $132.30 +46.33 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 12 | $322.49 | $2.07 | $-27.13 | $3,927.31 | ▼ -27.13 after sell → book $9,351.61; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 41 | $132.30 | $2.17 | $-392.14 | $9,349.44 | ▼ -392.14 after sell → book $9,349.44; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,349.44 | ▲ close $9,349.44 vs 09:30 $9,353.68 (session +0.00) | 16:00 close · cash $9,349.44 · no lots left · equity $9,349.44. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,349.44 | ▲ 09:30 equity $9,349.44 vs yday $9,349.44 (+0.00) | 09:30 open · cash $9,349.44 · no holdings · equity $9,349.44 vs prior close $9,349.44 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,349.44 | ▲ close $9,349.44 vs 09:30 $9,349.44 (session +0.00) | 16:00 close · cash $9,349.44 · no lots left · equity $9,349.44. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,349.44 | ▲ 09:30 equity $9,349.44 vs yday $9,349.44 (+0.00) | 09:30 open · cash $9,349.44 · no holdings · equity $9,349.44 vs prior close $9,349.44 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,349.44 | ▲ close $9,349.44 vs 09:30 $9,349.44 (session +0.00) | 16:00 close · cash $9,349.44 · no lots left · equity $9,349.44. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,349.44 | ▲ 09:30 equity $9,349.44 vs yday $9,349.44 (+0.00) | 09:30 open · cash $9,349.44 · no holdings · equity $9,349.44 vs prior close $9,349.44 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 10 | $351.74 | $2.02 | — | $5,830.02 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $3739.78 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 11 | $486.31 | $2.02 | — | $478.59 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $5609.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $478.59 | ▲ close $9,730.48 vs 09:30 $9,349.44 (session +385.08) | 16:00 close · cash $478.59 · equity $9,730.48 vs 09:30 $9,349.44 (+381.04; session marks +385.08) · 2 name(s) marked open→close (per-name table). AVGO×10 09:30 $351.74 → close $357.16 +54.20; DELL×11 09:30 $486.31 → close $516.39 +330.88 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $478.59 | ▼ 09:30 equity $9,727.17 vs yday $9,730.48 (-3.31) | 09:30 open · cash $478.59 (unchanged overnight, no fees) · equity $9,727.17 vs prior close $9,730.48 (-3.31) · 2 name(s) re-marked at the open (per-name table). AVGO×10 yday $357.16 → 09:30 $359.70 +25.40; DELL×11 yday $516.39 → 09:30 $513.78 -28.71 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 10 | $359.70 | $2.06 | $+75.52 | $4,073.53 | ▲ +75.52 after sell → book $9,725.11; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 11 | $513.78 | $2.08 | $+298.07 | $9,723.03 | ▲ +298.07 after sell → book $9,723.03; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 14 | $263.36 | $2.03 | — | $6,033.96 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $3889.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 355 | $16.40 | $4.58 | — | $207.38 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $5833.82 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $207.38 | ▼ close $9,626.65 vs 09:30 $9,727.17 (session -89.77) | 16:00 close · cash $207.38 · equity $9,626.65 vs 09:30 $9,727.17 (-100.52; session marks -89.77) · 2 name(s) marked open→close (per-name table). CRM×14 09:30 $263.36 → close $259.23 -57.82; FRNM×355 09:30 $16.40 → close $16.31 -31.95 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $207.38 | ▲ 09:30 equity $9,702.16 vs yday $9,626.65 (+75.51) | 09:30 open · cash $207.38 (unchanged overnight, no fees) · equity $9,702.16 vs prior close $9,626.65 (+75.51) · 2 name(s) re-marked at the open (per-name table). CRM×14 yday $259.23 → 09:30 $253.72 -77.14; FRNM×355 yday $16.31 → 09:30 $16.74 +152.65 | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 14 | $253.72 | $2.07 | $-139.06 | $3,757.39 | ▼ -139.06 after sell → book $9,700.09; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 355 | $16.74 | $4.69 | $+111.43 | $9,695.40 | ▲ +111.43 after sell → book $9,695.40; vs 09:30 mark -4.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,695.40 | ▲ close $9,695.40 vs 09:30 $9,702.16 (session +0.00) | 16:00 close · cash $9,695.40 · no lots left · equity $9,695.40. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,695.40 | ▲ 09:30 equity $9,695.40 vs yday $9,695.40 (+0.00) | 09:30 open · cash $9,695.40 · no holdings · equity $9,695.40 vs prior close $9,695.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,695.40 | ▲ close $9,695.40 vs 09:30 $9,695.40 (session +0.00) | 16:00 close · cash $9,695.40 · no lots left · equity $9,695.40. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,695.40 | ▲ 09:30 equity $9,695.40 vs yday $9,695.40 (+0.00) | 09:30 open · cash $9,695.40 · no holdings · equity $9,695.40 vs prior close $9,695.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,695.40 | ▲ close $9,695.40 vs 09:30 $9,695.40 (session +0.00) | 16:00 close · cash $9,695.40 · no lots left · equity $9,695.40. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,695.40 | ▲ 09:30 equity $9,695.40 vs yday $9,695.40 (+0.00) | 09:30 open · cash $9,695.40 · no holdings · equity $9,695.40 vs prior close $9,695.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 58 | $164.43 | $2.16 | — | $156.30 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $9695.40 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.30 | ▼ close $8,872.54 vs 09:30 $9,695.40 (session -820.70) | 16:00 close · cash $156.30 · equity $8,872.54 vs 09:30 $9,695.40 (-822.86; session marks -820.70) · 1 name(s) marked open→close (per-name table). ORCL×58 09:30 $164.43 → close $150.28 -820.70 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.30 | ▼ 09:30 equity $8,358.66 vs yday $8,872.54 (-513.88) | 09:30 open · cash $156.30 (unchanged overnight, no fees) · equity $8,358.66 vs prior close $8,872.54 (-513.88) · 1 name(s) re-marked at the open (per-name table). ORCL×58 yday $150.28 → 09:30 $141.42 -513.88 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 58 | $141.42 | $2.24 | $-1338.98 | $8,356.42 | ▼ -1,338.98 after sell → book $8,356.42; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,356.42 | ▲ close $8,356.42 vs 09:30 $8,358.66 (session +0.00) | 16:00 close · cash $8,356.42 · no lots left · equity $8,356.42. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,356.42 | ▲ 09:30 equity $8,356.42 vs yday $8,356.42 (+0.00) | 09:30 open · cash $8,356.42 · no holdings · equity $8,356.42 vs prior close $8,356.42 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,356.42 | ▲ close $8,356.42 vs 09:30 $8,356.42 (session +0.00) | 16:00 close · cash $8,356.42 · no lots left · equity $8,356.42. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,356.42 | ▲ 09:30 equity $8,356.42 vs yday $8,356.42 (+0.00) | 09:30 open · cash $8,356.42 · no holdings · equity $8,356.42 vs prior close $8,356.42 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 127 | $26.27 | $2.37 | — | $5,017.76 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $3342.57 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 26 | $189.17 | $2.07 | — | $97.27 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $5013.85 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.27 | ▼ close $8,280.04 vs 09:30 $8,356.42 (session -71.94) | 16:00 close · cash $97.27 · equity $8,280.04 vs 09:30 $8,356.42 (-76.38; session marks -71.94) · 2 name(s) marked open→close (per-name table). WAY×127 09:30 $26.27 → close $26.59 +40.64; QCOM×26 09:30 $189.17 → close $184.84 -112.58 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.27 | ▲ 09:30 equity $8,413.14 vs yday $8,280.04 (+133.10) | 09:30 open · cash $97.27 (unchanged overnight, no fees) · equity $8,413.14 vs prior close $8,280.04 (+133.10) · 2 name(s) re-marked at the open (per-name table). WAY×127 yday $26.59 → 09:30 $26.51 -10.16; QCOM×26 yday $184.84 → 09:30 $190.35 +143.26 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 127 | $26.51 | $2.42 | $+25.69 | $3,461.62 | ▲ +25.69 after sell → book $8,410.72; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 26 | $190.35 | $2.12 | $+26.49 | $8,408.61 | ▲ +26.49 after sell → book $8,408.61; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 19 | $170.85 | $2.05 | — | $5,160.41 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $3363.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CLS` | 14 | $337.75 | $2.03 | — | $429.88 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+10.2; leftover $5045.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $429.88 | ▲ close $8,434.65 vs 09:30 $8,413.14 (session +30.12) | 16:00 close · cash $429.88 · equity $8,434.65 vs 09:30 $8,413.14 (+21.51; session marks +30.12) · 2 name(s) marked open→close (per-name table). SMTC×19 09:30 $170.85 → close $178.19 +139.46; CLS×14 09:30 $337.75 → close $329.94 -109.34 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $429.88 | ▲ 09:30 equity $8,542.99 vs yday $8,434.65 (+108.34) | 09:30 open · cash $429.88 (unchanged overnight, no fees) · equity $8,542.99 vs prior close $8,434.65 (+108.34) · 2 name(s) re-marked at the open (per-name table). SMTC×19 yday $178.19 → 09:30 $182.33 +78.66; CLS×14 yday $329.94 → 09:30 $332.06 +29.68 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 19 | $182.33 | $2.08 | $+213.99 | $3,892.06 | ▲ +213.99 after sell → book $8,540.90; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `CLS` | 14 | $332.06 | $2.08 | $-83.77 | $8,538.82 | ▼ -83.77 after sell → book $8,538.82; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 163 | $20.91 | $2.48 | — | $5,128.01 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $3415.53 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 223 | $22.90 | $2.88 | — | $18.44 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $5123.29 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.44 | ▼ close $8,521.13 vs 09:30 $8,542.99 (session -12.34) | 16:00 close · cash $18.44 · equity $8,521.13 vs 09:30 $8,542.99 (-21.86; session marks -12.34) · 2 name(s) marked open→close (per-name table). TH×163 09:30 $20.91 → close $21.19 +45.64; GME×223 09:30 $22.90 → close $22.64 -57.98 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.44 | ▲ 09:30 equity $8,627.33 vs yday $8,521.13 (+106.20) | 09:30 open · cash $18.44 (unchanged overnight, no fees) · equity $8,627.33 vs prior close $8,521.13 (+106.20) · 2 name(s) re-marked at the open (per-name table). TH×163 yday $21.19 → 09:30 $21.65 +74.98; GME×223 yday $22.64 → 09:30 $22.78 +31.22 | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 163 | $21.65 | $2.53 | $+115.61 | $3,544.85 | ▲ +115.61 after sell → book $8,624.79; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 223 | $22.78 | $2.95 | $-32.59 | $8,621.84 | ▼ -32.59 after sell → book $8,621.84; vs 09:30 mark -2.95 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,621.84 | ▲ close $8,621.84 vs 09:30 $8,627.33 (session +0.00) | 16:00 close · cash $8,621.84 · no lots left · equity $8,621.84. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
