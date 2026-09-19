# Factor mine action — `union_news_g_cam71_n2_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 2 · rank `cond` · size `topheavy` · sell `list` · S-boost `none` · topheavy leftover on news🟢 +7 −≤1

Cash book **-3.16%** ($9,684) · signal-only (no cash/fees) was -2.33%. Starts YES **4/26**. Fills 40 · skips 2 · realized $-295.56.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $13.66.

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
| 2026-08-26 | `ASST` | 490 | — | $20.72 | +0.00 | $21.50 | +382.20 | +382.20 | +0.00 | +382.20 |
| 2026-08-27 | `ASST` | 490 | $21.50 | $22.45 | +465.50 | — | +0.00 | +465.50 | +847.70 | — |
| 2026-08-27 | `ACMR` | 53 | — | $81.65 | +0.00 | $80.49 | -61.48 | -61.48 | +0.00 | -61.48 |
| 2026-08-27 | `MU` | 6 | — | $967.01 | +0.00 | $935.39 | -189.72 | -189.72 | +0.00 | -189.72 |
| 2026-08-28 | `ACMR` | 53 | $80.49 | $79.27 | -64.66 | — | +0.00 | -64.66 | -126.14 | — |
| 2026-08-28 | `MU` | 6 | $935.39 | $919.29 | -96.60 | — | +0.00 | -96.60 | -286.32 | — |
| 2026-08-28 | `KEYS` | 13 | — | $324.41 | +0.00 | $319.97 | -57.72 | -57.72 | +0.00 | -57.72 |
| 2026-08-28 | `SMTC` | 44 | — | $141.76 | +0.00 | $131.17 | -465.96 | -465.96 | +0.00 | -465.96 |
| 2026-08-31 | `KEYS` | 13 | $319.97 | $322.49 | +32.76 | — | +0.00 | +32.76 | -24.96 | — |
| 2026-08-31 | `SMTC` | 44 | $131.17 | $132.30 | +49.72 | — | +0.00 | +49.72 | -416.24 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 11 | — | $351.74 | +0.00 | $357.16 | +59.62 | +59.62 | +0.00 | +59.62 |
| 2026-09-03 | `DELL` | 12 | — | $486.31 | +0.00 | $516.39 | +360.96 | +360.96 | +0.00 | +360.96 |
| 2026-09-04 | `AVGO` | 11 | $357.16 | $359.70 | +27.94 | — | +0.00 | +27.94 | +87.56 | — |
| 2026-09-04 | `DELL` | 12 | $516.39 | $513.78 | -31.32 | — | +0.00 | -31.32 | +329.64 | — |
| 2026-09-04 | `CRM` | 16 | — | $263.36 | +0.00 | $259.23 | -66.08 | -66.08 | +0.00 | -66.08 |
| 2026-09-04 | `FRNM` | 385 | — | $16.40 | +0.00 | $16.31 | -34.65 | -34.65 | +0.00 | -34.65 |
| 2026-09-08 | `CRM` | 16 | $259.23 | $253.72 | -88.16 | — | +0.00 | -88.16 | -154.24 | — |
| 2026-09-08 | `FRNM` | 385 | $16.31 | $16.74 | +165.55 | — | +0.00 | +165.55 | +130.90 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 63 | — | $164.43 | +0.00 | $150.28 | -891.45 | -891.45 | +0.00 | -891.45 |
| 2026-09-14 | `ORCL` | 63 | $150.28 | $141.42 | -558.18 | — | +0.00 | -558.18 | -1449.63 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 137 | — | $26.27 | +0.00 | $26.59 | +43.84 | +43.84 | +0.00 | +43.84 |
| 2026-09-16 | `QCOM` | 28 | — | $189.17 | +0.00 | $184.84 | -121.24 | -121.24 | +0.00 | -121.24 |
| 2026-09-17 | `WAY` | 137 | $26.59 | $26.51 | -10.96 | — | +0.00 | -10.96 | +32.88 | — |
| 2026-09-17 | `QCOM` | 28 | $184.84 | $190.35 | +154.28 | — | +0.00 | +154.28 | +33.04 | — |
| 2026-09-17 | `SMTC` | 53 | — | $170.85 | +0.00 | $178.19 | +389.02 | +389.02 | +0.00 | +389.02 |
| 2026-09-18 | `SMTC` | 53 | $178.19 | $182.33 | +219.42 | — | +0.00 | +219.42 | +608.44 | — |
| 2026-09-18 | `TH` | 185 | — | $20.91 | +0.00 | $21.19 | +51.80 | +51.80 | +0.00 | +51.80 |
| 2026-09-18 | `GME` | 254 | — | $22.90 | +0.00 | $22.64 | -66.04 | -66.04 | +0.00 | -66.04 |

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
| 2026-08-26 | +2.02 | $103.51 | AU×33, FCX×77 | $10,166.09 | -162.36 | +382.20 | ASST | AU, FCX | $2.55 | $10,537.55 | ASST×490 |
| 2026-08-27 | — | $2.55 | ASST×490 | $11,003.05 | +465.50 | -251.20 | ACMR, MU | ASST | $862.90 | $10,741.21 | ACMR×53, MU×6 |
| 2026-08-28 | +0.75 | $862.90 | ACMR×53, MU×6 | $10,579.95 | -161.26 | -523.68 | KEYS, SMTC | ACMR, MU | $116.77 | $10,047.86 | KEYS×13, SMTC×44 |
| 2026-08-31 | -5.85 | $116.77 | KEYS×13, SMTC×44 | $10,130.34 | +82.48 | +0.00 | — | KEYS, SMTC | $10,126.09 | $10,126.09 | — |
| 2026-09-01 | -6.30 | $10,126.09 | — | $10,126.09 | +0.00 | +0.00 | — | — | $10,126.09 | $10,126.09 | — |
| 2026-09-02 | -3.83 | $10,126.09 | — | $10,126.09 | +0.00 | +0.00 | — | — | $10,126.09 | $10,126.09 | — |
| 2026-09-03 | -0.90 | $10,126.09 | — | $10,126.09 | +0.00 | +420.58 | AVGO, DELL | — | $417.18 | $10,542.62 | AVGO×11, DELL×12 |
| 2026-09-04 | +2.25 | $417.18 | AVGO×11, DELL×12 | $10,539.24 | -3.38 | -100.73 | CRM, FRNM | AVGO, DELL | $0.33 | $10,427.36 | CRM×16, FRNM×385 |
| 2026-09-08 | -11.47 | $0.33 | CRM×16, FRNM×385 | $10,504.75 | +77.39 | +0.00 | — | CRM, FRNM | $10,497.58 | $10,497.58 | — |
| 2026-09-09 | -13.95 | $10,497.58 | — | $10,497.58 | +0.00 | +0.00 | — | — | $10,497.58 | $10,497.58 | — |
| 2026-09-10 | -13.28 | $10,497.58 | — | $10,497.58 | +0.00 | +0.00 | — | — | $10,497.58 | $10,497.58 | — |
| 2026-09-11 | +0.50 | $10,497.58 | — | $10,497.58 | +0.00 | -891.45 | ORCL | — | $136.32 | $9,603.96 | ORCL×63 |
| 2026-09-14 | -11.00 | $136.32 | ORCL×63 | $9,045.78 | -558.18 | +0.00 | — | ORCL | $9,043.51 | $9,043.51 | — |
| 2026-09-15 | -3.84 | $9,043.51 | — | $9,043.51 | +0.00 | +0.00 | — | — | $9,043.51 | $9,043.51 | — |
| 2026-09-16 | +5.30 | $9,043.51 | — | $9,043.51 | +0.00 | -77.40 | WAY, QCOM | — | $143.29 | $8,961.64 | WAY×137, QCOM×28 |
| 2026-09-17 | +7.38 | $143.29 | WAY×137, QCOM×28 | $9,104.96 | +143.32 | +389.02 | SMTC | WAY, QCOM | $43.18 | $9,487.25 | SMTC×53 |
| 2026-09-18 | +4.86 | $43.18 | SMTC×53 | $9,706.67 | +219.42 | -14.24 | TH, GME | SMTC | $13.66 | $9,684.37 | TH×185, GME×254 |

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
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 490 | $20.72 | $6.32 | — | $2.55 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list oppset; 🔵; ret5=+67.1; leftover $10161.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.55 | ▲ close $10,537.55 vs 09:30 $10,166.09 (session +382.20) | 16:00 close · cash $2.55 · equity $10,537.55 vs 09:30 $10,166.09 (+371.46; session marks +382.20) · 1 name(s) marked open→close (per-name table). ASST×490 09:30 $20.72 → close $21.50 +382.20 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.55 | ▲ 09:30 equity $11,003.05 vs yday $10,537.55 (+465.50) | 09:30 open · cash $2.55 (unchanged overnight, no fees) · equity $11,003.05 vs prior close $10,537.55 (+465.50) · 1 name(s) re-marked at the open (per-name table). ASST×490 yday $21.50 → 09:30 $22.45 +465.50 | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 490 | $22.45 | $6.49 | $+834.89 | $10,996.56 | ▲ +834.89 after sell → book $10,996.56; vs 09:30 mark -6.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 53 | $81.65 | $2.15 | — | $6,666.96 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $4398.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 6 | $967.01 | $2.01 | — | $862.90 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $6597.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $862.90 | ▼ close $10,741.21 vs 09:30 $11,003.05 (session -251.20) | 16:00 close · cash $862.90 · equity $10,741.21 vs 09:30 $11,003.05 (-261.84; session marks -251.20) · 2 name(s) marked open→close (per-name table). ACMR×53 09:30 $81.65 → close $80.49 -61.48; MU×6 09:30 $967.01 → close $935.39 -189.72 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $862.90 | ▼ 09:30 equity $10,579.95 vs yday $10,741.21 (-161.26) | 09:30 open · cash $862.90 (unchanged overnight, no fees) · equity $10,579.95 vs prior close $10,741.21 (-161.26) · 2 name(s) re-marked at the open (per-name table). ACMR×53 yday $80.49 → 09:30 $79.27 -64.66; MU×6 yday $935.39 → 09:30 $919.29 -96.60 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 53 | $79.27 | $2.19 | $-130.48 | $5,062.01 | ▼ -130.48 after sell → book $10,577.75; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 6 | $919.29 | $2.06 | $-290.39 | $10,575.69 | ▼ -290.39 after sell → book $10,575.69; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 13 | $324.41 | $2.03 | — | $6,356.33 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $4230.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 44 | $141.76 | $2.12 | — | $116.77 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $6345.42 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.77 | ▼ close $10,047.86 vs 09:30 $10,579.95 (session -523.68) | 16:00 close · cash $116.77 · equity $10,047.86 vs 09:30 $10,579.95 (-532.09; session marks -523.68) · 2 name(s) marked open→close (per-name table). KEYS×13 09:30 $324.41 → close $319.97 -57.72; SMTC×44 09:30 $141.76 → close $131.17 -465.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.77 | ▲ 09:30 equity $10,130.34 vs yday $10,047.86 (+82.48) | 09:30 open · cash $116.77 (unchanged overnight, no fees) · equity $10,130.34 vs prior close $10,047.86 (+82.48) · 2 name(s) re-marked at the open (per-name table). KEYS×13 yday $319.97 → 09:30 $322.49 +32.76; SMTC×44 yday $131.17 → 09:30 $132.30 +49.72 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 13 | $322.49 | $2.07 | $-29.06 | $4,307.07 | ▼ -29.06 after sell → book $10,128.27; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 44 | $132.30 | $2.18 | $-420.54 | $10,126.09 | ▼ -420.54 after sell → book $10,126.09; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,126.09 | ▲ close $10,126.09 vs 09:30 $10,130.34 (session +0.00) | 16:00 close · cash $10,126.09 · no lots left · equity $10,126.09. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,126.09 | ▲ 09:30 equity $10,126.09 vs yday $10,126.09 (+0.00) | 09:30 open · cash $10,126.09 · no holdings · equity $10,126.09 vs prior close $10,126.09 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,126.09 | ▲ close $10,126.09 vs 09:30 $10,126.09 (session +0.00) | 16:00 close · cash $10,126.09 · no lots left · equity $10,126.09. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,126.09 | ▲ 09:30 equity $10,126.09 vs yday $10,126.09 (+0.00) | 09:30 open · cash $10,126.09 · no holdings · equity $10,126.09 vs prior close $10,126.09 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,126.09 | ▲ close $10,126.09 vs 09:30 $10,126.09 (session +0.00) | 16:00 close · cash $10,126.09 · no lots left · equity $10,126.09. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,126.09 | ▲ 09:30 equity $10,126.09 vs yday $10,126.09 (+0.00) | 09:30 open · cash $10,126.09 · no holdings · equity $10,126.09 vs prior close $10,126.09 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 11 | $351.74 | $2.02 | — | $6,254.93 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $4050.44 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 12 | $486.31 | $2.03 | — | $417.18 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy,oppset; 🔵; ret5=+6.1; leftover $6075.65 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $417.18 | ▲ close $10,542.62 vs 09:30 $10,126.09 (session +420.58) | 16:00 close · cash $417.18 · equity $10,542.62 vs 09:30 $10,126.09 (+416.53; session marks +420.58) · 2 name(s) marked open→close (per-name table). AVGO×11 09:30 $351.74 → close $357.16 +59.62; DELL×12 09:30 $486.31 → close $516.39 +360.96 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $417.18 | ▼ 09:30 equity $10,539.24 vs yday $10,542.62 (-3.38) | 09:30 open · cash $417.18 (unchanged overnight, no fees) · equity $10,539.24 vs prior close $10,542.62 (-3.38) · 2 name(s) re-marked at the open (per-name table). AVGO×11 yday $357.16 → 09:30 $359.70 +27.94; DELL×12 yday $516.39 → 09:30 $513.78 -31.32 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 11 | $359.70 | $2.06 | $+83.47 | $4,371.82 | ▲ +83.47 after sell → book $10,537.18; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 12 | $513.78 | $2.09 | $+325.53 | $10,535.09 | ▲ +325.53 after sell → book $10,535.09; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 16 | $263.36 | $2.04 | — | $6,319.29 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $4214.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 385 | $16.40 | $4.97 | — | $0.33 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $6321.05 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.33 | ▼ close $10,427.36 vs 09:30 $10,539.24 (session -100.73) | 16:00 close · cash $0.33 · equity $10,427.36 vs 09:30 $10,539.24 (-111.88; session marks -100.73) · 2 name(s) marked open→close (per-name table). CRM×16 09:30 $263.36 → close $259.23 -66.08; FRNM×385 09:30 $16.40 → close $16.31 -34.65 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.33 | ▲ 09:30 equity $10,504.75 vs yday $10,427.36 (+77.39) | 09:30 open · cash $0.33 (unchanged overnight, no fees) · equity $10,504.75 vs prior close $10,427.36 (+77.39) · 2 name(s) re-marked at the open (per-name table). CRM×16 yday $259.23 → 09:30 $253.72 -88.16; FRNM×385 yday $16.31 → 09:30 $16.74 +165.55 | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 16 | $253.72 | $2.08 | $-158.36 | $4,057.77 | ▼ -158.36 after sell → book $10,502.67; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 385 | $16.74 | $5.08 | $+120.85 | $10,497.58 | ▲ +120.85 after sell → book $10,497.58; vs 09:30 mark -5.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,497.58 | ▲ close $10,497.58 vs 09:30 $10,504.75 (session +0.00) | 16:00 close · cash $10,497.58 · no lots left · equity $10,497.58. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,497.58 | ▲ 09:30 equity $10,497.58 vs yday $10,497.58 (+0.00) | 09:30 open · cash $10,497.58 · no holdings · equity $10,497.58 vs prior close $10,497.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,497.58 | ▲ close $10,497.58 vs 09:30 $10,497.58 (session +0.00) | 16:00 close · cash $10,497.58 · no lots left · equity $10,497.58. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,497.58 | ▲ 09:30 equity $10,497.58 vs yday $10,497.58 (+0.00) | 09:30 open · cash $10,497.58 · no holdings · equity $10,497.58 vs prior close $10,497.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,497.58 | ▲ close $10,497.58 vs 09:30 $10,497.58 (session +0.00) | 16:00 close · cash $10,497.58 · no lots left · equity $10,497.58. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,497.58 | ▲ 09:30 equity $10,497.58 vs yday $10,497.58 (+0.00) | 09:30 open · cash $10,497.58 · no holdings · equity $10,497.58 vs prior close $10,497.58 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 63 | $164.43 | $2.18 | — | $136.32 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10497.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $136.32 | ▼ close $9,603.96 vs 09:30 $10,497.58 (session -891.45) | 16:00 close · cash $136.32 · equity $9,603.96 vs 09:30 $10,497.58 (-893.62; session marks -891.45) · 1 name(s) marked open→close (per-name table). ORCL×63 09:30 $164.43 → close $150.28 -891.45 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.32 | ▼ 09:30 equity $9,045.78 vs yday $9,603.96 (-558.18) | 09:30 open · cash $136.32 (unchanged overnight, no fees) · equity $9,045.78 vs prior close $9,603.96 (-558.18) · 1 name(s) re-marked at the open (per-name table). ORCL×63 yday $150.28 → 09:30 $141.42 -558.18 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 63 | $141.42 | $2.26 | $-1454.07 | $9,043.51 | ▼ -1,454.07 after sell → book $9,043.51; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,043.51 | ▲ close $9,043.51 vs 09:30 $9,045.78 (session +0.00) | 16:00 close · cash $9,043.51 · no lots left · equity $9,043.51. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,043.51 | ▲ 09:30 equity $9,043.51 vs yday $9,043.51 (+0.00) | 09:30 open · cash $9,043.51 · no holdings · equity $9,043.51 vs prior close $9,043.51 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,043.51 | ▲ close $9,043.51 vs 09:30 $9,043.51 (session +0.00) | 16:00 close · cash $9,043.51 · no lots left · equity $9,043.51. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,043.51 | ▲ 09:30 equity $9,043.51 vs yday $9,043.51 (+0.00) | 09:30 open · cash $9,043.51 · no holdings · equity $9,043.51 vs prior close $9,043.51 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 137 | $26.27 | $2.40 | — | $5,442.12 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $3617.41 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 28 | $189.17 | $2.07 | — | $143.29 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+2.1; leftover $5426.11 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $143.29 | ▼ close $8,961.64 vs 09:30 $9,043.51 (session -77.40) | 16:00 close · cash $143.29 · equity $8,961.64 vs 09:30 $9,043.51 (-81.87; session marks -77.40) · 2 name(s) marked open→close (per-name table). WAY×137 09:30 $26.27 → close $26.59 +43.84; QCOM×28 09:30 $189.17 → close $184.84 -121.24 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $143.29 | ▲ 09:30 equity $9,104.96 vs yday $8,961.64 (+143.32) | 09:30 open · cash $143.29 (unchanged overnight, no fees) · equity $9,104.96 vs prior close $8,961.64 (+143.32) · 2 name(s) re-marked at the open (per-name table). WAY×137 yday $26.59 → 09:30 $26.51 -10.96; QCOM×28 yday $184.84 → 09:30 $190.35 +154.28 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 137 | $26.51 | $2.45 | $+28.03 | $3,772.71 | ▲ +28.03 after sell → book $9,102.51; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 28 | $190.35 | $2.13 | $+28.84 | $9,100.38 | ▲ +28.84 after sell → book $9,100.38; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 53 | $170.85 | $2.15 | — | $43.18 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $9100.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.18 | ▲ close $9,487.25 vs 09:30 $9,104.96 (session +389.02) | 16:00 close · cash $43.18 · equity $9,487.25 vs 09:30 $9,104.96 (+382.29; session marks +389.02) · 1 name(s) marked open→close (per-name table). SMTC×53 09:30 $170.85 → close $178.19 +389.02 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.18 | ▲ 09:30 equity $9,706.67 vs yday $9,487.25 (+219.42) | 09:30 open · cash $43.18 (unchanged overnight, no fees) · equity $9,706.67 vs prior close $9,487.25 (+219.42) · 1 name(s) re-marked at the open (per-name table). SMTC×53 yday $178.19 → 09:30 $182.33 +219.42 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 53 | $182.33 | $2.24 | $+604.05 | $9,704.43 | ▲ +604.05 after sell → book $9,704.43; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 185 | $20.91 | $2.54 | — | $5,833.54 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $3881.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 254 | $22.90 | $3.28 | — | $13.66 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $5822.66 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.66 | ▼ close $9,684.37 vs 09:30 $9,706.67 (session -14.24) | 16:00 close · cash $13.66 · equity $9,684.37 vs 09:30 $9,706.67 (-22.30; session marks -14.24) · 2 name(s) marked open→close (per-name table). TH×185 09:30 $20.91 → close $21.19 +51.80; GME×254 09:30 $22.90 → close $22.64 -66.04 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TH` | 185 | 2026-09-18 @ $20.91 | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $3881.77 |
| `GME` | 254 | 2026-09-18 @ $22.90 | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $5822.66 |
