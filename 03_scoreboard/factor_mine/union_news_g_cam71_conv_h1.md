# Factor mine action — `union_news_g_cam71_conv_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `conviction` · sell `list` · S-boost `none` · news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5

Cash book **-3.66%** ($9,634) · signal-only (no cash/fees) was -9.24%. Starts YES **6/28**. Fills 54 · skips 6 · realized $-365.91.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,634.10.

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
| 2026-08-26 | `CM` | 88 | — | $118.50 | +0.00 | $118.20 | -26.40 | -26.40 | +0.00 | -26.40 |
| 2026-08-27 | `CM` | 88 | $118.20 | $118.77 | +50.16 | $114.84 | -345.84 | -295.68 | +23.76 | -322.08 |
| 2026-08-28 | `CM` | 88 | $114.84 | $115.66 | +72.16 | — | +0.00 | +72.16 | -249.92 | — |
| 2026-08-28 | `KEYS` | 22 | — | $324.41 | +0.00 | $319.97 | -97.68 | -97.68 | +0.00 | -97.68 |
| 2026-08-28 | `SMTC` | 7 | — | $141.76 | +0.00 | $131.17 | -74.13 | -74.13 | +0.00 | -74.13 |
| 2026-08-28 | `CIEN` | 2 | — | $400.42 | +0.00 | $378.44 | -43.96 | -43.96 | +0.00 | -43.96 |
| 2026-08-31 | `KEYS` | 22 | $319.97 | $322.49 | +55.44 | — | +0.00 | +55.44 | -42.24 | — |
| 2026-08-31 | `SMTC` | 7 | $131.17 | $132.30 | +7.91 | — | +0.00 | +7.91 | -66.22 | — |
| 2026-08-31 | `CIEN` | 2 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -43.96 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 20 | — | $351.74 | +0.00 | $357.16 | +108.40 | +108.40 | +0.00 | +108.40 |
| 2026-09-03 | `DELL` | 6 | — | $486.31 | +0.00 | $516.39 | +180.48 | +180.48 | +0.00 | +180.48 |
| 2026-09-04 | `AVGO` | 20 | $357.16 | $359.70 | +50.80 | — | +0.00 | +50.80 | +159.20 | — |
| 2026-09-04 | `DELL` | 6 | $516.39 | $513.78 | -15.66 | — | +0.00 | -15.66 | +164.82 | — |
| 2026-09-04 | `CRM` | 27 | — | $263.36 | +0.00 | $259.23 | -111.51 | -111.51 | +0.00 | -111.51 |
| 2026-09-04 | `FRNM` | 95 | — | $16.40 | +0.00 | $16.31 | -8.55 | -8.55 | +0.00 | -8.55 |
| 2026-09-04 | `MRX` | 20 | — | $75.65 | +0.00 | $78.27 | +52.40 | +52.40 | +0.00 | +52.40 |
| 2026-09-08 | `CRM` | 27 | $259.23 | $253.72 | -148.77 | — | +0.00 | -148.77 | -260.28 | — |
| 2026-09-08 | `FRNM` | 95 | $16.31 | $16.74 | +40.85 | — | +0.00 | +40.85 | +32.30 | — |
| 2026-09-08 | `MRX` | 20 | $78.27 | $78.84 | +11.40 | — | +0.00 | +11.40 | +63.80 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 62 | — | $164.43 | +0.00 | $150.28 | -877.30 | -877.30 | +0.00 | -877.30 |
| 2026-09-14 | `ORCL` | 62 | $150.28 | $141.42 | -549.32 | — | +0.00 | -549.32 | -1426.62 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `WAY` | 235 | — | $26.27 | +0.00 | $26.59 | +75.20 | +75.20 | +0.00 | +75.20 |
| 2026-09-16 | `QCOM` | 6 | — | $189.17 | +0.00 | $184.84 | -25.98 | -25.98 | +0.00 | -25.98 |
| 2026-09-16 | `SM` | 33 | — | $39.99 | +0.00 | $38.16 | -60.39 | -60.39 | +0.00 | -60.39 |
| 2026-09-17 | `WAY` | 235 | $26.59 | $26.51 | -18.80 | — | +0.00 | -18.80 | +56.40 | — |
| 2026-09-17 | `QCOM` | 6 | $184.84 | $190.35 | +33.06 | — | +0.00 | +33.06 | +7.08 | — |
| 2026-09-17 | `SM` | 33 | $38.16 | $37.57 | -19.47 | — | +0.00 | -19.47 | -79.86 | — |
| 2026-09-17 | `SMTC` | 51 | — | $170.85 | +0.00 | $178.19 | +374.34 | +374.34 | +0.00 | +374.34 |
| 2026-09-18 | `SMTC` | 51 | $178.19 | $182.33 | +211.14 | — | +0.00 | +211.14 | +585.48 | — |
| 2026-09-18 | `TH` | 313 | — | $20.91 | +0.00 | $21.19 | +87.64 | +87.64 | +0.00 | +87.64 |
| 2026-09-18 | `GME` | 61 | — | $22.90 | +0.00 | $22.64 | -15.86 | -15.86 | +0.00 | -15.86 |
| 2026-09-18 | `RARE` | 95 | — | $14.79 | +0.00 | $14.51 | -26.60 | -26.60 | +0.00 | -26.60 |
| 2026-09-21 | `TH` | 313 | $21.19 | $21.65 | +143.98 | — | +0.00 | +143.98 | +231.62 | — |
| 2026-09-21 | `GME` | 61 | $22.64 | $22.78 | +8.54 | — | +0.00 | +8.54 | -7.32 | — |
| 2026-09-21 | `RARE` | 95 | $14.51 | $14.58 | +6.65 | — | +0.00 | +6.65 | -19.95 | — |
| 2026-09-21 | `VICR` | 29 | — | $230.25 | +0.00 | $223.90 | -184.15 | -184.15 | +0.00 | -184.15 |
| 2026-09-21 | `SMTC` | 15 | — | $190.30 | +0.00 | $177.37 | -193.95 | -193.95 | +0.00 | -193.95 |
| 2026-09-22 | `VICR` | 29 | $223.90 | $241.04 | +497.06 | — | +0.00 | +497.06 | +312.91 | — |
| 2026-09-22 | `SMTC` | 15 | $177.37 | $175.00 | -35.55 | — | +0.00 | -35.55 | -229.50 | — |

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
| 2026-08-26 | +2.02 | $55.94 | AU×61, FCX×40 | $10,537.34 | -241.79 | -26.40 | CM | AU, FCX | $102.70 | $10,504.30 | CM×88 |
| 2026-08-27 | — | $102.70 | CM×88 | $10,554.46 | +50.16 | -345.84 | — | — | $102.70 | $10,208.62 | CM×88 |
| 2026-08-28 | +0.75 | $102.70 | CM×88 | $10,280.78 | +72.16 | -215.77 | KEYS, SMTC, CIEN | CM | $1,342.19 | $10,056.60 | KEYS×22, SMTC×7, CIEN×2 |
| 2026-08-31 | -5.85 | $1,342.19 | KEYS×22, SMTC×7, CIEN×2 | $10,119.95 | +63.35 | +0.00 | — | KEYS, SMTC, CIEN | $10,113.78 | $10,113.78 | — |
| 2026-09-01 | -6.30 | $10,113.78 | — | $10,113.78 | -0.00 | +0.00 | — | — | $10,113.78 | $10,113.78 | — |
| 2026-09-02 | -3.83 | $10,113.78 | — | $10,113.78 | -0.00 | +0.00 | — | — | $10,113.78 | $10,113.78 | — |
| 2026-09-03 | -0.90 | $10,113.78 | — | $10,113.78 | -0.00 | +288.88 | AVGO, DELL | — | $157.06 | $10,398.60 | AVGO×20, DELL×6 |
| 2026-09-04 | +2.25 | $157.06 | AVGO×20, DELL×6 | $10,433.74 | +35.14 | -67.66 | CRM, FRNM, MRX | AVGO, DELL | $241.46 | $10,355.52 | CRM×27, FRNM×95, MRX×20 |
| 2026-09-08 | -11.47 | $241.46 | CRM×27, FRNM×95, MRX×20 | $10,259.00 | -96.52 | +0.00 | — | CRM, FRNM, MRX | $10,252.49 | $10,252.49 | — |
| 2026-09-09 | -13.95 | $10,252.49 | — | $10,252.49 | +0.00 | +0.00 | — | — | $10,252.49 | $10,252.49 | — |
| 2026-09-10 | -13.28 | $10,252.49 | — | $10,252.49 | +0.00 | +0.00 | — | — | $10,252.49 | $10,252.49 | — |
| 2026-09-11 | +0.50 | $10,252.49 | — | $10,252.49 | +0.00 | -877.30 | ORCL | — | $55.66 | $9,373.02 | ORCL×62 |
| 2026-09-14 | -11.00 | $55.66 | ORCL×62 | $8,823.70 | -549.32 | +0.00 | — | ORCL | $8,821.44 | $8,821.44 | — |
| 2026-09-15 | -3.84 | $8,821.44 | — | $8,821.44 | -0.00 | +0.00 | — | — | $8,821.44 | $8,821.44 | — |
| 2026-09-16 | +5.30 | $8,821.44 | — | $8,821.44 | -0.00 | -11.17 | WAY, QCOM, SM | — | $186.17 | $8,803.14 | WAY×235, QCOM×6, SM×33 |
| 2026-09-17 | +7.38 | $186.17 | WAY×235, QCOM×6, SM×33 | $8,797.93 | -5.21 | +374.34 | SMTC | WAY, QCOM, SM | $75.18 | $9,162.87 | SMTC×51 |
| 2026-09-18 | +4.86 | $75.18 | SMTC×51 | $9,374.01 | +211.14 | +45.18 | TH, GME, RARE | SMTC | $16.52 | $9,408.48 | TH×313, GME×61, RARE×95 |
| 2026-09-21 | +12.87 | $16.52 | TH×313, GME×61, RARE×95 | $9,567.65 | +159.17 | -378.10 | VICR, SMTC | TH, GME, RARE | $23.15 | $9,176.80 | VICR×29, SMTC×15 |
| 2026-09-22 | -0.50 | $23.15 | VICR×29, SMTC×15 | $9,638.31 | +461.51 | +0.00 | — | VICR, SMTC | $9,634.10 | $9,634.10 | — |

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
| 2026-08-26 09:30 ET | **BUY** | `CM` | 88 | $118.50 | $2.25 | — | $102.70 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $10532.95 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.70 | ▼ close $10,504.30 vs 09:30 $10,537.34 (session -26.40) | 16:00 close · cash $102.70 · equity $10,504.30 vs 09:30 $10,537.34 (-33.04; session marks -26.40) · 1 name(s) marked open→close (per-name table). CM×88 09:30 $118.50 → close $118.20 -26.40 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.70 | ▲ 09:30 equity $10,554.46 vs yday $10,504.30 (+50.16) | 09:30 open · cash $102.70 (unchanged overnight, no fees) · equity $10,554.46 vs prior close $10,504.30 (+50.16) · 1 name(s) re-marked at the open (per-name table). CM×88 yday $118.20 → 09:30 $118.77 +50.16 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.70 | ▼ close $10,208.62 vs 09:30 $10,554.46 (session -345.84) | 16:00 close · cash $102.70 · equity $10,208.62 vs 09:30 $10,554.46 (-345.84; session marks -345.84) · 1 name(s) marked open→close (per-name table). CM×88 09:30 $118.77 → close $114.84 -345.84 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.70 | ▲ 09:30 equity $10,280.78 vs yday $10,208.62 (+72.16) | 09:30 open · cash $102.70 (unchanged overnight, no fees) · equity $10,280.78 vs prior close $10,208.62 (+72.16) · 1 name(s) re-marked at the open (per-name table). CM×88 yday $114.84 → 09:30 $115.66 +72.16 | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 88 | $115.66 | $2.35 | $-254.52 | $10,278.43 | ▼ -254.52 after sell → book $10,278.43; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 22 | $324.41 | $2.06 | — | $3,139.35 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $7194.90 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $2,145.02 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1027.84 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $1,342.19 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1027.84 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,342.19 | ▼ close $10,056.60 vs 09:30 $10,280.78 (session -215.77) | 16:00 close · cash $1,342.19 · equity $10,056.60 vs 09:30 $10,280.78 (-224.18; session marks -215.77) · 3 name(s) marked open→close (per-name table). KEYS×22 09:30 $324.41 → close $319.97 -97.68; SMTC×7 09:30 $141.76 → close $131.17 -74.13; CIEN×2 09:30 $400.42 → close $378.44 -43.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,342.19 | ▲ 09:30 equity $10,119.95 vs yday $10,056.60 (+63.35) | 09:30 open · cash $1,342.19 (unchanged overnight, no fees) · equity $10,119.95 vs prior close $10,056.60 (+63.35) · 3 name(s) re-marked at the open (per-name table). KEYS×22 yday $319.97 → 09:30 $322.49 +55.44; SMTC×7 yday $131.17 → 09:30 $132.30 +7.91; CIEN×2 yday $378.44 → 09:30 $378.44 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 22 | $322.49 | $2.12 | $-46.42 | $8,434.84 | ▼ -46.42 after sell → book $10,117.82; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 7 | $132.30 | $2.03 | $-70.26 | $9,358.91 | ▼ -70.26 after sell → book $10,115.79; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 2 | $378.44 | $2.02 | $-47.97 | $10,113.78 | ▼ -47.97 after sell → book $10,113.78; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,113.78 | ▲ close $10,113.78 vs 09:30 $10,119.95 (session +0.00) | 16:00 close · cash $10,113.78 · no lots left · equity $10,113.78. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,113.78 | ▲ 09:30 equity $10,113.78 vs yday $10,113.78 (-0.00) | 09:30 open · cash $10,113.78 · no holdings · equity $10,113.78 vs prior close $10,113.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,113.78 | ▲ close $10,113.78 vs 09:30 $10,113.78 (session +0.00) | 16:00 close · cash $10,113.78 · no lots left · equity $10,113.78. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,113.78 | ▲ 09:30 equity $10,113.78 vs yday $10,113.78 (-0.00) | 09:30 open · cash $10,113.78 · no holdings · equity $10,113.78 vs prior close $10,113.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,113.78 | ▲ close $10,113.78 vs 09:30 $10,113.78 (session +0.00) | 16:00 close · cash $10,113.78 · no lots left · equity $10,113.78. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,113.78 | ▲ 09:30 equity $10,113.78 vs yday $10,113.78 (-0.00) | 09:30 open · cash $10,113.78 · no holdings · equity $10,113.78 vs prior close $10,113.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 20 | $351.74 | $2.05 | — | $3,076.93 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $7079.64 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 6 | $486.31 | $2.01 | — | $157.06 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $3034.13 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.06 | ▲ close $10,398.60 vs 09:30 $10,113.78 (session +288.88) | 16:00 close · cash $157.06 · equity $10,398.60 vs 09:30 $10,113.78 (+284.82; session marks +288.88) · 2 name(s) marked open→close (per-name table). AVGO×20 09:30 $351.74 → close $357.16 +108.40; DELL×6 09:30 $486.31 → close $516.39 +180.48 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.06 | ▲ 09:30 equity $10,433.74 vs yday $10,398.60 (+35.14) | 09:30 open · cash $157.06 (unchanged overnight, no fees) · equity $10,433.74 vs prior close $10,398.60 (+35.14) · 2 name(s) re-marked at the open (per-name table). AVGO×20 yday $357.16 → 09:30 $359.70 +50.80; DELL×6 yday $516.39 → 09:30 $513.78 -15.66 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 20 | $359.70 | $2.12 | $+155.03 | $7,348.94 | ▲ +155.03 after sell → book $10,431.62; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 6 | $513.78 | $2.04 | $+160.77 | $10,429.58 | ▲ +160.77 after sell → book $10,429.58; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 27 | $263.36 | $2.07 | — | $3,316.79 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $7300.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 95 | $16.40 | $2.27 | — | $1,756.51 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1564.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 20 | $75.65 | $2.05 | — | $241.46 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1564.44 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $241.46 | ▼ close $10,355.52 vs 09:30 $10,433.74 (session -67.66) | 16:00 close · cash $241.46 · equity $10,355.52 vs 09:30 $10,433.74 (-78.22; session marks -67.66) · 3 name(s) marked open→close (per-name table). CRM×27 09:30 $263.36 → close $259.23 -111.51; FRNM×95 09:30 $16.40 → close $16.31 -8.55; MRX×20 09:30 $75.65 → close $78.27 +52.40 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $241.46 | ▼ 09:30 equity $10,259.00 vs yday $10,355.52 (-96.52) | 09:30 open · cash $241.46 (unchanged overnight, no fees) · equity $10,259.00 vs prior close $10,355.52 (-96.52) · 3 name(s) re-marked at the open (per-name table). CRM×27 yday $259.23 → 09:30 $253.72 -148.77; FRNM×95 yday $16.31 → 09:30 $16.74 +40.85; MRX×20 yday $78.27 → 09:30 $78.84 +11.40 | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 27 | $253.72 | $2.14 | $-264.49 | $7,089.77 | ▼ -264.49 after sell → book $10,256.87; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 95 | $16.74 | $2.30 | $+27.72 | $8,677.76 | ▲ +27.72 after sell → book $10,254.56; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 20 | $78.84 | $2.07 | $+59.68 | $10,252.49 | ▲ +59.68 after sell → book $10,252.49; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,252.49 | ▲ close $10,252.49 vs 09:30 $10,259.00 (session +0.00) | 16:00 close · cash $10,252.49 · no lots left · equity $10,252.49. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,252.49 | ▲ 09:30 equity $10,252.49 vs yday $10,252.49 (+0.00) | 09:30 open · cash $10,252.49 · no holdings · equity $10,252.49 vs prior close $10,252.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,252.49 | ▲ close $10,252.49 vs 09:30 $10,252.49 (session +0.00) | 16:00 close · cash $10,252.49 · no lots left · equity $10,252.49. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,252.49 | ▲ 09:30 equity $10,252.49 vs yday $10,252.49 (+0.00) | 09:30 open · cash $10,252.49 · no holdings · equity $10,252.49 vs prior close $10,252.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,252.49 | ▲ close $10,252.49 vs 09:30 $10,252.49 (session +0.00) | 16:00 close · cash $10,252.49 · no lots left · equity $10,252.49. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,252.49 | ▲ 09:30 equity $10,252.49 vs yday $10,252.49 (+0.00) | 09:30 open · cash $10,252.49 · no holdings · equity $10,252.49 vs prior close $10,252.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 62 | $164.43 | $2.18 | — | $55.66 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10252.49 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.66 | ▼ close $9,373.02 vs 09:30 $10,252.49 (session -877.30) | 16:00 close · cash $55.66 · equity $9,373.02 vs 09:30 $10,252.49 (-879.47; session marks -877.30) · 1 name(s) marked open→close (per-name table). ORCL×62 09:30 $164.43 → close $150.28 -877.30 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.66 | ▼ 09:30 equity $8,823.70 vs yday $9,373.02 (-549.32) | 09:30 open · cash $55.66 (unchanged overnight, no fees) · equity $8,823.70 vs prior close $9,373.02 (-549.32) · 1 name(s) re-marked at the open (per-name table). ORCL×62 yday $150.28 → 09:30 $141.42 -549.32 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 62 | $141.42 | $2.26 | $-1431.05 | $8,821.44 | ▼ -1,431.05 after sell → book $8,821.44; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,821.44 | ▲ close $8,821.44 vs 09:30 $8,823.70 (session +0.00) | 16:00 close · cash $8,821.44 · no lots left · equity $8,821.44. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,821.44 | ▲ 09:30 equity $8,821.44 vs yday $8,821.44 (-0.00) | 09:30 open · cash $8,821.44 · no holdings · equity $8,821.44 vs prior close $8,821.44 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,821.44 | ▲ close $8,821.44 vs 09:30 $8,821.44 (session +0.00) | 16:00 close · cash $8,821.44 · no lots left · equity $8,821.44. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,821.44 | ▲ 09:30 equity $8,821.44 vs yday $8,821.44 (-0.00) | 09:30 open · cash $8,821.44 · no holdings · equity $8,821.44 vs prior close $8,821.44 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 235 | $26.27 | $3.03 | — | $2,644.96 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $6175.01 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 6 | $189.17 | $2.01 | — | $1,507.93 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $1323.22 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 33 | $39.99 | $2.09 | — | $186.17 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1323.22 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.17 | ▼ close $8,803.14 vs 09:30 $8,821.44 (session -11.17) | 16:00 close · cash $186.17 · equity $8,803.14 vs 09:30 $8,821.44 (-18.30; session marks -11.17) · 3 name(s) marked open→close (per-name table). WAY×235 09:30 $26.27 → close $26.59 +75.20; QCOM×6 09:30 $189.17 → close $184.84 -25.98; SM×33 09:30 $39.99 → close $38.16 -60.39 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.17 | ▼ 09:30 equity $8,797.93 vs yday $8,803.14 (-5.21) | 09:30 open · cash $186.17 (unchanged overnight, no fees) · equity $8,797.93 vs prior close $8,803.14 (-5.21) · 3 name(s) re-marked at the open (per-name table). WAY×235 yday $26.59 → 09:30 $26.51 -18.80; QCOM×6 yday $184.84 → 09:30 $190.35 +33.06; SM×33 yday $38.16 → 09:30 $37.57 -19.47 | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 235 | $26.51 | $3.12 | $+50.25 | $6,412.90 | ▲ +50.25 after sell → book $8,794.81; vs 09:30 mark -3.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 6 | $190.35 | $2.03 | $+3.04 | $7,552.97 | ▲ +3.04 after sell → book $8,792.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 33 | $37.57 | $2.11 | $-84.06 | $8,790.67 | ▼ -84.06 after sell → book $8,790.67; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 51 | $170.85 | $2.14 | — | $75.18 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $8790.67 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.18 | ▲ close $9,162.87 vs 09:30 $8,797.93 (session +374.34) | 16:00 close · cash $75.18 · equity $9,162.87 vs 09:30 $8,797.93 (+364.94; session marks +374.34) · 1 name(s) marked open→close (per-name table). SMTC×51 09:30 $170.85 → close $178.19 +374.34 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.18 | ▲ 09:30 equity $9,374.01 vs yday $9,162.87 (+211.14) | 09:30 open · cash $75.18 (unchanged overnight, no fees) · equity $9,374.01 vs prior close $9,162.87 (+211.14) · 1 name(s) re-marked at the open (per-name table). SMTC×51 yday $178.19 → 09:30 $182.33 +211.14 | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 51 | $182.33 | $2.23 | $+581.11 | $9,371.78 | ▲ +581.11 after sell → book $9,371.78; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 313 | $20.91 | $4.04 | — | $2,822.92 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $6560.25 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 61 | $22.90 | $2.17 | — | $1,423.84 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1405.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 95 | $14.79 | $2.27 | — | $16.52 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1405.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.52 | ▲ close $9,408.48 vs 09:30 $9,374.01 (session +45.18) | 16:00 close · cash $16.52 · equity $9,408.48 vs 09:30 $9,374.01 (+34.47; session marks +45.18) · 3 name(s) marked open→close (per-name table). TH×313 09:30 $20.91 → close $21.19 +87.64; GME×61 09:30 $22.90 → close $22.64 -15.86; RARE×95 09:30 $14.79 → close $14.51 -26.60 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.52 | ▲ 09:30 equity $9,567.65 vs yday $9,408.48 (+159.17) | 09:30 open · cash $16.52 (unchanged overnight, no fees) · equity $9,567.65 vs prior close $9,408.48 (+159.17) · 3 name(s) re-marked at the open (per-name table). TH×313 yday $21.19 → 09:30 $21.65 +143.98; GME×61 yday $22.64 → 09:30 $22.78 +8.54; RARE×95 yday $14.51 → 09:30 $14.58 +6.65 | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 313 | $21.65 | $4.14 | $+223.44 | $6,788.82 | ▲ +223.44 after sell → book $9,563.50; vs 09:30 mark -4.15 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 61 | $22.78 | $2.19 | $-11.69 | $8,176.21 | ▼ -11.69 after sell → book $9,561.31; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 95 | $14.58 | $2.30 | $-24.53 | $9,559.01 | ▼ -24.53 after sell → book $9,559.01; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 29 | $230.25 | $2.08 | — | $2,879.68 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+12.5; leftover $6691.31 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 15 | $190.30 | $2.04 | — | $23.15 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+10.6; leftover $2867.70 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.15 | ▼ close $9,176.80 vs 09:30 $9,567.65 (session -378.10) | 16:00 close · cash $23.15 · equity $9,176.80 vs 09:30 $9,567.65 (-390.85; session marks -378.10) · 2 name(s) marked open→close (per-name table). VICR×29 09:30 $230.25 → close $223.90 -184.15; SMTC×15 09:30 $190.30 → close $177.37 -193.95 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.15 | ▲ 09:30 equity $9,638.31 vs yday $9,176.80 (+461.51) | 09:30 open · cash $23.15 (unchanged overnight, no fees) · equity $9,638.31 vs prior close $9,176.80 (+461.51) · 2 name(s) re-marked at the open (per-name table). VICR×29 yday $223.90 → 09:30 $241.04 +497.06; SMTC×15 yday $177.37 → 09:30 $175.00 -35.55 | — |
| 2026-09-22 09:30 ET | **SELL** | `VICR` | 29 | $241.04 | $2.14 | $+308.69 | $7,011.16 | ▲ +308.69 after sell → book $9,636.16; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SMTC` | 15 | $175.00 | $2.07 | $-233.60 | $9,634.10 | ▼ -233.60 after sell → book $9,634.10; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,634.10 | ▲ close $9,634.10 vs 09:30 $9,638.31 (session +0.00) | 16:00 close · cash $9,634.10 · no lots left · equity $9,634.10. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ACMR` | cash | leftover split 71.89 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 15.41 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 15.41 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1027.84 < 1 share @ 1306.03 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
