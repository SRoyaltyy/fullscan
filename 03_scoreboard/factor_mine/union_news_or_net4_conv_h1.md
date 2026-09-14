# Factor mine action — `union_news_or_net4_conv_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `conviction` · sell `list` · S-boost `none` · OR news + net≥4; 70% leftover if #1 net ≥ 5

Cash book **-7.86%** ($9,214) · signal-only (no cash/fees) was -6.41%. Starts YES **0/22**. Fills 56 · skips 21 · realized $-786.35.

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
- **Gate** `news_or_headline=True,cam_net_min=4` · **rank** `cond` · **top_n** 4.
- **Size** `conviction` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,213.66.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ANGX` | 928 | — | $4.31 | +0.00 | $4.37 | +55.68 | +55.68 | +0.00 | +55.68 |
| 2026-08-14 | `ARX` | 153 | — | $19.57 | +0.00 | $19.58 | +1.53 | +1.53 | +0.00 | +1.53 |
| 2026-08-14 | `HLIT` | 151 | — | $13.18 | +0.00 | $13.92 | +111.74 | +111.74 | +0.00 | +111.74 |
| 2026-08-14 | `MH` | 73 | — | $13.55 | +0.00 | $13.10 | -32.85 | -32.85 | +0.00 | -32.85 |
| 2026-08-17 | `ANGX` | 928 | $4.37 | $4.60 | +213.44 | — | +0.00 | +213.44 | +269.12 | — |
| 2026-08-17 | `ARX` | 153 | $19.58 | $19.57 | -1.53 | — | +0.00 | -1.53 | +0.00 | — |
| 2026-08-17 | `HLIT` | 151 | $13.92 | $13.84 | -12.08 | — | +0.00 | -12.08 | +99.66 | — |
| 2026-08-17 | `MH` | 73 | $13.10 | $13.16 | +4.38 | — | +0.00 | +4.38 | -28.47 | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 79 | — | $91.01 | +0.00 | $93.63 | +206.98 | +206.98 | +0.00 | +206.98 |
| 2026-08-20 | `APA` | 23 | — | $44.76 | +0.00 | $44.39 | -8.51 | -8.51 | +0.00 | -8.51 |
| 2026-08-20 | `AUTL` | 417 | — | $2.47 | +0.00 | $2.46 | -4.17 | -4.17 | +0.00 | -4.17 |
| 2026-08-20 | `CRSP` | 17 | — | $58.73 | +0.00 | $58.12 | -10.37 | -10.37 | +0.00 | -10.37 |
| 2026-08-21 | `BHP` | 79 | $93.63 | $95.72 | +165.11 | — | +0.00 | +165.11 | +372.09 | — |
| 2026-08-21 | `APA` | 23 | $44.39 | $44.52 | +2.99 | — | +0.00 | +2.99 | -5.52 | — |
| 2026-08-21 | `AUTL` | 417 | $2.46 | $2.47 | +4.17 | $2.41 | -25.02 | -20.85 | +0.00 | -25.02 |
| 2026-08-21 | `CRSP` | 17 | $58.12 | $59.72 | +27.20 | $59.50 | -3.74 | +23.46 | +16.83 | +13.09 |
| 2026-08-21 | `AU` | 50 | — | $119.43 | +0.00 | $121.22 | +89.50 | +89.50 | +0.00 | +89.50 |
| 2026-08-21 | `FUTU` | 22 | — | $115.18 | +0.00 | $123.64 | +186.12 | +186.12 | +0.00 | +186.12 |
| 2026-08-24 | `AUTL` | 417 | $2.41 | $2.40 | -4.17 | — | +0.00 | -4.17 | -29.19 | — |
| 2026-08-24 | `CRSP` | 17 | $59.50 | $58.75 | -12.75 | $57.08 | -28.47 | -41.22 | +0.34 | -28.13 |
| 2026-08-24 | `AU` | 50 | $121.22 | $120.51 | -35.50 | — | +0.00 | -35.50 | +54.00 | — |
| 2026-08-24 | `FUTU` | 22 | $123.64 | $121.00 | -58.08 | — | +0.00 | -58.08 | +128.04 | — |
| 2026-08-25 | `CRSP` | 17 | $57.08 | $57.93 | +14.53 | — | +0.00 | +14.53 | -13.60 | — |
| 2026-08-25 | `AU` | 63 | — | $118.52 | +0.00 | $123.39 | +306.81 | +306.81 | +0.00 | +306.81 |
| 2026-08-25 | `FCX` | 20 | — | $77.13 | +0.00 | $79.91 | +55.60 | +55.60 | +0.00 | +55.60 |
| 2026-08-25 | `EZPW` | 46 | — | $35.05 | +0.00 | $35.23 | +8.28 | +8.28 | +0.00 | +8.28 |
| 2026-08-26 | `AU` | 63 | $123.39 | $119.80 | -226.17 | — | +0.00 | -226.17 | +80.64 | — |
| 2026-08-26 | `FCX` | 20 | $79.91 | $79.34 | -11.40 | — | +0.00 | -11.40 | +44.20 | — |
| 2026-08-26 | `EZPW` | 46 | $35.23 | $35.70 | +21.62 | — | +0.00 | +21.62 | +29.90 | — |
| 2026-08-26 | `FNV` | 40 | — | $267.02 | +0.00 | $267.37 | +14.00 | +14.00 | +0.00 | +14.00 |
| 2026-08-27 | `FNV` | 40 | $267.37 | $267.23 | -5.60 | — | +0.00 | -5.60 | +8.40 | — |
| 2026-08-27 | `ACMR` | 93 | — | $81.65 | +0.00 | $80.49 | -107.88 | -107.88 | +0.00 | -107.88 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `LRCX` | 3 | — | $318.88 | +0.00 | $318.58 | -0.90 | -0.90 | +0.00 | -0.90 |
| 2026-08-28 | `ACMR` | 93 | $80.49 | $79.27 | -113.46 | — | +0.00 | -113.46 | -221.34 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `LRCX` | 3 | $318.58 | $318.03 | -1.65 | — | +0.00 | -1.65 | -2.55 | — |
| 2026-08-28 | `KEYS` | 22 | — | $324.41 | +0.00 | $319.97 | -97.68 | -97.68 | +0.00 | -97.68 |
| 2026-08-28 | `SMTC` | 7 | — | $141.76 | +0.00 | $131.17 | -74.13 | -74.13 | +0.00 | -74.13 |
| 2026-08-28 | `CIEN` | 2 | — | $400.42 | +0.00 | $378.44 | -43.96 | -43.96 | +0.00 | -43.96 |
| 2026-08-31 | `KEYS` | 22 | $319.97 | $322.49 | +55.44 | — | +0.00 | +55.44 | -42.24 | — |
| 2026-08-31 | `SMTC` | 7 | $131.17 | $132.30 | +7.91 | — | +0.00 | +7.91 | -66.22 | — |
| 2026-08-31 | `CIEN` | 2 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -43.96 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 20 | — | $351.74 | +0.00 | $357.16 | +108.40 | +108.40 | +0.00 | +108.40 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 32 | — | $32.31 | +0.00 | $33.66 | +43.20 | +43.20 | +0.00 | +43.20 |
| 2026-09-03 | `FRNM` | 65 | — | $15.87 | +0.00 | $16.90 | +66.95 | +66.95 | +0.00 | +66.95 |
| 2026-09-04 | `AVGO` | 20 | $357.16 | $359.70 | +50.80 | — | +0.00 | +50.80 | +159.20 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `CXW` | 32 | $33.66 | $33.46 | -6.40 | — | +0.00 | -6.40 | +36.80 | — |
| 2026-09-04 | `FRNM` | 65 | $16.90 | $16.40 | -32.50 | $16.31 | -5.85 | -38.35 | +34.45 | +28.60 |
| 2026-09-04 | `CRM` | 25 | — | $263.36 | +0.00 | $259.23 | -103.25 | -103.25 | +0.00 | -103.25 |
| 2026-09-04 | `MRX` | 19 | — | $75.65 | +0.00 | $78.27 | +49.78 | +49.78 | +0.00 | +49.78 |
| 2026-09-04 | `BE` | 6 | — | $236.82 | +0.00 | $252.87 | +96.30 | +96.30 | +0.00 | +96.30 |
| 2026-09-08 | `FRNM` | 65 | $16.31 | $16.74 | +27.95 | — | +0.00 | +27.95 | +56.55 | — |
| 2026-09-08 | `CRM` | 25 | $259.23 | $253.72 | -137.75 | — | +0.00 | -137.75 | -241.00 | — |
| 2026-09-08 | `MRX` | 19 | $78.27 | $78.84 | +10.83 | $76.71 | -40.47 | -29.64 | +60.61 | +20.14 |
| 2026-09-08 | `BE` | 6 | $252.87 | $267.76 | +89.34 | — | +0.00 | +89.34 | +185.64 | — |
| 2026-09-09 | `MRX` | 19 | $76.71 | $76.60 | -2.09 | — | +0.00 | -2.09 | +18.05 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 65 | — | $164.43 | +0.00 | $150.28 | -919.75 | -919.75 | +0.00 | -919.75 |
| 2026-09-14 | `ORCL` | 65 | $150.28 | $141.42 | -575.90 | — | +0.00 | -575.90 | -1495.65 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +136.10 | ANGX, ARX, HLIT, MH | — | $7.71 | $10,117.03 | ANGX×928, ARX×153, HLIT×151, MH×73 |
| 2026-08-17 | +2.25 | $7.71 | ANGX×928, ARX×153, HLIT×151, MH×73 | $10,321.24 | +204.21 | +0.00 | — | ANGX, ARX, HLIT, MH | $10,301.86 | $10,301.86 | — |
| 2026-08-18 | -6.20 | $10,301.86 | — | $10,301.86 | +0.00 | +0.00 | — | — | $10,301.86 | $10,301.86 | — |
| 2026-08-19 | -7.20 | $10,301.86 | — | $10,301.86 | +0.00 | +0.00 | — | — | $10,301.86 | $10,301.86 | — |
| 2026-08-20 | +1.12 | $10,301.86 | — | $10,301.86 | +0.00 | +183.93 | BHP, APA, AUTL, CRSP | — | $42.49 | $10,474.09 | BHP×79, APA×23, AUTL×417, CRSP×17 |
| 2026-08-21 | +3.25 | $42.49 | BHP×79, APA×23, AUTL×417, CRSP×17 | $10,673.56 | +199.47 | +246.86 | AU, FUTU | BHP, APA | $114.29 | $10,911.84 | AUTL×417, CRSP×17, AU×50, FUTU×22 |
| 2026-08-24 | -5.17 | $114.29 | AUTL×417, CRSP×17, AU×50, FUTU×22 | $10,801.34 | -110.50 | -28.47 | — | AUTL, AU, FUTU | $9,792.85 | $10,763.12 | CRSP×17 |
| 2026-08-25 | +1.80 | $9,792.85 | CRSP×17 | $10,777.66 | +14.54 | +370.69 | AU, FCX, EZPW | CRSP | $147.58 | $11,139.93 | AU×63, FCX×20, EZPW×46 |
| 2026-08-26 | +2.02 | $147.58 | AU×63, FCX×20, EZPW×46 | $10,923.98 | -215.95 | +14.00 | FNV | AU, FCX, EZPW | $234.60 | $10,929.40 | FNV×40 |
| 2026-08-27 | — | $234.60 | FNV×40 | $10,923.80 | -5.60 | -140.40 | ACMR, MU, LRCX | FNV | $1,398.23 | $10,774.93 | ACMR×93, MU×1, LRCX×3 |
| 2026-08-28 | +0.75 | $1,398.23 | ACMR×93, MU×1, LRCX×3 | $10,643.72 | -131.21 | -215.77 | KEYS, SMTC, CIEN | ACMR, MU, LRCX | $1,701.10 | $10,415.51 | KEYS×22, SMTC×7, CIEN×2 |
| 2026-08-31 | -5.85 | $1,701.10 | KEYS×22, SMTC×7, CIEN×2 | $10,478.86 | +63.35 | +0.00 | — | KEYS, SMTC, CIEN | $10,472.69 | $10,472.69 | — |
| 2026-09-01 | -6.30 | $10,472.69 | — | $10,472.69 | +0.00 | +0.00 | — | — | $10,472.69 | $10,472.69 | — |
| 2026-09-02 | -3.83 | $10,472.69 | — | $10,472.69 | +0.00 | +0.00 | — | — | $10,472.69 | $10,472.69 | — |
| 2026-09-03 | -0.90 | $10,472.69 | — | $10,472.69 | +0.00 | +278.71 | AVGO, DELL, CXW, FRNM | — | $391.48 | $10,743.08 | AVGO×20, DELL×2, CXW×32, FRNM×65 |
| 2026-09-04 | +2.25 | $391.48 | AVGO×20, DELL×2, CXW×32, FRNM×65 | $10,749.76 | +6.68 | +36.98 | CRM, MRX, BE | AVGO, DELL, CXW | $229.14 | $10,774.39 | FRNM×65, CRM×25, MRX×19, BE×6 |
| 2026-09-08 | -11.47 | $229.14 | FRNM×65, CRM×25, MRX×19, BE×6 | $10,764.76 | -9.63 | -40.47 | — | FRNM, CRM, BE | $9,260.43 | $10,717.92 | MRX×19 |
| 2026-09-09 | -13.95 | $9,260.43 | MRX×19 | $10,715.83 | -2.09 | +0.00 | — | MRX | $10,713.76 | $10,713.76 | — |
| 2026-09-10 | -13.28 | $10,713.76 | — | $10,713.76 | +0.00 | +0.00 | — | — | $10,713.76 | $10,713.76 | — |
| 2026-09-11 | +0.50 | $10,713.76 | — | $10,713.76 | +0.00 | -919.75 | ORCL | — | $23.63 | $9,791.83 | ORCL×65 |
| 2026-09-14 | -11.00 | $23.63 | ORCL×65 | $9,215.93 | -575.90 | +0.00 | — | ORCL | $9,213.66 | $9,213.66 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 928 | $4.31 | $11.97 | — | $5,988.35 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $4000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 153 | $19.57 | $2.45 | — | $2,991.69 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $3000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 151 | $13.18 | $2.44 | — | $999.07 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 73 | $13.55 | $2.21 | — | $7.71 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.71 | ▲ close $10,117.03 vs 09:30 $10,000.00 (session +136.10) | 16:00 close · cash $7.71 · equity $10,117.03 vs 09:30 $10,000.00 (+117.03; session marks +136.10) · 4 name(s) marked open→close (per-name table). ANGX×928 09:30 $4.31 → close $4.37 +55.68; ARX×153 09:30 $19.57 → close $19.58 +1.53; HLIT×151 09:30 $13.18 → close $13.92 +111.74; MH×73 09:30 $13.55 → close $13.10 -32.85 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.71 | ▲ 09:30 equity $10,321.24 vs yday $10,117.03 (+204.21) | 09:30 open · cash $7.71 (unchanged overnight, no fees) · equity $10,321.24 vs prior close $10,117.03 (+204.21) · 4 name(s) re-marked at the open (per-name table). ANGX×928 yday $4.37 → 09:30 $4.60 +213.44; ARX×153 yday $19.58 → 09:30 $19.57 -1.53; HLIT×151 yday $13.92 → 09:30 $13.84 -12.08; MH×73 yday $13.10 → 09:30 $13.16 +4.38 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 928 | $4.60 | $12.16 | $+244.99 | $4,264.35 | ▲ +244.99 after sell → book $10,309.08; vs 09:30 mark -12.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 153 | $19.57 | $2.50 | $-4.95 | $7,256.06 | ▼ -4.95 after sell → book $10,306.58; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 151 | $13.84 | $2.48 | $+94.73 | $9,343.42 | ▲ +94.73 after sell → book $10,304.10; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 73 | $13.16 | $2.23 | $-32.91 | $10,301.86 | ▼ -32.91 after sell → book $10,301.86; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,301.86 | ▲ close $10,301.86 vs 09:30 $10,321.24 (session +0.00) | 16:00 close · cash $10,301.86 · no lots left · equity $10,301.86. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,301.86 | ▲ 09:30 equity $10,301.86 vs yday $10,301.86 (+0.00) | 09:30 open · cash $10,301.86 · no holdings · equity $10,301.86 vs prior close $10,301.86 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,301.86 | ▲ close $10,301.86 vs 09:30 $10,301.86 (session +0.00) | 16:00 close · cash $10,301.86 · no lots left · equity $10,301.86. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,301.86 | ▲ 09:30 equity $10,301.86 vs yday $10,301.86 (+0.00) | 09:30 open · cash $10,301.86 · no holdings · equity $10,301.86 vs prior close $10,301.86 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,301.86 | ▲ close $10,301.86 vs 09:30 $10,301.86 (session +0.00) | 16:00 close · cash $10,301.86 · no lots left · equity $10,301.86. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,301.86 | ▲ 09:30 equity $10,301.86 vs yday $10,301.86 (+0.00) | 09:30 open · cash $10,301.86 · no holdings · equity $10,301.86 vs prior close $10,301.86 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 79 | $91.01 | $2.23 | — | $3,109.85 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $7211.30 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 23 | $44.76 | $2.06 | — | $2,078.31 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1030.19 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 417 | $2.47 | $5.38 | — | $1,042.94 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1030.19 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 17 | $58.73 | $2.04 | — | $42.49 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1030.19 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.49 | ▲ close $10,474.09 vs 09:30 $10,301.86 (session +183.93) | 16:00 close · cash $42.49 · equity $10,474.09 vs 09:30 $10,301.86 (+172.23; session marks +183.93) · 4 name(s) marked open→close (per-name table). BHP×79 09:30 $91.01 → close $93.63 +206.98; APA×23 09:30 $44.76 → close $44.39 -8.51; AUTL×417 09:30 $2.47 → close $2.46 -4.17; CRSP×17 09:30 $58.73 → close $58.12 -10.37 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.49 | ▲ 09:30 equity $10,673.56 vs yday $10,474.09 (+199.47) | 09:30 open · cash $42.49 (unchanged overnight, no fees) · equity $10,673.56 vs prior close $10,474.09 (+199.47) · 4 name(s) re-marked at the open (per-name table). BHP×79 yday $93.63 → 09:30 $95.72 +165.11; APA×23 yday $44.39 → 09:30 $44.52 +2.99; AUTL×417 yday $2.46 → 09:30 $2.47 +4.17; CRSP×17 yday $58.12 → 09:30 $59.72 +27.20 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 79 | $95.72 | $2.30 | $+367.56 | $7,602.07 | ▲ +367.56 after sell → book $10,671.26; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 23 | $44.52 | $2.08 | $-9.66 | $8,623.95 | ▼ -9.66 after sell → book $10,669.18; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 50 | $119.43 | $2.14 | — | $2,650.31 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $6036.76 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 22 | $115.18 | $2.06 | — | $114.29 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $2587.18 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.29 | ▲ close $10,911.84 vs 09:30 $10,673.56 (session +246.86) | 16:00 close · cash $114.29 · equity $10,911.84 vs 09:30 $10,673.56 (+238.28; session marks +246.86) · 4 name(s) marked open→close (per-name table). AUTL×417 09:30 $2.47 → close $2.41 -25.02; CRSP×17 09:30 $59.72 → close $59.50 -3.74; AU×50 09:30 $119.43 → close $121.22 +89.50; FUTU×22 09:30 $115.18 → close $123.64 +186.12 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.29 | ▼ 09:30 equity $10,801.34 vs yday $10,911.84 (-110.50) | 09:30 open · cash $114.29 (unchanged overnight, no fees) · equity $10,801.34 vs prior close $10,911.84 (-110.50) · 4 name(s) re-marked at the open (per-name table). AUTL×417 yday $2.41 → 09:30 $2.40 -4.17; CRSP×17 yday $59.50 → 09:30 $58.75 -12.75; AU×50 yday $121.22 → 09:30 $120.51 -35.50; FUTU×22 yday $123.64 → 09:30 $121.00 -58.08 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 417 | $2.40 | $5.46 | $-40.03 | $1,109.63 | ▼ -40.03 after sell → book $10,795.88; vs 09:30 mark -5.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 50 | $120.51 | $2.20 | $+49.66 | $7,132.94 | ▲ +49.66 after sell → book $10,793.69; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 22 | $121.00 | $2.09 | $+123.90 | $9,792.85 | ▲ +123.90 after sell → book $10,791.60; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,792.85 | ▼ close $10,763.12 vs 09:30 $10,801.34 (session -28.47) | 16:00 close · cash $9,792.85 · equity $10,763.12 vs 09:30 $10,801.34 (-38.22; session marks -28.47) · 1 name(s) marked open→close (per-name table). CRSP×17 09:30 $58.75 → close $57.08 -28.47 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,792.85 | ▲ 09:30 equity $10,777.66 vs yday $10,763.12 (+14.54) | 09:30 open · cash $9,792.85 (unchanged overnight, no fees) · equity $10,777.66 vs prior close $10,763.12 (+14.54) · 1 name(s) re-marked at the open (per-name table). CRSP×17 yday $57.08 → 09:30 $57.93 +14.53 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 17 | $57.93 | $2.06 | $-17.70 | $10,775.60 | ▼ -17.70 after sell → book $10,775.60; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 63 | $118.52 | $2.18 | — | $3,306.66 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $7542.92 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 20 | $77.13 | $2.05 | — | $1,762.01 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1616.34 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 46 | $35.05 | $2.13 | — | $147.58 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1616.34 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $147.58 | ▲ close $11,139.93 vs 09:30 $10,777.66 (session +370.69) | 16:00 close · cash $147.58 · equity $11,139.93 vs 09:30 $10,777.66 (+362.27; session marks +370.69) · 3 name(s) marked open→close (per-name table). AU×63 09:30 $118.52 → close $123.39 +306.81; FCX×20 09:30 $77.13 → close $79.91 +55.60; EZPW×46 09:30 $35.05 → close $35.23 +8.28 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $147.58 | ▼ 09:30 equity $10,923.98 vs yday $11,139.93 (-215.95) | 09:30 open · cash $147.58 (unchanged overnight, no fees) · equity $10,923.98 vs prior close $11,139.93 (-215.95) · 3 name(s) re-marked at the open (per-name table). AU×63 yday $123.39 → 09:30 $119.80 -226.17; FCX×20 yday $79.91 → 09:30 $79.34 -11.40; EZPW×46 yday $35.23 → 09:30 $35.70 +21.62 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 63 | $119.80 | $2.25 | $+76.21 | $7,692.73 | ▲ +76.21 after sell → book $10,921.73; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 20 | $79.34 | $2.07 | $+40.08 | $9,277.46 | ▲ +40.08 after sell → book $10,919.66; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 46 | $35.70 | $2.15 | $+25.62 | $10,917.51 | ▲ +25.62 after sell → book $10,917.51; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 40 | $267.02 | $2.11 | — | $234.60 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $10917.51 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $234.60 | ▲ close $10,929.40 vs 09:30 $10,923.98 (session +14.00) | 16:00 close · cash $234.60 · equity $10,929.40 vs 09:30 $10,923.98 (+5.42; session marks +14.00) · 1 name(s) marked open→close (per-name table). FNV×40 09:30 $267.02 → close $267.37 +14.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $234.60 | ▼ 09:30 equity $10,923.80 vs yday $10,929.40 (-5.60) | 09:30 open · cash $234.60 (unchanged overnight, no fees) · equity $10,923.80 vs prior close $10,929.40 (-5.60) · 1 name(s) re-marked at the open (per-name table). FNV×40 yday $267.37 → 09:30 $267.23 -5.60 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 40 | $267.23 | $2.21 | $+4.08 | $10,921.59 | ▲ +4.08 after sell → book $10,921.59; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 93 | $81.65 | $2.27 | — | $3,325.87 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $7645.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $2,356.87 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1092.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 3 | $318.88 | $2.00 | — | $1,398.23 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1092.16 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,398.23 | ▼ close $10,774.93 vs 09:30 $10,923.80 (session -140.40) | 16:00 close · cash $1,398.23 · equity $10,774.93 vs 09:30 $10,923.80 (-148.87; session marks -140.40) · 3 name(s) marked open→close (per-name table). ACMR×93 09:30 $81.65 → close $80.49 -107.88; MU×1 09:30 $967.01 → close $935.39 -31.62; LRCX×3 09:30 $318.88 → close $318.58 -0.90 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,398.23 | ▼ 09:30 equity $10,643.72 vs yday $10,774.93 (-131.21) | 09:30 open · cash $1,398.23 (unchanged overnight, no fees) · equity $10,643.72 vs prior close $10,774.93 (-131.21) · 3 name(s) re-marked at the open (per-name table). ACMR×93 yday $80.49 → 09:30 $79.27 -113.46; MU×1 yday $935.39 → 09:30 $919.29 -16.10; LRCX×3 yday $318.58 → 09:30 $318.03 -1.65 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 93 | $79.27 | $2.34 | $-225.95 | $8,768.00 | ▼ -225.95 after sell → book $10,641.38; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $9,685.27 | ▼ -51.73 after sell → book $10,639.36; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 3 | $318.03 | $2.02 | $-6.57 | $10,637.34 | ▼ -6.57 after sell → book $10,637.34; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 22 | $324.41 | $2.06 | — | $3,498.27 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $7446.14 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $2,503.94 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1063.73 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $1,701.10 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1063.73 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,701.10 | ▼ close $10,415.51 vs 09:30 $10,643.72 (session -215.77) | 16:00 close · cash $1,701.10 · equity $10,415.51 vs 09:30 $10,643.72 (-228.21; session marks -215.77) · 3 name(s) marked open→close (per-name table). KEYS×22 09:30 $324.41 → close $319.97 -97.68; SMTC×7 09:30 $141.76 → close $131.17 -74.13; CIEN×2 09:30 $400.42 → close $378.44 -43.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,701.10 | ▲ 09:30 equity $10,478.86 vs yday $10,415.51 (+63.35) | 09:30 open · cash $1,701.10 (unchanged overnight, no fees) · equity $10,478.86 vs prior close $10,415.51 (+63.35) · 3 name(s) re-marked at the open (per-name table). KEYS×22 yday $319.97 → 09:30 $322.49 +55.44; SMTC×7 yday $131.17 → 09:30 $132.30 +7.91; CIEN×2 yday $378.44 → 09:30 $378.44 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 22 | $322.49 | $2.12 | $-46.42 | $8,793.76 | ▼ -46.42 after sell → book $10,476.74; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 7 | $132.30 | $2.03 | $-70.26 | $9,717.83 | ▼ -70.26 after sell → book $10,474.71; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 2 | $378.44 | $2.02 | $-47.97 | $10,472.69 | ▼ -47.97 after sell → book $10,472.69; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,472.69 | ▲ close $10,472.69 vs 09:30 $10,478.86 (session +0.00) | 16:00 close · cash $10,472.69 · no lots left · equity $10,472.69. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,472.69 | ▲ 09:30 equity $10,472.69 vs yday $10,472.69 (+0.00) | 09:30 open · cash $10,472.69 · no holdings · equity $10,472.69 vs prior close $10,472.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,472.69 | ▲ close $10,472.69 vs 09:30 $10,472.69 (session +0.00) | 16:00 close · cash $10,472.69 · no lots left · equity $10,472.69. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,472.69 | ▲ 09:30 equity $10,472.69 vs yday $10,472.69 (+0.00) | 09:30 open · cash $10,472.69 · no holdings · equity $10,472.69 vs prior close $10,472.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,472.69 | ▲ close $10,472.69 vs 09:30 $10,472.69 (session +0.00) | 16:00 close · cash $10,472.69 · no lots left · equity $10,472.69. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,472.69 | ▲ 09:30 equity $10,472.69 vs yday $10,472.69 (+0.00) | 09:30 open · cash $10,472.69 · no holdings · equity $10,472.69 vs prior close $10,472.69 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 20 | $351.74 | $2.05 | — | $3,435.84 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $7330.88 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $2,461.23 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1047.27 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 32 | $32.31 | $2.09 | — | $1,425.22 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1047.27 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 65 | $15.87 | $2.19 | — | $391.48 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1047.27 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $391.48 | ▲ close $10,743.08 vs 09:30 $10,472.69 (session +278.71) | 16:00 close · cash $391.48 · equity $10,743.08 vs 09:30 $10,472.69 (+270.39; session marks +278.71) · 4 name(s) marked open→close (per-name table). AVGO×20 09:30 $351.74 → close $357.16 +108.40; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×32 09:30 $32.31 → close $33.66 +43.20; FRNM×65 09:30 $15.87 → close $16.90 +66.95 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $391.48 | ▲ 09:30 equity $10,749.76 vs yday $10,743.08 (+6.68) | 09:30 open · cash $391.48 (unchanged overnight, no fees) · equity $10,749.76 vs prior close $10,743.08 (+6.68) · 4 name(s) re-marked at the open (per-name table). AVGO×20 yday $357.16 → 09:30 $359.70 +50.80; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×32 yday $33.66 → 09:30 $33.46 -6.40; FRNM×65 yday $16.90 → 09:30 $16.40 -32.50 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 20 | $359.70 | $2.12 | $+155.03 | $7,583.37 | ▲ +155.03 after sell → book $10,747.65; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $8,608.91 | ▲ +50.93 after sell → book $10,745.63; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 32 | $33.46 | $2.11 | $+32.61 | $9,677.53 | ▲ +32.61 after sell → book $10,743.53; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 25 | $263.36 | $2.06 | — | $3,091.46 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $6774.27 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 19 | $75.65 | $2.05 | — | $1,652.06 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1451.63 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 6 | $236.82 | $2.01 | — | $229.14 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+8.1; leftover $1451.63 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $229.14 | ▲ close $10,774.39 vs 09:30 $10,749.76 (session +36.98) | 16:00 close · cash $229.14 · equity $10,774.39 vs 09:30 $10,749.76 (+24.63; session marks +36.98) · 4 name(s) marked open→close (per-name table). FRNM×65 09:30 $16.40 → close $16.31 -5.85; CRM×25 09:30 $263.36 → close $259.23 -103.25; MRX×19 09:30 $75.65 → close $78.27 +49.78; BE×6 09:30 $236.82 → close $252.87 +96.30 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $229.14 | ▼ 09:30 equity $10,764.76 vs yday $10,774.39 (-9.63) | 09:30 open · cash $229.14 (unchanged overnight, no fees) · equity $10,764.76 vs prior close $10,774.39 (-9.63) · 4 name(s) re-marked at the open (per-name table). FRNM×65 yday $16.31 → 09:30 $16.74 +27.95; CRM×25 yday $259.23 → 09:30 $253.72 -137.75; MRX×19 yday $78.27 → 09:30 $78.84 +10.83; BE×6 yday $252.87 → 09:30 $267.76 +89.34 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 65 | $16.74 | $2.21 | $+52.16 | $1,315.03 | ▲ +52.16 after sell → book $10,762.55; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 25 | $253.72 | $2.13 | $-245.19 | $7,655.90 | ▼ -245.19 after sell → book $10,760.42; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 6 | $267.76 | $2.03 | $+181.60 | $9,260.43 | ▲ +181.60 after sell → book $10,758.39; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,260.43 | ▼ close $10,717.92 vs 09:30 $10,764.76 (session -40.47) | 16:00 close · cash $9,260.43 · equity $10,717.92 vs 09:30 $10,764.76 (-46.84; session marks -40.47) · 1 name(s) marked open→close (per-name table). MRX×19 09:30 $78.84 → close $76.71 -40.47 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,260.43 | ▼ 09:30 equity $10,715.83 vs yday $10,717.92 (-2.09) | 09:30 open · cash $9,260.43 (unchanged overnight, no fees) · equity $10,715.83 vs prior close $10,717.92 (-2.09) · 1 name(s) re-marked at the open (per-name table). MRX×19 yday $76.71 → 09:30 $76.60 -2.09 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 19 | $76.60 | $2.07 | $+13.93 | $10,713.76 | ▲ +13.93 after sell → book $10,713.76; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,713.76 | ▲ close $10,713.76 vs 09:30 $10,715.83 (session +0.00) | 16:00 close · cash $10,713.76 · no lots left · equity $10,713.76. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,713.76 | ▲ 09:30 equity $10,713.76 vs yday $10,713.76 (+0.00) | 09:30 open · cash $10,713.76 · no holdings · equity $10,713.76 vs prior close $10,713.76 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,713.76 | ▲ close $10,713.76 vs 09:30 $10,713.76 (session +0.00) | 16:00 close · cash $10,713.76 · no lots left · equity $10,713.76. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,713.76 | ▲ 09:30 equity $10,713.76 vs yday $10,713.76 (+0.00) | 09:30 open · cash $10,713.76 · no holdings · equity $10,713.76 vs prior close $10,713.76 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 65 | $164.43 | $2.19 | — | $23.63 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10713.76 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.63 | ▼ close $9,791.83 vs 09:30 $10,713.76 (session -919.75) | 16:00 close · cash $23.63 · equity $9,791.83 vs 09:30 $10,713.76 (-921.93; session marks -919.75) · 1 name(s) marked open→close (per-name table). ORCL×65 09:30 $164.43 → close $150.28 -919.75 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.63 | ▼ 09:30 equity $9,215.93 vs yday $9,791.83 (-575.90) | 09:30 open · cash $23.63 (unchanged overnight, no fees) · equity $9,215.93 vs prior close $9,791.83 (-575.90) · 1 name(s) re-marked at the open (per-name table). ORCL×65 yday $150.28 → 09:30 $141.42 -575.90 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 65 | $141.42 | $2.27 | $-1500.10 | $9,213.66 | ▼ -1,500.10 after sell → book $9,213.66; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,213.66 | ▲ close $9,213.66 vs 09:30 $9,215.93 (session +0.00) | 16:00 close · cash $9,213.66 · no lots left · equity $9,213.66. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1092.16 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1063.73 < 1 share @ 1306.03 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
