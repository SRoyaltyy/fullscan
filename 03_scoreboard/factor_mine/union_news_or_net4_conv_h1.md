# Factor mine action — `union_news_or_net4_conv_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `conviction` · sell `list` · S-boost `none` · OR news + net≥4; 70% leftover if #1 net ≥ 5

Cash book **-3.84%** ($9,616) · signal-only (no cash/fees) was +0.19%. Starts YES **0/23**. Fills 60 · skips 24 · realized $-384.16.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,615.85.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 531 | — | $13.18 | +0.00 | $13.92 | +392.94 | +392.94 | +0.00 | +392.94 |
| 2026-08-14 | `ANGX` | 232 | — | $4.31 | +0.00 | $4.37 | +13.92 | +13.92 | +0.00 | +13.92 |
| 2026-08-14 | `MH` | 73 | — | $13.55 | +0.00 | $13.10 | -32.85 | -32.85 | +0.00 | -32.85 |
| 2026-08-17 | `HLIT` | 531 | $13.92 | $13.84 | -42.48 | — | +0.00 | -42.48 | +350.46 | — |
| 2026-08-17 | `ANGX` | 232 | $4.37 | $4.60 | +53.36 | — | +0.00 | +53.36 | +67.28 | — |
| 2026-08-17 | `MH` | 73 | $13.10 | $13.16 | +4.38 | — | +0.00 | +4.38 | -28.47 | — |
| 2026-08-17 | `DVN` | 112 | — | $46.18 | +0.00 | $47.57 | +155.68 | +155.68 | +0.00 | +155.68 |
| 2026-08-17 | `EOG` | 24 | — | $142.77 | +0.00 | $146.15 | +81.12 | +81.12 | +0.00 | +81.12 |
| 2026-08-17 | `FANG` | 8 | — | $202.70 | +0.00 | $206.29 | +28.72 | +28.72 | +0.00 | +28.72 |
| 2026-08-18 | `DVN` | 112 | $47.57 | $48.00 | +48.16 | — | +0.00 | +48.16 | +203.84 | — |
| 2026-08-18 | `EOG` | 24 | $146.15 | $148.04 | +45.36 | — | +0.00 | +45.36 | +126.48 | — |
| 2026-08-18 | `FANG` | 8 | $206.29 | $208.93 | +21.12 | — | +0.00 | +21.12 | +49.84 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 82 | — | $91.01 | +0.00 | $93.63 | +214.84 | +214.84 | +0.00 | +214.84 |
| 2026-08-20 | `APA` | 23 | — | $44.76 | +0.00 | $44.39 | -8.51 | -8.51 | +0.00 | -8.51 |
| 2026-08-20 | `AUTL` | 434 | — | $2.47 | +0.00 | $2.46 | -4.34 | -4.34 | +0.00 | -4.34 |
| 2026-08-20 | `CRSP` | 18 | — | $58.73 | +0.00 | $58.12 | -10.98 | -10.98 | +0.00 | -10.98 |
| 2026-08-21 | `BHP` | 82 | $93.63 | $95.72 | +171.38 | — | +0.00 | +171.38 | +386.22 | — |
| 2026-08-21 | `APA` | 23 | $44.39 | $44.52 | +2.99 | — | +0.00 | +2.99 | -5.52 | — |
| 2026-08-21 | `AUTL` | 434 | $2.46 | $2.47 | +4.34 | $2.41 | -26.04 | -21.70 | +0.00 | -26.04 |
| 2026-08-21 | `CRSP` | 18 | $58.12 | $59.72 | +28.80 | $59.50 | -3.96 | +24.84 | +17.82 | +13.86 |
| 2026-08-21 | `AU` | 52 | — | $119.43 | +0.00 | $121.22 | +93.08 | +93.08 | +0.00 | +93.08 |
| 2026-08-21 | `FUTU` | 23 | — | $115.18 | +0.00 | $123.64 | +194.58 | +194.58 | +0.00 | +194.58 |
| 2026-08-24 | `AUTL` | 434 | $2.41 | $2.40 | -4.34 | — | +0.00 | -4.34 | -30.38 | — |
| 2026-08-24 | `CRSP` | 18 | $59.50 | $58.75 | -13.50 | $57.08 | -30.15 | -43.65 | +0.36 | -29.79 |
| 2026-08-24 | `AU` | 52 | $121.22 | $120.51 | -36.92 | — | +0.00 | -36.92 | +56.16 | — |
| 2026-08-24 | `FUTU` | 23 | $123.64 | $121.00 | -60.72 | — | +0.00 | -60.72 | +133.86 | — |
| 2026-08-25 | `CRSP` | 18 | $57.08 | $57.93 | +15.39 | — | +0.00 | +15.39 | -14.40 | — |
| 2026-08-25 | `AU` | 66 | — | $118.52 | +0.00 | $123.39 | +321.42 | +321.42 | +0.00 | +321.42 |
| 2026-08-25 | `FCX` | 21 | — | $77.13 | +0.00 | $79.91 | +58.38 | +58.38 | +0.00 | +58.38 |
| 2026-08-25 | `EZPW` | 48 | — | $35.05 | +0.00 | $35.23 | +8.64 | +8.64 | +0.00 | +8.64 |
| 2026-08-26 | `AU` | 66 | $123.39 | $119.80 | -236.94 | — | +0.00 | -236.94 | +84.48 | — |
| 2026-08-26 | `FCX` | 21 | $79.91 | $79.34 | -11.97 | — | +0.00 | -11.97 | +46.41 | — |
| 2026-08-26 | `EZPW` | 48 | $35.23 | $35.70 | +22.56 | — | +0.00 | +22.56 | +31.20 | — |
| 2026-08-26 | `FNV` | 42 | — | $267.02 | +0.00 | $267.37 | +14.70 | +14.70 | +0.00 | +14.70 |
| 2026-08-27 | `FNV` | 42 | $267.37 | $267.23 | -5.88 | — | +0.00 | -5.88 | +8.82 | — |
| 2026-08-27 | `ACMR` | 97 | — | $81.65 | +0.00 | $80.49 | -112.52 | -112.52 | +0.00 | -112.52 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `LRCX` | 3 | — | $318.88 | +0.00 | $318.58 | -0.90 | -0.90 | +0.00 | -0.90 |
| 2026-08-28 | `ACMR` | 97 | $80.49 | $79.27 | -118.34 | — | +0.00 | -118.34 | -230.86 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `LRCX` | 3 | $318.58 | $318.03 | -1.65 | — | +0.00 | -1.65 | -2.55 | — |
| 2026-08-28 | `KEYS` | 23 | — | $324.41 | +0.00 | $319.97 | -102.12 | -102.12 | +0.00 | -102.12 |
| 2026-08-28 | `SMTC` | 7 | — | $141.76 | +0.00 | $131.17 | -74.13 | -74.13 | +0.00 | -74.13 |
| 2026-08-28 | `CIEN` | 2 | — | $400.42 | +0.00 | $378.44 | -43.96 | -43.96 | +0.00 | -43.96 |
| 2026-08-31 | `KEYS` | 23 | $319.97 | $322.49 | +57.96 | — | +0.00 | +57.96 | -44.16 | — |
| 2026-08-31 | `SMTC` | 7 | $131.17 | $132.30 | +7.91 | — | +0.00 | +7.91 | -66.22 | — |
| 2026-08-31 | `CIEN` | 2 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -43.96 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 21 | — | $351.74 | +0.00 | $357.16 | +113.82 | +113.82 | +0.00 | +113.82 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 33 | — | $32.31 | +0.00 | $33.66 | +44.55 | +44.55 | +0.00 | +44.55 |
| 2026-09-03 | `FRNM` | 68 | — | $15.87 | +0.00 | $16.90 | +70.04 | +70.04 | +0.00 | +70.04 |
| 2026-09-04 | `AVGO` | 21 | $357.16 | $359.70 | +53.34 | — | +0.00 | +53.34 | +167.16 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `CXW` | 33 | $33.66 | $33.46 | -6.60 | — | +0.00 | -6.60 | +37.95 | — |
| 2026-09-04 | `FRNM` | 68 | $16.90 | $16.40 | -34.00 | $16.31 | -6.12 | -40.12 | +36.04 | +29.92 |
| 2026-09-04 | `CRM` | 26 | — | $263.36 | +0.00 | $259.23 | -107.38 | -107.38 | +0.00 | -107.38 |
| 2026-09-04 | `MRX` | 19 | — | $75.65 | +0.00 | $78.27 | +49.78 | +49.78 | +0.00 | +49.78 |
| 2026-09-04 | `BE` | 6 | — | $236.82 | +0.00 | $252.87 | +96.30 | +96.30 | +0.00 | +96.30 |
| 2026-09-08 | `FRNM` | 68 | $16.31 | $16.74 | +29.24 | — | +0.00 | +29.24 | +59.16 | — |
| 2026-09-08 | `CRM` | 26 | $259.23 | $253.72 | -143.26 | — | +0.00 | -143.26 | -250.64 | — |
| 2026-09-08 | `MRX` | 19 | $78.27 | $78.84 | +10.83 | $76.71 | -40.47 | -29.64 | +60.61 | +20.14 |
| 2026-09-08 | `BE` | 6 | $252.87 | $267.76 | +89.34 | — | +0.00 | +89.34 | +185.64 | — |
| 2026-09-09 | `MRX` | 19 | $76.71 | $76.60 | -2.09 | — | +0.00 | -2.09 | +18.05 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 67 | — | $164.43 | +0.00 | $150.28 | -948.05 | -948.05 | +0.00 | -948.05 |
| 2026-09-14 | `ORCL` | 67 | $150.28 | $141.42 | -593.62 | — | +0.00 | -593.62 | -1541.67 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +374.01 | HLIT, ANGX, MH | — | $1,000.30 | $10,361.96 | HLIT×531, ANGX×232, MH×73 |
| 2026-08-17 | +2.25 | $1,000.30 | HLIT×531, ANGX×232, MH×73 | $10,377.22 | +15.26 | +265.52 | DVN, EOG, FANG | HLIT, ANGX, MH | $138.31 | $10,624.07 | DVN×112, EOG×24, FANG×8 |
| 2026-08-18 | -6.20 | $138.31 | DVN×112, EOG×24, FANG×8 | $10,738.71 | +114.64 | +0.00 | — | DVN, EOG, FANG | $10,732.18 | $10,732.18 | — |
| 2026-08-19 | -7.20 | $10,732.18 | — | $10,732.18 | +0.00 | +0.00 | — | — | $10,732.18 | $10,732.18 | — |
| 2026-08-20 | +1.12 | $10,732.18 | — | $10,732.18 | +0.00 | +191.01 | BHP, APA, AUTL, CRSP | — | $98.82 | $10,911.25 | BHP×82, APA×23, AUTL×434, CRSP×18 |
| 2026-08-21 | +3.25 | $98.82 | BHP×82, APA×23, AUTL×434, CRSP×18 | $11,118.76 | +207.51 | +257.66 | AU, FUTU | BHP, APA | $103.73 | $11,367.83 | AUTL×434, CRSP×18, AU×52, FUTU×23 |
| 2026-08-24 | -5.17 | $103.73 | AUTL×434, CRSP×18, AU×52, FUTU×23 | $11,252.35 | -115.48 | -30.15 | — | AUTL, AU, FUTU | $10,184.87 | $11,212.22 | CRSP×18 |
| 2026-08-25 | +1.80 | $10,184.87 | CRSP×18 | $11,227.61 | +15.39 | +388.44 | AU, FCX, EZPW | CRSP | $94.72 | $11,607.61 | AU×66, FCX×21, EZPW×48 |
| 2026-08-26 | +2.02 | $94.72 | AU×66, FCX×21, EZPW×48 | $11,381.26 | -226.35 | +14.70 | FNV | AU, FCX, EZPW | $157.81 | $11,387.35 | FNV×42 |
| 2026-08-27 | — | $157.81 | FNV×42 | $11,381.47 | -5.88 | -145.04 | ACMR, MU, LRCX | FNV | $1,529.28 | $11,227.94 | ACMR×97, MU×1, LRCX×3 |
| 2026-08-28 | +0.75 | $1,529.28 | ACMR×97, MU×1, LRCX×3 | $11,091.85 | -136.09 | -220.21 | KEYS, SMTC, CIEN | ACMR, MU, LRCX | $1,824.80 | $10,859.18 | KEYS×23, SMTC×7, CIEN×2 |
| 2026-08-31 | -5.85 | $1,824.80 | KEYS×23, SMTC×7, CIEN×2 | $10,925.05 | +65.87 | +0.00 | — | KEYS, SMTC, CIEN | $10,918.88 | $10,918.88 | — |
| 2026-09-01 | -6.30 | $10,918.88 | — | $10,918.88 | -0.00 | +0.00 | — | — | $10,918.88 | $10,918.88 | — |
| 2026-09-02 | -3.83 | $10,918.88 | — | $10,918.88 | -0.00 | +0.00 | — | — | $10,918.88 | $10,918.88 | — |
| 2026-09-03 | -0.90 | $10,918.88 | — | $10,918.88 | -0.00 | +288.57 | AVGO, DELL, CXW, FRNM | — | $406.00 | $11,199.12 | AVGO×21, DELL×2, CXW×33, FRNM×68 |
| 2026-09-04 | +2.25 | $406.00 | AVGO×21, DELL×2, CXW×33, FRNM×68 | $11,206.64 | +7.52 | +32.58 | CRM, MRX, BE | AVGO, DELL, CXW | $373.43 | $11,226.84 | FRNM×68, CRM×26, MRX×19, BE×6 |
| 2026-09-08 | -11.47 | $373.43 | FRNM×68, CRM×26, MRX×19, BE×6 | $11,212.99 | -13.85 | -40.47 | — | FRNM, CRM, BE | $9,708.66 | $11,166.15 | MRX×19 |
| 2026-09-09 | -13.95 | $9,708.66 | MRX×19 | $11,164.06 | -2.09 | +0.00 | — | MRX | $11,161.99 | $11,161.99 | — |
| 2026-09-10 | -13.28 | $11,161.99 | — | $11,161.99 | -0.00 | +0.00 | — | — | $11,161.99 | $11,161.99 | — |
| 2026-09-11 | +0.50 | $11,161.99 | — | $11,161.99 | -0.00 | -948.05 | ORCL | — | $142.99 | $10,211.75 | ORCL×67 |
| 2026-09-14 | -11.00 | $142.99 | ORCL×67 | $9,618.13 | -593.62 | +0.00 | — | ORCL | $9,615.85 | $9,615.85 | — |
| 2026-09-15 | -3.84 | $9,615.85 | — | $9,615.85 | -0.00 | +0.00 | — | — | $9,615.85 | $9,615.85 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 531 | $13.18 | $6.85 | — | $2,994.57 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $7000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 232 | $4.31 | $2.99 | — | $1,991.66 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 73 | $13.55 | $2.21 | — | $1,000.30 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,000.30 | ▲ close $10,361.96 vs 09:30 $10,000.00 (session +374.01) | 16:00 close · cash $1,000.30 · equity $10,361.96 vs 09:30 $10,000.00 (+361.96; session marks +374.01) · 3 name(s) marked open→close (per-name table). HLIT×531 09:30 $13.18 → close $13.92 +392.94; ANGX×232 09:30 $4.31 → close $4.37 +13.92; MH×73 09:30 $13.55 → close $13.10 -32.85 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,000.30 | ▲ 09:30 equity $10,377.22 vs yday $10,361.96 (+15.26) | 09:30 open · cash $1,000.30 (unchanged overnight, no fees) · equity $10,377.22 vs prior close $10,361.96 (+15.26) · 3 name(s) re-marked at the open (per-name table). HLIT×531 yday $13.92 → 09:30 $13.84 -42.48; ANGX×232 yday $4.37 → 09:30 $4.60 +53.36; MH×73 yday $13.10 → 09:30 $13.16 +4.38 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 531 | $13.84 | $7.00 | $+336.61 | $8,342.34 | ▲ +336.61 after sell → book $10,370.22; vs 09:30 mark -7.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 232 | $4.60 | $3.04 | $+61.25 | $9,406.50 | ▲ +61.25 after sell → book $10,367.18; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 73 | $13.16 | $2.23 | $-32.91 | $10,364.95 | ▼ -32.91 after sell → book $10,364.95; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 112 | $46.18 | $2.33 | — | $5,190.46 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+6.7; leftover $5182.47 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 24 | $142.77 | $2.06 | — | $1,761.92 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3454.98 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 8 | $202.70 | $2.01 | — | $138.31 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+8.3; leftover $1727.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $138.31 | ▲ close $10,624.07 vs 09:30 $10,377.22 (session +265.52) | 16:00 close · cash $138.31 · equity $10,624.07 vs 09:30 $10,377.22 (+246.85; session marks +265.52) · 3 name(s) marked open→close (per-name table). DVN×112 09:30 $46.18 → close $47.57 +155.68; EOG×24 09:30 $142.77 → close $146.15 +81.12; FANG×8 09:30 $202.70 → close $206.29 +28.72 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $138.31 | ▲ 09:30 equity $10,738.71 vs yday $10,624.07 (+114.64) | 09:30 open · cash $138.31 (unchanged overnight, no fees) · equity $10,738.71 vs prior close $10,624.07 (+114.64) · 3 name(s) re-marked at the open (per-name table). DVN×112 yday $47.57 → 09:30 $48.00 +48.16; EOG×24 yday $146.15 → 09:30 $148.04 +45.36; FANG×8 yday $206.29 → 09:30 $208.93 +21.12 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 112 | $48.00 | $2.39 | $+199.13 | $5,511.92 | ▲ +199.13 after sell → book $10,736.32; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 24 | $148.04 | $2.10 | $+122.32 | $9,062.78 | ▲ +122.32 after sell → book $10,734.22; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 8 | $208.93 | $2.04 | $+45.79 | $10,732.18 | ▲ +45.79 after sell → book $10,732.18; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,732.18 | ▲ close $10,732.18 vs 09:30 $10,738.71 (session +0.00) | 16:00 close · cash $10,732.18 · no lots left · equity $10,732.18. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,732.18 | ▲ 09:30 equity $10,732.18 vs yday $10,732.18 (+0.00) | 09:30 open · cash $10,732.18 · no holdings · equity $10,732.18 vs prior close $10,732.18 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,732.18 | ▲ close $10,732.18 vs 09:30 $10,732.18 (session +0.00) | 16:00 close · cash $10,732.18 · no lots left · equity $10,732.18. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,732.18 | ▲ 09:30 equity $10,732.18 vs yday $10,732.18 (+0.00) | 09:30 open · cash $10,732.18 · no holdings · equity $10,732.18 vs prior close $10,732.18 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 82 | $91.01 | $2.24 | — | $3,267.13 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $7512.53 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 23 | $44.76 | $2.06 | — | $2,235.59 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1073.22 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 434 | $2.47 | $5.60 | — | $1,158.01 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1073.22 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 18 | $58.73 | $2.04 | — | $98.82 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1073.22 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.82 | ▲ close $10,911.25 vs 09:30 $10,732.18 (session +191.01) | 16:00 close · cash $98.82 · equity $10,911.25 vs 09:30 $10,732.18 (+179.07; session marks +191.01) · 4 name(s) marked open→close (per-name table). BHP×82 09:30 $91.01 → close $93.63 +214.84; APA×23 09:30 $44.76 → close $44.39 -8.51; AUTL×434 09:30 $2.47 → close $2.46 -4.34; CRSP×18 09:30 $58.73 → close $58.12 -10.98 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.82 | ▲ 09:30 equity $11,118.76 vs yday $10,911.25 (+207.51) | 09:30 open · cash $98.82 (unchanged overnight, no fees) · equity $11,118.76 vs prior close $10,911.25 (+207.51) · 4 name(s) re-marked at the open (per-name table). BHP×82 yday $93.63 → 09:30 $95.72 +171.38; APA×23 yday $44.39 → 09:30 $44.52 +2.99; AUTL×434 yday $2.46 → 09:30 $2.47 +4.34; CRSP×18 yday $58.12 → 09:30 $59.72 +28.80 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 82 | $95.72 | $2.31 | $+381.67 | $7,945.55 | ▲ +381.67 after sell → book $11,116.45; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 23 | $44.52 | $2.08 | $-9.66 | $8,967.43 | ▼ -9.66 after sell → book $11,114.37; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 52 | $119.43 | $2.15 | — | $2,754.93 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $6277.20 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 23 | $115.18 | $2.06 | — | $103.73 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $2690.23 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.73 | ▲ close $11,367.83 vs 09:30 $11,118.76 (session +257.66) | 16:00 close · cash $103.73 · equity $11,367.83 vs 09:30 $11,118.76 (+249.07; session marks +257.66) · 4 name(s) marked open→close (per-name table). AUTL×434 09:30 $2.47 → close $2.41 -26.04; CRSP×18 09:30 $59.72 → close $59.50 -3.96; AU×52 09:30 $119.43 → close $121.22 +93.08; FUTU×23 09:30 $115.18 → close $123.64 +194.58 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.73 | ▼ 09:30 equity $11,252.35 vs yday $11,367.83 (-115.48) | 09:30 open · cash $103.73 (unchanged overnight, no fees) · equity $11,252.35 vs prior close $11,367.83 (-115.48) · 4 name(s) re-marked at the open (per-name table). AUTL×434 yday $2.41 → 09:30 $2.40 -4.34; CRSP×18 yday $59.50 → 09:30 $58.75 -13.50; AU×52 yday $121.22 → 09:30 $120.51 -36.92; FUTU×23 yday $123.64 → 09:30 $121.00 -60.72 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 434 | $2.40 | $5.68 | $-41.66 | $1,139.65 | ▼ -41.66 after sell → book $11,246.67; vs 09:30 mark -5.68 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 52 | $120.51 | $2.21 | $+51.81 | $7,403.96 | ▲ +51.81 after sell → book $11,244.46; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 23 | $121.00 | $2.09 | $+129.71 | $10,184.87 | ▲ +129.71 after sell → book $11,242.37; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,184.87 | ▼ close $11,212.22 vs 09:30 $11,252.35 (session -30.15) | 16:00 close · cash $10,184.87 · equity $11,212.22 vs 09:30 $11,252.35 (-40.13; session marks -30.15) · 1 name(s) marked open→close (per-name table). CRSP×18 09:30 $58.75 → close $57.08 -30.15 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,184.87 | ▲ 09:30 equity $11,227.61 vs yday $11,212.22 (+15.39) | 09:30 open · cash $10,184.87 (unchanged overnight, no fees) · equity $11,227.61 vs prior close $11,212.22 (+15.39) · 1 name(s) re-marked at the open (per-name table). CRSP×18 yday $57.08 → 09:30 $57.93 +15.39 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 18 | $57.93 | $2.06 | $-18.51 | $11,225.55 | ▼ -18.51 after sell → book $11,225.55; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 66 | $118.52 | $2.19 | — | $3,401.04 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $7857.88 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 21 | $77.13 | $2.05 | — | $1,779.25 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1683.83 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 48 | $35.05 | $2.13 | — | $94.72 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1683.83 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.72 | ▲ close $11,607.61 vs 09:30 $11,227.61 (session +388.44) | 16:00 close · cash $94.72 · equity $11,607.61 vs 09:30 $11,227.61 (+380.00; session marks +388.44) · 3 name(s) marked open→close (per-name table). AU×66 09:30 $118.52 → close $123.39 +321.42; FCX×21 09:30 $77.13 → close $79.91 +58.38; EZPW×48 09:30 $35.05 → close $35.23 +8.64 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.72 | ▼ 09:30 equity $11,381.26 vs yday $11,607.61 (-226.35) | 09:30 open · cash $94.72 (unchanged overnight, no fees) · equity $11,381.26 vs prior close $11,607.61 (-226.35) · 3 name(s) re-marked at the open (per-name table). AU×66 yday $123.39 → 09:30 $119.80 -236.94; FCX×21 yday $79.91 → 09:30 $79.34 -11.97; EZPW×48 yday $35.23 → 09:30 $35.70 +22.56 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 66 | $119.80 | $2.26 | $+80.03 | $7,999.26 | ▲ +80.03 after sell → book $11,379.00; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 21 | $79.34 | $2.08 | $+42.28 | $9,663.32 | ▲ +42.28 after sell → book $11,376.92; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 48 | $35.70 | $2.16 | $+26.91 | $11,374.76 | ▲ +26.91 after sell → book $11,374.76; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 42 | $267.02 | $2.12 | — | $157.81 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $11374.76 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.81 | ▲ close $11,387.35 vs 09:30 $11,381.26 (session +14.70) | 16:00 close · cash $157.81 · equity $11,387.35 vs 09:30 $11,381.26 (+6.09; session marks +14.70) · 1 name(s) marked open→close (per-name table). FNV×42 09:30 $267.02 → close $267.37 +14.70 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.81 | ▼ 09:30 equity $11,381.47 vs yday $11,387.35 (-5.88) | 09:30 open · cash $157.81 (unchanged overnight, no fees) · equity $11,381.47 vs prior close $11,387.35 (-5.88) · 1 name(s) re-marked at the open (per-name table). FNV×42 yday $267.37 → 09:30 $267.23 -5.88 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 42 | $267.23 | $2.22 | $+4.49 | $11,379.25 | ▲ +4.49 after sell → book $11,379.25; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 97 | $81.65 | $2.28 | — | $3,456.92 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $7965.48 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $2,487.92 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1137.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 3 | $318.88 | $2.00 | — | $1,529.28 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1137.93 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,529.28 | ▼ close $11,227.94 vs 09:30 $11,381.47 (session -145.04) | 16:00 close · cash $1,529.28 · equity $11,227.94 vs 09:30 $11,381.47 (-153.53; session marks -145.04) · 3 name(s) marked open→close (per-name table). ACMR×97 09:30 $81.65 → close $80.49 -112.52; MU×1 09:30 $967.01 → close $935.39 -31.62; LRCX×3 09:30 $318.88 → close $318.58 -0.90 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,529.28 | ▼ 09:30 equity $11,091.85 vs yday $11,227.94 (-136.09) | 09:30 open · cash $1,529.28 (unchanged overnight, no fees) · equity $11,091.85 vs prior close $11,227.94 (-136.09) · 3 name(s) re-marked at the open (per-name table). ACMR×97 yday $80.49 → 09:30 $79.27 -118.34; MU×1 yday $935.39 → 09:30 $919.29 -16.10; LRCX×3 yday $318.58 → 09:30 $318.03 -1.65 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 97 | $79.27 | $2.36 | $-235.50 | $9,216.11 | ▼ -235.50 after sell → book $11,089.49; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $10,133.39 | ▼ -51.73 after sell → book $11,087.48; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 3 | $318.03 | $2.02 | $-6.57 | $11,085.46 | ▼ -6.57 after sell → book $11,085.46; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 23 | $324.41 | $2.06 | — | $3,621.97 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $7759.82 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $2,627.64 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1108.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $1,824.80 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1108.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,824.80 | ▼ close $10,859.18 vs 09:30 $11,091.85 (session -220.21) | 16:00 close · cash $1,824.80 · equity $10,859.18 vs 09:30 $11,091.85 (-232.67; session marks -220.21) · 3 name(s) marked open→close (per-name table). KEYS×23 09:30 $324.41 → close $319.97 -102.12; SMTC×7 09:30 $141.76 → close $131.17 -74.13; CIEN×2 09:30 $400.42 → close $378.44 -43.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,824.80 | ▲ 09:30 equity $10,925.05 vs yday $10,859.18 (+65.87) | 09:30 open · cash $1,824.80 (unchanged overnight, no fees) · equity $10,925.05 vs prior close $10,859.18 (+65.87) · 3 name(s) re-marked at the open (per-name table). KEYS×23 yday $319.97 → 09:30 $322.49 +57.96; SMTC×7 yday $131.17 → 09:30 $132.30 +7.91; CIEN×2 yday $378.44 → 09:30 $378.44 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 23 | $322.49 | $2.13 | $-48.35 | $9,239.94 | ▼ -48.35 after sell → book $10,922.92; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 7 | $132.30 | $2.03 | $-70.26 | $10,164.01 | ▼ -70.26 after sell → book $10,920.89; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 2 | $378.44 | $2.02 | $-47.97 | $10,918.88 | ▼ -47.97 after sell → book $10,918.88; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,918.88 | ▲ close $10,918.88 vs 09:30 $10,925.05 (session +0.00) | 16:00 close · cash $10,918.88 · no lots left · equity $10,918.88. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,918.88 | ▲ 09:30 equity $10,918.88 vs yday $10,918.88 (-0.00) | 09:30 open · cash $10,918.88 · no holdings · equity $10,918.88 vs prior close $10,918.88 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,918.88 | ▲ close $10,918.88 vs 09:30 $10,918.88 (session +0.00) | 16:00 close · cash $10,918.88 · no lots left · equity $10,918.88. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,918.88 | ▲ 09:30 equity $10,918.88 vs yday $10,918.88 (-0.00) | 09:30 open · cash $10,918.88 · no holdings · equity $10,918.88 vs prior close $10,918.88 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,918.88 | ▲ close $10,918.88 vs 09:30 $10,918.88 (session +0.00) | 16:00 close · cash $10,918.88 · no lots left · equity $10,918.88. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,918.88 | ▲ 09:30 equity $10,918.88 vs yday $10,918.88 (-0.00) | 09:30 open · cash $10,918.88 · no holdings · equity $10,918.88 vs prior close $10,918.88 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 21 | $351.74 | $2.05 | — | $3,530.28 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $7643.21 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $2,555.67 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1091.89 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 33 | $32.31 | $2.09 | — | $1,487.35 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1091.89 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 68 | $15.87 | $2.19 | — | $406.00 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1091.89 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $406.00 | ▲ close $11,199.12 vs 09:30 $10,918.88 (session +288.57) | 16:00 close · cash $406.00 · equity $11,199.12 vs 09:30 $10,918.88 (+280.24; session marks +288.57) · 4 name(s) marked open→close (per-name table). AVGO×21 09:30 $351.74 → close $357.16 +113.82; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×33 09:30 $32.31 → close $33.66 +44.55; FRNM×68 09:30 $15.87 → close $16.90 +70.04 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $406.00 | ▲ 09:30 equity $11,206.64 vs yday $11,199.12 (+7.52) | 09:30 open · cash $406.00 (unchanged overnight, no fees) · equity $11,206.64 vs prior close $11,199.12 (+7.52) · 4 name(s) re-marked at the open (per-name table). AVGO×21 yday $357.16 → 09:30 $359.70 +53.34; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×33 yday $33.66 → 09:30 $33.46 -6.60; FRNM×68 yday $16.90 → 09:30 $16.40 -34.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 21 | $359.70 | $2.12 | $+162.98 | $7,957.57 | ▲ +162.98 after sell → book $11,204.51; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $8,983.12 | ▲ +50.93 after sell → book $11,202.50; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 33 | $33.46 | $2.11 | $+33.75 | $10,085.19 | ▲ +33.75 after sell → book $11,200.39; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 26 | $263.36 | $2.07 | — | $3,235.76 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $7059.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 19 | $75.65 | $2.05 | — | $1,796.36 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1512.78 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 6 | $236.82 | $2.01 | — | $373.43 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+8.1; leftover $1512.78 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $373.43 | ▲ close $11,226.84 vs 09:30 $11,206.64 (session +32.58) | 16:00 close · cash $373.43 · equity $11,226.84 vs 09:30 $11,206.64 (+20.20; session marks +32.58) · 4 name(s) marked open→close (per-name table). FRNM×68 09:30 $16.40 → close $16.31 -6.12; CRM×26 09:30 $263.36 → close $259.23 -107.38; MRX×19 09:30 $75.65 → close $78.27 +49.78; BE×6 09:30 $236.82 → close $252.87 +96.30 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $373.43 | ▼ 09:30 equity $11,212.99 vs yday $11,226.84 (-13.85) | 09:30 open · cash $373.43 (unchanged overnight, no fees) · equity $11,212.99 vs prior close $11,226.84 (-13.85) · 4 name(s) re-marked at the open (per-name table). FRNM×68 yday $16.31 → 09:30 $16.74 +29.24; CRM×26 yday $259.23 → 09:30 $253.72 -143.26; MRX×19 yday $78.27 → 09:30 $78.84 +10.83; BE×6 yday $252.87 → 09:30 $267.76 +89.34 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 68 | $16.74 | $2.22 | $+54.75 | $1,509.54 | ▲ +54.75 after sell → book $11,210.78; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 26 | $253.72 | $2.13 | $-254.84 | $8,104.13 | ▼ -254.84 after sell → book $11,208.65; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 6 | $267.76 | $2.03 | $+181.60 | $9,708.66 | ▲ +181.60 after sell → book $11,206.62; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,708.66 | ▼ close $11,166.15 vs 09:30 $11,212.99 (session -40.47) | 16:00 close · cash $9,708.66 · equity $11,166.15 vs 09:30 $11,212.99 (-46.84; session marks -40.47) · 1 name(s) marked open→close (per-name table). MRX×19 09:30 $78.84 → close $76.71 -40.47 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,708.66 | ▼ 09:30 equity $11,164.06 vs yday $11,166.15 (-2.09) | 09:30 open · cash $9,708.66 (unchanged overnight, no fees) · equity $11,164.06 vs prior close $11,166.15 (-2.09) · 1 name(s) re-marked at the open (per-name table). MRX×19 yday $76.71 → 09:30 $76.60 -2.09 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 19 | $76.60 | $2.07 | $+13.93 | $11,161.99 | ▲ +13.93 after sell → book $11,161.99; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,161.99 | ▲ close $11,161.99 vs 09:30 $11,164.06 (session +0.00) | 16:00 close · cash $11,161.99 · no lots left · equity $11,161.99. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,161.99 | ▲ 09:30 equity $11,161.99 vs yday $11,161.99 (-0.00) | 09:30 open · cash $11,161.99 · no holdings · equity $11,161.99 vs prior close $11,161.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,161.99 | ▲ close $11,161.99 vs 09:30 $11,161.99 (session +0.00) | 16:00 close · cash $11,161.99 · no lots left · equity $11,161.99. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,161.99 | ▲ 09:30 equity $11,161.99 vs yday $11,161.99 (-0.00) | 09:30 open · cash $11,161.99 · no holdings · equity $11,161.99 vs prior close $11,161.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 67 | $164.43 | $2.19 | — | $142.99 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $11161.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.99 | ▼ close $10,211.75 vs 09:30 $11,161.99 (session -948.05) | 16:00 close · cash $142.99 · equity $10,211.75 vs 09:30 $11,161.99 (-950.24; session marks -948.05) · 1 name(s) marked open→close (per-name table). ORCL×67 09:30 $164.43 → close $150.28 -948.05 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.99 | ▼ 09:30 equity $9,618.13 vs yday $10,211.75 (-593.62) | 09:30 open · cash $142.99 (unchanged overnight, no fees) · equity $9,618.13 vs prior close $10,211.75 (-593.62) · 1 name(s) re-marked at the open (per-name table). ORCL×67 yday $150.28 → 09:30 $141.42 -593.62 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 67 | $141.42 | $2.28 | $-1546.14 | $9,615.85 | ▼ -1,546.14 after sell → book $9,615.85; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,615.85 | ▲ close $9,615.85 vs 09:30 $9,618.13 (session +0.00) | 16:00 close · cash $9,615.85 · no lots left · equity $9,615.85. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,615.85 | ▲ 09:30 equity $9,615.85 vs yday $9,615.85 (-0.00) | 09:30 open · cash $9,615.85 · no holdings · equity $9,615.85 vs prior close $9,615.85 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,615.85 | ▲ close $9,615.85 vs 09:30 $9,615.85 (session +0.00) | 16:00 close · cash $9,615.85 · no lots left · equity $9,615.85. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1000.00 < 1 share @ 1646.93 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1137.93 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1108.55 < 1 share @ 1306.03 |
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
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
