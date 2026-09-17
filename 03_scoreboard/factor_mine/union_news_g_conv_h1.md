# Factor mine action — `union_news_g_conv_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `conviction` · sell `list` · S-boost `none` · news🟢 rank cameras; 70% of leftover if +9 −≤1

Cash book **+1.09%** ($10,109) · signal-only (no cash/fees) was +14.81%. Starts YES **2/25**. Fills 76 · skips 42 · realized $+109.19.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,109.22.

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
| 2026-08-17 | `DVN` | 89 | — | $46.18 | +0.00 | $47.57 | +123.71 | +123.71 | +0.00 | +123.71 |
| 2026-08-17 | `EOG` | 21 | — | $142.77 | +0.00 | $146.15 | +70.98 | +70.98 | +0.00 | +70.98 |
| 2026-08-17 | `FANG` | 10 | — | $202.70 | +0.00 | $206.29 | +35.90 | +35.90 | +0.00 | +35.90 |
| 2026-08-17 | `OUST` | 21 | — | $49.00 | +0.00 | $48.13 | -18.27 | -18.27 | +0.00 | -18.27 |
| 2026-08-18 | `DVN` | 89 | $47.57 | $48.00 | +38.27 | — | +0.00 | +38.27 | +161.98 | — |
| 2026-08-18 | `EOG` | 21 | $146.15 | $148.04 | +39.69 | — | +0.00 | +39.69 | +110.67 | — |
| 2026-08-18 | `FANG` | 10 | $206.29 | $208.93 | +26.40 | — | +0.00 | +26.40 | +62.30 | — |
| 2026-08-18 | `OUST` | 21 | $48.13 | $45.09 | -63.84 | — | +0.00 | -63.84 | -82.11 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 81 | — | $91.01 | +0.00 | $93.63 | +212.22 | +212.22 | +0.00 | +212.22 |
| 2026-08-20 | `APA` | 23 | — | $44.76 | +0.00 | $44.39 | -8.51 | -8.51 | +0.00 | -8.51 |
| 2026-08-20 | `AUTL` | 429 | — | $2.47 | +0.00 | $2.46 | -4.29 | -4.29 | +0.00 | -4.29 |
| 2026-08-20 | `CRSP` | 18 | — | $58.73 | +0.00 | $58.12 | -10.98 | -10.98 | +0.00 | -10.98 |
| 2026-08-21 | `BHP` | 81 | $93.63 | $95.72 | +169.29 | — | +0.00 | +169.29 | +381.51 | — |
| 2026-08-21 | `APA` | 23 | $44.39 | $44.52 | +2.99 | — | +0.00 | +2.99 | -5.52 | — |
| 2026-08-21 | `AUTL` | 429 | $2.46 | $2.47 | +4.29 | $2.41 | -25.74 | -21.45 | +0.00 | -25.74 |
| 2026-08-21 | `CRSP` | 18 | $58.12 | $59.72 | +28.80 | $59.50 | -3.96 | +24.84 | +17.82 | +13.86 |
| 2026-08-21 | `AU` | 51 | — | $119.43 | +0.00 | $121.22 | +91.29 | +91.29 | +0.00 | +91.29 |
| 2026-08-21 | `FUTU` | 23 | — | $115.18 | +0.00 | $123.64 | +194.58 | +194.58 | +0.00 | +194.58 |
| 2026-08-24 | `AUTL` | 429 | $2.41 | $2.40 | -4.29 | — | +0.00 | -4.29 | -30.03 | — |
| 2026-08-24 | `CRSP` | 18 | $59.50 | $58.75 | -13.50 | $57.08 | -30.15 | -43.65 | +0.36 | -29.79 |
| 2026-08-24 | `AU` | 51 | $121.22 | $120.51 | -36.21 | — | +0.00 | -36.21 | +55.08 | — |
| 2026-08-24 | `FUTU` | 23 | $123.64 | $121.00 | -60.72 | — | +0.00 | -60.72 | +133.86 | — |
| 2026-08-25 | `CRSP` | 18 | $57.08 | $57.93 | +15.39 | — | +0.00 | +15.39 | -14.40 | — |
| 2026-08-25 | `AU` | 65 | — | $118.52 | +0.00 | $123.39 | +316.55 | +316.55 | +0.00 | +316.55 |
| 2026-08-25 | `FCX` | 14 | — | $77.13 | +0.00 | $79.91 | +38.92 | +38.92 | +0.00 | +38.92 |
| 2026-08-25 | `EZPW` | 31 | — | $35.05 | +0.00 | $35.23 | +5.58 | +5.58 | +0.00 | +5.58 |
| 2026-08-25 | `RUM` | 117 | — | $9.42 | +0.00 | $10.23 | +94.77 | +94.77 | +0.00 | +94.77 |
| 2026-08-26 | `AU` | 65 | $123.39 | $119.80 | -233.35 | — | +0.00 | -233.35 | +83.20 | — |
| 2026-08-26 | `FCX` | 14 | $79.91 | $79.34 | -7.98 | — | +0.00 | -7.98 | +30.94 | — |
| 2026-08-26 | `EZPW` | 31 | $35.23 | $35.70 | +14.57 | — | +0.00 | +14.57 | +20.15 | — |
| 2026-08-26 | `RUM` | 117 | $10.23 | $10.07 | -18.72 | — | +0.00 | -18.72 | +76.05 | — |
| 2026-08-26 | `FNV` | 29 | — | $267.02 | +0.00 | $267.37 | +10.15 | +10.15 | +0.00 | +10.15 |
| 2026-08-26 | `TRLV` | 100 | — | $11.22 | +0.00 | $11.43 | +21.00 | +21.00 | +0.00 | +21.00 |
| 2026-08-26 | `CAPR` | 136 | — | $8.29 | +0.00 | $9.36 | +145.52 | +145.52 | +0.00 | +145.52 |
| 2026-08-26 | `FWRD` | 64 | — | $17.41 | +0.00 | $17.63 | +14.08 | +14.08 | +0.00 | +14.08 |
| 2026-08-27 | `FNV` | 29 | $267.37 | $267.23 | -4.06 | — | +0.00 | -4.06 | +6.09 | — |
| 2026-08-27 | `TRLV` | 100 | $11.43 | $11.38 | -5.00 | — | +0.00 | -5.00 | +16.00 | — |
| 2026-08-27 | `CAPR` | 136 | $9.36 | $9.19 | -23.12 | — | +0.00 | -23.12 | +122.40 | — |
| 2026-08-27 | `FWRD` | 64 | $17.63 | $17.60 | -1.92 | — | +0.00 | -1.92 | +12.16 | — |
| 2026-08-27 | `ACMR` | 97 | — | $81.65 | +0.00 | $80.49 | -112.52 | -112.52 | +0.00 | -112.52 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `LRCX` | 3 | — | $318.88 | +0.00 | $318.58 | -0.90 | -0.90 | +0.00 | -0.90 |
| 2026-08-28 | `ACMR` | 97 | $80.49 | $79.27 | -118.34 | — | +0.00 | -118.34 | -230.86 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `LRCX` | 3 | $318.58 | $318.03 | -1.65 | — | +0.00 | -1.65 | -2.55 | — |
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
| 2026-09-11 | `BAK` | 528 | — | $2.12 | +0.00 | $2.08 | -21.12 | -21.12 | +0.00 | -21.12 |
| 2026-09-11 | `AMTX` | 549 | — | $2.04 | +0.00 | $2.01 | -16.47 | -16.47 | +0.00 | -16.47 |
| 2026-09-14 | `ORCL` | 47 | $150.28 | $141.42 | -416.42 | — | +0.00 | -416.42 | -1081.47 | — |
| 2026-09-14 | `ADBE` | 4 | $252.23 | $261.51 | +37.12 | — | +0.00 | +37.12 | +77.36 | — |
| 2026-09-14 | `BAK` | 528 | $2.08 | $2.05 | -15.84 | — | +0.00 | -15.84 | -36.96 | — |
| 2026-09-14 | `AMTX` | 549 | $2.01 | $2.01 | +0.00 | — | +0.00 | +0.00 | -16.47 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +374.01 | HLIT, ANGX, MH | — | $1,000.30 | $10,361.96 | HLIT×531, ANGX×232, MH×73 |
| 2026-08-17 | +2.25 | $1,000.30 | HLIT×531, ANGX×232, MH×73 | $10,377.22 | +15.26 | +212.32 | DVN, EOG, FANG, OUST | HLIT, ANGX, MH | $192.38 | $10,568.89 | DVN×89, EOG×21, FANG×10, OUST×21 |
| 2026-08-18 | -6.20 | $192.38 | DVN×89, EOG×21, FANG×10, OUST×21 | $10,609.41 | +40.52 | +0.00 | — | DVN, EOG, FANG, OUST | $10,600.89 | $10,600.89 | — |
| 2026-08-19 | -7.20 | $10,600.89 | — | $10,600.89 | +0.00 | +0.00 | — | — | $10,600.89 | $10,600.89 | — |
| 2026-08-20 | +1.12 | $10,600.89 | — | $10,600.89 | +0.00 | +188.44 | BHP, APA, AUTL, CRSP | — | $70.96 | $10,777.46 | BHP×81, APA×23, AUTL×429, CRSP×18 |
| 2026-08-21 | +3.25 | $70.96 | BHP×81, APA×23, AUTL×429, CRSP×18 | $10,982.83 | +205.37 | +256.17 | AU, FUTU | BHP, APA | $99.58 | $11,230.41 | AUTL×429, CRSP×18, AU×51, FUTU×23 |
| 2026-08-24 | -5.17 | $99.58 | AUTL×429, CRSP×18, AU×51, FUTU×23 | $11,115.69 | -114.72 | -30.15 | — | AUTL, AU, FUTU | $10,048.28 | $11,075.63 | CRSP×18 |
| 2026-08-25 | +1.80 | $10,048.28 | CRSP×18 | $11,091.02 | +15.39 | +455.82 | AU, FCX, EZPW, RUM | CRSP | $108.01 | $11,536.14 | AU×65, FCX×14, EZPW×31, RUM×117 |
| 2026-08-26 | +2.02 | $108.01 | AU×65, FCX×14, EZPW×31, RUM×117 | $11,290.66 | -245.48 | +190.75 | FNV, TRLV, CAPR, FWRD | AU, FCX, EZPW, RUM | $165.67 | $11,463.68 | FNV×29, TRLV×100, CAPR×136, FWRD×64 |
| 2026-08-27 | — | $165.67 | FNV×29, TRLV×100, CAPR×136, FWRD×64 | $11,429.58 | -34.10 | -145.04 | ACMR, MU, LRCX | FNV, TRLV, CAPR, FWRD | $1,570.51 | $11,269.17 | ACMR×97, MU×1, LRCX×3 |
| 2026-08-28 | +0.75 | $1,570.51 | ACMR×97, MU×1, LRCX×3 | $11,133.08 | -136.09 | -224.65 | KEYS, SMTC, CIEN | ACMR, MU, LRCX | $1,541.62 | $10,895.97 | KEYS×24, SMTC×7, CIEN×2 |
| 2026-08-31 | -5.85 | $1,541.62 | KEYS×24, SMTC×7, CIEN×2 | $10,964.36 | +68.39 | +0.00 | — | KEYS, SMTC, CIEN | $10,958.18 | $10,958.18 | — |
| 2026-09-01 | -6.30 | $10,958.18 | — | $10,958.18 | -0.00 | +0.00 | — | — | $10,958.18 | $10,958.18 | — |
| 2026-09-02 | -3.83 | $10,958.18 | — | $10,958.18 | -0.00 | +0.00 | — | — | $10,958.18 | $10,958.18 | — |
| 2026-09-03 | -0.90 | $10,958.18 | — | $10,958.18 | -0.00 | +289.60 | AVGO, DELL, CXW, FRNM | — | $429.42 | $11,239.44 | AVGO×21, DELL×2, CXW×33, FRNM×69 |
| 2026-09-04 | +2.25 | $429.42 | AVGO×21, DELL×2, CXW×33, FRNM×69 | $11,246.46 | +7.02 | +35.11 | CRM, MRX, BE | AVGO, DELL, CXW | $321.21 | $11,269.20 | FRNM×69, CRM×26, MRX×20, BE×6 |
| 2026-09-08 | -11.47 | $321.21 | FRNM×69, CRM×26, MRX×20, BE×6 | $11,256.35 | -12.85 | -42.60 | — | FRNM, CRM, BE | $9,673.17 | $11,207.37 | MRX×20 |
| 2026-09-09 | -13.95 | $9,673.17 | MRX×20 | $11,205.17 | -2.20 | +0.00 | — | MRX | $11,203.09 | $11,203.09 | — |
| 2026-09-10 | -13.28 | $11,203.09 | — | $11,203.09 | +0.00 | +0.00 | — | — | $11,203.09 | $11,203.09 | — |
| 2026-09-11 | +0.50 | $11,203.09 | — | $11,203.09 | +0.00 | -662.40 | ORCL, ADBE, BAK, AMTX | — | $248.86 | $10,522.67 | ORCL×47, ADBE×4, BAK×528, AMTX×549 |
| 2026-09-14 | -11.00 | $248.86 | ORCL×47, ADBE×4, BAK×528, AMTX×549 | $10,127.53 | -395.14 | +0.00 | — | ORCL, ADBE, BAK, AMTX | $10,109.22 | $10,109.22 | — |
| 2026-09-15 | -3.84 | $10,109.22 | — | $10,109.22 | -0.00 | +0.00 | — | — | $10,109.22 | $10,109.22 | — |
| 2026-09-16 | +5.30 | $10,109.22 | — | $10,109.22 | -0.00 | +0.00 | — | — | $10,109.22 | $10,109.22 | — |
| 2026-09-17 | +7.38 | $10,109.22 | — | $10,109.22 | -0.00 | +0.00 | — | — | $10,109.22 | $10,109.22 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 531 | $13.18 | $6.85 | — | $2,994.57 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $7000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 232 | $4.31 | $2.99 | — | $1,991.66 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 73 | $13.55 | $2.21 | — | $1,000.30 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,000.30 | ▲ close $10,361.96 vs 09:30 $10,000.00 (session +374.01) | 16:00 close · cash $1,000.30 · equity $10,361.96 vs 09:30 $10,000.00 (+361.96; session marks +374.01) · 3 name(s) marked open→close (per-name table). HLIT×531 09:30 $13.18 → close $13.92 +392.94; ANGX×232 09:30 $4.31 → close $4.37 +13.92; MH×73 09:30 $13.55 → close $13.10 -32.85 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,000.30 | ▲ 09:30 equity $10,377.22 vs yday $10,361.96 (+15.26) | 09:30 open · cash $1,000.30 (unchanged overnight, no fees) · equity $10,377.22 vs prior close $10,361.96 (+15.26) · 3 name(s) re-marked at the open (per-name table). HLIT×531 yday $13.92 → 09:30 $13.84 -42.48; ANGX×232 yday $4.37 → 09:30 $4.60 +53.36; MH×73 yday $13.10 → 09:30 $13.16 +4.38 | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 531 | $13.84 | $7.00 | $+336.61 | $8,342.34 | ▲ +336.61 after sell → book $10,370.22; vs 09:30 mark -7.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 232 | $4.60 | $3.04 | $+61.25 | $9,406.50 | ▲ +61.25 after sell → book $10,367.18; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 73 | $13.16 | $2.23 | $-32.91 | $10,364.95 | ▼ -32.91 after sell → book $10,364.95; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 89 | $46.18 | $2.26 | — | $6,252.67 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $4145.98 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 21 | $142.77 | $2.05 | — | $3,252.45 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3109.48 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $1,223.43 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2072.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 21 | $49.00 | $2.05 | — | $192.38 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $1036.49 | join🟡 sector🟢 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $192.38 | ▲ close $10,568.89 vs 09:30 $10,377.22 (session +212.32) | 16:00 close · cash $192.38 · equity $10,568.89 vs 09:30 $10,377.22 (+191.67; session marks +212.32) · 4 name(s) marked open→close (per-name table). DVN×89 09:30 $46.18 → close $47.57 +123.71; EOG×21 09:30 $142.77 → close $146.15 +70.98; FANG×10 09:30 $202.70 → close $206.29 +35.90; OUST×21 09:30 $49.00 → close $48.13 -18.27 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $192.38 | ▲ 09:30 equity $10,609.41 vs yday $10,568.89 (+40.52) | 09:30 open · cash $192.38 (unchanged overnight, no fees) · equity $10,609.41 vs prior close $10,568.89 (+40.52) · 4 name(s) re-marked at the open (per-name table). DVN×89 yday $47.57 → 09:30 $48.00 +38.27; EOG×21 yday $146.15 → 09:30 $148.04 +39.69; FANG×10 yday $206.29 → 09:30 $208.93 +26.40; OUST×21 yday $48.13 → 09:30 $45.09 -63.84 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 89 | $48.00 | $2.31 | $+157.42 | $4,462.07 | ▲ +157.42 after sell → book $10,607.10; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 21 | $148.04 | $2.09 | $+106.53 | $7,568.82 | ▲ +106.53 after sell → book $10,605.01; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $9,656.08 | ▲ +58.23 after sell → book $10,602.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 21 | $45.09 | $2.07 | $-86.24 | $10,600.89 | ▼ -86.24 after sell → book $10,600.89; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,600.89 | ▲ close $10,600.89 vs 09:30 $10,609.41 (session +0.00) | 16:00 close · cash $10,600.89 · no lots left · equity $10,600.89. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,600.89 | ▲ 09:30 equity $10,600.89 vs yday $10,600.89 (+0.00) | 09:30 open · cash $10,600.89 · no holdings · equity $10,600.89 vs prior close $10,600.89 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,600.89 | ▲ close $10,600.89 vs 09:30 $10,600.89 (session +0.00) | 16:00 close · cash $10,600.89 · no lots left · equity $10,600.89. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,600.89 | ▲ 09:30 equity $10,600.89 vs yday $10,600.89 (+0.00) | 09:30 open · cash $10,600.89 · no holdings · equity $10,600.89 vs prior close $10,600.89 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 81 | $91.01 | $2.23 | — | $3,226.85 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $7420.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 23 | $44.76 | $2.06 | — | $2,195.31 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1060.09 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 429 | $2.47 | $5.53 | — | $1,130.15 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1060.09 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 18 | $58.73 | $2.04 | — | $70.96 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1060.09 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.96 | ▲ close $10,777.46 vs 09:30 $10,600.89 (session +188.44) | 16:00 close · cash $70.96 · equity $10,777.46 vs 09:30 $10,600.89 (+176.57; session marks +188.44) · 4 name(s) marked open→close (per-name table). BHP×81 09:30 $91.01 → close $93.63 +212.22; APA×23 09:30 $44.76 → close $44.39 -8.51; AUTL×429 09:30 $2.47 → close $2.46 -4.29; CRSP×18 09:30 $58.73 → close $58.12 -10.98 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.96 | ▲ 09:30 equity $10,982.83 vs yday $10,777.46 (+205.37) | 09:30 open · cash $70.96 (unchanged overnight, no fees) · equity $10,982.83 vs prior close $10,777.46 (+205.37) · 4 name(s) re-marked at the open (per-name table). BHP×81 yday $93.63 → 09:30 $95.72 +169.29; APA×23 yday $44.39 → 09:30 $44.52 +2.99; AUTL×429 yday $2.46 → 09:30 $2.47 +4.29; CRSP×18 yday $58.12 → 09:30 $59.72 +28.80 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 81 | $95.72 | $2.31 | $+376.97 | $7,821.97 | ▲ +376.97 after sell → book $10,980.52; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 23 | $44.52 | $2.08 | $-9.66 | $8,843.85 | ▼ -9.66 after sell → book $10,978.44; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 51 | $119.43 | $2.14 | — | $2,750.78 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $6190.70 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 23 | $115.18 | $2.06 | — | $99.58 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $2653.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.58 | ▲ close $11,230.41 vs 09:30 $10,982.83 (session +256.17) | 16:00 close · cash $99.58 · equity $11,230.41 vs 09:30 $10,982.83 (+247.58; session marks +256.17) · 4 name(s) marked open→close (per-name table). AUTL×429 09:30 $2.47 → close $2.41 -25.74; CRSP×18 09:30 $59.72 → close $59.50 -3.96; AU×51 09:30 $119.43 → close $121.22 +91.29; FUTU×23 09:30 $115.18 → close $123.64 +194.58 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.58 | ▼ 09:30 equity $11,115.69 vs yday $11,230.41 (-114.72) | 09:30 open · cash $99.58 (unchanged overnight, no fees) · equity $11,115.69 vs prior close $11,230.41 (-114.72) · 4 name(s) re-marked at the open (per-name table). AUTL×429 yday $2.41 → 09:30 $2.40 -4.29; CRSP×18 yday $59.50 → 09:30 $58.75 -13.50; AU×51 yday $121.22 → 09:30 $120.51 -36.21; FUTU×23 yday $123.64 → 09:30 $121.00 -60.72 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 429 | $2.40 | $5.62 | $-41.18 | $1,123.57 | ▼ -41.18 after sell → book $11,110.08; vs 09:30 mark -5.61 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 51 | $120.51 | $2.20 | $+50.73 | $7,267.38 | ▲ +50.73 after sell → book $11,107.88; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 23 | $121.00 | $2.09 | $+129.71 | $10,048.28 | ▲ +129.71 after sell → book $11,105.78; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,048.28 | ▼ close $11,075.63 vs 09:30 $11,115.69 (session -30.15) | 16:00 close · cash $10,048.28 · equity $11,075.63 vs 09:30 $11,115.69 (-40.06; session marks -30.15) · 1 name(s) marked open→close (per-name table). CRSP×18 09:30 $58.75 → close $57.08 -30.15 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,048.28 | ▲ 09:30 equity $11,091.02 vs yday $11,075.63 (+15.39) | 09:30 open · cash $10,048.28 (unchanged overnight, no fees) · equity $11,091.02 vs prior close $11,075.63 (+15.39) · 1 name(s) re-marked at the open (per-name table). CRSP×18 yday $57.08 → 09:30 $57.93 +15.39 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 18 | $57.93 | $2.06 | $-18.51 | $11,088.96 | ▼ -18.51 after sell → book $11,088.96; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 65 | $118.52 | $2.19 | — | $3,382.98 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $7762.27 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 14 | $77.13 | $2.03 | — | $2,301.12 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1108.90 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 31 | $35.05 | $2.08 | — | $1,212.49 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1108.90 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 117 | $9.42 | $2.34 | — | $108.01 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1108.90 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $108.01 | ▲ close $11,536.14 vs 09:30 $11,091.02 (session +455.82) | 16:00 close · cash $108.01 · equity $11,536.14 vs 09:30 $11,091.02 (+445.12; session marks +455.82) · 4 name(s) marked open→close (per-name table). AU×65 09:30 $118.52 → close $123.39 +316.55; FCX×14 09:30 $77.13 → close $79.91 +38.92; EZPW×31 09:30 $35.05 → close $35.23 +5.58; RUM×117 09:30 $9.42 → close $10.23 +94.77 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $108.01 | ▼ 09:30 equity $11,290.66 vs yday $11,536.14 (-245.48) | 09:30 open · cash $108.01 (unchanged overnight, no fees) · equity $11,290.66 vs prior close $11,536.14 (-245.48) · 4 name(s) re-marked at the open (per-name table). AU×65 yday $123.39 → 09:30 $119.80 -233.35; FCX×14 yday $79.91 → 09:30 $79.34 -7.98; EZPW×31 yday $35.23 → 09:30 $35.70 +14.57; RUM×117 yday $10.23 → 09:30 $10.07 -18.72 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 65 | $119.80 | $2.26 | $+78.76 | $7,892.75 | ▲ +78.76 after sell → book $11,288.40; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 14 | $79.34 | $2.05 | $+26.86 | $9,001.46 | ▲ +26.86 after sell → book $11,286.35; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 31 | $35.70 | $2.10 | $+15.96 | $10,106.06 | ▲ +15.96 after sell → book $11,284.25; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 117 | $10.07 | $2.37 | $+71.34 | $11,281.88 | ▲ +71.34 after sell → book $11,281.88; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 29 | $267.02 | $2.08 | — | $3,536.22 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $7897.31 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 100 | $11.22 | $2.29 | — | $2,411.93 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $1128.19 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 136 | $8.29 | $2.40 | — | $1,282.09 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $1128.19 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 64 | $17.41 | $2.18 | — | $165.67 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_mover; ret5=-9.2; leftover $1128.19 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $165.67 | ▲ close $11,463.68 vs 09:30 $11,290.66 (session +190.75) | 16:00 close · cash $165.67 · equity $11,463.68 vs 09:30 $11,290.66 (+173.02; session marks +190.75) · 4 name(s) marked open→close (per-name table). FNV×29 09:30 $267.02 → close $267.37 +10.15; TRLV×100 09:30 $11.22 → close $11.43 +21.00; CAPR×136 09:30 $8.29 → close $9.36 +145.52; FWRD×64 09:30 $17.41 → close $17.63 +14.08 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $165.67 | ▼ 09:30 equity $11,429.58 vs yday $11,463.68 (-34.10) | 09:30 open · cash $165.67 (unchanged overnight, no fees) · equity $11,429.58 vs prior close $11,463.68 (-34.10) · 4 name(s) re-marked at the open (per-name table). FNV×29 yday $267.37 → 09:30 $267.23 -4.06; TRLV×100 yday $11.43 → 09:30 $11.38 -5.00; CAPR×136 yday $9.36 → 09:30 $9.19 -23.12; FWRD×64 yday $17.63 → 09:30 $17.60 -1.92 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 29 | $267.23 | $2.15 | $+1.86 | $7,913.19 | ▲ +1.86 after sell → book $11,427.43; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 100 | $11.38 | $2.32 | $+11.39 | $9,048.87 | ▲ +11.39 after sell → book $11,425.11; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 136 | $9.19 | $2.43 | $+117.57 | $10,296.28 | ▲ +117.57 after sell → book $11,422.68; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWRD` | 64 | $17.60 | $2.20 | $+7.78 | $11,420.48 | ▲ +7.78 after sell → book $11,420.48; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 97 | $81.65 | $2.28 | — | $3,498.15 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $7994.34 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $2,529.15 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1142.05 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 3 | $318.88 | $2.00 | — | $1,570.51 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1142.05 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,570.51 | ▼ close $11,269.17 vs 09:30 $11,429.58 (session -145.04) | 16:00 close · cash $1,570.51 · equity $11,269.17 vs 09:30 $11,429.58 (-160.41; session marks -145.04) · 3 name(s) marked open→close (per-name table). ACMR×97 09:30 $81.65 → close $80.49 -112.52; MU×1 09:30 $967.01 → close $935.39 -31.62; LRCX×3 09:30 $318.88 → close $318.58 -0.90 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,570.51 | ▼ 09:30 equity $11,133.08 vs yday $11,269.17 (-136.09) | 09:30 open · cash $1,570.51 (unchanged overnight, no fees) · equity $11,133.08 vs prior close $11,269.17 (-136.09) · 3 name(s) re-marked at the open (per-name table). ACMR×97 yday $80.49 → 09:30 $79.27 -118.34; MU×1 yday $935.39 → 09:30 $919.29 -16.10; LRCX×3 yday $318.58 → 09:30 $318.03 -1.65 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 97 | $79.27 | $2.36 | $-235.50 | $9,257.34 | ▼ -235.50 after sell → book $11,130.72; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $10,174.62 | ▼ -51.73 after sell → book $11,128.71; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 3 | $318.03 | $2.02 | $-6.57 | $11,126.69 | ▼ -6.57 after sell → book $11,126.69; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 24 | $324.41 | $2.06 | — | $3,338.78 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $7788.68 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $2,344.45 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1112.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $1,541.62 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1112.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,541.62 | ▼ close $10,895.97 vs 09:30 $11,133.08 (session -224.65) | 16:00 close · cash $1,541.62 · equity $10,895.97 vs 09:30 $11,133.08 (-237.11; session marks -224.65) · 3 name(s) marked open→close (per-name table). KEYS×24 09:30 $324.41 → close $319.97 -106.56; SMTC×7 09:30 $141.76 → close $131.17 -74.13; CIEN×2 09:30 $400.42 → close $378.44 -43.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,541.62 | ▲ 09:30 equity $10,964.36 vs yday $10,895.97 (+68.39) | 09:30 open · cash $1,541.62 (unchanged overnight, no fees) · equity $10,964.36 vs prior close $10,895.97 (+68.39) · 3 name(s) re-marked at the open (per-name table). KEYS×24 yday $319.97 → 09:30 $322.49 +60.48; SMTC×7 yday $131.17 → 09:30 $132.30 +7.91; CIEN×2 yday $378.44 → 09:30 $378.44 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 24 | $322.49 | $2.13 | $-50.28 | $9,279.24 | ▼ -50.28 after sell → book $10,962.22; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 7 | $132.30 | $2.03 | $-70.26 | $10,203.31 | ▼ -70.26 after sell → book $10,960.19; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 2 | $378.44 | $2.02 | $-47.97 | $10,958.18 | ▼ -47.97 after sell → book $10,958.18; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,958.18 | ▲ close $10,958.18 vs 09:30 $10,964.36 (session +0.00) | 16:00 close · cash $10,958.18 · no lots left · equity $10,958.18. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,958.18 | ▲ 09:30 equity $10,958.18 vs yday $10,958.18 (-0.00) | 09:30 open · cash $10,958.18 · no holdings · equity $10,958.18 vs prior close $10,958.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,958.18 | ▲ close $10,958.18 vs 09:30 $10,958.18 (session +0.00) | 16:00 close · cash $10,958.18 · no lots left · equity $10,958.18. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,958.18 | ▲ 09:30 equity $10,958.18 vs yday $10,958.18 (-0.00) | 09:30 open · cash $10,958.18 · no holdings · equity $10,958.18 vs prior close $10,958.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,958.18 | ▲ close $10,958.18 vs 09:30 $10,958.18 (session +0.00) | 16:00 close · cash $10,958.18 · no lots left · equity $10,958.18. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,958.18 | ▲ 09:30 equity $10,958.18 vs yday $10,958.18 (-0.00) | 09:30 open · cash $10,958.18 · no holdings · equity $10,958.18 vs prior close $10,958.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 21 | $351.74 | $2.05 | — | $3,569.58 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $7670.72 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $2,594.97 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1095.82 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 33 | $32.31 | $2.09 | — | $1,526.65 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1095.82 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 69 | $15.87 | $2.20 | — | $429.42 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1095.82 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $429.42 | ▲ close $11,239.44 vs 09:30 $10,958.18 (session +289.60) | 16:00 close · cash $429.42 · equity $11,239.44 vs 09:30 $10,958.18 (+281.26; session marks +289.60) · 4 name(s) marked open→close (per-name table). AVGO×21 09:30 $351.74 → close $357.16 +113.82; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×33 09:30 $32.31 → close $33.66 +44.55; FRNM×69 09:30 $15.87 → close $16.90 +71.07 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $429.42 | ▲ 09:30 equity $11,246.46 vs yday $11,239.44 (+7.02) | 09:30 open · cash $429.42 (unchanged overnight, no fees) · equity $11,246.46 vs prior close $11,239.44 (+7.02) · 4 name(s) re-marked at the open (per-name table). AVGO×21 yday $357.16 → 09:30 $359.70 +53.34; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×33 yday $33.66 → 09:30 $33.46 -6.60; FRNM×69 yday $16.90 → 09:30 $16.40 -34.50 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 21 | $359.70 | $2.12 | $+162.98 | $7,981.00 | ▲ +162.98 after sell → book $11,244.34; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $9,006.54 | ▲ +50.93 after sell → book $11,242.32; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 33 | $33.46 | $2.11 | $+33.75 | $10,108.61 | ▲ +33.75 after sell → book $11,240.21; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 26 | $263.36 | $2.07 | — | $3,259.18 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $7076.03 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 20 | $75.65 | $2.05 | — | $1,744.13 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1516.29 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 6 | $236.82 | $2.01 | — | $321.21 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list ohlc_hot; ret5=+8.1; leftover $1516.29 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.21 | ▲ close $11,269.20 vs 09:30 $11,246.46 (session +35.11) | 16:00 close · cash $321.21 · equity $11,269.20 vs 09:30 $11,246.46 (+22.74; session marks +35.11) · 4 name(s) marked open→close (per-name table). FRNM×69 09:30 $16.40 → close $16.31 -6.21; CRM×26 09:30 $263.36 → close $259.23 -107.38; MRX×20 09:30 $75.65 → close $78.27 +52.40; BE×6 09:30 $236.82 → close $252.87 +96.30 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $321.21 | ▼ 09:30 equity $11,256.35 vs yday $11,269.20 (-12.85) | 09:30 open · cash $321.21 (unchanged overnight, no fees) · equity $11,256.35 vs prior close $11,269.20 (-12.85) · 4 name(s) re-marked at the open (per-name table). FRNM×69 yday $16.31 → 09:30 $16.74 +29.67; CRM×26 yday $259.23 → 09:30 $253.72 -143.26; MRX×20 yday $78.27 → 09:30 $78.84 +11.40; BE×6 yday $252.87 → 09:30 $267.76 +89.34 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 69 | $16.74 | $2.22 | $+55.61 | $1,474.05 | ▲ +55.61 after sell → book $11,254.13; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 26 | $253.72 | $2.13 | $-254.84 | $8,068.64 | ▼ -254.84 after sell → book $11,252.00; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 6 | $267.76 | $2.03 | $+181.60 | $9,673.17 | ▲ +181.60 after sell → book $11,249.97; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,673.17 | ▼ close $11,207.37 vs 09:30 $11,256.35 (session -42.60) | 16:00 close · cash $9,673.17 · equity $11,207.37 vs 09:30 $11,256.35 (-48.98; session marks -42.60) · 1 name(s) marked open→close (per-name table). MRX×20 09:30 $78.84 → close $76.71 -42.60 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,673.17 | ▼ 09:30 equity $11,205.17 vs yday $11,207.37 (-2.20) | 09:30 open · cash $9,673.17 (unchanged overnight, no fees) · equity $11,205.17 vs prior close $11,207.37 (-2.20) · 1 name(s) re-marked at the open (per-name table). MRX×20 yday $76.71 → 09:30 $76.60 -2.20 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 20 | $76.60 | $2.07 | $+14.88 | $11,203.09 | ▲ +14.88 after sell → book $11,203.09; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,203.09 | ▲ close $11,203.09 vs 09:30 $11,205.17 (session +0.00) | 16:00 close · cash $11,203.09 · no lots left · equity $11,203.09. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,203.09 | ▲ 09:30 equity $11,203.09 vs yday $11,203.09 (+0.00) | 09:30 open · cash $11,203.09 · no holdings · equity $11,203.09 vs prior close $11,203.09 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,203.09 | ▲ close $11,203.09 vs 09:30 $11,203.09 (session +0.00) | 16:00 close · cash $11,203.09 · no lots left · equity $11,203.09. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,203.09 | ▲ 09:30 equity $11,203.09 vs yday $11,203.09 (+0.00) | 09:30 open · cash $11,203.09 · no holdings · equity $11,203.09 vs prior close $11,203.09 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 47 | $164.43 | $2.13 | — | $3,472.75 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $7842.17 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 4 | $242.17 | $2.00 | — | $2,502.07 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list earn_react; ret5=-11.1; leftover $1120.31 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 528 | $2.12 | $6.81 | — | $1,375.90 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1120.31 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 549 | $2.04 | $7.08 | — | $248.86 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1120.31 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $248.86 | ▼ close $10,522.67 vs 09:30 $11,203.09 (session -662.40) | 16:00 close · cash $248.86 · equity $10,522.67 vs 09:30 $11,203.09 (-680.42; session marks -662.40) · 4 name(s) marked open→close (per-name table). ORCL×47 09:30 $164.43 → close $150.28 -665.05; ADBE×4 09:30 $242.17 → close $252.23 +40.24; BAK×528 09:30 $2.12 → close $2.08 -21.12; AMTX×549 09:30 $2.04 → close $2.01 -16.47 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $248.86 | ▼ 09:30 equity $10,127.53 vs yday $10,522.67 (-395.14) | 09:30 open · cash $248.86 (unchanged overnight, no fees) · equity $10,127.53 vs prior close $10,522.67 (-395.14) · 4 name(s) re-marked at the open (per-name table). ORCL×47 yday $150.28 → 09:30 $141.42 -416.42; ADBE×4 yday $252.23 → 09:30 $261.51 +37.12; BAK×528 yday $2.08 → 09:30 $2.05 -15.84; AMTX×549 yday $2.01 → 09:30 $2.01 +0.00 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 47 | $141.42 | $2.19 | $-1085.80 | $6,893.40 | ▼ -1,085.80 after sell → book $10,125.33; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 4 | $261.51 | $2.02 | $+73.34 | $7,937.42 | ▲ +73.34 after sell → book $10,123.31; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 528 | $2.05 | $6.91 | $-50.68 | $9,012.91 | ▼ -50.68 after sell → book $10,116.40; vs 09:30 mark -6.91 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 549 | $2.01 | $7.18 | $-30.74 | $10,109.22 | ▼ -30.74 after sell → book $10,109.22; vs 09:30 mark -7.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,109.22 | ▲ close $10,109.22 vs 09:30 $10,127.53 (session +0.00) | 16:00 close · cash $10,109.22 · no lots left · equity $10,109.22. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,109.22 | ▲ 09:30 equity $10,109.22 vs yday $10,109.22 (-0.00) | 09:30 open · cash $10,109.22 · no holdings · equity $10,109.22 vs prior close $10,109.22 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,109.22 | ▲ close $10,109.22 vs 09:30 $10,109.22 (session +0.00) | 16:00 close · cash $10,109.22 · no lots left · equity $10,109.22. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,109.22 | ▲ 09:30 equity $10,109.22 vs yday $10,109.22 (-0.00) | 09:30 open · cash $10,109.22 · no holdings · equity $10,109.22 vs prior close $10,109.22 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,109.22 | ▲ close $10,109.22 vs 09:30 $10,109.22 (session +0.00) | 16:00 close · cash $10,109.22 · no lots left · equity $10,109.22. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,109.22 | ▲ 09:30 equity $10,109.22 vs yday $10,109.22 (-0.00) | 09:30 open · cash $10,109.22 · no holdings · equity $10,109.22 vs prior close $10,109.22 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,109.22 | ▲ close $10,109.22 vs 09:30 $10,109.22 (session +0.00) | 16:00 close · cash $10,109.22 · no lots left · equity $10,109.22. | — |

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
| 2026-08-27 | `ASML` | cash | leftover split 1142.05 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1112.67 < 1 share @ 1306.03 |
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
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
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
| 2026-09-14 | `AVTR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
