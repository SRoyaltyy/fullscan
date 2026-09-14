# Factor mine action — `union_news_g_conv_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `conviction` · sell `list` · S-boost `none` · news🟢 rank cameras; 70% of leftover if +9 −≤1

Cash book **+0.47%** ($10,047) · signal-only (no cash/fees) was +9.12%. Starts YES **2/22**. Fills 78 · skips 38 · realized $+46.74.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,046.76.

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
| 2026-08-20 | `AUTL` | 426 | — | $2.47 | +0.00 | $2.46 | -4.26 | -4.26 | +0.00 | -4.26 |
| 2026-08-20 | `CRSP` | 17 | — | $58.73 | +0.00 | $58.12 | -10.37 | -10.37 | +0.00 | -10.37 |
| 2026-08-21 | `BHP` | 81 | $93.63 | $95.72 | +169.29 | — | +0.00 | +169.29 | +381.51 | — |
| 2026-08-21 | `APA` | 23 | $44.39 | $44.52 | +2.99 | — | +0.00 | +2.99 | -5.52 | — |
| 2026-08-21 | `AUTL` | 426 | $2.46 | $2.47 | +4.26 | $2.41 | -25.56 | -21.30 | +0.00 | -25.56 |
| 2026-08-21 | `CRSP` | 17 | $58.12 | $59.72 | +27.20 | $59.50 | -3.74 | +23.46 | +16.83 | +13.09 |
| 2026-08-21 | `AU` | 51 | — | $119.43 | +0.00 | $121.22 | +91.29 | +91.29 | +0.00 | +91.29 |
| 2026-08-21 | `FUTU` | 23 | — | $115.18 | +0.00 | $123.64 | +194.58 | +194.58 | +0.00 | +194.58 |
| 2026-08-24 | `AUTL` | 426 | $2.41 | $2.40 | -4.26 | — | +0.00 | -4.26 | -29.82 | — |
| 2026-08-24 | `CRSP` | 17 | $59.50 | $58.75 | -12.75 | $57.08 | -28.47 | -41.22 | +0.34 | -28.13 |
| 2026-08-24 | `AU` | 51 | $121.22 | $120.51 | -36.21 | — | +0.00 | -36.21 | +55.08 | — |
| 2026-08-24 | `FUTU` | 23 | $123.64 | $121.00 | -60.72 | — | +0.00 | -60.72 | +133.86 | — |
| 2026-08-25 | `CRSP` | 17 | $57.08 | $57.93 | +14.53 | — | +0.00 | +14.53 | -13.60 | — |
| 2026-08-25 | `AU` | 65 | — | $118.52 | +0.00 | $123.39 | +316.55 | +316.55 | +0.00 | +316.55 |
| 2026-08-25 | `FCX` | 14 | — | $77.13 | +0.00 | $79.91 | +38.92 | +38.92 | +0.00 | +38.92 |
| 2026-08-25 | `EZPW` | 31 | — | $35.05 | +0.00 | $35.23 | +5.58 | +5.58 | +0.00 | +5.58 |
| 2026-08-25 | `RUM` | 117 | — | $9.42 | +0.00 | $10.23 | +94.77 | +94.77 | +0.00 | +94.77 |
| 2026-08-26 | `AU` | 65 | $123.39 | $119.80 | -233.35 | — | +0.00 | -233.35 | +83.20 | — |
| 2026-08-26 | `FCX` | 14 | $79.91 | $79.34 | -7.98 | — | +0.00 | -7.98 | +30.94 | — |
| 2026-08-26 | `EZPW` | 31 | $35.23 | $35.70 | +14.57 | — | +0.00 | +14.57 | +20.15 | — |
| 2026-08-26 | `RUM` | 117 | $10.23 | $10.07 | -18.72 | — | +0.00 | -18.72 | +76.05 | — |
| 2026-08-26 | `FNV` | 29 | — | $267.02 | +0.00 | $267.37 | +10.15 | +10.15 | +0.00 | +10.15 |
| 2026-08-26 | `TRLV` | 99 | — | $11.22 | +0.00 | $11.43 | +20.79 | +20.79 | +0.00 | +20.79 |
| 2026-08-26 | `CAPR` | 135 | — | $8.29 | +0.00 | $9.36 | +144.45 | +144.45 | +0.00 | +144.45 |
| 2026-08-26 | `FWRD` | 64 | — | $17.41 | +0.00 | $17.63 | +14.08 | +14.08 | +0.00 | +14.08 |
| 2026-08-27 | `FNV` | 29 | $267.37 | $267.23 | -4.06 | — | +0.00 | -4.06 | +6.09 | — |
| 2026-08-27 | `TRLV` | 99 | $11.43 | $11.38 | -4.95 | — | +0.00 | -4.95 | +15.84 | — |
| 2026-08-27 | `CAPR` | 135 | $9.36 | $9.19 | -22.95 | — | +0.00 | -22.95 | +121.50 | — |
| 2026-08-27 | `FWRD` | 64 | $17.63 | $17.60 | -1.92 | — | +0.00 | -1.92 | +12.16 | — |
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
| 2026-09-11 | `ORCL` | 47 | — | $164.43 | +0.00 | $150.28 | -665.05 | -665.05 | +0.00 | -665.05 |
| 2026-09-11 | `ADBE` | 4 | — | $242.17 | +0.00 | $252.23 | +40.24 | +40.24 | +0.00 | +40.24 |
| 2026-09-11 | `BAK` | 525 | — | $2.12 | +0.00 | $2.08 | -21.00 | -21.00 | +0.00 | -21.00 |
| 2026-09-11 | `AMTX` | 546 | — | $2.04 | +0.00 | $2.01 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-14 | `ORCL` | 47 | $150.28 | $141.42 | -416.42 | — | +0.00 | -416.42 | -1081.47 | — |
| 2026-09-14 | `ADBE` | 4 | $252.23 | $261.51 | +37.12 | — | +0.00 | +37.12 | +77.36 | — |
| 2026-09-14 | `BAK` | 525 | $2.08 | $2.05 | -15.75 | — | +0.00 | -15.75 | -36.75 | — |
| 2026-09-14 | `AMTX` | 546 | $2.01 | $2.01 | +0.00 | — | +0.00 | +0.00 | -16.38 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +136.10 | ANGX, ARX, HLIT, MH | — | $7.71 | $10,117.03 | ANGX×928, ARX×153, HLIT×151, MH×73 |
| 2026-08-17 | +2.25 | $7.71 | ANGX×928, ARX×153, HLIT×151, MH×73 | $10,321.24 | +204.21 | +212.32 | DVN, EOG, FANG, OUST | ANGX, ARX, HLIT, MH | $129.29 | $10,505.80 | DVN×89, EOG×21, FANG×10, OUST×21 |
| 2026-08-18 | -6.20 | $129.29 | DVN×89, EOG×21, FANG×10, OUST×21 | $10,546.32 | +40.52 | +0.00 | — | DVN, EOG, FANG, OUST | $10,537.81 | $10,537.81 | — |
| 2026-08-19 | -7.20 | $10,537.81 | — | $10,537.81 | -0.00 | +0.00 | — | — | $10,537.81 | $10,537.81 | — |
| 2026-08-20 | +1.12 | $10,537.81 | — | $10,537.81 | -0.00 | +189.08 | BHP, APA, AUTL, CRSP | — | $74.06 | $10,715.06 | BHP×81, APA×23, AUTL×426, CRSP×17 |
| 2026-08-21 | +3.25 | $74.06 | BHP×81, APA×23, AUTL×426, CRSP×17 | $10,918.80 | +203.74 | +256.57 | AU, FUTU | BHP, APA | $102.68 | $11,166.78 | AUTL×426, CRSP×17, AU×51, FUTU×23 |
| 2026-08-24 | -5.17 | $102.68 | AUTL×426, CRSP×17, AU×51, FUTU×23 | $11,052.84 | -113.94 | -28.47 | — | AUTL, AU, FUTU | $10,044.22 | $11,014.49 | CRSP×17 |
| 2026-08-25 | +1.80 | $10,044.22 | CRSP×17 | $11,029.03 | +14.54 | +455.82 | AU, FCX, EZPW, RUM | CRSP | $46.02 | $11,474.15 | AU×65, FCX×14, EZPW×31, RUM×117 |
| 2026-08-26 | +2.02 | $46.02 | AU×65, FCX×14, EZPW×31, RUM×117 | $11,228.67 | -245.48 | +189.47 | FNV, TRLV, CAPR, FWRD | AU, FCX, EZPW, RUM | $123.19 | $11,400.41 | FNV×29, TRLV×99, CAPR×135, FWRD×64 |
| 2026-08-27 | — | $123.19 | FNV×29, TRLV×99, CAPR×135, FWRD×64 | $11,366.53 | -33.88 | -145.04 | ACMR, MU, LRCX | FNV, TRLV, CAPR, FWRD | $1,507.47 | $11,206.13 | ACMR×97, MU×1, LRCX×3 |
| 2026-08-28 | +0.75 | $1,507.47 | ACMR×97, MU×1, LRCX×3 | $11,070.04 | -136.09 | -220.21 | KEYS, SMTC, CIEN | ACMR, MU, LRCX | $1,802.99 | $10,837.37 | KEYS×23, SMTC×7, CIEN×2 |
| 2026-08-31 | -5.85 | $1,802.99 | KEYS×23, SMTC×7, CIEN×2 | $10,903.24 | +65.87 | +0.00 | — | KEYS, SMTC, CIEN | $10,897.07 | $10,897.07 | — |
| 2026-09-01 | -6.30 | $10,897.07 | — | $10,897.07 | -0.00 | +0.00 | — | — | $10,897.07 | $10,897.07 | — |
| 2026-09-02 | -3.83 | $10,897.07 | — | $10,897.07 | -0.00 | +0.00 | — | — | $10,897.07 | $10,897.07 | — |
| 2026-09-03 | -0.90 | $10,897.07 | — | $10,897.07 | -0.00 | +288.57 | AVGO, DELL, CXW, FRNM | — | $384.18 | $11,177.30 | AVGO×21, DELL×2, CXW×33, FRNM×68 |
| 2026-09-04 | +2.25 | $384.18 | AVGO×21, DELL×2, CXW×33, FRNM×68 | $11,184.82 | +7.52 | +32.58 | CRM, MRX, BE | AVGO, DELL, CXW | $351.62 | $11,205.03 | FRNM×68, CRM×26, MRX×19, BE×6 |
| 2026-09-08 | -11.47 | $351.62 | FRNM×68, CRM×26, MRX×19, BE×6 | $11,191.18 | -13.85 | -40.47 | — | FRNM, CRM, BE | $9,686.85 | $11,144.34 | MRX×19 |
| 2026-09-09 | -13.95 | $9,686.85 | MRX×19 | $11,142.25 | -2.09 | +0.00 | — | MRX | $11,140.18 | $11,140.18 | — |
| 2026-09-10 | -13.28 | $11,140.18 | — | $11,140.18 | -0.00 | +0.00 | — | — | $11,140.18 | $11,140.18 | — |
| 2026-09-11 | +0.50 | $11,140.18 | — | $11,140.18 | -0.00 | -662.19 | ORCL, ADBE, BAK, AMTX | — | $198.50 | $10,460.04 | ORCL×47, ADBE×4, BAK×525, AMTX×546 |
| 2026-09-14 | -11.00 | $198.50 | ORCL×47, ADBE×4, BAK×525, AMTX×546 | $10,064.99 | -395.05 | +0.00 | — | ORCL, ADBE, BAK, AMTX | $10,046.76 | $10,046.76 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 928 | $4.31 | $11.97 | — | $5,988.35 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $4000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 153 | $19.57 | $2.45 | — | $2,991.69 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $3000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 151 | $13.18 | $2.44 | — | $999.07 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 73 | $13.55 | $2.21 | — | $7.71 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.71 | ▲ close $10,117.03 vs 09:30 $10,000.00 (session +136.10) | 16:00 close · cash $7.71 · equity $10,117.03 vs 09:30 $10,000.00 (+117.03; session marks +136.10) · 4 name(s) marked open→close (per-name table). ANGX×928 09:30 $4.31 → close $4.37 +55.68; ARX×153 09:30 $19.57 → close $19.58 +1.53; HLIT×151 09:30 $13.18 → close $13.92 +111.74; MH×73 09:30 $13.55 → close $13.10 -32.85 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.71 | ▲ 09:30 equity $10,321.24 vs yday $10,117.03 (+204.21) | 09:30 open · cash $7.71 (unchanged overnight, no fees) · equity $10,321.24 vs prior close $10,117.03 (+204.21) · 4 name(s) re-marked at the open (per-name table). ANGX×928 yday $4.37 → 09:30 $4.60 +213.44; ARX×153 yday $19.58 → 09:30 $19.57 -1.53; HLIT×151 yday $13.92 → 09:30 $13.84 -12.08; MH×73 yday $13.10 → 09:30 $13.16 +4.38 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 928 | $4.60 | $12.16 | $+244.99 | $4,264.35 | ▲ +244.99 after sell → book $10,309.08; vs 09:30 mark -12.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 153 | $19.57 | $2.50 | $-4.95 | $7,256.06 | ▼ -4.95 after sell → book $10,306.58; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 151 | $13.84 | $2.48 | $+94.73 | $9,343.42 | ▲ +94.73 after sell → book $10,304.10; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 73 | $13.16 | $2.23 | $-32.91 | $10,301.86 | ▼ -32.91 after sell → book $10,301.86; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 89 | $46.18 | $2.26 | — | $6,189.59 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $4120.75 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 21 | $142.77 | $2.05 | — | $3,189.36 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3090.56 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $1,160.34 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2060.37 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 21 | $49.00 | $2.05 | — | $129.29 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $1030.19 | join🟡 sector🟢 gen🟢 news🟢 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.29 | ▲ close $10,505.80 vs 09:30 $10,321.24 (session +212.32) | 16:00 close · cash $129.29 · equity $10,505.80 vs 09:30 $10,321.24 (+184.56; session marks +212.32) · 4 name(s) marked open→close (per-name table). DVN×89 09:30 $46.18 → close $47.57 +123.71; EOG×21 09:30 $142.77 → close $146.15 +70.98; FANG×10 09:30 $202.70 → close $206.29 +35.90; OUST×21 09:30 $49.00 → close $48.13 -18.27 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.29 | ▲ 09:30 equity $10,546.32 vs yday $10,505.80 (+40.52) | 09:30 open · cash $129.29 (unchanged overnight, no fees) · equity $10,546.32 vs prior close $10,505.80 (+40.52) · 4 name(s) re-marked at the open (per-name table). DVN×89 yday $47.57 → 09:30 $48.00 +38.27; EOG×21 yday $146.15 → 09:30 $148.04 +39.69; FANG×10 yday $206.29 → 09:30 $208.93 +26.40; OUST×21 yday $48.13 → 09:30 $45.09 -63.84 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 89 | $48.00 | $2.31 | $+157.42 | $4,398.99 | ▲ +157.42 after sell → book $10,544.02; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 21 | $148.04 | $2.09 | $+106.53 | $7,505.74 | ▲ +106.53 after sell → book $10,541.93; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $9,592.99 | ▲ +58.23 after sell → book $10,539.88; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 21 | $45.09 | $2.07 | $-86.24 | $10,537.81 | ▼ -86.24 after sell → book $10,537.81; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,537.81 | ▲ close $10,537.81 vs 09:30 $10,546.32 (session +0.00) | 16:00 close · cash $10,537.81 · no lots left · equity $10,537.81. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,537.81 | ▲ 09:30 equity $10,537.81 vs yday $10,537.81 (-0.00) | 09:30 open · cash $10,537.81 · no holdings · equity $10,537.81 vs prior close $10,537.81 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,537.81 | ▲ close $10,537.81 vs 09:30 $10,537.81 (session +0.00) | 16:00 close · cash $10,537.81 · no lots left · equity $10,537.81. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,537.81 | ▲ 09:30 equity $10,537.81 vs yday $10,537.81 (-0.00) | 09:30 open · cash $10,537.81 · no holdings · equity $10,537.81 vs prior close $10,537.81 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 81 | $91.01 | $2.23 | — | $3,163.76 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $7376.47 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 23 | $44.76 | $2.06 | — | $2,132.23 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1053.78 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 426 | $2.47 | $5.50 | — | $1,074.51 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1053.78 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 17 | $58.73 | $2.04 | — | $74.06 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1053.78 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.06 | ▲ close $10,715.06 vs 09:30 $10,537.81 (session +189.08) | 16:00 close · cash $74.06 · equity $10,715.06 vs 09:30 $10,537.81 (+177.25; session marks +189.08) · 4 name(s) marked open→close (per-name table). BHP×81 09:30 $91.01 → close $93.63 +212.22; APA×23 09:30 $44.76 → close $44.39 -8.51; AUTL×426 09:30 $2.47 → close $2.46 -4.26; CRSP×17 09:30 $58.73 → close $58.12 -10.37 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.06 | ▲ 09:30 equity $10,918.80 vs yday $10,715.06 (+203.74) | 09:30 open · cash $74.06 (unchanged overnight, no fees) · equity $10,918.80 vs prior close $10,715.06 (+203.74) · 4 name(s) re-marked at the open (per-name table). BHP×81 yday $93.63 → 09:30 $95.72 +169.29; APA×23 yday $44.39 → 09:30 $44.52 +2.99; AUTL×426 yday $2.46 → 09:30 $2.47 +4.26; CRSP×17 yday $58.12 → 09:30 $59.72 +27.20 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 81 | $95.72 | $2.31 | $+376.97 | $7,825.07 | ▲ +376.97 after sell → book $10,916.49; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 23 | $44.52 | $2.08 | $-9.66 | $8,846.95 | ▼ -9.66 after sell → book $10,914.41; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 51 | $119.43 | $2.14 | — | $2,753.88 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $6192.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 23 | $115.18 | $2.06 | — | $102.68 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $2654.09 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.68 | ▲ close $11,166.78 vs 09:30 $10,918.80 (session +256.57) | 16:00 close · cash $102.68 · equity $11,166.78 vs 09:30 $10,918.80 (+247.98; session marks +256.57) · 4 name(s) marked open→close (per-name table). AUTL×426 09:30 $2.47 → close $2.41 -25.56; CRSP×17 09:30 $59.72 → close $59.50 -3.74; AU×51 09:30 $119.43 → close $121.22 +91.29; FUTU×23 09:30 $115.18 → close $123.64 +194.58 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.68 | ▼ 09:30 equity $11,052.84 vs yday $11,166.78 (-113.94) | 09:30 open · cash $102.68 (unchanged overnight, no fees) · equity $11,052.84 vs prior close $11,166.78 (-113.94) · 4 name(s) re-marked at the open (per-name table). AUTL×426 yday $2.41 → 09:30 $2.40 -4.26; CRSP×17 yday $59.50 → 09:30 $58.75 -12.75; AU×51 yday $121.22 → 09:30 $120.51 -36.21; FUTU×23 yday $123.64 → 09:30 $121.00 -60.72 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 426 | $2.40 | $5.58 | $-40.89 | $1,119.50 | ▼ -40.89 after sell → book $11,047.26; vs 09:30 mark -5.58 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 51 | $120.51 | $2.20 | $+50.73 | $7,263.31 | ▲ +50.73 after sell → book $11,045.06; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 23 | $121.00 | $2.09 | $+129.71 | $10,044.22 | ▲ +129.71 after sell → book $11,042.97; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,044.22 | ▼ close $11,014.49 vs 09:30 $11,052.84 (session -28.47) | 16:00 close · cash $10,044.22 · equity $11,014.49 vs 09:30 $11,052.84 (-38.35; session marks -28.47) · 1 name(s) marked open→close (per-name table). CRSP×17 09:30 $58.75 → close $57.08 -28.47 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,044.22 | ▲ 09:30 equity $11,029.03 vs yday $11,014.49 (+14.54) | 09:30 open · cash $10,044.22 (unchanged overnight, no fees) · equity $11,029.03 vs prior close $11,014.49 (+14.54) · 1 name(s) re-marked at the open (per-name table). CRSP×17 yday $57.08 → 09:30 $57.93 +14.53 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 17 | $57.93 | $2.06 | $-17.70 | $11,026.97 | ▼ -17.70 after sell → book $11,026.97; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 65 | $118.52 | $2.19 | — | $3,320.98 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $7718.88 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 14 | $77.13 | $2.03 | — | $2,239.13 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1102.70 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 31 | $35.05 | $2.08 | — | $1,150.50 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1102.70 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 117 | $9.42 | $2.34 | — | $46.02 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1102.70 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.02 | ▲ close $11,474.15 vs 09:30 $11,029.03 (session +455.82) | 16:00 close · cash $46.02 · equity $11,474.15 vs 09:30 $11,029.03 (+445.12; session marks +455.82) · 4 name(s) marked open→close (per-name table). AU×65 09:30 $118.52 → close $123.39 +316.55; FCX×14 09:30 $77.13 → close $79.91 +38.92; EZPW×31 09:30 $35.05 → close $35.23 +5.58; RUM×117 09:30 $9.42 → close $10.23 +94.77 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.02 | ▼ 09:30 equity $11,228.67 vs yday $11,474.15 (-245.48) | 09:30 open · cash $46.02 (unchanged overnight, no fees) · equity $11,228.67 vs prior close $11,474.15 (-245.48) · 4 name(s) re-marked at the open (per-name table). AU×65 yday $123.39 → 09:30 $119.80 -233.35; FCX×14 yday $79.91 → 09:30 $79.34 -7.98; EZPW×31 yday $35.23 → 09:30 $35.70 +14.57; RUM×117 yday $10.23 → 09:30 $10.07 -18.72 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 65 | $119.80 | $2.26 | $+78.76 | $7,830.76 | ▲ +78.76 after sell → book $11,226.41; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 14 | $79.34 | $2.05 | $+26.86 | $8,939.47 | ▲ +26.86 after sell → book $11,224.36; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 31 | $35.70 | $2.10 | $+15.96 | $10,044.06 | ▲ +15.96 after sell → book $11,222.25; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 117 | $10.07 | $2.37 | $+71.34 | $11,219.88 | ▲ +71.34 after sell → book $11,219.88; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 29 | $267.02 | $2.08 | — | $3,474.23 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $7853.92 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 99 | $11.22 | $2.29 | — | $2,361.16 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $1121.99 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 135 | $8.29 | $2.40 | — | $1,239.62 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $1121.99 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 64 | $17.41 | $2.18 | — | $123.19 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_mover; ret5=-9.2; leftover $1121.99 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.19 | ▲ close $11,400.41 vs 09:30 $11,228.67 (session +189.47) | 16:00 close · cash $123.19 · equity $11,400.41 vs 09:30 $11,228.67 (+171.74; session marks +189.47) · 4 name(s) marked open→close (per-name table). FNV×29 09:30 $267.02 → close $267.37 +10.15; TRLV×99 09:30 $11.22 → close $11.43 +20.79; CAPR×135 09:30 $8.29 → close $9.36 +144.45; FWRD×64 09:30 $17.41 → close $17.63 +14.08 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.19 | ▼ 09:30 equity $11,366.53 vs yday $11,400.41 (-33.88) | 09:30 open · cash $123.19 (unchanged overnight, no fees) · equity $11,366.53 vs prior close $11,400.41 (-33.88) · 4 name(s) re-marked at the open (per-name table). FNV×29 yday $267.37 → 09:30 $267.23 -4.06; TRLV×99 yday $11.43 → 09:30 $11.38 -4.95; CAPR×135 yday $9.36 → 09:30 $9.19 -22.95; FWRD×64 yday $17.63 → 09:30 $17.60 -1.92 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 29 | $267.23 | $2.15 | $+1.86 | $7,870.71 | ▲ +1.86 after sell → book $11,364.38; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 99 | $11.38 | $2.31 | $+11.24 | $8,995.02 | ▲ +11.24 after sell → book $11,362.07; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 135 | $9.19 | $2.43 | $+116.68 | $10,233.24 | ▲ +116.68 after sell → book $11,359.64; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWRD` | 64 | $17.60 | $2.20 | $+7.78 | $11,357.44 | ▲ +7.78 after sell → book $11,357.44; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 97 | $81.65 | $2.28 | — | $3,435.11 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $7950.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $2,466.11 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1135.74 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 3 | $318.88 | $2.00 | — | $1,507.47 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1135.74 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,507.47 | ▼ close $11,206.13 vs 09:30 $11,366.53 (session -145.04) | 16:00 close · cash $1,507.47 · equity $11,206.13 vs 09:30 $11,366.53 (-160.40; session marks -145.04) · 3 name(s) marked open→close (per-name table). ACMR×97 09:30 $81.65 → close $80.49 -112.52; MU×1 09:30 $967.01 → close $935.39 -31.62; LRCX×3 09:30 $318.88 → close $318.58 -0.90 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,507.47 | ▼ 09:30 equity $11,070.04 vs yday $11,206.13 (-136.09) | 09:30 open · cash $1,507.47 (unchanged overnight, no fees) · equity $11,070.04 vs prior close $11,206.13 (-136.09) · 3 name(s) re-marked at the open (per-name table). ACMR×97 yday $80.49 → 09:30 $79.27 -118.34; MU×1 yday $935.39 → 09:30 $919.29 -16.10; LRCX×3 yday $318.58 → 09:30 $318.03 -1.65 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 97 | $79.27 | $2.36 | $-235.50 | $9,194.30 | ▼ -235.50 after sell → book $11,067.68; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $10,111.58 | ▼ -51.73 after sell → book $11,065.67; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 3 | $318.03 | $2.02 | $-6.57 | $11,063.65 | ▼ -6.57 after sell → book $11,063.65; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 23 | $324.41 | $2.06 | — | $3,600.16 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $7744.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $2,605.83 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1106.36 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $1,802.99 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1106.36 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,802.99 | ▼ close $10,837.37 vs 09:30 $11,070.04 (session -220.21) | 16:00 close · cash $1,802.99 · equity $10,837.37 vs 09:30 $11,070.04 (-232.67; session marks -220.21) · 3 name(s) marked open→close (per-name table). KEYS×23 09:30 $324.41 → close $319.97 -102.12; SMTC×7 09:30 $141.76 → close $131.17 -74.13; CIEN×2 09:30 $400.42 → close $378.44 -43.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,802.99 | ▲ 09:30 equity $10,903.24 vs yday $10,837.37 (+65.87) | 09:30 open · cash $1,802.99 (unchanged overnight, no fees) · equity $10,903.24 vs prior close $10,837.37 (+65.87) · 3 name(s) re-marked at the open (per-name table). KEYS×23 yday $319.97 → 09:30 $322.49 +57.96; SMTC×7 yday $131.17 → 09:30 $132.30 +7.91; CIEN×2 yday $378.44 → 09:30 $378.44 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 23 | $322.49 | $2.13 | $-48.35 | $9,218.13 | ▼ -48.35 after sell → book $10,901.11; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 7 | $132.30 | $2.03 | $-70.26 | $10,142.20 | ▼ -70.26 after sell → book $10,899.08; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 2 | $378.44 | $2.02 | $-47.97 | $10,897.07 | ▼ -47.97 after sell → book $10,897.07; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,897.07 | ▲ close $10,897.07 vs 09:30 $10,903.24 (session +0.00) | 16:00 close · cash $10,897.07 · no lots left · equity $10,897.07. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,897.07 | ▲ 09:30 equity $10,897.07 vs yday $10,897.07 (-0.00) | 09:30 open · cash $10,897.07 · no holdings · equity $10,897.07 vs prior close $10,897.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,897.07 | ▲ close $10,897.07 vs 09:30 $10,897.07 (session +0.00) | 16:00 close · cash $10,897.07 · no lots left · equity $10,897.07. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,897.07 | ▲ 09:30 equity $10,897.07 vs yday $10,897.07 (-0.00) | 09:30 open · cash $10,897.07 · no holdings · equity $10,897.07 vs prior close $10,897.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,897.07 | ▲ close $10,897.07 vs 09:30 $10,897.07 (session +0.00) | 16:00 close · cash $10,897.07 · no lots left · equity $10,897.07. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,897.07 | ▲ 09:30 equity $10,897.07 vs yday $10,897.07 (-0.00) | 09:30 open · cash $10,897.07 · no holdings · equity $10,897.07 vs prior close $10,897.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 21 | $351.74 | $2.05 | — | $3,508.47 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $7627.95 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $2,533.86 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1089.71 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 33 | $32.31 | $2.09 | — | $1,465.54 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1089.71 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 68 | $15.87 | $2.19 | — | $384.18 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1089.71 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $384.18 | ▲ close $11,177.30 vs 09:30 $10,897.07 (session +288.57) | 16:00 close · cash $384.18 · equity $11,177.30 vs 09:30 $10,897.07 (+280.23; session marks +288.57) · 4 name(s) marked open→close (per-name table). AVGO×21 09:30 $351.74 → close $357.16 +113.82; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×33 09:30 $32.31 → close $33.66 +44.55; FRNM×68 09:30 $15.87 → close $16.90 +70.04 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $384.18 | ▲ 09:30 equity $11,184.82 vs yday $11,177.30 (+7.52) | 09:30 open · cash $384.18 (unchanged overnight, no fees) · equity $11,184.82 vs prior close $11,177.30 (+7.52) · 4 name(s) re-marked at the open (per-name table). AVGO×21 yday $357.16 → 09:30 $359.70 +53.34; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×33 yday $33.66 → 09:30 $33.46 -6.60; FRNM×68 yday $16.90 → 09:30 $16.40 -34.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 21 | $359.70 | $2.12 | $+162.98 | $7,935.76 | ▲ +162.98 after sell → book $11,182.70; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $8,961.30 | ▲ +50.93 after sell → book $11,180.68; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 33 | $33.46 | $2.11 | $+33.75 | $10,063.38 | ▲ +33.75 after sell → book $11,178.58; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 26 | $263.36 | $2.07 | — | $3,213.95 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $7044.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 19 | $75.65 | $2.05 | — | $1,774.55 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1509.51 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 6 | $236.82 | $2.01 | — | $351.62 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list ohlc_hot; ret5=+8.1; leftover $1509.51 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $351.62 | ▲ close $11,205.03 vs 09:30 $11,184.82 (session +32.58) | 16:00 close · cash $351.62 · equity $11,205.03 vs 09:30 $11,184.82 (+20.21; session marks +32.58) · 4 name(s) marked open→close (per-name table). FRNM×68 09:30 $16.40 → close $16.31 -6.12; CRM×26 09:30 $263.36 → close $259.23 -107.38; MRX×19 09:30 $75.65 → close $78.27 +49.78; BE×6 09:30 $236.82 → close $252.87 +96.30 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $351.62 | ▼ 09:30 equity $11,191.18 vs yday $11,205.03 (-13.85) | 09:30 open · cash $351.62 (unchanged overnight, no fees) · equity $11,191.18 vs prior close $11,205.03 (-13.85) · 4 name(s) re-marked at the open (per-name table). FRNM×68 yday $16.31 → 09:30 $16.74 +29.24; CRM×26 yday $259.23 → 09:30 $253.72 -143.26; MRX×19 yday $78.27 → 09:30 $78.84 +10.83; BE×6 yday $252.87 → 09:30 $267.76 +89.34 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 68 | $16.74 | $2.22 | $+54.75 | $1,487.73 | ▲ +54.75 after sell → book $11,188.97; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 26 | $253.72 | $2.13 | $-254.84 | $8,082.32 | ▼ -254.84 after sell → book $11,186.84; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 6 | $267.76 | $2.03 | $+181.60 | $9,686.85 | ▲ +181.60 after sell → book $11,184.81; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,686.85 | ▼ close $11,144.34 vs 09:30 $11,191.18 (session -40.47) | 16:00 close · cash $9,686.85 · equity $11,144.34 vs 09:30 $11,191.18 (-46.84; session marks -40.47) · 1 name(s) marked open→close (per-name table). MRX×19 09:30 $78.84 → close $76.71 -40.47 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,686.85 | ▼ 09:30 equity $11,142.25 vs yday $11,144.34 (-2.09) | 09:30 open · cash $9,686.85 (unchanged overnight, no fees) · equity $11,142.25 vs prior close $11,144.34 (-2.09) · 1 name(s) re-marked at the open (per-name table). MRX×19 yday $76.71 → 09:30 $76.60 -2.09 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 19 | $76.60 | $2.07 | $+13.93 | $11,140.18 | ▲ +13.93 after sell → book $11,140.18; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,140.18 | ▲ close $11,140.18 vs 09:30 $11,142.25 (session +0.00) | 16:00 close · cash $11,140.18 · no lots left · equity $11,140.18. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,140.18 | ▲ 09:30 equity $11,140.18 vs yday $11,140.18 (-0.00) | 09:30 open · cash $11,140.18 · no holdings · equity $11,140.18 vs prior close $11,140.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,140.18 | ▲ close $11,140.18 vs 09:30 $11,140.18 (session +0.00) | 16:00 close · cash $11,140.18 · no lots left · equity $11,140.18. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,140.18 | ▲ 09:30 equity $11,140.18 vs yday $11,140.18 (-0.00) | 09:30 open · cash $11,140.18 · no holdings · equity $11,140.18 vs prior close $11,140.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 47 | $164.43 | $2.13 | — | $3,409.84 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $7798.12 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 4 | $242.17 | $2.00 | — | $2,439.15 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list earn_react; ret5=-11.1; leftover $1114.02 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 525 | $2.12 | $6.77 | — | $1,319.38 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1114.02 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 546 | $2.04 | $7.04 | — | $198.50 | — | news🟢 rank cameras; 70% of leftover if +9 −≤1; gate news=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1114.02 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $198.50 | ▼ close $10,460.04 vs 09:30 $11,140.18 (session -662.19) | 16:00 close · cash $198.50 · equity $10,460.04 vs 09:30 $11,140.18 (-680.14; session marks -662.19) · 4 name(s) marked open→close (per-name table). ORCL×47 09:30 $164.43 → close $150.28 -665.05; ADBE×4 09:30 $242.17 → close $252.23 +40.24; BAK×525 09:30 $2.12 → close $2.08 -21.00; AMTX×546 09:30 $2.04 → close $2.01 -16.38 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $198.50 | ▼ 09:30 equity $10,064.99 vs yday $10,460.04 (-395.05) | 09:30 open · cash $198.50 (unchanged overnight, no fees) · equity $10,064.99 vs prior close $10,460.04 (-395.05) · 4 name(s) re-marked at the open (per-name table). ORCL×47 yday $150.28 → 09:30 $141.42 -416.42; ADBE×4 yday $252.23 → 09:30 $261.51 +37.12; BAK×525 yday $2.08 → 09:30 $2.05 -15.75; AMTX×546 yday $2.01 → 09:30 $2.01 +0.00 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 47 | $141.42 | $2.19 | $-1085.80 | $6,843.04 | ▼ -1,085.80 after sell → book $10,062.79; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 4 | $261.51 | $2.02 | $+73.34 | $7,887.06 | ▲ +73.34 after sell → book $10,060.77; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 525 | $2.05 | $6.87 | $-50.39 | $8,956.44 | ▼ -50.39 after sell → book $10,053.90; vs 09:30 mark -6.87 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 546 | $2.01 | $7.14 | $-30.57 | $10,046.76 | ▼ -30.57 after sell → book $10,046.76; vs 09:30 mark -7.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,046.76 | ▲ close $10,046.76 vs 09:30 $10,064.99 (session +0.00) | 16:00 close · cash $10,046.76 · no lots left · equity $10,046.76. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
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
| 2026-08-27 | `ASML` | cash | leftover split 1135.74 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1106.36 < 1 share @ 1306.03 |
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
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
