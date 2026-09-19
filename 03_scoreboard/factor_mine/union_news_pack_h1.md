# Factor mine action — `union_news_pack_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · morning packet news🟢 only (not the merged box)

Cash book **+21.55%** ($12,155) · signal-only (no cash/fees) was +16.25%. Starts YES **21/26**. Fills 64 · skips 20 · realized $+2154.80.

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
- Must-have: the morning news packet box is green.
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
- **Gate** `news_box=good` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $12,154.79.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `NRG` | 27 | — | $120.00 | +0.00 | $126.24 | +168.48 | +168.48 | +0.00 | +168.48 |
| 2026-08-14 | `TLN` | 9 | — | $359.83 | +0.00 | $362.74 | +26.19 | +26.19 | +0.00 | +26.19 |
| 2026-08-14 | `VST` | 22 | — | $146.90 | +0.00 | $148.13 | +27.06 | +27.06 | +0.00 | +27.06 |
| 2026-08-17 | `NRG` | 27 | $126.24 | $127.40 | +31.32 | — | +0.00 | +31.32 | +199.80 | — |
| 2026-08-17 | `TLN` | 9 | $362.74 | $367.88 | +46.26 | — | +0.00 | +46.26 | +72.45 | — |
| 2026-08-17 | `VST` | 22 | $148.13 | $149.37 | +27.28 | — | +0.00 | +27.28 | +54.34 | — |
| 2026-08-17 | `DVN` | 74 | — | $46.18 | +0.00 | $47.57 | +102.86 | +102.86 | +0.00 | +102.86 |
| 2026-08-17 | `EOG` | 24 | — | $142.77 | +0.00 | $146.15 | +81.12 | +81.12 | +0.00 | +81.12 |
| 2026-08-17 | `FANG` | 16 | — | $202.70 | +0.00 | $206.29 | +57.44 | +57.44 | +0.00 | +57.44 |
| 2026-08-18 | `DVN` | 74 | $47.57 | $48.00 | +31.82 | — | +0.00 | +31.82 | +134.68 | — |
| 2026-08-18 | `EOG` | 24 | $146.15 | $148.04 | +45.36 | — | +0.00 | +45.36 | +126.48 | — |
| 2026-08-18 | `FANG` | 16 | $206.29 | $208.93 | +42.24 | — | +0.00 | +42.24 | +99.68 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `APA` | 238 | — | $44.76 | +0.00 | $44.39 | -88.06 | -88.06 | +0.00 | -88.06 |
| 2026-08-21 | `APA` | 238 | $44.39 | $44.52 | +30.94 | — | +0.00 | +30.94 | -57.12 | — |
| 2026-08-21 | `AU` | 44 | — | $119.43 | +0.00 | $121.22 | +78.76 | +78.76 | +0.00 | +78.76 |
| 2026-08-21 | `DE` | 8 | — | $623.26 | +0.00 | $647.47 | +193.68 | +193.68 | +0.00 | +193.68 |
| 2026-08-24 | `AU` | 44 | $121.22 | $120.51 | -31.24 | — | +0.00 | -31.24 | +47.52 | — |
| 2026-08-24 | `DE` | 8 | $647.47 | $653.04 | +44.56 | — | +0.00 | +44.56 | +238.24 | — |
| 2026-08-25 | `AU` | 45 | — | $118.52 | +0.00 | $123.39 | +219.15 | +219.15 | +0.00 | +219.15 |
| 2026-08-25 | `FCX` | 70 | — | $77.13 | +0.00 | $79.91 | +194.60 | +194.60 | +0.00 | +194.60 |
| 2026-08-26 | `AU` | 45 | $123.39 | $119.80 | -161.55 | — | +0.00 | -161.55 | +57.60 | — |
| 2026-08-26 | `FCX` | 70 | $79.91 | $79.34 | -39.90 | — | +0.00 | -39.90 | +154.70 | — |
| 2026-08-26 | `FNV` | 41 | — | $267.02 | +0.00 | $267.37 | +14.35 | +14.35 | +0.00 | +14.35 |
| 2026-08-27 | `FNV` | 41 | $267.37 | $267.23 | -5.74 | — | +0.00 | -5.74 | +8.61 | — |
| 2026-08-27 | `ACMR` | 22 | — | $81.65 | +0.00 | $80.49 | -25.52 | -25.52 | +0.00 | -25.52 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `ASML` | 1 | — | $1746.53 | +0.00 | $1735.01 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-08-27 | `LRCX` | 5 | — | $318.88 | +0.00 | $318.58 | -1.50 | -1.50 | +0.00 | -1.50 |
| 2026-08-27 | `NVDA` | 8 | — | $222.86 | +0.00 | $227.98 | +40.96 | +40.96 | +0.00 | +40.96 |
| 2026-08-27 | `RRC` | 44 | — | $41.44 | +0.00 | $41.64 | +8.80 | +8.80 | +0.00 | +8.80 |
| 2026-08-28 | `ACMR` | 22 | $80.49 | $79.27 | -26.84 | — | +0.00 | -26.84 | -52.36 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `ASML` | 1 | $1735.01 | $1734.75 | -0.26 | — | +0.00 | -0.26 | -11.78 | — |
| 2026-08-28 | `LRCX` | 5 | $318.58 | $318.03 | -2.75 | — | +0.00 | -2.75 | -4.25 | — |
| 2026-08-28 | `NVDA` | 8 | $227.98 | $227.36 | -4.96 | — | +0.00 | -4.96 | +36.00 | — |
| 2026-08-28 | `RRC` | 44 | $41.64 | $41.74 | +4.40 | $41.46 | -12.32 | -7.92 | +13.20 | +0.88 |
| 2026-08-28 | `KEYS` | 5 | — | $324.41 | +0.00 | $319.97 | -22.20 | -22.20 | +0.00 | -22.20 |
| 2026-08-28 | `CIEN` | 4 | — | $400.42 | +0.00 | $378.44 | -87.92 | -87.92 | +0.00 | -87.92 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 7 | — | $240.22 | +0.00 | $236.98 | -22.68 | -22.68 | +0.00 | -22.68 |
| 2026-08-28 | `ADSK` | 7 | — | $261.16 | +0.00 | $260.66 | -3.50 | -3.50 | +0.00 | -3.50 |
| 2026-08-31 | `RRC` | 44 | $41.46 | $42.00 | +23.76 | — | +0.00 | +23.76 | +24.64 | — |
| 2026-08-31 | `KEYS` | 5 | $319.97 | $322.49 | +12.60 | — | +0.00 | +12.60 | -9.60 | — |
| 2026-08-31 | `CIEN` | 4 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -87.92 | — |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | — | +0.00 | +5.64 | -44.13 | — |
| 2026-08-31 | `DDOG` | 7 | $236.98 | $233.97 | -21.10 | — | +0.00 | -21.10 | -43.78 | — |
| 2026-08-31 | `ADSK` | 7 | $260.66 | $257.71 | -20.65 | — | +0.00 | -20.65 | -24.15 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 7 | — | $351.74 | +0.00 | $357.16 | +37.94 | +37.94 | +0.00 | +37.94 |
| 2026-09-03 | `DELL` | 5 | — | $486.31 | +0.00 | $516.39 | +150.40 | +150.40 | +0.00 | +150.40 |
| 2026-09-03 | `HPE` | 56 | — | $47.60 | +0.00 | $54.44 | +383.04 | +383.04 | +0.00 | +383.04 |
| 2026-09-03 | `CIEN` | 7 | — | $354.49 | +0.00 | $317.46 | -259.21 | -259.21 | +0.00 | -259.21 |
| 2026-09-04 | `AVGO` | 7 | $357.16 | $359.70 | +17.78 | — | +0.00 | +17.78 | +55.72 | — |
| 2026-09-04 | `DELL` | 5 | $516.39 | $513.78 | -13.05 | — | +0.00 | -13.05 | +137.35 | — |
| 2026-09-04 | `HPE` | 56 | $54.44 | $53.85 | -33.04 | — | +0.00 | -33.04 | +350.00 | — |
| 2026-09-04 | `CIEN` | 7 | $317.46 | $321.67 | +29.47 | — | +0.00 | +29.47 | -229.74 | — |
| 2026-09-04 | `CRM` | 14 | — | $263.36 | +0.00 | $259.23 | -57.82 | -57.82 | +0.00 | -57.82 |
| 2026-09-04 | `BE` | 15 | — | $236.82 | +0.00 | $252.87 | +240.75 | +240.75 | +0.00 | +240.75 |
| 2026-09-04 | `MSTR` | 26 | — | $137.35 | +0.00 | $142.80 | +141.70 | +141.70 | +0.00 | +141.70 |
| 2026-09-08 | `CRM` | 14 | $259.23 | $253.72 | -77.14 | — | +0.00 | -77.14 | -134.96 | — |
| 2026-09-08 | `BE` | 15 | $252.87 | $267.76 | +223.35 | — | +0.00 | +223.35 | +464.10 | — |
| 2026-09-08 | `MSTR` | 26 | $142.80 | $137.62 | -134.68 | $136.52 | -28.60 | -163.28 | +7.02 | -21.58 |
| 2026-09-09 | `MSTR` | 26 | $136.52 | $141.82 | +137.80 | — | +0.00 | +137.80 | +116.22 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ADBE` | 47 | — | $242.17 | +0.00 | $252.23 | +472.82 | +472.82 | +0.00 | +472.82 |
| 2026-09-14 | `ADBE` | 47 | $252.23 | $261.51 | +436.16 | — | +0.00 | +436.16 | +908.98 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | `LITE` | 13 | — | $934.88 | +0.00 | $893.61 | -536.51 | -536.51 | +0.00 | -536.51 |
| 2026-09-18 | `LITE` | 13 | $893.61 | $915.66 | +286.65 | — | +0.00 | +286.65 | -249.86 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +221.73 | NRG, TLN, VST | — | $283.59 | $10,215.59 | NRG×27, TLN×9, VST×22 |
| 2026-08-17 | +2.25 | $283.59 | NRG×27, TLN×9, VST×22 | $10,320.45 | +104.86 | +241.42 | DVN, EOG, FANG | NRG, TLN, VST | $220.88 | $10,549.30 | DVN×74, EOG×24, FANG×16 |
| 2026-08-18 | -6.20 | $220.88 | DVN×74, EOG×24, FANG×16 | $10,668.72 | +119.42 | +0.00 | — | DVN, EOG, FANG | $10,662.29 | $10,662.29 | — |
| 2026-08-19 | -7.20 | $10,662.29 | — | $10,662.29 | +0.00 | +0.00 | — | — | $10,662.29 | $10,662.29 | — |
| 2026-08-20 | +1.12 | $10,662.29 | — | $10,662.29 | +0.00 | -88.06 | APA | — | $6.34 | $10,571.16 | APA×238 |
| 2026-08-21 | +3.25 | $6.34 | APA×238 | $10,602.10 | +30.94 | +272.44 | AU, DE | APA | $353.77 | $10,867.21 | AU×44, DE×8 |
| 2026-08-24 | -5.17 | $353.77 | AU×44, DE×8 | $10,880.53 | +13.32 | +0.00 | — | AU, DE | $10,876.29 | $10,876.29 | — |
| 2026-08-25 | +1.80 | $10,876.29 | — | $10,876.29 | +0.00 | +413.75 | AU, FCX | — | $139.47 | $11,285.72 | AU×45, FCX×70 |
| 2026-08-26 | +2.02 | $139.47 | AU×45, FCX×70 | $11,084.27 | -201.45 | +14.35 | FNV | AU, FCX | $129.90 | $11,092.07 | FNV×41 |
| 2026-08-27 | — | $129.90 | FNV×41 | $11,086.33 | -5.74 | -20.40 | ACMR, MU, ASML, LRCX, NVDA, RRC | FNV | $1,361.46 | $11,051.54 | ACMR×22, MU×1, ASML×1, LRCX×5, NVDA×8, RRC×44 |
| 2026-08-28 | +0.75 | $1,361.46 | ACMR×22, MU×1, ASML×1, LRCX×5, NVDA×8, RRC×44 | $11,005.03 | -46.51 | -198.39 | KEYS, CIEN, MPWR, DDOG, ADSK | ACMR, MU, ASML, LRCX, NVDA | $1,108.85 | $10,786.44 | RRC×44, KEYS×5, CIEN×4, MPWR×1, DDOG×7, ADSK×7 |
| 2026-08-31 | -5.85 | $1,108.85 | RRC×44, KEYS×5, CIEN×4, MPWR×1, DDOG×7, ADSK×7 | $10,786.68 | +0.24 | +0.00 | — | RRC, KEYS, CIEN, MPWR, DDOG, ADSK | $10,774.40 | $10,774.40 | — |
| 2026-09-01 | -6.30 | $10,774.40 | — | $10,774.40 | +0.00 | +0.00 | — | — | $10,774.40 | $10,774.40 | — |
| 2026-09-02 | -3.83 | $10,774.40 | — | $10,774.40 | +0.00 | +0.00 | — | — | $10,774.40 | $10,774.40 | — |
| 2026-09-03 | -0.90 | $10,774.40 | — | $10,774.40 | +0.00 | +312.17 | AVGO, DELL, HPE, CIEN | — | $725.46 | $11,078.39 | AVGO×7, DELL×5, HPE×56, CIEN×7 |
| 2026-09-04 | +2.25 | $725.46 | AVGO×7, DELL×5, HPE×56, CIEN×7 | $11,079.55 | +1.16 | +324.63 | CRM, BE, MSTR | AVGO, DELL, HPE, CIEN | $254.66 | $11,389.73 | CRM×14, BE×15, MSTR×26 |
| 2026-09-08 | -11.47 | $254.66 | CRM×14, BE×15, MSTR×26 | $11,401.26 | +11.53 | -28.60 | — | CRM, BE | $7,819.00 | $11,368.52 | MSTR×26 |
| 2026-09-09 | -13.95 | $7,819.00 | MSTR×26 | $11,506.32 | +137.80 | +0.00 | — | MSTR | $11,504.21 | $11,504.21 | — |
| 2026-09-10 | -13.28 | $11,504.21 | — | $11,504.21 | -0.00 | +0.00 | — | — | $11,504.21 | $11,504.21 | — |
| 2026-09-11 | +0.50 | $11,504.21 | — | $11,504.21 | -0.00 | +472.82 | ADBE | — | $120.09 | $11,974.90 | ADBE×47 |
| 2026-09-14 | -11.00 | $120.09 | ADBE×47 | $12,411.06 | +436.16 | +0.00 | — | ADBE | $12,408.82 | $12,408.82 | — |
| 2026-09-15 | -3.84 | $12,408.82 | — | $12,408.82 | -0.00 | +0.00 | — | — | $12,408.82 | $12,408.82 | — |
| 2026-09-16 | +5.30 | $12,408.82 | — | $12,408.82 | -0.00 | +0.00 | — | — | $12,408.82 | $12,408.82 | — |
| 2026-09-17 | +7.38 | $12,408.82 | — | $12,408.82 | -0.00 | -536.51 | LITE | — | $253.35 | $11,870.28 | LITE×13 |
| 2026-09-18 | +4.86 | $253.35 | LITE×13 | $12,156.93 | +286.65 | +0.00 | — | LITE | $12,154.79 | $12,154.79 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 27 | $120.00 | $2.07 | — | $6,757.93 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+0.6; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 9 | $359.83 | $2.02 | — | $3,517.44 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+5.9; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 22 | $146.90 | $2.06 | — | $283.59 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+3.6; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $283.59 | ▲ close $10,215.59 vs 09:30 $10,000.00 (session +221.73) | 16:00 close · cash $283.59 · equity $10,215.59 vs 09:30 $10,000.00 (+215.59; session marks +221.73) · 3 name(s) marked open→close (per-name table). NRG×27 09:30 $120.00 → close $126.24 +168.48; TLN×9 09:30 $359.83 → close $362.74 +26.19; VST×22 09:30 $146.90 → close $148.13 +27.06 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $283.59 | ▲ 09:30 equity $10,320.45 vs yday $10,215.59 (+104.86) | 09:30 open · cash $283.59 (unchanged overnight, no fees) · equity $10,320.45 vs prior close $10,215.59 (+104.86) · 3 name(s) re-marked at the open (per-name table). NRG×27 yday $126.24 → 09:30 $127.40 +31.32; TLN×9 yday $362.74 → 09:30 $367.88 +46.26; VST×22 yday $148.13 → 09:30 $149.37 +27.28 | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 27 | $127.40 | $2.11 | $+195.62 | $3,721.28 | ▲ +195.62 after sell → book $10,318.34; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 9 | $367.88 | $2.05 | $+68.38 | $7,030.14 | ▲ +68.38 after sell → book $10,316.28; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 22 | $149.37 | $2.09 | $+50.19 | $10,314.19 | ▲ +50.19 after sell → book $10,314.19; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 74 | $46.18 | $2.21 | — | $6,894.66 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $3438.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 24 | $142.77 | $2.06 | — | $3,466.12 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3438.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 16 | $202.70 | $2.04 | — | $220.88 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; 🔵; ret5=+8.3; leftover $3438.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $220.88 | ▲ close $10,549.30 vs 09:30 $10,320.45 (session +241.42) | 16:00 close · cash $220.88 · equity $10,549.30 vs 09:30 $10,320.45 (+228.85; session marks +241.42) · 3 name(s) marked open→close (per-name table). DVN×74 09:30 $46.18 → close $47.57 +102.86; EOG×24 09:30 $142.77 → close $146.15 +81.12; FANG×16 09:30 $202.70 → close $206.29 +57.44 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $220.88 | ▲ 09:30 equity $10,668.72 vs yday $10,549.30 (+119.42) | 09:30 open · cash $220.88 (unchanged overnight, no fees) · equity $10,668.72 vs prior close $10,549.30 (+119.42) · 3 name(s) re-marked at the open (per-name table). DVN×74 yday $47.57 → 09:30 $48.00 +31.82; EOG×24 yday $146.15 → 09:30 $148.04 +45.36; FANG×16 yday $206.29 → 09:30 $208.93 +42.24 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 74 | $48.00 | $2.25 | $+130.22 | $3,770.63 | ▲ +130.22 after sell → book $10,666.47; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 24 | $148.04 | $2.10 | $+122.32 | $7,321.49 | ▲ +122.32 after sell → book $10,664.37; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 16 | $208.93 | $2.07 | $+95.57 | $10,662.29 | ▲ +95.57 after sell → book $10,662.29; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.29 | ▲ close $10,662.29 vs 09:30 $10,668.72 (session +0.00) | 16:00 close · cash $10,662.29 · no lots left · equity $10,662.29. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.29 | ▲ 09:30 equity $10,662.29 vs yday $10,662.29 (+0.00) | 09:30 open · cash $10,662.29 · no holdings · equity $10,662.29 vs prior close $10,662.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.29 | ▲ close $10,662.29 vs 09:30 $10,662.29 (session +0.00) | 16:00 close · cash $10,662.29 · no lots left · equity $10,662.29. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.29 | ▲ 09:30 equity $10,662.29 vs yday $10,662.29 (+0.00) | 09:30 open · cash $10,662.29 · no holdings · equity $10,662.29 vs prior close $10,662.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 238 | $44.76 | $3.07 | — | $6.34 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $10662.29 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.34 | ▼ close $10,571.16 vs 09:30 $10,662.29 (session -88.06) | 16:00 close · cash $6.34 · equity $10,571.16 vs 09:30 $10,662.29 (-91.13; session marks -88.06) · 1 name(s) marked open→close (per-name table). APA×238 09:30 $44.76 → close $44.39 -88.06 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.34 | ▲ 09:30 equity $10,602.10 vs yday $10,571.16 (+30.94) | 09:30 open · cash $6.34 (unchanged overnight, no fees) · equity $10,602.10 vs prior close $10,571.16 (+30.94) · 1 name(s) re-marked at the open (per-name table). APA×238 yday $44.39 → 09:30 $44.52 +30.94 | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 238 | $44.52 | $3.19 | $-63.38 | $10,598.91 | ▼ -63.38 after sell → book $10,598.91; vs 09:30 mark -3.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 44 | $119.43 | $2.12 | — | $5,341.87 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $5299.45 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 8 | $623.26 | $2.01 | — | $353.77 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $5299.45 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $353.77 | ▲ close $10,867.21 vs 09:30 $10,602.10 (session +272.44) | 16:00 close · cash $353.77 · equity $10,867.21 vs 09:30 $10,602.10 (+265.11; session marks +272.44) · 2 name(s) marked open→close (per-name table). AU×44 09:30 $119.43 → close $121.22 +78.76; DE×8 09:30 $623.26 → close $647.47 +193.68 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $353.77 | ▲ 09:30 equity $10,880.53 vs yday $10,867.21 (+13.32) | 09:30 open · cash $353.77 (unchanged overnight, no fees) · equity $10,880.53 vs prior close $10,867.21 (+13.32) · 2 name(s) re-marked at the open (per-name table). AU×44 yday $121.22 → 09:30 $120.51 -31.24; DE×8 yday $647.47 → 09:30 $653.04 +44.56 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 44 | $120.51 | $2.17 | $+43.22 | $5,654.04 | ▲ +43.22 after sell → book $10,878.36; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 8 | $653.04 | $2.07 | $+234.16 | $10,876.29 | ▲ +234.16 after sell → book $10,876.29; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,876.29 | ▲ close $10,876.29 vs 09:30 $10,880.53 (session +0.00) | 16:00 close · cash $10,876.29 · no lots left · equity $10,876.29. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,876.29 | ▲ 09:30 equity $10,876.29 vs yday $10,876.29 (+0.00) | 09:30 open · cash $10,876.29 · no holdings · equity $10,876.29 vs prior close $10,876.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 45 | $118.52 | $2.12 | — | $5,540.77 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $5438.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 70 | $77.13 | $2.20 | — | $139.47 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $5438.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.47 | ▲ close $11,285.72 vs 09:30 $10,876.29 (session +413.75) | 16:00 close · cash $139.47 · equity $11,285.72 vs 09:30 $10,876.29 (+409.43; session marks +413.75) · 2 name(s) marked open→close (per-name table). AU×45 09:30 $118.52 → close $123.39 +219.15; FCX×70 09:30 $77.13 → close $79.91 +194.60 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.47 | ▼ 09:30 equity $11,084.27 vs yday $11,285.72 (-201.45) | 09:30 open · cash $139.47 (unchanged overnight, no fees) · equity $11,084.27 vs prior close $11,285.72 (-201.45) · 2 name(s) re-marked at the open (per-name table). AU×45 yday $123.39 → 09:30 $119.80 -161.55; FCX×70 yday $79.91 → 09:30 $79.34 -39.90 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 45 | $119.80 | $2.18 | $+53.30 | $5,528.29 | ▲ +53.30 after sell → book $11,082.09; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 70 | $79.34 | $2.26 | $+150.24 | $11,079.83 | ▲ +150.24 after sell → book $11,079.83; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 41 | $267.02 | $2.11 | — | $129.90 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $11079.83 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.90 | ▲ close $11,092.07 vs 09:30 $11,084.27 (session +14.35) | 16:00 close · cash $129.90 · equity $11,092.07 vs 09:30 $11,084.27 (+7.80; session marks +14.35) · 1 name(s) marked open→close (per-name table). FNV×41 09:30 $267.02 → close $267.37 +14.35 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.90 | ▼ 09:30 equity $11,086.33 vs yday $11,092.07 (-5.74) | 09:30 open · cash $129.90 (unchanged overnight, no fees) · equity $11,086.33 vs prior close $11,092.07 (-5.74) · 1 name(s) re-marked at the open (per-name table). FNV×41 yday $267.37 → 09:30 $267.23 -5.74 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 41 | $267.23 | $2.21 | $+4.29 | $11,084.12 | ▲ +4.29 after sell → book $11,084.12; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 22 | $81.65 | $2.06 | — | $9,285.76 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $1847.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $8,316.76 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1847.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ASML` | 1 | $1746.53 | $1.99 | — | $6,568.24 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=-0.3; leftover $1847.35 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 5 | $318.88 | $2.00 | — | $4,971.83 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1847.35 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 8 | $222.86 | $2.01 | — | $3,186.94 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $1847.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 44 | $41.44 | $2.12 | — | $1,361.46 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten; ret5=+3.1; leftover $1847.35 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,361.46 | ▼ close $11,051.54 vs 09:30 $11,086.33 (session -20.40) | 16:00 close · cash $1,361.46 · equity $11,051.54 vs 09:30 $11,086.33 (-34.79; session marks -20.40) · 6 name(s) marked open→close (per-name table). ACMR×22 09:30 $81.65 → close $80.49 -25.52; MU×1 09:30 $967.01 → close $935.39 -31.62; ASML×1 09:30 $1746.53 → close $1735.01 -11.52; LRCX×5 09:30 $318.88 → close $318.58 -1.50; NVDA×8 09:30 $222.86 → close $227.98 +40.96; RRC×44 09:30 $41.44 → close $41.64 +8.80 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,361.46 | ▼ 09:30 equity $11,005.03 vs yday $11,051.54 (-46.51) | 09:30 open · cash $1,361.46 (unchanged overnight, no fees) · equity $11,005.03 vs prior close $11,051.54 (-46.51) · 6 name(s) re-marked at the open (per-name table). ACMR×22 yday $80.49 → 09:30 $79.27 -26.84; MU×1 yday $935.39 → 09:30 $919.29 -16.10; ASML×1 yday $1735.01 → 09:30 $1734.75 -0.26; LRCX×5 yday $318.58 → 09:30 $318.03 -2.75; NVDA×8 yday $227.98 → 09:30 $227.36 -4.96; RRC×44 yday $41.64 → 09:30 $41.74 +4.40 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 22 | $79.27 | $2.08 | $-56.50 | $3,103.32 | ▼ -56.50 after sell → book $11,002.95; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $4,020.59 | ▼ -51.73 after sell → book $11,000.93; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASML` | 1 | $1734.75 | $2.02 | $-15.79 | $5,753.33 | ▼ -15.79 after sell → book $10,998.92; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 5 | $318.03 | $2.03 | $-8.28 | $7,341.45 | ▼ -8.28 after sell → book $10,996.89; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 8 | $227.36 | $2.04 | $+31.95 | $9,158.29 | ▲ +31.95 after sell → book $10,994.85; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 5 | $324.41 | $2.00 | — | $7,534.23 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1831.66 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 4 | $400.42 | $2.00 | — | $5,930.55 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1831.66 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,622.53 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1831.66 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 7 | $240.22 | $2.01 | — | $2,938.98 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1831.66 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 7 | $261.16 | $2.01 | — | $1,108.85 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=+7.8; leftover $1831.66 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,108.85 | ▼ close $10,786.44 vs 09:30 $11,005.03 (session -198.39) | 16:00 close · cash $1,108.85 · equity $10,786.44 vs 09:30 $11,005.03 (-218.59; session marks -198.39) · 6 name(s) marked open→close (per-name table). RRC×44 09:30 $41.74 → close $41.46 -12.32; KEYS×5 09:30 $324.41 → close $319.97 -22.20; CIEN×4 09:30 $400.42 → close $378.44 -87.92; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×7 09:30 $240.22 → close $236.98 -22.68; ADSK×7 09:30 $261.16 → close $260.66 -3.50 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,108.85 | ▲ 09:30 equity $10,786.68 vs yday $10,786.44 (+0.24) | 09:30 open · cash $1,108.85 (unchanged overnight, no fees) · equity $10,786.68 vs prior close $10,786.44 (+0.24) · 6 name(s) re-marked at the open (per-name table). RRC×44 yday $41.46 → 09:30 $42.00 +23.76; KEYS×5 yday $319.97 → 09:30 $322.49 +12.60; CIEN×4 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×7 yday $236.98 → 09:30 $233.97 -21.10; ADSK×7 yday $260.66 → 09:30 $257.71 -20.65 | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 44 | $42.00 | $2.15 | $+20.37 | $2,954.70 | ▲ +20.37 after sell → book $10,784.54; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 5 | $322.49 | $2.03 | $-13.63 | $4,565.12 | ▼ -13.63 after sell → book $10,782.51; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 4 | $378.44 | $2.02 | $-91.95 | $6,076.86 | ▼ -91.95 after sell → book $10,780.48; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $7,336.75 | ▼ -48.14 after sell → book $10,778.47; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 7 | $233.97 | $2.03 | $-47.83 | $8,972.47 | ▼ -47.83 after sell → book $10,776.44; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 7 | $257.71 | $2.04 | $-28.20 | $10,774.40 | ▼ -28.20 after sell → book $10,774.40; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,774.40 | ▲ close $10,774.40 vs 09:30 $10,786.68 (session +0.00) | 16:00 close · cash $10,774.40 · no lots left · equity $10,774.40. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,774.40 | ▲ 09:30 equity $10,774.40 vs yday $10,774.40 (+0.00) | 09:30 open · cash $10,774.40 · no holdings · equity $10,774.40 vs prior close $10,774.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,774.40 | ▲ close $10,774.40 vs 09:30 $10,774.40 (session +0.00) | 16:00 close · cash $10,774.40 · no lots left · equity $10,774.40. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,774.40 | ▲ 09:30 equity $10,774.40 vs yday $10,774.40 (+0.00) | 09:30 open · cash $10,774.40 · no holdings · equity $10,774.40 vs prior close $10,774.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,774.40 | ▲ close $10,774.40 vs 09:30 $10,774.40 (session +0.00) | 16:00 close · cash $10,774.40 · no lots left · equity $10,774.40. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,774.40 | ▲ 09:30 equity $10,774.40 vs yday $10,774.40 (+0.00) | 09:30 open · cash $10,774.40 · no holdings · equity $10,774.40 vs prior close $10,774.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 7 | $351.74 | $2.01 | — | $8,310.21 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $2693.60 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 5 | $486.31 | $2.00 | — | $5,876.65 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $2693.60 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 56 | $47.60 | $2.16 | — | $3,208.90 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $2693.60 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 7 | $354.49 | $2.01 | — | $725.46 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; 🔵; ret5=-12.3; leftover $2693.60 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $725.46 | ▲ close $11,078.39 vs 09:30 $10,774.40 (session +312.17) | 16:00 close · cash $725.46 · equity $11,078.39 vs 09:30 $10,774.40 (+303.99; session marks +312.17) · 4 name(s) marked open→close (per-name table). AVGO×7 09:30 $351.74 → close $357.16 +37.94; DELL×5 09:30 $486.31 → close $516.39 +150.40; HPE×56 09:30 $47.60 → close $54.44 +383.04; CIEN×7 09:30 $354.49 → close $317.46 -259.21 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $725.46 | ▲ 09:30 equity $11,079.55 vs yday $11,078.39 (+1.16) | 09:30 open · cash $725.46 (unchanged overnight, no fees) · equity $11,079.55 vs prior close $11,078.39 (+1.16) · 4 name(s) re-marked at the open (per-name table). AVGO×7 yday $357.16 → 09:30 $359.70 +17.78; DELL×5 yday $516.39 → 09:30 $513.78 -13.05; HPE×56 yday $54.44 → 09:30 $53.85 -33.04; CIEN×7 yday $317.46 → 09:30 $321.67 +29.47 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 7 | $359.70 | $2.04 | $+51.67 | $3,241.31 | ▲ +51.67 after sell → book $11,077.50; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 5 | $513.78 | $2.04 | $+133.31 | $5,808.18 | ▲ +133.31 after sell → book $11,075.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 56 | $53.85 | $2.19 | $+345.65 | $8,821.59 | ▲ +345.65 after sell → book $11,073.28; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 7 | $321.67 | $2.04 | $-233.79 | $11,071.24 | ▼ -233.79 after sell → book $11,071.24; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 14 | $263.36 | $2.03 | — | $7,382.17 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $3690.41 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 15 | $236.82 | $2.04 | — | $3,827.83 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+8.1; leftover $3690.41 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 26 | $137.35 | $2.07 | — | $254.66 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list ohlc_hot; ret5=+5.4; leftover $3690.41 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $254.66 | ▲ close $11,389.73 vs 09:30 $11,079.55 (session +324.63) | 16:00 close · cash $254.66 · equity $11,389.73 vs 09:30 $11,079.55 (+310.18; session marks +324.63) · 3 name(s) marked open→close (per-name table). CRM×14 09:30 $263.36 → close $259.23 -57.82; BE×15 09:30 $236.82 → close $252.87 +240.75; MSTR×26 09:30 $137.35 → close $142.80 +141.70 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $254.66 | ▲ 09:30 equity $11,401.26 vs yday $11,389.73 (+11.53) | 09:30 open · cash $254.66 (unchanged overnight, no fees) · equity $11,401.26 vs prior close $11,389.73 (+11.53) · 3 name(s) re-marked at the open (per-name table). CRM×14 yday $259.23 → 09:30 $253.72 -77.14; BE×15 yday $252.87 → 09:30 $267.76 +223.35; MSTR×26 yday $142.80 → 09:30 $137.62 -134.68 | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 14 | $253.72 | $2.07 | $-139.06 | $3,804.67 | ▼ -139.06 after sell → book $11,399.19; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 15 | $267.76 | $2.08 | $+459.99 | $7,819.00 | ▲ +459.99 after sell → book $11,397.12; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,819.00 | ▼ close $11,368.52 vs 09:30 $11,401.26 (session -28.60) | 16:00 close · cash $7,819.00 · equity $11,368.52 vs 09:30 $11,401.26 (-32.74; session marks -28.60) · 1 name(s) marked open→close (per-name table). MSTR×26 09:30 $137.62 → close $136.52 -28.60 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,819.00 | ▲ 09:30 equity $11,506.32 vs yday $11,368.52 (+137.80) | 09:30 open · cash $7,819.00 (unchanged overnight, no fees) · equity $11,506.32 vs prior close $11,368.52 (+137.80) · 1 name(s) re-marked at the open (per-name table). MSTR×26 yday $136.52 → 09:30 $141.82 +137.80 | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 26 | $141.82 | $2.11 | $+112.04 | $11,504.21 | ▲ +112.04 after sell → book $11,504.21; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,504.21 | ▲ close $11,504.21 vs 09:30 $11,506.32 (session +0.00) | 16:00 close · cash $11,504.21 · no lots left · equity $11,504.21. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,504.21 | ▲ 09:30 equity $11,504.21 vs yday $11,504.21 (-0.00) | 09:30 open · cash $11,504.21 · no holdings · equity $11,504.21 vs prior close $11,504.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,504.21 | ▲ close $11,504.21 vs 09:30 $11,504.21 (session +0.00) | 16:00 close · cash $11,504.21 · no lots left · equity $11,504.21. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,504.21 | ▲ 09:30 equity $11,504.21 vs yday $11,504.21 (-0.00) | 09:30 open · cash $11,504.21 · no holdings · equity $11,504.21 vs prior close $11,504.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 47 | $242.17 | $2.13 | — | $120.09 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list earn_react; ret5=-11.1; leftover $11504.21 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $120.09 | ▲ close $11,974.90 vs 09:30 $11,504.21 (session +472.82) | 16:00 close · cash $120.09 · equity $11,974.90 vs 09:30 $11,504.21 (+470.69; session marks +472.82) · 1 name(s) marked open→close (per-name table). ADBE×47 09:30 $242.17 → close $252.23 +472.82 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $120.09 | ▲ 09:30 equity $12,411.06 vs yday $11,974.90 (+436.16) | 09:30 open · cash $120.09 (unchanged overnight, no fees) · equity $12,411.06 vs prior close $11,974.90 (+436.16) · 1 name(s) re-marked at the open (per-name table). ADBE×47 yday $252.23 → 09:30 $261.51 +436.16 | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 47 | $261.51 | $2.24 | $+904.61 | $12,408.82 | ▲ +904.61 after sell → book $12,408.82; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,408.82 | ▲ close $12,408.82 vs 09:30 $12,411.06 (session +0.00) | 16:00 close · cash $12,408.82 · no lots left · equity $12,408.82. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,408.82 | ▲ 09:30 equity $12,408.82 vs yday $12,408.82 (-0.00) | 09:30 open · cash $12,408.82 · no holdings · equity $12,408.82 vs prior close $12,408.82 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,408.82 | ▲ close $12,408.82 vs 09:30 $12,408.82 (session +0.00) | 16:00 close · cash $12,408.82 · no lots left · equity $12,408.82. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,408.82 | ▲ 09:30 equity $12,408.82 vs yday $12,408.82 (-0.00) | 09:30 open · cash $12,408.82 · no holdings · equity $12,408.82 vs prior close $12,408.82 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,408.82 | ▲ close $12,408.82 vs 09:30 $12,408.82 (session +0.00) | 16:00 close · cash $12,408.82 · no lots left · equity $12,408.82. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,408.82 | ▲ 09:30 equity $12,408.82 vs yday $12,408.82 (-0.00) | 09:30 open · cash $12,408.82 · no holdings · equity $12,408.82 vs prior close $12,408.82 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 13 | $934.88 | $2.03 | — | $253.35 | — | morning packet news🟢 only (not the merged box); gate news_box=good; rank cond; list yday_gainer; ret5=+9.4; leftover $12408.82 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $253.35 | ▼ close $11,870.28 vs 09:30 $12,408.82 (session -536.51) | 16:00 close · cash $253.35 · equity $11,870.28 vs 09:30 $12,408.82 (-538.54; session marks -536.51) · 1 name(s) marked open→close (per-name table). LITE×13 09:30 $934.88 → close $893.61 -536.51 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $253.35 | ▲ 09:30 equity $12,156.93 vs yday $11,870.28 (+286.65) | 09:30 open · cash $253.35 (unchanged overnight, no fees) · equity $12,156.93 vs prior close $11,870.28 (+286.65) · 1 name(s) re-marked at the open (per-name table). LITE×13 yday $893.61 → 09:30 $915.66 +286.65 | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 13 | $915.66 | $2.13 | $-254.02 | $12,154.79 | ▼ -254.02 after sell → book $12,154.79; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,154.79 | ▲ close $12,154.79 vs 09:30 $12,156.93 (session +0.00) | 16:00 close · cash $12,154.79 · no lots left · equity $12,154.79. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
