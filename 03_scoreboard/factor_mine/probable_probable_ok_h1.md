# Factor mine action — `probable_probable_ok_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-1.01%** ($9,899) · signal-only (no cash/fees) was +1.27%. Starts YES **10/18**. Fills 78 · skips 32 · realized $+56.28.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at yesterday's 'likely to keep moving' list and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: yesterday's 'likely to keep moving' list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was green (closed up).
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

### When it buys

- At 09:30, take names on yesterday's 'likely to keep moving' list that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Keep the first 8 names in list order.
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

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True,ret_5_max=10.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $14.92.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ANGX` | 464 | — | $4.31 | +0.00 | $4.37 | +27.84 | +27.84 | +0.00 | +27.84 |
| 2026-08-14 | `HYLN` | 478 | — | $4.18 | +0.00 | $4.06 | -57.36 | -57.36 | +0.00 | -57.36 |
| 2026-08-14 | `WDC` | 3 | — | $503.50 | +0.00 | $508.80 | +15.90 | +15.90 | +0.00 | +15.90 |
| 2026-08-14 | `ADUR` | 121 | — | $16.50 | +0.00 | $16.17 | -39.93 | -39.93 | +0.00 | -39.93 |
| 2026-08-14 | `ALGM` | 45 | — | $44.06 | +0.00 | $44.39 | +14.85 | +14.85 | +0.00 | +14.85 |
| 2026-08-17 | `ANGX` | 464 | $4.37 | $4.60 | +106.72 | — | +0.00 | +106.72 | +134.56 | — |
| 2026-08-17 | `HYLN` | 478 | $4.06 | $4.10 | +19.12 | — | +0.00 | +19.12 | -38.24 | — |
| 2026-08-17 | `WDC` | 3 | $508.80 | $525.53 | +50.19 | — | +0.00 | +50.19 | +66.09 | — |
| 2026-08-17 | `ADUR` | 121 | $16.17 | $15.73 | -53.24 | — | +0.00 | -53.24 | -93.17 | — |
| 2026-08-17 | `ALGM` | 45 | $44.39 | $45.32 | +41.85 | — | +0.00 | +41.85 | +56.70 | — |
| 2026-08-17 | `CDNL` | 42 | — | $39.85 | +0.00 | $39.23 | -26.04 | -26.04 | +0.00 | -26.04 |
| 2026-08-17 | `ABX` | 184 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `VERA` | 53 | — | $31.30 | +0.00 | $31.63 | +17.49 | +17.49 | +0.00 | +17.49 |
| 2026-08-17 | `CELC` | 18 | — | $92.99 | +0.00 | $92.44 | -9.90 | -9.90 | +0.00 | -9.90 |
| 2026-08-17 | `OCC` | 92 | — | $18.24 | +0.00 | $17.12 | -103.04 | -103.04 | +0.00 | -103.04 |
| 2026-08-17 | `ALM` | 103 | — | $16.20 | +0.00 | $16.36 | +16.48 | +16.48 | +0.00 | +16.48 |
| 2026-08-18 | `CDNL` | 42 | $39.23 | $41.57 | +98.28 | — | +0.00 | +98.28 | +72.24 | — |
| 2026-08-18 | `ABX` | 184 | $9.12 | $9.03 | -16.56 | — | +0.00 | -16.56 | -16.56 | — |
| 2026-08-18 | `VERA` | 53 | $31.63 | $31.31 | -16.96 | — | +0.00 | -16.96 | +0.53 | — |
| 2026-08-18 | `CELC` | 18 | $92.44 | $92.38 | -1.08 | — | +0.00 | -1.08 | -10.98 | — |
| 2026-08-18 | `OCC` | 92 | $17.12 | $16.20 | -84.64 | — | +0.00 | -84.64 | -187.68 | — |
| 2026-08-18 | `ALM` | 103 | $16.36 | $15.78 | -59.74 | — | +0.00 | -59.74 | -43.26 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `DNA` | 220 | — | $7.45 | +0.00 | $6.96 | -107.80 | -107.80 | +0.00 | -107.80 |
| 2026-08-20 | `MSTR` | 14 | — | $113.23 | +0.00 | $112.39 | -11.76 | -11.76 | +0.00 | -11.76 |
| 2026-08-20 | `EXK` | 152 | — | $10.77 | +0.00 | $10.97 | +30.40 | +30.40 | +0.00 | +30.40 |
| 2026-08-20 | `SCZM` | 173 | — | $9.46 | +0.00 | $9.76 | +51.90 | +51.90 | +0.00 | +51.90 |
| 2026-08-20 | `NG` | 196 | — | $8.38 | +0.00 | $8.66 | +54.88 | +54.88 | +0.00 | +54.88 |
| 2026-08-20 | `BLSH` | 56 | — | $29.20 | +0.00 | $28.44 | -42.56 | -42.56 | +0.00 | -42.56 |
| 2026-08-21 | `DNA` | 220 | $6.96 | $7.09 | +28.60 | — | +0.00 | +28.60 | -79.20 | — |
| 2026-08-21 | `MSTR` | 14 | $112.39 | $119.69 | +102.20 | — | +0.00 | +102.20 | +90.44 | — |
| 2026-08-21 | `EXK` | 152 | $10.97 | $11.34 | +56.24 | — | +0.00 | +56.24 | +86.64 | — |
| 2026-08-21 | `SCZM` | 173 | $9.76 | $10.26 | +86.50 | — | +0.00 | +86.50 | +138.40 | — |
| 2026-08-21 | `NG` | 196 | $8.66 | $9.02 | +70.56 | — | +0.00 | +70.56 | +125.44 | — |
| 2026-08-21 | `BLSH` | 56 | $28.44 | $29.75 | +73.36 | — | +0.00 | +73.36 | +30.80 | — |
| 2026-08-21 | `BTBT` | 1233 | — | $1.66 | +0.00 | $1.53 | -160.29 | -160.29 | +0.00 | -160.29 |
| 2026-08-21 | `DE` | 3 | — | $623.26 | +0.00 | $647.47 | +72.63 | +72.63 | +0.00 | +72.63 |
| 2026-08-21 | `QDEL` | 136 | — | $14.96 | +0.00 | $14.74 | -29.92 | -29.92 | +0.00 | -29.92 |
| 2026-08-21 | `ORBS` | 2370 | — | $0.86 | +0.00 | $0.88 | +37.92 | +37.92 | +0.00 | +37.92 |
| 2026-08-21 | `GORO` | 658 | — | $3.11 | +0.00 | $3.19 | +52.64 | +52.64 | +0.00 | +52.64 |
| 2026-08-24 | `BTBT` | 1233 | $1.53 | $1.55 | +24.66 | — | +0.00 | +24.66 | -135.63 | — |
| 2026-08-24 | `DE` | 3 | $647.47 | $653.62 | +18.45 | — | +0.00 | +18.45 | +91.08 | — |
| 2026-08-24 | `QDEL` | 136 | $14.74 | $14.71 | -4.08 | — | +0.00 | -4.08 | -34.00 | — |
| 2026-08-24 | `ORBS` | 2370 | $0.88 | $0.89 | +23.70 | — | +0.00 | +23.70 | +61.62 | — |
| 2026-08-24 | `GORO` | 658 | $3.19 | $3.20 | +6.58 | — | +0.00 | +6.58 | +59.22 | — |
| 2026-08-25 | `NPWR` | 1270 | — | $2.00 | +0.00 | $2.02 | +25.40 | +25.40 | +0.00 | +25.40 |
| 2026-08-25 | `ALVO` | 486 | — | $5.22 | +0.00 | $5.25 | +14.58 | +14.58 | +0.00 | +14.58 |
| 2026-08-25 | `ALIT` | 171 | — | $14.86 | +0.00 | $14.87 | +1.71 | +1.71 | +0.00 | +1.71 |
| 2026-08-25 | `ZURA` | 394 | — | $6.38 | +0.00 | $6.50 | +47.28 | +47.28 | +0.00 | +47.28 |
| 2026-08-26 | `NPWR` | 1270 | $2.02 | $2.02 | +0.00 | $2.02 | +0.00 | +0.00 | +25.40 | +25.40 |
| 2026-08-26 | `ALVO` | 486 | $5.25 | $5.25 | +0.00 | $5.25 | +0.00 | +0.00 | +14.58 | +14.58 |
| 2026-08-26 | `ALIT` | 171 | $14.87 | $14.87 | +0.00 | $14.87 | +0.00 | +0.00 | +1.71 | +1.71 |
| 2026-08-26 | `ZURA` | 394 | $6.50 | $6.50 | +0.00 | $6.50 | +0.00 | +0.00 | +47.28 | +47.28 |
| 2026-08-27 | `NPWR` | 1270 | $2.02 | $1.93 | -114.30 | — | +0.00 | -114.30 | -88.90 | — |
| 2026-08-27 | `ALVO` | 486 | $5.25 | $4.98 | -131.22 | — | +0.00 | -131.22 | -116.64 | — |
| 2026-08-27 | `ALIT` | 171 | $14.87 | $14.85 | -3.42 | — | +0.00 | -3.42 | -1.71 | — |
| 2026-08-27 | `ZURA` | 394 | $6.50 | $6.13 | -145.78 | — | +0.00 | -145.78 | -98.50 | — |
| 2026-08-28 | `ANF` | 13 | — | $144.70 | +0.00 | $145.75 | +13.65 | +13.65 | +0.00 | +13.65 |
| 2026-08-28 | `BHVN` | 115 | — | $16.95 | +0.00 | $16.12 | -95.45 | -95.45 | +0.00 | -95.45 |
| 2026-08-28 | `BZ` | 105 | — | $18.50 | +0.00 | $18.00 | -52.50 | -52.50 | +0.00 | -52.50 |
| 2026-08-28 | `LVWR` | 1420 | — | $1.38 | +0.00 | $1.36 | -28.40 | -28.40 | +0.00 | -28.40 |
| 2026-08-28 | `GRRR` | 122 | — | $15.94 | +0.00 | $15.66 | -34.16 | -34.16 | +0.00 | -34.16 |
| 2026-08-31 | `ANF` | 13 | $145.75 | $148.67 | +37.96 | — | +0.00 | +37.96 | +51.61 | — |
| 2026-08-31 | `BHVN` | 115 | $16.12 | $15.44 | -78.20 | — | +0.00 | -78.20 | -173.65 | — |
| 2026-08-31 | `BZ` | 105 | $18.00 | $17.89 | -11.55 | — | +0.00 | -11.55 | -64.05 | — |
| 2026-08-31 | `LVWR` | 1420 | $1.36 | $1.37 | +14.20 | — | +0.00 | +14.20 | -14.20 | — |
| 2026-08-31 | `GRRR` | 122 | $15.66 | $14.32 | -163.48 | — | +0.00 | -163.48 | -197.64 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 2553 | — | $1.22 | +0.00 | $1.69 | +1199.91 | +1199.91 | +0.00 | +1199.91 |
| 2026-09-03 | `CRK` | 198 | — | $15.70 | +0.00 | $15.54 | -31.68 | -31.68 | +0.00 | -31.68 |
| 2026-09-03 | `MMED` | 135 | — | $22.78 | +0.00 | $23.76 | +132.30 | +132.30 | +0.00 | +132.30 |
| 2026-09-04 | `GPRO` | 2553 | $1.69 | $1.78 | +229.77 | $1.39 | -995.67 | -765.90 | +1429.68 | +434.01 |
| 2026-09-04 | `CRK` | 198 | $15.54 | $15.45 | -17.82 | — | +0.00 | -17.82 | -49.50 | — |
| 2026-09-04 | `MMED` | 135 | $23.76 | $23.88 | +16.20 | — | +0.00 | +16.20 | +148.50 | — |
| 2026-09-04 | `BAK` | 1074 | — | $1.95 | +0.00 | $1.94 | -10.74 | -10.74 | +0.00 | -10.74 |
| 2026-09-04 | `EOSE` | 587 | — | $3.57 | +0.00 | $3.50 | -41.09 | -41.09 | +0.00 | -41.09 |
| 2026-09-04 | `DELL` | 4 | — | $486.31 | +0.00 | $516.39 | +120.32 | +120.32 | +0.00 | +120.32 |
| 2026-09-07 | `GPRO` | 2553 | $1.39 | $1.48 | +229.77 | — | +0.00 | +229.77 | +663.78 | — |
| 2026-09-07 | `BAK` | 1074 | $1.94 | $1.94 | +0.00 | — | +0.00 | +0.00 | -10.74 | — |
| 2026-09-07 | `EOSE` | 587 | $3.50 | $3.52 | +11.74 | — | +0.00 | +11.74 | -29.35 | — |
| 2026-09-07 | `DELL` | 4 | $516.39 | $513.78 | -10.44 | — | +0.00 | -10.44 | +109.88 | — |
| 2026-09-07 | `CHPT` | 270 | — | $9.28 | +0.00 | $9.89 | +164.70 | +164.70 | +0.00 | +164.70 |
| 2026-09-07 | `CHGG` | 2646 | — | $0.95 | +0.00 | $0.85 | -264.60 | -264.60 | +0.00 | -264.60 |
| 2026-09-07 | `SMMT` | 148 | — | $16.93 | +0.00 | $17.60 | +99.16 | +99.16 | +0.00 | +99.16 |
| 2026-09-07 | `SNOW` | 7 | — | $353.63 | +0.00 | $337.18 | -115.15 | -115.15 | +0.00 | -115.15 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -38.70 | ANGX, HYLN, WDC, ADUR, ALGM | — | $493.79 | $9,942.67 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 |
| 2026-08-17 | +2.25 | $493.79 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 | $10,107.31 | +164.64 | -105.01 | CDNL, ABX, VERA, CELC, OCC, ALM | ANGX, HYLN, WDC, ADUR, ALGM | $43.81 | $9,969.98 | CDNL×42, ABX×184, VERA×53, CELC×18, OCC×92, ALM×103 |
| 2026-08-18 | -6.20 | $43.81 | CDNL×42, ABX×184, VERA×53, CELC×18, OCC×92, ALM×103 | $9,889.28 | -80.70 | +0.00 | — | CDNL, ABX, VERA, CELC, OCC, ALM | $9,875.70 | $9,875.70 | — |
| 2026-08-19 | -7.20 | $9,875.70 | — | $9,875.70 | -0.00 | +0.00 | — | — | $9,875.70 | $9,875.70 | — |
| 2026-08-20 | +1.12 | $9,875.70 | — | $9,875.70 | -0.00 | -24.94 | DNA, MSTR, EXK, SCZM, NG, BLSH | — | $85.61 | $9,836.19 | DNA×220, MSTR×14, EXK×152, SCZM×173, NG×196, BLSH×56 |
| 2026-08-21 | +3.25 | $85.61 | DNA×220, MSTR×14, EXK×152, SCZM×173, NG×196, BLSH×56 | $10,253.65 | +417.46 | -27.02 | BTBT, DE, QDEL, ORBS, GORO | DNA, MSTR, EXK, SCZM, NG, BLSH | $137.31 | $10,155.47 | BTBT×1233, DE×3, QDEL×136, ORBS×2370, GORO×658 |
| 2026-08-24 | -5.17 | $137.31 | BTBT×1233, DE×3, QDEL×136, ORBS×2370, GORO×658 | $10,224.78 | +69.31 | +0.00 | — | BTBT, DE, QDEL, ORBS, GORO | $10,166.97 | $10,166.97 | — |
| 2026-08-25 | +1.80 | $10,166.97 | — | $10,166.97 | -0.00 | +88.97 | NPWR, ALVO, ALIT, ZURA | — | $5.03 | $10,225.70 | NPWR×1270, ALVO×486, ALIT×171, ZURA×394 |
| 2026-08-26 | +2.02 | $5.03 | NPWR×1270, ALVO×486, ALIT×171, ZURA×394 | $10,225.70 | -0.00 | +0.00 | — | — | $5.03 | $10,225.70 | NPWR×1270, ALVO×486, ALIT×171, ZURA×394 |
| 2026-08-27 | — | $5.03 | NPWR×1270, ALVO×486, ALIT×171, ZURA×394 | $9,830.98 | -394.72 | +0.00 | — | NPWR, ALVO, ALIT, ZURA | $9,800.28 | $9,800.28 | — |
| 2026-08-28 | +0.75 | $9,800.28 | — | $9,800.28 | -0.00 | -196.86 | ANF, BHVN, BZ, LVWR, GRRR | — | $95.80 | $9,576.07 | ANF×13, BHVN×115, BZ×105, LVWR×1420, GRRR×122 |
| 2026-08-31 | -5.85 | $95.80 | ANF×13, BHVN×115, BZ×105, LVWR×1420, GRRR×122 | $9,375.00 | -201.07 | +0.00 | — | ANF, BHVN, BZ, LVWR, GRRR | $9,347.28 | $9,347.28 | — |
| 2026-09-01 | -6.30 | $9,347.28 | — | $9,347.28 | +0.00 | +0.00 | — | — | $9,347.28 | $9,347.28 | — |
| 2026-09-02 | -3.83 | $9,347.28 | — | $9,347.28 | +0.00 | +0.00 | — | — | $9,347.28 | $9,347.28 | — |
| 2026-09-03 | -0.90 | $9,347.28 | — | $9,347.28 | +0.00 | +1,300.53 | GPRO, CRK, MMED | — | $10.81 | $10,609.90 | GPRO×2553, CRK×198, MMED×135 |
| 2026-09-04 | — | $10.81 | GPRO×2553, CRK×198, MMED×135 | $10,838.05 | +228.15 | -927.18 | BAK, EOSE, DELL | CRK, MMED | $130.07 | $9,882.36 | GPRO×2553, BAK×1074, EOSE×587, DELL×4 |
| 2026-09-07 | — | $130.07 | GPRO×2553, BAK×1074, EOSE×587, DELL×4 | $10,113.43 | +231.07 | -115.89 | CHPT, CHGG, SMMT, SNOW | GPRO, BAK, EOSE, DELL | $14.92 | $9,899.38 | CHPT×270, CHGG×2646, SMMT×148, SNOW×7 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 478 | $4.18 | $6.17 | — | $5,989.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 3 | $503.50 | $2.00 | — | $4,477.47 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ⚪; ret5=+7.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 121 | $16.50 | $2.35 | — | $2,478.62 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 45 | $44.06 | $2.12 | — | $493.79 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $493.79 | ▼ close $9,942.67 vs 09:30 $10,000.00 (session -38.70) | 16:00 close · cash $493.79 · equity $9,942.67 vs 09:30 $10,000.00 (-57.33; session marks -38.70) · 5 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.31 → close $4.37 +27.84; HYLN×478 09:30 $4.18 → close $4.06 -57.36; WDC×3 09:30 $503.50 → close $508.80 +15.90; ADUR×121 09:30 $16.50 → close $16.17 -39.93; ALGM×45 09:30 $44.06 → close $44.39 +14.85 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $493.79 | ▲ 09:30 equity $10,107.31 vs yday $9,942.67 (+164.64) | 09:30 open · cash $493.79 (unchanged overnight, no fees) · equity $10,107.31 vs prior close $9,942.67 (+164.64) · 5 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; HYLN×478 yday $4.06 → 09:30 $4.10 +19.12; WDC×3 yday $508.80 → 09:30 $525.53 +50.19; ADUR×121 yday $16.17 → 09:30 $15.73 -53.24; ALGM×45 yday $44.39 → 09:30 $45.32 +41.85 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 464 | $4.60 | $6.08 | $+122.49 | $2,622.11 | ▲ +122.49 after sell → book $10,101.23; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 478 | $4.10 | $6.26 | $-50.67 | $4,575.65 | ▼ -50.67 after sell → book $10,094.97; vs 09:30 mark -6.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 3 | $525.53 | $2.02 | $+62.07 | $6,150.22 | ▲ +62.07 after sell → book $10,092.95; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 121 | $15.73 | $2.39 | $-97.91 | $8,051.16 | ▼ -97.91 after sell → book $10,090.56; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 45 | $45.32 | $2.15 | $+52.42 | $10,088.41 | ▲ +52.42 after sell → book $10,088.41; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 42 | $39.85 | $2.12 | — | $8,412.59 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1681.40 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 184 | $9.12 | $2.54 | — | $6,731.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1681.40 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 53 | $31.30 | $2.15 | — | $5,070.92 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-3.8; leftover $1681.40 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 18 | $92.99 | $2.04 | — | $3,395.06 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.8; leftover $1681.40 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 92 | $18.24 | $2.27 | — | $1,714.71 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1681.40 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 103 | $16.20 | $2.30 | — | $43.81 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1681.40 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.81 | ▼ close $9,969.98 vs 09:30 $10,107.31 (session -105.01) | 16:00 close · cash $43.81 · equity $9,969.98 vs 09:30 $10,107.31 (-137.33; session marks -105.01) · 6 name(s) marked open→close (per-name table). CDNL×42 09:30 $39.85 → close $39.23 -26.04; ABX×184 09:30 $9.12 → close $9.12 +0.00; VERA×53 09:30 $31.30 → close $31.63 +17.49; CELC×18 09:30 $92.99 → close $92.44 -9.90; OCC×92 09:30 $18.24 → close $17.12 -103.04; ALM×103 09:30 $16.20 → close $16.36 +16.48 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.81 | ▼ 09:30 equity $9,889.28 vs yday $9,969.98 (-80.70) | 09:30 open · cash $43.81 (unchanged overnight, no fees) · equity $9,889.28 vs prior close $9,969.98 (-80.70) · 6 name(s) re-marked at the open (per-name table). CDNL×42 yday $39.23 → 09:30 $41.57 +98.28; ABX×184 yday $9.12 → 09:30 $9.03 -16.56; VERA×53 yday $31.63 → 09:30 $31.31 -16.96; CELC×18 yday $92.44 → 09:30 $92.38 -1.08; OCC×92 yday $17.12 → 09:30 $16.20 -84.64; ALM×103 yday $16.36 → 09:30 $15.78 -59.74 | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 42 | $41.57 | $2.14 | $+67.98 | $1,787.61 | ▲ +67.98 after sell → book $9,887.14; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 184 | $9.03 | $2.59 | $-21.69 | $3,446.55 | ▼ -21.69 after sell → book $9,884.56; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 53 | $31.31 | $2.17 | $-3.79 | $5,103.81 | ▼ -3.79 after sell → book $9,882.39; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 18 | $92.38 | $2.07 | $-15.09 | $6,764.58 | ▼ -15.09 after sell → book $9,880.32; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 92 | $16.20 | $2.29 | $-192.24 | $8,252.68 | ▼ -192.24 after sell → book $9,878.02; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 103 | $15.78 | $2.33 | $-47.89 | $9,875.70 | ▼ -47.89 after sell → book $9,875.70; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,875.70 | ▲ close $9,875.70 vs 09:30 $9,889.28 (session +0.00) | 16:00 close · cash $9,875.70 · no lots left · equity $9,875.70. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,875.70 | ▲ 09:30 equity $9,875.70 vs yday $9,875.70 (-0.00) | 09:30 open · cash $9,875.70 · no holdings · equity $9,875.70 vs prior close $9,875.70 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,875.70 | ▲ close $9,875.70 vs 09:30 $9,875.70 (session +0.00) | 16:00 close · cash $9,875.70 · no lots left · equity $9,875.70. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,875.70 | ▲ 09:30 equity $9,875.70 vs yday $9,875.70 (-0.00) | 09:30 open · cash $9,875.70 · no holdings · equity $9,875.70 vs prior close $9,875.70 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 220 | $7.45 | $2.84 | — | $8,233.86 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1645.95 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 14 | $113.23 | $2.03 | — | $6,646.61 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1645.95 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 152 | $10.77 | $2.45 | — | $5,007.12 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1645.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 173 | $9.46 | $2.51 | — | $3,368.03 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1645.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 196 | $8.38 | $2.58 | — | $1,722.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1645.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 56 | $29.20 | $2.16 | — | $85.61 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1645.95 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.61 | ▼ close $9,836.19 vs 09:30 $9,875.70 (session -24.94) | 16:00 close · cash $85.61 · equity $9,836.19 vs 09:30 $9,875.70 (-39.51; session marks -24.94) · 6 name(s) marked open→close (per-name table). DNA×220 09:30 $7.45 → close $6.96 -107.80; MSTR×14 09:30 $113.23 → close $112.39 -11.76; EXK×152 09:30 $10.77 → close $10.97 +30.40; SCZM×173 09:30 $9.46 → close $9.76 +51.90; NG×196 09:30 $8.38 → close $8.66 +54.88; BLSH×56 09:30 $29.20 → close $28.44 -42.56 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.61 | ▲ 09:30 equity $10,253.65 vs yday $9,836.19 (+417.46) | 09:30 open · cash $85.61 (unchanged overnight, no fees) · equity $10,253.65 vs prior close $9,836.19 (+417.46) · 6 name(s) re-marked at the open (per-name table). DNA×220 yday $6.96 → 09:30 $7.09 +28.60; MSTR×14 yday $112.39 → 09:30 $119.69 +102.20; EXK×152 yday $10.97 → 09:30 $11.34 +56.24; SCZM×173 yday $9.76 → 09:30 $10.26 +86.50; NG×196 yday $8.66 → 09:30 $9.02 +70.56; BLSH×56 yday $28.44 → 09:30 $29.75 +73.36 | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 220 | $7.09 | $2.89 | $-84.92 | $1,642.53 | ▼ -84.92 after sell → book $10,250.77; vs 09:30 mark -2.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 14 | $119.69 | $2.06 | $+86.35 | $3,316.13 | ▲ +86.35 after sell → book $10,248.71; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 152 | $11.34 | $2.48 | $+81.71 | $5,037.33 | ▲ +81.71 after sell → book $10,246.23; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 173 | $10.26 | $2.55 | $+133.34 | $6,809.76 | ▲ +133.34 after sell → book $10,243.68; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NG` | 196 | $9.02 | $2.62 | $+120.24 | $8,575.05 | ▲ +120.24 after sell → book $10,241.05; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 56 | $29.75 | $2.18 | $+26.46 | $10,238.87 | ▲ +26.46 after sell → book $10,238.87; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 1233 | $1.66 | $15.91 | — | $8,176.18 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $2047.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 3 | $623.26 | $2.00 | — | $6,304.40 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $2047.77 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 136 | $14.96 | $2.40 | — | $4,267.45 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-1.6; leftover $2047.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 2370 | $0.86 | $27.59 | — | $2,192.18 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $2047.77 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 658 | $3.11 | $8.49 | — | $137.31 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+7.1; leftover $2047.77 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.31 | ▼ close $10,155.47 vs 09:30 $10,253.65 (session -27.02) | 16:00 close · cash $137.31 · equity $10,155.47 vs 09:30 $10,253.65 (-98.18; session marks -27.02) · 5 name(s) marked open→close (per-name table). BTBT×1233 09:30 $1.66 → close $1.53 -160.29; DE×3 09:30 $623.26 → close $647.47 +72.63; QDEL×136 09:30 $14.96 → close $14.74 -29.92; ORBS×2370 09:30 $0.86 → close $0.88 +37.92; GORO×658 09:30 $3.11 → close $3.19 +52.64 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.31 | ▲ 09:30 equity $10,224.78 vs yday $10,155.47 (+69.31) | 09:30 open · cash $137.31 (unchanged overnight, no fees) · equity $10,224.78 vs prior close $10,155.47 (+69.31) · 5 name(s) re-marked at the open (per-name table). BTBT×1233 yday $1.53 → 09:30 $1.55 +24.66; DE×3 yday $647.47 → 09:30 $653.62 +18.45; QDEL×136 yday $14.74 → 09:30 $14.71 -4.08; ORBS×2370 yday $0.88 → 09:30 $0.89 +23.70; GORO×658 yday $3.19 → 09:30 $3.20 +6.58 | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 1233 | $1.55 | $16.13 | $-167.66 | $2,032.34 | ▼ -167.66 after sell → book $10,208.66; vs 09:30 mark -16.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 3 | $653.62 | $2.02 | $+87.06 | $3,991.17 | ▲ +87.06 after sell → book $10,206.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 136 | $14.71 | $2.44 | $-38.83 | $5,989.29 | ▼ -38.83 after sell → book $10,204.19; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 2370 | $0.89 | $28.61 | $+5.42 | $8,069.98 | ▲ +5.42 after sell → book $10,175.58; vs 09:30 mark -28.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 658 | $3.20 | $8.61 | $+42.12 | $10,166.97 | ▲ +42.12 after sell → book $10,166.97; vs 09:30 mark -8.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,166.97 | ▲ close $10,166.97 vs 09:30 $10,224.78 (session +0.00) | 16:00 close · cash $10,166.97 · no lots left · equity $10,166.97. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,166.97 | ▲ 09:30 equity $10,166.97 vs yday $10,166.97 (-0.00) | 09:30 open · cash $10,166.97 · no holdings · equity $10,166.97 vs prior close $10,166.97 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 1270 | $2.00 | $16.38 | — | $7,610.58 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $2541.74 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 486 | $5.22 | $6.27 | — | $5,067.39 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $2541.74 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 171 | $14.86 | $2.50 | — | $2,523.83 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $2541.74 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 394 | $6.38 | $5.08 | — | $5.03 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $2541.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.03 | ▲ close $10,225.70 vs 09:30 $10,166.97 (session +88.97) | 16:00 close · cash $5.03 · equity $10,225.70 vs 09:30 $10,166.97 (+58.73; session marks +88.97) · 4 name(s) marked open→close (per-name table). NPWR×1270 09:30 $2.00 → close $2.02 +25.40; ALVO×486 09:30 $5.22 → close $5.25 +14.58; ALIT×171 09:30 $14.86 → close $14.87 +1.71; ZURA×394 09:30 $6.38 → close $6.50 +47.28 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.03 | ▲ 09:30 equity $10,225.70 vs yday $10,225.70 (-0.00) | 09:30 open · cash $5.03 (unchanged overnight, no fees) · equity $10,225.70 vs prior close $10,225.70 (-0.00) · 4 name(s) re-marked at the open (per-name table). NPWR×1270 yday $2.02 → 09:30 $2.02 +0.00; ALVO×486 yday $5.25 → 09:30 $5.25 +0.00; ALIT×171 yday $14.87 → 09:30 $14.87 +0.00; ZURA×394 yday $6.50 → 09:30 $6.50 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.03 | ▲ close $10,225.70 vs 09:30 $10,225.70 (session +0.00) | 16:00 close · cash $5.03 · equity $10,225.70 vs 09:30 $10,225.70 (-0.00; session marks +0.00) · 4 name(s) marked open→close (per-name table). NPWR×1270 09:30 $2.02 → close $2.02 +0.00; ALVO×486 09:30 $5.25 → close $5.25 +0.00; ALIT×171 09:30 $14.87 → close $14.87 +0.00; ZURA×394 09:30 $6.50 → close $6.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.03 | ▼ 09:30 equity $9,830.98 vs yday $10,225.70 (-394.72) | 09:30 open · cash $5.03 (unchanged overnight, no fees) · equity $9,830.98 vs prior close $10,225.70 (-394.72) · 4 name(s) re-marked at the open (per-name table). NPWR×1270 yday $2.02 → 09:30 $1.93 -114.30; ALVO×486 yday $5.25 → 09:30 $4.98 -131.22; ALIT×171 yday $14.87 → 09:30 $14.85 -3.42; ZURA×394 yday $6.50 → 09:30 $6.13 -145.78 | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 1270 | $1.93 | $16.61 | $-121.90 | $2,439.52 | ▼ -121.90 after sell → book $9,814.37; vs 09:30 mark -16.61 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 486 | $4.98 | $6.37 | $-129.28 | $4,853.43 | ▼ -129.28 after sell → book $9,808.00; vs 09:30 mark -6.37 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALIT` | 171 | $14.85 | $2.55 | $-6.76 | $7,390.22 | ▼ -6.76 after sell → book $9,805.44; vs 09:30 mark -2.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 394 | $6.13 | $5.17 | $-108.75 | $9,800.28 | ▼ -108.75 after sell → book $9,800.28; vs 09:30 mark -5.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,800.28 | ▲ close $9,800.28 vs 09:30 $9,830.98 (session +0.00) | 16:00 close · cash $9,800.28 · no lots left · equity $9,800.28. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,800.28 | ▲ 09:30 equity $9,800.28 vs yday $9,800.28 (-0.00) | 09:30 open · cash $9,800.28 · no holdings · equity $9,800.28 vs prior close $9,800.28 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 13 | $144.70 | $2.03 | — | $7,917.15 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1960.06 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 115 | $16.95 | $2.33 | — | $5,965.56 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1960.06 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 105 | $18.50 | $2.31 | — | $4,020.76 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1960.06 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 1420 | $1.38 | $18.32 | — | $2,042.84 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1960.06 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 122 | $15.94 | $2.36 | — | $95.80 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1960.06 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.80 | ▼ close $9,576.07 vs 09:30 $9,800.28 (session -196.86) | 16:00 close · cash $95.80 · equity $9,576.07 vs 09:30 $9,800.28 (-224.21; session marks -196.86) · 5 name(s) marked open→close (per-name table). ANF×13 09:30 $144.70 → close $145.75 +13.65; BHVN×115 09:30 $16.95 → close $16.12 -95.45; BZ×105 09:30 $18.50 → close $18.00 -52.50; LVWR×1420 09:30 $1.38 → close $1.36 -28.40; GRRR×122 09:30 $15.94 → close $15.66 -34.16 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.80 | ▼ 09:30 equity $9,375.00 vs yday $9,576.07 (-201.07) | 09:30 open · cash $95.80 (unchanged overnight, no fees) · equity $9,375.00 vs prior close $9,576.07 (-201.07) · 5 name(s) re-marked at the open (per-name table). ANF×13 yday $145.75 → 09:30 $148.67 +37.96; BHVN×115 yday $16.12 → 09:30 $15.44 -78.20; BZ×105 yday $18.00 → 09:30 $17.89 -11.55; LVWR×1420 yday $1.36 → 09:30 $1.37 +14.20; GRRR×122 yday $15.66 → 09:30 $14.32 -163.48 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 13 | $148.67 | $2.05 | $+47.53 | $2,026.46 | ▲ +47.53 after sell → book $9,372.95; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 115 | $15.44 | $2.37 | $-178.35 | $3,799.69 | ▼ -178.35 after sell → book $9,370.58; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 105 | $17.89 | $2.34 | $-68.69 | $5,675.80 | ▼ -68.69 after sell → book $9,368.24; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 1420 | $1.37 | $18.57 | $-51.09 | $7,602.63 | ▼ -51.09 after sell → book $9,349.67; vs 09:30 mark -18.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 122 | $14.32 | $2.39 | $-202.39 | $9,347.28 | ▼ -202.39 after sell → book $9,347.28; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,347.28 | ▲ close $9,347.28 vs 09:30 $9,375.00 (session +0.00) | 16:00 close · cash $9,347.28 · no lots left · equity $9,347.28. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,347.28 | ▲ 09:30 equity $9,347.28 vs yday $9,347.28 (+0.00) | 09:30 open · cash $9,347.28 · no holdings · equity $9,347.28 vs prior close $9,347.28 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,347.28 | ▲ close $9,347.28 vs 09:30 $9,347.28 (session +0.00) | 16:00 close · cash $9,347.28 · no lots left · equity $9,347.28. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,347.28 | ▲ 09:30 equity $9,347.28 vs yday $9,347.28 (+0.00) | 09:30 open · cash $9,347.28 · no holdings · equity $9,347.28 vs prior close $9,347.28 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,347.28 | ▲ close $9,347.28 vs 09:30 $9,347.28 (session +0.00) | 16:00 close · cash $9,347.28 · no lots left · equity $9,347.28. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,347.28 | ▲ 09:30 equity $9,347.28 vs yday $9,347.28 (+0.00) | 09:30 open · cash $9,347.28 · no holdings · equity $9,347.28 vs prior close $9,347.28 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 2553 | $1.22 | $32.93 | — | $6,199.69 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $3115.76 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 198 | $15.70 | $2.58 | — | $3,088.51 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $3115.76 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 135 | $22.78 | $2.40 | — | $10.81 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $3115.76 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.81 | ▲ close $10,609.90 vs 09:30 $9,347.28 (session +1,300.53) | 16:00 close · cash $10.81 · equity $10,609.90 vs 09:30 $9,347.28 (+1262.62; session marks +1300.53) · 3 name(s) marked open→close (per-name table). GPRO×2553 09:30 $1.22 → close $1.69 +1199.91; CRK×198 09:30 $15.70 → close $15.54 -31.68; MMED×135 09:30 $22.78 → close $23.76 +132.30 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.81 | ▲ 09:30 equity $10,838.05 vs yday $10,609.90 (+228.15) | 09:30 open · cash $10.81 (unchanged overnight, no fees) · equity $10,838.05 vs prior close $10,609.90 (+228.15) · 3 name(s) re-marked at the open (per-name table). GPRO×2553 yday $1.69 → 09:30 $1.78 +229.77; CRK×198 yday $15.54 → 09:30 $15.45 -17.82; MMED×135 yday $23.76 → 09:30 $23.88 +16.20 | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 198 | $15.45 | $2.64 | $-54.73 | $3,067.27 | ▼ -54.73 after sell → book $10,835.41; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 135 | $23.88 | $2.44 | $+143.66 | $6,288.63 | ▲ +143.66 after sell → book $10,832.97; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1074 | $1.95 | $13.85 | — | $4,180.47 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $2096.21 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 587 | $3.57 | $7.57 | — | $2,077.31 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $2096.21 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 4 | $486.31 | $2.00 | — | $130.07 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-9.9; leftover $2096.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.07 | ▼ close $9,882.36 vs 09:30 $10,838.05 (session -927.18) | 16:00 close · cash $130.07 · equity $9,882.36 vs 09:30 $10,838.05 (-955.69; session marks -927.18) · 4 name(s) marked open→close (per-name table). GPRO×2553 09:30 $1.78 → close $1.39 -995.67; BAK×1074 09:30 $1.95 → close $1.94 -10.74; EOSE×587 09:30 $3.57 → close $3.50 -41.09; DELL×4 09:30 $486.31 → close $516.39 +120.32 | — |
| 2026-09-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.07 | ▲ 09:30 equity $10,113.43 vs yday $9,882.36 (+231.07) | 09:30 open · cash $130.07 (unchanged overnight, no fees) · equity $10,113.43 vs prior close $9,882.36 (+231.07) · 4 name(s) re-marked at the open (per-name table). GPRO×2553 yday $1.39 → 09:30 $1.48 +229.77; BAK×1074 yday $1.94 → 09:30 $1.94 +0.00; EOSE×587 yday $3.50 → 09:30 $3.52 +11.74; DELL×4 yday $516.39 → 09:30 $513.78 -10.44 | — |
| 2026-09-07 09:30 ET | **SELL** | `GPRO` | 2553 | $1.48 | $33.39 | $+597.46 | $3,875.12 | ▲ +597.46 after sell → book $10,080.04; vs 09:30 mark -33.39 | dropped from list after 2 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **SELL** | `BAK` | 1074 | $1.94 | $14.05 | $-38.64 | $5,944.63 | ▼ -38.64 after sell → book $10,065.99; vs 09:30 mark -14.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `EOSE` | 587 | $3.52 | $7.69 | $-44.61 | $8,003.18 | ▼ -44.61 after sell → book $10,058.30; vs 09:30 mark -7.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `DELL` | 4 | $513.78 | $2.03 | $+105.85 | $10,056.28 | ▲ +105.85 after sell → book $10,056.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **BUY** | `CHPT` | 270 | $9.28 | $3.48 | — | $7,547.19 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.1; leftover $2514.07 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `CHGG` | 2646 | $0.95 | $33.08 | — | $5,000.42 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $2514.07 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `SMMT` | 148 | $16.93 | $2.43 | — | $2,492.34 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-1.4; leftover $2514.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `SNOW` | 7 | $353.63 | $2.01 | — | $14.92 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+1.2; leftover $2514.07 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.92 | ▼ close $9,899.38 vs 09:30 $10,113.43 (session -115.89) | 16:00 close · cash $14.92 · equity $9,899.38 vs 09:30 $10,113.43 (-214.05; session marks -115.89) · 4 name(s) marked open→close (per-name table). CHPT×270 09:30 $9.28 → close $9.89 +164.70; CHGG×2646 09:30 $0.95 → close $0.85 -264.60; SMMT×148 09:30 $16.93 → close $17.60 +99.16; SNOW×7 09:30 $353.63 → close $337.18 -115.15 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALIT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CHPT` | 270 | 2026-09-07 @ $9.28 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.1; leftover $2514.07 |
| `CHGG` | 2646 | 2026-09-07 @ $0.95 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $2514.07 |
| `SMMT` | 148 | 2026-09-07 @ $16.93 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-1.4; leftover $2514.07 |
| `SNOW` | 7 | 2026-09-07 @ $353.63 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+1.2; leftover $2514.07 |
