# Factor mine action — `probable_probable_ok_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-1.44%** ($9,856) · signal-only (no cash/fees) was +3.51%. Starts YES **1/21**. Fills 78 · skips 33 · realized $-144.22.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,855.77.

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
| 2026-08-21 | `BTBT` | 1027 | — | $1.66 | +0.00 | $1.53 | -133.51 | -133.51 | +0.00 | -133.51 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `QDEL` | 114 | — | $14.96 | +0.00 | $14.74 | -25.08 | -25.08 | +0.00 | -25.08 |
| 2026-08-21 | `ORBS` | 1975 | — | $0.86 | +0.00 | $0.88 | +31.60 | +31.60 | +0.00 | +31.60 |
| 2026-08-21 | `GORO` | 548 | — | $3.11 | +0.00 | $3.19 | +43.84 | +43.84 | +0.00 | +43.84 |
| 2026-08-21 | `CF` | 13 | — | $127.43 | +0.00 | $129.60 | +28.21 | +28.21 | +0.00 | +28.21 |
| 2026-08-24 | `BTBT` | 1027 | $1.53 | $1.55 | +20.54 | — | +0.00 | +20.54 | -112.97 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.04 | +11.14 | — | +0.00 | +11.14 | +59.56 | — |
| 2026-08-24 | `QDEL` | 114 | $14.74 | $14.74 | +0.00 | — | +0.00 | +0.00 | -25.08 | — |
| 2026-08-24 | `ORBS` | 1975 | $0.88 | $0.89 | +19.75 | — | +0.00 | +19.75 | +51.35 | — |
| 2026-08-24 | `GORO` | 548 | $3.19 | $3.20 | +5.48 | — | +0.00 | +5.48 | +49.32 | — |
| 2026-08-24 | `CF` | 13 | $129.60 | $129.99 | +5.07 | — | +0.00 | +5.07 | +33.28 | — |
| 2026-08-25 | `SAFX` | 4067 | — | $0.36 | +0.00 | $0.35 | -16.27 | -16.27 | +0.00 | -16.27 |
| 2026-08-25 | `VITL` | 130 | — | $11.12 | +0.00 | $11.11 | -1.30 | -1.30 | +0.00 | -1.30 |
| 2026-08-25 | `KURA` | 107 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CCOI` | 153 | — | $9.49 | +0.00 | $9.88 | +59.67 | +59.67 | +0.00 | +59.67 |
| 2026-08-25 | `LIFE` | 39 | — | $36.96 | +0.00 | $38.56 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-25 | `ZIP` | 320 | — | $4.55 | +0.00 | $4.35 | -64.00 | -64.00 | +0.00 | -64.00 |
| 2026-08-25 | `ADIG` | 66 | — | $21.79 | +0.00 | $22.27 | +31.68 | +31.68 | +0.00 | +31.68 |
| 2026-08-26 | `SAFX` | 4067 | $0.35 | $0.35 | -4.07 | — | +0.00 | -4.07 | -20.34 | — |
| 2026-08-26 | `VITL` | 130 | $11.11 | $11.03 | -10.40 | — | +0.00 | -10.40 | -11.70 | — |
| 2026-08-26 | `KURA` | 107 | $13.59 | $13.63 | +4.28 | — | +0.00 | +4.28 | +4.28 | — |
| 2026-08-26 | `CCOI` | 153 | $9.88 | $9.89 | +1.53 | — | +0.00 | +1.53 | +61.20 | — |
| 2026-08-26 | `LIFE` | 39 | $38.56 | $38.24 | -12.48 | — | +0.00 | -12.48 | +49.92 | — |
| 2026-08-26 | `ZIP` | 320 | $4.35 | $4.31 | -12.80 | — | +0.00 | -12.80 | -76.80 | — |
| 2026-08-26 | `ADIG` | 66 | $22.27 | $21.78 | -32.34 | — | +0.00 | -32.34 | -0.66 | — |
| 2026-08-26 | `AVBP` | 81 | — | $31.21 | +0.00 | $31.14 | -5.67 | -5.67 | +0.00 | -5.67 |
| 2026-08-26 | `ABX` | 257 | — | $9.83 | +0.00 | $9.78 | -12.85 | -12.85 | +0.00 | -12.85 |
| 2026-08-26 | `ITG` | 210 | — | $12.04 | +0.00 | $12.45 | +86.10 | +86.10 | +0.00 | +86.10 |
| 2026-08-26 | `SENS` | 265 | — | $9.48 | +0.00 | $9.34 | -37.10 | -37.10 | +0.00 | -37.10 |
| 2026-08-27 | `AVBP` | 81 | $31.14 | $30.79 | -28.35 | — | +0.00 | -28.35 | -34.02 | — |
| 2026-08-27 | `ABX` | 257 | $9.78 | $9.68 | -25.70 | — | +0.00 | -25.70 | -38.55 | — |
| 2026-08-27 | `ITG` | 210 | $12.45 | $12.36 | -18.90 | — | +0.00 | -18.90 | +67.20 | — |
| 2026-08-27 | `SENS` | 265 | $9.34 | $9.33 | -2.65 | — | +0.00 | -2.65 | -39.75 | — |
| 2026-08-28 | `OPTX` | 1164 | — | $8.61 | +0.00 | $8.52 | -104.76 | -104.76 | +0.00 | -104.76 |
| 2026-08-31 | `OPTX` | 1164 | $8.52 | $8.52 | +0.00 | — | +0.00 | +0.00 | -104.76 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ARCT` | 196 | — | $16.77 | +0.00 | $15.56 | -237.16 | -237.16 | +0.00 | -237.16 |
| 2026-09-03 | `CRDL` | 1515 | — | $2.18 | +0.00 | $2.16 | -30.30 | -30.30 | +0.00 | -30.30 |
| 2026-09-03 | `CLYM` | 236 | — | $13.96 | +0.00 | $14.59 | +148.68 | +148.68 | +0.00 | +148.68 |
| 2026-09-04 | `ARCT` | 196 | $15.56 | $15.61 | +9.80 | — | +0.00 | +9.80 | -227.36 | — |
| 2026-09-04 | `CRDL` | 1515 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -30.30 | — |
| 2026-09-04 | `CLYM` | 236 | $14.59 | $14.49 | -23.60 | — | +0.00 | -23.60 | +125.08 | — |
| 2026-09-04 | `DELL` | 18 | — | $513.78 | +0.00 | $524.14 | +186.48 | +186.48 | +0.00 | +186.48 |
| 2026-09-08 | `DELL` | 18 | $524.14 | $521.15 | -53.82 | — | +0.00 | -53.82 | +132.66 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -38.70 | ANGX, HYLN, WDC, ADUR, ALGM | — | $493.79 | $9,942.67 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 |
| 2026-08-17 | +2.25 | $493.79 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 | $10,107.31 | +164.64 | -105.01 | CDNL, ABX, VERA, CELC, OCC, ALM | ANGX, HYLN, WDC, ADUR, ALGM | $43.81 | $9,969.98 | CDNL×42, ABX×184, VERA×53, CELC×18, OCC×92, ALM×103 |
| 2026-08-18 | -6.20 | $43.81 | CDNL×42, ABX×184, VERA×53, CELC×18, OCC×92, ALM×103 | $9,889.28 | -80.70 | +0.00 | — | CDNL, ABX, VERA, CELC, OCC, ALM | $9,875.70 | $9,875.70 | — |
| 2026-08-19 | -7.20 | $9,875.70 | — | $9,875.70 | -0.00 | +0.00 | — | — | $9,875.70 | $9,875.70 | — |
| 2026-08-20 | +1.12 | $9,875.70 | — | $9,875.70 | -0.00 | -24.94 | DNA, MSTR, EXK, SCZM, NG, BLSH | — | $85.61 | $9,836.19 | DNA×220, MSTR×14, EXK×152, SCZM×173, NG×196, BLSH×56 |
| 2026-08-21 | +3.25 | $85.61 | DNA×220, MSTR×14, EXK×152, SCZM×173, NG×196, BLSH×56 | $10,253.65 | +417.46 | -6.52 | BTBT, DE, QDEL, ORBS, GORO, CF | DNA, MSTR, EXK, SCZM, NG, BLSH | $465.16 | $10,182.69 | BTBT×1027, DE×2, QDEL×114, ORBS×1975, GORO×548, CF×13 |
| 2026-08-24 | -5.17 | $465.16 | BTBT×1027, DE×2, QDEL×114, ORBS×1975, GORO×548, CF×13 | $10,244.67 | +61.98 | +0.00 | — | BTBT, DE, QDEL, ORBS, GORO, CF | $10,193.78 | $10,193.78 | — |
| 2026-08-25 | +1.80 | $10,193.78 | — | $10,193.78 | +0.00 | +72.18 | SAFX, VITL, KURA, CCOI, LIFE, ZIP, ADIG | — | $8.19 | $10,223.64 | SAFX×4067, VITL×130, KURA×107, CCOI×153, LIFE×39, ZIP×320, ADIG×66 |
| 2026-08-26 | +2.02 | $8.19 | SAFX×4067, VITL×130, KURA×107, CCOI×153, LIFE×39, ZIP×320, ADIG×66 | $10,157.36 | -66.28 | +30.48 | AVBP, ABX, ITG, SENS | SAFX, VITL, KURA, CCOI, LIFE, ZIP, ADIG | $7.75 | $10,133.15 | AVBP×81, ABX×257, ITG×210, SENS×265 |
| 2026-08-27 | — | $7.75 | AVBP×81, ABX×257, ITG×210, SENS×265 | $10,057.55 | -75.60 | +0.00 | — | AVBP, ABX, ITG, SENS | $10,045.66 | $10,045.66 | — |
| 2026-08-28 | +0.75 | $10,045.66 | — | $10,045.66 | +0.00 | -104.76 | OPTX | — | $8.61 | $9,925.89 | OPTX×1164 |
| 2026-08-31 | -5.85 | $8.61 | OPTX×1164 | $9,925.89 | -0.00 | +0.00 | — | OPTX | $9,910.60 | $9,910.60 | — |
| 2026-09-01 | -6.30 | $9,910.60 | — | $9,910.60 | -0.00 | +0.00 | — | — | $9,910.60 | $9,910.60 | — |
| 2026-09-02 | -3.83 | $9,910.60 | — | $9,910.60 | -0.00 | +0.00 | — | — | $9,910.60 | $9,910.60 | — |
| 2026-09-03 | -0.90 | $9,910.60 | — | $9,910.60 | -0.00 | -118.78 | ARCT, CRDL, CLYM | — | $1.25 | $9,766.65 | ARCT×196, CRDL×1515, CLYM×236 |
| 2026-09-04 | +2.25 | $1.25 | ARCT×196, CRDL×1515, CLYM×236 | $9,752.85 | -13.80 | +186.48 | DELL | ARCT, CRDL, CLYM | $477.20 | $9,911.72 | DELL×18 |
| 2026-09-08 | -11.47 | $477.20 | DELL×18 | $9,857.90 | -53.82 | +0.00 | — | DELL | $9,855.77 | $9,855.77 | — |
| 2026-09-09 | -13.95 | $9,855.77 | — | $9,855.77 | +0.00 | +0.00 | — | — | $9,855.77 | $9,855.77 | — |
| 2026-09-10 | -13.28 | $9,855.77 | — | $9,855.77 | +0.00 | +0.00 | — | — | $9,855.77 | $9,855.77 | — |
| 2026-09-11 | +0.50 | $9,855.77 | — | $9,855.77 | +0.00 | +0.00 | — | — | $9,855.77 | $9,855.77 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 478 | $4.18 | $6.17 | — | $5,989.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 3 | $503.50 | $2.00 | — | $4,477.47 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ⚪; ret5=+7.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 121 | $16.50 | $2.35 | — | $2,478.62 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 45 | $44.06 | $2.12 | — | $493.79 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $493.79 | ▼ close $9,942.67 vs 09:30 $10,000.00 (session -38.70) | 16:00 close · cash $493.79 · equity $9,942.67 vs 09:30 $10,000.00 (-57.33; session marks -38.70) · 5 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.31 → close $4.37 +27.84; HYLN×478 09:30 $4.18 → close $4.06 -57.36; WDC×3 09:30 $503.50 → close $508.80 +15.90; ADUR×121 09:30 $16.50 → close $16.17 -39.93; ALGM×45 09:30 $44.06 → close $44.39 +14.85 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $493.79 | ▲ 09:30 equity $10,107.31 vs yday $9,942.67 (+164.64) | 09:30 open · cash $493.79 (unchanged overnight, no fees) · equity $10,107.31 vs prior close $9,942.67 (+164.64) · 5 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; HYLN×478 yday $4.06 → 09:30 $4.10 +19.12; WDC×3 yday $508.80 → 09:30 $525.53 +50.19; ADUR×121 yday $16.17 → 09:30 $15.73 -53.24; ALGM×45 yday $44.39 → 09:30 $45.32 +41.85 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 464 | $4.60 | $6.08 | $+122.49 | $2,622.11 | ▲ +122.49 after sell → book $10,101.23; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 478 | $4.10 | $6.26 | $-50.67 | $4,575.65 | ▼ -50.67 after sell → book $10,094.97; vs 09:30 mark -6.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 3 | $525.53 | $2.02 | $+62.07 | $6,150.22 | ▲ +62.07 after sell → book $10,092.95; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 121 | $15.73 | $2.39 | $-97.91 | $8,051.16 | ▼ -97.91 after sell → book $10,090.56; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 45 | $45.32 | $2.15 | $+52.42 | $10,088.41 | ▲ +52.42 after sell → book $10,088.41; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 42 | $39.85 | $2.12 | — | $8,412.59 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1681.40 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 184 | $9.12 | $2.54 | — | $6,731.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1681.40 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 53 | $31.30 | $2.15 | — | $5,070.92 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-3.8; leftover $1681.40 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 18 | $92.99 | $2.04 | — | $3,395.06 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.8; leftover $1681.40 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 92 | $18.24 | $2.27 | — | $1,714.71 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1681.40 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
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
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 1027 | $1.66 | $13.25 | — | $8,520.80 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1706.48 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $7,272.28 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1706.48 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 114 | $14.96 | $2.33 | — | $5,564.51 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-1.6; leftover $1706.48 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1975 | $0.86 | $22.99 | — | $3,835.12 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1706.48 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 548 | $3.11 | $7.07 | — | $2,123.77 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+7.1; leftover $1706.48 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CF` | 13 | $127.43 | $2.03 | — | $465.16 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ⚪; ret5=+7.9; leftover $1706.48 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $465.16 | ▼ close $10,182.69 vs 09:30 $10,253.65 (session -6.52) | 16:00 close · cash $465.16 · equity $10,182.69 vs 09:30 $10,253.65 (-70.96; session marks -6.52) · 6 name(s) marked open→close (per-name table). BTBT×1027 09:30 $1.66 → close $1.53 -133.51; DE×2 09:30 $623.26 → close $647.47 +48.42; QDEL×114 09:30 $14.96 → close $14.74 -25.08; ORBS×1975 09:30 $0.86 → close $0.88 +31.60; GORO×548 09:30 $3.11 → close $3.19 +43.84; CF×13 09:30 $127.43 → close $129.60 +28.21 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $465.16 | ▲ 09:30 equity $10,244.67 vs yday $10,182.69 (+61.98) | 09:30 open · cash $465.16 (unchanged overnight, no fees) · equity $10,244.67 vs prior close $10,182.69 (+61.98) · 6 name(s) re-marked at the open (per-name table). BTBT×1027 yday $1.53 → 09:30 $1.55 +20.54; DE×2 yday $647.47 → 09:30 $653.04 +11.14; QDEL×114 yday $14.74 → 09:30 $14.74 +0.00; ORBS×1975 yday $0.88 → 09:30 $0.89 +19.75; GORO×548 yday $3.19 → 09:30 $3.20 +5.48; CF×13 yday $129.60 → 09:30 $129.99 +5.07 | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 1027 | $1.55 | $13.43 | $-139.65 | $2,043.57 | ▼ -139.65 after sell → book $10,231.23; vs 09:30 mark -13.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $3,347.64 | ▲ +55.55 after sell → book $10,229.22; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 114 | $14.74 | $2.36 | $-29.78 | $5,025.63 | ▼ -29.78 after sell → book $10,226.85; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1975 | $0.89 | $23.84 | $+4.52 | $6,759.54 | ▲ +4.52 after sell → book $10,203.01; vs 09:30 mark -23.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 548 | $3.20 | $7.17 | $+35.08 | $8,505.96 | ▲ +35.08 after sell → book $10,195.83; vs 09:30 mark -7.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CF` | 13 | $129.99 | $2.05 | $+29.20 | $10,193.78 | ▲ +29.20 after sell → book $10,193.78; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,193.78 | ▲ close $10,193.78 vs 09:30 $10,244.67 (session +0.00) | 16:00 close · cash $10,193.78 · no lots left · equity $10,193.78. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,193.78 | ▲ 09:30 equity $10,193.78 vs yday $10,193.78 (+0.00) | 09:30 open · cash $10,193.78 · no holdings · equity $10,193.78 vs prior close $10,193.78 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 4067 | $0.36 | $26.76 | — | $8,711.04 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-15.6; leftover $1456.25 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VITL` | 130 | $11.12 | $2.38 | — | $7,263.06 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.7; leftover $1456.25 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 107 | $13.59 | $2.31 | — | $5,806.61 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1456.25 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 153 | $9.49 | $2.45 | — | $4,352.20 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1456.25 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 39 | $36.96 | $2.11 | — | $2,908.65 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1456.25 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 320 | $4.55 | $4.13 | — | $1,448.52 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1456.25 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 66 | $21.79 | $2.19 | — | $8.19 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.1; leftover $1456.25 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.19 | ▲ close $10,223.64 vs 09:30 $10,193.78 (session +72.18) | 16:00 close · cash $8.19 · equity $10,223.64 vs 09:30 $10,193.78 (+29.86; session marks +72.18) · 7 name(s) marked open→close (per-name table). SAFX×4067 09:30 $0.36 → close $0.35 -16.27; VITL×130 09:30 $11.12 → close $11.11 -1.30; KURA×107 09:30 $13.59 → close $13.59 +0.00; CCOI×153 09:30 $9.49 → close $9.88 +59.67; LIFE×39 09:30 $36.96 → close $38.56 +62.40; ZIP×320 09:30 $4.55 → close $4.35 -64.00; ADIG×66 09:30 $21.79 → close $22.27 +31.68 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.19 | ▼ 09:30 equity $10,157.36 vs yday $10,223.64 (-66.28) | 09:30 open · cash $8.19 (unchanged overnight, no fees) · equity $10,157.36 vs prior close $10,223.64 (-66.28) · 7 name(s) re-marked at the open (per-name table). SAFX×4067 yday $0.35 → 09:30 $0.35 -4.07; VITL×130 yday $11.11 → 09:30 $11.03 -10.40; KURA×107 yday $13.59 → 09:30 $13.63 +4.28; CCOI×153 yday $9.88 → 09:30 $9.89 +1.53; LIFE×39 yday $38.56 → 09:30 $38.24 -12.48; ZIP×320 yday $4.35 → 09:30 $4.31 -12.80; ADIG×66 yday $22.27 → 09:30 $21.78 -32.34 | — |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 4067 | $0.35 | $27.24 | $-74.34 | $1,416.60 | ▼ -74.34 after sell → book $10,130.12; vs 09:30 mark -27.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VITL` | 130 | $11.03 | $2.41 | $-16.49 | $2,848.09 | ▼ -16.49 after sell → book $10,127.71; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 107 | $13.63 | $2.34 | $-0.37 | $4,304.16 | ▼ -0.37 after sell → book $10,125.37; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 153 | $9.89 | $2.49 | $+56.26 | $5,814.84 | ▲ +56.26 after sell → book $10,122.88; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 39 | $38.24 | $2.13 | $+45.68 | $7,304.07 | ▲ +45.68 after sell → book $10,120.75; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 320 | $4.31 | $4.19 | $-85.12 | $8,679.08 | ▼ -85.12 after sell → book $10,116.56; vs 09:30 mark -4.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ADIG` | 66 | $21.78 | $2.21 | $-5.06 | $10,114.35 | ▼ -5.06 after sell → book $10,114.35; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 81 | $31.21 | $2.23 | — | $7,584.10 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $2528.59 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 257 | $9.83 | $3.32 | — | $5,054.48 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $2528.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ITG` | 210 | $12.04 | $2.71 | — | $2,523.37 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-5.1; leftover $2528.59 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 265 | $9.48 | $3.42 | — | $7.75 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $2528.59 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.75 | ▲ close $10,133.15 vs 09:30 $10,157.36 (session +30.48) | 16:00 close · cash $7.75 · equity $10,133.15 vs 09:30 $10,157.36 (-24.21; session marks +30.48) · 4 name(s) marked open→close (per-name table). AVBP×81 09:30 $31.21 → close $31.14 -5.67; ABX×257 09:30 $9.83 → close $9.78 -12.85; ITG×210 09:30 $12.04 → close $12.45 +86.10; SENS×265 09:30 $9.48 → close $9.34 -37.10 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.75 | ▼ 09:30 equity $10,057.55 vs yday $10,133.15 (-75.60) | 09:30 open · cash $7.75 (unchanged overnight, no fees) · equity $10,057.55 vs prior close $10,133.15 (-75.60) · 4 name(s) re-marked at the open (per-name table). AVBP×81 yday $31.14 → 09:30 $30.79 -28.35; ABX×257 yday $9.78 → 09:30 $9.68 -25.70; ITG×210 yday $12.45 → 09:30 $12.36 -18.90; SENS×265 yday $9.34 → 09:30 $9.33 -2.65 | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 81 | $30.79 | $2.27 | $-38.52 | $2,499.48 | ▼ -38.52 after sell → book $10,055.29; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABX` | 257 | $9.68 | $3.38 | $-45.24 | $4,983.86 | ▼ -45.24 after sell → book $10,051.91; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ITG` | 210 | $12.36 | $2.76 | $+61.73 | $7,576.69 | ▲ +61.73 after sell → book $10,049.14; vs 09:30 mark -2.77 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SENS` | 265 | $9.33 | $3.48 | $-46.65 | $10,045.66 | ▼ -46.65 after sell → book $10,045.66; vs 09:30 mark -3.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,045.66 | ▲ close $10,045.66 vs 09:30 $10,057.55 (session +0.00) | 16:00 close · cash $10,045.66 · no lots left · equity $10,045.66. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,045.66 | ▲ 09:30 equity $10,045.66 vs yday $10,045.66 (+0.00) | 09:30 open · cash $10,045.66 · no holdings · equity $10,045.66 vs prior close $10,045.66 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 1164 | $8.61 | $15.02 | — | $8.61 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-0.7; leftover $10045.66 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.61 | ▼ close $9,925.89 vs 09:30 $10,045.66 (session -104.76) | 16:00 close · cash $8.61 · equity $9,925.89 vs 09:30 $10,045.66 (-119.77; session marks -104.76) · 1 name(s) marked open→close (per-name table). OPTX×1164 09:30 $8.61 → close $8.52 -104.76 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.61 | ▲ 09:30 equity $9,925.89 vs yday $9,925.89 (-0.00) | 09:30 open · cash $8.61 (unchanged overnight, no fees) · equity $9,925.89 vs prior close $9,925.89 (-0.00) · 1 name(s) re-marked at the open (per-name table). OPTX×1164 yday $8.52 → 09:30 $8.52 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 1164 | $8.52 | $15.29 | $-135.06 | $9,910.60 | ▼ -135.06 after sell → book $9,910.60; vs 09:30 mark -15.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,910.60 | ▲ close $9,910.60 vs 09:30 $9,925.89 (session +0.00) | 16:00 close · cash $9,910.60 · no lots left · equity $9,910.60. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,910.60 | ▲ 09:30 equity $9,910.60 vs yday $9,910.60 (-0.00) | 09:30 open · cash $9,910.60 · no holdings · equity $9,910.60 vs prior close $9,910.60 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,910.60 | ▲ close $9,910.60 vs 09:30 $9,910.60 (session +0.00) | 16:00 close · cash $9,910.60 · no lots left · equity $9,910.60. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,910.60 | ▲ 09:30 equity $9,910.60 vs yday $9,910.60 (-0.00) | 09:30 open · cash $9,910.60 · no holdings · equity $9,910.60 vs prior close $9,910.60 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,910.60 | ▲ close $9,910.60 vs 09:30 $9,910.60 (session +0.00) | 16:00 close · cash $9,910.60 · no lots left · equity $9,910.60. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,910.60 | ▲ 09:30 equity $9,910.60 vs yday $9,910.60 (-0.00) | 09:30 open · cash $9,910.60 · no holdings · equity $9,910.60 vs prior close $9,910.60 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 196 | $16.77 | $2.58 | — | $6,621.10 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $3303.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 1515 | $2.18 | $19.54 | — | $3,298.86 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $3303.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 236 | $13.96 | $3.04 | — | $1.25 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-6.4; leftover $3303.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.25 | ▼ close $9,766.65 vs 09:30 $9,910.60 (session -118.78) | 16:00 close · cash $1.25 · equity $9,766.65 vs 09:30 $9,910.60 (-143.95; session marks -118.78) · 3 name(s) marked open→close (per-name table). ARCT×196 09:30 $16.77 → close $15.56 -237.16; CRDL×1515 09:30 $2.18 → close $2.16 -30.30; CLYM×236 09:30 $13.96 → close $14.59 +148.68 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.25 | ▼ 09:30 equity $9,752.85 vs yday $9,766.65 (-13.80) | 09:30 open · cash $1.25 (unchanged overnight, no fees) · equity $9,752.85 vs prior close $9,766.65 (-13.80) · 3 name(s) re-marked at the open (per-name table). ARCT×196 yday $15.56 → 09:30 $15.61 +9.80; CRDL×1515 yday $2.16 → 09:30 $2.16 +0.00; CLYM×236 yday $14.59 → 09:30 $14.49 -23.60 | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 196 | $15.61 | $2.63 | $-232.57 | $3,058.18 | ▼ -232.57 after sell → book $9,750.22; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 1515 | $2.16 | $19.82 | $-69.66 | $6,310.75 | ▼ -69.66 after sell → book $9,730.39; vs 09:30 mark -19.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CLYM` | 236 | $14.49 | $3.11 | $+118.92 | $9,727.28 | ▲ +118.92 after sell → book $9,727.28; vs 09:30 mark -3.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 18 | $513.78 | $2.04 | — | $477.20 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $9727.28 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $477.20 | ▲ close $9,911.72 vs 09:30 $9,752.85 (session +186.48) | 16:00 close · cash $477.20 · equity $9,911.72 vs 09:30 $9,752.85 (+158.87; session marks +186.48) · 1 name(s) marked open→close (per-name table). DELL×18 09:30 $513.78 → close $524.14 +186.48 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $477.20 | ▼ 09:30 equity $9,857.90 vs yday $9,911.72 (-53.82) | 09:30 open · cash $477.20 (unchanged overnight, no fees) · equity $9,857.90 vs prior close $9,911.72 (-53.82) · 1 name(s) re-marked at the open (per-name table). DELL×18 yday $524.14 → 09:30 $521.15 -53.82 | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 18 | $521.15 | $2.13 | $+128.49 | $9,855.77 | ▲ +128.49 after sell → book $9,855.77; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,855.77 | ▲ close $9,855.77 vs 09:30 $9,857.90 (session +0.00) | 16:00 close · cash $9,855.77 · no lots left · equity $9,855.77. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,855.77 | ▲ 09:30 equity $9,855.77 vs yday $9,855.77 (+0.00) | 09:30 open · cash $9,855.77 · no holdings · equity $9,855.77 vs prior close $9,855.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,855.77 | ▲ close $9,855.77 vs 09:30 $9,855.77 (session +0.00) | 16:00 close · cash $9,855.77 · no lots left · equity $9,855.77. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,855.77 | ▲ 09:30 equity $9,855.77 vs yday $9,855.77 (+0.00) | 09:30 open · cash $9,855.77 · no holdings · equity $9,855.77 vs prior close $9,855.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,855.77 | ▲ close $9,855.77 vs 09:30 $9,855.77 (session +0.00) | 16:00 close · cash $9,855.77 · no lots left · equity $9,855.77. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,855.77 | ▲ 09:30 equity $9,855.77 vs yday $9,855.77 (+0.00) | 09:30 open · cash $9,855.77 · no holdings · equity $9,855.77 vs prior close $9,855.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,855.77 | ▲ close $9,855.77 vs 09:30 $9,855.77 (session +0.00) | 16:00 close · cash $9,855.77 · no lots left · equity $9,855.77. | — |

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
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XHLD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HELP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `CMRC` | no_price | no 09:30 open |
| 2026-09-11 | `CLOV` | no_price | no 09:30 open |
| 2026-09-11 | `APPS` | no_price | no 09:30 open |
