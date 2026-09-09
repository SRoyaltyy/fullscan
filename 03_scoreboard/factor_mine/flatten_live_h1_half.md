# Factor mine action — `flatten_live_h1_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **+1.96%** ($10,196) · signal-only (no cash/fees) was +1.27%. Starts YES **6/19**. Fills 48 · skips 0 · realized $+196.36.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the flatten wish-list (names the flatten board wanted that morning).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Live flatten gate: new buys only when flatten_robust would actually send 09:30 tickets.

### When it buys

- At 09:30, take names on the flatten wish-list (names the flatten board wanted that morning) that pass the must-haves.
- If the live flatten gate is HOLD / io that morning, buy nobody new.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- Keep the first 8 names in list order.
- Only spend half of leftover cash; the rest stays cash.
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

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,196.37.

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
| 2026-08-20 | `AG` | 30 | — | $20.55 | +0.00 | $21.19 | +19.20 | +19.20 | +0.00 | +19.20 |
| 2026-08-20 | `BHP` | 6 | — | $91.01 | +0.00 | $93.63 | +15.72 | +15.72 | +0.00 | +15.72 |
| 2026-08-20 | `CDE` | 30 | — | $20.65 | +0.00 | $21.11 | +13.80 | +13.80 | +0.00 | +13.80 |
| 2026-08-20 | `HDSN` | 108 | — | $5.77 | +0.00 | $5.57 | -21.60 | -21.60 | +0.00 | -21.60 |
| 2026-08-20 | `IAG` | 31 | — | $19.63 | +0.00 | $20.50 | +26.97 | +26.97 | +0.00 | +26.97 |
| 2026-08-20 | `KGC` | 21 | — | $29.63 | +0.00 | $31.43 | +37.80 | +37.80 | +0.00 | +37.80 |
| 2026-08-20 | `NFGC` | 357 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 4 | — | $144.54 | +0.00 | $150.25 | +22.84 | +22.84 | +0.00 | +22.84 |
| 2026-08-21 | `AG` | 30 | $21.19 | $21.90 | +21.30 | — | +0.00 | +21.30 | +40.50 | — |
| 2026-08-21 | `BHP` | 6 | $93.63 | $95.72 | +12.54 | — | +0.00 | +12.54 | +28.26 | — |
| 2026-08-21 | `CDE` | 30 | $21.11 | $21.75 | +19.20 | — | +0.00 | +19.20 | +33.00 | — |
| 2026-08-21 | `HDSN` | 108 | $5.57 | $5.67 | +10.80 | — | +0.00 | +10.80 | -10.80 | — |
| 2026-08-21 | `IAG` | 31 | $20.50 | $21.17 | +20.77 | — | +0.00 | +20.77 | +47.74 | — |
| 2026-08-21 | `KGC` | 21 | $31.43 | $32.17 | +15.54 | — | +0.00 | +15.54 | +53.34 | — |
| 2026-08-21 | `NFGC` | 357 | $1.75 | $1.79 | +14.28 | — | +0.00 | +14.28 | +14.28 | — |
| 2026-08-21 | `WPM` | 4 | $150.25 | $154.70 | +17.80 | — | +0.00 | +17.80 | +40.64 | — |
| 2026-08-21 | `AU` | 5 | — | $119.43 | +0.00 | $121.22 | +8.95 | +8.95 | +0.00 | +8.95 |
| 2026-08-21 | `AUPH` | 37 | — | $17.20 | +0.00 | $16.65 | -20.35 | -20.35 | +0.00 | -20.35 |
| 2026-08-21 | `AEM` | 2 | — | $216.30 | +0.00 | $216.06 | -0.48 | -0.48 | +0.00 | -0.48 |
| 2026-08-21 | `ARCT` | 57 | — | $11.13 | +0.00 | $13.45 | +132.24 | +132.24 | +0.00 | +132.24 |
| 2026-08-21 | `AUTL` | 258 | — | $2.47 | +0.00 | $2.41 | -15.48 | -15.48 | +0.00 | -15.48 |
| 2026-08-21 | `CRDL` | 330 | — | $1.93 | +0.00 | $1.86 | -23.10 | -23.10 | +0.00 | -23.10 |
| 2026-08-21 | `CRSP` | 10 | — | $59.72 | +0.00 | $59.50 | -2.20 | -2.20 | +0.00 | -2.20 |
| 2026-08-21 | `CYPH` | 483 | — | $1.32 | +0.00 | $1.42 | +48.30 | +48.30 | +0.00 | +48.30 |
| 2026-08-24 | `AU` | 5 | $121.22 | $120.50 | -3.60 | — | +0.00 | -3.60 | +5.35 | — |
| 2026-08-24 | `AUPH` | 37 | $16.65 | $16.60 | -1.85 | — | +0.00 | -1.85 | -22.20 | — |
| 2026-08-24 | `AEM` | 2 | $216.06 | $217.03 | +1.94 | — | +0.00 | +1.94 | +1.46 | — |
| 2026-08-24 | `ARCT` | 57 | $13.45 | $13.26 | -10.83 | — | +0.00 | -10.83 | +121.41 | — |
| 2026-08-24 | `AUTL` | 258 | $2.41 | $2.36 | -12.90 | — | +0.00 | -12.90 | -28.38 | — |
| 2026-08-24 | `CRDL` | 330 | $1.86 | $1.87 | +3.30 | — | +0.00 | +3.30 | -19.80 | — |
| 2026-08-24 | `CRSP` | 10 | $59.50 | $58.79 | -7.10 | — | +0.00 | -7.10 | -9.30 | — |
| 2026-08-24 | `CYPH` | 483 | $1.42 | $1.83 | +198.03 | — | +0.00 | +198.03 | +246.33 | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | `CABA` | 180 | — | $3.63 | +0.00 | $3.48 | -27.00 | -27.00 | +0.00 | -27.00 |
| 2026-09-04 | `ALEC` | 242 | — | $2.70 | +0.00 | $2.51 | -45.98 | -45.98 | +0.00 | -45.98 |
| 2026-09-04 | `BHC` | 94 | — | $6.91 | +0.00 | $6.76 | -14.10 | -14.10 | +0.00 | -14.10 |
| 2026-09-04 | `BMEA` | 338 | — | $1.93 | +0.00 | $1.91 | -6.76 | -6.76 | +0.00 | -6.76 |
| 2026-09-04 | `OABI` | 128 | — | $5.08 | +0.00 | $4.75 | -42.24 | -42.24 | +0.00 | -42.24 |
| 2026-09-04 | `OPK` | 382 | — | $1.71 | +0.00 | $1.61 | -38.20 | -38.20 | +0.00 | -38.20 |
| 2026-09-04 | `VIR` | 56 | — | $11.54 | +0.00 | $11.45 | -5.04 | -5.04 | +0.00 | -5.04 |
| 2026-09-04 | `ATRC` | 12 | — | $52.88 | +0.00 | $52.46 | -5.04 | -5.04 | +0.00 | -5.04 |
| 2026-09-07 | `CABA` | 180 | $3.48 | $3.46 | -3.60 | — | +0.00 | -3.60 | -30.60 | — |
| 2026-09-07 | `ALEC` | 242 | $2.51 | $2.52 | +2.42 | — | +0.00 | +2.42 | -43.56 | — |
| 2026-09-07 | `BHC` | 94 | $6.76 | $6.71 | -4.70 | — | +0.00 | -4.70 | -18.80 | — |
| 2026-09-07 | `BMEA` | 338 | $1.91 | $1.90 | -3.38 | — | +0.00 | -3.38 | -10.14 | — |
| 2026-09-07 | `OABI` | 128 | $4.75 | $4.78 | +3.84 | — | +0.00 | +3.84 | -38.40 | — |
| 2026-09-07 | `OPK` | 382 | $1.61 | $1.59 | -7.64 | — | +0.00 | -7.64 | -45.84 | — |
| 2026-09-07 | `VIR` | 56 | $11.45 | $11.31 | -7.84 | — | +0.00 | -7.84 | -12.88 | — |
| 2026-09-07 | `ATRC` | 12 | $52.46 | $52.03 | -5.16 | — | +0.00 | -5.16 | -10.20 | — |
| 2026-09-08 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +114.73 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $5,141.88 | $10,095.50 | AG×30, BHP×6, CDE×30, HDSN×108, IAG×31, KGC×21, NFGC×357, WPM×4 |
| 2026-08-21 | +3.25 | $5,141.88 | AG×30, BHP×6, CDE×30, HDSN×108, IAG×31, KGC×21, NFGC×357, WPM×4 | $10,227.73 | +132.23 | +127.88 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $5,374.71 | $10,312.07 | AU×5, AUPH×37, AEM×2, ARCT×57, AUTL×258, CRDL×330, CRSP×10, CYPH×483 |
| 2026-08-24 | -5.17 | $5,374.71 | AU×5, AUPH×37, AEM×2, ARCT×57, AUTL×258, CRDL×330, CRSP×10, CYPH×483 | $10,479.06 | +166.99 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,454.66 | $10,454.66 | — |
| 2026-08-25 | +1.80 | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-08-26 | +2.02 | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-08-27 | — | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-08-28 | +0.75 | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-08-31 | -5.85 | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-09-01 | -6.30 | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-09-02 | -3.83 | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-09-03 | -0.90 | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-09-04 | +2.25 | $10,454.66 | — | $10,454.66 | -0.00 | -184.36 | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | — | $5,237.95 | $10,246.53 | CABA×180, ALEC×242, BHC×94, BMEA×338, OABI×128, OPK×382, VIR×56, ATRC×12 |
| 2026-09-07 | — | $5,237.95 | CABA×180, ALEC×242, BHC×94, BMEA×338, OABI×128, OPK×382, VIR×56, ATRC×12 | $10,220.47 | -26.06 | +0.00 | — | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | $10,196.37 | $10,196.37 | — |
| 2026-09-08 | -11.47 | $10,196.37 | — | $10,196.37 | +0.00 | +0.00 | — | — | $10,196.37 | $10,196.37 | — |

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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 30 | $20.55 | $2.08 | — | $9,381.42 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 6 | $91.01 | $2.01 | — | $8,833.35 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 30 | $20.65 | $2.08 | — | $8,211.77 | — | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 108 | $5.77 | $2.31 | — | $7,586.30 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 31 | $19.63 | $2.08 | — | $6,975.69 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 21 | $29.63 | $2.05 | — | $6,351.40 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 357 | $1.75 | $4.61 | — | $5,722.05 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $5,141.88 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,141.88 | ▲ close $10,095.50 vs 09:30 $10,000.00 (session +114.73) | 16:00 close · cash $5,141.88 · equity $10,095.50 vs 09:30 $10,000.00 (+95.50; session marks +114.73) · 8 name(s) marked open→close (per-name table). AG×30 09:30 $20.55 → close $21.19 +19.20; BHP×6 09:30 $91.01 → close $93.63 +15.72; CDE×30 09:30 $20.65 → close $21.11 +13.80; HDSN×108 09:30 $5.77 → close $5.57 -21.60; IAG×31 09:30 $19.63 → close $20.50 +26.97; KGC×21 09:30 $29.63 → close $31.43 +37.80; NFGC×357 09:30 $1.75 → close $1.75 +0.00; WPM×4 09:30 $144.54 → close $150.25 +22.84 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,141.88 | ▲ 09:30 equity $10,227.73 vs yday $10,095.50 (+132.23) | 09:30 open · cash $5,141.88 (unchanged overnight, no fees) · equity $10,227.73 vs prior close $10,095.50 (+132.23) · 8 name(s) re-marked at the open (per-name table). AG×30 yday $21.19 → 09:30 $21.90 +21.30; BHP×6 yday $93.63 → 09:30 $95.72 +12.54; CDE×30 yday $21.11 → 09:30 $21.75 +19.20; HDSN×108 yday $5.57 → 09:30 $5.67 +10.80; IAG×31 yday $20.50 → 09:30 $21.17 +20.77; KGC×21 yday $31.43 → 09:30 $32.17 +15.54; NFGC×357 yday $1.75 → 09:30 $1.79 +14.28; WPM×4 yday $150.25 → 09:30 $154.70 +17.80 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 30 | $21.90 | $2.10 | $+36.32 | $5,796.78 | ▲ +36.32 after sell → book $10,225.63; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 6 | $95.72 | $2.03 | $+24.22 | $6,369.08 | ▲ +24.22 after sell → book $10,223.61; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 30 | $21.75 | $2.10 | $+28.82 | $7,019.48 | ▲ +28.82 after sell → book $10,221.51; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 108 | $5.67 | $2.34 | $-15.46 | $7,629.49 | ▼ -15.46 after sell → book $10,219.16; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 31 | $21.17 | $2.10 | $+43.55 | $8,283.66 | ▲ +43.55 after sell → book $10,217.06; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 21 | $32.17 | $2.07 | $+49.21 | $8,957.16 | ▲ +49.21 after sell → book $10,214.99; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 357 | $1.79 | $4.67 | $+5.00 | $9,591.51 | ▲ +5.00 after sell → book $10,210.31; vs 09:30 mark -4.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 4 | $154.70 | $2.02 | $+36.62 | $10,208.29 | ▲ +36.62 after sell → book $10,208.29; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 5 | $119.43 | $2.00 | — | $9,609.14 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+20.4; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 37 | $17.20 | $2.10 | — | $8,970.64 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 2 | $216.30 | $2.00 | — | $8,536.04 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 57 | $11.13 | $2.16 | — | $7,899.47 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 258 | $2.47 | $3.33 | — | $7,258.88 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 330 | $1.93 | $4.26 | — | $6,617.72 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 10 | $59.72 | $2.02 | — | $6,018.50 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 483 | $1.32 | $6.23 | — | $5,374.71 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,374.71 | ▲ close $10,312.07 vs 09:30 $10,227.73 (session +127.88) | 16:00 close · cash $5,374.71 · equity $10,312.07 vs 09:30 $10,227.73 (+84.34; session marks +127.88) · 8 name(s) marked open→close (per-name table). AU×5 09:30 $119.43 → close $121.22 +8.95; AUPH×37 09:30 $17.20 → close $16.65 -20.35; AEM×2 09:30 $216.30 → close $216.06 -0.48; ARCT×57 09:30 $11.13 → close $13.45 +132.24; AUTL×258 09:30 $2.47 → close $2.41 -15.48; CRDL×330 09:30 $1.93 → close $1.86 -23.10; CRSP×10 09:30 $59.72 → close $59.50 -2.20; CYPH×483 09:30 $1.32 → close $1.42 +48.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,374.71 | ▲ 09:30 equity $10,479.06 vs yday $10,312.07 (+166.99) | 09:30 open · cash $5,374.71 (unchanged overnight, no fees) · equity $10,479.06 vs prior close $10,312.07 (+166.99) · 8 name(s) re-marked at the open (per-name table). AU×5 yday $121.22 → 09:30 $120.50 -3.60; AUPH×37 yday $16.65 → 09:30 $16.60 -1.85; AEM×2 yday $216.06 → 09:30 $217.03 +1.94; ARCT×57 yday $13.45 → 09:30 $13.26 -10.83; AUTL×258 yday $2.41 → 09:30 $2.36 -12.90; CRDL×330 yday $1.86 → 09:30 $1.87 +3.30; CRSP×10 yday $59.50 → 09:30 $58.79 -7.10; CYPH×483 yday $1.42 → 09:30 $1.83 +198.03 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 5 | $120.50 | $2.02 | $+1.32 | $5,975.19 | ▲ +1.32 after sell → book $10,477.04; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 37 | $16.60 | $2.12 | $-26.42 | $6,587.27 | ▼ -26.42 after sell → book $10,474.92; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 2 | $217.03 | $2.02 | $-2.55 | $7,019.31 | ▼ -2.55 after sell → book $10,472.90; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 57 | $13.26 | $2.18 | $+117.07 | $7,772.95 | ▲ +117.07 after sell → book $10,470.72; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 258 | $2.36 | $3.38 | $-35.09 | $8,378.45 | ▼ -35.09 after sell → book $10,467.34; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 330 | $1.87 | $4.32 | $-28.38 | $8,991.23 | ▼ -28.38 after sell → book $10,463.02; vs 09:30 mark -4.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 10 | $58.79 | $2.04 | $-13.36 | $9,577.09 | ▼ -13.36 after sell → book $10,460.98; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 483 | $1.83 | $6.32 | $+233.78 | $10,454.66 | ▲ +233.78 after sell → book $10,454.66; vs 09:30 mark -6.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,479.06 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 180 | $3.63 | $2.53 | — | $9,798.73 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $653.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 242 | $2.70 | $3.12 | — | $9,142.20 | — | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+20.4; leftover $653.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 94 | $6.91 | $2.27 | — | $8,490.39 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=-1.1; leftover $653.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 338 | $1.93 | $4.36 | — | $7,833.69 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.8; leftover $653.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 128 | $5.08 | $2.37 | — | $7,181.08 | — | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+28.1; leftover $653.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 382 | $1.71 | $4.93 | — | $6,522.93 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.2; leftover $653.42 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 56 | $11.54 | $2.16 | — | $5,874.53 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.8; leftover $653.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 12 | $52.88 | $2.03 | — | $5,237.95 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.6; leftover $653.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,237.95 | ▼ close $10,246.53 vs 09:30 $10,454.66 (session -184.36) | 16:00 close · cash $5,237.95 · equity $10,246.53 vs 09:30 $10,454.66 (-208.13; session marks -184.36) · 8 name(s) marked open→close (per-name table). CABA×180 09:30 $3.63 → close $3.48 -27.00; ALEC×242 09:30 $2.70 → close $2.51 -45.98; BHC×94 09:30 $6.91 → close $6.76 -14.10; BMEA×338 09:30 $1.93 → close $1.91 -6.76; OABI×128 09:30 $5.08 → close $4.75 -42.24; OPK×382 09:30 $1.71 → close $1.61 -38.20; VIR×56 09:30 $11.54 → close $11.45 -5.04; ATRC×12 09:30 $52.88 → close $52.46 -5.04 | — |
| 2026-09-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,237.95 | ▼ 09:30 equity $10,220.47 vs yday $10,246.53 (-26.06) | 09:30 open · cash $5,237.95 (unchanged overnight, no fees) · equity $10,220.47 vs prior close $10,246.53 (-26.06) · 8 name(s) re-marked at the open (per-name table). CABA×180 yday $3.48 → 09:30 $3.46 -3.60; ALEC×242 yday $2.51 → 09:30 $2.52 +2.42; BHC×94 yday $6.76 → 09:30 $6.71 -4.70; BMEA×338 yday $1.91 → 09:30 $1.90 -3.38; OABI×128 yday $4.75 → 09:30 $4.78 +3.84; OPK×382 yday $1.61 → 09:30 $1.59 -7.64; VIR×56 yday $11.45 → 09:30 $11.31 -7.84; ATRC×12 yday $52.46 → 09:30 $52.03 -5.16 | — |
| 2026-09-07 09:30 ET | **SELL** | `CABA` | 180 | $3.46 | $2.57 | $-35.70 | $5,858.18 | ▼ -35.70 after sell → book $10,217.90; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **SELL** | `ALEC` | 242 | $2.52 | $3.17 | $-49.85 | $6,464.84 | ▼ -49.85 after sell → book $10,214.72; vs 09:30 mark -3.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `BHC` | 94 | $6.71 | $2.30 | $-23.37 | $7,093.29 | ▼ -23.37 after sell → book $10,212.43; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `BMEA` | 338 | $1.90 | $4.43 | $-18.93 | $7,731.06 | ▼ -18.93 after sell → book $10,208.00; vs 09:30 mark -4.43 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **SELL** | `OABI` | 128 | $4.78 | $2.41 | $-43.18 | $8,340.50 | ▼ -43.18 after sell → book $10,205.60; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `OPK` | 382 | $1.59 | $5.00 | $-55.77 | $8,942.87 | ▼ -55.77 after sell → book $10,200.59; vs 09:30 mark -5.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `VIR` | 56 | $11.31 | $2.18 | $-17.22 | $9,574.06 | ▼ -17.22 after sell → book $10,198.42; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **SELL** | `ATRC` | 12 | $52.03 | $2.05 | $-14.27 | $10,196.37 | ▼ -14.27 after sell → book $10,196.37; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,196.37 | ▲ close $10,196.37 vs 09:30 $10,220.47 (session +0.00) | 16:00 close · cash $10,196.37 · no lots left · equity $10,196.37. | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,196.37 | ▲ 09:30 equity $10,196.37 vs yday $10,196.37 (+0.00) | 09:30 open · cash $10,196.37 · no holdings · equity $10,196.37 vs prior close $10,196.37 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,196.37 | ▲ close $10,196.37 vs 09:30 $10,196.37 (session +0.00) | 16:00 close · cash $10,196.37 · no lots left · equity $10,196.37. | — |
