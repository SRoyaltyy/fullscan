# Factor mine action — `flatten_live_h1_rankw`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `rank_w` · sell `list` · S-boost `none` · rank-weighted leftover

Cash book **+4.03%** ($10,403) · signal-only (no cash/fees) was +4.45%. Starts YES **6/18**. Fills 48 · skips 0 · realized $+403.06.

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
- Split leftover cash by rank (first name gets the biggest slice).
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
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,403.05.

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
| 2026-08-20 | `AG` | 108 | — | $20.55 | +0.00 | $21.19 | +69.12 | +69.12 | +0.00 | +69.12 |
| 2026-08-20 | `BHP` | 21 | — | $91.01 | +0.00 | $93.63 | +55.02 | +55.02 | +0.00 | +55.02 |
| 2026-08-20 | `CDE` | 80 | — | $20.65 | +0.00 | $21.11 | +36.80 | +36.80 | +0.00 | +36.80 |
| 2026-08-20 | `HDSN` | 240 | — | $5.77 | +0.00 | $5.57 | -48.00 | -48.00 | +0.00 | -48.00 |
| 2026-08-20 | `IAG` | 56 | — | $19.63 | +0.00 | $20.50 | +48.72 | +48.72 | +0.00 | +48.72 |
| 2026-08-20 | `KGC` | 28 | — | $29.63 | +0.00 | $31.43 | +50.40 | +50.40 | +0.00 | +50.40 |
| 2026-08-20 | `NFGC` | 317 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 1 | — | $144.54 | +0.00 | $150.25 | +5.71 | +5.71 | +0.00 | +5.71 |
| 2026-08-21 | `AG` | 108 | $21.19 | $21.90 | +76.68 | — | +0.00 | +76.68 | +145.80 | — |
| 2026-08-21 | `BHP` | 21 | $93.63 | $95.72 | +43.89 | — | +0.00 | +43.89 | +98.91 | — |
| 2026-08-21 | `CDE` | 80 | $21.11 | $21.75 | +51.20 | — | +0.00 | +51.20 | +88.00 | — |
| 2026-08-21 | `HDSN` | 240 | $5.57 | $5.67 | +24.00 | — | +0.00 | +24.00 | -24.00 | — |
| 2026-08-21 | `IAG` | 56 | $20.50 | $21.17 | +37.52 | — | +0.00 | +37.52 | +86.24 | — |
| 2026-08-21 | `KGC` | 28 | $31.43 | $32.17 | +20.72 | — | +0.00 | +20.72 | +71.12 | — |
| 2026-08-21 | `NFGC` | 317 | $1.75 | $1.79 | +12.68 | — | +0.00 | +12.68 | +12.68 | — |
| 2026-08-21 | `WPM` | 1 | $150.25 | $154.70 | +4.45 | — | +0.00 | +4.45 | +10.16 | — |
| 2026-08-21 | `AU` | 19 | — | $119.43 | +0.00 | $121.22 | +34.01 | +34.01 | +0.00 | +34.01 |
| 2026-08-21 | `AUPH` | 118 | — | $17.20 | +0.00 | $16.65 | -64.90 | -64.90 | +0.00 | -64.90 |
| 2026-08-21 | `AEM` | 8 | — | $216.30 | +0.00 | $216.06 | -1.92 | -1.92 | +0.00 | -1.92 |
| 2026-08-21 | `ARCT` | 130 | — | $11.13 | +0.00 | $13.45 | +301.60 | +301.60 | +0.00 | +301.60 |
| 2026-08-21 | `AUTL` | 470 | — | $2.47 | +0.00 | $2.41 | -28.20 | -28.20 | +0.00 | -28.20 |
| 2026-08-21 | `CRDL` | 451 | — | $1.93 | +0.00 | $1.86 | -31.57 | -31.57 | +0.00 | -31.57 |
| 2026-08-21 | `CRSP` | 9 | — | $59.72 | +0.00 | $59.50 | -1.98 | -1.98 | +0.00 | -1.98 |
| 2026-08-21 | `CYPH` | 219 | — | $1.32 | +0.00 | $1.42 | +21.90 | +21.90 | +0.00 | +21.90 |
| 2026-08-24 | `AU` | 19 | $121.22 | $120.51 | -13.49 | — | +0.00 | -13.49 | +20.52 | — |
| 2026-08-24 | `AUPH` | 118 | $16.65 | $16.57 | -9.44 | — | +0.00 | -9.44 | -74.34 | — |
| 2026-08-24 | `AEM` | 8 | $216.06 | $217.03 | +7.76 | — | +0.00 | +7.76 | +5.84 | — |
| 2026-08-24 | `ARCT` | 130 | $13.45 | $13.33 | -15.60 | — | +0.00 | -15.60 | +286.00 | — |
| 2026-08-24 | `AUTL` | 470 | $2.41 | $2.40 | -4.70 | — | +0.00 | -4.70 | -32.90 | — |
| 2026-08-24 | `CRDL` | 451 | $1.86 | $1.88 | +9.02 | — | +0.00 | +9.02 | -22.55 | — |
| 2026-08-24 | `CRSP` | 9 | $59.50 | $58.75 | -6.75 | — | +0.00 | -6.75 | -8.73 | — |
| 2026-08-24 | `CYPH` | 219 | $1.42 | $1.83 | +89.79 | — | +0.00 | +89.79 | +111.69 | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | `CABA` | 686 | — | $3.46 | +0.00 | $3.47 | +6.86 | +6.86 | +0.00 | +6.86 |
| 2026-09-04 | `ALEC` | 824 | — | $2.52 | +0.00 | $2.46 | -49.44 | -49.44 | +0.00 | -49.44 |
| 2026-09-04 | `BHC` | 265 | — | $6.71 | +0.00 | $6.56 | -39.75 | -39.75 | +0.00 | -39.75 |
| 2026-09-04 | `BMEA` | 780 | — | $1.90 | +0.00 | $2.03 | +101.40 | +101.40 | +0.00 | +101.40 |
| 2026-09-04 | `OABI` | 248 | — | $4.78 | +0.00 | $4.33 | -111.60 | -111.60 | +0.00 | -111.60 |
| 2026-09-04 | `OPK` | 559 | — | $1.59 | +0.00 | $1.64 | +27.95 | +27.95 | +0.00 | +27.95 |
| 2026-09-04 | `VIR` | 52 | — | $11.31 | +0.00 | $11.38 | +3.90 | +3.90 | +0.00 | +3.90 |
| 2026-09-04 | `ATRC` | 5 | — | $52.03 | +0.00 | $51.52 | -2.55 | -2.55 | +0.00 | -2.55 |
| 2026-09-08 | `CABA` | 686 | $3.47 | $3.43 | -27.44 | — | +0.00 | -27.44 | -20.58 | — |
| 2026-09-08 | `ALEC` | 824 | $2.46 | $2.38 | -65.92 | — | +0.00 | -65.92 | -115.36 | — |
| 2026-09-08 | `BHC` | 265 | $6.56 | $6.57 | +2.65 | — | +0.00 | +2.65 | -37.10 | — |
| 2026-09-08 | `BMEA` | 780 | $2.03 | $2.00 | -23.40 | — | +0.00 | -23.40 | +78.00 | — |
| 2026-09-08 | `OABI` | 248 | $4.33 | $4.30 | -7.44 | — | +0.00 | -7.44 | -119.04 | — |
| 2026-09-08 | `OPK` | 559 | $1.64 | $1.63 | -5.59 | — | +0.00 | -5.59 | +22.36 | — |
| 2026-09-08 | `VIR` | 52 | $11.38 | $11.22 | -8.58 | — | +0.00 | -8.58 | -4.68 | — |
| 2026-09-08 | `ATRC` | 5 | $51.52 | $54.31 | +13.95 | — | +0.00 | +13.95 | +11.40 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +217.77 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $184.92 | $10,198.31 | AG×108, BHP×21, CDE×80, HDSN×240, IAG×56, KGC×28, NFGC×317, WPM×1 |
| 2026-08-21 | +3.25 | $184.92 | AG×108, BHP×21, CDE×80, HDSN×240, IAG×56, KGC×28, NFGC×317, WPM×1 | $10,469.45 | +271.14 | +228.94 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $90.15 | $10,653.05 | AU×19, AUPH×118, AEM×8, ARCT×130, AUTL×470, CRDL×451, CRSP×9, CYPH×219 |
| 2026-08-24 | -5.17 | $90.15 | AU×19, AUPH×118, AEM×8, ARCT×130, AUTL×470, CRDL×451, CRSP×9, CYPH×219 | $10,709.64 | +56.59 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,683.77 | $10,683.77 | — |
| 2026-08-25 | +1.80 | $10,683.77 | — | $10,683.77 | +0.00 | +0.00 | — | — | $10,683.77 | $10,683.77 | — |
| 2026-08-26 | +2.02 | $10,683.77 | — | $10,683.77 | +0.00 | +0.00 | — | — | $10,683.77 | $10,683.77 | — |
| 2026-08-27 | — | $10,683.77 | — | $10,683.77 | +0.00 | +0.00 | — | — | $10,683.77 | $10,683.77 | — |
| 2026-08-28 | +0.75 | $10,683.77 | — | $10,683.77 | +0.00 | +0.00 | — | — | $10,683.77 | $10,683.77 | — |
| 2026-08-31 | -5.85 | $10,683.77 | — | $10,683.77 | +0.00 | +0.00 | — | — | $10,683.77 | $10,683.77 | — |
| 2026-09-01 | -6.30 | $10,683.77 | — | $10,683.77 | +0.00 | +0.00 | — | — | $10,683.77 | $10,683.77 | — |
| 2026-09-02 | -3.83 | $10,683.77 | — | $10,683.77 | +0.00 | +0.00 | — | — | $10,683.77 | $10,683.77 | — |
| 2026-09-03 | -0.90 | $10,683.77 | — | $10,683.77 | +0.00 | +0.00 | — | — | $10,683.77 | $10,683.77 | — |
| 2026-09-04 | +2.25 | $10,683.77 | — | $10,683.77 | +0.00 | -63.23 | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | — | $3.54 | $10,573.02 | CABA×686, ALEC×824, BHC×265, BMEA×780, OABI×248, OPK×559, VIR×52, ATRC×5 |
| 2026-09-08 | -11.47 | $3.54 | CABA×686, ALEC×824, BHC×265, BMEA×780, OABI×248, OPK×559, VIR×52, ATRC×5 | $10,451.25 | -121.77 | +0.00 | — | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | $10,403.05 | $10,403.05 | — |

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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 108 | $20.55 | $2.31 | — | $7,778.29 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $2222.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 21 | $91.01 | $2.05 | — | $5,865.02 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1944.44 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 80 | $20.65 | $2.23 | — | $4,210.79 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1666.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 240 | $5.77 | $3.10 | — | $2,822.90 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1388.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 56 | $19.63 | $2.16 | — | $1,721.46 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1111.11 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 28 | $29.63 | $2.07 | — | $889.75 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $833.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 317 | $1.75 | $4.09 | — | $330.91 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $555.56 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 1 | $144.54 | $1.45 | — | $184.92 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $277.78 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $184.92 | ▲ close $10,198.31 vs 09:30 $10,000.00 (session +217.77) | 16:00 close · cash $184.92 · equity $10,198.31 vs 09:30 $10,000.00 (+198.31; session marks +217.77) · 8 name(s) marked open→close (per-name table). AG×108 09:30 $20.55 → close $21.19 +69.12; BHP×21 09:30 $91.01 → close $93.63 +55.02; CDE×80 09:30 $20.65 → close $21.11 +36.80; HDSN×240 09:30 $5.77 → close $5.57 -48.00; IAG×56 09:30 $19.63 → close $20.50 +48.72; KGC×28 09:30 $29.63 → close $31.43 +50.40; NFGC×317 09:30 $1.75 → close $1.75 +0.00; WPM×1 09:30 $144.54 → close $150.25 +5.71 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $184.92 | ▲ 09:30 equity $10,469.45 vs yday $10,198.31 (+271.14) | 09:30 open · cash $184.92 (unchanged overnight, no fees) · equity $10,469.45 vs prior close $10,198.31 (+271.14) · 8 name(s) re-marked at the open (per-name table). AG×108 yday $21.19 → 09:30 $21.90 +76.68; BHP×21 yday $93.63 → 09:30 $95.72 +43.89; CDE×80 yday $21.11 → 09:30 $21.75 +51.20; HDSN×240 yday $5.57 → 09:30 $5.67 +24.00; IAG×56 yday $20.50 → 09:30 $21.17 +37.52; KGC×28 yday $31.43 → 09:30 $32.17 +20.72; NFGC×317 yday $1.75 → 09:30 $1.79 +12.68; WPM×1 yday $150.25 → 09:30 $154.70 +4.45 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 108 | $21.90 | $2.35 | $+141.14 | $2,547.77 | ▲ +141.14 after sell → book $10,467.10; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 21 | $95.72 | $2.08 | $+94.78 | $4,555.81 | ▲ +94.78 after sell → book $10,465.02; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 80 | $21.75 | $2.26 | $+83.51 | $6,293.55 | ▲ +83.51 after sell → book $10,462.76; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 240 | $5.67 | $3.15 | $-30.24 | $7,651.20 | ▼ -30.24 after sell → book $10,459.61; vs 09:30 mark -3.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 56 | $21.17 | $2.18 | $+81.90 | $8,834.55 | ▲ +81.90 after sell → book $10,457.44; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 28 | $32.17 | $2.09 | $+66.95 | $9,733.21 | ▲ +66.95 after sell → book $10,455.34; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 317 | $1.79 | $4.15 | $+4.44 | $10,296.49 | ▲ +4.44 after sell → book $10,451.19; vs 09:30 mark -4.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 1 | $154.70 | $1.57 | $+7.14 | $10,449.62 | ▲ +7.14 after sell → book $10,449.62; vs 09:30 mark -1.57 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 19 | $119.43 | $2.05 | — | $8,178.40 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+21.1; leftover $2322.14 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 118 | $17.20 | $2.34 | — | $6,146.46 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $2031.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 8 | $216.30 | $2.01 | — | $4,414.04 | — | rank-weighted leftover; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $1741.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 130 | $11.13 | $2.38 | — | $2,964.76 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $1451.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 470 | $2.47 | $6.06 | — | $1,797.80 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $1161.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 451 | $1.93 | $5.82 | — | $921.55 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $870.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 9 | $59.72 | $2.02 | — | $382.06 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $580.53 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 219 | $1.32 | $2.83 | — | $90.15 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $290.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.15 | ▲ close $10,653.05 vs 09:30 $10,469.45 (session +228.94) | 16:00 close · cash $90.15 · equity $10,653.05 vs 09:30 $10,469.45 (+183.60; session marks +228.94) · 8 name(s) marked open→close (per-name table). AU×19 09:30 $119.43 → close $121.22 +34.01; AUPH×118 09:30 $17.20 → close $16.65 -64.90; AEM×8 09:30 $216.30 → close $216.06 -1.92; ARCT×130 09:30 $11.13 → close $13.45 +301.60; AUTL×470 09:30 $2.47 → close $2.41 -28.20; CRDL×451 09:30 $1.93 → close $1.86 -31.57; CRSP×9 09:30 $59.72 → close $59.50 -1.98; CYPH×219 09:30 $1.32 → close $1.42 +21.90 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.15 | ▲ 09:30 equity $10,709.64 vs yday $10,653.05 (+56.59) | 09:30 open · cash $90.15 (unchanged overnight, no fees) · equity $10,709.64 vs prior close $10,653.05 (+56.59) · 8 name(s) re-marked at the open (per-name table). AU×19 yday $121.22 → 09:30 $120.51 -13.49; AUPH×118 yday $16.65 → 09:30 $16.57 -9.44; AEM×8 yday $216.06 → 09:30 $217.03 +7.76; ARCT×130 yday $13.45 → 09:30 $13.33 -15.60; AUTL×470 yday $2.41 → 09:30 $2.40 -4.70; CRDL×451 yday $1.86 → 09:30 $1.88 +9.02; CRSP×9 yday $59.50 → 09:30 $58.75 -6.75; CYPH×219 yday $1.42 → 09:30 $1.83 +89.79 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 19 | $120.51 | $2.08 | $+16.40 | $2,377.77 | ▲ +16.40 after sell → book $10,707.57; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 118 | $16.57 | $2.38 | $-79.06 | $4,330.65 | ▼ -79.06 after sell → book $10,705.19; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 8 | $217.03 | $2.04 | $+1.79 | $6,064.85 | ▲ +1.79 after sell → book $10,703.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 130 | $13.33 | $2.42 | $+281.20 | $7,795.33 | ▲ +281.20 after sell → book $10,700.73; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 470 | $2.40 | $6.15 | $-45.11 | $8,917.18 | ▼ -45.11 after sell → book $10,694.58; vs 09:30 mark -6.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 451 | $1.88 | $5.90 | $-34.27 | $9,759.16 | ▼ -34.27 after sell → book $10,688.68; vs 09:30 mark -5.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 9 | $58.75 | $2.04 | $-12.78 | $10,285.87 | ▼ -12.78 after sell → book $10,686.64; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 219 | $1.83 | $2.87 | $+105.99 | $10,683.77 | ▲ +105.99 after sell → book $10,683.77; vs 09:30 mark -2.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,709.64 (session +0.00) | 16:00 close · cash $10,683.77 · no lots left · equity $10,683.77. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | 09:30 open · cash $10,683.77 · no holdings · equity $10,683.77 vs prior close $10,683.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | 16:00 close · cash $10,683.77 · no lots left · equity $10,683.77. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | 09:30 open · cash $10,683.77 · no holdings · equity $10,683.77 vs prior close $10,683.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | 16:00 close · cash $10,683.77 · no lots left · equity $10,683.77. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | 09:30 open · cash $10,683.77 · no holdings · equity $10,683.77 vs prior close $10,683.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | 16:00 close · cash $10,683.77 · no lots left · equity $10,683.77. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | 09:30 open · cash $10,683.77 · no holdings · equity $10,683.77 vs prior close $10,683.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | 16:00 close · cash $10,683.77 · no lots left · equity $10,683.77. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | 09:30 open · cash $10,683.77 · no holdings · equity $10,683.77 vs prior close $10,683.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | 16:00 close · cash $10,683.77 · no lots left · equity $10,683.77. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | 09:30 open · cash $10,683.77 · no holdings · equity $10,683.77 vs prior close $10,683.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | 16:00 close · cash $10,683.77 · no lots left · equity $10,683.77. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | 09:30 open · cash $10,683.77 · no holdings · equity $10,683.77 vs prior close $10,683.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | 16:00 close · cash $10,683.77 · no lots left · equity $10,683.77. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | 09:30 open · cash $10,683.77 · no holdings · equity $10,683.77 vs prior close $10,683.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,683.77 | ▲ close $10,683.77 vs 09:30 $10,683.77 (session +0.00) | 16:00 close · cash $10,683.77 · no lots left · equity $10,683.77. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,683.77 | ▲ 09:30 equity $10,683.77 vs yday $10,683.77 (+0.00) | 09:30 open · cash $10,683.77 · no holdings · equity $10,683.77 vs prior close $10,683.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 686 | $3.46 | $8.85 | — | $8,301.36 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $2374.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 824 | $2.52 | $10.63 | — | $6,214.25 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $2077.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 265 | $6.71 | $3.42 | — | $4,432.68 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1780.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 780 | $1.90 | $10.06 | — | $2,940.62 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $1483.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 248 | $4.78 | $3.20 | — | $1,751.98 | — | rank-weighted leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $1187.09 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 559 | $1.59 | $7.21 | — | $855.96 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $890.31 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 52 | $11.31 | $2.15 | — | $265.70 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $593.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 5 | $52.03 | $2.00 | — | $3.54 | — | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+6.5; leftover $296.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.54 | ▼ close $10,573.02 vs 09:30 $10,683.77 (session -63.23) | 16:00 close · cash $3.54 · equity $10,573.02 vs 09:30 $10,683.77 (-110.75; session marks -63.23) · 8 name(s) marked open→close (per-name table). CABA×686 09:30 $3.46 → close $3.47 +6.86; ALEC×824 09:30 $2.52 → close $2.46 -49.44; BHC×265 09:30 $6.71 → close $6.56 -39.75; BMEA×780 09:30 $1.90 → close $2.03 +101.40; OABI×248 09:30 $4.78 → close $4.33 -111.60; OPK×559 09:30 $1.59 → close $1.64 +27.95; VIR×52 09:30 $11.31 → close $11.38 +3.90; ATRC×5 09:30 $52.03 → close $51.52 -2.55 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.54 | ▼ 09:30 equity $10,451.25 vs yday $10,573.02 (-121.77) | 09:30 open · cash $3.54 (unchanged overnight, no fees) · equity $10,451.25 vs prior close $10,573.02 (-121.77) · 8 name(s) re-marked at the open (per-name table). CABA×686 yday $3.47 → 09:30 $3.43 -27.44; ALEC×824 yday $2.46 → 09:30 $2.38 -65.92; BHC×265 yday $6.56 → 09:30 $6.57 +2.65; BMEA×780 yday $2.03 → 09:30 $2.00 -23.40; OABI×248 yday $4.33 → 09:30 $4.30 -7.44; OPK×559 yday $1.64 → 09:30 $1.63 -5.59; VIR×52 yday $11.38 → 09:30 $11.22 -8.58; ATRC×5 yday $51.52 → 09:30 $54.31 +13.95 | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 686 | $3.43 | $8.98 | $-38.41 | $2,347.54 | ▼ -38.41 after sell → book $10,442.27; vs 09:30 mark -8.98 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 824 | $2.38 | $10.78 | $-136.77 | $4,297.88 | ▼ -136.77 after sell → book $10,431.49; vs 09:30 mark -10.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 265 | $6.57 | $3.48 | $-43.99 | $6,035.45 | ▼ -43.99 after sell → book $10,428.01; vs 09:30 mark -3.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 780 | $2.00 | $10.20 | $+57.73 | $7,585.25 | ▲ +57.73 after sell → book $10,417.81; vs 09:30 mark -10.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 248 | $4.30 | $3.25 | $-125.49 | $8,648.40 | ▼ -125.49 after sell → book $10,414.56; vs 09:30 mark -3.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 559 | $1.63 | $7.31 | $+7.83 | $9,552.25 | ▲ +7.83 after sell → book $10,407.24; vs 09:30 mark -7.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 52 | $11.22 | $2.17 | $-8.99 | $10,133.53 | ▼ -8.99 after sell → book $10,405.08; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 5 | $54.31 | $2.02 | $+7.37 | $10,403.05 | ▲ +7.37 after sell → book $10,403.05; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,403.05 | ▲ close $10,403.05 vs 09:30 $10,451.25 (session +0.00) | 16:00 close · cash $10,403.05 · no lots left · equity $10,403.05. | — |
