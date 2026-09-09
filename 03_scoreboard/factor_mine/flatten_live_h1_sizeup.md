# Factor mine action — `flatten_live_h1_sizeup`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `sizeup` · S≥+5: 1.35× leftover

Cash book **+8.22%** ($10,822) · signal-only (no cash/fees) was +4.45%. Starts YES **7/18**. Fills 48 · skips 0 · realized $+821.58.

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
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- On a strong morning (S ≥ +5), spend 1.35× leftover — still capped by cash.
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
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `sizeup`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,821.57.

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
| 2026-08-20 | `AG` | 60 | — | $20.55 | +0.00 | $21.19 | +38.40 | +38.40 | +0.00 | +38.40 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 60 | — | $20.65 | +0.00 | $21.11 | +27.60 | +27.60 | +0.00 | +27.60 |
| 2026-08-20 | `HDSN` | 216 | — | $5.77 | +0.00 | $5.57 | -43.20 | -43.20 | +0.00 | -43.20 |
| 2026-08-20 | `IAG` | 63 | — | $19.63 | +0.00 | $20.50 | +54.81 | +54.81 | +0.00 | +54.81 |
| 2026-08-20 | `KGC` | 42 | — | $29.63 | +0.00 | $31.43 | +75.60 | +75.60 | +0.00 | +75.60 |
| 2026-08-20 | `NFGC` | 714 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 60 | $21.19 | $21.90 | +42.60 | — | +0.00 | +42.60 | +81.00 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 60 | $21.11 | $21.75 | +38.40 | — | +0.00 | +38.40 | +66.00 | — |
| 2026-08-21 | `HDSN` | 216 | $5.57 | $5.67 | +21.60 | — | +0.00 | +21.60 | -21.60 | — |
| 2026-08-21 | `IAG` | 63 | $20.50 | $21.17 | +42.21 | — | +0.00 | +42.21 | +97.02 | — |
| 2026-08-21 | `KGC` | 42 | $31.43 | $32.17 | +31.08 | — | +0.00 | +31.08 | +106.68 | — |
| 2026-08-21 | `NFGC` | 714 | $1.75 | $1.79 | +28.56 | — | +0.00 | +28.56 | +28.56 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 75 | — | $17.20 | +0.00 | $16.65 | -41.25 | -41.25 | +0.00 | -41.25 |
| 2026-08-21 | `AEM` | 6 | — | $216.30 | +0.00 | $216.06 | -1.44 | -1.44 | +0.00 | -1.44 |
| 2026-08-21 | `ARCT` | 117 | — | $11.13 | +0.00 | $13.45 | +271.44 | +271.44 | +0.00 | +271.44 |
| 2026-08-21 | `AUTL` | 528 | — | $2.47 | +0.00 | $2.41 | -31.68 | -31.68 | +0.00 | -31.68 |
| 2026-08-21 | `CRDL` | 676 | — | $1.93 | +0.00 | $1.86 | -47.32 | -47.32 | +0.00 | -47.32 |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `CYPH` | 989 | — | $1.32 | +0.00 | $1.42 | +98.90 | +98.90 | +0.00 | +98.90 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 75 | $16.65 | $16.57 | -6.00 | — | +0.00 | -6.00 | -47.25 | — |
| 2026-08-24 | `AEM` | 6 | $216.06 | $217.03 | +5.82 | — | +0.00 | +5.82 | +4.38 | — |
| 2026-08-24 | `ARCT` | 117 | $13.45 | $13.33 | -14.04 | — | +0.00 | -14.04 | +257.40 | — |
| 2026-08-24 | `AUTL` | 528 | $2.41 | $2.40 | -5.28 | — | +0.00 | -5.28 | -36.96 | — |
| 2026-08-24 | `CRDL` | 676 | $1.86 | $1.88 | +13.52 | — | +0.00 | +13.52 | -33.80 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | — | +0.00 | -15.75 | -20.37 | — |
| 2026-08-24 | `CYPH` | 989 | $1.42 | $1.83 | +405.49 | — | +0.00 | +405.49 | +504.39 | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | `CABA` | 397 | — | $3.46 | +0.00 | $3.47 | +3.97 | +3.97 | +0.00 | +3.97 |
| 2026-09-04 | `ALEC` | 546 | — | $2.52 | +0.00 | $2.46 | -32.76 | -32.76 | +0.00 | -32.76 |
| 2026-09-04 | `BHC` | 205 | — | $6.71 | +0.00 | $6.56 | -30.75 | -30.75 | +0.00 | -30.75 |
| 2026-09-04 | `BMEA` | 724 | — | $1.90 | +0.00 | $2.03 | +94.12 | +94.12 | +0.00 | +94.12 |
| 2026-09-04 | `OABI` | 287 | — | $4.78 | +0.00 | $4.33 | -129.15 | -129.15 | +0.00 | -129.15 |
| 2026-09-04 | `OPK` | 865 | — | $1.59 | +0.00 | $1.64 | +43.25 | +43.25 | +0.00 | +43.25 |
| 2026-09-04 | `VIR` | 121 | — | $11.31 | +0.00 | $11.38 | +9.07 | +9.07 | +0.00 | +9.07 |
| 2026-09-04 | `ATRC` | 25 | — | $52.03 | +0.00 | $51.52 | -12.75 | -12.75 | +0.00 | -12.75 |
| 2026-09-08 | `CABA` | 397 | $3.47 | $3.43 | -15.88 | — | +0.00 | -15.88 | -11.91 | — |
| 2026-09-08 | `ALEC` | 546 | $2.46 | $2.38 | -43.68 | — | +0.00 | -43.68 | -76.44 | — |
| 2026-09-08 | `BHC` | 205 | $6.56 | $6.57 | +2.05 | — | +0.00 | +2.05 | -28.70 | — |
| 2026-09-08 | `BMEA` | 724 | $2.03 | $2.00 | -21.72 | — | +0.00 | -21.72 | +72.40 | — |
| 2026-09-08 | `OABI` | 287 | $4.33 | $4.30 | -8.61 | — | +0.00 | -8.61 | -137.76 | — |
| 2026-09-08 | `OPK` | 865 | $1.64 | $1.63 | -8.65 | — | +0.00 | -8.65 | +34.60 | — |
| 2026-09-08 | `VIR` | 121 | $11.38 | $11.22 | -19.96 | — | +0.00 | -19.96 | -10.89 | — |
| 2026-09-08 | `ATRC` | 25 | $51.52 | $54.31 | +69.75 | — | +0.00 | +69.75 | +57.00 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +232.95 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $186.91 | $10,208.28 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 |
| 2026-08-21 | +3.25 | $186.91 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 | $10,475.50 | +267.22 | +261.93 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $158.85 | $10,673.53 | AU×10, AUPH×75, AEM×6, ARCT×117, AUTL×528, CRDL×676, CRSP×21, CYPH×989 |
| 2026-08-24 | -5.17 | $158.85 | AU×10, AUPH×75, AEM×6, ARCT×117, AUTL×528, CRDL×676, CRSP×21, CYPH×989 | $11,050.19 | +376.66 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $11,010.75 | $11,010.75 | — |
| 2026-08-25 | +1.80 | $11,010.75 | — | $11,010.75 | -0.00 | +0.00 | — | — | $11,010.75 | $11,010.75 | — |
| 2026-08-26 | +2.02 | $11,010.75 | — | $11,010.75 | -0.00 | +0.00 | — | — | $11,010.75 | $11,010.75 | — |
| 2026-08-27 | — | $11,010.75 | — | $11,010.75 | -0.00 | +0.00 | — | — | $11,010.75 | $11,010.75 | — |
| 2026-08-28 | +0.75 | $11,010.75 | — | $11,010.75 | -0.00 | +0.00 | — | — | $11,010.75 | $11,010.75 | — |
| 2026-08-31 | -5.85 | $11,010.75 | — | $11,010.75 | -0.00 | +0.00 | — | — | $11,010.75 | $11,010.75 | — |
| 2026-09-01 | -6.30 | $11,010.75 | — | $11,010.75 | -0.00 | +0.00 | — | — | $11,010.75 | $11,010.75 | — |
| 2026-09-02 | -3.83 | $11,010.75 | — | $11,010.75 | -0.00 | +0.00 | — | — | $11,010.75 | $11,010.75 | — |
| 2026-09-03 | -0.90 | $11,010.75 | — | $11,010.75 | -0.00 | +0.00 | — | — | $11,010.75 | $11,010.75 | — |
| 2026-09-04 | +2.25 | $11,010.75 | — | $11,010.75 | -0.00 | -55.00 | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | — | $50.16 | $10,912.32 | CABA×397, ALEC×546, BHC×205, BMEA×724, OABI×287, OPK×865, VIR×121, ATRC×25 |
| 2026-09-08 | -11.47 | $50.16 | CABA×397, ALEC×546, BHC×205, BMEA×724, OABI×287, OPK×865, VIR×121, ATRC×25 | $10,865.62 | -46.70 | +0.00 | — | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | $10,821.57 | $10,821.57 | — |

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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | — | S≥+5: 1.35× leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.91 | ▲ close $10,208.28 vs 09:30 $10,000.00 (session +232.95) | 16:00 close · cash $186.91 · equity $10,208.28 vs 09:30 $10,000.00 (+208.28; session marks +232.95) · 8 name(s) marked open→close (per-name table). AG×60 09:30 $20.55 → close $21.19 +38.40; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×60 09:30 $20.65 → close $21.11 +27.60; HDSN×216 09:30 $5.77 → close $5.57 -43.20; IAG×63 09:30 $19.63 → close $20.50 +54.81; KGC×42 09:30 $29.63 → close $31.43 +75.60; NFGC×714 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.91 | ▲ 09:30 equity $10,475.50 vs yday $10,208.28 (+267.22) | 09:30 open · cash $186.91 (unchanged overnight, no fees) · equity $10,475.50 vs prior close $10,208.28 (+267.22) · 8 name(s) re-marked at the open (per-name table). AG×60 yday $21.19 → 09:30 $21.90 +42.60; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×60 yday $21.11 → 09:30 $21.75 +38.40; HDSN×216 yday $5.57 → 09:30 $5.67 +21.60; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×714 yday $1.75 → 09:30 $1.79 +28.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 60 | $21.90 | $2.19 | $+76.64 | $1,498.71 | ▲ +76.64 after sell → book $10,473.30; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,741.03 | ▲ +57.15 after sell → book $10,471.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 60 | $21.75 | $2.19 | $+61.64 | $4,043.84 | ▲ +61.64 after sell → book $10,469.07; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 216 | $5.67 | $2.83 | $-27.22 | $5,265.72 | ▼ -27.22 after sell → book $10,466.23; vs 09:30 mark -2.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $6,597.23 | ▲ +92.64 after sell → book $10,464.03; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $7,946.24 | ▲ +102.43 after sell → book $10,461.90; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 714 | $1.79 | $9.34 | $+10.01 | $9,214.96 | ▲ +10.01 after sell → book $10,452.56; vs 09:30 mark -9.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,450.52 | ▲ +77.23 after sell → book $10,450.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,254.20 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+21.1; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 75 | $17.20 | $2.21 | — | $7,961.99 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,662.18 | — | S≥+5: 1.35× leftover; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,357.63 | — | S≥+5: 1.35× leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 528 | $2.47 | $6.81 | — | $4,046.66 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 676 | $1.93 | $8.72 | — | $2,733.26 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,477.08 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 989 | $1.32 | $12.76 | — | $158.85 | — | S≥+5: 1.35× leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.85 | ▲ close $10,673.53 vs 09:30 $10,475.50 (session +261.93) | 16:00 close · cash $158.85 · equity $10,673.53 vs 09:30 $10,475.50 (+198.03; session marks +261.93) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×75 09:30 $17.20 → close $16.65 -41.25; AEM×6 09:30 $216.30 → close $216.06 -1.44; ARCT×117 09:30 $11.13 → close $13.45 +271.44; AUTL×528 09:30 $2.47 → close $2.41 -31.68; CRDL×676 09:30 $1.93 → close $1.86 -47.32; CRSP×21 09:30 $59.72 → close $59.50 -4.62; CYPH×989 09:30 $1.32 → close $1.42 +98.90 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.85 | ▲ 09:30 equity $11,050.19 vs yday $10,673.53 (+376.66) | 09:30 open · cash $158.85 (unchanged overnight, no fees) · equity $11,050.19 vs prior close $10,673.53 (+376.66) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×75 yday $16.65 → 09:30 $16.57 -6.00; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×117 yday $13.45 → 09:30 $13.33 -14.04; AUTL×528 yday $2.41 → 09:30 $2.40 -5.28; CRDL×676 yday $1.86 → 09:30 $1.88 +13.52; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; CYPH×989 yday $1.42 → 09:30 $1.83 +405.49 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,361.91 | ▲ +6.74 after sell → book $11,048.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 75 | $16.57 | $2.24 | $-51.70 | $2,602.42 | ▼ -51.70 after sell → book $11,045.91; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,902.57 | ▲ +0.34 after sell → book $11,043.88; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 117 | $13.33 | $2.37 | $+252.69 | $5,459.81 | ▲ +252.69 after sell → book $11,041.51; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 528 | $2.40 | $6.91 | $-50.68 | $6,720.10 | ▼ -50.68 after sell → book $11,034.60; vs 09:30 mark -6.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 676 | $1.88 | $8.84 | $-51.36 | $7,982.14 | ▼ -51.36 after sell → book $11,025.76; vs 09:30 mark -8.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $9,213.81 | ▼ -24.50 after sell → book $11,023.68; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 989 | $1.83 | $12.94 | $+478.70 | $11,010.75 | ▲ +478.70 after sell → book $11,010.75; vs 09:30 mark -12.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,010.75 | ▲ close $11,010.75 vs 09:30 $11,050.19 (session +0.00) | 16:00 close · cash $11,010.75 · no lots left · equity $11,010.75. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,010.75 | ▲ 09:30 equity $11,010.75 vs yday $11,010.75 (-0.00) | 09:30 open · cash $11,010.75 · no holdings · equity $11,010.75 vs prior close $11,010.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,010.75 | ▲ close $11,010.75 vs 09:30 $11,010.75 (session +0.00) | 16:00 close · cash $11,010.75 · no lots left · equity $11,010.75. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,010.75 | ▲ 09:30 equity $11,010.75 vs yday $11,010.75 (-0.00) | 09:30 open · cash $11,010.75 · no holdings · equity $11,010.75 vs prior close $11,010.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,010.75 | ▲ close $11,010.75 vs 09:30 $11,010.75 (session +0.00) | 16:00 close · cash $11,010.75 · no lots left · equity $11,010.75. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,010.75 | ▲ 09:30 equity $11,010.75 vs yday $11,010.75 (-0.00) | 09:30 open · cash $11,010.75 · no holdings · equity $11,010.75 vs prior close $11,010.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,010.75 | ▲ close $11,010.75 vs 09:30 $11,010.75 (session +0.00) | 16:00 close · cash $11,010.75 · no lots left · equity $11,010.75. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,010.75 | ▲ 09:30 equity $11,010.75 vs yday $11,010.75 (-0.00) | 09:30 open · cash $11,010.75 · no holdings · equity $11,010.75 vs prior close $11,010.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,010.75 | ▲ close $11,010.75 vs 09:30 $11,010.75 (session +0.00) | 16:00 close · cash $11,010.75 · no lots left · equity $11,010.75. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,010.75 | ▲ 09:30 equity $11,010.75 vs yday $11,010.75 (-0.00) | 09:30 open · cash $11,010.75 · no holdings · equity $11,010.75 vs prior close $11,010.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,010.75 | ▲ close $11,010.75 vs 09:30 $11,010.75 (session +0.00) | 16:00 close · cash $11,010.75 · no lots left · equity $11,010.75. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,010.75 | ▲ 09:30 equity $11,010.75 vs yday $11,010.75 (-0.00) | 09:30 open · cash $11,010.75 · no holdings · equity $11,010.75 vs prior close $11,010.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,010.75 | ▲ close $11,010.75 vs 09:30 $11,010.75 (session +0.00) | 16:00 close · cash $11,010.75 · no lots left · equity $11,010.75. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,010.75 | ▲ 09:30 equity $11,010.75 vs yday $11,010.75 (-0.00) | 09:30 open · cash $11,010.75 · no holdings · equity $11,010.75 vs prior close $11,010.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,010.75 | ▲ close $11,010.75 vs 09:30 $11,010.75 (session +0.00) | 16:00 close · cash $11,010.75 · no lots left · equity $11,010.75. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,010.75 | ▲ 09:30 equity $11,010.75 vs yday $11,010.75 (-0.00) | 09:30 open · cash $11,010.75 · no holdings · equity $11,010.75 vs prior close $11,010.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,010.75 | ▲ close $11,010.75 vs 09:30 $11,010.75 (session +0.00) | 16:00 close · cash $11,010.75 · no lots left · equity $11,010.75. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,010.75 | ▲ 09:30 equity $11,010.75 vs yday $11,010.75 (-0.00) | 09:30 open · cash $11,010.75 · no holdings · equity $11,010.75 vs prior close $11,010.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 397 | $3.46 | $5.12 | — | $9,632.00 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $1376.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 546 | $2.52 | $7.04 | — | $8,249.04 | — | S≥+5: 1.35× leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $1376.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 205 | $6.71 | $2.64 | — | $6,870.85 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1376.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 724 | $1.90 | $9.34 | — | $5,485.91 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $1376.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 287 | $4.78 | $3.70 | — | $4,110.34 | — | S≥+5: 1.35× leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $1376.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 865 | $1.59 | $11.16 | — | $2,723.84 | — | S≥+5: 1.35× leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1376.34 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 121 | $11.31 | $2.35 | — | $1,352.97 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $1376.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 25 | $52.03 | $2.06 | — | $50.16 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+6.5; leftover $1376.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.16 | ▼ close $10,912.32 vs 09:30 $11,010.75 (session -55.00) | 16:00 close · cash $50.16 · equity $10,912.32 vs 09:30 $11,010.75 (-98.43; session marks -55.00) · 8 name(s) marked open→close (per-name table). CABA×397 09:30 $3.46 → close $3.47 +3.97; ALEC×546 09:30 $2.52 → close $2.46 -32.76; BHC×205 09:30 $6.71 → close $6.56 -30.75; BMEA×724 09:30 $1.90 → close $2.03 +94.12; OABI×287 09:30 $4.78 → close $4.33 -129.15; OPK×865 09:30 $1.59 → close $1.64 +43.25; VIR×121 09:30 $11.31 → close $11.38 +9.07; ATRC×25 09:30 $52.03 → close $51.52 -12.75 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.16 | ▼ 09:30 equity $10,865.62 vs yday $10,912.32 (-46.70) | 09:30 open · cash $50.16 (unchanged overnight, no fees) · equity $10,865.62 vs prior close $10,912.32 (-46.70) · 8 name(s) re-marked at the open (per-name table). CABA×397 yday $3.47 → 09:30 $3.43 -15.88; ALEC×546 yday $2.46 → 09:30 $2.38 -43.68; BHC×205 yday $6.56 → 09:30 $6.57 +2.05; BMEA×724 yday $2.03 → 09:30 $2.00 -21.72; OABI×287 yday $4.33 → 09:30 $4.30 -8.61; OPK×865 yday $1.64 → 09:30 $1.63 -8.65; VIR×121 yday $11.38 → 09:30 $11.22 -19.96; ATRC×25 yday $51.52 → 09:30 $54.31 +69.75 | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 397 | $3.43 | $5.20 | $-22.23 | $1,406.67 | ▼ -22.23 after sell → book $10,860.42; vs 09:30 mark -5.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 546 | $2.38 | $7.14 | $-90.63 | $2,699.01 | ▼ -90.63 after sell → book $10,853.28; vs 09:30 mark -7.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 205 | $6.57 | $2.69 | $-34.03 | $4,043.17 | ▼ -34.03 after sell → book $10,850.59; vs 09:30 mark -2.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 724 | $2.00 | $9.47 | $+53.59 | $5,481.69 | ▲ +53.59 after sell → book $10,841.11; vs 09:30 mark -9.48 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 287 | $4.30 | $3.76 | $-145.22 | $6,712.03 | ▼ -145.22 after sell → book $10,837.35; vs 09:30 mark -3.76 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 865 | $1.63 | $11.31 | $+12.13 | $8,110.67 | ▲ +12.13 after sell → book $10,826.04; vs 09:30 mark -11.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 121 | $11.22 | $2.38 | $-15.63 | $9,465.91 | ▼ -15.63 after sell → book $10,823.66; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+52.85 | $10,821.57 | ▲ +52.85 after sell → book $10,821.57; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,821.57 | ▲ close $10,821.57 vs 09:30 $10,865.62 (session +0.00) | 16:00 close · cash $10,821.57 · no lots left · equity $10,821.57. | — |
