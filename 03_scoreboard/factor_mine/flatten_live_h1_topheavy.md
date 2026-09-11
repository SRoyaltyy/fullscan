# Factor mine action — `flatten_live_h1_topheavy`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `topheavy` · sell `list` · S-boost `none` · 40% to #1, rest split

Cash book **+7.19%** ($10,719) · signal-only (no cash/fees) was +4.45%. Starts YES **7/20**. Fills 48 · skips 0 · realized $+718.61.

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
- Give about 40% of leftover cash to the first name; split the rest.
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
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,718.60.

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
| 2026-08-20 | `AG` | 194 | — | $20.55 | +0.00 | $21.19 | +124.16 | +124.16 | +0.00 | +124.16 |
| 2026-08-20 | `BHP` | 9 | — | $91.01 | +0.00 | $93.63 | +23.58 | +23.58 | +0.00 | +23.58 |
| 2026-08-20 | `CDE` | 41 | — | $20.65 | +0.00 | $21.11 | +18.86 | +18.86 | +0.00 | +18.86 |
| 2026-08-20 | `HDSN` | 148 | — | $5.77 | +0.00 | $5.57 | -29.60 | -29.60 | +0.00 | -29.60 |
| 2026-08-20 | `IAG` | 43 | — | $19.63 | +0.00 | $20.50 | +37.41 | +37.41 | +0.00 | +37.41 |
| 2026-08-20 | `KGC` | 28 | — | $29.63 | +0.00 | $31.43 | +50.40 | +50.40 | +0.00 | +50.40 |
| 2026-08-20 | `NFGC` | 489 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 5 | — | $144.54 | +0.00 | $150.25 | +28.55 | +28.55 | +0.00 | +28.55 |
| 2026-08-21 | `AG` | 194 | $21.19 | $21.90 | +137.74 | — | +0.00 | +137.74 | +261.90 | — |
| 2026-08-21 | `BHP` | 9 | $93.63 | $95.72 | +18.81 | — | +0.00 | +18.81 | +42.39 | — |
| 2026-08-21 | `CDE` | 41 | $21.11 | $21.75 | +26.24 | — | +0.00 | +26.24 | +45.10 | — |
| 2026-08-21 | `HDSN` | 148 | $5.57 | $5.67 | +14.80 | — | +0.00 | +14.80 | -14.80 | — |
| 2026-08-21 | `IAG` | 43 | $20.50 | $21.17 | +28.81 | — | +0.00 | +28.81 | +66.22 | — |
| 2026-08-21 | `KGC` | 28 | $31.43 | $32.17 | +20.72 | — | +0.00 | +20.72 | +71.12 | — |
| 2026-08-21 | `NFGC` | 489 | $1.75 | $1.79 | +19.56 | — | +0.00 | +19.56 | +19.56 | — |
| 2026-08-21 | `WPM` | 5 | $150.25 | $154.70 | +22.25 | — | +0.00 | +22.25 | +50.80 | — |
| 2026-08-21 | `AU` | 35 | — | $119.43 | +0.00 | $121.22 | +62.65 | +62.65 | +0.00 | +62.65 |
| 2026-08-21 | `AUPH` | 52 | — | $17.20 | +0.00 | $16.65 | -28.60 | -28.60 | +0.00 | -28.60 |
| 2026-08-21 | `AEM` | 4 | — | $216.30 | +0.00 | $216.06 | -0.96 | -0.96 | +0.00 | -0.96 |
| 2026-08-21 | `ARCT` | 80 | — | $11.13 | +0.00 | $13.45 | +185.60 | +185.60 | +0.00 | +185.60 |
| 2026-08-21 | `AUTL` | 364 | — | $2.47 | +0.00 | $2.41 | -21.84 | -21.84 | +0.00 | -21.84 |
| 2026-08-21 | `CRDL` | 466 | — | $1.93 | +0.00 | $1.86 | -32.62 | -32.62 | +0.00 | -32.62 |
| 2026-08-21 | `CRSP` | 15 | — | $59.72 | +0.00 | $59.50 | -3.30 | -3.30 | +0.00 | -3.30 |
| 2026-08-21 | `CYPH` | 681 | — | $1.32 | +0.00 | $1.42 | +68.10 | +68.10 | +0.00 | +68.10 |
| 2026-08-24 | `AU` | 35 | $121.22 | $120.51 | -24.85 | — | +0.00 | -24.85 | +37.80 | — |
| 2026-08-24 | `AUPH` | 52 | $16.65 | $16.57 | -4.16 | — | +0.00 | -4.16 | -32.76 | — |
| 2026-08-24 | `AEM` | 4 | $216.06 | $217.03 | +3.88 | — | +0.00 | +3.88 | +2.92 | — |
| 2026-08-24 | `ARCT` | 80 | $13.45 | $13.33 | -9.60 | — | +0.00 | -9.60 | +176.00 | — |
| 2026-08-24 | `AUTL` | 364 | $2.41 | $2.40 | -3.64 | — | +0.00 | -3.64 | -25.48 | — |
| 2026-08-24 | `CRDL` | 466 | $1.86 | $1.88 | +9.32 | — | +0.00 | +9.32 | -23.30 | — |
| 2026-08-24 | `CRSP` | 15 | $59.50 | $58.75 | -11.25 | — | +0.00 | -11.25 | -14.55 | — |
| 2026-08-24 | `CYPH` | 681 | $1.42 | $1.83 | +279.21 | — | +0.00 | +279.21 | +347.31 | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | `CABA` | 1260 | — | $3.46 | +0.00 | $3.47 | +12.60 | +12.60 | +0.00 | +12.60 |
| 2026-09-04 | `ALEC` | 370 | — | $2.52 | +0.00 | $2.46 | -22.20 | -22.20 | +0.00 | -22.20 |
| 2026-09-04 | `BHC` | 139 | — | $6.71 | +0.00 | $6.56 | -20.85 | -20.85 | +0.00 | -20.85 |
| 2026-09-04 | `BMEA` | 492 | — | $1.90 | +0.00 | $2.03 | +63.96 | +63.96 | +0.00 | +63.96 |
| 2026-09-04 | `OABI` | 195 | — | $4.78 | +0.00 | $4.33 | -87.75 | -87.75 | +0.00 | -87.75 |
| 2026-09-04 | `OPK` | 587 | — | $1.59 | +0.00 | $1.64 | +29.35 | +29.35 | +0.00 | +29.35 |
| 2026-09-04 | `VIR` | 82 | — | $11.31 | +0.00 | $11.38 | +6.15 | +6.15 | +0.00 | +6.15 |
| 2026-09-04 | `ATRC` | 17 | — | $52.03 | +0.00 | $51.52 | -8.67 | -8.67 | +0.00 | -8.67 |
| 2026-09-08 | `CABA` | 1260 | $3.47 | $3.43 | -50.40 | — | +0.00 | -50.40 | -37.80 | — |
| 2026-09-08 | `ALEC` | 370 | $2.46 | $2.38 | -29.60 | — | +0.00 | -29.60 | -51.80 | — |
| 2026-09-08 | `BHC` | 139 | $6.56 | $6.57 | +1.39 | — | +0.00 | +1.39 | -19.46 | — |
| 2026-09-08 | `BMEA` | 492 | $2.03 | $2.00 | -14.76 | — | +0.00 | -14.76 | +49.20 | — |
| 2026-09-08 | `OABI` | 195 | $4.33 | $4.30 | -5.85 | — | +0.00 | -5.85 | -93.60 | — |
| 2026-09-08 | `OPK` | 587 | $1.64 | $1.63 | -5.87 | — | +0.00 | -5.87 | +23.48 | — |
| 2026-09-08 | `VIR` | 82 | $11.38 | $11.22 | -13.53 | — | +0.00 | -13.53 | -7.38 | — |
| 2026-09-08 | `ATRC` | 17 | $51.52 | $54.31 | +47.43 | — | +0.00 | +47.43 | +38.76 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +253.36 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $219.78 | $10,231.72 | AG×194, BHP×9, CDE×41, HDSN×148, IAG×43, KGC×28, NFGC×489, WPM×5 |
| 2026-08-21 | +3.25 | $219.78 | AG×194, BHP×9, CDE×41, HDSN×148, IAG×43, KGC×28, NFGC×489, WPM×5 | $10,520.65 | +288.93 | +229.03 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $45.48 | $10,697.74 | AU×35, AUPH×52, AEM×4, ARCT×80, AUTL×364, CRDL×466, CRSP×15, CYPH×681 |
| 2026-08-24 | -5.17 | $45.48 | AU×35, AUPH×52, AEM×4, ARCT×80, AUTL×364, CRDL×466, CRSP×15, CYPH×681 | $10,936.65 | +238.91 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,906.25 | $10,906.25 | — |
| 2026-08-25 | +1.80 | $10,906.25 | — | $10,906.25 | -0.00 | +0.00 | — | — | $10,906.25 | $10,906.25 | — |
| 2026-08-26 | +2.02 | $10,906.25 | — | $10,906.25 | -0.00 | +0.00 | — | — | $10,906.25 | $10,906.25 | — |
| 2026-08-27 | — | $10,906.25 | — | $10,906.25 | -0.00 | +0.00 | — | — | $10,906.25 | $10,906.25 | — |
| 2026-08-28 | +0.75 | $10,906.25 | — | $10,906.25 | -0.00 | +0.00 | — | — | $10,906.25 | $10,906.25 | — |
| 2026-08-31 | -5.85 | $10,906.25 | — | $10,906.25 | -0.00 | +0.00 | — | — | $10,906.25 | $10,906.25 | — |
| 2026-09-01 | -6.30 | $10,906.25 | — | $10,906.25 | -0.00 | +0.00 | — | — | $10,906.25 | $10,906.25 | — |
| 2026-09-02 | -3.83 | $10,906.25 | — | $10,906.25 | -0.00 | +0.00 | — | — | $10,906.25 | $10,906.25 | — |
| 2026-09-03 | -0.90 | $10,906.25 | — | $10,906.25 | -0.00 | +0.00 | — | — | $10,906.25 | $10,906.25 | — |
| 2026-09-04 | +2.25 | $10,906.25 | — | $10,906.25 | -0.00 | -27.41 | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | — | $25.19 | $10,834.63 | CABA×1260, ALEC×370, BHC×139, BMEA×492, OABI×195, OPK×587, VIR×82, ATRC×17 |
| 2026-09-08 | -11.47 | $25.19 | CABA×1260, ALEC×370, BHC×139, BMEA×492, OABI×195, OPK×587, VIR×82, ATRC×17 | $10,763.44 | -71.19 | +0.00 | — | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | $10,718.60 | $10,718.60 | — |
| 2026-09-09 | -13.95 | $10,718.60 | — | $10,718.60 | +0.00 | +0.00 | — | — | $10,718.60 | $10,718.60 | — |
| 2026-09-10 | -13.28 | $10,718.60 | — | $10,718.60 | +0.00 | +0.00 | — | — | $10,718.60 | $10,718.60 | — |

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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 194 | $20.55 | $2.57 | — | $6,010.73 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $4000.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 9 | $91.01 | $2.02 | — | $5,189.62 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 41 | $20.65 | $2.11 | — | $4,340.86 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 148 | $5.77 | $2.43 | — | $3,484.46 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 43 | $19.63 | $2.12 | — | $2,638.25 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 28 | $29.63 | $2.07 | — | $1,806.54 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 489 | $1.75 | $6.31 | — | $944.48 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 5 | $144.54 | $2.00 | — | $219.78 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $219.78 | ▲ close $10,231.72 vs 09:30 $10,000.00 (session +253.36) | 16:00 close · cash $219.78 · equity $10,231.72 vs 09:30 $10,000.00 (+231.72; session marks +253.36) · 8 name(s) marked open→close (per-name table). AG×194 09:30 $20.55 → close $21.19 +124.16; BHP×9 09:30 $91.01 → close $93.63 +23.58; CDE×41 09:30 $20.65 → close $21.11 +18.86; HDSN×148 09:30 $5.77 → close $5.57 -29.60; IAG×43 09:30 $19.63 → close $20.50 +37.41; KGC×28 09:30 $29.63 → close $31.43 +50.40; NFGC×489 09:30 $1.75 → close $1.75 +0.00; WPM×5 09:30 $144.54 → close $150.25 +28.55 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $219.78 | ▲ 09:30 equity $10,520.65 vs yday $10,231.72 (+288.93) | 09:30 open · cash $219.78 (unchanged overnight, no fees) · equity $10,520.65 vs prior close $10,231.72 (+288.93) · 8 name(s) re-marked at the open (per-name table). AG×194 yday $21.19 → 09:30 $21.90 +137.74; BHP×9 yday $93.63 → 09:30 $95.72 +18.81; CDE×41 yday $21.11 → 09:30 $21.75 +26.24; HDSN×148 yday $5.57 → 09:30 $5.67 +14.80; IAG×43 yday $20.50 → 09:30 $21.17 +28.81; KGC×28 yday $31.43 → 09:30 $32.17 +20.72; NFGC×489 yday $1.75 → 09:30 $1.79 +19.56; WPM×5 yday $150.25 → 09:30 $154.70 +22.25 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 194 | $21.90 | $2.64 | $+256.69 | $4,465.74 | ▲ +256.69 after sell → book $10,518.01; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 9 | $95.72 | $2.04 | $+38.34 | $5,325.18 | ▲ +38.34 after sell → book $10,515.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 41 | $21.75 | $2.13 | $+40.85 | $6,214.80 | ▲ +40.85 after sell → book $10,513.84; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 148 | $5.67 | $2.47 | $-19.70 | $7,051.49 | ▼ -19.70 after sell → book $10,511.37; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 43 | $21.17 | $2.14 | $+61.96 | $7,959.66 | ▲ +61.96 after sell → book $10,509.23; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 28 | $32.17 | $2.09 | $+66.95 | $8,858.33 | ▲ +66.95 after sell → book $10,507.14; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 489 | $1.79 | $6.40 | $+6.85 | $9,727.24 | ▲ +6.85 after sell → book $10,500.74; vs 09:30 mark -6.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 5 | $154.70 | $2.02 | $+46.77 | $10,498.71 | ▲ +46.77 after sell → book $10,498.71; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 35 | $119.43 | $2.10 | — | $6,316.57 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+21.1; leftover $4199.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 52 | $17.20 | $2.15 | — | $5,420.02 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 4 | $216.30 | $2.00 | — | $4,552.82 | — | 40% to #1, rest split; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 80 | $11.13 | $2.23 | — | $3,660.19 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 364 | $2.47 | $4.70 | — | $2,756.42 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 466 | $1.93 | $6.01 | — | $1,851.02 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 15 | $59.72 | $2.04 | — | $953.19 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 681 | $1.32 | $8.78 | — | $45.48 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.48 | ▲ close $10,697.74 vs 09:30 $10,520.65 (session +229.03) | 16:00 close · cash $45.48 · equity $10,697.74 vs 09:30 $10,520.65 (+177.09; session marks +229.03) · 8 name(s) marked open→close (per-name table). AU×35 09:30 $119.43 → close $121.22 +62.65; AUPH×52 09:30 $17.20 → close $16.65 -28.60; AEM×4 09:30 $216.30 → close $216.06 -0.96; ARCT×80 09:30 $11.13 → close $13.45 +185.60; AUTL×364 09:30 $2.47 → close $2.41 -21.84; CRDL×466 09:30 $1.93 → close $1.86 -32.62; CRSP×15 09:30 $59.72 → close $59.50 -3.30; CYPH×681 09:30 $1.32 → close $1.42 +68.10 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.48 | ▲ 09:30 equity $10,936.65 vs yday $10,697.74 (+238.91) | 09:30 open · cash $45.48 (unchanged overnight, no fees) · equity $10,936.65 vs prior close $10,697.74 (+238.91) · 8 name(s) re-marked at the open (per-name table). AU×35 yday $121.22 → 09:30 $120.51 -24.85; AUPH×52 yday $16.65 → 09:30 $16.57 -4.16; AEM×4 yday $216.06 → 09:30 $217.03 +3.88; ARCT×80 yday $13.45 → 09:30 $13.33 -9.60; AUTL×364 yday $2.41 → 09:30 $2.40 -3.64; CRDL×466 yday $1.86 → 09:30 $1.88 +9.32; CRSP×15 yday $59.50 → 09:30 $58.75 -11.25; CYPH×681 yday $1.42 → 09:30 $1.83 +279.21 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 35 | $120.51 | $2.14 | $+33.57 | $4,261.20 | ▲ +33.57 after sell → book $10,934.52; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 52 | $16.57 | $2.17 | $-37.07 | $5,120.67 | ▼ -37.07 after sell → book $10,932.35; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 4 | $217.03 | $2.02 | $-1.10 | $5,986.77 | ▼ -1.10 after sell → book $10,930.33; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 80 | $13.33 | $2.25 | $+171.52 | $7,050.91 | ▲ +171.52 after sell → book $10,928.07; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 364 | $2.40 | $4.77 | $-34.94 | $7,919.75 | ▼ -34.94 after sell → book $10,923.31; vs 09:30 mark -4.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 466 | $1.88 | $6.10 | $-35.41 | $8,789.73 | ▼ -35.41 after sell → book $10,917.21; vs 09:30 mark -6.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 15 | $58.75 | $2.06 | $-18.64 | $9,668.92 | ▼ -18.64 after sell → book $10,915.15; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 681 | $1.83 | $8.91 | $+329.62 | $10,906.25 | ▲ +329.62 after sell → book $10,906.25; vs 09:30 mark -8.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,936.65 (session +0.00) | 16:00 close · cash $10,906.25 · no lots left · equity $10,906.25. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | 09:30 open · cash $10,906.25 · no holdings · equity $10,906.25 vs prior close $10,906.25 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | 16:00 close · cash $10,906.25 · no lots left · equity $10,906.25. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | 09:30 open · cash $10,906.25 · no holdings · equity $10,906.25 vs prior close $10,906.25 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | 16:00 close · cash $10,906.25 · no lots left · equity $10,906.25. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | 09:30 open · cash $10,906.25 · no holdings · equity $10,906.25 vs prior close $10,906.25 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | 16:00 close · cash $10,906.25 · no lots left · equity $10,906.25. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | 09:30 open · cash $10,906.25 · no holdings · equity $10,906.25 vs prior close $10,906.25 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | 16:00 close · cash $10,906.25 · no lots left · equity $10,906.25. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | 09:30 open · cash $10,906.25 · no holdings · equity $10,906.25 vs prior close $10,906.25 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | 16:00 close · cash $10,906.25 · no lots left · equity $10,906.25. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | 09:30 open · cash $10,906.25 · no holdings · equity $10,906.25 vs prior close $10,906.25 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | 16:00 close · cash $10,906.25 · no lots left · equity $10,906.25. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | 09:30 open · cash $10,906.25 · no holdings · equity $10,906.25 vs prior close $10,906.25 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | 16:00 close · cash $10,906.25 · no lots left · equity $10,906.25. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | 09:30 open · cash $10,906.25 · no holdings · equity $10,906.25 vs prior close $10,906.25 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.25 | ▲ close $10,906.25 vs 09:30 $10,906.25 (session +0.00) | 16:00 close · cash $10,906.25 · no lots left · equity $10,906.25. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.25 | ▲ 09:30 equity $10,906.25 vs yday $10,906.25 (-0.00) | 09:30 open · cash $10,906.25 · no holdings · equity $10,906.25 vs prior close $10,906.25 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 1260 | $3.46 | $16.25 | — | $6,530.39 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $4362.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 370 | $2.52 | $4.77 | — | $5,593.22 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $934.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 139 | $6.71 | $2.41 | — | $4,658.12 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $934.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 492 | $1.90 | $6.35 | — | $3,716.98 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $934.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 195 | $4.78 | $2.58 | — | $2,782.30 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $934.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 587 | $1.59 | $7.57 | — | $1,841.40 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $934.82 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 82 | $11.31 | $2.24 | — | $911.74 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $934.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 17 | $52.03 | $2.04 | — | $25.19 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+6.5; leftover $934.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.19 | ▼ close $10,834.63 vs 09:30 $10,906.25 (session -27.41) | 16:00 close · cash $25.19 · equity $10,834.63 vs 09:30 $10,906.25 (-71.62; session marks -27.41) · 8 name(s) marked open→close (per-name table). CABA×1260 09:30 $3.46 → close $3.47 +12.60; ALEC×370 09:30 $2.52 → close $2.46 -22.20; BHC×139 09:30 $6.71 → close $6.56 -20.85; BMEA×492 09:30 $1.90 → close $2.03 +63.96; OABI×195 09:30 $4.78 → close $4.33 -87.75; OPK×587 09:30 $1.59 → close $1.64 +29.35; VIR×82 09:30 $11.31 → close $11.38 +6.15; ATRC×17 09:30 $52.03 → close $51.52 -8.67 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.19 | ▼ 09:30 equity $10,763.44 vs yday $10,834.63 (-71.19) | 09:30 open · cash $25.19 (unchanged overnight, no fees) · equity $10,763.44 vs prior close $10,834.63 (-71.19) · 8 name(s) re-marked at the open (per-name table). CABA×1260 yday $3.47 → 09:30 $3.43 -50.40; ALEC×370 yday $2.46 → 09:30 $2.38 -29.60; BHC×139 yday $6.56 → 09:30 $6.57 +1.39; BMEA×492 yday $2.03 → 09:30 $2.00 -14.76; OABI×195 yday $4.33 → 09:30 $4.30 -5.85; OPK×587 yday $1.64 → 09:30 $1.63 -5.87; VIR×82 yday $11.38 → 09:30 $11.22 -13.53; ATRC×17 yday $51.52 → 09:30 $54.31 +47.43 | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 1260 | $3.43 | $16.50 | $-70.55 | $4,330.49 | ▼ -70.55 after sell → book $10,746.94; vs 09:30 mark -16.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 370 | $2.38 | $4.84 | $-61.42 | $5,206.25 | ▼ -61.42 after sell → book $10,742.10; vs 09:30 mark -4.84 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 139 | $6.57 | $2.44 | $-24.31 | $6,117.04 | ▼ -24.31 after sell → book $10,739.66; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 492 | $2.00 | $6.44 | $+36.41 | $7,094.60 | ▲ +36.41 after sell → book $10,733.22; vs 09:30 mark -6.44 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 195 | $4.30 | $2.62 | $-98.79 | $7,930.48 | ▼ -98.79 after sell → book $10,730.60; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 587 | $1.63 | $7.68 | $+8.23 | $8,879.61 | ▲ +8.23 after sell → book $10,722.92; vs 09:30 mark -7.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 82 | $11.22 | $2.26 | $-11.88 | $9,797.39 | ▼ -11.88 after sell → book $10,720.66; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 17 | $54.31 | $2.06 | $+34.66 | $10,718.60 | ▲ +34.66 after sell → book $10,718.60; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,763.44 (session +0.00) | 16:00 close · cash $10,718.60 · no lots left · equity $10,718.60. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.60 | ▲ 09:30 equity $10,718.60 vs yday $10,718.60 (+0.00) | 09:30 open · cash $10,718.60 · no holdings · equity $10,718.60 vs prior close $10,718.60 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,718.60 (session +0.00) | 16:00 close · cash $10,718.60 · no lots left · equity $10,718.60. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.60 | ▲ 09:30 equity $10,718.60 vs yday $10,718.60 (+0.00) | 09:30 open · cash $10,718.60 · no holdings · equity $10,718.60 vs prior close $10,718.60 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.60 | ▲ close $10,718.60 vs 09:30 $10,718.60 (session +0.00) | 16:00 close · cash $10,718.60 · no lots left · equity $10,718.60. | — |
