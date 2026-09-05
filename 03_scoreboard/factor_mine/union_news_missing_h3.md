# Factor mine action — `union_news_missing_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_missing, no 🚨

Cash book **+3.51%** ($10,351) · signal-only (no cash/fees) was +5.14%. Starts YES **1/17**. Fills 32 · skips 32 · realized $+351.32.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=missing` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,351.31.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | — | — | $97.53 | $10,435.58 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 |
| 2026-08-17 | +2.25 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,415.19 | -20.39 | — | — | $97.53 | $10,525.50 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,415.19 vs prior close $10,435.58 (-20.39) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66 |
| 2026-08-18 | -6.20 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,393.29 | -132.21 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $10,358.15 | $10,358.15 | — | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,393.29 vs prior close $10,525.50 (-132.21) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03 |
| 2026-08-19 | -7.20 | $10,358.15 | — | $10,358.15 | +0.00 | — | — | $10,358.15 | $10,358.15 | — | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $10,358.15 | — | $10,358.15 | +0.00 | — | — | $10,358.15 | $10,358.15 | — | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $10,358.15 | — | $10,358.15 | +0.00 | — | — | $10,358.15 | $10,358.15 | — | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-24 | -5.17 | $10,358.15 | — | $10,358.15 | +0.00 | — | — | $10,358.15 | $10,358.15 | — | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-25 | +1.80 | $10,358.15 | — | $10,358.15 | +0.00 | — | — | $10,358.15 | $10,358.15 | — | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $10,358.15 | — | $10,358.15 | +0.00 | — | — | $10,358.15 | $10,358.15 | — | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-27 | — | $10,358.15 | — | $10,358.15 | +0.00 | CRK, MOS, SLI, GGB, MT, TX, ANET, DLO | — | $207.03 | $10,395.58 | CRK×91, MOS×52, SLI×499, GGB×292, MT×17, TX×23, ANET×6, DLO×82 | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-28 | +0.75 | $207.03 | CRK×91, MOS×52, SLI×499, GGB×292, MT×17, TX×23, ANET×6, DLO×82 | $10,429.48 | +33.90 | — | — | $207.03 | $10,463.55 | CRK×91, MOS×52, SLI×499, GGB×292, MT×17, TX×23, ANET×6, DLO×82 | 09:30 open · cash $207.03 (unchanged overnight, no fees) · equity $10,429.48 vs prior close $10,395.58 (+33.90) because holdings re-marked: CRK×91 yday $14.50 → 09:30 $14.42 -7.28; MOS×52 yday $24.16 → 09:30 $24.00 -8.32; SLI×499 yday $2.61 → 09:30 $2.60 -4.99; GGB×292 yday $4.46 → 09:30 $4.57 +32.12; MT×17 yday $74.53 → 09:30 $74.54 +0.17; TX×23 yday $55.13 → 09:30 $55.25 +2.76; ANET×6 yday $202.25 → 09:30 $205.90 +21.90; DLO×82 yday $15.36 → 09:30 $15.33 -2.46 |
| 2026-08-31 | -5.85 | $207.03 | CRK×91, MOS×52, SLI×499, GGB×292, MT×17, TX×23, ANET×6, DLO×82 | $10,310.41 | -153.14 | — | — | $207.03 | $10,287.77 | CRK×91, MOS×52, SLI×499, GGB×292, MT×17, TX×23, ANET×6, DLO×82 | 09:30 open · cash $207.03 (unchanged overnight, no fees) · equity $10,310.41 vs prior close $10,463.55 (-153.14) because holdings re-marked: CRK×91 yday $14.62 → 09:30 $14.56 -5.46; MOS×52 yday $23.76 → 09:30 $23.75 -0.52; SLI×499 yday $2.64 → 09:30 $2.51 -64.87; GGB×292 yday $4.70 → 09:30 $4.55 -43.80; MT×17 yday $74.63 → 09:30 $75.07 +7.48; TX×23 yday $55.83 → 09:30 $54.84 -22.77; ANET×6 yday $201.09 → 09:30 $199.00 -12.54; DLO×82 yday $15.14 → 09:30 $15.01 -10.66 |
| 2026-09-01 | -6.30 | $207.03 | CRK×91, MOS×52, SLI×499, GGB×292, MT×17, TX×23, ANET×6, DLO×82 | $10,374.55 | +86.78 | — | CRK, MOS, SLI, GGB, MT, TX, ANET, DLO | $10,351.31 | $10,351.31 | — | 09:30 open · cash $207.03 (unchanged overnight, no fees) · equity $10,374.55 vs prior close $10,287.77 (+86.78) because holdings re-marked: CRK×91 yday $14.51 → 09:30 $14.31 -18.20; MOS×52 yday $23.78 → 09:30 $24.00 +11.44; SLI×499 yday $2.51 → 09:30 $2.70 +94.81; GGB×292 yday $4.55 → 09:30 $4.61 +17.52; MT×17 yday $75.06 → 09:30 $74.31 -12.75; TX×23 yday $54.84 → 09:30 $54.82 -0.46; ANET×6 yday $195.89 → 09:30 $196.60 +4.26; DLO×82 yday $15.00 → 09:30 $14.88 -9.84 |
| 2026-09-02 | -3.83 | $10,351.31 | — | $10,351.31 | +0.00 | — | — | $10,351.31 | $10,351.31 | — | 09:30 open · cash $10,351.31 · no holdings · equity $10,351.31 vs prior close $10,351.31 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,351.31 | — | $10,351.31 | +0.00 | — | — | $10,351.31 | $10,351.31 | — | 09:30 open · cash $10,351.31 · no holdings · equity $10,351.31 vs prior close $10,351.31 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $10,351.31 | — | $10,351.31 | +0.00 | — | — | $10,351.31 | $10,351.31 | — | 09:30 open · cash $10,351.31 · no holdings · equity $10,351.31 vs prior close $10,351.31 (+0.00). Cash unchanged overnight; no fees. |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▼ 09:30 equity $10,415.19 vs yday $10,435.58 (-20.39) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,415.19 vs prior close $10,435.58 (-20.39) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▼ 09:30 equity $10,393.29 vs yday $10,525.50 (-132.21) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,393.29 vs prior close $10,525.50 (-132.21) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,295.46 | ▼ -0.12 after sell → book $10,391.22; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,469.49 | ▼ -69.50 after sell → book $10,389.13; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,709.89 | ▲ +23.38 after sell → book $10,387.05; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,939.81 | ▼ -14.65 after sell → book $10,384.97; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,279.43 | ▲ +97.12 after sell → book $10,382.63; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,446.99 | ▼ -83.63 after sell → book $10,380.49; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $9,185.84 | ▲ +471.89 after sell → book $10,360.32; vs 09:30 mark -20.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $10,358.15 | ▼ -66.33 after sell → book $10,358.15; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 91 | $14.09 | $2.26 | — | $9,073.70 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ret5=+1.1; leftover $1294.77 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 52 | $24.84 | $2.15 | — | $7,779.87 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ret5=+13.0; leftover $1294.77 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 499 | $2.59 | $6.44 | — | $6,481.02 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ret5=+4.2; leftover $1294.77 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 292 | $4.42 | $3.77 | — | $5,186.62 | — | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=-8.6; leftover $1294.77 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $75.12 | $2.04 | — | $3,907.54 | — | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=-2.2; leftover $1294.77 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.20 | $2.06 | — | $2,635.88 | — | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=+3.0; leftover $1294.77 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $190.90 | $2.01 | — | $1,488.47 | — | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=-5.1; leftover $1294.77 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 82 | $15.60 | $2.24 | — | $207.03 | — | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=+7.1; leftover $1294.77 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $207.03 | ▲ 09:30 equity $10,429.48 vs yday $10,395.58 (+33.90) | 09:30 open · cash $207.03 (unchanged overnight, no fees) · equity $10,429.48 vs prior close $10,395.58 (+33.90) because holdings re-marked: CRK×91 yday $14.50 → 09:30 $14.42 -7.28; MOS×52 yday $24.16 → 09:30 $24.00 -8.32; SLI×499 yday $2.61 → 09:30 $2.60 -4.99; GGB×292 yday $4.46 → 09:30 $4.57 +32.12; MT×17 yday $74.53 → 09:30 $74.54 +0.17; TX×23 yday $55.13 → 09:30 $55.25 +2.76; ANET×6 yday $202.25 → 09:30 $205.90 +21.90; DLO×82 yday $15.36 → 09:30 $15.33 -2.46 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $207.03 | ▼ 09:30 equity $10,310.41 vs yday $10,463.55 (-153.14) | 09:30 open · cash $207.03 (unchanged overnight, no fees) · equity $10,310.41 vs prior close $10,463.55 (-153.14) because holdings re-marked: CRK×91 yday $14.62 → 09:30 $14.56 -5.46; MOS×52 yday $23.76 → 09:30 $23.75 -0.52; SLI×499 yday $2.64 → 09:30 $2.51 -64.87; GGB×292 yday $4.70 → 09:30 $4.55 -43.80; MT×17 yday $74.63 → 09:30 $75.07 +7.48; TX×23 yday $55.83 → 09:30 $54.84 -22.77; ANET×6 yday $201.09 → 09:30 $199.00 -12.54; DLO×82 yday $15.14 → 09:30 $15.01 -10.66 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $207.03 | ▲ 09:30 equity $10,374.55 vs yday $10,287.77 (+86.78) | 09:30 open · cash $207.03 (unchanged overnight, no fees) · equity $10,374.55 vs prior close $10,287.77 (+86.78) because holdings re-marked: CRK×91 yday $14.51 → 09:30 $14.31 -18.20; MOS×52 yday $23.78 → 09:30 $24.00 +11.44; SLI×499 yday $2.51 → 09:30 $2.70 +94.81; GGB×292 yday $4.55 → 09:30 $4.61 +17.52; MT×17 yday $75.06 → 09:30 $74.31 -12.75; TX×23 yday $54.84 → 09:30 $54.82 -0.46; ANET×6 yday $195.89 → 09:30 $196.60 +4.26; DLO×82 yday $15.00 → 09:30 $14.88 -9.84 | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 91 | $14.31 | $2.29 | $+15.47 | $1,506.96 | ▲ +15.47 after sell → book $10,372.27; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 52 | $24.00 | $2.17 | $-47.99 | $2,752.79 | ▼ -47.99 after sell → book $10,370.10; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 499 | $2.70 | $6.53 | $+41.92 | $4,093.56 | ▲ +41.92 after sell → book $10,363.57; vs 09:30 mark -6.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 292 | $4.61 | $3.83 | $+47.89 | $5,435.85 | ▲ +47.89 after sell → book $10,359.74; vs 09:30 mark -3.83 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MT` | 17 | $74.31 | $2.06 | $-17.87 | $6,697.06 | ▼ -17.87 after sell → book $10,357.68; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TX` | 23 | $54.82 | $2.08 | $-12.88 | $7,955.84 | ▼ -12.88 after sell → book $10,355.60; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `ANET` | 6 | $196.60 | $2.03 | $+30.16 | $9,133.41 | ▲ +30.16 after sell → book $10,353.57; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `DLO` | 82 | $14.88 | $2.26 | $-63.54 | $10,351.31 | ▼ -63.54 after sell → book $10,351.31; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,351.31 | ▲ 09:30 equity $10,351.31 vs yday $10,351.31 (+0.00) | 09:30 open · cash $10,351.31 · no holdings · equity $10,351.31 vs prior close $10,351.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,351.31 | ▲ 09:30 equity $10,351.31 vs yday $10,351.31 (+0.00) | 09:30 open · cash $10,351.31 · no holdings · equity $10,351.31 vs prior close $10,351.31 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,351.31 | ▲ 09:30 equity $10,351.31 vs yday $10,351.31 (+0.00) | 09:30 open · cash $10,351.31 · no holdings · equity $10,351.31 vs prior close $10,351.31 (+0.00). Cash unchanged overnight; no fees. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `ANET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `DLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `DLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
