# Factor mine action — `union_vol_missing_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ vol_missing, no 🚨

Cash book **+3.58%** ($10,358) · signal-only (no cash/fees) was +5.59%. Starts YES **1/17**. Fills 16 · skips 16 · realized $+358.16.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=missing` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,358.15.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-13 | `TPG` | 24 | — | $50.62 | +0.00 | $54.62 | +95.92 | +95.92 | +0.00 | +95.92 |
| 2026-08-13 | `TGTX` | 25 | — | $49.70 | +0.00 | $47.94 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | -40.74 | -40.74 | +0.00 | -40.74 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | $61.71 | +41.20 | +29.60 | -3.00 | +38.20 |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | $44.06 | -0.81 | -18.90 | -51.03 | -51.84 |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | $53.03 | -54.24 | -38.16 | +112.00 | +57.76 |
| 2026-08-14 | `TGTX` | 25 | $47.94 | $47.27 | -16.75 | $48.74 | +36.75 | +20.00 | -60.75 | -24.00 |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | $12.78 | +40.28 | +44.52 | +74.20 | +114.48 |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | $28.15 | -42.00 | -26.04 | -24.78 | -66.78 |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | $1.09 | +246.88 | +293.17 | +185.16 | +432.04 |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | $22.72 | -10.60 | -21.73 | -21.73 | -32.33 |
| 2026-08-17 | `BTSG` | 20 | $61.71 | $61.69 | -0.40 | $60.38 | -26.20 | -26.60 | +37.80 | +11.60 |
| 2026-08-17 | `IREN` | 27 | $44.06 | $45.23 | +31.59 | $44.90 | -8.91 | +22.68 | -20.25 | -29.16 |
| 2026-08-17 | `TPG` | 24 | $53.03 | $52.67 | -8.64 | $51.77 | -21.60 | -30.24 | +49.12 | +27.52 |
| 2026-08-17 | `TGTX` | 25 | $48.74 | $48.74 | +0.00 | $49.28 | +13.50 | +13.50 | -24.00 | -10.50 |
| 2026-08-17 | `SLS` | 106 | $12.78 | $12.78 | +0.00 | $13.00 | +23.32 | +23.32 | +114.48 | +137.80 |
| 2026-08-17 | `HIMS` | 42 | $28.15 | $28.14 | -0.42 | $28.61 | +19.74 | +19.32 | -67.20 | -47.46 |
| 2026-08-17 | `INO` | 1543 | $1.09 | $1.07 | -30.86 | $1.15 | +123.44 | +92.58 | +401.18 | +524.62 |
| 2026-08-17 | `TNDM` | 53 | $22.72 | $22.50 | -11.66 | $22.25 | -12.99 | -24.65 | -43.99 | -56.97 |
| 2026-08-18 | `BTSG` | 20 | $60.38 | $60.00 | -7.60 | — | +0.00 | -7.60 | +4.00 | — |
| 2026-08-18 | `IREN` | 27 | $44.90 | $43.56 | -36.18 | — | +0.00 | -36.18 | -65.34 | — |
| 2026-08-18 | `TPG` | 24 | $51.77 | $51.77 | +0.00 | — | +0.00 | +0.00 | +27.52 | — |
| 2026-08-18 | `TGTX` | 25 | $49.28 | $49.28 | +0.00 | — | +0.00 | +0.00 | -10.50 | — |
| 2026-08-18 | `SLS` | 106 | $13.00 | $12.66 | -36.04 | — | +0.00 | -36.04 | +101.76 | — |
| 2026-08-18 | `HIMS` | 42 | $28.61 | $27.85 | -31.92 | — | +0.00 | -31.92 | -79.38 | — |
| 2026-08-18 | `INO` | 1543 | $1.15 | $1.14 | -15.43 | — | +0.00 | -15.43 | +509.19 | — |
| 2026-08-18 | `TNDM` | 53 | $22.25 | $22.16 | -5.03 | — | +0.00 | -5.03 | -62.01 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-21 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +185.07 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | +257.46 | — | — | $97.53 | $10,435.58 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 |
| 2026-08-17 | +2.25 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,415.19 | -20.39 | +110.30 | — | — | $97.53 | $10,525.50 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 |
| 2026-08-18 | -6.20 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,393.29 | -132.21 | +0.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $10,358.15 | $10,358.15 | — |
| 2026-08-19 | -7.20 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-08-20 | +1.12 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-08-21 | +3.25 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-08-24 | -5.17 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-08-25 | +1.80 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-08-26 | +2.02 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-08-27 | — | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-08-28 | +0.75 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-08-31 | -5.85 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-09-01 | -6.30 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-09-02 | -3.83 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-09-03 | -0.90 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-09-04 | — | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ vol_missing, no 🚨; gate vol=missing; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | 16:00 close · cash $97.53 · equity $10,153.12 vs 09:30 $10,000.00 (+153.12; session marks +185.07) · 8 name(s) marked open→close (per-name table). BTSG×20 09:30 $59.80 → close $60.23 +8.60; IREN×27 09:30 $45.98 → close $44.76 -32.94; TPG×24 09:30 $50.62 → close $54.62 +95.92; TGTX×25 09:30 $49.70 → close $47.94 -44.00; SLS×106 09:30 $11.70 → close $12.36 +69.96; HIMS×42 09:30 $29.74 → close $28.77 -40.74; INO×1543 09:30 $0.81 → close $0.90 +138.87; TNDM×53 09:30 $23.33 → close $23.13 -10.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) · 8 name(s) re-marked at the open (per-name table). BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,435.58 vs 09:30 $10,178.12 (session +257.46) | 16:00 close · cash $97.53 · equity $10,435.58 vs 09:30 $10,178.12 (+257.46; session marks +257.46) · 8 name(s) marked open→close (per-name table). BTSG×20 09:30 $59.65 → close $61.71 +41.20; IREN×27 09:30 $44.09 → close $44.06 -0.81; TPG×24 09:30 $55.29 → close $53.03 -54.24; TGTX×25 09:30 $47.27 → close $48.74 +36.75; SLS×106 09:30 $12.40 → close $12.78 +40.28; HIMS×42 09:30 $29.15 → close $28.15 -42.00; INO×1543 09:30 $0.93 → close $1.09 +246.88; TNDM×53 09:30 $22.92 → close $22.72 -10.60 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▼ 09:30 equity $10,415.19 vs yday $10,435.58 (-20.39) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,415.19 vs prior close $10,435.58 (-20.39) · 8 name(s) re-marked at the open (per-name table). BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,525.50 vs 09:30 $10,415.19 (session +110.30) | 16:00 close · cash $97.53 · equity $10,525.50 vs 09:30 $10,415.19 (+110.31; session marks +110.30) · 8 name(s) marked open→close (per-name table). BTSG×20 09:30 $61.69 → close $60.38 -26.20; IREN×27 09:30 $45.23 → close $44.90 -8.91; TPG×24 09:30 $52.67 → close $51.77 -21.60; TGTX×25 09:30 $48.74 → close $49.28 +13.50; SLS×106 09:30 $12.78 → close $13.00 +23.32; HIMS×42 09:30 $28.14 → close $28.61 +19.74; INO×1543 09:30 $1.07 → close $1.15 +123.44; TNDM×53 09:30 $22.50 → close $22.25 -12.99 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▼ 09:30 equity $10,393.29 vs yday $10,525.50 (-132.21) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,393.29 vs prior close $10,525.50 (-132.21) · 8 name(s) re-marked at the open (per-name table). BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,295.46 | ▼ -0.12 after sell → book $10,391.22; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,469.49 | ▼ -69.50 after sell → book $10,389.13; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,709.89 | ▲ +23.38 after sell → book $10,387.05; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,939.81 | ▼ -14.65 after sell → book $10,384.97; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,279.43 | ▲ +97.12 after sell → book $10,382.63; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,446.99 | ▼ -83.63 after sell → book $10,380.49; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $9,185.84 | ▲ +471.89 after sell → book $10,360.32; vs 09:30 mark -20.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $10,358.15 | ▼ -66.33 after sell → book $10,358.15; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,393.29 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |

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
