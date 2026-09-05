# Factor mine action — `union_vol_missing_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ vol_missing, no 🚨

Cash book **+1.44%** ($10,144) · signal-only (no cash/fees) was +1.88%. Starts YES **1/17**. Fills 16 · skips 0 · realized $+143.92.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=missing` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,143.91.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $10,143.91 | $10,143.91 | — | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 |
| 2026-08-17 | +2.25 | $10,143.91 | — | $10,143.91 | -0.00 | — | — | $10,143.91 | $10,143.91 | — | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-18 | -6.20 | $10,143.91 | — | $10,143.91 | -0.00 | — | — | $10,143.91 | $10,143.91 | — | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-19 | -7.20 | $10,143.91 | — | $10,143.91 | -0.00 | — | — | $10,143.91 | $10,143.91 | — | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $10,143.91 | — | $10,143.91 | -0.00 | — | — | $10,143.91 | $10,143.91 | — | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $10,143.91 | — | $10,143.91 | -0.00 | — | — | $10,143.91 | $10,143.91 | — | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-24 | -5.17 | $10,143.91 | — | $10,143.91 | -0.00 | — | — | $10,143.91 | $10,143.91 | — | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-25 | +1.80 | $10,143.91 | — | $10,143.91 | -0.00 | — | — | $10,143.91 | $10,143.91 | — | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $10,143.91 | — | $10,143.91 | -0.00 | — | — | $10,143.91 | $10,143.91 | — | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-27 | — | $10,143.91 | — | $10,143.91 | -0.00 | — | — | $10,143.91 | $10,143.91 | — | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-28 | +0.75 | $10,143.91 | — | $10,143.91 | -0.00 | — | — | $10,143.91 | $10,143.91 | — | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-31 | -5.85 | $10,143.91 | — | $10,143.91 | -0.00 | — | — | $10,143.91 | $10,143.91 | — | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-01 | -6.30 | $10,143.91 | — | $10,143.91 | -0.00 | — | — | $10,143.91 | $10,143.91 | — | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $10,143.91 | — | $10,143.91 | -0.00 | — | — | $10,143.91 | $10,143.91 | — | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,143.91 | — | $10,143.91 | -0.00 | — | — | $10,143.91 | $10,143.91 | — | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $10,143.91 | — | $10,143.91 | -0.00 | — | — | $10,143.91 | $10,143.91 | — | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
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
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▼ -7.12 after sell → book $10,176.05; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | ▼ -55.19 after sell → book $10,173.96; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | ▲ +107.86 after sell → book $10,171.88; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | ▼ -64.90 after sell → book $10,169.80; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | ▲ +69.56 after sell → book $10,167.46; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | ▼ -29.03 after sell → book $10,165.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | ▲ +148.79 after sell → book $10,146.08; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | ▼ -26.05 after sell → book $10,143.91; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
