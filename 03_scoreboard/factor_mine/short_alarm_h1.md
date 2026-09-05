# Factor mine action — `short_alarm_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · alarm

Cash book **+2.60%** ($10,260) · signal-only (no cash/fees) was +2.41%. Starts YES **12/17**. Fills 58 · skips 48 · realized $+259.97.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `alarm=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,259.99.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | WWW, FOSL, AIRS, OMER, MXCT, AVAH, CRMD, LVWR | — | $14,948.61 | $10,037.72 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $14,948.61 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500 | $10,069.07 | +31.35 | HNST, FCEL, BW, INO, BYND, AEHR, LUNR, IOVA | WWW, FOSL, AIRS, OMER, MXCT, AVAH, CRMD, LVWR | $14,896.98 | $9,992.62 | HNST×130, FCEL×28, BW×60, INO×586, BYND×48, AEHR×4, LUNR×30, IOVA×91 | 09:30 open · cash $14,948.61 (unchanged overnight, no fees) · equity $10,069.07 vs prior close $10,037.72 (+31.35) because holdings re-marked: WWW×30 yday $21.03 → 09:30 $20.98 +1.50; FOSL×110 yday $5.57 → 09:30 $5.50 +7.70; AIRS×185 yday $3.43 → 09:30 $3.40 +6.48; OMER×36 yday $17.19 → 09:30 $17.17 +0.72; MXCT×449 yday $1.32 → 09:30 $1.32 +0.00; AVAH×52 yday $12.32 → 09:30 $12.21 +5.72; CRMD×77 yday $7.54 → 09:30 $7.55 -0.77; LVWR×500 yday $1.20 → 09:30 $1.18 +10.00 |
| 2026-08-18 | -6.20 | $14,896.98 | HNST×130, FCEL×28, BW×60, INO×586, BYND×48, AEHR×4, LUNR×30, IOVA×91 | $10,160.42 | +167.80 | — | HNST, FCEL, BW, INO, BYND, AEHR, LUNR, IOVA | $10,137.75 | $10,137.75 | — | 09:30 open · cash $14,896.98 (unchanged overnight, no fees) · equity $10,160.42 vs prior close $9,992.62 (+167.80) because holdings re-marked: HNST×130 yday $4.70 → 09:30 $4.67 +3.90; FCEL×28 yday $22.36 → 09:30 $21.18 +33.04; BW×60 yday $9.92 → 09:30 $9.60 +19.20; INO×586 yday $1.15 → 09:30 $1.14 +5.86; BYND×48 yday $11.63 → 09:30 $11.12 +24.48; AEHR×4 yday $145.61 → 09:30 $135.58 +40.12; LUNR×30 yday $20.38 → 09:30 $19.31 +32.10; IOVA×91 yday $7.10 → 09:30 $7.00 +9.10 |
| 2026-08-19 | -7.20 | $10,137.75 | — | $10,137.75 | +0.00 | — | — | $10,137.75 | $10,137.75 | — | 09:30 open · cash $10,137.75 · no holdings · equity $10,137.75 vs prior close $10,137.75 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $10,137.75 | — | $10,137.75 | +0.00 | — | — | $10,137.75 | $10,137.75 | — | 09:30 open · cash $10,137.75 · no holdings · equity $10,137.75 vs prior close $10,137.75 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $10,137.75 | — | $10,137.75 | +0.00 | YSS, SMJF, NOG, CPRT, FLO | — | $15,152.12 | $10,114.27 | YSS×109, SMJF×89, NOG×37, CPRT×29, FLO×146 | 09:30 open · cash $10,137.75 · no holdings · equity $10,137.75 vs prior close $10,137.75 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-24 | -5.17 | $15,152.12 | YSS×109, SMJF×89, NOG×37, CPRT×29, FLO×146 | $10,158.39 | +44.12 | — | YSS, SMJF, NOG, CPRT, FLO | $10,147.21 | $10,147.21 | — | 09:30 open · cash $15,152.12 (unchanged overnight, no fees) · equity $10,158.39 vs prior close $10,114.27 (+44.12) because holdings re-marked: YSS×109 yday $9.32 → 09:30 $9.14 +19.62; SMJF×89 yday $11.41 → 09:30 $11.18 +20.47; NOG×37 yday $27.34 → 09:30 $27.09 +9.25; CPRT×29 yday $33.80 → 09:30 $33.98 -5.22; FLO×146 yday $6.95 → 09:30 $6.95 +0.00 |
| 2026-08-25 | +1.80 | $10,147.21 | — | $10,147.21 | -0.00 | — | — | $10,147.21 | $10,147.21 | — | 09:30 open · cash $10,147.21 · no holdings · equity $10,147.21 vs prior close $10,147.21 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $10,147.21 | — | $10,147.21 | -0.00 | — | — | $10,147.21 | $10,147.21 | — | 09:30 open · cash $10,147.21 · no holdings · equity $10,147.21 vs prior close $10,147.21 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-27 | — | $10,147.21 | — | $10,147.21 | -0.00 | — | — | $10,147.21 | $10,147.21 | — | 09:30 open · cash $10,147.21 · no holdings · equity $10,147.21 vs prior close $10,147.21 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-28 | +0.75 | $10,147.21 | — | $10,147.21 | -0.00 | PYXS, SAFX, XPOF, APMD, OPTU, ABTC, XHG, DEFT | — | $15,155.99 | $10,161.25 | PYXS×191, SAFX×1626, XPOF×113, APMD×21, OPTU×598, ABTC×75, XHG×156, DEFT×1057 | 09:30 open · cash $10,147.21 · no holdings · equity $10,147.21 vs prior close $10,147.21 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-31 | -5.85 | $15,155.99 | PYXS×191, SAFX×1626, XPOF×113, APMD×21, OPTU×598, ABTC×75, XHG×156, DEFT×1057 | $10,300.10 | +138.85 | — | PYXS, SAFX, XPOF, APMD, OPTU, ABTC, XHG, DEFT | $10,259.99 | $10,259.99 | — | 09:30 open · cash $15,155.99 (unchanged overnight, no fees) · equity $10,300.10 vs prior close $10,161.25 (+138.85) because holdings re-marked: PYXS×191 yday $3.32 → 09:30 $3.23 +17.19; SAFX×1626 yday $0.37 → 09:30 $0.38 -16.26; XPOF×113 yday $5.39 → 09:30 $5.43 -4.52; APMD×21 yday $28.72 → 09:30 $29.80 -22.68; OPTU×598 yday $1.02 → 09:30 $1.02 +0.00; ABTC×75 yday $8.76 → 09:30 $7.73 +77.25; XHG×156 yday $3.80 → 09:30 $3.44 +56.16; DEFT×1057 yday $0.65 → 09:30 $0.62 +31.71 |
| 2026-09-01 | -6.30 | $10,259.99 | — | $10,259.99 | -0.00 | — | — | $10,259.99 | $10,259.99 | — | 09:30 open · cash $10,259.99 · no holdings · equity $10,259.99 vs prior close $10,259.99 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $10,259.99 | — | $10,259.99 | -0.00 | — | — | $10,259.99 | $10,259.99 | — | 09:30 open · cash $10,259.99 · no holdings · equity $10,259.99 vs prior close $10,259.99 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,259.99 | — | $10,259.99 | -0.00 | — | — | $10,259.99 | $10,259.99 | — | 09:30 open · cash $10,259.99 · no holdings · equity $10,259.99 vs prior close $10,259.99 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $10,259.99 | — | $10,259.99 | -0.00 | — | — | $10,259.99 | $10,259.99 | — | 09:30 open · cash $10,259.99 · no holdings · equity $10,259.99 vs prior close $10,259.99 (-0.00). Cash unchanged overnight; no fees. |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **SHORT** | `WWW` | 30 | $20.60 | $2.12 | — | $10,615.88 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+4.4; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `FOSL` | 110 | $5.64 | $2.37 | — | $11,233.92 | — | alarm; gate alarm=True; list probable; 🔵; ret5=-4.1; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AIRS` | 185 | $3.37 | $2.60 | — | $11,854.76 | — | alarm; gate alarm=True; list probable; ret5=-29.1; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 36 | $17.35 | $2.14 | — | $12,477.23 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 449 | $1.39 | $5.89 | — | $13,095.45 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AVAH` | 52 | $11.91 | $2.18 | — | $13,712.58 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+21.3; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `CRMD` | 77 | $8.05 | $2.26 | — | $14,330.17 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LVWR` | 500 | $1.25 | $6.56 | — | $14,948.61 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+12.6; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,948.61 | ▲ 09:30 equity $10,069.07 vs yday $10,037.72 (+31.35) | 09:30 open · cash $14,948.61 (unchanged overnight, no fees) · equity $10,069.07 vs prior close $10,037.72 (+31.35) because holdings re-marked: WWW×30 yday $21.03 → 09:30 $20.98 +1.50; FOSL×110 yday $5.57 → 09:30 $5.50 +7.70; AIRS×185 yday $3.43 → 09:30 $3.40 +6.48; OMER×36 yday $17.19 → 09:30 $17.17 +0.72; MXCT×449 yday $1.32 → 09:30 $1.32 +0.00; AVAH×52 yday $12.32 → 09:30 $12.21 +5.72; CRMD×77 yday $7.54 → 09:30 $7.55 -0.77; LVWR×500 yday $1.20 → 09:30 $1.18 +10.00 | — |
| 2026-08-17 09:30 ET | **COVER** | `WWW` | 30 | $20.98 | $2.08 | $-15.60 | $14,317.13 | ▼ -15.60 after sell → book $10,066.99; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `FOSL` | 110 | $5.50 | $2.32 | $+10.71 | $13,709.81 | ▲ +10.71 after sell → book $10,064.67; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AIRS` | 185 | $3.40 | $2.54 | $-9.77 | $13,079.19 | ▼ -9.77 after sell → book $10,062.12; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `OMER` | 36 | $17.17 | $2.10 | $+2.25 | $12,458.97 | ▲ +2.25 after sell → book $10,060.02; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MXCT` | 449 | $1.32 | $5.79 | $+19.74 | $11,860.50 | ▲ +19.74 after sell → book $10,054.23; vs 09:30 mark -5.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AVAH` | 52 | $12.21 | $2.15 | $-19.93 | $11,223.44 | ▼ -19.93 after sell → book $10,052.09; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `CRMD` | 77 | $7.55 | $2.22 | $+34.02 | $10,639.87 | ▲ +34.02 after sell → book $10,049.87; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `LVWR` | 500 | $1.18 | $6.45 | $+21.99 | $10,043.42 | ▲ +21.99 after sell → book $10,043.42; vs 09:30 mark -6.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `HNST` | 130 | $4.81 | $2.43 | — | $10,666.29 | — | alarm; gate alarm=True; list flatten; ⚪; ret5=-11.4; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `FCEL` | 28 | $22.37 | $2.11 | — | $11,290.54 | — | alarm; gate alarm=True; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BW` | 60 | $10.35 | $2.21 | — | $11,909.33 | — | alarm; gate alarm=True; list probable; ⚪; ret5=+9.8; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `INO` | 586 | $1.07 | $7.68 | — | $12,528.66 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+62.7; leftover $627.71 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 48 | $12.83 | $2.17 | — | $13,142.33 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `AEHR` | 4 | $132.79 | $2.04 | — | $13,671.46 | — | alarm; gate alarm=True; list yday_gainer; ⚪; ret5=+30.1; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `LUNR` | 30 | $20.25 | $2.12 | — | $14,276.84 | — | alarm; gate alarm=True; list yday_gainer,ohlc_hot; ⚪; ret5=+15.9; leftover $627.71 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `IOVA` | 91 | $6.84 | $2.31 | — | $14,896.98 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $627.71 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,896.98 | ▲ 09:30 equity $10,160.42 vs yday $9,992.62 (+167.80) | 09:30 open · cash $14,896.98 (unchanged overnight, no fees) · equity $10,160.42 vs prior close $9,992.62 (+167.80) because holdings re-marked: HNST×130 yday $4.70 → 09:30 $4.67 +3.90; FCEL×28 yday $22.36 → 09:30 $21.18 +33.04; BW×60 yday $9.92 → 09:30 $9.60 +19.20; INO×586 yday $1.15 → 09:30 $1.14 +5.86; BYND×48 yday $11.63 → 09:30 $11.12 +24.48; AEHR×4 yday $145.61 → 09:30 $135.58 +40.12; LUNR×30 yday $20.38 → 09:30 $19.31 +32.10; IOVA×91 yday $7.10 → 09:30 $7.00 +9.10 | — |
| 2026-08-18 09:30 ET | **COVER** | `HNST` | 130 | $4.67 | $2.38 | $+13.39 | $14,287.50 | ▲ +13.39 after sell → book $10,158.04; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `FCEL` | 28 | $21.18 | $2.07 | $+29.13 | $13,692.38 | ▲ +29.13 after sell → book $10,155.96; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `BW` | 60 | $9.60 | $2.17 | $+40.62 | $13,114.21 | ▲ +40.62 after sell → book $10,153.79; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `INO` | 586 | $1.14 | $7.56 | $-56.26 | $12,438.61 | ▼ -56.26 after sell → book $10,146.23; vs 09:30 mark -7.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `BYND` | 48 | $11.12 | $2.13 | $+77.78 | $11,902.72 | ▲ +77.78 after sell → book $10,144.10; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `AEHR` | 4 | $135.58 | $2.00 | $-15.20 | $11,358.40 | ▼ -15.20 after sell → book $10,142.10; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `LUNR` | 30 | $19.31 | $2.08 | $+24.00 | $10,777.02 | ▲ +24.00 after sell → book $10,140.02; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `IOVA` | 91 | $7.00 | $2.26 | $-19.13 | $10,137.75 | ▼ -19.13 after sell → book $10,137.75; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,137.75 | ▲ 09:30 equity $10,137.75 vs yday $10,137.75 (+0.00) | 09:30 open · cash $10,137.75 · no holdings · equity $10,137.75 vs prior close $10,137.75 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,137.75 | ▲ 09:30 equity $10,137.75 vs yday $10,137.75 (+0.00) | 09:30 open · cash $10,137.75 · no holdings · equity $10,137.75 vs prior close $10,137.75 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,137.75 | ▲ 09:30 equity $10,137.75 vs yday $10,137.75 (+0.00) | 09:30 open · cash $10,137.75 · no holdings · equity $10,137.75 vs prior close $10,137.75 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 09:30 ET | **SHORT** | `YSS` | 109 | $9.26 | $2.37 | — | $11,144.72 | — | alarm; gate alarm=True; list yday_mover; ret5=-20.1; leftover $1013.78 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SMJF` | 89 | $11.35 | $2.31 | — | $12,152.56 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+13.4; leftover $1013.78 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 37 | $27.00 | $2.15 | — | $13,149.41 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $1013.78 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CPRT` | 29 | $34.48 | $2.12 | — | $14,147.21 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.8; leftover $1013.78 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `FLO` | 146 | $6.90 | $2.49 | — | $15,152.12 | — | alarm; gate alarm=True; list earn_react; ret5=-5.7; leftover $1013.78 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,152.12 | ▲ 09:30 equity $10,158.39 vs yday $10,114.27 (+44.12) | 09:30 open · cash $15,152.12 (unchanged overnight, no fees) · equity $10,158.39 vs prior close $10,114.27 (+44.12) because holdings re-marked: YSS×109 yday $9.32 → 09:30 $9.14 +19.62; SMJF×89 yday $11.41 → 09:30 $11.18 +20.47; NOG×37 yday $27.34 → 09:30 $27.09 +9.25; CPRT×29 yday $33.80 → 09:30 $33.98 -5.22; FLO×146 yday $6.95 → 09:30 $6.95 +0.00 | — |
| 2026-08-24 09:30 ET | **COVER** | `YSS` | 109 | $9.14 | $2.32 | $+8.39 | $14,153.54 | ▲ +8.39 after sell → book $10,156.07; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `SMJF` | 89 | $11.18 | $2.26 | $+10.56 | $13,156.26 | ▲ +10.56 after sell → book $10,153.81; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `NOG` | 37 | $27.09 | $2.10 | $-7.58 | $12,151.83 | ▼ -7.58 after sell → book $10,151.71; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `CPRT` | 29 | $33.98 | $2.08 | $+10.30 | $11,164.34 | ▲ +10.30 after sell → book $10,149.64; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `FLO` | 146 | $6.95 | $2.43 | $-12.22 | $10,147.21 | ▼ -12.22 after sell → book $10,147.21; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,147.21 | ▲ 09:30 equity $10,147.21 vs yday $10,147.21 (-0.00) | 09:30 open · cash $10,147.21 · no holdings · equity $10,147.21 vs prior close $10,147.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,147.21 | ▲ 09:30 equity $10,147.21 vs yday $10,147.21 (-0.00) | 09:30 open · cash $10,147.21 · no holdings · equity $10,147.21 vs prior close $10,147.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,147.21 | ▲ 09:30 equity $10,147.21 vs yday $10,147.21 (-0.00) | 09:30 open · cash $10,147.21 · no holdings · equity $10,147.21 vs prior close $10,147.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,147.21 | ▲ 09:30 equity $10,147.21 vs yday $10,147.21 (-0.00) | 09:30 open · cash $10,147.21 · no holdings · equity $10,147.21 vs prior close $10,147.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **SHORT** | `PYXS` | 191 | $3.31 | $2.62 | — | $10,776.80 | — | alarm; gate alarm=True; list yday_gainer; ret5=+2.3; leftover $634.20 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1626 | $0.39 | $11.52 | — | $11,399.42 | — | alarm; gate alarm=True; list yday_gainer; ret5=-26.5; leftover $634.20 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `XPOF` | 113 | $5.59 | $2.38 | — | $12,028.71 | — | alarm; gate alarm=True; list yday_gainer; ret5=+6.6; leftover $634.20 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `APMD` | 21 | $29.50 | $2.09 | — | $12,646.12 | — | alarm; gate alarm=True; list yday_gainer; ret5=-11.7; leftover $634.20 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `OPTU` | 598 | $1.06 | $7.84 | — | $13,272.16 | — | alarm; gate alarm=True; list yday_gainer; ret5=-7.8; leftover $634.20 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `ABTC` | 75 | $8.41 | $2.25 | — | $13,900.66 | — | alarm; gate alarm=True; list yday_mover; ret5=+9.2; leftover $634.20 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `XHG` | 156 | $4.06 | $2.51 | — | $14,531.51 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.1; leftover $634.20 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `DEFT` | 1057 | $0.60 | $9.72 | — | $15,155.99 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+17.6; leftover $634.20 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,155.99 | ▲ 09:30 equity $10,300.10 vs yday $10,161.25 (+138.85) | 09:30 open · cash $15,155.99 (unchanged overnight, no fees) · equity $10,300.10 vs prior close $10,161.25 (+138.85) because holdings re-marked: PYXS×191 yday $3.32 → 09:30 $3.23 +17.19; SAFX×1626 yday $0.37 → 09:30 $0.38 -16.26; XPOF×113 yday $5.39 → 09:30 $5.43 -4.52; APMD×21 yday $28.72 → 09:30 $29.80 -22.68; OPTU×598 yday $1.02 → 09:30 $1.02 +0.00; ABTC×75 yday $8.76 → 09:30 $7.73 +77.25; XHG×156 yday $3.80 → 09:30 $3.44 +56.16; DEFT×1057 yday $0.65 → 09:30 $0.62 +31.71 | — |
| 2026-08-31 09:30 ET | **COVER** | `PYXS` | 191 | $3.23 | $2.56 | $+10.09 | $14,536.50 | ▲ +10.09 after sell → book $10,297.54; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SAFX` | 1626 | $0.38 | $11.06 | $-6.31 | $13,907.56 | ▼ -6.31 after sell → book $10,286.48; vs 09:30 mark -11.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `XPOF` | 113 | $5.43 | $2.33 | $+13.38 | $13,291.64 | ▲ +13.38 after sell → book $10,284.15; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `APMD` | 21 | $29.80 | $2.05 | $-10.44 | $12,663.79 | ▼ -10.44 after sell → book $10,282.10; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `OPTU` | 598 | $1.02 | $7.71 | $+8.36 | $12,046.12 | ▲ +8.36 after sell → book $10,274.39; vs 09:30 mark -7.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABTC` | 75 | $7.73 | $2.21 | $+46.53 | $11,464.15 | ▲ +46.53 after sell → book $10,272.17; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `XHG` | 156 | $3.44 | $2.46 | $+91.75 | $10,925.05 | ▲ +91.75 after sell → book $10,269.71; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `DEFT` | 1057 | $0.62 | $9.72 | $-40.58 | $10,259.99 | ▼ -40.58 after sell → book $10,259.99; vs 09:30 mark -9.72 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,259.99 | ▲ 09:30 equity $10,259.99 vs yday $10,259.99 (-0.00) | 09:30 open · cash $10,259.99 · no holdings · equity $10,259.99 vs prior close $10,259.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,259.99 | ▲ 09:30 equity $10,259.99 vs yday $10,259.99 (-0.00) | 09:30 open · cash $10,259.99 · no holdings · equity $10,259.99 vs prior close $10,259.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,259.99 | ▲ 09:30 equity $10,259.99 vs yday $10,259.99 (-0.00) | 09:30 open · cash $10,259.99 · no holdings · equity $10,259.99 vs prior close $10,259.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,259.99 | ▲ 09:30 equity $10,259.99 vs yday $10,259.99 (-0.00) | 09:30 open · cash $10,259.99 · no holdings · equity $10,259.99 vs prior close $10,259.99 (-0.00). Cash unchanged overnight; no fees. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SNDK` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LITE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WDC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENHA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INV` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `INMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WFF` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EYPT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CAN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ARCT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ASST` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `ERO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TRLV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GUTS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WPM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `EGO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FCX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `AEM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `QMCO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `AREC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SNAP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `STT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PTRN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PCG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MNSO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ED` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DUOL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DEFT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ERO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FUTU` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CVI` | hard_red | hard-red S=-3.83 sit; no new buys |
