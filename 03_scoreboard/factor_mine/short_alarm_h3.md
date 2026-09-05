# Factor mine action — `short_alarm_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · alarm

Cash book **+2.77%** ($10,277) · signal-only (no cash/fees) was +11.88%. Starts YES **12/17**. Fills 58 · skips 110 · realized $+276.81.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `alarm=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,276.82.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | WWW, FOSL, AIRS, OMER, MXCT, AVAH, CRMD, LVWR | — | $14,948.61 | $10,037.72 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $14,948.61 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500 | $10,069.07 | +31.35 | HNST, FCEL, BW, INO, BYND, AEHR, LUNR, IOVA | — | $19,844.20 | $10,058.29 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500, HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 | 09:30 open · cash $14,948.61 (unchanged overnight, no fees) · equity $10,069.07 vs prior close $10,037.72 (+31.35) because holdings re-marked: WWW×30 yday $21.03 → 09:30 $20.98 +1.50; FOSL×110 yday $5.57 → 09:30 $5.50 +7.70; AIRS×185 yday $3.43 → 09:30 $3.40 +6.48; OMER×36 yday $17.19 → 09:30 $17.17 +0.72; MXCT×449 yday $1.32 → 09:30 $1.32 +0.00; AVAH×52 yday $12.32 → 09:30 $12.21 +5.72; CRMD×77 yday $7.54 → 09:30 $7.55 -0.77; LVWR×500 yday $1.20 → 09:30 $1.18 +10.00 |
| 2026-08-18 | -6.20 | $19,844.20 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500, HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 | $10,276.97 | +218.68 | — | — | $19,844.20 | $10,202.19 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500, HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 | 09:30 open · cash $19,844.20 (unchanged overnight, no fees) · equity $10,276.97 vs prior close $10,058.29 (+218.68) because holdings re-marked: WWW×30 yday $19.83 → 09:30 $19.95 -3.60; FOSL×110 yday $5.74 → 09:30 $5.78 -4.40; AIRS×185 yday $3.08 → 09:30 $3.01 +13.88; OMER×36 yday $17.36 → 09:30 $17.03 +11.88; MXCT×449 yday $1.32 → 09:30 $1.30 +8.98; AVAH×52 yday $12.69 → 09:30 $12.68 +0.52; CRMD×77 yday $7.67 → 09:30 $7.71 -3.08; LVWR×500 yday $1.15 → 09:30 $1.10 +25.00; HNST×130 yday $4.70 → 09:30 $4.67 +3.90; FCEL×28 yday $22.36 → 09:30 $21.18 +33.04; BW×60 yday $9.92 → 09:30 $9.60 +19.20; INO×588 yday $1.15 → 09:30 $1.14 +5.88; BYND×49 yday $11.63 → 09:30 $11.12 +24.99; AEHR×4 yday $145.61 → 09:30 $135.58 +40.12; LUNR×31 yday $20.38 → 09:30 $19.31 +33.17; IOVA×92 yday $7.10 → 09:30 $7.00 +9.20 |
| 2026-08-19 | -7.20 | $19,844.20 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500, HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 | $10,183.81 | -18.38 | — | WWW, FOSL, AIRS, OMER, MXCT, AVAH, CRMD, LVWR | $15,013.56 | $10,050.60 | HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 | 09:30 open · cash $19,844.20 (unchanged overnight, no fees) · equity $10,183.81 vs prior close $10,202.19 (-18.38) because holdings re-marked: WWW×30 yday $19.99 → 09:30 $20.08 -2.70; FOSL×110 yday $5.50 → 09:30 $5.54 -4.40; AIRS×185 yday $2.69 → 09:30 $2.71 -2.78; OMER×36 yday $17.19 → 09:30 $17.13 +2.16; MXCT×449 yday $1.27 → 09:30 $1.29 -8.98; AVAH×52 yday $12.67 → 09:30 $12.92 -13.00; CRMD×77 yday $8.17 → 09:30 $8.30 -10.01; LVWR×500 yday $1.24 → 09:30 $1.17 +35.00; HNST×130 yday $4.75 → 09:30 $4.80 -6.50; FCEL×28 yday $21.70 → 09:30 $21.48 +6.16; BW×60 yday $9.14 → 09:30 $9.14 +0.00; INO×588 yday $1.20 → 09:30 $1.22 -11.76; BYND×49 yday $12.74 → 09:30 $12.63 +5.39; AEHR×4 yday $123.25 → 09:30 $123.64 -1.56; LUNR×31 yday $19.31 → 09:30 $18.98 +10.23; IOVA×92 yday $7.03 → 09:30 $7.20 -15.64 |
| 2026-08-20 | +1.12 | $15,013.56 | HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 | $10,097.97 | +47.37 | — | HNST, FCEL, BW, INO, BYND, AEHR, LUNR, IOVA | $10,075.28 | $10,075.28 | — | 09:30 open · cash $15,013.56 (unchanged overnight, no fees) · equity $10,097.97 vs prior close $10,050.60 (+47.37) because holdings re-marked: HNST×130 yday $5.02 → 09:30 $4.98 +5.20; FCEL×28 yday $20.30 → 09:30 $20.21 +2.52; BW×60 yday $9.11 → 09:30 $9.05 +3.60; INO×588 yday $1.30 → 09:30 $1.30 +0.00; BYND×49 yday $14.08 → 09:30 $13.60 +23.52; AEHR×4 yday $107.96 → 09:30 $106.01 +7.80; LUNR×31 yday $18.52 → 09:30 $18.13 +12.09; IOVA×92 yday $7.99 → 09:30 $8.07 -7.36 |
| 2026-08-21 | +3.25 | $10,075.28 | — | $10,075.28 | -0.00 | YSS, SMJF, NOG, CPRT, FLO | — | $15,069.04 | $10,051.92 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | 09:30 open · cash $10,075.28 · no holdings · equity $10,075.28 vs prior close $10,075.28 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-24 | -5.17 | $15,069.04 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | $10,095.63 | +43.71 | — | — | $15,069.04 | $10,070.64 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | 09:30 open · cash $15,069.04 (unchanged overnight, no fees) · equity $10,095.63 vs prior close $10,051.92 (+43.71) because holdings re-marked: YSS×108 yday $9.32 → 09:30 $9.14 +19.44; SMJF×88 yday $11.41 → 09:30 $11.18 +20.24; NOG×37 yday $27.34 → 09:30 $27.09 +9.25; CPRT×29 yday $33.80 → 09:30 $33.98 -5.22; FLO×146 yday $6.95 → 09:30 $6.95 +0.00 |
| 2026-08-25 | +1.80 | $15,069.04 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | $10,023.77 | -46.87 | — | — | $15,069.04 | $10,006.22 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | 09:30 open · cash $15,069.04 (unchanged overnight, no fees) · equity $10,023.77 vs prior close $10,070.64 (-46.87) because holdings re-marked: YSS×108 yday $9.47 → 09:30 $9.77 -32.40; SMJF×88 yday $11.19 → 09:30 $11.20 -0.88; NOG×37 yday $26.49 → 09:30 $26.10 +14.43; CPRT×29 yday $33.19 → 09:30 $33.25 -1.74; FLO×146 yday $7.18 → 09:30 $7.36 -26.28 |
| 2026-08-26 | +2.02 | $15,069.04 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | $10,006.22 | -0.00 | — | — | $15,069.04 | $10,023.77 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | 09:30 open · cash $15,069.04 (unchanged overnight, no fees) · equity $10,006.22 vs prior close $10,006.22 (-0.00) because holdings re-marked: YSS×108 yday $9.99 → 09:30 $9.99 +0.00; SMJF×88 yday $11.25 → 09:30 $11.25 +0.00; NOG×37 yday $26.50 → 09:30 $26.50 +0.00; CPRT×29 yday $33.28 → 09:30 $33.28 +0.00; FLO×146 yday $7.18 → 09:30 $7.18 +0.00 |
| 2026-08-27 | — | $15,069.04 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | $10,134.26 | +110.49 | — | YSS, SMJF, NOG, CPRT, FLO | $10,123.08 | $10,123.08 | — | 09:30 open · cash $15,069.04 (unchanged overnight, no fees) · equity $10,134.26 vs prior close $10,023.77 (+110.49) because holdings re-marked: YSS×108 yday $9.99 → 09:30 $9.20 +85.32; SMJF×88 yday $11.25 → 09:30 $11.15 +8.80; NOG×37 yday $26.50 → 09:30 $26.00 +18.50; CPRT×29 yday $33.28 → 09:30 $33.00 +8.12; FLO×146 yday $7.18 → 09:30 $7.13 +7.30 |
| 2026-08-28 | +0.75 | $10,123.08 | — | $10,123.08 | +0.00 | PYXS, SAFX, XPOF, APMD, OPTU, ABTC, XHG, DEFT | — | $15,122.41 | $10,136.94 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 | 09:30 open · cash $10,123.08 · no holdings · equity $10,123.08 vs prior close $10,123.08 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-31 | -5.85 | $15,122.41 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 | $10,275.38 | +138.44 | — | — | $15,122.41 | $10,285.60 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 | 09:30 open · cash $15,122.41 (unchanged overnight, no fees) · equity $10,275.38 vs prior close $10,136.94 (+138.44) because holdings re-marked: PYXS×191 yday $3.32 → 09:30 $3.23 +17.19; SAFX×1622 yday $0.37 → 09:30 $0.38 -16.22; XPOF×113 yday $5.39 → 09:30 $5.43 -4.52; APMD×21 yday $28.72 → 09:30 $29.80 -22.68; OPTU×596 yday $1.02 → 09:30 $1.02 +0.00; ABTC×75 yday $8.76 → 09:30 $7.73 +77.25; XHG×155 yday $3.80 → 09:30 $3.44 +55.80; DEFT×1054 yday $0.65 → 09:30 $0.62 +31.62 |
| 2026-09-01 | -6.30 | $15,122.41 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 | $10,411.58 | +125.98 | — | — | $15,122.41 | $10,419.60 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 | 09:30 open · cash $15,122.41 (unchanged overnight, no fees) · equity $10,411.58 vs prior close $10,285.60 (+125.98) because holdings re-marked: PYXS×191 yday $3.23 → 09:30 $3.14 +17.19; SAFX×1622 yday $0.37 → 09:30 $0.37 +0.00; XPOF×113 yday $5.43 → 09:30 $5.44 -1.13; APMD×21 yday $29.80 → 09:30 $25.90 +81.90; OPTU×596 yday $1.02 → 09:30 $0.97 +29.80; ABTC×75 yday $7.81 → 09:30 $8.09 -21.00; XHG×155 yday $3.44 → 09:30 $3.52 -12.40; DEFT×1054 yday $0.62 → 09:30 $0.59 +31.62 |
| 2026-09-02 | -3.83 | $15,122.41 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 | $10,359.34 | -60.26 | — | PYXS, SAFX, XPOF, APMD, OPTU, ABTC, XHG | $10,993.22 | $10,297.58 | DEFT×1054 | 09:30 open · cash $15,122.41 (unchanged overnight, no fees) · equity $10,359.34 vs prior close $10,419.60 (-60.26) because holdings re-marked: PYXS×191 yday $3.14 → 09:30 $3.24 -19.10; SAFX×1622 yday $0.37 → 09:30 $0.37 +0.00; XPOF×113 yday $5.44 → 09:30 $5.39 +5.65; APMD×21 yday $26.00 → 09:30 $26.11 -2.31; OPTU×596 yday $0.97 → 09:30 $0.99 -11.92; ABTC×75 yday $7.86 → 09:30 $7.91 -3.75; XHG×155 yday $3.43 → 09:30 $3.48 -7.75; DEFT×1054 yday $0.61 → 09:30 $0.63 -21.08 |
| 2026-09-03 | -0.90 | $10,993.22 | DEFT×1054 | $10,287.04 | -10.54 | — | DEFT | $10,276.82 | $10,276.82 | — | 09:30 open · cash $10,993.22 (unchanged overnight, no fees) · equity $10,287.04 vs prior close $10,297.58 (-10.54) because holdings re-marked: DEFT×1054 yday $0.66 → 09:30 $0.67 -10.54 |
| 2026-09-04 | — | $10,276.82 | — | $10,276.82 | -0.00 | — | — | $10,276.82 | $10,276.82 | — | 09:30 open · cash $10,276.82 · no holdings · equity $10,276.82 vs prior close $10,276.82 (-0.00). Cash unchanged overnight; no fees. |

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
| 2026-08-17 09:30 ET | **SHORT** | `HNST` | 130 | $4.81 | $2.43 | — | $15,571.48 | — | alarm; gate alarm=True; list flatten; ⚪; ret5=-11.4; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `FCEL` | 28 | $22.37 | $2.11 | — | $16,195.73 | — | alarm; gate alarm=True; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BW` | 60 | $10.35 | $2.21 | — | $16,814.53 | — | alarm; gate alarm=True; list probable; ⚪; ret5=+9.8; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `INO` | 588 | $1.07 | $7.71 | — | $17,435.98 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+62.7; leftover $629.32 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 49 | $12.83 | $2.17 | — | $18,062.47 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `AEHR` | 4 | $132.79 | $2.04 | — | $18,591.59 | — | alarm; gate alarm=True; list yday_gainer; ⚪; ret5=+30.1; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `LUNR` | 31 | $20.25 | $2.12 | — | $19,217.22 | — | alarm; gate alarm=True; list yday_gainer,ohlc_hot; ⚪; ret5=+15.9; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `IOVA` | 92 | $6.84 | $2.31 | — | $19,844.20 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $629.32 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,844.20 | ▲ 09:30 equity $10,276.97 vs yday $10,058.29 (+218.68) | 09:30 open · cash $19,844.20 (unchanged overnight, no fees) · equity $10,276.97 vs prior close $10,058.29 (+218.68) because holdings re-marked: WWW×30 yday $19.83 → 09:30 $19.95 -3.60; FOSL×110 yday $5.74 → 09:30 $5.78 -4.40; AIRS×185 yday $3.08 → 09:30 $3.01 +13.88; OMER×36 yday $17.36 → 09:30 $17.03 +11.88; MXCT×449 yday $1.32 → 09:30 $1.30 +8.98; AVAH×52 yday $12.69 → 09:30 $12.68 +0.52; CRMD×77 yday $7.67 → 09:30 $7.71 -3.08; LVWR×500 yday $1.15 → 09:30 $1.10 +25.00; HNST×130 yday $4.70 → 09:30 $4.67 +3.90; FCEL×28 yday $22.36 → 09:30 $21.18 +33.04; BW×60 yday $9.92 → 09:30 $9.60 +19.20; INO×588 yday $1.15 → 09:30 $1.14 +5.88; BYND×49 yday $11.63 → 09:30 $11.12 +24.99; AEHR×4 yday $145.61 → 09:30 $135.58 +40.12; LUNR×31 yday $20.38 → 09:30 $19.31 +33.17; IOVA×92 yday $7.10 → 09:30 $7.00 +9.20 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,844.20 | ▼ 09:30 equity $10,183.81 vs yday $10,202.19 (-18.38) | 09:30 open · cash $19,844.20 (unchanged overnight, no fees) · equity $10,183.81 vs prior close $10,202.19 (-18.38) because holdings re-marked: WWW×30 yday $19.99 → 09:30 $20.08 -2.70; FOSL×110 yday $5.50 → 09:30 $5.54 -4.40; AIRS×185 yday $2.69 → 09:30 $2.71 -2.78; OMER×36 yday $17.19 → 09:30 $17.13 +2.16; MXCT×449 yday $1.27 → 09:30 $1.29 -8.98; AVAH×52 yday $12.67 → 09:30 $12.92 -13.00; CRMD×77 yday $8.17 → 09:30 $8.30 -10.01; LVWR×500 yday $1.24 → 09:30 $1.17 +35.00; HNST×130 yday $4.75 → 09:30 $4.80 -6.50; FCEL×28 yday $21.70 → 09:30 $21.48 +6.16; BW×60 yday $9.14 → 09:30 $9.14 +0.00; INO×588 yday $1.20 → 09:30 $1.22 -11.76; BYND×49 yday $12.74 → 09:30 $12.63 +5.39; AEHR×4 yday $123.25 → 09:30 $123.64 -1.56; LUNR×31 yday $19.31 → 09:30 $18.98 +10.23; IOVA×92 yday $7.03 → 09:30 $7.20 -15.64 | — |
| 2026-08-19 09:30 ET | **COVER** | `WWW` | 30 | $20.08 | $2.08 | $+11.40 | $19,239.72 | ▲ +11.40 after sell → book $10,181.73; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `FOSL` | 110 | $5.54 | $2.32 | $+6.31 | $18,628.00 | ▲ +6.31 after sell → book $10,179.41; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AIRS` | 185 | $2.71 | $2.54 | $+116.95 | $18,124.10 | ▲ +116.95 after sell → book $10,176.86; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OMER` | 36 | $17.13 | $2.10 | $+3.69 | $17,505.32 | ▲ +3.69 after sell → book $10,174.76; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `MXCT` | 449 | $1.29 | $5.79 | $+33.21 | $16,920.32 | ▲ +33.21 after sell → book $10,168.97; vs 09:30 mark -5.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AVAH` | 52 | $12.92 | $2.15 | $-56.85 | $16,246.33 | ▼ -56.85 after sell → book $10,166.82; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `CRMD` | 77 | $8.30 | $2.22 | $-23.73 | $15,605.01 | ▼ -23.73 after sell → book $10,164.60; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `LVWR` | 500 | $1.17 | $6.45 | $+26.99 | $15,013.56 | ▲ +26.99 after sell → book $10,158.15; vs 09:30 mark -6.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,013.56 | ▲ 09:30 equity $10,097.97 vs yday $10,050.60 (+47.37) | 09:30 open · cash $15,013.56 (unchanged overnight, no fees) · equity $10,097.97 vs prior close $10,050.60 (+47.37) because holdings re-marked: HNST×130 yday $5.02 → 09:30 $4.98 +5.20; FCEL×28 yday $20.30 → 09:30 $20.21 +2.52; BW×60 yday $9.11 → 09:30 $9.05 +3.60; INO×588 yday $1.30 → 09:30 $1.30 +0.00; BYND×49 yday $14.08 → 09:30 $13.60 +23.52; AEHR×4 yday $107.96 → 09:30 $106.01 +7.80; LUNR×31 yday $18.52 → 09:30 $18.13 +12.09; IOVA×92 yday $7.99 → 09:30 $8.07 -7.36 | — |
| 2026-08-20 09:30 ET | **COVER** | `HNST` | 130 | $4.98 | $2.38 | $-26.91 | $14,363.78 | ▼ -26.91 after sell → book $10,095.59; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `FCEL` | 28 | $20.21 | $2.07 | $+56.29 | $13,795.83 | ▲ +56.29 after sell → book $10,093.52; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `BW` | 60 | $9.05 | $2.17 | $+73.62 | $13,250.66 | ▲ +73.62 after sell → book $10,091.35; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `INO` | 588 | $1.30 | $7.59 | $-150.54 | $12,478.67 | ▼ -150.54 after sell → book $10,083.76; vs 09:30 mark -7.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `BYND` | 49 | $13.60 | $2.14 | $-42.04 | $11,810.14 | ▼ -42.04 after sell → book $10,081.63; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `AEHR` | 4 | $106.01 | $2.00 | $+103.08 | $11,384.10 | ▲ +103.08 after sell → book $10,079.63; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 31 | $18.13 | $2.08 | $+61.52 | $10,819.98 | ▲ +61.52 after sell → book $10,077.54; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `IOVA` | 92 | $8.07 | $2.27 | $-117.73 | $10,075.28 | ▼ -117.73 after sell → book $10,075.28; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,075.28 | ▲ 09:30 equity $10,075.28 vs yday $10,075.28 (-0.00) | 09:30 open · cash $10,075.28 · no holdings · equity $10,075.28 vs prior close $10,075.28 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 09:30 ET | **SHORT** | `YSS` | 108 | $9.26 | $2.37 | — | $11,072.99 | — | alarm; gate alarm=True; list yday_mover; ret5=-20.1; leftover $1007.53 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SMJF` | 88 | $11.35 | $2.31 | — | $12,069.48 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+13.4; leftover $1007.53 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 37 | $27.00 | $2.15 | — | $13,066.33 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $1007.53 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CPRT` | 29 | $34.48 | $2.12 | — | $14,064.13 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.8; leftover $1007.53 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `FLO` | 146 | $6.90 | $2.49 | — | $15,069.04 | — | alarm; gate alarm=True; list earn_react; ret5=-5.7; leftover $1007.53 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,069.04 | ▲ 09:30 equity $10,095.63 vs yday $10,051.92 (+43.71) | 09:30 open · cash $15,069.04 (unchanged overnight, no fees) · equity $10,095.63 vs prior close $10,051.92 (+43.71) because holdings re-marked: YSS×108 yday $9.32 → 09:30 $9.14 +19.44; SMJF×88 yday $11.41 → 09:30 $11.18 +20.24; NOG×37 yday $27.34 → 09:30 $27.09 +9.25; CPRT×29 yday $33.80 → 09:30 $33.98 -5.22; FLO×146 yday $6.95 → 09:30 $6.95 +0.00 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,069.04 | ▼ 09:30 equity $10,023.77 vs yday $10,070.64 (-46.87) | 09:30 open · cash $15,069.04 (unchanged overnight, no fees) · equity $10,023.77 vs prior close $10,070.64 (-46.87) because holdings re-marked: YSS×108 yday $9.47 → 09:30 $9.77 -32.40; SMJF×88 yday $11.19 → 09:30 $11.20 -0.88; NOG×37 yday $26.49 → 09:30 $26.10 +14.43; CPRT×29 yday $33.19 → 09:30 $33.25 -1.74; FLO×146 yday $7.18 → 09:30 $7.36 -26.28 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,069.04 | ▲ 09:30 equity $10,006.22 vs yday $10,006.22 (-0.00) | 09:30 open · cash $15,069.04 (unchanged overnight, no fees) · equity $10,006.22 vs prior close $10,006.22 (-0.00) because holdings re-marked: YSS×108 yday $9.99 → 09:30 $9.99 +0.00; SMJF×88 yday $11.25 → 09:30 $11.25 +0.00; NOG×37 yday $26.50 → 09:30 $26.50 +0.00; CPRT×29 yday $33.28 → 09:30 $33.28 +0.00; FLO×146 yday $7.18 → 09:30 $7.18 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,069.04 | ▲ 09:30 equity $10,134.26 vs yday $10,023.77 (+110.49) | 09:30 open · cash $15,069.04 (unchanged overnight, no fees) · equity $10,134.26 vs prior close $10,023.77 (+110.49) because holdings re-marked: YSS×108 yday $9.99 → 09:30 $9.20 +85.32; SMJF×88 yday $11.25 → 09:30 $11.15 +8.80; NOG×37 yday $26.50 → 09:30 $26.00 +18.50; CPRT×29 yday $33.28 → 09:30 $33.00 +8.12; FLO×146 yday $7.18 → 09:30 $7.13 +7.30 | — |
| 2026-08-27 09:30 ET | **COVER** | `YSS` | 108 | $9.20 | $2.31 | $+1.80 | $14,073.12 | ▲ +1.80 after sell → book $10,131.94; vs 09:30 mark -2.32 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `SMJF` | 88 | $11.15 | $2.25 | $+13.04 | $13,089.67 | ▲ +13.04 after sell → book $10,129.69; vs 09:30 mark -2.25 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `NOG` | 37 | $26.00 | $2.10 | $+32.75 | $12,125.57 | ▲ +32.75 after sell → book $10,127.59; vs 09:30 mark -2.10 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `CPRT` | 29 | $33.00 | $2.08 | $+38.72 | $11,166.49 | ▲ +38.72 after sell → book $10,125.51; vs 09:30 mark -2.08 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `FLO` | 146 | $7.13 | $2.43 | $-38.50 | $10,123.08 | ▼ -38.50 after sell → book $10,123.08; vs 09:30 mark -2.43 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,123.08 | ▲ 09:30 equity $10,123.08 vs yday $10,123.08 (+0.00) | 09:30 open · cash $10,123.08 · no holdings · equity $10,123.08 vs prior close $10,123.08 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **SHORT** | `PYXS` | 191 | $3.31 | $2.62 | — | $10,752.67 | — | alarm; gate alarm=True; list yday_gainer; ret5=+2.3; leftover $632.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1622 | $0.39 | $11.49 | — | $11,373.76 | — | alarm; gate alarm=True; list yday_gainer; ret5=-26.5; leftover $632.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `XPOF` | 113 | $5.59 | $2.38 | — | $12,003.06 | — | alarm; gate alarm=True; list yday_gainer; ret5=+6.6; leftover $632.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `APMD` | 21 | $29.50 | $2.09 | — | $12,620.47 | — | alarm; gate alarm=True; list yday_gainer; ret5=-11.7; leftover $632.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `OPTU` | 596 | $1.06 | $7.81 | — | $13,244.41 | — | alarm; gate alarm=True; list yday_gainer; ret5=-7.8; leftover $632.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `ABTC` | 75 | $8.41 | $2.25 | — | $13,872.91 | — | alarm; gate alarm=True; list yday_mover; ret5=+9.2; leftover $632.69 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `XHG` | 155 | $4.06 | $2.51 | — | $14,499.70 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.1; leftover $632.69 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `DEFT` | 1054 | $0.60 | $9.69 | — | $15,122.41 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+17.6; leftover $632.69 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,122.41 | ▲ 09:30 equity $10,275.38 vs yday $10,136.94 (+138.44) | 09:30 open · cash $15,122.41 (unchanged overnight, no fees) · equity $10,275.38 vs prior close $10,136.94 (+138.44) because holdings re-marked: PYXS×191 yday $3.32 → 09:30 $3.23 +17.19; SAFX×1622 yday $0.37 → 09:30 $0.38 -16.22; XPOF×113 yday $5.39 → 09:30 $5.43 -4.52; APMD×21 yday $28.72 → 09:30 $29.80 -22.68; OPTU×596 yday $1.02 → 09:30 $1.02 +0.00; ABTC×75 yday $8.76 → 09:30 $7.73 +77.25; XHG×155 yday $3.80 → 09:30 $3.44 +55.80; DEFT×1054 yday $0.65 → 09:30 $0.62 +31.62 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,122.41 | ▲ 09:30 equity $10,411.58 vs yday $10,285.60 (+125.98) | 09:30 open · cash $15,122.41 (unchanged overnight, no fees) · equity $10,411.58 vs prior close $10,285.60 (+125.98) because holdings re-marked: PYXS×191 yday $3.23 → 09:30 $3.14 +17.19; SAFX×1622 yday $0.37 → 09:30 $0.37 +0.00; XPOF×113 yday $5.43 → 09:30 $5.44 -1.13; APMD×21 yday $29.80 → 09:30 $25.90 +81.90; OPTU×596 yday $1.02 → 09:30 $0.97 +29.80; ABTC×75 yday $7.81 → 09:30 $8.09 -21.00; XHG×155 yday $3.44 → 09:30 $3.52 -12.40; DEFT×1054 yday $0.62 → 09:30 $0.59 +31.62 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,122.41 | ▼ 09:30 equity $10,359.34 vs yday $10,419.60 (-60.26) | 09:30 open · cash $15,122.41 (unchanged overnight, no fees) · equity $10,359.34 vs prior close $10,419.60 (-60.26) because holdings re-marked: PYXS×191 yday $3.14 → 09:30 $3.24 -19.10; SAFX×1622 yday $0.37 → 09:30 $0.37 +0.00; XPOF×113 yday $5.44 → 09:30 $5.39 +5.65; APMD×21 yday $26.00 → 09:30 $26.11 -2.31; OPTU×596 yday $0.97 → 09:30 $0.99 -11.92; ABTC×75 yday $7.86 → 09:30 $7.91 -3.75; XHG×155 yday $3.43 → 09:30 $3.48 -7.75; DEFT×1054 yday $0.61 → 09:30 $0.63 -21.08 | — |
| 2026-09-02 09:30 ET | **COVER** | `PYXS` | 191 | $3.24 | $2.56 | $+8.18 | $14,501.01 | ▲ +8.18 after sell → book $10,356.78; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SAFX` | 1622 | $0.37 | $10.87 | $+10.08 | $13,890.00 | ▲ +10.08 after sell → book $10,345.91; vs 09:30 mark -10.87 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `XPOF` | 113 | $5.39 | $2.33 | $+17.90 | $13,278.60 | ▲ +17.90 after sell → book $10,343.58; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `APMD` | 21 | $26.11 | $2.05 | $+67.05 | $12,728.24 | ▲ +67.05 after sell → book $10,341.53; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `OPTU` | 596 | $0.99 | $7.66 | $+26.25 | $12,130.54 | ▲ +26.25 after sell → book $10,333.87; vs 09:30 mark -7.66 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `ABTC` | 75 | $7.91 | $2.21 | $+33.03 | $11,535.08 | ▲ +33.03 after sell → book $10,331.66; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `XHG` | 155 | $3.48 | $2.46 | $+84.94 | $10,993.22 | ▲ +84.94 after sell → book $10,329.20; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,993.22 | ▼ 09:30 equity $10,287.04 vs yday $10,297.58 (-10.54) | 09:30 open · cash $10,993.22 (unchanged overnight, no fees) · equity $10,287.04 vs prior close $10,297.58 (-10.54) because holdings re-marked: DEFT×1054 yday $0.66 → 09:30 $0.67 -10.54 | — |
| 2026-09-03 09:30 ET | **COVER** | `DEFT` | 1054 | $0.67 | $10.22 | $-93.69 | $10,276.82 | ▼ -93.69 after sell → book $10,276.82; vs 09:30 mark -10.22 | dropped from list after 4 sess (min 3) | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,276.82 | ▲ 09:30 equity $10,276.82 vs yday $10,276.82 (-0.00) | 09:30 open · cash $10,276.82 · no holdings · equity $10,276.82 vs prior close $10,276.82 (-0.00). Cash unchanged overnight; no fees. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `WWW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `FOSL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MXCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `WWW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FOSL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OMER` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MXCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FCEL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BYND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `AEHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SNDK` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LITE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WDC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENHA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INV` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FCEL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BYND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AEHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `INMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WFF` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EYPT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `YSS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SMJF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `NOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CPRT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CAN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ARCT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ASST` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `YSS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `SMJF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `NOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CPRT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `FLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `YSS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SMJF` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NOG` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CPRT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FLO` | no_price | no 09:30 open — carry |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `XPOF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `APMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TRLV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GUTS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WPM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `EGO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FCX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `AEM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `QMCO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `XPOF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `APMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OPTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `XHG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-02 | `ERO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FUTU` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CVI` | hard_red | hard-red S=-3.83 sit; no new buys |
