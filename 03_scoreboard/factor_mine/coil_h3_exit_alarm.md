# Factor mine action — `coil_h3_exit_alarm`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · coil, exit on 🚨

Cash book **+4.05%** ($10,405) · signal-only (no cash/fees) was +8.25%. Starts YES **7/17**. Fills 76 · skips 152 · realized $+165.45.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $49.62.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | TPG, VOR | — | $37.44 | $10,677.03 | TPG×98, VOR×227 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $37.44 | TPG×98, VOR×227 | $10,751.77 | +74.74 | LDI, BTBT, ANGX, HYLN | — | $20.51 | $10,461.99 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 | 09:30 open · cash $37.44 (unchanged overnight, no fees) · equity $10,751.77 vs prior close $10,677.03 (+74.74) because holdings re-marked: TPG×98 yday $54.62 → 09:30 $55.29 +65.66; VOR×227 yday $23.29 → 09:30 $23.33 +9.08 |
| 2026-08-17 | +2.25 | $20.51 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 | $10,399.63 | -62.36 | — | — | $20.51 | $10,334.34 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 | 09:30 open · cash $20.51 (unchanged overnight, no fees) · equity $10,399.63 vs prior close $10,461.99 (-62.36) because holdings re-marked: TPG×98 yday $53.03 → 09:30 $52.67 -35.28; VOR×227 yday $23.03 → 09:30 $22.91 -27.24; LDI×4 yday $0.90 → 09:30 $0.91 +0.04; BTBT×3 yday $1.57 → 09:30 $1.52 -0.15; ANGX×1 yday $4.37 → 09:30 $4.60 +0.23; HYLN×1 yday $4.06 → 09:30 $4.10 +0.04 |
| 2026-08-18 | -6.20 | $20.51 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 | $10,290.95 | -43.39 | — | TPG, VOR | $10,268.76 | $10,285.26 | LDI×4, BTBT×3, ANGX×1, HYLN×1 | 09:30 open · cash $20.51 (unchanged overnight, no fees) · equity $10,290.95 vs prior close $10,334.34 (-43.39) because holdings re-marked: TPG×98 yday $51.77 → 09:30 $51.77 +0.00; VOR×227 yday $23.01 → 09:30 $22.82 -43.13; LDI×4 yday $0.88 → 09:30 $0.87 -0.02; BTBT×3 yday $1.60 → 09:30 $1.54 -0.18; ANGX×1 yday $4.71 → 09:30 $4.79 +0.08; HYLN×1 yday $4.09 → 09:30 $3.95 -0.14 |
| 2026-08-19 | -7.20 | $10,268.76 | LDI×4, BTBT×3, ANGX×1, HYLN×1 | $10,285.20 | -0.06 | — | LDI, BTBT, ANGX, HYLN | $10,284.93 | $10,284.93 | — | 09:30 open · cash $10,268.76 (unchanged overnight, no fees) · equity $10,285.20 vs prior close $10,285.26 (-0.06) because holdings re-marked: LDI×4 yday $0.86 → 09:30 $0.88 +0.09; BTBT×3 yday $1.45 → 09:30 $1.42 -0.09; ANGX×1 yday $4.85 → 09:30 $4.79 -0.06; HYLN×1 yday $3.86 → 09:30 $3.87 +0.01 |
| 2026-08-20 | +1.12 | $10,284.93 | — | $10,284.93 | +0.00 | AG, BHP, HDSN, IAG, KGC, NFGC, DNA, EXK | — | $32.48 | $10,364.66 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119 | 09:30 open · cash $10,284.93 · no holdings · equity $10,284.93 vs prior close $10,284.93 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $32.48 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119 | $10,631.26 | +266.60 | BTBT, ORBS, QTRX | — | $22.48 | $10,617.83 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4, QTRX×1 | 09:30 open · cash $32.48 (unchanged overnight, no fees) · equity $10,631.26 vs prior close $10,364.66 (+266.60) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; HDSN×222 yday $5.57 → 09:30 $5.67 +22.20; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×734 yday $1.75 → 09:30 $1.79 +29.36; DNA×172 yday $6.96 → 09:30 $7.09 +22.36; EXK×119 yday $10.97 → 09:30 $11.34 +44.03 |
| 2026-08-24 | -5.17 | $22.48 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4, QTRX×1 | $10,734.98 | +117.15 | — | — | $22.48 | $10,554.71 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4, QTRX×1 | 09:30 open · cash $22.48 (unchanged overnight, no fees) · equity $10,734.98 vs prior close $10,617.83 (+117.15) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; HDSN×222 yday $5.63 → 09:30 $5.69 +13.32; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×734 yday $1.84 → 09:30 $1.86 +14.68; DNA×172 yday $7.40 → 09:30 $7.26 -24.08; EXK×119 yday $10.62 → 09:30 $11.01 +46.41; BTBT×2 yday $1.53 → 09:30 $1.55 +0.04; ORBS×4 yday $0.88 → 09:30 $0.89 +0.04; QTRX×1 yday $2.99 → 09:30 $2.98 -0.01 |
| 2026-08-25 | +1.80 | $22.48 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4, QTRX×1 | $10,553.29 | -1.42 | INSP, CRMD, HCA, BMEA, ALIT, ZURA, JANX, KURA | AG, BHP, HDSN, IAG, KGC, NFGC, DNA, EXK | $69.17 | $10,576.41 | BTBT×2, ORBS×4, QTRX×1, INSP×21, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98 | 09:30 open · cash $22.48 (unchanged overnight, no fees) · equity $10,553.29 vs prior close $10,554.71 (-1.42) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; HDSN×222 yday $5.57 → 09:30 $5.53 -8.88; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×734 yday $1.90 → 09:30 $1.91 +7.34; DNA×172 yday $6.98 → 09:30 $6.82 -27.52; EXK×119 yday $10.74 → 09:30 $10.72 -2.38; BTBT×2 yday $1.56 → 09:30 $1.55 -0.02; ORBS×4 yday $0.85 → 09:30 $0.85 +0.00; QTRX×1 yday $2.76 → 09:30 $2.80 +0.04 |
| 2026-08-26 | +2.02 | $69.17 | BTBT×2, ORBS×4, QTRX×1, INSP×21, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98 | $10,576.41 | +0.00 | — | — | $69.17 | $10,500.88 | BTBT×2, ORBS×4, QTRX×1, INSP×21, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98 | 09:30 open · cash $69.17 (unchanged overnight, no fees) · equity $10,576.41 vs prior close $10,576.41 (+0.00) because holdings re-marked: BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; ORBS×4 yday $0.84 → 09:30 $0.84 +0.00; QTRX×1 yday $2.80 → 09:30 $2.80 +0.00; INSP×21 yday $61.47 → 09:30 $61.47 +0.00; CRMD×158 yday $8.28 → 09:30 $8.28 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×811 yday $1.61 → 09:30 $1.61 +0.00; ALIT×88 yday $14.87 → 09:30 $14.87 +0.00; ZURA×206 yday $6.50 → 09:30 $6.50 +0.00; JANX×70 yday $18.99 → 09:30 $18.99 +0.00; KURA×98 yday $13.58 → 09:30 $13.58 +0.00 |
| 2026-08-27 | — | $69.17 | BTBT×2, ORBS×4, QTRX×1, INSP×21, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98 | $10,606.90 | +106.02 | SLI | BTBT, ORBS, QTRX | $70.24 | $10,466.94 | INSP×21, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98, SLI×3 | 09:30 open · cash $69.17 (unchanged overnight, no fees) · equity $10,606.90 vs prior close $10,500.88 (+106.02) because holdings re-marked: BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; ORBS×4 yday $0.84 → 09:30 $0.80 -0.16; QTRX×1 yday $2.80 → 09:30 $2.83 +0.03; INSP×21 yday $61.47 → 09:30 $60.07 -29.40; CRMD×158 yday $8.28 → 09:30 $8.60 +50.56; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×811 yday $1.61 → 09:30 $1.75 +113.54; ALIT×88 yday $14.87 → 09:30 $14.85 -1.76; ZURA×206 yday $6.50 → 09:30 $6.13 -76.22; JANX×70 yday $18.99 → 09:30 $18.59 -28.00; KURA×98 yday $13.58 → 09:30 $13.63 +4.90 |
| 2026-08-28 | +0.75 | $70.24 | INSP×21, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98, SLI×3 | $10,530.21 | +63.27 | RRC, CRK, ANF, BZ, LVWR, BBWI, CRDL | INSP, CRMD, HCA, BMEA, ALIT, ZURA, JANX, KURA | $47.34 | $10,421.64 | SLI×3, RRC×36, CRK×103, ANF×10, BZ×81, LVWR×1086, BBWI×80, CRDL×717 | 09:30 open · cash $70.24 (unchanged overnight, no fees) · equity $10,530.21 vs prior close $10,466.94 (+63.27) because holdings re-marked: INSP×21 yday $61.80 → 09:30 $62.10 +6.30; CRMD×158 yday $8.39 → 09:30 $8.49 +15.80; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; BMEA×811 yday $1.71 → 09:30 $1.74 +24.33; ALIT×88 yday $14.33 → 09:30 $14.54 +18.48; ZURA×206 yday $5.99 → 09:30 $6.02 +6.18; JANX×70 yday $18.89 → 09:30 $19.00 +7.70; KURA×98 yday $13.06 → 09:30 $12.98 -7.84; SLI×3 yday $2.61 → 09:30 $2.60 -0.03 |
| 2026-08-31 | -5.85 | $47.34 | SLI×3, RRC×36, CRK×103, ANF×10, BZ×81, LVWR×1086, BBWI×80, CRDL×717 | $10,407.44 | -14.20 | — | — | $47.34 | $10,394.34 | SLI×3, RRC×36, CRK×103, ANF×10, BZ×81, LVWR×1086, BBWI×80, CRDL×717 | 09:30 open · cash $47.34 (unchanged overnight, no fees) · equity $10,407.44 vs prior close $10,421.64 (-14.20) because holdings re-marked: SLI×3 yday $2.64 → 09:30 $2.51 -0.39; RRC×36 yday $41.64 → 09:30 $41.11 -19.08; CRK×103 yday $14.62 → 09:30 $14.56 -6.18; ANF×10 yday $145.75 → 09:30 $148.67 +29.20; BZ×81 yday $18.00 → 09:30 $17.89 -8.91; LVWR×1086 yday $1.36 → 09:30 $1.37 +10.86; BBWI×80 yday $18.65 → 09:30 $19.30 +52.00; CRDL×717 yday $2.06 → 09:30 $1.96 -71.70 |
| 2026-09-01 | -6.30 | $47.34 | SLI×3, RRC×36, CRK×103, ANF×10, BZ×81, LVWR×1086, BBWI×80, CRDL×717 | $10,121.14 | -273.20 | — | SLI | $55.33 | $10,127.46 | RRC×36, CRK×103, ANF×10, BZ×81, LVWR×1086, BBWI×80, CRDL×717 | 09:30 open · cash $47.34 (unchanged overnight, no fees) · equity $10,121.14 vs prior close $10,394.34 (-273.20) because holdings re-marked: SLI×3 yday $2.51 → 09:30 $2.70 +0.57; RRC×36 yday $41.78 → 09:30 $41.32 -16.56; CRK×103 yday $14.51 → 09:30 $14.31 -20.60; ANF×10 yday $149.28 → 09:30 $142.47 -68.10; BZ×81 yday $17.90 → 09:30 $17.37 -42.93; LVWR×1086 yday $1.34 → 09:30 $1.22 -130.32; BBWI×80 yday $19.22 → 09:30 $19.10 -9.60; CRDL×717 yday $1.96 → 09:30 $1.98 +14.34 |
| 2026-09-02 | -3.83 | $55.33 | RRC×36, CRK×103, ANF×10, BZ×81, LVWR×1086, BBWI×80, CRDL×717 | $10,200.04 | +72.58 | — | RRC, CRK, ANF, BZ, LVWR, BBWI, CRDL | $10,165.46 | $10,165.46 | — | 09:30 open · cash $55.33 (unchanged overnight, no fees) · equity $10,200.04 vs prior close $10,127.46 (+72.58) because holdings re-marked: RRC×36 yday $41.32 → 09:30 $41.94 +22.32; CRK×103 yday $14.90 → 09:30 $15.82 +94.76; ANF×10 yday $143.00 → 09:30 $142.00 -10.00; BZ×81 yday $17.17 → 09:30 $17.29 +9.72; LVWR×1086 yday $1.18 → 09:30 $1.19 +10.86; BBWI×80 yday $19.10 → 09:30 $18.77 -26.40; CRDL×717 yday $1.98 → 09:30 $1.94 -28.68 |
| 2026-09-03 | -0.90 | $10,165.46 | — | $10,165.46 | +0.00 | HRMY, VSTM, RVTY, GPRO, CRK, MMED, EIX, CRDL | — | $64.06 | $10,781.99 | HRMY×30, VSTM×165, RVTY×10, GPRO×1041, CRK×80, MMED×55, EIX×22, CRDL×588 | 09:30 open · cash $10,165.46 · no holdings · equity $10,165.46 vs prior close $10,165.46 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $64.06 | HRMY×30, VSTM×165, RVTY×10, GPRO×1041, CRK×80, MMED×55, EIX×22, CRDL×588 | $10,904.87 | +122.88 | BAK, SGLD | — | $49.62 | $10,404.78 | HRMY×30, VSTM×165, RVTY×10, GPRO×1041, CRK×80, MMED×55, EIX×22, CRDL×588, BAK×4, SGLD×1 | 09:30 open · cash $64.06 (unchanged overnight, no fees) · equity $10,904.87 vs prior close $10,781.99 (+122.88) because holdings re-marked: HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; VSTM×165 yday $8.02 → 09:30 $8.03 +1.65; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1041 yday $1.69 → 09:30 $1.78 +93.69; CRK×80 yday $15.54 → 09:30 $15.45 -7.20; MMED×55 yday $23.76 → 09:30 $23.88 +6.60; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CRDL×588 yday $2.17 → 09:30 $2.18 +5.88 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $5,036.64 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 227 | $22.01 | $2.93 | — | $37.44 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.44 | ▲ 09:30 equity $10,751.77 vs yday $10,677.03 (+74.74) | 09:30 open · cash $37.44 (unchanged overnight, no fees) · equity $10,751.77 vs prior close $10,677.03 (+74.74) because holdings re-marked: TPG×98 yday $54.62 → 09:30 $55.29 +65.66; VOR×227 yday $23.29 → 09:30 $23.33 +9.08 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 4 | $0.94 | $0.05 | — | $33.65 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3 | $1.50 | $0.05 | — | $29.09 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $24.74 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $20.51 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.51 | ▼ 09:30 equity $10,399.63 vs yday $10,461.99 (-62.36) | 09:30 open · cash $20.51 (unchanged overnight, no fees) · equity $10,399.63 vs prior close $10,461.99 (-62.36) because holdings re-marked: TPG×98 yday $53.03 → 09:30 $52.67 -35.28; VOR×227 yday $23.03 → 09:30 $22.91 -27.24; LDI×4 yday $0.90 → 09:30 $0.91 +0.04; BTBT×3 yday $1.57 → 09:30 $1.52 -0.15; ANGX×1 yday $4.37 → 09:30 $4.60 +0.23; HYLN×1 yday $4.06 → 09:30 $4.10 +0.04 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.51 | ▼ 09:30 equity $10,290.95 vs yday $10,334.34 (-43.39) | 09:30 open · cash $20.51 (unchanged overnight, no fees) · equity $10,290.95 vs prior close $10,334.34 (-43.39) because holdings re-marked: TPG×98 yday $51.77 → 09:30 $51.77 +0.00; VOR×227 yday $23.01 → 09:30 $22.82 -43.13; LDI×4 yday $0.88 → 09:30 $0.87 -0.02; BTBT×3 yday $1.60 → 09:30 $1.54 -0.18; ANGX×1 yday $4.71 → 09:30 $4.79 +0.08; HYLN×1 yday $4.09 → 09:30 $3.95 -0.14 | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 98 | $51.77 | $2.34 | $+107.76 | $5,091.63 | ▲ +107.76 after sell → book $10,288.61; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 227 | $22.82 | $3.01 | $+177.93 | $10,268.76 | ▲ +177.93 after sell → book $10,285.60; vs 09:30 mark -3.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,268.76 | ▼ 09:30 equity $10,285.20 vs yday $10,285.26 (-0.06) | 09:30 open · cash $10,268.76 (unchanged overnight, no fees) · equity $10,285.20 vs prior close $10,285.26 (-0.06) because holdings re-marked: LDI×4 yday $0.86 → 09:30 $0.88 +0.09; BTBT×3 yday $1.45 → 09:30 $1.42 -0.09; ANGX×1 yday $4.85 → 09:30 $4.79 -0.06; HYLN×1 yday $3.86 → 09:30 $3.87 +0.01 | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 4 | $0.88 | $0.07 | $-0.34 | $10,272.22 | ▼ -0.34 after sell → book $10,285.14; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 3 | $1.42 | $0.07 | $-0.37 | $10,276.40 | ▼ -0.37 after sell → book $10,285.06; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 1 | $4.79 | $0.07 | $+0.36 | $10,281.12 | ▲ +0.36 after sell → book $10,284.99; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 1 | $3.87 | $0.06 | $-0.42 | $10,284.93 | ▼ -0.42 after sell → book $10,284.93; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,284.93 | ▲ 09:30 equity $10,284.93 vs yday $10,284.93 (+0.00) | 09:30 open · cash $10,284.93 · no holdings · equity $10,284.93 vs prior close $10,284.93 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,008.66 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1285.62 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,732.48 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1285.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 222 | $5.77 | $2.86 | — | $6,448.68 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1285.62 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $5,170.55 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1285.62 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $3,894.34 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1285.62 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 734 | $1.75 | $9.47 | — | $2,600.37 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1285.62 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 172 | $7.45 | $2.51 | — | $1,316.46 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1285.62 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 119 | $10.77 | $2.35 | — | $32.48 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1285.62 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.48 | ▲ 09:30 equity $10,631.26 vs yday $10,364.66 (+266.60) | 09:30 open · cash $32.48 (unchanged overnight, no fees) · equity $10,631.26 vs prior close $10,364.66 (+266.60) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; HDSN×222 yday $5.57 → 09:30 $5.67 +22.20; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×734 yday $1.75 → 09:30 $1.79 +29.36; DNA×172 yday $6.96 → 09:30 $7.09 +22.36; EXK×119 yday $10.97 → 09:30 $11.34 +44.03 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 2 | $1.66 | $0.04 | — | $29.13 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $4.06 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 4 | $0.86 | $0.05 | — | $25.62 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $4.06 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QTRX` | 1 | $3.11 | $0.03 | — | $22.48 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $4.06 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.48 | ▲ 09:30 equity $10,734.98 vs yday $10,617.83 (+117.15) | 09:30 open · cash $22.48 (unchanged overnight, no fees) · equity $10,734.98 vs prior close $10,617.83 (+117.15) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; HDSN×222 yday $5.63 → 09:30 $5.69 +13.32; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×734 yday $1.84 → 09:30 $1.86 +14.68; DNA×172 yday $7.40 → 09:30 $7.26 -24.08; EXK×119 yday $10.62 → 09:30 $11.01 +46.41; BTBT×2 yday $1.53 → 09:30 $1.55 +0.04; ORBS×4 yday $0.88 → 09:30 $0.89 +0.04; QTRX×1 yday $2.99 → 09:30 $2.98 -0.01 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.48 | ▼ 09:30 equity $10,553.29 vs yday $10,554.71 (-1.42) | 09:30 open · cash $22.48 (unchanged overnight, no fees) · equity $10,553.29 vs prior close $10,554.71 (-1.42) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; HDSN×222 yday $5.57 → 09:30 $5.53 -8.88; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×734 yday $1.90 → 09:30 $1.91 +7.34; DNA×172 yday $6.98 → 09:30 $6.82 -27.52; EXK×119 yday $10.74 → 09:30 $10.72 -2.38; BTBT×2 yday $1.56 → 09:30 $1.55 -0.02; ORBS×4 yday $0.85 → 09:30 $0.85 +0.00; QTRX×1 yday $2.76 → 09:30 $2.80 +0.04 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.73 | $2.20 | $+6.79 | $1,305.54 | ▲ +6.79 after sell → book $10,551.09; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.95 | $2.05 | $+65.08 | $2,646.79 | ▲ +65.08 after sell → book $10,549.04; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 222 | $5.53 | $2.91 | $-59.05 | $3,871.54 | ▼ -59.05 after sell → book $10,546.13; vs 09:30 mark -2.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.63 | $2.21 | $+125.61 | $5,275.28 | ▲ +125.61 after sell → book $10,543.92; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.76 | $2.14 | $+130.33 | $6,681.82 | ▲ +130.33 after sell → book $10,541.78; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 734 | $1.91 | $9.60 | $+98.37 | $8,074.16 | ▲ +98.37 after sell → book $10,532.18; vs 09:30 mark -9.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 172 | $6.82 | $2.54 | $-113.41 | $9,244.66 | ▼ -113.41 after sell → book $10,529.64; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 119 | $10.72 | $2.38 | $-10.67 | $10,517.96 | ▼ -10.67 after sell → book $10,527.26; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.47 | $2.05 | — | $9,225.04 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ret5=+9.2; leftover $1314.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 158 | $8.28 | $2.46 | — | $7,914.33 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1314.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $6,624.61 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+6.1; leftover $1314.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 811 | $1.62 | $10.46 | — | $5,300.33 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1314.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 88 | $14.86 | $2.25 | — | $3,990.40 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1314.74 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 206 | $6.38 | $2.66 | — | $2,673.46 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1314.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `JANX` | 70 | $18.52 | $2.20 | — | $1,374.86 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ret5=+7.9; leftover $1314.74 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 98 | $13.30 | $2.28 | — | $69.17 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ret5=+9.5; leftover $1314.74 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.17 | ▲ 09:30 equity $10,576.41 vs yday $10,576.41 (+0.00) | 09:30 open · cash $69.17 (unchanged overnight, no fees) · equity $10,576.41 vs prior close $10,576.41 (+0.00) because holdings re-marked: BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; ORBS×4 yday $0.84 → 09:30 $0.84 +0.00; QTRX×1 yday $2.80 → 09:30 $2.80 +0.00; INSP×21 yday $61.47 → 09:30 $61.47 +0.00; CRMD×158 yday $8.28 → 09:30 $8.28 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×811 yday $1.61 → 09:30 $1.61 +0.00; ALIT×88 yday $14.87 → 09:30 $14.87 +0.00; ZURA×206 yday $6.50 → 09:30 $6.50 +0.00; JANX×70 yday $18.99 → 09:30 $18.99 +0.00; KURA×98 yday $13.58 → 09:30 $13.58 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.17 | ▲ 09:30 equity $10,606.90 vs yday $10,500.88 (+106.02) | 09:30 open · cash $69.17 (unchanged overnight, no fees) · equity $10,606.90 vs prior close $10,500.88 (+106.02) because holdings re-marked: BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; ORBS×4 yday $0.84 → 09:30 $0.80 -0.16; QTRX×1 yday $2.80 → 09:30 $2.83 +0.03; INSP×21 yday $61.47 → 09:30 $60.07 -29.40; CRMD×158 yday $8.28 → 09:30 $8.60 +50.56; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×811 yday $1.61 → 09:30 $1.75 +113.54; ALIT×88 yday $14.87 → 09:30 $14.85 -1.76; ZURA×206 yday $6.50 → 09:30 $6.13 -76.22; JANX×70 yday $18.99 → 09:30 $18.59 -28.00; KURA×98 yday $13.58 → 09:30 $13.63 +4.90 | — |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 2 | $1.53 | $0.06 | $-0.36 | $72.18 | ▼ -0.36 after sell → book $10,606.85; vs 09:30 mark -0.05 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ORBS` | 4 | $0.80 | $0.06 | $-0.37 | $75.31 | ▼ -0.37 after sell → book $10,606.78; vs 09:30 mark -0.07 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `QTRX` | 1 | $2.83 | $0.05 | $-0.37 | $78.09 | ▼ -0.37 after sell → book $10,606.73; vs 09:30 mark -0.05 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 3 | $2.59 | $0.09 | — | $70.24 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+4.2; leftover $9.76 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.24 | ▲ 09:30 equity $10,530.21 vs yday $10,466.94 (+63.27) | 09:30 open · cash $70.24 (unchanged overnight, no fees) · equity $10,530.21 vs prior close $10,466.94 (+63.27) because holdings re-marked: INSP×21 yday $61.80 → 09:30 $62.10 +6.30; CRMD×158 yday $8.39 → 09:30 $8.49 +15.80; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; BMEA×811 yday $1.71 → 09:30 $1.74 +24.33; ALIT×88 yday $14.33 → 09:30 $14.54 +18.48; ZURA×206 yday $5.99 → 09:30 $6.02 +6.18; JANX×70 yday $18.89 → 09:30 $19.00 +7.70; KURA×98 yday $13.06 → 09:30 $12.98 -7.84; SLI×3 yday $2.61 → 09:30 $2.60 -0.03 | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 21 | $62.10 | $2.07 | $+9.10 | $1,372.26 | ▲ +9.10 after sell → book $10,528.13; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 158 | $8.49 | $2.50 | $+28.22 | $2,711.18 | ▲ +28.22 after sell → book $10,525.63; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-17.91 | $3,982.99 | ▼ -17.91 after sell → book $10,523.61; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 811 | $1.74 | $10.61 | $+76.25 | $5,383.52 | ▲ +76.25 after sell → book $10,513.00; vs 09:30 mark -10.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALIT` | 88 | $14.54 | $2.28 | $-32.69 | $6,660.77 | ▼ -32.69 after sell → book $10,510.73; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 206 | $6.02 | $2.70 | $-79.52 | $7,898.18 | ▼ -79.52 after sell → book $10,508.02; vs 09:30 mark -2.71 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `JANX` | 70 | $19.00 | $2.22 | $+29.18 | $9,225.96 | ▲ +29.18 after sell → book $10,505.80; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 98 | $12.98 | $2.31 | $-35.95 | $10,495.69 | ▼ -35.95 after sell → book $10,503.49; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 36 | $41.44 | $2.10 | — | $9,001.75 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+1.8; leftover $1499.38 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 103 | $14.42 | $2.30 | — | $7,514.19 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+1.1; leftover $1499.38 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 10 | $144.70 | $2.02 | — | $6,065.17 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1499.38 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 81 | $18.50 | $2.23 | — | $4,564.44 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1499.38 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 1086 | $1.38 | $14.01 | — | $3,051.75 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1499.38 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 80 | $18.68 | $2.23 | — | $1,555.12 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; ret5=+0.2; leftover $1499.38 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 717 | $2.09 | $9.25 | — | $47.34 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; ret5=+3.3; leftover $1499.38 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.34 | ▼ 09:30 equity $10,407.44 vs yday $10,421.64 (-14.20) | 09:30 open · cash $47.34 (unchanged overnight, no fees) · equity $10,407.44 vs prior close $10,421.64 (-14.20) because holdings re-marked: SLI×3 yday $2.64 → 09:30 $2.51 -0.39; RRC×36 yday $41.64 → 09:30 $41.11 -19.08; CRK×103 yday $14.62 → 09:30 $14.56 -6.18; ANF×10 yday $145.75 → 09:30 $148.67 +29.20; BZ×81 yday $18.00 → 09:30 $17.89 -8.91; LVWR×1086 yday $1.36 → 09:30 $1.37 +10.86; BBWI×80 yday $18.65 → 09:30 $19.30 +52.00; CRDL×717 yday $2.06 → 09:30 $1.96 -71.70 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.34 | ▼ 09:30 equity $10,121.14 vs yday $10,394.34 (-273.20) | 09:30 open · cash $47.34 (unchanged overnight, no fees) · equity $10,121.14 vs prior close $10,394.34 (-273.20) because holdings re-marked: SLI×3 yday $2.51 → 09:30 $2.70 +0.57; RRC×36 yday $41.78 → 09:30 $41.32 -16.56; CRK×103 yday $14.51 → 09:30 $14.31 -20.60; ANF×10 yday $149.28 → 09:30 $142.47 -68.10; BZ×81 yday $17.90 → 09:30 $17.37 -42.93; LVWR×1086 yday $1.34 → 09:30 $1.22 -130.32; BBWI×80 yday $19.22 → 09:30 $19.10 -9.60; CRDL×717 yday $1.96 → 09:30 $1.98 +14.34 | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 3 | $2.70 | $0.11 | $+0.13 | $55.33 | ▲ +0.13 after sell → book $10,121.03; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.33 | ▲ 09:30 equity $10,200.04 vs yday $10,127.46 (+72.58) | 09:30 open · cash $55.33 (unchanged overnight, no fees) · equity $10,200.04 vs prior close $10,127.46 (+72.58) because holdings re-marked: RRC×36 yday $41.32 → 09:30 $41.94 +22.32; CRK×103 yday $14.90 → 09:30 $15.82 +94.76; ANF×10 yday $143.00 → 09:30 $142.00 -10.00; BZ×81 yday $17.17 → 09:30 $17.29 +9.72; LVWR×1086 yday $1.18 → 09:30 $1.19 +10.86; BBWI×80 yday $19.10 → 09:30 $18.77 -26.40; CRDL×717 yday $1.98 → 09:30 $1.94 -28.68 | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 36 | $41.94 | $2.12 | $+13.78 | $1,563.05 | ▲ +13.78 after sell → book $10,197.92; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 103 | $15.82 | $2.33 | $+139.57 | $3,190.18 | ▲ +139.57 after sell → book $10,195.59; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 10 | $142.00 | $2.04 | $-31.06 | $4,608.14 | ▼ -31.06 after sell → book $10,193.55; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 81 | $17.29 | $2.26 | $-102.50 | $6,006.37 | ▼ -102.50 after sell → book $10,191.29; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LVWR` | 1086 | $1.19 | $14.20 | $-234.55 | $7,284.51 | ▼ -234.55 after sell → book $10,177.09; vs 09:30 mark -14.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 80 | $18.77 | $2.26 | $+2.71 | $8,783.86 | ▲ +2.71 after sell → book $10,174.84; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRDL` | 717 | $1.94 | $9.38 | $-126.18 | $10,165.46 | ▼ -126.18 after sell → book $10,165.46; vs 09:30 mark -9.38 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,165.46 | ▲ 09:30 equity $10,165.46 vs yday $10,165.46 (+0.00) | 09:30 open · cash $10,165.46 · no holdings · equity $10,165.46 vs prior close $10,165.46 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $41.31 | $2.08 | — | $8,924.08 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1270.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 165 | $7.70 | $2.48 | — | $7,651.10 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1270.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $6,389.68 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1270.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1041 | $1.22 | $13.43 | — | $5,106.23 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1270.68 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 80 | $15.70 | $2.23 | — | $3,848.00 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1270.68 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 55 | $22.78 | $2.15 | — | $2,592.94 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1270.68 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $56.78 | $2.06 | — | $1,341.73 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ret5=+0.3; leftover $1270.68 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 588 | $2.16 | $7.59 | — | $64.06 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1270.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.06 | ▲ 09:30 equity $10,904.87 vs yday $10,781.99 (+122.88) | 09:30 open · cash $64.06 (unchanged overnight, no fees) · equity $10,904.87 vs prior close $10,781.99 (+122.88) because holdings re-marked: HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; VSTM×165 yday $8.02 → 09:30 $8.03 +1.65; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1041 yday $1.69 → 09:30 $1.78 +93.69; CRK×80 yday $15.54 → 09:30 $15.45 -7.20; MMED×55 yday $23.76 → 09:30 $23.88 +6.60; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CRDL×588 yday $2.17 → 09:30 $2.18 +5.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 4 | $1.95 | $0.09 | — | $56.17 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $9.15 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SGLD` | 1 | $6.48 | $0.07 | — | $49.62 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,yday_mover; ret5=+0.0; leftover $9.15 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 4.68 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 4.68 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 4.68 < 1 share @ 120.00 |
| 2026-08-14 | `SLG` | cash | leftover split 4.68 < 1 share @ 57.61 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 2.56 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 2.56 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 2.56 < 1 share @ 202.70 |
| 2026-08-17 | `TGB` | cash | leftover split 2.56 < 1 share @ 8.46 |
| 2026-08-17 | `DNN` | cash | leftover split 2.56 < 1 share @ 3.24 |
| 2026-08-17 | `OCC` | cash | leftover split 2.56 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 2.56 < 1 share @ 16.20 |
| 2026-08-17 | `NEWP` | cash | leftover split 2.56 < 1 share @ 6.94 |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MXL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 4.06 < 1 share @ 59.72 |
| 2026-08-21 | `EMBC` | cash | leftover split 4.06 < 1 share @ 5.43 |
| 2026-08-21 | `TXG` | cash | leftover split 4.06 < 1 share @ 64.39 |
| 2026-08-21 | `DXYZ` | cash | leftover split 4.06 < 1 share @ 34.89 |
| 2026-08-21 | `BEKE` | cash | leftover split 4.06 < 1 share @ 17.93 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BTBT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ORBS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `QTRX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `JANX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `BZ` | no_price | no 09:30 open |
| 2026-08-26 | `CNTN` | no_price | no 09:30 open |
| 2026-08-26 | `OSUR` | no_price | no 09:30 open |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `JANX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 9.76 < 1 share @ 40.72 |
| 2026-08-27 | `CRK` | cash | leftover split 9.76 < 1 share @ 14.09 |
| 2026-08-27 | `TX` | cash | leftover split 9.76 < 1 share @ 55.20 |
| 2026-08-27 | `DLO` | cash | leftover split 9.76 < 1 share @ 15.60 |
| 2026-08-27 | `GEN` | cash | leftover split 9.76 < 1 share @ 28.89 |
| 2026-08-27 | `MRVL` | cash | leftover split 9.76 < 1 share @ 240.00 |
| 2026-08-27 | `PGY` | cash | leftover split 9.76 < 1 share @ 21.97 |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OHI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 9.15 < 1 share @ 266.94 |
| 2026-09-04 | `BVS` | cash | leftover split 9.15 < 1 share @ 14.50 |
| 2026-09-04 | `MLYS` | cash | leftover split 9.15 < 1 share @ 29.15 |
| 2026-09-04 | `FMC` | cash | leftover split 9.15 < 1 share @ 13.30 |
| 2026-09-04 | `TARS` | cash | leftover split 9.15 < 1 share @ 82.76 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `HRMY` | 30 | 2026-09-03 @ $41.31 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1270.68 |
| `VSTM` | 165 | 2026-09-03 @ $7.70 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1270.68 |
| `RVTY` | 10 | 2026-09-03 @ $125.94 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1270.68 |
| `GPRO` | 1041 | 2026-09-03 @ $1.22 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1270.68 |
| `CRK` | 80 | 2026-09-03 @ $15.70 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1270.68 |
| `MMED` | 55 | 2026-09-03 @ $22.78 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1270.68 |
| `EIX` | 22 | 2026-09-03 @ $56.78 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ret5=+0.3; leftover $1270.68 |
| `CRDL` | 588 | 2026-09-03 @ $2.16 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1270.68 |
| `BAK` | 4 | 2026-09-04 @ $1.95 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $9.15 |
| `SGLD` | 1 | 2026-09-04 @ $6.48 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,yday_mover; ret5=+0.0; leftover $9.15 |
