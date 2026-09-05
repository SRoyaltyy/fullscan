# Factor mine action — `union_ab_g_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ ab_g, no 🚨

Cash book **+0.79%** ($10,079) · signal-only (no cash/fees) was +12.84%. Starts YES **13/17**. Fills 70 · skips 103 · realized $-152.82.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ab=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $81.52.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $186.91 | $10,208.28 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $186.91 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 | $10,475.50 | +267.22 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $78.42 | $10,474.93 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | 09:30 open · cash $186.91 (unchanged overnight, no fees) · equity $10,475.50 vs prior close $10,208.28 (+267.22) because holdings re-marked: AG×60 yday $21.19 → 09:30 $21.90 +42.60; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×60 yday $21.11 → 09:30 $21.75 +38.40; HDSN×216 yday $5.57 → 09:30 $5.67 +21.60; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×714 yday $1.75 → 09:30 $1.79 +28.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 |
| 2026-08-24 | -5.17 | $78.42 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,599.85 | +124.92 | — | — | $78.42 | $10,446.76 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | 09:30 open · cash $78.42 (unchanged overnight, no fees) · equity $10,599.85 vs prior close $10,474.93 (+124.92) because holdings re-marked: AG×60 yday $21.09 → 09:30 $21.47 +22.80; BHP×13 yday $97.03 → 09:30 $97.34 +4.03; CDE×60 yday $20.97 → 09:30 $21.26 +17.40; HDSN×216 yday $5.63 → 09:30 $5.69 +12.96; IAG×63 yday $21.14 → 09:30 $21.44 +18.90; KGC×42 yday $32.76 → 09:30 $33.21 +18.90; NFGC×714 yday $1.84 → 09:30 $1.86 +14.28; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×9 yday $2.41 → 09:30 $2.36 -0.45; CRDL×12 yday $1.86 → 09:30 $1.87 +0.12; CYPH×17 yday $1.42 → 09:30 $1.83 +6.97 |
| 2026-08-25 | +1.80 | $78.42 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,515.37 | +68.61 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, ALVO | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $7.51 | $10,460.12 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, MOS×54, OCUL×118, INSP×21, CRMD×156, RZLT×247, HCA×3, BMEA×800, ALVO×248 | 09:30 open · cash $78.42 (unchanged overnight, no fees) · equity $10,515.37 vs prior close $10,446.76 (+68.61) because holdings re-marked: AG×60 yday $20.57 → 09:30 $20.73 +9.60; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; CDE×60 yday $20.49 → 09:30 $20.85 +21.60; HDSN×216 yday $5.57 → 09:30 $5.53 -8.64; IAG×63 yday $21.36 → 09:30 $21.63 +17.01; KGC×42 yday $32.47 → 09:30 $32.76 +12.18; NFGC×714 yday $1.90 → 09:30 $1.91 +7.14; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×9 yday $2.38 → 09:30 $2.32 -0.54; CRDL×12 yday $1.80 → 09:30 $1.90 +1.20; CYPH×17 yday $1.64 → 09:30 $1.70 +1.02 |
| 2026-08-26 | +2.02 | $7.51 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, MOS×54, OCUL×118, INSP×21, CRMD×156, RZLT×247, HCA×3, BMEA×800, ALVO×248 | $10,460.12 | +0.00 | — | — | $7.51 | $10,462.68 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, MOS×54, OCUL×118, INSP×21, CRMD×156, RZLT×247, HCA×3, BMEA×800, ALVO×248 | 09:30 open · cash $7.51 (unchanged overnight, no fees) · equity $10,460.12 vs prior close $10,460.12 (+0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×9 yday $2.34 → 09:30 $2.34 +0.00; CRDL×12 yday $1.90 → 09:30 $1.90 +0.00; CYPH×17 yday $1.64 → 09:30 $1.64 +0.00; MOS×54 yday $23.75 → 09:30 $23.75 +0.00; OCUL×118 yday $10.92 → 09:30 $10.92 +0.00; INSP×21 yday $61.47 → 09:30 $61.47 +0.00; CRMD×156 yday $8.28 → 09:30 $8.28 +0.00; RZLT×247 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×800 yday $1.61 → 09:30 $1.61 +0.00; ALVO×248 yday $5.25 → 09:30 $5.25 +0.00 |
| 2026-08-27 | — | $7.51 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, MOS×54, OCUL×118, INSP×21, CRMD×156, RZLT×247, HCA×3, BMEA×800, ALVO×248 | $10,500.72 | +38.04 | CRK, SLI, GGB | AUPH, ARCT, AUTL, CRDL, CYPH | $78.82 | $10,421.00 | MOS×54, OCUL×118, INSP×21, CRMD×156, RZLT×247, HCA×3, BMEA×800, ALVO×248, CRK×1, SLI×6, GGB×4 | 09:30 open · cash $7.51 (unchanged overnight, no fees) · equity $10,500.72 vs prior close $10,462.68 (+38.04) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×9 yday $2.34 → 09:30 $2.41 +0.63; CRDL×12 yday $1.90 → 09:30 $2.03 +1.56; CYPH×17 yday $1.64 → 09:30 $1.60 -0.68; MOS×54 yday $23.75 → 09:30 $24.84 +58.86; OCUL×118 yday $10.92 → 09:30 $10.79 -15.34; INSP×21 yday $61.47 → 09:30 $60.07 -29.40; CRMD×156 yday $8.28 → 09:30 $8.60 +49.92; RZLT×247 yday $5.29 → 09:30 $5.01 -69.16; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×800 yday $1.61 → 09:30 $1.75 +112.00; ALVO×248 yday $5.25 → 09:30 $4.98 -66.96 |
| 2026-08-28 | +0.75 | $78.82 | MOS×54, OCUL×118, INSP×21, CRMD×156, RZLT×247, HCA×3, BMEA×800, ALVO×248, CRK×1, SLI×6, GGB×4 | $10,434.36 | +13.36 | RRC, ANF, BZ, SMTC, GRRR | OCUL, INSP, CRMD, RZLT, HCA, BMEA, ALVO | $146.53 | $10,243.09 | MOS×54, CRK×1, SLI×6, GGB×4, RRC×43, ANF×12, BZ×97, SMTC×12, GRRR×113 | 09:30 open · cash $78.82 (unchanged overnight, no fees) · equity $10,434.36 vs prior close $10,421.00 (+13.36) because holdings re-marked: MOS×54 yday $24.16 → 09:30 $24.00 -8.64; OCUL×118 yday $10.77 → 09:30 $10.63 -16.52; INSP×21 yday $61.80 → 09:30 $62.10 +6.30; CRMD×156 yday $8.39 → 09:30 $8.49 +15.60; RZLT×247 yday $5.04 → 09:30 $5.07 +7.41; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; BMEA×800 yday $1.71 → 09:30 $1.74 +24.00; ALVO×248 yday $4.91 → 09:30 $4.88 -7.44; CRK×1 yday $14.50 → 09:30 $14.42 -0.08; SLI×6 yday $2.61 → 09:30 $2.60 -0.06; GGB×4 yday $4.46 → 09:30 $4.57 +0.44 |
| 2026-08-31 | -5.85 | $146.53 | MOS×54, CRK×1, SLI×6, GGB×4, RRC×43, ANF×12, BZ×97, SMTC×12, GRRR×113 | $9,978.59 | -264.50 | — | MOS | $1,426.85 | $9,993.90 | CRK×1, SLI×6, GGB×4, RRC×43, ANF×12, BZ×97, SMTC×12, GRRR×113 | 09:30 open · cash $146.53 (unchanged overnight, no fees) · equity $9,978.59 vs prior close $10,243.09 (-264.50) because holdings re-marked: MOS×54 yday $23.76 → 09:30 $23.75 -0.54; CRK×1 yday $14.62 → 09:30 $14.56 -0.06; SLI×6 yday $2.64 → 09:30 $2.51 -0.78; GGB×4 yday $4.70 → 09:30 $4.55 -0.60; RRC×43 yday $41.64 → 09:30 $41.11 -22.79; ANF×12 yday $145.75 → 09:30 $148.67 +35.04; BZ×97 yday $18.00 → 09:30 $17.89 -10.67; SMTC×12 yday $142.43 → 09:30 $133.04 -112.68; GRRR×113 yday $15.66 → 09:30 $14.32 -151.42 |
| 2026-09-01 | -6.30 | $1,426.85 | CRK×1, SLI×6, GGB×4, RRC×43, ANF×12, BZ×97, SMTC×12, GRRR×113 | $9,927.54 | -66.36 | — | CRK, SLI, GGB | $1,475.22 | $9,859.87 | RRC×43, ANF×12, BZ×97, SMTC×12, GRRR×113 | 09:30 open · cash $1,426.85 (unchanged overnight, no fees) · equity $9,927.54 vs prior close $9,993.90 (-66.36) because holdings re-marked: CRK×1 yday $14.51 → 09:30 $14.31 -0.20; SLI×6 yday $2.51 → 09:30 $2.70 +1.14; GGB×4 yday $4.55 → 09:30 $4.61 +0.24; RRC×43 yday $41.78 → 09:30 $41.32 -19.78; ANF×12 yday $149.28 → 09:30 $142.47 -81.72; BZ×97 yday $17.90 → 09:30 $17.37 -51.41; SMTC×12 yday $132.54 → 09:30 $131.65 -10.68; GRRR×113 yday $14.20 → 09:30 $15.05 +96.05 |
| 2026-09-02 | -3.83 | $1,475.22 | RRC×43, ANF×12, BZ×97, SMTC×12, GRRR×113 | $9,858.08 | -1.79 | — | RRC, ANF, BZ, SMTC, GRRR | $9,847.17 | $9,847.17 | — | 09:30 open · cash $1,475.22 (unchanged overnight, no fees) · equity $9,858.08 vs prior close $9,859.87 (-1.79) because holdings re-marked: RRC×43 yday $41.32 → 09:30 $41.94 +26.66; ANF×12 yday $143.00 → 09:30 $142.00 -12.00; BZ×97 yday $17.17 → 09:30 $17.29 +11.64; SMTC×12 yday $129.50 → 09:30 $127.63 -22.44; GRRR×113 yday $14.80 → 09:30 $14.75 -5.65 |
| 2026-09-03 | -0.90 | $9,847.17 | — | $9,847.17 | -0.00 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MMED, SLN | — | $172.75 | $10,196.54 | ATRC×24, HRMY×29, CABA×376, VSTM×159, RVTY×9, CRK×78, MMED×54, SLN×83 | 09:30 open · cash $9,847.17 · no holdings · equity $9,847.17 vs prior close $9,847.17 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $172.75 | ATRC×24, HRMY×29, CABA×376, VSTM×159, RVTY×9, CRK×78, MMED×54, SLN×83 | $10,247.71 | +51.17 | NVAX, BVS, BAK, SLBT | — | $81.52 | $10,079.05 | ATRC×24, HRMY×29, CABA×376, VSTM×159, RVTY×9, CRK×78, MMED×54, SLN×83, NVAX×2, BVS×1, BAK×14, SLBT×9 | 09:30 open · cash $172.75 (unchanged overnight, no fees) · equity $10,247.71 vs prior close $10,196.54 (+51.17) because holdings re-marked: ATRC×24 yday $52.59 → 09:30 $52.88 +6.96; HRMY×29 yday $42.86 → 09:30 $42.93 +2.03; CABA×376 yday $3.57 → 09:30 $3.63 +22.56; VSTM×159 yday $8.02 → 09:30 $8.03 +1.59; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×78 yday $15.54 → 09:30 $15.45 -7.02; MMED×54 yday $23.76 → 09:30 $23.88 +6.48; SLN×83 yday $14.79 → 09:30 $14.85 +4.98 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.91 | ▲ 09:30 equity $10,475.50 vs yday $10,208.28 (+267.22) | 09:30 open · cash $186.91 (unchanged overnight, no fees) · equity $10,475.50 vs prior close $10,208.28 (+267.22) because holdings re-marked: AG×60 yday $21.19 → 09:30 $21.90 +42.60; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×60 yday $21.11 → 09:30 $21.75 +38.40; HDSN×216 yday $5.57 → 09:30 $5.67 +21.60; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×714 yday $1.75 → 09:30 $1.79 +28.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $169.53 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $147.04 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $124.56 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $101.13 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 17 | $1.32 | $0.28 | — | $78.42 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.42 | ▲ 09:30 equity $10,599.85 vs yday $10,474.93 (+124.92) | 09:30 open · cash $78.42 (unchanged overnight, no fees) · equity $10,599.85 vs prior close $10,474.93 (+124.92) because holdings re-marked: AG×60 yday $21.09 → 09:30 $21.47 +22.80; BHP×13 yday $97.03 → 09:30 $97.34 +4.03; CDE×60 yday $20.97 → 09:30 $21.26 +17.40; HDSN×216 yday $5.63 → 09:30 $5.69 +12.96; IAG×63 yday $21.14 → 09:30 $21.44 +18.90; KGC×42 yday $32.76 → 09:30 $33.21 +18.90; NFGC×714 yday $1.84 → 09:30 $1.86 +14.28; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×9 yday $2.41 → 09:30 $2.36 -0.45; CRDL×12 yday $1.86 → 09:30 $1.87 +0.12; CYPH×17 yday $1.42 → 09:30 $1.83 +6.97 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.42 | ▲ 09:30 equity $10,515.37 vs yday $10,446.76 (+68.61) | 09:30 open · cash $78.42 (unchanged overnight, no fees) · equity $10,515.37 vs prior close $10,446.76 (+68.61) because holdings re-marked: AG×60 yday $20.57 → 09:30 $20.73 +9.60; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; CDE×60 yday $20.49 → 09:30 $20.85 +21.60; HDSN×216 yday $5.57 → 09:30 $5.53 -8.64; IAG×63 yday $21.36 → 09:30 $21.63 +17.01; KGC×42 yday $32.47 → 09:30 $32.76 +12.18; NFGC×714 yday $1.90 → 09:30 $1.91 +7.14; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×9 yday $2.38 → 09:30 $2.32 -0.54; CRDL×12 yday $1.80 → 09:30 $1.90 +1.20; CYPH×17 yday $1.64 → 09:30 $1.70 +1.02 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 60 | $20.73 | $2.19 | $+6.44 | $1,320.03 | ▲ +6.44 after sell → book $10,513.18; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $2,565.33 | ▲ +60.14 after sell → book $10,511.13; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 60 | $20.85 | $2.19 | $+7.64 | $3,814.14 | ▲ +7.64 after sell → book $10,508.94; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 216 | $5.53 | $2.83 | $-57.46 | $5,005.79 | ▼ -57.46 after sell → book $10,506.11; vs 09:30 mark -2.83 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 63 | $21.63 | $2.20 | $+121.62 | $6,366.28 | ▲ +121.62 after sell → book $10,503.91; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 42 | $32.76 | $2.14 | $+127.21 | $7,740.06 | ▲ +127.21 after sell → book $10,501.77; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 714 | $1.91 | $9.34 | $+95.69 | $9,094.46 | ▲ +95.69 after sell → book $10,492.43; vs 09:30 mark -9.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $10,372.43 | ▲ +119.63 after sell → book $10,490.40; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 54 | $24.00 | $2.15 | — | $9,074.27 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ⚪; ret5=+13.0; leftover $1296.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 118 | $10.92 | $2.34 | — | $7,783.37 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+10.4; leftover $1296.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.47 | $2.05 | — | $6,490.45 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+9.2; leftover $1296.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 156 | $8.28 | $2.46 | — | $5,196.31 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1296.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 247 | $5.23 | $3.19 | — | $3,901.31 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+10.7; leftover $1296.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $2,611.59 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+6.1; leftover $1296.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 800 | $1.62 | $10.32 | — | $1,305.27 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1296.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 248 | $5.22 | $3.20 | — | $7.51 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1296.55 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.51 | ▲ 09:30 equity $10,460.12 vs yday $10,460.12 (+0.00) | 09:30 open · cash $7.51 (unchanged overnight, no fees) · equity $10,460.12 vs prior close $10,460.12 (+0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×9 yday $2.34 → 09:30 $2.34 +0.00; CRDL×12 yday $1.90 → 09:30 $1.90 +0.00; CYPH×17 yday $1.64 → 09:30 $1.64 +0.00; MOS×54 yday $23.75 → 09:30 $23.75 +0.00; OCUL×118 yday $10.92 → 09:30 $10.92 +0.00; INSP×21 yday $61.47 → 09:30 $61.47 +0.00; CRMD×156 yday $8.28 → 09:30 $8.28 +0.00; RZLT×247 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×800 yday $1.61 → 09:30 $1.61 +0.00; ALVO×248 yday $5.25 → 09:30 $5.25 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.51 | ▲ 09:30 equity $10,500.72 vs yday $10,462.68 (+38.04) | 09:30 open · cash $7.51 (unchanged overnight, no fees) · equity $10,500.72 vs prior close $10,462.68 (+38.04) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×9 yday $2.34 → 09:30 $2.41 +0.63; CRDL×12 yday $1.90 → 09:30 $2.03 +1.56; CYPH×17 yday $1.64 → 09:30 $1.60 -0.68; MOS×54 yday $23.75 → 09:30 $24.84 +58.86; OCUL×118 yday $10.92 → 09:30 $10.79 -15.34; INSP×21 yday $61.47 → 09:30 $60.07 -29.40; CRMD×156 yday $8.28 → 09:30 $8.60 +49.92; RZLT×247 yday $5.29 → 09:30 $5.01 -69.16; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×800 yday $1.61 → 09:30 $1.75 +112.00; ALVO×248 yday $5.25 → 09:30 $4.98 -66.96 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $23.93 | ▼ -0.96 after sell → book $10,500.54; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $54.29 | ▲ +7.88 after sell → book $10,500.20; vs 09:30 mark -0.34 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 9 | $2.41 | $0.26 | $-1.05 | $75.72 | ▼ -1.05 after sell → book $10,499.94; vs 09:30 mark -0.26 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 12 | $2.03 | $0.30 | $+0.63 | $99.78 | ▲ +0.63 after sell → book $10,499.64; vs 09:30 mark -0.30 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 17 | $1.60 | $0.34 | $+4.14 | $126.64 | ▲ +4.14 after sell → book $10,499.30; vs 09:30 mark -0.34 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 1 | $14.09 | $0.14 | — | $112.40 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+1.1; leftover $18.09 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 6 | $2.59 | $0.17 | — | $96.69 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+4.2; leftover $18.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 4 | $4.42 | $0.19 | — | $78.82 | — | union ∩ ab_g, no 🚨; gate ab=good; list mover_buy; 🔵; ret5=-8.6; leftover $18.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.82 | ▲ 09:30 equity $10,434.36 vs yday $10,421.00 (+13.36) | 09:30 open · cash $78.82 (unchanged overnight, no fees) · equity $10,434.36 vs prior close $10,421.00 (+13.36) because holdings re-marked: MOS×54 yday $24.16 → 09:30 $24.00 -8.64; OCUL×118 yday $10.77 → 09:30 $10.63 -16.52; INSP×21 yday $61.80 → 09:30 $62.10 +6.30; CRMD×156 yday $8.39 → 09:30 $8.49 +15.60; RZLT×247 yday $5.04 → 09:30 $5.07 +7.41; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; BMEA×800 yday $1.71 → 09:30 $1.74 +24.00; ALVO×248 yday $4.91 → 09:30 $4.88 -7.44; CRK×1 yday $14.50 → 09:30 $14.42 -0.08; SLI×6 yday $2.61 → 09:30 $2.60 -0.06; GGB×4 yday $4.46 → 09:30 $4.57 +0.44 | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 118 | $10.63 | $2.37 | $-38.94 | $1,330.79 | ▼ -38.94 after sell → book $10,431.99; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 21 | $62.10 | $2.07 | $+9.10 | $2,632.81 | ▲ +9.10 after sell → book $10,429.91; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 156 | $8.49 | $2.49 | $+27.81 | $3,954.76 | ▲ +27.81 after sell → book $10,427.42; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 247 | $5.07 | $3.24 | $-45.94 | $5,203.81 | ▼ -45.94 after sell → book $10,424.18; vs 09:30 mark -3.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-17.91 | $6,475.62 | ▼ -17.91 after sell → book $10,422.16; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 800 | $1.74 | $10.46 | $+75.22 | $7,857.16 | ▲ +75.22 after sell → book $10,411.70; vs 09:30 mark -10.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 248 | $4.88 | $3.25 | $-90.77 | $9,064.15 | ▼ -90.77 after sell → book $10,408.45; vs 09:30 mark -3.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 43 | $41.44 | $2.12 | — | $7,280.11 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+1.8; leftover $1812.83 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 12 | $144.70 | $2.03 | — | $5,541.68 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1812.83 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 97 | $18.50 | $2.28 | — | $3,744.90 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1812.83 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 12 | $149.40 | $2.03 | — | $1,950.08 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1812.83 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 113 | $15.94 | $2.33 | — | $146.53 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1812.83 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.53 | ▼ 09:30 equity $9,978.59 vs yday $10,243.09 (-264.50) | 09:30 open · cash $146.53 (unchanged overnight, no fees) · equity $9,978.59 vs prior close $10,243.09 (-264.50) because holdings re-marked: MOS×54 yday $23.76 → 09:30 $23.75 -0.54; CRK×1 yday $14.62 → 09:30 $14.56 -0.06; SLI×6 yday $2.64 → 09:30 $2.51 -0.78; GGB×4 yday $4.70 → 09:30 $4.55 -0.60; RRC×43 yday $41.64 → 09:30 $41.11 -22.79; ANF×12 yday $145.75 → 09:30 $148.67 +35.04; BZ×97 yday $18.00 → 09:30 $17.89 -10.67; SMTC×12 yday $142.43 → 09:30 $133.04 -112.68; GRRR×113 yday $15.66 → 09:30 $14.32 -151.42 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 54 | $23.75 | $2.17 | $-17.82 | $1,426.85 | ▼ -17.82 after sell → book $9,976.41; vs 09:30 mark -2.18 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,426.85 | ▼ 09:30 equity $9,927.54 vs yday $9,993.90 (-66.36) | 09:30 open · cash $1,426.85 (unchanged overnight, no fees) · equity $9,927.54 vs prior close $9,993.90 (-66.36) because holdings re-marked: CRK×1 yday $14.51 → 09:30 $14.31 -0.20; SLI×6 yday $2.51 → 09:30 $2.70 +1.14; GGB×4 yday $4.55 → 09:30 $4.61 +0.24; RRC×43 yday $41.78 → 09:30 $41.32 -19.78; ANF×12 yday $149.28 → 09:30 $142.47 -81.72; BZ×97 yday $17.90 → 09:30 $17.37 -51.41; SMTC×12 yday $132.54 → 09:30 $131.65 -10.68; GRRR×113 yday $14.20 → 09:30 $15.05 +96.05 | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 1 | $14.31 | $0.17 | $-0.09 | $1,441.00 | ▼ -0.09 after sell → book $9,927.38; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 6 | $2.70 | $0.20 | $+0.29 | $1,457.00 | ▲ +0.29 after sell → book $9,927.18; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 4 | $4.61 | $0.22 | $+0.35 | $1,475.22 | ▲ +0.35 after sell → book $9,926.96; vs 09:30 mark -0.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,475.22 | ▼ 09:30 equity $9,858.08 vs yday $9,859.87 (-1.79) | 09:30 open · cash $1,475.22 (unchanged overnight, no fees) · equity $9,858.08 vs prior close $9,859.87 (-1.79) because holdings re-marked: RRC×43 yday $41.32 → 09:30 $41.94 +26.66; ANF×12 yday $143.00 → 09:30 $142.00 -12.00; BZ×97 yday $17.17 → 09:30 $17.29 +11.64; SMTC×12 yday $129.50 → 09:30 $127.63 -22.44; GRRR×113 yday $14.80 → 09:30 $14.75 -5.65 | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 43 | $41.94 | $2.14 | $+17.24 | $3,276.50 | ▲ +17.24 after sell → book $9,855.94; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 12 | $142.00 | $2.05 | $-36.48 | $4,978.45 | ▼ -36.48 after sell → book $9,853.89; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 97 | $17.29 | $2.31 | $-121.96 | $6,653.27 | ▼ -121.96 after sell → book $9,851.58; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 12 | $127.63 | $2.05 | $-265.31 | $8,182.78 | ▼ -265.31 after sell → book $9,849.53; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 113 | $14.75 | $2.36 | $-139.16 | $9,847.17 | ▼ -139.16 after sell → book $9,847.17; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,847.17 | ▲ 09:30 equity $9,847.17 vs yday $9,847.17 (-0.00) | 09:30 open · cash $9,847.17 · no holdings · equity $9,847.17 vs prior close $9,847.17 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $49.76 | $2.06 | — | $8,650.87 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1230.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $41.31 | $2.08 | — | $7,450.80 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1230.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 376 | $3.27 | $4.85 | — | $6,216.43 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1230.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 159 | $7.70 | $2.47 | — | $4,989.66 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1230.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $3,854.19 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1230.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 78 | $15.70 | $2.22 | — | $2,627.36 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1230.90 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 54 | $22.78 | $2.15 | — | $1,395.09 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1230.90 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 83 | $14.70 | $2.24 | — | $172.75 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1230.90 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.75 | ▲ 09:30 equity $10,247.71 vs yday $10,196.54 (+51.17) | 09:30 open · cash $172.75 (unchanged overnight, no fees) · equity $10,247.71 vs prior close $10,196.54 (+51.17) because holdings re-marked: ATRC×24 yday $52.59 → 09:30 $52.88 +6.96; HRMY×29 yday $42.86 → 09:30 $42.93 +2.03; CABA×376 yday $3.57 → 09:30 $3.63 +22.56; VSTM×159 yday $8.02 → 09:30 $8.03 +1.59; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×78 yday $15.54 → 09:30 $15.45 -7.02; MMED×54 yday $23.76 → 09:30 $23.88 +6.48; SLN×83 yday $14.79 → 09:30 $14.85 +4.98 | — |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 2 | $10.41 | $0.21 | — | $151.72 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $28.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 1 | $14.50 | $0.15 | — | $137.07 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+0.8; leftover $28.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 14 | $1.95 | $0.32 | — | $109.45 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $28.79 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 9 | $3.07 | $0.30 | — | $81.52 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $28.79 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 23.36 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 23.36 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 23.36 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `QSI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 18.09 < 1 share @ 40.72 |
| 2026-08-27 | `ACMR` | cash | leftover split 18.09 < 1 share @ 80.97 |
| 2026-08-27 | `MT` | cash | leftover split 18.09 < 1 share @ 75.12 |
| 2026-08-27 | `MU` | cash | leftover split 18.09 < 1 share @ 925.74 |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 28.79 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 28.79 < 1 share @ 30.65 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 24 | 2026-09-03 @ $49.76 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1230.90 |
| `HRMY` | 29 | 2026-09-03 @ $41.31 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1230.90 |
| `CABA` | 376 | 2026-09-03 @ $3.27 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1230.90 |
| `VSTM` | 159 | 2026-09-03 @ $7.70 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1230.90 |
| `RVTY` | 9 | 2026-09-03 @ $125.94 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1230.90 |
| `CRK` | 78 | 2026-09-03 @ $15.70 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1230.90 |
| `MMED` | 54 | 2026-09-03 @ $22.78 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1230.90 |
| `SLN` | 83 | 2026-09-03 @ $14.70 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1230.90 |
| `NVAX` | 2 | 2026-09-04 @ $10.41 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $28.79 |
| `BVS` | 1 | 2026-09-04 @ $14.50 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+0.8; leftover $28.79 |
| `BAK` | 14 | 2026-09-04 @ $1.95 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $28.79 |
| `SLBT` | 9 | 2026-09-04 @ $3.07 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $28.79 |
