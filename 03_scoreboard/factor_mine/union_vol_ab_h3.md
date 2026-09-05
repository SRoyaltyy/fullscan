# Factor mine action — `union_vol_ab_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-6.81%** ($9,319) · signal-only (no cash/fees) was -3.28%. Starts YES **0/17**. Fills 70 · skips 92 · realized $-608.28.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good,ab=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $60.25.

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
| 2026-08-25 | +1.80 | $78.42 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,515.37 | +68.61 | BMEA, ALVO, ZURA, DEFT, RUM, KURA, EZPW | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $2.29 | $10,461.41 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, BMEA×914, ALVO×283, ZURA×232, DEFT×2315, RUM×158, KURA×111, EZPW×42 | 09:30 open · cash $78.42 (unchanged overnight, no fees) · equity $10,515.37 vs prior close $10,446.76 (+68.61) because holdings re-marked: AG×60 yday $20.57 → 09:30 $20.73 +9.60; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; CDE×60 yday $20.49 → 09:30 $20.85 +21.60; HDSN×216 yday $5.57 → 09:30 $5.53 -8.64; IAG×63 yday $21.36 → 09:30 $21.63 +17.01; KGC×42 yday $32.47 → 09:30 $32.76 +12.18; NFGC×714 yday $1.90 → 09:30 $1.91 +7.14; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×9 yday $2.38 → 09:30 $2.32 -0.54; CRDL×12 yday $1.80 → 09:30 $1.90 +1.20; CYPH×17 yday $1.64 → 09:30 $1.70 +1.02 |
| 2026-08-26 | +2.02 | $2.29 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, BMEA×914, ALVO×283, ZURA×232, DEFT×2315, RUM×158, KURA×111, EZPW×42 | $10,461.41 | -0.00 | — | — | $2.29 | $10,443.30 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, BMEA×914, ALVO×283, ZURA×232, DEFT×2315, RUM×158, KURA×111, EZPW×42 | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $10,461.41 vs prior close $10,461.41 (-0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×9 yday $2.34 → 09:30 $2.34 +0.00; CRDL×12 yday $1.90 → 09:30 $1.90 +0.00; CYPH×17 yday $1.64 → 09:30 $1.64 +0.00; BMEA×914 yday $1.61 → 09:30 $1.61 +0.00; ALVO×283 yday $5.25 → 09:30 $5.25 +0.00; ZURA×232 yday $6.50 → 09:30 $6.50 +0.00; DEFT×2315 yday $0.62 → 09:30 $0.62 +0.00; RUM×158 yday $9.35 → 09:30 $9.35 +0.00; KURA×111 yday $13.58 → 09:30 $13.58 +0.00; EZPW×42 yday $34.69 → 09:30 $34.69 +0.00 |
| 2026-08-27 | — | $2.29 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17, BMEA×914, ALVO×283, ZURA×232, DEFT×2315, RUM×158, KURA×111, EZPW×42 | $10,546.23 | +102.93 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $121.41 | $10,184.91 | BMEA×914, ALVO×283, ZURA×232, DEFT×2315, RUM×158, KURA×111, EZPW×42 | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $10,546.23 vs prior close $10,443.30 (+102.93) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×9 yday $2.34 → 09:30 $2.41 +0.63; CRDL×12 yday $1.90 → 09:30 $2.03 +1.56; CYPH×17 yday $1.64 → 09:30 $1.60 -0.68; BMEA×914 yday $1.61 → 09:30 $1.75 +127.96; ALVO×283 yday $5.25 → 09:30 $4.98 -76.41; ZURA×232 yday $6.50 → 09:30 $6.13 -85.84; DEFT×2315 yday $0.62 → 09:30 $0.60 -46.30; RUM×158 yday $9.35 → 09:30 $10.07 +113.76; KURA×111 yday $13.58 → 09:30 $13.63 +5.55; EZPW×42 yday $34.69 → 09:30 $35.70 +42.42 |
| 2026-08-28 | +0.75 | $121.41 | BMEA×914, ALVO×283, ZURA×232, DEFT×2315, RUM×158, KURA×111, EZPW×42 | $10,228.81 | +43.90 | ANF, BZ, SMTC, URBN, BBWI, CRDL, TIGR, FINV | BMEA, ALVO, ZURA, DEFT, RUM, KURA, EZPW | $226.83 | $9,825.62 | ANF×8, BZ×68, SMTC×8, URBN×15, BBWI×68, CRDL×608, TIGR×231, FINV×298 | 09:30 open · cash $121.41 (unchanged overnight, no fees) · equity $10,228.81 vs prior close $10,184.91 (+43.90) because holdings re-marked: BMEA×914 yday $1.71 → 09:30 $1.74 +27.42; ALVO×283 yday $4.91 → 09:30 $4.88 -8.49; ZURA×232 yday $5.99 → 09:30 $6.02 +6.96; DEFT×2315 yday $0.59 → 09:30 $0.60 +23.15; RUM×158 yday $9.38 → 09:30 $9.51 +20.54; KURA×111 yday $13.06 → 09:30 $12.98 -8.88; EZPW×42 yday $33.90 → 09:30 $33.50 -16.80 |
| 2026-08-31 | -5.85 | $226.83 | ANF×8, BZ×68, SMTC×8, URBN×15, BBWI×68, CRDL×608, TIGR×231, FINV×298 | $9,594.30 | -231.32 | — | — | $226.83 | $9,601.97 | ANF×8, BZ×68, SMTC×8, URBN×15, BBWI×68, CRDL×608, TIGR×231, FINV×298 | 09:30 open · cash $226.83 (unchanged overnight, no fees) · equity $9,594.30 vs prior close $9,825.62 (-231.32) because holdings re-marked: ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BZ×68 yday $18.00 → 09:30 $17.89 -7.48; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; URBN×15 yday $78.79 → 09:30 $81.09 +34.50; BBWI×68 yday $18.65 → 09:30 $19.30 +44.20; CRDL×608 yday $2.06 → 09:30 $1.96 -60.80; TIGR×231 yday $5.06 → 09:30 $4.96 -23.10; FINV×298 yday $4.02 → 09:30 $3.46 -166.88 |
| 2026-09-01 | -6.30 | $226.83 | ANF×8, BZ×68, SMTC×8, URBN×15, BBWI×68, CRDL×608, TIGR×231, FINV×298 | $9,567.22 | -34.75 | — | — | $226.83 | $9,536.04 | ANF×8, BZ×68, SMTC×8, URBN×15, BBWI×68, CRDL×608, TIGR×231, FINV×298 | 09:30 open · cash $226.83 (unchanged overnight, no fees) · equity $9,567.22 vs prior close $9,601.97 (-34.75) because holdings re-marked: ANF×8 yday $149.28 → 09:30 $142.47 -54.48; BZ×68 yday $17.90 → 09:30 $17.37 -36.04; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; URBN×15 yday $81.09 → 09:30 $80.69 -6.00; BBWI×68 yday $19.22 → 09:30 $19.10 -8.16; CRDL×608 yday $1.96 → 09:30 $1.98 +12.16; TIGR×231 yday $5.01 → 09:30 $5.02 +2.31; FINV×298 yday $3.46 → 09:30 $3.67 +62.58 |
| 2026-09-02 | -3.83 | $226.83 | ANF×8, BZ×68, SMTC×8, URBN×15, BBWI×68, CRDL×608, TIGR×231, FINV×298 | $9,417.18 | -118.86 | — | ANF, BZ, SMTC, URBN, BBWI, CRDL, TIGR, FINV | $9,391.74 | $9,391.74 | — | 09:30 open · cash $226.83 (unchanged overnight, no fees) · equity $9,417.18 vs prior close $9,536.04 (-118.86) because holdings re-marked: ANF×8 yday $143.00 → 09:30 $142.00 -8.00; BZ×68 yday $17.17 → 09:30 $17.29 +8.16; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; URBN×15 yday $80.69 → 09:30 $79.12 -23.55; BBWI×68 yday $19.10 → 09:30 $18.77 -22.44; CRDL×608 yday $1.98 → 09:30 $1.94 -24.32; TIGR×231 yday $5.00 → 09:30 $4.97 -6.93; FINV×298 yday $3.67 → 09:30 $3.58 -26.82 |
| 2026-09-03 | -0.90 | $9,391.74 | — | $9,391.74 | +0.00 | RVTY, CRK, MMED, EIX, CRDL, MRNA, ARCT, NVAX | — | $205.01 | $9,447.99 | RVTY×9, CRK×74, MMED×51, EIX×20, CRDL×543, MRNA×7, ARCT×71, NVAX×114 | 09:30 open · cash $9,391.74 · no holdings · equity $9,391.74 vs prior close $9,391.74 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $205.01 | RVTY×9, CRK×74, MMED×51, EIX×20, CRDL×543, MRNA×7, ARCT×71, NVAX×114 | $9,449.44 | +1.45 | CABA, BAK, SGLD, IRD, OABI, ALEC | — | $60.25 | $9,318.66 | RVTY×9, CRK×74, MMED×51, EIX×20, CRDL×543, MRNA×7, ARCT×71, NVAX×114, CABA×7, BAK×13, SGLD×3, IRD×5, OABI×5, ALEC×9 | 09:30 open · cash $205.01 (unchanged overnight, no fees) · equity $9,449.44 vs prior close $9,447.99 (+1.45) because holdings re-marked: RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×74 yday $15.54 → 09:30 $15.45 -6.66; MMED×51 yday $23.76 → 09:30 $23.88 +6.12; EIX×20 yday $55.19 → 09:30 $55.42 +4.60; CRDL×543 yday $2.17 → 09:30 $2.18 +5.43; MRNA×7 yday $150.81 → 09:30 $145.95 -34.02; ARCT×71 yday $16.74 → 09:30 $16.77 +2.13; NVAX×114 yday $10.32 → 09:30 $10.41 +10.26 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.91 | ▲ 09:30 equity $10,475.50 vs yday $10,208.28 (+267.22) | 09:30 open · cash $186.91 (unchanged overnight, no fees) · equity $10,475.50 vs prior close $10,208.28 (+267.22) because holdings re-marked: AG×60 yday $21.19 → 09:30 $21.90 +42.60; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×60 yday $21.11 → 09:30 $21.75 +38.40; HDSN×216 yday $5.57 → 09:30 $5.67 +21.60; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×714 yday $1.75 → 09:30 $1.79 +28.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $169.53 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $147.04 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $124.56 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $101.13 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 17 | $1.32 | $0.28 | — | $78.42 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 914 | $1.62 | $11.79 | — | $8,879.96 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1481.78 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 283 | $5.22 | $3.65 | — | $7,399.04 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1481.78 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 232 | $6.38 | $2.99 | — | $5,915.89 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1481.78 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2315 | $0.64 | $21.76 | — | $4,412.53 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1481.78 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 158 | $9.36 | $2.46 | — | $2,931.19 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1481.78 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 111 | $13.30 | $2.32 | — | $1,452.56 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=+9.5; leftover $1481.78 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 42 | $34.48 | $2.12 | — | $2.29 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1481.78 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▲ 09:30 equity $10,461.41 vs yday $10,461.41 (-0.00) | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $10,461.41 vs prior close $10,461.41 (-0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×9 yday $2.34 → 09:30 $2.34 +0.00; CRDL×12 yday $1.90 → 09:30 $1.90 +0.00; CYPH×17 yday $1.64 → 09:30 $1.64 +0.00; BMEA×914 yday $1.61 → 09:30 $1.61 +0.00; ALVO×283 yday $5.25 → 09:30 $5.25 +0.00; ZURA×232 yday $6.50 → 09:30 $6.50 +0.00; DEFT×2315 yday $0.62 → 09:30 $0.62 +0.00; RUM×158 yday $9.35 → 09:30 $9.35 +0.00; KURA×111 yday $13.58 → 09:30 $13.58 +0.00; EZPW×42 yday $34.69 → 09:30 $34.69 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▲ 09:30 equity $10,546.23 vs yday $10,443.30 (+102.93) | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $10,546.23 vs prior close $10,443.30 (+102.93) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×9 yday $2.34 → 09:30 $2.41 +0.63; CRDL×12 yday $1.90 → 09:30 $2.03 +1.56; CYPH×17 yday $1.64 → 09:30 $1.60 -0.68; BMEA×914 yday $1.61 → 09:30 $1.75 +127.96; ALVO×283 yday $5.25 → 09:30 $4.98 -76.41; ZURA×232 yday $6.50 → 09:30 $6.13 -85.84; DEFT×2315 yday $0.62 → 09:30 $0.60 -46.30; RUM×158 yday $9.35 → 09:30 $10.07 +113.76; KURA×111 yday $13.58 → 09:30 $13.63 +5.55; EZPW×42 yday $34.69 → 09:30 $35.70 +42.42 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $18.70 | ▼ -0.96 after sell → book $10,546.04; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $49.07 | ▲ +7.88 after sell → book $10,545.71; vs 09:30 mark -0.33 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 9 | $2.41 | $0.26 | $-1.05 | $70.49 | ▼ -1.05 after sell → book $10,545.44; vs 09:30 mark -0.27 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 12 | $2.03 | $0.30 | $+0.63 | $94.55 | ▲ +0.63 after sell → book $10,545.14; vs 09:30 mark -0.30 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 17 | $1.60 | $0.34 | $+4.14 | $121.41 | ▲ +4.14 after sell → book $10,544.80; vs 09:30 mark -0.34 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.41 | ▲ 09:30 equity $10,228.81 vs yday $10,184.91 (+43.90) | 09:30 open · cash $121.41 (unchanged overnight, no fees) · equity $10,228.81 vs prior close $10,184.91 (+43.90) because holdings re-marked: BMEA×914 yday $1.71 → 09:30 $1.74 +27.42; ALVO×283 yday $4.91 → 09:30 $4.88 -8.49; ZURA×232 yday $5.99 → 09:30 $6.02 +6.96; DEFT×2315 yday $0.59 → 09:30 $0.60 +23.15; RUM×158 yday $9.38 → 09:30 $9.51 +20.54; KURA×111 yday $13.06 → 09:30 $12.98 -8.88; EZPW×42 yday $33.90 → 09:30 $33.50 -16.80 | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 914 | $1.74 | $11.96 | $+85.93 | $1,699.81 | ▲ +85.93 after sell → book $10,216.85; vs 09:30 mark -11.96 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 283 | $4.88 | $3.71 | $-103.58 | $3,077.15 | ▼ -103.58 after sell → book $10,213.15; vs 09:30 mark -3.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 232 | $6.02 | $3.04 | $-89.56 | $4,470.74 | ▼ -89.56 after sell → book $10,210.10; vs 09:30 mark -3.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DEFT` | 2315 | $0.60 | $21.23 | $-135.59 | $5,838.51 | ▼ -135.59 after sell → book $10,188.87; vs 09:30 mark -21.23 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 158 | $9.51 | $2.50 | $+18.73 | $7,338.59 | ▲ +18.73 after sell → book $10,186.37; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 111 | $12.98 | $2.35 | $-40.20 | $8,777.02 | ▼ -40.20 after sell → book $10,184.02; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 42 | $33.50 | $2.14 | $-45.41 | $10,181.88 | ▼ -45.41 after sell → book $10,181.88; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $9,022.27 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1272.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 68 | $18.50 | $2.19 | — | $7,762.07 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1272.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $6,564.86 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1272.74 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 15 | $82.70 | $2.04 | — | $5,322.32 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1272.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 68 | $18.68 | $2.19 | — | $4,049.89 | — | combo gate; gate vol=good,ab=good; list yday_gainer; ret5=+0.2; leftover $1272.74 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 608 | $2.09 | $7.84 | — | $2,771.33 | — | combo gate; gate vol=good,ab=good; list yday_gainer; ret5=+3.3; leftover $1272.74 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TIGR` | 231 | $5.49 | $2.98 | — | $1,500.16 | — | combo gate; gate vol=good,ab=good; list ohlc_hot; ret5=+15.9; leftover $1272.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 298 | $4.26 | $3.84 | — | $226.83 | — | combo gate; gate vol=good,ab=good; list earn_react; ret5=-0.7; leftover $1272.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.83 | ▼ 09:30 equity $9,594.30 vs yday $9,825.62 (-231.32) | 09:30 open · cash $226.83 (unchanged overnight, no fees) · equity $9,594.30 vs prior close $9,825.62 (-231.32) because holdings re-marked: ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BZ×68 yday $18.00 → 09:30 $17.89 -7.48; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; URBN×15 yday $78.79 → 09:30 $81.09 +34.50; BBWI×68 yday $18.65 → 09:30 $19.30 +44.20; CRDL×608 yday $2.06 → 09:30 $1.96 -60.80; TIGR×231 yday $5.06 → 09:30 $4.96 -23.10; FINV×298 yday $4.02 → 09:30 $3.46 -166.88 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.83 | ▼ 09:30 equity $9,567.22 vs yday $9,601.97 (-34.75) | 09:30 open · cash $226.83 (unchanged overnight, no fees) · equity $9,567.22 vs prior close $9,601.97 (-34.75) because holdings re-marked: ANF×8 yday $149.28 → 09:30 $142.47 -54.48; BZ×68 yday $17.90 → 09:30 $17.37 -36.04; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; URBN×15 yday $81.09 → 09:30 $80.69 -6.00; BBWI×68 yday $19.22 → 09:30 $19.10 -8.16; CRDL×608 yday $1.96 → 09:30 $1.98 +12.16; TIGR×231 yday $5.01 → 09:30 $5.02 +2.31; FINV×298 yday $3.46 → 09:30 $3.67 +62.58 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.83 | ▼ 09:30 equity $9,417.18 vs yday $9,536.04 (-118.86) | 09:30 open · cash $226.83 (unchanged overnight, no fees) · equity $9,417.18 vs prior close $9,536.04 (-118.86) because holdings re-marked: ANF×8 yday $143.00 → 09:30 $142.00 -8.00; BZ×68 yday $17.17 → 09:30 $17.29 +8.16; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; URBN×15 yday $80.69 → 09:30 $79.12 -23.55; BBWI×68 yday $19.10 → 09:30 $18.77 -22.44; CRDL×608 yday $1.98 → 09:30 $1.94 -24.32; TIGR×231 yday $5.00 → 09:30 $4.97 -6.93; FINV×298 yday $3.67 → 09:30 $3.58 -26.82 | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 8 | $142.00 | $2.03 | $-25.65 | $1,360.80 | ▼ -25.65 after sell → book $9,415.15; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 68 | $17.29 | $2.22 | $-86.69 | $2,534.30 | ▼ -86.69 after sell → book $9,412.93; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $3,553.31 | ▼ -178.21 after sell → book $9,410.90; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 15 | $79.12 | $2.06 | $-57.79 | $4,738.05 | ▼ -57.79 after sell → book $9,408.84; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 68 | $18.77 | $2.22 | $+1.71 | $6,012.20 | ▲ +1.71 after sell → book $9,406.63; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRDL` | 608 | $1.94 | $7.95 | $-107.00 | $7,183.76 | ▼ -107.00 after sell → book $9,398.67; vs 09:30 mark -7.96 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `TIGR` | 231 | $4.97 | $3.03 | $-126.13 | $8,328.81 | ▼ -126.13 after sell → book $9,395.65; vs 09:30 mark -3.02 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 298 | $3.58 | $3.90 | $-210.39 | $9,391.74 | ▼ -210.39 after sell → book $9,391.74; vs 09:30 mark -3.91 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,391.74 | ▲ 09:30 equity $9,391.74 vs yday $9,391.74 (+0.00) | 09:30 open · cash $9,391.74 · no holdings · equity $9,391.74 vs prior close $9,391.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $8,256.27 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1173.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 74 | $15.70 | $2.21 | — | $7,092.25 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1173.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $22.78 | $2.14 | — | $5,928.33 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1173.97 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 20 | $56.78 | $2.05 | — | $4,790.68 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; ret5=+0.3; leftover $1173.97 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 543 | $2.16 | $7.00 | — | $3,610.80 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1173.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 7 | $151.40 | $2.01 | — | $2,548.98 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1173.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 71 | $16.46 | $2.20 | — | $1,378.12 | — | combo gate; gate vol=good,ab=good; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1173.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 114 | $10.27 | $2.33 | — | $205.01 | — | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1173.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $205.01 | ▲ 09:30 equity $9,449.44 vs yday $9,447.99 (+1.45) | 09:30 open · cash $205.01 (unchanged overnight, no fees) · equity $9,449.44 vs prior close $9,447.99 (+1.45) because holdings re-marked: RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×74 yday $15.54 → 09:30 $15.45 -6.66; MMED×51 yday $23.76 → 09:30 $23.88 +6.12; EIX×20 yday $55.19 → 09:30 $55.42 +4.60; CRDL×543 yday $2.17 → 09:30 $2.18 +5.43; MRNA×7 yday $150.81 → 09:30 $145.95 -34.02; ARCT×71 yday $16.74 → 09:30 $16.77 +2.13; NVAX×114 yday $10.32 → 09:30 $10.41 +10.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 7 | $3.63 | $0.28 | — | $179.32 | — | combo gate; gate vol=good,ab=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $25.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 13 | $1.95 | $0.29 | — | $153.68 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $25.63 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SGLD` | 3 | $6.48 | $0.20 | — | $134.04 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+0.0; leftover $25.63 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 5 | $4.66 | $0.25 | — | $110.49 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $25.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 5 | $5.08 | $0.27 | — | $84.82 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $25.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 9 | $2.70 | $0.27 | — | $60.25 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $25.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `QSI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SLQT` | no_price | no 09:30 open |
| 2026-08-26 | `DKS` | no_price | no 09:30 open |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FINV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FINV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 25.63 < 1 share @ 486.31 |
| 2026-09-04 | `MLYS` | cash | leftover split 25.63 < 1 share @ 29.15 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RVTY` | 9 | 2026-09-03 @ $125.94 | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1173.97 |
| `CRK` | 74 | 2026-09-03 @ $15.70 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1173.97 |
| `MMED` | 51 | 2026-09-03 @ $22.78 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1173.97 |
| `EIX` | 20 | 2026-09-03 @ $56.78 | combo gate; gate vol=good,ab=good; list probable,yday_gainer; ret5=+0.3; leftover $1173.97 |
| `CRDL` | 543 | 2026-09-03 @ $2.16 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1173.97 |
| `MRNA` | 7 | 2026-09-03 @ $151.40 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1173.97 |
| `ARCT` | 71 | 2026-09-03 @ $16.46 | combo gate; gate vol=good,ab=good; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1173.97 |
| `NVAX` | 114 | 2026-09-03 @ $10.27 | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1173.97 |
| `CABA` | 7 | 2026-09-04 @ $3.63 | combo gate; gate vol=good,ab=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $25.63 |
| `BAK` | 13 | 2026-09-04 @ $1.95 | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $25.63 |
| `SGLD` | 3 | 2026-09-04 @ $6.48 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+0.0; leftover $25.63 |
| `IRD` | 5 | 2026-09-04 @ $4.66 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $25.63 |
| `OABI` | 5 | 2026-09-04 @ $5.08 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $25.63 |
| `ALEC` | 9 | 2026-09-04 @ $2.70 | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $25.63 |
