# Factor mine action — `flatten_live_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · 09:30 tickets only when flatten_robust gate fires (mover)

Cash book **+4.92%** ($10,492) · signal-only (no cash/fees) was +9.59%. Starts YES **7/17**. Fills 26 · skips 34 · realized $+491.55.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,491.55.

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
| 2026-08-25 | +1.80 | $78.42 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,515.37 | +68.61 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $10,372.43 | $10,489.30 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | 09:30 open · cash $78.42 (unchanged overnight, no fees) · equity $10,515.37 vs prior close $10,446.76 (+68.61) because holdings re-marked: AG×60 yday $20.57 → 09:30 $20.73 +9.60; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; CDE×60 yday $20.49 → 09:30 $20.85 +21.60; HDSN×216 yday $5.57 → 09:30 $5.53 -8.64; IAG×63 yday $21.36 → 09:30 $21.63 +17.01; KGC×42 yday $32.47 → 09:30 $32.76 +12.18; NFGC×714 yday $1.90 → 09:30 $1.91 +7.14; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×9 yday $2.38 → 09:30 $2.32 -0.54; CRDL×12 yday $1.80 → 09:30 $1.90 +1.20; CYPH×17 yday $1.64 → 09:30 $1.70 +1.02 |
| 2026-08-26 | +2.02 | $10,372.43 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,489.30 | -0.00 | — | — | $10,372.43 | $10,490.40 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | 09:30 open · cash $10,372.43 (unchanged overnight, no fees) · equity $10,489.30 vs prior close $10,489.30 (-0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×9 yday $2.34 → 09:30 $2.34 +0.00; CRDL×12 yday $1.90 → 09:30 $1.90 +0.00; CYPH×17 yday $1.64 → 09:30 $1.64 +0.00 |
| 2026-08-27 | — | $10,372.43 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,492.98 | +2.58 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $10,491.55 | $10,491.55 | — | 09:30 open · cash $10,372.43 (unchanged overnight, no fees) · equity $10,492.98 vs prior close $10,490.40 (+2.58) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×9 yday $2.34 → 09:30 $2.41 +0.63; CRDL×12 yday $1.90 → 09:30 $2.03 +1.56; CYPH×17 yday $1.64 → 09:30 $1.60 -0.68 |
| 2026-08-28 | +0.75 | $10,491.55 | — | $10,491.55 | -0.00 | — | — | $10,491.55 | $10,491.55 | — | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-31 | -5.85 | $10,491.55 | — | $10,491.55 | -0.00 | — | — | $10,491.55 | $10,491.55 | — | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-01 | -6.30 | $10,491.55 | — | $10,491.55 | -0.00 | — | — | $10,491.55 | $10,491.55 | — | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $10,491.55 | — | $10,491.55 | -0.00 | — | — | $10,491.55 | $10,491.55 | — | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,491.55 | — | $10,491.55 | -0.00 | — | — | $10,491.55 | $10,491.55 | — | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $10,491.55 | — | $10,491.55 | -0.00 | — | — | $10,491.55 | $10,491.55 | — | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.91 | ▲ 09:30 equity $10,475.50 vs yday $10,208.28 (+267.22) | 09:30 open · cash $186.91 (unchanged overnight, no fees) · equity $10,475.50 vs prior close $10,208.28 (+267.22) because holdings re-marked: AG×60 yday $21.19 → 09:30 $21.90 +42.60; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×60 yday $21.11 → 09:30 $21.75 +38.40; HDSN×216 yday $5.57 → 09:30 $5.67 +21.60; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×714 yday $1.75 → 09:30 $1.79 +28.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $169.53 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $147.04 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $124.56 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $101.13 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 17 | $1.32 | $0.28 | — | $78.42 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,372.43 | ▲ 09:30 equity $10,489.30 vs yday $10,489.30 (-0.00) | 09:30 open · cash $10,372.43 (unchanged overnight, no fees) · equity $10,489.30 vs prior close $10,489.30 (-0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×9 yday $2.34 → 09:30 $2.34 +0.00; CRDL×12 yday $1.90 → 09:30 $1.90 +0.00; CYPH×17 yday $1.64 → 09:30 $1.64 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,372.43 | ▲ 09:30 equity $10,492.98 vs yday $10,490.40 (+2.58) | 09:30 open · cash $10,372.43 (unchanged overnight, no fees) · equity $10,492.98 vs prior close $10,490.40 (+2.58) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×9 yday $2.34 → 09:30 $2.41 +0.63; CRDL×12 yday $1.90 → 09:30 $2.03 +1.56; CYPH×17 yday $1.64 → 09:30 $1.60 -0.68 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $10,388.84 | ▼ -0.96 after sell → book $10,492.79; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $10,419.20 | ▲ +7.88 after sell → book $10,492.45; vs 09:30 mark -0.34 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 9 | $2.41 | $0.26 | $-1.05 | $10,440.63 | ▼ -1.05 after sell → book $10,492.19; vs 09:30 mark -0.26 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 12 | $2.03 | $0.30 | $+0.63 | $10,464.69 | ▲ +0.63 after sell → book $10,491.89; vs 09:30 mark -0.30 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 17 | $1.60 | $0.34 | $+4.14 | $10,491.55 | ▲ +4.14 after sell → book $10,491.55; vs 09:30 mark -0.34 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,491.55 | ▲ 09:30 equity $10,491.55 vs yday $10,491.55 (-0.00) | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,491.55 | ▲ 09:30 equity $10,491.55 vs yday $10,491.55 (-0.00) | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,491.55 | ▲ 09:30 equity $10,491.55 vs yday $10,491.55 (-0.00) | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,491.55 | ▲ 09:30 equity $10,491.55 vs yday $10,491.55 (-0.00) | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,491.55 | ▲ 09:30 equity $10,491.55 vs yday $10,491.55 (-0.00) | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,491.55 | ▲ 09:30 equity $10,491.55 vs yday $10,491.55 (-0.00) | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. | — |

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
