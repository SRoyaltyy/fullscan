# Factor mine action — `short_extended_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · ret_5>15

Cash book **-8.50%** ($9,150) · signal-only (no cash/fees) was +3.26%. Starts YES **0/17**. Fills 108 · skips 55 · realized $-894.12.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=15.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $16,806.65.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | TNDM | — | $14,989.65 | $10,039.83 | TNDM×214 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $14,989.65 | TNDM×214 | $10,084.77 | +44.94 | ARX, OMER, AIRO, MXCT, QMLS, AVAH, TBBB, AMPY | TNDM | $15,023.36 | $10,193.38 | ARX×32, OMER×36, AIRO×56, MXCT×453, QMLS×86, AVAH×52, TBBB×12, AMPY×127 | 09:30 open · cash $14,989.65 (unchanged overnight, no fees) · equity $10,084.77 vs prior close $10,039.83 (+44.94) because holdings re-marked: TNDM×214 yday $23.13 → 09:30 $22.92 +44.94 |
| 2026-08-17 | +2.25 | $15,023.36 | ARX×32, OMER×36, AIRO×56, MXCT×453, QMLS×86, AVAH×52, TBBB×12, AMPY×127 | $10,201.66 | +8.28 | CAPR, HTFL, UMAC, NPWR, LPTH, NMAX, ALOY, INO | ARX, OMER, AIRO, MXCT, QMLS, AVAH, TBBB, AMPY | $15,189.73 | $10,228.41 | CAPR×92, HTFL×15, UMAC×19, NPWR×331, LPTH×42, NMAX×58, ALOY×43, INO×594 | 09:30 open · cash $15,023.36 (unchanged overnight, no fees) · equity $10,201.66 vs prior close $10,193.38 (+8.28) because holdings re-marked: ARX×32 yday $19.58 → 09:30 $19.57 +0.32; OMER×36 yday $17.19 → 09:30 $17.17 +0.72; AIRO×56 yday $9.57 → 09:30 $9.57 +0.00; MXCT×453 yday $1.32 → 09:30 $1.32 +0.00; QMLS×86 yday $7.32 → 09:30 $7.24 +6.88; AVAH×52 yday $12.32 → 09:30 $12.21 +5.72; TBBB×12 yday $47.79 → 09:30 $47.39 +4.80; AMPY×127 yday $4.78 → 09:30 $4.86 -10.16 |
| 2026-08-18 | -6.20 | $15,189.73 | CAPR×92, HTFL×15, UMAC×19, NPWR×331, LPTH×42, NMAX×58, ALOY×43, INO×594 | $10,340.59 | +112.18 | — | CAPR, HTFL, UMAC, NPWR, LPTH, NMAX, ALOY, INO | $10,315.91 | $10,315.91 | — | 09:30 open · cash $15,189.73 (unchanged overnight, no fees) · equity $10,340.59 vs prior close $10,228.41 (+112.18) because holdings re-marked: CAPR×92 yday $7.45 → 09:30 $7.50 -4.60; HTFL×15 yday $41.94 → 09:30 $41.50 +6.60; UMAC×19 yday $30.15 → 09:30 $28.59 +29.64; NPWR×331 yday $1.73 → 09:30 $1.70 +9.93; LPTH×42 yday $14.80 → 09:30 $14.01 +33.18; NMAX×58 yday $10.36 → 09:30 $10.31 +2.90; ALOY×43 yday $13.86 → 09:30 $13.19 +28.60; INO×594 yday $1.15 → 09:30 $1.14 +5.94 |
| 2026-08-19 | -7.20 | $10,315.91 | — | $10,315.91 | +0.00 | — | — | $10,315.91 | $10,315.91 | — | 09:30 open · cash $10,315.91 · no holdings · equity $10,315.91 vs prior close $10,315.91 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $10,315.91 | — | $10,315.91 | +0.00 | MRNA, AZI, CYPH, BNTX, BTGO, ASST, PPC, ABCL | — | $15,285.67 | $10,288.52 | MRNA×4, AZI×470, CYPH×560, BNTX×5, BTGO×97, ASST×40, PPC×21, ABCL×54 | 09:30 open · cash $10,315.91 · no holdings · equity $10,315.91 vs prior close $10,315.91 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $15,285.67 | MRNA×4, AZI×470, CYPH×560, BNTX×5, BTGO×97, ASST×40, PPC×21, ABCL×54 | $10,114.17 | -174.35 | AU, AEM, ARCT, INDP, CAN, DFDV, TEM | MRNA, AZI, BNTX, BTGO, ASST, PPC, ABCL | $15,695.79 | $9,697.39 | CYPH×560, AU×6, AEM×3, ARCT×64, INDP×518, CAN×2452, DFDV×178, TEM×10 | 09:30 open · cash $15,285.67 (unchanged overnight, no fees) · equity $10,114.17 vs prior close $10,288.52 (-174.35) because holdings re-marked: MRNA×4 yday $133.32 → 09:30 $133.11 +0.84; AZI×470 yday $1.44 → 09:30 $1.46 -9.40; CYPH×560 yday $1.19 → 09:30 $1.32 -72.80; BNTX×5 yday $110.89 → 09:30 $110.92 -0.15; BTGO×97 yday $6.60 → 09:30 $6.95 -33.95; ASST×40 yday $16.13 → 09:30 $17.66 -61.20; PPC×21 yday $31.24 → 09:30 $31.13 +2.31; ABCL×54 yday $11.57 → 09:30 $11.57 +0.00 |
| 2026-08-24 | -5.17 | $15,695.79 | CYPH×560, AU×6, AEM×3, ARCT×64, INDP×518, CAN×2452, DFDV×178, TEM×10 | $9,434.78 | -262.61 | — | CYPH, AU, AEM, INDP, DFDV, TEM | $11,192.72 | $9,404.84 | ARCT×64, CAN×2452 | 09:30 open · cash $15,695.79 (unchanged overnight, no fees) · equity $9,434.78 vs prior close $9,697.39 (-262.61) because holdings re-marked: CYPH×560 yday $1.42 → 09:30 $1.83 -229.60; AU×6 yday $121.22 → 09:30 $120.50 +4.32; AEM×3 yday $216.06 → 09:30 $217.03 -2.91; ARCT×64 yday $13.45 → 09:30 $13.26 +12.16; INDP×518 yday $1.29 → 09:30 $1.24 +25.90; CAN×2452 yday $0.35 → 09:30 $0.38 -61.30; DFDV×178 yday $3.94 → 09:30 $4.15 -37.38; TEM×10 yday $72.69 → 09:30 $70.07 +26.20 |
| 2026-08-25 | +1.80 | $11,192.72 | ARCT×64, CAN×2452 | $9,343.20 | -61.64 | SUJA, CYPH, FWDI, DEFT, GORO, ASST, BMNR, RUM | ARCT, CAN | $13,918.45 | $9,391.98 | SUJA×66, CYPH×342, FWDI×97, DEFT×910, GORO×165, ASST×27, BMNR×23, RUM×62 | 09:30 open · cash $11,192.72 (unchanged overnight, no fees) · equity $9,343.20 vs prior close $9,404.84 (-61.64) because holdings re-marked: ARCT×64 yday $13.76 → 09:30 $14.34 -37.12; CAN×2452 yday $0.37 → 09:30 $0.38 -24.52 |
| 2026-08-26 | +2.02 | $13,918.45 | SUJA×66, CYPH×342, FWDI×97, DEFT×910, GORO×165, ASST×27, BMNR×23, RUM×62 | $9,391.98 | -0.00 | — | — | $13,918.45 | $9,297.62 | SUJA×66, CYPH×342, FWDI×97, DEFT×910, GORO×165, ASST×27, BMNR×23, RUM×62 | 09:30 open · cash $13,918.45 (unchanged overnight, no fees) · equity $9,391.98 vs prior close $9,391.98 (-0.00) because holdings re-marked: SUJA×66 yday $8.54 → 09:30 $8.54 +0.00; CYPH×342 yday $1.64 → 09:30 $1.64 +0.00; FWDI×97 yday $5.86 → 09:30 $5.86 +0.00; DEFT×910 yday $0.62 → 09:30 $0.62 +0.00; GORO×165 yday $3.56 → 09:30 $3.56 +0.00; ASST×27 yday $20.20 → 09:30 $20.20 +0.00; BMNR×23 yday $24.21 → 09:30 $24.21 +0.00; RUM×62 yday $9.35 → 09:30 $9.35 +0.00 |
| 2026-08-27 | — | $13,918.45 | SUJA×66, CYPH×342, FWDI×97, DEFT×910, GORO×165, ASST×27, BMNR×23, RUM×62 | $9,263.07 | -34.55 | — | SUJA, CYPH, FWDI, DEFT, GORO, ASST, BMNR, RUM | $9,237.21 | $9,237.21 | — | 09:30 open · cash $13,918.45 (unchanged overnight, no fees) · equity $9,263.07 vs prior close $9,297.62 (-34.55) because holdings re-marked: SUJA×66 yday $8.54 → 09:30 $9.39 -56.10; CYPH×342 yday $1.64 → 09:30 $1.60 +13.68; FWDI×97 yday $5.86 → 09:30 $5.97 -10.67; DEFT×910 yday $0.62 → 09:30 $0.60 +18.20; GORO×165 yday $3.56 → 09:30 $3.77 -34.65; ASST×27 yday $20.20 → 09:30 $20.72 -14.04; BMNR×23 yday $24.21 → 09:30 $24.24 -0.69; RUM×62 yday $9.35 → 09:30 $10.07 -44.64 |
| 2026-08-28 | +0.75 | $9,237.21 | — | $9,237.21 | -0.00 | FIGR, XHG, DEFT, ERO, TRLV, FUTU, TXG, WPM | — | $13,602.85 | $9,202.77 | FIGR×15, XHG×142, DEFT×962, ERO×14, TRLV×50, FUTU×4, TXG×9, WPM×3 | 09:30 open · cash $9,237.21 · no holdings · equity $9,237.21 vs prior close $9,237.21 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-31 | -5.85 | $13,602.85 | FIGR×15, XHG×142, DEFT×962, ERO×14, TRLV×50, FUTU×4, TXG×9, WPM×3 | $9,327.68 | +124.91 | — | FIGR, DEFT, ERO, TRLV, FUTU, TXG, WPM | $9,795.09 | $9,306.61 | XHG×142 | 09:30 open · cash $13,602.85 (unchanged overnight, no fees) · equity $9,327.68 vs prior close $9,202.77 (+124.91) because holdings re-marked: FIGR×15 yday $38.02 → 09:30 $35.50 +37.80; XHG×142 yday $3.80 → 09:30 $3.44 +51.12; DEFT×962 yday $0.65 → 09:30 $0.62 +28.86; ERO×14 yday $39.82 → 09:30 $38.60 +17.08; TRLV×50 yday $11.03 → 09:30 $12.41 -69.00; FUTU×4 yday $124.57 → 09:30 $122.82 +7.00; TXG×9 yday $64.85 → 09:30 $60.90 +35.55; WPM×3 yday $157.99 → 09:30 $152.49 +16.50 |
| 2026-09-01 | -6.30 | $9,795.09 | XHG×142 | $9,295.25 | -11.36 | — | — | $9,795.09 | $9,308.03 | XHG×142 | 09:30 open · cash $9,795.09 (unchanged overnight, no fees) · equity $9,295.25 vs prior close $9,306.61 (-11.36) because holdings re-marked: XHG×142 yday $3.44 → 09:30 $3.52 -11.36 |
| 2026-09-02 | -3.83 | $9,795.09 | XHG×142 | $9,300.93 | -7.10 | — | — | $9,795.09 | $9,296.67 | XHG×142 | 09:30 open · cash $9,795.09 (unchanged overnight, no fees) · equity $9,300.93 vs prior close $9,308.03 (-7.10) because holdings re-marked: XHG×142 yday $3.43 → 09:30 $3.48 -7.10 |
| 2026-09-03 | -0.90 | $9,795.09 | XHG×142 | $9,288.15 | -8.52 | DEFT, MRNA, ARCT, ALEC, CAN, ERO, TRLV | — | $14,313.37 | $9,209.14 | XHG×142, DEFT×990, MRNA×4, ARCT×40, ALEC×276, CAN×2211, ERO×18, TRLV×56 | 09:30 open · cash $9,795.09 (unchanged overnight, no fees) · equity $9,288.15 vs prior close $9,296.67 (-8.52) because holdings re-marked: XHG×142 yday $3.51 → 09:30 $3.57 -8.52 |
| 2026-09-04 | — | $14,313.37 | XHG×142, DEFT×990, MRNA×4, ARCT×40, ALEC×276, CAN×2211, ERO×18, TRLV×56 | $9,127.77 | -81.37 | HQ, OABI, BRR | MRNA, ARCT, CAN | $16,806.65 | $9,150.32 | XHG×142, DEFT×990, ALEC×276, ERO×18, TRLV×56, HQ×88, OABI×298, BRR×643 | 09:30 open · cash $14,313.37 (unchanged overnight, no fees) · equity $9,127.77 vs prior close $9,209.14 (-81.37) because holdings re-marked: XHG×142 yday $3.32 → 09:30 $3.38 -8.52; DEFT×990 yday $0.65 → 09:30 $0.65 +0.00; MRNA×4 yday $150.81 → 09:30 $145.95 +19.44; ARCT×40 yday $16.74 → 09:30 $16.77 -1.20; ALEC×276 yday $2.72 → 09:30 $2.70 +5.52; CAN×2211 yday $0.31 → 09:30 $0.34 -66.33; ERO×18 yday $34.76 → 09:30 $35.82 -19.08; TRLV×56 yday $11.69 → 09:30 $11.89 -11.20 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **SHORT** | `TNDM` | 214 | $23.33 | $2.97 | — | $14,989.65 | — | ret_5>15; gate ret_5_min=15.0; list flatten; ⚪; ret5=+19.7; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,989.65 | ▲ 09:30 equity $10,084.77 vs yday $10,039.83 (+44.94) | 09:30 open · cash $14,989.65 (unchanged overnight, no fees) · equity $10,084.77 vs prior close $10,039.83 (+44.94) because holdings re-marked: TNDM×214 yday $23.13 → 09:30 $22.92 +44.94 | — |
| 2026-08-14 09:30 ET | **COVER** | `TNDM` | 214 | $22.92 | $2.76 | $+82.01 | $10,082.01 | ▲ +82.01 after sell → book $10,082.01; vs 09:30 mark -2.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 32 | $19.57 | $2.12 | — | $10,706.12 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 36 | $17.35 | $2.14 | — | $11,328.59 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $630.13 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AIRO` | 56 | $11.12 | $2.20 | — | $11,949.11 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 453 | $1.39 | $5.95 | — | $12,572.84 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $630.13 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `QMLS` | 86 | $7.29 | $2.29 | — | $13,197.49 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AVAH` | 52 | $11.91 | $2.18 | — | $13,814.62 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+21.3; leftover $630.13 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `TBBB` | 12 | $48.82 | $2.06 | — | $14,398.40 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AMPY` | 127 | $4.94 | $2.42 | — | $15,023.36 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,023.36 | ▲ 09:30 equity $10,201.66 vs yday $10,193.38 (+8.28) | 09:30 open · cash $15,023.36 (unchanged overnight, no fees) · equity $10,201.66 vs prior close $10,193.38 (+8.28) because holdings re-marked: ARX×32 yday $19.58 → 09:30 $19.57 +0.32; OMER×36 yday $17.19 → 09:30 $17.17 +0.72; AIRO×56 yday $9.57 → 09:30 $9.57 +0.00; MXCT×453 yday $1.32 → 09:30 $1.32 +0.00; QMLS×86 yday $7.32 → 09:30 $7.24 +6.88; AVAH×52 yday $12.32 → 09:30 $12.21 +5.72; TBBB×12 yday $47.79 → 09:30 $47.39 +4.80; AMPY×127 yday $4.78 → 09:30 $4.86 -10.16 | — |
| 2026-08-17 09:30 ET | **COVER** | `ARX` | 32 | $19.57 | $2.09 | $-4.21 | $14,395.04 | ▼ -4.21 after sell → book $10,199.58; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `OMER` | 36 | $17.17 | $2.10 | $+2.25 | $13,774.82 | ▲ +2.25 after sell → book $10,197.48; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AIRO` | 56 | $9.57 | $2.16 | $+82.45 | $13,236.74 | ▲ +82.45 after sell → book $10,195.32; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MXCT` | 453 | $1.32 | $5.84 | $+19.92 | $12,632.94 | ▲ +19.92 after sell → book $10,189.48; vs 09:30 mark -5.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `QMLS` | 86 | $7.24 | $2.25 | $-0.24 | $12,008.05 | ▼ -0.24 after sell → book $10,187.23; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AVAH` | 52 | $12.21 | $2.15 | $-19.93 | $11,370.98 | ▼ -19.93 after sell → book $10,185.08; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `TBBB` | 12 | $47.39 | $2.03 | $+13.07 | $10,800.28 | ▲ +13.07 after sell → book $10,183.06; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AMPY` | 127 | $4.86 | $2.37 | $+5.37 | $10,180.69 | ▲ +5.37 after sell → book $10,180.69; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 92 | $6.87 | $2.31 | — | $10,810.42 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+62.6; leftover $636.29 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HTFL` | 15 | $41.23 | $2.07 | — | $11,426.80 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+46.0; leftover $636.29 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `UMAC` | 19 | $32.55 | $2.08 | — | $12,043.16 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `NPWR` | 331 | $1.92 | $4.35 | — | $12,674.33 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `LPTH` | 42 | $14.94 | $2.15 | — | $13,299.66 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `NMAX` | 58 | $10.97 | $2.20 | — | $13,933.72 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `ALOY` | 43 | $14.66 | $2.16 | — | $14,561.94 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $636.29 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `INO` | 594 | $1.07 | $7.79 | — | $15,189.73 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+62.7; leftover $636.29 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,189.73 | ▲ 09:30 equity $10,340.59 vs yday $10,228.41 (+112.18) | 09:30 open · cash $15,189.73 (unchanged overnight, no fees) · equity $10,340.59 vs prior close $10,228.41 (+112.18) because holdings re-marked: CAPR×92 yday $7.45 → 09:30 $7.50 -4.60; HTFL×15 yday $41.94 → 09:30 $41.50 +6.60; UMAC×19 yday $30.15 → 09:30 $28.59 +29.64; NPWR×331 yday $1.73 → 09:30 $1.70 +9.93; LPTH×42 yday $14.80 → 09:30 $14.01 +33.18; NMAX×58 yday $10.36 → 09:30 $10.31 +2.90; ALOY×43 yday $13.86 → 09:30 $13.19 +28.60; INO×594 yday $1.15 → 09:30 $1.14 +5.94 | — |
| 2026-08-18 09:30 ET | **COVER** | `CAPR` | 92 | $7.50 | $2.27 | $-62.53 | $14,497.46 | ▼ -62.53 after sell → book $10,338.32; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `HTFL` | 15 | $41.50 | $2.04 | $-8.16 | $13,872.93 | ▼ -8.16 after sell → book $10,336.29; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `UMAC` | 19 | $28.59 | $2.05 | $+71.11 | $13,327.67 | ▲ +71.11 after sell → book $10,334.24; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NPWR` | 331 | $1.70 | $4.27 | $+64.20 | $12,760.70 | ▲ +64.20 after sell → book $10,329.97; vs 09:30 mark -4.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `LPTH` | 42 | $14.01 | $2.12 | $+34.79 | $12,170.17 | ▲ +34.79 after sell → book $10,327.86; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NMAX` | 58 | $10.31 | $2.16 | $+33.91 | $11,570.02 | ▲ +33.91 after sell → book $10,325.69; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `ALOY` | 43 | $13.19 | $2.12 | $+58.93 | $11,000.73 | ▲ +58.93 after sell → book $10,323.57; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `INO` | 594 | $1.14 | $7.66 | $-57.03 | $10,315.91 | ▼ -57.03 after sell → book $10,315.91; vs 09:30 mark -7.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,315.91 | ▲ 09:30 equity $10,315.91 vs yday $10,315.91 (+0.00) | 09:30 open · cash $10,315.91 · no holdings · equity $10,315.91 vs prior close $10,315.91 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,315.91 | ▲ 09:30 equity $10,315.91 vs yday $10,315.91 (+0.00) | 09:30 open · cash $10,315.91 · no holdings · equity $10,315.91 vs prior close $10,315.91 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **SHORT** | `MRNA` | 4 | $150.14 | $2.04 | — | $10,914.43 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $644.74 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AZI` | 470 | $1.37 | $6.17 | — | $11,552.16 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $644.74 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `CYPH` | 560 | $1.15 | $7.34 | — | $12,188.82 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $644.74 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `BNTX` | 5 | $109.06 | $2.04 | — | $12,732.08 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $644.74 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `BTGO` | 97 | $6.61 | $2.32 | — | $13,370.44 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $644.74 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ASST` | 40 | $16.00 | $2.15 | — | $14,008.29 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $644.74 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `PPC` | 21 | $30.65 | $2.09 | — | $14,649.85 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+16.5; leftover $644.74 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 54 | $11.81 | $2.19 | — | $15,285.67 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $644.74 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,285.67 | ▼ 09:30 equity $10,114.17 vs yday $10,288.52 (-174.35) | 09:30 open · cash $15,285.67 (unchanged overnight, no fees) · equity $10,114.17 vs prior close $10,288.52 (-174.35) because holdings re-marked: MRNA×4 yday $133.32 → 09:30 $133.11 +0.84; AZI×470 yday $1.44 → 09:30 $1.46 -9.40; CYPH×560 yday $1.19 → 09:30 $1.32 -72.80; BNTX×5 yday $110.89 → 09:30 $110.92 -0.15; BTGO×97 yday $6.60 → 09:30 $6.95 -33.95; ASST×40 yday $16.13 → 09:30 $17.66 -61.20; PPC×21 yday $31.24 → 09:30 $31.13 +2.31; ABCL×54 yday $11.57 → 09:30 $11.57 +0.00 | — |
| 2026-08-21 09:30 ET | **COVER** | `MRNA` | 4 | $133.11 | $2.00 | $+64.08 | $14,751.23 | ▲ +64.08 after sell → book $10,112.17; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `AZI` | 470 | $1.46 | $6.06 | $-54.53 | $14,058.97 | ▼ -54.53 after sell → book $10,106.11; vs 09:30 mark -6.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `BNTX` | 5 | $110.92 | $2.00 | $-13.34 | $13,502.36 | ▼ -13.34 after sell → book $10,104.10; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `BTGO` | 97 | $6.95 | $2.28 | $-38.07 | $12,825.93 | ▼ -38.07 after sell → book $10,101.82; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ASST` | 40 | $17.66 | $2.11 | $-70.66 | $12,117.42 | ▼ -70.66 after sell → book $10,099.71; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `PPC` | 21 | $31.13 | $2.05 | $-14.22 | $11,461.64 | ▼ -14.22 after sell → book $10,097.66; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ABCL` | 54 | $11.57 | $2.15 | $+8.89 | $10,834.71 | ▲ +8.89 after sell → book $10,095.51; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `AU` | 6 | $119.43 | $2.05 | — | $11,549.24 | — | ret_5>15; gate ret_5_min=15.0; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AEM` | 3 | $216.30 | $2.04 | — | $12,196.10 | — | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARCT` | 64 | $11.13 | $2.22 | — | $12,906.20 | — | ret_5>15; gate ret_5_min=15.0; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `INDP` | 518 | $1.39 | $6.80 | — | $13,619.42 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2452 | $0.29 | $15.00 | — | $14,325.31 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $721.11 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `DFDV` | 178 | $4.04 | $2.58 | — | $15,041.84 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $721.11 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `TEM` | 10 | $65.60 | $2.06 | — | $15,695.79 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+22.8; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,695.79 | ▼ 09:30 equity $9,434.78 vs yday $9,697.39 (-262.61) | 09:30 open · cash $15,695.79 (unchanged overnight, no fees) · equity $9,434.78 vs prior close $9,697.39 (-262.61) because holdings re-marked: CYPH×560 yday $1.42 → 09:30 $1.83 -229.60; AU×6 yday $121.22 → 09:30 $120.50 +4.32; AEM×3 yday $216.06 → 09:30 $217.03 -2.91; ARCT×64 yday $13.45 → 09:30 $13.26 +12.16; INDP×518 yday $1.29 → 09:30 $1.24 +25.90; CAN×2452 yday $0.35 → 09:30 $0.38 -61.30; DFDV×178 yday $3.94 → 09:30 $4.15 -37.38; TEM×10 yday $72.69 → 09:30 $70.07 +26.20 | — |
| 2026-08-24 09:30 ET | **COVER** | `CYPH` | 560 | $1.83 | $7.22 | $-395.37 | $14,663.76 | ▼ -395.37 after sell → book $9,427.55; vs 09:30 mark -7.23 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AU` | 6 | $120.50 | $2.01 | $-10.48 | $13,938.75 | ▼ -10.48 after sell → book $9,425.54; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AEM` | 3 | $217.03 | $2.00 | $-6.23 | $13,285.67 | ▼ -6.23 after sell → book $9,423.55; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `INDP` | 518 | $1.24 | $6.68 | $+64.22 | $12,636.66 | ▲ +64.22 after sell → book $9,416.86; vs 09:30 mark -6.69 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `DFDV` | 178 | $4.15 | $2.52 | $-24.69 | $11,895.44 | ▼ -24.69 after sell → book $9,414.34; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `TEM` | 10 | $70.07 | $2.02 | $-48.78 | $11,192.72 | ▼ -48.78 after sell → book $9,412.32; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,192.72 | ▼ 09:30 equity $9,343.20 vs yday $9,404.84 (-61.64) | 09:30 open · cash $11,192.72 (unchanged overnight, no fees) · equity $9,343.20 vs prior close $9,404.84 (-61.64) because holdings re-marked: ARCT×64 yday $13.76 → 09:30 $14.34 -37.12; CAN×2452 yday $0.37 → 09:30 $0.38 -24.52 | — |
| 2026-08-25 09:30 ET | **COVER** | `ARCT` | 64 | $14.34 | $2.18 | $-209.84 | $10,272.78 | ▼ -209.84 after sell → book $9,341.02; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **COVER** | `CAN` | 2452 | $0.38 | $16.67 | $-242.55 | $9,324.34 | ▼ -242.55 after sell → book $9,324.34; vs 09:30 mark -16.68 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **SHORT** | `SUJA` | 66 | $8.79 | $2.22 | — | $9,902.26 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $582.77 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `CYPH` | 342 | $1.70 | $4.49 | — | $10,479.16 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $582.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `FWDI` | 97 | $5.99 | $2.32 | — | $11,057.87 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $582.77 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `DEFT` | 910 | $0.64 | $8.73 | — | $11,631.54 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $582.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `GORO` | 165 | $3.53 | $2.54 | — | $12,211.45 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+16.0; leftover $582.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `ASST` | 27 | $20.90 | $2.11 | — | $12,773.65 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+47.9; leftover $582.77 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `BMNR` | 23 | $24.73 | $2.09 | — | $13,340.34 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; ret5=+26.3; leftover $582.77 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `RUM` | 62 | $9.36 | $2.21 | — | $13,918.45 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+21.3; leftover $582.77 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,918.45 | ▲ 09:30 equity $9,391.98 vs yday $9,391.98 (-0.00) | 09:30 open · cash $13,918.45 (unchanged overnight, no fees) · equity $9,391.98 vs prior close $9,391.98 (-0.00) because holdings re-marked: SUJA×66 yday $8.54 → 09:30 $8.54 +0.00; CYPH×342 yday $1.64 → 09:30 $1.64 +0.00; FWDI×97 yday $5.86 → 09:30 $5.86 +0.00; DEFT×910 yday $0.62 → 09:30 $0.62 +0.00; GORO×165 yday $3.56 → 09:30 $3.56 +0.00; ASST×27 yday $20.20 → 09:30 $20.20 +0.00; BMNR×23 yday $24.21 → 09:30 $24.21 +0.00; RUM×62 yday $9.35 → 09:30 $9.35 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,918.45 | ▼ 09:30 equity $9,263.07 vs yday $9,297.62 (-34.55) | 09:30 open · cash $13,918.45 (unchanged overnight, no fees) · equity $9,263.07 vs prior close $9,297.62 (-34.55) because holdings re-marked: SUJA×66 yday $8.54 → 09:30 $9.39 -56.10; CYPH×342 yday $1.64 → 09:30 $1.60 +13.68; FWDI×97 yday $5.86 → 09:30 $5.97 -10.67; DEFT×910 yday $0.62 → 09:30 $0.60 +18.20; GORO×165 yday $3.56 → 09:30 $3.77 -34.65; ASST×27 yday $20.20 → 09:30 $20.72 -14.04; BMNR×23 yday $24.21 → 09:30 $24.24 -0.69; RUM×62 yday $9.35 → 09:30 $10.07 -44.64 | — |
| 2026-08-27 09:30 ET | **COVER** | `SUJA` | 66 | $9.39 | $2.19 | $-44.01 | $13,296.52 | ▼ -44.01 after sell → book $9,260.88; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `CYPH` | 342 | $1.60 | $4.41 | $+25.29 | $12,744.91 | ▲ +25.29 after sell → book $9,256.47; vs 09:30 mark -4.41 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `FWDI` | 97 | $5.97 | $2.28 | $-2.66 | $12,163.54 | ▼ -2.66 after sell → book $9,254.19; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `DEFT` | 910 | $0.60 | $8.19 | $+19.48 | $11,609.35 | ▲ +19.48 after sell → book $9,246.00; vs 09:30 mark -8.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `GORO` | 165 | $3.77 | $2.48 | $-44.62 | $10,984.81 | ▼ -44.62 after sell → book $9,243.51; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `ASST` | 27 | $20.72 | $2.07 | $+0.68 | $10,423.30 | ▲ +0.68 after sell → book $9,241.44; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `BMNR` | 23 | $24.24 | $2.06 | $+7.12 | $9,863.72 | ▲ +7.12 after sell → book $9,239.38; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `RUM` | 62 | $10.07 | $2.18 | $-48.41 | $9,237.21 | ▼ -48.41 after sell → book $9,237.21; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,237.21 | ▲ 09:30 equity $9,237.21 vs yday $9,237.21 (-0.00) | 09:30 open · cash $9,237.21 · no holdings · equity $9,237.21 vs prior close $9,237.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIGR` | 15 | $37.42 | $2.07 | — | $9,796.44 | — | ret_5>15; gate ret_5_min=15.0; list yday_mover; ret5=+24.4; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SHORT** | `XHG` | 142 | $4.06 | $2.47 | — | $10,370.49 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.1; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `DEFT` | 962 | $0.60 | $8.84 | — | $10,938.85 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.6; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `ERO` | 14 | $39.20 | $2.07 | — | $11,485.58 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.6; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SHORT** | `TRLV` | 50 | $11.38 | $2.18 | — | $12,052.40 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+15.0; leftover $577.33 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `FUTU` | 4 | $128.00 | $2.04 | — | $12,562.37 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.5; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `TXG` | 9 | $64.10 | $2.05 | — | $13,137.22 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.1; leftover $577.33 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `WPM` | 3 | $155.89 | $2.03 | — | $13,602.85 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.6; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,602.85 | ▲ 09:30 equity $9,327.68 vs yday $9,202.77 (+124.91) | 09:30 open · cash $13,602.85 (unchanged overnight, no fees) · equity $9,327.68 vs prior close $9,202.77 (+124.91) because holdings re-marked: FIGR×15 yday $38.02 → 09:30 $35.50 +37.80; XHG×142 yday $3.80 → 09:30 $3.44 +51.12; DEFT×962 yday $0.65 → 09:30 $0.62 +28.86; ERO×14 yday $39.82 → 09:30 $38.60 +17.08; TRLV×50 yday $11.03 → 09:30 $12.41 -69.00; FUTU×4 yday $124.57 → 09:30 $122.82 +7.00; TXG×9 yday $64.85 → 09:30 $60.90 +35.55; WPM×3 yday $157.99 → 09:30 $152.49 +16.50 | — |
| 2026-08-31 09:30 ET | **COVER** | `FIGR` | 15 | $35.50 | $2.04 | $+24.69 | $13,068.32 | ▲ +24.69 after sell → book $9,325.65; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `DEFT` | 962 | $0.62 | $8.85 | $-36.93 | $12,463.03 | ▼ -36.93 after sell → book $9,316.80; vs 09:30 mark -8.85 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `ERO` | 14 | $38.60 | $2.03 | $+4.30 | $11,920.60 | ▲ +4.30 after sell → book $9,314.77; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **COVER** | `TRLV` | 50 | $12.41 | $2.14 | $-55.82 | $11,297.96 | ▼ -55.82 after sell → book $9,312.63; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `FUTU` | 4 | $122.82 | $2.00 | $+16.68 | $10,804.67 | ▲ +16.68 after sell → book $9,310.62; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `TXG` | 9 | $60.90 | $2.02 | $+24.73 | $10,254.56 | ▲ +24.73 after sell → book $9,308.61; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `WPM` | 3 | $152.49 | $2.00 | $+6.17 | $9,795.09 | ▲ +6.17 after sell → book $9,306.61; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,795.09 | ▼ 09:30 equity $9,295.25 vs yday $9,306.61 (-11.36) | 09:30 open · cash $9,795.09 (unchanged overnight, no fees) · equity $9,295.25 vs prior close $9,306.61 (-11.36) because holdings re-marked: XHG×142 yday $3.44 → 09:30 $3.52 -11.36 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,795.09 | ▼ 09:30 equity $9,300.93 vs yday $9,308.03 (-7.10) | 09:30 open · cash $9,795.09 (unchanged overnight, no fees) · equity $9,300.93 vs prior close $9,308.03 (-7.10) because holdings re-marked: XHG×142 yday $3.43 → 09:30 $3.48 -7.10 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,795.09 | ▼ 09:30 equity $9,288.15 vs yday $9,296.67 (-8.52) | 09:30 open · cash $9,795.09 (unchanged overnight, no fees) · equity $9,288.15 vs prior close $9,296.67 (-8.52) because holdings re-marked: XHG×142 yday $3.51 → 09:30 $3.57 -8.52 | — |
| 2026-09-03 09:30 ET | **SHORT** | `DEFT` | 990 | $0.67 | $9.80 | — | $10,448.59 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $663.44 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `MRNA` | 4 | $151.40 | $2.04 | — | $11,052.15 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $663.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `ARCT` | 40 | $16.46 | $2.15 | — | $11,708.41 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $663.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `ALEC` | 276 | $2.40 | $3.63 | — | $12,367.17 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+20.4; leftover $663.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CAN` | 2211 | $0.30 | $13.66 | — | $13,016.81 | — | ret_5>15; gate ret_5_min=15.0; list yday_mover; 🔵; ret5=+54.3; leftover $663.44 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `ERO` | 18 | $35.62 | $2.08 | — | $13,655.89 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+16.6; leftover $663.44 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `TRLV` | 56 | $11.78 | $2.20 | — | $14,313.37 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+15.0; leftover $663.44 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,313.37 | ▼ 09:30 equity $9,127.77 vs yday $9,209.14 (-81.37) | 09:30 open · cash $14,313.37 (unchanged overnight, no fees) · equity $9,127.77 vs prior close $9,209.14 (-81.37) because holdings re-marked: XHG×142 yday $3.32 → 09:30 $3.38 -8.52; DEFT×990 yday $0.65 → 09:30 $0.65 +0.00; MRNA×4 yday $150.81 → 09:30 $145.95 +19.44; ARCT×40 yday $16.74 → 09:30 $16.77 -1.20; ALEC×276 yday $2.72 → 09:30 $2.70 +5.52; CAN×2211 yday $0.31 → 09:30 $0.34 -66.33; ERO×18 yday $34.76 → 09:30 $35.82 -19.08; TRLV×56 yday $11.69 → 09:30 $11.89 -11.20 | — |
| 2026-09-04 09:30 ET | **COVER** | `MRNA` | 4 | $145.95 | $2.00 | $+17.76 | $13,727.57 | ▲ +17.76 after sell → book $9,125.77; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `ARCT` | 40 | $16.77 | $2.11 | $-16.66 | $13,054.66 | ▼ -16.66 after sell → book $9,123.66; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `CAN` | 2211 | $0.34 | $14.15 | $-116.25 | $12,288.77 | ▼ -116.25 after sell → book $9,109.51; vs 09:30 mark -14.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `HQ` | 88 | $17.06 | $2.32 | — | $13,787.73 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+17.3; leftover $1518.25 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `OABI` | 298 | $5.08 | $3.95 | — | $15,297.62 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1518.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `BRR` | 643 | $2.36 | $8.46 | — | $16,806.65 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $1518.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SNDK` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ASST` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `COIN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FWDI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GORO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMNR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RUM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `INDP` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `BRR` | no_price | no 09:30 open |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `XHG` | no_price | no 09:30 open |
| 2026-08-26 | `ERO` | no_price | no 09:30 open |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DEFT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DEFT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ERO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TRLV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FUTU` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XHG` | 142 | 2026-08-28 @ $4.06 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.1; leftover $577.33 |
| `DEFT` | 990 | 2026-09-03 @ $0.67 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $663.44 |
| `ALEC` | 276 | 2026-09-03 @ $2.40 | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+20.4; leftover $663.44 |
| `ERO` | 18 | 2026-09-03 @ $35.62 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+16.6; leftover $663.44 |
| `TRLV` | 56 | 2026-09-03 @ $11.78 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+15.0; leftover $663.44 |
| `HQ` | 88 | 2026-09-04 @ $17.06 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+17.3; leftover $1518.25 |
| `OABI` | 298 | 2026-09-04 @ $5.08 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1518.25 |
| `BRR` | 643 | 2026-09-04 @ $2.36 | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $1518.25 |
