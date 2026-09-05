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

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | TNDM | — | $14,989.65 | $-4,949.82 | $10,039.83 | TNDM×214 | SHORT TNDM x214 @ 23.33 |
| 2026-08-14 | +5.50 | $14,989.65 | TNDM×214 | ARX, OMER, AIRO, MXCT, QMLS, AVAH, TBBB, AMPY | TNDM | $15,023.36 | $-4,829.98 | $10,193.38 | ARX×32, OMER×36, AIRO×56, MXCT×453, QMLS×86, AVAH×52, TBBB×12, AMPY×127 | SELL TNDM (dropped from list after 1 sess (min 1)); SHORT ARX x32 @ 19.57; SHORT OMER x36 @ 17.35; SHORT AIRO x56 @ 11.12; SHORT MXCT x453 @ 1.39; SHORT QMLS x86 @ 7.29; SHORT AVAH x52 @ 11.91; SHORT TBBB x12 @ 48.82; SHORT AMPY x127 @ 4.94 |
| 2026-08-17 | +2.25 | $15,023.36 | ARX×32, OMER×36, AIRO×56, MXCT×453, QMLS×86, AVAH×52, TBBB×12, AMPY×127 | CAPR, HTFL, UMAC, NPWR, LPTH, NMAX, ALOY, INO | ARX, OMER, AIRO, MXCT, QMLS, AVAH, TBBB, AMPY | $15,189.73 | $-4,961.33 | $10,228.41 | CAPR×92, HTFL×15, UMAC×19, NPWR×331, LPTH×42, NMAX×58, ALOY×43, INO×594 | SELL ARX (dropped from list after 1 sess (min 1)); SELL OMER (dropped from list after 1 sess (min 1)); SELL AIRO (dropped from list after 1 sess (min 1)); SELL MXCT (dropped from list after 1 sess (min 1)); SELL QMLS (dropped from list after 1 sess (min 1)); SELL AVAH (dropped from list after 1 sess (min 1)); SELL TBBB (dropped from list after 1 sess (min 1)); SELL AMPY (dropped from list after 1 sess (min 1)); SHORT CAPR x92 @ 6.87; SHORT HTFL x15 @ 41.23; SHORT UMAC x19 @ 32.55; SHORT NPWR x331 @ 1.92; SHORT LPTH x42 @ 14.94; SHORT NMAX x58 @ 10.97; SHORT ALOY x43 @ 14.66; SHORT INO x594 @ 1.07 |
| 2026-08-18 | -6.20 | $15,189.73 | CAPR×92, HTFL×15, UMAC×19, NPWR×331, LPTH×42, NMAX×58, ALOY×43, INO×594 | — | CAPR, HTFL, UMAC, NPWR, LPTH, NMAX, ALOY, INO | $10,315.91 | $0.00 | $10,315.91 | — | SELL CAPR (dropped from list after 1 sess (min 1)); SELL HTFL (dropped from list after 1 sess (min 1)); SELL UMAC (dropped from list after 1 sess (min 1)); SELL NPWR (dropped from list after 1 sess (min 1)); SELL LPTH (dropped from list after 1 sess (min 1)); SELL NMAX (dropped from list after 1 sess (min 1)); SELL ALOY (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,315.91 | — | — | — | $10,315.91 | $0.00 | $10,315.91 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,315.91 | — | MRNA, AZI, CYPH, BNTX, BTGO, ASST, PPC, ABCL | — | $15,285.67 | $-4,997.15 | $10,288.52 | MRNA×4, AZI×470, CYPH×560, BNTX×5, BTGO×97, ASST×40, PPC×21, ABCL×54 | SHORT MRNA x4 @ 150.14; SHORT AZI x470 @ 1.37; SHORT CYPH x560 @ 1.15; SHORT BNTX x5 @ 109.06; SHORT BTGO x97 @ 6.61; SHORT ASST x40 @ 16.00; SHORT PPC x21 @ 30.65; SHORT ABCL x54 @ 11.81 |
| 2026-08-21 | +3.25 | $15,285.67 | MRNA×4, AZI×470, CYPH×560, BNTX×5, BTGO×97, ASST×40, PPC×21, ABCL×54 | AU, AEM, ARCT, INDP, CAN, DFDV, TEM | MRNA, AZI, BNTX, BTGO, ASST, PPC, ABCL | $15,695.79 | $-5,998.40 | $9,697.39 | CYPH×560, AU×6, AEM×3, ARCT×64, INDP×518, CAN×2452, DFDV×178, TEM×10 | SELL MRNA (dropped from list after 1 sess (min 1)); SELL AZI (dropped from list after 1 sess (min 1)); SELL BNTX (dropped from list after 1 sess (min 1)); SELL BTGO (dropped from list after 1 sess (min 1)); SELL ASST (dropped from list after 1 sess (min 1)); SELL PPC (dropped from list after 1 sess (min 1)); SELL ABCL (dropped from list after 1 sess (min 1)); SHORT AU x6 @ 119.43; SHORT AEM x3 @ 216.30; SHORT ARCT x64 @ 11.13; SHORT INDP x518 @ 1.39; SHORT CAN x2452 @ 0.29; SHORT DFDV x178 @ 4.04; SHORT TEM x10 @ 65.60 |
| 2026-08-24 | -5.17 | $15,695.79 | CYPH×560, AU×6, AEM×3, ARCT×64, INDP×518, CAN×2452, DFDV×178, TEM×10 | — | CYPH, AU, AEM, INDP, DFDV, TEM | $11,192.72 | $-1,787.88 | $9,404.84 | ARCT×64, CAN×2452 | SELL CYPH (dropped from list after 2 sess (min 1)); SELL AU (dropped from list after 1 sess (min 1)); SELL AEM (dropped from list after 1 sess (min 1)); SELL INDP (dropped from list after 1 sess (min 1)); SELL DFDV (dropped from list after 1 sess (min 1)); SELL TEM (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $11,192.72 | ARCT×64, CAN×2452 | SUJA, CYPH, FWDI, DEFT, GORO, ASST, BMNR, RUM | ARCT, CAN | $13,918.45 | $-4,526.47 | $9,391.98 | SUJA×66, CYPH×342, FWDI×97, DEFT×910, GORO×165, ASST×27, BMNR×23, RUM×62 | SELL ARCT (dropped from list after 2 sess (min 1)); SELL CAN (dropped from list after 2 sess (min 1)); SHORT SUJA x66 @ 8.79; SHORT CYPH x342 @ 1.70; SHORT FWDI x97 @ 5.99; SHORT DEFT x910 @ 0.64; SHORT GORO x165 @ 3.53; SHORT ASST x27 @ 20.90; SHORT BMNR x23 @ 24.73; SHORT RUM x62 @ 9.36 |
| 2026-08-26 | +2.02 | $13,918.45 | SUJA×66, CYPH×342, FWDI×97, DEFT×910, GORO×165, ASST×27, BMNR×23, RUM×62 | — | — | $13,918.45 | $-4,620.83 | $9,297.62 | SUJA×66, CYPH×342, FWDI×97, DEFT×910, GORO×165, ASST×27, BMNR×23, RUM×62 | hold SUJA,CYPH,FWDI,DEFT,GORO,ASST,BMNR,RUM |
| 2026-08-27 | — | $13,918.45 | SUJA×66, CYPH×342, FWDI×97, DEFT×910, GORO×165, ASST×27, BMNR×23, RUM×62 | — | SUJA, CYPH, FWDI, DEFT, GORO, ASST, BMNR, RUM | $9,237.21 | $0.00 | $9,237.21 | — | SELL SUJA (dropped from list after 2 sess (min 1)); SELL CYPH (dropped from list after 2 sess (min 1)); SELL FWDI (dropped from list after 2 sess (min 1)); SELL DEFT (dropped from list after 2 sess (min 1)); SELL GORO (dropped from list after 2 sess (min 1)); SELL ASST (dropped from list after 2 sess (min 1)); SELL BMNR (dropped from list after 2 sess (min 1)); SELL RUM (dropped from list after 2 sess (min 1)) |
| 2026-08-28 | +0.75 | $9,237.21 | — | FIGR, XHG, DEFT, ERO, TRLV, FUTU, TXG, WPM | — | $13,602.85 | $-4,400.08 | $9,202.77 | FIGR×15, XHG×142, DEFT×962, ERO×14, TRLV×50, FUTU×4, TXG×9, WPM×3 | SHORT FIGR x15 @ 37.42; SHORT XHG x142 @ 4.06; SHORT DEFT x962 @ 0.60; SHORT ERO x14 @ 39.20; SHORT TRLV x50 @ 11.38; SHORT FUTU x4 @ 128.00; SHORT TXG x9 @ 64.10; SHORT WPM x3 @ 155.89 |
| 2026-08-31 | -5.85 | $13,602.85 | FIGR×15, XHG×142, DEFT×962, ERO×14, TRLV×50, FUTU×4, TXG×9, WPM×3 | — | FIGR, DEFT, ERO, TRLV, FUTU, TXG, WPM | $9,795.09 | $-488.48 | $9,306.61 | XHG×142 | SELL FIGR (dropped from list after 1 sess (min 1)); SELL DEFT (dropped from list after 1 sess (min 1)); SELL ERO (dropped from list after 1 sess (min 1)); SELL TRLV (dropped from list after 1 sess (min 1)); SELL FUTU (dropped from list after 1 sess (min 1)); SELL TXG (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $9,795.09 | XHG×142 | — | — | $9,795.09 | $-487.06 | $9,308.03 | XHG×142 | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $9,795.09 | XHG×142 | — | — | $9,795.09 | $-498.42 | $9,296.67 | XHG×142 | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,795.09 | XHG×142 | DEFT, MRNA, ARCT, ALEC, CAN, ERO, TRLV | — | $14,313.37 | $-5,104.23 | $9,209.14 | XHG×142, DEFT×990, MRNA×4, ARCT×40, ALEC×276, CAN×2211, ERO×18, TRLV×56 | SHORT DEFT x990 @ 0.67; SHORT MRNA x4 @ 151.40; SHORT ARCT x40 @ 16.46; SHORT ALEC x276 @ 2.40; SHORT CAN x2211 @ 0.30; SHORT ERO x18 @ 35.62; SHORT TRLV x56 @ 11.78 |
| 2026-09-04 | — | $14,313.37 | XHG×142, DEFT×990, MRNA×4, ARCT×40, ALEC×276, CAN×2211, ERO×18, TRLV×56 | HQ, OABI, BRR | MRNA, ARCT, CAN | $16,806.65 | $-7,656.33 | $9,150.32 | XHG×142, DEFT×990, ALEC×276, ERO×18, TRLV×56, HQ×88, OABI×298, BRR×643 | SELL MRNA (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL CAN (dropped from list after 1 sess (min 1)); SHORT HQ x88 @ 17.06; SHORT OABI x298 @ 5.08; SHORT BRR x643 @ 2.36 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **SHORT** | `TNDM` | 214 | $23.33 | $2.97 | — | $14,989.65 | ret_5>15; gate ret_5_min=15.0; list flatten; ⚪; ret5=+19.7; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **COVER** | `TNDM` | 214 | $22.92 | $2.76 | $+82.01 | $10,082.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 32 | $19.57 | $2.12 | — | $10,706.12 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 36 | $17.35 | $2.14 | — | $11,328.59 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $630.13 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AIRO` | 56 | $11.12 | $2.20 | — | $11,949.11 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 453 | $1.39 | $5.95 | — | $12,572.84 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $630.13 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `QMLS` | 86 | $7.29 | $2.29 | — | $13,197.49 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AVAH` | 52 | $11.91 | $2.18 | — | $13,814.62 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+21.3; leftover $630.13 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `TBBB` | 12 | $48.82 | $2.06 | — | $14,398.40 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AMPY` | 127 | $4.94 | $2.42 | — | $15,023.36 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **COVER** | `ARX` | 32 | $19.57 | $2.09 | $-4.21 | $14,395.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `OMER` | 36 | $17.17 | $2.10 | $+2.25 | $13,774.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AIRO` | 56 | $9.57 | $2.16 | $+82.45 | $13,236.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MXCT` | 453 | $1.32 | $5.84 | $+19.92 | $12,632.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `QMLS` | 86 | $7.24 | $2.25 | $-0.24 | $12,008.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AVAH` | 52 | $12.21 | $2.15 | $-19.93 | $11,370.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `TBBB` | 12 | $47.39 | $2.03 | $+13.07 | $10,800.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AMPY` | 127 | $4.86 | $2.37 | $+5.37 | $10,180.69 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 92 | $6.87 | $2.31 | — | $10,810.42 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+62.6; leftover $636.29 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HTFL` | 15 | $41.23 | $2.07 | — | $11,426.80 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+46.0; leftover $636.29 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `UMAC` | 19 | $32.55 | $2.08 | — | $12,043.16 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `NPWR` | 331 | $1.92 | $4.35 | — | $12,674.33 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `LPTH` | 42 | $14.94 | $2.15 | — | $13,299.66 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `NMAX` | 58 | $10.97 | $2.20 | — | $13,933.72 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `ALOY` | 43 | $14.66 | $2.16 | — | $14,561.94 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $636.29 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `INO` | 594 | $1.07 | $7.79 | — | $15,189.73 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+62.7; leftover $636.29 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `CAPR` | 92 | $7.50 | $2.27 | $-62.53 | $14,497.46 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `HTFL` | 15 | $41.50 | $2.04 | $-8.16 | $13,872.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `UMAC` | 19 | $28.59 | $2.05 | $+71.11 | $13,327.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NPWR` | 331 | $1.70 | $4.27 | $+64.20 | $12,760.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `LPTH` | 42 | $14.01 | $2.12 | $+34.79 | $12,170.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NMAX` | 58 | $10.31 | $2.16 | $+33.91 | $11,570.02 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `ALOY` | 43 | $13.19 | $2.12 | $+58.93 | $11,000.73 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `INO` | 594 | $1.14 | $7.66 | $-57.03 | $10,315.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **SHORT** | `MRNA` | 4 | $150.14 | $2.04 | — | $10,914.43 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $644.74 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AZI` | 470 | $1.37 | $6.17 | — | $11,552.16 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $644.74 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `CYPH` | 560 | $1.15 | $7.34 | — | $12,188.82 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $644.74 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `BNTX` | 5 | $109.06 | $2.04 | — | $12,732.08 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $644.74 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `BTGO` | 97 | $6.61 | $2.32 | — | $13,370.44 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $644.74 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ASST` | 40 | $16.00 | $2.15 | — | $14,008.29 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $644.74 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `PPC` | 21 | $30.65 | $2.09 | — | $14,649.85 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+16.5; leftover $644.74 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 54 | $11.81 | $2.19 | — | $15,285.67 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $644.74 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `MRNA` | 4 | $133.11 | $2.00 | $+64.08 | $14,751.23 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `AZI` | 470 | $1.46 | $6.06 | $-54.53 | $14,058.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `BNTX` | 5 | $110.92 | $2.00 | $-13.34 | $13,502.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `BTGO` | 97 | $6.95 | $2.28 | $-38.07 | $12,825.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ASST` | 40 | $17.66 | $2.11 | $-70.66 | $12,117.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `PPC` | 21 | $31.13 | $2.05 | $-14.22 | $11,461.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ABCL` | 54 | $11.57 | $2.15 | $+8.89 | $10,834.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `AU` | 6 | $119.43 | $2.05 | — | $11,549.24 | ret_5>15; gate ret_5_min=15.0; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AEM` | 3 | $216.30 | $2.04 | — | $12,196.10 | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARCT` | 64 | $11.13 | $2.22 | — | $12,906.20 | ret_5>15; gate ret_5_min=15.0; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `INDP` | 518 | $1.39 | $6.80 | — | $13,619.42 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2452 | $0.29 | $15.00 | — | $14,325.31 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $721.11 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `DFDV` | 178 | $4.04 | $2.58 | — | $15,041.84 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $721.11 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `TEM` | 10 | $65.60 | $2.06 | — | $15,695.79 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+22.8; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `CYPH` | 560 | $1.83 | $7.22 | $-395.37 | $14,663.76 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AU` | 6 | $120.50 | $2.01 | $-10.48 | $13,938.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AEM` | 3 | $217.03 | $2.00 | $-6.23 | $13,285.67 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `INDP` | 518 | $1.24 | $6.68 | $+64.22 | $12,636.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `DFDV` | 178 | $4.15 | $2.52 | $-24.69 | $11,895.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `TEM` | 10 | $70.07 | $2.02 | $-48.78 | $11,192.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **COVER** | `ARCT` | 64 | $14.34 | $2.18 | $-209.84 | $10,272.78 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **COVER** | `CAN` | 2452 | $0.38 | $16.67 | $-242.55 | $9,324.34 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **SHORT** | `SUJA` | 66 | $8.79 | $2.22 | — | $9,902.26 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $582.77 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `CYPH` | 342 | $1.70 | $4.49 | — | $10,479.16 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $582.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `FWDI` | 97 | $5.99 | $2.32 | — | $11,057.87 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $582.77 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `DEFT` | 910 | $0.64 | $8.73 | — | $11,631.54 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $582.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `GORO` | 165 | $3.53 | $2.54 | — | $12,211.45 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+16.0; leftover $582.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `ASST` | 27 | $20.90 | $2.11 | — | $12,773.65 | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+47.9; leftover $582.77 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `BMNR` | 23 | $24.73 | $2.09 | — | $13,340.34 | ret_5>15; gate ret_5_min=15.0; list yday_gainer; ret5=+26.3; leftover $582.77 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `RUM` | 62 | $9.36 | $2.21 | — | $13,918.45 | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+21.3; leftover $582.77 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **COVER** | `SUJA` | 66 | $9.39 | $2.19 | $-44.01 | $13,296.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `CYPH` | 342 | $1.60 | $4.41 | $+25.29 | $12,744.91 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `FWDI` | 97 | $5.97 | $2.28 | $-2.66 | $12,163.54 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `DEFT` | 910 | $0.60 | $8.19 | $+19.48 | $11,609.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `GORO` | 165 | $3.77 | $2.48 | $-44.62 | $10,984.81 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `ASST` | 27 | $20.72 | $2.07 | $+0.68 | $10,423.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `BMNR` | 23 | $24.24 | $2.06 | $+7.12 | $9,863.72 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `RUM` | 62 | $10.07 | $2.18 | $-48.41 | $9,237.21 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIGR` | 15 | $37.42 | $2.07 | — | $9,796.44 | ret_5>15; gate ret_5_min=15.0; list yday_mover; ret5=+24.4; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SHORT** | `XHG` | 142 | $4.06 | $2.47 | — | $10,370.49 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.1; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `DEFT` | 962 | $0.60 | $8.84 | — | $10,938.85 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.6; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `ERO` | 14 | $39.20 | $2.07 | — | $11,485.58 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.6; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SHORT** | `TRLV` | 50 | $11.38 | $2.18 | — | $12,052.40 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+15.0; leftover $577.33 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `FUTU` | 4 | $128.00 | $2.04 | — | $12,562.37 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.5; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `TXG` | 9 | $64.10 | $2.05 | — | $13,137.22 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.1; leftover $577.33 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `WPM` | 3 | $155.89 | $2.03 | — | $13,602.85 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.6; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `FIGR` | 15 | $35.50 | $2.04 | $+24.69 | $13,068.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `DEFT` | 962 | $0.62 | $8.85 | $-36.93 | $12,463.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `ERO` | 14 | $38.60 | $2.03 | $+4.30 | $11,920.60 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **COVER** | `TRLV` | 50 | $12.41 | $2.14 | $-55.82 | $11,297.96 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `FUTU` | 4 | $122.82 | $2.00 | $+16.68 | $10,804.67 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `TXG` | 9 | $60.90 | $2.02 | $+24.73 | $10,254.56 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `WPM` | 3 | $152.49 | $2.00 | $+6.17 | $9,795.09 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `DEFT` | 990 | $0.67 | $9.80 | — | $10,448.59 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $663.44 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `MRNA` | 4 | $151.40 | $2.04 | — | $11,052.15 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $663.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `ARCT` | 40 | $16.46 | $2.15 | — | $11,708.41 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $663.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `ALEC` | 276 | $2.40 | $3.63 | — | $12,367.17 | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+20.4; leftover $663.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CAN` | 2211 | $0.30 | $13.66 | — | $13,016.81 | ret_5>15; gate ret_5_min=15.0; list yday_mover; 🔵; ret5=+54.3; leftover $663.44 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `ERO` | 18 | $35.62 | $2.08 | — | $13,655.89 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+16.6; leftover $663.44 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `TRLV` | 56 | $11.78 | $2.20 | — | $14,313.37 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+15.0; leftover $663.44 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **COVER** | `MRNA` | 4 | $145.95 | $2.00 | $+17.76 | $13,727.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `ARCT` | 40 | $16.77 | $2.11 | $-16.66 | $13,054.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `CAN` | 2211 | $0.34 | $14.15 | $-116.25 | $12,288.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `HQ` | 88 | $17.06 | $2.32 | — | $13,787.73 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+17.3; leftover $1518.25 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `OABI` | 298 | $5.08 | $3.95 | — | $15,297.62 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1518.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `BRR` | 643 | $2.36 | $8.46 | — | $16,806.65 | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $1518.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |

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
