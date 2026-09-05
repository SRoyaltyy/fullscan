# Factor mine action — `union_earn_react_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ earn_react, no 🚨

Cash book **+1.42%** ($10,142) · signal-only (no cash/fees) was -10.92%. Starts YES **4/17**. Fills 104 · skips 40 · realized $+149.88.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `earn_react=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $223.46.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | INO, VOR | — | $21.06 | $10,748.47 | $10,769.53 | INO×6172, VOR×223 | BUY INO x6172 @ 0.81; BUY VOR x223 @ 22.01 |
| 2026-08-14 | +5.50 | $21.06 | INO×6172, VOR×223 | NMAX, AIRJ, AMAT, AMPG, BRUN, BZAI, DEFT, DGXX | INO, VOR | $336.60 | $10,247.19 | $10,583.79 | NMAX×137, AIRJ×246, AMAT×2, AMPG×311, BRUN×51, BZAI×1776, DEFT×2894, DGXX×347 | SELL INO (dropped from list after 1 sess (min 1)); SELL VOR (dropped from list after 1 sess (min 1)); BUY NMAX x137 @ 9.89; BUY AIRJ x246 @ 5.51; BUY AMAT x2 @ 499.40; BUY AMPG x311 @ 4.37; BUY BRUN x51 @ 26.25; BUY BZAI x1776 @ 0.77; BUY DEFT x2894 @ 0.47; BUY DGXX x347 @ 3.92 |
| 2026-08-17 | +2.25 | $336.60 | NMAX×137, AIRJ×246, AMAT×2, AMPG×311, BRUN×51, BZAI×1776, DEFT×2894, DGXX×347 | — | NMAX, AIRJ, AMAT, AMPG, BRUN, BZAI, DEFT, DGXX | $10,521.80 | $0.00 | $10,521.80 | — | SELL NMAX (dropped from list after 1 sess (min 1)); SELL AIRJ (dropped from list after 1 sess (min 1)); SELL AMAT (dropped from list after 1 sess (min 1)); SELL AMPG (dropped from list after 1 sess (min 1)); SELL BRUN (dropped from list after 1 sess (min 1)); SELL BZAI (dropped from list after 1 sess (min 1)); SELL DEFT (dropped from list after 1 sess (min 1)); SELL DGXX (dropped from list after 1 sess (min 1)) |
| 2026-08-18 | -6.20 | $10,521.80 | — | — | — | $10,521.80 | $0.00 | $10,521.80 | — | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,521.80 | — | — | — | $10,521.80 | $0.00 | $10,521.80 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,521.80 | — | AAP, AEG, ALVO, ATAT, ATHM, BABA, BILL, BULL | — | $152.93 | $10,226.99 | $10,379.92 | AAP×28, AEG×145, ALVO×338, ATAT×38, ATHM×58, BABA×10, BILL×26, BULL×132 | BUY AAP x28 @ 46.85; BUY AEG x145 @ 9.01; BUY ALVO x338 @ 3.89; BUY ATAT x38 @ 34.05; BUY ATHM x58 @ 22.44; BUY BABA x10 @ 123.47; BUY BILL x26 @ 49.00; BUY BULL x132 @ 9.94 |
| 2026-08-21 | +3.25 | $152.93 | AAP×28, AEG×145, ALVO×338, ATAT×38, ATHM×58, BABA×10, BILL×26, BULL×132 | BEKE, BJ, BKE, PSEC, ROST | AAP, AEG, ALVO, ATAT, ATHM, BABA, BILL, BULL | $119.22 | $10,274.69 | $10,393.91 | BEKE×115, BJ×22, BKE×48, PSEC×900, ROST×8 | SELL AAP (dropped from list after 1 sess (min 1)); SELL AEG (dropped from list after 1 sess (min 1)); SELL ALVO (dropped from list after 1 sess (min 1)); SELL ATAT (dropped from list after 1 sess (min 1)); SELL ATHM (dropped from list after 1 sess (min 1)); SELL BABA (dropped from list after 1 sess (min 1)); SELL BILL (dropped from list after 1 sess (min 1)); SELL BULL (dropped from list after 1 sess (min 1)); BUY BEKE x115 @ 17.93; BUY BJ x22 @ 93.98; BUY BKE x48 @ 43.08; BUY PSEC x900 @ 2.30; BUY ROST x8 @ 243.85 |
| 2026-08-24 | -5.17 | $119.22 | BEKE×115, BJ×22, BKE×48, PSEC×900, ROST×8 | — | BEKE, BJ, BKE, PSEC, ROST | $10,459.33 | $0.00 | $10,459.33 | — | SELL BEKE (dropped from list after 1 sess (min 1)); SELL BJ (dropped from list after 1 sess (min 1)); SELL BKE (dropped from list after 1 sess (min 1)); SELL PSEC (dropped from list after 1 sess (min 1)); SELL ROST (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10,459.33 | — | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | — | $178.12 | $10,198.41 | $10,376.53 | BMO×7, BNS×15, BZ×85, DKS×7, EH×234, GFI×27, GRRR×91, SHMD×277 | BUY BMO x7 @ 172.40; BUY BNS x15 @ 86.86; BUY BZ x85 @ 15.34; BUY DKS x7 @ 179.33; BUY EH x234 @ 5.57; BUY GFI x27 @ 47.68; BUY GRRR x91 @ 14.26; BUY SHMD x277 @ 4.71 |
| 2026-08-26 | +2.02 | $178.12 | BMO×7, BNS×15, BZ×85, DKS×7, EH×234, GFI×27, GRRR×91, SHMD×277 | — | — | $178.12 | $10,261.98 | $10,440.10 | BMO×7, BNS×15, BZ×85, DKS×7, EH×234, GFI×27, GRRR×91, SHMD×277 | hold BMO,BNS,BZ,DKS,EH,GFI,GRRR,SHMD |
| 2026-08-27 | — | $178.12 | BMO×7, BNS×15, BZ×85, DKS×7, EH×234, GFI×27, GRRR×91, SHMD×277 | NVDA | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | $100.06 | $9,434.70 | $9,534.76 | NVDA×45 | SELL BMO (dropped from list after 2 sess (min 1)); SELL BNS (dropped from list after 2 sess (min 1)); SELL BZ (dropped from list after 2 sess (min 1)); SELL DKS (dropped from list after 2 sess (min 1)); SELL EH (dropped from list after 2 sess (min 1)); SELL GFI (dropped from list after 2 sess (min 1)); SELL GRRR (dropped from list after 2 sess (min 1)); SELL SHMD (dropped from list after 2 sess (min 1)); BUY NVDA x45 @ 212.64 |
| 2026-08-28 | +0.75 | $100.06 | NVDA×45 | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | NVDA | $276.00 | $9,878.53 | $10,154.53 | ADSK×4, BBAR×84, ESTC×15, FINV×297, FRO×29, GAP×61, HAFN×160, IREN×31 | SELL NVDA (dropped from list after 1 sess (min 1)); BUY ADSK x4 @ 261.47; BUY BBAR x84 @ 14.96; BUY ESTC x15 @ 82.64; BUY FINV x297 @ 4.26; BUY FRO x29 @ 42.51; BUY GAP x61 @ 20.75; BUY HAFN x160 @ 7.91; BUY IREN x31 @ 40.65 |
| 2026-08-31 | -5.85 | $276.00 | ADSK×4, BBAR×84, ESTC×15, FINV×297, FRO×29, GAP×61, HAFN×160, IREN×31 | — | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | $10,151.10 | $0.00 | $10,151.10 | — | SELL ADSK (dropped from list after 1 sess (min 1)); SELL BBAR (dropped from list after 1 sess (min 1)); SELL ESTC (dropped from list after 1 sess (min 1)); SELL FINV (dropped from list after 1 sess (min 1)); SELL FRO (dropped from list after 1 sess (min 1)); SELL GAP (dropped from list after 1 sess (min 1)); SELL HAFN (dropped from list after 1 sess (min 1)); SELL IREN (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $10,151.10 | — | — | — | $10,151.10 | $0.00 | $10,151.10 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,151.10 | — | — | — | $10,151.10 | $0.00 | $10,151.10 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,151.10 | — | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | — | $427.61 | $9,667.13 | $10,094.74 | AI×123, AVGO×3, CHPT×239, CIEN×3, CPB×53, FIVE×5, HPE×24, MEI×69 | BUY AI x123 @ 10.30; BUY AVGO x3 @ 369.68; BUY CHPT x239 @ 5.30; BUY CIEN x3 @ 357.25; BUY CPB x53 @ 23.80; BUY FIVE x5 @ 244.98; BUY HPE x24 @ 51.99; BUY MEI x69 @ 18.22 |
| 2026-09-04 | — | $427.61 | AI×123, AVGO×3, CHPT×239, CIEN×3, CPB×53, FIVE×5, HPE×24, MEI×69 | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | $223.46 | $9,918.86 | $10,142.32 | AMBA×19, ASAN×124, DOCU×18, DOMO×335, GWRE×6, IOT×33, LULU×10, MAMA×81 | SELL AI (dropped from list after 1 sess (min 1)); SELL AVGO (dropped from list after 1 sess (min 1)); SELL CHPT (dropped from list after 1 sess (min 1)); SELL CIEN (dropped from list after 1 sess (min 1)); SELL CPB (dropped from list after 1 sess (min 1)); SELL FIVE (dropped from list after 1 sess (min 1)); SELL HPE (dropped from list after 1 sess (min 1)); SELL MEI (dropped from list after 1 sess (min 1)); BUY AMBA x19 @ 66.61; BUY ASAN x124 @ 10.16; BUY DOCU x18 @ 67.06; BUY DOMO x335 @ 3.78; BUY GWRE x6 @ 198.00; BUY IOT x33 @ 37.69; BUY LULU x10 @ 121.15; BUY MAMA x81 @ 15.62 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | ▼ $9,931.49 (-68.51) | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+13.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | ▼ $9,928.61 (-71.39) | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 6172 | $0.93 | $76.99 | $+595.14 | $5,684.04 | ▲ $10,886.63 (+886.63) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 223 | $23.33 | $2.96 | $+288.53 | $10,883.67 | ▲ $10,883.67 (+883.67) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 137 | $9.89 | $2.40 | — | $9,525.66 | ▲ $10,881.27 (+881.27) | union ∩ earn_react, no 🚨; gate earn_react=True; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRJ` | 246 | $5.51 | $3.17 | — | $8,167.02 | ▲ $10,878.10 (+878.10) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+13.1; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMAT` | 2 | $499.40 | $2.00 | — | $7,166.23 | ▲ $10,876.10 (+876.10) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.3; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMPG` | 311 | $4.37 | $4.01 | — | $5,803.77 | ▲ $10,872.09 (+872.09) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+10.3; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 51 | $26.25 | $2.14 | — | $4,463.13 | ▲ $10,869.95 (+869.95) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1776 | $0.77 | $18.93 | — | $3,083.78 | ▲ $10,851.02 (+851.02) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DEFT` | 2894 | $0.47 | $22.28 | — | $1,701.32 | ▲ $10,828.73 (+828.73) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+11.1; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DGXX` | 347 | $3.92 | $4.48 | — | $336.60 | ▲ $10,824.26 (+824.26) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+10.1; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 137 | $10.97 | $2.44 | $+142.44 | $1,837.06 | ▲ $10,576.18 (+576.18) | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `AIRJ` | 246 | $6.22 | $3.23 | $+168.26 | $3,363.95 | ▲ $10,572.95 (+572.95) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMAT` | 2 | $517.45 | $2.02 | $+32.08 | $4,396.83 | ▲ $10,570.94 (+570.94) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMPG` | 311 | $4.09 | $4.07 | $-94.54 | $5,664.74 | ▲ $10,566.86 (+566.86) | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 51 | $23.00 | $2.16 | $-169.80 | $6,835.58 | ▲ $10,564.70 (+564.70) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1776 | $0.55 | $15.44 | $-414.43 | $7,800.50 | ▲ $10,549.27 (+549.27) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DEFT` | 2894 | $0.47 | $22.92 | $-30.73 | $9,152.23 | ▲ $10,526.35 (+526.35) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DGXX` | 347 | $3.96 | $4.54 | $+4.86 | $10,521.80 | ▲ $10,521.80 (+521.80) | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 28 | $46.85 | $2.07 | — | $9,207.93 | ▲ $10,519.73 (+519.73) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.0; leftover $1315.23 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 145 | $9.01 | $2.42 | — | $7,899.05 | ▲ $10,517.30 (+517.30) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $1315.23 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 338 | $3.89 | $4.36 | — | $6,579.87 | ▲ $10,512.94 (+512.94) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.5; leftover $1315.23 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 38 | $34.05 | $2.10 | — | $5,283.87 | ▲ $10,510.84 (+510.84) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+9.3; leftover $1315.23 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 58 | $22.44 | $2.16 | — | $3,980.18 | ▲ $10,508.67 (+508.67) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.1; leftover $1315.23 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 10 | $123.47 | $2.02 | — | $2,743.46 | ▲ $10,506.65 (+506.65) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.9; leftover $1315.23 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 26 | $49.00 | $2.07 | — | $1,467.40 | ▲ $10,504.59 (+504.59) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.0; leftover $1315.23 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 132 | $9.94 | $2.39 | — | $152.93 | ▲ $10,502.20 (+502.20) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+12.6; leftover $1315.23 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AAP` | 28 | $42.41 | $2.09 | $-128.49 | $1,338.32 | ▲ $10,375.84 (+375.84) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 145 | $9.04 | $2.46 | $-0.53 | $2,646.66 | ▲ $10,373.38 (+373.38) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 338 | $4.32 | $4.43 | $+136.55 | $4,102.39 | ▲ $10,368.95 (+368.95) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 38 | $34.31 | $2.12 | $+5.65 | $5,404.04 | ▲ $10,366.82 (+366.82) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 58 | $22.20 | $2.18 | $-18.27 | $6,689.46 | ▲ $10,364.64 (+364.64) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BABA` | 10 | $125.35 | $2.04 | $+14.74 | $7,940.92 | ▲ $10,362.60 (+362.60) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BILL` | 26 | $47.50 | $2.09 | $-43.16 | $9,173.83 | ▲ $10,360.51 (+360.51) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BULL` | 132 | $8.99 | $2.42 | $-130.20 | $10,358.09 | ▲ $10,358.09 (+358.09) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 115 | $17.93 | $2.33 | — | $8,293.23 | ▲ $10,355.76 (+355.76) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $2071.62 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 22 | $93.98 | $2.06 | — | $6,223.62 | ▲ $10,353.70 (+353.70) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.4; leftover $2071.62 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 48 | $43.08 | $2.13 | — | $4,153.64 | ▲ $10,351.57 (+351.57) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; leftover $2071.62 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 900 | $2.30 | $11.61 | — | $2,072.03 | ▲ $10,339.96 (+339.96) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.0; leftover $2071.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ROST` | 8 | $243.85 | $2.01 | — | $119.22 | ▲ $10,337.94 (+337.94) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.5; leftover $2071.62 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 115 | $18.06 | $2.37 | $+9.67 | $2,193.75 | ▲ $10,477.39 (+477.39) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 22 | $97.02 | $2.08 | $+62.74 | $4,326.11 | ▲ $10,475.31 (+475.31) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 48 | $44.54 | $2.16 | $+65.78 | $6,461.86 | ▲ $10,473.14 (+473.14) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 900 | $2.34 | $11.78 | $+12.61 | $8,556.09 | ▲ $10,461.37 (+461.37) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ROST` | 8 | $238.16 | $2.04 | $-49.57 | $10,459.33 | ▲ $10,459.33 (+459.33) | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 7 | $172.40 | $2.01 | — | $9,250.52 | ▲ $10,457.32 (+457.32) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.1; leftover $1307.42 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 15 | $86.86 | $2.04 | — | $7,945.58 | ▲ $10,455.28 (+455.28) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.3; leftover $1307.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 85 | $15.34 | $2.25 | — | $6,639.44 | ▲ $10,453.04 (+453.04) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1307.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 7 | $179.33 | $2.01 | — | $5,382.12 | ▲ $10,451.03 (+451.03) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-9.3; leftover $1307.42 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 234 | $5.57 | $3.02 | — | $4,075.72 | ▲ $10,448.01 (+448.01) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-7.1; leftover $1307.42 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 27 | $47.68 | $2.07 | — | $2,786.29 | ▲ $10,445.94 (+445.94) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ⚪; ret5=+18.8; leftover $1307.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 91 | $14.26 | $2.26 | — | $1,486.36 | ▲ $10,443.67 (+443.67) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-1.9; leftover $1307.42 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 277 | $4.71 | $3.57 | — | $178.12 | ▲ $10,440.10 (+440.10) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-9.9; leftover $1307.42 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BMO` | 7 | $173.22 | $2.03 | $+1.70 | $1,388.63 | ▼ $9,688.42 (-311.58) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BNS` | 15 | $92.64 | $2.06 | $+82.61 | $2,776.17 | ▼ $9,686.36 (-313.64) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 85 | $16.77 | $2.27 | $+117.03 | $4,199.35 | ▼ $9,684.09 (-315.91) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 7 | $121.87 | $2.03 | $-406.26 | $5,050.41 | ▼ $9,682.06 (-317.94) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `EH` | 234 | $4.77 | $3.07 | $-193.29 | $6,163.52 | ▼ $9,678.99 (-321.01) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GFI` | 27 | $48.24 | $2.09 | $+10.96 | $7,463.91 | ▼ $9,676.90 (-323.10) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GRRR` | 91 | $14.03 | $2.29 | $-25.48 | $8,738.36 | ▼ $9,674.62 (-325.38) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SHMD` | 277 | $3.38 | $3.63 | $-375.61 | $9,670.99 | ▼ $9,670.99 (-329.01) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 45 | $212.64 | $2.12 | — | $100.06 | ▼ $9,668.86 (-331.14) | union ∩ earn_react, no 🚨; gate earn_react=True; list mover_buy; 🔵; ret5=-4.6; leftover $9670.99 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 45 | $222.86 | $2.22 | $+455.56 | $10,126.55 | ▲ $10,126.55 (+126.55) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.47 | $2.00 | — | $9,078.66 | ▲ $10,124.54 (+124.54) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.9; leftover $1265.82 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 84 | $14.96 | $2.24 | — | $7,819.78 | ▲ $10,122.30 (+122.30) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-8.4; leftover $1265.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 15 | $82.64 | $2.04 | — | $6,578.15 | ▲ $10,120.27 (+120.27) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.9; leftover $1265.82 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 297 | $4.26 | $3.83 | — | $5,309.10 | ▲ $10,116.44 (+116.44) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.7; leftover $1265.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 29 | $42.51 | $2.08 | — | $4,074.23 | ▲ $10,114.36 (+114.36) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+6.0; leftover $1265.82 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 61 | $20.75 | $2.17 | — | $2,806.31 | ▲ $10,112.19 (+112.19) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.9; leftover $1265.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 160 | $7.91 | $2.47 | — | $1,538.24 | ▲ $10,109.72 (+109.72) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.4; leftover $1265.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 31 | $40.65 | $2.08 | — | $276.00 | ▲ $10,107.63 (+107.63) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; leftover $1265.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $258.50 | $2.02 | $-15.90 | $1,307.98 | ▲ $10,168.21 (+168.21) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 84 | $14.50 | $2.27 | $-43.15 | $2,523.71 | ▲ $10,165.94 (+165.94) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 15 | $99.99 | $2.06 | $+256.16 | $4,021.51 | ▲ $10,163.89 (+163.89) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 297 | $3.46 | $3.89 | $-245.32 | $5,045.24 | ▲ $10,160.00 (+160.00) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 29 | $43.54 | $2.10 | $+25.70 | $6,305.80 | ▲ $10,157.90 (+157.90) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 61 | $22.89 | $2.19 | $+126.17 | $7,699.90 | ▲ $10,155.71 (+155.71) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 160 | $8.43 | $2.51 | $+78.22 | $9,046.19 | ▲ $10,153.20 (+153.20) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 31 | $35.71 | $2.10 | $-157.33 | $10,151.10 | ▲ $10,151.10 (+151.10) | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 123 | $10.30 | $2.36 | — | $8,881.84 | ▲ $10,148.74 (+148.74) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.7; leftover $1268.89 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $369.68 | $2.00 | — | $7,770.80 | ▲ $10,146.74 (+146.74) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.2; leftover $1268.89 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 239 | $5.30 | $3.08 | — | $6,501.01 | ▲ $10,143.65 (+143.65) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.1; leftover $1268.89 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $357.25 | $2.00 | — | $5,427.26 | ▲ $10,141.65 (+141.65) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-7.7; leftover $1268.89 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 53 | $23.80 | $2.15 | — | $4,163.72 | ▲ $10,139.51 (+139.51) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.5; leftover $1268.89 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 5 | $244.98 | $2.00 | — | $2,936.81 | ▲ $10,137.50 (+137.50) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.3; leftover $1268.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 24 | $51.99 | $2.06 | — | $1,686.99 | ▲ $10,135.44 (+135.44) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-9.0; leftover $1268.89 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 69 | $18.22 | $2.20 | — | $427.61 | ▲ $10,133.24 (+133.24) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-16.7; leftover $1268.89 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 123 | $10.74 | $2.39 | $+49.37 | $1,746.24 | ▲ $10,165.55 (+165.55) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $351.74 | $2.02 | $-57.84 | $2,799.44 | ▲ $10,163.53 (+163.53) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 239 | $6.90 | $3.14 | $+376.18 | $4,445.41 | ▲ $10,160.40 (+160.40) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $354.49 | $2.02 | $-12.30 | $5,506.86 | ▲ $10,158.38 (+158.38) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 53 | $22.32 | $2.17 | $-82.76 | $6,687.65 | ▲ $10,156.21 (+156.21) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 5 | $256.99 | $2.03 | $+56.02 | $7,970.57 | ▲ $10,154.18 (+154.18) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 24 | $47.60 | $2.08 | $-109.50 | $9,110.89 | ▲ $10,152.10 (+152.10) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 69 | $15.09 | $2.22 | $-220.39 | $10,149.88 | ▲ $10,149.88 (+149.88) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 19 | $66.61 | $2.05 | — | $8,882.25 | ▲ $10,147.84 (+147.84) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-10.1; leftover $1268.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 124 | $10.16 | $2.36 | — | $7,620.04 | ▲ $10,145.47 (+145.47) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+4.8; leftover $1268.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 18 | $67.06 | $2.04 | — | $6,410.92 | ▲ $10,143.43 (+143.43) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.1; leftover $1268.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 335 | $3.78 | $4.32 | — | $5,140.30 | ▲ $10,139.11 (+139.11) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.8; leftover $1268.74 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 6 | $198.00 | $2.01 | — | $3,950.29 | ▲ $10,137.10 (+137.10) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+7.7; leftover $1268.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 33 | $37.69 | $2.09 | — | $2,704.43 | ▲ $10,135.01 (+135.01) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.4; leftover $1268.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 10 | $121.15 | $2.02 | — | $1,490.91 | ▲ $10,132.99 (+132.99) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.3; leftover $1268.74 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 81 | $15.62 | $2.23 | — | $223.46 | ▲ $10,130.76 (+130.76) | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.7; leftover $1268.74 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LOW` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LZB` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MRCY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BNS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BZ` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DKS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `EH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GFI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GRRR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SHMD` | no_price | no 09:30 open — carry |
| 2026-08-26 | `TIGR` | no_price | no 09:30 open |
| 2026-08-26 | `ANF` | no_price | no 09:30 open |
| 2026-08-26 | `BBWI` | no_price | no 09:30 open |
| 2026-08-26 | `BOX` | no_price | no 09:30 open |
| 2026-08-26 | `DY` | no_price | no 09:30 open |
| 2026-08-26 | `FSCO` | no_price | no 09:30 open |
| 2026-08-26 | `HEI` | no_price | no 09:30 open |
| 2026-08-26 | `INTU` | no_price | no 09:30 open |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AMBA` | 19 | 2026-09-04 @ $66.61 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-10.1; leftover $1268.74 |
| `ASAN` | 124 | 2026-09-04 @ $10.16 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+4.8; leftover $1268.74 |
| `DOCU` | 18 | 2026-09-04 @ $67.06 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.1; leftover $1268.74 |
| `DOMO` | 335 | 2026-09-04 @ $3.78 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.8; leftover $1268.74 |
| `GWRE` | 6 | 2026-09-04 @ $198.00 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+7.7; leftover $1268.74 |
| `IOT` | 33 | 2026-09-04 @ $37.69 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.4; leftover $1268.74 |
| `LULU` | 10 | 2026-09-04 @ $121.15 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.3; leftover $1268.74 |
| `MAMA` | 81 | 2026-09-04 @ $15.62 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.7; leftover $1268.74 |
