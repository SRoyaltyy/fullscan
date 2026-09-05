# Factor mine action — `union_vol_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-6.39%** ($9,361) · signal-only (no cash/fees) was -11.83%. Starts YES **3/17**. Fills 118 · skips 44 · realized $-614.43.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $193.84.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | — | $3.57 | $9,798.40 | $9,801.97 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | BUY BTBT x833 @ 1.50; BUY BETR x84 @ 14.80; BUY ANGX x290 @ 4.31; BUY HYLN x299 @ 4.18; BUY ADUR x75 @ 16.50; BUY AIRO x112 @ 11.12; BUY NCMI x464 @ 2.69; BUY QMLS x170 @ 7.29 |
| 2026-08-17 | +2.25 | $3.57 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | CDNL, ABX, VERA, HTFL, UMAC, NPWR, LPTH, NMAX | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | $71.87 | $9,357.10 | $9,428.97 | CDNL×30, ABX×133, VERA×38, HTFL×29, UMAC×37, NPWR×633, LPTH×81, NMAX×110 | SELL BTBT (dropped from list after 1 sess (min 1)); SELL BETR (dropped from list after 1 sess (min 1)); SELL ANGX (dropped from list after 1 sess (min 1)); SELL HYLN (dropped from list after 1 sess (min 1)); SELL ADUR (dropped from list after 1 sess (min 1)); SELL AIRO (dropped from list after 1 sess (min 1)); SELL NCMI (dropped from list after 1 sess (min 1)); SELL QMLS (dropped from list after 1 sess (min 1)); BUY CDNL x30 @ 39.85; BUY ABX x133 @ 9.12; BUY VERA x38 @ 31.30; BUY HTFL x29 @ 41.23; BUY UMAC x37 @ 32.55; BUY NPWR x633 @ 1.92; BUY LPTH x81 @ 14.94; BUY NMAX x110 @ 10.97 |
| 2026-08-18 | -6.20 | $71.87 | CDNL×30, ABX×133, VERA×38, HTFL×29, UMAC×37, NPWR×633, LPTH×81, NMAX×110 | — | CDNL, ABX, VERA, HTFL, UMAC, NPWR, LPTH, NMAX | $9,292.33 | $0.00 | $9,292.33 | — | SELL CDNL (dropped from list after 1 sess (min 1)); SELL ABX (dropped from list after 1 sess (min 1)); SELL VERA (dropped from list after 1 sess (min 1)); SELL HTFL (dropped from list after 1 sess (min 1)); SELL UMAC (dropped from list after 1 sess (min 1)); SELL NPWR (dropped from list after 1 sess (min 1)); SELL LPTH (dropped from list after 1 sess (min 1)); SELL NMAX (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $9,292.33 | — | — | — | $9,292.33 | $0.00 | $9,292.33 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,292.33 | — | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $9.13 | $9,411.61 | $9,420.74 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×663, WPM×8, ABUS×236 | BUY AG x56 @ 20.55; BUY CDE x56 @ 20.65; BUY HDSN x201 @ 5.77; BUY IAG x59 @ 19.63; BUY KGC x39 @ 29.63; BUY NFGC x663 @ 1.75; BUY WPM x8 @ 144.54; BUY ABUS x236 @ 4.92 |
| 2026-08-21 | +3.25 | $9.13 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×663, WPM×8, ABUS×236 | AU, AUPH, AEM, ARCT, CYPH, BTBT, DE, GORO | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $727.79 | $9,241.98 | $9,969.77 | AU×10, AUPH×70, AEM×5, ARCT×109, CYPH×920, BTBT×732, DE×1, GORO×390 | SELL AG (dropped from list after 1 sess (min 1)); SELL CDE (dropped from list after 1 sess (min 1)); SELL HDSN (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); SELL ABUS (dropped from list after 1 sess (min 1)); BUY AU x10 @ 119.43; BUY AUPH x70 @ 17.20; BUY AEM x5 @ 216.30; BUY ARCT x109 @ 11.13; BUY CYPH x920 @ 1.32; BUY BTBT x732 @ 1.66; BUY DE x1 @ 623.26; BUY GORO x390 @ 3.11 |
| 2026-08-24 | -5.17 | $727.79 | AU×10, AUPH×70, AEM×5, ARCT×109, CYPH×920, BTBT×732, DE×1, GORO×390 | — | AU, AUPH, AEM, ARCT, CYPH, BTBT, DE, GORO | $10,307.74 | $0.00 | $10,307.74 | — | SELL AU (dropped from list after 1 sess (min 1)); SELL AUPH (dropped from list after 1 sess (min 1)); SELL AEM (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL CYPH (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); SELL DE (dropped from list after 1 sess (min 1)); SELL GORO (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10,307.74 | — | NPWR, ALVO, ZURA, CYPH, DEFT, GORO, ASST, BMNR | — | $1.65 | $10,154.73 | $10,156.38 | NPWR×644, ALVO×246, ZURA×201, CYPH×757, DEFT×2013, GORO×365, ASST×61, BMNR×51 | BUY NPWR x644 @ 2.00; BUY ALVO x246 @ 5.22; BUY ZURA x201 @ 6.38; BUY CYPH x757 @ 1.70; BUY DEFT x2013 @ 0.64; BUY GORO x365 @ 3.53; BUY ASST x61 @ 20.90; BUY BMNR x51 @ 24.73 |
| 2026-08-26 | +2.02 | $1.65 | NPWR×644, ALVO×246, ZURA×201, CYPH×757, DEFT×2013, GORO×365, ASST×61, BMNR×51 | — | — | $1.65 | $10,254.30 | $10,255.95 | NPWR×644, ALVO×246, ZURA×201, CYPH×757, DEFT×2013, GORO×365, ASST×61, BMNR×51 | hold NPWR,ALVO,ZURA,CYPH,DEFT,GORO,ASST,BMNR |
| 2026-08-27 | — | $1.65 | NPWR×644, ALVO×246, ZURA×201, CYPH×757, DEFT×2013, GORO×365, ASST×61, BMNR×51 | — | NPWR, ALVO, ZURA, CYPH, DEFT, GORO, ASST, BMNR | $9,945.20 | $0.00 | $9,945.20 | — | SELL NPWR (dropped from list after 2 sess (min 1)); SELL ALVO (dropped from list after 2 sess (min 1)); SELL ZURA (dropped from list after 2 sess (min 1)); SELL CYPH (dropped from list after 2 sess (min 1)); SELL DEFT (dropped from list after 2 sess (min 1)); SELL GORO (dropped from list after 2 sess (min 1)); SELL ASST (dropped from list after 2 sess (min 1)); SELL BMNR (dropped from list after 2 sess (min 1)) |
| 2026-08-28 | +0.75 | $9,945.20 | — | ANF, BHVN, BZ, URBN, ERAS, ZYME, GENB, TIGR | — | $113.38 | $9,475.39 | $9,588.77 | ANF×8, BHVN×73, BZ×67, URBN×15, ERAS×64, ZYME×42, GENB×72, TIGR×226 | BUY ANF x8 @ 144.70; BUY BHVN x73 @ 16.95; BUY BZ x67 @ 18.50; BUY URBN x15 @ 82.70; BUY ERAS x64 @ 19.30; BUY ZYME x42 @ 29.33; BUY GENB x72 @ 17.10; BUY TIGR x226 @ 5.49 |
| 2026-08-31 | -5.85 | $113.38 | ANF×8, BHVN×73, BZ×67, URBN×15, ERAS×64, ZYME×42, GENB×72, TIGR×226 | — | ANF, BHVN, BZ, URBN, ERAS, ZYME, GENB, TIGR | $9,384.44 | $0.00 | $9,384.44 | — | SELL ANF (dropped from list after 1 sess (min 1)); SELL BHVN (dropped from list after 1 sess (min 1)); SELL BZ (dropped from list after 1 sess (min 1)); SELL URBN (dropped from list after 1 sess (min 1)); SELL ERAS (dropped from list after 1 sess (min 1)); SELL ZYME (dropped from list after 1 sess (min 1)); SELL GENB (dropped from list after 1 sess (min 1)); SELL TIGR (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $9,384.44 | — | — | — | $9,384.44 | $0.00 | $9,384.44 | — | hard-red sit S=-6.30 |
| 2026-09-02 | -3.83 | $9,384.44 | — | — | — | $9,384.44 | $0.00 | $9,384.44 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,384.44 | — | RVTY, GPRO, CRK, MMED, DEFT, MRNA, ARCT, NVAX | — | $140.95 | $9,722.46 | $9,863.41 | RVTY×9, GPRO×961, CRK×74, MMED×51, DEFT×1750, MRNA×7, ARCT×71, NVAX×114 | BUY RVTY x9 @ 125.94; BUY GPRO x961 @ 1.22; BUY CRK x74 @ 15.70; BUY MMED x51 @ 22.78; BUY DEFT x1750 @ 0.67; BUY MRNA x7 @ 151.40; BUY ARCT x71 @ 16.46; BUY NVAX x114 @ 10.27 |
| 2026-09-04 | — | $140.95 | RVTY×9, GPRO×961, CRK×74, MMED×51, DEFT×1750, MRNA×7, ARCT×71, NVAX×114 | BAK, EOSE, DELL, OABI, ALEC, FMC, TARS | RVTY, CRK, MMED, DEFT, MRNA, ARCT, NVAX | $193.84 | $9,167.08 | $9,360.92 | GPRO×961, BAK×600, EOSE×328, DELL×2, OABI×230, ALEC×433, FMC×88, TARS×14 | SELL RVTY (dropped from list after 1 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); SELL MMED (dropped from list after 1 sess (min 1)); SELL DEFT (dropped from list after 1 sess (min 1)); SELL MRNA (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL NVAX (dropped from list after 1 sess (min 1)); BUY BAK x600 @ 1.95; BUY EOSE x328 @ 3.57; BUY DELL x2 @ 486.31; BUY OABI x230 @ 5.08; BUY ALEC x433 @ 2.70; BUY FMC x88 @ 13.30; BUY TARS x14 @ 82.76 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | combo gate; gate vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | combo gate; gate vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | combo gate; gate vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $2,499.51 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $1,245.37 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 170 | $7.29 | $2.50 | — | $3.57 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $1,258.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $2,404.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,735.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,957.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $6,134.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $7,204.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $8,497.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 170 | $7.24 | $2.54 | $-13.54 | $9,725.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 30 | $39.85 | $2.08 | — | $8,527.84 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1215.68 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 133 | $9.12 | $2.39 | — | $7,312.49 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1215.68 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 38 | $31.30 | $2.10 | — | $6,120.98 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=-3.8; leftover $1215.68 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,923.24 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+46.0; leftover $1215.68 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $3,716.79 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1215.68 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 633 | $1.92 | $8.17 | — | $2,493.26 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1215.68 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 81 | $14.94 | $2.23 | — | $1,280.89 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1215.68 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 110 | $10.97 | $2.32 | — | $71.87 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $1215.68 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 30 | $41.57 | $2.10 | $+47.42 | $1,316.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 133 | $9.03 | $2.42 | $-16.78 | $2,515.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 38 | $31.31 | $2.12 | $-3.85 | $3,703.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $4,904.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $5,960.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 633 | $1.70 | $8.28 | $-155.71 | $7,028.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 81 | $14.01 | $2.26 | $-79.82 | $8,160.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 110 | $10.31 | $2.35 | $-77.27 | $9,292.33 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 56 | $20.55 | $2.16 | — | $8,139.37 | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $6,980.81 | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 201 | $5.77 | $2.60 | — | $5,818.45 | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $4,658.11 | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,500.43 | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 663 | $1.75 | $8.55 | — | $2,331.63 | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,173.29 | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 236 | $4.92 | $3.04 | — | $9.13 | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 56 | $21.90 | $2.18 | $+71.26 | $1,233.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 56 | $21.75 | $2.18 | $+57.26 | $2,449.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 201 | $5.67 | $2.64 | $-25.34 | $3,586.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 59 | $21.17 | $2.19 | $+86.51 | $4,833.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 39 | $32.17 | $2.13 | $+94.83 | $6,085.55 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 663 | $1.79 | $8.67 | $+9.29 | $7,263.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $8,499.21 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 236 | $5.20 | $3.09 | $+59.94 | $9,723.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,527.00 | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 70 | $17.20 | $2.20 | — | $7,320.80 | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,237.29 | combo gate; gate vol=good,last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 109 | $11.13 | $2.32 | — | $5,021.81 | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 920 | $1.32 | $11.87 | — | $3,795.54 | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 732 | $1.66 | $9.44 | — | $2,570.98 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $1,945.72 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1215.41 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 390 | $3.11 | $5.03 | — | $727.79 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=+7.1; leftover $1215.41 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,930.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 70 | $16.60 | $2.22 | $-46.42 | $3,090.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $4,173.66 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 109 | $13.26 | $2.35 | $+227.51 | $5,616.65 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 920 | $1.83 | $12.03 | $+445.30 | $7,288.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 732 | $1.55 | $9.57 | $-99.54 | $8,413.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 1 | $653.62 | $2.01 | $+26.35 | $9,064.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 390 | $3.20 | $5.11 | $+24.96 | $10,307.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 644 | $2.00 | $8.31 | — | $9,011.43 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1288.47 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 246 | $5.22 | $3.17 | — | $7,724.14 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1288.47 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 201 | $6.38 | $2.60 | — | $6,439.16 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1288.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 757 | $1.70 | $9.77 | — | $5,142.50 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1288.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2013 | $0.64 | $18.92 | — | $3,835.25 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1288.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 365 | $3.53 | $4.71 | — | $2,542.10 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+16.0; leftover $1288.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 61 | $20.90 | $2.17 | — | $1,265.02 | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ret5=+47.9; leftover $1288.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 51 | $24.73 | $2.14 | — | $1.65 | combo gate; gate vol=good,last_green=True; list yday_gainer; ret5=+26.3; leftover $1288.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 644 | $1.93 | $8.42 | $-61.81 | $1,236.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 246 | $4.98 | $3.22 | $-65.44 | $2,458.00 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 201 | $6.13 | $2.64 | $-55.49 | $3,687.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 757 | $1.60 | $9.90 | $-95.37 | $4,888.79 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DEFT` | 2013 | $0.60 | $18.46 | $-117.90 | $6,078.13 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GORO` | 365 | $3.77 | $4.78 | $+78.11 | $7,449.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 61 | $20.72 | $2.19 | $-15.35 | $8,711.12 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMNR` | 51 | $24.24 | $2.16 | $-29.30 | $9,945.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $8,785.59 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1243.15 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 73 | $16.95 | $2.21 | — | $7,546.03 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1243.15 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 67 | $18.50 | $2.19 | — | $6,304.34 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1243.15 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 15 | $82.70 | $2.04 | — | $5,061.80 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1243.15 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 64 | $19.30 | $2.18 | — | $3,824.42 | combo gate; gate vol=good,last_green=True; list yday_gainer; ret5=-4.1; leftover $1243.15 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 42 | $29.33 | $2.12 | — | $2,590.44 | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1243.15 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GENB` | 72 | $17.10 | $2.21 | — | $1,357.04 | combo gate; gate vol=good,last_green=True; list yday_mover; ret5=+3.1; leftover $1243.15 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TIGR` | 226 | $5.49 | $2.92 | — | $113.38 | combo gate; gate vol=good,last_green=True; list ohlc_hot; ret5=+15.9; leftover $1243.15 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.67 | $2.03 | $+27.71 | $1,300.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 73 | $15.44 | $2.23 | $-114.67 | $2,425.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 67 | $17.89 | $2.21 | $-45.27 | $3,622.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 15 | $81.09 | $2.06 | $-28.24 | $4,836.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 64 | $17.90 | $2.20 | $-93.98 | $5,979.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 42 | $28.27 | $2.14 | $-48.77 | $7,164.91 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `GENB` | 72 | $15.33 | $2.23 | $-131.87 | $8,266.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 226 | $4.96 | $2.96 | $-125.66 | $9,384.44 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $8,248.96 | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1173.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 961 | $1.22 | $12.40 | — | $7,064.15 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1173.06 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 74 | $15.70 | $2.21 | — | $5,900.14 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1173.06 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $22.78 | $2.14 | — | $4,736.21 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1173.06 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1750 | $0.67 | $16.98 | — | $3,546.74 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1173.06 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 7 | $151.40 | $2.01 | — | $2,484.93 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1173.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 71 | $16.46 | $2.20 | — | $1,314.06 | combo gate; gate vol=good,last_green=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1173.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 114 | $10.27 | $2.33 | — | $140.95 | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1173.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $1,330.96 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 74 | $15.45 | $2.23 | $-22.95 | $2,472.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 51 | $23.88 | $2.16 | $+51.79 | $3,687.75 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DEFT` | 1750 | $0.65 | $16.93 | $-68.90 | $4,808.32 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 7 | $145.95 | $2.03 | $-42.19 | $5,827.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 71 | $16.77 | $2.22 | $+17.58 | $7,016.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 114 | $10.41 | $2.36 | $+11.27 | $8,200.77 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 600 | $1.95 | $7.74 | — | $7,023.03 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1171.54 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 328 | $3.57 | $4.23 | — | $5,847.83 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1171.54 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $4,873.22 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=-9.9; leftover $1171.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 230 | $5.08 | $2.97 | — | $3,701.85 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1171.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 433 | $2.70 | $5.59 | — | $2,527.17 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1171.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FMC` | 88 | $13.30 | $2.25 | — | $1,354.51 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+8.6; leftover $1171.54 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 14 | $82.76 | $2.03 | — | $193.84 | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1171.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `CHRS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DEFT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GORO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMNR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `DKS` | no_price | no 09:30 open |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 961 | 2026-09-03 @ $1.22 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1173.06 |
| `BAK` | 600 | 2026-09-04 @ $1.95 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1171.54 |
| `EOSE` | 328 | 2026-09-04 @ $3.57 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1171.54 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=-9.9; leftover $1171.54 |
| `OABI` | 230 | 2026-09-04 @ $5.08 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1171.54 |
| `ALEC` | 433 | 2026-09-04 @ $2.70 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1171.54 |
| `FMC` | 88 | 2026-09-04 @ $13.30 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+8.6; leftover $1171.54 |
| `TARS` | 14 | 2026-09-04 @ $82.76 | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1171.54 |
