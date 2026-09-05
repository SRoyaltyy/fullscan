# Factor mine action — `union_join_vol_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-2.54%** ($9,746) · signal-only (no cash/fees) was -4.08%. Starts YES **5/17**. Fills 104 · skips 21 · realized $-146.75.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `join=good,vol=good,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $363.12.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | — | $3.57 | $9,798.40 | $9,801.97 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | BUY BTBT x833 @ 1.50; BUY BETR x84 @ 14.80; BUY ANGX x290 @ 4.31; BUY HYLN x299 @ 4.18; BUY ADUR x75 @ 16.50; BUY AIRO x112 @ 11.12; BUY NCMI x464 @ 2.69; BUY QMLS x170 @ 7.29 |
| 2026-08-17 | +2.25 | $3.57 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | ABX, ALOY, BORR, XHG, MP | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | $29.01 | $9,419.99 | $9,449.00 | ABX×213, ALOY×132, BORR×423, XHG×464, MP×33 | SELL BTBT (dropped from list after 1 sess (min 1)); SELL BETR (dropped from list after 1 sess (min 1)); SELL ANGX (dropped from list after 1 sess (min 1)); SELL HYLN (dropped from list after 1 sess (min 1)); SELL ADUR (dropped from list after 1 sess (min 1)); SELL AIRO (dropped from list after 1 sess (min 1)); SELL NCMI (dropped from list after 1 sess (min 1)); SELL QMLS (dropped from list after 1 sess (min 1)); BUY ABX x213 @ 9.12; BUY ALOY x132 @ 14.66; BUY BORR x423 @ 4.59; BUY XHG x464 @ 4.19; BUY MP x33 @ 58.01 |
| 2026-08-18 | -6.20 | $29.01 | ABX×213, ALOY×132, BORR×423, XHG×464, MP×33 | — | ABX, ALOY, BORR, XHG, MP | $9,291.12 | $0.00 | $9,291.12 | — | SELL ABX (dropped from list after 1 sess (min 1)); SELL ALOY (dropped from list after 1 sess (min 1)); SELL BORR (dropped from list after 1 sess (min 1)); SELL XHG (dropped from list after 1 sess (min 1)); SELL MP (dropped from list after 1 sess (min 1)) |
| 2026-08-19 | -7.20 | $9,291.12 | — | — | — | $9,291.12 | $0.00 | $9,291.12 | — | hard-red sit S=-7.20 |
| 2026-08-20 | +1.12 | $9,291.12 | — | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $7.92 | $9,411.61 | $9,419.53 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×663, WPM×8, ABUS×236 | BUY AG x56 @ 20.55; BUY CDE x56 @ 20.65; BUY HDSN x201 @ 5.77; BUY IAG x59 @ 19.63; BUY KGC x39 @ 29.63; BUY NFGC x663 @ 1.75; BUY WPM x8 @ 144.54; BUY ABUS x236 @ 4.92 |
| 2026-08-21 | +3.25 | $7.92 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×663, WPM×8, ABUS×236 | AU, AUPH, AEM, ARCT, CYPH, BTBT, INDP, TEM | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $160.79 | $9,786.29 | $9,947.08 | AU×10, AUPH×70, AEM×5, ARCT×109, CYPH×920, BTBT×732, INDP×874, TEM×18 | SELL AG (dropped from list after 1 sess (min 1)); SELL CDE (dropped from list after 1 sess (min 1)); SELL HDSN (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); SELL ABUS (dropped from list after 1 sess (min 1)); BUY AU x10 @ 119.43; BUY AUPH x70 @ 17.20; BUY AEM x5 @ 216.30; BUY ARCT x109 @ 11.13; BUY CYPH x920 @ 1.32; BUY BTBT x732 @ 1.66; BUY INDP x874 @ 1.39; BUY TEM x18 @ 65.60 |
| 2026-08-24 | -5.17 | $160.79 | AU×10, AUPH×70, AEM×5, ARCT×109, CYPH×920, BTBT×732, INDP×874, TEM×18 | — | AU, AUPH, AEM, ARCT, CYPH, BTBT, INDP, TEM | $10,177.76 | $0.00 | $10,177.76 | — | SELL AU (dropped from list after 1 sess (min 1)); SELL AUPH (dropped from list after 1 sess (min 1)); SELL AEM (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL CYPH (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); SELL INDP (dropped from list after 1 sess (min 1)); SELL TEM (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10,177.76 | — | ZURA, CYPH, DEFT, GORO, EZPW, ERO, WPM, FCX | — | $188.59 | $9,902.34 | $10,090.93 | ZURA×199, CYPH×748, DEFT×1987, GORO×360, EZPW×36, ERO×33, WPM×7, FCX×16 | BUY ZURA x199 @ 6.38; BUY CYPH x748 @ 1.70; BUY DEFT x1987 @ 0.64; BUY GORO x360 @ 3.53; BUY EZPW x36 @ 34.48; BUY ERO x33 @ 38.00; BUY WPM x7 @ 160.00; BUY FCX x16 @ 77.90 |
| 2026-08-26 | +2.02 | $188.59 | ZURA×199, CYPH×748, DEFT×1987, GORO×360, EZPW×36, ERO×33, WPM×7, FCX×16 | — | — | $188.59 | $9,945.38 | $10,133.97 | ZURA×199, CYPH×748, DEFT×1987, GORO×360, EZPW×36, ERO×33, WPM×7, FCX×16 | hold ZURA,CYPH,DEFT,GORO,EZPW,ERO,WPM,FCX |
| 2026-08-27 | — | $188.59 | ZURA×199, CYPH×748, DEFT×1987, GORO×360, EZPW×36, ERO×33, WPM×7, FCX×16 | — | ZURA, CYPH, DEFT, GORO, EZPW, ERO, WPM, FCX | $10,128.97 | $0.00 | $10,128.97 | — | SELL ZURA (dropped from list after 2 sess (min 1)); SELL CYPH (dropped from list after 2 sess (min 1)); SELL DEFT (dropped from list after 2 sess (min 1)); SELL GORO (dropped from list after 2 sess (min 1)); SELL EZPW (dropped from list after 2 sess (min 1)); SELL ERO (dropped from list after 2 sess (min 1)); SELL WPM (dropped from list after 2 sess (min 1)); SELL FCX (dropped from list after 2 sess (min 1)) |
| 2026-08-28 | +0.75 | $10,128.97 | — | ANF, BZ, URBN, TIGR | — | $128.71 | $9,622.11 | $9,750.82 | ANF×17, BZ×136, URBN×30, TIGR×461 | BUY ANF x17 @ 144.70; BUY BZ x136 @ 18.50; BUY URBN x30 @ 82.70; BUY TIGR x461 @ 5.49 |
| 2026-08-31 | -5.85 | $128.71 | ANF×17, BZ×136, URBN×30, TIGR×461 | — | ANF, BZ, URBN, TIGR | $9,795.74 | $0.00 | $9,795.74 | — | SELL ANF (dropped from list after 1 sess (min 1)); SELL BZ (dropped from list after 1 sess (min 1)); SELL URBN (dropped from list after 1 sess (min 1)); SELL TIGR (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $9,795.74 | — | — | — | $9,795.74 | $0.00 | $9,795.74 | — | hard-red sit S=-6.30 |
| 2026-09-02 | -3.83 | $9,795.74 | — | — | — | $9,795.74 | $0.00 | $9,795.74 | — | hard-red sit S=-3.83 |
| 2026-09-03 | -0.90 | $9,795.74 | — | RVTY, CRK, MMED, MRNA, ARCT, NVAX, ALMS, OSW | — | $142.59 | $9,732.24 | $9,874.83 | RVTY×9, CRK×77, MMED×53, MRNA×8, ARCT×74, NVAX×119, ALMS×120, OSW×54 | BUY RVTY x9 @ 125.94; BUY CRK x77 @ 15.70; BUY MMED x53 @ 22.78; BUY MRNA x8 @ 151.40; BUY ARCT x74 @ 16.46; BUY NVAX x119 @ 10.27; BUY ALMS x120 @ 10.15; BUY OSW x54 @ 22.53 |
| 2026-09-04 | — | $142.59 | RVTY×9, CRK×77, MMED×53, MRNA×8, ARCT×74, NVAX×119, ALMS×120, OSW×54 | DELL, OABI, ALEC, TARS, MDB, TRLV | RVTY, CRK, MMED, MRNA, ARCT, NVAX, ALMS, OSW | $363.12 | $9,382.91 | $9,746.03 | DELL×3, OABI×323, ALEC×608, TARS×19, MDB×4, TRLV×138 | SELL RVTY (dropped from list after 1 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); SELL MMED (dropped from list after 1 sess (min 1)); SELL MRNA (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL NVAX (dropped from list after 1 sess (min 1)); SELL ALMS (dropped from list after 1 sess (min 1)); SELL OSW (dropped from list after 1 sess (min 1)); BUY DELL x3 @ 486.31; BUY OABI x323 @ 5.08; BUY ALEC x608 @ 2.70; BUY TARS x19 @ 82.76; BUY MDB x4 @ 378.76; BUY TRLV x138 @ 11.89 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | ▼ $9,989.25 (-10.75) | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | ▼ $9,987.01 (-12.99) | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | ▼ $9,983.27 (-16.73) | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | ▼ $9,979.41 (-20.59) | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | ▼ $9,977.20 (-22.80) | combo gate; gate join=good,vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $2,499.51 | ▼ $9,974.87 (-25.13) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $1,245.37 | ▼ $9,968.89 (-31.11) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 170 | $7.29 | $2.50 | — | $3.57 | ▼ $9,966.39 (-33.61) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $1,258.83 | ▼ $9,748.60 (-251.40) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $2,404.85 | ▼ $9,746.34 (-253.66) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,735.05 | ▼ $9,742.54 (-257.46) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,957.03 | ▼ $9,738.62 (-261.38) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $6,134.54 | ▼ $9,736.38 (-263.62) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $7,204.03 | ▼ $9,734.03 (-265.97) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $8,497.16 | ▼ $9,727.96 (-272.04) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 170 | $7.24 | $2.54 | $-13.54 | $9,725.42 | ▼ $9,725.42 (-274.58) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 213 | $9.12 | $2.75 | — | $7,780.11 | ▼ $9,722.67 (-277.33) | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1945.08 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 132 | $14.66 | $2.39 | — | $5,842.60 | ▼ $9,720.28 (-279.72) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1945.08 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 423 | $4.59 | $5.46 | — | $3,895.58 | ▼ $9,714.83 (-285.17) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $1945.08 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 464 | $4.19 | $5.99 | — | $1,945.43 | ▼ $9,708.84 (-291.16) | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ⚪; ret5=+291.8; leftover $1945.08 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 33 | $58.01 | $2.09 | — | $29.01 | ▼ $9,706.75 (-293.25) | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1945.08 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 213 | $9.03 | $2.80 | $-24.72 | $1,949.60 | ▼ $9,307.27 (-692.73) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 132 | $13.19 | $2.42 | $-198.85 | $3,688.26 | ▼ $9,304.85 (-695.15) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 423 | $4.56 | $5.54 | $-23.69 | $5,611.60 | ▼ $9,299.31 (-700.69) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 464 | $3.94 | $6.08 | $-128.06 | $7,433.68 | ▼ $9,293.23 (-706.77) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 33 | $56.35 | $2.11 | $-58.98 | $9,291.12 | ▼ $9,291.12 (-708.88) | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 56 | $20.55 | $2.16 | — | $8,138.16 | ▼ $9,288.96 (-711.04) | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $6,979.60 | ▼ $9,286.80 (-713.20) | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 201 | $5.77 | $2.60 | — | $5,817.24 | ▼ $9,284.21 (-715.79) | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $4,656.90 | ▼ $9,282.04 (-717.96) | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,499.22 | ▼ $9,279.93 (-720.07) | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 663 | $1.75 | $8.55 | — | $2,330.42 | ▼ $9,271.38 (-728.62) | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,172.08 | ▼ $9,269.36 (-730.64) | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 236 | $4.92 | $3.04 | — | $7.92 | ▼ $9,266.32 (-733.68) | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 56 | $21.90 | $2.18 | $+71.26 | $1,232.14 | ▼ $9,745.04 (-254.96) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 56 | $21.75 | $2.18 | $+57.26 | $2,447.96 | ▼ $9,742.86 (-257.14) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 201 | $5.67 | $2.64 | $-25.34 | $3,584.99 | ▼ $9,740.22 (-259.78) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 59 | $21.17 | $2.19 | $+86.51 | $4,831.84 | ▼ $9,738.04 (-261.96) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 39 | $32.17 | $2.13 | $+94.83 | $6,084.34 | ▼ $9,735.91 (-264.09) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 663 | $1.79 | $8.67 | $+9.29 | $7,262.44 | ▼ $9,727.24 (-272.76) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $8,498.00 | ▼ $9,725.20 (-274.80) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 236 | $5.20 | $3.09 | $+59.94 | $9,722.11 | ▼ $9,722.11 (-277.89) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,525.79 | ▼ $9,720.09 (-279.91) | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 70 | $17.20 | $2.20 | — | $7,319.59 | ▼ $9,717.89 (-282.11) | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,236.08 | ▼ $9,715.88 (-284.12) | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 109 | $11.13 | $2.32 | — | $5,020.60 | ▼ $9,713.57 (-286.43) | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 920 | $1.32 | $11.87 | — | $3,794.33 | ▼ $9,701.70 (-298.30) | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 732 | $1.66 | $9.44 | — | $2,569.77 | ▼ $9,692.26 (-307.74) | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 874 | $1.39 | $11.27 | — | $1,343.63 | ▼ $9,680.98 (-319.02) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TEM` | 18 | $65.60 | $2.04 | — | $160.79 | ▼ $9,678.94 (-321.06) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+22.8; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,363.75 | ▲ $10,219.46 (+219.46) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 70 | $16.60 | $2.22 | $-46.42 | $2,523.53 | ▲ $10,217.24 (+217.24) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,606.65 | ▲ $10,215.21 (+215.21) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 109 | $13.26 | $2.35 | $+227.51 | $5,049.64 | ▲ $10,212.86 (+212.86) | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 920 | $1.83 | $12.03 | $+445.30 | $6,721.21 | ▲ $10,200.83 (+200.83) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 732 | $1.55 | $9.57 | $-99.54 | $7,846.24 | ▲ $10,191.26 (+191.26) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 874 | $1.24 | $11.43 | $-153.80 | $8,918.57 | ▲ $10,179.83 (+179.83) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TEM` | 18 | $70.07 | $2.06 | $+76.35 | $10,177.76 | ▲ $10,177.76 (+177.76) | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 199 | $6.38 | $2.59 | — | $8,905.55 | ▲ $10,175.17 (+175.17) | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 748 | $1.70 | $9.65 | — | $7,624.31 | ▲ $10,165.53 (+165.53) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 1987 | $0.64 | $18.68 | — | $6,333.95 | ▲ $10,146.85 (+146.85) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 360 | $3.53 | $4.64 | — | $5,058.50 | ▲ $10,142.20 (+142.20) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+16.0; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 36 | $34.48 | $2.10 | — | $3,815.13 | ▲ $10,140.11 (+140.11) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 33 | $38.00 | $2.09 | — | $2,559.04 | ▲ $10,138.02 (+138.02) | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 7 | $160.00 | $2.01 | — | $1,437.03 | ▲ $10,136.01 (+136.01) | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.6; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 16 | $77.90 | $2.04 | — | $188.59 | ▲ $10,133.97 (+133.97) | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 199 | $6.13 | $2.63 | $-54.97 | $1,405.83 | ▲ $10,170.01 (+170.01) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 748 | $1.60 | $9.78 | $-94.23 | $2,592.84 | ▲ $10,160.22 (+160.22) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DEFT` | 1987 | $0.60 | $18.22 | $-116.38 | $3,766.82 | ▲ $10,142.00 (+142.00) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GORO` | 360 | $3.77 | $4.71 | $+77.04 | $5,119.31 | ▲ $10,137.29 (+137.29) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `EZPW` | 36 | $35.70 | $2.12 | $+39.70 | $6,402.39 | ▲ $10,135.17 (+135.17) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ERO` | 33 | $40.51 | $2.11 | $+78.63 | $7,737.11 | ▲ $10,133.06 (+133.06) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 7 | $160.93 | $2.03 | $+2.47 | $8,861.59 | ▲ $10,131.03 (+131.03) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FCX` | 16 | $79.34 | $2.06 | $+18.94 | $10,128.97 | ▲ $10,128.97 (+128.97) | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 17 | $144.70 | $2.04 | — | $7,667.03 | ▲ $10,126.93 (+126.93) | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $2532.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 136 | $18.50 | $2.40 | — | $5,148.63 | ▲ $10,124.53 (+124.53) | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $2532.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 30 | $82.70 | $2.08 | — | $2,665.55 | ▲ $10,122.45 (+122.45) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $2532.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TIGR` | 461 | $5.49 | $5.95 | — | $128.71 | ▲ $10,116.50 (+116.50) | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+15.9; leftover $2532.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 17 | $148.67 | $2.07 | $+63.38 | $2,654.03 | ▼ $9,806.33 (-193.67) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 136 | $17.89 | $2.44 | $-87.80 | $5,084.63 | ▼ $9,803.89 (-196.11) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 30 | $81.09 | $2.11 | $-52.49 | $7,515.22 | ▼ $9,801.78 (-198.22) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 461 | $4.96 | $6.04 | $-256.32 | $9,795.74 | ▼ $9,795.74 (-204.26) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $8,660.26 | ▼ $9,793.72 (-206.28) | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1224.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 77 | $15.70 | $2.22 | — | $7,449.14 | ▼ $9,791.50 (-208.50) | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1224.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 53 | $22.78 | $2.15 | — | $6,239.65 | ▼ $9,789.35 (-210.65) | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1224.47 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $151.40 | $2.01 | — | $5,026.44 | ▼ $9,787.34 (-212.66) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1224.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.46 | $2.21 | — | $3,806.19 | ▼ $9,785.13 (-214.87) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1224.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 119 | $10.27 | $2.35 | — | $2,581.71 | ▼ $9,782.78 (-217.22) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1224.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 120 | $10.15 | $2.35 | — | $1,361.36 | ▼ $9,780.43 (-219.57) | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-4.5; leftover $1224.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OSW` | 54 | $22.53 | $2.15 | — | $142.59 | ▼ $9,778.28 (-221.72) | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-0.9; leftover $1224.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $1,332.60 | ▼ $9,868.86 (-131.14) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 77 | $15.45 | $2.24 | $-23.71 | $2,520.01 | ▼ $9,866.62 (-133.38) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 53 | $23.88 | $2.17 | $+53.98 | $3,783.48 | ▼ $9,864.45 (-135.55) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $145.95 | $2.03 | $-47.65 | $4,949.05 | ▼ $9,862.42 (-137.58) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 74 | $16.77 | $2.23 | $+18.49 | $6,187.79 | ▼ $9,860.18 (-139.82) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 119 | $10.41 | $2.38 | $+11.94 | $7,424.20 | ▼ $9,857.80 (-142.20) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 120 | $10.38 | $2.38 | $+22.87 | $8,667.42 | ▼ $9,855.42 (-144.58) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OSW` | 54 | $22.00 | $2.17 | $-32.94 | $9,853.25 | ▼ $9,853.25 (-146.75) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $8,392.32 | ▼ $9,851.25 (-148.75) | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1642.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 323 | $5.08 | $4.17 | — | $6,747.32 | ▼ $9,847.09 (-152.91) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1642.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 608 | $2.70 | $7.84 | — | $5,097.87 | ▼ $9,839.24 (-160.76) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1642.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 19 | $82.76 | $2.05 | — | $3,523.39 | ▼ $9,837.20 (-162.80) | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1642.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 4 | $378.76 | $2.00 | — | $2,006.34 | ▼ $9,835.19 (-164.81) | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-6.4; leftover $1642.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TRLV` | 138 | $11.89 | $2.40 | — | $363.12 | ▼ $9,832.79 (-167.21) | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $1642.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DEFT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GORO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `EZPW` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ERO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `WPM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FCX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `DKS` | no_price | no 09:30 open |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DELL` | 3 | 2026-09-04 @ $486.31 | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1642.21 |
| `OABI` | 323 | 2026-09-04 @ $5.08 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1642.21 |
| `ALEC` | 608 | 2026-09-04 @ $2.70 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1642.21 |
| `TARS` | 19 | 2026-09-04 @ $82.76 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1642.21 |
| `MDB` | 4 | 2026-09-04 @ $378.76 | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-6.4; leftover $1642.21 |
| `TRLV` | 138 | 2026-09-04 @ $11.89 | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $1642.21 |
