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

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | WWW, FOSL, AIRS, OMER, MXCT, AVAH, CRMD, LVWR | — | $14,948.61 | $-4,910.89 | $10,037.72 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500 | SHORT WWW x30 @ 20.60; SHORT FOSL x110 @ 5.64; SHORT AIRS x185 @ 3.37; SHORT OMER x36 @ 17.35; SHORT MXCT x449 @ 1.39; SHORT AVAH x52 @ 11.91; SHORT CRMD x77 @ 8.05; SHORT LVWR x500 @ 1.25 |
| 2026-08-17 | +2.25 | $14,948.61 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500 | HNST, FCEL, BW, INO, BYND, AEHR, LUNR, IOVA | — | $19,844.20 | $-9,785.91 | $10,058.29 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500, HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 | SHORT HNST x130 @ 4.81; SHORT FCEL x28 @ 22.37; SHORT BW x60 @ 10.35; SHORT INO x588 @ 1.07; SHORT BYND x49 @ 12.83; SHORT AEHR x4 @ 132.79; SHORT LUNR x31 @ 20.25; SHORT IOVA x92 @ 6.84 |
| 2026-08-18 | -6.20 | $19,844.20 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500, HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 | — | — | $19,844.20 | $-9,642.01 | $10,202.19 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500, HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $19,844.20 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500, HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 | — | WWW, FOSL, AIRS, OMER, MXCT, AVAH, CRMD, LVWR | $15,013.56 | $-4,962.96 | $10,050.60 | HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 | SELL WWW (dropped from list after 3 sess (min 3)); SELL FOSL (dropped from list after 3 sess (min 3)); SELL AIRS (dropped from list after 3 sess (min 3)); SELL OMER (dropped from list after 3 sess (min 3)); SELL MXCT (dropped from list after 3 sess (min 3)); SELL AVAH (dropped from list after 3 sess (min 3)); SELL CRMD (dropped from list after 3 sess (min 3)); SELL LVWR (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $15,013.56 | HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 | — | HNST, FCEL, BW, INO, BYND, AEHR, LUNR, IOVA | $10,075.28 | $0.00 | $10,075.28 | — | SELL HNST (dropped from list after 3 sess (min 3)); SELL FCEL (dropped from list after 3 sess (min 3)); SELL BW (dropped from list after 3 sess (min 3)); SELL INO (dropped from list after 3 sess (min 3)); SELL BYND (dropped from list after 3 sess (min 3)); SELL AEHR (dropped from list after 3 sess (min 3)); SELL LUNR (dropped from list after 3 sess (min 3)); SELL IOVA (dropped from list after 3 sess (min 3)) |
| 2026-08-21 | +3.25 | $10,075.28 | — | YSS, SMJF, NOG, CPRT, FLO | — | $15,069.04 | $-5,017.12 | $10,051.92 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | SHORT YSS x108 @ 9.26; SHORT SMJF x88 @ 11.35; SHORT NOG x37 @ 27.00; SHORT CPRT x29 @ 34.48; SHORT FLO x146 @ 6.90 |
| 2026-08-24 | -5.17 | $15,069.04 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | — | — | $15,069.04 | $-4,998.40 | $10,070.64 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $15,069.04 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | — | — | $15,069.04 | $-5,062.82 | $10,006.22 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | hold YSS,SMJF,NOG,CPRT,FLO |
| 2026-08-26 | +2.02 | $15,069.04 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | — | — | $15,069.04 | $-5,045.27 | $10,023.77 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | hold YSS,SMJF,NOG,CPRT,FLO |
| 2026-08-27 | — | $15,069.04 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | — | YSS, SMJF, NOG, CPRT, FLO | $10,123.08 | $0.00 | $10,123.08 | — | SELL YSS (dropped from list after 4 sess (min 3)); SELL SMJF (dropped from list after 4 sess (min 3)); SELL NOG (dropped from list after 4 sess (min 3)); SELL CPRT (dropped from list after 4 sess (min 3)); SELL FLO (dropped from list after 4 sess (min 3)) |
| 2026-08-28 | +0.75 | $10,123.08 | — | PYXS, SAFX, XPOF, APMD, OPTU, ABTC, XHG, DEFT | — | $15,122.41 | $-4,985.47 | $10,136.94 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 | SHORT PYXS x191 @ 3.31; SHORT SAFX x1622 @ 0.39; SHORT XPOF x113 @ 5.59; SHORT APMD x21 @ 29.50; SHORT OPTU x596 @ 1.06; SHORT ABTC x75 @ 8.41; SHORT XHG x155 @ 4.06; SHORT DEFT x1054 @ 0.60 |
| 2026-08-31 | -5.85 | $15,122.41 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 | — | — | $15,122.41 | $-4,836.81 | $10,285.60 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $15,122.41 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 | — | — | $15,122.41 | $-4,702.81 | $10,419.60 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $15,122.41 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 | — | PYXS, SAFX, XPOF, APMD, OPTU, ABTC, XHG | $10,993.22 | $-695.64 | $10,297.58 | DEFT×1054 | SELL PYXS (dropped from list after 3 sess (min 3)); SELL SAFX (dropped from list after 3 sess (min 3)); SELL XPOF (dropped from list after 3 sess (min 3)); SELL APMD (dropped from list after 3 sess (min 3)); SELL OPTU (dropped from list after 3 sess (min 3)); SELL ABTC (dropped from list after 3 sess (min 3)); SELL XHG (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,993.22 | DEFT×1054 | — | DEFT | $10,276.82 | $0.00 | $10,276.82 | — | SELL DEFT (dropped from list after 4 sess (min 3)) |
| 2026-09-04 | — | $10,276.82 | — | — | — | $10,276.82 | $0.00 | $10,276.82 | — | flat cash |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **SHORT** | `WWW` | 30 | $20.60 | $2.12 | — | $10,615.88 | ▼ $9,997.88 (-2.12) | alarm; gate alarm=True; list probable,yday_gainer; ret5=+4.4; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `FOSL` | 110 | $5.64 | $2.37 | — | $11,233.92 | ▼ $9,995.52 (-4.48) | alarm; gate alarm=True; list probable; 🔵; ret5=-4.1; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AIRS` | 185 | $3.37 | $2.60 | — | $11,854.76 | ▼ $9,992.91 (-7.09) | alarm; gate alarm=True; list probable; ret5=-29.1; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 36 | $17.35 | $2.14 | — | $12,477.23 | ▼ $9,990.78 (-9.22) | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 449 | $1.39 | $5.89 | — | $13,095.45 | ▼ $9,984.89 (-15.11) | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AVAH` | 52 | $11.91 | $2.18 | — | $13,712.58 | ▼ $9,982.70 (-17.30) | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+21.3; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `CRMD` | 77 | $8.05 | $2.26 | — | $14,330.17 | ▼ $9,980.44 (-19.56) | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LVWR` | 500 | $1.25 | $6.56 | — | $14,948.61 | ▼ $9,973.88 (-26.12) | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+12.6; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HNST` | 130 | $4.81 | $2.43 | — | $15,571.48 | ▲ $10,066.64 (+66.64) | alarm; gate alarm=True; list flatten; ⚪; ret5=-11.4; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `FCEL` | 28 | $22.37 | $2.11 | — | $16,195.73 | ▲ $10,064.53 (+64.53) | alarm; gate alarm=True; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BW` | 60 | $10.35 | $2.21 | — | $16,814.53 | ▲ $10,062.32 (+62.32) | alarm; gate alarm=True; list probable; ⚪; ret5=+9.8; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `INO` | 588 | $1.07 | $7.71 | — | $17,435.98 | ▲ $10,054.61 (+54.61) | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+62.7; leftover $629.32 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 49 | $12.83 | $2.17 | — | $18,062.47 | ▲ $10,052.44 (+52.44) | alarm; gate alarm=True; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `AEHR` | 4 | $132.79 | $2.04 | — | $18,591.59 | ▲ $10,050.40 (+50.40) | alarm; gate alarm=True; list yday_gainer; ⚪; ret5=+30.1; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `LUNR` | 31 | $20.25 | $2.12 | — | $19,217.22 | ▲ $10,048.28 (+48.28) | alarm; gate alarm=True; list yday_gainer,ohlc_hot; ⚪; ret5=+15.9; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `IOVA` | 92 | $6.84 | $2.31 | — | $19,844.20 | ▲ $10,045.97 (+45.97) | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $629.32 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-19 09:30 ET | **COVER** | `WWW` | 30 | $20.08 | $2.08 | $+11.40 | $19,239.72 | ▲ $10,181.73 (+181.73) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `FOSL` | 110 | $5.54 | $2.32 | $+6.31 | $18,628.00 | ▲ $10,179.41 (+179.41) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AIRS` | 185 | $2.71 | $2.54 | $+116.95 | $18,124.10 | ▲ $10,176.86 (+176.86) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OMER` | 36 | $17.13 | $2.10 | $+3.69 | $17,505.32 | ▲ $10,174.76 (+174.76) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `MXCT` | 449 | $1.29 | $5.79 | $+33.21 | $16,920.32 | ▲ $10,168.97 (+168.97) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AVAH` | 52 | $12.92 | $2.15 | $-56.85 | $16,246.33 | ▲ $10,166.82 (+166.82) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `CRMD` | 77 | $8.30 | $2.22 | $-23.73 | $15,605.01 | ▲ $10,164.60 (+164.60) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `LVWR` | 500 | $1.17 | $6.45 | $+26.99 | $15,013.56 | ▲ $10,158.15 (+158.15) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HNST` | 130 | $4.98 | $2.38 | $-26.91 | $14,363.78 | ▲ $10,095.59 (+95.59) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `FCEL` | 28 | $20.21 | $2.07 | $+56.29 | $13,795.83 | ▲ $10,093.52 (+93.52) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `BW` | 60 | $9.05 | $2.17 | $+73.62 | $13,250.66 | ▲ $10,091.35 (+91.35) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `INO` | 588 | $1.30 | $7.59 | $-150.54 | $12,478.67 | ▲ $10,083.76 (+83.76) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `BYND` | 49 | $13.60 | $2.14 | $-42.04 | $11,810.14 | ▲ $10,081.63 (+81.63) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `AEHR` | 4 | $106.01 | $2.00 | $+103.08 | $11,384.10 | ▲ $10,079.63 (+79.63) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 31 | $18.13 | $2.08 | $+61.52 | $10,819.98 | ▲ $10,077.54 (+77.54) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `IOVA` | 92 | $8.07 | $2.27 | $-117.73 | $10,075.28 | ▲ $10,075.28 (+75.28) | dropped from list after 3 sess (min 3) | — |
| 2026-08-21 09:30 ET | **SHORT** | `YSS` | 108 | $9.26 | $2.37 | — | $11,072.99 | ▲ $10,072.91 (+72.91) | alarm; gate alarm=True; list yday_mover; ret5=-20.1; leftover $1007.53 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SMJF` | 88 | $11.35 | $2.31 | — | $12,069.48 | ▲ $10,070.60 (+70.60) | alarm; gate alarm=True; list ohlc_hot; ret5=+13.4; leftover $1007.53 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 37 | $27.00 | $2.15 | — | $13,066.33 | ▲ $10,068.45 (+68.45) | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $1007.53 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CPRT` | 29 | $34.48 | $2.12 | — | $14,064.13 | ▲ $10,066.33 (+66.33) | alarm; gate alarm=True; list ohlc_hot; ret5=+16.8; leftover $1007.53 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `FLO` | 146 | $6.90 | $2.49 | — | $15,069.04 | ▲ $10,063.84 (+63.84) | alarm; gate alarm=True; list earn_react; ret5=-5.7; leftover $1007.53 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **COVER** | `YSS` | 108 | $9.20 | $2.31 | $+1.80 | $14,073.12 | ▲ $10,131.94 (+131.94) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `SMJF` | 88 | $11.15 | $2.25 | $+13.04 | $13,089.67 | ▲ $10,129.69 (+129.69) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `NOG` | 37 | $26.00 | $2.10 | $+32.75 | $12,125.57 | ▲ $10,127.59 (+127.59) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `CPRT` | 29 | $33.00 | $2.08 | $+38.72 | $11,166.49 | ▲ $10,125.51 (+125.51) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `FLO` | 146 | $7.13 | $2.43 | $-38.50 | $10,123.08 | ▲ $10,123.08 (+123.08) | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `PYXS` | 191 | $3.31 | $2.62 | — | $10,752.67 | ▲ $10,120.46 (+120.46) | alarm; gate alarm=True; list yday_gainer; ret5=+2.3; leftover $632.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1622 | $0.39 | $11.49 | — | $11,373.76 | ▲ $10,108.97 (+108.97) | alarm; gate alarm=True; list yday_gainer; ret5=-26.5; leftover $632.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `XPOF` | 113 | $5.59 | $2.38 | — | $12,003.06 | ▲ $10,106.60 (+106.60) | alarm; gate alarm=True; list yday_gainer; ret5=+6.6; leftover $632.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `APMD` | 21 | $29.50 | $2.09 | — | $12,620.47 | ▲ $10,104.51 (+104.51) | alarm; gate alarm=True; list yday_gainer; ret5=-11.7; leftover $632.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `OPTU` | 596 | $1.06 | $7.81 | — | $13,244.41 | ▲ $10,096.69 (+96.69) | alarm; gate alarm=True; list yday_gainer; ret5=-7.8; leftover $632.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `ABTC` | 75 | $8.41 | $2.25 | — | $13,872.91 | ▲ $10,094.44 (+94.44) | alarm; gate alarm=True; list yday_mover; ret5=+9.2; leftover $632.69 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `XHG` | 155 | $4.06 | $2.51 | — | $14,499.70 | ▲ $10,091.93 (+91.93) | alarm; gate alarm=True; list ohlc_hot; ret5=+16.1; leftover $632.69 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `DEFT` | 1054 | $0.60 | $9.69 | — | $15,122.41 | ▲ $10,082.24 (+82.24) | alarm; gate alarm=True; list ohlc_hot; ret5=+17.6; leftover $632.69 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **COVER** | `PYXS` | 191 | $3.24 | $2.56 | $+8.18 | $14,501.01 | ▲ $10,356.78 (+356.78) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SAFX` | 1622 | $0.37 | $10.87 | $+10.08 | $13,890.00 | ▲ $10,345.91 (+345.91) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `XPOF` | 113 | $5.39 | $2.33 | $+17.90 | $13,278.60 | ▲ $10,343.58 (+343.58) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `APMD` | 21 | $26.11 | $2.05 | $+67.05 | $12,728.24 | ▲ $10,341.53 (+341.53) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `OPTU` | 596 | $0.99 | $7.66 | $+26.25 | $12,130.54 | ▲ $10,333.87 (+333.87) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `ABTC` | 75 | $7.91 | $2.21 | $+33.03 | $11,535.08 | ▲ $10,331.66 (+331.66) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `XHG` | 155 | $3.48 | $2.46 | $+84.94 | $10,993.22 | ▲ $10,329.20 (+329.20) | dropped from list after 3 sess (min 3) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **COVER** | `DEFT` | 1054 | $0.67 | $10.22 | $-93.69 | $10,276.82 | ▲ $10,276.82 (+276.82) | dropped from list after 4 sess (min 3) | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |

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
