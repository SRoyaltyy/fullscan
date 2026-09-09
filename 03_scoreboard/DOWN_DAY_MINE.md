# Down-day winner mine — 2025-07-15 → 2026-08-21

Leak-free 09:30 inputs only. Official factor-mine books and live `flatten_robust` are untouched. Inverse / leveraged products are dropped — they are not a stock that won.

SPY close-to-close down days: **133** / 290 sessions. Liquid common-stock name-days: **1077928**.

## Verdict

On SPY close-to-close down days, a liquid common stock's open→close is green 39.4% of the time (mean -0.48%, n=489521). That is the hurdle. Prior-tape cameras (NR7, last-green, washed, breakout, SMA side) do not clear it — a few raise the win-rate and still lose money. The cleanest tape signal is a bounce the *next* morning: after a red SPY day, names already down ≥3% print a next-session open→close win 53.3% of the time (lift +3.2pp, mean +0.24%). That is winning *after* the red day, not during it. On all 127 red days (sector ETFs, no Finviz): Energy/oil is the only long sleeve that actually makes money — XLE 55.9% / +0.14%, XOP 60.6% / +0.18%, USO 54.3% / +0.40%. XLV Healthcare is 47.2% / -0.10% — more often green than a random stock, still a negative mean. UUP (dollar) 66.9% / +0.08% (risk-off). Inverse SPY ETFs are the control and should win; they are not the answer. On the smaller Finviz-overlap slice, Healthcare *names* win 50.5% vs a tagged base of 42.2% (lift +8.3pp, mean +0.31%, n=1849, t=2.236). That is a date-cluster (exports exist), not the full 127-day ETF result. Knowable at 09:30 the morning *after* a red SPY day: tagged Healthcare names win 60.8% (lift +18.3pp, mean +0.98%, n=1824, t=7.069). Biotech / diagnostics are in the same pocket. That is the strongest name-level edge in this mine. Tagged-sector table leader: Energy 52.6% / +0.03% (n=663). 3 near-miss tape cameras lift the same-day win-rate ≥3pp but still have a negative mean — they win more often and still lose money.

## What counts as a down day

- `spy_cc_down` — SPY close < prior close (the market finished red).
- `prior_spy_red` — yesterday was that red day (knowable at 09:30).
- `s_red` / `s_hard` — morning flatten score already red (thin; not promoted).

A *win* is the name's own 09:30 open → 16:00 close > 0. `beat_spy` is a second target (finished less-red than SPY). Finviz / Excel-card lifts are scored against **tagged** name-days only, so a missing export cannot inflate the base.

## Keepers vs liquid names on SPY-down days

| Feature | Regime | n | Win% | Lift | Mean oc | t | Why |
|---|---|---:|---:|---:|---:|---:|---|
| *(none cleared)* | — | — | — | — | — | — | — |

## Knowable at 09:30 (yesterday already red)

| Feature | Regime | n | Win% | Lift | Mean oc | t | Why |
|---|---|---:|---:|---:|---:|---:|---|
| `ret1_le_m3` | prior_spy_red | 69063 | 53.3% | +3.2pp | +0.24% | 12.252 | prior session ≤ −3% |

## Finviz / Excel-card overlay (tagged base, n≥80)

Two-day `s_red` overlays are not keepers. Camera rows need n≥80. **Strong** = n≥400 and t≥2. The rest are exploratory (small n or weak t).

### Strong

| Feature | Regime | n | Win% | Lift | Mean oc | t | Why |
|---|---|---:|---:|---:|---:|---:|---|
| `fv_biotech` | spy_cc_down | 892 | 54.4% | +12.2pp | +0.40% | 3.338 | Industry contains Biotech |
| `health_last_red` | spy_cc_down | 813 | 53.5% | +11.3pp | +0.34% | 2.963 | Healthcare + prior bar red |
| `fv_health` | spy_cc_down | 1849 | 50.5% | +8.3pp | +0.31% | 2.236 | Healthcare |
| `fv_diag` | prior_spy_red | 554 | 63.9% | +21.4pp | +1.21% | 2.968 | Diagnostics / devices / drugs |
| `fv_biotech` | prior_spy_red | 878 | 63.3% | +20.8pp | +1.21% | 9.853 | Industry contains Biotech |
| `health_last_green` | prior_spy_red | 950 | 61.3% | +18.7pp | +1.11% | 4.513 | Healthcare + prior bar green |
| `fv_health` | prior_spy_red | 1824 | 60.8% | +18.3pp | +0.98% | 7.069 | Healthcare |
| `health_last_red` | prior_spy_red | 857 | 60.8% | +18.3pp | +0.88% | 7.725 | Healthcare + prior bar red |
| `defensive_last_green` | prior_spy_red | 1318 | 56.4% | +13.8pp | +0.73% | 4.013 | Defensive sector + prior bar green |
| `fv_defensive` | prior_spy_red | 2645 | 54.4% | +11.9pp | +0.62% | 6.203 | Healthcare + Utilities + staples |
| `fv_short20` | prior_spy_red | 910 | 53.0% | +10.4pp | +0.61% | 4.733 | short float ≥ 20% |
| `defensive_washed` | spy_oc_down | 434 | 58.3% | +13.9pp | +0.84% | 3.861 | Defensive sector washed ≥3% yesterday |
| `health_last_red` | spy_oc_down | 1319 | 56.8% | +12.4pp | +0.56% | 5.843 | Healthcare + prior bar red |
| `fv_diag` | spy_oc_down | 839 | 56.0% | +11.6pp | +0.68% | 2.432 | Diagnostics / devices / drugs |
| `fv_biotech` | spy_oc_down | 1339 | 55.3% | +11.0pp | +0.61% | 5.89 | Industry contains Biotech |
| `fv_health` | spy_oc_down | 2772 | 54.7% | +10.3pp | +0.54% | 5.422 | Healthcare |
| `health_last_green` | spy_oc_down | 1431 | 53.0% | +8.7pp | +0.54% | 3.151 | Healthcare + prior bar green |
| `fv_staples` | spy_oc_down | 740 | 52.6% | +8.2pp | +0.25% | 2.951 | Consumer staples |
| `fv_defensive` | spy_oc_down | 4003 | 52.0% | +7.6pp | +0.35% | 4.855 | Healthcare + Utilities + staples |

### Exploratory

| Feature | Regime | n | Win% | Lift | Mean oc | t | Why |
|---|---|---:|---:|---:|---:|---:|---|
| `health_washed` | spy_cc_down | 156 | 63.5% | +21.2pp | +1.15% | 2.434 | Healthcare washed ≥3% yesterday |
| `defensive_washed` | spy_cc_down | 195 | 58.5% | +16.2pp | +0.83% | 2.134 | Defensive sector washed ≥3% yesterday |
| `fv_gold` | spy_cc_down | 158 | 57.0% | +14.8pp | +0.33% | 1.73 | Industry contains Gold/Silver |
| `energy_last_green` | spy_cc_down | 411 | 54.5% | +12.3pp | +0.11% | 1.022 | Energy + prior bar green |
| `fv_oil` | spy_cc_down | 622 | 53.7% | +11.5pp | +0.06% | 0.642 | Industry contains Oil / E&P |
| `fv_energy` | spy_cc_down | 663 | 52.6% | +10.4pp | +0.03% | 0.342 | Energy |
| `health_washed` | prior_spy_red | 261 | 61.3% | +18.8pp | +1.29% | 4.739 | Healthcare washed ≥3% yesterday |
| `defensive_washed` | prior_spy_red | 342 | 57.3% | +14.8pp | +0.97% | 4.372 | Defensive sector washed ≥3% yesterday |
| `fv_gold` | prior_spy_red | 154 | 50.0% | +7.5pp | +0.11% | 0.485 | Industry contains Gold/Silver |
| `fv_rsi70` | prior_spy_red | 765 | 48.1% | +5.6pp | +0.08% | 0.382 | Finviz RSI(14) ≥ 70 (prior export) |
| `fv_week_m5` | prior_spy_red | 1633 | 47.6% | +5.1pp | +0.08% | 0.964 | Finviz week performance ≤ −5% (prior export) |
| `fv_rsi30` | prior_spy_red | 244 | 45.9% | +3.4pp | +0.80% | 0.904 | Finviz RSI(14) ≤ 30 (prior export) |
| `health_washed` | spy_oc_down | 345 | 61.2% | +16.8pp | +1.04% | 3.974 | Healthcare washed ≥3% yesterday |
| `fv_gold` | spy_oc_down | 233 | 57.9% | +13.6pp | +0.57% | 3.076 | Industry contains Gold/Silver |
| `fv_materials` | spy_oc_down | 1050 | 51.8% | +7.4pp | +0.16% | 1.769 | Basic Materials |
| `fv_short20` | spy_oc_down | 1384 | 48.4% | +4.0pp | +0.20% | 1.876 | short float ≥ 20% |
| `fv_rsi30` | spy_oc_down | 343 | 47.5% | +3.1pp | +0.55% | 0.846 | Finviz RSI(14) ≤ 30 (prior export) |

## Near-misses — higher win%, still red money

| Feature | Regime | n | Win% | Lift | Mean oc | t | Why |
|---|---|---:|---:|---:|---:|---:|---|
| `fv_materials` | spy_cc_down | 704 | 49.4% | +7.2pp | -0.06% | -0.59 | Basic Materials |
| `fv_staples` | spy_cc_down | 490 | 45.3% | +3.1pp | -0.09% | -0.988 | Consumer staples |
| `ret5_ge_8` | spy_cc_down | 46685 | 42.4% | +3.1pp | -0.68% | -23.485 | prior 5-session ≥ +8% |

## Sector ETFs on every SPY-down day

Independent of Finviz. Inverse SPY products are the control.

| Ticker | Sleeve | Days | Win% | Mean oc | t |
|---|---|---:|---:|---:|---:|
| `SH` | Short S&P (control — should win) | 127 | 81.9% | +0.42% | 8.042 |
| `SPXU` | 3x short S&P (control) | 127 | 81.1% | +1.25% | 8.057 |
| `PSQ` | Short QQQ (control) | 127 | 80.3% | +0.57% | 7.576 |
| `UUP` | US dollar | 127 | 66.9% | +0.08% | 3.848 |
| `XOP` | Oil E&P | 127 | 60.6% | +0.18% | 1.353 |
| `VDE` | Energy (Vanguard) | 126 | 56.4% | +0.13% | 1.305 |
| `XLE` | Energy | 127 | 55.9% | +0.14% | 1.346 |
| `USO` | Crude oil | 127 | 54.3% | +0.40% | 2.772 |
| `TLT` | Long Treasuries | 127 | 52.8% | -0.03% | -0.693 |
| `UNG` | Natural gas | 127 | 51.2% | +0.08% | 0.541 |
| `XLP` | Consumer staples | 127 | 51.2% | +0.05% | 0.697 |
| `VHT` | Healthcare (Vanguard) | 126 | 47.6% | -0.11% | -1.516 |
| `XLV` | Healthcare | 127 | 47.2% | -0.10% | -1.38 |
| `XLU` | Utilities | 127 | 44.1% | -0.13% | -1.609 |
| `XLRE` | Real estate | 126 | 43.6% | -0.14% | -1.938 |
| `IYR` | Real estate (iShares) | 126 | 43.6% | -0.16% | -2.218 |
| `XBI` | Biotech (equal-weight) | 127 | 42.5% | -0.17% | -1.336 |
| `GLD` | Gold | 127 | 41.7% | -0.24% | -2.574 |
| `XLB` | Materials | 127 | 41.7% | -0.27% | -3.336 |
| `SLV` | Silver | 127 | 40.9% | -0.44% | -1.983 |
| `GDX` | Gold miners | 127 | 40.9% | -0.45% | -2.372 |
| `KRE` | Regional banks | 127 | 40.2% | -0.29% | -2.473 |
| `IBB` | Biotech | 126 | 39.7% | -0.21% | -2.09 |
| `XLF` | Financials | 127 | 32.3% | -0.31% | -4.369 |
| `XLC` | Communication | 127 | 31.5% | -0.34% | -5.313 |
| `SMH` | Semis | 127 | 29.9% | -0.82% | -5.449 |
| `XLI` | Industrials | 126 | 28.6% | -0.41% | -5.456 |
| `XLY` | Consumer discretionary | 126 | 25.4% | -0.48% | -6.482 |
| `XLK` | Technology | 127 | 24.4% | -0.68% | -6.61 |

## Tagged sectors on SPY-down days

| Sector | n | Win% | Lift vs tagged | Mean oc | t |
|---|---:|---:|---:|---:|---:|
| Energy | 663 | 52.6% | +10.4pp | +0.03% | 0.342 |
| Healthcare | 1849 | 50.5% | +8.3pp | +0.31% | 2.236 |
| Basic Materials | 704 | 49.4% | +7.2pp | -0.06% | -0.59 |
| Consumer Defensive | 490 | 45.3% | +3.1pp | -0.09% | -0.988 |
| Utilities | 326 | 44.2% | +2.0pp | -0.36% | -3.328 |
| Consumer Cyclical | 1204 | 40.8% | -1.4pp | -0.41% | -6.545 |
| Communication Services | 515 | 40.6% | -1.6pp | -0.31% | -2.363 |
| Industrials | 1654 | 38.3% | -3.9pp | -0.66% | -7.346 |
| Real Estate | 600 | 38.3% | -3.9pp | -0.27% | -4.698 |
| Technology | 1832 | 37.8% | -4.4pp | -0.63% | -6.003 |
| Financial | 1961 | 37.1% | -5.1pp | -0.26% | -3.558 |

## Excel emulator

Start-of-day colors: A B C G J K L M O. Close-knowable colors D E F H I N use day-t OHLC — they are not 09:30 inputs. core_score = A..J includes close-knowable cells, so a same-day core_score gate would leak. Prior-day close colors are fine.

- L1/L2: last green + month-vol < 3% → xl_l1_lowvol_green
- L3: last green + midcap $1–10B → xl_l3_mid_green
- L5: last green + midcap + beta > 1.5 → xl_l5_mid_hibeta
- S1/S2: optionable/hi-vol last-red — the inverse of a long-down-day bet

### Card-cohort results on SPY-down tagged days

| Feature | Regime | n | Win% | Lift | Mean oc | t | Why |
|---|---|---:|---:|---:|---:|---:|---|
| `xl_s2_hivol_red` | spy_cc_down | 415 | 44.1% | +1.9pp | +0.04% | 0.073 | Excel S2 short-red cohort (hi-vol + last red) |
| `xl_l3_mid_green` | spy_cc_down | 3006 | 43.1% | +0.9pp | -0.28% | -6.198 | Excel L3: last green + midcap $1–10B |
| `xl_l1_lowvol_green` | spy_cc_down | 1797 | 43.0% | +0.8pp | -0.15% | -5.307 | Excel L1/L2: last green + month vol < 3% |
| `xl_l5_mid_hibeta` | spy_cc_down | 564 | 42.7% | +0.5pp | -0.49% | -3.405 | Excel L5: last green + midcap + beta > 1.5 |
| `xl_s1_opt_red` | spy_cc_down | 5521 | 42.2% | -0.0pp | -0.29% | -9.455 | Excel S1 short-red cohort (optionable + last red) |

excel-state ships Yahoo row caches, not the 3,603 color grids. Cohorts are tested via the prior Finviz export; colors are not re-mined here (would remake the official excel cards).

