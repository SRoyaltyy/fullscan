# Weather report — 2026-10-22

Is today good for each *kind* of stock? Labels come from `data/universe/`; this file is the daily regime record the backtest will grade.

## Snapshot

- **Risk state:** OFF (general predict down score -14.3, conf 0.85)
- **Yields:** falling (fred_dgs10) | **Dollar:** flat (dxy) | **Oil:** rising | **VIX:** spiking (ratio 0.95 via vix/vix3m) spot 19.26
- **Fear & Greed:** n/a | **Yield/SPX 5d corr:** +0.71
- **High-impact events:** 10 bullish vs 7 bearish | China: bull

## Sectors

| Label | Weather | Conf | Why |
|---|---|---|---|
| sector:Basic Materials | 🌧️ hostile | medium | finviz sector median week -4.97% [tape] |
| sector:Communication Services | 🌧️ hostile | medium | finviz sector median week -2.75% [tape] |
| sector:Consumer Cyclical | ⛅ neutral | medium | finviz sector median week -1.38% [tape] |
| sector:Consumer Defensive | 🌧️ hostile | medium | finviz sector median week -1.62% [tape] |
| sector:Energy | 🌧️ hostile | medium | finviz sector median week -2.50% [tape] |
| sector:Financial | ⛅ neutral | medium | finviz sector median week -1.40% [tape] |
| sector:Healthcare | 🌧️ hostile | medium | finviz sector median week -2.99% [tape] |
| sector:Industrials | 🌧️ hostile | medium | finviz sector median week -1.83% [tape] |
| sector:Real Estate | 🌧️ hostile | medium | finviz sector median week -2.49% [tape] |
| sector:Technology | 🌧️ hostile | medium | finviz sector median week -2.62% [tape] |
| sector:Utilities | 🌧️ hostile | medium | finviz sector median week -3.43% [tape] |

## Size

| Label | Weather | Conf | Why |
|---|---|---|---|
| size:micro | 🌧️ hostile | medium | risk-off — small caps de-rate first; risk-off, dollar flat [general predict + factors] |
| size:small | 🌧️ hostile | medium | risk-off — small caps de-rate first; risk-off, dollar flat [general predict + factors] |
| size:large | 🌤️ favorable | medium | risk-off — defensive/quality bid concentrates in large & mega [general predict] |
| size:mega | 🌤️ favorable | medium | risk-off — defensive/quality bid concentrates in large & mega [general predict] |
| size:mid | ⛅ neutral | low | no dedicated mid-cap signal in v1 |

## Beta & volatility

| Label | Weather | Conf | Why |
|---|---|---|---|
| beta:high | 🌧️ hostile | high | VIX spiking — high beta is the exit door [channel1 + general] |
| beta:low | 🌤️ favorable | medium | defensive ballast bid in stress [general + channel1] |
| beta:mid | ⛅ neutral | low | beta-neutral zone |

## Short interest (multiplier, not direction)

| Label | Weather | Conf | Why |
|---|---|---|---|
| short:high | 🌧️ hostile | medium | risk-off — heavy short interest marks balance-sheet/dilution stress; it amplifies falls [general] |
| short:extreme | 🌧️ hostile | medium | risk-off — heavy short interest marks balance-sheet/dilution stress; it amplifies falls [general] |
| short:low | ⛅ neutral | low | low short is not a tailwind by itself |
| short:mid | ⛅ neutral | low | no strong crowding signal |

## Profitability & style

| Label | Weather | Conf | Why |
|---|---|---|---|
| profit:no | 🌧️ hostile | high | risk-off — unprofitable names are sold first [general] |
| profit:yes | 🌤️ favorable | medium | risk-off — quality/profitability bid [general] |
| profit:thin | ⛅ neutral | low | — |

## Style (growth/value)

| Label | Weather | Conf | Why |
|---|---|---|---|
| style:growth | 🌤️ favorable | medium | yields falling — duration/growth re-rates [factor: Bond yields] |
| style:value | ⛅ neutral | low | value lags duration rallies |
| style:blend | ⛅ neutral | low | — |

## Leverage

| Label | Weather | Conf | Why |
|---|---|---|---|
| lev:high | 🌧️ hostile | medium | risk-off — leverage amplifies the downside [factors + general] |
| lev:low | 🌤️ favorable | low | balance-sheet strength preferred in stress |
| lev:mid | ⛅ neutral | low | — |
| lev:neg_equity | 🌧️ hostile | low | negative equity is distressed in any regime |

## Momentum state

| Label | Weather | Conf | Why |
|---|---|---|---|
| mom:uptrend | ⛅ neutral | low | uptrends under test in risk-off |
| mom:downtrend | 🌧️ hostile | low | downtrends get no bid in risk-off |
| mom:mixed | ⛅ neutral | low | — |

## Extension state

| Label | Weather | Conf | Why |
|---|---|---|---|
| ext:washed | 🌧️ hostile | medium | falling knives stay sharp in risk-off |
| ext:extended | 🌧️ hostile | medium | parabolic + risk-off = nasty unwind risk |
| ext:extreme | 🌧️ hostile | medium | parabolic + risk-off = nasty unwind risk |
| ext:neutral | ⛅ neutral | low | — |

## 52-week zone

| Label | Weather | Conf | Why |
|---|---|---|---|
| range:deep_low | 🌧️ hostile | medium | falling knife zone in risk-off |
| range:top | 🌧️ hostile | low | high-zone names unwind in risk-off |
| range:breakout | 🌧️ hostile | low | high-zone names unwind in risk-off |
| range:low | ⛅ neutral | low | — |
| range:mid | ⛅ neutral | low | — |
| range:high | ⛅ neutral | low | — |

## Geography

| Label | Weather | Conf | Why |
|---|---|---|---|
| geo:US | 🌧️ hostile | low | mirrors general risk-off [general predict] |
| geo:ADR-China | 🌤️ favorable | medium | high-impact China event(s) lean bull [event scanner] |

## Gates (always-on cautions)

- **earn:today** — reports today — event risk, not a segment bet; size down or skip
- **earn:this_week** — reports within a week — flag, expect gap moves
- **liq:low** — thin dollar volume — gaps on news, hard to exit; down-rank
- **rvol:hot** — abnormal participation — moves are 'real' but confirm direction first
- **ext:extreme + risk-off** — parabolic names into a hostile tape = veto longs
- **elevated_short_caution** — True
- **earnings_proximity** — True
- **veto_earn_today** — True
- **veto_extreme_risk_off** — True

