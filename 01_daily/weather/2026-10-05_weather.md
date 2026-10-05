# Weather report — 2026-10-05

Is today good for each *kind* of stock? Labels come from `data/universe/`; this file is the daily regime record the backtest will grade.

## Snapshot

- **Risk state:** OFF (general predict flat score +0.1, conf 0.403)
- **Yields:** falling (fred_dgs10) | **Dollar:** strong (dxy) | **Oil:** falling | **VIX:** falling (ratio 0.86 via vix/vix3m) spot 15.58
- **Fear & Greed:** n/a | **Yield/SPX 5d corr:** +0.32
- **High-impact events:** 7 bullish vs 4 bearish | China: bear
- ⚠️ **Data gaps:** risk tilted off by news_judge

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
| size:micro | 🌧️ hostile | medium | strong dollar — small caps de-rate first; risk-mixed, dollar strong [general predict + factors] |
| size:small | 🌧️ hostile | medium | strong dollar — small caps de-rate first; risk-mixed, dollar strong [general predict + factors] |
| size:large | ⛅ neutral | low | risk-mixed [general predict] |
| size:mega | ⛅ neutral | low | risk-mixed [general predict] |
| size:mid | ⛅ neutral | low | no dedicated mid-cap signal in v1 |

## Beta & volatility

| Label | Weather | Conf | Why |
|---|---|---|---|
| beta:high | ⛅ neutral | low | risk-mixed, VIX falling |
| beta:low | ⛅ neutral | low | risk-mixed, VIX falling |
| beta:mid | ⛅ neutral | low | beta-neutral zone |

## Short interest (multiplier, not direction)

| Label | Weather | Conf | Why |
|---|---|---|---|
| short:high | ⛅ neutral | low | mixed tape — squeeze/stress unresolved |
| short:extreme | ⛅ neutral | low | mixed tape — squeeze/stress unresolved |
| short:low | ⛅ neutral | low | low short is not a tailwind by itself |
| short:mid | ⛅ neutral | low | no strong crowding signal |

## Profitability & style

| Label | Weather | Conf | Why |
|---|---|---|---|
| profit:no | ⛅ neutral | low | risk-mixed, F&G None |
| profit:yes | ⛅ neutral | low | — |
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
| lev:high | ⛅ neutral | low | — |
| lev:low | ⛅ neutral | low | — |
| lev:mid | ⛅ neutral | low | — |
| lev:neg_equity | 🌧️ hostile | low | negative equity is distressed in any regime |

## Momentum state

| Label | Weather | Conf | Why |
|---|---|---|---|
| mom:uptrend | ⛅ neutral | low | mixed tape |
| mom:downtrend | ⛅ neutral | low | — |
| mom:mixed | ⛅ neutral | low | — |

## Extension state

| Label | Weather | Conf | Why |
|---|---|---|---|
| ext:washed | ⛅ neutral | low | mixed tape |
| ext:extended | ⛅ neutral | low | extension tolerated while tape is firm |
| ext:extreme | ⛅ neutral | low | extension tolerated while tape is firm |
| ext:neutral | ⛅ neutral | low | — |

## 52-week zone

| Label | Weather | Conf | Why |
|---|---|---|---|
| range:deep_low | ⛅ neutral | low | mixed tape |
| range:top | ⛅ neutral | low | — |
| range:breakout | ⛅ neutral | low | — |
| range:low | ⛅ neutral | low | — |
| range:mid | ⛅ neutral | low | — |
| range:high | ⛅ neutral | low | — |

## Geography

| Label | Weather | Conf | Why |
|---|---|---|---|
| geo:US | ⛅ neutral | low | mirrors general risk-mixed [general predict] |
| geo:ADR-China | 🌧️ hostile | medium | high-impact China event(s) lean bear [event scanner] |

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

