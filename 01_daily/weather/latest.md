# Weather report — 2026-09-23

Is today good for each *kind* of stock? Labels come from `data/universe/`; this file is the daily regime record the backtest will grade.

## Snapshot

- **Risk state:** MIXED (general predict up score +2.3, conf 0.592)
- **Yields:** falling (fred_dgs10) | **Dollar:** strong (dxy) | **Oil:** falling | **VIX:** falling (ratio 0.78 via vix/vix3m) spot 14.13
- **Fear & Greed:** n/a | **Yield/SPX 5d corr:** -0.79
- **High-impact events:** 1 bullish vs 3 bearish

## Sectors

| Label | Weather | Conf | Why |
|---|---|---|---|
| sector:Basic Materials | 🌧️ hostile | medium | news_judge SECTOR Basic Materials [bearish] |
| sector:Communication Services | 🌧️ hostile | medium | finviz sector median week -1.50% [tape] |
| sector:Consumer Cyclical | 🌧️ hostile | medium | finviz sector median week -1.55% [tape] |
| sector:Consumer Defensive | 🌧️ hostile | medium | finviz sector median week -1.64% [tape] |
| sector:Energy | 🌧️ hostile | medium | news_judge SECTOR Energy [bearish] |
| sector:Financial | ⛅ neutral | medium | finviz sector median week +0.27% [tape] |
| sector:Healthcare | ⛅ neutral | medium | finviz sector median week -0.55% [tape] |
| sector:Industrials | ⛅ neutral | medium | finviz sector median week +0.25% [tape] |
| sector:Real Estate | ⛅ neutral | medium | finviz sector median week -1.23% [tape] |
| sector:Technology | 🌤️ favorable | medium | finviz sector median week +1.65% [tape] |
| sector:Utilities | ⛅ neutral | medium | finviz sector median week -0.55% [tape] |

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
| geo:ADR-China | ⛅ neutral | low | no high-impact China event flagged |

## Gates (always-on cautions)

- **earn:today** — reports today — event risk, not a segment bet; size down or skip
- **earn:this_week** — reports within a week — flag, expect gap moves
- **liq:low** — thin dollar volume — gaps on news, hard to exit; down-rank
- **rvol:hot** — abnormal participation — moves are 'real' but confirm direction first
- **ext:extreme + risk-off** — parabolic names into a hostile tape = veto longs
- **elevated_short_caution** — False
- **earnings_proximity** — True
- **veto_earn_today** — True
- **veto_extreme_risk_off** — False

