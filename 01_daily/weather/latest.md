# Weather report — 2026-09-25

Is today good for each *kind* of stock? Labels come from `data/universe/`; this file is the daily regime record the backtest will grade.

## Snapshot

- **Risk state:** OFF (general predict up score +2.7, conf 0.608)
- **Yields:** rising (fred_dgs10) | **Dollar:** soft (dxy) | **Oil:** falling | **VIX:** falling (ratio 0.83 via vix/vix3m) spot 15.34
- **Fear & Greed:** n/a | **Yield/SPX 5d corr:** -0.96
- **High-impact events:** 1 bullish vs 0 bearish
- ⚠️ **Data gaps:** risk tilted off by news_judge

## Sectors

| Label | Weather | Conf | Why |
|---|---|---|---|
| sector:Basic Materials | ⛅ neutral | medium | finviz sector median week -0.60% [tape] |
| sector:Communication Services | 🌧️ hostile | medium | finviz sector median week -1.78% [tape] |
| sector:Consumer Cyclical | ⛅ neutral | medium | finviz sector median week -0.74% [tape] |
| sector:Consumer Defensive | ⛅ neutral | medium | finviz sector median week -1.26% [tape] |
| sector:Energy | 🌧️ hostile | medium | finviz sector median week -2.97% [tape] |
| sector:Financial | ⛅ neutral | medium | finviz sector median week +0.00% [tape] |
| sector:Healthcare | ⛅ neutral | medium | finviz sector median week -1.30% [tape] |
| sector:Industrials | ⛅ neutral | medium | finviz sector median week -0.50% [tape] |
| sector:Real Estate | 🌧️ hostile | medium | finviz sector median week -1.78% [tape] |
| sector:Technology | 🌤️ favorable | medium | news_judge SECTOR Technology [bullish] |
| sector:Utilities | 🌧️ hostile | medium | finviz sector median week -3.09% [tape] |

## Size

| Label | Weather | Conf | Why |
|---|---|---|---|
| size:micro | ⛅ neutral | low | risk-mixed, dollar soft [general predict + factors] |
| size:small | ⛅ neutral | low | risk-mixed, dollar soft [general predict + factors] |
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
| style:growth | 🌧️ hostile | medium | yields rising — long-duration growth de-rates [factor: Bond yields] |
| style:value | 🌤️ favorable | medium | rising yields/reflation favors value & cyclicals [factor: Bond yields] |
| style:blend | ⛅ neutral | low | — |

## Leverage

| Label | Weather | Conf | Why |
|---|---|---|---|
| lev:high | 🌧️ hostile | medium | yields rising — leverage amplifies the downside [factors + general] |
| lev:low | 🌤️ favorable | low | balance-sheet strength preferred in stress |
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
- **elevated_short_caution** — True
- **earnings_proximity** — True
- **veto_earn_today** — True
- **veto_extreme_risk_off** — True

