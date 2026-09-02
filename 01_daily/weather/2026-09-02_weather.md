# Weather report — 2026-09-02

Is today good for each *kind* of stock? Labels come from `data/universe/`; this file is the daily regime record the backtest will grade.

## Snapshot

- **Risk state:** OFF (general predict down score -3.8, conf 0.52)
- **Yields:** flat (fred_dgs10) | **Dollar:** flat (dxy) | **Oil:** flat | **VIX:** falling (ratio 0.86 via vix/vix3m) spot 15.38
- **Fear & Greed:** 58 (Greed) | **Yield/SPX 5d corr:** -0.79
- **High-impact events:** 5 bullish vs 5 bearish
- ⚠️ **Data gaps:** risk tilted off by news_judge

## Sectors

| Label | Weather | Conf | Why |
|---|---|---|---|
| sector:Basic Materials | ⛅ neutral | high | sector predict score +0.0 dir flat conf 0.52 [sector board] |
| sector:Communication Services | ⛅ neutral | high | sector predict score +0.0 dir flat conf 0.5 [sector board] |
| sector:Consumer Cyclical | 🌧️ hostile | high | sector predict score -5.0 dir down conf 0.52 [sector board] |
| sector:Consumer Defensive | ⛅ neutral | high | sector predict score +2.7 dir up conf 0.52 [sector board] |
| sector:Energy | ⛅ neutral | high | sector predict score +2.7 dir up conf 0.52 [sector board] |
| sector:Financial | ⛅ neutral | high | sector predict score +0.0 dir flat conf 0.5 [sector board] |
| sector:Healthcare | 🌤️ favorable | medium | news_judge SECTOR Healthcare [bullish] |
| sector:Industrials | ⛅ neutral | high | sector predict score +0.0 dir flat conf 0.52 [sector board] |
| sector:Real Estate | 🌧️ hostile | high | sector predict score -5.8 dir down conf 0.52 [sector board] |
| sector:Technology | 🌧️ hostile | medium | finviz sector median week -2.83% [tape] |
| sector:Utilities | ⛅ neutral | medium | finviz sector median week -0.92% [tape] |

## Size

| Label | Weather | Conf | Why |
|---|---|---|---|
| size:micro | ⛅ neutral | low | risk-mixed, dollar flat [general predict + factors] |
| size:small | ⛅ neutral | low | risk-mixed, dollar flat [general predict + factors] |
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
| profit:no | ⛅ neutral | low | risk-mixed, F&G Greed |
| profit:yes | ⛅ neutral | low | — |
| profit:thin | ⛅ neutral | low | — |

## Style (growth/value)

| Label | Weather | Conf | Why |
|---|---|---|---|
| style:growth | ⛅ neutral | low | yields flat/unknown |
| style:value | ⛅ neutral | low | yields flat/unknown |
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
- **elevated_short_caution** — True
- **earnings_proximity** — True
- **veto_earn_today** — True
- **veto_extreme_risk_off** — True

