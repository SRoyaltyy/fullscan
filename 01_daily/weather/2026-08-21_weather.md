# Weather report — 2026-08-21

Is today good for each *kind* of stock? Labels come from `data/universe/`; this file is the daily regime record the backtest will grade.

## Snapshot

- **Risk state:** OFF (general predict up score +3.2, conf 0.55)
- **Yields:** falling (llm_factor_fallback) | **Dollar:** flat (dxy) | **Oil:** flat | **VIX:** falling (ratio 0.82 via vix/vix3m) spot 15.26
- **Fear & Greed:** n/a | **Yield/SPX 5d corr:** -0.17
- **High-impact events:** 0 bullish vs 0 bearish
- ⚠️ **Data gaps:** risk tilted off by news_judge

## Sectors

| Label | Weather | Conf | Why |
|---|---|---|---|
| sector:Basic Materials | 🌤️ favorable | high | sector predict score +13.2 dir up conf 0.65 [sector board] |
| sector:Communication Services | ⛅ neutral | high | sector predict score -2.0 dir down conf 0.5 [sector board] |
| sector:Consumer Cyclical | 🌧️ hostile | high | sector predict score -10.0 dir down conf 0.55 [sector board] |
| sector:Consumer Defensive | ⛅ neutral | high | sector predict score -2.7 dir down conf 0.55 [sector board] |
| sector:Energy | 🌤️ favorable | medium | news_judge SECTOR Energy [bullish] |
| sector:Financial | 🌤️ favorable | high | sector predict score +11.0 dir up conf 0.6 [sector board] |
| sector:Healthcare | 🌤️ favorable | high | sector predict score +6.3 dir up conf 0.6 [sector board] |
| sector:Industrials | 🌧️ hostile | high | sector predict score -3.1 dir down conf 0.55 [sector board] |
| sector:Real Estate | 🌤️ favorable | high | sector predict score +7.5 dir up conf 0.6 [sector board] |
| sector:Technology | 🌧️ hostile | medium | news_judge SECTOR Technology [bearish] |
| sector:Utilities | ⛅ neutral | high | sector predict score +2.7 dir up conf 0.55 [sector board] |

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
- **elevated_short_caution** — True
- **earnings_proximity** — True
- **veto_earn_today** — True
- **veto_extreme_risk_off** — True

