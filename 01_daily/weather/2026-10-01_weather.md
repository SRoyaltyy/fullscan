# Weather report — 2026-10-01

Is today good for each *kind* of stock? Labels come from `data/universe/`; this file is the daily regime record the backtest will grade.

## Snapshot

- **Risk state:** OFF (general predict down score -1.3, conf 0.554)
- **Yields:** flat (fred_dgs10) | **Dollar:** strong (dxy) | **Oil:** rising | **VIX:** calm (ratio 0.91 via vix/vix3m) spot 16.75
- **Fear & Greed:** n/a | **Yield/SPX 5d corr:** -0.63
- **High-impact events:** 2 bullish vs 2 bearish | China: bull
- ⚠️ **Data gaps:** risk tilted off by news_judge

## Sectors

| Label | Weather | Conf | Why |
|---|---|---|---|
| sector:Basic Materials | 🌧️ hostile | medium | news_judge SECTOR Basic Materials [bearish] |
| sector:Communication Services | 🌤️ favorable | high | sector predict score +7.5 dir up conf 0.799 [sector board] |
| sector:Consumer Cyclical | 🌧️ hostile | high | sector predict score -6.8 dir down conf 0.771 [sector board] |
| sector:Consumer Defensive | ⛅ neutral | high | sector predict score -2.5 dir down conf 0.6 [sector board] |
| sector:Energy | ⛅ neutral | high | sector predict score -2.4 dir down conf 0.497 [sector board] |
| sector:Financial | 🌧️ hostile | high | sector predict score -5.2 dir down conf 0.706 [sector board] |
| sector:Healthcare | 🌤️ favorable | medium | news_judge SECTOR Healthcare [bullish] |
| sector:Industrials | 🌧️ hostile | medium | news_judge SECTOR Industrials [bearish] |
| sector:Real Estate | 🌧️ hostile | high | sector predict score -5.9 dir down conf 0.634 [sector board] |
| sector:Technology | 🌤️ favorable | high | sector predict score +5.0 dir up conf 0.701 [sector board] |
| sector:Utilities | ⛅ neutral | high | sector predict score -0.8 dir down conf 0.432 [sector board] |

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
| beta:high | ⛅ neutral | low | risk-mixed, VIX calm |
| beta:low | ⛅ neutral | low | risk-mixed, VIX calm |
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

