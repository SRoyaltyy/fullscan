# Weather report — 2026-09-21

Is today good for each *kind* of stock? Labels come from `data/universe/`; this file is the daily regime record the backtest will grade.

## Snapshot

- **Risk state:** ON (general predict up score +12.9, conf 0.85)
- **Yields:** falling (fred_dgs10) | **Dollar:** flat (dxy) | **Oil:** falling | **VIX:** falling (ratio 0.82 via vix/vix3m) spot 14.92
- **Fear & Greed:** n/a | **Yield/SPX 5d corr:** -0.59
- **High-impact events:** 2 bullish vs 8 bearish

## Sectors

| Label | Weather | Conf | Why |
|---|---|---|---|
| sector:Basic Materials | 🌧️ hostile | medium | finviz sector median week -1.74% [tape] |
| sector:Communication Services | 🌧️ hostile | medium | finviz sector median week -2.68% [tape] |
| sector:Consumer Cyclical | 🌧️ hostile | medium | finviz sector median week -2.75% [tape] |
| sector:Consumer Defensive | ⛅ neutral | medium | finviz sector median week -1.48% [tape] |
| sector:Energy | 🌧️ hostile | medium | news_judge SECTOR Energy [bearish] |
| sector:Financial | ⛅ neutral | medium | finviz sector median week -0.34% [tape] |
| sector:Healthcare | ⛅ neutral | medium | finviz sector median week -0.55% [tape] |
| sector:Industrials | ⛅ neutral | medium | finviz sector median week -1.32% [tape] |
| sector:Real Estate | 🌧️ hostile | medium | finviz sector median week -2.61% [tape] |
| sector:Technology | 🌤️ favorable | medium | news_judge SECTOR Technology [bullish] |
| sector:Utilities | 🌧️ hostile | medium | finviz sector median week -1.88% [tape] |

## Size

| Label | Weather | Conf | Why |
|---|---|---|---|
| size:micro | ⛅ neutral | low | risk-on, dollar flat [general predict + factors] |
| size:small | ⛅ neutral | low | risk-on, dollar flat [general predict + factors] |
| size:large | ⛅ neutral | low | risk-on — mega leads less in broad rallies [general predict] |
| size:mega | ⛅ neutral | low | risk-on — mega leads less in broad rallies [general predict] |
| size:mid | ⛅ neutral | low | no dedicated mid-cap signal in v1 |

## Beta & volatility

| Label | Weather | Conf | Why |
|---|---|---|---|
| beta:high | 🌤️ favorable | medium | risk-on, VIX falling — high beta outperforms [general + channel1] |
| beta:low | ⛅ neutral | low | risk-on — low beta lags rallies |
| beta:mid | ⛅ neutral | low | beta-neutral zone |

## Short interest (multiplier, not direction)

| Label | Weather | Conf | Why |
|---|---|---|---|
| short:high | 🌤️ favorable | low | risk-on — crowded shorts are squeeze FUEL if the tape rises (multiplier, not a direction) [general] |
| short:extreme | 🌤️ favorable | low | risk-on — crowded shorts are squeeze FUEL if the tape rises (multiplier, not a direction) [general] |
| short:low | ⛅ neutral | low | low short is not a tailwind by itself |
| short:mid | ⛅ neutral | low | no strong crowding signal |

## Profitability & style

| Label | Weather | Conf | Why |
|---|---|---|---|
| profit:no | ⛅ neutral | low | risk-on, F&G None |
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
| lev:high | 🌤️ favorable | low | easing + risk-on — leverage amplifies the upside [factors + general] |
| lev:low | ⛅ neutral | low | low leverage lags melt-ups |
| lev:mid | ⛅ neutral | low | — |
| lev:neg_equity | 🌧️ hostile | low | negative equity is distressed in any regime |

## Momentum state

| Label | Weather | Conf | Why |
|---|---|---|---|
| mom:uptrend | 🌤️ favorable | medium | risk-on + positive futures — trend continuation day [general + factors] |
| mom:downtrend | ⛅ neutral | low | — |
| mom:mixed | ⛅ neutral | low | — |

## Extension state

| Label | Weather | Conf | Why |
|---|---|---|---|
| ext:washed | 🌤️ favorable | low | washouts bounce in risk-on turns |
| ext:extended | ⛅ neutral | low | extension tolerated while tape is firm |
| ext:extreme | ⛅ neutral | low | extension tolerated while tape is firm |
| ext:neutral | ⛅ neutral | low | — |

## 52-week zone

| Label | Weather | Conf | Why |
|---|---|---|---|
| range:deep_low | ⛅ neutral | low | bottom-fishing only with confirmation |
| range:top | 🌤️ favorable | low | breakouts follow through on trend days |
| range:breakout | 🌤️ favorable | low | breakouts follow through on trend days |
| range:low | ⛅ neutral | low | — |
| range:mid | ⛅ neutral | low | — |
| range:high | ⛅ neutral | low | — |

## Geography

| Label | Weather | Conf | Why |
|---|---|---|---|
| geo:US | 🌤️ favorable | low | mirrors general risk-on [general predict] |
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

