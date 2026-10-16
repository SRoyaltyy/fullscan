# Weather report — 2026-10-16

Is today good for each *kind* of stock? Labels come from `data/universe/`; this file is the daily regime record the backtest will grade.

## Snapshot

- **Risk state:** ON (general predict up score +5.5, conf 0.721)
- **Yields:** flat (fred_dgs10) | **Dollar:** flat (dxy) | **Oil:** flat | **VIX:** spiking (ratio 1.02 via vix/vix3m) spot 21.7
- **Fear & Greed:** n/a | **Yield/SPX 5d corr:** -0.63
- **High-impact events:** 8 bullish vs 10 bearish

## Sectors

| Label | Weather | Conf | Why |
|---|---|---|---|
| sector:Basic Materials | 🌧️ hostile | medium | finviz sector median week -4.97% [tape] |
| sector:Communication Services | 🌧️ hostile | medium | finviz sector median week -2.75% [tape] |
| sector:Consumer Cyclical | ⛅ neutral | medium | finviz sector median week -1.38% [tape] |
| sector:Consumer Defensive | 🌧️ hostile | medium | finviz sector median week -1.62% [tape] |
| sector:Energy | 🌧️ hostile | medium | finviz sector median week -2.50% [tape] |
| sector:Financial | ⛅ neutral | medium | finviz sector median week -1.40% [tape] |
| sector:Healthcare | 🌤️ favorable | medium | news_judge SECTOR Healthcare [bullish] |
| sector:Industrials | 🌧️ hostile | medium | news_judge SECTOR Industrials [bearish] |
| sector:Real Estate | 🌧️ hostile | medium | finviz sector median week -2.49% [tape] |
| sector:Technology | 🌧️ hostile | medium | news_judge SECTOR Technology [bearish] |
| sector:Utilities | 🌧️ hostile | medium | finviz sector median week -3.43% [tape] |

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
| beta:high | 🌧️ hostile | high | VIX spiking — high beta is the exit door [channel1 + general] |
| beta:low | 🌤️ favorable | medium | defensive ballast bid in stress [general + channel1] |
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

