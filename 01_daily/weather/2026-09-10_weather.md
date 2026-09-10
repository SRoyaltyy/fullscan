# Weather report — 2026-09-10

Is today good for each *kind* of stock? Labels come from `data/universe/`; this file is the daily regime record the backtest will grade.

## Snapshot

- **Risk state:** UNKNOWN
- **Yields:** flat (fred_dgs10) | **Dollar:** flat (dxy) | **Oil:** rising | **VIX:** calm (ratio 1.08 via vix/ma20) spot 16.51
- **Fear & Greed:** n/a | **Yield/SPX 5d corr:** -0.97
- **High-impact events:** 3 bullish vs 8 bearish | China: bull
- ⚠️ **Data gaps:** general predict run, general predict factor scoreboard

## Sectors

| Label | Weather | Conf | Why |
|---|---|---|---|
| sector:Basic Materials | ⛅ neutral | medium | finviz sector median week -0.12% [tape] |
| sector:Communication Services | 🌧️ hostile | medium | finviz sector median week -2.24% [tape] |
| sector:Consumer Cyclical | 🌧️ hostile | medium | finviz sector median week -2.02% [tape] |
| sector:Consumer Defensive | 🌧️ hostile | medium | finviz sector median week -2.97% [tape] |
| sector:Energy | 🌤️ favorable | medium | news_judge SECTOR Energy [bullish] |
| sector:Financial | ⛅ neutral | medium | finviz sector median week +0.08% [tape] |
| sector:Healthcare | 🌤️ favorable | medium | news_judge SECTOR Healthcare [bullish] |
| sector:Industrials | ⛅ neutral | medium | finviz sector median week +0.17% [tape] |
| sector:Real Estate | ⛅ neutral | medium | finviz sector median week -1.14% [tape] |
| sector:Technology | 🌤️ favorable | medium | news_judge SECTOR Technology [bullish] |
| sector:Utilities | 🌧️ hostile | medium | news_judge SECTOR Utilities [bearish] |

## Size

| Label | Weather | Conf | Why |
|---|---|---|---|
| size:micro | ❔ unknown | low | no general predict |
| size:small | ❔ unknown | low | no general predict |
| size:mid | ❔ unknown | low | no general predict |
| size:large | ❔ unknown | low | no general predict |
| size:mega | ❔ unknown | low | no general predict |

## Beta & volatility

| Label | Weather | Conf | Why |
|---|---|---|---|
| beta:high | ⛅ neutral | low | risk-unknown, VIX calm |
| beta:low | ⛅ neutral | low | risk-unknown, VIX calm |
| beta:mid | ⛅ neutral | low | beta-neutral zone |

## Short interest (multiplier, not direction)

| Label | Weather | Conf | Why |
|---|---|---|---|
| short:low | ❔ unknown | low | no general predict |
| short:mid | ❔ unknown | low | no general predict |
| short:high | ❔ unknown | low | no general predict |
| short:extreme | ❔ unknown | low | no general predict |

## Profitability & style

| Label | Weather | Conf | Why |
|---|---|---|---|
| profit:no | ⛅ neutral | low | risk-unknown, F&G None |
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
| mom:uptrend | ❔ unknown | low | no general predict |
| mom:downtrend | ❔ unknown | low | no general predict |
| mom:mixed | ❔ unknown | low | no general predict |

## Extension state

| Label | Weather | Conf | Why |
|---|---|---|---|
| ext:extreme | ❔ unknown | low | no general predict |
| ext:extended | ❔ unknown | low | no general predict |
| ext:washed | ❔ unknown | low | no general predict |
| ext:neutral | ❔ unknown | low | no general predict |

## 52-week zone

| Label | Weather | Conf | Why |
|---|---|---|---|
| range:deep_low | ❔ unknown | low | no general predict |
| range:low | ❔ unknown | low | no general predict |
| range:mid | ❔ unknown | low | no general predict |
| range:high | ❔ unknown | low | no general predict |
| range:top | ❔ unknown | low | no general predict |
| range:breakout | ❔ unknown | low | no general predict |

## Geography

| Label | Weather | Conf | Why |
|---|---|---|---|
| geo:US | ❔ unknown | low | mirrors general risk-unknown [general predict] |
| geo:ADR-China | 🌤️ favorable | medium | high-impact China event(s) lean bull [event scanner] |

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

