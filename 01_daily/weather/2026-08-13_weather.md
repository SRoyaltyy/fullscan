# Weather report — 2026-08-13

Is today good for each *kind* of stock? Labels come from `data/universe/`; this file is the daily regime record the backtest will grade.

## Snapshot

- **Risk state:** ON (general predict up score +8.5, conf 0.6)
- **Yields:** falling | **Dollar/oil:** soft | **VIX:** unknown
- **Fear & Greed:** 63 (Greed) | **Yield/SPX 5d corr:** -0.38
- **High-impact events:** 0 bullish vs 0 bearish
- ⚠️ **Data gaps:** event scanner output

## Sectors

| Label | Weather | Conf | Why |
|---|---|---|---|
| sector:Basic Materials | ⛅ neutral | high | sector predict score -6.0 dir down conf 0.6 [sector board] |
| sector:Communication Services | 🌧️ hostile | high | sector predict score -9.0 dir down conf 0.6 [sector board] |
| sector:Consumer Cyclical | ⛅ neutral | high | sector predict score +2.0 dir up conf 0.5 [sector board] |
| sector:Consumer Defensive | ⛅ neutral | high | sector predict score +1.8 dir up conf 0.55 [sector board] |
| sector:Energy | ⛅ neutral | high | sector predict score -3.2 dir down conf 0.55 [sector board] |
| sector:Financial | 🌤️ favorable | high | sector predict score +9.7 dir up conf 0.55 [sector board] |
| sector:Healthcare | 🌤️ favorable | high | sector predict score +10.2 dir up conf 0.65 [sector board] |
| sector:Industrials | ⛅ neutral | high | sector predict score +5.0 dir up conf 0.55 [sector board] |
| sector:Real Estate | ⛅ neutral | high | sector predict score +5.5 dir up conf 0.6 [sector board] |
| sector:Technology | 🌤️ favorable | high | sector predict score +10.8 dir up conf 0.7 [sector board] |
| sector:Utilities | ⛅ neutral | high | sector predict score +5.5 dir up conf 0.55 [sector board] |

## Size

| Label | Weather | Conf | Why |
|---|---|---|---|
| size:micro | 🌤️ favorable | medium | risk-on with soft dollar — small-cap tape works; risk-on, dollar soft [general predict + factors] |
| size:small | 🌤️ favorable | medium | risk-on with soft dollar — small-cap tape works; risk-on, dollar soft [general predict + factors] |
| size:large | ⛅ neutral | low | risk-on — mega leads less in broad rallies [general predict] |
| size:mega | ⛅ neutral | low | risk-on — mega leads less in broad rallies [general predict] |
| size:mid | ⛅ neutral | low | no dedicated mid-cap signal in v1 |

## Beta & volatility

| Label | Weather | Conf | Why |
|---|---|---|---|
| beta:high | 🌤️ favorable | medium | risk-on, VIX unknown — high beta outperforms [general + channel1] |
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
| profit:no | ⛅ neutral | low | risk-on, F&G Greed |
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

