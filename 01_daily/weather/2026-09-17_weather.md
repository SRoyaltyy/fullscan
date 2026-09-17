# Weather report — 2026-09-17

Is today good for each *kind* of stock? Labels come from `data/universe/`; this file is the daily regime record the backtest will grade.

## Snapshot

- **Risk state:** ON (general predict up score +7.4, conf 0.795)
- **Yields:** rising (fred_dgs10) | **Dollar:** soft (dxy) | **Oil:** falling | **VIX:** falling (ratio 0.81 via vix/vix3m) spot 15.99
- **Fear & Greed:** n/a | **Yield/SPX 5d corr:** -0.11
- **High-impact events:** 2 bullish vs 7 bearish

## Sectors

| Label | Weather | Conf | Why |
|---|---|---|---|
| sector:Basic Materials | 🌤️ favorable | high | sector predict score +4.9 dir flat conf 0.55 [sector board] |
| sector:Communication Services | 🌤️ favorable | high | sector predict score +5.3 dir up conf 0.65 [sector board] |
| sector:Consumer Cyclical | 🌤️ favorable | high | sector predict score +7.7 dir flat conf 0.55 [sector board] |
| sector:Consumer Defensive | ⛅ neutral | high | sector predict score +0.8 dir up conf 0.431 [sector board] |
| sector:Energy | 🌧️ hostile | medium | news_judge SECTOR Energy [bearish] |
| sector:Financial | 🌤️ favorable | high | sector predict score +6.1 dir flat conf 0.55 [sector board] |
| sector:Healthcare | 🌤️ favorable | high | sector predict score +4.8 dir up conf 0.594 [sector board] |
| sector:Industrials | 🌤️ favorable | high | sector predict score +6.3 dir flat conf 0.55 [sector board] |
| sector:Real Estate | 🌤️ favorable | high | sector predict score +4.9 dir flat conf 0.55 [sector board] |
| sector:Technology | 🌤️ favorable | medium | news_judge SECTOR Technology [bullish] |
| sector:Utilities | ⛅ neutral | high | sector predict score +1.8 dir flat conf 0.47 [sector board] |

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

