# Weather report — 2026-11-10

Is today good for each *kind* of stock? Labels come from `data/universe/`; this file is the daily regime record the backtest will grade.

## Snapshot

- **Risk state:** ON (general predict up score +6.9, conf 0.775)
- **Yields:** flat (fred_dgs10) | **Dollar:** strong (dxy) | **Oil:** falling | **VIX:** calm (ratio 1.02 via vix/vix3m) spot 21.23
- **Fear & Greed:** n/a | **Yield/SPX 5d corr:** +0.37
- **High-impact events:** 2 bullish vs 3 bearish | China: bull

## Sectors

| Label | Weather | Conf | Why |
|---|---|---|---|
| sector:Basic Materials | ⛅ neutral | high | sector predict score +1.5 dir up conf 0.558 [sector board] |
| sector:Communication Services | 🌤️ favorable | high | sector predict score +3.8 dir up conf 0.651 [sector board] |
| sector:Consumer Cyclical | 🌤️ favorable | high | sector predict score +6.4 dir up conf 0.755 [sector board] |
| sector:Consumer Defensive | ⛅ neutral | high | sector predict score -2.8 dir down conf 0.512 [sector board] |
| sector:Energy | 🌧️ hostile | high | sector predict score -9.1 dir down conf 0.85 [sector board] |
| sector:Financial | 🌤️ favorable | medium | news_judge SECTOR Financial [bullish] |
| sector:Healthcare | 🌤️ favorable | high | sector predict score +6.4 dir up conf 0.756 [sector board] |
| sector:Industrials | 🌤️ favorable | high | sector predict score +4.3 dir up conf 0.571 [sector board] |
| sector:Real Estate | 🌤️ favorable | high | sector predict score +7.5 dir up conf 0.798 [sector board] |
| sector:Technology | 🌧️ hostile | high | sector predict score -6.4 dir down conf 0.756 [sector board] |
| sector:Utilities | ⛅ neutral | high | sector predict score +0.5 dir up conf 0.421 [sector board] |

## Size

| Label | Weather | Conf | Why |
|---|---|---|---|
| size:micro | 🌧️ hostile | medium | strong dollar — small caps de-rate first; risk-on, dollar strong [general predict + factors] |
| size:small | 🌧️ hostile | medium | strong dollar — small caps de-rate first; risk-on, dollar strong [general predict + factors] |
| size:large | ⛅ neutral | low | risk-on — mega leads less in broad rallies [general predict] |
| size:mega | ⛅ neutral | low | risk-on — mega leads less in broad rallies [general predict] |
| size:mid | ⛅ neutral | low | no dedicated mid-cap signal in v1 |

## Beta & volatility

| Label | Weather | Conf | Why |
|---|---|---|---|
| beta:high | 🌤️ favorable | medium | risk-on, VIX calm — high beta outperforms [general + channel1] |
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

