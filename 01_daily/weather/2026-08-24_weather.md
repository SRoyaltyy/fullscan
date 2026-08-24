# Weather report — 2026-08-24

Is today good for each *kind* of stock? Labels come from `data/universe/`; this file is the daily regime record the backtest will grade.

## Snapshot

- **Risk state:** OFF (general predict down score -5.2, conf 0.55)
- **Yields:** rising (llm_factor_fallback) | **Dollar:** strong (dxy) | **Oil:** falling | **VIX:** falling (ratio 0.85 via vix/vix3m) spot 15.77
- **Fear & Greed:** n/a | **Yield/SPX 5d corr:** -0.16
- **High-impact events:** 0 bullish vs 2 bearish | China: bear

## Sectors

| Label | Weather | Conf | Why |
|---|---|---|---|
| sector:Basic Materials | 🌤️ favorable | high | sector predict score +3.1 dir up conf 0.55 [sector board] |
| sector:Communication Services | 🌧️ hostile | high | sector predict score -3.1 dir down conf 0.48 [sector board] |
| sector:Consumer Cyclical | 🌧️ hostile | high | sector predict score -3.6 dir down conf 0.48 [sector board] |
| sector:Consumer Defensive | 🌤️ favorable | high | sector predict score +6.8 dir up conf 0.55 [sector board] |
| sector:Energy | 🌧️ hostile | high | sector predict score -10.8 dir down conf 0.56 [sector board] |
| sector:Financial | ⛅ neutral | high | sector predict score +0.0 dir flat conf 0.5 [sector board] |
| sector:Healthcare | ⛅ neutral | high | sector predict score +0.0 dir flat conf 0.5 [sector board] |
| sector:Industrials | ⛅ neutral | high | sector predict score -0.9 dir flat conf 0.48 [sector board] |
| sector:Real Estate | ⛅ neutral | high | sector predict score -0.9 dir flat conf 0.52 [sector board] |
| sector:Technology | 🌧️ hostile | high | sector predict score -8.1 dir down conf 0.58 [sector board] |
| sector:Utilities | 🌧️ hostile | high | sector predict score -3.1 dir down conf 0.52 [sector board] |

## Size

| Label | Weather | Conf | Why |
|---|---|---|---|
| size:micro | 🌧️ hostile | medium | risk-off — small caps de-rate first; risk-off, dollar strong [general predict + factors] |
| size:small | 🌧️ hostile | medium | risk-off — small caps de-rate first; risk-off, dollar strong [general predict + factors] |
| size:large | 🌤️ favorable | medium | risk-off — defensive/quality bid concentrates in large & mega [general predict] |
| size:mega | 🌤️ favorable | medium | risk-off — defensive/quality bid concentrates in large & mega [general predict] |
| size:mid | ⛅ neutral | low | no dedicated mid-cap signal in v1 |

## Beta & volatility

| Label | Weather | Conf | Why |
|---|---|---|---|
| beta:high | 🌧️ hostile | medium | risk-off — high beta is the exit door [channel1 + general] |
| beta:low | 🌤️ favorable | medium | defensive ballast bid in stress [general + channel1] |
| beta:mid | ⛅ neutral | low | beta-neutral zone |

## Short interest (multiplier, not direction)

| Label | Weather | Conf | Why |
|---|---|---|---|
| short:high | 🌧️ hostile | medium | risk-off — heavy short interest marks balance-sheet/dilution stress; it amplifies falls [general] |
| short:extreme | 🌧️ hostile | medium | risk-off — heavy short interest marks balance-sheet/dilution stress; it amplifies falls [general] |
| short:low | ⛅ neutral | low | low short is not a tailwind by itself |
| short:mid | ⛅ neutral | low | no strong crowding signal |

## Profitability & style

| Label | Weather | Conf | Why |
|---|---|---|---|
| profit:no | 🌧️ hostile | high | risk-off — unprofitable names are sold first [general] |
| profit:yes | 🌤️ favorable | medium | risk-off — quality/profitability bid [general] |
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
| mom:uptrend | ⛅ neutral | low | uptrends under test in risk-off |
| mom:downtrend | 🌧️ hostile | low | downtrends get no bid in risk-off |
| mom:mixed | ⛅ neutral | low | — |

## Extension state

| Label | Weather | Conf | Why |
|---|---|---|---|
| ext:washed | 🌧️ hostile | medium | falling knives stay sharp in risk-off |
| ext:extended | 🌧️ hostile | medium | parabolic + risk-off = nasty unwind risk |
| ext:extreme | 🌧️ hostile | medium | parabolic + risk-off = nasty unwind risk |
| ext:neutral | ⛅ neutral | low | — |

## 52-week zone

| Label | Weather | Conf | Why |
|---|---|---|---|
| range:deep_low | 🌧️ hostile | medium | falling knife zone in risk-off |
| range:top | 🌧️ hostile | low | high-zone names unwind in risk-off |
| range:breakout | 🌧️ hostile | low | high-zone names unwind in risk-off |
| range:low | ⛅ neutral | low | — |
| range:mid | ⛅ neutral | low | — |
| range:high | ⛅ neutral | low | — |

## Geography

| Label | Weather | Conf | Why |
|---|---|---|---|
| geo:US | 🌧️ hostile | low | mirrors general risk-off [general predict] |
| geo:ADR-China | 🌧️ hostile | medium | high-impact China event(s) lean bear [event scanner] |

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

