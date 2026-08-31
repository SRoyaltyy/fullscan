# Stock book — 2026-08-31

_Generated 2026-08-31T12:30:06.130112-04:00_

This file is the **human read** of one run. CSV/JSON next to it are the machine files.

## How today's rank is built

Every liquid name ($80M+ mcap, 500k+ ADV) gets six signals, then a mid-cap opportunity add-on:

1. **Join × weather** — do this *kind* of stock (sector, size, trend, leverage, earnings) fit today's tape?
2. **Sector / general predict** — same-day LLM calls only. Missing file = 0, never yesterday's leftover.
3. **News / judge** — headlines plus the news-judge ticker list (AU/ADBE-style).
4. **AB checklist** — structure score + P01–P04 (beats peers? peers up? industry up? sector board up?).
5. **Peer RS** — this week's return vs that name's own correlated basket. Kills XLE clones.
6. **Mid-cap opportunity** — extra points for liquid small/mid ($400M–$20B) that are not jammed at the 52-week high. Micros skipped. Max 4 large/mega.

**All-green BUY.** A name is green when join, AB, and peer are all ≥ +0.05, sector and news are not red, and relvol is not dead (< 0.7). General is a market-wide SPX stamp — a modest red general does not empty the pile. A hard-red general (≤ −0.25) is a veto unless the same-day sector call is green. Event-scanner sector tilt is clipped to ±0.20 so it cannot invert the day's essay. mid_opp is capped at +0.20 and zeroed on hard sector-red names. BUY also drops LAG+peer-losers, printed dead volume, and the lookback marks the sheet already used: 🚨 alarm (purely worse), Cond-red, region-red, and featured fade setups (first_crack / alarm|heat=bad). 🔵 blue gets a small boost. ⚪ white (no red cells) is recorded but not a hard gate — a red general makes it empty. If ≥ 8 liquid green names survive the $400M / 4-per-sector / 3-per-industry / 4 large-mega caps, BUY 15 is filled from that pile by green_rank (no opp). If the pile is thinner (usually AB or peers all zeros), the ranker keeps the weighted walk under the same vetoes. **SELL always ranks on core weights** and never shorts a green name when the pile is used.

**1d** leans on news + AB + peers. **1m** drops news and leans on AB + peers + join.
A sector headline (e.g. ADBE) cannot zero a mid-cap that just beat earnings or is leading its own peers.

## Today's regime

- Weather risk: **off**
- General predict (same-day): -0.47 down (present)
- Stand-down: **YES — no BUY** — general down bias=-0.47 risk=off and 0 usable company dossiers — no BUY
- Sector predicts this date: 10/11 (ok)
- News tickers in play: 120
- AB coverage: 2551 names · peer RS: 2400
- Universe after liquidity: 2685
- BUY window: $80M ADV, opportunity $400M–$20B, max 4/sector, 3/industry, 4 large/mega
- News names after digest+judge: 105

## All-green BUY / SELL

- Stand-down: **no BUY.** general down bias=-0.47 risk=off and 0 usable company dossiers — no BUY
- Pile still computed (93 liquid green of 2685) but is not used to force names into a no-win open.
- SELL still ranks on full weights.

## Finviz outperform board (industry + theme)

This is the live Finviz groups tape — child industry vs parent sector, plus theme joins. Sector LLM essays are a separate (and often disagreeing) layer.

- Heat into the ranker today: **finviz_tape** (36 captains, 15 industries → s_heat).
- Board file: `01_daily/map_heat/2026-08-31_map_heat.json` · generated 2026-08-31T03:48:25.433368-04:00

### Sector RS vs same-day LLM essay

| Sector | Finviz 1d | Finviz 1w | LLM 1d | Tape vs essay |
|--------|----------:|----------:|-------:|---------------|
| Basic Materials | -1.5% | -1.2% | -0.55 |  |
| Communication Services | +1.4% | +1.3% | — |  |
| Consumer Cyclical | +1.4% | -0.6% | -0.55 |  |
| Consumer Defensive | +0.6% | -0.3% | +0.00 |  |
| Energy | +0.1% | -2.1% | +0.47 | essay UP, tape DOWN |
| Financial | +0.3% | +0.9% | +0.25 |  |
| Healthcare | -0.8% | -2.2% | -0.50 |  |
| Industrials | -1.0% | -1.5% | -0.28 |  |
| Real Estate | -0.4% | -1.3% | -0.55 |  |
| Technology | -1.4% | +1.2% | -0.28 | essay DOWN, tape UP |
| Utilities | -1.1% | -0.3% | -0.28 |  |

### Industry heat (1w vs parent)

**HOT**

- **Infrastructure Operations** (Industrials) -1.0% 1d · +8.8% 1w · vs parent +10.3% · —
- **Software - Infrastructure** (Technology) +0.2% 1d · +5.3% 1w · vs parent +4.1% · MSFT, PLTR, ZETA, QLYS
- **Software - Application** (Technology) +0.4% 1d · +4.2% 1w · vs parent +3.0% · CRM, UBER, FROG, TTAN
- **Thermal Coal** (Energy) -0.6% 1d · +4.1% 1w · vs parent +6.3% · CNR, BTU
- **Coking Coal** (Basic Materials) -1.2% 1d · +3.5% 1w · vs parent +4.7% · HCC, AMR
- **Consumer Electronics** (Technology) +1.7% 1d · +3.4% 1w · vs parent +2.1% · AAPL, SONO
- **Pharmaceutical Retailers** (Healthcare) -1.7% 1d · +3.1% 1w · vs parent +5.3% · —
- **Publishing** (Communication Services) +0.1% 1d · +3.0% 1w · vs parent +1.7% · WLY, TDAY

**COLD**

- **Textile Manufacturing** (Consumer Cyclical) -1.6% 1d · -7.8% 1w · vs parent -7.2% · AIN
- **Engineering & Construction** (Industrials) -3.2% 1d · -5.8% 1w · vs parent -4.4% · PWR, FIX, ACA, FLR
- **Specialty Retail** (Consumer Cyclical) -1.1% 1d · -5.8% 1w · vs parent -5.2% · CASY, WSM, RH, ASO
- **Chemicals** (Basic Materials) +0.2% 1d · -5.1% 1w · vs parent -3.9% · DOW, HUN, REX
- **Semiconductor Equipment & Materials** (Technology) -4.0% 1d · -4.8% 1w · vs parent -6.0% · LRCX, AMAT, ACMR, KLIC
- **Utilities - Renewable** (Utilities) -3.3% 1d · -4.3% 1w · vs parent -4.0% · ORA, FLNC
- **Trucking** (Industrials) -1.3% 1d · -4.1% 1w · vs parent -2.6% · ODFL, RXO, ARCB
- **Rental & Leasing Services** (Industrials) -0.8% 1d · -4.1% 1w · vs parent -2.6% · URI, GATX, HRI

### Overrides (child 1w residual ≥ 3pp)

| Action | Industry | 1w | Parent 1w | Gap | Captains |
|--------|----------|---:|----------:|----:|----------|
| OVERRIDE | Infrastructure Operations | +8.8% | -1.5% | +10.3% | — |
| SPLIT | Textile Manufacturing | -7.8% | -0.6% | -7.2% | AIN |
| OVERRIDE | Thermal Coal | +4.1% | -2.1% | +6.3% | CNR, BTU |
| OVERRIDE | Semiconductor Equipment & Materials | -4.8% | +1.2% | -6.0% | LRCX, AMAT, ACMR, KLIC |
| OVERRIDE | Pharmaceutical Retailers | +3.1% | -2.2% | +5.3% | — |
| SPLIT | Specialty Retail | -5.8% | -0.6% | -5.2% | CASY, WSM, RH, ASO |
| OVERRIDE | Medical Distribution | +2.7% | -2.2% | +4.9% | MCK, COR |
| OVERRIDE | Coking Coal | +3.5% | -1.2% | +4.7% | HCC, AMR |
| OVERRIDE | Solar | -3.2% | +1.2% | -4.5% | FSLR, RUN, SHLS |
| SPLIT | Engineering & Construction | -5.8% | -1.5% | -4.4% | PWR, FIX, ACA, FLR |
| SPLIT | Software - Infrastructure | +5.3% | +1.2% | +4.1% | MSFT, PLTR, ZETA, QLYS |
| SPLIT | Utilities - Renewable | -4.3% | -0.3% | -4.0% | ORA, FLNC |
| OVERRIDE | Electronic Gaming & Multimedia | -2.7% | +1.3% | -4.0% | TTWO |
| SPLIT | Chemicals | -5.1% | -1.2% | -3.9% | DOW, HUN, REX |
| OVERRIDE | Steel | +2.6% | -1.2% | +3.8% | NUE, STLD, WS, NWPX |

### Theme join (sub-sector vs GICS parent)

- **Energy Traditional** — Oil / Majors: -1.5% 1w vs parent -2.1% → AGREE; Oil E&P: -3.6% 1w vs parent -2.1% → AGREE; Oil Services: +1.6% 1w vs parent -2.1% → **DIVERGE**; Nuclear: -1.4% 1w vs parent -2.1% → AGREE
- **Commodities Energy** — Uranium: -2.9% 1w vs parent -1.7% → AGREE; Oil (commodity): -3.7% 1w vs parent -1.7% → AGREE
- **Energy Renewable** — Solar: -3.2% 1w vs parent -0.4% → AGREE; Renewable utilities: -4.3% 1w vs parent -0.4% → AGREE
- **Commodities Metals** — Gold: -3.1% 1w vs parent -1.2% → AGREE; Silver: +1.0% 1w vs parent -1.2% → **DIVERGE**; Copper: -1.7% 1w vs parent -1.2% → AGREE; Other precious: -2.0% 1w vs parent -1.2% → AGREE
- **Semiconductors** — Semis: -0.3% 1w vs parent +1.2% → **DIVERGE**; Semi equipment: -4.8% 1w vs parent +1.2% → **DIVERGE**
- **Artificial Intelligence** — AI compute / semis: -0.3% 1w vs parent +1.2% → **DIVERGE**; Software infra: +5.3% 1w vs parent +1.2% → AGREE
- **Defense & Aerospace** — Aero / defense: -0.2% 1w vs parent -1.5% → AGREE

### Theme ETF tape (biggest |1w| moves)

| Theme | 1d | 1w | Leaders |
|-------|---:|---:|---------|
| Space Exploration & Technology | -2.3% | -5.0% | NASA, ARKX, UFO |
| Industrials | -1.3% | -2.5% | XLI, ITA, AIRR |
| Future Mobility Production & Tech | -0.7% | -2.2% | DRIV, ROKT, IDRV |
| Healthcare | -1.5% | -2.2% | XLV, VHT, XBI |
| Fintech | -4.9% | -1.6% | BLOK, ARKF, BITQ |
| Materials | -3.4% | -1.5% | GDX, GDXJ, XLB |
| Natural Resources | -0.6% | -1.5% | GUNR, GNR, PHO |
| Cannabis Based Businesses | +3.3% | +1.2% | MSOS, MJ, CNBS |
| Real Estate | -0.6% | -1.2% | VNQ, SCHH, XLRE |
| Consumer Discretionary | +0.8% | -1.1% | XLY, VCR, TSLL |
| Real Assets | -0.7% | -1.1% | ABLD, VRAI, CSRA |
| Communication Services | +1.1% | +1.0% | XLC, VOX, FCOM |

## Inputs this run — every resource

If a row says **missing**, that layer scored 0 today. If it says **found**, it moved the rank.

| Resource | This run | Where it lands in the score |
|----------|----------|-----------------------------|
| Finviz Elite export | **found** | liquidity + labels + AB proxy + digest |
| Labels / membership | **found** | join + mid_opp + earnings/range |
| Weather (tape + FRED/DXY/VIX) | **found** | join × weather |
| Channel 1 raw | **found** | via weather |
| Join ranked universe | **found** | s_join |
| News parse + actions | **found** | s_news |
| News judge | **found** | s_news ticker tilts |
| Finviz daily digest | **found** | s_news company headlines |
| General predict | **found** | s_general × beta |
| Sector LLM essays | **found** | s_sector (0 if essays missing) |
| AB checklist + P01–P04 | **found** | s_ab |
| Peer RS | **found** | s_peer |
| Ticker checklist (rebound) | **found** | rebound_floor (dated file, else latest — can be stale) |
| Event scanner | **found** | sector tilt + weather |
| Finviz map heat (industry RS / themes) | **found** | industry residual + theme tape → s_heat when research is gone |
| Map heat captain research | **missing / not in ranker** | Grok captain essays (strict morning_refresh; else Finviz tape) |
| Catalyst overlays | **missing / not in ranker** | not in ranker — separate chart workflow |
| Insider / politician flow | **missing / not in ranker** | no daily file in repo |
| Industry predict | **found** | not scored (ad-hoc only) |
| Learnings / mutable policy | **missing / not in ranker** | next predict prompt, not a ticker score |

### Sector LLM bias (1d) — 0 / empty means that essay was not run today

| Sector | bias |
|--------|------|
| Basic Materials | -0.55 |
| Real Estate | -0.55 |
| Consumer Cyclical | -0.55 |
| Healthcare | -0.50 |
| Energy | +0.47 |
| Industrials | -0.28 |
| Technology | -0.28 |
| Utilities | -0.28 |
| Financial | +0.25 |
| Consumer Defensive | +0.00 |

### How much each predictor is trusted (graded hit rate)

| Topic | hit rate | n | weight |
|-------|----------|---|--------|
| general | 48% | 21 | ×0.85 |
| sector:Basic Materials | 58% | 12 | ×1.00 |
| sector:Communication Services | 25% | 12 | ×0.50 |
| sector:Consumer Cyclical | 58% | 12 | ×1.00 |
| sector:Consumer Defensive | 42% | 12 | ×0.50 |
| sector:Energy | 50% | 12 | ×0.85 |
| sector:Financial | 33% | 12 | ×0.50 |
| sector:Healthcare | 78% | 9 | ×1.00 |
| sector:Industrials | 25% | 12 | ×0.50 |
| sector:Real Estate | 58% | 12 | ×1.00 |
| sector:Technology | 42% | 12 | ×0.50 |
| sector:Utilities | 42% | 12 | ×0.50 |

## Horizon weights — book_policy.json v5

| Horizon | join | sector | general | news | AB | peer | + opportunity |
|---------|------|--------|---------|------|----|------|----------------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 | additive |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 | additive |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 | additive |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 | additive |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 | additive |

## 1d BUY — why these names

_general down bias=-0.47 risk=off and 0 usable company dossiers — no BUY_


## 1d AVOID — bottom of the same rank

- **LUNR** (mid, Industrials, $2.7B) score -0.565. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EH** (micro, Industrials, $259M) score -0.556. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AIIO** (small, Consumer Cyclical, $381M) score -0.555. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BKSY** (small, Industrials, $970M) score -0.553. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RKLB** (large, Industrials, $38.2B) score -0.548. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LWLG** (small, Basic Materials, $845M) score -0.548. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FIP** (small, Industrials, $422M) score -0.545. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **SPIR** (small, Industrials, $528M) score -0.544. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RCAT** (small, Industrials, $1.3B) score -0.543. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EVTL** (micro, Industrials, $118M) score -0.543. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HPP** (small, Real Estate, $729M) score -0.542. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **KODK** (small, Industrials, $885M) score -0.541. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **PENN** (mid, Consumer Cyclical, $2.3B) score -0.540. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **QTRX** (micro, Healthcare, $125M) score -0.532. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EVGO** (small, Consumer Cyclical, $437M) score -0.532. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SHMD** (micro, Industrials, $207M) score -0.524. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LFMD** (micro, Healthcare, $151M) score -0.515. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DHC** (small, Real Estate, $1.8B) score -0.515. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **OPEN** (mid, Real Estate, $3.2B) score -0.514. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GNRC** (large, Industrials, $10.8B) score -0.512. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FCEL** (small, Industrials, $1.4B) score -0.512. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **REAX** (small, Real Estate, $464M) score -0.511. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GUTS** (micro, Healthcare, $107M) score -0.507. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RDW** (mid, Industrials, $2.7B) score -0.504. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **HYPR** (micro, Healthcare, $88M) score -0.501. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## 3d BUY (compact — same names, different weights)

_general down bias=-0.47 risk=off and 0 usable company dossiers — no BUY_


## 1w BUY (compact — same names, different weights)

_general down bias=-0.47 risk=off and 0 usable company dossiers — no BUY_


## 2w BUY (compact — same names, different weights)

_general down bias=-0.47 risk=off and 0 usable company dossiers — no BUY_


## 1m BUY — why these names

_general down bias=-0.47 risk=off and 0 usable company dossiers — no BUY_


## 1m AVOID — bottom of the same rank

- **HPP** (small, Real Estate, $729M) score -0.666. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **LUNR** (mid, Industrials, $2.7B) score -0.666. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EH** (micro, Industrials, $259M) score -0.657. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **REAX** (small, Real Estate, $464M) score -0.652. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BKSY** (small, Industrials, $970M) score -0.650. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **OPEN** (mid, Real Estate, $3.2B) score -0.648. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RKLB** (large, Industrials, $38.2B) score -0.647. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **SPIR** (small, Industrials, $528M) score -0.646. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **RCAT** (small, Industrials, $1.3B) score -0.644. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **DHC** (small, Real Estate, $1.8B) score -0.644. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **CMTG** (micro, Real Estate, $229M) score -0.639. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FIP** (small, Industrials, $422M) score -0.635. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **SHMD** (micro, Industrials, $207M) score -0.625. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EVTL** (micro, Industrials, $118M) score -0.620. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PENN** (mid, Consumer Cyclical, $2.3B) score -0.607. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **FCEL** (small, Industrials, $1.4B) score -0.604. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **LWLG** (small, Basic Materials, $845M) score -0.603. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **KODK** (small, Industrials, $885M) score -0.603. this name **lagged its own correlated peers** this week; the peer basket itself was **up**; the Finviz industry was **down**
- **RDW** (mid, Industrials, $2.7B) score -0.597. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **AIIO** (small, Consumer Cyclical, $381M) score -0.597. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **PCT** (small, Industrials, $1.3B) score -0.587. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EVGO** (small, Consumer Cyclical, $437M) score -0.586. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **GNRC** (large, Industrials, $10.8B) score -0.582. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **BEPC** (mid, Utilities, $5.9B) score -0.582. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**
- **EOSE** (small, Industrials, $1.2B) score -0.581. this name **lagged its own correlated peers** this week; the peer basket itself was **down** (name-specific, not a sector tide); the Finviz industry was **down**

## Files for this run

- This rationale: `01_daily/2026-08-31_stock_book.md`
- Machine table: `data/stock_book/2026-08-31_stock_book.csv`
- Machine book: `data/stock_book/2026-08-31_stock_book.json`
- Join rank: `data/join/2026-08-31_ranked.csv`
- Weather: `01_daily/weather/2026-08-31_weather.md`
- AB enrich: `data/ab_checklist/2026-08-31_ab_checklist_enriched.md`
- Peer RS: `01_daily/2026-08-31_peer_rs.md`
- Finviz map heat: `01_daily/map_heat/2026-08-31_map_heat.md`
