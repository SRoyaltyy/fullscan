# Stock book — **2026-08-20** (1d / 3d / 1w / 2w / 1m)

Generated: 2026-08-21T01:46:02.738341-04:00

Layers: join (labels×weather) + same-day sector/general + news + AB checklist + peer RS. Max 4 names/sector, 3/industry. Names already on yesterday's book are penalized unless AB/peer/news is fresh.

## Regime snapshot

- **General bias (same-day):** +0.47 (yes)
- **Sector predicts this date:** 0/11
- **Weather risk:** off
- **News tickers:** 38
- **AB names:** 2425 · **peer RS names:** 2415
- **Universe (after liquidity):** 2707
- **Gates:** mcap ≥ $80.0M, avg vol ≥ 500.0k
- **Rebound floor tags:** 88

### Sector bias

| Sector | bias |
|--------|------|

### Learning gate (graded accuracy → how much each predictor is trusted)

| Topic | hit rate | graded runs | weight applied |
|-------|----------|-------------|----------------|
| general | 50% | 18 | ×0.85 |
| sector:Basic Materials | 57% | 7 | ×1.00 |
| sector:Communication Services | 43% | 7 | ×0.50 |
| sector:Consumer Cyclical | 86% | 7 | ×1.00 |
| sector:Consumer Defensive | 57% | 7 | ×1.00 |
| sector:Energy | 71% | 7 | ×1.00 |
| sector:Financial | 43% | 7 | ×0.50 |
| sector:Healthcare | 71% | 7 | ×1.00 |
| sector:Industrials | 29% | 7 | ×0.50 |
| sector:Real Estate | 71% | 7 | ×1.00 |
| sector:Technology | 57% | 7 | ×1.00 |
| sector:Utilities | 57% | 7 | ×1.00 |

## Horizon weights

| Horizon | join | sector | general | news | AB | peer |
|---------|------|--------|---------|------|----|------|
| 1d | 0.12 | 0.10 | 0.08 | 0.25 | 0.25 | 0.20 |
| 3d | 0.16 | 0.14 | 0.08 | 0.16 | 0.26 | 0.20 |
| 1w | 0.18 | 0.16 | 0.08 | 0.10 | 0.28 | 0.20 |
| 2w | 0.20 | 0.18 | 0.08 | 0.06 | 0.28 | 0.20 |
| 1m | 0.22 | 0.20 | 0.08 | 0.00 | 0.30 | 0.20 |

## 1d — BUY (top 15)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AU | +0.628 | Basic Materials | join=+0.99; gen1d=+0.07; news=+0.80; ev=news_judge; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.64 |
| DVN | +0.616 | Energy | join=+0.99; gen1d=+0.07; news=+0.83; ev=hormuz_energy_risk; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.41 |
| APA | +0.567 | Energy | join=+0.97; gen1d=+0.07; news=+0.83; ev=hormuz_energy_risk; ab=+0.55; LEAD,peers↑,ind↑; peer=+0.49 |
| COP | +0.565 | Energy | join=+0.99; gen1d=+0.07; news=+0.83; ev=hormuz_energy_risk; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.15 |
| TEM | +0.542 | Healthcare | join=+0.78; gen1d=+0.47; ab=+0.55; LEAD,peers↑,ind↓; peer=+0.96; rebound_floor |
| ELF | +0.523 | Consumer Defensive | join=+0.87; gen1d=+0.47; ab=+0.55; LEAD,peers↓,ind↓; peer=+0.81; rebound_floor |
| TRGP | +0.520 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.92 |
| MGTX | +0.518 | Healthcare | join=+0.94; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.87 |
| GRAL | +0.509 | Healthcare | join=+0.67; gen1d=+0.47; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.76; rebound_floor |
| AUPH | +0.504 | Healthcare | join=+0.93; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.76 |
| HL | +0.486 | Basic Materials | join=+0.99; gen1d=+0.47; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.70 |
| MEOH | +0.485 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.85 |
| HCC | +0.483 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.78 |
| SLQT | +0.457 | Financial | join=+0.45; gen1d=+0.47; ab=+0.70; LEAD,peers↓,ind↑; peer=+0.95 |
| GSHD | +0.444 | Financial | join=+0.91; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.47 |

## 1d — SELL / avoid (bottom 15)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AMPG | -0.465 | Technology | join=-0.93; gen1d=+0.07; ab=-0.64; LAG,peers↓,ind↓; peer=-1.00 |
| ALK | -0.453 | Industrials | join=-0.80; gen1d=+0.23; news=-0.69; ev=hormuz_energy_risk; ab=-0.36; LAG,peers↓,ind↓; peer=-0.57 |
| EOSE | -0.446 | Industrials | join=-0.98; gen1d=+0.47; ab=-0.70; LAG,peers↓,ind↓; peer=-0.95 |
| OI | -0.423 | Consumer Cyclical | join=-0.97; gen1d=+0.07; ab=-0.70; LAG,peers↓,ind↓; peer=-0.68 |
| SSTK | -0.423 | Communication Services | join=-0.84; gen1d=+0.23; ab=-0.76; LAG,peers↓,ind↓; peer=-0.75 |
| BIDU | -0.422 | Communication Services | join=-0.95; gen1d=+0.07; ab=-0.55; LAG,peers↓,ind↓; peer=-0.87 |
| JBLU | -0.413 | Industrials | join=-0.98; gen1d=+0.47; ab=-0.70; LAG,peers↓,ind↓; peer=-0.79 |
| GETY | -0.410 | Communication Services | join=-0.76; gen1d=+0.47; ab=-0.70; LAG,peers↓,ind↓; peer=-0.90 |
| FLNC | -0.409 | Utilities | join=-0.59; gen1d=+0.47; ab=-0.76; LAG,peers↓,ind↓; peer=-0.93 |
| TRIP | -0.408 | Consumer Cyclical | join=-0.94; gen1d=+0.23; ab=-0.64; LAG,peers↓,ind↓; peer=-0.77 |
| VNET | -0.406 | Technology | join=-0.94; gen1d=+0.07; ab=-0.55; LAG,peers↓,ind↓; peer=-0.80 |
| THRY | -0.398 | Technology | join=-0.81; gen1d=+0.23; ab=-0.64; LAG,peers↓,ind↓; peer=-0.80 |
| BYRN | -0.397 | Industrials | join=-0.84; gen1d=+0.47; ab=-0.64; LAG,peers↓,ind↓; peer=-0.88 |
| LCID | -0.396 | Consumer Cyclical | join=-0.98; gen1d=+0.23; ab=-0.46; LAG,peers↑,ind↓; peer=-0.91 |
| TTD | -0.395 | Communication Services | join=-0.95; gen1d=+0.23; ab=-0.64; LAG,peers↓,ind↓; peer=-0.70 |

### 1d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AU | +0.628 | Basic Materials | join=+0.99; gen1d=+0.07; news=+0.80; ev=news_judge; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.64 |
| DVN | +0.616 | Energy | join=+0.99; gen1d=+0.07; news=+0.83; ev=hormuz_energy_risk; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.41 |
| APA | +0.567 | Energy | join=+0.97; gen1d=+0.07; news=+0.83; ev=hormuz_energy_risk; ab=+0.55; LEAD,peers↑,ind↑; peer=+0.49 |
| COP | +0.565 | Energy | join=+0.99; gen1d=+0.07; news=+0.83; ev=hormuz_energy_risk; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.15 |
| TEM | +0.542 | Healthcare | join=+0.78; gen1d=+0.47; ab=+0.55; LEAD,peers↑,ind↓; peer=+0.96; rebound_floor |
| TRGP | +0.520 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.92 |
| DHR | +0.493 | Healthcare | join=+0.71; gen1d=+0.07; ab=+0.64; LEAD,peers↓,ind↑; peer=+0.82; rebound_floor |
| HL | +0.486 | Basic Materials | join=+0.99; gen1d=+0.47; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.70 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ELF | +0.523 | Consumer Defensive | join=+0.87; gen1d=+0.47; ab=+0.55; LEAD,peers↓,ind↓; peer=+0.81; rebound_floor |
| GRAL | +0.509 | Healthcare | join=+0.67; gen1d=+0.47; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.76; rebound_floor |
| AUPH | +0.504 | Healthcare | join=+0.93; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.76 |
| WRBY | +0.485 | Healthcare | join=+0.90; gen1d=+0.47; ab=+0.76; LEAD,peers↓,ind↑; peer=+0.75 |
| MEOH | +0.485 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.85 |
| HCC | +0.483 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.78 |
| SM | +0.480 | Energy | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.77 |
| TALO | +0.474 | Energy | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.80 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MGTX | +0.518 | Healthcare | join=+0.94; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.87 |
| CYPH | +0.485 | Healthcare | gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+1.00; rebound_floor |
| INO | +0.461 | Healthcare | join=+0.28; gen1d=+0.47; ab=+0.76; LEAD,peers↑,ind↑; peer=+1.00 |
| SLQT | +0.457 | Financial | join=+0.45; gen1d=+0.47; ab=+0.70; LEAD,peers↓,ind↑; peer=+0.95 |
| HCSG | +0.454 | Healthcare | join=+0.83; gen1d=+0.23; ab=+0.70; LEAD,peers↓,ind↑; peer=+0.80 |
| OBE | +0.453 | Energy | join=+0.88; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.76 |
| KOS | +0.447 | Energy | join=+0.97; gen1d=+0.07; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.83 |
| VET | +0.438 | Energy | join=+0.97; gen1d=+0.07; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.70 |

## 3d — BUY (top 15)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AU | +0.597 | Basic Materials | join=+0.99; gen1d=+0.07; news=+0.80; ev=news_judge; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.64 |
| DVN | +0.583 | Energy | join=+0.99; gen1d=+0.07; news=+0.83; ev=hormuz_energy_risk; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.41 |
| TRGP | +0.562 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.92 |
| MGTX | +0.546 | Healthcare | join=+0.94; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.87 |
| TEM | +0.541 | Healthcare | join=+0.78; gen1d=+0.47; ab=+0.55; LEAD,peers↑,ind↓; peer=+0.96; rebound_floor |
| COP | +0.532 | Energy | join=+0.99; gen1d=+0.07; news=+0.83; ev=hormuz_energy_risk; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.15 |
| APA | +0.530 | Energy | join=+0.97; gen1d=+0.07; news=+0.83; ev=hormuz_energy_risk; ab=+0.55; LEAD,peers↑,ind↑; peer=+0.49 |
| MEOH | +0.526 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.85 |
| ELF | +0.526 | Consumer Defensive | join=+0.87; gen1d=+0.47; ab=+0.55; LEAD,peers↓,ind↓; peer=+0.81; rebound_floor |
| HCC | +0.525 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.78 |
| DHR | +0.522 | Healthcare | join=+0.71; gen1d=+0.07; ab=+0.64; LEAD,peers↓,ind↑; peer=+0.82; rebound_floor |
| AUPH | +0.512 | Healthcare | join=+0.93; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.76 |
| DOW | +0.496 | Basic Materials | join=+0.96; gen1d=+0.07; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.79 |
| EIX | +0.480 | Utilities | join=+0.88; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.64 |
| LAUR | +0.464 | Consumer Defensive | join=+0.92; gen1d=+0.07; ab=+0.55; LEAD,peers↓,ind↓; peer=+0.86 |

## 3d — SELL / avoid (bottom 15)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| EOSE | -0.530 | Industrials | join=-0.98; gen1d=+0.47; ab=-0.70; LAG,peers↓,ind↓; peer=-0.95 |
| AMPG | -0.514 | Technology | join=-0.93; gen1d=+0.07; ab=-0.64; LAG,peers↓,ind↓; peer=-1.00 |
| JBLU | -0.496 | Industrials | join=-0.98; gen1d=+0.47; ab=-0.70; LAG,peers↓,ind↓; peer=-0.79 |
| GETY | -0.485 | Communication Services | join=-0.76; gen1d=+0.47; ab=-0.70; LAG,peers↓,ind↓; peer=-0.90 |
| SSTK | -0.483 | Communication Services | join=-0.84; gen1d=+0.23; ab=-0.76; LAG,peers↓,ind↓; peer=-0.75 |
| FLNC | -0.478 | Utilities | join=-0.59; gen1d=+0.47; ab=-0.76; LAG,peers↓,ind↓; peer=-0.93 |
| BKSY | -0.476 | Industrials | join=-0.99; gen1d=+0.47; ab=-0.55; LAG,peers↓,ind↓; peer=-0.87 |
| OI | -0.475 | Consumer Cyclical | join=-0.97; gen1d=+0.07; ab=-0.70; LAG,peers↓,ind↓; peer=-0.68 |
| BYRN | -0.475 | Industrials | join=-0.84; gen1d=+0.47; ab=-0.64; LAG,peers↓,ind↓; peer=-0.88 |
| BIDU | -0.472 | Communication Services | join=-0.95; gen1d=+0.07; ab=-0.55; LAG,peers↓,ind↓; peer=-0.87 |
| TRIP | -0.471 | Consumer Cyclical | join=-0.94; gen1d=+0.23; ab=-0.64; LAG,peers↓,ind↓; peer=-0.77 |
| ATOM | -0.469 | Technology | join=-0.79; gen1d=+0.47; ab=-0.64; LAG,peers↓,ind↓; peer=-0.89 |
| HYLN | -0.466 | Industrials | join=-0.94; gen1d=+0.47; ab=-0.46; LAG,peers↓,ind↓; peer=-0.98 |
| RXT | -0.466 | Technology | join=-0.92; gen1d=+0.47; ab=-0.46; LAG,peers↓,ind↓; peer=-0.99 |
| TDTH | -0.465 | Technology | join=-0.74; gen1d=+0.47; ab=-0.64; LAG,peers↓,ind↓; peer=-0.91 |

### 3d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AU | +0.597 | Basic Materials | join=+0.99; gen1d=+0.07; news=+0.80; ev=news_judge; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.64 |
| DVN | +0.583 | Energy | join=+0.99; gen1d=+0.07; news=+0.83; ev=hormuz_energy_risk; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.41 |
| TRGP | +0.562 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.92 |
| TEM | +0.541 | Healthcare | join=+0.78; gen1d=+0.47; ab=+0.55; LEAD,peers↑,ind↓; peer=+0.96; rebound_floor |
| COP | +0.532 | Energy | join=+0.99; gen1d=+0.07; news=+0.83; ev=hormuz_energy_risk; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.15 |
| APA | +0.530 | Energy | join=+0.97; gen1d=+0.07; news=+0.83; ev=hormuz_energy_risk; ab=+0.55; LEAD,peers↑,ind↑; peer=+0.49 |
| DHR | +0.522 | Healthcare | join=+0.71; gen1d=+0.07; ab=+0.64; LEAD,peers↓,ind↑; peer=+0.82; rebound_floor |
| CORT | +0.499 | Healthcare | join=+0.94; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.69 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MEOH | +0.526 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.85 |
| ELF | +0.526 | Consumer Defensive | join=+0.87; gen1d=+0.47; ab=+0.55; LEAD,peers↓,ind↓; peer=+0.81; rebound_floor |
| HCC | +0.525 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.78 |
| SM | +0.522 | Energy | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.77 |
| TALO | +0.516 | Energy | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.80 |
| AUPH | +0.512 | Healthcare | join=+0.93; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.76 |
| MTDR | +0.511 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.66 |
| GRAL | +0.505 | Healthcare | join=+0.67; gen1d=+0.47; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.76; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MGTX | +0.546 | Healthcare | join=+0.94; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.87 |
| CYPH | +0.490 | Healthcare | gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+1.00; rebound_floor |
| OBE | +0.490 | Energy | join=+0.88; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.76 |
| CLYM | +0.487 | Healthcare | join=+0.75; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.84 |
| KOS | +0.486 | Energy | join=+0.97; gen1d=+0.07; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.83 |
| VET | +0.478 | Energy | join=+0.97; gen1d=+0.07; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.70 |
| HCSG | +0.475 | Healthcare | join=+0.83; gen1d=+0.23; ab=+0.70; LEAD,peers↓,ind↑; peer=+0.80 |
| GPRE | +0.460 | Basic Materials | join=+0.99; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.41 |

## 1w — BUY (top 15)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TRGP | +0.599 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.92 |
| AU | +0.583 | Basic Materials | join=+0.99; gen1d=+0.07; news=+0.80; ev=news_judge; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.64 |
| MGTX | +0.581 | Healthcare | join=+0.94; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.87 |
| DVN | +0.569 | Energy | join=+0.99; gen1d=+0.07; news=+0.83; ev=hormuz_energy_risk; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.41 |
| TEM | +0.567 | Healthcare | join=+0.78; gen1d=+0.47; ab=+0.55; LEAD,peers↑,ind↓; peer=+0.96; rebound_floor |
| MEOH | +0.561 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.85 |
| HCC | +0.560 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.78 |
| SM | +0.558 | Energy | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.77 |
| ELF | +0.554 | Consumer Defensive | join=+0.87; gen1d=+0.47; ab=+0.55; LEAD,peers↓,ind↓; peer=+0.81; rebound_floor |
| TALO | +0.551 | Energy | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.80 |
| DHR | +0.549 | Healthcare | join=+0.71; gen1d=+0.07; ab=+0.64; LEAD,peers↓,ind↑; peer=+0.82; rebound_floor |
| AUPH | +0.547 | Healthcare | join=+0.93; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.76 |
| HL | +0.530 | Basic Materials | join=+0.99; gen1d=+0.47; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.70 |
| EIX | +0.513 | Utilities | join=+0.88; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.64 |
| WTW | +0.499 | Financial | join=+0.91; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.14; rebound_floor |

## 1w — SELL / avoid (bottom 15)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| EOSE | -0.564 | Industrials | join=-0.98; gen1d=+0.47; ab=-0.70; LAG,peers↓,ind↓; peer=-0.95 |
| AMPG | -0.546 | Technology | join=-0.93; gen1d=+0.07; ab=-0.64; LAG,peers↓,ind↓; peer=-1.00 |
| JBLU | -0.530 | Industrials | join=-0.98; gen1d=+0.47; ab=-0.70; LAG,peers↓,ind↓; peer=-0.79 |
| SSTK | -0.515 | Communication Services | join=-0.84; gen1d=+0.23; ab=-0.76; LAG,peers↓,ind↓; peer=-0.75 |
| GETY | -0.514 | Communication Services | join=-0.76; gen1d=+0.47; ab=-0.70; LAG,peers↓,ind↓; peer=-0.90 |
| OI | -0.508 | Consumer Cyclical | join=-0.97; gen1d=+0.07; ab=-0.70; LAG,peers↓,ind↓; peer=-0.68 |
| BKSY | -0.507 | Industrials | join=-0.99; gen1d=+0.47; ab=-0.55; LAG,peers↓,ind↓; peer=-0.87 |
| FLNC | -0.505 | Utilities | join=-0.59; gen1d=+0.47; ab=-0.76; LAG,peers↓,ind↓; peer=-0.93 |
| BYRN | -0.504 | Industrials | join=-0.84; gen1d=+0.47; ab=-0.64; LAG,peers↓,ind↓; peer=-0.88 |
| TRIP | -0.502 | Consumer Cyclical | join=-0.94; gen1d=+0.23; ab=-0.64; LAG,peers↓,ind↓; peer=-0.77 |
| BIDU | -0.502 | Communication Services | join=-0.95; gen1d=+0.07; ab=-0.55; LAG,peers↓,ind↓; peer=-0.87 |
| ATOM | -0.498 | Technology | join=-0.79; gen1d=+0.47; ab=-0.64; LAG,peers↓,ind↓; peer=-0.89 |
| HYLN | -0.494 | Industrials | join=-0.94; gen1d=+0.47; ab=-0.46; LAG,peers↓,ind↓; peer=-0.98 |
| BBAI | -0.494 | Technology | join=-0.99; gen1d=+0.47; ab=-0.64; LAG,peers↓,ind↓; peer=-0.69 |
| RXT | -0.494 | Technology | join=-0.92; gen1d=+0.47; ab=-0.46; LAG,peers↓,ind↓; peer=-0.99 |

### 1w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TRGP | +0.599 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.92 |
| AU | +0.583 | Basic Materials | join=+0.99; gen1d=+0.07; news=+0.80; ev=news_judge; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.64 |
| DVN | +0.569 | Energy | join=+0.99; gen1d=+0.07; news=+0.83; ev=hormuz_energy_risk; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.41 |
| TEM | +0.567 | Healthcare | join=+0.78; gen1d=+0.47; ab=+0.55; LEAD,peers↑,ind↓; peer=+0.96; rebound_floor |
| DHR | +0.549 | Healthcare | join=+0.71; gen1d=+0.07; ab=+0.64; LEAD,peers↓,ind↑; peer=+0.82; rebound_floor |
| CORT | +0.534 | Healthcare | join=+0.94; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.69 |
| HL | +0.530 | Basic Materials | join=+0.99; gen1d=+0.47; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.70 |
| PR | +0.530 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.57 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MEOH | +0.561 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.85 |
| HCC | +0.560 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.78 |
| SM | +0.558 | Energy | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.77 |
| ELF | +0.554 | Consumer Defensive | join=+0.87; gen1d=+0.47; ab=+0.55; LEAD,peers↓,ind↓; peer=+0.81; rebound_floor |
| TALO | +0.551 | Energy | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.80 |
| MTDR | +0.548 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.66 |
| AUPH | +0.547 | Healthcare | join=+0.93; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.76 |
| KNTK | +0.537 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.61 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MGTX | +0.581 | Healthcare | join=+0.94; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.87 |
| OBE | +0.523 | Energy | join=+0.88; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.76 |
| KOS | +0.518 | Energy | join=+0.97; gen1d=+0.07; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.83 |
| CLYM | +0.518 | Healthcare | join=+0.75; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.84 |
| VET | +0.512 | Energy | join=+0.97; gen1d=+0.07; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.70 |
| CYPH | +0.507 | Healthcare | gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+1.00; rebound_floor |
| HCSG | +0.506 | Healthcare | join=+0.83; gen1d=+0.23; ab=+0.70; LEAD,peers↓,ind↑; peer=+0.80 |
| GPRE | +0.497 | Basic Materials | join=+0.99; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.41 |

## 2w — BUY (top 15)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TRGP | +0.623 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.92 |
| MGTX | +0.617 | Healthcare | join=+0.94; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.87 |
| TEM | +0.617 | Healthcare | join=+0.78; gen1d=+0.47; ab=+0.55; LEAD,peers↑,ind↓; peer=+0.96; rebound_floor |
| ELF | +0.606 | Consumer Defensive | join=+0.87; gen1d=+0.47; ab=+0.55; LEAD,peers↓,ind↓; peer=+0.81; rebound_floor |
| AUPH | +0.599 | Healthcare | join=+0.93; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.76 |
| MEOH | +0.586 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.85 |
| HCC | +0.585 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.78 |
| HL | +0.584 | Basic Materials | join=+0.99; gen1d=+0.47; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.70 |
| SM | +0.583 | Energy | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.77 |
| GRAL | +0.578 | Healthcare | join=+0.67; gen1d=+0.47; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.76; rebound_floor |
| TALO | +0.576 | Energy | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.80 |
| AU | +0.576 | Basic Materials | join=+0.99; gen1d=+0.07; news=+0.80; ev=news_judge; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.64 |
| MTDR | +0.573 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.66 |
| GSHD | +0.538 | Financial | join=+0.91; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.47 |
| EIX | +0.536 | Utilities | join=+0.88; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.64 |

## 2w — SELL / avoid (bottom 15)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AMPG | -0.559 | Technology | join=-0.93; gen1d=+0.07; ab=-0.64; LAG,peers↓,ind↓; peer=-1.00 |
| EOSE | -0.549 | Industrials | join=-0.98; gen1d=+0.47; ab=-0.70; LAG,peers↓,ind↓; peer=-0.95 |
| OI | -0.523 | Consumer Cyclical | join=-0.97; gen1d=+0.07; ab=-0.70; LAG,peers↓,ind↓; peer=-0.68 |
| BIDU | -0.516 | Communication Services | join=-0.95; gen1d=+0.07; ab=-0.55; LAG,peers↓,ind↓; peer=-0.87 |
| JBLU | -0.515 | Industrials | join=-0.98; gen1d=+0.47; ab=-0.70; LAG,peers↓,ind↓; peer=-0.79 |
| SSTK | -0.515 | Communication Services | join=-0.84; gen1d=+0.23; ab=-0.76; LAG,peers↓,ind↓; peer=-0.75 |
| TRIP | -0.504 | Consumer Cyclical | join=-0.94; gen1d=+0.23; ab=-0.64; LAG,peers↓,ind↓; peer=-0.77 |
| VNET | -0.499 | Technology | join=-0.94; gen1d=+0.07; ab=-0.55; LAG,peers↓,ind↓; peer=-0.80 |
| GETY | -0.495 | Communication Services | join=-0.76; gen1d=+0.47; ab=-0.70; LAG,peers↓,ind↓; peer=-0.90 |
| BKSY | -0.492 | Industrials | join=-0.99; gen1d=+0.47; ab=-0.55; LAG,peers↓,ind↓; peer=-0.87 |
| TTD | -0.491 | Communication Services | join=-0.95; gen1d=+0.23; ab=-0.64; LAG,peers↓,ind↓; peer=-0.70 |
| LCID | -0.489 | Consumer Cyclical | join=-0.98; gen1d=+0.23; ab=-0.46; LAG,peers↑,ind↓; peer=-0.91 |
| BYRN | -0.487 | Industrials | join=-0.84; gen1d=+0.47; ab=-0.64; LAG,peers↓,ind↓; peer=-0.88 |
| THRY | -0.483 | Technology | join=-0.81; gen1d=+0.23; ab=-0.64; LAG,peers↓,ind↓; peer=-0.80 |
| FLNC | -0.483 | Utilities | join=-0.59; gen1d=+0.47; ab=-0.76; LAG,peers↓,ind↓; peer=-0.93 |

### 2w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TRGP | +0.623 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.92 |
| TEM | +0.617 | Healthcare | join=+0.78; gen1d=+0.47; ab=+0.55; LEAD,peers↑,ind↓; peer=+0.96; rebound_floor |
| HL | +0.584 | Basic Materials | join=+0.99; gen1d=+0.47; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.70 |
| AU | +0.576 | Basic Materials | join=+0.99; gen1d=+0.07; news=+0.80; ev=news_judge; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.64 |
| DHR | +0.568 | Healthcare | join=+0.71; gen1d=+0.07; ab=+0.64; LEAD,peers↓,ind↑; peer=+0.82; rebound_floor |
| ECL | +0.562 | Basic Materials | join=+0.99; gen1d=+0.23; ab=+0.70; LEAD,peers↑,ind↓; peer=+0.35; rebound_floor |
| DVN | +0.560 | Energy | join=+0.99; gen1d=+0.07; news=+0.83; ev=hormuz_energy_risk; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.41 |
| CORT | +0.558 | Healthcare | join=+0.94; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.69 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ELF | +0.606 | Consumer Defensive | join=+0.87; gen1d=+0.47; ab=+0.55; LEAD,peers↓,ind↓; peer=+0.81; rebound_floor |
| AUPH | +0.599 | Healthcare | join=+0.93; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.76 |
| MEOH | +0.586 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.85 |
| HCC | +0.585 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.78 |
| SM | +0.583 | Energy | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.77 |
| GRAL | +0.578 | Healthcare | join=+0.67; gen1d=+0.47; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.76; rebound_floor |
| WRBY | +0.577 | Healthcare | join=+0.90; gen1d=+0.47; ab=+0.76; LEAD,peers↓,ind↑; peer=+0.75 |
| TALO | +0.576 | Energy | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.80 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MGTX | +0.617 | Healthcare | join=+0.94; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.87 |
| OBE | +0.546 | Energy | join=+0.88; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.76 |
| KOS | +0.543 | Energy | join=+0.97; gen1d=+0.07; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.83 |
| HCSG | +0.540 | Healthcare | join=+0.83; gen1d=+0.23; ab=+0.70; LEAD,peers↓,ind↑; peer=+0.80 |
| CLYM | +0.538 | Healthcare | join=+0.75; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.84 |
| VET | +0.536 | Energy | join=+0.97; gen1d=+0.07; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.70 |
| GPRE | +0.534 | Basic Materials | join=+0.99; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.41 |
| ZVRA | +0.532 | Healthcare | join=+0.94; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.45 |

## 1m — BUY (top 15)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TRGP | +0.660 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.92 |
| MGTX | +0.653 | Healthcare | join=+0.94; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.87 |
| TEM | +0.643 | Healthcare | join=+0.78; gen1d=+0.47; ab=+0.55; LEAD,peers↑,ind↓; peer=+0.96; rebound_floor |
| AUPH | +0.634 | Healthcare | join=+0.93; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.76 |
| ELF | +0.634 | Consumer Defensive | join=+0.87; gen1d=+0.47; ab=+0.55; LEAD,peers↓,ind↓; peer=+0.81; rebound_floor |
| HCC | +0.621 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.78 |
| MEOH | +0.621 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.85 |
| HL | +0.619 | Basic Materials | join=+0.99; gen1d=+0.47; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.70 |
| SM | +0.619 | Energy | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.77 |
| TALO | +0.611 | Energy | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.80 |
| WRBY | +0.610 | Healthcare | join=+0.90; gen1d=+0.47; ab=+0.76; LEAD,peers↓,ind↑; peer=+0.75 |
| MTDR | +0.609 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.66 |
| ECL | +0.595 | Basic Materials | join=+0.99; gen1d=+0.23; ab=+0.70; LEAD,peers↑,ind↓; peer=+0.35; rebound_floor |
| GSHD | +0.573 | Financial | join=+0.91; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.47 |
| EIX | +0.570 | Utilities | join=+0.88; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.64 |

## 1m — SELL / avoid (bottom 15)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AMPG | -0.590 | Technology | join=-0.93; gen1d=+0.07; ab=-0.64; LAG,peers↓,ind↓; peer=-1.00 |
| EOSE | -0.583 | Industrials | join=-0.98; gen1d=+0.47; ab=-0.70; LAG,peers↓,ind↓; peer=-0.95 |
| OI | -0.556 | Consumer Cyclical | join=-0.97; gen1d=+0.07; ab=-0.70; LAG,peers↓,ind↓; peer=-0.68 |
| JBLU | -0.549 | Industrials | join=-0.98; gen1d=+0.47; ab=-0.70; LAG,peers↓,ind↓; peer=-0.79 |
| SSTK | -0.547 | Communication Services | join=-0.84; gen1d=+0.23; ab=-0.76; LAG,peers↓,ind↓; peer=-0.75 |
| BIDU | -0.546 | Communication Services | join=-0.95; gen1d=+0.07; ab=-0.55; LAG,peers↓,ind↓; peer=-0.87 |
| TRIP | -0.536 | Consumer Cyclical | join=-0.94; gen1d=+0.23; ab=-0.64; LAG,peers↓,ind↓; peer=-0.77 |
| VNET | -0.529 | Technology | join=-0.94; gen1d=+0.07; ab=-0.55; LAG,peers↓,ind↓; peer=-0.80 |
| GETY | -0.524 | Communication Services | join=-0.76; gen1d=+0.47; ab=-0.70; LAG,peers↓,ind↓; peer=-0.90 |
| BKSY | -0.523 | Industrials | join=-0.99; gen1d=+0.47; ab=-0.55; LAG,peers↓,ind↓; peer=-0.87 |
| TTD | -0.523 | Communication Services | join=-0.95; gen1d=+0.23; ab=-0.64; LAG,peers↓,ind↓; peer=-0.70 |
| LCID | -0.518 | Consumer Cyclical | join=-0.98; gen1d=+0.23; ab=-0.46; LAG,peers↑,ind↓; peer=-0.91 |
| BYRN | -0.516 | Industrials | join=-0.84; gen1d=+0.47; ab=-0.64; LAG,peers↓,ind↓; peer=-0.88 |
| THRY | -0.512 | Technology | join=-0.81; gen1d=+0.23; ab=-0.64; LAG,peers↓,ind↓; peer=-0.80 |
| BBAI | -0.512 | Technology | join=-0.99; gen1d=+0.47; ab=-0.64; LAG,peers↓,ind↓; peer=-0.69 |

### 1m — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TRGP | +0.660 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.92 |
| TEM | +0.643 | Healthcare | join=+0.78; gen1d=+0.47; ab=+0.55; LEAD,peers↑,ind↓; peer=+0.96; rebound_floor |
| HL | +0.619 | Basic Materials | join=+0.99; gen1d=+0.47; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.70 |
| ECL | +0.595 | Basic Materials | join=+0.99; gen1d=+0.23; ab=+0.70; LEAD,peers↑,ind↓; peer=+0.35; rebound_floor |
| DHR | +0.595 | Healthcare | join=+0.71; gen1d=+0.07; ab=+0.64; LEAD,peers↓,ind↑; peer=+0.82; rebound_floor |
| CORT | +0.593 | Healthcare | join=+0.94; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.69 |
| PR | +0.592 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.57 |
| DOW | +0.587 | Basic Materials | join=+0.96; gen1d=+0.07; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.79 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AUPH | +0.634 | Healthcare | join=+0.93; gen1d=+0.47; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.76 |
| ELF | +0.634 | Consumer Defensive | join=+0.87; gen1d=+0.47; ab=+0.55; LEAD,peers↓,ind↓; peer=+0.81; rebound_floor |
| HCC | +0.621 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.78 |
| MEOH | +0.621 | Basic Materials | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.85 |
| SM | +0.619 | Energy | join=+0.99; gen1d=+0.07; ab=+0.81; LEAD,peers↑,ind↑; peer=+0.77 |
| TALO | +0.611 | Energy | join=+0.99; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.80 |
| WRBY | +0.610 | Healthcare | join=+0.90; gen1d=+0.47; ab=+0.76; LEAD,peers↓,ind↑; peer=+0.75 |
| MTDR | +0.609 | Energy | join=+0.99; gen1d=+0.07; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.66 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MGTX | +0.653 | Healthcare | join=+0.94; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.87 |
| OBE | +0.578 | Energy | join=+0.88; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.76 |
| KOS | +0.575 | Energy | join=+0.97; gen1d=+0.07; ab=+0.64; LEAD,peers↑,ind↑; peer=+0.83 |
| GPRE | +0.570 | Basic Materials | join=+0.99; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.41 |
| HCSG | +0.570 | Healthcare | join=+0.83; gen1d=+0.23; ab=+0.70; LEAD,peers↓,ind↑; peer=+0.80 |
| VET | +0.570 | Energy | join=+0.97; gen1d=+0.07; ab=+0.70; LEAD,peers↑,ind↑; peer=+0.70 |
| CLYM | +0.568 | Healthcare | join=+0.75; gen1d=+0.07; ab=+0.76; LEAD,peers↑,ind↑; peer=+0.84 |
| ZVRA | +0.568 | Healthcare | join=+0.94; gen1d=+0.23; ab=+0.85; LEAD,peers↑,ind↑; peer=+0.45 |

## Read

- **1d** news + AB + peer; **1m** AB + peer + join + same-day sector.
- Universe gated: Market Cap ≥ $80M and Average Volume ≥ 500k shares (Finviz units).
- AB score (checklist + P01–P04 peer/industry/sector context) is a first-class rank, not a footnote.
- Peer RS (`rs_week` vs correlated basket) breaks ties inside a sector so the book is not 8 clones of XLE.
- Diversify: max 4 names per sector, 3 per industry. Persistence penalty if already on yesterday's list without fresh evidence.
- Same-day sector/general only — stale Monday calls are not reused on Wednesday.
- `rebound_floor` is a small boost from today's ticker checklist (tape at own-history low).
- Predictor bias is scaled by graded 1d hit rate. Weak topics move scores less.
- Backtest: `python -m src.stock_book_backtest` (or Stock Book Backtest action).

CSV: `data/stock_book/2026-08-20_stock_book.csv`
JSON: `data/stock_book/2026-08-20_stock_book.json`
