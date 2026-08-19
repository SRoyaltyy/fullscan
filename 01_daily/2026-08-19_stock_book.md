# Stock book — **2026-08-19** (1d / 3d / 1w / 2w / 1m)

Generated: 2026-08-19T13:02:37.452910-04:00

Layers: join (labels×weather) + sector predict + general regime + news actions.

## Regime snapshot

- **General bias:** -0.55
- **Weather risk:** off
- **News tickers:** 5
- **Universe (after liquidity):** 2702
- **Gates:** mcap ≥ $80.0M, avg vol ≥ 500.0k
- **Rebound floor tags:** 87

### Sector bias

| Sector | bias |
|--------|------|
| Technology | -0.75 |
| Healthcare | +0.70 |
| Consumer Cyclical | -0.65 |
| Energy | +0.65 |
| Consumer Defensive | +0.60 |
| Real Estate | -0.60 |
| Utilities | +0.60 |
| Basic Materials | -0.55 |
| Communication Services | -0.30 |
| Financial | -0.28 |
| Industrials | +0.28 |

### Learning gate (graded accuracy → how much each predictor is trusted)

| Topic | hit rate | graded runs | weight applied |
|-------|----------|-------------|----------------|
| general | 56% | 16 | ×1.00 |
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

| Horizon | join | sector | general | news |
|---------|------|--------|---------|------|
| 1d | 0.35 | 0.15 | 0.10 | 0.40 |
| 3d | 0.40 | 0.25 | 0.10 | 0.25 |
| 1w | 0.45 | 0.30 | 0.10 | 0.15 |
| 2w | 0.48 | 0.35 | 0.10 | 0.07 |
| 1m | 0.50 | 0.40 | 0.10 | 0.00 |

## 1d — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| NEE | +0.382 | Utilities | join=+0.17; sector=+0.60; gen=-0.08; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| STE | +0.349 | Healthcare | join=+0.35; sector=+0.70; gen=-0.28; rebound_floor |
| DHR | +0.332 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| SYK | +0.332 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| UTHR | +0.310 | Healthcare | join=+0.18; sector=+0.70; gen=-0.08; rebound_floor |
| CEG | +0.305 | Utilities | sector=+0.60; gen=-0.28; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| EW | +0.285 | Healthcare | join=+0.17; sector=+0.70; gen=-0.28; rebound_floor |
| FAST | +0.269 | Industrials | join=+0.24; sector=+0.28; gen=-0.08; rebound_floor |
| TMO | +0.257 | Healthcare | sector=+0.70; gen=-0.28; rebound_floor |
| COO | +0.257 | Healthcare | sector=+0.70; gen=-0.28; rebound_floor |
| VST | +0.248 | Utilities | sector=+0.60; gen=-0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| MDT | +0.246 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |
| MUR | +0.239 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| WTW | +0.222 | Financial | join=+0.35; sector=-0.28; gen=-0.08; rebound_floor |
| GGG | +0.222 | Industrials | join=+0.17; sector=+0.28; gen=-0.28; rebound_floor |
| RMD | +0.209 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| ES | +0.204 | Utilities | join=+0.35; sector=+0.60; gen=-0.08 |
| IQV | +0.198 | Healthcare | sector=+0.70; gen=-0.28; rebound_floor |
| ITW | +0.195 | Industrials | sector=+0.28; gen=-0.28; rebound_floor |
| AOS | +0.193 | Industrials | sector=+0.28; gen=-0.28; rebound_floor |
| NRG | +0.192 | Utilities | join=-0.32; sector=+0.60; gen=-0.28; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| ZBH | +0.190 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| ELV | +0.190 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| HLN | +0.190 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| MLYS | +0.184 | Healthcare | join=-0.18; sector=+0.70; gen=-0.08; rebound_floor |

## 1d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ADTN | -0.421 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| WOLF | -0.421 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| VERI | -0.421 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| CSIQ | -0.420 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| GPRO | -0.420 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| SEDG | -0.420 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| CNDT | -0.406 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| CRNC | -0.406 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| AIOT | -0.406 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| CIFR | -0.406 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| KEEL | -0.406 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| LVWR | -0.406 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| CWH | -0.406 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| TDUP | -0.406 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| CHPT | -0.405 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| EVGO | -0.405 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| DOMO | -0.403 | Technology | join=-0.67; sector=-0.75; gen=-0.55 |
| SECZ | -0.403 | Technology | join=-0.67; sector=-0.75; gen=-0.55 |
| BTDR | -0.403 | Technology | join=-0.67; sector=-0.75; gen=-0.55 |
| QMCO | -0.400 | Technology | join=-0.66; sector=-0.75; gen=-0.55 |
| CVGI | -0.391 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.55 |
| VENU | -0.391 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.55 |
| NXH | -0.391 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.55 |
| ASPI | -0.391 | Basic Materials | join=-0.72; sector=-0.55; gen=-0.55 |
| METC | -0.391 | Basic Materials | join=-0.72; sector=-0.55; gen=-0.55 |

### 1d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| NEE | +0.382 | Utilities | join=+0.17; sector=+0.60; gen=-0.08; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| STE | +0.349 | Healthcare | join=+0.35; sector=+0.70; gen=-0.28; rebound_floor |
| SYK | +0.332 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| DHR | +0.332 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| UTHR | +0.310 | Healthcare | join=+0.18; sector=+0.70; gen=-0.08; rebound_floor |
| CEG | +0.305 | Utilities | sector=+0.60; gen=-0.28; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| EW | +0.285 | Healthcare | join=+0.17; sector=+0.70; gen=-0.28; rebound_floor |
| FAST | +0.269 | Industrials | join=+0.24; sector=+0.28; gen=-0.08; rebound_floor |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MUR | +0.239 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| AOS | +0.193 | Industrials | sector=+0.28; gen=-0.28; rebound_floor |
| MLYS | +0.184 | Healthcare | join=-0.18; sector=+0.70; gen=-0.08; rebound_floor |
| CAI | +0.170 | Healthcare | join=-0.17; sector=+0.70; gen=-0.28; rebound_floor |
| TRMD | +0.158 | Energy | join=+0.20; sector=+0.65; gen=-0.08 |
| OLLI | +0.146 | Consumer Defensive | join=-0.24; sector=+0.60; gen=-0.08; rebound_floor |
| TAL | +0.145 | Consumer Defensive | join=+0.18; sector=+0.60; gen=-0.08 |
| XENE | +0.135 | Healthcare | sector=+0.70; gen=-0.08 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| OBE | +0.124 | Energy | sector=+0.65; gen=-0.08 |
| CYPH | +0.098 | Healthcare | join=-0.43; sector=+0.70; gen=-0.08; rebound_floor |
| TBPH | +0.097 | Healthcare | sector=+0.70; gen=-0.08 |
| AVXL | +0.095 | Healthcare | join=-0.38; sector=+0.70; gen=-0.28; rebound_floor |
| SEER | +0.087 | Healthcare | join=-0.32; sector=+0.70; gen=-0.55; rebound_floor |
| GERN | +0.085 | Healthcare | join=-0.46; sector=+0.70; gen=-0.08; rebound_floor |
| NOMD | +0.082 | Consumer Defensive | sector=+0.60; gen=-0.08 |
| INMD | +0.082 | Healthcare | sector=+0.70; gen=-0.55 |

## 3d — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.414 | Healthcare | join=+0.35; sector=+0.70; gen=-0.28; rebound_floor |
| SYK | +0.390 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| DHR | +0.390 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| UTHR | +0.364 | Healthcare | join=+0.18; sector=+0.70; gen=-0.08; rebound_floor |
| NEE | +0.360 | Utilities | join=+0.17; sector=+0.60; gen=-0.08; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| EW | +0.341 | Healthcare | join=+0.17; sector=+0.70; gen=-0.28; rebound_floor |
| MDT | +0.313 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |
| COO | +0.308 | Healthcare | sector=+0.70; gen=-0.28; rebound_floor |
| TMO | +0.308 | Healthcare | sector=+0.70; gen=-0.28; rebound_floor |
| MUR | +0.292 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| ES | +0.282 | Utilities | join=+0.35; sector=+0.60; gen=-0.08 |
| WTW | +0.282 | Financial | join=+0.35; sector=-0.28; gen=-0.08; rebound_floor |
| CEG | +0.276 | Utilities | sector=+0.60; gen=-0.28; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| RMD | +0.271 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| HLN | +0.249 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| ZBH | +0.249 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| OXY | +0.249 | Energy | join=+0.27; sector=+0.65; gen=-0.08 |
| EVRG | +0.249 | Utilities | join=+0.27; sector=+0.60; gen=-0.08 |
| ELV | +0.249 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| PCG | +0.249 | Utilities | join=+0.27; sector=+0.60; gen=-0.08 |
| PM | +0.247 | Consumer Defensive | join=+0.29; sector=+0.60; gen=-0.08 |
| IQV | +0.242 | Healthcare | sector=+0.70; gen=-0.28; rebound_floor |
| TS | +0.242 | Energy | join=+0.29; sector=+0.65; gen=-0.28 |
| CI | +0.240 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08 |
| ED | +0.240 | Utilities | join=+0.24; sector=+0.60; gen=-0.08 |

## 3d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| LVWR | -0.490 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| CWH | -0.490 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| TDUP | -0.490 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| EVGO | -0.488 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| CHPT | -0.488 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| WOLF | -0.477 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| VERI | -0.477 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| ADTN | -0.477 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| SEDG | -0.476 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| GPRO | -0.476 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| CSIQ | -0.476 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| CVGI | -0.473 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.55 |
| VENU | -0.473 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.55 |
| NXH | -0.473 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.55 |
| INVZ | -0.470 | Consumer Cyclical | join=-0.67; sector=-0.65; gen=-0.55 |
| AIIO | -0.466 | Consumer Cyclical | join=-0.66; sector=-0.65; gen=-0.55 |
| METC | -0.465 | Basic Materials | join=-0.72; sector=-0.55; gen=-0.55 |
| ASPI | -0.465 | Basic Materials | join=-0.72; sector=-0.55; gen=-0.55 |
| CRNC | -0.460 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| OPEN | -0.460 | Real Estate | join=-0.68; sector=-0.60; gen=-0.55 |
| AIOT | -0.460 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| CNDT | -0.460 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| RC | -0.460 | Real Estate | join=-0.68; sector=-0.60; gen=-0.55 |
| CIFR | -0.460 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| KEEL | -0.460 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |

### 3d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.414 | Healthcare | join=+0.35; sector=+0.70; gen=-0.28; rebound_floor |
| DHR | +0.390 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| SYK | +0.390 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| UTHR | +0.364 | Healthcare | join=+0.18; sector=+0.70; gen=-0.08; rebound_floor |
| NEE | +0.360 | Utilities | join=+0.17; sector=+0.60; gen=-0.08; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| EW | +0.341 | Healthcare | join=+0.17; sector=+0.70; gen=-0.28; rebound_floor |
| MDT | +0.313 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |
| TMO | +0.308 | Healthcare | sector=+0.70; gen=-0.28; rebound_floor |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MUR | +0.292 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| TRMD | +0.221 | Energy | join=+0.20; sector=+0.65; gen=-0.08 |
| MLYS | +0.221 | Healthcare | join=-0.18; sector=+0.70; gen=-0.08; rebound_floor |
| CAI | +0.209 | Healthcare | join=-0.17; sector=+0.70; gen=-0.28; rebound_floor |
| TAL | +0.202 | Consumer Defensive | join=+0.18; sector=+0.60; gen=-0.08 |
| XENE | +0.187 | Healthcare | sector=+0.70; gen=-0.08 |
| OLLI | +0.182 | Consumer Defensive | join=-0.24; sector=+0.60; gen=-0.08; rebound_floor |
| QGEN | +0.179 | Healthcare | sector=+0.70; gen=-0.08 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| OBE | +0.182 | Energy | sector=+0.65; gen=-0.08 |
| TBPH | +0.142 | Healthcare | sector=+0.70; gen=-0.08 |
| INMD | +0.136 | Healthcare | sector=+0.70; gen=-0.55 |
| NOMD | +0.130 | Consumer Defensive | sector=+0.60; gen=-0.08 |
| AVXL | +0.123 | Healthcare | join=-0.38; sector=+0.70; gen=-0.28; rebound_floor |
| CYPH | +0.122 | Healthcare | join=-0.43; sector=+0.70; gen=-0.08; rebound_floor |
| SEER | +0.121 | Healthcare | join=-0.32; sector=+0.70; gen=-0.55; rebound_floor |
| GERN | +0.108 | Healthcare | join=-0.46; sector=+0.70; gen=-0.08; rebound_floor |

## 1w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.464 | Healthcare | join=+0.35; sector=+0.70; gen=-0.28; rebound_floor |
| SYK | +0.433 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| DHR | +0.433 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| UTHR | +0.404 | Healthcare | join=+0.18; sector=+0.70; gen=-0.08; rebound_floor |
| EW | +0.382 | Healthcare | join=+0.17; sector=+0.70; gen=-0.28; rebound_floor |
| WTW | +0.375 | Financial | join=+0.35; sector=-0.28; gen=-0.08; rebound_floor |
| MDT | +0.365 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |
| TMO | +0.345 | Healthcare | sector=+0.70; gen=-0.28; rebound_floor |
| COO | +0.345 | Healthcare | sector=+0.70; gen=-0.28; rebound_floor |
| NEE | +0.323 | Utilities | join=+0.17; sector=+0.60; gen=-0.08; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| RMD | +0.318 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| ES | +0.315 | Utilities | join=+0.35; sector=+0.60; gen=-0.08 |
| ACGL | +0.310 | Financial | join=+0.54; sector=-0.28; gen=-0.08 |
| CB | +0.310 | Financial | join=+0.54; sector=-0.28; gen=-0.08 |
| MUR | +0.308 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| ELV | +0.293 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| ZBH | +0.293 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| HLN | +0.293 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| MRSH | +0.293 | Financial | join=+0.17; sector=-0.28; gen=-0.08; rebound_floor |
| HIG | +0.292 | Financial | join=+0.50; sector=-0.28; gen=-0.08 |
| PM | +0.289 | Consumer Defensive | join=+0.29; sector=+0.60; gen=-0.08 |
| VRTX | +0.283 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08 |
| REGN | +0.283 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08 |
| CVS | +0.283 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08 |
| CI | +0.283 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08 |

## 1w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TDUP | -0.536 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| LVWR | -0.536 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| CWH | -0.536 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| EVGO | -0.534 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| CHPT | -0.534 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| WOLF | -0.521 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| VERI | -0.521 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| ADTN | -0.521 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| SEDG | -0.519 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| GPRO | -0.519 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| CSIQ | -0.519 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| CVGI | -0.517 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.55 |
| VENU | -0.517 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.55 |
| NXH | -0.517 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.55 |
| INVZ | -0.513 | Consumer Cyclical | join=-0.67; sector=-0.65; gen=-0.55 |
| AIIO | -0.509 | Consumer Cyclical | join=-0.66; sector=-0.65; gen=-0.55 |
| KEEL | -0.502 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| CRNC | -0.502 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| CNDT | -0.502 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| CIFR | -0.502 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| AIOT | -0.502 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| BTDR | -0.498 | Technology | join=-0.67; sector=-0.75; gen=-0.55 |
| SECZ | -0.498 | Technology | join=-0.67; sector=-0.75; gen=-0.55 |
| DOMO | -0.498 | Technology | join=-0.67; sector=-0.75; gen=-0.55 |
| UA | -0.496 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.55 |

### 1w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.464 | Healthcare | join=+0.35; sector=+0.70; gen=-0.28; rebound_floor |
| DHR | +0.433 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| SYK | +0.433 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| UTHR | +0.404 | Healthcare | join=+0.18; sector=+0.70; gen=-0.08; rebound_floor |
| EW | +0.382 | Healthcare | join=+0.17; sector=+0.70; gen=-0.28; rebound_floor |
| WTW | +0.375 | Financial | join=+0.35; sector=-0.28; gen=-0.08; rebound_floor |
| MDT | +0.365 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |
| TMO | +0.345 | Healthcare | sector=+0.70; gen=-0.28; rebound_floor |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MUR | +0.308 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| TRMD | +0.247 | Energy | join=+0.20; sector=+0.65; gen=-0.08 |
| MLYS | +0.242 | Healthcare | join=-0.18; sector=+0.70; gen=-0.08; rebound_floor |
| TAL | +0.239 | Consumer Defensive | join=+0.18; sector=+0.60; gen=-0.08 |
| CAI | +0.233 | Healthcare | join=-0.17; sector=+0.70; gen=-0.28; rebound_floor |
| XENE | +0.223 | Healthcare | sector=+0.70; gen=-0.08 |
| QGEN | +0.214 | Healthcare | sector=+0.70; gen=-0.08 |
| MTDR | +0.199 | Energy | sector=+0.65; gen=-0.08 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| OBE | +0.203 | Energy | sector=+0.65; gen=-0.08 |
| INMD | +0.176 | Healthcare | sector=+0.70; gen=-0.55 |
| TBPH | +0.173 | Healthcare | sector=+0.70; gen=-0.08 |
| NOMD | +0.158 | Consumer Defensive | sector=+0.60; gen=-0.08 |
| SEER | +0.140 | Healthcare | join=-0.32; sector=+0.70; gen=-0.55; rebound_floor |
| AVXL | +0.137 | Healthcare | join=-0.38; sector=+0.70; gen=-0.28; rebound_floor |
| QTTB | +0.132 | Healthcare | sector=+0.70; gen=-0.08 |
| CYPH | +0.132 | Healthcare | join=-0.43; sector=+0.70; gen=-0.08; rebound_floor |

## 2w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.510 | Healthcare | join=+0.35; sector=+0.70; gen=-0.28; rebound_floor |
| DHR | +0.460 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| SYK | +0.460 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| UTHR | +0.429 | Healthcare | join=+0.18; sector=+0.70; gen=-0.08; rebound_floor |
| EW | +0.422 | Healthcare | join=+0.17; sector=+0.70; gen=-0.28; rebound_floor |
| WTW | +0.405 | Financial | join=+0.35; sector=-0.28; gen=-0.08; rebound_floor |
| MDT | +0.397 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |
| TMO | +0.382 | Healthcare | sector=+0.70; gen=-0.28; rebound_floor |
| COO | +0.382 | Healthcare | sector=+0.70; gen=-0.28; rebound_floor |
| ECL | +0.365 | Basic Materials | sector=-0.55; gen=-0.28; rebound_floor |
| FAST | +0.355 | Industrials | join=+0.24; sector=+0.28; gen=-0.08; rebound_floor |
| RMD | +0.347 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| CB | +0.345 | Financial | join=+0.54; sector=-0.28; gen=-0.08 |
| ACGL | +0.345 | Financial | join=+0.54; sector=-0.28; gen=-0.08 |
| LIN | +0.342 | Basic Materials | join=+0.35; sector=-0.55; gen=-0.08 |
| ES | +0.342 | Utilities | join=+0.35; sector=+0.60; gen=-0.08 |
| HIG | +0.326 | Financial | join=+0.50; sector=-0.28; gen=-0.08 |
| CRH | +0.325 | Basic Materials | sector=-0.55; gen=-0.28; rebound_floor |
| MUR | +0.325 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| ZBH | +0.320 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| HLN | +0.320 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| ELV | +0.320 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| MRSH | +0.317 | Financial | join=+0.17; sector=-0.28; gen=-0.08; rebound_floor |
| GGG | +0.317 | Industrials | join=+0.17; sector=+0.28; gen=-0.28; rebound_floor |
| TS | +0.315 | Energy | join=+0.29; sector=+0.65; gen=-0.28 |

## 2w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| GOGO | -0.348 | Communication Services | join=-0.72; sector=-0.30; gen=-0.28 |
| ADTN | -0.348 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| LVWR | -0.348 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| PLAY | -0.348 | Communication Services | join=-0.72; sector=-0.30; gen=-0.55 |
| KLC | -0.348 | Consumer Defensive | join=-0.72; sector=+0.60; gen=-0.55 |
| WOLF | -0.348 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| VERI | -0.348 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| TDUP | -0.348 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| CWH | -0.348 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| EVGO | -0.346 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| GPRO | -0.346 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| CSIQ | -0.346 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| CHPT | -0.346 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| SEDG | -0.346 | Technology | join=-0.72; sector=-0.75; gen=-0.55 |
| QMLS | -0.341 | Technology | join=-0.71; sector=-0.75; gen=-0.22 |
| GETY | -0.327 | Communication Services | join=-0.68; sector=-0.30; gen=-0.55 |
| OPEN | -0.327 | Real Estate | join=-0.68; sector=-0.60; gen=-0.55 |
| CRNC | -0.327 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| KEEL | -0.327 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| RC | -0.327 | Real Estate | join=-0.68; sector=-0.60; gen=-0.55 |
| VENU | -0.327 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.55 |
| ANGI | -0.327 | Communication Services | join=-0.68; sector=-0.30; gen=-0.55 |
| BRCC | -0.327 | Consumer Defensive | join=-0.68; sector=+0.60; gen=-0.28 |
| CNDT | -0.327 | Technology | join=-0.68; sector=-0.75; gen=-0.55 |
| THRY | -0.327 | Technology | join=-0.68; sector=-0.75; gen=-0.28 |

### 2w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.510 | Healthcare | join=+0.35; sector=+0.70; gen=-0.28; rebound_floor |
| SYK | +0.460 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| DHR | +0.460 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| UTHR | +0.429 | Healthcare | join=+0.18; sector=+0.70; gen=-0.08; rebound_floor |
| EW | +0.422 | Healthcare | join=+0.17; sector=+0.70; gen=-0.28; rebound_floor |
| WTW | +0.405 | Financial | join=+0.35; sector=-0.28; gen=-0.08; rebound_floor |
| MDT | +0.397 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |
| COO | +0.382 | Healthcare | sector=+0.70; gen=-0.28; rebound_floor |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MUR | +0.325 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| AOS | +0.277 | Industrials | sector=+0.28; gen=-0.28; rebound_floor |
| TRMD | +0.270 | Energy | join=+0.20; sector=+0.65; gen=-0.08 |
| CAI | +0.263 | Healthcare | join=-0.17; sector=+0.70; gen=-0.28; rebound_floor |
| MLYS | +0.256 | Healthcare | join=-0.18; sector=+0.70; gen=-0.08; rebound_floor |
| XENE | +0.246 | Healthcare | sector=+0.70; gen=-0.08 |
| ERO | +0.239 | Basic Materials | join=-0.18; sector=-0.55; gen=-0.28; rebound_floor |
| QGEN | +0.236 | Healthcare | sector=+0.70; gen=-0.08 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| INMD | +0.236 | Healthcare | sector=+0.70; gen=-0.55 |
| OBE | +0.223 | Energy | sector=+0.65; gen=-0.08 |
| TBPH | +0.193 | Healthcare | sector=+0.70; gen=-0.08 |
| SEER | +0.188 | Healthcare | join=-0.32; sector=+0.70; gen=-0.55; rebound_floor |
| AVXL | +0.160 | Healthcare | join=-0.38; sector=+0.70; gen=-0.28; rebound_floor |
| QTTB | +0.149 | Healthcare | sector=+0.70; gen=-0.08 |
| OCS | +0.145 | Healthcare | sector=+0.70; gen=-0.08 |
| CYPH | +0.138 | Healthcare | join=-0.43; sector=+0.70; gen=-0.08; rebound_floor |

## 1m — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.544 | Healthcare | join=+0.35; sector=+0.70; gen=-0.28; rebound_floor |
| DHR | +0.492 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| SYK | +0.492 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| UTHR | +0.460 | Healthcare | join=+0.18; sector=+0.70; gen=-0.08; rebound_floor |
| EW | +0.453 | Healthcare | join=+0.17; sector=+0.70; gen=-0.28; rebound_floor |
| MDT | +0.433 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |
| WTW | +0.424 | Financial | join=+0.35; sector=-0.28; gen=-0.08; rebound_floor |
| ACN | +0.413 | Technology | join=+0.43; sector=-0.75; gen=-0.28 |
| COO | +0.412 | Healthcare | sector=+0.70; gen=-0.28; rebound_floor |
| TMO | +0.412 | Healthcare | sector=+0.70; gen=-0.28; rebound_floor |
| ECL | +0.392 | Basic Materials | sector=-0.55; gen=-0.28; rebound_floor |
| FAST | +0.382 | Industrials | join=+0.24; sector=+0.28; gen=-0.08; rebound_floor |
| RMD | +0.381 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| LIN | +0.374 | Basic Materials | join=+0.35; sector=-0.55; gen=-0.08 |
| ES | +0.374 | Utilities | join=+0.35; sector=+0.60; gen=-0.08 |
| ACGL | +0.369 | Financial | join=+0.54; sector=-0.28; gen=-0.08 |
| CB | +0.369 | Financial | join=+0.54; sector=-0.28; gen=-0.08 |
| CTSH | +0.361 | Technology | join=+0.32; sector=-0.75; gen=-0.28 |
| JKHY | +0.361 | Technology | join=+0.32; sector=-0.75; gen=-0.08 |
| HLN | +0.353 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| ELV | +0.353 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| ZBH | +0.353 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| CRH | +0.350 | Basic Materials | sector=-0.55; gen=-0.28; rebound_floor |
| HIG | +0.349 | Financial | join=+0.50; sector=-0.28; gen=-0.08 |
| VRSN | +0.346 | Technology | join=+0.29; sector=-0.75; gen=-0.08 |

## 1m — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TDUP | -0.362 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| GOGO | -0.362 | Communication Services | join=-0.72; sector=-0.30; gen=-0.28 |
| KLC | -0.362 | Consumer Defensive | join=-0.72; sector=+0.60; gen=-0.55 |
| CWH | -0.362 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| LVWR | -0.362 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| PLAY | -0.362 | Communication Services | join=-0.72; sector=-0.30; gen=-0.55 |
| EVGO | -0.360 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| CHPT | -0.360 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.55 |
| NXH | -0.341 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.55 |
| RC | -0.341 | Real Estate | join=-0.68; sector=-0.60; gen=-0.55 |
| GETY | -0.341 | Communication Services | join=-0.68; sector=-0.30; gen=-0.55 |
| BRCC | -0.341 | Consumer Defensive | join=-0.68; sector=+0.60; gen=-0.28 |
| ANGI | -0.341 | Communication Services | join=-0.68; sector=-0.30; gen=-0.55 |
| OPEN | -0.341 | Real Estate | join=-0.68; sector=-0.60; gen=-0.55 |
| VENU | -0.341 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.55 |
| CVGI | -0.341 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.55 |
| XPOF | -0.337 | Consumer Cyclical | join=-0.67; sector=-0.65; gen=-0.28 |
| ODD | -0.337 | Consumer Defensive | join=-0.67; sector=+0.60; gen=-0.55 |
| IHRT | -0.337 | Communication Services | join=-0.67; sector=-0.30; gen=-0.55 |
| INVZ | -0.337 | Consumer Cyclical | join=-0.67; sector=-0.65; gen=-0.55 |
| PLTK | -0.332 | Communication Services | join=-0.66; sector=-0.30; gen=-0.28 |
| CAST | -0.332 | Communication Services | join=-0.66; sector=-0.30; gen=-0.22 |
| AIIO | -0.332 | Consumer Cyclical | join=-0.66; sector=-0.65; gen=-0.55 |
| UAA | -0.318 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.55 |
| MNRO | -0.318 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.28 |

### 1m — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.544 | Healthcare | join=+0.35; sector=+0.70; gen=-0.28; rebound_floor |
| SYK | +0.492 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| DHR | +0.492 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| UTHR | +0.460 | Healthcare | join=+0.18; sector=+0.70; gen=-0.08; rebound_floor |
| EW | +0.453 | Healthcare | join=+0.17; sector=+0.70; gen=-0.28; rebound_floor |
| MDT | +0.433 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |
| WTW | +0.424 | Financial | join=+0.35; sector=-0.28; gen=-0.08; rebound_floor |
| ACN | +0.413 | Technology | join=+0.43; sector=-0.75; gen=-0.28 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| NICE | +0.333 | Technology | join=+0.27; sector=-0.75; gen=-0.08 |
| MUR | +0.330 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| AOS | +0.302 | Industrials | sector=+0.28; gen=-0.28; rebound_floor |
| NTCT | +0.290 | Technology | join=+0.18; sector=-0.75; gen=-0.08 |
| G | +0.290 | Technology | join=+0.18; sector=-0.75; gen=-0.08 |
| DSGX | +0.290 | Technology | join=+0.18; sector=-0.75; gen=-0.08 |
| CAI | +0.287 | Healthcare | join=-0.17; sector=+0.70; gen=-0.28; rebound_floor |
| MLYS | +0.280 | Healthcare | join=-0.18; sector=+0.70; gen=-0.08; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| INMD | +0.265 | Healthcare | sector=+0.70; gen=-0.55 |
| TUYA | +0.242 | Technology | sector=-0.75; gen=-0.08 |
| OBE | +0.230 | Energy | sector=+0.65; gen=-0.08 |
| TBPH | +0.220 | Healthcare | sector=+0.70; gen=-0.08 |
| SEER | +0.209 | Healthcare | join=-0.32; sector=+0.70; gen=-0.55; rebound_floor |
| CXM | +0.200 | Technology | sector=-0.75; gen=-0.08 |
| CRCT | +0.200 | Technology | sector=-0.75; gen=-0.08 |
| AVXL | +0.180 | Healthcare | join=-0.38; sector=+0.70; gen=-0.28; rebound_floor |

## Read

- **1d** news-heavy; **1m** structural join + sector.
- Universe gated: Market Cap ≥ $80M and Average Volume ≥ 500k shares (Finviz units).
- `rebound_floor` = checklist own-history score low + green-body bias (sparse; soft boost only).
- Raw checklist total score is NOT used as a buy rank (failed forward IC).
- Longer horizons use the predictors' explicit 3d/1w/2w/1m calls when stored, else fall back to the 1d call.
- Predictor bias is scaled by its graded hit rate (learning gate) — weak topics move scores less.
- Backtest: `python -m src.stock_book_backtest` (or Stock Book Backtest action).

CSV: `data/stock_book/2026-08-19_stock_book.csv`
JSON: `data/stock_book/2026-08-19_stock_book.json`
