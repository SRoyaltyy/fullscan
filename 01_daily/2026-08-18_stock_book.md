# Stock book — **2026-08-18** (1d / 3d / 1w / 2w / 1m)

Generated: 2026-08-18T13:03:52.555198-04:00

Layers: join (labels×weather) + sector predict + general regime + news actions.

## Regime snapshot

- **General bias:** -0.51
- **Weather risk:** off
- **News tickers:** 31
- **Universe (after liquidity):** 2698
- **Gates:** mcap ≥ $80.0M, avg vol ≥ 500.0k
- **Rebound floor tags:** 88

### Sector bias

| Sector | bias |
|--------|------|
| Healthcare | +0.70 |
| Consumer Cyclical | -0.65 |
| Energy | +0.65 |
| Technology | -0.64 |
| Real Estate | -0.60 |
| Utilities | +0.60 |
| Consumer Defensive | +0.51 |
| Basic Materials | -0.47 |
| Financial | -0.47 |
| Communication Services | -0.30 |
| Industrials | +0.28 |

### Learning gate (graded accuracy → how much each predictor is trusted)

| Topic | hit rate | graded runs | weight applied |
|-------|----------|-------------|----------------|
| general | 53% | 15 | ×0.85 |
| sector:Basic Materials | 50% | 6 | ×0.85 |
| sector:Communication Services | 33% | 6 | ×0.50 |
| sector:Consumer Cyclical | 83% | 6 | ×1.00 |
| sector:Consumer Defensive | 50% | 6 | ×0.85 |
| sector:Energy | 67% | 6 | ×1.00 |
| sector:Financial | 50% | 6 | ×0.85 |
| sector:Healthcare | 67% | 6 | ×1.00 |
| sector:Industrials | 33% | 6 | ×0.50 |
| sector:Real Estate | 67% | 6 | ×1.00 |
| sector:Technology | 50% | 6 | ×0.85 |
| sector:Utilities | 67% | 6 | ×1.00 |

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
| OXY | +0.536 | Energy | join=+0.32; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.528 | Energy | join=+0.30; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.528 | Energy | join=+0.30; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.503 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| DVN | +0.503 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.450 | Energy | sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| CVX | +0.444 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| XOM | +0.444 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| STE | +0.367 | Healthcare | join=+0.39; sector=+0.70; gen=-0.26; rebound_floor |
| SYK | +0.352 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| DHR | +0.352 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| TMO | +0.315 | Healthcare | join=+0.24; sector=+0.70; gen=-0.26; rebound_floor |
| EW | +0.309 | Healthcare | join=+0.23; sector=+0.70; gen=-0.26; rebound_floor |
| UTHR | +0.305 | Healthcare | join=+0.17; sector=+0.70; gen=-0.08; rebound_floor |
| COO | +0.283 | Healthcare | join=+0.15; sector=+0.70; gen=-0.26; rebound_floor |
| MUR | +0.267 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| FAST | +0.263 | Industrials | join=+0.23; sector=+0.28; gen=-0.08; rebound_floor |
| MDT | +0.259 | Healthcare | join=+0.46; sector=+0.70; gen=-0.08 |
| IQV | +0.229 | Healthcare | sector=+0.70; gen=-0.26; rebound_floor |
| RMD | +0.226 | Healthcare | join=+0.37; sector=+0.70; gen=-0.08 |
| ES | +0.220 | Utilities | join=+0.39; sector=+0.60; gen=-0.08 |
| AOS | +0.219 | Industrials | join=+0.15; sector=+0.28; gen=-0.26; rebound_floor |
| GGG | +0.219 | Industrials | join=+0.15; sector=+0.28; gen=-0.26; rebound_floor |
| MLYS | +0.218 | Healthcare | sector=+0.70; gen=-0.08; rebound_floor |
| PFE | +0.210 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |

## 1d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| SLG | -0.574 | Real Estate | join=-0.53; sector=-0.60; gen=-0.51; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| VNO | -0.528 | Real Estate | join=-0.39; sector=-0.60; gen=-0.51; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| SNOW | -0.524 | Technology | join=-0.49; sector=-0.64; gen=-0.51; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| TEAM | -0.518 | Technology | join=-0.55; sector=-0.64; gen=-0.26; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| AAL | -0.422 | Industrials | join=-0.39; sector=+0.28; gen=-0.51; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| ALK | -0.410 | Industrials | join=-0.43; sector=+0.28; gen=-0.26; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| TDUP | -0.403 | Consumer Cyclical | join=-0.73; sector=-0.65; gen=-0.51 |
| CWH | -0.403 | Consumer Cyclical | join=-0.73; sector=-0.65; gen=-0.51 |
| CHPT | -0.402 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| EVGO | -0.402 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| VERI | -0.401 | Technology | join=-0.73; sector=-0.64; gen=-0.51 |
| ADTN | -0.401 | Technology | join=-0.73; sector=-0.64; gen=-0.51 |
| GPRO | -0.400 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| SEDG | -0.400 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| CSIQ | -0.400 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| LVWR | -0.390 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| BROS | -0.390 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| VENU | -0.390 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| NXH | -0.390 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| WOLF | -0.388 | Technology | join=-0.69; sector=-0.64; gen=-0.51 |
| CNDT | -0.388 | Technology | join=-0.69; sector=-0.64; gen=-0.51 |
| CRNC | -0.388 | Technology | join=-0.69; sector=-0.64; gen=-0.51 |
| AIOT | -0.388 | Technology | join=-0.69; sector=-0.64; gen=-0.51 |
| INVZ | -0.387 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| JMIA | -0.387 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |

### 1d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| OXY | +0.536 | Energy | join=+0.32; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.528 | Energy | join=+0.30; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.528 | Energy | join=+0.30; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| DVN | +0.503 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.503 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.450 | Energy | sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| XOM | +0.444 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| CVX | +0.444 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MUR | +0.267 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| AOS | +0.219 | Industrials | join=+0.15; sector=+0.28; gen=-0.26; rebound_floor |
| MLYS | +0.218 | Healthcare | sector=+0.70; gen=-0.08; rebound_floor |
| TRMD | +0.183 | Energy | join=+0.27; sector=+0.65; gen=-0.08 |
| CAI | +0.176 | Healthcare | join=-0.15; sector=+0.70; gen=-0.26; rebound_floor |
| XENE | +0.166 | Healthcare | join=+0.20; sector=+0.70; gen=-0.08 |
| OLLI | +0.165 | Consumer Defensive | join=-0.15; sector=+0.51; gen=-0.08; rebound_floor |
| AMLX | +0.162 | Healthcare | join=-0.24; sector=+0.70; gen=-0.08; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| CYPH | +0.162 | Healthcare | join=-0.24; sector=+0.70; gen=-0.08; rebound_floor |
| OBE | +0.153 | Energy | join=+0.18; sector=+0.65; gen=-0.08 |
| AVXL | +0.136 | Healthcare | join=-0.27; sector=+0.70; gen=-0.26; rebound_floor |
| SEER | +0.125 | Healthcare | join=-0.23; sector=+0.70; gen=-0.51; rebound_floor |
| TBPH | +0.124 | Healthcare | sector=+0.70; gen=-0.08 |
| GERN | +0.119 | Healthcare | join=-0.37; sector=+0.70; gen=-0.08; rebound_floor |
| INMD | +0.112 | Healthcare | join=+0.17; sector=+0.70; gen=-0.51 |
| NVCR | +0.108 | Healthcare | join=-0.35; sector=+0.70; gen=-0.26; rebound_floor |

## 3d — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| OXY | +0.480 | Energy | join=+0.32; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.471 | Energy | join=+0.30; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.471 | Energy | join=+0.30; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| DVN | +0.442 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.442 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| STE | +0.434 | Healthcare | join=+0.39; sector=+0.70; gen=-0.26; rebound_floor |
| SYK | +0.412 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| DHR | +0.412 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| XOM | +0.405 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| CVX | +0.405 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| FANG | +0.382 | Energy | sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| TMO | +0.375 | Healthcare | join=+0.24; sector=+0.70; gen=-0.26; rebound_floor |
| EW | +0.367 | Healthcare | join=+0.23; sector=+0.70; gen=-0.26; rebound_floor |
| UTHR | +0.359 | Healthcare | join=+0.17; sector=+0.70; gen=-0.08; rebound_floor |
| COO | +0.338 | Healthcare | join=+0.15; sector=+0.70; gen=-0.26; rebound_floor |
| MDT | +0.328 | Healthcare | join=+0.46; sector=+0.70; gen=-0.08 |
| MUR | +0.324 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| ES | +0.301 | Utilities | join=+0.39; sector=+0.60; gen=-0.08 |
| RMD | +0.290 | Healthcare | join=+0.37; sector=+0.70; gen=-0.08 |
| IQV | +0.277 | Healthcare | sector=+0.70; gen=-0.26; rebound_floor |
| ZBH | +0.272 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| EVRG | +0.272 | Utilities | join=+0.32; sector=+0.60; gen=-0.08 |
| PCG | +0.272 | Utilities | join=+0.32; sector=+0.60; gen=-0.08 |
| PFE | +0.272 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| NGG | +0.272 | Utilities | join=+0.32; sector=+0.60; gen=-0.08 |

## 3d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| SLG | -0.550 | Real Estate | join=-0.53; sector=-0.60; gen=-0.51; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| VNO | -0.498 | Real Estate | join=-0.39; sector=-0.60; gen=-0.51; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| SNOW | -0.488 | Technology | join=-0.49; sector=-0.64; gen=-0.51; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| TDUP | -0.488 | Consumer Cyclical | join=-0.73; sector=-0.65; gen=-0.51 |
| CWH | -0.488 | Consumer Cyclical | join=-0.73; sector=-0.65; gen=-0.51 |
| TEAM | -0.488 | Technology | join=-0.55; sector=-0.64; gen=-0.26; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| CHPT | -0.486 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| EVGO | -0.486 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| NXH | -0.472 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| BROS | -0.472 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| VENU | -0.472 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| LVWR | -0.472 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| JMIA | -0.470 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| INVZ | -0.470 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| FOSL | -0.455 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.51 |
| UAA | -0.455 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.51 |
| CVGI | -0.455 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.51 |
| SFIX | -0.455 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.51 |
| UA | -0.455 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.51 |
| ADTN | -0.455 | Technology | join=-0.73; sector=-0.64; gen=-0.51 |
| VERI | -0.455 | Technology | join=-0.73; sector=-0.64; gen=-0.51 |
| CSIQ | -0.453 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| GPRO | -0.453 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| SEDG | -0.453 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| MB | -0.451 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.51 |

### 3d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| OXY | +0.480 | Energy | join=+0.32; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.471 | Energy | join=+0.30; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.471 | Energy | join=+0.30; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| DVN | +0.442 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.442 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| STE | +0.434 | Healthcare | join=+0.39; sector=+0.70; gen=-0.26; rebound_floor |
| DHR | +0.412 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| SYK | +0.412 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MUR | +0.324 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| MLYS | +0.260 | Healthcare | sector=+0.70; gen=-0.08; rebound_floor |
| TRMD | +0.249 | Energy | join=+0.27; sector=+0.65; gen=-0.08 |
| XENE | +0.222 | Healthcare | join=+0.20; sector=+0.70; gen=-0.08 |
| CAI | +0.216 | Healthcare | join=-0.15; sector=+0.70; gen=-0.26; rebound_floor |
| QGEN | +0.209 | Healthcare | join=+0.17; sector=+0.70; gen=-0.08 |
| MTDR | +0.209 | Energy | join=+0.17; sector=+0.65; gen=-0.08 |
| TAL | +0.208 | Consumer Defensive | join=+0.24; sector=+0.51; gen=-0.08 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| OBE | +0.215 | Energy | join=+0.18; sector=+0.65; gen=-0.08 |
| CYPH | +0.195 | Healthcare | join=-0.24; sector=+0.70; gen=-0.08; rebound_floor |
| TBPH | +0.174 | Healthcare | sector=+0.70; gen=-0.08 |
| AVXL | +0.170 | Healthcare | join=-0.27; sector=+0.70; gen=-0.26; rebound_floor |
| INMD | +0.169 | Healthcare | join=+0.17; sector=+0.70; gen=-0.51 |
| SEER | +0.163 | Healthcare | join=-0.23; sector=+0.70; gen=-0.51; rebound_floor |
| GERN | +0.146 | Healthcare | join=-0.37; sector=+0.70; gen=-0.08; rebound_floor |
| NOMD | +0.143 | Consumer Defensive | sector=+0.51; gen=-0.08 |

## 1w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.507 | Healthcare | join=+0.39; sector=+0.70; gen=-0.26; rebound_floor |
| SYK | +0.464 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| DHR | +0.464 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| TMO | +0.440 | Healthcare | join=+0.24; sector=+0.70; gen=-0.26; rebound_floor |
| OXY | +0.435 | Energy | join=+0.32; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EW | +0.432 | Healthcare | join=+0.23; sector=+0.70; gen=-0.26; rebound_floor |
| COP | +0.424 | Energy | join=+0.30; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.424 | Energy | join=+0.30; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| WTW | +0.422 | Financial | join=+0.32; sector=-0.47; gen=-0.08; rebound_floor |
| UTHR | +0.404 | Healthcare | join=+0.17; sector=+0.70; gen=-0.08; rebound_floor |
| COO | +0.399 | Healthcare | join=+0.15; sector=+0.70; gen=-0.26; rebound_floor |
| DVN | +0.392 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.392 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| MDT | +0.388 | Healthcare | join=+0.46; sector=+0.70; gen=-0.08 |
| CVX | +0.370 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| XOM | +0.370 | Energy | join=+0.23; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| ACGL | +0.351 | Financial | join=+0.50; sector=-0.47; gen=-0.08 |
| MUR | +0.350 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| MRSH | +0.346 | Financial | join=+0.15; sector=-0.47; gen=-0.08; rebound_floor |
| RMD | +0.345 | Healthcare | join=+0.37; sector=+0.70; gen=-0.08 |
| ES | +0.342 | Utilities | join=+0.39; sector=+0.60; gen=-0.08 |
| HIG | +0.335 | Financial | join=+0.46; sector=-0.47; gen=-0.08 |
| IQV | +0.330 | Healthcare | sector=+0.70; gen=-0.26; rebound_floor |
| WST | +0.325 | Healthcare | join=+0.32; sector=+0.70; gen=-0.26 |
| ELV | +0.325 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |

## 1w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TDUP | -0.492 | Consumer Cyclical | join=-0.73; sector=-0.65; gen=-0.51 |
| CWH | -0.492 | Consumer Cyclical | join=-0.73; sector=-0.65; gen=-0.51 |
| CHPT | -0.491 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| EVGO | -0.491 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| LVWR | -0.475 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| NXH | -0.475 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| TRIP | -0.475 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.26 |
| BROS | -0.475 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| VENU | -0.475 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| XPOF | -0.472 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.26 |
| INVZ | -0.472 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| JMIA | -0.472 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| SFIX | -0.456 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.51 |
| UA | -0.456 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.51 |
| OI | -0.456 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.08 |
| FOSL | -0.456 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.51 |
| UAA | -0.456 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.51 |
| MNRO | -0.456 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.26 |
| LCID | -0.456 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.26 |
| GT | -0.456 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.26 |
| CVGI | -0.456 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.51 |
| ADTN | -0.455 | Technology | join=-0.73; sector=-0.64; gen=-0.51 |
| VERI | -0.455 | Technology | join=-0.73; sector=-0.64; gen=-0.51 |
| SEDG | -0.453 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| CSIQ | -0.453 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |

### 1w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.507 | Healthcare | join=+0.39; sector=+0.70; gen=-0.26; rebound_floor |
| SYK | +0.464 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| DHR | +0.464 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| TMO | +0.440 | Healthcare | join=+0.24; sector=+0.70; gen=-0.26; rebound_floor |
| OXY | +0.435 | Energy | join=+0.32; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EW | +0.432 | Healthcare | join=+0.23; sector=+0.70; gen=-0.26; rebound_floor |
| COP | +0.424 | Energy | join=+0.30; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.424 | Energy | join=+0.30; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MUR | +0.350 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| MLYS | +0.293 | Healthcare | sector=+0.70; gen=-0.08; rebound_floor |
| TRMD | +0.285 | Energy | join=+0.27; sector=+0.65; gen=-0.08 |
| XENE | +0.269 | Healthcare | join=+0.20; sector=+0.70; gen=-0.08 |
| CAI | +0.261 | Healthcare | join=-0.15; sector=+0.70; gen=-0.26; rebound_floor |
| QGEN | +0.254 | Healthcare | join=+0.17; sector=+0.70; gen=-0.08 |
| TAL | +0.250 | Consumer Defensive | join=+0.24; sector=+0.51; gen=-0.08 |
| OTF | +0.243 | Financial | sector=-0.47; gen=-0.08; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| INMD | +0.254 | Healthcare | join=+0.17; sector=+0.70; gen=-0.51 |
| OBE | +0.246 | Energy | join=+0.18; sector=+0.65; gen=-0.08 |
| SEER | +0.228 | Healthcare | join=-0.23; sector=+0.70; gen=-0.51; rebound_floor |
| CYPH | +0.220 | Healthcare | join=-0.24; sector=+0.70; gen=-0.08; rebound_floor |
| TBPH | +0.215 | Healthcare | sector=+0.70; gen=-0.08 |
| AVXL | +0.210 | Healthcare | join=-0.27; sector=+0.70; gen=-0.26; rebound_floor |
| QTTB | +0.180 | Healthcare | sector=+0.70; gen=-0.08 |
| ABUS | +0.180 | Healthcare | sector=+0.70; gen=-0.08 |

## 2w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.532 | Healthcare | join=+0.39; sector=+0.70; gen=-0.26; rebound_floor |
| SYK | +0.486 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| DHR | +0.486 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| TMO | +0.460 | Healthcare | join=+0.24; sector=+0.70; gen=-0.26; rebound_floor |
| WTW | +0.453 | Financial | join=+0.32; sector=-0.47; gen=-0.08; rebound_floor |
| EW | +0.451 | Healthcare | join=+0.23; sector=+0.70; gen=-0.26; rebound_floor |
| UTHR | +0.422 | Healthcare | join=+0.17; sector=+0.70; gen=-0.08; rebound_floor |
| COO | +0.416 | Healthcare | join=+0.15; sector=+0.70; gen=-0.26; rebound_floor |
| MDT | +0.414 | Healthcare | join=+0.46; sector=+0.70; gen=-0.08 |
| OXY | +0.388 | Energy | join=+0.32; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| ACGL | +0.387 | Financial | join=+0.50; sector=-0.47; gen=-0.08 |
| COP | +0.377 | Energy | join=+0.30; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.377 | Energy | join=+0.30; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| MRSH | +0.372 | Financial | join=+0.15; sector=-0.47; gen=-0.08; rebound_floor |
| HIG | +0.371 | Financial | join=+0.46; sector=-0.47; gen=-0.08 |
| RMD | +0.369 | Healthcare | join=+0.37; sector=+0.70; gen=-0.08 |
| ES | +0.364 | Utilities | join=+0.39; sector=+0.60; gen=-0.08 |
| MUR | +0.362 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| PFE | +0.347 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| WST | +0.347 | Healthcare | join=+0.32; sector=+0.70; gen=-0.26 |
| ELV | +0.347 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| ZBH | +0.347 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| HLN | +0.347 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| FAST | +0.346 | Industrials | join=+0.23; sector=+0.28; gen=-0.08; rebound_floor |
| IQV | +0.343 | Healthcare | sector=+0.70; gen=-0.26; rebound_floor |

## 2w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TDUP | -0.349 | Consumer Cyclical | join=-0.73; sector=-0.65; gen=-0.51 |
| ANGI | -0.349 | Communication Services | join=-0.73; sector=-0.30; gen=-0.51 |
| ADTN | -0.349 | Technology | join=-0.73; sector=-0.64; gen=-0.51 |
| PLAY | -0.349 | Communication Services | join=-0.73; sector=-0.30; gen=-0.51 |
| VERI | -0.349 | Technology | join=-0.73; sector=-0.64; gen=-0.51 |
| CWH | -0.349 | Consumer Cyclical | join=-0.73; sector=-0.65; gen=-0.51 |
| CSIQ | -0.348 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| SEDG | -0.348 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| GPRO | -0.348 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| EVGO | -0.348 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| IHRT | -0.348 | Communication Services | join=-0.72; sector=-0.30; gen=-0.51 |
| CHPT | -0.348 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| QMLS | -0.344 | Technology | join=-0.72; sector=-0.64; gen=-0.20 |
| WOLF | -0.331 | Technology | join=-0.69; sector=-0.64; gen=-0.51 |
| AIOT | -0.331 | Technology | join=-0.69; sector=-0.64; gen=-0.51 |
| TRIP | -0.331 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.26 |
| LVWR | -0.331 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| NXH | -0.331 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| CRNC | -0.331 | Technology | join=-0.69; sector=-0.64; gen=-0.51 |
| GETY | -0.331 | Communication Services | join=-0.69; sector=-0.30; gen=-0.51 |
| THRY | -0.331 | Technology | join=-0.69; sector=-0.64; gen=-0.26 |
| GOGO | -0.331 | Communication Services | join=-0.69; sector=-0.30; gen=-0.26 |
| CNDT | -0.331 | Technology | join=-0.69; sector=-0.64; gen=-0.51 |
| VENU | -0.331 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| BROS | -0.331 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |

### 2w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.532 | Healthcare | join=+0.39; sector=+0.70; gen=-0.26; rebound_floor |
| SYK | +0.486 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| DHR | +0.486 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| TMO | +0.460 | Healthcare | join=+0.24; sector=+0.70; gen=-0.26; rebound_floor |
| WTW | +0.453 | Financial | join=+0.32; sector=-0.47; gen=-0.08; rebound_floor |
| EW | +0.451 | Healthcare | join=+0.23; sector=+0.70; gen=-0.26; rebound_floor |
| UTHR | +0.422 | Healthcare | join=+0.17; sector=+0.70; gen=-0.08; rebound_floor |
| COO | +0.416 | Healthcare | join=+0.15; sector=+0.70; gen=-0.26; rebound_floor |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MUR | +0.362 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| AOS | +0.311 | Industrials | join=+0.15; sector=+0.28; gen=-0.26; rebound_floor |
| TRMD | +0.303 | Energy | join=+0.27; sector=+0.65; gen=-0.08 |
| MLYS | +0.303 | Healthcare | sector=+0.70; gen=-0.08; rebound_floor |
| XENE | +0.287 | Healthcare | join=+0.20; sector=+0.70; gen=-0.08 |
| QGEN | +0.272 | Healthcare | join=+0.17; sector=+0.70; gen=-0.08 |
| CAI | +0.269 | Healthcare | join=-0.15; sector=+0.70; gen=-0.26; rebound_floor |
| TCBI | +0.266 | Financial | join=+0.24; sector=-0.47; gen=-0.08 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| INMD | +0.272 | Healthcare | join=+0.17; sector=+0.70; gen=-0.51 |
| OBE | +0.261 | Energy | join=+0.18; sector=+0.65; gen=-0.08 |
| SEER | +0.234 | Healthcare | join=-0.23; sector=+0.70; gen=-0.51; rebound_floor |
| TBPH | +0.229 | Healthcare | sector=+0.70; gen=-0.08 |
| CYPH | +0.225 | Healthcare | join=-0.24; sector=+0.70; gen=-0.08; rebound_floor |
| AVXL | +0.215 | Healthcare | join=-0.27; sector=+0.70; gen=-0.26; rebound_floor |
| QTTB | +0.193 | Healthcare | sector=+0.70; gen=-0.08 |
| ABUS | +0.193 | Healthcare | sector=+0.70; gen=-0.08 |

## 1m — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.588 | Healthcare | join=+0.39; sector=+0.70; gen=-0.26; rebound_floor |
| DHR | +0.526 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| SYK | +0.526 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| TMO | +0.514 | Healthcare | join=+0.24; sector=+0.70; gen=-0.26; rebound_floor |
| EW | +0.505 | Healthcare | join=+0.23; sector=+0.70; gen=-0.26; rebound_floor |
| WTW | +0.487 | Financial | join=+0.32; sector=-0.47; gen=-0.08; rebound_floor |
| COO | +0.468 | Healthcare | join=+0.15; sector=+0.70; gen=-0.26; rebound_floor |
| UTHR | +0.459 | Healthcare | join=+0.17; sector=+0.70; gen=-0.08; rebound_floor |
| MDT | +0.457 | Healthcare | join=+0.46; sector=+0.70; gen=-0.08 |
| ACGL | +0.425 | Financial | join=+0.50; sector=-0.47; gen=-0.08 |
| RMD | +0.410 | Healthcare | join=+0.37; sector=+0.70; gen=-0.08 |
| HIG | +0.407 | Financial | join=+0.46; sector=-0.47; gen=-0.08 |
| ES | +0.403 | Utilities | join=+0.39; sector=+0.60; gen=-0.08 |
| MRSH | +0.403 | Financial | join=+0.15; sector=-0.47; gen=-0.08; rebound_floor |
| WST | +0.402 | Healthcare | join=+0.32; sector=+0.70; gen=-0.26 |
| IQV | +0.391 | Healthcare | sector=+0.70; gen=-0.26; rebound_floor |
| TECH | +0.390 | Healthcare | join=+0.30; sector=+0.70; gen=-0.26 |
| PFE | +0.387 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| ZBH | +0.387 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| HLN | +0.387 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| ELV | +0.387 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| FAST | +0.380 | Industrials | join=+0.23; sector=+0.28; gen=-0.08; rebound_floor |
| ECL | +0.380 | Basic Materials | sector=-0.47; gen=-0.26; rebound_floor |
| ALGN | +0.376 | Healthcare | join=+0.23; sector=+0.70; gen=-0.51 |
| CVS | +0.376 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08 |

## 1m — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TRIP | -0.323 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.26 |
| GOGO | -0.323 | Communication Services | join=-0.69; sector=-0.30; gen=-0.26 |
| TDUP | -0.321 | Consumer Cyclical | join=-0.73; sector=-0.65; gen=-0.51 |
| ANGI | -0.321 | Communication Services | join=-0.73; sector=-0.30; gen=-0.51 |
| PLAY | -0.321 | Communication Services | join=-0.73; sector=-0.30; gen=-0.51 |
| CWH | -0.321 | Consumer Cyclical | join=-0.73; sector=-0.65; gen=-0.51 |
| XPOF | -0.320 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.26 |
| IHRT | -0.320 | Communication Services | join=-0.72; sector=-0.30; gen=-0.51 |
| EVGO | -0.320 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| CHPT | -0.320 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| OI | -0.317 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.08 |
| PLTK | -0.316 | Communication Services | join=-0.67; sector=-0.30; gen=-0.26 |
| BROS | -0.302 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| GETY | -0.302 | Communication Services | join=-0.69; sector=-0.30; gen=-0.51 |
| VENU | -0.302 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| LVWR | -0.302 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| NXH | -0.302 | Consumer Cyclical | join=-0.69; sector=-0.65; gen=-0.51 |
| GT | -0.302 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.26 |
| MNRO | -0.302 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.26 |
| LCID | -0.302 | Consumer Cyclical | join=-0.65; sector=-0.65; gen=-0.26 |
| INVZ | -0.299 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| JMIA | -0.299 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| PZZA | -0.296 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.26 |
| OPTU | -0.296 | Communication Services | join=-0.64; sector=-0.30; gen=-0.26 |
| FUN | -0.293 | Consumer Cyclical | join=-0.60; sector=-0.65; gen=-0.08 |

### 1m — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.588 | Healthcare | join=+0.39; sector=+0.70; gen=-0.26; rebound_floor |
| DHR | +0.526 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| SYK | +0.526 | Healthcare | join=+0.30; sector=+0.70; gen=-0.08; rebound_floor |
| TMO | +0.514 | Healthcare | join=+0.24; sector=+0.70; gen=-0.26; rebound_floor |
| EW | +0.505 | Healthcare | join=+0.23; sector=+0.70; gen=-0.26; rebound_floor |
| WTW | +0.487 | Financial | join=+0.32; sector=-0.47; gen=-0.08; rebound_floor |
| COO | +0.468 | Healthcare | join=+0.15; sector=+0.70; gen=-0.26; rebound_floor |
| UTHR | +0.459 | Healthcare | join=+0.17; sector=+0.70; gen=-0.08; rebound_floor |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MUR | +0.375 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| AOS | +0.358 | Industrials | join=+0.15; sector=+0.28; gen=-0.26; rebound_floor |
| MLYS | +0.335 | Healthcare | sector=+0.70; gen=-0.08; rebound_floor |
| XENE | +0.325 | Healthcare | join=+0.20; sector=+0.70; gen=-0.08 |
| TRMD | +0.319 | Energy | join=+0.27; sector=+0.65; gen=-0.08 |
| CAI | +0.315 | Healthcare | join=-0.15; sector=+0.70; gen=-0.26; rebound_floor |
| QGEN | +0.309 | Healthcare | join=+0.17; sector=+0.70; gen=-0.08 |
| TCBI | +0.299 | Financial | join=+0.24; sector=-0.47; gen=-0.08 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| INMD | +0.345 | Healthcare | join=+0.17; sector=+0.70; gen=-0.51 |
| SEER | +0.299 | Healthcare | join=-0.23; sector=+0.70; gen=-0.51; rebound_floor |
| OBE | +0.276 | Energy | join=+0.18; sector=+0.65; gen=-0.08 |
| TBPH | +0.265 | Healthcare | sector=+0.70; gen=-0.08 |
| AVXL | +0.258 | Healthcare | join=-0.27; sector=+0.70; gen=-0.26; rebound_floor |
| CYPH | +0.254 | Healthcare | join=-0.24; sector=+0.70; gen=-0.08; rebound_floor |
| SDGR | +0.229 | Healthcare | join=-0.37; sector=+0.70; gen=-0.51; rebound_floor |
| ABUS | +0.226 | Healthcare | sector=+0.70; gen=-0.08 |

## Read

- **1d** news-heavy; **1m** structural join + sector.
- Universe gated: Market Cap ≥ $80M and Average Volume ≥ 500k shares (Finviz units).
- `rebound_floor` = checklist own-history score low + green-body bias (sparse; soft boost only).
- Raw checklist total score is NOT used as a buy rank (failed forward IC).
- Longer horizons use the predictors' explicit 3d/1w/2w/1m calls when stored, else fall back to the 1d call.
- Predictor bias is scaled by its graded hit rate (learning gate) — weak topics move scores less.
- Backtest: `python -m src.stock_book_backtest` (or Stock Book Backtest action).

CSV: `data/stock_book/2026-08-18_stock_book.csv`
JSON: `data/stock_book/2026-08-18_stock_book.json`
