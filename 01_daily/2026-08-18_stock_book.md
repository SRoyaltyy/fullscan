# Stock book — **2026-08-18** (1d / 3d / 1w / 2w / 1m)

Generated: 2026-08-18T10:41:32.655292-04:00

Layers: join (labels×weather) + sector predict + general regime + news actions.

## Regime snapshot

- **General bias:** -0.51
- **Weather risk:** off
- **News tickers:** 39
- **Universe (after liquidity):** 2697
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
| DVN | +0.516 | Energy | join=+0.27; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.516 | Energy | join=+0.27; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.509 | Energy | join=+0.24; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.509 | Energy | join=+0.24; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.481 | Energy | join=+0.17; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.423 | Energy | sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| XOM | +0.422 | Energy | join=+0.17; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| CVX | +0.422 | Energy | join=+0.17; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| STE | +0.351 | Healthcare | join=+0.35; sector=+0.70; gen=-0.26; rebound_floor |
| SYK | +0.333 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| DHR | +0.333 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| EW | +0.323 | Healthcare | join=+0.27; sector=+0.70; gen=-0.26; rebound_floor |
| TMO | +0.292 | Healthcare | join=+0.18; sector=+0.70; gen=-0.26; rebound_floor |
| UTHR | +0.279 | Healthcare | sector=+0.70; gen=-0.08; rebound_floor |
| FAST | +0.269 | Industrials | join=+0.24; sector=+0.28; gen=-0.08; rebound_floor |
| COO | +0.259 | Healthcare | sector=+0.70; gen=-0.26; rebound_floor |
| MDT | +0.246 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |
| MUR | +0.240 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| AOS | +0.224 | Industrials | join=+0.17; sector=+0.28; gen=-0.26; rebound_floor |
| GGG | +0.224 | Industrials | join=+0.17; sector=+0.28; gen=-0.26; rebound_floor |
| RMD | +0.210 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| ES | +0.204 | Utilities | join=+0.35; sector=+0.60; gen=-0.08 |
| IQV | +0.200 | Healthcare | sector=+0.70; gen=-0.26; rebound_floor |
| ITW | +0.197 | Industrials | sector=+0.28; gen=-0.26; rebound_floor |
| WTW | +0.194 | Financial | join=+0.35; sector=-0.47; gen=-0.08; rebound_floor |

## 1d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| SNOW | -0.663 | Technology | join=-0.46; sector=-0.64; gen=-0.51; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| TEAM | -0.660 | Technology | join=-0.53; sector=-0.64; gen=-0.26; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| SLG | -0.587 | Real Estate | join=-0.56; sector=-0.60; gen=-0.51; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| VNO | -0.539 | Real Estate | join=-0.43; sector=-0.60; gen=-0.51; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| WDAY | -0.476 | Technology | sector=-0.64; gen=-0.26; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| NOW | -0.476 | Technology | sector=-0.64; gen=-0.26; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| ADBE | -0.472 | Technology | sector=-0.64; gen=-0.51; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| CRM | -0.447 | Technology | sector=-0.64; gen=-0.26; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| AAL | -0.433 | Industrials | join=-0.43; sector=+0.28; gen=-0.51; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| ALK | -0.420 | Industrials | join=-0.46; sector=+0.28; gen=-0.26; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| CWH | -0.402 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| VENU | -0.402 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| TDUP | -0.402 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| EVGO | -0.401 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| CHPT | -0.401 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| VERI | -0.400 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| ADTN | -0.400 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| SEDG | -0.399 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| GPRO | -0.399 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| CSIQ | -0.399 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| NXH | -0.387 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| BROS | -0.387 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| LVWR | -0.387 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| AIOT | -0.385 | Technology | join=-0.68; sector=-0.64; gen=-0.51 |
| WOLF | -0.385 | Technology | join=-0.68; sector=-0.64; gen=-0.51 |

### 1d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DVN | +0.516 | Energy | join=+0.27; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.516 | Energy | join=+0.27; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.509 | Energy | join=+0.24; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.509 | Energy | join=+0.24; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.481 | Energy | join=+0.17; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.423 | Energy | sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| XOM | +0.422 | Energy | join=+0.17; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| CVX | +0.422 | Energy | join=+0.17; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MUR | +0.240 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| AOS | +0.224 | Industrials | join=+0.17; sector=+0.28; gen=-0.26; rebound_floor |
| MLYS | +0.184 | Healthcare | join=-0.18; sector=+0.70; gen=-0.08; rebound_floor |
| CAI | +0.172 | Healthcare | join=-0.17; sector=+0.70; gen=-0.26; rebound_floor |
| TRMD | +0.159 | Energy | join=+0.20; sector=+0.65; gen=-0.08 |
| XENE | +0.136 | Healthcare | sector=+0.70; gen=-0.08 |
| OLLI | +0.133 | Consumer Defensive | join=-0.24; sector=+0.51; gen=-0.08; rebound_floor |
| TAL | +0.132 | Consumer Defensive | join=+0.18; sector=+0.51; gen=-0.08 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| CYPH | +0.125 | Healthcare | join=-0.35; sector=+0.70; gen=-0.08; rebound_floor |
| OBE | +0.125 | Energy | sector=+0.65; gen=-0.08 |
| TBPH | +0.097 | Healthcare | sector=+0.70; gen=-0.08 |
| AVXL | +0.097 | Healthcare | join=-0.38; sector=+0.70; gen=-0.26; rebound_floor |
| SEER | +0.091 | Healthcare | join=-0.32; sector=+0.70; gen=-0.51; rebound_floor |
| INMD | +0.086 | Healthcare | sector=+0.70; gen=-0.51 |
| GERN | +0.086 | Healthcare | join=-0.46; sector=+0.70; gen=-0.08; rebound_floor |
| NOMD | +0.069 | Consumer Defensive | sector=+0.51; gen=-0.08 |

## 3d — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| OXY | +0.458 | Energy | join=+0.27; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| DVN | +0.458 | Energy | join=+0.27; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.449 | Energy | join=+0.24; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.449 | Energy | join=+0.24; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.417 | Energy | join=+0.17; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| STE | +0.416 | Healthcare | join=+0.35; sector=+0.70; gen=-0.26; rebound_floor |
| DHR | +0.391 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| SYK | +0.391 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| EW | +0.383 | Healthcare | join=+0.27; sector=+0.70; gen=-0.26; rebound_floor |
| CVX | +0.381 | Energy | join=+0.17; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| XOM | +0.381 | Energy | join=+0.17; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| FANG | +0.351 | Energy | sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| TMO | +0.349 | Healthcare | join=+0.18; sector=+0.70; gen=-0.26; rebound_floor |
| UTHR | +0.329 | Healthcare | sector=+0.70; gen=-0.08; rebound_floor |
| MDT | +0.313 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |
| COO | +0.310 | Healthcare | sector=+0.70; gen=-0.26; rebound_floor |
| MUR | +0.293 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| ES | +0.282 | Utilities | join=+0.35; sector=+0.60; gen=-0.08 |
| WTW | +0.282 | Financial | join=+0.35; sector=-0.47; gen=-0.08; rebound_floor |
| RMD | +0.272 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| PFE | +0.249 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| ZBH | +0.249 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |
| AEE | +0.249 | Utilities | join=+0.27; sector=+0.60; gen=-0.08 |
| NGG | +0.249 | Utilities | join=+0.27; sector=+0.60; gen=-0.08 |
| EVRG | +0.249 | Utilities | join=+0.27; sector=+0.60; gen=-0.08 |

## 3d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TEAM | -0.572 | Technology | join=-0.53; sector=-0.64; gen=-0.26; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| SNOW | -0.570 | Technology | join=-0.46; sector=-0.64; gen=-0.51; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| SLG | -0.565 | Real Estate | join=-0.56; sector=-0.60; gen=-0.51; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| VNO | -0.510 | Real Estate | join=-0.43; sector=-0.60; gen=-0.51; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| CWH | -0.486 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| VENU | -0.486 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| TDUP | -0.486 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| EVGO | -0.485 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| CHPT | -0.485 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| NXH | -0.470 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| LVWR | -0.470 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| BROS | -0.470 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| JMIA | -0.466 | Consumer Cyclical | join=-0.67; sector=-0.65; gen=-0.51 |
| INVZ | -0.466 | Consumer Cyclical | join=-0.67; sector=-0.65; gen=-0.51 |
| OPEN | -0.457 | Real Estate | join=-0.68; sector=-0.60; gen=-0.51 |
| RC | -0.457 | Real Estate | join=-0.68; sector=-0.60; gen=-0.51 |
| ADTN | -0.453 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| VERI | -0.453 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| SEDG | -0.452 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| GPRO | -0.452 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| CSIQ | -0.452 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| AAL | -0.451 | Industrials | join=-0.43; sector=+0.28; gen=-0.51; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| SFIX | -0.451 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.51 |
| CVGI | -0.451 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.51 |
| FOSL | -0.451 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.51 |

### 3d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DVN | +0.458 | Energy | join=+0.27; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.458 | Energy | join=+0.27; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.449 | Energy | join=+0.24; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.449 | Energy | join=+0.24; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.417 | Energy | join=+0.17; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| STE | +0.416 | Healthcare | join=+0.35; sector=+0.70; gen=-0.26; rebound_floor |
| DHR | +0.391 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| SYK | +0.391 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MUR | +0.293 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| TRMD | +0.222 | Energy | join=+0.20; sector=+0.65; gen=-0.08 |
| MLYS | +0.221 | Healthcare | join=-0.18; sector=+0.70; gen=-0.08; rebound_floor |
| CAI | +0.211 | Healthcare | join=-0.17; sector=+0.70; gen=-0.26; rebound_floor |
| XENE | +0.187 | Healthcare | sector=+0.70; gen=-0.08 |
| TAL | +0.182 | Consumer Defensive | join=+0.18; sector=+0.51; gen=-0.08 |
| QGEN | +0.179 | Healthcare | sector=+0.70; gen=-0.08 |
| MTDR | +0.179 | Energy | sector=+0.65; gen=-0.08 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| OBE | +0.183 | Energy | sector=+0.65; gen=-0.08 |
| CYPH | +0.154 | Healthcare | join=-0.35; sector=+0.70; gen=-0.08; rebound_floor |
| TBPH | +0.143 | Healthcare | sector=+0.70; gen=-0.08 |
| INMD | +0.140 | Healthcare | sector=+0.70; gen=-0.51 |
| SEER | +0.125 | Healthcare | join=-0.32; sector=+0.70; gen=-0.51; rebound_floor |
| AVXL | +0.125 | Healthcare | join=-0.38; sector=+0.70; gen=-0.26; rebound_floor |
| NOMD | +0.110 | Consumer Defensive | sector=+0.51; gen=-0.08 |
| GERN | +0.108 | Healthcare | join=-0.46; sector=+0.70; gen=-0.08; rebound_floor |

## 1w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.487 | Healthcare | join=+0.35; sector=+0.70; gen=-0.26; rebound_floor |
| EW | +0.450 | Healthcare | join=+0.27; sector=+0.70; gen=-0.26; rebound_floor |
| DHR | +0.440 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| SYK | +0.440 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| WTW | +0.434 | Financial | join=+0.35; sector=-0.47; gen=-0.08; rebound_floor |
| TMO | +0.411 | Healthcare | join=+0.18; sector=+0.70; gen=-0.26; rebound_floor |
| OXY | +0.410 | Energy | join=+0.27; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| DVN | +0.410 | Energy | join=+0.27; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.400 | Energy | join=+0.24; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.400 | Energy | join=+0.24; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| MDT | +0.372 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |
| UTHR | +0.371 | Healthcare | sector=+0.70; gen=-0.08; rebound_floor |
| ACGL | +0.369 | Financial | join=+0.54; sector=-0.47; gen=-0.08 |
| COO | +0.367 | Healthcare | sector=+0.70; gen=-0.26; rebound_floor |
| EOG | +0.364 | Energy | join=+0.17; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| MRSH | +0.352 | Financial | join=+0.17; sector=-0.47; gen=-0.08; rebound_floor |
| HIG | +0.351 | Financial | join=+0.50; sector=-0.47; gen=-0.08 |
| CVX | +0.342 | Energy | join=+0.17; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| XOM | +0.342 | Energy | join=+0.17; sector=+0.65; gen=-0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| RMD | +0.325 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| ES | +0.322 | Utilities | join=+0.35; sector=+0.60; gen=-0.08 |
| CB | +0.319 | Financial | join=+0.43; sector=-0.47; gen=-0.08 |
| AFL | +0.319 | Financial | join=+0.43; sector=-0.47; gen=-0.08 |
| MUR | +0.315 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| BRK-B | +0.305 | Financial | join=+0.39; sector=-0.47; gen=-0.08 |

## 1w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TEAM | -0.497 | Technology | join=-0.53; sector=-0.64; gen=-0.26; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| TDUP | -0.491 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| VENU | -0.491 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| CWH | -0.491 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| EVGO | -0.489 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| CHPT | -0.489 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| LVWR | -0.472 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| NXH | -0.472 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| BROS | -0.472 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| TRIP | -0.472 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.26 |
| SNOW | -0.468 | Technology | join=-0.46; sector=-0.64; gen=-0.51; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| INVZ | -0.468 | Consumer Cyclical | join=-0.67; sector=-0.65; gen=-0.51 |
| XPOF | -0.468 | Consumer Cyclical | join=-0.67; sector=-0.65; gen=-0.26 |
| JMIA | -0.468 | Consumer Cyclical | join=-0.67; sector=-0.65; gen=-0.51 |
| VERI | -0.453 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| ADTN | -0.453 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| GPRO | -0.452 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| CSIQ | -0.452 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| SEDG | -0.452 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| UA | -0.451 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.51 |
| FOSL | -0.451 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.51 |
| UAA | -0.451 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.51 |
| FUN | -0.451 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.08 |
| CVGI | -0.451 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.51 |
| GT | -0.451 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.26 |

### 1w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.487 | Healthcare | join=+0.35; sector=+0.70; gen=-0.26; rebound_floor |
| EW | +0.450 | Healthcare | join=+0.27; sector=+0.70; gen=-0.26; rebound_floor |
| SYK | +0.440 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| DHR | +0.440 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| WTW | +0.434 | Financial | join=+0.35; sector=-0.47; gen=-0.08; rebound_floor |
| TMO | +0.411 | Healthcare | join=+0.18; sector=+0.70; gen=-0.26; rebound_floor |
| DVN | +0.410 | Energy | join=+0.27; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.410 | Energy | join=+0.27; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MUR | +0.315 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| CAI | +0.256 | Healthcare | join=-0.17; sector=+0.70; gen=-0.26; rebound_floor |
| TRMD | +0.254 | Energy | join=+0.20; sector=+0.65; gen=-0.08 |
| MLYS | +0.249 | Healthcare | join=-0.18; sector=+0.70; gen=-0.08; rebound_floor |
| TCBI | +0.247 | Financial | join=+0.27; sector=-0.47; gen=-0.08 |
| OTF | +0.240 | Financial | sector=-0.47; gen=-0.08; rebound_floor |
| FHB | +0.238 | Financial | join=+0.24; sector=-0.47; gen=-0.08 |
| XENE | +0.230 | Healthcare | sector=+0.70; gen=-0.08 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| INMD | +0.221 | Healthcare | sector=+0.70; gen=-0.51 |
| OBE | +0.210 | Energy | sector=+0.65; gen=-0.08 |
| SEER | +0.185 | Healthcare | join=-0.32; sector=+0.70; gen=-0.51; rebound_floor |
| TBPH | +0.180 | Healthcare | sector=+0.70; gen=-0.08 |
| CYPH | +0.173 | Healthcare | join=-0.35; sector=+0.70; gen=-0.08; rebound_floor |
| FINV | +0.165 | Financial | sector=-0.47; gen=-0.08 |
| AVXL | +0.159 | Healthcare | join=-0.38; sector=+0.70; gen=-0.26; rebound_floor |
| NOMD | +0.140 | Consumer Defensive | sector=+0.51; gen=-0.08 |

## 2w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.510 | Healthcare | join=+0.35; sector=+0.70; gen=-0.26; rebound_floor |
| EW | +0.470 | Healthcare | join=+0.27; sector=+0.70; gen=-0.26; rebound_floor |
| WTW | +0.466 | Financial | join=+0.35; sector=-0.47; gen=-0.08; rebound_floor |
| DHR | +0.460 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| SYK | +0.460 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| TMO | +0.429 | Healthcare | join=+0.18; sector=+0.70; gen=-0.26; rebound_floor |
| ACGL | +0.407 | Financial | join=+0.54; sector=-0.47; gen=-0.08 |
| MDT | +0.397 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |
| HIG | +0.387 | Financial | join=+0.50; sector=-0.47; gen=-0.08 |
| UTHR | +0.386 | Healthcare | sector=+0.70; gen=-0.08; rebound_floor |
| COO | +0.382 | Healthcare | sector=+0.70; gen=-0.26; rebound_floor |
| MRSH | +0.378 | Financial | join=+0.17; sector=-0.47; gen=-0.08; rebound_floor |
| OXY | +0.361 | Energy | join=+0.27; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| DVN | +0.361 | Energy | join=+0.27; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FAST | +0.355 | Industrials | join=+0.24; sector=+0.28; gen=-0.08; rebound_floor |
| CB | +0.353 | Financial | join=+0.43; sector=-0.47; gen=-0.08 |
| AFL | +0.353 | Financial | join=+0.43; sector=-0.47; gen=-0.08 |
| COP | +0.351 | Energy | join=+0.24; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.351 | Energy | join=+0.24; sector=+0.65; gen=-0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| RMD | +0.347 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| ES | +0.342 | Utilities | join=+0.35; sector=+0.60; gen=-0.08 |
| ECL | +0.339 | Basic Materials | sector=-0.47; gen=-0.26; rebound_floor |
| BRK-B | +0.338 | Financial | join=+0.39; sector=-0.47; gen=-0.08 |
| MUR | +0.325 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| ELV | +0.320 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |

## 2w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| VENU | -0.348 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| TDUP | -0.348 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| ANGI | -0.348 | Communication Services | join=-0.72; sector=-0.30; gen=-0.51 |
| VERI | -0.348 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| PLAY | -0.348 | Communication Services | join=-0.72; sector=-0.30; gen=-0.51 |
| KLC | -0.348 | Consumer Defensive | join=-0.72; sector=+0.51; gen=-0.51 |
| ADTN | -0.348 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| CWH | -0.348 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| CHPT | -0.346 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| GPRO | -0.346 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| SEDG | -0.346 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| IHRT | -0.346 | Communication Services | join=-0.72; sector=-0.30; gen=-0.51 |
| ODD | -0.346 | Consumer Defensive | join=-0.72; sector=+0.51; gen=-0.51 |
| CSIQ | -0.346 | Technology | join=-0.72; sector=-0.64; gen=-0.51 |
| EVGO | -0.346 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| QMLS | -0.341 | Technology | join=-0.71; sector=-0.64; gen=-0.20 |
| THRY | -0.327 | Technology | join=-0.68; sector=-0.64; gen=-0.26 |
| WOLF | -0.327 | Technology | join=-0.68; sector=-0.64; gen=-0.51 |
| BRCC | -0.327 | Consumer Defensive | join=-0.68; sector=+0.51; gen=-0.26 |
| LVWR | -0.327 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| GETY | -0.327 | Communication Services | join=-0.68; sector=-0.30; gen=-0.51 |
| NXH | -0.327 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| OPEN | -0.327 | Real Estate | join=-0.68; sector=-0.60; gen=-0.51 |
| BROS | -0.327 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| CNDT | -0.327 | Technology | join=-0.68; sector=-0.64; gen=-0.51 |

### 2w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.510 | Healthcare | join=+0.35; sector=+0.70; gen=-0.26; rebound_floor |
| EW | +0.470 | Healthcare | join=+0.27; sector=+0.70; gen=-0.26; rebound_floor |
| WTW | +0.466 | Financial | join=+0.35; sector=-0.47; gen=-0.08; rebound_floor |
| SYK | +0.460 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| DHR | +0.460 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| TMO | +0.429 | Healthcare | join=+0.18; sector=+0.70; gen=-0.26; rebound_floor |
| ACGL | +0.407 | Financial | join=+0.54; sector=-0.47; gen=-0.08 |
| MDT | +0.397 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MUR | +0.325 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| AOS | +0.317 | Industrials | join=+0.17; sector=+0.28; gen=-0.26; rebound_floor |
| TCBI | +0.276 | Financial | join=+0.27; sector=-0.47; gen=-0.08 |
| TRMD | +0.270 | Energy | join=+0.20; sector=+0.65; gen=-0.08 |
| FHB | +0.266 | Financial | join=+0.24; sector=-0.47; gen=-0.08 |
| CAI | +0.263 | Healthcare | join=-0.17; sector=+0.70; gen=-0.26; rebound_floor |
| OTF | +0.259 | Financial | sector=-0.47; gen=-0.08; rebound_floor |
| MLYS | +0.256 | Healthcare | join=-0.18; sector=+0.70; gen=-0.08; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| INMD | +0.236 | Healthcare | sector=+0.70; gen=-0.51 |
| OBE | +0.223 | Energy | sector=+0.65; gen=-0.08 |
| TBPH | +0.193 | Healthcare | sector=+0.70; gen=-0.08 |
| FINV | +0.189 | Financial | sector=-0.47; gen=-0.08 |
| SEER | +0.188 | Healthcare | join=-0.32; sector=+0.70; gen=-0.51; rebound_floor |
| CYPH | +0.175 | Healthcare | join=-0.35; sector=+0.70; gen=-0.08; rebound_floor |
| AVXL | +0.160 | Healthcare | join=-0.38; sector=+0.70; gen=-0.26; rebound_floor |
| QTTB | +0.149 | Healthcare | sector=+0.70; gen=-0.08 |

## 1m — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.565 | Healthcare | join=+0.35; sector=+0.70; gen=-0.26; rebound_floor |
| EW | +0.524 | Healthcare | join=+0.27; sector=+0.70; gen=-0.26; rebound_floor |
| WTW | +0.501 | Financial | join=+0.35; sector=-0.47; gen=-0.08; rebound_floor |
| DHR | +0.499 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| SYK | +0.499 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| TMO | +0.481 | Healthcare | join=+0.18; sector=+0.70; gen=-0.26; rebound_floor |
| ACGL | +0.445 | Financial | join=+0.54; sector=-0.47; gen=-0.08 |
| MDT | +0.439 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |
| COO | +0.433 | Healthcare | sector=+0.70; gen=-0.26; rebound_floor |
| HIG | +0.425 | Financial | join=+0.50; sector=-0.47; gen=-0.08 |
| UTHR | +0.422 | Healthcare | sector=+0.70; gen=-0.08; rebound_floor |
| MRSH | +0.409 | Financial | join=+0.17; sector=-0.47; gen=-0.08; rebound_floor |
| ACN | +0.404 | Technology | join=+0.43; sector=-0.64; gen=-0.26 |
| CB | +0.389 | Financial | join=+0.43; sector=-0.47; gen=-0.08 |
| AFL | +0.389 | Financial | join=+0.43; sector=-0.47; gen=-0.08 |
| FAST | +0.389 | Industrials | join=+0.24; sector=+0.28; gen=-0.08; rebound_floor |
| RMD | +0.387 | Healthcare | join=+0.32; sector=+0.70; gen=-0.08 |
| ECL | +0.383 | Basic Materials | sector=-0.47; gen=-0.26; rebound_floor |
| ES | +0.381 | Utilities | join=+0.35; sector=+0.60; gen=-0.08 |
| WST | +0.374 | Healthcare | join=+0.27; sector=+0.70; gen=-0.26 |
| BRK-B | +0.373 | Financial | join=+0.39; sector=-0.47; gen=-0.08 |
| AOS | +0.364 | Industrials | join=+0.17; sector=+0.28; gen=-0.26; rebound_floor |
| GGG | +0.364 | Industrials | join=+0.17; sector=+0.28; gen=-0.26; rebound_floor |
| TECH | +0.364 | Healthcare | join=+0.24; sector=+0.70; gen=-0.26 |
| ZBH | +0.359 | Healthcare | join=+0.27; sector=+0.70; gen=-0.08 |

## 1m — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DNUT | -0.320 | Consumer Defensive | join=-0.68; sector=+0.51; gen=-0.26 |
| BRCC | -0.320 | Consumer Defensive | join=-0.68; sector=+0.51; gen=-0.26 |
| GOGO | -0.320 | Communication Services | join=-0.68; sector=-0.30; gen=-0.26 |
| TRIP | -0.320 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.26 |
| ANGI | -0.320 | Communication Services | join=-0.72; sector=-0.30; gen=-0.51 |
| VENU | -0.320 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| CWH | -0.320 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| TDUP | -0.320 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| KLC | -0.320 | Consumer Defensive | join=-0.72; sector=+0.51; gen=-0.51 |
| PLAY | -0.320 | Communication Services | join=-0.72; sector=-0.30; gen=-0.51 |
| IHRT | -0.318 | Communication Services | join=-0.72; sector=-0.30; gen=-0.51 |
| ODD | -0.318 | Consumer Defensive | join=-0.72; sector=+0.51; gen=-0.51 |
| EVGO | -0.318 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| CHPT | -0.318 | Consumer Cyclical | join=-0.72; sector=-0.65; gen=-0.51 |
| XPOF | -0.316 | Consumer Cyclical | join=-0.67; sector=-0.65; gen=-0.26 |
| FUN | -0.311 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.08 |
| OI | -0.311 | Consumer Cyclical | join=-0.64; sector=-0.65; gen=-0.08 |
| PLTK | -0.311 | Communication Services | join=-0.66; sector=-0.30; gen=-0.26 |
| GETY | -0.299 | Communication Services | join=-0.68; sector=-0.30; gen=-0.51 |
| NXH | -0.299 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| BROS | -0.299 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| RC | -0.299 | Real Estate | join=-0.68; sector=-0.60; gen=-0.51 |
| LVWR | -0.299 | Consumer Cyclical | join=-0.68; sector=-0.65; gen=-0.51 |
| OPEN | -0.299 | Real Estate | join=-0.68; sector=-0.60; gen=-0.51 |
| UTI | -0.296 | Consumer Defensive | join=-0.64; sector=+0.51; gen=-0.26 |

### 1m — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| STE | +0.565 | Healthcare | join=+0.35; sector=+0.70; gen=-0.26; rebound_floor |
| EW | +0.524 | Healthcare | join=+0.27; sector=+0.70; gen=-0.26; rebound_floor |
| WTW | +0.501 | Financial | join=+0.35; sector=-0.47; gen=-0.08; rebound_floor |
| DHR | +0.499 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| SYK | +0.499 | Healthcare | join=+0.24; sector=+0.70; gen=-0.08; rebound_floor |
| TMO | +0.481 | Healthcare | join=+0.18; sector=+0.70; gen=-0.26; rebound_floor |
| ACGL | +0.445 | Financial | join=+0.54; sector=-0.47; gen=-0.08 |
| MDT | +0.439 | Healthcare | join=+0.43; sector=+0.70; gen=-0.08 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AOS | +0.364 | Industrials | join=+0.17; sector=+0.28; gen=-0.26; rebound_floor |
| MUR | +0.336 | Energy | sector=+0.65; gen=-0.08; rebound_floor |
| TCBI | +0.309 | Financial | join=+0.27; sector=-0.47; gen=-0.08 |
| NICE | +0.309 | Technology | join=+0.27; sector=-0.64; gen=-0.08 |
| CAI | +0.309 | Healthcare | join=-0.17; sector=+0.70; gen=-0.26; rebound_floor |
| FHB | +0.299 | Financial | join=+0.24; sector=-0.47; gen=-0.08 |
| ERO | +0.291 | Basic Materials | sector=-0.47; gen=-0.26; rebound_floor |
| MLYS | +0.286 | Healthcare | join=-0.18; sector=+0.70; gen=-0.08; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| INMD | +0.308 | Healthcare | sector=+0.70; gen=-0.51 |
| SEER | +0.252 | Healthcare | join=-0.32; sector=+0.70; gen=-0.51; rebound_floor |
| OBE | +0.236 | Energy | sector=+0.65; gen=-0.08 |
| TBPH | +0.226 | Healthcare | sector=+0.70; gen=-0.08 |
| TUYA | +0.218 | Technology | sector=-0.64; gen=-0.08 |
| FINV | +0.218 | Financial | sector=-0.47; gen=-0.08 |
| CYPH | +0.202 | Healthcare | join=-0.35; sector=+0.70; gen=-0.08; rebound_floor |
| AVXL | +0.201 | Healthcare | join=-0.38; sector=+0.70; gen=-0.26; rebound_floor |

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
