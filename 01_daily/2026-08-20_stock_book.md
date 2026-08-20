# Stock book — **2026-08-20** (1d / 3d / 1w / 2w / 1m)

Generated: 2026-08-20T13:06:18.499575-04:00

Layers: join (labels×weather) + sector predict + general regime + news actions.

## Regime snapshot

- **General bias:** +0.47
- **Weather risk:** mixed
- **News tickers:** 34
- **Universe (after liquidity):** 2707
- **Gates:** mcap ≥ $80.0M, avg vol ≥ 500.0k
- **Rebound floor tags:** 88

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
| general | 53% | 17 | ×0.85 |
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
| EOG | +0.467 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| DVN | +0.467 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.467 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.438 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.438 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.438 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| NRG | +0.385 | Utilities | sector=+0.60; gen=+0.23; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| CEG | +0.385 | Utilities | sector=+0.60; gen=+0.23; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| VST | +0.379 | Utilities | sector=+0.60; gen=+0.47; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| TLN | +0.379 | Utilities | sector=+0.60; gen=+0.47; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| XOM | +0.379 | Energy | sector=+0.65; gen=+0.07; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| CVX | +0.379 | Energy | sector=+0.65; gen=+0.07; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| NEE | +0.339 | Utilities | sector=+0.60; gen=+0.07; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| TEM | +0.302 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| HIMS | +0.302 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| GRAL | +0.302 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| NEO | +0.302 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| ELF | +0.287 | Consumer Defensive | sector=+0.60; gen=+0.47; rebound_floor |
| IQV | +0.278 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| CAI | +0.278 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| STE | +0.278 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| COO | +0.278 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| NVCR | +0.278 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| TMO | +0.278 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| HALO | +0.278 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |

## 1d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| BXP | -0.316 | Real Estate | sector=-0.60; gen=+0.23; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| TEAM | -0.294 | Technology | sector=-0.75; gen=+0.23; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| WDAY | -0.294 | Technology | sector=-0.75; gen=+0.23; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| CRM | -0.294 | Technology | sector=-0.75; gen=+0.23; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| VNO | -0.292 | Real Estate | sector=-0.60; gen=+0.47; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| SLG | -0.292 | Real Estate | sector=-0.60; gen=+0.47; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| AEM | -0.280 | Basic Materials | sector=-0.55; gen=+0.07; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'gold'} |
| ADBE | -0.271 | Technology | sector=-0.75; gen=+0.47; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| SNOW | -0.271 | Technology | sector=-0.75; gen=+0.47; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| NOW | -0.265 | Technology | sector=-0.75; gen=+0.23; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| NEM | -0.251 | Basic Materials | sector=-0.55; gen=+0.07; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'gold'} |
| ALK | -0.210 | Industrials | sector=+0.28; gen=+0.23; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| LYNX | -0.191 | Technology | join=-0.28; sector=-0.75; gen=+0.19 |
| AAL | -0.186 | Industrials | sector=+0.28; gen=+0.47; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| LUV | -0.181 | Industrials | sector=+0.28; gen=+0.23; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| UAL | -0.181 | Industrials | sector=+0.28; gen=+0.23; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| ADIG | -0.180 | Technology | join=-0.24; sector=-0.75; gen=+0.19 |
| HQ | -0.175 | Technology | join=-0.20; sector=-0.75; gen=+0.07 |
| QMLS | -0.170 | Technology | join=-0.22; sector=-0.75; gen=+0.19 |
| GRRR | -0.168 | Technology | join=-0.18; sector=-0.75; gen=+0.07 |
| IQMX | -0.168 | Technology | join=-0.18; sector=-0.75; gen=+0.07 |
| QNC | -0.168 | Technology | join=-0.18; sector=-0.75; gen=+0.07 |
| KDK | -0.163 | Technology | join=-0.17; sector=-0.75; gen=+0.07 |
| OCC | -0.163 | Technology | join=-0.17; sector=-0.75; gen=+0.07 |
| NABL | -0.163 | Technology | join=-0.17; sector=-0.75; gen=+0.07 |

### 1d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DVN | +0.467 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.467 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.467 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.438 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.438 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.438 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| NRG | +0.385 | Utilities | sector=+0.60; gen=+0.23; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| CEG | +0.385 | Utilities | sector=+0.60; gen=+0.23; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| HIMS | +0.302 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| GRAL | +0.302 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| NEO | +0.302 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| ELF | +0.287 | Consumer Defensive | sector=+0.60; gen=+0.47; rebound_floor |
| NVCR | +0.278 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| CAI | +0.278 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| BLLN | +0.274 | Healthcare | sector=+0.70; gen=+0.19; rebound_floor |
| DNN | +0.271 | Energy | sector=+0.65; gen=+0.23; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| SEER | +0.244 | Healthcare | join=-0.17; sector=+0.70; gen=+0.47; rebound_floor |
| PRME | +0.244 | Healthcare | join=-0.17; sector=+0.70; gen=+0.47; rebound_floor |
| SANA | +0.239 | Healthcare | join=-0.18; sector=+0.70; gen=+0.47; rebound_floor |
| SDGR | +0.239 | Healthcare | join=-0.18; sector=+0.70; gen=+0.47; rebound_floor |
| VSTM | +0.233 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| GERN | +0.233 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| CYPH | +0.230 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| OTLK | +0.230 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |

## 3d — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DVN | +0.392 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.392 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.392 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.358 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.358 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.358 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| CEG | +0.335 | Utilities | sector=+0.60; gen=+0.23; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| NRG | +0.335 | Utilities | sector=+0.60; gen=+0.23; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| XOM | +0.321 | Energy | sector=+0.65; gen=+0.07; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| CVX | +0.321 | Energy | sector=+0.65; gen=+0.07; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| VST | +0.301 | Utilities | sector=+0.60; gen=+0.47; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| NEE | +0.301 | Utilities | sector=+0.60; gen=+0.07; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| TLN | +0.301 | Utilities | sector=+0.60; gen=+0.47; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| AMLX | +0.300 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| DNN | +0.300 | Energy | sector=+0.65; gen=+0.23; rebound_floor |
| TEM | +0.300 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| EW | +0.300 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| PTCT | +0.300 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| CWEN | +0.300 | Utilities | sector=+0.60; gen=+0.23; rebound_floor |
| DHR | +0.300 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| CAI | +0.300 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| NVCR | +0.300 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| MLYS | +0.300 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| COO | +0.300 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| BLLN | +0.300 | Healthcare | sector=+0.70; gen=+0.19; rebound_floor |

## 3d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| SLG | -0.293 | Real Estate | sector=-0.60; gen=+0.47; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| VNO | -0.293 | Real Estate | sector=-0.60; gen=+0.47; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| BXP | -0.293 | Real Estate | sector=-0.60; gen=+0.23; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| WDAY | -0.266 | Technology | sector=-0.75; gen=+0.23; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| TEAM | -0.266 | Technology | sector=-0.75; gen=+0.23; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| SNOW | -0.266 | Technology | sector=-0.75; gen=+0.47; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| CRM | -0.266 | Technology | sector=-0.75; gen=+0.23; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| ADBE | -0.266 | Technology | sector=-0.75; gen=+0.47; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| AEM | -0.253 | Basic Materials | sector=-0.55; gen=+0.07; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'gold'} |
| LYNX | -0.249 | Technology | join=-0.28; sector=-0.75; gen=+0.19 |
| REF | -0.237 | Consumer Cyclical | join=-0.22; sector=-0.65; gen=+0.19 |
| MWC | -0.237 | Consumer Cyclical | join=-0.22; sector=-0.65; gen=+0.19 |
| SMJF | -0.237 | Consumer Cyclical | join=-0.22; sector=-0.65; gen=+0.19 |
| ADIG | -0.235 | Technology | join=-0.24; sector=-0.75; gen=+0.19 |
| AAL | -0.234 | Industrials | sector=+0.28; gen=+0.47; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| ALK | -0.234 | Industrials | sector=+0.28; gen=+0.23; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| NOW | -0.232 | Technology | sector=-0.75; gen=+0.23; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| AIIO | -0.229 | Consumer Cyclical | join=-0.20; sector=-0.65; gen=+0.47 |
| QMLS | -0.225 | Technology | join=-0.22; sector=-0.75; gen=+0.19 |
| STDN | -0.223 | Basic Materials | join=-0.24; sector=-0.55; gen=+0.19 |
| THM | -0.223 | Basic Materials | join=-0.24; sector=-0.55; gen=+0.23 |
| BJRI | -0.222 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.47 |
| MB | -0.222 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.47 |
| PZZA | -0.222 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.23 |
| JACK | -0.222 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.47 |

### 3d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| EOG | +0.392 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| DVN | +0.392 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.392 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.358 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.358 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.358 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| NRG | +0.335 | Utilities | sector=+0.60; gen=+0.23; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| CEG | +0.335 | Utilities | sector=+0.60; gen=+0.23; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| GRAL | +0.300 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| HIMS | +0.300 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| BLLN | +0.300 | Healthcare | sector=+0.70; gen=+0.19; rebound_floor |
| DNN | +0.300 | Energy | sector=+0.65; gen=+0.23; rebound_floor |
| PTCT | +0.300 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| CAI | +0.300 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| AMLX | +0.300 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| NEO | +0.300 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| GERN | +0.267 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| VSTM | +0.267 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| OTLK | +0.264 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| CYPH | +0.264 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| PRME | +0.234 | Healthcare | join=-0.17; sector=+0.70; gen=+0.47; rebound_floor |
| SEER | +0.234 | Healthcare | join=-0.17; sector=+0.70; gen=+0.47; rebound_floor |
| LXRX | +0.228 | Healthcare | join=-0.18; sector=+0.70; gen=+0.23; rebound_floor |
| SDGR | +0.228 | Healthcare | join=-0.18; sector=+0.70; gen=+0.47; rebound_floor |

## 1w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MLYS | +0.330 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| PTCT | +0.330 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| TMO | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| DHR | +0.330 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| EW | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| TEM | +0.330 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| HIMS | +0.330 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| COO | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| BLLN | +0.330 | Healthcare | sector=+0.70; gen=+0.19; rebound_floor |
| HALO | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| NVCR | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| SYK | +0.330 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| GRAL | +0.330 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| IQV | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| CAI | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| NEO | +0.330 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| STE | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| AMLX | +0.330 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| DVN | +0.327 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.327 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.327 | Energy | sector=+0.65; gen=+0.07; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EL | +0.315 | Consumer Defensive | sector=+0.60; gen=+0.23; rebound_floor |
| OLLI | +0.315 | Consumer Defensive | sector=+0.60; gen=+0.07; rebound_floor |
| CWEN | +0.315 | Utilities | sector=+0.60; gen=+0.23; rebound_floor |
| DNN | +0.315 | Energy | sector=+0.65; gen=+0.23; rebound_floor |

## 1w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| LYNX | -0.275 | Technology | join=-0.28; sector=-0.75; gen=+0.19 |
| SMJF | -0.263 | Consumer Cyclical | join=-0.22; sector=-0.65; gen=+0.19 |
| MWC | -0.263 | Consumer Cyclical | join=-0.22; sector=-0.65; gen=+0.19 |
| REF | -0.263 | Consumer Cyclical | join=-0.22; sector=-0.65; gen=+0.19 |
| ADIG | -0.260 | Technology | join=-0.24; sector=-0.75; gen=+0.19 |
| AIIO | -0.254 | Consumer Cyclical | join=-0.20; sector=-0.65; gen=+0.47 |
| QMLS | -0.248 | Technology | join=-0.22; sector=-0.75; gen=+0.19 |
| ARHS | -0.246 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.47 |
| GOOS | -0.246 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.47 |
| PZZA | -0.246 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.23 |
| BJRI | -0.246 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.47 |
| BLMN | -0.246 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.23 |
| MB | -0.246 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.47 |
| JACK | -0.246 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.47 |
| CPRI | -0.246 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.47 |
| GIII | -0.246 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.23 |
| ARCO | -0.246 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.07 |
| SG | -0.239 | Consumer Cyclical | join=-0.17; sector=-0.65; gen=+0.47 |
| SLDP | -0.239 | Consumer Cyclical | join=-0.17; sector=-0.65; gen=+0.47 |
| FNKO | -0.239 | Consumer Cyclical | join=-0.17; sector=-0.65; gen=+0.23 |
| ETD | -0.239 | Consumer Cyclical | join=-0.17; sector=-0.65; gen=+0.23 |
| WWW | -0.239 | Consumer Cyclical | join=-0.17; sector=-0.65; gen=+0.47 |
| KSS | -0.239 | Consumer Cyclical | join=-0.17; sector=-0.65; gen=+0.47 |
| MLKN | -0.239 | Consumer Cyclical | join=-0.17; sector=-0.65; gen=+0.47 |
| HLLY | -0.239 | Consumer Cyclical | join=-0.17; sector=-0.65; gen=+0.47 |

### 1w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMO | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| IQV | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| TEM | +0.330 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| STE | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| EW | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| COO | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| HALO | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| DHR | +0.330 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| GRAL | +0.330 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| HIMS | +0.330 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| MLYS | +0.330 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| AMLX | +0.330 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| NVCR | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| BLLN | +0.330 | Healthcare | sector=+0.70; gen=+0.19; rebound_floor |
| NEO | +0.330 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| CAI | +0.330 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| VSTM | +0.293 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| GERN | +0.293 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| CYPH | +0.289 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| OTLK | +0.289 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| SEER | +0.256 | Healthcare | join=-0.17; sector=+0.70; gen=+0.47; rebound_floor |
| PRME | +0.256 | Healthcare | join=-0.17; sector=+0.70; gen=+0.47; rebound_floor |
| LXRX | +0.249 | Healthcare | join=-0.18; sector=+0.70; gen=+0.23; rebound_floor |
| TSHA | +0.249 | Healthcare | join=-0.18; sector=+0.70; gen=+0.23; rebound_floor |

## 2w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MOS | +0.386 | Basic Materials | sector=-0.55; gen=+0.23; rebound_floor |
| HIMS | +0.385 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| NEO | +0.385 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| GRAL | +0.385 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| TEM | +0.385 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| TGB | +0.367 | Basic Materials | sector=-0.55; gen=+0.47; rebound_floor |
| HALO | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| TMO | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| STE | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| IQV | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| NVCR | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| CAI | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| COO | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| EW | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| BLLN | +0.360 | Healthcare | sector=+0.70; gen=+0.19; rebound_floor |
| SYK | +0.349 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| DHR | +0.349 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| MLYS | +0.349 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| PTCT | +0.349 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| AMLX | +0.349 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| ECL | +0.346 | Basic Materials | sector=-0.55; gen=+0.23; rebound_floor |
| CRH | +0.346 | Basic Materials | sector=-0.55; gen=+0.23; rebound_floor |
| ERO | +0.346 | Basic Materials | sector=-0.55; gen=+0.23; rebound_floor |
| CWEN | +0.346 | Utilities | sector=+0.60; gen=+0.23; rebound_floor |
| DNN | +0.346 | Energy | sector=+0.65; gen=+0.23; rebound_floor |

## 2w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| LYNX | -0.117 | Technology | join=-0.28; sector=-0.75; gen=+0.19 |
| ADIG | -0.101 | Technology | join=-0.24; sector=-0.75; gen=+0.19 |
| GCL | -0.088 | Communication Services | join=-0.20; sector=-0.30; gen=+0.07 |
| HQ | -0.088 | Technology | join=-0.20; sector=-0.75; gen=+0.07 |
| REF | -0.088 | Consumer Cyclical | join=-0.22; sector=-0.65; gen=+0.19 |
| SMJF | -0.088 | Consumer Cyclical | join=-0.22; sector=-0.65; gen=+0.19 |
| QMLS | -0.088 | Technology | join=-0.22; sector=-0.75; gen=+0.19 |
| MWC | -0.088 | Consumer Cyclical | join=-0.22; sector=-0.65; gen=+0.19 |
| GRRR | -0.080 | Technology | join=-0.18; sector=-0.75; gen=+0.07 |
| IQMX | -0.080 | Technology | join=-0.18; sector=-0.75; gen=+0.07 |
| DOLE | -0.080 | Consumer Defensive | join=-0.18; sector=+0.60; gen=+0.07 |
| CCOI | -0.080 | Communication Services | join=-0.18; sector=-0.30; gen=+0.07 |
| GLIBK | -0.080 | Communication Services | join=-0.18; sector=-0.30; gen=+0.07 |
| QNC | -0.080 | Technology | join=-0.18; sector=-0.75; gen=+0.07 |
| NOMD | -0.080 | Consumer Defensive | join=-0.18; sector=+0.60; gen=+0.07 |
| AGRO | -0.080 | Consumer Defensive | join=-0.18; sector=+0.60; gen=+0.07 |
| ARCO | -0.080 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.07 |
| CAST | -0.078 | Communication Services | join=-0.20; sector=-0.30; gen=+0.19 |
| NABL | -0.073 | Technology | join=-0.17; sector=-0.75; gen=+0.07 |
| KDK | -0.073 | Technology | join=-0.17; sector=-0.75; gen=+0.07 |
| FLO | -0.073 | Consumer Defensive | join=-0.17; sector=+0.60; gen=+0.07 |
| VRRM | -0.073 | Technology | join=-0.17; sector=-0.75; gen=+0.07 |
| CXM | -0.073 | Technology | join=-0.17; sector=-0.75; gen=+0.07 |
| BL | -0.073 | Technology | join=-0.17; sector=-0.75; gen=+0.07 |
| WALD | -0.073 | Consumer Defensive | join=-0.17; sector=+0.60; gen=+0.07 |

### 2w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TEM | +0.385 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| TMO | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| HALO | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| COO | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| STE | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| IQV | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| EW | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| DHR | +0.349 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MOS | +0.386 | Basic Materials | sector=-0.55; gen=+0.23; rebound_floor |
| HIMS | +0.385 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| GRAL | +0.385 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| NEO | +0.385 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| TGB | +0.367 | Basic Materials | sector=-0.55; gen=+0.47; rebound_floor |
| CAI | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| NVCR | +0.364 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| BLLN | +0.360 | Healthcare | sector=+0.70; gen=+0.19; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.314 | Basic Materials | sector=-0.55; gen=+0.47; rebound_floor |
| VSTM | +0.309 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| GERN | +0.309 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| SEER | +0.306 | Healthcare | join=-0.17; sector=+0.70; gen=+0.47; rebound_floor |
| PRME | +0.306 | Healthcare | join=-0.17; sector=+0.70; gen=+0.47; rebound_floor |
| OTLK | +0.305 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| CYPH | +0.305 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| SDGR | +0.299 | Healthcare | join=-0.18; sector=+0.70; gen=+0.47; rebound_floor |

## 1m — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MOS | +0.413 | Basic Materials | sector=-0.55; gen=+0.23; rebound_floor |
| HIMS | +0.412 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| TEM | +0.412 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| NEO | +0.412 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| GRAL | +0.412 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| TGB | +0.393 | Basic Materials | sector=-0.55; gen=+0.47; rebound_floor |
| IQV | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| NVCR | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| TMO | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| STE | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| HALO | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| EW | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| CAI | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| COO | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| BLLN | +0.387 | Healthcare | sector=+0.70; gen=+0.19; rebound_floor |
| SYK | +0.376 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| PTCT | +0.376 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| MLYS | +0.376 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| DHR | +0.376 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| AMLX | +0.376 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| ECL | +0.371 | Basic Materials | sector=-0.55; gen=+0.23; rebound_floor |
| CWEN | +0.371 | Utilities | sector=+0.60; gen=+0.23; rebound_floor |
| ERO | +0.371 | Basic Materials | sector=-0.55; gen=+0.23; rebound_floor |
| CRH | +0.371 | Basic Materials | sector=-0.55; gen=+0.23; rebound_floor |
| DNN | +0.351 | Energy | sector=+0.65; gen=+0.23; rebound_floor |

## 1m — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| GCL | -0.092 | Communication Services | join=-0.20; sector=-0.30; gen=+0.07 |
| SMJF | -0.092 | Consumer Cyclical | join=-0.22; sector=-0.65; gen=+0.19 |
| REF | -0.092 | Consumer Cyclical | join=-0.22; sector=-0.65; gen=+0.19 |
| MWC | -0.092 | Consumer Cyclical | join=-0.22; sector=-0.65; gen=+0.19 |
| NOMD | -0.084 | Consumer Defensive | join=-0.18; sector=+0.60; gen=+0.07 |
| GLIBK | -0.084 | Communication Services | join=-0.18; sector=-0.30; gen=+0.07 |
| CCOI | -0.084 | Communication Services | join=-0.18; sector=-0.30; gen=+0.07 |
| AGRO | -0.084 | Consumer Defensive | join=-0.18; sector=+0.60; gen=+0.07 |
| DOLE | -0.084 | Consumer Defensive | join=-0.18; sector=+0.60; gen=+0.07 |
| ARCO | -0.084 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.07 |
| CAST | -0.082 | Communication Services | join=-0.20; sector=-0.30; gen=+0.19 |
| SMPL | -0.076 | Consumer Defensive | join=-0.17; sector=+0.60; gen=+0.07 |
| WEN | -0.076 | Consumer Cyclical | join=-0.17; sector=-0.65; gen=+0.07 |
| WALD | -0.076 | Consumer Defensive | join=-0.17; sector=+0.60; gen=+0.07 |
| FLO | -0.076 | Consumer Defensive | join=-0.17; sector=+0.60; gen=+0.07 |
| EPC | -0.076 | Consumer Defensive | join=-0.17; sector=+0.60; gen=+0.07 |
| LEG | -0.076 | Consumer Cyclical | join=-0.17; sector=-0.65; gen=+0.07 |
| BGS | -0.076 | Consumer Defensive | join=-0.17; sector=+0.60; gen=+0.07 |
| NHP | -0.073 | Real Estate | join=-0.18; sector=-0.60; gen=+0.19 |
| BLMN | -0.069 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.23 |
| INN | -0.069 | Real Estate | join=-0.18; sector=-0.60; gen=+0.23 |
| GIII | -0.069 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.23 |
| PZZA | -0.069 | Consumer Cyclical | join=-0.18; sector=-0.65; gen=+0.23 |
| ETD | -0.061 | Consumer Cyclical | join=-0.17; sector=-0.65; gen=+0.23 |
| SITC | -0.061 | Real Estate | join=-0.17; sector=-0.60; gen=+0.23 |

### 1m — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TEM | +0.412 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| TMO | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| HALO | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| EW | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| STE | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| COO | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| IQV | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| DHR | +0.376 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MOS | +0.413 | Basic Materials | sector=-0.55; gen=+0.23; rebound_floor |
| NEO | +0.412 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| HIMS | +0.412 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| GRAL | +0.412 | Healthcare | sector=+0.70; gen=+0.47; rebound_floor |
| TGB | +0.393 | Basic Materials | sector=-0.55; gen=+0.47; rebound_floor |
| CAI | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| NVCR | +0.391 | Healthcare | sector=+0.70; gen=+0.23; rebound_floor |
| BLLN | +0.387 | Healthcare | sector=+0.70; gen=+0.19; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.337 | Basic Materials | sector=-0.55; gen=+0.47; rebound_floor |
| GERN | +0.335 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| VSTM | +0.335 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| CYPH | +0.331 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| OTLK | +0.331 | Healthcare | sector=+0.70; gen=+0.07; rebound_floor |
| SEER | +0.330 | Healthcare | join=-0.17; sector=+0.70; gen=+0.47; rebound_floor |
| PRME | +0.330 | Healthcare | join=-0.17; sector=+0.70; gen=+0.47; rebound_floor |
| SDGR | +0.323 | Healthcare | join=-0.18; sector=+0.70; gen=+0.47; rebound_floor |

## Read

- **1d** news-heavy; **1m** structural join + sector.
- Universe gated: Market Cap ≥ $80M and Average Volume ≥ 500k shares (Finviz units).
- `rebound_floor` = checklist own-history score low + green-body bias (sparse; soft boost only).
- Raw checklist total score is NOT used as a buy rank (failed forward IC).
- Longer horizons use the predictors' explicit 3d/1w/2w/1m calls when stored, else fall back to the 1d call.
- Predictor bias is scaled by its graded hit rate (learning gate) — weak topics move scores less.
- Backtest: `python -m src.stock_book_backtest` (or Stock Book Backtest action).

CSV: `data/stock_book/2026-08-20_stock_book.csv`
JSON: `data/stock_book/2026-08-20_stock_book.json`
