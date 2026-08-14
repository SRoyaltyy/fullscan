# Stock book — **2026-08-14** (1d / 3d / 1w / 2w / 1m)

Generated: 2026-08-14T06:54:38.186278-04:00

Layers: join (labels×weather) + sector predict + general regime + news actions.

## Regime snapshot

- **General bias:** +0.55
- **Weather risk:** unknown
- **News tickers:** 32
- **Universe:** 11586

### Sector bias

| Sector | bias |
|--------|------|
| Basic Materials | -0.60 |
| Real Estate | +0.60 |
| Utilities | +0.60 |
| Technology | +0.55 |
| Consumer Cyclical | -0.55 |
| Financial | +0.55 |
| Consumer Defensive | +0.51 |
| Healthcare | -0.51 |
| Energy | +0.47 |
| Communication Services | +0.30 |
| Industrials | +0.28 |

### Learning gate (graded accuracy → how much each predictor is trusted)

Bias from a topic with a weak graded track record is scaled down before it can move scores.

| Topic | hit rate | graded runs | weight applied |
|-------|----------|-------------|----------------|
| general | 62% | 13 | ×1.00 |
| sector:Basic Materials | 75% | 4 | ×1.00 |
| sector:Communication Services | 25% | 4 | ×0.50 |
| sector:Consumer Cyclical | 75% | 4 | ×1.00 |
| sector:Consumer Defensive | 50% | 4 | ×0.85 |
| sector:Energy | 50% | 4 | ×0.85 |
| sector:Financial | 75% | 4 | ×1.00 |
| sector:Healthcare | 50% | 4 | ×0.85 |
| sector:Industrials | 25% | 4 | ×0.50 |
| sector:Real Estate | 75% | 4 | ×1.00 |
| sector:Technology | 50% | 4 | ×0.85 |
| sector:Utilities | 75% | 4 | ×1.00 |

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
| COP | +0.429 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| OXY | +0.429 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| FANG | +0.429 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| EOG | +0.429 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| APA | +0.429 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| DVN | +0.429 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| DUO | +0.412 | Real Estate | join=+0.76; sector=+0.60; gen=+0.55 |
| PETZ | +0.412 | Real Estate | join=+0.76; sector=+0.60; gen=+0.55 |
| AIOS | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| KC | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YAAS | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| LGCL | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| PONY | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| LHSW | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| JZ | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YIBO | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| WETO | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| NXTT | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| BIYA | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| EBON | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YXT | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| HKIT | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| AIXI | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YMT | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| WRD | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |

## 1d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| UAL | -0.228 | Industrials | sector=+0.28; gen=+0.28; news=-0.74; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.76, 'bucket': 'airlines'} |
| ALK | -0.228 | Industrials | sector=+0.28; gen=+0.28; news=-0.74; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.76, 'bucket': 'airlines'} |
| LUV | -0.228 | Industrials | sector=+0.28; gen=+0.28; news=-0.74; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.76, 'bucket': 'airlines'} |
| DAL | -0.200 | Industrials | sector=+0.28; gen=+0.55; news=-0.74; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.76, 'bucket': 'airlines'} |
| AAL | -0.200 | Industrials | sector=+0.28; gen=+0.55; news=-0.74; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.76, 'bucket': 'airlines'} |
| SEDG | -0.145 | Technology | sector=+0.55; gen=+0.55; news=-0.71; ev={'event': 'tariff_semis_solar', 'side': 'sell', 'weight': 4.41, 'bucket': 'solar'} |
| ENPH | -0.145 | Technology | sector=+0.55; gen=+0.55; news=-0.71; ev={'event': 'tariff_semis_solar', 'side': 'sell', 'weight': 4.41, 'bucket': 'solar'} |
| FSLR | -0.145 | Technology | sector=+0.55; gen=+0.55; news=-0.71; ev={'event': 'tariff_semis_solar', 'side': 'sell', 'weight': 4.41, 'bucket': 'solar'} |
| RUN | -0.145 | Technology | sector=+0.55; gen=+0.55; news=-0.71; ev={'event': 'tariff_semis_solar', 'side': 'sell', 'weight': 4.41, 'bucket': 'solar'} |
| TFPM | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| SNES | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| NAK | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| TROX | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| TII | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| FNV | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| FNUC | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| NB | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| TRX | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| ORLA | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| NEU | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| OR | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| COPR | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| AUGO | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| PPTA | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| SXT | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |

### 1d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| OXY | +0.429 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| FANG | +0.429 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| APA | +0.429 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| EOG | +0.429 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| COP | +0.429 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| DVN | +0.429 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| VST | +0.387 | Utilities | sector=+0.60; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| TLN | +0.387 | Utilities | sector=+0.60; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| KC | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| PONY | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| GDS | +0.358 | Technology | join=+0.76; sector=+0.55; gen=+0.08 |
| VNET | +0.358 | Technology | join=+0.76; sector=+0.55; gen=+0.08 |
| YMM | +0.358 | Technology | join=+0.76; sector=+0.55; gen=+0.08 |
| MAAS | +0.357 | Financial | join=+0.76; sector=+0.55; gen=+0.08 |
| EDU | +0.351 | Consumer Defensive | join=+0.76; sector=+0.51; gen=+0.08 |
| TAL | +0.351 | Consumer Defensive | join=+0.76; sector=+0.51; gen=+0.08 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| PETZ | +0.412 | Real Estate | join=+0.76; sector=+0.60; gen=+0.55 |
| DUO | +0.412 | Real Estate | join=+0.76; sector=+0.60; gen=+0.55 |
| LGCL | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| LHSW | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YAAS | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| MASK | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| AIXI | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| HKIT | +0.404 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |

## 3d — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DUO | +0.510 | Real Estate | join=+0.76; sector=+0.60; gen=+0.55 |
| PETZ | +0.510 | Real Estate | join=+0.76; sector=+0.60; gen=+0.55 |
| NCTY | +0.497 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| RDAC | +0.497 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| DXF | +0.497 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| DPU | +0.497 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| ZBAO | +0.497 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| SNTG | +0.497 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| SOS | +0.497 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| EBON | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| JZ | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| LGCL | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YAAS | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| AIXI | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| AIOS | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| WETO | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| LHSW | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| PONY | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YXT | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| MASK | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| BIYA | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| NXTT | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| KC | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YIBO | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| HOLO | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |

## 3d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ALK | -0.158 | Industrials | sector=+0.28; gen=+0.28; news=-0.74; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.76, 'bucket': 'airlines'} |
| LUV | -0.158 | Industrials | sector=+0.28; gen=+0.28; news=-0.74; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.76, 'bucket': 'airlines'} |
| UAL | -0.158 | Industrials | sector=+0.28; gen=+0.28; news=-0.74; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.76, 'bucket': 'airlines'} |
| AAL | -0.130 | Industrials | sector=+0.28; gen=+0.55; news=-0.74; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.76, 'bucket': 'airlines'} |
| DAL | -0.130 | Industrials | sector=+0.28; gen=+0.55; news=-0.74; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.76, 'bucket': 'airlines'} |
| BMM | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| FNV | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| FNUC | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| ATLX | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| WLK | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| WDFC | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| WPM | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| B | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| BIOX | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| AGI | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| PAAS | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| WLKP | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| SLVM | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| SGML | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| NTR | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| NTIC | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| AEM | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| AUGO | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| NAMM | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| OGG | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |

### 3d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| BEKE | +0.463 | Real Estate | join=+0.76; sector=+0.60; gen=+0.08 |
| QH | +0.460 | Technology | join=+0.76; sector=+0.55; gen=+0.28 |
| NTES | +0.382 | Communication Services | join=+0.76; sector=+0.30; gen=+0.08 |
| BIDU | +0.382 | Communication Services | join=+0.76; sector=+0.30; gen=+0.08 |
| TLN | +0.356 | Utilities | sector=+0.60; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| VST | +0.356 | Utilities | sector=+0.60; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| OXY | +0.334 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| FANG | +0.334 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| KC | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| PONY | +0.487 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| MAAS | +0.450 | Financial | join=+0.76; sector=+0.55; gen=+0.08 |
| YMM | +0.440 | Technology | join=+0.76; sector=+0.55; gen=+0.08 |
| VNET | +0.440 | Technology | join=+0.76; sector=+0.55; gen=+0.08 |
| GDS | +0.440 | Technology | join=+0.76; sector=+0.55; gen=+0.08 |
| EDU | +0.430 | Consumer Defensive | join=+0.76; sector=+0.51; gen=+0.08 |
| TAL | +0.430 | Consumer Defensive | join=+0.76; sector=+0.51; gen=+0.08 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| PETZ | +0.510 | Real Estate | join=+0.76; sector=+0.60; gen=+0.55 |
| DUO | +0.510 | Real Estate | join=+0.76; sector=+0.60; gen=+0.55 |
| SOS | +0.497 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| SNTG | +0.497 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| ZBAO | +0.497 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| NCTY | +0.497 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| DXF | +0.497 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| DPU | +0.497 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |

## 1w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| LGCL | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| AIXI | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YAAS | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| LHSW | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| AIOS | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| HOLO | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YXT | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| EBON | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YIBO | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| HKIT | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| PONY | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| MASK | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| BIYA | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| JZ | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YMT | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| NXTT | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| WETO | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| KC | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| WRD | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| PETZ | +0.558 | Real Estate | join=+0.76; sector=+0.60; gen=+0.55 |
| DUO | +0.558 | Real Estate | join=+0.76; sector=+0.60; gen=+0.55 |
| SOS | +0.543 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| ZBAO | +0.543 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| DXF | +0.543 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| SNTG | +0.543 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |

## 1w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AAUC | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| NAMM | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| VALE | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| PAAS | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| B | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| TROX | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| OMEX | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| NAK | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| ACNT | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| ARIS | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| TFPM | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| PPTA | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| MERC | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| MEOH | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| OR | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| FMC | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| ALTO | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| APD | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| AUGO | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| ORLA | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| ASH | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| IONR | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| VOXR | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| MSB | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| WPM | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |

### 1w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| QH | +0.533 | Technology | join=+0.76; sector=+0.55; gen=+0.28 |
| BEKE | +0.515 | Real Estate | join=+0.76; sector=+0.60; gen=+0.08 |
| NTES | +0.425 | Communication Services | join=+0.76; sector=+0.30; gen=+0.08 |
| BIDU | +0.425 | Communication Services | join=+0.76; sector=+0.30; gen=+0.08 |
| ZTO | +0.425 | Industrials | join=+0.76; sector=+0.28; gen=+0.08 |
| HSAI | +0.393 | Consumer Cyclical | join=+0.76; sector=-0.55; gen=+0.55 |
| NIO | +0.368 | Consumer Cyclical | join=+0.76; sector=-0.55; gen=+0.28 |
| YUMC | +0.350 | Consumer Cyclical | join=+0.76; sector=-0.55; gen=+0.08 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| PONY | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| KC | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| VNET | +0.516 | Technology | join=+0.76; sector=+0.55; gen=+0.08 |
| YMM | +0.516 | Technology | join=+0.76; sector=+0.55; gen=+0.08 |
| GDS | +0.516 | Technology | join=+0.76; sector=+0.55; gen=+0.08 |
| MAAS | +0.500 | Financial | join=+0.76; sector=+0.55; gen=+0.08 |
| TAL | +0.478 | Consumer Defensive | join=+0.76; sector=+0.51; gen=+0.08 |
| EDU | +0.478 | Consumer Defensive | join=+0.76; sector=+0.51; gen=+0.08 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| MASK | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| EBON | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YXT | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YMT | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| WRD | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| LHSW | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| AIXI | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| AIOS | +0.558 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |

## 2w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| NXTT | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| LHSW | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| BIYA | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| LGCL | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YXT | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| AIOS | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| PONY | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| JZ | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| HOLO | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| WRD | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YAAS | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| AIXI | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YMT | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| EBON | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| MASK | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YIBO | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| HKIT | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| KC | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| WETO | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| RDAC | +0.586 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| ZBAO | +0.586 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| NCTY | +0.586 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| DXF | +0.586 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| SNTG | +0.586 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| SOS | +0.586 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |

## 2w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AIIR | +0.007 | Consumer Defensive | sector=+0.51; gen=+0.08 |
| ASH | +0.007 | Basic Materials | sector=-0.60; gen=+0.08 |
| SSP | +0.007 | Communication Services | sector=+0.30; gen=+0.08 |
| RGCO | +0.007 | Utilities | sector=+0.60; gen=+0.08 |
| RGLD | +0.007 | Basic Materials | sector=-0.60; gen=+0.08 |
| CPB | +0.007 | Consumer Defensive | sector=+0.51; gen=+0.08 |
| WALD | +0.007 | Consumer Defensive | sector=+0.51; gen=+0.08 |
| DMLP | +0.007 | Energy | sector=+0.47; gen=+0.08 |
| PRTS | +0.007 | Consumer Cyclical | sector=-0.55; gen=+0.08 |
| PBR-A | +0.007 | Energy | sector=+0.47; gen=+0.08 |
| VNOM | +0.007 | Energy | sector=+0.47; gen=+0.08 |
| VOC | +0.007 | Energy | sector=+0.47; gen=+0.08 |
| VOXR | +0.007 | Basic Materials | sector=-0.60; gen=+0.08 |
| LYB | +0.007 | Basic Materials | sector=-0.60; gen=+0.08 |
| FTRK | +0.007 | Communication Services | sector=+0.30; gen=+0.08 |
| FTW | +0.007 | Energy | sector=+0.47; gen=+0.08 |
| WAVE | +0.007 | Utilities | sector=+0.60; gen=+0.08 |
| NLOP | +0.007 | Real Estate | sector=+0.60; gen=+0.08 |
| FTS | +0.007 | Utilities | sector=+0.60; gen=+0.08 |
| FTI | +0.007 | Energy | sector=+0.47; gen=+0.08 |
| FTLF | +0.007 | Consumer Defensive | sector=+0.51; gen=+0.08 |
| VZ | +0.007 | Communication Services | sector=+0.30; gen=+0.08 |
| DRD | +0.007 | Basic Materials | sector=-0.60; gen=+0.08 |
| LAUR | +0.007 | Consumer Defensive | sector=+0.51; gen=+0.08 |
| RFL | +0.007 | Real Estate | sector=+0.60; gen=+0.08 |

### 2w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| QH | +0.567 | Technology | join=+0.76; sector=+0.55; gen=+0.28 |
| ZTO | +0.469 | Industrials | join=+0.76; sector=+0.28; gen=+0.08 |
| HSAI | +0.411 | Consumer Cyclical | join=+0.76; sector=-0.55; gen=+0.55 |
| NIO | +0.388 | Consumer Cyclical | join=+0.76; sector=-0.55; gen=+0.28 |
| NTES | +0.372 | Communication Services | join=+0.76; sector=+0.30; gen=+0.08 |
| LI | +0.372 | Consumer Cyclical | join=+0.76; sector=-0.55; gen=+0.08 |
| HTHT | +0.372 | Consumer Cyclical | join=+0.76; sector=-0.55; gen=+0.08 |
| BEKE | +0.372 | Real Estate | join=+0.76; sector=+0.60; gen=+0.08 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| PONY | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| KC | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| VNET | +0.551 | Technology | join=+0.76; sector=+0.55; gen=+0.08 |
| GDS | +0.551 | Technology | join=+0.76; sector=+0.55; gen=+0.08 |
| YMM | +0.551 | Technology | join=+0.76; sector=+0.55; gen=+0.08 |
| MAAS | +0.547 | Financial | join=+0.76; sector=+0.55; gen=+0.08 |
| ZLAB | +0.537 | Healthcare | join=+0.76; sector=-0.51; gen=+0.28 |
| XPEV | +0.388 | Consumer Cyclical | join=+0.76; sector=-0.55; gen=+0.28 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| JZ | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YAAS | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YMT | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| HKIT | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| EBON | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| BIYA | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| NXTT | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| YIBO | +0.589 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |

## 1m — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| RDAC | +0.641 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| DPU | +0.641 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| SOS | +0.641 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| NCTY | +0.641 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| DXF | +0.641 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| SNTG | +0.641 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| ZBAO | +0.641 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| HTT | +0.621 | Financial | join=+0.76; sector=+0.55; gen=+0.28 |
| CANG | +0.621 | Financial | join=+0.76; sector=+0.55; gen=+0.28 |
| JF | +0.621 | Financial | join=+0.76; sector=+0.55; gen=+0.28 |
| LX | +0.621 | Financial | join=+0.76; sector=+0.55; gen=+0.28 |
| NOAH | +0.621 | Financial | join=+0.76; sector=+0.55; gen=+0.28 |
| YRD | +0.621 | Financial | join=+0.76; sector=+0.55; gen=+0.28 |
| JFIN | +0.621 | Financial | join=+0.76; sector=+0.55; gen=+0.28 |
| AIFU | +0.621 | Financial | join=+0.76; sector=+0.55; gen=+0.28 |
| HUIZ | +0.621 | Financial | join=+0.76; sector=+0.55; gen=+0.28 |
| LU | +0.621 | Financial | join=+0.76; sector=+0.55; gen=+0.28 |
| SRL | +0.617 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| PONY | +0.608 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| NXTT | +0.608 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| LHSW | +0.608 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| MASK | +0.608 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| WOK | +0.608 | Healthcare | join=+0.76; sector=-0.51; gen=+0.55 |
| ZJYL | +0.608 | Healthcare | join=+0.76; sector=-0.51; gen=+0.55 |
| WRD | +0.608 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |

## 1m — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| PAAS | +0.006 | Basic Materials | sector=-0.60; gen=+0.08 |
| FNUC | +0.006 | Basic Materials | sector=-0.60; gen=+0.08 |
| PPC | +0.006 | Consumer Defensive | sector=+0.51; gen=+0.08 |
| ASH | +0.006 | Basic Materials | sector=-0.60; gen=+0.08 |
| EMPD | +0.006 | Consumer Cyclical | sector=-0.55; gen=+0.08 |
| ELPC | +0.006 | Utilities | sector=+0.60; gen=+0.08 |
| ELS | +0.006 | Real Estate | sector=+0.60; gen=+0.08 |
| BMM | +0.006 | Basic Materials | sector=-0.60; gen=+0.08 |
| PPL | +0.006 | Utilities | sector=+0.60; gen=+0.08 |
| TWG | +0.006 | Consumer Defensive | sector=+0.51; gen=+0.08 |
| EP | +0.006 | Energy | sector=+0.47; gen=+0.08 |
| EPC | +0.006 | Consumer Defensive | sector=+0.51; gen=+0.08 |
| EPD | +0.006 | Energy | sector=+0.47; gen=+0.08 |
| EPSN | +0.006 | Energy | sector=+0.47; gen=+0.08 |
| EPM | +0.006 | Energy | sector=+0.47; gen=+0.08 |
| TU | +0.006 | Communication Services | sector=+0.30; gen=+0.08 |
| GRO | +0.006 | Basic Materials | sector=-0.60; gen=+0.08 |
| BSM | +0.006 | Energy | sector=+0.47; gen=+0.08 |
| CCEP | +0.006 | Consumer Defensive | sector=+0.51; gen=+0.08 |
| BTBD | +0.006 | Consumer Cyclical | sector=-0.55; gen=+0.08 |
| NC | +0.006 | Energy | sector=+0.47; gen=+0.08 |
| FNV | +0.006 | Basic Materials | sector=-0.60; gen=+0.08 |
| PRMB | +0.006 | Consumer Defensive | sector=+0.51; gen=+0.08 |
| BTG | +0.006 | Basic Materials | sector=-0.60; gen=+0.08 |
| BTI | +0.006 | Consumer Defensive | sector=+0.51; gen=+0.08 |

### 1m — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| QH | +0.588 | Technology | join=+0.76; sector=+0.55; gen=+0.28 |
| ZTO | +0.497 | Industrials | join=+0.76; sector=+0.28; gen=+0.08 |
| HSAI | +0.421 | Consumer Cyclical | join=+0.76; sector=-0.55; gen=+0.55 |
| NIO | +0.401 | Consumer Cyclical | join=+0.76; sector=-0.55; gen=+0.28 |
| BEKE | +0.387 | Real Estate | join=+0.76; sector=+0.60; gen=+0.08 |
| JD | +0.387 | Consumer Cyclical | join=+0.76; sector=-0.55; gen=+0.08 |
| HTHT | +0.387 | Consumer Cyclical | join=+0.76; sector=-0.55; gen=+0.08 |
| NTES | +0.387 | Communication Services | join=+0.76; sector=+0.30; gen=+0.08 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| KC | +0.608 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| PONY | +0.608 | Technology | join=+0.76; sector=+0.55; gen=+0.55 |
| MAAS | +0.607 | Financial | join=+0.76; sector=+0.55; gen=+0.08 |
| ZLAB | +0.588 | Healthcare | join=+0.76; sector=-0.51; gen=+0.28 |
| GDS | +0.574 | Technology | join=+0.76; sector=+0.55; gen=+0.08 |
| VNET | +0.574 | Technology | join=+0.76; sector=+0.55; gen=+0.08 |
| YMM | +0.574 | Technology | join=+0.76; sector=+0.55; gen=+0.08 |
| TME | +0.401 | Communication Services | join=+0.76; sector=+0.30; gen=+0.28 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| RDAC | +0.641 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| SNTG | +0.641 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| SOS | +0.641 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| DXF | +0.641 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| NCTY | +0.641 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| DPU | +0.641 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| ZBAO | +0.641 | Financial | join=+0.76; sector=+0.55; gen=+0.55 |
| LU | +0.621 | Financial | join=+0.76; sector=+0.55; gen=+0.28 |

## Read

- **1d** news-heavy; **1m** structural join + sector.
- Longer horizons use the predictors' explicit 3d/1w/2w/1m calls when stored, else fall back to the 1d call.
- Predictor bias is scaled by its graded hit rate (learning gate) — weak topics move scores less.
- Backtest: `python -m src.stock_book_backtest` (or Stock Book Backtest action).

CSV: `data/stock_book/2026-08-14_stock_book.csv`
JSON: `data/stock_book/2026-08-14_stock_book.json`
