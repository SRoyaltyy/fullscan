# Stock book — **2026-08-14** (1d / 3d / 1w / 2w / 1m)

Generated: 2026-08-14T13:37:12.421708-04:00

Layers: join (labels×weather) + sector predict + general regime + news actions.

## Regime snapshot

- **General bias:** +0.55
- **Weather risk:** on
- **News tickers:** 18
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
| TLN | +0.516 | Utilities | join=+0.37; sector=+0.60; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| VST | +0.492 | Utilities | join=+0.30; sector=+0.60; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| DVN | +0.487 | Energy | join=+0.17; sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| FANG | +0.482 | Energy | join=+0.15; sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| EOG | +0.482 | Energy | join=+0.15; sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| OXY | +0.458 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| COP | +0.456 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| APA | +0.456 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| NRG | +0.439 | Utilities | join=+0.23; sector=+0.60; gen=+0.28; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| CEG | +0.413 | Utilities | join=+0.15; sector=+0.60; gen=+0.28; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| XOM | +0.404 | Energy | sector=+0.47; gen=+0.08; news=+0.74; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.76, 'bucket': 'oil_integrated'} |
| CVX | +0.402 | Energy | sector=+0.47; gen=+0.08; news=+0.74; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.76, 'bucket': 'oil_integrated'} |
| NEE | +0.394 | Utilities | join=+0.15; sector=+0.60; gen=+0.08; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| PHAXU | +0.371 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| SKAIU | +0.371 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| LEDRU | +0.371 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| SCATU | +0.371 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| IDIAU | +0.371 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| BCACU | +0.371 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| APNAU | +0.371 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| BEMD | +0.371 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| CLRS | +0.337 | Financial | join=+0.66; sector=+0.55; gen=+0.22 |
| TBCH | +0.330 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| WOLF | +0.330 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| PGY | +0.330 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |

## 1d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| LUV | -0.174 | Industrials | join=+0.15; sector=+0.28; gen=+0.28; news=-0.74; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.76, 'bucket': 'airlines'} |
| UAL | -0.142 | Industrials | join=+0.24; sector=+0.28; gen=+0.28; news=-0.74; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.76, 'bucket': 'airlines'} |
| ALK | -0.123 | Industrials | join=+0.30; sector=+0.28; gen=+0.28; news=-0.74; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.76, 'bucket': 'airlines'} |
| DAL | -0.121 | Industrials | join=+0.23; sector=+0.28; gen=+0.55; news=-0.74; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.76, 'bucket': 'airlines'} |
| LIN | -0.111 | Basic Materials | sector=-0.60; gen=+0.08 |
| AAL | -0.088 | Industrials | join=+0.32; sector=+0.28; gen=+0.55; news=-0.74; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.76, 'bucket': 'airlines'} |
| VALE | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| SSL | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| CTVA | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| RIO | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| NTR | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| LYB | -0.082 | Basic Materials | sector=-0.60; gen=+0.08 |
| MDT | -0.068 | Healthcare | sector=-0.51; gen=+0.08 |
| NICM | -0.068 | Basic Materials | sector=-0.60; gen=+0.22 |
| SIND | -0.068 | Basic Materials | sector=-0.60; gen=+0.22 |
| SIM | -0.068 | Basic Materials | sector=-0.60; gen=+0.22 |
| PHOS | -0.068 | Basic Materials | sector=-0.60; gen=+0.22 |
| PPG | -0.062 | Basic Materials | sector=-0.60; gen=+0.28 |
| MLM | -0.062 | Basic Materials | sector=-0.60; gen=+0.28 |
| IFF | -0.062 | Basic Materials | sector=-0.60; gen=+0.28 |
| BHP | -0.062 | Basic Materials | sector=-0.60; gen=+0.28 |
| CRH | -0.062 | Basic Materials | sector=-0.60; gen=+0.28 |
| VMC | -0.062 | Basic Materials | sector=-0.60; gen=+0.28 |
| ICL | -0.062 | Basic Materials | sector=-0.60; gen=+0.28 |
| TTAM | -0.062 | Basic Materials | sector=-0.60; gen=+0.28 |

### 1d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TLN | +0.516 | Utilities | join=+0.37; sector=+0.60; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| VST | +0.492 | Utilities | join=+0.30; sector=+0.60; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| DVN | +0.487 | Energy | join=+0.17; sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| FANG | +0.482 | Energy | join=+0.15; sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| EOG | +0.482 | Energy | join=+0.15; sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| OXY | +0.458 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| COP | +0.456 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| APA | +0.456 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DAVE | +0.322 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| RUN | +0.310 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| FOUR | +0.310 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| KEEL | +0.310 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| CIFR | +0.310 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| APLD | +0.310 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| UWMC | +0.310 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |
| WULF | +0.310 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| WOLF | +0.330 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| INDI | +0.330 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| TBCH | +0.330 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| VERI | +0.330 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| PGY | +0.330 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| CRNC | +0.330 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| LDI | +0.329 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| BETR | +0.329 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |

## 3d — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TLN | +0.503 | Utilities | join=+0.37; sector=+0.60; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| VST | +0.476 | Utilities | join=+0.30; sector=+0.60; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| LEDRU | +0.464 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| PHAXU | +0.464 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| SCATU | +0.464 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| BEMD | +0.464 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| BCACU | +0.464 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| APNAU | +0.464 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| SKAIU | +0.464 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| IDIAU | +0.464 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| CLRS | +0.425 | Financial | join=+0.66; sector=+0.55; gen=+0.22 |
| NRG | +0.420 | Utilities | join=+0.23; sector=+0.60; gen=+0.28; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| BETR | +0.412 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| BTBT | +0.412 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| LDI | +0.412 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| PRCH | +0.403 | Financial | join=+0.53; sector=+0.55; gen=+0.55 |
| ATLC | +0.403 | Financial | join=+0.53; sector=+0.55; gen=+0.55 |
| RC | +0.402 | Real Estate | join=+0.49; sector=+0.60; gen=+0.55 |
| CRNC | +0.402 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| TBCH | +0.402 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| WOLF | +0.402 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| VERI | +0.402 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| INDI | +0.402 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| PGY | +0.402 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| DVN | +0.400 | Energy | join=+0.17; sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |

## 3d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| LIN | -0.162 | Basic Materials | sector=-0.60; gen=+0.08 |
| RIO | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| VALE | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| SSL | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| LYB | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| NTR | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| CTVA | -0.129 | Basic Materials | sector=-0.60; gen=+0.08 |
| SIM | -0.116 | Basic Materials | sector=-0.60; gen=+0.22 |
| SIND | -0.116 | Basic Materials | sector=-0.60; gen=+0.22 |
| NICM | -0.116 | Basic Materials | sector=-0.60; gen=+0.22 |
| PHOS | -0.116 | Basic Materials | sector=-0.60; gen=+0.22 |
| DD | -0.110 | Basic Materials | sector=-0.60; gen=+0.28 |
| PPG | -0.110 | Basic Materials | sector=-0.60; gen=+0.28 |
| VMC | -0.110 | Basic Materials | sector=-0.60; gen=+0.28 |
| CRH | -0.110 | Basic Materials | sector=-0.60; gen=+0.28 |
| SA | -0.110 | Basic Materials | sector=-0.60; gen=+0.28 |
| TX | -0.110 | Basic Materials | sector=-0.60; gen=+0.28 |
| TTAM | -0.110 | Basic Materials | sector=-0.60; gen=+0.28 |
| IFF | -0.110 | Basic Materials | sector=-0.60; gen=+0.28 |
| ICL | -0.110 | Basic Materials | sector=-0.60; gen=+0.28 |
| MLM | -0.110 | Basic Materials | sector=-0.60; gen=+0.28 |
| BHP | -0.110 | Basic Materials | sector=-0.60; gen=+0.28 |
| DOW | -0.099 | Basic Materials | sector=-0.60; gen=+0.08 |
| SXT | -0.099 | Basic Materials | sector=-0.60; gen=+0.08 |
| WLK | -0.099 | Basic Materials | sector=-0.60; gen=+0.08 |

### 3d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TLN | +0.503 | Utilities | join=+0.37; sector=+0.60; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| VST | +0.476 | Utilities | join=+0.30; sector=+0.60; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| NRG | +0.420 | Utilities | join=+0.23; sector=+0.60; gen=+0.28; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| DVN | +0.400 | Energy | join=+0.17; sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| FANG | +0.395 | Energy | join=+0.15; sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| EOG | +0.395 | Energy | join=+0.15; sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| CEG | +0.390 | Utilities | join=+0.15; sector=+0.60; gen=+0.28; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| ASTS | +0.379 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DAVE | +0.393 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| SLG | +0.390 | Real Estate | join=+0.46; sector=+0.60; gen=+0.55 |
| MARA | +0.389 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |
| UWMC | +0.389 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |
| UPST | +0.389 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |
| GLXY | +0.389 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |
| WULF | +0.389 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |
| CLSK | +0.389 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| LDI | +0.412 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| BTBT | +0.412 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| BETR | +0.412 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| PRCH | +0.403 | Financial | join=+0.53; sector=+0.55; gen=+0.55 |
| ATLC | +0.403 | Financial | join=+0.53; sector=+0.55; gen=+0.55 |
| RC | +0.402 | Real Estate | join=+0.49; sector=+0.60; gen=+0.55 |
| TBCH | +0.402 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| VERI | +0.402 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |

## 1w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| BCACU | +0.513 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| SKAIU | +0.513 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| IDIAU | +0.513 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| LEDRU | +0.513 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| APNAU | +0.513 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| BEMD | +0.513 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| PHAXU | +0.513 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| SCATU | +0.513 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| TLN | +0.471 | Utilities | join=+0.37; sector=+0.60; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| CLRS | +0.469 | Financial | join=+0.66; sector=+0.55; gen=+0.22 |
| TBCH | +0.462 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| CRNC | +0.462 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| INDI | +0.462 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| VERI | +0.462 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| PGY | +0.462 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| WOLF | +0.462 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| APPS | +0.452 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| KOPN | +0.452 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| DAVE | +0.452 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| CSIQ | +0.452 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| BAND | +0.452 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| GPRO | +0.452 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| AEVA | +0.452 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| RXT | +0.452 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| SKYT | +0.452 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |

## 1w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| LIN | -0.180 | Basic Materials | sector=-0.60; gen=+0.08 |
| VALE | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| LYB | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| NTR | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| SSL | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| CTVA | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| RIO | -0.142 | Basic Materials | sector=-0.60; gen=+0.08 |
| PHOS | -0.130 | Basic Materials | sector=-0.60; gen=+0.22 |
| SIND | -0.130 | Basic Materials | sector=-0.60; gen=+0.22 |
| NICM | -0.130 | Basic Materials | sector=-0.60; gen=+0.22 |
| SIM | -0.130 | Basic Materials | sector=-0.60; gen=+0.22 |
| SA | -0.125 | Basic Materials | sector=-0.60; gen=+0.28 |
| DD | -0.125 | Basic Materials | sector=-0.60; gen=+0.28 |
| PPG | -0.125 | Basic Materials | sector=-0.60; gen=+0.28 |
| CRH | -0.125 | Basic Materials | sector=-0.60; gen=+0.28 |
| TX | -0.125 | Basic Materials | sector=-0.60; gen=+0.28 |
| BHP | -0.125 | Basic Materials | sector=-0.60; gen=+0.28 |
| ICL | -0.125 | Basic Materials | sector=-0.60; gen=+0.28 |
| IFF | -0.125 | Basic Materials | sector=-0.60; gen=+0.28 |
| MLM | -0.125 | Basic Materials | sector=-0.60; gen=+0.28 |
| VMC | -0.125 | Basic Materials | sector=-0.60; gen=+0.28 |
| TTAM | -0.125 | Basic Materials | sector=-0.60; gen=+0.28 |
| AU | -0.108 | Basic Materials | sector=-0.60; gen=+0.08 |
| AMRZ | -0.108 | Basic Materials | sector=-0.60; gen=+0.08 |
| APD | -0.108 | Basic Materials | sector=-0.60; gen=+0.08 |

### 1w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TLN | +0.471 | Utilities | join=+0.37; sector=+0.60; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| VST | +0.440 | Utilities | join=+0.30; sector=+0.60; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| ASTS | +0.437 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| DOCN | +0.424 | Technology | join=+0.46; sector=+0.55; gen=+0.55 |
| SAIL | +0.424 | Technology | join=+0.46; sector=+0.55; gen=+0.55 |
| SITM | +0.424 | Technology | join=+0.46; sector=+0.55; gen=+0.55 |
| CRWV | +0.410 | Technology | join=+0.43; sector=+0.55; gen=+0.55 |
| NET | +0.410 | Technology | join=+0.43; sector=+0.55; gen=+0.55 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DAVE | +0.452 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| FOUR | +0.437 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| KEEL | +0.437 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| APLD | +0.437 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| CIFR | +0.437 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| RUN | +0.437 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| BTDR | +0.424 | Technology | join=+0.46; sector=+0.55; gen=+0.55 |
| CORZ | +0.424 | Technology | join=+0.46; sector=+0.55; gen=+0.55 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| WOLF | +0.462 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| PGY | +0.462 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| INDI | +0.462 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| TBCH | +0.462 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| VERI | +0.462 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| CRNC | +0.462 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| CSIQ | +0.452 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| LPTH | +0.452 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |

## 2w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| IDIAU | +0.559 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| LEDRU | +0.559 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| PHAXU | +0.559 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| SKAIU | +0.559 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| SCATU | +0.559 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| BEMD | +0.559 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| APNAU | +0.559 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| BCACU | +0.559 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| CLRS | +0.512 | Financial | join=+0.66; sector=+0.55; gen=+0.22 |
| CRNC | +0.487 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| INDI | +0.487 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| PGY | +0.487 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| TBCH | +0.487 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| WOLF | +0.487 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| VERI | +0.487 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| BTBT | +0.483 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| BETR | +0.483 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| LDI | +0.483 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| KOPN | +0.476 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| CSIQ | +0.476 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| APPS | +0.476 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| RXT | +0.476 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| SKYT | +0.476 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| BAND | +0.476 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| LPTH | +0.476 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |

## 2w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| LIN | -0.033 | Basic Materials | sector=-0.60; gen=+0.08 |
| SSL | +0.007 | Basic Materials | sector=-0.60; gen=+0.08 |
| VALE | +0.007 | Basic Materials | sector=-0.60; gen=+0.08 |
| CTVA | +0.007 | Basic Materials | sector=-0.60; gen=+0.08 |
| LYB | +0.007 | Basic Materials | sector=-0.60; gen=+0.08 |
| NTR | +0.007 | Basic Materials | sector=-0.60; gen=+0.08 |
| RIO | +0.007 | Basic Materials | sector=-0.60; gen=+0.08 |
| SIM | +0.018 | Basic Materials | sector=-0.60; gen=+0.22 |
| NICM | +0.018 | Basic Materials | sector=-0.60; gen=+0.22 |
| SIND | +0.018 | Basic Materials | sector=-0.60; gen=+0.22 |
| PHOS | +0.018 | Basic Materials | sector=-0.60; gen=+0.22 |
| TTAM | +0.023 | Basic Materials | sector=-0.60; gen=+0.28 |
| CRH | +0.023 | Basic Materials | sector=-0.60; gen=+0.28 |
| TX | +0.023 | Basic Materials | sector=-0.60; gen=+0.28 |
| PPG | +0.023 | Basic Materials | sector=-0.60; gen=+0.28 |
| DD | +0.023 | Basic Materials | sector=-0.60; gen=+0.28 |
| ICL | +0.023 | Basic Materials | sector=-0.60; gen=+0.28 |
| IFF | +0.023 | Basic Materials | sector=-0.60; gen=+0.28 |
| SA | +0.023 | Basic Materials | sector=-0.60; gen=+0.28 |
| VMC | +0.023 | Basic Materials | sector=-0.60; gen=+0.28 |
| BHP | +0.023 | Basic Materials | sector=-0.60; gen=+0.28 |
| LULU | +0.023 | Consumer Cyclical | sector=-0.55; gen=+0.28 |
| MLM | +0.023 | Basic Materials | sector=-0.60; gen=+0.28 |
| MDLZ | +0.044 | Consumer Defensive | sector=+0.51; gen=+0.08 |
| CHD | +0.044 | Consumer Defensive | sector=+0.51; gen=+0.08 |

### 2w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ASTS | +0.460 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| SITM | +0.445 | Technology | join=+0.46; sector=+0.55; gen=+0.55 |
| DOCN | +0.445 | Technology | join=+0.46; sector=+0.55; gen=+0.55 |
| SAIL | +0.445 | Technology | join=+0.46; sector=+0.55; gen=+0.55 |
| IREN | +0.442 | Financial | join=+0.46; sector=+0.55; gen=+0.55 |
| AFRM | +0.442 | Financial | join=+0.46; sector=+0.55; gen=+0.55 |
| MKSI | +0.431 | Technology | join=+0.43; sector=+0.55; gen=+0.55 |
| SNOW | +0.431 | Technology | join=+0.43; sector=+0.55; gen=+0.55 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DAVE | +0.476 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |
| APLD | +0.460 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| FOUR | +0.460 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| RUN | +0.460 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| KEEL | +0.460 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| CIFR | +0.460 | Technology | join=+0.49; sector=+0.55; gen=+0.55 |
| GLXY | +0.456 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |
| WULF | +0.456 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TBCH | +0.487 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| WOLF | +0.487 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| VERI | +0.487 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| INDI | +0.487 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| PGY | +0.487 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| CRNC | +0.487 | Technology | join=+0.55; sector=+0.55; gen=+0.55 |
| BETR | +0.483 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| LDI | +0.483 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |

## 1m — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| SKAIU | +0.617 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| APNAU | +0.617 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| SCATU | +0.617 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| BCACU | +0.617 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| IDIAU | +0.617 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| BEMD | +0.617 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| PHAXU | +0.617 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| LEDRU | +0.617 | Financial | join=+0.76; sector=+0.55; gen=+0.22 |
| CLRS | +0.568 | Financial | join=+0.66; sector=+0.55; gen=+0.22 |
| BTBT | +0.534 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| BETR | +0.534 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| LDI | +0.534 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| PRCH | +0.523 | Financial | join=+0.53; sector=+0.55; gen=+0.55 |
| ATLC | +0.523 | Financial | join=+0.53; sector=+0.55; gen=+0.55 |
| FCUS | +0.518 | Financial | join=+0.52; sector=+0.55; gen=+0.55 |
| DRN | +0.518 | Financial | join=+0.52; sector=+0.55; gen=+0.55 |
| HECO | +0.518 | Financial | join=+0.52; sector=+0.55; gen=+0.55 |
| VERS | +0.518 | Financial | join=+0.52; sector=+0.55; gen=+0.55 |
| BOBP | +0.518 | Financial | join=+0.52; sector=+0.55; gen=+0.55 |
| IBAT | +0.518 | Financial | join=+0.52; sector=+0.55; gen=+0.55 |
| ARKX | +0.518 | Financial | join=+0.52; sector=+0.55; gen=+0.55 |
| SMHX | +0.518 | Financial | join=+0.52; sector=+0.55; gen=+0.55 |
| CHAT | +0.518 | Financial | join=+0.52; sector=+0.55; gen=+0.55 |
| AIS | +0.518 | Financial | join=+0.52; sector=+0.55; gen=+0.55 |
| EWY | +0.518 | Financial | join=+0.52; sector=+0.55; gen=+0.55 |

## 1m — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| LIN | -0.036 | Basic Materials | sector=-0.60; gen=+0.08 |
| VALE | +0.006 | Basic Materials | sector=-0.60; gen=+0.08 |
| SSL | +0.006 | Basic Materials | sector=-0.60; gen=+0.08 |
| CTVA | +0.006 | Basic Materials | sector=-0.60; gen=+0.08 |
| NTR | +0.006 | Basic Materials | sector=-0.60; gen=+0.08 |
| RIO | +0.006 | Basic Materials | sector=-0.60; gen=+0.08 |
| LYB | +0.006 | Basic Materials | sector=-0.60; gen=+0.08 |
| SIM | +0.016 | Basic Materials | sector=-0.60; gen=+0.22 |
| SIND | +0.016 | Basic Materials | sector=-0.60; gen=+0.22 |
| NICM | +0.016 | Basic Materials | sector=-0.60; gen=+0.22 |
| PHOS | +0.016 | Basic Materials | sector=-0.60; gen=+0.22 |
| CRH | +0.020 | Basic Materials | sector=-0.60; gen=+0.28 |
| TX | +0.020 | Basic Materials | sector=-0.60; gen=+0.28 |
| DD | +0.020 | Basic Materials | sector=-0.60; gen=+0.28 |
| TTAM | +0.020 | Basic Materials | sector=-0.60; gen=+0.28 |
| PPG | +0.020 | Basic Materials | sector=-0.60; gen=+0.28 |
| IFF | +0.020 | Basic Materials | sector=-0.60; gen=+0.28 |
| SA | +0.020 | Basic Materials | sector=-0.60; gen=+0.28 |
| LULU | +0.020 | Consumer Cyclical | sector=-0.55; gen=+0.28 |
| MLM | +0.020 | Basic Materials | sector=-0.60; gen=+0.28 |
| ICL | +0.020 | Basic Materials | sector=-0.60; gen=+0.28 |
| VMC | +0.020 | Basic Materials | sector=-0.60; gen=+0.28 |
| BHP | +0.020 | Basic Materials | sector=-0.60; gen=+0.28 |
| COP | +0.044 | Energy | sector=+0.47; gen=+0.08; news=+0.88; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.8, 'bucket': 'oil_ep'} |
| USLM | +0.044 | Basic Materials | sector=-0.60; gen=+0.08 |

### 1m — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| IREN | +0.491 | Financial | join=+0.46; sector=+0.55; gen=+0.55 |
| AFRM | +0.491 | Financial | join=+0.46; sector=+0.55; gen=+0.55 |
| RKT | +0.476 | Financial | join=+0.43; sector=+0.55; gen=+0.55 |
| SOFI | +0.476 | Financial | join=+0.43; sector=+0.55; gen=+0.55 |
| TPG | +0.476 | Financial | join=+0.43; sector=+0.55; gen=+0.55 |
| HUT | +0.476 | Financial | join=+0.43; sector=+0.55; gen=+0.55 |
| CG | +0.476 | Financial | join=+0.43; sector=+0.55; gen=+0.55 |
| JEF | +0.476 | Financial | join=+0.43; sector=+0.55; gen=+0.55 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| UWMC | +0.506 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |
| MARA | +0.506 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |
| UPST | +0.506 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |
| WULF | +0.506 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |
| CLSK | +0.506 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |
| GLXY | +0.506 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |
| CACC | +0.491 | Financial | join=+0.46; sector=+0.55; gen=+0.55 |
| DAVE | +0.490 | Technology | join=+0.53; sector=+0.55; gen=+0.55 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| BETR | +0.534 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| LDI | +0.534 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| BTBT | +0.534 | Financial | join=+0.55; sector=+0.55; gen=+0.55 |
| ATLC | +0.523 | Financial | join=+0.53; sector=+0.55; gen=+0.55 |
| PRCH | +0.523 | Financial | join=+0.53; sector=+0.55; gen=+0.55 |
| FDMM | +0.513 | Financial | join=+0.55; sector=+0.55; gen=+0.22 |
| KF | +0.512 | Financial | join=+0.50; sector=+0.55; gen=+0.55 |
| ZBAO | +0.506 | Financial | join=+0.49; sector=+0.55; gen=+0.55 |

## Read

- **1d** news-heavy; **1m** structural join + sector.
- Longer horizons use the predictors' explicit 3d/1w/2w/1m calls when stored, else fall back to the 1d call.
- Predictor bias is scaled by its graded hit rate (learning gate) — weak topics move scores less.
- Backtest: `python -m src.stock_book_backtest` (or Stock Book Backtest action).

CSV: `data/stock_book/2026-08-14_stock_book.csv`
JSON: `data/stock_book/2026-08-14_stock_book.json`
