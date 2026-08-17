# Stock book — **2026-08-17** (1d / 3d / 1w / 2w / 1m)

Generated: 2026-08-17T13:32:02.012791-04:00

Layers: join (labels×weather) + sector predict + general regime + news actions.

## Regime snapshot

- **General bias:** +0.55
- **Weather risk:** mixed
- **News tickers:** 28
- **Universe (after liquidity):** 2697
- **Gates:** mcap ≥ $80.0M, avg vol ≥ 500.0k
- **Rebound floor tags:** 87

### Sector bias

| Sector | bias |
|--------|------|
| Energy | +0.65 |
| Basic Materials | +0.62 |
| Consumer Cyclical | -0.60 |
| Consumer Defensive | +0.60 |
| Healthcare | -0.60 |
| Real Estate | +0.60 |
| Financial | +0.55 |
| Utilities | +0.55 |
| Communication Services | +0.28 |
| Industrials | +0.28 |
| Technology | +0.28 |

### Learning gate (graded accuracy → how much each predictor is trusted)

| Topic | hit rate | graded runs | weight applied |
|-------|----------|-------------|----------------|
| general | 57% | 14 | ×1.00 |
| sector:Basic Materials | 60% | 5 | ×1.00 |
| sector:Communication Services | 40% | 5 | ×0.50 |
| sector:Consumer Cyclical | 80% | 5 | ×1.00 |
| sector:Consumer Defensive | 60% | 5 | ×1.00 |
| sector:Energy | 60% | 5 | ×1.00 |
| sector:Financial | 60% | 5 | ×1.00 |
| sector:Healthcare | 60% | 5 | ×1.00 |
| sector:Industrials | 40% | 5 | ×0.50 |
| sector:Real Estate | 80% | 5 | ×1.00 |
| sector:Technology | 40% | 5 | ×0.50 |
| sector:Utilities | 80% | 5 | ×1.00 |

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
| DVN | +0.497 | Energy | join=+0.17; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.493 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.493 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.468 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.466 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.466 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| XOM | +0.409 | Energy | sector=+0.65; gen=+0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| CVX | +0.407 | Energy | sector=+0.65; gen=+0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| TLN | +0.407 | Utilities | sector=+0.55; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| VST | +0.380 | Utilities | sector=+0.55; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| CEG | +0.379 | Utilities | sector=+0.55; gen=+0.28; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| NRG | +0.379 | Utilities | sector=+0.55; gen=+0.28; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| TMC | +0.367 | Basic Materials | join=+0.20; sector=+0.62; gen=+0.55; rebound_floor |
| TGB | +0.356 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.55; rebound_floor |
| ELF | +0.348 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.55; rebound_floor |
| NEE | +0.333 | Utilities | sector=+0.55; gen=+0.08; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| DNN | +0.333 | Energy | join=+0.17; sector=+0.65; gen=+0.28; rebound_floor |
| ERO | +0.328 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.28; rebound_floor |
| HNST | +0.324 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| MOS | +0.324 | Basic Materials | join=+0.15; sector=+0.62; gen=+0.28; rebound_floor |
| EL | +0.321 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.28; rebound_floor |
| INTC | +0.311 | Technology | sector=+0.28; gen=+0.55; news=+0.47; ev={'event': 'ai_chip_demand_spike', 'side': 'buy', 'weight': 2.52, 'bucket': 'semiconductors'} |
| NVDA | +0.309 | Technology | sector=+0.28; gen=+0.55; news=+0.47; ev={'event': 'ai_chip_demand_spike', 'side': 'buy', 'weight': 2.52, 'bucket': 'semiconductors'} |
| MU | +0.309 | Technology | sector=+0.28; gen=+0.55; news=+0.47; ev={'event': 'ai_chip_demand_spike', 'side': 'buy', 'weight': 2.52, 'bucket': 'semiconductors'} |
| AMD | +0.309 | Technology | sector=+0.28; gen=+0.55; news=+0.47; ev={'event': 'ai_chip_demand_spike', 'side': 'buy', 'weight': 2.52, 'bucket': 'semiconductors'} |

## 1d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| LUV | -0.179 | Industrials | sector=+0.28; gen=+0.28; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| ALK | -0.179 | Industrials | sector=+0.28; gen=+0.28; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| UAL | -0.176 | Industrials | sector=+0.28; gen=+0.28; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| DAL | -0.151 | Industrials | sector=+0.28; gen=+0.55; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| AAL | -0.149 | Industrials | sector=+0.28; gen=+0.55; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| AMBP | -0.113 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| GTX | -0.113 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| YUM | -0.111 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| QSR | -0.111 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| ARCO | -0.111 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| HRB | -0.111 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| MCD | -0.111 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| ORLY | -0.111 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| GPC | -0.111 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| TXRH | -0.109 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| WH | -0.109 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| SON | -0.109 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| SLGN | -0.109 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| REYN | -0.109 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CHH | -0.109 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CCK | -0.109 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CHDN | -0.109 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| LEG | -0.109 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| WEN | -0.109 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| DRI | -0.109 | Consumer Cyclical | sector=-0.60; gen=+0.08 |

### 1d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DVN | +0.497 | Energy | join=+0.17; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.493 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.493 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.468 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.466 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.466 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| XOM | +0.409 | Energy | sector=+0.65; gen=+0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| CVX | +0.407 | Energy | sector=+0.65; gen=+0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TGB | +0.356 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.55; rebound_floor |
| ELF | +0.348 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.55; rebound_floor |
| DNN | +0.333 | Energy | join=+0.17; sector=+0.65; gen=+0.28; rebound_floor |
| ERO | +0.328 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.28; rebound_floor |
| MOS | +0.324 | Basic Materials | join=+0.15; sector=+0.62; gen=+0.28; rebound_floor |
| OLLI | +0.302 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.08; rebound_floor |
| OTF | +0.294 | Financial | join=+0.15; sector=+0.55; gen=+0.08; rebound_floor |
| CWEN | +0.287 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.367 | Basic Materials | join=+0.20; sector=+0.62; gen=+0.55; rebound_floor |
| HNST | +0.324 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| NB | +0.309 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.08; rebound_floor |
| JLHL | +0.275 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| NRDY | +0.273 | Technology | sector=+0.28; gen=+0.55; rebound_floor |
| PCT | +0.273 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| ABAT | +0.246 | Industrials | sector=+0.28; gen=+0.28; rebound_floor |
| BTQ | +0.231 | Technology | sector=+0.28; gen=+0.08; rebound_floor |

## 3d — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DVN | +0.433 | Energy | join=+0.17; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.428 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.428 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| TMC | +0.421 | Basic Materials | join=+0.20; sector=+0.62; gen=+0.55; rebound_floor |
| TGB | +0.409 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.55; rebound_floor |
| ELF | +0.404 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.55; rebound_floor |
| OXY | +0.400 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.397 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.397 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| DNN | +0.394 | Energy | join=+0.17; sector=+0.65; gen=+0.28; rebound_floor |
| ERO | +0.381 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.28; rebound_floor |
| EL | +0.376 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.28; rebound_floor |
| MOS | +0.376 | Basic Materials | join=+0.15; sector=+0.62; gen=+0.28; rebound_floor |
| HNST | +0.376 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| TLN | +0.375 | Utilities | sector=+0.55; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| XOM | +0.363 | Energy | sector=+0.65; gen=+0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| NB | +0.362 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.08; rebound_floor |
| CVX | +0.360 | Energy | sector=+0.65; gen=+0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| OLLI | +0.357 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.08; rebound_floor |
| OTF | +0.357 | Financial | join=+0.15; sector=+0.55; gen=+0.08; rebound_floor |
| NRG | +0.347 | Utilities | sector=+0.55; gen=+0.28; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| CEG | +0.347 | Utilities | sector=+0.55; gen=+0.28; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |
| CRH | +0.346 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| ECL | +0.346 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| CWEN | +0.346 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |

## 3d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AMBP | -0.166 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| GTX | -0.166 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| ARCO | -0.162 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| HRB | -0.162 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| GPC | -0.162 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| MCD | -0.162 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| ORLY | -0.162 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| YUM | -0.162 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| QSR | -0.162 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| WH | -0.160 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| TXRH | -0.160 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CCK | -0.160 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| DRI | -0.160 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| LEG | -0.160 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CHDN | -0.160 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CASY | -0.160 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| WEN | -0.160 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| SON | -0.160 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CHH | -0.160 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| SLGN | -0.160 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| TJX | -0.160 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| TSCO | -0.160 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| REYN | -0.160 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| ROL | -0.160 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| MWC | -0.155 | Consumer Cyclical | sector=-0.60; gen=+0.22 |

### 3d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DVN | +0.433 | Energy | join=+0.17; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.428 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.428 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.400 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.397 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.397 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EL | +0.376 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.28; rebound_floor |
| TLN | +0.375 | Utilities | sector=+0.55; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TGB | +0.409 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.55; rebound_floor |
| ELF | +0.404 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.55; rebound_floor |
| DNN | +0.394 | Energy | join=+0.17; sector=+0.65; gen=+0.28; rebound_floor |
| ERO | +0.381 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.28; rebound_floor |
| MOS | +0.376 | Basic Materials | join=+0.15; sector=+0.62; gen=+0.28; rebound_floor |
| OTF | +0.357 | Financial | join=+0.15; sector=+0.55; gen=+0.08; rebound_floor |
| OLLI | +0.357 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.08; rebound_floor |
| CWEN | +0.346 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.421 | Basic Materials | join=+0.20; sector=+0.62; gen=+0.55; rebound_floor |
| HNST | +0.376 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| NB | +0.362 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.08; rebound_floor |
| NRDY | +0.304 | Technology | sector=+0.28; gen=+0.55; rebound_floor |
| JLHL | +0.301 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| PCT | +0.298 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| NEXT | +0.277 | Energy | join=+0.18; sector=+0.65; gen=+0.55 |
| UROY | +0.271 | Energy | join=+0.17; sector=+0.65; gen=+0.55 |

## 1w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.439 | Basic Materials | join=+0.20; sector=+0.62; gen=+0.55; rebound_floor |
| DNN | +0.429 | Energy | join=+0.17; sector=+0.65; gen=+0.28; rebound_floor |
| TGB | +0.424 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.55; rebound_floor |
| ELF | +0.419 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.55; rebound_floor |
| ERO | +0.399 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.28; rebound_floor |
| EL | +0.394 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.28; rebound_floor |
| MOS | +0.394 | Basic Materials | join=+0.15; sector=+0.62; gen=+0.28; rebound_floor |
| HNST | +0.387 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| DVN | +0.387 | Energy | join=+0.17; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| NB | +0.382 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.08; rebound_floor |
| FANG | +0.381 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.381 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OTF | +0.376 | Financial | join=+0.15; sector=+0.55; gen=+0.08; rebound_floor |
| OLLI | +0.376 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.08; rebound_floor |
| CWEN | +0.375 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |
| MUR | +0.372 | Energy | sector=+0.65; gen=+0.08; rebound_floor |
| CRH | +0.360 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| ECL | +0.360 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| GOF | +0.352 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| OXY | +0.350 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.347 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.347 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| WTW | +0.345 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| MRSH | +0.342 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| TLN | +0.340 | Utilities | sector=+0.55; gen=+0.55; news=+0.61; ev={'event': 'ai_power_demand', 'side': 'buy', 'weight': 3.51, 'bucket': 'utilities_power'} |

## 1w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AMBP | -0.183 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| GTX | -0.183 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| QSR | -0.180 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| HRB | -0.180 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| ORLY | -0.180 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| YUM | -0.180 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| GPC | -0.180 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| MCD | -0.180 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| ARCO | -0.180 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| LEG | -0.177 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| REYN | -0.177 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CHH | -0.177 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| SON | -0.177 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CHDN | -0.177 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| WH | -0.177 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| WEN | -0.177 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| TSCO | -0.177 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| SLGN | -0.177 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CASY | -0.177 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| ROL | -0.177 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| TXRH | -0.177 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CCK | -0.177 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| TJX | -0.177 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| DRI | -0.177 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| ALH | -0.175 | Consumer Cyclical | sector=-0.60; gen=+0.22 |

### 1w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| EL | +0.394 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.28; rebound_floor |
| DVN | +0.387 | Energy | join=+0.17; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.381 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.381 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| ECL | +0.360 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| CRH | +0.360 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| OXY | +0.350 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.347 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DNN | +0.429 | Energy | join=+0.17; sector=+0.65; gen=+0.28; rebound_floor |
| TGB | +0.424 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.55; rebound_floor |
| ELF | +0.419 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.55; rebound_floor |
| ERO | +0.399 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.28; rebound_floor |
| MOS | +0.394 | Basic Materials | join=+0.15; sector=+0.62; gen=+0.28; rebound_floor |
| OLLI | +0.376 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.08; rebound_floor |
| OTF | +0.376 | Financial | join=+0.15; sector=+0.55; gen=+0.08; rebound_floor |
| CWEN | +0.375 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.439 | Basic Materials | join=+0.20; sector=+0.62; gen=+0.55; rebound_floor |
| HNST | +0.387 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| NB | +0.382 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.08; rebound_floor |
| NRDY | +0.317 | Technology | sector=+0.28; gen=+0.55; rebound_floor |
| JLHL | +0.312 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| NEXT | +0.311 | Energy | join=+0.18; sector=+0.65; gen=+0.55 |
| PCT | +0.310 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| EU | +0.304 | Energy | join=+0.17; sector=+0.65; gen=+0.55 |

## 2w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.420 | Basic Materials | join=+0.20; sector=+0.62; gen=+0.55; rebound_floor |
| ERO | +0.404 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.28; rebound_floor |
| TGB | +0.404 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.55; rebound_floor |
| NB | +0.404 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.08; rebound_floor |
| DNN | +0.404 | Energy | join=+0.17; sector=+0.65; gen=+0.28; rebound_floor |
| MOS | +0.398 | Basic Materials | join=+0.15; sector=+0.62; gen=+0.28; rebound_floor |
| OTF | +0.398 | Financial | join=+0.15; sector=+0.55; gen=+0.08; rebound_floor |
| EL | +0.398 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.28; rebound_floor |
| ELF | +0.398 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.55; rebound_floor |
| OLLI | +0.398 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.08; rebound_floor |
| GOF | +0.373 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| WTW | +0.365 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| HNST | +0.365 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| CRH | +0.362 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| CWEN | +0.362 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |
| MUR | +0.362 | Energy | sector=+0.65; gen=+0.08; rebound_floor |
| MRSH | +0.362 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| ECL | +0.362 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| DVN | +0.313 | Energy | join=+0.17; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.307 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.307 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| TAL | +0.293 | Consumer Defensive | join=+0.24; sector=+0.60; gen=+0.08 |
| HLP | +0.293 | Basic Materials | join=+0.24; sector=+0.62; gen=+0.08 |
| RLX | +0.284 | Consumer Defensive | join=+0.23; sector=+0.60; gen=+0.28 |
| LX | +0.284 | Financial | join=+0.23; sector=+0.55; gen=+0.28 |

## 2w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ALH | -0.048 | Consumer Cyclical | sector=-0.60; gen=+0.22 |
| MWC | -0.048 | Consumer Cyclical | sector=-0.60; gen=+0.22 |
| JMKE | -0.048 | Consumer Cyclical | sector=-0.60; gen=+0.22 |
| VGNT | -0.048 | Consumer Cyclical | sector=-0.60; gen=+0.22 |
| SMJF | -0.048 | Consumer Cyclical | sector=-0.60; gen=+0.22 |
| REF | -0.048 | Consumer Cyclical | sector=-0.60; gen=+0.22 |
| GTX | -0.044 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| HLT | -0.044 | Consumer Cyclical | sector=-0.60; gen=+0.28 |
| TNL | -0.044 | Consumer Cyclical | sector=-0.60; gen=+0.28 |
| MAR | -0.044 | Consumer Cyclical | sector=-0.60; gen=+0.28 |
| ALV | -0.044 | Consumer Cyclical | sector=-0.60; gen=+0.55 |
| AMBP | -0.044 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| BOBS | -0.044 | Consumer Cyclical | sector=-0.60; gen=+0.22 |
| BKNG | -0.044 | Consumer Cyclical | sector=-0.60; gen=+0.28 |
| OSW | -0.044 | Consumer Cyclical | sector=-0.60; gen=+0.28 |
| ORLY | -0.040 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CRI | -0.040 | Consumer Cyclical | sector=-0.60; gen=+0.28 |
| GOOS | -0.040 | Consumer Cyclical | sector=-0.60; gen=+0.55 |
| CPRI | -0.040 | Consumer Cyclical | sector=-0.60; gen=+0.55 |
| WYNN | -0.040 | Consumer Cyclical | sector=-0.60; gen=+0.28 |
| GIII | -0.040 | Consumer Cyclical | sector=-0.60; gen=+0.28 |
| ARCO | -0.040 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| MGA | -0.040 | Consumer Cyclical | sector=-0.60; gen=+0.55 |
| RL | -0.040 | Consumer Cyclical | sector=-0.60; gen=+0.55 |
| JACK | -0.040 | Consumer Cyclical | sector=-0.60; gen=+0.55 |

### 2w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| EL | +0.398 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.28; rebound_floor |
| WTW | +0.365 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| ECL | +0.362 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| MRSH | +0.362 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| CRH | +0.362 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| DVN | +0.313 | Energy | join=+0.17; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.307 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.307 | Energy | join=+0.15; sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DNN | +0.404 | Energy | join=+0.17; sector=+0.65; gen=+0.28; rebound_floor |
| TGB | +0.404 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.55; rebound_floor |
| ERO | +0.404 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.28; rebound_floor |
| OTF | +0.398 | Financial | join=+0.15; sector=+0.55; gen=+0.08; rebound_floor |
| MOS | +0.398 | Basic Materials | join=+0.15; sector=+0.62; gen=+0.28; rebound_floor |
| ELF | +0.398 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.55; rebound_floor |
| OLLI | +0.398 | Consumer Defensive | join=+0.15; sector=+0.60; gen=+0.08; rebound_floor |
| GOF | +0.373 | Financial | sector=+0.55; gen=+0.08; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.420 | Basic Materials | join=+0.20; sector=+0.62; gen=+0.55; rebound_floor |
| NB | +0.404 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.08; rebound_floor |
| HNST | +0.365 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| HLP | +0.293 | Basic Materials | join=+0.24; sector=+0.62; gen=+0.08 |
| RLX | +0.284 | Consumer Defensive | join=+0.23; sector=+0.60; gen=+0.28 |
| LU | +0.284 | Financial | join=+0.23; sector=+0.55; gen=+0.28 |
| LX | +0.284 | Financial | join=+0.23; sector=+0.55; gen=+0.28 |
| BTQ | +0.281 | Technology | sector=+0.28; gen=+0.08; rebound_floor |

## 1m — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.499 | Basic Materials | join=+0.20; sector=+0.62; gen=+0.55; rebound_floor |
| TGB | +0.483 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.55; rebound_floor |
| DNN | +0.458 | Energy | join=+0.17; sector=+0.65; gen=+0.28; rebound_floor |
| ERO | +0.458 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.28; rebound_floor |
| MOS | +0.451 | Basic Materials | join=+0.15; sector=+0.62; gen=+0.28; rebound_floor |
| NB | +0.440 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.08; rebound_floor |
| OTF | +0.414 | Financial | join=+0.15; sector=+0.55; gen=+0.08; rebound_floor |
| CRH | +0.413 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| CWEN | +0.413 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |
| ECL | +0.413 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| MUR | +0.396 | Energy | sector=+0.65; gen=+0.08; rebound_floor |
| GOF | +0.387 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| WTW | +0.379 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| MRSH | +0.376 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| JLHL | +0.342 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| EFXT | +0.340 | Energy | join=+0.18; sector=+0.65; gen=+0.55 |
| VZLA | +0.340 | Basic Materials | join=+0.18; sector=+0.62; gen=+0.55 |
| CSTM | +0.340 | Basic Materials | join=+0.18; sector=+0.62; gen=+0.55 |
| SDRL | +0.340 | Energy | join=+0.18; sector=+0.65; gen=+0.55 |
| IAG | +0.340 | Basic Materials | join=+0.18; sector=+0.62; gen=+0.55 |
| HBM | +0.340 | Basic Materials | join=+0.18; sector=+0.62; gen=+0.55 |
| TMQ | +0.340 | Basic Materials | join=+0.18; sector=+0.62; gen=+0.55 |
| NEXT | +0.340 | Energy | join=+0.18; sector=+0.65; gen=+0.55 |
| NRDY | +0.338 | Technology | sector=+0.28; gen=+0.55; rebound_floor |
| TIC | +0.338 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |

## 1m — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| GTX | -0.038 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| AMBP | -0.038 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| GPC | -0.034 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| YUM | -0.034 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| HRB | -0.034 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| MCD | -0.034 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| QSR | -0.034 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| ARCO | -0.034 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| ORLY | -0.034 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| SLGN | -0.031 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| SON | -0.031 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| DRI | -0.031 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| ROL | -0.031 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| LEG | -0.031 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CASY | -0.031 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CCK | -0.031 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CHH | -0.031 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| REYN | -0.031 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| WH | -0.031 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| WEN | -0.031 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CHDN | -0.031 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| TJX | -0.031 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| TSCO | -0.031 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| TXRH | -0.031 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| JMKE | -0.030 | Consumer Cyclical | sector=-0.60; gen=+0.22 |

### 1m — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| CRH | +0.413 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| ECL | +0.413 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| WTW | +0.379 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| MRSH | +0.376 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| HBM | +0.340 | Basic Materials | join=+0.18; sector=+0.62; gen=+0.55 |
| IAG | +0.340 | Basic Materials | join=+0.18; sector=+0.62; gen=+0.55 |
| OC | +0.338 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| MT | +0.333 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.55 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TGB | +0.483 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.55; rebound_floor |
| ERO | +0.458 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.28; rebound_floor |
| DNN | +0.458 | Energy | join=+0.17; sector=+0.65; gen=+0.28; rebound_floor |
| MOS | +0.451 | Basic Materials | join=+0.15; sector=+0.62; gen=+0.28; rebound_floor |
| OTF | +0.414 | Financial | join=+0.15; sector=+0.55; gen=+0.08; rebound_floor |
| CWEN | +0.413 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |
| MUR | +0.396 | Energy | sector=+0.65; gen=+0.08; rebound_floor |
| GOF | +0.387 | Financial | sector=+0.55; gen=+0.08; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.499 | Basic Materials | join=+0.20; sector=+0.62; gen=+0.55; rebound_floor |
| NB | +0.440 | Basic Materials | join=+0.17; sector=+0.62; gen=+0.08; rebound_floor |
| JLHL | +0.342 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| VZLA | +0.340 | Basic Materials | join=+0.18; sector=+0.62; gen=+0.55 |
| NEXT | +0.340 | Energy | join=+0.18; sector=+0.65; gen=+0.55 |
| TMQ | +0.340 | Basic Materials | join=+0.18; sector=+0.62; gen=+0.55 |
| NRDY | +0.338 | Technology | sector=+0.28; gen=+0.55; rebound_floor |
| PCT | +0.338 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |

## Read

- **1d** news-heavy; **1m** structural join + sector.
- Universe gated: Market Cap ≥ $80M and Average Volume ≥ 500k shares (Finviz units).
- `rebound_floor` = checklist own-history score low + green-body bias (sparse; soft boost only).
- Raw checklist total score is NOT used as a buy rank (failed forward IC).
- Longer horizons use the predictors' explicit 3d/1w/2w/1m calls when stored, else fall back to the 1d call.
- Predictor bias is scaled by its graded hit rate (learning gate) — weak topics move scores less.
- Backtest: `python -m src.stock_book_backtest` (or Stock Book Backtest action).

CSV: `data/stock_book/2026-08-17_stock_book.csv`
JSON: `data/stock_book/2026-08-17_stock_book.json`
