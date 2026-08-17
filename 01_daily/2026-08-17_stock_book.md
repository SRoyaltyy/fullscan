# Stock book — **2026-08-17** (1d / 3d / 1w / 2w / 1m)

Generated: 2026-08-17T09:23:37.707913-04:00

Layers: join (labels×weather) + sector predict + general regime + news actions.

## Regime snapshot

- **General bias:** +0.55
- **Weather risk:** mixed
- **News tickers:** 29
- **Universe (after liquidity):** 2698
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
| DVN | +0.471 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.468 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.468 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.439 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.439 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.439 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| CVX | +0.380 | Energy | sector=+0.65; gen=+0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| XOM | +0.380 | Energy | sector=+0.65; gen=+0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| TMC | +0.337 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| TGB | +0.330 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| ELF | +0.324 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| DNN | +0.307 | Energy | sector=+0.65; gen=+0.28; rebound_floor |
| ERO | +0.305 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| MOS | +0.300 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| EL | +0.297 | Consumer Defensive | sector=+0.60; gen=+0.28; rebound_floor |
| HNST | +0.295 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| CWEN | +0.289 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |
| NB | +0.283 | Basic Materials | sector=+0.62; gen=+0.08; rebound_floor |
| JLHL | +0.278 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| OLLI | +0.277 | Consumer Defensive | sector=+0.60; gen=+0.08; rebound_floor |
| PCT | +0.275 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| NRDY | +0.275 | Technology | sector=+0.28; gen=+0.55; rebound_floor |
| GSAT | +0.275 | Communication Services | sector=+0.28; gen=+0.55; rebound_floor |
| OC | +0.275 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| TIC | +0.275 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |

## 1d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| WDAY | -0.286 | Technology | sector=+0.28; gen=+0.28; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| CRM | -0.286 | Technology | sector=+0.28; gen=+0.28; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| ADBE | -0.258 | Technology | sector=+0.28; gen=+0.55; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| TEAM | -0.257 | Technology | sector=+0.28; gen=+0.28; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| NOW | -0.257 | Technology | sector=+0.28; gen=+0.28; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| SNOW | -0.229 | Technology | sector=+0.28; gen=+0.55; news=-0.89; ev={'event': 'saas_multiple_compression', 'side': 'sell', 'weight': 4.2, 'bucket': 'software_app'},{'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| ALK | -0.176 | Industrials | sector=+0.28; gen=+0.28; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| LUV | -0.176 | Industrials | sector=+0.28; gen=+0.28; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| UAL | -0.174 | Industrials | sector=+0.28; gen=+0.28; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| DAL | -0.149 | Industrials | sector=+0.28; gen=+0.55; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| AAL | -0.146 | Industrials | sector=+0.28; gen=+0.55; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| BXP | -0.132 | Real Estate | sector=+0.60; gen=+0.28; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| ROL | -0.082 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| RPRX | -0.082 | Healthcare | sector=-0.60; gen=+0.08 |
| GPCR | -0.082 | Healthcare | sector=-0.60; gen=+0.08 |
| GPC | -0.082 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| LNTH | -0.082 | Healthcare | sector=-0.60; gen=+0.08 |
| REGN | -0.082 | Healthcare | sector=-0.60; gen=+0.08 |
| REYN | -0.082 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| BAX | -0.082 | Healthcare | sector=-0.60; gen=+0.08 |
| AURA | -0.082 | Healthcare | sector=-0.60; gen=+0.08 |
| CAPR | -0.082 | Healthcare | sector=-0.60; gen=+0.08 |
| LEG | -0.082 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CASY | -0.082 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| NAMS | -0.082 | Healthcare | sector=-0.60; gen=+0.08 |

### 1d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DVN | +0.471 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.468 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.468 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.439 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.439 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.439 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| CVX | +0.380 | Energy | sector=+0.65; gen=+0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| XOM | +0.380 | Energy | sector=+0.65; gen=+0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TGB | +0.330 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| ELF | +0.324 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| DNN | +0.307 | Energy | sector=+0.65; gen=+0.28; rebound_floor |
| ERO | +0.305 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| MOS | +0.300 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| CWEN | +0.289 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |
| OLLI | +0.277 | Consumer Defensive | sector=+0.60; gen=+0.08; rebound_floor |
| TIC | +0.275 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.337 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| HNST | +0.295 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| NB | +0.283 | Basic Materials | sector=+0.62; gen=+0.08; rebound_floor |
| JLHL | +0.278 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| PCT | +0.275 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| NRDY | +0.275 | Technology | sector=+0.28; gen=+0.55; rebound_floor |
| ABAT | +0.248 | Industrials | sector=+0.28; gen=+0.28; rebound_floor |
| BTQ | +0.234 | Technology | sector=+0.28; gen=+0.08; rebound_floor |

## 3d — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DVN | +0.403 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.400 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.400 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| TMC | +0.387 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| TGB | +0.379 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| ELF | +0.376 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| APA | +0.367 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.367 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.367 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| DNN | +0.364 | Energy | sector=+0.65; gen=+0.28; rebound_floor |
| ERO | +0.355 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| MOS | +0.348 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| CWEN | +0.348 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |
| EL | +0.348 | Consumer Defensive | sector=+0.60; gen=+0.28; rebound_floor |
| HNST | +0.343 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| NB | +0.332 | Basic Materials | sector=+0.62; gen=+0.08; rebound_floor |
| XOM | +0.330 | Energy | sector=+0.65; gen=+0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| CVX | +0.330 | Energy | sector=+0.65; gen=+0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| OTF | +0.329 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| OLLI | +0.329 | Consumer Defensive | sector=+0.60; gen=+0.08; rebound_floor |
| STWD | +0.328 | Real Estate | sector=+0.60; gen=+0.28; rebound_floor |
| ECL | +0.315 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| CRH | +0.315 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| MUR | +0.308 | Energy | sector=+0.65; gen=+0.08; rebound_floor |
| NRDY | +0.307 | Technology | sector=+0.28; gen=+0.55; rebound_floor |

## 3d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ZURA | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| ZTS | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| ABT | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| XFOR | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| XENE | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| OPCH | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| ORLY | -0.129 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| WH | -0.129 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| WEN | -0.129 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| DGX | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| DRI | -0.129 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| PFE | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| PCRX | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| LNTH | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| CAPR | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| CASY | -0.129 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| AZN | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| CADL | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| LRMR | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| WVE | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| BVS | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| LEG | -0.129 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| PBH | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| ADMA | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |
| CAH | -0.129 | Healthcare | sector=-0.60; gen=+0.08 |

### 3d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DVN | +0.403 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.400 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.400 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.367 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.367 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.367 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EL | +0.348 | Consumer Defensive | sector=+0.60; gen=+0.28; rebound_floor |
| CVX | +0.330 | Energy | sector=+0.65; gen=+0.08; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TGB | +0.379 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| ELF | +0.376 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| DNN | +0.364 | Energy | sector=+0.65; gen=+0.28; rebound_floor |
| ERO | +0.355 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| CWEN | +0.348 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |
| MOS | +0.348 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| OTF | +0.329 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| OLLI | +0.329 | Consumer Defensive | sector=+0.60; gen=+0.08; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.387 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| HNST | +0.343 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| NB | +0.332 | Basic Materials | sector=+0.62; gen=+0.08; rebound_floor |
| NRDY | +0.307 | Technology | sector=+0.28; gen=+0.55; rebound_floor |
| JLHL | +0.304 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| PCT | +0.301 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| ABAT | +0.273 | Industrials | sector=+0.28; gen=+0.28; rebound_floor |
| BTQ | +0.267 | Technology | sector=+0.28; gen=+0.08; rebound_floor |

## 1w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.400 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| DNN | +0.396 | Energy | sector=+0.65; gen=+0.28; rebound_floor |
| TGB | +0.391 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| ELF | +0.387 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| CWEN | +0.377 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |
| ERO | +0.370 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| EL | +0.362 | Consumer Defensive | sector=+0.60; gen=+0.28; rebound_floor |
| MOS | +0.362 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| DVN | +0.353 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| HNST | +0.350 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| EOG | +0.350 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.350 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| NB | +0.348 | Basic Materials | sector=+0.62; gen=+0.08; rebound_floor |
| OLLI | +0.345 | Consumer Defensive | sector=+0.60; gen=+0.08; rebound_floor |
| OTF | +0.345 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| STWD | +0.340 | Real Estate | sector=+0.60; gen=+0.28; rebound_floor |
| MUR | +0.338 | Energy | sector=+0.65; gen=+0.08; rebound_floor |
| CRH | +0.325 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| ECL | +0.325 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| NRDY | +0.320 | Technology | sector=+0.28; gen=+0.55; rebound_floor |
| JLHL | +0.316 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| APA | +0.313 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.313 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.313 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| TIC | +0.312 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |

## 1w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| YUM | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| REYN | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| QSR | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| ROL | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| TJX | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| TSCO | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| ORLY | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| WH | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| DRI | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| HRB | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CASY | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| TXRH | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| LEG | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| WEN | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| MCD | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| AMBP | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CCK | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| ARCO | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| SON | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| SLGN | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| GTX | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| GPC | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CHDN | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| CHH | -0.142 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| VGNT | -0.130 | Consumer Cyclical | sector=-0.60; gen=+0.22 |

### 1w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| EL | +0.362 | Consumer Defensive | sector=+0.60; gen=+0.28; rebound_floor |
| DVN | +0.353 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.350 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.350 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| ECL | +0.325 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| CRH | +0.325 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| OXY | +0.313 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.313 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| DNN | +0.396 | Energy | sector=+0.65; gen=+0.28; rebound_floor |
| TGB | +0.391 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| ELF | +0.387 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| CWEN | +0.377 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |
| ERO | +0.370 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| MOS | +0.362 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| OTF | +0.345 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| OLLI | +0.345 | Consumer Defensive | sector=+0.60; gen=+0.08; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.400 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| HNST | +0.350 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| NB | +0.348 | Basic Materials | sector=+0.62; gen=+0.08; rebound_floor |
| NRDY | +0.320 | Technology | sector=+0.28; gen=+0.55; rebound_floor |
| JLHL | +0.316 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| PCT | +0.312 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| ABAT | +0.287 | Industrials | sector=+0.28; gen=+0.28; rebound_floor |
| BTQ | +0.285 | Technology | sector=+0.28; gen=+0.08; rebound_floor |

## 2w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.378 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| ERO | +0.373 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| TGB | +0.369 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| DNN | +0.369 | Energy | sector=+0.65; gen=+0.28; rebound_floor |
| NB | +0.369 | Basic Materials | sector=+0.62; gen=+0.08; rebound_floor |
| CWEN | +0.365 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |
| MOS | +0.365 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| OLLI | +0.365 | Consumer Defensive | sector=+0.60; gen=+0.08; rebound_floor |
| ELF | +0.365 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| EL | +0.365 | Consumer Defensive | sector=+0.60; gen=+0.28; rebound_floor |
| OTF | +0.365 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| GOF | +0.325 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| MUR | +0.325 | Energy | sector=+0.65; gen=+0.08; rebound_floor |
| MRSH | +0.325 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| WTW | +0.325 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| CRH | +0.325 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| HNST | +0.325 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| ECL | +0.325 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| BTQ | +0.285 | Technology | sector=+0.28; gen=+0.08; rebound_floor |
| JLHL | +0.281 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| HAWK | +0.281 | Industrials | sector=+0.28; gen=+0.22; rebound_floor |
| SVCO | +0.277 | Technology | sector=+0.28; gen=+0.08; rebound_floor |
| NRDY | +0.277 | Technology | sector=+0.28; gen=+0.55; rebound_floor |
| ABAT | +0.277 | Industrials | sector=+0.28; gen=+0.28; rebound_floor |
| PCT | +0.277 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |

## 2w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| BXP | -0.044 | Real Estate | sector=+0.60; gen=+0.28; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| VNO | -0.000 | Real Estate | sector=+0.60; gen=+0.55; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| SLG | -0.000 | Real Estate | sector=+0.60; gen=+0.55; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| SHO | +0.000 | Real Estate | sector=+0.60; gen=+0.28 |
| DECK | +0.000 | Consumer Cyclical | sector=-0.60; gen=+0.28 |
| SBAC | +0.000 | Real Estate | sector=+0.60; gen=+0.28 |
| AVTR | +0.000 | Healthcare | sector=-0.60; gen=+0.28 |
| AZN | +0.000 | Healthcare | sector=-0.60; gen=+0.08 |
| RZLT | +0.000 | Healthcare | sector=-0.60; gen=+0.08 |
| RXST | +0.000 | Healthcare | sector=-0.60; gen=+0.28 |
| RXRX | +0.000 | Healthcare | sector=-0.60; gen=+0.28 |
| RVTY | +0.000 | Healthcare | sector=-0.60; gen=+0.28 |
| RRR | +0.000 | Consumer Cyclical | sector=-0.60; gen=+0.55 |
| ROST | +0.000 | Consumer Cyclical | sector=-0.60; gen=+0.28 |
| NAUT | +0.000 | Healthcare | sector=-0.60; gen=+0.28 |
| NAMS | +0.000 | Healthcare | sector=-0.60; gen=+0.08 |
| SION | +0.000 | Healthcare | sector=-0.60; gen=+0.08 |
| SIGA | +0.000 | Healthcare | sector=-0.60; gen=+0.28 |
| AVB | +0.000 | Real Estate | sector=+0.60; gen=+0.08 |
| MLTX | +0.000 | Healthcare | sector=-0.60; gen=+0.28 |
| SIG | +0.000 | Consumer Cyclical | sector=-0.60; gen=+0.28 |
| SHC | +0.000 | Healthcare | sector=-0.60; gen=+0.55 |
| AURA | +0.000 | Healthcare | sector=-0.60; gen=+0.08 |
| SGMT | +0.000 | Healthcare | sector=-0.60; gen=+0.55 |
| SG | +0.000 | Consumer Cyclical | sector=-0.60; gen=+0.55 |

### 2w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| EL | +0.365 | Consumer Defensive | sector=+0.60; gen=+0.28; rebound_floor |
| ECL | +0.325 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| MRSH | +0.325 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| CRH | +0.325 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| WTW | +0.325 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| OC | +0.277 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| DVN | +0.277 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.273 | Energy | sector=+0.65; gen=+0.08; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ERO | +0.373 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| DNN | +0.369 | Energy | sector=+0.65; gen=+0.28; rebound_floor |
| TGB | +0.369 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| OLLI | +0.365 | Consumer Defensive | sector=+0.60; gen=+0.08; rebound_floor |
| OTF | +0.365 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| MOS | +0.365 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| ELF | +0.365 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| CWEN | +0.365 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.378 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| NB | +0.369 | Basic Materials | sector=+0.62; gen=+0.08; rebound_floor |
| HNST | +0.325 | Consumer Defensive | sector=+0.60; gen=+0.55; rebound_floor |
| BTQ | +0.285 | Technology | sector=+0.28; gen=+0.08; rebound_floor |
| JLHL | +0.281 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| SVCO | +0.277 | Technology | sector=+0.28; gen=+0.08; rebound_floor |
| NRDY | +0.277 | Technology | sector=+0.28; gen=+0.55; rebound_floor |
| PCT | +0.277 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |

## 1m — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.455 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| TGB | +0.445 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| ERO | +0.425 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| DNN | +0.420 | Energy | sector=+0.65; gen=+0.28; rebound_floor |
| MOS | +0.417 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| CWEN | +0.417 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |
| NB | +0.403 | Basic Materials | sector=+0.62; gen=+0.08; rebound_floor |
| OTF | +0.379 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| CRH | +0.375 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| ECL | +0.375 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| MUR | +0.358 | Energy | sector=+0.65; gen=+0.08; rebound_floor |
| JLHL | +0.345 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| NRDY | +0.342 | Technology | sector=+0.28; gen=+0.55; rebound_floor |
| OC | +0.342 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| TIC | +0.342 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| PCT | +0.342 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| GOF | +0.338 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| MRSH | +0.338 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| WTW | +0.338 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| ABAT | +0.317 | Industrials | sector=+0.28; gen=+0.28; rebound_floor |
| HAWK | +0.315 | Industrials | sector=+0.28; gen=+0.22; rebound_floor |
| BTQ | +0.307 | Technology | sector=+0.28; gen=+0.08; rebound_floor |
| OLED | +0.300 | Technology | sector=+0.28; gen=+0.55; rebound_floor |
| TMQ | +0.300 | Basic Materials | sector=+0.62; gen=+0.55 |
| CSTM | +0.300 | Basic Materials | sector=+0.62; gen=+0.55 |

## 1m — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ZURA | +0.007 | Healthcare | sector=-0.60; gen=+0.08 |
| ZTS | +0.007 | Healthcare | sector=-0.60; gen=+0.08 |
| ABEV | +0.007 | Consumer Defensive | sector=+0.60; gen=+0.08 |
| EPC | +0.007 | Consumer Defensive | sector=+0.60; gen=+0.08 |
| ACHC | +0.007 | Healthcare | sector=-0.60; gen=+0.08 |
| ENGN | +0.007 | Healthcare | sector=-0.60; gen=+0.08 |
| ELS | +0.007 | Real Estate | sector=+0.60; gen=+0.08 |
| ACI | +0.007 | Consumer Defensive | sector=+0.60; gen=+0.08 |
| BTI | +0.007 | Consumer Defensive | sector=+0.60; gen=+0.08 |
| MKC | +0.007 | Consumer Defensive | sector=+0.60; gen=+0.08 |
| XFOR | +0.007 | Healthcare | sector=-0.60; gen=+0.08 |
| XENE | +0.007 | Healthcare | sector=-0.60; gen=+0.08 |
| YUM | +0.007 | Consumer Cyclical | sector=-0.60; gen=+0.08 |
| WVE | +0.007 | Healthcare | sector=-0.60; gen=+0.08 |
| VZ | +0.007 | Communication Services | sector=+0.28; gen=+0.08 |
| PHG | +0.007 | Healthcare | sector=-0.60; gen=+0.08 |
| VRTX | +0.007 | Healthcare | sector=-0.60; gen=+0.08 |
| PFE | +0.007 | Healthcare | sector=-0.60; gen=+0.08 |
| PECO | +0.007 | Real Estate | sector=+0.60; gen=+0.08 |
| PEP | +0.007 | Consumer Defensive | sector=+0.60; gen=+0.08 |
| PCRX | +0.007 | Healthcare | sector=-0.60; gen=+0.08 |
| PBH | +0.007 | Healthcare | sector=-0.60; gen=+0.08 |
| BJ | +0.007 | Consumer Defensive | sector=+0.60; gen=+0.08 |
| COR | +0.007 | Healthcare | sector=-0.60; gen=+0.08 |
| EHC | +0.007 | Healthcare | sector=-0.60; gen=+0.08 |

### 1m — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ECL | +0.375 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| CRH | +0.375 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| OC | +0.342 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| MRSH | +0.338 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| WTW | +0.338 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| IAG | +0.300 | Basic Materials | sector=+0.62; gen=+0.55 |
| HBM | +0.300 | Basic Materials | sector=+0.62; gen=+0.55 |
| EQX | +0.295 | Basic Materials | sector=+0.62; gen=+0.55 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TGB | +0.445 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| ERO | +0.425 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| DNN | +0.420 | Energy | sector=+0.65; gen=+0.28; rebound_floor |
| MOS | +0.417 | Basic Materials | sector=+0.62; gen=+0.28; rebound_floor |
| CWEN | +0.417 | Utilities | sector=+0.55; gen=+0.28; rebound_floor |
| OTF | +0.379 | Financial | sector=+0.55; gen=+0.08; rebound_floor |
| MUR | +0.358 | Energy | sector=+0.65; gen=+0.08; rebound_floor |
| TIC | +0.342 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TMC | +0.455 | Basic Materials | sector=+0.62; gen=+0.55; rebound_floor |
| NB | +0.403 | Basic Materials | sector=+0.62; gen=+0.08; rebound_floor |
| JLHL | +0.345 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| NRDY | +0.342 | Technology | sector=+0.28; gen=+0.55; rebound_floor |
| PCT | +0.342 | Industrials | sector=+0.28; gen=+0.55; rebound_floor |
| ABAT | +0.317 | Industrials | sector=+0.28; gen=+0.28; rebound_floor |
| BTQ | +0.307 | Technology | sector=+0.28; gen=+0.08; rebound_floor |
| NEXT | +0.300 | Energy | sector=+0.65; gen=+0.55 |

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
