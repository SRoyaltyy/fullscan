# Stock book — **2026-08-13** (1d / 3d / 1w / 2w / 1m)

Generated: 2026-08-13T13:41:51.517716-04:00

Layers: join (labels×weather) + sector predict + general regime + news actions.

## Regime snapshot

- **General bias:** +0.60
- **Weather risk:** on
- **News tickers:** 13
- **Universe:** 11579

### Sector bias

| Sector | bias |
|--------|------|
| Healthcare | +0.65 |
| Basic Materials | -0.60 |
| Real Estate | +0.60 |
| Energy | -0.55 |
| Financial | +0.55 |
| Utilities | +0.55 |
| Consumer Cyclical | +0.50 |
| Technology | +0.35 |
| Communication Services | -0.30 |
| Consumer Defensive | +0.28 |
| Industrials | +0.28 |

### Learning gate (graded accuracy → how much each predictor is trusted)

Bias from a topic with a weak graded track record is scaled down before it can move scores.

| Topic | hit rate | graded runs | weight applied |
|-------|----------|-------------|----------------|
| general | 58% | 12 | ×1.00 |
| sector:Basic Materials | 67% | 3 | ×1.00 |
| sector:Communication Services | 33% | 3 | ×0.50 |
| sector:Consumer Cyclical | 67% | 3 | ×1.00 |
| sector:Consumer Defensive | 33% | 3 | ×0.50 |
| sector:Energy | 67% | 3 | ×1.00 |
| sector:Financial | 67% | 3 | ×1.00 |
| sector:Healthcare | 67% | 3 | ×1.00 |
| sector:Industrials | 33% | 3 | ×0.50 |
| sector:Real Estate | 67% | 3 | ×1.00 |
| sector:Technology | 33% | 3 | ×0.50 |
| sector:Utilities | 67% | 3 | ×1.00 |

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
| TKVA | +0.388 | Healthcare | join=+0.76; sector=+0.65; gen=+0.24 |
| SKAIU | +0.373 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| IDIAU | +0.373 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| BCACU | +0.373 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| LEDRU | +0.373 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| SCATU | +0.373 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| APNAU | +0.373 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| PHAXU | +0.373 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| TNDM | +0.361 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| INO | +0.361 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| ACHV | +0.354 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| VOR | +0.354 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| AGEN | +0.354 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| WW | +0.349 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| SGRY | +0.349 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| INBX | +0.341 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| IMNN | +0.341 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| NRXP | +0.341 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| GOSS | +0.341 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| FTRE | +0.341 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| SPRB | +0.341 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| TGTX | +0.341 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| UNCY | +0.341 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| FDMT | +0.341 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| CERS | +0.341 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |

## 1d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| LUV | -0.145 | Industrials | join=+0.17; sector=+0.28; gen=+0.30; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| UAL | -0.124 | Industrials | join=+0.23; sector=+0.28; gen=+0.30; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| ALK | -0.099 | Industrials | join=+0.30; sector=+0.28; gen=+0.30; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| DAL | -0.094 | Industrials | join=+0.23; sector=+0.28; gen=+0.60; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| LIN | -0.081 | Basic Materials | sector=-0.60; gen=+0.09 |
| AAL | -0.061 | Industrials | join=+0.32; sector=+0.28; gen=+0.60; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| CTVA | -0.054 | Basic Materials | sector=-0.60; gen=+0.09 |
| NTR | -0.052 | Basic Materials | sector=-0.60; gen=+0.09 |
| VALE | -0.052 | Basic Materials | sector=-0.60; gen=+0.09 |
| LYB | -0.052 | Basic Materials | sector=-0.60; gen=+0.09 |
| SSL | -0.049 | Basic Materials | sector=-0.60; gen=+0.09 |
| RIO | -0.049 | Basic Materials | sector=-0.60; gen=+0.09 |
| VLO | -0.047 | Energy | sector=-0.55; gen=+0.09 |
| IMO | -0.044 | Energy | sector=-0.55; gen=+0.09 |
| CVE | -0.044 | Energy | sector=-0.55; gen=+0.09 |
| E | -0.044 | Energy | sector=-0.55; gen=+0.09 |
| EQNR | -0.044 | Energy | sector=-0.55; gen=+0.09 |
| SU | -0.044 | Energy | sector=-0.55; gen=+0.09 |
| TTE | -0.044 | Energy | sector=-0.55; gen=+0.09 |
| SHEL | -0.044 | Energy | sector=-0.55; gen=+0.09 |
| PBA | -0.044 | Energy | sector=-0.55; gen=+0.09 |
| WDS | -0.042 | Energy | sector=-0.55; gen=+0.09 |
| TRMD | -0.042 | Energy | sector=-0.55; gen=+0.09 |
| SKM | -0.036 | Communication Services | sector=-0.30; gen=+0.09 |
| FOX | -0.036 | Communication Services | sector=-0.30; gen=+0.09 |

### 1d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| BTSG | +0.319 | Healthcare | join=+0.46; sector=+0.65; gen=+0.60 |
| EOG | +0.313 | Energy | join=+0.15; sector=-0.55; gen=+0.09; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.313 | Energy | join=+0.15; sector=-0.55; gen=+0.09; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| DVN | +0.313 | Energy | join=+0.15; sector=-0.55; gen=+0.09; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| IREN | +0.304 | Financial | join=+0.46; sector=+0.55; gen=+0.60 |
| GH | +0.295 | Healthcare | join=+0.39; sector=+0.65; gen=+0.60 |
| RVMD | +0.295 | Healthcare | join=+0.39; sector=+0.65; gen=+0.60 |
| PRAX | +0.295 | Healthcare | join=+0.39; sector=+0.65; gen=+0.60 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TGTX | +0.341 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| SLS | +0.331 | Healthcare | join=+0.50; sector=+0.65; gen=+0.60 |
| HIMS | +0.330 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| NUVB | +0.330 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| TMDX | +0.330 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| TEM | +0.330 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| WRBY | +0.319 | Healthcare | join=+0.46; sector=+0.65; gen=+0.60 |
| KOD | +0.319 | Healthcare | join=+0.46; sector=+0.65; gen=+0.60 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| INO | +0.361 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| TNDM | +0.361 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| VOR | +0.354 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| AGEN | +0.354 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| ACHV | +0.354 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| WW | +0.349 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| SGRY | +0.349 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| NRXP | +0.341 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |

## 3d — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TKVA | +0.477 | Healthcare | join=+0.76; sector=+0.65; gen=+0.24 |
| SKAIU | +0.464 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| APNAU | +0.464 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| LEDRU | +0.464 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| SCATU | +0.464 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| BCACU | +0.464 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| PHAXU | +0.464 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| IDIAU | +0.464 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| TNDM | +0.438 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| INO | +0.438 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| ACHV | +0.430 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| AGEN | +0.430 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| VOR | +0.430 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| CLRS | +0.425 | Financial | join=+0.66; sector=+0.55; gen=+0.24 |
| WW | +0.424 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| SGRY | +0.424 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| PRCH | +0.417 | Financial | join=+0.56; sector=+0.55; gen=+0.60 |
| NVAX | +0.415 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| TGTX | +0.415 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| ABEO | +0.415 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| CARL | +0.415 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| PROK | +0.415 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| INBX | +0.415 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| FDMT | +0.415 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| SPRB | +0.415 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |

## 3d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| LIN | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| CTVA | -0.099 | Basic Materials | sector=-0.60; gen=+0.09 |
| NTR | -0.096 | Basic Materials | sector=-0.60; gen=+0.09 |
| LYB | -0.096 | Basic Materials | sector=-0.60; gen=+0.09 |
| VALE | -0.096 | Basic Materials | sector=-0.60; gen=+0.09 |
| RIO | -0.093 | Basic Materials | sector=-0.60; gen=+0.09 |
| SSL | -0.093 | Basic Materials | sector=-0.60; gen=+0.09 |
| VLO | -0.086 | Energy | sector=-0.55; gen=+0.09 |
| SHEL | -0.084 | Energy | sector=-0.55; gen=+0.09 |
| EQNR | -0.084 | Energy | sector=-0.55; gen=+0.09 |
| TTE | -0.084 | Energy | sector=-0.55; gen=+0.09 |
| PBA | -0.084 | Energy | sector=-0.55; gen=+0.09 |
| IMO | -0.084 | Energy | sector=-0.55; gen=+0.09 |
| CVE | -0.084 | Energy | sector=-0.55; gen=+0.09 |
| E | -0.084 | Energy | sector=-0.55; gen=+0.09 |
| SU | -0.084 | Energy | sector=-0.55; gen=+0.09 |
| TRMD | -0.080 | Energy | sector=-0.55; gen=+0.09 |
| WDS | -0.080 | Energy | sector=-0.55; gen=+0.09 |
| PPG | -0.079 | Basic Materials | sector=-0.60; gen=+0.30 |
| VMC | -0.079 | Basic Materials | sector=-0.60; gen=+0.30 |
| MLM | -0.079 | Basic Materials | sector=-0.60; gen=+0.30 |
| CRH | -0.079 | Basic Materials | sector=-0.60; gen=+0.30 |
| IFF | -0.079 | Basic Materials | sector=-0.60; gen=+0.30 |
| BHP | -0.077 | Basic Materials | sector=-0.60; gen=+0.30 |
| DD | -0.077 | Basic Materials | sector=-0.60; gen=+0.30 |

### 3d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| BTSG | +0.390 | Healthcare | join=+0.46; sector=+0.65; gen=+0.60 |
| IREN | +0.377 | Financial | join=+0.46; sector=+0.55; gen=+0.60 |
| SOFI | +0.365 | Financial | join=+0.43; sector=+0.55; gen=+0.60 |
| CG | +0.365 | Financial | join=+0.43; sector=+0.55; gen=+0.60 |
| TPG | +0.365 | Financial | join=+0.43; sector=+0.55; gen=+0.60 |
| HUT | +0.365 | Financial | join=+0.43; sector=+0.55; gen=+0.60 |
| JEF | +0.365 | Financial | join=+0.43; sector=+0.55; gen=+0.60 |
| RKT | +0.365 | Financial | join=+0.43; sector=+0.55; gen=+0.60 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TGTX | +0.415 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| SLS | +0.404 | Healthcare | join=+0.50; sector=+0.65; gen=+0.60 |
| NUVB | +0.402 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| TMDX | +0.402 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| HIMS | +0.402 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| TEM | +0.402 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| WRBY | +0.390 | Healthcare | join=+0.46; sector=+0.65; gen=+0.60 |
| RDNT | +0.390 | Healthcare | join=+0.46; sector=+0.65; gen=+0.60 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TNDM | +0.438 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| INO | +0.438 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| ACHV | +0.430 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| VOR | +0.430 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| AGEN | +0.430 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| SGRY | +0.424 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| WW | +0.424 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| PRCH | +0.417 | Financial | join=+0.56; sector=+0.55; gen=+0.60 |

## 1w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TKVA | +0.543 | Healthcare | join=+0.76; sector=+0.65; gen=+0.24 |
| PHAXU | +0.513 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| SKAIU | +0.513 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| LEDRU | +0.513 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| BCACU | +0.513 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| APNAU | +0.513 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| SCATU | +0.513 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| IDIAU | +0.513 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| TNDM | +0.492 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| INO | +0.492 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| VOR | +0.483 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| AGEN | +0.483 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| ACHV | +0.483 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| WW | +0.477 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| SGRY | +0.477 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| CLRS | +0.469 | Financial | join=+0.66; sector=+0.55; gen=+0.24 |
| CERS | +0.466 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| FDMT | +0.466 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| SPRB | +0.466 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| TGTX | +0.466 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| UNCY | +0.466 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| AQST | +0.466 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| MBRX | +0.466 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| NVAX | +0.466 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| FTRE | +0.466 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |

## 1w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TIMB | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| FOX | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| KT | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| TLK | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| ATHM | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| SKM | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| PSO | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| BIDU | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| NTES | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| JOYY | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| CHT | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| BZ | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| NWS | -0.050 | Communication Services | sector=-0.30; gen=+0.30 |
| NWSA | -0.050 | Communication Services | sector=-0.30; gen=+0.30 |
| KYIV | -0.050 | Communication Services | sector=-0.30; gen=+0.30 |
| LBTYK | -0.050 | Communication Services | sector=-0.30; gen=+0.30 |
| LBTYA | -0.050 | Communication Services | sector=-0.30; gen=+0.30 |
| EA | -0.033 | Communication Services | sector=-0.30; gen=+0.09 |
| T | -0.033 | Communication Services | sector=-0.30; gen=+0.09 |
| CMCSA | -0.033 | Communication Services | sector=-0.30; gen=+0.09 |
| WIMI | -0.033 | Communication Services | sector=-0.30; gen=+0.09 |
| WB | -0.033 | Communication Services | sector=-0.30; gen=+0.09 |
| MOMO | -0.033 | Communication Services | sector=-0.30; gen=+0.09 |
| BILI | -0.033 | Communication Services | sector=-0.30; gen=+0.09 |
| MF | -0.033 | Communication Services | sector=-0.30; gen=+0.09 |

### 1w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| BTSG | +0.438 | Healthcare | join=+0.46; sector=+0.65; gen=+0.60 |
| IREN | +0.408 | Financial | join=+0.46; sector=+0.55; gen=+0.60 |
| GH | +0.407 | Healthcare | join=+0.39; sector=+0.65; gen=+0.60 |
| PRAX | +0.407 | Healthcare | join=+0.39; sector=+0.65; gen=+0.60 |
| RVMD | +0.407 | Healthcare | join=+0.39; sector=+0.65; gen=+0.60 |
| NTRA | +0.395 | Healthcare | join=+0.37; sector=+0.65; gen=+0.60 |
| JEF | +0.394 | Financial | join=+0.43; sector=+0.55; gen=+0.60 |
| AFRM | +0.394 | Financial | join=+0.43; sector=+0.55; gen=+0.60 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TGTX | +0.466 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| SLS | +0.454 | Healthcare | join=+0.50; sector=+0.65; gen=+0.60 |
| NUVB | +0.451 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| HIMS | +0.451 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| TMDX | +0.451 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| TEM | +0.451 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| WRBY | +0.438 | Healthcare | join=+0.46; sector=+0.65; gen=+0.60 |
| RDNT | +0.438 | Healthcare | join=+0.46; sector=+0.65; gen=+0.60 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TNDM | +0.492 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| INO | +0.492 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| ACHV | +0.483 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| AGEN | +0.483 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| VOR | +0.483 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| WW | +0.477 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| SGRY | +0.477 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| PROK | +0.466 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |

## 2w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TKVA | +0.558 | Healthcare | join=+0.76; sector=+0.65; gen=+0.24 |
| LEDRU | +0.523 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| PHAXU | +0.523 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| BCACU | +0.523 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| SKAIU | +0.523 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| APNAU | +0.523 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| SCATU | +0.523 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| IDIAU | +0.523 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| CLRS | +0.476 | Financial | join=+0.66; sector=+0.55; gen=+0.24 |
| INO | +0.472 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| TNDM | +0.472 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| VOR | +0.462 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| ACHV | +0.462 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| AGEN | +0.462 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| SGRY | +0.456 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| WW | +0.456 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| INBX | +0.445 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| PROK | +0.445 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| AQST | +0.445 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| STIM | +0.445 | Healthcare | join=+0.53; sector=+0.65; gen=+0.30 |
| TGTX | +0.445 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| CARL | +0.445 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| IMNN | +0.445 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| SPRB | +0.445 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| ANAB | +0.445 | Healthcare | join=+0.53; sector=+0.65; gen=+0.30 |

## 2w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TIMB | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| ATHM | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| SKM | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| PSO | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| TLK | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| BIDU | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| NWS | +0.000 | Communication Services | sector=-0.30; gen=+0.30 |
| NWSA | +0.000 | Communication Services | sector=-0.30; gen=+0.30 |
| LIN | +0.000 | Basic Materials | sector=-0.60; gen=+0.09 |
| BZ | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| CHT | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| KYIV | +0.000 | Communication Services | sector=-0.30; gen=+0.30 |
| JOYY | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| LBTYK | +0.000 | Communication Services | sector=-0.30; gen=+0.30 |
| LBTYA | +0.000 | Communication Services | sector=-0.30; gen=+0.30 |
| NTES | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| FOX | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| KT | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| MF | +0.037 | Communication Services | sector=-0.30; gen=+0.09 |
| WB | +0.037 | Communication Services | sector=-0.30; gen=+0.09 |
| ARE | +0.037 | Real Estate | sector=+0.60; gen=+0.30 |
| TTD | +0.037 | Communication Services | sector=-0.30; gen=+0.30 |
| META | +0.037 | Communication Services | sector=-0.30; gen=+0.30 |
| VICI | +0.037 | Real Estate | sector=+0.60; gen=+0.09 |
| MLM | +0.037 | Basic Materials | sector=-0.60; gen=+0.30 |

### 2w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| BTSG | +0.414 | Healthcare | join=+0.46; sector=+0.65; gen=+0.60 |
| PRAX | +0.382 | Healthcare | join=+0.39; sector=+0.65; gen=+0.60 |
| RVMD | +0.382 | Healthcare | join=+0.39; sector=+0.65; gen=+0.60 |
| GH | +0.382 | Healthcare | join=+0.39; sector=+0.65; gen=+0.60 |
| AXSM | +0.382 | Healthcare | join=+0.39; sector=+0.65; gen=+0.09 |
| IREN | +0.379 | Financial | join=+0.46; sector=+0.55; gen=+0.60 |
| HALO | +0.369 | Healthcare | join=+0.37; sector=+0.65; gen=+0.30 |
| ARWR | +0.369 | Healthcare | join=+0.37; sector=+0.65; gen=+0.30 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TGTX | +0.445 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| SLS | +0.431 | Healthcare | join=+0.50; sector=+0.65; gen=+0.60 |
| TMDX | +0.429 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| NUVB | +0.429 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| TEM | +0.429 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| HIMS | +0.429 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| WRBY | +0.414 | Healthcare | join=+0.46; sector=+0.65; gen=+0.60 |
| KOD | +0.414 | Healthcare | join=+0.46; sector=+0.65; gen=+0.60 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TNDM | +0.472 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| INO | +0.472 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| AGEN | +0.462 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| ACHV | +0.462 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| VOR | +0.462 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| WW | +0.456 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| SGRY | +0.456 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| NVAX | +0.445 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |

## 1m — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TKVA | +0.601 | Healthcare | join=+0.76; sector=+0.65; gen=+0.24 |
| SKAIU | +0.581 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| BCACU | +0.581 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| LEDRU | +0.581 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| PHAXU | +0.581 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| APNAU | +0.581 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| IDIAU | +0.581 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| SCATU | +0.581 | Financial | join=+0.76; sector=+0.55; gen=+0.24 |
| CLRS | +0.532 | Financial | join=+0.66; sector=+0.55; gen=+0.24 |
| TNDM | +0.511 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| INO | +0.511 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| AGEN | +0.501 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| ACHV | +0.501 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| VOR | +0.501 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| SGRY | +0.494 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| WW | +0.494 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| INBX | +0.483 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| ABEO | +0.483 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| PROK | +0.483 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| NVAX | +0.483 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| TGTX | +0.483 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| NRXP | +0.483 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| ANAB | +0.483 | Healthcare | join=+0.53; sector=+0.65; gen=+0.30 |
| IMNN | +0.483 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| GOSS | +0.483 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |

## 1m — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TIMB | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| FOX | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| TLK | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| ATHM | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| SKM | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| PSO | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| BIDU | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| NWS | +0.000 | Communication Services | sector=-0.30; gen=+0.30 |
| CHT | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| KYIV | +0.000 | Communication Services | sector=-0.30; gen=+0.30 |
| LBTYA | +0.000 | Communication Services | sector=-0.30; gen=+0.30 |
| NTES | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| BZ | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| NWSA | +0.000 | Communication Services | sector=-0.30; gen=+0.30 |
| KT | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| JOYY | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| LBTYK | +0.000 | Communication Services | sector=-0.30; gen=+0.30 |
| META | +0.038 | Communication Services | sector=-0.30; gen=+0.30 |
| SOHU | +0.038 | Communication Services | sector=-0.30; gen=+0.09 |
| WB | +0.038 | Communication Services | sector=-0.30; gen=+0.09 |
| MKC | +0.038 | Consumer Defensive | sector=+0.28; gen=+0.09 |
| TME | +0.038 | Communication Services | sector=-0.30; gen=+0.30 |
| T | +0.038 | Communication Services | sector=-0.30; gen=+0.09 |
| MF | +0.038 | Communication Services | sector=-0.30; gen=+0.09 |
| BILI | +0.038 | Communication Services | sector=-0.30; gen=+0.09 |

### 1m — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| BTSG | +0.451 | Healthcare | join=+0.46; sector=+0.65; gen=+0.60 |
| IREN | +0.431 | Financial | join=+0.46; sector=+0.55; gen=+0.60 |
| PRAX | +0.417 | Healthcare | join=+0.39; sector=+0.65; gen=+0.60 |
| AXSM | +0.417 | Healthcare | join=+0.39; sector=+0.65; gen=+0.09 |
| RVMD | +0.417 | Healthcare | join=+0.39; sector=+0.65; gen=+0.60 |
| GH | +0.417 | Healthcare | join=+0.39; sector=+0.65; gen=+0.60 |
| CG | +0.416 | Financial | join=+0.43; sector=+0.55; gen=+0.60 |
| TPG | +0.416 | Financial | join=+0.43; sector=+0.55; gen=+0.60 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| TGTX | +0.483 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |
| SLS | +0.469 | Healthcare | join=+0.50; sector=+0.65; gen=+0.60 |
| NUVB | +0.466 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| HIMS | +0.466 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| TMDX | +0.466 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| TEM | +0.466 | Healthcare | join=+0.49; sector=+0.65; gen=+0.60 |
| CELC | +0.451 | Healthcare | join=+0.46; sector=+0.65; gen=+0.09 |
| ESTA | +0.451 | Healthcare | join=+0.46; sector=+0.65; gen=+0.30 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| INO | +0.511 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| TNDM | +0.511 | Healthcare | join=+0.58; sector=+0.65; gen=+0.60 |
| ACHV | +0.501 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| AGEN | +0.501 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| VOR | +0.501 | Healthcare | join=+0.56; sector=+0.65; gen=+0.60 |
| WW | +0.494 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| SGRY | +0.494 | Healthcare | join=+0.55; sector=+0.65; gen=+0.60 |
| ABEO | +0.483 | Healthcare | join=+0.53; sector=+0.65; gen=+0.60 |

## Read

- **1d** news-heavy; **1m** structural join + sector.
- Longer horizons use the predictors' explicit 3d/1w/2w/1m calls when stored, else fall back to the 1d call.
- Predictor bias is scaled by its graded hit rate (learning gate) — weak topics move scores less.
- Backtest: `python -m src.stock_book_backtest` (or Stock Book Backtest action).

CSV: `data/stock_book/2026-08-13_stock_book.csv`
JSON: `data/stock_book/2026-08-13_stock_book.json`
