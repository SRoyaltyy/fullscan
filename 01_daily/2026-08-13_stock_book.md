# Stock book — **2026-08-13** (1d / 3d / 1w / 2w / 1m)

Generated: 2026-08-13T07:32:06.290739-04:00

Layers: join (labels×weather) + sector predict + general regime + news actions.

## Regime snapshot

- **General bias:** +0.60
- **Weather risk:** unknown
- **News tickers:** 29
- **Universe:** 11568

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
| WFC | +0.317 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| BAC | +0.317 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| C | +0.317 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| JPM | +0.317 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| DVN | +0.260 | Energy | sector=-0.55; gen=+0.09; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.260 | Energy | sector=-0.55; gen=+0.09; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.260 | Energy | sector=-0.55; gen=+0.09; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| FANG | +0.260 | Energy | sector=-0.55; gen=+0.09; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.260 | Energy | sector=-0.55; gen=+0.09; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| APA | +0.260 | Energy | sector=-0.55; gen=+0.09; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| XOM | +0.201 | Energy | sector=-0.55; gen=+0.09; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| CVX | +0.201 | Energy | sector=-0.55; gen=+0.09; news=+0.69; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 4.2, 'bucket': 'oil_integrated'} |
| DFTX | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| VYGR | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| VVOS | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| AIRS | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| GYRE | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| TARA | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| AVBP | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| TEM | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| TELA | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| AVAH | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| LFMD | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| LCTX | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| STXS | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |

## 1d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AEM | -0.286 | Basic Materials | sector=-0.60; gen=+0.09; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'gold'} |
| NEM | -0.286 | Basic Materials | sector=-0.60; gen=+0.09; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'gold'} |
| UAL | -0.203 | Industrials | sector=+0.28; gen=+0.30; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| LUV | -0.203 | Industrials | sector=+0.28; gen=+0.30; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| ALK | -0.173 | Industrials | sector=+0.28; gen=+0.60; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| AAL | -0.173 | Industrials | sector=+0.28; gen=+0.60; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| DAL | -0.173 | Industrials | sector=+0.28; gen=+0.60; news=-0.69; ev={'event': 'hormuz_energy_risk', 'side': 'sell', 'weight': 4.2, 'bucket': 'airlines'} |
| BXP | -0.129 | Real Estate | sector=+0.60; gen=+0.30; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| CRM | -0.122 | Technology | sector=+0.35; gen=+0.30; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| NOW | -0.122 | Technology | sector=+0.35; gen=+0.30; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| WDAY | -0.122 | Technology | sector=+0.35; gen=+0.30; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| TEAM | -0.122 | Technology | sector=+0.35; gen=+0.30; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| GOLD | -0.113 | Financial | sector=+0.55; gen=+0.09; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'gold'} |
| VNO | -0.099 | Real Estate | sector=+0.60; gen=+0.60; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| SLG | -0.099 | Real Estate | sector=+0.60; gen=+0.60; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| ADBE | -0.092 | Technology | sector=+0.35; gen=+0.60; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| SNOW | -0.092 | Technology | sector=+0.35; gen=+0.60; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'software_app'} |
| FMC | -0.081 | Basic Materials | sector=-0.60; gen=+0.09 |
| AMR | -0.081 | Basic Materials | sector=-0.60; gen=+0.09 |
| VALE | -0.081 | Basic Materials | sector=-0.60; gen=+0.09 |
| BIOX | -0.081 | Basic Materials | sector=-0.60; gen=+0.09 |
| SSRM | -0.081 | Basic Materials | sector=-0.60; gen=+0.09 |
| CTVA | -0.081 | Basic Materials | sector=-0.60; gen=+0.09 |
| SSL | -0.081 | Basic Materials | sector=-0.60; gen=+0.09 |
| TRX | -0.081 | Basic Materials | sector=-0.60; gen=+0.09 |

### 1d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| JPM | +0.317 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| BAC | +0.317 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| C | +0.317 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| WFC | +0.317 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| FANG | +0.260 | Energy | sector=-0.55; gen=+0.09; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| EOG | +0.260 | Energy | sector=-0.55; gen=+0.09; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| COP | +0.260 | Energy | sector=-0.55; gen=+0.09; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |
| OXY | +0.260 | Energy | sector=-0.55; gen=+0.09; news=+0.83; ev={'event': 'hormuz_energy_risk', 'side': 'buy', 'weight': 6.0, 'bucket': 'oil_ep'} |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ADPT | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| PLSE | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| NUVB | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| PTGX | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| OSCR | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| OGN | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| NRIX | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| WGS | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ZJYL | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| ACIU | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| ACHV | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| PASG | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| ACH | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| ZNTL | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| ACRV | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |
| UPC | +0.158 | Healthcare | sector=+0.65; gen=+0.60 |

## 3d — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| JPM | +0.293 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| C | +0.293 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| BAC | +0.293 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| WFC | +0.293 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| RMTI | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| RNTX | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| CLOV | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| INGN | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| INFU | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| ARCT | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| CLLS | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| CLDI | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| RPID | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| IBO | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| FDMT | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| XCUR | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| XGN | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| MTNB | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| VCYT | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| INO | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| INMD | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| MBRX | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| FEED | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| IBIO | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| COCH | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |

## 3d — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AEM | -0.257 | Basic Materials | sector=-0.60; gen=+0.09; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'gold'} |
| NEM | -0.257 | Basic Materials | sector=-0.60; gen=+0.09; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'gold'} |
| AAUC | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| SSRM | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| EMAT | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| HCC | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| CPAC | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| SSL | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| AMR | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| SLVM | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| OGC | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| OGG | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| BVN | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| CF | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| ITP | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| AMRZ | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| LOMA | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| JCTC | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| CMCL | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| MSB | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| ATLX | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| KNF | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| APD | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| VOXR | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |
| OR | -0.129 | Basic Materials | sector=-0.60; gen=+0.09 |

### 3d — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| JPM | +0.293 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| BAC | +0.293 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| C | +0.293 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| WFC | +0.293 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| ISRG | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| ILMN | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| DXCM | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| NTRA | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ADPT | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| SLS | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| PTGX | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| DFTX | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| WGS | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| NRIX | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| NEO | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| NEOG | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ACIU | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| ACHV | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| ACH | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| PTHS | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| PSNL | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| PYXS | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| PYPD | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |
| ADXN | +0.205 | Healthcare | sector=+0.65; gen=+0.60 |

## 1w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| BAC | +0.252 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| JPM | +0.252 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| C | +0.252 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| WFC | +0.252 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| GUTS | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| GYRE | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| NGNE | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| VCYT | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| CRBU | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| CRBP | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| ATAI | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| NEPH | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| RYTM | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| RVMD | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| NCNA | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| NBP | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| NAGE | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| MYO | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| AGL | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| NRSN | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| NRIX | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| FEED | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| NPCE | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| NEOG | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| VRCA | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |

## 1w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| NEM | -0.069 | Basic Materials | sector=-0.60; gen=+0.09; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'gold'} |
| AEM | -0.069 | Basic Materials | sector=-0.60; gen=+0.09; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'gold'} |
| IDT | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| SGA | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| FLNT | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| ACCS | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| RDI | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| GRPN | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| KVHI | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| KT | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| ATNI | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| FWONK | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| FWONA | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| MDIA | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| MCS | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| RDCM | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| MF | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| CRTO | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| SHEN | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| ATHM | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| QNST | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| TDS | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| VSME | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| LBRDK | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |
| GCL | -0.068 | Communication Services | sector=-0.30; gen=+0.09 |

### 1w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| JPM | +0.252 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| C | +0.252 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| BAC | +0.252 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| WFC | +0.252 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| GH | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| ISRG | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| ILMN | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| DXCM | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| GRAL | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| WRBY | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| NEO | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| NRIX | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| HNGE | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| RDNT | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| SHC | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| AUPH | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ZJYL | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| ACHV | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| ACH | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| ACIU | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| ACET | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| ABSI | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| ABEO | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |
| AARD | +0.230 | Healthcare | sector=+0.65; gen=+0.60 |

## 2w — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| BAC | +0.193 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| JPM | +0.193 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| C | +0.193 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| WFC | +0.193 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| FLGT | +0.193 | Healthcare | sector=+0.65; gen=+0.30 |
| GENB | +0.193 | Healthcare | sector=+0.65; gen=+0.24 |
| GELS | +0.193 | Healthcare | sector=+0.65; gen=+0.09 |
| GEHC | +0.193 | Healthcare | sector=+0.65; gen=+0.30 |
| TNYA | +0.193 | Healthcare | sector=+0.65; gen=+0.60 |
| GDTC | +0.193 | Healthcare | sector=+0.65; gen=+0.09 |
| GDRX | +0.193 | Healthcare | sector=+0.65; gen=+0.60 |
| ATHE | +0.193 | Healthcare | sector=+0.65; gen=+0.09 |
| ATYR | +0.193 | Healthcare | sector=+0.65; gen=+0.09 |
| ATTO | +0.193 | Healthcare | sector=+0.65; gen=+0.24 |
| ATRC | +0.193 | Healthcare | sector=+0.65; gen=+0.30 |
| GCTK | +0.193 | Healthcare | sector=+0.65; gen=+0.09 |
| GANX | +0.193 | Healthcare | sector=+0.65; gen=+0.09 |
| TOVX | +0.193 | Healthcare | sector=+0.65; gen=+0.30 |
| TNDM | +0.193 | Healthcare | sector=+0.65; gen=+0.60 |
| EXOZ | +0.193 | Healthcare | sector=+0.65; gen=+0.60 |
| TRVI | +0.193 | Healthcare | sector=+0.65; gen=+0.30 |
| FMS | +0.193 | Healthcare | sector=+0.65; gen=+0.09 |
| FOCL | +0.193 | Healthcare | sector=+0.65; gen=+0.09 |
| FRNM | +0.193 | Healthcare | sector=+0.65; gen=+0.09 |
| TRIB | +0.193 | Healthcare | sector=+0.65; gen=+0.30 |

## 2w — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| SLG | -0.044 | Real Estate | sector=+0.60; gen=+0.60; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| VNO | -0.044 | Real Estate | sector=+0.60; gen=+0.60; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| BXP | -0.044 | Real Estate | sector=+0.60; gen=+0.30; news=-0.62; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 3.65, 'bucket': 'reit_office'} |
| NEM | -0.036 | Basic Materials | sector=-0.60; gen=+0.09; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'gold'} |
| AEM | -0.036 | Basic Materials | sector=-0.60; gen=+0.09; news=-0.51; ev={'event': 'fed_rate_path', 'side': 'sell', 'weight': 2.83, 'bucket': 'gold'} |
| DOC | +0.000 | Real Estate | sector=+0.60; gen=+0.30 |
| HST | +0.000 | Real Estate | sector=+0.60; gen=+0.30 |
| HSLV | +0.000 | Basic Materials | sector=-0.60; gen=+0.09 |
| TECK | +0.000 | Basic Materials | sector=-0.60; gen=+0.30 |
| TEAD | +0.000 | Communication Services | sector=-0.30; gen=+0.60 |
| DRCT | +0.000 | Communication Services | sector=-0.30; gen=+0.60 |
| USNA | +0.000 | Consumer Defensive | sector=+0.28; gen=+0.09 |
| DMLP | +0.000 | Energy | sector=-0.55; gen=+0.09 |
| DMC | +0.000 | Consumer Defensive | sector=+0.28; gen=+0.09 |
| DLXY | +0.000 | Energy | sector=-0.55; gen=+0.60 |
| DLTR | +0.000 | Consumer Defensive | sector=+0.28; gen=+0.09 |
| HWKN | +0.000 | Basic Materials | sector=-0.60; gen=+0.09 |
| DNUT | +0.000 | Consumer Defensive | sector=+0.28; gen=+0.30 |
| DNN | +0.000 | Energy | sector=-0.55; gen=+0.30 |
| DOLE | +0.000 | Consumer Defensive | sector=+0.28; gen=+0.09 |
| DIBS | +0.000 | Communication Services | sector=-0.30; gen=+0.30 |
| UTL | +0.000 | Utilities | sector=+0.55; gen=+0.09 |
| UTI | +0.000 | Consumer Defensive | sector=+0.28; gen=+0.30 |
| DIT | +0.000 | Consumer Defensive | sector=+0.28; gen=+0.09 |
| DIS | +0.000 | Communication Services | sector=-0.30; gen=+0.60 |

### 2w — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| BAC | +0.193 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| JPM | +0.193 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| C | +0.193 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| WFC | +0.193 | Financial | sector=+0.55; gen=+0.30; news=+0.51; ev={'event': 'fed_rate_path', 'side': 'buy', 'weight': 2.83, 'bucket': 'banks_diversified'} |
| ZTS | +0.193 | Healthcare | sector=+0.65; gen=+0.09 |
| WST | +0.193 | Healthcare | sector=+0.65; gen=+0.30 |
| ALGN | +0.193 | Healthcare | sector=+0.65; gen=+0.60 |
| GKOS | +0.193 | Healthcare | sector=+0.65; gen=+0.09 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ZLAB | +0.193 | Healthcare | sector=+0.65; gen=+0.30 |
| ACAD | +0.193 | Healthcare | sector=+0.65; gen=+0.30 |
| RDNT | +0.193 | Healthcare | sector=+0.65; gen=+0.60 |
| RGC | +0.193 | Healthcare | sector=+0.65; gen=+0.60 |
| DFTX | +0.193 | Healthcare | sector=+0.65; gen=+0.60 |
| RGEN | +0.193 | Healthcare | sector=+0.65; gen=+0.30 |
| NEO | +0.193 | Healthcare | sector=+0.65; gen=+0.60 |
| NEOG | +0.193 | Healthcare | sector=+0.65; gen=+0.60 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| AAPG | +0.193 | Healthcare | sector=+0.65; gen=+0.09 |
| ZYME | +0.193 | Healthcare | sector=+0.65; gen=+0.30 |
| AARD | +0.193 | Healthcare | sector=+0.65; gen=+0.60 |
| ABEO | +0.193 | Healthcare | sector=+0.65; gen=+0.60 |
| ABOS | +0.193 | Healthcare | sector=+0.65; gen=+0.09 |
| ZJYL | +0.193 | Healthcare | sector=+0.65; gen=+0.60 |
| ZCMD | +0.193 | Healthcare | sector=+0.65; gen=+0.09 |
| MIRA | +0.193 | Healthcare | sector=+0.65; gen=+0.60 |

## 1m — BUY (top 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ABBV | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| SANA | +0.220 | Healthcare | sector=+0.65; gen=+0.60 |
| SBFM | +0.220 | Healthcare | sector=+0.65; gen=+0.60 |
| AARD | +0.220 | Healthcare | sector=+0.65; gen=+0.60 |
| ZTS | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| ZURA | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| SABS | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| RYTM | +0.220 | Healthcare | sector=+0.65; gen=+0.60 |
| RZLT | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| RXRX | +0.220 | Healthcare | sector=+0.65; gen=+0.30 |
| RXST | +0.220 | Healthcare | sector=+0.65; gen=+0.30 |
| RVMD | +0.220 | Healthcare | sector=+0.65; gen=+0.60 |
| RVP | +0.220 | Healthcare | sector=+0.65; gen=+0.30 |
| RVTY | +0.220 | Healthcare | sector=+0.65; gen=+0.30 |
| RPRX | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| RPID | +0.220 | Healthcare | sector=+0.65; gen=+0.60 |
| ROIV | +0.220 | Healthcare | sector=+0.65; gen=+0.30 |
| RNTX | +0.220 | Healthcare | sector=+0.65; gen=+0.60 |
| RNXT | +0.220 | Healthcare | sector=+0.65; gen=+0.30 |
| ABEO | +0.220 | Healthcare | sector=+0.65; gen=+0.60 |
| RMTI | +0.220 | Healthcare | sector=+0.65; gen=+0.60 |
| RNA | +0.220 | Healthcare | sector=+0.65; gen=+0.24 |
| RNAC | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| RNAZ | +0.220 | Healthcare | sector=+0.65; gen=+0.30 |
| RLYB | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |

## 1m — SELL / avoid (bottom 25)

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ACI | +0.000 | Consumer Defensive | sector=+0.28; gen=+0.09 |
| ZIP | +0.000 | Communication Services | sector=-0.30; gen=+0.60 |
| AACG | +0.000 | Consumer Defensive | sector=+0.28; gen=+0.30 |
| FLNT | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| FLO | +0.000 | Consumer Defensive | sector=+0.28; gen=+0.09 |
| FLOC | +0.000 | Energy | sector=-0.55; gen=+0.30 |
| FLNC | +0.000 | Utilities | sector=+0.55; gen=+0.60 |
| FIZZ | +0.000 | Consumer Defensive | sector=+0.28; gen=+0.09 |
| UDR | +0.000 | Real Estate | sector=+0.60; gen=+0.09 |
| UE | +0.000 | Real Estate | sector=+0.60; gen=+0.30 |
| UEC | +0.000 | Energy | sector=-0.55; gen=+0.30 |
| FIRY | +0.000 | Communication Services | sector=-0.30; gen=+0.60 |
| FMX | +0.000 | Consumer Defensive | sector=+0.28; gen=+0.09 |
| FLZH | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| UCL | +0.000 | Communication Services | sector=-0.30; gen=+0.60 |
| FLNG | +0.000 | Energy | sector=-0.55; gen=+0.09 |
| MGPI | +0.000 | Consumer Defensive | sector=+0.28; gen=+0.09 |
| MGEE | +0.000 | Utilities | sector=+0.55; gen=+0.09 |
| STAG | +0.000 | Real Estate | sector=+0.60; gen=+0.30 |
| STAK | +0.000 | Energy | sector=-0.55; gen=+0.09 |
| STEM | +0.000 | Utilities | sector=+0.55; gen=+0.60 |
| MF | +0.000 | Communication Services | sector=-0.30; gen=+0.09 |
| MFA | +0.000 | Real Estate | sector=+0.60; gen=+0.60 |
| ACU | +0.000 | Consumer Defensive | sector=+0.28; gen=+0.09 |
| ZDGE | +0.000 | Communication Services | sector=-0.30; gen=+0.30 |

### 1m — BUY by size bucket


**large+**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ABBV | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| ABVX | +0.220 | Healthcare | sector=+0.65; gen=+0.30 |
| ABT | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| GSK | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| HSIC | +0.220 | Healthcare | sector=+0.65; gen=+0.30 |
| APGE | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| AMGN | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| ALNY | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |

**mid**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| NKTR | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| XENE | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| NAMS | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| NEO | +0.220 | Healthcare | sector=+0.65; gen=+0.60 |
| WRBY | +0.220 | Healthcare | sector=+0.65; gen=+0.60 |
| IONS | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| IOVA | +0.220 | Healthcare | sector=+0.65; gen=+0.30 |
| IRON | +0.220 | Healthcare | sector=+0.65; gen=+0.30 |

**small/micro**

| Ticker | Score | Sector | Reasons |
|--------|-------|--------|---------|
| ACET | +0.220 | Healthcare | sector=+0.65; gen=+0.60 |
| ACB | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| ZYME | +0.220 | Healthcare | sector=+0.65; gen=+0.30 |
| ABVC | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| ABEO | +0.220 | Healthcare | sector=+0.65; gen=+0.60 |
| AARD | +0.220 | Healthcare | sector=+0.65; gen=+0.60 |
| AAPG | +0.220 | Healthcare | sector=+0.65; gen=+0.09 |
| NAUT | +0.220 | Healthcare | sector=+0.65; gen=+0.30 |

## Read

- **1d** news-heavy; **1m** structural join + sector.
- Longer horizons use the predictors' explicit 3d/1w/2w/1m calls when stored, else fall back to the 1d call.
- Predictor bias is scaled by its graded hit rate (learning gate) — weak topics move scores less.
- Backtest: `python -m src.stock_book_backtest` (or Stock Book Backtest action).

CSV: `data/stock_book/2026-08-13_stock_book.csv`
JSON: `data/stock_book/2026-08-13_stock_book.json`
