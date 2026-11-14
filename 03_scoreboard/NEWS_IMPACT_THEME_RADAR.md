# Theme-radar Elite news-impact

Leak-free Lane/router backtest on **every** theme-radar snapshot date. Headlines only (Ticker, News Title, Daily Digest, News Time). theme-radar is read-only. The repos are not merged.

## Path

deterministic router on every unique Elite title. Indirect roles are the family template (substitute / stays_out / arms_dealer / peer / sector basket), not a Lane tag. Lane quota is dead in this run (no provider keys), so clash/converge groups were not re-hopped. Watermark is deterministic::news_impact_v2::theme_radar_elite. Re-run with --lane when current-flash keys exist; 429 hops to the next allowlisted model on the same lane and does not fall through to pre-2025 flash.

## Clock

Entry is the next regular session after News Time. Published at/after 09:30 ET, including 09:30+30m (10:00), is not eligible for that cash session. 0-1d is that session's open→close. 2d/3d/4d/5d are the close N sessions after the entry session. 1-4w is 20 sessions and stays secondary.

Rubric hit rates count a call only when q5=impulse, direction in {up, down}, tradeable_expression=direct, tape exists, and (for 0-1d and 2-5d) the class natural window is not 1-4w/1-6m. factor_impulse and index proxies stay ungraded. FOMC reprints collapse to one (factor, session, sign) and do not enter these rates. Reaction-in-title rows are killed by the router. Broad 2-5d rates keep long-horizon classes in the denominator so the windows Cyrus asked for are visible.

## Funnel

| step | n |
| --- | ---: |
| Elite raw titles | 665108 |
| unique (ticker + title, earliest News Time) | 72564 |
| non-weather | 32816 |
| reaction-in-title (killed by the router) | 1279 |
| impulse + up/down + listed | 17355 |
| graded articles 0-1d | 1879 |
| graded articles 2d | 1844 |
| graded articles 3d | 1817 |
| graded articles 4d | 1799 |
| graded articles 5d | 1777 |
| graded articles 1-4w | 8168 |

## Hit rates

Rubric column skips long-horizon classes on 0-1d and 2-5d. Broad column keeps those classes so the 2-5d windows stay visible. Rates are calls (one listed ticker), not articles.

| horizon | rubric hits | rubric n | rubric rate | broad hits | broad n | broad rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0-1d | 1726 | 3414 | 1726/3414 = 50.6% | 9220 | 18745 | 9220/18745 = 49.2% |
| 2d | 1653 | 3333 | 1653/3333 = 49.6% | 9182 | 18366 | 9182/18366 = 50.0% |
| 3d | 1572 | 3282 | 1572/3282 = 47.9% | 9013 | 18122 | 9013/18122 = 49.7% |
| 4d | 1565 | 3259 | 1565/3259 = 48.0% | 8933 | 17929 | 8933/17929 = 49.8% |
| 5d | 1561 | 3223 | 1561/3223 = 48.4% | 8564 | 17410 | 8564/17410 = 49.2% |
| 1-4w | 6002 | 12738 | 6002/12738 = 47.1% | — | — | — |

## Convergence / clash ledger

One entity per session. n_bull / n_bear count impulse up/down stories after title-normalize. Regime and weather do not vote. clash = both sides on that entity that session. converge = two or more stories, one side. Hit rates grade the entity once: converge uses that side, clash uses the net side (net 0 is ungraded). Long-horizon classes stay out of the 0-1d..5d denominator.

Direct = the article names the ticker. Indirect = substitute / stays_out / arms_dealer / peer / sector basket from the family template. Theme groups are router factor/macro keys only. They are not a second trade. The Elite Sector column is not a key.

Router theme keys (not traded): **16**. Name groups: **12790**.

| bucket | groups |
| --- | ---: |
| singleton | 11594 |
| converge | 1015 (up 679, down 336) |
| clash | 181 |

Hit rates grade the entity once per session. Converge uses that side. Clash uses the net side (net 0 is ungraded). Long-horizon classes are out of these denominators.

| bucket | 0-1d | 2d | 3d | 4d | 5d |
| --- | --- | --- | --- | --- | --- |
| singleton | 852/1767 = 48.2% | 873/1737 = 50.3% | 842/1720 = 49.0% | 847/1706 = 49.6% | 844/1688 = 50.0% |
| converge | 103/193 = 53.4% | 83/183 = 45.4% | 86/176 = 48.9% | 81/174 = 46.6% | 82/169 = 48.5% |
| clash | 6/15 = 40.0% | 7/14 = 50.0% | 10/14 = 71.4% | 6/14 = 42.9% | 6/14 = 42.9% |

### Top converge names

| ticker | date | n_bull | n_bear | net | 0-1d | 5d |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| SECZ | 2026-09-18 | 6 | 0 | 6 | +16.52% | +71.24% |
| COIN | 2026-09-18 | 6 | 0 | 6 | +9.03% | +9.51% |
| CRCL | 2026-09-18 | 6 | 0 | 6 | +5.01% | +1.82% |
| MSFT | 2026-10-29 | 0 | 6 | -6 | -1.87% | -0.79% |
| SCHW | 2026-09-18 | 0 | 6 | -6 | +0.54% | -5.40% |
| SECZ | 2026-09-21 | 5 | 0 | 5 | +15.68% | +42.50% |
| CRCL | 2026-09-21 | 5 | 0 | 5 | -3.67% | -12.53% |
| COIN | 2026-09-28 | 5 | 0 | 5 | -2.15% | -3.97% |
| COIN | 2026-09-21 | 5 | 0 | 5 | -2.02% | -6.53% |
| SCHW | 2026-09-21 | 0 | 5 | -5 | +0.93% | -7.18% |
| SECZ | 2026-09-24 | 4 | 0 | 4 | +12.68% | -6.34% |
| CRCL | 2026-11-05 | 4 | 0 | 4 | -5.47% | -13.17% |
| CRCL | 2026-09-25 | 4 | 0 | 4 | -3.26% | -11.68% |
| COIN | 2026-09-25 | 4 | 0 | 4 | -2.03% | -8.11% |
| CRCL | 2026-09-24 | 4 | 0 | 4 | +1.74% | -9.29% |
| SECZ | 2026-09-25 | 4 | 0 | 4 | -1.54% | -19.86% |
| ICE | 2026-08-20 | 4 | 0 | 4 | +1.39% | +3.07% |
| COIN | 2026-09-24 | 4 | 0 | 4 | +1.24% | -3.80% |
| GOOGL | 2026-10-29 | 0 | 4 | -4 | -1.03% | -1.46% |
| SCHW | 2026-09-25 | 0 | 4 | -4 | -0.49% | -2.83% |
| SCHW | 2026-09-24 | 0 | 4 | -4 | -0.16% | -1.28% |
| SPY | 2026-08-17 | 0 | 4 | -4 | n/a | n/a |
| TLT | 2026-08-17 | 4 | 0 | 4 | n/a | n/a |
| GLD | 2026-08-17 | 4 | 0 | 4 | n/a | n/a |
| AEYE | 2026-08-14 | 3 | 0 | 3 | +29.08% | +16.32% |

## Rippers

Same direction as the call and |forward return| at the threshold. 5% rows are ≥5% and <10%. 10% rows are ≥10%.

### 0-1d

- ≥5% and <10%: **980**
- ≥10%: **531**

| threshold | ticker | date | ret | class | title |
| --- | --- | --- | ---: | --- | --- |
| ≥10% | NUWE | 2026-10-27 | +69.59% | gate | Nuwellis Announces Acceptance of Multicenter Pediatric Aquadex Study in Pediatric Nephrology |
| ≥10% | JZ | 2026-10-09 | +60.00% | capital_return | Jianzhi Education Announces Share Repurchase Program |
| ≥10% | SDA | 2026-11-09 | +50.68% | capital_return | SunCar Technology Group Announces Share Repurchase Program |
| ≥10% | SUNE | 2026-09-09 | +49.83% | capacity | Suniva Completes $835 Million Capital Raise to Build Second Major U.S. Solar Cell Manufacturing Facility and M |
| ≥10% | DKI | 2026-10-09 | -44.84% | dilution | DarkIris Prices $6 Million Public Offering at $1.44 Per Unit With Potential for Significant Share Dilution |
| ≥10% | OLB | 2026-10-07 | +44.19% | capital_return | The OLB Group Launches Share Buyback; Suspends ATM Program |
| ≥10% | SPAI | 2026-08-14 | +43.49% | print_vs_priced | Safe Pro Reports Record 1,336% Revenue Growth in Q2 2026 Driven by Multiple Government Contracts for AI-Powere |
| ≥10% | STLN | 2026-11-06 | -40.26% | print_vs_priced | Starling Oncology Misses On GAAP EPS, Beats On EBITDA, But A Capitation Guidance Cut Undercuts The Growth Stor |
| ≥10% | ZIP | 2026-11-10 | +40.06% | guidance | ZipRecruiter Flips To A Surprise Profit And Raises Again, But Slowing Engine Metrics And A Skeptical Market Te |
| ≥10% | FGI | 2026-08-13 | +38.37% | print_vs_priced | FGI Industries Ltd. Q2 2026 Earnings Call Summary |
| ≥10% | SUJA | 2026-08-05 | -36.64% | print_vs_priced | Suja Life Inc (SUJA) (Q2 2026) Earnings Call Highlights: Strong Profit Growth Amid Grocery Softness |
| ≥10% | SKIN | 2026-11-06 | +36.37% | print_vs_priced | SkinHealth Systems Reports Third Quarter 2026 Financial Results |
| ≥10% | SKIN | 2026-11-06 | +36.37% | guidance | SkinHealth Systems (SKIN) Beats On Cost Discipline And A Consumables Recovery, But Device Weakness And A Narro |
| ≥10% | IHRT | 2026-11-10 | -36.19% | print_vs_priced | iHeartMedia, Inc. Reports Results for 2026 Third Quarter |
| ≥10% | IHRT | 2026-11-10 | -36.19% | guidance | IHeartMedia Slashes 2026 Outlook As Radio Erosion Overwhelms Podcast Strength |
| ≥10% | BSEM | 2026-08-13 | +34.78% | guidance | BioStem Technologies Inc (BSEM) (Q2 2026) Earnings Call Highlights: Revenue Surges to $7. ... |
| ≥10% | SLSN | 2026-11-13 | +34.51% | print_vs_priced | Solesence Posts A Record Quarter With 39% Gross Margin, But Trims Its Full-Year EBITDA Goal And Signals Slower |
| ≥10% | QMCO | 2026-08-11 | +33.61% | print_vs_priced | Quantum Corp (QMCO) (Q1 2027) Earnings Call Highlights: Debt-Free Turnaround with Record ... |
| ≥10% | QMCO | 2026-08-11 | +33.61% | print_vs_priced | Quantum Corporation Q1 2027 Earnings Call Summary |
| ≥10% | MDXH | 2026-08-14 | +32.89% | print_vs_priced | MDxHealth SA (MDXH) (Q2 2026) Earnings Call Highlights: Record Sequential Growth and Strategic ... |
| ≥10% | MDXH | 2026-08-14 | +32.89% | print_vs_priced | MDxHealth S.A. Q2 2026 Earnings Call Summary |
| ≥10% | ALGS | 2026-08-06 | +31.82% | print_vs_priced | Aligos Therapeutics Reports Recent Business Progress and Second Quarter 2026 Financial Results |
| ≥10% | BDSX | 2026-11-06 | -31.32% | print_vs_priced | Biodesix Announces Third Quarter 2026 Results and Highlights |
| ≥10% | BDSX | 2026-11-06 | -31.32% | print_vs_priced | Biodesix Breaks Its Beat-And-Raise Streak As Growth Slows To 10% And Guidance Falls Below The Original Outlook |
| ≥10% | SNT | 2026-11-09 | +31.02% | print_vs_priced | Senstar (SNT) Delivers Its Promised Inflection With 37.5% Revenue Growth, But Preliminary Blickfeld Accounting |
| … | | | | | 1486 more in the JSON |

### 2d

- ≥5% and <10%: **1732**
- ≥10%: **1080**

| threshold | ticker | date | ret | class | title |
| --- | --- | --- | ---: | --- | --- |
| ≥10% | PMI | 2026-08-24 | +116.34% | print_vs_priced | Picard Medical Shares Soar After Quarterly Revenue Beats Forecasts |
| ≥10% | MRNA | 2026-08-18 | +111.48% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +111.48% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +111.48% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +111.48% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +111.48% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +111.48% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | DKI | 2026-10-09 | -86.13% | dilution | DarkIris Prices $6 Million Public Offering at $1.44 Per Unit With Potential for Significant Share Dilution |
| ≥10% | OLB | 2026-10-07 | +84.43% | capital_return | The OLB Group Launches Share Buyback; Suspends ATM Program |
| ≥10% | ALGS | 2026-08-06 | +83.01% | print_vs_priced | Aligos Therapeutics Reports Recent Business Progress and Second Quarter 2026 Financial Results |
| ≥10% | NUWE | 2026-10-27 | +81.19% | gate | Nuwellis Announces Acceptance of Multicenter Pediatric Aquadex Study in Pediatric Nephrology |
| ≥10% | AEYE | 2026-10-27 | +74.32% | guidance | AudioEye Beats As AI Efficiencies Drive Margins And First GAAP Profit, But Revenue Growth Slows To 7% |
| ≥10% | JZ | 2026-10-09 | +73.75% | capital_return | Jianzhi Education Announces Share Repurchase Program |
| ≥10% | QMCO | 2026-08-11 | +69.97% | print_vs_priced | Quantum Corp (QMCO) (Q1 2027) Earnings Call Highlights: Debt-Free Turnaround with Record ... |
| ≥10% | QMCO | 2026-08-11 | +69.97% | print_vs_priced | Quantum Corporation Q1 2027 Earnings Call Summary |
| ≥10% | ULH | 2026-10-30 | +65.98% | print_vs_priced | Universal Truckload: Q3 Earnings Snapshot |
| ≥10% | PARA | 2026-11-04 | -64.06% | dilution | Parabolic Technologies, Inc. Announces Pricing of $13.5 Million Underwritten Public Offering |
| ≥10% | YQ | 2026-09-03 | +61.97% | capital_return | 17 Education & Technology Group Inc. Announces New Share Repurchase Program of Up to US$10 Million |
| ≥10% | ABCL | 2026-08-06 | +61.31% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +61.31% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +61.31% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | PMI | 2026-08-20 | +60.31% | print_vs_priced | Picard Medical Reports Second Quarter 2026 Financial Results |
| ≥10% | BWMN | 2026-08-07 | +55.03% | gate | Earnings To Watch: Bowman Consulting Group Ltd (BWMN) Q2 2026 -- GF Value Sees 64% Upside |
| ≥10% | APH | 2026-08-19 | -51.54% | print_vs_priced | Amphenol Corp. Cl A stock underperforms Tuesday when compared to competitors |
| ≥10% | LUNG | 2026-07-30 | +51.43% | print_vs_priced | Pulmonx Corporation Q2 2026 Earnings Call Summary |
| … | | | | | 2787 more in the JSON |

### 3d

- ≥5% and <10%: **1887**
- ≥10%: **1253**

| threshold | ticker | date | ret | class | title |
| --- | --- | --- | ---: | --- | --- |
| ≥10% | CYCU | 2026-08-17 | +667.60% | capital_return | Cycurion Board Authorizes $500,000 Share Repurchase Program, Citing Meaningful Undervaluation |
| ≥10% | PMI | 2026-08-20 | +198.44% | print_vs_priced | Picard Medical Reports Second Quarter 2026 Financial Results |
| ≥10% | OLB | 2026-10-07 | +143.74% | capital_return | The OLB Group Launches Share Buyback; Suspends ATM Program |
| ≥10% | MRNA | 2026-08-18 | +130.22% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +130.22% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +130.22% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +130.22% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +130.22% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +130.22% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | PMI | 2026-08-24 | +100.66% | print_vs_priced | Picard Medical Shares Soar After Quarterly Revenue Beats Forecasts |
| ≥10% | YQ | 2026-09-03 | +95.30% | capital_return | 17 Education & Technology Group Inc. Announces New Share Repurchase Program of Up to US$10 Million |
| ≥10% | DKI | 2026-10-09 | -89.03% | dilution | DarkIris Prices $6 Million Public Offering at $1.44 Per Unit With Potential for Significant Share Dilution |
| ≥10% | JZ | 2026-10-09 | +85.00% | capital_return | Jianzhi Education Announces Share Repurchase Program |
| ≥10% | QMCO | 2026-08-11 | +79.82% | print_vs_priced | Quantum Corp (QMCO) (Q1 2027) Earnings Call Highlights: Debt-Free Turnaround with Record ... |
| ≥10% | QMCO | 2026-08-11 | +79.82% | print_vs_priced | Quantum Corporation Q1 2027 Earnings Call Summary |
| ≥10% | NUWE | 2026-10-27 | +78.06% | gate | Nuwellis Announces Acceptance of Multicenter Pediatric Aquadex Study in Pediatric Nephrology |
| ≥10% | ALGS | 2026-08-06 | +74.16% | print_vs_priced | Aligos Therapeutics Reports Recent Business Progress and Second Quarter 2026 Financial Results |
| ≥10% | PARA | 2026-11-04 | -72.50% | dilution | Parabolic Technologies, Inc. Announces Pricing of $13.5 Million Underwritten Public Offering |
| ≥10% | QMCO | 2026-08-06 | +70.32% | print_vs_priced | IonQ CEO Niccolo de Masi: Quantum growth accelerates with record demand |
| ≥10% | ULH | 2026-10-30 | +69.71% | print_vs_priced | Universal Truckload: Q3 Earnings Snapshot |
| ≥10% | ABCL | 2026-08-06 | +68.57% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +68.57% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +68.57% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | GCTK | 2026-09-02 | +67.91% | capacity | Lokahi Therapeutics Expands ai² Futures Lab Asset Origination Network Through Collaboration with San Diego Sta |
| ≥10% | AEYE | 2026-10-27 | +60.86% | guidance | AudioEye Beats As AI Efficiencies Drive Margins And First GAAP Profit, But Revenue Growth Slows To 7% |
| … | | | | | 3115 more in the JSON |

### 4d

- ≥5% and <10%: **1995**
- ≥10%: **1456**

| threshold | ticker | date | ret | class | title |
| --- | --- | --- | ---: | --- | --- |
| ≥10% | BYND | 2026-08-06 | +1838.33% | print_vs_priced | BYND Stock Heads For Second Weekly Gains: Beyond Meat Sees Early Turnaround Signs As CEO Maps Three-Part Growt |
| ≥10% | BYND | 2026-08-06 | +1838.33% | print_vs_priced | Beyond Meat, Inc. Q2 2026 Earnings Call Summary |
| ≥10% | CYCU | 2026-08-17 | +630.73% | capital_return | Cycurion Board Authorizes $500,000 Share Repurchase Program, Citing Meaningful Undervaluation |
| ≥10% | CYCU | 2026-08-14 | +510.67% | print_vs_priced | Cycurion Beats Consensus on Revenue and EPS; Gross Margin Improves Nearly 5x as Company Positions for Stronger |
| ≥10% | PMI | 2026-08-20 | +206.25% | print_vs_priced | Picard Medical Reports Second Quarter 2026 Financial Results |
| ≥10% | MRNA | 2026-08-18 | +120.32% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +120.32% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +120.32% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +120.32% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +120.32% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +120.32% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | PMI | 2026-08-24 | +94.26% | print_vs_priced | Picard Medical Shares Soar After Quarterly Revenue Beats Forecasts |
| ≥10% | QMCO | 2026-08-06 | +92.63% | print_vs_priced | IonQ CEO Niccolo de Masi: Quantum growth accelerates with record demand |
| ≥10% | OLB | 2026-10-07 | +90.05% | capital_return | The OLB Group Launches Share Buyback; Suspends ATM Program |
| ≥10% | DKI | 2026-10-09 | -89.84% | dilution | DarkIris Prices $6 Million Public Offering at $1.44 Per Unit With Potential for Significant Share Dilution |
| ≥10% | JZ | 2026-10-09 | +83.75% | capital_return | Jianzhi Education Announces Share Repurchase Program |
| ≥10% | YQ | 2026-09-03 | +81.84% | capital_return | 17 Education & Technology Group Inc. Announces New Share Repurchase Program of Up to US$10 Million |
| ≥10% | GCTK | 2026-09-02 | +79.94% | capacity | Lokahi Therapeutics Expands ai² Futures Lab Asset Origination Network Through Collaboration with San Diego Sta |
| ≥10% | ABCL | 2026-08-06 | +78.84% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +78.84% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +78.84% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | SECZ | 2026-09-18 | +77.36% | market_structure | Robinhood Rises as the SEC Clears Tokenized Stocks |
| ≥10% | SECZ | 2026-09-18 | +77.36% | market_structure | Securitize Surges After SEC Clears Path for Tokenized Stock Trading |
| ≥10% | SECZ | 2026-09-18 | +77.36% | market_structure | Polymarket Hires Coinbase's Failed Social-Coin Architect |
| ≥10% | SECZ | 2026-09-18 | +77.36% | market_structure | SEC Sends Strong Signal to Robinhood, Coinbase Investors |
| … | | | | | 3426 more in the JSON |

### 5d

- ≥5% and <10%: **2059**
- ≥10%: **1509**

| threshold | ticker | date | ret | class | title |
| --- | --- | --- | ---: | --- | --- |
| ≥10% | BYND | 2026-08-06 | +2023.33% | print_vs_priced | BYND Stock Heads For Second Weekly Gains: Beyond Meat Sees Early Turnaround Signs As CEO Maps Three-Part Growt |
| ≥10% | BYND | 2026-08-06 | +2023.33% | print_vs_priced | Beyond Meat, Inc. Q2 2026 Earnings Call Summary |
| ≥10% | GAME | 2026-08-13 | +796.41% | print_vs_priced | GameSquare Appoints Tom Wild as Vice President of Creative Strategy for EMEA |
| ≥10% | CYCU | 2026-08-17 | +616.20% | capital_return | Cycurion Board Authorizes $500,000 Share Repurchase Program, Citing Meaningful Undervaluation |
| ≥10% | CYCU | 2026-08-14 | +481.33% | print_vs_priced | Cycurion Beats Consensus on Revenue and EPS; Gross Margin Improves Nearly 5x as Company Positions for Stronger |
| ≥10% | PMI | 2026-08-20 | +184.06% | print_vs_priced | Picard Medical Reports Second Quarter 2026 Financial Results |
| ≥10% | MRNA | 2026-08-18 | +151.95% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +151.95% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +151.95% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +151.95% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +151.95% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +151.95% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | QMCO | 2026-08-06 | +116.68% | print_vs_priced | IonQ CEO Niccolo de Masi: Quantum growth accelerates with record demand |
| ≥10% | DKI | 2026-10-09 | -91.45% | dilution | DarkIris Prices $6 Million Public Offering at $1.44 Per Unit With Potential for Significant Share Dilution |
| ≥10% | ABCL | 2026-08-06 | +89.46% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +89.46% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +89.46% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | PMI | 2026-08-24 | +86.75% | print_vs_priced | Picard Medical Shares Soar After Quarterly Revenue Beats Forecasts |
| ≥10% | NUWE | 2026-10-27 | +85.27% | gate | Nuwellis Announces Acceptance of Multicenter Pediatric Aquadex Study in Pediatric Nephrology |
| ≥10% | GCTK | 2026-09-02 | +85.10% | capacity | Lokahi Therapeutics Expands ai² Futures Lab Asset Origination Network Through Collaboration with San Diego Sta |
| ≥10% | ZJYL | 2026-11-03 | +80.69% | peer_spill | Jin Medical Supports the 12th Jiangsu Provincial Games for Persons with Disabilities with Professional Wheelch |
| ≥10% | PARA | 2026-11-04 | -77.50% | dilution | Parabolic Technologies, Inc. Announces Pricing of $13.5 Million Underwritten Public Offering |
| ≥10% | BETR | 2026-10-12 | +72.89% | capital_return | Grant Cardone Says He Never Tells Fannie Mae About The Bitcoin In His Real Estate Deals: Its Just Too Much |
| ≥10% | SECZ | 2026-09-18 | +71.24% | market_structure | Robinhood Rises as the SEC Clears Tokenized Stocks |
| ≥10% | SECZ | 2026-09-18 | +71.24% | market_structure | Securitize Surges After SEC Clears Path for Tokenized Stock Trading |
| … | | | | | 3543 more in the JSON |

## AMRX

| field | value |
| --- | --- |
| ticker | AMRX |
| title | Amneal Announces FDA Approval and Launch of Lanreotide Injection |
| News Time | 2026-09-18 16:01:00 |
| entry_date | 2026-09-21 |
| entry_clock | published |
| class | gate / q5=impulse / sign=open |
| direction | up |
| natural horizon | 1-6m |
| 0-1d | +6.33% agree=True skip_01d=True |
| 2d | +1.95% agree=True |
| 3d | +4.46% agree=True |
| 4d | +4.27% agree=True |
| 5d | +8.12% agree=True |
| 1-4w | +2.11% agree=True |
| note | window open (1-6m not elapsed; through 2026-11-12) |

Friday 2026-09-18 16:01 is after the cash close. Entry is Monday 2026-09-21 open→close for 0-1d, never Friday cash. Published at/after 09:30 ET (09:30+30m included) waits for the next RTH.

### AMRX week

No second story voted AMRX on the lanreotide session 2026-09-21. The print is a singleton on that entity that day.

Window 2026-09-14..2026-09-21. Lanreotide session `2026-09-21`. Same-session label `singleton` n_bull=1 n_bear=0 direct=1 indirect=0 0-1d +6.33%.

| session | label | n_bull | n_bear | stories | 0-1d | titles |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| 2026-09-21 | singleton | 1 | 0 | 1 | +6.33% | Amneal Announces FDA Approval and Launch of Lanreotide Injection |


## Harvest source mix

Elite is in the book. `unused_readonly` applies only when no snapshot file is on disk.

| source | raw titles | unique |
| --- | ---: | ---: |
| theme_radar_elite | 665108 | 72564 |

theme_radar_elite unique n = **72564**.

## Hopper watermark (lane::model::source)

- `deterministic::news_impact_v2::theme_radar_elite`: 72564

## FOMC / macro collapse

- stories=19 reprints_collapsed=18
- headline basket 0-1d: n/a

Reprints of the same factor/session/sign are one story. They are not in the hit rates above.
