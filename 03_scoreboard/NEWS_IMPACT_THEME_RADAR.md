# Theme-radar Elite news-impact

Leak-free Lane/router backtest on **every** theme-radar snapshot date. Headlines only (Ticker, News Title, Daily Digest, News Time). theme-radar is read-only. The repos are not merged.

## Path

deterministic router on every unique Elite title. Indirect roles are the family template (substitute / stays_out / arms_dealer / peer / sector basket), not a Lane tag. Lane quota is dead in this run (no provider keys), so clash/converge groups were not re-hopped. Watermark is deterministic::news_impact_v2::theme_radar_elite. Re-run with --lane when current-flash keys exist; 429 leaves that provider and does not fall through to pre-2025 flash.

## Clock

Entry is the next regular session after News Time. Published at/after 09:30 ET, including 09:30+30m (10:00), is not eligible for that cash session. 0-1d is that session's open→close. 2d/3d/4d/5d are the close N sessions after the entry session. 1-4w is 20 sessions and stays secondary.

Rubric hit rates count a call only when q5=impulse, direction in {up, down}, tradeable_expression=direct, tape exists, and (for 0-1d and 2-5d) the class natural window is not 1-4w/1-6m. factor_impulse and index proxies stay ungraded. FOMC reprints collapse to one (factor, session, sign) and do not enter these rates. Reaction-in-title rows are killed by the router. Broad 2-5d rates keep long-horizon classes in the denominator so the windows Cyrus asked for are visible.

## Funnel

| step | n |
| --- | ---: |
| Elite raw titles | 293563 |
| unique (ticker + title, earliest News Time) | 35255 |
| non-weather | 16774 |
| reaction-in-title (killed by the router) | 564 |
| impulse + up/down + listed | 9496 |
| graded articles 0-1d | 1048 |
| graded articles 2d | 994 |
| graded articles 3d | 955 |
| graded articles 4d | 925 |
| graded articles 5d | 902 |
| graded articles 1-4w | 4291 |

## Hit rates

Rubric column skips long-horizon classes on 0-1d and 2-5d. Broad column keeps those classes so the 2-5d windows stay visible. Rates are calls (one listed ticker), not articles.

| horizon | rubric hits | rubric n | rubric rate | broad hits | broad n | broad rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0-1d | 839 | 1829 | 839/1829 = 45.9% | 4939 | 10190 | 4939/10190 = 48.5% |
| 2d | 835 | 1687 | 835/1687 = 49.5% | 4838 | 9803 | 4838/9803 = 49.4% |
| 3d | 746 | 1592 | 746/1592 = 46.9% | 4699 | 9565 | 4699/9565 = 49.1% |
| 4d | 746 | 1545 | 746/1545 = 48.3% | 4578 | 9382 | 4578/9382 = 48.8% |
| 5d | 738 | 1506 | 738/1506 = 49.0% | 4463 | 9218 | 4463/9218 = 48.4% |
| 1-4w | 2774 | 5899 | 2774/5899 = 47.0% | — | — | — |

## Convergence / clash ledger

One entity per session. n_bull / n_bear count impulse up/down stories after title-normalize. Regime and weather do not vote. clash = both sides on that entity that session. converge = two or more stories, one side. Hit rates grade the entity once: converge uses that side, clash uses the net side (net 0 is ungraded). Long-horizon classes stay out of the 0-1d..5d denominator.

Direct = the article names the ticker. Indirect = substitute / stays_out / arms_dealer / peer / sector basket from the family template. Theme groups are router factor/macro keys only. They are not a second trade. The Elite Sector column is not a key.

Router theme keys (not traded): **11**. Name groups: **6692**.

| bucket | groups |
| --- | ---: |
| singleton | 6128 |
| converge | 497 (up 344, down 153) |
| clash | 67 |

Hit rates grade the entity once per session. Converge uses that side. Clash uses the net side (net 0 is ungraded). Long-horizon classes are out of these denominators.

| bucket | 0-1d | 2d | 3d | 4d | 5d |
| --- | --- | --- | --- | --- | --- |
| singleton | 432/952 = 45.4% | 454/916 = 49.6% | 429/877 = 48.9% | 420/852 = 49.3% | 418/826 = 50.6% |
| converge | 50/101 = 49.5% | 40/83 = 48.2% | 40/80 = 50.0% | 37/77 = 48.1% | 38/76 = 50.0% |
| clash | 4/10 = 40.0% | 4/10 = 40.0% | 4/9 = 44.4% | 2/9 = 22.2% | 2/8 = 25.0% |

### Top converge names

| ticker | date | n_bull | n_bear | net | 0-1d | 5d |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| SECZ | 2026-09-18 | 6 | 0 | 6 | +16.52% | n/a |
| COIN | 2026-09-18 | 6 | 0 | 6 | +9.03% | n/a |
| CRCL | 2026-09-18 | 6 | 0 | 6 | +5.01% | n/a |
| SCHW | 2026-09-18 | 0 | 6 | -6 | +0.54% | n/a |
| SECZ | 2026-09-21 | 5 | 0 | 5 | +15.68% | n/a |
| CRCL | 2026-09-21 | 5 | 0 | 5 | -3.67% | n/a |
| COIN | 2026-09-21 | 5 | 0 | 5 | -2.02% | n/a |
| SCHW | 2026-09-21 | 0 | 5 | -5 | +0.93% | n/a |
| ICE | 2026-08-20 | 4 | 0 | 4 | +1.39% | +3.07% |
| SPY | 2026-08-17 | 0 | 4 | -4 | n/a | n/a |
| TLT | 2026-08-17 | 4 | 0 | 4 | n/a | n/a |
| GLD | 2026-08-17 | 4 | 0 | 4 | n/a | n/a |
| AEYE | 2026-08-14 | 3 | 0 | 3 | +29.08% | +16.32% |
| SKIL | 2026-09-10 | 0 | 3 | -3 | -22.98% | +16.36% |
| TLYS | 2026-09-03 | 3 | 0 | 3 | -20.72% | -17.12% |
| KMTS | 2026-09-15 | 3 | 0 | 3 | +14.98% | n/a |
| ALTG | 2026-08-07 | 0 | 3 | -3 | +12.11% | +8.37% |
| LIDR | 2026-08-07 | 3 | 0 | 3 | +10.92% | +10.08% |
| NEPH | 2026-08-07 | 3 | 0 | 3 | +10.79% | +21.84% |
| CELC | 2026-08-14 | 3 | 0 | 3 | +10.79% | +12.63% |
| NMAX | 2026-08-14 | 3 | 0 | 3 | +9.85% | +8.64% |
| LAKE | 2026-09-10 | 0 | 3 | -3 | -8.45% | -0.38% |
| REZI | 2026-08-13 | 3 | 0 | 3 | -8.04% | -9.98% |
| SPT | 2026-08-07 | 3 | 0 | 3 | +7.60% | +2.91% |
| QNST | 2026-08-07 | 3 | 0 | 3 | +7.06% | +4.11% |

## Rippers

Same direction as the call and |forward return| at the threshold. 5% rows are ≥5% and <10%. 10% rows are ≥10%.

### 0-1d

- ≥5% and <10%: **491**
- ≥10%: **269**

| threshold | ticker | date | ret | class | title |
| --- | --- | --- | ---: | --- | --- |
| ≥10% | SPAI | 2026-08-14 | +43.49% | print_vs_priced | Safe Pro Reports Record 1,336% Revenue Growth in Q2 2026 Driven by Multiple Government Contracts for AI-Powere |
| ≥10% | FGI | 2026-08-13 | +38.37% | print_vs_priced | FGI Industries Ltd. Q2 2026 Earnings Call Summary |
| ≥10% | SUJA | 2026-08-05 | -36.64% | print_vs_priced | Suja Life Inc (SUJA) (Q2 2026) Earnings Call Highlights: Strong Profit Growth Amid Grocery Softness |
| ≥10% | BSEM | 2026-08-13 | +34.78% | guidance | BioStem Technologies Inc (BSEM) (Q2 2026) Earnings Call Highlights: Revenue Surges to $7. ... |
| ≥10% | QMCO | 2026-08-11 | +33.61% | print_vs_priced | Quantum Corp (QMCO) (Q1 2027) Earnings Call Highlights: Debt-Free Turnaround with Record ... |
| ≥10% | QMCO | 2026-08-11 | +33.61% | print_vs_priced | Quantum Corporation Q1 2027 Earnings Call Summary |
| ≥10% | MDXH | 2026-08-14 | +32.89% | print_vs_priced | MDxHealth SA (MDXH) (Q2 2026) Earnings Call Highlights: Record Sequential Growth and Strategic ... |
| ≥10% | MDXH | 2026-08-14 | +32.89% | print_vs_priced | MDxHealth S.A. Q2 2026 Earnings Call Summary |
| ≥10% | ALGS | 2026-08-06 | +31.82% | print_vs_priced | Aligos Therapeutics Reports Recent Business Progress and Second Quarter 2026 Financial Results |
| ≥10% | OABI | 2026-08-07 | +29.96% | guidance | VERAXA Biotech (VRXA) Advances VXA-222 Cancer Program While Expanding Patent Portfolio for Next-Generation Ant |
| ≥10% | OABI | 2026-08-07 | +29.96% | guidance | VERAXA Biotech (VRXA) Advances VXA-222 Cancer Program While Expanding Patent Portfolio for Next-Generation Ant |
| ≥10% | BODI | 2026-08-11 | -29.56% | guidance | The Beachbody Company, Inc. Q2 2026 Earnings Call Summary |
| ≥10% | DOCS | 2026-08-07 | -29.50% | print_vs_priced | Doximity: Fiscal Q1 Earnings Snapshot |
| ≥10% | RCEL | 2026-08-07 | +29.28% | guidance | AVITA Medical Reports Second Quarter Results, Raises 2026 Revenue Guidance, and Expects Fourth Quarter Cash Fl |
| ≥10% | RCEL | 2026-08-07 | +29.28% | print_vs_priced | AVITA Medical Announces Participation at Canaccord Genuity Growth Conference |
| ≥10% | ULH | 2026-08-03 | +29.15% | print_vs_priced | Universal Truckload: Q2 Earnings Snapshot |
| ≥10% | AEYE | 2026-08-14 | +29.08% | print_vs_priced | AudioEye: Q2 Earnings Snapshot |
| ≥10% | AEYE | 2026-08-14 | +29.08% | guidance | AudioEye Inc (AEYE) (Q2 2026) Earnings Call Highlights: Record Adjusted EBITDA and Raised ... |
| ≥10% | AEYE | 2026-08-14 | +29.08% | guidance | AudioEye, Inc. Q2 2026 Earnings Call Summary |
| ≥10% | OPRX | 2026-08-13 | +28.74% | print_vs_priced | OptimizeRx Corp (OPRX) (Q2 2026) Earnings Call Highlights: Navigating Revenue Headwinds with ... |
| ≥10% | OPRX | 2026-08-13 | +28.74% | print_vs_priced | OptimizeRx Corporation Q2 2026 Earnings Call Summary |
| ≥10% | MYO | 2026-08-06 | +28.43% | guidance | Myomo Inc (MYO) (Q2 2026) Earnings Call Highlights: Record Orders and Margin Expansion Drive ... |
| ≥10% | ZONE | 2026-08-11 | -27.85% | dilution | CleanCore Solutions, Inc. (NYSE AMERICAN: ZONE) Announces Proposed Public Offering |
| ≥10% | ZONE | 2026-08-11 | -27.85% | dilution | CleanCore Solutions, Inc. (NYSE American: ZONE) Announces Pricing of $100 Million Public Offering |
| ≥10% | HURN | 2026-07-29 | +27.06% | guidance | Huron Consulting Group Inc. Q2 2026 Earnings Call Summary |
| … | | | | | 735 more in the JSON |

### 2d

- ≥5% and <10%: **869**
- ≥10%: **493**

| threshold | ticker | date | ret | class | title |
| --- | --- | --- | ---: | --- | --- |
| ≥10% | PMI | 2026-08-24 | +116.34% | print_vs_priced | Picard Medical Shares Soar After Quarterly Revenue Beats Forecasts |
| ≥10% | MRNA | 2026-08-18 | +111.48% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +111.48% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +111.48% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +111.48% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +111.48% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +111.48% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | ALGS | 2026-08-06 | +83.01% | print_vs_priced | Aligos Therapeutics Reports Recent Business Progress and Second Quarter 2026 Financial Results |
| ≥10% | LHSW | 2026-09-04 | -76.28% | dilution | Lianhe Sowell International Group Ltd. Announces Closing of an $11 Million Best-efforts Follow-on Public Offer |
| ≥10% | QMCO | 2026-08-11 | +69.97% | print_vs_priced | Quantum Corp (QMCO) (Q1 2027) Earnings Call Highlights: Debt-Free Turnaround with Record ... |
| ≥10% | QMCO | 2026-08-11 | +69.97% | print_vs_priced | Quantum Corporation Q1 2027 Earnings Call Summary |
| ≥10% | YQ | 2026-09-03 | +61.97% | capital_return | 17 Education & Technology Group Inc. Announces New Share Repurchase Program of Up to US$10 Million |
| ≥10% | ABCL | 2026-08-06 | +61.31% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +61.31% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +61.31% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | PMI | 2026-08-20 | +60.31% | print_vs_priced | Picard Medical Reports Second Quarter 2026 Financial Results |
| ≥10% | BWMN | 2026-08-07 | +55.03% | gate | Earnings To Watch: Bowman Consulting Group Ltd (BWMN) Q2 2026 -- GF Value Sees 64% Upside |
| ≥10% | APH | 2026-08-19 | -51.54% | print_vs_priced | Amphenol Corp. Cl A stock underperforms Tuesday when compared to competitors |
| ≥10% | LUNG | 2026-07-30 | +51.43% | print_vs_priced | Pulmonx Corporation Q2 2026 Earnings Call Summary |
| ≥10% | SDGR | 2026-09-15 | +45.52% | guidance | AI Drug Discovery Market Projected to Reach $13.8 Billion by 2033 |
| ≥10% | SDGR | 2026-09-15 | +45.52% | guidance | AI Drug Discovery Market Projected to Reach $13.8 Billion by 2033 |
| ≥10% | OABI | 2026-08-07 | +42.62% | guidance | VERAXA Biotech (VRXA) Advances VXA-222 Cancer Program While Expanding Patent Portfolio for Next-Generation Ant |
| ≥10% | OABI | 2026-08-07 | +42.62% | guidance | VERAXA Biotech (VRXA) Advances VXA-222 Cancer Program While Expanding Patent Portfolio for Next-Generation Ant |
| ≥10% | PEPG | 2026-08-06 | +42.38% | print_vs_priced | PepGen Reports Second Quarter 2026 Financial Results and Recent Corporate Highlights |
| ≥10% | ACDC | 2026-08-06 | +42.06% | print_vs_priced | ProFrac Holding Corp. Reports Second Quarter 2026 Results |
| … | | | | | 1337 more in the JSON |

### 3d

- ≥5% and <10%: **953**
- ≥10%: **557**

| threshold | ticker | date | ret | class | title |
| --- | --- | --- | ---: | --- | --- |
| ≥10% | CYCU | 2026-08-17 | +667.60% | capital_return | Cycurion Board Authorizes $500,000 Share Repurchase Program, Citing Meaningful Undervaluation |
| ≥10% | PMI | 2026-08-20 | +198.44% | print_vs_priced | Picard Medical Reports Second Quarter 2026 Financial Results |
| ≥10% | MRNA | 2026-08-18 | +130.22% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +130.22% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +130.22% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +130.22% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +130.22% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | MRNA | 2026-08-18 | +130.22% | gate | Behind the Ticker: How VFLO Beats Growth at Its Own Game |
| ≥10% | PMI | 2026-08-24 | +100.66% | print_vs_priced | Picard Medical Shares Soar After Quarterly Revenue Beats Forecasts |
| ≥10% | YQ | 2026-09-03 | +95.30% | capital_return | 17 Education & Technology Group Inc. Announces New Share Repurchase Program of Up to US$10 Million |
| ≥10% | LHSW | 2026-09-04 | -84.88% | dilution | Lianhe Sowell International Group Ltd. Announces Closing of an $11 Million Best-efforts Follow-on Public Offer |
| ≥10% | QMCO | 2026-08-11 | +79.82% | print_vs_priced | Quantum Corp (QMCO) (Q1 2027) Earnings Call Highlights: Debt-Free Turnaround with Record ... |
| ≥10% | QMCO | 2026-08-11 | +79.82% | print_vs_priced | Quantum Corporation Q1 2027 Earnings Call Summary |
| ≥10% | ALGS | 2026-08-06 | +74.16% | print_vs_priced | Aligos Therapeutics Reports Recent Business Progress and Second Quarter 2026 Financial Results |
| ≥10% | QMCO | 2026-08-06 | +70.32% | print_vs_priced | IonQ CEO Niccolo de Masi: Quantum growth accelerates with record demand |
| ≥10% | ABCL | 2026-08-06 | +68.57% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +68.57% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +68.57% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | BWMN | 2026-08-07 | +54.48% | gate | Earnings To Watch: Bowman Consulting Group Ltd (BWMN) Q2 2026 -- GF Value Sees 64% Upside |
| ≥10% | APH | 2026-08-19 | -51.99% | print_vs_priced | Amphenol Corp. Cl A stock underperforms Tuesday when compared to competitors |
| ≥10% | PRE | 2026-08-21 | +48.16% | guidance | Prenetics Global Ltd (PRE) (Q2 2026) Earnings Call Highlights: Record Revenue and Raised ... |
| ≥10% | MYO | 2026-08-06 | +48.04% | guidance | Myomo Inc (MYO) (Q2 2026) Earnings Call Highlights: Record Orders and Margin Expansion Drive ... |
| ≥10% | LUNG | 2026-07-30 | +47.14% | print_vs_priced | Pulmonx Corporation Q2 2026 Earnings Call Summary |
| ≥10% | ACDC | 2026-08-06 | +46.80% | print_vs_priced | ProFrac Holding Corp. Reports Second Quarter 2026 Results |
| ≥10% | OTLY | 2026-07-23 | +44.38% | guidance | Oatly ups revenue forecast as Europe outperforms on volumes |
| … | | | | | 1485 more in the JSON |

### 4d

- ≥5% and <10%: **1008**
- ≥10%: **687**

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
| ≥10% | LHSW | 2026-09-04 | -88.79% | dilution | Lianhe Sowell International Group Ltd. Announces Closing of an $11 Million Best-efforts Follow-on Public Offer |
| ≥10% | YQ | 2026-09-03 | +81.84% | capital_return | 17 Education & Technology Group Inc. Announces New Share Repurchase Program of Up to US$10 Million |
| ≥10% | ABCL | 2026-08-06 | +78.84% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +78.84% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +78.84% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | QMCO | 2026-08-11 | +71.63% | print_vs_priced | Quantum Corp (QMCO) (Q1 2027) Earnings Call Highlights: Debt-Free Turnaround with Record ... |
| ≥10% | QMCO | 2026-08-11 | +71.63% | print_vs_priced | Quantum Corporation Q1 2027 Earnings Call Summary |
| ≥10% | ALGS | 2026-08-06 | +67.46% | print_vs_priced | Aligos Therapeutics Reports Recent Business Progress and Second Quarter 2026 Financial Results |
| ≥10% | SECZ | 2026-09-15 | +66.67% | market_structure | Robinhood To Offer Share Redemptions And Voting Rights On Tokenized Stocks |
| ≥10% | AXTI | 2026-08-03 | +55.70% | print_vs_priced | AXTI Stock Skyrockets As Investors Cheer Blowout Guidance, Record Quarterly Revenue  Needham Spots A 'Real' AI |
| ≥10% | LHSW | 2026-09-02 | -54.67% | dilution | Lianhe Sowell International Group Ltd. Announces Pricing of an $11 Million Best-efforts Follow-on Public Offer |
| ≥10% | BWMN | 2026-08-07 | +54.41% | gate | Earnings To Watch: Bowman Consulting Group Ltd (BWMN) Q2 2026 -- GF Value Sees 64% Upside |
| … | | | | | 1670 more in the JSON |

### 5d

- ≥5% and <10%: **1008**
- ≥10%: **743**

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
| ≥10% | ABCL | 2026-08-06 | +89.46% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +89.46% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | ABCL | 2026-08-06 | +89.46% | guidance | AbCellera Biologics Inc (ABCL) (Q2 2026) Earnings Call Highlights: Strong Pipeline Progress and ... |
| ≥10% | PMI | 2026-08-24 | +86.75% | print_vs_priced | Picard Medical Shares Soar After Quarterly Revenue Beats Forecasts |
| ≥10% | LHSW | 2026-09-04 | -85.51% | dilution | Lianhe Sowell International Group Ltd. Announces Closing of an $11 Million Best-efforts Follow-on Public Offer |
| ≥10% | LHSW | 2026-09-02 | -71.11% | dilution | Lianhe Sowell International Group Ltd. Announces Pricing of an $11 Million Best-efforts Follow-on Public Offer |
| ≥10% | SECZ | 2026-09-14 | +68.33% | market_structure | Broadridge Expands Next-gen Digital Assets Capabilities to U.S. Wealth Management Firms |
| ≥10% | QMCO | 2026-08-11 | +61.98% | print_vs_priced | Quantum Corp (QMCO) (Q1 2027) Earnings Call Highlights: Debt-Free Turnaround with Record ... |
| ≥10% | QMCO | 2026-08-11 | +61.98% | print_vs_priced | Quantum Corporation Q1 2027 Earnings Call Summary |
| ≥10% | ALGS | 2026-08-06 | +61.96% | print_vs_priced | Aligos Therapeutics Reports Recent Business Progress and Second Quarter 2026 Financial Results |
| ≥10% | YQ | 2026-09-03 | +60.68% | capital_return | 17 Education & Technology Group Inc. Announces New Share Repurchase Program of Up to US$10 Million |
| ≥10% | BSEM | 2026-08-13 | +60.00% | guidance | BioStem Technologies Inc (BSEM) (Q2 2026) Earnings Call Highlights: Revenue Surges to $7. ... |
| … | | | | | 1726 more in the JSON |

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
| 2d | n/a agree=None |
| 3d | n/a agree=None |
| 4d | n/a agree=None |
| 5d | n/a agree=None |
| 1-4w | n/a agree=None |
| note | window open (1-6m not elapsed; through 2026-09-21) |

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
| theme_radar_elite | 293563 | 35255 |

theme_radar_elite unique n = **35255**.

## Hopper watermark (lane::model::source)

- `deterministic::news_impact_v2::theme_radar_elite`: 35255

## FOMC / macro collapse

- stories=12 reprints_collapsed=5
- headline basket 0-1d: n/a

Reprints of the same factor/session/sign are one story. They are not in the hit rates above.
