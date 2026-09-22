# Theme-radar Elite Lane backtest

**Tier A n = 2993 articles. Lane-ok 1488 / Lane-fail 1505 / deterministic leftover 1505 / missing 0.**

**0-1d paired Lane 419/909 = 46.1% vs deterministic 427/909 = 47.0% on the same calls (n=909).**

**2-5d pooled paired Lane 1488/3060 = 48.6% vs deterministic 1526/3060 = 49.9% (n=3060).**

Impulse + up/down + listed is about 9.5k unique Elite titles. Two current-flash hops on that set do not finish in one Actions runner, so Tier A is the #314 0-1d graded articles plus every converge/clash group plus AMRX lanreotide plus the 2026-09-18 SECZ/COIN/CRCL/SCHW stack. Raw 293k wraps are not rescored.

Watermark on every attempted row is `lane::model::inference_source`. A deterministic watermark is a leftover, not a Lane call. theme-radar is read-only. The repos are not merged.

## Env (redacted)

| name | status |
| --- | --- |
| ZHIPU_API_KEY | PRESENT |
| GLM_API_KEY | PRESENT |
| SILICONFLOW_API_KEY | PRESENT |
| OPENROUTER_API_KEY | PRESENT |
| DASHSCOPE_API_KEY | PRESENT |
| QWEN_API_KEY | PRESENT |
| DASHSCOPE_BASE_URL | PRESENT |
| TOKENHUB_API_KEY | PRESENT |
| TOKENHUB_BASE_URL | PRESENT |
| TENCENT_API_KEY | PRESENT |
| TENCENT_BASE_URL | PRESENT |
| HUNYUAN_API_KEY | PRESENT |
| MOONSHOT_API_KEY | MISSING |
| DEEPSEEK_API_KEY | PRESENT |
| GROQ_API_KEY | MISSING |
| GEMINI_API_KEY | PRESENT |
| HF_TOKEN | MISSING |
| SAMBANOVA_API_KEY | MISSING |
| MODELSCOPE_API_KEY | MISSING |
| MODELSCOPE_API_TOKEN | MISSING |
| MODELSCOPE_SDK_TOKEN | MISSING |
| OLLAMA_URL | MISSING |
| LANE_URL | MISSING |
| GITHUB_MODELS_TOKEN | MISSING |
| CLOUDFLARE_API_TOKEN | MISSING |
| CLOUDFLARE_ACCOUNT_ID | MISSING |

Loaded current-flash hoppers: **deepseek, gemini, openrouter, qwen, siliconflow, tokenhub, zhipu**.

## Tier

| reason | articles |
| --- | ---: |
| amrx_lanreotide | 1 |
| converge_or_clash | 1740 |
| graded_0_1d | 1533 |
| stack_2026-09-18 | 8 |

Exact Tier A n = **2993**.

| | n |
| --- | ---: |
| Lane-ok | 1488 |
| Lane-fail | 1505 |
| deterministic leftover | 1505 |
| missing (not attempted) | 0 |

## Hop histogram (provider::model)

Lane-ok rows only. New calls are strict free tier: Zhipu glm-4.7-flash, SiliconFlow THUDM/GLM-Z1-9B-0414 and deepseek-ai/DeepSeek-R1-Distill-Qwen-7B, OpenRouter :free. DashScope qwen-flash is not called (free grant exhausted). Existing qwen-flash and deepseek-chat / deepseek-flash rows are kept and are not re-called. A 402 tries the next free id on that provider. A 403 or 429 abandons the provider. DeepSeek, TokenHub, Gemini, and Qwen/Qwen3-8B are not called.

- `qwen::qwen-flash`: 1304
- `deepseek::deepseek-chat`: 168
- `deepseek::deepseek-flash`: 9
- `openrouter::openrouter/free`: 5
- `openrouter::z-ai/glm-5.2:free`: 2

## Paired rubric (same rows)

Strict columns use article+ticker calls that are gradeable on both the #314 deterministic tag and the Lane tag. Own columns let the denominator move when Lane changes class or direction. This is the Tier A subset, not the full Elite book.

| horizon | det strict | lane strict | n | det own | lane own |
| --- | --- | --- | ---: | --- | --- |
| 0-1d | 427/909 = 47.0% | 419/909 = 46.1% | 909 | 569/1233 = 46.1% | 481/1021 = 47.1% |
| 2d | 405/819 = 49.5% | 393/819 = 48.0% | 819 | 561/1133 = 49.5% | 449/931 = 48.2% |
| 3d | 378/774 = 48.8% | 373/774 = 48.2% | 774 | 530/1083 = 48.9% | 430/886 = 48.5% |
| 4d | 374/747 = 50.1% | 367/747 = 49.1% | 747 | 521/1051 = 49.6% | 428/859 = 49.8% |
| 5d | 369/720 = 51.2% | 355/720 = 49.3% | 720 | 520/1023 = 50.8% | 414/832 = 49.8% |
| 1-4w | 363/796 = 45.6% | 367/796 = 46.1% | 796 | 619/1317 = 47.0% | 374/806 = 46.4% |
| 2-5d | 1526/3060 = 49.9% | 1488/3060 = 48.6% | 3060 | 2132/4290 = 49.7% | 1721/3508 = 49.1% |

### Published #314 full book (not this universe)

Full Elite book from #314. Not the paired Tier A universe. That run's probe can read keys_present while attempted stays 0 because the push path did not pass --lane.

- full-book 0-1d rubric: 839/1829
- impulse + up/down + listed: 9496
- graded articles 0-1d: 1048
- ledger counts: `{"clash": 67, "converge": 497, "converge_down": 153, "converge_up": 344, "singleton": 6128}`

## Rippers (Lane calls only)

Lane-ok calls only. Same direction as the call. 5% rows are ≥5% and <10%. 10% rows are ≥10%.

### 0-1d

- ≥5% and <10%: **56**
- ≥10%: **62**

| threshold | ticker | date | ret | class | watermark | title |
| --- | --- | --- | ---: | --- | --- | --- |
| ≥10% | QMCO | 2026-08-11 | +33.61% | print_vs_priced | `qwen::qwen-flash::qwen` | Quantum Corporation Q1 2027 Earnings Call Summary |
| ≥10% | MDXH | 2026-08-14 | +32.89% | print_vs_priced | `qwen::qwen-flash::qwen` | MDxHealth SA (MDXH) (Q2 2026) Earnings Call Highlights: Record Sequential Growth and Strat |
| ≥10% | MDXH | 2026-08-14 | +32.89% | preannounce | `deepseek::deepseek-chat::deepseek` | MDxHealth S.A. Q2 2026 Earnings Call Summary |
| ≥10% | BODI | 2026-08-11 | -29.56% | regime_break | `qwen::qwen-flash::qwen` | The Beachbody Company, Inc. Q2 2026 Earnings Call Summary |
| ≥10% | RCEL | 2026-08-07 | +29.28% | guidance | `qwen::qwen-flash::qwen` | AVITA Medical Reports Second Quarter Results, Raises 2026 Revenue Guidance, and Expects Fo |
| ≥10% | AEYE | 2026-08-14 | +29.08% | guidance | `qwen::qwen-flash::qwen` | AudioEye: Q2 Earnings Snapshot |
| ≥10% | AEYE | 2026-08-14 | +29.08% | print_vs_priced | `qwen::qwen-flash::qwen` | AudioEye Inc (AEYE) (Q2 2026) Earnings Call Highlights: Record Adjusted EBITDA and Raised  |
| ≥10% | AEYE | 2026-08-14 | +29.08% | guidance | `deepseek::deepseek-chat::deepseek` | AudioEye, Inc. Q2 2026 Earnings Call Summary |
| ≥10% | OPRX | 2026-08-13 | +28.74% | print_vs_priced | `qwen::qwen-flash::qwen` | OptimizeRx Corporation Q2 2026 Earnings Call Summary |
| ≥10% | CVRX | 2026-08-07 | -20.60% | guidance | `qwen::qwen-flash::qwen` | CVRx: Q2 Earnings Snapshot |
| ≥10% | CVRX | 2026-08-07 | -20.60% | guidance | `qwen::qwen-flash::qwen` | CVRx Inc (CVRX) (Q2 2026) Earnings Call Highlights: Sales Execution Challenges Offset Stro |
| ≥10% | OPRT | 2026-08-06 | +19.44% | print_vs_priced | `qwen::qwen-flash::qwen` | Oportun Financial Corp (OPRT) (Q2 2026) Earnings Call Highlights: Strong Profitability and |
| … | | | | | | 106 more in the JSON |

### 2d

- ≥5% and <10%: **97**
- ≥10%: **76**

| threshold | ticker | date | ret | class | watermark | title |
| --- | --- | --- | ---: | --- | --- | --- |
| ≥10% | QMCO | 2026-08-11 | +69.97% | print_vs_priced | `qwen::qwen-flash::qwen` | Quantum Corporation Q1 2027 Earnings Call Summary |
| ≥10% | VOGX | 2026-08-14 | +37.73% | listing_flow | `qwen::qwen-flash::qwen` | Vogenx Announces Closing of Initial Public Offering |
| ≥10% | RCEL | 2026-08-07 | +36.44% | guidance | `qwen::qwen-flash::qwen` | AVITA Medical Reports Second Quarter Results, Raises 2026 Revenue Guidance, and Expects Fo |
| ≥10% | TXG | 2026-08-07 | +28.50% | print_vs_priced | `qwen::qwen-flash::qwen` | 10x Genomics, Inc. Q2 2026 Earnings Call Summary |
| ≥10% | VOGX | 2026-08-18 | +26.95% | listing_flow | `deepseek::deepseek-chat::deepseek` | Jones Served as Sole Book-Running Manager for Vogenx's $81.3 Million Initial Public Offeri |
| ≥10% | BODI | 2026-08-11 | -26.88% | regime_break | `qwen::qwen-flash::qwen` | The Beachbody Company, Inc. Q2 2026 Earnings Call Summary |
| ≥10% | RUM | 2026-08-11 | +25.67% | capacity | `qwen::qwen-flash::qwen` | RUM Group Inc (RUM) (Q2 2026) Earnings Call Highlights: Record Revenue and AI Infrastructu |
| ≥10% | RUM | 2026-08-11 | +25.67% | print_vs_priced | `qwen::qwen-flash::qwen` | Rumble Inc. Q2 2026 Earnings Call Summary |
| ≥10% | EBS | 2026-08-06 | -24.87% | guidance | `qwen::qwen-flash::qwen` | Emergent Biosolutions: Q2 Earnings Snapshot |
| ≥10% | EBS | 2026-08-06 | -24.87% | guidance | `qwen::qwen-flash::qwen` | Emergent BioSolutions Inc. Q2 2026 Earnings Call Summary |
| ≥10% | OPRX | 2026-08-13 | +24.34% | print_vs_priced | `qwen::qwen-flash::qwen` | OptimizeRx Corporation Q2 2026 Earnings Call Summary |
| ≥10% | MDXH | 2026-08-14 | +23.52% | print_vs_priced | `qwen::qwen-flash::qwen` | MDxHealth SA (MDXH) (Q2 2026) Earnings Call Highlights: Record Sequential Growth and Strat |
| … | | | | | | 161 more in the JSON |

### 3d

- ≥5% and <10%: **113**
- ≥10%: **82**

| threshold | ticker | date | ret | class | watermark | title |
| --- | --- | --- | ---: | --- | --- | --- |
| ≥10% | CYCU | 2026-08-17 | +667.60% | capital_return | `qwen::qwen-flash::qwen` | Cycurion Board Authorizes $500,000 Share Repurchase Program, Citing Meaningful Undervaluat |
| ≥10% | QMCO | 2026-08-11 | +79.82% | print_vs_priced | `qwen::qwen-flash::qwen` | Quantum Corporation Q1 2027 Earnings Call Summary |
| ≥10% | VOGX | 2026-08-14 | +61.51% | listing_flow | `qwen::qwen-flash::qwen` | Vogenx Announces Closing of Initial Public Offering |
| ≥10% | VOGX | 2026-08-18 | +58.45% | listing_flow | `deepseek::deepseek-chat::deepseek` | Jones Served as Sole Book-Running Manager for Vogenx's $81.3 Million Initial Public Offeri |
| ≥10% | RCEL | 2026-08-07 | +35.11% | guidance | `qwen::qwen-flash::qwen` | AVITA Medical Reports Second Quarter Results, Raises 2026 Revenue Guidance, and Expects Fo |
| ≥10% | CTEV | 2026-08-10 | +26.98% | print_vs_priced | `qwen::qwen-flash::qwen` | Claritev Corp (CTEV) (Q2 2026) Earnings Call Highlights: Record Bookings and AI-Driven Gro |
| ≥10% | CTEV | 2026-08-10 | +26.98% | print_vs_priced | `qwen::qwen-flash::qwen` | Claritev Corporation Q2 2026 Earnings Call Summary |
| ≥10% | TXG | 2026-08-07 | +26.90% | print_vs_priced | `qwen::qwen-flash::qwen` | 10x Genomics, Inc. Q2 2026 Earnings Call Summary |
| ≥10% | FSLY | 2026-08-06 | +26.62% | demand | `qwen::qwen-flash::qwen` | Fastly Inc (FSLY) (Q2 2026) Earnings Call Highlights: Record Revenue and Security Growth D |
| ≥10% | EBS | 2026-08-06 | -25.21% | guidance | `qwen::qwen-flash::qwen` | Emergent Biosolutions: Q2 Earnings Snapshot |
| ≥10% | EBS | 2026-08-06 | -25.21% | guidance | `qwen::qwen-flash::qwen` | Emergent BioSolutions Inc. Q2 2026 Earnings Call Summary |
| ≥10% | MDXH | 2026-08-14 | +24.84% | print_vs_priced | `qwen::qwen-flash::qwen` | MDxHealth SA (MDXH) (Q2 2026) Earnings Call Highlights: Record Sequential Growth and Strat |
| … | | | | | | 183 more in the JSON |

### 4d

- ≥5% and <10%: **112**
- ≥10%: **96**

| threshold | ticker | date | ret | class | watermark | title |
| --- | --- | --- | ---: | --- | --- | --- |
| ≥10% | BYND | 2026-08-06 | +1838.33% | print_vs_priced | `qwen::qwen-flash::qwen` | BYND Stock Heads For Second Weekly Gains: Beyond Meat Sees Early Turnaround Signs As CEO M |
| ≥10% | BYND | 2026-08-06 | +1838.33% | print_vs_priced | `qwen::qwen-flash::qwen` | Beyond Meat, Inc. Q2 2026 Earnings Call Summary |
| ≥10% | CYCU | 2026-08-17 | +630.73% | capital_return | `qwen::qwen-flash::qwen` | Cycurion Board Authorizes $500,000 Share Repurchase Program, Citing Meaningful Undervaluat |
| ≥10% | QMCO | 2026-08-11 | +71.63% | print_vs_priced | `qwen::qwen-flash::qwen` | Quantum Corporation Q1 2027 Earnings Call Summary |
| ≥10% | VOGX | 2026-08-18 | +64.95% | listing_flow | `deepseek::deepseek-chat::deepseek` | Jones Served as Sole Book-Running Manager for Vogenx's $81.3 Million Initial Public Offeri |
| ≥10% | VOGX | 2026-08-14 | +64.92% | listing_flow | `qwen::qwen-flash::qwen` | Vogenx Announces Closing of Initial Public Offering |
| ≥10% | RUM | 2026-08-11 | +37.33% | capacity | `qwen::qwen-flash::qwen` | RUM Group Inc (RUM) (Q2 2026) Earnings Call Highlights: Record Revenue and AI Infrastructu |
| ≥10% | RUM | 2026-08-11 | +37.33% | print_vs_priced | `qwen::qwen-flash::qwen` | Rumble Inc. Q2 2026 Earnings Call Summary |
| ≥10% | RCEL | 2026-08-07 | +33.28% | guidance | `qwen::qwen-flash::qwen` | AVITA Medical Reports Second Quarter Results, Raises 2026 Revenue Guidance, and Expects Fo |
| ≥10% | BODI | 2026-08-11 | -28.33% | regime_break | `qwen::qwen-flash::qwen` | The Beachbody Company, Inc. Q2 2026 Earnings Call Summary |
| ≥10% | TXG | 2026-08-07 | +27.77% | print_vs_priced | `qwen::qwen-flash::qwen` | 10x Genomics, Inc. Q2 2026 Earnings Call Summary |
| ≥10% | BTMD | 2026-08-06 | -25.65% | regime_break | `qwen::qwen-flash::qwen` | Biote Corp (BTMD) (Q2 2026) Earnings Call Highlights: Navigating Recall Challenges and ... |
| … | | | | | | 196 more in the JSON |

### 5d

- ≥5% and <10%: **103**
- ≥10%: **106**

| threshold | ticker | date | ret | class | watermark | title |
| --- | --- | --- | ---: | --- | --- | --- |
| ≥10% | BYND | 2026-08-06 | +2023.33% | print_vs_priced | `qwen::qwen-flash::qwen` | BYND Stock Heads For Second Weekly Gains: Beyond Meat Sees Early Turnaround Signs As CEO M |
| ≥10% | BYND | 2026-08-06 | +2023.33% | print_vs_priced | `qwen::qwen-flash::qwen` | Beyond Meat, Inc. Q2 2026 Earnings Call Summary |
| ≥10% | CYCU | 2026-08-17 | +616.20% | capital_return | `qwen::qwen-flash::qwen` | Cycurion Board Authorizes $500,000 Share Repurchase Program, Citing Meaningful Undervaluat |
| ≥10% | VOGX | 2026-08-14 | +105.86% | listing_flow | `qwen::qwen-flash::qwen` | Vogenx Announces Closing of Initial Public Offering |
| ≥10% | VOGX | 2026-08-18 | +69.24% | listing_flow | `deepseek::deepseek-chat::deepseek` | Jones Served as Sole Book-Running Manager for Vogenx's $81.3 Million Initial Public Offeri |
| ≥10% | QMCO | 2026-08-11 | +61.98% | print_vs_priced | `qwen::qwen-flash::qwen` | Quantum Corporation Q1 2027 Earnings Call Summary |
| ≥10% | UMAC | 2026-08-07 | +35.16% | regime_break | `qwen::qwen-flash::qwen` | Unusual Machines, Inc. Q2 2026 Earnings Call Summary |
| ≥10% | RUM | 2026-08-11 | +35.00% | capacity | `qwen::qwen-flash::qwen` | RUM Group Inc (RUM) (Q2 2026) Earnings Call Highlights: Record Revenue and AI Infrastructu |
| ≥10% | RUM | 2026-08-11 | +35.00% | print_vs_priced | `qwen::qwen-flash::qwen` | Rumble Inc. Q2 2026 Earnings Call Summary |
| ≥10% | FSLY | 2026-08-06 | +32.07% | demand | `qwen::qwen-flash::qwen` | Fastly Inc (FSLY) (Q2 2026) Earnings Call Highlights: Record Revenue and Security Growth D |
| ≥10% | BODI | 2026-08-11 | -31.35% | regime_break | `qwen::qwen-flash::qwen` | The Beachbody Company, Inc. Q2 2026 Earnings Call Summary |
| ≥10% | RCEL | 2026-08-07 | +30.62% | guidance | `qwen::qwen-flash::qwen` | AVITA Medical Reports Second Quarter Results, Raises 2026 Revenue Guidance, and Expects Fo |
| … | | | | | | 197 more in the JSON |

## AMRX

| field | value |
| --- | --- |
| watermark | `deterministic::news_impact_v2::deterministic` |
| lane_ok | False |
| title | Amneal Announces FDA Approval and Launch of Lanreotide Injection |
| News Time | 2026-09-18 16:01:00 |
| entry_date | 2026-09-21 |
| class | gate / q5=impulse / sign=open |
| direction | up |
| 0-1d | +6.33% agree=True |
| 2d | n/a agree=None |
| 5d | n/a agree=None |
| 1-4w | n/a agree=None |

Friday 2026-09-18 16:01 is after the cash close. Entry is Monday 2026-09-21 open to close for 0-1d.

## 2026-09-18 stack

SECZ / COIN / CRCL / SCHW rows whose session or News Time is 2026-09-18, plus the AMRX lanreotide row. Watermark is Lane when the hop landed.

| ticker | session | lane_ok | watermark | direction | 0-1d | title |
| --- | --- | --- | --- | --- | ---: | --- |
| COIN | 2026-09-18 | False | `deterministic::news_impact_v2::deterministic` | up | +9.03% | Polymarket Hires Coinbase's Failed Social-Coin Architect |
| CRCL | 2026-09-18 | False | `deterministic::news_impact_v2::deterministic` |  | n/a | Is Being Indian a Fraud Signal? Arc Traders Sold Like It Is |
| SECZ | 2026-09-18 | False | `deterministic::news_impact_v2::deterministic` | up | +16.52% | SEC Sends Strong Signal to Robinhood, Coinbase Investors |
| SCHW | 2026-09-18 | False | `deterministic::news_impact_v2::deterministic` |  | n/a | Bond Rout Spurs Record Outflow From $3.6 Billion Schwab Muni ETF |
| SECZ | 2026-09-18 | False | `deterministic::news_impact_v2::deterministic` | up | +16.52% | SEC Sends Strong Signal to Robinhood, Coinbase Investors |
| AMRX | 2026-09-21 | False | `deterministic::news_impact_v2::deterministic` | up | +6.33% | Amneal Announces FDA Approval and Launch of Lanreotide Injection |
| CRCL | 2026-09-21 | False | `deterministic::news_impact_v2::deterministic` |  | n/a | Weekly Wrap: Bitcoin Climbs Back Above $80,000 |
| CRCL | 2026-09-21 | False | `deterministic::news_impact_v2::deterministic` |  | n/a | Weekly Wrap: Bitcoin Climbs Back Above $80,000 |
| SCHW | 2026-09-21 | False | `deterministic::news_impact_v2::deterministic` |  | n/a | ETF League Tables: Schwab Registers Hefty Inflows |

## Converge / singleton / clash under Lane tags

Ledger is the Tier A rows under Lane tags, not the full Elite singleton book. Converge/clash membership started from the #314 groups; Lane may retag a group.

One entity per session. n_bull / n_bear count impulse up/down stories after title-normalize. Regime and weather do not vote. clash = both sides on that entity that session. converge = two or more stories, one side. Hit rates grade the entity once: converge uses that side, clash uses the net side (net 0 is ungraded). Long-horizon classes stay out of the 0-1d..5d denominator.

| bucket | Lane groups | det groups (same rows) | Lane 0-1d | det 0-1d | Lane 5d | det 5d |
| --- | ---: | ---: | --- | --- | --- | --- |
| singleton | 1033 | 1189 | 334/753 = 44.4% | 432/952 = 45.4% | 311/640 = 48.6% | 418/826 = 50.6% |
| converge | 328 | 497 | 73/127 = 57.5% | 55/109 = 50.5% | 52/99 = 52.5% | 41/79 = 51.9% |
| clash | 34 | 67 | 0/1 = 0.0% | 4/10 = 40.0% | n/a | 2/8 = 25.0% |

### Top Lane converge names

| ticker | date | n_bull | n_bear | net | 0-1d | 5d |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| SECZ | 2026-09-18 | 6 | 0 | 6 | +16.52% | n/a |
| COIN | 2026-09-18 | 6 | 0 | 6 | +9.03% | n/a |
| CRCL | 2026-09-18 | 6 | 0 | 6 | +5.01% | n/a |
| SCHW | 2026-09-18 | 0 | 6 | -6 | +0.54% | n/a |
| SECZ | 2026-09-21 | 4 | 0 | 4 | +15.68% | n/a |
| CRCL | 2026-09-21 | 4 | 0 | 4 | -3.67% | n/a |
| COIN | 2026-09-21 | 4 | 0 | 4 | -2.02% | n/a |
| SCHW | 2026-09-21 | 0 | 4 | -4 | +0.93% | n/a |
| AEYE | 2026-08-14 | 3 | 0 | 3 | +29.08% | +16.32% |
| SKIL | 2026-09-10 | 0 | 3 | -3 | -22.98% | +16.36% |
| TLYS | 2026-09-03 | 3 | 0 | 3 | -20.72% | -17.12% |
| KMTS | 2026-09-15 | 3 | 0 | 3 | +14.98% | n/a |
| ALTG | 2026-08-07 | 0 | 3 | -3 | +12.11% | +8.37% |
| LIDR | 2026-08-07 | 3 | 0 | 3 | +10.92% | +10.08% |
| LAKE | 2026-09-10 | 0 | 3 | -3 | -8.45% | -0.38% |

## Clock

Entry is the next regular session after News Time. Published at/after 09:30 ET, including 09:30+30m, waits for the next RTH. 0-1d is that session's open to close. 2d/3d/4d/5d are the close N sessions later. 1-4w is 20 sessions.

## Hopper watermark (lane::model::inference_source)

- `deterministic::news_impact_v2::deterministic`: 1505
- `qwen::qwen-flash::qwen`: 1304
- `deepseek::deepseek-chat::deepseek`: 168
- `deepseek::deepseek-flash::deepseek`: 9
- `openrouter::openrouter/free::openrouter`: 5
- `openrouter::z-ai/glm-5.2:free::openrouter`: 2

ship_lane = **False** (true only when every Tier A row has a live current-flash watermark).
