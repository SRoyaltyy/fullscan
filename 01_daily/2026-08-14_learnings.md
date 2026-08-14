# Learnings report — 2026-08-14

Generated: **2026-08-14T17:50:20.508795-04:00** by `src/learn_cycle.py`.

This is the human-readable digest of what the bot **actually learned** this cycle: graded evidence, hypotheses (wins and losses), promoted standing rules, and **how that changes every daily workflow**.

Machine policy file (injected into predicts): `00_grounding/mutable_policy.md`.

---

## 1. Snapshot

| Item | Value |
|------|-------|
| Graded runs mined | 55 |
| Hypotheses written | 56 (wins=34, losses=22) |
| News hypotheses | 1 |
| Lessons promoted to active | 12 |
| Active lesson files now | 45 |

## 2. Accuracy by topic (evidence this cycle learned from)

| Topic | Direction HIT% | hits/n | Read |
|-------|----------------|--------|------|
| general | 73% | 8/11 | ok |
| sector:Basic Materials | 75% | 3/4 | ok |
| sector:Communication Services | 25% | 1/4 | thin/weak |
| sector:Consumer Cyclical | 75% | 3/4 | ok |
| sector:Consumer Defensive | 50% | 2/4 | thin/weak |
| sector:Energy | 50% | 2/4 | thin/weak |
| sector:Financial | 75% | 3/4 | ok |
| sector:Healthcare | 50% | 2/4 | thin/weak |
| sector:Industrials | 25% | 1/4 | thin/weak |
| sector:Real Estate | 75% | 3/4 | ok |
| sector:Technology | 50% | 2/4 | thin/weak |
| sector:Utilities | 75% | 3/4 | ok |

## 3. What we learned (by scope)

Each scope lists recent win and loss hypotheses: the **counterfactual ask**, the **experiment** to run next, and the **policy candidate** (do instead).

### `general` — 8 wins, 3 losses

#### LOSS — 2026-08-10
- **When:** [general] Predicted down but went flat (pct=-0.06, score=-3.6, sector=).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [general] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [general] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [general] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-08-11
- **When:** [general] Predicted down, market/sector went down (pct=-0.32, score=-2.475, sector=).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [general] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [general] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-08-12
- **When:** [general] Predicted up, market/sector went up (pct=0.26, score=2.25, sector=).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [general] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [general] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-08-13
- **When:** [general] Predicted up, market/sector went up (pct=0.65, score=8.525, sector=).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [general] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [general] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-08-14
- **When:** [general] Predicted up but went down (pct=-0.17, score=5.5, sector=).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [general] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [general] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [general] Wrong if this hedge reduces direction accuracy over 10 runs.

### `news` — 1 wins, 0 losses

#### WIN — news
- **When:** [news] summary={"n_suggestions": 157, "ever_profitable": {"n": 157, "wins": 154, "win_rate": 98.1}, "close_1d": {"n": 123, "wins": 69, "win_rate": 56.1, "avg": 0.32}, "close_3d": {"n": 44, "wins": 28, "win_rate": 63.6, "avg": 0.94}, "close_5d": null, "close_10d": null, "close_14d": null, "side_buy": {"n": 89, "eve
- **Ask:** Which event families drive ever-profitable vs 1d close?
- **Experiment:** [news] Track event-level 1d close win rate daily in learn_cycle.
- **Do instead:** [news] Rank event families by 1d close, not ever-touch MFE.
- **Wrong if:** [news] Wrong if ever-touch is the better trading objective for you.

### `sector_basic_materials` — 3 wins, 1 losses

#### WIN — 2026-08-10
- **When:** [sector_basic_materials] Predicted up, market/sector went up (pct=0.6053720982393429, score=14.4, sector=Basic Materials).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_basic_materials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_basic_materials] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-08-11
- **When:** [sector_basic_materials] Predicted up, market/sector went up (pct=0.11282695176135782, score=18.2, sector=Basic Materials).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_basic_materials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_basic_materials] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-08-12
- **When:** [sector_basic_materials] Predicted up but went down (pct=-1.2396690958014212, score=14.4, sector=Basic Materials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_basic_materials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_basic_materials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-08-13
- **When:** [sector_basic_materials] Predicted down, market/sector went down (pct=-0.5135040858903261, score=-6.0, sector=Basic Materials).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_basic_materials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_basic_materials] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_communication_services` — 1 wins, 3 losses

#### LOSS — 2026-08-10
- **When:** [sector_communication_services] Predicted down but went up (pct=0.521349960498596, score=-4.5, sector=Communication Services).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_communication_services] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_communication_services] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-08-11
- **When:** [sector_communication_services] Predicted up but went down (pct=-0.5007647132424298, score=7.5, sector=Communication Services).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_communication_services] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_communication_services] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-08-12
- **When:** [sector_communication_services] Predicted down, market/sector went down (pct=-0.8987148648955334, score=-6.0, sector=Communication Services).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_communication_services] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_communication_services] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_communication_services] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-08-13
- **When:** [sector_communication_services] Predicted down but went up (pct=2.067658001361483, score=-9.0, sector=Communication Services).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_communication_services] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_communication_services] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_consumer_cyclical` — 3 wins, 1 losses

#### WIN — 2026-08-10
- **When:** [sector_consumer_cyclical] Predicted down, market/sector went down (pct=-0.15852030739088585, score=-1.5, sector=Consumer Cyclical).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_cyclical] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_cyclical] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-08-11
- **When:** [sector_consumer_cyclical] Predicted up but went down (pct=-0.35932172788096794, score=3.0, sector=Consumer Cyclical).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_cyclical] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_cyclical] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-08-12
- **When:** [sector_consumer_cyclical] Predicted down, market/sector went down (pct=-1.1321691532261258, score=-4.0, sector=Consumer Cyclical).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_cyclical] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_cyclical] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-08-13
- **When:** [sector_consumer_cyclical] Predicted up, market/sector went up (pct=0.47501701712870936, score=2.0, sector=Consumer Cyclical).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_cyclical] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_cyclical] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_consumer_defensive` — 2 wins, 2 losses

#### WIN — 2026-08-10
- **When:** [sector_consumer_defensive] Predicted down, market/sector went down (pct=-0.19972485062762502, score=-9.6, sector=Consumer Defensive).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_defensive] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_defensive] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-08-11
- **When:** [sector_consumer_defensive] Predicted flat but went down (pct=-0.3060559342860758, score=-0.9, sector=Consumer Defensive).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_defensive] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_defensive] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-08-12
- **When:** [sector_consumer_defensive] Predicted down but went up (pct=0.4605022770170164, score=-6.75, sector=Consumer Defensive).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_defensive] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_defensive] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-08-13
- **When:** [sector_consumer_defensive] Predicted up, market/sector went up (pct=1.0813330384879194, score=1.8, sector=Consumer Defensive).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_defensive] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_defensive] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_energy` — 2 wins, 2 losses

#### WIN — 2026-08-10
- **When:** [sector_energy] Predicted up, market/sector went up (pct=4.660870095957881, score=13.0, sector=Energy).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_energy] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_energy] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-08-11
- **When:** [sector_energy] Predicted flat but went up (pct=1.2462612100310855, score=-0.9, sector=Energy).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_energy] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_energy] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-08-12
- **When:** [sector_energy] Predicted up, market/sector went up (pct=0.16412025869068092, score=12.0, sector=Energy).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_energy] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_energy] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-08-13
- **When:** [sector_energy] Predicted down but went flat (pct=0.04916040405413824, score=-3.25, sector=Energy).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_energy] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_energy] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_financial` — 3 wins, 1 losses

#### WIN — 2026-08-10
- **When:** [sector_financial] Predicted up, market/sector went up (pct=0.3645883762727342, score=18.525, sector=Financial).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_financial] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_financial] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-08-11
- **When:** [sector_financial] Predicted up but went flat (pct=-0.017301740171016267, score=15.0, sector=Financial).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_financial] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_financial] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-08-12
- **When:** [sector_financial] Predicted up, market/sector went up (pct=0.20761061153755644, score=15.3, sector=Financial).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_financial] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_financial] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-08-13
- **When:** [sector_financial] Predicted up, market/sector went up (pct=0.5870168565892397, score=9.675, sector=Financial).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_financial] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_financial] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_healthcare` — 2 wins, 2 losses

#### WIN — 2026-08-10
- **When:** [sector_healthcare] Predicted up, market/sector went up (pct=1.6658678703747043, score=14.7, sector=Healthcare).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_healthcare] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_healthcare] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-08-11
- **When:** [sector_healthcare] Predicted up but went down (pct=-0.2552884874956529, score=11.0, sector=Healthcare).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_healthcare] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_healthcare] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-08-12
- **When:** [sector_healthcare] Predicted up, market/sector went up (pct=0.2559418776439726, score=15.9, sector=Healthcare).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_healthcare] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_healthcare] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-08-13
- **When:** [sector_healthcare] Predicted up but went flat (pct=-0.03561954270014933, score=10.25, sector=Healthcare).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_healthcare] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_healthcare] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_industrials` — 1 wins, 3 losses

#### LOSS — 2026-08-10
- **When:** [sector_industrials] Predicted up but went down (pct=-0.3132015310536751, score=8.25, sector=Industrials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_industrials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-08-11
- **When:** [sector_industrials] Predicted up, market/sector went up (pct=0.5958780110276507, score=14.4, sector=Industrials).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_industrials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_industrials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_industrials] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-08-12
- **When:** [sector_industrials] Predicted up but went flat (pct=0.09693480750054828, score=12.65, sector=Industrials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_industrials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-08-13
- **When:** [sector_industrials] Predicted up but went flat (pct=-0.04842457193631189, score=4.95, sector=Industrials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_industrials] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_real_estate` — 3 wins, 1 losses

#### WIN — 2026-08-10
- **When:** [sector_real_estate] Predicted down, market/sector went down (pct=-1.2894575861718272, score=-6.75, sector=Real Estate).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_real_estate] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_real_estate] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-08-11
- **When:** [sector_real_estate] Predicted down, market/sector went down (pct=-0.7207200086191579, score=-2.25, sector=Real Estate).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_real_estate] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_real_estate] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-08-12
- **When:** [sector_real_estate] Predicted down but went up (pct=0.9301266569441413, score=-6.75, sector=Real Estate).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_real_estate] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_real_estate] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-08-13
- **When:** [sector_real_estate] Predicted up, market/sector went up (pct=1.4160423233314567, score=5.5, sector=Real Estate).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_real_estate] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_real_estate] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_technology` — 2 wins, 2 losses

#### LOSS — 2026-08-10
- **When:** [sector_technology] Predicted up but went down (pct=-0.8777963961105972, score=10.8, sector=Technology).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_technology] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_technology] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-08-11
- **When:** [sector_technology] Predicted up but went down (pct=-0.12344942962989602, score=2.0, sector=Technology).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_technology] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_technology] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-08-12
- **When:** [sector_technology] Predicted up, market/sector went up (pct=1.4885293820046774, score=11.7, sector=Technology).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_technology] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_technology] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_technology] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-08-13
- **When:** [sector_technology] Predicted up, market/sector went up (pct=1.0113330805552767, score=10.8, sector=Technology).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_technology] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_technology] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_technology] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_utilities` — 3 wins, 1 losses

#### WIN — 2026-08-10
- **When:** [sector_utilities] Predicted down, market/sector went down (pct=-1.1006639200146995, score=-8.8, sector=Utilities).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_utilities] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-08-11
- **When:** [sector_utilities] Predicted down but went up (pct=1.159285851188252, score=-1.8, sector=Utilities).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_utilities] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-08-12
- **When:** [sector_utilities] Predicted up, market/sector went up (pct=0.4813180823553198, score=10.0, sector=Utilities).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_utilities] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-08-13
- **When:** [sector_utilities] Predicted up, market/sector went up (pct=0.4562061182557864, score=5.5, sector=Utilities).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_utilities] Wrong if milder bands hurt direction accuracy over 10 runs.

## 4. Promoted standing rules (this cycle)

- `existing-basic-materials-rule-is-confirmed-after-a-hard-xlb.md`
- `a-prior-session-geopolitical-oil-supply-shock-e-g-hormuz-clo.md`
- `when-a-sector-call-is-verified-correct-predicted-up-flat-act.md`
- `a-rate-sensitive-defensive-sector-consumer-staples-has-been.md`
- `no-new-lesson-needed-confirmed-pattern-an-energy-xle-call-ha.md`
- `a-sector-call-has-a-scheduled-8-30-et-macro-release-pending.md`
- `a-sector-has-led-spy-on-a-defensive-relative-strength-rotati.md`
- `when-an-oil-sensitive-cyclical-sector-industrials-is-being-s.md`
- `rule.md`
- `a-technology-xlk-call-has-fresh-real-catalysts-ai-infrastruc.md`
- `in-a-utilities-xlu-call-a-second-soft-inflation-print-has-al.md`
- `rule.md`

Full text lives in `02_lessons/active/`. Summaries also feed `mutable_policy.md`.

## 5. How these learnings affect daily workflows

### General market predict (`run_predict` / daily pipeline)
- Loads `mutable_policy.md` + `02_lessons/active/*` via `memory.prediction_context()`.
- Must answer methodology checklist in MEMORY_CONFIRM.
- Ops lessons (missing predict file) change grading, not B0–B7 math.
- Macro / geo / regime lessons change how Channel-2 evidence is weighted in narrative.

### Per-sector predict (`run_sector_predict` / sector daily)
- Loads the **same** `mutable_policy.md` via `sector_memory` (filter lines for this sector + general).
- Sector-specific active lessons (XLB temper, XLK geo, XLE Hormuz, staples/CPI) apply to S0–S4 scoring judgment.
- Weak sectors in accuracy table → extra caution, milder bands, demand confirming tape.

### Sector / general outcome + reflect
- Outcomes grade hits; reflect writes new candidates.
- Next learn_cycle mines those candidates again (promote if complete).

### News parse + news actions
- Hypotheses under scope `news` steer event-family conviction and ticker mapping.
- Prefer 1d close quality over ever-touch MFE when ranking event edges.
- Does not change SCORES format; changes which edges deserve size.

### Label + Weather + Join
- `weather_rules_proposals.json` may suggest threshold nudges (not auto-applied).
- Label membership unchanged; weather stances may tighten if proposals are accepted later.
- Join/match inherits weather; better weather → cleaner favorable/hostile books.

### HIT board / report card
- Still pure arithmetic over scoreboard; learn_cycle does not rewrite history.
- Accuracy-by-topic in this file should match HIT board trends over time.

## 6. Concrete operating rules for tomorrow

1. **General:** Follow active ops + macro lessons; apply open experiments when setup matches.
2. **Weak sectors** (HIT% soft in §2): default to milder magnitude; demand ETF tape confirmation.
3. **Strong general / solid sectors:** Do not loosen risk controls only because recent hits look good.
4. **News:** Prefer event families with clean 1d close evidence; do not size on MFE alone.
5. **Weather/join:** Review `weather_rules_proposals.json` before accepting threshold changes.
6. **All predicts:** Core output blocks stay fixed; only judgment/weights/search emphasis change.


## 7. Files touched

| File | Role |
|------|------|
| `03_scoreboard/LEARNINGS.md` | This digest (latest) |
| `01_daily/2026-08-14_learnings.md` | Dated copy |
| `00_grounding/mutable_policy.md` | Injected into general + sector predict |
| `02_lessons/hypotheses/*` | Per-event experiments |
| `02_lessons/active/*` | Standing rules |
| `00_grounding/weather_rules_proposals.json` | Optional weather threshold deltas |
