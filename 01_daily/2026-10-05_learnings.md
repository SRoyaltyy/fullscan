# Learnings report — 2026-10-05

Generated: **2026-10-06T19:51:39.185530-04:00** by `src/learn_cycle.py`.

This is the human-readable digest of what the bot **actually learned** this cycle: graded evidence, hypotheses (wins and losses), promoted standing rules, and **how that changes every daily workflow**.

Machine policy file (injected into predicts): `00_grounding/mutable_policy.md`.

---

## 1. Snapshot

| Item | Value |
|------|-------|
| Graded runs mined | 180 |
| Hypotheses written | 181 (wins=79, losses=102) |
| News hypotheses | 1 |
| Lessons promoted to active | 0 |
| Lessons retired (efficacy-gated) | 10 |
| Active lesson files now | 207 |
| Engine policy version | 54 |

## 2. Accuracy by topic (evidence this cycle learned from)

| Topic | Direction HIT% | hits/n | Read |
|-------|----------------|--------|------|
| general | 53% | 8/15 | weak — priority |
| sector:Basic Materials | 33% | 5/15 | weak — priority |
| sector:Communication Services | 20% | 3/15 | weak — priority |
| sector:Consumer Cyclical | 27% | 4/15 | weak — priority |
| sector:Consumer Defensive | 60% | 9/15 | ok |
| sector:Energy | 53% | 8/15 | weak — priority |
| sector:Financial | 60% | 9/15 | ok |
| sector:Healthcare | 40% | 6/15 | weak — priority |
| sector:Industrials | 47% | 7/15 | weak — priority |
| sector:Real Estate | 60% | 9/15 | ok |
| sector:Technology | 33% | 5/15 | weak — priority |
| sector:Utilities | 40% | 6/15 | weak — priority |

## 3. What we learned (by scope)

Each scope lists recent win and loss hypotheses: the **counterfactual ask**, the **experiment** to run next, and the **policy candidate** (do instead).

### `general` — 8 wins, 7 losses

#### WIN — 2026-09-28
- **When:** [general] Predicted down, market/sector went down (pct=-0.77, score=-6.14, sector=).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [general] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [general] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-29
- **When:** [general] Predicted up but went down (pct=-0.17, score=1.292, sector=).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [general] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [general] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [general] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-10-01
- **When:** [general] Predicted down but went up (pct=0.19, score=-1.343, sector=).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [general] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [general] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [general] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-10-02
- **When:** [general] Predicted up, market/sector went up (pct=0.73, score=4.365, sector=).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [general] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [general] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-10-05
- **When:** [general] Predicted flat but went up (pct=0.66, score=0.086, sector=).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [general] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [general] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [general] Wrong if this hedge reduces direction accuracy over 10 runs.

### `news` — 0 wins, 1 losses

#### LOSS — news
- **When:** [news] Global 1d close win rate 53.5% (n=1967).
- **Ask:** Entry timing, side mix, or event taxonomy noise?
- **Experiment:** [news] Raise min net weight to map a ticker; drop weak edges.
- **Do instead:** [news] Only emit actions with |net| above a higher floor.
- **Wrong if:** [news] Wrong if higher floor reduces 1d win rate further.

### `sector_basic_materials` — 5 wins, 10 losses

#### WIN — 2026-09-24
- **When:** [sector_basic_materials] Predicted down, market/sector went down (pct=-1.1933144166426435, score=-2.88, sector=Basic Materials).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_basic_materials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_basic_materials] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-25
- **When:** [sector_basic_materials] Predicted flat but went up (pct=0.24154374224563124, score=1.377, sector=Basic Materials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_basic_materials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_basic_materials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-28
- **When:** [sector_basic_materials] Predicted down, market/sector went down (pct=-0.6626466293433997, score=-6.779, sector=Basic Materials).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_basic_materials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_basic_materials] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-10-01
- **When:** [sector_basic_materials] Predicted down, market/sector went down (pct=-0.32854177598672374, score=-0.769, sector=Basic Materials).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_basic_materials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_basic_materials] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-10-02
- **When:** [sector_basic_materials] Predicted flat but went up (pct=0.700453535589296, score=3.008, sector=Basic Materials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_basic_materials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_basic_materials] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_communication_services` — 3 wins, 12 losses

#### LOSS — 2026-09-24
- **When:** [sector_communication_services] Predicted down but went up (pct=1.2704338452312003, score=-11.341, sector=Communication Services).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_communication_services] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_communication_services] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-25
- **When:** [sector_communication_services] Predicted flat but went down (pct=-0.9035869800856022, score=-0.386, sector=Communication Services).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_communication_services] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_communication_services] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-28
- **When:** [sector_communication_services] Predicted flat but went down (pct=-1.5757779689479001, score=-4.633, sector=Communication Services).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_communication_services] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_communication_services] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-10-01
- **When:** [sector_communication_services] Predicted up but went down (pct=-0.928177676819486, score=7.467, sector=Communication Services).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_communication_services] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_communication_services] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-10-02
- **When:** [sector_communication_services] Predicted up, market/sector went up (pct=0.3092562582685421, score=4.631, sector=Communication Services).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_communication_services] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_communication_services] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_communication_services] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_consumer_cyclical` — 4 wins, 11 losses

#### WIN — 2026-09-24
- **When:** [sector_consumer_cyclical] Predicted down, market/sector went down (pct=-0.2982393371025016, score=-8.472, sector=Consumer Cyclical).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_cyclical] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_cyclical] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-25
- **When:** [sector_consumer_cyclical] Predicted down but went up (pct=0.21754701272065358, score=-3.2, sector=Consumer Cyclical).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_cyclical] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_cyclical] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-28
- **When:** [sector_consumer_cyclical] Predicted down, market/sector went down (pct=-1.4109963757614863, score=-6.753, sector=Consumer Cyclical).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_cyclical] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_cyclical] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-10-01
- **When:** [sector_consumer_cyclical] Predicted down but went flat (pct=-0.02756227518029286, score=-6.764, sector=Consumer Cyclical).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_cyclical] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_cyclical] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-10-02
- **When:** [sector_consumer_cyclical] Predicted flat but went up (pct=1.130413918326978, score=3.723, sector=Consumer Cyclical).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_cyclical] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_cyclical] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_consumer_defensive` — 9 wins, 6 losses

#### WIN — 2026-09-23
- **When:** [sector_consumer_defensive] Predicted up, market/sector went up (pct=0.6225612153685445, score=0.282, sector=Consumer Defensive).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_defensive] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_defensive] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-25
- **When:** [sector_consumer_defensive] Predicted up, market/sector went up (pct=0.4406372384317514, score=2.978, sector=Consumer Defensive).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_defensive] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_defensive] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-28
- **When:** [sector_consumer_defensive] Predicted flat but went up (pct=0.2680980102955033, score=-0.622, sector=Consumer Defensive).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_defensive] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_defensive] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-10-01
- **When:** [sector_consumer_defensive] Predicted down, market/sector went down (pct=-0.3349834344638314, score=-2.512, sector=Consumer Defensive).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_defensive] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_defensive] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-10-02
- **When:** [sector_consumer_defensive] Predicted up, market/sector went up (pct=0.24896918172965243, score=3.994, sector=Consumer Defensive).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_defensive] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_defensive] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_energy` — 8 wins, 7 losses

#### WIN — 2026-09-24
- **When:** [sector_energy] Predicted up, market/sector went up (pct=0.36876630780051656, score=6.779, sector=Energy).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_energy] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_energy] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-25
- **When:** [sector_energy] Predicted down, market/sector went down (pct=-0.8945648118909411, score=-9.556, sector=Energy).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_energy] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_energy] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-28
- **When:** [sector_energy] Predicted up but went flat (pct=0.0967078621991746, score=7.946, sector=Energy).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_energy] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_energy] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-10-01
- **When:** [sector_energy] Predicted down but went up (pct=1.9512207527470737, score=-2.413, sector=Energy).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_energy] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_energy] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-10-02
- **When:** [sector_energy] Predicted down but went up (pct=0.39872407808290156, score=-7.204, sector=Energy).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_energy] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_energy] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_financial` — 9 wins, 6 losses

#### LOSS — 2026-09-23
- **When:** [sector_financial] Predicted flat but went down (pct=-0.4744494984543213, score=1.027, sector=Financial).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_financial] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_financial] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-24
- **When:** [sector_financial] Predicted down but went flat (pct=-0.018339083356377905, score=-4.898, sector=Financial).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_financial] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_financial] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-25
- **When:** [sector_financial] Predicted flat but went up (pct=0.5684969378886295, score=-0.085, sector=Financial).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_financial] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_financial] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-10-01
- **When:** [sector_financial] Predicted down but went up (pct=0.11235497542949613, score=-5.156, sector=Financial).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_financial] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_financial] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-10-02
- **When:** [sector_financial] Predicted flat, market/sector went flat (pct=0.03741200543618817, score=2.893, sector=Financial).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_financial] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_financial] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_healthcare` — 6 wins, 9 losses

#### LOSS — 2026-09-23
- **When:** [sector_healthcare] Predicted up but went down (pct=-0.1242479509515837, score=0.563, sector=Healthcare).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_healthcare] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_healthcare] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-24
- **When:** [sector_healthcare] Predicted down but went up (pct=0.6338815438893031, score=-4.286, sector=Healthcare).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_healthcare] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_healthcare] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-25
- **When:** [sector_healthcare] Predicted up, market/sector went up (pct=0.4886100282054606, score=0.936, sector=Healthcare).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_healthcare] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_healthcare] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-10-01
- **When:** [sector_healthcare] Predicted down, market/sector went down (pct=-1.318133977460445, score=-3.878, sector=Healthcare).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_healthcare] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_healthcare] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-10-02
- **When:** [sector_healthcare] Predicted up but went flat (pct=-0.03008607260888141, score=2.597, sector=Healthcare).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_healthcare] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_healthcare] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_industrials` — 7 wins, 8 losses

#### WIN — 2026-09-23
- **When:** [sector_industrials] Predicted flat, market/sector went flat (pct=0.07060264677787309, score=0.68, sector=Industrials).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_industrials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_industrials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_industrials] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-24
- **When:** [sector_industrials] Predicted down, market/sector went down (pct=-0.7466221204531109, score=-4.289, sector=Industrials).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_industrials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_industrials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_industrials] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-25
- **When:** [sector_industrials] Predicted down but went up (pct=0.9476934356297928, score=-5.994, sector=Industrials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_industrials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-10-01
- **When:** [sector_industrials] Predicted down but went up (pct=0.9941332522357849, score=-0.378, sector=Industrials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_industrials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-10-02
- **When:** [sector_industrials] Predicted flat but went up (pct=0.7886633277207622, score=6.163, sector=Industrials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_industrials] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_real_estate` — 9 wins, 6 losses

#### WIN — 2026-09-23
- **When:** [sector_real_estate] Predicted down, market/sector went down (pct=-1.760976748797749, score=-0.428, sector=Real Estate).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_real_estate] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_real_estate] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-24
- **When:** [sector_real_estate] Predicted down, market/sector went down (pct=-0.4541076147611678, score=-9.97, sector=Real Estate).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_real_estate] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_real_estate] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-25
- **When:** [sector_real_estate] Predicted down, market/sector went down (pct=-0.2160867930148136, score=-4.619, sector=Real Estate).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_real_estate] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_real_estate] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-10-01
- **When:** [sector_real_estate] Predicted down, market/sector went down (pct=-0.5622086118166503, score=-5.858, sector=Real Estate).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_real_estate] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_real_estate] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-10-02
- **When:** [sector_real_estate] Predicted flat but went up (pct=0.2949826228725527, score=5.006, sector=Real Estate).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_real_estate] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_real_estate] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_technology` — 5 wins, 10 losses

#### LOSS — 2026-09-25
- **When:** [sector_technology] Predicted flat but went up (pct=0.8011902340932187, score=2.841, sector=Technology).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_technology] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_technology] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-28
- **When:** [sector_technology] Predicted down, market/sector went down (pct=-0.8865366359031568, score=-13.383, sector=Technology).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_technology] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_technology] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_technology] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-10-01
- **When:** [sector_technology] Predicted up, market/sector went up (pct=1.0523614603288545, score=5.037, sector=Technology).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_technology] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_technology] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_technology] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-10-02
- **When:** [sector_technology] Predicted flat but went up (pct=1.0312969870583055, score=7.741, sector=Technology).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_technology] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_technology] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-10-06
- **When:** [sector_technology] Predicted flat but went up (pct=0.5325274290659543, score=2.701, sector=Technology).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_technology] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_technology] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_utilities` — 6 wins, 9 losses

#### LOSS — 2026-09-23
- **When:** [sector_utilities] Predicted flat but went down (pct=-2.2380714481729846, score=0.717, sector=Utilities).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_utilities] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-24
- **When:** [sector_utilities] Predicted down, market/sector went down (pct=-0.9811305399960668, score=-5.818, sector=Utilities).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_utilities] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-25
- **When:** [sector_utilities] Predicted down but went up (pct=0.3810917399787561, score=-3.24, sector=Utilities).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_utilities] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-10-01
- **When:** [sector_utilities] Predicted down but went up (pct=0.608523546712969, score=-0.795, sector=Utilities).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_utilities] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-10-02
- **When:** [sector_utilities] Predicted flat but went up (pct=0.3528210397472531, score=0.943, sector=Utilities).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_utilities] Wrong if this hedge reduces direction accuracy over 10 runs.

## 4. Promoted standing rules (this cycle)

_No new promotions this cycle (candidates incomplete, not yet recurring, or already active)._

Full text lives in `02_lessons/active/`. Summaries also feed `mutable_policy.md`.

## 4b. Retired this cycle (topic got worse after the lesson went live)

- `a-utilities-xlu-call-is-built-after-a-stretch-of-risk-on-gro.md` (sector:Utilities): 80% → 14% (-66%) → `02_lessons/retired/`
- `a-sector-call-has-a-scheduled-8-30-et-macro-release-pending.md` (sector:Financial): 75% → 14% (-61%) → `02_lessons/retired/`
- `in-a-utilities-xlu-call-a-second-soft-inflation-print-has-al.md` (sector:Utilities): 75% → 14% (-61%) → `02_lessons/retired/`
- `a-consumer-cyclical-down-call-is-driven-by-a-genuinely-negat.md` (sector:Consumer Cyclical): 86% → 29% (-57%) → `02_lessons/retired/`
- `a-mega-cap-duration-heavy-consumer-cyclical-etf-xly-like-amz.md` (sector:Consumer Cyclical): 71% → 14% (-57%) → `02_lessons/retired/`
- `when-the-pre-fetched-commodity-tape-conflicts-with-live-sour.md` (sector:Energy): 71% → 14% (-57%) → `02_lessons/retired/`
- `a-sector-call-has-a-decisively-negative-fundamental-spine-fr.md` (sector:Consumer Cyclical): 83% → 29% (-55%) → `02_lessons/retired/`
- `a-utility-defensive-sector-call-is-built-on-a-carried-defens.md` (sector:Utilities): 67% → 14% (-52%) → `02_lessons/retired/`
- `sector-prediction-made-when-the-sector-s-dominant-commodity.md` (sector:Energy): 67% → 14% (-52%) → `02_lessons/retired/`
- `a-sector-call-has-a-scheduled-8-30-et-high-impact-macro-rele.md` (sector:Financial): 60% → 14% (-46%) → `02_lessons/retired/`

## 4c. Numeric factor weights (engine_policy.json)

These are applied by `compute_scores` / `compute_sector_scores` regardless of what the LLM writes — the part of learning that cannot be ignored.

General (B0–B7 LLM components; multiplier applied by compute_scores):
- B0_ASIA: n=21 sign-hit=0.57 → ×1.0
- B0_EUROPE: n=13 sign-hit=0.85 → ×1.25
- B1_CATALYSTS: n=26 sign-hit=0.77 → ×1.25
- B2_BONDS: n=36 sign-hit=0.36 → ×0.0
- B3_FEDPATH: n=31 sign-hit=0.45 → ×0.5
- B4_VIX: n=13 sign-hit=0.54 → ×0.5
- B5_SENTIMENT: n=22 sign-hit=0.32 → ×0.0
- B6_FUTURES: n=26 sign-hit=0.77 → ×1.25
- B7_OIL_DOLLAR: n=35 sign-hit=0.63 → ×1.0
Sectors (pooled S0–S4; per-sector overrides in engine_policy.json):
- S0_SHARED_MACRO: n=188 sign-hit=0.60 → ×1.0
- S1_SECTOR_FACTORS: n=212 sign-hit=0.57 → ×1.0
- S2_BREADTH: n=161 sign-hit=0.60 → ×1.0
- S3_FLOWS_POSITIONING: n=117 sign-hit=0.51 → ×0.5
- S4_ETF_TAPE: n=173 sign-hit=0.61 → ×1.0
Last change: hold

Progress vs baselines: `03_scoreboard/IMPROVEMENT_TRACKER.md`.

## 5. How these learnings affect daily workflows

### General market predict (`run_predict` / daily pipeline)
- `compute_scores` applies `engine_policy.json` multipliers to B0–B7 and anchors on the pre-open tape (ES/NQ/Europe/VIX) — numeric learning the LLM cannot skip.
- Loads `mutable_policy.md` + its own topic's efficacy-ranked lessons (`lesson_select`) via `memory.prediction_context()`.
- Must answer methodology checklist in MEMORY_CONFIRM.
- Ops lessons (missing predict file) change grading, not B0–B7 math.
- Macro / geo / regime lessons change how Channel-2 evidence is weighted in narrative.

### Per-sector predict (`run_sector_predict` / sector daily)
- `compute_sector_scores` applies per-sector S0–S4 multipliers, a beta-weighted futures anchor and a carry from the general call.
- Loads the **same** `mutable_policy.md` via `sector_memory` plus only this sector's active lessons (`lesson_select`).
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
| `01_daily/2026-10-05_learnings.md` | Dated copy |
| `00_grounding/mutable_policy.md` | Injected into general + sector predict |
| `02_lessons/hypotheses/*` | Per-event experiments |
| `02_lessons/active/*` | Standing rules |
| `00_grounding/weather_rules_proposals.json` | Optional weather threshold deltas |
