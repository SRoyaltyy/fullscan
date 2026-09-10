# Learnings report — 2026-09-10

Generated: **2026-09-10T18:41:20.852490-04:00** by `src/learn_cycle.py`.

This is the human-readable digest of what the bot **actually learned** this cycle: graded evidence, hypotheses (wins and losses), promoted standing rules, and **how that changes every daily workflow**.

Machine policy file (injected into predicts): `00_grounding/mutable_policy.md`.

---

## 1. Snapshot

| Item | Value |
|------|-------|
| Graded runs mined | 179 |
| Hypotheses written | 180 (wins=80, losses=100) |
| News hypotheses | 1 |
| Lessons promoted to active | 12 |
| Active lesson files now | 181 |

## 2. Accuracy by topic (evidence this cycle learned from)

| Topic | Direction HIT% | hits/n | Read |
|-------|----------------|--------|------|
| general | 40% | 6/15 | weak — priority |
| sector:Basic Materials | 53% | 8/15 | weak — priority |
| sector:Communication Services | 27% | 4/15 | weak — priority |
| sector:Consumer Cyclical | 67% | 10/15 | ok |
| sector:Consumer Defensive | 40% | 6/15 | weak — priority |
| sector:Energy | 47% | 7/15 | weak — priority |
| sector:Financial | 40% | 6/15 | weak — priority |
| sector:Healthcare | 64% | 9/14 | ok |
| sector:Industrials | 27% | 4/15 | weak — priority |
| sector:Real Estate | 47% | 7/15 | weak — priority |
| sector:Technology | 40% | 6/15 | weak — priority |
| sector:Utilities | 40% | 6/15 | weak — priority |

## 3. What we learned (by scope)

Each scope lists recent win and loss hypotheses: the **counterfactual ask**, the **experiment** to run next, and the **policy candidate** (do instead).

### `general` — 6 wins, 9 losses

#### LOSS — 2026-09-03
- **When:** [general] Predicted flat but went up (pct=1.06, score=-0.9, sector=).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [general] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [general] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [general] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-04
- **When:** [general] Predicted up but went down (pct=-0.38, score=2.25, sector=).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [general] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [general] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [general] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-08
- **When:** [general] Predicted down, market/sector went down (pct=-0.58, score=-11.475, sector=).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [general] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [general] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-09
- **When:** [general] Predicted down, market/sector went down (pct=-0.48, score=-13.95, sector=).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [general] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [general] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-10
- **When:** [general] Predicted down, market/sector went down (pct=-0.58, score=-13.275, sector=).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [general] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [general] Wrong if milder bands hurt direction accuracy over 10 runs.

### `news` — 1 wins, 0 losses

#### WIN — news
- **When:** [news] summary={"n_suggestions": 950, "ever_profitable": {"n": 950, "wins": 941, "win_rate": 99.1}, "close_1d": {"n": 914, "wins": 507, "win_rate": 55.5, "avg": 0.21}, "close_3d": {"n": 912, "wins": 563, "win_rate": 61.7, "avg": 0.96}, "close_5d": {"n": 867, "wins": 533, "win_rate": 61.5, "avg": 1.33}, "close_10d"
- **Ask:** Which event families drive ever-profitable vs 1d close?
- **Experiment:** [news] Track event-level 1d close win rate daily in learn_cycle.
- **Do instead:** [news] Rank event families by 1d close, not ever-touch MFE.
- **Wrong if:** [news] Wrong if ever-touch is the better trading objective for you.

### `sector_basic_materials` — 8 wins, 7 losses

#### LOSS — 2026-09-03
- **When:** [sector_basic_materials] Predicted up but went down (pct=-0.6232329108589174, score=3.15, sector=Basic Materials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_basic_materials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_basic_materials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-04
- **When:** [sector_basic_materials] Predicted down, market/sector went down (pct=-0.3420758434616977, score=-4.95, sector=Basic Materials).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_basic_materials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_basic_materials] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-08
- **When:** [sector_basic_materials] Predicted flat but went down (pct=-0.9534706580738517, score=1.125, sector=Basic Materials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_basic_materials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_basic_materials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-09
- **When:** [sector_basic_materials] Predicted down, market/sector went down (pct=-1.058912690801883, score=-4.5, sector=Basic Materials).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_basic_materials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_basic_materials] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-10
- **When:** [sector_basic_materials] Predicted down, market/sector went down (pct=-1.2259215325893469, score=-9.0, sector=Basic Materials).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_basic_materials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_basic_materials] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_communication_services` — 4 wins, 11 losses

#### LOSS — 2026-09-03
- **When:** [sector_communication_services] Predicted flat but went up (pct=0.8539397794954384, score=0.0, sector=Communication Services).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_communication_services] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_communication_services] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-04
- **When:** [sector_communication_services] Predicted flat but went down (pct=-1.1906848710744655, score=0.0, sector=Communication Services).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_communication_services] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_communication_services] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-08
- **When:** [sector_communication_services] Predicted down, market/sector went down (pct=-0.4552371166540725, score=-6.75, sector=Communication Services).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_communication_services] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_communication_services] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_communication_services] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-09
- **When:** [sector_communication_services] Predicted flat but went down (pct=-0.6187184655502942, score=0.9, sector=Communication Services).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_communication_services] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_communication_services] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-10
- **When:** [sector_communication_services] Predicted down but went up (pct=0.6045277974159324, score=-3.6, sector=Communication Services).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_communication_services] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_communication_services] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_consumer_cyclical` — 10 wins, 5 losses

#### LOSS — 2026-09-03
- **When:** [sector_consumer_cyclical] Predicted down but went up (pct=1.3929988382543224, score=-2.7, sector=Consumer Cyclical).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_cyclical] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_cyclical] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-04
- **When:** [sector_consumer_cyclical] Predicted down, market/sector went down (pct=-1.3309251541716138, score=-4.5, sector=Consumer Cyclical).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_cyclical] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_cyclical] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-08
- **When:** [sector_consumer_cyclical] Predicted down, market/sector went down (pct=-0.8006315977894363, score=-7.5, sector=Consumer Cyclical).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_cyclical] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_cyclical] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-09
- **When:** [sector_consumer_cyclical] Predicted down, market/sector went down (pct=-1.3422219562854942, score=-7.5, sector=Consumer Cyclical).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_cyclical] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_cyclical] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-10
- **When:** [sector_consumer_cyclical] Predicted down, market/sector went down (pct=-0.44460252896181274, score=-7.5, sector=Consumer Cyclical).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_cyclical] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_cyclical] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_consumer_defensive` — 6 wins, 9 losses

#### LOSS — 2026-09-03
- **When:** [sector_consumer_defensive] Predicted up but went down (pct=-0.3156747888692357, score=1.35, sector=Consumer Defensive).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_defensive] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_defensive] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-04
- **When:** [sector_consumer_defensive] Predicted down, market/sector went down (pct=-0.7975607414239305, score=-4.95, sector=Consumer Defensive).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_defensive] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_defensive] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-08
- **When:** [sector_consumer_defensive] Predicted flat but went down (pct=-0.6621011774235575, score=-0.675, sector=Consumer Defensive).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_defensive] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_defensive] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-09
- **When:** [sector_consumer_defensive] Predicted down, market/sector went down (pct=-1.1544794454459661, score=-2.925, sector=Consumer Defensive).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_defensive] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_defensive] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-10
- **When:** [sector_consumer_defensive] Predicted down but went flat (pct=0.04815567087683714, score=-2.925, sector=Consumer Defensive).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_defensive] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_defensive] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_energy` — 7 wins, 8 losses

#### LOSS — 2026-09-03
- **When:** [sector_energy] Predicted flat but went down (pct=-0.7373206433021195, score=-2.7, sector=Energy).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_energy] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_energy] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-04
- **When:** [sector_energy] Predicted flat but went down (pct=-0.866612757948082, score=-3.15, sector=Energy).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_energy] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_energy] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-08
- **When:** [sector_energy] Predicted up, market/sector went up (pct=1.108334548129264, score=8.5, sector=Energy).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_energy] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_energy] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-09
- **When:** [sector_energy] Predicted up, market/sector went up (pct=0.83372077121322, score=8.5, sector=Energy).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_energy] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_energy] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-10
- **When:** [sector_energy] Predicted up but went down (pct=-0.5818362695191537, score=8.5, sector=Energy).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_energy] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_energy] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_financial` — 6 wins, 9 losses

#### LOSS — 2026-09-03
- **When:** [sector_financial] Predicted flat but went up (pct=1.560876739959438, score=0.0, sector=Financial).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_financial] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_financial] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-04
- **When:** [sector_financial] Predicted up but went down (pct=-0.7855240580300404, score=5.4, sector=Financial).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_financial] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_financial] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-08
- **When:** [sector_financial] Predicted down, market/sector went down (pct=-1.3769350397089597, score=-2.925, sector=Financial).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_financial] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_financial] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-09
- **When:** [sector_financial] Predicted down, market/sector went down (pct=-0.4188444449652051, score=-6.075, sector=Financial).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_financial] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_financial] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-10
- **When:** [sector_financial] Predicted down, market/sector went down (pct=-0.33298709574722807, score=-6.075, sector=Financial).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_financial] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_financial] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_healthcare` — 9 wins, 5 losses

#### LOSS — 2026-09-03
- **When:** [sector_healthcare] Predicted flat but went up (pct=0.17924114718921302, score=0.0, sector=Healthcare).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_healthcare] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_healthcare] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-04
- **When:** [sector_healthcare] Predicted up but went down (pct=-1.0446713701831145, score=1.35, sector=Healthcare).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_healthcare] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_healthcare] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-08
- **When:** [sector_healthcare] Predicted flat but went down (pct=-2.5196804562987674, score=0.9, sector=Healthcare).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_healthcare] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_healthcare] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-09
- **When:** [sector_healthcare] Predicted down, market/sector went down (pct=-0.3290869596655921, score=-6.525, sector=Healthcare).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_healthcare] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_healthcare] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-10
- **When:** [sector_healthcare] Predicted down, market/sector went down (pct=-0.5522860840633026, score=-6.525, sector=Healthcare).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_healthcare] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_healthcare] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_industrials` — 4 wins, 11 losses

#### LOSS — 2026-09-03
- **When:** [sector_industrials] Predicted flat but went up (pct=1.0302111308442496, score=0.0, sector=Industrials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_industrials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-04
- **When:** [sector_industrials] Predicted down but went up (pct=0.4067407904430498, score=-1.35, sector=Industrials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_industrials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-08
- **When:** [sector_industrials] Predicted flat but went down (pct=-0.48496952290494333, score=-3.6, sector=Industrials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_industrials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-09
- **When:** [sector_industrials] Predicted flat but went down (pct=-1.5078574191160432, score=-4.05, sector=Industrials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_industrials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-10
- **When:** [sector_industrials] Predicted down, market/sector went down (pct=-0.7218058576378694, score=-4.05, sector=Industrials).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_industrials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_industrials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_industrials] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_real_estate` — 7 wins, 8 losses

#### LOSS — 2026-09-03
- **When:** [sector_real_estate] Predicted flat but went up (pct=1.1891160832540937, score=-0.45, sector=Real Estate).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_real_estate] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_real_estate] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-04
- **When:** [sector_real_estate] Predicted flat but went down (pct=-0.7231631521451232, score=0.0, sector=Real Estate).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_real_estate] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_real_estate] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-08
- **When:** [sector_real_estate] Predicted down but went flat (pct=-0.06828768287838738, score=-4.5, sector=Real Estate).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_real_estate] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_real_estate] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-09
- **When:** [sector_real_estate] Predicted down, market/sector went down (pct=-1.1161769053195658, score=-6.75, sector=Real Estate).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_real_estate] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_real_estate] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-10
- **When:** [sector_real_estate] Predicted down, market/sector went down (pct=-0.8293034130775867, score=-6.75, sector=Real Estate).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_real_estate] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_real_estate] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_technology` — 6 wins, 9 losses

#### LOSS — 2026-08-28
- **When:** [sector_technology] Predicted up but went down (pct=-1.5481672018960002, score=2.7, sector=Technology).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_technology] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_technology] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-03
- **When:** [sector_technology] Predicted flat but went up (pct=1.2908469708063475, score=-1.35, sector=Technology).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_technology] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_technology] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-04
- **When:** [sector_technology] Predicted flat but went up (pct=0.7151700932003013, score=-0.45, sector=Technology).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_technology] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_technology] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-09
- **When:** [sector_technology] Predicted flat, market/sector went flat (pct=0.0, score=0.0, sector=Technology).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_technology] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_technology] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_technology] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-10
- **When:** [sector_technology] Predicted flat but went down (pct=-1.410546636162624, score=-0.45, sector=Technology).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_technology] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_technology] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_utilities` — 6 wins, 9 losses

#### WIN — 2026-08-28
- **When:** [sector_utilities] Predicted down, market/sector went down (pct=-1.0421509026379394, score=-4.95, sector=Utilities).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_utilities] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-03
- **When:** [sector_utilities] Predicted flat but went up (pct=0.8436855537846455, score=0.0, sector=Utilities).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_utilities] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-04
- **When:** [sector_utilities] Predicted flat but went up (pct=0.11620509685412728, score=0.0, sector=Utilities).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_utilities] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-09
- **When:** [sector_utilities] Predicted flat but went down (pct=-1.1737678418304531, score=0.45, sector=Utilities).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_utilities] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-10
- **When:** [sector_utilities] Predicted down, market/sector went down (pct=-0.9781047563519718, score=-4.725, sector=Utilities).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_utilities] Wrong if milder bands hurt direction accuracy over 10 runs.

## 4. Promoted standing rules (this cycle)

- `a-fresh-overnight-kinetic-oil-supply-escalation-iran-hormuz.md`
- `a-sector-etf-gaps-down-1-at-the-open-on-a-risk-off-commodity.md`
- `a-concentrated-sector-etf-two-mega-cap-names-35-combined-wei.md`
- `a-live-macro-shock-oil-geopolitical-is-present-at-the-open-t.md`
- `a-sector-specific-negative-cluster-e-g-packaged-food-margin.md`
- `a-sector-etf-gaps-up-1-at-the-open-on-the-back-of-a-multi-da.md`
- `a-live-macro-shock-oil-geopolitical-risk-off-is-present-at-t.md`
- `a-market-wide-shock-oil-rates-risk-off-is-present-at-the-ope.md`
- `a-sector-etf-is-a-deep-multi-horizon-relative-laggard-1m-rel.md`
- `a-rate-sensitive-bond-proxy-sector-xlre-is-predicted-down-mi.md`
- `a-live-macro-risk-off-shock-oil-geopolitical-escalation-bren.md`
- `a-rate-sensitive-bond-proxy-sector-xlu-xlre-type-is-predicte.md`

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
| `01_daily/2026-09-10_learnings.md` | Dated copy |
| `00_grounding/mutable_policy.md` | Injected into general + sector predict |
| `02_lessons/hypotheses/*` | Per-event experiments |
| `02_lessons/active/*` | Standing rules |
| `00_grounding/weather_rules_proposals.json` | Optional weather threshold deltas |
