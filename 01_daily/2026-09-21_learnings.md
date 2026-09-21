# Learnings report — 2026-09-21

Generated: **2026-09-21T16:18:36.323059-04:00** by `src/learn_cycle.py`.

This is the human-readable digest of what the bot **actually learned** this cycle: graded evidence, hypotheses (wins and losses), promoted standing rules, and **how that changes every daily workflow**.

Machine policy file (injected into predicts): `00_grounding/mutable_policy.md`.

---

## 1. Snapshot

| Item | Value |
|------|-------|
| Graded runs mined | 180 |
| Hypotheses written | 181 (wins=73, losses=108) |
| News hypotheses | 1 |
| Lessons promoted to active | 2 |
| Lessons retired (efficacy-gated) | 10 |
| Active lesson files now | 202 |
| Engine policy version | 16 |

## 2. Accuracy by topic (evidence this cycle learned from)

| Topic | Direction HIT% | hits/n | Read |
|-------|----------------|--------|------|
| general | 60% | 9/15 | ok |
| sector:Basic Materials | 33% | 5/15 | weak — priority |
| sector:Communication Services | 20% | 3/15 | weak — priority |
| sector:Consumer Cyclical | 40% | 6/15 | weak — priority |
| sector:Consumer Defensive | 40% | 6/15 | weak — priority |
| sector:Energy | 53% | 8/15 | weak — priority |
| sector:Financial | 53% | 8/15 | weak — priority |
| sector:Healthcare | 47% | 7/15 | weak — priority |
| sector:Industrials | 40% | 6/15 | weak — priority |
| sector:Real Estate | 33% | 5/15 | weak — priority |
| sector:Technology | 33% | 5/15 | weak — priority |
| sector:Utilities | 33% | 5/15 | weak — priority |

## 3. What we learned (by scope)

Each scope lists recent win and loss hypotheses: the **counterfactual ask**, the **experiment** to run next, and the **policy candidate** (do instead).

### `general` — 9 wins, 6 losses

#### WIN — 2026-09-15
- **When:** [general] Predicted down, market/sector went down (pct=-0.45, score=-6.215, sector=).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [general] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [general] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-16
- **When:** [general] Predicted up but went down (pct=-0.45, score=5.297, sector=).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [general] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [general] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [general] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-17
- **When:** [general] Predicted up, market/sector went up (pct=1.14, score=7.383, sector=).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [general] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [general] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-18
- **When:** [general] Predicted up, market/sector went up (pct=0.17, score=4.861, sector=).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [general] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [general] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-21
- **When:** [general] Predicted up, market/sector went up (pct=1.49, score=12.871, sector=).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [general] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [general] Wrong if milder bands hurt direction accuracy over 10 runs.

### `news` — 0 wins, 1 losses

#### LOSS — news
- **When:** [news] Global 1d close win rate 52.0% (n=1379).
- **Ask:** Entry timing, side mix, or event taxonomy noise?
- **Experiment:** [news] Raise min net weight to map a ticker; drop weak edges.
- **Do instead:** [news] Only emit actions with |net| above a higher floor.
- **Wrong if:** [news] Wrong if higher floor reduces 1d win rate further.

### `sector_basic_materials` — 5 wins, 10 losses

#### LOSS — 2026-09-15
- **When:** [sector_basic_materials] Predicted down but went up (pct=0.47533740501317645, score=-7.604, sector=Basic Materials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_basic_materials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_basic_materials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-16
- **When:** [sector_basic_materials] Predicted flat but went down (pct=-0.7293493696500342, score=9.667, sector=Basic Materials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_basic_materials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_basic_materials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-17
- **When:** [sector_basic_materials] Predicted flat but went up (pct=0.6949929902287488, score=4.936, sector=Basic Materials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_basic_materials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_basic_materials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-18
- **When:** [sector_basic_materials] Predicted flat but went down (pct=-1.4198332064776609, score=6.18, sector=Basic Materials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_basic_materials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_basic_materials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-21
- **When:** [sector_basic_materials] Predicted flat but went down (pct=-0.5001000032126224, score=1.904, sector=Basic Materials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_basic_materials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_basic_materials] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_communication_services` — 3 wins, 12 losses

#### LOSS — 2026-09-15
- **When:** [sector_communication_services] Predicted up but went down (pct=-0.9037984863869974, score=0.396, sector=Communication Services).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_communication_services] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_communication_services] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-16
- **When:** [sector_communication_services] Predicted up but went down (pct=-0.903270008176027, score=9.244, sector=Communication Services).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_communication_services] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_communication_services] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-17
- **When:** [sector_communication_services] Predicted up but went down (pct=-0.575222589273372, score=5.275, sector=Communication Services).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_communication_services] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_communication_services] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-18
- **When:** [sector_communication_services] Predicted up but went down (pct=-1.3707173444083898, score=3.549, sector=Communication Services).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_communication_services] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_communication_services] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-21
- **When:** [sector_communication_services] Predicted up, market/sector went up (pct=3.564664438827414, score=12.219, sector=Communication Services).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_communication_services] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_communication_services] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_communication_services] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_consumer_cyclical` — 6 wins, 9 losses

#### WIN — 2026-09-15
- **When:** [sector_consumer_cyclical] Predicted down, market/sector went down (pct=-1.745681211643868, score=-4.704, sector=Consumer Cyclical).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_cyclical] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_cyclical] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-16
- **When:** [sector_consumer_cyclical] Predicted flat but went down (pct=-0.6313103946443466, score=2.692, sector=Consumer Cyclical).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_cyclical] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_cyclical] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-17
- **When:** [sector_consumer_cyclical] Predicted flat but went up (pct=1.0982021066629155, score=7.655, sector=Consumer Cyclical).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_cyclical] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_cyclical] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-18
- **When:** [sector_consumer_cyclical] Predicted flat but went down (pct=-0.3231893458336965, score=4.013, sector=Consumer Cyclical).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_cyclical] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_cyclical] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-21
- **When:** [sector_consumer_cyclical] Predicted flat but went up (pct=1.062776112896624, score=12.338, sector=Consumer Cyclical).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_cyclical] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_cyclical] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_consumer_defensive` — 6 wins, 9 losses

#### LOSS — 2026-09-15
- **When:** [sector_consumer_defensive] Predicted flat but went down (pct=-0.8173357343965737, score=-8.086, sector=Consumer Defensive).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_defensive] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_defensive] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-16
- **When:** [sector_consumer_defensive] Predicted flat but went down (pct=-0.4777278273520813, score=-0.417, sector=Consumer Defensive).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_defensive] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_defensive] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-17
- **When:** [sector_consumer_defensive] Predicted up, market/sector went up (pct=0.19200291515559798, score=0.765, sector=Consumer Defensive).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_consumer_defensive] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_consumer_defensive] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-18
- **When:** [sector_consumer_defensive] Predicted up but went down (pct=-0.8264400882337819, score=-0.125, sector=Consumer Defensive).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_defensive] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_defensive] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-21
- **When:** [sector_consumer_defensive] Predicted flat but went down (pct=-1.086958324526055, score=-1.24, sector=Consumer Defensive).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_consumer_defensive] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_consumer_defensive] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_energy` — 8 wins, 7 losses

#### WIN — 2026-09-15
- **When:** [sector_energy] Predicted up, market/sector went up (pct=2.1695359559313454, score=4.023, sector=Energy).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_energy] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_energy] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-16
- **When:** [sector_energy] Predicted down, market/sector went down (pct=-2.8818466814564014, score=-2.88, sector=Energy).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_energy] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_energy] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-17
- **When:** [sector_energy] Predicted up, market/sector went up (pct=0.7028027271839044, score=0.202, sector=Energy).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_energy] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_energy] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-18
- **When:** [sector_energy] Predicted down, market/sector went down (pct=-0.26365662141604185, score=-3.733, sector=Energy).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_energy] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_energy] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-21
- **When:** [sector_energy] Predicted down, market/sector went down (pct=-2.907788839800507, score=-6.269, sector=Energy).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_energy] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_energy] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_financial` — 8 wins, 7 losses

#### WIN — 2026-09-15
- **When:** [sector_financial] Predicted down, market/sector went down (pct=-0.3156238979986181, score=-4.063, sector=Financial).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_financial] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_financial] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-16
- **When:** [sector_financial] Predicted flat but went down (pct=-1.6182905780799728, score=2.998, sector=Financial).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_financial] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_financial] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-17
- **When:** [sector_financial] Predicted flat, market/sector went flat (pct=-0.08939609652732772, score=6.147, sector=Financial).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_financial] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_financial] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-18
- **When:** [sector_financial] Predicted flat, market/sector went flat (pct=-0.03579179917926334, score=3.519, sector=Financial).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_financial] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_financial] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-21
- **When:** [sector_financial] Predicted flat, market/sector went flat (pct=0.035804614294909776, score=5.194, sector=Financial).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_financial] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_financial] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_financial] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_healthcare` — 7 wins, 8 losses

#### LOSS — 2026-09-15
- **When:** [sector_healthcare] Predicted up but went flat (pct=-0.05364908369038801, score=0.93, sector=Healthcare).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_healthcare] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_healthcare] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-16
- **When:** [sector_healthcare] Predicted up but went flat (pct=0.06560933314379014, score=2.995, sector=Healthcare).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_healthcare] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_healthcare] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-17
- **When:** [sector_healthcare] Predicted up, market/sector went up (pct=0.619892268968325, score=4.847, sector=Healthcare).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_healthcare] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_healthcare] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-18
- **When:** [sector_healthcare] Predicted up but went down (pct=-0.24879934542948456, score=2.404, sector=Healthcare).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_healthcare] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_healthcare] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_healthcare] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-21
- **When:** [sector_healthcare] Predicted up, market/sector went up (pct=0.35037492726952557, score=3.016, sector=Healthcare).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_healthcare] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_healthcare] Wrong if milder bands hurt direction accuracy over 10 runs.

### `sector_industrials` — 6 wins, 9 losses

#### WIN — 2026-09-15
- **When:** [sector_industrials] Predicted down, market/sector went down (pct=-0.6355479425731447, score=-5.881, sector=Industrials).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_industrials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_industrials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_industrials] Wrong if milder bands hurt direction accuracy over 10 runs.

#### WIN — 2026-09-16
- **When:** [sector_industrials] Predicted flat, market/sector went flat (pct=-0.08291346436943847, score=7.288, sector=Industrials).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_industrials] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_industrials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_industrials] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-17
- **When:** [sector_industrials] Predicted flat but went up (pct=0.17781268509906578, score=6.262, sector=Industrials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_industrials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-18
- **When:** [sector_industrials] Predicted flat but went up (pct=0.4378471789928007, score=4.138, sector=Industrials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_industrials] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-21
- **When:** [sector_industrials] Predicted flat but went up (pct=0.11193074604196607, score=10.19, sector=Industrials).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_industrials] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_industrials] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_real_estate` — 5 wins, 10 losses

#### WIN — 2026-09-15
- **When:** [sector_real_estate] Predicted down, market/sector went down (pct=-0.11595370663048943, score=-8.5, sector=Real Estate).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_real_estate] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_real_estate] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-16
- **When:** [sector_real_estate] Predicted flat but went down (pct=-0.6036645539248653, score=2.52, sector=Real Estate).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_real_estate] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_real_estate] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-17
- **When:** [sector_real_estate] Predicted flat but went up (pct=0.3036609419477143, score=4.899, sector=Real Estate).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_real_estate] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_real_estate] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-18
- **When:** [sector_real_estate] Predicted flat but went down (pct=-0.9548203552039447, score=2.368, sector=Real Estate).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_real_estate] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_real_estate] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-21
- **When:** [sector_real_estate] Predicted flat but went up (pct=0.14108011994635739, score=3.17, sector=Real Estate).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_real_estate] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_real_estate] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_technology` — 5 wins, 10 losses

#### WIN — 2026-09-14
- **When:** [sector_technology] Predicted down, market/sector went down (pct=-1.8063619239750195, score=-19.586, sector=Technology).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_technology] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_technology] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_technology] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-16
- **When:** [sector_technology] Predicted flat but went up (pct=0.10340000921804648, score=10.23, sector=Technology).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_technology] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_technology] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-17
- **When:** [sector_technology] Predicted flat but went up (pct=2.2454221971794253, score=15.669, sector=Technology).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_technology] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_technology] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-18
- **When:** [sector_technology] Predicted flat but went up (pct=0.818892143419303, score=11.236, sector=Technology).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_technology] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_technology] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-21
- **When:** [sector_technology] Predicted flat but went up (pct=2.7637078405546633, score=17.116, sector=Technology).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_technology] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_technology] Wrong if this hedge reduces direction accuracy over 10 runs.

### `sector_utilities` — 5 wins, 10 losses

#### WIN — 2026-09-14
- **When:** [sector_utilities] Predicted down, market/sector went down (pct=-1.3446560581065081, score=-7.559, sector=Utilities).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_utilities] Wrong if milder bands hurt direction accuracy over 10 runs.

#### LOSS — 2026-09-16
- **When:** [sector_utilities] Predicted down but went flat (pct=0.0, score=-0.468, sector=Utilities).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_utilities] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-17
- **When:** [sector_utilities] Predicted flat but went up (pct=0.8954475668379924, score=1.751, sector=Utilities).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_utilities] Wrong if this hedge reduces direction accuracy over 10 runs.

#### LOSS — 2026-09-18
- **When:** [sector_utilities] Predicted flat but went down (pct=-1.4152078964327464, score=1.496, sector=Utilities).
- **Ask:** Dominant factor family? Regime misread vs sector-specific shock? Shared macro S0 wrong or sector factors S1 wrong?
- **Experiment:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **Do instead:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **Wrong if:** [sector_utilities] Wrong if this hedge reduces direction accuracy over 10 runs.

#### WIN — 2026-09-21
- **When:** [sector_utilities] Predicted down, market/sector went down (pct=-1.0948831263960512, score=-1.439, sector=Utilities).
- **Ask:** Could magnitude/conviction have been better? Double-count in factors? Missing confirming source?
- **Experiment:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **Do instead:** [sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **Wrong if:** [sector_utilities] Wrong if milder bands hurt direction accuracy over 10 runs.

## 4. Promoted standing rules (this cycle)

- `a-low-beta-defensive-sector-etf-xlp-xlu-xlv-like-posts-a-net.md`
- `a-sector-card-whose-leading-s0-s4-components-are-unanimously.md`

Full text lives in `02_lessons/active/`. Summaries also feed `mutable_policy.md`.

## 4b. Retired this cycle (topic got worse after the lesson went live)

- `a-utilities-xlu-call-is-built-after-a-stretch-of-risk-on-gro.md` (sector:Utilities): 80% → 14% (-66%) → `02_lessons/retired/`
- `a-sector-call-has-a-scheduled-8-30-et-macro-release-pending.md` (sector:Financial): 75% → 14% (-61%) → `02_lessons/retired/`
- `in-a-utilities-xlu-call-a-second-soft-inflation-print-has-al.md` (sector:Utilities): 75% → 14% (-61%) → `02_lessons/retired/`
- `a-consumer-cyclical-down-call-is-driven-by-a-genuinely-negat.md` (sector:Consumer Cyclical): 86% → 29% (-57%) → `02_lessons/retired/`
- `when-the-pre-fetched-commodity-tape-conflicts-with-live-sour.md` (sector:Energy): 71% → 14% (-57%) → `02_lessons/retired/`
- `a-sector-call-has-a-decisively-negative-fundamental-spine-fr.md` (sector:Consumer Cyclical): 83% → 29% (-55%) → `02_lessons/retired/`
- `a-utility-defensive-sector-call-is-built-on-a-carried-defens.md` (sector:Utilities): 67% → 14% (-52%) → `02_lessons/retired/`
- `sector-prediction-made-when-the-sector-s-dominant-commodity.md` (sector:Energy): 67% → 14% (-52%) → `02_lessons/retired/`
- `a-sector-call-has-a-scheduled-8-30-et-high-impact-macro-rele.md` (sector:Financial): 60% → 14% (-46%) → `02_lessons/retired/`
- `a-fresh-top-holding-legal-regulatory-catalyst-e-g-a-trial-op.md` (sector:Communication Services): 43% → 0% (-43%) → `02_lessons/retired/`

## 4c. Numeric factor weights (engine_policy.json)

These are applied by `compute_scores` / `compute_sector_scores` regardless of what the LLM writes — the part of learning that cannot be ignored.

General (B0–B7 LLM components; multiplier applied by compute_scores):
- B0_ASIA: n=18 sign-hit=0.50 → ×0.5
- B0_EUROPE: n=11 sign-hit=0.91 → ×1.25
- B1_CATALYSTS: n=24 sign-hit=0.79 → ×1.25
- B2_BONDS: n=29 sign-hit=0.38 → ×0.0
- B3_FEDPATH: n=28 sign-hit=0.46 → ×0.5
- B4_VIX: n=11 sign-hit=0.64 → ×1.0
- B5_SENTIMENT: n=21 sign-hit=0.33 → ×0.0
- B6_FUTURES: n=21 sign-hit=0.76 → ×1.25
- B7_OIL_DOLLAR: n=27 sign-hit=0.70 → ×1.25
Sectors (pooled S0–S4; per-sector overrides in engine_policy.json):
- S0_SHARED_MACRO: n=149 sign-hit=0.60 → ×1.0
- S1_SECTOR_FACTORS: n=175 sign-hit=0.57 → ×1.0
- S2_BREADTH: n=134 sign-hit=0.62 → ×1.0
- S3_FLOWS_POSITIONING: n=95 sign-hit=0.49 → ×0.5
- S4_ETF_TAPE: n=141 sign-hit=0.62 → ×1.0
Last change: Basic Materials.S2_BREADTH: 0.5 -> 1.0 (n=12, hit=0.583); Consumer Defensive.S0_SHARED_MACRO: 1.0 -> 1.25 (n=12, hit=0.667); Healthcare.S0_SHARED_MACRO: 0.5 -> 0.0 (n=14, hit=0.429); Real Estate.S4_ETF_TAPE: 1.0 -> 0.5 (n=15, hit=0.533)

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
| `01_daily/2026-09-21_learnings.md` | Dated copy |
| `00_grounding/mutable_policy.md` | Injected into general + sector predict |
| `02_lessons/hypotheses/*` | Per-event experiments |
| `02_lessons/active/*` | Standing rules |
| `00_grounding/weather_rules_proposals.json` | Optional weather threshold deltas |
