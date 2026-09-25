# Sector Prediction — Basic Materials — 2026-09-25

- news_mode: **on**
- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **1.377** (mult 0.85)
- regime: mixed
- divergence_flagged: **True**
- engine: v2 · tape_anchor **2.772** (ES +0.28%, HG +0.66%, GC +0.90%, DX -0.02%) · index_carry **0.676** (general 2.706) · llm_overlay **-2.072** (raw -2.072)

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-24):
  1d: XLB -1.19% | SPY -0.08% | rel -1.11%
  3d: XLB -0.16% | SPY +0.72% | rel -0.88%
  1w: XLB -0.90% | SPY +1.99% | rel -2.89%
  1m: XLB -6.85% | SPY +0.74% | rel -7.59%
```

I have sufficient Channel 2 coverage. Writing the analysis.

---

MEMORY_CONFIRM: Sector **Basic Materials / XLB only**. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.2 mag=0.2 (n=10); last-30 dir=0.444 mag=0.37 (n=27). Last graded **2026-09-24 down/mild vs XLB −1.193% / SPY −0.08% / rel −1.11% (dir HIT, mag MISS — 1.19% is notable on the published bands)**. Active XLB rules checked: **09-24 rate-shock-vs-minority-sleeve is BINDING** — when a live hawkish duration shock hits a chemicals-majority book with red ES/NQ beyond ±0.5%, do **not** pay a minority metals sleeve as S1 +1 and net leading factors to 0; cap S1 at 0 and keep the S0 down sign. **09-23 divergence-resolution** — when the card's own flag fires and the stated rule is "trust the live spine over leftover tape," resolve *toward* the spine, not to flat; do not use premarket single-name prints to cap a live physical-tightness signal. **09-22 Cu-tightness vs flat-index** — |ES|,|NQ| < 0.5% + copper continuation must not be signed down; **today ES=F +0.28% / NQ=F +0.57% — NQ is outside the band, ES inside; the gate is only partially on.** **09-18 HEAT-down→down/mild** — OFF as a down trigger when the industrial spine is independently green/tight. **09-21 RS-veto triad** — needs PM red AND ES/NQ ≥ +1%; ES/NQ are +0.28/+0.57, so OFF. **09-17 residual-mild-up** — needs ES/NQ ≥ +0.5% and a held green PM; NQ +0.57% qualifies, ES +0.28% does not; XLB is **absent from the Channel 1 PM board** — partial. **09-16 oil+gold cash-transmission haircut** — ON as process (do not pay oil-offered + gold as cash-XLB support on a rate-shock day). **09-15 nested-bid** — ON as process, OFF as copper-HEAT long. **09-11 four-index ≥ +0.5% up-gate** — OFF (SPX +0.20 / NQ +0.41 / RTY +0.08 / DJIA +0.11). **09-10 gap-at-open** — OFF (no XLB PM print). **09-09 / 8/18** — OFF (oil offered, metals not co-moving down). **8/14 gold sleeve** — ON (GC +0.90%, SI +1.96%) but not a book bid. **8/25 / 8/27** — confirmed-up ban / S4 conviction cap. **8/17 / commodity-vs-flat-futures** — cap severe, not direction. **09-04 / 8/28** — do not copy the prior −1.11% rel into S4. DO-INSTEAD (last three BM losses): when factor sign fights leftover tape/HEAT, cut conviction; prefer flat/mild — **do not flip to down on a stale tape alone**. size_gate=True.

## Analysis — XLB, session of 2026-09-25 (Friday)

This is a **Friday T+1 after Thursday's rate-shock dump**, with a **live, tight industrial-metals spine** (copper nudging up, set for a weekly gain on China tightness), a **green monetary-metals sleeve**, and a **hawkish-but-already-printed** Fed path. It is not a Hormuz liquidation and not a same-morning China print. Channel 1 tape through 09-24: 1d rel **−1.11%**, 3d **−0.88%**, 1w **−2.89%**, 1m **−7.59%** — a deep multi-horizon hole that is **T-1 leftover**, not today's signal.

### 1. Shared macro as it hits materials (S0)

Knowable this morning (Channel 1, do not re-derive):

- **The dominant live driver is the rates regime, and it is still hostile but no longer accelerating.** News Judge #1: **US 10Y tops 5.2%**, yields spiking, ES/NQ futures ease. #2: **NY Fed Williams — another hike by year-end is "reasonable," officials not done.** #3: Warsh/JH lift September hike odds, gold slides >3%. #4: mortgage surge, 8% "not an impossibility," affordability 21-year low. This is a **rate/duration shock** mapping **negative** to a chemicals-heavy cyclical whose majority sleeve is rate- and demand-sensitive. **But**: the hike itself is **printed** (09-16, 25 bp to 3.75–4.00%, SEP 16/18 another hike), Williams/Warsh comments are **already printed**, and the 5-day 10Y–SPX corr is **−0.958** — the tightest stress coupling in the window. Do **not** re-score the binary (09-15/09-16); do score the *level* as a live duration overlay.
- **Futures are GREEN, not red.** ES=F **+0.28%**, NQ=F **+0.57%** vs prior close. Finviz four-index fails ≥ +0.5% (SPX +0.20 / NQ +0.41 / RTY +0.08 / DJIA +0.11). **09-24's red-ES/NQ condition is OFF** — so the "rate shock + red tape → down" protection does **not** fire today. **09-22's flat-index condition is partially on** (ES +0.28% inside ±0.5%; NQ +0.57% outside) — so the "don't sign down on a dead index" protection is **partially** live.
- **Oil offered, 8/18 OFF.** WTI **−1.59%** to $104.16, Brent **−1.02%**, CL=F **−1.71%** 1d, BZ=F **−7.41%** 1d. Hormuz remains a *level* (Brent >$100); the live increment is down. Count feedstock relief in S1 with the **09-16 haircut**, not a second S0 plus.
- **USD / real yields.** DXY **99.32, 1d −0.20% / 1m +2.2%** — firm on the month, **easing today**, not a spike. DFII10 **2.76 (+0.13 1d, +0.38 1m)** — real yields elevated and rising. DGS10 **5.11 (+0.15 1d, +0.41 1m)**, DGS30 **5.40**. Live notes slightly bid (10Y −0.03%, 30Y −0.06%). HY OAS **2.73 (+0.05 1d)** — contained but widening.
- **VIX 15.38 (−0.29)** with VIX/VIX3M **0.835 contango**. EPU **99.37 (−17.5 1d, −151.93 1w)** — policy uncertainty collapsing. **Asia composite −0.06%** (Nikkei +1.3%, Kospi +1.04%, but **Hang Seng −1.01%, Shanghai −1.22%** — no China impulse); **Europe +0.64%** (DAX +0.85%, EuroStoxx50 +0.88%) — green.

**S0 = −0.5.** The rate/duration level (10Y >5.2%, real yields 2.76 and rising, hawkish Williams/Warsh path) is a genuine live negative for a chemicals-majority cyclical, and the 10Y–SPX corr at −0.958 says yields are the transmission. But it is **not −1**: futures are green, USD is easing today, oil is offered, VIX is calm and in contango, Europe is green, and the hike path is already printed. This is the **09-24 lesson applied correctly** — keep the S0 down sign, but do not let a printed hawkish *level* force a full down call against a non-red tape.

### 2. Spine + secondary (S1)

**Industrial metals — continuation/tightness HIT, not collapse.** Channel 1: copper **$6.489 (+0.66%)**, aluminum **$3,430.75 (+1.10%)**, iron ore **$97.41 (−0.14%)**, steel HRC **$1,230 (−0.16%)**. Channel 2 (live): **"Copper nudges up, set for a weekly gain on China tightness"** (Business Recorder, 09-25); LME 3M ~**$14,616.50/t** on 09-24, ~1.7% below the **$14,875** record, with the pullback "driven primarily by dollar strength and Fed rate expectations rather than a breakdown in the [physical] story" (discoveryalert, 09-24). LME stocks **dropped 1,625 t**; cash-3M swung from an **$86/t discount on 09-14 to a ~$62–65/t premium by 09-23** — a **backwardation HIT**. Cancelled warrants **~45–48% of on-warrant** (115,450 t of 255,900 t) — **available-metal tightness HIT**. Spine "surge" is **partial** (copper near record, aluminum +1.10%); spine "collapse" is **OFF**. Iron/steel are **not** in the surge — composition stays chemicals-heavy.

**Inventory — available-metal tightness HIT, not a total-tonnes glut.** LME cancelled warrants ~45–48%; SHFE stocks down sharply since June; Yangshan premium near 4-year highs. Caveat (discoveryalert, 09-22): "the apparent tightness signalled by low LME and SHFE stocks is partly a geographical mirage… locational, not fundamental." Score the tightness as a **partial** HIT, not a full one.

**China demand — still contraction, not a rebound; do not let gold cancel it.** NBS mfg PMI **49.8** (<50), property FAI still ~**−19–20% YoY**, China net refined-copper imports **−13% y/y in H1 2026**. **Hang Seng −1.01% / Shanghai −1.22%** this morning — no China impulse. But the *physical* restocking signal (Yangshan premium, weekly copper gain on China tightness) is a **partial industrial offset**, not a PMI/property rebound HIT.

**Monetary metals — 8/14 sleeve ON, book bid OFF.** Finviz gold **+0.90%**, silver **+1.96%**, platinum **+0.61%**, palladium **+1.64%**. GC=F **+0.67%** 1d. But News Judge #3 flags the Warsh/JH gold slide >3% as the same hawkish impulse — the sleeve is **recovering**, not surging. NEM ~8% of XLB is a **sleeve**, not the book (09-16 NEM miss).

**Chemicals majority sleeve — oil-offered cost relief (the composition math).** LIN ~13%, SHW ~4.8%, ECL, DOW, APD — chemicals **49% of XLB** (Seeking Alpha, 09-24). Oil offered is feedstock relief, but the **09-16 haircut** says do not pay it as cash-XLB support on a rate-shock day. MAP HEAT Chemicals **flat, conv low** — "beat parent by ~4pts w1 but DOW is silent; HUN HSR and REX EPS are name-specific, not a sector UP."

**MAP HEAT nested overrides (do not average into parent):** Copper **down** (FCX neg, IE neg — "tariff-policy air pocket in FCX plus IE capex inflation"); Aluminum **down**; Gold **down** (NEM none, SSRM/NG small-pos); Other Industrial Metals **down**; Other Precious **down**; Building Materials **down**; Coking Coal **SPLIT/down**; Chemicals **flat**; Lumber **flat**. **The nested book is majority-down vs parent** — this is the 09-18 condition, and it argues against paying a metals bid.

**S1 = 0.** Net of: copper/aluminum continuation + backwardation + cancelled-warrant tightness + green gold/silver sleeve **versus** nested HEAT majority-down, China PMI/property contraction, iron/steel flat-to-down, chemicals flat with no confirmation, and the 09-16 haircut on oil-relief + gold as cash-XLB support. The spine is **genuinely two-sided** — a live tight industrial spine against a majority-down nested book. Not +1: the 09-24 lesson explicitly forbids paying a minority metals sleeve as S1 +1 when the nested book is down and the rate overlay is live. Not −1: copper is green and set for a weekly gain, backwardation is real, and the 09-22 flat-index gate is partially on.

### 3. Breadth (S2)

**S2 = −0.5.** MAP HEAT nested breadth is **majority-down** (Copper, Aluminum, Gold, Other Industrial Metals, Other Precious, Building Materials, Coking Coal all down; only Chemicals and Lumber flat). XLB is **absent from the Channel 1 sector PM board** — no same-morning participation print. 1w rel **−2.89%** and 1m rel **−7.59%** describe a sector that has been a persistent funding source. Not −1: the 1d rel **−1.11%** is T-1 leftover (09-04/8/28), and copper/aluminum are green this morning.

### 4. Flows / positioning (S3)

**S3 = −0.5.** ETFdb: XLB **5-day net AUM change −$281.25M**, **1-month −$541.67M**, 3-month −$161.93M — persistent outflows against a 1m rel of −7.59%. That is **sector rotation out of materials** with flow confirmation, not a washout setup. Not −1: 6-month +$1.86B and 1-year +$2.61B show the structural bid is intact, and the 1m rel hole is deep enough that a mean-reversion bounce is live (09-23's +1.87% rel day).

### 5. ETF tape (S4) — CONFIRMATION ONLY

**S4 = 0.** 1d rel **−1.11%** is **T-1 leftover** (09-04/8/28: do not copy a prior-day lag into S4 as fresh). 3d rel −0.88%, 1w −2.89%, 1m −7.59% are structural descriptors, not same-session signals. No XLB premarket print is available. **8/27 S4-cap**: a sub-0.5% or stale tape cannot be a ± confirmation. S4 = 0.

### Divergence check

Leading factor sum (S0 −0.5, S1 0, S2 −0.5, S3 −0.5) = **−1.5**, net negative. S4 = 0 (no confirmation). **Divergence: the factor card is mildly negative while the live tape (ES +0.28%, NQ +0.57%, Europe +0.64%, copper +0.66%, gold +0.90%, silver +1.96%, USD easing) is green.** Per the shared method, **trust factors over tape** — but the factor sum is only mildly negative, and the 09-22 flat-index gate is partially on (ES inside ±0.5%). The honest resolution is **flat-to-mild-down**, not a confident down. The 09-23 lesson warns against neutralizing a flag into flat; the 09-24 lesson warns against paying a minority sleeve to net to zero. Both point to: **keep the mild negative sign, do not widen the band.**

### Self-audit

- **Lens**: XLB only, chemicals 49% of the book — not a copper-miner ETF. ✓
- **Band**: |leading sum| = 1.5 with mult 0.85 → mild, not notable. The 09-24 mag miss (1.19% actual vs mild band) argues for **not** widening; the 09-24 reflect explicitly says "do not learn a 'must be notable' rule from a 19 bp overshoot." ✓
- **Skew**: S0 −0.5 (not −1) because futures are green and the hike is printed; S1 0 (not +1) because the nested book is majority-down. ✓
- **Same-shock double-count**: the rate shock is scored **once** in S0; oil-offered feedstock relief is scored **once** in S1 with the 09-16 haircut; the 1d rel lag is **not** re-scored in S2 or S4. ✓
- **Single-ticker**: FCX/NEM/LIN do not drive the sector call — MAP HEAT nested overrides are explicitly not averaged into the parent. ✓

### HORIZON

- **3D**: The rate level (10Y >5.2%, real yields 2.76) is the dominant drag; copper tightness is a partial offset. Mild-down bias, with a live bounce risk if yields stabilize.
- **1W**: XLB has been a funding source for 1m (−7.59% rel). Without a China PMI/property rebound or a copper breakout above $14,875, the path of least resistance is flat-to-down with relative lag.
- **2W**: Watch the year-end hike path (Williams/Warsh) and any China stimulus. A confirmed China credit/property headline or a fresh LME squeeze would flip the spine positive.
- **1M**: The 1m rel hole (−7.59%) is extreme; mean-reversion risk is real, but flows (−$541.67M 1m) have not turned. Structural bid intact (1y +$2.61B).

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -0.5
S1_SECTOR_FACTORS: 0
S2_BREADTH: -0.5
S3_FLOWS_POSITIONING: -0.5
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.42
REGIME: mixed
DIVERGENCE: 1
SECTOR_SCORES_END

HIT_GRID_BEGIN
Real yields rising|HIT|0.75|2026-09-25|https://fred.stlouisfed.org/series/DFII10
USD strengthening|PARTIAL|0.40|2026-09-25|https://www.marketwatch.com/investing/index/dxy
Risk-off tape / flight to safety|PARTIAL|0.45|2026-09-25|https://www.cnbc.com/quotes/US10Y
Industrial metal price surge (copper/aluminum/iron ore)|PARTIAL|0.55|2026-09-25|https://news.google.com/rss/articles/CBMimwFBVV95cUxOR3JNVFc1Q1QwV3A3Si1fSkxwNWpUV2s5cFEwYXVxV1hWZFRDR0t3VjFlX3QtdDRURXduakhtR1hrQTAteTZ0dTh3WUtXVzdUem1Jd1Mxc1NhbkFtUUU1eWpWMl9jV3VqNEs4c3hwLWZRbXFYX0dIRHNMQl9uR0c3a3ZKRjEwOTdSRElZZzg0TFdDOXluYlNtdV9Sc9IBVkFVX3lxTE9LT0lmbUFMQ08zQnJxNEpHdFVOd05Sc1pHQzNWZjdQV3BiV3pickVuZHAydzBsQU5vSVJEYkhHdXZTbFdIZ3hucEFEWkMzeHR5OEphTWVn
Inventory draw (LME/exchange stocks down)|HIT|0.65|2026-09-25|https://nai500.com/blog/2026/09/copper-rises-for-fifth-day-as-china-supply-tightness-signals-spot-squeeze/
Gold/silver price surge (monetary metals)|PARTIAL|0.45|2026-09-25|https://www.kitco.com/news/article/2026-09-16/gold-silver-prices-fall-warsh-signals-more-tightening-ahead-kitco-pm-report
China demand shock / property stress|HIT|0.70|2026-09-25|https://www.mysteel.net/top-news/copper/
Supply glut / new capacity online|PARTIAL|0.35|2026-09-25|https://discoveryalert.com/news/copper-market-risks-september-2026/
Critical-minerals policy / domestic tariff support|PARTIAL|0.50|2026-09-25|https://ustariffrates.com/section-232-tariffs
Sector rotation out of materials|HIT|0.65|2026-09-25|https://etfdb.com/etf/XLB/
Sector breadth failure (ETF up, names flat)|HIT|0.60|2026-09-25|https://seekingalpha.com/article/4948016-xlb-materials-dashboard-for-september
Sector ETF outflow / volume dry-up|HIT|0.60|2026-09-25|https://etfdb.com/etf/XLB/
Margin compression / cost inflation without pricing power|PARTIAL|0.35|2026-09-25|https://seekingalpha.com/article/4948016-xlb-materials-dashboard-for-september
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -0.5, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': -0.5, 'S3_FLOWS_POSITIONING': -0.5, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': -2.0, 'divergence_flagged': True, 'total_score': 1.377, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.455, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.462, 'score': 2.772, 'legs': [{'leg': 'ES', 'pct': 0.28, 'w': 0.6}, {'leg': 'HG', 'pct': 0.66, 'w': 0.3}, {'leg': 'GC', 'pct': 0.9, 'w': 0.1}, {'leg': 'DX', 'pct': -0.02, 'w': -0.3}]}, 'overlay_score': -2.072, 'overlay_raw': -2.072, 'index_carry': 0.676, 'general_total': 2.706, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 0.0}, 'llm_confidence': 0.42, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -2.08, 'w1': -4.78}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
