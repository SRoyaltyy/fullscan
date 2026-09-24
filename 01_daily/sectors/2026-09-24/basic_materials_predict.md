# Sector Prediction — Basic Materials — 2026-09-24

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-2.88** (mult 0.85)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-0.54** (ES -0.64%, HG +0.66%, GC +0.90%, DX -0.02%) · index_carry **-1.915** (general -7.659) · llm_overlay **-0.425** (raw -0.425)

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-23):
  1d: XLB +1.15% | SPY -0.74% | rel +1.88%
  3d: XLB -0.39% | SPY +0.93% | rel -1.32%
  1w: XLB -0.43% | SPY +1.63% | rel -2.06%
  1m: XLB -5.66% | SPY +0.52% | rel -6.18%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.2 mag=0.2 (n=10); last-30 dir=0.423 mag=0.385 (n=26). Last graded 2026-09-23 flat/flat vs XLB **+1.1466%** / SPY **−0.7202%** / rel **+1.8669%** (dir MISS, mag MISS). Active XLB rules checked: **09-23 divergence-resolution is BINDING** — when the card's own divergence flag fires and the stated resolution rule is "trust the live spine over leftover tape," the output must resolve *toward* the live spine (mild-up), not split the difference to flat; do not use premarket single-name prints (FCX/NEM PM) to cap a live physical-tightness signal or discount a live green sleeve; "cap conviction not direction" caps multiplier/confidence, not sign. **09-22 Cu-tightness vs flat-index** — |ES|,|NQ| < 0.5% + copper continuation/tightness must not be signed down; pay the spine in S1. **09-18 HEAT-down→down/mild** — OFF as a down trigger when the industrial spine is independently green/tight. **09-21 RS-veto triad** — needs PM red AND ES/NQ ≥ +1%; today PM:XLB is not on the board and ES/NQ are RED, so the triad is off. **09-17 residual-mild-up** — needs ES/NQ ≥ +0.5% and a held green PM; ES=F **−0.64%** / NQ=F **−1.09%**, so OFF. **09-16 oil+gold cash-transmission haircut** — ON as process (do not pay oil-offered + gold as cash-XLB support on a rate-shock day). **09-15 nested-bid** — ON as process, OFF as copper-HEAT long. **09-11 four-index ≥ +0.5% up-gate** — OFF (SPX +0.20 / NQ +0.41 / RTY +0.08 / DJIA +0.11). **09-10 gap-at-open** — OFF (no XLB PM print; ES/NQ red). **09-09 / 8/18** — OFF (oil offered, metals not co-moving down). **8/14 gold sleeve** — ON (GC +0.90%, SI +1.96%) but not a book bid. **8/25 / 8/27** — confirmed-up ban / S4 conviction cap. **8/17 / commodity-vs-flat-futures** — cap severe, not direction. **09-04 / 8/28** — do not copy the prior −1.65% rel into S4. DO-INSTEAD (last three BM losses): when factor sign fights leftover tape/HEAT, cut conviction; prefer flat/mild — do not flip to down. size_gate=True.

## Analysis — XLB, session of 2026-09-24

This is a **hawkish-rate-shock Thursday** — Warsh explicitly putting a September hike back on the table, 10Y backing up, Wall Street lower — colliding with a **live, tight industrial-metals spine** (copper continuation, backwardation, cancelled-warrant tightness) and a **green monetary-metals sleeve**. It is not a Hormuz liquidation and not a same-morning China print. Channel 1 tape through 09-23: 1d rel **+1.88%** (the safe-haven/real-asset rotation day), but 3d **−1.32%**, 1w **−2.06%**, 1m **−6.18%** — a deep multi-horizon hole that is **T-1 leftover**, not today's signal.

### 1. Shared macro as it hits materials (S0)

Knowable this morning (Channel 1, do not re-derive):

- **The dominant live driver is the Warsh hawkish regime shift** (News Judge #1/#2): explicit hike signal, 10Y backup, SPX lower. This is a **rate/duration shock**, and it maps **negative** to a chemicals-heavy cyclical whose majority sleeve is rate- and demand-sensitive.
- **Futures are RED, not flat.** ES=F **−0.64%**, NQ=F **−1.09%** vs prior close. Finviz four-index fails ≥ +0.5% (SPX +0.20 / NQ +0.41 / RTY +0.08 / DJIA +0.11). **09-17 residual-mild-up is OFF** (needs ES/NQ ≥ +0.5%). **09-22's flat-index condition is also OFF** (|ES|,|NQ| are now > 0.5% and negative) — so the "don't sign down on a dead index" protection does **not** apply today.
- **Oil offered, 8/18 OFF.** WTI **−1.59%** to $104.16, Brent **−1.02%**, CL=F **+1.86%** 1d (rebound off the prior drop). Hormuz remains a *level* (Brent >$100); the live increment is not a fresh squeeze. Count feedstock relief in S1 with the **09-16 haircut**, not a second S0 plus.
- **USD / real yields.** DXY **+0.13% 1d / +2.25% 1m** — firm, grinding higher, a headwind for the commodity complex but **not a spike**. DFII10 **2.63 (+0.01 1d, +0.23 1m)** — real yields elevated and rising. DGS10 **4.96**, DGS30 **5.29**. Live notes slightly offered (10Y −0.03%, 30Y −0.06%). 5-day 10Y–SPX corr **−0.826** — yields are the live transmission, and they are working against equity multiples.
- **VIX 16.44 (+1.26 1d)** with VIX/VIX3M **0.908** — stress building, near backwardation, not panic. HY OAS **2.68** contained. **Asia composite −0.09%** (Shanghai **−1.22%**, Hang Seng −0.29%, Nikkei +0.76%, Kospi +1.04%); **Europe −0.40%** (DAX −0.52%, CAC −0.56%). Both mildly red — no China impulse, no risk-on confirmation.
- **US–Canada trade war / recession fears** (News Judge #8) is a tariff/cyclical overhang.

**S0 = −1.** The Warsh hawkish regime shift + 10Y backup + red ES/NQ + firm USD + rising real yields + red Asia/Europe map negative to this cyclical. Not −2: DXY is firm not spiking, HY is tight, VIX is elevated not panicked, and the metals spine is independently green/tight (which is a *sector* offset, scored in S1, not a macro plus). This is the inverse of 09-22/09-23 — the index is no longer dead-flat, so the "don't sign down on a flat index" protection is off, and the rate shock is a genuine live negative.

### 2. Spine + secondary (S1)

**Industrial metals — continuation/tightness HIT, not collapse.** Channel 1: copper **$6.489 (+0.66%)**, aluminum **+1.10%**, iron ore **−0.14%**, steel HRC **−0.16%**. Channel 2 (09-22/09-23 close, still the live physical story): LME 3M into **~$14,745–14,766/t**, within ~1% of the **$14,875** Sep-10 record; sixth straight up day; cash-3M **backwardation ~$62/t** (from an **$86/t contango** a week earlier). LME cancelled warrants **122,150 t (48% of ~255,100 t on-warrant)** → **~133,725 t** actually available; SHFE stocks **−70% since early June**; Shanghai cathode **43,900 t** (lowest since 2023); Yangshan premium near **4-year highs**. This is a **genuine tightness HIT** — the spine's "inventory draw" is *inverted from a glut to a squeeze*, and "industrial metal price surge" is a partial HIT (copper/aluminum green, iron/steel flat).

**Monetary metals — 8/14 sleeve ON.** Gold **+0.90%** (GC=F −0.68% 1d — a stale futures print vs the live spot bid), silver **+1.96%**, platinum **+0.61%**, palladium **+1.64%**. Per 09-23, do **not** discount a live green sleeve using a stale 1d futures print. NEM ~8% of XLB is a **sleeve**, not the book.

**China demand — still contraction, not a rebound; do not let gold cancel it.** NBS mfg PMI **49.8** (<50), property FAI still deeply negative. T-2, not a same-morning miss. Shanghai **−1.22%** today is a mild China-equity drag, not a hard-data shock.

**Chemicals majority sleeve — the composition math.** LIN ~13%, SHW ~4.8%, ECL — ~40–50% combined. On a **rate-shock day** with red ES/NQ, this majority sleeve is the **rate/demand-sensitive drag**, and the 09-16 haircut says do **not** pay oil-offered feedstock relief as cash-XLB support when the live macro is a rate shock. This is the key asymmetry: the *minority* metals sleeve is green/tight, but the *majority* chemicals sleeve is exposed to the hawkish-rate/demand drag.

**Supply disruption / tariffs — stale.** DRC concentrate ban and Section 232 refined-copper tariff uncertainty remain on the books (News Judge #7 BHP/tariff line); not a same-open catalyst.

**S1 = +1.** Net of the live copper tightness/continuation + green monetary sleeve versus the chemicals-majority rate/demand drag + carried China contraction + tariff stall + incomplete iron/steel. Not +2/+3: chemicals are the book, the 09-16 haircut applies on a rate-shock day, and 8/17 caps severe on copper into a genuine risk-off. Not 0/−1: the spine is independently green and tight, and 09-22/09-23 forbid signing the spine down.

### 3. Breadth (S2)

The 09-23 session was a **safe-haven/real-asset rotation** (XLB +1.15% while SPY −0.74%) — that is a *relative* breadth expansion for materials on a down-index day, and it is the one genuinely bullish structural signal. But it is **T-1 leftover**, and today's live tape is red ES/NQ with a rate shock. The nested book is mixed: metals names bid, chemicals names exposed to the rate/demand drag. **S2 = 0** — the prior-session relative breadth expansion is real but stale, and today's live index tape is red; do not double-count the 09-23 rotation into both S2 and S4.

### 4. Flows / positioning (S3)

No fresh XLB-specific flow or crowding signal in Channel 1/2. The 1m rel **−6.18%** hole means XLB is a **relative laggard**, not a crowded long — so the crowded-long mean-reversion risk is **not** firing. **S3 = 0.**

### 5. ETF tape (S4) — confirmation only

1d rel **+1.88%** is confirmation-eligible (>0.5%), but it is the **prior session's** print and the 09-04/8/28 T-1 rule says do not copy it as fresh. The 3d/1w/1m rel are all negative. **S4 = 0** — the 1d rel is stale, and the multi-horizon tape is negative; do not pay it as same-session confirmation.

### Divergence and resolution

Leading factor sum (S0 −1, S1 +1, S2 0, S3 0) = **0**, versus S4 0 and the leftover 1d rel +1.88%. The **live spine is green/tight** (copper continuation, backwardation, cancelled-warrant squeeze) but the **live macro is a hawkish rate shock with red ES/NQ**. Per the 09-23 binding rule, a divergence flag must resolve *toward the live spine* — but here the live spine is a **minority sleeve** and the live macro is a **majority-sleeve rate shock**. The honest resolution is **flat/mild with a relative-lag lean**: the metals tightness prevents a down call (09-22/09-23 forbid signing the spine down), while the Warsh rate shock + red ES/NQ + chemicals-majority exposure prevents an up call (09-16 haircut, 8/25 confirmed-up ban). This is a genuine two-sided session — the correct expression is **flat**, with the divergence flagged.

**Direction: flat. Band: flat.** Confidence modest (0.40) given the two-sided setup and the BM scope's poor recent hit rate.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: 1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.85
CONFIDENCE: 0.40
REGIME: mixed
DIVERGENCE: 1
SECTOR_SCORES_END

HIT_GRID_BEGIN
Industrial metal price surge (copper/aluminum/iron ore)|HIT|0.70|2026-09-24|https://www.mining.com/copper/
Inventory draw (LME/exchange stocks down)|HIT|0.65|2026-09-24|https://www.lme.com/
Gold/silver price surge (monetary metals)|HIT|0.60|2026-09-24|https://www.kitco.com/
Real yields rising|HIT|0.65|2026-09-24|https://fred.stlouisfed.org/series/DFII10
USD strengthening|PARTIAL|0.50|2026-09-24|https://www.marketwatch.com/investing/index/dxy
Risk-off tape / flight to safety|PARTIAL|0.55|2026-09-24|https://www.cnbc.com/
China demand shock / property stress|PARTIAL|0.50|2026-09-24|https://www.reuters.com/
Critical-minerals policy / domestic tariff support|PARTIAL|0.45|2026-09-24|https://www.mining.com/
Margin compression / cost inflation without pricing power|PARTIAL|0.40|2026-09-24|https://www.reuters.com/
Supply glut / new capacity online|MISS|0.30|2026-09-24|https://www.lme.com/
Industrial metal price collapse|MISS|0.25|2026-09-24|https://www.mining.com/
Sector rotation into materials|PARTIAL|0.45|2026-09-24|https://www.finviz.com/
Sector breadth expansion (% names up)|PARTIAL|0.40|2026-09-24|https://www.finviz.com/
Sector ETF outflow / volume dry-up|MISS|0.30|2026-09-24|https://www.etf.com/
HORIZON_3D|flat|0.40|2026-09-24|https://www.finviz.com/
HORIZON_1W|down|0.40|2026-09-24|https://www.finviz.com/
HORIZON_2W|down|0.40|2026-09-24|https://www.finviz.com/
HORIZON_1M|down|0.45|2026-09-24|https://www.finviz.com/
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.85, 'leading_sum': 1.0, 'divergence_flagged': False, 'total_score': -2.88, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.615, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': -0.09, 'score': -0.54, 'legs': [{'leg': 'ES', 'pct': -0.64, 'w': 0.6}, {'leg': 'HG', 'pct': 0.66, 'w': 0.3}, {'leg': 'GC', 'pct': 0.9, 'w': 0.1}, {'leg': 'DX', 'pct': -0.02, 'w': -0.3}]}, 'overlay_score': -0.425, 'overlay_raw': -0.425, 'index_carry': -1.915, 'general_total': -7.659, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 0.0}, 'llm_confidence': 0.4}
```
