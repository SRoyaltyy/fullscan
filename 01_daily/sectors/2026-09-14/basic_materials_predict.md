# Sector Prediction — Basic Materials — 2026-09-14

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **notable**
- total_score: **-15.213** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**
- engine: v2 · tape_anchor **-6.462** (ES -0.66%, HG -1.44%, GC -1.26%, DX +0.41%) · index_carry **-2.751** (general -11.002) · llm_overlay **-6.0** (raw -7.425)

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-11):
  1d: XLB +0.37% | SPY +0.85% | rel -0.48%
  3d: XLB -1.91% | SPY -0.22% | rel -1.69%
  1w: XLB -3.17% | SPY -1.15% | rel -2.03%
  1m: XLB -3.10% | SPY -1.06% | rel -2.04%
```

# Sector Prediction — Basic Materials — 2026-09-14

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **−4.05** (mult 0.9)
- regime: risk_off
- divergence_flagged: **True**

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-09-11):
  1d: XLB +0.37% | SPY +0.85% | rel -0.48%
  3d: XLB -1.91% | SPY -0.22% | rel -1.69%
  1w: XLB -3.17% | SPY -1.15% | rel -2.03%
  1m: XLB -3.10% | SPY -1.06% | rel -2.04%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Memory index unavailable this run (embedding metadata missing) — used injected scoreboard/lessons only. Rolling last-10 dir=0.5 mag=0.6 (n=10); last graded 2026-09-11 flat/flat vs XLB +0.37% / SPY +0.85% / rel −0.48% (dir MISS, mag MISS). Active XLB rules checked: **09-11 pre-binary-tape rule is the BINDING rule** — separate the unknowable binary from the knowable pre-binary tape; when futures confirm a directional move, score a modest S0 lean in that direction rather than zeroing everything, and carry an explicit relative-underperformance lean when 1w/1m rel are persistently negative. **09-10 gap-at-open rule** — if |open vs prior close| ≥ 1.0%, the mild band is falsified at the bell and must be raised to at least notable (must check the XLB open). **09-09 composition/magnitude rule** — when the 8/18 metals-co-move floor ban fires with all four S1 sub-channels negative and zero offset, score S1=−2, S2=−1. **8/18 metals-as-floor ban** — do NOT use copper/gold as a floor on oil-shock risk-off days. **8/14 gold-offset** — score the monetary bid only if gold/silver are green (today they are NOT). **8/25 composition/transmission** — chemicals ~40–50% of XLB vs copper miners ~10–15%; NQ>>ES is not a materials green light. **8/27 S4-cap** — 1d rel <0.5% cannot be a ± confirmation. **8/28 leftover-S2/S4 down-mandate** — S4 confirms only the session being predicted. **09-03 exhaustion-bounce** — sleeve-driven >+1% reversal within a negative 1w is a bounce, not an inflection. **09-04 T-1-lag** — do not copy a prior-day lag into S4 as fresh. No open experiment for `sector_basic_materials`. DO-INSTEAD: keep direction; shrink confidence on modest |score| when magnitude historically misses.

## Analysis — XLB, session of 2026-09-14

This is a **Monday risk-off open into a hawkish-Fed / hot-CPI / oil-spike stack**, with the metals complex in a **broad, fresh collapse** — not a copper-squeeze day and not a leftover-chemicals fade. Channel 1 tape through 09-11 is decisively negative across every horizon: 1d rel **−0.48%**, 3d **−1.69%**, 1w **−2.03%**, 1m **−2.04%**.

### 1. Shared macro as it hits materials (S0)

The dominant live driver is the **hawkish Fed repricing confirmed by a hot CPI**. August CPI printed **3.4% y/y, above forecasts** (CNBC/CBS/Morningstar, 09-11), and Reuters headlined "Fed rate-hike case builds as inflation fails to cool." CME FedWatch now puts the **September 16 hike at ~56–60%** — a coin-flip that has tipped hawkish. This is the regime object: it repriced the front end, firmed the dollar, and is the reason the entire commodity complex is being liquidated.

Live tape (Channel 1, do not re-derive): **ES −0.67%, NQ −1.60%, Russell −0.27%, DJIA −0.15%** — a **tech-led risk-off**, with NQ far weaker than ES. **VIX 17.67 (+1.83 1d, +2.37 1w)** with **VIX/VIX3M 1.135 backwardation** — stress building, not panic. **DXY +0.41%** (1m −0.39%) — a firm dollar, not a spike, but a headwind for the complex. **Real yields grinding higher**: DFII10 **2.55 (+0.09 1d, +0.12 1m)**, DGS10 **4.95 (+0.12 1d, +0.25 1m)**, DGS30 **5.37**. **USEPUINDXD 725.88 (+451 1d)** — policy uncertainty spiking. **HY OAS 2.70** still tight. **Asia composite −0.72%** (Kospi −3.26% the outlier, Nikkei −0.81%, Hang Seng +0.45%); **Europe −0.35%**.

The materials-specific overlay is the **8/18 metals-co-move floor ban firing cleanly**: oil is **spiking** (WTI **$102.29 +2.44%**, Brent **$107.33 +2.80%**, on fresh strikes on Saudi/Hormuz per ET), and the **entire metals complex is co-moving DOWN with equities** — copper **−1.44%**, silver **−2.17%**, platinum **−0.87%**, palladium **−1.84%**, gold **−1.26%**, aluminum **−1.91%**, iron ore **−0.67%**. This is risk-asset liquidation, not a hedge. Do **not** score S0 as risk_on because oil is up.

**S0 = −1.** Hot CPI → hawkish Fed repricing + firm USD + rising real yields + oil-spike risk-off + backwardation map negative to this cyclical. Not −2: ES is only −0.67%, DXY is not a spike, HY is tight, and the FOMC binary (09-16) is still two days out — but the pre-binary tape is knowable and it is red (09-11 rule).

### 2. Spine + secondary (S1)

**Industrial metals — COLLAPSE, not surge.** Copper **−1.44%** to $6.4545, aluminum **−1.91%**, iron ore **−0.67%**. Mysteel (09-14): "Copper prices plunge on Fed rate-hike bets and U.S. tariff uncertainty." Bloomberg (09-14): "Copper slips as inflation data raises bets on Fed hiking rates." mining.com.au: "Copper's record run loses steam as US tariff fears rattle markets." Spine "surge" **off**; spine "collapse" is a **clean HIT** (copper down from record highs on two consecutive drivers: hawkish Fed + unresolved Section 232 refined-copper tariff).

**Inventory draw — off.** LME copper warehouse stocks **234.5K mt as of 09-11, +10.5% over 30 days** — a rebuild, not a draw. Spine "inventory draw" is a **MISS**.

**China demand — still the industrial offset, and a fresh data risk.** August NBS mfg PMI **49.8** (still <50, second straight month of contraction per CNBC). **China's August activity data (IP, retail sales, FAI) is due this week (14–18 Sep)** — a live, knowable-at-open event risk for the industrial-demand spine. Lundgreen's: retail sales and FAI "likely to underline the continued divergence between resilient services demand and weak momentum for goods and investments." Do **not** pre-score the print, but do not treat China as a rebound either.

**Monetary metals — FADE.** Gold **−1.26%**, silver **−2.17%**. 8/14 does **not** pay (gold is not green). The Warsh hawkish repricing is the driver; gold miners (NEM/GOLD/AU/SSRM) are the exposed sleeve.

**Oil spike = cost headwind for the chemicals majority sleeve.** The chemicals-heavy book (LIN ~13%, SHW, ECL, DOW — ~40–50% combined) faces a direct oil feedstock/energy cost squeeze from WTI +2.44% / Brent +2.80%. Per 8/25 composition-weighting, this is a genuine majority-sleeve negative.

**Supply disruption / tariffs — stale but live-adjacent.** Section 232 copper (50%) and steel/aluminum (50%) remain on the books; the **refined-copper tariff decision is still unresolved** (White House missed its June 30 deadline; Reuters 09-10 report that no decision has been made). This is a **live overhang on the miner sleeve**, not a fresh positive. APD's Q3 beat/raise is **already traded** (late July) and carries a $2.9B clean-energy exit charge — a single-name positive, not an XLB-wide thrust.

**S1 = −2.** Per the 09-09 lesson: all four sub-channels align negative (chemicals oil-cost drag + copper collapse + gold/silver fade + China contraction) with **zero offsetting positive** anywhere in the book. The "not a collapse" cap does **not** apply — copper is down ~1.4% and off record highs on a fresh hawkish-Fed + tariff-uncertainty combination, and the minority sleeve that could have provided offset (copper miners) is also negative.

### 3. Breadth (S2)

The 09-11 partial breadth repair (oil offered → chemicals cost relief) has **fully reversed**: oil is now spiking, so the chemicals majority sleeve is back to a cost squeeze, and the metals sleeve is collapsing. There is **no defensive pocket** inside XLB — chemicals, copper miners, and gold miners are all negative together (the 8/18 pattern). Per the 09-09 lesson, when the 8/18 metals-co-move pattern fires, breadth is uniformly negative.

**S2 = −1.**

### 4. Flows / positioning (S3)

XLB is a **deep multi-horizon relative laggard** (1w rel −2.03%, 1m rel −2.04%) with persistent outflows from prior logs (~−$180M 1m range). Not a washout, not a volume spike. Per the 09-11 rule, the persistent relative lag is a **standing relative-underperformance lean**, not a fresh 1-day signal — score it once here, not again in S4.

**S3 = −0.5.**

### 5. Tape (S4, confirmation only)

1d rel **−0.48%** is sub-0.5% → per the 8/27 S4-cap, it **cannot** be a ± confirmation. The 3d/1w/1m lags are already scored in S3 (do not double-count). **S4 = 0.**

### Reconciliation

Total = (−1 + −2 + −1 + −0.5 + 0) × 0.9 = **−4.05** → **down/mild**.

**Gap check (09-10 rule):** XLB premarket is not in the Channel 1 feed; the sector ETF premarket table shows XLI −1.13% / XLK −1.95% / XLE +1.50% but no XLB line. With ES −0.67% and the metals complex down 1–2%, an XLB open in the −0.5% to −1.0% range is the base case — **below** the 1.0% notable threshold. The 09-10 gap rule therefore does **not** fire at the snapshot; if the actual open prints ≥1.0% down, the band must be raised to notable at the bell.

**Divergence flag = True.** The leading-factor sum (S0+S1+S2+S3 = −4.5) fights the tape confirmation score (S4 = 0, 1d rel sub-0.5%). Per the shared method, **trust the factors over the tape** — the tape is a stale Friday close that already mean-reverted once (09-04 lesson), while the factors are live and uniformly negative. Direction stays **down**; magnitude is capped at **mild** because (a) the 1d rel is sub-0.5% and cannot confirm, (b) rolling mag accuracy is 0.6 with two consecutive mag misses in the same direction, and (c) the FOMC binary (09-16) and China activity data (this week) are unresolved and cap extension.

**Relative lean:** XLB should **underperform SPY** on this session — the persistent 1w/1m lag plus the oil-cost squeeze on the chemicals majority sleeve plus the metals collapse. Absolute down/mild, relative lag.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -2
S2_BREADTH: -1
S3_FLOWS_POSITIONING: -0.5
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.6
REGIME: risk_off
DIVERGENCE_FLAGGED: True
TOTAL_SCORE: -4.05
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.85|2026-09-14|https://www.cnbc.com/2026/09/11/cpi-inflation-august-2026.html
Real yields rising|HIT|0.8|2026-09-14|https://fred.stlouisfed.org/series/DFII10
USD strengthening|HIT|0.6|2026-09-14|https://www.marketwatch.com/investing/index/dxy
Industrial metal price collapse|HIT|0.9|2026-09-14|https://www.bloomberg.com/news/articles/2026-09-14/copper-slips-as-inflation-data-raises-bets-on-fed-hiking-rates
Gold/silver price surge (monetary metals)|MISS|0.9|2026-09-14|https://discoveryalert.com/news/gold-silver-selloff-warsh-jackson-hole-august-2026/
China demand shock / property stress|HIT|0.7|2026-09-14|https://www.cnbc.com/2026/08/31/china-pmi-august-economy-slowdown.html
Inventory draw (LME/exchange stocks down)|MISS|0.85|2026-09-14|https://thevaultreport.com/lme/copper
Supply disruption (mine/export ban)|NEUTRAL|0.5|2026-09-14|https://www.techtimes.com/articles/327262/20260910/white-house-copper-tariff-stall-creates-costs-no-decision-fixes-supply-before-mid-2040s.htm
Critical-minerals policy / domestic tariff support|NEUTRAL|0.55|2026-09-14|https://www.ghy.com/trade-compliance/us-adjusts-section-232-tariffs-on-aluminum-steel-and-copper-full-customs-value-now-applies/
Margin compression / cost inflation without pricing power|HIT|0.7|2026-09-14|https://www.investing.com/etfs/spdr-materials-select-sector-etf
Sector breadth failure (ETF up, names flat)|HIT|0.65|2026-09-14|https://www.thetrading.tools/sector-performance
Sector rotation out of materials|HIT|0.7|2026-09-14|https://www.thetrading.tools/sector-performance
Sector ETF outflow / volume dry-up|HIT|0.55|2026-09-14|https://securitiesdb.com/etf/xlb
Crowded long (extreme relative performance + valuation)|MISS|0.6|2026-09-14|https://www.thetrading.tools/sector-performance
HORIZON_3D: down|0.55|2026-09-14|https://www.thetrading.tools/sector-performance
HORIZON_1W: down|0.55|2026-09-14|https://www.thetrading.tools/sector-performance
HORIZON_2W: flat|0.45|2026-09-14|https://www.thetrading.tools/sector-performance
HORIZON_1M: flat|0.4|2026-09-14|https://www.thetrading.tools/sector-performance
HIT_GRID_END

**Channel 2 coverage note:** Covered — (1) shared macro regime as it hits materials (hot CPI → hawkish Fed repricing, firm USD, rising real yields, oil-spike risk-off); (2) sector spine factors (industrial metal collapse HIT, inventory draw MISS, China contraction HIT, supply disruption/tariff overhang NEUTRAL); (3) secondary factors (gold/silver fade, margin compression on chemicals, rotation out of materials); (4) breadth/leadership (uniformly negative, no defensive pocket); (5) flows/positioning (persistent outflows, deep multi-horizon laggard); (6) catalysts (FOMC 09-16, China August activity data this week, unresolved Section 232 refined-copper decision). Nothing material found on XLB-specific premarket gap or a fresh single-name catalyst beyond the already-traded APD beat.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -2.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -0.5, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -10.0, 'divergence_flagged': False, 'total_score': -15.213, 'predicted_direction': 'down', 'predicted_magnitude_band': 'notable', 'confidence_score': 0.85, 'regime': 'risk_off', 'engine': 'v2', 'anchor': {'available': True, 'pct': -1.077, 'score': -6.462, 'legs': [{'leg': 'ES', 'pct': -0.66, 'w': 0.6}, {'leg': 'HG', 'pct': -1.44, 'w': 0.3}, {'leg': 'GC', 'pct': -1.26, 'w': 0.1}, {'leg': 'DX', 'pct': 0.41, 'w': -0.3}]}, 'overlay_score': -6.0, 'overlay_raw': -7.425, 'index_carry': -2.751, 'general_total': -11.002, 'skill_multipliers': {'S0_SHARED_MACRO': 1.25, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 1.0, 'S4_ETF_TAPE': 0.0}, 'llm_confidence': 0.6}
```
