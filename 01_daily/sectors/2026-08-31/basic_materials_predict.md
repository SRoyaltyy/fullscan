# Sector Prediction — Basic Materials — 2026-08-31

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-6.75** (mult 0.9)
- regime: risk_off
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-08-27):
  1d: XLB -0.82% | SPY +0.66% | rel -1.48%
  3d: XLB -0.65% | SPY +1.00% | rel -1.65%
  1w: XLB +1.55% | SPY +1.11% | rel +0.43%
  1m: XLB +2.88% | SPY +5.71% | rel -2.83%
```

I have enough to finalize. XLB trades at $53.18 (RSI 50, above 50-day avg $51.71) — neutral positioning, not crowded. Let me write the final analysis.

**MEMORY_CONFIRM:** Sector Basic Materials / XLB only. Scoreboard last-10 dir=0.5 mag=0.6 (n=10); last graded 2026-08-28 down/flat vs XLB −0.094% / SPY −0.227% / rel +0.133% (dir MISS, mag HIT). Active XLB rules checked: **8/18 commodity-co-move floor ban FIRES** — live Hormuz/oil risk-off (CL +1.97%, Brent ~$90, US-Iran strikes) is the exact pattern where metals co-move DOWN with equities, so do NOT use the copper/gold bid as a floor; **8/17 China-miss + flat-futures severe ban PARTIAL** — fresh China PMI 49.8 (improved but still contraction) released this morning, futures mildly negative; **8/14 gold-offset does NOT pay** — gold −0.81%, silver −0.26% (fading, not green); **8/25 composition/transmission PARTIAL** — 1d rel −1.48% << 0.5%, chemicals-heavy book, but NQ>>ES leg off; **8/27 enforcement applies** — do not emit up, do not score S4 positive from sub-0.5% tape; **8/28 rule does NOT apply** — S0 is NOT zero today (live Hormuz/oil risk-off + hawkish Fed), so residual is down-biased, not flat. No open experiment for `sector_basic_materials`. Memory index unavailable; used injected scoreboard/lessons + live search.

---

## ANALYSIS: Basic Materials (XLB) — 2026-08-31

### CHANNEL 2 RESEARCH FINDINGS

**1. Shared macro regime as it hits THIS sector — RISK-OFF overlay.**
This is the critical input. **US-Iran strikes near Hormuz** (Larak Island strike, Iran Guards claim retaliatory attack on US assets in Jordan) have oil up **+2%** (CL +1.97%, Brent ~$90). This is a **live, escalating geopolitical/oil supply-shock** — the exact pattern the 8/18 lesson targets: on geopolitical/oil risk-off days, the entire commodity complex (copper, gold, silver) co-moves DOWN with equities as risk assets are liquidated. Do NOT treat the copper/gold bid as a floor.

Compounding the risk-off: **Fed Chair Warsh signals rate hikes may be needed; September hike odds ~coin flip.** Treasury yields rising (10Y 4.67, 30Y 5.19). Hawkish Fed + rising yields = headwind for a cyclical, rate-sensitive materials book.

Broad tape is only mildly negative (ES −0.2%, NQ −0.15%), VIX calm at 15.22, Fear&Greed 58 (Greed). So this is a **moderate risk-off**, not a panic. But the oil/Hormuz escalation is the live sector-relevant driver.

**2. Sector SPINE factors (mandatory):**
- **Industrial metal price surge:** **OFF.** Copper +0.42% (firm but off the record), LME stocks have **rebuilt** to ~235-240kt from ~205kt mid-August — the acute tightness/backwardation has eased. Aluminum soft. Iron ore ~$95/t on China property. Not a surge day.
- **Inventory draw:** **OFF.** LME copper stocks rebuilt; backwardation narrowed (LME stocks jumped 63,000t per IndexBox).
- **China PMI / property demand:** **FRESH PRINT — improved but still contraction.** August official PMI **49.8** (up from 49.2 July), released this morning. CNBC: "shrank for a second straight month, though by less than market estimates." AP: "recovered slightly... overall manufacturing still in contraction." Property downturn continues to weigh. This is a **mild improvement, not a rebound** — still sub-50, still a drag on industrial metals demand.
- **Supply disruption:** Hormuz/oil is a supply shock but that's **oil**, not industrial metals. DRC concentrate ban is stale/narrow. Not a fresh metals supply shock.

**3. Sector secondary factors:**
- **Gold/silver (monetary metals):** **FADING, not green.** GC=F **−0.81%**, silver **−0.26%**. The 8/14 guardrail (gold green + USD weak → score the bid) does **NOT** pay today. Gold pressured as USD firms after PCE and on hawkish Fed. The monetary-metals offset is absent.
- **USD:** DXY +0.38% 1d (firming on hawkish Fed) — a mild headwind for the commodity complex.
- **Real yields:** DFII10 2.34, flat 1d — not a fresh easing impulse.
- **Critical minerals/tariffs:** Section 232 copper carried, not fresh.

**4. Breadth / leadership:** Prior session was **chemicals-led lag** (LIN/SHW/ECL) — the XLB book itself. No fresh same-morning breadth expansion; metals/miners split. Rotation is **out of materials** on a software/AI + risk-off tape.

**5. Flows / positioning:** XLB ~$53.18, RSI 50 (neutral), above 50-day avg $51.71. Mild outflows (~−$42M 5d, −$182M 1m). Not crowded, not a washout.

**6. Earnings/policy catalysts:** APD beat Q3 (single-name, already traded). No fresh sector-wide catalyst.

### SELF-AUDIT
- **Lens:** XLB environment, not SPX, not FCX/NEM single-name.
- **Band:** Live Hormuz/oil risk-off + hawkish Fed + China still contracting + gold fading → **down/mild**. Not notable (VIX calm, Fear&Greed Greed, broad tape only mildly negative, China PMI improved).
- **Skew:** Gold is NOT green today — the 8/14 offset is off. Do not let copper's +0.42% wash out the oil/Hormuz risk-off (8/18 co-move lesson).
- **Same-shock:** Hormuz/oil counted once in S0; not double-counted in S1.
- **Single-ticker:** APD beat is single-name, not a sector driver.

**Divergence:** Leading factors (S0 −1, S1 −1, S2 −1) and tape (S4 −1) **agree** — no divergence. Trust the factor stack.

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: risk_off
TOTAL_SCORE: -3.6
PREDICTED_DIRECTION: down
PREDICTED_MAGNITUDE_BAND: mild
HORIZON_3D: down:mild:0.5
HORIZON_1W: flat:mild:0.45
HORIZON_2W: flat:mild:0.4
HORIZON_1M: flat:mild:0.4
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-off tape / flight to safety|HIT|0.7|2026-08-31|US-Iran Hormuz strikes, oil +2%, Brent ~$90
Real yields rising|HIT|0.6|2026-08-31|Warsh hawkish, Sep hike odds ~coin flip, 10Y 4.67
USD strengthening|HIT|0.5|2026-08-31|DXY +0.38% 1d on hawkish Fed
China PMI / property demand rebound|MISS|0.6|2026-08-31|Aug PMI 49.8, improved but still contraction
Gold/silver price surge (monetary metals)|MISS|0.7|2026-08-31|GC=F -0.81%, silver -0.26% (fading)
Industrial metal price surge (copper/aluminum/iron ore)|MISS|0.6|2026-08-31|Copper +0.42% off record, LME stocks rebuilt
Inventory draw (LME/exchange stocks down)|MISS|0.6|2026-08-31|LME copper stocks rebuilt ~235-240kt
Sector rotation out of materials|HIT|0.6|2026-08-31|Chemicals-led lag, software/AI + risk-off tape
Sector breadth failure (ETF up, names flat)|HIT|0.5|2026-08-31|Prior chemicals-led lag, no fresh expansion
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': -7.0, 'divergence_flagged': False, 'total_score': -6.75, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.55, 'regime': 'risk_off'}
```
