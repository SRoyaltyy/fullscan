# Sector Outcome — Healthcare — 2026-09-23

Actuals: {'etf': 'XLV', 'pct': -0.1242479509515837, 'spy_pct': -0.7202161019229769, 'rel': 0.5959681509713932, 'open': 169.9250030517578, 'close': 168.8000030517578, 'source': 'yf_download'}

# Sector Post-Session Review — Healthcare / XLV — 2026-09-23

## 0. FACTS

**Channel 1 (deterministic actuals):**

| Metric | Value |
|---|---|
| XLV open | 169.925 |
| XLV close | 168.800 |
| XLV % | **−0.124%** |
| SPY % | **−0.720%** |
| **Relative (XLV − SPY)** | **+0.596%** |
| Actual direction | **down** (absolute), **up** (relative) |
| Actual magnitude | **flat** (absolute), **notable** (relative) |

Path: XLV opened at 169.925 (essentially flat vs the 09-22 close of 169.89 — consistent with the injected PM print of −0.01%), then drifted down through the session to close at 168.800. SPY fell harder (−0.72%), so XLV's absolute decline of ~12 bp translated into a **+0.60% relative outperformance** — a defensive-rotation day, not a healthcare-specific down day.

**Cross-check:** Business Insider / stoxline quote panel shows XLV **168.80, −1.09, −0.64%** as of 09/23/2026 22:15 NYA. The −0.64% figure is the **cumulative move from the prior close including the ex-dividend adjustment** (XLV went ex-distribution $0.6422 on 09-21, payable 09-23). The deterministic pipeline's −0.124% is the clean price return used for grading; the −0.64% panel number embeds the distribution mechanics. I flag this because it matters for the audit: **the sector did not "fall 0.64%" on the day — it fell ~12 bp on price and paid a distribution.**

---

## 1. WHAT DROVE THE SECTOR

**Primary driver: defensive relative bid inside a down-tape, not a healthcare catalyst.**

The taxonomy-aligned read:

- **Risk-off tape / flight to safety — partial HIT in relative terms.** SPY −0.72% with XLV −0.12% is the classic defensive-rotation signature. Healthcare is a low-beta, domestic-revenue, non-cyclical sleeve; on a red SPY day it outperforms by construction. The HIT_GRID scored "Risk-off tape / flight to safety" as **MISS** at 0.70 confidence — that was scored on the *absolute* direction (XLV was down, so a naive "flight to safety = XLV up" test fails). But the **relative** outcome (+0.60%) is exactly the flight-to-safety transmission. This is a **grid-definition miss, not a thesis miss** — the grid's binary was absolute, the sector's behavior was relative.

- **USD strengthening — HIT (macro), weak transmission.** DXY +0.38% 1d was correctly flagged as a macro HIT with weak XLV transmission (domestic-heavy book). No evidence it moved XLV either way today.

- **No same-session sector catalyst fired.** The morning card's exhaustive check held up: no CMS rate surprise, no mega-cap Rx headline, no IRA print (final offers Sep 30), no FDA breadth event. The single-name items (AMGN OASIZ-301 T+1, Ionis zilganersen, Achieve CRL, Disc bitopertin CRL) were correctly classified as non-dominant. **None of them show up in the XLV tape today.**

- **Oil-offered / real-yield dip:** correctly zeroed in the morning. No transmission visible.

**Net:** the sector's absolute −0.12% is **beta to a down SPY tape**, and its +0.60% relative is **defensive rotation**. There was no healthcare-specific driver in either direction.

---

## 2. AUDIT OF MORNING S0–S4 READS

The morning card scored **S0=S1=S2=S3=S4=0**, multiplier 0.8, leading_sum 0.0, no divergence, and — critically — the **LLM overlay was 0.0** while the **deterministic engine emitted predicted_direction = up / mild** off `index_carry 0.573` (general_total 2.293) and a tape_anchor of −0.01. That is the central audit finding: **the factor card said flat, the engine said up/mild, and the engine's up call was wrong on absolute direction.**

**S0 (shared macro) = 0 — CORRECT.**
The card refused to score Finviz ES +0.20% / NQ +0.41% as a beta certificate, refused to map Nasdaq-record/XLK into S0=+1, refused to restack the paid real-yield dip, and refused to score oil-falling as rotation. Reality: SPY closed **−0.72%**. The pre-open "modest green, NQ leading" panel was a **false positive** — exactly the conflict class the card flagged (ES=F +0.03% / NQ=F −0.10% vs Finviz green). **S0=0 was the right call and the engine's index_carry 0.573 was the wrong one.**

**S1 (sector factors) = 0 — CORRECT.**
No same-morning CMS/IRA/FDA/mega-cap Rx HIT. AMGN OASIZ-301 correctly held as T+1/paid/single-name and barred from dominating. The card's discipline here was validated: had it scored S1+ off the AMGN print, it would have overfit a paid catalyst into a day where XLV did nothing sector-specific.

**S2 (breadth) = 0 — CORRECT, with a caveat.**
The card excluded the nested Facilities OVERRIDE (dir=up, conv=high) and treated MAP HEAT as split sleeve noise. Given XLV's flat absolute print, there is no evidence of breadth expansion or failure. The caveat: the card did not have same-session breadth data, and the flat print is consistent with "names mixed, ETF flat." No error.

**S3 (flows) = 0 — CORRECT.**
1m rel −3.27% = not crowded-long; trailing flows modestly negative; no volume spike. Nothing in the outcome contradicts this. The ex-div on 09-21 (payable 09-23) is mechanical and was correctly excluded from S1.

**S4 (tape) = 0 — CORRECT, and this is the most important validation.**
The card explicitly refused to leak Monday 09-21's 1d rel −0.80% into S4, noting 09-22 had already reversed it (+0.52% / rel +0.54%) and that live PM −0.01% confirmed flat. Reality: XLV opened flat and closed −0.12% absolute / **+0.60% relative**. The card's "flat" S4 read was **directionally right on the absolute and right on the relative sign** (it did not predict the relative outperformance, but it did not predict relative weakness either). **S4=0 was correct.**

**Verdict on the factor card:** S0–S4 = 0/0/0/0/0 was **substantially correct**. The card's own self-audit — "Band = unsigned card + size_gate → flat, not mild" — was the right magnitude call. The card's **explicit instruction to trust the factor card (flat) over the engine's leftover ES tape_anchor / leftover RS veto** was **vindicated**: the engine's up/mild was wrong, the card's flat was right.

---

## 3. INTERACTIONS / DOUBLE-COUNT / KNOWABLE-AT-OPEN

**Double-count check:** The card scored oil once and zeroed it (09-11 trigger not met); real yields not restacked into S0 and S1; single-tickers barred from the ETF call; nested Facilities excluded. **No double-count found.** The only "double-count" risk was the engine's `index_carry 0.573` — a general-market carry signal (general_total 2.293) that is **not a healthcare factor** and should not have driven an XLV up/mild call. That is a **cross-object leakage** issue, not a within-card double-count.

**Knowable-at-open test:**
- The **down SPY tape** was **not knowable at open** — pre-open panels were green (Finviz ES +0.20%) or mixed (ES=F +0.03% / NQ=F −0.10%). The card correctly refused to lean either way. **Partially knowable** at best.
- The **defensive relative bid** was **knowable at open** in the sense that XLV's low-beta/domestic profile is structural — but the card did not (and should not) have predicted a +0.60% relative outperformance from a flat PM print. **Not knowable** as a magnitude.
- **No same-session healthcare catalyst** was knowable and none fired. **Correctly knowable as absent.**

**The one thing that WAS knowable and was mis-weighted:** the engine's `index_carry 0.573` (general 2.293) was a **stale general-market carry** that the card explicitly warned against ("If the engine tries leftover ES tape_anchor or leftover RS veto, trust this factor card (flat) over tape"). The engine did not heed its own card. **This is the audit's actionable finding.**

---

## 4. OUTLIERS INSIDE THE SECTOR

Without same-session constituent data in the injected panel, I can only flag the **structural** outliers the card identified and check them against the flat ETF print:

- **AMGN (~3.4% weight):** OASIZ-301 was T+1/paid. If AMGN gave back part of its +3.5–4% on 09-23, that would be a **mild absolute drag** consistent with XLV −0.12%. Not confirmable from injected data, but the card's "must not dominate" rule held — XLV did not move like an AMGN-driven tape.
- **XBI / biotech:** XBI +2.30% on 09-22 was paid. No evidence of a same-session biotech leadership or funding-winter event. The duration sleeve is a minority XLV weight; its behavior is not visible in the flat ETF print.
- **Facilities (nested OVERRIDE, dir=up conv=high):** correctly excluded. If facilities names were up, they were too small a weight to move XLV off flat.
- **Diagnostics (DGX/LH CMS-payment chatter):** flagged as nested, not XLV spine. Consistent with no visible ETF impact.

**No outlier dominated the ETF.** The flat absolute print is the signature of a **balanced internal tape** — which is exactly what the card's S2=0 predicted.

---

## OUTCOME

OUTCOME_BEGIN
SECTOR: Healthcare
ETF: XLV
ETF_PCT: -0.124
SPY_PCT: -0.720
REL_PCT: +0.596
ACTUAL_DIRECTION: down (absolute) / up (relative)
ACTUAL_MAGNITUDE: flat (absolute) / notable (relative)
PRIMARY_DRIVER: Defensive relative bid inside a down SPY tape; no healthcare-specific catalyst fired
KEY_INTERACTION: Engine's stale general index_carry (0.573) overrode the factor card's flat read and produced a wrong up/mild call; card's S0–S4=0 was correct
KNOWABLE_AT_OPEN: partially (down tape not knowable; defensive relative profile structural but magnitude not knowable)
MORNING_READ_VERDICT: Factor card substantially correct (flat, no divergence, no catalyst); deterministic engine's up/mild was wrong on absolute direction — trust-the-card instruction vindicated
OUTCOME_END

---

## METHODOLOGY NOTES

1. **The factor card beat the engine.** S0–S4 = 0/0/0/0/0 with multiplier 0.8 and leading_sum 0.0 was the right read. The engine's `predicted_direction: up / mild` off `index_carry 0.573` was a **cross-object leakage** — a general-market carry signal driving a sector call. The card's explicit warning ("trust this factor card (flat) over tape") should be promoted from a note to a **hard gate**: when leading_sum = 0 and divergence = false, the engine should not emit a signed direction off index_carry alone.

2. **Grid definition issue.** "Risk-off tape / flight to safety" scored MISS on absolute direction, but the sector's **relative** behavior (+0.60%) was exactly flight-to-safety. The grid should either (a) score defensive-rotation HITs on **relative** return, or (b) add a separate "defensive relative outperformance" row. As written, the grid cannot credit the correct thesis.

3. **Ex-dividend contamination.** The −0.64% panel print vs the −0.124% clean price return is a **grading hazard**. The pipeline's deterministic actuals correctly use clean price return; downstream consumers reading quote panels will see −0.64% and mis-grade. Flag ex-div dates in the actuals feed.

4. **Open experiment status.** The 09-22 loss (dir MISS on down/mild vs actual +0.52%) drove the "prefer flat/mild + shrink confidence when factor sign fights tape" rule. Today the factor sign did **not** fight the tape (both flat), and the card correctly stayed flat. The experiment **applies and is working** — but the engine did not honor it. The experiment should be extended to **suppress engine index_carry when leading_sum = 0**.

5. **Rolling accuracy.** HC dir=0.4 mag=0.6 (n=10); last-30 dir=0.5 mag=0.318 (n=22). Today's flat/mild call is a **magnitude HIT** (flat) and a **direction MISS on absolute** (down) but a **direction HIT on relative** (up). Net: consistent with the ~0.5 dir / ~0.3 mag rolling profile — the sector is hard to call directionally on mixed empty-S1 days, which is precisely why the card's flat band was the right default.