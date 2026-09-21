# Sector Outcome — Real Estate — 2026-09-21

Actuals: {'etf': 'XLRE', 'pct': 0.14108011994635739, 'spy_pct': 1.5518133737258744, 'rel': -1.410733253779517, 'open': 42.349998474121094, 'close': 42.59000015258789, 'source': 'yf_download'}

# Sector Post-Session Review — Real Estate (XLRE) — 2026-09-21

## 0. FACTS

**Channel 1 actuals (deterministic, injected):**

| Item | Value |
|---|---|
| XLRE open | 42.35 |
| XLRE close | 42.59 |
| XLRE % | **+0.141%** |
| SPY % | **+1.552%** |
| Relative (XLRE − SPY) | **−1.411%** |
| Actual direction | **up** (barely) |
| Actual magnitude | **flat** (absolute) / **notable** (relative) |

Path: XLRE opened at 42.35 and closed at 42.59 — a **+0.24 intraday drift off the open**, i.e. a slow grind higher, not a gap-and-fade. The open itself was essentially flat vs. the prior close (the morning tape printed PM:XLRE −0.05%, and the cash open landed at a level consistent with that). So the day's shape was: **flat open → mild positive drift → close +0.14%**, while SPY ran +1.55%.

**The single most important fact of the session:** XLRE was **up in absolute terms** and **badly down in relative terms**. This is the exact "risk-on leaves REITs lagging" configuration the morning note flagged as the skew — and it is the configuration that the morning *direction* call got right and the morning *magnitude* call got right, while the *relative* story (which the note correctly identified as the real risk) played out at roughly 1.4% of underperformance.

**Cross-check on the tape (search, same session):**
- CLAIM: Nasdaq was the day's leader, +2.28%; 17 of 18 global indices finished positive. URL: https://theclosereport.com/ PUBLISHED: 2026-09-21 (post-session). QUOTE: "Best performer was Nasdaq (+2.28%) and the biggest decliner was IDX Composite (-0.88%)." SUMMARY: Confirms a broad risk-on, tech-led session — the regime the morning note called "Monday risk-on / oil-slide / post-FOMC digestion."
- CLAIM: A rally in technology giants sent stocks to their best day since early August, with oil sinking on diplomatic hopes. URL: https://www.bloomberg.com/news/articles/2026-09-20/us-stock-futures-up-ahead-of-talks-dollar-steady-markets-wrap PUBLISHED: 2026-09-20/21. QUOTE: "A rally in technology giants sent stocks to their best day since early August, with the market also gaining as oil prices sank on hopes for diplomatic efforts to end the war in Iran." SUMMARY: The driver was **tech beta + oil slide on de-escalation hopes** — not a rates-relief bid into duration. That is precisely the "funding source" dynamic the morning note named.
- CLAIM: Trump preparing to meet Xi Jinping; markets coming off a volatile week. URL: https://www.thestreet.com/stock-market-today/stock-market-today-dow-jones-sp-500-nasdaq-updates-sept-21-2026 PUBLISHED: 2026-09-21. SUMMARY: Geopolitical de-escalation (Iran) + trade-talks optimism = risk-on impulse, again not a REIT-specific catalyst.
- CLAIM: Mid-cap REIT momentum building; JLL/DOC among large/mega-cap REITs with strong momentum; 4 REITs with A+ momentum grades lead small-caps. URLs: Seeking Alpha items, PUBLISHED 2026-09-21 15:30–15:40 GMT. SUMMARY: There *was* REIT-specific momentum chatter intraday, but it was **stock-picking/momentum-screener content**, not a sector-wide flow event — consistent with XLRE's flat absolute print despite the chatter.

**Reconciliation:** XLRE +0.14% vs SPY +1.55% → rel −1.41%. The morning Channel 1 lag table (1d rel −1.08%, 3d −2.07%, 1w −1.96%, 1m −4.76%) **extended** by roughly another 1.4% on the day. The relative-laggard thesis was not just confirmed — it was the dominant feature of the session.

---

## 1. What drove the sector today

**Primary driver: risk-on rotation OUT of defensives/duration into tech beta, with REITs as the funding source.** This is the taxonomy-aligned "sector rotation out of real estate" factor (HIT in the morning grid, scored once in S1) plus "risk-on tape / equity beta expansion" (HIT, 0.72). The two are the same object viewed from opposite ends: money left rate-sensitive defensives (XLRE, XLU, XLP) and went to XLK/NQ.

**Secondary driver: the rate object was a non-event.** The morning note's central macro judgment was that the live 10Y at 4.967% (−3 bp) off the 5.041% 19-year high was **stabilization, not relief** — and that the 30Y at 5.306% remained in the ≥5.15% stress zone. Today's session validated that read: a 3 bp easing did **not** produce a duration bid in REITs. XLRE's +0.14% is roughly what you'd expect from a sector with no positive catalyst being carried slightly higher by a +1.55% SPY tape while simultaneously being sold as a funding source. The rate channel was correctly scored as **neutral (S0 = 0)** — it neither helped nor hurt in a way that dominated.

**Tertiary: oil slide was correctly NOT booked as REIT relief.** WTI −1.59% / CL −5.94% on de-escalation hopes. The morning note explicitly refused to double-count oil and the yield dip as two separate duration-relief objects (08-11 spike OFF; count once). Today confirmed: oil down + yields down 3 bp produced **no** REIT bid. Had the note booked the oil slide as S1 duration relief, it would have been wrong twice over.

**What did NOT drive it:** No REIT earnings. No data-center or industrial catalyst (both correctly marked STALE). No refinancing-window news (correctly MISS). No index event (correctly MISS). The office/mortgage/specialty nested-down sleeves did not drag the parent — consistent with the instruction not to let EQIX/BXP define XLRE.

---

## 2. Audit of morning S0–S4 reads against reality

**S0_SHARED_MACRO = 0.** **Verdict: CORRECT.** The morning note's core S0 judgment was that this was a risk-on/oil-slide/post-FOMC-digestion tape, not a rates-backup session and not a flight-to-safety bid; that the FOMC hike was T+3 printed and must not be restacked; that the live −3 bp was stabilization not relief; and that ES=F +1.35% was a weekend gap, not a second tape. Every one of those held. The one thing S0 arguably under-weighted: the *magnitude* of the risk-on impulse (SPY +1.55%, NQ +2.28%) was larger than the Finviz ES +0.20% / NQ +0.41% pre-open board implied. But S0 is a *sign* score, and the sign was right — risk-on with REITs as funding source nets to zero for the sector's absolute direction. **No change warranted.**

**S1_SECTOR_FACTORS = −0.5.** **Verdict: CORRECT, and arguably the best-scored line of the morning.** The note scored "sector rotation out of real estate" as HIT, live, once — and refused to also use it as an S0 offset. It also refused to promote the stale data-center/industrial HITs into same-day up votes, and refused to let the XLRE −0.05% PM tick set sign (09-14 rule). The result: a modest negative S1 that correctly captured the *relative* drag without forcing an absolute down call. XLRE closed +0.14% absolute / −1.41% relative — exactly the "modest negative factor score, flat absolute" outcome S1 = −0.5 implies. **No change warranted.**

**S2_BREADTH = 0.** **Verdict: CORRECT.** The note flagged MAP HEAT as split (Hotel/Residential up vs Office/Mortgage/Specialty down), invoked the nested-OVERRIDE-beats-parent rule, and explicitly refused to dump the 1w/1m lag into both S2 and S4 (09-11 rule). It also correctly treated the ~3% above-20d-SMA print as structural, not a same-morning breadth smash. Today's session gave no evidence of a breadth *event* in either direction — XLRE's flat absolute print is consistent with a split internal map. **No change warranted.**

**S3_FLOWS_POSITIONING = −0.5.** **Verdict: CORRECT.** Recent 5d/1m net outflows (−$136M / −$328M), 1y flows negative, not a crowded long. The note scored this as modest negative near-term demand, not a washout buy. Today's relative underperformance is consistent with continued outflow pressure / absence of incremental REIT demand while SPY absorbed +1.55%. **No change warranted.**

**S4_ETF_TAPE = −1.** **Verdict: CORRECT on sign, and this is where the morning earned its keep.** The 1d rel −1.08% hard lag was the freshest, most reliable input, and the note used it as confirmation (S4 = −1) while explicitly refusing to *force down* off Friday's print (09-18 joint gate did NOT fire because there was no live failed round-number hold). That distinction — "hard lag present, but no live failed-hold, so don't force down" — is exactly right. XLRE did not go down; it went up 0.14% while lagging 1.41%. A forced-down call would have been a direction MISS. **No change warranted.**

**Divergence check.** The note reported leading S0–S3 = −1.0 and S4 = −1, "they agree, no leading-vs-tape fight." That was accurate and it mattered: because the factors and the tape agreed on *relative* weakness but the anchor (ES +1.35%, ZN −0.03%, PM:XLRE −0.05%) pulled the absolute call toward flat, the engine landed on **flat/flat** — which is precisely what happened in absolute terms. The divergence_flagged=True in the pipeline JSON reflects the anchor-vs-factor tension, and the resolution (trust factors for sign, anchor for magnitude) produced the right absolute call.

**Direction verdict: HIT.** Predicted flat, actual +0.14% — inside any reasonable flat band.
**Magnitude verdict: HIT.** Predicted flat band, actual flat absolute magnitude.
**Relative verdict: the note's stated skew ("risk-on leaves REITs lagging on a relative basis") was the day's headline outcome at −1.41%.**

This is a clean **dir HIT / mag HIT** after a run of dir MISS / mag MISS on 09-16, 09-17, 09-18. The open `sector_real_estate` experiment — "when score sign conflicts with tape/breadth, cut conviction, prefer flat/mild; shrink confidence on modest |score|" — was applied and it worked.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count audit — did the morning avoid stacking the same object?**
- **Oil slide + 10Y −3 bp:** counted **once** as "not relief" (S0 = 0), explicitly not booked as positive S1 duration relief. ✅ Correct — neither produced a REIT bid.
- **FOMC hike:** treated as T+3 printed, scored 0, not restacked. ✅ Correct — no FOMC-driven move today.
- **ES=F +1.35% weekend gap:** capped, not promoted into an up vote (09-17 rule). ✅ Correct — the gap was already in Friday's cash close; today's SPY move was a *new* +1.55% session, and XLRE captured almost none of it.
- **1w/1m lag:** not dumped into both S2 and S4. ✅ Correct.
- **Sector rotation out of RE:** scored once in S1, not also as an S0 offset. ✅ Correct.
- **XLRE −0.05% PM tick:** scored 0, did not set sign (09-14 rule). ✅ Correct — the cash open was flat and the close was +0.14%; the PM tick had no predictive content.

**Knowable-at-open test.** Was the day's outcome knowable at the open? **Partially — and the morning captured the knowable part.**
- *Knowable:* the relative-laggard configuration (every horizon red), the risk-on regime (ES green, NQ leading, oil offered), the absence of any REIT-specific catalyst, the split internal map, the outflow backdrop, and the fact that a 3 bp yield dip off a 19-year high is not relief. All of these were in hand pre-open and all pointed to "flat absolute, lagging relative."
- *Not knowable:* the **size** of the risk-on impulse. The pre-open board showed Finviz ES +0.20% / NQ +0.41%; the actual session delivered SPY +1.55% / NQ +2.28%. That is a ~4–5x larger risk-on move than the pre-open board implied. The morning note correctly refused to promote the leftover ES=F +1.35% into a sector up vote — but in hindsight the *actual* session was closer to the ES=F magnitude than to the Finviz magnitude. This is the one place where the note's caution (correctly) cost it the ability to predict the *relative* magnitude more precisely. It did not cost it the direction or the absolute magnitude call.
- *The honest read:* the morning got the **shape** right (flat absolute, lagging relative) and the **sign** right, but under-estimated the **intensity** of the risk-on rotation. That is a magnitude-of-relative-underperformance miss, not a direction miss — and the note's own skew paragraph flagged exactly this risk ("risk-on leaves REITs lagging on a relative basis").

**Interaction that mattered most:** the anchor (ES +1.35% / ZN −0.03% / PM:XLRE −0.05%) vs. the factor sum (−1.5 leading). The anchor said "flat-to-slightly-up"; the factors said "modestly negative." The engine resolved this to **flat/flat** with divergence_flagged=True. In reality, the anchor's *direction* was right (XLRE up) and the factors' *relative* signal was right (XLRE lagged). The divergence flag was the correct epistemic state: two inputs pointing at different aspects of the same outcome, not a contradiction.

---

## 4. Outliers inside the sector

- **The parent itself is the outlier.** XLRE +0.14% against SPY +1.55% is a −1.41% relative print on a day when 17 of 18 global indices were green and Nasdaq ran +2.28%. A sector that is *up* on an overwhelmingly risk-on day but lags by 1.4% is the defining outlier of the session — it is the "funding source" signature in its purest form.
- **Mid-cap / small-cap REIT momentum names.** The Seeking Alpha items (15:30–15:40 GMT) flag mid-cap REIT momentum building and 4 small-cap REITs with A+ momentum grades. If those names outperformed, they did so *inside* a flat parent — consistent with the MAP HEAT split (Hotel/Residential nested up) and with the note's instruction not to let single names define XLRE. This is a **sub-type dispersion** story, not a parent story.
- **JLL / DOC (large/mega-cap momentum).** Same read: stock-level momentum chatter, not a sector flow event. The note's bar on WELL/EQIX/PLD/BXP defining the parent applies equally here.
- **Data-center / industrial sleeves (EQIX, DLR, PLD).** Correctly marked STALE and nested (Specialty down, Industrial flat). No evidence they drove the parent today. The note's refusal to promote stale DC/industrial HITs into same-day up votes was validated — if anything, the parent's flat print suggests those sleeves did not rescue the sector.
- **Office / Mortgage (BXP, SLG).** Nested down, ~17.8% national vacancy, structural. No same-day catalyst; correctly excluded from the parent call.

**No single-name outlier appears to have driven the parent.** The dispersion was internal and consistent with the split map.

---

## 5. Verdict and lessons

**The morning call was correct on direction and magnitude, and correct on the relative skew that turned out to be the day's dominant feature.** After three consecutive dir-MISS/mag-MISS sessions (09-16, 09-17, 09-18), the experiment's discipline — cut conviction when score sign conflicts with tape/breadth, prefer flat/mild, shrink confidence on modest |score|, refuse to force down off a stale lag without a live failed-hold — produced a clean HIT.

**What worked:**
1. Refusing to force down off Friday's −0.95% print without a live failed round-number hold (09-18 gate correctly did NOT fire). XLRE went *up*; a forced-down call would have missed.
2. Refusing to force up off the leftover ES=F +1.35% weekend gap (09-17 rule). XLRE captured almost none of the risk-on impulse.
3. Counting the oil slide and the yield dip **once** as "not relief." Neither produced a REIT bid.
4. Treating the FOMC hike as T+3 printed and scoring it 0.
5. Respecting the split MAP HEAT and barring single names from the parent call.
6. Scoring the "rotation out of real estate" factor once in S1 and not double-counting it as an S0 offset.

**What to carry forward:**
- The **relative-laggard configuration** (every horizon red + risk-on regime + no sector catalyst + outflows) is a reliable *relative* short signal even when the absolute call is flat. The note's skew paragraph was the most valuable line in the document. Future RE sessions in this regime should keep the skew explicit and consider whether the magnitude band for *relative* underperformance deserves its own expression.
- The **pre-open board understated the risk-on impulse** (Finviz ES +0.20% vs. actual SPY +1.55%). This is a known limitation of the Finviz snapshot vs. the ES=F weekend gap. The note handled it correctly by capping ES=F — but the lesson is that on a Monday after a strong Friday, the *actual* session can run closer to the ES=F magnitude than to the Finviz magnitude. That argues for keeping the absolute call flat (as done) while acknowledging the relative drag may be larger than the lag table alone implies.
- **Do not re-litigate the FOMC.** It is now T+5. The October hike ~45–57% remains path, not a 1d object.

**Scorecard for the session:**
- Direction: **HIT** (flat → +0.14%)
- Magnitude: **HIT** (flat → flat absolute)
- Relative skew: **HIT** (lagging → −1.41%)
- Double-count discipline: **clean**
- Knowable-at-open: **partially** — shape and sign knowable; intensity of risk-on not knowable from the pre-open board

---

OUTCOME_BEGIN
SECTOR: Real Estate
ETF: XLRE
ETF_PCT: 0.141
SPY_PCT: 1.552
REL_PCT: -1.411
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: Risk-on rotation out of rate-sensitive defensives into tech beta (NQ +2.28%), with REITs as the funding source; the 3 bp yield dip off the 19-year high was stabilization, not duration relief, and produced no REIT bid.
KEY_INTERACTION: Anchor (ES +1.35% / ZN −0.03% / PM:XLRE −0.05%) pulled the absolute call to flat while the factor sum (−1.5 leading) correctly captured relative weakness — divergence_flagged=True resolved to the right absolute call and the right relative skew.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Clean dir HIT / mag HIT — flat/flat called correctly, relative-laggard skew (−1.41%) was the day's dominant feature and was explicitly flagged pre-open; only the intensity of the risk-on impulse was under-estimated.
OUTCOME_END