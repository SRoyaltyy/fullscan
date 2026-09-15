# Sector Outcome — Consumer Defensive — 2026-09-15

Actuals: {'etf': 'XLP', 'pct': -0.8173357343965737, 'spy_pct': -0.45867813741702346, 'rel': -0.35865759697955024, 'open': 83.99500274658203, 'close': 83.7300033569336, 'source': 'yf_download'}

# Sector Post-Session Review — Consumer Defensive (XLP) — 2026-09-15

## 0. FACTS

**CLAIM:** XLP closed 2026-09-15 at $83.73, −0.82% on the day.
**URL:** https://stockanalysis.com/etf/xlp/
**PUBLISHED:** 2026-09-15 (after close)
**QUOTE:** "State Street® Consumer Staples Select Sector SPDR® ETF ETF, XLP 83.70 -0.72 -0.85% 09/15/2026 09:55 PM NYA"
**SUMMARY:** Confirms the deterministic feed: XLP −0.82% (feed −0.8173%), close $83.73, open $83.995. The tape opened essentially flat-to-slightly-down and bled through the session — a one-way drift, not a gap-and-recover.

**CLAIM:** SPY −0.46% on the day; XLP relative return −0.36%.
**URL:** (deterministic feed, Channel 1)
**PUBLISHED:** 2026-09-15
**QUOTE:** `ETF_PCT: -0.8173 | SPY_PCT: -0.4587 | REL_PCT: -0.3587`
**SUMMARY:** XLP underperformed SPY by ~36bp. This is the *opposite* of the 08-18 template the morning note explicitly invoked ("rising long-end yields + risk-off → relative outperformance / flat-to-negative absolute"). The absolute call was right; the relative lean was wrong in direction.

**CLAIM:** The 10Y Treasury yield breached 5% on 2026-09-15, highest since 2007, in a deepening global bond selloff ahead of the Fed decision.
**URL:** https://www.cnbc.com/2026/09/15/10-year-treasury-yield-rises-to-highest-since-2007.html
**PUBLISHED:** 2026-09-15
**QUOTE:** "The 10-year yield reached 5.041%, the highest level since July 2007. The yield on the longer-dated 30-year Treasury bond hit its highest level since June 2007."
**SUMMARY:** The morning note's central macro object — "10Y breaches 5%, highest since 2007" — was **correct and it was the day's dominant driver**. Bloomberg attributes the selloff to "booming capital investment and soaring energy prices," i.e. the same oil/stagflation overlay the note flagged. This is the rare case where the morning's #1 macro read was both right and load-bearing.

**CLAIM:** The bond selloff was global and energy-driven, not a US-only event.
**URL:** https://www.bloomberg.com/news/articles/2026-09-15/us-10-year-treasury-yields-rise-to-highest-level-since-2007
**PUBLISHED:** 2026-09-15
**QUOTE:** "the latest milestone in a bruising global bond selloff driven by booming capital investment and soaring energy prices"
**SUMMARY:** Confirms the note's "global bond selloff + oil >$103" framing as one rates/energy regime object — the note's decision to count it once was structurally sound.

**Path:** Open $83.995 → Close $83.73. No intraday high/low given, but the open-to-close decline of ~0.32% on top of a soft premarket (PM:XLP −0.33%) means XLP spent the day drifting lower with the long end. There is no evidence of a defensive bid materializing at any point.

---

## 1. WHAT DROVE THE SECTOR

**Primary driver: the duration channel, exactly as the morning note's S0 identified it — but it hit harder than the note's net score implied.**

XLP is a ~2.6%-yield bond-proxy. On a day when the 10Y printed 5.041% (highest since July 2007) and the 30Y hit its highest since June 2007, the discount-rate channel is not a "modest headwind" — it is the whole story. The note scored S0 at only −0.5 because it *partially credited* a theoretical flight-to-safety bid. That credit was the error: on a rates-led selloff, staples do **not** get the FTS bid, because the FTS bid in a rate shock goes to **cash and the front end**, not to a long-duration equity proxy. XLP's −0.36% relative is the market telling you that staples were treated as *duration*, not as *defense*.

**Secondary driver: energy as an input-cost shock, not an FTS tailwind.** WTI $103.79 / Brent $108.11 (+2.3%) is a freight/packaging/feedstock cost for staples with no offsetting pricing power print. The note's S1 = −1.0 correctly identified this. Bloomberg's "soaring energy prices" attribution confirms it was a live, market-recognized channel.

**What did NOT drive it:** no XLP constituent earnings, no CPI/PPI print (CPI was 09-11), no index event. This was a pure macro-regime day for the sector.

**Taxonomy alignment:** the note's own taxonomy hits — *Real yields rising* (HIT, 0.8), *Risk-off tape / flight to safety* (HIT, 0.7), *Input cost spike without pricing power* (HIT, 0.7), *Sector breadth failure* (HIT, 0.6) — were all correct. The taxonomy was read well. The **weighting** was wrong.

---

## 2. AUDIT OF MORNING S0–S4 READS

I use the morning numbers as written, not post-close rewrites.

### S0_SHARED_MACRO = −0.5 → **UNDERWEIGHTED. Should have been ≈ −1.5.**

The note wrote: *"The theoretical FTS bid (risk-off + oil >$103 + Asia red) is partially credited but dampened... Net modestly negative."* The dampening was correct in spirit but far too small in size. The note had **all the evidence it needed** to score this harder:

- It explicitly wrote *"the duration shock is the dominant channel for a bond-proxy"* and *"Do not upgrade to absolute up (08-18 utilities)."*
- It explicitly wrote *"XLP is a bond-proxy, not the clean haven today."*
- It then scored S0 at −0.5 anyway, because it credited a partial FTS bid.

**This is an internal contradiction in the morning note.** The prose says "duration is dominant, XLP is not the haven"; the score says "partially credit the FTS bid." When prose and score disagree, the prose was right. The correct S0 was roughly −1.5: full duration weight, zero FTS credit. The 08-18 template the note invoked is a *relative-outperformance* template — and the note itself flagged that XLP was "not the clean haven today," which should have killed the relative-outperformance expectation outright.

### S1_SECTOR_FACTORS = −1.0 → **CORRECT.**

Oil >$103 as an input-cost spike with no fresh pricing-power print, MAP HEAT internal read negative (PG drag, CL −2.72% w1, KO/PEP soft, COST friction, KR DOJ probe, STZ/TAP neg). This was the right call at the right size. The ag relief was correctly judged "mild" and not over-credited.

### S2_BREADTH = −0.5 → **CORRECT, arguably slightly light.**

The note correctly identified **breadth failure**: the only clean up-tape was Farm Products (ADM/BG — commodity-linked, not the defensive core), while HPC, Beverages, Confectioners, Grocery were down/flat with red captains. On a −0.82% day with the core sleeves red, −0.5 is defensible; −0.75 would have been better. Not a material error.

### S3_FLOWS_POSITIONING = 0 → **CORRECT.**

No confirmed flow print. The note's observation that the 09-14 +1.69% rel day had *partially relieved* the washed-out condition — shrinking reflex-bounce fuel — was a genuinely good read and it proved right: there was no bounce.

### S4_ETF_TAPE = 0 → **CORRECT, and the reasoning was the best part of the note.**

The note refused to copy the −0.25% 1d rel give-back as a fresh negative (08-28 rule) and refused to copy the +0.94%/+0.88% 3d/1w rel as fresh confirmation (already paid). Scoring S4 at 0 was disciplined and correct. The actual −0.36% rel is close enough to the sub-threshold give-back that S4=0 was the right neutral.

### MULTIPLIER = 0.9 / CONFIDENCE = 0.5 → **REASONABLE.**

FOMC pending, magnitude historically misses. Fine.

### The pipeline's own decision vs the LLM's

Note the divergence inside the morning packet: the **LLM wrote "down / mild"** with leading sum −2.0, while the **pipeline computed "flat / flat"** with total_score −5.776 and applied a `sector_rs_veto` (sector_rs_tape d1=1.44, w1=0.52) plus a `calendar_size_gate`. The pipeline's veto — using the *positive* 3d/1w relative tape to flatten the call — was the **wrong** application: it let an already-paid multi-horizon cushion veto a live, dominant, same-day duration signal. The LLM's "down/mild" was the better call and it was the one that hit on direction. **The veto mechanism cost the pipeline a direction hit.**

---

## 3. INTERACTIONS / DOUBLE-COUNT / KNOWABLE-AT-OPEN

**Double-count check:** The note's stated discipline — treat "10Y>5% duration shock + FOMC binary + oil >$103 as **one** rates/risk-off regime object counted once" — was correct and it held. It did not restack oil as both an FTS bid and an input-cost hit. Good.

**The real interaction error was the opposite of double-counting: it was *under*-counting.** By folding the duration shock, the FOMC binary, and oil into "one object" and then scoring that object at only −0.5, the note diluted a severe, live, same-day channel into a mild one. The consolidation was right; the magnitude assigned to the consolidated object was too small.

**Knowable-at-open test:** **YES — fully knowable.** Every input needed for the correct call was in the premarket packet:
- 10Y >5%, DGS30 5.35, DFII10 2.60 (+0.18 1m) — the duration shock was *already printed*.
- PM:XLP −0.33%, mid-pack of eleven — the FTS bid was *already absent* in the premarket.
- Oil >$103 — *already printed*.
- MAP HEAT internal read negative — *already known*.

The note had the correct answer in its own prose ("XLP is a bond-proxy, not the clean haven today") and then scored against it. **This was not an information failure; it was a weighting failure.** The knowable-at-open verdict is unambiguous: a −0.8%/−0.4% rel day was the base case, not a surprise.

**One thing that was genuinely unknowable:** the *exact* magnitude of the long-end move (5.041% vs "above 5%") and whether the selloff would accelerate intraday. But the *sign* and the *relative direction* were knowable.

---

## 4. OUTLIERS INSIDE THE SECTOR

- **Farm Products (ADM +3.37%, BG +2.94% w1)** — the only clean up-tape, and it is commodity-linked, not defensive-core. This is consistent with the energy/ag complex being the day's live theme; it is *not* evidence of a staples bid. The note correctly refused to read it as breadth expansion.
- **Orange juice +5.30%** — a softs outlier in the morning ag panel; too small a sleeve to move XLP, but worth noting as the one ag input that spiked *against* the "ag relief" thesis.
- **Lean hogs −2.36%** — the one sharp ag decliner; again immaterial to XLP.
- **No XLP constituent was a same-day outlier** — no earnings, no guidance, no M&A. The sector moved as a bloc on macro, which is itself the diagnostic: **when a defensive moves as a bloc on a rates day, it is trading as duration, not as defense.**

---

## 5. VERDICT

The morning note got the **direction right** (down), the **magnitude right** (mild — −0.82% is mild), the **primary driver right** (duration shock), and the **internal breadth read right**. It got the **relative lean wrong** (predicted flat-to-slightly-negative; actual −0.36%, a clear underperformance) and it **under-weighted S0** by roughly 1.0 point because it credited an FTS bid its own prose had already ruled out.

The single lesson: **when the note's prose says "XLP is a bond-proxy, not the clean haven today," the score must follow the prose.** The 08-18 relative-outperformance template does not apply when the sector is the duration instrument in a duration shock. The pipeline's `sector_rs_veto` compounded this by letting a stale, already-paid multi-horizon cushion flatten a live same-day signal — that veto should not fire when the same-day macro object is the dominant driver.

OUTCOME_BEGIN
SECTOR: Consumer Defensive
ETF: XLP
ETF_PCT: -0.82
SPY_PCT: -0.46
REL_PCT: -0.36
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: 10Y yield breach of 5% (highest since 2007) in a global bond selloff — XLP traded as a long-duration bond-proxy, not as a defensive haven
KEY_INTERACTION: Duration shock + oil >$103 + FOMC binary correctly consolidated into one rates/risk-off object, but that object was scored at only −0.5 when its live, same-day, dominant nature warranted ≈ −1.5
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction, magnitude, and primary driver all correct; relative lean wrong (predicted flat-to-slightly-negative vs actual −0.36% underperformance) because S0 credited an FTS bid the note's own prose had already ruled out — a weighting failure, not an information failure
OUTCOME_END