# Sector Outcome — Consumer Cyclical — 2026-09-23

Actuals: {'etf': 'XLY', 'pct': -1.407824809582947, 'spy_pct': -0.7202161019229769, 'rel': -0.6876087076599702, 'open': 111.70500183105469, 'close': 110.6500015258789, 'source': 'yf_download'}

# Sector Post-Session Review — Consumer Cyclical (XLY) — 2026-09-23

## 0. FACTS

| Item | Value |
|---|---|
| XLY % | **−1.408%** |
| SPY % | **−0.720%** |
| Relative % | **−0.688%** |
| Open | 111.705 |
| Close | 110.650 |
| Path | Open 111.705 → close 110.650; XLY opened **above** prior close (112.33 on 09-22) and sold off through the session. Intraday range ≈ 1.06 pts, i.e. the entire move was a one-way grind lower from the open, not a gap-and-reverse. |

**ACTUAL_DIRECTION: down. ACTUAL_MAGNITUDE: notable** (XLY −1.41% vs SPY −0.72%; a ~0.69 pt relative underperformance on a down tape is a real sector-specific drag, not noise).

Morning card called **flat / flat** with total_score 1.029, all five components 0, confidence 0.42, regime mixed, divergence_flagged False. That is a **direction MISS** and a **magnitude MISS** — the card was unsigned and the session was signed down and notable.

---

## 1. What actually drove the sector

**Primary driver: a broad risk-off session with XLY as a high-beta, high-multiple casualty — plus a sector-specific leg from the two mega-caps that are ~40–45% of the ETF.**

Evidence:

- CLAIM: The session was a broad down day led by Nasdaq, with S&P 500 down ~0.6% at midday and the decline broad.
  URL: https://news.google.com/rss/articles/CBMiygFBVV95cUxPQXpYZUkydldLSnptWk40cVB1M3BYdVhrNnp1MEdqelhHMm1lbHphbF90QVpMZ2FiQTE4elhaclZFZ3hqUF9jSjk2M25iUUtKblJsU0Y1QXlITDlNa1VBeGxsLVdkakw5NVBmb3lOZWpvSUVyQ1lzaHRoSUlNLTZSZ3kxbUw2RXRkQ2tPZlRZR0FxVDRqbDRqS0lpTGtsNzRSeUJCWGVXeTZZckc2Rnp4SkNBSURZdTExRmNTQ0hIa2E3YUZsT3MyOW93
  PUBLISHED: 2026-09-23 16:07 GMT
  QUOTE: "S&P 500 Slides 0.6% at Midday as Nasdaq Leads Broad Decline"
  SUMMARY: Confirms the tape was down and Nasdaq-led — the classic configuration in which XLY (AMZN + TSLA ≈ 40–45%) underperforms SPY.

- CLAIM: A geopolitical shock — Trump threatening to "annihilate" Iran at UNGA — hit futures and the session.
  URL: https://news.google.com/rss/articles/CBMikwJBVV95cUxNZnJESGowQVpLQ0JfSGF6VlU0S0YtTGNkSG5KUkxkV3hwWUxxM2NleFpkWG82bjFXZVR4RmZlUWJoQUEtUXQ2UjVqcVFmalNvb3AxRGFJdWZIVXVWTWk1TTJNTU9pYkdBOXNtYUMxOUtBUFpPM1gwbkxDVUJlNmprQ21ISVVIbkJGamlSMnBOTy1DOTY5U0loalFWaVZESEp1RWItVTBTa01lNmYtaDRrWWdwVk1CT01tMXZmUDBONnk4dnpXQ3hkemVnUFJ2aUpLcGZRcXBfSDNrb1BJYkktejZ3R2k3SjZrazJSLUJJeUY2TWtBbVpBOGRWdE5SQ20zQnU1SkJJc2MzSk0zRW01U0Nvdw
  PUBLISHED: 2026-09-23 12:31 GMT
  QUOTE: "Dow, Nasdaq, S&P 500 Futures Drop as Trump Threatens to 'Annihilate' Iran at UNGA"
  SUMMARY: A **live, knowable-at-open** geopolitical headline that was NOT in the morning card's S0. This is the single most important audit finding.

- CLAIM: Oil "steadied" and markets were watching a looming Trump–Xi meeting; Dow/S&P/Nasdaq slipped.
  URL: https://news.google.com/rss/articles/CBMid0FVX3lxTE5DaXVwc1V1d0tZSHlEeC1qVGNfRTg4Y3RrUGJINVlrT1l1UUctekhLeU1CVGJKaHFQYVNwYThkUUlSTFdBbXZIUWdQdEwxTmNudmJuVURyaEhoNzlOZWxzLVUwRERtM0pvaE45eE9RYUR1Z1J4OUw4
  PUBLISHED: 2026-09-23 08:05 GMT
  QUOTE: "Dow, S&P 500, Nasdaq slip as oil steadies, markets eye looming Trump-Xi meeting"
  SUMMARY: Note the **oil sign flipped intraday** — the morning card's "oil offered / relief" read did not hold as a same-session tailwind.

**Taxonomy-aligned driver decomposition:**

1. **Risk-off tape / equity beta contraction** — the dominant factor. XLY is a high-beta, long-duration sleeve; on a Nasdaq-led down day it should underperform SPY. It did (−0.69 rel).
2. **Geopolitical shock (Iran/UNGA headline)** — a live S0-class shock that the card explicitly ruled out ("no CPI/NFP/FOMC/retail-sales binary" was read as "no catalyst"). A geopolitical binary is not on the economic calendar but is absolutely a same-session macro driver.
3. **Oil sign reversal** — the card's central S1 claim was that oil's live sign was *down* = relief. The 08:05 GMT headline says oil **steadied**, and the session was a risk-off day. The "relief" increment did not materialize as a sector tailwind.
4. **Mega-cap composition** — AMZN/TSLA ≈ 40–45% of XLY. On a Nasdaq-led decline, the composition leg is a *drag*, not the neutral "split book" the card described from 09-22 cash.

---

## 2. Audit of morning S0–S4 reads against reality

### S0_SHARED_MACRO = 0 → **MISS (should have been −1)**

The card's S0 reasoning was: "Paid hawkish FOMC, oil offered, live futures inside ±0.5%, leftover chips banned, Barr/PMI = distribution not mean. Not −1: that re-votes 09-16 and fights an oil-offered, modest-green Finviz book."

**What was knowable at the open and was missed:**

- The **Iran/UNGA headline** was published 12:31 GMT (08:31 ET) — *before* the 09:30 ET open. The card's research appendix lists no query for geopolitics/Iran/UNGA. This is a **knowable-at-open** shock that was simply not in the input set.
- The card treated "no CPI/NFP/FOMC/retail-sales binary" as equivalent to "no catalyst." That is a category error: the calendar was empty of *scheduled economic* binaries, but the geopolitical tape was not empty.
- The card's own 09-21 lesson said "sign S0+ when *cross-asset* confirms a shared-macro risk-on." The symmetric case — sign S0− when cross-asset confirms a shared-macro risk-off — was not applied. Europe was **red** (−0.25%) in the morning card and Asia was only +0.33%. That is a *mixed-to-soft* cross-asset book, not a neutral one. The card read "mixed" and defaulted to 0; the correct read of "Europe red + geopolitical headline + Nasdaq-led futures" was **−1**.

**Verdict on S0: MISS.** The card had the ingredients (Europe red, NQ negative, geopolitical headline available) and netted them to zero.

### S1_SECTOR_FACTORS = 0 → **MISS (should have been −1)**

The card's S1 net was: "stale spend/labor/SAAR positives vs stale confidence/credit/nested-retail negatives. Live increment is oil *relief*."

Two problems:

1. **The oil "relief" increment was the card's only live signed input, and it was pointed the wrong way.** The card scored oil relief as a *positive* for discretionary (cheaper gasoline → more discretionary spend). But on a risk-off day, oil *steadiness* removes the one offsetting tailwind, and the sector's problem was not gasoline — it was multiple compression on high-beta growth. The card over-weighted a second-order consumer-spending channel and under-weighted the first-order discount-rate/beta channel.
2. **The nested retail-down signals were dismissed as "nested, not parent."** MAP HEAT showed Apparel / Dept stores (KSS) / Footwear (NKE, DECK) / Home Improvement (HD/LOW week −3%) **down**, Internet Retail mixed/down. The card's rule "nested OVERRIDE stays nested — do not average into XLY" is defensible in isolation, but when *every* nested consumer-discretionary sub-industry is down and the parent is a high-beta sleeve on a risk-off day, the nested signals were **confirmatory**, not noise. The card used the nesting rule to discard a unanimous negative breadth signal.

**Verdict on S1: MISS.** The live increment was mis-signed and the nested negatives were wrongly discarded.

### S2_BREADTH = 0 → **MISS (should have been −1)**

The card said: "Nested heat is a split book... Mega-cap bid vs weak broad book, if it were live, would be 'up but lagging' — it is **not** live this morning."

The card's own data: structural 50-day breadth ~17% above 50-dma (very weak), nested sub-industries mostly down, and 09-22 cash internally split (AMZN −1.12/−1.34% vs HD +2.74/+2.8%, TSLA +0.82/+0.96%). The card read the split as "cancels." Reality: on a Nasdaq-led down day, the **AMZN/TSLA leg dominates** and the HD leg cannot offset it. The "split book" was actually a **fragile book** — one strong defensive-ish name (HD) against two high-beta mega-caps that get hit hardest in a Nasdaq-led selloff.

**Verdict on S2: MISS.** Weak structural breadth + high-beta mega-cap concentration was a *down* setup on a risk-off day, not a neutral one.

### S3_FLOWS_POSITIONING = 0 → **PARTIAL / defensible**

ETFdb 5d +$714M, 1m −$253M; XLY not in top creation/redemption lists. The card correctly noted 1m rel −5.28% is *not* a crowded long. This component was genuinely unsigned and the 0 is defensible. No material flow-driven move is evident in the actuals.

**Verdict on S3: HIT (0 was correct).**

### S4_ETF_TAPE = 0 → **MISS (should have been −1)**

The card said: "Channel 1 1d rel −0.25% is sub-gate. 3d/1w/1m remain negative leftover. Live PM XLY +0.10% is middle-of-pack... 08-28: do not re-vote the lag as a full S4 down when S0=0."

The 08-28 rule ("do not restack 1w/1m lag into S4 when S0=0") is a *conditional* rule — it applies when S0=0. But S0 should not have been 0 (see above). Once S0 is correctly −1, the 1w/1m relative lag (−2.24% / −5.28%) is no longer "leftover to be suppressed" — it is **confirmatory trend evidence**. The card used a conditional suppression rule to discard a persistent, one-directional relative downtrend.

**Verdict on S4: MISS.** The suppression rule was applied outside its precondition.

### Multiplier / confidence

MULTIPLIER 0.9 and CONFIDENCE 0.42 were reasonable *given* an all-zero card. The problem was not the multiplier — it was that all five components were zero on a day with a knowable-at-open geopolitical shock and a red European session.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:** The card was careful not to double-count oil (kept out of S0 and S1) and not to restack Warsh/FOMC. That discipline is good. But the discipline was applied to *suppress* signals, not to *find* them. The card's anti-double-count rules became a one-way ratchet toward zero.

**Knowable-at-open test — the decisive audit:**

| Signal | Knowable at open? | In card? | Correct sign |
|---|---|---|---|
| Iran/UNGA "annihilate" headline (12:31 GMT) | **Yes** | **No** | −1 (S0) |
| Europe red (−0.25%) | Yes | Yes | −1 (S0) |
| NQ futures negative (−0.10%) | Yes | Yes | −1 (S0) |
| Nasdaq-led futures configuration | Yes | Yes | −1 (S0) |
| Nested consumer-discretionary sub-industries down | Yes | Yes | −1 (S1/S2) |
| Structural 50-dma breadth ~17% | Yes | Yes | −1 (S2) |
| 1w/1m relative lag −2.24%/−5.28% | Yes | Yes | −1 (S4, once S0≠0) |
| Oil "relief" | Yes | Yes | ~0 (second-order) |

**The card had 6 of 7 down-signals in hand and netted them to zero.** The one signal it lacked (Iran/UNGA) was the catalyst, but the *setup* was already visible without it. This is not a "unknowable shock" miss — it is a **netting-to-zero miss** on a card whose own inputs leaned negative.

**The 09-22 pathology repeated in mirror image.** The memory note says: "Last graded 2026-09-22 official down/mild vs XLY +0.089%... dir MISS — LLM S0–S4 all 0 audited correct; engine leftover tape_anchor minted the signed call." On 09-22 the engine minted a *down* call the LLM correctly refused. On 09-23 the LLM minted a *flat* call the tape refused. The lesson: **the all-zero card is not a safe default.** It is a specific claim ("no signed information") that must be earned, and on 09-23 it was not earned.

---

## 4. Outliers inside the sector

- **HD** was the standout positive on 09-22 (+2.74%) and the card leaned on it as the "split book" offset. On a Nasdaq-led risk-off day, HD's defensive-ish profile cannot offset AMZN+TSLA (~40–45% combined). The card treated HD as a co-equal leg; it is not.
- **AMZN/TSLA** are the swing factor. The card's own 09-22 data (AMZN −1.12/−1.34%, TSLA +0.82/+0.96%) showed AMZN already weak. On 09-23, with Nasdaq leading down, the AMZN leg almost certainly extended lower — this is the mechanical source of the −0.69 rel.
- **Nested sub-industries** (apparel, footwear, dept stores, home improvement) were unanimously down in MAP HEAT. No positive outlier emerged to rescue breadth.

---

## 5. Lessons for the next Consumer Cyclical card

1. **"No scheduled economic binary" ≠ "no catalyst."** Add a standing pre-open check for geopolitical headlines (UNGA, Iran, Trump–Xi, etc.). The 09-23 Iran headline was published 08:31 ET and was absent from the research appendix.
2. **The all-zero card must be earned, not defaulted to.** When ≥5 of 7 knowable-at-open signals lean one direction, the card should not net to zero. Require an explicit "why is this genuinely unsigned?" justification when the leading sum is 0 but the input lean is directional.
3. **Conditional suppression rules (08-28) must be gated on their precondition.** "Do not restack 1w/1m lag when S0=0" is void when S0 should be −1. The card applied the rule without re-checking S0.
4. **Nested unanimity is signal, not noise.** When every nested consumer-discretionary sub-industry is down, the "nested stays nested" rule should not be used to discard the cluster — it should be used to *size* it, not to zero it.
5. **High-beta mega-cap concentration is a directional amplifier, not a neutralizer.** On a Nasdaq-led day, AMZN+TSLA ≈ 40–45% of XLY means the ETF *is* a Nasdaq-beta proxy. The "split book" framing understated this.
6. **Oil relief is second-order for XLY.** The card's only live signed input was oil relief, and it was pointed the wrong way. Demote oil to a tiebreaker, not a primary S1 input.

---

OUTCOME_BEGIN
SECTOR: Consumer Cyclical
ETF: XLY
ETF_PCT: -1.407824809582947
SPY_PCT: -0.7202161019229769
REL_PCT: -0.6876087076599702
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Nasdaq-led broad risk-off session (Iran/UNGA geopolitical headline + Europe red) hitting a high-beta sleeve whose ~40-45% AMZN+TSLA concentration amplified the decline
KEY_INTERACTION: The card's only live signed input (oil "relief") was second-order and mis-signed, while six knowable-at-open down-signals (Europe red, NQ negative, nested sub-industry unanimity, ~17% 50-dma breadth, 1w/1m relative lag, Nasdaq-led futures) were netted to zero by conditional suppression rules applied outside their preconditions
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: MISS — all-zero card on a day whose own inputs leaned negative; S0/S1/S2/S4 each should have been -1, S3 correctly 0
OUTCOME_END