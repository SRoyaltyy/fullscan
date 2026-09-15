# Sector Outcome — Consumer Cyclical — 2026-09-15

Actuals: {'etf': 'XLY', 'pct': -1.745681211643868, 'spy_pct': -0.45867813741702346, 'rel': -1.2870030742268446, 'open': 112.52999877929688, 'close': 110.87999725341797, 'source': 'yf_download'}

# Sector Post-Session Review — Consumer Cyclical (XLY) — 2026-09-15

## 0. FACTS

**CLAIM:** XLY closed at $110.88, down 1.75% on the day, versus SPY −0.46%, for a relative return of −1.29%.
**URL:** https://stockanalysis.com/etf/xly/ (intraday print $110.81 −1.81% at 15:31 ET); deterministic actuals injected: OPEN 112.53 / CLOSE 110.88 / ETF_PCT −1.7457 / SPY_PCT −0.4587 / REL_PCT −1.2870.
**PUBLISHED:** 2026-09-15
**QUOTE:** "As of Sep 15, 2026 03:31 PM ET … $110.81 USD -2.04 (-1.81%)"
**SUMMARY:** XLY opened at 112.53 and closed at 110.88 — a **monotone-ish down day with no meaningful morning bounce**: the open was essentially the high, and the ETF bled ~1.65 points into the close. Relative to SPY it lost 1.29 points, i.e. the sector was a **clear relative loser**, not a rotation destination.

**CLAIM:** The broad tape was red on the day, driven by an oil spike and 10Y breaching 5%.
**URL:** https://news.google.com/rss/articles/CBMisgFBVV95cUxQM1BPdzlzdEkxaTg2MURwV0psdFNmbUZ0ZnZreDZEZkFfT0JXSGUxcU9lZ0JGc09LMkRZYTMxYVNwU2hVYjdfcnl1UzhnS0M5eHF1bVdwWFh2Rmp6SmlZMUo2MjlaODdFRHBBVGQ2OHVtcjBQVTFIekZjdkdKbkh5dVhTRUVtOEE1cjRwM21UVXNlV25LUzhiMFNEeWd4a3hGMExiczlZNC1pN2tpQmhPTFJn?oc=5
**PUBLISHED:** 2026-09-15 18:31 GMT
**QUOTE:** "Wall Street dips as oil spikes and benchmark Treasury yields breach 5%"
**SUMMARY:** The session's macro spine was exactly the two objects the morning note flagged as live: **crude spiking** and **10Y ≥ 5%**. This is the regime the morning call was built on, and it is confirmed by the close.

**Path:** Open 112.53 → close 110.88. No recovery. The morning premarket print of XLY −0.13% (middle-of-pack, better than ES −0.54%) **did not hold** — the sector underperformed the index by ~1.3 points on the day, meaning the relative resilience visible pre-open was a **false tell**.

---

## 1. What actually drove the sector

The day's driver set is the morning's driver set, but with the **weights wrong**:

**(a) The oil shock — the dominant, sector-specific negative.** WTI $103.79 / Brent $108.11 at the open, with RBOB +1.92%, heating oil +3.09%, gasoil +2.68%. This is the 08-11 object firing for a sixth session, and it is the one object that is *both* macro and *consumer-specific* — gasoline transmission hits discretionary purchasing power directly. The morning note scored this as S0 = −1.5 (dominant but not −2). **Reality: it was closer to a full −2 for XLY specifically**, because the sector is the most gasoline-sensitive of the eleven (autos, travel, restaurants, broad discretionary).

**(b) The rates object — scored once in S0, but under-weighted.** 10Y breached 5% (highest since 2007), DFII10 +0.05d / +0.18w / +0.18m. XLY is ~46–49% AMZN + TSLA + HD — a **duration-heavy growth sleeve**. The morning note explicitly discounted this because "5-day 10Y–SPX corr is only −0.151 — yields are *not* the dominant equity driver." That was the single most consequential misjudgment of the session: the correlation was low *because the prior week's driver was the AI/semis unwind*, not because rates were inert. When the AI object faded and the rates object took over, XLY — the most duration-sensitive cyclical — took the hit.

**(c) The AI/semis rotation — scored ZERO for XLY, correctly.** AMD −5% premarket, APH −6.5%, XLK the weak leg. XLY holds zero semis. The 09-14 lesson said: when the dominant risk-off object is exogenous to XLY's holdings, XLY is a rotation *destination*. **This part of the read was directionally right but the conclusion was wrong** — XLY was not a destination today because the *rates* leg of the same risk-off impulse hit it directly. The exogenous-object lesson protected against a double-count but also **masked the fact that XLY had its own live negative (rates + oil) that was not exogenous at all.**

**(d) The fresh consumer-specific negatives — all confirmed.** UMich Sept prelim 47.8 vs 51.0 expected (09-11), 1-yr inflation expectations 4.6%; $528.3M weekly outflows from consumer discretionary ETFs (Jefferies, 09-13/09-15); HD/LOW both red on the week; Footwear breadth 0.077. These were the "genuine −1" S1 factors, and they showed up.

**Primary driver:** Oil spike + 10Y ≥ 5% hitting a duration-heavy, gasoline-sensitive cyclical, with fresh consumer-confidence and flow negatives as the sector-specific accelerant.

---

## 2. Audit of morning S0–S4 reads against reality

Using the **morning numbers as written**, not post-close rewrites:

| Component | Morning score | Reality check | Verdict |
|---|---|---|---|
| **S0 Shared macro** | −1.5 | Oil + rates both hit XLY harder than a "mild risk-off" implies. The note's own cap logic ("not −2 because XLY premarket is middle-of-pack") was the error — premarket middle-of-pack did not survive the open. | **Under-scored by ~0.5** |
| **S1 Sector factors** | −1.0 | UMich 47.8, $528M outflows, HD lag, gasoline — all real and dated. This was the best-calibrated component. | **Correct** |
| **S2 Breadth** | −0.5 | Narrow internals confirmed; HD (top-3 weight) lagged. Mild breadth failure was right in kind, slightly light in degree. | **Correct** |
| **S3 Flows** | −0.5 | The $528M outflow print was fresh and dated; the note correctly gave it modest weight. | **Correct** |
| **S4 ETF tape** | −1.0 | 1d rel −1.43% at the time; the note said "tape is confirming down, but not a sector-specific breakdown." Reality: −1.29% rel *is* a sector-specific breakdown. | **Correct in sign, light in degree** |

**Net:** the direction was right, the magnitude band was **wrong by one notch** (mild predicted, notable realized), and the error was concentrated in **S0**, where the note applied a cap that the tape did not justify.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count audit — the morning note was disciplined here.** Oil was scored once in S0; S1's gasoline got only partial weight as the transmission channel; the AI/semis object was scored zero for XLY; rates were scored once in S0. **No double-count occurred.** The problem was not stacking — it was **under-weighting a single correctly-identified object.**

**The knowable-at-open test — this is the crux.** Everything needed to call this a *notable* down day was visible before the open:

- WTI $103.79 / Brent $108.11 — **knowable at open**, and the highest crude of the run.
- 10Y breached 5% overnight — **knowable at open**.
- UMich 47.8 (09-11) — **knowable at open**, 4 days old.
- $528.3M sector outflows (09-13/09-15) — **knowable at open**.
- XLY premarket −0.13% — **knowable at open**, and the note *used it as a reason to cap the score*.

The one thing that was **not** knowable at open was that the premarket middle-of-pack print would fail to hold. But the note's own logic should have flagged this: **a sector with a live, dominant, sector-specific negative (oil) plus a live duration headwind (rates) plus fresh consumer-specific negatives should not be capped at mild just because its premarket print is only −0.13%.** The premarket print is a *tape* input, not a *factor* input, and the note let a tape input override four factor inputs.

**Knowable-at-open: YES** — the direction and the *notable* magnitude were both derivable from pre-open information. The morning call had the right factors and the wrong cap.

---

## 4. Outliers inside the sector

- **HD / LOW (Home Improvement):** the morning note flagged HD as "a genuine lag" with a −1.87 vs-parent residual. That read was correct and HD is a top-3 XLY weight — it contributed materially to the relative underperformance.
- **Footwear (NKE, DECK):** breadth 0.077, worst in batch. Confirmed as a drag.
- **Department Stores (KSS +9.43% w1):** the one nested **up** override. Small weight, so it did not offset the HD/Footwear drag — consistent with the morning's "more down than up" net read.
- **Gambling (SGHC, Caesars, Wynn):** ad-hoc-news items on Caesars insider trades and Wynn's $900M debt deal suggest the gambling sub-group was mixed-to-firm, again a small weight.
- **Autos (TSLA):** the morning note called TSLA's d1 +2.13 "a bounce, not a trend" and capped auto upside. With rates breaching 5%, TSLA's duration profile made it a likely drag rather than a support — consistent with the flat-to-down auto read.

No single outlier flipped the sector; the drag was broad and concentrated in the mega-cap duration sleeve (AMZN/TSLA) plus HD.

---

## 5. Verdict

The morning call was **directionally correct (down) and magnitude-light (mild vs notable)**. The factor identification was strong — oil, rates, UMich, outflows, HD lag were all correctly named. The failure was a **cap discipline error**: the note let a single premarket tape input (XLY −0.13%, middle-of-pack) override four live, dated, sector-relevant factor negatives, and it explicitly discounted the rates object on a correlation statistic that was regime-dependent rather than structural. The 09-14 exogenous-object lesson — designed to prevent over-scoring XLY on AI/semis news — was applied correctly to the AI object but **spilled over into an unjustified discount of the rates object**, which is *not* exogenous to a duration-heavy consumer cyclical.

**Lesson for the book:** when a sector has a live, dominant, *sector-specific* negative (oil for XLY) **plus** a live duration headwind (rates) **plus** fresh sector-specific negatives (UMich, outflows), the premarket tape print is a *confirming* input, not a *capping* input. The cap should bind on factor absence, not on relative premarket resilience.

OUTCOME_BEGIN
SECTOR: Consumer Cyclical
ETF: XLY
ETF_PCT: -1.7457
SPY_PCT: -0.4587
REL_PCT: -1.2870
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Oil spike (WTI $103.79 / Brent $108.11) plus 10Y breaching 5% hitting a duration-heavy, gasoline-sensitive cyclical, with fresh UMich 47.8 and $528M sector outflows as accelerants
KEY_INTERACTION: The 09-14 exogenous-object lesson (correctly applied to AI/semis) spilled over into an unjustified discount of the rates object, which is NOT exogenous to XLY's AMZN/TSLA duration sleeve
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction correct, magnitude one notch light — S0 under-scored by ~0.5 because a premarket tape input was allowed to cap four live factor negatives
OUTCOME_END