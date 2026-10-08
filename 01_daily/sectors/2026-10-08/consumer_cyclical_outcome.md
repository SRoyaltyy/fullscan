# Sector Outcome — Consumer Cyclical — 2026-10-08

Actuals: {'etf': 'XLY', 'pct': 0.3142946050671558, 'spy_pct': -0.4233007580038639, 'rel': 0.7375953630710197, 'open': 110.81999969482422, 'close': 111.70999908447266, 'source': 'yf_download'}

# Sector Post-Session Review — Consumer Cyclical (XLY) — 2026-10-08

## 0. FACTS

**CLAIM:** XLY closed **+0.31%** on 2026-10-08, from open 110.82 to close 111.71.
**URL:** deterministic actuals (injected)
**PUBLISHED:** 2026-10-08
**QUOTE:** `ETF_PCT: 0.3142946050671558 | OPEN: 110.81999969482422 CLOSE: 111.70999908447266`
**SUMMARY:** XLY finished green on the day.

**CLAIM:** SPY closed **−0.42%** on 2026-10-08.
**URL:** deterministic actuals (injected)
**PUBLISHED:** 2026-10-08
**QUOTE:** `SPY_PCT: -0.4233007580038639`
**SUMMARY:** The broad market was red; XLY was green.

**CLAIM:** XLY relative return vs SPY was **+0.74%**.
**URL:** deterministic actuals (injected)
**PUBLISHED:** 2026-10-08
**QUOTE:** `REL_PCT: 0.7375953630710197`
**SUMMARY:** XLY outperformed SPY by ~74 bp — a decisive relative win on a down tape.

**Path:** Open 110.82 → close 111.71. The morning PM print was **XLY −0.68%**; the ETF opened *above* that implied level and then rallied to close +0.31%. So the realized path was **up-and-away from a red pre-market**, i.e. the pre-market weakness was fully faded intraday. This is the single most important fact of the session: the morning tape was red, and the sector *reversed it*.

**Direction:** **up** (predicted: down). **Magnitude:** **mild** (+0.31% absolute, but +0.74% relative — the relative move is the real story).

---

## 1. What actually drove the sector

The morning card's central macro object was a **fresh hawkish rates increment** (FOMC minutes "another hike likely this year," 10Y testing ~5.3%, 10Y auction same-session) landing on a duration-heavy book (AMZN ~23% + TSLA ~17–18%). The search results confirm the *macro* half of that read was correct:

**CLAIM:** "S&P 500 Slips as Oil Spikes 5%, 10-Year Yields Near 5.35%."
**URL:** https://news.google.com/rss/articles/CBMizAFBVV95cUxOazNxUkozME41YlJvcjB5TXRQVXEyMW0tNi0tZFJpOEFJcWNLS0xFNUFXdVBJZVZ3clJwT2VNaWY1WDhiT0tjSUpaa2ZwS0phaWRjejk4Z1czRUpYdzFrQWwtaG14bkFRYXVOVlVMMDd2d3RVclRRdmcwRW93TW5XZWJSTDc2QVBIcEpsVmhpck9nY0t1LWJQcjZwbXZ5N2VLTzdMV0NnZWQ3WXVnYk5uQ2ZpbW9uQkR0QzkyTDgyWi1FcXNucW1FbTctbno
**PUBLISHED:** 2026-10-08 17:19 GMT
**QUOTE:** "S&P 500 Slips as Oil Spikes 5%, 10-Year Yields Near 5.35%"
**SUMMARY:** The hawkish-rates + oil-shock macro frame was real and it did pressure the broad tape.

**CLAIM:** "Oil Rise Spurs Fresh Selling in Stocks and Bonds: Markets Wrap."
**URL:** https://news.google.com/rss/articles/CBMilAFBVV95cUxNQU1SaHdYWU1rZkFDZjFUZTVMU2dkQ2dCRUF4eXhJRmdMSzFuZHFqSDYtdF9VNWZEV3doQkh6TGVtZklPWWc4NkpYQ0s2by1pMUZNMmFaWERvblBiZXRzZnJ0ajRaaXh0NVo0VlRYWmVENFVJeFhidG5IdXVna0FxdHMxanN1YjIyRmczQXFlS0xGMXph
**PUBLISHED:** 2026-10-07 22:03 GMT
**QUOTE:** "Oil Rise Spurs Fresh Selling in Stocks and Bonds"
**SUMMARY:** Oil was the cross-asset driver — and note this is the *opposite* sign from what the morning card assumed.

**CLAIM:** "Nasdaq Falls 0.6% as Oil Shock Sends Energy Up 2.3%."
**URL:** https://news.google.com/rss/articles/CBMiekFVX3lxTFBWYUpLQmF5ZjNtZzUwbi05TC10OTI3cGYtUnpzM0Z6cWEwdGViWmw1Ti1YcERacGtha05sOHFLSXVRR1lxNlpvbVJaeWE1Qml4a0VoTktieDRIeGtvNG1ZNXhiSzdaUFhnYXVIVEg1Q0I2RWs0Z1JBXy1B
**PUBLISHED:** 2026-10-08 13:56 GMT
**QUOTE:** "Nasdaq Falls 0.6% as Oil Shock Sends Energy Up 2.3%"
**SUMMARY:** The selloff was **tech/Nasdaq-led with an energy bid** — exactly the "duration/tech-led selloff with a defensive bid" shape the morning card described, but the card drew the wrong conclusion about XLY's place in it.

**The critical error in the morning frame:** the card treated XLY as a *duration-heavy victim* of the rates move. But the actual tape shows XLY **outperformed SPY by 74 bp on a day when the Nasdaq fell 0.6% and yields hit 5.35%**. The correct taxonomy is:

- **PRIMARY DRIVER: Rotation *into* consumer discretionary as a non-tech, non-AI-hardware, domestic-demand sleeve while tech/AI-hardware sold off.** The morning card explicitly identified "money rotating to semis/AI hardware, away from consumer discretionary" as an S2/S3 headwind. The realized flow was the **reverse**: on a day when the AI-hardware complex and long-duration tech were the funding source, XLY was a *beneficiary* of the rotation, not a victim.
- **Secondary driver: the oil spike.** The morning card scored oil as *relief* (live sign down, WTI −1.59%). The search results say oil **spiked ~5%** and energy rose 2.3%. The card's "discarded prior-close anchor" (CL=F +4.79%) was **not stale — it was the live move.** This is a direct, documented miss on a scored input.

---

## 2. Audit of morning S0–S4 reads against reality

### S0 (Shared macro) — scored **−1**. **WRONG SIGN.**

The card's own reasoning contained the tell it ignored: *"the 1w rel is positive — the sector has been outperforming into this."* It then scored S0 negative anyway on the hawkish-rates increment. The realized session proved the 1w relative strength was the **leading** signal and the rates increment was **not** transmitted to XLY as a negative. Two sub-errors:

1. **Oil sign inversion.** The card wrote: *"CL=F +4.79% / BZ=F +4.77% 1d (discarded prior-close anchor — use Finviz live sign = down)."* The search results confirm oil **spiked ~5%** on the day. The card discarded the correct live signal as "stale" and substituted a stale Finviz snapshot. This is a **knowable-at-open error**: the futures print was the live move.
2. **Rates→XLY transmission assumed, not verified.** The card noted the 5-day 10Y–SPX correlation had *broken* (+0.303, no longer cleanly inverse) — and then still scored the rates move as a clean negative for a duration-heavy book. If the coupling has broken, the mechanical duration penalty is not warranted.

### S1 (Sector factors) — scored **−1**. **WRONG SIGN.**

The nested MAP HEAT was "unanimously down or flat" (Department Stores, Footwear, Home Improvement, Apparel). The card scored this as a HIT for "Retail miss / traffic down." But **nested sub-industry weakness is not the same as ETF weakness when the ETF is cap-weighted and its top-2 holdings (~41%) are AMZN and TSLA** — neither of which is a department store, footwear, or home-improvement name. The card *acknowledged* this ("no single name drives the call") but then let the nested heat drive the sign anyway. The realized +0.74% relative says the nested heat was **irrelevant to the ETF's return** — it was a small-cap/mid-cap consumer story, not an XLY story.

The "Credit tightening / delinquency rise" HIT (HY OAS +35 bp 1m) was a **level**, correctly labeled as such — but a level is not a same-day driver, and it did not move XLY today.

### S2 (Breadth) — scored **−1**. **WRONG SIGN.**

"Breadth failure inside the sector" was inferred from nested MAP HEAT. But the ETF closed **green while SPY was red** — that is *relative* breadth strength at the index level, which is what matters for an ETF call. The card conflated **sub-industry breadth** (weak) with **ETF-level relative performance** (strong). These diverged, and the ETF-level measure won.

### S3 (Flows/positioning) — scored **0**. **CORRECT (no signal).**

The card correctly declined to manufacture a flow signal. The 1w rel +0.40% it flagged as "slightly favored" was, in hindsight, the **most informative** input in the entire card — and it was scored as neutral.

### S4 (ETF tape) — scored **−1**. **WRONG SIGN.**

The card called the 1d rel −0.08% "sub-gate" (correct) but then used the *shape* (1m lag, 1w catch-up) plus PM −0.68% as "mild negative confirmation." The realized path — open above the PM print, close +0.31% — shows the PM weakness was **faded**, not confirmed. The card treated a red pre-market as confirmation of a red day; the pre-market was the *fade candidate*, not the signal.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count:** The card scored the hawkish-rates increment in S0 and then scored "Sector rotation out of discretionary" in S1 — but the rotation it described (into semis/AI hardware) was the *same* rates/AI-hardware complex. More importantly, the card scored **oil as relief in S0** while the live oil move was a **spike** — and then separately scored "Gasoline spike crushing discretionary" as a MISS. So oil was scored **twice, both times with the wrong sign**: once as relief (S0), once as a non-event (S1 MISS). The realized oil spike was a genuine input that the card actively *discarded*.

**Knowable-at-open test:** The single most knowable-at-open signal was the **1w relative strength (+0.40%)** — the card had it, flagged it, and then overrode it with a macro narrative. The second was the **live oil futures print** — the card had it and explicitly discarded it as "stale." Both were knowable and both pointed the *other* way. **KNOWABLE_AT_OPEN: yes** — the information to call this flat-to-up was present; the card chose the macro story over the tape.

**The 10-02 lesson fired, but was misapplied.** The card invoked "10-02 lean-toward-live-tape-when-binary-unprinted" and concluded: *"the live tape is red, so the rule authorizes following the red tape."* But the 10-02 lesson's actual content was that the **engine's up-lean was the better call** and the card's error was *flattening a one-sided conditional*. Today the card used the lesson to **justify a directional down call off a red pre-market** — which is the *opposite* of what the lesson taught. The lesson says: when the tape is live and the card is unsigned, **follow the tape**. The card followed the *pre-market* tape, not the *live* tape — and the live tape (XLY opening above PM, then rallying) was up.

---

## 4. Outliers inside the sector

The search results surface one relevant outlier:

**CLAIM:** "Higher rates are pummeling consumer discretionary stocks — here's the damage: Chart of the Day."
**URL:** https://news.google.com/rss/articles/CBMi4gFBVV95cUxPS1l5cXdYVWUzVHFnclhMTzZjUEpLZ2JObGNqRlRnY29EZDRiYUNuMmVDQ0F1Z0Y1aUlrRWxvWEhLdGhZZnJUYWhGWDBqMU9CSk41SzU3WmUwYjRRa1lGMndnUG9tNTJvUXFyeVVrT3lJTy1YbkpyZEJtd3Y4T1N1N24wOWY3QjgxSktRZTlkT1M3Z21vMHJpdVBmbHVsZmNZdEN5T1E4ODdXZEpjdFNiTkVuWFZMX3NzVkc1Yng0YWNuSjRpSTY4MFNXbWg0SmFUS0oxQ3ZHaVZ6R2owVkdXVHRB
**PUBLISHED:** 2026-10-05 16:10 GMT
**QUOTE:** "Higher rates are pummeling consumer discretionary stocks — here's the damage"
**SUMMARY:** This was the *prevailing narrative* going into 10-08 — and XLY's +0.74% relative on a day when yields hit 5.35% is a direct **falsification** of that narrative for this session. The narrative was the trap the morning card walked into.

**Outlier within XLY:** The ETF's green close on a red SPY day, with the Nasdaq −0.6% and energy +2.3%, implies the **cap-weighted mega-cap consumer names (AMZN, TSLA) held up or rose** while the broad tape sold off. That is the opposite of the "duration-heavy book gets hit by rates" thesis. The most likely explanation is that AMZN/TSLA were **funding-source rotation beneficiaries** — money leaving AI-hardware/semis and long-duration tech found a home in the two largest XLY names, which are themselves large-cap quality with domestic-demand exposure. The nested small/mid-cap consumer weakness (department stores, footwear) was real but **immaterial to the cap-weighted ETF**.

---

## 5. Verdict

The morning card was **directionally wrong** (predicted down/mild; realized up/mild) and, more importantly, **wrong on the relative call** (predicted XLY would lag; realized XLY outperformed by 74 bp). The failure was not a magnitude miss — it was a **sign error on the primary driver**. The card correctly identified the macro environment (hawkish rates, oil move, tech-led selloff) but **mis-assigned XLY's role in it**: it treated XLY as a duration victim when the tape treated it as a rotation beneficiary. The two knowable-at-open signals that pointed the right way — the 1w relative strength and the live oil futures print — were both present in the card and both **overridden or discarded**. The 10-02 lesson was invoked but inverted in application.

---

OUTCOME_BEGIN
SECTOR: Consumer Cyclical
ETF: XLY
ETF_PCT: 0.31
SPY_PCT: -0.42
REL_PCT: 0.74
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Rotation into cap-weighted mega-cap consumer (AMZN/TSLA) as a non-tech, non-AI-hardware sleeve while tech/Nasdaq sold off on a hawkish-rates + oil-spike tape; XLY outperformed SPY by 74 bp.
KEY_INTERACTION: The card scored the hawkish-rates increment as a negative for a "duration-heavy book" and discarded the live oil futures print as stale — but the realized tape showed XLY as a rotation beneficiary, not a duration victim, and oil spiked ~5% (the discarded print was the live move).
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Directionally wrong (down/mild vs up/mild) and wrong on the relative call; the two knowable-at-open signals that pointed the right way (1w rel +0.40%, live oil futures) were present and overridden/discarded, and the 10-02 lesson was invoked but inverted.
OUTCOME_END