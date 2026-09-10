# Sector Outcome — Consumer Cyclical — 2026-09-10

Actuals: {'etf': 'XLY', 'pct': -0.44460252896181274, 'spy_pct': -0.5994238166152965, 'rel': 0.1548212876534838, 'open': 111.48999786376953, 'close': 111.95999908447266, 'source': 'yf_download'}

# Sector Post-Session Review — Consumer Cyclical (XLY) — 2026-09-10

## 0. FACTS

**Tape (deterministic actuals):**
- XLY: **−0.44%** (open 111.49 → close 111.96)
- SPY: **−0.60%**
- Relative: **+0.15%** (XLY *outperformed* SPY)
- Path: opened at 111.49, closed at 111.96 — i.e., XLY **gained ~0.42% from the open to the close**. The negative daily print is entirely an overnight/gap artifact; the intraday session was a *recovery*.

**Index context (search-confirmed):**
- S&P 500 −0.6% to 7,591.70; Nasdaq −0.7%; Dow −0.6% — **fourth straight loss** (MarketWatch / NH Register, 2026-09-10).
- Driver per MarketWatch headline: *"Dow, S&P 500 and Nasdaq end lower as U.S. oil prices surge"* — the oil shock thesis was directionally correct at the index level.

**Morning prediction:** down / **notable** (total_score −7.5, mult 1.0, confidence 0.6, regime risk_off).

**Verdict on the call:** **Direction HIT. Magnitude MISS (badly).** Predicted notable down; realized −0.44% is *flat-to-mild*, and XLY was a **relative winner** on a day the model had it as a deep laggard. The single most important fact of the session: **the sector the model called "notable down" was the thing that held up while SPY fell.**

---

## 1. What actually drove the sector

**Primary driver: the oil shock hit the index, but XLY's mega-cap core absorbed it and the ETF mean-reverted off an oversold open.**

The macro regime read was right — oil surging, yields up, risk-off tape, SPY −0.60%. But the transmission into XLY failed in two specific ways:

1. **The gasoline/discretionary-demand channel did not show up in the tape.** XLY fell *less* than SPY. If the oil shock were genuinely crushing discretionary demand expectations, XLY should have underperformed — it did the opposite. The market treated the oil spike as an *index-level* and *energy-sector* event, not a consumer-discretionary earnings event.

2. **XLY's concentration cut the other way.** AMZN ~24% + TSLA ~17% + HD ~5.4%. On a day when the Nasdaq fell 0.7% but XLY fell only 0.44%, the mega-cap growth sleeve was *not* the drag the model assumed. The "rising real yields → duration headwind for AMZN/TSLA" mechanism (S0's rates leg) did not dominate.

**Secondary: the open was the low.** XLY gapped down and then bought all day (+0.42% open-to-close). That is the signature of an **oversold bounce** — RSI ~31, price below the 50-day ($116.15) per the search data — not of a fresh leg down. The 1m rel −4.65% laggard status that the morning read treated as *confirmation of weakness* was, in real time, the setup for a **mean-reversion bid**.

---

## 2. Audit of morning S0–S4 reads

| Component | Morning | Reality check | Grade |
|---|---|---|---|
| **S0 shared macro −2** | Oil shock dominant, more negative for XLY | Oil shock real (SPY −0.6%, 4th straight loss), but it did **not** transmit more negatively to XLY. Direction right, sector-specific sign wrong. | **Half-credit** |
| **S1 sector factors −1** | Gasoline spike crushing discretionary (HIT 0.85) | No evidence in the tape. XLY outperformed. The "fresh spine hit" did not bite. | **MISS** |
| **S2 breadth 0** | No breadth expansion | Correct — but the *absence* of breadth failure was the tell: no % names expansion needed for XLY to hold. | **Neutral, fine** |
| **S3 flows 0** | Laggard status, no same-day bid | The laggard status was the *bid*. Oversold → bounce. Model explicitly refused to restack it as confirmation (good) but also refused to read it as a contrarian setup (missed). | **Neutral, incomplete** |
| **S4 ETF tape −1** | Multi-horizon lag confirms weakness | **This is the core error.** Multi-horizon lag was read as *momentum*, when at RSI 31 / below 50-day it was *stretched*. | **MISS** |

**The compounding error:** S0 (−2) + S1 (−1) + S4 (−1) all pointed the same way, and the divergence check declared "no divergence — both point down." But three factors pointing down that are **all the same oil/rates object viewed through different windows** is not confirmation — it is **triple-counting one shock**. The self-audit claimed "oil counted once in S0 and once in S1 — distinct channels." In practice the market priced oil **once**, at the index level, and XLY's idiosyncratic oversold condition dominated.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count confirmed.** S0 (macro overlay), S1 (gasoline transmission), and S4 (tape lag) were treated as three independent negative votes summing to −4 of the −7.5. They were one shock plus one stretched tape. The correct read: **one macro negative, offset by an oversold mean-reversion positive that was fully knowable at the open** (RSI 31, below 50-day, 1m rel −4.65%).

**Knowable-at-open test — the damning one:**
- **RSI ~31 and price below the 50-day** were in the data at the open. The morning read used the 1m −4.65% lag *only* as a bearish confirmation and never as a stretch/mean-reversion signal.
- **Futures were flat/mixed (ES +0.11%, NQ −0.17%)** — the morning read itself flagged this as "the one honest tension" and "caps magnitude at mild." It then **overrode its own correct observation** and let the pipeline print **notable** (total_score −7.5). The narrative said mild-upper-edge; the deterministic output said notable. **The narrative was right and the score was wrong.**
- **The 09-09 upper-edge lesson** was applied in the wrong direction — it was used to justify *escalating* toward notable, when the flat futures and oversold tape argued for *capping at mild or even flat*.

**The honest conclusion:** the correct call at the open was **down/flat or flat**, with XLY possibly *outperforming* on an oversold bounce. The model had all the inputs to see this and chose the more aggressive band.

---

## 4. Outliers inside the sector

- **XLY itself is the outlier** — the only major complex that outperformed SPY on a risk-off, oil-shock day. Relative +0.15% when the morning thesis implied rel ≈ −0.9% or worse.
- **The mega-cap core (AMZN/TSLA/HD ~46%)** did not break down premarket and did not lead lower — consistent with the 08-18 severe-cap lesson (no severe authorized), but the model failed to extend that logic to *capping the downside entirely*.
- **The AMZN sterling bond item** (financing/capex, neutral-to-slightly-negative) was correctly dismissed as non-driving. Good call — it did not matter.
- **Energy was the real winner** (oil surge), and the rotation was *into* energy / *out of* nothing in particular within consumer — XLY simply wasn't the funding source.

---

## 5. Lessons for the scoreboard

1. **When futures are flat and the tape is oversold (RSI <35, below 50-day), a multi-horizon lag is a mean-reversion setup, not momentum confirmation.** Do not let S4 stack on top of S0/S1 when all three are the same shock.
2. **Cap the band at the futures.** The morning narrative said "flat futures cap magnitude at mild." The pipeline printed notable. **The narrative must bind the score, not the reverse.**
3. **"More negative for Consumer Cyclical" (08-11 oil lesson) is conditional, not automatic.** It requires the shock to be *newly kinetic* AND the sector not already stretched to the downside. On the third consecutive session of the same regime with XLY already −4.65% 1m, the marginal sector-specific negativity is ~0.
4. **Relative-outperformance risk in a concentrated ETF:** when the drag is a *macro/index* shock and the ETF's top holdings are mega-cap growth that the market is treating as quality, XLY can outperform SPY even while falling. The model's "growth duration headwind" leg needs a yields-*spike* confirmation (e.g., 10Y +10bp+), not just a drift.

---

OUTCOME_BEGIN
SECTOR: Consumer Cyclical
ETF: XLY
ETF_PCT: -0.44
SPY_PCT: -0.60
REL_PCT: +0.15
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: flat
PRIMARY_DRIVER: Index-level oil shock hit SPY, but XLY's mega-cap core absorbed it and the ETF mean-reverted off an oversold open (RSI ~31, below 50-day), outperforming SPY.
KEY_INTERACTION: S0/S1/S4 were one oil/rates shock triple-counted as three independent negatives; the oversold tape that was knowable at the open was read as bearish momentum instead of a reversion setup.
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Direction HIT, magnitude MISS — narrative correctly flagged flat futures capping at mild, but the pipeline overrode it to notable and missed XLY's relative outperformance.
OUTCOME_END