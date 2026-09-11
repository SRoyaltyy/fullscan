# Sector Outcome — Financial — 2026-09-11

Actuals: {'etf': 'XLF', 'pct': 0.6681925008832357, 'spy_pct': 0.8524287494320992, 'rel': -0.18423624854886356, 'open': 57.439998626708984, 'close': 57.25, 'source': 'yf_download'}

# Sector Post-Session Review — Financial (XLF) — 2026-09-11

## 0. FACTS

**Tape (deterministic actuals):**
- XLF: **+0.67%** (open 57.44 → close 57.25; note the ETF printed a *higher open than close* — it gapped up and faded into the bell)
- SPY: **+0.85%**
- Relative: **−0.18%** (XLF underperformed SPY)
- Actual direction: **up**; actual magnitude: **mild** (sub-1%, no outsized move)

**The load-bearing fact the morning did not have:** CPI landed **hot**.

CLAIM: August 2026 CPI rose 0.4% m/m and 3.4% y/y, above target.
URL: https://www.stephens.com/perspectives/consumer-price-index-update-september-11-2026
PUBLISHED: 2026-09-11
QUOTE: "The report showed prices increased 0.4% from July to August and increased 3.4% year over year."
SUMMARY: The pending binary resolved to the *hot* side — the opposite of the "cool print relieves duration pressure" branch the morning leaned toward.

CLAIM: Markets read the hot CPI as raising the odds of a Fed hike at the meeting the following week.
URL: https://www.nytimes.com/live/2026/09/11/business/inflation-cpi-report
PUBLISHED: 2026-09-11
QUOTE: "Investors believe the Federal Reserve is very likely to raise rates at its meeting next week."
SUMMARY: The rate-path read turned hawkish — a duration/funding headwind for financials, and the reason XLF lagged a green SPY.

**Path:** XLF gapped up with the broad green futures, then faded to close *below* its open while SPY held a larger gain. The intraday shape is "risk-on open, hawkish-CPI fade, financials lag on the fade."

---

## 1. What drove the sector

The dominant driver was **the CPI binary resolving hot**, which re-priced the rate path hawkish. That is a *shared-macro* event, not a bank-specific one — but it transmits to financials through two channels that the morning had explicitly parked:

1. **Duration / funding channel.** A hot print + hike odds up = higher front-end and real yields, tighter financial conditions. For a sector whose 1m relative tape was already flat (+0.04%) and whose 3d/1w relative tape was already red (−0.51% / −0.41%), this is a *continuation* headwind, not a new shock.
2. **Rotation channel.** In a hot-CPI, hike-odds-up tape, the marginal dollar went to the index (SPY +0.85%) but *not* disproportionately to financials — XLF captured only +0.67%, i.e. it **underperformed by 0.18%**. Financials were not the value-shield beneficiary of the day; they were the funding-cost casualty of it.

The **oil relief** the morning leaned on (WTI −2.54%, Brent −2.86%) was real but *insufficient*: it relieved the inflation impulse at the margin, yet CPI still printed hot, so the relief did not translate into a financials-relative bid. This is the key taxonomy point — **oil falling is an input to the inflation impulse, not a bank earnings catalyst**, and when CPI itself comes in hot, the input is dominated by the print.

Secondary/foreign items (BBVA, BCS, BNS) correctly did **not** drive XLF — the morning's instruction to keep them out of S1 was right.

---

## 2. Audit of morning S0–S4 reads

**S0_SHARED_MACRO = 0 (mixed, binary pending).** *Verdict: directionally defensible, magnitude under-scored.* The morning correctly refused the 09-08/09-09 S0 = −2 oil-shock configuration (oil was falling, futures green) — that was the right call and it avoided a large absolute-down error. But it treated the CPI binary as symmetric ("less one-sided than 09-04") and netted oil-relief against long-end stress to zero. In reality the binary resolved **hot**, and the correct *ex-post* S0 for a hot-CPI, hike-odds-up tape is mildly **negative** for financials (funding/duration), not zero. The morning's own note that "CPI is the load-bearing binary" was correct; it simply assigned the wrong sign to the resolution. This is a *knowable-at-open* limitation, not a process error — see §3.

**S1_SECTOR_FACTORS = 0 (capped per 09-10 lesson).** *Verdict: correct, and the cap saved the call.* The 09-10 lesson said: when 1d rel is not confirmed by the sector's own tape, do not score the long-end/credit channel as an independent negative on the macro narrative alone. 1d rel was +0.27% (below the 08-18 ≥ +0.4% gate), so the transmission channel was *not* confirmed — capping S1 at 0 was right. Had the morning stacked the long-end/credit channel into S1 *and* S0, it would have double-counted the same macro impulse. The cap is vindicated: XLF's actual relative move was only −0.18%, i.e. the sector-specific channel was indeed weak, exactly as the cap implied.

**S2_BREADTH = 0.** *Verdict: correct.* No live premarket BKX/XLF breakdown was confirmed, and the actual relative move (−0.18%) is consistent with "no breadth event," not a breadth failure. The 08-28 anti-triple-count lesson held.

**S3_FLOWS_POSITIONING = 0.** *Verdict: correct.* Trailing outflows, no fresh spike; nothing in the actual tape suggests a flow-driven move. Not a driver either way.

**S4_ETF_TAPE = +0.5.** *Verdict: the one component that pointed the wrong way.* The morning read the modestly-positive 1d rel (+0.27%) as a mildly *positive* base case and let S4 carry +0.5, producing the "mild divergence" (leading sum 0 vs S4 +0.5) that it resolved by "trusting the live macro over the modest 1d tape." In fact the 1d rel was *stale* — it described 09-10, and the live macro (hot CPI) reversed it. The morning's own self-audit flagged this tension and chose the wrong side of it. The lesson: **a +0.27% 1d rel is below the confirmation gate and should not be scored as a positive S4 when a same-day binary is pending** — it is noise, not signal.

**Multiplier 0.9, confidence 0.5.** *Verdict: reasonable.* The 0.9 mult correctly tempered magnitude; confidence 0.5 correctly reflected the binary. The failure was not in the multiplier but in the *sign* of the binary resolution.

---

## 3. Interactions / double-count / knowable-at-open test

**Double-count check:** The morning avoided the classic error — it counted oil/yields **once** in S0 and explicitly refused to restack the long-end/credit channel into S1 (09-10 cap). That discipline is why the call was only mildly wrong rather than badly wrong. Good process.

**The real interaction the morning mis-weighted:** It treated "oil falling" and "CPI pending" as *partially offsetting* (oil relief vs binary risk), netting S0 to 0. But these are not independent offsets — **oil is an input to CPI**, so a hot CPI print *overrides* the oil-relief signal. The morning implicitly treated them as two separate votes when one is downstream of the other. That is a genuine interaction error, and it is the single most instructive item in this review.

**Knowable-at-open test:** **Partially.** The *existence* of the CPI binary was fully knowable and the morning flagged it as load-bearing — good. The *resolution* (hot vs cool) was **not** knowable at the open; it is a genuine coin-flip event. So the morning cannot be faulted for not knowing the print. It *can* be faulted for:
- assigning S4 a positive score on a stale, sub-gate 1d rel while a binary was pending (should have been 0 or near-0), and
- netting oil-relief against binary risk to a clean zero rather than holding S0 at a small negative skew given the sector's already-red 3d/1w relative tape and stress-zone long end (30Y 5.28%).

Neither error was large, but both pushed the call toward "flat" when the correct *pre-print* posture was "flat-to-mildly-down relative, with the binary as the swing factor."

---

## 4. Outliers inside the sector

- **The gap-and-fade in XLF itself** is the notable intra-sector outlier: open 57.44 → close 57.25, i.e. the ETF gave back its opening gain while SPY did not. That is the fingerprint of a **hawkish-CPI fade concentrated in rate-sensitive financials** — money-center banks and rate-sensitive names sold into the print while the broad index held. This is consistent with the "hot CPI → hike odds up → funding-cost headwind" read and is the cleanest internal confirmation of the driver.
- **Foreign/Canadian banks (BBVA, BCS, BNS)** were correctly excluded as XLF drivers; no evidence they moved the ETF.
- **No single-name blowup or melt-up** is implied by a +0.67% ETF move with −0.18% relative — this was a **macro-driven, broad, mild** session, not an idiosyncratic one.

---

## 5. Scorecard

| Component | Morning | Ex-post fair | Error |
|---|---|---|---|
| S0 shared macro | 0 | ~−0.5 (hot CPI, hike odds up) | sign/magnitude |
| S1 sector factors | 0 | 0 | none (cap vindicated) |
| S2 breadth | 0 | 0 | none |
| S3 flows | 0 | 0 | none |
| S4 ETF tape | +0.5 | ~0 (stale, sub-gate) | over-positive |
| Direction | flat | up (mild) | **HIT** (flat≈up-mild) |
| Magnitude | flat | mild | **MISS** (actual mild, called flat) |
| Relative | — | −0.18% | underperformance not anticipated |

**Direction:** the "flat" call is a soft hit — XLF rose +0.67%, which is mild-up, and the morning's own band logic ("no absolute up, mult ≤1.0") leaned flat. Reasonable.
**Magnitude:** called flat, actual mild — a **miss** on the band, consistent with the rolling mag record (0.5) which the morning itself flagged as a temper.
**Relative:** the morning's S4-positive lean implied XLF would at least match SPY; it underperformed by 0.18%. Small, but the *sign* was wrong.

---

## 6. Lessons to promote

1. **When a same-day high-impact binary is pending, do not score S4 on a sub-gate 1d relative tape.** A +0.27% 1d rel (below the 08-18 ≥ +0.4% gate) is noise; scoring it +0.5 created a false "mild divergence" that the morning then resolved in the wrong direction. Set S4 ≈ 0 when a binary is pending and the 1d rel is sub-gate.
2. **Oil and CPI are not independent votes — oil is an input to CPI.** Do not net "oil relief" against "binary risk" to a clean zero. If CPI is the load-bearing print, the oil move is *subordinate* to it; hold S0 at a small skew rather than a symmetric zero.
3. **The 09-10 S1 cap is confirmed and should be kept.** Capping the long-end/credit channel at 0 when the sector's own tape doesn't confirm it prevented a double-count and kept the error small. This is a *good* lesson, not a failure.
4. **A gap-and-fade in the ETF while SPY holds is the tell for a hawkish-rate fade in financials.** Add this as a live intraday confirmation pattern for the sector.

---

OUTCOME_BEGIN
SECTOR: Financial
ETF: XLF
ETF_PCT: 0.67
SPY_PCT: 0.85
REL_PCT: -0.18
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Hot August CPI (0.4% m/m, 3.4% y/y) lifted Fed hike odds, a duration/funding headwind that made financials lag a green SPY tape.
KEY_INTERACTION: Morning netted oil-relief against CPI-binary risk to a symmetric S0=0, but oil is an input to CPI — the hot print overrode the relief, so S0 should have skewed mildly negative.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Direction (flat≈mild-up) defensible and the 09-10 S1 cap was vindicated, but S4 was over-scored on a stale sub-gate 1d rel and the CPI binary resolved hot, producing a mild magnitude miss and a small relative-underperformance miss.
OUTCOME_END