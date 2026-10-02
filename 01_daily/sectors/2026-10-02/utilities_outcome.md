# Sector Outcome — Utilities — 2026-10-02

Actuals: {'etf': 'XLU', 'pct': 0.3528210397472531, 'spy_pct': 0.742154083513169, 'rel': -0.38933304376591593, 'open': 39.880001068115234, 'close': 39.81999969482422, 'source': 'yf_download'}

# Sector Post-Session Review — Utilities (XLU) — 2026-10-02

## 0. FACTS

| Item | Value |
|---|---|
| XLU % | **+0.35%** |
| SPY % | **+0.74%** |
| Relative % | **−0.39%** |
| Open / Close | 39.88 / 39.82 |
| Actual direction | **up** |
| Actual magnitude | **flat/mild** (absolute), **lagging** (relative) |

Path note: the deterministic open/close pair (39.88 → 39.82) is a *net-down* intraday path, yet the reported ETF_PCT is **+0.35%** — meaning the prior close was ~39.68 and XLU gapped up at the open, then bled ~0.15% through the session. One third-party print (OHLCX) shows open 39.79 / close 39.95 / high 40.21 / low 39.55, which is directionally consistent (green day, intraday fade from the high) but not tick-identical to the deterministic feed. I treat the deterministic numbers as authoritative and the OHLCX print as corroboration of *shape* (early pop, mid-session fade, green close).

So the day was: **XLU up modestly, SPY up more, XLU lagged by ~0.4pp.** The morning call was **flat / flat**. Absolute direction was right (flat-to-mild up, not a smash either way); relative direction was wrong (XLU underperformed, and the morning read had leaned on a small positive duration residual that did not translate into relative strength).

---

## 1. WHAT DROVE THE SECTOR

**Primary driver: the post-NFP duration bid was real but small, and it was swamped by a risk-on/growth-led tape that utilities did not participate in.**

The morning's own Channel 2 had the key fact: soft NFP (+29k vs ~84–90k, U-rate 4.2%, AHE +0.1%) drove a same-session easing impulse — 10Y −6 bp to 5.18%, 2Y −6 bp, 30Y −3 bp, October hold ~84% on FedWatch. That is a genuine bond-proxy tailwind for XLU. It showed up: XLU closed green. But it was **pace-gated at 6 bp** (below the ≥10 bp "smash" threshold the playbook itself set), so it was never going to be a notable-band engine — and it wasn't.

The offsetting force was the one the morning flagged as a **headwind, not a cushion**: NQ-led risk-on. Channel 1 PM had XLK +0.78%, XLP +0.60%, XLI +0.62%, XLY +0.39% — a broad cyclical/growth bid with XLU flat-to-red and second-worst after XLE. The close confirmed that structure: **8 of 11 sectors rose Friday as growth led** (Benzinga, 2026-10-02 15:01 GMT), and the tape's own headline was "Rising yields are wreaking havoc on stocks" (Boston Herald, 2026-10-02 15:51 GMT) — i.e., the *long-end* story that mattered to equities was not the 2Y easing impulse but the persistent 1m/1w yield level (DGS10 +54 bp 1m, DFII10 +49 bp 1m). Utilities, as the purest long-duration equity proxy, sat on the wrong side of that.

Taxonomy alignment:
- **Rates falling (bond-proxy bid)** — HIT, but small. This is the +0.35% absolute.
- **Risk-on rotation away from utilities** — PARTIAL→effectively the relative driver. This is the −0.39% rel.
- **Real yields rising (level)** — the 1m structural descriptor that kept XLU from converting the easing impulse into relative outperformance.

The morning's own framing was correct on mechanism and wrong on net sign of the *relative* trade: it said "small positive duration, not a risk-off haven day" and "relative lag even if absolute catches a duration bounce (08-27 with a live yield impulse)." That 08-27 clause is exactly what happened — XLU caught the absolute bounce and lagged relatively.

---

## 2. AUDIT OF MORNING S0–S4 READS

**S0_SHARED_MACRO = +0.5 — CORRECT, well-sized.**
The morning paid the NFP/6 bp easing impulse in S0 only, half-weight, and explicitly refused to let it become a notable-band engine. XLU's +0.35% absolute is consistent with a small positive duration residual. The morning also correctly labeled the risk-on inputs as *offsets, not cushions* (09-11 rule) — and that is precisely how they behaved: they capped the duration bid rather than adding to it. No double-count: rates were not re-HIT in S1 or S4.

**S1_SECTOR_FACTORS = 0 — CORRECT.**
The morning refused to promote Amazon–Calvert Cliffs (T-1, Oct 1), APCo VA hearings (not a decision), ERCOT Batch Zero (Sep commentary), or AI-power into a same-session HIT. Nothing in the close contradicts that. The only live S1-relevant item was the risk-on rotation, which the morning scored as PARTIAL and did not let carry full weight because XLP +0.60% showed defensives were not uniformly dumped. That extra-confirm gate was the right instinct — but note it *understated* the rotation's relative bite: XLU lagged even though XLP rose, because the rotation was growth-led (XLK) rather than a clean defensive-to-cyclical dump. The gate was correct in logic, slightly too generous in effect.

**S2_BREADTH = 0 — CORRECT.**
No live % names-up print existed at the open; MAP HEAT captains all `none`. The morning refused to pay the collapsed 200d breadth (~3% above) as a second down sleeve while 3d/1w rel were green. The close does not give us a names-up print to grade against, so this stays a clean "checked, nothing material." The 24/7 Wall St. headline "Utility Stocks Are the Most Oversold Since 2023" (2026-10-02 19:45 GMT) is a *descriptor* of the same structural condition, not a same-session breadth HIT — correctly not paid.

**S3_FLOWS_POSITIONING = 0 — CORRECT.**
No Oct 2 flow print existed at the open; the morning refused to score on admitted absence of today's flow (09-25 rule) and correctly rejected "crowded long" given 1m rel −6.62% and near-52w-lows positioning. Nothing in the close changes that. The TradingView short-interest item (OKLO/BKH highest, RNW/SRE lowest, 2026-10-02 19:15 GMT) is a positioning descriptor, not a same-session flow HIT.

**S4_ETF_TAPE = +0.5 — DIRECTIONALLY RIGHT, but this is where the relative miss lives.**
The morning read 1d/3d/1w rel all green and set S4 = +0.5, with the 09-14 floor off. That was a fair read of the *pre-open* tape. But the morning's own divergence check said "leading S0–S3 = +0.5 vs S4 +0.5 — no fight," and its Channel 1 PM note said XLU PM −0.09% was a **relative lag** signal. The close vindicated the PM signal over the green 1d/3d/1w rel: XLU lagged SPY by 0.39pp. The morning had the right *absolute* S4 sign but did not let the PM relative-lag observation pull the relative call — it stayed flat/flat rather than flat-with-relative-lag.

**Multiplier 0.9 / confidence 0.58 — reasonable.** The morning shrank confidence on modest |score| (10-01 do-instead), which was appropriate. The `sector_rs_veto_applied: True` with `sector_rs_tape: {d1: -1.5, w1: -3.18}` is interesting — the pipeline applied a relative-strength veto that the prose did not surface, and that veto was *correct* in hindsight (XLU did underperform). Worth flagging as a prose/pipeline divergence.

---

## 3. INTERACTIONS / DOUBLE-COUNT / KNOWABLE-AT-OPEN

**Double-count check: clean.** NFP/6 bp in S0 only; rotation in S1 only; tape in S4 only. No sleeve was paid twice. The morning explicitly refused to re-HIT rates in S1 and refused to let 1m lag pay S2 *and* S4 (08-13). Good hygiene.

**The real interaction the morning under-weighted:** the duration bid (S0 +) and the risk-on rotation (S1 −) were **not independent** — they were two faces of the same soft-NFP print. Soft labor → lower front-end yields (helps XLU) *and* higher growth/risk appetite (hurts XLU relatively). The morning treated them as additive offsets and netted to a small positive. In reality the rotation leg had more relative bite than the duration leg had absolute lift, because the duration impulse was pace-gated at 6 bp while the risk-on impulse was broad (8 of 11 sectors up, growth leading). The morning's own 08-27 clause ("relative lag even if absolute catches a duration bounce") was the correct prior, and it should have pulled the *relative* call to lag rather than leaving it flat.

**Knowable-at-open test: PARTIALLY knowable.**
- The soft NFP print and the 6 bp easing impulse were **known** at the open (paid 8:30, snapshot 09:04).
- The risk-on/growth-led structure was **known** — Channel 1 PM had XLK +0.78% vs XLU −0.09%, NQ leading ES.
- The relative-lag conclusion was therefore **knowable at the open**: a pace-gated duration bid + a broad growth-led risk-on tape + XLU as the worst-but-one PM sleeve = absolute green, relative lag. The morning had every input to make that call and chose flat/flat instead of flat-with-relative-lag.
- What was **not** knowable: the exact magnitude of the fade (open 39.88 → close 39.82) and whether the duration bid would hold into the close. It did hold enough for a green close.

So: the *absolute* call was knowable and correct; the *relative* call was knowable and missed.

---

## 4. OUTLIERS INSIDE THE SECTOR

- **XLE −0.99% PM** was the only sleeve worse than XLU in the pre-open book — energy, not utilities, was the day's funding source. That is consistent with oil offering (CL=F −3.97% / BZ=F −2.48%) and reinforces that this was a growth-led, not a defensive-led, tape.
- **XLP +0.60%** — the defensive that *did* participate. This is the tell that the rotation was not a clean "dump defensives" day; it was a "buy growth, hold staples, sell utilities" day. Utilities' long-duration character, not its defensiveness, was the liability.
- **OKLO / BKH highest short interest; RNW / SRE lowest** (TradingView, 2026-10-02) — positioning dispersion inside the sector, not a same-session driver, but consistent with the "most oversold since 2023" descriptor and with the absence of a crowded-long unwind.
- **No single-name outlier drove the ETF.** CEG/VST/Amazon items stayed T-1 and did not move the tape. The move was macro/rotation, not idiosyncratic.

---

## 5. VERDICT

The morning got the **absolute** call right (flat-to-mild up, +0.35%) and the **mechanism** right (small pace-gated duration bid, risk-on as offset not cushion, no haven bid). It got the **relative** call wrong: XLU lagged SPY by 0.39pp, and the morning's own Channel 1 PM signal (XLU −0.09%, second-worst after XLE, non-haven) plus its own 08-27 clause ("relative lag even if absolute catches a duration bounce") pointed to exactly that. The pipeline's `sector_rs_veto` apparently caught it; the prose did not.

The lesson to carry: when a pace-gated duration impulse (6 bp, half-weight) coincides with a broad growth-led risk-on tape and the sector's own PM print is a laggard, the correct call is **flat absolute with relative lag** — not flat/flat. The morning had all three inputs at the open.

OUTCOME_BEGIN
SECTOR: Utilities
ETF: XLU
ETF_PCT: 0.35
SPY_PCT: 0.74
REL_PCT: -0.39
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Pace-gated post-NFP duration bid (10Y −6 bp) lifted XLU green, but a broad growth-led risk-on tape (8 of 11 sectors up, XLK leading) left utilities lagging relatively.
KEY_INTERACTION: Soft NFP produced two non-independent legs — lower front-end yields (helps XLU absolutely) and higher risk appetite (hurts XLU relatively); the rotation leg had more relative bite than the pace-gated duration leg had absolute lift.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Absolute call correct (flat/mild up, +0.35%); relative call missed — morning's own PM lag signal and 08-27 clause pointed to the −0.39pp underperformance it declined to call.
OUTCOME_END