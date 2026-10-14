# Sector Reflect — Energy — 2026-10-13

Memory search is paused (index metadata missing). Diagnostic uses the injected Energy card, outcome, scoreboard, on-disk rubric, and active/candidate Energy lessons only.

**TRIAGE:** Reasoning, not tool/data. Direction **HIT** (up vs XLE **+1.80%**). Magnitude **MISS** (predicted **mild** vs actual **notable**). Live oil **was** offered ~1% at the open (CL −1.59% / live WTI ~$92.53); 08-11 rejected the stale Finviz $104 column. The cash driver was a **same-session** WTI reversal to **+$0.40 / $94.14**, diesel **~+5%**, and Hormuz still printing into US hours (UKMTO Tuesday hits, Iran navy, Kpler 2.9 vs 20.8 mb/d). That increment was only **partially** knowable at the open — discount A. Residual error is **B**: 08-12 “day-3 priced premium” plus the offered CL print were used as a **mild lid** even though the card already had the 09-23 rotation tell (XLE PM **+0.52%** leading NQ **−0.73%**) and a live Hormuz cluster. S0=0, S3=0, leftover-S4=0 were right. S1=+1 was the right **sign**; S2=0 was a PM-map call, not the band miss.

KNOWABLE_AT_OPEN = **partial**. Do not rewrite the open from diesel +5% or the WTI settle. Do not flip S1 to +2 (08-14 needs green CL). The fix is a **magnitude-lid veto**, not a new spine.

### CHECK 1 — LESSON MATCH
No unapplied twin. **08-12** matched and **fired** (1w rel +3.61% borderline; continuation Hormuz; sub-1.5% oil increment) — that is the miss, not a retrieval failure. 08-12’s own ceiling is **at most up/notable**, not a mandatory mild pin. **08-14** correctly **off** (CL not green). **09-23** matched the rotation geometry and **was** used for sign (up, not down). **10-02** is the offered-snapshot cousin but that card **signed down** on a laggard gap; today the ETF was the leader. **09-28** gap-fade **off** (PM +0.52% < 1%). **10-01** is the inverse (green oil, red PM). New card is an 08-12 **boundary**, not a duplicate.

### CHECK 2 — BACKWARD TEST
Narrow trigger: XLE board-leader vs NQ ≤ −0.5%, Hormuz still in the same-morning cycle, live crude offered ~1% (not a collapse), 1m not crowded, majors not breaking → do not let 08-12 + offered CL pin **mild** if that is the only lid.

- **09-23** (+0.96% mild, dir MISS): cousin, but |NQ| < 0.5% — trigger **does not fire**; would not have forced notable.
- **10-02** (+0.40%, dir MISS): offered snapshot, XLE **laggard** — no fire.
- **09-28** (+0.10%): oil **green**, gap ≥1% — no fire.
- **10-01** (+1.95%): oil green, PM red — no fire.
- **09-21/09-22/09-25** down HITs: sector not the leader — no fire.

Helps 10-13; inert on the last-10 otherwise. Not a one-day fit if NQ ≤ −0.5% and live kinetic are required.

### CHECK 3 — CONFLICT SCAN
- **08-12:** narrow, don’t retire. Caps **severe** after a 1w rel > +4% continuation when oil is **up** and tape isn’t confirming. It does **not** also pin notable→mild just because CL is offered while XLE is leading a red-NQ board with live Hormuz.
- **08-14:** still owns green-CL + escalating kinetic → S1 can be +2. This lesson does **not** raise S1 on a red barrel.
- **09-23:** owns **direction** (don’t sign down). This owns the **mild lid** once direction is already up.
- **09-08/09-18 size_gate:** exception, not repeal. Size_gate still blocks notable on ordinary continuation. Exception only when the mild pin is 08-12 + offered CL **and** the board-leader/live-Hormuz/NQ≤−0.5% cluster is present.
- **10-02:** complementary (don’t sign down on a snapshot). This says don’t use that snapshot as the band lid either when equities lead.
- **08-21** overbought decoupling: 1w rel not > +5%, 1m rel −0.12% — off.

### CHECK 4 — APPLIED-LESSON REVIEW
- **08-11** live-oil verify: applied, **helped** (sign of barrel = down ~1%).
- **08-12** stale-surge: applied, **hurt** the band (over-read as a mild pin).
- **09-08/09-18 mag-discipline + size_gate:** applied, **hurt this print**, still the right default on mag 0.367.
- **09-23** rotation-bid: applied for sign, **helped direction**.
- **09-17 leftover-S4:** applied, **helped**.
- **09-10 crowded-long / 09-28 gap-fade / 08-14 green-oil / 10-01–10-02 laggard:** correctly **off**.
- Open **keep-dir, shrink-conf** (llm 0.45): **helped direction**, did not lift the lid.

### CHECK 5 — FALSIFIER
Same open (XLE leading vs NQ ≤ −0.5%, Hormuz still printing, CL offered ~1%, 1m not crowded, majors not breaking), overlay allowed to emit **up/notable**, yet XLE still closes **mild** (0.3–1.0%) or fades — then the 08-12 mild pin was right and this exception is wrong. If the book only rips after an **unknowable** afternoon kinetic/product print, do not treat open-to-close notable as knowable — keep mild.

**DIVERGENCE:** morning `divergence_flagged: False`. PM vs offered CL was narrated, not flagged. Close followed the equity leadership / WTI-product path, not the morning CL print. **none_flagged.**

**Verdict:** Category **B**. Keep **up**. Stop using 08-12 + a ~1% offered CL as the mild lid when energy is already the risk-off destination and Hormuz is still live.

LESSON_BEGIN
ERROR_CATEGORY: B
TRIGGER_PATTERN: Energy/XLE is the premarket board leader (green PM) against NQ ≤ −0.5% / a soft ES tape, Hormuz/Gulf kinetic is still in the same-morning news cycle (not leftover-only), live crude is offered only ~1% (not a collapse, not a ≥+1.5% spike), 1m relative is not crowded, and majors are not breaking — then 08-12 “day-3 priced premium” plus the offered CL print are used to pin the band at mild.
CURRENT_BEHAVIOR: Verifies the offered barrel, counts continuation Hormuz once at S1=+1, correctly calls up on the rotation tell, then lets 08-12 + size_gate + the −1% CL snapshot cap magnitude at mild as if the premium were already paid and the equity bid could not extend.
CORRECTED_BEHAVIOR: Keep direction up. Do not treat offered ~1% CL as the load-bearing magnitude lid when XLE is already leading a red-NQ board and Hormuz is still printing. 08-12 still blocks severe and still caps S1 at +1 on continuation; it does not also force mild. 08-14 still requires green CL to raise S1. Size_gate remains the default notable blocker; exception only when the mild pin is 08-12+offered-CL and the board-leader/live-kinetic cluster is present — then allow up/notable. Do not double-count Hormuz + WTI reversal + rotation as three S1 hits. If no same-morning kinetic/product increment is in the pre-open book, do not rewrite notable from the close.
EVIDENCE: 2026-10-13 predicted up/mild (S1=+1, S0=S2=S3=S4=0, mult 0.85, size_gate on, total 1.668) vs XLE +1.80% / SPY −0.01% / rel +1.81% (dir HIT, mag MISS). Open: CL −1.59%, PM:XLE +0.52%, NQ −0.73%, 1w rel +3.61%, 1m rel −0.12%. Close: WTI $94.14 +0.4% after a morning offer, Brent $105.69 −0.9%, diesel ~+5%, VLO +4.69% / MPC +3.96% / XOM +1.12% / CVX +1.27%. Knowable-at-open: partial.
LESSON_MATCH_CHECK: 08-12 matched and was applied — over-applied as a mild pin, not an unapplied-retrieval miss. 08-14 correctly off (oil not green). 09-23 matched rotation geometry and was used for sign. 10-02 is the offered-snapshot cousin but signed down on a laggard; 09-28/10-01 do not match.
BACKWARD_CHECK: helps 10-13; no fire on 09-23 (|NQ|<0.5%, actual +0.96% mild); no fire on 10-02 (laggard); no fire on 09-28 (green oil, ≥1% gap); no fire on 10-01 (green oil, red PM); inert on 09-21/09-22/09-25 down HITs.
CONFLICT_CHECK: narrows 08-12 (severe cap stays; mild pin does not); 08-14 still owns green-CL S1=+2; 09-23 owns direction; 09-08/09-18 size_gate kept as default with this cluster as the only notable exception; 10-02 complementary; 08-21 off (not 1w rel >+5%).
FALSIFIER: Same open (XLE leading vs NQ ≤ −0.5%, Hormuz still printing, CL offered ~1%, 1m not crowded, majors not breaking), model emits up/notable, yet XLE still closes mild (0.3–1.0%) or fades — then the 08-12 mild pin was right. If the rip waits on an unknowable afternoon kinetic/product print, do not treat notable as knowable.
DIVERGENCE_VERDICT: none_flagged
ACTIVE_LESSON_REVIEW: 08-11 helped; 08-12 applied and hurt the band; 09-08/09-18 size_gate applied, hurt this print, remain the default; 09-23 helped direction; 09-17 leftover-S4 helped; 09-10/09-28/08-14/10-01/10-02 correctly off; keep-dir/shrink-conf helped direction, did not lift the lid.
SECTOR: Energy
LESSON_END
