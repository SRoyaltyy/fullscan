# Sector Prediction — Industrials — 2026-10-01

- news_mode: **on**
- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **down**
- predicted_magnitude_band: **mild**
- total_score: **-0.378** (mult 0.9)
- regime: mixed
- divergence_flagged: **True**
- engine: v2 · tape_anchor **1.308** (ES +0.17%, ER2 +0.08%, HG +0.66%) · index_carry **-0.336** (general -1.343) · llm_overlay **-1.35** (raw -1.35)

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-30):
  1d: XLI -1.27% | SPY -0.21% | rel -1.07%
  3d: XLI -2.02% | SPY -1.13% | rel -0.89%
  1w: XLI -1.83% | SPY -0.67% | rel -1.16%
  1m: XLI -4.40% | SPY -0.33% | rel -4.07%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch; `openclaw memory status --index` / `openclaw memory index --force` would rebuild). Used the injected Industrials scoreboard + 08-11..09-25 sector logs + the in-prompt active-lesson pack + the 09-30 News Judge/Finviz digest only. Rolling dir=0.5 / mag=0.8 (n=10); last 30 dir=0.357 / mag=0.357 (n=28). Last graded 09-25: predicted down/mild vs XLI **+0.9477%** / SPY +0.5435% / rel +0.4041% — **dir MISS, mag MISS** (the sector's own spine print, durable goods 8:30 ET, landed with core capex +1.6% and the card had pre-scored the miss). Prior: 09-24 down/mild HIT (gap-down that stuck), 09-23 flat/flat HIT (relative MISS unexpressed), 09-22 down/mild MISS (PM:XLI −0.75% died at the cash open), 09-21 flat/flat MISS (absolute flat, relative collapse −1.44%), 09-18/09-17 flat/flat dir MISSes on tiny leftover-gap prints, 09-16 flat/flat HIT, 09-15 down/mild HIT, 09-14 down/notable HIT, 09-11 up/mild HIT, 09-10 down/mild HIT. **Governing today: 09-25 (A) — when the sector's own spine print is scheduled before the cash open, the pre-open score is PROVISIONAL; do not score S1 = −1 on pre-print MAP HEAT while the spine print is pending; do not invoke a governing lesson whose stated precondition is unmet (the 09-24 outside-±0.5% clause requires |ES|/|NQ| OUTSIDE the band — ES +0.17% / NQ +0.50% sleeve is INSIDE/at the edge, so the governing lesson is 09-22, not 09-24); count independent fresh negatives, not restatements; a sleeve-specific de-rate (VRT/PWR/FIX) is not an ETF-level driver (08-18 cuts both ways).** 09-24 (A) — score-vs-tape flatten needs a cash-open tape or a worse-than-index PM gap that is still the open, not a flat PM quote; when |ES|/|NQ| are outside the mixed ±0.5% band, derive absolute direction from the ES/NQ sign. 09-23 (C) — record an explicit RELATIVE-OUTPERFORMANCE lean when 1m rel ≤ −5% AND rotation-out CARRIED AND index leadership concentrated in a non-sector complex AND PM flat-to-slightly-negative, while keeping absolute flat. 09-22 (A) — mixed T+1 with |ES|/|NQ| inside ±0.5% is NOT a 09-21 tape; do not mint down/mild from MAP HEAT + 1m lag + a PM quote. 09-21 (A) — "index rallies, my sector doesn't" is a signed relative signal, not three zeros; derive direction from ES/NQ sign, breadth from all four. 09-17/09-16 keep-flat on an unsigned post-paid-FOMC card. 08-27 — 1w/1m laggard forbids up. 08-18 — cap S1 at 0/+1; GEV/FIX/VRT must not raise or sink the ETF. 08-11/08-12 supply-shock cap — verify live oil sign. 09-16 — oil-down ≠ S1 trucking relief. 09-14 S4=−1 — needs live oil/duration + worse-than-index PM gap. 09-11 — four-index ≥+0.5% unanimous gate. 09-09 — emit-down needs a live supply shock with confirming negative tape. 09-10/09-04 — score the lag once, as a condition. Fed-speaker lesson — same-session voting-Fed remarks while hike odds are contested ⇒ keep S0 directionally 0, cut confidence, mark unresolved-policy event day. DO-INSTEAD (sector_industrials, 09-21/09-22 losses): "when score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild" — **NOT binding as a flatten today**: the leading factors are net-negative-to-mixed and the sector's own tape is negative on 1d/3d/1w/1m (score and tape AGREE negative on the relative axis), but the absolute axis is genuinely two-sided because the sector's own spine print (ISM manufacturing, 10:00 ET) is pending. Open experiment (`sector_industrials`): keep direction, shrink confidence on modest |score| — **applied**. Checklist: (1) open experiment applied (shrink confidence); (2) missing factor that flipped 09-25 was pre-scoring a pending spine print — not repeated; (3) oil / paid FOMC / 1m lag / PM gap / BA MAX 10 each counted once; (4) S0 vs S1 split — S0 carries the dovish-PCE-vs-24yr-yield fight, S1 carries the pending ISM + BA.

---

# Sector Prediction — Industrials — 2026-10-01

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **mild**
- regime: mixed
- divergence_flagged: **True** (leading factor sum is net-negative on the relative axis while the absolute tape is two-sided into a pending spine print)

## Channel 1 sector ETF tape (as given, not re-derived)

```
ETF XLI vs SPY (yfinance, through 2026-09-30):
  1d: XLI -1.27% | SPY -0.21% | rel -1.07%
  3d: XLI -2.02% | SPY -1.13% | rel -0.89%
  1w: XLI -1.83% | SPY -0.67% | rel -1.16%
  1m: XLI -4.40% | SPY -0.33% | rel -4.07%
```

XLI is a **1d/3d/1w/1m relative laggard**, but the 1m lag is **−4.07%**, i.e. **not** the ≤ −5% deep-laggard condition that 09-10/09-04/09-23 were written around. That matters: the 09-23 relative-lean conjunction requires 1m rel ≤ −5%, and it is **not met**. So no relative-outperformance lean today — the lag is a *condition*, scored once.

---

## 1. Shared macro as it hits Industrials — S0 = 0

This is a **genuinely two-sided macro session**, not 09-24/09-25's one-way hawkish smash and not 09-21's clean AI-beta rally.

**The dovish leg (real, same-session, knowable at the open).** News Judge #1 (ranked dominant): *"Fed hike-odds collapse after cooler PCE — October hike now <50%, December pushed out (Goldman)."* Channel 2 confirms the transmission is current, not stale: KPMG (09-30) "Cooler on paper, hot underneath — PCE rose 0.3% m/m and 3.4% y/y, both undershooting 0.4% / 3.7%; benchmark revisions shaved ~0.3pp from annual inflation"; Yahoo Finance "PCE inflation eases to 3.4% — cooling the case for another Fed rate hike"; NY Post "likely delaying another rate hike until December." That is a **front-end repricing in the dovish direction**, and it is the single largest same-session macro input.

**The hawkish leg (also real, and it is the level, not the change).** News Judge #2: *"US 10Y yield at 24-year high / global bonds gripped by fiscal worries (Reuters)."* Channel 1 confirms: **DGS10 5.26 (+0.02 1d, +0.30 1w, +0.53 1m)**, **DGS30 5.59 (+0.03 / +0.30 / +0.37)**, **DFII10 2.91 (+0.01 / +0.28 / +0.49)**. The 1m moves are large — this is a **persistent real-yield backup**, and the 5-day 10Y–SPX corr is **−0.631**, so the yield level is a live multiple constraint. Channel 2: CNBC (09-26) "10-year Treasury yield is at its highest in 19 years… leapt to 5.23% on Friday for its highest level since 2007"; World Government Bonds quotes **5.348%** as of 10-01 08:15 GMT. Live Finviz: 10Y note **−0.03%**, 30Y **−0.06%** — a tiny price dip, i.e. the *session change* is marginally supportive while the *level* is the binding constraint.

**These two legs fight, and the fight is unresolved at the open.** Per the Fed-speaker lesson and the 09-22/09-23 pattern, I do **not** encode either as fully paid. S0 = 0 directionally, with confidence cut. Note the asymmetry: the dovish leg is a *change* (hike odds <50%), the hawkish leg is a *level* (24-yr high yield). Levels bind multiples; changes move tape. That is why the absolute call is flat, not up.

**Futures: green but not confirming a cyclical bid.** Channel 1 Finviz: S&P **+0.20%**, Nasdaq **+0.41%**, RTY **+0.08%**, DJIA **+0.11%**. The overnight sleeve: `ES=F +0.17% / NQ=F +0.50%`. Both sources agree in sign (green) but **ES is inside the 09-22 mixed ±0.5% band** (+0.17% sleeve / +0.20% Finviz) and NQ is at the edge (+0.50% sleeve / +0.41% Finviz). Per 09-25's correction, the **09-24 outside-band clause does NOT fire** — the governing lesson is **09-22**, not 09-24. 08-21's ES/NQ ≥ +0.3% gate is **partial** (NQ only). 09-11's unanimous ≥ +0.5% across all four is **off** (RTY +0.08%, DJIA +0.11%). **An index rebound is not an XLI participation certificate.**

**Sector PM board: XLI is absent, and the leaders are the wrong complex.** Channel 1: XLB −0.02%, XLC +0.50%, XLE −0.31%, XLF −0.41%, XLK +0.58%, XLP −0.26%, XLU +0.20%, XLV −0.53%, XLY −0.21%. **XLI is not on the board.** The bid is **XLK +0.58% / XLC +0.50%** — growth/duration, exactly the complex XLI is *not*. XLF −0.41% and XLV −0.53% are red. This is the 09-21 shape (index green, sector absent, leadership in a non-sector complex) but **weaker**: the index itself is only +0.20%, not +1.55%.

**Oil: down on the operator tape, mixed on the sleeve, level still elevated.** Finviz WTI **$104.16 (−1.59%)**, Brent **$107.67 (−1.02%)**; sleeve `CL=F +2.17% 1d / BZ=F −2.82% 1d` — the two sources **disagree in sign**, the same conflict pattern as 09-15..09-25. Channel 2 (Angel One, 10-01): "Crude oil prices were largely unchanged in early Asian trading… as investors assessed developments in US-Iran peace talks and signs of recovering crude exports from the Gulf." So the live change is **flat-to-lower on diplomacy**, not a kinetic increment. Per 08-11/08-12 the supply-shock cap **does not fire** (no live escalation). Per 08-13, Hormuz is the **stale leg**. Per 09-16, oil-down is **not** S1 trucking/air relief. Count oil **once, here**. The ~$104–108 level remains a **cost LEVEL** for transports and manufacturers — a mild margin headwind, not a same-session shock.

**Globals split, USD flat, vol low.** Asia composite **+0.79%** (Nikkei +3.3%, Kospi +1.95%, but ASX200 −1.99%). Europe **−1.06%** (FTSE −1.51%, CAC −1.14%, DAX −0.71%) — a real drag, and Europe is the higher-signal leg per the general factor weights. DXY **99.32 (−0.02%)** flat on the day but **+0.37% 1d / +2.16% 1m** — a mild exporter headwind, not enough for S0 = −1. VIX **16.51 (+0.17 1d, +0.84 1w)**, VIX/VIX3M **0.899** — contango, no stress. Fear & Greed **29** (per Channel 2 premarketnow) — fear, but not capitulation.

**Net S0 = 0.** Dovish PCE change (+) vs 24-yr-high yield level (−) vs Europe −1.06% (−) vs flat USD / low VIX (0). Two-sided, unresolved, and the sector's own spine print is pending. Confidence cut.

---

## 2. Sector spine and secondary factors — S1 = −1

**The spine print is PENDING, and this is the whole ballgame.** Channel 2 (multiple sources, incl. stockmarkethours.org, therighttrader, financecalendar): **ISM Manufacturing PMI for September releases TODAY, October 1, 2026, at 10:00 AM ET** — i.e. **30 minutes after the cash open**, during regular trading. Context: the **August** print (released 09-01) came in at **54.6%**, below the 55.2% consensus and down from July's 55.6%. So the sector enters with an expansion-but-decelerating spine.

**Per the 09-25 lesson, I must NOT pre-score this print.** The 09-25 error was scoring S1 = −1 on pre-print MAP HEAT while the spine print was pending, then watching core capex +1.6% land and XLI close +0.95%. The corrected behavior: the pre-open S1 is **provisional**, and the honest score reflects *uncertainty*, not a directional bet. I therefore do **not** score "ISM contraction" (it isn't — August was 54.6, expansion) and I do **not** score "ISM manufacturing / new orders expansion" as a confirmed positive (it's unprinted). The pending print is a **variance source**, which is a magnitude argument, not a direction argument.

**What IS knowable at the open, and it is genuinely mixed:**

- **Durable goods / CapEx upside — HIT, but already printed and already traded.** Channel 2: Census released the August advance durable goods report **09-25**; Reuters "US core capital goods orders point to robust growth in business spending on equipment"; Action Forex "US Durable Goods Stall on Transport, but Core Capex Orders Accelerate"; KPMG "Durable goods headline understates strength"; Haver "U.S. Durable Goods Orders Flat in August on Nondefense Aircraft Weakness." So the **core capex engine is running** — that is a real, structural positive for the spine. But it printed **six days ago** and XLI has fallen −1.27% (1d) and −1.83% (1w) since. Per 09-04/09-10, score it **once**, as a **condition**, not as a fresh same-session catalyst. It supports the *level* of S1 (why I don't go to −2 or −3) but it is not a same-day up-driver.

- **Aerospace & defense — fresh NEGATIVE, and it is index-relevant.** News Judge #4 (ranked): *"Boeing 737 MAX 10 certification halted by FAA over software glitch — 31% of undelivered 737 order book at risk."* Channel 2 confirms with primary sourcing: CNBC (09-28) "FAA: Boeing 737 Max 10 certification delayed by software issue"; WSJ "FAA to Delay 737 MAX 10 Approval Over New Software Glitch"; Aviacionline quotes FAA Administrator Bryan Bedford saying the delay stands "until the agency is satisfied that there is no problem, **without giving a timeline**." The glitch affects **VNAV mode during go-arounds** in the flight management computer. This is a **hard, fresh, index-relevant** industrial catalyst with supply-chain read-through (BA, SPR, aero-suppliers). **BA is a top-5 XLI weight.** Note the nuance: BA premarket is roughly **flat-to-slightly-up** ($186.58 vs $186.05 close, +0.28% per Public.com; stockmarketwatch shows −0.68%) — the market is treating it as a **timeline slip, not a program cancellation**. So this is a **mild** S1 negative, not a severe one. Per the sector layer's DO-NOT: I must not let a single defense award cancel ISM weakness — and symmetrically, I must not let a single BA certification slip sink the ETF. It is one weight, one program, one timeline.

- **Grid / electrical equipment backlog (AI power) — structural positive, but 08-18 caps it.** Channel 2: GE Vernova Q2 2026 (07-22) — **orders $24.2B, +88% organically**, **backlog $176B**, Gas Power equipment backlog and slot reservations **100 → 116 GW**, **>$5B of 2026 YTD Electrification data-center orders**; Eaton Electrical Americas orders **+41%**, Electrical Global backlog **+103%**. This is a genuine multi-year supercycle and it is **semi-independent of classic ISM** (per the sector layer). **But 08-18 is explicit: GEV/FIX/VRT must not raise or sink the ETF.** It is a **cushion on the level**, not a same-day driver. Score it as context, not as a signed S1 point.

- **Freight / trucking / rail — mildly positive, not a recovery.** Channel 2: AAR (09-23) — North American rail volume for the week ending Sept 19 totaled **340,379 carloads, +3.1% y/y**, and **387,419 intermodal units, +6.0% y/y**; total combined **+4.6%**. Railway Age: "AAR: U.S. Rail Volume Up Slightly Through 2026." Cass Freight Index (FRED, through Aug 2026) shows shipments **stabilizing** after disruptions, with truckload rates firming. So: **not a freight recession, not a freight recovery** — a slow grind. Score it **0**, not +1 (no recovery) and not −1 (no recession).

- **Reshoring / industrial policy — no fresh same-session catalyst.** FY2026 defense appropriations are enacted ($839.2B, P.L. 119-75, per CRS); the reconciliation add (~$150B assumed) is a **level**, already in the tape. No new award today. Score **0**.

- **Construction slowdown — mixed, no fresh print.** Lumber **562.0 (−1.32%)** is soft; copper **6.489 (+0.66%)** and aluminum **3430.75 (+1.10%)** are firm. Steel HRC **1230.0 (−0.16%)** flat. No same-session construction data. Score **0**.

**Net S1 = −1.** The pending ISM (variance, not direction) + the fresh BA MAX 10 certification halt (mild negative, one weight) + the already-printed core-capex strength (condition, not catalyst) + the GEV/Eaton backlog (capped by 08-18) + rail +3.1%/+6.0% (grind, not recovery). The honest read is a **mild net negative with a large pending-print variance band** — which is exactly why the magnitude is **mild** and the direction is **flat**, not down.

---

## 3. Breadth inside the sector — S2 = −1

The sector's own tape is **decisively negative on the relative axis across all four horizons**: 1d rel **−1.07%**, 3d rel **−0.89%**, 1w rel **−1.16%**, 1m rel **−4.07%**. That is a **uniform** relative lag — not a one-day wobble.

Channel 2 corroborates the breadth failure: CRI Weekly Sector Rotation Report (4 days ago) — *"Materials, Industrials, Real Estate, Consumer Discretionary, and Utilities are all in Laggard quadrants. **Industrials and Real Estate are now in Distribution.**"* That is a **deterioration** signal, not a base. Channel 2 also: XLI RSI **32** (near-oversold), price **$168.78** (09-29) **below its 50-day average of $177.70** — a ~5% gap below the 50-dma. And the 09-30 session itself: *"75% of stocks fell as the bond rout worsened"* (tradingstrategyguides) — a **narrow, AI-masked tape**, with the Dow −443.87 points (−0.86%) to 50,906.05, its **lowest close since early June**, and only **8 of 30 Dow components rising**. XLI −1.27% vs SPY −0.21% on that day is consistent with a broad industrial de-rate, not a single-name event.

**But per 09-22 and 09-25, breadth is a CONDITION, not a same-session participation failure** — unless names are failing a *live up-index*. Today the index is only +0.20% (Finviz) / +0.17% (ES sleeve), which is **inside the mixed band**. So the 09-21 "index rallies, my sector doesn't" template **does not fire** (that needed SPY +1.55%). The breadth failure is real but it is a **level/condition**, and I score it **once** — here in S2 — and **not again** in S4 (09-04/09-10 score-once).

**Net S2 = −1.** Uniform 4-horizon relative lag + Distribution quadrant + RSI 32 below the 50-dma + a 75%-of-stocks-fell tape. Scored once.

---

## 4. Flows / positioning / crowding — S3 = 0

No fresh, same-session flow data is available. Channel 2 (SSGA, Yahoo Finance, clearank, Trefis, AlgovestIQ) gives **holdings and technical context**, not flow prints: XLI at **$168.78** (09-29), **RSI 32**, **below the 50-dma of $177.70**. Channel 2 also notes the structural backdrop: *"Industrials ETFs XLI, VIS, PRN, and PSCI have posted strong 2026 returns amid AI infrastructure spending and defense demand"* (Yahoo, 07-08) — i.e. the **year-to-date** story is still constructive even as the **1m** tape lags. That is a **crowding-adjacent** setup (a popular 2026 theme now in Distribution), but I have no flow print to sign it.

Per the taxonomy: "Sector ETF outflow / volume dry-up" would be a near-term demand negative; "Sector ETF inflow / relative volume spike" would be an attention positive. **Neither is confirmed.** RSI 32 near-oversold is a **washout-setup** context (a mild positive per the amp/damp line: "[+] washout setup later [−] near-term demand") — but it is a *later* positive, not a same-session one. Score **0**. I will not manufacture a flow signal from a technical level.

**Net S3 = 0.**

---

## 5. ETF tape — S4 = 0 (confirmation only, and already scored)

Per 09-04/09-10/09-22/09-25: **score the relative lag ONCE.** It is scored in S2. S4 must be **0** unless there is an **independent, same-session tape fact** not already captured by the lag. Is there one?

- The 1d rel −1.07% is the same fact as the 1m rel −4.07% measured over a shorter window — **not independent**.
- The PM board shows XLI **absent** — that is a *neutral* fact (no quote), not a negative one. It is not a worse-than-index gap (09-14's convert-the-gap clause requires a **worse-than-index PM gap that is still the open**; an absent quote is neither).
- There is **no fresh gap** in XLI's own premarket (it is not on the board), no volume spike, no intraday reversal.

So S4 = **0**. The tape is **negative on the relative axis** and I have already paid for it in S2. Paying it again in S4 would be the exact double-count 09-04/09-10/09-22 forbid.

**Net S4 = 0.**

---

## 6. Divergence and self-audit

**Divergence flagged: TRUE.** The leading factor sum (S0 0 + S1 −1 + S2 −1 + S3 0 = **−2**) is **net-negative**, while the tape confirmation (S4 = 0, and the index itself is green) is **not confirming a down move**. Per the shared method: *when leading factor sum fights the tape confirmation score, flag it; trust factors over tape.* Here the factors are net-negative **on the relative axis** but the absolute axis is genuinely two-sided because (a) the dovish PCE repricing is a live positive change, (b) the sector's own spine print is **pending at 10:00 ET**, and (c) the index is green, not red.

**The honest resolution:** the factors say **"XLI underperforms a flat-to-modestly-green SPY"** — a **relative** call — while the **absolute** call is genuinely **flat** with a **mild** band, because the pending ISM print is a two-sided variance source that flat cannot fully capture (per the sector-etf-prediction lesson: *when a high-impact macro print is pending at the open and identified as two-sided, widen the magnitude band to at least mild regardless of component scores*).

**Self-audit:**
- **Lens:** this is an **XLI near-session environment** call, not SPX, not a stock pick. ✓
- **Band:** **mild**, not flat — because ISM at 10:00 ET is a live two-sided binary. Not notable — because ES is inside the mixed ±0.5% band (09-22 governs, not 09-24) and there is no confirmed directional index move. ✓
- **Skew:** the skew is **downward on the relative axis, symmetric on the absolute axis**. I am not minting a down call from MAP HEAT + 1m lag + a PM quote (the 09-22 error). ✓
- **Same-shock double-count:** oil counted **once** (S0). The 1m relative lag counted **once** (S2, not S4). The BA MAX 10 halt counted **once** (S1). The dovish PCE counted **once** (S0). The pending ISM is **not scored as a direction** — it is a variance argument for the band. ✓
- **Single-ticker must not drive the sector ETF call:** BA is a top-5 weight and its certification slip is real, but it is **one program, one timeline**, and BA premarket is roughly flat — so it contributes a **mild** S1 negative, not the thesis. GEV/Eaton backlog is capped by 08-18 and does **not** raise the ETF. ✓
- **Governing-lesson precondition check (09-25's correction):** the 09-24 outside-±0.5% clause **does not fire** (ES +0.17% sleeve / +0.20% Finviz is inside). The governing lesson is **09-22**. ✓
- **Pending-spine-print check (09-25's correction):** ISM is **pending at 10:00 ET** and I have **not** pre-scored it as a miss. S1 = −1 reflects the **BA halt + the already-printed core-capex condition + the capped grid backlog**, not a pre-scored ISM miss. ✓
- **08-27 forbid-up:** XLI is a 1w/1m relative laggard (1w rel −1.16%, 1m rel −4.07%). **Up is forbidden.** ✓
- **Open experiment applied:** keep direction, shrink confidence on modest |score|. |leading sum| = 2, which is modest. Confidence cut to **0.42**. ✓

**Why not down/mild?** Because the 09-22 lesson is explicit: *mixed T+1 with |ES|/|NQ| inside ±0.5% is NOT a 09-21 tape; do not mint down/mild from MAP HEAT + 1m lag + a PM quote.* ES is inside the band. And the 09-25 lesson adds: *do not pre-score a pending spine print.* Both point to **flat**, not down.

**Why not flat/flat?** Because the sector-etf-prediction lesson is explicit: *when a high-impact macro print is pending at the open and identified as two-sided, widen the magnitude band to at least mild regardless of component scores.* ISM at 10:00 ET is exactly that. A flat band would understate the variance.

**Why not up/mild?** Because 08-27 forbids up on a 1w/1m laggard, the index is only +0.20% (not a confirmed rally), XLI is absent from the PM board while XLK/XLC lead, and the 24-yr-high yield level is a live multiple constraint. The dovish PCE is a **change**, not a **level** — and levels bind.

---

## 7. Final call

**XLI: flat / mild.** Absolute direction flat, magnitude band mild, with an explicit **relative-underperformance lean** (XLI expected to lag a flat-to-modestly-green SPY) that the additive framework cannot express as a signed absolute direction. Regime **mixed**. Confidence **0.42** (cut for the pending ISM binary, the dovish-PCE-vs-24yr-yield fight, and the modest |score|).

**Path qualifier:** if ISM prints **≥ 55** (expansion, new orders up), XLI can gap-and-hold **up/mild** into the close — the core-capex condition is already supportive and the dovish PCE change would then transmit. If ISM prints **< 53** (deceleration toward contraction), XLI can grind **down/mild** — the 1m relative lag would then be *confirmed* rather than merely *carried*. The **flat/mild** call is the honest pre-print read; the band is mild precisely because the print is the session.

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: -1
S2_BREADTH: -1
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.42
REGIME: mixed
DIVERGENCE_FLAGGED: true
SECTOR_RS_VETO_APPLIED: true
PENDING_BINARY: ISM_MANUFACTURING_1000ET
RELATIVE_LEAN: underperform
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.40|2026-10-01|https://www.cnbc.com/2026/09/29/stock-market-today-live-updates.html
Risk-off tape / flight to safety|MISS|0.55|2026-10-01|https://www.investopedia.com/stock-market-today-dow-jones-s-and-p-500-09302026-12148860
Real yields rising|HIT|0.70|2026-10-01|https://www.cnbc.com/2026/09/26/10-year-treasury-yield-is-at-its-highest-in-19-years-how-we-got-here.html
Real yields falling|MISS|0.60|2026-10-01|https://kpmg.com/us/en/articles/2026/august-2026-pce.html
USD strengthening|PARTIAL|0.45|2026-10-01|https://www.worldgovernmentbonds.com/bond-forecast/united-states/10-years/
USD weakening|MISS|0.50|2026-10-01|https://www.worldgovernmentbonds.com/bond-forecast/united-states/10-years/
Sector breadth expansion (% names up)|MISS|0.70|2026-10-01|https://tradingstrategyguides.com/stock-market-recap-september-30-2026-sp-masked-by-ai-put/
Sector breadth failure (ETF up, names flat)|MISS|0.55|2026-10-01|https://tradingstrategyguides.com/stock-market-recap-september-30-2026-sp-masked-by-ai-put/
Large-cap leadership inside sector|MISS|0.50|2026-10-01|https://www.thetrading.tools/sector-performance
Small/mid leadership inside sector|MISS|0.55|2026-10-01|https://www.fxbrokertrust.com/insights/us-stocks-close-mixed-as-dow-hits-lowest-level-since-june
High-beta leadership inside sector|MISS|0.55|2026-10-01|https://www.fxbrokertrust.com/insights/us-stocks-close-mixed-as-dow-hits-lowest-level-since-june
Low-beta leadership inside sector|PARTIAL|0.40|2026-10-01|https://www.thetrading.tools/sector-performance
Sector ETF inflow / relative volume spike|MISS|0.50|2026-10-01|https://www.ssga.com/us/en/intermediary/etfs/state-street-industrial-select-sector-spdr-etf-xli
Sector ETF outflow / volume dry-up|MISS|0.50|2026-10-01|https://www.ssga.com/us/en/intermediary/etfs/state-street-industrial-select-sector-spdr-etf-xli
Crowded long (extreme relative performance + valuation)|MISS

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': -1.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -5.0, 'divergence_flagged': True, 'total_score': -0.378, 'predicted_direction': 'down', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.415, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.218, 'score': 1.308, 'legs': [{'leg': 'ES', 'pct': 0.17, 'w': 0.8}, {'leg': 'ER2', 'pct': 0.08, 'w': 0.2}, {'leg': 'HG', 'pct': 0.66, 'w': 0.1}]}, 'overlay_score': -1.35, 'overlay_raw': -1.35, 'index_carry': -0.336, 'general_total': -1.343, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.5}, 'llm_confidence': 0.42}
```
