# Sector Prediction — Industrials — 2026-09-16

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **7.288** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **5.964** (ES +1.14%, ER2 +0.08%, HG +0.66%) · index_carry **1.324** (general 5.297) · llm_overlay **0.0** (raw 0.0)

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-09-15):
  1d: XLI -0.64% | SPY -0.46% | rel -0.18%
  3d: XLI -1.00% | SPY -0.06% | rel -0.94%
  1w: XLI -3.19% | SPY -1.12% | rel -2.07%
  1m: XLI -9.47% | SPY -2.44% | rel -7.03%
```

MEMORY_CONFIRM: Memory index unavailable this run (embedding metadata mismatch; `openclaw memory status --index` or `openclaw memory index --force` would rebuild). Used injected Industrials scoreboard + 08-11..09-15 sector logs only. Rolling dir=0.5 / mag=0.3 (n=10); last 30 dir=0.35 / mag=0.2 (n=20). Last graded 09-15: predicted down/mild, actual XLI −0.636% / SPY −0.459% / rel −0.177% — **dir HIT, mag HIT** (gap-and-fade). Prior: 09-14 down/notable vs −1.42% (dir HIT, mag HIT), 09-11 up/mild vs +1.07% (dir HIT, mag HIT), 09-10 down/mild vs −0.72% (dir HIT, mag HIT), 09-09/09-08 flat/flat MISSes on oil-shock days. **Governing today: 09-15 (NONE) — better-than-index PM gap is a fade setup only when a live oil/duration shock is present; that precondition is OFF (oil down, long end quiet).** 09-14 S4=−1 for persistent lag does **not** fire as a down-driver: it required a live oil/duration stack plus a *worse*-than-index PM gap. 09-11 pending-binary + unanimous ≥+0.5% across all four futures → treat binary as neutral — **OFF** (Finviz ES +0.20% / NQ +0.41% / RTY +0.08% / DJIA +0.11%). 09-03 — pending high-impact print ⇒ magnitude **at least mild**, not flat. 09-09 emit-down — **OFF** (no negative leading score for the tape to confirm). 09-10 decay — 1d rel −0.18% is a decaying/sub-gate print; RSI ~30.6, 1m rel −7.03%. 09-04 score the laggard **once**. 08-27 — 1w/1m laggard **forbids up**. 08-21 reversal **partial** (NQ +0.41% ≥ +0.3%, ES +0.20% not). 08-18 — cap S1 at 0/+1; GEV/BE/ETN are **not** a cushion. 08-11/08-12 supply-shock cap **does not fire** (live oil **down**). 08-13 — Hormuz is the stale leg; session oil change is down. Fed-speaker/FOMC lesson — Warsh presser **today 14:00–14:30 ET**; do **not** encode the hike as fully paid; keep S0 directionally 0 and cut confidence. Open experiment (`sector_industrials`): keep direction, shrink confidence on modest |score| — **applied**. DO-INSTEAD “score fights tape → flat/mild”: leading factors are 0 vs a still-negative multi-horizon tape — **binding as a flatten, not as a down call**.

## XLI near-session environment (not an SPX call)

Object is the **Sep 16 cash session for XLI**, not SPX and not a stock pick. Channel 1 numbers used as given. FOMC/SEP/Warsh **prints this afternoon** — two-sided, unscored until it prints.

### 1. Shared macro as it hits Industrials — S0 = 0
This is a **mixed, pre-FOMC pause**, not 09-14/09-15’s oil-up/yields-up smash and not 09-11’s unanimous +0.5% de-risking bounce.

- **Futures are green but not independently confirming.** Channel 1 Finviz: ES **+0.20%**, NQ **+0.41%**, RTY **+0.08%**, DJIA **+0.11%**. The `ES=F +1.14% / NQ=F +1.50% vs prev close` sleeve is the same sign, larger gap — **do not re-derive**; I treat Finviz as the live session tape (same conflict-handling as 09-15) and note both are green. 08-21’s ES/NQ ≥ +0.3% gate is **partial** (NQ only). The 09-11 unanimous ≥ +0.5% *across all four* test is **off**. RTY +0.08% is not a cyclical bid.
- **Oil is DOWN, not a fresh squeeze.** Channel 1: `CL=F −2.36%`, `BZ=F −1.50%`; Finviz WTI **$104.16 (−1.59%)**, Brent **$107.67 (−1.02%)**. Absolute level is still elevated — a **cost LEVEL** for transports/manufacturers, not a same-session supply shock. News Judge: **no kinetic/oil increment**. 08-11/08-12 **does not fire**. 08-13: old Hormuz/tanker-war headline is the stale leg; live change is a pullback. Count oil **once, here**. Do **not** treat oil-down as a full cyclical tailwind (08-13), and do **not** treat $104 oil as a live squeeze.
- **FOMC/SEP/Warsh is PENDING (14:00 / 14:30 ET).** CME-implied hike odds ~90%+ for +25 bp to 3.75–4.00%; SEP/dots (first 2029 dots) and the presser are the path binary. Per the Fed-speaker lesson: **do not encode the hike as fully paid**. Per 09-11: pending binary + **non-unanimous** flow = genuine two-sided event risk, not a dampener and not a license for S0 = +2. Do **not** pre-score hawkish or dovish.
- **Rates: level still high, session change is not a backup.** DGS10 **4.97** / DGS30 **5.34** / DFII10 **2.60** (through 09-14). Live Finviz: 10Y note **−0.03%**, 30Y **−0.06%** — tiny price dip, not 09-15’s long-end washout. 5-day 10Y–SPX corr **−0.155** (weak). Real-yield *level* is a condition; the *1d change* is not a second S0 shock.
- **Globals constructive, USD flat.** Asia composite **+0.65%**, Europe **+0.45%**. DXY **−0.02% / +0.02% 1d**. Gold **+0.90%**, silver **+1.96%**, copper **+0.66%** — this morning’s metals tape does **not** confirm yesterday’s hawkish gold smash (News Judge #2 is prior-session). VIX **16.98** (1d −0.22), VIX/VIX3M **0.877** (contango). HY OAS **2.71** (tight). Not a credit-stress crash.
- **News Judge #1–3 (indices lower / Warsh / IWM)** describe **09-15’s close**, already in yesterday’s tape. Do not restack as today’s S0.

**S0 = 0, regime mixed.** Not −1: oil is confirmed down, futures are green, no live yield spike, no kinetic increment, hike not to be pre-scored. Not +1: FOMC is unresolved, four-index confirmation fails, 08-13 blocks treating oil-down as a cyclical green light, 1w/1m XLI is a laggard. Oil counted **once here**.

### 2. Spine + secondary — S1 = 0 (capped)
**No fresh same-morning industrials hard print.** August ISM manufacturing already printed **09-01**: PMI **54.6** vs July 55.6; new orders **53.7** (−3.0 pts). Still **expansion** (8th month) — **not** an ISM-contraction HIT, but **slowing**. In the tape. 08-18/08-27: **cap S1 at 0/+1**; +2 forbidden without same-morning confirmation.

- **Durable goods / CapEx — stale.** July durables **+1.1%** / core ~+0.2% (08-26). August durables ~Sep 25. Not a HIT.
- **Grid / electrical equipment (AI power) — HIT, carried, and SPLIT live.** GEV ~$176B RPO / 116 GW gas book remains structural. Finviz: **BE** Mizuho PT to **$351** (fresh analyst, not a print). MAP HEAT **SPLIT Electrical Equipment dir=down** — VRT’s **~35% Q3 organic bar** is the AI-power crush; ATKR is a takeout, not electrical beta. Nested OVERRIDE beats the parent. 08-18: GEV/BE are **not** a downside cushion and **must not** cancel the VRT bar or drive the ETF. Single-ticker rule: VRT does **not** set the XLI call.
- **Aerospace & defense — MIXED / not a HIT.** SPEEA reached a **tentative** 4-year deal (09-11); ratification vote later this month; earliest strike still post-Oct 6. MAP HEAT A&D **dir=down** (GE aftermarket Hold). Do **not** cancel ISM (expansion) with one award, and do **not** treat geo as a fresh defense-order HIT.
- **Freight — PARTIAL, not a same-morning recovery HIT.** Cass August shipments **+2.1% y/y** (first gain after 42 months, released ~09-14) + TL linehaul **+11.3% y/y**; AAR rail/intermodal still strong. Oil-down is cost relief for trucking/air. Already in the prior two sessions’ information set — not a 09-16 open binary.
- **Construction slowdown — HIT, carried.** July total construction **−0.5% m/m / −3.8% y/y**; manufacturing construction **~$170B**, **−21% y/y** off the 2024 peak. AI/nonres/power is the offset, not a broad build boom. In the tape.
- **Reshoring / industrial policy — checked, nothing material** same-morning.
- **MAP HEAT (nested beats parent):** Building products **down** (medium), conglomerates **down** (HON Hold), E&C **down** (28% breadth), machinery residual **down** (CAT −4% session not offset by autonomy/DE recalls), consulting **up** (VRSK/ICFI — the XLI miss). Net captains do **not** support an S1 raise.

Net: slowing ISM expansion + carried grid + Cass inflection vs construction drag + electrical SPLIT + A&D heat down. **S1 = 0.** Not +1: no same-morning spine confirmation; 08-18 forbids using GEV/BE as the raise. Not −1: ISM is still expansion; VRT must not drive the ETF; do not invent an ISM-contraction HIT.

### 3. Breadth — S2 = 0
Channel 1 through 09-15: 1d rel **−0.18%**, 3d **−0.94%**, 1w **−2.07%**, 1m **−7.03%**. Deep multi-horizon laggard, but the **same fact** cannot sit in S2 and S4 (09-04 / “score once”). MAP HEAT is nested color (most HEATs down, consulting up, electrical SPLIT) — not an independent % names-up print, and not “ETF up / names flat” (ETF was also down). Leadership is not a healthy expansion. Score the lag **once** as a **condition** under S4’s audit, not as a second negative here. **S2 = 0.**

### 4. Flows / positioning — S3 = 0
Channel 2: ETF Channel flagged a **weekly** XLI creation ~**$259M** (SO 153.6M → 155.3M, dated 09-16); ETFdb **5-day −$121M**, **1-month −$1.13B**. Mixed — 1m bleed vs a weekly create. Premarket volume on the MW print was tiny (dozens of shares). Not a crowded long (1m rel −7%). Not a same-open volume spike. **S3 = 0.** Checked; nothing material to drive the session.

### 5. ETF tape (confirmation only) — S4 = 0
Channel 1: 1d rel **−0.18%** (sub-gate), 3d **−0.94%**, 1w **−2.07%**, 1m **−7.03%**. Persistent laggard is a **condition**, not a same-day forecast without a fresh catalyst. 09-10: with RSI ~30.6 and 1m ≤ −5%, the prior 1d rel is a **decaying** signal — do not read −0.18% as acceleration. 09-14’s “S4 = −1 for the 1m lag” required a **live** oil/duration down-stack and a PM gap *worse* than ES; today oil is down, the long end is quiet, and Channel 2 XLI PM **~$169.85 (+0.59%)** is *better* than Finviz ES +0.20% (and *worse* than ES=F +1.14% — conflict, so I do not convert PM into sign). 09-15: do **not** treat a better-than-index gap as a cushion **and** do not invert it into a fade when the oil/duration shock is absent. S4 is confirmation only and **must not become the thesis**. **S4 = 0.**

### 6. Catalysts / calendar
- **FOMC + SEP/dots + Warsh presser today 14:00/14:30 ET** — dominant variance source. 09-03: magnitude **≥ mild**. Do not pre-score the print.
- Adobe / ASML are **non-holdings** XLK/software beta (08-27: forbid mapping into XLI up).
- No same-morning ISM / durables / factory orders.

### Self-audit
- Lens: cyclical; rates/oil only in S0.
- Band: **mild**, not flat (pending FOMC), not notable (futures not ≤ −0.5% and not unanimous ≥ +0.5%).
- Skew: VRT / BE / GEV / CAT / HON do not drive the ETF call.
- Same-shock: oil counted once in S0; 1m lag not stacked in S2+S4.
- 08-27: 1w/1m laggard → **forbid up**.
- 08-21: only a partial bounce; not a down-forbid by itself, but combined with green oil and no fresh negative, 08-18 says direction should be **non-negative**.
- Divergence: leading S0+S1+S2 = 0 vs still-negative 1w/1m tape. **Flagged.** Trust **factors over tape** → do not emit down from leftover RS. 09-09’s emit-down rule needs a negative leading score **and** confirming 1d tape; 1d rel −0.18% is not that confirmation.

**Divergence flagged: True.** Leading factor sum does not fight a large tape score (S4 = 0); the leftover 1m lag is the residual. Per DO-INSTEAD and trust-factors: **flat direction**, **mild** band for the 14:00 binary. Open experiment: shrink confidence on modest |score|.

**Path qualifier:** if the overnight ES=F gap is the live one, this is a **gap-and-hold / event-wait** morning, not an industrials trend day. Do not read PM:XLI +0.59% as S0 or S4 sign.

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 0
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.9
CONFIDENCE: 0.46
REGIME: mixed
HORIZON_3D: mixed
HORIZON_1W: mixed
HORIZON_2W: mixed
HORIZON_1M: mixed
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.45|2026-09-16|https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html
Risk-off tape / flight to safety|MISS|0.70|2026-09-16|https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html
Real yields rising|PARTIAL|0.55|2026-09-16|https://fred.stlouisfed.org/series/DFII10
Real yields falling|MISS|0.60|2026-09-16|https://fred.stlouisfed.org/series/DFII10
USD strengthening|MISS|0.65|2026-09-16|Channel 1 DXY 1d +0.02%
USD weakening|MISS|0.65|2026-09-16|Channel 1 USD −0.02%
Sector breadth expansion (% names up)|MISS|0.60|2026-09-16|MAP HEAT nested (most HEATs dir=down)
Sector breadth failure (ETF up, names flat)|MISS|0.55|2026-09-16|Channel 1 XLI 1d −0.64% with names also soft
Large-cap leadership inside sector|PARTIAL|0.50|2026-09-16|MAP HEAT CAT/GE/HON not expanding
Small/mid leadership inside sector|MISS|0.50|2026-09-16|MAP HEAT RUT captains no tape-changing news
High-beta leadership inside sector|MISS|0.55|2026-09-16|MAP HEAT VRT electrical SPLIT down
Low-beta leadership inside sector|MISS|0.45|2026-09-16|checked, nothing material
Sector ETF inflow / relative volume spike|PARTIAL|0.40|2026-09-16|https://www.etfchannel.com/article/202509/notable-etf-inflow-detected-xli-ge-uber-cat-xli-ge-uber-cat-XLI09162025.htm
Sector ETF outflow / volume dry-up|PARTIAL|0.45|2026-09-16|https://etfdb.com/etf/XLI/
Crowded long (extreme relative performance + valuation)|MISS|0.75|2026-09-16|Channel 1 1m rel −7.03%
Index rebalance / inclusion tailwind|MISS|0.40|2026-09-16|checked, nothing material
Index exclusion / forced selling|MISS|0.40|2026-09-16|checked, nothing material
ISM manufacturing / new orders expansion|HIT|0.80|2026-09-01|https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html
Durable goods / CapEx upside|PARTIAL|0.50|2026-08-26|https://www.census.gov/manufacturing/m3/adv/pdf/durgd.pdf
Grid / electrical equipment backlog (AI power)|HIT|0.70|2026-09-16|https://www.turbomachinerymag.com/view/ge-vernova-gas-turbine-backlog-hits-116-gw-as-power-orders-more-than-double
Aerospace & defense order / budget upside|MISS|0.55|2026-09-16|https://www.reuters.com/business/world-at-work/boeing-engineers-union-reach-tentative-contract-agreement-2026-09-11/
Freight / trucking / rail volume recovery|PARTIAL|0.60|2026-09-14|https://www.freightwaves.com/news/cass-tl-rates-jump-11-in-august-freight-shipments-turn-positive
Reshoring / industrial policy funding|MISS|0.40|2026-09-16|checked, nothing material
ISM contraction|MISS|0.85|2026-09-01|https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html
CapEx cuts / order cancellation|MISS|0.45|2026-09-16|checked, nothing material
Freight recession|MISS|0.55|2026-09-14|https://www.cassinfo.com/freight-audit-payment/cass-transportation-indexes/august-2026
Construction slowdown|HIT|0.70|2026-09-01|https://www.census.gov/construction/c30/pdf/totsa.pdf
Sector rotation into industrials|MISS|0.60|2026-09-16|Channel 1 1m rel −7.03%
Sector rotation out of industrials|PARTIAL|0.55|2026-09-16|https://etfdb.com/etf/XLI/
HIT_GRID_END

## RESEARCH APPENDIX

**Queries run**
- web_search: `FOMC meeting date September 2026 Fed decision calendar`
- web_search: `ISM manufacturing durable goods factory orders September 2026`
- web_search: `XLI ETF flows industrials sector premarket September 16 2026`
- web_search: `GE Vernova grid backlog Bloom Energy Caterpillar Boeing industrials news September 2026`
- web_search: `oil prices Hormuz Iran tanker war September 16 2026`
- web_search: `CME FedWatch September 2026 rate hike odds Warsh`
- web_search: `XLI stock premarket September 16 2026`
- web_search: `Cass Freight Index rail carloads trucking September 2026`
- web_search: `Vertiv VRT Q3 organic growth industrials electrical equipment September 2026`
- web_search: `US construction spending manufacturing construction slowdown September 2026`
- web_search: `XLI RSI oversold industrials relative performance September 16 2026`
- web_search: `Boeing SPEEA strike aerospace defense orders September 2026`
- web_search: `risk on equity market breadth industrials XLI September 16 2026`
- web_fetch: `https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html`
- web_fetch: `https://www.etfdb.com/etf/XLI/` (403)
- x_search: `XLI industrials premarket FOMC oil September 16 2026` (from 2026-09-15 to 2026-09-16)
- memory_search: Industrials XLI lessons (index disabled)

**Key sources and facts taken**
- CNBC Fed live blog (fetched 2026-09-16): FOMC decision today; hike odds >90% to 3.75–4.00%; Warsh JH + sticky CPI + oil >$100 as the repricing; SEP/dots including 2029. https://www.cnbc.com/2026/09/16/fed-meeting-today-live-updates.html
- Fed calendar aggregators: meeting Sep 15–16, announcement 14:00 ET, presser 14:30 ET. https://fedratecalc.com/fomc-meeting-schedule/september-2026/
- ISM PR (Aug 2026, released Sep 1): PMI 54.6, new orders 53.7; next mfg PMI Oct 1. https://www.prnewswire.com/news-releases/manufacturing-pmi-at-54-6-august-2026-ism-manufacturing-pmi-report-302865127.html
- Census durables (July): new orders +1.1%; August durables ~Sep 25. https://www.census.gov/manufacturing/m3/adv/pdf/durgd.pdf
- MarketWatch XLI: prior close 09-15 $168.85 (−0.64%); PM ~$169.85 (+0.59%) ~5:17 a.m. EDT, very low volume. https://www.marketwatch.com/investing/fund/xli/download-data
- ETF Channel (dated 09-16): weekly XLI inflow ~$258.9M / +1.1% SO. ETFdb: 5-day −$121M, 1m −$1.13B.
- GEV: $176B backlog, 116 GW gas book, DC electrification orders >$5B H1 — carried. https://www.turbomachinerymag.com/view/ge-vernova-gas-turbine-backlog-hits-116-gw-as-power-orders-more-than-double
- Hormuz/tanker war still structurally active; live session oil is **down** (Channel 1). https://johnmenadue.com/post/2026/09/us-says-hormuz-is-under-control-oil-markets-say-otherwise/
- Cass August (released ~Sep 14): shipments +2.1% y/y first gain in 42 months; TL linehaul +11.3% y/y. https://www.freightwaves.com/news/cass-tl-rates-jump-11-in-august-freight-shipments-turn-positive
- VRT: Q3 organic guide ~35%; MAP HEAT SPLIT down on that bar. https://www.trefis.com/stock/vrt/articles/615230/vertiv-has-guided-itself-into-a-steep-second-half/2026-09-14
- Census construction (July): total −0.5% m/m / −3.8% y/y; manufacturing construction ~$170B, −21% y/y. https://www.census.gov/construction/c30/pdf/totsa.pdf
- XLI RSI ~30.56 at 09-15 close; below 20/50/200-day SMAs. https://wallstreetnumbers.com/etfs/xli/rsi
- SPEEA: tentative deal 09-11, vote later; no active strike. https://www.reuters.com/business/world-at-work/boeing-engineers-union-reach-tentative-contract-agreement-2026-09-11/
- Channel 1 (unaltered): VIX 16.98, VIX/VIX3M 0.877, Finviz ES +0.20% / NQ +0.41% / RTY +0.08% / DJIA +0.11%, WTI −1.59% / Brent −1.02%, CL −2.36% / BZ −1.50%, copper +0.66%, gold +0.90%, DGS10 4.97 / DFII10 2.60, Asia +0.65%, Europe +0.45%, XLI vs SPY 1d/3d/1w/1m rel −0.18% / −0.94% / −2.07% / −7.03%.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 0.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': 0.0, 'divergence_flagged': False, 'total_score': 7.288, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.994, 'score': 5.964, 'legs': [{'leg': 'ES', 'pct': 1.14, 'w': 0.8}, {'leg': 'ER2', 'pct': 0.08, 'w': 0.2}, {'leg': 'HG', 'pct': 0.66, 'w': 0.1}]}, 'overlay_score': 0.0, 'overlay_raw': 0.0, 'index_carry': 1.324, 'general_total': 5.297, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 0.5, 'S4_ETF_TAPE': 0.5}, 'llm_confidence': 0.46, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -1.65, 'w1': -2.74}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
