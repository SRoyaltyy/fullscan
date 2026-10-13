# Sector Prediction — Healthcare — 2026-10-13

- news_mode: **on**
- ETF: **XLV**
- rubric: `00_grounding/sectors/healthcare.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **2.601** (mult 0.8)
- regime: mixed
- divergence_flagged: **False**
- engine: v2 · tape_anchor **1.793** (ES -0.09%, PM:XLV +0.45%) · index_carry **-1.692** (general -6.767) · llm_overlay **2.5** (raw 2.5)

## Channel 1 sector ETF tape

```
ETF XLV vs SPY (yfinance, through 2026-10-12):
  1d: XLV +0.69% | SPY -0.06% | rel +0.75%
  3d: XLV +1.88% | SPY +0.11% | rel +1.77%
  1w: XLV +2.76% | SPY +0.42% | rel +2.34%
  1m: XLV +4.41% | SPY +2.06% | rel +2.35%
```

MEMORY_CONFIRM: Healthcare/XLV 2026-10-13. Rolling HC dir=0.4 mag=0.5 (n=10); last-30 dir=0.48 mag=0.33 (n=27) — historically weak scope, so prefer mild/low-confidence. Open experiment applies: shrink confidence on modest |score|. Calendar: **Tue cash session; Fed minutes catalyst is this week's rates cluster; CPI + Powell pending — unsigned binary, size_gate=True**. Binding lessons: **08-13 leftover-RS ban** — XLV shows 3d +1.77 / 1w +2.34 / 1m +2.35 rel leadership on a defensive rotation, BUT 1d rel is **+0.75% (not flat)**, so the flat-1d reversal-warning lesson does **not** fire. **09-16-style force-flat off (FOMC paid)**. **09-11 funding-source does not fire at full weight** (ES −0.09%, no rip). **08-14 policy audit:** no same-morning mega-cap Rx smash — search returns only IRA cycle-1/2 price cap reporting (paid/ongoing, no fresh crackdown headline); CMS MA 2027 +2.48% stale. **08-21:** AMGN dazodalibep, ABBV JUVMO, ARGX–Forte, BDX beat, BSX downgrade, CI downgrade = single-name/nested — must not dominate XLV. **10-01 reflect:** don't zero S2 on nested names when the parent tape is green. Fear & Greed unavailable; FedWatch not scrapable.

# Healthcare / XLV — 2026-10-13

**Object:** near-session XLV environment.

## Channel 1 (trusted)
XLV vs SPY through 10-12: **1d +0.69%/−0.06% (rel +0.75%)**; 3d rel **+1.77%**; 1w rel **+2.34%**; 1m rel **+2.35%** — all four horizons green; healthcare is leading SPY on a defensive relative-strength rotation. Yesterday XLV was green on a flat SPY tape. Macro: VIX 15.42 (contango 0.86); futures **ES −0.09% / NQ −0.73%** — tech-led softness, not broad risk-off; Asia composite −1.37% (Kospi −5.46%); Europe mildly green. 10Y 5.22, DFII10 2.87 — real yields still elevated on 1m (+0.41), hostile to the XBI duration sleeve. Oil mixed (CL −1.25%). DXY soft 1d. **PM: XLV +0.45% — mid-pack, behind staples +0.98% / RE +0.78%, ahead of XLK −0.97%.** News Judge #1 = Hormuz/Iran escalation (risk-off skew favors defensives relatively), #2/#3 = hawkish Fed/yields (unresolved binary, S0 drag via XBI sleeve).

## Channel 2
1. **Shared macro → sector (S0):** NQ-led tech weakness + Hormuz risk-off skew is *defensive-favoring context*, and healthcare has been the defensive leader. But hawkish Fed minutes + 5.2% 10Y keep real-yield pressure on the biotech/duration sleeve, and healthcare was NOT the haven leader in PM (staples outran it). Net ≈ **0 to +0.5**; take +0.5 (relative-defensive regime intact, PM green).
2. **Spine (S1):** CMS/MA rate — stale (+). Drug pricing — IRA reporting is process, no fresh crackdown (+0 offset). Biotech — XBI hit "rate wall" per TradingView, pullback noted by BioPharma Dive; MAP HEAT biotech flat/low; AMGN positive Phase 3 is single-name (nested, not breadth). Utilization — nothing fresh. Managed-care nested HEAT up (UNH/CVS/CLOV) and facilities/devices/distribution HEAT mostly up. Net **+0.5**.
3. **Secondary:** JNJ earnings this week = watch item; FDA target dates pending (nested). No CRL cluster. Checked news: IRA coverage is retrospective; nothing material fresh.
4. **Breadth (S2):** MAP HEAT shows breadth across facilities/devices/distribution/managed care — not mega-cap-only. +0.5.
5. **Flows/positioning (S3):** no flow data in Channel 1/2; after a strong 1m run, extension/crowding risk caps this at 0 (mild mean-reversion caution on crowded-defensive extension, but no evidence of outflows).
6. **Catalysts:** JNJ print this week, CPI/Powell pending — binary risk, not signed.

**Divergence check:** factors lean mildly up; S4 tape confirms (all horizons green + PM +0.45%). No fight. Per 10-02 lesson, don't let index_carry mint a big up call — ES/NQ are red-ish and PM XLV is only mid-pack; band capped at mild, confidence low given scope's 40% hit rate and magnitude 0.33.

HIT_GRID_BEGIN
HIT_GRID_BEGIN
Risk-off tape / flight to safety|partial (defensive context, healthcare not the haven leader)|0.6|2026-10-13|channel1
Real yields rising|HIT (1m +0.41, XBI headwind)|0.7|2026-10-08|https://fred.stlouisfed.org/series/DFII10
Sector rotation into healthcare|HIT (1m rel +2.35, defensive RS)|0.65|2026-10-12|channel1
Large-cap leadership inside sector|HIT (UNH/CVS/managed-care bid)|0.6|2026-10-13|map_heat
Biotech risk-off / funding winter|PARTIAL (XBI rate-wall pullback, not winter)|0.5|2026-10-09|https://news.google.com/rss/ (BioPharma Dive)
Drug pricing crackdown / IRA expansion risk|MISS (retrospective coverage only, nothing fresh)|0.7|2026-10-13|https://www.ajmc.com/view/medicare-drug-price-negotiations-yield-uneven-out-of-pocket-savings
Sector ETF outflow / volume dry-up|MISS (no flow evidence)|0.5|2026-10-13|search
HIT_GRID_END

HORIZON_3D: mild up — defensive RS rotation carried by unresolved hawkish-rates week (CPI/Powell), JNJ print; mean-reversion risk on extended 1m run.
HORIZON_1W: mixed — binary-heavy week (CPI, Powell, bank earnings) plus Hormuz; healthcare likely holds relative leadership in mild/hawkish tape, vulnerable to risk-on reversal.
HORIZON_2W: mixed — Q3 HC mega-cap earnings (JNJ lead, UNH early) begins; utilization and 2027 MA commentary are the swing factors.
HORIZON_1M: mild up bias in relative terms; absolute capped by real-yield level on the XBI sleeve.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0.5
S1_SECTOR_FACTORS: 0.5
S2_BREADTH: 0.5
S3_FLOWS_POSITIONING: 0.0
S4_ETF_TAPE: 1.0
MULTIPLIER: 0.8
CONFIDENCE: 0.45
REGIME: mixed
DIRECTION: up
MAGNITUDE_BAND: mild
SECTOR_SCORES_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.5, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 0.5, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 1.0}, 'multiplier': 0.8, 'leading_sum': 3.5, 'divergence_flagged': False, 'total_score': 2.601, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.604, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.2988, 'score': 1.793, 'legs': [{'leg': 'ES', 'pct': -0.09, 'w': 0.6}, {'leg': 'PM:XLV', 'pct': 0.45, 'w': 0.7}]}, 'overlay_score': 2.5, 'overlay_raw': 2.5, 'index_carry': -1.692, 'general_total': -6.767, 'skill_multipliers': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.25, 'S2_BREADTH': 1.25, 'S3_FLOWS_POSITIONING': 1.25, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.45, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
