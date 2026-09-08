# Sector Outcome — Basic Materials — 2026-09-08

Actuals: {'etf': 'XLB', 'pct': -0.9534706580738517, 'spy_pct': -0.5492125910932955, 'rel': -0.4042580669805562, 'open': 52.36000061035156, 'close': 51.939998626708984, 'source': 'yf_download'}

## Sector Post-Session Review — Basic Materials (XLB) — 2026-09-08

### 0. FACTS

- XLB: **−0.95%** (open 52.36 → close 51.94)
- SPY: **−0.55%**
- Relative: **−0.40%** (XLB underperformed SPY)
- Path: Opened near flat/positive territory, faded through the session to close at session lows — a **down day** that worsened into the close.

---

### 1. What Drove the Sector Today

The session was dominated by the **escalating Hormuz/oil supply shock** — WTI +3.18% to $94.4, Brent +2.16% to $99.1 — which pushed the broad tape decisively risk-off (ES −0.44%, DJIA −0.91%, Russell −0.63%). For XLB specifically, the oil spike functioned as a **cost headwind on the chemicals-heavy book** (LIN ~13%, SHW, ECL), compressing margins on feedstock/energy inputs.

| CLAIM | URL | PUBLISHED | QUOTE/SUMMARY |
|---|---|---|---|
| Wall Street slid on inflation/Middle East worries | Reuters via Google News | 2026-09-08 20:17 GMT | "Wall Street slides, oil surges amid worries over inflation, Middle East" |
| Copper hit record high intraday | Crux Investor via Google News | 2026-09-08 16:59 GMT | "Copper Hits Record High as US Tariff Delay Divides Producers" |
| FCX closed up +5.35% | TradingKey via Google News | 2026-09-08 20:15 GMT | "Freeport-McMoRan Inc Stock (FCX) Closed Up by 5.35% on Sep 8" |

The **copper bid was real** — FCX +5.35%, copper at record highs — but it was **not sufficient to carry the whole XLB book**. The chemicals sleeve (LIN, SHW, ECL) faced the oil cost squeeze and dragged the ETF lower. The net was a sector that **underperformed SPY** despite a genuine metals surge, because the composition of XLB is heavily weighted toward chemicals/processors rather than pure copper miners.

---

### 2. Audit of Morning S0–S4 Reads

**S0 (Shared Macro) = −1 → Verdict: CORRECT DIRECTION, UNDERWEIGHTED MAGNITUDE.**
The morning correctly identified the risk-off tape as negative for XLB. However, the score of −1 (not −2) was justified by "copper green + USD weakening" as offsets. In reality, the risk-off tape **overwhelmed** the copper bid for the overall ETF. The S0 read was directionally right but the magnitude contribution was understated — the oil shock's cost-channel impact on chemicals was more damaging than the +1 offset credit implied.

**S1 (Sector Factors) = +1 → Verdict: WRONG — OVERWEIGHTED COPPER, UNDERWEIGHTED OIL COST CHANNEL.**
The morning scored copper at records as a +1 driver. Copper did hit records and FCX surged +5.35%. **But the S1 read failed to properly weight the oil-cost channel on the chemicals sleeve.** The morning acknowledged the headwind ("margin compression pressure") but scored it as a minor offset within a +1 net positive. In reality, the chemicals drag (LIN/SHW/ECL — roughly 40-50% of XLB weight) overwhelmed the copper miner gains (FCX ~10-15% weight). The composition math was knowable: copper miners are a **minority** of XLB; chemicals are the **majority**. The +1 for S1 was too generous given the book composition.

**S2 (Breadth) = 0 → Verdict: CORRECT.**
The compositional split was real — copper miners up, chemicals down — and netted to no clean breadth signal. This was the most accurate read of the morning.

**S3 (Flows/Positioning) = 0 → Verdict: CORRECT.**
No notable flow or positioning signal; this was not a flow-driven day.

**S4 (Tape) = +0.5 → Verdict: WRONG — CONFIRMATION MISREAD.**
The morning used the 1d rel +0.56% (XLB outperforming SPY) as confirmation-eligible positive. But this was **stale tape from the prior session's close** (through 09-08 pre-market), not live intraday confirmation. The actual session saw XLB **underperform** SPY by −0.40%. The S4 read was based on yesterday's relative strength, not today's tape — a classic **T-1 lag error** that the morning's own rules (09-04 T-1-lag rule) were supposed to prevent.

---

### 3. Interactions / Double-Count / Knowable-at-Open Test

**Key interaction: Copper surge vs. oil cost headwind.** The morning treated these as partially offsetting within S1 (+1 net). But the **compositional asymmetry** — copper miners are ~10-15% of XLB while chemicals/processors are ~40-50% — meant the oil cost channel should have dominated. This was **knowable at open** from XLB's holdings breakdown.

**Double-count check:** The morning double-counted the copper bid in S1 (+1) and S4 (+0.5 via relative strength). The S4 credit was derived from the same copper-driven outperformance already scored in S1. This inflated the total.

**Knowable-at-open test:** The direction (down) was knowable — the risk-off tape was decisive, oil was spiking, and XLB's chemical-heavy composition meant the cost channel would dominate. The magnitude (−0.95%, notable underperformance vs SPY) was also knowable given the composition math. **This was knowable at open.**

**8/18 rule application:** The morning correctly identified the 8/18 lesson (don't use copper as a floor on oil-shock days) but then **violated its own logic** by scoring S1 = +1 on copper strength. The rule said: on oil-shock risk-off days, metals co-move down with equities. The morning rationalized that "copper is genuinely green" — but the rule's spirit was about the **sector ETF**, not the commodity. The ETF still fell because the chemicals sleeve dominates.

---

### 4. Outliers Inside the Sector

- **FCX +5.35%** — copper at records drove a massive single-stock gain (TradingKey, 2026-09-08). This was the copper bid working exactly as the morning predicted — but for one stock, not the ETF.
- **Chemicals complex (LIN/SHW/ECL)** — the oil cost headwind hit this sleeve hard. No single-stock data available in search results, but the ETF's −0.95% close despite FCX +5.35% implies the chemicals sleeve fell roughly −1.5% to −2% to drag the book down.
- **Gold miners (NEM)** — gold was −0.84%, consistent with the morning's fade call; this sleeve added to downside.

---

### Outcome

OUTCOME_BEGIN
SECTOR: Basic Materials
ETF: XLB
ETF_PCT: -0.95
SPY_PCT: -0.55
REL_PCT: -0.40
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Oil cost headwind on chemicals-heavy XLB book overwhelmed copper miner gains on risk-off Hormuz shock day
KEY_INTERACTION: Copper surge (FCX +5.35%) vs. oil cost squeeze on LIN/SHW/ECL — composition asymmetry made chemicals drag dominate
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Wrong direction — over-weighted copper bid relative to XLB's chemical-heavy composition; S4 used stale prior-day tape
OUTCOME_END