# Sector Prediction — Consumer Defensive — 2026-09-18

- ETF: **XLP**
- rubric: `00_grounding/sectors/consumer_defensive.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **-0.125** (mult 0.8)
- regime: risk_on
- divergence_flagged: **True**
- engine: v2 · tape_anchor **0.86** (ES +1.14%, ZN -0.03%, PM:XLP -0.01%) · index_carry **1.215** (general 4.861) · llm_overlay **-2.2** (raw -2.2)

## Channel 1 sector ETF tape

```
ETF XLP vs SPY (yfinance, through 2026-09-17):
  1d: XLP +0.19% | SPY +1.13% | rel -0.94%
  3d: XLP -1.10% | SPY +0.23% | rel -1.33%
  1w: XLP +0.48% | SPY +0.63% | rel -0.15%
  1m: XLP -2.44% | SPY -0.63% | rel -1.81%
```

MEMORY_CONFIRM: Consumer Defensive / XLP only — memory index paused (embedding metadata mismatch); using injected sector scoreboard + last-10 logs, not MEMORY.md. Rolling dir=0.5 / mag=0.2 (n=10); last-30 dir=0.455 / mag=0.318 (n=22). Last graded 2026-09-17 predicted up/mild vs XLP +0.192% / SPY +1.134% / rel −0.942% (dir HIT on 19 bp, mag MISS): net-negative S0–S4 + non-haven PM in the flat band, v2 tape_anchor/index_carry still printed up/mild. 09-16/09-15 flat/flat were leftover-RS veto misses. 09-14 up/mild dir HIT / mag MISS. No open experiment tagged to this sector (utilities/news only). Today I do **not** re-litigate stale WMT (08-20), do **not** copy leftover 3d/1w/1m lag into S2+S4 or let Finviz RS veto (09-15/09-16; Channel 1 1d rel already −0.94%), do **not** apply food-crash dominance at full weight (CPB cut 09-03, T+15, no fresh print — 09-10 one-session cap), do **not** fire 08-27 down/notable (NQ leads ES by ~36 bp, not ≥50 bp), do **not** convert a theoretical FTS bid into absolute up (08-18; PM is not a haven), do **not** restack yesterday’s paid anti-FTS smash (rel −0.94%) into S1+S2+S4 (08-28), do **not** accept ES tape_anchor as an XLP-up certificate when PM is flat-band non-haven (09-17), and I **do** treat the live post-FOMC risk-on continuation (ES/NQ both ≥ +1%, XLK lead, oil offered, VIX contango) as **one** anti-FTS / rotation-out object counted once in S0, with oil’s input-cost leg capped in S1 — not a second S0 haven bid.

# Consumer Defensive (XLP) — 2026-09-18

Object is the **near-session XLP environment**, not SPX and not a stock picker. Channel 1 numbers are used as given.

## Channel 1 tape (confirmation only)

```
ETF XLP vs SPY (yfinance, through 2026-09-17):
  1d: XLP +0.19% | SPY +1.13% | rel -0.94%
  3d: XLP -1.10% | SPY +0.23% | rel -1.33%
  1w: XLP +0.48% | SPY +0.63% | rel -0.15%
  1m: XLP -2.44% | SPY -0.63% | rel -1.81%
```

The 1d print is **yesterday’s already-paid anti-FTS day** on the post-FOMC rebound: +19 bp absolute vs SPY +1.13% (rel **−0.94%**). That is the 09-17 lesson realized — beta, not a staples-up day. S4 may describe it; it does **not** forecast a second down day (08-28). 3d rel **−1.33%** and 1m rel **−1.81%** are the multi-horizon lag; 1w rel **−0.15%** is a wash. Leftover 09-14 FTS RS is **gone** — the 09-15/09-16 veto setup (green leftover 1d/1w RS) does **not** fire. This is a **caught-up-then-funding-source** bond-proxy, not an under-owned washout (09-14 magnitude-widen does **not** fire: PM is not best-of-eleven).

Live board that *is* knowable at the open: **ES=F +1.14% / NQ=F +1.50%** (both ≥ +0.5%; NQ leads by ~36 bp — **not** the 08-27 ≥50 bp notable gate). Finviz cash futures SPX **+0.20% / NDX +0.41%** are the same sign, smaller print — trust Channel 1, do not average. **Sector PM: XLP −0.01%** vs **XLK +0.60%**, **XLB +0.51%**, XLI +0.27%, XLY +0.14%, XLF +0.15%, XLRE +0.12%, XLC −0.01%, XLV −0.01%, XLU −0.07%, XLE −0.56%. That is **not a haven print**. It is mid/bottom of a green book — the 09-15 gate (“if PM is not a haven, zero FTS credit”) is on. Absolute flat is **non-participation**, not outperformance.

Macro panel as it maps here: **VIX 15.22 (−0.22 1d) / VIX3M 18.55 / ratio 0.82 CONTANGO** — no vol-FTS. **WTI Finviz $104.16 −1.59% / Brent $107.67 −1.02% / CL=F −6.31% / BZ=F −6.17%** — oil still war-premium *level*, live *sign* is a hard offer (08-11 spike rule **off**). Gold **+0.90%**, silver **+1.96%**, copper **+0.66%** — metals bid, **not** the 08-18 co-move crash, and **not** a staples floor (XLP is not participating). DXY **+0.12% 1d** / Finviz USD **−0.02%** (flat). **DGS10 5.01 / DGS30 5.35 / DFII10 2.68** (real yield **+6 bp 1d, +22 bp 1w**) — duration still in the stress zone; **not** a fresh 10Y>5% break (09-15 smash is paid) but **not** duration relief either. 10Y note **−0.03% / 30Y −0.06%** (bond prices marginally down). HY OAS **2.70** (tight). 5-day 10Y–SPX corr **−0.437** (not the −0.9 regime). Asia **+1.11%** (Kospi +2.66%), Europe **−0.46%** (in progress). Fear & Greed **UNAVAILABLE**. EPU **106.54 (−150 1d)** — uncertainty spike collapsed. `size_gate=True`.

## Channel 2 — required categories

**1. Shared macro → this sector.** Live tape is **session-2 of the post-FOMC risk-on rebound**: ES/NQ both ≥ +1% with NQ leading, Asia green, Europe soft, VIX contango, oil offering hard, XLK lead. News Judge #1/#2 (Warsh JH / hike odds / BNY prime) are **paid as of 09-16**. News Judge #3 (oil drop / crude build / DVN) is the live *sign* on crude — inflation-optics for SPX, XLE hit, **input-cost relief** for staples, **not** a Hormuz FTS bid. News Judge #4–#7 (ASML, BAC, copper tariffs, AI-infra split) are XLK/XLF/XLB objects — not XLP. No pending CPI/NFP/FOMC binary (News Judge RULES_APPLIED: none). X search: XLP tagged lagging/damaged on the 09-17 rebound; **no fresh same-morning staples flow print**.

For staples the map is **one object**:
- Risk-on / equity-beta expansion is **[−] defensives** (amp/damp). 09-11: “no FTS bid” is a **relative negative**, not S0=0. Named-headwind rule: this rotation **must** be scored, not merely narrated.
- PM XLP **−0.01%** vs XLK/XLY leaders → **zero FTS credit** (09-15). 08-18 relative-outperformance is **off**.
- Oil offering is **input-cost relief** (S1, capped) and **removes** the Hormuz FTS trigger. Do not score oil as a defensive bid in S0.
- Real yields still backing up on the 1w (**DFII10 +22 bp**) is a **duration overlay** for a bond-proxy, not a second independent shock. Count it inside the same S0 risk-on/rates object, not as a stacked S1.
- Europe red is a dampener, not a haven regime. Asia + US futures dominate the US cash open.
- Residual-is-mild-up rule does **not** fire: yields are not falling, PM is not green, S0–S3 is not net zero.
- 09-17 engine path: do **not** treat ES +1.14% + tiny/flat PM as an XLP-up day. Absolute object is **flat-band**; relative object is **funding source**.

S0 carries the **risk-on rotation overlay only** → **−1**. Not −2 (NQ lead 36 bp, XLP only −1 bp PM, no fresh duration break, Europe the only red book). Not 0 (naming a relative headwind without scoring it is banned).

**Relative lean (explicit):** negative vs SPY / XLY / XLK. Absolute lean: flat-band (PM −0.01%), not a notable down day.

**2. Spine (mandatory).**
- **Flight-to-safety RS vs cyclicals (primary):** **MISS live.** XLP PM lags XLY and XLK; 1d rel **−0.94%** already paid. Primary regime signal is **not** FTS.
- **Risk-on rotation away from defensives:** **HIT.** Counted in S0 as the regime object. Do **not** restack a second full HIT in S1 (same-shock audit). Residual sector-factor lean only.
- **Pricing power held without volume collapse:** **MISS / structural.** Private-label (Kirkland / store brands) is still taking unit share from national brands; PG FY27 organic 1–3% with a ~$1B cost headwind; KR cut identical-sales guide to 0.2–0.8% (09-11, now T+7 — carried, not a fresh morning print). Half weight, not 09-08 dominance.
- **Volume decline accelerating / elasticity break:** **PARTIAL structural, not a same-morning print.** National-brand units still soft; no new dividend cut/guidance slash this morning. 09-10 cap applies.

**3. Secondary.**
- **Input cost relief (ag, packaging, freight):** **PARTIAL.** Crude offering hard (CL −6.31%) is real packaging/freight relief, but Channel 1 grains are **firm this morning** (corn +0.56%, soy +0.70%, wheat +0.86%, meal +0.75%) while coffee/sugar/cocoa offer. 09-11: cap this channel at ~**+0.2** for a single-session *relative* outcome — gross-margin relief does not outrun a same-day rotation impulse. Score the positive; do not let it cancel S0.
- **Volume stabilization / sequential improvement:** **MISS.**
- **Staples earnings beat stable margins:** **MISS as a same-session catalyst.** WMT FY27 raise is 08-20 (stale). KR beat EPS but **cut sales guide** (09-11). PG/KO/COST: no fresh September print. Nested HEAT: do not let WMT/COST bid become the ETF thesis.
- **Input cost spike without pricing power:** **MISS** (oil is down).
- **Private-label share gain against brands:** **HIT structural** (09-16 MarketWatch: store brands squeezing national brands). Carried weight, not a new shock.
- **Sector rotation into defensives:** **MISS.**
- **Sector rotation out of defensives:** **HIT** — same object as S0; grid coverage only, not a second S1 full weight.

S1 net → **−0.5** (capped oil relief vs carried private-label/pricing/volume drag). Rotation is **not** re-added here.

**4. Breadth / leadership.** MAP HEAT (nested, **do not average**): Discount Stores **up** (WMT/COST, breadth 0.889) but the note is “defensive rotation, not a discount-store story”; Grocery **up** (KR mixed / WMK); Non-Alc **up** on **KO only** (PEP flat, FIZZ neg); Brewers **down** (TAP Jefferies trim); Confectioners **flat**; Household **flat** (PG/CL newsless); Farm Products week **spent** (d1 red); Food Distribution **flat**. That is **large-cap / mega-name carry** (WMT, COST, KO), not sector-wide expansion. Single-ticker must not drive the ETF call. Live PM is flat, so this is not “ETF up / names flat” either. Do **not** copy 3d/1m lag into S2 (08-28). S2 → **0**.

**5. Flows / positioning.** InvestingLive (17 Sep, through 16 Sep close): XLP weekly flows **+$160M → −$134M**, ~$126M latest-day redemptions; staples classification **Early Accumulation → Cooling Off**; BofA FMS **largest staples underweight since Jan 2004**. That is **not** a crowded long. Under-owned without a live haven bid is **not** a 09-14 bounce setup. Flows are 1 session lagged — not a same-morning forced-selling event. S3 engine weight is already ×0.5 and a poor sign-hit. Do not let S3 create the sign. S3 → **0**.

**6. Earnings / policy.** No same-session staples print. FOMC/SEP **printed 09-16**. IORB/primary-credit plumbing is not a 14:00 binary. KR sales-cut is carried. TAP trim is a nested brewer, not XLP. Policy/EPU collapse is risk-on confirmation (already in S0).

## Self-audit
- **Lens:** XLP near-session, not SPX, not WMT/PG stock pick.
- **Band:** `size_gate=True`; mag hit-rate 0.2 — keep multiplier **0.8**, confidence **0.48**. No notable.
- **Skew:** absolute flat-band / **relative down**. Emit the relative lean; do not call this an up day because ES is green (09-17).
- **Same-shock:** risk-on + rotation-away + rotation-out = **one S0 −1**. Oil = S1 relief only. Real-yield 1w grind folded into S0, not restacked. Yesterday’s −0.94% rel not copied into S4.
- **Single-ticker:** WMT/COST/KO HEAT bid is nested carry, not the ETF thesis. KR/TAP not the call.
- **Divergence:** leading S0+S1 = **−1.5** vs green ES/NQ tape_anchor and a flat S4. **Flag it. Trust factors over tape.** 09-17 DO-INSTEAD: keep the factor-card direction, shrink confidence on modest |score|. Residual-up rule does not fire.

Open experiment for this scope: **none**. Missing factor vs recent losses: leftover-RS veto is **off** (1d rel already ≤0; 1w rel ~0). Overweight risk: restacking paid 09-17 lag — avoided.

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: -0.5
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 0
MULTIPLIER: 0.8
CONFIDENCE: 0.48
REGIME: risk_on
HORIZON_3D: -1
HORIZON_1W: 0
HORIZON_2W: 0
HORIZON_1M: -1
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|HIT|0.85|2026-09-18|channel1 ES=F +1.14% NQ=F +1.50%
Risk-off tape / flight to safety|MISS|0.80|2026-09-18|channel1 VIX/VIX3M 0.82 contango; XLP PM -0.01%
Real yields rising|HIT|0.80|2026-09-16|channel1 DFII10 2.68 +0.06 1d +0.22 1w
Real yields falling|MISS|0.75|2026-09-18|channel1 DFII10 not down
USD strengthening|MISS|0.55|2026-09-18|channel1 DXY +0.12% 1d / Finviz USD -0.02% (flat)
USD weakening|MISS|0.55|2026-09-18|channel1 USD mixed-flat
Sector breadth expansion (% names up)|MISS|0.70|2026-09-18|MAP HEAT mixed; KO-only beverage bid
Sector breadth failure (ETF up, names flat)|MISS|0.60|2026-09-18|XLP PM -0.01% (ETF not up)
Large-cap leadership inside sector|HIT|0.70|2026-09-18|MAP HEAT WMT/COST/KO carry
Small/mid leadership inside sector|MISS|0.65|2026-09-18|MAP HEAT brewers/confectioners/household not leading
High-beta leadership inside sector|MISS|0.60|2026-09-18|inside-staples leadership is low-beta mega retail, not high-beta
Low-beta leadership inside sector|MISS|0.70|2026-09-18|XLP not leading the book vs XLK/XLY
Sector ETF inflow / relative volume spike|MISS|0.70|2026-09-16|https://investinglive.com/stocks/stock-sector-rotation-with-the-fed-decision-healthcare-attracts-fresh-interest-as-consumer-staples-lose-support/
Sector ETF outflow / volume dry-up|HIT|0.70|2026-09-16|https://investinglive.com/stocks/stock-sector-rotation-with-the-fed-decision-healthcare-attracts-fresh-interest-as-consumer-staples-lose-support/
Crowded long (extreme relative performance + valuation)|MISS|0.75|2026-09-16|BofA FMS largest staples underweight since Jan 2004 (same URL)
Index rebalance / inclusion tailwind|MISS|0.50|2026-09-18|checked, nothing material
Index exclusion / forced selling|MISS|0.50|2026-09-18|checked, nothing material
Flight-to-safety relative strength vs cyclicals|MISS|0.85|2026-09-18|channel1 1d rel -0.94%; PM XLP -0.01% vs XLY +0.14% XLK +0.60%
Input cost relief (ag, packaging, freight)|PARTIAL|0.65|2026-09-18|channel1 CL=F -6.31% / grains corn+0.56% wheat+0.86%
Pricing power held without volume collapse|MISS|0.70|2026-09-16|https://www.morningstar.com/news/marketwatch/20260916116/store-brands-like-kirkland-are-winning-the-war-for-consumer-wallets-squeezing-out-national-brands
Volume stabilization / sequential improvement|MISS|0.60|2026-09-18|structural brand-unit softness; no sequential confirmation
Staples earnings beat stable margins|MISS|0.65|2026-09-17|https://www.trefis.com/articles/615689/can-kroger-keep-its-earnings-promise-with-sales-this-weak/2026-09-17
Volume decline accelerating|PARTIAL|0.50|2026-09-18|carried structural; no fresh morning print
Elasticity break (price up, volume down hard)|MISS|0.50|2026-09-18|checked, nothing material
Input cost spike without pricing power|MISS|0.80|2026-09-18|channel1 oil offering, not spiking
Risk-on rotation away from defensives|HIT|0.85|2026-09-18|channel1 + PM board; same object as S0
Private-label share gain against brands|HIT|0.70|2026-09-16|https://www.marketwatch.com/story/store-brands-like-kirkland-are-winning-the-war-for-consumer-wallets-squeezing-out-national-brands-a43ad2d9
Sector rotation into defensives|MISS|0.80|2026-09-18|PM XLP mid/bottom of green book
Sector rotation out of defensives|HIT|0.80|2026-09-18|https://x.com/CiovaccoCapital/status/2100647430865686942
HIT_GRID_END

---

## RESEARCH APPENDIX

**Queries run**
- memory_search: “Consumer Defensive XLP lessons predictions preferences” → index paused / unavailable
- web_search: “XLP consumer staples ETF premarket September 18 2026 flows rotation”
- web_search: “consumer staples XLP WMT PG COST volume pricing power private label 2026”
- web_search: “CME FedWatch September 2026 rate hike odds Warsh”
- web_search: “risk on rotation defensives staples vs cyclicals XLY XLP September 18 2026”
- web_search: “oil prices drop consumer staples input costs wheat corn packaging freight September 2026”
- web_search: “XLP ETF flows outflows September 2026 Bank of America underweight staples”
- web_search: “consumer staples earnings guidance WMT PG KO COST KR September 2026”
- web_search: “XLP vs XLY relative performance September 17 2026 sector leaders laggards”
- web_search: “US stock futures September 18 2026 Nasdaq S&P risk on oil drop”
- x_search: “XLP consumer staples ETF flows rotation defensive lagging SPY September 18 2026” (from 2026-09-16 to 2026-09-18)
- web_fetch: investinglive sector-rotation article (XLP flows / BofA underweight)

**Key sources (title + URL + timestamp / as-of)**
1. Channel 1 pre-fetched panel — 2026-09-18 premarket (ES +1.14%, NQ +1.50%, XLP PM −0.01%, CL=F −6.31%, VIX 15.22 / ratio 0.82, DFII10 2.68, XLP vs SPY tape through 2026-09-17). Facts used: all tape/macro prints above; not re-derived.
2. InvestingLive — “Stock sector rotation with the Fed decision…” — https://investinglive.com/stocks/stock-sector-rotation-with-the-fed-decision-healthcare-attracts-fresh-interest-as-consumer-staples-lose-support/ — dated 2026-09-17, through 2026-09-16 close. Facts: XLP weekly flows +$160M → −$134M, ~$126M latest-day outflow; staples Cooling Off; BofA FMS largest staples underweight since Jan 2004; 1m XLP ~−3.2% vs SPY ~−1.8%.
3. MarketWatch / Morningstar — store brands / Kirkland squeezing national brands — https://www.morningstar.com/news/marketwatch/20260916116/store-brands-like-kirkland-are-winning-the-war-for-consumer-wallets-squeezing-out-national-brands — 2026-09-16. Facts: private-label share/unit lead vs national brands; PG/branded pressure vs WMT/COST as retailers.
4. Benzinga — leading/lagging sectors 2026-09-17 — https://www.benzinga.com/etfs/sector-etfs/26/09/61841529/leading-and-lagging-sectors-september-17-2026. Facts: XLK/XLI/XLY led; XLP small green lag; energy the decliner.
5. Trefis / Zacks — Kroger Q2/guide — https://www.trefis.com/articles/615689/can-kroger-keep-its-earnings-promise-with-sales-this-weak/2026-09-17 — 2026-09-17. Facts: KR identical-sales guide cut to 0.2–0.8%, EPS/profit guide held.
6. Fool — WMT Q2 FY27 transcript — https://www.fool.com/earnings/call-transcripts/2026/08/27/walmart-wmt-q2-2027-earnings-call-transcript/ — 2026-08-27 (stale). Facts: FY27 sales/OI/EPS raised 08-20; not a 09-18 catalyst.
7. Fool — PG Q4 FY26 transcript — https://www.fool.com/earnings/call-transcripts/2026/08/07/procter-gamble-pg-q4-2026-earnings-call-transcript/ — early Aug 2026. Facts: FY27 organic 1–3%, ~$1B cost headwind; no September update.
8. NYT / Axios — Warsh FOMC hike — https://www.nytimes.com/2026/09/17/business/economy/fed-interest-rates-warsh.html — 2026-09-16/17. Facts: 25 bp hike to 3.75–4.00% already printed; not a live binary today.
9. X — Ciovacco 2026-09-17 — https://x.com/CiovaccoCapital/status/2100647430865686942. Fact: staples the only red sector intraday while SPY/XLK advanced.
10. X — TashfeenEkram 2026-09-16 — https://x.com/TashfeenEkram/status/2100214369317539940. Fact: XLP RRG leading → weakening.
11. X — oobiewpb 2026-09-17 — https://x.com/oobiewpb/status/2100690430702592137. Fact: XLP tagged lagging/damaged vs XLV.
12. MAP HEAT research block (injected, 2026-09-18 pre-open). Facts: discount-store/grocery/KO-narrow bids; brewers down; household flat; nested not averaged.
13. News Judge 2026-09-18 (injected). Facts: no pending CPI/NFP/FOMC; oil down; Warsh/hike path paid; ASML/BAC/copper not XLP objects.
14. ETF Action / flow roundup (search) — https://www.etfaction.com/small-cap-blend-rotation-overshadows-large-cap-outflows/. Fact: ~$192M XLP redemption cited around 09-15; used only as corroboration of outflow tape, not a second independent shock.
15. Premarket XLP quotes (search) — https://stockmarketwatch.com/stock/XLP/premarket — 2026-09-18. Fact: premarket ~flat $83.48–$83.57; consistent with Channel 1 PM −0.01%.

**Not used as live Channel 1 substitutes:** Finviz digest gold/Warsh headlines (paid 09-16); search blurbs that claimed XLP still outperforming XLY on YTD (conflicts with live 1d/3d/1m Channel 1 rel and 09-17 session leadership — Channel 1 wins).

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': -0.5, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.8, 'leading_sum': -3.5, 'divergence_flagged': True, 'total_score': -0.125, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.405, 'regime': 'risk_on', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.1433, 'score': 0.86, 'legs': [{'leg': 'ES', 'pct': 1.14, 'w': 0.45}, {'leg': 'ZN', 'pct': -0.03, 'w': 0.4}, {'leg': 'PM:XLP', 'pct': -0.01, 'w': 0.7}]}, 'overlay_score': -2.2, 'overlay_raw': -2.2, 'index_carry': 1.215, 'general_total': 4.861, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.25, 'S3_FLOWS_POSITIONING': 1.0, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.48, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
