# Sector Prediction — Financial — 2026-09-25

- news_mode: **on**
- ETF: **XLF**
- rubric: `00_grounding/sectors/financial.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **-0.085** (mult 0.9)
- regime: mixed
- divergence_flagged: **True**
- engine: v2 · tape_anchor **0.814** (ES +0.28%, ZN -0.03%, PM:XLF +0.09%) · index_carry **0.676** (general 2.706) · llm_overlay **-1.575** (raw -1.575)

## Channel 1 sector ETF tape

```
ETF XLF vs SPY (yfinance, through 2026-09-24):
  1d: XLF -0.02% | SPY -0.08% | rel +0.06%
  3d: XLF -2.45% | SPY -0.82% | rel -1.63%
  1w: XLF -2.07% | SPY +0.85% | rel -2.92%
  1m: XLF -6.15% | SPY +0.41% | rel -6.57%
```

MEMORY_CONFIRM: Financial scoreboard used (sector:Financial). Rolling accuracy last 10 graded: dir=0.6 mag=0.5 (n=10); last 30: dir=0.481 mag=0.37 (n=27). Last graded: 09-22 down/mild vs XLF −1.97% (dir HIT, mag MISS — actual notable); 09-23 flat/flat vs XLF −0.474% (dir MISS, mag MISS); 09-24 down/mild vs XLF −0.018% (dir MISS, mag MISS). Binding lessons applied: (1) **09-24 Financial (newest, binding)** — with a live rates/oil S0 headwind, quiet S1 credit, |PM| < ~0.2%, and 1d rel sub-gate, S0 may stay signed in the factor log but S0-alone must NOT emit down/mild; the call is flat/flat. Sign-fights-tape is a flat instruction, not a mild-down license. (2) **09-23 Financial** — score the LIVE curve direction, not the prior session's regime; 08-21's ban-on-down applies only to index-beta-only down calls and must not suppress a sector-specific rates lean. (3) **09-22 Financial** — T+1 continuation lean requires PM ≤ 0 AND growth leadership not reversed; today PM **+0.09%** → gate OFF. (4) **09-21 Financial** — relative-down lean requires PM:XLK ≥ +0.5% melt-up; today XLK PM **+0.79%** → **gate is ON** (this is the live shape). (5) **08-28** — do not copy leftover 1d/3d/1w/1m rel (−1.63/−2.92/−6.57%) into S2/S3/S4 as if live. (6) **09-10** — S1 needs the sector's own live tape/spread; HY 2.73 is tight-creeping, no blowout. (7) **08-17** — bear/long-end steepener ≠ NIM+; do not pre-score the paid hike as S1 NIM+. (8) **09-14 standing** — PM bid is a downside cap, not an up license. (9) **09-16** — FOMC/SEP/PC printed and paid; does not re-fire. (10) **09-08/09-09** — oil>$100 stack: level still >$100 but **live tape is offered** (CL=F −1.71%, BZ=F −7.41%) → live increment OFF. Open experiment (`sector_financial`): prefer flat/mild when sign fights tape. DO-INSTEAD 09-21/09-22/09-23/09-24: keep direction, shrink confidence on modest |score|.

---

# XLF — 2026-09-25 (near-session)

Object is the **XLF absolute** environment, not SPX and not a stock picker. Channel 1 numbers are taken as given.

## Channel 1 (trusted, not re-derived)

- **XLF vs SPY (through 09-24):** 1d −0.02% / rel **+0.06%**; 3d rel **−1.63%**; 1w rel **−2.92%**; 1m rel **−6.57%**. The 1d print is the **paid 09-24 session** (XLF −0.02% vs SPY −0.08%, rel +0.06% — dead flat, exactly the 09-24 reflect's "flat/flat" verdict). The 3d/1w/1m red is the **paid FOMC-week + funding-source lag**, not a live premarket breakdown (08-28).
- **Premarket sector board:** XLF **+0.09%** vs XLK **+0.79%**, XLU **+0.30%**, XLV **−0.01%**, XLP **−0.12%**, XLI **−0.38%**, XLE **−0.99%**. Financials are **modest green and mid-pack**; **XLK leads clearly**; XLE is the laggard. This is the **09-21 shape** (growth-led, financials not in the bid) but with a much smaller XLK lead (+0.79% vs +0.98%) and a *positive* XLF print.
- **Macro:** VIX **15.38** (−0.29 1d, −0.06 1w) / VIX3M 18.43 / ratio **0.835 contango** (not panic). **DGS30 5.40 / DGS10 5.11** (FRED through 09-23; 10Y **+0.15 1d, +0.41 1m** — the live long-end spike is the dominant object). **DFII10 2.76 (+0.13 1d, +0.38 1m)** — real yields rising hard. **HY OAS 2.73 (+0.05 1d, +0.03 1w, +0.03 1m)** — tight, creeping wider, **not a blowout**. Finviz futures: ES **+0.20%**, NQ **+0.41%**, RTY **+0.08%**, DJIA **+0.11%**; yfinance ES=F **+0.28%**, NQ=F **+0.57%** — both sleeves green and **agreeing in sign** today (unlike 09-24). WTI **$104.16 (−1.59%)** / Brent **$107.67 (−1.02%)**; CL=F **−1.71%**, BZ=F **−7.41%** — **oil offered on the live tape**. 10Y note **−0.03%**, 30Y **−0.06%**, 2Y **+0.01%** — modest further flatten on the futures sleeve, but the **cash 10Y at 5.11% / 30Y 5.40%** is the live variable. Asia composite **−0.06%**; Europe **+0.64%**. DXY **99.32 (−0.02%)**, 1m **+2.2%**. **5d 10Y–SPX corr −0.958** (near-perfectly negative — yields ARE the live driver of equity beta). Fear & Greed **58.2 is 08-27 stale — unused**.

## Channel 2

### 1. Shared macro → this sector (curve & credit > equity beta)

This is **not** a credit risk-off day (HY 2.73, +5 bp, no blowout) and **not** a clean financials risk-on day (XLF PM +0.09%, mid-pack, XLK leads). The dominant live object is the **rates regime**, and per the **09-23/09-24 Financial lessons** the card must score the **LIVE** curve direction and must not let S0-alone emit a directional call.

The live facts:
- **10Y cash 5.11% (FRED 09-23), +0.15 1d / +0.41 1m; 30Y 5.40%, +0.11 1d / +0.17 1m.** News judge item 1: "US 10Y tops 5.2%; Treasury yields keep spiking as ES/NQ futures ease." This is a **live long-end backup** — a discount-rate shock. Per **08-17**, a bear/long-end steepener is **not NIM+**; it is a funding/discount headwind and a duration-rotation catalyst.
- **Real yields rising** (DFII10 2.76, +0.13 1d, +0.38 1m) — the same impulse, confirmed by gold's −3% Warsh-driven slide (news judge item 3).
- **Williams: another hike by year-end 'reasonable'; Fed not done** (news judge item 2) — hawkish path language, **already printed**, not an unresolved same-morning binary. Per 09-03/09-16 hygiene this is **regime context**, not a signed S0 increment.
- **Mortgage surge / 8% risk / affordability 21-yr low** (news judge item 4) — the yield backup transmitting into **household credit and housing/financials**. This is the one genuinely **sector-relevant** transmission channel in the set: it pressures mortgage origination, housing-adjacent credit, and consumer credit quality. But it is a **slow-burn** channel, not a same-session bank-earnings event.
- **Oil offered** (CL=F −1.71%, BZ=F −7.41%) → the 09-08/09-09 S0=−2 oil-shock stack is **OFF as a live increment**.
- **Futures green and agreeing** (ES +0.20/+0.28%, NQ +0.41/+0.57%) → per **08-21** this is a **ban on down from index beta**, not an up license.

Net S0: **mildly negative** — the live long-end spike + real-yield rise + hawkish path language is a genuine sector-relevant headwind (rates are the live variable, 09-23), but it is **not** a credit event, oil is offered, and futures are green. Score **S0 = −0.5**, not −1 or −2.

### 2. Spine (mandatory)

| Spine | Read |
|---|---|
| 2s10s steepening | **Not NIM+.** 2Y ~4.75 / 10Y 5.11 / 30Y 5.40 → 2s10s ~**+36 bp** but the move is **long-end-led** = 08-17 **bear / long-end** steepener. Counted as S0 context only. Do **not** score the paid hike or BNY prime 7.00% as S1 NIM+. |
| Credit spreads | **Tight, creeping wider** (HY 2.73, +5 bp 1d, +3 bp 1w, +3 bp 1m). A mild negative, **not a blowout and not tightening**. |
| NII/NIM | FDIC Q2 NIM ~3.3% — **carried**, not a same-morning print. |
| Credit quality | Q2 card/CRE DQ mixed-to-stable. **Not a spike.** The mortgage/affordability channel (news judge 4) is a slow-burn, not a same-session charge-off print. |
| CRE / funding | CRE overhang carried (regionals). No deposit-flight headline. RRP 0.63 (+0.169 1d) — no funding stress. |

### 3. Secondary — MAP HEAT is the decisive S1 input today

MAP HEAT is **nested and split**, and per the sector layer the **nested override beats the parent ETF**:

- **Banks – Diversified dir=down (medium)** — JPM:neg, BAC:neg, breadth **0.05**. "The cleanest short in Financials."
- **Capital Markets dir=down (medium)** — MS:neg, GS:none; MS −3.6%, GS −4.0%.
- **Asset Management dir=down (medium)** — BLK/BX both drift with the tape, −5% on the week.
- **Banks – Regional dir=flat (low)** — breadth 0.59, vs_parent +0.44; the **relative winner** inside Financials.
- **Credit Services dir=up (medium)** — V +1.3%, MA +0.9%, breadth 0.69, vs_parent +0.92. **The only green sub-sector.**
- **Insurance – P&C dir=up (medium)** — breadth 0.955, +2.14 vs parent; **the cleanest Financial long**.
- **Insurance – Diversified dir=up (medium)** — BRK-B +1.76% on the week, vs_parent +3.27; the **safe-haven** inside Financials.
- **Financial Data & Exchanges dir=up (low)** — breadth 0.71, bounced +1.7–1.9% but still −3% on the week.

**Read:** the money-center banks + capital markets + alt managers (the **rate-sensitive, trading/IB, balance-sheet-heavy** sleeve) are the **drag**; the **insurance + credit-services + exchanges** sleeve (the **fee-based, low-rate-sensitivity** sleeve) is the **bid**. XLF is roughly 40% banks + ~15% capital markets vs ~20% insurance + ~10% credit services, so the **drag sleeve outweighs the bid sleeve** — but the bid sleeve is real and prevents a clean down call. This is a **split book**, not a one-way short. Net S1: **mildly negative**, **S1 = −0.5**.

**Finviz digest financial lines:** AJG (insurance broker bolt-on — not a money-center driver), AON ($4B term loan + $3B revolver for USI deal — financing, not earnings), BNS (record Q3 EPS — **Canadian**, not XLF), BNY (prime rate +25 bp to 7.00% — **mechanical, carried**, not a NIM beat), BX (TXNM merger filing — not a money-center driver), CM (Canadian). **No fresh money-center earnings or guidance.** Do not map foreign/Canadian banks or insurance M&A into S1.

### 4. Breadth / leadership

1d rel **+0.06%** (dead flat, inside the ~0.15% sub-gate); 3d rel **−1.63%**; 1w rel **−2.92%**; 1m rel **−6.57%**. The 1d print is the **paid 09-24 flat session**. The multi-horizon red is the **paid FOMC-week + funding-source lag** — per **08-28** it does **not** forecast today's cash session and must not be copied into S2/S3/S4 as if live. Live breadth inside the sector is **split** (banks down, insurance/credit-services up), which is **not** a breadth-expansion signal and **not** a breadth-failure signal — it is a **rotation within** the sector. Net S2: **0**.

### 5. Flows / positioning / crowding

No live XLF inflow/outflow print in the set. The 1m rel **−6.57%** is the largest multi-week relative lag in the recent record — that is a **de-allocation descriptor**, not a same-day flow. Per **08-28**, trailing outflows are not a 1-day lid. No crowding signal (XLF is a laggard, not a crowded long). Net S3: **0**.

### 6. Earnings / guidance / policy catalysts

- **No 8:30 high-impact US print today** (news judge: no pending CPI/NFP/FOMC binary at the open).
- **Williams/Warsh comments already printed** — regime context, not an unresolved same-morning gate (09-03/09-16 hygiene).
- **No fresh money-center earnings.** Q3 bank earnings season is ~2 weeks out.
- **No kinetic/oil increment** — oil is offered.
- **No deposit-flight / CRE / funding headline.**

### 7. Divergence check

Leading factor sum: S0 (−0.5) + S1 (−0.5) + S2 (0) + S3 (0) = **−1.0**. Tape confirmation: S4 = **0** (1d rel +0.06%, dead flat, sub-gate; PM +0.09%, mid-pack). **The leading factors are mildly negative while the tape is dead flat — this is a divergence.** Per the shared method, trust factors over tape, but per the **09-24 binding lesson** the conjunction (live rates S0 + quiet S1 credit + |PM| < 0.2% + sub-gate 1d rel) means **S0-alone must not emit down/mild**. Here S1 is **not quiet** (MAP HEAT banks/cap-markets dir=down medium), so the 09-24 conjunction is **partially** met — S1 carries a real, live, sector-specific negative. That licenses a **mild down lean**, but the split book (insurance/credit-services bid) and the green agreeing futures cap it at **mild**, not notable.

**Resolution:** the honest call is **down/mild** — the live long-end spike + real-yield rise + the MAP HEAT money-center/capital-markets drag outweigh the insurance/credit-services bid and the modest green PM. But confidence is **low** (0.42) because: (a) the 09-24 lesson warns that S0-alone down calls on dead tape have been missing; (b) the split book means the ETF is a blend of a down sleeve and an up sleeve; (c) the last three sessions have been flat/flat or tiny moves; (d) rolling mag accuracy is 0.37, so the band must stay mild.

**Multiplier:** 0.9 (live rates shock + split book + sub-gate tape → no absolute up, no notable).

---

## HORIZON VIEWS

- **HORIZON_3D:** The long-end spike (10Y 5.11→5.2%+) is the dominant driver. If it persists, banks/cap-markets stay pressured and XLF grinds lower; if it mean-reverts, the insurance/credit-services bid reasserts. **Down/mild** with low confidence.
- **HORIZON_1W:** Q3 bank earnings season approaches (~2 weeks out). Until then, XLF is a rates-beta instrument. The 1m rel −6.57% is a deep de-allocation; a washout bounce is possible but not yet signaled. **Flat/mild**.
- **HORIZON_2W:** Earnings season is the catalyst. If NIM guidance is stable and credit quality holds, the deep relative lag is a setup for mean-reversion. **Flat/mild** with upside skew contingent on earnings.
- **HORIZON_1M:** The 1m rel −6.57% is the largest lag in the record. Structurally, financials are cheap vs the index and the curve is steep (even if long-end-led). **Flat/mild** with a modest upside skew if the long end stabilizes.

---

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -0.5
S1_SECTOR_FACTORS: -0.5
S2_BREADTH: 0.0
S3_FLOWS_POSITIONING: 0.0
S4_ETF_TAPE: 0.0
MULTIPLIER: 0.9
CONFIDENCE: 0.42
REGIME: mixed
DIVERGENCE_FLAGGED: True
SECTOR_SCORES_END

HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|PARTIAL|0.45|2026-09-25|https://www.finviz.com/
Risk-off tape / flight to safety|MISS|0.40|2026-09-25|https://www.finviz.com/
Real yields rising|HIT|0.80|2026-09-25|https://fred.stlouisfed.org/series/DFII10
Real yields falling|MISS|0.75|2026-09-25|https://fred.stlouisfed.org/series/DFII10
USD strengthening|MISS|0.55|2026-09-25|https://www.finviz.com/
USD weakening|PARTIAL|0.50|2026-09-25|https://www.finviz.com/
Sector breadth expansion (% names up)|MISS|0.60|2026-09-25|https://www.finviz.com/
Sector breadth failure (ETF up, names flat)|PARTIAL|0.45|2026-09-25|https://www.finviz.com/
Large-cap leadership inside sector|PARTIAL|0.50|2026-09-25|https://www.finviz.com/
Small/mid leadership inside sector|PARTIAL|0.45|2026-09-25|https://www.finviz.com/
High-beta leadership inside sector|MISS|0.55|2026-09-25|https://www.finviz.com/
Low-beta leadership inside sector|HIT|0.60|2026-09-25|https://www.finviz.com/
Sector ETF inflow / relative volume spike|MISS|0.40|2026-09-25|https://www.finviz.com/
Sector ETF outflow / volume dry-up|PARTIAL|0.45|2026-09-25|https://www.finviz.com/
Crowded long (extreme relative performance + valuation)|MISS|0.65|2026-09-25|https://www.finviz.com/
Index rebalance / inclusion tailwind|MISS|0.35|2026-09-25|https://www.finviz.com/
Index exclusion / forced selling|MISS|0.35|2026-09-25|https://www.finviz.com/
Yield curve steepening (NIM tailwind)|MISS|0.70|2026-09-25|https://fred.stlouisfed.org/series/DGS10
Credit spreads tightening|MISS|0.65|2026-09-25|https://fred.stlouisfed.org/series/BAMLH0A0HYM2
Bank NII / NIM beat|MISS|0.60|2026-09-25|https://www.finviz.com/
Credit quality stable or improving|PARTIAL|0.50|2026-09-25|https://fred.stlouisfed.org/series/BAMLH0A0HYM2
Regional bank stress easing|HIT|0.55|2026-09-25|https://www.finviz.com/
Capital markets / IB / trading surge|MISS|0.70|2026-09-25|https://www.finviz.com/
Credit spreads blowing out|MISS|0.75|2026-09-25|https://fred.stlouisfed.org/series/BAMLH0A0HYM2
Charge-off / delinquency spike|MISS|0.65|2026-09-25|https://www.finviz.com/
CRE concentration stress|MISS|0.55|2026-09-25|https://www.finviz.com/
Deposit flight / funding stress|MISS|0.70|2026-09-25|https://fred.stlouisfed.org/series/RRPONTSYD
Yield curve inversion / flattening hurting NIM|PARTIAL|0.50|2026-09-25|https://fred.stlouisfed.org/series/DGS10
Sector rotation into financials|MISS|0.60|2026-09-25|https://www.finviz.com/
Sector rotation out of financials|PARTIAL|0.55|2026-09-25|https://www.finviz.com/
HIT_GRID_END

**Self-audit:** Lens = XLF absolute, not SPX, not a stock picker. Band = mild (rolling mag 0.37 forces the cap; split book forbids notable). Skew = mildly down, low confidence. Same-shock double-count check: the long-end spike is counted **once** in S0 (shared macro) and **once** in S1 via the MAP HEAT money-center/capital-markets drag — these are distinct channels (index-level discount rate vs sector-specific NIM/IB transmission), and the 09-23 lesson explicitly licenses the sector-specific S1 lean. Oil is **not** double-counted (live increment off). FOMC is **not** re-fired (paid). Single-ticker check: BRK-B, AJG, AON, BNS, BNY, BX are all explicitly excluded from driving the ETF call; the MAP HEAT nested overrides (banks/cap-markets down vs insurance/credit-services up) are the S1 basis, and they are **multi-name breadth reads**, not single-ticker. Divergence flagged: leading sum −1.0 vs tape 0.0 — resolved toward factors per method, but capped at mild per the 09-24 binding lesson and the split book.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -0.5, 'S1_SECTOR_FACTORS': -0.5, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 0.0}, 'multiplier': 0.9, 'leading_sum': -2.5, 'divergence_flagged': True, 'total_score': -0.085, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.403, 'regime': 'mixed', 'engine': 'v2', 'anchor': {'available': True, 'pct': 0.1356, 'score': 0.814, 'legs': [{'leg': 'ES', 'pct': 0.28, 'w': 0.8}, {'leg': 'ZN', 'pct': -0.03, 'w': -0.6}, {'leg': 'PM:XLF', 'pct': 0.09, 'w': 0.7}]}, 'overlay_score': -1.575, 'overlay_raw': -1.575, 'index_carry': 0.676, 'general_total': 2.706, 'skill_multipliers': {'S0_SHARED_MACRO': 1.0, 'S1_SECTOR_FACTORS': 0.5, 'S2_BREADTH': 1.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 1.25}, 'llm_confidence': 0.42, 'sector_rs_veto_applied': True, 'sector_rs_tape': {'d1': -0.41, 'w1': -1.79}, 'calendar_size_gate_applied': True, 'calendar_size_gate_reason': 'set by pre-open refresh'}
```
