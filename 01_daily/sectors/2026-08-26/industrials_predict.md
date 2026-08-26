# Sector Prediction — Industrials — 2026-08-26

- ETF: **XLI**
- rubric: `00_grounding/sectors/industrials.md`
- predicted_direction: **flat**
- predicted_magnitude_band: **flat**
- total_score: **0.0** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLI vs SPY (yfinance, through 2026-08-25):
  1d: XLI -0.34% | SPY +0.32% | rel -0.65%
  3d: XLI -0.76% | SPY +0.43% | rel -1.20%
  1w: XLI -2.82% | SPY -0.20% | rel -2.62%
  1m: XLI -2.62% | SPY +3.63% | rel -6.25%
```

I have enough data now. Let me compile the analysis.

MEMORY_CONFIRM: Reviewed prior runs for sector Industrials — last run 2026-08-25 predicted flat/flat, actual XLI -0.335% (dir MISS). Rolling dir=0.222 / mag=0.111 (n=9). Governing sector rules today: (1) 08-11/08-12 supply-shock cap does **not** fire — oil is sharply down (CL -2.5%, BZ -3.94%) on demand/risk, not a Hormuz squeeze; (2) 08-13 — oil down on demand/risk while old supply headline remains → S0 ≤ 0, do not treat old headline as live; (3) 08-18 — on a scheduled hard-data release, do not keep S1 at +2 on soft survey evidence if hard data misses; cap S1 at 0/+1; (4) 08-21 reversal checklist — positive futures → avoid down call, but 08-25 reflect refined this: when S4=-1 (decisive negative tape across all timeframes) AND a fresh single-name negative exists, the futures bounce does NOT rescue the laggard sector; predict down:mild. Per-scope DO-INSTEAD for sector_industrials (losses 08-10, 08-12, 08-17, 08-18, 08-25): "When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild."

## Sector Environment Analysis: Industrials (XLI)

### Channel 2 Research Findings

**1. Shared Macro Regime (risk-on/off, yields, USD):**
- **The dominant scheduled catalyst today is July core PCE (due today).** This is the Fed's preferred inflation gauge and the market is positioned for a cool print. The News Judge ranks it #1. A hot surprise is the asymmetric risk given Fed's Collins hawkish comments and regional Fed directors seeking a hike. This is a **two-sided binary event** for rates and risk appetite.
- **Futures flat-to-mildly-negative.** ES -0.07%, NQ -0.15% premarket. Asia composite +0.47% (Nikkei +0.62%, Kospi +0.97%), Europe +0.18%. Mixed, no strong directional confirmation.
- **Rates elevated but easing on the day.** DGS10 4.70% (-0.04 1d), DGS30 5.23% (-0.04 1d), DFII10 (real yield) 2.38% (-0.02 1d, -0.06 1w). Long-end yields are easing slightly today — a mild relief but still elevated. For this cyclical, ISM/CapEx outrank duration.
- **Oil sharply down.** CL -2.5%, BZ -3.94% — a demand/risk-driven decline, not a supply squeeze. Lower crude is mild cost relief for transports/manufacturers, but the sign of the move is risk/demand, so it does not get scored as a cyclical tailwind (08-13 lesson).
- **USD flat.** DXY +0.1% 1d, -2.33% 1m. Neutral.
- **VIX 15.69 (low), Fear & Greed 58.6 (Greed).** Calm risk appetite, but the PCE print is the swing factor.
- **5-day corr 10Y vs SPX: -0.39** — moderate negative, rising yields pressuring equities.

**S0 = -1.** The PCE binary event risk + hawkish Fed commentary + elevated long-end yields are a mild negative overlay for a cyclical sector. Not -2: no fresh hard-data miss, oil is down (cost relief), futures are only mildly negative, and a cool PCE could relieve the rate pressure. Not 0: the hawkish Collins/regional-director cluster and the asymmetric hot-PCE risk argue for a mild negative.

**2. Sector Spine + Secondary:**
- **ISM manufacturing / new orders expansion — HIT (high confidence, but SOFT survey).** ISM New Orders at 56.7 (July, 89th percentile), released 8/3. August ISM is 9/1. This is the sector spine — positive. **BUT** per the 08-18 lesson, do not keep S1 at +2 on soft survey evidence when hard coincident data (G.17 IP) has been weak. G.17 is stale (last week's number).
- **Grid / electrical equipment backlog (AI power) — HIT (high confidence).** GE Vernova $176B backlog, gas turbine book 116 GW, data-center orders surging. Structural, semi-independent of classic ISM. **BUT** 08-18 warned not to use GEV/Eaton as a downside cushion after those names rolled.
- **Aerospace & defense order / budget upside — HIT (fresh, high confidence).** **Boeing won a $131.23B-ceiling F-15 Eagle Crest contract from the US Air Force through 2037** (Reuters, Aug 24). This is a fresh, knowable-at-open defense award on a top XLI weight. Per the AMP/DAMP note, "do not cancel ISM weakness with one award" — but ISM is NOT weak (it's expansion), so this is a genuine positive S1 driver. This is a fresh catalyst, not stale.
- **Freight / trucking / rail volume recovery — HIT (medium-high).** AAR week to 8/15: North American carloads +4.7% y/y, intermodal +5.1% y/y. Rail recovery is genuine. Cass July shipments still negative (-4.8% y/y) — rail ≠ trucking, but rail is the cleaner signal.
- **Construction slowdown — HIT (negative).** June total construction -3.2% y/y; manufacturing construction ~21% off the 2025 peak. Data-center/nonres is the offset, not a broad industrial-build boom.
- **Reshoring / industrial policy — HIT (medium).** $1.6T announced reshoring investments.

Net: one live spine HIT (ISM expansion) + one structural HIT (grid backlog) + a **fresh defense award (Boeing F-15)** against construction drag and soft trucking. The Boeing F-15 contract is a fresh positive catalyst on a top weight. **S1 = +2** (the fresh Boeing defense award + ISM expansion + grid backlog justify this; the 08-18 cap applies to hard-data misses, which are not present today — G.17 is stale).

**3. Breadth / Leadership:**
XLI is a **laggard, not a leader**. 1w rel -2.62%, 1m rel -6.25%. 1d rel -0.65% (negative). Leadership that still works is large-cap AI-power/machinery (CAT/GEV/ETN) and now defense (BA on the F-15 award), i.e. mega-name carry, not % of names expanding. **S2 = -1.**

**4. Flows / Positioning:**
BofA (Aug 19): "Tech hits record weekly inflows while **industrials exodus deepens**." This is a fresh flow signal — industrials are seeing outflows/rotation out. XLI ~+$370M over 1m but 5-day negative. Not a crowded long (the 1m relative dump argues the opposite). **S3 = -1** (the BofA "industrials exodus" is a fresh negative flow signal).

**5. ETF Tape (confirmation only):**
Channel 1: 1d XLI -0.34% vs SPY +0.32% (rel -0.65%); 3d rel -1.20%; 1w -2.62%; 1m -6.25%. The 1d relative is **negative** — the tape is NOT confirming any up move. **S4 = -1.**

**6. Earnings / Policy:**
No index-wide industrials earnings cluster at the open. The Boeing F-15 defense award is a fresh positive catalyst. Policy path is PCE (today) + Warsh Jackson Hole (Fri) — two-sided for a cyclical. Do not cancel ISM (still expansion) with construction noise, and do not let the Boeing award single-handedly flip the sector against a decisively negative tape.

### Self-audit
- **Lens:** Cyclical; rates secondary to ISM/CapEx — rates used only in S0, not re-counted in S1.
- **Band:** No severe path. Multiplier 0.9.
- **Skew:** Not treating Boeing as the ETF; the F-15 award is a real S1 positive but the tape is decisively negative.
- **Same-shock double-count:** The Boeing F-15 award is counted once in S1, not re-counted in S2/S4.
- **Single-ticker:** Boeing is a top XLI weight but the F-15 award is a genuine sector catalyst, not a single-name distortion.
- **Divergence:** Leading factors (S1 +2 on ISM/grid/defense) fight the tape (S4 -1, S2 -1, S3 -1). Per the 08-25 reflect lesson, when the sector tape is decisively negative across all timeframes AND there's a fresh negative (BofA industrials exodus), the positive structural factors should NOT override the tape. But there IS a fresh positive (Boeing F-15) offsetting the fresh negative. Net: the tape is decisively negative (1m rel -6.25%), so I should trust the tape over the structural factors and lean flat/down.

**Final scores:**
- S0 = -1 (PCE binary risk + hawkish Fed + elevated yields)
- S1 = +2 (ISM expansion + grid backlog + fresh Boeing F-15 defense award)
- S2 = -1 (laggard, mega-name carry only)
- S3 = -1 (BofA "industrials exodus" fresh outflow signal)
- S4 = -1 (decisive negative tape, 1d rel -0.65%, 1m rel -6.25%)

Total = (-1 + 2 - 1 - 1 - 1) = -2.0 × 0.9 = **-1.8** → down/flat.

**Direction:** The tape is decisively negative (S4 -1, S2 -1, S3 -1) and the macro overlay is mildly negative (S0 -1). The structural positives (S1 +2) are real but the 08-25 lesson says trust the tape when it's decisively negative. The Boeing F-15 award is a genuine positive but cannot single-handedly flip a sector that's down -6.25% relative over 1m. **Direction: down.**

**Magnitude:** flat-to-mild. The Boeing defense award and ISM expansion cushion the downside. **Band: flat** (leaning mild-negative). Given the decisively negative tape and the fresh outflow signal, I'll go **down/flat** with a mild-negative bias.

**Regime:** mixed (PCE binary event, hawkish Fed, but oil down and futures only mildly negative).

SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: -1
S1_SECTOR_FACTORS: 2
S2_BREADTH: -1
S3_FLOWS_POSITIONING: -1
S4_ETF_TAPE: -1
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: mixed
HORIZON_3D: down:mild:0.5
HORIZON_1W: down:mild:0.5
HORIZON_2W: flat:mild:0.45
HORIZON_1M: flat:mild:0.4
SECTOR_SCORES_END

HIT_GRID_BEGIN
ISM manufacturing / new orders expansion|HIT|0.8|2026-08-03|https://www.ismworld.org
Grid / electrical equipment backlog (AI power)|HIT|0.8|2026-08-05|https://news.google.com
Aerospace & defense order / budget upside|HIT|0.8|2026-08-24|https://www.reuters.com
Freight / trucking / rail volume recovery|HIT|0.6|2026-08-19|https://www.aar.org
Construction slowdown|HIT|0.6|2026-08-24|https://www.census.gov
Sector rotation out of industrials|HIT|0.7|2026-08-19|https://seekingalpha.com
Sector breadth failure (ETF up, names flat)|HIT|0.6|2026-08-25|https://finance.yahoo.com
Risk-off tape / flight to safety|HIT|0.5|2026-08-26|https://www.cnbc.com
HIT_GRID_END

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': -1.0, 'S1_SECTOR_FACTORS': 2.0, 'S2_BREADTH': -1.0, 'S3_FLOWS_POSITIONING': -1.0, 'S4_ETF_TAPE': -1.0}, 'multiplier': 0.9, 'leading_sum': 2.0, 'divergence_flagged': False, 'total_score': 0.0, 'predicted_direction': 'flat', 'predicted_magnitude_band': 'flat', 'confidence_score': 0.55, 'regime': 'mixed'}
```
