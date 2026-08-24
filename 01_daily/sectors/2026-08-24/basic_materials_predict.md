# Sector Prediction — Basic Materials — 2026-08-24

- ETF: **XLB**
- rubric: `00_grounding/sectors/basic_materials.md`
- predicted_direction: **up**
- predicted_magnitude_band: **mild**
- total_score: **3.15** (mult 0.9)
- regime: mixed
- divergence_flagged: **False**

## Channel 1 sector ETF tape

```
ETF XLB vs SPY (yfinance, through 2026-08-21):
  1d: XLB +2.14% | SPY +0.41% | rel +1.73%
  3d: XLB +3.40% | SPY -0.23% | rel +3.62%
  1w: XLB +1.90% | SPY -1.37% | rel +3.27%
  1m: XLB +6.46% | SPY +3.73% | rel +2.73%
```

MEMORY_CONFIRM: Sector Basic Materials / XLB only. Scoreboard n=8, dir=0.625, mag=0.375. Last graded 2026-08-21 was up/severe and HIT (+2.14% / rel +1.73%). Active XLB rules checked: temper-severe does **not** fire (1d rel +1.73% is not <0.5%); 8/17 China-miss + flat-futures severe ban does **not** fully fire (no same-morning China print; Friday SPY was up, not an oil/stagflation down day); 8/18 metals-co-move floor ban does **not** fire (CL −1.75%, BZ −1.31% — oil is falling, not a Hormuz/$90 squeeze); 8/14 gold-offset **does** apply (score the monetary-metals bid; do not ignore it); 8/21 “keep pipeline severe if no temper” does **not** apply because this is a post-+1.7% rel follow-through with non-confirming futures, not a fresh severe setup. No open experiment for `sector_basic_materials`. Memory index search was unavailable; used the injected sector scoreboard/lessons only.

## Analysis — XLB, session of 2026-08-24

This is a **Monday follow-through** after Friday’s materials blowout, not a new squeeze day. Channel 1 tape is unambiguously strong (1d rel **+1.73%**, 3d **+3.62%**, 1w **+3.27%**, 1m **+2.73%**). That sets **direction bias up**. It does **not** set magnitude to severe: after a >+1% relative day the standing follow-through rule defaults the next session to **mild** unless a new comparable catalyst is knowable at the open. There isn’t one.

### 1. Shared macro as it hits materials
Premarket is **not** risk-on for cyclicals: ES **−0.18%**, NQ **−0.58%**, Asia composite **−1.17%** (Kospi −3.12%, Hang Seng −1.89%, Shanghai −0.59%), Europe ~flat. VIX 15.91 (**+0.78**) and VIX/VIX3M just into backwardation. That is a mild equity-beta headwind for XLB’s industrial/chemical sleeve.

Offsets that **do** map to this sector: oil is **down** (CL −1.75%, BZ −1.31%) so the 8/18 “oil-shock liquidates the whole commodity complex” pattern is off; News Judge is explicit that Iran sanctions are risk-off **without** a Hormuz supply squeeze. Real yields are flat on 1d and slightly lower on 1w/1m (DFII10 2.35). DXY is only **+0.13%** today against a still-weak **−2.5%** 1m — not a USD spike vs the complex. Gold is the live cross-asset tell (GC=F **+2.14%**, spot ~$4,640–$4,660, 3-month high) into Warsh’s first Jackson Hole (Fri 8/28) and PCE (Wed 8/26). Today’s US calendar is light (Chicago Fed NAEI); do not pretend an 8:30 CPI/PPI is printing.

**S0 = 0 (mixed).** Futures/Asia keep this from being a risk-on cyclical tape; falling oil + gold/policy-uncertainty keep it from being a hard risk-off overlay for XLB.

### 2. Spine + secondary (S1)
**Industrial metals:** Copper is still **firm, not collapsing** — LME 3m ~**$14,252/t** (+0.5% early Europe), above $14,200, near a record settlement (Dow Jones/Morningstar, 24 Aug). That is **not** a fresh 8/17-style squeeze. LME stocks have **built** to ~**238.6kt** from mid-August ~205kt; ANZ: deliveries “eased the market’s most acute tightness.” Aluminum ~$3,238–3,242/t, iron ore still soft ~$95/t on China property. Spine “surge” is **partial**; spine “inventory draw” is **off**.

**China demand:** Still the industrial offset. July NBS mfg PMI **49.2**, construction **47.0**, property/new orders weak. No August PMI until ~31 Aug. Asia China/HK red this morning is consistent with that drag. **Do not let gold cancel this.**

**Supply disruption:** DRC concentrate ban still on the books (order 29 Jun, reported 6 Aug) but BMI/Fitch: concentrates are a small share vs DRC cathode exports — **stale/narrow**, not a same-day shock.

**Monetary metals (different driver):** **HIT.** Gold 3-month high, silver ~$69, AU/Barrick already ripped Friday. News Judge: materials/miners bid is a **substitution/hedge**, not an SPX risk-on green light — and **do not emit XLB severe-up from gold alone**.

**Policy/tariffs:** Copper still supported by the US-tariff pull-into-COMEX story (MS: tariff decision is the H2 catalyst). Carried, not fresh today.

**S1 = +1.** Gold + still-elevated copper minus China/property minus the inventory rebuild. Capped well below +2/+3 so gold cannot wash out the industrial breakdown.

### 3. Breadth
Friday was **high-beta / large-cap miner leadership** (FCX, NEM, gold names), not a chemicals/forest-products advance. That is already in the 1d tape. Scoring it again as today’s breadth expansion would triple-count the same reversal (S1/S2/S4) — the exact failure the follow-through lesson forbids. No live Monday A/D for the XLB book.

**S2 = 0.**

### 4. Flows / positioning
XLB ~$8.7B. Recent 1m net flows about **−$8M** (slight outflow) despite the price run; one +$91.5M print on 17 Aug is not a persistent bid. 1w/1m relative is extended enough for **crowding risk**, not enough for a forced-selling signal.

**S3 = 0.**

### 5. Tape (confirmation only)
Channel 1 1d rel **+1.73%** is a real **+1** confirmation of **direction**. It is Friday’s close, already traded. Use it as confirmation, not as a second magnitude engine.

**S4 = +1.** Leading sum (0+1+0+0 = +1) and tape (+1) **agree** — no leading-vs-tape fight. Trust the modest factor stack; do not let S4 upgrade the band.

### Self-audit
- **Lens:** XLB environment, not SPX, not FCX/NEM single-name.
- **Band:** After +2.14% Friday, non-confirming ES/NQ, copper tightness easing, China still contracting → **mild**, not notable/severe.
- **Skew:** Gold ≠ industrial metals. Stated separately; gold does not cancel China.
- **Same-shock:** Iran sanctions counted once in S0 as non-squeeze geopolitics; not re-used as an S1 metals floor.
- **Single-ticker:** APD Q3 (adj $3.47 beat, $2.9B exit charge) is 30 Jul — stale. Not driving the ETF call.

**Multiplier 0.9** (follow-through + flat/red futures + historically weak mag accuracy 0.375). **Confidence 0.55.** Regime **mixed**.

```
SECTOR_SCORES_BEGIN
S0_SHARED_MACRO: 0
S1_SECTOR_FACTORS: 1
S2_BREADTH: 0
S3_FLOWS_POSITIONING: 0
S4_ETF_TAPE: 1
MULTIPLIER: 0.9
CONFIDENCE: 0.55
REGIME: mixed
HORIZON_3D: flat:mild:0.50
HORIZON_1W: flat:mild:0.48
HORIZON_2W: up:mild:0.52
HORIZON_1M: up:mild:0.55
SECTOR_SCORES_END
```

```
HIT_GRID_BEGIN
Risk-on tape / equity beta expansion|MISS|0.70|2026-08-24|channel1 ES -0.18% NQ -0.58%
Risk-off tape / flight to safety|PARTIAL|0.55|2026-08-24|https://www.reuters.com/world/india/gold-hits-over-3-month-high-ahead-us-inflation-data-fed-chair-speech-2026-08-24/
Real yields rising|MISS|0.65|2026-08-24|channel1 DFII10 1d 0.0 / 1w -0.04
Real yields falling|PARTIAL|0.55|2026-08-24|channel1 DFII10 1w/1m -0.04
USD strengthening|PARTIAL|0.50|2026-08-24|channel1 DXY 1d +0.13%
USD weakening|HIT|0.70|2026-08-24|channel1 DXY 1m -2.5%
Sector breadth expansion (% names up)|MISS|0.45|2026-08-24|checked, nothing material for Monday A/D
Sector breadth failure (ETF up, names flat)|MISS|0.40|2026-08-24|checked, nothing material
Large-cap leadership inside sector|HIT|0.70|2026-08-21|Friday NEM/FCX/LIN-heavy tape (already traded)
Small/mid leadership inside sector|MISS|0.40|2026-08-24|checked, nothing material
High-beta leadership inside sector|HIT|0.70|2026-08-21|Friday miner/high-beta lead; not a fresh Monday print
Low-beta leadership inside sector|MISS|0.45|2026-08-24|checked, nothing material
Sector ETF inflow / relative volume spike|MISS|0.55|2026-08-24|https://etfdb.com/etf/XLB
Sector ETF outflow / volume dry-up|PARTIAL|0.50|2026-08-24|https://etfdb.com/etf/XLB
Crowded long (extreme relative performance + valuation)|PARTIAL|0.55|2026-08-24|channel1 1w rel +3.27% / 1m rel +2.73%
Index rebalance / inclusion tailwind|MISS|0.30|2026-08-24|checked, nothing material
Index exclusion / forced selling|MISS|0.30|2026-08-24|checked, nothing material
Industrial metal price surge (copper/aluminum/iron ore)|PARTIAL|0.70|2026-08-24|https://www.morningstar.com/news/dow-jones/202608241870/copper-above-14200-a-ton-with-global-inventory-shifts-in-focus
Gold/silver price surge (monetary metals)|HIT|0.85|2026-08-24|https://www.reuters.com/world/india/gold-hits-over-3-month-high-ahead-us-inflation-data-fed-chair-speech-2026-08-24/
China PMI / property demand rebound|MISS|0.80|2026-08-24|https://www.stats.gov.cn/english/PressRelease/202608/t20260803_1964272.html
Inventory draw (LME/exchange stocks down)|MISS|0.75|2026-08-24|https://www.morningstar.com/news/dow-jones/202608241870/copper-above-14200-a-ton-with-global-inventory-shifts-in-focus
Supply disruption (mine/export ban)|PARTIAL|0.55|2026-08-06|https://www.reuters.com/world/africa/congo-bans-exports-copper-cobalt-concentrates-official-order-says-2026-08-06/
Critical-minerals policy / domestic tariff support|HIT|0.60|2026-08-24|https://www.morningstar.com/news/dow-jones/202608241870/copper-above-14200-a-ton-with-global-inventory-shifts-in-focus
Industrial metal price collapse|MISS|0.75|2026-08-24|https://www.morningstar.com/news/dow-jones/202608241870/copper-above-14200-a-ton-with-global-inventory-shifts-in-focus
China demand shock / property stress|HIT|0.75|2026-08-24|https://www.stats.gov.cn/english/PressRelease/202608/t20260803_1964272.html
USD spike vs commodity complex|MISS|0.70|2026-08-24|channel1 DXY 1d +0.13% / 1m -2.5%
Supply glut / new capacity online|PARTIAL|0.50|2026-08-24|LME copper stock rebuild, not a new-mine glut
Margin compression / cost inflation without pricing power|MISS|0.40|2026-08-24|checked, nothing material (APD charge is stale 7/30)
Sector rotation into materials|PARTIAL|0.65|2026-08-21|channel1 Friday rel +1.73%; Monday futures do not confirm
Sector rotation out of materials|MISS|0.60|2026-08-24|channel1 1d/3d/1w/1m all positive rel
HIT_GRID_END
```

## RESEARCH APPENDIX

**Queries run**
- `copper price LME inventory August 24 2026`
- `gold silver price today August 24 2026`
- `China PMI property copper demand August 2026`
- `XLB materials stocks breadth gold miners Air Products August 24 2026`
- `XLB ETF inflows outflows August 2026 materials sector rotation`
- `Jackson Hole Warsh Fed Chair materials gold copper August 24 2026`
- `DRC Congo copper cobalt export ban August 2026`
- `aluminum iron ore price LME August 2026`
- `US Iran sanctions oil falling gold 3 month high August 24 2026`
- `Air Products APD Q3 2026 earnings XLB materials`
- `economic calendar August 24 2026 CPI PPI Jackson Hole inflation data`
- X search: `XLB materials gold copper miners August 24 2026 tape` (timed out)
- Fetches: Reuters gold (401/JS block); Morningstar/Dow Jones copper (ok)
- `memory_search` on sector lessons (index unavailable)

**Key sources and facts used**
- Channel 1 panel (injected, unaltered): VIX 15.91; DFII10 2.35 flat 1d; CL −1.75% / BZ −1.31%; GC +2.14%; DXY +0.13% 1d / −2.5% 1m; ES −0.18% / NQ −0.58%; Asia −1.17%; Europe −0.13%; XLB vs SPY 1d/3d/1w/1m rel +1.73% / +3.62% / +3.27% / +2.73%.
- Dow Jones via Morningstar, 24 Aug 2026 06:12 ET — https://www.morningstar.com/news/dow-jones/202608241870/copper-above-14200-a-ton-with-global-inventory-shifts-in-focus — LME 3m copper +0.5% to $14,252/t; LME deliveries eased acute tightness; tariff pull still a H2 catalyst.
- Reuters gold wire (search extract; page fetch blocked) — https://www.reuters.com/world/india/gold-hits-over-3-month-high-ahead-us-inflation-data-fed-chair-speech-2026-08-24/ — gold >3-month high into inflation data + Warsh; oil lower on Iran-sanctions headlines.
- NBS China, 3 Aug 2026 — https://www.stats.gov.cn/english/PressRelease/202608/t20260803_1964272.html — July mfg PMI 49.2; construction 47.0.
- Reuters, 6 Aug 2026 — https://www.reuters.com/world/africa/congo-bans-exports-copper-cobalt-concentrates-official-order-says-2026-08-06/ — DRC concentrate export ban; mostly cathode already.
- ETFDB / ETF Action (search) — https://etfdb.com/etf/XLB — ~−$8M 1m XLB flows; +$91.5M on 17 Aug.
- Reuters, 24 Aug 2026 — https://www.reuters.com/business/bond-market-anxiety-raises-stakes-warshs-debut-jackson-hole-speech-2026-08-24/ — Warsh JH keynote Friday 28 Aug; two-sided rates event.
- Scotiabank / BLS calendars — no CPI/PPI on 24 Aug; PCE 26 Aug; JH 27–29 Aug.
- Air Products, 30 Jul 2026 — adj EPS $3.47 beat, FY guide up, $2.9B exit charge — **stale**, not a Monday catalyst.
- News Judge / Finviz digest (injected): gold 3-month high + miner rip as substitution; Iran sanctions with oil down; do not emit XLB severe from gold alone.

**Not used as live positives:** Friday FCX/NEM pop (already in S4 tape); APD July print; DRC ban as a fresh squeeze.

---
## Pipeline-computed decision (deterministic)

```json
{'components': {'S0_SHARED_MACRO': 0.0, 'S1_SECTOR_FACTORS': 1.0, 'S2_BREADTH': 0.0, 'S3_FLOWS_POSITIONING': 0.0, 'S4_ETF_TAPE': 1.0}, 'multiplier': 0.9, 'leading_sum': 3.0, 'divergence_flagged': False, 'total_score': 3.15, 'predicted_direction': 'up', 'predicted_magnitude_band': 'mild', 'confidence_score': 0.55, 'regime': 'mixed'}
```
