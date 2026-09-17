# Sector Outcome — Utilities — 2026-09-17

Actuals: {'etf': 'XLU', 'pct': 0.8954475668379924, 'spy_pct': 1.1338754633487547, 'rel': -0.23842789651076224, 'open': 41.5, 'close': 41.689998626708984, 'source': 'yf_download'}

Memory index is paused this run (`openclaw memory status --index` / `openclaw memory index --force`); review uses injected morning logs, Channel 1 actuals, and live sources only.

## 0. Facts

Trusted Channel 1 close: **XLU +0.90%**, **SPY +1.13%**, **rel −0.24%**. Open **41.50** → close **41.69**. Implied prior close ~**41.32**, so the session was a **~+0.44% gap** (matches AM PM +0.41%) then a **grind higher**, not a reversal smash.

**ACTUAL_DIRECTION: up. ACTUAL_MAGNITUDE: mild.** Predicted **flat / flat**. Absolute direction miss; relative was a small lag, not a defensive beat.

Path: gap with the risk-on open, then duration helped the rest of the day. No late collapse.

---

## 1. What drove the sector

Taxonomy, in order:

**Primary — rates falling (bond-proxy bid), live after 8:30.** Morning 10Y was **~4.99%** with ZN **−0.03%** (not an impulse). Close: 10Y **4.94% (−6 bp)** vs 09-16 **5.00%**; 30Y ~**5.29–5.32%** vs ~**5.35–5.36%**. That is a same-session duration bid the AM tape did not have. XLU is a bond proxy first; **+0.90% absolute** is the yield-easing print, not FTS (VIX ~**15.4–15.5**, equities green).

**Secondary — risk-on rotation away (relative).** XLK ~**+2.2%**, XLY ~**+1.2%**, SPY **+1.13%**, XLU **+0.90%** / rel **−0.24%**. NQ-led beta expansion continued. Defensives participated but **lagged growth**. XLP even softer (~+0.13% intraday). Rotation-away **HIT on relative**, **MISS as an absolute smash**.

**Claims 8:30 — strong labor, two-sided mapping broke on the yield reaction.**

- CLAIM: Initial claims 196k, −10k vs 206k prior, vs ~208k consensus  
  URL: https://www.reuters.com/business/us-weekly-jobless-claims-unexpectedly-fall-2026-09-17/  
  PUBLISHED: 2026-09-17  
  QUOTE: “fell by 10,000 to a seasonally adjusted 196,000… below economists’ expectations (Reuters poll: 208,000)”  
  SUMMARY: Tight labor / holiday-distorted; morning’s **in-line/strong → risk-on / rotation-away** branch, **not** the weak-labor duration-bid branch.

Housing was the offset that helps explain why yields **fell** on strong claims:

- CLAIM: August housing starts 1.275M SAAR, −2.6% vs revised 1.309M; permits 1.394M (−2.7%)  
  URL: https://www.census.gov/construction/nrc/current/index.html  
  PUBLISHED: 2026-09-17  
  QUOTE: (via contemporaneous wrap) 1.275 million, down 2.6%; single-family 918k (+7.6%); multi-family 344k (−22.5%)  
  SUMMARY: Soft housing / permits vs strong claims = mixed 8:30, duration-friendly even with a tight labor print.

Philly Fed stayed expansionary (not a growth scare):

- CLAIM: September current activity 37.8 vs 47.4 August, still well above 0 and above ~30–34 cons.  
  URL: https://www.philadelphiafed.org/surveys-and-data/regional-economic-analysis/mbos-2026-09  
  PUBLISHED: 2026-09-17 08:30 ET  
  QUOTE: “The diffusion index for current general activity fell from 47.4 in August to 37.8 in September.”  
  SUMMARY: Cool-off from a hot August, not contraction; prices paid 48.6 / received 31.3 higher. Supports risk-on, not FTS.

**Not drivers (stale / nested / already paid):** FOMC +25 bp / Warsh (09-16, XLU already 0% that day). AI-power / nuclear / grid CapEx (structural). CEG ~**+1.2%** is IPP nested; NEE ~**+1.1%**, DUK ~**+0.66%**, SO ~**+0.2–0.6%** — regulated tape was a **mild up**, not a CEG ETF hijack. Oil still offered (WTI ~$101–102). VIX contango / sub-20: **no FTS**.

---

## 2. Audit morning S0–S4 vs reality

Use **morning numbers**, not a rewrite.

| Bucket | AM | Reality | Verdict |
|---|---|---|---|
| **S0 = 0** | Sticky real yields **carried**; FOMC **paid**; claims **unscored**; oil offering forbids a rates smash; no live duration impulse | 10Y **−6 bp** after 8:30; claims strong; housing soft | **Process right, impulse miss.** Not scoring carried FOMC/5% was correct (09-16 lesson). S0 should have stayed 0 **at the open**. The miss is failing to treat **unprinted 8:30 as able to mint a rates-falling HIT**, which it did. Strong claims were mapped as rotation-away, not as “yields must backup.” Yields eased anyway. |
| **S1 = −1** | Rotation-away **HIT** (NQ lead, XLK PM +1.28% vs XLU +0.41%); rates-rising **not** re-HIT; AI-power **dampener only** | Rel **−0.24%**, XLK >> XLU; absolute **+0.90%** on duration | **Relative HIT, absolute overstated.** Rotation was real. Full −1 assumed no same-session yield impulse. There was one. Extra-confirm experiment (Europe, VIX, oil) correctly confirmed **risk-on**, not a utilities smash. |
| **S2 = 0** | 1d rel +0.44% reserved for S4; MAP HEAT none; CEG size-gated | NEE/DUK/SO/CEG all green, no smash, no ETF-up/names-flat | **Hold.** Mild breadth with the ETF, not an independent expansion. |
| **S3 = 0** | 5d +$17M / 1m +$65M, not crowded | Volume ~18–20M, no flow spike in same-day reports | **Hold.** |
| **S4 = 0** | Mixed: 1d rel **+0.44%** vs 3d/1w/1m lag; 09-14 −1 floor needs all horizons red and \|1d rel\|≥~1% — **off** | Fresh 1d rel **−0.24%**; still mixed/lag, not a ≥1% beat or smash | **Hold.** Yesterday’s relative cushion did **not** pay a second beat (09-13/09-09). Divergence flag was honest. |

**Pipeline:** `predicted flat/flat`, `divergence_flagged True`, `sector_rs_veto` + `calendar_size_gate` kept it from a **down** call. That restraint vs 09-16’s **down/mild miss** (XLU 0% / SPY −0.44%) was the right lesson application. The new error is the other side: **flat** into a **duration-bid up/mild**.

---

## 3. Interactions / double-count / knowable-at-open

**Same-shock discipline held.** FOMC/carried 5% lived in S0 as **0**, rotation only in S1. Oil-offering not stacked as FTS. 1d rel not paid in both S2 and S4. CEG not allowed to set the ETF. No double-count of “sector rotation out” with “rotation away.”

**The interaction that actually printed:** strong claims (risk-on / rotation-away) **plus** 10Y −6 bp (bond-proxy bid) **plus** housing miss. Those are **not** the same object. Morning treated claims as two-sided and **pre-scored neither branch** — correct. It did **not** leave a live slot for “mixed 8:30 → yields off the 5% overshoot.” That combo is what lifted XLU absolutely while leaving it **lagging SPY/XLK**.

**Knowable at open (06:30 ET snapshot):**
- Risk-on, NQ lead, XLK > XLU, VIX 16 contango, FOMC paid, 10Y ~4.99 not ripping: **yes**
- Claims/housing/Philly prints and the **−6 bp** 10Y rally: **no**
- That strong labor would **fail** to backup the long end: **no**

**KNOWABLE_AT_OPEN: partially.**

---

## 4. Outliers inside the sector

- **NEE ~+1.1%** — large-cap regulated leadership, in line with duration + residual AI-power beta; not a one-name event.
- **CEG ~+1.2%** — IPP/nuclear, **nested**, as AM size-gated. Did not set XLU.
- **SO ~+0.2–0.6%** — laggard vs NEE/DUK; no adverse rate-case headline found for the session.
- **DUK ~+0.66%** — mid-pack regulated.
- No MAP HEAT captain, no rate-case smash, no index rebalance. Breadth = **mild up tape**, not an internal war.

Structural 09-17 color (Meta nuclear PPAs, PPL capex, House Ratepayer Protection Act 09-16) is **multi-year / regulatory**, not the 1d tape. Morning was right not to let AI-power override rotation **and** right that it can still **dampen** a down close — here it dampened the lag while duration did the lifting.

---

## Lesson (do-instead)

1. **Unprinted claims on a bond-proxy is not “S0=0 forever.”** Keep S0=0 at the open, but the **weak-labor duration branch is not the only XLU-up path**. Mixed 8:30 (strong claims + soft housing) can ease the long end off a 5% overshoot **without** FTS.
2. **S1 rotation-away at −1 does not entitle a flat-to-down absolute** if a live yield impulse can still print. Extra-confirm risk-on is a **relative** headwind, not an absolute ceiling, when ZN can rally 6 bp.
3. Keep **09-16**: do not restack FOMC. That part worked. The 09-17 miss is **underweighting the duration bid that was not on the AM tape**, not re-litigating Warsh.

---

OUTCOME_BEGIN
SECTOR: Utilities
ETF: XLU
ETF_PCT: 0.895
SPY_PCT: 1.134
REL_PCT: -0.238
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: 10Y −6 bp to 4.94% bid the bond-proxy; risk-on rotation still left XLU lagging SPY/XLK
KEY_INTERACTION: Strong claims (196k) mapped to rotation-away, but mixed 8:30 (housing miss) eased yields — duration vs rotation pulled opposite ways
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Flat/flat missed absolute up/mild; relative rotation HIT; S0=0 correctly refused carried FOMC but had no slot for the live post-8:30 duration impulse
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- memory_search: Utilities XLU sector prediction lessons 2026-09-17 rates FOMC rotation *(index unavailable)*
- web_search: XLU utilities ETF September 17 2026 close performance
- web_search: initial jobless claims September 17 2026 actual housing starts Philly Fed
- web_search: US 10 year treasury yield September 17 2026
- web_search: NEE SO DUK CEG stock September 17 2026 utilities
- x_search: XLU utilities sector September 17 2026 performance yields jobless claims (2026-09-17 to 2026-09-18)
- web_search: SPY close September 17 2026 percentage change
- web_search: XLK XLU September 17 2026 sector performance rotation
- web_search: utilities sector news September 17 2026 rate case nuclear data center
- web_search: VIX oil WTI September 17 2026 close
- web_search: site:dol.gov unemployment insurance weekly claims September 17 2026
- web_search: Philadelphia Fed manufacturing index September 2026 37.8
- web_search: XLU NEE DUK AEP SRE XEL September 17 2026 close
- web_search: US 30 year treasury yield September 17 2026
- web_search: housing starts August 2026 1.275 million September 17
- web_search: NEE NextEra Energy September 17 2026 percent change close
- web_search: leading lagging sectors September 17 2026 XLK XLY XLU XLP
- web_search: Duke Energy DUK percent change September 17 2026
- web_search: Southern Company SO percent change September 17 2026
- web_search: Reuters US weekly jobless claims unexpectedly fall 196000 September 17 2026
- web_search: XLU volume September 17 2026 ETF flows
- web_fetch: https://countryeconomy.com/bonds/usa
- web_fetch: https://www.philadelphiafed.org/surveys-and-data/regional-economic-analysis/mbos-2026-09
- web_fetch failed: Reuters claims (401/JS), DOL (403), Census (403), Benzinga (403), 247wallst (403), TradingEconomics (403)

**Key sources and facts taken**
- Channel 1 actuals (injected): XLU +0.895%, SPY +1.134%, rel −0.238%; open 41.50 / close 41.69
- CountryEconomy (fetched 2026-09-17): 10Y **4.94% on 09/17 (−0.06)** vs **5.00% on 09/16**. https://countryeconomy.com/bonds/usa
- Philadelphia Fed MBOS (2026-09-17 08:30 ET): current activity **37.8** (from 47.4); new orders 29.2; employment 11.8; prices paid 48.6; prices received 31.3. https://www.philadelphiafed.org/surveys-and-data/regional-economic-analysis/mbos-2026-09
- Reuters (2026-09-17): claims **196,000** vs 208k poll / 206k prior; 4-wma 203,250; continuing 1.730M. https://www.reuters.com/business/us-weekly-jobless-claims-unexpectedly-fall-2026-09-17/
- Census/HUD via contemporaneous wrap: housing starts **1.275M (−2.6%)**, permits **1.394M (−2.7%)**. https://www.census.gov/construction/nrc/current/index.html
- ChartExchange / EODData: XLU close ~**41.67–41.70**, volume ~18–20.5M. https://chartexchange.com/symbol/nyse-xlu/historical/
- StockAnalysis / StockScan: NEE ~**+1.12%** (~$81.27); DUK ~**+0.66%** (~$118.55); SO ~**+0.15–0.56%**; CEG ~**+1.22–1.26%**
- StreetStats / sector wraps: XLK ~**+2.2–2.4%**, XLY ~**+1.2%**, XLU lagging growth, XLP muted
- GuruFocus / MacroMicro / Investing: 30Y ~**5.29–5.32%** (down from ~5.35%)
- WSJ/Cboe wraps: VIX ~**15.44–15.49**; WTI ~**$101–102**
- X posts 2026-09-17: claims 196k vs 208k; 10Y back below 5%; XLU/SPY ratio at long-term lows (relative disinterest, not a 1d smash)
- Morning prediction (injected): S0=0, S1=−1, S2=0, S3=0, S4=0, predicted flat/flat, divergence true, FOMC not restacked, claims unscored

**Checked, nothing material for the 1d tape:** XLU-wide rate-case outcome; ETF flow spike (same-day creations not published); kinetic oil shock; CEG as ETF driver; FOMC leftover as a fresh smash.