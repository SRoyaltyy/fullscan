# Sector Outcome — Technology — 2026-09-21

Actuals: {'etf': 'XLK', 'pct': 2.7637078405546633, 'spy_pct': 1.5518133737258744, 'rel': 1.211894466828789, 'open': 191.7899932861328, 'close': 194.83999633789062, 'source': 'yf_download'}

# Sector Post-Session Review — Technology (XLK) — 2026-09-21

## 0. FACTS

**CLAIM:** XLK closed +2.764% on 2026-09-21, from an open of 191.79 to a close of 194.84.
**URL:** Injected deterministic actuals (yfinance); corroborated by Zacks/BATS real-time quote.
**PUBLISHED:** 2026-09-21.
**QUOTE:** "State Street Technology Select Sector SPDR ETF: (XLK) (Real Time Quote from BATS) As of Sep 21, 2026 03:56 PM ET $194.91 USD +5.31 (2.80%) Volume: 6,451,694."
**SUMMARY:** The injected close (194.84, +2.764%) and the BATS print (194.91, +2.80%) agree within rounding; the ETF closed near session highs.

**CLAIM:** SPY closed +1.552% on the same session.
**URL:** Injected deterministic actuals.
**PUBLISHED:** 2026-09-21.
**SUMMARY:** Broad market was strongly up, but XLK outran it.

**CLAIM:** XLK relative return vs SPY = +1.212%.
**URL:** Injected deterministic actuals (XLK 2.764% − SPY 1.552%).
**PUBLISHED:** 2026-09-21.
**SUMMARY:** Technology was a genuine relative leader, not merely beta.

**CLAIM:** The session was a broad tech-led rally, with the Nasdaq at a record high and chip/software/hyperscaler names leading.
**URL:** https://finance.yahoo.com/markets/live/stock-market-today-monday-september-21-dow-sp-500-nasdaq-080214605.html ; https://www.wsj.com/livecoverage/stock-market-today-dow-sp-500-nasdaq-09-21-2026/card/tVbFucRJIto5MWy0sbwX
**PUBLISHED:** 2026-09-21.
**QUOTE:** "US stocks surge on Monday, Nasdaq powers to all-time high as oil retreats… joined by a rally in bitcoin (BTC-USD) past $86,000 as tech stocks surged." / "The S&P 500 has swung back into positive territory for the month, thanks to a rally in tech shares today. Chip stocks, software firms, hyperscalers and shares of companies whose fortunes are tied…"
**SUMMARY:** The rally was explicitly attributed to tech/chips/software/hyperscalers plus retreating oil and falling yields — the exact factor set the morning card had identified.

**Path:** Open 191.79 → close 194.84. The ETF opened already up (PM:XLK +0.98% carried into the print) and extended through the day to close near the high. This was a trend-up day, not a gap-and-fade.

**ACTUAL_DIRECTION:** up. **ACTUAL_MAGNITUDE:** notable (+2.76% absolute, +1.21% relative).

---

## 1. What drove the sector

The driver set is the one the morning card named, and it fired harder than the card's band allowed:

1. **Shared macro impulse (S0's object).** Oil offered hard (CL=F −5.94%, BZ=F −5.76% on the morning print), real yields easing on the 1d impulse (DFII10 2.61, 1d −0.07), VIX ~15 in contango, Asia green with Kospi +1.65%, Europe green. That is a textbook long-duration risk-on impulse, and it resolved in the direction the card said. The post-close coverage confirms the causal chain: "investors cheered a decline in Treasury yields" and "oil retreats."
2. **AI-infra spine with a same-session HBM confirmation.** Samsung HBM4 yields ~80% / share gap narrowing to 17 pts (Sedaily 09-21), transmitted live via Kospi +1.65%. This was the one genuinely same-session sector fact, and it landed on the right side.
3. **Sector rotation into technology.** PM:XLK led the sector board; the 4-horizon relative tape was uniformly green; the close confirmed with a +1.21% relative day.
4. **Breadth inside the sector was better than the card credited.** The card scored S2 at +0.5 on the grounds that RTY +0.08% / DJIA +0.11% failed a broad-melt-up test. That was the right *index-level* observation but the wrong *sector-level* inference: the WSJ line names "chip stocks, software firms, hyperscalers" as a *group*, i.e. the sector's own breadth expanded well beyond a single mega-cap.

**PRIMARY_DRIVER:** Risk-on long-duration impulse (oil-off, yields-down, NQ +2.12%) landing on an intact AI-infra spine with a live HBM confirmation — resolved as a notable tech-led up day.

---

## 2. Audit of morning S0–S4 reads against reality

**S0_SHARED_MACRO = +1.0 — UNDERSCORED, direction right.** The card explicitly reasoned: "oil down ~6%… VIX 15 and in contango… NQ independently +2.12%… Real yields are falling today (−7 bp DFII10) — that is [+] duration/growth." Every leg resolved correctly. The card then capped S0 "below +2" because of the real-yield *level* (DFII10 2.61, 1m +20 bp). That cap was the single largest source of magnitude error: the level was a real tax, but it did not bind on a day when the 1d impulse was −7 bp and the tape was this strong. **Verdict: right sign, understated size.**

**S1_SECTOR_FACTORS = +1.0 — RIGHT, and the card's own discipline cost it size.** The card correctly refused to count capex + foundry + HBM as three spines, and correctly refused to treat Samsung HBM as an 08-12-style mega-cap beat. That anti-double-count discipline is sound. But it then discounted the *one* live same-session fact (Samsung HBM via Kospi) to "modest confirmation" and set S1 = +1. The tape says the AI-infra complex was the leadership group on the day. **Verdict: right sign, slightly understated.**

**S2_BREADTH = +0.5 — the weakest read.** The card's own HIT_GRID marked "Sector breadth expansion (% names up)" as PARTIAL and "Large-cap leadership inside sector" as HIT. Both were directionally right, but the card anchored breadth on RTY/DJIA (index breadth) rather than on the sector's internal participation. The post-close record shows chips + software + hyperscalers all bid — that is sector breadth expansion, not a narrow mega-cap tape. **Verdict: right sign, understated; the wrong breadth proxy was used.**

**S3_FLOWS_POSITIONING = 0.0 — CORRECT, and the zeroing was the right call.** The card zeroed the 09-10 crowded-long-fuel penalty because the causal overlay was inverted (corr −0.592 not ≤ −0.9, contango, oil offered, DFII down). Had it instead applied a crowding penalty, it would have subtracted from a day that went +2.76%. This is the cleanest decision on the card. **Verdict: correct.**

**S4_ETF_TAPE = +1.0 — RIGHT.** Uniformly positive 1d/3d/1w/1m relative plus green PM. Confirmation, correctly used as confirmation and not as thesis. **Verdict: correct.**

**Score arithmetic:** leading sum S0+S1+S2+S3 = +2.5, S4 = +1, same sign, no divergence. The factor card was *directionally unanimous and correct*. The failure was entirely in the band.

---

## 3. Interactions / double-count / knowable-at-open

**Double-count check — passed.** Oil + real-yield impulse + risk-on were counted once in S0. Samsung HBM was correctly filed as the *same* AI-infra cluster as the carried TSMC/HBM spine, not a second independent HIT. Software was correctly held to a low-weight sleeve. No double-count inflated the score.

**Knowable-at-open test — the magnitude was knowable.** At the open the desk had: NQ=F +2.12%, ES=F +1.35%, PM:XLK +0.98%, Kospi +1.65%, oil −6%, DFII10 −7 bp, VIX contango, and a 4-horizon relative-leadership tape. That is an unusually complete, unusually aligned bullish set. The card saw all of it and still published **flat/flat**. The information was there; the band rule threw it away.

**The specific mechanism of the miss.** The pipeline JSON shows `sector_rs_veto_applied: True` with `sector_rs_tape: {d1: -2.05, w1: -2.07}`, and `calendar_size_gate_applied: True`. The morning narrative explicitly invoked the **09-17 stale-RS-veto** lesson ("live tape outranks leftover 1w/1m lag") and the **09-18 NQ-binds-direction** lesson. But the *pipeline* still applied a stale-RS veto using d1/w1 values of −2.05/−2.07 that **contradict the injected Channel 1 tape** (1d rel +0.69%, 1w rel +1.12%). The narrative and the deterministic engine disagreed about what the relative tape even was, and the engine's stale, negative RS numbers flattened the published direction to flat. That is a data-plumbing bug, not a judgment error — and it is the highest-value fix from this session.

**The band rule is the second failure.** Even granting the RS veto, the card's own logic ("NQ ≥ +0.5% and leading scores agree up → do not publish flat") should have produced **up/mild**. The 09-14 lesson ("PM gap is direction, not a notable/severe extrapolant") was applied to cap magnitude at mild — but that lesson was written for a *gap* day, and this was a trend day with a confirming close. The card had no rule that lets a fully-aligned factor set with a live same-session sector catalyst reach **notable**.

**KNOWABLE_AT_OPEN:** yes — direction and at least "mild, likely notable" magnitude were both derivable from the open tape.

---

## 4. Outliers inside the sector

- **Semiconductors / AI-infra** were the leadership group (WSJ: "chip stocks, software firms, hyperscalers"). The card's MAP HEAT had semis "HEAT down (leftover)" and hardware/components down; the live tape overrode that, exactly as the 09-18 reflect lesson said it should. The card *knew* this rule and applied it in the narrative — but the nested-HEAT drag still shows up in the S2 = +0.5.
- **Software** was named as a leader post-close, validating the card's decision to keep it as a low-weight sleeve rather than a kill (the "SaaSpocalypse" debate did not bind).
- **Samsung / memory** was the single cleanest same-session outlier: an 09-21-dated HBM datapoint transmitted through Kospi +1.65% into the US semi complex. The card identified it correctly and then under-weighted it.
- **No negative outlier** inside the sector materialized. Every bearish candidate on the card (export controls, AI-spend peak, semi inventory correction, software multiple compression) was correctly marked MISS/CHECKED_EMPTY and none fired.

---

## 5. Verdict and carry-forward

The morning card got the **direction, the driver set, the double-count discipline, and the flows zeroing all right** — and still published **flat/flat** against a +2.76% / +1.21% relative day. That is a pure band-and-plumbing failure, and it is the fourth consecutive flat/flat miss in this scope (09-16, 09-17, 09-18, 09-21), all in the same direction. The rolling dir=0.3 / mag=0.2 is now a *systematic* bias, not noise.

**Two concrete fixes:**
1. **Repair the RS feed.** `sector_rs_tape {d1: -2.05, w1: -2.07}` is inconsistent with the injected Channel 1 tape (+0.69% / +1.12%). A stale-RS veto built on wrong numbers must not be allowed to flatten a direction that NQ, PM, and the factor card all confirm.
2. **Add a trend-day band rule.** When NQ ≥ +0.5%, PM:XLK ≥ +0.5%, the 4-horizon relative tape is uniformly green, *and* there is a live same-session sector catalyst (here: Samsung HBM via Kospi), the band should be allowed to reach **notable** — the 09-14 "PM gap is direction, not a notable extrapolant" rule should be scoped to gap days only, not trend days.

OUTCOME_BEGIN
SECTOR: Technology
ETF: XLK
ETF_PCT: 2.764
SPY_PCT: 1.552
REL_PCT: 1.212
ACTUAL_DIRECTION: up
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: Risk-on long-duration impulse (oil −6%, DFII10 −7bp, NQ +2.12%) landing on an intact AI-infra spine with a live same-session HBM confirmation (Samsung via Kospi +1.65%)
KEY_INTERACTION: Factor card was directionally unanimous and correct (S0+S1+S2+S3 = +2.5, S4 = +1, no divergence) — but a stale-RS veto (d1 −2.05/w1 −2.07, contradicting the injected +0.69%/+1.12% tape) plus a gap-day band rule flattened a fully-aligned setup to flat/flat
KNOWABLE_AT_OPEN: yes
MORNING_READ_VERDICT: Right direction, right drivers, right double-count discipline, right flows zeroing — wrong band; published flat/flat against a +2.76%/+1.21% relative day, the 4th consecutive same-direction flat/flat miss in this scope
OUTCOME_END