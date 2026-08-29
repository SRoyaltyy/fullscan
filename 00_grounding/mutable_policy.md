---
status: living_policy
updated: 2026-08-29
source: src/learn_cycle.py
covers: general, sectors, news
note: Injected into general + sector PREDICT. Core output formats unchanged.
see_also: 03_scoreboard/LEARNINGS.md
---

# Mutable policy (all workflows)

Last learn_cycle: **2026-08-29**. Promoted: 0. Human digest: `03_scoreboard/LEARNINGS.md`.

## Accuracy by topic (graded window)

- **general**: 53% (8/15)
- **sector:Basic Materials**: 55% (6/11)
- **sector:Communication Services**: 27% (3/11)
- **sector:Consumer Cyclical**: 64% (7/11)
- **sector:Consumer Defensive**: 45% (5/11)
- **sector:Energy**: 55% (6/11)
- **sector:Financial**: 36% (4/11)
- **sector:Healthcare**: 78% (7/9)
- **sector:Industrials**: 27% (3/11)
- **sector:Real Estate**: 55% (6/11)
- **sector:Technology**: 45% (5/11)
- **sector:Utilities**: 45% (5/11)

## Active adjustments (promoted lessons, truncated)

### a-basic-materials-xlb-call-builds-a-severe-up-score-from-str.md
---
trigger_pattern: "A Basic Materials/XLB call builds a severe-up score from strong structural supply/monetary-metal factors (record copper, DRC/Congo export ban, gold/silver surge, ultra-low inventories), but the current 1d XLB relative return is modest (<0.5%), a China-demand/PMI contraction is explicitly present as an offset, an active geopolitics/oil headline can flip the broad equity tape risk-off, and the analysis text says “temper.” The deterministic output still emits severe because component scores and multiplier are not adjusted to match the tempered conclusion."
corrected_behavior: "When the text concludes “temper” and the 1d XLB relative return is <0.5% with an active geopolitics/oil risk-off headline and an explicit China/PMI offset, score S0 as 0 or negative for the equity 

### a-bond-proxy-defensive-sector-utilities-xlu-is-scored-absolu.md
---
trigger_pattern: "A bond-proxy/defensive sector (Utilities/XLU) is scored absolute up from carried prior-session yield easing, with that same easing shock paid in both S0 and S1, on a morning when a high-impact inflation print has already been released, mega-cap AI/software earnings are already public from the prior after-hours, NQ is leading ES, and the only “fresh” sector tape is a 1–3 day relative bounce against still-ugly 1w/1m underperformance."
corrected_behavior: "Verify the economic calendar against a primary source before scoring any same-day 8:30 event. Do not score S0/S1 from carried yield easing; one easing fact cannot pay twice. If mega-cap growth earnings are already public and NQ leads ES, default Utilities to relative lag / flat-to-down absolute unless there is a fresh 

### a-bond-proxy-rate-sensitive-defensive-sector-staples-has-bee.md
---
trigger_pattern: "A bond-proxy/rate-sensitive defensive sector (staples) has been underperforming on 1w/1m because of a real-yield/duration headwind, and a scheduled high-impact CPI print is the dominant catalyst. Premarket equity futures are risk-on and the sector has positive leading flow/rotation signals (e.g., first net inflows in months, contrarian defensive-rotation calls). The model treats the CPI print itself as a risk-off trigger for defensives, scores S0_SHARED_MACRO negative, and emits down/mild — ignoring that an in-line/cool CPI would relieve the duration headwind and can make the defensive ETF outperform."
corrected_behavior: "Before scoring S0 negative for a rate-sensitive/bond-proxy sector on a CPI day, identify the sector’s dominant driver. If the dominant driver is re

### a-bond-proxy-rate-sensitive-sector-has-been-sold-off-for-mul.md
---
trigger_pattern: "A bond-proxy/rate-sensitive sector has been sold off for multiple sessions by a specific macro driver (rising yields). On the next call, pre-fetched data shows that driver is now easing (1d/1w yields tick down) and the sector ETF already shows 1d relative outperformance vs SPY. The analysis text identifies this as the key new input and may even call it a “leading positive” divergence, but component scores remain at or below neutral and the deterministic output continues to emit down/flat. The miss is amplified when a geopolitical/oil supply-shock risk-off headline is knowable at open, because it adds a defensive bid into the same sector."
corrected_behavior: "When the named prior driver is easing and the 1d tape is already showing relative outperformance, do not conti

### a-catalyst-is-labeled-fresh-positive-based-on-deal-size-and.md
---
trigger_pattern: "A catalyst is labeled “fresh positive” based on deal size and participant names without checking how the market already traded it after the announcement. The catalyst is actually stale-negative (supplier financing its own customers = circular-financing alarm) and the relevant stock already fell days earlier on the same news. This stale-positive read is counted in S1, the multiplier, and the fresh-catalyst list, while a scheduled high-impact macro release is pending and the tech tape is crowded/extended."
corrected_behavior: "Before scoring any catalyst, verify how the market already reacted to it. If the relevant stock/ETF fell on the news, classify it as stale-negative or neutral, not fresh-positive, and do not use it as a positive S1 driver, multiplier input, or fre

### a-communication-services-xlc-call-faces-a-live-geopolitical.md
---
trigger_pattern: "A Communication Services/XLC call faces a live geopolitical/oil supply-shock risk (e.g., Hormuz/oil near $90), rising real yields, unresolved Alphabet/Meta AI-capex/FCF negatives, and XLC persistently lagging SPY on 1d/1w/1m, while premarket futures are positive and a scheduled CPI print is moderate. The correct output is down/mild; positive futures and a benign CPI are offsets, not reasons to flip the sector call to up."
corrected_behavior: "No correction required. Continue to cap at flat/down caution when the 2026-08-11 geopolitical/oil risk-off lesson is active, even with positive futures. Optional refinement: when both Alphabet and Meta are under fresh negative AI-capex/FCF catalysts at the open, S1 could be scored negative rather than neutral, since XLC’s two-sto

### a-consumer-cyclical-down-call-is-driven-by-a-genuinely-negat.md
---
trigger_pattern: "A Consumer Cyclical down call is driven by a genuinely negative macro spine (geopolitical/oil risk-off, negative futures, weak retail/sales/sentiment data) but the model fails to scan for knowable sector-level offsets: a positive pre-market catalyst in a major holding (Home Depot Q2 beat), defensive rotation into discretionary on a tech-led selloff, and the concentrated ETF’s largest weights (AMZN/TSLA) trading flat/mildly down. The model converts strong negative S0/S1 into severe without checking whether the selloff’s composition actually hits XLY’s dominant holdings."
corrected_behavior: "Before selecting severe for XLY, decompose the risk-off by leadership (tech/semis vs broad) and inventory knowable same-morning sector positives among top holdings. If the selloff 

### a-consumer-cyclical-xly-call-is-built-from-prior-period-cons.md
---
trigger_pattern: "A Consumer Cyclical (XLY) call is built from prior-period consumer-fundamental positives (falling gasoline, resilient labor, strong travel/RevPAR) while a fresh knowable-at-open geopolitical/oil supply shock is active — e.g., Iran/Hormuz, Brent near $90 — and/or a high-impact CPI print is looming. Futures are flat-to-negative and XLY’s mega-cap concentration is not confirmed by the 1d tape. The morning treats the live energy-cost risk as a stale/inverted factor, keeps S1/S3/S4 positive, and emits an absolute up bias."
corrected_behavior: "If an active geopolitical/oil supply shock is knowable at the open, treat S0 as the dominant score and make it more negative for Consumer Cyclical (e.g., S0 = -2). Set the live energy-cost factor negative when oil is spiking rather t

### a-consumer-staples-defensive-call-during-retail-earnings-wee.md
---
trigger_pattern: "A consumer staples/defensive call during retail-earnings week with weak same-morning consumer data and a risk-off tape. The correct pattern is to treat the shared risk-off shock as a single flight-to-safety signal, not double-count weak consumer data as a pure tailwind, and to use live top-holding premarket strength or a sector bellwether beat as confirmation before allowing a notable magnitude."
corrected_behavior: "No correction required. Maintain the two-sided treatment of weak consumer data during retail-earnings week; count risk-off and flight-to-safety as one regime signal; require live top-holding/bellwether confirmation before converting a defensive bid into notable magnitude."
falsifier: "A future identical setup — risk-off tape, Walmart premarket strength ah

### a-correct-direction-financial-sector-call-on-a-constructive.md
---
trigger_pattern: "A correct-direction Financial sector call on a constructive tape (positive futures, easing long-end yields, positive S4 relative strength) where the deterministic pipeline total maps to “notable,” while the narrative, applying rolling magnitude-accuracy discipline and residual macro overhangs, independently caps the band at “mild.” Both bands are left in the emitted output, the scorecard snapshots the higher official pipeline band, and the magnitude is marked a miss despite the direction being correct."
corrected_behavior: "Before emitting the final SECTOR_SCORES, reconcile to one unambiguous total and magnitude band. If the narrative applies a magnitude temper — here, rolling mag accuracy 0.0 plus lingering Fed-minutes/consumer-credit/CRE overhangs — that temper must

### a-cyclical-sector-etf-that-remains-a-1w-1m-relative-laggard.md
---
trigger_pattern: "A cyclical sector ETF that remains a 1w/1m relative laggard has just posted a strong one-day relative bounce (>~1% vs SPY); the next session a mega-cap AI/tech earnings print is already public from the prior after-hours with NQ leading ES, and S1 is being filled with stale surveys plus already-traded awards (IDIQ ceilings, closed M&A)."
corrected_behavior: "Score that 1d bounce once — not in S2, S4, and HIT_GRID. When the live catalyst is a non-holdings mega-cap AHR already public and NQ leads ES, do not default follow-through up for a 1w/1m laggard cyclical; prefer flat or down:mild. Cap S1 at 0/+1 unless a fresh same-morning sector print confirms; verify award economics (ceiling vs obligated) and whether the name already traded the news. Confirm the economic calenda

### a-defensive-bond-proxy-sector-is-predicted-down-flat-because.md
---
trigger_pattern: "A defensive/bond-proxy sector is predicted down/flat because (1) a top-holding bellwether dropped sharply in the prior session, (2) a prior-day hawkish Fed-minutes headline is still being treated as a live negative, and (3) positive futures are assumed to mean defensives will lag. Meanwhile the same-morning reversal checklist is positive — futures ≥ +0.3%, real yields easing, oil not spiking — and the prior session’s tape already absorbed the top-holding drop without the sector falling. The stale negative is over-weighted, causing a missed defensive flight-to-safety up day."
corrected_behavior: "For a defensive sector, first check catalyst freshness. If the negative catalyst is ≥1 session old and the same-morning reversal checklist is positive, treat the negative as a

### a-defensive-bond-proxy-sector-utilities-faces-a-risk-off-tap.md
---
trigger_pattern: "A defensive bond-proxy sector (Utilities) faces a risk-off tape while long-end/10Y yields are rising. The model sees a fresh geopolitical defensive bid, positive relative tape, and confirmed inflows, scores all components positive, and treats the “fresh” risk-off catalyst as an exception to the standing relative-vs-absolute lesson. The result is an absolute up call when the correct default is relative outperformance with a flat-to-negative absolute close."
corrected_behavior: "When a bond-proxy sector has both a risk-off defensive bid and rising 10Y/long-end yields, default to relative outperformance / flat-to-negative absolute. Do not upgrade to absolute up just because the risk-off catalyst is fresh. Weight the rising-yield headwind at least as heavily as the defens

### a-defensive-relative-strength-healthcare-call-is-already-cor.md
---
trigger_pattern: "A defensive/relative-strength Healthcare call is already correctly read as a reversal—negative 1d/3d relative tape plus a growth/tech-led tape unwinding the prior defensive bid—but S1 is left at 0 because the policy category was checked too superficially as “nothing material,” while a same-day drug-pricing/policy overhang is actually live in sector media. The result is a directionally correct but magnitude-understated down/flat call where the actual is down/mild."
corrected_behavior: "When a reversal call is already confirmed by S2/S4 tape, run a same-day audit of policy/regulatory headlines before scoring S1=0. If a live negative policy/pricing narrative targets mega-cap pharma/insurers (drug-cost executive orders, record price-drop claims, combination-shot cost warn

### a-defensive-sector-relative-call-is-built-on-a-carried-rotat.md
---
trigger_pattern: "A defensive-sector relative call is built on a carried rotation theme (“tech-led risk-on unwinds the healthcare hedge”) without re-checking the live commodity/geopolitical tape. When oil/geopolitical supply-risk headlines are live (chokepoint disruption, Brent up sharply) and the prior session was already dragged by energy/stagflation, the actual regime is risk-off, so the defensive sector should be scored as a relative bid, not as a reversal/lag."
corrected_behavior: "Before scoring S2/S3/S4 for healthcare or any defensive sector, audit the live macro tape: (1) Is there an oil spike tied to a chokepoint/supply-risk story? (2) Is SPY under pressure from rising yields/geopolitics rather than tech rotation? (3) Are futures flat only because the oil shock is still develo

### a-defensive-staples-call-on-a-session-where-a-mega-cap-ai-te.md
---
trigger_pattern: "A defensive/staples call on a session where a mega-cap AI/tech earnings print is already public from the prior after-hours, Nasdaq futures lead ES (risk-on, NQ ≥ +0.5%), and the sector is already a 1d/1m relative laggard — but the model misdates the print as pending after today's close, parks it as unscored event risk, keeps S0 at 0, and applies the no-fresh-catalyst mild/flat cap."
corrected_behavior: "Before labeling mega-cap earnings “pending,” verify the actual report timestamp. If results/guide are already public overnight, treat them as a live shared-macro anti-FTS shock for staples (S0 negative), count NVDA + NQ>ES + XLP lag once, and do not fire the no-fresh-catalyst mild cap. With NQ ≥ +0.5% and a confirming negative 1d/1m relative tape, allow down/notable. D

### a-defensive-staples-sector-is-entering-a-heavy-retail-earnin.md
---
trigger_pattern: "A defensive/staples sector is entering a heavy retail-earnings week (WMT/TGT/HD) while the same morning's macro data is weak (retail sales, consumer sentiment). The model scores that weak consumer data as a one-way flight-to-safety tailwind in both S0 and S1, treats positive 1d/3d/1w relative tape as confirmation in S2/S4, and underweights the event risk that XLP's largest holdings are the exact retailers about to report on that weak data. Result: all-components-positive up/notable call in a session that de-risks the sector."
corrected_behavior: "Before scoring Consumer Defensive in a retail-earnings week, classify weak consumer spending data as two-sided: it may support staples as bond-proxies, but it also undermines the revenue base of the mega-cap retailers reporti

### a-defensive-staples-sector-prediction-is-set-to-flat-solely.md
---
trigger_pattern: "A defensive/staples sector prediction is set to flat solely because premarket US futures are flat and global sessions are mixed, while an active geopolitical/oil supply-shock headline (e.g., Strait-of-Hormuz style impasse) and/or a scheduled high-impact CPI print is present in the news cycle. The model treats the day as neutral/risk-on, scores S0_SHARED_MACRO at 0 or positive, and emits flat/flat — but a defensive ETF can still fall modestly in absolute terms on broad risk-off while outperforming SPY relatively, so flat/flat is a graded miss."
corrected_behavior: "When an active geopolitical/oil/CPI risk-off catalyst is present, score S0_SHARED_MACRO negative/risk-off rather than neutral, keep magnitude capped at mild (not notable), and emit down/mild when the defensi

### a-financials-call-scores-an-extended-multi-week-win-streak-a.md
---
trigger_pattern: "A Financials call scores an extended multi-week win streak and strong 3d/1w relative tape as rotation/IB support in S1, S2, and S3 at once, while S4 1d relative tape is already flat/neutral, and a mega-cap AI/tech earnings print is already public from the prior after-hours with NQ leading ES. The book still emits absolute up from that streak, treats a prior-session PCE print as both still-pending and in-line relief, and does not treat AI re-acceleration as rotation-reversal risk to the crowded financials bid."
corrected_behavior: "When a non-holdings mega-cap AI/tech print is already public overnight and NQ leads ES, that is the live flow regime for Financials — the inverse of the tech-risk-off rotation-into-banks license. Do not triple-count one 3d/1w streak across S

### a-financials-call-treats-a-long-end-yield-spike-as-a-one-sid.md
---
trigger_pattern: "A financials call treats a long-end yield spike as a one-sided headwind and applies 'relative outperformance does not make an absolute up call' mechanically, even when the risk-off tape is tech/growth-specific, the long-end selloff is the rotation catalyst out of tech, and XLF has strong sustained relative strength (1d rel ≥ +0.4%, positive 3d/1w, bank index in multi-session uptrend)."
corrected_behavior: "On tech-specific yield-driven risk-off days, treat the long-end move as two-sided for financials: (1) a NIM/rate headwind, but (2) a rotation tailwind out of high-multiple growth into value/financials. If XLF premarket relative strength is strong (≥ +0.4% 1d rel), 3d/1w relative tape is positive, the bank index is in a multi-day uptrend, and credit spreads are not b

### a-financials-prediction-treats-any-2s10s-steepening-as-an-un.md
---
trigger_pattern: "A Financials prediction treats any 2s10s steepening as an unconditional bank NIM tailwind and scores it positively in S0/S1, even when the steepening is actually a bearish long-end Treasury selloff — 30y auction at ~5.22% (highest since 2001), 10y at multi-decade highs, oil/geopolitical inflation fears live. The model records the steepening as a positive, double-counts it, and leaves direction up, while the flat S4 tape is used only to cap magnitude in prose and the pipeline still emits a higher official band. The sector then underperforms because rate-sensitive financials are hurt by long-end yields and term-premium repricing."
corrected_behavior: "Before scoring S0/S1, decompose the steepening. If it is long-end-driven — 30y/10y at multi-decade highs, term premium e

### a-financials-sector-call-has-strongly-positive-structural-fa.md
---
trigger_pattern: "A Financials sector call has strongly positive structural factor scores (curve steepening, credit tightening, capital-markets/trading strength) and a positive shared-macro score taken from pre-market risk-on indicators, but the sector ETF tape at open is flat/neutral (S4_ETF_TAPE ≈ 0.0) and divergence_flagged is True. A live geopolitical/oil supply-shock headline (e.g., Iran/Hormuz) and/or a high-impact CPI print is knowable at open. The analysis text explicitly says “damp/mild,” but the deterministic score still emits up/severe because the multiplier and component scores are left unchanged. Financials may still outperform SPY relatively, but that does not make the absolute move up or severe."
corrected_behavior: "When S4 is flat/neutral and divergence_flagged=True, c

### a-financials-xlf-call-with-s0-0-and-s1-0-mixed-flat-es-nq-le.md
---
trigger_pattern: "A Financials / XLF call with S0=0 and S1=0 — mixed/flat ES/NQ, leftover non-holdings AI impulse already public, two-sided scheduled policy event, no 8:30, long-end steepener correctly refused as NIM+ — emits absolute down because yesterday’s completed rotation-out is copied into S2 (prior-session breadth), S3 (trailing outflows), and S4 (prior 1d/3d rel) and treated as independent confirmation. A ban on calling up off a broken streak is used as a license to call down."
corrected_behavior: "Do not triple-count a completed prior-session rotation-out. With S0=0 and S1=0, set S2=0 unless a live premarket BKX/XLF breakdown is confirmed; trailing unit outflows are not a 1-day lid; S4 may describe the prior close, it does not forecast the next session after a large lag. Pref

### a-follow-through-session-where-the-dominant-catalyst-is-a-pr.md
---
trigger_pattern: "A follow-through session where the dominant catalyst is a prior-day macro print (e.g., benign CPI) that already drove the prior session's rally, overnight catalysts are positive but US index futures (B6) are flat within ±0.5% (zero futures confirmation), and a scheduled 8:30 ET data release (PPI/jobless claims) is pending — the pipeline emits NOTABLE from the carried catalyst alone, but the already-traded setup plus flat futures cap the day at MILD."
corrected_behavior: "When the dominant catalyst is a prior-day macro print and B6 is flat (within ±0.5%), treat the catalyst as partially priced: cap the final magnitude band at MILD and use multiplier ≤1.0 unless (a) a fresh same-day catalyst is known premarket (e.g., an 8:30 release direction, a dominant |B1|≥2 event), 

### a-fresh-same-morning-hard-data-macro-miss-e-g-china-ip-retai.md
---
trigger_pattern: "A fresh same-morning hard-data macro miss (e.g., China IP/retail) is released while a stagflation/oil narrative has already dragged US stocks lower the prior session; US futures are only flat-to-mildly positive (ES within ±0.5%); the strongest positive component is a carried, LOW-confidence Fed-easing repricing from the prior week; and there is no fresh index-relevant mega-cap earnings catalyst. Asia rallies but is led by non-China-demand sectors (e.g., Korean chip rebound) and is misread as confirmation that the market embraces the bad-news-good channel."
corrected_behavior: "When the fresh same-morning catalyst is a hard-data growth miss feeding an existing stagflation/down tape and the main positive is a carried repricing (not a fresh catalyst), score B1 at -2 for 

### a-fresh-same-morning-negative-mega-cap-bellwether-earnings-m.md
---
trigger_pattern: "A fresh same-morning negative mega-cap/bellwether earnings miss (Walmart-type) or hard-data macro miss coincides with an active geopolitical/oil supply shock (Hormuz-type, Brent up sharply), while US index futures are flat-to-mildly-negative (ES/NQ within ±0.5% and not confirming up) and the main positives are carried (prior-day Treasury-buyback yield relief, Asia/Kospi chip rebound, low-confidence dovish Fed repricing)."
corrected_behavior: "When a fresh same-morning negative mega-cap/bellwether earnings miss or hard-data miss is present together with an active geopolitical/oil supply shock, and ES/NQ do not independently confirm up (net ≥ +0.5%), score B1 at -2 (or at least -1.5) for the negative cluster, B7 at -1 when oil is spiking, cap carried positives (B3 at +0

### a-fresh-top-holding-legal-regulatory-catalyst-e-g-a-trial-op.md
---
trigger_pattern: "A fresh top-holding legal/regulatory catalyst (e.g., a trial opening with large headline damages) is correctly identified and the sector is correctly scored down, but the model escalates the magnitude to severe based on the severity of the single-company headline alone. It fails to scan for knowable offsetting positives in the other mega-cap leaders of the same concentrated sector ETF. On a risk-off day, a concentrated sector can outperform SPY when the negative catalyst hits only one mega-cap leader while others carry positive flows/news. The severe band should require the legal shock to threaten both mega-cap leaders, or to hit one leader with no offsetting positives elsewhere in the sector."
corrected_behavior: "Whenever a top-holding legal catalyst is present, aft

### a-healthcare-xlv-call-after-a-strong-multi-week-relative-run.md
---
trigger_pattern: "A Healthcare/XLV call after a strong multi-week relative run with many carried positive factors, but the pre-fetched 1d tape is negative absolute and a fresh, knowable-at-open negative catalyst hits a high-weight XLV sub-industry — e.g., a Medicare Advantage rate proposal shock to managed care (UNH, HUM, CVS). The analysis may cite the negative catalyst but treat it as a “caution” or “partial offset” while keeping S1/S4 positive and emitting up/notable."
corrected_behavior: "When a fresh negative policy/regulatory catalyst directly hits a high-weight sub-industry and the 1d XLV tape is already negative, reweight S1 to neutral/negative, set S4 to ≤0 for absolute direction, and do not use 3d/1w/1m relative strength to justify an absolute up call. The sector may still ou

### a-healthcare-xlv-call-with-s0-0-flat-mixed-es-nq-leftover-te.md
---
trigger_pattern: "A Healthcare/XLV call with S0=0 (flat/mixed ES/NQ, leftover tech impulse already in the prior close, two-sided scheduled policy event, oil not spiking) scores S1 negative from residual drug-pricing overhang plus “rotation out,” then copies the same completed prior-session relative lag into S2 (breadth), S3 (trailing outflows), and S4 (prior 1d/3d rel), treats that as independent confirmation, and emits down/mild. A rates/Fed binary is parked as “not an HC spine” even though duration hits XBI while XLV mega-caps typically buffer the ETF to SPY beta."
corrected_behavior: "Do not triple-count a completed prior-session XLV relative lag. With S0=0 and flat futures, set S2=0 unless a live premarket XLV/mega-cap breakdown is confirmed; trailing unit outflows are not a 1-day 

### a-high-beta-consumer-cyclical-etf-xly-is-scored-s0-positive.md
---
trigger_pattern: "A high-beta consumer/cyclical ETF (XLY) is scored S0 positive because ES/NQ are green and the 08-21 reversal checklist is ticked, while the morning itself attributes the NQ impulse to a non-holdings mega-cap earnings print (NVDA/XLK) and the sector’s own 1d/3d/1w relative tape is already negative."
corrected_behavior: "Do not score S0=+1 or invoke 08-21-up when the futures impulse is explicitly a non-XLY holdings earnings print and XLY 1d/3d/1w rel is already negative. Map S0 from consumer-beta participation (AMZN/TSLA/HD), not index futures. Let S4 go negative on that lag; do not keep S4=0 on the same green-futures story. Verify the sector-owned print calendar; a T-1 sticky PCE/confidence print stays in the consumer tape and does not relieve S1. Default to down/mild 

### a-high-beta-cyclical-sector-xly-is-predicted-down-after-a-sh.md
---
trigger_pattern: "A high-beta/cyclical sector (XLY) is predicted down after a sharp risk-off day, but the following morning has a clear reversal checklist: US index futures ≥ +0.3% (ES/NQ positive), real yields easing, oil not spiking, and the leading negative fundamental factors (retail sales miss, consumer sentiment collapse, Fed minutes, bellwether earnings) are stale — released 1–7 days earlier — with no fresh same-day negative catalyst. The model flags a leading-vs-tape divergence but keeps the stale down bias and only shrinks confidence."
corrected_behavior: "When the same-morning reversal checklist is positive and no fresh negative catalyst hits the sector that day, set the day-horizon direction to the tape for high-beta/cyclical sectors — up if futures are clearly positive, rea

### a-high-conviction-technology-xlk-call-with-a-benign-schedule.md
---
trigger_pattern: "A high-conviction Technology/XLK call with a benign scheduled macro print, fresh mega-cap/AI-infrastructure earnings beats, strongly positive Nasdaq futures, and no leading-vs-tape divergence. The narrative and deterministic pipeline agree, and the outcome fully confirms the call."
corrected_behavior: "No change required. Keep the freshness gate: fresh index-relevant AI/mega-cap catalysts plus non-negative/risk-on futures allow the model to override crowding and real-yield dampeners. Do not generalize this to stale catalysts or negative futures. Also avoid over-weighting S3 crowding as a magnitude cap when fresh catalysts have already drawn flows."
falsifier: "This no-error pattern is falsified if XLK declines despite an in-line CPI, fresh index-relevant AI-infra earn

### a-large-positive-trial-regulatory-catalyst-originates-in-one.md
---
trigger_pattern: "A large positive trial/regulatory catalyst originates in one sector name one to two sessions earlier, but the prior/current tape shows broad sector-wide spillover — large-cap partner co-movement, sub-complex group move, multiple unrelated names participating, and a multi-week milestone/best-week context. The model nevertheless classifies the catalyst as “single-ticker,” applies the single-ticker no-domination rule, and uses a flat 1d relative tape to cap the magnitude at mild."
corrected_behavior: "Before applying the single-ticker breadth rule, audit the prior 1–2 sessions for catalyst spillover: did the large-cap partner/competitor move, did the sub-complex trade as a group, are multiple unrelated names up, and is the sector at a notable multi-week milestone? If spi

### a-long-duration-rate-sensitive-sector-reits-faces-a-live-rat.md
---
trigger_pattern: "A long-duration, rate-sensitive sector (REITs) faces a live rate spine of rising long-end yields — 30Y at a multi-decade high, real yields higher, 10Y rising — on a risk-off tape, while the sector’s 1d/3d relative tape is positive/defensive. The correct output is down/mild: the leading rate shock drives absolute direction down, and the defensive relative bid caps magnitude at mild rather than allowing notable/severe."
corrected_behavior: "No behavioral correction is needed. Continue to treat a live long-end rate shock at a multi-decade high as directionally dominant for REITs, while treating short-term positive relative tape as a magnitude cap, not an absolute up signal. Optionally reconcile the internal total-score/divergence-flag inconsistency for cleanliness, but i

### a-long-duration-rate-sensitive-sector-reits-has-been-lagging.md
---
trigger_pattern: "A long-duration/rate-sensitive sector (REITs) has been lagging, real/nominal yields tick up into a scheduled CPI print, and premarket equity futures are positive. The model applies an active “CPI imminent => S0 negative for REITs” lesson without treating the CPI surprise as binary. It also sees a lower 2Y pre-CPI but treats it as a minor offset rather than an easing-expectation tell that a cool print could flip the rate spine and rally rate-sensitive assets."
corrected_behavior: "When CPI is imminent for a long-duration/rate-sensitive sector, treat the catalyst as two-sided. Before scoring S0, check the pre-CPI yield-curve positioning: if the 2Y is drifting lower / easing expectations are visible, do not default S0 to -1; score at least 0 and consider positive S0, bec

### a-long-duration-rate-sensitive-sector-reits-utilities-is-cal.md
---
trigger_pattern: "A long-duration, rate-sensitive sector (REITs/utilities) is called up/positive because prior-session real yields were easing and a CPI print was cool, but the live morning tape contains a long-end Treasury selloff/term-premium repricing (30Y near multi-decade high), oil/geopolitical supply-risk headlines, and hawkish Fed repricing. The model double-counts the same “duration relief” theme across S0/S1/S4, underweights the live rate shock, and leaves the official band unreconciled with the deterministic pipeline."
corrected_behavior: "Before scoring a REIT/long-duration up call, run an explicit live-rate check: are 10Y/30Y yields rising at open, is the 30Y at/near a multi-decade high, is there a Treasury supply/term-premium story, is oil/geopolitical inflation risk acti

### a-macro-headwind-hawkish-fed-minutes-rate-hike-risk-was-rele.md
---
trigger_pattern: "A macro headwind (hawkish Fed minutes, rate-hike risk) was released 1–2 sessions earlier and is already reflected in the prior week tape, while the same-morning reversal checklist is positive: US index futures ≥ +0.3%, real yields easing, oil off highs. The model runs the reversal checklist but still lets the stale macro headline keep S0 negative, producing down/flat and missing a rebound."
corrected_behavior: "When a macro headwind is already priced over the prior 1–2 sessions AND the morning reversal checklist is positive, score S0 at 0 or positive, not -1, unless a fresh same-morning shock has appeared. A non-fresh legal/regulatory overhang should keep S1 at 0 and must not convert the call to down. The output should be flat/up, not down/flat."
falsifier: "If the sa

### a-materials-call-has-a-strong-commodity-specific-bullish-cat.md
---
trigger_pattern: "A materials call has a strong commodity-specific bullish catalyst (fresh copper squeeze / record backwardation) while the same-morning macro tape is risk-off: US futures flat-to-mildly-negative, prior session SPY dragged lower by oil/stagflation, a China hard-data miss is released, and oil/geopolitical headlines are live. The model focuses on the sector-specific positive and misreads flat futures/Asia strength as bad-news-good confirmation, producing an up/severe call that is reversed by the macro drag."
corrected_behavior: "Before weighting sector-specific positives, check the macro risk-off overlay. If US futures are flat-to-negative, the prior session saw SPY lower on oil/stagflation concerns, and a same-morning China hard-data miss is live, do not build an up/seve

### a-predict-file-is-dated-on-a-us-cash-closed-day-weekend-holi.md
---
trigger_pattern: "A predict file is dated on a US cash-closed day (weekend/holiday) and the note already treats the prior Friday close as fact while forecasting the next cash session, but the grader injects that already-printed Friday OHLC and records a direction/magnitude miss with ops_fail false."
corrected_behavior: "OPS gate before scoring: if predict.md’s calendar date is not a US cash session, or the note already cites the injected SPX close as prior-session fact and forecasts the next cash day, set ops_fail=true and leave direction_hit/magnitude_hit null; pair the file to the next cash session or leave ungraded. Do not rewrite B0–B7 or flip direction off the prior Friday print."
falsifier: "If a weekend-dated file is written as a same-session Friday forecast (does not treat Frid

### a-prior-session-geopolitical-oil-supply-shock-e-g-hormuz-clo.md
---
trigger_pattern: "A prior-session geopolitical/oil supply-shock (e.g., Hormuz closure, Brent near $90) is still treated as “live” on an XLC call even though the pre-open package already contains reversal signals: oil is falling, an official source reports supply routes/export volumes near normal, scheduled inflation data is in hand/benign, futures are positive, and/or a fresh knowable catalyst exists in a top XLC holding. The model double-counts the stale risk-off across S0/S2/S3/S4, mechanically applies the older “positive futures are only offsets” rule, and emits down/notable. Actual outcome is a risk-on reversal in which XLC outperforms SPY by >1%."
corrected_behavior: "Before finalizing a down call on XLC, run a reversal checklist: (1) is oil actually falling, not just below a head

### a-rate-sensitive-defensive-sector-consumer-staples-has-been.md
---
trigger_pattern: "A rate-sensitive/defensive sector (Consumer Staples) has been lagging on 1w/1m due to a real-yield/duration headwind. A benign CPI print has already produced one session of yield relief, and positive leading signals are present: first ETF inflows in months, a defensive-rotation call, and 1d/3d relative tape inflecting positive. On the follow-through morning, a same-day scheduled inflation release (PPI) is pending. If PPI also prints cool, the second consecutive tame inflation print can push rates down further and make the bond-proxy defensive outperform SPY by >0.3%, producing a notable move — not merely flat/mild — even though the broad tape is risk-on."
corrected_behavior: "Before calling “no fresh catalyst,” check the economic calendar for a scheduled same-day infl

### a-rate-sensitive-defensive-sector-utilities-has-a-confirmed.md
---
trigger_pattern: "A rate-sensitive/defensive sector (utilities) has a confirmed positive catalyst (inline CPI → second session of yield relief) and a strong structural narrative (AI data-center load growth), but the broad tape is risk-on with growth/tech leading and a same-day sector-narrative headwind appears (e.g., a credible research report questioning AI power-demand realization). The model correctly flips direction to up but over-corrects magnitude by letting structural/flow component scores dominate, producing “notable” when the defensive bid is structurally capped."
corrected_behavior: "When the broad tape is risk-on with tech leading and a same-day or fresh sector-narrative headwind challenges the structural thesis, cap the magnitude to “mild” unless there is durable sector lea

### a-rate-sensitive-sector-call-uses-a-yield-change-table-label.md
---
trigger_pattern: "A rate-sensitive sector call uses a yield-change table labeled through the prior close and treats those 1d changes as the live open tape for the current session. When long-end yields are already at a multi-decade high, the model applies the live-rate and level-vs-change lessons to force a down spine without verifying whether the current open/premarket curve is actually falling for a second day. An oil slide is noted but dismissed under a geopolitical-oil “doesn’t fire” heuristic, even though the oil → inflation → yield → bond-proxy channel is exactly the live driver. Result: S0/S1 are inverted, the direction is forced down, and the flat/close-to-flat actual becomes a miss."
corrected_behavior: "Before scoring a duration sector, verify the actual open/premarket Treasur

### a-reit-call-is-made-up-because-same-day-nominal-real-yields.md
---
trigger_pattern: "A REIT call is made 'up' because same-day nominal/real yields are easing (DFII10, 10Y, and 30Y down on the day), while the 30Y remains at or near a multi-decade high and bond-market strain is the persistent structural backdrop. The same easing factor is scored positively in S0, S1, and S4, producing an up call that misses a flat or underperforming sector close."
corrected_behavior: "Before scoring rate easing as positive for REITs, check the absolute yield level and prior-session context. If 30Y/10Y remain near multi-decade highs and bond-market strain is persistent, treat a small same-day decline as stabilization/noise, not relief. Cap S0/S1 at 0 or negative, avoid placing the same easing factor in S4, default to flat/underperform relative to SPY, and reconcile any p

### a-reit-xlre-session-with-s0-0-and-s1-0-mixed-flat-futures-li.md
---
trigger_pattern: "A REIT/XLRE session with S0=0 and S1=0 — mixed/flat futures, live 10Y/30Y not independently verified falling, 30Y still in a multi-decade stress zone, sticky inflation already in, a two-sided scheduled policy speech not yet delivered — plus confirming multi-horizon relative lag in S2/S3/S4, leftover mega-cap/NQ beta that is not duration relief, and stale DC/industrial occupancy that is not a same-day up vote."
corrected_behavior: "No correction. Continue: do not one-way score a two-sided policy event in S0; verify the live curve rather than the prior-close 1d column; cap S0/S1 at 0 while 30Y remains in the stress zone and the open change is 1–2 bp; do not double-count the same inflation/policy object into S1 after the speech; keep magnitude at mild unless a verified o

### a-scheduled-high-impact-macro-data-release-nfp-cpi-fomc-with.md
---
trigger_pattern: "A scheduled high-impact macro data release (NFP/CPI/FOMC) with a soft/expected-easing narrative is the flagged dominant event-risk of the day, while a separate geopolitical de-escalation story is generating positive overnight momentum; the market's actual driver becomes the macro print's repricing of the Fed path, and the geopolitical/oil catalyst fades or flips as attention shifts."
corrected_behavior: "When a scheduled high-impact macro release is flagged as the day's dominant event risk, set the macro-linked components (Fed path B3, bonds B2) from the expected-print conditional under the regime lens — a soft-print-expected day under bad-news-good cannot carry a negative B3. Independently cap/discount geopolitical-oil components (B1/B7) to at most ±0.5 unless US fut

### a-sector-call-correctly-identifies-a-dominant-risk-off-macro.md
---
trigger_pattern: "A sector call correctly identifies a dominant risk-off macro overlay (S0 negative) but leans on a commodity-specific supply-squeeze bid (copper record backwardation, firm gold/silver) as a 'floor' that will keep the sector's decline mild and produce relative outperformance vs SPY. On a geopolitical/oil-driven risk-off day, the commodity bid co-moves with equities — copper and precious metals reverse lower with the tape rather than acting as a hedge — so the floor inverts into a drag, and the sector underperforms SPY. The model double-counts the same commodity positive as both an S1 positive AND a magnitude floor (S4 0), so when the commodity reverses, both the component score and the magnitude cushion are removed simultaneously."
corrected_behavior: "When the macro ri

### a-sector-call-has-a-decisively-negative-fundamental-spine-fr.md
---
trigger_pattern: "A sector call has a decisively negative fundamental spine from a prior-session hard-data macro miss (e.g., retail sales, consumer sentiment) plus an imminent sector-specific event risk (retailer earnings week), and the sector ETF already underperforms SPY across 3d/1w/1m. Premarket futures are flat (ES within ±0.5%) and there is no fresh same-day macro print. The model treats flat futures as a universal magnitude cap and downgrades the call from notable/severe to mild, producing a magnitude miss."
corrected_behavior: "The flat-futures cap should apply only when the call’s primary confirmation source is the premarket tape/broad-market follow-through. When the call is driven by a pre-existing hard-data spine and sector-specific event risk, flat futures should reduce con

### a-sector-call-has-a-scheduled-8-30-et-high-impact-macro-rele.md
---
trigger_pattern: "A sector call has a scheduled 8:30 ET high-impact macro release (including retail sales, PPI, CPI, jobless claims) but the narrative claims “no scheduled high-impact macro print today.” At the same time, S4 is flat/zero, divergence_flagged is True, and the narrative caps magnitude at MILD while the deterministic pipeline prints a different total_score/band from the same components, flipping the official band to NOTABLE. The scoreboard grades the pipeline output, producing avoidable magnitude misses and masking the correct mild/flat read."
corrected_behavior: "Before finalizing, reconcile the deterministic total with the narrative component arithmetic. If they disagree, treat the narrative component-derived score as authoritative or block the prediction until the misma

### a-sector-call-has-a-scheduled-8-30-et-macro-release-pending.md
---
trigger_pattern: "A sector call has a scheduled 8:30 ET macro release pending (PPI/CPI/jobless claims) but the narrative states “no scheduled high-impact macro print today.” At the same time, the narrative and SECTOR_SCORES block cap magnitude at MILD (S4=0, multiplier ≤1.0), while the deterministic pipeline prints a different leading_sum/total from the same components and flips the official band to NOTABLE. The scoreboard grades the pipeline output, not the narrative, producing a magnitude miss on what was actually an up/mild day."
corrected_behavior: "Before emitting, reconcile the final deterministic total with the component arithmetic: total = (S0+S1+S2+S3+S4) × multiplier. If the narrative caps at mild, the pipeline output must also be mild; a pipeline total that contradicts the w

### a-sector-call-s-narrative-and-sector-scores-block-cap-magnit.md
---
trigger_pattern: "A sector call’s narrative and SECTOR_SCORES block cap magnitude at mild (component sum × multiplier = 4.0), but the deterministic pipeline prints a different `leading_sum`/`total_score` from the identical components and emits the official band as NOTABLE. The scoreboard grades the pipeline output rather than the narrative, converting a correct mild call into a magnitude miss."
corrected_behavior: "Before accepting the pipeline output, validate `leading_sum` and `total_score` against the SECTOR_SCORES components. If Σ(S0..S4) × multiplier equals 4.0 and the pipeline emits NOTABLE, treat the pipeline total as erroneous and emit up/mild until the pipeline logic is fixed. Additionally, on a follow-through session with flat futures and no fresh same-day scheduled catalyst,

### a-sector-has-led-spy-on-a-defensive-relative-strength-rotati.md
---
trigger_pattern: "A sector has led SPY on a defensive/relative-strength rotation (3d/1w/1m all positive relative), but the current 1d relative tape is flat or barely positive; there is no fresh same-day sector catalyst; and the broad tape is setting up as a tech/growth-led risk-on day (Big Tech strong, Nasdaq futures ≥ S&P futures, SPY near record highs). In that setup, the carried rotation is at risk of reversing because the prior healthcare/defensive leadership was partly the inverse of tech momentum. The flat 1d tape is a reversal tell, not merely a magnitude cap."
corrected_behavior: "When a defensive/relative-strength sector has flat 1d relative tape after a strong 3d/1w/1m run, and the incoming tape is tech/growth-led with no fresh sector catalyst, do not convert the carried rota

### a-sector-predict-block-contains-explicit-predicted-direction.md
---
trigger_pattern: "A sector PREDICT block contains explicit `predicted_direction` and `predicted_magnitude_band`, but the scoreboard entry later records `predicted None/None` and marks both `direction_hit` and `magnitude_hit` False, even when the actual close confirms both. This is a grader/extraction/pipeline failure, not a forecasting failure."
corrected_behavior: "The scoreboard must extract predicted direction and magnitude from the PREDICT block fields before grading. If those fields are non-null and equal the actual direction/band, record `direction_hit: True` and `magnitude_hit: True`. For 2026-08-25 Energy, the row should be corrected to a HIT/HIT on the down/notable call. Rolling stats should be recomputed from corrected rows rather than propagating extraction `None` as a marke

### a-sector-prediction-is-narratively-scored-as-down-biased-bec.md
---
trigger_pattern: "A sector prediction is narratively scored as down-biased because of a live risk-off macro overlay (geopolitical/oil supply shock, sharply negative futures) and a fresh same-morning hard-data miss that hits the sector’s own fundamental spine, but the final official output is taken from the deterministic pipeline without reconciliation, allowing positive structural factors (S1) or flows (S3) to override the macro overlay and emit up/flat. The model also keeps soft survey data (ISM) positive while ignoring the harder coincident manufacturing print (G.17 industrial production) released the same morning."
corrected_behavior: "Before finalizing, reconcile the deterministic pipeline with the narrative score and live macro inputs. If the pipeline output conflicts with a narra

### a-sector-prediction-s-stated-magnitude-band-equals-the-outco.md
---
trigger_pattern: "A sector prediction’s stated magnitude band equals the outcome’s stated actual magnitude (e.g., predicted down/mild, actual magnitude = mild), but the scoreboard line records magnitude_hit False. This scoreboard/accounting inconsistency repeats across runs and can cause a false magnitude lesson to be learned from a correct call."
corrected_behavior: "Before writing a magnitude/reasoning lesson, cross-check the scoreboard magnitude flag against the OUTCOME block’s ACTUAL_MAGNITUDE and the predicted magnitude band. If predicted band == actual magnitude, treat the scoreboard False as a scoreboard/accounting flag error, flag the line for correction, and do not create a magnitude-threshold reasoning lesson."
falsifier: "If a published magnitude rubric is audited and shows 

### a-technology-xlk-call-has-fresh-real-catalysts-ai-infrastruc.md
---
trigger_pattern: "A Technology/XLK call has fresh, real catalysts (AI-infrastructure earnings: SMCI/CRWV/NBIS) but those catalysts were reported after the prior close and are already embedded in the strong 1d/1w relative tape. The macro driver (benign CPI) already produced the prior day’s rally; US equity futures, especially NQ, are flat/non-confirming; a scheduled 8:30 ET data release (PPI) is pending. The model converts fresh earnings catalysts plus strong S4 tape into NOTABLE without recognizing that S4 is partly double-counting S1 and that flat futures should cap the day at MILD."
corrected_behavior: "When the dominant catalyst is already reflected in the prior tape and NQ futures are flat/non-positive, cap the day at up/mild (multiplier ≤1.0). Treat flat NQ as failing the “positiv

### a-technology-xlk-down-call-has-strongly-negative-leading-com.md
---
trigger_pattern: "A Technology/XLK down call has strongly negative leading components (S0/S1 at -2), broad semiconductor/foundry weakness visible premarket, sharply negative NQ futures, and the deterministic pipeline emits a severe band while the narrative text drifts to “notable.” When the official pipeline band is the scored output and is confirmed by the actual tape, the narrative undercall is an interpretability issue, not a scorecard error."
corrected_behavior: "When S0 and S1 are both strongly negative, NQ futures are independently confirming downside, and a broad high-beta sector driver (semiconductor/foundry/AI-capex fear) is active, the narrative should adopt the pipeline’s severe band unless a concrete offsetting sector composition or beta argument exists. Reconcile any narra

### a-technology-xlk-session-is-scored-as-a-pending-binary-de-ri.md
---
trigger_pattern: "A Technology/XLK session is scored as a pending binary de-risk day (scheduled 8:30 macro still due, top-weight mega-cap earnings still after the close) so the 08-12 up/notable gate is marked not met and magnitude is capped at mild, without verifying primary-source timestamps or the cash gap, when those events already printed prior session/AHR and the confirmed beat is the live AI-infra spine."
corrected_behavior: "Before applying the pending-binary / no-confirmed-beat mild cap, verify company IR/BEA timestamps and open vs prior close. If the top-weight print is already public and market-confirmed and NQ is green, fire 08-12: one AI-infra cluster in S1 at +2/+3, do not score S3 as unresolved event-risk, and allow notable. A gap already through the mild band is a magnit

### a-two-name-duration-growth-sector-etf-xlc-meta-googl-heavy-i.md
---
trigger_pattern: "A two-name duration/growth sector ETF (XLC: META+GOOGL heavy) is scored S0=+1 because ES/NQ are green and real yields are easing, while the morning itself attributes the NQ impulse to a non-holdings mega-cap earnings print (NVDA/XLK spillover) and the sector’s own 1d relative tape is already negative with mixed-to-flat premarket leaders. The 08-21 green-futures rule is used as a license to call up rather than as a ban on keeping S0 negative. Pipeline emits up/flat; the sector lags a narrow tech rally."
corrected_behavior: "Green ES/NQ does not equal XLC participation when the futures impulse is a print in names XLC does not hold. Keep S0 at 0 in that case (08-21 still forbids flipping S0 to −1 on stale macro alone). If 1d XLC vs SPY is already negative and META/GOOGL 

### a-utilities-xlu-call-is-built-after-a-stretch-of-risk-on-gro.md
---
trigger_pattern: "A Utilities/XLU call is built after a stretch of risk-on, growth/tech-led tape (low VIX, Greed, strong Asia tech), but the same session has a scheduled 8:30 ET high-impact consumer/macro release (retail sales, sentiment) that can miss consensus. The model anchors S0 to the prior session’s risk-on rotation and treats that rotation as the permanent cap on the defensive/bond-proxy bid, without stress-testing the scheduled macro calendar for a regime-flip catalyst."
corrected_behavior: "Before finalizing a Utilities call, explicitly scan the day’s economic calendar for 8:30 ET high-impact releases. If a downside miss would plausibly flip a growth-led tape into a defensive rotation, do not let the prior day’s risk-on tape keep S0 at 0; score the bond-proxy/defensive bid as

### a-utility-defensive-sector-call-is-built-on-a-carried-defens.md
---
trigger_pattern: "A utility/defensive-sector call is built on a *carried* defensive rotation from a prior-session macro miss, with no fresh same-day macro release, no new absolute yield impulse, and an active sector-specific negative overhang (e.g., load-growth disappointment). The model treats the sector’s positive relative tape (1d/3d/1w rel > 0) as an absolute up signal in S0/S4, and leaves the official pipeline band stronger than the narrative score. On a flat-to-negative tape, XLU can outperform SPY while still closing slightly negative."
corrected_behavior: "When the only fresh positive driver is a carried defensive rotation — not a same-day macro miss and not a fresh yield rally — cap expected absolute direction to flat/down. Score S0/S4 as relative-support only: they justify XL

### active-geopolitical-oil-supply-risk-off-e-g-iran-hormuz-and.md
---
trigger_pattern: "Active geopolitical oil-supply risk-off (e.g., Iran/Hormuz) and/or an imminent high-impact CPI print is knowable at open, while the target sector is long-duration/rate-sensitive (REITs). Premarket equity futures are flat and global equity indices are mildly positive, so the model scores S0_SHARED_MACRO as 0 and treats 1d/1w real-yield easing as a sufficient offset, even though the 1m real-yield trend is still elevated and the sector ETF has been chronically lagging."
corrected_behavior: "When an active geopolitical/oil risk-off story and/or imminent CPI is present for long-duration REITs, score S0_SHARED_MACRO negative rather than neutral, even if premarket equity futures are flat and global equity indices are mildly positive. Treat the 1m real-yield trend as the oper

### after-a-duration-sector-miss-from-treating-a-prior-close-yie.md
---
trigger_pattern: "After a duration-sector miss from treating a prior-close yield table as the live curve, the next REIT call over-corrects: it treats the next prior-close easing / oil-slide snapshot as a live positive rate spine, scores that same shock in both S0 and S1, and emits up even though the long end is still in a multi-decade stress zone, the sector’s own 1d relative tape is already red, a sticky inflation print is already in the market, and a two-sided policy event is next session."
corrected_behavior: "08-25 is a ban on forcing down, not an up authorization — default flat unless the open/premarket 10Y/30Y/TIPS curve is independently verified live and still falling. If 30Y remains in the stress zone, cap S0/S1 at 0 (or negative) and do not double-count one easing/oil-slide sh

### after-a-multi-day-oil-down-stack-if-xle-s-last-completed-ses.md
---
trigger_pattern: "After a multi-day oil-down stack, if XLE’s last completed session was green / S4=0 (oil not transmitting) and the inventory/macro prints treated as live catalysts are already released, do not keep S1 at −2 or emit notable. Keep direction on relative fade; cap magnitude at mild/flat; shrink the multiplier when divergence is flagged."
corrected_behavior: "Re-verify oil as live for this session. If prior-day XLE was green despite CL/BZ down, the oil-down is in the price: S1 at most −1/0 unless a fresh same-session crude or inventory shock exists. Do not date already-released EIA/PCE as live event risk. When S4=0 and divergence is flagged, cap mag at mild/flat even if leading_sum is ≤−8. Keep direction if S2/S3 relative fade is intact. After a non-transmitting green XLE d

### after-a-multi-day-oil-down-xle-relative-fade-stack-that-alre.md
---
trigger_pattern: "After a multi-day oil-down / XLE-relative-fade stack that already missed magnitude (last session flat or non-transmitting), a still-red but sub-1% WTI/CL open is treated as a live S1 down spine and then copied into S2 (prior-day internals), S3 (trailing outflows), and S4 (yesterday’s 1d/3d/1w rel). Extreme cracks are scored HIT then damped out of S1. S0 is mixed/flat. The book still emits down."
corrected_behavior: "After that stack has already failed to deliver, default **flat/mild** unless **WTI/CL** (not Brent alone) is still ≥~1% down **and** XOM/COP are confirming in the **premarket**, not yesterday’s close. Count the oil shock once. Do not copy yesterday’s relative tape into S4 or restack S2/S3 as independent confirmation. If cracks/RBOB are the live mover, let 

### after-a-sector-etf-delivers-a-strong-one-day-relative-revers.md
---
trigger_pattern: "After a sector ETF delivers a strong one-day relative reversal (> +1% vs SPY) on the back of a fresh catalyst in a top holding, the next session treats that single reversal event as multiple independent positives: S1 credits the catalyst, S2 credits the breadth, S4 credits the tape. Absent a new comparable catalyst knowable at the open, this double-counting pushes the call into a NOTABLE magnitude band. The correct prior is that follow-through after an outsized reversal is usually MILD, especially when the broad market is extended near record highs and a soft-data/fade risk is present."
corrected_behavior: "Score the prior reversal once. Use the strong 1d relative tape as directional confirmation, not as an additional independent magnitude input. After a > +1% relativ

### an-effectively-two-stock-sector-etf-meta-alphabet-heavy-has.md
---
trigger_pattern: "An effectively two-stock sector ETF (Meta + Alphabet heavy) has a bullish case built from structural positives that are really one underlying thesis, prior-week flows are treated as same-day support, the largest holdings have unresolved capex/FCF vulnerability, and a fresh geopolitical/oil supply-shock risk is active at the open while futures are flat. The model emits up/notable from structural positives instead of flat/down caution."
corrected_behavior: "Deduplicate sector positives: ad-spend recovery + AI monetization = one ad/AI thesis; rotation + sector inflows = one flow observation. Before emitting an up call on XLC, check for knowable-at-open geopolitical/oil/high-impact-print suppressors. If a live geopolitical risk-off signal is present, score S0 negatively a

### an-energy-xle-call-is-scored-up-severe-from-a-geopolitical-s.md
---
trigger_pattern: "An Energy/XLE call is scored up/severe from a geopolitical supply-shock catalyst whose oil-price sign is correct, but the catalyst has already driven a large relative run in XLE (3d/1w rel > +4%) and is therefore largely priced in; a same-day official report (IEA/OPEC/EIA) contains demand-destruction or two-sided supply/demand signals, and the ETF's current-day relative tape is not confirming fresh leadership at the open."
corrected_behavior: "Before assigning severe to Energy, decompose S1 into (a) catalyst sign/freshness and (b) expected transmission to XLE. If XLE has already rallied 1w rel > +4% on the same geopolitical narrative, treat the headline as a continuation, not a new shock. If a same-day official report contains demand destruction or a two-sided signal,

### an-industrials-down-call-is-made-after-a-sharp-prior-risk-of.md
---
trigger_pattern: "An Industrials down call is made after a sharp prior risk-off day (e.g., a large Dow decline) when the same-morning reversal checklist is positive — US index futures ≥ +0.3%, no fresh same-day negative hard-data/earnings catalyst, and the macro negatives (hawkish Fed minutes, elevated yields) are one to two sessions old and already absorbed in the prior tape — while the medium-term sector tape is negative. The model weights the continuing macro overlay and negative 1w/1m relative tape over the knowable futures bounce and single-name positives, producing an absolute down call when the sector closes up but underperforms SPY."
corrected_behavior: "When the reversal checklist is positive in a cyclical sector (ES/NQ ≥ +0.3%, no fresh same-day negative catalyst), do not let

### basic-materials-xlb-has-s0-and-s1-net-zero-no-fresh-china-pr.md
---
trigger_pattern: "Basic Materials / XLB has S0 and S1 net-zero (no fresh China print, no metal surge/collapse, no Hormuz/oil>$90, two-sided scheduled event), and the only negatives are a prior-session chemicals-led breadth score (S2) plus the prior day's large negative 1d relative tape copied into S4. The model treats 8/25–8/27 'do not emit up' as a license to emit down."
corrected_behavior: "S4 confirms only the session being predicted, not the prior close. If 8/25/8/27 forbids up (chemicals-heavy book, mixed/two-sided S1, no NQ>>ES materials impulse), the residual is flat (or flat/mild), not down. Do not double-count the same prior-session chemicals lag in S2 and S4. Require same-morning chemicals/premarket confirmation before S2 = −1. A live gold fade is an S1 sleeve, not a reason t

### basic-materials-xlb-is-predicted-up-from-a-a-broad-risk-on-t.md
---
trigger_pattern: "Basic Materials / XLB is predicted up from (a) a broad risk-on tape that is actually Nasdaq/tech-led and (b) firm copper/gold prices, while XLB's own 1d relative tape is <0.5%, premarket breadth is mixed, and the ETF is chemicals-heavy rather than a pure metals basket. The model passes full S0/S1 macro/metal credit through to XLB without a composition/transmission discount, so a flat ETF can coexist with the metal/macro story."
corrected_behavior: "Before converting the leading sum to direction for XLB, apply a composition/transmission discount. If NQ is much stronger than ES, cap S0's sector push because the risk-on is tech-led, not materials-led. If S1 is two-sided (gold/miners HIT vs China/property copper-demand MISS) and XLB's 1d relative tape is <0.5% with mixed 

### bond-proxy-utilities-with-s0-0-on-a-two-sided-scheduled-fed.md
---
trigger_pattern: "Bond-proxy/Utilities with S0=0 on a two-sided scheduled Fed Chair speech, S1 negative from sticky/not-falling yields plus prior-session rotation, confirming red multi-horizon relative tape, flat futures, and no FTS bid — open-book down/mild."
corrected_behavior: "No open-book S0–S4 change. Keep an unresolved two-sided policy speech as event risk (S0=0), not a scored hawkish HIT; keep duration-over-FTS when a growth miss collides with sticky long-end; do not promote a single-name regulatory smash (PCG-class) into S1. Do not invent a notable-down path from the hawkish branch before the speech."
falsifier: "Same open book prints notable down without an in-session hawkish yield/hike-odds shock and without a large idiosyncratic name crash — then the mild cap is too tight a

### consumer-cyclical-xly-is-called-down-mild-from-macro-caution.md
---
trigger_pattern: "Consumer Cyclical (XLY) is called down/mild from macro caution, but actual outcome is down/notable because a same-day company-specific shock hits one of XLY's top 2-3 mega-cap holdings (e.g., CEO leadership news, single-name earnings/valuation shock) while the broad tape (SPY) is flat/up and the scheduled macro catalyst is benign. The specific shock is absent from all pre-open channels and is therefore not knowable at the open."
corrected_behavior: "Do not retrofit a magnitude correction. Keep the pre-open output when it is supported by the available data; do not systematically change down/mild to down/notable merely because XLY has high single-name concentration. In concentrated sectors, note explicitly that magnitude bands are less reliable and set confidence lower,

### consumer-defensive-xlp-after-an-outsized-prior-session-anti.md
---
trigger_pattern: "Consumer Defensive / XLP after an outsized prior-session anti-FTS relative smash, with S0=0 (flat ES/NQ, leftover mega-cap impulse already in the close, two-sided scheduled policy event, no 8:30), S1 only residual “relative still soft,” and the only independent negatives yesterday’s breadth (S2), trailing outflows (S3), and the completed 1d relative tape copied into S4 — then emit down/mild and map a hawkish policy branch as further lag."
corrected_behavior: "Do not copy a completed anti-FTS smash into S1+S2+S4 as a fresh down stack. With S0=0 and flat futures, treat S2/S4 as confirm-only of a shock already paid and default residual direction to flat/mild (keep the mild cap). 08-21 ES≥+0.3% licenses an up/reversal call; it does not force down when ES is flat. Premarke

### correct-severe-up-energy-call-driven-by-a-knowable-at-open-g.md
---
trigger_pattern: "Correct severe-up Energy call driven by a knowable-at-open geopolitical crude supply shock; no corrective trigger pattern established."
corrected_behavior: "No score change needed. For future Energy sector shocks, if the catalyst is sector-specific geopolitical supply and the broad tape is flat/red, classify the regime as sector_shock rather than broad risk_on; keep S0 muted and let S1 plus confirming tape carry the prediction."
falsifier: "Same setup producing XLE not severe or negative would falsify the crude-surge ⇒ XLE severe-up pattern."
current_behavior: "Predicted XLE up/severe from the oil surge (S1=2.0, score 13.0), but labeled the shared-macro regime as broad risk_on even though SPY was flat/red and the move was sector-specific."
evidence_cited: "XLE +4.66%,

### day-2-technology-xlk-after-a-confirmed-already-traded-mega-c.md
---
trigger_pattern: "Day-2 Technology/XLK after a confirmed, already-traded mega-cap/AI-infra beat (prior session rel >+1%), NQ inside ±0.5% (not independently weak or green), crowded long, and a still-pending two-sided high-impact policy event (Fed Chair speech / FOMC-class). The model treats mega-cap-earnings-over-macro-drag as a close-direction floor that forbids down, leaves S1 at +1 on the carried spine, and emits up (or “flat not down”) instead of allowing the fade path."
corrected_behavior: "mega-cap-earnings-over-macro-drag is an **open-session** constraint only: forbid down solely when the index-relevant beat is same-session/not fully in the tape **and** futures/internals do not independently confirm weakness. On day-2 with a live two-sided policy binary, do **not** forbid down; 

### dominant-positive-catalyst-geopolitical-de-escalation-strong.md
---
trigger_pattern: "Dominant positive catalyst (geopolitical de-escalation / strong earnings) is confirmed by Europe and US futures, but a single idiosyncratic Asia market crash (e.g., Kospi chip unwind) drags the Asia composite negative; the market follows the confirmed positive catalyst and ignores the outlier."
corrected_behavior: "When Asia composite negativity is driven by a single >4% idiosyncratic market move while other Asia markets are mixed and Europe + US futures are clearly positive, set B0 to 0/-0.5 rather than -1; do not let that outlier temper a dominant, independently confirmed catalyst; if the leading sum and futures are both positive and no divergence is flagged, do not cap the prediction in the MILD band solely on secondary macro noise."
falsifier: "If this exact setup

### energy-xle-prediction-where-premarket-oil-is-green-cl-bz-up.md
---
trigger_pattern: "Energy/XLE prediction where premarket oil is green (CL/BZ up) and a geopolitical supply-shock catalyst is actively in the same-day news cycle, but the model anchors to the prior 1d flat/negative XLE tape and to negative demand-side offsets (inventory build, IEA/OPEC) and mechanically caps the call at up/mild. The catalyst can re-ignite intraday and produce notable relative outperformance."
corrected_behavior: "Separate a stale catalyst from an escalating one. If oil futures are green premarket AND the geopolitical supply-risk catalyst is still in current headlines, treat the oil spine as the dominant S1 driver; do not let demand-side negatives cap S1 at +1.0 when the live catalyst is supply-positive. Do not require the prior 1d XLE tape to have already confirmed leade

### escalating-geopolitical-oil-supply-shock-catalyst-hormuz-typ.md
---
trigger_pattern: "Escalating geopolitical oil/supply-shock catalyst (Hormuz-type, Brent up >+1% and near multi-year highs) is present, but the energy equity ETF is extremely extended/overbought (RSI >70, 1w relative return >+5%) and the broad equity tape is turning risk-off into commodities while equities fade. In this setup, oil can rally while XLE closes flat-to-down as profit-taking and crowding unwind in the equity complex. An oil-up signal is not sufficient for an energy-equity-up call."
corrected_behavior: "When oil is up on an escalating supply shock but XLE is overbought/extended (RSI >70, 1w rel >+5%, already leading YTD) and the broad equity tape is risk-off or rotating into commodities rather than equities, the model should treat the overbought/crowding condition as a negati

### existing-basic-materials-rule-is-confirmed-after-a-hard-xlb.md
---
trigger_pattern: "Existing Basic Materials rule is confirmed: after a hard XLB run, with live geopolitical/oil risk, China demand contraction, and a decisively negative XLB relative tape, the correct output is down/mild — not up, and not notable."
corrected_behavior: "No change required. Maintain the active a-basic-materials rule and its 8/12 extension; do not assume S0 risk_off is the dominant driver when same-day macro is actually risk-on but sector factors are negative."
falsifier: "If the same negative-tape + China-drag setup closes up or notable, the rule would need revision."
current_behavior: "Pipeline can emit down/mild when sector-specific leading factors and XLB tape are negative even if broad futures/SPY are positive."
evidence_cited: "2026-08-13 XLB actual -0.51%, SPY +0.70

### for-a-bond-proxy-defensive-sector-when-s0-and-s1-are-both-ne.md
---
trigger_pattern: "For a bond-proxy/defensive sector, when S0 and S1 are both neutral because the macro forces are offsetting (risk-on equity tape vs easing/intermediate yields but elevated long-end), and the only negative components are carried relative-breadth/outflow scores (S2/S3) with no fresh decisive sector-level negative, the model over-commits to `down` instead of `flat`. Separately, if the PREDICT block contains explicit `predicted_direction` and `predicted_magnitude_band`, the scoreboard must not later record `predicted None/None`."
corrected_behavior: "When S0=0 and S1=0, and the only negatives are carried S2/S3 relative/flow scores, prefer `flat` (or `flat/up`) with a mild band on a risk-on tape; do not manufacture a directional down call from carried negatives alone. Reser

### fresh-same-morning-hard-data-macro-miss-china-ip-retail-plus.md
---
trigger_pattern: "Fresh same-morning hard-data macro miss (China IP/retail) plus an active geopolitical/oil risk-off escalation (Trump/Oman threat, Hormuz ceasefire expiry), a carried low-confidence Fed-easing repricing as the only strong positive, and clearly negative US index futures (NQ ≤ −1%, ES < −0.4%); no fresh index-relevant mega-cap earnings catalyst. The correct output is down/mild — full confirmation of the 08-17 fresh-hard-data-miss lesson."
corrected_behavior: "No change. Continue enforcing a-fresh-same-morning-hard-data-macro-miss at predict time: score B1 at −2 for the miss+geopolitical cluster, cap B3 at +0.5 when the repricing is carried and low-confidence, score B0/B6 from raw session/futures strength without importing regime confirmation, and emit DOWN capped at MILD

### fresh-same-morning-positive-catalyst-cluster-china-fiscal-su.md
---
trigger_pattern: "Fresh same-morning positive catalyst cluster (China fiscal support pledge, positive premarket earnings beat like BJ's) with confirming US index futures (ES/NQ net ≥ +0.5%) and falling EPU, while oil is elevated but not spiking and a scheduled two-sided event (Jackson Hole) remains pending."
corrected_behavior: "No change required. Continue gating the 08-20 fresh-negative-cluster lesson on its own trigger: without a fresh negative mega-cap/hard-data miss AND with futures confirming up (net ≥ +0.5%), fresh positives plus confirming futures justify up/mild. Do not let elevated-but-stable oil or two-sided event risk alone suppress a futures-confirmed positive call."
falsifier: "If this setup recurs (fresh positives + futures confirming + oil elevated-but-stable) and SPX c

### in-a-utilities-xlu-call-a-second-soft-inflation-print-has-al.md
---
trigger_pattern: "In a Utilities/XLU call, a second soft inflation print has already produced yield relief, but the broad tape is risk-on with growth/tech leading and a fresh data-center load-growth disappointment is present. Direction/magnitude can be up/mild and correct even when XLU underperforms SPY on a relative basis; the relative underperformance is not the graded target."
corrected_behavior: "When the tape is risk-on/tech-led and the bond-proxy bid is capped by same-day sector headwinds, treat S2/S4 as confirmation only for the absolute up/mild move. Do not imply or rely on relative outperformance; explicitly allow XLU to lag SPY. If future runs cite relative tape, label it as “absolute confirmation only.”"
falsifier: "If XLU in this same macro setup (soft CPI/PPI, risk-on tech

### industrials-xli-narrative-and-sector-scores-cap-down-mild-s0.md
---
trigger_pattern: "Industrials/XLI narrative and SECTOR_SCORES cap down/mild (Σ(S0..S4)×mult ≈ −1.8; two-sided scheduled policy event; no same-morning print; 1w/1m laggard after a non-holdings mega-cap AHR) but the deterministic pipeline emits down/flat from a more negative total_score and the scoreboard grades the pipeline band."
corrected_behavior: "Reconcile before emit: if narrative arithmetic and the 08-27 laggard-after-AHR rule say down/mild, emit down/mild — do not let pipeline −2.25 rewrite the band to flat. Do not write a new Industrials factor lesson. Do not import 08-28 leftover-S2/S4 down-bans from XLF/XLY/XLC; 08-27’s down:mild branch remains valid when the bounce gate is off. Keep S0/S1 at 0 until Chicago PMI / policy remarks print."
falsifier: "Narrative down/mild vs pipe

### long-duration-technology-semis-prediction-turns-up-on-a-stal.md
---
trigger_pattern: "Long-duration technology/semis prediction turns up on a stale positive mega-cap catalyst carried from prior context, while a fresh knowable-at-open inflation/geopolitical shock is present, real yields are rising, and the yield-equity correlation is strongly negative — with crowded long positioning making the sector asymmetrically vulnerable to risk-off."
corrected_behavior: "Before using any catalyst from prior context, verify it is fresh for the current session; archived catalysts cannot override a live macro shock. When real yields are elevated/rising and the 5-day 10Y-SPX correlation is strongly negative, treat the macro read as net negative for long-duration tech even if VIX is calm and credit spreads are tightening. When the most-crowded trade faces a fresh infla

### mega-cap-cyclical-etf-xly-amzn-tsla-hd-with-s0-0-mixed-flat.md
---
trigger_pattern: "Mega-cap cyclical ETF (XLY: AMZN/TSLA/HD) with S0=0 (mixed/flat ES/NQ, leftover impulse in a non-holding, two-sided policy event, no same-morning consumer print) and S1 only a stale/confirming consumer spine; the only negatives are yesterday’s completed relative fade copied into S2 (prior-session composition), S3 (trailing 5d outflows), and S4 (prior 1d/3d rel) and treated as independent confirmation for a down call."
corrected_behavior: "Do not triple-count a completed prior-session lag. With S0=0 and stale S1, set S2=0 unless a live premarket AMZN/TSLA/HD breakdown is confirmed; do not treat 5d outflows as a 1-day lid; do not re-vote yesterday’s 1d rel as a full S4 down. Prefer flat/mild. A ban on mapping XLK/NVDA into S0=+1 is not a license to extrapolate the secto

### mega-cap-earnings-over-macro-drag.md
---
trigger_pattern: "Strong positive mega-cap earnings/AI momentum coincides with negative macro/geopolitical headlines (oil spike, China PMI miss, hawkish Fed) — market follows earnings unless futures independently confirm weakness."
corrected_behavior: "When Channel 2 has an index-relevant positive mega-cap earnings catalyst and B6 futures are not negative: set B1 at least 0; cap B2/B3/B7 combined drag; FORBID predicted_direction=down unless futures or leading internals independently confirm weakness."
falsifier: "Wrong if mega-cap earnings green but SPX still falls that day while futures were non-negative at the open."
current_behavior: "Over-weight oil/China/Fed negatives vs Amazon/MSFT-type prints; call down."
evidence_cited: "2026-07-31 predicted down/mild; actual SPX +0.70%; Amazon

### mixed-catalyst-session-positive-geopolitical-de-escalation-h.md
---
trigger_pattern: "Mixed-catalyst session — positive geopolitical de-escalation headline versus a single-market Asia chip crash, with US futures flat and giving no confirmation in either direction; low-conviction flat-band call is the correct calibration."
corrected_behavior: "No change. When leading indicators are offsetting and futures give no confirmation, hold the flat band at low confidence; weight a positive headline only when Europe/US futures confirm it (08-03 rule), and weight an Asia crash only when futures independently turn negative (07-31 rule)."
falsifier: "If this exact pattern recurs (positive de-escalation headline + Asia chip crash + flat futures) and SPX moves >=1.0% in either direction, the flat-band default is wrong and must be recalibrated. Also, if a futures-confi

### no-corrective-trigger-full-hit-on-both-axes-under-an-activel.md
---
trigger_pattern: "No corrective trigger — full hit on both axes under an actively-fired mega-cap-earnings-over-macro-drag pattern (index-relevant AI earnings catalyst + non-negative futures + negative macro/geopolitical headlines)."
corrected_behavior: "No change. Continue enforcing the mega-cap-earnings-over-macro-drag forced-checklist at predict time; maintain magnitude humility on borderline flat calls (0.26% vs 0.30% band edge)."
falsifier: "The mega-cap rule is wrong if an index-relevant positive mega-cap earnings catalyst with non-negative futures is followed by a down SPX day — not observed today, so the lesson stands."
current_behavior: "Pipeline predicted up/flat; applied the standing mega-cap-earnings-over-macro-drag rule (B1=+1, capped B2/B3/B7 at −0.5 each), total score 2.2

### no-error-full-direction-magnitude-hit-no-corrective-trigger.md
---
trigger_pattern: "No error — full direction/magnitude hit; no corrective trigger pattern established"
corrected_behavior: "No correction required; continue treating a verified negative catalyst cluster as sufficient to override positive global-session/futures momentum at low confidence"
falsifier: "N/A for this hit; for candidate 07-31, a repeat positive-futures/positive-earnings setup that fades would falsify it"
current_behavior: "Predicted down/flat on negative AI-capex and geopolitical catalysts despite strong Asia and positive futures; output was directionally/magnitude accurate"
evidence_cited: "2026-08-05 — predicted DOWN/FLAT at -1.575; actual SPX -0.17%, direction HIT and magnitude HIT; no factor mismatch"
error_category: "NONE"
scope: "general"
date: "2026-08-05"
status: "act

### no-new-lesson-needed-confirmed-pattern-an-energy-xle-call-ha.md
---
trigger_pattern: "No new lesson needed. Confirmed pattern: an Energy/XLE call has a correct negative oil spine (crude down, EIA inventory build, IEA/OPEC demand destruction), but XLE has already run >4% 1w relative on the same geopolitical catalyst; the current-day 1d tape is flat and not confirming fresh leadership. The correct output is capped at mild — down/flat-to-mild — and the realized session is likely to be flat absolute with negative relative performance."
corrected_behavior: "No change required. If scoring absolute ETF return, “flat/mild” is an equally valid point estimate when the prior relative run is very large, because the prior run cushions absolute XLE even as oil falls. The reliable signal is relative underperformance, which was correctly captured."
falsifier: "This pa

### no-scored-error-the-repeatable-non-scoring-pattern-is-a-narr.md
---
trigger_pattern: "No scored error. The repeatable non-scoring pattern is a narrative/pipeline band mismatch: when the deterministic pipeline emits severe and none of the active temper lessons are triggered, the narrative should not restate the band as notable based on background macro commentary that is not showing up in the live sector tape."
corrected_behavior: "When the pipeline emits severe and the active temper triggers are absent (1d rel >0.5%, no oil/geopolitical risk-off tape, no metals/equities co-move), keep the narrative band aligned with the pipeline rather than downgrading on background macro commentary that the sector tape is not confirming."
falsifier: "A future XLB call with an equally strong positive factor stack, a deterministic severe output, and no live temper trigg

### none-no-corrective-trigger-established-correct-down-notable.md
---
trigger_pattern: "None — no corrective trigger established. Correct down/notable Utilities call was driven by knowable rising real/nominal yields plus sustained sector-level relative underperformance."
corrected_behavior: "No correction required. Preserve the same process: treat rates and real yields as one macro channel, confirm with sector ETF tape, avoid double-counting, and keep structural positives from overweighting a rate-driven defensive selloff."
falsifier: "A future Utilities call with the same high-yield, high-real-rate conditions and same pre-existing XLU relative weakness that fails to produce down/notable relative performance would weaken the rate-driven bond-proxy thesis. No lesson is being added, so no direct falsifier is required."
current_behavior: "Utilities predicte

### on-a-confirmed-risk-off-day-for-a-defensive-sector-healthcar.md
---
trigger_pattern: "On a confirmed risk-off day for a defensive sector (Healthcare/XLV), a strong relative tape (rel > +2%) and a high deterministic total are treated as evidence of a severe absolute move. This ignores the beta translation problem: with SPY down only modestly, XLV's low beta converts a large relative beat into a moderate absolute move. The official band is also left unreconciled when the narrative score implies notable but the pipeline emits severe."
corrected_behavior: "For Healthcare/XLV on a risk-off day, if SPY is down less than roughly -1% and there is no fresh sector-wide catalyst (broad biotech M&A/FDA cluster, major policy resolution, etc.), cap the official magnitude at notable even if the relative tape is +2% or stronger and the pipeline total suggests severe. 

### on-a-scheduled-release-day-a-sector-s-own-high-impact-hard-d.md
---
trigger_pattern: "On a scheduled release day, a sector’s own high-impact hard-data print is pending; consensus is below the prior reading and the forward-looking component is a spending lead. The model names the print “the sector’s own print” but then treats it as neutral event risk in S1, while giving full positive credit to a broad risk-on tape in S0. When the print resolves below consensus, the discretionary ETF underperforms even on a rising SPY."
corrected_behavior: "When a same-day sector-owned print is scheduled with consensus below prior and the relevant component is forward-looking (e.g., Conference Board Expectations), do not neutralize it as pure event risk. Score a conditional downside tilt — either S1 slightly negative or S0 capped/discounted — rather than emitting a full 

### on-a-scheduled-trading-day-a-sector-predict-block-contains-e.md
---
trigger_pattern: "On a scheduled trading day, a sector PREDICT block contains explicit predicted_direction and predicted_magnitude_band, but the scoreboard entry records predicted None/None and marks direction_hit/magnitude_hit False, producing a false miss and corrupting rolling accuracy."
corrected_behavior: "The scoreboard must populate predicted_direction and predicted_magnitude_band from the PREDICT block whenever that block exists and contains explicit values. Only if no PREDICT block or no explicit prediction exists should the row be marked None/unavailable. If the PREDICT block has explicit `up` / `mild` and actual is `up` / `mild`, the row must be scored as `direction_hit: True | magnitude_hit: True`. A validation check should flag any `None/None` row on a trading day that has

### ops-missing-predict-file.md
---
trigger_pattern: "Scheduled trading day where premarket YYYY-MM-DD_predict.md is absent or empty at open/grading time."
corrected_behavior: "OPS: verify predict.md exists with SCORES_BEGIN before 09:30 ET; retry if missing. At grading mark ops_fail=true and leave direction_hit/magnitude_hit null — never score as a market miss."
falsifier: "Wrong if a present predict.md is marked ops_fail, or a missing file is still counted direction_hit=false."
current_behavior: "Outcome grades null prediction as miss."
evidence_cited: "2026-08-02 and 2026-08-08 missing predict files."
error_category: "D"
scope: "ops"
date: "2026-08-08"
status: "active"
schema_ok: "true"
---

## RULE
OPS: verify predict.md exists with SCORES_BEGIN before 09:30 ET; retry if missing. At grading mark ops_fail=true and lea

### premarket-tape-shows-no-directional-confirmation-global-sess.md
---
trigger_pattern: "Premarket tape shows no directional confirmation: global sessions flat (±0.5%), US index futures flat (±0.5%), and overnight catalysts are moderate (|B1| around 1) with headline risk (stalled geopolitical deal, oil spike, looming CPI) but no panic selloff — S&P pausing near records after a prior rally; no index-relevant earnings catalyst active."
corrected_behavior: "When B6=0 (±0.5%), B0=0 (±0.5%), and no index-relevant earnings catalyst is active: cap |B1| at 0.5 and |B7| at 0.5 (a headline that does not move futures is not worth full weight, especially in a bad-news-good regime), and force the predicted magnitude band to FLAT — do not let a moderate-catalyst raw sum of ~-4.0 produce a mild call absent futures confirmation. Only allow a mild/severe band if a non-fla

### rule.md
---
trigger_pattern: ""
corrected_behavior: ""
falsifier: ""
current_behavior: ""
evidence_cited: ""
error_category: "NONE"
scope: "general"
date: "2026-08-25"
status: "active"
occurrences: "1"
promoted_on: "2026-08-27"
sources: "['2026-08-25_sector_financial_lesson.md']"
schema_ok: "true"
---

## RULE


## WHEN IT FIRES


## WRONG IF


## EVIDENCE


(learn_cycle promote)

### scheduled-same-session-fed-chair-fomc-keynote-is-the-unresol.md
---
trigger_pattern: "Scheduled same-session Fed Chair/FOMC keynote is the unresolved two-sided policy binary; latest index-relevant mega-cap print is already T+1/paid; B6 is flat within ±0.5%; B1 and B3 left at 0."
corrected_behavior: "No score change. Keep B1=0 and B3=0 until the speech prints; keep magnitude FLAT when B6 is unconfirmed; do not convert an unresolved Chair binary into a down call or a B3 pre-score. Mega-cap down-forbid remains a freshness gate (same-morning/untraded cash), not a T+1 direction lock — but it was not binding on today’s +0.75 total."
falsifier: "If this trigger recurs and a hawkish Chair resolution still prints SPX ≥ +0.3% (or a dovish resolution still prints ≤ −0.3%), treating today’s −0.25% as a missing down-gate would be wrong — it is flat-band noise."
cur

### scheduled-trading-day-opens-with-the-premarket-prediction-fi.md
---
trigger_pattern: "Scheduled trading day opens with the premarket prediction file (YYYY-MM-DD_predict.md) missing or empty at open/grading time — a pipeline failure upstream of reasoning, not a market condition."
corrected_behavior: "Deploy a pre-open watchdog: verify YYYY-MM-DD_predict.md exists and is non-empty before market open; if missing, retry generation, alert loudly, and mark the run 'no baseline — ungraded' instead of grading a default miss. At grading time, a missing baseline is always D-category pipeline error, never a reasoning miss. Consolidate with candidate 2026-08-08_lesson.md and promote; do not create a duplicate lesson."
falsifier: "If the watchdog is live and a scheduled trading day still reaches grading with no predict file and no loud alert, the deployment is brok

### scheduled-trading-day-where-the-premarket-prediction-file-yy.md
---
trigger_pattern: "Scheduled trading day where the premarket prediction file (YYYY-MM-DD_predict.md) is absent, empty, or unavailable to the grader at grading time — recurring pipeline failure upstream of reasoning; no baseline exists to grade."
corrected_behavior: "Deploy the hard pre-open gate before 09:30 ET: verify YYYY-MM-DD_predict.md exists, is non-empty, and contains the SCORES_BEGIN block at the canonical path the grader reads; retry generation once; alert loudly on the ops channel on failure. At grading, always mark ops_fail=True with direction_hit/magnitude_hit None — never a default market miss, never a hit/miss against a missing baseline. Consolidate the three standing ops lessons into one D-category rule with occurrences=7 (08-02, 08-08, 08-09, 08-15, 08-16, 08-22, 08-25) 

### scheduled-us-cash-session-where-premarket-yyyy-mm-dd-predict.md
---
trigger_pattern: "Scheduled US cash session where premarket YYYY-MM-DD_predict.md is absent, empty, or missing SCORES_BEGIN at open/grading — pipeline failure upstream of reasoning, not a market condition."
corrected_behavior: "OPS gate before 09:30 ET: verify predict.md exists, is non-empty, and contains SCORES_BEGIN; retry generation once; alert ops on failure; do not score B0–B7, direction, or magnitude from a null baseline. At grade time keep ops_fail=True and hits None. Consolidate duplicate D-ops actives; increment occurrences; write no reasoning lesson."
falsifier: "If the 09:30 ET watchdog is live and a scheduled session still grades a silent missing predict.md, or a valid predict.md is marked ops_fail / direction_hit=false, fix tooling rather than defend or add this lesson"
cu

### sector-has-just-recorded-a-strong-positive-relative-reversal.md
---
trigger_pattern: "Sector has just recorded a strong positive relative reversal (1d/3d/1w rel > 0) and the model invokes a follow-through up/mild default, but a fresh legal/regulatory catalyst hits a top holding at the open. Instead of treating that catalyst as the explicit exception to the follow-through lesson, the model treats it only as a magnitude cap, and it fails to scan for same-theme legal catalysts (Section 230, parallel state suits, antitrust) that can turn a single-stock event into a sector-wide liability repricing."
corrected_behavior: "When a fresh comparable top-holding catalyst is present, do not default up/mild. Treat it as the follow-through lesson's exception, score S1 negative, and search for compounding industry-wide legal/regulatory catalysts affecting the same the

### sector-prediction-made-when-the-sector-s-dominant-commodity.md
---
trigger_pattern: "Sector prediction made when the sector's dominant commodity spine (oil) is green premarket and the geopolitical supply-risk catalyst is actively escalating (fresh attacks, disrupted chokepoint, collapsed talks), while the broad market is flat/negative. The sector has already run hard over 1w, creating a temptation to cap S1 or downgrade magnitude."
corrected_behavior: "No correction. Continue applying the 08-14 active-escalation refinement: verify oil's live sign, distinguish active vs stale catalyst using same-morning headlines, and let S1 carry with S0 muted. The scoreboard magnitude hit is supported by the +1.55% relative beat even though the absolute +1.08% can read as mild in prose."
falsifier: "The 08-14 refinement would be falsified by a day with green CL/BZ fu

### two-name-duration-growth-book-xlc-like-meta-googl-spine-with.md
---
trigger_pattern: "Two-name duration/growth book (XLC-like: META+GOOGL spine) with S0=0 and S1=0 — mixed/non-transmitting futures, leftover impulse in a non-holding, no fresh ad/legal print — must not emit down just because yesterday’s large-cap failure sits in S2 and a large negative 1d relative print sits in S4. A ban on mapping foreign mega-cap beta into S0=+ is not a license to extrapolate the sector lag. Stale unit outflows are not same-day resistance. A two-sided policy speech is not an XLC-down spine; hawkish inflation can hit XLK/NVDA while ad/AI platforms mean-revert."
corrected_behavior: "Keep S0=0 when ES/NQ are mixed and the prior impulse is a non-holding (NVDA/XLK). Score yesterday’s two-name washout as history, not live breadth — S2=0 unless premarket dual-leaders are stil

### when-a-defensive-sector-prediction-is-directionally-negative.md
---
trigger_pattern: "When a defensive-sector prediction is directionally negative but the premarket/global tape shows no directional confirmation (flat US futures, flat global sessions) and the analysis text itself flags an offsetting/dampening signal (e.g., negative 10Y-SPX correlation, cooling risk appetite, nascent defensive bid), the deterministic score still emits a high-magnitude band solely from structural factor scores."
corrected_behavior: "When the analysis text identifies a dampening factor that reduces conviction, the final magnitude band should be capped at mild/flat unless the tape independently confirms notable movement. Structural negatives can still justify a negative direction, but a high negative total like -9.6 requires confirmation from futures/tape, not just S0–S4 fa

### when-a-geopolitical-supply-shock-headline-is-active-but-inte.md
---
trigger_pattern: "When a geopolitical supply-shock headline is active but internally conflicting — one source says a deal is agreed / strikes called off, another says it is stalled / demands unresolved — and the pre-fetched Channel 1 oil-futures tape shows a move consistent with the premature/deal-resolved headline, do not treat that pre-fetched tape as authoritative for an Energy call. The oil-price sign is the load-bearing factor for S1; if it is stale or wrong, it cascades into S1, the divergence check, the multiplier, and the final direction call."
corrected_behavior: "For Energy, before scoring S1 and final direction, verify the current oil-price sign against at least one independent live source. If the pre-fetched Channel 1 oil tape conflicts with active headlines or with indepen

### when-a-sector-call-is-verified-correct-predicted-up-flat-act.md
---
trigger_pattern: "When a sector call is verified correct — predicted up/flat, actual XLY +0.475% classified as flat, post-session review says both direction and magnitude HIT — but the individual scoreboard line records magnitude_hit False while the same scoreboard's rolling mag=0.5 (n=4) arithmetically requires the current run to be a hit, the False flag is a scoreboard/accounting data error, not a sector reasoning miss."
corrected_behavior: "Reconcile the scoreboard flag against the band classification and rolling accuracy before writing a lesson. If predicted flat and actual +0.475% is classified as flat by the outcome, and the post-session verdict says magnitude HIT, score it as a magnitude HIT and flag the individual False line as a data-entry/accounting error. Do not convert a ph

### when-a-sector-s-own-etf-tape-is-decisively-negative-across-a.md
---
trigger_pattern: "When a sector's own ETF tape is decisively negative across all timeframes (1d/3d/1w/1m relative all negative) AND there is a fresh knowable-at-open single-name negative (e.g., strike authorization on a top-weight name), a positive broad-market futures bounce (ES/NQ/Asia/Europe all up) does NOT automatically rescue the sector. The 08-21 reversal checklist (positive futures → avoid down call) must be weighed against the sector's own tape; when S4=-1 is decisive and S1 contains a fresh negative, the relative call should be down:mild, not flat."
corrected_behavior: "When S4=-1 (decisive negative tape across all timeframes) AND S1 contains a fresh knowable-at-open negative (not just carried/structural positives), the positive-futures bounce should be treated as a SPY-level

### when-a-technology-xlk-narrative-explicitly-applies-an-active.md
---
trigger_pattern: "When a Technology/XLK narrative explicitly applies an active risk-off reflect lesson and says “flat” or “flat/down,” but the deterministic pipeline still emits “up” because the signed component effects were lost in aggregation (e.g., leading_sum is computed from absolute magnitudes rather than the signed S0+S1+S2+S3 sum), the final graded prediction must be reconciled. Relative tape strength vs SPY must not be converted into absolute up direction; XLK can fall while outperforming SPY."
corrected_behavior: "When the narrative override and the deterministic output disagree, resolve the conflict before finalizing. Use signed component scores rather than absolute magnitudes when computing the leading sum and direction. If a fresh macro shock + crowded tech + stale catalys

### when-an-active-sector-lesson-says-a-live-two-sided-geopoliti.md
---
trigger_pattern: "When an active sector lesson says a live two-sided geopolitical/oil supply-shock headline caps magnitude, and the next prediction narrative concludes “oil flat / no overhang” based only on low CL/BZ percentage prints, verify the oil claim against Brent's absolute level and shipping-attack headlines before emitting up/severe. On scheduled CPI days, in-line CPI relief tends to rotate into growth/tech, not into oil-sensitive cyclicals; if the geopolitical oil shock is still active, Industrials can close flat/lag SPY even when premarket futures are risk-on."
corrected_behavior: "Before finalizing an Industrials severe call, check (1) Brent absolute level and overnight move, (2) shipping-attack/Hormuz headline status, and (3) whether the active 08-11 lesson trigger is stil

### when-an-industrials-xli-severe-up-call-is-built-on-strong-st.md
---
trigger_pattern: "When an Industrials/XLI severe-up call is built on strong structural sector factors (ISM expansion, AI-power/grid backlog, defense budgets) plus positive tape/flow confirmations, while a live two-sided geopolitical/oil supply-shock headline (e.g., US-Iran/Hormuz) is active, do not treat the pre-fetched oil direction as authoritative and do not score S0_SHARED_MACRO +1 merely by taking the constructive side (“peace-deal hopes, oil down”). A stale/misread oil print can flip the regime read from risk-on to risk-off at the open. On such days SPY may fall while XLI still rises modestly through defense/AI-power composition: direction can be right, but severe is not justified."
corrected_behavior: "When an Iran/Hormuz-style two-sided geopolitical headline is active, verify t

### when-an-oil-sensitive-cyclical-sector-industrials-is-being-s.md
---
trigger_pattern: "When an oil-sensitive cyclical sector (Industrials) is being scored while a prior-session geopolitical supply-shock headline (e.g., Hormuz, Brent near $90) is still in the news, but the pre-fetched oil tape is down and fresh demand-side catalysts are available (OPEC/IEA demand-forecast cuts, large inventory builds, official comments on normal flows), the model treats the headline as the current-session truth and discards the tape. The correct behavior is to check whether the demand-side catalyst has already flipped the day’s oil direction; if so, the headline is the stale leg and the sector bias should be down/flat-to-mild, not up/mild."
corrected_behavior: "Before discarding a pre-fetched oil direction under an active geopolitical headline, verify whether the oil mov

### when-the-pre-fetched-commodity-tape-conflicts-with-live-sour.md
---
trigger_pattern: "When the pre-fetched commodity tape conflicts with live sources, resolve with live verification. When the oil spine is up and the geopolitical supply-risk catalyst is actively escalating (ceasefire lapsed, chokepoint closed, oil at multi-year highs), treat it as the dominant S1 driver even if demand-side negatives exist. If the sector has already run hard over 1w and is overbought, cap magnitude at notable rather than severe, but do not downgrade the positive direction."
corrected_behavior: "No correction required. Maintain the pattern: live-verified oil up + actively escalating supply-risk catalyst → positive oil spine; cap only the magnitude band at notable when the run is extended, unless the tape supports severe."
falsifier: "This pattern would be falsified if liv

### xlb-has-negative-1d-3d-relative-tape-china-demand-pmi-drag-a.md
---
trigger_pattern: "XLB has negative 1d/3d relative tape, China demand/PMI drag, and copper is off its recent peak, but gold/silver are green, USD is weak, and the broad equity index is extended near record highs after a long run. The model treats the monetary-metals/commodity bid as a mere dampener, scores the industrial-demand drag multiple times, and emits a confident down/notable call by extrapolating the prior day’s underperformance."
corrected_behavior: "If gold/silver futures are green and USD is weakening, do not downgrade the monetary-metals bid to “dampener only.” Score the firm metals bid as a positive/neutral S1 offset, and do not build all five components negative from the prior day’s relative tape. When the broad index is at/near record highs and futures are flat or only mi

### xlb-writeup-already-diagnoses-an-8-25-composition-transmissi.md
---
trigger_pattern: "XLB writeup already diagnoses an 8/25 composition/transmission setup (NQ >> ES, XLB 1d rel <0.5%, mixed premarket breadth, two-sided S1) and hand-scores flat, but the emitted/graded call is still pipeline up because S4 is scored positive from a sub-0.5% tape and/or leading_sum/total_score disagrees with the written S0–S4 components. Gold HIT is allowed to keep S1 at +1 even when copper is off the record, LME stocks have rebuilt, and China/property remains HIT."
corrected_behavior: "If 8/25 conditions hold, emitted predicted_direction cannot be up — use the writeup/hand-sum (flat/flat-mild), not a conflicting pipeline up. Cap S4 at 0 when 1d rel <0.5% (1m rel already negative is not confirmation). Net S1 at 0 when gold HIT is offset by fading copper + inventory rebuild

## Per-scope DO-INSTEAD

### scope `general` — wins=8 losses=7
- **win 2026-08-21:** [general] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-08-23:** [general] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-08-28:** [general] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `news` — wins=1 losses=0
- **win news:** [news] Rank event families by 1d close, not ever-touch MFE.

### scope `sector_basic_materials` — wins=6 losses=5
- **win 2026-08-26:** [sector_basic_materials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-08-27:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-08-28:** [sector_basic_materials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_communication_services` — wins=3 losses=8
- **loss 2026-08-26:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-08-27:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-08-28:** [sector_communication_services] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_consumer_cyclical` — wins=7 losses=4
- **win 2026-08-26:** [sector_consumer_cyclical] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-08-27:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-08-28:** [sector_consumer_cyclical] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_consumer_defensive` — wins=5 losses=6
- **loss 2026-08-26:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-08-27:** [sector_consumer_defensive] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-08-28:** [sector_consumer_defensive] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_energy` — wins=6 losses=5
- **loss 2026-08-26:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-08-27:** [sector_energy] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-08-28:** [sector_energy] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_financial` — wins=4 losses=7
- **loss 2026-08-26:** [sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-08-27:** [sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-08-28:** [sector_financial] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_healthcare` — wins=7 losses=2
- **win 2026-08-18:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-08-21:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **win 2026-08-28:** [sector_healthcare] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_industrials` — wins=3 losses=8
- **loss 2026-08-26:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-08-27:** [sector_industrials] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-08-28:** [sector_industrials] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_real_estate` — wins=6 losses=5
- **loss 2026-08-26:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-08-27:** [sector_real_estate] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-08-28:** [sector_real_estate] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

### scope `sector_technology` — wins=5 losses=6
- **loss 2026-08-26:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-08-27:** [sector_technology] Keep direction; shrink confidence on modest |score| when magnitude historically misses.
- **loss 2026-08-28:** [sector_technology] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.

### scope `sector_utilities` — wins=5 losses=6
- **loss 2026-08-26:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **loss 2026-08-27:** [sector_utilities] When score sign conflicts with sector ETF tape / breadth, cut conviction; prefer flat/mild.
- **win 2026-08-28:** [sector_utilities] Keep direction; shrink confidence on modest |score| when magnitude historically misses.

## Open experiments

- **sector_utilities/win 2026-08-10:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/loss 2026-08-11:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/win 2026-08-12:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/win 2026-08-13:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/win 2026-08-14:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **sector_utilities/loss 2026-08-17:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-08-18:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-08-21:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-08-26:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/loss 2026-08-27:** [sector_utilities] Require one extra confirming source in the dominant bucket before full weight when score sign matches this fail pattern.
- **sector_utilities/win 2026-08-28:** [sector_utilities] On similar setups, test milder bands when |score|<4; log whether lagging tape factors overrode leading ones.
- **news/win news:** [news] Track event-level 1d close win rate daily in learn_cycle.

## Methodology checklist (MEMORY_CONFIRM)

1. Did any open experiment for THIS scope apply today?
2. Missing factor that would have flipped a recent loss?
3. Overweighting one bucket / double-counting one headline?
4. Sectors: S0 macro vs S1 sector factors — which failed?
5. News: event family still earning weight on 1d close?

## Retired / falsified

_(append when a falsifier triggers)_
