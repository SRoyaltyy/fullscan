# Sector Outcome — Healthcare — 2026-10-13

Actuals: {'etf': 'XLV', 'pct': -1.354730947518512, 'spy_pct': -0.011562244330465266, 'rel': -1.3431687031880468, 'open': 172.32000732421875, 'close': 169.66000366210938, 'source': 'yf_download'}

Memory search is paused (index was built with a different embedding setup), so this autopsy uses the injected morning note plus live sources.

## 0. Facts

XLV **−1.355%** (open **172.32** → close **169.66**). SPY **−0.012%**. Relative **−1.343%**. Path: opened on the pre-open bid, then sold through the session (high ~172.79, low ~169.48) — a pop-and-drop, not a gap-and-go.

**CLAIM:** XLV closed −1.35% on 2026-10-13 while SPY was essentially unchanged.  
**URL:** https://stocknear.com/etf/XLV/history  
**PUBLISHED:** 2026-10-13 session  
**QUOTE:** Closing price $169.66 after opening at $172.32, range $169.48–$172.79.  
**SUMMARY:** Matches the deterministic actuals; healthcare was an idiosyncratic down day, not beta.

**CLAIM:** Energy/cyclicals led Tuesday; healthcare trailed on a near-flat tape.  
**URL:** https://www.fool.com/coverage/stock-market-today/2026/10/13/stock-market-today-oct-13-markets-edge-lower-as-earnings-season-kicks-off/  
**PUBLISHED:** 2026-10-13  
**QUOTE:** “energy led all sectors while industrials and healthcare trailed.”  
**SUMMARY:** The morning’s “defensive-favoring / Hormuz risk-off” regime did not show up in the cash session. SPY was flat; leadership was cyclical, not haven.

Direction **down**. Magnitude **notable** (≈1.35% absolute and relative vs a flat SPY; not a 2%+ crash).

---

## 1. What drove the sector

Primary driver was a **mega-cap earnings smash in JNJ**, a top XLV weight, with **MedTech sympathy** and a **leftover-RS unwind** in the rest of large-cap healthcare. UNH printed the same morning and *held* (beat-and-raise); it did not rescue the ETF.

**CLAIM:** JNJ beat adj. EPS/sales and nudged FY26 guidance, but MedTech missed a second straight quarter; shares fell >6%.  
**URL:** https://www.cnbc.com/2026/10/13/jnj-earnings-q3-2026.html  
**PUBLISHED:** 2026-10-13  
**QUOTE:** “Medical technology division sales rose 2.5% to $8.6 billion overall, missing analysts' estimates of $8.71 billion… shares fell more than 6%.” CFO Wolk: Middle East conflict, European austerity, and China pricing weighed “in a little bit more pronounced way.” MedTech chief Schmid: “Our performance is below the standard we have set internally.”  
**SUMMARY:** Signed negative was MedTech execution + international budget mix, not a pharma miss. Pharma (Innovative Medicine) was the good news (+7.7–7.9% op.).

**CLAIM:** GAAP optics and a modest raise were not enough for the multiple.  
**URL:** https://www.fool.com/investing/2026/10/13/why-johnson-and-johnson-stock-dropped-today/  
**PUBLISHED:** 2026-10-13  
**QUOTE:** “stock tumbled 6.8% through 12:30 p.m. ET Tuesday despite delivering an earnings beat this morning and raising guidance… GAAP… $0.09 per share… 96% year-over-year decline.”  
**SUMMARY:** Market sold the mix (MedTech miss, YoY adj. EPS −7.5%, talc/M&A charge) through a small FY raise.

**CLAIM:** Device peers sold in sympathy.  
**URL:** https://www.cnbc.com/2026/10/13/jnj-earnings-q3-2026.html  
**PUBLISHED:** 2026-10-13  
**QUOTE:** “Boston Scientific down nearly 3% and Intuitive Surgical shares off about 2%.” JPM’s Robbie Marcus: hard to generalize JNJ’s miss to the rest of medtech (China supply, ortho spin, greater ME exposure).  
**SUMMARY:** Enough sympathy to hit XLV’s device sleeve; not a clean sector-wide utilization break.

**CLAIM:** JNJ is a first-tier XLV weight (~9.7–10%).  
**URL:** https://www.ssga.com/us/en/intermediary/etfs/state-street-health-care-select-sector-spdr-etf-xlv  
**PUBLISHED:** holdings as of ~2026-10-13  
**QUOTE:** JNJ weight ~9.73% (fund) / ~9.74% (index).  
**SUMMARY:** A 6% JNJ smash is ~−0.6 pp of XLV by itself — enough to dominate a “mild up” call even before sympathy.

**CLAIM:** UNH beat, cut MCR, and hiked FY adj. EPS to ≥$20.50; stock did not offset JNJ.  
**URL:** https://www.cnbc.com/2026/10/13/unitedhealth-group-unh-earnings-q3-2026.html  
**PUBLISHED:** 2026-10-13  
**QUOTE:** “adjusted earnings of at least $20.50 per share, up from… $19.50 to $20… EPS: $4.69 adjusted vs. $4.14 expected… medical cost ratio… 88.4%… expecting 89.9%.”  
**SUMMARY:** Managed-care print was the *positive* nested outlier. XLV still closed −1.35% because JNJ + broad pharma/biotech outweighed UNH.

Taxonomy: **S1 earnings/utilization (MedTech miss)** first; **S4 leftover-RS mean-reversion** second; **S0 regime fail** (cyclicals > defensives on a flat SPY) third. Not drug-pricing, not CMS/MA, not a fresh IRA crackdown, not a Fed-minutes dump (minutes were Oct 7; CPI was the *next* day).

---

## 2. Audit of morning S0–S4 (use morning numbers, do not rewrite them)

Morning call: **up / mild**, score **2.601**, conf **0.45–0.60**, size_gate on. Components: S0 **+0.5**, S1 **+0.5**, S2 **+0.5**, S3 **0**, S4 **+1.0**, overlay **+2.5**, index_carry **−1.692**.

| Sleeve | Morning | Reality | Verdict |
|---|---|---|---|
| **S0 +0.5** | NQ-soft + Hormuz = defensive-favoring; HC the RS leader, though staples won PM | SPY flat; energy/cyclicals led; HC trailed | **Miss.** Relative-defensive regime did not trade. Index_carry (−1.69) was the honest macro sleeve; overlay/S0 overrode it. |
| **S1 +0.5** | CMS/MA stale +; IRA process not a crackdown; XBI “rate wall” partial; AMGN/ABBV/ARGX nested; **JNJ = watch item, unsigned** | JNJ MedTech miss was the session. UNH beat was the offset that didn’t clear the tape | **Miss on taxonomy, not on facts.** Morning *saw* the calendar and then 08-21’d it into nested. A ~10% weight reporting is spine, not nested. |
| **S2 +0.5** | MAP HEAT breadth across facilities/devices/MC | Down-tape was broad: LLY ~−1.4%, ABBV ~−2.4%, MRK ~−2.4%, XBI ~−3.0%, IBB ~−2%, BSX/ISRG red; UNH ~flat | **Miss.** Prior-day HEAT was stale into an earnings morning. |
| **S3 0.0** | No flow data; 1m extension → cap at 0 | Crowded-defensive RS did unwind | **Least-wrong sleeve.** The 0 was a ceiling, not a short. Should have been allowed to go negative given 3d/1w/1m rel +1.8/+2.3/+2.4 plus a mega-cap print. |
| **S4 +1.0** | 1d/3d/1w/1m all green; PM XLV +0.45% | Open held the PM bid (172.32) then trend-sold to 169.66 | **Classic continuation trap.** Green PM into a known print is not confirmation. |

**08-13 leftover-RS ban:** morning refused it because 1d rel was **+0.75% (not flat)**. The 3d/1w/1m RS *did* reverse. The 1d-must-be-flat gate was too strict on an earnings morning.

**08-21 nested-name ban:** correctly blocked AMGN/ABBV/ARGX/BDX/BSX/CI from dominating *pre-open*. Incorrectly applied the same logic to **JNJ**, which was the week’s lead HC print and a top holding.

**08-14 policy audit:** still correct — no IRA smash. Wrong war.

**10-02 / index_carry:** engine printed up anyway because overlay **+2.5** + S4 **+1.0** beat carry **−1.69**. The “don’t let carry mint a big up call” lesson fired on *magnitude* (mild + size_gate) but not on *direction*.

---

## 3. Interactions / double-count / knowable-at-open

**Double-count:** S0 “defensive regime” + S4 “all-horizon RS” + S1 “managed-care HEAT” were three labels on one leftover-leadership story. Overlay +2.5 then re-counted it a fourth time.

**Under-count:** JNJ/UNH calendar was in the prose (“watch item”, “JNJ lead”) but **not in the scores**. 08-21 + “unsigned binary” zeroed the only event that could flip the day.

**Knowable at open: partially.**
- Knowable: Tuesday is JNJ (and UNH) morning; 1m RS is extended; PM XLV only mid-pack; ES/NQ soft; size_gate already on.
- Not knowable: MedTech miss vs beat-and-raise, −6% JNJ, device sympathy, UNH holding while the rest of XLV sold.
- Path test: open **above** prior close, then trend-down — the smash was a **cash-session discovery**, not a pre-open gap that the morning note ignored.

Do not treat the JNJ print as if the morning “should have known the sign.” Treat it as: **unsigned mega-cap print + leftover RS should not have been a +2.6 up call.**

---

## 4. Outliers inside the sector

- **JNJ ~−6%:** primary drag; MedTech miss, second straight; not a pharma collapse.  
- **BSX ~−3%, ISRG ~−2%:** device sympathy; JPM said not fully generalizable.  
- **XBI ~−3.0%, IBB ~−2%:** duration/biotech sleeve worse than XLV — morning’s real-yield/XBI caution was the one S1 fragment that aged well, but it was scored only “partial.”  
- **ABBV ~−2.4%, MRK ~−2.4%:** large-cap pharma sold with the complex, not just JNJ.  
- **LLY ~−1.4%:** in line with the ETF (top weight, not the spark).  
- **UNH ~−0.2% on a beat-and-raise:** relative *winner*; morning’s managed-care HEAT was right on UNH and still lost the ETF. Proof that nested-positive MC cannot offset a 10% weight down 6%.

---

OUTCOME_BEGIN
SECTOR: Healthcare
ETF: XLV
ETF_PCT: -1.355
SPY_PCT: -0.012
REL_PCT: -1.343
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: notable
PRIMARY_DRIVER: JNJ Q3 MedTech miss (~6% smash in a ~10% XLV weight) plus device/pharma sympathy; leftover defensive RS unwound on a flat, cyclical-led SPY tape.
KEY_INTERACTION: S0/S4/overlay quadruple-counted leftover HC leadership while 08-21 nested-name logic stripped the known JNJ print out of S1; UNH’s beat-and-raise was the offset that did not clear.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Wrong way (up/mild vs down/notable): continuation tape beat a known mega-cap earnings binary; leftover-RS should have constrained direction even with 1d rel +0.75%.
OUTCOME_END

---

## RESEARCH APPENDIX

**Queries run**
- Web: `XLV healthcare ETF October 13 2026 why down`
- Web: `XLV stock October 13 2026 healthcare sector selloff`
- Web: `Johnson & Johnson Q3 2026 earnings October 13 guidance JNJ stock drop`
- Web: `Fed minutes October 13 2026 stock market healthcare`
- Web: `XLV holdings weight JNJ UNH LLY October 2026`
- Web: `UnitedHealth UNH earnings October 13 2026 outlook`
- Web: `XBI IBB ABBV LLY MRK UNH stock performance October 13 2026`
- Web: `site:ssga.com XLV holdings JNJ weight`
- Web: `"XLV" -1.35 OR "1.35%" October 13 2026 healthcare`
- X: `What happened to XLV healthcare stocks on October 13 2026? Why did healthcare ETF drop?` (2026-10-13 to 2026-10-14)
- X: `JNJ earnings MedTech miss XLV healthcare October 13 2026 stock reaction BSX ISRG` (2026-10-13 to 2026-10-14)
- Fetches: Fool market recap; Fool JNJ drop; CNBC JNJ; CNBC UNH; Fool JNJ transcript; JNJ IR (maintenance); Reuters/Finimize/Futunn (blocked or empty)

**Key sources and facts taken**

1. **Deterministic actuals (injected)** — XLV −1.3547%, SPY −0.0116%, rel −1.3432%, open 172.32 / close 169.66. Used as Channel 1 tape.
2. **stocknear XLV history** — https://stocknear.com/etf/XLV/history — close $169.66, open $172.32, range $169.48–$172.79, volume ~8.7M. Confirms path.
3. **Motley Fool, Stock Market Today Oct 13** — https://www.fool.com/coverage/stock-market-today/2026/10/13/stock-market-today-oct-13-markets-edge-lower-as-earnings-season-kicks-off/ — SPY ~−0.02%; energy led, healthcare trailed; JNJ ~−6% on MedTech miss. Fetched 2026-10-14T20:56Z.
4. **CNBC JNJ** — https://www.cnbc.com/2026/10/13/jnj-earnings-q3-2026.html — MedTech $8.6B vs $8.71B est., +2.5%; second miss; ME/Europe/China; JNJ −6%+; BSX ~−3%, ISRG ~−2%; pharma +7.7%; FY sales/EPS nudge. Fetched 2026-10-14T21:01Z.
5. **Motley Fool, Why JNJ dropped** — https://www.fool.com/investing/2026/10/13/why-johnson-and-johnson-stock-dropped-today/ — −6.8% by 12:30 p.m. ET; adj. EPS $2.59 vs $2.53; GAAP $0.09; talc/Firefly/Sail charge. Fetched 2026-10-14T21:01Z.
6. **Fool JNJ Q3 2026 transcript** — https://www.fool.com/earnings/call-transcripts/2026/10/13/jj-jnj-q3-2026-earnings-call-transcript/ — $25.4B sales, +6.0% op.; MedTech $8.6B +2.5% op.; IM $16.8B +7.9%; adj. EPS $2.59 −7.5% YoY; FY adj. EPS $11.05–$11.15. Fetched 2026-10-14T21:02Z.
7. **CNBC UNH** — https://www.cnbc.com/2026/10/13/unitedhealth-group-unh-earnings-q3-2026.html — adj. EPS $4.69 vs $4.14; rev $112.69B vs $111.40B; FY adj. EPS ≥$20.50; MCR 88.4% vs 89.9% est. Fetched 2026-10-14T21:02Z.
8. **SSGA XLV** — https://www.ssga.com/us/en/intermediary/etfs/state-street-health-care-select-sector-spdr-etf-xlv — JNJ ~9.73% weight.
9. **Name tape (search synthesis, 2026-10-13 closes)** — XBI ~−3.04%; IBB ~−2.1%; ABBV ~−2.40%; LLY ~−1.36%; MRK ~−2.39%; UNH ~−0.20%. Used only as sleeve color, not as Channel 1.
10. **X (2026-10-13)** — rotation-day / “defensives smoked” / JNJ as the drag; consistent with cyclicals-led, HC-lag narrative; not used as primary evidence.

**Not used / failed:** JNJ IR press-release page (maintenance); Reuters 401; Finimize empty extract; Futunn empty. Fed-minutes search confirmed **no minutes on Oct 13** (Sept FOMC minutes were Oct 7; CPI was Oct 14) — so S0 was not a same-day FOMC event.