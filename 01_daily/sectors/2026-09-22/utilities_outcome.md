# Sector Outcome — Utilities — 2026-09-22

Actuals: {'etf': 'XLU', 'pct': -0.3197271731507634, 'spy_pct': -0.015513266604716414, 'rel': -0.30421390654604696, 'open': 40.59000015258789, 'close': 40.529998779296875, 'source': 'yf_download'}

OUTCOME_BEGIN
SECTOR: Utilities
ETF: XLU
ETF_PCT: -0.3197
SPY_PCT: -0.0155
REL_PCT: -0.3042
ACTUAL_DIRECTION: down
ACTUAL_MAGNITUDE: mild
PRIMARY_DRIVER: Sticky ~5% long-end kept the bond-proxy bid dead; XLU drifted −0.32% vs a flat SPY with no fresh rates smash and no FTS.
KEY_INTERACTION: S4 lag confirmation vs S0–S3 zeros — rates counted once as carried, not re-HIT; residual was a mild down, not a manufactured smash.
KNOWABLE_AT_OPEN: partially
MORNING_READ_VERDICT: Down/mild HIT — zeros correctly refused a smash; S4 was the only signed input and it paid a small lag, not a notable.
OUTCOME_END

## 0. Facts

Trusted Channel 1 close (do not re-derive):

- **XLU** −0.320% (open 40.59 → close 40.53)
- **SPY** −0.016% (flat)
- **Rel** −0.304%
- Path: mild red open, two-sided tape (intraday high ~40.82 / low ~40.45), faded to a small down close. Volume ~18.2M vs ~20–21M recent average — no flow spike.

Direction **down**, magnitude **mild**. Not a Monday-style rotation day (09-21 was XLU −1.09% / SPY +1.55% / rel −2.65%).

## 1. What drove the sector

Taxonomy, in order:

1. **Rates / bond-proxy (S0/S1, carried not HIT).** 10Y finished **4.966%** (+0.4 bp vs prior; CountryEconomy **4.97% / 0.00** vs 09-21 4.96%). Still inside the ~5% stress zone, **not** a live smash and **not** relief. Classical utilities map is real/nominal yields; that map stayed a **headwind descriptor**, not a same-session impulse.
2. **Risk-on rotation away (S1, carried).** SPY was dead flat, not a ≥0.5% rip. Leftover Nasdaq/AMD from Monday did **not** fund another utilities dump. Relative lag continued (−30 bp) but shrank vs Monday’s −189 bp.
3. **FTS / risk-off (MISS).** VIX stayed mid-teens / no stress. This was not a flight-to-safety bid.
4. **Calendar leftovers (event risk, not a signed HIT).** Richmond Fed manufacturing composite **−2** (from +4); Barkin (“Why Hike”) left the door open to further hikes. Two-sided as the morning said: weak regional factory vs hawkish leftover. Neither printed an XLU-wide smash or duration bid.
5. **2Y auction (front-end, not S1 long-end).** $69B 2s stopped at **4.787%**, bid-to-cover **2.63**. Highest 2Y stop since 2024, but 09-10’s auction rule is 10Y/30Y. Did not reprice the long end.
6. **AI-power / nuclear / CapEx (stale / single-name).** Not an ETF 1d engine. CEG’s modest green is an IPP outlier, not XLU breadth.
7. **Ex-div.** Already printed 09-21 (~$0.30). Do not restack.

Net: a **no-catalyst grind** in a still-hostile yield regime. The close is the leftover lag, not a new shock.

**CLAIM:** 10Y yield +0.4 bp to 4.966% on 2026-09-22 (3 p.m. ET Tradeweb).
**URL:** https://www.morningstar.com/news/dow-jones/202609227245/10-year-treasury-yield-rises-to-4966-data-talk
**PUBLISHED:** 2026-09-22 15:54 ET
**QUOTE:** “The 10-year yield rose 0.004 percentage point to 4.966% today.”
**SUMMARY:** Tiny backup, fifth-highest yield this year, still 3.7 bp off the 09-16 5.003% high — sticky zone, not a smash.

**CLAIM:** CountryEconomy 10Y 4.97% on 09/22, unchanged vs 4.96% on 09/21.
**URL:** https://countryeconomy.com/bonds/usa
**PUBLISHED:** as-of 2026-09-22 (fetched 2026-09-22T21:29Z)
**QUOTE:** “09/22/2026 4.97% 0.00”
**SUMMARY:** Confirms the morning live-curve read (4.97% unchanged), not FRED 09-18 5.01 as “today.”

**CLAIM:** Richmond Fed manufacturing composite fell to −2 in September from +4 in August.
**URL:** https://www.richmondfed.org/region_communities/regional_data_analysis/business_surveys/manufacturing
**PUBLISHED:** 2026-09-22
**QUOTE:** composite −2 vs +4; shipments −5 vs +11; new orders −6 vs +3; employment +7 vs −2
**SUMMARY:** Regional factory contraction, first negative composite in months — two-sided vs Barkin’s firming/hawkish speech, not CPI-class.

**CLAIM:** Barkin justified last week’s hike and left the door open to more.
**URL:** https://www.richmondfed.org/press_room/speeches/thomas_i_barkin/2026/barkin_speech_20260922
**PUBLISHED:** 2026-09-22
**QUOTE:** (speech title/theme “Why Hike”; inflation risks outweigh employment; additional hikes “we’ll see”)
**SUMMARY:** Leftover FOMC messaging, not a new decision. Matches morning “Fed-speaker leftover → lower confidence, not a signed call.”

**CLAIM:** $69B 2-year note auction high yield 4.787%, BTC 2.63.
**URL:** https://www.treasurydirect.gov/instit/annceresult/press/preanre/2026/R_20260922_2.pdf
**PUBLISHED:** 2026-09-22
**QUOTE:** High yield 4.787%; bid-to-cover 2.63; offering $69B
**SUMMARY:** Solid front-end demand at a multi-year-high stop; not a 10Y/30Y supply HIT.

## 2. Audit morning S0–S4 vs reality

Use **morning numbers**, not post-close rewrites.

| Slot | AM | Reality | Verdict |
|---|---|---|---|
| **S0 = 0** | No live rip (ES/NQ vs-close −0.07%; Finviz NQ +0.41% under 0.5% gate). No live rates smash (ZN −0.03%, 10Y unchanged). Oil offering. Extra-confirm **caps** smash weight, does not force a down HIT. Do not HIT Barkin/2Y. Do not pay Monday −1.89% rel twice. | SPY −0.02%. 10Y +0.4 bp. VIX still ~14s. XLU −0.32% — mild, not a macro smash. Barkin hawkish + Richmond −2 cancelled as a signed utilities impulse. | **Correct zero.** |
| **S1 = 0** | No live spine HIT. Rates-rising **CARRIED**. Rotation-away **CARRIED** (printed Monday). Bond-proxy bid **MISS**. FTS **MISS**. AI-power/nuclear/CapEx stale or single-name (08-28, 08-12). | No XLU-wide news impulse. Captains NEE/SO/DUK modestly red in line with the ETF. CEG green is IPP, not the fund. | **Correct zero.** |
| **S2 = 0** | Diversified tape still red; regulated-electric breadth 0.098; IPP 0.0 SPLIT. 08-13: don’t pay lag in S2 **and** S4. | Captains slightly red; AEP/XEL modest green; CEG +0.5% vs VST −0.3% — mixed, not a breadth expansion or failure that should have been scored. | **Correct zero.** |
| **S3 = 0** | No confirmed same-day flow spike; 09-21 vol ~21M in-line; 1m rel −8.10% is not a crowded long. | ~18.2M shares (~90% of ~20.3M 65d avg). Dry-ish, not an outflow lid. | **Correct zero.** |
| **S4 = −1** | 09-14 **binds**: 1d/3d/1w/1m rel all < 0 **and** \|1d rel\| 1.89% ≥ ~1%. Confirmation only. 08-13: S2 stays 0. | Rel −0.30% on a flat SPY day — lag continued, much smaller. Confirmation paid **mild**, not notable. | **Signed slot that matched direction.** Magnitude of the confirmation was smaller than Monday’s tape implied. |

Self-audit tension: AM said **leading S0–S3 = 0 vs S4 = −1 → trust factors over tape → residual flat**. Pipeline still emitted **down / mild** (total −0.286) because S4 was allowed to sign the call. Reality sided with the **pipeline sign**, not the “residual flat” prose: a 32 bp down vs a 2 bp SPY is a real lag, just small.

08-25 held: S0=S1=0 → did **not** manufacture down from carried S2/S3 (both 0). 09-16 held: trailing 1w/1m lag did **not** pay a second notable down close. 09-21 extra-confirm held: no license to re-HIT Monday’s rotation. 09-11 held: did not apply both-branches S0=−1 to Richmond/Barkin.

## 3. Interactions / double-count / knowable-at-open

- **Same-shock, rates:** Sticky ~5% long end lived in **S0 as carried/not HIT** and was **not** re-HIT in S1. Close-of-day 10Y was unchanged/tiny backup — double-counting a smash that never printed would have been the error. Avoided.
- **Same-shock, rotation:** Monday’s −1.89% rel was S4 confirmation fuel (09-14), **not** a fresh S1 rotation HIT. Today’s −0.30% rel is the leftover, not a second paid thesis.
- **S4 vs factors:** Only signed slot. Factors said “nothing live.” Tape said “still lagging.” Both were true. The miss-risk was **overstating magnitude**, not direction. Milder-band / shrink-confidence experiment (|score| < 4, mag hit-rate 0.2) was the right posture; today the mag **HIT**.
- **Calendar:** 2Y + Barkin + Richmond were **knowable as event risk**, not as a signed utilities direction. Hawkish Barkin vs weak Richmond is the two-sided branch the AM wrote. Neither dominated the close.
- **Knowable at open:** **Partially.** Knowable: no CPI/FOMC, no long-end auction, PM:XLU −0.02%, 10Y unchanged, four-horizon lag, ex-div already paid, extra-confirm fails. Not knowable: whether S4 residual would print −30 bp vs flat, or fade to true flat; whether Barkin would be treated as a rates re-HIT (it wasn’t, correctly).

## 4. Outliers inside the sector

Do **not** let these drive the ETF read (08-28).

- **CEG ~+0.52%** vs XLU −0.32% — IPP/nuclear AI-power name, MAP HEAT SPLIT. Opposite of a utilities-wide bid.
- **VST ~−0.26%** — other IPP, not confirming CEG. Split inside high-beta generation.
- **NEE ~−0.46%, SO ~−0.36%, DUK ~−0.37%** — captains in line with XLU. Morning heat tagged NEE/SO pos; tape still not. Heat ≠ close.
- **D ~−0.62%** — slightly worse than the ETF (NEE/Dominion merger leftover is not an XLU 1d engine).
- **AEP ~+0.20%, XEL ~+0.29%** — modest regulated greens; not enough to flip breadth.
- Intraday **Benzinga snapshot XLU +0.24%** was a **path print**, not the close. Channel 1 close is −0.32%. Do not rewrite the session from a midday sector board.

## Scorecard vs prediction

- Predicted **down / mild** (score −0.286, conf ~0.51, mixed).
- Actual **down / mild** (XLU −0.32%, SPY flat, rel −0.30%).
- **Direction HIT. Magnitude HIT.**
- vs 09-21: that was dir HIT / mag MISS (predicted mild, got −1.09%). Today the “nothing live + lag confirmation” setup actually **was** a mild day. Rolling mag 0.2 needed this kind of session.

Lesson to keep: when S0–S3 are honestly zero and S4 is a 09-14 confirmation, **mild down vs a flat tape is the base case**, not “residual flat” and not “restack the lag into notable.” Extra-confirm correctly blocked a smash HIT. Do not treat today’s HIT as license to re-arm S0/S1 on sticky 5% yields tomorrow.

---

## RESEARCH APPENDIX

**Queries run**
- XLU utilities ETF September 22 2026 performance news
- US 10 year Treasury yield September 22 2026 utilities stocks
- Richmond Fed manufacturing Barkin September 22 2026 market reaction
- 2-year note auction results September 22 2026 Treasury
- NextEra Southern Duke Constellation Vistra stock September 22 2026
- X search: XLU utilities ETF vs SPY yields 10-year September 22 2026 what moved utilities stocks that day (from 2026-09-22 to 2026-09-23)
- leading lagging sectors September 22 2026 XLU XLP XLK XLE
- NEE SO DUK AEP D XEL CEG VST percent change September 22 2026
- SPY close September 22 2026 oil WTI VIX sector performance
- site:finance.yahoo.com NEE historical data September 22 2026
- XLU volume September 22 2026 close 40.53 MarketWatch
- Richmond Fed manufacturing index September 2026 composite -2 Barkin hike
- web_fetch: https://www.morningstar.com/news/dow-jones/202609227245/10-year-treasury-yield-rises-to-4966-data-talk
- web_fetch: https://countryeconomy.com/bonds/usa
- web_fetch attempts (blocked/failed): Benzinga sector movers, Reuters Barkin, Yahoo XLU history, MarketWatch XLU, Morningstar 2Y auction page, Yahoo Barkin, StockTitan heatmap

**Key sources (title + URL + timestamp / as-of)**
- Dow Jones / Morningstar Data Talk — https://www.morningstar.com/news/dow-jones/202609227245/10-year-treasury-yield-rises-to-4966-data-talk — 2026-09-22 15:54 ET — 10Y **+0.4 bp to 4.966%**; off 3.7 bp from 09-16 5.003% high.
- CountryEconomy US 10Y — https://countryeconomy.com/bonds/usa — fetched 2026-09-22T21:29:20Z — **4.97% on 09/22, 0.00 vs 4.96% on 09/21**.
- TreasuryDirect 2Y results — https://www.treasurydirect.gov/instit/annceresult/press/preanre/2026/R_20260922_2.pdf — 2026-09-22 — **$69B, high 4.787%, BTC 2.63**.
- Morningstar/Dow Jones 2Y auction wrap — https://www.morningstar.com/news/dow-jones/202609226708/investors-demand-highest-yield-at-two-year-us-treasury-auction-since-2024 — 2026-09-22 — highest 2Y stop since 2024; demand described as solid.
- Richmond Fed manufacturing — https://www.richmondfed.org/region_communities/regional_data_analysis/business_surveys/manufacturing — 2026-09-22 — composite **−2** vs +4.
- Barkin speech — https://www.richmondfed.org/press_room/speeches/thomas_i_barkin/2026/barkin_speech_20260922 — 2026-09-22 — “Why Hike,” door open to further hikes.
- MarketWatch XLU — https://www.marketwatch.com/investing/fund/xlu — as-of close 2026-09-22 — close **$40.53 (−0.32%)**, volume **~18.19M** vs ~20.31M 65d avg.
- Yahoo/search cluster for names — NEE **−0.46%** (79.63 → 79.26), SO −0.36%, DUK −0.37%, AEP +0.20%, D −0.62%, XEL +0.29%, CEG +0.52%, VST −0.26%.
- Benzinga sector movers (intraday, not close) — https://www.benzinga.com/etfs/sector-etfs/26/09/61919139/leading-and-lagging-sectors-september-22-2026 — XLU snapshot **+0.24%** midday; **do not use as the close**.
- X posts 2026-09-22 — https://x.com/kurtsaltrichter/status/2102356740603818023 — utilities failed resistance as 10Y rose; relative strength vs SPX falling. Color, not a price source.
- Channel 1 actuals (injected, trusted) — XLU **−0.3197%**, SPY **−0.0155%**, rel **−0.3042%**, open 40.59 / close 40.53.

**Facts taken**
- Close: XLU −0.32%, SPY flat, rel −0.30% (Channel 1).
- 10Y sticky at 4.966–4.97%, **not** a smash.
- 2Y auction solid at a high stop; front-end only.
- Richmond Fed −2 and hawkish Barkin were two-sided, not an XLU engine.
- Volume below average; no flow spike.
- Captains in line with ETF; CEG the IPP outlier to the upside.
- Intraday XLU green snapshots are path, not the session result.