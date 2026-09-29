# Jev hop-0 rubric

Living criteria for how Jev analyses a title. Hop-0 filters trash and
holds anything Lane should classify. Jev never picks event_class,
polarity, or a ticker.

Each jev-train submit appends a session below and replaces the pending
one-change block. Closed lists (`jev_closed_lists.json`) are never
edited. At most one `decide()` / threshold change per session. Question
instructions and criteria update from the misses and `human_reason`
lines.

Thresholds stay put unless the pending block names one:
`TRASH_NOUL=0.70`, `MATERIAL_KEEP=0.65`, `INSTRUMENT_KEEP=0.60`,
`CROWD_DROP=0.50`.

<!-- PENDING_BEGIN -->
## Pending one change (session `20260929_1803`) — landed

- question: `action_material` + `is_reaction`
- kind: `criteria` + `decide`
- misses on that question: 7 material / 13 sheet
- change: Recaps and rumor-wants are not facts. Rewrite `action_material` / `is_opinion` / `is_reaction` / `new_instrument` from this session. Score toward KEEP when: sitting-president 401k / IRA investment-rule proposal; named-ticker results plus outlook raise; named product launch; named LNG / project FID; named-state pact; Iran hits / war-spreading. Do not loosen so these become keeps: 'reportedly wants' a buyer; law-firm tombstone of a foreign IPO; gold-fell-amid a rate-hike threat; 'how rate hikes impact' explainer; 'stock market today' recap of already-printed earnings; Fed-hike odds / 'sees rising odds' tape (409 DROP anchor). `decide()` this round: drop `reaction` unless `new_instrument >= 0.60`. Material-only `fact_keep` no longer skips reaction. Opinion skip stays. Thresholds stay. Do not edit closed lists.
- do not edit `jev_closed_lists.json`
<!-- PENDING_END -->

## How Jev is allowed to change

Jev analysis is the `QUESTIONS` pack plus `decide()`. It is not more
regex keep-lists. Code-keep is a safety net so a classifiable title
still reaches Lane; the long-term fix is Jev scoring those titles as
keep.

After every jev-train issue:

1. Map each false drop / false keep to one question (or `code:*`).
2. Append the session table (title, You, reason, `human_reason`).
3. Replace the pending block with **one** Jev-analysis change.
4. Land that change (criteria text, or one decide / threshold move).
5. New draw. Do not auto-patch closed lists.

`?` is not a miss. Blank `human_reason` on a disagreement is flagged
on the issue but still allowed.

## Current QUESTIONS (after session 1803)

### is_opinion

True only if the title is commentary / column / recap **and** has no
first-class fact. False if the title contains Fed / EIA / House /
CFTC / FDA action, a named IPO or M&A or IPO delay, pathogen +
consumers/recalls, a Fed officer sees/says/warns inflation or oil,
Fed chair / Jackson Hole remarks, jobs / rents as Fed-path context,
peace accept/reject, a dated print or hold, a Conference Board /
consumer-confidence print, court dismiss or listed-name litigation,
a housing or credit freeze that if true changes a listed sector
prior, EV / tariff / Social Security policy, sitting-president
foreign-policy action or a scheduled summit/dinner with a
counterpart, US/China national industrial policy (AI / chips /
capital markets), named-firm mass layoff / cost-cut / Chapter 11,
G7/UK national fiscal budget or tax rise, EU/G7 trade or
Buy-European procurement rules, G10 CB hawkish/dovish stance,
national exchange trading debut, outbreak + vaccination drive,
food-import supply-shock, tanker/newbuild cost inversion,
yield-driven gold crash, CDS affecting a listed sector,
sitting-president secondary sanctions, Hormuz / Iran escalation
or a named-strait status-change, named-fund / 13F / billionaire
flow into a listed ticker, named-officer insider sale,
named-broker downgrade plus price-target cut, a national
pump-price cut, sitting-president 401k / IRA investment-rule
proposal, named-ticker results plus outlook raise, named product
launch, named LNG / project FID, named-state pact / alliance, or
Iran hits / war-spreading strikes — even inside a listicle or
"what it means" frame.

Must stay true: "best move if the market crashes", celebrity, theme
park, gold-tumbles forecast tape, earnings-look-right, forecast /
odds / live tape that only name-drops PCE, IPO, or a rate-hike,
investor-rebuke or "could still gain" columns with no warn / hold /
print (Warsh rebuked; Treasury/Bitcoin leftover), leaders
"expected" an outcome with no accept/reject/deal, a Social Security
explainer with no tax/plan/reform, reclaim / IPO price of an
already-public name, an open-bell "5 things" listicle with no
named print/deal/policy, a "tech stocks today" OpenAI / AI recap
or "earnings provide next test" with no dated print, a
"Should You?" whale / billionaire column with no named 13F /
insider lot, "reportedly wants" a buyer with no signed deal, a
law-firm tombstone guiding a foreign IPO, "how rate hikes impact"
with no new hike, a "stock market today" recap of already-printed
earnings, gold-fell-amid a rate-hike threat (not a yield-driven
crash), and Fed-hike odds / "sees rising odds" tape that only
name-drops PCE.

### is_tabloid

Unchanged. Sensational / celebrity / crime-blotter with no policy or
company action. Messi, theme park stay drop.

### is_reaction

True if the title only describes how stocks or traders already
reacted, with no new underlying event.

Yes: "stock market today" recap of already-printed earnings,
"tech stocks gain on" IPO optimism / aftereffects, gold-fell-amid
a rate-hike threat, "how rate hikes impact" with no new hike.

No: "things to know before the open", a dated Fed hold / EIA / CR,
or a first-class print that has not happened yet. A yield-driven
gold crash (yields + gold plunge) is a fact, not this tape.

### geo

- core: US / China / EU / Japan / Korea / India, G10 CB / regulator,
  G10 FX (yen + dollar + Fed), numbered oil-price print (Brent, WTI,
  Azeri Light, EIA storage, basin rigs), G7/UK national fiscal
  budget or tax rise (not a local coin / council story),
  sitting-US-president action in Korea or on an India-Europe
  corridor, named steel / freight trade-flow disruption, Conference
  Board / consumer-confidence print, national exchange trading
  debut, EU/G7 public-procurement or Buy-European trade rules,
  yield-driven gold / Treasury move, tanker / newbuild vessel-cost
  inversion, food-import supply-shock, national fuel pump-price
  cut, sitting-president 401k / IRA investment-rule proposal, a
  US named product launch, named-state pact involving a US ally.
- chokepoint: named strait / tanker / port, including an
  escalation or other status-change there.
- other: no US / G10 / oil-print / G7-fiscal hook and no named M&A
  or named-firm layoff. Yemen / Palestine / UK-local stay other.
  Leaders "expected" an Iran/geo outcome with no accept/reject/deal
  stays other and is not a fact. A named-strait escalation is
  chokepoint, not other. Africa outbreak with no
  vaccination / pharma-contract hook stays other. A law-firm
  tombstone guiding a foreign IPO is not the debut. Gold-fell-amid
  a rate-hike threat is tape, not a yield-driven crash.

### actor_power

- state_head: president / PM / monarch / minister / central banker,
  US House or Senate acting as a body.
- regulator: SEC FDA Fed FOMC CFTC NHTSA NBS ECB PBOC EIA, a named
  Fed governor, Conference Board official print, Fed chair / named
  governor sees/says, court with a binding order.
- listed_firm: named US-ticker (or plausible), named acquirer,
  major AI lab with a dated product or IPO, named firm with an IPO
  delay, mass layoff, cost-cut / deleveraging, Chapter 11,
  authorized buyback, listed-name going to court, named-fund /
  13F / billionaire flow into that ticker, named-officer insider
  sale, named-broker downgrade plus price-target cut, named
  product launch, or named-ticker results plus outlook raise.
- infrastructure: port / strait / exchange / grid / pipeline,
  G10 FX pair / oil benchmark, named steel / freight trade-flow
  channel, national exchange debut, industry-association network
  launch, tanker / newbuild channel, national oil marketers
  executing a posted pump-price cut, named LNG / project FID.
- crowd: protesters / activists / tourists / unnamed residents.
  **Not** FX tape + Fed hike bets. **Not** a Conference Board /
  consumer-confidence print. **Not** an industry-wide association
  launching a network. **Not** national fuel marketers posting a
  pump-price cut.
- other_person: private individual with no seat.

### action_material

True if the headline, if true, could change prices, policy, cash
flows, or the prior for a US-listed name **or** a core macro / oil /
FX / steel-freight factor this week.

Yes: weekly EIA / storage / rig count, Fed hold, House CR,
Conference Board / consumer-confidence print, named M&A or IPO
delay, regulator exploring rules, major-AI product, court dismiss
or listed-name litigation, pathogen scare that can trigger FDA /
recalls or outbreak + vaccination drive, EV / tariff / Social
Security policy, "mulls" a named EV / tariff / Social Security
instrument, housing or credit freeze that if true changes a listed
sector prior, sitting-US-president arrest, foreign-policy action,
or scheduled summit/dinner, US/China national industrial policy,
named steel / freight trade-flow disruption, named-firm mass
layoff / cost-cut / Chapter 11 / authorized buyback, G7/UK
national fiscal budget or tax rise, EU/G7 trade or Buy-European
procurement rules, G10 CB hawkish/dovish, national exchange
trading debut, food-import supply-shock, tanker/newbuild cost
inversion, yield-driven gold crash, jobs / rents as Fed-path
context, Fed officer sees/says/warns, Fed chair / Jackson Hole,
final CAFE, sitting-president secondary sanctions, Hormuz / Iran
escalation, named-fund / 13F / billionaire flow, named-officer
insider sale, named-broker downgrade plus price-target cut,
national pump-price cut, sitting-president 401k / IRA
investment-rule proposal, named-ticker results plus outlook raise,
named product launch, named LNG / project FID, named-state pact /
alliance, Iran hits / war-spreading strikes.

No: protester arrest, local rally, celebrity, theme park, "best move
if crash", gold-tumbles forecast tape, earnings-look-right,
forecast / odds / live tape that only name-drops a print, leaders
"expected" an outcome with no accept/reject/deal, Social Security
explainer with no tax/plan/reform, reclaim / IPO price of an
already-public name, open-bell "5 things" listicle with no named
print/deal/policy, "tech stocks today" OpenAI recap or "earnings
provide next test", "Should You?" whale column with no named 13F
/ insider lot, "reportedly wants" a buyer with no signed deal,
law-firm tombstone guiding a foreign IPO, "how rate hikes impact"
with no new hike, "stock market today" recap of already-printed
earnings, gold-fell-amid a rate-hike threat.

### new_instrument

Yes: signed rule, print, halt, filing, seizure, dated official
decision, weekly official figure, rate hold / hike / cut, CR through
a date, regulator exploring or endorsing a rule, advisory-panel
endorse, named dollar deal, court dismiss, IPO delay, dated G7/UK
budget / tax rise, "mulls" a named policy instrument (EV, tariff,
Social Security, CAFE), Conference Board / consumer-confidence
print, Chapter 11 / bankruptcy filing, authorized dollar buyback,
national exchange trading debut, G10 CB hawkish/dovish, published
EU/G7 trade / procurement rule, official vaccination drive,
secondary sanctions, named 13F / fund stake or Form-4 insider
lot, named-broker rating plus price-target change, sitting-
president 401k / IRA investment-rule proposal, named-ticker
results plus outlook raise, named product launch, named LNG /
project FID, named-state pact / alliance.

No: speech, protest, rumor, "reportedly wants" a buyer, law-firm
tombstone, "mulls" with no named instrument, forecast / odds /
live tape, "sees rising odds" of a Fed hike, "how rate hikes
impact", "stock market today" recap, gold-fell-amid a threat,
leaders "expected" an outcome, IPO-price reclaim, Social Security
explainer.

### reprint_weather

True only if this is a months-old situation with **no** new closure,
ceasefire, first strike, escalation / de-escalation /
status-change, or new accept / reject / seize verb.
Trump rejects Iran peace / Hormuz is false (new verb).
"US-Iran Strait of Hormuz conflict escalation" is false
(status-change). "Tensions persist as tankers transit Hormuz"
stays true.

## Anchors from session 409 (do not regress)

Jev and human already agreed KEEP — leave these keep:

- FDA advisory panel endorses Grail blood test
- Brent climbs as US-Iran conflict fuels supply concerns
- Fed warns of new inflationary pressures and leaves the door open

Human DROP — leave these trash:

- Messi / theme park / footballer drowning / Ugandan king
- gold-tumbles tape, "best move if crash", earnings look-right
- Seeking Alpha / Motley Fool / tip-sheet / theme-park / personality-hire
- Warsh inflation column, Fed-hike odds, gold/platinum forecast + PCE
- HK IPO revival, retail-sales live tape, Treasury/Bitcoin leftover

## Session 409 → question map

| You | reason | question | why the old rubric missed | human_reason |
|---|---|---|---|---|
| KEEP | opinion | is_opinion | health scare scored as a column | could trigger recalls, FDA, policies |
| KEEP | opinion | is_opinion | listicle frame hid a named AI IPO | major upcoming IPO of a geopolitically significant AI company |
| KEEP | opinion | is_opinion | "warns" made a Fed officer a column | Fed officer warning — context |
| KEEP | opinion | is_opinion | same Cook + oil/inflation | Fed officer opinion — context |
| KEEP | opinion | is_opinion | bond/housing frame looked like a recap | if true, context for homebuilding stocks |
| KEEP | low_material | action_material | product launch scored below 0.65 | product launch from major AI company |
| KEEP | low_material | action_material | CFTC "explores rules" not cash-flow | gov department policy |
| KEEP | low_material | action_material | named $ deal not "this week cash flow" | potential merger/acquisition |
| KEEP | low_material | action_material | rig count not a ticker cash-flow | useful context for oil prices |
| KEEP | low_material | action_material | Fed hold had print bits; Jev material low | US Fed action |
| KEEP | low_material | action_material | House CR not a ticker rule | House action — affects policy |
| KEEP | low_material | action_material | UP–NS merger scored low | merger involving US company |
| KEEP | low_material | action_material | EIA print bits; Jev material low | oil and petrol information |
| KEEP | low_material | action_material | court dismiss scored low | major company charges dismissed |
| KEEP | low_material | action_material | "mulls" EV policy scored low | potential EV policy from Trump |
| KEEP | geo_other | geo | numbered oil print as other | useful oil price context |
| KEEP | geo_other | geo | named Nordic M&A as other | company acquisition/merger |
| KEEP | reprint_weather | reprint_weather | reject/peace treated as stale Hormuz | explicit accept/reject moves oil |
| KEEP | crowd | actor_power | yen + Fed hike bets as crowd | yen is a US-market factor |
| KEEP | punct | code:punct | `?` in a Social Security policy title | Trump-linked Social Security policy |
| KEEP | punct | code:punct | `Explainer-` + `?` on CDS | CDS now affecting the AI market |
| KEEP | source | code:source | simplywall deny hid US-China tariffs | US China tariffs |

False keep: 0. Do not loosen so the trash anchors become keeps.

`decide()` this round (one knob): if `action_material >= 0.65` or
`new_instrument >= 0.60`, do not drop for `opinion` or `reaction`.
Use Jev's own material/instrument, not a closed-list agency boost.
Threshold numbers stay. Code punct/source stay on the code path;
classifiable keep is a safety net, not the long-term fix.

## Session 1640 → question map

| You | reason | question | why the old rubric missed | human_reason |
|---|---|---|---|---|
| KEEP | low_material | action_material | US/China AI-chip industrial policy scored low | Potential national policy---worth keeping watch |
| KEEP | opinion | is_opinion | sitting-president Korea action read as a column | Korean gambit by Trump? Worth watching |
| KEEP | low_material | action_material | steel/freight trade-flow had no ticker cash-flow | Steel is a valuable commodity--disruption to trade flow worth watching |
| KEEP | low_material | action_material | IPO delay scored below 0.65 | IPO delay by company can affect stock |
| KEEP | opinion | is_opinion | jobs/rents + Fed path read as "could" tape | Job data and rent data--good context |
| KEEP | geo_other | geo | Canadian named-firm layoff as other | Company mass layoff action |
| KEEP | geo_other | geo | UK autumn Budget as UK-local | UK tax rise could affect markets since UK is part of the developed economy |
| KEEP | punct | code:punct | `?` hid Trump backing a trade corridor | any policy by Trump is worth watching |
| KEEP | source | code:source | thestreet deny hid Fed-hike context | Rate hike context---worth considering |
| KEEP | source | code:source | MarketBeat deny hid a named-firm stake | investments in another firm--could be bullish tailwind |

False keep: 0. Miss rate 10/98 = 10.2%. Do not loosen so 409 trash anchors become keeps.

`decide()` this round (one knob): if `action_material >= 0.65` or
`new_instrument >= 0.60`, do not drop for `geo_other`. Yemen /
UK-local with low material still drop.

## Session 1703 → question map

| You | reason | question | why the old rubric missed | human_reason |
|---|---|---|---|---|
| KEEP | low_material | action_material | listed-name court scored below 0.65 | Meta going to court poses potential legal problems for a large AI company. |
| KEEP | low_material | action_material | named-firm cost-cut / deleveraging scored low | Company action. Should keep |
| KEEP | crowd | actor_power | Conference Board print tagged crowd | Consumer confidence is a good indicator of market sentiment |
| KEEP | crowd | actor_power | industry-association launch tagged crowd | State Banking Associations(and any industry-wide union) taking a new initiative is HUGE news and should not be dropped |
| KEEP | geo_other | geo | Africa vaccination drive as other-trash | While Africa is normally outside the bounds of US market considerations, it is a huge testing ground for US phramaceutical companies. If US companies are contracted, could represent an opportunity |
| KEEP | low_material | action_material | scheduled Trump-Xi dinner scored low | Attendees of the Trump-Xi dinner indicate who is relevant in geopolitics. Should monitor for context |
| KEEP | low_material | action_material | India exchange debut scored low | National Stock Exchange of an emerging economy is huge--worth watching because India never had this sort of thing before |
| KEEP | opinion | is_opinion | BoE hawkishness inside an FX forecast | Any dovish/hawkish movement of central banks in relevant regions should be monitored |
| KEEP | low_material | action_material | Cook "sees" inflation scored low | Fed officer opinions--worth keeping |
| KEEP | low_material | action_material | yield-driven gold crash treated as tape | Gold crash is worth monitoring |
| KEEP | low_material | action_material | Fed chair / Jackson Hole scored low | Fed chair action--worth watching |
| KEEP | low_material | action_material | EU Buy-European rules scored low | EU trade policy--worth looking at |
| KEEP | low_material | action_material | Chapter 11 filing scored low | Corporate filing for bankruptcy--worth looking at |
| KEEP | geo_other | geo | food-import supply-shock as other | Food import shocks is worth monitoring |
| KEEP | geo_other | geo | tanker/newbuild cost inversion as other | Concerns shipbuilding stocks--worth looking at |
| DROP | other_powerful | geo | 1640 other_powerful fired on "expected" Iran | Their expectations is irrelevant and usually noise--only concrete actions/policy(like escalation, peace deals, disarmament is real) |

False keep: 3 (2 code-keep, 1 other_powerful). Miss rate 23/99 = 23.2%. Draw 1646 was scored before the 1640 land. Do not loosen so 409 trash anchors become keeps. Do not keep every open-bell "5 things" listicle.

`decide()` this round (one knob): if actor is `crowd` and
`action_material >= 0.65` or `new_instrument >= 0.60`, keep as
`crowd_fact`. Protesters with low material still drop. 1640 already
drops "expected" geo outcomes that lack fact_keep.

Code-keep tightened (not a new keep-list): `_CLASS_DEAL` no longer
matches "IPO price" / reclaim; `_CLASS_POLICY` needs a Social
Security tax/plan/reform verb.

## Session 1732 → question map

| You | reason | question | why the old rubric missed | human_reason |
|---|---|---|---|---|
| KEEP | opinion | is_opinion | named-fund stake in EQIX read as a column | investing in a specific firm. Worth looking at |
| KEEP | reprint_weather | reprint_weather | Hormuz escalation scored as months-old weather | Status change in major geopoplitical event |
| KEEP | crowd | actor_power | national pump-price cut tagged crowd | price price reduction could mean things for the economy |
| KEEP | low_material | action_material | billionaire 13F / ticker flow scored low | Following trades of billionaires is generally good practice |
| KEEP | low_material | action_material | named-officer insider sale scored low | Insider movements |
| KEEP | punct | code:punct | `?` hid sitting-president secondary sanctions | Secondary sanctions is a heavy hitter---see how it plays out |
| KEEP | junk_shape | code:junk_shape | named-broker downgrade + PT cut died as junk | Analyst downgrade of a firm |
| KEEP | source | code:source | Motley Fool deny hid an AI-cooling concept | Interesting AI concept |
| DROP | code_ai | code:code_ai | bare OpenAI name-drop kept a tech-stocks recap | not exactly earnings surprise despite the presence of the word "Earnings" |

False keep: 1 (code_ai). False drop: 8. Miss rate 9/100 = 9.0%. Draw 1720 was scored after the 1703 land. Do not loosen so 409 trash anchors become keeps. Do not keep every "Should You?" whale column.

`decide()` this round (one knob): if `action_material >= 0.65` or
`new_instrument >= 0.60`, do not drop a chokepoint reprint — keep
as `choke_fact`. Months-old "tensions persist" with low material
still drops.

Code-keep tightened (not a new keep-list): `_CLASS_AI` needs a
first-class OpenAI/Anthropic verb, not a "tech stocks today"
recap. `_CLASS_POLICY` matches secondary sanctions.
`_CLASS_CHOKE_NEW` matches escalation.

## Session 1803 → question map

| You | reason | question | why the old rubric missed | human_reason |
|---|---|---|---|---|
| KEEP | punct | code:punct | `?` hid a named-state pact | geopolitical development |
| KEEP | low_material | action_material | 401k / IRA investment-rule scored low | Again, new 401K+IRA policy from Trump admin--worth looking into |
| KEEP | punct | code:punct | `?` hid Iran hits / war-spreading | escalation in major war |
| KEEP | geo_other | geo | named product launch sat in other | new product launch |
| KEEP | opinion | is_opinion | Fed-hike odds / sees-rising-odds (409 DROP; blank reason FLAG) |  |
| KEEP | geo_other | geo | LNG After FID sat in other | LNG news |
| KEEP | low_material | action_material | results + outlook raise scored low | analyst outlook raise and strong results---confluence of positive factors in one stock |
| DROP | core_material | action_material | rumor-wants scored as a signed deal | Although this may look like corporate acquisition, theres too many reportedly, wants. This means it hasnt found a buyer yet |
| DROP | core_material | action_material | law-firm tombstone scored as the NSE debut | While the establishment of Indian National Stock Market is newsworthy, its individual stocks are not |
| DROP | core_material | action_material | gold-fell-amid scored as yield-driven crash |  |
| DROP | core_material | action_material | how-rate-hikes-impact scored as the hike | This is a description of how rate hikes affect something, not the action of rate hikes itself |
| DROP | code_ai | code:code_ai | Anthropic IPO optimism kept as the IPO | This is not Anthropic IPO. Just a description of the aftereffects of the news |
| DROP | core_material | action_material | stock-market-today recap scored as the print | This is "Stock Market today", which is merely a description of things already happened. However, stuff like "Things you should know before tomorrow's open" is acceptable since it hasn't happeend yet |

False keep: 6. False drop: 7. Miss rate 13/100 = 13.0%. Draw 1749 was scored after the 1732 land. Do not loosen so 409 trash anchors become keeps. Do not keep "reportedly wants", law-firm IPO tombstones, or Fed-hike odds / "sees rising odds" tape (409/1640 DROP). TD Securities KEEP has a blank `human_reason` FLAG — leave the opinion drop.

`decide()` this round (one knob): if `is_reaction >= 0.70` and
`new_instrument < 0.60`, drop as `reaction`. Material-only
`fact_keep` no longer skips reaction. Dated instrument (Fed hold /
EIA / CR) still keeps. Opinion skip stays (409).

Code-keep tightened / expanded (not a new keep-list): `_CLASS_AI`
no longer matches bare "ipo" after OpenAI/Anthropic — needs
introduce / launch / file / targets / warn / product.
`_CLASS_POLICY` matches 401k / IRA proposal, named-state pact,
After FID. `_CLASS_DEAL` matches outlook raise / strong results +
outlook. `_CLASS_CHOKE_NEW` matches hits / strikes / Iran / Iraq /
Jordan / war-spreading.

<!-- SESSIONS_BEGIN -->

## Session `20260929_1223`

Draw `20260929_1120`. Human keep 25, drop 75. Jev keep 3, drop 97. False keep 0, false drop 22.

| You | reason | question | human_reason | title |
|---|---|---|---|---|
| KEEP | opinion | is_opinion | Health scare--could trigger recalls, FDA, policies etc | Cyclospora fears lead consumers to lose their appetite for salads - CNBC |
| KEEP | opinion | is_opinion | Major upcoming IPO of a geopolitically significant AI company | Anthropic Targets $2 Trillion Record IPO: 8 Key Items Shaping the Stock Market Thursday - TheStreet Pro |
| KEEP | low_material | action_material | Product launch from major AI company---worth a watch | OpenAI Introduces ‘ChatGPT for Teens’ as Safety Concerns Grow - The New York Times |
| KEEP | low_material | action_material | CFTC part worth looking into--gov department policy | Bitcoin Rally Tops $79K. Crypto Shorts, ETF Flows Soar. CFTC Explores Crypto Rules. - Investor's Business Daily |
| KEEP | low_material | action_material | potential merger/acquisition | Vanguard pays $4.6B for RIA software startup Altruist - Axios |
| KEEP | low_material | action_material | Basin rig count steady--could serve as useful context for oil prices | Basin rig count steady as prices drop |
| KEEP | low_material | action_material | US Fed action | US Federal Reserve holds rates steady as inflation hawks call for hike - CNA |
| KEEP | low_material | action_material | House of representatives action--affects policy | House clears FY2027 CR through Dec. 11, shutdown risk off |
| KEEP | low_material | action_material | Merger involving US company | Canadian National Railway outlines conditions to U.S. regulators for proposed Union Pacific–Norfolk Southern merger |
| KEEP | low_material | action_material | Oil and petrol information | EIA weekly petroleum and natural gas storage — crude -0.4mb, gas +40 Bcf to 3,254 Bcf |
| KEEP | geo_other | geo | Useful oil price context | Azeri Light oil price decreases by 1.96% on world market - Report.az |
| KEEP | opinion | is_opinion | Fed officer warning--worth useing as context | Fed’s Lisa Cook Warns AI Won’t Save The Economy From Near-Term Inflation - TradingView |
| KEEP | reprint_weather | reprint_weather | Although Hormuz is background weather at this point, explicit acceptance/rejection of peace deals do move oil, market fear and volatility in the short term | Oil Surges Over 3% as Trump Rejects Iran Peace Proposal and Hormuz Risk Returns - EnergyNow.com |
| KEEP | opinion | is_opinion | Fed officer opinion--use as context | Fed's Cook Warns AI Demand and Oil Prices to Keep Inflation Elevated - IndexBox |
| KEEP | crowd | actor_power | Japanese Yen is a factor in US markets and should not be skipped | Tokyo yen trades in lower 157 range against dollar as U.S. rate hike bets fuel yen selling - finance.biggo.com |
| KEEP | source | code:source | US China Tariffs | 3 Export Stocks Linked To Lower US China Tariffs - simplywall.st |
| KEEP | punct | code:punct | Trump-linked government policy on Social Security--could massively affect consumer spending, sentiment, market mood etc | No tax on Social Security? The facts about Trump’s plan are here — and they could hurt US retirees the most |
| KEEP | geo_other | geo | Company acquisition/merger | Seafood groups Nordian Group, Norvelita sold to PE firm |
| KEEP | opinion | is_opinion | If true, could serve as context in homebuilding related stocks | The Bond Market Sell-Off Is Freezing American Homebuilding |
| KEEP | low_material | action_material | Major company charges dismissed--serves as bullish tailwind | 4th Circuit dismisses some charges against Wells Fargo after jury’s $22.1M fee |
| KEEP | punct | code:punct | Credit default swaps is a policy that is now affecting the AI market | Explainer-What are credit default swaps and why are they spooking AI investors? - Yahoo Finance |
| KEEP | low_material | action_material | Potential EV policy from Trump | As Trump mulls building Chinese EVs in U.S., automakers point to Germany as a cautionary tale - NBC News |

Proposed one change: `action_material` (criteria, n=10). Landed.

Code-path misses (not the Jev knob this round):

- `source` — 3 Export Stocks Linked To Lower US China Tariffs - simplywall.st (US China Tariffs)
- `punct` — No tax on Social Security? The facts about Trump’s plan are here — and they could hurt US retirees the most (Trump-linked Social Security policy)
- `punct` — Explainer-What are credit default swaps and why are they spooking AI investors? - Yahoo Finance (CDS affecting the AI market)

## Session `20260929_1640`

Draw `20260929_1627`. Human keep 33, drop 65. Jev keep 23, drop 77. False keep 0, false drop 10.

| You | reason | question | human_reason | title |
|---|---|---|---|---|
| KEEP | low_material | action_material | Potential national policy---worth keeping watch | China uses capital markets to fund AI and chip race against U.S. - qz.com |
| KEEP | opinion | is_opinion | Korean gambit by Trump? Worth watching | Trump’s Korean gambit exposes a shifting East Asian order |
| KEEP | low_material | action_material | Steel is a valuable commodity--disruption to trade flow worth watching | Geopolitical disruptions push up freight costs and alter steel trade flows - eurometal.net |
| KEEP | punct | code:punct | India Europe Trade corridor is worth watching as it may stimulate stock prices in multiple companies---in general, any policy by Trump is worth watching | India-Europe trade corridor gets Trump’s backing, but can it survive war? - South China Morning Post |
| KEEP | source | code:source | Rate hike context---worth considering | October Fed rate hike hinges on two looming economic reports - thestreet.com |
| KEEP | low_material | action_material | IPO delay by company can affect stock | Oura stock listing in limbo as smart ring startup delays IPO just days after launching it - Fast Company |
| KEEP | opinion | is_opinion | Job data and rent data--good context | Jobs and rents could shape the Fed's 2027 path - TradingView |
| KEEP | geo_other | geo | Company mass layoff action | Stelco layoffs put hundreds of Hamilton steel jobs at risk - CTV News |
| KEEP | source | code:source | investments in another firm--could be  bullish tailwind | Abacus FCF Advisors LLC Makes New Investment in Cardinal Health, Inc. $CAH |
| KEEP | geo_other | geo | UK tax rise could affect markets since UK is part of the developed economy | Burnham refuses to rule out tax rises in autumn Budget |

Proposed one change: `action_material` (criteria, n=3) + `geo_other` fact_keep. Landed.

Code-path misses (not the Jev knob this round):
- `punct` — India-Europe trade corridor gets Trump’s backing, but can it survive war? - South China Morning Post (India Europe Trade corridor is worth watching as it may stimulate stock prices in multiple companies---in general, any policy by Trump is worth watching)
- `source` — October Fed rate hike hinges on two looming economic reports - thestreet.com (Rate hike context---worth considering)
- `source` — Abacus FCF Advisors LLC Makes New Investment in Cardinal Health, Inc. $CAH (investments in another firm--could be  bullish tailwind)

## Session `20260929_1703`

Draw `20260929_1646`. Human keep 43, drop 56. Jev keep 26, drop 74. False keep 3, false drop 20.

| You | reason | question | human_reason | title |
|---|---|---|---|---|
| KEEP | low_material | action_material | Meta going to court poses potential legal problems for a large AI company. | Berkshire's Alphabet stake, Meta goes back to court, Europe's champagne problems and more in Morning Squawk - CNBC |
| KEEP | low_material | action_material | Company action. Should keep | LANXESS Targets Deeper Cuts, Deleveraging as Chemical Markets Stay Weak |
| KEEP | crowd | actor_power | Consumer confidence is a good indicator of market sentiment | US consumer confidence falls in August, Conference Board says - Reuters |
| KEEP | crowd | actor_power | State Banking Associations(and any industry-wide union) taking a new initiative is HUGE news and should not be dropped | U.S. State Banking Associations To Launch Blockchain Network |
| KEEP | punct | code:punct | Stock buybacks should be considered as good news | Almonty (ALM) Authorized a $300M Buyback Before Sangdong Fully Ramps. Is that the Best Use of Capital? |
| KEEP | geo_other | geo | While Africa is normally outside the bounds of US market considerations, it is a huge testing ground for US phramaceutical companies. If US companies are contracted, could represent an opportunity | Congo launches Ebola vaccination drive to tackle deadly outbreak |
| KEEP | low_material | action_material | Attendees of the Trump-Xi dinner indicate who is relevant in geopolitics. Should monitor for context | Here's who we know is going to the Trump-Xi dinner so far |
| KEEP | low_material | action_material | National Stock Exchange of an emerging economy is huge--worth watching because India never had this sort of thing before | National Stock Exchange of India Rises in Trading Debut - WSJ |
| KEEP | opinion | is_opinion | Any dovish/hawkish movement of central banks in relevant regions should be monitored | EUR/GBP Price Forecast: Fails at 0.8610 amid higher Oil prices, BoE hawkishness - tmgm.com |
| KEEP | low_material | action_material | Fed officer opinions--worth keeping | Fed’s Cook sees AI, oil prices adding to inflation - Finance & Commerce |
| KEEP | source | code:source | PCE Inflation Report is good context | Tomorrow's PCE Inflation Report May Boost Fed Hike Bets - Seeking Alpha |
| KEEP | opinion | is_opinion | From sources like Investopedia, "5 things to know before market opens" is good context actually and should be considered | 5 Things to Know Before the Stock Market Opens on Tuesday - investopedia.com |
| KEEP | source | code:source | Stock split action | Stock-Split Watch: Is Caterpillar Next? - The Motley Fool |
| KEEP | geo_other | geo | Food import shocks is worth monitoring | Food imports raise supply shock risk - The Express Tribune |
| KEEP | low_material | action_material | Gold crash is worth monitoring | Gold price crashes as 2007-era yields trigger brutal sell-off - tmgm.com |
| KEEP | low_material | action_material | Fed chair action--worth watching | Warsh settles some nerves at Jackson Hole |
| KEEP | junk_shape | code:junk_shape | Analyst upgrade of a certain company | Eaton shares rise after UBS upgrades the stock to Buy from Neutral and raises its price target to $515, citing strong sales growth and expected margin improvement. |
| KEEP | low_material | action_material | EU trade policy--worth looking at | EU prepares 'Buy European' public-procurement rules aimed at China |
| KEEP | low_material | action_material | Corporate filing for bankruptcy--worth looking at | Reports: Brightline preparing to file for Chapter 11 bankruptcy - WKMG |
| KEEP | geo_other | geo | Concerns shipbuilding stocks--worth looking at | Used oil supertanker prices surpass new vessel costs - finway.com.ua |
| DROP | code_deal | code:code_deal | While the words "IPO" is present and SpaceX is a large company, theres a difference between "SpaceX embarks on upcoming IPO" and "SpaceX regaining IPO price". The IPO has already happened and Jev should make that distinction | SpaceX Stock Looks To Reclaim IPO Price After Earnings, Share Unlock |
| DROP | code_policy | code:code_policy | While "Social Security" is mentioned, this makes no mention of any changes to Social Security. Likely just an explainer and should be discarded | What Every 65-Year-Old Should Know About Social Security |
| DROP | other_powerful | geo | Their expectations is irrelevant and usually noise--only concrete actions/policy(like escalation, peace deals, disarmament is real) | US and Israeli leaders expected ‘swift outcome’ in Iran |

Proposed one change: `action_material` (criteria, n=9) + `crowd` fact_keep. Landed.

Code-path misses (not the Jev knob this round):
- `punct` — Almonty (ALM) Authorized a $300M Buyback Before Sangdong Fully Ramps. Is that the Best Use of Capital? (Stock buybacks should be considered as good news)
- `source` — Tomorrow's PCE Inflation Report May Boost Fed Hike Bets - Seeking Alpha (PCE Inflation Report is good context)
- `source` — Stock-Split Watch: Is Caterpillar Next? - The Motley Fool (Stock split action)
- `junk_shape` — Eaton shares rise after UBS upgrades the stock to Buy from Neutral and raises its price target to $515, citing strong sales growth and expected margin improvement. (Analyst upgrade of a certain company)
- `code_deal` — SpaceX Stock Looks To Reclaim IPO Price After Earnings, Share Unlock (While the words "IPO" is present and SpaceX is a large company, theres a difference between "SpaceX embarks on upcoming IPO" and "SpaceX regaining IPO price". The IPO has already happened and Jev should make that distinction)
- `code_policy` — What Every 65-Year-Old Should Know About Social Security (While "Social Security" is mentioned, this makes no mention of any changes to Social Security. Likely just an explainer and should be discarded)

## Session `20260929_1732`

Draw `20260929_1720`. Human keep 36, drop 64. Jev keep 29, drop 71. False keep 1, false drop 8.

| You | reason | question | human_reason | title |
|---|---|---|---|---|
| KEEP | opinion | is_opinion | investing in a specific firm. Worth looking at | SGA Is Betting on Equinix (EQIX) as AI Demand Accelerates |
| KEEP | punct | code:punct | Secondary sanctions is a heavy hitter---see how it plays out | Trump threatens Iran’s partners: How do secondary sanctions work? |
| KEEP | reprint_weather | reprint_weather | Status change in major geopoplitical event | US-Iran Strait of Hormuz conflict escalation |
| KEEP | junk_shape | code:junk_shape | Analyst downgrade of a firm | HSBC downgraded Amgen to Hold from Buy and cut its price target to $425 in a new biopharma catalyst review published today. |
| KEEP | crowd | actor_power | price price reduction could mean things for the economy | FCT oil marketers begin pump price reduction by N20 - Realnews Magazine |
| KEEP | source | code:source | Interesting AI concept | This Growth Stock Is Quietly Solving the AI Supercycle's Cooling Problem. Here's Why It Could Soar. - The Motley Fool |
| KEEP | low_material | action_material | Following trades of billionaires is generally good practice | Billionaire David Tepper Trimmed Micron and Sold out of SanDisk. Here’s the New AI Stocks He’s Buying |
| KEEP | low_material | action_material | Insider movements | Primerica President Sells 1,800 Shares for $562,770 |
| DROP | code_ai | code:code_ai | not exactly earnings surprise despite the presence of the word "Earnings" | Tech stocks today: OpenAI's growth disappoints, Nvidia earnings provide next test for AI trade |

Proposed one change: `action_material` (criteria, n=2) + `reprint` fact_keep. Landed.

Code-path misses (not the Jev knob this round):
- `punct` — Trump threatens Iran’s partners: How do secondary sanctions work? (Secondary sanctions is a heavy hitter---see how it plays out)
- `junk_shape` — HSBC downgraded Amgen to Hold from Buy and cut its price target to $425 in a new biopharma catalyst review published today. (Analyst downgrade of a firm)
- `source` — This Growth Stock Is Quietly Solving the AI Supercycle's Cooling Problem. Here's Why It Could Soar. - The Motley Fool (Interesting AI concept)
- `code_ai` — Tech stocks today: OpenAI's growth disappoints, Nvidia earnings provide next test for AI trade (not exactly earnings surprise despite the presence of the word "Earnings")

## Session `20260929_1803`

Draw `20260929_1749`. Human keep 38, drop 62. Jev keep 37, drop 63. False keep 6, false drop 7.

| You | reason | question | human_reason | title |
|---|---|---|---|---|
| KEEP | punct | code:punct | geopolitical development | Saudi-Pakistan-Turkiye pact: A new shield or strategic signal? |
| KEEP | low_material | action_material | Again, new 401K+IRA policy from Trump admin--worth looking into | Proposal backed by Trump administration would allow 401K and IRA funds to be used in riskier investments - Yahoo |
| KEEP | punct | code:punct | escalation in major war | Iran hits US in Jordan, US-Saudi strikes on Iraq: Is war spreading? |
| KEEP | geo_other | geo | new product launch | The New Miller Copilot Builder With Blue iQ Brings Adaptive Automation to Real-World Fabrication Environments |
| KEEP | opinion | is_opinion |  | TD Securities sees rising odds of October Fed hike as PCE inflation stays firm - vtmarkets.com |
| KEEP | geo_other | geo | LNG news | LNG Canada Phase 2: After FID key questions remain - Institute for Energy Economics and Financial Analysis (IEEFA) |
| KEEP | low_material | action_material | analyst outlook raise and strong results---confluence of positive factors in one stock | TTM Technologies (TTMI) Shares Surge Following Strong Results and Outlook Raise |
| DROP | core_material | action_material | Although this may look like corporate acquisition, theres too many reportedly, wants. This means it hasnt found a buyer yet | Hugging Face Reportedly Wants to Be Acquired for About $13 Billion - Gizmodo |
| DROP | core_material | action_material | While the establishment of Indian National Stock Market is newsworthy, its individual stocks are not | CAM, Latham, Khaitan, Sidley Guide India's National Stock Exchange Through $2.4B IPO - Law.com |
| DROP | core_material | action_material |  | Gold fell to a seven-week low amid the threat of another rate hike in the United States - Українські Національні Новини (УНН) |
| DROP | core_material | action_material | This is a description of how rate hikes affect something, not the action of rate hikes itself | How Federal Reserve Interest Rate Hikes Impact Global Assets - Bitget |
| DROP | code_ai | code:code_ai | This is not Anthropic IPO. Just a description of the aftereffects of the news | Tech stocks gain on Anthropic IPO optimism, offsetting high oil, yields - Yahoo Finance |
| DROP | core_material | action_material | This is "Stock Market today", which is merely a description of things already happened. However, stuff like "Things you should know before tomorrow's open" is acceptable since it hasn't happeend yet | Stock Market Today: Nasdaq, S&P 500 Futures Surge After Blockbuster Nvidia Earnings Report; Salesforce, CrowdStrike Also Power Tech Gains - Yahoo Finance |

Proposed one change: `action_material` (criteria, n=7) + reaction instrument-gate. Landed.

Code-path misses (not the Jev knob this round):
- `punct` — Saudi-Pakistan-Turkiye pact: A new shield or strategic signal? (geopolitical development)
- `punct` — Iran hits US in Jordan, US-Saudi strikes on Iraq: Is war spreading? (escalation in major war)
- `code_ai` — Tech stocks gain on Anthropic IPO optimism, offsetting high oil, yields - Yahoo Finance (This is not Anthropic IPO. Just a description of the aftereffects of the news)
