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
## Pending one change (session `20260929_1640`) — landed

- question: `action_material`
- kind: `criteria` + one `decide()` knob
- misses on that question: 3 (plus opinion 2, geo 2)
- change: Rewrite `action_material` so US/China national industrial policy, named steel/freight trade-flow disruption, IPO delay, sitting-president foreign-policy action, named-firm mass layoff, G7/UK national fiscal budget or tax rise, and jobs/rents as Fed-path context are true. Sibling `is_opinion` false for those facts. Sibling `geo` core for G7/UK fiscal and steel/freight flows (UK-local stays other). Sibling `new_instrument` true for IPO delay and dated budget/tax rise. The one `decide()` change this round: if `action_material >= 0.65` or `new_instrument >= 0.60`, do not drop for `geo_other`. Threshold numbers stay. Code punct/source stay on the code path.
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

## Current QUESTIONS (after session 1640)

### is_opinion

True only if the title is commentary / column / recap **and** has no
first-class fact. False if the title contains Fed / EIA / House /
CFTC / FDA action, a named IPO or M&A or IPO delay, pathogen +
consumers/recalls, a Fed officer + inflation or oil, jobs / rents as
Fed-path context, peace accept/reject, a dated print or hold, a
court dismiss of a listed name, a housing or credit freeze that if
true changes a listed sector prior, EV / tariff / Social Security
policy, sitting-president foreign-policy action (gambit, backing,
corridor), US/China national industrial policy (AI / chips / capital
markets), named-firm mass layoff, G7/UK national fiscal budget or
tax rise, or CDS affecting a listed sector — even inside a listicle
or "what it means" frame.

Must stay true: "best move if the market crashes", celebrity, theme
park, gold-tumbles tape, earnings-look-right, forecast / odds / live
tape that only name-drops PCE, IPO, or a rate-hike, and
investor-rebuke or "could still gain" columns with no warn / hold /
print (Warsh rebuked; Treasury/Bitcoin leftover).

### is_tabloid

Unchanged. Sensational / celebrity / crime-blotter with no policy or
company action. Messi, theme park stay drop.

### is_reaction

Unchanged. Title only describes how stocks or traders already reacted.

### geo

- core: US / China / EU / Japan / Korea / India, G10 CB / regulator,
  G10 FX (yen + dollar + Fed), numbered oil-price print (Brent, WTI,
  Azeri Light, EIA storage, basin rigs), G7/UK national fiscal
  budget or tax rise (not a local coin / council story),
  sitting-US-president action in Korea or on an India-Europe
  corridor, named steel / freight trade-flow disruption.
- chokepoint: named strait / tanker / port.
- other: no US / G10 / oil-print / G7-fiscal hook and no named M&A
  or named-firm layoff. Yemen / Palestine / UK-local stay other.

### actor_power

- state_head: president / PM / monarch / minister / central banker,
  US House or Senate acting as a body.
- regulator: SEC FDA Fed FOMC CFTC NHTSA NBS ECB PBOC EIA, a named
  Fed governor, court with a binding order.
- listed_firm: named US-ticker (or plausible), named acquirer,
  major AI lab with a dated product or IPO, named firm with an IPO
  delay or mass layoff.
- infrastructure: port / strait / exchange / grid / pipeline,
  G10 FX pair / oil benchmark, named steel / freight trade-flow
  channel.
- crowd: protesters / activists / tourists / unnamed residents.
  **Not** FX tape + Fed hike bets.
- other_person: private individual with no seat.

### action_material

True if the headline, if true, could change prices, policy, cash
flows, or the prior for a US-listed name **or** a core macro / oil /
FX / steel-freight factor this week.

Yes: weekly EIA / storage / rig count, Fed hold, House CR, named M&A
or IPO delay, regulator exploring rules, major-AI product, court
dismiss of a listed name, pathogen scare that can trigger FDA /
recalls, EV / tariff / Social Security policy, "mulls" a named EV /
tariff / Social Security instrument, housing or credit freeze that
if true changes a listed sector prior, sitting-US-president arrest
or foreign-policy action, US/China national industrial policy,
named steel / freight trade-flow disruption, named-firm mass
layoff, G7/UK national fiscal budget or tax rise, jobs / rents as
Fed-path context, final CAFE.

No: protester arrest, local rally, celebrity, theme park, "best move
if crash", gold-tumbles tape, earnings-look-right, forecast / odds /
live tape that only name-drops a print.

### new_instrument

Yes: signed rule, print, halt, filing, seizure, dated official
decision, weekly official figure, rate hold / hike / cut, CR through
a date, regulator exploring or endorsing a rule, advisory-panel
endorse, named dollar deal, court dismiss, IPO delay, dated G7/UK
budget / tax rise, "mulls" a named policy instrument (EV, tariff,
Social Security, CAFE).

No: speech, protest, rumor, "mulls" with no named instrument,
forecast / odds / live tape.

### reprint_weather

True only if this is a months-old situation with **no** new closure,
ceasefire, first strike, or new accept / reject / seize verb.
Trump rejects Iran peace / Hormuz is false (new verb).

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
