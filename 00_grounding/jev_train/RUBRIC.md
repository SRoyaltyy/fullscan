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
## Pending one change (session `20260929_1223`)

- question: `action_material`
- kind: `criteria`
- misses on that question: 10
- change: Rewrite `action_material` so a dated official print / hold / CR, named M&A, regulator exploring rules, major-AI product or IPO, court dismiss, pathogen-recall scare, or EV / tariff / Social Security policy is true — not only "cash flows to a US ticker this week." Sibling `new_instrument`: EIA figures, Fed holds rates, House CR through a date, CFTC explores rules, FDA panel endorse, named dollar deal, court dismiss are true. The one `decide()` change this round: do not drop `opinion` / `reaction` when `action_material >= 0.65` or `new_instrument >= 0.60`.
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

## Current QUESTIONS (after session 409)

### is_opinion

True only if the title is commentary / column / recap **and** has no
first-class fact. False if the title contains Fed / EIA / House /
CFTC / FDA action, a named IPO or M&A, pathogen + consumers/recalls,
a Fed officer + inflation or oil, peace accept/reject, a dated print
or hold, or a court dismiss of a listed name — even inside a listicle
or "what it means" frame.

Must stay true: "best move if the market crashes", celebrity, theme
park, gold-tumbles tape, earnings-look-right.

### is_tabloid

Unchanged. Sensational / celebrity / crime-blotter with no policy or
company action. Messi, theme park stay drop.

### is_reaction

Unchanged. Title only describes how stocks or traders already reacted.

### geo

- core: US / China / EU / Japan / Korea / India, G10 CB / regulator,
  G10 FX (yen + dollar + Fed), numbered oil-price print (Brent, WTI,
  Azeri Light, EIA storage, basin rigs).
- chokepoint: named strait / tanker / port.
- other: no US / G10 / oil-print hook and no named M&A.
  Yemen / Palestine / UK-local stay other.

### actor_power

- state_head: president / PM / monarch / minister / central banker,
  US House or Senate acting as a body.
- regulator: SEC FDA Fed FOMC CFTC NHTSA NBS ECB PBOC EIA, a named
  Fed governor, court with a binding order.
- listed_firm: named US-ticker (or plausible), named acquirer,
  major AI lab with a dated product or IPO.
- infrastructure: port / strait / exchange / grid / pipeline,
  G10 FX pair / oil benchmark.
- crowd: protesters / activists / tourists / unnamed residents.
  **Not** FX tape + Fed hike bets.
- other_person: private individual with no seat.

### action_material

True if the headline, if true, could change prices, policy, cash
flows, or the prior for a US-listed name **or** a core macro / oil /
FX factor this week.

Yes: weekly EIA / storage / rig count, Fed hold, House CR, named M&A,
regulator exploring rules, major-AI product or IPO, court dismiss of
a listed name, pathogen scare that can trigger FDA / recalls, EV /
tariff / Social Security policy, sitting-US-president arrest, final
CAFE.

No: protester arrest, local rally, celebrity, theme park, "best move
if crash", gold-tumbles tape, earnings-look-right.

### new_instrument

Yes: signed rule, print, halt, filing, seizure, dated official
decision, weekly official figure, rate hold / hike / cut, CR through
a date, regulator exploring or endorsing a rule, advisory-panel
endorse, named dollar deal, court dismiss.

No: speech, protest, rumor, "mulls" without a named instrument.

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
Threshold numbers stay. Code punct/source stay on the code path.

<!-- SESSIONS_BEGIN -->
