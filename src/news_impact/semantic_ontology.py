"""Versioned headline semantics, not benchmark-specific recognition rules.

The model makes every semantic decision. These definitions constrain meanings
of the existing enum; they do not add classes or silently rewrite predictions.
"""
from .schema import EVENT_CLASSES

ONTOLOGY_VERSION = 'headline-ontology-v4'
DEFINITIONS = {
    'capacity': 'Physical production/service capacity added, expanded, closed or destroyed; not share issuance or market capitalization.',
    'demand': 'Orders, bookings, consumption, sales volumes or customer demand change; not merely a share-price move.',
    'input_cost': 'Operating input, fuel, freight or procurement cost change; ordinary commercial price/surcharge increases belong here when cost transmission is the stated mechanism, not price_cap.',
    'inventory_print': 'New measured producer/commodity inventories or stockpiles; not a company stock price.',
    'channel_stock': 'Distributor/dealer/channel inventory, stocking or destocking.',
    'gate': 'Product-specific permission to sell/build/operate: FDA approval, licence, permit, certification or rejection. Clinical trial results alone are trial_readout. Broad trade prohibitions are access_control.',
    'trial_readout': 'Clinical study efficacy/safety/endpoints or interim/final trial results; not regulatory approval.',
    'access_control': 'Operative sanctions, import/export controls, embargoes, transaction restrictions or broad market-access bans. Not an FDA approval, meeting, proposed bill or generic court dispute.',
    'sanction_lift': 'Reported removal/easing of sanctions or broad trade/transaction restrictions; not a request or hope that they will be lifted.',
    'standard_mandate': 'Mandatory technical, safety, environmental or compliance standard affecting how a business must operate.',
    'ip_ruling': 'Patent, copyright, trademark or other intellectual-property decision/judgment; supersedes generic blast_legal for explicit IP rulings.',
    'market_structure': 'Rules changing venue/intermediation, competition arrangements or who may collect market rents. Not a catch-all for every law; use the narrower mechanism when available.',
    'tax_fiscal': 'Tax rates, levies, duties/tariffs, budget or fiscal spending decisions. Export/import prohibition is access_control; targeted financial support is subsidy.',
    'price_cap': 'Regulated/legal ceiling, floor or administered rate on what may be charged, including a bill advancing such a ceiling. Market prices reaching a high are never price_cap.',
    'subsidy': 'Government grant, incentive, subsidy, rebate or targeted financial support. Historical totals alone are regime_state, not a newly awarded subsidy.',
    'breakup_remedy': 'Antitrust divestiture, forced breakup or structural remedy; not a voluntary spinoff or mere investigation.',
    'print_vs_priced': 'New reported financial/economic results, earnings, revenue, balance sheet, GDP, payroll or inflation data. Do not invent a consensus surprise; the class can identify a results print without a supplied expectation.',
    'guidance': 'Issuer formally raises/cuts/reaffirms forward financial or operating guidance; not analyst opinion.',
    'preannounce': 'Issuer preliminary earnings/results or profit warning before scheduled full results.',
    'peer_spill': 'A named peer results/event read-through explicitly driving another firm; do not infer unnamed peers.',
    'factor_impulse': 'Concrete monetary-policy decision, central-bank operational rate/fixing/purchase or other new broad macro-factor impulse. Gold purchases by a central bank are not geological reserve_revision.',
    'corporate_action_mna': 'Corporate acquisition, merger, takeover, asset/business sale or completion/termination of such a deal. Explicit regulatory clearance/review is deal_review.',
    'corporate_action_spinoff': 'Voluntary corporate separation, demerger or spinoff.',
    'dilution': 'Equity/share issuance, private placement, rights offering, warrants, convertible equity financing or stock-funded issuance. Unspecified private placement is equity financing unless debt is explicit.',
    'capital_return': 'Dividend, share repurchase/buyback or distribution of capital to shareholders.',
    'listing_flow': 'IPO, listing, delisting, listing transfer or exchange admission; not ordinary exchange-hosted news.',
    'lockup_expiry': 'Expiry/release of contractual insider/investor share-selling restrictions.',
    'credit_funding': 'Debt, loan, bond, credit facility, refinancing or lending arrangement. Equity/private placement alone is dilution.',
    'distress_restruct': 'Default, insolvency, bankruptcy, distressed debt restructuring or rescue recapitalization.',
    'integrity': 'Accounting fraud, restatement, audit failure or disclosure-integrity breakdown. Mere price weakness is not integrity.',
    'key_person': 'Corporate executive/board appointment, departure, death or succession. Public-office personnel are policy_personnel.',
    'insider_flow': 'Reported insider purchase/sale of issuer shares; not general investor opinion.',
    'blast_legal': 'Lawsuit, liability judgment, fine, settlement or injunction causing legal harm. IP rulings, regulatory probes and merger review use their narrower classes.',
    'blast_ops': 'Concrete operational failure, accident, disruption or outage; planned capacity changes are capacity.',
    'blast_cyber': 'Cyberattack, breach, ransomware or cyber-caused operational disruption.',
    'product_harm': 'Actual unsafe/defective product, adverse harm report, recall or withdrawal. A recall how-to/playbook is discard; clinical efficacy data are trial_readout.',
    'labor_stop': 'Strike, walkout, lockout or actual work stoppage.',
    'cat_weather': 'Concrete natural disaster/severe weather with an operational/economic channel; a financial market storm metaphor is not weather.',
    'flow_index': 'Index addition/removal/rebalance with mechanical index-linked buying/selling.',
    'flow_mechanical': 'Explicit non-discretionary portfolio/vehicle flow or technical share adjustment; not ordinary price action.',
    'flow_forced_liq': 'Margin-triggered liquidation, compulsory asset selling or redemption-driven forced sale.',
    'regime_state': 'Market prices, yields, trends, historical summaries, ongoing conditions or forecasts with no newly reported operative event. A price reaction with a concrete embedded event retains that event.',
    'regime_break': 'Verified broad physical/legal regime change with no more specific enum, such as an implemented ceasefire reopening a critical route. Not general geopolitical commentary.',
    'statement_public': 'Public comment, official clarification, advocacy, request, warning, proposed policy or scheduled public engagement without an operative economic mechanism. Proposals are not enacted restrictions.',
    'rumor': 'Unconfirmed reports, speculation or attributed rumours of an event; not a documented proposal or confirmed decision.',
    'fx_translation': 'Explicit currency translation effect on company reported revenue/earnings, not generic FX market moves.',
    'regulatory_probe': 'Regulator opens/conducts an investigation, inquiry or enforcement probe; not a final judgment or product approval.',
    'activist_campaign': 'Activist shareholder demand/campaign, proxy fight or pressure for corporate change.',
    'policy_personnel': 'Government, regulatory, judicial or central-bank official appointment, resignation, removal or succession.',
    'sovereign_credit': 'Sovereign credit rating, default, debt restructuring or creditworthiness decision; not ordinary bond yield changes.',
    'deal_review': 'Antitrust/national-security regulator or court review, approval, block or clearance of a specific acquisition/merger.',
    'labor_organize': 'Union formation, recognition, organizing election or collective-bargaining development without a work stoppage.',
    'strategic_review': 'Issuer formally explores strategic alternatives/business sale/restructuring; not a completed deal or general analyst suggestion.',
    'reserve_revision': 'Geological mineral/oil/gas resource or economically recoverable reserve estimate revision. Never central-bank gold holdings, cash reserves or share placements.',
    'discard': 'Non-economic noise, generic listicle/how-to, stock tip, analyst target/valuation opinion or routine IR scheduling without a concrete embedded event.',
}
assert set(DEFINITIONS) == set(EVENT_CLASSES), 'Every locked class needs exactly one definition'

DECISION_RULES = '''DECISION ORDER (reason internally, output only the requested JSON):
1. Parse independent clauses. Identify the actor, its verb, object and temporal/modal scope before classifying.
2. Reported means the action/decision/procedural step actually occurred. A reported decision effective next year is reported, not future. Announcing an intention is proposal/future, not implementation. Committee clearance is a completed procedural step, not enactment. Explicit retrospective aggregates/history are retrospective; do not infer stale reporting merely from an analyst-style headline when it says fresh/new approval.
3. Public authority is government/court/regulator/central bank/public international institution, including an official acting in office. Completed public electoral procedures are civic_electorate acts. An omitted institution name does not erase a type established by an explicit judicial/regulatory instrument. Unknown approving parties stay unknown: merger approval alone may be shareholder rather than regulatory approval. Do not treat a quoted official as the actor of another person's verb.
4. Concrete public actions include reported public decisions (including rejecting a measure), completed official engagements/procedures/operations, personnel acts, formal official clarification and operational monetary acts. Scope the stage to the action itself; rejecting a future measure is a reported rejection, not a negated rejection. Mere speech/opinion, advocacy, forecasts, economic statistics, historical totals and scheduled events are not completed decisions. New operational activity extending a sequence is not just a historical total. A title without supported public actor/action returns actions=[] or private actions, not an invented public actor.
5. Use the most specific class for the principal concrete economic event, even if the title also reports a stock reaction. Do not collapse proposals into completed policy: proposals/threats/requests usually primary statement_public; retain proposed mechanism in additional events with proposal stage. An actually completed committee step can retain the bill's specific mechanism. Multiple actual events may be listed; choose the headline's main predicate for primary.
6. Sector means directly affected business activity, not every conceivable downstream winner/loser. Classify the named subsidiary/activity, not mechanically the parent's dominant sector. Ignore publisher names. No inferred tickers, cross-sector spillovers or homonym identities. Unknown company business must be disclosed, not guessed. Pure macro baskets, government administration, commodity prices and central-bank reserves alone do not require a stock-sector tag.
7. Sector conventions: mining/metals/chemicals=Basic Materials; oil/gas producers/refining/oilfield services=Energy; power/water/gas utility operation=Utilities; banks/insurance/credit/asset management=Financial; drug/device/clinical care=Healthcare; chips/software/cloud/IT services=Technology; telecom/media/social/search/advertising/gaming content/live entertainment=Communication Services; airlines/transport/freight/machinery/defence/construction services=Industrials; autos/retail/restaurants/hotels/leisure goods=Consumer Cyclical; food/beverage/tobacco/staples=Consumer Defensive; property/REIT/development/real-estate operation=Real Estate. Chip fabrication is Technology, not Industrials. Power generation is Utilities, not Energy. Airline fuel costs do not automatically add Energy. Apply the activity actually targeted by the event when a diversified firm is named.
8. Before submitting, test competing interpretations: price level vs legal cap; equity vs debt; clinical data vs permission; geology vs central-bank reserves; IP vs generic lawsuit; decision vs intention; completed act vs historical summary. Check every quote literally and disclose genuinely unresolvable title-only facts in unknowns. Do not invent certainty to fill fields.
9. A scheduled earnings release/trial readout without actual released results is primary regime_state (a calendar/preview); retain the possible future print/readout only as an additional future event. preannounce requires preliminary results actually disclosed, not an earnings reminder. A review of previously released earnings numbers may retain print_vs_priced; generic upcoming/results expectations are not a print.
10. Distinguish the main predicate from cause/background. A signed order/final issued rule is an operative procedural/decision event, not merely statement_public because a press release describes it. Choose the specific permission, mandatory standard, subsidy, tax or restriction mechanism if stated/known. A corporate financial result/guidance cut caused by input costs remains primarily results/guidance, not input_cost. A sentence/judgment after a cyberattack is primarily blast_legal, not a new cyberattack. Actual bankruptcy is distress_restruct, not generic litigation. An awarded order/contract is demand, not physical capacity already added. An activist stake is not an insider trade; routine external fund holdings do not make the manager an insider.
'''

def ontology_text():
    return 'LOCKED LANE DEFINITIONS (' + ONTOLOGY_VERSION + '):\n' + '\n'.join(
        name + ': ' + DEFINITIONS[name] for name in EVENT_CLASSES
    ) + '\n\n' + DECISION_RULES
