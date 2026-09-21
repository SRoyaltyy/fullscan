# News-impact backtest — all

articles=5657  pipeline=news_impact_v1

## Usable / discarded

- Old news_parse: usable=431 discarded=5226 ratio=0.0762
- New router: usable=1195 discarded=4462 ratio=0.2112
- Rescued (old discard → new usable): 930
- Killed (old usable → new weather/discard): 166
- Ratio delta: 0.135

## Hopper watermark

- deterministic::news_impact_v1: 5657

## Event classes

- discard: 4436
- factor_impulse: 465
- print_vs_priced: 356
- corporate_action_mna: 91
- listing_flow: 68
- capital_return: 45
- guidance: 43
- regime_state: 26
- product_harm: 25
- access_control: 25
- gate: 15
- market_structure: 10
- blast_legal: 9
- labor_stop: 7
- blast_cyber: 6
- capacity: 6
- trial_readout: 5
- insider_flow: 4
- activist_campaign: 3
- peer_spill: 3
- inventory_print: 2
- input_cost: 2
- regime_break: 1
- statement_public: 1
- demand: 1
- regulatory_probe: 1
- price_cap: 1

## Rescued sample

- [market_structure] Dallas Fed Economists Warn Tokenized Deposits Could Trigger Bank Rate Wars - PYMNTS.com → ['listed expression unknown:not_determined']
- [blast_legal] Two drivers of Mercedes AMG cars file class action lawsuit over an alleged burn hazard in its logo - AP News → ['listed expression unknown:not_determined']
- [factor_impulse] Wall Street Loses Its Compass as Warsh’s Fed Strips Away All Rate Guidance - NAI500 → ['SPY:not_determined']
- [factor_impulse] Oil settles lower on clues about Fed policy, rumors of Hormuz deal - Reuters → ['SPY:not_determined']
- [factor_impulse] Warsh calls for a ‘quieter’ Fed focused on reducing inflation - The Center Square → ['SPY:not_determined']
- [input_cost] Airlines Scramble for Jet Fuel as Hormuz Disruption Drags On → ['AAL:down', 'DAL:down', 'UAL:down', 'LUV:down', 'ALK:down', 'XLE:up']
- [guidance] Elevance Health disclosed in an SEC filing that it reaffirmed its 2026 guidance for at least $27.00 in adjusted EPS and a 90.2% benefit expense ratio. → ['ELV:up']
- [factor_impulse] Fed rate hikes likely even with weakening jobs outlook - Washington Examiner → ['QQQ:down']
- [factor_impulse] Investors seek clearer Fed guidance from Warsh at Jackson Hole address → ['SPY:not_determined']
- [guidance] Wall Street looks to retail earnings as softer data reshapes Fed rate outlook: Dow Jones, S&P, Nasdaq, Futures - Yahoo Finance UK → ['listed expression unknown:not_determined']
- [factor_impulse] Wall Street slips as stubborn inflation keeps Fed outlook in focus - Malay Mail → ['SPY:not_determined']
- [factor_impulse] Warsh's inflation warning changes Fed rate outlook - thestreet.com → ['SPY:not_determined']
- [guidance] Abercrombie & Fitch Soars 37% on a $100M Tariff Refund and Raised Guidance, Ross and Kohl’s Hold Steady → ['ANF:up']
- [blast_legal] Amazon is trying to crush class action suits before they get started - The Verge → ['AMZN:down']
- [blast_cyber] Boston Scientific says cyberattack will materially hit Q3 and full-year 2026 results, no longer expects to meet sales and adjusted profit guidance → ['BSX:down', 'security vendors:not_determined']
- [market_structure] Coinbase Debuts Tokenized Stocks On Base Network → ['COIN:up', 'HOOD:mixed', 'CRCL:up', 'NDAQ:mixed', 'ICE:mixed', 'SCHW:down']
- [market_structure] Coinbase Enters the Tokenized Stock Wars → ['COIN:up', 'HOOD:mixed', 'CRCL:up', 'NDAQ:mixed', 'ICE:mixed', 'SCHW:down']
- [market_structure] Crypto.com Launches Tokenized Stock Derivatives → ['COIN:up', 'HOOD:mixed', 'CRCL:up', 'NDAQ:mixed', 'ICE:mixed', 'SCHW:down']
- [market_structure] Franklin Templeton's Tokenized Treasury Fund Lands on HashKey → ['listed expression unknown:not_determined']
- [market_structure] India Is Finally Getting Its First Tokenized Corporate Bond → ['listed expression unknown:not_determined']
- [market_structure] Japan To Proceed With Stock And Bond Tokenization → ['listed expression unknown:not_determined']
- [blast_legal] Kaplan Fox & Kilsheimer LLP Alerts Embecta Corp. (NASDAQ: EMBC) Investors to a Securities Class Action Deadline on August 17, 2026 → ['EMBC:down']
- [corporate_action_mna] Paramount seeks $1.88 billion bond in Warner Bros. merger antitrust case → ['WBD:mixed']
- [market_structure] RWA Market and Tokenized Assets Beat Meme Coins With a 50% Rally → ['listed expression unknown:not_determined']
- [market_structure] Robinhood CEO Calls On U.S. To Approve Tokenized Stocks → ['COIN:up', 'HOOD:mixed', 'CRCL:up', 'NDAQ:mixed', 'ICE:mixed', 'SCHW:down']

## Killed sample (old usable, now weather/junk)

- [regime/discard] OPEC+ loses oil market sway in Iran war as China gains influence - Business Recorder (no constraint identified)
- [regime/regime_state] Gold Is Now Pricing Fed Credibility Rather Than the Inflation Data - StoneX (gold-on-Fed reaction reprint)
- [regime/regime_state] Gold Price Forecast: Cooling Fed Rate Hike Expectations Boost Appeal as Price May Hit $4,500? - TradingKey (gold-on-Fed reaction reprint)
- [regime/regime_state] Gold falls amid firmer dollar after PCE, GDP data muddles Fed interest rate path - Investing.com (gold-on-Fed reaction reprint)
- [regime/regime_state] Gold hits over 3-month high ahead of US inflation data, Fed chair’s speech - The Business Times (gold-on-Fed reaction reprint)
- [regime/regime_state] Gold holds near $4,400 as fading Fed rate hike bets weigh on US Dollar - FXStreet (gold-on-Fed reaction reprint)
- [regime/regime_state] Gold price surge on Fed rate cut bets lifts Barrick Mining (B) 8.21% (gold-on-Fed reaction reprint)
- [regime/regime_state] Gold prices slide more than 3% after Fed Chair Kevin Warshâs Jackson Hole comments boost September U.S. rate hike expectations (gold-on-Fed reaction reprint)
- [regime/regime_state] Gold prices slide more than 3% after Fed Chair Kevin Warsh’s Jackson Hole comments boost September U.S. rate hike expectations (gold-on-Fed reaction reprint)
- [regime/discard] Brent Tops $89 Amid U.S.-Iran Stalemate Over Hormuz - Yahoo Finance (no constraint identified)
- [regime/regime_state] Hormuz blockade, crude, and CPI keep D-Street on edge (Hormuz already the weather)
- [regime/discard] Iran says it will name its terms for reopening Strait of Hormuz (no constraint identified)
- [regime/discard] Iran war live: Tehran demands end to US blockade to reopen Strait of Hormuz (no constraint identified)
- [regime/discard] Iran war live: Tehran prepares conditions to open Strait of Hormuz (no constraint identified)
- [regime/discard] Iran war mediators focus on reopening Strait of Hormuz - NBC News (no constraint identified)
- [regime/discard] Oil prices tumble amid renewed optimism over the Strait of Hormuz - NBC News (no constraint identified)
- [regime/regime_state] Strait of Hormuz shipping grinds to a halt ahead of U.S.-Iran ceasefire expiry - CNBC (Hormuz already the weather)
- [regime/discard] U.S. oil price hovers around $82 as traders weigh conflicting signals on Strait of Hormuz deal - cnbc.com (no constraint identified)
- [regime/discard] UAE says Iran targeted ADNOC tanker in Strait of Hormuz, no casualties (no constraint identified)
- [regime/discard] US-Iran Strait of Hormuz conflict escalation (no constraint identified)
- [regime/discard] US-Iran tanker war; Brent ~$107, Hormuz traffic impaired (no constraint identified)
- [regime/discard] US-Iran tanker war; Hormuz traffic impaired; Brent ~$107-109 (no constraint identified)
- [regime/discard] Klarna stock plunges 20% on trimmed guidance as German retail sales slow (no constraint identified)
- [regime/discard] 1 Wall Street Analyst Just Upgraded AMD Stock and Estimates 30% Upside. Here's Why He's Bullish on This Semiconductor Stock. (no constraint identified)
- [regime/discard] 10-year Treasury yield breaches 5% — global bond selloff (no constraint identified)
