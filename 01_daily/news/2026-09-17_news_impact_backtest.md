# News-impact backtest — 2026-09-17

articles=121  pipeline=news_impact_v1

## Usable / discarded

- Old news_parse: usable=8 discarded=113 ratio=0.0661
- New router: usable=38 discarded=83 ratio=0.314
- Rescued (old discard → new usable): 32
- Killed (old usable → new weather/discard): 2
- Ratio delta: 0.2479

## Hopper watermark

- deterministic::news_impact_v1: 121

## Event classes

- discard: 81
- print_vs_priced: 10
- guidance: 8
- corporate_action_mna: 4
- gate: 3
- capital_return: 3
- inventory_print: 2
- regime_state: 2
- blast_cyber: 1
- factor_impulse: 1
- capacity: 1
- statement_public: 1
- insider_flow: 1
- demand: 1
- price_cap: 1
- trial_readout: 1

## Rescued sample

- [guidance] Elevance Health disclosed in an SEC filing that it reaffirmed its 2026 guidance for at least $27.00 in adjusted EPS and a 90.2% benefit expense ratio. → ['ELV:up']
- [blast_cyber] Boston Scientific says cyberattack will materially hit Q3 and full-year 2026 results, no longer expects to meet sales and adjusted profit guidance → ['BSX:down', 'security vendors:not_determined']
- [capacity] ASML nearly sold out of 2027 EUV capacity amid very strong AI-driven demand, JPMorgan says → ['ASML:up', 'TSM:mixed']
- [guidance] Adobe posts record Q3 revenue $6.76B and non-GAAP EPS $6.13, raises FY26 outlook and leans into AI-driven freemium strategy → ['ADBE:up']
- [guidance] BAC CEO's soft Q3 outlook at Barclays conference drives 5% plunge → ['BAC:down']
- [capital_return] BBVA posts record Q2 2026 net profit, upgrades Mexico and South America guidance, announces extraordinary €2 billion share buyback program → ['BBVA:up']
- [print_vs_priced] Bank of America Falls 1.4% as Fee Guidance Drops 10%--20% → ['BAC:down']
- [guidance] Canadian Natural Resources posts record Q2 2026 results with EPS $1.58, raises 2026 production guidance and returns about $4B to shareholders → ['CNQ:up']
- [guidance] Cencora reaffirms fiscal 2026 adjusted EPS guidance despite Walgreens prescription volume shift → ['COR:up']
- [guidance] Cenovus Energy Q2 2026 non-GAAP EPS $1.08 misses estimates, revenue $14.7B beats, company raises full-year production guidance → ['CVE:up']
- [guidance] KMI rebounds on pipeline project momentum and raised 2026 guidance → ['KMI:up']
- [guidance] Linde posts record Q2 EPS, raises 2026 EPS guidance floor as project backlog grows about $1B to $8.1B → ['LIN:up']
- [capital_return] Strong H1 profits, guidance upgrade and £1bn buyback drive BCS 6.48% surge → ['BCS:up']
- [corporate_action_mna] Arthur J. Gallagher & Co. Acquires Innovise Business Consultants → ['AJG:mixed']
- [print_vs_priced] Bank of Nova Scotia posts record Q3 2026 EPS $2.28 as National Bank upgrades to Outperform, lifts target to C$142 → ['BNS:up']
- [print_vs_priced] Barron’s reports that Cardinal Health CEO Jason Hollar recently sold about $29 million of company stock following the post-earnings share surge to record levels. → ['CAH:up']
- [corporate_action_mna] Blackstone Infrastructure and PNM file amended New Mexico PRC merger plan with $220M ratepayer credits and $80M community benefits for $11.5B TXNM deal → ['listed expression unknown:not_determined']
- [print_vs_priced] Canadian Imperial Bank Of Commerce reports fiscal Q3 2026 results with non-GAAP EPS $1.97 (+25% YoY) and revenue $6.0B (+14% YoY), beats EPS and revenue estimates → ['CM:up']
- [insider_flow] Cardinal Health CEO Sells $29 Million in Stock After Earnings Send Shares to Record → ['CAH:not_determined']
- [demand] Cummins wins largest-ever BESS contract to supply storage systems for major U.S. data center project → ['CMI:up']
- [corporate_action_mna] Dominion, NextEra enhance Virginia merger benefits with 1,000 new jobs, doubled bill credits and $1B-a-year supplier program → ['D:mixed', 'NEE:mixed']
- [capital_return] Eni lifts 2026 hydrocarbon output growth target to ~5% and boosts 2026 buyback to €3.4B on stronger Q2 results → ['E:up']
- [print_vs_priced] Fabrinet earnings weakness and rising yields spark 6.5% APH drop → ['APH:down']
- [print_vs_priced] Fastenal beats Q2 estimates with EPS $0.33 (+14% YoY), revenue $2.4B (+15% YoY), notes gross margin pressure and lowers 2026 digital sales mix target → ['FAST:up']
- [gate] General Dynamics' GDIT wins $1.3B multi-year Enterprise Network Operations and Cybersecurity Support contract for Army National Guard, federal partners → ['GD:up']

## Killed sample (old usable, now weather/junk)

- [regime/regime_state] Gold price surge on Fed rate cut bets lifts Barrick Mining (B) 8.21% (gold-on-Fed reaction reprint)
- [regime/regime_state] Gold prices slide more than 3% after Fed Chair Kevin Warsh’s Jackson Hole comments boost September U.S. rate hike expectations (gold-on-Fed reaction reprint)
