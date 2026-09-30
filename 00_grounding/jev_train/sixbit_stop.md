# Jev six-bit stop test

n=100 gold_keep=26 pred_keep=18 precision=0.6667 recall_print_done=0.4615 pass=False

Need precision ≥ 0.85, print/done recall ≥ 0.8, zero call-highlights / should-you-buy keeps.

## Misses
- gold=keep pred=drop/soft label=officer_voice | Fed’s Barr Says More Hikes Likely Needed as Growth Picks Up - Bloomberg.com
- gold=keep pred=drop/no_keep_bit label=officer_voice | New York Fed president sees no need to rush another rate hike — Channel NewsAsia - ua.news
- gold=keep pred=drop/soft label=officer_voice | St. Louis Fed President Warns: Excessive Silence from the Fed Could Push Up Rates and Inflation - finance.biggo.com
- gold=keep pred=drop/soft label=officer_voice | Fed governor Lisa Cook says AI boom is creating broader inflation pressures - TheGrio
- gold=keep pred=drop/soft label=officer_voice | Horizon over which FOMC can achieve dual mandate could be communicated: St. Louis Fed's Musalem - tradingview.com
- gold=keep pred=drop/soft label=officer_voice | Federal Reserve Board Member Says More Rate Hikes May Be Needed - tokenpost.com
- gold=keep pred=drop/no_keep_bit label=finished_act | Nvidia launches security platform to keep AI agents from going rogue - Fox Business
- gold=keep pred=drop/no_keep_bit label=finished_act | AbbVie's Cerevel buyout delivers as FDA gives go-ahead to 1st-in-class Juvmo in Parkinson's disease - Fierce Pharma
- gold=keep pred=drop/no_keep_bit label=finished_act | Merck, Daiichi pull lung cancer filing after FDA pushback deals another blow to $4B deal - Fierce Biotech
- gold=keep pred=drop/source label=finished_act | Only US private passenger train files Chapter 11 bankruptcy - thestreet.com
- gold=keep pred=drop/no_keep_bit label=finished_act | Singapore gets its first advanced semiconductor materials plant to meet AI demand - The Straits Times
- gold=keep pred=drop/tape label=finished_act | U.S. Taps Strategic Oil Reserve Again as Diesel Tops $6 - Crude Oil Prices Today | OilPrice.com
- gold=keep pred=drop/tape label=finished_act | Oil Gains after Trump Denies He Is Willing to Ease Sanctions on Iran - twaslnews1.twaslnews.com
- gold=keep pred=drop/soft label=finished_act | Anthropic warns AI may pose ‘existential risks to humanity’ in IPO filing - The Straits Times
- gold=drop pred=keep/done label=junk | Crystal Jade holding companies in S’pore, Hong Kong placed under receivership - The Straits Times
- gold=drop pred=keep/done label=tape | Pressure on U.S. Treasurys eases after 30-year yield hits highest level since 2002 - CNBC
- gold=drop pred=keep/done label=tape | Caltex follows Shell's lead, raises petrol prices, Money News - AsiaOne
- gold=drop pred=keep/print label=junk | Real estate expert reacts to Federal Reserve's rate hike and how it affects housing market - CBS News
- gold=drop pred=keep/done label=junk | Citi Credit Cards offering S$288 cash rebate for FCY spend - The MileLion
- gold=drop pred=keep/done label=junk | Around 100 businesses caught buying fake reviews in largest probe by Singapore's competition watchdog - CNA

JEV_SIXBIT_STOP_PASS=0
