# Jev six-bit stop test

n=100 gold_keep=26 pred_keep=8 precision=1.0 recall_print_done=0.3077 pass=False

Need precision ≥ 0.85, print/done recall ≥ 0.8, zero call-highlights / should-you-buy keeps.

## Misses
- gold=keep pred=drop/no_keep_bit label=officer_voice | Fed’s Williams Hints Next Rate Increase Can Wait - WSJ
- gold=keep pred=drop/no_keep_bit label=officer_voice | New York Fed president sees no need to rush another rate hike — Channel NewsAsia - ua.news
- gold=keep pred=drop/soft label=officer_voice | St. Louis Fed President Warns: Excessive Silence from the Fed Could Push Up Rates and Inflation - finance.biggo.com
- gold=keep pred=drop/soft label=officer_voice | Horizon over which FOMC can achieve dual mandate could be communicated: St. Louis Fed's Musalem - tradingview.com
- gold=keep pred=drop/no_keep_bit label=official_print | US consumer confidence sinks to 12-year low over inflation, stagnant wages - South China Morning Post
- gold=keep pred=drop/no_keep_bit label=official_print | FDA Announces Nationwide Cheese Recall—Products Linked to Multistate E. Coli Outbreak - health.com
- gold=keep pred=drop/no_keep_bit label=finished_act | Nvidia launches security platform to keep AI agents from going rogue - Fox Business
- gold=keep pred=drop/no_keep_bit label=finished_act | AbbVie's Cerevel buyout delivers as FDA gives go-ahead to 1st-in-class Juvmo in Parkinson's disease - Fierce Pharma
- gold=keep pred=drop/no_keep_bit label=finished_act | Merck, Daiichi pull lung cancer filing after FDA pushback deals another blow to $4B deal - Fierce Biotech
- gold=keep pred=drop/source label=finished_act | Only US private passenger train files Chapter 11 bankruptcy - thestreet.com
- gold=keep pred=drop/no_keep_bit label=finished_act | Oura becomes latest US IPO hopeful to delay listing as market jitters deepen - Reuters
- gold=keep pred=drop/no_keep_bit label=finished_act | China unveils rate cut, mortgage subsidies to spur growth - CNA
- gold=keep pred=drop/no_keep_bit label=finished_act | Singapore gets its first advanced semiconductor materials plant to meet AI demand - The Straits Times
- gold=keep pred=drop/no_keep_bit label=finished_act | U.S. Taps Strategic Oil Reserve Again as Diesel Tops $6 - Crude Oil Prices Today | OilPrice.com
- gold=keep pred=drop/tape label=finished_act | Oil Gains after Trump Denies He Is Willing to Ease Sanctions on Iran - twaslnews1.twaslnews.com
- gold=keep pred=drop/no_keep_bit label=finished_act | Anthropic warns AI may pose ‘existential risks to humanity’ in IPO filing - The Straits Times
- gold=keep pred=drop/no_keep_bit label=finished_act | Geopolitical disruptions push up freight costs and alter steel trade flows - EUROMETAL
- gold=keep pred=drop/no_keep_bit label=finished_act | Selected steel intake transiting through the Strait of Hormuz dropped - Shanghai Metals Market

JEV_SIXBIT_STOP_PASS=0
