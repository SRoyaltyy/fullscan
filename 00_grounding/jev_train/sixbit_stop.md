# Jev six-bit stop test

n=100 gold_keep=26 pred_keep=22 precision=0.8182 recall_print_done=0.6923 pass=False

Need precision ≥ 0.85, print/done recall ≥ 0.8, zero call-highlights / should-you-buy keeps.

## Misses
- gold=keep pred=drop/soft label=officer_voice | St. Louis Fed President Warns: Excessive Silence from the Fed Could Push Up Rates and Inflation - finance.biggo.com
- gold=keep pred=drop/soft label=officer_voice | Horizon over which FOMC can achieve dual mandate could be communicated: St. Louis Fed's Musalem - tradingview.com
- gold=keep pred=drop/no_keep_bit label=official_print | US consumer confidence sinks to 12-year low over inflation, stagnant wages - South China Morning Post
- gold=keep pred=drop/no_keep_bit label=official_print | FDA Announces Nationwide Cheese Recall—Products Linked to Multistate E. Coli Outbreak - health.com
- gold=keep pred=drop/no_keep_bit label=finished_act | Singapore gets its first advanced semiconductor materials plant to meet AI demand - The Straits Times
- gold=keep pred=drop/tape label=finished_act | Oil Gains after Trump Denies He Is Willing to Ease Sanctions on Iran - twaslnews1.twaslnews.com
- gold=keep pred=drop/no_keep_bit label=finished_act | Anthropic warns AI may pose ‘existential risks to humanity’ in IPO filing - The Straits Times
- gold=keep pred=drop/no_keep_bit label=finished_act | Geopolitical disruptions push up freight costs and alter steel trade flows - EUROMETAL
- gold=drop pred=keep/done label=junk | Crystal Jade holding companies in S’pore, Hong Kong placed under receivership - The Straits Times
- gold=drop pred=keep/print label=preview | Odds of October Rate Increase Drop as Fed’s Williams Signals He’s Open to a Pause - Barron's
- gold=drop pred=keep/print label=preview | Traders Cut October Fed Rate-Hike Bets After Williams Comment - tokenpost.com
- gold=drop pred=keep/done label=junk | Around 100 businesses caught buying fake reviews in largest probe by Singapore's competition watchdog - CNA

JEV_SIXBIT_STOP_PASS=0
