# Jev six-bit stop test

n=100 gold_keep=24 pred_keep=23 precision=0.7826 recall_print_done=0.75 pass=False

Need precision ≥ 0.85, print/done recall ≥ 0.8, zero call-highlights / should-you-buy keeps.

## Misses
- gold=keep pred=drop/soft label=officer_voice | St. Louis Fed President Warns: Excessive Silence from the Fed Could Push Up Rates and Inflation - finance.biggo.com
- gold=keep pred=drop/soft label=officer_voice | Horizon over which FOMC can achieve dual mandate could be communicated: St. Louis Fed's Musalem - tradingview.com
- gold=keep pred=drop/no_keep_bit label=official_print | US consumer confidence sinks to 12-year low over inflation, stagnant wages - South China Morning Post
- gold=keep pred=drop/no_keep_bit label=official_print | FDA Announces Nationwide Cheese Recall—Products Linked to Multistate E. Coli Outbreak - health.com
- gold=keep pred=drop/no_keep_bit label=finished_act | Singapore gets its first advanced semiconductor materials plant to meet AI demand - The Straits Times
- gold=keep pred=drop/no_keep_bit label=finished_act | Anthropic warns AI may pose ‘existential risks to humanity’ in IPO filing - The Straits Times
- gold=drop pred=keep/done label=tape | Pressure on U.S. Treasurys eases after 30-year yield hits highest level since 2002 - CNBC
- gold=drop pred=keep/done label=tape | Caltex follows Shell's lead, raises petrol prices, Money News - AsiaOne
- gold=drop pred=keep/print label=preview | Odds of October Rate Increase Drop as Fed’s Williams Signals He’s Open to a Pause - Barron's
- gold=drop pred=keep/print label=preview | Traders Cut October Fed Rate-Hike Bets After Williams Comment - tokenpost.com
- gold=drop pred=keep/done label=junk | Real estate expert reacts to Federal Reserve's rate hike and how it affects housing market - CBS News

JEV_SIXBIT_STOP_PASS=0
