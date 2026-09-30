# Jev six-bit stop test

n=100 gold_keep=24 pred_keep=21 precision=0.9048 recall_print_done=0.7917 pass=False

Need precision ≥ 0.85, print/done recall ≥ 0.8, zero call-highlights / should-you-buy keeps.

## Misses
- gold=keep pred=drop/soft label=officer_voice | Horizon over which FOMC can achieve dual mandate could be communicated: St. Louis Fed's Musalem - tradingview.com
- gold=keep pred=drop/soft label=officer_voice | Federal Reserve Board Member Says More Rate Hikes May Be Needed - tokenpost.com
- gold=keep pred=drop/no_keep_bit label=official_print | US consumer confidence sinks to 12-year low over inflation, stagnant wages - South China Morning Post
- gold=keep pred=drop/no_keep_bit label=official_print | FDA Announces Nationwide Cheese Recall—Products Linked to Multistate E. Coli Outbreak - health.com
- gold=keep pred=drop/no_keep_bit label=finished_act | Singapore gets its first advanced semiconductor materials plant to meet AI demand - The Straits Times
- gold=drop pred=keep/print label=preview | Odds of October Rate Increase Drop as Fed’s Williams Signals He’s Open to a Pause - Barron's
- gold=drop pred=keep/print label=preview | Traders Cut October Fed Rate-Hike Bets After Williams Comment - tokenpost.com

JEV_SIXBIT_STOP_PASS=0
