# Jev six-bit stop test

n=100 gold_keep=24 pred_keep=20 precision=1.0 recall_print_done=0.8333 pass=True

Need precision ≥ 0.85, print/done recall ≥ 0.8, zero call-highlights / should-you-buy keeps.

## Misses
- gold=keep pred=drop/soft label=officer_voice | St. Louis Fed President Warns: Excessive Silence from the Fed Could Push Up Rates and Inflation - finance.biggo.com
- gold=keep pred=drop/soft label=officer_voice | Horizon over which FOMC can achieve dual mandate could be communicated: St. Louis Fed's Musalem - tradingview.com
- gold=keep pred=drop/no_keep_bit label=finished_act | Singapore gets its first advanced semiconductor materials plant to meet AI demand - The Straits Times
- gold=keep pred=drop/no_keep_bit label=finished_act | Anthropic warns AI may pose ‘existential risks to humanity’ in IPO filing - The Straits Times

JEV_SIXBIT_STOP_PASS=1
