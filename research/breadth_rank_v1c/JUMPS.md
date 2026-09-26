# Jump resolutions — breadth_rank_v1c

The `breadth_mine_v1b` jump gate flags 96 legs and 93 are unexplained. A leg is `open` divided by the previous close, or `close` divided by the same bar's `open`, strictly above 3 or below 1/3. A Yahoo split on the later print's date explains the leg when the ratio is within 25% of `1/split` or of the share factor. Earlier tables counted only the 57 unexplained open/previous-close legs. This table is all 93.

Each name was re-pulled on 2026-09-26 with yfinance 1.7.0, `auto_adjust=false`, `actions=true`, `repair=false`. Close and Adj Close stayed equal. No price was edited. Four open/previous-close legs match a split on a session with no print (ALP, NFE, TNMG, WCT). Those series were not rescaled. Every one of the 93 is dropped. YAAS is listed after the 93; it is not one of them.

Eight of these names were still inside the pinned `breadth_rank_v1b` snapshot and are removed from `research/breadth_rank_v1c/bars/ohlc.parquet`: ADBT, FIRY, JLHL, LGCL, NXTT, SMJF, XHLD, YXT. The other jump names were already absent.

| ticker | date | leg | ratio | resolution |
| --- | --- | --- | --- | --- |
| ADBT | 2026-09-03 | close_over_open | 0.223684 | Re-pull matches the old print (ratio 0.223684). Yahoo splits (none) do not explain this close_over_open within 25%. Not patched. Dropped. |
| AIXI | 2026-08-25 | open_over_prev_close | 3.187773 | Re-pull matches the old print (ratio 3.187773). Yahoo splits (2026-05-11 0.05, 2026-09-08 0.14285714) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| AKAN | 2026-04-22 | close_over_open | 3.141538 | Re-pull matches the old print (ratio 3.141538). Yahoo splits (2026-04-13 0.222) do not explain this close_over_open within 25%. Not patched. Dropped. |
| ALP | 2026-09-10 | open_over_prev_close | 52.121211 | Yahoo split on 2026-09-09 share factor 0.02 (price factor 50). Re-pulled Close still jumps 52.121211 and Adj Close equals Close. Not rescaled. Dropped. |
| AMIX | 2026-08-04 | close_over_open | 3.624535 | Re-pull matches the old print (ratio 3.624535). Yahoo splits (2026-06-24 0.047619048) do not explain this close_over_open within 25%. Not patched. Dropped. |
| ASTC | 2026-05-27 | open_over_prev_close | 3.004049 | Re-pull matches the old print (ratio 3.004049). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| CAPR | 2026-07-27 | open_over_prev_close | 0.29797 | Re-pull matches the old print (ratio 0.297970). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| CAST | 2026-03-10 | close_over_open | 0.276667 | Re-pull matches the old print (ratio 0.276667). Yahoo splits (none) do not explain this close_over_open within 25%. Not patched. Dropped. |
| CAST | 2026-06-15 | open_over_prev_close | 3.122581 | Re-pull matches the old print (ratio 3.122581). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| CELZ | 2026-06-30 | open_over_prev_close | 3.221427 | Re-pull matches the old print (ratio 3.221427). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| CHAI | 2026-06-09 | open_over_prev_close | 4.353658 | Re-pull matches the old print (ratio 4.353658). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| CISS | 2026-07-27 | open_over_prev_close | 0.269231 | Re-pull matches the old print (ratio 0.269231). Yahoo splits (2026-04-27 0.14285714, 2026-08-19 0.025) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| CLRO | 2026-08-06 | open_over_prev_close | 3.17663 | Re-pull matches the old print (ratio 3.176630). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| DSY | 2026-06-10 | open_over_prev_close | 5.206522 | Re-pull matches the old print (ratio 5.206522). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| EDHL | 2026-06-11 | open_over_prev_close | 4.722857 | Re-pull matches the old print (ratio 4.722857). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| EHGO | 2026-06-17 | open_over_prev_close | 3.643939 | Re-pull matches the old print (ratio 3.643939). Yahoo splits (2026-04-20 0.0625) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| EJH | 2026-03-09 | close_over_open | 0.278889 | Re-pull matches the old print (ratio 0.278889). Yahoo splits (2026-03-30 0.04) do not explain this close_over_open within 25%. Not patched. Dropped. |
| ENGN | 2026-05-07 | open_over_prev_close | 0.242938 | Re-pull matches the old print (ratio 0.242938). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| EYPT | 2026-08-17 | open_over_prev_close | 0.275254 | Re-pull matches the old print (ratio 0.275254). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| FCUV | 2026-07-31 | open_over_prev_close | 5.43617 | Re-pull matches the old print (ratio 5.436170). Yahoo splits (2026-06-23 0.25) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| FEED | 2026-09-15 | close_over_open | 0.308383 | Re-pull matches the old print (ratio 0.308383). Yahoo splits (2026-09-01 0.083333333) do not explain this close_over_open within 25%. Not patched. Dropped. |
| FIRY | 2026-04-23 | close_over_open | 3.439227 | Re-pull matches the old print (ratio 3.439227). Yahoo splits (none) do not explain this close_over_open within 25%. Not patched. Dropped. |
| FTRK | 2026-08-28 | open_over_prev_close | 0.14 | Re-pull matches the old print (ratio 0.140000). Yahoo splits (2026-08-28 0.05) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| GIPR | 2026-09-18 | open_over_prev_close | 3.386364 | Re-pull matches the old print (ratio 3.386364). Yahoo splits (2026-07-10 0.1) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| HAO | 2026-05-11 | open_over_prev_close | 0.087671 | Re-pull matches the old print (ratio 0.087671). Yahoo splits (2026-05-21 0.0078125, 2026-08-14 0.05) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| HAO | 2026-07-10 | close_over_open | 0.220245 | Re-pull matches the old print (ratio 0.220245). Yahoo splits (2026-05-21 0.0078125, 2026-08-14 0.05) do not explain this close_over_open within 25%. Not patched. Dropped. |
| HKIT | 2026-03-23 | close_over_open | 0.097436 | Re-pull matches the old print (ratio 0.097436). Yahoo splits (2026-04-06 0.02, 2026-05-29 0.33333333, 2026-07-06 0.04) do not explain this close_over_open within 25%. Not patched. Dropped. |
| HKIT | 2026-06-02 | open_over_prev_close | 0.253799 | Re-pull matches the old print (ratio 0.253799). Yahoo splits (2026-04-06 0.02, 2026-05-29 0.33333333, 2026-07-06 0.04) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| HYFM | 2026-08-03 | open_over_prev_close | 5.759259 | Re-pull matches the old print (ratio 5.759259). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| ILLR | 2026-03-20 | close_over_open | 150.000003 | Re-pull matches the old print (ratio 150.000003). Yahoo splits (2026-06-23 0.1) do not explain this close_over_open within 25%. Not patched. Dropped. |
| ILLR | 2026-03-20 | open_over_prev_close | 0.005 | Re-pull matches the old print (ratio 0.005000). Yahoo splits (2026-06-23 0.1) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| ILLR | 2026-06-25 | open_over_prev_close | 3.459038 | Re-pull matches the old print (ratio 3.459038). Yahoo splits (2026-06-23 0.1) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| INHD | 2026-06-08 | close_over_open | 35.576578 | Re-pull matches the old print (ratio 35.576578). Yahoo splits (2026-05-04 0.05) do not explain this close_over_open within 25%. Not patched. Dropped. |
| IPST | 2026-08-17 | open_over_prev_close | 3.6 | Re-pull matches the old print (ratio 3.600000). Yahoo splits (2026-04-23 0.05) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| JEM | 2026-06-30 | open_over_prev_close | 4.574074 | Re-pull matches the old print (ratio 4.574074). Yahoo splits (2026-04-13 0.05, 2026-07-14 0.083333333) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| JLHL | 2026-07-09 | close_over_open | 3.428954 | Re-pull matches the old print (ratio 3.428954). Yahoo splits (none) do not explain this close_over_open within 25%. Not patched. Dropped. |
| JZ | 2026-06-01 | close_over_open | 3.202479 | Re-pull matches the old print (ratio 3.202479). Yahoo splits (2026-07-06 0.033333333) do not explain this close_over_open within 25%. Not patched. Dropped. |
| JZ | 2026-06-02 | open_over_prev_close | 0.222581 | Re-pull matches the old print (ratio 0.222581). Yahoo splits (2026-07-06 0.033333333) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| LBGJ | 2026-07-20 | close_over_open | 0.102778 | Re-pull matches the old print (ratio 0.102778). Yahoo splits (2026-03-27 0.01, 2026-08-03 0.005) do not explain this close_over_open within 25%. Not patched. Dropped. |
| LGCL | 2026-08-18 | close_over_open | 0.210435 | Re-pull matches the old print (ratio 0.210435). Yahoo splits (2026-09-01 0.008) do not explain this close_over_open within 25%. Not patched. Dropped. |
| LHSW | 2026-07-06 | open_over_prev_close | 3.777778 | Re-pull matches the old print (ratio 3.777778). Yahoo splits (2026-06-22 0.0625) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| LHSW | 2026-09-08 | close_over_open | 0.293333 | Re-pull matches the old print (ratio 0.293333). Yahoo splits (2026-06-22 0.0625) do not explain this close_over_open within 25%. Not patched. Dropped. |
| LZMH | 2026-04-17 | close_over_open | 0.141463 | Re-pull matches the old print (ratio 0.141463). Yahoo splits (2026-05-22 0.05) do not explain this close_over_open within 25%. Not patched. Dropped. |
| MB | 2026-08-07 | open_over_prev_close | 3.813333 | Re-pull matches the old print (ratio 3.813333). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| MGN | 2026-03-26 | open_over_prev_close | 0.099764 | Re-pull matches the old print (ratio 0.099764). Yahoo splits (2026-09-08 0.025, 2026-09-17 0.033333333) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| MGN | 2026-09-09 | open_over_prev_close | 3.46 | Re-pull matches the old print (ratio 3.460000). Yahoo splits (2026-09-08 0.025, 2026-09-17 0.033333333) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| MPU | 2026-09-16 | open_over_prev_close | 19.4 | Re-pull matches the old print (ratio 19.400000). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| MSGY | 2026-09-25 | close_over_open | 3.788732 | Re-pull matches the old print (ratio 3.788732). Yahoo splits (2026-08-11 0.125) do not explain this close_over_open within 25%. Not patched. Dropped. |
| MYSE | 2026-04-16 | open_over_prev_close | 3.833333 | Re-pull matches the old print (ratio 3.833333). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| NAMI | 2026-08-07 | open_over_prev_close | 3.130584 | Re-pull matches the old print (ratio 3.130584). Yahoo splits (2026-06-25 0.04) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| NFE | 2026-09-15 | open_over_prev_close | 41.484846 | Yahoo split on 2026-09-14 share factor 0.02 (price factor 50). Re-pulled Close still jumps 41.484846 and Adj Close equals Close. Not rescaled. Dropped. |
| NXTT | 2026-08-04 | close_over_open | 0.302632 | Re-pull matches the old print (ratio 0.302632). Yahoo splits (2026-08-10 0.01) do not explain this close_over_open within 25%. Not patched. Dropped. |
| OFAL | 2026-08-12 | open_over_prev_close | 3.562412 | Re-pull matches the old print (ratio 3.562412). Yahoo splits (2026-07-31 0.1) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| OMH | 2026-07-21 | close_over_open | 3.224 | Re-pull matches the old print (ratio 3.224000). Yahoo splits (2026-08-31 0.02) do not explain this close_over_open within 25%. Not patched. Dropped. |
| ONCO | 2026-03-27 | close_over_open | 0.332192 | Re-pull matches the old print (ratio 0.332192). Yahoo splits (2026-03-25 0.2, 2026-05-21 0.1) do not explain this close_over_open within 25%. Not patched. Dropped. |
| PAAI | 2026-09-17 | close_over_open | 3.553191 | Re-pull matches the old print (ratio 3.553191). Yahoo splits (none) do not explain this close_over_open within 25%. Not patched. Dropped. |
| PFSA | 2026-08-18 | close_over_open | 3.115646 | Re-pull matches the old print (ratio 3.115646). Yahoo splits (2026-07-07 0.04, 2026-08-17 0.25) do not explain this close_over_open within 25%. Not patched. Dropped. |
| PLAG | 2026-08-11 | close_over_open | 5.429906 | Re-pull matches the old print (ratio 5.429906). Yahoo splits (none) do not explain this close_over_open within 25%. Not patched. Dropped. |
| PLAG | 2026-08-12 | open_over_prev_close | 0.232358 | Re-pull matches the old print (ratio 0.232358). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| PPCB | 2026-08-27 | open_over_prev_close | 3.280374 | Re-pull matches the old print (ratio 3.280374). Yahoo splits (2026-05-18 0.04) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| QH | 2026-04-20 | close_over_open | 5.111111 | Re-pull matches the old print (ratio 5.111111). Yahoo splits (none) do not explain this close_over_open within 25%. Not patched. Dropped. |
| QH | 2026-04-20 | open_over_prev_close | 0.209302 | Re-pull matches the old print (ratio 0.209302). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| QH | 2026-04-27 | open_over_prev_close | 30.0 | Re-pull matches the old print (ratio 30.000000). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| QNME | 2026-08-04 | open_over_prev_close | 3.566879 | Re-pull matches the old print (ratio 3.566879). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| RCON | 2026-08-05 | close_over_open | 0.153846 | Re-pull matches the old print (ratio 0.153846). Yahoo splits (2026-08-18 0.005) do not explain this close_over_open within 25%. Not patched. Dropped. |
| RITR | 2026-09-17 | close_over_open | 0.195 | Re-pull matches the old print (ratio 0.195000). Yahoo splits (none) do not explain this close_over_open within 25%. Not patched. Dropped. |
| RITR | 2026-09-17 | open_over_prev_close | 0.262812 | Re-pull matches the old print (ratio 0.262812). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| SCKT | 2026-08-10 | open_over_prev_close | 6.692308 | Re-pull matches the old print (ratio 6.692308). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| SION | 2026-08-10 | open_over_prev_close | 0.095024 | Re-pull matches the old print (ratio 0.095024). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| SLBT | 2026-06-16 | open_over_prev_close | 3.366366 | Re-pull matches the old print (ratio 3.366366). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| SMJF | 2026-08-27 | close_over_open | 0.131603 | Re-pull matches the old print (ratio 0.131603). Yahoo splits (none) do not explain this close_over_open within 25%. Not patched. Dropped. |
| STAK | 2026-07-24 | close_over_open | 7.536586 | Re-pull matches the old print (ratio 7.536586). Yahoo splits (none) do not explain this close_over_open within 25%. Not patched. Dropped. |
| SXTC | 2026-07-23 | open_over_prev_close | 0.202475 | Re-pull matches the old print (ratio 0.202475). Yahoo splits (2026-08-10 0.0125) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| SXTC | 2026-07-24 | close_over_open | 0.25098 | Re-pull matches the old print (ratio 0.250980). Yahoo splits (2026-08-10 0.0125) do not explain this close_over_open within 25%. Not patched. Dropped. |
| TGHL | 2026-06-01 | open_over_prev_close | 6.091954 | Re-pull matches the old print (ratio 6.091954). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| TNMG | 2026-09-09 | open_over_prev_close | 7.91762 | Yahoo split on 2026-09-08 share factor 0.125 (price factor 8). Re-pulled Close still jumps 7.917620 and Adj Close equals Close. Not rescaled. Dropped. |
| UPC | 2026-06-29 | open_over_prev_close | 5.800676 | Re-pull matches the old print (ratio 5.800676). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| VSME | 2026-06-10 | open_over_prev_close | 4.495747 | Re-pull matches the old print (ratio 4.495747). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| VWAV | 2026-09-14 | open_over_prev_close | 0.043255 | Re-pull matches the old print (ratio 0.043255). Yahoo splits (2026-09-22 0.05) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| VWAV | 2026-09-17 | open_over_prev_close | 19.787798 | Re-pull matches the old print (ratio 19.787798). Yahoo splits (2026-09-22 0.05) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| WCT | 2026-09-09 | open_over_prev_close | 4.8125 | Yahoo split on 2026-09-08 share factor 0.2 (price factor 5). Re-pulled Close still jumps 4.812500 and Adj Close equals Close. Not rescaled. Dropped. |
| WETO | 2026-07-22 | open_over_prev_close | 0.2325 | Re-pull matches the old print (ratio 0.232500). Yahoo splits (2026-08-03 0.01) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| WETO | 2026-07-31 | open_over_prev_close | 3.466667 | Re-pull matches the old print (ratio 3.466667). Yahoo splits (2026-08-03 0.01) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| WOK | 2026-05-13 | open_over_prev_close | 0.186186 | Re-pull matches the old print (ratio 0.186186). Yahoo splits (2026-06-18 0.01) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| XHG | 2026-08-13 | open_over_prev_close | 6.338798 | Re-pull matches the old print (ratio 6.338798). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| XHLD | 2026-08-06 | close_over_open | 3.469136 | Re-pull matches the old print (ratio 3.469136). Yahoo splits (none) do not explain this close_over_open within 25%. Not patched. Dropped. |
| YJ | 2026-08-19 | open_over_prev_close | 3.373563 | Re-pull matches the old print (ratio 3.373563). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |
| YXT | 2026-08-05 | close_over_open | 3.20082 | Re-pull matches the old print (ratio 3.200820). Yahoo splits (2026-07-14 0.1) do not explain this close_over_open within 25%. Not patched. Dropped. |
| YYAI | 2026-07-27 | close_over_open | 0.277264 | Re-pull matches the old print (ratio 0.277264). Yahoo splits (2026-05-18 0.025, 2026-08-17 0.05) do not explain this close_over_open within 25%. Not patched. Dropped. |
| YYAI | 2026-07-28 | close_over_open | 0.312195 | Re-pull matches the old print (ratio 0.312195). Yahoo splits (2026-05-18 0.025, 2026-08-17 0.05) do not explain this close_over_open within 25%. Not patched. Dropped. |
| ZCMD | 2026-05-29 | close_over_open | 0.283531 | Re-pull matches the old print (ratio 0.283531). Yahoo splits (2026-03-02 0.125, 2026-06-08 0.032258065, 2026-06-29 0.33333333) do not explain this close_over_open within 25%. Not patched. Dropped. |
| ZTG | 2026-08-17 | close_over_open | 0.332994 | Re-pull matches the old print (ratio 0.332994). Yahoo splits (none) do not explain this close_over_open within 25%. Not patched. Dropped. |
| ZTG | 2026-09-16 | open_over_prev_close | 3.432392 | Re-pull matches the old print (ratio 3.432392). Yahoo splits (none) do not explain this open_over_prev_close within 25%. Not patched. Dropped. |

## Not one of the 93

| ticker | date | leg | ratio | resolution |
| --- | --- | --- | --- | --- |
| YAAS | 2026-07-30 | open_over_prev_close | 5.202703 | Not one of the 93. Jump 5.202703 on 2026-07-30 matches Yahoo split share factor 0.2. Left dropped. Not rescaled. |
