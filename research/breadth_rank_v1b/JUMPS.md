# Jump resolutions — breadth_rank_v1b

Source of the 57 unexplained jumps: `breadth_mine_v1b` bars `research/breadth_mine_v1b/bars/ohlc.parquet`, sha256 `53ea564340a6d7fb452dc44c798f583d50d39968af145b577a8929d5e1daa797`. A jump is that session's open divided by the previous priced session's close, above 3 or below 1/3, with no Yahoo split on the later date within 25% of the price factor `1/split` or the share factor.

Each ticker was re-pulled on 2026-09-26 with yfinance 1.7.0, `auto_adjust=false`, `actions=true`, `repair=false`, window 2026-03-02 inclusive through 2026-09-26 exclusive. Chart `adjclose` was compared with Close. Prices were not edited.

A split matches when its Yahoo date is the jump session, or it falls after the previous priced session and on or before the jump session, and the ratio is within 25% of `1/split` or of the share factor. Four jumps match a split whose date has no print, so the jump shows up on the next priced session. Close and Adj Close are equal and still contain the jump. Those names are dropped. Rescaling by the split factor would be a price patch.

The other 53 jumps are unchanged in the re-pull and are not explained by a Yahoo split in that gap. Those names are dropped.

Kept prices are the `breadth_rank_v1` snapshot copied to `research/breadth_rank_v1b/bars/ohlc.parquet`. That file's sha256 is `8ac67b7110176e954c6cc938dea58f821aabef352da6205fa3b79015505a5339`, the same bytes as the old snapshot, because every jump name was already absent. The old path is not modified. This file has 0 unexplained jumps.

| ticker | date | prev priced | ratio | resolution |
| --- | --- | --- | --- | --- |
| AIXI | 2026-08-25 | 2026-08-24 | 3.187773 | Re-pull matches the old print (ratio 3.187773). Yahoo splits on this name (2026-05-11 0.05, 2026-09-08 0.14285714) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| ALP | 2026-09-10 | 2026-09-04 | 52.121211 | Yahoo split on 2026-09-09, after 2026-09-04 and before the next priced session 2026-09-10, share factor 0.02 (price factor 50). Re-pulled Close still jumps 52.121211 and Adj Close equals Close. Not rescaled. Dropped. |
| ASTC | 2026-05-27 | 2026-05-26 | 3.004049 | Re-pull matches the old print (ratio 3.004049). No Yahoo split in the download. Not patched. Dropped. |
| CAPR | 2026-07-27 | 2026-07-24 | 0.29797 | Re-pull matches the old print (ratio 0.297970). No Yahoo split in the download. Not patched. Dropped. |
| CAST | 2026-06-15 | 2026-06-12 | 3.122581 | Re-pull matches the old print (ratio 3.122581). No Yahoo split in the download. Not patched. Dropped. |
| CELZ | 2026-06-30 | 2026-06-29 | 3.221427 | Re-pull matches the old print (ratio 3.221427). No Yahoo split in the download. Not patched. Dropped. |
| CHAI | 2026-06-09 | 2026-06-08 | 4.353658 | Re-pull matches the old print (ratio 4.353658). No Yahoo split in the download. Not patched. Dropped. |
| CISS | 2026-07-27 | 2026-07-24 | 0.269231 | Re-pull matches the old print (ratio 0.269231). Yahoo splits on this name (2026-04-27 0.14285714, 2026-08-19 0.025) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| CLRO | 2026-08-06 | 2026-08-05 | 3.17663 | Re-pull matches the old print (ratio 3.176630). No Yahoo split in the download. Not patched. Dropped. |
| DSY | 2026-06-10 | 2026-06-09 | 5.206522 | Re-pull matches the old print (ratio 5.206522). No Yahoo split in the download. Not patched. Dropped. |
| EDHL | 2026-06-11 | 2026-06-10 | 4.722857 | Re-pull matches the old print (ratio 4.722857). No Yahoo split in the download. Not patched. Dropped. |
| EHGO | 2026-06-17 | 2026-06-16 | 3.643939 | Re-pull matches the old print (ratio 3.643939). Yahoo splits on this name (2026-04-20 0.0625) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| ENGN | 2026-05-07 | 2026-05-06 | 0.242938 | Re-pull matches the old print (ratio 0.242938). No Yahoo split in the download. Not patched. Dropped. |
| EYPT | 2026-08-17 | 2026-08-14 | 0.275254 | Re-pull matches the old print (ratio 0.275254). No Yahoo split in the download. Not patched. Dropped. |
| FCUV | 2026-07-31 | 2026-07-30 | 5.43617 | Re-pull matches the old print (ratio 5.436170). Yahoo splits on this name (2026-06-23 0.25) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| FTRK | 2026-08-28 | 2026-08-27 | 0.14 | Re-pull matches the old print (ratio 0.140000). Yahoo split on 2026-08-28 share factor 0.05 is outside 25% of the jump. No other split in the gap. Not patched. Dropped. |
| GIPR | 2026-09-18 | 2026-09-17 | 3.386364 | Re-pull matches the old print (ratio 3.386364). Yahoo splits on this name (2026-07-10 0.1) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| HAO | 2026-05-11 | 2026-05-08 | 0.087671 | Re-pull matches the old print (ratio 0.087671). Yahoo splits on this name (2026-05-21 0.0078125, 2026-08-14 0.05) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| HKIT | 2026-06-02 | 2026-06-01 | 0.253799 | Re-pull matches the old print (ratio 0.253799). Yahoo splits on this name (2026-04-06 0.02, 2026-05-29 0.33333333, 2026-07-06 0.04) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| HYFM | 2026-08-03 | 2026-07-31 | 5.759259 | Re-pull matches the old print (ratio 5.759259). No Yahoo split in the download. Not patched. Dropped. |
| ILLR | 2026-03-20 | 2026-03-19 | 0.005 | Re-pull matches the old print (ratio 0.005000). Yahoo splits on this name (2026-06-23 0.1) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| ILLR | 2026-06-25 | 2026-06-24 | 3.459038 | Re-pull matches the old print (ratio 3.459038). Yahoo splits on this name (2026-06-23 0.1) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| IPST | 2026-08-17 | 2026-08-14 | 3.6 | Re-pull matches the old print (ratio 3.600000). Yahoo splits on this name (2026-04-23 0.05) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| JEM | 2026-06-30 | 2026-06-29 | 4.574074 | Re-pull matches the old print (ratio 4.574074). Yahoo splits on this name (2026-04-13 0.05, 2026-07-14 0.08333333) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| JZ | 2026-06-02 | 2026-06-01 | 0.222581 | Re-pull matches the old print (ratio 0.222581). Yahoo splits on this name (2026-07-06 0.03333333) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| LHSW | 2026-07-06 | 2026-07-02 | 3.777778 | Re-pull matches the old print (ratio 3.777778). Yahoo splits on this name (2026-06-22 0.0625) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| MB | 2026-08-07 | 2026-08-06 | 3.813333 | Re-pull matches the old print (ratio 3.813333). No Yahoo split in the download. Not patched. Dropped. |
| MGN | 2026-03-26 | 2026-03-25 | 0.099764 | Re-pull matches the old print (ratio 0.099764). Yahoo splits on this name (2026-09-08 0.025, 2026-09-17 0.03333333) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| MGN | 2026-09-09 | 2026-09-08 | 3.46 | Re-pull matches the old print (ratio 3.460000). Yahoo splits on this name (2026-09-08 0.025, 2026-09-17 0.03333333) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| MPU | 2026-09-16 | 2026-09-11 | 19.4 | Re-pull matches the old print (ratio 19.400000). No Yahoo split in the download. Not patched. Dropped. |
| MYSE | 2026-04-16 | 2026-04-15 | 3.833333 | Re-pull matches the old print (ratio 3.833333). No Yahoo split in the download. Not patched. Dropped. |
| NAMI | 2026-08-07 | 2026-08-06 | 3.130584 | Re-pull matches the old print (ratio 3.130584). Yahoo splits on this name (2026-06-25 0.04) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| NFE | 2026-09-15 | 2026-09-11 | 41.484846 | Yahoo split on 2026-09-14, after 2026-09-11 and before the next priced session 2026-09-15, share factor 0.02 (price factor 50). Re-pulled Close still jumps 41.484846 and Adj Close equals Close. Not rescaled. Dropped. |
| OFAL | 2026-08-12 | 2026-08-11 | 3.562412 | Re-pull matches the old print (ratio 3.562412). Yahoo splits on this name (2026-07-31 0.1) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| PLAG | 2026-08-12 | 2026-08-11 | 0.232358 | Re-pull matches the old print (ratio 0.232358). No Yahoo split in the download. Not patched. Dropped. |
| PPCB | 2026-08-27 | 2026-08-26 | 3.280374 | Re-pull matches the old print (ratio 3.280374). Yahoo splits on this name (2026-05-18 0.04) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| QH | 2026-04-20 | 2026-04-17 | 0.209302 | Re-pull matches the old print (ratio 0.209302). No Yahoo split in the download. Not patched. Dropped. |
| QH | 2026-04-27 | 2026-04-24 | 30.0 | Re-pull matches the old print (ratio 30.000000). No Yahoo split in the download. Not patched. Dropped. |
| QNME | 2026-08-04 | 2026-08-03 | 3.566879 | Re-pull matches the old print (ratio 3.566879). No Yahoo split in the download. Not patched. Dropped. |
| RITR | 2026-09-17 | 2026-09-16 | 0.262812 | Re-pull matches the old print (ratio 0.262812). No Yahoo split in the download. Not patched. Dropped. |
| SCKT | 2026-08-10 | 2026-08-07 | 6.692308 | Re-pull matches the old print (ratio 6.692308). No Yahoo split in the download. Not patched. Dropped. |
| SION | 2026-08-10 | 2026-08-07 | 0.095024 | Re-pull matches the old print (ratio 0.095024). No Yahoo split in the download. Not patched. Dropped. |
| SLBT | 2026-06-16 | 2026-06-15 | 3.366366 | Re-pull matches the old print (ratio 3.366366). No Yahoo split in the download. Not patched. Dropped. |
| SXTC | 2026-07-23 | 2026-07-22 | 0.202475 | Re-pull matches the old print (ratio 0.202475). Yahoo splits on this name (2026-08-10 0.0125) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| TGHL | 2026-06-01 | 2026-05-29 | 6.091954 | Re-pull matches the old print (ratio 6.091954). No Yahoo split in the download. Not patched. Dropped. |
| TNMG | 2026-09-09 | 2026-09-04 | 7.91762 | Yahoo split on 2026-09-08, after 2026-09-04 and before the next priced session 2026-09-09, share factor 0.125 (price factor 8). Re-pulled Close still jumps 7.917620 and Adj Close equals Close. Not rescaled. Dropped. |
| UPC | 2026-06-29 | 2026-06-26 | 5.800676 | Re-pull matches the old print (ratio 5.800676). No Yahoo split in the download. Not patched. Dropped. |
| VSME | 2026-06-10 | 2026-06-09 | 4.495747 | Re-pull matches the old print (ratio 4.495747). No Yahoo split in the download. Not patched. Dropped. |
| VWAV | 2026-09-14 | 2026-09-11 | 0.043255 | Re-pull matches the old print (ratio 0.043255). Yahoo splits on this name (2026-09-22 0.05) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| VWAV | 2026-09-17 | 2026-09-16 | 19.787798 | Re-pull matches the old print (ratio 19.787798). Yahoo splits on this name (2026-09-22 0.05) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| WCT | 2026-09-09 | 2026-09-04 | 4.8125 | Yahoo split on 2026-09-08, after 2026-09-04 and before the next priced session 2026-09-09, share factor 0.2 (price factor 5). Re-pulled Close still jumps 4.812500 and Adj Close equals Close. Not rescaled. Dropped. |
| WETO | 2026-07-22 | 2026-07-21 | 0.2325 | Re-pull matches the old print (ratio 0.232500). Yahoo splits on this name (2026-08-03 0.01) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| WETO | 2026-07-31 | 2026-07-30 | 3.466667 | Re-pull matches the old print (ratio 3.466667). Yahoo splits on this name (2026-08-03 0.01) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| WOK | 2026-05-13 | 2026-05-12 | 0.186186 | Re-pull matches the old print (ratio 0.186186). Yahoo splits on this name (2026-06-18 0.01) do not fall in the gap or do not match within 25%. Not patched. Dropped. |
| XHG | 2026-08-13 | 2026-08-12 | 6.338798 | Re-pull matches the old print (ratio 6.338798). No Yahoo split in the download. Not patched. Dropped. |
| YJ | 2026-08-19 | 2026-08-18 | 3.373563 | Re-pull matches the old print (ratio 3.373563). No Yahoo split in the download. Not patched. Dropped. |
| ZTG | 2026-09-16 | 2026-09-15 | 3.432392 | Re-pull matches the old print (ratio 3.432392). No Yahoo split in the download. Not patched. Dropped. |

## Not one of the 57

| ticker | date | ratio | resolution |
| --- | --- | --- | --- |
| YAAS | 2026-07-30 | 5.202703 | Not one of the 57 unexplained v1b jumps. breadth_rank_v1 already dropped it: open/previous-close 5.202703 on 2026-07-30 is within 25% of Yahoo split share factor 0.2 (price factor 5). The old snapshot removed it to keep one scale. Not restored and not rescaled. |
