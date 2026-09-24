# Hard-red sit experiment

**A short-only (pre-open): KILL.** **B dip-scoop X=1.0% (intraday): KILL.**

Research only. Live `flatten_robust` buys and Webull `combo_sh_macd_5050_shared` stay on full hard-red sit. This board does not wire either idea. Excel does not own the sit. Deep-corr was not waited on.

## Clock split (Manager lock)

**A short-only** is a **pre-open** policy decision, made before 09:30 when morning S is already ≤−3. The short kid may fire at the official open. Excel may pre-filter short-eligible names via open-knowable CLEAR letters only (`FQ` / `ER` / `EP` stretch = yesterday already ran). Those letters CLEAR as *long avoids* on the Yahoo analog — they are a fade hint here, **not** a short CLEAR. Missing letters pass through. Excel does **not** own the sit.

**B long dip-scoop** is **intraday**, after 09:30. Open is known. The trigger is first touch of open−X% on same-day OHLC (session low = daily first-hit proxy). Close / last / Gap / Finviz Price never trigger. Close only grades after fees.

## Board — what would have happened

KILL A short-only (pre-open): thin n=11 fires (bar ≥30). After-fee win 50.0%. webull n=11 win=50%; flatten n=0 win=—. Do not change live sit on this sample.

KILL B dip-scoop X=1.0% (intraday): 55 fires but after-fee win 49.1% ≤ 55%. webull n=26 win=54%; flatten n=29 win=45%

On **2026-09-14** morning S was **-11.002** (hard-red). Live policy `combo_sh_macd_5050_shared` (#232) and `flatten_robust` sat every new lot.

Webull longs that sat: CMRC (open 3.510, low −1.14% — would scoop at 0.5%, 1.0%, last 3.470 (not 16:00)); GPRO (open 1.370, low −4.38% — would scoop at 0.5%, 1.0%, 1.5%, 2.0%, 3.0%, last 1.360 (not 16:00)); VERI (open 1.035, low −5.30% — would scoop at 0.5%, 1.0%, 1.5%, 2.0%, 3.0%, last 1.075 (not 16:00)); HUT (open 92.300, low −2.95% — would scoop at 0.5%, 1.0%, 1.5%, 2.0%, last 92.670 (not 16:00)); INDP (open 2.800, low −1.07% — would scoop at 0.5%, 1.0%, last 2.890 (not 16:00)).
Webull shorts that sat (the short kid): BKV (short at open 24.26 → last 24.31; 1-share after-fee -0.56, fee-dominated, not cash-book size, Excel ineligible); AMD (short at open 486.13 → last 491.90; 1-share after-fee -9.78, fee-dominated, not cash-book size, Excel eligible (FQ,EP)).
Flatten/io would-haves that sat: CVE (open 33.640, low −0.51% — would scoop at 0.5%, last 33.875 (not 16:00)); BG (open 123.850, low −0.33% — no X on the grid hit, last 124.930 (not 16:00)); NVT (open 150.000, low −2.80% — would scoop at 0.5%, 1.0%, 1.5%, 2.0%, last 147.600 (not 16:00)); DK (open 77.760, low −3.07% — would scoop at 0.5%, 1.0%, 1.5%, 2.0%, 3.0%, last 78.290 (not 16:00)).

Clock split: (A) short-only is a pre-open policy call — shorts fire at the clock-clean 09:30 open if S≤−3. Excel may pre-filter those names via open-knowable letters (FQ/ER/EP stretch); it does not own the sit. Deep-corr was not run. (B) long scoop is *intraday* after 09:30: trigger is first touch of open−X% (session low as the daily proxy). Close / last grades the fire — it does not trigger it. 09-14 Yahoo close is last-so-far, not 16:00. Flatten's live card used last-close for DK; scoop uses the official 09:30 open.

## RESEARCH per sleeve (paper, not a wire)

**2026-09-24** — `278` sit sleeves with looked names / `340` sit sleeves total. Live policy sits. KEEP bar unchanged. Label: **RESEARCH**.

Per-sleeve paper counterfactuals on looked names. Live sit stays default. KEEP bar unchanged. (A) short-only fires the short kid at the 09:30 open. (B) dip-scoop longs wait for open−X% (session low / Elite live). Close does not trigger.

| Sleeve | Side | (A) short-only @ open | (B) dip-scoop X% |
|---|---|---|---|
| `flatten_robust` | long | — | EOG miss; CVE miss; RRC miss; CHKP miss; S miss; BAH miss; VKTX miss; MAZE miss; A miss; HUM miss; DXCM miss; MGTX miss; CYPH miss |
| `union_h1` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h3` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h5` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `flatten_h1` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h3` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h5` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `probable_h1` | long | — | NNBR miss; INVZ miss; TLSA miss; GLND miss; QNC miss; EGHT miss; SRFM miss; FSLY miss |
| `probable_h3` | long | — | NNBR miss; INVZ miss; TLSA miss; GLND miss; QNC miss; EGHT miss; SRFM miss; FSLY miss |
| `probable_h5` | long | — | NNBR miss; INVZ miss; TLSA miss; GLND miss; QNC miss; EGHT miss; SRFM miss; FSLY miss |
| `yday_gainer_h1` | long | — | NNBR miss; INVZ miss; DH miss; ADCT miss; ACMR miss; ZSQR miss; AMPL miss; TLSA miss |
| `yday_gainer_h3` | long | — | NNBR miss; INVZ miss; DH miss; ADCT miss; ACMR miss; ZSQR miss; AMPL miss; TLSA miss |
| `yday_gainer_h5` | long | — | NNBR miss; INVZ miss; DH miss; ADCT miss; ACMR miss; ZSQR miss; AMPL miss; TLSA miss |
| `ohlc_hot_h1` | long | — | AMD miss; OPRT miss; INSP miss; SION miss; DELL miss; BRVE miss; CLOV miss; GME miss |
| `ohlc_hot_h3` | long | — | AMD miss; OPRT miss; INSP miss; SION miss; DELL miss; BRVE miss; CLOV miss; GME miss |
| `ohlc_hot_h5` | long | — | AMD miss; OPRT miss; INSP miss; SION miss; DELL miss; BRVE miss; CLOV miss; GME miss |
| `overnight_h1` | long | — | COST miss |
| `overnight_h3` | long | — | COST miss |
| `overnight_h5` | long | — | COST miss |
| `union_vol_g_h1` | long | — | CVE miss; NNBR miss; OPRT miss; FUL miss; NEOV miss; SFIX miss; SNX miss; INVZ miss |
| `union_vol_g_h3` | long | — | CVE miss; NNBR miss; OPRT miss; FUL miss; NEOV miss; SFIX miss; SNX miss; INVZ miss |
| `union_ab_g_h1` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss; NEOV miss |
| `union_ab_g_h3` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss; NEOV miss |
| `union_join_g_h1` | long | — | CVE miss; AMD miss; BB miss; DRI miss; SNX miss; EOG miss; RRC miss; ADCT miss |
| `union_join_g_h3` | long | — | CVE miss; AMD miss; BB miss; DRI miss; SNX miss; EOG miss; RRC miss; ADCT miss |
| `union_join_present_h1` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss; NEOV miss |
| `union_join_present_h3` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss; NEOV miss |
| `union_news_g_h1` | long | — | CVE miss; NNBR miss; AMD miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss |
| `union_news_g_h3` | long | — | CVE miss; NNBR miss; AMD miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss |
| `union_news_present_h1` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss; NEOV miss |
| `union_news_present_h3` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss; NEOV miss |
| `union_blue_h1` | long | — | ACMR miss; ZSQR miss; AMPL miss; TLSA miss; GLND miss; QNC miss; AIB miss; LU miss |
| `union_blue_h3` | long | — | ACMR miss; ZSQR miss; AMPL miss; TLSA miss; GLND miss; QNC miss; AIB miss; LU miss |
| `union_last_green_h1` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; BB miss; EOG miss; INVZ miss; ZSQR miss |
| `union_last_green_h3` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; BB miss; EOG miss; INVZ miss; ZSQR miss |
| `union_last_red_h1` | long | — | DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; RRC miss; DH miss; ADCT miss |
| `union_last_red_h3` | long | — | DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; RRC miss; DH miss; ADCT miss |
| `union_candle_h1` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; INVZ miss; DELL miss; BRVE miss; DHT miss |
| `union_candle_h3` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; INVZ miss; DELL miss; BRVE miss; DHT miss |
| `union_coil_off_h1` | long | — | CVE miss; AMD miss; OPRT miss; SNX miss; EOG miss; DH miss; ACMR miss; ZSQR miss |
| `union_coil_off_h3` | long | — | CVE miss; AMD miss; OPRT miss; SNX miss; EOG miss; DH miss; ACMR miss; ZSQR miss |
| `union_earn_react_h1` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `union_earn_react_h3` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `union_overnight_h1` | long | — | COST miss |
| `union_overnight_h3` | long | — | COST miss |
| `union_e_fresh_h1` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `union_e_fresh_h3` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `union_break10_h1` | long | — | AMD miss; OPRT miss; DELL miss; BRVE miss; DHT miss; AVT miss; UMC miss; VSTS miss |
| `union_break10_h3` | long | — | AMD miss; OPRT miss; DELL miss; BRVE miss; DHT miss; AVT miss; UMC miss; VSTS miss |
| `union_rsi_os_h1` | long | — | CGEM miss; ALKT miss; LXEO miss |
| `union_rsi_os_h3` | long | — | CGEM miss; ALKT miss; LXEO miss |
| `union_macd_up_h1` | long | — | AMD miss; BB miss; SNX miss; INVZ miss; DH miss; ACMR miss; ZSQR miss; CCOI miss |
| `union_macd_up_h3` | long | — | AMD miss; BB miss; SNX miss; INVZ miss; DH miss; ACMR miss; ZSQR miss; CCOI miss |
| `union_macd_xup_h1` | long | — | BB miss; AIB miss; VICR miss |
| `union_macd_xup_h3` | long | — | BB miss; AIB miss; VICR miss |
| `union_flow_in_h1` | long | — | ARLO miss; CHKP miss |
| `union_flow_in_h3` | long | — | ARLO miss; CHKP miss |
| `union_vol_g_h5` | long | — | CVE miss; NNBR miss; OPRT miss; FUL miss; NEOV miss; SFIX miss; SNX miss; INVZ miss |
| `union_coil_off_h5` | long | — | CVE miss; AMD miss; OPRT miss; SNX miss; EOG miss; DH miss; ACMR miss; ZSQR miss |
| `union_last_green_h5` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; BB miss; EOG miss; INVZ miss; ZSQR miss |
| `union_news_g_h5` | long | — | CVE miss; NNBR miss; AMD miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss |
| `union_rsi_os_h5` | long | — | CGEM miss; ALKT miss; LXEO miss |
| `union_flow_in_h5` | long | — | ARLO miss; CHKP miss |
| `union_news_pack_h1` | long | — | AMD miss; CVE miss; ACMR miss; EOG miss; RRC miss |
| `union_news_head_h1` | long | — | NNBR miss; AMPL miss; ADCT miss; ZSQR miss |
| `union_news_or_h1` | long | — | AMD miss; CVE miss; NNBR miss; ACMR miss; AMPL miss; EOG miss; ADCT miss; ZSQR miss |
| `union_news_g_cond_h1` | long | — | AMD miss; CVE miss; NNBR miss; ACMR miss; AMPL miss; EOG miss; ADCT miss; ZSQR miss |
| `union_news_g_cam61_h1` | long | — | AMPL miss; EOG miss |
| `union_news_or_net2_h1` | long | — | AMD miss; CVE miss; NNBR miss; ACMR miss; AMPL miss; EOG miss; ADCT miss; ZSQR miss |
| `union_news_or_net3_h1` | long | — | AMD miss; CVE miss; ACMR miss; AMPL miss; EOG miss; ADCT miss; ZSQR miss |
| `union_news_or_net4_h1` | long | — | AMD miss; ACMR miss; AMPL miss; EOG miss; ADCT miss; ZSQR miss |
| `union_news_or_net5_h1` | long | — | ACMR miss; AMPL miss; EOG miss |
| `union_news_pack_net3_h1` | long | — | AMD miss; CVE miss; ACMR miss; EOG miss |
| `union_news_pack_net2_h1` | long | — | AMD miss; CVE miss; ACMR miss; EOG miss; RRC miss |
| `union_news_pack_h3` | long | — | AMD miss; CVE miss; ACMR miss; EOG miss; RRC miss |
| `union_news_head_h3` | long | — | NNBR miss; AMPL miss; ADCT miss; ZSQR miss |
| `union_news_or_h3` | long | — | AMD miss; CVE miss; NNBR miss; ACMR miss; AMPL miss; EOG miss; ADCT miss; ZSQR miss |
| `union_news_g_cond_h3` | long | — | AMD miss; CVE miss; NNBR miss; ACMR miss; AMPL miss; EOG miss; ADCT miss; ZSQR miss |
| `union_news_g_cam61_h3` | long | — | AMPL miss; EOG miss |
| `union_news_or_net2_h3` | long | — | AMD miss; CVE miss; NNBR miss; ACMR miss; AMPL miss; EOG miss; ADCT miss; ZSQR miss |
| `union_news_or_net3_h3` | long | — | AMD miss; CVE miss; ACMR miss; AMPL miss; EOG miss; ADCT miss; ZSQR miss |
| `union_news_or_net4_h3` | long | — | AMD miss; ACMR miss; AMPL miss; EOG miss; ADCT miss; ZSQR miss |
| `union_news_or_net5_h3` | long | — | ACMR miss; AMPL miss; EOG miss |
| `union_news_pack_net3_h3` | long | — | AMD miss; CVE miss; ACMR miss; EOG miss |
| `union_news_pack_net2_h3` | long | — | AMD miss; CVE miss; ACMR miss; EOG miss; RRC miss |
| `union_news_or_net4_rw_h1` | long | — | AMD miss; ACMR miss; AMPL miss; EOG miss |
| `union_news_or_net4_conv_h1` | long | — | AMD miss; ACMR miss; AMPL miss; EOG miss |
| `union_news_g_conv_h1` | long | — | AMD miss; CVE miss; NNBR miss; ACMR miss |
| `union_news_g_conv_h3` | long | — | AMD miss; CVE miss; NNBR miss; ACMR miss |
| `short_news_head_h3` | short | BMEA @—; AEHL @—; PANW @—; VOYG @— | — |
| `short_news_or_h3` | short | BMEA @—; AEHL @—; PANW @—; VOYG @— | — |
| `union_vol_ab_h1` | long | — | CVE miss; NNBR miss; OPRT miss; FUL miss; NEOV miss; SFIX miss; SNX miss; INVZ miss |
| `union_vol_ab_h3` | long | — | CVE miss; NNBR miss; OPRT miss; FUL miss; NEOV miss; SFIX miss; SNX miss; INVZ miss |
| `union_blue_vol_h1` | long | — | ACMR miss; TLSA miss; GLND miss; QNC miss; LU miss; KVYO miss |
| `union_blue_vol_h3` | long | — | ACMR miss; TLSA miss; GLND miss; QNC miss; LU miss; KVYO miss |
| `union_news_vol_h1` | long | — | CVE miss; NNBR miss; ACMR miss |
| `union_news_vol_h3` | long | — | CVE miss; NNBR miss; ACMR miss |
| `union_e_green_h1` | long | — | BB miss |
| `union_e_green_h3` | long | — | BB miss |
| `probable_probable_ok_h1` | long | — | NNBR miss; INVZ miss; QNC miss; FSLY miss |
| `probable_probable_ok_h3` | long | — | NNBR miss; INVZ miss; QNC miss; FSLY miss |
| `union_vol_green_h1` | long | — | CVE miss; NNBR miss; OPRT miss; INVZ miss; FRO miss; QNC miss; FSLY miss; SECZ miss |
| `union_vol_green_h3` | long | — | CVE miss; NNBR miss; OPRT miss; INVZ miss; FRO miss; QNC miss; FSLY miss; SECZ miss |
| `union_coil_green_h1` | long | — | CVE miss; AMD miss; OPRT miss; EOG miss; ZSQR miss; DELL miss; DHT miss; AVT miss |
| `union_coil_green_h3` | long | — | CVE miss; AMD miss; OPRT miss; EOG miss; ZSQR miss; DELL miss; DHT miss; AVT miss |
| `union_blue_coil_h1` | long | — | ACMR miss; ZSQR miss; AMPL miss; TLSA miss; GLND miss; QNC miss; LU miss; KVYO miss |
| `union_blue_coil_h3` | long | — | ACMR miss; ZSQR miss; AMPL miss; TLSA miss; GLND miss; QNC miss; LU miss; KVYO miss |
| `union_join_vol_green_h1` | long | — | CVE miss; FRO miss; QNC miss; FSLY miss; KVYO miss; VICR miss |
| `union_join_vol_green_h3` | long | — | CVE miss; FRO miss; QNC miss; FSLY miss; KVYO miss; VICR miss |
| `flatten_vol_g_h3` | long | — | CVE miss |
| `ohlc_hot_coil_h1` | long | — | AMD miss; OPRT miss; DELL miss; DHT miss; AVT miss; UMC miss; VSTS miss; FRO miss |
| `union_hot_score_h1` | long | — | AMD miss; OPRT miss; NNBR miss; CVE miss; SNX miss; BB miss; SFIX miss; DRI miss |
| `union_hot_score_h3` | long | — | AMD miss; OPRT miss; NNBR miss; CVE miss; SNX miss; BB miss; SFIX miss; DRI miss |
| `union_candle_score_h1` | long | — | OPRT miss; CVE miss; NNBR miss; AMD miss; BB miss; SNX miss; SFIX miss; NEOV miss |
| `union_candle_score_h3` | long | — | OPRT miss; CVE miss; NNBR miss; AMD miss; BB miss; SNX miss; SFIX miss; NEOV miss |
| `union_ret_5_h1` | long | — | NNBR miss; OPRT miss; AMD miss; CVE miss; SNX miss; BB miss; DRI miss; FUL miss |
| `union_ret_5_h3` | long | — | NNBR miss; OPRT miss; AMD miss; CVE miss; SNX miss; BB miss; DRI miss; FUL miss |
| `union_cond_h1` | long | — | BB miss; SNX miss; AMD miss; CVE miss; NNBR miss; OPRT miss; NEOV miss; DRI miss |
| `union_cond_h3` | long | — | BB miss; SNX miss; AMD miss; CVE miss; NNBR miss; OPRT miss; NEOV miss; DRI miss |
| `union_w_hot_cond_h1` | long | — | AMD miss; BB miss; OPRT miss; SNX miss; CVE miss; NNBR miss; SFIX miss; DRI miss |
| `union_w_hot_cond_h3` | long | — | AMD miss; BB miss; OPRT miss; SNX miss; CVE miss; NNBR miss; SFIX miss; DRI miss |
| `union_w_hot_candle_h1` | long | — | OPRT miss; AMD miss; NNBR miss; CVE miss; SNX miss; BB miss; SFIX miss; NEOV miss |
| `union_w_hot_candle_h3` | long | — | OPRT miss; AMD miss; NNBR miss; CVE miss; SNX miss; BB miss; SFIX miss; NEOV miss |
| `union_rsi_h1` | long | — | FUL miss; SFIX miss; CVE miss; NEOV miss; DRI miss; BB miss; NNBR miss; OPRT miss |
| `union_rsi_h3` | long | — | FUL miss; SFIX miss; CVE miss; NEOV miss; DRI miss; BB miss; NNBR miss; OPRT miss |
| `union_macd_hist_h1` | long | — | AMD miss; SNX miss; BB miss; NNBR miss; SFIX miss; CVE miss; OPRT miss; NEOV miss |
| `union_macd_hist_h3` | long | — | AMD miss; SNX miss; BB miss; NNBR miss; SFIX miss; CVE miss; OPRT miss; NEOV miss |
| `union_hot_n4_h1` | long | — | GPRO miss; TJGC miss; AIB miss; QRVO miss; GLND miss; FEAM miss; XHLD miss; INDP miss |
| `union_hot_n4_holdup` | long | — | AMD miss; OPRT miss; NNBR miss; CVE miss |
| `overnight_mega_h1` | long | — | COST miss |
| `overnight_mega_h2` | long | — | COST miss |
| `union_hot_n12_h1` | long | — | AMD miss; OPRT miss; NNBR miss; CVE miss; SNX miss; BB miss; SFIX miss; DRI miss; NEOV miss; FUL miss; DELL miss; BRVE miss |
| `union_cond_n4_h3` | long | — | BB miss; SNX miss; AMD miss; CVE miss |
| `union_h3_exit_alarm` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss; NEOV miss |
| `union_h5_exit_alarm` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss; NEOV miss |
| `union_h3_exit_red` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; BB miss; EOG miss; INVZ miss; ZSQR miss |
| `union_h3_exit_news_r` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss; NEOV miss |
| `coil_h3_exit_alarm` | long | — | CVE miss; NNBR miss; AMD miss; OPRT miss; SNX miss; EOG miss; INVZ miss; DH miss |
| `short_alarm_h1` | short | BMEA @—; AIRS @—; INSP @—; SION @—; CLOV @—; GME @—; SES @—; SYM @— | — |
| `short_alarm_h3` | short | BMEA @—; AIRS @—; INSP @—; SION @—; CLOV @—; GME @—; SES @—; SYM @— | — |
| `short_news_r_h1` | short | BMEA @—; AEHL @—; PANW @—; VOYG @— | — |
| `short_news_r_h3` | short | BMEA @—; AEHL @—; PANW @—; VOYG @— | — |
| `short_extended_h1` | short | AEHL @—; INSP @—; SION @—; BRVE @—; SECZ @—; AIB @—; QRVO @—; BE @— | — |
| `short_extended_h3` | short | AEHL @—; INSP @—; SION @—; BRVE @—; SECZ @—; AIB @—; QRVO @—; BE @— | — |
| `short_last_red_h1` | short | BMEA @—; AEHL @—; PANW @—; AIRS @—; DRI @—; FUL @—; NEOV @—; SFIX @— | — |
| `short_last_red_h3` | short | BMEA @—; AEHL @—; PANW @—; AIRS @—; DRI @—; FUL @—; NEOV @—; SFIX @— | — |
| `short_rsi_ob_h1` | short | AMD @—; SNX @—; TJGC @—; GME @—; GLND @—; SECZ @—; VICR @—; SVIA @— | — |
| `short_rsi_ob_h3` | short | AMD @—; SNX @—; TJGC @—; GME @—; GLND @—; SECZ @—; VICR @—; SVIA @— | — |
| `short_macd_dn_h1` | short | BMEA @—; PANW @—; CVE @—; NNBR @—; OPRT @—; DRI @—; FUL @—; NEOV @— | — |
| `short_macd_dn_h3` | short | BMEA @—; PANW @—; CVE @—; NNBR @—; OPRT @—; DRI @—; FUL @—; NEOV @— | — |
| `short_news_r_macd_h3` | short | AEHL @— | — |
| `flatten_h5_rankw` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h5_topheavy` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h5_half` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h5_time` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h5_cut` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h5_trail` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h5_sboost` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h5_sizeup` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h3_rankw` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h3_topheavy` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h3_half` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h3_time` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h3_cut` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h3_trail` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h3_sboost` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `flatten_h3_sizeup` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `union_h5_rankw` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h5_topheavy` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h5_half` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h5_time` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h5_cut` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h5_trail` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h5_sboost` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h5_sizeup` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h3_rankw` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h3_topheavy` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h3_half` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h3_time` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h3_cut` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h3_trail` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h3_sboost` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h3_sizeup` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h1_rankw` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h1_topheavy` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h1_half` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h1_time` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h1_cut` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h1_trail` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h1_sboost` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `union_h1_sizeup` | long | — | CVE miss; NNBR miss; AIRS miss; AMD miss; OPRT miss; BB miss; DRI miss; FUL miss |
| `flatten_h5_s8` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `union_clk_mom_break_peer_h1` | long | — | AMD miss; OPRT miss; NNBR miss; CVE miss; DELL miss; DHT miss; AVT miss; UMC miss |
| `union_clk_fresh_cat_coil_h1` | long | — | BB miss; SNX miss; AMD miss; CVE miss; NNBR miss; NEOV miss; DRI miss; FUL miss |
| `short_clk_neg_weak_fail_h3` | short | BMEA @—; AEHL @—; PANW @— | — |
| `short_clk_ext_veto_h3` | short | TJGC @— | — |
| `union_clk_hold_vs_sector_h1` | long | — | OPRT miss; AKBA miss; BRVE miss; CCOI miss |
| `union_clk_flow_coil_h1` | long | — | ARLO miss; CHKP miss |
| `union_clk_nr7_mom_h1` | long | — | DH miss |
| `combo_seh_333_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_seh_333_split` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_seh_502525_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_seh_502525_split` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_seh_404020_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_seh_403525_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_seh_451540_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_seh_601525_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_se_5050_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_se_7030_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_eh_5050_shared` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_sh_5050_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_sh_macd_5050_shared` | mixed | AEHL @— | AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_se_5050_skip` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_seh_333_skip` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_seh_333_weather` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_se_5050_weather` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_se_5050_split` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_es_8020_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_es_9010_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_ehs_702010_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_ehs_601525_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_ps_5050_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | AMD miss; CVE miss; ACMR miss; EOG miss; RRC miss |
| `combo_ps_7030_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | AMD miss; CVE miss; ACMR miss; EOG miss; RRC miss |
| `combo_p2s_5050_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | AMD miss; CVE miss; ACMR miss; EOG miss; RRC miss |
| `combo_oh_5050_shared` | long | — | COST miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_sn_5050_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | CVE miss; NNBR miss; AMD miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss |
| `combo_sj_5050_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | CVE miss; FRO miss; QNC miss; FSLY miss; KVYO miss; VICR miss |
| `combo_sf_5050_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `combo_snj_333_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | CVE miss; NNBR miss; AMD miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss; FRO miss; QNC miss; FSLY miss; KVYO miss; VICR miss |
| `combo_nse_333_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | CVE miss; NNBR miss; AMD miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss; BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_jse_333_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | CVE miss; FRO miss; QNC miss; FSLY miss; KVYO miss; VICR miss; BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_fse_333_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss; BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_e1s_7030_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_ers_7030_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_eh_7030_shared` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_fh_7030_shared` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss; AMD miss; OPRT miss; NNBR miss |
| `combo_fe_5050_shared` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss; BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_fes_403030_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss; BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_se1_5050_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_ser_5050_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_en_5050_shared` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; CVE miss; NNBR miss; AMD miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss |
| `combo_ej_5050_shared` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; CVE miss; FRO miss; QNC miss; FSLY miss; KVYO miss; VICR miss |
| `combo_ef_5050_shared` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `combo_ee1_5050_shared` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_eer_5050_shared` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_hn_5050_shared` | long | — | AMD miss; OPRT miss; NNBR miss; CVE miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss |
| `combo_hj_5050_shared` | long | — | AMD miss; OPRT miss; NNBR miss; CVE miss; FRO miss; QNC miss; FSLY miss; KVYO miss; VICR miss |
| `combo_hf_5050_shared` | long | — | AMD miss; OPRT miss; NNBR miss; CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `combo_he1_5050_shared` | long | — | AMD miss; OPRT miss; NNBR miss; CVE miss; BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_her_5050_shared` | long | — | AMD miss; OPRT miss; NNBR miss; CVE miss; BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_nj_5050_shared` | long | — | CVE miss; NNBR miss; AMD miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss; FRO miss; QNC miss; FSLY miss; KVYO miss; VICR miss |
| `combo_nf_5050_shared` | long | — | CVE miss; NNBR miss; AMD miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss; CHKP miss; S miss; BAH miss |
| `combo_ne1_5050_shared` | long | — | CVE miss; NNBR miss; AMD miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss; BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_ner_5050_shared` | long | — | CVE miss; NNBR miss; AMD miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss; BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_jf_5050_shared` | long | — | CVE miss; FRO miss; QNC miss; FSLY miss; KVYO miss; VICR miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `combo_je1_5050_shared` | long | — | CVE miss; FRO miss; QNC miss; FSLY miss; KVYO miss; VICR miss; BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_jer_5050_shared` | long | — | CVE miss; FRO miss; QNC miss; FSLY miss; KVYO miss; VICR miss; BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_fe1_5050_shared` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss; BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_fer_5050_shared` | long | — | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss; BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_e1er_5050_shared` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_se_3070_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_sh_7030_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_sh_3070_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_eh_3070_shared` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; AMD miss; OPRT miss; NNBR miss; CVE miss |
| `combo_sn_7030_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | CVE miss; NNBR miss; AMD miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss |
| `combo_sn_3070_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | CVE miss; NNBR miss; AMD miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss |
| `combo_sj_7030_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | CVE miss; FRO miss; QNC miss; FSLY miss; KVYO miss; VICR miss |
| `combo_sj_3070_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | CVE miss; FRO miss; QNC miss; FSLY miss; KVYO miss; VICR miss |
| `combo_sf_7030_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `combo_sf_3070_shared` | mixed | BMEA @—; AEHL @—; PANW @—; VOYG @— | CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `combo_en_7030_shared` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; CVE miss; NNBR miss; AMD miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss |
| `combo_en_3070_shared` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; CVE miss; NNBR miss; AMD miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss |
| `combo_ef_7030_shared` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `combo_ef_3070_shared` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss; CVE miss; EOG miss; RRC miss; CHKP miss; S miss; BAH miss |
| `combo_hn_7030_shared` | long | — | AMD miss; OPRT miss; NNBR miss; CVE miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss |
| `combo_hn_3070_shared` | long | — | AMD miss; OPRT miss; NNBR miss; CVE miss; EOG miss; RRC miss; ADCT miss; ACMR miss; ZSQR miss |
| `combo_ee1_7030_shared` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |
| `combo_ee1_3070_shared` | long | — | BB miss; DRI miss; FUL miss; NEOV miss; SFIX miss; SNX miss |

Scoop trigger = official open + session low (Elite live only when the low has not printed). Close / last / Theme Radar never trigger. #236 KILL of global short-only / dip-scoop stands — this table is display/paper only.

Window `2026-08-13 → 2026-09-14` (22 sessions). Hard-red mornings (S≤-3): **10** — `2026-08-18` S=-6.2, `2026-08-19` S=-7.2, `2026-08-24` S=-5.175, `2026-08-31` S=-5.85, `2026-09-01` S=-6.3, `2026-09-02` S=-3.825, `2026-09-08` S=-11.475, `2026-09-09` S=-13.95, `2026-09-10` S=-13.275, `2026-09-14` S=-11.002.

Book% on the continuous $10k combo path is **not** the KEEP bar. Scoop / short-only books look richer because those extra lots stay held into later non-red days. KEEP only grades the hard-red **fires** after Futubull fees. A fat Book% with a coin-flip hard-red win rate is still KILL.

## KEEP bar

Need **≥30 fires** where the tape can print them, **> 55% after Futubull fees**, and **both tapes** (Webull combo + flatten/io) when that tape can actually fire. Walk-forward mines X on hidden windows; a full-sample winner that dies OOS is KILL. Thin n is KILL.

After-fee caveat: every graded fire pays the Futubull US round-trip. Scoop **trigger** is open + session low only (first touch of open−X%). Close is the grade, not the trigger. Daily OHLC cannot prove the print happened after 09:30, so scoop P&L is slightly optimistic. A missing 09:30 open is a skip — never Gap, last, or prior close. 09-14 may lack a 16:00 mark; those rows stay ungraded.

## Webull combo tape

Cash book is the audited `simulate_shared` ledger on `combo_sh_macd_5050_shared` (short news🔴 ∩ MACD-up + hot-4, shared 50/50, $10k, whole shares, sell first). Live sit is the control. (A) is pre-open short-only. (B) is the after-09:30 open−X% scoop (low touches the limit; close grades).

| Variant | Mode | Book% | Hard-red fires | After-fee win | Hard-red $ | Book win | Audit |
|---|---|---:|---:|---:|---:|---:|---|
| live sit (control) | `sit` | +37.68 | 0 | — | +0.00 | 61.2% | PASS |
| (A) short-only · pre-open | `short_only` | +51.83 | 11 | 50.0% | -904.58 | 62.2% | PASS |
| (A) short-only · Excel letter pre-filter | `short_only_excel` | +51.83 | 7 | 57.1% | -914.03 | 62.2% | PASS |
| (B) scoop 0.5% · after 09:30 | `dip_scoop 0.5%` | +74.18 | 26 | 50.0% | +723.23 | 59.6% | PASS |
| (B) scoop 1% · after 09:30 | `dip_scoop 1%` | +83.24 | 26 | 54.2% | +1387.90 | 60.7% | PASS |
| (B) scoop 1.5% · after 09:30 | `dip_scoop 1.5%` | +88.87 | 25 | 52.2% | +1835.80 | 60.2% | PASS |
| (B) scoop 2% · after 09:30 | `dip_scoop 2%` | +66.45 | 23 | 45.5% | +2263.94 | 58.0% | PASS |
| (B) scoop 3% · after 09:30 | `dip_scoop 3%` | +63.30 | 20 | 47.4% | +2065.04 | 58.8% | PASS |

## Flatten / .io tape

Long book only (`flatten_robust` has no short kid). Live sit and (A) short-only print **zero** new lots — same as live. (B) scoops the hard-red would-have .io names after open−X%, 1 share each, exit at the 3d horizon close. Trigger still uses open+low only.

| Variant | Hard-red fires | After-fee win | After-fee $ |
|---|---:|---:|---:|
| live sit (control) | 0 | — | +0.00 |
| (A) short-only (N/A — flatten has no short kid) | 0 | — | +0.00 |
| (B) scoop 0.5% | 34 | 32.4% | -17.09 |
| (B) scoop 1% | 29 | 44.8% | -14.15 |
| (B) scoop 1.5% | 23 | 43.5% | -4.35 |
| (B) scoop 2% | 15 | 40.0% | -3.20 |
| (B) scoop 3% | 8 | 50.0% | -1.81 |

## Walk-forward (mine X, freeze, score hidden)

4 folds — same cut as `walkforward_factor_mine` (first cutoff 2026-08-20, step 4, forward 4). Each cutoff picks the IS scoop X with the best after-fee hard-red win%, then scores that logic on the hidden window. Short-only has nothing to mine; OOS fires are pooled.

| Cutoff | Hidden | IS best X | IS win | OOS short n/win | OOS 0.5% | OOS 1% | OOS 1.5% | OOS 2% | OOS 3% |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `2026-08-20` | 2026-08-21 → 2026-08-26 | 1.0 | 80.0% | 3/33.3% | 4/25.0% | 4/25.0% | 4/25.0% | 4/25.0% | 4/75.0% |
| `2026-08-26` | 2026-08-27 → 2026-09-01 | 3.0 | 66.7% | 0/— | 6/50.0% | 6/50.0% | 6/50.0% | 5/40.0% | 5/60.0% |
| `2026-09-01` | 2026-09-02 → 2026-09-08 | 3.0 | 63.6% | 3/0.0% | 8/37.5% | 8/50.0% | 8/62.5% | 7/57.1% | 6/50.0% |
| `2026-09-08` | 2026-09-09 → 2026-09-14 | 1.5 | 57.1% | 1/— | 4/0.0% | 4/0.0% | 4/0.0% | 3/0.0% | 3/0.0% |

Pooled OOS short-only: n=7 win=16.7% $=-791.82.

Pooled OOS scoop by X:

| X | n | After-fee win | $ |
|---:|---:|---:|---:|
| 0.5% | 22 | 35.0% | +118.28 |
| 1% | 22 | 40.0% | +485.07 |
| 1.5% | 22 | 45.0% | +871.04 |
| 2% | 19 | 38.9% | +801.71 |
| 3% | 18 | 52.9% | +1411.26 |

## Gate (do not change live)

Hard-red S≤−3 currently blocks **long and short** new lots in:

1. `src/combo_broker.py` `size_combo_tickets` — Webull paper `combo_sh_macd_5050_shared` tickets (`hard_red` → skip every kid).
2. `src/factor_mine_combo.py` `simulate_shared` — the cash book used to grade that combo.
3. `src/factor_mine_book.py` `simulate_book` / `flatten_robust` `hard_red_no_new` — flatten/io sits new buys.

This experiment adds opt-in `hard_red_mode` (`sit` / `short_only` / `dip_scoop` / `short_and_scoop`) with default **sit**. Live callers do not pass a mode.

## KEEP / KILL

**A short-only (pre-open): KILL.** KILL A short-only (pre-open): thin n=11 fires (bar ≥30). After-fee win 50.0%. webull n=11 win=50%; flatten n=0 win=—. Do not change live sit on this sample.

**A · Excel letter pre-filter: KILL.** KILL A short-only · Excel letter pre-filter: thin n=7 fires (bar ≥30). After-fee win 57.1%. webull n=7 win=57%. Do not change live sit on this sample.

**B dip-scoop X=1.0% (intraday): KILL.** KILL B dip-scoop X=1.0% (intraday): 55 fires but after-fee win 49.1% ≤ 55%. webull n=26 win=54%; flatten n=29 win=45%

Do not merge a live policy change from this PR.
