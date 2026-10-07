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

**2026-10-07** — `282` sit sleeves with looked names / `340` sit sleeves total. Live policy sits. KEEP bar unchanged. Label: **RESEARCH**.

Per-sleeve paper counterfactuals on looked names. Live sit stays default. KEEP bar unchanged. (A) short-only fires the short kid at the 09:30 open. (B) dip-scoop longs wait for open−X% (session low / Elite live). Close does not trigger.

| Sleeve | Side | (A) short-only @ open | (B) dip-scoop X% |
|---|---|---|---|
| `flatten_robust` | long | — | WH miss; SIRI miss; AVPT miss; TWLO miss; GWRE miss; OKTA miss; BB miss; EPAM miss; IOT miss; KSPI miss; PANW miss; PAYS miss |
| `union_h1` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h3` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h5` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `flatten_h1` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h3` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h5` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `probable_h1` | long | — | CTVA miss; LVWR miss; XHG miss; WYHG miss; SRFM miss; SWRD miss; XNDU miss; KLC miss |
| `probable_h3` | long | — | CTVA miss; LVWR miss; XHG miss; WYHG miss; SRFM miss; SWRD miss; XNDU miss; KLC miss |
| `probable_h5` | long | — | CTVA miss; LVWR miss; XHG miss; WYHG miss; SRFM miss; SWRD miss; XNDU miss; KLC miss |
| `yday_gainer_h1` | long | — | CTVA miss; LVWR miss; XHG miss; WYHG miss; SRFM miss; SWRD miss; XNDU miss; KLC miss |
| `yday_gainer_h3` | long | — | CTVA miss; LVWR miss; XHG miss; WYHG miss; SRFM miss; SWRD miss; XNDU miss; KLC miss |
| `yday_gainer_h5` | long | — | CTVA miss; LVWR miss; XHG miss; WYHG miss; SRFM miss; SWRD miss; XNDU miss; KLC miss |
| `ohlc_hot_h1` | long | — | XPOF miss; ONON miss; INIO miss; EROC miss; SDEV miss; FN miss; MXL miss; SEI miss |
| `ohlc_hot_h3` | long | — | XPOF miss; ONON miss; INIO miss; EROC miss; SDEV miss; FN miss; MXL miss; SEI miss |
| `ohlc_hot_h5` | long | — | XPOF miss; ONON miss; INIO miss; EROC miss; SDEV miss; FN miss; MXL miss; SEI miss |
| `overnight_h1` | long | — | APLD miss; LEVI miss |
| `overnight_h3` | long | — | APLD miss; LEVI miss |
| `overnight_h5` | long | — | APLD miss; LEVI miss |
| `union_vol_g_h1` | long | — | NEOG miss; NEO miss; PENG miss; STZ miss; AVPT miss; XHG miss; SRFM miss; XNDU miss |
| `union_vol_g_h3` | long | — | NEOG miss; NEO miss; PENG miss; STZ miss; AVPT miss; XHG miss; SRFM miss; XNDU miss |
| `union_ab_g_h1` | long | — | NEOG miss; CTVA miss; NEO miss; PENG miss; STZ miss; BLFS miss; TOST miss; ONON miss |
| `union_ab_g_h3` | long | — | NEOG miss; CTVA miss; NEO miss; PENG miss; STZ miss; BLFS miss; TOST miss; ONON miss |
| `union_join_g_h1` | long | — | PENG miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss; XHG miss; XNDU miss |
| `union_join_g_h3` | long | — | PENG miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss; XHG miss; XNDU miss |
| `union_join_present_h1` | long | — | NEOG miss; CTVA miss; GLND miss; NEO miss; PENG miss; STZ miss; BLFS miss; TOST miss |
| `union_join_present_h3` | long | — | NEOG miss; CTVA miss; GLND miss; NEO miss; PENG miss; STZ miss; BLFS miss; TOST miss |
| `union_news_g_h1` | long | — | CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss |
| `union_news_g_h3` | long | — | CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss |
| `union_news_present_h1` | long | — | NEOG miss; CTVA miss; GLND miss; NEO miss; PENG miss; STZ miss; BLFS miss; TOST miss |
| `union_news_present_h3` | long | — | NEOG miss; CTVA miss; GLND miss; NEO miss; PENG miss; STZ miss; BLFS miss; TOST miss |
| `union_blue_h1` | long | — | PENG miss; STZ miss; PDFS miss; AVPT miss; XHG miss; XNDU miss; STDN miss; HTZ miss |
| `union_blue_h3` | long | — | PENG miss; STZ miss; PDFS miss; AVPT miss; XHG miss; XNDU miss; STDN miss; HTZ miss |
| `union_white_h1` | long | — | ONON miss; PDFS miss; AVPT miss; RCL miss; NTSK miss; SHAK miss |
| `union_white_h3` | long | — | ONON miss; PDFS miss; AVPT miss; RCL miss; NTSK miss; SHAK miss |
| `union_last_green_h1` | long | — | CTVA miss; PENG miss; STZ miss; BLFS miss; TOST miss; ONON miss; WYHG miss; SRFM miss |
| `union_last_green_h3` | long | — | CTVA miss; PENG miss; STZ miss; BLFS miss; TOST miss; ONON miss; WYHG miss; SRFM miss |
| `union_last_red_h1` | long | — | NEOG miss; GLND miss; NEO miss; PDFS miss; AVPT miss; XHG miss; PRME miss; TWST miss |
| `union_last_red_h3` | long | — | NEOG miss; GLND miss; NEO miss; PDFS miss; AVPT miss; XHG miss; PRME miss; TWST miss |
| `union_candle_h1` | long | — | PENG miss; STZ miss; BLFS miss; ONON miss; SRFM miss; STDN miss; HTZ miss; FCEL miss |
| `union_candle_h3` | long | — | PENG miss; STZ miss; BLFS miss; ONON miss; SRFM miss; STDN miss; HTZ miss; FCEL miss |
| `union_coil_off_h1` | long | — | PDFS miss; AVPT miss; KLC miss; FTAI miss |
| `union_coil_off_h3` | long | — | PDFS miss; AVPT miss; KLC miss; FTAI miss |
| `union_earn_react_h1` | long | — | NEOG miss; PENG miss; STZ miss |
| `union_earn_react_h3` | long | — | NEOG miss; PENG miss; STZ miss |
| `union_overnight_h1` | long | — | APLD miss |
| `union_overnight_h3` | long | — | APLD miss |
| `union_e_fresh_h1` | long | — | NEOG miss; PENG miss; STZ miss |
| `union_e_fresh_h3` | long | — | NEOG miss; PENG miss; STZ miss |
| `union_break10_h1` | long | — | PENG miss; ONON miss; STDN miss; HTZ miss; FCEL miss; CIEN miss; INIO miss; TLN miss |
| `union_break10_h3` | long | — | PENG miss; ONON miss; STDN miss; HTZ miss; FCEL miss; CIEN miss; INIO miss; TLN miss |
| `union_rsi_os_h1` | long | — | XNDU miss; AVBP miss |
| `union_rsi_os_h3` | long | — | XNDU miss; AVBP miss |
| `union_macd_up_h1` | long | — | PENG miss; STZ miss; TOST miss; ONON miss; PDFS miss; AVPT miss; SRFM miss; KLC miss |
| `union_macd_up_h3` | long | — | PENG miss; STZ miss; TOST miss; ONON miss; PDFS miss; AVPT miss; SRFM miss; KLC miss |
| `union_macd_xup_h1` | long | — | STZ miss; TOST miss; STDN miss; HTZ miss; CEG miss; PRME miss; FTAI miss; LEU miss |
| `union_macd_xup_h3` | long | — | STZ miss; TOST miss; STDN miss; HTZ miss; CEG miss; PRME miss; FTAI miss; LEU miss |
| `union_vol_g_h5` | long | — | NEOG miss; NEO miss; PENG miss; STZ miss; AVPT miss; XHG miss; SRFM miss; XNDU miss |
| `union_coil_off_h5` | long | — | PDFS miss; AVPT miss; KLC miss; FTAI miss |
| `union_last_green_h5` | long | — | CTVA miss; PENG miss; STZ miss; BLFS miss; TOST miss; ONON miss; WYHG miss; SRFM miss |
| `union_news_g_h5` | long | — | CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss |
| `union_white_h5` | long | — | ONON miss; PDFS miss; AVPT miss; RCL miss; NTSK miss; SHAK miss |
| `union_rsi_os_h5` | long | — | XNDU miss; AVBP miss |
| `union_news_pack_h1` | long | — | CEG miss |
| `union_news_head_h1` | long | — | NEO miss; CTVA miss; GLND miss; CEG miss; TWST miss; TXG miss; ELMT miss; EROC miss |
| `union_news_or_h1` | long | — | NEO miss; CTVA miss; GLND miss; CEG miss; TWST miss; TXG miss; ELMT miss; EROC miss |
| `union_news_both_h1` | long | — | CEG miss |
| `union_news_g_cond_h1` | long | — | NEO miss; CTVA miss; GLND miss; CEG miss; TWST miss; TXG miss; ELMT miss; EROC miss |
| `union_news_g_cam61_h1` | long | — | CEG miss |
| `union_news_or_net2_h1` | long | — | NEO miss; CEG miss; TWST miss; TXG miss; ELMT miss |
| `union_news_or_net3_h1` | long | — | CEG miss; TWST miss; TXG miss |
| `union_news_or_net4_h1` | long | — | CEG miss; TWST miss; TXG miss |
| `union_news_or_net5_h1` | long | — | CEG miss |
| `union_news_pack_net3_h1` | long | — | CEG miss |
| `union_news_pack_net2_h1` | long | — | CEG miss |
| `union_news_pack_h3` | long | — | CEG miss |
| `union_news_head_h3` | long | — | NEO miss; CTVA miss; GLND miss; CEG miss; TWST miss; TXG miss; ELMT miss; EROC miss |
| `union_news_or_h3` | long | — | NEO miss; CTVA miss; GLND miss; CEG miss; TWST miss; TXG miss; ELMT miss; EROC miss |
| `union_news_both_h3` | long | — | CEG miss |
| `union_news_g_cond_h3` | long | — | NEO miss; CTVA miss; GLND miss; CEG miss; TWST miss; TXG miss; ELMT miss; EROC miss |
| `union_news_g_cam61_h3` | long | — | CEG miss |
| `union_news_or_net2_h3` | long | — | NEO miss; CEG miss; TWST miss; TXG miss; ELMT miss |
| `union_news_or_net3_h3` | long | — | CEG miss; TWST miss; TXG miss |
| `union_news_or_net4_h3` | long | — | CEG miss; TWST miss; TXG miss |
| `union_news_or_net5_h3` | long | — | CEG miss |
| `union_news_pack_net3_h3` | long | — | CEG miss |
| `union_news_pack_net2_h3` | long | — | CEG miss |
| `union_news_or_net4_rw_h1` | long | — | CEG miss; TWST miss; TXG miss |
| `union_news_or_net4_conv_h1` | long | — | CEG miss; TWST miss; TXG miss |
| `union_news_g_conv_h1` | long | — | NEO miss; CTVA miss; GLND miss; CEG miss |
| `union_news_g_conv_h3` | long | — | NEO miss; CTVA miss; GLND miss; CEG miss |
| `short_news_head_h3` | short | SYRE @—; MRVL @— | — |
| `short_news_or_h3` | short | SYRE @—; MRVL @— | — |
| `union_vol_ab_h1` | long | — | NEOG miss; NEO miss; PENG miss; STZ miss; AVPT miss; CIEN miss; INIO miss; CEG miss |
| `union_vol_ab_h3` | long | — | NEOG miss; NEO miss; PENG miss; STZ miss; AVPT miss; CIEN miss; INIO miss; CEG miss |
| `union_blue_vol_h1` | long | — | PENG miss; STZ miss; AVPT miss; XHG miss; XNDU miss; HTZ miss; FCEL miss; CIEN miss |
| `union_blue_vol_h3` | long | — | PENG miss; STZ miss; AVPT miss; XHG miss; XNDU miss; HTZ miss; FCEL miss; CIEN miss |
| `union_news_vol_h1` | long | — | NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss |
| `union_news_vol_h3` | long | — | NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss |
| `union_e_green_h1` | long | — | PENG miss; STZ miss |
| `union_e_green_h3` | long | — | PENG miss; STZ miss |
| `probable_probable_ok_h1` | long | — | CTVA miss; WYHG miss; SRFM miss; XNDU miss; KLC miss |
| `probable_probable_ok_h3` | long | — | CTVA miss; WYHG miss; SRFM miss; XNDU miss; KLC miss |
| `union_vol_green_h1` | long | — | PENG miss; STZ miss; SRFM miss; XNDU miss; HTZ miss; FCEL miss; CIEN miss; INIO miss |
| `union_vol_green_h3` | long | — | PENG miss; STZ miss; SRFM miss; XNDU miss; HTZ miss; FCEL miss; CIEN miss; INIO miss |
| `union_coil_green_h1` | long | — | KLC miss; FTAI miss |
| `union_coil_green_h3` | long | — | KLC miss; FTAI miss |
| `union_blue_coil_h1` | long | — | STZ miss; PDFS miss; AVPT miss; XHG miss; XNDU miss; SUJA miss; AVBP miss; TWST miss |
| `union_blue_coil_h3` | long | — | STZ miss; PDFS miss; AVPT miss; XHG miss; XNDU miss; SUJA miss; AVBP miss; TWST miss |
| `union_join_vol_green_h1` | long | — | PENG miss; XNDU miss; CIEN miss; CEG miss; PSQL miss; ELMT miss; FN miss; DIOD miss |
| `union_join_vol_green_h3` | long | — | PENG miss; XNDU miss; CIEN miss; CEG miss; PSQL miss; ELMT miss; FN miss; DIOD miss |
| `union_white_coil_h1` | long | — | PDFS miss; AVPT miss |
| `union_white_coil_h3` | long | — | PDFS miss; AVPT miss |
| `flatten_vol_g_h3` | long | — | AVPT miss |
| `union_hot_score_h1` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss; NEO miss; GLND miss; SDEV miss; CIEN miss |
| `union_hot_score_h3` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss; NEO miss; GLND miss; SDEV miss; CIEN miss |
| `union_candle_score_h1` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss; GLND miss; NEO miss; CSCO miss; PWR miss |
| `union_candle_score_h3` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss; GLND miss; NEO miss; CSCO miss; PWR miss |
| `union_ret_5_h1` | long | — | NEOG miss; PENG miss; STZ miss; NEO miss; GLND miss; CTVA miss; STDN miss; CIEN miss |
| `union_ret_5_h3` | long | — | NEOG miss; PENG miss; STZ miss; NEO miss; GLND miss; CTVA miss; STDN miss; CIEN miss |
| `union_cond_h1` | long | — | NEOG miss; PENG miss; NEO miss; STZ miss; CTVA miss; GLND miss; AVPT miss; NTSK miss |
| `union_cond_h3` | long | — | NEOG miss; PENG miss; NEO miss; STZ miss; CTVA miss; GLND miss; AVPT miss; NTSK miss |
| `union_w_hot_cond_h1` | long | — | NEOG miss; PENG miss; NEO miss; STZ miss; CTVA miss; GLND miss; CEG miss; SDEV miss |
| `union_w_hot_cond_h3` | long | — | NEOG miss; PENG miss; NEO miss; STZ miss; CTVA miss; GLND miss; CEG miss; SDEV miss |
| `union_w_hot_candle_h1` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss; GLND miss; NEO miss; TLN miss; CIEN miss |
| `union_w_hot_candle_h3` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss; GLND miss; NEO miss; TLN miss; CIEN miss |
| `union_rsi_h1` | long | — | NEOG miss; STZ miss; NEO miss; GLND miss; PENG miss; CTVA miss; AVBP miss; XNDU miss |
| `union_rsi_h3` | long | — | NEOG miss; STZ miss; NEO miss; GLND miss; PENG miss; CTVA miss; AVBP miss; XNDU miss |
| `union_macd_hist_h1` | long | — | NEOG miss; PENG miss; STZ miss; GLND miss; NEO miss; CTVA miss; LITE miss; FN miss |
| `union_macd_hist_h3` | long | — | NEOG miss; PENG miss; STZ miss; GLND miss; NEO miss; CTVA miss; LITE miss; FN miss |
| `union_hot_n4_h1` | long | — | SDEV miss; OPCH miss; CIEN miss; STDN miss; QTEX miss; DNA miss; XP miss |
| `union_hot_n4_holdup` | long | — | SDEV miss; OPCH miss; CIEN miss; STDN miss |
| `union_hot_n12_h1` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss; NEO miss; GLND miss; SDEV miss; CIEN miss; STDN miss; TLN miss; HTZ miss; FCEL miss |
| `union_cond_n4_h3` | long | — | NEOG miss; PENG miss; NEO miss; STZ miss |
| `union_h3_exit_alarm` | long | — | NEOG miss; CTVA miss; GLND miss; NEO miss; PENG miss; STZ miss; BLFS miss; TOST miss |
| `union_h5_exit_alarm` | long | — | NEOG miss; CTVA miss; GLND miss; NEO miss; PENG miss; STZ miss; BLFS miss; TOST miss |
| `union_h3_exit_red` | long | — | CTVA miss; PENG miss; STZ miss; BLFS miss; TOST miss; ONON miss; WYHG miss; SRFM miss |
| `union_h3_exit_news_r` | long | — | NEOG miss; CTVA miss; GLND miss; NEO miss; PENG miss; STZ miss; BLFS miss; TOST miss |
| `coil_h3_exit_alarm` | long | — | PDFS miss; AVPT miss; KLC miss; FTAI miss; SUJA miss |
| `short_alarm_h1` | short | COP @—; LVWR @—; KOD @—; XPOF @—; SWRD @—; CVI @—; LEVI @— | — |
| `short_alarm_h3` | short | COP @—; LVWR @—; KOD @—; XPOF @—; SWRD @—; CVI @—; LEVI @— | — |
| `short_news_r_h1` | short | SYRE @—; MRVL @— | — |
| `short_news_r_h3` | short | SYRE @—; MRVL @— | — |
| `short_extended_h1` | short | PENG @—; OPCH @—; STDN @—; HTZ @—; FCEL @—; CIEN @—; INIO @—; TLN @— | — |
| `short_extended_h3` | short | PENG @—; OPCH @—; STDN @—; HTZ @—; FCEL @—; CIEN @—; INIO @—; TLN @— | — |
| `short_last_red_h1` | short | SYRE @—; NEOG @—; GLND @—; NEO @—; KOD @—; PDFS @—; AVPT @—; XHG @— | — |
| `short_last_red_h3` | short | SYRE @—; NEOG @—; GLND @—; NEO @—; KOD @—; PDFS @—; AVPT @—; XHG @— | — |
| `short_rsi_ob_h1` | short | KOD @—; ONON @—; OPCH @—; TLN @—; VST @—; LIND @—; MXL @—; SEI @— | — |
| `short_rsi_ob_h3` | short | KOD @—; ONON @—; OPCH @—; TLN @—; VST @—; LIND @—; MXL @—; SEI @— | — |
| `short_macd_dn_h1` | short | NEOG @—; COP @—; CTVA @—; GLND @—; NEO @—; BLFS @—; XHG @—; WYHG @— | — |
| `short_macd_dn_h3` | short | NEOG @—; COP @—; CTVA @—; GLND @—; NEO @—; BLFS @—; XHG @—; WYHG @— | — |
| `short_news_r_macd_h3` | short | SYRE @—; MRVL @— | — |
| `flatten_h5_rankw` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h5_topheavy` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h5_half` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h5_time` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h5_cut` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h5_trail` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h5_sboost` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h5_sizeup` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h3_rankw` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h3_topheavy` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h3_half` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h3_time` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h3_cut` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h3_trail` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h3_sboost` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `flatten_h3_sizeup` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `union_h5_rankw` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h5_topheavy` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h5_half` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h5_time` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h5_cut` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h5_trail` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h5_sboost` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h5_sizeup` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h3_rankw` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h3_topheavy` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h3_half` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h3_time` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h3_cut` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h3_trail` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h3_sboost` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h3_sizeup` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h1_rankw` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h1_topheavy` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h1_half` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h1_time` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h1_cut` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h1_trail` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h1_sboost` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_h1_sizeup` | long | — | NEOG miss; COP miss; CTVA miss; LVWR miss; GLND miss; NEO miss; XPOF miss; PENG miss |
| `union_white_yday_h1` | long | — | NTSK miss; ONON miss; RCL miss; SHAK miss |
| `union_white_yday_h3` | long | — | NTSK miss; ONON miss; RCL miss; SHAK miss |
| `flatten_white_yday_h5` | long | — | ONON miss |
| `union_white_any_h1` | long | — | ONON miss; RCL miss; NTSK miss; SHAK miss |
| `union_white_any_h2` | long | — | ONON miss; RCL miss; NTSK miss; SHAK miss |
| `union_white_any_h3` | long | — | ONON miss; RCL miss; NTSK miss; SHAK miss |
| `union_white_any_h5` | long | — | ONON miss; RCL miss; NTSK miss; SHAK miss |
| `flatten_h5_s8` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `union_clk_fresh_cat_coil_h1` | long | — | NEOG miss; NEO miss; CTVA miss; GLND miss |
| `short_clk_neg_weak_fail_h3` | short | SYRE @— | — |
| `short_clk_ext_veto_h3` | short | KOD @—; OPCH @—; LIND @—; P @— | — |
| `combo_seh_333_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_seh_333_split` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_seh_502525_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_seh_502525_split` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_seh_404020_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_seh_403525_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_seh_451540_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_seh_601525_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_se_5050_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss |
| `combo_se_7030_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss |
| `combo_eh_5050_shared` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_sh_5050_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_sh_macd_5050_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_se_5050_skip` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss |
| `combo_seh_333_skip` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_seh_333_weather` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_se_5050_weather` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss |
| `combo_se_5050_split` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss |
| `combo_es_8020_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss |
| `combo_es_9010_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss |
| `combo_ehs_702010_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_ehs_601525_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_ps_5050_shared` | mixed | SYRE @—; MRVL @— | CEG miss |
| `combo_ps_7030_shared` | mixed | SYRE @—; MRVL @— | CEG miss |
| `combo_p2s_5050_shared` | mixed | SYRE @—; MRVL @— | CEG miss |
| `combo_oh_5050_shared` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_sn_5050_shared` | mixed | SYRE @—; MRVL @— | CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss |
| `combo_sj_5050_shared` | mixed | SYRE @—; MRVL @— | PENG miss; XNDU miss; CIEN miss; CEG miss; PSQL miss; ELMT miss; FN miss; DIOD miss |
| `combo_sf_5050_shared` | mixed | SYRE @—; MRVL @— | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `combo_snj_333_shared` | mixed | SYRE @—; MRVL @— | CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss; PENG miss; XNDU miss; CIEN miss; PSQL miss; FN miss; DIOD miss |
| `combo_nse_333_shared` | mixed | SYRE @—; MRVL @— | CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss; NEOG miss; PENG miss; STZ miss |
| `combo_jse_333_shared` | mixed | SYRE @—; MRVL @— | PENG miss; XNDU miss; CIEN miss; CEG miss; PSQL miss; ELMT miss; FN miss; DIOD miss; NEOG miss; STZ miss |
| `combo_fse_333_shared` | mixed | SYRE @—; MRVL @— | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss; NEOG miss; PENG miss; STZ miss |
| `combo_e1s_7030_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss |
| `combo_ers_7030_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss |
| `combo_eh_7030_shared` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_fh_7030_shared` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss; NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_fe_5050_shared` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss; NEOG miss; PENG miss; STZ miss |
| `combo_fes_403030_shared` | mixed | SYRE @—; MRVL @— | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss; NEOG miss; PENG miss; STZ miss |
| `combo_se1_5050_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss |
| `combo_ser_5050_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss |
| `combo_en_5050_shared` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss |
| `combo_ej_5050_shared` | long | — | NEOG miss; PENG miss; STZ miss; XNDU miss; CIEN miss; CEG miss; PSQL miss; ELMT miss; FN miss; DIOD miss |
| `combo_ef_5050_shared` | long | — | NEOG miss; PENG miss; STZ miss; COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `combo_ee1_5050_shared` | long | — | NEOG miss; PENG miss; STZ miss |
| `combo_eer_5050_shared` | long | — | NEOG miss; PENG miss; STZ miss |
| `combo_hn_5050_shared` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss |
| `combo_hj_5050_shared` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss; XNDU miss; CIEN miss; CEG miss; PSQL miss; ELMT miss; FN miss; DIOD miss |
| `combo_hf_5050_shared` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss; COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `combo_he1_5050_shared` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_her_5050_shared` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_nj_5050_shared` | long | — | CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss; PENG miss; XNDU miss; CIEN miss; PSQL miss; FN miss; DIOD miss |
| `combo_nf_5050_shared` | long | — | CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss; COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `combo_ne1_5050_shared` | long | — | CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss; NEOG miss; PENG miss; STZ miss |
| `combo_ner_5050_shared` | long | — | CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss; NEOG miss; PENG miss; STZ miss |
| `combo_jf_5050_shared` | long | — | PENG miss; XNDU miss; CIEN miss; CEG miss; PSQL miss; ELMT miss; FN miss; DIOD miss; COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `combo_je1_5050_shared` | long | — | PENG miss; XNDU miss; CIEN miss; CEG miss; PSQL miss; ELMT miss; FN miss; DIOD miss; NEOG miss; STZ miss |
| `combo_jer_5050_shared` | long | — | PENG miss; XNDU miss; CIEN miss; CEG miss; PSQL miss; ELMT miss; FN miss; DIOD miss; NEOG miss; STZ miss |
| `combo_fe1_5050_shared` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss; NEOG miss; PENG miss; STZ miss |
| `combo_fer_5050_shared` | long | — | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss; NEOG miss; PENG miss; STZ miss |
| `combo_e1er_5050_shared` | long | — | NEOG miss; PENG miss; STZ miss |
| `combo_se_3070_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss |
| `combo_sh_7030_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_sh_3070_shared` | mixed | SYRE @—; MRVL @— | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_eh_3070_shared` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss |
| `combo_sn_7030_shared` | mixed | SYRE @—; MRVL @— | CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss |
| `combo_sn_3070_shared` | mixed | SYRE @—; MRVL @— | CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss |
| `combo_sj_7030_shared` | mixed | SYRE @—; MRVL @— | PENG miss; XNDU miss; CIEN miss; CEG miss; PSQL miss; ELMT miss; FN miss; DIOD miss |
| `combo_sj_3070_shared` | mixed | SYRE @—; MRVL @— | PENG miss; XNDU miss; CIEN miss; CEG miss; PSQL miss; ELMT miss; FN miss; DIOD miss |
| `combo_sf_7030_shared` | mixed | SYRE @—; MRVL @— | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `combo_sf_3070_shared` | mixed | SYRE @—; MRVL @— | COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `combo_en_7030_shared` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss |
| `combo_en_3070_shared` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss |
| `combo_ef_7030_shared` | long | — | NEOG miss; PENG miss; STZ miss; COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `combo_ef_3070_shared` | long | — | NEOG miss; PENG miss; STZ miss; COP miss; BLFS miss; TOST miss; ONON miss; PDFS miss; AVPT miss |
| `combo_hn_7030_shared` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss |
| `combo_hn_3070_shared` | long | — | NEOG miss; PENG miss; STZ miss; CTVA miss; GLND miss; NEO miss; CEG miss; EROC miss; ELMT miss; TWST miss; TXG miss |
| `combo_ee1_7030_shared` | long | — | NEOG miss; PENG miss; STZ miss |
| `combo_ee1_3070_shared` | long | — | NEOG miss; PENG miss; STZ miss |

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
