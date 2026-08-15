# Ticker checklist (1,3,4,5) — 2026-08-14

Source: local `data/prices/ohlc.parquet` + Correlations peers.

| # | Check | Bull when |
|---|---|---|
| 1 | candle_bias | green body sum > red (last 10 sessions) |
| 3 | consecutive_down | ≥3 down closes in a row |
| 4 | peer_outperform | 1Y rel-line overtook / led peers over last 7 sessions |
| 5 | peer_breadth | ≥50% of peers up over last 7 sessions |

- Names: **11,587** | with bars: **11,566**
- Full detail CSV: `data/checklist/2026-08-14_checklist.csv`

## Top 20

| Ticker | Score | c1 | c3_n | c4 overtake | c5 breadth | detail |
|---|---|---|---|---|---|---|
| RCEL | +5 | True | 3 | True | 1.0 | ret7=+75.6% breadth=100% rs7=+56.62% overtake=True |
| HALO | +5 | True | 3 | True | 0.7 | ret7=+18.0% breadth=70% rs7=+21.11% overtake=True  |
| ICFI | +5 | True | 5 | True | 0.7 | ret7=+4.2% breadth=70% rs7=+1.53% overtake=True le |
| ORA | +4 | True | 0 | True | 1.0 | ret7=+17.1% breadth=100% rs7=+17.02% overtake=True |
| SAIC | +4 | True | 0 | True | 0.7 | ret7=+5.3% breadth=70% rs7=+4.13% overtake=True le |
| PMTS | +4 | True | 3 | False | 0.6 | ret7=+25.4% breadth=60% rs7=+33.34% overtake=False |
| SLNG | +4 | True | 0 | True | 1.0 | ret7=+39.1% breadth=100% rs7=+34.03% overtake=True |
| LXEO | +4 | True | 1 | True | 0.8571428571428571 | ret7=+8.8% breadth=86% rs7=+5.78% overtake=True le |
| STI | +4 | True | 0 | True | 0.8 | ret7=+22.7% breadth=80% rs7=+40.90% overtake=True  |
| RCMT | +4 | True | 0 | True | 0.6666666666666666 | ret7=+20.6% breadth=67% rs7=+27.52% overtake=True  |
| TKO | +4 | True | 0 | True | 0.8 | ret7=+9.9% breadth=80% rs7=+2.23% overtake=True le |
| TKNO | +4 | True | 2 | True | 0.7 | ret7=+29.9% breadth=70% rs7=+30.39% overtake=True  |
| PIII | +4 | True | 4 | False | 0.625 | ret7=+5.1% breadth=62% rs7=+6.28% overtake=False l |
| MASS | +4 | True | 1 | True | 0.8888888888888888 | ret7=+19.0% breadth=89% rs7=+17.79% overtake=True  |
| GFS | +4 | True | 0 | True | 0.9 | ret7=+10.4% breadth=90% rs7=+7.41% overtake=True l |
| GLSI | +4 | True | 0 | True | 0.8 | ret7=+16.7% breadth=80% rs7=+20.24% overtake=True  |
| NE | +4 | True | 0 | True | 1.0 | ret7=+13.0% breadth=100% rs7=+5.58% overtake=True  |
| RXO | +4 | True | 0 | True | 0.7 | ret7=+12.0% breadth=70% rs7=+13.37% overtake=True  |
| NDAQ | +4 | True | 1 | True | 0.9 | ret7=+2.6% breadth=90% rs7=+1.73% overtake=True le |
| TK | +4 | True | 0 | True | 1.0 | ret7=+14.7% breadth=100% rs7=+11.27% overtake=True |

## Full check dump (sample)

### XPON  score=+1
- **1 candle:** pass=False | green=0.5780 red=0.7700 bias=-0.1920 n=10
- **3 consecutive down:** pass=False n=0 | 0 consecutive down sessions
- **4 peer outperform:** pass=True overtake=False lead=True rs7=0.04708989586458945
  - ret7=+9.3% breadth=62% rs7=+4.71% overtake=False lead=True peers=8
  - peers: BLDP|ENPH|FCEL|FSLR|PLUG|RUN|SEDG|TSLA
- **5 peer breadth:** pass=True breadth=0.625

### AAPL  score=-1
- **1 candle:** pass=False | green=12.6413 red=13.8430 bias=-1.2017 n=10
- **3 consecutive down:** pass=False n=0 | 0 consecutive down sessions
- **4 peer outperform:** pass=False overtake=False lead=False rs7=-0.03274390892867973
  - ret7=-1.5% breadth=80% rs7=-3.27% overtake=False lead=False peers=10
  - peers: AMZN|DELL|GOOGL|HPQ|IBM|META|MSFT|NFLX|NVDA|SONY
- **5 peer breadth:** pass=True breadth=0.8

### NVDA  score=+1
- **1 candle:** pass=True | green=17.6600 red=14.6700 bias=2.9900 n=10
- **3 consecutive down:** pass=False n=1 | 1 consecutive down sessions
- **4 peer outperform:** pass=False overtake=False lead=False rs7=-0.06210389796371474
  - ret7=+2.7% breadth=80% rs7=-6.21% overtake=False lead=False peers=10
  - peers: ADI|AMAT|AMD|AVGO|INTC|LRCX|MRVL|MU|QCOM|TSM
- **5 peer breadth:** pass=True breadth=0.8

### ETON  score=+1
- **1 candle:** pass=False | green=4.6700 red=10.1500 bias=-5.4800 n=10
- **3 consecutive down:** pass=False n=0 | 0 consecutive down sessions
- **4 peer outperform:** pass=True overtake=False lead=True rs7=0.7588275622995384
  - ret7=+28.0% breadth=80% rs7=+75.88% overtake=False lead=True peers=10
  - peers: BMY|JNJ|LLY|MRK|NVS|PFE|REGN|SNY|VRTX|ZTS
- **5 peer breadth:** pass=True breadth=0.8

