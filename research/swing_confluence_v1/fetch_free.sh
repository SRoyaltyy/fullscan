#!/bin/bash
# free sources for updown_v5 / swing_confluence_v1 (box scratch only; nothing here is committed)
D=$1; cd $D; UA="fullscan-research cyrus.fung.research@gmail.com"
mkdir -p finra ftd fred form4
for s in DGS10 DGS2 BAMLH0A0HYM2 T10Y2Y NFCI; do curl -sfL -o fred/$s.csv "https://fred.stlouisfed.org/graph/fredgraph.csv?id=$s"; done
curl -sfL -o SKEW_History.csv https://cdn.cboe.com/api/global/us_indices/daily_prices/SKEW_History.csv
python3 - <<'P'
import pandas as pd, os, subprocess
for d in pd.bdate_range('2018-08-01','2026-10-19'):
    f=f"finra/{d:%Y%m%d}.txt.gz"
    if os.path.exists(f): continue
    r=subprocess.run(f"curl -sf https://cdn.finra.org/equity/regsho/daily/CNMSshvol{d:%Y%m%d}.txt | gzip > {f}",shell=True)
    if os.path.getsize(f)<100: os.remove(f)
P
for y in $(seq 2019 2026); do for m in 01 02 03 04 05 06 07 08 09 10 11 12; do for h in a b; do
  f=ftd/cnsfails$y$m$h.zip; [ -s $f ] || curl -sf -A "$UA" -o $f "https://www.sec.gov/files/data/fails-deliver-data/cnsfails$y$m$h.zip"; sleep 0.3; done; done; done
for y in $(seq 2019 2026); do for q in 1 2 3 4; do
  f=form4/${y}q$q.zip; [ -s $f ] || curl -sf -A "$UA" -o $f "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/${y}q${q}_form345.zip"; sleep 0.5; done; done
echo FREE_DONE
