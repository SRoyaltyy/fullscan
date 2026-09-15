# Excel statistical mine — whole sheet A..JL

status=PARTIAL phase=disc_uni TIME-SPLIT cutoff=2026-07-02 tickers_done=0 sessions=0 letters=275 univ=mcap>$50M & vol>100000

Method: every letter as lag-1 fill / value / 10-row paint + same-row open-12 fills and open-44 numbers. chi2 + mutual info on discovery, Benjamini-Hochberg FDR q=0.1, holdout must keep lift>1. TIME-SPLIT: last 30% of session dates are holdout. Discovery feature date AND the full label window must be strictly before the cutoff. Pairs = Apriori AND of FDR survivors.

Discovery base P(I1 green)=0.000 · holdout base=0.000

## Holdout-confirmed singles (lowest p, lift>1 both sides)

| rule | disc n | disc lift | disc p | MI | hold n | hold lift |
|---|---:|---:|---:|---:|---:|---:|

## Holdout-confirmed pairs

| rule | disc n | disc lift | disc p | hold n | hold lift |
|---|---:|---:|---:|---:|---:|
