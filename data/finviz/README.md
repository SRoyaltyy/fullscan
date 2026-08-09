# Finviz universe snapshots

Place `finviz_with_descriptions.csv` here as `latest.csv` (or set `FINVIZ_CSV`).

Required columns:
- Ticker
- Industry
- Sector (optional)
- Market Cap (optional, used for ranking)
- Finviz_Description (optional, used to refine buckets like car_rental)

GitHub Action `News Actions` will try `secrets.FINVIZ_EXPORT` download first.
