# News pipes

One directory for every news job that touches this repo. Parsed means a feed landed a row. Published means a later job wrote a product from those rows.

Ask a day:

```
PYTHONPATH=. python3 -m src.news_day_index --date 2026-10-01
```

The catalog is `catalog.json`. A writer that is not in it shows up as unlisted, not as a silent extra pipe.

Live morning path: Pre-Open ALL writes the parse and the event scan. News actions turns edges into tickers. JEV hop-0, cron `25 11 * * 1-6` UTC, writes the keep and junk files. The trainer page is a separate publish and does not feed the book.

RSS, NewsAPI, and Reddit collectors have no schedule. They are not the live door.
