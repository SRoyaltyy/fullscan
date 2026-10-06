# Voided pre-close draft lock for 2026-10-06

These 104 suggestion rows were locked before the cash close and then set aside. `2026-10-06_pre_close_draft.csv` is those rows plus the `suggestions.csv` header, byte for byte. They are no longer in `suggestions.csv`.

The original lock entry in `freeze_manifest.json` is unchanged. The next manifest entry has kind `void`, names the sha256 below, and points at this csv.

- signal_date: 2026-10-06
- voided sha256: 9d910cb9de3933b888159d438e95925d105bd8d485b5aaeda60272ad65e44992
- GitHub run: 37499263674
- commit: 856c5f64c033af7fecd410c2220ee73e29d873da (856c5f64c)
- locked at: 13:37 ET on 2026-10-06 (2026-10-06T17:37:43Z)
- void added at: 2026-10-06T21:25:00Z
- reason: pre-close draft lock, run 37499263674, commit 856c5f64c, 13:37 ET 2026-10-06; no book traded on it

No book traded on this lock. After the void, 2026-10-06 may be locked once from the real close.
