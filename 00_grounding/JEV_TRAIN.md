# Jev train

Cyrus grades hop-0 like homework. The page is `dashboard/jev-train/` and, once this folder is on `main` and Deploy dashboard runs, it is served at:

https://sroyaltyy.github.io/fullscan/dashboard/jev-train/

Deploy dashboard checks out `main` only. Until that merge, open `dashboard/jev-train/index.html` from this branch. The page has no `JEV_API_KEY` and no GitHub token in the source.

## Draw

`.github/workflows/jev_train.yml` is `workflow_dispatch` only. It has no schedule. It does not enable `jev_hop0.yml`.

New draw on the page dispatches that workflow with `mode=draw`, using a token typed into the page (memory only, sent only to `api.github.com`). The action checks out `main` and runs the current hop-0 gate (code bits + Jev) on 100 titles:

- 40 from `01_daily/news/*_parsed.json`, stratified by date (digest only if that day has no parse)
- 40 from today's Google News RSS watchlist, after the existing Jaccard dedupe (0.72)
- 20 from the frozen holdout in `00_grounding/jev_holdout.json` while 20 of those titles are still absent from `00_grounding/jev_train/`. Otherwise the next unseen slice of `00_grounding/jev_hard_misses.json`, starting at `cursor`

Excluded everywhere: titles in `jev_gold.json`, and any title hash already in `00_grounding/jev_train/*`. An existing holdout file is never rewritten. `jev_closed_lists.json` is never edited. `keep.json` is not written and Lane is not called.

The action commits, with `[skip ci]`:

- `00_grounding/jev_train/YYYYMMDD_HHMM_draw.json`
- `dashboard/jev-train/draw.json` (what the page fetches from raw `main`)
- `00_grounding/jev_hard_misses.json` only when the exam rotated and the cursor moved

The hard-misses file shipped here is the initial rotating bank (seed `20260929`, stratified from the parsed archive, gold and holdout removed). Graded false keeps and false drops are appended later. A hash already stored under `jev_train/` is not drawn again.

## Submit

Submit stays disabled until at least 30 rows are `K` or `D`. `?` does not count. Each row has a one-line reason next to K / D / ?. An empty reason is allowed. The session JSON stores it as `human_reason`, the markdown sheet puts it in the `note` column, and the issue lists every row as title, Jev, You, human_reason. The issue flags rows where You does not match Jev (K is KEEP, D is DROP, ? matches neither) and `human_reason` is blank.

The page builds the grades JSON in the browser and downloads the full sheet if dispatch fails. Submit itself only sends each row's id, K/D/?, and `human_reason`. GitHub's `workflow_dispatch` input is too small for 100 full titles. The action joins those marks to `draw.json` already on `main`. With a token, it POSTs `jev_train.yml` at ref `main`, `mode=grade`, grades as base64 JSON. The bot token stays in Actions.

The action then:

1. Writes `00_grounding/jev_train/YYYYMMDD_HHMM.json` and `.md` (Jev grade, human grade, bits).
2. Runs the grade script. False keep = Jev KEEP and human D. False drop = Jev DROP and human K. `?` is not a miss. Bits on those misses are recomputed from the title and the gate fields. Output: `YYYYMMDD_HHMM_grade.json`.
3. Opens or updates the issue titled `jev-train YYYYMMDD_HHMM` with label `jev-train`. The body is the count table, the miss tables, and links to the JSON files.
4. The page polls that label until the issue body contains its nonce, then shows the issue URL in large type: "Paste this to Grok to discuss."

Issue URL pattern: `https://github.com/SRoyaltyy/fullscan/issues/<number>` with title `jev-train YYYYMMDD_HHMM`.

Those files are committed to `main` by the action only. The page never patches a closed list.
