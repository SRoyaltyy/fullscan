# Lane inbox

Research / side-path JSON jobs. Official free APIs only (`:free`, SiliconFlow
non-`Pro/`, Zhipu Flash IDs). Does not touch `flatten_robust` or the cash book.

- Live inbox: `inbox.json` (existing `key_people` / `key_products` / `revenue_mix` / `custom`)
- Example jobs: `inbox.examples.json`
- Answers: `outbox/YYYY-MM-DD.json`
- Router: `src/lane_route.py` via `.github/workflows/lane_json.yml`

## $0 allowlist

OpenRouter IDs must be `openrouter/free` or end in `:free`. SiliconFlow strips
`Pro/`. Zhipu primary Flash is `glm-4.7-flash` only (Cyrus 2026-09-21:
never `glm-4-flash-250414` / old glm-4-flash / `qwen2.5-7b-instruct` on
primary or news). On 429, abandon that provider — do not fall down older
sibling IDs. DashScope primary is `qwen-flash`.
`DASHSCOPE_BASE_URL` (if set) is tried before public `dashscope*.aliyuncs.com`.
TokenHub is overflow **behind** those $0 hoppers: flash IDs first
(`glm-5.3-flash`, `glm-5.3-flashx`, `deepseek-v4-flash`) then `hy3`.
`TOKENHUB_BASE_URL` or `TENCENT_BASE_URL` (if set) is tried before
`https://tokenhub.tencentmaas.com/v1`; `/chat/completions` is appended when
missing. Bearer: `TOKENHUB_API_KEY` or `TENCENT_API_KEY` (`HUNYUAN_API_KEY`
is an optional key alias only). Missing hopper secrets skip; they do not
fail the job.

## Enqueue

Copy a question into `inbox.json` and run **Lane JSON wire**
(`workflow_dispatch`). Leave the live NVDA smoke rows in place unless you
mean to replace that batch.

### `news_to_tickers` (high volume)

Zhipu `glm-4.7-flash` first, else SiliconFlow current (`Qwen/Qwen3-8B`),
OpenRouter `:free`, DashScope `qwen-flash`. Exact listed tickers only.

```json
{
  "id": "chip-curbs-1",
  "template": "news_to_tickers",
  "articles": [
    {"title": "…", "body": "…", "known_at": "2026-09-19T20:15:00Z"}
  ]
}
```

No `ticker` field. `#290` Grok-news overlay can enqueue this later.

### `company_dig` (longer context)

SiliconFlow Qwen / DeepSeek free non-Pro, then native DeepSeek, OpenRouter
`:free` overflow; Zhipu Flash is OK for quality. Still free-only.

```json
{
  "id": "NVDA-dig",
  "ticker": "NVDA",
  "template": "company_dig",
  "brief": "Data-center GPU franchise…",
  "questions": ["Who competes for training clusters?"]
}
```
