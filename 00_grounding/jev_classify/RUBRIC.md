# Jev hop-1 rubric

Living criteria for how Jev **names** a kept title. Hop-0 already
filtered trash. Hop-1 emits the locked PR #301 JSON and nothing else:

```
{"event_class":"","sign":null,"q5":"impulse|regime|regime_break",
 "constraint":"","split":false,"split_facts":[],"why":""}
```

`event_class` is one of the closed `EVENT_CLASSES` in
`src/news_impact/schema.py`. `q5` is the logic type. `sign` is the
closed set (add/destroy, up/down, open/shut, tighten/lift, cut/raise)
or null. Themes (AI, geopolitics, China) are not classes. Second order
is a role, not a class. A new class needs a new loser AND winner set.

Hop-1 does not pick a ticker, polarity, or winner. It does not call
Lane. `keep.json` is not written and is not wired into Lane.

`discard` / `regime` here means "no tradeable mechanism," not "trash
title." If hop-0 kept it, hop-1 still classifies it.

Jev analysis is the hop-1 `QUESTIONS` pack plus `decide_classify()`.
Code prior is `classify_text`. Live Jev overrides when the answers land
on the enum.

The visible reason on a keep is `event_class|q5` (and `|sign` when
signed), not `core_material` / `choke_fact` / `code_print`. Hop-0
`reason` stays the filter reason for the keep/drop sheet. Hop-2
(`00_grounding/jev_book/RUBRIC.md`) sides Finviz names after that.
