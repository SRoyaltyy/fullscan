# Jev hop-2 rubric

Living criteria for how Jev **sides** a kept, classified title.
Hop-1 already named `event_class|q5`. Hop-2 only attaches listed
expressions from the on-disk Finviz export (company / industry /
brand). It does not invent a ticker. It does not call Lane.

This hop exists so three jobs stay split. One fat model can do any
one of them and then loses the other two.

```
parse   hop-1 closed enum (event_class|q5|sign). Not a research note.
context Finviz hit list. Listed + larger cap first. Retrieved, not written.
cost    one small choice/noul pack. No free-text ticker. No Lane HTTP.
```

Jev sides. The universe and the ranking are the database. Asking Jev
to name a ticker, write a brief, or pick winners outside the hit list
is how the cost/quality bargain dies.

```
named names  → named_side  up|down|mixed|out
industry cousins → peer_side  up|down|mixed|out
attach_peers → noul (high only when the family needs a substitute)
```

Code prior is `candidate_rows`. Live Jev overrides sides only among
those candidates. Weather / `discard` / `regime` → empty book.

A name that is not in the Finviz hit list is not in the book.
When both sides attach, the book keeps up to 4 named + 2 peers,
largest listed first. `keep.json` stays unwired.

The unique-title tape check is `src/jev_backtest.py`: X / Y =
n_bull / n_bear on one ticker in the entry session after
`published_at`. Z is trading sessions after that open.
