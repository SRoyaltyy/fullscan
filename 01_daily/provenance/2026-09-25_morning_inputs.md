# 2026-09-25 morning inputs: provenance note

Written by Trading Bot Taskforce at 07:15 ET, 2026-09-25, so the 09-25 locked day is recorded as it actually happened.

- The first Pre-Open ALL run, 36112063800 (ECS, Grok), finished but produced **no** sector essays. All 11 failed closed after the OpenClaw gateway timed out three times and the run marked it DOWN. It produced no stock book, tickets or flatten card either.
- A second Pre-Open ALL run, 36118877330 (ubuntu, a manual dispatch at 05:31 ET, likely the 17:30 HKT poke), wrote all 11 sector essays with **DeepSeek (deepseek-chat), not Grok**. It also wrote the sector board, weather, join, map_heat_research, stock_book, live_boards, the flatten card (`data/sleeve_merge/today.json`) and paper, then was force-cancelled at about 07:00 ET.
- The strategy tickets on main (`data/day_board/2026-09-25_strategy_tickets.json` @ 643984255d) were rewritten by Stock Book ALL 36126554738.
- HOT4 (`union_hot_n4_h1`) picks come from the price-only panel baked on 2026-09-24: BUY GPRO, INSP, TJGC, QRVO; SELL GLND. They read no sector essay or stock book content. The sector board is only a readiness input for the paper send.
- Today's peers file (`data/peers/2026-09-25_peer_rs.csv`) was missing because of a date-fallback bug in `peer_rs._resolve_export`, which wrote into 09-24's file instead. It was rebuilt by AB Enrich 36127214334 (commit 93e67f9234).
- The catalyst file was stamped OK with 0 of 8 dossiers. Its retry, 36118879001, hung and was cancelled.
- The Factor Mine lock for 09-25 uses Yahoo bars and the 09-24 panel only. No DeepSeek-written content is among its inputs.
