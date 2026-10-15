# OpenClaw SuperGrok ping

- generated: 2026-10-15T14:13:26.647333+00:00
- gateway: http://127.0.0.1:18789
- chosen_model: `xai/grok-4.3`
- available: openclaw, openclaw/default, openclaw/main
- grok_busy: False
- ok: **True**

## PONG

- http: 200 in 7.91s via `xai/grok-4.3`
- content: PONG
- error: —

## News classify prompt

- title: Brent crude jumps after a Hormuz tanker attack
- http: 200 in 12.47s via `xai/grok-4.3`
- content: {"event_class":"regime_state","sign":null,"q5":"regime","constraint":"Hormuz shipping risk","split":false,"split_facts":[],"why":"Hormuz-today per rules"}
- parsed_event_class: regime_state

VERDICT=OK
