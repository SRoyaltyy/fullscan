# Restatements

Records are append-only. A restatement is a one-time named correction of a day that was never computed. A date listed here cannot be restated again. The restated day stays designed after the fact and is not part of the clean record.

## 2026-09-25 oos0914

OOS0914_RESTATE 2026-09-25

- book: oos0914
- date: 2026-09-25
- approver: Cyrus
- approved: 2026-09-26 HKT
- record: designed after the fact; not part of the clean record
- reason: The first lock walked the retro store, which ends 2026-09-24, so 2026-09-25 had no bars. Buys, sells, and equity were carried from 2026-09-24. Cyrus approved one restatement from the same frozen snapshot, the same frozen rules, and the 2026-09-25 Yahoo prices (frozen price pin, then the live store).
- old_fingerprint: 655fb675b3f8934bde91cd9c47cbed145a173363a61c3cd291de965e759b469c
- new_fingerprint: 426d1d9e8e9ba595c4c75b937a6e10dc2051eae76b8a1d2028a7bd21fd7cfca0
- files:
- `data/factor_mine/oos0914/ledgers/2026-09-25.json` `655fb675b3f8934bde91cd9c47cbed145a173363a61c3cd291de965e759b469c` -> `426d1d9e8e9ba595c4c75b937a6e10dc2051eae76b8a1d2028a7bd21fd7cfca0`
- `data/factor_mine/oos0914/ledgers/2026-09-25.json.sha256` `45693879baa439caa839fd20afc746b56fac6e56d34d0026d5fd006ad84f9614` -> `db2dae5a8d17c3662d67ca01d3ecb0ca41b23c9e72f20a40a47c0fb4c3ce829d`
- `data/factor_mine/oos0914/state/oos0914_break10_h2_sx/2026-09-25.json` `51fad220510e8b0093cd92dea2cd2d56ed91b951d9b13761249d3d2befff9212` -> `0b51bbe7e42db444a587e7c54cf5e5da192b7bb0e53e5efce8884e6e53160e4a`
- `data/factor_mine/oos0914/state/oos0914_rvol_lg_h1_sx/2026-09-25.json` `f78557521a50e4e6893ad52fdea0b194ca731b0c145ac58cead035cb80b42a07` -> `59f2f83e5c5ddf78f9fe67cada16dab1cc8788d2c95bdfc46db34cffefaab0ed`
- `data/factor_mine/oos0914/state/oos0914_break10_h1_sx/2026-09-25.json` `ca1804a16248b60fbff5d12ae4fa2aa7ff65f830459b63978d0df6cfef8afc29` -> `4efbac9d8701acf3acaf57f2fcbfb7bb0ac7755b0d1d095597b56892f2270858`
- `data/factor_mine/oos0914/state/oos0914_zero_candle_h2_sx/2026-09-25.json` `ab4bf27273f620fc3e80cb5739371ae881e2117ff94a0ab73e03ab90df22d225` -> `61297bf32a8b2d32f917bd4e1f71662b1c7f532b9cac1bed4e5eb12c982d0bf2`

## keep-held fill option

KEEP_HELD_FILL 2026-09-26

- book: none. This entry does not restate a locked day.
- date: none
- approver: Cyrus
- approved: 2026-09-26
- record: no past ledger, fingerprint, or score is rewritten
- reason: The live paper path keeps a name that is still selected and already held. `size_hot4_tickets` records `already held — skip` and sends no buy. Sells come only from recipe exits: list-drop through `lot_should_sell` after the min-hold, or an early exit that function already fires. Hard-red sits new buys. It does not sell the lot by itself. List-drop exits on a hard-red morning still sell. The Group 3 walker `walk_recipe` still renews a held h1 name as a sell plus a buy at the same open, and pays Futubull fees on both sides, when `fill` is omitted. That default stays so the locked Group 3 scores stay as they were scored. `fill="keep_held"` is a new option. It is not applied to any locked record.
