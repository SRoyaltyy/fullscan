import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from . import portfolio_risk as risk
from .execution_clock import fixed_bps, timestamped_quotes
from .research_validation import clock_errors, promotion_evidence, write_once
from .learning_trials import govern
from . import factor_mine as fm, factor_mine_book as book, factor_mine_combo as combo
from .test_book_fill_reality import ZERO_FEES, _panel, _row, CAL


class ValidationTests(unittest.TestCase):
    def test_forward_label_uses_open_and_requires_maturity(self):
        import pandas as pd
        from .book_learn import _fwd_returns
        dates = pd.to_datetime(["2026-08-13", "2026-08-14"])
        panel = pd.DataFrame({"A": [11., 12.]}, index=dates)
        panel.attrs["opens"] = pd.DataFrame({"A": [10., 11.]}, index=dates)
        self.assertAlmostEqual(_fwd_returns(panel, "2026-08-13", 1)["A"], .1)
        self.assertIsNone(_fwd_returns(panel, "2026-08-14", 2))
        self.assertIsNone(_fwd_returns(panel, "2026-08-12", 1))

    def test_quotes_sorted_by_absolute_time_and_timestamp_retained(self):
        events = {"A": [{"timestamp": CAL[0]+"T13:30:06Z", "ask": 10, "ask_size": 5},
                         {"timestamp": CAL[0]+"T09:30:07-04:00", "ask": 20, "ask_size": 5}]}
        result = book.resolve_exec_fill(timestamped_quotes(events), ticker="A", date=CAL[0],
                    side="long", action="entry", official_px=9, intended_shares=5)
        self.assertEqual(result["px"], 10)
        self.assertTrue(result["filled_at"])

    def test_negative_profit_never_kept(self):
        from .open_bell_slip import decide_verdict
        out = decide_verdict(label="loser", n_fires=100, n_filled=100,
                             win_rate=.6, pnl=-10000)
        self.assertFalse(out["keep"])
        self.assertEqual(out["label"], "NEGATIVE_DIAGNOSTIC")

    def test_missing_or_late_provenance(self):
        self.assertTrue(clock_errors({}))
        row = {"decision_at": "2026-08-13T09:30:00-04:00",
               "available_at": "2026-08-13T09:31:00-04:00",
               "observed_at": "2026-08-13T09:20:00-04:00", "input_hash": "abc"}
        self.assertIn("input arrived after decision", clock_errors(row))

    def test_write_once_rejects_reconstruction(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "snap.json"
            write_once(path, {"x": 1})
            write_once(path, {"x": 1})
            with self.assertRaises(ValueError):
                write_once(path, {"x": 2})

    def test_proposal_cannot_self_validate(self):
        with tempfile.TemporaryDirectory() as tmp:
            base, proposed = {"weight": .5}, {"weight": .6}
            result, report = govern(base, proposed, "2026-08-13", root=tmp,
                                    now="2026-08-13T21:00:00Z")
            self.assertEqual(result, base)
            self.assertEqual(report["trials"][0]["status"], "shadow")
            p = next((Path(tmp)/"proposals").glob("*.json"))
            frozen = json.loads(p.read_text())["frozen_at"]
            govern(base, proposed, "2026-08-14", root=tmp, now="2026-08-14T21:00:00Z")
            self.assertEqual(json.loads(p.read_text())["frozen_at"], frozen)

    def test_prospective_evidence_rejects_unmatured_wrong_version_and_duplicate(self):
        row = {"session": "2026-08-14", "decision_at": "2026-08-14T13:30:00Z",
               "available_at": "2026-08-14T13:20:00Z", "observed_at": "2026-08-14T13:20:00Z",
               "outcome_at": "2026-08-14T20:00:00Z", "candidate_hash": "v1", "input_hash": "a",
               "execution_verified": True, "baseline_net": 0., "challenger_net": .01}
        kw = dict(frozen_at="2026-08-13T21:00:00Z", asof="2026-08-14T21:00:00Z", candidate_hash="v1")
        self.assertEqual(promotion_evidence([row, row], **kw)["n_sessions"], 1)
        self.assertTrue(promotion_evidence([dict(row, candidate_hash="v2")], **kw)["rejected"])
        self.assertTrue(promotion_evidence([dict(row, outcome_at="2026-08-15T20:00:00Z")], **kw)["rejected"])

    def test_short_proceeds_are_not_free_long_cash(self):
        pos = {"S": {"shares": 500, "side": "short"}}
        p = risk.policy()
        self.assertEqual(risk.room(15000, pos, lambda *_: 10, "short", p), 0)
        self.assertEqual(risk.room(15000, pos, lambda *_: 10, "long", p), 5000)

    def test_borrow_accrues_weekend_before_partial_cover(self):
        pos = {"S": {"shares": 100, "side": "short", "entry_date": "2026-08-14",
                     "borrow_annual": .365, "fee_in": 0, "cost": 0}}
        ledger = []
        cash = risk.accrue(10000, pos, "2026-08-17", lambda *_: 10, ledger)
        self.assertAlmostEqual(cash, 9997.)
        self.assertEqual(risk.accrue(cash, pos, "2026-08-17", lambda *_: 10, ledger), cash)

    def test_locate_must_precede_open(self):
        p = risk.policy()
        self.assertFalse(risk.locate(p, CAL[0], "S")[0])
        p["borrow"] = {CAL[0]: {"S": {"available": True, "annual_rate": .1,
                                        "observed_at": CAL[0]+"T10:00:00-04:00"}}}
        self.assertFalse(risk.locate(p, CAL[0], "S")[0])

    def test_quote_after_submission_and_shared_size(self):
        events = {"A": [{"timestamp": CAL[0]+"T09:30:01-04:00", "ask": 8, "ask_size": 100},
                         {"timestamp": CAL[0]+"T09:30:06-04:00", "ask": 10, "ask_size": 3}]}
        fill = timestamped_quotes(events)
        kw = dict(ticker="A", date=CAL[0], side="long", action="entry", official_px=9, intended_shares=5)
        got = fill(**kw)
        self.assertEqual((got["px"], got["shares"]), (10, 3))
        self.assertTrue(fill(**kw)["miss"])

    def test_fill_callback_cannot_overfill_or_return_nan(self):
        kw = dict(ticker="A", date=CAL[0], side="long", action="entry", official_px=10, intended_shares=5)
        self.assertEqual(book.resolve_exec_fill(lambda **_: {"px": 10, "shares": 100}, **kw)["shares"], 5)
        self.assertTrue(book.resolve_exec_fill(lambda **_: {"px": float("nan"), "shares": 5}, **kw)["miss"])

    def test_shared_partial_exit_and_financing_audit(self):
        rows = [_row(d, "A", rsi=50, e_pol="neutral", ohlc_ret_1=1) for d in CAL[:1]]
        panel = _panel(rows)
        bars = {("A", d): {"open": .81081, "close": .82} for d in CAL}
        rec = fm.make_recipe("test", universe="union", hold=1, top_n=1, sell="time")
        def fill(**kw):
            out = fixed_bps(10)(**kw)
            if kw["action"] == "exit":
                out["shares"] = max(1, out["shares"]//2)
                out["partial"] = True
            return out
        with patch.object(fm, "flatten_plan", return_value={}):
            b = combo.simulate_shared(panel, [rec], [1], bars=bars, fees=ZERO_FEES,
                 regime={d: {"predict_score": 1} for d in CAL}, exec_fill=fill)
        self.assertTrue(b["audit"]["ok"], b["audit"])
        self.assertGreater(b["n_open"], 0)

    def test_default_blocks_unlocated_short(self):
        panel = _panel([_row(CAL[0], "S", rsi=50, e_pol="neutral", ohlc_ret_1=1)])
        bars = {("S", d): {"open": 10, "close": 10} for d in CAL}
        rec = fm.make_recipe("short", universe="union", hold=1, side="short")
        with patch.object(fm, "flatten_plan", return_value={}):
            b = book.simulate_book(panel, rec, bars=bars, fees=ZERO_FEES,
                                 regime={d: {"predict_score": 1} for d in CAL})
        self.assertEqual(b["n_trades"], 0)
        self.assertTrue(any(x["kind"] == "borrow_unavailable" for x in b["skips"]))


if __name__ == "__main__":
    unittest.main()
