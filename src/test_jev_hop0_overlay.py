"""Overlay formula: keep bits beat tape/soft; spoke is a keep bit."""
from src.jev_hop0_overlay import decide, formula_reason, SIX_BITS, BIT_QUESTIONS


def _ans(**fired):
    out = {key: 0.05 for key in BIT_QUESTIONS}
    out.update(fired)
    return out


def test_six_bits_are_spoke_not_listed():
    assert SIX_BITS == ("tape", "soft", "tip", "done", "print", "spoke")
    assert "spoke" in BIT_QUESTIONS and "listed" not in BIT_QUESTIONS


def test_formula():
    assert formula_reason(_ans(done=0.9)) == ("keep", "done")
    assert formula_reason(_ans(print=0.9)) == ("keep", "print")
    assert formula_reason(_ans(spoke=0.9)) == ("keep", "spoke")
    assert formula_reason(_ans(done=0.9, tape=0.9)) == ("keep", "done")
    assert formula_reason(_ans(print=0.9, soft=0.9)) == ("keep", "print")
    assert formula_reason(_ans(spoke=0.9, soft=0.9)) == ("keep", "spoke")
    assert formula_reason(_ans(print=0.9, tip=0.9)) == ("drop", "tip")


def test_spoke_beats_soft_on_fed_president():
    got = decide(
        {"title": "New York Fed president sees no need to rush another rate hike"},
        _ans(spoke=0.88, soft=0.81),
    )
    assert got["decision"] == "keep" and got["reason"] == "spoke"


def test_done_beats_tape_wrapper():
    got = decide(
        {"title": "Oil Prices React as Trump Denies Easing Sanctions on Iran"},
        _ans(done=0.84, tape=0.90),
    )
    assert got["decision"] == "keep" and got["reason"] == "done"


def test_tip_still_vetoes_print():
    got = decide(
        {"title": "3 Financial Mutual Funds to Consider as Fed Signals More Rate Hikes"},
        _ans(tip=0.93, print=0.80),
    )
    assert got["decision"] == "drop" and got["reason"] == "tip"


def main():
    test_six_bits_are_spoke_not_listed()
    test_formula()
    test_spoke_beats_soft_on_fed_president()
    test_done_beats_tape_wrapper()
    test_tip_still_vetoes_print()
    print("ok overlay")


if __name__ == "__main__":
    main()
