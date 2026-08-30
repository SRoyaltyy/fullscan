"""One-time generator for engine/.parse_cache.pkl.

Parses every formula in model.json once and pickles the {text: AST} map so
every daily_run worker process starts with a warm parse cache (saves ~5s of
re-parsing per worker per chunk). Re-run this if engine/model.json changes.

Usage:  python engine/warm_parse_cache.py
"""
import json
import os
import pickle
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from xlparse import parse, ParseError  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
MODEL = os.path.join(HERE, "model.json")
OUT = os.path.join(HERE, ".parse_cache.pkl")


def main():
    formulas = json.load(open(MODEL))["formulas"]
    cache, bad = {}, 0
    for coord, f in formulas.items():
        text = f.get("f") if isinstance(f, dict) else f
        if not text or text in cache:
            continue
        try:
            cache[text] = parse(text)
        except ParseError:
            bad += 1
    with open(OUT, "wb") as fh:
        pickle.dump(cache, fh, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"warmed {len(cache)} unique formulas ({bad} unparseable, "
          f"left to on-demand handling) -> {OUT}")


if __name__ == "__main__":
    main()
