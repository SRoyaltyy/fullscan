"""News-impact router: classify mechanism, then family winner/loser analysis."""

from importlib import import_module

from .schema import PIPELINE_VERSION


def __getattr__(name):
    # Schema/coverage consumers must not import web-search or paid clients.
    modules = {"run_backtest": "backtest", "classify_article": "classify",
               "classify_text": "classify", "rank_articles": "classify",
               "analyze_article": "pipeline", "analyze_many": "pipeline", "rollup": "pipeline"}
    if name not in modules:
        raise AttributeError(name)
    value = getattr(import_module(f".{modules[name]}", __name__), name)
    globals()[name] = value
    return value

__all__ = [
    "PIPELINE_VERSION",
    "analyze_article",
    "analyze_many",
    "classify_article",
    "classify_text",
    "rank_articles",
    "rollup",
    "run_backtest",
]
