"""News-impact router: classify mechanism, then family winner/loser analysis."""

from .backtest import run_backtest
from .classify import classify_article, classify_text, rank_articles
from .pipeline import analyze_article, analyze_many, rollup
from .schema import PIPELINE_VERSION

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
