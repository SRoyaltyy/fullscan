"""Which forward book a run writes.

Holdup is the default, so existing callers keep the sealed holdup log.
h1 is the same study's other prereg recipe, in its own directory. A rule
that is not the prereg recipe is not switched in place: it would be a new
study name and a new log.
"""
from __future__ import annotations

import os
from contextvars import ContextVar, Token
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
STUDY = ROOT / "research" / "hot_n4_clean_v4"


@dataclass(frozen=True)
class Book:
    recipe: str
    folder_name: str
    page_name: str
    log_name: str

    @property
    def folder(self) -> Path:
        return STUDY / self.folder_name

    @property
    def page(self) -> Path:
        return ROOT / "dashboard" / self.page_name


HOLDUP = Book(
    recipe="union_hot_n4_holdup__w0",
    folder_name="forward",
    page_name="holdup",
    log_name="holdup_log.jsonl",
)
H1 = Book(
    recipe="union_hot_n4_h1__w0",
    folder_name="forward_h1",
    page_name="h1",
    log_name="h1_log.jsonl",
)
BOOKS = (HOLDUP, H1)

_CURRENT: ContextVar[Book | None] = ContextVar("forward_book", default=None)


def current_book() -> Book:
    """The book for this run.

    ``use_book`` wins. Otherwise ``FORWARD_BOOK`` (``holdup`` or ``h1``).
    Unset means holdup, which is the book already on main.
    """
    chosen = _CURRENT.get()
    if chosen is not None:
        return chosen
    name = os.environ.get("FORWARD_BOOK", "holdup")
    if name == "h1":
        return H1
    if name != "holdup":
        raise RuntimeError(f"FORWARD_BOOK {name}")
    return HOLDUP


def use_book(book: Book) -> Token:
    return _CURRENT.set(book)


def reset_book(token: Token) -> None:
    _CURRENT.reset(token)
