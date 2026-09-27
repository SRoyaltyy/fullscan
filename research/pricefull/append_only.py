"""Refuse a rewrite of a pricefull scoring file that was already written."""
from __future__ import annotations

from pathlib import Path


class AppendOnlyError(Exception):
    """An existing scoring file would have been changed."""


def write_once(path: Path, data: bytes) -> None:
    """Create ``path`` or leave identical bytes in place.

    A different existing body is an error. The caller does not delete it.
    """
    if path.exists():
        current = path.read_bytes()
        if current != data:
            raise AppendOnlyError(f"refusing to rewrite {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def assert_bytes_extend(base: bytes, head: bytes) -> None:
    """``head`` may only add a suffix. A shorter or edited body fails."""
    if head == base:
        return
    if base and not head.startswith(base):
        raise AppendOnlyError("scoring bytes changed")
    if len(head) < len(base):
        raise AppendOnlyError("scoring bytes removed")
