"""Compatibility bridge for remote company-archive ZIP downloads.

This module provides the import expected by scripts/stock_parser_request.py.
It reuses the repository's verified artifact-download helpers rather than
re-implementing artifact access in a different path.
"""
from __future__ import annotations

import zipfile
from typing import Any

from scripts.materialize_filing_texts import RangeReader, http_fetcher, signed_artifact_url


class RemoteZipArchive:
    """A zipfile.ZipFile wrapper around a GitHub Actions artifact ZIP.

    The stock parser workflow uses this object exactly like a local zip archive:
    `with remote_zip(archive_id) as source: source.read(...)
    `
    """

    def __init__(self, artifact_id: int | str, repository: str = "SRoyaltyy/fullscan"):
        artifact_id = int(artifact_id)
        url = signed_artifact_url(repository, artifact_id)
        reader = RangeReader(http_fetcher(url))
        self._zip = zipfile.ZipFile(reader)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def close(self):
        self._zip.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._zip, name)

    def read(self, name: str, *args, **kwargs):
        return self._zip.read(name, *args, **kwargs)

    def open(self, name: str, mode: str = "r", *args, **kwargs):
        return self._zip.open(name, mode, *args, **kwargs)


def remote_zip(artifact_id: int | str, repository: str = "SRoyaltyy/fullscan") -> RemoteZipArchive:
    """Return a zip archive backed by the signed GitHub artifact URL."""
    return RemoteZipArchive(artifact_id, repository=repository)
