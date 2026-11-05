"""Append scanner-backtest articles after the sealed snapshot.

Sealed ``results.jsonl`` lines stay byte-identical. A later intake only
adds articles whose id is not already scored and whose published day is
after the sealed cutoff. This does not rescore a sealed row.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def assert_sealed_prefix(sealed: str, current: str) -> None:
    """Fail when a fill-forward write changed any sealed result byte."""
    if not sealed:
        return
    if not current.startswith(sealed):
        raise ValueError('fill-forward rewrote a sealed result line')


def _docs(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding='utf-8'))
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        rows = data.get('documents') or data.get('rows') or []
        return [row for row in rows if isinstance(row, dict)]
    return []


def _day(value) -> str:
    text = str(value or '')[:10]
    if len(text) == 10 and text[4] == '-' and text[7] == '-':
        return text
    return ''


def sealed_ids(results_path: Path) -> set[str]:
    if not results_path.is_file():
        return set()
    out = set()
    for line in results_path.read_text(encoding='utf-8').splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get('id') is not None:
            out.add(str(row['id']))
    return out


def prepare_forward_news(base_docs: list[dict], later: list[tuple[str, list[dict]]],
                         already: set[str], *, after: str, through: str) -> list[dict]:
    """Canonical docs, unchanged, plus new articles in ``(after, through]``.

    ``later`` is ``(folder_or_published_hint, documents)``. An article is
    new only when its id is absent and its published day is inside the
    window. Base documents are not edited and are not replaced.
    """
    after = _day(after)
    through = _day(through)
    have = set(already)
    out = [dict(doc) for doc in base_docs]
    for doc in out:
        if doc.get('id') is not None:
            have.add(str(doc['id']))
    added: list[dict] = []
    for _hint, docs in later:
        for doc in docs:
            published = _day(doc.get('published_at') or doc.get('date'))
            if not published or published <= after or published > through:
                continue
            ident = str(doc.get('id') or '')
            if not ident or ident in have:
                continue
            added.append(dict(doc))
            have.add(ident)
    added.sort(key=lambda doc: (_day(doc.get('published_at')), str(doc.get('id'))))
    return out + added


def earliest_published_day(docs: list[dict], base_count: int) -> list[dict]:
    """Keep the sealed base plus the earliest new published day only.

    One run must not dump every later intake into one snapshot. The
    publication budget is 99MB per file, and a three-week pool does not fit.
    """
    added = docs[base_count:]
    if not added:
        return docs
    first = _day(added[0].get('published_at') or added[0].get('date'))
    kept = [doc for doc in added if _day(doc.get('published_at') or doc.get('date')) == first]
    return docs[:base_count] + kept


def prepare_file(base: Path, intake_root: Path, results: Path, *,
                 after: str, through: str, out: Path, one_day: bool = False) -> int:
    later = []
    if intake_root.is_dir():
        for path in sorted(intake_root.glob('*/documents.json')):
            if path.resolve() == base.resolve():
                continue
            later.append((path.parent.name, _docs(path)))
    base_docs = _docs(base)
    docs = prepare_forward_news(base_docs, later, sealed_ids(results), after=after, through=through)
    if one_day:
        docs = earliest_published_day(docs, len(base_docs))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(docs, ensure_ascii=False), encoding='utf-8')
    added = len(docs) - len(base_docs)
    print(f'fill-forward news {len(docs)} articles ({added} new) -> {out}')
    return added


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest='cmd', required=True)
    prep = sub.add_parser('prepare')
    prep.add_argument('--base', type=Path, required=True)
    prep.add_argument('--intake-root', type=Path, required=True)
    prep.add_argument('--sealed', type=Path, required=True)
    prep.add_argument('--after', required=True)
    prep.add_argument('--through', required=True)
    prep.add_argument('--out', type=Path, required=True)
    prep.add_argument('--one-day', action='store_true')
    check = sub.add_parser('check-prefix')
    check.add_argument('--sealed', type=Path, required=True)
    check.add_argument('--current', type=Path, required=True)
    args = parser.parse_args(argv)
    if args.cmd == 'prepare':
        prepare_file(args.base, args.intake_root, args.sealed, after=args.after, through=args.through, out=args.out, one_day=args.one_day)
        return 0
    assert_sealed_prefix(args.sealed.read_text(encoding='utf-8'), args.current.read_text(encoding='utf-8'))
    print('sealed results prefix held')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
