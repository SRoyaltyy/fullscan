"""Append-only paper curve. The pinned ``src/paper_trade.py`` stays a full rebuild.

Printed curve rows stay byte-for-byte. A later run resumes from state.json
and appends only stock-book sessions after the last printed date. The first
curve (no file yet) is still one chronological pass. A session is appended
only when that day's stock book was first committed on main before 09:30 ET.
Fill rules stay in the pinned engine; this module only chooses which sessions
to replay and refuses to rewrite a printed row. Sealed past days are checked
with ``past_day_lock.commit_open_tail_csv``: the newest day stays open, and
a sealed buy/sell day that would change fails closed.
"""
from __future__ import annotations

import argparse
import ast
import csv
import io
import json
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from src import paper_trade as engine

def _sleeve_state(raw: dict | None, capital: float) -> dict:
    src = raw or {}
    pos = {}
    for ticker, lot in (src.get('pos') or {}).items():
        if isinstance(lot, dict):
            pos[str(ticker)] = dict(lot)
    return {'cash': float(src.get('cash', capital)), 'pos': pos, 'realized': float(src.get('realized', 0.0)), 'fees': float(src.get('fees', 0.0)), 'trades': int(src.get('trades', 0)), 'wins': int(src.get('wins', 0)), 'closed': int(src.get('closed', 0))}

def run_sim(books: list[tuple[str, Path]], prices: pd.DataFrame, capital: float, top_n: int, fees: dict, session_ix: dict[str, int] | None=None, initial: dict | None=None, spy0: float | None=None, stop_when_unpriced: bool=False):
    sleeves = [f'{h}_{k}' for h in engine.HORIZONS for k in ('top', 'size')]
    if initial:
        st = {s: _sleeve_state(initial.get(s), capital) for s in sleeves}
    else:
        st = {s: {'cash': capital, 'pos': {}, 'realized': 0.0, 'fees': 0.0, 'trades': 0, 'wins': 0, 'closed': 0} for s in sleeves}
    risk_pol = engine.load_risk_policy()
    curve_rows: list[dict] = []
    trade_rows: list[dict] = []
    date_ix = session_ix or {d: i for i, (d, _) in enumerate(books)}
    for date, path in books:
        day_px = prices.loc[:date]
        if day_px.empty:
            if stop_when_unpriced:
                print(f'[paper] no price on or before {date}; later books stay unprinted', flush=True)
                break
            continue
        px = engine.asof_closes(prices, date)

        def price_of(t: str) -> float | None:
            return engine._finite_positive(px.get(t))
        book = json.loads(path.read_text(encoding='utf-8'))
        picks = engine.picks_from_book(book, top_n)
        day_meta = engine.load_day_meta(date)

        def _fill_meta(ticker: str) -> dict:
            m = day_meta.get(str(ticker).upper(), {}) or {}
            return {'cash_before': None, 'cash_after': round(S['cash'], 2), 'sector': m.get('sector'), 'industry': m.get('industry'), 'ab_score': m.get('ab_score'), 'ab_context': m.get('ab_context'), 'ab_kind': m.get('ab_kind')}
        weather_risk = str((book.get('meta') or {}).get('weather_risk') or '')
        entry_scale = 1.0
        if weather_risk == 'off' and risk_pol['scale'] < 1.0 and (date >= risk_pol['effective']):
            entry_scale = risk_pol['scale']
        try:
            calendar_scale = float((book.get('meta') or {}).get('calendar_entry_scale', 1.0))
            entry_scale = min(entry_scale, max(0.0, min(1.0, calendar_scale)))
        except (TypeError, ValueError):
            pass
        earn_half = {str(t).upper() for t in (book.get('meta') or {}).get('earnings_entry_tickers') or [] if t}
        for sleeve, targets in picks.items():
            S = st[sleeve]
            tset = set(targets)
            horizon = sleeve.split('_')[0]
            min_hold = engine.HOLD_DAYS[horizon]
            for t in list(S['pos']):
                if t in tset:
                    continue
                pos = S['pos'][t]
                held = engine.sessions_held(pos['entry_date'], date, date_ix)
                if held < min_hold:
                    continue
                p = price_of(t)
                if p is None:
                    continue
                pos = S['pos'].pop(t)
                fee = engine.order_fees(pos['shares'], p, 'sell', fees)
                proceeds = pos['shares'] * p - fee
                S['cash'] += proceeds
                pnl = proceeds - pos['cost']
                S['realized'] += pnl
                S['fees'] += fee
                S['trades'] += 1
                S['closed'] += 1
                S['wins'] += 1 if pnl > 0 else 0
                cash_before = round(S['cash'] - proceeds, 2)
                extra = _fill_meta(t)
                extra['cash_before'] = cash_before
                extra['cash_after'] = round(S['cash'], 2)
                trade_rows.append({'date': date, 'sleeve': sleeve, 'ticker': t, 'side': 'sell', 'shares': pos['shares'], 'price': round(p, 4), 'fees': fee, 'amount': round(proceeds, 2), 'realized_pnl': round(pnl, 2), 'reason': f'dropped from {sleeve} after {held} sess (min {min_hold} sess)', **extra})
            new = [t for t in targets if t not in S['pos']]
            if new:
                per = S['cash'] * entry_scale / len(new)
                for t in new:
                    sized = per * (0.5 if str(t).upper() in earn_half else 1.0)
                    p = price_of(t)
                    if p is None or sized <= 0:
                        continue
                    shares = int(sized // p)
                    if shares < 1:
                        continue
                    fee = engine.order_fees(shares, p, 'buy', fees)
                    cost = shares * p + fee
                    if cost > S['cash']:
                        shares = int((S['cash'] - fee) // p)
                        if shares < 1:
                            continue
                        fee = engine.order_fees(shares, p, 'buy', fees)
                        cost = shares * p + fee
                    S['cash'] -= cost
                    S['pos'][t] = {'shares': shares, 'entry_date': date, 'entry_px': p, 'cost': cost}
                    S['fees'] += fee
                    S['trades'] += 1
                    reason = f'entered {sleeve} book'
                    if entry_scale < 1.0:
                        reason += f' (risk/calendar gate: deploying {entry_scale:.0%} of cash)'
                    if str(t).upper() in earn_half:
                        reason += ' (mega-cap earnings: half-size this name)'
                    extra = _fill_meta(t)
                    extra['cash_before'] = round(S['cash'] + cost, 2)
                    extra['cash_after'] = round(S['cash'], 2)
                    trade_rows.append({'date': date, 'sleeve': sleeve, 'ticker': t, 'side': 'buy', 'shares': shares, 'price': round(p, 4), 'fees': fee, 'amount': round(cost, 2), 'realized_pnl': '', 'reason': reason, **extra})
        spy = price_of('SPY')
        if spy and spy0 is None:
            spy0 = spy
        for sleeve in sleeves:
            S = st[sleeve]
            invested = 0.0
            for t, pos in S['pos'].items():
                p = price_of(t)
                invested += pos['shares'] * (p if p else pos['entry_px'])
            curve_rows.append({'date': date, 'sleeve': sleeve, 'equity': round(S['cash'] + invested, 2), 'cash': round(S['cash'], 2), 'invested': round(invested, 2), 'fees_cum': round(S['fees'], 2), 'realized_cum': round(S['realized'], 2)})
        if spy and spy0:
            curve_rows.append({'date': date, 'sleeve': 'SPY (benchmark)', 'equity': round(capital * spy / spy0, 2), 'cash': '', 'invested': '', 'fees_cum': '', 'realized_cum': ''})
    return (st, curve_rows, trade_rows)

_CURVE_COLS = ["date", "sleeve", "equity", "cash", "invested", "fees_cum", "realized_cum"]

def _read_csv_rows(path: Path) -> list[dict]:
    if not path.is_file() or path.stat().st_size == 0:
        return []
    with path.open(newline='', encoding='utf-8') as handle:
        return list(csv.DictReader(handle))

def _csv_columns(path: Path, fallback: list[str]) -> list[str]:
    if not path.is_file():
        return list(fallback)
    with path.open(encoding='utf-8') as handle:
        header = handle.readline().strip()
    cols = [c.strip() for c in header.split(',') if c.strip()]
    return cols or list(fallback)

def _max_date(rows: list[dict]) -> str:
    dates = [str(r.get('date') or '')[:10] for r in rows if r.get('date')]
    return max(dates) if dates else ''

def _spy0_from_curve(rows: list[dict], prices: pd.DataFrame, capital: float) -> float | None:
    first = next((r for r in rows if str(r.get('sleeve') or '').startswith('SPY')), None)
    if not first:
        return None
    try:
        equity = float(first.get('equity') or 0)
    except (TypeError, ValueError):
        return None
    if equity <= 0:
        return None
    px = engine._finite_positive(engine.asof_closes(prices, str(first.get('date') or '')[:10]).get('SPY'))
    if px is None:
        return None
    return capital * px / equity

def committed_before_session_open(when: datetime, session: str) -> bool:
    """True when ``when`` is strictly before 09:30 ET on ``session``."""
    et = ZoneInfo('America/New_York')
    if when.tzinfo is None:
        when = when.replace(tzinfo=ZoneInfo('UTC'))
    local = when.astimezone(et)
    year, month, day = (int(part) for part in str(session)[:10].split('-'))
    deadline = datetime(year, month, day, 9, 30, tzinfo=et)
    return local < deadline

def _repo_is_shallow() -> bool:
    try:
        out = subprocess.run(['git', 'rev-parse', '--is-shallow-repository'], cwd=str(engine.ROOT), capture_output=True, text=True, timeout=15, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return True
    return (out.stdout or '').strip() == 'true'

def first_main_commit_at(rel: str) -> datetime | None:
    """Committer time of the oldest commit on main that touches ``rel``.

    A shallow clone cannot prove that commit, so this returns None
    rather than treating the tip as the first landing.
    """
    rel = str(rel or '').replace('\\', '/').lstrip('/')
    if not rel or _repo_is_shallow():
        return None
    for ref in ('origin/main', 'main'):
        try:
            verify = subprocess.run(['git', 'rev-parse', '--verify', ref], cwd=str(engine.ROOT), capture_output=True, timeout=15, check=False)
        except (OSError, subprocess.TimeoutExpired):
            return None
        if verify.returncode != 0:
            continue
        try:
            rev = subprocess.run(['git', 'rev-list', '--reverse', ref, '--', rel], cwd=str(engine.ROOT), capture_output=True, text=True, timeout=60, check=False)
        except (OSError, subprocess.TimeoutExpired):
            return None
        if rev.returncode != 0:
            continue
        sha = next((line.strip() for line in (rev.stdout or '').splitlines() if line.strip()), '')
        if not sha:
            continue
        try:
            show = subprocess.run(['git', 'show', '-s', '--format=%cI', sha], cwd=str(engine.ROOT), capture_output=True, text=True, timeout=15, check=False)
        except (OSError, subprocess.TimeoutExpired):
            return None
        stamp = (show.stdout or '').strip()
        if show.returncode != 0 or not stamp:
            continue
        try:
            return datetime.fromisoformat(stamp)
        except ValueError:
            return None
    return None

def book_on_main_before_open(session: str) -> bool:
    """The dated stock book was first committed on main before 09:30 ET that day."""
    rel = f'data/stock_book/{str(session)[:10]}_stock_book.json'
    when = first_main_commit_at(rel)
    if when is None:
        return False
    return committed_before_session_open(when, session)

def _eligible_append_books(books: list[tuple[str, Path]]) -> list[tuple[str, Path]]:
    """Drop sessions whose book was not on main before that day's 09:30 ET.

    Later eligible sessions stay in order. No curve row is invented for
    a dropped day; the sim carries the last printed state forward.
    """
    kept: list[tuple[str, Path]] = []
    for session, path in books:
        if book_on_main_before_open(session):
            kept.append((session, path))
            continue
        print(f'missing: book not on main before 09:30 ET {str(session)[:10]}', flush=True)
        record_missed_session(session)
    return kept


def record_missed_session(session: str) -> None:
    """Remember a correctly refused session. Does not append a curve row.

    The record is ``status=missed``. A later append does not change a
    session that is already missed, and it does not write the equity curve.
    """
    day = str(session or '')[:10]
    if len(day) != 10:
        return
    path = engine.PAPER_DIR / 'missed.json'
    try:
        current = json.loads(path.read_text(encoding='utf-8')) if path.is_file() else {}
    except (OSError, json.JSONDecodeError):
        current = {}
    if not isinstance(current, dict):
        current = {}
    sessions = current.get('sessions')
    if not isinstance(sessions, dict):
        sessions = {}
    row = sessions.get(day)
    if not isinstance(row, dict) or row.get('status') != 'missed':
        sessions[day] = {
            'status': 'missed',
            'reason': 'stock book was not on main before 09:30 ET',
        }
    current['sessions'] = sessions
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(current, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    page = engine.DASH_DIR / 'index.html'
    if page.is_file():
        from src.pages_publish_gate import stamp_missed_note
        stamp_missed_note(page, day)

def _open_tickers(state: dict) -> set[str]:
    names = {'SPY'}
    for sleeve in state.values():
        if isinstance(sleeve, dict):
            names.update((str(t) for t in sleeve.get('pos') or {}))
    return names

def _frame_csv(frame: pd.DataFrame) -> str:
    buf = io.StringIO()
    frame.to_csv(buf, index=False)
    return buf.getvalue()

def _commit_csv(record: str, filename: str, new_text: str, column: str) -> None:
    from src import past_day_lock as pdl
    pdl.commit_open_tail_csv(record, engine.PAPER_DIR / filename, new_text, column=column)

def _text_with_appended_rows(path: Path, rows: list[dict], columns: list[str]) -> str:
    """Full CSV text with ``rows`` appended. Does not touch ``path``."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns, lineterminator='\n', extrasaction='ignore')
    for row in rows:
        writer.writerow({c: '' if row.get(c) is None else row.get(c, '') for c in columns})
    blob = buf.getvalue()
    if path.is_file() and path.stat().st_size > 0:
        existing = path.read_text(encoding='utf-8')
        if existing and not existing.endswith('\n'):
            existing += '\n'
        return existing + blob
    header = io.StringIO()
    head = csv.DictWriter(header, fieldnames=columns, lineterminator='\n')
    head.writeheader()
    return header.getvalue() + blob

def _write_fresh(curve_rows, trade_rows, skips) -> None:
    _commit_csv('paper_equity', 'equity_curve.csv', _frame_csv(pd.DataFrame(curve_rows)), 'date')
    _commit_csv('paper_trades', 'trades.csv', _frame_csv(pd.DataFrame(trade_rows)), 'date')
    if skips:
        _commit_csv('paper_skipped', 'skipped.csv', _frame_csv(pd.DataFrame(skips)), 'date')

def _csv_number(value, *, as_int: bool = False):
    if value is None or value == '':
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if as_int and value.is_integer() else value
    text = str(value).strip()
    if text == '':
        return None
    try:
        if as_int or text.lstrip('-').isdigit():
            return int(text)
        return float(text)
    except ValueError:
        return value

def _csv_struct(value):
    if isinstance(value, str) and value[:1] in '[{':
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return value
    return value

def _typed_trade_rows(rows: list[dict]) -> list[dict]:
    """CSV fills use the same types write_dashboard stored on 2026-10-07."""
    out = []
    for row in rows:
        item = dict(row)
        for key in ('shares', 'ab_score'):
            if key in item:
                item[key] = _csv_number(item.get(key), as_int=True)
        for key in ('price', 'fees', 'amount', 'realized_pnl', 'cash_before', 'cash_after'):
            if key in item:
                item[key] = _csv_number(item.get(key))
        if 'ab_trail' in item:
            item['ab_trail'] = _csv_struct(item.get('ab_trail')) or []
        out.append(item)
    return out

def _typed_roundtrip_rows(rows: list[dict]) -> list[dict]:
    """Disk roundtrips match the live page: ab_score stays text, money is numeric."""
    ints = {'shares', 'held_sessions', 'held_cal_days', 'sell_ab_score'}
    floats = {'buy_px', 'sell_px', 'last', 'realized_pnl', 'unrealized_pnl', 'buy_fees', 'sell_fees'}
    out = []
    for row in rows:
        item = dict(row)
        for key in ints:
            if key in item:
                item[key] = _csv_number(item.get(key), as_int=True)
        for key in floats:
            if key in item:
                item[key] = _csv_number(item.get(key))
        for key in ('buy_trail', 'sell_trail'):
            if key in item:
                item[key] = _csv_struct(item.get(key))
        if not str(item.get('sell_date') or '').strip():
            item['sell_date'] = None
            if item.get('sell_ab_context') in ('', None):
                item['sell_ab_context'] = None
            if 'ab_score' in item:
                item['ab_score'] = _csv_number(item.get('ab_score'), as_int=True)
        out.append(item)
    return out

def _stamp_dashboard(asof: str) -> None:
    """Stamp the page write_dashboard just wrote. The engine file stays pinned."""
    from src.pages_publish_gate import stamp_data_asof
    stamp_data_asof(engine.DASH_DIR / 'index.html', asof)

def _restore_bytes(path: Path, before: bytes | None) -> None:
    if before is None:
        if path.is_file():
            path.unlink()
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(before)

def _last_sealed_sell_day(text: str) -> str:
    """Newest sell_date that must not change. The open tail is the newest day."""
    from src import past_day_lock as pdl
    rows = pdl.load_manifest()
    seed = pdl._seed_row(rows, 'paper_roundtrips')
    watermark = str((seed or {}).get('watermark') or '')
    locked = [str(row.get('date') or '') for row in pdl._day_rows(rows, 'paper_roundtrips')]
    locked = [day for day in locked if day]
    if watermark:
        locked.append(watermark)
    if text.strip():
        _header, days, _undated = pdl._split_csv(text, 'sell_date')
        dates = sorted(days)
        if len(dates) >= 2:
            locked.extend(dates[:-1])
        elif len(dates) == 1 and watermark and dates[0] <= watermark:
            locked.append(dates[0])
    return max(locked) if locked else ''

def _dated_roundtrip_lines(text: str) -> dict[str, list[str]]:
    if not text.strip():
        return {}
    from src import past_day_lock as pdl
    _header, days, _undated = pdl._split_csv(text, 'sell_date')
    return {day: list(lines) for day, lines in days.items()}

def _sealed_roundtrip_rows_held(before: bytes | None, after: bytes) -> bool:
    """True when no row dated on or before the last sealed day was edited or added."""
    old = before.decode('utf-8') if before else ''
    new = after.decode('utf-8')
    limit = _last_sealed_sell_day(old)
    if not limit:
        return True
    old_days = _dated_roundtrip_lines(old)
    new_days = _dated_roundtrip_lines(new)
    for day, lines in new_days.items():
        if day <= limit and lines != old_days.get(day):
            return False
    for day, lines in old_days.items():
        if day <= limit and new_days.get(day) != lines:
            return False
    return True

def _commit_roundtrips(new_text: str) -> bool:
    """Write roundtrips, or leave the file untouched when a sealed day would change.

    A past-day lock refusal only re-renders the page. Rows dated on or
    before the last sealed sell day are never modified or added.
    """
    from src import past_day_lock as pdl
    path = engine.PAPER_DIR / 'roundtrips.csv'
    before = path.read_bytes() if path.is_file() else None
    try:
        _commit_csv('paper_roundtrips', 'roundtrips.csv', new_text, 'sell_date')
    except SystemExit as exc:
        if 'past-day lock' not in str(exc) or 'paper_roundtrips' not in str(exc):
            _restore_bytes(path, before)
            raise
        _restore_bytes(path, before)
        print(f'[paper] {exc}; roundtrips file unchanged; refreshing the page', flush=True)
        return False
    if pdl.in_repo(path):
        after = path.read_bytes() if path.is_file() else b''
        if not _sealed_roundtrip_rows_held(before, after):
            _restore_bytes(path, before)
            print('[paper] refused roundtrip write: a row on or before the last sealed day would change', flush=True)
            return False
    return True

def _sleeve_book(state: dict) -> dict:
    out = {}
    for key, sleeve in (state or {}).items():
        if isinstance(sleeve, dict) and 'pos' in sleeve and 'cash' in sleeve:
            out[str(key)] = sleeve
    return out

def _refresh_dashboard_from_disk(books: list[tuple[str, Path]], top_n: int, capital: float, fees: dict) -> None:
    """Render the main page from the printed curve, state, and trades.

    Read-only: equity, trades, roundtrips, and state are not written.
    """
    curve_rows = _read_csv_rows(engine.PAPER_DIR / 'equity_curve.csv')
    if not curve_rows:
        print('[paper] no curve on disk; dashboard not rendered', flush=True)
        return
    state_path = engine.PAPER_DIR / 'state.json'
    if not state_path.is_file():
        print('[paper] state.json missing; dashboard not rendered', flush=True)
        return
    try:
        state = json.loads(state_path.read_text(encoding='utf-8'))
    except json.JSONDecodeError:
        print('[paper] state.json is unreadable; dashboard not rendered', flush=True)
        return
    st = _sleeve_book(state if isinstance(state, dict) else {})
    if not st:
        print('[paper] state.json has no sleeves; dashboard not rendered', flush=True)
        return
    trade_rows = _typed_trade_rows(_read_csv_rows(engine.PAPER_DIR / 'trades.csv'))
    skips = _read_csv_rows(engine.PAPER_DIR / 'skipped.csv')
    trips = _typed_roundtrip_rows(_read_csv_rows(engine.PAPER_DIR / 'roundtrips.csv'))
    last = _max_date(curve_rows)
    tickers = _open_tickers(st)
    last_book = None
    for session, path in books:
        if session <= last:
            last_book = (session, path)
    if last_book is None and books:
        last_book = books[-1]
    if last_book:
        book = json.loads(last_book[1].read_text(encoding='utf-8'))
        for picks in engine.picks_from_book(book, top_n).values():
            tickers.update(picks)
    anchor = str(curve_rows[0].get('date') or '')[:10]
    cache_path = engine.PRICE_CACHE
    cache_before = cache_path.read_bytes() if cache_path.is_file() else None
    try:
        prices = engine.get_prices(sorted(tickers), anchor or last, last)
    finally:
        _restore_bytes(cache_path, cache_before)
    curve = pd.DataFrame(curve_rows)
    for col in ('equity', 'cash', 'invested', 'fees_cum', 'realized_cum'):
        if col in curve.columns:
            curve[col] = pd.to_numeric(curve[col], errors='coerce')
    stats = [engine.sleeve_stats(s, st[s], prices, capital) for s in st]
    last_picks = {}
    if last_book:
        last_picks = engine.picks_from_book(json.loads(last_book[1].read_text(encoding='utf-8')), top_n)
    cal = [d for d, _ in books]
    cal.extend((str(r.get('date') or '')[:10] for r in curve_rows))
    sess_ix = engine.session_index(engine.trading_calendar(prices, cal))
    engine.write_report(stats, last, capital)
    engine.write_dashboard(curve, stats, st, prices, last, capital, fees, trade_rows, last_picks=last_picks, book_dates=[d for d, _ in books], roundtrips=trips, skipped=skips, session_ix=sess_ix)
    _stamp_dashboard(last)
    print(f'[paper] rendered dashboard through {last} from the curve on disk (no rows rewritten)', flush=True)

def run(date: str | None=None, top_n: int=10, capital: float | None=None, render_only: bool=False) -> None:
    fees = engine.load_fees()
    capital = capital or float(fees['paper_account']['starting_capital_per_sleeve'])
    books = engine.list_books()
    if date:
        books = [b for b in books if b[0] <= date]
    if not books:
        raise SystemExit('[paper] no stock books found — run stock_book first')
    engine.PAPER_DIR.mkdir(parents=True, exist_ok=True)
    if render_only:
        _refresh_dashboard_from_disk(books, top_n, capital, fees)
        return
    curve_path = engine.PAPER_DIR / 'equity_curve.csv'
    trades_path = engine.PAPER_DIR / 'trades.csv'
    state_path = engine.PAPER_DIR / 'state.json'
    prior_curve = _read_csv_rows(curve_path)
    prior_trades = _read_csv_rows(trades_path)
    prior_skips = _read_csv_rows(engine.PAPER_DIR / 'skipped.csv')
    last_printed = _max_date(prior_curve)
    if curve_path.is_file() and curve_path.stat().st_size > 0 and (not last_printed):
        raise SystemExit('[paper] equity curve has no dates; refusing to rewrite it')
    resume = bool(last_printed)
    initial = None
    spy0 = None
    if resume:
        if not state_path.is_file():
            raise SystemExit(f'[paper] equity curve is locked through {last_printed} but state.json is missing; refusing to rewrite printed days')
        try:
            initial = json.loads(state_path.read_text(encoding='utf-8'))
        except json.JSONDecodeError as exc:
            raise SystemExit(f'[paper] state.json is unreadable; refusing to rewrite printed days through {last_printed}') from exc
        if not isinstance(initial, dict) or '1d_top' not in initial:
            raise SystemExit('[paper] state.json is not a sleeve book; refusing to rewrite printed days')
        books_to_sim = _eligible_append_books([(d, p) for d, p in books if d > last_printed])
        if not books_to_sim:
            print(f'[paper] curve already through {last_printed}; nothing eligible to append', flush=True)
            _refresh_dashboard_from_disk(books, top_n, capital, fees)
            return
        print(f'[paper] resume after {last_printed}: {len(books_to_sim)} book session(s) to append', flush=True)
    else:
        books_to_sim = books
    tickers = _open_tickers(initial or {})
    for _, path in books_to_sim:
        bk = json.loads(path.read_text(encoding='utf-8'))
        for picks in engine.picks_from_book(bk, top_n).values():
            tickers.update(picks)
    end = books_to_sim[-1][0]
    price_start = books_to_sim[0][0]
    anchor = str(prior_curve[0].get('date') or '')[:10] if prior_curve else price_start
    prices = engine.get_prices(sorted(tickers), anchor or price_start, end)
    if resume:
        spy0 = _spy0_from_curve(prior_curve, prices, capital)
    cal_extra = [d for d, _ in books]
    cal_extra.extend((str(r.get('date') or '')[:10] for r in prior_curve))
    sess_ix = engine.session_index(engine.trading_calendar(prices, cal_extra))
    st, curve_rows, trade_rows = run_sim(books_to_sim, prices, capital, top_n, fees, session_ix=sess_ix, initial=initial, spy0=spy0, stop_when_unpriced=resume)
    if not curve_rows:
        if resume:
            print('[paper] no new priced session to append', flush=True)
            _refresh_dashboard_from_disk(books, top_n, capital, fees)
            return
        raise SystemExit(f'[paper] no price data on/before {books_to_sim[0][0]} — cannot simulate. Check yfinance connectivity.')
    if resume and any((str(r.get('date') or '')[:10] <= last_printed for r in curve_rows)):
        raise SystemExit(f'[paper] refuse to rewrite a printed session through {last_printed}')
    all_trades = prior_trades + trade_rows
    trips = engine.match_roundtrips(all_trades, prices, session_ix=sess_ix, asof=end)
    engine.attach_trails(all_trades, trips)
    new_skips = engine.collect_skips(books_to_sim, prices, trade_rows, top_n, capital, session_ix=sess_ix)
    skips = prior_skips + new_skips
    if resume:
        locked = curve_path.read_bytes()
        _commit_csv('paper_equity', 'equity_curve.csv', _text_with_appended_rows(curve_path, curve_rows, _csv_columns(curve_path, _CURVE_COLS)), 'date')
        if not curve_path.read_bytes().startswith(locked):
            raise SystemExit('[paper] append changed a printed curve row')
        if trade_rows:
            _commit_csv('paper_trades', 'trades.csv', _text_with_appended_rows(trades_path, trade_rows, _csv_columns(trades_path, list(trade_rows[0].keys()))), 'date')
        if new_skips:
            skip_path = engine.PAPER_DIR / 'skipped.csv'
            _commit_csv('paper_skipped', 'skipped.csv', _text_with_appended_rows(skip_path, new_skips, _csv_columns(skip_path, list(new_skips[0].keys()))), 'date')
    else:
        _write_fresh(curve_rows, trade_rows, skips)
    if trips:
        if not _commit_roundtrips(_frame_csv(pd.DataFrame(trips))):
            trips = _typed_roundtrip_rows(_read_csv_rows(engine.PAPER_DIR / 'roundtrips.csv'))
    (engine.PAPER_DIR / 'state.json').write_text(json.dumps(st, indent=2, default=str), encoding='utf-8')
    curve = pd.DataFrame(prior_curve + curve_rows)
    for col in ('equity', 'cash', 'invested', 'fees_cum', 'realized_cum'):
        if col in curve.columns:
            curve[col] = pd.to_numeric(curve[col], errors='coerce')
    stats = [engine.sleeve_stats(s, st[s], prices, capital) for s in st]
    last = str(curve_rows[-1]['date'])[:10]
    last_book = books_to_sim[-1]
    last_picks = engine.picks_from_book(json.loads(last_book[1].read_text(encoding='utf-8')), top_n)
    engine.write_report(stats, last, capital)
    engine.write_dashboard(curve, stats, st, prices, last, capital, fees, all_trades, last_picks=last_picks, book_dates=[d for d, _ in books], roundtrips=trips, skipped=skips, session_ix=sess_ix)
    _stamp_dashboard(last)
    n_closed = sum((1 for t in trips if t['status'] == 'closed'))
    n_open = sum((1 for t in trips if t['status'] == 'open'))
    print(f'[paper] appended {len(curve_rows)} curve rows from {len(books_to_sim)} book(s), {len(trade_rows)} new trades ({n_closed} closed pairs, {n_open} open lots, {len(skips)} not taken), curves → dashboard/index.html, summary → 03_scoreboard/PAPER_TRADING.md')


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--capital", type=float, default=None)
    ap.add_argument("--render-only", action="store_true",
                    help="Rewrite dashboard/index.html from the curve on disk. Do not append.")
    args = ap.parse_args()
    run(date=args.date, top_n=args.top, capital=args.capital, render_only=args.render_only)


if __name__ == "__main__":
    main()
