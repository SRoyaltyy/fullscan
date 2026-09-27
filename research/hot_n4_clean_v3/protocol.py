"""Locked rules for hot_n4_clean_v3. No score is imported here.

This module does not walk a book, does not read a return, and does not
write a day file into the study folder. ``commit_day`` is the writer a
later run must use. A test calls it only in a temporary directory.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from research.concentration_cap_v3.protocol import LUCK_N as CAP_V3_LUCK_N
from research.hot_n4_clean_v1.protocol import LUCK_N as V1_LUCK_N
from research.hot_n4_clean_v2.protocol import LUCK_N as V2_LUCK_N

ROOT = Path(__file__).resolve().parents[2]
STUDY = "hot_n4_clean_v3"
HERE = ROOT / "research" / STUDY
PREREG = HERE / "PREREG.md"
SPLITS_MD = HERE / "SPLITS.md"
SPLITS_MD_SHA256 = "024443b1f994c81c3058c47cb535e8a1250a0c058b16d441d03531236c68f1ab"
JUMP_SCAN = HERE / "JUMP_SCAN.csv"
JUMP_SCAN_SHA256 = "52ec0d95f5295d0b5005de786bcb58519ec66ef7305639dd842279e15ea4155e"
REAL_MOVES = HERE / "REAL_MOVES.csv"
REAL_MOVES_SHA256 = "03e5c6704a6d3af65810080cf90e4017e784626e55ee4e397e0ceda87156c149"
DAYS = HERE / "days"
LEDGER_NAME = "LEDGER.jsonl"
MARKER = "<!-- BEGIN COVERED -->\n"

CAPITAL = 10_000.0
HOLDUP_S = 0.0
HOLDUP_SESS = 2
TOP_N = 4
HOLD = 1
RANDOM_SEED = 20260813
RANDOM_DRAWS = 1000
RANDOM_N = 4
MIN_TRADES = 30
REJECT_JOINT = 0.5
BP_SIDE = 0.000075
OPEN_UTC = "13:30"
CREATION = "2026-09-27"
N_VARIANTS = 2

# v2 locks 22,013. This study adds the same two recipes again. Keep-held and
# the flat 15bp print are one try. Renew is not a third try.
LUCK_N = V2_LUCK_N + N_VARIANTS
if CAP_V3_LUCK_N != 22009 or V1_LUCK_N != 22011 or V2_LUCK_N != 22013 or LUCK_N != 22015:
    raise RuntimeError("luck N")

TUNE = (
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19",
    "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26",
    "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02",
    "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10",
    "2026-09-11",
)
FORWARD = (
    "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18",
    "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-25",
)
SESSIONS = TUNE + FORWARD
STARTS = ("2026-08-17", "2026-08-24", "2026-08-31")
TUNE_END = "2026-09-11"
FORWARD_START = "2026-09-14"
GAP_DAY = "2026-08-28"

# Yahoo split-adjusted prints are not what the price store committed.
# data/prices/ohlc.parquet is the raw session print (price_store AUTO_ADJUST
# False). A split is a level jump. Returns and ratios are computed only after
# the split adjustment in section 5 of PREREG.md.
OHLC_PATH = "data/prices/ohlc.parquet"
OHLC_SHA256 = "559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55"
OHLC_BLOB = "3456f7f489a6fa7033e8ae5cc942d8279f0113e3"

# The live actions file has no nonzero split in this window. The split
# factors used here are the retro table.
ACTIONS_PATH = "data/prices/actions.parquet"
ACTIONS_SHA256 = "0471b2d76c30960eb494134a7bd34eea499bb5c696190f5fb3628c4becd94e4a"
ACTIONS_BLOB = "ebbda85df7e4ee81f78adf04208ca9d5ea148c48"
RETRO_ACTIONS_PATH = "data/factor_mine/retro_prices/actions.parquet"
RETRO_ACTIONS_SHA256 = "9007c3aec0db3fa42735bf10ad8611e19ba78a2d532cec0df8f99205fbd6b633"
RETRO_ACTIONS_BLOB = "b583117809d0ccac78b766084d5cb04ca6d6dec3"

FEES_PATH = "00_grounding/futubull_fees.json"
FEES_SHA256 = "019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3"
FEES_BLOB = "ab966d09cab6dcb55f3dfdc4ca17c8c81c099605"

FULLSCAN_COMMIT = "4d964db7a8c975ff5151a859577a9d34d993e6ca"
PROVENANCE_SHA256 = "cafff6a314723ed14b9d9cdf9b52658129981421dcd3a22e3a8945960fe68569"
FULLSCAN_CSV_SHA256 = "7b60805ca3260f783313737f0e35c3d9010f50092a1c88fc500242c2d39a812a"
FULLSCAN_MD_SHA256 = "8c81c4c507a7b09982d9ea08fb50cabaae6aa5039c25f21dd7c11d85727a3c1a"
REBUILD_MATCH_SHA256 = "21f69d69095f3b8b5c5db92fd63327b8e25903ce9e7fb96867047f4affffc72a"

THEME_RADAR_REPO = "https://github.com/SRoyaltyy/theme-radar"
THEME_RADAR_HEAD = "b5a324f8531af33c1f3879f64992f505d71fa18b"
THEME_RADAR_PANEL_COMMIT = "3973e13cd953e5705d08d8d9f78a5b1b9dd1a1d0"
AUG_GZ = "research/lever_panel/finviz_panel_asof0930_2026-08.csv.gz"
SEP_GZ = "research/lever_panel/finviz_panel_asof0930_2026-09.csv.gz"
AUG_SHA256 = "c8977b8eea8e74115899e9d4cc04d5b4ea67490376d972905781eb8e1aeb6459"
SEP_SHA256 = "cbf35da9e1587703059abd9ff77525a1047c67a91edc3276ca93db4cd8669c16"
AUG_BLOB = "762462c4a6d11ec7dea49fca3f9bfcc11e712393"
SEP_BLOB = "1f2da45043200e0100c33e97989a9244e760bbf0"
AUG_BYTES = 55836833
SEP_BYTES = 71452629

ENGINE_SHA256 = {
    "src/factor_mine.py": "ce4f1954b0c5e97009dedf6c2d7604d8c225a5633c96090d6e2b20f9e04a9272",
    "src/factor_mine_book.py": "1fc16961b2680ea7fa2b4f6939402176b27f20c38d0ceda85826d674f4ecb76a",
    "src/ohlc_ripper.py": "7b3d674b2f4b5f5e7c52331543b7e0abf24219c3a3d0f2a178476842783bf641",
    "src/gainer_capture.py": "ee03ea4d5cfad51dddcf3dc24b434b63fd19169ad0aacf657d505f5c5da82af5",
    "src/gainer_asof.py": "43b36e5a17b7ffb07ddbee6ecb7b047a03281513fd9670d1828abf427e27f51e",
    "src/paper_trade.py": "54e70b314dc0b959b45573343a234f46bb396588ed7f65dff678d79c88d23f9d",
    "src/ticker_lookback.py": "1e08f2f42c732407f834847f2e347b0218d8a18b2612a2ee565f24bcc6a2731e",
}

# IRONCLAD jump check, src/breadth_rank_v1c_bars.py.
RATIO_HI = 3.0
RATIO_LO = 1.0 / 3.0
SPLIT_TOL = 0.25
# A matching split may be recorded after the jumping bar. Five NYSE sessions
# covers REAX (jump 2026-08-20, recorded ex-date 2026-08-25).
SPLIT_DATE_SLACK_SESSIONS = 5

# Candidate-universe splits: liquid at least once, nonzero retro split
# with ex-date inside the session window. share_factor is the stored Yahoo
# column. price_factor is 1/share_factor. boundary is the first stored bar
# that already shows the new price level when a 3x leg matches, otherwise
# the recorded ex-date.
# (ex_date, ticker, share_factor, boundary)
# MVIS and AEHL are primary-source rows. Their ex-dates are before the
# session window. The jumping bar is inside a feature window this study reads.
SPLITS = (
    ("2026-08-03", "MVIS", 0.06666666666666667, "2026-08-03"),
    ("2026-08-10", "AEHL", 0.0625, "2026-08-10"),
    ("2026-08-14", "BYND", 0.03333333333333333, "2026-08-17"),
    ("2026-08-17", "AVB", 2.793, "2026-08-17"),
    ("2026-08-19", "NRDY", 0.06666666666666667, "2026-08-17"),
    ("2026-08-21", "SFBS", 2.0, "2026-08-21"),
    ("2026-08-24", "BRCC", 0.1, "2026-08-20"),
    ("2026-08-25", "REAX", 0.1, "2026-08-20"),
    ("2026-09-01", "RUSHA", 1.5, "2026-09-01"),
    ("2026-09-03", "APH", 2.0, "2026-09-03"),
    ("2026-09-08", "IGR", 0.3333333333333333, "2026-09-08"),
)

# >3x legs with a primary filing in the five-session window that shows no share
# change. Bars stay as stored. The leg does not halt.
# (ticker, bar_date, leg, previous_stored_bar, filing_type, filing_date, accession)
REAL_MOVE_LEGS = (
    ("SION", "2026-08-10", "open_over_prev_close", "2026-08-07", "8-K", "2026-08-10", "0001193125-26-341230"),
    ("SION", "2026-08-10", "close_over_prev_close", "2026-08-07", "8-K", "2026-08-10", "0001193125-26-341230"),
    ("CAPR", "2026-07-27", "open_over_prev_close", "2026-07-24", "8-K", "2026-07-29", "0001104659-26-087891"),
    ("EYPT", "2026-08-17", "open_over_prev_close", "2026-08-14", "8-K", "2026-08-17", "0001193125-26-353308"),
    ("EYPT", "2026-08-17", "close_over_prev_close", "2026-08-14", "8-K", "2026-08-17", "0001193125-26-353308"),
    ("XHLD", "2026-08-06", "close_over_open", "2026-08-06", "8-K", "2026-08-10", "0001493152-26-036864"),
    ("XHLD", "2026-08-06", "close_over_prev_close", "2026-08-05", "8-K", "2026-08-10", "0001493152-26-036864"),
    ("ADBT", "2026-09-03", "close_over_open", "2026-09-03", "8-K", "2026-09-02", "0001493152-26-041157"),
    ("ADBT", "2026-09-03", "close_over_prev_close", "2026-09-02", "8-K", "2026-09-02", "0001493152-26-041157"),
    ("VRRM", "2026-05-27", "close_over_prev_close", "2026-05-26", "8-K", "2026-05-26", "0001193125-26-239351"),
    ("MPLT", "2026-07-27", "close_over_prev_close", "2026-07-24", "8-K", "2026-07-27", "0001193125-26-316820"),
    ("SMJF", "2026-08-27", "close_over_open", "2026-08-27", "6-K", "2026-08-28", "0001213900-26-094830"),
    ("SMJF", "2026-08-27", "close_over_prev_close", "2026-08-26", "6-K", "2026-08-28", "0001213900-26-094830"),
)

# 3x legs that still halt: no primary filing in the window, or a filing that
# shows a share change this study did not verify as the split.
# (ticker, bar_date, leg, previous_stored_bar, first_halt_session)
# first_halt_session is "" when the name does not enter the candidate list on
# any session that loads the bar. This prereg holds no book.
UNEXPLAINED = (
    ("XHG", "2026-08-13", "open_over_prev_close", "2026-08-12", "2026-08-19"),
    ("XHG", "2026-08-13", "close_over_prev_close", "2026-08-12", "2026-08-19"),
    ("JLHL", "2026-07-09", "close_over_open", "2026-07-09", "2026-09-03"),
    ("JLHL", "2026-07-09", "close_over_prev_close", "2026-07-08", "2026-09-03"),
    ("SLBT", "2026-06-16", "open_over_prev_close", "2026-06-15", "2026-09-03"),
    ("MFP", "2026-06-29", "open_over_prev_close", "2026-06-26", ""),
    ("MFP", "2026-06-29", "close_over_prev_close", "2026-06-26", ""),
    ("MB", "2026-08-07", "open_over_prev_close", "2026-08-06", ""),
)
# First session the jump check halts. XHG enters the candidate list on 2026-08-19.
FIRST_HALT_SESSION = "2026-08-19"

# Not in the candidate universe. Listed so a later run does not add them.
EXCLUDED_SPLITS = (
    ("2026-08-27", "YMT", 0.0625, "v1c splits.json only; market cap stays under 100"),
)
NO_BAR = ("AAC-U", "HYAC-U", "PNAQ-U")

# Rule 9. Primary path is Futubull fees plus this slip per side.
# 0 and 0.01 are sensitivity prints of the same shares. Not extra luck tries.
SLIP_PRIMARY = 0.005
SLIP_LINES = (0.0, 0.005, 0.01)
LIQ_CAP_FRAC = 0.01
# Finviz Average Volume is thousands of shares. Dollar ADV is Price * ADV * 1000.
ADV_SHARE_SCALE = 1000
WILSON_Z = 1.96
MISSING_BARS_CSV = "research/hot_n4_clean_v3/MISSING_BARS.csv"
MISSING_BARS_SHA256 = "75e2778152e04cb0c0e059aca0c7c7c2aa4cdaf1b512260c4c7f6c8afeeb82b0"
# ticker-days on the frozen record with no pinned bar dated on or before D.
# 2026-08-28 has no frozen row and is not a line.
MISSING_COUNTS = (
    ("2026-08-13", 21, 3),
    ("2026-08-14", 20, 3),
    ("2026-08-17", 19, 3),
    ("2026-08-18", 19, 3),
    ("2026-08-19", 19, 3),
    ("2026-08-20", 19, 3),
    ("2026-08-21", 19, 3),
    ("2026-08-24", 20, 2),
    ("2026-08-25", 19, 2),
    ("2026-08-26", 18, 2),
    ("2026-08-27", 23, 1),
    ("2026-08-31", 21, 1),
    ("2026-09-01", 21, 1),
    ("2026-09-02", 20, 1),
    ("2026-09-03", 19, 1),
    ("2026-09-04", 18, 0),
    ("2026-09-08", 18, 0),
    ("2026-09-09", 14, 0),
    ("2026-09-10", 21, 0),
    ("2026-09-11", 38, 0),
    ("2026-09-14", 42, 0),
    ("2026-09-15", 48, 0),
    ("2026-09-16", 65, 0),
    ("2026-09-17", 72, 0),
    ("2026-09-18", 83, 0),
    ("2026-09-21", 86, 1),
    ("2026-09-22", 88, 1),
    ("2026-09-23", 94, 1),
    ("2026-09-24", 111, 1),
    ("2026-09-25", 119, 1),
)
# Observed paper fills. (session, ticker, side, avg_fill_px, official_open)
# Source: data/paper_open/2026-09-21_status.json sent rows with a fill price.
PAPER_FILL_SHA256 = "0a88a810ec35a11dd94dc641edb8ef328c67b1e226c3697b7be50f89ec70050e"
PAPER_FILLS = (
    ("2026-09-21", "DELL", "BUY", 587.89, 586.77001953125),
    ("2026-09-21", "GME", "BUY", 22.9, 22.780000686645508),
    ("2026-09-21", "UMC", "BUY", 24.99, 24.93000030517578),
)

# Candidate names that leave the frozen panel before 2026-09-25 and do not
# return. last_bar is the last pinned ohlc date in August–September 2026,
# or None when the price store has no bar. This is the in-window delist list.
# (ticker, last_panel, last_bar)
DELISTED = (
    ("BBBY", "2026-08-17", "2026-08-14"),
    ("EWAVU", "2026-08-17", "2026-08-14"),
    ("THEOU", "2026-08-17", "2026-08-14"),
    ("EQR", "2026-08-18", "2026-08-14"),
    ("CGCFU", "2026-08-24", "2026-08-21"),
    ("OSPRU", "2026-08-24", "2026-08-21"),
    ("AVB", "2026-08-31", "2026-08-21"),
    ("JAB", "2026-08-31", "2026-08-21"),
    ("TALK", "2026-08-31", "2026-08-18"),
    ("HLX", "2026-09-02", "2026-08-21"),
    ("AAC-U", "2026-09-08", None),
    ("FBRX", "2026-09-08", "2026-08-21"),
    ("JONEU", "2026-09-08", "2026-08-21"),
    ("LBRDK", "2026-09-08", "2026-08-19"),
    ("LEG", "2026-09-08", "2026-08-21"),
    ("NSAIU", "2026-09-08", "2026-08-21"),
    ("TWO", "2026-09-08", "2026-08-21"),
    ("WBS", "2026-09-08", "2026-08-20"),
    ("XTERU", "2026-09-08", "2026-08-21"),
    ("BCAR", "2026-09-14", "2026-09-11"),
    ("BRTMU", "2026-09-14", "2026-09-08"),
    ("CRNX", "2026-09-14", "2026-09-02"),
    ("OCLTU", "2026-09-14", "2026-09-08"),
    ("APGE", "2026-09-21", "2026-09-02"),
    ("CATLU", "2026-09-21", "2026-09-08"),
    ("DUKU", "2026-09-21", "2026-09-08"),
    ("MTAKU", "2026-09-21", "2026-09-08"),
    ("TLACU", "2026-09-21", "2026-09-08"),
    ("XIIIU", "2026-09-21", "2026-09-08"),
    ("BRR", "2026-09-22", "2026-09-11"),
    ("DOMO", "2026-09-24", "2026-09-11"),
)

PRICE_SOURCES = ("ohlc_hot", "probable", "yday_gainer", "yday_mover")
ALWAYS_ABSENT = (
    "earn_react", "flatten", "mover_buy", "overnight", "overnight_mega",
)
ALL_SOURCES = (
    "ohlc_hot", "probable", "yday_gainer", "yday_mover",
    "earn_react", "overnight", "overnight_mega", "flatten", "mover_buy",
)

# Morning S. Predict first when that row is proven and before_0930, else
# weather. Every session has one. PROVEN_BUT_CHANGED counts. The time is the
# pre-open blob, not a later edit.
# (session, kind, status, server_time_utc)
S_SOURCE = (
    ("2026-08-13", "predict", "PROVEN", "2026-08-13T12:10:30Z"),
    ("2026-08-14", "predict", "PROVEN", "2026-08-14T12:09:44Z"),
    ("2026-08-17", "predict", "PROVEN", "2026-08-17T11:51:24Z"),
    ("2026-08-18", "predict", "PROVEN", "2026-08-18T11:53:13Z"),
    ("2026-08-19", "predict", "PROVEN", "2026-08-19T11:52:33Z"),
    ("2026-08-20", "predict", "PROVEN", "2026-08-20T11:55:52Z"),
    ("2026-08-21", "predict", "PROVEN", "2026-08-21T11:54:18Z"),
    ("2026-08-24", "predict", "PROVEN_BUT_CHANGED", "2026-08-24T11:27:45Z"),
    ("2026-08-25", "weather", "PROVEN", "2026-08-25T11:16:03Z"),
    ("2026-08-26", "predict", "PROVEN", "2026-08-26T11:25:52Z"),
    ("2026-08-27", "weather", "PROVEN_BUT_CHANGED", "2026-08-27T06:29:13Z"),
    ("2026-08-28", "predict", "PROVEN", "2026-08-28T08:16:51Z"),
    ("2026-08-31", "predict", "PROVEN_BUT_CHANGED", "2026-08-31T11:09:45Z"),
    ("2026-09-01", "predict", "PROVEN", "2026-09-01T13:13:03Z"),
    ("2026-09-02", "predict", "PROVEN", "2026-09-02T13:29:24Z"),
    ("2026-09-03", "predict", "PROVEN", "2026-09-03T11:25:21Z"),
    ("2026-09-04", "predict", "PROVEN", "2026-09-04T12:21:59Z"),
    ("2026-09-08", "weather", "PROVEN_BUT_CHANGED", "2026-09-08T12:34:01Z"),
    ("2026-09-09", "weather", "PROVEN_BUT_CHANGED", "2026-09-09T13:04:53Z"),
    ("2026-09-10", "predict", "PROVEN", "2026-09-10T10:32:14Z"),
    ("2026-09-11", "predict", "PROVEN", "2026-09-11T09:12:05Z"),
    ("2026-09-14", "predict", "PROVEN", "2026-09-14T08:27:06Z"),
    ("2026-09-15", "predict", "PROVEN_BUT_CHANGED", "2026-09-15T08:32:39Z"),
    ("2026-09-16", "predict", "PROVEN", "2026-09-16T08:32:10Z"),
    ("2026-09-17", "predict", "PROVEN", "2026-09-17T08:38:40Z"),
    ("2026-09-18", "predict", "PROVEN", "2026-09-18T08:37:27Z"),
    ("2026-09-21", "predict", "PROVEN", "2026-09-21T08:40:31Z"),
    ("2026-09-22", "predict", "PROVEN", "2026-09-22T08:39:43Z"),
    ("2026-09-23", "predict", "PROVEN", "2026-09-23T08:39:57Z"),
    ("2026-09-24", "predict", "PROVEN", "2026-09-24T10:08:33Z"),
    ("2026-09-25", "predict", "PROVEN", "2026-09-25T08:44:07Z"),
)
WEATHER_DAYS = tuple(row[0] for row in S_SOURCE if row[1] == "weather")

OUTCOME_KEYS = frozenset({
    "best_share", "compound", "compound_15", "down", "equity", "equity_15",
    "ex_best", "ex_best_ticker", "flat", "joint", "mean_joint", "pnl",
    "ret_15", "ret_after_fee", "under_3", "up", "up_share", "win", "win_rate",
})


def _variant(name: str, s_boost: str) -> dict:
    return {
        "earn_news": False,
        "exit_when": {},
        "forbid": {},
        "hold": HOLD,
        "id": f"{name}__w0",
        "name": name,
        "rank": "hot_score",
        "require": {},
        "s_boost": s_boost,
        "sell": "list",
        "side": "long",
        "skip_first": False,
        "top_n": TOP_N,
        "universe": "union",
        "weather": False,
    }


VARIANTS = (
    _variant("union_hot_n4_h1", "none"),
    _variant("union_hot_n4_holdup", "holdup"),
)


def s_path(session: str, kind: str) -> str:
    if kind == "predict":
        return f"01_daily/general/{session}_predict.md"
    if kind == "weather":
        return f"01_daily/weather/{session}_weather.json"
    raise RuntimeError(kind)


def sources_on(session: str) -> dict[str, str]:
    """present or absent for every locked source. No substitute."""
    if session not in SESSIONS:
        raise RuntimeError(session)
    present = () if session == GAP_DAY else PRICE_SOURCES
    return {name: ("present" if name in present else "absent") for name in ALL_SOURCES}


def price_factor(share_factor: float) -> float:
    if share_factor <= 0:
        raise RuntimeError("split")
    return 1.0 / share_factor


def covered_bytes(text: str) -> bytes:
    idx = text.find(MARKER)
    if idx < 0 or text.find(MARKER, idx + 1) >= 0:
        raise RuntimeError("covered marker")
    return text[idx + len(MARKER):].encode("utf-8")


def fingerprint_sha256(text: str) -> str:
    return hashlib.sha256(covered_bytes(text)).hexdigest()


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_bytes(obj: dict) -> bytes:
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return (raw + "\n").encode("utf-8")


def _reject_outcomes(obj) -> None:
    if isinstance(obj, dict):
        bad = OUTCOME_KEYS.intersection(obj)
        if bad:
            raise RuntimeError("outcome key in a day file")
        for value in obj.values():
            _reject_outcomes(value)
    elif isinstance(obj, list):
        for value in obj:
            _reject_outcomes(value)


def commit_day(session: str, payload: dict, *, days_dir: Path | None = None) -> str:
    """Write days/D.json once. A different rerun raises and does not overwrite."""
    if session not in SESSIONS:
        raise RuntimeError("session")
    if not isinstance(payload, dict):
        raise RuntimeError("payload")
    _reject_outcomes(payload)
    days = days_dir or DAYS
    days.mkdir(parents=True, exist_ok=True)
    idx = SESSIONS.index(session)
    for prev in SESSIONS[:idx]:
        if not (days / f"{prev}.json").is_file():
            raise RuntimeError("out of order")
    for nxt in SESSIONS[idx + 1:]:
        if (days / f"{nxt}.json").is_file():
            raise RuntimeError("later day exists")
    raw = canonical_bytes(payload)
    digest = hashlib.sha256(raw).hexdigest()
    path = days / f"{session}.json"
    if path.exists():
        if path.read_bytes() != raw:
            raise RuntimeError("day bytes differ")
        return digest
    ledger = days / LEDGER_NAME
    path.write_bytes(raw)
    line = canonical_bytes({"bytes": len(raw), "date": session, "sha256": digest})
    with ledger.open("ab") as handle:
        handle.write(line)
    return digest


def verify_ledger(days_dir: Path | None = None) -> None:
    """Every ledger hash matches the day file. An empty ledger is valid."""
    days = days_dir or DAYS
    ledger = days / LEDGER_NAME
    text = ledger.read_bytes() if ledger.is_file() else b""
    if b"\r" in text:
        raise RuntimeError("ledger cr")
    if text and not text.endswith(b"\n"):
        raise RuntimeError("ledger newline")
    rows = []
    for line in text.splitlines():
        if not line:
            raise RuntimeError("blank ledger line")
        rows.append(json.loads(line))
    dates = [row["date"] for row in rows]
    if len(dates) != len(set(dates)):
        raise RuntimeError("duplicate date")
    if tuple(dates) != tuple(SESSIONS[:len(dates)]):
        raise RuntimeError("ledger order")
    files = sorted(path.stem for path in days.glob("*.json"))
    if files != dates:
        raise RuntimeError("day files")
    for row in rows:
        raw = (days / f"{row['date']}.json").read_bytes()
        if hashlib.sha256(raw).hexdigest() != row["sha256"]:
            raise RuntimeError("ledger hash")
        if len(raw) != int(row["bytes"]):
            raise RuntimeError("ledger bytes")
        _reject_outcomes(json.loads(raw))
