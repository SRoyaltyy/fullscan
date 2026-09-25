"""Excel's five OOS-0914 books.

Fit only on bars through 2026-09-11. A test run loads the saved fit and
does not train again. Each book buys the top 4 at the open and sells
them at that session's close, equal weight, Futubull fees.
"""
from __future__ import annotations

import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

from . import factor_mine as fm
from . import factor_mine_oos0914 as oos
from . import paper_trade as pt

FEATURES = (
    "ret1", "ret5", "ret20", "gap1", "range1", "relvol", "log_relvol",
    "atr14_pct", "dist_hi20", "dist_lo20", "log_price", "log_dolvol",
    "ret1_x_log_relvol",
)
FIT_PATH = oos.OUT_DIR / "excel_fit.pkl"
CAND_DIR = oos.ROOT / "data" / "factor_mine" / "candidates"


def _as_day(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series).dt.strftime("%Y-%m-%d")


def load_prices(max_date: str, *, allow_test: bool) -> pd.DataFrame:
    """OHLC through ``max_date``. Train never opens the retro store."""
    if not allow_test and max_date >= oos.CUTOFF:
        raise oos.FutureLeak(f"excel prices cannot go through {max_date}")
    ended = oos._meta_end(oos.LIVE_META)
    if ended and ended >= oos.CUTOFF and not allow_test:
        raise oos.FutureLeak(f"live price meta ends {ended}")
    cols = ["date", "ticker", "open", "high", "low", "close", "volume"]
    live = pd.read_parquet(oos.LIVE_OHLC, columns=cols)
    live["date"] = _as_day(live["date"])
    live = live[live["date"] <= min(max_date, oos.TRAIN_END)]
    if allow_test and max_date >= oos.CUTOFF:
        retro = pd.read_parquet(oos.RETRO_OHLC, columns=cols)
        retro["date"] = _as_day(retro["date"])
        retro = retro[(retro["date"] >= oos.CUTOFF) & (retro["date"] <= max_date)]
        live = pd.concat([live, retro], ignore_index=True)
    live["ticker"] = live["ticker"].astype(str).str.upper()
    if (not allow_test) and bool((live["date"] >= oos.CUTOFF).any()):
        raise oos.FutureLeak("excel frame contains a bar on or after the cutoff")
    return live


def build_features(frame: pd.DataFrame) -> pd.DataFrame:
    """One row per stock-day. Features use bars through the prior close."""
    df = frame.sort_values(["ticker", "date"]).copy()
    g = df.groupby("ticker", sort=False)
    prev_close = g["close"].shift(1)
    prev2 = g["close"].shift(2)
    tr = pd.concat([
        (df["high"] - df["low"]).abs(),
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    df["_tr"] = tr
    df["_dollar"] = df["close"] * df["volume"]
    out = pd.DataFrame({
        "date": df["date"].values,
        "ticker": df["ticker"].values,
        "open": df["open"].values,
        "close": df["close"].values,
        "ret1": (prev_close / prev2 - 1.0).values,
    })
    out["ret5"] = (prev_close / g["close"].shift(6) - 1.0).values
    out["ret20"] = (prev_close / g["close"].shift(21) - 1.0).values
    out["gap1"] = (g["open"].shift(1) / prev2 - 1.0).values
    out["range1"] = ((g["high"].shift(1) - g["low"].shift(1)) / prev2).values
    df["vol_s2"] = g["volume"].shift(2)
    df["tr_s1"] = g["_tr"].shift(1)
    df["hi_s1"] = g["high"].shift(1)
    df["lo_s1"] = g["low"].shift(1)
    df["dol_s1"] = g["_dollar"].shift(1)
    g2 = df.groupby("ticker", sort=False)
    vol_avg = g2["vol_s2"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True).reindex(df.index)
    atr14 = g2["tr_s1"].rolling(14, min_periods=14).mean().reset_index(level=0, drop=True).reindex(df.index)
    hi20 = g2["hi_s1"].rolling(20, min_periods=20).max().reset_index(level=0, drop=True).reindex(df.index)
    lo20 = g2["lo_s1"].rolling(20, min_periods=20).min().reset_index(level=0, drop=True).reindex(df.index)
    dol = g2["dol_s1"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True).reindex(df.index)
    out["relvol"] = (g["volume"].shift(1) / vol_avg).to_numpy()
    out["atr14_pct"] = (atr14 / prev_close).to_numpy()
    out["dist_hi20"] = (prev_close / hi20 - 1.0).to_numpy()
    out["dist_lo20"] = (prev_close / lo20 - 1.0).to_numpy()
    out["log_price"] = np.log(prev_close.where(prev_close > 0)).to_numpy()
    out["log_dolvol"] = np.log(dol.where(dol > 0)).to_numpy()
    out["prev_close"] = prev_close.values
    out["avg_dollar"] = dol.values
    with np.errstate(divide="ignore", invalid="ignore"):
        out["log_relvol"] = np.log(out["relvol"].where(out["relvol"] > 0))
    out["ret1_x_log_relvol"] = out["ret1"] * out["log_relvol"]
    out["target"] = df["close"] / df["open"] - 1.0 - 0.0015
    out.loc[~np.isfinite(out["target"]), "target"] = np.nan
    return out


def _zscore(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.DataFrame:
    """Median-fill, cross-sectional z-score, cap at ±5. A flat day is zero."""
    out = df.copy()
    date = out["date"]
    for col in cols:
        med = out.groupby(date)[col].transform("median")
        filled = out[col].fillna(med)
        mu = filled.groupby(date).transform("mean")
        m2 = (filled * filled).groupby(date).transform("mean")
        var = (m2 - mu * mu).clip(lower=0.0)
        sd = np.sqrt(var.to_numpy())
        z = np.zeros(len(out), dtype=float)
        raw = filled.to_numpy(dtype=float)
        mu_v = mu.to_numpy(dtype=float)
        ok = np.isfinite(raw) & np.isfinite(mu_v) & (sd > 1e-12)
        z[ok] = (raw[ok] - mu_v[ok]) / sd[ok]
        out[col] = np.clip(z, -5.0, 5.0)
    return out


def _train_mask(feat: pd.DataFrame, spec: dict) -> pd.Series:
    uni = spec.get("train_universe") or {}
    ret_ok = feat["ret1"].abs() >= float(uni.get("ret1_abs_min") or 0.05)
    rvol_ok = feat["relvol"] >= float(uni.get("relvol_min") or 2.0)
    price_ok = feat["prev_close"] >= float(uni.get("min_prev_close") or 1.0)
    dol_ok = feat["avg_dollar"] >= float(uni.get("min_avg_dollar_vol") or 1_000_000)
    return (ret_ok | rvol_ok) & price_ok & dol_ok & feat["target"].notna()


def _xy(frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    return frame.loc[:, FEATURES].to_numpy(dtype=float), frame["target"].to_numpy(dtype=float)


def _top_mean(dates, scores_by_date, target_by_date) -> float:
    days = []
    for date in dates:
        scores = scores_by_date.get(date)
        if not scores:
            continue
        ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))[:4]
        targets = target_by_date.get(date) or {}
        vals = [targets[t] for t, _ in ranked if t in targets]
        if vals:
            days.append(float(np.mean(vals)))
    if not days:
        return -1e9
    return float(np.mean(days))


def _chunks(dates: list[str], n: int) -> list[list[str]]:
    dates = list(dates)
    return [list(part) for part in np.array_split(np.array(dates, dtype=object), n)]


def _cv_ridge(train: pd.DataFrame, alphas: list[float]) -> float:
    from sklearn.linear_model import Ridge

    dates = sorted(train["date"].unique())
    parts = _chunks(dates, 6)
    target = {
        date: dict(zip(g["ticker"], g["target"]))
        for date, g in train.groupby("date")
    }
    best_alpha, best_score = alphas[0], -1e18
    for alpha in alphas:
        fold_scores = []
        for k in range(1, 6):
            tr_dates = set(d for part in parts[:k] for d in part)
            va_dates = parts[k]
            if not tr_dates or not va_dates:
                continue
            sub = train[train["date"].isin(tr_dates)]
            model = Ridge(alpha=float(alpha))
            x, y = _xy(sub)
            model.fit(x, y)
            scores = {}
            for date, g in train[train["date"].isin(va_dates)].groupby("date"):
                pred = model.predict(g.loc[:, FEATURES].to_numpy(dtype=float))
                scores[date] = dict(zip(g["ticker"], pred))
            fold_scores.append(_top_mean(va_dates, scores, target))
        score = float(np.mean(fold_scores)) if fold_scores else -1e18
        if score > best_score:
            best_alpha, best_score = float(alpha), score
    return best_alpha


def _cv_logit(train: pd.DataFrame, choices: list[float], threshold: float) -> float:
    from sklearn.linear_model import LogisticRegression

    dates = sorted(train["date"].unique())
    parts = _chunks(dates, 6)
    y_all = (train["target"] > threshold).astype(int)
    best_c, best_score = choices[0], -1e18
    target = {
        date: dict(zip(g["ticker"], g["target"]))
        for date, g in train.groupby("date")
    }
    for c_value in choices:
        fold_scores = []
        for k in range(1, 6):
            tr_dates = set(d for part in parts[:k] for d in part)
            va_dates = parts[k]
            if not tr_dates or not va_dates:
                continue
            sub_x = train.loc[train["date"].isin(tr_dates), FEATURES].to_numpy(dtype=float)
            sub_y = y_all.loc[train["date"].isin(tr_dates)].to_numpy()
            if len(set(sub_y.tolist())) < 2:
                continue
            model = LogisticRegression(
                C=float(c_value), random_state=7, max_iter=400, solver="lbfgs",
            )
            model.fit(sub_x, sub_y)
            scores = {}
            va = train[train["date"].isin(va_dates)]
            for date, g in va.groupby("date"):
                proba = _positive_proba(model, g.loc[:, FEATURES].to_numpy(dtype=float))
                scores[date] = dict(zip(g["ticker"], proba))
            fold_scores.append(_top_mean(va_dates, scores, target))
        score = float(np.mean(fold_scores)) if fold_scores else -1e18
        if score > best_score:
            best_c, best_score = float(c_value), score
    return best_c


def _positive_proba(model, x: np.ndarray) -> np.ndarray:
    classes = list(model.classes_)
    if 1 not in classes:
        return np.zeros(len(x))
    return model.predict_proba(x)[:, classes.index(1)]


def fit_models(feat: pd.DataFrame, spec: dict) -> dict:
    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.linear_model import LogisticRegression, Ridge

    train = _zscore(feat.loc[_train_mask(feat, spec)].copy(), FEATURES)
    models = {row["id"]: row for row in spec["models"]}
    alpha = _cv_ridge(train, list(models["ridge_px"]["alphas"]))
    c_value = _cv_logit(
        train, list(models["logit_px"]["C"]), float(models["logit_px"]["positive_above"]),
    )
    x, y = _xy(train)
    ridge = Ridge(alpha=alpha)
    ridge.fit(x, y)
    logit = LogisticRegression(C=c_value, random_state=7, max_iter=400, solver="lbfgs")
    y_bin = (train["target"] > float(models["logit_px"]["positive_above"])).astype(int).to_numpy()
    if len(set(y_bin.tolist())) < 2:
        raise RuntimeError("excel logit has a single class on the train window")
    logit.fit(x, y_bin)
    gbm_spec = models["gbm_px"]
    gbm = HistGradientBoostingRegressor(
        max_depth=int(gbm_spec["max_depth"]),
        max_iter=int(gbm_spec["max_iter"]),
        learning_rate=float(gbm_spec["learning_rate"]),
        min_samples_leaf=int(gbm_spec["min_samples_leaf"]),
        random_state=int(spec.get("seed") or 7),
    )
    gbm.fit(x, y)
    return {
        "alpha": alpha,
        "C": c_value,
        "ridge": ridge,
        "logit": logit,
        "gbm": gbm,
        "spec": spec,
    }


def candidate_names(date: str, *, allow_test: bool) -> list[str] | None:
    if (not allow_test) and date >= oos.CUTOFF:
        raise oos.FutureLeak(date)
    path = CAND_DIR / f"{date}.json"
    if not path.is_file():
        return None
    doc = json.loads(path.read_text(encoding="utf-8"))
    body = str(doc.get("date") or date)[:10]
    if (not allow_test) and body >= oos.CUTOFF:
        raise oos.FutureLeak(body)
    names = []
    for row in doc.get("names") or []:
        ticker = str(row.get("ticker") or "").upper()
        if ticker:
            names.append(ticker)
    return names


def _scores_for_day(fit: dict, day: pd.DataFrame) -> dict[str, dict[str, float]]:
    z = _zscore(day.copy(), FEATURES)
    x = z.loc[:, FEATURES].to_numpy(dtype=float)
    tickers = list(z["ticker"])
    ridge = dict(zip(tickers, fit["ridge"].predict(x)))
    logit = dict(zip(tickers, _positive_proba(fit["logit"], x)))
    gbm = dict(zip(tickers, fit["gbm"].predict(x)))
    ridge_rank = pd.Series(ridge).rank(pct=True, method="average")
    gbm_rank = pd.Series(gbm).rank(pct=True, method="average")
    ens = {t: float(ridge_rank[t] + gbm_rank[t]) / 2.0 for t in tickers}
    return {"ridge_px": ridge, "logit_px": logit, "gbm_px": gbm, "ens_px": ens}


def _pick(scores: dict[str, float], raw: pd.DataFrame, *, limit: float | None) -> list[str]:
    rows = []
    ret1 = dict(zip(raw["ticker"], raw["ret1"]))
    for ticker, score in scores.items():
        if limit is not None and abs(float(ret1.get(ticker) or 0.0)) > limit:
            continue
        rows.append((ticker, float(score)))
    rows.sort(key=lambda item: (-item[1], item[0]))
    return [ticker for ticker, _ in rows[:4]]


def _simulate(dates: list[str], picks_for, bars: dict, model_ids: list[str]) -> dict[str, list]:
    fees = fm.pt_fees()
    cash = {name: float(fm.CAPITAL) for name in model_ids}
    history = {name: [] for name in model_ids}
    for date in dates:
        for name in model_ids:
            start = cash[name]
            picks = picks_for(name, date)
            orders = []
            if picks and start > 0:
                budget = start / float(len(picks))
                for ticker in picks:
                    bar = bars.get((ticker, date)) or {}
                    px = bar.get("open")
                    if px is None or not np.isfinite(px) or px <= 0:
                        continue
                    shares = int(budget // float(px))
                    while shares > 0:
                        fee = float(pt.order_fees(shares, float(px), "buy", fees))
                        if shares * float(px) + fee <= budget + 1e-6:
                            break
                        shares -= 1
                    if shares < 1:
                        continue
                    fee = float(pt.order_fees(shares, float(px), "buy", fees))
                    cash[name] -= shares * float(px) + fee
                    orders.append({
                        "ticker": ticker, "side": "BUY", "shares": shares,
                        "price": round(float(px), 4), "fees": round(fee, 4),
                    })
            sells = []
            pnl_fills = []
            for order in orders:
                bar = bars.get((order["ticker"], date)) or {}
                px = bar.get("close")
                if px is None or not np.isfinite(px) or px <= 0:
                    px = order["price"]
                fee = float(pt.order_fees(order["shares"], float(px), "sell", fees))
                proceeds = order["shares"] * float(px) - fee
                cost = order["shares"] * order["price"] + order["fees"]
                cash[name] += proceeds
                pnl = round(proceeds - cost, 4)
                sells.append({
                    "ticker": order["ticker"], "side": "SELL", "shares": order["shares"],
                    "price": round(float(px), 4), "fees": round(fee, 4), "fill_rule": "close",
                })
                pnl_fills.append({
                    "ticker": order["ticker"], "side": "SELL", "pnl": pnl,
                    "shares": order["shares"], "price": round(float(px), 4),
                })
            equity = round(cash[name], 4)
            mean = round(100.0 * (equity / start - 1.0), 4) if start > 0 else 0.0
            history[name].append({
                "date": date,
                "buys": orders,
                "sells": sells,
                "fills": pnl_fills,
                "fees": round(sum(o["fees"] for o in orders) + sum(s["fees"] for s in sells), 4),
                "cash": round(cash[name], 4),
                "equity": equity,
                "mean": mean,
                "state": {"cash": round(cash[name], 4), "pos": {}},
            })
    return history


def _bars_from(feat: pd.DataFrame, dates: list[str]) -> dict:
    bars = {}
    sub = feat[feat["date"].isin(dates)]
    for row in sub.itertuples(index=False):
        bars[(row.ticker, row.date)] = {"open": row.open, "close": row.close}
    return bars


def _picks_table(fit: dict, feat: pd.DataFrame, dates: list[str], *, allow_test: bool):
    spec = fit["spec"]
    limit = float(spec["models"][-1].get("ret1_abs_max") or 0.40)
    by_date = {}
    for date in dates:
        names = candidate_names(date, allow_test=allow_test)
        if not names:
            by_date[date] = None
            continue
        day = feat[(feat["date"] == date) & (feat["ticker"].isin(names))].copy()
        if day.empty:
            by_date[date] = {name: [] for name in (
                "ridge_px", "logit_px", "gbm_px", "ens_px", "ridge_px_exmega",
            )}
            continue
        scored = _scores_for_day(fit, day)
        by_date[date] = {
            "ridge_px": _pick(scored["ridge_px"], day, limit=None),
            "logit_px": _pick(scored["logit_px"], day, limit=None),
            "gbm_px": _pick(scored["gbm_px"], day, limit=None),
            "ens_px": _pick(scored["ens_px"], day, limit=None),
            "ridge_px_exmega": _pick(scored["ridge_px"], day, limit=limit),
        }
    return by_date


_SCORE_CACHE: dict = {}


def score_dates(dates: list[str], *, allow_test: bool, fit: dict | None = None,
                exclude: str | None = None) -> dict[str, list]:
    if not dates:
        return {}
    key = (tuple(dates), bool(allow_test))
    cached = _SCORE_CACHE.get(key)
    if cached is None:
        frame = load_prices(dates[-1], allow_test=allow_test)
        feat = build_features(frame)
        feat = feat[feat["date"] <= dates[-1]]
        if fit is None:
            spec = oos.load_preregister()["excel"]
            print("[oos0914] excel fitting on the train window", flush=True)
            fit = fit_models(feat[feat["date"] <= oos.TRAIN_END], spec)
            if not allow_test:
                FIT_PATH.parent.mkdir(parents=True, exist_ok=True)
                with FIT_PATH.open("wb") as handle:
                    pickle.dump(fit, handle)
        table = _picks_table(fit, feat, dates, allow_test=allow_test)
        bars = _bars_from(feat, dates)
        cached = {"fit": fit, "table": table, "bars": bars}
        _SCORE_CACHE[key] = cached
    elif fit is not None:
        cached = dict(cached)
        cached["fit"] = fit
    table = cached["table"]
    bars = cached["bars"]
    ban = str(exclude or "").upper()

    def picks_for(name, date):
        day = table.get(date)
        if not day:
            return []
        names = list(day.get(name) or [])
        if ban:
            names = [t for t in names if t != ban]
        return names

    return _simulate(dates, picks_for, bars, [
        "ridge_px", "logit_px", "gbm_px", "ens_px", "ridge_px_exmega",
    ])


def load_fit() -> dict:
    if not FIT_PATH.is_file():
        raise SystemExit("excel fit missing; run mine before score")
    with FIT_PATH.open("rb") as handle:
        return pickle.load(handle)
