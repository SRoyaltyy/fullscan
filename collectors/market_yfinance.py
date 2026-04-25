"""
Collector: Market Data via yfinance
Sources: Commodity futures, major indices, currency pairs
Schedule: Every 4 hours
Populates: commodity_prices table

yfinance is an unofficial Yahoo Finance wrapper — no API key needed.
Be aware it can break if Yahoo changes their page structure.
"""

import yfinance as yf
from db.db_utils import get_db, ensure_schema

# ── Commodities (futures) ─────────────────────────────────────
COMMODITIES = {
    "CL=F":     "Crude Oil WTI Futures",
    "BZ=F":     "Brent Crude Oil Futures",
    "GC=F":     "Gold Futures",
    "SI=F":     "Silver Futures",
    "HG=F":     "Copper Futures",
    "PL=F":     "Platinum Futures",
    "PA=F":     "Palladium Futures",
    "NG=F":     "Natural Gas Futures",
    "ZW=F":     "Wheat Futures",
    "ZC=F":     "Corn Futures",
    "ZS=F":     "Soybean Futures",
    "KC=F":     "Coffee Futures",
    "CT=F":     "Cotton Futures",
    "SB=F":     "Sugar Futures",
    "LE=F":     "Live Cattle Futures",
    "HE=F":     "Lean Hogs Futures",
    "LBS=F":    "Lumber Futures",
}

# ── Major indices ──────────────────────────────────────────────
INDICES = {
    "^GSPC":    "S&P 500",
    "^DJI":     "Dow Jones Industrial Average",
    "^IXIC":    "NASDAQ Composite",
    "^RUT":     "Russell 2000",
    "^VIX":     "CBOE Volatility Index (VIX)",
    "^TNX":     "10-Year Treasury Yield",
    "^TYX":     "30-Year Treasury Yield",
}

# ── Currency & Dollar ──────────────────────────────────────────
CURRENCIES = {
    "DX-Y.NYB":    "US Dollar Index (DXY)",
    "EURUSD=X":    "EUR/USD",
    "USDJPY=X":    "USD/JPY",
    "GBPUSD=X":    "GBP/USD",
    "USDCNY=X":    "USD/CNY",
}


def collect():
    print("[yfinance] Collecting commodity, index, and currency data...")

    db = get_db()
    ensure_schema(db)
    cursor = db.cursor()

    all_symbols = {**COMMODITIES, **INDICES, **CURRENCIES}
    total_points = 0
    errors = 0

    for symbol, name in all_symbols.items():
        try:
            ticker = yf.Ticker(symbol)
            # Pull last 5 trading days to catch any gaps
            hist = ticker.history(period="5d")

            if hist.empty:
                print(f"  [WARN] {symbol} ({name}): no data returned")
                continue

            count = 0
            for date_idx, row in hist.iterrows():
                date_str = date_idx.strftime("%Y-%m-%d")

                cursor.execute("""
                    INSERT INTO commodity_prices
                        (symbol, name, date, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, date) DO UPDATE SET
                        open = excluded.open,
                        high = excluded.high,
                        low = excluded.low,
                        close = excluded.close,
                        volume = excluded.volume,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    symbol, name, date_str,
                    round(float(row["Open"]), 4) if row["Open"] else None,
                    round(float(row["High"]), 4) if row["High"] else None,
                    round(float(row["Low"]), 4) if row["Low"] else None,
                    round(float(row["Close"]), 4) if row["Close"] else None,
                    int(row["Volume"]) if row["Volume"] else 0,
                ))
                count += 1

            total_points += count
            latest = hist.iloc[-1]
            print(f"  {symbol:12s} ({name:35s}): close={latest['Close']:.2f}  ({count} pts)")

        except Exception as e:
            print(f"  [ERROR] {symbol} ({name}): {e}")
            errors += 1

    db.commit()
    db.close()
    print(f"[yfinance] Done. {total_points} price points updated, {errors} errors.")


if __name__ == "__main__":
    collect()
