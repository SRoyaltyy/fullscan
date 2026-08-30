"""US ticker universe from NASDAQ Trader symbol directories (ex-ETF)."""
import csv
import re

JUNK_NAME = re.compile(r"warrant|unit|right|acquisition|blank check|spac|"
                       r"debenture|preferred|depositary|notes\b|% ", re.I)


def _clean(sym, name):
    if JUNK_NAME.search(name or ""):
        return False
    if len(sym) > 5:
        return False
    return True


def load_universe(data_dir="data"):
    """Return list of dicts: symbol, name, exchange. Excludes ETFs, test issues,
    NextShares, warrants/units/rights/SPACs/preferreds."""
    out = []
    # NASDAQ-listed
    with open(f"{data_dir}/nasdaqlisted.txt", encoding="utf-8") as fh:
        for rec in csv.DictReader(fh, delimiter="|"):
            sym = rec.get("Symbol", "")
            if not sym or sym == "Symbol" or sym.startswith("File"):
                continue
            if rec.get("ETF") == "Y" or rec.get("Test Issue") == "Y":
                continue
            if rec.get("NextShares") == "Y":
                continue
            if _clean(sym, rec.get("Security Name", "")):
                out.append({"symbol": sym, "name": rec.get("Security Name", ""),
                            "exchange": "NASDAQ"})
    # NYSE / AMEX / ARCA listed
    with open(f"{data_dir}/otherlisted.txt", encoding="utf-8") as fh:
        for rec in csv.DictReader(fh, delimiter="|"):
            sym = rec.get("ACT Symbol", "")
            if not sym or sym == "ACT Symbol" or sym.startswith("File"):
                continue
            if rec.get("ETF") == "Y" or rec.get("Test Issue") == "Y":
                continue
            exch = {"N": "NYSE", "A": "AMEX", "P": "ARCA", "Z": "BATS",
                    "V": "IEX"}.get(rec.get("Exchange", ""), rec.get("Exchange", ""))
            if _clean(sym, rec.get("Security Name", "")):
                out.append({"symbol": sym, "name": rec.get("Security Name", ""),
                            "exchange": exch})
    return out


if __name__ == "__main__":
    u = load_universe()
    print(f"{len(u)} non-ETF tickers")
    from collections import Counter
    print(Counter(x["exchange"] for x in u))
