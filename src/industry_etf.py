"""Optional ETF proxies for Finviz industries (for backtest grade only)."""
from __future__ import annotations

# Not exhaustive. Missing key → grade member-basket only.
INDUSTRY_ETF: dict[str, str] = {
    "Aerospace & Defense": "ITA",
    "Airlines": "JETS",
    "Auto Manufacturers": "CARZ",
    "Banks - Diversified": "XLF",
    "Banks - Regional": "KRE",
    "Biotechnology": "XBI",
    "Capital Markets": "IAI",
    "Communication Equipment": "IYZ",
    "Copper": "CPER",
    "Drug Manufacturers - General": "XLV",
    "Drug Manufacturers - Specialty & Generic": "XPH",
    "Gold": "GLD",
    "Home Improvement Retail": "XRT",
    "Internet Content & Information": "FDN",
    "Oil & Gas E&P": "XOP",
    "Oil & Gas Equipment & Services": "OIH",
    "Oil & Gas Integrated": "XLE",
    "Oil & Gas Midstream": "AMLP",
    "REIT - Industrial": "VNQ",
    "REIT - Office": "VNQ",
    "REIT - Residential": "REZ",
    "REIT - Retail": "RTL",
    "Restaurants": "XLY",
    "Semiconductor Equipment & Materials": "SMH",
    "Semiconductors": "SMH",
    "Silver": "SLV",
    "Software - Application": "IGV",
    "Software - Infrastructure": "IGV",
    "Solar": "TAN",
    "Steel": "SLX",
    "Telecom Services": "IYZ",
    "Uranium": "URA",
    "Utilities - Regulated Electric": "XLU",
    "Utilities - Renewable": "ICLN",
}


def etf_for(industry: str) -> str | None:
    return INDUSTRY_ETF.get(industry)
