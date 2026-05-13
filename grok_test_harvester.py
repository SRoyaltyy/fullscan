import os
import time
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import sys
import json

# ========================= CONFIG =========================
SHEET_NAME = "Grok test"           # Exact file name
WORKSHEET_NAME = "Stocks"          # Exact tab name

TICKER_CELL = "A2"                 # ← This is where we put the ticker

WAIT_SECONDS = 35                  # 30–40s is safe for 66-category Gemini calls

# ========================= AUTH =========================
def get_sheets_client():
    creds = Credentials.from_service_account_file(
        "service_account.json",
        scopes=["https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)

# ========================= MAIN FUNCTION =========================
def analyze_ticker(ticker: str):
    client = get_sheets_client()
    sheet = client.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
    
    print(f"🔄 Setting ticker in A2 → {ticker}")
    sheet.update_acell(TICKER_CELL, ticker)
    
    print(f"⏳ Waiting {WAIT_SECONDS} seconds for Gemini to run all 66 categories...")
    time.sleep(WAIT_SECONDS)
    
    # Read headers (row 1) and the result row (row 2)
    headers = sheet.row_values(1)
    values = sheet.row_values(2)
    
    # Build clean dictionary: Category → Result
    results = {}
    for i, header in enumerate(headers):
        if i < len(values):
            results[header] = values[i]
        else:
            results[header] = None
    
    print(f"✅ Success for {ticker}")
    return {
        "ticker": ticker,
        "timestamp": datetime.now().isoformat(),
        "results": results
    }

# ========================= BATCH RUN =========================
if __name__ == "__main__":
    if len(sys.argv) > 1:
        tickers_to_process = sys.argv[1:]
    else:
        tickers_to_process = ["APLD"]
    
    all_results = {}
    for ticker in tickers_to_process:
        result = analyze_ticker(ticker)
        all_results[ticker] = result
        
        print(f"\n=== {ticker} — 66 Category Results ===\n")
        for category, value in result["results"].items():
            if value and str(value).strip():
                print(f"{category:<50} : {value}")
        print("-" * 80)
    
    # Save full JSON for GitHub Actions artifact
    with open("grok_analysis_output.json", "w") as f:
        json.dump(all_results, f, indent=2)
    print("\n📁 Full results saved to grok_analysis_output.json")