import yfinance as yf
import pandas as pd
import os
import time
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

load_dotenv()
WEBHOOK_INCOME = os.getenv("WEBHOOK_INCOME_CCETFS")

# Your categorized watchlist
CORE_INCOME = ["JEPQ", "JEPI", "SPYI", "QQQI", "QYLD", "DIVO", "TSPY", "TDAQ"]
YIELDMAX_HYPE = ["MSTY", "NVDY"]

def get_etf_metrics(ticker):
    """Calculates Total Return and Yield with data cleaning."""
    try:
        data = yf.Ticker(ticker)
        # Fetch 35 days to ensure we have a full month of trading even with weekends
        hist = data.history(period="35d").dropna() 
        
        if hist.empty:
            return None
        
        # Calculate 1-Month Total Return
        # We use the oldest available close in this 35d window as the start
        start_price = hist['Close'].iloc[0]
        end_price = hist['Close'].iloc[-1]
        divs = hist['Dividends'].sum()
        total_return = ((end_price + divs) - start_price) / start_price * 100
        
        info = data.info
        raw_yield = info.get('dividendYield', 0)
        
        # YFinance consistency fix: some yields come as 0.11, some as 11.0
        # If yield > 2 (200%), it's likely already a percentage or a data error
        if raw_yield is not None:
            if raw_yield < 2:
                current_yield = raw_yield * 100
            else:
                current_yield = raw_yield
        else:
            current_yield = 0
            
        return {
            "price": end_price,
            "yield": current_yield,
            "return_1m": total_return
        }
    except Exception as e:
        print(f"Error on {ticker}: {e}")
        return None

def run_income_report():
    print("--- INCOME ETF SCANNER START ---")
    report = "📊 **Monthly Performance Pulse**\n\n"
    
    # 1. Process Core Favorites
    report += "🔹 **Core & Tax-Efficient Income**\n"
    for ticker in CORE_INCOME:
        m = get_etf_metrics(ticker)
        if m:
            indicator = "📈" if m['return_1m'] > 0 else "📉"
            report += f"**{ticker}**: ${m['price']:.2f} | Yield: {m['yield']:.1f}% | 1M Ret: {indicator} {m['return_1m']:.1f}%\n"
        time.sleep(1)

    # 2. Process High-Hype YieldMax
    report += "\n🔥 **Ultra-High Yield (Speculative)**\n"
    for ticker in YIELDMAX_HYPE:
        m = get_etf_metrics(ticker)
        if m:
            # Indicator for YieldMax volatility
            indicator = "🚀" if m['return_1m'] > 10 else ("📈" if m['return_1m'] > 0 else "📉")
            report += f"**{ticker}**: ${m['price']:.2f} | Yield: {m['yield']:.1f}% | 1M Ret: {indicator} {m['return_1m']:.1f}%\n"

    # Dispatch to Discord
    send_essentials_embed(
        webhook_url=WEBHOOK_INCOME,
        title="Income ETF Performance Radar",
        description=report,
        color=0x2ecc71 
    )
    print("--- DISPATCHED TO DISCORD ---")

if __name__ == "__main__":
    run_income_report()
