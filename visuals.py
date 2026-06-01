import os
import io
import requests
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from database import EcosystemDatabase

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
db = EcosystemDatabase()

def generate_institutional_chart(symbol="SPY"):
    """
    Generates a dark-mode institutional chart plotting the current price
    against the mathematical boundaries stored in the database.
    Returns the file path to the generated image.
    """
    # 1. Pull historical intraday data for the chart backdrop
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1h&outputsize=78&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=10).json()
        if "values" not in res:
            return None
        df = pd.DataFrame(res["values"])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['close'] = df['close'].astype(float)
        df = df.sort_values('datetime')
    except Exception as e:
        print(f"Chart data fetch failed: {e}")
        return None

    # 2. Extract Calculated Levels from the Ecosystem DB
    current_price = df['close'].iloc[-1]
    poc = float(db.get_state(f"{symbol}_poc", current_price))
    expected_upper = float(db.get_state(f"{symbol}_expected_upper", current_price * 1.01))
    expected_lower = float(db.get_state(f"{symbol}_expected_lower", current_price * 0.99))
    vwap = float(db.get_state(f"{symbol}_vwap", current_price))

    # 3. Configure Dark-Mode Bloomberg Aesthetic
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(10, 5), dpi=150)
    fig.patch.set_facecolor('#0d1117') 
    ax.set_facecolor('#0d1117')

    # Plot Price Line
    ax.plot(df['datetime'], df['close'], color='#3498db', linewidth=2, label="Spot Price")

    # Draw Matrix Boundaries
    ax.axhline(y=expected_upper, color='#e74c3c', linestyle='--', linewidth=1.5, alpha=0.8, label="Expected Move Ceiling")
    ax.axhline(y=expected_lower, color='#2ecc71', linestyle='--', linewidth=1.5, alpha=0.8, label="Expected Move Floor")
    ax.axhline(y=poc, color='#f1c40f', linestyle='-', linewidth=2, alpha=0.9, label="Point of Control (POC)")
    ax.axhline(y=vwap, color='#9b59b6', linestyle='-.', linewidth=1.5, alpha=0.8, label="Institutional VWAP")

    # Format Chart
    ax.set_title(f"ESSENTIALS MATRIX: {symbol} Volatility Bounds", color='white', pad=15, weight='bold', fontsize=14)
    ax.tick_params(axis='x', colors='gray', rotation=45)
    ax.tick_params(axis='y', colors='gray')
    ax.grid(color='#30363d', linestyle='-', linewidth=0.5, alpha=0.5)
    
    # Hide top and right spines
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#30363d')
    ax.spines['bottom'].set_color('#30363d')

    # Add legend
    legend = ax.legend(loc="upper left", frameon=True, facecolor='#0d1117', edgecolor='#30363d')
    for text in legend.get_texts(): text.set_color("white")

    plt.tight_layout()
    
    # Save chart locally
    chart_path = os.path.join(BASE_DIR, "matrix_chart.png")
    plt.savefig(chart_path, facecolor=fig.get_facecolor(), edgecolor='none')
    plt.close()
    
    return chart_path

if __name__ == "__main__":
    path = generate_institutional_chart("SPY")
    if path:
        print(f"Chart successfully rendered at: {path}")
