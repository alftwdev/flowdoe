import os
import csv
from datetime import datetime  # Corrected import for your usage

# --- STEP 1: ABSOLUTE PATH LOGIC ---
# This ensures macro_history.csv is always in /home/alftw/scripts/ or your local dev folder
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HISTORY_FILE = os.path.join(BASE_DIR, 'macro_history.csv')

def save_to_history(vix_price, regime, spy_price): # Add spy_price here
    file_exists = os.path.isfile(HISTORY_FILE)
    try:
        with open(HISTORY_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                # Add spy_price to the header
                writer.writerow(['Date', 'VIX', 'Regime', 'spy_price']) 
            
            writer.writerow([
                datetime.now().strftime('%Y-%m-%d'),
                vix_price,
                regime,
                spy_price # Log the price
            ])
        print(f"✅ History logged to: {HISTORY_FILE}")
    except Exception as e:
        print(f"❌ Error saving history: {e}")

def run_macro_check():
    # --- STEP 2: DATETIME FIX ---
    # Removed the extra '.datetime' that caused your AttributeError
    print(f"\n--- VENTURE MACRO RADAR START: {datetime.now()} ---")
    
    # ... (Rest of your logic to fetch VIX) ...
    vix_current = 15.50  # Placeholder for your fetch logic
    regime = "Risk-On" if vix_current < 20 else "Risk-Off"
    
    save_to_history(vix_current, regime)

if __name__ == "__main__":
    run_macro_check()
