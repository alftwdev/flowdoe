import os
import sys
import time
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from database import EcosystemDatabase
try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

logger = logging.getLogger("TQQQ_Sniper")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
db = EcosystemDatabase()

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_TRADE_SIGNALS = os.getenv("WEBHOOK_TRADE_SIGNALS")

class TQQQTacticalSniper:
    def __init__(self):
        self.symbol = "TQQQ"
        self.proxy_symbol = "QQQ"
        self.base_url = "https://api.twelvedata.com"
        
    def fetch_technical_baseline(self):
        params = {"symbol": self.proxy_symbol, "interval": "1day", "outputsize": "200", "apikey": TWELVE_DATA_API_KEY}
        try:
            res = requests.get(f"{self.base_url}/time_series", params=params, timeout=12).json()
            if "values" not in res: return None
            
            df = pd.DataFrame(res["values"])
            df["close"], df["high"], df["low"] = df["close"].astype(float), df["high"].astype(float), df["low"].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)
            
            spot = df["close"].iloc[-1]
            sma200 = df["close"].rolling(window=200).mean().iloc[-1]
            sma50 = df["close"].rolling(window=50).mean().iloc[-1]
            
            high_low = df['high'] - df['low']
            tr = pd.concat([high_low, np.abs(df['high'] - df['close'].shift()), np.abs(df['low'] - df['close'].shift())], axis=1).max(axis=1)
            atr_pct = (tr.rolling(14).mean().iloc[-1] / spot) * 100
            
            return {"spot": spot, "sma200": sma200, "sma50": sma50, "atr_pct": atr_pct}
        except Exception as e: return None

    def determine_regime(self, tech):
        spot, sma200, sma50, atr_pct = tech["spot"], tech["sma200"], tech["sma50"], tech["atr_pct"]
        if atr_pct > 3.5: return {"regime": "ATR_EXTREME", "action": "HOLD", "strategy": "Cash / SGOV", "risk": "High Volatility Wipeout"}
        if spot > sma200:
            if sma50 > sma200: return {"regime": "STRONG_BULL", "action": "STO", "strategy": "Bull Put Spread", "otm_buffer": atr_pct * 1.5}
            else: return {"regime": "WEAK_BULL", "action": "BTO", "strategy": "Call Debit Spread", "otm_buffer": atr_pct * 0.5}
        else: return {"regime": "BEAR", "action": "STO", "strategy": "Bear Call Spread", "otm_buffer": atr_pct * 1.5}

    def fetch_options_chain(self):
        try:
            res = requests.get(f"{self.base_url}/options/chain", params={"symbol": self.symbol, "apikey": TWELVE_DATA_API_KEY}, timeout=15).json()
            return res.get("data", None)
        except: return None

    def calculate_ideal_setup(self, regime_data, tqqq_spot):
        if regime_data["action"] == "HOLD": return None
        buffer_pct = regime_data["otm_buffer"] / 100.0
        
        if regime_data["strategy"] in ["Bull Put Spread", "Call Debit Spread"]:
            short_strike = tqqq_spot * (1 - buffer_pct)
            long_strike = short_strike * 0.95 
            alt_strategy, alt_strike = "Long Call (BTO)", round(tqqq_spot - 1.5, 1)
        else: 
            short_strike = tqqq_spot * (1 + buffer_pct)
            long_strike = short_strike * 1.05
            alt_strategy, alt_strike = "Long Put (BTO)", round(tqqq_spot + 1.5, 1)
            
        spread_width = abs(short_strike - long_strike)
        est_credit = spread_width * 0.30 
        
        return {
            "strategy": regime_data["strategy"], "action": regime_data["action"], "tqqq_spot": tqqq_spot,
            "target_dte": 35, "short_strike": round(short_strike, 1), "long_strike": round(long_strike, 1),
            "est_credit": round(est_credit, 2), "alternative_strategy": alt_strategy, "alternative_strike": alt_strike
        }

    def execute_sniper_sweep(self):
        tech = self.fetch_technical_baseline()
        if not tech: return
        
        regime = self.determine_regime(tech)
        if regime["action"] == "HOLD": return
            
        try: spot_res = requests.get(f"{self.base_url}/price", params={"symbol": self.symbol, "apikey": TWELVE_DATA_API_KEY}).json()
        except: return
        tqqq_spot = float(spot_res.get("price", 0.0))
        if tqqq_spot == 0: return

        setup = self.calculate_ideal_setup(regime, tqqq_spot)
        if not setup: return
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        alert_id = f"tqqq_sniper_{today_str}"
        state_string = f"REGIME_{regime['regime']}_ACT_{setup['action']}"
        
        if db.track_and_limit_alerts(alert_id, state_string, tqqq_spot, max_broadcasts=1, threshold_pct=0.03):
            self.dispatch_intelligence(setup)

    def dispatch_intelligence(self, setup):
        embed_desc = (
            f"### **🛡️ Primary Play: Tactical Structure**\n"
            f"┣ **Setup**: `{setup['strategy']} ({setup['action']})`\n"
            f"┣ **TQQQ Spot**: `${setup['tqqq_spot']:.2f}` | **DTE**: {setup['target_dte']} Days\n"
            f"┣ **Short Strike (Sell)**: `${setup['short_strike']}`\n"
            f"┗ **Long Strike (Buy)**: `${setup['long_strike']}`\n\n"
            f"### **⚡ Frictionless Alternative: Velocity Direction**\n"
            f"┣ **Setup**: `{setup['alternative_strategy']}` (No multi-leg complexity)\n"
            f"┗ **Target Execution Strike**: `${setup['alternative_strike']}`\n"
        )
        if HAS_ESSENTIALS and WEBHOOK_TRADE_SIGNALS:
            send_essentials_embed(WEBHOOK_TRADE_SIGNALS, "🎯 TQQQ SYSTEMIC OPTIONS SNIPER", embed_desc, 0xe67e22)

if __name__ == "__main__":
    logger.info("Initializing TQQQ Tactical Sniper Daemon...")
    sniper = TQQQTacticalSniper()
    while True:
        try:
            sniper.execute_sniper_sweep()
        except Exception as e:
            logger.error(f"Daemon error: {e}")
        # Sleep 15 minutes. Prevents PythonAnywhere from immediately restarting the file.
        time.sleep(900)
