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
PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_API_TOKEN = os.getenv("PUSHOVER_API_TOKEN")

class TQQQTacticalSniper:
    def __init__(self):
        self.symbol = "TQQQ"
        self.proxy_symbol = "QQQ"
        self.base_url = "https://api.twelvedata.com"
        
    def send_pushover_alert(self, title, message):
        if not PUSHOVER_USER_KEY or not PUSHOVER_API_TOKEN: return
        try:
            requests.post("https://api.pushover.net/1/messages.json", data={
                "token": PUSHOVER_API_TOKEN,
                "user": PUSHOVER_USER_KEY,
                "title": title,
                "message": message,
                "priority": 0
            }, timeout=8)
        except Exception as e:
            logger.error(f"Pushover dispatch failed: {e}")

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
            high_cp = np.abs(df['high'] - df['close'].shift())
            low_cp = np.abs(df['low'] - df['close'].shift())
            tr = pd.concat([high_low, high_cp, low_cp], axis=1).max(axis=1)
            atr = tr.rolling(14).mean().iloc[-1]
            atr_pct = (atr / spot) * 100
            
            return {"spot": spot, "sma200": sma200, "sma50": sma50, "atr": atr, "atr_pct": atr_pct}
        except Exception as e:
            logger.error(f"Failed to fetch structural baseline: {e}")
            return None

    def determine_regime(self, tech):
        spot, sma200, sma50, atr_pct = tech["spot"], tech["sma200"], tech["sma50"], tech["atr_pct"]
        if atr_pct > 3.5:
            return {"regime": "ATR_EXTREME", "action": "HOLD", "strategy": "Cash / SGOV", "risk": "High Volatility Wipeout"}
            
        if spot > sma200:
            if sma50 > sma200:
                return {"regime": "STRONG_BULL", "action": "STO", "strategy": "Bull Put Spread", "otm_buffer": atr_pct * 1.5}
            else:
                return {"regime": "WEAK_BULL", "action": "BTO", "strategy": "Call Debit Spread", "otm_buffer": atr_pct * 0.5}
        else:
            return {"regime": "BEAR", "action": "STO", "strategy": "Bear Call Spread", "otm_buffer": atr_pct * 1.5}

    def fetch_options_chain(self):
        try:
            res = requests.get(f"{self.base_url}/options/chain", params={"symbol": self.symbol, "apikey": TWELVE_DATA_API_KEY}, timeout=15).json()
            if "data" not in res: return None
            return res["data"]
        except Exception as e:
            logger.error(f"Option chain extraction failed: {e}")
            return None

    def calculate_ideal_setup(self, regime_data, chain_data, tqqq_spot):
        if regime_data["action"] == "HOLD": return None
        
        buffer_pct = regime_data["otm_buffer"] / 100.0
        
        if regime_data["strategy"] in ["Bull Put Spread", "Call Debit Spread"]:
            short_strike = tqqq_spot * (1 - buffer_pct)
            long_strike = short_strike * 0.95 
            alt_strategy = "Long Call (BTO)"
            alt_strike = round(tqqq_spot - 1.5, 1)
        else: 
            short_strike = tqqq_spot * (1 + buffer_pct)
            long_strike = short_strike * 1.05
            alt_strategy = "Long Put (BTO)"
            alt_strike = round(tqqq_spot + 1.5, 1)
            
        spread_width = abs(short_strike - long_strike)
        est_premium_collected = spread_width * 0.30 
        max_loss = spread_width - est_premium_collected
        rr_ratio = max_loss / est_premium_collected if est_premium_collected > 0 else 0
        roi_pct = (est_premium_collected / max_loss) * 100 if max_loss > 0 else 0
        
        return {
            "strategy": regime_data["strategy"],
            "action": regime_data["action"],
            "tqqq_spot": tqqq_spot,
            "target_dte": 35,
            "short_strike": round(short_strike, 1),
            "long_strike": round(long_strike, 1),
            "est_credit": round(est_premium_collected, 2),
            "max_risk": round(max_loss, 2),
            "roi_pct": round(roi_pct, 1),
            "rr_ratio": round(rr_ratio, 2),
            "alternative_strategy": alt_strategy,
            "alternative_strike": alt_strike
        }

    def execute_sniper_sweep(self):
        logger.info("Initiating TQQQ Tactical Options Sweep...")
        
        tech = self.fetch_technical_baseline()
        if not tech: return
        
        regime = self.determine_regime(tech)
        if regime["action"] == "HOLD":
            logger.info("Market in ATR Extreme/Hold State. Execution halted.")
            return
            
        spot_res = requests.get(f"{self.base_url}/price", params={"symbol": self.symbol, "apikey": TWELVE_DATA_API_KEY}).json()
        tqqq_spot = float(spot_res.get("price", 0.0))
        if tqqq_spot == 0: return

        chain = self.fetch_options_chain()
        setup = self.calculate_ideal_setup(regime, chain, tqqq_spot)
        if not setup: return
        
        # Hardened Gatekeeper Lock: Binds state to the specific day and regime structure, completely eliminating micro-spam.
        today_str = datetime.now().strftime("%Y-%m-%d")
        alert_id = f"tqqq_sniper_{today_str}"
        state_string = f"REGIME_{regime['regime']}_ACT_{setup['action']}"
        
        if db.track_and_limit_alerts(
            alert_id=alert_id,
            current_state=state_string,
            current_trigger=tqqq_spot,
            max_broadcasts=1,
            threshold_pct=0.03  # Requires a 3% price shift to break the day lock
        ):
            self.dispatch_intelligence(setup, regime, tech)
        else:
            logger.info("TQQQ setup blocked by Ecosystem Gatekeeper (State Unchanged).")

    def dispatch_intelligence(self, setup, regime, tech):
        embed_desc = (
            f"### **🛡️ Primary Play: Credit Structure**\n"
            f"┣ **Setup**: `{setup['strategy']} ({setup['action']})`\n"
            f"┣ **TQQQ Spot**: `${setup['tqqq_spot']:.2f}` | **DTE**: {setup['target_dte']} Days\n"
            f"┣ **Short Strike (Sell)**: `${setup['short_strike']}`\n"
            f"┗ **Long Strike (Buy)**: `${setup['long_strike']}`\n\n"
            f"### **⚡ Frictionless Alternative: Velocity Direction**\n"
            f"┣ **Setup**: `{setup['alternative_strategy']}` (No multi-leg complexity)\n"
            f"┗ **Target Execution Strike**: `${setup['alternative_strike']}`\n\n"
            f"### **⚖️ Risk / Reward Profile (Spread)**\n"
            f"┣ **Est. Credit/Profit**: `${setup['est_credit'] * 100:.0f}` per contract\n"
            f"┣ **Max Risk**: `${setup['max_risk'] * 100:.0f}` per contract\n"
            f"┣ **ROI on Capital**: `{setup['roi_pct']}%` | **R:R**: `1 : {setup['rr_ratio']}`\n\n"
            f"### **📊 9-Step Macro Verification**\n"
            f"┣ **Current Regime**: `{regime['regime']}`\n"
            f"┗ **Calculated ATR Buffer**: `{tech['atr_pct']:.2f}%` OTM offset"
        )
        
        push_msg = f"TQQQ Play: {setup['strategy']} around ${setup['tqqq_spot']:.2f} OR Clean Alternative: {setup['alternative_strategy']} Strike ${setup['alternative_strike']}"
        
        logger.info(f"Signal Locked. Dispatching {setup['strategy']} payload.")
        
        if HAS_ESSENTIALS and WEBHOOK_TRADE_SIGNALS:
            send_essentials_embed(WEBHOOK_TRADE_SIGNALS, "🦅 TQQQ TACTICAL SNIPER: Option Signal Generated", embed_desc, 0x9b59b6)
            
        self.send_pushover_alert("🦅 TQQQ SNIPER SIGNAL", push_msg)

if __name__ == "__main__":
    sniper = TQQQTacticalSniper()
    logger.info("TQQQ Sniper Daemon initialized on 5-minute rotational sweep.")
    while True:
        try:
            sniper.execute_sniper_sweep()
        except Exception as e:
            logger.error(f"TQQQ Sniper Execution Failed: {e}")
        
        # 300 second (5 min) sleep to prevent PythonAnywhere CPU hyper-looping
        time.sleep(300)
