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
        
    def fetch_daily_baseline(self):
        params = {"symbol": self.proxy_symbol, "interval": "1day", "outputsize": "200", "apikey": TWELVE_DATA_API_KEY}
        try:
            res = requests.get(f"{self.base_url}/time_series", params=params, timeout=12).json()
            if "values" not in res: return None
            df = pd.DataFrame(res["values"])
            df["close"] = df["close"].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)
            
            return {
                "spot": df["close"].iloc[-1],
                "sma200": df["close"].rolling(window=200).mean().iloc[-1],
                "sma50": df["close"].rolling(window=50).mean().iloc[-1]
            }
        except Exception: return None

    def fetch_intraday_metrics(self):
        params = {"symbol": self.proxy_symbol, "interval": "5min", "outputsize": "100", "apikey": TWELVE_DATA_API_KEY}
        try:
            res = requests.get(f"{self.base_url}/time_series", params=params, timeout=12).json()
            if "values" not in res: return None
            df = pd.DataFrame(res["values"])
            df["close"] = df["close"].astype(float)
            df["volume"] = df["volume"].astype(int)
            df = df.iloc[::-1].reset_index(drop=True)

            # Intraday VWAP & Deviation Mapping
            df['pv'] = df['close'] * df['volume']
            df['vwap'] = df['pv'].rolling(window=78, min_periods=1).sum() / df['volume'].rolling(window=78, min_periods=1).sum()
            vwap_std = df['close'].rolling(window=78, min_periods=2).std()
            df['z_score'] = (df['close'] - df['vwap']) / vwap_std

            # Whale Block Flow Volume Z-Score
            vol_mean = df['volume'].rolling(window=10).mean()
            vol_std = df['volume'].rolling(window=10).std()
            df['vol_z'] = (df['volume'] - vol_mean) / vol_std

            latest = df.iloc[-1]
            return {
                "spot": latest["close"],
                "vwap": latest["vwap"],
                "z_score": latest["z_score"] if pd.notna(latest["z_score"]) else 0.0,
                "vol_z": latest["vol_z"] if pd.notna(latest["vol_z"]) else 0.0
            }
        except Exception: return None

    def evaluate_snipe(self, daily, intraday):
        spot, sma200, sma50 = daily["spot"], daily["sma200"], daily["sma50"]
        z_score, vol_z = intraday["z_score"], intraday["vol_z"]

        macro_bull = spot > sma200
        macro_bear = spot < sma200 or spot > (sma50 * 1.08)

        # Single-Leg Wave Riding Criteria
        if macro_bull and z_score <= -2.0 and vol_z >= 2.0:
            action, contract = "Buy to Open (BTO)", "CALL"
        elif macro_bear and z_score >= 2.0 and vol_z >= 2.0:
            action, contract = "Buy to Open (BTO)", "PUT"
        elif z_score <= -1.5 or z_score >= 1.5:
            # Active Watchlist - Building Pressure
            action, contract = "MONITORING SETUP", "CALL" if z_score < 0 else "PUT"
        else:
            return None

        # Fetch active TQQQ price mapping
        try:
            tqqq_spot = float(requests.get(f"{self.base_url}/price", params={"symbol": self.symbol, "apikey": TWELVE_DATA_API_KEY}).json().get("price", 0.0))
        except: tqqq_spot = 0.0

        if tqqq_spot == 0.0: return None

        return {
            "action": action,
            "contract": contract,
            "z_score": z_score,
            "vol_z": vol_z,
            "qqq_spot": spot,
            "qqq_vwap": intraday["vwap"],
            "tqqq_spot": tqqq_spot
        }

    def execute_sniper_sweep(self):
        daily = self.fetch_daily_baseline()
        intraday = self.fetch_intraday_metrics()
        
        if not daily or not intraday: return
        setup = self.evaluate_snipe(daily, intraday)
        if not setup: return
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        alert_id = f"tqqq_sniper_flow"
        state_string = f"ACT_{setup['action']}_Z_{round(setup['z_score'], 1)}"
        
        # Deploy Gatekeeper limits to prevent rapid-fire execution logs
        if db.track_and_limit_alerts(alert_id, state_string, setup['tqqq_spot'], max_broadcasts=3, threshold_pct=0.01):
            self.dispatch_intelligence(setup)

    def dispatch_intelligence(self, setup):
        if setup['action'] == "MONITORING SETUP":
            status_tag = "⚠️ SETUP FORMING (Monitoring Z-Score Deviation)"
        else:
            status_tag = "🎯 EXECUTION FRAMEWORK (Single-Leg Wave Snipe)"

        payload = (
            f"⚡ TQQQ TACTICAL OPTIONS SNIPER | WHALE FLOW ALIGNMENT\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Market & Flow Metrics (QQQ Proxy):\n"
            f"┣ Intraday Spot:      ${setup['qqq_spot']:,.2f}\n"
            f"┣ Intraday VWAP:      ${setup['qqq_vwap']:,.2f}\n"
            f"┣ Price vs VWAP (Z):  {setup['z_score']:+.1f}σ\n"
            f"┗ Volume Surge (Z):   {setup['vol_z']:+.1f}σ\n\n"
            f"{status_tag}:\n"
            f"┣ Action:             {setup['action']}\n"
            f"┣ Contract:           {setup['contract']}\n"
            f"┣ Strike:             ~${setup['tqqq_spot']:.2f} (0.45 - 0.50 Delta)\n"
            f"┣ Expiration (DTE):   14 - 30 Days\n"
            f"┗ Risk / Reward:      1 : 3.0\n\n"
            f"🛡️ Risk Management Guidelines:\n"
            f"┣ Stop Loss:          35% premium drop OR structural pivot break\n"
            f"┗ Take Profit:        Scale 50% at +100%, trail remainder with 15m 21-EMA\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ ESSENTIALS Macro-Quant Architecture | Data Secured"
        )
        
        if WEBHOOK_TRADE_SIGNALS:
            try: requests.post(WEBHOOK_TRADE_SIGNALS, json={"content": payload}, timeout=10)
            except: pass

if __name__ == "__main__":
    logger.info("Initializing TQQQ Tactical Sniper Daemon...")
    sniper = TQQQTacticalSniper()
    while True:
        try: sniper.execute_sniper_sweep()
        except Exception as e: logger.error(f"Daemon error: {e}")
        time.sleep(900)
