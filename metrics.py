import os
import sqlite3
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Ensure VADER lexicon is available locally
try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon', quiet=True)


class SystemicSentimentEngine:
    def __init__(self, db_path="ecosystem.db"):
        self.db_path = db_path
        self.sia = SentimentIntensityAnalyzer()
        # Institutional macro query targeting structural economic risk and policy
        self.rss_url = "https://news.google.com/rss/search?q=macroeconomics+OR+federal+reserve+OR+liquidity+OR+inflation&hl=en-US&gl=US&ceid=US:en"

    def fetch_rss_headlines(self, limit=20):
        """
        Fetches the top institutional macro headlines via Google News RSS 
        without reliance on heavy external scraping frameworks.
        """
        headlines = []
        try:
            req = urllib.request.Request(
                self.rss_url, 
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            )
            with urllib.request.urlopen(req, timeout=10) as response:
                xml_data = response.read()
                
            root = ET.fromstring(xml_data)
            for item in root.findall('.//item')[:limit]:
                title = item.find('title')
                if title is not None:
                    # Strip source publication tail from title (e.g., " - Bloomberg")
                    clean_title = title.text.split(' - ')[0].strip()
                    headlines.append(clean_title)
        except Exception as e:
            print(f"[ERROR] RSS Sentiment Fetch Failed: {str(e)}")
        return headlines

    def analyze_polarity(self):
        """
        Processes aggregated headlines through VADER to extract structural 
        polarity dynamics.
        """
        headlines = self.fetch_rss_headlines(limit=20)
        if not headlines:
            return {"compound": 0.0, "pos": 0.0, "neg": 0.0, "neu": 1.0, "sample_size": 0}

        total_scores = {"compound": 0.0, "pos": 0.0, "neg": 0.0, "neu": 0.0}
        
        for headline in headlines:
            scores = self.sia.polarity_scores(headline)
            for key in total_scores:
                total_scores[key] += scores[key]

        count = len(headlines)
        avg_scores = {key: round(total_scores[key] / count, 4) for key in total_scores}
        avg_scores["sample_size"] = count
        return avg_scores

    def compute_macro_calm_gauge(self, polarity_metrics):
        """
        Adapts the Bollen Paper 'Calm' metric into an actionable Macro Fear & Greed Index.
        Maps the compound spectrum (-1.0 to +1.0) into a 0-100 institutional layer.
        
        Formula: Index = (Compound Polarity + 1) * 50
        """
        compound = polarity_metrics["compound"]
        
        # Calculate base index line
        fear_greed_index = round((compound + 1) * 50, 1)
        
        # Map to specific multi-dimensional mood states (Calm vs. Anxiety)
        if fear_greed_index >= 75:
            state = "EXCESSIVE GREED / COMPLACENT"
            calm_gauge = "HIGH COMPLACENCY (Anxiety Risk)"
        elif fear_greed_index >= 55:
            state = "GREED / EXPANSIVE"
            calm_gauge = "STABLE / CALM"
        elif fear_greed_index >= 45:
            state = "NEUTRAL / BALANCED"
            calm_gauge = "EQUILIBRIUM"
        elif fear_greed_index >= 25:
            state = "FEAR / PROTECTIVE"
            calm_gauge = "ELEVATED ANXIETY"
        else:
            state = "EXTREME FEAR / LIQUIDATION"
            calm_gauge = "MACRO PANIC (High Distribution Risk)"

        return {
            "index_value": fear_greed_index,
            "regime_state": state,
            "calmness_state": calm_gauge
        }

    def _get_database_fallbacks(self):
        """
        Queries the centralized database to pull calculated states from 
        the daily tracking engines. Fallbacks map safely if records are empty.
        """
        data = {
            "regime": "N/A", "vrp": "N/A", "vix": "N/A",
            "net_liq": "N/A", "liq_delta": "N/A", "yield_spread": "N/A"
        }
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Extract most recent regime data
                cursor.execute("SELECT regime, vrp, vix FROM market_regimes ORDER BY timestamp DESC LIMIT 1")
                regime_row = cursor.fetchone()
                if regime_row:
                    data["regime"] = regime_row[0]
                    data["vrp"] = regime_row[1]
                    data["vix"] = f"{regime_row[2]:.2f}" if isinstance(regime_row[2], (int, float)) else regime_row[2]

                # Extract most recent macro liquidity cluster
                cursor.execute("SELECT net_liquidity, liquidity_delta, yield_spread FROM macro_liquidity ORDER BY timestamp DESC LIMIT 1")
                liq_row = cursor.fetchone()
                if liq_row:
                    data["net_liq"] = f"${liq_row[0]:,.2f}B" if isinstance(liq_row[0], (int, float)) else liq_row[0]
                    data["liq_delta"] = f"${liq_row[1]:+,.2f}B" if isinstance(liq_row[1], (int, float)) else liq_row[1]
                    data["yield_spread"] = f"{liq_row[2]:.2f}%" if isinstance(liq_row[2], (int, float)) else liq_row[2]
        except Exception as e:
            print(f"[WARNING] Database context query bypassed: {str(e)}")
        return data

    def generate_weekend_embed_payload(self):
        """
        Compiles structural calculations and external telemetry into the 
        exact tree-leg formatted Discord payload architecture.
        """
        # Compute fresh text-based sentiment models
        polarity = self.analyze_polarity()
        gauge = self.compute_macro_calm_gauge(polarity)
        
        # Gather trailing metrics from current state storage
        db_metrics = self._get_database_fallbacks()

        # Build structural tree layout
        payload = (
            "====================================================================\n"
            "Title: ROCKEFELLER STRATEGIC INTELLIGENCE | WEEKEND PREP SUMMARY\n"
            f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M')} EST\n"
            "====================================================================\n\n"
            "## 📊 MARKET REGIME & VOLATILITY LAYERS\n"
            f"┣ Regime Mode: {db_metrics['regime']}\n"
            f"┣ Volatility Risk Premium (VRP): {db_metrics['vrp']}\n"
            f"┗ Trailing Average VIX: {db_metrics['vix']}\n\n"
            "## 🏦 SYSTEMIC LIQUIDITY MATRIX\n"
            f"┣ Central Net Liquidity: {db_metrics['net_liq']}\n"
            f"┣ Weekly Liquidity Delta: {db_metrics['liq_delta']}\n"
            f"┗ Institutional Yield Spread: {db_metrics['yield_spread']}\n\n"
            "## 🧠 SYSTEMIC SENTIMENT & CALM GAUGE\n"
            f"┣ Macro RSS Polarity Score: {polarity['compound']:+.4f} (Sample: {polarity['sample_size']} Headlines)\n"
            f"┣ Behavioral Calmness Layer: {gauge['calmness_state']}\n"
            f"┗ Derived Fear & Greed Index: {gauge['index_value']} | {gauge['regime_state']}\n\n"
            "--------------------------------------------------------------------\n"
            "🔒 Operational Outlook: Automated quantitative snapshot of backend telemetry. "
            "Isolate the signals from trading noise before weekly market open.\n"
            "===================================================================="
        )
        return payload


if __name__ == "__main__":
    # Local execution test
    engine = SystemicSentimentEngine()
    print(engine.generate_weekend_embed_payload())
