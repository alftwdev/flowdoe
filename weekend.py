# weekend_prep.py
# Schedule on PythonAnywhere to run Fridays at 17:01 EST

from metrics import SystemicSentimentEngine

def execute_weekend_broadcast():
    # Initialize calculated metrics engine
    engine = SystemicSentimentEngine(db_path="/home/yourusername/ecosystem.db")
    
    # Render final string format containing tree-leg layouts
    formatted_payload = engine.generate_weekend_embed_payload()
    
    # Intertwine into the current webhook engine (e.g., matching send_essentials_embed structure)
    try:
        # Assuming your Discord sender takes a raw block or structured text
        # Replace this with the specific import or module handling the webhook delivery
        from ocr import send_essentials_embed 
        
        send_essentials_embed(
            title="WEEKEND PREP SYSTEMIC ANALYSIS", 
            description=formatted_payload, 
            color=0x1A1A1A
        )
        print("[SUCCESS] Weekend summary compiled and pushed down the pipeline.")
    except ImportError:
        # Direct fallback print if running as standalone decoupled tool
        print("\n[STANDALONE RENDER OUTPUT]:")
        print(formatted_payload)

if __name__ == "__main__":
    execute_weekend_broadcast()
