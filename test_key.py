import os
from dotenv import load_dotenv
import requests

load_dotenv()
key = os.getenv("ALPHAVANTAGE_API_KEY")
print(f"Key Found: {key}")

url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=SPY&apikey={key}"
r = requests.get(url)
print(f"Response: {r.json()}")
