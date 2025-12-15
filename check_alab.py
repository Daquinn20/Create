import requests
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('FMP_API_KEY')

r = requests.get(f'https://financialmodelingprep.com/api/v3/quote/ALAB?apikey={api_key}')
data = r.json()

print('ALAB Current Quote:')
print(f"  Price: ${data[0]['price']:.2f}")
print(f"  Change: ${data[0]['change']:.2f}")
print(f"  Change %: {data[0]['changesPercentage']:.2f}%")
print(f"  Name: {data[0]['name']}")
