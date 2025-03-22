import os
import asyncio
import aiohttp
import requests
import json
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from pytrends.request import TrendReq

# Define API URLs
NOAA_WEATHER_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
USDA_CROP_DATA_URL = "https://quickstats.nass.usda.gov/api/api_GET/"
TRADE_DATA_URL = "https://api.worldbank.org/v2/en/indicator/TM.TAX.MRCH.SM.AR.ZS?format=json"

# API Keys
NOAA_API_KEY = "vcKoRAknqZosZkNAvdFDqVjRzKogPvuV"
USDA_API_KEY = "93D6A8BC-F435-3C10-AD6A-12453EC6CAE1"

# Data storage directory
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


### **🔹 Fetch CME Historical Prices (Yahoo Finance)**
def fetch_cme_historical_prices():
    """Fetches historical prices for CME Corn, Soybeans, and Rice futures from Yahoo Finance and saves as CSV."""
    FILE_PATH = os.path.join(DATA_DIR, "cme_historical_prices.csv")

    tickers = {
        "Corn": "ZC=F",
        "Soybeans": "ZS=F",
        "Rice": "ZR=F"
    }
    
    combined_data = []
    
    for crop, ticker in tickers.items():
        print(f"🔄 Fetching {crop} futures prices from Yahoo Finance...")
        try:
            data = yf.download(ticker, period="5y", interval="1d")  # Fetch 5 years of daily data
            if data.empty:
                print(f"🚨 No data found for {crop} ({ticker})")
                continue

            data["Crop"] = crop  # Label the data
            data["Ticker"] = ticker
            combined_data.append(data)

            print(f"✅ {crop} data collected: {len(data)} records.")

        except Exception as e:
            print(f"🚨 Error fetching {crop} ({ticker}) data: {e}")

    # Save the final dataset
    if combined_data:
        final_df = pd.concat(combined_data)
        final_df.to_csv(FILE_PATH)
        print(f"✅ CME Historical Prices saved successfully at {FILE_PATH}")
    else:
        print("🚨 No data was saved. Verify API response and try again.")


### **🔹 Fetch USDA Farm Data**
def fetch_usda_farm_data():
    """Fetch farm-level data for Rice, Corn, Soybeans from USDA API."""
    FILE_PATH = os.path.join(DATA_DIR, "usda_farm_data.csv")
    
    crops = ["CORN", "SOYBEANS", "RICE"]
    all_data = []
    page_size = 1000  

    def fetch_crop_data(crop):
        params = {
            "key": USDA_API_KEY,
            "commodity_desc": crop,
            "sector_desc": "CROPS",
            "statisticcat_desc": "AREA HARVESTED",
            "unit_desc": "ACRES",
            "year__GE": 2015,
            "agg_level_desc": "COUNTY",
            "format": "json",
            "page_size": page_size,
            "page": 1
        }

        try:
            response = requests.get(USDA_CROP_DATA_URL, params=params)
            response.raise_for_status()
            data = response.json()

            if "data" in data:
                return data["data"]
            return []

        except requests.exceptions.RequestException as e:
            print(f"🚨 Error fetching USDA data for {crop}: {e}")
            return []

    with ThreadPoolExecutor() as executor:
        results = executor.map(fetch_crop_data, crops)

    for crop_data in results:
        all_data.extend(crop_data)

    if not all_data:
        print("🚨 No USDA farm data found.")
        return None

    df = pd.DataFrame(all_data)
    df.to_csv(FILE_PATH, index=False)
    print(f"✅ USDA farm data saved successfully! {len(df)} records collected.")


### **🔹 Fetch NOAA Weather Data (Async)**
async def fetch_noaa_weather_data():
    """Fetch historical weather data from NOAA API."""
    FILE_PATH = os.path.join(DATA_DIR, "noaa_weather_data.csv")
    
    end_date = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")

    params = {
        "datasetid": "GHCND",
        "stationid": "GHCND:USW00094846",
        "startdate": start_date,
        "enddate": end_date,
        "limit": 1000,
        "units": "metric",
    }
    headers = {"token": NOAA_API_KEY}

    async with aiohttp.ClientSession() as session:
        async with session.get(NOAA_WEATHER_URL, params=params, headers=headers) as response:
            if response.status != 200:
                print(f"🚨 NOAA API error: {response.status}")
                return None

            data = await response.json()
            df = pd.DataFrame(data["results"])
            df.to_csv(FILE_PATH, index=False)
            print(f"✅ NOAA weather data saved successfully! {len(df)} records collected.")
            return df


### **🔹 Fetch Google Trends Data**
def fetch_google_trends():
    """Fetch search interest for Corn, Soybeans, and Rice over time from Google Trends."""
    FILE_PATH = os.path.join(DATA_DIR, "google_trends.csv")

    pytrends = TrendReq(hl='en-US', tz=360)
    keywords = ["Corn", "Soybeans", "Rice"]
    pytrends.build_payload(keywords, timeframe="today 5-y", geo="US")

    data = pytrends.interest_over_time()
    if data.empty:
        print("🚨 No Google Trends data available.")
        return None

    data.to_csv(FILE_PATH)
    print(f"✅ Google Trends data saved successfully! {len(data)} records collected.")


### **🔹 Fetch Trade Data (Imports/Exports)**
async def fetch_trade_data():
    """Fetch global trade data for Corn, Soybeans, and Rice."""
    FILE_PATH = os.path.join(DATA_DIR, "trade_data.csv")

    async with aiohttp.ClientSession() as session:
        async with session.get(TRADE_DATA_URL) as response:
            if response.status != 200:
                print(f"🚨 Trade Data API error: {response.status}")
                return None

            data = await response.json()
            df = pd.DataFrame(data[1])
            df.to_csv(FILE_PATH, index=False)
            print(f"✅ Trade data saved successfully! {len(df)} records collected.")


### **🔹 Run All Tasks**
async def main():
    with ThreadPoolExecutor() as executor:
        executor.submit(fetch_cme_historical_prices)
        executor.submit(fetch_usda_farm_data)
        executor.submit(fetch_google_trends)

    await asyncio.gather(fetch_noaa_weather_data(), fetch_trade_data())
    print("✅ Data collection complete!")


if __name__ == "__main__":
    asyncio.run(main())