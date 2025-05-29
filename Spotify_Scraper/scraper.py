#!/usr/bin/env python3
import requests
import pandas as pd
import os
import io
from datetime import datetime
import logging
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from mongo_cnx import save_to_mongo


# Configuration
OUTPUT_DIR = "data/spotify_charts"
LOG_FILE = "logs/spotify_scraper.log"
COUNTRIES = ["us", "gb", "fr", "de", "jp"]  # Add/remove countries as needed
CHART_TYPE = "regional"  # "regional" or "viral"
DATE = datetime.now().strftime("%Y-%m-%d")

# Set up logging
# logging.basicConfig(
#     filename=LOG_FILE,
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )

def fetch_spotify_chart(country, chart_type):
    """Fetch daily chart data from Spotify"""
    url = f"https://spotifycharts.com/{chart_type}/{country}/daily/{DATE}/download"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse CSV data
        df = pd.read_csv(io.StringIO(response.text))
        
        # Add metadata
        df['country'] = country
        df['chart_type'] = chart_type
        df['extraction_date'] = datetime.now().isoformat()
        
        return df
    
    except Exception as e:
        logging.error(f"Error fetching data for {country}: {str(e)}")
        return None

def save_data(df, country):
    """Save data to CSV with timestamp"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    filename = f"{OUTPUT_DIR}/{country}_{DATE}.csv"
    df.to_csv(filename, index=False)
    logging.info(f"Saved data for {country} to {filename}")

def main():
    logging.info("Starting Spotify chart scraping job")
    
    all_data = []
    for country in COUNTRIES:
        logging.info(f"Fetching data for {country}")
        df = fetch_spotify_chart(country, CHART_TYPE)
        if df is not None:
            save_data(df, country)
            records = df.to_dict(orient='records')
            save_to_mongo(records, "spotify_charts")
            all_data.append(df)
    
    # Combine all data if needed
    if all_data:
        combined_df = pd.concat(all_data)
        combined_filename = f"{OUTPUT_DIR}/combined_{DATE}.csv"
        combined_df.to_csv(combined_filename, index=False)
        logging.info(f"Saved combined data to {combined_filename}")

if __name__ == "__main__":
    main()