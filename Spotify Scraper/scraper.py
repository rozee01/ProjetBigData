import requests
import pandas as pd
from datetime import datetime
import os

def get_spotify_chart():
    url = 'https://charts-spotify-com-service.spotify.com/public/v0/charts'
    response = requests.get(url)
    chart = []
    for entry in response.json()['chartEntryViewResponses'][0]['entries']:
        chart.append({
            "Rank": entry['chartEntryData']['currentRank'],
            "Artist": ', '.join([artist['name'] for artist in entry['trackMetadata']['artists']]),
            "TrackName": entry['trackMetadata']['trackName']
        })
    df = pd.DataFrame(chart)
    return df

def save_chart_to_file():
    # Get today's date in DD/MM/YYYY format
    today = datetime.now().strftime('%d-%m-%Y')
    
    # Create a directory for the charts if it doesn't exist
    os.makedirs('spotify_charts', exist_ok=True)
    
    # Generate filename
    filename = f'spotify_charts/spotify_top_charts_{today}.csv'
    
    # Get the chart data
    df = get_spotify_chart()
    
    # Save to CSV
    df.to_csv(filename, index=False)
    print(f"Chart saved to {filename}")

if __name__ == "__main__":
    save_chart_to_file()