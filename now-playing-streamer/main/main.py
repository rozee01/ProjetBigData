import asyncio
import csv
import json
import random
from datetime import datetime
from pathlib import Path

import pandas as pd
import websockets

# Configuration
CSV_FILE = "../../spotify_dataset.csv"
MIN_DELAY = 0.5  # Minimum delay between messages in seconds
MAX_DELAY = 3.0  # Maximum delay between messages in seconds

class DataStreamer:
    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.data = None
        self.load_data()

    def load_data(self):
        """Load the CSV data into memory with error handling"""
        print(f"Loading data from {self.csv_file}...")
        try:
            # Read CSV with error handling
            self.data = pd.read_csv(
                self.csv_file,
                on_bad_lines='skip',  # Skip problematic lines
                encoding='utf-8',
                low_memory=False  # Prevent mixed type inference
            )
            print(f"Successfully loaded {len(self.data)} records")
            
            # Clean the data
            self.data = self.data.dropna(how='all')  # Remove completely empty rows
            self.data = self.data.fillna('')  # Replace NaN with empty string
            
            # Convert all columns to string to ensure JSON serialization
            self.data = self.data.astype(str)
            
        except Exception as e:
            print(f"Error loading CSV: {str(e)}")
            raise

    def get_random_row(self):
        """Get a random row from the dataset"""
        if self.data is None or len(self.data) == 0:
            return None
        random_index = random.randint(0, len(self.data) - 1)
        return self.data.iloc[random_index].to_dict()

async def stream_data(websocket, path):
    """Handle WebSocket connections and stream data"""
    streamer = DataStreamer(CSV_FILE)
    print(f"New client connected: {websocket.remote_address}")
    
    try:
        while True:
            # Get random row from dataset
            row = streamer.get_random_row()
            if row:
                # Add timestamp to the data
                row['timestamp'] = datetime.now().isoformat()
                
                # Send the data as JSON
                await websocket.send(json.dumps(row))
                
                # Random delay between messages
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                await asyncio.sleep(delay)
    except websockets.exceptions.ConnectionClosed:
        print(f"Client disconnected: {websocket.remote_address}")
    except Exception as e:
        print(f"Error: {str(e)}")

async def main():
    # Start WebSocket server
    server = await websockets.serve(stream_data, "localhost", 8765)
    print("WebSocket server started on ws://localhost:8765")
    
    # Keep the server running
    await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
