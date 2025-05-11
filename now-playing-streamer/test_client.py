import asyncio
import json
from datetime import datetime

import websockets


async def connect_and_listen():
    uri = "ws://localhost:8765"
    print(f"Connecting to {uri}...")
    
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected! Waiting for messages...")
            print("-" * 50)
            
            while True:
                message = await websocket.recv()
                data = json.loads(message)
                
                # Print the received data in a readable format
                print(f"\nReceived at: {datetime.now().strftime('%H:%M:%S')}")
                for key, value in data.items():
                    print(f"{key}: {value}")
                print("-" * 50)
                
    except websockets.exceptions.ConnectionClosed:
        print("Connection closed by server")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    print("Starting WebSocket client...")
    asyncio.run(connect_and_listen()) 