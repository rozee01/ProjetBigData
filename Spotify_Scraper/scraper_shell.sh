#!/bin/bash

# Set up environment
export PATH="/usr/local/bin:$PATH"
cd /path/to/your/project

# Run the scraper
python3 spotify_scraper.py >> logs/cron.log 2>&1