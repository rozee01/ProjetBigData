Here's the complete cron job setup guide in Markdown format:

```markdown
# Spotify Data Scraper - Cron Job Setup Guide

## 📋 Prerequisites
- Python 3.x installed
- Required packages: `pandas`, `requests`
- Existing files:
  - `scraper.py` (your Python scraper)
  - `scraper_shell.sh` (your shell wrapper)

## 🔧 Setup Steps

### 1. Verify File Permissions
```bash
chmod +x spotify_scraper.py
chmod +x run_spotify_scraper.sh
```

### 2. Test Script Execution
```bash
./run_spotify_scraper.sh
```

Check for:
```bash
ls -l data/spotify_charts/  # Should show new CSV files
tail -f spotify_scraper.log  # Check for errors
```

### 3. Configure Cron Job

Edit your crontab:
```bash
crontab -e
```

Add this line (adjust paths as needed):
```bash
0 3 * * * /usr/bin/bash /full/path/to/run_spotify_scraper.sh > /full/path/to/cron.log 2>&1
```

### 4. Verify Cron Setup
```bash
crontab -l  # List your scheduled jobs
```

### 5. Monitoring (Optional)
To monitor future executions:
```bash
# Check cron logs (system dependent)
grep CRON /var/log/syslog  # Ubuntu/Debian
journalctl -u cron.service  # Systemd systems

# Check your script's log
tail -f /full/path/to/spotify_scraper.log
```

## ⚙️ Cron Syntax Explained
```
* * * * * command_to_execute
| | | | |
| | | | +----- Day of week (0-6) (Sunday=0)
| | | +------- Month (1-12)
| | +--------- Day of month (1-31)
| +----------- Hour (0-23)
+------------- Minute (0-59)
```

## 🚨 Troubleshooting
Common issues:
1. **Path problems**: Always use full paths in cron
2. **Environment differences**: Cron runs with minimal environment
3. **Permission errors**: Ensure scripts are executable
4. **Logging**: Check both cron system logs and your script logs