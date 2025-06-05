# wu-historical-data-recovery
Python script to recover historical weather data from Weather Underground personal weather stations. Features API rate limiting, data validation, monthly file organization, and checkpoint system for interrupted downloads.

# Weather Underground Historical Data Recovery Tool

This Python script allows you to recover historical weather data from Weather Underground for personal weather stations. It handles API rate limiting, data validation, and organizes data into monthly files.

## Features
- Fetches historical weather data from Weather Underground API
- Handles API rate limiting (20 calls per 30 minutes)
- Validates temperature and wind speed data
- Removes duplicate entries
- Creates monthly CSV files
- Includes checkpoint system for interrupted downloads
- Detailed logging of data cleanup

## Requirements
- Python 3.x
- Required Python packages:
  - requests
  - pandas
  - datetime
  - json

Install required packages using:
```pip install requests pandas```

## Configuration
1. Get your API key from Weather Underground
2. Find your station ID
3. Edit the script to set:
   - API key
   - Station ID
   - Start date
   - End date

Example configuration:
```python
api_key = "YOUR_API_KEY_HERE"
station_id = "YOUR_STATION_ID"
start_date = datetime(2018, 1, 1)
end_date = datetime.now()
```
## Data Validation
The script validates:
- Temperatures between -31°C and 31°C
- Wind speeds up to 144.841 kph (90 mph)
- Invalid data is logged and removed

## Output Files
- Temporary files: weather_data_YYYYMM_DD.csv
- Monthly files: weather_data_YYYYMM_complete.csv
- Cleanup log: weather_data_cleanup.log
- Checkpoint file: weather_fetch_checkpoint.json

## CSV File Structure
The output CSV files contain:
- date_time
- temperature_high_c
- temperature_low_c
- temperature_avg_c
- wind_speed_high_kph
- wind_gust_high_kph

## Sample Data
Check the `sample_data` folder for example files:
- sample_output_weather_data_201908_complete.csv - Sample monthly data output
- sample_output_weather_data_cleanup.log - Sample log file
- weather_fetch_checkpoint.json - Sample checkpoint file
- sample_console_output.txt - Sample of script running with API requests and data processing

## Checkpoint System
The script saves progress after each month and can resume from the last completed month if interrupted.

## Rate Limiting
- Maximum 20 API calls per 30 minutes
- Automatic waiting period when limit is reached
- Progress tracking and status updates

## Error Handling
- HTTP error recovery
- API timeout handling
- Invalid data detection
- File operation error handling
