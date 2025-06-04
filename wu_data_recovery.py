import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import calendar
from collections import deque
import glob
import os
import logging
import json

# Set up logging
logging.basicConfig(
    filename='weather_data_cleanup.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

# Credentials and settings
api_key = "Your API Key Here"  # Replace with your actual API key
station_id = "Station ID"  # Replace with your station ID
start_date = datetime(2018, 1, 1) # Repalce with your start date
end_date = datetime.now()
checkpoint_file = "weather_fetch_checkpoint.json"


def save_checkpoint(year, month):
    """Save the last processed year and month to a checkpoint file."""
    checkpoint = {
        'last_year': year,
        'last_month': month,
        'timestamp': datetime.now().isoformat()
    }
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint, f)
    logging.info(f"Saved checkpoint: Year {year}, Month {month}")


def load_checkpoint():
    """Load the last processed year and month from the checkpoint file."""
    try:
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r') as f:
                checkpoint = json.load(f)
            logging.info(f"Loaded checkpoint: Year {checkpoint['last_year']}, Month {checkpoint['last_month']}")
            return checkpoint['last_year'], checkpoint['last_month']
    except Exception as e:
        logging.error(f"Error loading checkpoint: {str(e)}")
    return None, None


class RateLimiter:
    def __init__(self, max_calls, time_window):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = deque()

    def wait_if_needed(self):
        now = time.time()
        while self.calls and (now - self.calls[0]) >= self.time_window:
            self.calls.popleft()

        if len(self.calls) >= self.max_calls:
            oldest_call_time = self.calls[0]
            wait_time = oldest_call_time + self.time_window - now
            if wait_time > 0:
                print(f"\nRate limit reached. Waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)
                now = time.time()  # Update 'now' after sleeping

            self.calls.clear()  # Clear the queue *only after waiting*

        self.calls.append(now)

    def can_save_file(self, filename):
        return True

    def process_and_save_data(self, data_list, filename):
        if data_list:
            df = pd.DataFrame(data_list)
            df.to_csv(filename, index=False)
            print(f"\nSaved temporary file: {filename}")
            print(f"Records: {len(df)}")


def validate_weather_data(df):
    """Validate and clean weather data"""
    original_len = len(df)

    temp_mask = (
        (df['temperature_high_c'] <= 31) &
        (df['temperature_low_c'] >= -31) &
        (df['temperature_avg_c'] <= 31) &
        (df['temperature_avg_c'] >= -31)
    )

    wind_mask = (
        (df['wind_speed_high_kph'] <= 144.841) &
        (df['wind_gust_high_kph'] <= 144.841)
    )

    invalid_temp_entries = df[~temp_mask]
    invalid_wind_entries = df[~wind_mask]

    if len(invalid_temp_entries) > 0:
        for _, row in invalid_temp_entries.iterrows():
            logging.info(
                f"Removing invalid temperature entry: Date: {row['date_time']}, "
                f"High: {row['temperature_high_c']}°C, Low: {row['temperature_low_c']}°C, Avg: {row['temperature_avg_c']}°C"
            )

    if len(invalid_wind_entries) > 0:
        for _, row in invalid_wind_entries.iterrows():
            logging.info(
                f"Removing invalid wind entry: Date: {row['date_time']}, "
                f"Wind Speed High: {row['wind_speed_high_kph']} kph ({row['wind_speed_high_kph'] * 0.621371:.1f} mph), "
                f"Wind Gust High: {row['wind_gust_high_kph']} kph ({row['wind_gust_high_kph'] * 0.621371:.1f} mph)"
            )

    df = df[temp_mask & wind_mask]

    removed_count = original_len - len(df)
    if removed_count > 0:
        print(f"\nRemoved {removed_count} entries with invalid data:")
        print("- Temperature outside range: -31°C to 31°C")
        print("- Wind speed above 90 mph (144.8 kph)")
        print("Check weather_data_cleanup.log for details")

    return df


def merge_monthly_files(year, month):
    """Merge all temporary files for a given month into one monthly file"""
    pattern = f'weather_data_{year}{month:02d}_*.csv'
    files = glob.glob(pattern)
    if not files:
        return None

    print(f"\nFound {len(files)} files to merge for {year}-{month:02d}:")
    for file in sorted(files):
        print(f"- {file}")

    dfs = []
    total_rows = 0
    for file in sorted(files):
        try:
            df = pd.read_csv(file)
            print(f"\nFile {file}:")
            print(f"Shape before validation: {df.shape}")
            print("Date range:", df['date_time'].min(), "to", df['date_time'].max())
            print("Unique dates:", sorted(pd.to_datetime(df['date_time']).dt.strftime('%Y-%m-%d').unique()))

            total_rows += len(df)
            df = validate_weather_data(df)
            dfs.append(df)
            os.remove(file)
        except pd.errors.EmptyDataError:
            print(f"Warning: File {file} is empty. Skipping.")
            os.remove(file)
        except Exception as e:
            print(f"Error reading file {file}: {e}")

    if dfs:
        merged_df = pd.concat(dfs, ignore_index=True)
        original_len = len(merged_df)

        print(f"\nBefore deduplication:")
        print(f"Total records: {original_len}")
        print("Date range:", merged_df['date_time'].min(), "to", merged_df['date_time'].max())
        print("All dates present:", sorted(pd.to_datetime(merged_df['date_time']).dt.strftime('%Y-%m-%d').unique()))

        merged_df['date_only'] = pd.to_datetime(merged_df['date_time']).dt.strftime('%Y-%m-%d')
        duplicates = merged_df[merged_df.duplicated(subset=['date_only'], keep=False)]
        if not duplicates.empty:
            print("\nDuplicate dates found:")
            for date in sorted(duplicates['date_only'].unique()):
                dupes = duplicates[duplicates['date_only'] == date]
                print(f"\nDate {date} appears {len(dupes)} times:")
                for _, row in dupes.iterrows():
                    print(f"Time: {row['date_time']}, Temp: {row['temperature_avg_c']}°C")

        merged_df = merged_df.drop_duplicates(subset=['date_only'], keep='first')
        merged_df = merged_df.drop(columns=['date_only'])
        merged_df = merged_df.sort_values('date_time')

        duplicates_removed = original_len - len(merged_df)
        if duplicates_removed > 0:
            logging.info(f"Removed {duplicates_removed} duplicate entries during monthly merge for {year}-{month:02d}")
            print(f"\nRemoved {duplicates_removed} duplicate entries during monthly merge")

        print(f"\nAfter deduplication:")
        print(f"Final records: {len(merged_df)}")
        print("Date range:", merged_df['date_time'].min(), "to", merged_df['date_time'].max())
        print("Final dates:", sorted(pd.to_datetime(merged_df['date_time']).dt.strftime('%Y-%m-%d').unique()))

        monthly_filename = f'weather_data_{year}{month:02d}_complete.csv'

        if not merged_df.empty:
            merged_df.to_csv(monthly_filename, index=False)
            print(f"\nMerged monthly data saved to {monthly_filename}")
        else:
            print("\nNo valid data to save after merging and deduplication.")

        save_checkpoint(year, month)

        return merged_df

    return None


def fetch_wu_history_monthly(api_key, station_id, start_date, end_date):
    """
    Fetch historical data from Weather Underground month by month.
    """
    base_url = "https://api.weather.com/v2/pws/history/daily"
    data_list = []
    api_calls = 0
    daily_api_calls = 0

    # Check for checkpoint
    last_year, last_month = load_checkpoint()
    if last_year and last_month:
        print(f"\nFound checkpoint - Resuming from {last_year}-{last_month:02d}")
        # Adjust start date to next month after checkpoint
        if last_month == 12:
            start_date = datetime(last_year + 1, 1, 1)
        else:
            start_date = datetime(last_year, last_month + 1, 1)

    # Initialize rate limiter (20 calls per 30 minutes)
    rate_limiter = RateLimiter(max_calls=20, time_window=1800)

    print(f"Starting data fetch from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    current_date = start_date
    while current_date <= end_date:
        # Get the number of days in the current month
        _, days_in_month = calendar.monthrange(current_date.year, current_date.month)
        month_end = min(
            datetime(current_date.year, current_date.month, days_in_month),
            end_date
        )

        print(f"\nProcessing month: {current_date.strftime('%B %Y')}")

        # Clear any existing temporary files for this month
        temp_files_pattern = f'weather_data_{current_date.strftime("%Y%m")}_*.csv'
        temp_files = glob.glob(temp_files_pattern)
        for file in temp_files:
            try:
                os.remove(file)
                print(f"Removed old temporary file: {file}")
            except Exception as e:
                print(f"Error removing file {file}: {e}")

        # Reset daily API call counter at the beginning of each *day*
        daily_api_calls = 0

        fetch_date = current_date
        last_fetch_date = fetch_date  # Keep track of last fetched date
        while fetch_date <= month_end:
            # Check rate limit before making request
            rate_limiter.wait_if_needed()

            params = {
                'stationId': station_id,
                'format': 'json',
                'units': 'm',
                'date': fetch_date.strftime('%Y%m%d'),
                'apiKey': api_key
            }

            try:
                response = requests.get(base_url, params=params)
                response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
                api_calls += 1
                daily_api_calls += 1

                print(f"\nAPI Request {api_calls}:")
                print(f"URL: {response.url}")
                print(f"Status Code: {response.status_code}")

                daily_data = response.json()

                print("\nResponse Data:")
                print(f"Date: {fetch_date.strftime('%Y-%m-%d')}")
                observations = daily_data.get('observations', [])
                print(f"Observations found: {len(observations)}")

                if observations:
                    print("\nSample observation data:")
                    print(json.dumps(observations[0], indent=2))

                for obs in observations:
                    data_list.append({
                        'date_time': obs.get('obsTimeLocal'),
                        'temperature_high_c': obs.get('metric', {}).get('tempHigh'),
                        'temperature_low_c': obs.get('metric', {}).get('tempLow'),
                        'temperature_avg_c': obs.get('metric', {}).get('tempAvg'),
                        'wind_speed_high_kph': obs.get('metric', {}).get('windspeedHigh'),
                        'wind_gust_high_kph': obs.get('metric', {}).get('windgustHigh'),
                    })

                # Save after every 20 calls OR at the end of the month
                if daily_api_calls % 20 == 0 or fetch_date == month_end:
                    filename = f'weather_data_{current_date.strftime("%Y%m")}_{fetch_date.strftime("%d")}.csv'
                    if rate_limiter.can_save_file(filename):
                        rate_limiter.process_and_save_data(data_list, filename)
                        if fetch_date != month_end:
                            data_list.clear()

                time.sleep(1)
                last_fetch_date = fetch_date

            except requests.exceptions.HTTPError as http_err:
                print(f"\nHTTP error occurred: {http_err}")
                logging.error(f"HTTP error: {http_err}")
                time.sleep(60)  # Wait a minute before retrying

            except Exception as e:
                print(f"\nAn error occurred: {str(e)}")
                logging.error(f"Error: {str(e)}")
                time.sleep(60)  # Wait a minute before retrying

            fetch_date += timedelta(days=1)

        # Merge and save monthly data
        if data_list:
            merged_df = merge_monthly_files(current_date.year, current_date.month)
            if merged_df is not None:
                print(f"\nMonthly Summary for {current_date.strftime('%B %Y')}:")
                print(f"Records in this month: {len(merged_df)}")
                print(f"Temperature range: {merged_df['temperature_low_c'].min():.1f}°C to {merged_df['temperature_high_c'].max():.1f}°C")

            data_list.clear()

        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)

    return None


if __name__ == "__main__":
    try:
        print("Starting Weather Underground data fetch...")
        fetch_wu_history_monthly(api_key, station_id, start_date, end_date)
        print("\nData fetch completed successfully!")
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Any saved data files will be available.")
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        print("Check saved files for partial data.")
