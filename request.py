import pandas as pd

# Load your dataset
df = pd.read_csv('glbx-mdp3-20170601-20170630.ohlcv-1m.csv')
df['ts_event'] = pd.to_datetime(df['ts_event'])  # Converting ISO format to datetime

# Set 'ts_event' as the index
df.set_index('ts_event', inplace=True)

# Filter the data for the date range in June 2017
df = df['2017-06-01':'2017-06-30']

# Initialize counters
days_returned_to_high = 0
days_returned_to_low = 0
total_days = 0

# Analyze each day separately
for date, day_data in df.groupby(df.index.date):
    total_days += 1
    # Data between 13:30 and 14:30
    analysis_session = day_data.between_time('13:30:00', '14:30:00')
    highest_point = analysis_session['high'].max()
    lowest_point = analysis_session['low'].min()

    # Data after 14:30
    post_break_data = day_data.between_time('14:31:00', '16:00:00')
    
    # Check returns to high and low points
    if (post_break_data['high'] >= highest_point).any():
        days_returned_to_high += 1
    if (post_break_data['low'] <= lowest_point).any():
        days_returned_to_low += 1

# Calculate percentages
percentage_high = (days_returned_to_high / total_days) * 100
percentage_low = (days_returned_to_low / total_days) * 100

# Print results
print(f"Percentage of days price returned to the highest point post 14:30: {percentage_high:.2f}%")
print(f"Percentage of days price returned to the lowest point post 14:30: {percentage_low:.2f}%")
