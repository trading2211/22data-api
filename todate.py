import pandas as pd

# Load the CSV file
df = pd.read_csv('glbx-mdp3-20190501-20190531.ohlcv-1m.MESM9.csv')

# Convert nanoseconds to datetime
df['date'] = pd.to_datetime(df['ts_event'], unit='ns')

# Save the new CSV file
df.to_csv('new_date.csv', index=False)