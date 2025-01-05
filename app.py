import os
import psycopg2
from flask import Flask, jsonify
import pandas as pd

# Connect to Supabase (PostgreSQL)
try:
    conn = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        sslmode="require"  # Use secure SSL connection
    )
    print("Successfully connected to Supabase!")
except Exception as e:
    print("Error connecting to Supabase:", e)
    conn = None

# Initialize Flask app
app = Flask(__name__)

@app.route('/')
def home():
    return "Bienvenue dans ma maison magique !"

@app.route('/get_max_retracement')
def get_max_retracement():
    if conn is None:
        return {"error": "Database connection not available"}, 500

    try:
        cur = conn.cursor()

        # Initialize variables for tracking DR and retracements
        dr_high = float('-inf')
        dr_low = float('inf')
        inside_retracements = []
        outside_retracements = []

        # Batch size for processing
        batch_size = 10000
        offset = 0

        while True:
            # Fetch a batch of data
            cur.execute(f"""
                SELECT id, ts_event, rtype, publisher_id, instrument_id, open, high, low, close, volume, symbol, date 
                FROM micro_e_mini_sp500_2019_2024_1min 
                ORDER BY date ASC 
                LIMIT {batch_size} OFFSET {offset};
            """)
            rows = cur.fetchall()
            if not rows:
                break  # Exit loop when no more data is available

            # Convert batch to DataFrame
            columns = [desc[0] for desc in cur.description]
            batch_df = pd.DataFrame(rows, columns=columns)

            # Ensure 'date' column is in datetime format
            try:
                batch_df['date'] = pd.to_datetime(batch_df['date'], errors='coerce')
            except Exception as e:
                print(f"Error converting 'date' column: {e}")
                return {"error": f"Date conversion error: {e}"}, 500

            # Drop rows where 'date' could not be converted
            invalid_dates = batch_df[batch_df['date'].isna()]
            if not invalid_dates.empty:
                print("Invalid dates found:", invalid_dates)
                batch_df.dropna(subset=['date'], inplace=True)

            # Process DR data (13:30-14:30 UTC+0)
            dr_data = batch_df[
                ((batch_df['date'].dt.hour == 13) & (batch_df['date'].dt.minute >= 30)) |
                ((batch_df['date'].dt.hour == 14) & (batch_df['date'].dt.minute <= 30))
            ]
            if not dr_data.empty:
                dr_high = max(dr_high, dr_data['high'].max())
                dr_low = min(dr_low, dr_data['low'].min())

                # Calculate inside retracements
                inside_df = dr_data[(dr_data['high'] < dr_high) & (dr_data['low'] > dr_low)].copy()
                inside_df.loc[:, 'high_ret'] = (dr_high - inside_df['high']) / dr_high * 100
                inside_df.loc[:, 'low_ret'] = (inside_df['low'] - dr_low) / dr_low * 100
                inside_df.loc[:, 'value'] = inside_df[['high_ret', 'low_ret']].max(axis=1)
                inside_retracements.extend(inside_df[['date', 'value']].to_dict(orient='records'))

            # Process post-DR data (14:30-19:00 UTC+0)
            post_dr_data = batch_df[
                ((batch_df['date'].dt.hour == 14) & (batch_df['date'].dt.minute > 30)) |
                ((batch_df['date'].dt.hour >= 15) & (batch_df['date'].dt.hour <= 18))
            ]
            if not post_dr_data.empty:
                post_dr_array = post_dr_data[['date', 'high', 'low']].to_numpy()
                current_direction = None
                breakout_price = None

                for row in post_dr_array:
                    date, high, low = row

                    if high > dr_high and current_direction != 'up':
                        current_direction = 'up'
                        breakout_price = dr_high
                    elif low < dr_low and current_direction != 'down':
                        current_direction = 'down'
                        breakout_price = dr_low

                    if current_direction == 'up' and breakout_price:
                        retracement = (low - breakout_price) / breakout_price * 100
                        if retracement < 0:
                            outside_retracements.append({'date': date, 'value': abs(retracement)})
                    elif current_direction == 'down' and breakout_price:
                        retracement = (high - breakout_price) / breakout_price * 100
                        if retracement > 0:
                            outside_retracements.append({'date': date, 'value': retracement})

            # Increment offset for next batch
            offset += batch_size

        # Calculate max retracements
        max_inside_ret = max([r['value'] for r in inside_retracements]) if inside_retracements else 0
        max_outside_ret = max([r['value'] for r in outside_retracements]) if outside_retracements else 0

        return jsonify({
            'dr_high': float(dr_high),
            'dr_low': float(dr_low),
            'max_inside_retracement': float(max_inside_ret),
            'max_outside_retracement': float(max_outside_ret),
            'inside_retracements': inside_retracements,
            'outside_retracements': outside_retracements,
        })

    except Exception as e:
        print(f"Error in /get_max_retracement: {e}")
        return {"error": str(e)}, 500


if __name__ == '__main__':
    if conn is not None:
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
    else:
        print("Unable to start Flask app due to database connection issues.")
