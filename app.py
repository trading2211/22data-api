import os
import psycopg2
from flask import Flask, jsonify
import pandas as pd

# Connect to Supabase (PostgreSQL) using environment variables
try:
    conn = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        sslmode="require"  # Use 'require' for secure SSL connections
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

        # Step 1: Fetch all data from the database
        cur.execute("""
            SELECT id, ts_event, rtype, publisher_id, instrument_id, open, high, low, close, volume, symbol, date 
            FROM micro_e_mini_sp500_2019_2024_1min
            ORDER BY date ASC;
        """)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=columns)

        # Step 2: Ensure 'date' column is in datetime format
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])  # Drop rows where 'date' could not be parsed

        # Step 3: Extract DR data (13:30-14:30 UTC+0)
        dr_data = df[
            ((df['date'].dt.hour == 13) & (df['date'].dt.minute >= 30)) |
            ((df['date'].dt.hour == 14) & (df['date'].dt.minute <= 30))
        ]

        if dr_data.empty:
            return {"error": "No DR data found for the specified time range"}, 404

        dr_high = dr_data['high'].max()
        dr_low = dr_data['low'].min()

        # Step 4: Extract post-DR data (14:30-19:00 UTC+0)
        post_dr_data = df[
            ((df['date'].dt.hour == 14) & (df['date'].dt.minute > 30)) |
            ((df['date'].dt.hour >= 15) & (df['date'].dt.hour <= 18))
        ]

        if post_dr_data.empty:
            return {"error": "No post-DR data found for the specified time range"}, 404

        # Step 5: Calculate inside and outside retracements
        inside_retracements = []
        outside_retracements = []

        # Inside DR retracements
        for _, row in dr_data.iterrows():
            if row['high'] < dr_high and row['low'] > dr_low:
                high_ret = (dr_high - row['high']) / dr_high * 100
                low_ret = (row['low'] - dr_low) / dr_low * 100
                inside_retracements.append({
                    'date': row['date'].isoformat(),
                    'value': max(high_ret, low_ret)
                })

        # Outside DR retracements
        current_direction = None
        breakout_price = None

        for _, row in post_dr_data.iterrows():
            if row['high'] > dr_high and current_direction != 'up':
                current_direction = 'up'
                breakout_price = dr_high
            elif row['low'] < dr_low and current_direction != 'down':
                current_direction = 'down'
                breakout_price = dr_low

            if current_direction == 'up' and breakout_price:
                retracement = (row['low'] - breakout_price) / breakout_price * 100
                if retracement < 0:
                    outside_retracements.append({
                        'date': row['date'].isoformat(),
                        'value': abs(retracement)
                    })
            elif current_direction == 'down' and breakout_price:
                retracement = (row['high'] - breakout_price) / breakout_price * 100
                if retracement > 0:
                    outside_retracements.append({
                        'date': row['date'].isoformat(),
                        'value': retracement
                    })

        # Step 6: Calculate max retracements
        max_inside_ret = max([r['value'] for r in inside_retracements]) if inside_retracements else 0
        max_outside_ret = max([r['value'] for r in outside_retracements]) if outside_retracements else 0

        return jsonify({
            'dr_high': float(dr_high),
            'dr_low': float(dr_low),
            'max_inside_retracement': float(max_inside_ret),
            'max_outside_retracement': float(max_outside_ret),
            'inside_retracements': inside_retracements,
            'outside_retracements': outside_retracements
        })

    except Exception as e:
        print(f"Error in /get_max_retracement: {e}")
        return {"error": str(e)}, 500


if __name__ == '__main__':
    if conn is not None:
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
    else:
        print("Unable to start Flask app due to database connection issues.")
