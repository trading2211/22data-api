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

        # Step 1: Fetch DR data (13:30-14:30 UTC+0)
        cur.execute("""
            SELECT date, high, low, close 
            FROM micro_e_mini_sp500_2019_2024_1min 
            WHERE 
                (EXTRACT(HOUR FROM date) = 13 AND EXTRACT(MINUTE FROM date) >= 30)
                OR 
                (EXTRACT(HOUR FROM date) = 14 AND EXTRACT(MINUTE FROM date) <= 30)
            ORDER BY date ASC;
        """)
        dr_data_rows = cur.fetchall()
        dr_columns = [desc[0] for desc in cur.description]
        dr_data = pd.DataFrame(dr_data_rows, columns=dr_columns)

        # Ensure 'date' column is datetime format
        dr_data['date'] = pd.to_datetime(dr_data['date'], errors='coerce')
        dr_data.dropna(subset=['date'], inplace=True)

        # Calculate DR high and low
        dr_high = dr_data['high'].max()
        dr_low = dr_data['low'].min()

        # Step 2: Fetch post-DR data (14:30-19:00 UTC+0)
        cur.execute("""
            SELECT date, high, low, close 
            FROM micro_e_mini_sp500_2019_2024_1min 
            WHERE 
                (EXTRACT(HOUR FROM date) = 14 AND EXTRACT(MINUTE FROM date) > 30)
                OR 
                (EXTRACT(HOUR FROM date) BETWEEN 15 AND 18)
            ORDER BY date ASC;
        """)
        post_dr_rows = cur.fetchall()
        post_dr_data = pd.DataFrame(post_dr_rows, columns=dr_columns)

        post_dr_data['date'] = pd.to_datetime(post_dr_data['date'], errors='coerce')
        post_dr_data.dropna(subset=['date'], inplace=True)

        # Step 3: Calculate inside retracements
        inside_df = dr_data[(dr_data['high'] < dr_high) & (dr_data['low'] > dr_low)]
        inside_df['high_ret'] = (dr_high - inside_df['high']) / dr_high * 100
        inside_df['low_ret'] = (inside_df['low'] - dr_low) / dr_low * 100
        inside_df['value'] = inside_df[['high_ret', 'low_ret']].max(axis=1)
        inside_retracements = inside_df[['date', 'value']].to_dict(orient='records')

        # Step 4: Calculate outside retracements
        current_direction = None
        breakout_price = None

        outside_retracements = []
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

        # Step 5: Calculate max retracements
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
