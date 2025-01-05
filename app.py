import os
import psycopg2
from flask import Flask, jsonify
import pandas as pd
import numpy as np

# Connect to Supabase (PostgreSQL)
try:
    conn = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        sslmode="require"
    )
    print("Successfully connected to Supabase!")
except Exception as e:
    print("Error connecting to Supabase:", e)
    conn = None

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

        # Fetch all data from the database
        cur.execute("""
            SELECT date, high, low, close 
            FROM micro_e_mini_sp500_2019_2024_1min 
            ORDER BY date ASC;
        """)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=columns)

        # Ensure 'date' column is in datetime format
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df.dropna(subset=['date'], inplace=True)

        # Define DR period (13:30-14:30 UTC+0)
        dr_data = df[
            ((df['date'].dt.hour == 13) & (df['date'].dt.minute >= 30)) |
            ((df['date'].dt.hour == 14) & (df['date'].dt.minute <= 30))
        ]
        
        if dr_data.empty:
            return {"error": "No DR data found for the specified time range"}, 404

        # Calculate DR high and low
        dr_high = dr_data['high'].max()
        dr_low = dr_data['low'].min()

        # Post-DR period (14:30-19:00 UTC+0)
        post_dr_data = df[
            ((df['date'].dt.hour == 14) & (df['date'].dt.minute > 30)) |
            ((df['date'].dt.hour >= 15) & (df['date'].dt.hour <= 18))
        ]

        if post_dr_data.empty:
            return {"error": "No post-DR data found for the specified time range"}, 404

        # Calculate retracements
        retracements = []
        
        for _, row in post_dr_data.iterrows():
            if row['high'] > dr_high:
                retracement = (row['low'] - dr_high) / dr_high * 100
                retracements.append(retracement)
            elif row['low'] < dr_low:
                retracement = (row['high'] - dr_low) / dr_low * 100
                retracements.append(retracement)

        # Bin retracements into a histogram
        bins = np.arange(-2.2, 0.6, 0.1)  # Bins from -2.2 to +0.5 with step size of 0.1
        histogram, bin_edges = np.histogram(retracements, bins=bins)

        # Convert histogram to JSON format
        distribution = {
            "bins": bin_edges.tolist(),
            "counts": histogram.tolist()
        }

        return jsonify(distribution)

    except Exception as e:
        print(f"Error in /get_max_retracement: {e}")
        return {"error": str(e)}, 500


if __name__ == '__main__':
    if conn is not None:
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
    else:
        print("Unable to start Flask app due to database connection issues.")
