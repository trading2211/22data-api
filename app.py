import os  # Import os to read environment variables
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
        sslmode="disable"
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

@app.route('/max_drawdown')
def max_drawdown():
    return jsonify({"max_drawdown": "-15%"})

@app.route('/get_data')
def get_data():
    try:
        cur = conn.cursor()
        cur.execute('SELECT * FROM micro_e_mini_sp500_2019_2024_1min ORDER BY date ASC LIMIT 10;')
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        data = [dict(zip(columns, row)) for row in rows]
        return {"data": data}
    except Exception as e:
        return {"error": str(e)}, 500

@app.route('/average_close')
def average_close():
    try:
        cur = conn.cursor()
        cur.execute('SELECT close FROM micro_e_mini_sp500_2019_2024_1min ORDER BY date ASC LIMIT 10000000;')
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=['close'])
        average = df['close'].mean()
        return {"average_close": average}
    except Exception as e:
        return {"error": str(e)}, 500
    
@app.route('/get_dr_data')
def get_dr_data():
    try:
        cur = conn.cursor()
        
        # Get RDR session data (9:30-10:30 UTC-4 = 13:30-14:30 UTC+0)
        cur.execute("""
            SELECT date, high, low, close 
            FROM micro_e_mini_sp500_2019_2024_1min 
            WHERE 
                (EXTRACT(HOUR FROM date) = 13 AND EXTRACT(MINUTE FROM date) >= 30)
                OR 
                (EXTRACT(HOUR FROM date) = 14 AND EXTRACT(MINUTE FROM date) <= 30)
            ORDER BY date ASC;
        """)
        
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        data = [dict(zip(columns, row)) for row in rows]
        
        # Calculate DR high and low
        dr_high = max(row['high'] for row in data)
        dr_low = min(row['low'] for row in data)
        
        return jsonify({
            'dr_high': dr_high,
            'dr_low': dr_low,
            'dr_data': data
        })
        
    except Exception as e:
        return {'error': str(e)}, 500

@app.route('/get_max_retracement')
def get_max_retracement():
    if conn is None:
        return {"error": "Database connection not available"}, 500
    try:
        cur = conn.cursor()
        # ... rest of your code ...
        
        # First get DR data (13:30-14:30 UTC+0)
        cur.execute("""
            SELECT date, high, low, close 
            FROM micro_e_mini_sp500_2019_2024_1min 
            WHERE 
                (EXTRACT(HOUR FROM date) = 13 AND EXTRACT(MINUTE FROM date) >= 30)
                OR 
                (EXTRACT(HOUR FROM date) = 14 AND EXTRACT(MINUTE FROM date) <= 30)
            ORDER BY date ASC;
        """)
        
        dr_data = cur.fetchall()
        dr_columns = [desc[0] for desc in cur.description]
        dr_df = pd.DataFrame(dr_data, columns=dr_columns)
        
        # Calculate DR high and low
        dr_high = dr_df['high'].max()
        dr_low = dr_df['low'].min()
        
        # Get post-DR data (14:30-19:00 UTC+0)
        cur.execute("""
            SELECT date, high, low, close 
            FROM micro_e_mini_sp500_2019_2024_1min 
            WHERE 
                (EXTRACT(HOUR FROM date) = 14 AND EXTRACT(MINUTE FROM date) > 30)
                OR 
                (EXTRACT(HOUR FROM date) BETWEEN 15 AND 18)
            ORDER BY date ASC;
        """)
        
        post_dr_data = cur.fetchall()
        post_dr_df = pd.DataFrame(post_dr_data, columns=dr_columns)
        
        # Initialize retracement lists
        inside_retracements = []
        outside_retracements = []
        
        # Calculate inside DR retracements
        for _, row in dr_df.iterrows():
            if row['high'] < dr_high and row['low'] > dr_low:
                high_ret = (dr_high - row['high']) / dr_high * 100
                low_ret = (row['low'] - dr_low) / dr_low * 100
                inside_retracements.append({
                    'date': row['date'].isoformat(),
                    'value': max(high_ret, low_ret)
                })
        
        # Calculate outside DR retracements
        current_direction = None
        breakout_price = None
        
        for _, row in post_dr_df.iterrows():
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
        
        # Calculate max retracements
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
        print(f"Error in get_max_retracement: {str(e)}")  # Add logging
        return {"error": str(e)}, 500

if __name__ == '__main__':
    if conn is not None:
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
    else:
        print("Unable to start Flask app due to database connection issues.")


